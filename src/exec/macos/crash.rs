//! Crash reports: ReportCrash writes one `.ips` file per crash into the user's
//! `~/Library/Logs/DiagnosticReports`. Its first line is a JSON header (`timestamp`, `name`,
//! `bug_type`, ...); the rest is a JSON body with `pid`, `parentPid`, `procLaunch` and
//! `captureTime` (local time with an offset: `2026-11-03 19:45:56.2893 -0800`).
//!
//! A report belongs to an invocation when it was captured after the invocation started and its
//! process is one of the invocation's tracked processes (same pid, launched when that process
//! started), or its parent is one (which catches crashes of processes too short-lived for any
//! sample to see): a tracked process with that pid that had started, and was not yet seen gone,
//! when the crashed process launched. Without the time it was seen gone (a sample before a daemon
//! restart saw it last) a later process with the same pid can't be told from it. A report whose
//! capture or launch time is there but can't be read is left where it is.

use anyhow::Context;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

/// Slack when comparing times from a report with start times from libproc: report times have a
/// tenth of a millisecond, file times and clocks may differ a little.
const SLACK_US: u64 = 2_000_000;

/// Moves the reports in `reports` that belong to the invocation into `crash_dir`, and returns
/// their new paths. `leader_start` is the supervisor's start time and `tracked` every (pid, start)
/// seen in the invocation; `gone` says, for those a sample saw gone, when it did. All times are
/// microseconds since the Unix epoch. A missing `reports` directory is not an error. `crash_dir`
/// is made if need be, but not its parent, the job's directory: once that is gone (the job was
/// deleted after it ended, before the late collection), its reports stay where they are.
pub fn move_reports(
    reports: &Path,
    leader_start: u64,
    tracked: &[(u32, u64)],
    gone: &HashMap<(u32, u64), u64>,
    crash_dir: &Path,
) -> anyhow::Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(reports) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", reports.display())),
    };
    let mut starts: HashMap<u32, Vec<Life>> = HashMap::new();
    for &(pid, start) in tracked {
        starts.entry(pid).or_default().push(Life {
            start,
            gone: gone.get(&(pid, start)).copied(),
        });
    }
    let since = leader_start.saturating_sub(SLACK_US);
    let mut moved = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("ips") {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_file() {
            continue;
        }
        // Written after the invocation started, judged by the file's own time first (cheap).
        let mtime_us = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        if mtime_us < since {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        if !belongs(&text, since, &starts) {
            continue;
        }
        match std::fs::create_dir(crash_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => return Err(e).with_context(|| format!("creating {}", crash_dir.display())),
        }
        let dest = crash_dir.join(entry.file_name());
        if std::fs::rename(&path, &dest).is_err() {
            // Another filesystem: copy, then remove the original.
            std::fs::copy(&path, &dest)
                .with_context(|| format!("copying {} to {}", path.display(), dest.display()))?;
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        }
        moved.push(dest);
    }
    moved.sort();
    Ok(moved)
}

/// A tracked process: when it started, and when a sample first saw it gone.
#[derive(Clone, Copy)]
struct Life {
    start: u64,
    gone: Option<u64>,
}

/// Whether the report `text` was captured at or after `since` by a tracked process or a child of
/// one.
fn belongs(text: &str, since: u64, starts: &HashMap<u32, Vec<Life>>) -> bool {
    let Some((header, body)) = text.split_once('\n') else {
        return false;
    };
    let Ok(header) = serde_json::from_str::<serde_json::Value>(header) else {
        return false;
    };
    let Ok(body) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    // A time that is there but can't be read (a format this doesn't know) places the report
    // nowhere in time: it is not this invocation's. One that is missing leaves the pids to decide.
    let time = |v: &serde_json::Value, key: &str| -> Result<Option<u64>, ()> {
        match v.get(key) {
            None => Ok(None),
            Some(t) => t.as_str().and_then(parse_time).map(Some).ok_or(()),
        }
    };
    let Ok(captured) = time(&body, "captureTime").and_then(|c| match c {
        Some(c) => Ok(Some(c)),
        None => time(&header, "timestamp"),
    }) else {
        return false;
    };
    if captured.is_some_and(|t| t < since) {
        return false;
    }
    let Ok(launched) = time(&body, "procLaunch") else {
        return false;
    };
    let pid = |key: &str| {
        body.get(key)
            .and_then(|p| p.as_u64())
            .and_then(|p| u32::try_from(p).ok())
    };
    let near = |a: u64, b: u64| a.abs_diff(b) <= SLACK_US;
    if let Some(list) = pid("pid").and_then(|p| starts.get(&p))
        && list
            .iter()
            .any(|life| launched.is_none_or(|l| near(l, life.start)))
    {
        return true;
    }
    // The parent was that tracked process if it had started and was still there at the launch.
    if let Some(list) = pid("parentPid").and_then(|p| starts.get(&p))
        && list.iter().any(|life| {
            launched.is_none_or(|l| {
                l + SLACK_US >= life.start && life.gone.is_none_or(|g| l <= g + SLACK_US)
            })
        })
    {
        return true;
    }
    false
}

/// Parses a report time, `2026-11-03 19:45:56.2893 -0800` (the fraction is optional), into
/// microseconds since the Unix epoch.
fn parse_time(s: &str) -> Option<u64> {
    let mut parts = s.split_whitespace();
    let (date, clock, zone) = (parts.next()?, parts.next()?, parts.next()?);
    let mut d = date.split('-');
    let (y, mo, da): (i64, i64, i64) = (
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
    );
    let (hms, frac) = clock.split_once('.').unwrap_or((clock, ""));
    let mut t = hms.split(':');
    let (h, mi, se): (i64, i64, i64) = (
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
    );
    let frac_us: i64 = if frac.is_empty() {
        0
    } else {
        let digits: String = frac.chars().take(6).collect();
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse::<i64>().ok()? * 10i64.pow(6 - digits.len() as u32)
    };
    let (sign, zh, zm) = match zone.as_bytes() {
        [s @ (b'+' | b'-'), rest @ ..]
            if rest.len() == 4 && rest.iter().all(u8::is_ascii_digit) =>
        {
            (
                if *s == b'-' { -1 } else { 1 },
                zone[1..3].parse::<i64>().ok()?,
                zone[3..5].parse::<i64>().ok()?,
            )
        }
        _ => return None,
    };
    // Years of four digits keep the arithmetic below far from overflowing.
    if !(0..=9999).contains(&y)
        || !(1..=12).contains(&mo)
        || !(1..=31).contains(&da)
        || !(0..=23).contains(&h)
        || !(0..=59).contains(&mi)
        || !(0..=60).contains(&se)
    {
        return None;
    }
    let days = days_from_civil(y, mo, da);
    let secs = days * 86_400 + h * 3600 + mi * 60 + se - sign * (zh * 3600 + zm * 60);
    let us = secs.checked_mul(1_000_000)?.checked_add(frac_us)?;
    u64::try_from(us).ok()
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_report_times() {
        // 2026-11-04 03:45:56.2893 UTC
        assert_eq!(
            parse_time("2026-11-03 19:45:56.2893 -0800"),
            Some(1_793_763_956_289_300)
        );
        assert_eq!(
            parse_time("2026-11-04 03:45:56 +0000"),
            Some(1_793_763_956_000_000)
        );
        assert_eq!(
            parse_time("2026-11-04 09:15:56.00 +0530"),
            Some(1_793_763_956_000_000)
        );
        assert_eq!(parse_time("1970-01-01 00:00:00 +0000"), Some(0));
        for bad in [
            "",
            "2026-11-03",
            "2026-11-03 19:45:56",
            "2026-13-03 19:45:56 -0800",
            "x y z",
        ] {
            assert_eq!(parse_time(bad), None, "{bad}");
        }
    }

    #[test]
    fn bad_report_times_are_not_a_panic() {
        // Any process of the user can write into DiagnosticReports.
        for bad in [
            "2026-11-03 19:45:56 +0\u{e9}0",
            "2026-11-03 19:45:56 -\u{e9}00",
            "9000000000000000000-01-01 00:00:00 +0000",
            "-9000000000000000000-01-01 00:00:00 +0000",
            "2026-11-03 19:45:56.\u{e9}1 +0000",
            "2026-11-03 19:45:-9000000000000000000 +0000",
            "2026-11-03 -9000000000000000000:45:56 +0000",
        ] {
            let got = std::panic::catch_unwind(|| parse_time(bad));
            assert!(matches!(got, Ok(None)), "{bad}: {got:?}");
        }
    }

    fn fmt_time(us: u64) -> String {
        // UTC, in the reports' format.
        let secs = (us / 1_000_000) as i64;
        let days = secs.div_euclid(86_400);
        let rem = secs.rem_euclid(86_400);
        // civil_from_days
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + i64::from(m <= 2);
        format!(
            "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:04} +0000",
            rem / 3600,
            (rem / 60) % 60,
            rem % 60,
            (us % 1_000_000) / 100
        )
    }

    fn report(dir: &Path, name: &str, pid: u32, ppid: u32, launched: u64, captured: u64) {
        let header = serde_json::json!({
            "app_name": "t", "timestamp": fmt_time(captured), "bug_type": "309", "name": "t"
        });
        let body = serde_json::json!({
            "pid": pid, "parentPid": ppid, "procLaunch": fmt_time(launched),
            "captureTime": fmt_time(captured), "procName": "t", "threads": []
        });
        std::fs::write(dir.join(name), format!("{header}\n{body:#}")).unwrap();
    }

    #[test]
    fn moves_only_the_invocations_reports() {
        let tmp = tempfile::tempdir().unwrap();
        let reports = tmp.path().join("DiagnosticReports");
        let crash = tmp.path().join("job/crash");
        std::fs::create_dir_all(&reports).unwrap();
        // The job's directory, which the daemon makes when the job starts.
        std::fs::create_dir(tmp.path().join("job")).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let start = now - 60_000_000; // the invocation started a minute ago
        let tracked = [(40_001, start), (40_002, start + 5_000_000)];
        // A tracked process that crashed during the invocation.
        report(
            &reports,
            "a.ips",
            40_002,
            40_001,
            start + 5_000_000,
            start + 20_000_000,
        );
        // A short-lived child of a tracked process, never sampled.
        report(
            &reports,
            "b.ips",
            50_000,
            40_001,
            start + 30_000_000,
            start + 30_100_000,
        );
        // The same pid as a tracked process, but another process (launched long after).
        report(
            &reports,
            "c.ips",
            40_002,
            1,
            start + 40_000_000,
            start + 41_000_000,
        );
        // A tracked pid, but the crash was before the invocation started.
        report(
            &reports,
            "d.ips",
            40_001,
            1,
            start - 90_000_000,
            start - 80_000_000,
        );
        // Someone else's.
        report(
            &reports,
            "e.ips",
            60_000,
            1,
            start + 1_000_000,
            start + 2_000_000,
        );
        std::fs::write(reports.join("f.diag"), "not a report").unwrap();
        std::fs::write(reports.join("g.ips"), "garbage").unwrap();

        let moved = move_reports(&reports, start, &tracked, &HashMap::new(), &crash).unwrap();
        assert_eq!(moved, vec![crash.join("a.ips"), crash.join("b.ips")]);
        let mut left: Vec<String> = std::fs::read_dir(&reports)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(left, ["c.ips", "d.ips", "e.ips", "f.diag", "g.ips"]);

        // Nothing to move and no reports directory are both fine.
        assert!(
            move_reports(&reports, start, &[], &HashMap::new(), &crash)
                .unwrap()
                .is_empty()
        );
        assert!(
            move_reports(
                &tmp.path().join("none"),
                start,
                &tracked,
                &HashMap::new(),
                &crash
            )
            .unwrap()
            .is_empty()
        );
    }

    /// A report whose times are there but in a form this can't read (another time zone format)
    /// can't be placed in time: it stays where it is, even with a tracked pid (which another
    /// process may have now). One without the times is judged by the pids alone, as before.
    #[test]
    fn a_report_with_times_it_cannot_read_stays() {
        let tmp = tempfile::tempdir().unwrap();
        let reports = tmp.path().join("DiagnosticReports");
        let crash = tmp.path().join("job/crash");
        std::fs::create_dir_all(&reports).unwrap();
        std::fs::create_dir(tmp.path().join("job")).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let start = now - 60_000_000;
        let tracked = [(40_001, start), (40_002, start + 5_000_000)];
        let write =
            |name: &str, pid: u32, ppid: u32, launch: Option<&str>, capture: Option<&str>| {
                let mut header =
                    serde_json::json!({ "app_name": "t", "bug_type": "309", "name": "t" });
                let mut body = serde_json::json!({ "pid": pid, "parentPid": ppid, "threads": [] });
                if let Some(c) = capture {
                    header["timestamp"] = c.into();
                    body["captureTime"] = c.into();
                }
                if let Some(l) = launch {
                    body["procLaunch"] = l.into();
                }
                std::fs::write(reports.join(name), format!("{header}\n{body:#}")).unwrap();
            };
        let good_capture = fmt_time(start + 20_000_000);
        // Another process with a tracked pid, launched later; its launch time in a zone format
        // this doesn't read.
        write(
            "zone.ips",
            40_002,
            1,
            Some("2026-11-03 19:45:56.2893 +05:30"),
            Some(&good_capture),
        );
        // Its capture time unreadable: maybe before the invocation started.
        write(
            "capture.ips",
            40_001,
            1,
            Some(&fmt_time(start)),
            Some("2026-11-03 19:45:56 PST"),
        );
        // A child of a tracked process, its launch time unreadable.
        write(
            "child.ips",
            50_000,
            40_001,
            Some("yesterday"),
            Some(&good_capture),
        );
        // No times at all: the pid decides.
        write("bare.ips", 40_001, 1, None, None);
        let moved = move_reports(&reports, start, &tracked, &HashMap::new(), &crash).unwrap();
        assert_eq!(moved, vec![crash.join("bare.ips")]);
        for name in ["zone.ips", "capture.ips", "child.ips"] {
            assert!(reports.join(name).exists(), "{name}");
        }
    }

    /// A tracked process exits and its pid goes to another process, whose child then crashes:
    /// that report is not the invocation's. A child the tracked process launched before it was
    /// seen gone is.
    #[test]
    fn a_reused_parent_pid_is_not_the_invocations() {
        let tmp = tempfile::tempdir().unwrap();
        let reports = tmp.path().join("DiagnosticReports");
        let crash = tmp.path().join("job/crash");
        std::fs::create_dir_all(&reports).unwrap();
        std::fs::create_dir(tmp.path().join("job")).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let start = now - 600_000_000; // ten minutes ago
        let tracked = [(40_001, start), (40_002, start + 1_000_000)];
        // 40_002 was seen gone 60 s in; its pid then went to another process.
        let gone = HashMap::from([((40_002, start + 1_000_000), start + 60_000_000)]);
        // A child it launched 30 s in, crashing at once.
        report(
            &reports,
            "ours.ips",
            50_001,
            40_002,
            start + 30_000_000,
            start + 30_100_000,
        );
        // A child of the pid's next holder, launched 5 minutes in.
        report(
            &reports,
            "reused.ips",
            50_002,
            40_002,
            start + 300_000_000,
            start + 300_100_000,
        );
        // Without a time it was seen gone, the old rule: any child launched after its start.
        report(
            &reports,
            "unknown.ips",
            50_003,
            40_001,
            start + 300_000_000,
            start + 300_100_000,
        );
        let moved = move_reports(&reports, start, &tracked, &gone, &crash).unwrap();
        assert_eq!(
            moved,
            vec![crash.join("ours.ips"), crash.join("unknown.ips")]
        );
        assert!(reports.join("reused.ips").exists());
    }
}
