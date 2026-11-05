//! Reading and killing a unit's cgroup (cgroup v2, mounted at /sys/fs/cgroup).

use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

pub(super) const ROOT: &str = "/sys/fs/cgroup";

/// The directory of a cgroup given as systemd's ControlGroup property (`/user.slice/...`).
pub(super) fn dir(cgroup: &str) -> PathBuf {
    Path::new(ROOT).join(cgroup.trim_start_matches('/'))
}

/// One reading of a cgroup and everything below it.
#[derive(Debug, Default)]
pub(super) struct Stats {
    pub pids: Vec<u32>,
    /// cpu.stat usage_usec: user and system time of every process that ever ran in it.
    pub cpu_usec: u64,
    /// memory.current.
    pub mem_bytes: u64,
    /// memory.peak (Linux 5.19 and later; 0 before): the most it has used since it was made.
    pub peak_bytes: u64,
    /// memory.events oom_kill + oom_group_kill: processes the OOM killer ended, whatever ran
    /// short (this cgroup's limit, an ancestor's, or the whole machine's memory).
    pub oom_kills: u64,
    /// memory.events oom: times this cgroup, or one below it, reached its own limit with an
    /// allocation about to fail. A shortage above it (an ancestor's limit, the machine) doesn't
    /// count here.
    pub limit_ooms: u64,
}

/// Reads the cgroup at `dir`; `None` once it is gone (the unit has ended and systemd removed it).
pub(super) fn read(dir: &Path) -> io::Result<Option<Stats>> {
    let pids = match pids(dir) {
        Ok(pids) => pids,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let cpu_usec = keyed(&dir.join("cpu.stat"), &["usage_usec"]);
    let events = std::fs::read_to_string(dir.join("memory.events")).unwrap_or_default();
    let oom_kills = parse_keyed(&events, &["oom_kill", "oom_group_kill"]);
    let limit_ooms = parse_keyed(&events, &["oom"]);
    let number = |file: &str| {
        std::fs::read_to_string(dir.join(file))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    };
    Ok(Some(Stats {
        pids,
        cpu_usec,
        mem_bytes: number("memory.current"),
        peak_bytes: number("memory.peak"),
        oom_kills,
        limit_ooms,
    }))
}

/// Every process in the cgroup and in the cgroups below it, sorted. Zombies are not listed.
pub(super) fn pids(dir: &Path) -> io::Result<Vec<u32>> {
    let mut out = Vec::new();
    collect_pids(dir, &mut out, true)?;
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn collect_pids(dir: &Path, out: &mut Vec<u32>, top: bool) -> io::Result<()> {
    // Below the top, a cgroup may vanish while we walk, or be threaded (no cgroup.procs to read):
    // skip it.
    let text = match std::fs::read_to_string(dir.join("cgroup.procs")) {
        Ok(text) => text,
        Err(e) if top => return Err(e),
        Err(_) => return Ok(()),
    };
    out.extend(text.lines().filter_map(|l| l.trim().parse::<u32>().ok()));
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if top => return Err(e),
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            collect_pids(&entry.path(), out, false)?;
        }
    }
    Ok(())
}

/// SIGKILL to every process in the cgroup and below, atomically with respect to forks
/// (cgroup.kill, Linux 5.14+).
pub(super) fn kill(dir: &Path) -> io::Result<()> {
    std::fs::write(dir.join("cgroup.kill"), "1")
}

/// The sum of the named fields of a flat-keyed cgroup file (`key value` per line); 0 if unreadable.
fn keyed(path: &Path, keys: &[&str]) -> u64 {
    let Ok(text) = std::fs::read_to_string(path) else {
        return 0;
    };
    parse_keyed(&text, keys)
}

fn parse_keyed(text: &str, keys: &[&str]) -> u64 {
    text.lines()
        .filter_map(|line| line.split_once(' '))
        .filter(|(k, _)| keys.contains(k))
        .filter_map(|(_, v)| v.trim().parse::<u64>().ok())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flat_keyed_files() {
        let events =
            "low 0\nhigh 0\nmax 5\noom 1\noom_kill 1\noom_group_kill 2\nsock_throttled 0\n";
        assert_eq!(parse_keyed(events, &["oom_kill", "oom_group_kill"]), 3);
        assert_eq!(parse_keyed(events, &["oom"]), 1);
        let stat = "usage_usec 2989000\nuser_usec 1000\nsystem_usec 1989000\n";
        assert_eq!(parse_keyed(stat, &["usage_usec"]), 2_989_000);
        assert_eq!(parse_keyed("", &["usage_usec"]), 0);
    }

    #[test]
    fn paths_from_control_group() {
        assert_eq!(
            dir("/user.slice/user-1000.slice/user@1000.service/app.slice/x.service"),
            Path::new(
                "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/x.service"
            )
        );
    }

    #[test]
    fn reads_this_process_cgroup() {
        // Whatever cgroup this test runs in, it contains this process.
        let Ok(line) = std::fs::read_to_string("/proc/self/cgroup") else {
            return;
        };
        let Some(path) = line.lines().find_map(|l| l.strip_prefix("0::")) else {
            return; // not cgroup v2
        };
        let stats = read(&dir(path)).unwrap().unwrap();
        assert!(stats.pids.contains(&std::process::id()), "{stats:?}");
        assert!(
            read(&dir("/no-such-cgroup-potluck-test"))
                .unwrap()
                .is_none()
        );
    }
}
