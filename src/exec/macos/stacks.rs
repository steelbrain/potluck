//! Stacks of a stalled invocation, from `/usr/bin/sample <pid> 1` per process.
//!
//! `sample` suspends the process every millisecond for one second and prints a call tree per
//! thread (`Call graph:`), a summary sorted by the top of the stack, and a list of loaded images.
//! The images are dropped here: they are long and say little about where a job is stuck.
//!
//! A `sample` killed at the time limit leaves its target running: the kernel lifts a task
//! suspension when the task holding it dies (checked by killing `sample` at random moments,
//! `a_killed_sample_leaves_its_target_running`).

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const SAMPLE: &str = "/usr/bin/sample";
/// At most this many processes are sampled, this many at a time, within this time.
const MAX_PROCS: usize = 16;
const PARALLEL: usize = 8;
const TIME_LIMIT: Duration = Duration::from_secs(30);

struct Running {
    pid: u32,
    child: Child,
    out: PathBuf,
    err: PathBuf,
}

/// Samples each of `pids` (with its name) for one second and returns the reports as one text.
pub(super) fn sample_all(procs: &[(u32, String)]) -> anyhow::Result<String> {
    let dir = scratch_dir()?;
    let result = run(procs, &dir);
    let _ = std::fs::remove_dir_all(&dir);
    result
}

/// Numbers the scratch directories of this process image.
static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A new, empty directory for one round of samples. One that exists already is skipped, never
/// reused: this process before an exec (a restart in place keeps the pid, and SEQ starts again)
/// may have left one behind, with another round's reports in it.
fn scratch_dir() -> anyhow::Result<PathBuf> {
    use std::sync::atomic::Ordering;
    loop {
        let dir = std::env::temp_dir().join(format!(
            "potluck-stacks-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&dir) {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            r => return r.map(|()| dir).map_err(Into::into),
        }
    }
}

fn run(procs: &[(u32, String)], dir: &Path) -> anyhow::Result<String> {
    let deadline = Instant::now() + TIME_LIMIT;
    let mut text = String::new();
    let skipped = procs.len().saturating_sub(MAX_PROCS);
    let mut waiting: Vec<&(u32, String)> = procs.iter().take(MAX_PROCS).collect();
    waiting.reverse();
    let mut running: Vec<Running> = Vec::new();
    let mut done: Vec<(u32, String)> = Vec::new();
    while !waiting.is_empty() || !running.is_empty() {
        while running.len() < PARALLEL
            && let Some((pid, _)) = waiting.pop()
        {
            let out = dir.join(format!("{pid}.txt"));
            let err = dir.join(format!("{pid}.err"));
            let spawned = std::fs::File::create(&err).and_then(|errf| {
                Command::new(SAMPLE)
                    .arg(pid.to_string())
                    .arg("1")
                    .arg("-mayDie")
                    .arg("-file")
                    .arg(&out)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(errf)
                    .spawn()
            });
            match spawned {
                Ok(child) => running.push(Running {
                    pid: *pid,
                    child,
                    out,
                    err,
                }),
                Err(e) => done.push((*pid, format!("could not run {SAMPLE}: {e}\n"))),
            }
        }
        let late = Instant::now() >= deadline;
        let mut i = 0;
        while i < running.len() {
            let r = &mut running[i];
            let finished = match r.child.try_wait() {
                Ok(Some(_)) => true,
                Ok(None) if late => {
                    // Our own child: kill it by its handle and reap it.
                    let _ = r.child.kill();
                    let _ = r.child.wait();
                    true
                }
                Ok(None) => false,
                Err(_) => true,
            };
            if finished {
                let r = running.swap_remove(i);
                done.push((r.pid, report(&r, late)));
            } else {
                i += 1;
            }
        }
        if late {
            for (pid, _) in waiting.drain(..) {
                done.push((*pid, "not sampled: out of time\n".to_string()));
            }
        }
        if !running.is_empty() {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    for (pid, name) in procs.iter().take(MAX_PROCS) {
        let body = done
            .iter()
            .find(|(p, _)| p == pid)
            .map(|(_, b)| b.as_str())
            .unwrap_or("");
        text.push_str(&format!("--- stacks of pid {pid} ({name}) ---\n{body}"));
        if !body.ends_with('\n') {
            text.push('\n');
        }
    }
    if skipped > 0 {
        text.push_str(&format!(
            "--- {skipped} more processes not sampled (at most {MAX_PROCS}) ---\n"
        ));
    }
    Ok(text)
}

fn report(r: &Running, killed: bool) -> String {
    let out = std::fs::read_to_string(&r.out).unwrap_or_default();
    if !out.trim().is_empty() {
        let end = out.find("\nBinary Images:").unwrap_or(out.len());
        return out[..end].trim_end().to_string() + "\n";
    }
    let err = std::fs::read_to_string(&r.err).unwrap_or_default();
    let mut s = if killed {
        format!("{SAMPLE} did not finish in time\n")
    } else {
        format!("{SAMPLE} produced no report\n")
    };
    s.push_str(err.trim_end());
    s.push('\n');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory left by this process before an exec (same pid, SEQ from 0 again) isn't
    /// reused: its reports would read as this round's.
    #[test]
    fn a_scratch_directory_left_by_an_earlier_image_is_skipped() {
        use std::sync::atomic::Ordering;
        let left = std::env::temp_dir().join(format!(
            "potluck-stacks-{}-{}",
            std::process::id(),
            SEQ.load(Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&left).unwrap();
        std::fs::write(left.join("1.txt"), "another round's report").unwrap();
        let dir = scratch_dir().unwrap();
        let _ = std::fs::remove_dir_all(&left);
        let _ = std::fs::remove_dir_all(&dir);
        assert_ne!(dir, left);
    }

    /// Killing `sample` (as `run` does at the time limit) while it suspends its target every
    /// millisecond never leaves the target suspended: it keeps counting.
    #[test]
    fn a_killed_sample_leaves_its_target_running() {
        let dir = tempfile::tempdir().unwrap();
        let tick = dir.path().join("tick");
        let mut target = Command::new("/usr/bin/perl")
            .args([
                "-e",
                "my $n = 0; while (1) { open(F, '>', $ARGV[0]); print F $n++; close F; \
                 select(undef, undef, undef, 0.01) }",
            ])
            .arg(&tick)
            .spawn()
            .unwrap();
        // The file is truncated and rewritten every 10 ms: a read can find it empty.
        let read = || {
            (0..100).find_map(|_| {
                let v = std::fs::read_to_string(&tick)
                    .ok()?
                    .trim()
                    .parse::<u64>()
                    .ok();
                if v.is_none() {
                    std::thread::sleep(Duration::from_millis(2));
                }
                v
            })
        };
        // Whether the count moves on within 5 s (a loaded machine may be slow to run it).
        let counting = || {
            let before = read();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
                if read().is_some_and(|now| before.is_none_or(|b| now > b)) {
                    return true;
                }
            }
            false
        };
        let mut stuck = Vec::new();
        for (i, wait_ms) in [300u64, 450, 600, 750, 900].into_iter().enumerate() {
            let mut sampler = Command::new(SAMPLE)
                .arg(target.id().to_string())
                .args(["10", "-mayDie", "-file"])
                .arg(dir.path().join(format!("s{i}.txt")))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            std::thread::sleep(Duration::from_millis(wait_ms));
            // Our own child: kill it by its handle and reap it.
            sampler.kill().unwrap();
            sampler.wait().unwrap();
            if !counting() {
                let state = Command::new("/bin/ps")
                    .args(["-o", "stat=", "-p", &target.id().to_string()])
                    .output()
                    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                    .unwrap_or_default();
                stuck.push((wait_ms, state));
            }
        }
        // Our own child, not yet reaped.
        target.kill().unwrap();
        target.wait().unwrap();
        assert!(
            stuck.is_empty(),
            "not counting after a kill at (ms, ps state) {stuck:?}"
        );
    }
}
