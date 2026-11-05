//! `potluck _supervise` as its own process, for what needs a whole process: its signal handling.
//! Its stdout is a log file, as the executors arrange.

use potluck::types::ExitStatus;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const POTLUCK: &str = env!("CARGO_BIN_EXE_potluck");

fn supervisor(dir: &Path, script: &str) -> Command {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("out.log"))
        .unwrap();
    let mut cmd = Command::new(POTLUCK);
    cmd.arg("_supervise")
        .arg(dir.join("status.json"))
        .args(["--", "sh", "-c", script])
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    cmd
}

fn status(dir: &Path) -> ExitStatus {
    serde_json::from_slice(&std::fs::read(dir.join("status.json")).unwrap()).unwrap()
}

fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// SIGTERM to the supervisor alone (as `systemctl kill` or the macOS executor send it to every
/// process of the job) doesn't end it: the command runs on and its exit code is recorded.
#[test]
fn a_term_to_the_supervisor_alone_is_survived() {
    let dir = tempfile::tempdir().unwrap();
    let go = dir.path().join("go");
    let script = format!(
        "echo started; while [ ! -e '{}' ]; do sleep 0.02; done; exit 7",
        go.display()
    );
    let mut child = supervisor(dir.path(), &script).spawn().unwrap();
    let log = dir.path().join("out.log");
    wait_for("the command to start", || {
        std::fs::read_to_string(&log).is_ok_and(|l| l.contains("started"))
    });
    for sig in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
        // SAFETY: a signal to the supervisor this test started.
        unsafe { libc::kill(child.id() as i32, sig) };
    }
    std::thread::sleep(Duration::from_millis(200));
    assert!(child.try_wait().unwrap().is_none(), "the supervisor ended");
    std::fs::write(&go, "").unwrap();
    let exit = child.wait().unwrap();
    assert_eq!(exit.code(), Some(7));
    assert_eq!(status(dir.path()).code, Some(7));
}

/// A SIGTERM that reaches the supervisor before its command exists (the macOS executor starts it
/// with these signals blocked, so one sent right after the start waits) is passed on to the
/// command once it runs, and recorded as what ended it.
#[test]
fn a_term_before_the_command_starts_is_passed_on() {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = supervisor(dir.path(), "sleep 30; exit 0");
    // SAFETY: only async-signal-safe calls between fork and exec. SIGTERM is blocked, then sent
    // to this process: it stays pending across exec until the supervisor unblocks it.
    unsafe {
        cmd.pre_exec(|| {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&raw mut set);
            libc::sigaddset(&raw mut set, libc::SIGTERM);
            libc::sigprocmask(libc::SIG_BLOCK, &raw const set, std::ptr::null_mut());
            libc::raise(libc::SIGTERM);
            Ok(())
        });
    }
    let started = Instant::now();
    let exit = cmd.status().unwrap();
    assert!(started.elapsed() < Duration::from_secs(20));
    assert_eq!(exit.code(), Some(128 + libc::SIGTERM));
    let s = status(dir.path());
    assert_eq!((s.code, s.signal), (None, Some(libc::SIGTERM)));
}

/// `--log-max` keeps the first bytes and the last 16 MiB, whatever the command writes.
#[test]
fn log_max_is_applied() {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(POTLUCK);
    let log = std::fs::File::create(dir.path().join("out.log")).unwrap();
    // 20 MiB of 'x' then a last line.
    cmd.arg("_supervise")
        .args(["--log-max", "1048576"])
        .arg(dir.path().join("status.json"))
        .args([
            "--",
            "sh",
            "-c",
            "head -c 20971520 /dev/zero | tr '\\0' x; echo; echo the end",
        ])
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    assert_eq!(cmd.status().unwrap().code(), Some(0));
    let log = std::fs::read(dir.path().join("out.log")).unwrap();
    let tail = 16 << 20;
    let at_limit = "\n--- potluck: the output reached log_max_mib (1 MiB): what follows is dropped, \
                    except its last 16 MiB, added when the command ends ---\n";
    let total: u64 = 20 * (1 << 20) + 1 + 8;
    let end = format!(
        "--- potluck: {} bytes of output dropped after the log reached log_max_mib (1 MiB); the \
         last {tail} bytes follow ---\n",
        total - (1 << 20) - tail as u64
    );
    assert_eq!(log.len(), (1 << 20) + at_limit.len() + end.len() + tail);
    assert!(log[..1 << 20].iter().all(|&b| b == b'x'));
    let rest = &log[1 << 20..];
    assert!(rest.starts_with(at_limit.as_bytes()));
    let rest = &rest[at_limit.len()..];
    assert!(
        rest.starts_with(end.as_bytes()),
        "{}",
        String::from_utf8_lossy(&rest[..200])
    );
    assert!(rest.ends_with(b"xxx\nthe end\n"));
}
