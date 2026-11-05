//! The macOS executor against real processes: `potluck _supervise` from this build, perl and sh as
//! jobs. Every test kills what it started (by the invocation's verified (pid, start) pairs) and
//! checks that nothing is left, zombies included.
#![cfg(target_os = "macos")]

use potluck::exec::macos::{ProcessExecutor, exists, start_time};
use potluck::exec::{Executor, Launch, Limits, Sample};
use potluck::types::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MIB: u64 = 1024 * 1024;

fn job(cmd: &[&str]) -> JobRecord {
    JobRecord {
        id: "test-1".into(),
        spec: JobSpec {
            key: None,
            labels: BTreeMap::new(),
            priority: DEFAULT_PRIORITY,
            request: Request::default(),
            on: BTreeMap::new(),
            repo: RepoRef {
                id: "repo-00000000".into(),
                source: PathBuf::from("/nonexistent"),
                commit: "0".repeat(40),
                branch: None,
                submodules: vec![],
                carried: vec![],
            },
            workspace: None,
            advance: false,
            cmd: cmd.iter().map(|s| s.to_string()).collect(),
            env: BTreeMap::new(),
            unset_env: Default::default(),
            timeout_ms: 60_000,
            hold_ms: None,
            keep: vec![],
            rerun_if_lost: false,
            kill_on_stall: false,
            recipe: None,
            submitted_from: "test".into(),
            pools: Vec::new(),
            auth: None,
        },
        state: JobState::Running,
        submitted_at: 0,
        started_at: None,
        ended_at: None,
        node: None,
        workspace: None,
        lease: None,
        handle: None,
        exit_code: None,
        signal: None,
        message: None,
        stuck_pids: vec![],
        usage: Usage::default(),
        passes: 0,
        stalled: false,
        kept: vec![],
        notes: Vec::new(),
    }
}

fn base_env() -> BTreeMap<String, String> {
    [("PATH", "/usr/bin:/bin"), ("LANG", "C")]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// A started invocation. Dropping it kills whatever is left and checks nothing remains.
struct Inv {
    dir: tempfile::TempDir,
    handle: Handle,
}

impl Inv {
    fn start(cmd: &[&str], env: BTreeMap<String, String>, mem_max: u64) -> Inv {
        Self::start_with(&ProcessExecutor::default(), cmd, env, mem_max)
    }

    fn start_with(
        exec: &ProcessExecutor,
        cmd: &[&str],
        env: BTreeMap<String, String>,
        mem_max: u64,
    ) -> Inv {
        Self::start_full(exec, cmd, env, mem_max, 1000)
    }

    fn start_full(
        exec: &ProcessExecutor,
        cmd: &[&str],
        env: BTreeMap<String, String>,
        mem_max: u64,
        tasks_max: u32,
    ) -> Inv {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("work")).unwrap();
        let job = job(cmd);
        let grant = Grant::default();
        let launch = Launch {
            job: &job,
            grant: &grant,
            cwd: dir.path().join("work"),
            env,
            log: dir.path().join("out.log"),
            status: dir.path().join("status.json"),
            potluck_bin: PathBuf::from(env!("CARGO_BIN_EXE_potluck")),
            name: "potluck-test-1".into(),
            limits: Limits {
                mem_max_bytes: mem_max,
                cpu_weight: 100,
                tasks_max,
                kill_grace_ms: 10_000,
                log_max_bytes: 0,
            },
        };
        let handle = exec.start(&launch).unwrap();
        Inv { dir, handle }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn work(&self) -> PathBuf {
        self.path("work")
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.path("out.log")).unwrap_or_default()
    }

    fn status(&self, timeout: Duration) -> Option<ExitStatus> {
        let path = self.path("status.json");
        wait_for(timeout, || std::fs::read(&path).ok()).map(|b| serde_json::from_slice(&b).unwrap())
    }

    fn sample(&mut self, exec: &ProcessExecutor) -> Sample {
        exec.sample(&mut self.handle, 0).unwrap()
    }

    fn tracked(&self) -> Vec<(u32, u64)> {
        match &self.handle {
            Handle::Process { tracked, .. } => tracked.clone(),
            _ => unreachable!(),
        }
    }

    fn leader(&self) -> (u32, u64) {
        match &self.handle {
            Handle::Process {
                pid, start_time, ..
            } => (*pid, *start_time),
            _ => unreachable!(),
        }
    }

    /// Waits until no tracked process exists in any state (launchd reaps the orphans).
    fn assert_all_gone(&self) {
        let left = wait_for(Duration::from_secs(10), || {
            let left: Vec<_> = self
                .tracked()
                .into_iter()
                .filter(|&(p, s)| exists(p, s))
                .collect();
            left.is_empty().then_some(())
        });
        assert!(
            left.is_some(),
            "processes left: {:?}",
            self.tracked()
                .into_iter()
                .filter(|&(p, s)| exists(p, s))
                .collect::<Vec<_>>()
        );
    }
}

impl Drop for Inv {
    fn drop(&mut self) {
        let exec = ProcessExecutor::default();
        let _ = exec.sample(&mut self.handle, 0);
        let _ = exec.signal(&self.handle, libc::SIGKILL);
        if !std::thread::panicking() {
            self.assert_all_gone();
        }
    }
}

fn wait_for<T>(timeout: Duration, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn read_pid(path: &Path) -> Option<u32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[test]
fn exit_code_comes_from_the_status_file() {
    let inv = Inv::start(&["/bin/sh", "-c", "exit 7"], base_env(), 0);
    let st = inv.status(Duration::from_secs(10)).expect("status file");
    assert_eq!((st.code, st.signal), (Some(7), None));
}

#[test]
fn environment_is_exactly_the_given_one() {
    let mut env = base_env();
    env.insert("POTLUCK_JOB".into(), "test-1".into());
    env.insert("WITH SPACE".into(), "a b  c".into());
    env.insert("EMPTY".into(), String::new());
    env.insert("UNICODE".into(), "é→✓".into());
    let inv = Inv::start(&["/usr/bin/env"], env.clone(), 0);
    let st = inv.status(Duration::from_secs(10)).expect("status file");
    assert_eq!(st.code, Some(0));
    let mut got: Vec<String> = inv.log().lines().map(str::to_string).collect();
    got.sort();
    let mut want: Vec<String> = env.iter().map(|(k, v)| format!("{k}={v}")).collect();
    want.sort();
    assert_eq!(got, want);
}

/// Environment values reach the command through execve's environment, never an argument list:
/// `ps` shows every user's arguments.
#[test]
fn env_values_are_on_no_command_line() {
    let secret = format!("potluck-test-secret-{}", std::process::id());
    let mut env = base_env();
    env.insert("SECRET_TOKEN".into(), secret.clone());
    let script = r#"printf %s "$SECRET_TOKEN" > token; exec /bin/sleep 30"#;
    let mut inv = Inv::start(&["/bin/sh", "-c", script], env, 0);
    let exec = ProcessExecutor::default();
    // The command has it.
    let token = wait_for(Duration::from_secs(10), || {
        inv.sample(&exec);
        std::fs::read_to_string(inv.work().join("token"))
            .ok()
            .filter(|t| !t.is_empty())
    });
    assert_eq!(token.as_deref(), Some(secret.as_str()));
    let out = std::process::Command::new("/bin/ps")
        .args(["-axww", "-o", "args="])
        .output()
        .unwrap();
    let everyone = String::from_utf8_lossy(&out.stdout);
    assert!(everyone.contains("_supervise"), "{everyone}");
    assert!(!everyone.contains(&secret), "{everyone}");
}

#[test]
fn runs_in_cwd_and_appends_to_the_log() {
    let exec = ProcessExecutor::default();
    let inv = Inv::start_with(
        &exec,
        &["/bin/sh", "-c", "/bin/pwd; echo err >&2"],
        base_env(),
        0,
    );
    inv.status(Duration::from_secs(10)).expect("status file");
    let cwd = inv.work().canonicalize().unwrap();
    let first = format!("{}\nerr\n", cwd.display());
    assert_eq!(inv.log(), first);
    // A second invocation with the same log appends to it.
    let job = job(&["/bin/sh", "-c", "echo second"]);
    let grant = Grant::default();
    let launch = Launch {
        job: &job,
        grant: &grant,
        cwd: inv.work(),
        env: base_env(),
        log: inv.path("out.log"),
        status: inv.path("status2.json"),
        potluck_bin: PathBuf::from(env!("CARGO_BIN_EXE_potluck")),
        name: "potluck-test-2".into(),
        limits: Limits {
            mem_max_bytes: 0,
            cpu_weight: 100,
            tasks_max: 100,
            kill_grace_ms: 10_000,
            log_max_bytes: 0,
        },
    };
    let second = exec.start(&launch).unwrap();
    let Handle::Process {
        pid, start_time, ..
    } = second
    else {
        unreachable!()
    };
    assert!(
        wait_for(Duration::from_secs(10), || inv
            .path("status2.json")
            .exists()
            .then_some(()))
        .is_some()
    );
    assert_eq!(inv.log(), format!("{first}second\n"));
    assert!(
        wait_for(Duration::from_secs(10), || (!exists(pid, start_time))
            .then_some(()))
        .is_some()
    );
}

/// The command writes to a pipe that the supervisor copies into the log, and the log limit
/// (`Limits::log_max_bytes`) reaches the supervisor.
#[test]
fn output_goes_through_the_supervisor_and_the_limit_applies() {
    let exec = ProcessExecutor::default();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("work")).unwrap();
    let script = "[ -p /dev/stdout ] && echo a pipe; echo two > /dev/stdout; \
                  i=1000; while [ $i -lt 1500 ]; do echo line $i; i=$((i+1)); done";
    let job = job(&["/bin/sh", "-c", script]);
    let grant = Grant::default();
    let launch = Launch {
        job: &job,
        grant: &grant,
        cwd: dir.path().join("work"),
        env: base_env(),
        log: dir.path().join("out.log"),
        status: dir.path().join("status.json"),
        potluck_bin: PathBuf::from(env!("CARGO_BIN_EXE_potluck")),
        name: "potluck-test-1".into(),
        limits: Limits {
            mem_max_bytes: 0,
            cpu_weight: 100,
            tasks_max: 100,
            kill_grace_ms: 10_000,
            log_max_bytes: 1000,
        },
    };
    let inv = Inv {
        handle: exec.start(&launch).unwrap(),
        dir,
    };
    assert_eq!(inv.status(Duration::from_secs(10)).unwrap().code, Some(0));
    let log = inv.log();
    assert!(log.starts_with("a pipe\ntwo\nline 1000\n"), "{log}");
    assert!(
        log.contains(
            "--- potluck: the output reached log_max_mib (1000 bytes): what follows is dropped"
        ),
        "{log}"
    );
    assert!(
        log.contains("--- potluck: 0 bytes of output dropped after the log reached log_max_mib")
            && log.ends_with("line 1499\n"),
        "{log}"
    );
}

/// A supervisor stopped from outside (SIGSTOP) is continued by the next sample, so it reads the
/// job's output again and records the exit code of a command that ended meanwhile.
#[test]
fn a_stopped_supervisor_is_continued() {
    let mut inv = Inv::start(&["/bin/sh", "-c", "sleep 1; exit 5"], base_env(), 0);
    let exec = ProcessExecutor::default();
    let (pid, _) = inv.leader();
    // SAFETY: a signal to the supervisor this test started.
    unsafe { libc::kill(pid as i32, libc::SIGSTOP) };
    std::thread::sleep(Duration::from_secs(2));
    assert!(inv.status(Duration::from_millis(100)).is_none());
    inv.sample(&exec);
    let status = inv.status(Duration::from_secs(10)).expect("a status file");
    assert_eq!(status.code, Some(5));
}

#[test]
fn group_and_session() {
    let mut inv = Inv::start_with(
        &ProcessExecutor::default(),
        &["/bin/sh", "-c", "ps -o pgid= -p $$; echo; sleep 30"],
        base_env(),
        0,
    );
    let leader = inv.leader();
    let exec = ProcessExecutor::default();
    let log = wait_for(Duration::from_secs(10), || {
        let l = inv.log();
        (!l.is_empty()).then_some(l)
    })
    .unwrap();
    let fields: Vec<&str> = log.split_whitespace().collect();
    // The command leads a process group of its own (so `kill 0` in a job can't reach the
    // supervisor), in the supervisor's session.
    assert_ne!(fields[0], leader.0.to_string(), "{log}");
    // SAFETY: getsid of a pid this test started.
    let sid = unsafe { libc::getsid(fields[0].parse().unwrap()) };
    assert_eq!(sid, leader.0 as i32, "{log}");
    let s = inv.sample(&exec);
    assert!(s.pids.contains(&leader.0) && s.pids.len() >= 2, "{s:?}");
}

#[test]
fn over_memory_is_killed_and_reported() {
    let mut inv = Inv::start(
        &[
            "/usr/bin/perl",
            "-e",
            r#"$x = "a" x (300*1024*1024); print "touched\n"; sleep 30"#,
        ],
        base_env(),
        0,
    );
    let exec = ProcessExecutor::default();
    let limit = 128 * MIB;
    let over = wait_for(Duration::from_secs(20), || {
        let s = exec.sample(&mut inv.handle, limit).unwrap();
        s.over_memory.then_some(s)
    })
    .expect("over memory");
    assert!(over.mem_bytes > limit, "{over:?}");
    // Everything is gone and the supervisor wrote no status: it was killed too.
    inv.assert_all_gone();
    let later = exec.sample(&mut inv.handle, limit).unwrap();
    assert!(later.over_memory && later.pids.is_empty(), "{later:?}");
    assert!(inv.status(Duration::from_millis(200)).is_none());
}

#[test]
fn under_the_limit_is_left_alone() {
    let mut inv = Inv::start(
        &[
            "/usr/bin/perl",
            "-e",
            r#"$x = "a" x (64*1024*1024); print "ok\n"; sleep 2; exit 3"#,
        ],
        base_env(),
        0,
    );
    let exec = ProcessExecutor::default();
    let mut peak = 0;
    let st = wait_for(Duration::from_secs(10), || {
        let s = exec.sample(&mut inv.handle, 512 * MIB).unwrap();
        assert!(!s.over_memory);
        peak = peak.max(s.mem_bytes);
        inv.status(Duration::ZERO)
    })
    .unwrap();
    assert_eq!(st.code, Some(3));
    assert!(peak > 64 * MIB, "peak {peak}");
}

/// The command forks a child that calls setsid and forks again; that child exits, so the
/// grandchild has left the session and has launchd as its parent. It is still tracked, it is
/// killed by signal(SIGKILL), and so is the rest once the command itself has ended.
#[test]
fn grandchild_that_leaves_the_session_is_followed() {
    let script = r#"
        use POSIX;
        if (fork == 0) {
            POSIX::setsid();
            if (fork == 0) { open(F, ">gc.pid"); print F $$; close F; sleep 60; exit 0 }
            sleep 2; exit 0;
        }
        wait; print "parent done\n"; exit 0;
    "#;
    let mut inv = Inv::start(&["/usr/bin/perl", "-e", script], base_env(), 0);
    let exec = ProcessExecutor::default();
    let gc = wait_for(Duration::from_secs(10), || {
        inv.sample(&exec);
        let pid = read_pid(&inv.work().join("gc.pid"))?;
        inv.tracked().iter().any(|t| t.0 == pid).then_some(pid)
    })
    .expect("grandchild tracked");
    let gc_start = start_time(gc).unwrap();
    // The command ends (its child exited after 2 s); the grandchild is left behind.
    let st = inv.status(Duration::from_secs(10)).expect("status file");
    assert_eq!(st.code, Some(0));
    let s = inv.sample(&exec);
    assert!(s.pids.contains(&gc), "{s:?}");
    // SAFETY: getsid/getppid of a pid we verified above.
    assert_ne!(unsafe { libc::getsid(gc as i32) }, inv.leader().0 as i32);
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    assert!(
        wait_for(Duration::from_secs(5), || (!exists(gc, gc_start))
            .then_some(()))
        .is_some(),
        "grandchild survived"
    );
    inv.assert_all_gone();
}

/// The run-limit sequence: SIGTERM to the whole job, SIGKILL after a grace period. The job
/// ignores SIGTERM in a process group of its own, so only the SIGKILL ends it.
#[test]
fn run_limit_term_then_kill() {
    let script = r#"
        $| = 1;
        if (fork == 0) {
            setpgrp(0, 0); $SIG{TERM} = "IGNORE";
            open(F, ">child.pid"); print F $$; close F;
            sleep 60; exit 0;
        }
        $SIG{TERM} = sub { print "got TERM\n" };
        sleep 1 while 1;
    "#;
    let mut inv = Inv::start(&["/usr/bin/perl", "-e", script], base_env(), 0);
    let exec = ProcessExecutor::default();
    let child = wait_for(Duration::from_secs(10), || {
        inv.sample(&exec);
        read_pid(&inv.work().join("child.pid"))
    })
    .unwrap();
    // The child is in its own group now; make sure a sample saw it there.
    wait_for(Duration::from_secs(5), || {
        inv.sample(&exec);
        // SAFETY: getpgid of a pid we started.
        (unsafe { libc::getpgid(child as i32) } == child as i32).then_some(())
    })
    .unwrap();
    let before = inv.sample(&exec);
    assert!(before.pids.len() >= 3, "{before:?}");
    exec.signal(&inv.handle, libc::SIGTERM).unwrap();
    assert!(
        wait_for(Duration::from_secs(5), || inv
            .log()
            .contains("got TERM")
            .then_some(()))
        .is_some()
    );
    std::thread::sleep(Duration::from_millis(300));
    let during = inv.sample(&exec);
    assert_eq!(during.pids.len(), before.pids.len(), "{during:?}");
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    inv.assert_all_gone();
    assert!(inv.sample(&exec).pids.is_empty());
    // The supervisor was killed too, so there is no status file.
    assert!(inv.status(Duration::from_millis(200)).is_none());
}

/// SIGTERM to a command that dies of it: the supervisor survives it and records the signal.
#[test]
fn term_ends_a_plain_command_with_its_signal() {
    let mut inv = Inv::start(&["/bin/sleep", "60"], base_env(), 0);
    let exec = ProcessExecutor::default();
    wait_for(Duration::from_secs(5), || {
        (inv.sample(&exec).pids.len() >= 2).then_some(())
    })
    .unwrap();
    exec.signal(&inv.handle, libc::SIGTERM).unwrap();
    let st = inv.status(Duration::from_secs(10)).expect("status file");
    assert_eq!((st.code, st.signal), (None, Some(libc::SIGTERM)));
}

/// A tracked pid whose start time doesn't match is another process: never signalled. A leader
/// pid that now names another process doesn't lend its session either.
#[test]
fn reused_pids_are_not_signalled() {
    let mut other = std::process::Command::new("/usr/bin/perl")
        .args(["-e", "use POSIX; POSIX::setsid(); sleep 60"])
        .spawn()
        .unwrap();
    let pid = other.id();
    let start = wait_for(Duration::from_secs(5), || start_time(pid)).unwrap();
    // Let it reach setsid.
    wait_for(Duration::from_secs(5), || {
        // SAFETY: getsid of our own child.
        (unsafe { libc::getsid(pid as i32) } == pid as i32).then_some(())
    })
    .unwrap();
    let exec = ProcessExecutor::default();
    let stale = Handle::Process {
        pid,
        start_time: start + 1,
        tracked: vec![(pid, start + 1), (pid, start - 1)],
        gone: Vec::new(),
        lease: String::new(),
        tasks_max: 0,
        boot: String::new(),
    };
    exec.signal(&stale, libc::SIGKILL).unwrap();
    exec.signal(&stale, libc::SIGTERM).unwrap();
    let mut h = stale.clone();
    let s = exec.sample(&mut h, 1).unwrap();
    assert!(s.pids.is_empty() && !s.over_memory, "{s:?}");
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        other.try_wait().unwrap().is_none(),
        "the other process was signalled"
    );
    assert!(exists(pid, start));
    // With the right start time it is ours to kill.
    let ours = Handle::Process {
        pid,
        start_time: start,
        tracked: vec![(pid, start)],
        gone: Vec::new(),
        lease: String::new(),
        tasks_max: 0,
        boot: String::new(),
    };
    exec.signal(&ours, libc::SIGKILL).unwrap();
    let st = other.wait().unwrap();
    assert_eq!(std::os::unix::process::ExitStatusExt::signal(&st), Some(9));
}

/// A daemon restart: a fresh executor finds, samples, signals and finishes the invocation from
/// the handle alone.
#[test]
fn adoption_by_a_fresh_executor() {
    let script = r#"if (fork == 0) { sleep 60; exit 0 } sleep 60"#;
    let mut inv = {
        let first = ProcessExecutor::default();
        let mut inv = Inv::start_with(&first, &["/usr/bin/perl", "-e", script], base_env(), 0);
        wait_for(Duration::from_secs(5), || {
            (inv.sample(&first).pids.len() >= 3).then_some(())
        })
        .unwrap();
        inv
    };
    // Stored and loaded the way the daemon keeps it.
    let stored = serde_json::to_string(&inv.handle).unwrap();
    inv.handle = serde_json::from_str(&stored).unwrap();
    let fresh = ProcessExecutor::with_reports_dir(inv.path("no-reports"));
    let s = inv.sample(&fresh);
    assert_eq!(s.pids.len(), 3, "{s:?}");
    assert!(s.pids.contains(&inv.leader().0));
    assert!(s.mem_bytes > 0);
    fresh.signal(&inv.handle, libc::SIGTERM).unwrap();
    let st = inv.status(Duration::from_secs(10)).expect("status file");
    assert_eq!(st.signal, Some(libc::SIGTERM));
    // The forked child got SIGTERM too, from the group.
    inv.assert_all_gone();
    fresh.finish(&inv.handle, &inv.path("crash"), 0).unwrap();
}

/// cpu_ms grows under a busy loop and does not drop when the busy processes exit: one is reaped by
/// the command (its time moves into the command's), one is an orphan reaped by launchd. Each loop
/// runs for one second of its own CPU time, however busy the machine is.
#[test]
fn cpu_time_accumulates() {
    let script = r#"
        sub busy { my $x = 0; while (1) { my @t = times; last if $t[0] + $t[1] >= $_[0]; $x++ } }
        if (fork == 0) { busy(1.0); exit 0 }
        wait;
        my $pid = fork;
        if ($pid == 0) {
            if (fork == 0) { busy(1.0); open(F, ">done"); close F; exit 0 }
            exit 0;
        }
        waitpid($pid, 0);
        sleep 60;
    "#;
    let mut inv = Inv::start(&["/usr/bin/perl", "-e", script], base_env(), 0);
    let exec = ProcessExecutor::default();
    let mut values = Vec::new();
    let done = wait_for(Duration::from_secs(60), || {
        values.push(inv.sample(&exec).cpu_ms);
        std::thread::sleep(Duration::from_millis(80));
        inv.work().join("done").exists().then_some(())
    });
    assert!(done.is_some(), "{values:?}");
    // Let the orphan exit and launchd reap it.
    std::thread::sleep(Duration::from_millis(300));
    let last = inv.sample(&exec).cpu_ms;
    values.push(last);
    println!("cpu_ms over time: {values:?}");
    assert!(values.windows(2).all(|w| w[0] <= w[1]), "{values:?}");
    // Either loop alone is about 1000 ms: more means the first was kept when it exited. The
    // orphan's last ~100 ms may fall between two samples.
    assert!((1700..4000).contains(&last), "{last} ms: {values:?}");
}

#[test]
fn stacks_show_where_processes_are() {
    let mut inv = Inv::start(
        &[
            "/usr/bin/perl",
            "-e",
            "if (fork == 0) { sleep 60 } sleep 60",
        ],
        base_env(),
        0,
    );
    let exec = ProcessExecutor::default();
    wait_for(Duration::from_secs(5), || {
        (inv.sample(&exec).pids.len() >= 3).then_some(())
    })
    .unwrap();
    let t = Instant::now();
    let text = exec.stacks(&inv.handle).unwrap();
    let took = t.elapsed();
    println!("stacks took {took:?}:\n{text}");
    assert_eq!(text.matches("--- stacks of pid ").count(), 2, "{text}");
    assert!(text.contains("(perl) ---"), "{text}");
    assert!(text.contains("Call graph:"), "{text}");
    assert!(!text.contains("Binary Images:"), "{text}");
    assert!(took < Duration::from_secs(20), "{took:?}");
}

/// finish moves the invocation's crash reports (fake .ips files in a temp dir) and nothing else.
#[test]
fn finish_moves_crash_reports() {
    let mut inv = Inv::start(&["/bin/sleep", "60"], base_env(), 0);
    let reports = inv.path("DiagnosticReports");
    std::fs::create_dir(&reports).unwrap();
    let exec = ProcessExecutor::with_reports_dir(&reports);
    wait_for(Duration::from_secs(5), || {
        (inv.sample(&exec).pids.len() >= 2).then_some(())
    })
    .unwrap();
    let (sleep_pid, sleep_start) = inv
        .tracked()
        .into_iter()
        .find(|t| t.0 != inv.leader().0)
        .unwrap();
    let fmt = |us: u64| {
        // Local time with its offset, as ReportCrash writes it, via date(1).
        let out = std::process::Command::new("/bin/date")
            .args(["-r", &(us / 1_000_000).to_string(), "+%Y-%m-%d %H:%M:%S"])
            .output()
            .unwrap();
        let zone = std::process::Command::new("/bin/date")
            .args(["-r", &(us / 1_000_000).to_string(), "+%z"])
            .output()
            .unwrap();
        format!(
            "{}.{:04} {}",
            String::from_utf8(out.stdout).unwrap().trim(),
            (us % 1_000_000) / 100,
            String::from_utf8(zone.stdout).unwrap().trim()
        )
    };
    let now = now_ms() * 1000;
    let write = |name: &str, pid: u32, launched: u64| {
        let header =
            serde_json::json!({"app_name": "sleep", "timestamp": fmt(now), "bug_type": "309"});
        let body = serde_json::json!({
            "pid": pid, "parentPid": 1, "procLaunch": fmt(launched), "captureTime": fmt(now),
        });
        std::fs::write(reports.join(name), format!("{header}\n{body}")).unwrap();
    };
    write("sleep-ours.ips", sleep_pid, sleep_start);
    write("sleep-other.ips", sleep_pid + 1, sleep_start);
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    inv.assert_all_gone();
    let crash = inv.path("crash");
    exec.finish(&inv.handle, &crash, 0).unwrap();
    assert!(crash.join("sleep-ours.ips").exists());
    assert!(!reports.join("sleep-ours.ips").exists());
    assert!(reports.join("sleep-other.ips").exists());
    assert!(!crash.join("sleep-other.ips").exists());
}

/// The second collection of crash reports, seconds after the job ended, can come after the job
/// was deleted (by retention) with its directory: a report of the job's written by then stays
/// where it is, and the directory is not made again. With the directory there, it is moved.
#[test]
fn crash_reports_of_a_deleted_job_stay() {
    let mut inv = Inv::start(&["/bin/sleep", "60"], base_env(), 0);
    let reports = inv.path("DiagnosticReports");
    std::fs::create_dir(&reports).unwrap();
    let exec = ProcessExecutor::with_reports_dir(&reports);
    wait_for(Duration::from_secs(5), || {
        (inv.sample(&exec).pids.len() >= 2).then_some(())
    })
    .unwrap();
    let (pid, start) = inv
        .tracked()
        .into_iter()
        .find(|t| t.0 != inv.leader().0)
        .unwrap();
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    inv.assert_all_gone();
    // The job's directory, as the daemon makes it when the job starts.
    let job_dir = inv.path("job");
    let crash = job_dir.join("crash");
    std::fs::create_dir_all(&crash).unwrap();
    exec.finish(&inv.handle, &crash, 0).unwrap();
    std::fs::remove_dir_all(&job_dir).unwrap();
    let now = now_ms() * 1000;
    let header = serde_json::json!({"app_name": "sleep", "timestamp": report_time(now)});
    let body = serde_json::json!({
        "pid": pid, "parentPid": 1, "procLaunch": report_time(start),
        "captureTime": report_time(now),
    });
    std::fs::write(reports.join("late.ips"), format!("{header}\n{body}")).unwrap();
    exec.finish(&inv.handle, &crash, 0).unwrap();
    assert!(!job_dir.exists(), "{} was made again", job_dir.display());
    assert!(reports.join("late.ips").exists());
    // It is the job's report: crash/ is made again in a job directory that is there.
    std::fs::create_dir(&job_dir).unwrap();
    exec.finish(&inv.handle, &crash, 0).unwrap();
    assert!(crash.join("late.ips").exists());
}

#[test]
fn start_errors_are_reported() {
    let exec = ProcessExecutor::default();
    let dir = tempfile::tempdir().unwrap();
    let job = job(&["/bin/true"]);
    let grant = Grant::default();
    let mut launch = Launch {
        job: &job,
        grant: &grant,
        cwd: dir.path().join("missing"),
        env: base_env(),
        log: dir.path().join("out.log"),
        status: dir.path().join("status.json"),
        potluck_bin: PathBuf::from(env!("CARGO_BIN_EXE_potluck")),
        name: "potluck-test-3".into(),
        limits: Limits {
            mem_max_bytes: 0,
            cpu_weight: 100,
            tasks_max: 100,
            kill_grace_ms: 10_000,
            log_max_bytes: 0,
        },
    };
    let e = exec.start(&launch).unwrap_err().to_string();
    assert!(
        e.contains("changing directory") && e.contains("No such file"),
        "{e}"
    );
    launch.cwd = dir.path().to_path_buf();
    launch.potluck_bin = dir.path().join("no-such-potluck");
    let e = exec.start(&launch).unwrap_err().to_string();
    assert!(e.contains("executing") && e.contains("No such file"), "{e}");
    launch.potluck_bin = PathBuf::from(env!("CARGO_BIN_EXE_potluck"));
    launch.env.insert("BAD=NAME".into(), "x".into());
    assert!(exec.start(&launch).is_err());
}

/// Descriptors the daemon holds open without close-on-exec (like a lock file opened carelessly)
/// are not inherited by the job.
#[test]
fn job_inherits_no_descriptors() {
    let dir = tempfile::tempdir().unwrap();
    let f = std::fs::File::create(dir.path().join("lock")).unwrap();
    let fd = std::os::fd::AsRawFd::as_raw_fd(&f);
    // SAFETY: clears FD_CLOEXEC on a descriptor we own.
    unsafe { libc::fcntl(fd, libc::F_SETFD, 0) };
    let inv = Inv::start(
        &[
            "/usr/bin/perl",
            "-e",
            "opendir(D, '/dev/fd'); print join(' ', sort grep { /^\\d+$/ } readdir(D)), \"\\n\"",
        ],
        base_env(),
        0,
    );
    inv.status(Duration::from_secs(10)).expect("status file");
    // 0, 1, 2 and the directory handle perl itself opened to list them.
    let fds: Vec<u32> = inv
        .log()
        .split_whitespace()
        .map(|s| s.parse().unwrap())
        .collect();
    assert!(
        fds.starts_with(&[0, 1, 2]) && fds.len() <= 4,
        "{fds:?} (the test holds {fd})"
    );
    drop(f);
}

/// Held by tests that start many processes or need room under a small process limit: the
/// kernel counts processes per user, so one test's 300 children would leave another's job, held
/// to a few dozen above the count at its start, no room to fork.
static MANY_PROCESSES: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn many_processes() -> std::sync::MutexGuard<'static, ()> {
    MANY_PROCESSES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Sampling cost on this machine, with 300 extra processes in the invocation.
#[test]
fn sampling_cost() {
    let _many = many_processes();
    let script = r#"
        for (1..300) { if (fork == 0) { sleep 60; exit 0 } }
        open(F, ">ready"); close F; sleep 60
    "#;
    let mut inv = Inv::start(&["/usr/bin/perl", "-e", script], base_env(), 0);
    let exec = ProcessExecutor::default();
    wait_for(Duration::from_secs(20), || {
        (inv.work().join("ready").exists() && inv.sample(&exec).pids.len() >= 302).then_some(())
    })
    .expect("300 children");
    // SAFETY: a null buffer asks for the count only.
    let total = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    let n = 20;
    let t = Instant::now();
    for _ in 0..n {
        let s = inv.sample(&exec);
        assert_eq!(s.pids.len(), 302);
    }
    let per = t.elapsed() / n;
    println!("sample() with {total} processes on the machine, 302 in the invocation: {per:?}");
    let mut idle = Inv::start(&["/bin/sleep", "60"], base_env(), 0);
    let t = Instant::now();
    for _ in 0..n {
        idle.sample(&exec);
    }
    println!(
        "sample() of a 2-process invocation alongside: {:?}",
        t.elapsed() / n
    );
    assert!(per < Duration::from_millis(250), "{per:?}");
    let t = Instant::now();
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    println!("signal(SIGKILL) of 302 processes: {:?}", t.elapsed());
    inv.assert_all_gone();
}

/// This machine's boot time, in microseconds since the Unix epoch.
fn boot_time_us() -> u64 {
    let mut tv = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    let mut len = std::mem::size_of::<libc::timeval>();
    // SAFETY: kern.boottime is a struct timeval; len says how much room there is.
    let r = unsafe {
        libc::sysctlbyname(
            c"kern.boottime".as_ptr(),
            (&raw mut tv).cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    assert_eq!(r, 0);
    tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64
}

/// A session whose leader is exiting: `perl` calls setsid, forks a child that sleeps, and exits
/// once this has read its start time (a zombie has none), or after 30 s. Returns the perl process
/// (not reaped), its start time, and the child's pid.
fn leaderless_session(dir: &Path) -> (std::process::Child, u64, u32) {
    let script = r#"use POSIX; POSIX::setsid();
        if (fork == 0) { open(F, ">child.pid"); print F $$; close F; sleep 60; exit 0 }
        until (-e "leader.go" || time - $^T > 30) { select(undef, undef, undef, 0.02) } exit 0"#;
    let leader = std::process::Command::new("/usr/bin/perl")
        .args(["-e", script])
        .current_dir(dir)
        .spawn()
        .unwrap();
    let start = wait_for(Duration::from_secs(5), || start_time(leader.id())).unwrap();
    let child = wait_for(Duration::from_secs(5), || read_pid(&dir.join("child.pid"))).unwrap();
    // SAFETY: getsid of a process we started.
    assert_eq!(unsafe { libc::getsid(child as i32) }, leader.id() as i32);
    std::fs::write(dir.join("leader.go"), "").unwrap();
    (leader, start, child)
}

/// Kills the child of [`leaderless_session`] after checking it is still the same process.
fn kill_child(child: u32, child_start: u64) {
    if start_time(child) == Some(child_start) {
        // SAFETY: a pid we started, verified by its start time just above.
        unsafe { libc::kill(child as i32, libc::SIGKILL) };
    }
    assert!(
        wait_for(Duration::from_secs(5), || (!exists(child, child_start))
            .then_some(()))
        .is_some()
    );
}

/// A leader pid that names another process, even one the snapshot can't describe (a zombie,
/// another user's process), lends its session to nothing. The leader itself as a zombie still
/// does: its session's leftovers are the invocation's.
#[test]
fn a_leader_pid_held_by_another_process_lends_no_session() {
    let dir = tempfile::tempdir().unwrap();
    let (mut leader, start, child) = leaderless_session(dir.path());
    let pid = leader.id();
    let child_start = start_time(child).unwrap();
    // The leader exits and stays a zombie: we don't reap it yet.
    wait_for(Duration::from_secs(10), || {
        (start_time(pid).is_none() && exists(pid, start)).then_some(())
    })
    .expect("a zombie leader");
    let exec = ProcessExecutor::default();
    let mut stale = Handle::Process {
        pid,
        start_time: start + 1,
        tracked: vec![],
        gone: Vec::new(),
        lease: String::new(),
        tasks_max: 0,
        boot: String::new(),
    };
    let s = exec.sample(&mut stale, 0).unwrap();
    let mut ours = Handle::Process {
        pid,
        start_time: start,
        tracked: vec![],
        gone: Vec::new(),
        lease: String::new(),
        tasks_max: 0,
        boot: String::new(),
    };
    let o = exec.sample(&mut ours, 0).unwrap();
    kill_child(child, child_start);
    leader.wait().unwrap();
    assert!(
        s.pids.is_empty(),
        "another process's session was taken: {s:?}"
    );
    assert_eq!(o.pids, vec![child], "{o:?}");
}

/// A handle from before this boot (a daemon restarted after a reboot) matches nothing, not even
/// a session whose id is its leader's pid.
#[test]
fn a_handle_from_before_this_boot_matches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (mut leader, start, child) = leaderless_session(dir.path());
    let pid = leader.id();
    let child_start = start_time(child).unwrap();
    leader.wait().unwrap();
    let exec = ProcessExecutor::default();
    let mut stale = Handle::Process {
        pid,
        start_time: boot_time_us() - 3_600_000_000,
        tracked: vec![],
        gone: Vec::new(),
        lease: String::new(),
        tasks_max: 0,
        boot: String::new(),
    };
    let s = exec.sample(&mut stale, 0).unwrap();
    // The same boot: the leader's session outlives it, and its processes are the invocation's.
    let mut ours = Handle::Process {
        pid,
        start_time: start,
        tracked: vec![],
        gone: Vec::new(),
        lease: String::new(),
        tasks_max: 0,
        boot: String::new(),
    };
    let o = exec.sample(&mut ours, 0).unwrap();
    kill_child(child, child_start);
    assert!(s.pids.is_empty(), "a session of this boot was taken: {s:?}");
    assert_eq!(o.pids, vec![child], "{o:?}");
}

/// The case behind the two tests above, as it happens on a desktop Mac: `login` or `sshd` (root)
/// leads a session holding this user's shells. A stale handle whose leader pid is now such a root
/// process must not claim those shells. Sampling only (a limit of 0 never kills); skipped when
/// this machine has no such session.
#[test]
fn a_session_led_by_another_user_is_not_claimed() {
    // SAFETY: a null buffer asks for the count only.
    let n = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    let mut pids = vec![0i32; n as usize + 256];
    // SAFETY: the buffer holds pids.len() pids; the size is in bytes.
    let got = unsafe {
        libc::proc_listallpids(
            pids.as_mut_ptr().cast(),
            (pids.len() * std::mem::size_of::<i32>()) as i32,
        )
    };
    pids.truncate(got.max(0) as usize);
    let foreign_leader = pids.iter().find_map(|&p| {
        start_time(p as u32)?;
        // SAFETY: getsid and a null signal have no memory preconditions.
        let sid = unsafe { libc::getsid(p) };
        // The leader exists, but libproc won't describe it to us: it runs as another user.
        let foreign = sid > 1
            && start_time(sid as u32).is_none()
            && (unsafe { libc::kill(sid, 0) } == 0
                || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM));
        foreign.then_some(sid as u32)
    });
    let Some(sid) = foreign_leader else {
        println!("no session led by another user's process here: nothing to check");
        return;
    };
    let exec = ProcessExecutor::default();
    let mut stale = Handle::Process {
        pid: sid,
        start_time: boot_time_us() + 1,
        tracked: vec![],
        gone: Vec::new(),
        lease: String::new(),
        tasks_max: 0,
        boot: String::new(),
    };
    let s = exec.sample(&mut stale, 0).unwrap();
    assert!(
        s.pids.is_empty(),
        "session {sid} (led by another user) was taken: {s:?}"
    );
}

/// No start waits on a process another thread spawned at the same moment: such a process
/// inherits whatever descriptors are open without close-on-exec (std's Command passes them on),
/// and the start's report channel had a moment like that, so a child living 0.3 s held a start
/// up for as long (19 of 8476 starts here, before the fix). 25 s of process churn, so only when
/// asked: `cargo test --test macos_exec -- --ignored no_start_waits`.
#[test]
#[ignore = "25 s of process churn"]
fn no_start_waits_on_a_process_spawned_meanwhile() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    let stop = Arc::new(AtomicBool::new(false));
    let left: Arc<Mutex<Vec<std::process::Child>>> = Arc::default();
    let spawners: Vec<_> = (0..6)
        .map(|_| {
            let (stop, left) = (stop.clone(), left.clone());
            std::thread::spawn(move || {
                let mut mine = std::collections::VecDeque::new();
                while !stop.load(Ordering::Relaxed) {
                    let child = std::process::Command::new("/bin/sleep")
                        .arg("0.3")
                        .spawn()
                        .unwrap();
                    mine.push_back((Instant::now(), child));
                    while mine
                        .front()
                        .is_some_and(|(t, _)| t.elapsed() > Duration::from_millis(400))
                    {
                        // Our own child: reap it.
                        mine.pop_front().unwrap().1.wait().unwrap();
                    }
                    if mine.len() > 120 {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
                left.lock()
                    .unwrap()
                    .extend(mine.into_iter().map(|(_, c)| c));
            })
        })
        .collect();
    let exec = ProcessExecutor::default();
    let dir = tempfile::tempdir().unwrap();
    let job = job(&["/usr/bin/true"]);
    let grant = Grant::default();
    let (started, mut n, mut slow) = (Instant::now(), 0, Vec::new());
    while started.elapsed() < Duration::from_secs(25) {
        n += 1;
        let launch = Launch {
            job: &job,
            grant: &grant,
            cwd: dir.path().to_path_buf(),
            env: base_env(),
            log: dir.path().join("out.log"),
            status: dir.path().join(format!("status-{n}.json")),
            potluck_bin: PathBuf::from(env!("CARGO_BIN_EXE_potluck")),
            name: format!("potluck-test-{n}"),
            limits: Limits {
                mem_max_bytes: 0,
                cpu_weight: 100,
                tasks_max: 100,
                kill_grace_ms: 10_000,
                log_max_bytes: 0,
            },
        };
        let t = Instant::now();
        exec.start(&launch).unwrap();
        if t.elapsed() > Duration::from_millis(150) {
            slow.push((n, t.elapsed()));
        }
    }
    stop.store(true, Ordering::Relaxed);
    for s in spawners {
        s.join().unwrap();
    }
    for mut child in left.lock().unwrap().drain(..) {
        // Our own children, not yet reaped.
        let _ = child.kill();
        let _ = child.wait();
    }
    println!("{n} starts, slow: {slow:?}");
    assert!(
        slow.is_empty(),
        "{} of {n} starts were slow: {slow:?}",
        slow.len()
    );
}

/// A crash report time as ReportCrash writes it: local time with its offset, via date(1).
fn report_time(us: u64) -> String {
    let date = |format: &str| {
        let out = std::process::Command::new("/bin/date")
            .args(["-r", &(us / 1_000_000).to_string(), format])
            .output()
            .unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    format!(
        "{}.{:04} {}",
        date("+%Y-%m-%d %H:%M:%S"),
        (us % 1_000_000) / 100,
        date("+%z")
    )
}

/// A process of the job exits and a sample sees it gone; a crash report naming its pid as the
/// parent of a process launched after that belongs to whoever has the pid now, not to the job.
/// One launched while it lived does, also when finish is called again later.
#[test]
fn crash_reports_of_a_reused_parent_pid_stay() {
    let script = r#"
        if (fork == 0) { open(F, ">child.pid"); print F $$; close F; sleep 1; exit 0 }
        sleep 60
    "#;
    let mut inv = Inv::start(&["/usr/bin/perl", "-e", script], base_env(), 0);
    let reports = inv.path("DiagnosticReports");
    std::fs::create_dir(&reports).unwrap();
    let exec = ProcessExecutor::with_reports_dir(&reports);
    let child = wait_for(Duration::from_secs(10), || {
        inv.sample(&exec);
        let pid = read_pid(&inv.work().join("child.pid"))?;
        inv.tracked().into_iter().find(|t| t.0 == pid)
    })
    .expect("the child tracked");
    wait_for(Duration::from_secs(10), || {
        (!inv.sample(&exec).pids.contains(&child.0)).then_some(())
    })
    .expect("the child seen gone");
    let gone_seen = now_ms() * 1000;
    let write = |name: &str, launched: u64| {
        let header = serde_json::json!({"timestamp": report_time(launched + 100_000)});
        let body = serde_json::json!({
            "pid": 99_999, "parentPid": child.0, "procLaunch": report_time(launched),
            "captureTime": report_time(launched + 100_000),
        });
        std::fs::write(reports.join(name), format!("{header}\n{body}")).unwrap();
    };
    write("while-alive.ips", child.1 + 200_000);
    write("after-gone.ips", gone_seen + 5_000_000);
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    inv.assert_all_gone();
    let crash = inv.path("crash");
    exec.finish(&inv.handle, &crash, 0).unwrap();
    assert!(crash.join("while-alive.ips").exists());
    assert!(!crash.join("after-gone.ips").exists());
    // A report written late, moved by a second finish: the same rule.
    write("late.ips", gone_seen + 6_000_000);
    exec.finish(&inv.handle, &crash, 0).unwrap();
    assert!(reports.join("after-gone.ips").exists() && reports.join("late.ips").exists());
}

/// After a restart that cut a start short, a fresh executor finds the supervisor from the record
/// it wrote beside its status file before it exec'd; where no start got that far, nothing.
#[test]
fn find_returns_what_a_cut_short_start_left() {
    let mut inv = Inv::start(&["/bin/sleep", "30"], job_env("test-find"), 0);
    let fresh = ProcessExecutor::default();
    let claimed = now_ms() - 60_000;
    let found = fresh
        .find("potluck-test-1", &inv.path("status.json"), claimed)
        .unwrap()
        .expect("found");
    // The handle start gave: its process limit, lease and boot too.
    assert_eq!(found, inv.handle);
    let Handle::Process {
        tasks_max,
        lease,
        boot,
        ..
    } = &found
    else {
        unreachable!()
    };
    // Inv::start asks for 1000; a machine whose kern.maxprocperuid is under 2000 caps it (a
    // GitHub runner's is 1333).
    let limit = 1000.min(max_procs_per_user() / 2);
    assert_eq!((*tasks_max, lease.as_str()), (limit, "test-find"));
    assert_eq!(boot, &boot_session());
    let mut adopted = found.clone();
    let s = wait_for(Duration::from_secs(10), || {
        let s = fresh.sample(&mut adopted, 0).unwrap();
        (s.pids.len() >= 2).then_some(s)
    })
    .expect("supervisor and sleep");
    assert_eq!(s.pids, inv.sample(&fresh).pids);
    assert_eq!(
        fresh
            .find("potluck-test-9", &inv.path("other.json"), claimed)
            .unwrap(),
        None
    );
    // Claimed again after this supervisor started: its record is an earlier attempt's.
    let later = now_ms() + 1_000;
    assert_eq!(
        fresh
            .find("potluck-test-1", &inv.path("status.json"), later)
            .unwrap(),
        None
    );
}

/// The same after a daemon restart: the handle, stored and loaded, says when the child was seen
/// gone, so a fresh executor's finish leaves the report of a later holder's child where it is.
#[test]
fn crash_reports_of_a_reused_parent_pid_stay_after_a_restart() {
    let script = r#"
        if (fork == 0) { open(F, ">child.pid"); print F $$; close F; sleep 1; exit 0 }
        sleep 60
    "#;
    let mut inv = Inv::start(&["/usr/bin/perl", "-e", script], base_env(), 0);
    let reports = inv.path("DiagnosticReports");
    std::fs::create_dir(&reports).unwrap();
    let exec = ProcessExecutor::with_reports_dir(&reports);
    let child = wait_for(Duration::from_secs(10), || {
        inv.sample(&exec);
        let pid = read_pid(&inv.work().join("child.pid"))?;
        inv.tracked().into_iter().find(|t| t.0 == pid)
    })
    .expect("the child tracked");
    wait_for(Duration::from_secs(10), || {
        (!inv.sample(&exec).pids.contains(&child.0)).then_some(())
    })
    .expect("the child seen gone");
    let gone_seen = now_ms() * 1000;
    let write = |name: &str, launched: u64| {
        let header = serde_json::json!({"timestamp": report_time(launched + 100_000)});
        let body = serde_json::json!({
            "pid": 99_999, "parentPid": child.0, "procLaunch": report_time(launched),
            "captureTime": report_time(launched + 100_000),
        });
        std::fs::write(reports.join(name), format!("{header}\n{body}")).unwrap();
    };
    write("while-alive.ips", child.1 + 200_000);
    write("after-gone.ips", gone_seen + 5_000_000);
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    inv.assert_all_gone();
    // The daemon restarts: the handle comes back from the store, the executor is new.
    let stored = serde_json::to_string(&inv.handle).unwrap();
    let handle: Handle = serde_json::from_str(&stored).unwrap();
    let fresh = ProcessExecutor::with_reports_dir(&reports);
    let crash = inv.path("crash");
    fresh.finish(&handle, &crash, 0).unwrap();
    assert!(crash.join("while-alive.ips").exists());
    assert!(
        reports.join("after-gone.ips").exists(),
        "a later holder's child's report was taken"
    );
}

/// This boot's session id, `kern.bootsessionuuid`.
fn boot_session() -> String {
    let out = std::process::Command::new("/usr/sbin/sysctl")
        .args(["-n", "kern.bootsessionuuid"])
        .output()
        .unwrap();
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

/// The kernel moves kern.boottime with every step of the wall clock, and start times stay: after
/// the clock is stepped forward by more than a job has run, its supervisor seems to have started
/// before this boot. A handle that names its boot session still keeps its session; one from
/// another boot session doesn't get it.
#[test]
fn a_session_of_this_boot_survives_a_forward_clock_step() {
    let dir = tempfile::tempdir().unwrap();
    let (mut leader, _start, child) = leaderless_session(dir.path());
    let pid = leader.id();
    let child_start = start_time(child).unwrap();
    leader.wait().unwrap();
    let exec = ProcessExecutor::default();
    // As recorded: started an hour before what kern.boottime says now.
    let stepped = boot_time_us() - 3_600_000_000;
    let handle = |boot: String| Handle::Process {
        pid,
        start_time: stepped,
        tracked: vec![],
        gone: Vec::new(),
        lease: String::new(),
        tasks_max: 0,
        boot,
    };
    let this_boot = exec.sample(&mut handle(boot_session()), 0).unwrap();
    let other_boot = exec
        .sample(
            &mut handle("00000000-0000-0000-0000-000000000000".into()),
            0,
        )
        .unwrap();
    kill_child(child, child_start);
    assert_eq!(this_boot.pids, vec![child], "{this_boot:?}");
    assert!(other_boot.pids.is_empty(), "{other_boot:?}");
}

/// `kern.maxprocperuid`: the most processes one user may have.
fn max_procs_per_user() -> u32 {
    let out = std::process::Command::new("/usr/sbin/sysctl")
        .args(["-n", "kern.maxprocperuid"])
        .output()
        .unwrap();
    String::from_utf8(out.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// This user's processes, counted by real user id as `ps -U` lists them (ps included).
fn user_processes() -> u64 {
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let out = std::process::Command::new("/bin/ps")
        .args(["-U", &uid.to_string(), "-o", "pid="])
        .output()
        .unwrap();
    String::from_utf8(out.stdout).unwrap().lines().count() as u64
}

/// The process limit the handle keeps: the one asked for, but at most half of
/// kern.maxprocperuid.
#[test]
fn the_process_limit_is_at_most_half_the_users() {
    let half = max_procs_per_user() / 2;
    for (asked, want) in [(40, 40.min(half)), (u32::MAX, half), (0, 0)] {
        let inv = Inv::start_full(
            &ProcessExecutor::default(),
            &["/bin/sh", "-c", "exit 0"],
            base_env(),
            0,
            asked,
        );
        let Handle::Process { tasks_max, .. } = &inv.handle else {
            unreachable!()
        };
        assert_eq!(*tasks_max, want, "asked {asked}");
        inv.status(Duration::from_secs(10)).expect("status file");
    }
}

/// Past its process limit the monitor kills every process of the invocation and says how many
/// it saw; under it nothing happens. The job here forks 12 children under a limit of 1000 (its
/// RLIMIT_NPROC leaves room for them), then the limit in its handle is lowered to 8.
#[test]
fn too_many_processes_are_killed_and_reported() {
    let script = r#"
        for (1..12) { if (fork == 0) { sleep 60; exit 0 } }
        open(F, ">ready"); close F; sleep 60
    "#;
    let mut inv = Inv::start(&["/usr/bin/perl", "-e", script], base_env(), 0);
    let exec = ProcessExecutor::default();
    let all = wait_for(Duration::from_secs(10), || {
        let s = inv.sample(&exec);
        (inv.work().join("ready").exists() && s.pids.len() == 14).then_some(s)
    })
    .expect("supervisor, perl and 12 children");
    assert_eq!(all.over_tasks, None, "{all:?}");
    let Handle::Process { tasks_max, .. } = &mut inv.handle else {
        unreachable!()
    };
    *tasks_max = 8;
    let over = inv.sample(&exec);
    assert_eq!(over.over_tasks, Some((14, 8)), "{over:?}");
    assert_eq!(over.pids.len(), 14, "{over:?}");
    inv.assert_all_gone();
    // It stays over for this executor; the supervisor was killed too, so there is no status.
    let later = inv.sample(&exec);
    assert!(
        later.pids.is_empty() && later.over_tasks == Some((14, 8)),
        "{later:?}"
    );
    assert!(inv.status(Duration::from_millis(200)).is_none());
}

/// The backstop between samples: the job starts with RLIMIT_NPROC, soft and hard, at the user's
/// process count plus its limit, and can't raise it.
#[test]
fn the_job_starts_with_a_process_rlimit_it_cannot_raise() {
    let _many = many_processes();
    let before = user_processes();
    let inv = Inv::start_full(
        &ProcessExecutor::default(),
        &[
            "/bin/sh",
            "-c",
            "ulimit -u; ulimit -Hu; (ulimit -u unlimited) 2>/dev/null && echo raised || echo refused",
        ],
        base_env(),
        0,
        40,
    );
    let after = user_processes();
    inv.status(Duration::from_secs(10)).expect("status file");
    let log = inv.log();
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(lines.len(), 3, "{log}");
    let (soft, hard): (u64, u64) = (lines[0].parse().unwrap(), lines[1].parse().unwrap());
    assert_eq!(soft, hard, "{log}");
    // Other processes of this user come and go meanwhile.
    let (low, high) = (before.min(after) + 41 - 30, before.max(after) + 41 + 30);
    assert!((low..=high).contains(&soft), "{soft} not in {low}..={high}");
    assert_eq!(lines[2], "refused", "{log}");
}

/// Without a sample in between, the RLIMIT_NPROC stops a job that forks as fast as it can near
/// its limit (40): forks fail with EAGAIN. Then a sample finds it over its limit, unless the
/// user's other processes grew meanwhile and took the room.
#[test]
fn the_rlimit_stops_forks_between_samples() {
    let _many = many_processes();
    let script = r#"
        use POSIX;
        my ($n, $err) = (0, "");
        for (1..80) {
            my $pid = fork;
            if (!defined $pid) { $err = $! + 0; last }
            if ($pid == 0) { sleep 60; POSIX::_exit(0) }
            $n++;
        }
        open(F, ">forked"); print F "$n $err\n"; close F; sleep 60
    "#;
    let mut inv = Inv::start_full(
        &ProcessExecutor::default(),
        &["/usr/bin/perl", "-e", script],
        base_env(),
        0,
        40,
    );
    let got = wait_for(Duration::from_secs(10), || {
        std::fs::read_to_string(inv.work().join("forked"))
            .ok()
            .filter(|s| s.ends_with('\n'))
    })
    .expect("the job reports its forks");
    let fields: Vec<&str> = got.split_whitespace().collect();
    let n: u32 = fields[0].parse().unwrap();
    println!("forked {n} of 80 under a limit of 40: {got:?}");
    assert!(n < 80, "{got}");
    assert_eq!(
        fields.get(1),
        Some(&libc::EAGAIN.to_string().as_str()),
        "{got}"
    );
    let s = inv.sample(&ProcessExecutor::default());
    if s.pids.len() > 40 {
        assert_eq!(s.over_tasks, Some((s.pids.len() as u32, 40)), "{s:?}");
    }
}

/// The environment a job's processes get from the daemon: its POTLUCK_JOB, and `tag` as its
/// POTLUCK_LEASE (each test its own, so tests running at once don't adopt each other's daemons).
fn job_env(tag: &str) -> BTreeMap<String, String> {
    let mut env = base_env();
    env.insert("POTLUCK_JOB".into(), "test-1".into());
    env.insert("POTLUCK_LEASE".into(), tag.into());
    env
}

/// Perl that starts a daemon the way `daemon(3)` does: fork, setsid, fork, and the middle process
/// exits at once, so no sample sees the chain; the daemon's parent is launchd. The daemon writes
/// its pid to `file` and execs `potluck _supervise` over a 60 s sleep: macOS hides the
/// environment of its own programs (/bin/sleep, /usr/bin/perl), not of others. With `env -i`
/// first, it clears its environment.
fn daemon_script(file: &str, clear_env: bool) -> String {
    let potluck = env!("CARGO_BIN_EXE_potluck");
    let env = if clear_env {
        r#""/usr/bin/env", "-i", "#
    } else {
        ""
    };
    format!(
        r#"use POSIX;
        if (fork == 0) {{
            POSIX::setsid();
            if (fork == 0) {{
                open(F, ">{file}"); print F $$; close F;
                exec {env}"{potluck}", "_supervise", "{file}.status", "--", "/bin/sleep", "60";
            }}
            POSIX::_exit(0);
        }}
        wait;"#
    )
}

/// Waits for the pid a daemon of [`daemon_script`] wrote, once it is `potluck _supervise` with
/// launchd as its parent, and its sleep has started.
fn daemon_pid(dir: &Path, file: &str) -> (u32, u64) {
    let pid = wait_for(Duration::from_secs(10), || {
        read_pid(&dir.join(file)).filter(|&p| {
            let ps = |args: &[&str]| {
                let out = std::process::Command::new("/bin/ps")
                    .args(args)
                    .output()
                    .unwrap();
                String::from_utf8_lossy(&out.stdout).into_owned()
            };
            let me = ps(&["-o", "ppid=,command=", "-p", &p.to_string()]);
            let children = ps(&["-axo", "ppid=,command="]);
            me.trim_start().starts_with("1 ")
                && me.contains("_supervise")
                && children
                    .lines()
                    .any(|l| l.trim_start().starts_with(&format!("{p} /bin/sleep")))
        })
    })
    .expect("a daemon reparented to launchd");
    (pid, start_time(pid).unwrap())
}

/// Kills a daemon of [`daemon_script`] and its sleep: SIGKILL to its process group, the one its
/// exited parent made with setsid, holding just the two of them. After checking it is still the
/// same process.
fn kill_daemon(pid: u32, start: u64) {
    if start_time(pid) == Some(start) {
        // SAFETY: getpgid and killpg of a process we started, verified by its start time just
        // above; a group can't be reused while it has members.
        unsafe {
            let group = libc::getpgid(pid as i32);
            if group > 1 {
                libc::killpg(group, libc::SIGKILL);
            }
        }
    }
    assert!(
        wait_for(Duration::from_secs(5), || (!exists(pid, start))
            .then_some(()))
        .is_some()
    );
}

/// A daemon a job starts by a double fork, whose middle process exits before any sample, is
/// still the job's: reparented to launchd after the job started, with the job's POTLUCK_LEASE. It
/// and its child are counted and killed with the job.
#[test]
fn a_daemon_the_job_starts_is_adopted_and_killed() {
    let script = format!("{} sleep 60", daemon_script("daemon.pid", false));
    let mut inv = Inv::start(
        &["/usr/bin/perl", "-e", &script],
        job_env("test-adopted"),
        0,
    );
    let exec = ProcessExecutor::default();
    let (pid, start) = daemon_pid(&inv.work(), "daemon.pid");
    let s = inv.sample(&exec);
    let adopted = s.pids.contains(&pid);
    if !adopted {
        kill_daemon(pid, start);
    }
    assert!(adopted, "the daemon {pid} escaped: {s:?}");
    // The supervisor, perl, the daemon and its sleep.
    assert_eq!(s.pids.len(), 4, "{s:?}");
    assert!(inv.tracked().contains(&(pid, start)));
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    inv.assert_all_gone();
}

/// Only the job's own escapes are adopted: a daemon started with another POTLUCK_LEASE and one
/// that cleared its environment (best effort: it escapes) are left alone.
#[test]
fn only_the_jobs_own_daemons_are_adopted() {
    let script = format!(
        "{} {} $ENV{{POTLUCK_LEASE}} = 'test-another'; {} sleep 60",
        daemon_script("ours.pid", false),
        daemon_script("cleared.pid", true),
        daemon_script("other.pid", false),
    );
    let mut inv = Inv::start(
        &["/usr/bin/perl", "-e", &script],
        job_env("test-only-own"),
        0,
    );
    let exec = ProcessExecutor::default();
    let ours = daemon_pid(&inv.work(), "ours.pid");
    let cleared = daemon_pid(&inv.work(), "cleared.pid");
    let other = daemon_pid(&inv.work(), "other.pid");
    let s = inv.sample(&exec);
    exec.signal(&inv.handle, libc::SIGKILL).unwrap();
    let survivors: Vec<bool> = [cleared, other, ours]
        .iter()
        .map(|&(p, t)| {
            let alive = exists(p, t);
            kill_daemon(p, t);
            alive
        })
        .collect();
    assert!(s.pids.contains(&ours.0), "{s:?}");
    assert!(!survivors[2], "ours survived the job's SIGKILL");
    for (name, p) in [("cleared", cleared), ("other", other)] {
        assert!(!s.pids.contains(&p.0), "{name} {p:?} was adopted: {s:?}");
    }
    assert_eq!(survivors[..2], [true, true], "cleared, other");
    inv.assert_all_gone();
}

/// A job can run again on a machine it ran on (queued again after its machine was lost): that
/// run's processes carry the same POTLUCK_JOB, and its supervisor's parent is launchd too. A
/// late kill or finish of the earlier run (a cleanup tried again, crash reports collected late)
/// leaves the later run alone: escapes are told by POTLUCK_LEASE, new for every run.
#[test]
fn a_later_run_of_the_same_job_is_not_adopted() {
    let exec = ProcessExecutor::default();
    let run = |lease: &str| {
        let mut env = job_env("test-again");
        env.insert("POTLUCK_LEASE".into(), lease.into());
        Inv::start_with(&exec, &["/bin/sleep", "60"], env, 0)
    };
    let mut first = run("lease-first");
    wait_for(Duration::from_secs(5), || {
        (first.sample(&exec).pids.len() == 2).then_some(())
    })
    .unwrap();
    exec.signal(&first.handle, libc::SIGKILL).unwrap();
    first.assert_all_gone();
    let mut second = run("lease-second");
    wait_for(Duration::from_secs(5), || {
        (second.sample(&exec).pids.len() == 2).then_some(())
    })
    .unwrap();
    // The first run's handle, killed again and finished again.
    let late = first.sample(&exec);
    exec.signal(&first.handle, libc::SIGKILL).unwrap();
    exec.finish(&first.handle, &first.path("crash"), 0).unwrap();
    let s = second.sample(&exec);
    assert!(late.pids.is_empty(), "{late:?}");
    assert_eq!(s.pids.len(), 2, "the later run was killed: {s:?}");
}

/// A job that ends with nothing killed by potluck, as one lost after 20 failed samples does
/// (no sample saw its processes either): finish kills what is left of it, a daemon it started
/// included, before the daemon frees its lease.
#[test]
fn finish_kills_what_is_left() {
    let script = format!(
        r#"if (fork == 0) {{ open(F, ">child.pid"); print F $$; close F; sleep 60; exit 0 }}
        {} sleep 60"#,
        daemon_script("daemon.pid", false)
    );
    let inv = Inv::start(
        &["/usr/bin/perl", "-e", &script],
        job_env("test-finish-kills"),
        0,
    );
    let child = wait_for(Duration::from_secs(10), || {
        read_pid(&inv.work().join("child.pid"))
    })
    .expect("child");
    let child = (child, start_time(child).unwrap());
    let daemon = daemon_pid(&inv.work(), "daemon.pid");
    let exec = ProcessExecutor::default();
    let finished = exec.finish(&inv.handle, &inv.path("crash"), 0);
    let left: Vec<(u32, u64)> = [inv.leader(), child, daemon]
        .into_iter()
        .filter(|&(p, s)| exists(p, s))
        .collect();
    if left.contains(&daemon) {
        kill_daemon(daemon.0, daemon.1);
    }
    assert!(finished.is_ok(), "{finished:?}");
    assert!(left.is_empty(), "left after finish: {left:?}");
}

/// A fork bomb (every process forks in a loop) under a limit of 30: RLIMIT_NPROC holds it to a
/// few dozen processes, the next sample finds it over its limit and kills all of it. Only when
/// asked, under a safety net in case the limit fails: `ulimit -u` set a little above this user's
/// process count, then `cargo test --test macos_exec -- --ignored a_fork_bomb`.
#[test]
#[ignore = "a fork bomb: run it under ulimit -u"]
fn a_fork_bomb_is_held_and_ended() {
    let _many = many_processes();
    let mut inv = Inv::start_full(
        &ProcessExecutor::default(),
        &["/usr/bin/perl", "-e", "while (1) { fork }"],
        base_env(),
        0,
        30,
    );
    let exec = ProcessExecutor::default();
    let mut most = 0;
    let over = wait_for(Duration::from_secs(10), || {
        let s = inv.sample(&exec);
        most = most.max(s.pids.len());
        std::thread::sleep(Duration::from_millis(100));
        s.over_tasks.map(|o| (o, s.pids.len()))
    });
    println!("over: {over:?}, most processes seen: {most}");
    let ((seen, limit), _) = over.expect("over its process limit");
    assert_eq!(limit, 30);
    assert!(seen > 30 && seen < 100, "{seen}");
    inv.assert_all_gone();
}

/// Start times are the wall clock at each start: after the clock is set back, a daemon the job
/// starts can show an earlier start than the job's supervisor. It is still the job's: its
/// POTLUCK_LEASE says so, new for every run. (Here the supervisor's start in the handle is
/// moved an hour later instead.)
#[test]
fn a_daemon_is_adopted_after_the_clock_goes_back() {
    let script = format!("{} sleep 60", daemon_script("daemon.pid", false));
    let inv = Inv::start(
        &["/usr/bin/perl", "-e", &script],
        job_env("test-clock-back"),
        0,
    );
    let exec = ProcessExecutor::default();
    let daemon = daemon_pid(&inv.work(), "daemon.pid");
    let Handle::Process {
        pid,
        start_time,
        lease,
        boot,
        ..
    } = inv.handle.clone()
    else {
        unreachable!()
    };
    let mut later = Handle::Process {
        pid,
        start_time: start_time + 3_600_000_000,
        tracked: vec![],
        gone: Vec::new(),
        lease,
        tasks_max: 0,
        boot,
    };
    let s = exec.sample(&mut later, 0).unwrap();
    let adopted = s.pids.contains(&daemon.0);
    kill_daemon(daemon.0, daemon.1);
    assert!(adopted, "the daemon {daemon:?} escaped: {s:?}");
}
