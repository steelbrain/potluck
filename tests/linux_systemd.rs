//! The systemd executor against the real user manager: units, limits, signals, adoption. Linux
//! only. Each test is skipped, with a note, where no systemd user manager answers.
//!
//! Units are named `potluck-test-<tag>-<random>`; every test finishes its units (also when it
//! fails, from `Drop`) and checks that systemd no longer knows them.

#![cfg(target_os = "linux")]

mod common;

use potluck::exec::linux::SystemdExecutor;
use potluck::exec::{Executor, Launch, Limits, Sample};
use potluck::types::{
    ExitStatus, Grant, Handle, JobRecord, JobSpec, JobState, RepoRef, Request, Usage, now_ms,
};
use std::collections::{BTreeMap, BTreeSet};
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const POTLUCK: &str = env!("CARGO_BIN_EXE_potluck");
/// Processes started over ssh run SCHED_IDLE on the test machine, and a loaded machine is slow
/// to schedule them: be patient.
const PATIENCE: Duration = Duration::from_secs(120);
const PATH: (&str, &str) = ("PATH", "/usr/local/bin:/usr/bin:/bin");

fn user_manager() -> bool {
    let ok = Command::new("systemctl")
        .args(["--user", "show-environment"])
        .output()
        .is_ok_and(|o| o.status.success());
    if !ok {
        eprintln!("skipped: no systemd user manager answers");
    }
    ok
}

/// Whether this systemd keeps an ended unit's MemoryPeak (256 and later). systemd 255 (Ubuntu 24.04)
/// shows a stale one, so potluck counts every OOM kill there as over the unit's own limit.
fn keeps_memory_peak() -> bool {
    let out = Command::new("systemd-run")
        .arg("--version")
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    let version = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse::<u32>().ok());
    version.is_some_and(|v| v >= 256)
}

fn limits() -> Limits {
    Limits {
        mem_max_bytes: 512 << 20,
        cpu_weight: 100,
        tasks_max: 256,
        kill_grace_ms: 10_000,
        log_max_bytes: 0,
    }
}

/// One invocation in its own temp dir. Dropping it finishes the unit.
struct Job {
    dir: tempfile::TempDir,
    name: String,
    handle: Option<Handle>,
    /// What runs as `_supervise`: this build's potluck unless a test says otherwise.
    supervisor: PathBuf,
    /// When it was started (the claim time `finish` gets).
    started: u64,
}

impl Job {
    fn new(tag: &str) -> Job {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("work")).unwrap();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let random = std::collections::hash_map::RandomState::new().hash_one(nanos);
        Job {
            dir,
            name: format!("potluck-test-{tag}-{random:016x}"),
            handle: None,
            supervisor: POTLUCK.into(),
            started: 0,
        }
    }

    fn start(tag: &str, cmd: &[&str], env: &[(&str, &str)], limits: Limits) -> Job {
        let mut job = Job::new(tag);
        job.try_start(cmd, env, limits).unwrap();
        job
    }

    fn try_start(
        &mut self,
        cmd: &[&str],
        env: &[(&str, &str)],
        limits: Limits,
    ) -> anyhow::Result<()> {
        let record = record(cmd);
        let grant = Grant::default();
        let launch = Launch {
            job: &record,
            grant: &grant,
            cwd: self.cwd(),
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            log: self.log_path(),
            status: self.dir.path().join("status.json"),
            potluck_bin: self.supervisor.clone(),
            name: self.name.clone(),
            limits,
        };
        self.started = potluck::types::now_ms();
        self.handle = Some(SystemdExecutor.start(&launch)?);
        Ok(())
    }

    fn unit(&self) -> String {
        format!("{}.service", self.name)
    }
    fn env_file(&self) -> PathBuf {
        SystemdExecutor::env_file(&self.unit())
    }
    fn cwd(&self) -> PathBuf {
        self.dir.path().join("work")
    }
    fn log_path(&self) -> PathBuf {
        self.dir.path().join("out.log")
    }
    fn crash_dir(&self) -> PathBuf {
        self.dir.path().join("crash")
    }
    fn log(&self) -> String {
        std::fs::read_to_string(self.log_path()).unwrap_or_default()
    }
    fn status(&self) -> Option<ExitStatus> {
        let bytes = std::fs::read(self.dir.path().join("status.json")).ok()?;
        Some(serde_json::from_slice(&bytes).unwrap())
    }
    fn handle(&mut self) -> &mut Handle {
        self.handle.as_mut().unwrap()
    }
    fn sample(&mut self) -> Sample {
        SystemdExecutor.sample(self.handle(), 0).unwrap()
    }
    fn signal(&mut self, sig: i32) {
        SystemdExecutor.signal(self.handle(), sig).unwrap();
    }

    /// Samples until every process has exited; returns the last sample.
    fn wait_end(&mut self) -> Sample {
        wait_for("the job to end", || {
            let s = self.sample();
            s.pids.is_empty().then_some(s)
        })
    }

    /// Finishes the unit and checks that systemd no longer knows it.
    fn finish(&mut self) {
        let handle = self.handle.take().unwrap();
        SystemdExecutor
            .finish(&handle, &self.crash_dir(), self.started)
            .unwrap();
        assert_eq!(unit_prop(&self.unit(), "LoadState"), "not-found");
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = SystemdExecutor.finish(&handle, &self.crash_dir(), self.started);
        }
    }
}

fn record(cmd: &[&str]) -> JobRecord {
    JobRecord {
        id: "test-1".into(),
        spec: JobSpec {
            key: None,
            labels: BTreeMap::new(),
            priority: 3,
            request: Request::default(),
            on: BTreeMap::new(),
            repo: RepoRef {
                id: "test-00000000".into(),
                source: "/nonexistent".into(),
                commit: "0".repeat(40),
                branch: None,
                submodules: Vec::new(),
                carried: Vec::new(),
            },
            workspace: None,
            advance: false,
            cmd: cmd.iter().map(|s| s.to_string()).collect(),
            env: BTreeMap::new(),
            unset_env: Default::default(),
            timeout_ms: 60_000,
            hold_ms: None,
            keep: Vec::new(),
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
        stuck_pids: Vec::new(),
        usage: Usage::default(),
        passes: 0,
        stalled: false,
        kept: Vec::new(),
        notes: Vec::new(),
    }
}

fn wait_for<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let started = Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(started.elapsed() < PATIENCE, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn unit_prop(unit: &str, prop: &str) -> String {
    let out = Command::new("systemctl")
        .args([
            "--user",
            "show",
            "--value",
            &format!("--property={prop}"),
            unit,
        ])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn cgroup_file(handle: &Handle, file: &str) -> String {
    let Handle::Systemd {
        cgroup: Some(cg), ..
    } = handle
    else {
        panic!("no cgroup in {handle:?}");
    };
    let path = Path::new("/sys/fs/cgroup")
        .join(cg.trim_start_matches('/'))
        .join(file);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .trim()
        .to_string()
}

fn gone(pid: u32) -> bool {
    !Path::new(&format!("/proc/{pid}")).exists()
}

#[test]
fn exit_codes_reach_the_status_file() {
    if !user_manager() {
        return;
    }
    let mut failing = Job::start("exit7", &["sh", "-c", "exit 7"], &[PATH], limits());
    let mut passing = Job::start("exit0", &["true"], &[PATH], limits());
    for (job, code) in [(&mut failing, 7), (&mut passing, 0)] {
        let sample = job.wait_end();
        assert!(!sample.over_memory);
        let status = wait_for("the status file", || job.status());
        assert_eq!((status.code, status.signal), (Some(code), None));
    }
    // A failed unit stays loaded, so the daemon can still read how it ended, until finish.
    let unit = failing.unit();
    wait_for("the unit to fail", || {
        (unit_prop(&unit, "ActiveState") == "failed").then_some(())
    });
    assert_eq!(unit_prop(&unit, "Result"), "exit-code");
    failing.finish();
    passing.finish();
}

#[test]
fn environment_is_exactly_the_launch_env() {
    if !user_manager() {
        return;
    }
    let env = [
        ("PATH", "/usr/bin:/bin"),
        ("POTLUCK_TEST_SPACES", " a  b "),
        ("POTLUCK_TEST_DOLLARS", "$HOME ${HOME} $$ %n %h"),
        ("POTLUCK_TEST_NEWLINE", "one\ntwo"),
        ("POTLUCK_TEST_QUOTES", "'\"\\"),
        ("POTLUCK_TEST_EMPTY", ""),
    ];
    let mut job = Job::start("env", &["cat", "/proc/self/environ"], &env, limits());
    job.wait_end();
    let got: BTreeSet<String> = job
        .log()
        .split('\0')
        .filter(|v| !v.is_empty())
        .map(String::from)
        .collect();
    let want: BTreeSet<String> = env.iter().map(|(k, v)| format!("{k}={v}")).collect();
    assert_eq!(got, want);
    job.finish();
}

#[test]
fn arguments_reach_the_command_untouched() {
    if !user_manager() {
        return;
    }
    let args = ["$HOME", "${HOME}", "$$", "%n", "a b", "-", "--", "x=y"];
    let mut cmd = vec!["printf", "%s|"];
    cmd.extend(args);
    let mut job = Job::start("args", &cmd, &[PATH], limits());
    job.wait_end();
    assert_eq!(job.log(), format!("{}|", args.join("|")));
    job.finish();
}

#[test]
fn working_directory_and_appended_log() {
    if !user_manager() {
        return;
    }
    let mut job = Job::new("cwd");
    std::fs::write(job.log_path(), "earlier line\n").unwrap();
    job.try_start(&["sh", "-c", "pwd; echo to-stderr >&2"], &[PATH], limits())
        .unwrap();
    job.wait_end();
    let cwd = job.cwd().canonicalize().unwrap();
    assert_eq!(
        job.log(),
        format!("earlier line\n{}\nto-stderr\n", cwd.display())
    );
    job.finish();
}

/// A job's unit gets the daemon's limits on open files, soft and hard (the daemon raises its soft
/// one at start), as a macOS job inherits them; not the user manager's default soft 1024.
#[test]
fn a_job_gets_the_daemon_s_limits_on_open_files() {
    if !user_manager() {
        return;
    }
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit fills `lim`.
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) }, 0);
    // Values no default has. Lowering the hard limit is always allowed; this process doesn't need
    // more.
    assert!(
        lim.rlim_max >= 4000,
        "a hard limit of {} is too low",
        lim.rlim_max
    );
    lim.rlim_cur = 3000;
    lim.rlim_max = 4000;
    // SAFETY: setrlimit reads `lim`.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) }, 0);
    let mut job = Job::start(
        "nofile",
        &["sh", "-c", "echo $(ulimit -Sn) $(ulimit -Hn)"],
        &[PATH],
        limits(),
    );
    job.wait_end();
    assert_eq!(job.log(), "3000 4000\n");
    job.finish();
}

/// The job's stdout is a pipe the supervisor copies into the log, not the log file: opening
/// /dev/stdout (which on Linux reopens the file behind descriptor 1, truncating it for `>`) can't
/// cut the log short. The log limit reaches the supervisor.
#[test]
fn dev_stdout_in_a_job_leaves_its_log_whole() {
    if !user_manager() {
        return;
    }
    let script = "echo one; echo two > /dev/stdout; echo three 2>/dev/stdout >&2; \
                  echo four | tee /dev/stdout; echo five";
    let mut job = Job::start("devstdout", &["sh", "-c", script], &[PATH], limits());
    job.wait_end();
    assert_eq!(job.log(), "one\ntwo\nthree\nfour\nfour\nfive\n");
    assert_eq!(job.status().map(|s| s.code), Some(Some(0)));
    job.finish();

    let script = "i=1000; while [ $i -lt 1500 ]; do echo line $i; i=$((i+1)); done";
    let mut job = Job::start(
        "logmax",
        &["sh", "-c", script],
        &[PATH],
        Limits {
            log_max_bytes: 1000,
            ..limits()
        },
    );
    job.wait_end();
    let log = job.log();
    assert!(log.starts_with("line 1000\n"), "{log}");
    assert!(
        log.contains(
            "--- potluck: the output reached log_max_mib (1000 bytes): what follows is dropped"
        ) && log.contains("--- potluck: 0 bytes of output dropped after the log reached")
            && log.ends_with("line 1499\n"),
        "{log}"
    );
    job.finish();
}

/// A supervisor stopped from outside (SIGSTOP) is continued by the next sample, so it reads the
/// job's output again and records the exit code of a command that ended meanwhile.
#[test]
fn a_stopped_supervisor_is_continued() {
    if !user_manager() {
        return;
    }
    let mut job = Job::start(
        "stopped",
        &["sh", "-c", "sleep 1; exit 5"],
        &[PATH],
        limits(),
    );
    let pid: i32 = unit_prop(&job.unit(), "MainPID").parse().unwrap();
    assert!(pid > 1);
    // SAFETY: a signal to the supervisor this test started.
    unsafe { libc::kill(pid, libc::SIGSTOP) };
    std::thread::sleep(Duration::from_secs(3));
    assert!(job.status().is_none());
    job.sample();
    let status = wait_for("the status file", || job.status());
    assert_eq!(status.code, Some(5));
    job.wait_end();
    job.finish();
}

#[test]
fn going_over_the_memory_limit_is_reported_as_over_memory() {
    if !user_manager() {
        return;
    }
    // Non-zero bytes: zero-filled pages can end up backed by the shared zero page and not count.
    let cmd: &[&str] = if Path::new("/usr/bin/python3").exists() {
        &[
            "python3",
            "-c",
            "import time; b = b'x' * (300 << 20); time.sleep(60)",
        ]
    } else {
        &["perl", "-e", "my $x = 'x' x (300 << 20); sleep 60"]
    };
    let mut job = Job::start(
        "oom",
        cmd,
        &[PATH],
        Limits {
            mem_max_bytes: 64 << 20,
            ..limits()
        },
    );
    let sample = job.wait_end();
    assert!(sample.over_memory && !sample.oom_killed, "{sample:?}");
    // It reached its limit, whatever the samples a second apart saw (memory.peak, then
    // systemd's MemoryPeak once the cgroup is gone, which systemd before 256 doesn't keep).
    if keeps_memory_peak() {
        assert!(sample.peak_mem_bytes >= 62 << 20, "{sample:?}");
    }
    // The kernel killed the allocating process; the supervisor survived to record it.
    let status = wait_for("the status file", || job.status());
    assert_eq!(status.signal, Some(libc::SIGKILL), "{status:?}");
    assert_eq!(unit_prop(&job.unit(), "Result"), "oom-kill");
    // Still reported from a fresh look at the unit, after its cgroup is gone.
    let mut handle = job.handle().clone();
    if let Handle::Systemd { cgroup, .. } = &mut handle {
        *cgroup = None;
    }
    let again = potluck::exec::platform().sample(&mut handle, 0).unwrap();
    assert!(
        again.over_memory && !again.oom_killed && again.pids.is_empty(),
        "{again:?}"
    );
    if keeps_memory_peak() {
        assert!(again.peak_mem_bytes >= 62 << 20, "{again:?}");
    }
    job.finish();
}

/// When the OOM killer acts before systemd has seen the start through, systemd-run fails, but
/// the job ran and went over its limit: that is reported, not "could not start". Under a busy
/// user manager this happens to jobs that go over within milliseconds; a 1 MiB limit makes it
/// happen every time (the launcher itself is killed).
#[test]
fn going_over_before_the_start_is_seen_is_over_memory() {
    if !user_manager() {
        return;
    }
    let mut job = Job::new("oomstart");
    job.try_start(
        &["true"],
        &[PATH],
        Limits {
            mem_max_bytes: 1 << 20,
            ..limits()
        },
    )
    .unwrap();
    let sample = job.wait_end();
    assert!(sample.over_memory && !sample.oom_killed, "{sample:?}");
    job.finish();
    assert!(!job.env_file().exists(), "the env file is left behind");
}

/// A slice of this test run's own, `potluck-test-<random>.slice`, removed with its unit on drop.
/// Starting a unit in it makes systemd start its parents, `potluck-test.slice` and
/// `potluck.slice`, and keep them after: those are stopped too, unless another test process still
/// has a slice under them. Each run holds a shared lock on a file in the user's runtime directory
/// while its slice exists, and stops the parents only if it gets the exclusive lock.
struct LimitedSlice {
    name: String,
    unit: String,
    lock: std::fs::File,
}

impl LimitedSlice {
    fn new(unit: &str) -> LimitedSlice {
        use std::os::fd::AsRawFd;
        let runtime = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| format!("/run/user/{}", unsafe { libc::getuid() }).into());
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(runtime.join("potluck-test-slices.lock"))
            .unwrap();
        // SAFETY: flock on a descriptor this function owns.
        assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_SH) }, 0);
        let random = std::collections::hash_map::RandomState::new().hash_one(unit);
        LimitedSlice {
            name: format!("potluck-test-{random:016x}.slice"),
            unit: unit.to_string(),
            lock,
        }
    }
}

impl Drop for LimitedSlice {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        let systemctl = |args: &[&str]| {
            let _ = Command::new("systemctl").arg("--user").args(args).output();
        };
        systemctl(&["stop", &self.unit]);
        systemctl(&["reset-failed", &self.unit]);
        systemctl(&["revert", &self.name]);
        systemctl(&["stop", &self.name]);
        // This run's shared lock goes first: of runs ending together, the last one gets the
        // exclusive lock.
        // SAFETY: flock on a descriptor this value owns.
        let fd = self.lock.as_raw_fd();
        unsafe { libc::flock(fd, libc::LOCK_UN) };
        if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            systemctl(&["stop", "potluck-test.slice", "potluck.slice"]);
        }
    }
}

/// When the OOM killer acts because something above the job ran short (here a slice with a
/// lower limit than the unit's; on a real machine the machine's own memory, or systemd-oomd),
/// the job didn't go over its own limit: that is `oom_killed`, not `over_memory`. The unit is
/// started by hand in that slice, the way the executor would start it, then sampled from its
/// handle once systemd has removed its cgroup.
#[test]
fn an_oom_kill_above_the_unit_is_not_over_its_limit() {
    if !user_manager() {
        return;
    }
    if !Path::new("/usr/bin/python3").exists() {
        eprintln!("skipped: no /usr/bin/python3");
        return;
    }
    let job = Job::new("oomabove");
    let unit = job.unit();
    let slice = LimitedSlice::new(&unit);
    let go = job.cwd().join("go");
    let script = format!(
        "import os, time\nwhile not os.path.exists({go:?}): time.sleep(0.05)\nb = b'x' * (300 << 20)\ntime.sleep(60)\n"
    );
    let started = Command::new("systemd-run")
        .args(["--user", "--quiet", "--service-type=exec"])
        .arg(format!("--unit={unit}"))
        .arg(format!("--slice={}", slice.name))
        .args(["-p", "MemoryMax=1G"])
        .args(["-p", "MemorySwapMax=0", "-p", "OOMPolicy=stop"])
        .args([
            "-p",
            "KillMode=control-group",
            "--",
            "python3",
            "-c",
            &script,
        ])
        .status()
        .unwrap();
    assert!(started.success());
    let limited = Command::new("systemctl")
        .args(["--user", "set-property", "--runtime", &slice.name])
        .args(["MemoryMax=128M", "MemorySwapMax=0"])
        .status()
        .unwrap();
    assert!(limited.success());
    std::fs::write(&go, "").unwrap();
    wait_for("the unit to fail", || {
        (unit_prop(&unit, "ActiveState") == "failed").then_some(())
    });
    assert_eq!(unit_prop(&unit, "Result"), "oom-kill");
    let mut handle = Handle::Systemd {
        unit: unit.clone(),
        cgroup: None,
        invocation: None,
    };
    let sample = SystemdExecutor.sample(&mut handle, 0).unwrap();
    // systemd before 256 keeps no peak to tell this from going over its own limit by.
    let want_oom_killed = keeps_memory_peak();
    assert!(
        sample.oom_killed == want_oom_killed
            && sample.over_memory != want_oom_killed
            && sample.pids.is_empty(),
        "{sample:?} (MemoryPeak {}, MemoryMax {})",
        unit_prop(&unit, "MemoryPeak"),
        unit_prop(&unit, "MemoryMax")
    );
    SystemdExecutor
        .finish(&handle, &job.crash_dir(), job.started)
        .unwrap();
    assert_eq!(unit_prop(&unit, "LoadState"), "not-found");
}

#[test]
fn limits_apply_and_scheduling_is_normal() {
    if !user_manager() {
        return;
    }
    let mut job = Job::start(
        "limits",
        &["sleep", "300"],
        &[PATH],
        Limits {
            mem_max_bytes: 96 << 20,
            cpu_weight: 250,
            tasks_max: 77,
            kill_grace_ms: 10_000,
            log_max_bytes: 0,
        },
    );
    assert_eq!(
        cgroup_file(job.handle(), "memory.max"),
        (96u64 << 20).to_string()
    );
    assert_eq!(cgroup_file(job.handle(), "memory.swap.max"), "0");
    assert_eq!(cgroup_file(job.handle(), "cpu.weight"), "250");
    assert_eq!(cgroup_file(job.handle(), "pids.max"), "77");
    let sample = wait_for("the supervisor and sleep", || {
        let s = job.sample();
        (s.pids.len() == 2).then_some(s)
    });
    // SAFETY: sched_getscheduler and getpriority only read another process's settings.
    let ours = unsafe { libc::sched_getscheduler(0) };
    eprintln!(
        "this test runs with scheduling policy {ours} (SCHED_IDLE is {})",
        libc::SCHED_IDLE
    );
    for &pid in &sample.pids {
        let policy = unsafe { libc::sched_getscheduler(pid as i32) };
        assert_eq!(policy, libc::SCHED_OTHER, "pid {pid}");
        let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, pid) };
        assert_eq!(nice, 0, "pid {pid}");
    }
    job.signal(libc::SIGKILL);
    job.wait_end();
    job.finish();
}

#[test]
fn a_setsid_grandchild_stays_in_the_cgroup_and_dies_on_sigkill() {
    if !user_manager() {
        return;
    }
    // The subshell exits at once, so its child (in a session of its own) is orphaned and adopted
    // by the user manager.
    let script = r#"( setsid sh -c 'echo $$ > grandchild.pid; exec sleep 300' & ); exec sleep 301"#;
    let mut job = Job::start("setsid", &["sh", "-c", script], &[PATH], limits());
    let pidfile = job.cwd().join("grandchild.pid");
    let pid: u32 = wait_for("the grandchild's pid", || {
        std::fs::read_to_string(&pidfile).ok()?.trim().parse().ok()
    });
    // SAFETY: getsid only reads another process's session id.
    assert_eq!(
        unsafe { libc::getsid(pid as i32) },
        pid as i32,
        "own session"
    );
    let sample = wait_for("the grandchild in the sample", || {
        let s = job.sample();
        s.pids.contains(&pid).then_some(s)
    });
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let ppid: u32 = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert!(!sample.pids.contains(&ppid), "reparented outside the job");

    job.signal(libc::SIGKILL);
    job.wait_end();
    wait_for("the grandchild to be gone", || gone(pid).then_some(()));
    // The supervisor was killed too, before it could write a status.
    assert!(job.status().is_none());
    job.finish();
}

#[test]
fn run_limit_sigterm_then_sigkill() {
    if !user_manager() {
        return;
    }
    let mut job = Job::start(
        "runlimit",
        &["sh", "-c", "trap '' TERM; sleep 300; echo after"],
        &[PATH],
        limits(),
    );
    let before = wait_for("supervisor, sh and sleep", || {
        let s = job.sample();
        (s.pids.len() == 3).then_some(s)
    });
    job.signal(libc::SIGTERM);
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(job.sample().pids, before.pids, "SIGTERM is ignored");
    job.signal(libc::SIGKILL);
    job.wait_end();
    // Out of the cgroup once they exit, but in /proc until their parent (or systemd) reaps them.
    wait_for("its processes to be reaped", || {
        before.pids.iter().all(|&p| gone(p)).then_some(())
    });
    assert!(job.status().is_none());
    job.finish();
}

#[test]
fn sigterm_ends_a_plain_command_and_is_recorded() {
    if !user_manager() {
        return;
    }
    let mut job = Job::start("term", &["sleep", "300"], &[PATH], limits());
    wait_for("supervisor and sleep", || {
        (job.sample().pids.len() == 2).then_some(())
    });
    job.signal(libc::SIGTERM);
    job.wait_end();
    let status = wait_for("the status file", || job.status());
    assert_eq!((status.code, status.signal), (None, Some(libc::SIGTERM)));
    job.finish();
}

#[test]
fn a_fresh_executor_adopts_a_running_unit() {
    if !user_manager() {
        return;
    }
    let mut job = Job::start(
        "adopt",
        &["sh", "-c", "sleep 300 & sleep 301; wait"],
        &[PATH],
        limits(),
    );
    let original = job.handle().clone();
    // What a restarted daemon has: the handle from its store, and a new executor.
    let stored = serde_json::to_string(&original).unwrap();
    let mut handle: Handle = serde_json::from_str(&stored).unwrap();
    let mut without_cgroup = handle.clone();
    if let Handle::Systemd { cgroup, .. } = &mut without_cgroup {
        *cgroup = None;
    }
    let exec = potluck::exec::platform();

    let sample = wait_for("four processes", || {
        let s = exec.sample(&mut handle, 0).unwrap();
        (s.pids.len() == 4).then_some(s)
    });
    let found = exec.sample(&mut without_cgroup, 0).unwrap();
    assert_eq!(found.pids, sample.pids);
    assert_eq!(without_cgroup, original, "the cgroup is found again");

    let stacks = exec.stacks(&handle).unwrap();
    println!("{stacks}");
    // The supervisor only waits for its command and copies its output: it is left out, as on
    // macOS, and no debugger stops it.
    let supervisor: u32 = unit_prop(&job.unit(), "MainPID").parse().unwrap();
    assert!(sample.pids.contains(&supervisor), "{:?}", sample.pids);
    assert!(
        stacks.starts_with(&format!("stacks of {}: 3 processes", job.unit())),
        "{stacks}"
    );
    for pid in &sample.pids {
        assert_eq!(
            stacks.contains(&format!("pid {pid} ")),
            *pid != supervisor,
            "{pid}: {stacks}"
        );
    }
    assert!(!stacks.contains("_supervise"), "{stacks}");
    assert!(stacks.contains("cmdline: sleep 301"), "{stacks}");
    assert!(stacks.contains("S (sleeping)"), "{stacks}");

    exec.signal(&handle, libc::SIGTERM).unwrap();
    wait_for("the end", || {
        exec.sample(&mut handle, 0)
            .unwrap()
            .pids
            .is_empty()
            .then_some(())
    });
    let status = wait_for("the status file", || job.status());
    assert_eq!(status.signal, Some(libc::SIGTERM));
    let crash_dir = job.crash_dir();
    exec.finish(&handle, &crash_dir, job.started).unwrap();
    assert_eq!(unit_prop(&job.unit(), "LoadState"), "not-found");
    assert!(exec.stacks(&handle).unwrap().contains("no processes left"));
    job.handle = None;
}

/// After a restart that cut a start short, a fresh executor finds the unit by its name: running,
/// or unloaded after it exited 0 (its status file is there); never started, nothing.
#[test]
fn find_returns_what_a_cut_short_start_left() {
    if !user_manager() {
        return;
    }
    let claimed = now_ms() - 60_000;
    let mut job = Job::start("find", &["sleep", "300"], &[PATH], limits());
    let status = job.dir.path().join("status.json");
    let exec = potluck::exec::platform();
    let found = exec
        .find(&job.name, &status, claimed)
        .unwrap()
        .expect("found");
    assert_eq!(&found, job.handle());
    job.signal(libc::SIGKILL);
    job.wait_end();
    job.finish();

    let mut quick = Job::start("findquick", &["true"], &[PATH], limits());
    let status = quick.dir.path().join("status.json");
    wait_for("the status file", || quick.status());
    wait_for("the unit to unload", || {
        (unit_prop(&quick.unit(), "LoadState") == "not-found").then_some(())
    });
    let found = exec
        .find(&quick.name, &status, claimed)
        .unwrap()
        .expect("found");
    assert_eq!(
        found,
        Handle::Systemd {
            unit: quick.unit(),
            cgroup: None,
            invocation: None,
        }
    );
    // Claimed again after it ended: that status file is an earlier attempt's.
    let later = now_ms() + 1_000;
    assert_eq!(exec.find(&quick.name, &status, later).unwrap(), None);
    quick.finish();

    let never = Job::new("findnever");
    let status = never.dir.path().join("status.json");
    assert_eq!(exec.find(&never.name, &status, claimed).unwrap(), None);
}

#[test]
fn a_unit_that_cannot_start_leaves_nothing() {
    if !user_manager() {
        return;
    }
    let mut job = Job::new("nostart");
    std::fs::remove_dir(job.cwd()).unwrap();
    let err = job
        .try_start(&["true"], &[PATH, ("SECRET", "x")], limits())
        .unwrap_err();
    assert!(format!("{err:#}").contains("could not start"), "{err:#}");
    assert_eq!(unit_prop(&job.unit(), "LoadState"), "not-found");
    assert!(!job.env_file().exists(), "the env file is left behind");
}

/// Environment values never sit on a command line, where every user of the machine can read
/// them (/proc/PID/cmdline is world-readable), nor in the unit's ExecStart: the supervisor reads
/// them from a file only this user can read, and removes it.
#[test]
fn env_values_are_on_no_command_line() {
    if !user_manager() {
        return;
    }
    let secret = format!("potluck-test-secret-{}", std::process::id());
    let mut job = Job::start(
        "secret",
        &["sleep", "300"],
        &[PATH, ("SECRET_TOKEN", &secret)],
        limits(),
    );
    let exec_start = unit_prop(&job.unit(), "ExecStart");
    assert!(exec_start.contains("_supervise"), "{exec_start}");
    assert!(!exec_start.contains(&secret), "{exec_start}");
    let sample = wait_for("the supervisor and sleep", || {
        let s = job.sample();
        (s.pids.len() == 2).then_some(s)
    });
    wait_for("the env file to be read", || {
        (!job.env_file().exists()).then_some(())
    });
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        if let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) {
            let cmdline = String::from_utf8_lossy(&cmdline).replace('\0', " ");
            assert!(!cmdline.contains(&secret), "{:?}: {cmdline}", entry.path());
        }
    }
    // The command has it.
    let sleep = *sample
        .pids
        .iter()
        .find(|p| {
            std::fs::read_to_string(format!("/proc/{p}/comm")).is_ok_and(|c| c.trim() == "sleep")
        })
        .unwrap();
    let environ = std::fs::read(format!("/proc/{sleep}/environ")).unwrap();
    let want = format!("SECRET_TOKEN={secret}");
    assert!(
        environ.split(|&b| b == 0).any(|e| e == want.as_bytes()),
        "{}",
        String::from_utf8_lossy(&environ)
    );
    job.signal(libc::SIGKILL);
    job.wait_end();
    job.finish();
}

/// A supervisor that dies before it reads its env file (here: a program that isn't potluck)
/// leaves the file until finish, which removes it.
#[test]
fn an_unread_env_file_is_removed_by_finish() {
    if !user_manager() {
        return;
    }
    let mut job = Job::new("unread");
    job.supervisor = ["/usr/bin/false", "/bin/false"]
        .into_iter()
        .map(PathBuf::from)
        .find(|p| p.is_file())
        .unwrap();
    job.try_start(&["true"], &[PATH, ("SECRET", "x")], limits())
        .unwrap();
    job.wait_end();
    assert!(job.env_file().exists(), "nothing read the env file");
    job.finish();
    assert!(!job.env_file().exists(), "the env file is left behind");
}

#[test]
fn a_running_unit_name_is_not_taken_over_and_finish_ends_it() {
    if !user_manager() {
        return;
    }
    let mut first = Job::start("name", &["sleep", "300"], &[PATH], limits());
    let mut second = Job::new("name");
    second.name = first.name.clone();
    let err = second.try_start(&["true"], &[PATH], limits()).unwrap_err();
    assert!(format!("{err:#}").contains("already exists"), "{err:#}");
    let pids = first.sample().pids;
    assert_eq!(pids.len(), 2, "the first one runs on");
    // finish ends whatever is still running.
    first.finish();
    wait_for("its processes to be gone", || {
        pids.iter().all(|&p| gone(p)).then_some(())
    });
}

/// What is left of an invocation once its command has ended gets the daemon's kill grace between
/// SIGTERM and SIGKILL (the unit's TimeoutStopSec), not systemd's 10 s.
#[test]
fn leftovers_get_the_kill_grace() {
    if !user_manager() {
        return;
    }
    // The command ends at once, leaving a child that ignores SIGTERM.
    let cmd = [
        "sh",
        "-c",
        "(trap '' TERM; exec sleep 300) & echo $! > leftover.pid",
    ];
    let mut job = Job::start(
        "grace",
        &cmd,
        &[PATH],
        Limits {
            kill_grace_ms: 2_000,
            ..limits()
        },
    );
    let pidfile = job.cwd().join("leftover.pid");
    let pid: u32 = wait_for("the leftover's pid", || {
        std::fs::read_to_string(&pidfile).ok()?.trim().parse().ok()
    });
    wait_for("the status file", || job.status());
    let ended = Instant::now();
    wait_for("the leftover to be killed", || gone(pid).then_some(()));
    let lived = ended.elapsed();
    assert!(
        lived >= Duration::from_millis(1_000) && lived < Duration::from_secs(8),
        "killed {lived:?} after its command ended"
    );
    assert_eq!(unit_prop(&job.unit(), "TimeoutStopUSec"), "2s");
    job.wait_end();
    job.finish();
}

/// A job id, and so its unit name, can come back to a machine (a job queued again after its
/// machine was lost runs there again), and a cleanup that failed is tried again later, with the
/// earlier run's handle: signal and finish with that handle leave the later run alone.
#[test]
fn an_earlier_runs_handle_leaves_a_later_run_of_its_name_alone() {
    if !user_manager() {
        return;
    }
    let mut first = Job::start("again", &["sleep", "300"], &[PATH], limits());
    let old = first.handle().clone();
    first.signal(libc::SIGKILL);
    first.wait_end();
    first.finish();
    let mut second = Job::new("again");
    second.name = first.name.clone();
    second
        .try_start(&["sleep", "300"], &[PATH], limits())
        .unwrap();
    let pids = wait_for("the supervisor and sleep", || {
        let s = second.sample();
        (s.pids.len() == 2).then_some(s.pids)
    });
    SystemdExecutor.signal(&old, libc::SIGKILL).unwrap();
    SystemdExecutor
        .finish(&old, &first.crash_dir(), first.started)
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(second.sample().pids, pids, "the later run was killed");
    assert_eq!(unit_prop(&second.unit(), "ActiveState"), "active");
    second.signal(libc::SIGKILL);
    second.wait_end();
    second.finish();
}

/// Leaves a small core file in /var/lib/systemd/coredump, so it runs only when asked for:
/// `cargo test --test linux_systemd -- --ignored core_dumps`.
#[test]
#[ignore = "leaves a core file behind"]
fn core_dumps_go_to_the_crash_dir() {
    if !user_manager() {
        return;
    }
    let mut job = Job::start("core", &["sh", "-c", "kill -SEGV $$"], &[PATH], limits());
    job.wait_end();
    let status = wait_for("the status file", || job.status());
    assert_eq!(status.signal, Some(libc::SIGSEGV));
    job.finish();
    let text = std::fs::read_to_string(job.crash_dir().join("coredumps.txt")).unwrap();
    println!("{text}");
    assert!(text.contains("Signal: 11"), "{text}");
}

/// Another user's manager can run a unit of the same name (a second potluck on the machine, with
/// the same node name), and the journal holds its core dumps too, readable by a user in `wheel`
/// or `adm`. Here this user sends the journal entry such a dump makes: the job's unit, another
/// uid. It stays out of the job's crash reports. (A real dump records the crashed process's uid,
/// and any user can send an entry naming any uid, which `_UID` then gives away.)
#[test]
fn core_dumps_of_another_users_unit_of_the_same_name_stay_out() {
    if !user_manager() {
        return;
    }
    let mut job = Job::start("coreuid", &["sleep", "300"], &[PATH], limits());
    // SAFETY: getuid has no preconditions.
    let other = unsafe { libc::getuid() } + 1;
    let unit = job.unit();
    let fields = [
        "MESSAGE_ID=fc2e22bc6ee647b6b90729ab34a250b1".to_string(),
        format!("MESSAGE=Process 4242 (sh) of user {other} dumped core."),
        format!("COREDUMP_USER_UNIT={unit}"),
        format!("COREDUMP_UID={other}"),
        format!("COREDUMP_GID={other}"),
        "COREDUMP_PID=4242".to_string(),
        "COREDUMP_SIGNAL=11".to_string(),
        "COREDUMP_COMM=sh".to_string(),
        "COREDUMP_EXE=/usr/bin/sh".to_string(),
    ];
    let entry = fields.join("\n") + "\n";
    let sock = std::os::unix::net::UnixDatagram::unbound().unwrap();
    if let Err(e) = sock.send_to(entry.as_bytes(), "/run/systemd/journal/socket") {
        eprintln!("skipped: can't write to the journal: {e}");
        return;
    }
    let since = format!("--since=@{}", job.started / 1000);
    let listed = || {
        let out = Command::new("coredumpctl")
            .args(["--no-pager", "--no-legend", "list", &since])
            .arg(format!("COREDUMP_USER_UNIT={unit}"))
            .output()
            .ok()?;
        (!String::from_utf8_lossy(&out.stdout).trim().is_empty()).then_some(())
    };
    let start = Instant::now();
    while listed().is_none() {
        if start.elapsed() > Duration::from_secs(20) {
            eprintln!("skipped: coredumpctl doesn't list this user's journal entries");
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    job.signal(libc::SIGKILL);
    job.wait_end();
    job.finish();
    let crash = job.crash_dir().join("coredumps.txt");
    assert!(
        !crash.exists(),
        "another user's dump: {}",
        std::fs::read_to_string(&crash).unwrap_or_default()
    );
}

/// The daemon refuses to start when the user manager doesn't answer it (here: a wrong
/// XDG_RUNTIME_DIR), instead of starting and failing every job with "could not start".
#[test]
fn the_daemon_refuses_to_start_when_the_user_manager_does_not_answer() {
    if !user_manager() {
        return;
    }
    let tmp = tempfile::Builder::new()
        .prefix("nomanager")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let home = tmp.path().join("home");
    let config = tmp.path().join("node.toml");
    std::fs::write(&config, "name = \"test-nomanager\"\n").unwrap();
    let mut daemon = Command::new(POTLUCK)
        .arg("daemon")
        .env("POTLUCK_HOME", &home)
        .env("POTLUCK_CONFIG", &config)
        .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
        .env("XDG_RUNTIME_DIR", tmp.path().join("no-runtime-dir"))
        // systemctl --user would find the manager through the user bus otherwise.
        .env_remove("DBUS_SESSION_BUS_ADDRESS")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while daemon.try_wait().unwrap().is_none() {
        if start.elapsed() > PATIENCE {
            let _ = daemon.kill();
            panic!("the daemon started without a user manager");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let out = daemon.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(
        stderr.contains("the systemd user manager doesn't answer")
            && stderr.contains("no-runtime-dir"),
        "{stderr}"
    );
}

/// Writes an executable script at `path` through a child process, so this (multithreaded) test
/// process never holds it open for writing: another test thread forking meanwhile would leave a
/// copy of that descriptor in its child until the child execs, and running the script in that
/// time fails with ETXTBSY.
fn write_script(path: &Path, text: &str) {
    use std::io::Write;
    let mut child = Command::new("sh")
        .args(["-c", "cat > \"$1\" && chmod 755 \"$1\"", "sh"])
        .arg(path)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(text.as_bytes())
        .unwrap();
    assert!(
        child.wait().unwrap().success(),
        "writing {}",
        path.display()
    );
}

/// Without lingering, systemd stops the user's services, and so the daemon and its jobs, when
/// the user's last session ends: the daemon refuses to start, and `service install` refuses
/// before it writes anything, each saying how to turn lingering on. Lingering is reported off by
/// a `loginctl` earlier in PATH (it is on for this machine's user, and isn't potluck's to change).
#[test]
fn the_daemon_and_service_install_refuse_without_lingering() {
    if !user_manager() {
        return;
    }
    let tmp = tempfile::Builder::new()
        .prefix("nolinger")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let bin = tmp.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    write_script(
        &bin.join("loginctl"),
        "#!/bin/sh\n# Lingering is off; anything else goes to the real loginctl.\nif [ \"$1\" = show-user ]; then echo no; exit 0; fi\nexec /usr/bin/loginctl \"$@\"\n",
    );
    let path = format!("{}:/usr/local/bin:/usr/bin:/bin", bin.display());
    let home = tmp.path().join("home");
    let config = tmp.path().join("node.toml");
    std::fs::write(&config, "name = \"test-nolinger\"\n").unwrap();
    let services = tmp.path().join("services");
    let command = |args: &[&str]| {
        let mut cmd = Command::new(POTLUCK);
        cmd.args(args)
            .env("PATH", &path)
            .env("POTLUCK_HOME", &home)
            .env("POTLUCK_CONFIG", &config)
            .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
            .env("POTLUCK_SERVICE_DIR", &services)
            .env(
                "POTLUCK_SERVICE_NAME",
                format!("potluck-test-nolinger-{}", std::process::id()),
            )
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        cmd
    };
    let user = potluck::service::user_name().unwrap();
    let says_why = |stderr: &str| {
        stderr.contains(&format!("lingering is off for user {user}"))
            && stderr.contains(&format!("sudo loginctl enable-linger {user}"))
    };

    let mut daemon = command(&["daemon"]).spawn().unwrap();
    let start = Instant::now();
    while daemon.try_wait().unwrap().is_none() {
        if start.elapsed() > PATIENCE {
            let _ = daemon.kill();
            let _ = daemon.wait();
            panic!("the daemon started without lingering");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let out = daemon.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(says_why(&stderr), "{stderr}");

    let out = command(&["service", "install"]).output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(says_why(&stderr), "{stderr}");
    assert!(!services.exists(), "service install wrote its unit");
}

/// A job id can come back to a machine (a job queued again after its machine was lost, a state
/// directory started afresh), and with it its unit name: a later run gets only its own core
/// dumps, not those of the earlier run. Leaves a core file, as above:
/// `cargo test --test linux_systemd -- --ignored core_dumps`.
#[test]
#[ignore = "leaves a core file behind"]
fn core_dumps_of_an_earlier_run_of_the_unit_stay_out() {
    if !user_manager() {
        return;
    }
    let mut first = Job::start(
        "coreagain",
        &["sh", "-c", "kill -SEGV $$"],
        &[PATH],
        limits(),
    );
    first.wait_end();
    first.finish();
    let text = std::fs::read_to_string(first.crash_dir().join("coredumps.txt")).unwrap();
    assert!(text.contains("Signal: 11"), "{text}");
    // The same unit name, a second later: it ends without a crash.
    std::thread::sleep(Duration::from_millis(1_100));
    let mut second = Job::new("coreagain");
    second.name = first.name.clone();
    second.try_start(&["true"], &[PATH], limits()).unwrap();
    second.wait_end();
    second.finish();
    let crash = second.crash_dir().join("coredumps.txt");
    assert!(
        !crash.exists(),
        "the earlier run's dump: {}",
        std::fs::read_to_string(&crash).unwrap_or_default()
    );
}
