//! End to end: the real `potluck` binary as daemon and CLI, this platform's real executor
//! (systemd user units on Linux, process groups on macOS), real git repositories (one with a
//! submodule) and real workspaces on a copy-on-write filesystem.
//!
//! Each test runs its own daemon on a home under `CARGO_TARGET_TMPDIR`, on the target directory's
//! filesystem, which must be copy-on-write (APFS on a Mac, btrfs or XFS with reflink on Linux),
//! with a node name starting `test-`, so Linux units are named `potluck-test-...`. The daemon's
//! login shell is a fake one that exports a marker variable, so environment capture is checked
//! without depending on the runner's profile. GPU tests lease the machine's first GPU for jobs
//! that only run `sleep`; they are skipped on a machine without a GPU.

mod common;

use potluck::client::Client;
use potluck::config::Paths;
use potluck::proto::{ClientContext, Request, Response};
use potluck::types::{Facts, JobRecord, JobState};
use serde_json::Value;
use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_potluck");
const BRANCH: &str = "agent-e2e";
const ALIAS: &str = "e2egpu";

// ---------------------------------------------------------------------------------------------
// Helpers that run as job commands (the test binary itself), doing nothing in a normal test run.

/// Allocates `E2E_ALLOC_MB` MiB of non-zero bytes (zero pages would not count) and holds them.
#[test]
fn helper_alloc() {
    let Ok(mb) = std::env::var("E2E_ALLOC_MB") else {
        return;
    };
    let mb: usize = mb.parse().unwrap();
    let mut v: Vec<u8> = Vec::with_capacity(mb << 20);
    for i in 0..(mb << 20) {
        v.push((i % 251) as u8 | 1);
    }
    println!(
        "allocated {mb} MiB, sum {}",
        v.iter().step_by(4096).map(|&b| b as u64).sum::<u64>()
    );
    std::thread::sleep(Duration::from_secs(60));
    drop(v);
}

/// Leaves the job's session and ignores SIGTERM, then sleeps: a grandchild only SIGKILL ends.
#[test]
fn helper_setsid() {
    let Ok(pidfile) = std::env::var("E2E_PIDFILE") else {
        return;
    };
    unsafe {
        libc::setsid();
        libc::signal(libc::SIGTERM, libc::SIG_IGN);
    }
    std::fs::write(&pidfile, std::process::id().to_string()).unwrap();
    std::thread::sleep(Duration::from_secs(120));
}

/// Starts a daemon the way `daemon(3)` does, each step a fresh exec of this binary: the helper
/// starts a middle process and waits for it; the middle calls setsid, starts the daemon and exits
/// at once, so the daemon's parent is launchd (init on Linux). The daemon writes its pid to
/// `E2E_PIDFILE` and sleeps for 120 seconds.
#[test]
fn helper_daemon() {
    let Ok(pidfile) = std::env::var("E2E_PIDFILE") else {
        return;
    };
    let me = std::env::current_exe().unwrap();
    let again = |stage: &str| {
        let mut c = Command::new(&me);
        c.args([
            "--exact",
            "helper_daemon",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("E2E_DAEMON_STAGE", stage)
        .stdin(Stdio::null());
        c
    };
    match std::env::var("E2E_DAEMON_STAGE").as_deref() {
        Err(_) => {
            again("middle").status().unwrap();
        }
        Ok("middle") => {
            unsafe { libc::setsid() };
            again("daemon").spawn().unwrap();
            std::process::exit(0);
        }
        Ok(_) => {
            std::fs::write(&pidfile, std::process::id().to_string()).unwrap();
            std::thread::sleep(Duration::from_secs(120));
        }
    }
}

fn helper_cmd(name: &str) -> String {
    format!(
        "{} --exact {name} --nocapture --test-threads=1",
        std::env::current_exe().unwrap().display()
    )
}

// ---------------------------------------------------------------------------------------------
// Fixture

/// Paths with no potluck state behind them, for detecting this machine's real facts.
fn nowhere() -> Paths {
    Paths {
        home: PathBuf::from("/nonexistent"),
        config: PathBuf::from("/nonexistent"),
    }
}

/// This machine's first GPU (by the real facts), if it has one.
fn first_gpu() -> Option<String> {
    static GPU: OnceLock<Option<String>> = OnceLock::new();
    GPU.get_or_init(|| {
        let facts: Facts = potluck::facts::detect(&nowhere(), &Default::default()).ok()?;
        let nvidia = facts.gpus.iter().find(|g| g.vendor == "10de");
        nvidia.or(facts.gpus.first()).map(|g| g.id.clone())
    })
    .clone()
}

/// The fake login shell, written once before any daemon starts (exec of a file another thread
/// is still writing fails with ETXTBSY on Linux).
fn fake_shell() -> PathBuf {
    static SHELL: OnceLock<PathBuf> = OnceLock::new();
    SHELL
        .get_or_init(|| {
            let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("e2e-shell");
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join(format!("login-{}.sh", std::process::id()));
            std::fs::write(
                &path,
                "#!/bin/sh\n# A login shell: potluck runs it as `SHELL -l -c CMD`. Run by a name ending in -slow, it takes 2 s.\n[ \"$1\" = -l ] || exit 64\ncase \"$0\" in *-slow) sleep 2 ;; esac\nexport E2E_LOGIN=from-login-profile\nexport E2E_OVERRIDE=login\nshift 2\nexec /bin/sh -c \"$1\"\n",
            )
            .unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path
        })
        .clone()
}

/// The fake login shell by a name that makes it take 2 s (a symlink: no file is written that a
/// daemon then runs).
fn slow_shell() -> PathBuf {
    static SHELL: OnceLock<PathBuf> = OnceLock::new();
    SHELL
        .get_or_init(|| {
            let shell = fake_shell();
            let path = shell.with_file_name(format!("login-{}-slow", std::process::id()));
            let _ = std::fs::remove_file(&path);
            std::os::unix::fs::symlink(&shell, &path).unwrap();
            path
        })
        .clone()
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args([
            "-c",
            "user.name=e2e",
            "-c",
            "user.email=e2e@example.invalid",
            "-c",
            "protocol.file.allow=always",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "init.defaultBranch=main",
        ])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

struct Opts {
    budget_zero: bool,
    /// The mesh's port, for tests that pair daemons (a daemon in no cluster doesn't listen).
    port: u16,
}

struct Node {
    dir: tempfile::TempDir,
    paths: Paths,
    name: String,
    proj: PathBuf,
    daemon: Option<Child>,
    /// The binary the daemon runs (BIN unless a test put another in its place).
    bin: PathBuf,
    /// More environment for the daemon.
    daemon_env: Vec<(String, String)>,
    /// Where the daemon's stdout and stderr go.
    daemon_out: PathBuf,
}

/// What a CLI run printed.
struct Out {
    code: i32,
    json: Value,
    stderr: String,
}

impl Node {
    fn new() -> Node {
        Node::with(Opts {
            budget_zero: false,
            port: 0,
        })
    }

    fn with(opts: Opts) -> Node {
        static N: AtomicU32 = AtomicU32::new(0);
        let shell = fake_shell();
        let dir = tempfile::Builder::new()
            .prefix("e2e")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .unwrap();
        let name = format!(
            "test-{}-{}",
            std::process::id() % 100_000,
            N.fetch_add(1, Ordering::SeqCst)
        );
        let paths = Paths {
            home: dir.path().join("home"),
            config: dir.path().join("node.toml"),
        };
        // Members only on this machine.
        let mut config = format!(
            "name = \"{name}\"\nram_gib = 8\nkill_grace_s = 1\nstall_minutes = 1\nlisten = \"127.0.0.1\"\nport = {}\naddress = \"127.0.0.1\"\n",
            opts.port
        );
        if opts.budget_zero {
            config.push_str("workspace_budget_percent = 0\nworkspace_budget_min_gib = 0\n");
        }
        if let Some(gpu) = first_gpu() {
            config.push_str(&format!(
                "\n[[gpu]]\nmatch = \"{gpu}\"\nalias = \"{ALIAS}\"\nenv = {{ E2E_GPU = \"held\" }}\n"
            ));
        }
        std::fs::write(&paths.config, config).unwrap();

        // Repositories: lib, and proj with lib as a submodule and a committed potluck.toml.
        let repos = dir.path().join("repos");
        let lib = repos.join("lib");
        let proj = repos.join("proj");
        std::fs::create_dir_all(&lib).unwrap();
        std::fs::create_dir_all(&proj).unwrap();
        git(&lib, &["init", "-q"]);
        std::fs::write(lib.join("lib.txt"), "lib v1\n").unwrap();
        git(&lib, &["add", "."]);
        git(&lib, &["commit", "-qm", "lib"]);
        git(&proj, &["init", "-q"]);
        std::fs::write(proj.join("README"), "hello\n").unwrap();
        std::fs::write(
            proj.join("potluck.toml"),
            "[env]\nFROM_TOML = \"toml\"\nE2E_OVERRIDE = \"toml\"\n\n[recipe.hello]\ncmd = [\"sh\", \"-c\", \"echo recipe $FROM_TOML; cat vendor/lib/lib.txt\"]\ntimeout = \"1m\"\nram = \"1G\"\n",
        )
        .unwrap();
        git(
            &proj,
            &[
                "submodule",
                "add",
                "-q",
                lib.to_str().unwrap(),
                "vendor/lib",
            ],
        );
        git(&proj, &["add", "."]);
        git(&proj, &["commit", "-qm", "init"]);
        git(&proj, &["checkout", "-q", "-b", BRANCH]);

        let daemon_out = dir.path().join("daemon.out");
        let mut node = Node {
            dir,
            paths,
            name,
            proj,
            daemon: None,
            bin: PathBuf::from(BIN),
            daemon_env: Vec::new(),
            daemon_out,
        };
        let _ = shell;
        node.start_daemon();
        node
    }

    fn start_daemon(&mut self) {
        let log = File::options()
            .create(true)
            .append(true)
            .open(&self.daemon_out)
            .unwrap();
        self.start_daemon_with(log.try_clone().unwrap().into(), log.into());
    }

    fn start_daemon_with(&mut self, stdout: Stdio, stderr: Stdio) {
        assert!(self.daemon.is_none());
        let child = Command::new(&self.bin)
            .arg("daemon")
            .env("POTLUCK_HOME", &self.paths.home)
            .env("POTLUCK_CONFIG", &self.paths.config)
            .env("SHELL", fake_shell())
            // Set in the daemon's environment and the CLI's: no job may see it.
            .env("E2E_CALLER", "leaked")
            .envs(self.daemon_env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap();
        self.daemon = Some(child);
        let start = Instant::now();
        loop {
            if let Ok(mut c) = Client::connect(&self.paths, ClientContext::default())
                && c.call(Request::Ping).is_ok()
            {
                return;
            }
            if let Some(status) = self.daemon.as_mut().unwrap().try_wait().unwrap() {
                panic!("the daemon exited ({status}): {}", self.daemon_log());
            }
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "the daemon never answered: {}",
                self.daemon_log()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Stops the daemon (SIGTERM, as a service manager would). Jobs keep running.
    fn stop_daemon(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
            let start = Instant::now();
            while child.try_wait().unwrap().is_none() {
                if start.elapsed() > Duration::from_secs(10) {
                    let _ = child.kill();
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }

    fn daemon_log(&self) -> String {
        std::fs::read_to_string(&self.daemon_out).unwrap_or_default()
    }

    /// Runs the CLI with `--json` in `cwd` (the project by default).
    fn cli_in(&self, cwd: &Path, args: &[&str]) -> Out {
        let out = Command::new(BIN)
            .arg("--json")
            .args(args)
            .current_dir(cwd)
            .env("POTLUCK_HOME", &self.paths.home)
            .env("POTLUCK_CONFIG", &self.paths.config)
            .env("E2E_CALLER", "leaked")
            .env_remove("POTLUCK_JOB")
            .env_remove("POTLUCK_LEASE")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let json = stdout
            .lines()
            .rev()
            .find_map(|l| serde_json::from_str(l).ok())
            .unwrap_or(Value::Null);
        Out {
            code: out.status.code().unwrap_or(-1),
            json,
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        }
    }

    fn cli(&self, args: &[&str]) -> Out {
        self.cli_in(&self.proj, args)
    }

    fn cli_ok(&self, args: &[&str]) -> Value {
        let o = self.cli(args);
        assert_eq!(
            o.code,
            0,
            "potluck {args:?}: {} {}\n{}",
            o.json,
            o.stderr,
            self.daemon_log()
        );
        o.json
    }

    /// `potluck dispatch ARGS` → the job id.
    fn dispatch(&self, args: &[&str]) -> String {
        let mut all = vec!["dispatch"];
        all.extend_from_slice(args);
        let j = self.cli_ok(&all);
        j["job"].as_str().unwrap().to_string()
    }

    fn client(&self) -> Client {
        Client::connect(
            &self.paths,
            ClientContext {
                repo_id: potluck::client::repo_of(&self.proj).map(|r| r.0),
                branch: Some(BRANCH.into()),
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn get(&self, job: &str) -> JobRecord {
        match self
            .client()
            .call(Request::Get { job: job.into() })
            .unwrap()
        {
            Response::Job { job } => *job,
            other => panic!("{other:?}"),
        }
    }

    fn wait_for(
        &self,
        job: &str,
        what: &str,
        secs: u64,
        pred: impl Fn(&JobRecord) -> bool,
    ) -> JobRecord {
        let start = Instant::now();
        loop {
            let j = self.get(job);
            if pred(&j) {
                return j;
            }
            assert!(
                start.elapsed() < Duration::from_secs(secs),
                "{job}: never {what}: {j:#?}\n{}",
                self.daemon_log()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn running(&self, job: &str) -> JobRecord {
        self.wait_for(job, "running", 60, |j| j.state == JobState::Running)
    }

    /// `potluck wait JOB` → (exit code, its JSON, the output it streamed).
    fn wait(&self, job: &str) -> (i32, Value, String) {
        let o = self.cli(&["wait", job]);
        (o.code, o.json, o.stderr)
    }

    fn output(&self, job: &str) -> String {
        let j = self.cli_ok(&["logs", job]);
        j["output"].as_str().unwrap_or_default().to_string()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Cancel whatever still runs, so nothing outlives the test.
        if self.daemon.is_some()
            && let Ok(mut c) = Client::connect(&self.paths, ClientContext::default())
            && let Ok(Response::Status { status }) = c.call(Request::Status)
        {
            let active: Vec<String> = status
                .running
                .iter()
                .chain(status.queued.iter())
                .map(|j| j.id.clone())
                .collect();
            for id in &active {
                let _ = c.call(Request::Cancel { job: id.clone() });
            }
            for id in &active {
                let _ = c.call(Request::Wait {
                    job: id.clone(),
                    timeout_ms: Some(20_000),
                });
            }
        }
        self.stop_daemon();
    }
}

fn alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

// ---------------------------------------------------------------------------------------------
// Tests

/// CPU time `pid` has used so far, in milliseconds.
#[cfg(target_os = "linux")]
fn cpu_ms(pid: u32) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    // The fields after the command name, which ends at the last ')': utime and stime are the
    // 14th and 15th fields of the line, the 12th and 13th after it.
    let rest = &stat[stat.rfind(')').unwrap() + 2..];
    let fields: Vec<&str> = rest.split(' ').collect();
    let ticks: u64 = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    let per_s = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
    ticks * 1000 / per_s
}

/// CPU time `pid` has used so far, in milliseconds (`ps` prints [[dd-]hh:]mm:ss.cc here).
#[cfg(target_os = "macos")]
fn cpu_ms(pid: u32) -> u64 {
    let out = Command::new("ps")
        .args(["-o", "time=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let (days, rest) = match text.split_once('-') {
        Some((d, r)) => (d.parse::<f64>().unwrap(), r.to_string()),
        None => (0.0, text.clone()),
    };
    let secs = rest.split(':').fold(0.0, |total, part| {
        total * 60.0 + part.parse::<f64>().unwrap()
    });
    ((days * 86400.0 + secs) * 1000.0) as u64
}

/// The cluster loop sleeps between ticks: a daemon with nothing to do uses almost no CPU. (Its
/// loop once woke itself on every tick and kept a core busy.)
#[test]
fn an_idle_daemon_uses_almost_no_cpu() {
    let node = Node::new();
    let pid = node.daemon.as_ref().unwrap().id();
    std::thread::sleep(Duration::from_secs(1));
    let before = cpu_ms(pid);
    std::thread::sleep(Duration::from_secs(4));
    let used = cpu_ms(pid) - before;
    assert!(
        used < 800,
        "an idle daemon used {used} ms of CPU in 4 s: {}",
        node.daemon_log()
    );
}

#[test]
fn dispatch_wait_logs_env_status_why_cancel() {
    let n = Node::new();
    let job = n.dispatch(&[
        "--timeout",
        "1m",
        "--env",
        "E2E_OVERRIDE=flag",
        "--",
        "sh",
        "-c",
        "echo out; echo err >&2; echo \"LOGIN=$E2E_LOGIN OVERRIDE=$E2E_OVERRIDE TOML=$FROM_TOML CALLER=${E2E_CALLER-unset} CARGO=${CARGO_PKG_NAME-unset} RUSTFLAGS=${RUSTFLAGS-unset} JOB=$POTLUCK_JOB WS=$POTLUCK_WORKSPACE NODE=$POTLUCK_NODE TMP=$TMPDIR\"; pwd; exit 3",
    ]);
    assert!(job.starts_with(&n.name), "{job}");
    let (code, j, streamed) = n.wait(&job);
    assert_eq!(code, 3, "{j}");
    assert_eq!(j["state"], "failed");
    assert_eq!(j["exit_code"], 3);
    assert!(
        streamed.contains("out\n") && streamed.contains("err\n"),
        "{streamed}"
    );

    // The environment: the login shell's, then potluck.toml's, then --env, then potluck's own;
    // nothing from the caller or the daemon's own environment.
    let out = n.output(&job);
    let ws = j["workspace"].as_str().unwrap().to_string();
    for want in [
        "LOGIN=from-login-profile".to_string(),
        "OVERRIDE=flag".into(),
        "TOML=toml".into(),
        "CALLER=unset".into(),
        "CARGO=unset".into(),
        "RUSTFLAGS=unset".into(),
        format!("JOB={job}"),
        format!("WS={ws}"),
        format!("NODE={}", n.name),
        format!("TMP={}", n.paths.workspace_tmp(&ws).display()),
    ] {
        assert!(out.contains(&want), "{want} in {out}");
    }
    assert!(
        out.contains(&n.paths.workspace(&ws).display().to_string()),
        "{out}"
    );

    // status and why, as JSON.
    let big = n.dispatch(&["--timeout", "1m", "--ram", "8G", "--", "sleep", "60"]);
    n.running(&big);
    let queued = n.dispatch(&["--timeout", "1m", "--ram", "1G", "--", "true"]);
    let s = n.cli_ok(&["status"]);
    let ids = |k: &str| -> Vec<String> {
        s[k].as_array()
            .unwrap()
            .iter()
            .map(|j| j["id"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(ids("running"), std::slice::from_ref(&big));
    assert_eq!(ids("queued"), std::slice::from_ref(&queued));
    assert_eq!(s["nodes"][0]["ram_reserved_mib"], 8192);
    let w = n.cli_ok(&["why", &queued]);
    let lines = w["lines"].to_string();
    assert!(lines.contains("RAM"), "{w}");
    assert!(lines.contains(&big), "names the holder: {w}");

    // Cancel: queued at once, running by signal.
    let c = n.cli_ok(&["cancel", &queued]);
    assert_eq!(c["record"]["state"], "cancelled");
    assert_eq!(n.wait(&queued).0, 130);
    n.cli_ok(&["cancel", &big]);
    let (code, j, _) = n.wait(&big);
    assert_eq!((code, j["state"].as_str()), (130, Some("cancelled")));
}

#[test]
fn workspaces_seed_reuse_cp_hold_idle_remove_release() {
    let n = Node::with(Opts {
        budget_zero: true,
        port: 0,
    });
    // Seeded with the submodule at its commit.
    let j1 = n.dispatch(&[
        "--timeout",
        "1m",
        "--",
        "sh",
        "-c",
        "cat vendor/lib/lib.txt; echo made > made.txt; git status --porcelain --ignore-submodules=none | grep -v made.txt; true",
    ]);
    let (code, j, out) = n.wait(&j1);
    assert_eq!(code, 0, "{j} {out}");
    assert!(out.contains("lib v1"), "{out}");
    assert!(
        !out.contains(" M ") && !out.contains("??"),
        "a clean tree: {out}"
    );
    let w1 = j["workspace"].as_str().unwrap().to_string();

    // Reminders on every command, for this branch.
    let s = n.cli_ok(&["status"]);
    let rem = s["reminders"].as_array().unwrap();
    assert_eq!(rem.len(), 1, "{s}");
    assert_eq!(rem[0]["workspace"], w1.as_str());
    assert_eq!(rem[0]["state"], "held");

    // cp: a directory (the submodule) and a file the job made.
    let dest = n.dir.path().join("out");
    n.cli_ok(&[
        "cp",
        &format!("{w1}:vendor/lib"),
        dest.join("lib").to_str().unwrap(),
    ]);
    assert_eq!(
        std::fs::read_to_string(dest.join("lib/lib.txt")).unwrap(),
        "lib v1\n"
    );
    n.cli_ok(&[
        "cp",
        &format!("{w1}:made.txt"),
        dest.join("made.txt").to_str().unwrap(),
    ]);
    assert_eq!(
        std::fs::read_to_string(dest.join("made.txt")).unwrap(),
        "made\n"
    );
    assert_ne!(
        n.cli(&["cp", &format!("{w1}:../../potluck.db"), "x"]).code,
        0
    );

    // Another invocation in the same workspace sees the first one's files.
    let j2 = n.dispatch(&[
        "--timeout",
        "1m",
        "--workspace",
        &w1,
        "--",
        "cat",
        "made.txt",
    ]);
    let (code, j, out) = n.wait(&j2);
    assert_eq!((code, j["workspace"].as_str()), (0, Some(w1.as_str())));
    assert!(out.contains("made"), "{out}");

    // Hold: DUR from now.
    let h = n.cli_ok(&["hold", &w1, "2h"]);
    let left = h["held_until"].as_u64().unwrap() - potluck::types::now_ms();
    assert!(left > 7_190_000 && left <= 7_200_000, "{left}");

    // A second workspace, then both holds end: idle; over the (zero) budget the older idle one is
    // removed and the newest of the repository kept.
    let j3 = n.dispatch(&["--timeout", "1m", "--", "true"]);
    let w2 = n.wait(&j3).1["workspace"].as_str().unwrap().to_string();
    assert_ne!(w1, w2);
    n.cli_ok(&["hold", &w1, "1s"]);
    n.cli_ok(&["hold", &w2, "1s"]);
    let start = Instant::now();
    loop {
        let s = n.cli_ok(&["status"]);
        let ws: Vec<(String, String)> = s["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| {
                (
                    w["id"].as_str().unwrap().to_string(),
                    w["state"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        if ws == [(w2.clone(), "idle".to_string())] {
            assert!(
                s["reminders"].as_array().unwrap().is_empty(),
                "idle: no reminders"
            );
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(30), "{ws:?}");
        std::thread::sleep(Duration::from_millis(200));
    }
    // Removal itself runs off the loop.
    let start = Instant::now();
    while n.paths.workspace(&w1).exists() || n.paths.workspace_tmp(&w1).exists() {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "{w1} not removed"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // Release: the workspace goes now.
    let r = n.cli_ok(&["release", &w2]);
    assert_eq!(r["record"]["state"], "removed");
    assert!(!n.paths.workspace(&w2).exists());
    let gone = n.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        &w2,
        "--",
        "true",
    ]);
    assert_ne!(gone.code, 0);
    assert!(
        gone.json["error"].as_str().unwrap().contains("is gone"),
        "{}",
        gone.json
    );
}

#[test]
fn ram_limit_ends_the_job() {
    let n = Node::new();
    let job = n.dispatch(&[
        "--timeout",
        "2m",
        "--ram",
        "256M",
        "--env",
        "E2E_ALLOC_MB=700",
        "--",
        "sh",
        "-c",
        &helper_cmd("helper_alloc"),
    ]);
    let (code, j, _) = n.wait(&job);
    assert_eq!(code, 137, "{j}\n{}", n.daemon_log());
    assert_eq!(j["state"], "over_memory");
    let msg = j["message"].as_str().unwrap();
    assert!(
        msg.starts_with("over memory: used ") && msg.ends_with(" MiB of its 256 MiB"),
        "{msg}"
    );
}

#[test]
fn run_limit_terms_then_kills_a_setsid_grandchild() {
    let n = Node::new();
    let pidfile = n.dir.path().join("grandchild.pid");
    let job = n.dispatch(&[
        "--timeout",
        "3s",
        "--env",
        &format!("E2E_PIDFILE={}", pidfile.display()),
        "--",
        "sh",
        "-c",
        &format!("{} & sleep 60", helper_cmd("helper_setsid")),
    ]);
    let rec = n.running(&job);
    let start = Instant::now();
    while !pidfile.exists() {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "{}",
            n.output(&job)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let pid: i32 = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(alive(pid));
    let (code, j, _) = n.wait(&job);
    assert_eq!(code, 124, "{j}");
    assert_eq!(j["message"], "killed at its run limit (3s)");
    let ended = n.get(&job);
    let ran = ended.ended_at.unwrap() - rec.started_at.unwrap();
    assert!(ran >= 3_000 + 1_000, "SIGKILL after the 1s grace: {ran}");
    assert!(ended.stuck_pids.is_empty(), "{:?}", ended.stuck_pids);
    let start = Instant::now();
    while alive(pid) {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "grandchild {pid} survived"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A daemon a job starts by a double fork ends with the job: on Linux it stays in the job's unit;
/// on macOS it is found by the POTLUCK_LEASE in its environment, though no sample saw it leave.
#[test]
fn a_daemon_the_job_starts_ends_with_it() {
    let n = Node::new();
    let pidfile = n.dir.path().join("daemon.pid");
    let job = n.dispatch(&[
        "--timeout",
        "1m",
        "--env",
        &format!("E2E_PIDFILE={}", pidfile.display()),
        "--",
        "sh",
        "-c",
        &format!(
            "{} >/dev/null; while [ ! -s {} ]; do sleep 0.1; done; echo daemon started",
            helper_cmd("helper_daemon"),
            pidfile.display()
        ),
    ]);
    let (code, j, _) = n.wait(&job);
    let pid: i32 = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let start = Instant::now();
    while alive(pid) && start.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(50));
    }
    let survived = alive(pid);
    if survived {
        // Ours: the pid it wrote, still running the helper.
        let out = Command::new("ps")
            .args(["-o", "command=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&out.stdout).contains("helper_daemon") {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }
    assert_eq!(code, 0, "{j}");
    assert!(!survived, "the daemon {pid} outlived its job");
}

/// A supervisor killed while its command runs (SIGKILL from outside) ends the job at once, with
/// the processes it left: on Linux systemd stops the unit; on macOS the daemon does it.
#[test]
fn a_killed_supervisor_ends_its_job() {
    let n = Node::new();
    let pidfile = n.dir.path().join("pids");
    let job = n.dispatch(&[
        "--timeout",
        "2m",
        "--env",
        &format!("E2E_PIDFILE={}", pidfile.display()),
        "--",
        "sh",
        "-c",
        "echo $PPID $$ > \"$E2E_PIDFILE.tmp\"; mv \"$E2E_PIDFILE.tmp\" \"$E2E_PIDFILE\"; exec sleep 100",
    ]);
    n.running(&job);
    let start = Instant::now();
    let pids = loop {
        if let Ok(text) = std::fs::read_to_string(&pidfile) {
            break text;
        }
        assert!(start.elapsed() < Duration::from_secs(30), "no pid file");
        std::thread::sleep(Duration::from_millis(50));
    };
    let pids: Vec<i32> = pids
        .split_whitespace()
        .map(|p| p.parse().unwrap())
        .collect();
    let (supervisor, sleep) = (pids[0], pids[1]);
    assert!(alive(supervisor) && alive(sleep));
    // SAFETY: the job's own supervisor, as the job reported it.
    unsafe { libc::kill(supervisor, libc::SIGKILL) };
    let killed = Instant::now();
    let (code, j, _) = n.wait(&job);
    assert_eq!(code, 1, "{j}");
    assert_eq!(j["state"], "failed", "{j}");
    assert!(killed.elapsed() < Duration::from_secs(30), "{j}");
    let start = Instant::now();
    while alive(sleep) {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{sleep} survived"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// With `log_max_mib = 1`, a job writing 20 MiB keeps its first MiB and its last 16 MiB, with
/// a line between them saying how much was dropped; `logs --json` returns it all.
#[test]
fn a_log_keeps_its_first_mib_and_its_last_16() {
    let mut n = Node::new();
    n.stop_daemon();
    let mut config = std::fs::read_to_string(&n.paths.config).unwrap();
    config = config.replacen("name =", "log_max_mib = 1\nname =", 1);
    std::fs::write(&n.paths.config, config).unwrap();
    n.start_daemon();
    let job = n.dispatch(&[
        "--timeout",
        "2m",
        "--",
        "sh",
        "-c",
        "head -c 20971520 /dev/zero | tr '\\0' x; echo; echo the end",
    ]);
    let (code, j, _) = n.wait(&job);
    assert_eq!(code, 0, "{j}");
    let out = n.output(&job);
    let mib = 1 << 20;
    assert!(out[..mib].bytes().all(|b| b == b'x'));
    let rest = &out[mib..];
    let at_limit = "\n--- potluck: the output reached log_max_mib (1 MiB): what follows is dropped, \
                    except its last 16 MiB, added when the command ends ---\n";
    assert!(rest.starts_with(at_limit), "{}", &rest[..300]);
    let rest = &rest[at_limit.len()..];
    let total = 20 * mib + "\nthe end\n".len();
    let end = format!(
        "--- potluck: {} bytes of output dropped after the log reached log_max_mib (1 MiB); the \
         last {} bytes follow ---\n",
        total - mib - 16 * mib,
        16 * mib
    );
    assert!(rest.starts_with(&end), "{}", &rest[..300]);
    let tail = &rest[end.len()..];
    assert_eq!(tail.len(), 16 * mib);
    assert!(tail.ends_with("xxx\nthe end\n"));
}

#[test]
fn stalls_are_sampled_into_the_log() {
    let n = Node::new();
    let job = n.dispatch(&[
        "--timeout",
        "5m",
        "--",
        "sh",
        "-c",
        "echo started; sleep 600",
    ]);
    n.running(&job);
    let rec = n.wait_for(&job, "stalled", 150, |j| j.stalled);
    assert_eq!(rec.state, JobState::Running, "only marked, not ended");
    let start = Instant::now();
    loop {
        let out = n.output(&job);
        if out.contains("stacks of its processes") && out.contains("end of stacks") {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(90), "{out}");
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        n.cli_ok(&["why", &job])["lines"]
            .to_string()
            .contains("stall")
    );
    n.cli_ok(&["cancel", &job]);
    assert_eq!(n.wait(&job).0, 130);
}

#[test]
fn gpu_lease_serializes() {
    let Some(gpu) = first_gpu() else {
        eprintln!("no GPU on this machine: skipped");
        return;
    };
    let n = Node::new();
    // A holds the GPU until the test lets it end (a fixed sleep would race with the dispatch of
    // B, which takes seconds in a slow run).
    let flag = n.dir.path().join("a-may-end");
    let a = n.dispatch(&[
        "--timeout",
        "1m",
        "--gpu",
        ALIAS,
        "--env",
        &format!("E2E_FLAG={}", flag.display()),
        "--",
        "sh",
        "-c",
        "echo gpu=$E2E_GPU; while [ ! -e \"$E2E_FLAG\" ]; do sleep 0.1; done",
    ]);
    let b = n.dispatch(&["--timeout", "1m", "--gpu", ALIAS, "--", "sleep", "1"]);
    let ra = n.running(&a);
    assert_eq!(
        ra.lease.as_ref().unwrap().grant.gpus,
        std::slice::from_ref(&gpu)
    );
    assert_eq!(n.get(&b).state, JobState::Queued, "one lease per GPU");
    std::fs::write(&flag, "").unwrap();
    assert_eq!(n.wait(&a).0, 0);
    assert_eq!(n.wait(&b).0, 0);
    let (ra, rb) = (n.get(&a), n.get(&b));
    assert!(
        rb.started_at.unwrap() >= ra.ended_at.unwrap(),
        "{ra:?} {rb:?}"
    );
    assert!(n.output(&a).contains("gpu=held"));
}

#[test]
fn exclusive_waits_for_an_empty_machine() {
    let n = Node::new();
    // A runs until the test lets it end: a fixed sleep would race with the dispatches below,
    // which take seconds in a slow run.
    let flag = n.dir.path().join("a-may-end");
    let a = n.dispatch(&[
        "--timeout",
        "1m",
        "--ram",
        "1G",
        "--env",
        &format!("E2E_FLAG={}", flag.display()),
        "--",
        "sh",
        "-c",
        "while [ ! -e \"$E2E_FLAG\" ]; do sleep 0.1; done",
    ]);
    n.running(&a);
    let b = n.dispatch(&[
        "--timeout",
        "1m",
        "--ram",
        "1G",
        "--exclusive",
        "--",
        "sleep",
        "1",
    ]);
    // Would fit by RAM, but the exclusive job waits for the machine and it ends after A's limit.
    let c = n.dispatch(&["--timeout", "10m", "--ram", "1G", "--", "true"]);
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(n.get(&b).state, JobState::Queued);
    assert_eq!(n.get(&c).state, JobState::Queued);
    std::fs::write(&flag, "").unwrap();
    for j in [&a, &b, &c] {
        assert_eq!(n.wait(j).0, 0);
    }
    let (ra, rb, rc) = (n.get(&a), n.get(&b), n.get(&c));
    assert!(rb.started_at >= ra.ended_at, "{ra:?} {rb:?}");
    assert!(rc.started_at >= rb.ended_at, "{rb:?} {rc:?}");
    let grant = &rb.lease.as_ref().unwrap().grant;
    assert!(grant.exclusive);
    if let Some(gpu) = first_gpu() {
        assert!(grant.gpus.contains(&gpu), "{grant:?}");
    }
}

#[test]
fn priority_aging_and_backfill() {
    let n = Node::new();
    // 8 GiB capacity: A takes 6, until the test lets it end (a fixed sleep would race with the
    // dispatches below, which take seconds in a slow run).
    let flag = n.dir.path().join("a-may-end");
    let a = n.dispatch(&[
        "--timeout",
        "5m",
        "--ram",
        "6G",
        "--env",
        &format!("E2E_FLAG={}", flag.display()),
        "--",
        "sh",
        "-c",
        "while [ ! -e \"$E2E_FLAG\" ]; do sleep 0.1; done",
    ]);
    n.running(&a);
    let low = n.dispatch(&[
        "--timeout",
        "1m",
        "--ram",
        "5G",
        "--priority",
        "1",
        "--",
        "sleep",
        "1",
    ]);
    let high = n.dispatch(&[
        "--timeout",
        "1m",
        "--ram",
        "5G",
        "--priority",
        "5",
        "--",
        "sleep",
        "1",
    ]);
    // Small and short: ends by its run limit before A's resources are due, so it backfills.
    let small = n.dispatch(&["--timeout", "20s", "--ram", "1G", "--", "sleep", "1"]);
    n.running(&small);
    assert_eq!(n.get(&a).state, JobState::Running, "started beside A");
    assert_eq!(n.get(&high).state, JobState::Queued);
    std::fs::write(&flag, "").unwrap();
    for j in [&a, &low, &high, &small] {
        assert_eq!(n.wait(j).0, 0, "{j}");
    }
    let (rl, rh) = (n.get(&low), n.get(&high));
    assert!(
        rh.started_at < rl.started_at,
        "priority 5 first: {rh:?} {rl:?}"
    );
    assert!(rl.passes >= 1, "the low one was passed over: {}", rl.passes);
}

#[test]
fn nested_dispatch_runs_in_place_within_the_lease() {
    let n = Node::new();
    let script = format!(
        "{BIN} dispatch --timeout 10s --ram 1G -- echo nested-in-place; echo rc=$?; \
         {BIN} dispatch --timeout 10s --ram 4G -- echo too-big; echo rc=$?"
    );
    let job = n.dispatch(&["--timeout", "1m", "--ram", "2G", "--", "sh", "-c", &script]);
    let (code, j, _) = n.wait(&job);
    let out = n.output(&job);
    assert_eq!(code, 0, "{j} {out}");
    assert!(out.contains("nested-in-place\nrc=0"), "{out}");
    assert!(!out.contains("too-big\n"), "{out}");
    assert!(out.contains("can't run inside job"), "{out}");
    assert!(out.contains("rc=2"), "{out}");
}

#[test]
fn a_restarted_daemon_adopts_running_jobs() {
    let mut n = Node::new();
    let slow = n.dispatch(&[
        "--timeout",
        "1m",
        "--",
        "sh",
        "-c",
        "sleep 4; echo survived; exit 4",
    ]);
    let quick = n.dispatch(&["--timeout", "1m", "--", "sh", "-c", "sleep 1; exit 5"]);
    n.running(&slow);
    n.running(&quick);
    n.stop_daemon();
    // `quick` ends while no daemon runs: its supervisor writes its exit status.
    let status = n.paths.job_dir(&quick).join("status.json");
    let start = Instant::now();
    while !status.exists() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "quick never ended"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    n.start_daemon();
    let (code, _, _) = n.wait(&quick);
    assert_eq!(code, 5);
    let (code, j, out) = n.wait(&slow);
    assert_eq!(code, 4, "{j} {}", n.daemon_log());
    assert!(out.contains("survived") || n.output(&slow).contains("survived"));
}

#[test]
fn a_daemon_log_that_is_also_stderr_gets_each_line_once() {
    // As under launchd, whose StandardOutPath and StandardErrorPath are the daemon log.
    let mut n = Node::new();
    n.stop_daemon();
    n.daemon_out = n.paths.daemon_log();
    n.start_daemon();
    n.stop_daemon();
    let log = std::fs::read_to_string(n.paths.daemon_log()).unwrap();
    let serving = log.lines().filter(|l| l.contains(" serving ")).count();
    assert_eq!(serving, 2, "one line for each of the two starts:\n{log}");
}

/// Makes the daemon log as long as it gets before it starts afresh (16 MiB), but for `short`
/// bytes, with a hole (the next daemon only reads its length).
fn fill_daemon_log(n: &Node, short: u64) {
    let f = File::options()
        .create(true)
        .append(true)
        .open(n.paths.daemon_log())
        .unwrap();
    f.set_len((16 << 20) - short).unwrap();
}

/// The inode of the file process `pid` has open as descriptor `fd`.
fn fd_inode(pid: u32, fd: i32) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    if cfg!(target_os = "linux") {
        return std::fs::metadata(format!("/proc/{pid}/fd/{fd}"))
            .ok()
            .map(|m| m.ino());
    }
    let out = Command::new("lsof")
        .args(["-n", "-P", "-a", "-p", &pid.to_string(), "-d"])
        .arg(fd.to_string())
        .args(["-F", "i"])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .find_map(|l| l.strip_prefix('i')?.parse().ok())
}

#[test]
fn a_daemon_log_that_is_also_stderr_starts_afresh_with_it() {
    use std::os::unix::fs::MetadataExt;
    // As under launchd: stdout and stderr are the daemon log, which is full.
    let mut n = Node::new();
    n.stop_daemon();
    fill_daemon_log(&n, 1);
    n.daemon_out = n.paths.daemon_log();
    n.start_daemon();
    let job = n.dispatch(&["--timeout", "1m", "--", "true"]);
    assert_eq!(n.wait(&job).0, 0);
    // The daemon's stdout and stderr moved to the new log with it.
    let pid = n.daemon.as_ref().unwrap().id();
    let log = std::fs::metadata(n.paths.daemon_log()).unwrap().ino();
    assert_eq!(fd_inode(pid, 1), Some(log), "stdout");
    assert_eq!(fd_inode(pid, 2), Some(log), "stderr");
    n.stop_daemon();

    let old = n.paths.home.join("daemon.log.1");
    assert_eq!(
        std::fs::metadata(&old).unwrap().len(),
        (16 << 20) - 1,
        "nothing more went to the old log"
    );
    let new = std::fs::read_to_string(n.paths.daemon_log()).unwrap();
    assert!(
        new.lines()
            .next()
            .unwrap()
            .ends_with(" the lines before this are in daemon.log.1"),
        "{new}"
    );
    let mut seen = std::collections::HashSet::new();
    for line in new.lines() {
        assert!(seen.insert(line), "twice: {line}\n{new}");
    }
    assert_eq!(new.matches(" serving ").count(), 1, "{new}");
    assert!(new.contains(&format!("{job}: succeeded")), "{new}");
}

#[test]
fn a_daemon_log_that_starts_afresh_keeps_sending_every_line_to_stderr() {
    use std::io::Read;
    // As under systemd: stderr is a socket to the journal, stdout nowhere.
    let mut n = Node::new();
    n.stop_daemon();
    fill_daemon_log(&n, 1);
    let (mut pipe, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
    n.start_daemon_with(Stdio::null(), std::os::fd::OwnedFd::from(theirs).into());
    let journal = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let into = journal.clone();
    std::thread::spawn(move || {
        let mut buf = [0; 4096];
        while let Ok(k @ 1..) = pipe.read(&mut buf) {
            into.lock().unwrap().extend_from_slice(&buf[..k]);
        }
    });
    let job = n.dispatch(&["--timeout", "1m", "--", "true"]);
    assert_eq!(n.wait(&job).0, 0);
    n.stop_daemon();

    let new = std::fs::read_to_string(n.paths.daemon_log()).unwrap();
    let mut lines = new.lines();
    assert!(
        lines
            .next()
            .unwrap()
            .ends_with(" the lines before this are in daemon.log.1"),
        "{new}"
    );
    let logged: Vec<&str> = lines.collect();
    assert!(new.contains(&format!("{job}: succeeded")), "{new}");
    let start = Instant::now();
    loop {
        let journal = String::from_utf8_lossy(&journal.lock().unwrap()).into_owned();
        let sent: Vec<&str> = journal.lines().collect();
        if sent == logged {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the log:\n{new}\nstderr:\n{journal}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn recipes_come_from_the_committed_file() {
    let n = Node::new();
    let job = n.dispatch(&["hello"]);
    let (code, j, out) = n.wait(&job);
    assert_eq!(code, 0, "{j}");
    assert!(
        out.contains("recipe toml") && out.contains("lib v1"),
        "{out}"
    );
    assert_eq!(n.get(&job).spec.request.ram_mib, 1024);
    assert_eq!(n.get(&job).spec.recipe.as_deref(), Some("hello"));

    // Uncommitted work never runs.
    std::fs::write(n.proj.join("README"), "changed\n").unwrap();
    let o = n.cli(&["dispatch", "hello"]);
    assert_ne!(o.code, 0);
    assert!(
        o.json["error"]
            .as_str()
            .unwrap()
            .contains("only committed work runs"),
        "{}",
        o.json
    );
}

/// `--on macos` and `--on linux` against this machine's real facts. The other OS, or a version
/// this machine doesn't have, is refused at dispatch with what this machine has; this OS at its
/// own major version (macOS) or major.minor kernel (Linux) runs, from the command line and from
/// a recipe.
#[test]
fn os_shorthands_choose_the_machine() {
    let n = Node::new();
    let facts = potluck::facts::detect(&nowhere(), &Default::default()).unwrap();
    // This machine's OS, the other one, and the version its shorthand compares against.
    let (this, other, version) = if cfg!(target_os = "macos") {
        ("macos", "linux", facts.os_version.clone())
    } else {
        ("linux", "macos", facts.kernel.clone())
    };
    let parts = potluck::types::version_parts(&version);
    // macOS 27.0.1 is asked for as macos=27; Linux 7.2.8-2-cachyos as linux=7.2.
    let ver = if this == "macos" {
        parts[0].clone()
    } else {
        format!("{}.{}", parts[0], parts[1])
    };
    let want = format!("{this}={ver}");
    let has = if this == "macos" {
        format!("macOS {version}")
    } else {
        format!("linux {version}")
    };
    let nowhere = |need: &str| {
        format!(
            "no machine could ever run this job ({}: needs {need} (this machine: {has}))",
            n.name
        )
    };
    let refused = |args: &[&str], why: &str| {
        let mut all = vec!["dispatch"];
        all.extend_from_slice(args);
        let o = n.cli(&all);
        assert_eq!(o.code, 2, "{args:?}: {} {}", o.json, o.stderr);
        assert_eq!(o.json["error"].as_str(), Some(why), "{args:?}");
    };

    let t = ["--timeout", "1m"];
    refused(
        &[&t[..], &["--on", other, "--", "true"]].concat(),
        &nowhere(other),
    );
    let zero = format!("{this}=0");
    refused(
        &[&t[..], &["--on", &zero, "--", "true"]].concat(),
        &nowhere(&zero),
    );
    refused(
        &[&t[..], &["--on", this, "--on", other, "--", "true"]].concat(),
        "--on: macos and linux contradict each other: a machine runs one OS",
    );

    let job = n.dispatch(&[
        "--timeout",
        "1m",
        "--on",
        &want,
        "--",
        "sh",
        "-c",
        "echo ran on $POTLUCK_NODE",
    ]);
    let (code, j, out) = n.wait(&job);
    assert_eq!(code, 0, "{j}");
    assert!(out.contains(&format!("ran on {}", n.name)), "{out}");
    let on = n.get(&job).spec.on;
    assert_eq!(on.get(this), Some(&ver), "{on:?}");
    assert_eq!(on.len(), 1, "{on:?}");

    // A recipe for the other OS is refused; an --on naming this OS replaces the recipe's.
    std::fs::write(
        n.proj.join("potluck.toml"),
        format!(
            "[recipe.elsewhere]\ncmd = [\"true\"]\ntimeout = \"1m\"\non = [\"{other}\"]\n\n[recipe.here]\ncmd = [\"true\"]\ntimeout = \"1m\"\non = {{ {this} = \"{ver}\" }}\n"
        ),
    )
    .unwrap();
    git(&n.proj, &["commit", "-qam", "recipes with on"]);
    refused(&["elsewhere"], &nowhere(other));
    for args in [&["here"][..], &["elsewhere", "--on", this]] {
        let job = n.dispatch(args);
        let (code, j, _) = n.wait(&job);
        assert_eq!(code, 0, "{args:?}: {j}");
    }
}

/// Two daemons on this machine, joined through the CLI: each lists both, and a job one of them
/// owns is followed (wait, logs, cancel, why) from the other.
#[test]
fn two_daemons_pair_and_follow_each_others_jobs() {
    // Configured ports, which the daemons bind only once in a cluster.
    let a = Node::with(Opts {
        budget_zero: false,
        port: common::free_port(),
    });
    let b = Node::with(Opts {
        budget_zero: false,
        port: common::free_port(),
    });
    assert!(
        !a.daemon_log().contains("listening on"),
        "in no cluster yet"
    );
    let request = b.cli_ok(&["pair"])["text"].as_str().unwrap().to_string();
    let reply = a.cli_ok(&["admit", &request])["text"]
        .as_str()
        .unwrap()
        .to_string();
    b.cli_ok(&["pair", "--finish", &reply]);
    let sees = |n: &Node, other: &str| {
        n.cli_ok(&["nodes"])["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["facts"]["node"] == other && m["alive"] == true && m["facts"]["os"] != "")
    };
    let start = Instant::now();
    while !(sees(&a, &b.name) && sees(&b, &a.name)) {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "never paired:\n{}\n{}",
            a.daemon_log(),
            b.daemon_log()
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // A job a owns and runs, followed from b.
    let on_a = format!("node={}", a.name);
    let job = a.dispatch(&[
        "--timeout",
        "1m",
        "--on",
        &on_a,
        "--",
        "sh",
        "-c",
        "echo hello from $POTLUCK_NODE; sleep 1; exit 5",
    ]);
    let o = b.cli(&["wait", &job]);
    assert_eq!(o.code, 5, "{} {}", o.json, o.stderr);
    assert!(
        o.stderr.contains(&format!("hello from {}", a.name)),
        "{}",
        o.stderr
    );
    assert_eq!(o.json["record"]["node"], a.name.as_str());
    assert!(b.output(&job).contains("hello from"));

    // Cancelled from b while it runs on a; why from b is a's answer.
    let long = a.dispatch(&["--timeout", "1m", "--on", &on_a, "--", "sleep", "60"]);
    a.running(&long);
    let w = b.cli_ok(&["why", &long]);
    assert!(w["lines"].to_string().contains("running"), "{w}");
    b.cli_ok(&["cancel", &long]);
    let (code, j, _) = a.wait(&long);
    assert_eq!((code, j["state"].as_str()), (130, Some("cancelled")));

    // status on either covers both machines.
    for n in [&a, &b] {
        let s = n.cli_ok(&["status"]);
        assert_eq!(s["nodes"].as_array().unwrap().len(), 2, "{s}");
    }
}

/// Pairs `b` into `a`'s cluster through the CLI and waits until each sees the other alive.
fn pair(a: &Node, b: &Node) {
    let request = b.cli_ok(&["pair"])["text"].as_str().unwrap().to_string();
    let reply = a.cli_ok(&["admit", &request])["text"]
        .as_str()
        .unwrap()
        .to_string();
    b.cli_ok(&["pair", "--finish", &reply]);
    let sees = |n: &Node, other: &str| {
        n.cli_ok(&["nodes"])["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["facts"]["node"] == other && m["alive"] == true && m["facts"]["os"] != "")
    };
    let start = Instant::now();
    while !(sees(a, &b.name) && sees(b, &a.name)) {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "never paired:\n{}\n{}",
            a.daemon_log(),
            b.daemon_log()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// `node`'s address as `of` shows it in `nodes`.
fn address_of(of: &Node, node: &str) -> String {
    of.cli_ok(&["nodes"])["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["facts"]["node"] == node)
        .and_then(|m| m["address"].as_str())
        .unwrap_or_default()
        .to_string()
}

/// A job one daemon owns and only the other can run: the runner's mirrors of the repository and
/// its submodule sync from the owner through the tunnel (git runs `potluck _tunnel` over the
/// mesh), and the job runs at the dispatched commits.
#[test]
fn a_runner_syncs_through_the_tunnel_and_runs_at_the_dispatched_commit() {
    let a = Node::new();
    let mut b = Node::new();
    // b's git commands are traced, to see the tunnel at work.
    let trace = b.dir.path().join("git-trace.txt");
    b.stop_daemon();
    b.daemon_env
        .push(("GIT_TRACE".into(), trace.display().to_string()));
    b.start_daemon();
    pair(&a, &b);
    let b_address = address_of(&b, &b.name);

    // New commits in a's project and its submodule, which only a has.
    let lib = a.proj.join("vendor/lib");
    std::fs::write(lib.join("lib.txt"), "lib v2\n").unwrap();
    git(&lib, &["commit", "-qam", "lib v2"]);
    std::fs::write(a.proj.join("README"), "through the tunnel\n").unwrap();
    git(&a.proj, &["commit", "-qam", "for b"]);
    let head = git(&a.proj, &["rev-parse", "HEAD"]);
    let lib_head = git(&lib, &["rev-parse", "HEAD"]);

    let on_b = format!("node={}", b.name);
    let job = a.dispatch(&[
        "--timeout",
        "1m",
        "--on",
        &on_b,
        "--",
        "sh",
        "-c",
        "git rev-parse HEAD; git -C vendor/lib rev-parse HEAD; cat README vendor/lib/lib.txt",
    ]);
    let (code, j, _) = a.wait(&job);
    let logs = || format!("{}\n{}", a.daemon_log(), b.daemon_log());
    assert_eq!(code, 0, "{j}\n{}", logs());
    assert_eq!(j["record"]["node"], b.name.as_str());
    let output = a.output(&job);
    for want in [&head, &lib_head, "through the tunnel", "lib v2"] {
        assert!(
            output.contains(want),
            "{want:?} not in {output:?}\n{}",
            logs()
        );
    }

    // b has mirrors of both repositories, and they came from a through `potluck _tunnel`.
    let mirrors = std::fs::read_dir(b.paths.home.join("mirrors"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".git"))
        .count();
    assert_eq!(mirrors, 2);
    let trace = std::fs::read_to_string(&trace).unwrap();
    let tunnels = trace
        .lines()
        .filter(|l| l.contains(&format!("_tunnel -- {} ", a.name)))
        .count();
    assert!(tunnels >= 2, "no tunnel in b's git trace:\n{}", logs());

    // a still reaches b where b listens.
    assert_eq!(address_of(&a, &b.name), b_address);
}

/// Worktrees of one project keep their submodules' git dirs apart: a submodule commit made in one
/// worktree is only in that worktree's. The runner syncs after another worktree dispatched, and
/// still gets each job's commits.
#[test]
fn a_runner_gets_a_submodule_commit_only_one_worktree_has() {
    let a = Node::new();
    let mut b = Node::new();
    pair(&a, &b);
    // A second worktree of a's project, with its own clone of the submodule.
    let w2 = a.dir.path().join("repos/w2");
    git(
        &a.proj,
        &["worktree", "add", "-q", "-b", "w2", w2.to_str().unwrap()],
    );
    git(&w2, &["submodule", "update", "--init", "-q"]);
    // A submodule commit made in the first worktree only.
    let lib = a.proj.join("vendor/lib");
    std::fs::write(lib.join("lib.txt"), "lib local\n").unwrap();
    git(&lib, &["commit", "-qam", "lib local"]);
    git(&a.proj, &["commit", "-qam", "lib local"]);

    // Both worktrees dispatch while b is away (not lost yet); b syncs when it is back.
    b.stop_daemon();
    let on_b = format!("node={}", b.name);
    let args = [
        "dispatch",
        "--timeout",
        "1m",
        "--on",
        &on_b,
        "--",
        "cat",
        "vendor/lib/lib.txt",
    ];
    let job = |cwd: &Path| {
        let o = a.cli_in(cwd, &args);
        assert_eq!(o.code, 0, "{} {}", o.json, o.stderr);
        o.json["job"].as_str().unwrap().to_string()
    };
    let first = job(&a.proj);
    let second = job(&w2);
    b.start_daemon();
    for (j, want) in [(&first, "lib local"), (&second, "lib v1")] {
        let r = a.wait_for(j, "ended", 60, |r| r.state.is_done());
        assert_eq!(
            r.state,
            JobState::Succeeded,
            "{r:#?}\n{}\n{}",
            a.daemon_log(),
            b.daemon_log()
        );
        assert_eq!(a.output(j).trim(), want);
    }
}

/// The `*.git` mirrors in a node's state directory.
fn mirrors_of(node: &Node) -> Vec<PathBuf> {
    let Ok(dir) = std::fs::read_dir(node.paths.home.join("mirrors")) else {
        return Vec::new();
    };
    dir.flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "git"))
        .collect()
}

/// An owner serves a runner's sync from its own mirror of a repository, and from the source
/// repository when that mirror is gone. Here a's mirrors (the project's and its submodule's) are
/// removed after the dispatch, while b is away; back, b syncs from a's sources through the tunnel
/// and runs the job at its commits, and a's mirrors are still gone.
#[test]
fn a_runner_syncs_from_the_source_when_the_owners_mirror_is_gone() {
    let a = Node::new();
    let mut b = Node::new();
    pair(&a, &b);
    let lib = a.proj.join("vendor/lib");
    std::fs::write(lib.join("lib.txt"), "lib from the source\n").unwrap();
    git(&lib, &["commit", "-qam", "lib v2"]);
    git(&a.proj, &["commit", "-qam", "lib v2"]);
    let head = git(&a.proj, &["rev-parse", "HEAD"]);
    let lib_head = git(&lib, &["rev-parse", "HEAD"]);

    b.stop_daemon();
    let on_b = format!("node={}", b.name);
    let job = a.dispatch(&[
        "--timeout",
        "1m",
        "--on",
        &on_b,
        "--",
        "sh",
        "-c",
        "git rev-parse HEAD; git -C vendor/lib rev-parse HEAD; cat vendor/lib/lib.txt",
    ]);
    let mirrors = mirrors_of(&a);
    assert_eq!(mirrors.len(), 2, "{mirrors:?}");
    for m in &mirrors {
        std::fs::remove_dir_all(m).unwrap();
    }
    b.start_daemon();
    let r = a.wait_for(&job, "ended", 90, |r| r.state.is_done());
    let logs = || format!("{}\n{}", a.daemon_log(), b.daemon_log());
    assert_eq!(r.state, JobState::Succeeded, "{r:#?}\n{}", logs());
    assert_eq!(r.node.as_deref(), Some(b.name.as_str()));
    assert_eq!(
        a.output(&job),
        format!("{head}\n{lib_head}\nlib from the source\n"),
        "{}",
        logs()
    );
    assert!(mirrors_of(&a).is_empty(), "{:?}", mirrors_of(&a));
    assert_eq!(mirrors_of(&b).len(), 2);
}

// ---------------------------------------------------------------------------------------------
// Restart in place (`potluck restart`; src/daemon/handover.rs)

/// `status` called in a loop on a thread of its own until it is finished: when each answer came,
/// and each error.
struct Caller {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<(Vec<Instant>, Vec<String>)>>,
}

impl Caller {
    /// On a new connection for each call, or (`kept`) on one connection throughout.
    fn start(paths: &Paths, kept: bool) -> Caller {
        use std::sync::atomic::AtomicBool;
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let (paths, stop2) = (paths.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            let mut answers = Vec::new();
            let mut errors = Vec::new();
            let mut conn: Option<Client> = None;
            while !stop2.load(Ordering::SeqCst) {
                if conn.is_none() {
                    match Client::connect(&paths, ClientContext::default()) {
                        Ok(c) => conn = Some(c),
                        Err(e) => {
                            errors.push(format!("{e:#}"));
                            std::thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                    }
                }
                match conn.as_mut().unwrap().call(Request::Status) {
                    Ok(Response::Status { .. }) => answers.push(Instant::now()),
                    Ok(other) => errors.push(format!("{other:?}")),
                    Err(e) => {
                        errors.push(format!("{e:#}"));
                        conn = None;
                    }
                }
                if !kept {
                    conn = None;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            (answers, errors)
        });
        Caller {
            stop,
            thread: Some(thread),
        }
    }

    fn finish(mut self) -> (Vec<Instant>, Vec<String>) {
        self.stop.store(true, Ordering::SeqCst);
        self.thread.take().unwrap().join().unwrap()
    }
}

/// The longest time between two answers.
fn longest_gap(answers: &[Instant]) -> Duration {
    answers
        .windows(2)
        .map(|w| w[1] - w[0])
        .max()
        .unwrap_or_default()
}

/// Runs the CLI (without `--json`) as a child process, its output piped.
fn spawn_cli(n: &Node, args: &[&str]) -> Child {
    Command::new(BIN)
        .args(args)
        .current_dir(&n.proj)
        .env("POTLUCK_HOME", &n.paths.home)
        .env("POTLUCK_CONFIG", &n.paths.config)
        .env_remove("POTLUCK_JOB")
        .env_remove("POTLUCK_LEASE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// `potluck restart`: the daemon runs its binary again in its own process and hands it the socket
/// and its connections. A client calling `status` in a loop, on a new connection each time or on
/// one throughout, never sees an error, and the daemon keeps its pid. Before, the socket closed
/// while the new daemon started (connections refused, a kept one broken). The longest time
/// between two answers is printed: the restart's gap as a client sees it.
#[test]
fn a_restart_in_place_loses_no_request_and_no_connection() {
    let n = Node::new();
    let pid = n.daemon.as_ref().unwrap().id();
    let fresh = Caller::start(&n.paths, false);
    let kept = Caller::start(&n.paths, true);
    std::thread::sleep(Duration::from_millis(500));
    let o = n.cli(&["restart"]);
    std::thread::sleep(Duration::from_millis(500));
    let (answers, errors) = fresh.finish();
    let (kept_answers, kept_errors) = kept.finish();
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    let text = o.json["text"].as_str().unwrap_or_default();
    assert!(
        text.starts_with("Restarted in place") && text.contains(&format!("(pid {pid})")),
        "{text}"
    );
    assert!(errors.is_empty(), "{errors:?}\n{}", n.daemon_log());
    assert!(
        kept_errors.is_empty(),
        "{kept_errors:?}\n{}",
        n.daemon_log()
    );
    let gap = longest_gap(&answers);
    eprintln!(
        "restart in place: at most {gap:?} between two status answers ({} answers on new \
         connections, {} on the kept one)",
        answers.len(),
        kept_answers.len()
    );
    assert!(gap < Duration::from_secs(5), "{gap:?}");
    assert!(
        n.daemon_log().contains("restarted in place from potluck"),
        "{}",
        n.daemon_log()
    );
    assert!(alive(pid as i32));
}

/// Requests a client sends right behind `restart` on the same connection, before its answer,
/// are answered after it, by the new binary. The daemon before had read them along with the
/// `restart` and let them go with its exec: the client waited for answers that never came.
#[test]
fn requests_sent_behind_a_restart_on_its_connection_are_answered() {
    use std::io::{BufRead, Write};
    let n = Node::new();
    let mut s = std::os::unix::net::UnixStream::connect(n.paths.socket()).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let reqs: String = ["restart", "ping", "status"]
        .iter()
        .map(|op| format!("{{\"ctx\":{{}},\"req\":{{\"op\":\"{op}\"}}}}\n"))
        .collect();
    s.write_all(reqs.as_bytes()).unwrap();
    let mut r = std::io::BufReader::new(s);
    let kinds: Vec<String> = (0..3)
        .map(|_| {
            let mut line = String::new();
            match r.read_line(&mut line) {
                Ok(0) => "closed".to_string(),
                Ok(_) => serde_json::from_str::<Value>(&line).unwrap()["resp"]["kind"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                Err(e) => format!("{e}"),
            }
        })
        .collect();
    assert_eq!(kinds, ["text", "pong", "status"], "{}", n.daemon_log());
}

/// `wait` and `logs -f` started before a restart in place go on on the same connection: the
/// daemon ends their long polls early with what there is, and their next request is read by the
/// new binary. Each prints the job's whole output once, and `wait` exits with the job's code; a
/// client's own `logs` long polls on one connection get every byte once. The same from inside
/// another job: `logs -f`, `wait --max-wait` and a dispatch that runs in place within that job's
/// lease. The job runs on across the restart and ends with its code and its whole log.
#[test]
fn wait_and_logs_follow_go_on_across_a_restart_in_place() {
    let n = Node::new();
    let lines: String = (1..=30).map(|i| format!("line {i}\n")).collect();
    let job = n.dispatch(&[
        "--timeout",
        "1m",
        "--ram",
        "1G",
        "--",
        "sh",
        "-c",
        "i=1; while [ $i -le 30 ]; do echo line $i; i=$((i+1)); sleep 0.1; done; exit 7",
    ]);
    // `wait` says how the job ended as `logs -f` ends: what it says is put after, in order.
    let inner = format!(
        "{BIN} wait -q --max-wait 60s {job} 2>\"$TMPDIR/wait.err\" & w=$!; {BIN} logs -f {job}; \
         echo logs=$?; wait $w; echo wait=$?; cat \"$TMPDIR/wait.err\"; \
         {BIN} dispatch --timeout 10s --ram 1G -- echo nested; echo dispatch=$?"
    );
    let outer = n.dispatch(&["--timeout", "1m", "--ram", "2G", "--", "sh", "-c", &inner]);
    let waiting = spawn_cli(&n, &["wait", &job]);
    let following = spawn_cli(&n, &["logs", "-f", &job]);
    let (paths, id) = (n.paths.clone(), job.clone());
    let polling = std::thread::spawn(move || -> Result<Vec<u8>, String> {
        let mut c = Client::connect(&paths, ClientContext::default()).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        loop {
            let r = c.call(Request::Logs {
                job: id.clone(),
                offset: out.len() as u64,
                max: 0,
                wait_ms: 20_000,
            });
            match r.map_err(|e| format!("{e:#} after {} bytes", out.len()))? {
                Response::Bytes { len, done, .. } => {
                    out.extend(c.read_bytes(len).map_err(|e| e.to_string())?);
                    if done {
                        return Ok(out);
                    }
                }
                other => return Err(format!("{other:?}")),
            }
        }
    });
    n.running(&job);
    n.running(&outer);
    std::thread::sleep(Duration::from_millis(1200));
    let o = n.cli(&["restart"]);
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    let w = waiting.wait_with_output().unwrap();
    let say = |o: &std::process::Output| String::from_utf8_lossy(&o.stderr).into_owned();
    assert_eq!(w.status.code(), Some(7), "{}", say(&w));
    assert_eq!(String::from_utf8_lossy(&w.stdout), lines, "{}", say(&w));
    let f = following.wait_with_output().unwrap();
    assert_eq!(f.status.code(), Some(0), "{}", say(&f));
    assert_eq!(String::from_utf8_lossy(&f.stdout), lines, "{}", say(&f));
    let polled = polling.join().unwrap().unwrap();
    assert_eq!(String::from_utf8_lossy(&polled), lines);
    let (code, j, _) = n.wait(&outer);
    assert_eq!(code, 0, "{j}\n{}", n.output(&outer));
    assert_eq!(
        n.output(&outer),
        format!(
            "{lines}logs=0\nwait=7\npotluck: job {job} failed, exit code 7\npotluck: inside job \
             {outer}: running in place, within its lease (not dispatched)\nnested\ndispatch=0\n"
        )
    );
    let r = n.get(&job);
    assert_eq!(r.exit_code, Some(7), "{r:#?}");
    assert_eq!(n.output(&job), lines);
}

/// Restores a directory's mode when dropped (a test that made it read-only may fail first).
struct Mode(PathBuf, u32);

impl Drop for Mode {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(self.1));
    }
}

/// The binary at the daemon's path is checked before anything is handed over. One that doesn't
/// run leaves the daemon serving on the binary it runs, saying why; one that runs but can't start
/// from a handover (here it answers `_handover-check` as a potluck from before it would, with a
/// usage error) gets a plain restart on it, which `potluck restart` reports as done (exit 0),
/// saying it was plain and why. Before, it exited 2 as if nothing had restarted, and install.sh
/// reported an update that had worked as failed. Running jobs go on through both.
#[test]
fn a_broken_new_binary_leaves_the_daemon_serving_and_saying_why() {
    let mut n = Node::new();
    let installed = n.dir.path().join("bin/potluck");
    std::fs::create_dir_all(installed.parent().unwrap()).unwrap();
    common::copy_binary(Path::new(BIN), &installed);
    n.stop_daemon();
    n.bin = installed.clone();
    n.start_daemon();
    let pid = n.daemon.as_ref().unwrap().id();
    let job = n.dispatch(&[
        "--timeout",
        "1m",
        "--",
        "sh",
        "-c",
        "sleep 4; echo survived; exit 3",
    ]);
    n.running(&job);
    let replace = |contents: &str| {
        let new = installed.with_file_name(".potluck.new");
        std::fs::write(&new, contents).unwrap();
        std::fs::set_permissions(&new, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::rename(&new, &installed).unwrap();
    };

    replace("not a program\n");
    let o = n.cli(&["restart"]);
    let said = o.json["error"].as_str().unwrap_or_default().to_string();
    assert_eq!(o.code, 2, "{} {}", o.json, o.stderr);
    assert!(
        said.contains("doesn't run")
            && said.contains(&format!("keeps running potluck {}", potluck::VERSION)),
        "{said}"
    );
    n.cli_ok(&["status"]);
    assert!(alive(pid as i32));
    assert!(
        n.daemon_log().contains("not restarting: "),
        "{}",
        n.daemon_log()
    );

    replace(&format!(
        "#!/bin/sh\ncase \"$1\" in\n--version) echo 'potluck 0.0.9' ;;\n_handover-check) echo \"error: unrecognized subcommand '_handover-check'\" >&2; exit 2 ;;\n*) exec {BIN} \"$@\" ;;\nesac\n"
    ));
    // A plain restart did happen: `potluck restart` says so and why, and exits 0 once the
    // daemon answers again (install.sh then reports the update as done, not as failed).
    let o = n.cli(&["restart"]);
    let said = o.json["text"].as_str().unwrap_or_default().to_string();
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    assert!(
        said.starts_with("Restarted plainly, not in place")
            && said.contains("potluck 0.0.9")
            && said.contains("can't take over")
            && said.contains("unrecognized subcommand")
            && said.contains("Open connections were closed"),
        "{said}"
    );
    // The same process, on the new binary, started without a handover, serving already.
    n.cli_ok(&["status"]);
    assert!(alive(pid as i32));
    let log = n.daemon_log();
    let last = log.lines().rfind(|l| l.contains(" serving ")).unwrap();
    assert!(!last.contains("restarted in place"), "{log}");
    let (code, j, _) = n.wait(&job);
    assert_eq!(code, 3, "{j}");
    assert_eq!(n.output(&job), "survived\n");
}

/// A plain restart (the binary at the daemon's path can't take a handover) closes the socket, and
/// the binary it starts listens only once it has captured the login environment and detected the
/// facts (seconds: here the login shell takes 2 s). Clients that connect meanwhile wait for it, as
/// they wait in a restart in place; before, each was told "the potluck daemon is not running
/// (...: No such file or directory)" for those 2 s. What is left is the exec itself: from the
/// moment the old binary's exec lets go of `daemon.lock` to the new binary taking it first thing,
/// a client finds no daemon (a few ms; the span is printed).
#[test]
fn clients_connecting_during_a_plain_restart_wait_for_the_new_daemon() {
    use std::sync::atomic::AtomicBool;
    let mut n = Node::new();
    let installed = n.dir.path().join("bin/potluck");
    std::fs::create_dir_all(installed.parent().unwrap()).unwrap();
    common::copy_binary(Path::new(BIN), &installed);
    n.stop_daemon();
    n.bin = installed.clone();
    n.daemon_env
        .push(("SHELL".into(), slow_shell().display().to_string()));
    n.start_daemon();
    let pid = n.daemon.as_ref().unwrap().id();
    // A binary that answers `_handover-check` as a potluck from before it would.
    let new = installed.with_file_name(".potluck.new");
    std::fs::write(
        &new,
        format!(
            "#!/bin/sh\ncase \"$1\" in\n_handover-check) echo \"error: unrecognized subcommand '_handover-check'\" >&2; exit 2 ;;\nesac\nexec {BIN} \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&new, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&new, &installed).unwrap();
    // `status` on a new connection each time: when each answer came, and each "not running".
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let (paths, stop2) = (n.paths.clone(), stop.clone());
    let caller = std::thread::spawn(move || {
        let (mut answers, mut refused, mut other) = (Vec::new(), Vec::new(), Vec::new());
        while !stop2.load(Ordering::SeqCst) {
            match Client::connect(&paths, ClientContext::default()) {
                Ok(mut c) => match c.call(Request::Status) {
                    Ok(_) => answers.push(Instant::now()),
                    // A request in flight when the old binary stops: its connection closes.
                    Err(e) => other.push(format!("{e:#}")),
                },
                Err(e) => refused.push((Instant::now(), format!("{e:#}"))),
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        (answers, refused, other)
    });
    std::thread::sleep(Duration::from_millis(300));
    let o = n.cli(&["restart"]);
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::SeqCst);
    let (answers, refused, other) = caller.join().unwrap();
    let said = o.json["text"].as_str().unwrap_or_default().to_string();
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    assert!(
        said.starts_with("Restarted plainly, not in place"),
        "{said}"
    );
    let span = match (refused.first(), refused.last()) {
        (Some(a), Some(b)) => b.0 - a.0,
        _ => Duration::ZERO,
    };
    eprintln!(
        "plain restart: {} answers; {} connections found no daemon, within {span:?}; {} closed; \
         at most {:?} between two answers",
        answers.len(),
        refused.len(),
        other.len(),
        longest_gap(&answers)
    );
    assert!(
        span < Duration::from_secs(1),
        "{} connections found no daemon within {span:?}: first {:?}, last {:?}\n{}",
        refused.len(),
        refused.first(),
        refused.last(),
        n.daemon_log()
    );
    n.cli_ok(&["status"]);
    assert!(alive(pid as i32));
}

/// A plain restart (the binary at the daemon's path can't take a handover) whose binary can't be
/// executed any more when the daemon gets to its exec leaves the daemon serving on the binary it
/// runs. `potluck restart` says it failed and why, and exits 2. Before, it was told "Restarted
/// plainly" as the daemon began to stop, and the failed exec then ended the daemon: nothing
/// answered again.
#[test]
fn a_plain_restart_that_fails_at_the_exec_is_not_reported_as_done() {
    let mut n = Node::new();
    let installed = n.dir.path().join("bin/potluck");
    std::fs::create_dir_all(installed.parent().unwrap()).unwrap();
    common::copy_binary(Path::new(BIN), &installed);
    n.stop_daemon();
    n.bin = installed.clone();
    n.start_daemon();
    let pid = n.daemon.as_ref().unwrap().id();
    // Answers `_handover-check` as a potluck from before it would, and can't be executed after.
    let new = installed.with_file_name(".potluck.new");
    std::fs::write(
        &new,
        format!(
            "#!/bin/sh\ncase \"$1\" in\n_handover-check) chmod 0644 \"$0\"; echo \"error: unrecognized subcommand '_handover-check'\" >&2; exit 2 ;;\nesac\nexec {BIN} \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&new, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&new, &installed).unwrap();
    let o = n.cli(&["restart"]);
    let said = o.json["error"].as_str().unwrap_or_default().to_string();
    assert_eq!(o.code, 2, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    assert!(
        said.contains("restarting plainly failed")
            && said.contains(&installed.display().to_string())
            && said.contains(&format!("potluck {}", potluck::VERSION)),
        "{said}"
    );
    assert!(alive(pid as i32));
    n.cli_ok(&["status"]);
    let log = n.daemon_log();
    assert!(log.contains("restarting plainly failed: "), "{log}");
}

/// A restart in place that fails after the new binary passed its check (here the binary at the
/// daemon's path can't be executed any more when the daemon execs it) leaves the daemon serving
/// on the binary it runs, from what it handed over. `potluck restart` says it failed and why, and
/// exits 2; before, the daemon answered it as if the new binary had taken over ("Restarted in
/// place"), so install.sh and `self-update --here` reported an update the daemon never ran.
#[test]
fn a_restart_in_place_that_fails_at_the_exec_is_not_reported_as_done() {
    let mut n = Node::new();
    let installed = n.dir.path().join("bin/potluck");
    std::fs::create_dir_all(installed.parent().unwrap()).unwrap();
    common::copy_binary(Path::new(BIN), &installed);
    n.stop_daemon();
    n.bin = installed.clone();
    n.start_daemon();
    let pid = n.daemon.as_ref().unwrap().id();
    // Passes the check, then can't be executed: what a binary replaced between the two would do.
    let new = installed.with_file_name(".potluck.new");
    std::fs::write(
        &new,
        format!(
            "#!/bin/sh\ncase \"$1\" in\n_handover-check) chmod 0644 \"$0\" ;;\nesac\nexec {BIN} \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&new, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&new, &installed).unwrap();
    let o = n.cli(&["restart"]);
    let said = o.json["error"].as_str().unwrap_or_default().to_string();
    assert_eq!(o.code, 2, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    assert!(
        said.contains("restarting in place failed")
            && said.contains(&installed.display().to_string())
            && said.contains(&format!("potluck {}", potluck::VERSION)),
        "{said}"
    );
    assert!(alive(pid as i32));
    n.cli_ok(&["status"]);
    let log = n.daemon_log();
    assert!(log.contains("restarting in place failed: "), "{log}");
    let last = log.lines().rfind(|l| l.contains(" serving ")).unwrap();
    assert!(!last.contains("restarted in place from"), "{log}");
}

/// A daemon that serves on after its exec failed (a restart in place or a plain one) still
/// ignores SIGPIPE, as every potluck does: a pipe or socket written to after its reader went
/// (here the daemon's stderr, as journald's stream when journald restarts, or a `| tee` that
/// ended) fails that write only. Before, the failed exec left SIGPIPE at its default (std sets it
/// so for the program it executes), and the next log line killed the daemon.
#[test]
fn a_daemon_serving_on_after_a_failed_exec_outlives_its_stderr() {
    let mut n = Node::new();
    let installed = n.dir.path().join("bin/potluck");
    std::fs::create_dir_all(installed.parent().unwrap()).unwrap();
    common::copy_binary(Path::new(BIN), &installed);
    n.stop_daemon();
    n.bin = installed.clone();
    let out = File::options()
        .create(true)
        .append(true)
        .open(&n.daemon_out)
        .unwrap();
    n.start_daemon_with(out.into(), Stdio::piped());
    let stderr = n.daemon.as_mut().unwrap().stderr.take().unwrap();
    // Passes the check, then can't be executed.
    let new = installed.with_file_name(".potluck.new");
    std::fs::write(
        &new,
        format!(
            "#!/bin/sh\ncase \"$1\" in\n_handover-check) chmod 0644 \"$0\" ;;\nesac\nexec {BIN} \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&new, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&new, &installed).unwrap();
    let o = n.cli(&["restart"]);
    assert_eq!(o.code, 2, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    // Its stderr's reader goes; the daemon logs on (a restart asked again, refused the same way).
    drop(stderr);
    let o = n.cli(&["restart"]);
    std::thread::sleep(Duration::from_millis(500));
    let ended = n.daemon.as_mut().unwrap().try_wait().unwrap();
    assert!(
        ended.is_none(),
        "the daemon ended: {ended:?}; restart said {} {}",
        o.json,
        o.stderr
    );
    n.cli_ok(&["status"]);
}

/// `ps -o stat=,ppid=` of `pid`: None once it is gone (reaped).
fn proc_state(pid: i32) -> Option<String> {
    let out = Command::new("ps")
        .args(["-o", "stat=,ppid=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (out.status.success() && !s.is_empty()).then_some(s)
}

/// No git that work cut short by a restart in place started outlives the exec. Here two
/// dispatches hang in `git fetch` (a wrapper in the daemon's PATH) when the restart comes: one
/// fetch ignores SIGTERM, so the old binary waits its grace before the SIGKILL, and meanwhile the
/// other dispatch's thread, its fetch ended, tries the fetch again. That second fetch was started
/// after the old binary ended its children: before, it ran on under the new binary, which knew
/// nothing of it but its pid (to reap), writing into the mirror the new binary syncs.
#[test]
fn no_git_cut_short_by_a_restart_in_place_outlives_the_exec() {
    let mut n = Node::new();
    let wrap = n.dir.path().join("wrap");
    std::fs::create_dir_all(&wrap).unwrap();
    let hang = n.dir.path().join("hang");
    let log = n.dir.path().join("fetches.log");
    let real = String::from_utf8(
        Command::new("sh")
            .args(["-c", "command -v git"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    let real = real.trim();
    // While `hang` exists, a fetch of lib is a sleep SIGTERM ends; of proj, one that ignores it.
    std::fs::write(
        wrap.join("git"),
        format!(
            "#!/bin/sh\nf=no\nfor a in \"$@\"; do [ \"$a\" = fetch ] && f=yes; done\n\
             if [ $f = yes ] && [ -e '{hang}' ]; then\n\
             case \"$*\" in\n\
             */repos/lib*) echo \"lib $$ $*\" >> '{log}'; exec sleep 30 ;;\n\
             *) echo \"proj $$ $*\" >> '{log}'; trap '' TERM; exec sleep 30 ;;\n\
             esac\nfi\nexec '{real}' \"$@\"\n",
            hang = hang.display(),
            log = log.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(wrap.join("git"), std::fs::Permissions::from_mode(0o755)).unwrap();
    n.stop_daemon();
    n.daemon_env.push((
        "PATH".into(),
        format!("{}:{}", wrap.display(), std::env::var("PATH").unwrap()),
    ));
    n.start_daemon();
    let lib = n.dir.path().join("repos/lib");
    // The mirrors exist: the next syncs fetch. Those jobs are over (their ends unpin their
    // commits, which waits for a sync of the mirror).
    for cwd in [&n.proj, &lib] {
        let o = n.cli_in(cwd, &["dispatch", "--timeout", "1m", "--", "true"]);
        assert_eq!(o.code, 0, "{} {}", o.json, o.stderr);
        let job = o.json["job"].as_str().unwrap().to_string();
        assert_eq!(n.wait(&job).0, 0);
    }
    std::thread::sleep(Duration::from_millis(500));
    std::fs::write(&hang, "").unwrap();
    let dispatch = |cwd: &Path| {
        Command::new(BIN)
            .args(["--json", "dispatch", "--timeout", "1m", "--", "true"])
            .current_dir(cwd)
            .env("POTLUCK_HOME", &n.paths.home)
            .env("POTLUCK_CONFIG", &n.paths.config)
            .env_remove("POTLUCK_JOB")
            .env_remove("POTLUCK_LEASE")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    };
    let mut clients = [dispatch(&n.proj), dispatch(&lib)];
    let fetches = || -> Vec<(String, i32)> {
        std::fs::read_to_string(&log)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| {
                let mut w = l.split(' ');
                let (what, pid) = (w.next()?, w.next()?);
                Some((what.to_string(), pid.parse().ok()?))
            })
            .collect()
    };
    let start = Instant::now();
    while fetches().len() < 2 {
        assert!(start.elapsed() < Duration::from_secs(30), "{:?}", fetches());
        std::thread::sleep(Duration::from_millis(50));
    }
    let hung_at = Instant::now();
    let o = n.cli(&["restart"]);
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    let daemon = n.daemon.as_ref().unwrap().id();
    // The first fetches sleep 30 s unless a signal ends them, and only the old binary's end of
    // its children sends one: any fetch after them was started after that.
    assert!(hung_at.elapsed() < Duration::from_secs(25));
    let all = fetches();
    let left: Vec<(i32, String)> = all
        .iter()
        .filter_map(|(_, pid)| proc_state(*pid).map(|s| (*pid, s)))
        .collect();
    // Ours, by pid, whatever the result: the sleeps the wrapper became.
    for (_, pid) in &all {
        let comm = Command::new("ps")
            .args(["-o", "comm=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&comm.stdout)
            .trim()
            .ends_with("sleep")
        {
            unsafe { libc::kill(*pid, libc::SIGKILL) };
        }
    }
    for c in &mut clients {
        let _ = c.wait();
    }
    // Tried again by the dispatch's thread once its first fetch was ended.
    assert!(
        all.iter().filter(|(what, _)| what == "lib").count() >= 2,
        "lib's fetch wasn't tried again: {}\n{}",
        std::fs::read_to_string(&log).unwrap_or_default(),
        n.daemon_log()
    );
    assert!(
        left.is_empty(),
        "still there after the restart in place (pid, state and parent): {left:?} of {all:?}; \
         the daemon is {daemon}\n{}",
        n.daemon_log()
    );
}

/// A restart waits while the daemon can't write down what it would hand over (here its state
/// directory is read-only, so no handover file can be made): `potluck restart` says why, and the
/// daemon serves on and tries again every 10 s. A second `potluck restart` meanwhile joins the
/// one that waits: once the directory can be written again, the daemon restarts once and
/// answers it.
#[test]
fn a_restart_waits_while_the_state_directory_cant_be_written() {
    let n = Node::new();
    let home = n.paths.home.clone();
    let _restore = Mode(home.clone(), 0o700);
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o500)).unwrap();
    let o = n.cli(&["restart"]);
    let said = o.json["error"].as_str().unwrap_or_default().to_string();
    assert_eq!(o.code, 2, "{} {}", o.json, o.stderr);
    assert!(
        said.contains("the restart in place waits: writing")
            && said.contains("handover-check.json")
            && said.contains("tries again every 10s"),
        "{said}"
    );
    n.cli_ok(&["status"]);
    let second = spawn_cli(&n, &["--json", "restart"]);
    std::thread::sleep(Duration::from_millis(500));
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o700)).unwrap();
    let out = second.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}\n{}", n.daemon_log());
    assert!(stdout.contains("Restarted in place"), "{stdout}");
    let log = n.daemon_log();
    assert_eq!(
        log.matches("restarted in place from potluck").count(),
        1,
        "{log}"
    );
    n.cli_ok(&["status"]);
}

/// The handover file itself can't be written after the drain, as when the disk fills between the
/// check's sample file and it (here a directory stands where the file goes; the sample, written
/// under another name, fits): the restart waits too. `potluck restart` says why, and the daemon
/// serves on and tries again every 10 s on its own, so once the file can be written it restarts
/// in place without being asked again. Before, the failed write counted as a failed exec:
/// "restarting in place failed: writing .../handover.json", and nothing tried again.
#[test]
fn a_restart_waits_while_its_handover_file_cant_be_written() {
    let n = Node::new();
    let file = n.paths.home.join("handover.json");
    std::fs::create_dir(&file).unwrap();
    let o = n.cli(&["restart"]);
    let said = o.json["error"].as_str().unwrap_or_default().to_string();
    assert_eq!(o.code, 2, "{} {}", o.json, o.stderr);
    assert!(
        said.contains("the restart in place waits: writing")
            && said.contains("handover.json")
            && said.contains("tries again every 10s"),
        "{said}\n{}",
        n.daemon_log()
    );
    n.cli_ok(&["status"]);
    std::fs::remove_dir(&file).unwrap();
    let start = Instant::now();
    while !n.daemon_log().contains("restarted in place from potluck") {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "not tried again: {}",
            n.daemon_log()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    n.cli_ok(&["status"]);
}

/// Connections made while a restart in place has them wait in the socket's backlog all get their
/// answers, however many there are. The backlog holds at most 128 on macOS
/// (`kern.ipc.somaxconn`), which refuses the connections past it at once: the CLI said "the
/// potluck daemon is not running" for each (Linux makes them wait instead). Here a copy read
/// slowly keeps the restart waiting for it (a request in flight finishes first) while 300
/// clients connect, then the copy is read to its end.
#[test]
fn clients_past_the_backlog_during_a_restart_in_place_get_answers() {
    const CLIENTS: usize = 300;
    let n = Node::new();
    let job = n.dispatch(&[
        "--timeout",
        "1m",
        "--",
        "sh",
        "-c",
        "head -c 1048576 /dev/zero > big",
    ]);
    let (code, j, _) = n.wait(&job);
    assert_eq!(code, 0, "{j}");
    let ws = j["record"]["workspace"].as_str().unwrap().to_string();
    // The copy's first piece is in; the rest waits for this client to read it.
    let mut copy = n.client();
    let first = copy
        .call(Request::Copy {
            workspace: ws,
            path: "big".into(),
        })
        .unwrap();
    let Response::CopyEntry { len, .. } = first else {
        panic!("{first:?}")
    };
    let restart = spawn_cli(&n, &["--json", "restart"]);
    let start = Instant::now();
    while !n.daemon_log().contains(": draining") {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "{}",
            n.daemon_log()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // Past the drain (nothing to wait for), connections wait in the backlog.
    std::thread::sleep(Duration::from_millis(300));
    let callers: Vec<_> = (0..CLIENTS)
        .map(|_| {
            let paths = n.paths.clone();
            std::thread::spawn(move || -> Result<(), String> {
                let mut c = Client::connect(&paths, ClientContext::default())
                    .map_err(|e| format!("{e:#}"))?;
                match c.call(Request::Ping) {
                    Ok(Response::Pong { .. }) => Ok(()),
                    other => Err(format!("{other:?}")),
                }
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(500));
    copy.copy_bytes(len, &mut std::io::sink()).unwrap();
    assert!(matches!(copy.next_frame().unwrap(), Response::CopyDone));
    let failed: Vec<String> = callers
        .into_iter()
        .filter_map(|t| t.join().unwrap().err())
        .collect();
    let out = restart.wait_with_output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        failed.is_empty(),
        "{} of {CLIENTS} clients failed, first: {}\n{}",
        failed.len(),
        failed[0],
        n.daemon_log()
    );
}

/// `potluck restart` applies node.toml as a plain start would, and a node.toml `potluck daemon`
/// would refuse leaves the daemon serving, saying why: here one with a key given twice, then one
/// renaming the machine (a plain start refuses a name other than the one its records give it).
/// Before, the first handed over to a binary that ended at once (the daemon gone, its socket
/// left, the restart's caller told only that the connection closed), and the second was taken in
/// half: `nodes` named the machine anew while its jobs and the cluster kept the old name. A job
/// runs on throughout, and a good node.toml restarts it.
#[test]
fn a_restart_in_place_with_a_node_toml_the_daemon_would_refuse_keeps_it_serving() {
    let n = Node::new();
    let pid = n.daemon.as_ref().unwrap().id();
    let job = n.dispatch(&["--timeout", "1m", "--", "sh", "-c", "sleep 3; echo kept"]);
    n.running(&job);
    let good = std::fs::read_to_string(&n.paths.config).unwrap();
    let refused = |config: &str, says: &[&str]| {
        std::fs::write(&n.paths.config, config).unwrap();
        let o = n.cli(&["restart"]);
        let said = o.json["error"].as_str().unwrap_or_default().to_string();
        assert_eq!(o.code, 2, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
        assert!(
            said.contains("wouldn't start here")
                && said.contains(&format!("keeps running potluck {}", potluck::VERSION))
                && says.iter().all(|s| said.contains(s)),
            "{said}"
        );
        let nodes = n.cli_ok(&["nodes"]);
        assert_eq!(
            nodes["nodes"][0]["facts"]["node"],
            n.name.as_str(),
            "{nodes}"
        );
        assert!(alive(pid as i32));
    };
    refused(
        &format!("ram_gib = 4\n{good}"),
        &["node.toml", "duplicate key"],
    );
    let renamed = good.replace(&format!("name = \"{}\"", n.name), "name = \"renamed\"");
    assert_ne!(renamed, good);
    refused(
        &renamed,
        &[&format!("has been {}", n.name), "names it renamed"],
    );
    std::fs::write(&n.paths.config, &good).unwrap();
    let o = n.cli(&["restart"]);
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    let (code, j, _) = n.wait(&job);
    assert_eq!(code, 0, "{j}");
    assert_eq!(n.output(&job), "kept\n");
}

/// A machine whose node.toml names none keeps the name its daemon first ran under when the
/// hostname changes (here `name` is taken out of node.toml, so the hostname gives another name):
/// `potluck restart` and a plain start go on under that name. Before, both were refused, saying
/// node.toml now named the machine after its hostname.
#[test]
fn a_machine_named_by_its_hostname_keeps_its_name_when_the_hostname_changes() {
    let mut n = Node::new();
    let good = std::fs::read_to_string(&n.paths.config).unwrap();
    let unnamed = good.replace(&format!("name = \"{}\"\n", n.name), "");
    assert_ne!(unnamed, good);
    std::fs::write(&n.paths.config, &unnamed).unwrap();
    let named = |n: &Node| {
        let nodes = n.cli_ok(&["nodes"]);
        assert_eq!(
            nodes["nodes"][0]["facts"]["node"],
            n.name.as_str(),
            "{nodes}"
        );
    };
    let o = n.cli(&["restart"]);
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    named(&n);
    n.stop_daemon();
    n.start_daemon();
    named(&n);
}

/// A new binary that passes the check but can't start from the handover ends with an error code
/// (a service manager then starts it again plainly), saying why. Here the binary at the daemon's
/// path answers the check as if all were well, and node.toml renames the machine: the next
/// daemon goes by its own node.toml, as a plain start does, and refuses the name. Before, it took
/// the name in half and served on.
#[test]
fn a_binary_that_cannot_start_from_the_handover_exits_with_an_error() {
    let mut n = Node::new();
    let installed = n.dir.path().join("bin/potluck");
    std::fs::create_dir_all(installed.parent().unwrap()).unwrap();
    common::copy_binary(Path::new(BIN), &installed);
    n.stop_daemon();
    n.bin = installed.clone();
    n.start_daemon();
    let new = installed.with_file_name(".potluck.new");
    std::fs::write(
        &new,
        format!("#!/bin/sh\n[ \"$1\" = _handover-check ] && exit 0\nexec {BIN} \"$@\"\n"),
    )
    .unwrap();
    std::fs::set_permissions(&new, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&new, &installed).unwrap();
    let good = std::fs::read_to_string(&n.paths.config).unwrap();
    std::fs::write(
        &n.paths.config,
        good.replace(&format!("name = \"{}\"", n.name), "name = \"renamed\""),
    )
    .unwrap();
    let o = n.cli(&["restart"]);
    assert_eq!(o.code, 2, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
    let mut daemon = n.daemon.take().unwrap();
    let start = Instant::now();
    let status = loop {
        if let Some(s) = daemon.try_wait().unwrap() {
            break s;
        }
        if start.elapsed() > Duration::from_secs(30) {
            let _ = daemon.kill();
            let _ = daemon.wait();
            panic!(
                "the daemon serves on after a renaming handover:\n{}",
                n.daemon_log()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(status.code(), Some(2), "{}", n.daemon_log());
    assert!(
        n.daemon_log().contains("names it renamed"),
        "{}",
        n.daemon_log()
    );
}

/// b's node.toml moves it to another port, with an address naming that port, and `potluck
/// restart` applies it as a plain start would: b listens there, a reaches it there, and a job b
/// owns runs on a (a's mirror syncs from b at its new address). Before, b kept listening on the
/// listener handed over (the old port) while it gave members the new address, which nobody
/// listened on: a job b owned never ran on a ("can't reach b: ...: Connection refused").
#[test]
fn a_restart_in_place_listens_where_node_toml_now_says() {
    let a = Node::with(Opts {
        budget_zero: false,
        port: common::free_port(),
    });
    let old = common::free_port();
    let b = Node::with(Opts {
        budget_zero: false,
        port: old,
    });
    pair(&a, &b);
    let new = common::free_port();
    let config = std::fs::read_to_string(&b.paths.config).unwrap();
    let moved = config
        .replace(&format!("port = {old}\n"), &format!("port = {new}\n"))
        .replace(
            "address = \"127.0.0.1\"\n",
            &format!("address = \"127.0.0.1:{new}\"\n"),
        );
    assert_ne!(moved, config);
    std::fs::write(&b.paths.config, moved).unwrap();
    let o = b.cli(&["restart"]);
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, b.daemon_log());
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", new)).is_ok(),
        "nothing listens on the new port {new}:\n{}",
        b.daemon_log()
    );
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", old)).is_err(),
        "b still listens on its old port {old}"
    );
    let job = b.dispatch(&[
        "--timeout",
        "1m",
        "--on",
        &format!("node={}", a.name),
        "--",
        "sh",
        "-c",
        "echo ran on $POTLUCK_NODE",
    ]);
    let o = b.cli(&["wait", "--max-wait", "40s", &job]);
    assert_eq!(
        o.code,
        0,
        "{} {}\n{}\n{}",
        o.json,
        o.stderr,
        a.daemon_log(),
        b.daemon_log()
    );
    assert_eq!(b.output(&job), format!("ran on {}\n", a.name));
    assert_eq!(address_of(&a, &b.name), format!("127.0.0.1:{new}"));
}

/// Two members: a owns a job that runs on b. b restarts in place, then a does. Neither ever
/// lists the other as down, the job isn't queued again, its whole log reaches a, and `wait` on a
/// and on b (relayed to a, the owner) go on across both restarts. Right after its restart, each
/// lists the other as up: it took the mesh's view of the members over (a plain restart knows no
/// member as up until it hears from each).
#[test]
fn members_restarting_in_place_never_look_down_to_each_other() {
    let a = Node::with(Opts {
        budget_zero: false,
        port: common::free_port(),
    });
    let b = Node::with(Opts {
        budget_zero: false,
        port: common::free_port(),
    });
    pair(&a, &b);
    let lines: String = (1..=50).map(|i| format!("line {i}\n")).collect();
    let on_b = format!("node={}", b.name);
    let job = a.dispatch(&[
        "--timeout",
        "1m",
        "--on",
        &on_b,
        "--",
        "sh",
        "-c",
        "i=1; while [ $i -le 50 ]; do echo line $i; i=$((i+1)); sleep 0.1; done; exit 6",
    ]);
    let started = a
        .wait_for(&job, "running on b", 60, |j| {
            j.state == JobState::Running && j.node.as_deref() == Some(b.name.as_str())
        })
        .started_at;
    // Each watches the other: `nodes` in a loop.
    let watch = |of: &Node, other: &str| {
        use std::sync::atomic::AtomicBool;
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let (paths, other, stop2) = (of.paths.clone(), other.to_string(), stop.clone());
        let t = std::thread::spawn(move || {
            let mut seen = Vec::new();
            while !stop2.load(Ordering::SeqCst) {
                let r = Client::connect(&paths, ClientContext::default())
                    .and_then(|mut c| c.call(Request::Nodes));
                match r {
                    Ok(Response::Nodes { nodes }) => {
                        if !nodes.iter().any(|m| m.facts.node == other && m.alive) {
                            seen.push(format!("{other} not alive"));
                        }
                    }
                    other => seen.push(format!("{other:?}")),
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            seen
        });
        (stop, t)
    };
    let watchers = [watch(&a, &b.name), watch(&b, &a.name)];
    let waits = [
        spawn_cli(&a, &["wait", &job]),
        spawn_cli(&b, &["wait", &job]),
    ];
    std::thread::sleep(Duration::from_millis(1000));
    for (n, other) in [(&b, &a), (&a, &b)] {
        let o = n.cli(&["restart"]);
        assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, n.daemon_log());
        let nodes = n.cli_ok(&["nodes"]);
        assert!(
            nodes["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["facts"]["node"] == other.name.as_str() && m["alive"] == true),
            "right after its restart {} lists {} as not up: {nodes}",
            n.name,
            other.name
        );
        std::thread::sleep(Duration::from_millis(1000));
    }
    for w in waits {
        let o = w.wait_with_output().unwrap();
        let err = String::from_utf8_lossy(&o.stderr);
        assert_eq!(o.status.code(), Some(6), "{err}");
        assert_eq!(String::from_utf8_lossy(&o.stdout), lines, "{err}");
    }
    for (stop, t) in watchers {
        stop.store(true, Ordering::SeqCst);
        let seen = t.join().unwrap();
        assert!(seen.is_empty(), "{seen:?}");
    }
    let j = a.get(&job);
    assert_eq!(j.node.as_deref(), Some(b.name.as_str()), "{j:#?}");
    assert_eq!(j.started_at, started, "{j:#?}");
    assert_eq!(j.message.as_deref(), Some("exited with code 6"), "{j:#?}");
    assert_eq!(a.output(&job), lines);
    for n in [&a, &b] {
        let log = n.daemon_log();
        assert!(!log.contains(" is down"), "{log}");
        assert!(log.contains("restarted in place from potluck"), "{log}");
    }
}

/// Kept files a member sends after their owner restarted in place go into a directory of the
/// new binary's own. One that a stream cut by the exec left (same pid) is removed, not written
/// into, so no other run's half-delivered files reach the job's `keep/`.
#[test]
fn kept_files_after_a_restart_in_place_dont_mix_with_a_cut_streams() {
    let a = Node::with(Opts {
        budget_zero: false,
        port: common::free_port(),
    });
    let b = Node::with(Opts {
        budget_zero: false,
        port: common::free_port(),
    });
    pair(&a, &b);
    let on_b = format!("node={}", b.name);
    let job = a.dispatch(&[
        "--timeout",
        "1m",
        "--on",
        &on_b,
        "--keep",
        "made.txt",
        "--",
        "sh",
        "-c",
        "sleep 3; echo made > made.txt",
    ]);
    a.wait_for(&job, "running on b", 60, |j| {
        j.state == JobState::Running && j.node.as_deref() == Some(b.name.as_str())
    });
    // What streams cut by an exec in this process would leave.
    let pid = a.daemon.as_ref().unwrap().id();
    let dir = a.paths.job_dir(&job);
    for n in 0..4 {
        let left = dir.join(format!("keep.part-{pid}-{n}"));
        std::fs::create_dir_all(&left).unwrap();
        std::fs::write(left.join("stray.txt"), "another run's\n").unwrap();
    }
    let o = a.cli(&["restart"]);
    assert_eq!(o.code, 0, "{} {}\n{}", o.json, o.stderr, a.daemon_log());
    let (code, j, _) = a.wait(&job);
    assert_eq!(code, 0, "{j}");
    let keep = dir.join("keep");
    assert_eq!(
        std::fs::read_to_string(keep.join("made.txt")).unwrap(),
        "made\n"
    );
    assert!(
        !keep.join("stray.txt").exists(),
        "another run's file is kept"
    );
    let left: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("keep.part"))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}
