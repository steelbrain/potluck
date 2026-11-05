//! The daemon over its real socket, with real `potluck _supervise` processes, the real scheduler
//! and fakes for the executor (a process group per invocation), workspaces (plain directories) and
//! facts. Every test has its own temp home.

mod common;

use potluck::client::Client;
use potluck::config::{GpuConfig, NodeConfig, Paths};
use potluck::daemon::{
    Clock, Daemon, Options, Parts, RealScheduler, SystemClock, WorkspaceProvider,
};
use potluck::exec::{Executor, Launch, Sample};
use potluck::proto::{ClientContext, Request, Response};
use potluck::types::{
    Facts, Gpu, Handle, JobRecord, JobSpec, JobState, MAX_HOLD_MS, MIN_HOLD_MS, Millis, Reminder,
    RepoRef, WorkspaceId, WorkspaceRecord, WorkspaceState, now_ms,
};
use potluck::workspace::Action;
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const REPO: &str = "repo-0123abcd";
const BRANCH: &str = "agent-1";
const GPU: &str = "10de:2c19@0000:01:00.0";

// ---------------------------------------------------------------------------------------------
// Fakes

/// What the fake executor was asked to start.
#[derive(Clone, Debug)]
struct Launched {
    job: String,
    env: BTreeMap<String, String>,
    cwd: PathBuf,
    mem_max: u64,
    cpu_weight: u32,
    /// The potluck binary it was started with.
    potluck_bin: PathBuf,
}

/// Runs each invocation as `potluck _supervise` in a process group of its own and reaps it.
#[derive(Default)]
struct FakeExec {
    /// Supervisor pid -> whether it has exited (and was reaped).
    procs: Mutex<BTreeMap<u32, Arc<AtomicBool>>>,
    launched: Mutex<Vec<Launched>>,
    finished: Mutex<Vec<u32>>,
    cpu: AtomicU64,
    /// CPU time stops going up (for stalls).
    frozen: AtomicBool,
    /// CPU time goes up 1 ms every fifth sample (0.2% of a core at the fixture's 100 ms tick),
    /// as an idle thread that wakes to poll would.
    trickle: AtomicBool,
    samples: AtomicU64,
    /// The next sample reports the invocation over memory (and kills it, as the macOS monitor
    /// would).
    over_memory: AtomicBool,
    /// The next sample says a process was OOM-killed while under its limit, and kills the job.
    oom_killed: AtomicBool,
    /// The next sample finds too many processes (macOS's monitor) and kills it.
    over_tasks: AtomicBool,
    /// The next sample reports the invocation over memory as Linux does once its unit has
    /// ended: no memory in use, the kernel's peak at the limit (and kills it).
    over_memory_at_peak: AtomicBool,
    /// Signals are dropped (processes that won't die).
    deaf: AtomicBool,
    /// `finish` fails (a crash report that can't be moved, a unit systemd won't answer for).
    fail_finish: AtomicBool,
    /// `start` fails (systemd refuses the unit, the supervisor can't be spawned).
    fail_start: AtomicBool,
    /// Launch name -> the handle `start` gave, for `find`.
    names: Mutex<BTreeMap<String, Handle>>,
    /// Every sample tracks one more process (as macOS's monitor does for a job that keeps
    /// starting processes).
    churn: AtomicBool,
    /// How many processes the handle of the latest sample tracked, as the daemon keeps it.
    sampled_tracked: AtomicUsize,
    /// Sampling stacks takes this long (a debugger attaching to many processes).
    stacks_ms: AtomicU64,
    /// `find` goes by the status file, as Linux does for a unit systemd unloaded after it exited
    /// 0: only one written since `since` says the start started something.
    find_by_status: AtomicBool,
    /// `find` fails this many more times (a user manager too busy to answer), then answers.
    fail_find: Mutex<usize>,
    /// `finish` waits while this is set (a slow cleanup), after saying so in `in_finish`.
    hold_finish: AtomicBool,
    in_finish: AtomicBool,
    /// Where crash reports are written (ReportCrash's directory): `finish` moves what is there
    /// by then into the job's crash dir, as the macOS executor does.
    reports: Mutex<Option<PathBuf>>,
}

fn pid_of(h: &Handle) -> u32 {
    match h {
        Handle::Process { pid, .. } => *pid,
        other => panic!("{other:?}"),
    }
}

impl FakeExec {
    fn alive(&self, pid: u32) -> bool {
        self.procs
            .lock()
            .unwrap()
            .get(&pid)
            .is_some_and(|e| !e.load(Ordering::SeqCst))
    }

    /// Signals the supervisor's group and its command's (a group of its own).
    fn kill_group(&self, pid: u32, sig: i32) {
        // Only while the leader is unreaped: its pid (the group id) can't have been reused, nor
        // can its child's, which it hasn't reaped either.
        if self.alive(pid) {
            for g in common::supervisor_groups(pid) {
                unsafe { libc::kill(-(g as i32), sig) };
            }
        }
    }

    fn launched(&self, job: &str) -> Launched {
        self.launched
            .lock()
            .unwrap()
            .iter()
            .find(|l| l.job == job)
            .cloned()
            .unwrap_or_else(|| panic!("{job} was not launched"))
    }

    fn kill_all(&self) {
        let pids: Vec<u32> = self.procs.lock().unwrap().keys().copied().collect();
        for pid in pids {
            self.kill_group(pid, libc::SIGKILL);
        }
    }
}

impl Executor for FakeExec {
    fn start(&self, l: &Launch) -> anyhow::Result<Handle> {
        if self.fail_start.load(Ordering::SeqCst) {
            anyhow::bail!("systemd said no");
        }
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&l.log)?;
        let mut cmd = Command::new(&l.potluck_bin);
        cmd.arg("_supervise");
        if l.limits.log_max_bytes > 0 {
            cmd.arg("--log-max").arg(l.limits.log_max_bytes.to_string());
        }
        let mut child = cmd
            .arg(&l.status)
            .arg("--")
            .args(&l.job.spec.cmd)
            .current_dir(&l.cwd)
            .env_clear()
            .envs(&l.env)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0)
            .spawn()?;
        let pid = child.id();
        let exited = Arc::new(AtomicBool::new(false));
        self.procs.lock().unwrap().insert(pid, exited.clone());
        std::thread::spawn(move || {
            let _ = child.wait();
            exited.store(true, Ordering::SeqCst);
        });
        self.launched.lock().unwrap().push(Launched {
            job: l.job.id.clone(),
            env: l.env.clone(),
            cwd: l.cwd.clone(),
            mem_max: l.limits.mem_max_bytes,
            cpu_weight: l.limits.cpu_weight,
            potluck_bin: l.potluck_bin.clone(),
        });
        let handle = Handle::Process {
            pid,
            start_time: 0,
            tracked: vec![],
            gone: Vec::new(),
            lease: String::new(),
            tasks_max: 0,
            boot: String::new(),
        };
        self.names
            .lock()
            .unwrap()
            .insert(l.name.clone(), handle.clone());
        Ok(handle)
    }

    fn sample(&self, h: &mut Handle, mem_max: u64) -> anyhow::Result<Sample> {
        let pid = pid_of(h);
        let alive = self.alive(pid);
        let pids = if alive { vec![pid] } else { vec![] };
        if let Handle::Process { tracked, .. } = h {
            if alive && self.churn.load(Ordering::SeqCst) {
                let n = tracked.len() as u32;
                tracked.push((1_000_000 + n, 0));
            }
            self.sampled_tracked.store(tracked.len(), Ordering::SeqCst);
        }
        if alive && self.over_memory_at_peak.swap(false, Ordering::SeqCst) {
            self.kill_group(pid, libc::SIGKILL);
            return Ok(Sample {
                pids,
                cpu_ms: self.cpu.load(Ordering::SeqCst),
                mem_bytes: 0,
                peak_mem_bytes: mem_max,
                over_memory: true,
                oom_killed: false,
                over_tasks: None,
            });
        }
        if alive && self.over_tasks.swap(false, Ordering::SeqCst) {
            self.kill_group(pid, libc::SIGKILL);
            return Ok(Sample {
                pids,
                cpu_ms: self.cpu.load(Ordering::SeqCst),
                mem_bytes: 1 << 20,
                peak_mem_bytes: 0,
                over_memory: false,
                oom_killed: false,
                over_tasks: Some((5334, 5333)),
            });
        }
        if alive && self.oom_killed.swap(false, Ordering::SeqCst) {
            self.kill_group(pid, libc::SIGKILL);
            return Ok(Sample {
                pids,
                cpu_ms: self.cpu.load(Ordering::SeqCst),
                mem_bytes: 300 << 20,
                peak_mem_bytes: 0,
                over_memory: false,
                oom_killed: true,
                over_tasks: None,
            });
        }
        if alive && self.over_memory.swap(false, Ordering::SeqCst) {
            self.kill_group(pid, libc::SIGKILL);
            return Ok(Sample {
                pids,
                cpu_ms: self.cpu.load(Ordering::SeqCst),
                mem_bytes: 5 << 30,
                peak_mem_bytes: 0,
                over_memory: true,
                oom_killed: false,
                over_tasks: None,
            });
        }
        let n = self.samples.fetch_add(1, Ordering::SeqCst);
        let cpu = if self.trickle.load(Ordering::SeqCst) {
            if n.is_multiple_of(5) {
                self.cpu.fetch_add(1, Ordering::SeqCst) + 1
            } else {
                self.cpu.load(Ordering::SeqCst)
            }
        } else if self.frozen.load(Ordering::SeqCst) {
            self.cpu.load(Ordering::SeqCst)
        } else {
            self.cpu.fetch_add(10, Ordering::SeqCst) + 10
        };
        Ok(Sample {
            pids,
            cpu_ms: cpu,
            mem_bytes: 1 << 20,
            peak_mem_bytes: 0,
            over_memory: false,
            oom_killed: false,
            over_tasks: None,
        })
    }

    fn signal(&self, h: &Handle, sig: i32) -> anyhow::Result<()> {
        if !self.deaf.load(Ordering::SeqCst) {
            self.kill_group(pid_of(h), sig);
        }
        Ok(())
    }

    fn stacks(&self, h: &Handle) -> anyhow::Result<String> {
        std::thread::sleep(Duration::from_millis(self.stacks_ms.load(Ordering::SeqCst)));
        Ok(format!("fake stacks of {}", pid_of(h)))
    }

    fn finish(&self, h: &Handle, crash_dir: &Path, _since: Millis) -> anyhow::Result<()> {
        self.in_finish.store(true, Ordering::SeqCst);
        while self.hold_finish.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        if self.fail_finish.load(Ordering::SeqCst) {
            anyhow::bail!("could not clean up");
        }
        self.finished.lock().unwrap().push(pid_of(h));
        // As the executors do: crash/ is made if need be, but not the job's directory (gone when
        // the job was deleted before the late collection).
        if let Some(dir) = self.reports.lock().unwrap().as_ref()
            && crash_dir.parent().is_some_and(Path::is_dir)
        {
            let _ = std::fs::create_dir(crash_dir);
            for e in std::fs::read_dir(dir)?.flatten() {
                std::fs::rename(e.path(), crash_dir.join(e.file_name()))?;
            }
        }
        Ok(())
    }

    fn find(&self, name: &str, status: &Path, since: Millis) -> anyhow::Result<Option<Handle>> {
        {
            let mut failing = self.fail_find.lock().unwrap();
            if *failing > 0 {
                *failing -= 1;
                anyhow::bail!("systemctl show timed out");
            }
        }
        if self.find_by_status.load(Ordering::SeqCst) {
            let written = std::fs::metadata(status)
                .and_then(|m| m.modified())
                .is_ok_and(|t| {
                    t.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as Millis >= since
                });
            if !written {
                return Ok(None);
            }
        }
        Ok(self.names.lock().unwrap().get(name).cloned())
    }
}

/// The machine's clocks, with steps a test can take: the wall clock set forward or back (as NTP
/// or someone setting the date would), time passing (all three clocks move on) or passing while
/// the machine sleeps (the awake clock stays).
#[derive(Default)]
struct SteppedClock {
    wall_step: std::sync::atomic::AtomicI64,
    boot_step: AtomicU64,
    awake_step: AtomicU64,
}

impl SteppedClock {
    fn step_wall(&self, ms: i64) {
        self.wall_step.fetch_add(ms, Ordering::SeqCst);
    }

    fn pass(&self, ms: u64) {
        self.awake_step.fetch_add(ms, Ordering::SeqCst);
        self.sleep(ms);
    }

    fn sleep(&self, ms: u64) {
        self.boot_step.fetch_add(ms, Ordering::SeqCst);
        self.step_wall(ms as i64);
    }
}

impl Clock for SteppedClock {
    fn wall_ms(&self) -> Millis {
        now_ms().saturating_add_signed(self.wall_step.load(Ordering::SeqCst))
    }
    fn boot_ms(&self) -> u64 {
        SystemClock.boot_ms() + self.boot_step.load(Ordering::SeqCst)
    }
    fn awake_ms(&self) -> u64 {
        SystemClock.awake_ms() + self.awake_step.load(Ordering::SeqCst)
    }
    fn boot_id(&self) -> String {
        SystemClock.boot_id()
    }
}

/// Workspaces as plain directories holding the commit in a file; mirrors and pins as records.
struct FakeWs {
    paths: Paths,
    synced: Mutex<Vec<(String, String)>>,
    pins: Mutex<BTreeSet<(String, String)>>,
    removed: Mutex<Vec<String>>,
    /// Removals fail, leaving the files (as a removal cut short by a restart does).
    fail_remove: AtomicBool,
    /// Removals move the tree aside to `<path>.removing`, as the real one does, and fail to delete
    /// it there (as with files a job made that this user can't delete); the TMPDIR goes.
    stuck_aside: AtomicBool,
    /// Seeding waits while this is set.
    hold_create: AtomicBool,
    /// No hold after a job: workspaces go idle at the next tick.
    no_hold: AtomicBool,
    /// The lifecycle policy removes this workspace once it is idle (as when over the budget),
    /// first waiting while `pause_tick` is set, after saying so in `in_tick`.
    remove_when_idle: Mutex<Option<String>>,
    pause_tick: AtomicBool,
    in_tick: AtomicBool,
}

impl FakeWs {
    fn pinned(&self, name: &str) -> bool {
        self.pins.lock().unwrap().iter().any(|(_, n)| n == name)
    }
}

/// Waits up to 10 s for `f`. A job is done for `wait` before the daemon releases its pins and
/// removes a half-made workspace, so tests wait for those.
fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timed out waiting until {what}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl WorkspaceProvider for FakeWs {
    fn sync_mirror(&self, repo_id: &str, url: &str) -> anyhow::Result<Option<String>> {
        if url.contains("panic") {
            panic!("the sync blew up");
        }
        if url.contains("unreachable") {
            anyhow::bail!("no such repository");
        }
        self.synced
            .lock()
            .unwrap()
            .push((repo_id.into(), url.into()));
        if url.contains("diverged") {
            return Ok(Some(format!(
                "the mirror of {repo_id} had changed since its last sync (test)"
            )));
        }
        Ok(None)
    }
    fn fetch_pin(&self, repo_id: &str, _url: &str, commit: &str, name: &str) -> anyhow::Result<()> {
        if commit.starts_with("dead") {
            anyhow::bail!("not our ref {commit}");
        }
        self.pin(repo_id, commit, name)
    }
    fn has_commit(&self, _repo_id: &str, commit: &str) -> bool {
        // "f37c...": on no branch or tag, so only a fetch of the commit itself brings it.
        !commit.starts_with("dead") && !commit.starts_with("f37c")
    }
    fn pin(&self, repo_id: &str, _commit: &str, name: &str) -> anyhow::Result<()> {
        self.pins
            .lock()
            .unwrap()
            .insert((repo_id.into(), name.into()));
        Ok(())
    }
    fn unpin(&self, repo_id: &str, name: &str) -> anyhow::Result<()> {
        self.pins
            .lock()
            .unwrap()
            .remove(&(repo_id.to_string(), name.to_string()));
        Ok(())
    }
    fn pins(&self) -> anyhow::Result<Vec<(String, String)>> {
        Ok(self.pins.lock().unwrap().iter().cloned().collect())
    }
    fn create(&self, id: &WorkspaceId, repo: &RepoRef) -> anyhow::Result<PathBuf> {
        while self.hold_create.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        if repo.commit.starts_with("bad") {
            anyhow::bail!("checkout failed");
        }
        let root = self.paths.workspace(id);
        std::fs::create_dir_all(root.join("data/sub"))?;
        std::fs::write(root.join("COMMIT"), &repo.commit)?;
        std::fs::write(root.join("data/a.txt"), b"alpha")?;
        std::fs::write(root.join("data/sub/b.bin"), [0u8, 1, 2, 255])?;
        std::os::unix::fs::symlink("/etc/hosts", root.join("data/escape"))?;
        std::fs::create_dir_all(self.paths.workspace_tmp(id))?;
        // Like the real one: creating pins its commits under the workspace id, removing unpins.
        self.pin(&repo.id, &repo.commit, id)?;
        for sub in &repo.submodules {
            self.pin(&sub.repo_id, &sub.commit, id)?;
        }
        Ok(root)
    }
    fn verify(&self, root: &Path, repo: &RepoRef) -> anyhow::Result<()> {
        let at = std::fs::read_to_string(root.join("COMMIT"))?;
        anyhow::ensure!(at == repo.commit, "HEAD is {at}, not {}", repo.commit);
        Ok(())
    }
    fn remove(&self, record: &WorkspaceRecord) -> anyhow::Result<()> {
        if self.fail_remove.load(Ordering::SeqCst) {
            anyhow::bail!("removal failed");
        }
        // Like the real one: a tree moved aside by an earlier removal goes too.
        let aside = potluck::workspace::aside(&record.path);
        if let Some(aside) = &aside
            && self.stuck_aside.load(Ordering::SeqCst)
        {
            let _ = std::fs::rename(&record.path, aside);
            let _ = std::fs::remove_dir_all(self.paths.workspace_tmp(&record.id));
            anyhow::bail!("can't remove {}: Operation not permitted", aside.display());
        }
        for p in aside.iter().chain([&record.path]) {
            match std::fs::remove_dir_all(p) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
                _ => {}
            }
        }
        self.pins.lock().unwrap().retain(|(_, n)| n != &record.id);
        self.removed.lock().unwrap().push(record.id.clone());
        Ok(())
    }
    fn size(&self, _root: &Path) -> anyhow::Result<u64> {
        Ok(4096)
    }
    // The lifecycle policy belongs to the workspace module; these stand-ins follow its rules.
    fn tick(&self, now: Millis, records: &[WorkspaceRecord], _budget: u64) -> Vec<Action> {
        let mut actions: Vec<Action> = records
            .iter()
            .filter(|w| w.state == WorkspaceState::Held && w.held_until <= now)
            .map(|w| Action::MakeIdle(w.id.clone()))
            .collect();
        let target = self.remove_when_idle.lock().unwrap().clone();
        if let Some(w) = target
            && records
                .iter()
                .any(|r| r.id == w && r.state == WorkspaceState::Idle)
        {
            self.in_tick.store(true, Ordering::SeqCst);
            while self.pause_tick.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(10));
            }
            actions.push(Action::Remove(w));
        }
        actions
    }
    fn hold_until(&self, ended_at: Millis, requested_ms: Option<u64>, current: Millis) -> Millis {
        if self.no_hold.load(Ordering::SeqCst) {
            return ended_at;
        }
        let hold = requested_ms.unwrap_or(0).clamp(MIN_HOLD_MS, MAX_HOLD_MS);
        current.max(ended_at + hold)
    }
    fn reminders(
        &self,
        now: Millis,
        records: &[WorkspaceRecord],
        repo_id: &str,
        branch: &str,
    ) -> Vec<Reminder> {
        records
            .iter()
            .filter(|w| w.repo_id == repo_id && w.branch.as_deref() == Some(branch))
            .filter(|w| w.held_until > now && w.state != WorkspaceState::Idle)
            .map(|w| Reminder {
                workspace: w.id.clone(),
                node: w.node.clone(),
                commit: w.commit.clone(),
                state: w.state,
                remaining_ms: w.held_until - now,
                size_bytes: w.size_bytes,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------------------------
// Fixture

struct Fixture {
    _dir: tempfile::TempDir,
    paths: Paths,
    config: NodeConfig,
    opts: Options,
    exec: Arc<FakeExec>,
    ws: Arc<FakeWs>,
    login_env: BTreeMap<String, String>,
    clock: Arc<SteppedClock>,
    daemon: Option<Daemon>,
}

fn facts() -> Facts {
    Facts {
        node: "testnode".into(),
        os: std::env::consts::OS.into(),
        os_version: "1".into(),
        arch: std::env::consts::ARCH.into(),
        kernel: "k".into(),
        cpu_model: "fake cpu".into(),
        cpus: 8,
        ram_mib: 16 * 1024,
        gpus: vec![Gpu {
            id: GPU.into(),
            vendor: "10de".into(),
            device: "2c19".into(),
            name: "Fake GPU".into(),
            alias: Some("fake".into()),
            ..Default::default()
        }],
        display: false,
        version: potluck::VERSION.into(),
    }
}

impl Fixture {
    fn new() -> Fixture {
        Fixture::with_home(None)
    }

    /// With its state directory at `home` (default: in its own temp dir).
    fn with_home(home: Option<PathBuf>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: home.unwrap_or_else(|| dir.path().join("home")),
            config: dir.path().join("node.toml"),
        };
        let config = NodeConfig {
            ram_gib: Some(8.0),
            gpu: vec![GpuConfig {
                matches: "nvidia".into(),
                alias: Some("fake".into()),
                env: [("FAKE_GPU_HELD".to_string(), "1".to_string())].into(),
            }],
            ..Default::default()
        };
        let opts = Options {
            potluck_bin: PathBuf::from(env!("CARGO_BIN_EXE_potluck")),
            tick_ms: 100,
            kill_grace_ms: 600,
            stall_ms: 0,
            retention_ms: 14 * 86_400_000,
            facts_refresh_ms: 60_000,
            idle_ms: 600_000,
        };
        let login_env: BTreeMap<String, String> = [
            ("PATH", "/usr/bin:/bin:/usr/sbin:/sbin"),
            ("HOME", dir.path().to_str().unwrap()),
            ("FROM_LOGIN", "login"),
            ("OVERRIDDEN", "login"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        Fixture {
            exec: Arc::new(FakeExec::default()),
            ws: Arc::new(FakeWs {
                paths: paths.clone(),
                synced: Mutex::default(),
                pins: Mutex::default(),
                removed: Mutex::default(),
                fail_remove: AtomicBool::new(false),
                stuck_aside: AtomicBool::new(false),
                hold_create: AtomicBool::new(false),
                no_hold: AtomicBool::new(false),
                remove_when_idle: Mutex::default(),
                pause_tick: AtomicBool::new(false),
                in_tick: AtomicBool::new(false),
            }),
            paths,
            config,
            opts,
            login_env,
            clock: Arc::new(SteppedClock::default()),
            daemon: None,
            _dir: dir,
        }
    }

    fn start(mut self) -> Fixture {
        self.restart();
        self
    }

    fn restart(&mut self) {
        assert!(self.daemon.is_none());
        let parts = Parts {
            executor: self.exec.clone(),
            workspaces: self.ws.clone(),
            scheduler: Arc::new(RealScheduler),
            facts: Box::new(|| Ok(facts())),
            login_env: self.login_env.clone(),
            clock: self.clock.clone(),
        };
        self.daemon =
            Some(Daemon::start(&self.paths, &self.config, parts, self.opts.clone()).unwrap());
    }

    fn stop(&mut self) {
        self.daemon.take().unwrap().stop();
    }

    /// A fixture on a copy of this one's store as it stands now: what a daemon killed at this
    /// moment leaves for the next one.
    fn crash_copy(&self) -> Fixture {
        let copy = Fixture::new();
        std::fs::create_dir_all(&copy.paths.home).unwrap();
        let db = rusqlite::Connection::open(self.paths.db()).unwrap();
        db.execute("VACUUM INTO ?1", [copy.paths.db().to_str().unwrap()])
            .unwrap();
        copy
    }

    fn client(&self) -> Client {
        Client::connect(
            &self.paths,
            ClientContext {
                repo_id: Some(REPO.into()),
                branch: Some(BRANCH.into()),
                job: None,
                lease_token: None,
            },
        )
        .unwrap()
    }

    fn dispatch(&self, spec: JobSpec) -> String {
        match self.client().call(Request::Dispatch {
            spec: Box::new(spec),
        }) {
            Ok(Response::Dispatched { job, .. }) => job,
            other => panic!("{other:?}"),
        }
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

    /// Waits (through the daemon's long poll) for the job to end.
    fn wait(&self, job: &str) -> JobRecord {
        match self
            .client()
            .call(Request::Wait {
                job: job.into(),
                timeout_ms: Some(30_000),
            })
            .unwrap()
        {
            Response::Job { job } if job.state.is_done() => *job,
            other => panic!("{other:?}"),
        }
    }

    fn wait_for(&self, job: &str, what: &str, pred: impl Fn(&JobRecord) -> bool) -> JobRecord {
        let start = Instant::now();
        loop {
            let j = self.get(job);
            if pred(&j) {
                return j;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "{job}: never {what}: {j:#?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn log(&self, job: &str) -> String {
        std::fs::read_to_string(self.paths.job_dir(job).join("output.log")).unwrap_or_default()
    }

    fn workspace(&self, id: &str) -> WorkspaceRecord {
        match self.client().call(Request::Status).unwrap() {
            Response::Status { status } => status
                .workspaces
                .into_iter()
                .find(|w| w.id == id)
                .unwrap_or_else(|| panic!("no workspace {id}")),
            other => panic!("{other:?}"),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(d) = self.daemon.take() {
            d.stop();
        }
        self.exec.kill_all();
    }
}

fn spec(cmd: &str) -> JobSpec {
    JobSpec {
        key: None,
        labels: BTreeMap::new(),
        priority: 3,
        request: potluck::types::Request {
            ram_mib: 512,
            cpu: 1,
            gpus: vec![],
            exclusive: false,
        },
        on: BTreeMap::new(),
        repo: RepoRef {
            id: REPO.into(),
            source: PathBuf::from("/src/project/.git"),
            commit: "c0ffee".repeat(6) + "abcd",
            branch: Some(BRANCH.into()),
            submodules: vec![potluck::types::SubmoduleRef {
                path: "vendor/lib".into(),
                repo_id: "lib-89abcdef".into(),
                source: PathBuf::from("/src/project/.git/modules/vendor/lib"),
                commit: "1".repeat(40),
            }],
            carried: vec![],
        },
        workspace: None,
        advance: false,
        cmd: vec!["sh".into(), "-c".into(), cmd.into()],
        env: BTreeMap::new(),
        unset_env: Default::default(),
        timeout_ms: 60_000,
        hold_ms: None,
        keep: vec![],
        rerun_if_lost: true,
        kill_on_stall: false,
        recipe: None,
        submitted_from: String::new(),
        pools: Vec::new(),
        auth: None,
    }
}

/// The ids [`spec`]'s repository and its submodule get when `owner` dispatches it.
fn owned_ids(owner: &str) -> (String, String) {
    let top = potluck::types::owned_repo_id(owner, REPO);
    let lib = potluck::gitinfo::submodule_repo_id(&top, "vendor/lib");
    (top, lib)
}

// ---------------------------------------------------------------------------------------------
// Tests

/// A huge `--cpu` (only 0 is refused, and cpu never blocks admission) gets the largest weight,
/// not one that wrapped around.
#[test]
fn a_huge_cpu_request_gets_the_largest_weight() {
    let f = Fixture::new().start();
    let mut s = spec("true");
    s.request.cpu = 42_949_673;
    let id = f.dispatch(s);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    assert_eq!(f.exec.launched(&id).cpu_weight, 10_000);
}

#[test]
fn dispatch_and_wait_pass_the_exit_code_through() {
    let f = Fixture::new().start();
    let mut c = f.client();
    assert!(
        matches!(c.call(Request::Ping).unwrap(), Response::Pong { node, .. } if node == "testnode")
    );

    let mut s = spec("echo out; echo err >&2; exit 7");
    s.env.insert("FROM_SPEC".into(), "spec".into());
    s.env.insert("OVERRIDDEN".into(), "spec".into());
    s.hold_ms = Some(30 * 60_000);
    let id = f.dispatch(s);
    assert_eq!(id, "testnode-1");
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Failed);
    assert_eq!(j.exit_code_for_client(), 7);
    assert_eq!(j.message.as_deref(), Some("exited with code 7"));
    let log = f.log(&id);
    assert!(log.contains("out\n") && log.contains("err\n"), "{log}");
    let dir = f.paths.job_dir(&id);
    assert!(dir.join("status.json").is_file() && dir.join("keep").is_dir());

    // The environment: login, then the spec's, then potluck's own.
    let ws = j.workspace.clone().unwrap();
    assert_eq!(ws, "testnode-w1");
    let l = f.exec.launched(&id);
    assert_eq!(l.env["FROM_LOGIN"], "login");
    assert_eq!(l.env["FROM_SPEC"], "spec");
    assert_eq!(l.env["OVERRIDDEN"], "spec");
    assert_eq!(l.env["POTLUCK_JOB"], id);
    assert_eq!(l.env["POTLUCK_WORKSPACE"], ws);
    assert_eq!(l.env["POTLUCK_NODE"], "testnode");
    assert_eq!(l.env["POTLUCK_LEASE"], j.lease.as_ref().unwrap().token);
    assert_eq!(PathBuf::from(&l.env["TMPDIR"]), f.paths.workspace_tmp(&ws));
    for k in l.env.keys() {
        assert!(
            !k.starts_with("CARGO") && k != "RUSTFLAGS" && k != "RUSTC_WRAPPER",
            "{k} set"
        );
    }
    assert_eq!(l.cwd, f.paths.workspace(&ws));
    assert_eq!(l.mem_max, 512 << 20);
    assert_eq!(l.cpu_weight, 100);

    // Mirrors synced from the sources (repository and submodule), under ids salted with this
    // machine's name; pins: the job's released, the workspace's kept.
    let (top, lib) = owned_ids("testnode");
    assert_eq!(j.spec.repo.id, top);
    assert_eq!(j.spec.repo.submodules[0].repo_id, lib);
    assert_eq!(f.workspace(&ws).repo_id, top);
    let synced = f.ws.synced.lock().unwrap().clone();
    assert_eq!(
        synced,
        [
            (top.clone(), "/src/project/.git".to_string()),
            (
                lib.clone(),
                "/src/project/.git/modules/vendor/lib".to_string()
            )
        ]
    );
    wait_until("the job's pins are released", || !f.ws.pinned(&id));
    assert_eq!(
        f.ws.pins.lock().unwrap().clone(),
        [(lib, ws.clone()), (top, ws.clone())].into()
    );
    assert_eq!(f.exec.finished.lock().unwrap().len(), 1);

    // Held for the hold it asked for, and reminded on every response.
    let w = f.workspace(&ws);
    assert_eq!(w.state, WorkspaceState::Held);
    assert_eq!(w.pending, 0);
    let left = w.held_until - now_ms();
    assert!(left > 29 * 60_000 && left <= 30 * 60_000, "{left}");
    c.call(Request::Ping).unwrap();
    assert_eq!(c.reminders.len(), 1);
    assert_eq!(c.reminders[0].workspace, ws);

    // A command that succeeds.
    let id = f.dispatch(spec("true"));
    let j = f.wait(&id);
    assert_eq!(
        (j.state, j.exit_code_for_client()),
        (JobState::Succeeded, 0)
    );
    assert_eq!(j.workspace.as_deref(), Some("testnode-w2"));
}

/// A workspace made before repository ids were salted with the owner (left by an older version):
/// it is still reminded of, a dispatch into it keeps its unsalted ids (its mirrors and pins have
/// those) and runs, and a dispatch without it gets the salted ids.
#[test]
fn a_workspace_from_before_ids_were_salted_keeps_working() {
    let f = Fixture::new();
    let s = spec("true");
    let w = "testnode-w7".to_string();
    let root = f.ws.create(&w, &s.repo).unwrap();
    let now = now_ms();
    potluck::store::Store::open(&f.paths.db())
        .unwrap()
        .put_workspace(&WorkspaceRecord {
            id: w.clone(),
            node: "testnode".into(),
            repo_id: REPO.into(),
            commit: s.repo.commit.clone(),
            branch: Some(BRANCH.into()),
            path: root,
            state: WorkspaceState::Held,
            created_at: now - 60_000,
            last_used_at: now - 60_000,
            held_until: now + 3_600_000,
            pending: 0,
            size_bytes: Some(4096),
            advanced_by: None,
        })
        .unwrap();
    let f = f.start();
    let mut c = f.client();
    c.call(Request::Ping).unwrap();
    let held: Vec<&str> = c.reminders.iter().map(|r| r.workspace.as_str()).collect();
    assert_eq!(held, [w.as_str()]);

    let mut into = spec("true");
    into.workspace = Some(w.clone());
    let id = f.dispatch(into);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    assert_eq!(j.workspace.as_deref(), Some(w.as_str()));
    assert_eq!(j.spec.repo.id, REPO);
    assert_eq!(j.spec.repo.submodules[0].repo_id, "lib-89abcdef");
    assert_eq!(f.workspace(&w).repo_id, REPO);
    assert_eq!(f.ws.synced.lock().unwrap()[0].0, REPO);

    let id = f.dispatch(spec("true"));
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    assert_eq!(j.spec.repo.id, owned_ids("testnode").0);
    let fresh = j.workspace.unwrap();
    assert_ne!(fresh, w);
    // Both are reminded of: the caller names the repository by its unsalted id.
    c.call(Request::Ping).unwrap();
    let mut held: Vec<String> = c.reminders.iter().map(|r| r.workspace.clone()).collect();
    held.sort();
    assert_eq!(held, [fresh, w]);
}

/// A carried repository (`--carry`): its mirror synced from its source under the id a dispatch
/// from it gets here (salted, also in a dispatch into a workspace from before salting), its commit
/// pinned for the job until it ends and not for the workspace, and the job told where that mirror
/// is in POTLUCK_MIRROR_<name>, which no env sets. A raw dispatch with a bad name, or one
/// repository twice, is refused.
#[test]
fn a_carried_repository_is_synced_pinned_and_named_to_the_job() {
    let f = Fixture::new();
    let s = spec("true");
    let old = "testnode-w7".to_string();
    let root = f.ws.create(&old, &s.repo).unwrap();
    let now = now_ms();
    potluck::store::Store::open(&f.paths.db())
        .unwrap()
        .put_workspace(&WorkspaceRecord {
            id: old.clone(),
            node: "testnode".into(),
            repo_id: REPO.into(),
            commit: s.repo.commit.clone(),
            branch: Some(BRANCH.into()),
            path: root,
            state: WorkspaceState::Held,
            created_at: now - 60_000,
            last_used_at: now - 60_000,
            held_until: now + 3_600_000,
            pending: 0,
            size_bytes: Some(4096),
            advanced_by: None,
        })
        .unwrap();
    let f = f.start();
    let carried = |name: &str, id: &str| potluck::types::CarriedRef {
        name: name.into(),
        repo_id: id.into(),
        source: PathBuf::from(format!("/src/{name}/.git")),
        commit: "2".repeat(40),
    };
    let dep = potluck::types::owned_repo_id("testnode", "dep-01234567");
    for into in [None, Some(old.clone())] {
        let mut s = spec("sleep 0.5");
        s.workspace = into.clone();
        s.repo.carried = vec![carried("dep", "dep-01234567")];
        s.env
            .insert("POTLUCK_MIRROR_dep".into(), "/elsewhere".into());
        s.env
            .insert("POTLUCK_MIRROR_other".into(), "/elsewhere".into());
        let id = f.dispatch(s);
        f.wait_for(&id, "running", |j| j.state == JobState::Running);
        assert!(
            f.ws.pins
                .lock()
                .unwrap()
                .contains(&(dep.clone(), id.clone())),
            "{into:?}: {:?}",
            f.ws.pins.lock().unwrap()
        );
        let j = f.wait(&id);
        assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
        assert_eq!(j.spec.repo.carried[0].repo_id, dep, "{into:?}");
        assert!(
            f.ws.synced
                .lock()
                .unwrap()
                .contains(&(dep.clone(), "/src/dep/.git".to_string())),
            "{:?}",
            f.ws.synced.lock().unwrap()
        );
        let env = &f.exec.launched(&id).env;
        assert_eq!(
            PathBuf::from(&env["POTLUCK_MIRROR_dep"]),
            f.paths.mirror(&dep)
        );
        assert!(!env.contains_key("POTLUCK_MIRROR_other"), "{env:?}");
        wait_until("the job's pins are released", || !f.ws.pinned(&id));
        assert!(
            !f.ws.pins.lock().unwrap().iter().any(|(r, _)| r == &dep),
            "{:?}",
            f.ws.pins.lock().unwrap()
        );
    }

    let mut c = f.client();
    let mut call = |carried: Vec<potluck::types::CarriedRef>| {
        let mut s = spec("true");
        s.repo.carried = carried;
        c.call(Request::Dispatch { spec: Box::new(s) })
            .unwrap_err()
            .to_string()
    };
    let e = call(vec![carried("de-p", "dep-01234567")]);
    assert!(
        e.contains("a name holds only ASCII letters, digits and _"),
        "{e}"
    );
    let e = call(vec![
        carried("dep", "dep-01234567"),
        carried("dep", "dep-89abcdef"),
    ]);
    assert!(e.contains("two carried repositories are named dep"), "{e}");
    let e = call(vec![
        carried("a", "dep-01234567"),
        carried("b", "dep-01234567"),
    ]);
    assert!(e.contains("carried twice"), "{e}");
    let e = call(vec![carried("own", REPO)]);
    assert!(e.contains("is the job's own"), "{e}");
}

/// A mirror the dispatch synced had diverged from its source (or was recloned): the job's log
/// and `why` say so, naming the machine, besides the daemon's log.
#[test]
fn a_warning_from_a_sync_shows_in_the_jobs_log_and_why() {
    let f = Fixture::new().start();
    let mut s = spec("echo ran");
    s.repo.source = PathBuf::from("/src/diverged/.git");
    let id = f.dispatch(s);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    let (top, _) = owned_ids("testnode");
    let note = format!("testnode: the mirror of {top} had changed since its last sync (test)");
    assert_eq!(j.notes, std::slice::from_ref(&note));
    let log = f.log(&id);
    assert!(
        log.starts_with(&format!("--- potluck: warning: {note} ---\n")) && log.contains("ran\n"),
        "{log}"
    );
    let why = match f.client().call(Request::Why { job: id.clone() }).unwrap() {
        Response::Why { why } => why,
        other => panic!("{other:?}"),
    };
    assert!(
        why.lines.contains(&format!("warning: {note}")),
        "{:?}",
        why.lines
    );
    let daemon_log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        daemon_log.contains("WARNING: testnode-1: the mirror of"),
        "{daemon_log}"
    );
    // A sync with nothing to report adds no note.
    let id = f.dispatch(spec("true"));
    assert!(f.wait(&id).notes.is_empty());
}

#[test]
fn dispatch_refuses_what_cannot_run() {
    let f = Fixture::new().start();
    let mut c = f.client();
    let mut call = |s: JobSpec| {
        c.call(Request::Dispatch { spec: Box::new(s) })
            .unwrap_err()
            .to_string()
    };
    let mut s = spec("true");
    s.on.insert("os".into(), "plan9".into());
    let e = call(s);
    assert!(
        e.contains("no machine could ever run") && e.contains("os=plan9"),
        "{e}"
    );
    let mut s = spec("true");
    s.request.ram_mib = 100 << 10;
    assert!(call(s).contains("RAM"));
    let mut s = spec("true");
    s.request.gpus = vec!["amd".into()];
    assert!(call(s).contains("amd"));
    let mut s = spec("true");
    s.on.insert("macos".into(), String::new());
    s.on.insert("linux".into(), String::new());
    let e = call(s);
    assert!(
        e.contains("--on: macos and linux contradict each other: a machine runs one OS"),
        "{e}"
    );
    let mut s = spec("true");
    s.priority = 6;
    assert!(call(s).contains("priority must be 1 to 5"));
    let mut s = spec("true");
    s.timeout_ms = 0;
    assert!(call(s).contains("run limit"));
    // --advance moves an existing workspace: a request without one is refused.
    let mut s = spec("true");
    s.advance = true;
    assert!(call(s).contains("--advance needs --workspace W"));
    // What no process can be given (a committed potluck.toml can say it; a command line can't).
    let mut s = spec("true");
    s.cmd.push("a\0b".into());
    let e = call(s);
    assert!(e.contains("\"a\\0b\"") && e.contains("NUL byte"), "{e}");
    for (k, v) in [("A=B", "x"), ("", "x"), ("A\0B", "x"), ("V", "x\0y")] {
        let mut s = spec("true");
        s.env.insert(k.into(), v.into());
        let e = call(s);
        assert!(
            e.contains("env") && e.contains(&format!("{k:?}")),
            "{k:?}: {e}"
        );
    }
    // --keep globs that could never match inside the workspace: refused now, not dropped at the
    // end with a line in daemon.log.
    for (g, why) in [
        ("../secrets/*", "paths with .. are refused"),
        ("/etc/passwd", "relative to the workspace"),
        ("target/[", "bad pattern"),
    ] {
        let mut s = spec("true");
        s.keep = vec![g.into()];
        let e = call(s);
        assert!(e.contains("--keep") && e.contains(why), "{g}: {e}");
    }
    let mut s = spec("true");
    s.repo.source = PathBuf::from("/unreachable/.git");
    assert!(call(s).contains("could not sync"));
    let mut s = spec("true");
    s.repo.commit = "dead".repeat(10);
    assert!(call(s).contains("only committed work runs"));
    // A workspace that never existed isn't "gone" (one removed is, below).
    let mut s = spec("true");
    s.workspace = Some("testnode-w9".into());
    let e = call(s);
    assert!(
        e.contains("no workspace testnode-w9") && !e.contains("gone"),
        "{e}"
    );
    // A commit fetched (and pinned) for the dispatch, then a submodule that can't be synced: the
    // refused dispatch leaves no pin behind.
    let mut s = spec("true");
    s.repo.commit = "f37c".repeat(10);
    s.repo.submodules[0].source = PathBuf::from("/unreachable/lib");
    assert!(call(s).contains("could not sync"));
    assert!(
        f.ws.pins.lock().unwrap().is_empty(),
        "{:?}",
        f.ws.pins.lock().unwrap()
    );
    for req in [
        Request::PairRequest,
        Request::Admit {
            request: "x".into(),
            admission: None,
        },
        Request::SelfUpdate,
    ] {
        let e = c.call(req).unwrap_err().to_string();
        assert!(e.contains("not available yet"), "{e}");
    }
    // Nothing was queued or pinned.
    assert!(f.ws.pins.lock().unwrap().is_empty());
    match c.call(Request::Status).unwrap() {
        Response::Status { status } => {
            assert!(status.queued.is_empty() && status.running.is_empty())
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn keys_make_dispatch_idempotent() {
    let f = Fixture::new().start();
    let mut s = spec("sleep 0.3");
    s.key = Some("build-abc".into());
    let ids: Vec<String> = std::thread::scope(|scope| {
        let hs: Vec<_> = (0..4)
            .map(|_| {
                let s = s.clone();
                let f = &f;
                scope.spawn(move || f.dispatch(s))
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(ids.iter().all(|i| i == &ids[0]), "{ids:?}");
    match f
        .client()
        .call(Request::Dispatch {
            spec: Box::new(s.clone()),
        })
        .unwrap()
    {
        Response::Dispatched { job, existing } => assert!(existing && job == ids[0]),
        other => panic!("{other:?}"),
    }
    f.wait(&ids[0]);
    // Still the same job after it ended.
    assert_eq!(f.dispatch(s), ids[0]);
    assert_eq!(f.exec.launched.lock().unwrap().len(), 1);
}

#[test]
fn a_job_outlives_its_client() {
    let f = Fixture::new().start();
    let id = {
        let mut c = f.client();
        match c
            .call(Request::Dispatch {
                spec: Box::new(spec("sleep 0.5; echo done; exit 3")),
            })
            .unwrap()
        {
            Response::Dispatched { job, .. } => job,
            other => panic!("{other:?}"),
        }
        // The client goes away here.
    };
    let j = f.wait(&id);
    assert_eq!(j.exit_code_for_client(), 3);
    assert!(f.log(&id).contains("done"));
}

#[test]
fn cancel_queued_and_running() {
    let f = Fixture::new().start();
    // The first job takes all the RAM, so the second queues.
    let mut big = spec("sleep 30");
    big.request.ram_mib = 8 << 10;
    let running = f.dispatch(big);
    f.wait_for(&running, "running", |j| j.state == JobState::Running);
    let queued = f.dispatch(spec("true"));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(f.get(&queued).state, JobState::Queued);
    assert!(f.ws.pinned(&queued));

    let mut c = f.client();
    match c
        .call(Request::Cancel {
            job: queued.clone(),
        })
        .unwrap()
    {
        Response::Job { job } => {
            assert_eq!(job.state, JobState::Cancelled);
            assert_eq!(job.exit_code_for_client(), 130);
        }
        other => panic!("{other:?}"),
    }
    assert!(!f.ws.pinned(&queued), "a cancelled job's pins go");

    let start = Instant::now();
    c.call(Request::Cancel {
        job: running.clone(),
    })
    .unwrap();
    let j = f.wait(&running);
    assert_eq!(j.state, JobState::Cancelled);
    assert_eq!(j.exit_code_for_client(), 130);
    assert_eq!(j.signal, Some(libc::SIGTERM), "{j:#?}");
    assert!(start.elapsed() < Duration::from_secs(5));
    let pid = pid_of(j.handle.as_ref().unwrap());
    assert!(!f.exec.alive(pid));
    // Cancelling again is harmless.
    assert!(matches!(
        c.call(Request::Cancel { job: running }).unwrap(),
        Response::Job { job } if job.state == JobState::Cancelled
    ));
}

/// What the daemon answers a resize of `job` to `ram_mib`, unsigned: the job and its request
/// before, or the refusal.
fn resized(f: &Fixture, job: &str, ram_mib: u64) -> Result<(JobRecord, u64), String> {
    match f.client().call(Request::Resize {
        job: job.into(),
        ram_mib,
        auth: None,
    }) {
        Ok(Response::Resized { job, from_ram_mib }) => Ok((*job, from_ram_mib)),
        Ok(other) => panic!("{other:?}"),
        Err(e) => Err(e.to_string()),
    }
}

/// A resize lowers a queued job's RAM request where it stands in line: its priority, passes,
/// submit time and id stay, so the job dispatched after it, which fits but may not jump the line,
/// stays behind it; once its request fits, it starts, and then the one behind it. The same amount
/// changes nothing; more, or a job that runs or has ended, is refused.
#[test]
fn resize_lowers_a_queued_jobs_request_in_place() {
    let f = Fixture::new().start();
    // 4 of the machine's 8 GiB taken.
    let mut big = spec("sleep 30");
    big.request.ram_mib = 4 << 10;
    let running = f.dispatch(big);
    f.wait_for(&running, "running", |j| j.state == JobState::Running);
    let mut first = spec("sleep 0.3");
    first.request.ram_mib = 6 << 10;
    let first = f.dispatch(first);
    let behind = f.dispatch(spec("sleep 0.3"));
    std::thread::sleep(Duration::from_millis(300));
    let before = f.get(&first);
    assert_eq!(before.state, JobState::Queued);
    assert_eq!(f.get(&behind).state, JobState::Queued);
    let queued = || match f.client().call(Request::Status).unwrap() {
        Response::Status { status } => status
            .queued
            .iter()
            .map(|j| j.id.clone())
            .collect::<Vec<_>>(),
        other => panic!("{other:?}"),
    };
    assert_eq!(queued(), [first.clone(), behind.clone()]);

    // 5 GiB: still more than the 4 free. It stays first, and the job behind it waits.
    let (j, from) = resized(&f, &first, 5 << 10).unwrap();
    assert_eq!((from, j.spec.request.ram_mib), (6 << 10, 5 << 10));
    let after = f.get(&first);
    assert_eq!(after.state, JobState::Queued);
    assert_eq!(
        (after.passes, after.submitted_at, after.spec.priority),
        (before.passes, before.submitted_at, before.spec.priority)
    );
    let mut want = before.spec.clone();
    want.request.ram_mib = 5 << 10;
    assert_eq!(after.spec, want, "only the request changed");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(queued(), [first.clone(), behind.clone()]);
    assert_eq!(f.get(&behind).state, JobState::Queued);
    // The same: nothing changes.
    let (j, from) = resized(&f, &first, 5 << 10).unwrap();
    assert_eq!((from, j.spec.request.ram_mib), (5 << 10, 5 << 10));
    // More, or a job that isn't queued: refused.
    let e = resized(&f, &first, 6 << 10).unwrap_err();
    assert_eq!(
        e,
        format!(
            "resize only lowers a job's RAM request: job {first} asks for 5 GiB, and 6 GiB is more"
        )
    );
    let e = resized(&f, &running, 1 << 10).unwrap_err();
    assert_eq!(
        e,
        format!(
            "job {running} is running on testnode, and only a queued job's RAM request changes"
        )
    );
    let e = resized(&f, "testnode-999", 1 << 10).unwrap_err();
    assert_eq!(e, "no job testnode-999");
    assert_eq!(f.get(&first).spec.request.ram_mib, 5 << 10);

    // 2 GiB fits: it starts, and the job behind it after it.
    resized(&f, &first, 2 << 10).unwrap();
    let a = f.wait(&first);
    let b = f.wait(&behind);
    assert_eq!(
        (a.state, b.state),
        (JobState::Succeeded, JobState::Succeeded)
    );
    assert!(a.started_at <= b.started_at, "{a:#?}\n{b:#?}");
    assert_eq!(a.lease.map(|l| l.grant.ram_mib), Some(2 << 10));
    assert_eq!(f.exec.launched(&first).mem_max, 2 << 30);
    let e = resized(&f, &first, 1 << 10).unwrap_err();
    assert_eq!(
        e,
        format!("job {first} has ended (succeeded), and only a queued job's RAM request changes")
    );
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        log.contains(&format!(
            "{first}: resized: asks for 5 GiB of RAM, not 6 GiB"
        )),
        "{log}"
    );
}

#[test]
fn run_limit_terms_then_kills() {
    let f = Fixture::new().start();
    let mut s = spec("sleep 30");
    s.timeout_ms = 1_000;
    let polite = f.dispatch(s);
    // This one ignores SIGTERM, so it takes SIGKILL after the grace period.
    let mut s = spec("trap '' TERM; sleep 30");
    s.timeout_ms = 1_000;
    let stubborn = f.dispatch(s);

    let j = f.wait(&polite);
    assert_eq!(j.state, JobState::TimedOut);
    assert_eq!(j.exit_code_for_client(), 124);
    assert_eq!(j.message.as_deref(), Some("killed at its run limit (1s)"));
    let ran = j.ended_at.unwrap() - j.started_at.unwrap();
    assert!((1_000..5_000).contains(&ran), "{ran}");

    let j = f.wait(&stubborn);
    assert_eq!(j.state, JobState::TimedOut);
    assert_eq!(j.exit_code_for_client(), 124);
    let ran = j.ended_at.unwrap() - j.started_at.unwrap();
    assert!(
        ran >= 1_000 + 600,
        "SIGKILL came after the grace period: {ran}"
    );
    assert!(j.stuck_pids.is_empty());
}

#[test]
fn processes_that_wont_die_dont_keep_the_lease() {
    let f = Fixture::new().start();
    f.exec.deaf.store(true, Ordering::SeqCst);
    let mut s = spec("sleep 30");
    s.timeout_ms = 500;
    s.request.ram_mib = 8 << 10;
    let stuck = f.dispatch(s);
    let next = f.dispatch(spec("true"));
    let j = f.wait(&stuck);
    assert_eq!(j.state, JobState::TimedOut);
    let pid = pid_of(j.handle.as_ref().unwrap());
    assert_eq!(j.stuck_pids, [pid]);
    assert!(f.exec.alive(pid), "still there");
    // The RAM it held is free again.
    f.exec.deaf.store(false, Ordering::SeqCst);
    assert_eq!(f.wait(&next).state, JobState::Succeeded);
}

/// A crash report ReportCrash writes seconds after the crash reaches the job's crash dir when the
/// job's own command didn't die of the crash: a test binary crashed and cargo (make, a shell)
/// exited with a code.
#[test]
fn a_crash_report_written_late_is_collected_after_an_exit_code() {
    let f = Fixture::new().start();
    let reports = f._dir.path().join("DiagnosticReports");
    std::fs::create_dir_all(&reports).unwrap();
    *f.exec.reports.lock().unwrap() = Some(reports.clone());
    let id = f.dispatch(spec("exit 101"));
    let j = f.wait(&id);
    assert_eq!((j.exit_code, j.signal), (Some(101), None), "{j:#?}");
    // The report of the child that crashed is written after the job ended.
    std::fs::write(reports.join("test-late.ips"), "a crash\n").unwrap();
    let report = f.paths.job_dir(&id).join("crash/test-late.ips");
    wait_until("the late report is collected", || report.is_file());
    assert!(!reports.join("test-late.ips").exists());
}

/// A job deleted before the late collection of its crash reports (by retention) stays deleted:
/// that collection leaves it alone, and a report written after the deletion stays where it is.
#[test]
fn the_late_crash_collection_leaves_a_deleted_job_alone() {
    let mut f = Fixture::new().start();
    let reports = f._dir.path().join("DiagnosticReports");
    std::fs::create_dir_all(&reports).unwrap();
    *f.exec.reports.lock().unwrap() = Some(reports.clone());
    let id = f.dispatch(spec("exit 101"));
    let j = f.wait(&id);
    let pid = pid_of(j.handle.as_ref().unwrap());
    let dir = f.paths.job_dir(&id);
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::write(reports.join("test-late.ips"), "a crash\n").unwrap();
    // The stop cuts the wait short, and waits for the late collection.
    f.stop();
    assert!(!dir.exists(), "{} was made again", dir.display());
    assert!(reports.join("test-late.ips").exists());
    let finishes = f.exec.finished.lock().unwrap().clone();
    assert_eq!(finishes, [pid], "only the collection at the end");
}

#[test]
fn a_cleanup_that_failed_is_done_again() {
    let mut f = Fixture::new().start();
    f.exec.fail_finish.store(true, Ordering::SeqCst);
    let id = f.dispatch(spec("sleep 0.3"));
    let j = f.wait(&id);
    // A failed cleanup with nothing left of the job: no processes are stuck.
    assert_eq!(j.state, JobState::Succeeded);
    assert!(j.stuck_pids.is_empty(), "{:?}", j.stuck_pids);
    let pid = pid_of(j.handle.as_ref().unwrap());
    assert!(!f.exec.finished.lock().unwrap().contains(&pid));
    // Systemd answers again (or the crash reports can be moved): the cleanup is done then.
    f.exec.fail_finish.store(false, Ordering::SeqCst);
    f.stop();
    // Not the late collection of crash reports, which the stop cut short.
    f.exec.finished.lock().unwrap().clear();
    f.restart();
    let start = Instant::now();
    while !f.exec.finished.lock().unwrap().contains(&pid) {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "never cleaned up"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // Once.
    f.stop();
    f.restart();
    std::thread::sleep(Duration::from_millis(500));
    let n = f
        .exec
        .finished
        .lock()
        .unwrap()
        .iter()
        .filter(|p| **p == pid)
        .count();
    assert_eq!(n, 1);
}

#[test]
fn over_memory_ends_the_job() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec("sleep 30"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.exec.over_memory.store(true, Ordering::SeqCst);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::OverMemory);
    assert_eq!(j.exit_code_for_client(), 137);
    assert_eq!(
        j.message.as_deref(),
        Some("over memory: used 5 GiB of its 512 MiB")
    );
    // daemon.log says the state once.
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        log.contains(&format!("{id}: over memory: used 5 GiB of its 512 MiB")),
        "{log}"
    );
    assert!(!log.contains("over memory: over memory"), "{log}");
}

/// Over memory as Linux reports it (samples a second apart saw little; the kernel's peak is at
/// the limit): the message gives the peak, not the last sample.
#[test]
fn over_memory_says_the_peak() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec("sleep 30"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.exec.over_memory_at_peak.store(true, Ordering::SeqCst);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::OverMemory);
    assert_eq!(
        j.message.as_deref(),
        Some("over memory: used 512 MiB of its 512 MiB")
    );
    assert_eq!(j.usage.peak_mem_bytes, 512 << 20);
}

/// Ended by the monitor for too many processes (macOS): failed by SIGKILL, so `wait` exits 137,
/// and the message gives the count, the limit and where the limit comes from.
#[test]
fn too_many_processes_ends_the_job_with_its_own_message() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec("sleep 30"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.exec.over_tasks.store(true, Ordering::SeqCst);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Failed);
    assert_eq!((j.exit_code, j.signal), (None, Some(libc::SIGKILL)));
    assert_eq!(j.exit_code_for_client(), 137);
    let message = "too many processes: 5334, over its limit of 5333 (half of kern.maxprocperuid, the most processes one user may have)";
    assert_eq!(j.message.as_deref(), Some(message));
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(log.contains(&format!("{id}: failed: {message}")), "{log}");
}

/// Killed by the OOM killer while under its own limit (the machine ran short): not over memory,
/// and the message says so.
#[test]
fn an_oom_kill_under_the_limit_is_not_over_memory() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec("sleep 30"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.exec.oom_killed.store(true, Ordering::SeqCst);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Failed);
    assert_eq!(j.exit_code_for_client(), 137);
    assert_eq!(
        j.message.as_deref(),
        Some(
            "killed because the machine ran short of memory, under its own limit: used 300 MiB of its 512 MiB"
        )
    );
}

#[test]
fn stalls_are_sampled_and_ended_only_when_asked() {
    let mut f = Fixture::new();
    f.opts.stall_ms = 800;
    let f = f.start();
    f.exec.frozen.store(true, Ordering::SeqCst);
    let quiet = f.dispatch(spec("echo started; sleep 30"));
    let j = f.wait_for(&quiet, "stalled", |j| j.stalled);
    assert_eq!(j.state, JobState::Running);
    let log = f.log(&quiet);
    assert!(
        log.contains("under 1% CPU and no output") && log.contains("fake stacks of"),
        "{log}"
    );
    std::thread::sleep(Duration::from_millis(2_000));
    let j = f.get(&quiet);
    assert_eq!(j.state, JobState::Running, "only marked");
    // The stacks the daemon wrote into the log are not the job's output: it stays stalled, and
    // its stacks are taken once.
    assert!(j.stalled, "{j:#?}");
    let log = f.log(&quiet);
    assert_eq!(log.matches("fake stacks of").count(), 1, "{log}");

    let mut s = spec("sleep 30");
    s.kill_on_stall = true;
    let killed = f.dispatch(s);
    let j = f.wait(&killed);
    assert_eq!(j.state, JobState::TimedOut);
    assert!(j.message.unwrap().starts_with("stalled"));

    f.client()
        .call(Request::Cancel { job: quiet.clone() })
        .unwrap();
    assert_eq!(f.wait(&quiet).state, JobState::Cancelled);
}

/// Kill on stall ends a job that is still stalled once its stacks are in its log; one that made
/// progress again while they were sampled (which can take many seconds) goes on.
#[test]
fn a_job_that_recovers_while_its_stacks_are_sampled_is_not_killed() {
    let mut f = Fixture::new();
    f.opts.stall_ms = 800;
    let f = f.start();
    f.exec.frozen.store(true, Ordering::SeqCst);
    f.exec.stacks_ms.store(2_000, Ordering::SeqCst);
    let mut s = spec("echo started; sleep 30");
    s.kill_on_stall = true;
    let id = f.dispatch(s);
    f.wait_for(&id, "stalled", |j| j.stalled);
    // It uses the CPU again while its stacks are being sampled.
    f.exec.frozen.store(false, Ordering::SeqCst);
    f.wait_for(&id, "going again", |j| !j.stalled);
    wait_until("its stacks are in the log", || {
        f.log(&id).contains("fake stacks of")
    });
    std::thread::sleep(Duration::from_millis(500));
    let j = f.get(&id);
    assert_eq!(
        j.state,
        JobState::Running,
        "killed after it recovered: {j:#?}"
    );
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    assert_eq!(f.wait(&id).state, JobState::Cancelled);
}

/// A job stalled before a daemon restart stays stalled after it: no second stack dump in its log,
/// and why/status keep saying so.
#[test]
fn a_stalled_job_stays_stalled_across_a_restart() {
    let mut f = Fixture::new();
    f.opts.stall_ms = 800;
    let mut f = f.start();
    f.exec.frozen.store(true, Ordering::SeqCst);
    let quiet = f.dispatch(spec("echo started; sleep 30"));
    f.wait_for(&quiet, "stalled", |j| j.stalled);
    wait_until("its stacks are in the log", || {
        f.log(&quiet).contains("fake stacks of")
    });
    f.stop();
    f.restart();
    std::thread::sleep(Duration::from_millis(1_500));
    assert!(f.get(&quiet).stalled, "unmarked by the restart");
    let log = f.log(&quiet);
    assert_eq!(log.matches("stacks of its processes").count(), 1, "{log}");
    f.client()
        .call(Request::Cancel { job: quiet.clone() })
        .unwrap();
    assert_eq!(f.wait(&quiet).state, JobState::Cancelled);
}

/// Past its log's limit (node.toml `log_max_mib`) a job's log stops growing, but what it writes
/// still counts as output: it doesn't stall while it writes.
#[test]
fn output_past_the_log_limit_is_still_output() {
    let mut f = Fixture::new();
    f.opts.stall_ms = 2_500;
    f.config.log_max_mib = Some(1);
    let f = f.start();
    f.exec.frozen.store(true, Ordering::SeqCst);
    let job = f.dispatch(spec(
        "head -c 1048576 /dev/zero; echo; while true; do echo still here; sleep 0.2; done",
    ));
    f.wait_for(&job, "past its log limit", |_| {
        f.log(&job).contains("what follows is dropped")
    });
    let len = f.log(&job).len();
    std::thread::sleep(Duration::from_millis(6_000));
    let j = f.get(&job);
    assert!(!j.stalled, "stalled while writing: {j:#?}");
    assert_eq!(f.log(&job).len(), len, "the log grew past its limit");
    f.client()
        .call(Request::Cancel { job: job.clone() })
        .unwrap();
    let j = f.wait(&job);
    assert_eq!(j.state, JobState::Cancelled);
    // SIGTERM reached the supervisor: the tail of what was dropped is in the log.
    let log = f.log(&job);
    assert!(
        log.contains("bytes of output dropped after the log reached log_max_mib (1 MiB)")
            && log.trim_end().ends_with("still here"),
        "{}",
        &log[log.len().saturating_sub(500)..]
    );
}

/// The owner's rule: under 1% of one core counts as no CPU. A job whose CPU time rises only by a
/// trickle (a polling thread) stalls; one at 10% of a core doesn't.
#[test]
fn a_trickle_of_cpu_time_is_a_stall() {
    let mut f = Fixture::new();
    f.opts.stall_ms = 800;
    let f = f.start();
    let busy = f.dispatch(spec("echo started; sleep 30"));
    f.wait_for(&busy, "running", |j| j.state == JobState::Running);
    std::thread::sleep(Duration::from_millis(2_000));
    let j = f.get(&busy);
    assert!(!j.stalled, "stalled at 10% of a core: {j:#?}");
    f.client()
        .call(Request::Cancel { job: busy.clone() })
        .unwrap();
    f.wait(&busy);

    f.exec.trickle.store(true, Ordering::SeqCst);
    let polling = f.dispatch(spec("echo started; sleep 30"));
    let j = f.wait_for(&polling, "stalled", |j| j.stalled);
    assert!(j.usage.cpu_ms > 0);
    let log = f.log(&polling);
    assert!(log.contains("under 1% CPU and no output for"), "{log}");
    f.client()
        .call(Request::Cancel {
            job: polling.clone(),
        })
        .unwrap();
    assert_eq!(f.wait(&polling).state, JobState::Cancelled);
}

/// Run limits are measured on the boot clock: a step of the wall clock (NTP, someone setting the
/// date) neither ends a job early nor lets it run past its limit.
#[test]
fn a_wall_clock_step_moves_no_run_limit() {
    let f = Fixture::new().start();
    let mut s = spec("echo started; sleep 30");
    s.timeout_ms = 4_000;
    let started = Instant::now();
    let id = f.dispatch(s);
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    // A day forward: the limit is still 4 s after the start.
    f.clock.step_wall(86_400_000);
    std::thread::sleep(Duration::from_millis(1_000));
    let j = f.get(&id);
    assert_eq!(j.state, JobState::Running, "ended by a clock step: {j:#?}");
    // Two days back, a day behind: the limit still comes 4 s after the start.
    f.clock.step_wall(-2 * 86_400_000);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::TimedOut, "{j:#?}");
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(3_500) && took < Duration::from_secs(10),
        "{took:?}"
    );
}

/// The grace between SIGTERM and SIGKILL is on the awake clock, which no step of the wall clock
/// moves: a wall clock set back right after the SIGTERM doesn't hold the SIGKILL back.
#[test]
fn a_wall_clock_set_back_does_not_hold_back_the_sigkill() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec(
        "trap '' TERM; echo started; while true; do sleep 1; done",
    ));
    f.wait_for(&id, "running", |_| f.log(&id).contains("started"));
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    // The loop (every 100 ms) has sent the SIGTERM.
    std::thread::sleep(Duration::from_millis(400));
    f.clock.step_wall(-3_600_000);
    let start = Instant::now();
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Cancelled, "{j:#?}");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "{:?}",
        start.elapsed()
    );
}

/// The grace between SIGTERM and SIGKILL is the job's time to shut down, so it is on the awake
/// clock: a machine that sleeps (a laptop's lid closed) right after the SIGTERM gives the job the
/// rest of its grace once it wakes, rather than a SIGKILL at once. The SIGKILL still comes once
/// the grace has passed awake.
#[test]
fn time_asleep_is_no_part_of_the_kill_grace() {
    let mut f = Fixture::new();
    f.opts.kill_grace_ms = 3_000;
    let f = f.start();
    let id = f.dispatch(spec(
        "trap 'echo got TERM' TERM; echo started; while true; do sleep 0.1; done",
    ));
    f.wait_for(&id, "running", |_| f.log(&id).contains("started"));
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    f.wait_for(&id, "sent SIGTERM", |_| f.log(&id).contains("got TERM"));
    let start = Instant::now();
    f.clock.sleep(3_600_000);
    let j = f.wait(&id);
    let took = start.elapsed();
    assert_eq!(j.state, JobState::Cancelled, "{j:#?}");
    assert!(
        took >= Duration::from_millis(2_000) && took < Duration::from_secs(8),
        "SIGKILL {took:?} after waking"
    );
}

/// Stall windows are on the boot clock: a job that uses the CPU isn't stalled by a step of the
/// wall clock, and one that stalls is still found stalled after the clock is set back.
#[test]
fn a_wall_clock_step_is_no_stall_and_hides_none() {
    let mut f = Fixture::new();
    f.opts.stall_ms = 1_500;
    let f = f.start();
    let mut s = spec("echo started; sleep 30");
    s.kill_on_stall = true;
    let busy = f.dispatch(s);
    f.wait_for(&busy, "running", |j| j.state == JobState::Running);
    std::thread::sleep(Duration::from_millis(300));
    f.clock.step_wall(3_600_000);
    std::thread::sleep(Duration::from_millis(1_000));
    f.clock.step_wall(-7_200_000);
    std::thread::sleep(Duration::from_millis(1_000));
    let j = f.get(&busy);
    assert!(
        j.state == JobState::Running && !j.stalled,
        "stalled by a clock step: {j:#?}"
    );
    assert!(!f.log(&busy).contains("stacks"), "{}", f.log(&busy));
    f.client()
        .call(Request::Cancel { job: busy.clone() })
        .unwrap();
    assert_eq!(f.wait(&busy).state, JobState::Cancelled);

    f.exec.frozen.store(true, Ordering::SeqCst);
    let mut s = spec("echo started; sleep 30");
    s.kill_on_stall = true;
    let quiet = f.dispatch(s);
    f.wait_for(&quiet, "running", |j| j.state == JobState::Running);
    f.clock.step_wall(-3_600_000);
    let start = Instant::now();
    let j = f.wait(&quiet);
    assert_eq!(j.state, JobState::TimedOut, "{j:#?}");
    assert!(j.message.unwrap().starts_with("stalled"));
    assert!(
        start.elapsed() < Duration::from_secs(6),
        "{:?}",
        start.elapsed()
    );
}

/// A machine that sleeps (a laptop's lid closed) stops its jobs with it: on waking, the time it
/// slept is no stall, so a job that made progress until then isn't marked stalled, sampled or
/// ended by kill on stall. Here the boot and wall clocks jump an hour, as across a sleep. Quiet
/// for the stall time while awake, it stalls.
#[test]
fn time_asleep_is_no_stall() {
    let mut f = Fixture::new();
    f.opts.stall_ms = 60_000;
    let f = f.start();
    let mut s = spec("echo started; sleep 30");
    s.kill_on_stall = true;
    s.timeout_ms = 3 * 3_600_000;
    let id = f.dispatch(s);
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    std::thread::sleep(Duration::from_millis(300));
    f.clock.sleep(3_600_000);
    std::thread::sleep(Duration::from_millis(1_000));
    let j = f.get(&id);
    assert!(
        j.state == JobState::Running && !j.stalled,
        "stalled by a sleep: {j:#?}\n{}",
        f.log(&id)
    );
    assert!(!f.log(&id).contains("stacks"), "{}", f.log(&id));
    // Awake and quiet for the stall time: stalled.
    f.exec.frozen.store(true, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(300));
    f.clock.pass(60_000);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::TimedOut, "{j:#?}");
    assert!(j.message.unwrap().starts_with("stalled"));
}

/// The loop's periodic work (here the facts refresh) is on the boot clock: a wall clock set back
/// doesn't pause it until the wall clock catches up.
#[test]
fn a_wall_clock_set_back_does_not_pause_periodic_work() {
    let mut f = Fixture::new();
    f.opts.facts_refresh_ms = 100;
    let calls = Arc::new(AtomicU64::new(0));
    let parts = Parts {
        executor: f.exec.clone(),
        workspaces: f.ws.clone(),
        scheduler: Arc::new(RealScheduler),
        facts: Box::new({
            let calls = calls.clone();
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(facts())
            }
        }),
        login_env: BTreeMap::new(),
        clock: f.clock.clone(),
    };
    f.daemon = Some(Daemon::start(&f.paths, &f.config, parts, f.opts.clone()).unwrap());
    wait_until("the first refresh", || calls.load(Ordering::SeqCst) >= 2);
    f.clock.step_wall(-3_600_000);
    let n = calls.load(Ordering::SeqCst);
    wait_until("refreshes go on", || calls.load(Ordering::SeqCst) >= n + 3);
}

/// Facts detected again under another name (the OS changed the hostname while the daemon runs)
/// don't rename the machine: it keeps the name it started with in its facts, and a job asking
/// for `node=` that name still runs here.
#[test]
fn a_facts_refresh_keeps_the_machines_name() {
    let mut f = Fixture::new();
    f.opts.facts_refresh_ms = 100;
    let calls = Arc::new(AtomicU64::new(0));
    let parts = Parts {
        executor: f.exec.clone(),
        workspaces: f.ws.clone(),
        scheduler: Arc::new(RealScheduler),
        facts: Box::new({
            let calls = calls.clone();
            move || {
                let mut detected = facts();
                if calls.fetch_add(1, Ordering::SeqCst) > 0 {
                    detected.node = "renamed".into();
                }
                Ok(detected)
            }
        }),
        login_env: BTreeMap::new(),
        clock: f.clock.clone(),
    };
    f.daemon = Some(Daemon::start(&f.paths, &f.config, parts, f.opts.clone()).unwrap());
    // The third detection starts once the second (the first refresh) has been applied.
    wait_until("two refreshes", || calls.load(Ordering::SeqCst) >= 3);
    match f.client().call(Request::Nodes).unwrap() {
        Response::Nodes { nodes } => {
            let names: Vec<&str> = nodes.iter().map(|n| n.facts.node.as_str()).collect();
            assert_eq!(names, ["testnode"]);
        }
        other => panic!("{other:?}"),
    }
    let mut s = spec("true");
    s.on.insert("node".into(), "testnode".into());
    let id = f.dispatch(s);
    assert_eq!(f.wait(&id).state, JobState::Succeeded);
}

/// A daemon restarted in the same boot goes on with a job's run limit by the boot clock: a step
/// of the wall clock while it was down moves nothing, the time it was down counts.
#[test]
fn a_run_limit_survives_a_restart_and_a_clock_step() {
    let mut f = Fixture::new().start();
    let mut s = spec("echo started; sleep 60");
    s.timeout_ms = 20_000;
    let id = f.dispatch(s);
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.stop();
    f.clock.step_wall(86_400_000);
    f.restart();
    std::thread::sleep(Duration::from_millis(1_000));
    let j = f.get(&id);
    assert_eq!(j.state, JobState::Running, "ended by a clock step: {j:#?}");
    f.stop();
    // Set back two days, and 30 s go by while no daemon runs.
    f.clock.step_wall(-2 * 86_400_000);
    f.clock.pass(30_000);
    let start = Instant::now();
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::TimedOut, "{j:#?}");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "{:?}",
        start.elapsed()
    );
}

/// A run record written before the daemon kept boot-clock readings has wall times: a cancel's
/// SIGTERM time from it still leads to the SIGKILL after the grace.
#[test]
fn an_older_run_record_carries_its_times_over() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec(
        "trap '' TERM; echo started; while true; do sleep 1; done",
    ));
    f.wait_for(&id, "running", |_| f.log(&id).contains("started"));
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));
    f.stop();
    {
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        let key = format!("run/{id}");
        let run: String = db
            .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
            .unwrap();
        let mut run: serde_json::Value = serde_json::from_str(&run).unwrap();
        let old = run.as_object_mut().unwrap();
        assert!(old["term_at"].is_u64(), "{run}");
        old.remove("boot");
        old.remove("deadline");
        old.remove("starting_at");
        old.insert("term_at".into(), (now_ms() - 400).into());
        db.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            [&key, &run.to_string()],
        )
        .unwrap();
    }
    let start = Instant::now();
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Cancelled, "{j:#?}");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "{:?}",
        start.elapsed()
    );
}

#[test]
fn logs_long_poll() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec("echo one; sleep 1; echo two"));
    let mut c = f.client();
    let mut read = |offset: u64, wait_ms: u64| -> (Vec<u8>, bool, Duration) {
        let start = Instant::now();
        match c
            .call(Request::Logs {
                job: id.clone(),
                offset,
                max: 1 << 20,
                wait_ms,
            })
            .unwrap()
        {
            Response::Bytes {
                offset: o,
                len,
                done,
            } => {
                assert_eq!(o, offset);
                (c.read_bytes(len).unwrap(), done, start.elapsed())
            }
            other => panic!("{other:?}"),
        }
    };
    let (b, done, _) = read(0, 10_000);
    assert_eq!((b.as_slice(), done), (&b"one\n"[..], false));
    let (b, _, took) = read(4, 10_000);
    assert_eq!(b, b"two\n");
    assert!(
        took >= Duration::from_millis(500),
        "waited for it: {took:?}"
    );
    let (b, done, _) = read(8, 10_000);
    assert!(b.is_empty() && done);
    // The whole log in one go, and a short wait with nothing new.
    let (b, done, _) = read(0, 0);
    assert_eq!((b.as_slice(), done), (&b"one\ntwo\n"[..], true));

    let quiet = f.dispatch(spec("sleep 5"));
    let mut c = f.client();
    let start = Instant::now();
    match c
        .call(Request::Logs {
            job: quiet.clone(),
            offset: 0,
            max: 0,
            wait_ms: 300,
        })
        .unwrap()
    {
        Response::Bytes { len, done, .. } => assert_eq!((len, done), (0, false)),
        other => panic!("{other:?}"),
    }
    assert!(start.elapsed() >= Duration::from_millis(300));
    c.call(Request::Cancel { job: quiet }).unwrap();
}

/// A hold belongs to the branch whose request set when it ends: `potluck hold` from another
/// branch, or a job of another branch whose end holds it longer, makes it that branch's, and the
/// reminders follow.
#[test]
fn a_hold_belongs_to_the_branch_that_set_it() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec("true"));
    let ws = f.wait(&id).workspace.unwrap();
    let branch = |name: &str| {
        Client::connect(
            &f.paths,
            ClientContext {
                repo_id: Some(REPO.into()),
                branch: Some(name.into()),
                ..Default::default()
            },
        )
        .unwrap()
    };
    let reminded = |c: &mut Client| {
        c.call(Request::Ping).unwrap();
        c.reminders
            .iter()
            .map(|r| r.workspace.clone())
            .collect::<Vec<_>>()
    };
    let (mut mine, mut other) = (branch(BRANCH), branch("agent-2"));
    assert_eq!(reminded(&mut mine), vec![ws.clone()]);
    assert!(reminded(&mut other).is_empty());
    // agent-2 holds it for an hour: its hold, its reminder.
    other
        .call(Request::Hold {
            workspace: ws.clone(),
            duration_ms: 3_600_000,
        })
        .unwrap();
    assert_eq!(other.reminders.len(), 1, "{:?}", other.reminders);
    assert!(reminded(&mut mine).is_empty());
    // A job of agent-1's in it ends with a 2 h hold: agent-1's again.
    let mut s = spec("true");
    s.workspace = Some(ws.clone());
    s.hold_ms = Some(2 * 3_600_000);
    f.wait(&f.dispatch(s));
    assert_eq!(reminded(&mut mine), vec![ws.clone()]);
    assert!(reminded(&mut other).is_empty());
    // One of agent-2's ending with the 5 minutes every job gets leaves the 2 h hold agent-1's.
    let mut s = spec("true");
    s.workspace = Some(ws.clone());
    s.repo.branch = Some("agent-2".into());
    f.wait(&f.dispatch(s));
    assert_eq!(reminded(&mut mine), vec![ws.clone()]);
    assert!(reminded(&mut other).is_empty());
}

#[test]
fn hold_release_and_reminders() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec("true"));
    let ws = f.wait(&id).workspace.unwrap();
    let mut c = f.client();
    c.call(Request::Ping).unwrap();
    assert_eq!(c.reminders.len(), 1);
    let r = &c.reminders[0];
    assert_eq!(
        (r.workspace.as_str(), r.state),
        (ws.as_str(), WorkspaceState::Held)
    );
    assert!(r.remaining_ms <= MIN_HOLD_MS && r.remaining_ms > MIN_HOLD_MS - 60_000);
    // Another branch gets no reminders.
    let mut other = Client::connect(
        &f.paths,
        ClientContext {
            repo_id: Some(REPO.into()),
            branch: Some("someone-else".into()),
            ..Default::default()
        },
    )
    .unwrap();
    other.call(Request::Ping).unwrap();
    assert!(other.reminders.is_empty());

    // Hold: DUR from now, at most 24 h.
    let held = |c: &mut Client, ms: u64| match c
        .call(Request::Hold {
            workspace: ws.clone(),
            duration_ms: ms,
        })
        .unwrap()
    {
        Response::Workspace { workspace } => workspace.held_until - now_ms(),
        other => panic!("{other:?}"),
    };
    let left = held(&mut c, 3_600_000);
    assert!(left > 3_590_000 && left <= 3_600_000, "{left}");
    let left = held(&mut c, 48 * 3_600_000);
    assert!(left > MAX_HOLD_MS - 10_000 && left <= MAX_HOLD_MS, "{left}");
    assert!(c.reminders[0].remaining_ms > MAX_HOLD_MS - 10_000);

    // Another invocation in the held workspace runs there, at its commit.
    let mut s = spec("cat COMMIT");
    s.workspace = Some(ws.clone());
    let again = f.dispatch(s.clone());
    let j = f.wait(&again);
    assert_eq!(
        (j.state, j.workspace.as_deref()),
        (JobState::Succeeded, Some(ws.as_str()))
    );
    assert_eq!(f.log(&again), spec("").repo.commit);
    // A different commit can't use it.
    let mut moved = s.clone();
    moved.repo.commit = "e".repeat(40);
    let e = c
        .call(Request::Dispatch {
            spec: Box::new(moved),
        })
        .unwrap_err()
        .to_string();
    assert!(e.contains("is at commit"), "{e}");

    // Release with an invocation running in it ends the hold but keeps it.
    let mut s2 = s.clone();
    s2.cmd = vec!["sleep".into(), "1".into()];
    let busy = f.dispatch(s2);
    f.wait_for(&busy, "running", |j| j.state == JobState::Running);
    match c
        .call(Request::Release {
            workspace: ws.clone(),
        })
        .unwrap()
    {
        Response::Workspace { workspace } => assert_ne!(workspace.state, WorkspaceState::Removed),
        other => panic!("{other:?}"),
    }
    f.wait(&busy);
    // Its end doesn't hold it again: no reminder of it comes back.
    let w = f.workspace(&ws);
    assert_eq!(w.state, WorkspaceState::Idle, "{w:#?}");
    c.call(Request::Ping).unwrap();
    assert!(c.reminders.is_empty(), "{:?}", c.reminders);

    // Release: removed, its pins and TMPDIR gone, no more reminders.
    match c
        .call(Request::Release {
            workspace: ws.clone(),
        })
        .unwrap()
    {
        Response::Workspace { workspace } => assert_eq!(workspace.state, WorkspaceState::Removed),
        other => panic!("{other:?}"),
    }
    assert!(c.reminders.is_empty());
    assert_eq!(*f.ws.removed.lock().unwrap(), std::slice::from_ref(&ws));
    assert!(!f.paths.workspace(&ws).exists() && !f.paths.workspace_tmp(&ws).exists());
    assert!(!f.ws.pinned(&ws));
    let e = c
        .call(Request::Release {
            workspace: ws.clone(),
        })
        .unwrap_err()
        .to_string();
    assert!(e.contains("is gone"), "{e}");
    let e = c
        .call(Request::Dispatch { spec: Box::new(s) })
        .unwrap_err()
        .to_string();
    assert!(e.contains("is gone"), "{e}");
}

/// The lifecycle policy decides on a snapshot of the records, outside the state lock. A
/// workspace someone holds after that (here, while the policy runs) is held, not removed: the
/// removal goes ahead only for a workspace still idle.
#[test]
fn a_workspace_held_while_the_policy_runs_is_not_removed() {
    let f = Fixture::new().start();
    f.ws.no_hold.store(true, Ordering::SeqCst);
    let job = f.dispatch(spec("true"));
    let ws = f.wait(&job).workspace.unwrap();
    wait_until("the workspace is idle", || {
        f.workspace(&ws).state == WorkspaceState::Idle
    });
    f.ws.pause_tick.store(true, Ordering::SeqCst);
    *f.ws.remove_when_idle.lock().unwrap() = Some(ws.clone());
    wait_until("the policy runs", || f.ws.in_tick.load(Ordering::SeqCst));
    match f
        .client()
        .call(Request::Hold {
            workspace: ws.clone(),
            duration_ms: 3_600_000,
        })
        .unwrap()
    {
        Response::Workspace { workspace } => assert_eq!(workspace.state, WorkspaceState::Held),
        other => panic!("{other:?}"),
    }
    f.ws.pause_tick.store(false, Ordering::SeqCst);
    // The policy's answer is applied, then a few more ticks.
    std::thread::sleep(Duration::from_millis(500));
    let w = f.workspace(&ws);
    assert_eq!(w.state, WorkspaceState::Held, "{w:?}");
    assert!(f.ws.removed.lock().unwrap().is_empty());
    assert!(f.paths.workspace(&ws).exists());
}

/// A kept file that can't be copied (a full disk; here a `keep/` the job made read-only) is a
/// warning on the job, which `why` shows, not something lost without a word.
#[test]
fn a_kept_file_that_cannot_be_copied_is_a_warning() {
    let f = Fixture::new().start();
    let mut s = spec(r#"echo k > k.txt; chmod 500 "$POTLUCK_HOME/jobs/$POTLUCK_JOB/keep""#);
    s.keep = vec!["k.txt".into()];
    let id = f.dispatch(s);
    let j = f.wait(&id);
    let keep = f.paths.job_dir(&id).join("keep");
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&keep, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    assert!(j.kept.is_empty(), "{j:#?}");
    assert_eq!(j.notes.len(), 1, "{j:#?}");
    assert!(
        j.notes[0].starts_with("--keep on testnode: could not copy k.txt: "),
        "{j:#?}"
    );
    assert!(!keep.join("k.txt").exists());
}

#[test]
fn copy_out_of_a_workspace() {
    let f = Fixture::new().start();
    let mut s = spec("echo made > data/made.txt; mkdir -p out; echo k > out/k.txt");
    s.keep = vec!["out/*.txt".into(), "data/escape".into()];
    let id = f.dispatch(s);
    let j = f.wait(&id);
    assert_eq!(j.kept, ["keep/out/k.txt"]);
    assert_eq!(
        std::fs::read(f.paths.job_dir(&id).join("keep/out/k.txt")).unwrap(),
        b"k\n"
    );
    let ws = j.workspace.unwrap();
    let mut c = f.client();
    let mut first = c
        .call(Request::Copy {
            workspace: ws.clone(),
            path: "data".into(),
        })
        .unwrap();
    let mut got = Vec::new();
    loop {
        match first {
            Response::CopyEntry { path, dir, len, .. } => {
                let bytes = c.read_bytes(len).unwrap();
                got.push((path, dir, bytes));
            }
            Response::CopyDone => break,
            other => panic!("{other:?}"),
        }
        first = c.next_frame().unwrap();
    }
    let names: Vec<&str> = got.iter().map(|(p, _, _)| p.as_str()).collect();
    assert_eq!(names, ["", "a.txt", "made.txt", "sub", "sub/b.bin"]);
    assert_eq!(got[1].2, b"alpha");
    assert_eq!(got[4].2, [0u8, 1, 2, 255]);
    assert_eq!(c.reminders.len(), 1, "copy frames carry reminders");
    for bad in ["../../etc", "data/escape", "/etc/hosts"] {
        assert!(
            c.call(Request::Copy {
                workspace: ws.clone(),
                path: bad.into(),
            })
            .is_err(),
            "{bad}"
        );
    }
    // A path that isn't there says so in words, naming where it looked.
    let e = c
        .call(Request::Copy {
            workspace: ws.clone(),
            path: "nosuch/file".into(),
        })
        .unwrap_err();
    assert_eq!(
        format!("{e:#}"),
        "nosuch/file: no such file or directory in the workspace"
    );
    // The connection is still usable after an error.
    c.call(Request::Ping).unwrap();
}

#[test]
fn gpu_lease_sets_gpu_env() {
    let f = Fixture::new().start();
    let mut s = spec("sleep 1");
    s.request.gpus = vec!["fake".into()];
    let id = f.dispatch(s);
    let j = f.wait_for(&id, "running", |j| j.state == JobState::Running);
    assert_eq!(j.lease.as_ref().unwrap().grant.gpus, [GPU]);
    assert_eq!(f.exec.launched(&id).env["FAKE_GPU_HELD"], "1");
    f.wait(&id);
}

/// A `[[gpu]]` entry's env goes over the login environment, but `--env` (and the committed
/// potluck.toml) go over it, and potluck's own variables over everything.
#[test]
fn gpu_env_yields_to_the_dispatch_and_to_potlucks_own_variables() {
    let mut f = Fixture::new();
    f.config.gpu[0].env.extend(
        [
            ("FROM_LOGIN", "gpu"),
            ("OVERRIDDEN", "gpu"),
            ("POTLUCK_JOB", "gpu"),
            ("POTLUCK_LEASE", "gpu"),
            ("POTLUCK_GPUS", "gpu"),
            ("TMPDIR", "/gpu"),
        ]
        .map(|(k, v)| (k.to_string(), v.to_string())),
    );
    let f = f.start();
    let mut s = spec("true");
    s.request.gpus = vec!["fake".into()];
    s.env.insert("OVERRIDDEN".into(), "spec".into());
    let id = f.dispatch(s);
    let j = f.wait(&id);
    let env = f.exec.launched(&id).env;
    assert_eq!(env["FAKE_GPU_HELD"], "1");
    assert_eq!(env["FROM_LOGIN"], "gpu");
    assert_eq!(env["OVERRIDDEN"], "spec");
    assert_eq!(env["POTLUCK_JOB"], id);
    assert_eq!(env["POTLUCK_LEASE"], j.lease.unwrap().token);
    assert_eq!(env["POTLUCK_GPUS"], GPU);
    let ws = j.workspace.unwrap();
    assert_eq!(PathBuf::from(&env["TMPDIR"]), f.paths.workspace_tmp(&ws));
}

/// `unset_env` leaves a variable out of the job's environment, whether the login environment
/// or a `[[gpu]]` entry set it; potluck's own variables can't be unset, and a spec can't both
/// set and unset one: those are refused at dispatch.
#[test]
fn unset_env_leaves_variables_out_of_the_job() {
    let mut f = Fixture::new();
    f.config.gpu[0]
        .env
        .insert("GPU_KEPT".to_string(), "gpu".to_string());
    let f = f.start();
    let mut s = spec("true");
    s.request.gpus = vec!["fake".into()];
    s.env.insert("FROM_SPEC".into(), "spec".into());
    s.unset_env = ["FROM_LOGIN", "FAKE_GPU_HELD", "SET_NOWHERE"]
        .map(String::from)
        .into();
    let id = f.dispatch(s);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    assert!(j.spec.unset_env.contains("FROM_LOGIN"));
    let env = f.exec.launched(&id).env;
    for k in ["FROM_LOGIN", "FAKE_GPU_HELD", "SET_NOWHERE"] {
        assert!(!env.contains_key(k), "{k} set: {env:?}");
    }
    assert_eq!(env["GPU_KEPT"], "gpu");
    assert_eq!(env["FROM_SPEC"], "spec");
    assert_eq!(env["POTLUCK_JOB"], id);

    let mut c = f.client();
    let mut call = |s: JobSpec| {
        c.call(Request::Dispatch { spec: Box::new(s) })
            .unwrap_err()
            .to_string()
    };
    for (k, want) in [
        (
            "TMPDIR",
            "--unset-env: TMPDIR is one of potluck's own variables, which every job gets",
        ),
        (
            "POTLUCK_LEASE",
            "--unset-env: POTLUCK_LEASE is one of potluck's own variables",
        ),
        ("A=B", "--unset-env: \"A=B\" is not a variable name"),
        ("FROM_SPEC", "env and unset_env both name FROM_SPEC"),
    ] {
        let mut s = spec("true");
        s.env.insert("FROM_SPEC".into(), "spec".into());
        s.unset_env.insert(k.into());
        let e = call(s);
        assert!(e.contains(want), "{k}: {e}");
    }
}

/// `potluck env`: the login environment this daemon holds; another machine's is refused on one
/// machine.
#[test]
fn env_is_the_login_environment_jobs_start_from() {
    let f = Fixture::new().start();
    let mut c = f.client();
    for node in [None, Some("testnode".to_string())] {
        match c.call(Request::Env { node }).unwrap() {
            Response::Env { node, env } => {
                assert_eq!(node, "testnode");
                assert_eq!(env["FROM_LOGIN"], "login");
                assert!(!env.contains_key("POTLUCK_JOB"), "{env:?}");
            }
            other => panic!("{other:?}"),
        }
    }
    let e = c
        .call(Request::Env {
            node: Some("msi".into()),
        })
        .unwrap_err()
        .to_string();
    assert_eq!(
        e,
        "msi is not a member of the cluster: this machine, testnode, is in no cluster"
    );
}

#[test]
fn restart_adopts_running_jobs() {
    let mut f = Fixture::new().start();
    let quick = f.dispatch(spec("sleep 0.5; exit 5"));
    let mut s = spec("sleep 3; echo survived; exit 4");
    s.request.gpus = vec!["any".into()];
    let slow = f.dispatch(s);
    f.wait_for(&quick, "running", |j| j.state == JobState::Running);
    f.wait_for(&slow, "running", |j| j.state == JobState::Running);
    let queued = {
        let mut s = spec("true");
        s.request.gpus = vec!["any".into()];
        f.dispatch(s)
    };
    let quick_pid = pid_of(f.get(&quick).handle.as_ref().unwrap());
    f.stop();
    assert!(!f.paths.socket().exists());
    // `quick` ends while no daemon runs.
    wait_until("quick ended", || !f.exec.alive(quick_pid));
    f.restart();
    let j = f.wait(&quick);
    assert_eq!((j.state, j.exit_code_for_client()), (JobState::Failed, 5));
    let j = f.wait(&slow);
    assert_eq!(j.exit_code_for_client(), 4);
    assert!(f.log(&slow).contains("survived"));
    assert_eq!(f.wait(&queued).state, JobState::Succeeded);
    // Workspace ids keep counting from the store.
    assert_eq!(f.get(&queued).workspace.as_deref(), Some("testnode-w3"));
}

/// A run record the next daemon can't read (a newer potluck wrote it, or it is damaged) is said
/// in the log, with what that loses, not dropped in silence.
#[test]
fn a_run_record_that_cannot_be_read_is_logged() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("echo started; sleep 30"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.stop();
    {
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        db.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            [&format!("run/{id}"), "{\"phase\": \"sideways\"}"],
        )
        .unwrap();
    }
    f.restart();
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        log.contains(&format!("{id}: its run record can't be read")),
        "{log}"
    );
    // It is still watched, and a cancel still ends it.
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    assert_eq!(f.wait(&id).state, JobState::Cancelled);
}

/// A start cut short by a restart, adopted by the next daemon, keeps its run limit from when it
/// started: the time before the restart counts.
#[test]
fn an_adopted_start_keeps_its_run_limit() {
    let mut f = Fixture::new().start();
    let mut s = spec("echo ran; sleep 30");
    s.timeout_ms = 6_000;
    let started = Instant::now();
    let id = f.dispatch(s);
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    std::thread::sleep(Duration::from_millis(3_000));
    f.stop();
    {
        // What a daemon killed inside start leaves: the job starting, no handle, and the run
        // record as it was saved just before the start.
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        let json: String = db
            .query_row("SELECT json FROM jobs WHERE id = ?1", [&id], |r| r.get(0))
            .unwrap();
        let mut job: serde_json::Value = serde_json::from_str(&json).unwrap();
        job["state"] = "starting".into();
        job["handle"] = serde_json::Value::Null;
        job["started_at"] = serde_json::Value::Null;
        db.execute(
            "UPDATE jobs SET state = 'starting', json = ?2 WHERE id = ?1",
            [&id, &job.to_string()],
        )
        .unwrap();
        let key = format!("run/{id}");
        let run: String = db
            .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
            .unwrap();
        let mut run: serde_json::Value = serde_json::from_str(&run).unwrap();
        run["phase"] = "starting".into();
        assert!(run["starting_at"].is_u64(), "{run}");
        run.as_object_mut().unwrap().remove("deadline");
        db.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            [&key, &run.to_string()],
        )
        .unwrap();
    }
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::TimedOut, "{j:#?}");
    // 6 s after its start, not 6 s after the restart (9 s after the start).
    let took = started.elapsed();
    assert!(took < Duration::from_millis(8_000), "{took:?}");
    let (start, end) = (j.started_at.unwrap(), j.ended_at.unwrap());
    assert!(end - start < 7_500, "{j:#?}");
}

/// A start cut short by a restart whose run record can't be read: adopted, and the record is said
/// once, without claiming the job is queued again.
#[test]
fn an_unreadable_run_record_of_an_adopted_start_is_said_once() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("echo ran; sleep 30"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.stop();
    {
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        let json: String = db
            .query_row("SELECT json FROM jobs WHERE id = ?1", [&id], |r| r.get(0))
            .unwrap();
        let mut job: serde_json::Value = serde_json::from_str(&json).unwrap();
        job["state"] = "starting".into();
        job["handle"] = serde_json::Value::Null;
        db.execute(
            "UPDATE jobs SET state = 'starting', json = ?2 WHERE id = ?1",
            [&id, &job.to_string()],
        )
        .unwrap();
        db.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            [&format!("run/{id}"), "{\"phase\": \"sideways\"}"],
        )
        .unwrap();
    }
    f.restart();
    f.wait_for(&id, "adopted", |j| j.state == JobState::Running);
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    let said: Vec<&str> = log
        .lines()
        .filter(|l| l.contains(&format!("{id}: its run record can't be read")))
        .collect();
    assert_eq!(said.len(), 1, "{log}");
    assert!(!said[0].contains("queued again"), "{log}");
    assert!(log.contains(&format!(
        "{id}: its start was cut short by a restart, but it runs"
    )));
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    assert_eq!(f.wait(&id).state, JobState::Cancelled);
}

/// An update renames a new binary over the path the daemon started from before the daemon
/// restarts (and never restarts it with POTLUCK_NO_RESTART=1). Until it restarts, the daemon runs
/// its helpers from the binary it started from: a `_supervise` of another version may read its
/// arguments, env file or status file differently.
#[test]
fn a_replaced_binary_is_not_what_the_daemon_runs_its_helpers_from() {
    let mut f = Fixture::new();
    let bin_dir = f._dir.path().join("installed");
    std::fs::create_dir(&bin_dir).unwrap();
    let installed = bin_dir.join("potluck");
    common::copy_binary(Path::new(env!("CARGO_BIN_EXE_potluck")), &installed);
    f.opts.potluck_bin = installed.clone();
    let f = f.start();
    let before = f.dispatch(spec("echo before"));
    assert_eq!(f.wait(&before).state, JobState::Succeeded);
    assert_eq!(f.exec.launched(&before).potluck_bin, installed);
    // The update: a new file renamed over the old one. This "version" says so and fails.
    let new = bin_dir.join(".potluck.new");
    std::fs::write(&new, "#!/bin/sh\necho the new binary ran\nexit 3\n").unwrap();
    std::fs::set_permissions(&new, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    std::fs::rename(&new, &installed).unwrap();
    let after = f.dispatch(spec("echo after"));
    let j = f.wait(&after);
    let log = f.log(&after);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}\n{log}");
    assert!(
        log.contains("after") && !log.contains("the new binary"),
        "{log}"
    );
    // A copy of the binary it started from, in its own state directory.
    let used = f.exec.launched(&after).potluck_bin;
    assert!(used.starts_with(&f.paths.home), "{}", used.display());
    assert_eq!(
        std::fs::read(&used).unwrap(),
        std::fs::read(env!("CARGO_BIN_EXE_potluck")).unwrap()
    );
}

/// A running job's handle, which changes at every sample when it tracks the processes of a job
/// that keeps starting them, is written every few seconds and when the daemon stops: the next
/// daemon has the latest (what the executor's samples see it track).
#[test]
fn a_stopping_daemon_writes_the_latest_handle_of_a_running_job() {
    let mut f = Fixture::new().start();
    let job = f.dispatch(spec("sleep 30"));
    f.wait_for(&job, "running", |j| j.state == JobState::Running);
    f.exec.churn.store(true, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(1_500));
    f.exec.churn.store(false, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(300));
    let live = f.exec.sampled_tracked.load(Ordering::SeqCst);
    assert!(live >= 5, "{live}");
    f.stop();
    f.exec.sampled_tracked.store(usize::MAX, Ordering::SeqCst);
    f.restart();
    wait_until("the next daemon samples the job", || {
        f.exec.sampled_tracked.load(Ordering::SeqCst) != usize::MAX
    });
    assert_eq!(f.exec.sampled_tracked.load(Ordering::SeqCst), live);
}

/// The processes a macOS handle tracks (up to 10,000, about 670 KB of JSON) stay in the daemon:
/// `status`, `wait` and a job's record carry the handle without them.
#[test]
fn answers_carry_a_handle_without_its_process_lists() {
    let f = Fixture::new().start();
    let job = f.dispatch(spec("sleep 30"));
    f.wait_for(&job, "running", |j| j.state == JobState::Running);
    f.exec.churn.store(true, Ordering::SeqCst);
    wait_until("the job tracks 20 processes", || {
        f.exec.sampled_tracked.load(Ordering::SeqCst) >= 20
    });
    f.exec.churn.store(false, Ordering::SeqCst);
    let lists = |j: &JobRecord| match &j.handle {
        Some(Handle::Process {
            pid, tracked, gone, ..
        }) => (*pid > 0, tracked.len(), gone.len()),
        other => panic!("{other:?}"),
    };
    assert_eq!(lists(&f.get(&job)), (true, 0, 0));
    let waited = match f
        .client()
        .call(Request::Wait {
            job: job.clone(),
            timeout_ms: Some(10),
        })
        .unwrap()
    {
        Response::Job { job } => *job,
        other => panic!("{other:?}"),
    };
    assert_eq!(lists(&waited), (true, 0, 0));
    match f.client().call(Request::Status).unwrap() {
        Response::Status { status } => {
            let j = status.running.iter().find(|j| j.id == job).unwrap();
            assert_eq!(lists(j), (true, 0, 0));
        }
        other => panic!("{other:?}"),
    }
    // The daemon still has them.
    assert!(f.exec.sampled_tracked.load(Ordering::SeqCst) >= 20);
}

/// The daemon is stopped inside `Executor::start`: the invocation runs, but the store still says
/// the job is starting and has no handle. The next daemon finds the invocation and adopts it; it
/// doesn't queue the job again and run it twice.
#[test]
fn a_start_cut_short_by_a_restart_is_adopted_not_run_again() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("echo ran; sleep 2; exit 4"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.stop();
    {
        // What a daemon killed inside start leaves: the job starting, no handle.
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        let json: String = db
            .query_row("SELECT json FROM jobs WHERE id = ?1", [&id], |r| r.get(0))
            .unwrap();
        let mut job: serde_json::Value = serde_json::from_str(&json).unwrap();
        job["state"] = "starting".into();
        job["handle"] = serde_json::Value::Null;
        job["started_at"] = serde_json::Value::Null;
        db.execute(
            "UPDATE jobs SET state = 'starting', json = ?2 WHERE id = ?1",
            [&id, &job.to_string()],
        )
        .unwrap();
        let key = format!("run/{id}");
        let run: String = db
            .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
            .unwrap();
        let mut run: serde_json::Value = serde_json::from_str(&run).unwrap();
        run["phase"] = "starting".into();
        db.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            [&key, &run.to_string()],
        )
        .unwrap();
    }
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.exit_code_for_client(), 4, "{j:#?}");
    assert_eq!(f.log(&id).matches("ran").count(), 1, "{}", f.log(&id));
    let starts = f
        .exec
        .launched
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.job == id)
        .count();
    assert_eq!(starts, 1, "started again");
}

/// A start cut short by a restart whose invocation ended while no daemon ran: Linux unloads a unit
/// that exited 0, so only its status file says it ran. The wall clock was set back an hour after
/// the claim, so that file looks older than the claim; it is still this run's, and the job isn't
/// run again.
#[test]
fn a_start_cut_short_that_ended_after_the_clock_was_set_back_is_not_run_again() {
    let mut f = Fixture::new().start();
    f.exec.find_by_status.store(true, Ordering::SeqCst);
    let id = f.dispatch(spec("echo ran; sleep 2; exit 4"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.stop();
    {
        // What a daemon killed inside start leaves: the job starting, no handle.
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        let json: String = db
            .query_row("SELECT json FROM jobs WHERE id = ?1", [&id], |r| r.get(0))
            .unwrap();
        let mut job: serde_json::Value = serde_json::from_str(&json).unwrap();
        job["state"] = "starting".into();
        job["handle"] = serde_json::Value::Null;
        job["started_at"] = serde_json::Value::Null;
        db.execute(
            "UPDATE jobs SET state = 'starting', json = ?2 WHERE id = ?1",
            [&id, &job.to_string()],
        )
        .unwrap();
        let key = format!("run/{id}");
        let run: String = db
            .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
            .unwrap();
        let mut run: serde_json::Value = serde_json::from_str(&run).unwrap();
        run["phase"] = "starting".into();
        db.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            [&key, &run.to_string()],
        )
        .unwrap();
    }
    let status = f.paths.job_dir(&id).join("status.json");
    wait_until("the command ends", || status.exists());
    // Set back an hour: the status file got the clock's time then.
    f.clock.step_wall(-3_600_000);
    let stamped = std::time::SystemTime::now() - Duration::from_secs(3_600);
    std::fs::File::options()
        .write(true)
        .open(&status)
        .unwrap()
        .set_modified(stamped)
        .unwrap();
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.exit_code_for_client(), 4, "{j:#?}");
    assert_eq!(f.log(&id).matches("ran").count(), 1, "{}", f.log(&id));
}

/// Makes the store say what a daemon stopped inside `Executor::start` for `id` leaves: the job
/// starting without a handle, its run record as saved just before the start (with when the start
/// began), changed by `edit`.
fn cut_start_short(paths: &Paths, id: &str, edit: impl FnOnce(&mut serde_json::Value)) {
    let db = rusqlite::Connection::open(paths.db()).unwrap();
    let json: String = db
        .query_row("SELECT json FROM jobs WHERE id = ?1", [id], |r| r.get(0))
        .unwrap();
    let mut job: serde_json::Value = serde_json::from_str(&json).unwrap();
    job["state"] = "starting".into();
    job["handle"] = serde_json::Value::Null;
    job["started_at"] = serde_json::Value::Null;
    db.execute(
        "UPDATE jobs SET state = 'starting', json = ?2 WHERE id = ?1",
        [id, &job.to_string()],
    )
    .unwrap();
    let key = format!("run/{id}");
    let run: String = db
        .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
        .unwrap();
    let mut run: serde_json::Value = serde_json::from_str(&run).unwrap();
    run["phase"] = "starting".into();
    assert!(run["starting_at"].is_u64(), "{run}");
    run.as_object_mut().unwrap().remove("deadline");
    edit(&mut run);
    db.execute(
        "UPDATE kv SET value = ?2 WHERE key = ?1",
        [&key, &run.to_string()],
    )
    .unwrap();
}

/// A start cut short by a restart, and the next daemon can't tell whether it started the
/// invocation (the executor's `find` fails, as `systemctl --user show` does on a busy user
/// manager): it asks again until it can, and adopts the invocation. Its workspace isn't seeded
/// afresh under it and it isn't started a second time.
#[test]
fn a_start_cut_short_that_find_cannot_tell_about_is_not_run_again() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("echo ran; sleep 2; exit 4"));
    let j = f.wait_for(&id, "running", |j| j.state == JobState::Running);
    let ws = j.workspace.unwrap();
    f.stop();
    cut_start_short(&f.paths, &id, |_| {});
    *f.exec.fail_find.lock().unwrap() = 2;
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.exit_code_for_client(), 4, "{j:#?}");
    assert_eq!(f.log(&id).matches("ran").count(), 1, "{}", f.log(&id));
    let starts = f
        .exec
        .launched
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.job == id)
        .count();
    assert_eq!(starts, 1, "started again");
    assert!(!f.ws.removed.lock().unwrap().contains(&ws), "seeded afresh");
    assert_eq!(*f.exec.fail_find.lock().unwrap(), 0);
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        log.contains(&format!(
            "{id}: its start was cut short by a restart, but it runs; adopted"
        )),
        "{log}"
    );
}

/// The same when that start had started nothing: once the executor can tell, the job starts again
/// in its workspace, seeded afresh.
#[test]
fn a_start_cut_short_that_started_nothing_starts_once_find_can_tell() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("echo ran; sleep 30"));
    let j = f.wait_for(&id, "running", |j| j.state == JobState::Running);
    let ws = j.workspace.unwrap();
    let first = pid_of(j.handle.as_ref().unwrap());
    f.stop();
    // Nothing of the first start is left to find.
    f.exec.kill_all();
    wait_until("the first start's processes end", || !f.exec.alive(first));
    f.exec.names.lock().unwrap().clear();
    cut_start_short(&f.paths, &id, |_| {});
    *f.exec.fail_find.lock().unwrap() = 1;
    f.restart();
    let j = f.wait_for(&id, "running again", |j| {
        j.state == JobState::Running && j.handle.as_ref().is_some_and(|h| pid_of(h) != first)
    });
    assert_eq!(j.workspace.as_deref(), Some(ws.as_str()));
    assert!(
        f.ws.removed.lock().unwrap().contains(&ws),
        "not seeded afresh"
    );
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        log.contains(&format!(
            "{id}: its start had started nothing; starting again"
        )),
        "{log}"
    );
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    assert_eq!(f.wait(&id).state, JobState::Cancelled);
}

/// The same, with a cancel asked while the start was under way: the invocation is found and
/// cancelled (its processes are ended), not taken as never started while it runs on.
#[test]
fn a_cancelled_start_cut_short_that_find_cannot_tell_about_is_ended() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("echo ran; sleep 30"));
    let j = f.wait_for(&id, "running", |j| j.state == JobState::Running);
    let ws = j.workspace.unwrap();
    let pid = pid_of(j.handle.as_ref().unwrap());
    f.stop();
    cut_start_short(&f.paths, &id, |run| run["cancel"] = true.into());
    *f.exec.fail_find.lock().unwrap() = 1;
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Cancelled, "{j:#?}");
    wait_until("its processes are ended", || !f.exec.alive(pid));
    assert!(f.exec.finished.lock().unwrap().contains(&pid));
    assert!(
        !f.ws.removed.lock().unwrap().contains(&ws),
        "removed under it"
    );
}

/// Makes the store act as if the daemon died right after the first of its writes that end `job`
/// (its record saved as done, or its run record deleted): every write after that one fails.
fn die_after_first_end_write(paths: &Paths, job: &str) {
    let db = rusqlite::Connection::open(paths.db()).unwrap();
    let dead = "WHEN (SELECT dead FROM fault) = 1 BEGIN SELECT RAISE(ABORT, 'the daemon was killed'); END;";
    db.execute_batch(&format!(
        "CREATE TABLE fault (dead INTEGER NOT NULL);
         INSERT INTO fault VALUES (0);
         CREATE TRIGGER fault_job AFTER UPDATE ON jobs WHEN NEW.id = '{job}' AND NEW.done = 1
             BEGIN UPDATE fault SET dead = 1; END;
         CREATE TRIGGER fault_run AFTER DELETE ON kv WHEN OLD.key = 'run/{job}'
             BEGIN UPDATE fault SET dead = 1; END;
         CREATE TRIGGER dead_job_insert BEFORE INSERT ON jobs {dead}
         CREATE TRIGGER dead_job_update BEFORE UPDATE ON jobs {dead}
         CREATE TRIGGER dead_kv_insert BEFORE INSERT ON kv {dead}
         CREATE TRIGGER dead_kv_update BEFORE UPDATE ON kv {dead}
         CREATE TRIGGER dead_kv_delete BEFORE DELETE ON kv {dead}
         CREATE TRIGGER dead_ws_insert BEFORE INSERT ON workspaces {dead}
         CREATE TRIGGER dead_ws_update BEFORE UPDATE ON workspaces {dead}"
    ))
    .unwrap();
}

/// Undoes [`die_after_first_end_write`], as a restart on the same store would see it.
fn revive(paths: &Paths) {
    let db = rusqlite::Connection::open(paths.db()).unwrap();
    db.execute_batch(
        "DROP TRIGGER fault_job; DROP TRIGGER fault_run;
         DROP TRIGGER dead_job_insert; DROP TRIGGER dead_job_update;
         DROP TRIGGER dead_kv_insert; DROP TRIGGER dead_kv_update; DROP TRIGGER dead_kv_delete;
         DROP TRIGGER dead_ws_insert; DROP TRIGGER dead_ws_update;
         DROP TABLE fault;",
    )
    .unwrap();
}

/// The daemon is killed while it records the end of a cancelled job, between its two writes. The
/// next daemon still ends it as cancelled (130): the job's record is saved as ended before its run
/// record (which says the end was a cancel) goes, and a run record left without its job is dropped.
#[test]
fn a_kill_while_recording_an_end_keeps_how_it_ended() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("sleep 30"));
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    die_after_first_end_write(&f.paths, &id);
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    f.wait(&id);
    f.stop();
    revive(&f.paths);
    f.restart();
    let j = f.wait(&id);
    assert_eq!(
        (j.state, j.exit_code_for_client()),
        (JobState::Cancelled, 130),
        "{j:#?}"
    );
    f.stop();
    let db = rusqlite::Connection::open(f.paths.db()).unwrap();
    let runs: i64 = db
        .query_row("SELECT count(*) FROM kv WHERE key LIKE 'run/%'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(runs, 0, "a run record outlived its job");
}

/// Pins that no job or workspace holds are dropped when the daemon starts: those of a dispatch
/// whose daemon was killed between pinning its commits and recording the job, and those of a job
/// whose daemon was killed between its end and its unpinning. A running job's pins and its
/// workspace's stay.
#[test]
fn pins_nothing_holds_are_dropped_at_start() {
    let mut f = Fixture::new().start();
    let running = f.dispatch(spec("sleep 30"));
    let j = f.wait_for(&running, "running", |j| j.state == JobState::Running);
    let ws = j.workspace.unwrap();
    let ended = f.dispatch(spec("true"));
    f.wait(&ended);
    // Killed before its unpinning: the job is over, its pins are still there.
    f.ws.pin(REPO, "c0ffee", &ended).unwrap();
    // Killed between pinning and recording: no new job reaches the store.
    {
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        db.execute_batch(
            "CREATE TRIGGER no_new_jobs BEFORE INSERT ON jobs
                 WHEN NOT EXISTS (SELECT 1 FROM jobs WHERE id = NEW.id)
                 BEGIN SELECT RAISE(ABORT, 'the daemon was killed'); END;",
        )
        .unwrap();
    }
    // All the RAM: it stays queued (in memory only) behind the running job.
    let mut big = spec("true");
    big.request.ram_mib = 8 << 10;
    let lost = f.dispatch(big);
    assert!(f.ws.pinned(&lost));
    f.stop();
    {
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        db.execute_batch("DROP TRIGGER no_new_jobs;").unwrap();
    }
    f.restart();
    assert!(!f.ws.pinned(&lost), "the unrecorded dispatch's pins stayed");
    assert!(!f.ws.pinned(&ended), "the ended job's pins stayed");
    assert!(f.ws.pinned(&running), "the running job lost its pins");
    assert!(f.ws.pinned(&ws), "its workspace lost its pins");
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    let line = log
        .lines()
        .find(|l| l.contains("pins no job or workspace here holds, dropped: "))
        .unwrap_or_else(|| panic!("{log}"));
    assert!(
        line.contains(&format!("{ended} in ")) && line.contains(&format!("{lost} in ")),
        "{line}"
    );
}

#[test]
fn a_restart_finds_jobs_whose_processes_vanished_lost() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("sleep 30"));
    let j = f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.stop();
    // Killed while no daemon watched, so nothing records how it ended.
    f.exec
        .kill_group(pid_of(j.handle.as_ref().unwrap()), libc::SIGKILL);
    std::thread::sleep(Duration::from_millis(300));
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Lost);
    assert_eq!(j.exit_code_for_client(), 125);
    // The state goes before the message wherever it is shown, so the message doesn't repeat it.
    assert_eq!(
        j.message.as_deref(),
        Some("its processes were gone after a daemon restart, and nothing recorded how it ended")
    );
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        log.contains(&format!("{id}: lost: its processes were gone"))
            && !log.contains("lost: lost:"),
        "{log}"
    );
}

/// The record of a removed workspace is pruned only once its files are gone: a removal that keeps
/// failing is retried at each upkeep however long ago the workspace was last used, and its
/// record goes after that.
#[test]
fn a_removed_workspace_is_remembered_until_its_files_are_gone() {
    let mut f = Fixture::new();
    f.opts.retention_ms = 1_000;
    let f = f.start();
    let id = f.dispatch(spec("true"));
    let ws = f.wait(&id).workspace.unwrap();
    let root = f.workspace(&ws).path;
    f.ws.fail_remove.store(true, Ordering::SeqCst);
    let r = f.client().call(Request::Release {
        workspace: ws.clone(),
    });
    assert!(r.is_err(), "{r:?}");
    std::thread::sleep(Duration::from_millis(1_200));
    // An upkeep (every 10 minutes) while the removal still fails: its last use is past the
    // retention time.
    f.clock.pass(10 * 60_000);
    wait_until("the removal is tried again", || {
        std::fs::read_to_string(f.paths.daemon_log())
            .unwrap()
            .contains(&format!("workspace {ws}: removing what is left of it"))
    });
    // The next upkeep, with removals working again, removes it.
    f.ws.fail_remove.store(false, Ordering::SeqCst);
    f.clock.pass(10 * 60_000);
    wait_until("the workspace is removed", || {
        f.ws.removed.lock().unwrap().contains(&ws)
    });
    assert!(!root.exists(), "{} is still there", root.display());
    // And then its record goes.
    f.clock.pass(10 * 60_000);
    wait_until("its record is pruned", || {
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        let n: i64 = db
            .query_row(
                "SELECT count(*) FROM workspaces WHERE id = ?1",
                [&ws],
                |r| r.get(0),
            )
            .unwrap();
        n == 0
    });
}

/// A removal moves the workspace's tree aside to `<path>.removing` and deletes it there. When
/// that deletion fails (root-owned files a job made through a container, immutable files) and the
/// TMPDIR goes, the tree used to stay for good: nothing looked for it, so the removal was never
/// tried again and the record was pruned after the retention period. It is now tried again at
/// each upkeep, and the record kept, until the tree is gone.
#[test]
fn a_tree_left_aside_by_a_failed_removal_is_removed_again() {
    let mut f = Fixture::new();
    f.opts.retention_ms = 1_000;
    let f = f.start();
    let id = f.dispatch(spec("true"));
    let ws = f.wait(&id).workspace.unwrap();
    let root = f.workspace(&ws).path;
    let aside = root.with_file_name(format!("{ws}.removing"));
    f.ws.stuck_aside.store(true, Ordering::SeqCst);
    let r = f.client().call(Request::Release {
        workspace: ws.clone(),
    });
    assert!(r.is_err(), "{r:?}");
    assert!(aside.is_dir() && !root.exists() && !f.paths.workspace_tmp(&ws).exists());
    std::thread::sleep(Duration::from_millis(1_200));
    // An upkeep while the deletion still fails: its last use is past the retention time.
    f.clock.pass(10 * 60_000);
    wait_until("the removal is tried again", || {
        std::fs::read_to_string(f.paths.daemon_log())
            .unwrap()
            .contains(&format!("workspace {ws}: removing what is left of it"))
    });
    // The next upkeep, with the deletion working again, removes it.
    f.ws.stuck_aside.store(false, Ordering::SeqCst);
    f.clock.pass(10 * 60_000);
    wait_until("the workspace is removed", || {
        f.ws.removed.lock().unwrap().contains(&ws)
    });
    assert!(!aside.exists(), "{} is still there", aside.display());
    // And then its record goes.
    f.clock.pass(10 * 60_000);
    wait_until("its record is pruned", || {
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        let n: i64 = db
            .query_row(
                "SELECT count(*) FROM workspaces WHERE id = ?1",
                [&ws],
                |r| r.get(0),
            )
            .unwrap();
        n == 0
    });
}

#[test]
fn a_removal_that_failed_or_was_cut_short_is_done_after_a_restart() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("true"));
    let ws = f.wait(&id).workspace.unwrap();
    let root = f.workspace(&ws).path;
    assert!(root.is_dir());
    // The record says removed and the files stay, as when the daemon is killed mid-removal.
    f.ws.fail_remove.store(true, Ordering::SeqCst);
    let r = f.client().call(Request::Release {
        workspace: ws.clone(),
    });
    assert!(
        r.as_ref()
            .is_err_and(|e| e.to_string().contains("removal failed")),
        "{r:?}"
    );
    assert!(root.is_dir());
    f.ws.fail_remove.store(false, Ordering::SeqCst);
    f.stop();
    f.restart();
    // The fake removes the files, then records the removal.
    wait_until("the workspace is removed", || {
        f.ws.removed.lock().unwrap().contains(&ws)
    });
    assert!(!root.exists(), "{} is still there", root.display());
    // Once its files are gone it is left alone.
    f.stop();
    f.restart();
    std::thread::sleep(Duration::from_millis(500));
    let n =
        f.ws.removed
            .lock()
            .unwrap()
            .iter()
            .filter(|w| **w == ws)
            .count();
    assert_eq!(n, 1);
}

#[test]
fn a_cancel_of_a_starting_job_holds_after_a_restart() {
    let f = Fixture::new().start();
    f.ws.hold_create.store(true, Ordering::SeqCst);
    let id = f.dispatch(spec("echo ran"));
    f.wait_for(&id, "starting", |j| j.state == JobState::Starting);
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    // The daemon is killed while the workspace is still being seeded.
    let g = f.crash_copy().start();
    f.ws.hold_create.store(false, Ordering::SeqCst);
    let j = g.wait(&id);
    assert_eq!(
        (j.state, j.exit_code_for_client()),
        (JobState::Cancelled, 130),
        "{j:#?}"
    );
    assert!(g.exec.launched.lock().unwrap().is_empty(), "it never ran");
    assert!(!g.ws.pinned(&id), "its pins went with it");
}

/// A job caught while its workspace was being seeded starts again in that workspace after a
/// restart, so a job queued into the workspace meanwhile still runs there, after it (the
/// workspace isn't removed while a job is queued into it).
#[test]
fn a_restart_while_seeding_keeps_the_workspace_and_what_is_queued_into_it() {
    let f = Fixture::new().start();
    f.ws.hold_create.store(true, Ordering::SeqCst);
    let first = f.dispatch(spec("echo first > made-by-first"));
    let j = f.wait_for(&first, "starting", |j| j.state == JobState::Starting);
    let ws = j.workspace.unwrap();
    let mut s = spec("cat made-by-first");
    s.workspace = Some(ws.clone());
    let second = f.dispatch(s);
    assert_eq!(f.get(&second).state, JobState::Queued);
    // The daemon is killed while the workspace is still being seeded.
    let g = f.crash_copy().start();
    f.ws.hold_create.store(false, Ordering::SeqCst);
    let a = g.wait(&first);
    let b = g.wait(&second);
    assert_eq!(
        (a.state, a.workspace.as_deref()),
        (JobState::Succeeded, Some(ws.as_str())),
        "{a:#?}"
    );
    assert_eq!(b.state, JobState::Succeeded, "{b:#?}");
    assert!(g.log(&second).contains("first"), "{}", g.log(&second));
}

/// A start that failed is over: a daemon killed before it recorded the job's end ends it so after
/// the restart, and doesn't start it again.
#[test]
fn a_start_failure_outlives_a_restart() {
    let mut f = Fixture::new().start();
    f.exec.fail_start.store(true, Ordering::SeqCst);
    let id = f.dispatch(spec("echo ran"));
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Failed);
    f.stop();
    {
        // What a daemon killed between deciding the end and recording it leaves: the job still
        // starting, its run record with the ending.
        let db = rusqlite::Connection::open(f.paths.db()).unwrap();
        let json: String = db
            .query_row("SELECT json FROM jobs WHERE id = ?1", [&id], |r| r.get(0))
            .unwrap();
        let mut job: serde_json::Value = serde_json::from_str(&json).unwrap();
        let message = job["message"].clone();
        job["state"] = "starting".into();
        job["ended_at"] = serde_json::Value::Null;
        job["message"] = serde_json::Value::Null;
        db.execute(
            "UPDATE jobs SET state = 'starting', done = 0, ended_at = NULL, json = ?2 WHERE id = ?1",
            [&id, &job.to_string()],
        )
        .unwrap();
        let run = serde_json::json!({
            "phase": "finishing",
            "new_workspace": true,
            "cancel": false,
            "adopted": false,
            "ending": {
                "state": "failed",
                "exit_code": null,
                "signal": null,
                "message": message,
                "at": now_ms(),
            },
            "term_at": null,
            "kill_at": null,
        });
        db.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2)",
            [&format!("run/{id}"), &run.to_string()],
        )
        .unwrap();
    }
    f.exec.fail_start.store(false, Ordering::SeqCst);
    f.restart();
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Failed, "{j:#?}");
    assert!(
        j.message
            .as_deref()
            .unwrap_or("")
            .contains("systemd said no"),
        "{j:#?}"
    );
    assert!(f.exec.launched.lock().unwrap().is_empty(), "started again");
}

#[test]
fn a_request_that_panics_is_logged_without_its_env() {
    let f = Fixture::new().start();
    let mut s = spec("true");
    s.env.insert("API_TOKEN".into(), "hunter2-secret".into());
    s.repo.source = PathBuf::from("/panic/.git");
    let e = f
        .client()
        .call(Request::Dispatch { spec: Box::new(s) })
        .unwrap_err()
        .to_string();
    assert!(e.contains("internal error: the sync blew up"), "{e}");
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        log.contains("request dispatch: panicked: the sync blew up"),
        "{log}"
    );
    assert!(!log.contains("hunter2"), "{log}");
}

/// Reads until the daemon closes the connection; how many bytes came. Fails if it stays open.
fn read_until_closed(s: &mut std::os::unix::net::UnixStream, within: Duration) -> usize {
    use std::io::Read;
    s.set_read_timeout(Some(within)).unwrap();
    let mut n = 0;
    let mut buf = [0u8; 64 << 10];
    loop {
        match s.read(&mut buf) {
            Ok(0) => return n,
            Ok(k) => n += k,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return n,
            Err(e) => panic!("still open after {within:?} ({n} bytes read): {e}"),
        }
    }
}

#[test]
fn idle_connections_are_let_go() {
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    let mut f = Fixture::new();
    f.opts.idle_ms = 300;
    let f = f.start();
    // One that never sends a request, one that stops halfway through one, and a client that is
    // quiet after an answer: each held a daemon thread for good.
    let mut silent = UnixStream::connect(f.paths.socket()).unwrap();
    let mut half = UnixStream::connect(f.paths.socket()).unwrap();
    half.write_all(br#"{"ctx": {"#).unwrap();
    let mut quiet = f.client();
    quiet.call(Request::Ping).unwrap();
    assert_eq!(read_until_closed(&mut silent, Duration::from_secs(5)), 0);
    assert_eq!(read_until_closed(&mut half, Duration::from_secs(5)), 0);
    std::thread::sleep(Duration::from_millis(600));
    let e = quiet.call(Request::Ping).unwrap_err();
    assert!(potluck::client::is_disconnected(&e), "{e:#}");

    // One that asks for 3 MB of output and takes none of it: the daemon stops writing after the
    // idle limit, so the reader gets part of it and then the end.
    let id = f.dispatch(spec("head -c 3000000 /dev/zero | tr '\\0' x"));
    assert_eq!(f.wait(&id).exit_code, Some(0));
    let mut stuck = UnixStream::connect(f.paths.socket()).unwrap();
    let req = potluck::proto::ClientRequest {
        ctx: ClientContext::default(),
        req: Request::Logs {
            job: id.clone(),
            offset: 0,
            max: 4 << 20,
            wait_ms: 0,
        },
        auth: None,
    };
    potluck::proto::write_msg(&mut stuck, &req).unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    let got = read_until_closed(&mut stuck, Duration::from_secs(5));
    assert!(got < 3_000_000, "{got} bytes");

    // Those that keep asking are served as before.
    let mut c = f.client();
    for _ in 0..3 {
        c.call(Request::Ping).unwrap();
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn a_refresh_that_hangs_is_not_started_again_beside_itself() {
    let mut f = Fixture::new();
    f.opts.facts_refresh_ms = 100;
    let calls = Arc::new(AtomicU64::new(0));
    let hang = Arc::new(AtomicBool::new(true));
    let parts = Parts {
        executor: f.exec.clone(),
        workspaces: f.ws.clone(),
        scheduler: Arc::new(RealScheduler),
        facts: Box::new({
            let (calls, hang) = (calls.clone(), hang.clone());
            move || {
                // The first refresh (after the start's detection) hangs, as a stuck `lspci` or
                // `system_profiler` would.
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    while hang.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                Ok(facts())
            }
        }),
        login_env: BTreeMap::new(),
        clock: f.clock.clone(),
    };
    f.daemon = Some(Daemon::start(&f.paths, &f.config, parts, f.opts.clone()).unwrap());
    wait_until("the first refresh", || calls.load(Ordering::SeqCst) >= 2);
    std::thread::sleep(Duration::from_millis(1_000));
    // Each tick started another refresh beside the one that hangs: threads without end.
    let n = calls.load(Ordering::SeqCst);
    hang.store(false, Ordering::SeqCst);
    assert_eq!(n, 2, "refreshes started while the first one hung");
    wait_until("refreshes go on", || calls.load(Ordering::SeqCst) >= 4);
}

#[test]
fn only_one_daemon_per_home() {
    let f = Fixture::new().start();
    let parts = Parts {
        executor: f.exec.clone(),
        workspaces: f.ws.clone(),
        scheduler: Arc::new(RealScheduler),
        facts: Box::new(|| Ok(facts())),
        login_env: BTreeMap::new(),
        clock: f.clock.clone(),
    };
    let e = Daemon::start(&f.paths, &f.config, parts, f.opts.clone())
        .err()
        .unwrap()
        .to_string();
    assert!(e.contains("another potluck daemon"), "{e}");
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(f.paths.socket())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[test]
fn seeding_failures_fail_the_job_and_remove_the_workspace() {
    let f = Fixture::new().start();
    let mut s = spec("true");
    s.repo.commit = "bad".repeat(13) + "x";
    let id = f.dispatch(s);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Failed);
    assert!(j.message.unwrap().contains("could not seed workspace"));
    let ws = j.workspace.unwrap();
    wait_until("the half-made workspace is removed", || {
        f.ws.removed.lock().unwrap().contains(&ws)
    });
    wait_until("the pins are released", || {
        !f.ws.pinned(&id) && !f.ws.pinned(&ws)
    });
}

/// A job queued into a workspace whose seeding then failed fails (exit 1, as its owner fails one
/// whose workspace went from another machine), and the daemon's log says so.
#[test]
fn a_job_queued_into_a_workspace_that_failed_to_seed_fails_and_says_so() {
    let f = Fixture::new().start();
    f.ws.hold_create.store(true, Ordering::SeqCst);
    let mut s = spec("true");
    s.repo.commit = "bad".repeat(13) + "x";
    let first = f.dispatch(s.clone());
    let ws = f
        .wait_for(&first, "starting", |j| j.state == JobState::Starting)
        .workspace
        .unwrap();
    s.workspace = Some(ws.clone());
    let second = f.dispatch(s);
    f.ws.hold_create.store(false, Ordering::SeqCst);
    let j = f.wait(&second);
    assert_eq!(
        (j.state, j.exit_code_for_client()),
        (JobState::Failed, 1),
        "{j:#?}"
    );
    assert_eq!(
        j.message.as_deref(),
        Some(format!("workspace {ws} is gone").as_str())
    );
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(
        log.contains(&format!("{second}: failed: workspace {ws} is gone")),
        "{log}"
    );
}

#[test]
fn status_why_and_nodes() {
    let f = Fixture::new().start();
    let mut big = spec("sleep 30");
    big.request.ram_mib = 6 << 10;
    let running = f.dispatch(big);
    f.wait_for(&running, "running", |j| j.state == JobState::Running);
    let mut low = spec("true");
    low.request.ram_mib = 4 << 10;
    low.priority = 1;
    let low = f.dispatch(low);
    let mut high = spec("true");
    high.request.ram_mib = 4 << 10;
    high.priority = 5;
    let high = f.dispatch(high);
    let mut c = f.client();
    match c.call(Request::Status).unwrap() {
        Response::Status { status } => {
            let q: Vec<&str> = status.queued.iter().map(|j| j.id.as_str()).collect();
            assert_eq!(q, [high.as_str(), low.as_str()]);
            assert_eq!(status.running.len(), 1);
            assert_eq!(status.nodes[0].ram_reserved_mib, 6 << 10);
            assert_eq!(status.nodes[0].ram_capacity_mib, 8 << 10);
        }
        other => panic!("{other:?}"),
    }
    match c.call(Request::Why { job: high.clone() }).unwrap() {
        Response::Why { why } => {
            assert_eq!(why.job, high);
            assert!(why.lines.iter().any(|l| l.contains("RAM")), "{why:?}");
        }
        other => panic!("{other:?}"),
    }
    c.call(Request::Cancel {
        job: running.clone(),
    })
    .unwrap();
    assert_eq!(f.wait(&high).state, JobState::Succeeded);
    assert_eq!(f.wait(&low).state, JobState::Succeeded);
    match c.call(Request::Status).unwrap() {
        Response::Status { status } => assert_eq!(status.recent.len(), 3),
        other => panic!("{other:?}"),
    }
}

/// Run as a job's command by `nested_lease_check`: asks the daemon whether this job's lease is
/// live, as a potluck call from inside a job would.
#[test]
fn nested_helper() {
    let Ok(home) = std::env::var("NESTED_HOME") else {
        return;
    };
    let paths = Paths {
        home: PathBuf::from(home),
        config: PathBuf::from("/nonexistent"),
    };
    let job = std::env::var("POTLUCK_JOB").unwrap();
    let token = std::env::var("POTLUCK_LEASE").unwrap();
    let mut c = Client::connect(
        &paths,
        ClientContext {
            job: Some(job.clone()),
            lease_token: Some(token.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    for (label, t) in [("mine", token.clone()), ("forged", "0".repeat(32))] {
        match c
            .call(Request::LeaseCheck {
                job: job.clone(),
                token: t,
            })
            .unwrap()
        {
            Response::Lease {
                live,
                grant,
                remaining_ms,
            } => println!(
                "NESTED {label} live={live} ram={} left={}",
                grant.map(|g| g.ram_mib).unwrap_or(0),
                remaining_ms.is_some_and(|ms| ms > 0),
            ),
            other => panic!("{other:?}"),
        }
    }
}

/// The time a job's lease has left (a nested dispatch must fit in it) is measured on the clock
/// that ends it: a step of the wall clock doesn't change it.
#[test]
fn a_lease_says_its_time_left_whatever_the_wall_clock_says() {
    let f = Fixture::new().start();
    let mut s = spec("echo started; sleep 30");
    s.timeout_ms = 60_000;
    let id = f.dispatch(s);
    f.wait_for(&id, "running", |j| j.state == JobState::Running);
    let token = f.exec.launched(&id).env["POTLUCK_LEASE"].clone();
    let left = |f: &Fixture| match f
        .client()
        .call(Request::LeaseCheck {
            job: id.clone(),
            token: token.clone(),
        })
        .unwrap()
    {
        Response::Lease {
            live: true,
            remaining_ms: Some(ms),
            ..
        } => ms,
        other => panic!("{other:?}"),
    };
    for step in [86_400_000, -2 * 86_400_000] {
        f.clock.step_wall(step);
        let ms = left(&f);
        assert!((50_000..=60_000).contains(&ms), "after {step}: {ms}");
    }
    f.client()
        .call(Request::Cancel { job: id.clone() })
        .unwrap();
    f.wait(&id);
}

#[test]
fn nested_lease_check() {
    let f = Fixture::new().start();
    let mut s = spec("");
    s.cmd = vec![
        std::env::current_exe().unwrap().display().to_string(),
        "--exact".into(),
        "nested_helper".into(),
        "--nocapture".into(),
        "--test-threads=1".into(),
    ];
    s.env
        .insert("NESTED_HOME".into(), f.paths.home.display().to_string());
    let id = f.dispatch(s);
    let j = f.wait(&id);
    let log = f.log(&id);
    assert_eq!(j.state, JobState::Succeeded, "{log}");
    assert!(
        log.contains("NESTED mine live=true ram=512 left=true"),
        "{log}"
    );
    assert!(
        log.contains("NESTED forged live=false ram=0 left=false"),
        "{log}"
    );
    // After the job, its token is no longer live.
    match f
        .client()
        .call(Request::LeaseCheck {
            job: id,
            token: j.lease.unwrap().token,
        })
        .unwrap()
    {
        Response::Lease { live, .. } => assert!(!live),
        other => panic!("{other:?}"),
    }
}

/// The state directory holds job specs with their `--env` values, logs and sources: only this
/// user may read it, also when something else created it first with the umask's mode (the
/// daemon's checks, `potluck service install`, an older potluck).
#[test]
fn the_state_directory_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new();
    std::fs::create_dir_all(&f.paths.home).unwrap();
    std::fs::set_permissions(&f.paths.home, std::fs::Permissions::from_mode(0o755)).unwrap();
    let f = f.start();
    let mode = std::fs::metadata(&f.paths.home)
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o700, "{mode:o}");
}

/// A store write that fails (a full disk; here the jobs table is briefly gone) is made again
/// once the store works: the store ends up with the job's ending. (It kept the job running, so
/// once the ended job left memory, `wait` and `get` read it as running for good.)
#[test]
fn a_store_write_that_failed_is_made_again() {
    let f = Fixture::new().start();
    let job = f.dispatch(spec(
        "while [ ! -e \"$TMPDIR/go\" ]; do sleep 0.05; done; exit 5",
    ));
    f.wait_for(&job, "running", |j| j.state == JobState::Running);
    let tmp = PathBuf::from(&f.exec.launched(&job).env["TMPDIR"]);
    let db = rusqlite::Connection::open(f.paths.db()).unwrap();
    let state = || -> Option<String> {
        db.query_row("SELECT state FROM jobs WHERE id = ?1", [&job], |r| r.get(0))
            .ok()
    };
    assert_eq!(state().as_deref(), Some("running"));
    db.execute_batch("ALTER TABLE jobs RENAME TO jobs_away")
        .unwrap();
    std::fs::write(tmp.join("go"), "").unwrap();
    let j = f.wait(&job);
    assert_eq!((j.state, j.exit_code), (JobState::Failed, Some(5)));
    db.execute_batch("ALTER TABLE jobs_away RENAME TO jobs")
        .unwrap();
    let start = Instant::now();
    while state().as_deref() != Some("failed") {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the store still has {job} {:?}",
            state()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A workspace whose checkout no longer matches its commit fails verification before anything
/// runs in it: the job fails and says why (spec: the workspace's HEAD is checked before anything
/// runs).
#[test]
fn a_workspace_that_fails_verification_fails_the_job() {
    let f = Fixture::new().start();
    let first = f.dispatch(spec("true"));
    let ws = f.wait(&first).workspace.unwrap();
    std::fs::write(f.paths.workspace(&ws).join("COMMIT"), "something else").unwrap();
    let mut s = spec("echo ran");
    s.workspace = Some(ws.clone());
    let id = f.dispatch(s);
    let j = f.wait(&id);
    assert_eq!(
        (j.state, j.exit_code_for_client()),
        (JobState::Failed, 1),
        "{j:#?}"
    );
    let msg = j.message.unwrap_or_default();
    assert!(
        msg.starts_with(&format!("workspace {ws} failed verification")),
        "{msg}"
    );
    assert!(
        f.exec.launched.lock().unwrap().iter().all(|l| l.job != id),
        "it ran"
    );
}

/// An executor that can't start the invocation fails the job, and its lease and pins go: a job
/// that needs the whole machine starts next.
#[test]
fn a_start_that_fails_fails_the_job_and_frees_its_lease() {
    let f = Fixture::new().start();
    f.exec.fail_start.store(true, Ordering::SeqCst);
    let mut whole = spec("true");
    whole.request.ram_mib = 8 << 10;
    let id = f.dispatch(whole.clone());
    let j = f.wait(&id);
    assert_eq!(
        (j.state, j.exit_code_for_client()),
        (JobState::Failed, 1),
        "{j:#?}"
    );
    assert_eq!(
        j.message.as_deref(),
        Some("could not start: systemd said no")
    );
    wait_until("its pins are released", || !f.ws.pinned(&id));
    f.exec.fail_start.store(false, Ordering::SeqCst);
    let next = f.dispatch(whole);
    assert_eq!(f.wait(&next).state, JobState::Succeeded);
}

/// The supervisor killed while the daemon watches: nothing recorded how the command ended, and
/// the daemon saw it go, so the job failed (Lost is for endings no daemon saw).
#[test]
fn a_supervisor_killed_while_the_daemon_watches_fails_the_job() {
    let f = Fixture::new().start();
    let id = f.dispatch(spec("sleep 30"));
    let j = f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.exec
        .kill_group(pid_of(j.handle.as_ref().unwrap()), libc::SIGKILL);
    let j = f.wait(&id);
    assert_eq!(
        (j.state, j.exit_code_for_client()),
        (JobState::Failed, 1),
        "{j:#?}"
    );
    assert_eq!(
        j.message.as_deref(),
        Some("killed: its processes ended without recording an exit status")
    );
}

/// The same once a restarted daemon has seen the adopted job running: the supervisor killed then
/// was seen to go by this daemon, so the job failed; Lost is for endings no daemon saw.
#[test]
fn a_supervisor_killed_after_a_restart_while_the_daemon_watches_fails_the_job() {
    let mut f = Fixture::new().start();
    let id = f.dispatch(spec("sleep 30"));
    let j = f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.stop();
    f.restart();
    // Some samples of it running, by the new daemon.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(f.get(&id).state, JobState::Running);
    f.exec
        .kill_group(pid_of(j.handle.as_ref().unwrap()), libc::SIGKILL);
    let j = f.wait(&id);
    assert_eq!(
        (j.state, j.exit_code_for_client()),
        (JobState::Failed, 1),
        "{j:#?}"
    );
}

/// A job whose processes ended without a status file while the disk holding it was full (its
/// supervisor could not write it) says so, not that it was killed. Any disk with less than 256 MiB
/// free counts as full, as for a seeding: here a small disk image.
#[cfg(target_os = "macos")]
#[test]
fn a_job_ended_without_a_status_on_a_full_disk_says_so() {
    let img = tempfile::tempdir().unwrap();
    let mnt = tempfile::tempdir().unwrap();
    let image = img.path().join("d.dmg");
    let ok = |c: &mut Command| c.status().is_ok_and(|s| s.success());
    let attached = ok(Command::new("hdiutil")
        .args([
            "create",
            "-quiet",
            "-size",
            "32m",
            "-fs",
            "APFS",
            "-volname",
            "potlucktest",
        ])
        .arg(&image))
        && ok(Command::new("hdiutil")
            .args(["attach", "-quiet", "-nobrowse", "-noverify", "-mountpoint"])
            .arg(mnt.path())
            .arg(&image));
    if !attached {
        eprintln!("no disk image at hand: skipped");
        return;
    }
    let detach = || {
        let _ = Command::new("hdiutil")
            .args(["detach", "-quiet", "-force"])
            .arg(mnt.path())
            .status();
    };
    let f = Fixture::with_home(Some(mnt.path().join("home"))).start();
    let id = f.dispatch(spec("sleep 30"));
    let j = f.wait_for(&id, "running", |j| j.state == JobState::Running);
    f.exec
        .kill_group(pid_of(j.handle.as_ref().unwrap()), libc::SIGKILL);
    let j = f.wait(&id);
    drop(f);
    detach();
    assert_eq!(j.state, JobState::Failed, "{j:#?}");
    let message = j.message.unwrap_or_default();
    assert!(
        message
            .starts_with("its processes ended, but how could not be recorded: the disk holding ")
            && message.ends_with("free); free space there"),
        "{message}"
    );
}

/// An idle workspace (its hold over) still takes a dispatch; the run holds it again.
#[test]
fn a_dispatch_into_an_idle_workspace_runs_there() {
    let f = Fixture::new().start();
    let first = f.dispatch(spec("true"));
    let ws = f.wait(&first).workspace.unwrap();
    f.client()
        .call(Request::Hold {
            workspace: ws.clone(),
            duration_ms: 1,
        })
        .unwrap();
    let start = Instant::now();
    while f.workspace(&ws).state != WorkspaceState::Idle {
        assert!(start.elapsed() < Duration::from_secs(10), "never idle");
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut s = spec("echo again");
    s.workspace = Some(ws.clone());
    let id = f.dispatch(s);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    assert_eq!(j.workspace.as_deref(), Some(ws.as_str()));
    let w = f.workspace(&ws);
    assert_eq!(w.state, WorkspaceState::Held);
    assert!(w.held_until >= j.ended_at.unwrap() + MIN_HOLD_MS, "{w:#?}");
}

/// `release` of a workspace with an invocation queued in it ends the hold but keeps it; the
/// invocation runs there later.
#[test]
fn release_keeps_a_workspace_with_a_queued_invocation() {
    let f = Fixture::new().start();
    let first = f.dispatch(spec("true"));
    let ws = f.wait(&first).workspace.unwrap();
    // The whole machine's RAM, so the next one queues.
    let mut big = spec("sleep 30");
    big.request.ram_mib = 8 << 10;
    let big = f.dispatch(big);
    f.wait_for(&big, "running", |j| j.state == JobState::Running);
    let mut s = spec("echo queued");
    s.workspace = Some(ws.clone());
    let queued = f.dispatch(s);
    assert_eq!(f.get(&queued).state, JobState::Queued);
    match f
        .client()
        .call(Request::Release {
            workspace: ws.clone(),
        })
        .unwrap()
    {
        Response::Workspace { workspace } => {
            assert_ne!(workspace.state, WorkspaceState::Removed, "{workspace:#?}")
        }
        other => panic!("{other:?}"),
    }
    assert!(f.paths.workspace(&ws).is_dir());
    f.client().call(Request::Cancel { job: big }).unwrap();
    let j = f.wait(&queued);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    assert_eq!(j.workspace.as_deref(), Some(ws.as_str()));
}

/// `release` of a workspace whose invocation is ending (its cleanup still under way) ends the
/// hold, and the invocation's end doesn't hold the workspace again: the release isn't undone.
#[test]
fn a_release_while_the_invocation_is_ending_is_kept() {
    let f = Fixture::new().start();
    f.exec.hold_finish.store(true, Ordering::SeqCst);
    let mut s = spec("true");
    s.hold_ms = Some(3_600_000);
    let id = f.dispatch(s);
    wait_until("its cleanup is under way", || {
        f.exec.in_finish.load(Ordering::SeqCst)
    });
    let ws = f.get(&id).workspace.unwrap();
    match f
        .client()
        .call(Request::Release {
            workspace: ws.clone(),
        })
        .unwrap()
    {
        Response::Workspace { workspace } => {
            assert_ne!(workspace.state, WorkspaceState::Removed, "{workspace:#?}")
        }
        other => panic!("{other:?}"),
    }
    f.exec.hold_finish.store(false, Ordering::SeqCst);
    let j = f.wait(&id);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    let w = f.workspace(&ws);
    assert_eq!(w.state, WorkspaceState::Idle, "{w:#?}");
    assert!(w.held_until <= now_ms(), "{w:#?}");
}

/// Retention goes by how long ago this machine recorded each end, and within one boot the boot
/// clock must say so too: a forward step of the wall clock (NTP correcting a clock weeks behind,
/// someone setting the date) doesn't delete a job that ended an hour ago, while one that ended
/// 14 days ago by both clocks goes at the same upkeep.
#[test]
fn a_forward_step_of_the_wall_clock_does_not_delete_recent_jobs() {
    const DAY: u64 = 86_400_000;
    let mut f = Fixture::new();
    f.opts.retention_ms = NodeConfig::default().retention_ms();
    let f = f.start();
    let gone = |id: &str| {
        matches!(
            f.client().call(Request::Get { job: id.into() }),
            Err(e) if e.to_string().contains(&format!("no job {id}"))
        )
    };
    let old = f.dispatch(spec("echo old"));
    assert_eq!(f.wait(&old).state, JobState::Succeeded);
    // Nearly 14 days go by (asleep: the wall and boot clocks move on), and an upkeep runs.
    f.clock.sleep(14 * DAY - 3_600_000);
    std::thread::sleep(Duration::from_millis(500));
    assert!(!gone(&old));
    let recent = f.dispatch(spec("echo recent"));
    assert_eq!(f.wait(&recent).state, JobState::Succeeded);
    // The wall clock is set 15 days forward; two hours later the next upkeep runs.
    f.clock.step_wall(15 * DAY as i64);
    f.clock.pass(2 * 3_600_000);
    wait_until("the old job is deleted", || {
        gone(&old) && !f.paths.job_dir(&old).exists()
    });
    let j = f.get(&recent);
    assert_eq!(j.state, JobState::Succeeded);
    assert_eq!(f.log(&recent), "recent\n");
}

/// Sets a finished job's end in the store to `ended_at`, as if it ended then, in an earlier boot
/// (the daemon stopped): the record's end and when this machine recorded it, without a reading
/// of this boot's clock.
fn backdate_end(paths: &Paths, job: &str, ended_at: Millis) {
    let db = rusqlite::Connection::open(paths.db()).unwrap();
    let json: String = db
        .query_row("SELECT json FROM jobs WHERE id = ?1", [job], |r| r.get(0))
        .unwrap();
    let mut rec: serde_json::Value = serde_json::from_str(&json).unwrap();
    rec["ended_at"] = ended_at.into();
    let changed = db
        .execute(
            "UPDATE jobs SET ended_at = ?2, json = ?3 WHERE id = ?1 AND done = 1",
            rusqlite::params![job, ended_at as i64, rec.to_string()],
        )
        .unwrap();
    assert_eq!(changed, 1, "{job} is not finished in the store");
    db.execute("DELETE FROM kv WHERE key = ?1", [format!("ended/{job}")])
        .unwrap();
}

/// A finished job is kept for the retention period, 14 days unless node.toml says otherwise, then
/// deleted with its directory (output log, kept files, crash reports). One that ended a little
/// under 14 days ago stays, and so does a running one. The clock is injected through the store:
/// the jobs' ends are moved back (into an earlier boot) while the daemon is stopped, and it prunes
/// at its first upkeep after it starts again.
#[test]
fn finished_jobs_are_deleted_after_14_days() {
    let mut f = Fixture::new();
    f.opts.retention_ms = NodeConfig::default().retention_ms();
    assert_eq!(f.opts.retention_ms, 14 * 86_400_000);
    let mut f = f.start();
    let old = f.dispatch(spec("echo old"));
    let recent = f.dispatch(spec("echo recent"));
    for id in [&old, &recent] {
        assert_eq!(f.wait(id).state, JobState::Succeeded);
        // What an owner keeps for a job besides its log.
        let dir = f.paths.job_dir(id);
        std::fs::create_dir_all(dir.join("keep/out")).unwrap();
        std::fs::write(dir.join("keep/out/result.txt"), "kept").unwrap();
        std::fs::create_dir_all(dir.join("crash")).unwrap();
        std::fs::write(dir.join("crash/report.txt"), "crashed").unwrap();
    }
    let running = f.dispatch(spec("sleep 30"));
    f.wait_for(&running, "running", |j| j.state == JobState::Running);
    f.stop();
    const DAY: u64 = 86_400_000;
    let now = now_ms();
    backdate_end(&f.paths, &old, now - 14 * DAY - 60_000);
    backdate_end(&f.paths, &recent, now - 14 * DAY + 3_600_000);
    f.restart();

    let gone = |id: &str| {
        matches!(
            f.client().call(Request::Get { job: id.into() }),
            Err(e) if e.to_string().contains(&format!("no job {id}"))
        )
    };
    wait_until("the old job is deleted", || {
        gone(&old) && !f.paths.job_dir(&old).exists()
    });
    let j = f.get(&recent);
    assert_eq!(j.state, JobState::Succeeded);
    let dir = f.paths.job_dir(&recent);
    assert_eq!(f.log(&recent), "recent\n");
    assert!(dir.join("keep/out/result.txt").is_file() && dir.join("crash/report.txt").is_file());
    assert_eq!(f.get(&running).state, JobState::Running);
    let log = std::fs::read_to_string(f.paths.daemon_log()).unwrap();
    assert!(!log.contains("could not remove"), "{log}");
}

/// A restart in place drains first: a job dispatched while a seeding is going on is queued, not
/// started, and runs once the next daemon has taken over; the seeding the drain waited for ends
/// and its job runs too. `potluck restart` (a Restart request) gets its answer from the next
/// daemon, on the same connection.
#[test]
fn a_job_queued_during_the_drain_runs_after_a_restart_in_place() {
    let mut f = Fixture::new().start();
    f.ws.hold_create.store(true, Ordering::SeqCst);
    let first = f.dispatch(spec("echo first"));
    f.wait_for(&first, "starting", |j| j.state == JobState::Starting);
    let daemon = f.daemon.take().unwrap();
    let handing = std::thread::spawn(move || {
        let why = daemon.wait_restart();
        daemon.restart_verdict(Ok(()));
        (why, daemon.hand_over(Duration::from_secs(30)))
    });
    let paths = f.paths.clone();
    let asking = std::thread::spawn(move || {
        Client::connect(&paths, ClientContext::default())
            .unwrap()
            .call(Request::Restart { binary: None })
    });
    // Draining: the seeding goes on, and nothing new starts.
    std::thread::sleep(Duration::from_millis(300));
    let second = f.dispatch(spec("echo second"));
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(f.get(&second).state, JobState::Queued);
    assert_eq!(f.get(&first).state, JobState::Starting);
    f.ws.hold_create.store(false, Ordering::SeqCst);
    let (why, inherited) = handing.join().unwrap();
    assert_eq!(why.as_deref(), Some("asked by `potluck restart`"));
    let parts = Parts {
        executor: f.exec.clone(),
        workspaces: f.ws.clone(),
        scheduler: Arc::new(RealScheduler),
        facts: Box::new(|| Ok(facts())),
        login_env: f.login_env.clone(),
        clock: f.clock.clone(),
    };
    f.daemon = Some(
        Daemon::start_from(&f.paths, &f.config, parts, f.opts.clone(), None, inherited).unwrap(),
    );
    match asking.join().unwrap() {
        Ok(Response::Text { text }) => assert!(text.starts_with("Restarted in place"), "{text}"),
        other => panic!("{other:?}"),
    }
    for (job, said) in [(&first, "first"), (&second, "second")] {
        let j = f.wait(job);
        assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
        assert_eq!(f.log(job), format!("{said}\n"));
    }
    let log = std::fs::read_to_string(f.paths.home.join("daemon.log")).unwrap();
    assert!(log.contains("restarted in place from potluck"), "{log}");
}
