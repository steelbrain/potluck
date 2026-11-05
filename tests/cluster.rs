//! Several daemons in one process, each with its own temporary home, in one cluster: the mesh on
//! 127.0.0.1 with free ports and short heartbeat and down times. Fakes stand in for the executor
//! (each invocation is `potluck _supervise` in a process group of its own) and the workspaces
//! (plain directories; mirrors are sets of commits, fed by syncs from one shared source). The CLI
//! is the real binary, run in a real git repository.

mod common;

use potluck::client::Client;
use potluck::config::{NodeConfig, Paths};
use potluck::daemon::{
    Clock, ClusterOptions, Daemon, Options, Parts, RealScheduler, Tunnel, Updater,
    WorkspaceProvider,
};
use potluck::exec::{Executor, Launch, Sample};
use potluck::mesh::{Handler, MeshConfig, Peer, Stream};
use potluck::proto::{ClientContext, Request, Response};
use potluck::types::{
    Facts, Gpu, Handle, JobRecord, JobState, MAX_HOLD_MS, MIN_HOLD_MS, Millis, Reminder, RepoRef,
    WorkspaceId, WorkspaceRecord, WorkspaceState, now_ms,
};
use potluck::workspace::Action;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_potluck");
const BRANCH: &str = "agent-1";
const GPU: &str = "10de:2c19@0000:01:00.0";
const WAIT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------------------------
// Fakes

/// The machine's clocks, with steps a test can take: the wall clock set forward or back, or time
/// passing while the machine sleeps (the wall and boot clocks move on, the awake clock stays).
#[derive(Default)]
struct SteppedClock {
    wall_step: std::sync::atomic::AtomicI64,
    boot_step: AtomicU64,
}

impl SteppedClock {
    fn step_wall(&self, ms: i64) {
        self.wall_step.fetch_add(ms, Ordering::SeqCst);
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
        potluck::daemon::SystemClock.boot_ms() + self.boot_step.load(Ordering::SeqCst)
    }
    fn awake_ms(&self) -> u64 {
        potluck::daemon::SystemClock.awake_ms()
    }
    fn boot_id(&self) -> String {
        potluck::daemon::SystemClock.boot_id()
    }
}

#[derive(Clone, Debug)]
struct Launched {
    job: String,
    env: BTreeMap<String, String>,
    pid: u32,
}

/// Runs each invocation as `potluck _supervise` in a process group of its own and reaps it.
#[derive(Default)]
struct FakeExec {
    procs: Mutex<BTreeMap<u32, Arc<AtomicBool>>>,
    launched: Mutex<Vec<Launched>>,
    cpu: AtomicU64,
    /// Each `finish` leaves a crash report `report-<n>.ips` in the crash dir (n counts the calls),
    /// as ReportCrash does, sometimes seconds after the crash.
    crash_reports: AtomicBool,
    finishes: AtomicU64,
    /// Launch name -> the handle `start` gave, for `find`.
    names: Mutex<BTreeMap<String, Handle>>,
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

    fn launched(&self) -> Vec<Launched> {
        self.launched.lock().unwrap().clone()
    }

    fn launch_of(&self, job: &str) -> Option<Launched> {
        self.launched().into_iter().find(|l| l.job == job)
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
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&l.log)?;
        let mut child = Command::new(&l.potluck_bin)
            .arg("_supervise")
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
            pid,
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

    fn sample(&self, h: &mut Handle, _mem_max: u64) -> anyhow::Result<Sample> {
        let pid = pid_of(h);
        let pids = if self.alive(pid) { vec![pid] } else { vec![] };
        Ok(Sample {
            pids,
            cpu_ms: self.cpu.fetch_add(10, Ordering::SeqCst) + 10,
            mem_bytes: 1 << 20,
            peak_mem_bytes: 0,
            over_memory: false,
            oom_killed: false,
            over_tasks: None,
        })
    }

    fn signal(&self, h: &Handle, sig: i32) -> anyhow::Result<()> {
        self.kill_group(pid_of(h), sig);
        Ok(())
    }

    fn stacks(&self, h: &Handle) -> anyhow::Result<String> {
        Ok(format!("fake stacks of {}", pid_of(h)))
    }

    fn find(&self, name: &str, _status: &Path, _since: Millis) -> anyhow::Result<Option<Handle>> {
        Ok(self.names.lock().unwrap().get(name).cloned())
    }

    fn finish(&self, _h: &Handle, crash_dir: &Path, _since: Millis) -> anyhow::Result<()> {
        if self.crash_reports.load(Ordering::SeqCst) {
            let n = self.finishes.fetch_add(1, Ordering::SeqCst) + 1;
            std::fs::create_dir_all(crash_dir)?;
            std::fs::write(crash_dir.join(format!("report-{n}.ips")), "a crash\n")?;
        }
        Ok(())
    }
}

/// Whether the source's repository `source_id` (the CLI's unsalted id) is the one a mirror of
/// `repo_id` holds: the same id, or that id salted with an owner (the same name; the test
/// repositories all have different names).
fn same_repo(source_id: &str, repo_id: &str) -> bool {
    let name = |id: &str| id.rsplit_once('-').map_or(id, |(n, _)| n).to_string();
    source_id == repo_id || name(source_id) == name(repo_id)
}

/// Workspaces as plain directories holding their commit in a file. A mirror is the set of commits
/// this machine has; a sync copies in what the shared source has.
struct FakeWs {
    paths: Paths,
    source: Arc<Mutex<BTreeSet<(String, String)>>>,
    mirror: Mutex<BTreeSet<(String, String)>>,
    pins: Mutex<BTreeSet<(String, String)>>,
    synced: Mutex<Vec<String>>,
    removed: Mutex<Vec<String>>,
    sync_fail: AtomicBool,
    /// What a sync reports loudly (a mirror that had diverged, say).
    sync_warning: Mutex<Option<String>>,
    sync_delay_ms: AtomicU64,
    /// Syncs wait while this is set (up to [`WAIT`]: a test that fails meanwhile still stops).
    sync_hold: AtomicBool,
    /// Seeding waits while this is set; it fails once `create_fail` is set.
    create_hold: AtomicBool,
    create_fail: AtomicBool,
    /// Syncs running now, and the most that ever ran at once.
    syncing: AtomicU64,
    max_syncing: AtomicU64,
}

impl FakeWs {
    fn sync_mirror_now(&self, repo_id: &str, url: &str) -> anyhow::Result<Option<String>> {
        std::thread::sleep(Duration::from_millis(
            self.sync_delay_ms.load(Ordering::SeqCst),
        ));
        let held = Instant::now();
        while self.sync_hold.load(Ordering::SeqCst) && held.elapsed() < WAIT {
            std::thread::sleep(Duration::from_millis(10));
        }
        if self.sync_fail.load(Ordering::SeqCst) {
            anyhow::bail!("the tunnel is down (test)");
        }
        let have: Vec<(String, String)> = self
            .source
            .lock()
            .unwrap()
            .iter()
            .filter(|(r, _)| same_repo(r, repo_id))
            .map(|(_, c)| (repo_id.to_string(), c.clone()))
            .collect();
        self.mirror.lock().unwrap().extend(have);
        self.synced.lock().unwrap().push(url.to_string());
        Ok(self.sync_warning.lock().unwrap().clone())
    }
}

impl WorkspaceProvider for FakeWs {
    fn sync_mirror(&self, repo_id: &str, url: &str) -> anyhow::Result<Option<String>> {
        let n = self.syncing.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_syncing.fetch_max(n, Ordering::SeqCst);
        let r = self.sync_mirror_now(repo_id, url);
        self.syncing.fetch_sub(1, Ordering::SeqCst);
        r
    }
    fn has_commit(&self, repo_id: &str, commit: &str) -> bool {
        self.mirror
            .lock()
            .unwrap()
            .contains(&(repo_id.to_string(), commit.to_string()))
    }
    fn fetch_pin(&self, repo_id: &str, _url: &str, commit: &str, name: &str) -> anyhow::Result<()> {
        let rc = (repo_id.to_string(), commit.to_string());
        anyhow::ensure!(
            self.source
                .lock()
                .unwrap()
                .iter()
                .any(|(r, c)| same_repo(r, repo_id) && c == commit),
            "no {commit}"
        );
        self.mirror.lock().unwrap().insert(rc);
        self.pin(repo_id, commit, name)
    }
    fn pin(&self, repo_id: &str, commit: &str, name: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.has_commit(repo_id, commit),
            "the mirror lacks {commit}"
        );
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
    fn create(&self, id: &WorkspaceId, repo: &RepoRef) -> anyhow::Result<PathBuf> {
        while self.create_hold.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        anyhow::ensure!(
            !self.create_fail.load(Ordering::SeqCst),
            "seeding failed (test)"
        );
        anyhow::ensure!(self.has_commit(&repo.id, &repo.commit), "no commit to seed");
        let root = self.paths.workspace(id);
        std::fs::create_dir_all(root.join("data"))?;
        std::fs::write(root.join("COMMIT"), &repo.commit)?;
        std::fs::write(root.join("data/a.txt"), b"alpha")?;
        std::fs::create_dir_all(self.paths.workspace_tmp(id))?;
        self.pin(&repo.id, &repo.commit, id)?;
        Ok(root)
    }
    fn verify(&self, root: &Path, repo: &RepoRef) -> anyhow::Result<()> {
        let at = std::fs::read_to_string(root.join("COMMIT"))?;
        anyhow::ensure!(at == repo.commit, "HEAD is {at}, not {}", repo.commit);
        Ok(())
    }
    fn advance(
        &self,
        id: &WorkspaceId,
        root: &Path,
        from: &str,
        repo: &RepoRef,
    ) -> anyhow::Result<()> {
        let at = std::fs::read_to_string(root.join("COMMIT"))?;
        anyhow::ensure!(at == from, "HEAD is {at}, not {from}");
        anyhow::ensure!(
            self.has_commit(&repo.id, &repo.commit),
            "no commit to check out"
        );
        std::fs::write(root.join("COMMIT"), &repo.commit)?;
        self.pin(&repo.id, &repo.commit, id)
    }
    fn remove(&self, record: &WorkspaceRecord) -> anyhow::Result<()> {
        let _ = std::fs::remove_dir_all(&record.path);
        self.pins.lock().unwrap().retain(|(_, n)| n != &record.id);
        self.removed.lock().unwrap().push(record.id.clone());
        Ok(())
    }
    fn size(&self, _root: &Path) -> anyhow::Result<u64> {
        Ok(4096)
    }
    fn tick(&self, now: Millis, records: &[WorkspaceRecord], _budget: u64) -> Vec<Action> {
        records
            .iter()
            .filter(|w| w.state == WorkspaceState::Held && w.held_until <= now)
            .map(|w| Action::MakeIdle(w.id.clone()))
            .collect()
    }
    fn hold_until(&self, ended_at: Millis, requested_ms: Option<u64>, current: Millis) -> Millis {
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

/// Syncs name their source by URL; serving git isn't needed with fake mirrors. A member that asks
/// an owner for a repository it may fetch gets `served`.
struct FakeTunnel;

impl Tunnel for FakeTunnel {
    fn fetch_url(&self, _potluck_bin: &Path, owner: &str, repo_id: &str) -> String {
        format!("fake://{owner}/{repo_id}")
    }
    fn serve_upload_pack(&self, _git_dir: &Path, mut stream: Stream) -> anyhow::Result<()> {
        use std::io::Write;
        stream.write_all(b"served\n")?;
        stream.close_write()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Fixture

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args([
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.invalid",
            "-c",
            "commit.gpgsign=false",
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

/// What the nodes of one test share: the repository jobs are dispatched from, and its source
/// (the commits a sync can bring).
struct World {
    dir: tempfile::TempDir,
    repo: PathBuf,
    source: Arc<Mutex<BTreeSet<(String, String)>>>,
}

impl World {
    fn new() -> World {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("proj");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", BRANCH]);
        std::fs::write(repo.join("README"), "hello\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-qm", "init"]);
        let id = potluck::types::repo_id(&repo.join(".git"));
        let commit = git(&repo, &["rev-parse", "HEAD"]);
        World {
            source: Arc::new(Mutex::new([(id, commit)].into())),
            dir,
            repo,
        }
    }

    /// Another repository beside the first, with one commit the source has.
    fn another_repo(&self, name: &str) -> PathBuf {
        let repo = self.dir.path().join(name);
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", BRANCH]);
        std::fs::write(repo.join("README"), format!("{name}\n")).unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-qm", "init"]);
        let id = potluck::types::repo_id(&repo.join(".git"));
        let commit = git(&repo, &["rev-parse", "HEAD"]);
        self.source.lock().unwrap().insert((id, commit));
        repo
    }

    fn node(&self, name: &str) -> NodeBuilder<'_> {
        NodeBuilder {
            world: self,
            name: name.into(),
            gpu: false,
            cpu_model: "slow".into(),
            version: TEST_VERSION.into(),
            locality_ms: 0,
            updating_ms: 600_000,
            down_after_ms: 1_500,
            claim_grace_ms: 2_000,
            keep_give_up_ms: 600_000,
            updater: None,
            clock: Arc::default(),
        }
    }
}

struct NodeBuilder<'w> {
    world: &'w World,
    name: String,
    gpu: bool,
    cpu_model: String,
    version: String,
    locality_ms: u64,
    updating_ms: u64,
    down_after_ms: u64,
    claim_grace_ms: u64,
    keep_give_up_ms: u64,
    updater: Option<Updater>,
    clock: Arc<SteppedClock>,
}

impl NodeBuilder<'_> {
    /// The daemon's clocks (the cluster's own reads the machine's).
    fn clock(mut self, clock: Arc<SteppedClock>) -> Self {
        self.clock = clock;
        self
    }
    /// How long kept files that can't be delivered are tried before they are given up on.
    fn keep_give_up(mut self, ms: u64) -> Self {
        self.keep_give_up_ms = ms;
        self
    }
    fn gpu(mut self) -> Self {
        self.gpu = true;
        self
    }
    fn cpu(mut self, model: &str) -> Self {
        self.cpu_model = model.into();
        self
    }
    fn locality(mut self, ms: u64) -> Self {
        self.locality_ms = ms;
        self
    }
    /// How long a silent member takes to count as down here.
    fn down_after(mut self, ms: u64) -> Self {
        self.down_after_ms = ms;
        self
    }
    /// How long after granting a claim this machine waits before it queues the job again when
    /// the runner doesn't list it.
    fn claim_grace(mut self, ms: u64) -> Self {
        self.claim_grace_ms = ms;
        self
    }
    fn updater(mut self, u: Updater) -> Self {
        self.updater = Some(u);
        self
    }
    fn version(mut self, v: &str) -> Self {
        self.version = v.into();
        self
    }
    /// How long a member on another version counts as alive (updating).
    fn updating(mut self, ms: u64) -> Self {
        self.updating_ms = ms;
        self
    }

    fn start(self) -> Node {
        let dir = self.world.dir.path().join(&self.name);
        std::fs::create_dir_all(&dir).unwrap();
        let paths = Paths {
            home: dir.join("home"),
            config: dir.join("node.toml"),
        };
        let toml = format!(
            "name = \"{}\"\nlisten = \"127.0.0.1\"\naddress = \"127.0.0.1\"\nram_gib = 8\n",
            self.name
        );
        std::fs::write(&paths.config, &toml).unwrap();
        let config: NodeConfig = toml::from_str(&toml).unwrap();
        let facts = Facts {
            node: self.name.clone(),
            os: std::env::consts::OS.into(),
            os_version: "1".into(),
            arch: std::env::consts::ARCH.into(),
            kernel: "k".into(),
            cpu_model: self.cpu_model.clone(),
            cpus: 8,
            ram_mib: 16 * 1024,
            gpus: if self.gpu {
                vec![Gpu {
                    id: GPU.into(),
                    vendor: "10de".into(),
                    device: "2c19".into(),
                    name: "Fake GPU".into(),
                    ..Default::default()
                }]
            } else {
                vec![]
            },
            display: false,
            version: self.version.clone(),
        };
        let updates: Arc<Mutex<Vec<Option<String>>>> = Arc::default();
        let updater = self.updater.unwrap_or_else(|| {
            let updates = updates.clone();
            Arc::new(move |v: Option<&str>| {
                updates.lock().unwrap().push(v.map(str::to_string));
                Ok("updated (test)".to_string().into())
            })
        });
        let mut node = Node {
            name: self.name,
            dir: dir.clone(),
            paths: paths.clone(),
            repo: self.world.repo.clone(),
            config,
            facts,
            exec: Arc::new(FakeExec::default()),
            ws: Arc::new(FakeWs {
                paths,
                source: self.world.source.clone(),
                mirror: Mutex::default(),
                pins: Mutex::default(),
                synced: Mutex::default(),
                removed: Mutex::default(),
                sync_fail: AtomicBool::new(false),
                sync_warning: Mutex::default(),
                sync_delay_ms: AtomicU64::new(0),
                sync_hold: AtomicBool::new(false),
                create_hold: AtomicBool::new(false),
                create_fail: AtomicBool::new(false),
                syncing: AtomicU64::new(0),
                max_syncing: AtomicU64::new(0),
            }),
            version: self.version,
            locality_ms: self.locality_ms,
            updating_ms: self.updating_ms,
            down_after_ms: self.down_after_ms,
            claim_grace_ms: self.claim_grace_ms,
            keep_give_up_ms: self.keep_give_up_ms,
            port: common::free_port(),
            updates,
            updater,
            clock: self.clock,
            daemon: None,
        };
        node.start();
        node
    }
}

struct Node {
    name: String,
    dir: PathBuf,
    paths: Paths,
    repo: PathBuf,
    config: NodeConfig,
    facts: Facts,
    exec: Arc<FakeExec>,
    ws: Arc<FakeWs>,
    version: String,
    locality_ms: u64,
    updating_ms: u64,
    down_after_ms: u64,
    claim_grace_ms: u64,
    keep_give_up_ms: u64,
    /// The mesh's port, kept across restarts (members dial it once in a cluster).
    port: u16,
    /// What the default updater was asked for.
    updates: Arc<Mutex<Vec<Option<String>>>>,
    updater: Updater,
    clock: Arc<SteppedClock>,
    daemon: Option<Daemon>,
}

/// What a CLI run printed (`--json`).
struct Out {
    code: i32,
    json: Value,
    stdout: String,
    stderr: String,
}

impl Node {
    fn start(&mut self) {
        assert!(self.daemon.is_none());
        let facts = self.facts.clone();
        let parts = Parts {
            executor: self.exec.clone(),
            workspaces: self.ws.clone(),
            scheduler: Arc::new(RealScheduler),
            facts: Box::new(move || Ok(facts.clone())),
            login_env: [
                ("PATH", "/usr/bin:/bin:/usr/sbin:/sbin"),
                ("HOME", self.dir.to_str().unwrap()),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            clock: self.clock.clone(),
        };
        let opts = Options {
            potluck_bin: PathBuf::from(BIN),
            tick_ms: 100,
            kill_grace_ms: 600,
            stall_ms: 0,
            retention_ms: 14 * 86_400_000,
            facts_refresh_ms: 60_000,
            idle_ms: 600_000,
        };
        let cluster = ClusterOptions {
            mesh: self.mesh_config(),
            tick_ms: 100,
            pull_ms: 200,
            locality_wait_ms: self.locality_ms,
            reconcile_ms: 1_000,
            claim_grace_ms: self.claim_grace_ms,
            usage_report_ms: 300,
            linger_ms: 1_000,
            keep_give_up_ms: self.keep_give_up_ms,
            run_limit_grace_ms: 600_000,
            tunnel: Arc::new(FakeTunnel),
            updater: self.updater.clone(),
        };
        self.daemon =
            Some(Daemon::start_cluster(&self.paths, &self.config, parts, opts, cluster).unwrap());
    }

    /// The mesh the daemon runs: this machine's key and port, the test's short times.
    fn mesh_config(&self) -> MeshConfig {
        MeshConfig {
            name: self.name.clone(),
            keys_dir: self.paths.keys_dir(),
            listen: format!("127.0.0.1:{}", self.port).parse().unwrap(),
            address: Some("127.0.0.1".into()),
            version: self.version.clone(),
            heartbeat: Duration::from_millis(100),
            down_after: Duration::from_millis(self.down_after_ms),
            updating_for: Duration::from_millis(self.updating_ms),
            connect_timeout: Duration::from_secs(2),
            call_timeout: Duration::from_secs(10),
        }
    }

    /// Stops the daemon and runs a bare mesh in its place, with this machine's key and port: a
    /// member that stays up and says to the others' calls what `handler` makes it say.
    fn impostor(&mut self, handler: Arc<dyn Handler>) -> potluck::mesh::Mesh {
        self.stop();
        potluck::mesh::Mesh::start(self.mesh_config(), handler).unwrap()
    }

    /// Stops the daemon and its mesh. Invocations keep running.
    fn stop(&mut self) {
        self.daemon.take().unwrap().stop();
    }

    fn daemon_log(&self) -> String {
        std::fs::read_to_string(self.paths.daemon_log()).unwrap_or_default()
    }

    fn cli(&self, args: &[&str]) -> Out {
        self.cli_in(&self.repo, args)
    }

    /// The CLI run in repository `dir`.
    fn cli_in(&self, dir: &Path, args: &[&str]) -> Out {
        let out = Command::new(BIN)
            .arg("--json")
            .args(args)
            .current_dir(dir)
            .env("POTLUCK_HOME", &self.paths.home)
            .env("POTLUCK_CONFIG", &self.paths.config)
            .env_remove("POTLUCK_JOB")
            .env_remove("POTLUCK_LEASE")
            .env_remove("POTLUCK_WORKSPACE")
            // Each node's user key is user.key beside its node.toml.
            .env_remove("POTLUCK_USER_KEY")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let json = stdout
            .lines()
            .rev()
            .find_map(|l| serde_json::from_str(l).ok())
            .unwrap_or(Value::Null);
        Out {
            code: out.status.code().unwrap_or(-1),
            json,
            stdout,
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        }
    }

    /// The CLI (without `--json`) started in the repository, its stdout and stderr piped.
    fn spawn_cli(&self, args: &[&str]) -> std::process::Child {
        Command::new(BIN)
            .args(args)
            .current_dir(&self.repo)
            .env("POTLUCK_HOME", &self.paths.home)
            .env("POTLUCK_CONFIG", &self.paths.config)
            .env_remove("POTLUCK_JOB")
            .env_remove("POTLUCK_LEASE")
            .env_remove("POTLUCK_WORKSPACE")
            // Each node's user key is user.key beside its node.toml.
            .env_remove("POTLUCK_USER_KEY")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn cli_ok(&self, args: &[&str]) -> Value {
        let o = self.cli(args);
        assert_eq!(
            o.code, 0,
            "{}: potluck {args:?}: {}\n{}",
            self.name, o.stdout, o.stderr
        );
        o.json
    }

    /// Dispatches from the repository with a 60s run limit; returns the job id.
    fn dispatch(&self, args: &[&str]) -> String {
        let mut all = vec!["dispatch", "--timeout", "60s"];
        all.extend_from_slice(args);
        let v = self.cli_ok(&all);
        v["job"].as_str().unwrap().to_string()
    }

    fn client(&self) -> Client {
        Client::connect(
            &self.paths,
            ClientContext {
                repo_id: None,
                branch: None,
                job: None,
                lease_token: None,
            },
        )
        .unwrap()
    }

    fn get(&self, job: &str) -> JobRecord {
        match self.client().call(Request::Get { job: job.into() }) {
            Ok(Response::Job { job }) => *job,
            other => panic!("{}: get {job}: {other:?}", self.name),
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
                start.elapsed() < WAIT,
                "{}: {job} never {what}: {j:#?}\n{}",
                self.name,
                self.daemon_log()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn running_on(&self, job: &str, node: &str) -> JobRecord {
        self.wait_for(job, &format!("ran on {node}"), |j| {
            j.state == JobState::Running && j.node.as_deref() == Some(node)
        })
    }

    fn done(&self, job: &str) -> JobRecord {
        self.wait_for(job, "ended", |j| j.state.is_done())
    }

    /// Where the mesh listens, if it does.
    fn listening(&self) -> Option<std::net::SocketAddr> {
        self.daemon.as_ref().unwrap().cluster_addr()
    }

    /// A value the daemon keeps in its store's `kv` table, read as it stands now.
    fn kv(&self, key: &str) -> Option<String> {
        let db = rusqlite::Connection::open(self.paths.db()).unwrap();
        db.query_row("SELECT value FROM kv WHERE key = ?1", [key], |r| r.get(0))
            .ok()
    }

    fn node_alive(&self, other: &str) -> bool {
        match self.client().call(Request::Nodes) {
            Ok(Response::Nodes { nodes }) => nodes
                .iter()
                .any(|n| n.facts.node == other && n.alive && !n.facts.os.is_empty()),
            _ => false,
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(d) = self.daemon.take() {
            d.stop();
        }
        self.exec.kill_all();
    }
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < WAIT, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The version test nodes run unless a test gives another: not this build's, so a test that
/// compares versions doesn't change meaning when Cargo.toml's does.
const TEST_VERSION: &str = "0.1.0";

/// A mesh that only connects, as `node`'s machine, on the test nodes' version: members talk only on
/// the same version.
fn client_mesh(node: &Node) -> potluck::mesh::Mesh {
    let mut cfg = MeshConfig::new(&node.paths, &node.config);
    cfg.version = TEST_VERSION.to_string();
    potluck::mesh::Mesh::connect_only(cfg).unwrap()
}

/// What an impostor ([`Node::impostor`]) answers: for each op a test gave, that answer to every
/// call with it, an error to any other call; and how many calls of each op came.
#[derive(Default)]
struct Scripted {
    answers: Mutex<BTreeMap<String, Value>>,
    calls: Mutex<BTreeMap<String, u64>>,
}

impl Scripted {
    fn answer(&self, op: &str, v: Value) {
        self.answers.lock().unwrap().insert(op.into(), v);
    }

    fn calls(&self, op: &str) -> u64 {
        self.calls.lock().unwrap().get(op).copied().unwrap_or(0)
    }
}

impl Handler for Scripted {
    fn call(&self, _from: &Peer, msg: Value) -> Result<Value, String> {
        let op = msg["op"].as_str().unwrap_or_default().to_string();
        *self.calls.lock().unwrap().entry(op.clone()).or_default() += 1;
        let answer = self.answers.lock().unwrap().get(&op).cloned();
        answer.ok_or_else(|| format!("no {op} here"))
    }
}

/// Pairs `new` into `member`'s cluster through the CLI: `pair`, `admit`, `pair --finish`.
fn pair(member: &Node, new: &Node) {
    let request = new.cli_ok(&["pair"])["text"].as_str().unwrap().to_string();
    let reply = member.cli_ok(&["admit", &request])["text"]
        .as_str()
        .unwrap()
        .to_string();
    let joined = new.cli_ok(&["pair", "--finish", &reply]);
    assert!(
        joined["text"].as_str().unwrap().contains(&member.name),
        "{joined}"
    );
    wait_until(&format!("{} sees {}", member.name, new.name), || {
        member.node_alive(&new.name) && new.node_alive(&member.name)
    });
}

fn output_of(node: &Node, job: &str) -> String {
    std::fs::read_to_string(node.paths.job_dir(job).join("output.log")).unwrap_or_default()
}

// ---------------------------------------------------------------------------------------------
// Tests

/// A node's port, free when picked, is bound only once it is in a cluster (and again after each
/// restart). Meanwhile it is claimed for this test process: other test processes, picking from the
/// same range, skip a port whose UDP twin they can't bind. The daemon still listens on it.
#[test]
fn a_nodes_port_is_claimed_for_this_process_until_it_listens() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    assert!(
        std::net::UdpSocket::bind(("127.0.0.1", a.port)).is_err(),
        "another process could pick {}",
        a.port
    );
    pair(&a, &b);
    assert_eq!(
        a.listening(),
        Some(format!("127.0.0.1:{}", a.port).parse().unwrap())
    );
}

/// Three machines joined through the chain of trust (b admitted by a, c by b); a job dispatched on
/// a runs on b, the only machine with a GPU; a streams its log and waits for it, gets its exit
/// code and kept files; c reads the same log through a.
#[test]
fn three_machines_one_job_runs_elsewhere_and_reports_home() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").gpu().start();
    let c = w.node("c").start();
    pair(&a, &b);
    pair(&b, &c);
    // a never admitted c but takes b's word for it.
    wait_until("a sees c", || a.node_alive("c") && c.node_alive("a"));
    let nodes = a.cli_ok(&["nodes"]);
    let names: Vec<&str> = nodes["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["facts"]["node"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["a", "b", "c"]);
    assert!(
        nodes["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n["alive"] == true)
    );

    let job = a.dispatch(&[
        "--on",
        "gpu=nvidia",
        "--gpu",
        "any",
        "--keep",
        "out/*.txt",
        "--",
        "sh",
        "-c",
        "echo start on $POTLUCK_NODE with gpus=$POTLUCK_GPUS; for i in 1 2 3 4 5; do echo tick $i; sleep 0.2; done; mkdir -p out; echo kept-content > out/k.txt; exit 3",
    ]);
    assert!(job.starts_with("a-"), "{job}");
    // While it runs: a's log grows, and a third machine reads it through a.
    let j = a.running_on(&job, "b");
    assert_eq!(j.workspace.as_deref(), Some("b-w1"));
    wait_until("a has some of the log", || {
        output_of(&a, &job).contains("tick 1")
    });
    let mid = c.cli_ok(&["logs", &job]);
    assert!(
        mid["output"].as_str().unwrap().contains("start on b"),
        "{mid}"
    );

    // `wait` on a streams the output and exits with the job's code.
    let out = Command::new(BIN)
        .args(["wait", &job])
        .current_dir(&w.repo)
        .env("POTLUCK_HOME", &a.paths.home)
        .env("POTLUCK_CONFIG", &a.paths.config)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(3), "{text}");
    assert!(
        text.contains(&format!("start on b with gpus={GPU}")) && text.contains("tick 5"),
        "{text}"
    );
    let j = a.get(&job);
    assert_eq!(
        (j.state, j.exit_code, j.node.as_deref()),
        (JobState::Failed, Some(3), Some("b"))
    );
    assert_eq!(j.kept, ["keep/out/k.txt"]);
    assert_eq!(
        std::fs::read_to_string(a.paths.job_dir(&job).join("keep/out/k.txt")).unwrap(),
        "kept-content\n"
    );
    assert_eq!(output_of(&a, &job), output_of(&b, &job));
    // b ran it with its GPU, and synced its mirror from a.
    let l = b.exec.launch_of(&job).unwrap();
    assert_eq!(l.env["POTLUCK_GPUS"], GPU);
    assert_eq!(l.env["POTLUCK_NODE"], "b");
    assert!(
        b.ws.synced
            .lock()
            .unwrap()
            .iter()
            .any(|u| u.starts_with("fake://a/")),
        "{:?}",
        b.ws.synced.lock().unwrap()
    );
    assert!(a.exec.launched().is_empty() && c.exec.launched().is_empty());
    // c asks a about a's job: the same record.
    let cw = c.cli(&["wait", &job]);
    assert_eq!(cw.code, 3, "{}", cw.stderr);
    // The workspace on b is held for a's branch: a reminds it, and a's status shows it.
    wait_until("a reminds b-w1", || {
        let st = a.cli_ok(&["status"]);
        st["reminders"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["workspace"] == "b-w1")
    });
    let st = a.cli_ok(&["status"]);
    assert!(st["running"].as_array().unwrap().is_empty(), "{st}");
    assert!(
        st["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["id"] == "b-w1" && w["node"] == "b" && w["state"] == "held"),
        "{st}"
    );
}

/// A daemon listens for members only while its machine is in a cluster: the founder from its
/// first admission, the new machine from the end of pairing; a removed machine stops at once, a
/// member left alone shortly after.
#[test]
fn listening_only_in_a_cluster() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let c = w.node("c").start();
    let refused = |n: &Node| std::net::TcpStream::connect(("127.0.0.1", n.port)).is_err();
    assert!(
        [&a, &b, &c]
            .iter()
            .all(|n| n.listening().is_none() && refused(n))
    );
    let request = b.cli_ok(&["pair"])["text"].as_str().unwrap().to_string();
    assert!(b.listening().is_none(), "a request needs no listener");
    let reply = a.cli_ok(&["admit", &request])["text"]
        .as_str()
        .unwrap()
        .to_string();
    let at = |n: &Node| Some(format!("127.0.0.1:{}", n.port).parse().unwrap());
    assert_eq!(a.listening(), at(&a), "the founder listens once it admits");
    assert!(b.listening().is_none());
    b.cli_ok(&["pair", "--finish", &reply]);
    assert_eq!(b.listening(), at(&b), "the new member listens once paired");
    wait_until("a and b meet", || a.node_alive("b") && b.node_alive("a"));
    assert!(c.listening().is_none() && refused(&c));
    assert!(a.daemon_log().contains("listening on 127.0.0.1:"));

    // Removed: b stops listening at once; a, alone, after the linger.
    a.cli_ok(&["remove", "b"]);
    wait_until("b stopped listening", || b.listening().is_none());
    wait_until("a stopped listening", || a.listening().is_none());
    assert!(refused(&a) && refused(&b));
    // a settled the removal once: as the command made it, not again for the mesh's event.
    let log = a.daemon_log();
    assert_eq!(log.matches("cluster: removed b\n").count(), 1, "{log}");
    assert!(!log.contains("cluster: a removed b"), "{log}");
    // b pairs again (with a new key): both listen again.
    pair(&a, &b);
    assert_eq!((a.listening(), b.listening()), (at(&a), at(&b)));
}

/// Many jobs that two machines can both run: each runs exactly once.
#[test]
fn racing_machines_run_each_job_once() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").cpu("fast").start();
    let c = w.node("c").cpu("fast").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b sees c", || b.node_alive("c"));
    let jobs: Vec<String> = (0..10)
        .map(|_| {
            a.dispatch(&[
                "--on",
                "cpu_model=fast",
                "--ram",
                "1",
                "--",
                "sh",
                "-c",
                "sleep 0.2",
            ])
        })
        .collect();
    for j in &jobs {
        let r = a.done(j);
        assert_eq!(r.state, JobState::Succeeded, "{r:#?}");
    }
    let mut ran: Vec<String> = b
        .exec
        .launched()
        .into_iter()
        .chain(c.exec.launched())
        .map(|l| l.job)
        .collect();
    ran.sort();
    let mut want = jobs.clone();
    want.sort();
    assert_eq!(ran, want, "every job ran exactly once");
    assert!(a.exec.launched().is_empty());
    // The claims each lost undid their reservations: nothing stays reserved.
    for n in [&b, &c] {
        wait_until(&format!("{} has nothing reserved", n.name), || {
            n.cli_ok(&["nodes"])["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["facts"]["node"] == n.name.as_str())
                .is_some_and(|m| m["ram_reserved_mib"] == 0)
        });
    }
}

/// One GPU on b: queued jobs start there by priority; the passes b's starts make reach their
/// owner.
#[test]
fn priority_and_aging_across_machines() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").gpu().start();
    pair(&a, &b);
    let first = a.dispatch(&["--gpu", "any", "--", "sleep", "60"]);
    a.running_on(&first, "b");
    let low = a.dispatch(&["--gpu", "any", "--priority", "1", "--", "true"]);
    let mid = a.dispatch(&["--gpu", "any", "--priority", "3", "--", "true"]);
    let high = a.dispatch(&["--gpu", "any", "--priority", "5", "--", "true"]);
    // Once b knows all three are queued, the GPU frees up.
    wait_until("b sees the three queued", || {
        b.cli_ok(&["status"])["queued"]
            .as_array()
            .is_some_and(|q| q.len() == 3)
    });
    a.cli_ok(&["cancel", &first]);
    for j in [&low, &mid, &high] {
        assert_eq!(a.done(j).state, JobState::Succeeded);
    }
    let order: Vec<String> = b.exec.launched().into_iter().map(|l| l.job).collect();
    assert_eq!(
        order,
        [first.clone(), high.clone(), mid.clone(), low.clone()],
        "a:\n{}\nb:\n{}",
        a.daemon_log(),
        b.daemon_log()
    );
    // high's start passed over low and mid; mid's passed over low.
    assert_eq!(a.get(&low).passes, 2);
    assert_eq!(a.get(&mid).passes, 1);
    assert_eq!(a.get(&high).passes, 0);
}

/// A runner keeps its ended record of a job it ran before for a while (b ran it, was lost, and
/// stopped its copy when back). When its plan passes over that job, queued again, the pass goes
/// to the owner, not onto that old record.
#[test]
fn a_pass_over_a_job_the_runner_ran_before_reaches_its_owner() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let job = a.dispatch(&["--gpu", "any", "--", "sleep", "60"]);
    a.running_on(&job, "b");
    let mut c = w.node("c").gpu().start();
    pair(&a, &c);
    // b is lost and c takes the job; b, back, stops its copy (its record of it ends).
    let first = b.exec.launch_of(&job).unwrap().pid;
    b.stop();
    a.running_on(&job, "c");
    b.start();
    wait_until("b's record of its copy ended", || {
        !b.exec.alive(first) && ended_in_store(&b.paths, &job)
    });
    wait_until("a sees b again", || a.node_alive("b"));
    // b's GPU is busy; c is lost: the job waits for b, and a later job of higher priority goes
    // first there.
    let busy = a.dispatch(&["--on", "node=b", "--gpu", "any", "--", "sleep", "60"]);
    a.running_on(&busy, "b");
    c.stop();
    a.wait_for(&job, "queued again", |j| j.state == JobState::Queued);
    let high = a.dispatch(&["--gpu", "any", "--priority", "5", "--", "true"]);
    wait_until("b sees the two queued", || {
        b.cli_ok(&["status"])["queued"]
            .as_array()
            .is_some_and(|q| q.len() == 2)
    });
    a.cli_ok(&["cancel", &busy]);
    assert_eq!(a.done(&high).state, JobState::Succeeded);
    a.running_on(&job, "b");
    // high's start passed over the job.
    assert_eq!(a.get(&job).passes, 1, "{}", b.daemon_log());
    a.cli_ok(&["cancel", &job]);
    a.done(&job);
}

/// Whether a daemon's store has job `job` ended.
fn ended_in_store(paths: &Paths, job: &str) -> bool {
    let db = rusqlite::Connection::open(paths.db()).unwrap();
    db.query_row("SELECT done FROM jobs WHERE id = ?1", [job], |r| {
        r.get::<_, i64>(0)
    })
    .is_ok_and(|d| d == 1)
}

/// A workspace on b, used from a: dispatch pinned to it, cp, hold, release; then a cancel of a
/// job running on b; then b removed: pinning to it fails at once, it refuses work and says so.
#[test]
fn workspaces_elsewhere_cancel_and_a_removed_machine() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let first = a.dispatch(&["--on", "node=b", "--", "sh", "-c", "echo made > made.txt"]);
    let j = a.done(&first);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    let ws = j.workspace.clone().unwrap();
    assert_eq!(ws, "b-w1");

    // cp from a, of a workspace on b.
    let dest = w.dir.path().join("copied");
    let v = a.cli_ok(&["cp", &format!("{ws}:made.txt"), dest.to_str().unwrap()]);
    assert_eq!(v["files"], 1, "{v}");
    assert_eq!(std::fs::read_to_string(&dest).unwrap(), "made\n");
    let dir = w.dir.path().join("data-copy");
    a.cli_ok(&["cp", &format!("{ws}:data"), dir.to_str().unwrap()]);
    assert_eq!(std::fs::read_to_string(dir.join("a.txt")).unwrap(), "alpha");

    // hold from a: b's record. The reminder that comes with it says so already.
    let reminded = |v: &Value| -> Option<u64> {
        v["reminders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["workspace"] == ws.as_str())
            .map(|r| r["remaining_ms"].as_u64().unwrap())
    };
    // Asked from another branch: the hold is that branch's (its reminders go there), on b too.
    git(&a.repo, &["checkout", "-q", "-b", "agent-2"]);
    let v = a.cli_ok(&["hold", &ws, "2h"]);
    let until = v["held_until"].as_u64().unwrap();
    assert!(until > now_ms() + 110 * 60_000, "{v}");
    assert_eq!(v["record"]["branch"], "agent-2", "{v}");
    assert!(reminded(&v).unwrap() > 110 * 60_000, "{v}");
    // A pinned dispatch runs in it, on b.
    let pinned = a.dispatch(&["--workspace", &ws, "--", "sh", "-c", "cat made.txt"]);
    let j = a.done(&pinned);
    assert_eq!(
        (j.state, j.node.as_deref(), j.workspace.as_deref()),
        (JobState::Succeeded, Some("b"), Some(ws.as_str()))
    );
    assert_eq!(output_of(&a, &pinned), "made\n");

    // release from a: removed on b, and no reminder of it comes with the answer.
    let v = a.cli_ok(&["release", &ws]);
    assert_eq!(reminded(&v), None, "{v}");
    wait_until("b removed the workspace", || {
        b.ws.removed.lock().unwrap().contains(&ws)
    });
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        &ws,
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2);
    assert_eq!(
        o.json["error"].as_str().unwrap(),
        format!("workspace {ws} is gone from b"),
        "{}",
        o.json
    );
    // One b never had isn't "gone".
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        "b-w99",
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2);
    assert_eq!(
        o.json["error"].as_str().unwrap(),
        "no workspace b-w99 on b",
        "{}",
        o.json
    );

    // cancel from a, of a job running on b.
    let long = a.dispatch(&["--on", "node=b", "--", "sleep", "30"]);
    a.running_on(&long, "b");
    let pid = b.exec.launch_of(&long).unwrap().pid;
    a.cli_ok(&["cancel", &long]);
    let j = a.done(&long);
    assert_eq!((j.state, j.exit_code), (JobState::Cancelled, Some(130)));
    assert!(!b.exec.alive(pid));
    // The logs say the state once.
    wait_until("a logs the cancel", || {
        a.daemon_log()
            .contains(&format!("{long}: cancelled on b\n"))
    });
    assert!(
        b.daemon_log().contains(&format!("{long}: cancelled\n")),
        "{}",
        b.daemon_log()
    );
    assert!(!a.daemon_log().contains("cancelled: cancelled"));
    assert!(!b.daemon_log().contains("cancelled: cancelled"));

    // b is removed: pinning to a workspace there fails at once; b says so and refuses work.
    a.dispatch(&["--on", "node=b", "--", "true"]);
    let ws2 = format!("b-w{}", 2);
    a.cli_ok(&["remove", "b"]);
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        &ws2,
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2);
    assert!(
        o.json["error"]
            .as_str()
            .unwrap()
            .contains("b is not in the cluster"),
        "{}",
        o.json
    );
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--on",
        "node=b",
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2, "a job only b could run is refused: {}", o.json);
    wait_until("b knows it was removed", || {
        b.cli_ok(&["status"])["notes"]
            .as_array()
            .is_some_and(|n| n.iter().any(|x| x.as_str().unwrap().contains("removed")))
    });
    let o = b.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--on",
        "node=a",
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2, "{}", o.json);
    let nodes = b.cli_ok(&["nodes"]);
    assert_eq!(nodes["nodes"].as_array().unwrap().len(), 1, "{nodes}");
}

/// `--advance` into a workspace on another member: the owner asks its own repository whether the
/// commit descends from the one the workspace's machine says it is at, and the workspace's machine
/// checks it out when the job starts. Its record there, and the owner's reminders, follow.
#[test]
fn an_advance_into_a_workspace_elsewhere() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let first = a.dispatch(&["--on", "node=b", "--", "sh", "-c", "echo made > made.txt"]);
    let j = a.done(&first);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    let ws = j.workspace.clone().unwrap();
    let old = git(&w.repo, &["rev-parse", "HEAD"]);

    std::fs::write(w.repo.join("README"), "hello v2\n").unwrap();
    git(&w.repo, &["commit", "-qam", "v2"]);
    let new = git(&w.repo, &["rev-parse", "HEAD"]);
    let id = potluck::types::repo_id(&w.repo.join(".git"));
    w.source.lock().unwrap().insert((id, new.clone()));

    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        &ws,
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2, "{}", o.json);
    let msg = o.json["error"].as_str().unwrap();
    assert!(
        msg.contains(&format!("workspace {ws} is at commit {}", &old[..12]))
            && msg.contains("dispatch with --advance"),
        "{msg}"
    );

    let adv = a.dispatch(&[
        "--workspace",
        &ws,
        "--advance",
        "--",
        "sh",
        "-c",
        "cat made.txt COMMIT",
    ]);
    let j = a.done(&adv);
    assert_eq!(
        (j.state, j.node.as_deref(), j.workspace.as_deref()),
        (JobState::Succeeded, Some("b"), Some(ws.as_str())),
        "{j:#?}\n{}",
        b.daemon_log()
    );
    assert_eq!(output_of(&a, &adv), format!("made\n{new}"));
    let rec = b.cli_ok(&["status"])["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["id"] == ws.as_str())
        .cloned()
        .unwrap();
    assert_eq!(
        (rec["commit"].as_str(), rec["advanced_by"].as_str()),
        (Some(new.as_str()), Some(adv.as_str())),
        "{rec}"
    );
    assert!(
        b.ws.pins
            .lock()
            .unwrap()
            .iter()
            .any(|(r, n)| same_repo(&potluck::types::repo_id(&w.repo.join(".git")), r) && n == &ws)
    );
    wait_until("a's reminder shows the new commit", || {
        a.cli_ok(&["status"])["reminders"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["workspace"] == ws.as_str() && r["commit"] == new.as_str())
    });

    // Back at the old commit: the owner's repository says it doesn't descend.
    git(&w.repo, &["checkout", "-q", &old]);
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        &ws,
        "--advance",
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2, "{}", o.json);
    let msg = o.json["error"].as_str().unwrap();
    assert!(
        msg.contains(&format!(
            "workspace {ws} is at commit {}, and this dispatch's commit {} doesn't descend from it",
            &new[..12],
            &old[..12]
        )),
        "{msg}"
    );
}

/// Whether `node`'s status says it is out of the cluster.
fn says_out(node: &Node) -> bool {
    node.cli_ok(&["status"])["notes"]
        .as_array()
        .is_some_and(|n| n.iter().any(|x| x.as_str().unwrap().contains("removed")))
}

/// b removes c while a, stopped, removes b, not knowing of b's removal of c; c hears of b's
/// first. a's removal voids b's (a hadn't seen it), so once c hears of a's, c is a member again,
/// for its mesh as for every member: its daemon says so and runs work for the others again.
#[test]
fn a_machine_whose_removal_turns_out_void_takes_work_again() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").start();
    let c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    a.stop();
    b.cli_ok(&["remove", "c"]);
    wait_until("c knows b removed it", || says_out(&c));
    {
        use potluck::mesh::{Record, Removal, identity::Identity, trust::Trust};
        let keys = a.paths.keys_dir();
        let id = Identity::load(&keys).unwrap();
        let mut t = Trust::load_or_found(&keys, &id, "a", "127.0.0.1:1").unwrap();
        let key = t.view.by_name("b").unwrap().key;
        let rec = Removal::sign(&id, "a", "b", key, t.signed_by(&key), t.next_clock());
        t.merge(vec![Record::Removal(rec)]);
        assert!(t.view.removed.contains_key(&key));
        t.save().unwrap();
    }
    a.start();
    wait_until("c is in the cluster again", || !says_out(&c));
    let job = a.dispatch(&["--on", "node=c", "--", "true"]);
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("c")),
        "{j:#?}"
    );
}

/// A child process that another thread is starting holds a copy of every descriptor until it
/// execs. One that holds c's listener while c is removed and then back in (its removal turned
/// out void) doesn't keep c from listening again: c's listener stopped listening when c was
/// removed, whoever held a copy. It listened on in the child, and c, back in, couldn't listen
/// ("Address already in use") until the child let go. Linux only: macOS can't stop a listener
/// another process holds a copy of, and a stopping mesh waits there a moment for it instead.
#[cfg(target_os = "linux")]
#[test]
fn a_member_back_in_listens_again_while_a_child_holds_its_old_listener() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").start();
    let c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let held = ChildHolding::listener(c.port);
    a.stop();
    b.cli_ok(&["remove", "c"]);
    wait_until("c knows b removed it", || says_out(&c));
    wait_until("c stopped listening", || c.listening().is_none());
    {
        use potluck::mesh::{Record, Removal, identity::Identity, trust::Trust};
        let keys = a.paths.keys_dir();
        let id = Identity::load(&keys).unwrap();
        let mut t = Trust::load_or_found(&keys, &id, "a", "127.0.0.1:1").unwrap();
        let key = t.view.by_name("b").unwrap().key;
        let rec = Removal::sign(&id, "a", "b", key, t.signed_by(&key), t.next_clock());
        t.merge(vec![Record::Removal(rec)]);
        t.save().unwrap();
    }
    a.start();
    wait_until("c is in the cluster again", || !says_out(&c));
    wait_until("c listens again", || c.listening().is_some());
    held.let_go();
    let log = c.daemon_log();
    assert!(!log.contains("can't listen for members"), "{log}");
}

/// A child process stopped before its exec, holding a copy of one listener of this process (the
/// one on 127.0.0.1:`port`) and no other descriptor of it, until [`ChildHolding::let_go`].
#[cfg(target_os = "linux")]
struct ChildHolding {
    talk: std::os::unix::net::UnixStream,
    starting: std::thread::JoinHandle<std::process::ExitStatus>,
}

#[cfg(target_os = "linux")]
impl ChildHolding {
    fn listener(port: u16) -> ChildHolding {
        use std::io::Read;
        use std::os::fd::AsRawFd;
        let (talk, child_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let starting = std::thread::spawn(move || {
            let me = child_end.as_raw_fd();
            let mut cmd = Command::new("true");
            // SAFETY: only getsockname, getsockopt, close, write and read between fork and exec.
            unsafe {
                cmd.pre_exec(move || {
                    let mut found = false;
                    for fd in 3..4096 {
                        if fd == me {
                            continue;
                        }
                        let mut a: libc::sockaddr_in = std::mem::zeroed();
                        let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
                        let mut listening: libc::c_int = 0;
                        let mut ol = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                        let it = libc::getsockname(fd, (&raw mut a).cast(), &raw mut len) == 0
                            && a.sin_family == libc::AF_INET as libc::sa_family_t
                            && u16::from_be(a.sin_port) == port
                            && libc::getsockopt(
                                fd,
                                libc::SOL_SOCKET,
                                libc::SO_ACCEPTCONN,
                                (&raw mut listening).cast(),
                                &raw mut ol,
                            ) == 0
                            && listening == 1;
                        if it {
                            found = true;
                        } else {
                            libc::close(fd);
                        }
                    }
                    let said = if found { b"y" } else { b"n" };
                    libc::write(me, said.as_ptr().cast(), 1);
                    let mut go = [0u8; 1];
                    libc::read(me, go.as_mut_ptr().cast(), 1);
                    Ok(())
                });
            }
            cmd.status().unwrap()
        });
        let mut said = [0u8; 1];
        (&talk).read_exact(&mut said).unwrap();
        assert_eq!(&said, b"y", "the child found no listener on port {port}");
        ChildHolding { talk, starting }
    }

    fn let_go(self) {
        use std::io::Write;
        (&self.talk).write_all(b"g").unwrap();
        assert!(self.starting.join().unwrap().success());
    }
}

/// The store `node`'s daemon saved, and `who`'s removal of `node2` signed with `who`'s key
/// (keeping none of `node2`'s records), as `potluck remove` there would sign it.
fn removal_by(who: &Node, by: &str, gone: &Node) -> potluck::mesh::Record {
    use potluck::mesh::{Record, Removal, identity::Identity, trust::Trust};
    let id = Identity::load(&who.paths.keys_dir()).unwrap();
    let t = Trust::load_read_only(&who.paths.keys_dir()).unwrap();
    let key = t
        .records()
        .iter()
        .find_map(|r| match r {
            Record::Admission(a) if a.node == gone.name => Some(a.key),
            _ => None,
        })
        .unwrap();
    Record::Removal(Removal::sign(
        &id,
        by,
        &gone.name,
        key,
        Vec::new(),
        t.next_clock(),
    ))
}

/// Whether `node`'s saved store holds a removal of `gone` signed by `by` that counts.
fn holds_removal(node: &Node, gone: &str, by: &str) -> bool {
    use potluck::mesh::Record;
    let t = potluck::mesh::trust::Trust::load_read_only(&node.paths.keys_dir()).unwrap();
    t.records().iter().any(|r| {
        matches!(r, Record::Removal(rm) if rm.node == gone && rm.by == by
            && t.view.valid.contains(&rm.id()))
    })
}

/// The members `node`'s store implies once `rec` is added.
fn members_with(node: &Node, rec: potluck::mesh::Record) -> Vec<String> {
    let mut t = potluck::mesh::trust::Trust::load_read_only(&node.paths.keys_dir()).unwrap();
    t.merge(vec![rec]);
    let mut names: Vec<String> = t.view.members.values().map(|a| a.node.clone()).collect();
    names.sort();
    names
}

/// x was admitted before y, so a removal of x that y signs, x can undo by removing y back
/// (removals that void each other go to the member admitted first). `potluck remove x` on y has
/// the founder sign it: final, and x removing the founder back changes nothing.
#[test]
fn a_removal_asked_of_a_young_member_is_signed_by_the_founder() {
    let w = World::new();
    let a = w.node("a").start();
    let x = w.node("x").start();
    let y = w.node("y").start();
    pair(&a, &x);
    pair(&a, &y);
    wait_until("y sees x", || y.node_alive("x"));
    let out = y.cli_ok(&["remove", "x"]);
    assert_eq!(out["final"], true, "{out}");
    assert!(out["warning"].is_null(), "{out}");
    wait_until("y holds a's removal of x", || holds_removal(&y, "x", "a"));
    assert!(!holds_removal(&y, "x", "y"));
    assert!(
        a.daemon_log()
            .contains("cluster: removal of x signed for y")
    );
    let back = removal_by(&x, "x", &a);
    assert_eq!(members_with(&y, back), ["a", "y"]);
}

/// Audit repro (round 7 trust): the founder is up but can't sign the removal (it can't save its
/// records: a full disk; here its keys directory is read-only). y's `potluck remove x` failed with
/// the founder's error, its path on the founder's disk unexplained, and x stayed a member. Now y
/// signs the removal itself, as with the founder down, and says why the founder didn't.
#[test]
fn a_removal_the_founder_cant_sign_is_signed_here_and_says_why() {
    use std::os::unix::fs::PermissionsExt;
    let w = World::new();
    let a = w.node("a").start();
    let x = w.node("x").start();
    let y = w.node("y").start();
    pair(&a, &x);
    pair(&a, &y);
    wait_until("y sees x", || y.node_alive("x"));
    let keys = a.paths.keys_dir();
    let mode = |m: u32| std::fs::set_permissions(&keys, std::fs::Permissions::from_mode(m));
    mode(0o500).unwrap();
    let out = y.cli(&["remove", "x"]);
    mode(0o700).unwrap();
    assert_eq!(out.code, 0, "{}\n{}", out.stdout, out.stderr);
    assert_eq!(out.json["final"], false, "{}", out.json);
    assert!(holds_removal(&y, "x", "y"));
    let warning = out.json["warning"].as_str().unwrap();
    assert!(
        warning.contains("x can undo this removal")
            && warning.contains("a did not sign it: ")
            && warning.contains("members.json"),
        "{warning}"
    );
    wait_until("a takes y's removal of x", || !a.node_alive("x"));
}

/// With the founder down, y signs the removal of x itself, says that x can undo it and how to
/// make it final; and x can.
#[test]
fn with_the_founder_down_a_removal_says_it_can_be_undone() {
    let w = World::new();
    let mut a = w.node("a").start();
    let x = w.node("x").start();
    let y = w.node("y").start();
    pair(&a, &x);
    pair(&a, &y);
    wait_until("y sees x", || y.node_alive("x"));
    a.stop();
    let out = y.cli_ok(&["remove", "x"]);
    assert_eq!(out["final"], false, "{out}");
    assert!(holds_removal(&y, "x", "y"));
    let warning = out["warning"].as_str().unwrap();
    assert!(
        warning.contains("x can undo this removal")
            && warning.contains("run `potluck remove x` again when a is up"),
        "{warning}"
    );
    let back = removal_by(&x, "x", &y);
    assert_eq!(members_with(&y, back), ["a", "x"]);
    // With a up again, the removal asked again is a's: final.
    a.start();
    wait_until("y sees a", || y.node_alive("a"));
    let out = y.cli_ok(&["remove", "x"]);
    assert_eq!(out["final"], true, "{out}");
    wait_until("y holds a's removal of x", || holds_removal(&y, "x", "a"));
    let back = removal_by(&x, "x", &y);
    assert_eq!(members_with(&y, back), ["a", "y"]);
}

/// `node`'s saved store, as its daemon last saved it.
fn trust_of(node: &Node) -> potluck::mesh::trust::Trust {
    potluck::mesh::trust::Trust::load_read_only(&node.paths.keys_dir()).unwrap()
}

/// `node`'s machine key.
fn key_of(node: &Node) -> potluck::mesh::PublicKey {
    potluck::mesh::identity::Identity::load(&node.paths.keys_dir())
        .unwrap()
        .public()
}

fn error_of(o: &Out) -> String {
    o.json["error"].as_str().unwrap_or_default().to_string()
}

/// `potluck user init` on the founder of a cluster of three: every member stays one, now by the
/// user's admission, and jobs run across them as before. From then on only what a user signs
/// admits or removes a machine: `admit --pool cloud` with the user's key joins a machine in that
/// pool; a member without a user's key can't admit or remove, its daemon signs neither, and the
/// records members sign count for nothing; a removal the user signs is final.
#[test]
fn after_user_init_only_what_users_sign_admits_and_removes() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let out = a.cli_ok(&["user", "init", "--name", "owner"]);
    assert_eq!(out["user"], "owner", "{out}");
    assert_eq!(out["members"], serde_json::json!(["a", "b", "c"]), "{out}");
    assert!(out["warning"].is_null(), "a founded the cluster: {out}");
    assert_eq!(
        std::path::PathBuf::from(out["file"].as_str().unwrap()),
        a.dir.join("user.key")
    );
    for n in [&a, &b, &c] {
        wait_until(&format!("{} has the first user", n.name), || {
            let t = trust_of(n);
            t.users_exist()
                && t.view.members.len() == 3
                && t.view
                    .members
                    .values()
                    .all(|m| m.by_user() && m.by == "owner")
        });
    }
    for n in [&a, &b, &c] {
        let log = n.daemon_log();
        assert!(
            !log.contains(" left (") && !log.contains(" removed "),
            "{log}"
        );
    }
    let job = a.dispatch(&["--on", "node=c", "--", "true"]);
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("c"))
    );
    let users = a.cli_ok(&["user", "list"]);
    assert_eq!(users["users"][0]["name"], "owner", "{users}");
    assert_eq!(users["users"][0]["this_cli"], true, "{users}");
    assert_eq!(b.cli_ok(&["user", "list"])["users"][0]["this_cli"], false);
    let again = a.cli(&["user", "init"]);
    assert!(
        again.code == 2 && error_of(&again).contains("has users already"),
        "{}",
        again.stdout
    );

    // d joins in pool cloud, admitted with the user's key.
    let d = w.node("d").start();
    let request = d.cli_ok(&["pair"])["text"].as_str().unwrap().to_string();
    let refused = b.cli(&["admit", &request]);
    assert!(
        refused.code == 2 && error_of(&refused).contains("signed with a user's key"),
        "{}",
        refused.stdout
    );
    let e = b
        .client()
        .call(Request::Admit {
            request: request.clone(),
            admission: None,
        })
        .unwrap_err()
        .to_string();
    assert!(e.contains("only a user's signed admission"), "{e}");
    let reply = a.cli_ok(&["admit", "--pool", "cloud", &request]);
    assert_eq!(reply["pool"], "cloud", "{reply}");
    d.cli_ok(&["pair", "--finish", reply["text"].as_str().unwrap()]);
    wait_until("a and d meet", || a.node_alive("d") && d.node_alive("a"));
    let dk = key_of(&d);
    for n in [&a, &b, &c, &d] {
        wait_until(&format!("{} has d in pool cloud", n.name), || {
            trust_of(n).pool_of(&dk) == Some("cloud")
        });
    }
    assert_eq!(trust_of(&a).pool_of(&key_of(&b)), Some("default"));

    // What a member signs counts for nothing: an admission of another machine, a removal.
    let stranger = tempfile::tempdir().unwrap();
    let ek = potluck::mesh::identity::Identity::load_or_create(stranger.path())
        .unwrap()
        .public();
    let by_b = {
        let id = potluck::mesh::identity::Identity::load(&b.paths.keys_dir()).unwrap();
        let t = trust_of(&b);
        potluck::mesh::Record::Admission(potluck::mesh::Admission::sign(
            &id,
            "b",
            "e",
            ek,
            "127.0.0.1:1",
            "",
            t.next_clock(),
        ))
    };
    assert_eq!(members_with(&a, by_b), ["a", "b", "c", "d"]);
    assert_eq!(
        members_with(&a, removal_by(&b, "b", &c)),
        ["a", "b", "c", "d"]
    );
    let refused = b.cli(&["remove", "c"]);
    assert!(
        refused.code == 2 && error_of(&refused).contains("signed with a user's key"),
        "{}",
        refused.stdout
    );
    let e = b
        .client()
        .call(Request::Remove {
            node: "c".into(),
            removal: None,
        })
        .unwrap_err()
        .to_string();
    assert!(e.contains("only a user's signed removal"), "{e}");
    // Nor does a sign one that b relays (as it signs, without users, one of a machine admitted
    // after it).
    let relayed = {
        let req = potluck::proto::ClientRequest {
            ctx: ClientContext::default(),
            req: Request::Remove {
                node: "c".into(),
                removal: None,
            },
        };
        let header = serde_json::json!({ "kind": "client", "req": req });
        let stream = client_mesh(&b).open_stream("a", header).unwrap();
        let mut r = std::io::BufReader::new(stream);
        potluck::proto::read_msg::<_, potluck::proto::ServerResponse>(&mut r)
            .unwrap()
            .unwrap()
            .resp
    };
    match relayed {
        Response::Error { message } => assert!(message.contains("signs no removal"), "{message}"),
        other => panic!("a relayed removal was answered: {other:?}"),
    }
    assert!(trust_of(&a).view.by_name("c").is_some());
    assert!(trust_of(&b).view.by_name("c").is_some());

    // The user's removal: final, everywhere.
    let out = a.cli_ok(&["remove", "c"]);
    assert_eq!(out["final"], true, "{out}");
    assert!(out["warning"].is_null(), "{out}");
    for n in [&a, &b, &d] {
        wait_until(&format!("{} holds owner's removal of c", n.name), || {
            holds_removal(n, "c", "owner")
        });
    }
    wait_until("c knows it was removed", || says_out(&c));
    // a settled the removal once, as the command made it.
    let log = a.daemon_log();
    assert_eq!(log.matches("cluster: removed c\n").count(), 1, "{log}");
    assert!(!log.contains("cluster: owner removed c"), "{log}");
    assert!(b.daemon_log().contains("cluster: owner removed c"));
}

/// On a machine in no cluster, `potluck user init` founds one. A second command line's user key,
/// added with `potluck user add`, signs admissions there until a user removes it; what it signed
/// before stays. The last user stays.
#[test]
fn a_second_users_key_signs_until_it_is_removed() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let out = a.cli_ok(&["user", "init", "--name", "owner"]);
    assert_eq!(out["members"], serde_json::json!(["a"]), "{out}");
    assert!(trust_of(&a).users_exist());
    assert!(a.listening().is_none(), "a cluster of one doesn't listen");
    pair(&a, &b);
    let made = b.cli_ok(&["user", "key"]);
    assert_eq!(made["made"], true, "{made}");
    let key = made["key"].as_str().unwrap().to_string();
    assert_eq!(b.cli_ok(&["user", "key"])["key"], key.as_str());
    let bob = potluck::mesh::PublicKey::from_text(&key).unwrap();
    let refused = b.cli(&["user", "add", "eve", &key]);
    assert!(
        refused.code == 2 && error_of(&refused).contains("is no user of this cluster"),
        "{}",
        refused.stdout
    );
    // A machine's key is no user's: its daemon holds it.
    let machine = a.cli(&["user", "add", "bkey", &key_of(&b).to_text()]);
    assert!(
        machine.code == 2 && error_of(&machine).contains("b's machine key"),
        "{}",
        machine.stdout
    );
    a.cli_ok(&["user", "add", "bob", &key]);
    wait_until("b knows bob", || trust_of(&b).recognized_user(&bob));
    // bob, on b, admits c.
    let c = w.node("c").start();
    pair(&b, &c);
    wait_until("a sees c", || a.node_alive("c"));
    assert_eq!(trust_of(&a).view.by_name("c").unwrap().by, "bob");
    // owner removes bob: b's key signs nothing that counts after, and c, which bob admitted
    // before, stays.
    a.cli_ok(&["user", "remove", "bob"]);
    wait_until("b knows bob is no user", || {
        !trust_of(&b).recognized_user(&bob)
    });
    let refused = b.cli(&["remove", "c"]);
    assert!(
        refused.code == 2 && error_of(&refused).contains("is no user of this cluster"),
        "{}",
        refused.stdout
    );
    assert!(trust_of(&a).view.by_name("c").is_some());
    assert!(a.node_alive("c"));
    let last = a.cli(&["user", "remove", "owner"]);
    assert!(
        last.code == 2 && error_of(&last).contains("the last user"),
        "{}",
        last.stdout
    );
    let users = a.cli_ok(&["user", "list"]);
    assert_eq!(users["users"].as_array().unwrap().len(), 1, "{users}");
}

/// b can't save its records (a full disk; here its keys directory is read-only) when it hears it
/// was removed. The mesh that replaced its listening one read the records from disk, where b is
/// still a member: it listened again, heard of the removal again, and so on at every heartbeat.
/// Now the mesh stays until the records are saved, and b stays out throughout.
#[test]
fn a_removal_this_machine_cant_save_yet_puts_it_out_once() {
    use std::os::unix::fs::PermissionsExt;
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let keys = b.paths.keys_dir();
    let mode = |m: u32| std::fs::set_permissions(&keys, std::fs::Permissions::from_mode(m));
    mode(0o500).unwrap();
    a.cli_ok(&["remove", "b"]);
    wait_until("b hears of its removal", || {
        b.daemon_log().contains("WARNING: this machine was removed")
    });
    std::thread::sleep(Duration::from_secs(3));
    let out = says_out(&b);
    mode(0o700).unwrap();
    let warnings = b
        .daemon_log()
        .matches("WARNING: this machine was removed")
        .count();
    assert!(out && warnings == 1, "out {out}, {warnings} warnings");
    let file = keys.join("members.json");
    wait_until("b saves the removal", || {
        std::fs::read_to_string(&file).is_ok_and(|s| s.contains("\"kind\": \"removal\""))
    });
    wait_until("b stops listening", || b.listening().is_none());
    std::thread::sleep(Duration::from_secs(1));
    assert!(says_out(&b));
    let warnings = b
        .daemon_log()
        .matches("WARNING: this machine was removed")
        .count();
    assert_eq!(warnings, 1);
}

/// b dies while running two jobs: one queues again and runs on c, the `--no-rerun` one ends as
/// lost (125). When b comes back it learns the jobs are no longer its and stops them.
#[test]
fn a_lost_runner_and_its_return() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").cpu("fast").start();
    pair(&a, &b);
    let cmd = "echo first on $POTLUCK_NODE; if [ $POTLUCK_NODE = b ]; then sleep 60; fi; echo done on $POTLUCK_NODE";
    let rerun = a.dispatch(&["--on", "cpu_model=fast", "--", "sh", "-c", cmd]);
    let once = a.dispatch(&["--on", "cpu_model=fast", "--no-rerun", "--", "sleep", "60"]);
    a.running_on(&rerun, "b");
    a.running_on(&once, "b");
    wait_until("a has b's first line", || {
        output_of(&a, &rerun).contains("first on b")
    });
    let c = w.node("c").cpu("fast").start();
    pair(&a, &c);
    let pids: Vec<u32> = [&rerun, &once]
        .iter()
        .map(|j| b.exec.launch_of(j).unwrap().pid)
        .collect();
    b.stop();
    let j = a.done(&once);
    assert_eq!(
        (j.state, j.exit_code),
        (JobState::Lost, Some(125)),
        "{j:#?}"
    );
    assert_eq!(
        j.message.as_deref(),
        Some("b was lost; not queued again: its dispatch said --no-rerun"),
        "{j:#?}"
    );
    let o = a.cli(&["wait", &once]);
    assert_eq!(o.code, 125);
    // The state is said once.
    assert!(
        a.daemon_log()
            .contains(&format!("{once}: lost: b was lost; not queued again")),
        "{}",
        a.daemon_log()
    );
    assert!(!a.daemon_log().contains("lost: lost:"));
    let j = a.done(&rerun);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("c")),
        "{j:#?}"
    );
    let log = output_of(&a, &rerun);
    assert!(
        log.contains("first on b")
            && log.contains("requeued: b was lost")
            && log.ends_with("first on c\ndone on c\n"),
        "{log}"
    );
    // b's processes still run; b comes back, reconciles, and stops them.
    assert!(pids.iter().all(|p| b.exec.alive(*p)));
    b.start();
    wait_until("b stopped what is no longer its", || {
        pids.iter().all(|p| !b.exec.alive(*p))
    });
    // a's records stand.
    assert_eq!(a.get(&rerun).node.as_deref(), Some("c"));
    assert_eq!(a.get(&once).state, JobState::Lost);
    // a said so once.
    std::thread::sleep(Duration::from_secs(3));
    let log = a.daemon_log();
    assert_eq!(
        log.matches("holds jobs that are no longer its").count(),
        1,
        "{log}"
    );
    // Where the requeued job's second run started in its log mattered until it ended: a keeps
    // that (and rewrites it whole at each requeue) no longer.
    wait_until(
        "a forgot where the ended job's run started in its log",
        || {
            !a.kv("cluster/log-base")
                .unwrap_or_default()
                .contains(&rerun)
        },
    );
}

/// A job queued again after its runner was lost leaves that runner's warnings behind: its log has
/// them once, from the lost run; the next run, on another machine, doesn't repeat them, and the
/// job's record no longer carries them.
#[test]
fn a_requeued_job_leaves_the_lost_runners_warnings_behind() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    *b.ws.sync_warning.lock().unwrap() = Some("the mirror had changed (test)".into());
    let job = a.dispatch(&[
        "--gpu",
        "any",
        "--",
        "sh",
        "-c",
        "echo run on $POTLUCK_NODE; sleep 60",
    ]);
    a.running_on(&job, "b");
    wait_until("a has b's output", || {
        output_of(&a, &job).contains("run on b")
    });
    let c = w.node("c").gpu().start();
    pair(&a, &c);
    b.stop();
    a.running_on(&job, "c");
    wait_until("a has c's output", || {
        output_of(&a, &job).contains("run on c")
    });
    let log = output_of(&a, &job);
    assert_eq!(
        log,
        "--- potluck: warning: b: the mirror had changed (test) ---\nrun on b\n\n--- potluck: requeued: b was lost ---\nrun on c\n",
        "{log}"
    );
    assert_eq!(a.get(&job).notes, Vec::<String>::new());
    assert_eq!(c.get(&job).notes, Vec::<String>::new());
    a.cli_ok(&["cancel", &job]);
    a.done(&job);
}

/// A job queued again can go back to a machine that ran it before: b runs it, is lost, c takes it
/// and b stops its copy when back; then c is lost and b, the only machine up that can run it,
/// takes it again. a's log has each run once.
#[test]
fn a_job_goes_back_to_a_machine_that_ran_it_before() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let job = a.dispatch(&[
        "--gpu",
        "any",
        "--keep",
        "k.txt",
        "--",
        "sh",
        "-c",
        "echo run on $POTLUCK_NODE; echo $POTLUCK_NODE > k.txt; sleep 60",
    ]);
    a.running_on(&job, "b");
    wait_until("a has b's output", || {
        output_of(&a, &job).contains("run on b")
    });
    let mut c = w.node("c").gpu().start();
    pair(&a, &c);
    b.stop();
    // Running on c, the job no longer says it was queued again (its log does).
    let j = a.running_on(&job, "c");
    assert_eq!(j.message, None, "{j:#?}");
    assert_eq!(c.get(&job).message, None);
    b.start();
    let first = b.exec.launch_of(&job).unwrap().pid;
    wait_until("b stopped its copy", || !b.exec.alive(first));
    wait_until("a sees b up", || a.node_alive("b"));
    wait_until("a has c's output", || {
        output_of(&a, &job).contains("run on c")
    });
    c.stop();
    let j = a.running_on(&job, "b");
    assert_ne!(j.workspace.as_deref(), Some("b-w1"), "{j:#?}");
    wait_until("a has b's second output", || {
        output_of(&a, &job).matches("run on ").count() >= 3
    });
    a.cli_ok(&["cancel", &job]);
    let j = a.done(&job);
    assert_eq!(j.state, JobState::Cancelled, "{j:#?}");
    let log = output_of(&a, &job);
    assert_eq!(
        log,
        "run on b\n\n--- potluck: requeued: b was lost ---\nrun on c\n\n--- potluck: requeued: c was lost ---\nrun on b\n",
        "{log}"
    );
}

/// A job queued again after its runner was lost, and taken by its own (dispatching) machine,
/// no longer says it was queued again once it runs there; its log does.
#[test]
fn a_requeued_job_run_by_its_owner_drops_the_requeue_message() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").start();
    pair(&a, &b);
    // a is full, so the job goes to b.
    let filler = a.dispatch(&["--on", "node=a", "--ram", "6", "--", "sleep", "60"]);
    a.running_on(&filler, "a");
    let job = a.dispatch(&[
        "--ram",
        "4",
        "--",
        "sh",
        "-c",
        "echo run on $POTLUCK_NODE; sleep 60",
    ]);
    a.running_on(&job, "b");
    a.cli_ok(&["cancel", &filler]);
    a.done(&filler);
    // The line goes after the lost run's output, so only into a log that has some.
    wait_until("a has b's output", || {
        output_of(&a, &job).contains("run on b")
    });
    b.stop();
    let j = a.running_on(&job, "a");
    assert_eq!(j.message, None, "{j:#?}");
    wait_until("a has its own output", || {
        output_of(&a, &job).contains("run on a")
    });
    assert!(output_of(&a, &job).contains("--- potluck: requeued: b was lost ---"));
    a.cli_ok(&["cancel", &job]);
    a.done(&job);
}

/// a restarts while another process holds its port: it runs without a listener for longer than
/// the down time, then listens once the port is free. The new listener starts knowing no member
/// alive; that doesn't make b lost, so a queued job only b can run keeps waiting for it.
#[test]
fn a_late_listener_does_not_lose_queued_jobs() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").gpu().start();
    pair(&a, &b);
    let first = a.dispatch(&["--gpu", "any", "--", "sleep", "60"]);
    a.running_on(&first, "b");
    let second = a.dispatch(&["--gpu", "any", "--", "true"]);
    a.stop();
    let blocker = std::net::TcpListener::bind(("127.0.0.1", a.port)).unwrap();
    a.start();
    wait_until("a can't listen", || {
        a.daemon_log().contains("can't listen for members")
    });
    // Longer than the down time without a listener.
    std::thread::sleep(Duration::from_millis(a.down_after_ms + 1_000));
    drop(blocker);
    wait_until("a listens", || a.listening().is_some());
    std::thread::sleep(Duration::from_millis(500));
    let j = a.get(&second);
    assert_eq!(j.state, JobState::Queued, "{j:#?}\n{}", a.daemon_log());
    // b was up all along: the mesh that listens, replacing the one that didn't, met it again,
    // but a says it is up only once.
    let log = a.daemon_log();
    let since_restart = log.rsplit_once(" serving ").unwrap().1;
    assert_eq!(
        since_restart.matches("cluster: b is up").count(),
        1,
        "{since_restart}"
    );
    a.cli_ok(&["cancel", &second]);
    a.cli_ok(&["cancel", &first]);
    a.done(&first);
}

/// While its port is taken, a tries it again now and then (after 1 s, 2 s, 4 s, ...). Meanwhile b
/// stays up as far as a knows: a job only b can run is never refused for want of a machine.
#[test]
fn a_member_trying_its_port_again_keeps_its_members_up() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    a.stop();
    let _blocker = std::net::TcpListener::bind(("127.0.0.1", a.port)).unwrap();
    a.start();
    wait_until("a hears from b", || a.node_alive("b"));
    // Past the tries at 1 s and 3 s.
    let start = Instant::now();
    let mut dispatched = 0;
    while start.elapsed() < Duration::from_millis(4_500) {
        let o = a.cli(&[
            "dispatch",
            "--timeout",
            "1m",
            "--on",
            "node=b",
            "--",
            "true",
        ]);
        assert_eq!(o.code, 0, "after {:?}: {}", start.elapsed(), o.json);
        dispatched += 1;
    }
    assert!(dispatched > 10, "{dispatched}");
    let log = a.daemon_log();
    assert_eq!(log.matches("can't listen for members").count(), 1, "{log}");
}

/// While its port is taken, a still pings the members: b going silent then is noticed, and the
/// job a had running there ends as lost.
#[test]
fn a_member_that_cant_listen_still_notices_a_lost_one() {
    let w = World::new();
    let mut a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let job = a.dispatch(&["--gpu", "any", "--no-rerun", "--", "sleep", "60"]);
    a.running_on(&job, "b");
    a.stop();
    let blocker = std::net::TcpListener::bind(("127.0.0.1", a.port)).unwrap();
    a.start();
    wait_until("a can't listen", || {
        a.daemon_log().contains("can't listen for members")
    });
    wait_until("a hears from b", || a.node_alive("b"));
    // Said once, not again on every try.
    let log = a.daemon_log();
    assert_eq!(log.matches("can't listen for members").count(), 1, "{log}");
    let pid = b.exec.launch_of(&job).unwrap().pid;
    b.stop();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.exit_code),
        (JobState::Lost, Some(125)),
        "{j:#?}"
    );
    // Once a listens again, b back reaches it and stops what is no longer its.
    drop(blocker);
    b.start();
    wait_until("b stops what is no longer its", || !b.exec.alive(pid));
}

/// The runner's daemon is stopped inside `Executor::start` of another machine's job (the job
/// runs, but the runner's store still says starting, with no handle) and comes back before the
/// owner counts it lost. It adopts the run: the job ends once, with its own exit code, its
/// output reaches the owner once, and nothing is handed back or run again.
#[test]
fn a_start_cut_short_on_the_runner_is_adopted() {
    let w = World::new();
    // b's restart, however slow, is over before a would count it lost.
    let a = w.node("a").down_after(20_000).start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let job = a.dispatch(&[
        "--on",
        "node=b",
        "--",
        "sh",
        "-c",
        "echo ran; sleep 3; exit 4",
    ]);
    a.running_on(&job, "b");
    b.stop();
    {
        // What a runner killed inside start leaves: the job starting, no handle.
        let db = rusqlite::Connection::open(b.paths.db()).unwrap();
        let json: String = db
            .query_row("SELECT json FROM jobs WHERE id = ?1", [&job], |r| r.get(0))
            .unwrap();
        let mut rec: Value = serde_json::from_str(&json).unwrap();
        rec["state"] = "starting".into();
        rec["handle"] = Value::Null;
        rec["started_at"] = Value::Null;
        db.execute(
            "UPDATE jobs SET state = 'starting', json = ?2 WHERE id = ?1",
            [&job, &rec.to_string()],
        )
        .unwrap();
        let key = format!("run/{job}");
        let run: String = db
            .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
            .unwrap();
        let mut run: Value = serde_json::from_str(&run).unwrap();
        run["phase"] = "starting".into();
        db.execute(
            "UPDATE kv SET value = ?2 WHERE key = ?1",
            [&key, &run.to_string()],
        )
        .unwrap();
    }
    b.start();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.exit_code_for_client(), j.node.as_deref()),
        (JobState::Failed, 4, Some("b")),
        "{j:#?}\n{}",
        b.daemon_log()
    );
    assert_eq!(output_of(&a, &job), "ran\n");
    let starts = b.exec.launched().iter().filter(|l| l.job == job).count();
    assert_eq!(starts, 1, "started again");
    let log = b.daemon_log();
    assert!(
        log.contains(&format!(
            "{job}: its start was cut short by a restart, but it runs; adopted"
        )) && !log.contains("handed back"),
        "{log}"
    );
    assert!(!a.daemon_log().contains("requeued"), "{}", a.daemon_log());
}

/// A commit b's mirror held and then lost (git gc pruned it, a reclone didn't bring it back) is
/// synced again when a job needs it, not taken as still there.
#[test]
fn a_commit_gone_from_a_mirror_is_synced_again() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").gpu().start();
    pair(&a, &b);
    let first = a.dispatch(&["--gpu", "any", "--", "true"]);
    assert_eq!(a.done(&first).node.as_deref(), Some("b"));
    b.ws.mirror.lock().unwrap().clear();
    let syncs = b.ws.synced.lock().unwrap().len();
    let second = a.dispatch(&["--gpu", "any", "--", "true"]);
    let r = a.done(&second);
    assert_eq!(
        (r.state, r.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{r:#?}"
    );
    assert!(b.ws.synced.lock().unwrap().len() > syncs);
}

/// Two owners whose repositories have the same path (on their own machines; here one directory)
/// run jobs on one runner: each owner salts the repository id with its name, so the runner keeps
/// a mirror for each, synced from its own owner, and never mixes their branches and pins.
#[test]
fn two_owners_with_the_same_repository_path_get_a_mirror_each() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let c = w.node("c").gpu().start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let ja = a.dispatch(&["--gpu", "any", "--", "true"]);
    let jb = b.dispatch(&["--gpu", "any", "--", "true"]);
    let (ra, rb) = (a.done(&ja), b.done(&jb));
    for r in [&ra, &rb] {
        assert_eq!(
            (r.state, r.node.as_deref()),
            (JobState::Succeeded, Some("c")),
            "{r:#?}"
        );
    }
    let local = potluck::types::repo_id(&w.repo.join(".git"));
    assert_eq!(ra.spec.repo.id, potluck::types::owned_repo_id("a", &local));
    assert_eq!(rb.spec.repo.id, potluck::types::owned_repo_id("b", &local));
    assert_ne!(ra.spec.repo.id, rb.spec.repo.id);
    let synced = c.ws.synced.lock().unwrap().clone();
    for (owner, id) in [("a", &ra.spec.repo.id), ("b", &rb.spec.repo.id)] {
        let url = format!("fake://{owner}/{id}");
        assert!(synced.contains(&url), "{url} not in {synced:?}");
    }
}

/// The runner's sync for a job reports its mirror had diverged (or was recloned): the job's log
/// on its owner and `why` there say so, naming the runner.
#[test]
fn a_runners_sync_warning_reaches_the_jobs_log_and_why() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").gpu().start();
    pair(&a, &b);
    *b.ws.sync_warning.lock().unwrap() = Some("the mirror had changed (test)".into());
    let job = a.dispatch(&["--gpu", "any", "--", "sh", "-c", "echo ran"]);
    let r = a.done(&job);
    assert_eq!(
        (r.state, r.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{r:#?}"
    );
    let note = "b: the mirror had changed (test)".to_string();
    assert_eq!(r.notes, std::slice::from_ref(&note));
    let log = output_of(&a, &job);
    assert!(
        log.starts_with(&format!("--- potluck: warning: {note} ---\n")) && log.contains("ran"),
        "{log}"
    );
    let why = a.cli_ok(&["why", &job]);
    let lines: Vec<&str> = why["lines"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|l| l.as_str())
        .collect();
    assert!(
        lines.contains(&format!("warning: {note}").as_str()),
        "{why}"
    );
}

/// a stops while b runs its job; the job ends meanwhile; when a is back, b delivers the log, the
/// kept file and the end.
#[test]
fn the_owner_away_while_its_job_runs() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").gpu().start();
    pair(&a, &b);
    let job = a.dispatch(&[
        "--on",
        "gpu=nvidia",
        "--keep",
        "k.txt",
        "--",
        "sh",
        "-c",
        "echo before; sleep 2; echo kept > k.txt; echo after; exit 4",
    ]);
    a.running_on(&job, "b");
    a.stop();
    // b answers about a's job from its own record while a is away.
    b.wait_for(&job, "ended on b", |j| j.state.is_done());
    let local = b.get(&job);
    assert_eq!(local.exit_code, Some(4));
    a.start();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.exit_code),
        (JobState::Failed, Some(4)),
        "{j:#?}"
    );
    assert_eq!(output_of(&a, &job), "before\nafter\n");
    assert_eq!(
        std::fs::read_to_string(a.paths.job_dir(&job).join("keep/k.txt")).unwrap(),
        "kept\n"
    );
    assert_eq!(a.cli(&["wait", &job]).code, 4);
}

/// Dispatches on `a` a job that runs on b and keeps `k.txt`, and puts a file where `a` keeps
/// kept files (its job's `keep`), so they can't be delivered there.
fn dispatch_undeliverable_keep(a: &Node, exit: i32) -> String {
    let job = a.dispatch(&[
        "--on",
        "node=b",
        "--keep",
        "k.txt",
        "--",
        "sh",
        "-c",
        &format!("sleep 1; echo kept > k.txt; exit {exit}"),
    ]);
    let dir = a.paths.job_dir(&job);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("keep"), "in the way\n").unwrap();
    job
}

/// Whether job `job`'s notes say its kept files from b were not delivered.
fn keep_not_delivered(j: &JobRecord) -> bool {
    j.notes
        .iter()
        .any(|n| n.starts_with("kept files from b were not delivered: "))
}

/// Kept files the owner can't take: the runner gives up on them after the give-up time, keeps
/// them and says where; the job ends with its own code and a note, and `wait` returns.
#[test]
fn kept_files_that_cannot_be_delivered_are_given_up_on() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").keep_give_up(3_000).start();
    pair(&a, &b);
    let job = dispatch_undeliverable_keep(&a, 3);
    let o = a.cli(&["wait", &job]);
    assert_eq!(o.code, 3, "{}\n{}", o.stdout, a.daemon_log());
    let j = a.get(&job);
    assert_eq!(j.state, JobState::Failed, "{j:#?}");
    assert!(keep_not_delivered(&j), "{j:#?}");
    assert_eq!(j.kept, Vec::<String>::new());
    assert!(!a.paths.job_dir(&job).join("keep/k.txt").exists());
    assert!(
        b.daemon_log().contains(&format!(
            "{job}: WARNING: its kept files could not be delivered to a"
        )),
        "{}",
        b.daemon_log()
    );
    assert_eq!(
        std::fs::read_to_string(b.paths.job_dir(&job).join("keep/k.txt")).unwrap(),
        "kept\n"
    );
}

/// Kept files that couldn't be delivered at first and then were: the job lists them, as when
/// they arrive at the first try.
#[test]
fn kept_files_delivered_after_a_failure_are_the_jobs() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let job = dispatch_undeliverable_keep(&a, 0);
    wait_until("a waits for the kept files", || {
        a.daemon_log().contains(&format!(
            "{job}: ended on b; its kept files have not arrived"
        ))
    });
    std::fs::remove_file(a.paths.job_dir(&job).join("keep")).unwrap();
    let j = a.done(&job);
    assert_eq!((j.state, j.exit_code), (JobState::Succeeded, Some(0)));
    assert_eq!(
        std::fs::read_to_string(a.paths.job_dir(&job).join("keep/k.txt")).unwrap(),
        "kept\n"
    );
    assert!(!keep_not_delivered(&j), "{j:#?}");
    assert_eq!(j.kept, ["keep/k.txt"], "{j:#?}");
}

/// The owner gives up on kept files after the give-up time on its own clock too: here the runner
/// went down meanwhile. The job ran, so it isn't queued again while the runner is down.
#[test]
fn an_owner_gives_up_on_kept_files_from_a_runner_that_is_down() {
    let w = World::new();
    let a = w.node("a").keep_give_up(6_000).start();
    let mut b = w.node("b").start();
    pair(&a, &b);
    let job = dispatch_undeliverable_keep(&a, 0);
    wait_until("a waits for the kept files", || {
        a.daemon_log().contains(&format!(
            "{job}: ended on b; its kept files have not arrived"
        ))
    });
    let why = a.cli_ok(&["why", &job])["lines"].to_string();
    assert!(why.contains("its kept files have not arrived"), "{why}");
    b.stop();
    std::thread::sleep(Duration::from_millis(a.down_after_ms + 1_000));
    let j = a.get(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Running, Some("b")),
        "{j:#?}"
    );
    let j = a.done(&job);
    assert_eq!((j.state, j.exit_code), (JobState::Succeeded, Some(0)));
    assert!(keep_not_delivered(&j), "{j:#?}");
    assert_eq!(a.cli(&["wait", &job]).code, 0);
}

/// A runner removed while a job's kept files are on their way: the owner ends the job at once,
/// without them.
#[test]
fn kept_files_from_a_removed_runner_are_given_up_on_at_once() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").start();
    pair(&a, &b);
    let job = dispatch_undeliverable_keep(&a, 0);
    wait_until("a waits for the kept files", || {
        a.daemon_log().contains(&format!(
            "{job}: ended on b; its kept files have not arrived"
        ))
    });
    b.stop();
    std::thread::sleep(Duration::from_millis(a.down_after_ms + 1_000));
    assert_eq!(a.get(&job).state, JobState::Running);
    let start = Instant::now();
    a.cli_ok(&["remove", "b"]);
    let j = a.done(&job);
    assert!(start.elapsed() < Duration::from_secs(10));
    assert_eq!((j.state, j.exit_code), (JobState::Succeeded, Some(0)));
    assert!(keep_not_delivered(&j), "{j:#?}");
}

/// A cold machine leaves a job to a warm one (a workspace of its repository) for a few seconds;
/// when the warm one is busy for longer, the cold one takes it at once.
#[test]
fn locality_prefers_a_warm_machine() {
    // Long enough that neither a slow claim by the warm machine nor a slow CLI looks like a wait
    // for locality.
    const WAIT_MS: u64 = 10_000;
    let w = World::new();
    let a = w.node("a").locality(WAIT_MS).start();
    let b = w.node("b").cpu("fast").locality(WAIT_MS).start();
    let c = w.node("c").cpu("fast").locality(WAIT_MS).start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b sees c", || b.node_alive("c"));
    let warm = a.dispatch(&["--on", "node=b", "--", "true"]);
    a.done(&warm);
    let next = a.dispatch(&["--on", "cpu_model=fast", "--ram", "1", "--", "true"]);
    assert_eq!(a.done(&next).node.as_deref(), Some("b"));
    // b full for a minute: c takes the next one without waiting.
    let busy = a.dispatch(&["--on", "node=b", "--ram", "8", "--", "sleep", "60"]);
    a.running_on(&busy, "b");
    let other = a.dispatch(&["--on", "cpu_model=fast", "--ram", "1", "--", "true"]);
    let j = a.done(&other);
    assert_eq!(j.node.as_deref(), Some("c"));
    let waited = j.started_at.unwrap() - j.submitted_at;
    assert!(waited < WAIT_MS, "started {waited} ms after its dispatch");
    a.cli_ok(&["cancel", &busy]);
    a.done(&busy);
}

/// A job that crashes on another machine: its crash reports reach the owner's `jobs/<id>/crash`,
/// those written seconds after the crash too (the runner collects again 5 s later).
#[test]
fn crash_reports_reach_the_owner() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").gpu().start();
    pair(&a, &b);
    b.exec.crash_reports.store(true, Ordering::SeqCst);
    let job = a.dispatch(&["--on", "node=b", "--", "sh", "-c", "kill -SEGV $$"]);
    let rec = a.done(&job);
    assert_eq!(rec.signal, Some(libc::SIGSEGV), "{rec:#?}");
    let crash = a.paths.job_dir(&job).join("crash");
    let names = || -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(&crash)
            .map(|d| {
                d.filter_map(|e| e.ok()?.file_name().into_string().ok())
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    };
    wait_until("the crash reports reach a", || names().len() == 2);
    assert_eq!(names(), ["report-1.ips", "report-2.ips"]);
    assert!(b.daemon_log().contains(&format!("{job}: delivered to a")));
}

/// The same for a job whose command exits with a code after a child of it crashed (cargo after a
/// test binary did): the reports written late reach the owner too.
#[test]
fn crash_reports_of_a_child_reach_the_owner() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    b.exec.crash_reports.store(true, Ordering::SeqCst);
    let job = a.dispatch(&["--on", "node=b", "--", "sh", "-c", "exit 101"]);
    let rec = a.done(&job);
    assert_eq!((rec.exit_code, rec.signal), (Some(101), None), "{rec:#?}");
    let crash = a.paths.job_dir(&job).join("crash");
    let names = || -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(&crash)
            .map(|d| {
                d.filter_map(|e| e.ok()?.file_name().into_string().ok())
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    };
    wait_until("the crash reports reach a", || names().len() == 2);
    assert_eq!(names(), ["report-1.ips", "report-2.ips"]);
}

/// While a cold machine leaves a job to a warm one that will be free within the locality wait,
/// `why` says so, not that the cold machine can start it now.
#[test]
fn why_says_a_cold_machine_waits_for_a_warm_one() {
    let w = World::new();
    let a = w.node("a").locality(8_000).start();
    let b = w.node("b").cpu("fast").locality(8_000).start();
    let c = w.node("c").cpu("fast").locality(8_000).start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b sees c", || b.node_alive("c"));
    let warm = a.dispatch(&["--on", "node=b", "--", "true"]);
    a.done(&warm);
    // b is full until its job's run limit, a few seconds away: within c's wait.
    let busy = a.cli_ok(&[
        "dispatch",
        "--timeout",
        "4s",
        "--on",
        "node=b",
        "--ram",
        "8",
        "--",
        "sleep",
        "60",
    ])["job"]
        .as_str()
        .unwrap()
        .to_string();
    a.running_on(&busy, "b");
    let next = a.dispatch(&["--on", "cpu_model=fast", "--ram", "1", "--", "true"]);
    let mut lines = String::new();
    wait_until("why names the locality wait", || {
        lines = a.cli_ok(&["why", &next])["lines"].to_string();
        lines.contains("c could start it now, but leaves it to b, which has a workspace of its repository, for up to ")
    });
    assert!(!lines.contains("can start now on c"), "{lines}");
    assert!(a.done(&next).state == JobState::Succeeded);
    a.done(&busy);
}

/// A runner syncs its mirrors from one owner a few at a time: each sync dials the owner (through
/// `potluck _tunnel`), and the owner closes connections from one address beyond 8 still in their
/// handshake. Jobs from six repositories all run.
#[test]
fn syncs_from_one_owner_run_a_few_at_a_time() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").cpu("fast").start();
    b.ws.sync_delay_ms.store(500, Ordering::SeqCst);
    pair(&a, &b);
    let jobs: Vec<String> = (0..6)
        .map(|i| {
            let repo = w.another_repo(&format!("repo{i}"));
            let o = a.cli_in(
                &repo,
                &[
                    "dispatch",
                    "--timeout",
                    "60s",
                    "--on",
                    "cpu_model=fast",
                    "--ram",
                    "1",
                    "--",
                    "true",
                ],
            );
            assert_eq!(o.code, 0, "{}\n{}", o.stdout, o.stderr);
            o.json["job"].as_str().unwrap().to_string()
        })
        .collect();
    for j in &jobs {
        let r = a.done(j);
        assert_eq!(
            (r.state, r.node.as_deref()),
            (JobState::Succeeded, Some("b")),
            "{r:#?}"
        );
    }
    let most = b.ws.max_syncing.load(Ordering::SeqCst);
    assert!((1..=3).contains(&most), "{most} syncs from a at once");
}

/// status, nodes and why cover every member; a machine whose mirror can't sync shows as a `sync`
/// blocker until it can.
#[test]
fn status_nodes_and_why_across_members() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").gpu().start();
    let c = w.node("c").gpu().start();
    b.ws.sync_fail.store(true, Ordering::SeqCst);
    c.ws.sync_fail.store(true, Ordering::SeqCst);
    pair(&a, &b);
    pair(&a, &c);
    let job = a.dispatch(&["--gpu", "any", "--", "true"]);
    // Lines that don't change with time ("queued for 3s" does).
    let lines_of = |why: &Value| -> Vec<String> {
        why["lines"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l.as_str().unwrap().to_string())
            .filter(|l| !l.starts_with("queued for "))
            .collect()
    };
    let failed = |l: &str, n: &str| {
        l.starts_with(&format!("{n} can't take it yet: its mirror of"))
            && l.contains("the last sync from a failed")
    };
    // Both syncs failed once (until then, why says they are syncing).
    wait_until("why shows both syncs failed", || {
        let why = a.cli_ok(&["why", &job]);
        let lines = lines_of(&why);
        let syncs = why["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|b| b["resource"] == "sync")
            .count();
        syncs == 2
            && ["b", "c"]
                .iter()
                .all(|n| lines.iter().any(|l| failed(l, n)))
    });
    let why = a.cli_ok(&["why", &job]);
    let lines = lines_of(&why);
    let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
    assert!(
        lines.contains(&"a can't run it: no GPU there matches any"),
        "{lines:#?}"
    );
    assert!(
        lines.contains(&"no machine that could run it has its commits yet (the sync is behind)"),
        "{lines:#?}"
    );
    // why from b, of a's job, is a's answer.
    assert_eq!(lines_of(&b.cli_ok(&["why", &job])), lines_of(&why));
    let st = a.cli_ok(&["status"]);
    assert_eq!(st["nodes"].as_array().unwrap().len(), 3);
    assert_eq!(st["queued"][0]["id"], job.as_str());
    // b's status shows a's queued job too (the global queue), and b's sync trouble.
    let st = b.cli_ok(&["status"]);
    assert_eq!(st["queued"][0]["id"], job.as_str(), "{st}");
    assert!(
        st["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n.as_str().unwrap().contains("could not sync from a")),
        "{st}"
    );
    let nodes = c.cli_ok(&["nodes"]);
    assert_eq!(nodes["nodes"].as_array().unwrap().len(), 3);
    // c's sync works again: c runs it, at its next try (the tries back off, up to a minute).
    c.ws.sync_fail.store(false, Ordering::SeqCst);
    let start = Instant::now();
    let j = loop {
        let j = a.get(&job);
        if j.state.is_done() || start.elapsed() > Duration::from_secs(90) {
            break j;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("c"))
    );
}

/// `why` of a job pinned to a workspace on another machine, where another job runs: it waits for
/// that workspace, as the scheduler there does. It said "can start now on b; the scheduler takes
/// it on its next pass", with no blockers, for as long as the other job ran.
#[test]
fn why_of_a_job_pinned_to_a_busy_workspace_elsewhere_names_the_job_in_it() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let first = a.dispatch(&["--on", "node=b", "--", "sleep", "30"]);
    let ws = a.running_on(&first, "b").workspace.unwrap();
    let pinned = a.dispatch(&["--workspace", &ws, "--", "true"]);
    let why = a.cli_ok(&["why", &pinned]);
    assert_eq!(a.get(&pinned).state, JobState::Queued);
    let lines = why["lines"].to_string();
    assert!(
        lines.contains(&format!(
            "waits for its workspace {ws} on b: job {first} runs in it"
        )) && !lines.contains("can start now"),
        "{why}"
    );
    assert!(
        why["blockers"].as_array().unwrap().iter().any(|x| {
            x["resource"] == "workspace" && x["node"] == "b" && x["holder"] == first.as_str()
        }),
        "{why}"
    );
    a.cli_ok(&["cancel", &first]);
    let j = a.done(&pinned);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
}

/// `status` on a member lists a job where it runs now, not where a member that is down last said
/// it ran. d's job ran on a; a was lost, and b runs it now. c listed it as running on a, from the
/// last report a sent it, which c keeps, and that hid b's.
#[test]
fn status_lists_a_job_where_it_runs_now_not_where_a_lost_member_said() {
    let w = World::new();
    let d = w.node("d").start();
    let mut a = w.node("a").cpu("fast").start();
    let c = w.node("c").start();
    pair(&d, &a);
    pair(&d, &c);
    let job = d.dispatch(&["--on", "cpu_model=fast", "--", "sleep", "60"]);
    d.running_on(&job, "a");
    let listed_on = |n: &Node| -> Vec<String> {
        n.cli_ok(&["status"])["running"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["id"] == job.as_str())
            .map(|r| r["node"].as_str().unwrap_or_default().to_string())
            .collect()
    };
    wait_until("c has a's report of the job", || listed_on(&c) == ["a"]);
    let b = w.node("b").cpu("fast").start();
    pair(&d, &b);
    a.stop();
    d.running_on(&job, "b");
    wait_until("c counts a down", || !c.node_alive("a"));
    let start = Instant::now();
    loop {
        let on = listed_on(&c);
        if on == ["b"] {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "c's status lists {job} as running on {on:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    d.cli_ok(&["cancel", &job]);
    d.done(&job);
}

/// A member on a newer version makes the older one update itself, from a local test release;
/// `self-update` answers first, then updates.
#[test]
fn a_newer_member_makes_the_older_update() {
    let w = World::new();
    let server = release::Server::start(&release::files("0.1.1"));
    // The binary the older machine replaces: a stand-in that answers --version.
    let bin = w.dir.path().join("bin/potluck");
    std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
    std::fs::write(&bin, "#!/bin/sh\necho 'potluck 0.1.0'\n").unwrap();
    std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let asked: Arc<Mutex<Vec<Option<String>>>> = Arc::default();
    let updater: Updater = {
        let (asked, bin, url) = (asked.clone(), bin.clone(), server.url());
        Arc::new(move |v: Option<&str>| {
            // As update_here_to does with POTLUCK_RELEASES_URL set, without the restart.
            let source = potluck::update::Source::Directory(url.clone());
            let plan = potluck::update::Plan {
                source: &source,
                target: &bin,
                version: v,
                allow_lower: false,
                daemon: None,
            };
            let r = potluck::update::update(&plan, false);
            asked.lock().unwrap().push(v.map(str::to_string));
            r.map(Into::into)
        })
    };
    let a = w.node("a").updater(updater).start();
    let mut b = w.node("b").start();
    pair(&a, &b);
    b.stop();
    b.version = "0.1.1".into();
    b.start();
    wait_until("a updated itself to 0.1.1", || {
        asked.lock().unwrap().contains(&Some("0.1.1".into()))
    });
    let out = Command::new(&bin).arg("--version").output().unwrap();
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "potluck 0.1.1");
    assert!(a.daemon_log().contains("UPDATING: b runs potluck 0.1.1"));
    // They refuse each other: neither counts the other as up (a's daemon still runs 0.1.0).
    let up = |n: &Node, other: &str| {
        n.cli_ok(&["nodes"])["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["facts"]["node"] == other && m["alive"] == true)
    };
    assert!(!up(&a, "b") && !up(&b, "a"));
    // b logs a's version when the two first meet, which may come after a's update.
    wait_until("b logged a's older version", || {
        b.daemon_log()
            .contains("a runs potluck 0.1.0, older than this machine's 0.1.1")
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(asked.lock().unwrap().len(), 1, "once per version");
    assert!(
        b.updates.lock().unwrap().is_empty(),
        "the newer side never moves"
    );

    // self-update: the answer first, then the update of this machine to the latest release.
    let v = b.cli_ok(&["self-update"]);
    let text = v["text"].as_str().unwrap();
    // The update runs after the answer, so the answer says where its outcome goes.
    let log = b.paths.daemon_log();
    assert!(
        text.contains("latest release")
            && text.contains(&format!("how it went goes to {}", log.display())),
        "{v}"
    );
    wait_until("b updated itself", || {
        b.updates.lock().unwrap().contains(&None)
    });
    wait_until("the outcome is in the daemon log", || {
        std::fs::read_to_string(&log).is_ok_and(|l| l.contains("self-update: "))
    });
}

/// `self-update` while a restart in place drains: the daemon starts no update then, and the
/// daemon after the restart knows nothing of the request, so it says it isn't updating and to
/// ask again. Before, it answered "updating a to the latest release now", the CLI exited 0, and
/// no update ran and nothing was logged about it.
#[test]
fn self_update_while_a_restart_in_place_drains_is_refused() {
    let w = World::new();
    let a = w.node("a").start();
    a.daemon
        .as_ref()
        .unwrap()
        .drain(Duration::from_secs(5))
        .unwrap();
    let o = a.cli(&["self-update"]);
    let said = o.json["error"].as_str().unwrap_or_default();
    assert_ne!(o.code, 0, "{}{}", o.stdout, o.stderr);
    assert!(
        said.starts_with("not updating a now: its daemon is restarting in place")
            && said.contains("run `potluck self-update` again"),
        "{said}"
    );
    // An update started would have been asked of the updater by now.
    std::thread::sleep(Duration::from_millis(600));
    assert!(a.updates.lock().unwrap().is_empty());
}

/// A machine back from being away (here its daemon restarted) compares membership records with
/// every other member before it hands work to one, and waits up to the down time for those it
/// doesn't hear from: with c off, a grants b's claim only once the down time has passed. (A
/// member removed while a was away would be known as removed by then.)
#[test]
fn a_machine_back_hands_out_work_after_comparing_records_with_the_others() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").gpu().start();
    let mut c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b sees c", || b.node_alive("c"));
    c.stop();
    a.stop();
    let back = Instant::now();
    a.start();
    wait_until("a sees b", || a.node_alive("b"));
    let j = a.dispatch(&["--gpu", "any", "--", "true"]);
    let r = a.done(&j);
    assert_eq!(
        (r.state, r.node.as_deref()),
        (JobState::Succeeded, Some("b"))
    );
    let started = r.started_at.unwrap();
    let elapsed = back.elapsed().as_millis() as u64;
    let since_start = potluck::types::now_ms().saturating_sub(started);
    assert!(
        elapsed.saturating_sub(since_start) >= 1_500,
        "started {}ms after a came back",
        elapsed.saturating_sub(since_start)
    );
}

/// While the owner, back from being away, compares membership records before it grants a claim,
/// `why` says the runner waits for that, not that it can start the job now, nor (while the runner
/// tries a claim the owner refuses) that the job waits on what the runner reserved for it.
#[test]
fn why_says_a_runner_waits_while_its_owner_catches_up() {
    let w = World::new();
    let mut a = w.node("a").down_after(8_000).start();
    let b = w.node("b").gpu().start();
    let mut c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b sees c", || b.node_alive("c"));
    c.stop();
    a.stop();
    let back = Instant::now();
    a.start();
    wait_until("a sees b", || a.node_alive("b"));
    // Until b's mirror has the job's commit, why says b is syncing. Before a has b's report on
    // the job, why names no sync either: b syncs only once why has said so, and the wait below
    // ends when why says what b waits for once it has the commit.
    b.ws.sync_hold.store(true, Ordering::SeqCst);
    let j = a.dispatch(&["--gpu", "any", "--", "true"]);
    let first_blocker = || a.cli_ok(&["why", &j])["blockers"][0]["resource"].clone();
    wait_until("why says b syncs", || first_blocker() == "sync");
    b.ws.sync_hold.store(false, Ordering::SeqCst);
    wait_until("b has the job's commit", || first_blocker() == "catch_up");
    // b's claims are refused all along (a waits its 8 s down time for c): ask through several,
    // up to 2 s before a may grant one.
    let mut asked = 0;
    while back.elapsed() < Duration::from_secs(6) || asked == 0 {
        let why = a.cli_ok(&["why", &j]);
        let lines = why["lines"].to_string();
        assert!(
            lines.contains("b could start it now, but waits while a, back from being away, compares membership records with the other members (up to 8s)"),
            "{why:#}\nA:\n{}\nB:\n{}", a.daemon_log(), b.daemon_log()
        );
        assert!(!lines.contains("can start now"), "{lines}");
        assert_eq!(why["blockers"][0]["resource"], "catch_up", "{why}");
        asked += 1;
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(a.done(&j).state, JobState::Succeeded);
}

/// While a runner, back from being away, compares membership records before it takes an owner's
/// jobs, `why` on the owner says so, not that it can start the job now.
#[test]
fn why_says_a_runner_back_from_away_catches_up_first() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").gpu().down_after(8_000).start();
    let mut c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b sees c", || b.node_alive("c"));
    c.stop();
    b.stop();
    b.start();
    wait_until("a sees b", || a.node_alive("b"));
    let j = a.dispatch(&["--gpu", "any", "--", "true"]);
    let mut why = serde_json::Value::Null;
    wait_until("why names b's catch-up", || {
        why = a.cli_ok(&["why", &j]);
        why["lines"].to_string().contains(
            "b could start it now, but is back from being away and compares membership records with the other members first\"",
        )
    });
    assert!(!why["lines"].to_string().contains("can start now"), "{why}");
    assert_eq!(why["blockers"][0]["resource"], "catch_up", "{why}");
    assert_eq!(a.done(&j).state, JobState::Succeeded);
}

/// A member restarted on another version mid-job (one of the two is updating) still counts as
/// alive for a while: its jobs run on and are neither queued again nor ended as lost, and `nodes`
/// shows it as updating, with its version.
#[test]
fn a_member_on_another_version_is_updating_not_lost() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let j = a.dispatch(&["--gpu", "any", "--", "sleep", "60"]);
    a.running_on(&j, "b");
    let pid = b.exec.launch_of(&j).unwrap().pid;
    b.stop();
    b.version = "0.1.1".into();
    b.start();
    // Longer than the down time (1.5 s).
    std::thread::sleep(Duration::from_secs(4));
    let r = a.get(&j);
    assert_eq!(
        (r.state, r.node.as_deref()),
        (JobState::Running, Some("b")),
        "{r:#?}\n{}",
        a.daemon_log()
    );
    assert!(b.exec.alive(pid));
    assert!(!a.daemon_log().contains("b is down"), "{}", a.daemon_log());
    let nodes = a.cli_ok(&["nodes"]);
    let nb = nodes["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["facts"]["node"] == "b")
        .unwrap()
        .clone();
    assert_eq!(
        (&nb["alive"], &nb["updating"], &nb["facts"]["version"]),
        (
            &serde_json::json!(false),
            &serde_json::json!(true),
            &serde_json::json!("0.1.1")
        ),
        "{nb}"
    );
    let human = Command::new(BIN)
        .arg("nodes")
        .env("POTLUCK_HOME", &a.paths.home)
        .env("POTLUCK_CONFIG", &a.paths.config)
        .output()
        .unwrap();
    let human = String::from_utf8_lossy(&human.stdout).into_owned();
    let row = human.lines().find(|l| l.starts_with("b ")).unwrap_or("");
    assert!(row.contains("updating") && row.contains("0.1.1"), "{human}");
    // b's workspace is still there: a job pinned to it is queued for b, which is updating (not
    // refused, nor ended because b were down), and `why` says it waits for the update.
    let ws = r.workspace.clone().unwrap();
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        &ws,
        "--",
        "true",
    ]);
    assert_eq!(o.code, 0, "{}", o.json);
    let k = o.json["job"].as_str().unwrap().to_string();
    assert_eq!(a.get(&k).state, JobState::Queued);
    let why = a.cli_ok(&["why", &k])["lines"].to_string();
    assert!(
        why.contains("b could run it but runs potluck 0.1.1, not this machine's 0.1.0")
            && why.contains("it waits for the members to run one version"),
        "{why}"
    );
}

/// `potluck env NODE` on any member: NODE's login environment, asked of NODE's daemon over the
/// cluster connection. A machine that isn't a member, a member that is down, and one on another
/// version are said.
#[test]
fn env_of_another_member_comes_from_its_daemon() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").start();
    pair(&a, &b);
    let home = |n: &Node| n.dir.display().to_string();
    let here = a.cli_ok(&["env"]);
    assert_eq!(here["node"], "a");
    assert_eq!(here["env"]["HOME"], home(&a).as_str(), "{here}");
    for asker in [&a, &b] {
        let there = asker.cli_ok(&["env", "b"]);
        assert_eq!(there["node"], "b");
        assert_eq!(there["env"]["HOME"], home(&b).as_str(), "{there}");
        assert_eq!(there["env"]["PATH"], "/usr/bin:/bin:/usr/sbin:/sbin");
    }
    let human = Command::new(BIN)
        .args(["env", "b"])
        .env("POTLUCK_HOME", &a.paths.home)
        .env("POTLUCK_CONFIG", &a.paths.config)
        .output()
        .unwrap();
    assert_eq!(human.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&human.stdout),
        format!("HOME={}\nPATH=/usr/bin:/bin:/usr/sbin:/sbin\n", home(&b))
    );
    let note = String::from_utf8_lossy(&human.stderr).into_owned();
    assert!(
        note.starts_with(
            "potluck: this is b's login environment, as its daemon captured it; a job on b gets"
        ),
        "{note}"
    );

    let o = a.cli(&["env", "nosuch"]);
    assert_eq!(
        (o.code, o.json["error"].as_str()),
        (
            2,
            Some("nosuch is not a member of the cluster (`potluck nodes` lists them)")
        ),
        "{}",
        o.stdout
    );

    b.stop();
    wait_until("a counts b down", || !a.node_alive("b"));
    let o = a.cli(&["env", "b"]);
    assert_eq!(
        (o.code, o.json["error"].as_str()),
        (
            2,
            Some("b is down, and only its own daemon has its environment")
        ),
        "{}",
        o.stdout
    );

    b.version = "0.1.1".into();
    b.start();
    wait_until("a hears b on 0.1.1", || {
        let nodes = a.cli_ok(&["nodes"]);
        nodes["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["facts"]["node"] == "b" && n["updating"] == true)
    });
    let o = a.cli(&["env", "b"]);
    assert_eq!(
        (o.code, o.json["error"].as_str()),
        (
            2,
            Some(
                "b runs potluck 0.1.1, not this machine's 0.1.0, and members on two versions \
                 don't answer each other's requests"
            )
        ),
        "{}",
        o.stdout
    );
}

/// A dispatch only a member on another version can run (the two are mid-update: one restarted on
/// a new version, the other updates itself next) is queued, not refused, and runs there once the
/// two run one version again. Before, it was refused: "no machine that can run this is up (b:
/// runs potluck 0.1.1, not this machine's 0.1.0)" (in a cluster-wide update across two machines,
/// 3 of 31 dispatches made during it).
#[test]
fn a_dispatch_only_an_updating_member_can_run_waits_for_it() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    wait_until("a sees b", || a.node_alive("b"));
    b.stop();
    b.version = "0.1.1".into();
    b.start();
    wait_until("a hears b on 0.1.1", || {
        a.cli_ok(&["nodes"])["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["facts"]["node"] == "b" && n["updating"] == true)
    });
    let o = a.cli(&["dispatch", "--timeout", "1m", "--gpu", "any", "--", "true"]);
    assert_eq!(o.code, 0, "{}\n{}", o.json, a.daemon_log());
    let j = o.json["job"].as_str().unwrap().to_string();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(a.get(&j).state, JobState::Queued);
    let why = a.cli_ok(&["why", &j])["lines"].to_string();
    assert!(
        why.contains("b could run it but runs potluck 0.1.1, not this machine's 0.1.0")
            && why.contains("it waits for the members to run one version"),
        "{why}"
    );
    // A job no machine could run at all is still refused at once.
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--on",
        "os=plan9",
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2, "{}", o.json);
    // b back on a's version (as when a has updated too): the job runs there.
    b.stop();
    b.version = "0.1.0".into();
    b.start();
    let r = a.done(&j);
    assert_eq!(
        (r.state, r.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{r:#?}\n{}",
        a.daemon_log()
    );
}

/// `wait` on c, of a job a owns and b ran, while a can't be reached but still counts as alive
/// (restarting) and b is down: the job is over (a ended it as lost once b was), and nothing c
/// can reach knows how it is now. `wait --max-wait` said "job ... is still running" (exit 3),
/// from the last report b sent c before it went down, which c keeps; now it says it can't reach
/// a, as for a request that isn't a long poll.
#[test]
fn a_wait_while_its_owner_restarts_is_not_answered_from_a_runner_that_is_down() {
    let w = World::new();
    let mut a = w.node("a").start();
    let mut b = w.node("b").start();
    // c counts a member down after 8 s: a restarting stays alive for it meanwhile.
    let c = w.node("c").down_after(8_000).start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let j = a.dispatch(&["--on", "node=b", "--", "sleep", "60"]);
    a.running_on(&j, "b");
    wait_until("c has b's report of it", || {
        c.cli_ok(&["status"])["running"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["id"] == j.as_str())
    });
    b.stop();
    let r = a.done(&j);
    assert_eq!(
        (r.state, r.exit_code),
        (JobState::Lost, Some(125)),
        "{r:#?}"
    );
    wait_until("c counts b down", || !c.node_alive("b"));
    a.stop();
    let o = c.cli(&["wait", "-q", &j, "--max-wait", "2s"]);
    assert_eq!(o.code, 2, "{}\nC:\n{}", o.json, c.daemon_log());
    let error = o.json["error"].as_str().unwrap_or_default();
    assert!(
        error.starts_with(&format!("can't reach a, the machine job {j} belongs to")),
        "{error}"
    );
}

/// `wait -q` on b, of a job a owns that ended before a went to another version (one of the two
/// updating), waits for a past the CLI's long poll, as for a job that runs. It exited 2 after
/// about 20 s ("can't reach a, the machine job ... belongs to"): no report of a member up lists
/// a job that ended.
#[test]
fn a_wait_for_an_ended_job_waits_while_its_owner_updates() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").down_after(30_000).start();
    pair(&a, &b);
    let j = a.dispatch(&["--on", "node=a", "--", "true"]);
    a.done(&j);
    wait_until("b has a's report without the job", || {
        b.cli_ok(&["status"])["running"]
            .as_array()
            .unwrap()
            .is_empty()
    });
    a.stop();
    a.version = "0.1.1".into();
    a.start();
    wait_until("b hears a on 0.1.1", || {
        b.cli_ok(&["nodes"])["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["facts"]["node"] == "a" && n["updating"] == true)
    });
    let mut wait = b.spawn_cli(&["wait", "-q", &j]);
    // Past the CLI's long poll (20 s).
    std::thread::sleep(Duration::from_secs(23));
    let early = wait.try_wait().unwrap();
    a.stop();
    a.version = "0.1.0".into();
    a.start();
    let out = wait.wait_with_output().unwrap();
    assert_eq!(
        (early, out.status.code()),
        (None, Some(0)),
        "wait -q on b: {}\nB:\n{}",
        String::from_utf8_lossy(&out.stderr),
        b.daemon_log()
    );
}

/// `wait` and `logs -f` on one member, of a job another member owns and runs, go on while that
/// member can't be reached but still counts as alive: restarting, then on another version (one
/// of the two updating, as in a cluster-wide update), then back on this one's. They used to fail
/// with "can't reach a ...: a runs potluck 0.1.1 and this machine 0.1.0; members talk only on the
/// same version".
#[test]
fn wait_and_logs_of_a_job_owned_elsewhere_go_on_while_its_owner_updates() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").down_after(30_000).start();
    pair(&a, &b);
    wait_until("b sees a", || b.node_alive("a"));
    let j = a.dispatch(&[
        "--on",
        "node=a",
        "--",
        "sh",
        "-c",
        "i=1; while [ $i -le 80 ]; do echo line $i; i=$((i+1)); sleep 0.1; done; exit 3",
    ]);
    a.running_on(&j, "a");
    let wait = b.spawn_cli(&["wait", &j]);
    let logs = b.spawn_cli(&["logs", "-f", &j]);
    std::thread::sleep(Duration::from_millis(500));
    a.stop();
    a.version = "0.1.1".into();
    a.start();
    wait_until("b hears a on 0.1.1", || {
        b.cli_ok(&["nodes"])["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["facts"]["node"] == "a" && n["updating"] == true)
    });
    std::thread::sleep(Duration::from_secs(1));
    a.stop();
    a.version = "0.1.0".into();
    a.start();
    let want: String = (1..=80).map(|i| format!("line {i}\n")).collect();
    for (what, child, code) in [("wait", wait, 3), ("logs -f", logs, 0)] {
        let out = child.wait_with_output().unwrap();
        assert_eq!(
            (
                out.status.code(),
                String::from_utf8_lossy(&out.stdout).into_owned()
            ),
            (Some(code), want.clone()),
            "{what} on b: {}\nB:\n{}",
            String::from_utf8_lossy(&out.stderr),
            b.daemon_log()
        );
    }
}

/// A child's stdout, read into a buffer as it comes.
fn collect_stdout(child: &mut std::process::Child) -> Arc<Mutex<Vec<u8>>> {
    use std::io::Read;
    let buf: Arc<Mutex<Vec<u8>>> = Arc::default();
    let mut out = child.stdout.take().unwrap();
    let to = buf.clone();
    std::thread::spawn(move || {
        let mut chunk = [0u8; 4096];
        while let Ok(n) = out.read(&mut chunk) {
            if n == 0 {
                break;
            }
            to.lock().unwrap().extend_from_slice(&chunk[..n]);
        }
    });
    buf
}

/// While a job's owner can't be reached, only the machine running it answers about it from its
/// own record and log. a's job ran on c, was queued again when c was lost, and runs on b: a's
/// log of it is c's output and a line saying why, then b's. `logs -f` on b, while a restarts,
/// read b's log at a's offsets (skipping as much as a's log held before b's run) and, a back,
/// read a's at the offsets it reached in b's (repeating it). `wait` on c, whose run a took away
/// (its record ended cancelled), exited 130 at once from that record, while b ran the job.
#[test]
fn only_the_runner_answers_for_an_owner_that_cant_be_reached() {
    let w = World::new();
    let mut a = w.node("a").start();
    // c counts a member down after 8 s: a restarting stays alive for it meanwhile.
    let mut c = w.node("c").cpu("fast").down_after(8_000).start();
    pair(&a, &c);
    let gate = |n: u32| w.dir.path().join(format!("gate{n}"));
    let cmd = format!(
        "if [ $POTLUCK_NODE = c ]; then echo first on c; sleep 60; fi; \
         for n in 1 2 3; do \
           i=1; while [ $i -le 20 ]; do echo line $n.$i; i=$((i+1)); done; \
           while [ ! -e '{}/gate'$n ]; do sleep 0.05; done; \
         done",
        w.dir.path().display()
    );
    let job = a.dispatch(&["--on", "cpu_model=fast", "--", "sh", "-c", &cmd]);
    a.running_on(&job, "c");
    wait_until("a has c's output", || {
        output_of(&a, &job).contains("first on c")
    });
    let b = w.node("b").cpu("fast").start();
    pair(&a, &b);
    let first = c.exec.launch_of(&job).unwrap().pid;
    c.stop();
    a.running_on(&job, "b");
    // c back: its run is no longer its, and ends cancelled there.
    c.start();
    wait_until("c stopped its run", || !c.exec.alive(first));
    wait_until("c forgot its run", || {
        !c.kv("cluster/outbox").unwrap_or_default().contains(&job)
    });
    let mut logs = b.spawn_cli(&["logs", "-f", &job]);
    let seen = collect_stdout(&mut logs);
    let has = |s: &str| String::from_utf8_lossy(&seen.lock().unwrap()).contains(s);
    wait_until("logs -f on b reached line 1.20", || has("line 1.20\n"));
    a.stop();
    let mut wait = c.spawn_cli(&["wait", "-q", &job]);
    std::fs::write(gate(1), "").unwrap();
    wait_until("logs -f on b reached line 2.20", || has("line 2.20\n"));
    std::thread::sleep(Duration::from_millis(500));
    let answered_early = wait.try_wait().unwrap();
    a.start();
    std::fs::write(gate(2), "").unwrap();
    std::fs::write(gate(3), "").unwrap();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{j:#?}"
    );
    let status = logs.wait().unwrap();
    let want = output_of(&a, &job);
    assert!(
        want.starts_with("first on c\n\n--- potluck: requeued: c was lost ---\nline 1.1\n"),
        "{want}"
    );
    assert_eq!(
        (
            status.code(),
            String::from_utf8_lossy(&seen.lock().unwrap()).into_owned()
        ),
        (Some(0), want),
        "logs -f on b\nB:\n{}",
        b.daemon_log()
    );
    let out = wait.wait_with_output().unwrap();
    assert_eq!(
        (answered_early, out.status.code()),
        (None, Some(0)),
        "wait on c: {}\nC:\n{}",
        String::from_utf8_lossy(&out.stderr),
        c.daemon_log()
    );
}

/// Builds of one version that differ only in build metadata (`+...`) are the same version:
/// neither updates to the other, so they pair and one runs the other's jobs.
#[test]
fn builds_that_differ_only_in_build_metadata_work_together() {
    let w = World::new();
    let a = w.node("a").version("0.1.0+build.1").start();
    let b = w.node("b").version("0.1.0+build.2").gpu().start();
    pair(&a, &b);
    let j = a.dispatch(&["--gpu", "any", "--", "true"]);
    let r = a.done(&j);
    assert_eq!(
        (r.state, r.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{r:#?}\n{}",
        a.daemon_log()
    );
}

/// A member on another version counts as alive for [`MeshConfig::updating_for`] only: after
/// that (here 2.5 s) it is down, and its jobs are settled as a lost machine's.
#[test]
fn a_member_on_another_version_for_too_long_is_down() {
    let w = World::new();
    let a = w.node("a").updating(2_500).start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let j = a.dispatch(&["--gpu", "any", "--", "sleep", "60"]);
    a.running_on(&j, "b");
    b.stop();
    b.version = "0.1.1".into();
    b.start();
    let since = Instant::now();
    let r = a.done(&j);
    assert_eq!(
        (r.state, r.exit_code, r.message.as_deref()),
        (
            JobState::Lost,
            Some(125),
            Some("no machine that can run it is up (b was lost)")
        ),
        "{r:#?}"
    );
    assert!(since.elapsed() >= Duration::from_millis(2_500));
}

/// Jobs dispatched for a member on another version (only it can run them, or into one of its
/// workspaces) wait for it only for the updating window (here 2.5 s): then it counts as down,
/// and once it has been for the down time they end as lost (125), as jobs queued before do.
#[test]
fn a_dispatch_for_an_updating_member_ends_lost_after_the_window() {
    let w = World::new();
    let a = w.node("a").updating(2_500).start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let first = a.dispatch(&["--gpu", "any", "--", "sleep", "60"]);
    let ws = a.running_on(&first, "b").workspace.unwrap();
    b.stop();
    b.version = "0.1.1".into();
    b.start();
    wait_until("a hears b on 0.1.1", || {
        a.cli_ok(&["nodes"])["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["facts"]["node"] == "b" && n["updating"] == true)
    });
    let since = Instant::now();
    let j = a.dispatch(&["--gpu", "any", "--", "true"]);
    let k = a.dispatch(&["--workspace", &ws, "--", "true"]);
    for (job, why) in [
        (
            &j,
            "no machine that can run it is up (b was lost)".to_string(),
        ),
        (
            &k,
            format!("workspace {ws} is gone: its machine b was lost"),
        ),
    ] {
        let r = a.done(job);
        assert_eq!(
            (r.state, r.exit_code, r.message.as_deref()),
            (JobState::Lost, Some(125), Some(why.as_str())),
            "{r:#?}\n{}",
            a.daemon_log()
        );
    }
    assert!(since.elapsed() >= Duration::from_millis(2_000));
}

/// Right after pairing, `nodes` and `status` show a member that is up with its facts, not as up
/// with none (its report hadn't been pulled yet).
#[test]
fn a_member_just_paired_is_shown_with_its_facts() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let request = b.cli_ok(&["pair"])["text"].as_str().unwrap().to_string();
    let reply = a.cli_ok(&["admit", &request])["text"]
        .as_str()
        .unwrap()
        .to_string();
    b.cli_ok(&["pair", "--finish", &reply]);
    let start = Instant::now();
    let mut seen_up = false;
    while start.elapsed() < Duration::from_secs(3) {
        for (n, other) in [(&a, "b"), (&b, "a")] {
            for cmd in ["nodes", "status"] {
                let v = n.cli_ok(&[cmd]);
                let nodes = v["nodes"].as_array().cloned().unwrap_or_default();
                assert!(!nodes.is_empty(), "{}: {cmd}: {v}", n.name);
                for m in nodes.iter().filter(|m| m["facts"]["node"] == other) {
                    if m["alive"] == true {
                        seen_up = true;
                        assert_ne!(m["facts"]["os"], "", "{}: {cmd}: {m}", n.name);
                    }
                }
            }
        }
    }
    assert!(seen_up);
}

/// A release directory served over plain HTTP on 127.0.0.1.
mod release {
    use std::io::{BufRead, BufReader, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;

    pub struct Server {
        addr: SocketAddr,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    /// This platform's asset (a script answering `--version`) and SHA256SUMS.
    pub fn files(version: &str) -> Vec<(String, Vec<u8>)> {
        let asset = potluck::update::asset_name().unwrap();
        let binary = format!("#!/bin/sh\necho 'potluck {version}'\n").into_bytes();
        let digest = ring::digest::digest(&ring::digest::SHA256, &binary);
        let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        vec![
            (
                "SHA256SUMS".into(),
                format!("{hex}  {asset}\n").into_bytes(),
            ),
            (asset.to_string(), binary),
        ]
    }

    impl Server {
        pub fn start(files: &[(String, Vec<u8>)]) -> Server {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let files: Vec<(String, Vec<u8>)> = files
                .iter()
                .map(|(n, d)| (format!("/rel/{n}"), d.clone()))
                .collect();
            let stop = Arc::new(AtomicBool::new(false));
            let s2 = stop.clone();
            let thread = std::thread::spawn(move || {
                for conn in listener.incoming() {
                    if s2.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(mut conn) = conn else { continue };
                    let mut r = BufReader::new(conn.try_clone().unwrap());
                    let mut line = String::new();
                    if r.read_line(&mut line).is_err() {
                        continue;
                    }
                    loop {
                        let mut h = String::new();
                        if r.read_line(&mut h).unwrap_or(0) == 0 || h.trim().is_empty() {
                            break;
                        }
                    }
                    let path = line.split_whitespace().nth(1).unwrap_or("");
                    let (status, body) = match files.iter().find(|(n, _)| n == path) {
                        Some((_, d)) => ("200 OK", d.as_slice()),
                        None => ("404 Not Found", &b"not found\n"[..]),
                    };
                    let head = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = conn.write_all(head.as_bytes());
                    let _ = conn.write_all(body);
                }
            });
            Server {
                addr,
                stop,
                thread: Some(thread),
            }
        }

        pub fn url(&self) -> String {
            format!("http://{}/rel/", self.addr)
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.addr);
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }
}

/// A dispatch counts only machines that are up: with the only capable member down, a job only it
/// could run is refused, saying so (also after this daemon restarts, from the facts it kept).
#[test]
fn dispatch_refused_while_the_only_capable_member_is_down() {
    let w = World::new();
    let mut a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let j = a.dispatch(&["--gpu", "any", "--", "true"]);
    assert_eq!(a.done(&j).node.as_deref(), Some("b"));
    b.stop();
    wait_until("a sees b down", || !a.node_alive("b"));
    let refused = |a: &Node| {
        let o = a.cli(&["dispatch", "--timeout", "1m", "--gpu", "any", "--", "true"]);
        assert_eq!(o.code, 2, "{} {}", o.json, o.stderr);
        o.json["error"].as_str().unwrap().to_string()
    };
    let e = refused(&a);
    assert!(
        e.starts_with("no machine that can run this is up (b: down since ") && e.contains("a: "),
        "{e}"
    );
    // A job a can run is still taken.
    let mine = a.dispatch(&["--", "true"]);
    assert_eq!(a.done(&mine).node.as_deref(), Some("a"));
    a.stop();
    a.start();
    let e = refused(&a);
    assert!(
        e.starts_with("no machine that can run this is up (b: "),
        "{e}"
    );
    // b back: taken again.
    b.start();
    wait_until("a sees b up", || a.node_alive("b"));
    let j = a.dispatch(&["--gpu", "any", "--", "true"]);
    assert_eq!(a.done(&j).node.as_deref(), Some("b"));
}

/// b, the only machine with a GPU, is lost while it runs one GPU job and another waits for the
/// GPU: neither waits for b to come back. The running one isn't queued again (no machine up could
/// run it) and the queued one ends: both lost, exit 125.
#[test]
fn jobs_only_a_lost_machine_could_run_end_as_lost() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let running = a.dispatch(&["--gpu", "any", "--", "sleep", "60"]);
    a.running_on(&running, "b");
    let queued = a.dispatch(&["--gpu", "any", "--", "true"]);
    let why = a.cli_ok(&["why", &queued]);
    assert_eq!(why["state"], "queued", "{why}");
    // And one queued in the running job's workspace on b.
    let ws = a.get(&running).workspace.unwrap();
    let pinned = a.dispatch(&["--workspace", &ws, "--", "true"]);
    b.stop();
    let r = a.done(&pinned);
    assert_eq!(
        (r.state, r.exit_code, r.message.as_deref()),
        (
            JobState::Lost,
            Some(125),
            Some(format!("workspace {ws} is gone: its machine b was lost").as_str())
        ),
        "{r:#?}"
    );
    for j in [&running, &queued] {
        let r = a.done(j);
        assert_eq!(
            (r.state, r.exit_code, r.message.as_deref()),
            (
                JobState::Lost,
                Some(125),
                Some("no machine that can run it is up (b was lost)")
            ),
            "{r:#?}"
        );
        assert_eq!(a.cli(&["wait", j]).code, 125);
    }
    // why says what happened, nothing more: the queued one never had a machine.
    let why = a.cli_ok(&["why", &queued]);
    let lines: Vec<&str> = why["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l.as_str().unwrap())
        .collect();
    assert!(
        lines[0].starts_with("ended ") && lines[0].ends_with(" ago without starting"),
        "{lines:?}"
    );
    assert_eq!(
        lines[1..],
        [
            "no machine that can run it is up (b was lost)",
            "potluck wait exits with 125"
        ]
    );
    // A job a itself can run is unaffected.
    let mine = a.dispatch(&["--", "true"]);
    assert_eq!(a.done(&mine).state, JobState::Succeeded);
}

/// Members forward only what cluster-design.md lists (a job's requests to its owner, a
/// workspace's to its machine, a removal to a member admitted before the machine removed).
/// Anything else relayed by a member, such as an admission, is refused: it would have this
/// machine sign a membership record on another member's word.
#[test]
fn only_job_workspace_and_removal_requests_are_relayed() {
    use potluck::proto::{ClientRequest, ServerResponse, read_msg};
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("c sees b", || c.node_alive("b"));
    // c relays to a, with its own keys (as `potluck _tunnel` dials), a request to admit a
    // machine.
    let mesh = client_mesh(&c);
    let relay = |req: Request| -> Response {
        let req = ClientRequest {
            ctx: ClientContext {
                repo_id: None,
                branch: None,
                job: None,
                lease_token: None,
            },
            req,
        };
        let header = serde_json::json!({ "kind": "client", "req": req });
        let stream = mesh.open_stream("a", header).unwrap();
        let mut r = std::io::BufReader::new(stream);
        read_msg::<_, ServerResponse>(&mut r).unwrap().unwrap().resp
    };
    let request = w.node("d").start().cli_ok(&["pair"])["text"]
        .as_str()
        .unwrap()
        .to_string();
    match relay(Request::Admit {
        request,
        admission: None,
    }) {
        Response::Error { message } => assert!(message.contains("relayed"), "{message}"),
        other => panic!("a relayed admission was answered: {other:?}"),
    }
    assert_eq!(a.cli_ok(&["nodes"])["nodes"].as_array().unwrap().len(), 3);
    // What members do relay still works.
    assert!(matches!(
        relay(Request::Get { job: "a-999".into() }),
        Response::Error { message } if message.contains("no job a-999")
    ));
    // A removal only when a was admitted before the machine removed: never of a, the founder.
    match relay(Request::Remove {
        node: "a".into(),
        removal: None,
    }) {
        Response::Error { message } => {
            assert!(message.contains("a was not admitted before a"), "{message}")
        }
        other => panic!("a relayed removal of the founder was answered: {other:?}"),
    }
    assert!(matches!(
        relay(Request::Remove {
            node: "b".into(),
            removal: None
        }),
        Response::Ok
    ));
    assert!(
        a.daemon_log()
            .contains("cluster: removal of b signed for c"),
        "{}",
        a.daemon_log()
    );
}

/// An owner that can't write a job's log as it arrives (a full disk; here a log file it may not
/// write) says so in its daemon log, and the runner's says why the owner refused; once the owner
/// can write again, the log arrives whole and the job ends there.
#[test]
fn an_owner_that_cannot_write_a_log_says_why() {
    use std::os::unix::fs::PermissionsExt;
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let job = a.dispatch(&[
        "--on",
        "node=b",
        "--",
        "sh",
        "-c",
        "while [ ! -e \"$TMPDIR/go\" ]; do sleep 0.05; done; echo hello; exit 3",
    ]);
    let j = a.running_on(&job, "b");
    let log = a.paths.job_dir(&job).join("output.log");
    std::fs::create_dir_all(log.parent().unwrap()).unwrap();
    let _ = std::fs::remove_file(&log);
    std::fs::write(&log, "").unwrap();
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o444)).unwrap();
    let tmp = b.paths.workspace_tmp(j.workspace.as_deref().unwrap());
    std::fs::write(tmp.join("go"), "").unwrap();
    wait_until("a says it can't write the log", || {
        a.daemon_log()
            .contains(&format!("{job}: could not write the output log b sends"))
    });
    wait_until("b says why", || {
        b.daemon_log().contains(&format!(
            "{job}: reporting to a: could not write the output log on a"
        ))
    });
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o644)).unwrap();
    let j = a.done(&job);
    assert_eq!(j.exit_code, Some(3), "{j:#?}");
    assert_eq!(output_of(&a, &job), "hello\n");
}

/// A job that cuts its own output log short on its runner (by writing to the log's path; its
/// stdout is a pipe, so `> /dev/stdout` no longer does) still reports home: the owner keeps the
/// output it already has, a line saying the log was cut short, and the runner's log after that;
/// the job ends there with its code. (The runner used to wait forever for its log to grow back
/// to the owner's length, so the owner never heard that the job ended.)
#[test]
fn a_log_the_job_cut_short_on_its_runner_still_reports_home() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let job = a.dispatch(&[
        "--on",
        "node=b",
        "--",
        "sh",
        "-c",
        "i=0; while [ $i -lt 40 ]; do echo line $i of the output; i=$((i+1)); done; \
         while [ ! -e \"$TMPDIR/go\" ]; do sleep 0.05; done; \
         : > \"$POTLUCK_HOME/jobs/$POTLUCK_JOB/output.log\"; echo cut; exit 4",
    ]);
    let j = a.running_on(&job, "b");
    wait_until("a has the first output", || {
        output_of(&a, &job).contains("line 39 of the output")
    });
    let tmp = b.paths.workspace_tmp(j.workspace.as_deref().unwrap());
    std::fs::write(tmp.join("go"), "").unwrap();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.exit_code),
        (JobState::Failed, Some(4)),
        "{j:#?}"
    );
    let out = output_of(&a, &job);
    assert!(out.starts_with("line 0 of the output\n"), "{out}");
    assert!(out.contains("line 39 of the output\n"), "{out}");
    // Then a line saying what happened, and b's log as it is now.
    assert!(
        out.contains("--- potluck: the output log on b was cut short")
            && out.ends_with("---\ncut\n"),
        "{out}"
    );
    let w = a.cli(&["wait", &job]);
    assert_eq!(w.code, 4, "{}", w.stderr);

    // Cut short, then written past the length a already has before b looks again: the bytes
    // where b's log ended last time are not what b sent, so it starts over the same way.
    let job = a.dispatch(&[
        "--on",
        "node=b",
        "--",
        "sh",
        "-c",
        "i=0; while [ $i -lt 40 ]; do echo line $i of the output; i=$((i+1)); done; \
         while [ ! -e \"$TMPDIR/go2\" ]; do sleep 0.05; done; \
         : > \"$POTLUCK_HOME/jobs/$POTLUCK_JOB/output.log\"; \
         i=0; while [ $i -lt 80 ]; do echo after the cut, line $i; i=$((i+1)); done",
    ]);
    let j = a.running_on(&job, "b");
    wait_until("a has the first output", || {
        output_of(&a, &job).contains("line 39 of the output")
    });
    let tmp = b.paths.workspace_tmp(j.workspace.as_deref().unwrap());
    std::fs::write(tmp.join("go2"), "").unwrap();
    let j = a.done(&job);
    assert_eq!(j.state, JobState::Succeeded, "{j:#?}");
    let out = output_of(&a, &job);
    let (before, after) = out
        .split_once("follows ---\n")
        .unwrap_or_else(|| panic!("no line about the cut: {out}"));
    assert!(before.contains("line 39 of the output\n"), "{out}");
    assert_eq!(after, output_of(&b, &job), "{out}");
    assert!(after.starts_with("after the cut, line 0\n"), "{out}");
}

/// `dispatch --workspace W` while W's machine is down fails at once and says so.
#[test]
fn a_dispatch_into_a_workspace_whose_machine_is_down_fails_at_once() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").start();
    pair(&a, &b);
    let first = a.dispatch(&["--on", "node=b", "--", "true"]);
    let ws = a.done(&first).workspace.unwrap();
    b.stop();
    wait_until("a sees b down", || !a.node_alive("b"));
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        &ws,
        "--",
        "true",
    ]);
    assert_eq!(o.code, 2, "{} {}", o.json, o.stderr);
    assert_eq!(
        o.json["error"].as_str(),
        Some(format!("workspace {ws} is gone: its machine b is down").as_str()),
        "{}",
        o.json
    );
}

/// A running job pinned to a workspace is not queued again when its machine is lost, though
/// other machines could run it: it ends lost (125).
#[test]
fn a_running_job_pinned_to_a_workspace_is_not_queued_again_when_its_machine_is_lost() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").cpu("fast").start();
    let c = w.node("c").cpu("fast").start();
    pair(&a, &b);
    pair(&a, &c);
    let first = a.dispatch(&["--on", "node=b", "--", "true"]);
    let ws = a.done(&first).workspace.unwrap();
    let job = a.dispatch(&["--workspace", &ws, "--", "sleep", "60"]);
    a.running_on(&job, "b");
    b.stop();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.exit_code, j.message.as_deref()),
        (
            JobState::Lost,
            Some(125),
            Some("b was lost; not queued again: it is pinned to a workspace there")
        ),
        "{j:#?}"
    );
    assert!(a.exec.launch_of(&job).is_none() && c.exec.launch_of(&job).is_none());
}

/// The runner restarts while the job's owner is away and the job ends: what it still has to
/// deliver (log, kept file, end) is kept across the restart and reaches the owner when it is
/// back.
#[test]
fn a_runner_restarted_while_the_owner_is_away_still_delivers() {
    let w = World::new();
    let mut a = w.node("a").start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let job = a.dispatch(&[
        "--on",
        "gpu=nvidia",
        "--keep",
        "k.txt",
        "--",
        "sh",
        "-c",
        "echo before; sleep 1; echo kept > k.txt; echo after; exit 4",
    ]);
    a.running_on(&job, "b");
    a.stop();
    b.wait_for(&job, "ended on b", |j| j.state.is_done());
    b.stop();
    b.start();
    a.start();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.exit_code),
        (JobState::Failed, Some(4)),
        "{j:#?}\n{}",
        b.daemon_log()
    );
    assert_eq!(output_of(&a, &job), "before\nafter\n");
    assert_eq!(
        std::fs::read_to_string(a.paths.job_dir(&job).join("keep/k.txt")).unwrap(),
        "kept\n"
    );
}

/// A workspace another machine's queued job is pinned to stays pending while that machine is away
/// (asleep, or updating), so the workspace isn't removed from under the job.
#[test]
fn a_workspace_pinned_by_an_away_owners_job_stays_pending() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").gpu().start();
    pair(&a, &b);
    let first = a.dispatch(&["--gpu", "any", "--", "sleep", "2"]);
    let ws = a.running_on(&first, "b").workspace.unwrap();
    let second = a.dispatch(&["--workspace", &ws, "--", "true"]);
    let pending = |n: &Node| -> u64 {
        let st = n.cli_ok(&["status"]);
        st["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["id"] == ws.as_str() && x["node"] == "b")
            .map_or(0, |x| x["pending"].as_u64().unwrap())
    };
    wait_until("b counts a's queued job", || pending(&b) == 2);
    a.stop();
    wait_until("b sees a down", || !b.node_alive("a"));
    wait_until("the first job is over on b", || pending(&b) <= 1);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(pending(&b), 1, "{}", b.daemon_log());
    a.start();
    assert_eq!(a.done(&second).state, JobState::Succeeded);
}

/// A claim the owner granted, sent again by the same runner with the same lease (the mesh resends
/// a call whose connection closed before its answer came), is granted again, not refused as
/// "taken by" that same runner (which would undo its reservation of a job the owner says it has).
#[test]
fn a_repeated_claim_is_granted_again() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").gpu().start();
    pair(&a, &b);
    // b's GPU is busy, so b itself doesn't claim the second job.
    let first = a.dispatch(&["--gpu", "any", "--", "sleep", "60"]);
    a.running_on(&first, "b");
    let second = a.dispatch(&["--gpu", "any", "--", "true"]);
    let mesh = client_mesh(&b);
    let claim = serde_json::json!({
        "op": "claim",
        "job": second,
        "lease": {
            "grant": {"ram_mib": 4096, "cpu": 1, "gpus": [GPU], "exclusive": false},
            "granted_at": 1,
            "deadline": 60_001,
            "token": "repeat-token",
        },
        "workspace": "b-w9",
    });
    for n in 1..=2 {
        let r = mesh.call("a", claim.clone()).unwrap();
        assert_eq!(r["kind"], "granted", "claim {n}: {r}");
    }
    mesh.stop();
    a.cli_ok(&["cancel", &second]);
    a.cli_ok(&["cancel", &first]);
    a.done(&first);
}

/// An owner serves `git upload-pack` only for repositories its jobs name: a member asking for any
/// other gets git's error, not a repository.
#[test]
fn upload_pack_is_refused_for_a_repository_no_dispatch_named() {
    use std::io::Read;
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let mesh = client_mesh(&b);
    let mut stream = mesh
        .open_stream(
            "a",
            serde_json::json!({"kind": "upload-pack", "repo_id": "nope-0123abcd"}),
        )
        .unwrap();
    let mut got = String::new();
    stream.read_to_string(&mut got).unwrap();
    assert!(
        got.ends_with(
            "ERR a has no job queued, or run by b, that names repository nope-0123abcd\n"
        ),
        "{got:?}"
    );
    mesh.stop();
}

/// A command that sleeps a minute the first time it runs and ends at once after that: `marker`
/// says it ran.
fn sleeps_the_first_time(marker: &Path) -> Vec<String> {
    let m = marker.display();
    let script = format!(
        "if [ -e '{m}' ]; then echo again on $POTLUCK_NODE; else touch '{m}'; echo first on $POTLUCK_NODE; sleep 60; fi"
    );
    vec!["sh".into(), "-c".into(), script]
}

fn dispatch_cmd(node: &Node, flags: &[&str], cmd: &[String]) -> String {
    let mut args: Vec<&str> = flags.to_vec();
    args.push("--");
    args.extend(cmd.iter().map(String::as_str));
    node.dispatch(&args)
}

/// A cancel still on its way to the runner when the runner is lost: the job ends cancelled (130),
/// not queued again, though another machine could run it. When the runner is back it stops its
/// copy.
#[test]
fn a_cancel_on_its_way_when_the_runner_is_lost_ends_the_job_cancelled() {
    let w = World::new();
    // a counts b lost only well after the cancel is asked.
    let a = w.node("a").down_after(4_000).start();
    let mut b = w.node("b").cpu("fast").start();
    pair(&a, &b);
    let job = a.dispatch(&["--on", "cpu_model=fast", "--", "sleep", "60"]);
    a.running_on(&job, "b");
    // c could run it, were it queued again.
    let c = w.node("c").cpu("fast").start();
    pair(&a, &c);
    let pid = b.exec.launch_of(&job).unwrap().pid;
    b.stop();
    a.cli_ok(&["cancel", &job]);
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.exit_code, j.message.as_deref()),
        (
            JobState::Cancelled,
            Some(130),
            Some("b was lost before it stopped")
        ),
        "{j:#?}\n{}",
        a.daemon_log()
    );
    assert_eq!(a.cli(&["wait", &job]).code, 130);
    assert!(
        a.daemon_log()
            .contains(&format!("{job}: cancel asked; forwarded to b"))
    );
    // Never queued again: c never ran it.
    std::thread::sleep(Duration::from_secs(1));
    assert!(c.exec.launch_of(&job).is_none());
    assert_eq!(a.get(&job).state, JobState::Cancelled);
    // b's copy ran on while b was away; back, b learns it is no longer its and stops it.
    assert!(b.exec.alive(pid));
    b.start();
    wait_until("b stopped its copy", || !b.exec.alive(pid));
    assert_eq!(a.get(&job).state, JobState::Cancelled);
}

/// A cold machine leaves a job to a warm one (a workspace of its repository) that could start it
/// soon, but for the locality wait only: when the warm one doesn't take it in that time (here its
/// mirror is slow to get the job's commit), the cold one takes it.
#[test]
fn a_cold_machine_waits_for_a_warm_one_then_takes_the_job() {
    const WAIT_MS: u64 = 3_000;
    let w = World::new();
    let a = w.node("a").locality(WAIT_MS).start();
    let b = w.node("b").cpu("fast").locality(WAIT_MS).start();
    let c = w.node("c").cpu("fast").locality(WAIT_MS).start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b sees c", || b.node_alive("c"));
    let warm = a.dispatch(&["--on", "node=b", "--", "true"]);
    a.done(&warm);
    // A commit b's mirror lacks, and b slow to sync it (longer than the wait, with room to spare).
    std::fs::write(w.repo.join("README"), "a new commit\n").unwrap();
    git(&w.repo, &["commit", "-qam", "new"]);
    let id = potluck::types::repo_id(&w.repo.join(".git"));
    w.source
        .lock()
        .unwrap()
        .insert((id, git(&w.repo, &["rev-parse", "HEAD"])));
    b.ws.sync_delay_ms.store(15_000, Ordering::SeqCst);
    let job = a.dispatch(&["--on", "cpu_model=fast", "--ram", "1", "--", "true"]);
    wait_until("c leaves it to b for now", || {
        a.cli_ok(&["why", &job])["lines"]
            .to_string()
            .contains("c could start it now, but leaves it to b")
    });
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("c")),
        "{j:#?}"
    );
    let waited = j.started_at.unwrap() - j.submitted_at;
    assert!(waited >= WAIT_MS, "started {waited} ms after its dispatch");
    assert!(b.exec.launch_of(&job).is_none());
}

/// --exclusive among several machines. While an exclusive job waits for b to empty, jobs c can run
/// as well go to c: on b they would end after b is expected to empty. While it runs, b runs
/// nothing else: a job only b can run waits for its end, and one c can run goes to c.
#[test]
fn exclusive_across_machines() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").cpu("fast").start();
    let c = w.node("c").cpu("fast").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b sees c", || b.node_alive("c"));
    let busy = a.dispatch(&["--on", "node=b", "--ram", "1", "--", "sleep", "60"]);
    a.running_on(&busy, "b");
    let excl = a.dispatch(&["--on", "node=b", "--exclusive", "--", "sleep", "3"]);
    let others: Vec<String> = (0..3)
        .map(|_| a.dispatch(&["--on", "cpu_model=fast", "--ram", "1", "--", "sleep", "1"]))
        .collect();
    for j in &others {
        assert_eq!(a.done(j).node.as_deref(), Some("c"), "{j}");
    }
    assert_eq!(a.get(&excl).state, JobState::Queued);
    a.cli_ok(&["cancel", &busy]);
    a.running_on(&excl, "b");
    let only_b = a.dispatch(&["--on", "node=b", "--ram", "1", "--", "true"]);
    let either = a.dispatch(&["--on", "cpu_model=fast", "--ram", "1", "--", "true"]);
    let mut lines = String::new();
    wait_until("why says b is the exclusive job's", || {
        lines = a.cli_ok(&["why", &only_b])["lines"].to_string();
        lines.contains(&format!(
            "waits for job {excl} to end: it has b to itself (exclusive)"
        ))
    });
    assert_eq!(a.done(&either).node.as_deref(), Some("c"));
    let x = a.done(&excl);
    assert_eq!(x.state, JobState::Succeeded, "{x:#?}");
    let after = a.done(&only_b);
    assert_eq!(after.node.as_deref(), Some("b"));
    assert!(
        after.started_at.unwrap() >= x.ended_at.unwrap(),
        "{after:#?}\n{x:#?}"
    );
    // Nothing else ran on b while the exclusive job did.
    let started = x.started_at.unwrap();
    for j in others.iter().chain([&either]) {
        assert_ne!(a.get(j).node.as_deref(), Some("b"), "{j}");
    }
    assert!(a.get(&busy).ended_at.unwrap() <= started);
}

/// What a runner killed while it started another machine's job leaves in its store: the job
/// starting, no handle, its run record starting (none with `run_record` false: a record that
/// could not be read, say).
fn leave_starting(paths: &Paths, job: &str, run_record: bool) {
    let db = rusqlite::Connection::open(paths.db()).unwrap();
    let json: String = db
        .query_row("SELECT json FROM jobs WHERE id = ?1", [job], |r| r.get(0))
        .unwrap();
    let mut rec: Value = serde_json::from_str(&json).unwrap();
    rec["state"] = "starting".into();
    rec["handle"] = Value::Null;
    rec["started_at"] = Value::Null;
    db.execute(
        "UPDATE jobs SET state = 'starting', json = ?2 WHERE id = ?1",
        [job, &rec.to_string()],
    )
    .unwrap();
    let key = format!("run/{job}");
    if !run_record {
        db.execute("DELETE FROM kv WHERE key = ?1", [&key]).unwrap();
        return;
    }
    let run: String = db
        .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
        .unwrap();
    let mut run: Value = serde_json::from_str(&run).unwrap();
    run["phase"] = "starting".into();
    db.execute(
        "UPDATE kv SET value = ?2 WHERE key = ?1",
        [&key, &run.to_string()],
    )
    .unwrap();
}

/// a, and b running a's job (`--on node=b`) that printed a line and sleeps, until b's daemon
/// stops while it starts the job: b is back with what [`leave_starting`] leaves.
fn start_cut_short(w: &World, run_record: bool) -> (Node, Node, String) {
    // b's restart, however slow, is over before a would count it lost.
    let a = w.node("a").down_after(20_000).start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let cmd = sleeps_the_first_time(&w.dir.path().join("ran"));
    let job = dispatch_cmd(&a, &["--on", "node=b"], &cmd);
    a.running_on(&job, "b");
    wait_until("a has b's first line", || {
        output_of(&a, &job).contains("first on b")
    });
    let pid = b.exec.launch_of(&job).unwrap().pid;
    b.stop();
    // Nothing of it ran: no processes, and the executor finds no invocation by its name.
    b.exec.kill_group(pid, libc::SIGKILL);
    wait_until("its processes are gone", || !b.exec.alive(pid));
    b.exec
        .names
        .lock()
        .unwrap()
        .remove(&format!("potluck-{job}"));
    leave_starting(&b.paths, &job, run_record);
    b.start();
    (a, b, job)
}

/// The runner's daemon stops while it starts another machine's job, before anything of it ran:
/// back, it starts it again where it was, with the lease it holds, as it does its own (a job
/// the owner queued into its workspace meanwhile still runs there). The job then ends with its
/// own code.
#[test]
fn a_start_cut_short_before_anything_ran_starts_again_where_it_was() {
    let w = World::new();
    let (a, b, job) = start_cut_short(&w, true);
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{j:#?}\n{}\n{}",
        a.daemon_log(),
        b.daemon_log()
    );
    assert_eq!(b.exec.launched().iter().filter(|l| l.job == job).count(), 2);
    let log = b.daemon_log();
    assert!(
        log.contains(&format!("{job}: was starting; starting again"))
            && !log.contains("handed back"),
        "{log}"
    );
    assert_eq!(output_of(&a, &job), "first on b\nagain on b\n");
}

/// The same with no record of how far its start got: b can't resume it, so it hands the job back,
/// and the owner queues it again with a line in its log saying why. The job then runs (here on
/// the same machine) and ends with its own code.
#[test]
fn a_start_cut_short_without_its_run_record_is_handed_back() {
    let w = World::new();
    let (a, b, job) = start_cut_short(&w, false);
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{j:#?}\n{}\n{}",
        a.daemon_log(),
        b.daemon_log()
    );
    assert_eq!(b.exec.launched().iter().filter(|l| l.job == job).count(), 2);
    let why = "requeued: b's daemon restarted while it was starting";
    assert!(
        b.daemon_log()
            .contains(&format!("{job}: was starting for a; handed back")),
        "{}",
        b.daemon_log()
    );
    assert!(
        a.daemon_log().contains(&format!("{job}: {why}")),
        "{}",
        a.daemon_log()
    );
    assert_eq!(
        output_of(&a, &job),
        format!("first on b\n\n--- potluck: {why} ---\nagain on b\n")
    );
}

/// Removes a job from a runner's store, as if its claim had never been granted there: the job,
/// its run record and what the runner would deliver to its owner.
fn forget_job(paths: &Paths, job: &str) {
    let db = rusqlite::Connection::open(paths.db()).unwrap();
    db.execute("DELETE FROM jobs WHERE id = ?1", [job]).unwrap();
    db.execute("DELETE FROM kv WHERE key = ?1", [format!("run/{job}")])
        .unwrap();
    let outbox: String = db
        .query_row(
            "SELECT value FROM kv WHERE key = 'cluster/outbox'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let mut outbox: serde_json::Map<String, Value> = serde_json::from_str(&outbox).unwrap();
    assert!(outbox.remove(job).is_some(), "{outbox:?}");
    db.execute(
        "UPDATE kv SET value = ?1 WHERE key = 'cluster/outbox'",
        [Value::Object(outbox).to_string()],
    )
    .unwrap();
}

/// The owner granted a claim the runner doesn't hold (here b lost its records of the job while
/// its daemon was down). When the runner reconciles without it, the owner queues the job again,
/// but only once the claim grace has passed since the grant: a claim's answer may still be on its
/// way. The job then runs again.
#[test]
fn a_granted_job_the_runner_does_not_hold_is_queued_again_after_the_claim_grace() {
    const GRACE_MS: u64 = 8_000;
    let w = World::new();
    let a = w.node("a").claim_grace(GRACE_MS).down_after(30_000).start();
    let mut b = w.node("b").gpu().start();
    pair(&a, &b);
    let cmd = sleeps_the_first_time(&w.dir.path().join("ran"));
    let job = dispatch_cmd(&a, &["--on", "node=b"], &cmd);
    let rec = a.running_on(&job, "b");
    wait_until("a has b's first line", || {
        output_of(&a, &job).contains("first on b")
    });
    // The runner reserved it just before a granted it.
    let granted = rec.lease.as_ref().unwrap().granted_at;
    let pid = b.exec.launch_of(&job).unwrap().pid;
    b.stop();
    b.exec.kill_group(pid, libc::SIGKILL);
    wait_until("its processes are gone", || !b.exec.alive(pid));
    forget_job(&b.paths, &job);
    b.start();
    let back = now_ms();
    // b reconciles with a when it comes up and every second; a waits out the grace.
    let requeued = loop {
        let j = a.get(&job);
        if j.message.as_deref() == Some("requeued: b no longer has it") || j.state.is_done() {
            break now_ms();
        }
        assert!(
            now_ms() < granted + GRACE_MS + 20_000,
            "never queued again: {j:#?}\n{}",
            a.daemon_log()
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(
        back < granted + GRACE_MS,
        "b's restart took longer than the grace: the test shows nothing"
    );
    assert!(
        requeued >= granted + GRACE_MS,
        "queued again {} ms after the grant",
        requeued - granted
    );
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{j:#?}"
    );
    assert!(
        output_of(&a, &job)
            .ends_with("--- potluck: requeued: b no longer has it ---\nagain on b\n"),
        "{}",
        output_of(&a, &job)
    );
}

/// A daemon restarting in place after an update drains first: work going gets a while to end
/// ([`potluck::daemon::handover::DRAIN_MAX`]), then is cut short and done again by the next
/// daemon. A seeding of minutes doesn't hold the restart up past the drain (while it drains, the
/// daemon serves).
#[test]
fn a_restart_in_place_does_not_wait_for_a_slow_seeding() {
    let w = World::new();
    let updater: Updater = Arc::new(|_| {
        Ok(potluck::update::Updated {
            text: "updated (test)".into(),
            restart_here: true,
        })
    });
    let mut a = w.node("a").updater(updater).start();
    a.ws.create_hold.store(true, Ordering::SeqCst);
    let job = a.dispatch(&["--", "true"]);
    a.wait_for(&job, "seeding", |j| j.state == JobState::Starting);
    let daemon = a.daemon.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let handing = std::thread::spawn(move || {
        let why = daemon.wait_restart();
        daemon.restart_verdict(Ok(()));
        let start = Instant::now();
        let handed = daemon.hand_over(Duration::from_secs(2));
        let _ = tx.send((why, start.elapsed()));
        handed
    });
    a.cli_ok(&["self-update"]);
    let took = rx.recv_timeout(Duration::from_secs(20));
    // What the daemon left behind ends now (in the real daemon, starting the new binary ends it).
    a.ws.create_fail.store(true, Ordering::SeqCst);
    a.ws.create_hold.store(false, Ordering::SeqCst);
    drop(handing.join().unwrap());
    let (why, took) = took.expect("the restart waited for the seeding");
    assert_eq!(why.as_deref(), Some("the potluck binary was updated"));
    assert!(took < Duration::from_secs(6), "{took:?}");
    assert!(
        a.daemon_log()
            .contains(&format!("not waiting for start-{job}")),
        "{}",
        a.daemon_log()
    );
}

/// Only the machine's own socket restarts its daemon: a Restart request relayed by a member is
/// refused like any other request that isn't about a job or a workspace, and the daemon goes on.
#[test]
fn a_restart_relayed_by_a_member_is_refused() {
    use std::io::BufRead;
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    let mesh = client_mesh(&b);
    let req = potluck::proto::ClientRequest {
        ctx: ClientContext::default(),
        req: Request::Restart { binary: None },
    };
    let stream = mesh
        .open_stream("a", serde_json::json!({"kind": "client", "req": req}))
        .unwrap();
    let mut line = String::new();
    std::io::BufReader::new(stream)
        .read_line(&mut line)
        .unwrap();
    let answer: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(
        answer["resp"]["message"],
        "restart requests are not relayed between members: run it on the machine itself",
        "{answer}"
    );
    mesh.stop();
    std::thread::sleep(Duration::from_millis(300));
    let log = a.daemon_log();
    assert!(
        log.contains("cluster: refused a relayed restart request"),
        "{log}"
    );
    assert!(!log.contains("restarting in place"), "{log}");
    a.cli_ok(&["status"]);
}

/// An owner keeps a job another machine ran for the retention period from when the runner's
/// report of its end arrived, by its own clocks: a runner whose clock is weeks behind doesn't get
/// the job's record and log deleted at the owner's next upkeep. A job that ended 14 days before
/// goes at that upkeep.
#[test]
fn a_runner_whose_clock_is_behind_does_not_get_its_jobs_deleted() {
    const DAY: u64 = 86_400_000;
    let w = World::new();
    let clock = Arc::new(SteppedClock::default());
    let a = w.node("a").clock(clock.clone()).start();
    let behind = Arc::new(SteppedClock::default());
    behind.step_wall(-15 * DAY as i64);
    let b = w.node("b").cpu("fast").clock(behind).start();
    pair(&a, &b);
    let old = a.dispatch(&["--on", "cpu_model=slow", "--", "true"]);
    assert_eq!(a.done(&old).state, JobState::Succeeded);
    // Nearly 14 days go by on a (asleep), and an upkeep runs.
    clock.sleep(14 * DAY - 3_600_000);
    std::thread::sleep(Duration::from_millis(500));
    a.get(&old);
    // Ended on b at its run limit, when b's clock says.
    let v = a.cli_ok(&[
        "dispatch",
        "--timeout",
        "1s",
        "--on",
        "cpu_model=fast",
        "--",
        "sh",
        "-c",
        "echo ran; sleep 30",
    ]);
    let job = v["job"].as_str().unwrap().to_string();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::TimedOut, Some("b")),
        "{j:#?}"
    );
    assert!(j.ended_at.unwrap() < now_ms() - 14 * DAY, "{j:#?}");
    wait_until("the log arrives", || output_of(&a, &job).contains("ran"));
    // Two hours later on a: the next upkeep.
    clock.sleep(2 * 3_600_000);
    wait_until("the old job is deleted", || {
        !a.paths.job_dir(&old).exists() && !ended_in_store(&a.paths, &old)
    });
    let j = a.get(&job);
    assert_eq!(j.state, JobState::TimedOut);
    assert!(output_of(&a, &job).contains("ran"));
    drop(b);
}

// ---------------------------------------------------------------------------------------------
// What a member's word alone does on the others

/// A member's answer to a reconcile stops only its own jobs that the runner listed: c, answering
/// b's reconciles with a's job running on b and b's own job, stops neither.
#[test]
fn a_reconcile_answer_stops_only_the_answering_owners_jobs() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let mut c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let theirs = a.dispatch(&["--on", "node=b", "--", "sleep", "60"]);
    let own = b.dispatch(&["--on", "node=b", "--", "sleep", "60"]);
    a.running_on(&theirs, "b");
    b.running_on(&own, "b");
    let script = Arc::new(Scripted::default());
    script.answer(
        "reconcile",
        serde_json::json!({"kind": "reconciled", "drop": [theirs, own]}),
    );
    let mesh = c.impostor(script.clone());
    // a and b each reconcile with c about once a second.
    wait_until("c answered a few reconciles", || {
        script.calls("reconcile") >= 6
    });
    std::thread::sleep(Duration::from_millis(500));
    let log = b.daemon_log();
    assert!(
        !log.contains("is no longer this machine's job") && !log.contains(": stopping: "),
        "{log}"
    );
    assert_eq!(a.get(&theirs).state, JobState::Running);
    assert_eq!(b.get(&own).state, JobState::Running);
    let ignored = format!(
        "cluster: c answered a reconcile naming jobs that are not its own held here: {theirs}, {own}; ignored"
    );
    assert!(log.contains(&ignored), "{log}");
    mesh.stop();
    a.cli_ok(&["cancel", &theirs]);
    b.cli_ok(&["cancel", &own]);
    a.done(&theirs);
    b.done(&own);
}

/// `report` of `of`, as `asker` gets it.
fn report_to(asker: &Node, of: &str) -> Value {
    let mesh = client_mesh(asker);
    let r = mesh.call(of, serde_json::json!({"op": "report"})).unwrap();
    mesh.stop();
    assert_eq!(r["kind"], "report", "{r}");
    r["report"].clone()
}

/// The entry of job `id` in a report's `list` (`running`, `queue`).
fn entry_of(report: &Value, list: &str, id: &str) -> Value {
    report[list]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"] == id)
        .cloned()
        .unwrap_or_else(|| panic!("{id} is not in {list}: {report}"))
}

/// What a member's report tells each other member: no lease tokens; of a job running there, its
/// command, env, unset env, kept-file globs, labels and source paths only to its owner; of a
/// queued job, no command, env, unset env or source paths (the grant brings them to the machine
/// that claims it); no workspace paths. `status` on a third machine shows such jobs without them.
#[test]
fn reports_tell_each_member_only_what_it_needs() {
    use serde_json::json;
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let spec = [
        "--env",
        "SECRET=s3cret",
        "--unset-env",
        "HTTPS_PROXY",
        "--label",
        "team=x",
        "--keep",
        "out.txt",
        "--on",
        "node=b",
    ];
    let mut args = spec.to_vec();
    args.extend(["--exclusive", "--", "sleep", "60"]);
    let running = a.dispatch(&args);
    a.running_on(&running, "b");
    // b runs the exclusive one, so this one waits in a's queue.
    let mut args = spec.to_vec();
    args.extend(["--", "sleep", "60"]);
    let queued = a.dispatch(&args);

    let to_c = report_to(&c, "b");
    let j = entry_of(&to_c, "running", &running);
    assert_eq!(
        (
            &j["spec"]["cmd"],
            &j["spec"]["env"],
            &j["spec"]["unset_env"],
            &j["spec"]["keep"],
            &j["spec"]["labels"],
            &j["spec"]["repo"]["source"],
            &j["lease"]["token"],
        ),
        (
            &json!([]),
            &json!({}),
            &json!([]),
            &json!([]),
            &json!({}),
            &json!(""),
            &json!("")
        ),
        "{j}"
    );
    // What c plans with and shows.
    assert_eq!(j["lease"]["grant"]["exclusive"], true, "{j}");
    assert!(j["lease"]["deadline"].as_u64().unwrap() > now_ms(), "{j}");
    assert_eq!(j["spec"]["timeout_ms"], 60_000, "{j}");
    let workspaces = to_c["workspaces"].as_array().unwrap();
    assert!(!workspaces.is_empty(), "{to_c}");
    assert!(workspaces.iter().all(|w| w["path"] == ""), "{to_c}");

    // a owns it: the whole spec, but no token.
    let j = entry_of(&report_to(&a, "b"), "running", &running);
    assert_eq!(j["spec"]["cmd"], json!(["sleep", "60"]), "{j}");
    assert_eq!(j["spec"]["env"]["SECRET"], "s3cret", "{j}");
    assert_eq!(j["lease"]["token"], "", "{j}");

    // a's queue, to b and to c alike.
    for asker in [&b, &c] {
        let q = entry_of(&report_to(asker, "a"), "queue", &queued);
        assert_eq!(
            (
                &q["spec"]["cmd"],
                &q["spec"]["env"],
                &q["spec"]["unset_env"],
                &q["spec"]["repo"]["source"],
            ),
            (&json!([]), &json!({}), &json!([]), &json!("")),
            "{}: {q}",
            asker.name
        );
        assert_eq!(q["spec"]["labels"]["team"], "x", "{q}");
        assert_eq!(q["spec"]["keep"], json!(["out.txt"]), "{q}");
        assert_eq!(q["spec"]["on"]["node"], "b", "{q}");
        assert_eq!(
            q["spec"]["repo"]["commit"].as_str().unwrap().len(),
            40,
            "{q}"
        );
    }

    // c's status shows both, without their commands.
    wait_until("c's status lists both", || {
        let st = c.cli_ok(&["status"]);
        let has = |list: &str, id: &str| {
            st[list]
                .as_array()
                .unwrap()
                .iter()
                .any(|j| j["id"] == id && j["spec"]["cmd"] == json!([]))
        };
        has("running", &running) && has("queued", &queued)
    });
    let out = Command::new(BIN)
        .arg("status")
        .current_dir(&c.repo)
        .env("POTLUCK_HOME", &c.paths.home)
        .env("POTLUCK_CONFIG", &c.paths.config)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    for id in [&running, &queued] {
        let line = text
            .lines()
            .find(|l| l.starts_with(&format!("{id} ")))
            .unwrap_or_else(|| panic!("{id}: {text}"));
        assert!(line.ends_with("  -"), "{line}");
    }

    // Granted, b runs the queued one with its whole spec.
    a.cli_ok(&["cancel", &running]);
    a.running_on(&queued, "b");
    let launch = b.exec.launch_of(&queued).unwrap();
    assert_eq!(launch.env.get("SECRET").map(String::as_str), Some("s3cret"));
    assert_eq!(b.get(&queued).spec.cmd, ["sleep", "60"]);
    a.cli_ok(&["cancel", &queued]);
    a.done(&queued);
}

/// What `mesh` gets from `owner` for `git upload-pack` of repository `repo_id`.
fn upload_pack(mesh: &potluck::mesh::Mesh, owner: &str, repo_id: &str) -> String {
    use std::io::Read;
    let mut stream = mesh
        .open_stream(
            owner,
            serde_json::json!({"kind": "upload-pack", "repo_id": repo_id}),
        )
        .unwrap();
    let mut got = String::new();
    stream.read_to_string(&mut got).unwrap();
    got
}

/// A member fetches through an owner a repository one of the owner's jobs names: while the job is
/// queued (any member may take it, and syncs its mirrors before it claims), or while that member
/// holds it. Not one that only an ended job named, nor that of a job another member holds.
#[test]
fn upload_pack_serves_the_repositories_of_queued_jobs_and_of_the_askers_own() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").start();
    let c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let ended = a.dispatch(&["--on", "node=a", "--", "true"]);
    let ended_repo = a.done(&ended).spec.repo.id;
    // b takes nothing: a's job for it stays queued.
    let as_b = b.impostor(Arc::new(Scripted::default()));
    let other = w.another_repo("other");
    let o = a.cli_in(
        &other,
        &[
            "dispatch",
            "--timeout",
            "60s",
            "--on",
            "node=b",
            "--",
            "true",
        ],
    );
    assert_eq!(o.code, 0, "{}\n{}", o.stdout, o.stderr);
    let job = o.json["job"].as_str().unwrap().to_string();
    let repo = a.get(&job).spec.repo.id;
    assert_ne!(repo, ended_repo);
    let as_c = client_mesh(&c);
    let refused = |who: &str, id: &str| {
        format!("ERR a has no job queued, or run by {who}, that names repository {id}\n")
    };
    assert_eq!(upload_pack(&as_b, "a", &repo), "served\n");
    assert_eq!(upload_pack(&as_c, "a", &repo), "served\n");
    let got = upload_pack(&as_b, "a", &ended_repo);
    assert!(got.ends_with(&refused("b", &ended_repo)), "{got:?}");
    // b claims it: then it is b's to fetch, not c's.
    let claim = serde_json::json!({
        "op": "claim",
        "job": job,
        "lease": {
            "grant": {"ram_mib": 4096, "cpu": 1, "gpus": [], "exclusive": false},
            "granted_at": now_ms(),
            "deadline": now_ms() + 60_000,
            "token": "t1",
        },
        "workspace": "b-w9",
    });
    let r = as_b.call("a", claim).unwrap();
    assert_eq!(r["kind"], "granted", "{r}");
    assert_eq!(upload_pack(&as_b, "a", &repo), "served\n");
    let got = upload_pack(&as_c, "a", &repo);
    assert!(got.ends_with(&refused("c", &repo)), "{got:?}");
    as_c.stop();
    as_b.stop();
}

/// A queued job pinned to a workspace keeps it from removal only as its owner reports it: c's
/// report naming a's job pinned to b's workspace counts for nothing on b.
#[test]
fn a_workspace_is_kept_only_by_its_owners_report_of_a_job_pinned_to_it() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").gpu().start();
    let mut c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let first = a.dispatch(&["--gpu", "any", "--", "sleep", "2"]);
    let ws = a.running_on(&first, "b").workspace.unwrap();
    let second = a.dispatch(&["--workspace", &ws, "--", "true"]);
    let pending = |n: &Node| -> u64 {
        let st = n.cli_ok(&["status"]);
        st["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["id"] == ws.as_str() && x["node"] == "b")
            .map_or(0, |x| x["pending"].as_u64().unwrap())
    };
    wait_until("b counts a's queued job", || pending(&b) == 2);
    let pinned = entry_of(&report_to(&b, "a"), "queue", &second);
    let mut report = report_to(&b, "c");
    a.cli_ok(&["cancel", &second]);
    wait_until("b counts nothing", || pending(&b) == 0);
    // c's next report, as c could make it: a's job in c's queue.
    c.stop();
    report["queue"] = serde_json::json!([pinned]);
    report["seq"] = 0.into();
    let mesh = client_mesh(&c);
    let r = mesh
        .call("b", serde_json::json!({"op": "push", "report": report}))
        .unwrap();
    assert_eq!(r["kind"], "ok", "{r}");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(pending(&b), 0, "{}", b.daemon_log());
    mesh.stop();
}

/// While a job's owner is away, `wait` elsewhere takes the job's record only from its owner's
/// or its runner's report: c, saying a's failed job succeeded (queued there, or running there),
/// doesn't end a `wait` on b, which waits for a.
#[test]
fn a_wait_while_the_owner_is_away_takes_no_third_members_word() {
    let w = World::new();
    let mut a = w.node("a").start();
    let b = w.node("b").down_after(30_000).start();
    let mut c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    let j = a.dispatch(&["--on", "node=a", "--", "false"]);
    assert_eq!(a.done(&j).state, JobState::Failed);
    wait_until("b has a's report without the job", || {
        b.cli_ok(&["status"])["running"]
            .as_array()
            .unwrap()
            .is_empty()
    });
    let mut report = report_to(&b, "c");
    let mut rec = a.get(&j);
    rec.state = JobState::Succeeded;
    rec.exit_code = Some(0);
    let mut there = rec.clone();
    there.node = Some("c".into());
    report["queue"] = serde_json::json!([rec]);
    report["running"] = serde_json::json!([there]);
    report["seq"] = 0.into();
    c.stop();
    a.stop();
    let mesh = client_mesh(&c);
    let r = mesh
        .call("b", serde_json::json!({"op": "push", "report": report}))
        .unwrap();
    assert_eq!(r["kind"], "ok", "{r}");
    let o = b.cli(&["wait", "-q", &j, "--max-wait", "2s"]);
    mesh.stop();
    assert_eq!(o.code, 2, "{}\nB:\n{}", o.json, b.daemon_log());
    let error = o.json["error"].as_str().unwrap_or_default();
    assert!(
        error.starts_with(&format!("can't reach a, the machine job {j} belongs to")),
        "{error}"
    );
}

// ---------------------------------------------------------------------------------------------
// Signed jobs and pools

/// The user key `node`'s command line holds (user.key beside its node.toml).
fn user_key(node: &Node) -> potluck::mesh::user::UserKey {
    potluck::mesh::user::UserKey::load(&node.dir.join("user.key"))
        .unwrap()
        .unwrap()
}

/// The spec `owner`'s command line sent for `job`, its signature with it: what a replay of that
/// dispatch sends.
fn sent_spec(owner: &Node, job: &str) -> potluck::types::JobSpec {
    let auth = owner.get(job).spec.auth.expect("a signed job");
    potluck::types::JobSpec {
        auth: Some(auth.clone()),
        ..potluck::jobauth::open(&auth).unwrap()
    }
}

/// What `node`'s daemon answers a dispatch of `spec` made straight to its socket.
fn dispatched(node: &Node, spec: potluck::types::JobSpec) -> Result<String, String> {
    match node.client().call(Request::Dispatch {
        spec: Box::new(spec),
    }) {
        Ok(Response::Dispatched { job, .. }) => Ok(job),
        Ok(other) => panic!("{other:?}"),
        Err(e) => Err(e.to_string()),
    }
}

/// After `potluck user init` a's command line signs what it dispatches, and a job runs on another
/// member, which checks the signature first; queued again when that runner is lost, the same
/// signed job runs on a third. A dispatch without a user's signature is refused: from c's command
/// line, which has no user key, and sent straight to a's daemon. So is a replay of one of a's, and
/// one signed by a key no user has, for another machine, or long ago.
#[test]
fn after_user_init_jobs_are_signed_and_checked_where_they_run() {
    let w = World::new();
    let a = w.node("a").start();
    let mut b = w.node("b").cpu("fast").start();
    pair(&a, &b);
    a.cli_ok(&["user", "init", "--name", "owner"]);
    for n in [&a, &b] {
        wait_until(&format!("{} has the user", n.name), || {
            trust_of(n).users_exist()
        });
    }
    let user = user_key(&a);
    let cmd = "echo on $POTLUCK_NODE; if [ $POTLUCK_NODE = b ]; then sleep 60; fi";
    let job = a.dispatch(&["--on", "cpu_model=fast", "--", "sh", "-c", cmd]);
    let signed = a.get(&job).spec;
    let auth = signed.auth.clone().unwrap();
    assert_eq!(auth.key, user.public());
    assert_eq!(potluck::jobauth::open(&auth).unwrap().submitted_from, "a");
    a.running_on(&job, "b");
    assert_eq!(b.get(&job).spec.auth.unwrap().key, user.public());
    wait_until("a has b's line", || output_of(&a, &job).contains("on b"));

    // b is lost: c, admitted since with the user's key, runs the same signed job.
    let c = w.node("c").cpu("fast").start();
    pair(&a, &c);
    b.stop();
    let j = a.done(&job);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("c")),
        "{j:#?}"
    );
    assert_eq!(j.spec, signed);
    assert_eq!(c.get(&job).spec.auth, Some(auth.clone()));
    assert!(output_of(&a, &job).contains("requeued: b was lost"));

    // c's command line has no user key.
    let o = c.cli(&["dispatch", "--timeout", "60s", "--", "true"]);
    assert!(
        o.code == 2
            && error_of(&o).starts_with(
                "this cluster has users, so a dispatch is signed with a user's key, and there is none at"
            ),
        "{}",
        o.stdout
    );
    // Unsigned, to the socket.
    let sent = sent_spec(&a, &job);
    let unsigned = potluck::types::JobSpec {
        auth: None,
        ..sent.clone()
    };
    let e = dispatched(&a, unsigned.clone()).unwrap_err();
    assert!(
        e.contains("this cluster has users, so a dispatch is signed with a user's key, and this one is unsigned"),
        "{e}"
    );
    // a's dispatch again, as it was signed.
    let e = dispatched(&a, sent).unwrap_err();
    assert!(
        e.contains("this signed dispatch was made here before"),
        "{e}"
    );
    // A key no user has.
    let stranger = tempfile::tempdir().unwrap();
    let other = potluck::mesh::user::UserKey::load_or_create(&stranger.path().join("k"))
        .unwrap()
        .0;
    let e = dispatched(&a, potluck::jobauth::sign(&unsigned, &other, now_ms())).unwrap_err();
    assert!(e.contains("which is no user of this cluster"), "{e}");
    // The user's key, for another machine.
    let for_c = potluck::types::JobSpec {
        submitted_from: "c".into(),
        ..unsigned.clone()
    };
    let e = dispatched(&a, potluck::jobauth::sign(&for_c, &user, now_ms())).unwrap_err();
    assert!(
        e.contains("signed for machine \"c\", not this one, a"),
        "{e}"
    );
    // Eleven minutes ago.
    let e = dispatched(
        &a,
        potluck::jobauth::sign(&unsigned, &user, now_ms() - 660_000),
    )
    .unwrap_err();
    assert!(e.contains("more than 10m from this machine's clock"), "{e}");
    // Signed now, it runs.
    let again = dispatched(&a, potluck::jobauth::sign(&unsigned, &user, now_ms())).unwrap();
    let j = a.done(&again);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("c")),
        "{j:#?}"
    );
}

/// A job runs only on machines in the pools its signed spec names. One for pool cloud runs on d,
/// admitted with `--pool cloud`, not on a or b (pool default); a dispatch only d could run is
/// refused unless it is for pool cloud. A default job waiting in a's queue never reaches d: not in
/// a's report to it, not through upload-pack, and a refuses d's claim of it. b's report holds it
/// with who signed it, without the spec signed.
#[test]
fn a_job_runs_only_in_its_pools() {
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    pair(&a, &b);
    a.cli_ok(&["user", "init", "--name", "owner"]);
    let d = w.node("d").start();
    let request = d.cli_ok(&["pair"])["text"].as_str().unwrap().to_string();
    let reply = a.cli_ok(&["admit", "--pool", "cloud", &request]);
    d.cli_ok(&["pair", "--finish", reply["text"].as_str().unwrap()]);
    wait_until("a and d meet", || a.node_alive("d") && d.node_alive("a"));
    let dk = key_of(&d);
    for n in [&a, &b, &d] {
        wait_until(&format!("{} has d in pool cloud", n.name), || {
            trust_of(n).pool_of(&dk) == Some("cloud")
        });
    }
    wait_until("b and d see each other", || {
        b.node_alive("d") && d.node_alive("b")
    });

    let cloud = a.dispatch(&["--pool", "cloud", "--", "true"]);
    let j = a.done(&cloud);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("d")),
        "{j:#?}"
    );
    assert_eq!(j.spec.pools, ["cloud"]);
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "60s",
        "--on",
        "node=d",
        "--",
        "true",
    ]);
    assert!(
        o.code == 2 && error_of(&o).contains("d: in pool cloud, not the job's pool default"),
        "{}",
        o.stdout
    );
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "60s",
        "--pool",
        "cloud",
        "--on",
        "node=b",
        "--",
        "true",
    ]);
    assert!(
        o.code == 2 && error_of(&o).contains("b: in pool default, not the job's pool cloud"),
        "{}",
        o.stdout
    );
    let o = a.cli(&[
        "dispatch",
        "--timeout",
        "60s",
        "--pool",
        "a/b",
        "--",
        "true",
    ]);
    assert!(
        o.code == 2 && error_of(&o).contains("--pool: bad pool name \"a/b\""),
        "{}",
        o.stdout
    );

    // A default job waits for b, which runs an exclusive one.
    let busy = a.dispatch(&["--on", "node=b", "--exclusive", "--", "sleep", "60"]);
    a.running_on(&busy, "b");
    let waits = a.dispatch(&["--on", "node=b", "--", "true"]);
    let repo = a.get(&waits).spec.repo.id;
    let to_b = entry_of(&report_to(&b, "a"), "queue", &waits);
    assert_eq!(to_b["spec"]["cmd"], serde_json::json!([]), "{to_b}");
    assert_eq!(
        to_b["spec"]["auth"]["key"],
        user_key(&a).public().to_text(),
        "{to_b}"
    );
    assert!(to_b["spec"]["auth"].get("signed_spec").is_none(), "{to_b}");
    let to_d = report_to(&d, "a");
    assert!(
        !to_d["queue"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["id"] == waits.as_str()),
        "{to_d}"
    );
    let as_d = client_mesh(&d);
    let as_b = client_mesh(&b);
    let got = upload_pack(&as_d, "a", &repo);
    assert!(
        got.ends_with(&format!(
            "ERR a has no job queued, or run by d, that names repository {repo}\n"
        )),
        "{got:?}"
    );
    assert_eq!(upload_pack(&as_b, "a", &repo), "served\n");
    let claim = serde_json::json!({
        "op": "claim",
        "job": waits,
        "lease": {
            "grant": {"ram_mib": 4096, "cpu": 1, "gpus": [], "exclusive": false},
            "granted_at": now_ms(),
            "deadline": now_ms() + 60_000,
            "token": "t1",
        },
        "workspace": "d-w9",
    });
    let r = as_d.call("a", claim).unwrap();
    assert_eq!(r["kind"], "refused", "{r}");
    assert_eq!(
        r["reason"],
        format!("{waits} is not for d: it runs in pool default, and d is in pool cloud"),
        "{r}"
    );
    as_d.stop();
    as_b.stop();
    assert_eq!(a.get(&waits).state, JobState::Queued);
    a.cli_ok(&["cancel", &busy]);
    let j = a.done(&waits);
    assert_eq!(
        (j.state, j.node.as_deref()),
        (JobState::Succeeded, Some("b")),
        "{j:#?}"
    );
    assert!(d.exec.launch_of(&waits).is_none());
    assert!(a.exec.launch_of(&cloud).is_none() && b.exec.launch_of(&cloud).is_none());
}

/// An impostor owner: its report's queue holds the jobs a test made, and it grants each to the
/// member that claims it. It counts the claims, by member and job.
struct Forger {
    report: Value,
    jobs: BTreeMap<String, Value>,
    claims: Mutex<BTreeMap<(String, String), u64>>,
}

impl Forger {
    fn claims(&self, by: &str, job: &str) -> u64 {
        let key = (by.to_string(), job.to_string());
        self.claims.lock().unwrap().get(&key).copied().unwrap_or(0)
    }
}

impl Handler for Forger {
    fn call(&self, from: &Peer, msg: Value) -> Result<Value, String> {
        match msg["op"].as_str().unwrap_or_default() {
            "report" => Ok(serde_json::json!({"kind": "report", "report": self.report})),
            "claim" => {
                let id = msg["job"].as_str().unwrap_or_default().to_string();
                *self
                    .claims
                    .lock()
                    .unwrap()
                    .entry((from.name.clone(), id.clone()))
                    .or_default() += 1;
                let mut rec = self.jobs.get(&id).cloned().ok_or("no such job")?;
                rec["state"] = "starting".into();
                rec["node"] = from.name.clone().into();
                rec["lease"] = msg["lease"].clone();
                rec["workspace"] = msg["workspace"].clone();
                Ok(serde_json::json!({"kind": "granted", "job": rec, "reported": null}))
            }
            op => Err(format!("no {op} here")),
        }
    }
}

/// c, an impostor, queues jobs for b and grants them. b claims none that is unsigned or signed by
/// a key no user has, and runs none whose grant doesn't verify: a's signed job passed off as c's,
/// and a job the user signed for c with another command granted. Each refused one is claimed once.
/// One the user signed for c, granted as signed, runs.
#[test]
fn a_forged_or_unsigned_job_from_another_owner_is_never_run() {
    use potluck::jobauth;
    let w = World::new();
    let a = w.node("a").start();
    let b = w.node("b").start();
    let mut c = w.node("c").start();
    pair(&a, &b);
    pair(&a, &c);
    wait_until("b and c see each other", || {
        b.node_alive("c") && c.node_alive("b")
    });
    a.cli_ok(&["user", "init", "--name", "owner"]);
    for n in [&a, &b, &c] {
        wait_until(&format!("{} has the user", n.name), || {
            trust_of(n).users_exist()
        });
    }
    let user = user_key(&a);
    let theirs = a.dispatch(&["--on", "node=b", "--", "true"]);
    assert_eq!(a.done(&theirs).state, JobState::Succeeded);
    let record = serde_json::to_value(a.get(&theirs)).unwrap();
    // What a's command line signed, for c.
    let signed_for_c = potluck::types::JobSpec {
        submitted_from: "c".into(),
        ..jobauth::open(a.get(&theirs).spec.auth.as_ref().unwrap()).unwrap()
    };
    let stranger = tempfile::tempdir().unwrap();
    let other = potluck::mesh::user::UserKey::load_or_create(&stranger.path().join("k"))
        .unwrap()
        .0;
    let held = |spec: &potluck::types::JobSpec| JobRecord {
        // As c would hold it: its ids salted with c's name.
        spec: potluck::types::JobSpec {
            auth: spec.auth.clone(),
            ..jobauth::owned("c", spec, false)
        },
        ..serde_json::from_value(record.clone()).unwrap()
    };
    let unsigned = held(&signed_for_c);
    let by_stranger = held(&jobauth::sign(&signed_for_c, &other, now_ms()));
    // a's job as a holds it, signed for a.
    let replayed: JobRecord = serde_json::from_value(record.clone()).unwrap();
    let mut changed = held(&jobauth::sign(&signed_for_c, &user, now_ms()));
    changed.spec.cmd = vec!["sh".into(), "-c".into(), "echo forged".into()];
    let fine = held(&jobauth::sign(&signed_for_c, &user, now_ms()));
    let mut jobs = BTreeMap::new();
    let mut queue = Vec::new();
    for (id, mut rec) in [
        ("c-901", unsigned),
        ("c-902", by_stranger),
        ("c-903", replayed),
        ("c-904", changed),
        ("c-905", fine),
    ] {
        rec.id = id.into();
        rec.state = JobState::Queued;
        (rec.node, rec.lease, rec.workspace) = (None, None, None);
        (rec.started_at, rec.ended_at, rec.exit_code) = (None, None, None);
        rec.spec.key = None;
        let v = serde_json::to_value(&rec).unwrap();
        jobs.insert(id.to_string(), v.clone());
        queue.push(v);
    }
    let mut report = report_to(&b, "c");
    report["queue"] = Value::Array(queue);
    report["seq"] = 0.into();
    let forger = Arc::new(Forger {
        report,
        jobs,
        claims: Mutex::default(),
    });
    let mesh = c.impostor(forger.clone());
    wait_until("b runs c-905", || b.exec.launch_of("c-905").is_some());
    wait_until("b claimed c-903 and c-904", || {
        forger.claims("b", "c-903") >= 1 && forger.claims("b", "c-904") >= 1
    });
    // Refused once, never claimed again.
    std::thread::sleep(Duration::from_secs(2));
    let log = b.daemon_log();
    for (id, claims) in [
        ("c-901", 0),
        ("c-902", 0),
        ("c-903", 1),
        ("c-904", 1),
        ("c-905", 1),
    ] {
        assert_eq!(forger.claims("b", id), claims, "{id}\n{log}");
        assert_eq!(forger.claims("a", id), 0, "{id}");
        if id != "c-905" {
            assert!(b.exec.launch_of(id).is_none(), "{id}\n{log}");
        }
    }
    assert!(
        log.contains(
            "WARNING: c-903: granted by c, but not run here: it was signed for \"a\", not for c, which granted it"
        ),
        "{log}"
    );
    assert!(
        log.contains(
            "WARNING: c-904: granted by c, but not run here: its cmd is not what its signature covers"
        ),
        "{log}"
    );
    assert_eq!(b.get("c-905").spec.cmd, ["true"]);
    mesh.stop();
}
