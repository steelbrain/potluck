//! `potluck daemon`: the machine's scheduler, job runner and workspace keeper. One per user per
//! machine. Phase 1 is one machine; phase 2 adds the mesh, the global queue and claims.
//!
//! - Startup: take `daemon.lock` in the home (one daemon per home; clients that find no socket
//!   while it is held wait for one); [`crate::preflight`] checks (linger on Linux, CoW work dir);
//!   capture the login environment; detect facts; open the store; adopt invocations still
//!   running from their handles (the unit, or the supervisor's pid and start time); end those
//!   whose processes are gone from their status file, or as lost. Jobs caught while their
//!   workspace was being seeded or verified start again in it, with the lease they hold (a
//!   workspace seeded for them is seeded afresh), unless they had been cancelled or their start
//!   had failed (then they end so). Pins that no job or workspace holds any more (a
//!   daemon killed between pinning and recording, or between a job's end and its unpinning) are
//!   dropped.
//! - Serves `potluck.sock` (mode 0600 in a 0700 home), one thread per connection, closed after
//!   10 minutes without a request or without taking any of an answer; state behind a mutex,
//!   written through to the store on every change. Every response carries the caller's reminders.
//! - Dispatch: idempotent by key; refuses specs that no machine could ever run (facts, RAM) and
//!   priority outside 1-5; syncs the mirror from the source and pins the commits; queues.
//! - A loop every half second (and on each dispatch, cancel or exit):
//!   1. sample running invocations; end those whose status file exists (killing leftovers), those
//!      over memory, and those past their run limit (SIGTERM, SIGKILL after the grace period; stuck
//!      processes don't keep the lease: it is released and the pids recorded);
//!   2. stall check: under 1% of one core and no output for the stall time (time asleep not
//!      counted): sample stacks into the log, mark stalled, end it if the spec asked;
//!   3. finished invocations: copy `keep` files into the job's directory (its log is written
//!      there as it runs), release the lease, set the workspace's hold;
//!   4. [`crate::sched::plan`]; add passes; for each start create or reuse the workspace
//!      (seeding), verify it, build the environment, start through the executor;
//!   5. [`crate::workspace::tick`]; and every 10 minutes job retention, workspace sizes, and
//!      retries: workspaces marked removed whose files are still there (the removal failed, or a
//!      restart cut it short) are removed again, and executor cleanups that failed (a Linux unit
//!      left loaded) are tried again, up to 6 times.
//! - Restart in place ([`handover`]): on `potluck restart`, or once an update has replaced the
//!   binary, the daemon drains, hands its socket, the mesh's listener, its open connections and
//!   the mesh's view of the members to the binary at its path, and execs it in its own process.
//!
//! Time: records carry wall time, but run limits, the grace before SIGKILL and the loop's periodic
//! work are measured on the boot clock ([`clock`]), so a step of the wall clock moves none of them,
//! and a daemon restarted in the same boot goes on from the readings its run records keep. Stall
//! windows are measured on the awake clock, which stops while the machine sleeps. Holds stay on
//! wall time.
//!
//! The core runs with injected [`Parts`] (executor, workspaces, scheduler, facts, login
//! environment, clocks), so it is tested with fakes; [`run`] wires in the real ones.
//!
//! In a cluster ([`Daemon::start_cluster`]), the mesh and the cluster's loop run beside this:
//! see [`cluster`] for the global queue, claims, jobs run for other machines and remote commands.
//! The mesh listens on its port only while this machine is in a cluster (from the founder's first
//! `admit`, from a new member's `pair --finish`, until it is removed); a daemon on a machine in no
//! cluster opens no port.
//!
//! Pins: a dispatch pins its commits (the repository's and each submodule's) under the job id
//! until the job ends; the workspace module pins them under the workspace id when it seeds a
//! workspace and drops those pins when it removes it.
//!
//! A job's environment: the login environment, then the `[[gpu]]` env of each GPU it holds, then
//! the spec's env, less the spec's `unset_env`, then potluck's own, which nothing overrides or
//! unsets: POTLUCK_JOB, POTLUCK_LEASE, POTLUCK_WORKSPACE, POTLUCK_NODE, POTLUCK_GPUS, TMPDIR
//! (beside the workspace), POTLUCK_HOME and POTLUCK_CONFIG (so a potluck call inside the job
//! reaches this daemon).
//!
//! The names of potluck's own variables are [`JOB_VARIABLES`].
//!
//! A job's directory ([`Paths::job_dir`]) holds `output.log` (stdout and stderr, copied there by
//! the supervisor, up to the config's `log_max_mib` and the last 16 MiB after it), `status.json`
//! (written by the supervisor), `keep/` (files matched by `--keep`, by their path in the
//! workspace) and `crash/` (crash reports of its processes).

pub mod clock;
pub mod cluster;
mod files;
pub mod handover;
mod helpers;
mod parts;
mod runner;
mod server;
mod state;

pub use clock::{Clock, SystemClock};
pub use cluster::{ClusterOptions, GitTunnel, Tunnel, Updater, real_updater};
pub use parts::{Parts, RealScheduler, Scheduler, WorkspaceProvider};

use crate::config::{NodeConfig, Paths};
use crate::store::Store;
use crate::types::{MINUTE_MS, now_ms};
use state::State;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::JoinHandle;

/// The daemon's tunables. [`Options::from_config`] reads them from the node config; tests shorten
/// them.
#[derive(Clone, Debug)]
pub struct Options {
    /// The `potluck` binary the daemon runs its helpers from: `_supervise` for each invocation,
    /// `_tunnel` for git. Once an update replaces it, a copy of the file it was when the daemon
    /// started ([`helpers`]).
    pub potluck_bin: PathBuf,
    /// The loop's period.
    pub tick_ms: u64,
    /// SIGTERM to SIGKILL; also how long after SIGKILL processes count as stuck.
    pub kill_grace_ms: u64,
    /// Under 1% of one core and no output this long is a stall. 0 turns stall checks off.
    pub stall_ms: u64,
    /// Finished jobs, their logs and kept files are kept this long.
    pub retention_ms: u64,
    /// How often facts are detected again.
    pub facts_refresh_ms: u64,
    /// A client connection that sends no request, or takes no bytes of an answer, for this long
    /// is closed, and its thread ends. 0: never.
    pub idle_ms: u64,
}

impl Options {
    pub fn from_config(config: &NodeConfig) -> anyhow::Result<Options> {
        Ok(Options {
            potluck_bin: crate::update::running_binary()
                .map_err(|e| anyhow::anyhow!("can't find the potluck binary: {e:#}"))?,
            tick_ms: 500,
            kill_grace_ms: config.kill_grace_ms(),
            stall_ms: config.stall_ms(),
            retention_ms: config.retention_ms(),
            facts_refresh_ms: MINUTE_MS,
            idle_ms: CLIENT_IDLE_MS,
        })
    }
}

/// How long a client connection may sit idle ([`Options::idle_ms`]). Clients ask well within it:
/// `wait` and `logs -f` long-poll for 20 s at a time, and connect again if they were let go.
const CLIENT_IDLE_MS: u64 = 10 * MINUTE_MS;

/// `daemon.log` starts afresh once it would pass this size: its lines move to `daemon.log.1`,
/// replacing the lines before them, so the two take at most about twice this.
const DAEMON_LOG_MAX: u64 = 16 << 20;

/// Appends timestamped lines to `daemon.log`, and to stderr unless stderr is that same file (as
/// under launchd, which sends the daemon's stdout and stderr to the daemon log). Past
/// [`DAEMON_LOG_MAX`] the log moves to `daemon.log.1` and a new one starts (stdout and stderr
/// follow it when they were the log). A log that can't be opened is said on stderr and tried
/// again at each line.
#[derive(Clone)]
pub(crate) struct Logger {
    file: Arc<Mutex<LogFile>>,
    to_stderr: bool,
}

struct LogFile {
    path: PathBuf,
    file: Option<File>,
    /// Its length, as far as this daemon wrote it.
    len: u64,
    max: u64,
    /// The standard descriptors open on the log (launchd's stdout and stderr).
    std_fds: Vec<std::os::fd::RawFd>,
}

impl Logger {
    fn open(path: &std::path::Path) -> Logger {
        Logger::open_max(path, DAEMON_LOG_MAX)
    }

    fn open_max(path: &std::path::Path, max: u64) -> Logger {
        let file = match open_append(path) {
            Ok(f) => Some(f),
            Err(e) => {
                // Said where the lines go meanwhile (a service's journal, the terminal).
                let _ = writeln!(
                    std::io::stderr(),
                    "potluck: can't open {}: {e}; the daemon's log lines go to stderr until it can",
                    path.display()
                );
                None
            }
        };
        let std_fds: Vec<std::os::fd::RawFd> = [libc::STDOUT_FILENO, libc::STDERR_FILENO]
            .into_iter()
            .filter(|&fd| file.as_ref().is_some_and(|f| same_file(f, fd)))
            .collect();
        let to_stderr = !std_fds.contains(&libc::STDERR_FILENO);
        Logger {
            file: Arc::new(Mutex::new(LogFile {
                path: path.to_path_buf(),
                len: file
                    .as_ref()
                    .and_then(|f| f.metadata().ok())
                    .map_or(0, |m| m.len()),
                file,
                max,
                std_fds,
            })),
            to_stderr,
        }
    }

    pub fn log(&self, msg: impl AsRef<str>) {
        let time = crate::types::fmt_local_time(now_ms());
        let line = format!("{time} {}\n", msg.as_ref());
        let mut f = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        if f.file.is_none() {
            // It couldn't be opened when the daemon started (or since): try again.
            f.file = open_append(&f.path).ok();
            f.len = f
                .file
                .as_ref()
                .and_then(|f| f.metadata().ok())
                .map_or(0, |m| m.len());
        }
        if f.len > 0 && f.len.saturating_add(line.len() as u64) > f.max {
            f.start_afresh(&time);
        }
        if let Some(file) = f.file.as_mut() {
            let _ = file.write_all(line.as_bytes());
            f.len += line.len() as u64;
        }
        if self.to_stderr {
            let _ = std::io::stderr().write_all(line.as_bytes());
        }
    }
}

impl LogFile {
    /// Moves the log to `<name>.1` and starts a new one. If it can't be moved, the log goes on
    /// (it is tried again after another `max` bytes).
    fn start_afresh(&mut self, time: &str) {
        self.len = 0;
        let mut old = self.path.clone().into_os_string();
        old.push(".1");
        let old = PathBuf::from(old);
        if std::fs::rename(&self.path, &old).is_err() {
            return;
        }
        let Ok(new) = open_append(&self.path) else {
            return;
        };
        for &fd in &self.std_fds {
            // SAFETY: both descriptors are open; dup2 replaces `fd` with a copy of the new log's.
            unsafe { libc::dup2(new.as_raw_fd(), fd) };
        }
        let name = old.file_name().map(|n| n.to_string_lossy().into_owned());
        let line = format!(
            "{time} the lines before this are in {}\n",
            name.unwrap_or_default()
        );
        let mut new = new;
        if new.write_all(line.as_bytes()).is_ok() {
            self.len = line.len() as u64;
        }
        self.file = Some(new);
    }
}

fn open_append(path: &std::path::Path) -> std::io::Result<File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

/// Whether descriptor `fd` is open on the same file as `f`.
fn same_file(f: &File, fd: std::os::fd::RawFd) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(meta) = f.metadata() else {
        return false;
    };
    // SAFETY: fstat fills `st`; an fd that isn't open only makes it fail.
    #[allow(clippy::unnecessary_cast)] // dev_t is i32 on macOS, u64 on Linux
    unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        libc::fstat(fd, &mut st) == 0
            && st.st_dev as u64 == meta.dev()
            && st.st_ino as u64 == meta.ino()
    }
}

/// What every thread of the daemon shares.
pub(crate) struct Shared {
    pub paths: Paths,
    pub config: NodeConfig,
    pub opts: Options,
    pub parts: Parts,
    pub log: Logger,
    /// Where invocations' supervisors and git's tunnels run from.
    pub(crate) helpers: helpers::Helpers,
    pub state: Mutex<State>,
    /// Notified whenever a job changes state (long polls wait on it).
    pub changed: Condvar,
    kick: Mutex<bool>,
    kick_cv: Condvar,
    stopping: AtomicBool,
    /// A restart in place: asked, draining, handing over ([`handover`]).
    pub(crate) up: handover::Upgrade,
    /// The login environment jobs start from: the one captured at start, or handed over and
    /// captured again since.
    login_env: std::sync::RwLock<BTreeMap<String, String>>,
    /// This daemon started from a handover: its facts and login environment are detected again
    /// in the background.
    from_handover: bool,
    /// Children of this process an earlier daemon in it left running: reaped as they end.
    orphans: Mutex<Vec<i32>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) conns: Mutex<Vec<(UnixStream, JoinHandle<()>)>>,
    /// One mirror sync at a time per repository.
    sync_locks: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    /// Ended jobs whose crash reports are still being collected (a few seconds after a crash);
    /// a job run for another machine sends them to its owner once they are in.
    pub(crate) crash_pending: Mutex<std::collections::BTreeSet<String>>,
    /// The cluster, when the daemon runs in one.
    pub(crate) cluster: OnceLock<Arc<cluster::Cluster>>,
}

impl Shared {
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wakes the loop now.
    pub fn kick(&self) {
        *self.kick.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.kick_cv.notify_all();
    }

    /// Wakes long polls.
    pub fn notify(&self) {
        self.changed.notify_all();
    }

    pub fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Waits for a kick or `ms`, whichever comes first.
    fn wait_kick(&self, ms: u64) {
        let mut k = self.kick.lock().unwrap_or_else(PoisonError::into_inner);
        if !*k {
            k = self
                .kick_cv
                .wait_timeout(k, std::time::Duration::from_millis(ms))
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        *k = false;
    }

    /// Runs `f` on a thread of its own; [`Daemon::stop`] waits for it. While the daemon drains
    /// for a restart in place, long work ([`drain_refuses`]) isn't started: the next daemon does
    /// it (as after any restart), and once it hands over nothing is. Returns whether `f` was
    /// started.
    pub fn spawn_worker(
        self: &Arc<Self>,
        name: &str,
        f: impl FnOnce(&Arc<Shared>) + Send + 'static,
    ) -> bool {
        if self.up.quiescing() || (self.up.draining() && drain_refuses(name)) {
            return false;
        }
        let sh = self.clone();
        let log = self.log.clone();
        let name = name.to_string();
        let thread_name = name.clone();
        let spawned = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                if let Err(p) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&sh))) {
                    log.log(format!("{thread_name}: panicked: {}", panic_text(&p)));
                }
            });
        match spawned {
            Ok(h) => {
                let mut w = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
                w.retain(|h| !h.is_finished());
                w.push(h);
                true
            }
            Err(e) => {
                self.log
                    .log(format!("{name}: could not start a thread: {e}"));
                false
            }
        }
    }

    /// [`Shared::spawn_worker`] for periodic work: not while a worker of that name still runs (a
    /// refresh that hangs, or runs longer than its period, isn't started again beside itself).
    pub fn spawn_worker_once(
        self: &Arc<Self>,
        name: &str,
        f: impl FnOnce(&Arc<Shared>) + Send + 'static,
    ) {
        let running = self
            .workers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|h| !h.is_finished() && h.thread().name() == Some(name));
        if !running {
            self.spawn_worker(name, f);
        }
    }

    /// After an update replaced the potluck binary: [`run`] restarts the daemon in place on it
    /// ([`handover`]). Daemons not started by [`run`] (tests) are only asked
    /// ([`Daemon::wait_restart`]).
    pub fn restart_here(&self) {
        self.up.ask("the potluck binary was updated");
    }

    /// The login environment jobs start from.
    pub fn login_env(&self) -> BTreeMap<String, String> {
        self.login_env
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Reaps the children an earlier daemon in this process left running, those that ended.
    pub(crate) fn reap_orphans(&self) {
        let mut o = self.orphans.lock().unwrap_or_else(PoisonError::into_inner);
        if !o.is_empty() {
            *o = handover::reap(&o);
        }
    }

    /// Whether a worker that isn't long-lived (a courier lives as long as its job) still runs.
    fn busy(&self) -> bool {
        self.workers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|h| !h.is_finished() && !h.thread().name().is_some_and(long_lived))
    }

    /// The cluster, when the daemon runs in one.
    pub(crate) fn cluster(&self) -> Option<Arc<cluster::Cluster>> {
        self.cluster.get().cloned()
    }

    /// Whether job `id`'s crash reports are still being collected.
    pub fn crash_pending(&self, id: &str) -> bool {
        self.crash_pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(id)
    }

    /// The lock serializing syncs of one repository's mirror.
    pub fn sync_lock(&self, repo_id: &str) -> Arc<Mutex<()>> {
        self.sync_locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(repo_id.to_string())
            .or_default()
            .clone()
    }
}

/// Work not started while the daemon drains for a restart in place: mirror syncs, workspace
/// removals, periodic upkeep, facts and updates. Job starts and claims aren't listed: only the
/// loop's plan makes them, and none is made while the daemon drains; those of a plan made as the
/// drain began are work already going, which the drain waits for ([`handover::Upgrade::planning`]).
/// Refused, they left the job starting with nothing starting it.
fn drain_refuses(name: &str) -> bool {
    const LONG: [&str; 7] = [
        "sync-",
        "remove-",
        "cleanups",
        "sizes",
        "facts",
        "update",
        "self-update",
    ];
    LONG.iter().any(|p| name.starts_with(p))
}

/// Workers that live as long as a job runs (a runner's courier to its owner): the drain doesn't
/// wait for them; they stop with the daemon and the next one starts them again.
fn long_lived(name: &str) -> bool {
    name.starts_with("courier-")
}

pub(crate) fn panic_text(p: &Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".into())
}

/// A running daemon: its socket, its loop and its workers.
pub struct Daemon {
    shared: Arc<Shared>,
    accept: Option<JoinHandle<()>>,
    ticker: Option<JoinHandle<()>>,
    /// The socket's listener (the accept loop has a copy), kept for a handover.
    socket: Option<UnixListener>,
    /// `daemon.lock`, held while this daemon serves its home.
    lock: Option<File>,
}

impl Daemon {
    /// Starts serving `paths`' home: opens the store, adopts running invocations, binds the
    /// socket and starts the loop. Returns once the socket accepts connections.
    pub fn start(
        paths: &Paths,
        config: &NodeConfig,
        parts: Parts,
        opts: Options,
    ) -> anyhow::Result<Daemon> {
        Daemon::start_inner(paths, config, parts, opts, None, None, None)
    }

    /// [`Daemon::start`] as a member of a cluster: also starts the mesh (before the loop, so the
    /// loop plans with the other members) and the cluster's own loop.
    pub fn start_cluster(
        paths: &Paths,
        config: &NodeConfig,
        parts: Parts,
        opts: Options,
        cluster: ClusterOptions,
    ) -> anyhow::Result<Daemon> {
        Daemon::start_inner(paths, config, parts, opts, Some(cluster), None, None)
    }

    /// [`Daemon::start`] (`cluster`: [`Daemon::start_cluster`]) from what an earlier daemon in
    /// this process handed over ([`Daemon::hand_over`], then an exec): its lock, socket and
    /// connections, facts, mesh listener and members. Nothing is bound again; connections
    /// waiting in the backlog are taken from here on.
    pub fn start_from(
        paths: &Paths,
        config: &NodeConfig,
        parts: Parts,
        opts: Options,
        cluster: Option<ClusterOptions>,
        inherited: handover::Inherited,
    ) -> anyhow::Result<Daemon> {
        Daemon::start_inner(paths, config, parts, opts, cluster, Some(inherited), None)
    }

    /// [`Daemon::start_cluster`] on a home `lock`ed already ([`lock_home`]).
    fn start_cluster_locked(
        paths: &Paths,
        config: &NodeConfig,
        parts: Parts,
        opts: Options,
        cluster: ClusterOptions,
        lock: File,
    ) -> anyhow::Result<Daemon> {
        Daemon::start_inner(paths, config, parts, opts, Some(cluster), None, Some(lock))
    }

    fn start_inner(
        paths: &Paths,
        config: &NodeConfig,
        parts: Parts,
        opts: Options,
        cluster: Option<ClusterOptions>,
        inherited: Option<handover::Inherited>,
        locked: Option<File>,
    ) -> anyhow::Result<Daemon> {
        let locked = match (&inherited, locked) {
            (None, None) => Some(lock_home(paths)?),
            (_, locked) => {
                prepare_home(paths)?;
                locked
            }
        };
        let from_handover = inherited.is_some();
        let (lock, socket, mesh_listener, conns, facts, members, orphans, came, unwritten) =
            match inherited {
                Some(i) => (
                    i.lock,
                    Some(i.socket),
                    i.mesh,
                    i.conns,
                    Some(i.facts),
                    i.members,
                    i.children,
                    Some(handover::Came {
                        from_version: i.from_version,
                        quiesced_at: i.quiesced_at,
                        running_jobs: i.running_jobs,
                        drained_ms: i.drained_ms,
                        failed: i.failed,
                        waits: i.waits,
                    }),
                    i.unwritten,
                ),
                None => (
                    locked.expect("taken above"),
                    None,
                    None,
                    Vec::new(),
                    None,
                    Vec::new(),
                    Vec::new(),
                    None,
                    Default::default(),
                ),
            };
        let log = Logger::open(&paths.daemon_log());
        let helpers = helpers::Helpers::new(&opts.potluck_bin, paths.home.join("bin"), log.clone());
        let store = Store::open(&paths.db())?;
        let facts = match facts {
            Some(f) => f,
            None => (parts.facts)()?,
        };
        let state = runner::restore(
            config,
            store,
            facts,
            log.clone(),
            &*parts.executor,
            parts.clock.clone(),
            paths,
            unwritten,
        )?;
        let node = state.node.clone();
        let listener = match socket {
            Some(l) => l,
            None => bind(paths)?,
        };
        let login_env = parts.login_env.clone();
        let shared = Arc::new(Shared {
            paths: paths.clone(),
            config: config.clone(),
            opts,
            parts,
            log,
            helpers,
            state: Mutex::new(state),
            changed: Condvar::new(),
            kick: Mutex::new(false),
            kick_cv: Condvar::new(),
            stopping: AtomicBool::new(false),
            up: handover::Upgrade::new()?,
            login_env: std::sync::RwLock::new(login_env),
            from_handover,
            orphans: Mutex::new(orphans),
            workers: Mutex::new(Vec::new()),
            conns: Mutex::new(Vec::new()),
            sync_locks: Mutex::new(BTreeMap::new()),
            crash_pending: Mutex::new(Default::default()),
            cluster: OnceLock::new(),
        });
        if let Some(opts) = cluster {
            cluster::init(&shared, opts, mesh_listener, members)?;
        }
        runner::sweep_pins(&shared);
        runner::adopt(&shared);
        let sh = shared.clone();
        let ticker = std::thread::Builder::new()
            .name("potluck-loop".into())
            .spawn(move || runner::ticker(sh))?;
        let mut daemon = Daemon {
            shared,
            accept: None,
            ticker: Some(ticker),
            socket: Some(listener),
            lock: Some(lock),
        };
        // The mesh before the first request: a status answered from now on lists the members as
        // the mesh sees them (after a restart in place, as the one before saw them).
        cluster::start(&daemon.shared)?;
        let shared = daemon.shared.clone();
        // Logged before the first request can be answered: a client that has had a reply knows
        // this line is in the log.
        shared.log.log(format!(
            "potluck {} serving {} as {node}{}",
            crate::VERSION,
            paths.socket().display(),
            match &came {
                Some(c) if c.waits.is_some() => format!(
                    ", again after its restart didn't go on, with {} kept",
                    handover::counted(conns.len(), "connection", "connections")
                ),
                Some(c) if c.failed.is_some() => format!(
                    ", again after its restart failed, with {} kept",
                    handover::counted(conns.len(), "connection", "connections")
                ),
                Some(c) => format!(
                    ", restarted in place from potluck {} with {} handed over",
                    c.from_version,
                    handover::counted(conns.len(), "connection", "connections")
                ),
                None => String::new(),
            }
        ));
        // On macOS, an unsigned binary (built for an Intel Mac) can't reach members at all.
        if let Some(why) = crate::service::this_executable()
            .ok()
            .and_then(|exe| crate::service::signature_problem(&exe))
        {
            shared.log.log(format!("warning: {why}"));
        }
        let sh = shared.clone();
        let accepting = daemon.socket.as_ref().expect("set above").try_clone()?;
        daemon.accept = Some(
            std::thread::Builder::new()
                .name("potluck-accept".into())
                .spawn(move || server::accept_loop(sh, accepting))?,
        );
        for (conn, asked) in conns {
            let behind = asked
                .as_ref()
                .map(|a| a.sent_behind.clone())
                .unwrap_or_default();
            let first = asked.zip(came.as_ref()).map(|(a, c)| c.answer(&a, &shared));
            server::spawn_conn(&shared, conn, first, behind);
        }
        Ok(daemon)
    }

    /// Where the mesh listens, in a cluster.
    pub fn cluster_addr(&self) -> Option<std::net::SocketAddr> {
        self.shared.cluster().and_then(|c| c.mesh().local_addr())
    }

    /// This machine's name.
    pub fn node(&self) -> String {
        self.shared.lock().node.clone()
    }

    /// Stops serving and lets go of the home once every worker is done (another daemon may start
    /// on the home in this process). Running invocations keep running; the next daemon on this
    /// home adopts them.
    pub fn stop(mut self) {
        self.shutdown(None);
    }

    /// Waits until a restart in place is asked (`Shared::restart_here`, a `restart` request),
    /// and says why; None once the daemon stops. [`run`] waits so; tests that restart a daemon
    /// in their own process do too, then [`Daemon::hand_over`] and [`Daemon::start_from`].
    pub fn wait_restart(&self) -> Option<String> {
        let sh = &self.shared;
        sh.up.wait_asked(&|| sh.stopping())
    }

    /// The verdict on the binary a restart asked for ([`Daemon::wait_restart`]): those who asked
    /// get it (`Err`: why it isn't restarting).
    pub fn restart_verdict(&self, v: Result<(), String>) {
        self.shared.up.verdict(v);
    }

    /// Drains for a restart in place ([`handover`]): no job starts and no long work begins;
    /// work going gets up to `max` to finish. Then what the daemon holds must all be written
    /// down: a write that failed since the restart was asked (a full disk) would be lost with the
    /// handover, so the drain is undone, the daemon serves on, and the reason is returned. Ok
    /// with how long the drain took, in ms.
    pub fn drain(&self, max: std::time::Duration) -> Result<u64, String> {
        let sh = &self.shared;
        let began = std::time::Instant::now();
        sh.up.set_phase_draining();
        sh.kick();
        while began.elapsed() < max && (sh.busy() || sh.up.relays() || sh.up.plans()) {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let ms = began.elapsed().as_millis() as u64;
        if let Some(why) = handover::unsaved(sh) {
            sh.up.set_phase_serving();
            sh.kick();
            return Err(why);
        }
        sh.up.set_drained_ms(ms);
        Ok(ms)
    }

    /// Hands this daemon over for a restart in place ([`handover`]): drains (no new work; work
    /// going gets up to `drain`), then quiesces (connections wait in the backlog, long polls
    /// answer, requests in flight finish), stops the cluster, the loop and the workers, and
    /// returns what the next daemon needs. Running invocations keep running. The store stays open
    /// until the caller closes it. The process's child processes are left alone: [`run`] ends
    /// them before its exec (a test running daemons in its own process has children of its own).
    pub fn hand_over(self, drain: std::time::Duration) -> handover::Inherited {
        self.hand_over_as(drain, "in place")
    }

    /// [`Daemon::hand_over`] for a restart `how` ("in place", "plainly"), as its log says.
    pub(crate) fn hand_over_as(
        mut self,
        drain: std::time::Duration,
        how: &str,
    ) -> handover::Inherited {
        use std::time::{Duration, Instant};
        let sh = self.shared.clone();
        let asked_at = sh.up.asked_at();
        // Drain.
        let began = Instant::now();
        sh.up.set_phase_draining();
        sh.kick();
        while began.elapsed() < drain && (sh.busy() || sh.up.relays() || sh.up.plans()) {
            std::thread::sleep(Duration::from_millis(20));
        }
        // With what a drain before took ([`Daemon::drain`]).
        let drained_ms = sh.up.drained_ms() + began.elapsed().as_millis() as u64;
        if sh.busy() || sh.up.relays() {
            sh.log.log(format!(
                "restarting {how}: work still going after {}s is cut short and done again \
                 after the restart",
                drained_ms / 1000
            ));
        }
        // Quiesce: nothing takes connections off the socket from here on.
        sh.up.set_phase_quiescing();
        let quiesced_at = clock::boot_ms();
        sh.up.wake();
        if let Some(c) = sh.cluster() {
            c.stop_accepting();
        }
        sh.notify();
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
        let until = Instant::now() + handover::QUIESCE_MAX;
        let conns_left = |sh: &Shared| {
            sh.conns
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .any(|(_, h)| !h.is_finished())
        };
        while Instant::now() < until && (conns_left(&sh) || sh.up.answering()) {
            sh.notify();
            std::thread::sleep(Duration::from_millis(5));
        }
        // The cluster: member connections close, the listener stays open.
        let (mesh, members) = match sh.cluster() {
            Some(c) => c.hand_over(),
            None => (None, Vec::new()),
        };
        // The loop and the workers.
        sh.stopping.store(true, Ordering::SeqCst);
        sh.kick();
        sh.notify();
        // A tick going on gets a while to end. One held up outside the state lock (a signal
        // through a busy systemd) is left behind: once the store closes it can't write, the state
        // being locked until the exec.
        let loop_left = self.ticker.take().and_then(|h| {
            let until = Instant::now() + handover::LOOP_WAIT;
            while !h.is_finished() && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(10));
            }
            if h.is_finished() {
                let _ = h.join();
                return None;
            }
            h.thread().name().map(str::to_string)
        });
        let wait_workers = |d: Duration| {
            let until = Instant::now() + d;
            while Instant::now() < until && sh.busy() {
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        wait_workers(Duration::from_secs(1));
        let running_jobs = {
            // The loop writes a running job's usage and handle every few seconds: the latest go
            // now, for the next daemon.
            let mut st = sh.lock();
            let running: Vec<String> = st
                .runs
                .iter()
                .filter(|(_, r)| r.phase == state::Phase::Running)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &running {
                st.save_job(id);
            }
            running.len()
        };
        let conns = sh.up.take_handed();
        // Requests still going are cut off: their connections close now, so no answer reaches
        // the client from this daemon if the restart doesn't go on (then the daemon started
        // again in this process doesn't have what such a request did after its store closed).
        // Not one whose thread handed it over and hasn't ended yet.
        let handed: Vec<_> = conns
            .iter()
            .filter_map(|(s, _)| handover::socket_id(s.as_raw_fd()))
            .collect();
        let cut = sh
            .conns
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .filter(|(stream, h)| {
                let going = !h.is_finished()
                    && !handover::socket_id(stream.as_raw_fd())
                        .is_some_and(|id| handed.contains(&id));
                if going {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
                going
            })
            .count();
        let unfinished: Vec<String> = sh
            .workers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|h| !h.is_finished())
            .filter_map(|h| h.thread().name().map(str::to_string))
            .chain(loop_left)
            .collect();
        sh.log.log(format!(
            "restarting {how}: {} {} and {} (drained in {drained_ms} ms){}{}",
            // A plain restart's exec closes them.
            if how == "plainly" {
                "stopped, with"
            } else {
                "handing over"
            },
            handover::counted(conns.len(), "connection", "connections"),
            handover::counted(running_jobs, "running job", "running jobs"),
            if cut > 0 {
                format!(
                    "; {} still going cut off",
                    handover::counted(cut, "request", "requests")
                )
            } else {
                String::new()
            },
            if unfinished.is_empty() {
                String::new()
            } else {
                format!("; not waiting for {}", unfinished.join(", "))
            }
        ));
        let facts = sh.lock().local().facts.clone();
        handover::Inherited {
            lock: self.lock.take().expect("a running daemon holds its lock"),
            socket: self.socket.take().expect("a running daemon has its socket"),
            mesh,
            conns,
            facts,
            login_env: sh.login_env(),
            members,
            // The process's children are ended before the exec ([`run`]).
            children: Vec::new(),
            from_version: crate::VERSION.to_string(),
            asked_at,
            quiesced_at,
            running_jobs,
            drained_ms,
            failed: None,
            waits: None,
            unwritten: Default::default(),
        }
    }

    /// Stops; waits for the workers until `until`, or for all of them.
    fn shutdown(&mut self, until: Option<std::time::Instant>) {
        let sh = &self.shared;
        sh.stopping.store(true, Ordering::SeqCst);
        // The mesh first: the other members see this machine go, and streams and calls in
        // progress end, so the threads below can be joined.
        if let Some(c) = sh.cluster() {
            c.stop();
        }
        sh.kick();
        sh.notify();
        // Wake the accept loop and the connections waiting for a request.
        sh.up.wake();
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
        if let Some(h) = self.ticker.take() {
            let _ = h.join();
        }
        {
            // The loop writes a running job's usage and handle every few seconds, not at every
            // sample: the latest go now, for the next daemon.
            let mut st = sh.lock();
            let running: Vec<String> = st
                .runs
                .iter()
                .filter(|(_, r)| r.phase == state::Phase::Running)
                .map(|(id, _)| id.clone())
                .collect();
            for id in running {
                st.save_job(&id);
            }
        }
        let conns = std::mem::take(&mut *sh.conns.lock().unwrap_or_else(PoisonError::into_inner));
        for (stream, _) in &conns {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
        for (_, h) in conns {
            let _ = h.join();
        }
        let mut left = Vec::new();
        loop {
            let workers =
                std::mem::take(&mut *sh.workers.lock().unwrap_or_else(PoisonError::into_inner));
            if workers.is_empty() {
                break;
            }
            for h in workers {
                while let Some(t) = until
                    && !h.is_finished()
                    && std::time::Instant::now() < t
                {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                if until.is_none() || h.is_finished() {
                    let _ = h.join();
                } else {
                    left.push(h.thread().name().unwrap_or("?").to_string());
                }
            }
        }
        let _ = std::fs::remove_file(sh.paths.socket());
        if left.is_empty() {
            sh.log.log("stopped");
        } else {
            sh.log.log(format!(
                "stopped, without waiting for {} (the next daemon goes on from what is stored)",
                left.join(", ")
            ));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if self.accept.is_some() || self.ticker.is_some() {
            self.shutdown(None);
        }
    }
}

/// The variables potluck sets in every job's environment, over anything the machine or the
/// dispatch sets (a dispatch from inside a job keeps the job's).
pub const JOB_VARIABLES: [&str; 8] = [
    "POTLUCK_JOB",
    "POTLUCK_LEASE",
    "POTLUCK_WORKSPACE",
    "POTLUCK_NODE",
    "POTLUCK_GPUS",
    "POTLUCK_HOME",
    "POTLUCK_CONFIG",
    "TMPDIR",
];

/// How long a starting daemon waits for `daemon.lock`. The previous daemon may be letting go of it
/// right now: exiting during a restart, or (in one process) closed while a child forked by another
/// thread still holds a copy of its descriptor until that child's exec closes it.
const HOME_LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

/// Clears the group and other permission bits on the state directory. It holds job specs with
/// their `--env` values, logs and sources, and may have been created with the umask's mode by
/// something else first (`potluck service install`, an older potluck).
fn make_private(home: &std::path::Path) -> anyhow::Result<()> {
    let mode = std::fs::metadata(home)
        .map_err(|e| anyhow::anyhow!("{}: {e}", home.display()))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        std::fs::set_permissions(home, std::fs::Permissions::from_mode(mode & 0o7700))
            .map_err(|e| anyhow::anyhow!("{}: {e}", home.display()))?;
    }
    Ok(())
}

/// Makes the home (0700) and clears the group and other bits on one made before.
fn prepare_home(paths: &Paths) -> anyhow::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&paths.home)
        .map_err(|e| anyhow::anyhow!("{}: {e}", paths.home.display()))?;
    make_private(&paths.home)
}

/// [`prepare_home`], then [`take_home_lock`]: what a daemon starting without a handover does
/// first, so clients wait for it from then on rather than find no daemon ([`crate::client`]).
fn lock_home(paths: &Paths) -> anyhow::Result<File> {
    prepare_home(paths)?;
    take_home_lock(paths)
}

/// One daemon per home: `daemon.lock`, flocked for the daemon's lifetime.
fn take_home_lock(paths: &Paths) -> anyhow::Result<File> {
    let path = paths.home.join("daemon.lock");
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    let start = std::time::Instant::now();
    // SAFETY: flock on an fd we own.
    while unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        if start.elapsed() >= HOME_LOCK_WAIT {
            anyhow::bail!(
                "another potluck daemon is running for {} (as the service, or a `potluck daemon` \
                 started elsewhere); stop it first, or use it: `potluck status` talks to it",
                paths.home.display()
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    Ok(f)
}

/// Whether a daemon holds `daemon.lock` in `paths`' home now. Only probes the lock: creates
/// nothing, and a daemon starting meanwhile waits [`HOME_LOCK_WAIT`] for it.
pub fn running_here(paths: &Paths) -> bool {
    let Ok(f) = std::fs::OpenOptions::new()
        .write(true)
        .open(paths.home.join("daemon.lock"))
    else {
        return false;
    };
    // SAFETY: flock on an fd we own; dropping it releases a lock taken here.
    unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) != 0 }
}

/// Binds the socket, replacing a stale one, readable and writable by this user only.
fn bind(paths: &Paths) -> anyhow::Result<UnixListener> {
    let sock = paths.socket();
    if sock.exists() {
        if UnixStream::connect(&sock).is_ok() {
            anyhow::bail!("{} is in use by another process", sock.display());
        }
        std::fs::remove_file(&sock).map_err(|e| anyhow::anyhow!("{}: {e}", sock.display()))?;
    }
    let listener =
        UnixListener::bind(&sock).map_err(|e| anyhow::anyhow!("{}: {e}", sock.display()))?;
    std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Raises this process's soft limit on open files to its hard limit (on macOS at most
/// `kern.maxfilesperproc`, the most setrlimit takes there). A service starts with a low one
/// (systemd's user manager gives 1024, launchd 256), and the daemon holds a descriptor for each
/// client and member connection, stream, log and copy. Returns the soft limit before and after.
/// The jobs it starts get the new limits: on macOS they inherit them, on Linux their units are
/// given them.
fn raise_open_files() -> std::io::Result<(u64, u64)> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit fills `lim`.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let before = lim.rlim_cur;
    #[allow(unused_mut)]
    let mut want = lim.rlim_max;
    #[cfg(target_os = "macos")]
    {
        let mut max: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>();
        // SAFETY: kern.maxfilesperproc is an int; `len` says how much room there is.
        let r = unsafe {
            libc::sysctlbyname(
                c"kern.maxfilesperproc".as_ptr(),
                (&raw mut max).cast(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if r == 0 && max > 0 {
            want = want.min(max as libc::rlim_t);
        }
    }
    if want > before {
        lim.rlim_cur = want;
        // SAFETY: setrlimit reads `lim`; the soft limit stays within the hard one.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    #[allow(clippy::useless_conversion)]
    Ok((u64::from(before), u64::from(want.max(before))))
}

/// `potluck daemon`: checks the machine, captures the login environment, detects facts, joins the
/// mesh and serves until the process ends, restarting in place when asked ([`handover`]). A
/// daemon started from a handover (`POTLUCK_HANDOVER`) takes what the one before it in this
/// process handed over, and detects facts and the login environment again in the background.
pub fn run(paths: &Paths, config: &NodeConfig) -> anyhow::Result<()> {
    // First, before any thread: the handover's variable leaves the environment.
    let mut inherited = handover::take()?;
    if let Some(i) = inherited.as_mut() {
        // Every child this process has now was started by the binary before the exec: this one
        // started none yet. That one ended its children before it handed over, but a thread of
        // its work cut short may have started another since (a git fetch tried again once the
        // first was ended), which would run on unknown to this daemon, writing into what it
        // syncs and seeds. They end now, as that binary meant; those that can't are reaped as
        // they end.
        let left = handover::end_children(handover::CHILD_GRACE);
        i.children = handover::reap(&left);
    }
    // Then: an update may replace the binary while the daemon starts.
    helpers::remember_own_binary();
    // Then the home's lock, for a daemon starting without a handover (one started from a
    // handover holds it already): clients connecting from now on wait for this daemon to listen
    // rather than find none, while it captures the login environment and detects facts (seconds).
    let mut locked = match inherited {
        Some(_) => None,
        None => Some(lock_home(paths)?),
    };
    let open_files = raise_open_files();
    config.listen_ip()?;
    let (login_env, left_out) = match &inherited {
        Some(i) => (i.login_env.clone(), Vec::new()),
        None => {
            crate::preflight::check(paths)?;
            crate::envcapture::login_env()?
        }
    };
    let parts = |login_env| {
        let (cfg, at) = (config.clone(), paths.clone());
        Parts {
            executor: Arc::from(crate::exec::platform()),
            workspaces: Arc::new(crate::workspace::Workspaces::new(paths.clone())),
            scheduler: Arc::new(RealScheduler),
            facts: Box::new(move || crate::facts::detect(&at, &cfg)),
            login_env,
            clock: Arc::new(SystemClock),
        }
    };
    let opts = Options::from_config(config)?;
    let cluster = || ClusterOptions::new(paths, config);
    let mut daemon = match inherited {
        Some(mut i) => {
            // Named by its own node.toml, as a plain start is: a name other than the one this
            // machine's records give it is refused there, not taken in half (the facts would
            // say the new name, the jobs and the cluster the old). The version and the GPUs'
            // aliases too, until the facts are detected again.
            crate::facts::as_configured(&mut i.facts, paths, config);
            Daemon::start_from(
                paths,
                config,
                parts(login_env),
                opts.clone(),
                Some(cluster()),
                i,
            )?
        }
        None => Daemon::start_cluster_locked(
            paths,
            config,
            parts(login_env),
            opts.clone(),
            cluster(),
            locked.take().expect("taken above"),
        )?,
    };
    let shared = daemon.shared.clone();
    for v in left_out {
        shared.log.log(format!(
            "login environment: {v}; jobs don't get it (a job's environment holds text only)"
        ));
    }
    match open_files {
        Ok((before, after)) if after > before => shared.log.log(format!(
            "open files: soft limit raised from {before} to {after}"
        )),
        Ok(_) => {}
        Err(e) => shared.log.log(format!(
            "open files: could not raise the soft limit: {e}; connections may fail with \
             \"too many open files\""
        )),
    }
    if shared.from_handover {
        capture_login_env_again(&shared);
    }
    let mut retry = None;
    loop {
        let (inherited, shared, asked, how) = match handover::serve(daemon, retry.take())? {
            handover::Ending::Plain {
                bin,
                inherited,
                shared,
                why,
                told,
            } => (
                exec_plainly(
                    &shared,
                    &bin,
                    inherited,
                    &told,
                    || handover::end_children(handover::CHILD_GRACE),
                    || handover::exec(&bin, None),
                ),
                shared,
                why,
                "plainly",
            ),
            handover::Ending::InPlace {
                bin,
                inherited,
                shared,
                why,
            } => (
                exec_in_place(&shared, &bin, inherited)?,
                shared,
                why,
                "in place",
            ),
        };
        // Still here: the old binary serves on, from what it handed over.
        match (&inherited.waits, &inherited.failed) {
            (Some(waits), _) => {
                shared.log.log(waits);
                retry = Some(asked);
            }
            (None, why) => shared.log.log(format!(
                "restarting {how} failed: {}; this daemon (potluck {}) serves on",
                why.as_deref().unwrap_or("?"),
                crate::VERSION
            )),
        }
        wait_for_workers(&shared, std::time::Duration::from_secs(30));
        drop(shared);
        daemon = Daemon::start_from(
            paths,
            config,
            parts(inherited.login_env.clone()),
            opts.clone(),
            Some(cluster()),
            inherited,
        )?;
    }
}

/// The last steps of a restart in place: closes the store (with the state locked until the exec,
/// so nothing writes after), reaps children that ended, writes the handover file and execs `bin`
/// with it. Returns only if that didn't happen: what was handed over, back, with why
/// ([`handover::Inherited::waits`] when records the daemon held, which it keeps, or the handover
/// file couldn't be written; [`handover::Inherited::failed`] when the exec failed), or an error if
/// even that can't be had: then the process ends, and a service starts the daemon again plainly.
fn exec_in_place(
    sh: &Arc<Shared>,
    bin: &std::path::Path,
    inherited: handover::Inherited,
) -> anyhow::Result<handover::Inherited> {
    let ended = || handover::end_children(handover::CHILD_GRACE);
    let (st, inherited) = match close_for_exec(sh, inherited, ended) {
        Ok(ready) => ready,
        Err(back) => return Ok(*back),
    };
    let path = handover::file_path(&sh.paths.home);
    let file = inherited
        .into_file()
        .map_err(|e| anyhow::anyhow!("restarting in place: handing over descriptors: {e}"))?;
    // A handover file that can't be written (the disk filled since the sample fit) makes the
    // restart wait, as the sample does: tried again every RETRY.
    let (why, waits) = match file.write(&path) {
        Err(e) => (format!("writing {}: {e}", path.display()), true),
        Ok(()) => {
            sh.log
                .log(format!("restarting in place: starting {}", bin.display()));
            let e = handover::exec(bin, Some(&path));
            let _ = std::fs::remove_file(&path);
            (format!("starting {}: {e}", bin.display()), false)
        }
    };
    drop(st);
    let mut back = handover::Inherited::from_file(file)
        .map_err(|e| anyhow::anyhow!("restarting in place failed ({why}), and then: {e:#}"))?;
    // The `potluck restart` that asked is told, by the daemon serving on.
    if waits {
        back.waits = Some(format!(
            "the restart in place waits: {why}; this daemon serves on and tries again every {}s",
            handover::RETRY.as_secs()
        ));
    } else {
        back.failed = Some(why);
    }
    Ok(back)
}

/// The last steps of a plain restart (`bin` can't take a handover): those of
/// [`exec_in_place`] up to the store's closing, then the `potluck restart`s that asked are told
/// `told`, and `bin` is executed without a handover; the connections close with the exec.
/// `end_children`: as for [`cut_short`]; `exec`: [`handover::exec`] of `bin` (returns if it
/// fails).
/// Returns only if that didn't happen, as [`exec_in_place`] does: with
/// [`handover::Inherited::waits`] when records the daemon held couldn't be written (those who
/// asked are told by the daemon serving on), else [`handover::Inherited::failed`] (told here when
/// `bin` can't be executed any more; an exec that fails after all comes after they were told).
fn exec_plainly(
    sh: &Arc<Shared>,
    bin: &std::path::Path,
    inherited: handover::Inherited,
    told: &str,
    end_children: impl FnOnce() -> Vec<i32>,
    exec: impl FnOnce() -> std::io::Error,
) -> handover::Inherited {
    let (st, mut inherited) = match close_for_exec_as(sh, inherited, end_children, "plain restart")
    {
        Ok(ready) => ready,
        Err(back) => return *back,
    };
    let tell = |inherited: &mut handover::Inherited, resp: crate::proto::Response| {
        for (conn, asked) in &mut inherited.conns {
            if asked.take().is_some() {
                let _ = conn.set_write_timeout(Some(std::time::Duration::from_secs(1)));
                let _ = files::reply(&mut &*conn, &[], resp.clone());
            }
        }
    };
    let c = std::ffi::CString::new(bin.as_os_str().as_encoded_bytes()).unwrap_or_default();
    // SAFETY: access(2) reads a valid C string.
    let failed = if unsafe { libc::access(c.as_ptr(), libc::X_OK) } != 0 {
        let failed = format!(
            "starting {}: {}",
            bin.display(),
            std::io::Error::last_os_error()
        );
        tell(
            &mut inherited,
            crate::proto::Response::Error {
                message: format!(
                    "restarting plainly failed: {failed}; this daemon serves on, still potluck {}",
                    crate::VERSION
                ),
            },
        );
        failed
    } else {
        // It goes ahead: those who asked are told now (the exec closes their connections).
        tell(
            &mut inherited,
            crate::proto::Response::Text {
                text: told.to_string(),
            },
        );
        sh.log
            .log(format!("restarting plainly: starting {}", bin.display()));
        let e = exec();
        format!("starting {}: {e}", bin.display())
    };
    drop(st);
    inherited.failed = Some(failed);
    inherited
}

/// Before an exec: locks the state (the caller keeps it locked until the exec), then ends the
/// work cut short (`end_children`: the daemon's child processes, git and tools, or they would
/// run on unknown to the next daemon and git write into what it seeds again; returns those still
/// there). The state first: a thread whose git was ended would otherwise write that its work
/// failed (a job "could not seed workspace": failed), and the next daemon would read that from
/// the store instead of doing the work again. Returns the children left that didn't end yet,
/// the next daemon's to reap.
fn cut_short(
    sh: &Shared,
    end_children: impl FnOnce() -> Vec<i32>,
) -> (MutexGuard<'_, State>, Vec<i32>) {
    let st = sh.lock();
    let left = end_children();
    (st, handover::reap(&left))
}

/// [`exec_in_place`] up to the exec: [`cut_short`], then closes the store
/// ([`handover::close_store`]). Err when records the daemon holds couldn't be written:
/// `inherited` back with them, and why the restart waits.
fn close_for_exec(
    sh: &Shared,
    inherited: handover::Inherited,
    end_children: impl FnOnce() -> Vec<i32>,
) -> Result<(MutexGuard<'_, State>, handover::Inherited), Box<handover::Inherited>> {
    close_for_exec_as(sh, inherited, end_children, "restart in place")
}

/// [`close_for_exec`] for a `restart` ("restart in place", "plain restart"), as why it waits
/// says.
fn close_for_exec_as<'a>(
    sh: &'a Shared,
    mut inherited: handover::Inherited,
    end_children: impl FnOnce() -> Vec<i32>,
    restart: &str,
) -> Result<(MutexGuard<'a, State>, handover::Inherited), Box<handover::Inherited>> {
    let (mut st, left) = cut_short(sh, end_children);
    inherited.children = left;
    let unwritten = handover::close_store(&mut st, restart);
    match unwritten {
        None => Ok((st, inherited)),
        Some((why, unwritten)) => {
            drop(st);
            inherited.waits = Some(why);
            inherited.unwritten = unwritten;
            Err(Box::new(inherited))
        }
    }
}

/// Waits at most `wait` for the workers of a daemon that handed over to end.
fn wait_for_workers(sh: &Shared, wait: std::time::Duration) {
    let until = std::time::Instant::now() + wait;
    while std::time::Instant::now() < until && sh.busy() {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Captures the login environment again in the background (a daemon started from a handover
/// began with the one handed over).
fn capture_login_env_again(sh: &Arc<Shared>) {
    sh.spawn_worker("login-env", |sh| match crate::envcapture::login_env() {
        Ok((env, left_out)) => {
            for v in left_out {
                sh.log.log(format!(
                    "login environment: {v}; jobs don't get it (a job's environment holds text \
                     only)"
                ));
            }
            *sh.login_env.write().unwrap_or_else(PoisonError::into_inner) = env;
        }
        Err(e) => sh.log.log(format!(
            "login environment: capturing it again: {e:#}; jobs get the one from before the \
             restart"
        )),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run in a child of the test binary (the parent below starts it with a soft limit of 128):
    /// raises the limit and prints what it got. Does nothing in a plain test run.
    #[test]
    fn open_files_child() {
        if std::env::var_os("POTLUCK_TEST_OPEN_FILES_CHILD").is_none() {
            return;
        }
        let (before, after) = raise_open_files().unwrap();
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) };
        println!(
            "open-files {before} {after} {} {}",
            lim.rlim_cur, lim.rlim_max
        );
    }

    /// A daemon started by a service gets a low soft limit (1024 under systemd's user manager, 256
    /// under launchd); it raises it to its hard limit.
    #[test]
    fn the_open_files_limit_is_raised_to_the_hard_limit() {
        use std::os::unix::process::CommandExt;
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "daemon::tests::open_files_child", "--nocapture"])
            .env("POTLUCK_TEST_OPEN_FILES_CHILD", "1");
        // SAFETY: only getrlimit and setrlimit between fork and exec.
        unsafe {
            cmd.pre_exec(|| {
                let mut lim = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim);
                lim.rlim_cur = 128;
                if libc::setrlimit(libc::RLIMIT_NOFILE, &lim) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let out = cmd.output().unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        let line = text
            .lines()
            .find_map(|l| l.strip_prefix("open-files "))
            .unwrap_or_else(|| panic!("{text}{}", String::from_utf8_lossy(&out.stderr)));
        let n: Vec<u64> = line.split(' ').map(|x| x.parse().unwrap()).collect();
        let (before, after, soft, hard) = (n[0], n[1], n[2], n[3]);
        assert_eq!(before, 128, "{line}");
        assert_eq!(after, soft, "{line}");
        if cfg!(target_os = "linux") {
            assert_eq!(soft, hard, "{line}");
        } else {
            assert!(soft > 128 && soft <= hard, "{line}");
        }
    }

    /// A log that can't be opened when the daemon starts (here a directory stands in its way) is
    /// opened once it can be: the lines from then on are in it.
    #[test]
    fn a_daemon_log_that_cannot_be_opened_is_opened_later() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.log");
        std::fs::create_dir(&path).unwrap();
        let log = Logger::open_max(&path, 4096);
        log.log("while it can't be opened");
        std::fs::remove_dir(&path).unwrap();
        log.log("once it can");
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.ends_with(" once it can\n"), "{text:?}");
    }

    #[test]
    fn the_daemon_log_starts_afresh_past_its_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.log");
        std::fs::write(&path, "an earlier daemon's line\n").unwrap();
        let log = Logger::open_max(&path, 4096);
        for i in 0..1000 {
            log.log(format!("line {i:04} {}", "x".repeat(40)));
        }
        let now = std::fs::read_to_string(&path).unwrap();
        let before = std::fs::read_to_string(dir.path().join("daemon.log.1")).unwrap();
        assert!(
            now.len() <= 4096 && before.len() <= 4096,
            "{} {}",
            now.len(),
            before.len()
        );
        assert!(
            now.lines()
                .next()
                .unwrap()
                .ends_with("the lines before this are in daemon.log.1"),
            "{now}"
        );
        assert!(
            now.ends_with(&format!("line 0999 {}\n", "x".repeat(40))),
            "{now}"
        );
        // The lines just before the new log's are in the old one, in order.
        let first = now.lines().nth(1).unwrap();
        let n: usize = first.split(' ').nth(3).unwrap().parse().unwrap();
        assert!(
            before
                .trim_end()
                .ends_with(&format!("line {:04} {}", n - 1, "x".repeat(40))),
            "{before}"
        );
    }
}
