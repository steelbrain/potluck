//! Restarting the daemon in place (`potluck restart`, and after an update replaced the binary):
//! the daemon runs the potluck binary at its path in its own process (same pid, so a service
//! manager sees no restart) and hands it what it has open, so no client connection, member
//! connection attempt, running job or queued job is lost.
//!
//! 1. **Check.** The new binary must answer `--version`, then `_handover-check FILE` on a file of
//!    this version's format ([`check_binary`]), which also reads node.toml as `potluck daemon`
//!    would ([`starts_here`]). One that doesn't run, or wouldn't start with that node.toml, leaves
//!    the daemon serving on the old binary, saying why; one that runs but can't take a handover
//!    (an older potluck) gets a plain restart: steps 2 to 4 up to the store's closing, then the
//!    `potluck restart` that asked is told, and the same exec without a handover file closes the
//!    connections (running jobs survive it, as any restart).
//! 2. **Drain.** No job starts, no claim is made, no periodic work, mirror sync or update begins;
//!    work already going gets up to [`DRAIN_MAX`] to finish, and requests relayed to other
//!    members with it. The daemon serves requests meanwhile; a job dispatched now is queued.
//!    Then everything the daemon holds must be written down: a write that failed meanwhile (the
//!    disk filled since the check) undoes the drain, and the restart waits and is tried again
//!    every [`RETRY`], as while records can't be written when it is asked.
//! 3. **Quiesce.** The local socket and the mesh port stay open but nothing takes connections off
//!    them: new connections wait in the kernel's backlog. `wait` and `logs` long polls answer at
//!    once with what there is (a valid answer: the clients ask again on the same connection), and
//!    requests in flight finish (up to [`QUIESCE_MAX`]). A connection between two requests is
//!    handed over as it is: its next request is read by the new binary. The mesh's member
//!    connections are closed (TLS sessions can't be carried across an exec); members dial again
//!    and wait in the backlog. Work still going is cut short (child processes get SIGTERM, then
//!    SIGKILL after [`CHILD_GRACE`]) and done again by the new binary from the store, as after
//!    any restart.
//! 4. **Hand over.** A 0600 file in the state directory ([`HandoverFile`]) names the descriptors
//!    kept open across the exec (the socket, the mesh's listener, `daemon.lock`, each handed-over
//!    connection) and carries the facts, the login environment, the mesh's view of the members
//!    and the pids of children that did not end. The store is closed; the binary is started with
//!    the same arguments and `POTLUCK_HANDOVER=<file>`. A record that couldn't be written by
//!    then (a request answered during the quiesce, the last save of running jobs: the disk
//!    filled since the drain) keeps the restart from going on ([`close_store`]): the next binary
//!    starts from the store and would lose it. So does a handover file that can't be written.
//!    The daemon starts again in its own process as in 6, with the records it held over its
//!    store, and tries again every [`RETRY`].
//! 5. **Start from it.** The new binary takes the descriptors (nothing is bound again), reads the
//!    file, removes it, ends the child processes the binary before it left (it ended its own, but
//!    a thread of work cut short may have started another meanwhile, such as a git fetch tried
//!    again), opens the store, adopts running jobs as after any restart, serves the
//!    handed-over connections and starts the mesh on the kept listener with the members as they
//!    were. Facts and the login environment are detected again in the background. Members are
//!    compared records with before claims, as after any start (one heartbeat when they answer).
//! 6. **Failure.** If the exec fails, the daemon starts again in its own process from what it
//!    handed over ([`super::Daemon::start_from`]), on the binary it runs, and serves on; a
//!    `potluck restart` that asked is told it failed, and why.
//!    It goes on from the store as it was closed: everything was written to it by then, and what
//!    work cut short wrote after is done again, as after any restart.
//!    A new binary that can't start from the handover exits
//!    non-zero; its service starts it again plainly.
//!
//! Running jobs go on under the supervisors of the binary that started them, so the daemon after
//! a restart in place (or an update) reads what an older version left: the status file
//! ([`crate::types::ExitStatus`]), on macOS the supervisor's record beside it, the job records'
//! [`crate::types::Handle`] and the log. Each must stay readable across one version step; the
//! tests here pin today's formats.

use super::{Daemon, Shared};
use crate::types::Facts;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// The variable naming the handover file a daemon starts from.
pub const ENV: &str = "POTLUCK_HANDOVER";
/// The handover file's format. A binary reads only its own: a change that an older binary would
/// misread takes a new number (a field it would only miss doesn't).
pub const FORMAT: u32 = 1;
/// How long work already going gets to finish before the handover.
pub const DRAIN_MAX: Duration = Duration::from_secs(60);
/// How long requests in flight get to finish once connections wait in the backlog.
pub const QUIESCE_MAX: Duration = Duration::from_secs(10);
/// How long the handover waits for the loop's tick going on to end. One held up in something
/// slow outside the state lock (a signal through a busy systemd) is left behind: it can't write
/// after the store closes, the state being locked until the exec.
pub const LOOP_WAIT: Duration = Duration::from_secs(5);
/// SIGTERM to SIGKILL for the daemon's own child processes (git, tools) cut short.
pub const CHILD_GRACE: Duration = Duration::from_secs(1);
/// How long a request relayed to another member may wait for news while the daemon drains: its
/// long polls are cut to this, so they end within the drain.
pub const DRAIN_POLL_MS: u64 = 500;

/// The handover file: what a daemon restarting in place hands the binary it starts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandoverFile {
    pub format: u32,
    /// The potluck version that wrote it.
    pub from_version: String,
    /// When the restart was asked, on the boot clock.
    pub asked_at: u64,
    /// When connections began to wait in the backlog, on the boot clock.
    pub quiesced_at: u64,
    /// `daemon.lock`, still locked.
    pub lock_fd: RawFd,
    /// The local socket's listener.
    pub socket_fd: RawFd,
    /// The mesh's listener, when the mesh listened.
    #[serde(default)]
    pub mesh_fd: Option<RawFd>,
    /// Client connections between two requests.
    #[serde(default)]
    pub conns: Vec<HandedConn>,
    pub facts: Facts,
    pub login_env: BTreeMap<String, String>,
    /// The mesh's view of the members.
    #[serde(default)]
    pub members: Vec<crate::mesh::Seen>,
    /// Child processes of the daemon that had not ended: the new binary reaps them.
    #[serde(default)]
    pub children: Vec<i32>,
    /// Running jobs at the handover (for the answer to `potluck restart`).
    #[serde(default)]
    pub running_jobs: usize,
    /// How long the drain took, in ms.
    #[serde(default)]
    pub drained_ms: u64,
}

/// A client connection handed over.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandedConn {
    pub fd: RawFd,
    /// The connection asked for this restart: the new binary answers it.
    #[serde(default)]
    pub asked: Option<Asked>,
}

/// A `potluck restart` waiting for its answer from the new binary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Asked {
    pub from_version: String,
    /// What the client sent behind the `restart`, read with it and not answered yet: the new
    /// binary reads it before what is still in the connection.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sent_behind: Vec<u8>,
}

impl HandoverFile {
    /// Reads and checks a handover file.
    pub fn read(path: &Path) -> anyhow::Result<HandoverFile> {
        let text = std::fs::read(path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let v: serde_json::Value = serde_json::from_slice(&text)
            .map_err(|e| anyhow::anyhow!("{}: not a handover file: {e}", path.display()))?;
        let format = v.get("format").and_then(|f| f.as_u64()).unwrap_or(0);
        if format != u64::from(FORMAT) {
            anyhow::bail!(
                "{}: a handover file of format {format}; potluck {} reads format {FORMAT}",
                path.display(),
                crate::VERSION
            );
        }
        serde_json::from_value(v).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
    }

    /// Writes it as a new 0600 file at `path`, replacing one there.
    pub fn write(&self, path: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        let _ = std::fs::remove_file(path);
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(&serde_json::to_vec(self)?)?;
        f.sync_all()
    }
}

/// `potluck _handover-check FILE`: whether this binary can start from a handover file like it.
pub fn check_file(path: &Path) -> anyhow::Result<()> {
    HandoverFile::read(path).map(|_| ())
}

/// `_handover-check` exits with this when this binary wouldn't start here at all, with or without
/// a handover: `potluck daemon` would refuse this machine's node.toml ([`starts_here`]). The
/// daemon asking then keeps running, and says why, rather than hand over to a binary that ends at
/// once (a plain restart would end the same way).
pub const EXIT_WONT_START: i32 = 5;

/// Why `_handover-check` refuses.
pub enum Refused {
    /// This binary wouldn't start here at all ([`EXIT_WONT_START`]).
    WontStart(anyhow::Error),
    /// It can't start from that handover file (another format): a plain start would do.
    File(anyhow::Error),
}

/// `potluck _handover-check FILE`: [`starts_here`], then [`check_file`].
pub fn check(path: &Path) -> Result<(), Refused> {
    starts_here().map_err(Refused::WontStart)?;
    check_file(path).map_err(Refused::File)
}

/// What `potluck daemon` refuses to start with, read from this machine's node.toml (the daemon
/// running the check passes its environment on): a config it can't read or whose values are out
/// of bounds, a listen address that isn't one, and a name other than the one this machine's
/// records give it. A restart in place applies node.toml as a plain start does. `potluck service
/// install` checks it too, before it starts a service that would fail on it again and again.
pub fn starts_here() -> anyhow::Result<()> {
    let paths = crate::config::Paths::from_env()?;
    let config = crate::config::NodeConfig::load(&paths)?;
    config.listen_ip()?;
    super::Options::from_config(&config)?;
    crate::preflight::check_name(&paths, &crate::facts::node_name(&paths, &config))
}

/// Where the restart stands.
const SERVING: u8 = 0;
const DRAINING: u8 = 1;
const QUIESCING: u8 = 2;

/// A restart asked for: why, and the verdict on the new binary once it was checked.
#[derive(Default)]
struct Ask {
    /// Why a restart was asked (a log line), until it is checked.
    why: Option<String>,
    /// When it was asked, on the boot clock.
    at: u64,
    /// Bumped each time a check gives its verdict.
    round: u64,
    /// Ok: it restarts (in place, or plainly when the new binary can't take a handover: either
    /// way the connection that asked is handed over and answered once it goes ahead); Err: it
    /// doesn't restart, and why.
    verdict: Option<Result<(), String>>,
}

/// What every thread of a daemon shares about a restart in place.
pub(crate) struct Upgrade {
    phase: AtomicU8,
    /// Readable once something changed (a restart began, or the daemon stops): the accept loop
    /// and connections waiting for a request look at the flags then. Never read from.
    wake_r: UnixStream,
    wake_w: UnixStream,
    ask: Mutex<Ask>,
    ask_cv: Condvar,
    /// Connections handed over by their threads.
    handed: Mutex<Vec<(UnixStream, Option<Asked>)>>,
    /// Requests being answered (here, and those relayed here by members).
    serving: AtomicUsize,
    /// Requests this daemon relays to another member.
    relaying: AtomicUsize,
    /// The loop deciding which jobs to start ([`Upgrade::planning`]).
    planning: AtomicUsize,
    /// How long the drain took ([`super::Daemon::drain`]), in ms.
    drained_ms: std::sync::atomic::AtomicU64,
}

impl Upgrade {
    pub fn new() -> std::io::Result<Upgrade> {
        let (wake_r, wake_w) = UnixStream::pair()?;
        Ok(Upgrade {
            phase: AtomicU8::new(SERVING),
            wake_r,
            wake_w,
            ask: Mutex::new(Ask::default()),
            ask_cv: Condvar::new(),
            handed: Mutex::new(Vec::new()),
            serving: AtomicUsize::new(0),
            relaying: AtomicUsize::new(0),
            planning: AtomicUsize::new(0),
            drained_ms: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Draining or past it: no new work starts.
    pub fn draining(&self) -> bool {
        self.phase.load(Ordering::SeqCst) >= DRAINING
    }

    /// Connections wait in the backlog; long polls answer at once.
    pub fn quiescing(&self) -> bool {
        self.phase.load(Ordering::SeqCst) >= QUIESCING
    }

    /// The descriptor that turns readable when something changed.
    pub fn wake_fd(&self) -> RawFd {
        self.wake_r.as_raw_fd()
    }

    /// Makes [`Upgrade::wake_fd`] readable, for good.
    pub fn wake(&self) {
        let _ = (&self.wake_w).write(&[1]);
    }

    /// Asks for a restart in place (once; asking again while one is pending changes nothing).
    pub fn ask(&self, why: &str) {
        let mut a = self.ask.lock().unwrap_or_else(PoisonError::into_inner);
        if a.why.is_none() {
            a.why = Some(why.to_string());
            a.at = crate::daemon::clock::boot_ms();
        }
        self.ask_cv.notify_all();
    }

    /// Asks for a restart in place and waits for the verdict on the new binary: Ok when the
    /// restart goes on (in place, or plainly when the new binary can't take a handover): the
    /// caller hands its connection over, to be answered once the restart goes ahead (or doesn't
    /// after all); else why not.
    pub fn ask_and_wait(&self, why: &str, stopping: &dyn Fn() -> bool) -> Result<(), String> {
        let mut a = self.ask.lock().unwrap_or_else(PoisonError::into_inner);
        let round = a.round;
        if a.why.is_none() {
            a.why = Some(why.to_string());
            a.at = crate::daemon::clock::boot_ms();
        }
        self.ask_cv.notify_all();
        while a.round == round {
            if stopping() {
                return Err("the daemon is stopping".into());
            }
            a = self
                .ask_cv
                .wait_timeout(a, Duration::from_millis(200))
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        a.verdict.clone().unwrap_or(Err("no verdict".into()))
    }

    /// Waits until a restart is asked, or `until` returns true; the reason asked.
    pub fn wait_asked(&self, until: &dyn Fn() -> bool) -> Option<String> {
        let mut a = self.ask.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if let Some(why) = a.why.clone() {
                return Some(why);
            }
            if until() {
                return None;
            }
            a = self
                .ask_cv
                .wait_timeout(a, Duration::from_millis(200))
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// The verdict on the binary for the restart asked: those waiting get it, and a new ask can
    /// come.
    pub fn verdict(&self, v: Result<(), String>) {
        let mut a = self.ask.lock().unwrap_or_else(PoisonError::into_inner);
        a.why = None;
        a.round += 1;
        a.verdict = Some(v);
        self.ask_cv.notify_all();
    }

    /// When the restart going on was asked, on the boot clock.
    pub fn asked_at(&self) -> u64 {
        self.ask.lock().unwrap_or_else(PoisonError::into_inner).at
    }

    pub fn set_phase_draining(&self) {
        self.phase.store(DRAINING, Ordering::SeqCst);
    }

    /// A drain undone: the daemon serves as before.
    pub fn set_phase_serving(&self) {
        self.phase.store(SERVING, Ordering::SeqCst);
    }

    /// How long the drain took, in ms.
    pub fn drained_ms(&self) -> u64 {
        self.drained_ms.load(Ordering::SeqCst)
    }

    pub fn set_drained_ms(&self, ms: u64) {
        self.drained_ms.store(ms, Ordering::SeqCst);
    }

    pub fn set_phase_quiescing(&self) {
        self.phase.store(QUIESCING, Ordering::SeqCst);
    }

    /// Whether a request is being relayed to another member.
    pub fn relays(&self) -> bool {
        self.relaying.load(Ordering::SeqCst) > 0
    }

    /// Whether a request is being answered.
    pub fn answering(&self) -> bool {
        self.serving.load(Ordering::SeqCst) > 0
    }

    /// The connections handed over so far.
    pub fn take_handed(&self) -> Vec<(UnixStream, Option<Asked>)> {
        std::mem::take(&mut *self.handed.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// A connection thread's connection, handed over between two requests.
    pub fn hand_over(&self, stream: UnixStream, asked: Option<Asked>) {
        self.handed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((stream, asked));
    }

    /// Counts a request being answered while the guard lives.
    pub fn serving(&self) -> Counted<'_> {
        Counted::new(&self.serving)
    }

    /// Counts a request relayed to another member while the guard lives.
    pub fn relaying(&self) -> Counted<'_> {
        Counted::new(&self.relaying)
    }

    /// Counts the loop planning while the guard lives: taken before it looks whether the daemon
    /// drains, so a drain that begins meanwhile waits for the starts and claims of that plan
    /// (work already going), rather than ending before they begin.
    pub fn planning(&self) -> Counted<'_> {
        Counted::new(&self.planning)
    }

    /// Whether the loop is planning.
    pub fn plans(&self) -> bool {
        self.planning.load(Ordering::SeqCst) > 0
    }
}

/// Counts something going on while it lives.
pub(crate) struct Counted<'a>(&'a AtomicUsize);

impl Counted<'_> {
    fn new(n: &AtomicUsize) -> Counted<'_> {
        n.fetch_add(1, Ordering::SeqCst);
        Counted(n)
    }
}

impl Drop for Counted<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Cuts a long poll's wait to [`DRAIN_POLL_MS`] (a request relayed while the daemon drains).
pub(crate) fn cap_long_poll(req: &mut crate::proto::Request) {
    use crate::proto::Request;
    match req {
        Request::Wait { timeout_ms, .. } => {
            *timeout_ms = Some(timeout_ms.unwrap_or(DRAIN_POLL_MS).min(DRAIN_POLL_MS));
        }
        Request::Logs { wait_ms, .. } => *wait_ms = (*wait_ms).min(DRAIN_POLL_MS),
        _ => {}
    }
}

/// What [`wait_readable`] saw.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Ready {
    Fd,
    Wake,
    TimedOut,
}

/// Waits until `fd` is readable (or closed, or in error) or `wake` is, at most `timeout`.
pub(crate) fn wait_readable(fd: RawFd, wake: RawFd, timeout: Option<Duration>) -> Ready {
    let deadline = timeout.map(|t| Instant::now() + t);
    loop {
        let ms = match deadline {
            None => -1,
            Some(d) => {
                let left = d.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Ready::TimedOut;
                }
                // Rounded up: poll's 0 would spin.
                left.as_millis().clamp(1, i32::MAX as u128) as libc::c_int
            }
        };
        let mut fds = [
            libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: two pollfds we own, for descriptors that stay open during the call.
        let n = unsafe { libc::poll(fds.as_mut_ptr(), 2, ms) };
        if n < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            // Let the caller's read report it.
            return Ready::Fd;
        }
        if fds[1].revents != 0 {
            return Ready::Wake;
        }
        if fds[0].revents != 0 {
            return Ready::Fd;
        }
    }
}

/// Sets or clears FD_CLOEXEC on `fd`.
pub(crate) fn set_cloexec(fd: RawFd, on: bool) -> std::io::Result<()> {
    // SAFETY: fcntl on a descriptor the caller owns.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let flags = if on {
            flags | libc::FD_CLOEXEC
        } else {
            flags & !libc::FD_CLOEXEC
        };
        if libc::fcntl(fd, libc::F_SETFD, flags) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Whether `fd` is an open socket.
fn is_socket(fd: RawFd) -> bool {
    // SAFETY: fstat fills `st`; a descriptor that isn't open only makes it fail.
    unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        libc::fstat(fd, &mut st) == 0 && (st.st_mode & libc::S_IFMT) == libc::S_IFSOCK
    }
}

/// Which socket `fd` is (its device and inode): the same for every descriptor of it.
pub(crate) fn socket_id(fd: RawFd) -> Option<(libc::dev_t, libc::ino_t)> {
    // SAFETY: fstat fills `st`; a descriptor that isn't open only makes it fail.
    unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        if libc::fstat(fd, &mut st) != 0 {
            return None;
        }
        Some((st.st_dev, st.st_ino))
    }
}

/// The daemon's child processes now (zombies too).
pub(crate) fn own_children() -> Vec<i32> {
    // SAFETY: getpid has no preconditions.
    let me = unsafe { libc::getpid() };
    children_of(me)
}

#[cfg(target_os = "linux")]
fn children_of(parent: i32) -> Vec<i32> {
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    dir.filter_map(|e| {
        let name = e.ok()?.file_name();
        let pid: i32 = name.to_str()?.parse().ok()?;
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        // "pid (comm) state ppid ...": comm may hold spaces and parentheses.
        let rest = &stat[stat.rfind(')')? + 1..];
        let ppid: i32 = rest.split_whitespace().nth(1)?.parse().ok()?;
        (ppid == parent).then_some(pid)
    })
    .collect()
}

#[cfg(target_os = "macos")]
fn children_of(parent: i32) -> Vec<i32> {
    const CAP: usize = 4096;
    let mut buf = vec![0 as libc::pid_t; CAP];
    // SAFETY: the buffer holds CAP pids and says so in bytes.
    let n = unsafe {
        libc::proc_listchildpids(
            parent,
            buf.as_mut_ptr().cast(),
            (CAP * std::mem::size_of::<libc::pid_t>()) as libc::c_int,
        )
    };
    if n <= 0 {
        return Vec::new();
    }
    // A count of pids (or of bytes on some systems): the buffer was zeroed either way.
    buf.truncate((n as usize).min(CAP));
    buf.into_iter().filter(|&p| p > 0).collect()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn children_of(_parent: i32) -> Vec<i32> {
    Vec::new()
}

/// Reaps those of `pids` that ended (they are this process's children nobody waits for any
/// more); returns those still running.
pub(crate) fn reap(pids: &[i32]) -> Vec<i32> {
    pids.iter()
        .copied()
        .filter(|&pid| {
            let mut status = 0;
            // SAFETY: waitpid on one pid, without blocking.
            let r = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            r == 0
        })
        .collect()
}

/// Ends the daemon's child processes still running (git, tools: work cut short): SIGTERM, which
/// lets git remove its lock files, then SIGKILL after `grace` to those left. The threads that
/// started them reap them; returns the children still there after (zombies of threads that
/// stopped waiting, or processes that can't die).
pub(crate) fn end_children(grace: Duration) -> Vec<i32> {
    let first = own_children();
    if first.is_empty() {
        return first;
    }
    end(&first, grace);
    own_children()
}

/// [`end_children`] for children `pids`: SIGTERM, SIGKILL after `grace` to those still running,
/// then a short wait for them to go.
fn end(pids: &[i32], grace: Duration) {
    for &pid in pids {
        // SAFETY: a signal to a child of this process.
        unsafe { libc::kill(pid, libc::SIGTERM) };
    }
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline && pids.iter().any(|p| running(*p)) {
        std::thread::sleep(Duration::from_millis(20));
    }
    for &pid in pids {
        if running(pid) {
            // SAFETY: a signal to a child of this process.
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline && pids.iter().any(|p| running(*p)) {
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Whether child `pid` still runs (not a zombie).
fn running(pid: i32) -> bool {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|s| {
                let rest = &s[s.rfind(')')? + 1..];
                rest.split_whitespace().next().map(|st| st != "Z")
            })
            .unwrap_or(false)
    }
    #[cfg(not(target_os = "linux"))]
    {
        // SAFETY: proc_pidinfo fills `info`; an ended or unknown pid only makes it fail.
        unsafe {
            let mut info: libc::proc_bsdinfo = std::mem::zeroed();
            let n = libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                (&raw mut info).cast(),
                std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int,
            );
            n > 0 && info.pbi_status != libc::SZOMB
        }
    }
}

/// Why a binary can't take over.
#[derive(Debug)]
pub enum CheckFailed {
    /// It doesn't run on this machine (or isn't potluck): the daemon keeps running.
    DoesntRun(String),
    /// It runs, but can't start from this version's handover (an older potluck): a plain
    /// restart.
    CantTakeOver { version: String, why: String },
    /// It runs, but wouldn't start here at all (node.toml): the daemon keeps running.
    WontStart { version: String, why: String },
}

/// Checks that `bin` can take over from this daemon: it answers `--version`, and
/// `_handover-check` reads `sample` (a handover file of this version's format). Returns its
/// version.
pub fn check_binary(bin: &Path, sample: &Path) -> Result<String, CheckFailed> {
    let version =
        crate::update::binary_version(bin).map_err(|e| CheckFailed::DoesntRun(format!("{e:#}")))?;
    let out = run_limited(
        std::process::Command::new(bin)
            .arg("_handover-check")
            .arg(sample),
        Duration::from_secs(30),
    );
    match out {
        Ok((status, _)) if status.success() => Ok(version),
        Ok((status, stderr)) => {
            let said = String::from_utf8_lossy(&stderr)
                .lines()
                .map(|l| l.trim().trim_start_matches("potluck: ").to_string())
                .find(|l| !l.is_empty())
                .unwrap_or_else(|| status.to_string());
            if status.code() == Some(EXIT_WONT_START) {
                // All of it: a config's parse error spans lines (where, then what).
                let all = String::from_utf8_lossy(&stderr);
                let why = all.trim().trim_start_matches("potluck: ").to_string();
                return Err(CheckFailed::WontStart { version, why });
            }
            Err(CheckFailed::CantTakeOver { version, why: said })
        }
        Err(e) => Err(CheckFailed::DoesntRun(format!(
            "{} _handover-check: {e}",
            bin.display()
        ))),
    }
}

/// Runs `cmd` (stdin closed, stderr kept), killing it after `limit`.
fn run_limited(
    cmd: &mut std::process::Command,
    limit: Duration,
) -> std::io::Result<(std::process::ExitStatus, Vec<u8>)> {
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    // Retried while the file is busy: a new binary just written may still be open in a child
    // another thread forked meanwhile, until that child execs.
    let mut tries = 0;
    let mut child = loop {
        match cmd.spawn() {
            Err(e) if e.raw_os_error() == Some(libc::ETXTBSY) && tries < 50 => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(20));
            }
            other => break other?,
        }
    };
    let mut stderr = child.stderr.take();
    let reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(e) = stderr.as_mut() {
            let _ = e.take(64 << 10).read_to_end(&mut buf);
        }
        buf
    });
    let deadline = Instant::now() + limit;
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("didn't finish in {}s", limit.as_secs()),
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    Ok((status, reader.join().unwrap_or_default()))
}

/// Where a daemon started from a handover came from: for the answer to `potluck restart`.
pub(crate) struct Came {
    pub from_version: String,
    pub quiesced_at: u64,
    pub running_jobs: usize,
    pub drained_ms: u64,
    /// The exec failed, and why: this is the daemon before it, started again in its own process
    /// from what it handed over.
    pub failed: Option<String>,
    /// The restart didn't go on, as this says ([`Inherited::waits`]): this is the daemon before
    /// it, started again in its own process with what it held.
    pub waits: Option<String>,
}

impl Came {
    /// The answer to the `potluck restart` that asked for this restart, sent once this daemon
    /// serves.
    pub fn answer(&self, _asked: &Asked, sh: &Shared) -> crate::proto::Response {
        if let Some(message) = &self.waits {
            return crate::proto::Response::Error {
                message: message.clone(),
            };
        }
        if let Some(why) = &self.failed {
            return crate::proto::Response::Error {
                message: format!(
                    "restarting in place failed: {why}; this daemon serves on, still potluck {}",
                    crate::VERSION
                ),
            };
        }
        let waited = crate::daemon::clock::boot_ms().saturating_sub(self.quiesced_at);
        let jobs = match self.running_jobs {
            0 => "No job was running".to_string(),
            1 => "Its 1 running job kept running".to_string(),
            n => format!("Its {n} running jobs kept running"),
        };
        let drained = if self.drained_ms >= 1000 {
            format!(
                " It waited {:.1} s for work already going to finish first.",
                self.drained_ms as f64 / 1000.0
            )
        } else {
            String::new()
        };
        let _ = sh;
        crate::proto::Response::Text {
            text: format!(
                "Restarted in place: potluck {} became potluck {} in the same process (pid {}). \
                 {jobs}; open connections and the socket were handed over, and \
                 new connections waited {waited} ms.{drained}",
                self.from_version,
                crate::VERSION,
                std::process::id()
            ),
        }
    }
}

/// `n` with the noun for one or for many: "1 connection", "2 connections".
pub(crate) fn counted(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// What a daemon hands the next one, in this process: [`Daemon::hand_over`] makes it, and
/// [`Daemon::start_from`] starts from it (the exec in between is [`exec`]'s).
pub struct Inherited {
    pub lock: std::fs::File,
    pub socket: UnixListener,
    pub mesh: Option<std::net::TcpListener>,
    pub conns: Vec<(UnixStream, Option<Asked>)>,
    pub facts: Facts,
    pub login_env: BTreeMap<String, String>,
    pub members: Vec<crate::mesh::Seen>,
    pub children: Vec<i32>,
    pub from_version: String,
    pub asked_at: u64,
    pub quiesced_at: u64,
    pub running_jobs: usize,
    pub drained_ms: u64,
    /// Set when the exec failed and the daemon starts again in its own process from this, on the
    /// binary it runs: why ([`Came::failed`]). Never in the handover file.
    pub failed: Option<String>,
    /// Set when the restart didn't go on because records the daemon held couldn't be written
    /// before the store closed ([`close_store`]), or the handover file couldn't be: why
    /// ([`Came::waits`]); the daemon starts again in its own process from this and tries again
    /// every [`RETRY`]. Never in the handover file.
    pub waits: Option<String>,
    /// Those records, for the daemon started again ([`super::runner::restore`]). Never in the
    /// handover file (there is none then).
    pub(crate) unwritten: super::state::Unwritten,
}

impl Inherited {
    /// The file naming its descriptors, which stay open across an exec from now on.
    pub fn into_file(self) -> std::io::Result<HandoverFile> {
        let keep = |fd: RawFd| set_cloexec(fd, false).map(|()| fd);
        let lock_fd = keep(self.lock.into_raw_fd())?;
        let socket_fd = keep(self.socket.into_raw_fd())?;
        let mesh_fd = self.mesh.map(|l| keep(l.into_raw_fd())).transpose()?;
        let mut conns = Vec::new();
        for (s, asked) in self.conns {
            conns.push(HandedConn {
                fd: keep(s.into_raw_fd())?,
                asked,
            });
        }
        Ok(HandoverFile {
            format: FORMAT,
            from_version: self.from_version,
            asked_at: self.asked_at,
            quiesced_at: self.quiesced_at,
            lock_fd,
            socket_fd,
            mesh_fd,
            conns,
            facts: self.facts,
            login_env: self.login_env,
            members: self.members,
            children: self.children,
            running_jobs: self.running_jobs,
            drained_ms: self.drained_ms,
        })
    }

    /// Takes the descriptors a handover file names (inherited across the exec).
    pub fn from_file(f: HandoverFile) -> anyhow::Result<Inherited> {
        let take = |fd: RawFd, what: &str| -> anyhow::Result<RawFd> {
            if fd < 0 {
                anyhow::bail!("the handover names no {what}");
            }
            set_cloexec(fd, true)
                .map_err(|e| anyhow::anyhow!("the handed-over {what} (fd {fd}): {e}"))?;
            Ok(fd)
        };
        let lock = take(f.lock_fd, "daemon.lock")?;
        let socket = take(f.socket_fd, "socket")?;
        if !is_socket(socket) {
            anyhow::bail!("the handed-over socket (fd {socket}) is not a socket");
        }
        let mesh = f.mesh_fd.map(|fd| take(fd, "mesh listener")).transpose()?;
        let mut conns = Vec::new();
        for c in f.conns {
            // A connection whose descriptor is gone (it can't be, short of a bug) is skipped.
            if let Ok(fd) = take(c.fd, "connection")
                && is_socket(fd)
            {
                // SAFETY: an open socket this process inherited, owned from here on.
                conns.push((unsafe { UnixStream::from_raw_fd(fd) }, c.asked));
            }
        }
        // SAFETY: open descriptors this process inherited, each owned from here on.
        Ok(unsafe {
            Inherited {
                lock: std::fs::File::from_raw_fd(lock),
                socket: UnixListener::from_raw_fd(socket),
                mesh: mesh.map(|fd| std::net::TcpListener::from_raw_fd(fd)),
                conns,
                facts: f.facts,
                login_env: f.login_env,
                members: f.members,
                children: f.children,
                from_version: f.from_version,
                asked_at: f.asked_at,
                quiesced_at: f.quiesced_at,
                running_jobs: f.running_jobs,
                drained_ms: f.drained_ms,
                failed: None,
                waits: None,
                unwritten: Default::default(),
            }
        })
    }
}

/// The handover a daemon starting now was given (`POTLUCK_HANDOVER`), taken out of the
/// environment (so no child sees it) and its file removed. Call before any thread starts.
pub fn take() -> anyhow::Result<Option<Inherited>> {
    let Some(path) = std::env::var_os(ENV) else {
        return Ok(None);
    };
    // SAFETY: called first thing in `potluck daemon`, before the daemon starts any thread.
    unsafe { std::env::remove_var(ENV) };
    take_from(&PathBuf::from(path)).map(Some)
}

/// [`take`] from the file at `path`. It is removed once read, and when it is a handover file of
/// another format; a file that isn't a handover file at all (the variable set by hand, naming
/// some other file) is left alone.
fn take_from(path: &Path) -> anyhow::Result<Inherited> {
    let read = HandoverFile::read(path);
    if read.is_ok() || of_some_format(path) {
        let _ = std::fs::remove_file(path);
    }
    let file = read.map_err(|e| anyhow::anyhow!("starting from a handover: {e:#}"))?;
    Inherited::from_file(file).map_err(|e| anyhow::anyhow!("starting from a handover: {e:#}"))
}

/// Whether the file at `path` is a handover file of some format: a JSON object with a number
/// `format`.
fn of_some_format(path: &Path) -> bool {
    std::fs::read(path)
        .ok()
        .and_then(|t| serde_json::from_slice::<serde_json::Value>(&t).ok())
        .is_some_and(|v| v.get("format").is_some_and(serde_json::Value::is_u64))
}

/// Where the daemon writes the handover file, and the sample it checks a binary with.
pub fn file_path(home: &Path) -> PathBuf {
    home.join("handover.json")
}

fn sample_path(home: &Path) -> PathBuf {
    home.join("handover-check.json")
}

/// Starts `bin` in this process with the same arguments and `POTLUCK_HANDOVER=file`, or with no
/// handover (`None`: a plain restart). Returns only if the exec failed.
pub fn exec(bin: &Path, file: Option<&Path>) -> std::io::Error {
    use std::os::unix::process::CommandExt;
    let mut cmd = std::process::Command::new(bin);
    cmd.args(std::env::args_os().skip(1));
    if let Some(f) = file {
        cmd.env(ENV, f);
    }
    // std sets SIGPIPE to its default for the program it executes, in this process, and leaves
    // it so when the exec fails. The daemon then serves on, and the next write to a pipe whose
    // reader went (its stderr when journald restarts, a git filter that exits early) would kill
    // it: put back as it was.
    // SAFETY: all zeros is a valid sigaction; sigaction(2) only reads SIGPIPE's disposition into
    // it.
    let mut was: libc::sigaction = unsafe { std::mem::zeroed() };
    let kept = unsafe { libc::sigaction(libc::SIGPIPE, std::ptr::null(), &mut was) } == 0;
    let e = cmd.exec();
    if kept {
        // SAFETY: restores the disposition read above.
        unsafe { libc::sigaction(libc::SIGPIPE, &was, std::ptr::null_mut()) };
    }
    e
}

/// How a daemon's run ends: [`serve`] returns it. Made once per restart.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Ending {
    /// Exec `bin` with this handover (if that fails, the daemon starts again from it in this
    /// process, on the binary it runs).
    InPlace {
        bin: PathBuf,
        inherited: Inherited,
        shared: std::sync::Arc<Shared>,
        /// Why the restart was asked, to ask again if it doesn't go on.
        why: String,
    },
    /// Exec `bin` without a handover (it can't take one), through the same last steps as
    /// [`Ending::InPlace`] up to the exec; those who asked are told `told` right before it.
    Plain {
        bin: PathBuf,
        inherited: Inherited,
        shared: std::sync::Arc<Shared>,
        why: String,
        told: String,
    },
}

/// `potluck daemon` after the daemon started: serves until a restart is asked and the new
/// binary passes its check, then drains and hands over, and says what to exec. A binary that
/// doesn't run is refused, and the daemon serves on; one that runs but can't take a handover gets
/// a plain restart, through the same drain and handover. While what the daemon holds can't all be
/// written down (`unsaved`), the restart waits and is tried again every [`RETRY`]; `retry`: a
/// restart (why it was asked) that waits so already, tried again [`RETRY`] from now.
pub(crate) fn serve(daemon: Daemon, retry: Option<String>) -> anyhow::Result<Ending> {
    let sh = daemon.shared.clone();
    let mut pending: Option<String> = retry.inspect(|_| {
        let until = Instant::now() + RETRY;
        while Instant::now() < until && !sh.stopping() {
            std::thread::sleep(Duration::from_millis(100));
        }
    });
    loop {
        let why = match pending.take() {
            Some(why) => why,
            None => match sh.up.wait_asked(&|| sh.stopping()) {
                Some(why) => why,
                None => anyhow::bail!("the daemon stopped"),
            },
        };
        let bin = crate::update::running_binary()?;
        sh.log.log(format!(
            "restarting in place ({why}): checking {}",
            bin.display()
        ));
        let checked = unsaved(&sh).map_or_else(
            || {
                let sample = sample_path(&sh.paths.home);
                let r = write_sample(&sh, &sample)
                    .map_err(|e| Checked::Waits(format!("writing {}: {e}", sample.display())))
                    .and_then(|()| check_binary(&bin, &sample).map_err(Checked::Failed));
                let _ = std::fs::remove_file(&sample);
                r
            },
            |e| Err(Checked::Waits(e)),
        );
        let (version, plain) = match checked {
            Ok(version) => (version, None),
            // A plain restart: the same exec without a handover file, through the same drain,
            // quiesce and last writes, so it loses nothing the next binary would need from the
            // store either; the `potluck restart`s that asked are told once it goes ahead.
            Err(Checked::Failed(CheckFailed::CantTakeOver { version, why })) => {
                sh.log.log(format!(
                    "potluck {version} at {} can't take over in place ({why}): restarting it \
                     plainly; open connections close, running jobs keep running",
                    bin.display()
                ));
                let told = format!(
                    "Restarted plainly, not in place: potluck {version} at {} can't take over \
                     from potluck {} ({why}), so the daemon stopped and started it without a \
                     handover. Open connections were closed; running jobs kept running.",
                    bin.display(),
                    crate::VERSION
                );
                (version, Some(told))
            }
            Err(Checked::Waits(e)) => {
                waits(&sh, "restart in place", &e);
                pending = Some(why);
                continue;
            }
            Err(Checked::Failed(CheckFailed::DoesntRun(e))) => {
                let msg = format!(
                    "not restarting: {} doesn't run ({e}); this daemon keeps running potluck {}",
                    bin.display(),
                    crate::VERSION
                );
                sh.log.log(&msg);
                sh.up.verdict(Err(msg));
                continue;
            }
            Err(Checked::Failed(CheckFailed::WontStart { version, why })) => {
                let msg = format!(
                    "not restarting: potluck {version} at {} wouldn't start here, so this daemon \
                     keeps running potluck {}: {why}",
                    bin.display(),
                    crate::VERSION
                );
                sh.log.log(&msg);
                sh.up.verdict(Err(msg));
                continue;
            }
        };
        let restart = if plain.is_some() {
            "plain restart"
        } else {
            "restart in place"
        };
        // Drained, and only then is what the daemon holds all written down for sure: a write
        // that failed meanwhile makes the restart wait.
        sh.log.log(format!(
            "restarting {} as potluck {version} ({}): draining",
            if plain.is_some() {
                "plainly"
            } else {
                "in place"
            },
            bin.display()
        ));
        match daemon.drain(DRAIN_MAX) {
            Ok(drained) => return Ok(go_on(daemon, bin, why, drained, plain)),
            Err(e) => {
                waits(&sh, restart, &e);
                pending = Some(why);
            }
        }
    }
}

/// The restart (drained for `drained` ms) goes on: those who asked hand their connections over,
/// and the daemon hands over. `plain`: a plain restart, and what they are told once it goes
/// ahead.
pub(crate) fn go_on(
    daemon: Daemon,
    bin: PathBuf,
    why: String,
    drained: u64,
    plain: Option<String>,
) -> Ending {
    let shared = daemon.shared.clone();
    shared.up.verdict(Ok(()));
    let left = DRAIN_MAX.saturating_sub(Duration::from_millis(drained));
    let how = if plain.is_some() {
        "plainly"
    } else {
        "in place"
    };
    let inherited = daemon.hand_over_as(left, how);
    match plain {
        None => Ending::InPlace {
            bin,
            inherited,
            shared,
            why,
        },
        Some(told) => Ending::Plain {
            bin,
            inherited,
            shared,
            why,
            told,
        },
    }
}

/// The `restart` (in place, or plain) waits, as `e` says: logged, told to those who asked, then
/// [`RETRY`] (unless the daemon stops meanwhile).
fn waits(sh: &Shared, restart: &str, e: &str) {
    let msg = format!(
        "the {restart} waits: {e}; this daemon serves on and tries again every {}s",
        RETRY.as_secs()
    );
    sh.log.log(&msg);
    sh.up.verdict(Err(msg));
    let until = Instant::now() + RETRY;
    while Instant::now() < until && !sh.stopping() {
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// How often a restart that waits (`unsaved`) is tried again.
pub const RETRY: Duration = Duration::from_secs(10);

/// Why the restart can't go on now: what the daemon holds that isn't written down yet (records
/// whose write to the store failed, membership records the mesh couldn't save: a full disk). The
/// next binary starts from the store and the files, so it would lose them.
pub(crate) fn unsaved(sh: &Shared) -> Option<String> {
    if let Some(why) = unwritten_records(&mut sh.lock()) {
        return Some(why);
    }
    if let Some(c) = sh.cluster()
        && !c.mesh().records_saved()
    {
        return Some(format!(
            "the cluster's membership records can't be written to {} yet (is the disk full?)",
            sh.paths.keys_dir().display()
        ));
    }
    None
}

/// Records whose writes failed, written again now if they can be; if some still can't, how
/// many and which first.
fn unwritten_records(st: &mut super::state::State) -> Option<String> {
    if !st.unsaved.is_empty() {
        st.save_again();
    }
    let first = st.unsaved.iter().next()?;
    Some(format!(
        "{} can't be written to the store yet ({first} first; is the disk full?)",
        counted(st.unsaved.len(), "record", "records")
    ))
}

/// The last step before the exec: closes the store (the caller keeps the state locked until the
/// exec, so nothing writes after). The drain saw everything written ([`super::Daemon::drain`]),
/// but a request answered during the quiesce or the last save of running jobs may have failed to
/// write since (the disk filled): the next binary starts from the store and would lose it, so
/// then the restart doesn't go on. Returned with the store closed all the same (the daemon
/// started again in this process opens it again): why, and what isn't written
/// ([`Inherited::unwritten`]).
pub(crate) fn close_store(
    st: &mut super::state::State,
    restart: &str,
) -> Option<(String, super::state::Unwritten)> {
    let left = unwritten_records(st).map(|why| {
        let message = format!(
            "the {restart} waits: {why}; this daemon serves on and tries again every {}s",
            RETRY.as_secs()
        );
        (message, st.unwritten())
    });
    st.store.close();
    left
}

/// Why [`serve`] doesn't restart now.
enum Checked {
    /// Not now: tried again.
    Waits(String),
    Failed(CheckFailed),
}

/// A handover file of this version's format, with no descriptors, for the new binary's check
/// (it is about the size of the real one: writing it shows there is room for that too).
fn write_sample(sh: &Shared, path: &Path) -> std::io::Result<()> {
    let facts = sh.lock().local().facts.clone();
    HandoverFile {
        format: FORMAT,
        from_version: crate::VERSION.to_string(),
        asked_at: 0,
        quiesced_at: 0,
        lock_fd: -1,
        socket_fd: -1,
        mesh_fd: None,
        conns: Vec::new(),
        facts,
        login_env: sh.login_env(),
        members: Vec::new(),
        children: Vec::new(),
        running_jobs: 0,
        drained_ms: 0,
    }
    .write(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handover_file_of_another_format_is_refused_with_both_formats() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("h.json");
        std::fs::write(&p, r#"{"format": 99, "lock_fd": 3}"#).unwrap();
        let e = HandoverFile::read(&p).unwrap_err().to_string();
        assert!(
            e.contains("format 99") && e.contains(&format!("format {FORMAT}")),
            "{e}"
        );
        std::fs::write(&p, "not json").unwrap();
        assert!(check_file(&p).is_err());
    }

    /// `POTLUCK_HANDOVER` set by hand on a plain `potluck daemon`, naming a file that isn't a
    /// handover file: the daemon refuses to start, and leaves the file alone (it removed it).
    #[test]
    fn a_file_that_isnt_a_handover_file_is_refused_and_left_alone() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("notes.txt");
        std::fs::write(&p, "keep me\n").unwrap();
        let e = take_from(&p).err().unwrap().to_string();
        assert!(e.contains("not a handover file"), "{e}");
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "keep me\n");
        // One of another format is a handover file: it goes.
        std::fs::write(&p, r#"{"format": 99}"#).unwrap();
        let e = take_from(&p).err().unwrap().to_string();
        assert!(e.contains("format 99"), "{e}");
        assert!(!p.exists());
    }

    #[test]
    fn a_written_handover_file_is_private_and_reads_back() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("h.json");
        let f = HandoverFile {
            format: FORMAT,
            from_version: "0.1.0".into(),
            asked_at: 1,
            quiesced_at: 2,
            lock_fd: 3,
            socket_fd: 4,
            mesh_fd: Some(5),
            conns: vec![HandedConn {
                fd: 6,
                asked: Some(Asked {
                    from_version: "0.1.0".into(),
                    sent_behind: b"{}\n".to_vec(),
                }),
            }],
            facts: Facts::default(),
            login_env: [("PATH".to_string(), "/bin".to_string())].into(),
            members: Vec::new(),
            children: vec![42],
            running_jobs: 1,
            drained_ms: 7,
        };
        f.write(&p).unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&p).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "{mode:o}");
        let back = HandoverFile::read(&p).unwrap();
        assert_eq!(back.conns[0].fd, 6);
        assert_eq!(back.conns[0].asked.as_ref().unwrap().sent_behind, b"{}\n");
        assert_eq!(back.children, [42]);
        assert_eq!(back.login_env["PATH"], "/bin");
    }

    #[test]
    fn a_child_left_running_is_ended_and_reaped() {
        // A child nobody waits for (as one whose thread stopped waiting): SIGTERM ignored, so it
        // takes the SIGKILL; then reaped here.
        let child = std::process::Command::new("sh")
            .args(["-c", "trap '' TERM; sleep 30"])
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        std::mem::forget(child);
        std::thread::sleep(Duration::from_millis(100));
        assert!(own_children().contains(&pid));
        assert!(running(pid));
        // Only this test's child: the test process runs other tests' children beside it.
        end(&[pid], Duration::from_millis(200));
        // Killed, a zombie until reaped.
        assert!(own_children().contains(&pid));
        assert!(!running(pid));
        // On macOS, `running` says no as soon as the child is on its way out, a moment before
        // waitpid has it as a zombie (seen on a slow CI runner): reaped within a moment, as the
        // daemon reaps such a child later (`reap_orphans`).
        let until = Instant::now() + Duration::from_secs(5);
        while !reap(&[pid]).is_empty() {
            assert!(Instant::now() < until, "{pid} not reaped");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!own_children().contains(&pid));
    }

    /// What a job's supervisor (an older binary's, after a restart in place) leaves, as written
    /// today: a change that the next daemon would misread fails here.
    #[test]
    fn what_a_running_job_leaves_is_read_in_todays_formats() {
        use crate::types::{ExitStatus, Handle};
        let status = r#"{"code":7,"signal":null,"ended_at":1793813647376}"#;
        let read: ExitStatus = serde_json::from_str(status).unwrap();
        assert_eq!(
            read,
            ExitStatus {
                code: Some(7),
                signal: None,
                ended_at: 1793813647376,
            }
        );
        assert_eq!(serde_json::to_string(&read).unwrap(), status);
        let killed: ExitStatus =
            serde_json::from_str(r#"{"code":null,"signal":9,"ended_at":5}"#).unwrap();
        assert_eq!((killed.code, killed.signal), (None, Some(9)));
        // Handles as the oldest daemons stored them, without the fields added since.
        let unit: Handle =
            serde_json::from_str(r#"{"kind":"systemd","unit":"potluck-a-1","cgroup":null}"#)
                .unwrap();
        assert!(
            matches!(
                unit,
                Handle::Systemd {
                    invocation: None,
                    ..
                }
            ),
            "{unit:?}"
        );
        let process: Handle = serde_json::from_str(
            r#"{"kind":"process","pid":4321,"start_time":99,"tracked":[[4322,100]]}"#,
        )
        .unwrap();
        assert!(
            matches!(&process, Handle::Process { pid: 4321, start_time: 99, tracked, .. } if tracked == &[(4322, 100)]),
            "{process:?}"
        );
    }

    mod gate {
        use super::super::*;
        use crate::config::{NodeConfig, Paths};
        use crate::daemon::state::Unsaved;
        use crate::daemon::{Options, Parts};
        use crate::exec::{Executor, Launch, Sample};
        use crate::types::{Handle, JobRecord, JobState};
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;

        /// Says every handle runs, and (as macOS follows processes that leave a job's session)
        /// adds a process to it at every sample.
        #[derive(Default)]
        pub(crate) struct Runs(AtomicU32, pub AtomicU32);

        impl Executor for Runs {
            fn start(&self, _: &Launch) -> anyhow::Result<Handle> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Ok(Handle::Process {
                    pid: 4_000_001,
                    start_time: 0,
                    tracked: Vec::new(),
                    gone: Vec::new(),
                    lease: String::new(),
                    tasks_max: 0,
                    boot: String::new(),
                })
            }
            fn sample(&self, h: &mut Handle, _: u64) -> anyhow::Result<Sample> {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                let Handle::Process { pid, tracked, .. } = h else {
                    anyhow::bail!("not a process handle");
                };
                tracked.push((100_000 + n, 0));
                Ok(Sample {
                    pids: vec![*pid],
                    cpu_ms: u64::from(n) * 10,
                    ..Default::default()
                })
            }
            fn signal(&self, _: &Handle, _: i32) -> anyhow::Result<()> {
                Ok(())
            }
            fn stacks(&self, _: &Handle) -> anyhow::Result<String> {
                Ok(String::new())
            }
            fn finish(&self, _: &Handle, _: &Path, _: crate::types::Millis) -> anyhow::Result<()> {
                Ok(())
            }
        }

        /// Seeds a workspace in a child process (as git does), the first time for 30 s, failing
        /// if the child is killed.
        #[derive(Default)]
        pub(crate) struct SlowSeed {
            pub child: std::sync::Mutex<Option<i32>>,
            seeded: AtomicU32,
            /// Mirror syncs wait while this is set.
            pub hold_sync: std::sync::atomic::AtomicBool,
            pub syncing: std::sync::atomic::AtomicBool,
            /// Seeds wait while this is set, with no child process (as on a mirror's lock).
            pub hold_create: std::sync::atomic::AtomicBool,
            pub creating: AtomicU32,
        }

        impl crate::daemon::WorkspaceProvider for SlowSeed {
            fn sync_mirror(&self, _: &str, _: &str) -> anyhow::Result<Option<String>> {
                self.syncing.store(true, Ordering::SeqCst);
                while self.hold_sync.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(None)
            }
            fn has_commit(&self, _: &str, _: &str) -> bool {
                true
            }
            fn pin(&self, _: &str, _: &str, _: &str) -> anyhow::Result<()> {
                Ok(())
            }
            fn unpin(&self, _: &str, _: &str) -> anyhow::Result<()> {
                Ok(())
            }
            fn create(&self, id: &String, _: &crate::types::RepoRef) -> anyhow::Result<PathBuf> {
                if self.hold_create.load(Ordering::SeqCst) {
                    self.creating.fetch_add(1, Ordering::SeqCst);
                    while self.hold_create.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    return Ok(PathBuf::from("/tmp").join(id));
                }
                if self.seeded.fetch_add(1, Ordering::SeqCst) == 0 {
                    let mut child = std::process::Command::new("sleep").arg("30").spawn()?;
                    *self.child.lock().unwrap() = Some(child.id() as i32);
                    let status = child.wait()?;
                    if !status.success() {
                        anyhow::bail!("git worktree add: {status}");
                    }
                }
                Ok(PathBuf::from("/tmp").join(id))
            }
            fn verify(&self, _: &Path, _: &crate::types::RepoRef) -> anyhow::Result<()> {
                Ok(())
            }
            fn remove(&self, _: &crate::types::WorkspaceRecord) -> anyhow::Result<()> {
                Ok(())
            }
            fn size(&self, _: &Path) -> anyhow::Result<u64> {
                Ok(0)
            }
        }

        /// Says every handle runs over its RAM limit; a signal waits while `hold` is set, as a
        /// `systemctl kill` (Linux) does while systemd is busy.
        #[derive(Default)]
        pub(crate) struct SlowSignal {
            pub hold: std::sync::atomic::AtomicBool,
            pub signalling: std::sync::atomic::AtomicBool,
        }

        impl Executor for SlowSignal {
            fn start(&self, _: &Launch) -> anyhow::Result<Handle> {
                anyhow::bail!("not here")
            }
            fn sample(&self, h: &mut Handle, _: u64) -> anyhow::Result<Sample> {
                let Handle::Process { pid, .. } = h else {
                    anyhow::bail!("not a process handle");
                };
                Ok(Sample {
                    pids: vec![*pid],
                    over_memory: true,
                    ..Default::default()
                })
            }
            fn signal(&self, _: &Handle, _: i32) -> anyhow::Result<()> {
                self.signalling.store(true, Ordering::SeqCst);
                while self.hold.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(())
            }
            fn stacks(&self, _: &Handle) -> anyhow::Result<String> {
                Ok(String::new())
            }
            fn finish(&self, _: &Handle, _: &Path, _: crate::types::Millis) -> anyhow::Result<()> {
                Ok(())
            }
        }

        /// The real scheduler, its plans waiting while `hold` is set.
        #[derive(Default)]
        pub(crate) struct SlowPlan {
            pub hold: std::sync::atomic::AtomicBool,
            pub planning: std::sync::atomic::AtomicBool,
        }

        impl crate::daemon::Scheduler for SlowPlan {
            fn plan(
                &self,
                now: crate::types::Millis,
                machine: &crate::sched::MachineView,
                queue: &[crate::sched::Queued],
            ) -> crate::sched::Plan {
                if !queue.is_empty() {
                    self.planning.store(true, Ordering::SeqCst);
                    while self.hold.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                crate::sched::plan(now, machine, queue)
            }
        }

        pub(crate) struct Lone {
            pub paths: Paths,
            pub config: NodeConfig,
            pub opts: Options,
            pub exec: Arc<Runs>,
            /// Another executor than `exec`.
            pub executor: Option<Arc<dyn Executor>>,
            pub ws: Arc<dyn crate::daemon::WorkspaceProvider>,
            pub sched: Arc<dyn crate::daemon::Scheduler>,
        }

        impl Lone {
            pub fn new(dir: &Path) -> Lone {
                Lone {
                    paths: Paths {
                        home: dir.join("home"),
                        config: dir.join("node.toml"),
                    },
                    config: NodeConfig {
                        name: Some("lone".into()),
                        ..Default::default()
                    },
                    opts: Options {
                        potluck_bin: PathBuf::from("/nonexistent/potluck"),
                        tick_ms: 50,
                        kill_grace_ms: 600,
                        stall_ms: 0,
                        retention_ms: 14 * 86_400_000,
                        facts_refresh_ms: 600_000,
                        idle_ms: 600_000,
                    },
                    exec: Arc::new(Runs::default()),
                    executor: None,
                    sched: Arc::new(crate::daemon::RealScheduler),
                    ws: Arc::new(crate::workspace::Workspaces::new(Paths {
                        home: dir.join("home"),
                        config: dir.join("node.toml"),
                    })),
                }
            }

            pub fn parts(&self) -> Parts {
                Parts {
                    executor: self.executor.clone().unwrap_or_else(|| self.exec.clone()),
                    workspaces: self.ws.clone(),
                    scheduler: self.sched.clone(),
                    facts: Box::new(|| {
                        Ok(Facts {
                            node: "lone".into(),
                            os: std::env::consts::OS.into(),
                            ram_mib: 16 << 10,
                            ..Default::default()
                        })
                    }),
                    login_env: BTreeMap::new(),
                    clock: Arc::new(crate::daemon::SystemClock),
                }
            }

            pub fn start(&self) -> Daemon {
                Daemon::start(&self.paths, &self.config, self.parts(), self.opts.clone()).unwrap()
            }

            /// The daemon that starts from `inherited` in this process.
            pub fn start_from(&self, inherited: Inherited) -> Daemon {
                Daemon::start_from(
                    &self.paths,
                    &self.config,
                    self.parts(),
                    self.opts.clone(),
                    None,
                    inherited,
                )
                .unwrap()
            }
        }

        /// Job `id` in `state`; no machine can run it (`--on node=elsewhere`).
        pub(crate) fn job(id: &str, state: JobState) -> JobRecord {
            let spec = crate::types::JobSpec {
                key: None,
                labels: BTreeMap::new(),
                priority: 3,
                request: Default::default(),
                on: [("node".to_string(), "elsewhere".to_string())].into(),
                repo: crate::types::RepoRef {
                    id: "proj-1234abcd".into(),
                    source: PathBuf::from("/nonexistent"),
                    commit: "0".repeat(40),
                    branch: None,
                    submodules: Vec::new(),
                    carried: Vec::new(),
                },
                workspace: None,
                cmd: vec!["true".into()],
                env: BTreeMap::new(),
                unset_env: Default::default(),
                timeout_ms: 600_000,
                hold_ms: None,
                keep: Vec::new(),
                rerun_if_lost: true,
                kill_on_stall: false,
                recipe: None,
                submitted_from: "lone".into(),
            };
            let mut j = crate::daemon::runner::new_record(id.into(), spec, crate::types::now_ms());
            j.state = state;
            j
        }

        fn stored(paths: &Paths, id: &str) -> Option<JobRecord> {
            crate::store::Store::open(&paths.db())
                .unwrap()
                .job(id)
                .unwrap()
        }

        /// Writes `line` up to `at` on a new connection to the daemon: a request in flight.
        fn half_sent(sh: &Shared, line: &[u8], at: usize) -> UnixStream {
            let mut conn = UnixStream::connect(sh.paths.socket()).unwrap();
            conn.write_all(&line[..at]).unwrap();
            // The daemon has read the start of it.
            std::thread::sleep(Duration::from_millis(200));
            conn
        }

        fn request(req: crate::proto::Request) -> Vec<u8> {
            let mut line = serde_json::to_vec(&crate::proto::ClientRequest {
                ctx: Default::default(),
                req,
            })
            .unwrap();
            line.push(b'\n');
            line
        }

        /// A cancel answered during the quiesce whose write fails (the disk filled after the
        /// drain saw everything written) keeps the restart from going on: the daemon starts
        /// again in its process with the cancel, writes it once it can, and the `potluck restart`
        /// that asked is told the restart waits. Before, the store closed with the job still
        /// queued in it, and the next binary (or the daemon serving on after a failed exec) ran
        /// a job its client had been told was cancelled.
        #[test]
        fn a_write_that_fails_during_the_quiesce_holds_the_restart_back_and_is_kept() {
            use std::io::{BufRead, BufReader};
            let dir = tempfile::tempdir().unwrap();
            let lone = Lone::new(dir.path());
            let d = lone.start();
            let sh = d.shared.clone();
            let id = "lone-77".to_string();
            {
                let mut st = sh.lock();
                st.jobs.insert(id.clone(), job(&id, JobState::Queued));
                st.save_job(&id);
            }
            let paths = sh.paths.clone();
            let asking = std::thread::spawn(move || {
                crate::client::Client::connect(&paths, Default::default())
                    .unwrap()
                    .call(crate::proto::Request::Restart { binary: None })
            });
            assert!(d.wait_restart().is_some());
            d.restart_verdict(Ok(()));
            d.drain(Duration::from_secs(5)).unwrap();
            let line = request(crate::proto::Request::Cancel { job: id.clone() });
            let mut conn = half_sent(&sh, &line, 10);
            let handing = std::thread::spawn(move || d.hand_over(Duration::from_secs(5)));
            while !sh.up.quiescing() {
                std::thread::sleep(Duration::from_millis(5));
            }
            // The disk fills: the store takes no writes (as once closed).
            sh.lock().store.close();
            conn.write_all(&line[10..]).unwrap();
            let mut told = String::new();
            BufReader::new(&conn).read_line(&mut told).unwrap();
            assert!(told.contains(r#""state":"cancelled""#), "{told}");
            let inherited = handing.join().unwrap();
            let back = crate::daemon::close_for_exec(&sh, inherited, Vec::new)
                .err()
                .expect("the restart doesn't go on");
            let waits = back.waits.clone().unwrap();
            assert!(waits.contains("job lone-77"), "{waits}");
            // What `run` does then: the daemon starts again in this process from what it handed
            // over, with what it held. (Here the store takes writes again at once.)
            let again = lone.start_from(*back);
            assert_eq!(
                again.shared.lock().jobs.get(&id).map(|j| j.state),
                Some(JobState::Cancelled)
            );
            assert_eq!(
                stored(&sh.paths, &id).map(|j| j.state),
                Some(JobState::Cancelled)
            );
            match asking.join().unwrap() {
                Err(e) => assert_eq!(e.to_string(), waits),
                other => panic!("{other:?}"),
            }
            again.stop();
        }

        /// Job `id` starting here in a new workspace `w` (seeding it), stored.
        fn starting(st: &mut crate::daemon::state::State, id: &str, w: &str) {
            use crate::daemon::state::{Phase, Run};
            let now = crate::types::now_ms();
            st.workspaces.insert(
                w.to_string(),
                crate::types::WorkspaceRecord {
                    id: w.to_string(),
                    node: "lone".into(),
                    repo_id: "proj-1234abcd".into(),
                    commit: "0".repeat(40),
                    branch: None,
                    path: PathBuf::new(),
                    state: crate::types::WorkspaceState::Seeding,
                    created_at: now,
                    last_used_at: now,
                    held_until: 0,
                    pending: 0,
                    size_bytes: None,
                },
            );
            let mut j = job(id, JobState::Starting);
            j.spec.on.clear();
            j.node = Some("lone".into());
            j.workspace = Some(w.to_string());
            j.lease = Some(crate::types::Lease {
                grant: Default::default(),
                granted_at: now,
                deadline: now + 600_000,
                token: "t".into(),
            });
            st.jobs.insert(id.to_string(), j);
            st.runs.insert(
                id.to_string(),
                Run {
                    phase: Phase::Starting,
                    new_workspace: true,
                    ..Default::default()
                },
            );
            st.save_ws(w);
            st.save_job(id);
            st.save_run(id);
        }

        /// A cancel of a job being started, asked during the quiesce, is kept in the job's run
        /// record; its write fails. The daemon started again in this process reads that record
        /// as held, not as stored, and the job ends cancelled instead of being started again.
        #[test]
        fn a_run_record_that_cant_be_written_is_read_as_held_by_the_daemon_started_again() {
            let dir = tempfile::tempdir().unwrap();
            let lone = Lone::new(dir.path());
            let d = lone.start();
            let sh = d.shared.clone();
            let id = "lone-80".to_string();
            starting(&mut sh.lock(), &id, "lone-w1");
            d.drain(Duration::from_secs(5)).unwrap();
            let inherited = d.hand_over(Duration::from_secs(5));
            {
                // As a cancel answered during the quiesce, once the disk is full.
                let mut st = sh.lock();
                st.store.close();
                st.runs.get_mut(&id).unwrap().cancel = true;
                st.save_run(&id);
            }
            let back = crate::daemon::close_for_exec(&sh, inherited, Vec::new)
                .err()
                .expect("the restart doesn't go on");
            assert!(back.waits.as_deref().unwrap().contains("run of lone-80"));
            let again = lone.start_from(*back);
            let start = Instant::now();
            let job = loop {
                let j = again.shared.lock().jobs.get(&id).cloned();
                if let Some(j) = j.as_ref().filter(|j| j.state.is_done()) {
                    break j.clone();
                }
                assert!(start.elapsed() < Duration::from_secs(10), "{j:?}");
                std::thread::sleep(Duration::from_millis(20));
            };
            assert_eq!(
                (job.state, job.exit_code),
                (JobState::Cancelled, Some(crate::types::EXIT_CANCELLED)),
                "{job:?}"
            );
            again.stop();
        }

        /// A job whose workspace is still being seeded when the drain is over: the hand-over ends
        /// the seeding's git, and the job's thread, seeing it fail, would write the job failed.
        /// The state is locked first, so the store closes with the job starting, and the next
        /// binary seeds it again and runs it. Before, the thread had 500 ms to write "could not
        /// seed workspace lone-w1: git worktree add: signal: 15 (SIGTERM)" before the store
        /// closed, and the job was failed by the upgrade.
        #[test]
        fn a_seeding_cut_short_by_a_restart_in_place_is_done_again_not_failed() {
            let dir = tempfile::tempdir().unwrap();
            let mut lone = Lone::new(dir.path());
            let seed = Arc::new(SlowSeed::default());
            lone.ws = seed.clone();
            let d = lone.start();
            let sh = d.shared.clone();
            let id = "lone-79".to_string();
            starting(&mut sh.lock(), &id, "lone-w1");
            let start = id.clone();
            sh.spawn_worker(&format!("start-{id}"), move |sh| {
                crate::daemon::runner::start_job(sh, &start)
            });
            while seed.child.lock().unwrap().is_none() {
                std::thread::sleep(Duration::from_millis(5));
            }
            // The drain doesn't wait long enough for it.
            d.drain(Duration::from_millis(300)).unwrap();
            let inherited = d.hand_over(Duration::from_millis(300));
            // The children of the daemon's work end (here this test's own child only: the test
            // process runs other tests' beside it), and the seeding's thread sees its git fail.
            let pid = seed.child.lock().unwrap().unwrap();
            let end = || {
                unsafe { libc::kill(pid, libc::SIGTERM) };
                std::thread::sleep(Duration::from_millis(500));
                Vec::new()
            };
            let (st, inherited) = crate::daemon::close_for_exec(&sh, inherited, end)
                .ok()
                .expect("everything is written");
            // Until the exec.
            std::thread::sleep(Duration::from_millis(300));
            let rec = stored(&sh.paths, &id).unwrap();
            assert_eq!(rec.state, JobState::Starting, "{rec:?}");
            drop(st);
            let again = lone.start_from(inherited);
            let started = Instant::now();
            loop {
                let state = again.shared.lock().jobs.get(&id).map(|j| j.state);
                if state == Some(JobState::Running) {
                    break;
                }
                assert!(started.elapsed() < Duration::from_secs(10), "{state:?}");
                std::thread::sleep(Duration::from_millis(20));
            }
            again.stop();
        }

        /// A job being started is cancelled during the quiesce, and ends: neither its end nor
        /// the removal of its run record can be written (the disk is full), so the restart doesn't
        /// go on. The daemon started again in this process, with the disk still full, reads the
        /// job as ended and the stored run record as left over: removing that record fails too,
        /// and must not keep the daemon from starting. Before, the start failed ("attempt to
        /// write a readonly database" here, "database or disk is full" on a full disk) and the
        /// process ended with what it held.
        #[test]
        fn the_daemon_started_again_starts_while_its_store_takes_no_writes() {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().unwrap();
            let lone = Lone::new(dir.path());
            let d = lone.start();
            let sh = d.shared.clone();
            let id = "lone-81".to_string();
            starting(&mut sh.lock(), &id, "lone-w1");
            d.drain(Duration::from_secs(5)).unwrap();
            let inherited = d.hand_over(Duration::from_secs(5));
            {
                // Cancelled and ended during the quiesce, once the disk is full.
                let mut st = sh.lock();
                st.store.close();
                let j = st.jobs.get_mut(&id).unwrap();
                j.state = JobState::Cancelled;
                j.exit_code = Some(crate::types::EXIT_CANCELLED);
                st.save_job(&id);
                st.runs.remove(&id);
                st.save_run(&id);
            }
            let back = crate::daemon::close_for_exec(&sh, inherited, Vec::new)
                .err()
                .expect("the restart doesn't go on");
            // Still full: the store opens and reads, and takes no writes (a connection opened so
            // stays read-only).
            let db = sh.paths.db();
            let mode = |m| std::fs::set_permissions(&db, std::fs::Permissions::from_mode(m));
            mode(0o444).unwrap();
            let again = Daemon::start_from(
                &lone.paths,
                &lone.config,
                lone.parts(),
                lone.opts.clone(),
                None,
                *back,
            );
            mode(0o644).unwrap();
            let again = again.unwrap();
            assert_eq!(
                again.shared.lock().jobs.get(&id).map(|j| j.state),
                Some(JobState::Cancelled)
            );
            // Held, to be written once the store takes writes.
            let unsaved = again.shared.lock().unsaved.clone();
            for what in [Unsaved::Job(id.clone()), Unsaved::Run(id.clone())] {
                assert!(unsaved.contains(&what), "{what} not in {unsaved:?}");
            }
            again.stop();
        }

        /// A dispatch still going when the quiesce's 10 s are up (its mirror sync is long) is cut
        /// off, and the exec fails: the daemon serving on starts from the store, which the
        /// dispatch's job never reached. The client gets no answer, as when the exec works.
        /// Before, the thread of the daemon that handed over went on and answered "Dispatched
        /// lone-1" for a job no daemon has.
        #[test]
        fn a_request_cut_off_by_the_quiesce_is_not_answered_by_the_daemon_that_handed_over() {
            let dir = tempfile::tempdir().unwrap();
            let mut lone = Lone::new(dir.path());
            let seed = Arc::new(SlowSeed::default());
            seed.hold_sync.store(true, Ordering::SeqCst);
            lone.ws = seed.clone();
            let d = lone.start();
            let sh = d.shared.clone();
            let mut spec = job("x", JobState::Queued).spec;
            spec.on.clear();
            let paths = sh.paths.clone();
            let dispatching = std::thread::spawn(move || {
                crate::client::Client::connect(&paths, Default::default())
                    .unwrap()
                    .call(crate::proto::Request::Dispatch {
                        spec: Box::new(spec),
                    })
            });
            while !seed.syncing.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
            d.drain(Duration::from_secs(1)).unwrap();
            let inherited = d.hand_over(Duration::from_secs(1));
            let (st, mut inherited) = crate::daemon::close_for_exec(&sh, inherited, Vec::new)
                .ok()
                .expect("everything is written");
            // The exec fails (as in `exec_in_place`).
            drop(st);
            inherited.failed = Some("starting /nonexistent/potluck: No such file".into());
            let again = lone.start_from(inherited);
            seed.hold_sync.store(false, Ordering::SeqCst);
            let told = dispatching.join().unwrap();
            assert!(told.is_err(), "{told:?}");
            assert!(again.shared.lock().jobs.is_empty());
            let log = std::fs::read_to_string(lone.paths.daemon_log()).unwrap();
            assert!(log.contains("1 request still going cut off"), "{log}");
            again.stop();
        }

        /// A job's start waits through the hand-over with no child process to end (its seeding
        /// waits for a mirror's lock, say), and the exec fails: the daemon serving on starts the
        /// job again from the store. The thread of the daemon that handed over, when its seeding
        /// goes on, doesn't start it too. Before, both did ("lone-82: started in lone-w1" twice,
        /// two launches): one job running twice, the first unknown to any daemon (on Linux the
        /// second launch fails instead, its unit's name taken, and the job fails).
        #[test]
        fn a_start_left_going_by_a_restart_that_didnt_go_on_doesnt_start_the_job_twice() {
            let dir = tempfile::tempdir().unwrap();
            let mut lone = Lone::new(dir.path());
            let seed = Arc::new(SlowSeed::default());
            seed.hold_create.store(true, Ordering::SeqCst);
            lone.ws = seed.clone();
            let d = lone.start();
            let sh = d.shared.clone();
            let id = "lone-82".to_string();
            starting(&mut sh.lock(), &id, "lone-w1");
            let start = id.clone();
            sh.spawn_worker(&format!("start-{id}"), move |sh| {
                crate::daemon::runner::start_job(sh, &start)
            });
            while seed.creating.load(Ordering::SeqCst) == 0 {
                std::thread::sleep(Duration::from_millis(5));
            }
            d.drain(Duration::from_millis(300)).unwrap();
            let inherited = d.hand_over(Duration::from_millis(300));
            let (st, mut inherited) = crate::daemon::close_for_exec(&sh, inherited, Vec::new)
                .ok()
                .expect("everything is written");
            // The exec fails (as in `exec_in_place`).
            drop(st);
            inherited.failed = Some("starting /nonexistent/potluck: No such file".into());
            let again = lone.start_from(inherited);
            // Both seedings wait; then they go on.
            while seed.creating.load(Ordering::SeqCst) < 2 {
                std::thread::sleep(Duration::from_millis(5));
            }
            seed.hold_create.store(false, Ordering::SeqCst);
            let started = Instant::now();
            while again.shared.lock().jobs[&id].state != JobState::Running {
                assert!(started.elapsed() < Duration::from_secs(10));
                std::thread::sleep(Duration::from_millis(20));
            }
            // The other thread has had its time.
            std::thread::sleep(Duration::from_millis(500));
            assert_eq!(lone.exec.1.load(Ordering::SeqCst), 1);
            again.stop();
        }

        /// A plan made as a drain begins (it started before, the state locked) starts its job:
        /// the drain waits for the plan, then for the job's seeding, as for any work already
        /// going. Before, the drain ended at once (nothing was busy yet), and the plan's start
        /// was then refused, being made during a drain: the job stayed starting with nothing
        /// starting it, for good if the drain was undone (a record that can't be written).
        #[test]
        fn a_plan_made_as_a_drain_begins_is_waited_for_and_its_job_started() {
            let dir = tempfile::tempdir().unwrap();
            let mut lone = Lone::new(dir.path());
            let seed = Arc::new(SlowSeed::default());
            seed.hold_create.store(true, Ordering::SeqCst);
            lone.ws = seed.clone();
            let plans = Arc::new(SlowPlan::default());
            plans.hold.store(true, Ordering::SeqCst);
            lone.sched = plans.clone();
            let d = Arc::new(lone.start());
            let sh = d.shared.clone();
            let id = "lone-83".to_string();
            {
                let mut st = sh.lock();
                let mut j = job(&id, JobState::Queued);
                j.spec.on.clear();
                st.jobs.insert(id.clone(), j);
                st.save_job(&id);
            }
            sh.kick();
            while !plans.planning.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
            let draining = {
                let d = d.clone();
                std::thread::spawn(move || d.drain(Duration::from_secs(10)))
            };
            std::thread::sleep(Duration::from_millis(300));
            assert!(
                !draining.is_finished(),
                "the drain didn't wait for the plan"
            );
            plans.hold.store(false, Ordering::SeqCst);
            while seed.creating.load(Ordering::SeqCst) == 0 {
                assert!(
                    !draining.is_finished(),
                    "the drain didn't wait for the start"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            std::thread::sleep(Duration::from_millis(300));
            assert!(
                !draining.is_finished(),
                "the drain didn't wait for the seeding"
            );
            seed.hold_create.store(false, Ordering::SeqCst);
            draining.join().unwrap().unwrap();
            assert_eq!(sh.lock().jobs[&id].state, JobState::Running);
            sh.up.set_phase_serving();
            drop(sh);
            Arc::try_unwrap(d).ok().unwrap().stop();
        }

        /// The last save of a running job's record before the store closes (its usage, and on
        /// macOS the processes it started that left its session) fails: the restart doesn't go
        /// on, and the daemon started again has the record as it was held. Before, the next
        /// binary started from the record saved seconds earlier: here 1 process tracked of 14.
        #[test]
        fn a_running_jobs_last_save_that_fails_holds_the_restart_back_and_is_kept() {
            let dir = tempfile::tempdir().unwrap();
            let lone = Lone::new(dir.path());
            let d = lone.start();
            let sh = d.shared.clone();
            let id = "lone-78".to_string();
            {
                let mut st = sh.lock();
                let mut j = job(&id, JobState::Running);
                j.spec.on.clear();
                j.node = Some("lone".into());
                j.lease = Some(crate::types::Lease {
                    grant: Default::default(),
                    granted_at: crate::types::now_ms(),
                    deadline: crate::types::now_ms() + 600_000,
                    token: "t".into(),
                });
                j.handle = Some(Handle::Process {
                    pid: 4_000_000,
                    start_time: 0,
                    tracked: Vec::new(),
                    gone: Vec::new(),
                    lease: String::new(),
                    tasks_max: 0,
                    boot: String::new(),
                });
                st.jobs.insert(id.clone(), j);
                st.runs.insert(
                    id.clone(),
                    crate::daemon::state::Run {
                        phase: crate::daemon::state::Phase::Running,
                        ..Default::default()
                    },
                );
                st.save_job(&id);
                st.save_run(&id);
            }
            // Sampled a few times: the first sample's record is stored, the rest are held.
            std::thread::sleep(Duration::from_millis(500));
            d.drain(Duration::from_secs(5)).unwrap();
            // A request in flight holds the quiesce until the disk is full.
            let line = request(crate::proto::Request::Ping);
            let mut conn = half_sent(&sh, &line, 5);
            let handing = std::thread::spawn(move || d.hand_over(Duration::from_secs(5)));
            while !sh.up.quiescing() {
                std::thread::sleep(Duration::from_millis(5));
            }
            sh.lock().store.close();
            conn.write_all(&line[5..]).unwrap();
            let inherited = handing.join().unwrap();
            let tracked = |j: &JobRecord| match &j.handle {
                Some(Handle::Process { tracked, .. }) => tracked.len(),
                _ => 0,
            };
            let held = tracked(&sh.lock().jobs[&id]);
            assert!(held > 1, "{held}");
            let back = crate::daemon::close_for_exec(&sh, inherited, Vec::new)
                .err()
                .expect("the restart doesn't go on");
            assert!(back.waits.as_deref().unwrap().contains("job lone-78"));
            let again = lone.start_from(*back);
            let kept = tracked(&again.shared.lock().jobs[&id]);
            assert!(kept >= held, "{kept} < {held}");
            assert!(stored(&sh.paths, &id).is_some_and(|j| tracked(&j) >= held));
            again.stop();
        }

        fn git(args: &[&str]) -> String {
            let out = std::process::Command::new("git")
                .args(["-c", "user.name=t", "-c", "user.email=t@example.invalid"])
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?}: {out:?}");
            String::from_utf8(out.stdout).unwrap().trim().to_string()
        }

        /// Waits up to `limit` for `f`; whether it came true.
        fn within(limit: Duration, mut f: impl FnMut() -> bool) -> bool {
            let until = Instant::now() + limit;
            while !f() {
                if Instant::now() >= until {
                    return false;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            true
        }

        /// A queued job whose workspace is gone fails, and its commits are unpinned. The unpin
        /// waits for the mirror's lock, which a sync holds for as long as it fetches (minutes
        /// for a big repository): it waits on a worker, and the loop goes on meanwhile. Before,
        /// the loop waited in it: no job started and no running one was sampled for the whole
        /// sync, a drain waited for that plan (up to a minute), and a restart in place then
        /// waited for the loop with every connection in the backlog, for the rest of the sync.
        #[test]
        fn the_loop_goes_on_while_a_mirror_sync_holds_up_an_unpin() {
            use std::os::fd::AsRawFd;
            let dir = tempfile::tempdir().unwrap();
            let lone = Lone::new(dir.path());
            let d = lone.start();
            let sh = d.shared.clone();
            let mirror = sh.paths.mirror("proj-1234abcd");
            let m = mirror.to_str().unwrap();
            git(&["init", "-q", "--bare", m]);
            let tree = git(&["-C", m, "hash-object", "-t", "tree", "-w", "/dev/null"]);
            let commit = git(&["-C", m, "commit-tree", &tree, "-m", "pinned"]);
            git(&["-C", m, "update-ref", "refs/potluck/pins/lone-90", &commit]);
            let pins = || git(&["-C", m, "for-each-ref", "refs/potluck/pins/"]);
            // A sync of the mirror holds its lock, as `sync_mirror` does while it fetches.
            let sync = std::fs::File::create(mirror.with_extension("lock")).unwrap();
            // SAFETY: flock on a file this test holds open.
            assert_eq!(unsafe { libc::flock(sync.as_raw_fd(), libc::LOCK_EX) }, 0);
            let orphan = "lone-90".to_string();
            {
                let mut st = sh.lock();
                let mut j = job(&orphan, JobState::Queued);
                j.spec.on.clear();
                j.spec.workspace = Some("lone-w9".into());
                st.jobs.insert(orphan.clone(), j);
                st.save_job(&orphan);
            }
            sh.kick();
            assert!(within(Duration::from_secs(5), || sh.lock().jobs[&orphan]
                .state
                == JobState::Failed));
            // A job dispatched meanwhile is planned at once (its seeding then waits for the sync,
            // on its own worker).
            let next = "lone-91".to_string();
            {
                let mut st = sh.lock();
                let mut j = job(&next, JobState::Queued);
                j.spec.on.clear();
                st.jobs.insert(next.clone(), j);
                st.save_job(&next);
            }
            sh.kick();
            let planned = within(Duration::from_secs(3), || {
                sh.lock().jobs[&next].state != JobState::Queued
            });
            let plans = sh.up.plans();
            // The sync ends: the pin goes.
            drop(sync);
            assert!(
                planned,
                "a job dispatched while a sync held up an unpin wasn't planned (the loop waits \
                 in the unpin: {plans})"
            );
            assert!(within(Duration::from_secs(5), || !pins().contains("lone-90")));
            d.stop();
        }

        /// Job `id` running here, stored.
        fn running(st: &mut crate::daemon::state::State, id: &str) {
            let mut j = job(id, JobState::Running);
            j.spec.on.clear();
            j.node = Some("lone".into());
            j.lease = Some(crate::types::Lease {
                grant: Default::default(),
                granted_at: crate::types::now_ms(),
                deadline: crate::types::now_ms() + 600_000,
                token: "t".into(),
            });
            j.handle = Some(Handle::Process {
                pid: 4_000_000,
                start_time: 0,
                tracked: Vec::new(),
                gone: Vec::new(),
                lease: String::new(),
                tasks_max: 0,
                boot: String::new(),
            });
            st.jobs.insert(id.to_string(), j);
            st.runs.insert(
                id.to_string(),
                crate::daemon::state::Run {
                    phase: crate::daemon::state::Phase::Running,
                    ..Default::default()
                },
            );
            st.save_job(id);
            st.save_run(id);
        }

        /// A `potluck restart` on a raw connection, asked: (the connection, why it was asked).
        fn ask_restart(d: &Daemon) -> (UnixStream, String) {
            let mut asker = UnixStream::connect(d.shared.paths.socket()).unwrap();
            asker
                .write_all(&request(crate::proto::Request::Restart { binary: None }))
                .unwrap();
            let why = d.wait_restart().unwrap();
            (asker, why)
        }

        /// Whether anything was sent on `conn` yet (looked at, not read).
        fn told_yet(conn: &UnixStream) -> bool {
            let mut b = 0u8;
            // SAFETY: recv(2) into one byte this function owns.
            let n = unsafe {
                libc::recv(
                    conn.as_raw_fd(),
                    (&raw mut b).cast(),
                    1,
                    libc::MSG_PEEK | libc::MSG_DONTWAIT,
                )
            };
            n != -1 || std::io::Error::last_os_error().kind() != std::io::ErrorKind::WouldBlock
        }

        fn processes_tracked(j: &JobRecord) -> usize {
            match &j.handle {
                Some(Handle::Process { tracked, .. }) => tracked.len(),
                _ => 0,
            }
        }

        /// A plain restart (the binary at the daemon's path can't take a handover) goes through
        /// the gates of one in place: the last save of a running job before the store closes
        /// fails (the disk filled after the drain saw everything written), so there's no exec;
        /// the daemon starts again in its process with the record as it held it, and the
        /// `potluck restart` that asked is told the restart waits. Before, the plain restart
        /// stopped and executed the binary whatever its writes did, and the next daemon started
        /// from the record saved earlier (1 process tracked of 9), having told the asker it
        /// restarted plainly before it stopped.
        #[test]
        fn a_write_that_fails_before_a_plain_restart_holds_it_back_and_is_kept() {
            use std::io::{BufRead, BufReader};
            let dir = tempfile::tempdir().unwrap();
            let lone = Lone::new(dir.path());
            let d = lone.start();
            let sh = d.shared.clone();
            let id = "lone-78";
            running(&mut sh.lock(), id);
            // Sampled a few times: the first sample's record is stored, the rest are held.
            std::thread::sleep(Duration::from_millis(500));
            let (asker, why) = ask_restart(&d);
            // What `serve` does with a binary that can't take a handover: drains, goes on.
            let drained = d.drain(Duration::from_secs(5)).unwrap();
            // A request in flight holds the quiesce until the disk is full.
            let line = request(crate::proto::Request::Ping);
            let mut conn = half_sent(&sh, &line, 5);
            let bin = PathBuf::from("/nonexistent/potluck");
            let going = std::thread::spawn(move || {
                go_on(d, bin, why, drained, Some("Restarted plainly".into()))
            });
            while !sh.up.quiescing() {
                std::thread::sleep(Duration::from_millis(5));
            }
            sh.lock().store.close();
            conn.write_all(&line[5..]).unwrap();
            let Ending::Plain {
                bin,
                inherited,
                shared,
                told,
                ..
            } = going.join().unwrap()
            else {
                panic!("not a plain restart");
            };
            let held = processes_tracked(&sh.lock().jobs[id]);
            assert!(held > 1, "{held}");
            let back =
                crate::daemon::exec_plainly(&shared, &bin, inherited, &told, Vec::new, || {
                    panic!("executed")
                });
            let waits = back.waits.clone().unwrap_or_default();
            assert!(
                waits.contains("the plain restart waits") && waits.contains("job lone-78"),
                "{waits:?} {:?}",
                back.failed
            );
            assert!(!told_yet(&asker));
            let again = lone.start_from(back);
            let mut said = String::new();
            BufReader::new(&asker).read_line(&mut said).unwrap();
            assert!(
                said.contains("the plain restart waits") && !said.contains(&told),
                "{said}"
            );
            let kept = processes_tracked(&again.shared.lock().jobs[id]);
            assert!(kept >= held, "{kept} < {held}");
            assert!(stored(&sh.paths, id).is_some_and(|j| processes_tracked(&j) >= held));
            again.stop();
        }

        /// The `potluck restart` that asks for a plain restart is told it restarted only once
        /// the restart goes ahead: past the drain, the quiesce and the last writes, at the exec
        /// (here a stand-in that fails, as an exec of a binary replaced at that moment would).
        /// One whose binary can't be executed any more by then is told that instead, and the
        /// daemon serves on. Before, it was told as the stop began, and an exec that failed then
        /// ended the daemon.
        #[test]
        fn the_asker_of_a_plain_restart_is_told_once_it_goes_ahead() {
            use std::io::{BufRead, BufReader};
            for gone in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let lone = Lone::new(dir.path());
                let d = lone.start();
                let sh = d.shared.clone();
                let (asker, why) = ask_restart(&d);
                let drained = d.drain(Duration::from_secs(5)).unwrap();
                let bin = match gone {
                    false => PathBuf::from("/bin/sh"),
                    true => dir.path().join("gone"),
                };
                let ending = go_on(d, bin, why, drained, Some("Restarted plainly.".into()));
                let Ending::Plain {
                    bin,
                    inherited,
                    shared,
                    told,
                    ..
                } = ending
                else {
                    panic!("not a plain restart");
                };
                assert!(sh.up.quiescing());
                assert!(!told_yet(&asker), "told before it went ahead");
                let execs = std::sync::atomic::AtomicU32::new(0);
                let back =
                    crate::daemon::exec_plainly(&shared, &bin, inherited, &told, Vec::new, || {
                        execs.fetch_add(1, Ordering::SeqCst);
                        assert!(told_yet(&asker), "executed before it told");
                        std::io::Error::from_raw_os_error(libc::ETXTBSY)
                    });
                assert_eq!(execs.load(Ordering::SeqCst), u32::from(!gone));
                assert!(back.waits.is_none(), "{:?}", back.waits);
                let failed = back.failed.clone().unwrap();
                let mut said = String::new();
                BufReader::new(&asker).read_line(&mut said).unwrap();
                if gone {
                    assert!(
                        said.contains("restarting plainly failed")
                            && said.contains(&failed)
                            && !said.contains(&told),
                        "{said}"
                    );
                } else {
                    assert!(said.contains(&told), "{said}");
                }
                // The daemon serves on, and the asker's connection with it.
                let again = lone.start_from(back);
                let mut asker = asker;
                asker
                    .write_all(&request(crate::proto::Request::Ping))
                    .unwrap();
                let mut pong = String::new();
                BufReader::new(&asker).read_line(&mut pong).unwrap();
                assert!(pong.contains("pong"), "{pong}");
                again.stop();
            }
        }

        /// The loop held up outside the state (here a signal to a job over its RAM limit, as
        /// `systemctl kill` waits while systemd is busy) doesn't hold up a restart in place: the
        /// handover waits for it at most [`LOOP_WAIT`], then goes on without it (it can't write
        /// once the store is closing: the state stays locked until the exec). Before, the
        /// handover waited for the loop with no limit, every connection in the backlog.
        #[test]
        fn a_loop_held_up_outside_the_state_holds_a_handover_up_for_a_while_only() {
            let dir = tempfile::tempdir().unwrap();
            let mut lone = Lone::new(dir.path());
            let signals = Arc::new(SlowSignal::default());
            signals.hold.store(true, Ordering::SeqCst);
            lone.executor = Some(signals.clone());
            let d = lone.start();
            let sh = d.shared.clone();
            running(&mut sh.lock(), "lone-79");
            assert!(within(Duration::from_secs(5), || signals
                .signalling
                .load(Ordering::SeqCst)));
            let began = Instant::now();
            let handing = std::thread::spawn(move || d.hand_over(Duration::from_secs(5)));
            let handed = within(LOOP_WAIT + Duration::from_secs(5), || handing.is_finished());
            signals.hold.store(false, Ordering::SeqCst);
            let took = began.elapsed();
            assert!(handed, "the handover waited for the loop for {took:?}");
            let again = lone.start_from(handing.join().unwrap());
            again.stop();
        }
    }

    #[test]
    fn wait_readable_sees_data_the_wake_and_the_time() {
        let up = Upgrade::new().unwrap();
        let (a, mut b) = UnixStream::pair().unwrap();
        let t = Some(Duration::from_millis(50));
        assert_eq!(
            wait_readable(a.as_raw_fd(), up.wake_fd(), t),
            Ready::TimedOut
        );
        b.write_all(b"x").unwrap();
        assert_eq!(wait_readable(a.as_raw_fd(), up.wake_fd(), t), Ready::Fd);
        up.wake();
        assert_eq!(wait_readable(a.as_raw_fd(), up.wake_fd(), t), Ready::Wake);
    }
}
