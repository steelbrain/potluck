//! `potluck _supervise [--env-file FILE] [--log-max BYTES] STATUS -- CMD...`: the first process of
//! every invocation.
//!
//! Its stdout and stderr are the invocation's log, opened for appending by the executor. It starts
//! CMD in a process group of its own, with stdin inherited (/dev/null) and stdout and stderr on one
//! pipe, copies what comes through the pipe into the log, waits for CMD, and writes an
//! [`ExitStatus`] to STATUS atomically (write, then rename; on a full disk it tries again every
//! second for a minute). The daemon reads STATUS, so exit codes survive a daemon restart and the
//! daemon needs no child of its own. If the supervisor is killed or gives up before writing
//! STATUS, the daemon records the invocation as ended without an exit status (saying so when the
//! disk is full). A command that can't be started ends with the shell's codes: 127 when it isn't
//! found, 126 otherwise, with the reason in the log.
//!
//! - **The log is never the command's own file.** Opening `/dev/stdout` (on Linux a reopening of
//!   the file behind descriptor 1, so `> /dev/stdout` would truncate it), `2>/dev/stdout` and
//!   `tee /dev/stdout` reach the pipe, and the log only ever grows.
//! - **The end.** Once CMD has exited, the supervisor reads what is left in the pipe, then writes
//!   STATUS: the log is complete by the time the daemon sees the status. Processes CMD left
//!   running may hold the pipe open; what they write in the next second is still read, and after
//!   that the supervisor writes STATUS without waiting for them (the daemon then kills them, and
//!   anything they write after the supervisor has exited fails with EPIPE or SIGPIPE).
//! - **Signals.** SIGINT, SIGTERM and SIGHUP meant for the whole invocation reach the supervisor
//!   too; it catches them, so it outlives CMD and records how CMD ended. CMD gets the default
//!   handling (caught signals are reset by exec). One that arrives before CMD exists is passed on
//!   to CMD once it does. On macOS the executor starts the supervisor with these signals blocked
//!   until its handlers are in place; on Linux, one that arrives in the few milliseconds before
//!   that ends the supervisor, and systemd then stops the unit. CMD's process group is not the
//!   supervisor's, so a job that signals its own group (`kill 0`, `kill -STOP 0`) doesn't reach
//!   the supervisor.
//! - **The log limit** (`--log-max`, node.toml `log_max_mib`). The log keeps the first BYTES of
//!   the output. Past them a line says so, and the rest is dropped except its last
//!   [`LOG_TAIL_BYTES`], which the supervisor holds in memory (in the job's RAM) and adds at the
//!   end after a line saying how many bytes were dropped. The pipe is always read, so the job never
//!   waits on the log. While output is dropped, the log's modification time still moves (at most
//!   once a second), which the daemon counts as output for stall checks. The first SIGINT, SIGTERM
//!   or SIGHUP that finds dropped output adds the tail at once, so a job killed at its run limit
//!   keeps it; a job killed outright (SIGKILL) loses what was held.
//! - **A log that can't be written** (a full disk): the pipe is still read, and what couldn't be
//!   written is counted; at the next write that works a line says how many bytes are missing.
//!
//! With `--env-file`, CMD gets exactly the environment in FILE ([`write_env_file`]'s format: each
//! `KEY=VALUE` ended by a NUL byte), and FILE is removed as soon as it has been read, whether or
//! not that worked. An executor passes the environment this way when a command line would show it
//! to other users (`/proc/PID/cmdline` is readable by everyone). A FILE that can't be read ends the
//! invocation with 126 and the reason in the log.

use crate::types::{ExitStatus, now_ms};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::Write;
use std::os::fd::{AsFd, AsRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// How much of the output past the log limit the log keeps: its last bytes, added at the end.
pub const LOG_TAIL_BYTES: usize = 16 << 20;
/// After CMD has exited, how long the supervisor goes on reading what processes it left write.
const DRAIN_LIMIT: Duration = Duration::from_secs(1);
/// While output is dropped, the log's modification time is set at most this often.
const TOUCH_EVERY: Duration = Duration::from_secs(1);
/// How often the supervisor looks whether the command has exited when nothing wakes it (in
/// `_supervise`, SIGCHLD does at once).
const CHECK_EVERY: Duration = Duration::from_millis(250);
/// The most read from the pipe at once.
const READ_BUF: usize = 64 << 10;
/// The signals that end an invocation, which the supervisor outlives.
const ENDING_SIGNALS: [i32; 3] = [libc::SIGINT, libc::SIGTERM, libc::SIGHUP];

/// The ending signals the supervisor has caught.
struct Signals {
    count: AtomicUsize,
    last: AtomicI32,
}

impl Signals {
    const fn new() -> Signals {
        Signals {
            count: AtomicUsize::new(0),
            last: AtomicI32::new(0),
        }
    }
}

/// The process's: its handler counts here.
static SIGNALS: Signals = Signals::new();
/// The write end of the process's [`Wake`] pipe, for the handler; -1 when there is none.
static WAKE_FD: AtomicI32 = AtomicI32::new(-1);

/// Runs CMD as the invocation's first process (the entry point of `_supervise`): its output goes
/// to this process's stdout.
pub fn run(
    status: &Path,
    env_file: Option<&Path>,
    log_max: Option<u64>,
    cmd: &[String],
) -> anyhow::Result<i32> {
    let wake = Wake::new()?;
    WAKE_FD.store(wake.w.as_raw_fd(), Ordering::SeqCst);
    install_handlers()?;
    let out = File::from(std::io::stdout().as_fd().try_clone_to_owned()?);
    let limit = Limit {
        max: log_max.unwrap_or(0),
        tail: LOG_TAIL_BYTES,
    };
    let result = supervise(out, status, env_file, limit, cmd, &wake, &SIGNALS);
    // The handler must never write to a descriptor that is closed, or reused for something else.
    WAKE_FD.store(-1, Ordering::SeqCst);
    result
}

/// How much output the log keeps: the first `max` bytes (0: all of it), then the last `tail`.
#[derive(Clone, Copy)]
struct Limit {
    max: u64,
    tail: usize,
}

/// Runs CMD with its output copied to `out`. `signals` counts the ending signals caught (the
/// handler wakes `wake` for each).
fn supervise(
    mut out: File,
    status: &Path,
    env_file: Option<&Path>,
    limit: Limit,
    cmd: &[String],
    wake: &Wake,
    signals: &Signals,
) -> anyhow::Result<i32> {
    let (prog, args) = cmd
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("_supervise needs a command"))?;
    let mut command = Command::new(prog);
    command.args(args);
    if let Some(file) = env_file {
        match read_env_file(file) {
            Ok(env) => {
                command.env_clear().envs(env);
            }
            Err(e) => {
                say(
                    &mut out,
                    &format!(
                        "could not read the job's environment from {}: {e}",
                        file.display()
                    ),
                );
                return cannot_start(status, 126);
            }
        }
    }
    let pipe = match std::io::pipe().and_then(|(r, w)| Ok((r, w.try_clone()?, w))) {
        Ok((r, w1, w2)) => {
            command.stdout(w1).stderr(w2);
            OwnedFd::from(r)
        }
        Err(e) => {
            say(
                &mut out,
                &format!("could not make a pipe for {prog}'s output: {e}"),
            );
            return cannot_start(status, 126);
        }
    };
    command.process_group(0);
    let spawned = command.spawn();
    // The pipe's write ends live in `command`: only the command's processes may hold them, so the
    // pipe ends when they are gone.
    drop(command);
    let mut child = match spawned {
        Ok(child) => child,
        Err(e) => {
            say(&mut out, &format!("could not start {prog}: {e}"));
            let code = if e.kind() == std::io::ErrorKind::NotFound {
                127
            } else {
                126
            };
            return cannot_start(status, code);
        }
    };
    // A signal caught before the command existed was meant for it.
    let seen = signals.count.load(Ordering::SeqCst);
    if seen > 0 {
        let sig = signals.last.load(Ordering::SeqCst);
        // SAFETY: kill has no memory preconditions; the pid is our own child, not yet reaped.
        unsafe { libc::kill(child.id() as libc::pid_t, sig) };
    }
    let _ = set_nonblocking(pipe.as_raw_fd());
    let mut sink = Sink::new(out, limit);
    let exit = copy(&pipe, wake, &mut sink, &mut child, signals, seen)?;
    write_status(
        status,
        &ExitStatus {
            code: exit.code(),
            signal: exit.signal(),
            ended_at: now_ms(),
        },
    )?;
    Ok(exit.code().unwrap_or(128 + exit.signal().unwrap_or(0)))
}

/// A line of the supervisor's own in the log. Never fails: a log that can't be written loses it.
fn say(out: &mut File, msg: &str) {
    let _ = writeln!(out, "potluck: {msg}");
}

fn cannot_start(status: &Path, code: i32) -> anyhow::Result<i32> {
    write_status(
        status,
        &ExitStatus {
            code: Some(code),
            signal: None,
            ended_at: now_ms(),
        },
    )?;
    Ok(code)
}

/// How often a status write that found the disk full is tried again, and for how long.
const STATUS_RETRY: Duration = Duration::from_secs(1);
const STATUS_PATIENCE: Duration = Duration::from_secs(60);

/// Writes STATUS atomically: a temporary file, then a rename. On a full disk (or over quota) it
/// tries again every second for a minute, saying so once in the log: only the supervisor knows
/// how the command ended, and until STATUS is written the daemon sees the invocation running.
/// (Setting room aside for it doesn't work: on a full APFS volume even overwriting a file's own
/// bytes can fail.)
fn write_status(status: &Path, record: &ExitStatus) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(record)?;
    let tmp = status.with_extension("json.tmp");
    let since = Instant::now();
    let mut told = false;
    loop {
        match std::fs::write(&tmp, &bytes).and_then(|()| std::fs::rename(&tmp, status)) {
            Ok(()) => return Ok(()),
            Err(e)
                if matches!(e.raw_os_error(), Some(libc::ENOSPC | libc::EDQUOT))
                    && since.elapsed() < STATUS_PATIENCE =>
            {
                if !told {
                    // Not eprintln: it panics when stderr can't be written, as on this full disk.
                    let _ = writeln!(
                        std::io::stderr(),
                        "potluck: could not record how the command ended in {}: {e}; trying \
                         again every second for a minute",
                        status.display()
                    );
                    told = true;
                }
                std::thread::sleep(STATUS_RETRY);
            }
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(anyhow::anyhow!(
                    "could not record how the command ended in {}: {e}",
                    status.display()
                ));
            }
        }
    }
}

/// Writes `env` for `--env-file` to a new file at `path` that only this user can read: each
/// `KEY=VALUE` ended by a NUL byte. A file already at `path` is replaced. Keys must be non-empty
/// and hold no `=` or NUL, values no NUL.
pub fn write_env_file(path: &Path, env: &BTreeMap<String, String>) -> std::io::Result<()> {
    let mut bytes = Vec::new();
    for (key, value) in env {
        if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid environment variable {key:?}"),
            ));
        }
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(b'=');
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0);
    }
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    let written = file.write_all(&bytes).and_then(|()| file.sync_all());
    if written.is_err() {
        let _ = std::fs::remove_file(path);
    }
    written
}

/// Reads an env file and removes it, also when reading fails.
fn read_env_file(path: &Path) -> std::io::Result<Vec<(OsString, OsString)>> {
    let read = std::fs::read(path);
    let removed = std::fs::remove_file(path);
    let bytes = read?;
    removed?;
    let mut env = Vec::new();
    for entry in bytes.split(|&b| b == 0).filter(|e| !e.is_empty()) {
        let Some(eq) = entry.iter().position(|&b| b == b'=').filter(|&i| i > 0) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "an entry is not KEY=VALUE",
            ));
        };
        env.push((
            OsString::from_vec(entry[..eq].to_vec()),
            OsString::from_vec(entry[eq + 1..].to_vec()),
        ));
    }
    Ok(env)
}

/// Wakes the copy loop: for each signal caught, and when the command exits (SIGCHLD).
/// Both ends are non-blocking, so the handler never waits on a full pipe.
struct Wake {
    r: OwnedFd,
    w: OwnedFd,
}

impl Wake {
    fn new() -> std::io::Result<Wake> {
        let (r, w) = std::io::pipe()?;
        let (r, w) = (OwnedFd::from(r), OwnedFd::from(w));
        set_nonblocking(r.as_raw_fd())?;
        set_nonblocking(w.as_raw_fd())?;
        Ok(Wake { r, w })
    }

    /// What a handler does.
    #[cfg(test)]
    fn notify(&self) {
        // SAFETY: writes one byte from a static buffer to a non-blocking descriptor we own.
        unsafe { libc::write(self.w.as_raw_fd(), b"t".as_ptr().cast(), 1) };
    }

    /// Empties the pipe.
    fn clear(&self) {
        let mut buf = [0u8; 64];
        // SAFETY: reads into a local buffer from a non-blocking descriptor we own.
        while unsafe { libc::read(self.r.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) } > 0 {}
    }
}

fn set_nonblocking(fd: RawFd) -> std::io::Result<()> {
    // SAFETY: F_GETFL and F_SETFL on a descriptor the caller owns.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn errno_ptr() -> *mut libc::c_int {
    // SAFETY: returns this thread's errno location.
    unsafe { libc::__errno_location() }
}

#[cfg(target_os = "macos")]
fn errno_ptr() -> *mut libc::c_int {
    // SAFETY: returns this thread's errno location.
    unsafe { libc::__error() }
}

/// Counts an ending signal and wakes the copier. Async-signal-safe: atomics and write(2), and
/// errno is left as it was.
extern "C" fn on_signal(sig: libc::c_int) {
    let errno = errno_ptr();
    // SAFETY: errno_ptr points at this thread's errno.
    let saved = unsafe { *errno };
    SIGNALS.count.fetch_add(1, Ordering::SeqCst);
    SIGNALS.last.store(sig, Ordering::SeqCst);
    let fd = WAKE_FD.load(Ordering::SeqCst);
    if fd >= 0 {
        // SAFETY: writes one byte from a static buffer to a non-blocking descriptor.
        unsafe { libc::write(fd, b"s".as_ptr().cast(), 1) };
    }
    // SAFETY: as above.
    unsafe { *errno = saved };
}

/// Wakes the copy loop when the command exits. Async-signal-safe, errno kept.
extern "C" fn on_child(_sig: libc::c_int) {
    let errno = errno_ptr();
    // SAFETY: errno_ptr points at this thread's errno.
    let saved = unsafe { *errno };
    let fd = WAKE_FD.load(Ordering::SeqCst);
    if fd >= 0 {
        // SAFETY: writes one byte from a static buffer to a non-blocking descriptor.
        unsafe { libc::write(fd, b"c".as_ptr().cast(), 1) };
    }
    // SAFETY: as above.
    unsafe { *errno = saved };
}

/// Catches the ending signals (a caught signal is reset to its default by exec, so the command
/// gets the default handling) and SIGCHLD, then lets through any ending signal the executor held
/// back until now.
fn install_handlers() -> std::io::Result<()> {
    // SAFETY: plain sigaction and sigset calls on local, initialized values; the handler is
    // async-signal-safe.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&raw mut set);
        for sig in ENDING_SIGNALS {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
            action.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&raw mut action.sa_mask);
            if libc::sigaction(sig, &raw const action, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            libc::sigaddset(&raw mut set, sig);
        }
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = on_child as extern "C" fn(libc::c_int) as libc::sighandler_t;
        action.sa_flags = libc::SA_RESTART | libc::SA_NOCLDSTOP;
        libc::sigemptyset(&raw mut action.sa_mask);
        if libc::sigaction(libc::SIGCHLD, &raw const action, std::ptr::null_mut()) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        libc::pthread_sigmask(libc::SIG_UNBLOCK, &raw const set, std::ptr::null_mut());
    }
    Ok(())
}

/// Copies the pipe into the sink until the command has exited, then what is left (see
/// [`drain`]), then the tail of dropped output; returns how the command ended. One thread does it
/// all: a thread that can't be created (process limits) must not cost the status. `seen` is how
/// many signals had been caught before the command started: the first one after that which finds
/// dropped output adds its tail at once.
fn copy(
    pipe: &OwnedFd,
    wake: &Wake,
    sink: &mut Sink,
    child: &mut std::process::Child,
    signals: &Signals,
    mut seen: usize,
) -> std::io::Result<std::process::ExitStatus> {
    let mut buf = vec![0u8; READ_BUF];
    // Until every write end is closed.
    let mut open = true;
    let mut flushed = false;
    let exit = loop {
        let caught = signals.count.load(Ordering::SeqCst);
        if caught != seen {
            seen = caught;
            if !flushed {
                flushed = sink.flush_tail();
            }
        }
        if let Some(exit) = child.try_wait()? {
            if open {
                drain(pipe, sink, &mut buf);
            }
            break exit;
        }
        let mut fds = [
            libc::pollfd {
                fd: wake.r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: pipe.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let n = if open { 2 } else { 1 };
        let wait = CHECK_EVERY.as_millis() as libc::c_int;
        // SAFETY: `n` pollfds from a local array.
        let r = unsafe { libc::poll(fds.as_mut_ptr(), n, wait) };
        if r < 0 {
            if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
                // Not expected; don't spin.
                std::thread::sleep(Duration::from_millis(10));
            }
            continue;
        }
        if fds[0].revents != 0 {
            wake.clear();
        }
        if open && fds[1].revents != 0 {
            match read(pipe, &mut buf) {
                Ok(0) => open = false,
                Ok(n) => sink.take(&buf[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) => {}
                Err(_) => open = false,
            }
        }
    };
    sink.flush_tail();
    sink.out.recover();
    Ok(exit)
}

/// After the command has exited: reads what is in the pipe until it is empty or at its end, or
/// for at most [`DRAIN_LIMIT`] while processes it left keep writing.
fn drain(pipe: &OwnedFd, sink: &mut Sink, buf: &mut [u8]) {
    let until = Instant::now() + DRAIN_LIMIT;
    loop {
        match read(pipe, buf) {
            Ok(0) => return,
            Ok(n) => sink.take(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Empty for now. Whatever the command wrote is in by now; a moment more lets a
                // process it left finish a write already under way.
                if Instant::now() >= until {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
                if read_ready(pipe) {
                    continue;
                }
                return;
            }
            Err(_) => return,
        }
        if Instant::now() >= until {
            return;
        }
    }
}

/// Whether the pipe has data, without waiting.
fn read_ready(pipe: &OwnedFd) -> bool {
    let mut fd = libc::pollfd {
        fd: pipe.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one pollfd; no wait.
    unsafe { libc::poll(&raw mut fd, 1, 0) > 0 && fd.revents & libc::POLLIN != 0 }
}

fn read(fd: &OwnedFd, buf: &mut [u8]) -> std::io::Result<usize> {
    // SAFETY: reads into a live buffer of `buf.len()` bytes.
    let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// The log as the copier writes to it: output that couldn't be written is counted, and said at
/// the next write that works.
struct Out {
    file: File,
    /// Output bytes that couldn't be written since the last line about it.
    lost: u64,
    /// Why, the last time.
    why: String,
    /// The last byte written ended a line (or nothing was written yet).
    line_start: bool,
}

impl Out {
    /// Writes all of `data`; on an error, how much was written and the error.
    fn put(&mut self, data: &[u8]) -> Result<(), (usize, std::io::Error)> {
        let mut done = 0;
        while done < data.len() {
            match self.file.write(&data[done..]) {
                Ok(0) => return Err((done, std::io::ErrorKind::WriteZero.into())),
                Ok(n) => {
                    done += n;
                    self.line_start = data[done - 1] == b'\n';
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err((done, e)),
            }
        }
        Ok(())
    }

    /// The command's output.
    fn write(&mut self, data: &[u8]) {
        if !self.recover() {
            self.lost += data.len() as u64;
            return;
        }
        if let Err((done, e)) = self.put(data) {
            self.lost += (data.len() - done) as u64;
            self.why = e.to_string();
        }
    }

    /// A line of potluck's own, on a line of its own.
    fn note(&mut self, text: &str) {
        if self.recover() {
            let nl = if self.line_start { "" } else { "\n" };
            let _ = self.put(format!("{nl}--- potluck: {text} ---\n").as_bytes());
        }
    }

    /// Says how much output couldn't be written, if any; false while the log still can't be
    /// written.
    fn recover(&mut self) -> bool {
        if self.lost == 0 {
            return true;
        }
        let nl = if self.line_start { "" } else { "\n" };
        let line = format!(
            "{nl}--- potluck: {} bytes of output are missing here: writing the log failed ({}) ---\n",
            self.lost, self.why
        );
        if self.put(line.as_bytes()).is_err() {
            return false;
        }
        self.lost = 0;
        true
    }
}

/// Where the copier puts the output: the log up to the limit, then the tail.
struct Sink {
    out: Out,
    limit: Limit,
    /// Output bytes that went to the log before the limit.
    head: u64,
    /// Output bytes past the limit since its tail was last written.
    past: u64,
    /// The last of them.
    ring: Ring,
    /// The line at the limit is written.
    marked: bool,
    touched: Option<Instant>,
}

impl Sink {
    fn new(file: File, limit: Limit) -> Sink {
        Sink {
            out: Out {
                file,
                lost: 0,
                why: String::new(),
                line_start: true,
            },
            limit,
            head: 0,
            past: 0,
            ring: Ring::new(limit.tail),
            marked: false,
            touched: None,
        }
    }

    fn take(&mut self, data: &[u8]) {
        let room = if self.limit.max == 0 {
            data.len()
        } else {
            usize::try_from(self.limit.max.saturating_sub(self.head))
                .unwrap_or(usize::MAX)
                .min(data.len())
        };
        let (keep, over) = data.split_at(room);
        if !keep.is_empty() {
            self.head += keep.len() as u64;
            self.out.write(keep);
        }
        if over.is_empty() {
            return;
        }
        if !self.marked {
            self.marked = true;
            self.out.note(&format!(
                "the output reached log_max_mib ({}): what follows is dropped, except its last {}, \
                 added when the command ends",
                size_text(self.limit.max),
                size_text(self.limit.tail as u64)
            ));
        }
        self.past += over.len() as u64;
        self.ring.push(over);
        // So the daemon still sees output (stall checks).
        if self.touched.is_none_or(|t| t.elapsed() >= TOUCH_EVERY) {
            // SAFETY: futimens on a descriptor we own; a null times pointer means now.
            unsafe { libc::futimens(self.out.file.as_raw_fd(), std::ptr::null()) };
            self.touched = Some(Instant::now());
        }
    }

    /// Writes the output held past the limit, after a line saying how much was dropped; true if
    /// there was any.
    fn flush_tail(&mut self) -> bool {
        if self.past == 0 {
            return false;
        }
        let kept = self.ring.len() as u64;
        self.out.note(&format!(
            "{} bytes of output dropped after the log reached log_max_mib ({}); the last {kept} \
             bytes follow",
            self.past - kept,
            size_text(self.limit.max)
        ));
        let (a, b) = self.ring.parts();
        self.out.write(a);
        self.out.write(b);
        self.ring.clear();
        self.past = 0;
        true
    }
}

/// `n` bytes as MiB when it is a whole number of them.
fn size_text(n: u64) -> String {
    if n > 0 && n.is_multiple_of(1 << 20) {
        format!("{} MiB", n >> 20)
    } else {
        format!("{n} bytes")
    }
}

/// The last `cap` bytes pushed.
struct Ring {
    buf: Vec<u8>,
    cap: usize,
    /// Where the oldest byte is, once `buf` is full.
    start: usize,
}

impl Ring {
    fn new(cap: usize) -> Ring {
        Ring {
            buf: Vec::new(),
            cap,
            start: 0,
        }
    }

    fn len(&self) -> usize {
        self.buf.len()
    }

    fn push(&mut self, mut data: &[u8]) {
        if self.cap == 0 {
            return;
        }
        if data.len() >= self.cap {
            self.buf.clear();
            self.buf.extend_from_slice(&data[data.len() - self.cap..]);
            self.start = 0;
            return;
        }
        if self.buf.len() < self.cap {
            let n = (self.cap - self.buf.len()).min(data.len());
            self.buf.extend_from_slice(&data[..n]);
            data = &data[n..];
        }
        // Full: overwrite from the oldest byte on, wrapping.
        let first = (self.cap - self.start).min(data.len());
        self.buf[self.start..self.start + first].copy_from_slice(&data[..first]);
        let second = data.len() - first;
        self.buf[..second].copy_from_slice(&data[first..]);
        self.start = (self.start + data.len()) % self.cap;
    }

    /// The bytes, oldest first, in two pieces.
    fn parts(&self) -> (&[u8], &[u8]) {
        (&self.buf[self.start..], &self.buf[..self.start])
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.start = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::os::unix::fs::PermissionsExt;

    /// The supervisor's core, as `_supervise` runs it but with no signal handlers (they would
    /// stay in this test process): output into `out.log` beside `status`, no limit.
    fn run(status: &Path, env_file: Option<&Path>, cmd: &[String]) -> i32 {
        let limit = Limit {
            max: 0,
            tail: LOG_TAIL_BYTES,
        };
        run_limited(status, env_file, limit, cmd)
    }

    fn run_limited(status: &Path, env_file: Option<&Path>, limit: Limit, cmd: &[String]) -> i32 {
        let log = status.with_file_name("out.log");
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .unwrap();
        let wake = Wake::new().unwrap();
        let signals = Signals::new();
        supervise(out, status, env_file, limit, cmd, &wake, &signals).unwrap()
    }

    fn sh(script: &str) -> Vec<String> {
        vec!["sh".into(), "-c".into(), script.into()]
    }

    fn read(status: &std::path::Path) -> crate::types::ExitStatus {
        serde_json::from_slice(&std::fs::read(status).unwrap()).unwrap()
    }

    #[test]
    fn records_exit_code_and_signal() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        let code = run(&status, None, &["sh".into(), "-c".into(), "exit 7".into()]);
        assert_eq!(code, 7);
        let s = read(&status);
        assert_eq!((s.code, s.signal), (Some(7), None));

        run(
            &status,
            None,
            &["sh".into(), "-c".into(), "kill -KILL $$".into()],
        );
        let s = read(&status);
        assert_eq!((s.code, s.signal), (None, Some(9)));
    }

    #[test]
    fn records_a_command_that_cannot_start() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        let missing = dir.path().join("no-such-command");
        let code = run(&status, None, &[missing.display().to_string()]);
        assert_eq!((code, read(&status).code), (127, Some(127)));

        let not_exec = dir.path().join("not-executable");
        std::fs::write(&not_exec, "#!/bin/sh\n").unwrap();
        let code = run(&status, None, &[not_exec.display().to_string()]);
        assert_eq!((code, read(&status).code), (126, Some(126)));
    }

    /// The command gets exactly the file's environment, not the supervisor's, and the file is
    /// gone once read.
    #[test]
    fn the_environment_comes_from_the_env_file() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        let file = dir.path().join("env");
        let out = dir.path().join("out");
        let env: BTreeMap<String, String> = [
            ("PATH", "/usr/bin:/bin"),
            ("SPACES", " a  b "),
            ("NEWLINE", "one\ntwo"),
            ("EQUALS", "x=y=z"),
            ("EMPTY", ""),
            ("UNICODE", "é→✓"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        write_env_file(&file, &env).unwrap();
        let mode = std::fs::metadata(&file).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let script = format!("env -0 > '{}'", out.display());
        let code = run(&status, Some(&file), &["sh".into(), "-c".into(), script]);
        assert_eq!(code, 0);
        assert!(!file.exists(), "the env file is left behind");
        let got: BTreeSet<String> = std::fs::read_to_string(&out)
            .unwrap()
            .split('\0')
            .filter(|e| !e.is_empty())
            // The shell may add its own (PWD, SHLVL, _).
            .filter(|e| !["PWD=", "SHLVL=", "_="].iter().any(|p| e.starts_with(p)))
            .map(String::from)
            .collect();
        let want: BTreeSet<String> = env.iter().map(|(k, v)| format!("{k}={v}")).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn a_bad_env_file_ends_the_invocation_and_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        let marker = dir.path().join("ran");
        let cmd = ["touch".to_string(), marker.display().to_string()];

        let missing = dir.path().join("missing");
        let code = run(&status, Some(&missing), &cmd);
        assert_eq!((code, read(&status).code), (126, Some(126)));

        let bad = dir.path().join("bad");
        std::fs::write(&bad, b"PATH=/bin\0no-equals-sign\0").unwrap();
        let code = run(&status, Some(&bad), &cmd);
        assert_eq!((code, read(&status).code), (126, Some(126)));
        assert!(!bad.exists(), "a bad env file is left behind");
        assert!(!marker.exists(), "the command ran");
    }

    #[test]
    fn env_files_refuse_what_an_environment_cannot_hold() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("env");
        for (k, v) in [("", "x"), ("A=B", "x"), ("A\0B", "x"), ("A", "x\0y")] {
            let env = BTreeMap::from([(k.to_string(), v.to_string())]);
            assert!(write_env_file(&file, &env).is_err(), "{k:?}={v:?}");
            assert!(!file.exists());
        }
        // An old file at the path is replaced.
        std::fs::write(&file, "OLD=1\0").unwrap();
        let env = BTreeMap::from([("NEW".to_string(), "2".to_string())]);
        write_env_file(&file, &env).unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"NEW=2\0");
    }

    /// A place to write `name` in where writing fails for want of space until `free` is called.
    #[cfg(target_os = "linux")]
    fn full_disk(dir: &std::path::Path, name: &str) -> Option<Box<dyn FnOnce() + Send>> {
        // Writes to /dev/full fail with ENOSPC.
        let link = dir.join(name);
        std::os::unix::fs::symlink("/dev/full", &link).ok()?;
        Some(Box::new(move || std::fs::remove_file(link).unwrap()))
    }

    /// A small APFS disk image attached at `dir` (detached by [`detach`]).
    #[cfg(target_os = "macos")]
    fn attach_image(dir: &std::path::Path) -> bool {
        use std::process::Command;
        let image = dir.with_extension("dmg");
        let ok = |c: &mut Command| c.status().is_ok_and(|s| s.success());
        ok(Command::new("hdiutil")
            .args(["create", "-quiet", "-size", "4m", "-fs", "APFS"])
            .args(["-volname", "potlucktest"])
            .arg(&image))
            && ok(Command::new("hdiutil")
                .args(["attach", "-quiet", "-nobrowse", "-noverify", "-mountpoint"])
                .arg(dir)
                .arg(&image))
    }

    /// Fills the disk holding `dir` with one file, to the last byte it takes; returns the file.
    #[cfg(target_os = "macos")]
    fn fill(dir: &std::path::Path) -> Option<std::path::PathBuf> {
        let fill = dir.join("fill");
        let mut f = std::fs::File::create(&fill).ok()?;
        for size in [64 << 10, 4 << 10, 512, 1] {
            let block = vec![0u8; size];
            while std::io::Write::write_all(&mut f, &block)
                .and_then(|()| f.sync_all())
                .is_ok()
            {}
        }
        Some(fill)
    }

    /// A small disk image filled up; `free` deletes what filled it.
    #[cfg(target_os = "macos")]
    fn full_disk(dir: &std::path::Path, _name: &str) -> Option<Box<dyn FnOnce() + Send>> {
        if !attach_image(dir) {
            return None;
        }
        let fill = fill(dir)?;
        Some(Box::new(move || std::fs::remove_file(fill).unwrap()))
    }

    #[cfg(target_os = "macos")]
    fn detach(dir: &std::path::Path) {
        let _ = std::process::Command::new("hdiutil")
            .args(["detach", "-quiet", "-force"])
            .arg(dir)
            .status();
        let _ = std::fs::remove_file(dir.with_extension("dmg"));
    }

    #[cfg(target_os = "linux")]
    fn detach(_dir: &std::path::Path) {}

    /// On a full disk the exit status isn't lost: it is written once there is room.
    #[test]
    fn the_status_waits_for_room_on_a_full_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("disk");
        std::fs::create_dir(&dir).unwrap();
        let status = dir.join("status.json");
        let Some(free) = full_disk(&dir, "status.json.tmp") else {
            eprintln!("no full disk at hand: skipped");
            return;
        };
        let freer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(1500));
            free();
        });
        let record = crate::types::ExitStatus {
            code: Some(3),
            signal: None,
            ended_at: 7,
        };
        let since = std::time::Instant::now();
        let written = super::write_status(&status, &record);
        let waited = since.elapsed();
        freer.join().unwrap();
        let got = std::fs::read(&status).ok();
        detach(&dir);
        written.unwrap();
        assert!(
            waited >= std::time::Duration::from_millis(1500),
            "{waited:?}"
        );
        let got: crate::types::ExitStatus = serde_json::from_slice(&got.unwrap()).unwrap();
        assert_eq!((got.code, got.ended_at), (Some(3), 7));
    }

    fn log_of(status: &Path) -> Vec<u8> {
        std::fs::read(status.with_file_name("out.log")).unwrap_or_default()
    }

    /// stdout and stderr reach the log through one pipe, in order. Opening /dev/stdout opens the
    /// pipe, not the log: on Linux, where it reopens the file behind descriptor 1, `> /dev/stdout`
    /// used to truncate the log.
    #[test]
    fn output_reaches_the_log_through_a_pipe() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        let script = "echo one; echo two >&2; echo three > /dev/stdout; \
                      echo four 2>/dev/stdout >&2; echo five | tee /dev/stdout; \
                      [ -p /dev/stdout ] && [ -p /dev/stderr ] && echo pipes";
        assert_eq!(run(&status, None, &sh(script)), 0);
        assert_eq!(
            String::from_utf8(log_of(&status)).unwrap(),
            "one\ntwo\nthree\nfour\nfive\nfive\npipes\n"
        );
    }

    /// The command leads a process group of its own, so `kill 0` in it doesn't reach the
    /// supervisor, which records how the command ended.
    #[test]
    fn the_command_has_a_process_group_of_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        let code = run(&status, None, &sh("ps -o pgid= -p $$ | tr -d ' '; echo $$"));
        assert_eq!(code, 0);
        let log = String::from_utf8(log_of(&status)).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(lines.len(), 2, "{log}");
        assert_eq!(lines[0], lines[1], "{log}");

        let code = run(&status, None, &sh("echo before; kill -KILL 0; echo after"));
        assert_eq!(code, 128 + libc::SIGKILL);
        let s = read(&status);
        assert_eq!((s.code, s.signal), (None, Some(libc::SIGKILL)));
    }

    /// A process the command leaves running keeps the pipe open; the status comes anyway, and
    /// the log has what the command wrote.
    #[test]
    fn a_process_left_holding_the_pipe_does_not_hold_up_the_status() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        let started = Instant::now();
        let code = run(&status, None, &sh("sleep 30 & echo $!; echo done; exit 3"));
        let took = started.elapsed();
        let log = String::from_utf8(log_of(&status)).unwrap();
        let pid: i32 = log.lines().next().unwrap().parse().unwrap();
        // SAFETY: kill of the sleep this test started (its pid from its own output).
        unsafe { libc::kill(pid, libc::SIGKILL) };
        assert_eq!(code, 3);
        assert!(log.ends_with("\ndone\n"), "{log}");
        assert!(took < Duration::from_secs(10), "took {took:?}");
    }

    /// The first `max` bytes, a line at the limit, then at the end a line with what was dropped
    /// and the last `tail` bytes.
    #[test]
    fn the_log_keeps_its_first_bytes_and_the_last() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        // 2000 lines of "line NNNN\n" (10 bytes each): 20000 bytes.
        let script = "i=1000; while [ $i -lt 3000 ]; do echo line $i; i=$((i+1)); done";
        let limit = Limit {
            max: 1000,
            tail: 300,
        };
        assert_eq!(run_limited(&status, None, limit, &sh(script)), 0);
        let log = String::from_utf8(log_of(&status)).unwrap();
        let all: String = (1000..3000).map(|i| format!("line {i}\n")).collect();
        let want = format!(
            "{}--- potluck: the output reached log_max_mib (1000 bytes): what follows is dropped, \
             except its last 300 bytes, added when the command ends ---\n\
             --- potluck: 18700 bytes of output dropped after the log reached log_max_mib \
             (1000 bytes); the last 300 bytes follow ---\n{}",
            &all[..1000],
            &all[all.len() - 300..]
        );
        assert_eq!(log, want);

        // Under the limit, the log is the output.
        let _ = std::fs::remove_file(status.with_file_name("out.log"));
        let limit = Limit {
            max: 20000,
            tail: 300,
        };
        assert_eq!(run_limited(&status, None, limit, &sh(script)), 0);
        assert_eq!(log_of(&status), all.as_bytes());
    }

    /// A limit reached in the middle of a line: potluck's lines start on a line of their own.
    #[test]
    fn lines_about_the_limit_start_on_a_line_of_their_own() {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::File::create(dir.path().join("log")).unwrap();
        let mut sink = Sink::new(file, Limit { max: 5, tail: 4 });
        sink.take(b"abcdefgh");
        sink.take(b"ij");
        assert!(sink.flush_tail());
        assert!(!sink.flush_tail());
        let log = std::fs::read_to_string(dir.path().join("log")).unwrap();
        assert_eq!(
            log,
            "abcde\n--- potluck: the output reached log_max_mib (5 bytes): what follows is \
             dropped, except its last 4 bytes, added when the command ends ---\n\
             --- potluck: 1 bytes of output dropped after the log reached log_max_mib (5 bytes); \
             the last 4 bytes follow ---\nghij"
        );
    }

    #[test]
    fn the_ring_keeps_the_last_bytes() {
        let mut r = Ring::new(5);
        let all = |r: &Ring| {
            let (a, b) = r.parts();
            [a, b].concat()
        };
        r.push(b"ab");
        assert_eq!(all(&r), b"ab");
        r.push(b"cdef");
        assert_eq!(all(&r), b"bcdef");
        r.push(b"g");
        assert_eq!(all(&r), b"cdefg");
        r.push(b"hijk");
        assert_eq!(all(&r), b"ghijk");
        r.push(b"0123456789");
        assert_eq!(all(&r), b"56789");
        r.push(b"");
        assert_eq!(all(&r), b"56789");
        r.clear();
        assert_eq!(all(&r), b"");
        let mut none = Ring::new(0);
        none.push(b"abc");
        assert_eq!(none.len(), 0);
        // Against a plain model, in pieces of every size.
        let mut r = Ring::new(7);
        let mut model: Vec<u8> = Vec::new();
        for n in 0..40u8 {
            let piece: Vec<u8> = (0..n % 9)
                .map(|i| n.wrapping_mul(31).wrapping_add(i))
                .collect();
            r.push(&piece);
            model.extend_from_slice(&piece);
            let from = model.len().saturating_sub(7);
            assert_eq!(all(&r), &model[from..], "after piece {n}");
        }
    }

    /// While output is dropped the log doesn't grow, but its modification time moves (once a
    /// second at most), which the daemon's stall checks count as output.
    #[test]
    fn dropped_output_still_moves_the_logs_time() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let file = std::fs::File::create(&path).unwrap();
        let mut sink = Sink::new(file, Limit { max: 4, tail: 4 });
        sink.take(b"0123456789");
        let set_old = || {
            let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
            let old = [libc::timespec {
                tv_sec: 1_000_000_000,
                tv_nsec: 0,
            }; 2];
            // SAFETY: a NUL-terminated path and two timespecs.
            assert_eq!(
                unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), old.as_ptr(), 0) },
                0
            );
        };
        let meta = || std::fs::metadata(&path).unwrap();
        set_old();
        let len = meta().len();
        std::thread::sleep(TOUCH_EVERY + Duration::from_millis(100));
        sink.take(b"more");
        assert_eq!(meta().len(), len);
        let moved = meta().modified().unwrap();
        assert!(
            moved > std::time::UNIX_EPOCH + Duration::from_secs(1_000_000_000),
            "{moved:?}"
        );
    }

    /// Output that can't be written (here a full non-blocking pipe stands in for a full disk) is
    /// counted, and the next write that works says how much is missing.
    #[test]
    fn output_that_cannot_be_written_is_counted() {
        let (mut r, w) = std::io::pipe().unwrap();
        let w = OwnedFd::from(w);
        set_nonblocking(w.as_raw_fd()).unwrap();
        let mut sink = Sink::new(File::from(w), Limit { max: 0, tail: 0 });
        // Fill the pipe.
        let chunk = vec![b'x'; 4096];
        let mut filled = 0u64;
        while sink.out.lost == 0 {
            sink.take(&chunk);
            filled += chunk.len() as u64;
        }
        sink.take(b"lost too\n");
        let lost = sink.out.lost;
        assert!(lost > 0 && lost <= 4096 + 9, "{lost}");
        // Room again: the next write says what is missing, then goes on.
        let reader = std::thread::spawn(move || {
            let mut all = Vec::new();
            std::io::Read::read_to_end(&mut r, &mut all).unwrap();
            all
        });
        std::thread::sleep(Duration::from_millis(100));
        sink.take(b"back\n");
        drop(sink);
        let all = String::from_utf8(reader.join().unwrap()).unwrap();
        let want_tail = format!(
            "\n--- potluck: {lost} bytes of output are missing here: writing the log failed ("
        );
        let at = all.find(&want_tail).unwrap_or_else(|| {
            panic!(
                "no missing line: {:?}",
                &all[all.len().saturating_sub(300)..]
            )
        });
        // Everything before it is the output that was written.
        assert_eq!(at as u64 + lost, filled + 9);
        assert!(all.ends_with(") ---\nback\n"), "{}", &all[at..]);
    }

    /// A log that can't be written at all doesn't stop the command: the pipe is still read.
    #[test]
    fn a_log_that_cannot_be_written_still_lets_the_command_end() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        std::fs::write(dir.path().join("ro"), "").unwrap();
        let out = File::open(dir.path().join("ro")).unwrap();
        let wake = Wake::new().unwrap();
        let signals = Signals::new();
        let limit = Limit { max: 0, tail: 0 };
        // 4 MiB, far more than a pipe holds.
        let cmd = sh("head -c 4194304 /dev/zero; exit 6");
        let started = Instant::now();
        let code = supervise(out, &status, None, limit, &cmd, &wake, &signals).unwrap();
        assert_eq!(code, 6);
        assert!(started.elapsed() < Duration::from_secs(30));
    }

    /// The first ending signal that finds dropped output adds its tail at once, so a job killed
    /// soon after (at its run limit) keeps it; the end adds what was dropped after that.
    #[test]
    fn a_signal_adds_the_tail_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status.json");
        let log = status.with_file_name("out.log");
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .unwrap();
        let wake = Wake::new().unwrap();
        let signals = Signals::new();
        let limit = Limit { max: 100, tail: 50 };
        let go = dir.path().join("go");
        let written = dir.path().join("written");
        let cmd = sh(&format!(
            "i=1000; while [ $i -lt 1100 ]; do echo line $i; i=$((i+1)); done; : > '{}'; \
             while [ ! -e '{}' ]; do sleep 0.02; done; echo after the signal",
            written.display(),
            go.display()
        ));
        /// Lets the command end when the test fails, so the scope's join doesn't wait forever.
        struct Go<'a>(&'a Path);
        impl Drop for Go<'_> {
            fn drop(&mut self) {
                let _ = std::fs::write(self.0, "");
            }
        }
        let read_log = || std::fs::read_to_string(&log).unwrap_or_default();
        let code = std::thread::scope(|s| {
            let sup = s.spawn(|| supervise(out, &status, None, limit, &cmd, &wake, &signals));
            let _go = Go(&go);
            let deadline = Instant::now() + Duration::from_secs(30);
            // All 1000 bytes are in the pipe once the command has made `written`.
            while !(read_log().contains("what follows is dropped") && written.exists()) {
                assert!(Instant::now() < deadline, "no limit line: {}", read_log());
                std::thread::sleep(Duration::from_millis(20));
            }
            // Let the copier take what is left in the pipe.
            std::thread::sleep(Duration::from_millis(300));
            signals.count.fetch_add(1, Ordering::SeqCst);
            wake.notify();
            // The line saying how much was dropped and the 50 bytes it announces are written one
            // after the other: read the log once all of them are there.
            let tail_len = |log: &str| {
                log.split_once("bytes follow ---\n")
                    .map(|(_, tail)| tail.len())
            };
            while tail_len(&read_log()) != Some(50) {
                assert!(Instant::now() < deadline, "no tail: {}", read_log());
                std::thread::sleep(Duration::from_millis(20));
            }
            let now = read_log();
            assert!(
                now.ends_with("line 1095\nline 1096\nline 1097\nline 1098\nline 1099\n"),
                "{now}"
            );
            // A second signal adds nothing.
            signals.count.fetch_add(1, Ordering::SeqCst);
            wake.notify();
            std::thread::sleep(Duration::from_millis(200));
            assert_eq!(read_log(), now);
            std::fs::write(&go, "").unwrap();
            sup.join().unwrap().unwrap()
        });
        assert_eq!(code, 0);
        let log = read_log();
        assert!(
            log.ends_with(
                "--- potluck: 0 bytes of output dropped after the log reached log_max_mib \
                 (100 bytes); the last 17 bytes follow ---\nafter the signal\n"
            ),
            "{log}"
        );
    }
}
