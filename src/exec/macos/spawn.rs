//! Starting the supervisor detached from the daemon, by a double fork.
//!
//! The daemon forks an intermediate process, which forks the supervisor and exits at once. The
//! daemon reaps the intermediate right away; the supervisor's parent is then launchd, which reaps
//! it when it exits. So the daemon never has a long-lived child: nothing becomes a zombie whether
//! or not the daemon is sampling, restarting or gone, and the exit status always comes from the
//! status file the supervisor writes.
//!
//! Between fork and exec only async-signal-safe calls are made, on memory prepared before the
//! fork: the supervisor reports its pid and start time (and, if a step fails, which step and the
//! errno) on a close-on-exec channel ([`report_channel`]), calls setsid, sets its process limit
//! (RLIMIT_NPROC, soft and hard, so the job can't raise it), changes directory, puts /dev/null on
//! stdin and the log on stdout and stderr, closes every other descriptor, resets signal
//! dispositions, blocks SIGINT, SIGTERM and SIGHUP (the supervisor lets them through once it
//! catches them) and nothing else, and executes the supervisor with exactly the given environment.

use anyhow::{Context, anyhow, bail};
use std::collections::BTreeMap;
use std::ffi::{CString, OsStr};
use std::fs::{File, OpenOptions};
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, Instant};

/// The supervisor that started.
pub(super) struct Started {
    pub pid: u32,
    /// Microseconds since the Unix epoch.
    pub start: u64,
}

/// What to run, prepared before the fork.
pub(super) struct Spawn<'a> {
    pub program: &'a Path,
    /// Arguments after the program name.
    pub args: Vec<&'a OsStr>,
    pub env: &'a BTreeMap<String, String>,
    pub cwd: &'a Path,
    pub log: &'a Path,
    /// RLIMIT_NPROC for the supervisor and everything it starts, soft and hard (at most the
    /// current hard limit), or None to leave it as it is.
    pub nproc: Option<u64>,
    /// Where the supervisor writes its pid and start time (see [`write_record`]) before it
    /// execs, so a daemon that stops before it learns them can find it after a restart.
    pub record: &'a Path,
    /// Written into the record after the pid and start time, for whoever reads it back.
    pub record_extra: &'a [u8],
}

/// The size of the record's head: the pid (u32) and start time (u64), in native byte order. The
/// spawn's `record_extra` follows.
pub(super) const RECORD_LEN: usize = 4 + 8;

/// The pid and start time from a record the supervisor wrote, and the extra bytes after them, or
/// None if `bytes` isn't one.
pub(super) fn read_record(bytes: &[u8]) -> Option<(Started, &[u8])> {
    let (head, extra) = bytes.split_at_checked(RECORD_LEN)?;
    let pid = u32::from_ne_bytes(head[..4].try_into().ok()?);
    let start = u64::from_ne_bytes(head[4..].try_into().ok()?);
    (pid > 1 && start > 0).then_some((Started { pid, start }, extra))
}

const MSG_STARTED: u8 = b'S';
const MSG_FAILED: u8 = b'F';
const STARTED_LEN: usize = 1 + 4 + 8;
const FAILED_LEN: usize = 1 + 4 + 4;

/// The steps that can fail in the forked processes, as reported on the pipe.
const STEP_FORK: u32 = 1;
const STEP_SETSID: u32 = 2;
const STEP_CHDIR: u32 = 3;
const STEP_STDIO: u32 = 4;
const STEP_EXEC: u32 = 5;
const STEP_NPROC: u32 = 6;

/// How long to wait for the exec once the supervisor has reported its pid; after this the start
/// counts as successful. Only a process that inherited the channel's write end without
/// close-on-exec could hold it open that long, and [`report_channel`] makes none.
const EXEC_WAIT: Duration = Duration::from_secs(10);
/// How long to wait for the forked processes' first record.
const FIRST_REPORT_WAIT: Duration = Duration::from_secs(10);
/// How often to look for the end of the report while nothing arrives (the exec normally comes
/// within a few milliseconds of the started record).
const REPORT_POLL: Duration = Duration::from_millis(1);

fn cstring(bytes: &[u8], what: &str) -> anyhow::Result<CString> {
    CString::new(bytes).map_err(|_| anyhow!("{what} contains a NUL byte"))
}

/// A duplicate of `fd` numbered 3 or higher, close-on-exec, so the dup2 calls onto 0, 1 and 2
/// can't overwrite one another.
fn high_fd(fd: RawFd) -> std::io::Result<OwnedFd> {
    // SAFETY: F_DUPFD_CLOEXEC returns a new descriptor that we then own.
    let n = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: n is a fresh descriptor owned by nobody else.
    Ok(unsafe { OwnedFd::from_raw_fd(n) })
}

/// The read and write ends of the channel the forked processes report on, both close-on-exec
/// from the moment they exist and numbered 3 or higher.
///
/// The end of the report is the end of the file: the supervisor's write end closes when it
/// execs. macOS has no pipe2, so a pipe's descriptors are inheritable until fcntl marks them,
/// and a process another thread of the daemon spawns in that moment (std's Command passes such
/// descriptors on) keeps the write end open for as long as it runs, holding up the start by up
/// to [`EXEC_WAIT`]. A FIFO opened with O_CLOEXEC has no such moment. It is made in `dir` and
/// removed as soon as both ends are open.
fn report_channel(dir: &Path) -> std::io::Result<(OwnedFd, OwnedFd)> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let open = |path: &CString, flags: i32| -> std::io::Result<OwnedFd> {
        // SAFETY: a NUL-terminated path; open returns a new descriptor or -1.
        let fd = unsafe { libc::open(path.as_ptr(), flags | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: fd is a fresh descriptor owned by nobody else.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // Already close-on-exec; only moved above 2 if it is below.
        if fd.as_raw_fd() < 3 {
            high_fd(fd.as_raw_fd())
        } else {
            Ok(fd)
        }
    };
    loop {
        let path = dir.join(format!(
            ".potluck-spawn-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let c = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| std::io::Error::other("the log's directory contains a NUL byte"))?;
        // SAFETY: a NUL-terminated path.
        if unsafe { libc::mkfifo(c.as_ptr(), 0o600) } != 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                continue;
            }
            return Err(e);
        }
        // Non-blocking, so opening the read end doesn't wait for a writer; with a reader open,
        // opening the write end doesn't wait either.
        let ends = open(&c, libc::O_RDONLY | libc::O_NONBLOCK)
            .and_then(|r| Ok((r, open(&c, libc::O_WRONLY)?)));
        let _ = std::fs::remove_file(&path);
        return ends;
    }
}

/// Everything the forked processes touch, allocated before the fork.
struct Prepared<'a> {
    program: CString,
    argv: Vec<*const libc::c_char>,
    envp: Vec<*const libc::c_char>,
    cwd: CString,
    stdin: RawFd,
    log: RawFd,
    report: RawFd,
    record: CString,
    record_extra: &'a [u8],
    nproc: Option<libc::rlim_t>,
    fd_buf: *mut libc::proc_fdinfo,
    fd_buf_bytes: i32,
}

pub(super) fn spawn(s: &Spawn) -> anyhow::Result<Started> {
    let program = std::path::absolute(s.program)
        .with_context(|| format!("resolving {}", s.program.display()))?;
    let program = cstring(program.as_os_str().as_bytes(), "the potluck binary's path")?;
    let mut args = vec![program.clone()];
    for a in &s.args {
        args.push(cstring(a.as_bytes(), "an argument")?);
    }
    let mut env = Vec::with_capacity(s.env.len());
    for (k, v) in s.env {
        if k.is_empty() || k.contains('=') {
            bail!("bad environment variable name {k:?}");
        }
        env.push(cstring(
            format!("{k}={v}").as_bytes(),
            "an environment variable",
        )?);
    }
    let argv: Vec<*const libc::c_char> = args
        .iter()
        .map(|c| c.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect();
    let envp: Vec<*const libc::c_char> = env
        .iter()
        .map(|c| c.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect();
    let cwd = cstring(s.cwd.as_os_str().as_bytes(), "the working directory")?;
    let record = cstring(s.record.as_os_str().as_bytes(), "the record's path")?;
    match std::fs::remove_file(s.record) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            return Err(e).with_context(|| format!("removing {}", s.record.display()));
        }
        _ => {}
    }

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(s.log)
        .with_context(|| format!("opening the log {}", s.log.display()))?;
    let log = high_fd(log.as_raw_fd())?;
    let null = File::open("/dev/null").context("opening /dev/null")?;
    let null = high_fd(null.as_raw_fd())?;
    let dir = s
        .log
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let (report_r, report_w) = report_channel(dir)
        .with_context(|| format!("creating a channel to the supervisor in {}", dir.display()))?;

    // Room for every descriptor this process has open, with headroom for ones opened meanwhile
    // by other threads (those are close-on-exec anyway).
    // SAFETY: a null buffer asks for the size needed.
    let need = unsafe {
        libc::proc_pidinfo(
            std::process::id() as i32,
            libc::PROC_PIDLISTFDS,
            0,
            std::ptr::null_mut(),
            0,
        )
    };
    let entries = (need.max(0) as usize / size_of::<libc::proc_fdinfo>()) * 2 + 256;
    let mut fd_buf: Vec<libc::proc_fdinfo> = Vec::with_capacity(entries);

    let prep = Prepared {
        program,
        argv,
        envp,
        cwd,
        stdin: null.as_raw_fd(),
        log: log.as_raw_fd(),
        report: report_w.as_raw_fd(),
        record,
        record_extra: s.record_extra,
        nproc: s.nproc.map(nproc_within_hard_limit),
        fd_buf: fd_buf.as_mut_ptr(),
        fd_buf_bytes: (fd_buf.capacity() * size_of::<libc::proc_fdinfo>()).min(i32::MAX as usize)
            as i32,
    };

    // Block every signal while forking, so no handler of the daemon's runs in the forked
    // processes before the supervisor resets them.
    // SAFETY: plain sigset manipulation on local values.
    let mut all: libc::sigset_t = 0;
    let mut old: libc::sigset_t = 0;
    unsafe {
        libc::sigfillset(&raw mut all);
        libc::pthread_sigmask(libc::SIG_SETMASK, &raw const all, &raw mut old);
    }
    // SAFETY: the child branch only makes async-signal-safe calls and never returns.
    let child = unsafe { libc::fork() };
    if child == 0 {
        // SAFETY: we are the single-threaded intermediate process.
        unsafe { intermediate(&prep) }
    }
    let fork_err = std::io::Error::last_os_error();
    // SAFETY: restores the mask saved above.
    unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &raw const old, std::ptr::null_mut()) };
    if child < 0 {
        return Err(fork_err).context("fork");
    }
    drop(report_w);
    let mut status = 0;
    // SAFETY: `child` is our own child; reaping it here is what keeps it from being a zombie.
    while unsafe { libc::waitpid(child, &raw mut status, 0) } < 0 {
        if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
            break;
        }
    }
    drop(fd_buf);

    let report = read_report(&report_r)?;
    let started = (report.len() >= STARTED_LEN && report[0] == MSG_STARTED).then(|| {
        let pid = u32::from_ne_bytes(report[1..5].try_into().unwrap());
        let start = u64::from_ne_bytes(report[5..13].try_into().unwrap());
        Started { pid, start }
    });
    // A failure record sits either first (the intermediate's fork failed) or right after the
    // started record.
    let at = if started.is_some() { STARTED_LEN } else { 0 };
    let failed = (report.len() >= at + FAILED_LEN && report[at] == MSG_FAILED)
        .then_some(at)
        .map(|i| {
            let step = u32::from_ne_bytes(report[i + 1..i + 5].try_into().unwrap());
            let errno = i32::from_ne_bytes(report[i + 5..i + 9].try_into().unwrap());
            (step, std::io::Error::from_raw_os_error(errno))
        });
    if let Some((step, err)) = failed {
        let what = match step {
            STEP_FORK => "fork".to_string(),
            STEP_SETSID => "setsid".to_string(),
            STEP_CHDIR => format!("changing directory to {}", s.cwd.display()),
            STEP_STDIO => "redirecting stdin, stdout and stderr".to_string(),
            STEP_EXEC => format!("executing {}", s.program.display()),
            STEP_NPROC => "setting its process limit (RLIMIT_NPROC)".to_string(),
            _ => "an unknown step".to_string(),
        };
        bail!("could not start the supervisor: {what}: {err}");
    }
    let mut started = started.ok_or_else(|| {
        anyhow!("could not start the supervisor: the forked process ended early (status {status})")
    })?;
    if started.start == 0 {
        started.start = super::procs::start_time(started.pid)
            .ok_or_else(|| anyhow!("could not read the supervisor's start time"))?;
    }
    Ok(started)
}

/// `n`, but no more than this process's hard RLIMIT_NPROC (an unprivileged process can't raise it).
fn nproc_within_hard_limit(n: u64) -> libc::rlim_t {
    let mut now = libc::rlimit {
        rlim_cur: 0,
        rlim_max: libc::RLIM_INFINITY,
    };
    // SAFETY: getrlimit writes one rlimit.
    unsafe { libc::getrlimit(libc::RLIMIT_NPROC, &raw mut now) };
    n.min(now.rlim_max)
}

/// Reads the forked processes' reports until the channel's end (the exec succeeded or everything
/// ended) or [`EXEC_WAIT`] passes after the started record. The read end is non-blocking: a read
/// returns what is there, 0 at the end, or EAGAIN while a write end is open with nothing written.
/// poll() and kqueue wake for data but on macOS never for the end of a FIFO, so poll only paces
/// the reads.
fn read_report(fd: &OwnedFd) -> anyhow::Result<Vec<u8>> {
    // The intermediate has been reaped; its child writes the first record at once.
    let first = Instant::now() + FIRST_REPORT_WAIT;
    let mut out = Vec::new();
    let mut exec_by: Option<Instant> = None;
    loop {
        if out.len() >= STARTED_LEN && exec_by.is_none() {
            exec_by = Some(Instant::now() + EXEC_WAIT);
        }
        let mut buf = [0u8; 64];
        // SAFETY: reads into a local buffer.
        let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
        if n > 0 {
            out.extend_from_slice(&buf[..n as usize]);
            continue;
        }
        if n == 0 {
            return Ok(out);
        }
        let e = std::io::Error::last_os_error();
        match e.kind() {
            std::io::ErrorKind::Interrupted => continue,
            std::io::ErrorKind::WouldBlock => {}
            _ => return Err(e).context("reading the supervisor's start report"),
        }
        let left = exec_by
            .unwrap_or(first)
            .saturating_duration_since(Instant::now());
        if left.is_zero() {
            if exec_by.is_none() {
                bail!("could not start the supervisor: no report from the forked process");
            }
            return Ok(out);
        }
        let mut pfd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let wait = left.min(REPORT_POLL).as_millis().max(1) as i32;
        // SAFETY: one pollfd. Its result doesn't matter: the read above decides.
        unsafe { libc::poll(&raw mut pfd, 1, wait) };
    }
}

/// Signal numbers run from 1 to 31 on macOS (`__DARWIN_NSIG`).
const NSIG: i32 = 32;

fn errno() -> i32 {
    // SAFETY: reads this thread's errno.
    unsafe { *libc::__error() }
}

/// Writes all of `buf` to `fd`, retrying on EINTR. Async-signal-safe.
unsafe fn write_all(fd: RawFd, buf: &[u8]) {
    let mut done = 0;
    while done < buf.len() {
        // SAFETY: writes from a live buffer.
        let n = unsafe { libc::write(fd, buf[done..].as_ptr().cast(), buf.len() - done) };
        if n < 0 {
            if errno() == libc::EINTR {
                continue;
            }
            return;
        }
        done += n as usize;
    }
}

unsafe fn fail(report: RawFd, step: u32, err: i32) -> ! {
    let mut msg = [0u8; FAILED_LEN];
    msg[0] = MSG_FAILED;
    msg[1..5].copy_from_slice(&step.to_ne_bytes());
    msg[5..9].copy_from_slice(&err.to_ne_bytes());
    // SAFETY: async-signal-safe write, then _exit.
    unsafe {
        write_all(report, &msg);
        libc::_exit(127)
    }
}

/// The intermediate process: fork the supervisor and exit at once.
unsafe fn intermediate(p: &Prepared) -> ! {
    // SAFETY: single-threaded after fork; only async-signal-safe calls follow.
    unsafe {
        let pid = libc::fork();
        if pid == 0 {
            supervisor(p)
        }
        if pid < 0 {
            fail(p.report, STEP_FORK, errno())
        }
        libc::_exit(0)
    }
}

/// The supervisor before exec.
unsafe fn supervisor(p: &Prepared) -> ! {
    // SAFETY: single-threaded after fork; every call below is async-signal-safe and works on
    // memory prepared before the fork.
    unsafe {
        let pid = libc::getpid();
        let mut info: libc::proc_bsdinfo = zeroed();
        let size = size_of::<libc::proc_bsdinfo>() as i32;
        let start =
            if libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size)
                == size
            {
                info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec
            } else {
                0
            };
        write_record(p, pid as u32, start);
        let mut msg = [0u8; STARTED_LEN];
        msg[0] = MSG_STARTED;
        msg[1..5].copy_from_slice(&(pid as u32).to_ne_bytes());
        msg[5..13].copy_from_slice(&start.to_ne_bytes());
        write_all(p.report, &msg);

        if libc::setsid() < 0 {
            fail(p.report, STEP_SETSID, errno());
        }
        if let Some(n) = p.nproc {
            let limit = libc::rlimit {
                rlim_cur: n,
                rlim_max: n,
            };
            if libc::setrlimit(libc::RLIMIT_NPROC, &raw const limit) < 0 {
                fail(p.report, STEP_NPROC, errno());
            }
        }
        if libc::chdir(p.cwd.as_ptr()) < 0 {
            fail(p.report, STEP_CHDIR, errno());
        }
        if libc::dup2(p.stdin, 0) < 0 || libc::dup2(p.log, 1) < 0 || libc::dup2(p.log, 2) < 0 {
            fail(p.report, STEP_STDIO, errno());
        }
        close_other_fds(p);
        for sig in 1..NSIG {
            libc::signal(sig, libc::SIG_DFL);
        }
        // The supervisor unblocks these once its handlers are in place: one sent right after
        // the start waits for them instead of ending it before it can record anything.
        let mut held: libc::sigset_t = 0;
        libc::sigemptyset(&raw mut held);
        for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            libc::sigaddset(&raw mut held, sig);
        }
        libc::sigprocmask(libc::SIG_SETMASK, &raw const held, std::ptr::null_mut());
        libc::execve(p.program.as_ptr(), p.argv.as_ptr(), p.envp.as_ptr());
        fail(p.report, STEP_EXEC, errno())
    }
}

/// Writes the supervisor's record (pid, start time, then `p.record_extra`) to `p.record`, mode
/// 0600. Best effort: a
/// record that can't be written only means a restart in the middle of this start won't find it.
unsafe fn write_record(p: &Prepared, pid: u32, start: u64) {
    // SAFETY: async-signal-safe calls on memory prepared before the fork.
    unsafe {
        let fd = libc::open(
            p.record.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_CLOEXEC,
            0o600,
        );
        if fd < 0 {
            return;
        }
        let mut rec = [0u8; RECORD_LEN];
        rec[..4].copy_from_slice(&pid.to_ne_bytes());
        rec[4..].copy_from_slice(&start.to_ne_bytes());
        write_all(fd, &rec);
        write_all(fd, p.record_extra);
        libc::close(fd);
    }
}

/// Closes every descriptor above 2 except the report pipe (close-on-exec), so nothing the daemon
/// holds, such as its `daemon.lock`, is inherited by the job.
unsafe fn close_other_fds(p: &Prepared) {
    // SAFETY: the buffer was allocated before the fork with `fd_buf_bytes` bytes of room.
    unsafe {
        let got = libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDLISTFDS,
            0,
            p.fd_buf.cast(),
            p.fd_buf_bytes,
        );
        if got > 0 && got < p.fd_buf_bytes {
            let n = got as usize / size_of::<libc::proc_fdinfo>();
            for i in 0..n {
                let fd = (*p.fd_buf.add(i)).proc_fd;
                if fd > 2 && fd != p.report {
                    libc::close(fd);
                }
            }
        } else {
            // The list didn't fit or failed: close by number.
            let max = libc::getdtablesize().clamp(3, 1 << 16);
            for fd in 3..max {
                if fd != p.report {
                    libc::close(fd);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cloexec(fd: &OwnedFd) -> bool {
        // SAFETY: F_GETFD on a descriptor we own.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
        flags >= 0 && flags & libc::FD_CLOEXEC != 0
    }

    /// Both ends are close-on-exec, above 2, carry data, and see the end of the file once the
    /// write end closes; the FIFO itself is gone.
    #[test]
    fn the_report_channel_is_close_on_exec_and_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let (r, w) = report_channel(dir.path()).unwrap();
        assert!(cloexec(&r) && cloexec(&w));
        assert!(r.as_raw_fd() > 2 && w.as_raw_fd() > 2);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        // SAFETY: writes from a live buffer to a descriptor we own.
        unsafe { write_all(w.as_raw_fd(), b"S1234") };
        drop(w);
        assert_eq!(read_report(&r).unwrap(), b"S1234");
    }
}
