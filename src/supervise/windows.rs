//! The supervisor on Windows (see [`super`]): CMD in a console process group and a nested job
//! object of its own ([`Group`]), its output read by a thread of its own, the stop event and
//! CMD's end waited on by the main thread, and the job's completion port by a third.
//!
//! Every thread and buffer is made before CMD starts: once the job is at its RAM limit, an
//! allocation in the supervisor fails too (it runs in the job), and the threads wait on the
//! kernel without allocating.

use super::{
    CHECK_EVERY, DRAIN_LIMIT, JobStatus, LOG_TAIL_BYTES, Limit, READ_BUF, Sink, cannot_start, say,
    write_status,
};
use crate::exec::windows::job;
use crate::sys::Group;
use crate::sys::win::Owned;
use crate::types::{ExitStatus, now_ms};
use std::fs::File;
use std::io::{PipeReader, Read, Write};
use std::os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use windows_sys::Win32::Foundation::{
    DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_OPERATION_ABORTED, HANDLE, HANDLE_FLAG_INHERIT,
    INVALID_HANDLE_VALUE, SetHandleInformation, WAIT_OBJECT_0,
};
use windows_sys::Win32::System::Console::{
    CTRL_BREAK_EVENT, CTRL_C_EVENT, GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE,
    STD_OUTPUT_HANDLE, SetConsoleCtrlHandler,
};
use windows_sys::Win32::System::IO::{
    CancelSynchronousIo, CreateIoCompletionPort, GetQueuedCompletionStatus,
    PostQueuedCompletionStatus,
};
use windows_sys::Win32::System::JobObjects::{
    IsProcessInJob, JOBOBJECT_ASSOCIATE_COMPLETION_PORT,
    JobObjectAssociateCompletionPortInformation, SetInformationJobObject,
};
use windows_sys::Win32::System::Pipes::{CreatePipe, PeekNamedPipe};
use windows_sys::Win32::System::SystemServices::{
    JOB_OBJECT_MSG_JOB_MEMORY_LIMIT, JOB_OBJECT_QUERY, JOB_OBJECT_SET_ATTRIBUTES,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, INFINITE, SYNCHRONIZATION_SYNCHRONIZE, WaitForMultipleObjects,
    WaitForSingleObject,
};

/// The pipe's buffer, as large as a Linux pipe's.
const PIPE_BUFFER: u32 = 64 << 10;
/// The completion key of the job's messages, and of the one that ends the port's thread.
const JOB_KEY: usize = 1;
const QUIT_KEY: usize = 2;

/// The entry point of `_supervise` on Windows: CMD's output goes to this process's stdout.
pub fn run(
    status: &Path,
    log_max: Option<u64>,
    job: Option<&str>,
    cmd: &[String],
) -> anyhow::Result<i32> {
    ignore_ctrl_events();
    no_error_boxes();
    keep_std_handles();
    let out = File::from(std::io::stdout().as_handle().try_clone_to_owned()?);
    let limit = Limit {
        max: log_max.unwrap_or(0),
        tail: LOG_TAIL_BYTES,
    };
    supervise(out, status, job, limit, cmd)
}

/// No message box waits for a click: not for a crash (Windows Error Reporting is not invoked, so
/// a crashed process ends at once instead of staying alive behind a "stopped working" dialog in
/// the user's session), a drive that isn't ready, or a file that can't be opened. CMD and what it
/// starts inherit this error mode; the job object also forces the crash part on every process in
/// it (JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION).
fn no_error_boxes() {
    use windows_sys::Win32::System::Diagnostics::Debug::{
        SEM_FAILCRITICALERRORS, SEM_NOGPFAULTERRORBOX, SEM_NOOPENFILEERRORBOX, SetErrorMode,
    };
    // SAFETY: plain call; it only sets this process's error mode.
    unsafe {
        SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX)
    };
}

/// CTRL_C and CTRL_BREAK meant for CMD's process group, or sent to the whole console by a
/// process of the job, don't end the supervisor: it records how CMD ended.
fn ignore_ctrl_events() {
    unsafe extern "system" fn on_ctrl(kind: u32) -> windows_sys::core::BOOL {
        (kind == CTRL_C_EVENT || kind == CTRL_BREAK_EVENT).into()
    }
    // SAFETY: the handler is a plain function that touches nothing.
    unsafe { SetConsoleCtrlHandler(Some(on_ctrl), 1) };
}

/// The handles the supervisor inherited (stdin, and the log as stdout and stderr) were
/// inheritable for that: no longer, so CMD and what it starts don't inherit them too.
fn keep_std_handles() {
    for which in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
        // SAFETY: plain calls; a missing standard handle is null or invalid and skipped.
        unsafe {
            let h = GetStdHandle(which);
            if !h.is_null() && h != INVALID_HANDLE_VALUE {
                SetHandleInformation(h, HANDLE_FLAG_INHERIT, 0);
            }
        }
    }
}

/// What the threads share.
struct Shared {
    sink: Mutex<Sink>,
    /// The main thread wants the reader to stop.
    stop: AtomicBool,
    /// The job reached its RAM limit.
    over_memory: AtomicBool,
    /// CMD's job object, once CMD runs: ended when the job reaches its RAM limit.
    cmd_job: Mutex<Option<Owned>>,
}

fn lock(sink: &Mutex<Sink>) -> MutexGuard<'_, Sink> {
    sink.lock().unwrap_or_else(|e| e.into_inner())
}

/// The job object the supervisor runs in, and the event that asks it to stop CMD.
struct JobParts {
    job: Owned,
    stop: Owned,
}

impl JobParts {
    fn open(name: &str) -> std::io::Result<JobParts> {
        let missing = |what: &str| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("there is no {what} named {name}"),
            )
        };
        let job = job::open(name, JOB_OBJECT_QUERY | JOB_OBJECT_SET_ATTRIBUTES)?
            .ok_or_else(|| missing("job object"))?;
        let mut inside = 0;
        // SAFETY: this process's pseudo handle and a valid job handle.
        let ok = unsafe { IsProcessInJob(GetCurrentProcess(), job.0, &mut inside) };
        if ok == 0 {
            return Err(std::io::Error::last_os_error());
        }
        if inside == 0 {
            return Err(std::io::Error::other(format!(
                "the supervisor doesn't run in job object {name}"
            )));
        }
        let stop = job::open_stop_event(name, SYNCHRONIZATION_SYNCHRONIZE)?
            .ok_or_else(|| missing("stop event for the job object"))?;
        Ok(JobParts { job, stop })
    }
}

/// Runs CMD with its output copied to `out`, in the job object `job` names (none: no stop
/// event and no RAM watch, as when a test runs the supervisor by itself).
fn supervise(
    mut out: File,
    status: &Path,
    job: Option<&str>,
    limit: Limit,
    cmd: &[String],
) -> anyhow::Result<i32> {
    let (prog, args) = cmd
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("_supervise needs a command"))?;
    let parts = match job.map(JobParts::open).transpose() {
        Ok(parts) => parts,
        Err(e) => {
            say(
                &mut out,
                &format!("could not open the job's job object: {e}"),
            );
            return cannot_start(status, 126);
        }
    };
    let (reader, writer) = match pipe() {
        Ok(p) => p,
        Err(e) => {
            say(
                &mut out,
                &format!("could not make a pipe for {prog}'s output: {e}"),
            );
            return cannot_start(status, 126);
        }
    };
    let mut command = Command::new(prog);
    add_args(&mut command, prog, args);
    command.stdin(Stdio::null());
    match writer.try_clone() {
        Ok(w) => {
            command.stdout(w).stderr(writer);
        }
        Err(e) => {
            say(
                &mut out,
                &format!("could not make a pipe for {prog}'s output: {e}"),
            );
            return cannot_start(status, 126);
        }
    }
    let port = match parts.as_ref().map(|p| watch_port(&p.job)).transpose() {
        Ok(port) => port,
        Err(e) => {
            say(
                &mut out,
                &format!("could not watch the job's RAM limit: {e}"),
            );
            return cannot_start(status, 126);
        }
    };
    let shared = Arc::new(Shared {
        sink: Mutex::new(Sink::new(out, limit)),
        stop: AtomicBool::new(false),
        over_memory: AtomicBool::new(false),
        cmd_job: Mutex::new(None),
    });
    let reader = Arc::new(reader);
    let threads = start_threads(&shared, &reader, port);
    let (reading, watching) = match threads {
        Ok(t) => t,
        Err(e) => {
            drop(command);
            let _ = writeln!(
                lock(&shared.sink).out.file,
                "potluck: could not start a thread: {e}"
            );
            return cannot_start(status, 126);
        }
    };
    let spawned = Group::spawn(&mut command);
    // The pipe's write ends live in `command`: only CMD's processes may hold them, so the pipe
    // ends when they are gone.
    drop(command);
    let mut group = match spawned {
        Ok(group) => group,
        Err(e) => {
            let code = if e.kind() == std::io::ErrorKind::NotFound {
                127
            } else {
                126
            };
            let _ = writeln!(lock(&shared.sink).out.file, "{}", start_error(prog, &e));
            stop_reading(&shared, reading);
            drop(watching);
            return cannot_start(status, code);
        }
    };
    *shared.cmd_job.lock().unwrap_or_else(|e| e.into_inner()) = duplicate(group.job());
    if shared.over_memory.load(Ordering::SeqCst) {
        group.kill();
    }
    let stop = parts.as_ref().map(|p| p.stop.0);
    let exit = wait(&mut group, stop, &shared)?;
    let (at_end, mut buf) = stop_reading(&shared, reading);
    let mut sink = lock(&shared.sink);
    if !at_end {
        drain(&reader, &mut sink, &mut buf);
    }
    sink.flush_tail();
    sink.out.recover();
    drop(sink);
    drop(watching);
    let mut record = JobStatus {
        exit: ExitStatus {
            code: exit.code(),
            signal: None,
            ended_at: now_ms(),
        },
        over_memory: shared.over_memory.load(Ordering::SeqCst),
        peak_mem_bytes: 0,
        cpu_ms: 0,
    };
    if let Some(p) = &parts {
        if let Ok(limits) = job::limits(p.job.0) {
            record.peak_mem_bytes = limits.PeakJobMemoryUsed as u64;
        }
        if let Ok(account) = job::accounting(p.job.0) {
            record.cpu_ms = job::cpu_ms(&account);
        }
    }
    write_status(status, &record)?;
    Ok(exit.code().unwrap_or(1))
}

/// A pipe with a buffer of [`PIPE_BUFFER`] bytes.
fn pipe() -> std::io::Result<(PipeReader, std::io::PipeWriter)> {
    let mut r: HANDLE = std::ptr::null_mut();
    let mut w: HANDLE = std::ptr::null_mut();
    // SAFETY: r and w receive new handles, which we own from here on; default security, not
    // inheritable.
    if unsafe { CreatePipe(&mut r, &mut w, std::ptr::null(), PIPE_BUFFER) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: two valid handles that nothing else owns.
    let (r, w) = unsafe {
        (
            OwnedHandle::from_raw_handle(r),
            OwnedHandle::from_raw_handle(w),
        )
    };
    Ok((PipeReader::from(r), std::io::PipeWriter::from(w)))
}

/// A completion port associated with `job`, before CMD starts (so no message is missed): the
/// job's messages come to it with [`JOB_KEY`].
fn watch_port(job: &Owned) -> std::io::Result<Owned> {
    // SAFETY: a new port, not tied to a file; closed by Owned.
    let port = Owned::new(unsafe {
        CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1)
    })
    .ok_or_else(std::io::Error::last_os_error)?;
    let assoc = JOBOBJECT_ASSOCIATE_COMPLETION_PORT {
        CompletionKey: JOB_KEY as *mut core::ffi::c_void,
        CompletionPort: port.0,
    };
    // SAFETY: a valid job handle with JOB_OBJECT_SET_ATTRIBUTES and a structure of the size given.
    let ok = unsafe {
        SetInformationJobObject(
            job.0,
            JobObjectAssociateCompletionPortInformation,
            (&raw const assoc).cast(),
            std::mem::size_of::<JOBOBJECT_ASSOCIATE_COMPLETION_PORT>() as u32,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(port)
}

/// The reader's thread: whether the pipe ended, and its buffer back.
type Reading = JoinHandle<(bool, Vec<u8>)>;

/// The port's thread, ended when dropped.
struct Watching {
    port: Arc<Owned>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Watching {
    fn drop(&mut self) {
        // SAFETY: a valid port; the packet carries no OVERLAPPED.
        unsafe { PostQueuedCompletionStatus(self.port.0, 0, QUIT_KEY, std::ptr::null_mut()) };
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Starts the reader of the pipe and, with a port, its watcher.
fn start_threads(
    shared: &Arc<Shared>,
    reader: &Arc<PipeReader>,
    port: Option<Owned>,
) -> std::io::Result<(Reading, Option<Watching>)> {
    let buf = vec![0u8; READ_BUF];
    let watching = match port {
        Some(port) => {
            let port = Arc::new(port);
            let (p, s) = (Arc::clone(&port), Arc::clone(shared));
            let thread = std::thread::Builder::new()
                .name("job port".into())
                .spawn(move || watch(&p, &s))?;
            Some(Watching {
                port,
                thread: Some(thread),
            })
        }
        None => None,
    };
    let (r, s) = (Arc::clone(reader), Arc::clone(shared));
    let reading = std::thread::Builder::new()
        .name("output".into())
        .spawn(move || read_pipe(&r, &s, buf))?;
    Ok((reading, watching))
}

/// The port's thread: when the job reaches its RAM limit (an allocation in it failed), ends
/// CMD and everything it started, and notes it for STATUS.
fn watch(port: &Owned, shared: &Shared) {
    loop {
        let mut message = 0u32;
        let mut key = 0usize;
        let mut overlapped = std::ptr::null_mut();
        // SAFETY: a valid port and three outputs.
        let ok = unsafe {
            GetQueuedCompletionStatus(port.0, &mut message, &mut key, &mut overlapped, INFINITE)
        };
        if ok == 0 || key == QUIT_KEY {
            return;
        }
        if key == JOB_KEY && message == JOB_OBJECT_MSG_JOB_MEMORY_LIMIT {
            shared.over_memory.store(true, Ordering::SeqCst);
            if let Some(j) = shared
                .cmd_job
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
            {
                let _ = job::terminate(j.0);
            }
        }
    }
}

/// The reader's thread: copies the pipe into the sink until the pipe ends (true) or the main
/// thread stops it (false); gives back its buffer for the drain.
fn read_pipe(pipe: &PipeReader, shared: &Shared, mut buf: Vec<u8>) -> (bool, Vec<u8>) {
    loop {
        if shared.stop.load(Ordering::SeqCst) {
            return (false, buf);
        }
        match (&*pipe).read(&mut buf) {
            Ok(0) => return (true, buf),
            Ok(n) => lock(&shared.sink).take(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            // The main thread cancelled the read to stop this thread.
            Err(e) if e.raw_os_error() == Some(ERROR_OPERATION_ABORTED as i32) => {}
            Err(_) => return (true, buf),
        }
    }
}

/// Stops the reader's thread, which may be waiting in a read: cancels that read until the thread
/// has ended. Returns whether the pipe had ended, and the thread's buffer.
fn stop_reading(shared: &Shared, reading: Reading) -> (bool, Vec<u8>) {
    shared.stop.store(true, Ordering::SeqCst);
    let thread = reading.as_raw_handle();
    loop {
        // SAFETY: the thread's handle, valid until it is joined below; cancelling no pending
        // read does nothing.
        let ended = unsafe {
            CancelSynchronousIo(thread);
            WaitForSingleObject(thread, 10) == WAIT_OBJECT_0
        };
        if ended {
            break;
        }
    }
    reading
        .join()
        .unwrap_or_else(|_| (false, vec![0u8; READ_BUF]))
}

/// Waits for CMD to exit. Each stop request first writes the held tail of dropped output (the
/// first one that finds any), then sends CTRL_BREAK to CMD's process group.
fn wait(
    group: &mut Group,
    stop: Option<HANDLE>,
    shared: &Shared,
) -> std::io::Result<std::process::ExitStatus> {
    let child = group.child().as_raw_handle();
    let mut handles = vec![child];
    handles.extend(stop);
    let mut flushed = false;
    loop {
        // SAFETY: valid handles (CMD's process, and the stop event while `stop`'s owner lives).
        let r =
            unsafe { WaitForMultipleObjects(handles.len() as u32, handles.as_ptr(), 0, INFINITE) };
        if r == WAIT_OBJECT_0 {
            return group.child().wait();
        }
        if r == WAIT_OBJECT_0 + 1 {
            if !flushed {
                flushed = lock(&shared.sink).flush_tail();
            }
            group.interrupt();
            continue;
        }
        // Not expected: look now and then instead of spinning.
        if let Some(exit) = group.child().try_wait()? {
            return Ok(exit);
        }
        std::thread::sleep(CHECK_EVERY);
    }
}

/// After CMD has exited: reads what is in the pipe until it is empty or at its end, or for at
/// most [`DRAIN_LIMIT`] while processes it left keep writing (as on Unix).
fn drain(pipe: &PipeReader, sink: &mut Sink, buf: &mut [u8]) {
    let until = Instant::now() + DRAIN_LIMIT;
    loop {
        match available(pipe) {
            // At its end: every write end is closed and nothing is left.
            None => return,
            Some(0) => {
                // Empty for now. A moment more lets a process CMD left finish a write.
                if Instant::now() >= until {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
                if available(pipe).is_some_and(|n| n > 0) {
                    continue;
                }
                return;
            }
            Some(n) => {
                let want = (n as usize).min(buf.len());
                // Doesn't wait: `want` bytes are there.
                match (&*pipe).read(&mut buf[..want]) {
                    Ok(0) | Err(_) => return,
                    Ok(got) => sink.take(&buf[..got]),
                }
            }
        }
        if Instant::now() >= until {
            return;
        }
    }
}

/// How many bytes the pipe holds; `None` once it has ended (or can't be read).
fn available(pipe: &PipeReader) -> Option<u32> {
    let mut n = 0u32;
    // SAFETY: a valid pipe handle; only the count is asked for.
    let ok = unsafe {
        PeekNamedPipe(
            pipe.as_raw_handle(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            &mut n,
            std::ptr::null_mut(),
        )
    };
    (ok != 0).then_some(n)
}

/// A handle of this process's own to what `h` names; `None` if it can't be made.
fn duplicate(h: HANDLE) -> Option<Owned> {
    let mut dup: HANDLE = std::ptr::null_mut();
    // SAFETY: valid handles of this process; dup receives a new one, closed by Owned.
    let ok = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            h,
            GetCurrentProcess(),
            &mut dup,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if ok == 0 { None } else { Owned::new(dup) }
}

/// What the log says when `prog` couldn't start. Windows says only "The system cannot find the
/// path specified" for a program that names its arguments too (`-- "bash -lc 'make'"`): the
/// whole string is taken for a path. So a program not found whose name holds a space or tab
/// gets told that the program and its arguments are separate arguments.
fn start_error(prog: &str, e: &std::io::Error) -> String {
    let mut msg = format!("potluck: could not start {prog}: {e}");
    if e.kind() == std::io::ErrorKind::NotFound && prog.contains([' ', '\t']) {
        msg.push_str(
            "; the program is the command's first argument and each of its arguments one of \
             their own: `potluck dispatch -- PROGRAM ARG...`",
        );
    }
    msg
}

/// Gives CMD's arguments to `command`: quoted as the C runtime reads them (`\"` for a quote,
/// as `Command::args` writes them), except for cmd.exe, which doesn't read that quoting. For
/// `cmd [SWITCH]... /c ARG...` (or `/k`) cmd gets the line after the switch as one typed after
/// it, in the form it reads literally (`/s /c "LINE"`): one ARG as it is, or several joined by
/// spaces, with quotes around each that holds a space or tab and no quote.
fn add_args(command: &mut Command, prog: &str, args: &[String]) {
    use std::os::windows::process::CommandExt;
    let program = Path::new(prog);
    let is_cmd = program
        .file_stem()
        .is_some_and(|s| s.eq_ignore_ascii_case("cmd"))
        && program
            .extension()
            .is_none_or(|e| e.eq_ignore_ascii_case("exe"));
    let at = args.iter().position(|a| {
        a.eq_ignore_ascii_case("/c") || a.eq_ignore_ascii_case("/k") || !a.starts_with('/')
    });
    let Some(at) = at.filter(|&i| is_cmd && args[i].starts_with('/')) else {
        command.args(args);
        return;
    };
    let rest = &args[at + 1..];
    let line = match rest {
        [one] => one.clone(),
        _ => rest
            .iter()
            .map(|a| {
                if a.contains([' ', '\t']) && !a.contains('"') {
                    format!("\"{a}\"")
                } else {
                    a.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
    };
    for switch in &args[..at] {
        command.raw_arg(switch);
    }
    if !args[..at].iter().any(|a| a.eq_ignore_ascii_case("/s")) {
        command.raw_arg("/s");
    }
    command.raw_arg(format!("{} \"{line}\"", args[at]));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_cmd(dir: &Path, limit: Limit, cmd: &[&str]) -> (i32, String) {
        // As `run` does: else CMD, and a process it leaves running, would hold this test's own
        // stdout and stderr too.
        keep_std_handles();
        let status = dir.join("status.json");
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("out.log"))
            .unwrap();
        let cmd: Vec<String> = cmd.iter().map(|s| s.to_string()).collect();
        let code = supervise(out, &status, None, limit, &cmd).unwrap();
        let log = std::fs::read_to_string(dir.join("out.log")).unwrap_or_default();
        (code, log)
    }

    fn read(dir: &Path) -> JobStatus {
        serde_json::from_slice(&std::fs::read(dir.join("status.json")).unwrap()).unwrap()
    }

    const ALL: Limit = Limit {
        max: 0,
        tail: LOG_TAIL_BYTES,
    };

    #[test]
    fn records_the_exit_code_and_both_streams() {
        let dir = tempfile::tempdir().unwrap();
        let (code, log) = run_cmd(
            dir.path(),
            ALL,
            &["cmd", "/d", "/c", "echo one& echo two 1>&2& exit /b 7"],
        );
        assert_eq!(code, 7);
        assert_eq!(read(dir.path()).exit.code, Some(7));
        assert_eq!(log.replace("\r\n", "\n"), "one\ntwo \n");
    }

    #[test]
    fn a_command_that_cannot_start_is_127() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such-command.exe");
        let (code, log) = run_cmd(dir.path(), ALL, &[missing.to_str().unwrap()]);
        assert_eq!((code, read(dir.path()).exit.code), (127, Some(127)));
        assert!(log.contains("could not start"), "{log}");
        let hint = "`potluck dispatch -- PROGRAM ARG...`";
        assert_eq!(
            log.contains(hint),
            missing.to_str().unwrap().contains(' '),
            "{log}"
        );
        // A program given with its arguments in one string: Windows looks for a path of that name.
        let (code, log) = run_cmd(
            dir.path(),
            ALL,
            &["C:\\Windows\\System32\\ping.exe -n 1 ::1"],
        );
        assert_eq!(code, 127);
        assert!(log.contains(hint), "{log}");
    }

    #[test]
    fn the_log_limit_keeps_the_first_bytes_and_the_last() {
        let dir = tempfile::tempdir().unwrap();
        // 200 lines of "line NNN\r\n" (10 bytes each).
        let (code, log) = run_cmd(
            dir.path(),
            Limit { max: 100, tail: 30 },
            &[
                "cmd",
                "/d",
                "/c",
                "for /l %i in (100,1,299) do @echo line %i",
            ],
        );
        assert_eq!(code, 0);
        assert!(log.starts_with("line 100\r\nline 101\r\n"), "{log}");
        assert!(log.contains("--- potluck: the output reached log_max_mib (100 bytes)"));
        assert!(
            log.ends_with("line 297\r\nline 298\r\nline 299\r\n"),
            "{log}"
        );
    }

    /// A process CMD leaves running holds the pipe open; the status comes anyway.
    #[test]
    fn a_process_left_holding_the_pipe_does_not_hold_up_the_status() {
        let dir = tempfile::tempdir().unwrap();
        let started = Instant::now();
        let (code, log) = run_cmd(
            dir.path(),
            ALL,
            &[
                "cmd",
                "/d",
                "/c",
                "start /b ping -n 30 127.0.0.1 >NUL & echo done& exit /b 3",
            ],
        );
        assert_eq!(code, 3);
        assert!(log.contains("done"), "{log}");
        assert!(started.elapsed() < Duration::from_secs(15));
    }

    /// cmd gets the line after /c as typed: quotes stay quotes, and of several arguments one
    /// with a space is quoted.
    #[test]
    fn cmd_reads_its_line_as_typed() {
        let dir = tempfile::tempdir().unwrap();
        let (code, log) = run_cmd(
            dir.path(),
            ALL,
            &[
                "cmd",
                "/d",
                "/c",
                "set \"X=a b\"& echo \"q u\"& call echo [%X%]",
            ],
        );
        assert_eq!(code, 0, "{log}");
        assert_eq!(log, "\"q u\"\r\n[a b]\r\n");
        let (code, log) = run_cmd(dir.path(), ALL, &["CMD.EXE", "/C", "echo", "a b", "c\"d"]);
        assert_eq!(code, 0, "{log}");
        assert!(log.ends_with("\"a b\" c\"d\r\n"), "{log}");
    }
}
