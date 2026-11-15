//! Windows: `potluck daemon` is a keeper, and the daemon proper runs as its child.
//!
//! A running process on Windows can't become another binary (there is no exec), so a restart in
//! place ([`super::handover`]) can't keep the daemon's process there. The keeper does what the
//! exec does on Unix:
//!
//! - It runs the binary at its own path with its own arguments (`potluck daemon`), marked by
//!   [`MARKER`] in the environment (taken out of it at once: nothing the daemon starts sees it),
//!   in a job object of its own: `KILL_ON_JOB_CLOSE`, so the daemon and what it started end with
//!   the keeper (the service's task stopped), and `BREAKAWAY_OK`, so a job's supervisor leaves the
//!   job (jobs run on through a restart, or a stop).
//! - When the daemon exits with [`RESTART_EXIT`] (a restart: it drained, quiesced, wrote
//!   everything, closed its store and let go of `daemon.lock` and the socket, after checking the
//!   binary at its path as on Unix), the keeper ends what is left in that job (git and tools the
//!   daemon ran, cut short, as the daemon ends its children before an exec on Unix) and at once
//!   starts the binary at its path again, in a new job. The new daemon starts as any daemon
//!   does, and adopts the running jobs. Any other exit ends the keeper with the daemon's code.
//! - While it runs it holds [`LOCK_FILE`] in the state directory, so a command line that finds no
//!   socket meanwhile waits for the daemon ([`super::running_here`]) rather than report none.
//! - A console's Ctrl+C or Ctrl+Break reaches the daemon too, which shares the keeper's console
//!   and ends; the keeper lets them pass and exits with the daemon's code. A keeper without a
//!   console (the service's task, which lets go of the window Windows opens for it) starts the
//!   daemon with `CREATE_NO_WINDOW`: a console program started without a console gets a new
//!   window otherwise.
//!
//! The keeper itself runs the code it started with until it is started again (its service
//! restarted); it does only this.

use crate::config::Paths;
use crate::sys::win::Owned;
use anyhow::Context;
use std::fs::File;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use windows_sys::Win32::Foundation::{FALSE, STATUS_CONTROL_C_EXIT, TRUE};
use windows_sys::Win32::System::Console::{
    CTRL_BREAK_EVENT, CTRL_C_EVENT, GetConsoleWindow, SetConsoleCtrlHandler,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_BREAKAWAY_OK,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicAccountingInformation,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED, ExitProcess};

/// What the daemon exits with to be restarted by its keeper.
pub const RESTART_EXIT: i32 = 75;

/// The variable marking the daemon a keeper started (its value: the keeper's pid).
pub const MARKER: &str = "POTLUCK_KEPT";

/// The file in the state directory a keeper holds locked while it runs.
pub const LOCK_FILE: &str = "keeper.lock";

/// How long the keeper waits for what a restarting daemon left running to end.
const END_WAIT: Duration = Duration::from_secs(5);

/// Whether this process is the daemon a keeper started ([`MARKER`]); the marker leaves the
/// environment. Call before any thread starts.
pub fn kept() -> bool {
    let kept = std::env::var_os(MARKER).is_some();
    if kept {
        // SAFETY: called first thing in `potluck daemon`, before it starts any thread.
        unsafe { std::env::remove_var(MARKER) };
    }
    kept
}

/// `potluck daemon` on Windows: runs the daemon proper until it exits other than to restart, and
/// returns its exit code.
pub fn run(paths: &Paths) -> anyhow::Result<i32> {
    let exe = crate::update::running_binary()?;
    let _lock = hold_lock(paths);
    keep_on_console_events();
    loop {
        let job = new_job().context("making a job object for the daemon")?;
        let mut child = start(&job, &exe)?;
        let status = child
            .wait()
            .with_context(|| format!("waiting for the daemon ({})", exe.display()))?;
        let code = status.code().unwrap_or(1);
        if code != RESTART_EXIT {
            // Dropping the job ends what the daemon left running.
            return Ok(code);
        }
        end_job(&job);
    }
}

/// [`LOCK_FILE`], locked; None if it can't be (another keeper holds it: its daemon refuses this
/// one's on `daemon.lock`, saying so).
fn hold_lock(paths: &Paths) -> Option<File> {
    crate::sys::create_dir_all(&paths.home, 0o700).ok()?;
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(paths.home.join(LOCK_FILE))
        .ok()?;
    super::lock_now(&f).ok()?.then_some(f)
}

/// A job object that ends what is in it when its last handle closes, and lets processes started
/// with `CREATE_BREAKAWAY_FROM_JOB` leave it.
fn new_job() -> std::io::Result<Owned> {
    // SAFETY: an unnamed job with default security; closed by Owned.
    let job = Owned::new(unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) })
        .ok_or_else(std::io::Error::last_os_error)?;
    // SAFETY: plain data; only the flags below are set.
    let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
    info.BasicLimitInformation.LimitFlags =
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_BREAKAWAY_OK;
    // SAFETY: a valid job handle and a structure of the size given.
    let ok = unsafe {
        SetInformationJobObject(
            job.0,
            JobObjectExtendedLimitInformation,
            (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(job)
}

/// Starts the daemon proper from `exe` in `job`: suspended until it is in the job, so everything
/// it starts is in the job too; in the keeper's console, or without a window if it has none.
fn start(job: &Owned, exe: &Path) -> anyhow::Result<Child> {
    use std::os::windows::io::AsRawHandle;
    use std::os::windows::process::CommandExt;
    // SAFETY: a plain query.
    let console = !unsafe { GetConsoleWindow() }.is_null();
    let window = if console { 0 } else { CREATE_NO_WINDOW };
    let mut child = Command::new(exe)
        .args(std::env::args_os().skip(1))
        .env(MARKER, std::process::id().to_string())
        .creation_flags(CREATE_SUSPENDED | window)
        .spawn()
        .with_context(|| format!("starting the daemon ({})", exe.display()))?;
    // SAFETY: both handles are valid; the child hasn't run an instruction yet.
    let joined = unsafe { AssignProcessToJobObject(job.0, child.as_raw_handle()) } != 0;
    let resumed = if joined {
        crate::sys::resume_threads(child.id())
    } else {
        Err(std::io::Error::last_os_error())
    };
    if let Err(e) = resumed {
        let _ = child.kill();
        let _ = child.wait();
        anyhow::bail!("starting the daemon ({}) in its job: {e}", exe.display());
    }
    Ok(child)
}

/// Ends every process in `job` and waits, [`END_WAIT`] at most, until none is left.
fn end_job(job: &Owned) {
    // SAFETY: a valid job handle.
    unsafe { TerminateJobObject(job.0, 1) };
    let until = Instant::now() + END_WAIT;
    while active(job) > 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// How many processes are in `job`.
fn active(job: &Owned) -> u32 {
    // SAFETY: plain data, filled by the call.
    let mut info: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: a valid job handle and a buffer of the size given.
    let ok = unsafe {
        QueryInformationJobObject(
            job.0,
            JobObjectBasicAccountingInformation,
            (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
            std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 { 0 } else { info.ActiveProcesses }
}

/// The keeper outlives Ctrl+C and Ctrl+Break (the daemon gets them too, and ends): it then exits
/// with the daemon's code. Any other event (the console closing, a logoff or shutdown) ends it
/// as by default, and the daemon with it.
fn keep_on_console_events() {
    unsafe extern "system" fn handler(event: u32) -> windows_sys::core::BOOL {
        match event {
            CTRL_C_EVENT | CTRL_BREAK_EVENT => TRUE,
            _ => FALSE,
        }
    }
    // SAFETY: a handler that only returns, for the life of the process.
    unsafe { SetConsoleCtrlHandler(Some(handler), TRUE) };
}

/// The daemon proper ends at once on any console event (Ctrl+C, Ctrl+Break, the console closing,
/// a logoff or shutdown), as SIGTERM ends it on Unix: its jobs run on, and the next daemon
/// adopts them. Ctrl+C is let in even if whatever started the keeper turned it off.
pub fn end_on_console_events() {
    unsafe extern "system" fn handler(_event: u32) -> windows_sys::core::BOOL {
        // SAFETY: ends the process; nothing runs after it.
        unsafe { ExitProcess(STATUS_CONTROL_C_EXIT as u32) }
    }
    // SAFETY: plain calls; the handler lives as long as the process.
    unsafe {
        SetConsoleCtrlHandler(None, FALSE);
        SetConsoleCtrlHandler(Some(handler), TRUE);
    }
}
