//! Processes: whether a pid is alive, killing one, and [`Group`], a child with everything it
//! starts.

use std::io;
use std::process::{Child, Command};

/// Whether a process with this pid exists (on Unix, one this user may not signal counts too).
/// Pid 0 never does.
pub fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(unix)]
    {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return false;
        };
        // SAFETY: signal 0 only checks whether the process exists.
        (unsafe { libc::kill(pid, 0) } == 0)
            || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(windows)]
    {
        use super::win::Owned;
        use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, GetLastError, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
            WaitForSingleObject,
        };
        // SAFETY: plain call; the handle, if any, is closed by Owned.
        let h = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        match Owned::new(h) {
            // SAFETY: a valid process handle; a zero timeout only polls.
            Some(h) => unsafe { WaitForSingleObject(h.0, 0) == WAIT_TIMEOUT },
            // SAFETY: plain call.
            None => (unsafe { GetLastError() }) == ERROR_ACCESS_DENIED,
        }
    }
}

/// Kills the process with this pid at once (SIGKILL; on Windows `TerminateProcess`). Pid 0, and
/// on Unix pid 1 and anything that would be read as a negative pid, is refused.
pub fn kill_pid(pid: u32) -> io::Result<()> {
    if pid == 0 || (cfg!(unix) && (pid == 1 || pid > i32::MAX as u32)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to kill pid {pid}"),
        ));
    }
    #[cfg(unix)]
    {
        // SAFETY: a positive pid other than 1, checked above.
        if unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        use super::win::Owned;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_TERMINATE, TerminateProcess,
        };
        // SAFETY: plain call; the handle is closed by Owned.
        let h = Owned::new(unsafe { OpenProcess(PROCESS_TERMINATE, 0, pid) })
            .ok_or_else(io::Error::last_os_error)?;
        // SAFETY: a valid process handle with terminate access.
        if unsafe { TerminateProcess(h.0, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// A child process and everything it starts, to end together. On Unix the child leads a process
/// group of its own (`process_group(0)`); a process that leaves the group (setsid) is no longer
/// in it. On Windows the child starts suspended, joins a job object of its own and only then runs,
/// so every process it starts is in the job too (unless the job lets it break away, which this one
/// doesn't). Dropping a `Group` ends nothing, as on Unix: [`Group::kill`] does.
pub struct Group {
    child: Child,
    #[cfg(windows)]
    job: super::win::Owned,
}

impl Group {
    /// Spawns `cmd` in a group of its own. On Windows this sets `cmd`'s creation flags
    /// (CREATE_SUSPENDED and CREATE_NEW_PROCESS_GROUP, so [`Group::interrupt`] can reach it).
    pub fn spawn(cmd: &mut Command) -> io::Result<Group> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            Ok(Group {
                child: cmd.process_group(0).spawn()?,
            })
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW,
            };
            use windows_sys::Win32::System::Threading::{
                CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED,
            };
            // SAFETY: an unnamed job with default security; closed by Owned.
            let job = super::win::Owned::new(unsafe {
                CreateJobObjectW(std::ptr::null(), std::ptr::null())
            })
            .ok_or_else(io::Error::last_os_error)?;
            let mut child = cmd
                .creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP)
                .spawn()?;
            // SAFETY: both handles are valid; the child hasn't run an instruction yet.
            let joined = unsafe { AssignProcessToJobObject(job.0, child.as_raw_handle()) } != 0;
            let resumed = if joined {
                resume_threads(child.id())
            } else {
                Err(io::Error::last_os_error())
            };
            if let Err(e) = resumed {
                let _ = child.kill();
                let _ = child.wait();
                return Err(e);
            }
            Ok(Group { child, job })
        }
    }

    /// The child's pid (on Unix also the group's id).
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// The child, to wait for it or take its pipes.
    pub fn child(&mut self) -> &mut Child {
        &mut self.child
    }

    /// Asks the group to stop: SIGTERM to the process group on Unix; on Windows CTRL_BREAK to the
    /// child's console process group, which reaches it only when it shares this process's
    /// console. Best effort.
    pub fn interrupt(&self) {
        #[cfg(unix)]
        {
            if let Ok(pgid) = libc::pid_t::try_from(self.child.id())
                && pgid > 1
            {
                // SAFETY: a negative pid other than -1 names this child's process group.
                unsafe { libc::kill(-pgid, libc::SIGTERM) };
            }
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
            // SAFETY: plain call; it fails harmlessly when the group isn't on our console.
            unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, self.child.id()) };
        }
    }

    /// Kills every process of the group at once: SIGKILL to the process group, or ends the job
    /// object. Best effort; the child still needs waiting for.
    pub fn kill(&self) {
        #[cfg(unix)]
        {
            if let Ok(pgid) = libc::pid_t::try_from(self.child.id())
                && pgid > 1
            {
                // SAFETY: a negative pid other than -1 names this child's process group.
                unsafe { libc::kill(-pgid, libc::SIGKILL) };
            }
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::JobObjects::TerminateJobObject;
            // SAFETY: a valid job handle.
            unsafe { TerminateJobObject(self.job.0, 137) };
        }
    }

    /// Whether any process of the group is left (the child included until it has been waited
    /// for on Unix, where a zombie still counts as a member).
    pub fn alive(&self) -> bool {
        #[cfg(unix)]
        {
            let Ok(pgid) = libc::pid_t::try_from(self.child.id()) else {
                return false;
            };
            // SAFETY: signal 0 to the group only checks whether a member exists.
            pgid > 1 && unsafe { libc::kill(-pgid, 0) } == 0
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::JobObjects::{
                JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
                QueryInformationJobObject,
            };
            // SAFETY: plain data, filled by the call.
            let mut info: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
            // SAFETY: a valid job handle and a buffer of the size given.
            let ok = unsafe {
                QueryInformationJobObject(
                    self.job.0,
                    JobObjectBasicAccountingInformation,
                    (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    std::ptr::null_mut(),
                )
            };
            ok != 0 && info.ActiveProcesses > 0
        }
    }

    /// The job object's handle (Windows), for callers that query or limit it.
    #[cfg(windows)]
    pub fn job(&self) -> windows_sys::Win32::Foundation::HANDLE {
        self.job.0
    }
}

/// Resumes every thread of process `pid` (a process created suspended has one).
#[cfg(windows)]
fn resume_threads(pid: u32) -> io::Result<()> {
    use super::win::Owned;
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};
    // SAFETY: a snapshot of every thread on the machine; closed by Owned.
    let snap = Owned::new(unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) })
        .ok_or_else(io::Error::last_os_error)?;
    // SAFETY: plain data; dwSize must be set before the first call.
    let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
    let mut resumed = 0;
    // SAFETY: a valid snapshot handle and entry.
    let mut more = unsafe { Thread32First(snap.0, &mut entry) } != 0;
    while more {
        if entry.th32OwnerProcessID == pid {
            // SAFETY: plain call; the handle is closed by Owned.
            if let Some(t) =
                Owned::new(unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) })
            {
                // SAFETY: a valid thread handle with suspend/resume access.
                if unsafe { ResumeThread(t.0) } != u32::MAX {
                    resumed += 1;
                }
            }
        }
        // SAFETY: as for Thread32First.
        more = unsafe { Thread32Next(snap.0, &mut entry) } != 0;
    }
    if resumed == 0 {
        return Err(io::Error::other(format!(
            "can't resume process {pid}: no thread of it could be resumed"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_process_is_alive_and_pid_0_is_not() {
        assert!(pid_alive(std::process::id()));
        assert!(!pid_alive(0));
        assert!(kill_pid(0).is_err());
    }

    /// A child's own child is in its group, and `kill` ends both.
    #[test]
    fn a_group_ends_with_what_it_started() {
        #[cfg(unix)]
        let mut cmd = {
            let mut c = Command::new("sh");
            c.args(["-c", "sleep 30 & sleep 30"]);
            c
        };
        #[cfg(windows)]
        let mut cmd = {
            let mut c = Command::new("cmd");
            c.args([
                "/c",
                "start /b ping -n 30 127.0.0.1 >NUL & ping -n 30 127.0.0.1 >NUL",
            ]);
            c
        };
        cmd.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let mut g = Group::spawn(&mut cmd).unwrap();
        assert!(pid_alive(g.id()));
        std::thread::sleep(std::time::Duration::from_millis(300));
        assert!(g.alive());
        g.kill();
        g.child().wait().unwrap();
        let give_up = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while g.alive() && std::time::Instant::now() < give_up {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(!g.alive(), "a process of the group outlived its kill");
    }
}
