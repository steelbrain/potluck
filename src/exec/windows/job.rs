//! Job objects: making one for an invocation, finding it again by name, and what it says about
//! its processes. Shared by the executor and the supervisor ([`crate::supervise`]).

use crate::exec::Limits;
use crate::sys::win::{Owned, wide};
use std::io;
use windows_sys::Win32::Foundation::{
    ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND, ERROR_MORE_DATA, FILETIME, GetLastError, HANDLE,
};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
};
use windows_sys::Win32::System::JobObjects::{
    CreateJobObjectW, JOB_OBJECT_LIMIT_ACTIVE_PROCESS, JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION,
    JOB_OBJECT_LIMIT_JOB_MEMORY, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_BASIC_PROCESS_ID_LIST,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicAccountingInformation,
    JobObjectBasicProcessIdList, JobObjectExtendedLimitInformation, OpenJobObjectW,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::ProcessStatus::{
    K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
};
use windows_sys::Win32::System::Threading::{
    CreateEventW, GetProcessTimes, OpenEventW, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    SetEvent,
};

/// The exit code of every process [`terminate`] ends: 128 + SIGKILL, as `sys::Group::kill`.
pub const KILLED: u32 = 137;

/// Makes the job object `name` for an invocation, with its limits: `mem_max_bytes` as the job's
/// commit limit and `tasks_max` as its active process limit (0: none), every process in it
/// killed when its last handle is closed, and an unhandled exception ending a process at once
/// (no Windows Error Reporting dialog to wait on). Fails if an object of that name exists.
pub fn create(name: &str, limits: &Limits) -> io::Result<Owned> {
    let w = wide(name);
    // SAFETY: w is NUL-terminated; default security (the token's default: this user, SYSTEM
    // and administrators); the handle is closed by Owned.
    let job = Owned::new(unsafe { CreateJobObjectW(std::ptr::null(), w.as_ptr()) })
        .ok_or_else(io::Error::last_os_error)?;
    // SAFETY: plain call, right after the one whose error it reads.
    if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("a job object named {name} exists already"),
        ));
    }
    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    let basic = &mut info.BasicLimitInformation;
    basic.LimitFlags =
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;
    if limits.mem_max_bytes > 0 {
        basic.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY;
        info.JobMemoryLimit = usize::try_from(limits.mem_max_bytes).unwrap_or(usize::MAX);
    }
    if limits.tasks_max > 0 {
        let basic = &mut info.BasicLimitInformation;
        basic.LimitFlags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
        basic.ActiveProcessLimit = limits.tasks_max;
    }
    // SAFETY: a valid job handle and a structure of the size given.
    let ok = unsafe {
        SetInformationJobObject(
            job.0,
            JobObjectExtendedLimitInformation,
            (&raw const info).cast(),
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(job)
}

/// Opens the job object `name` with `access`; `None` when there is none by that name (every
/// handle to it is closed: everything that ran in it has ended, or been killed with it).
pub fn open(name: &str, access: u32) -> io::Result<Option<Owned>> {
    let w = wide(name);
    // SAFETY: w is NUL-terminated; the handle is closed by Owned.
    match Owned::new(unsafe { OpenJobObjectW(access, 0, w.as_ptr()) }) {
        Some(h) => Ok(Some(h)),
        None => not_found(),
    }
}

/// `Ok(None)` when the last call failed for want of an object by its name, else its error.
fn not_found<T>() -> io::Result<Option<T>> {
    let e = io::Error::last_os_error();
    if e.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) {
        Ok(None)
    } else {
        Err(e)
    }
}

/// The name of the event that asks the supervisor of the invocation in job object `job` to stop
/// its command ([`crate::supervise`]).
pub fn stop_event_name(job: &str) -> String {
    format!("{job}.stop")
}

/// Makes the stop event of job object `job`: auto-reset, so each request stops the command once.
/// Fails if it exists.
pub fn create_stop_event(job: &str) -> io::Result<Owned> {
    let w = wide(stop_event_name(job));
    // SAFETY: w is NUL-terminated; default security; the handle is closed by Owned.
    let event = Owned::new(unsafe { CreateEventW(std::ptr::null(), 0, 0, w.as_ptr()) })
        .ok_or_else(io::Error::last_os_error)?;
    // SAFETY: plain call, right after the one whose error it reads.
    if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("an event named {} exists already", stop_event_name(job)),
        ));
    }
    Ok(event)
}

/// Opens the stop event of job object `job` with `access`; `None` when there is none (its
/// supervisor has exited).
pub fn open_stop_event(job: &str, access: u32) -> io::Result<Option<Owned>> {
    let w = wide(stop_event_name(job));
    // SAFETY: w is NUL-terminated; the handle is closed by Owned.
    match Owned::new(unsafe { OpenEventW(access, 0, w.as_ptr()) }) {
        Some(h) => Ok(Some(h)),
        None => not_found(),
    }
}

/// Sets an event.
pub fn set_event(event: HANDLE) -> io::Result<()> {
    // SAFETY: a valid event handle with EVENT_MODIFY_STATE.
    if unsafe { SetEvent(event) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Ends every process in the job at once, with exit code [`KILLED`].
pub fn terminate(job: HANDLE) -> io::Result<()> {
    // SAFETY: a valid job handle with JOB_OBJECT_TERMINATE.
    if unsafe { TerminateJobObject(job, KILLED) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The pids of the processes in the job that still run. A process that has exited stays in the
/// job's list while anything holds a handle to it; it isn't counted.
pub fn live_pids(job: HANDLE) -> io::Result<Vec<u32>> {
    let mut pids = all_pids(job)?;
    pids.retain(|&p| crate::sys::pid_alive(p));
    Ok(pids)
}

/// Every pid in the job's process list.
fn all_pids(job: HANDLE) -> io::Result<Vec<u32>> {
    let header = std::mem::offset_of!(JOBOBJECT_BASIC_PROCESS_ID_LIST, ProcessIdList);
    let mut room = 64usize;
    loop {
        // usizes keep the buffer aligned for the structure, whose list is of usizes.
        let words = header.div_ceil(std::mem::size_of::<usize>()) + room;
        let mut buf = vec![0usize; words];
        let bytes = words * std::mem::size_of::<usize>();
        // SAFETY: a valid job handle and a buffer of `bytes` bytes, aligned for the structure.
        let ok = unsafe {
            QueryInformationJobObject(
                job,
                JobObjectBasicProcessIdList,
                buf.as_mut_ptr().cast(),
                bytes as u32,
                std::ptr::null_mut(),
            )
        };
        // SAFETY: the buffer starts with the structure's header, filled by the call (or zeroed).
        let list = unsafe { &*(buf.as_ptr() as *const JOBOBJECT_BASIC_PROCESS_ID_LIST) };
        let assigned = list.NumberOfAssignedProcesses as usize;
        if ok == 0 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(ERROR_MORE_DATA as i32) {
                return Err(e);
            }
        } else if (list.NumberOfProcessIdsInList as usize) >= assigned {
            let n = (list.NumberOfProcessIdsInList as usize).min(room);
            // SAFETY: the list holds n usizes after the header, within buf.
            let ids = unsafe {
                std::slice::from_raw_parts(
                    (buf.as_ptr() as *const u8).add(header) as *const usize,
                    n,
                )
            };
            return Ok(ids.iter().map(|&p| p as u32).collect());
        }
        // More processes than room: again, with room for them and some that start meanwhile.
        room = assigned.max(room) * 2;
    }
}

/// The job's accounting: the CPU time of every process it ever held, and how many it holds.
pub fn accounting(job: HANDLE) -> io::Result<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION> {
    let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
    // SAFETY: a valid job handle and a structure of the size given.
    let ok = unsafe {
        QueryInformationJobObject(
            job,
            JobObjectBasicAccountingInformation,
            (&raw mut info).cast(),
            std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

/// User and kernel time from the accounting, in milliseconds (they count 100 ns units).
pub fn cpu_ms(info: &JOBOBJECT_BASIC_ACCOUNTING_INFORMATION) -> u64 {
    let units = info.TotalUserTime.max(0) as u64 + info.TotalKernelTime.max(0) as u64;
    units / 10_000
}

/// The job's limits and peaks: `PeakJobMemoryUsed` is the most commit its processes had at once,
/// `JobMemoryLimit` its commit limit.
pub fn limits(job: HANDLE) -> io::Result<JOBOBJECT_EXTENDED_LIMIT_INFORMATION> {
    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    // SAFETY: a valid job handle and a structure of the size given.
    let ok = unsafe {
        QueryInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            (&raw mut info).cast(),
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

/// A process: its commit charge (private bytes) and CPU time, if it can be opened.
pub struct ProcessUse {
    pub commit: u64,
    pub cpu_ms: u64,
}

/// What process `pid` uses now; `None` if it can't be opened (it has gone).
pub fn process_use(pid: u32) -> Option<ProcessUse> {
    // SAFETY: plain call; the handle is closed by Owned.
    let h = Owned::new(unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) })?;
    let mut mem = PROCESS_MEMORY_COUNTERS_EX {
        cb: std::mem::size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
        ..Default::default()
    };
    // SAFETY: a valid process handle; the EX structure starts with the plain one and cb says
    // which it is.
    let ok = unsafe {
        K32GetProcessMemoryInfo(
            h.0,
            (&raw mut mem).cast::<PROCESS_MEMORY_COUNTERS>(),
            mem.cb,
        )
    };
    let commit = if ok != 0 { mem.PrivateUsage as u64 } else { 0 };
    let zero = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let (mut created, mut exited, mut kernel, mut user) = (zero, zero, zero, zero);
    // SAFETY: a valid process handle; four FILETIMEs to fill.
    let ok = unsafe { GetProcessTimes(h.0, &mut created, &mut exited, &mut kernel, &mut user) };
    let units = |t: FILETIME| (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime);
    let cpu_ms = if ok != 0 {
        (units(kernel) + units(user)) / 10_000
    } else {
        0
    };
    Some(ProcessUse { commit, cpu_ms })
}

/// What a snapshot of the machine's processes says of one: its parent, threads and image name.
pub struct ProcessInfo {
    pub pid: u32,
    pub parent: u32,
    pub threads: u32,
    pub image: String,
}

/// The machine's processes among `pids`, in `pids`' order.
pub fn process_info(pids: &[u32]) -> io::Result<Vec<ProcessInfo>> {
    // SAFETY: a snapshot of the machine's processes; closed by Owned.
    let snap = Owned::new(unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) })
        .ok_or_else(io::Error::last_os_error)?;
    let mut entry = PROCESSENTRY32W {
        dwSize: std::mem::size_of::<PROCESSENTRY32W>() as u32,
        ..Default::default()
    };
    let mut found = Vec::new();
    // SAFETY: a valid snapshot handle and an entry whose size is set.
    let mut more = unsafe { Process32FirstW(snap.0, &mut entry) } != 0;
    while more {
        if pids.contains(&entry.th32ProcessID) {
            found.push(ProcessInfo {
                pid: entry.th32ProcessID,
                parent: entry.th32ParentProcessID,
                threads: entry.cntThreads,
                image: crate::sys::win::from_wide(&entry.szExeFile),
            });
        }
        // SAFETY: as for Process32FirstW.
        more = unsafe { Process32NextW(snap.0, &mut entry) } != 0;
    }
    found.sort_by_key(|p| pids.iter().position(|&q| q == p.pid));
    Ok(found)
}
