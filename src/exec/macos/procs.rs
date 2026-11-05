//! Process snapshots from libproc: which processes belong to an invocation, what they use, and
//! signals that can never reach pid 0, 1, -1 or the caller.

use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::{size_of, zeroed};
use std::sync::OnceLock;

/// One live process of this user, as a snapshot saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Proc {
    pub pid: u32,
    pub ppid: u32,
    pub pgid: u32,
    /// Start time in microseconds since the Unix epoch: with the pid, it names one process.
    pub start: u64,
}

/// Every process proc_pidinfo describes to us: this user's live processes. Zombies and other
/// users' processes are absent (the kernel refuses their BSD info to a non-root caller).
pub(super) struct Snapshot {
    pub procs: HashMap<u32, Proc>,
}

/// The processes of one invocation in one snapshot.
#[derive(Default)]
pub(super) struct Members {
    /// Live (not zombie) processes, in the order they were found, each once.
    pub procs: Vec<Proc>,
    /// Their process groups, without any group that must never be signalled.
    pub pgids: Vec<u32>,
}

fn bsd_info(pid: u32) -> Option<libc::proc_bsdinfo> {
    let pid = i32::try_from(pid).ok()?;
    // SAFETY: proc_bsdinfo is plain data; proc_pidinfo writes at most `size` bytes into it.
    let mut info: libc::proc_bsdinfo = unsafe { zeroed() };
    let size = size_of::<libc::proc_bsdinfo>() as i32;
    let n =
        unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size) };
    (n == size && info.pbi_status != libc::SZOMB).then_some(info)
}

fn start_of(info: &libc::proc_bsdinfo) -> u64 {
    info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec
}

/// A live process's start time (microseconds since the Unix epoch), or None when there is no live
/// process of this user with that pid.
pub fn start_time(pid: u32) -> Option<u64> {
    bsd_info(pid).map(|i| start_of(&i))
}

/// Whether process `pid` started at `start` is stopped by a signal.
pub(super) fn stopped(pid: u32, start: u64) -> bool {
    bsd_info(pid).is_some_and(|i| start_of(&i) == start && i.pbi_status == libc::SSTOP)
}

/// The process's name (up to 32 bytes), or "?".
pub(super) fn name(pid: u32) -> String {
    let Some(info) = bsd_info(pid) else {
        return "?".to_string();
    };
    let raw = if info.pbi_name[0] != 0 {
        &info.pbi_name[..]
    } else {
        &info.pbi_comm[..]
    };
    let bytes: Vec<u8> = raw
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Whether process `pid` started at `start` still exists in any state, zombie included.
/// A zombie has no BSD info, so it is recognised by its pid still being listed with readable
/// resource usage (the kernel keeps a zombie's pid reserved until it is reaped).
pub fn exists(pid: u32, start: u64) -> bool {
    if let Some(t) = start_time(pid) {
        return t == start;
    }
    let Ok(p) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: rusage_info_v2 is plain data of the size the flavor asks for.
    let mut ri: libc::rusage_info_v2 = unsafe { zeroed() };
    let r = unsafe { libc::proc_pid_rusage(p, libc::RUSAGE_INFO_V2, (&raw mut ri).cast()) };
    r == 0 && list_pids().is_ok_and(|pids| pids.contains(&pid))
}

fn list_pids() -> std::io::Result<Vec<u32>> {
    // SAFETY: a null buffer asks for the current count only.
    let n = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
    if n < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut cap = n as usize + 256;
    loop {
        let mut buf = vec![0i32; cap];
        // SAFETY: the buffer holds `cap` pids; the size is in bytes.
        let got = unsafe {
            libc::proc_listallpids(buf.as_mut_ptr().cast(), (cap * size_of::<i32>()) as i32)
        };
        if got < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let got = got as usize;
        if got < cap {
            buf.truncate(got);
            return Ok(buf
                .into_iter()
                .filter(|&p| p > 0)
                .map(|p| p as u32)
                .collect());
        }
        cap *= 2;
    }
}

/// Every live process of this user, with its parent, group and start time.
pub(super) fn snapshot() -> std::io::Result<Snapshot> {
    let pids = list_pids()?;
    let mut procs = HashMap::with_capacity(pids.len());
    for pid in pids {
        if let Some(i) = bsd_info(pid) {
            procs.insert(
                pid,
                Proc {
                    pid,
                    ppid: i.pbi_ppid,
                    pgid: i.pbi_pgid,
                    start: start_of(&i),
                },
            );
        }
    }
    Ok(Snapshot { procs })
}

/// The invocation's live processes in `snap`:
/// - the leader and every tracked (pid, start) pair still alive with the same start time;
/// - the leader's session, unless its id may have been reused: when the leader's pid now names
///   another process (of any user, zombies included: the snapshot sees neither), or no process
///   has it and the leader started in another boot (`boot`, the boot session the leader started
///   in; see [`same_boot`]);
/// - every descendant (by parent pid, transitively) of the above.
///
/// Start times say which process a pid names, never which came first: they are the wall clock
/// at each start, so after the clock is set back a child can show an earlier start than its
/// parent. A live process's parent pid always names a live process (an orphan's parent becomes
/// launchd), so a child found by its parent pid needs no check of its start.
pub(super) fn members(
    snap: &Snapshot,
    leader: (u32, u64),
    tracked: &[(u32, u64)],
    boot: &str,
) -> Members {
    let mut found: HashSet<u32> = HashSet::new();
    let mut procs: Vec<Proc> = Vec::new();
    let mut queue: VecDeque<Proc> = VecDeque::new();
    let mut take = |p: Proc, procs: &mut Vec<Proc>, queue: &mut VecDeque<Proc>| {
        if p.pid > 1 && p.pid != std::process::id() && found.insert(p.pid) {
            procs.push(p);
            queue.push_back(p);
        }
    };
    for &(pid, start) in std::iter::once(&leader).chain(tracked) {
        if let Some(&p) = snap.procs.get(&pid)
            && p.start == start
        {
            take(p, &mut procs, &mut queue);
        }
    }
    // The kernel never gives a live session's id to a new process, so the session is the
    // leader's while the leader is there (alive or a zombie). Once no process has its pid, a
    // session with that id is either what the leader left behind or, in a later boot, another's.
    let session_ours = match snap.procs.get(&leader.0) {
        Some(p) => p.start == leader.1,
        None => match holder_start(leader.0) {
            Some(t) => t == leader.1,
            None => same_boot(boot, leader.1, boot_session(), boot_time()),
        },
    };
    if session_ours && let Ok(sid) = i32::try_from(leader.0) {
        for &p in snap.procs.values() {
            // SAFETY: getsid has no memory preconditions.
            if unsafe { libc::getsid(p.pid as i32) } == sid {
                take(p, &mut procs, &mut queue);
            }
        }
    }
    let mut children: HashMap<u32, Vec<Proc>> = HashMap::new();
    for &p in snap.procs.values() {
        children.entry(p.ppid).or_default().push(p);
    }
    while let Some(parent) = queue.pop_front() {
        for &c in children.get(&parent.pid).map(Vec::as_slice).unwrap_or(&[]) {
            take(c, &mut procs, &mut queue);
        }
    }
    let mut pgids: Vec<u32> = Vec::new();
    for p in &procs {
        if group_signallable(p.pgid) && !pgids.contains(&p.pgid) {
            pgids.push(p.pgid);
        }
    }
    Members { procs, pgids }
}

/// `struct kinfo_proc` from sysctl(KERN_PROC_PID): 648 bytes; `kp_proc` comes first, and in it
/// `p_starttime` (a timeval: 8-byte seconds, 4-byte microseconds) at offset 0 and `p_pid` at 40.
const KINFO_PROC_SIZE: usize = 648;
const KP_START_SEC: usize = 0;
const KP_START_USEC: usize = 8;
const KP_PID: usize = 40;

/// The start time of whatever process has `pid` now, whatever its user or state (zombies
/// included), or None when no process has it. Unlike libproc, sysctl describes every process.
fn holder_start(pid: u32) -> Option<u64> {
    let p = i32::try_from(pid).ok()?;
    let mut mib = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_PID, p];
    let mut buf = [0u8; KINFO_PROC_SIZE];
    let mut len = buf.len();
    // SAFETY: the buffer has `len` bytes of room; sysctl writes at most that many.
    let r = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as u32,
            buf.as_mut_ptr().cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if r != 0 || len != KINFO_PROC_SIZE {
        return None;
    }
    let kp_pid = i32::from_ne_bytes(buf[KP_PID..KP_PID + 4].try_into().ok()?);
    if kp_pid != p {
        return None;
    }
    let sec = i64::from_ne_bytes(buf[KP_START_SEC..KP_START_SEC + 8].try_into().ok()?);
    let usec = i32::from_ne_bytes(buf[KP_START_USEC..KP_START_USEC + 4].try_into().ok()?);
    Some((sec.max(0) as u64) * 1_000_000 + usec.max(0) as u64)
}

/// When this machine booted, in microseconds since the Unix epoch.
fn boot_time() -> Option<u64> {
    let mut tv = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    let mut len = size_of::<libc::timeval>();
    // SAFETY: kern.boottime is a struct timeval; `len` says how much room there is.
    let r = unsafe {
        libc::sysctlbyname(
            c"kern.boottime".as_ptr(),
            (&raw mut tv).cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (r == 0 && tv.tv_sec > 0).then(|| tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64)
}

/// Whether a leader that started at `start` (microseconds since the Unix epoch) in boot session
/// `boot` started in this boot, `now` (this boot's session id) and `boot_time` (when this boot
/// began). With the boot session known, it decides. Without it (a handle kept from before
/// potluck recorded it), the leader's start is compared with the boot time; the kernel moves the
/// boot time with every step of the wall clock and start times stay as they were, so a clock
/// stepped forward by more than the job had run makes a job of this boot look older.
fn same_boot(boot: &str, start: u64, now: Option<&str>, boot_time: Option<u64>) -> bool {
    if boot.is_empty() {
        boot_time.is_some_and(|b| start >= b)
    } else {
        now == Some(boot)
    }
}

/// This boot's session id (`kern.bootsessionuuid`), a new UUID at every boot.
pub(super) fn boot_session() -> Option<&'static str> {
    static BOOT: OnceLock<Option<String>> = OnceLock::new();
    BOOT.get_or_init(|| {
        let mut buf = [0u8; 64];
        let mut len = buf.len();
        // SAFETY: the buffer has `len` bytes of room; sysctlbyname writes at most that many.
        let r = unsafe {
            libc::sysctlbyname(
                c"kern.bootsessionuuid".as_ptr(),
                buf.as_mut_ptr().cast(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if r != 0 {
            return None;
        }
        let text = buf[..len.min(buf.len())].split(|&b| b == 0).next()?;
        let text = std::str::from_utf8(text).ok()?.trim();
        (!text.is_empty()).then(|| text.to_string())
    })
    .as_deref()
}

/// How many processes this user has, counted as the kernel counts them for RLIMIT_NPROC: by real
/// user id, zombies included.
pub(super) fn user_process_count() -> std::io::Result<u64> {
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let mut mib = [
        libc::CTL_KERN,
        libc::KERN_PROC,
        libc::KERN_PROC_RUID,
        uid as libc::c_int,
    ];
    loop {
        let mut len = 0usize;
        // SAFETY: a null buffer asks for the size needed.
        let r = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as u32,
                std::ptr::null_mut(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if r != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut buf = vec![0u8; len + 64 * KINFO_PROC_SIZE];
        let mut got = buf.len();
        // SAFETY: the buffer has `got` bytes of room; sysctl writes at most that many.
        let r = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as u32,
                buf.as_mut_ptr().cast(),
                &raw mut got,
                std::ptr::null_mut(),
                0,
            )
        };
        if r == 0 {
            return Ok((got / KINFO_PROC_SIZE) as u64);
        }
        let e = std::io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::ENOMEM) {
            return Err(e);
        }
    }
}

/// The most processes one user may have (`kern.maxprocperuid`), or None if it can't be read.
pub(super) fn max_procs_per_user() -> Option<u64> {
    let mut n: libc::c_int = 0;
    let mut len = size_of::<libc::c_int>();
    // SAFETY: kern.maxprocperuid is an int; `len` says how much room there is.
    let r = unsafe {
        libc::sysctlbyname(
            c"kern.maxprocperuid".as_ptr(),
            (&raw mut n).cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (r == 0 && n > 0).then_some(n as u64)
}

/// The value of `name` in the environment `pid` was started with (sysctl KERN_PROCARGS2: what
/// execve was given, whatever the process changed since), "" when it has none, or None when the
/// kernel won't say (a process of another user, or one that is exiting). macOS leaves the
/// environment out for its own programs (/bin/sleep, /usr/bin/perl): those have none here.
pub(super) fn initial_env_var(pid: u32, name: &str) -> Option<String> {
    let p = i32::try_from(pid).ok()?;
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, p];
    let mut read = |buf: &mut Vec<u8>| -> std::io::Result<usize> {
        let mut len = buf.len();
        // SAFETY: the buffer has `len` bytes of room (none for a null one, which asks for the
        // size needed); sysctl writes at most that many.
        let r = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as u32,
                if buf.is_empty() {
                    std::ptr::null_mut()
                } else {
                    buf.as_mut_ptr().cast()
                },
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if r == 0 {
            Ok(len)
        } else {
            Err(std::io::Error::last_os_error())
        }
    };
    let mut buf = Vec::new();
    let need = read(&mut buf).ok()?;
    buf.resize(need.max(4), 0);
    let len = read(&mut buf).ok()?;
    buf.truncate(len);
    let value = env_var_in_procargs(&buf, name.as_bytes())?;
    Some(String::from_utf8_lossy(value.unwrap_or_default()).into_owned())
}

/// `name`'s value in a KERN_PROCARGS2 buffer: argc (a native int), the executable's path, NUL
/// padding, argc NUL-terminated arguments, then the environment's `KEY=VALUE` strings up to an
/// empty one or the end. Some(None) when the environment has no `name`; None when the buffer
/// isn't one.
fn env_var_in_procargs<'a>(buf: &'a [u8], name: &[u8]) -> Option<Option<&'a [u8]>> {
    let argc = i32::from_ne_bytes(buf.get(..4)?.try_into().ok()?);
    let argc = usize::try_from(argc).ok()?;
    let mut rest = &buf[4..];
    // The executable's path, then the padding after it.
    let end = rest.iter().position(|&b| b == 0)?;
    rest = &rest[end..];
    let start = rest.iter().position(|&b| b != 0).unwrap_or(rest.len());
    rest = &rest[start..];
    let mut strings = rest.split(|&b| b == 0);
    for _ in 0..argc {
        strings.next()?;
    }
    for s in strings {
        if s.is_empty() {
            break;
        }
        if let Some(value) = s.strip_prefix(name).and_then(|r| r.strip_prefix(b"=")) {
            return Some(Some(value));
        }
    }
    Some(None)
}

fn own_ids() -> (u32, u32) {
    // SAFETY: getpgrp has no preconditions.
    (std::process::id(), unsafe { libc::getpgrp() } as u32)
}

fn pid_signallable(pid: u32) -> bool {
    pid > 1 && pid <= i32::MAX as u32 && pid != own_ids().0
}

fn group_signallable(pgid: u32) -> bool {
    let (me, my_group) = own_ids();
    pgid > 1 && pgid <= i32::MAX as u32 && pgid != me && pgid != my_group
}

/// Sends `sig` to `pid`, unless that would reach pid 0, 1, every process, or the caller.
pub(super) fn kill_pid(pid: u32, sig: i32) {
    if pid_signallable(pid) {
        // SAFETY: pid is a positive pid other than 1 and our own.
        unsafe { libc::kill(pid as i32, sig) };
    }
}

/// Sends `sig` to process group `pgid`, unless it is 0, 1 or the caller's own group.
pub(super) fn kill_group(pgid: u32, sig: i32) {
    if group_signallable(pgid) {
        // SAFETY: pgid is a positive group id other than 1 and the caller's.
        unsafe { libc::killpg(pgid as i32, sig) };
    }
}

/// What one process uses.
pub(super) struct Use {
    /// User and system time of the process and of its children it has reaped, in nanoseconds.
    pub cpu_ns: u64,
    pub footprint: u64,
}

/// Resource usage of `pid`, or None if it is gone or not ours.
pub(super) fn usage(pid: u32) -> Option<Use> {
    let p = i32::try_from(pid).ok()?;
    // SAFETY: rusage_info_v2 is plain data of the size the flavor asks for.
    let mut ri: libc::rusage_info_v2 = unsafe { zeroed() };
    let r = unsafe { libc::proc_pid_rusage(p, libc::RUSAGE_INFO_V2, (&raw mut ri).cast()) };
    if r != 0 {
        return None;
    }
    let ticks = ri.ri_user_time as u128
        + ri.ri_system_time as u128
        + ri.ri_child_user_time as u128
        + ri.ri_child_system_time as u128;
    Some(Use {
        cpu_ns: ticks_to_ns(ticks),
        footprint: ri.ri_phys_footprint,
    })
}

/// `struct mach_timebase_info`, declared here with its function: the libc crate marks its own
/// binding deprecated in favour of another crate.
#[repr(C)]
struct Timebase {
    numer: u32,
    denom: u32,
}

unsafe extern "C" {
    fn mach_timebase_info(info: *mut Timebase) -> libc::c_int;
}

/// rusage times are in Mach absolute time units: 1 ns on Intel, 125/3 ns on Apple silicon.
fn ticks_to_ns(ticks: u128) -> u64 {
    static TIMEBASE: OnceLock<(u128, u128)> = OnceLock::new();
    let &(numer, denom) = TIMEBASE.get_or_init(|| {
        let mut tb = Timebase { numer: 0, denom: 0 };
        // SAFETY: writes the two fields of `tb`.
        let r = unsafe { mach_timebase_info(&raw mut tb) };
        if r != 0 || tb.numer == 0 || tb.denom == 0 {
            (1, 1)
        } else {
            (tb.numer as u128, tb.denom as u128)
        }
    });
    (ticks * numer / denom).min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sees_itself_and_its_parent() {
        let snap = snapshot().unwrap();
        let me = snap.procs[&std::process::id()];
        assert!(me.start > 1_700_000_000_000_000, "{me:?}");
        // SAFETY: getppid and getpgrp have no preconditions.
        let (ppid, pgid) = unsafe { (libc::getppid() as u32, libc::getpgrp() as u32) };
        assert_eq!((me.ppid, me.pgid), (ppid, pgid), "{me:?}");
        // The parent (cargo or a shell) is normally this user's, and then the snapshot has it.
        match snap.procs.get(&ppid) {
            Some(parent) => assert_eq!(start_time(ppid), Some(parent.start), "{parent:?}"),
            None => assert!(start_time(ppid).is_none(), "the parent is another user's"),
        }
        assert_eq!(start_time(me.pid), Some(me.start));
        assert!(exists(me.pid, me.start));
        assert!(!exists(me.pid, me.start + 1));
        // The caller is never a member, even when tracked.
        let m = members(&snap, (me.pid, me.start), &[(me.pid, me.start)], "");
        assert!(m.procs.iter().all(|p| p.pid != me.pid));
    }

    /// Start times are the wall clock when each process started, so after the clock is set back
    /// a child can have an earlier start than its parent, and a session member than its leader.
    /// Membership doesn't depend on that order.
    #[test]
    fn membership_survives_the_clock_going_back() {
        // Pids above PID_MAX (99999): no real process has them, so getsid finds none.
        let leader = Proc {
            pid: 4_000_000,
            ppid: 1,
            pgid: 4_000_000,
            start: 1_800_000_000_000_000,
        };
        let child = Proc {
            pid: 4_000_001,
            ppid: leader.pid,
            pgid: leader.pid,
            // Started 3 s after its parent, with the clock set back 10 s in between.
            start: leader.start - 7_000_000,
        };
        let grandchild = Proc {
            pid: 4_000_002,
            ppid: child.pid,
            pgid: leader.pid,
            start: child.start + 1_000,
        };
        let snap = Snapshot {
            procs: [leader, child, grandchild]
                .into_iter()
                .map(|p| (p.pid, p))
                .collect(),
        };
        let m = members(&snap, (leader.pid, leader.start), &[], "");
        let mut pids: Vec<u32> = m.procs.iter().map(|p| p.pid).collect();
        pids.sort_unstable();
        assert_eq!(pids, [leader.pid, child.pid, grandchild.pid]);
    }

    /// The same for the session: a member whose start is before its leader's still belongs.
    /// Here a real session (perl calls setsid and forks a child), whose leader has exited, and a
    /// snapshot that gives the child an earlier start than the leader's.
    #[test]
    fn session_membership_survives_the_clock_going_back() {
        let dir = tempfile::tempdir().unwrap();
        let script = r#"use POSIX; POSIX::setsid();
            if (fork == 0) { open(F, ">child.pid"); print F $$; close F; sleep 60; exit 0 }
            exit 0"#;
        let mut leader = std::process::Command::new("/usr/bin/perl")
            .args(["-e", script])
            .current_dir(dir.path())
            .spawn()
            .unwrap();
        let leader_pid = leader.id();
        let leader_start = start_time(leader_pid);
        leader.wait().unwrap();
        let child_file = dir.path().join("child.pid");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let child_pid: u32 = loop {
            if let Some(p) = std::fs::read_to_string(&child_file)
                .ok()
                .and_then(|s| s.trim().parse().ok())
            {
                break p;
            }
            assert!(std::time::Instant::now() < deadline, "no child");
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        let child_start = start_time(child_pid).unwrap();
        let mut snap = snapshot().unwrap();
        // The leader is gone; as recorded after the clock went back, the child started before
        // it did.
        let leader_start = leader_start.unwrap_or(child_start);
        snap.procs.get_mut(&child_pid).unwrap().start = leader_start - 5_000_000;
        let m = members(&snap, (leader_pid, leader_start), &[], "");
        // SAFETY: the child we started, checked by its start time just before.
        if start_time(child_pid) == Some(child_start) {
            unsafe { libc::kill(child_pid as i32, libc::SIGKILL) };
        }
        assert_eq!(
            m.procs.iter().map(|p| p.pid).collect::<Vec<_>>(),
            [child_pid]
        );
    }

    #[test]
    fn sysctl_sees_what_libproc_does_and_more() {
        let me = std::process::id();
        assert_eq!(holder_start(me), start_time(me));
        // launchd is root's: libproc won't describe it to us, sysctl does.
        assert_eq!(start_time(1), None);
        let boot = boot_time().unwrap();
        assert!(
            holder_start(1).is_some_and(|t| t >= boot - 1_000_000),
            "{boot}"
        );
        assert!(start_time(me).unwrap() >= boot);
        assert_eq!(holder_start(0x7fff_fff0), None);
    }

    /// A forward clock step moves the boot time past a job's start; the boot session doesn't
    /// move.
    #[test]
    fn the_boot_session_decides_over_the_boot_time() {
        let (start, boot_time) = (1_800_000_000_000_000, 1_700_000_000_000_000);
        let stepped = start + 3_600_000_000;
        assert!(same_boot("A", start, Some("A"), Some(stepped)));
        assert!(!same_boot("A", start, Some("B"), Some(boot_time)));
        assert!(!same_boot("A", start, None, Some(boot_time)));
        // A handle from before boot sessions were recorded: the boot time decides.
        assert!(same_boot("", start, Some("A"), Some(boot_time)));
        assert!(!same_boot("", start, Some("A"), Some(stepped)));
        assert!(!same_boot("", start, Some("A"), None));
        assert!(
            boot_session().is_some_and(|b| b.len() == 36),
            "{:?}",
            boot_session()
        );
    }

    /// The count RLIMIT_NPROC is checked against: this user's processes by real user id, as
    /// `ps -U` lists them (give or take what starts and ends meanwhile).
    #[test]
    fn counts_this_users_processes() {
        // SAFETY: getuid has no preconditions.
        let uid = unsafe { libc::getuid() };
        let ps = || {
            let out = std::process::Command::new("/bin/ps")
                .args(["-U", &uid.to_string(), "-o", "pid="])
                .output()
                .unwrap();
            String::from_utf8(out.stdout).unwrap().lines().count() as u64
        };
        let before = ps();
        let n = user_process_count().unwrap();
        let after = ps();
        println!("ps {before}, sysctl {n}, ps {after}");
        assert!(
            (before.min(after).saturating_sub(10)..=before.max(after) + 10).contains(&n),
            "ps {before}, sysctl {n}, ps {after}"
        );
        assert!(max_procs_per_user().is_some_and(|m| m >= n));
    }

    #[test]
    fn reads_a_variable_from_the_initial_environment() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2i32.to_ne_bytes());
        buf.extend_from_slice(
            b"/bin/sh\0\0\0\0sh\0POTLUCK_JOB=arg\0A=1\0POTLUCK_JOB=studio-7\0B=\0\0junk",
        );
        assert_eq!(
            env_var_in_procargs(&buf, b"POTLUCK_JOB"),
            Some(Some(&b"studio-7"[..]))
        );
        assert_eq!(env_var_in_procargs(&buf, b"B"), Some(Some(&b""[..])));
        assert_eq!(env_var_in_procargs(&buf, b"POTLUCK"), Some(None));
        assert_eq!(env_var_in_procargs(&buf, b"junk"), Some(None));
        assert_eq!(env_var_in_procargs(&buf[..3], b"A"), None);
        assert_eq!(env_var_in_procargs(&9i32.to_ne_bytes(), b"A"), None);
        // This process's own, and none of a process that isn't this user's.
        let home = std::env::var("HOME").unwrap();
        assert_eq!(initial_env_var(std::process::id(), "HOME"), Some(home));
        assert_eq!(initial_env_var(1, "HOME"), None);
    }

    #[test]
    fn never_signals_reserved_pids() {
        let (me, group) = own_ids();
        for pid in [0, 1, me, u32::MAX, i32::MAX as u32 + 1] {
            assert!(!pid_signallable(pid), "{pid}");
        }
        for pgid in [0, 1, me, group, u32::MAX] {
            assert!(!group_signallable(pgid), "{pgid}");
        }
        assert!(pid_signallable(me + 1) || me + 1 > i32::MAX as u32);
    }

    #[test]
    fn cpu_time_is_in_nanoseconds() {
        let before = usage(std::process::id()).unwrap().cpu_ns;
        let t = std::time::Instant::now();
        let mut x = 0u64;
        while t.elapsed().as_millis() < 300 {
            x = std::hint::black_box(x.wrapping_add(1));
        }
        let used = usage(std::process::id()).unwrap().cpu_ns - before;
        // Other test threads may add CPU time too; the unit is what matters (ticks would be ~24x
        // smaller on Apple silicon).
        assert!(used >= 250_000_000, "{used} ns");
    }
}
