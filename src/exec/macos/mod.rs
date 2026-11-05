//! macOS: each invocation runs in a session and process group of its own.
//!
//! - **Start:** the supervisor (`potluck _supervise`) is started by a double fork (`spawn.rs`):
//!   its parent is launchd, which reaps it, so the daemon never holds a child that could become a
//!   zombie, and the exit status always comes from the status file. Before it execs, the
//!   supervisor writes its pid and start time beside the status file (`status.pid`), which
//!   `find` reads after a restart that cut a start short. It calls setsid; the command
//!   inherits its session and leads a process group of its own. The supervisor inherits no
//!   descriptor but stdin (/dev/null), stdout and stderr (the log, appended), and exactly the given
//!   environment; the command's stdout and stderr are a pipe the supervisor copies into the log
//!   ([`crate::supervise`]). SIGINT, SIGTERM and SIGHUP are blocked when it starts, until its
//!   handlers are in place, so one sent right after the start can't end it before it records
//!   anything.
//! - **A stopped supervisor** (SIGSTOP from outside the job) can't read the job's output, so the
//!   job would block once the pipe is full, and can't record how the command ended: `sample`
//!   continues it (SIGCONT).
//! - **Membership**, from pid snapshots (libproc: pid, parent, group, start time; getsid): the
//!   supervisor's session, every descendant of a tracked process, and every tracked
//!   (pid, start time) pair still alive. So a process that leaves the session (setsid, double
//!   fork) is still followed once a sample has seen it or its parent. One that leaves and loses
//!   its parent between two samples, as a daemon started by a double fork does, is adopted if
//!   its parent is now launchd and it was started with this run's `POTLUCK_LEASE` in its
//!   environment (sysctl KERN_PROCARGS2, read once per process; the lease is new for every run,
//!   so such a process started after the supervisor, whatever the clock says). Not
//!   found: one started with another environment (`env -i`), and Apple's own programs
//!   (/bin/sleep, /usr/bin/perl), whose environment macOS shows to no other process of the user.
//! - **Memory:** the sum of phys_footprint; over the limit, every process is killed and the sample
//!   says `over_memory` (from then on, for this executor).
//! - **Processes:** at most `tasks_max`, but no more than half of `kern.maxprocperuid` (the most
//!   processes one user may have), counting the supervisor; past it, every process is killed and
//!   the sample says `over_tasks`. A backstop between samples: the supervisor starts with
//!   RLIMIT_NPROC (soft and hard) at the user's process count, itself included, plus that limit.
//!   The kernel counts it per user, so a job is refused forks earlier when the user's other
//!   processes grow meanwhile. When they don't, it gets at least one more than its limit, enough
//!   to be seen over it (the kernel's own count can run a few below the processes listed, which
//!   adds that many; 8 to 11 where it was measured).
//! - **CPU:** per process, user and system time plus that of the children it has reaped, summed
//!   over the live processes, plus the last value seen of each process that exited without an
//!   invocation process to reap it (an orphan reaped by launchd). The result never goes down.
//!   That running total lives in the executor; a fresh executor (after a daemon restart) starts
//!   again from what the live processes show.
//! - **Signals:** to the process groups of the invocation's processes, then by pid to any process
//!   outside those groups; SIGKILL first stops everything (repeating until no new process shows
//!   up), so nothing can fork away during the kill. Pid reuse: a tracked pid is signalled only
//!   while its start time matches, and the session only while its id can't have been reused
//!   (`procs::members`); pid 0, 1, -1 (or negatives) and the caller are never signalled.
//! - **Finish** kills whatever is left of the invocation, as Linux stops the unit: a job can end
//!   with nothing killed (lost after its samples failed) and must not run on after its lease is
//!   freed. Processes still there after [`LEFT_WAIT`] make it an error, so the daemon tries it
//!   again later and reports them as stuck.
//! - **Crash reports** ([`move_reports`]) of its processes, and of short-lived children of them,
//!   move from ~/Library/Logs/DiagnosticReports into its crash dir. A child counts as one of its
//!   processes' only if it launched before a sample saw that process gone, so a later holder of
//!   the pid lends it nothing; the handle keeps those times (`gone`), so they outlive a daemon
//!   restart. A process that ended while no daemon ran is taken as gone at the first sample
//!   after. ReportCrash may write a report a few seconds after the crash; `finish` can be called
//!   again later and moves what is there by then.
//! - **Stacks** for a stall come from `/usr/bin/sample`, one second per process, at most 16
//!   processes, 8 at a time, within 30 seconds.
//!
//! Start times are microseconds since the Unix epoch, the wall clock when each process started;
//! the handle also names the boot (`kern.bootsessionuuid`), which no clock step changes. Only
//! this user's processes are visible: a setuid program in a job (and anything it starts) is
//! neither counted nor signalled.

mod crash;
mod procs;
mod spawn;
mod stacks;

pub use crash::move_reports;
pub use procs::{exists, start_time};

use super::{Executor, Launch, Sample};
use crate::types::{Handle, Millis};
use anyhow::Context;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The handle's `tracked` list keeps every process seen, dead ones too (crash reports are matched
/// against them), up to this many; beyond it the oldest dead ones are dropped.
const MAX_TRACKED: usize = 10_000;
/// Rounds of SIGSTOP before the SIGKILL, each catching processes forked since the last.
const STOP_ROUNDS: usize = 20;
/// How long `finish` waits for what it killed to go.
const LEFT_WAIT: Duration = Duration::from_secs(2);
/// An invocation's account goes once neither `sample` nor `finish` has used it for this long, so
/// a sample after the end (the daemon looks for stuck processes when a finish fails) doesn't
/// keep it. Instant doesn't advance while the Mac sleeps.
const KEEP_UNUSED: Duration = Duration::from_secs(600);

pub struct ProcessExecutor {
    /// Where ReportCrash writes this user's crash reports.
    reports_dir: Option<PathBuf>,
    /// CPU accounting per invocation, keyed by the supervisor's (pid, start time).
    accounts: Mutex<HashMap<(u32, u64), Account>>,
    /// The `POTLUCK_LEASE` that live processes of the user reparented to launchd were started
    /// with ("" for none), for those a sample has asked about.
    tags: Mutex<HashMap<Key, String>>,
}

impl Default for ProcessExecutor {
    /// Crash reports from `$HOME/Library/Logs/DiagnosticReports`.
    fn default() -> Self {
        let reports = std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join("Library/Logs/DiagnosticReports"));
        ProcessExecutor {
            reports_dir: reports,
            accounts: Mutex::new(HashMap::new()),
            tags: Mutex::new(HashMap::new()),
        }
    }
}

/// The process limit a job gets here: `tasks_max` (0: none), but at most half of
/// `kern.maxprocperuid`, so a job that spreads leaves the user's other work room to start
/// processes.
fn tasks_cap(tasks_max: u32) -> u32 {
    match procs::max_procs_per_user() {
        Some(n) if tasks_max > 0 => tasks_max.min(u32::try_from(n / 2).unwrap_or(u32::MAX).max(1)),
        _ => tasks_max,
    }
}

impl ProcessExecutor {
    /// An executor that takes crash reports from `dir` instead (tests use a temp dir).
    pub fn with_reports_dir(dir: impl Into<PathBuf>) -> Self {
        ProcessExecutor {
            reports_dir: Some(dir.into()),
            ..Default::default()
        }
    }
}

/// A process: its pid and start time.
type Key = (u32, u64);
/// A process's parent pid and CPU time in ns, its reaped children's included.
type Seen = (u32, u64);

#[derive(Default)]
struct Account {
    /// Live processes at the last sample.
    seen: HashMap<Key, Seen>,
    /// CPU of processes that exited with no invocation process to reap them.
    exited_ns: u64,
    /// The last value returned, so it never goes down.
    reported_ms: u64,
    over_memory: bool,
    /// The processes a sample saw and the limit, once a sample found too many.
    over_tasks: Option<(u32, u32)>,
    /// When `sample` or `finish` last used it.
    used: Option<Instant>,
}

/// Drops the accounts not used for [`KEEP_UNUSED`], except `keep`'s.
fn prune(accounts: &mut HashMap<Key, Account>, keep: Key) {
    accounts.retain(|k, a| *k == keep || a.used.is_none_or(|t| t.elapsed() < KEEP_UNUSED));
}

impl Account {
    /// Takes the live processes of a sample.
    fn update(&mut self, live: HashMap<Key, Seen>) -> u64 {
        let pids: HashSet<u32> = self.seen.keys().chain(live.keys()).map(|k| k.0).collect();
        for (key, (ppid, ns)) in self.seen.drain() {
            if live.contains_key(&key) {
                continue;
            }
            // A parent inside the invocation reaps it, and its time then shows in that parent's
            // children's time. Otherwise (launchd) it would be lost: keep what we saw.
            if ppid <= 1 || !pids.contains(&ppid) {
                self.exited_ns += ns;
            }
        }
        let total_ns = self.exited_ns + live.values().map(|v| v.1).sum::<u64>();
        self.seen = live;
        self.reported_ms = self.reported_ms.max(total_ns / 1_000_000);
        self.reported_ms
    }
}

/// What finding an invocation's processes takes, from its handle.
#[derive(Clone, Copy)]
struct Job<'a> {
    /// The supervisor.
    leader: Key,
    tracked: &'a [Key],
    /// The `POTLUCK_LEASE` its processes carry, new for every run ("" adopts nothing).
    tag: &'a str,
    /// The boot session the supervisor started in ("" when unknown).
    boot: &'a str,
}

fn job_of(handle: &Handle) -> anyhow::Result<Job<'_>> {
    match handle {
        Handle::Process {
            pid,
            start_time,
            tracked,
            lease,
            boot,
            ..
        } => Ok(Job {
            leader: (*pid, *start_time),
            tracked,
            tag: lease,
            boot,
        }),
        Handle::Systemd { .. } => anyhow::bail!("a systemd handle on macOS"),
    }
}

/// Keeps `gone` (pid, start, when first seen gone) for the tracked processes not in `live`: one
/// newly gone is noted at `now_us`; one seen again (a read that failed for a moment) or no longer
/// tracked is dropped. In `tracked`'s order, so an unchanged list compares equal.
fn note_gone(tracked: &[Key], gone: &mut Vec<(u32, u64, u64)>, live: &HashSet<Key>, now_us: u64) {
    let seen: HashMap<Key, u64> = gone.iter().map(|&(p, s, t)| ((p, s), t)).collect();
    *gone = tracked
        .iter()
        .filter(|k| !live.contains(k))
        .map(|&k| (k.0, k.1, seen.get(&k).copied().unwrap_or(now_us)))
        .collect();
}

impl ProcessExecutor {
    /// The invocation's live processes in `snap`, its escapes (below) included.
    fn members(&self, snap: &procs::Snapshot, job: Job) -> procs::Members {
        let escapes = self.escapes(snap, job);
        if escapes.is_empty() {
            return procs::members(snap, job.leader, job.tracked, job.boot);
        }
        let roots: Vec<Key> = job.tracked.iter().copied().chain(escapes).collect();
        procs::members(snap, job.leader, &roots, job.boot)
    }

    /// Processes of the user whose parent is launchd that were started with this run's
    /// `POTLUCK_LEASE` in their environment: the job's processes that left it and lost their
    /// parent before a sample saw them, as a daemon started by a double fork does. The lease is
    /// made for this run, so they started after its supervisor; their start times aren't
    /// compared with its, which a clock set back would mislead. (Not `POTLUCK_JOB`: a later run
    /// of the job on this machine has the same, and its supervisor's parent is launchd too.)
    fn escapes(&self, snap: &procs::Snapshot, job: Job) -> Vec<Key> {
        if job.tag.is_empty() {
            return Vec::new();
        }
        let mut tags = self.tags.lock().unwrap_or_else(|e| e.into_inner());
        tags.retain(|k, _| snap.procs.get(&k.0).is_some_and(|p| p.start == k.1));
        let mut found = Vec::new();
        for p in snap.procs.values() {
            if p.ppid != 1 {
                continue;
            }
            let key = (p.pid, p.start);
            let tag = match tags.entry(key) {
                Entry::Occupied(e) => e.into_mut(),
                // Unreadable now (it is exiting): asked again at the next sample.
                Entry::Vacant(e) => match procs::initial_env_var(p.pid, "POTLUCK_LEASE") {
                    Some(tag) => e.insert(tag),
                    None => continue,
                },
            };
            if tag == job.tag {
                found.push(key);
            }
        }
        found
    }

    /// Sends `sig` once to every process of the invocation: to their process groups, and by pid to
    /// any outside those groups (so no process gets it twice).
    fn signal_once(&self, job: Job, sig: i32) -> anyhow::Result<()> {
        let snap = procs::snapshot()?;
        let m = self.members(&snap, job);
        for &g in &m.pgids {
            procs::kill_group(g, sig);
        }
        for p in &m.procs {
            if !m.pgids.contains(&p.pgid) {
                procs::kill_pid(p.pid, sig);
            }
        }
        Ok(())
    }

    /// Kills every process of the invocation: SIGSTOP until a snapshot shows nothing new (a stopped
    /// process can't fork), then SIGKILL to all of them.
    fn kill_all(&self, job: Job) -> anyhow::Result<()> {
        let mut known: Vec<Key> = job.tracked.to_vec();
        let mut stopped: HashSet<Key> = HashSet::new();
        for round in 0..STOP_ROUNDS {
            let snap = procs::snapshot()?;
            let m = self.members(
                &snap,
                Job {
                    tracked: &known,
                    ..job
                },
            );
            if m.procs.is_empty() {
                return Ok(());
            }
            let fresh: Vec<procs::Proc> = m
                .procs
                .iter()
                .filter(|p| !stopped.contains(&(p.pid, p.start)))
                .copied()
                .collect();
            if fresh.is_empty() || round + 1 == STOP_ROUNDS {
                for &g in &m.pgids {
                    procs::kill_group(g, libc::SIGKILL);
                }
                for p in &m.procs {
                    procs::kill_pid(p.pid, libc::SIGKILL);
                }
                return Ok(());
            }
            let mut groups: Vec<u32> = Vec::new();
            for p in &fresh {
                if m.pgids.contains(&p.pgid) && !groups.contains(&p.pgid) {
                    groups.push(p.pgid);
                }
            }
            for g in groups {
                procs::kill_group(g, libc::SIGSTOP);
            }
            for p in fresh {
                procs::kill_pid(p.pid, libc::SIGSTOP);
                stopped.insert((p.pid, p.start));
                known.push((p.pid, p.start));
            }
        }
        Ok(())
    }

    /// Kills what is left of the invocation and waits up to [`LEFT_WAIT`] for it to go; an
    /// error names what didn't.
    fn kill_left(&self, job: Job) -> anyhow::Result<()> {
        if let Err(e) = self.kill_all(job) {
            // No snapshot: the supervisor's group at least, while it is the process we started.
            if procs::start_time(job.leader.0) == Some(job.leader.1) {
                procs::kill_group(job.leader.0, libc::SIGKILL);
            }
            return Err(e.context("killing what is left of it"));
        }
        let deadline = Instant::now() + LEFT_WAIT;
        loop {
            let snap = procs::snapshot().context("looking for what is left of it")?;
            let m = self.members(&snap, job);
            if m.procs.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let pids: Vec<u32> = m.procs.iter().map(|p| p.pid).collect();
                anyhow::bail!("processes {pids:?} are left after SIGKILL");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Executor for ProcessExecutor {
    fn start(&self, launch: &Launch) -> anyhow::Result<Handle> {
        if launch.job.spec.cmd.is_empty() {
            anyhow::bail!("the job has no command");
        }
        let log_max = launch.limits.log_max_bytes.to_string();
        let mut args: Vec<&OsStr> = vec![OsStr::new("_supervise")];
        if launch.limits.log_max_bytes > 0 {
            args.extend([OsStr::new("--log-max"), OsStr::new(&log_max)]);
        }
        args.extend([launch.status.as_os_str(), OsStr::new("--")]);
        args.extend(launch.job.spec.cmd.iter().map(OsStr::new));
        let tasks_max = tasks_cap(launch.limits.tasks_max);
        let nproc = if tasks_max == 0 {
            None
        } else {
            let n = procs::user_process_count().context("counting this user's processes")?;
            // The supervisor, about to start, counts too.
            Some(n + 1 + u64::from(tasks_max))
        };
        let lease = launch.env.get("POTLUCK_LEASE").cloned().unwrap_or_default();
        let boot = procs::boot_session().unwrap_or_default().to_string();
        let extra = record_extra(tasks_max, &boot, &lease);
        let started = spawn::spawn(&spawn::Spawn {
            program: &launch.potluck_bin,
            args,
            env: &launch.env,
            cwd: &launch.cwd,
            log: &launch.log,
            nproc,
            record: &record_path(&launch.status),
            record_extra: &extra,
        })?;
        Ok(Handle::Process {
            pid: started.pid,
            start_time: started.start,
            tracked: vec![(started.pid, started.start)],
            gone: Vec::new(),
            lease,
            tasks_max,
            boot,
        })
    }

    fn sample(&self, handle: &mut Handle, mem_max_bytes: u64) -> anyhow::Result<Sample> {
        let Handle::Process {
            pid,
            start_time,
            tracked,
            gone,
            lease,
            tasks_max,
            boot,
        } = handle
        else {
            anyhow::bail!("a systemd handle on macOS");
        };
        let leader = (*pid, *start_time);
        if procs::stopped(leader.0, leader.1) {
            procs::kill_pid(leader.0, libc::SIGCONT);
        }
        let snap = procs::snapshot()?;
        let m = self.members(
            &snap,
            Job {
                leader,
                tracked,
                tag: lease,
                boot,
            },
        );

        let known: HashSet<Key> = tracked.iter().copied().collect();
        tracked.extend(
            m.procs
                .iter()
                .map(|p| (p.pid, p.start))
                .filter(|k| !known.contains(k)),
        );
        let live: HashSet<Key> = m.procs.iter().map(|p| (p.pid, p.start)).collect();
        if tracked.len() > MAX_TRACKED {
            let mut excess = tracked.len() - MAX_TRACKED;
            tracked.retain(|k| {
                if excess > 0 && *k != leader && !live.contains(k) {
                    excess -= 1;
                    false
                } else {
                    true
                }
            });
        }
        note_gone(tracked, gone, &live, now_us());

        let mut mem = 0u64;
        let mut usage = HashMap::with_capacity(m.procs.len());
        for p in &m.procs {
            if let Some(u) = procs::usage(p.pid) {
                mem += u.footprint;
                usage.insert((p.pid, p.start), (p.ppid, u.cpu_ns));
            }
        }
        let over = mem_max_bytes > 0 && mem > mem_max_bytes;
        let count = u32::try_from(m.procs.len()).unwrap_or(u32::MAX);
        let over_tasks = *tasks_max > 0 && count > *tasks_max;
        if over || over_tasks {
            self.kill_all(Job {
                leader,
                tracked,
                tag: lease,
                boot,
            })?;
        }
        let mut accounts = self.accounts.lock().unwrap_or_else(|e| e.into_inner());
        prune(&mut accounts, leader);
        let account = accounts.entry(leader).or_default();
        account.used = Some(Instant::now());
        let cpu_ms = account.update(usage);
        account.over_memory |= over;
        if over_tasks && account.over_tasks.is_none() {
            account.over_tasks = Some((count, *tasks_max));
        }
        Ok(Sample {
            pids: m.procs.iter().map(|p| p.pid).collect(),
            cpu_ms,
            mem_bytes: mem,
            peak_mem_bytes: 0,
            over_memory: account.over_memory,
            oom_killed: false,
            over_tasks: account.over_tasks,
        })
    }

    fn signal(&self, handle: &Handle, sig: i32) -> anyhow::Result<()> {
        let job = job_of(handle)?;
        if sig == libc::SIGKILL {
            self.kill_all(job)
        } else {
            self.signal_once(job, sig)
        }
    }

    fn stacks(&self, handle: &Handle) -> anyhow::Result<String> {
        let job = job_of(handle)?;
        let snap = procs::snapshot()?;
        let m = self.members(&snap, job);
        // The supervisor only waits for its command: skip it.
        let list: Vec<(u32, String)> = m
            .procs
            .iter()
            .filter(|p| p.pid != job.leader.0)
            .map(|p| (p.pid, procs::name(p.pid)))
            .collect();
        if list.is_empty() {
            return Ok("no processes to sample\n".to_string());
        }
        stacks::sample_all(&list)
    }

    /// The supervisor from the record it wrote beside `status` before it exec'd, if it started
    /// since the claim, with the process limit, lease and boot its start gave the handle.
    fn find(&self, _name: &str, status: &Path, since: Millis) -> anyhow::Result<Option<Handle>> {
        let bytes = match std::fs::read(record_path(status)) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(spawn::read_record(&bytes)
            .filter(|(s, _)| s.start / 1000 >= since)
            .map(|(s, extra)| {
                let (tasks_max, boot, lease) = read_record_extra(extra).unwrap_or_default();
                Handle::Process {
                    pid: s.pid,
                    start_time: s.start,
                    tracked: vec![(s.pid, s.start)],
                    gone: Vec::new(),
                    lease,
                    tasks_max,
                    boot,
                }
            }))
    }

    fn finish(&self, handle: &Handle, crash_dir: &Path, _since: Millis) -> anyhow::Result<()> {
        let job = job_of(handle)?;
        {
            let mut accounts = self.accounts.lock().unwrap_or_else(|e| e.into_inner());
            prune(&mut accounts, job.leader);
            if let Some(a) = accounts.get_mut(&job.leader) {
                a.used = Some(Instant::now());
            }
        }
        let left = self.kill_left(job);
        if let Some(dir) = &self.reports_dir {
            let gone: HashMap<Key, u64> = match handle {
                Handle::Process { gone, .. } => gone.iter().map(|&(p, s, t)| ((p, s), t)).collect(),
                Handle::Systemd { .. } => HashMap::new(),
            };
            crash::move_reports(dir, job.leader.1, job.tracked, &gone, crash_dir)?;
        }
        left
    }
}

/// What the supervisor's record holds after its pid and start time: the handle's process limit
/// (u32, native byte order), boot session (a length byte, then the bytes) and lease (the rest).
/// The daemon after a restart in place or an update reads records an older binary wrote, so a
/// change must stay readable across one version step.
fn record_extra(tasks_max: u32, boot: &str, lease: &str) -> Vec<u8> {
    let boot = &boot.as_bytes()[..boot.len().min(255)];
    let mut b = tasks_max.to_ne_bytes().to_vec();
    b.push(boot.len() as u8);
    b.extend_from_slice(boot);
    b.extend_from_slice(lease.as_bytes());
    b
}

/// The process limit, boot session and lease from [`record_extra`]'s bytes; None for a record
/// without them (written by an older potluck) or one cut short.
fn read_record_extra(b: &[u8]) -> Option<(u32, String, String)> {
    let tasks_max = u32::from_ne_bytes(b.get(..4)?.try_into().ok()?);
    let n = usize::from(*b.get(4)?);
    let boot = std::str::from_utf8(b.get(5..5 + n)?).ok()?;
    let lease = std::str::from_utf8(&b[5 + n..]).ok()?;
    Some((tasks_max, boot.to_string(), lease.to_string()))
}

/// Where the supervisor of the invocation with this status file records its pid and start time:
/// `status.pid` beside `status.json`.
fn record_path(status: &Path) -> PathBuf {
    status.with_extension("pid")
}

/// Microseconds since the Unix epoch, the unit of process start times.
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounts_unused_for_ten_minutes_go() {
        let ago = |secs| Instant::now().checked_sub(Duration::from_secs(secs));
        let (Some(old), Some(recent)) = (ago(KEEP_UNUSED.as_secs() + 1), ago(5)) else {
            return; // booted less than ten minutes ago
        };
        let account = |used| Account {
            used: Some(used),
            ..Default::default()
        };
        let mut accounts = HashMap::from([
            ((10, 1), account(old)),
            ((20, 2), account(recent)),
            ((30, 3), account(old)),
        ]);
        prune(&mut accounts, (30, 3));
        let mut left: Vec<Key> = accounts.into_keys().collect();
        left.sort_unstable();
        assert_eq!(left, [(20, 2), (30, 3)]);
    }

    #[test]
    fn the_record_keeps_the_handles_limit_lease_and_boot() {
        let b = record_extra(5333, "57AACF38-452D-49B7-85B0-2B0DF49A292C", "0f1e");
        assert_eq!(
            read_record_extra(&b),
            Some((
                5333,
                "57AACF38-452D-49B7-85B0-2B0DF49A292C".to_string(),
                "0f1e".to_string()
            ))
        );
        assert_eq!(
            read_record_extra(&record_extra(0, "", "")),
            Some((0, String::new(), String::new()))
        );
        // An older record has nothing after the start time; a cut-short one, part of it.
        assert_eq!(read_record_extra(&[]), None);
        assert_eq!(read_record_extra(&b[..10]), None);
    }

    /// A record as the supervisor writes it today, byte by byte: a change that the next daemon
    /// (after a restart in place) would misread fails here.
    #[test]
    fn a_record_in_todays_format_is_read() {
        let mut b = 4321u32.to_ne_bytes().to_vec();
        b.extend_from_slice(&99u64.to_ne_bytes());
        b.extend_from_slice(&5333u32.to_ne_bytes());
        b.push(4);
        b.extend_from_slice(b"boot");
        b.extend_from_slice(b"0f1e");
        let (started, extra) = spawn::read_record(&b).unwrap();
        assert_eq!((started.pid, started.start), (4321, 99));
        assert_eq!(
            read_record_extra(extra),
            Some((5333, "boot".to_string(), "0f1e".to_string()))
        );
    }

    #[test]
    fn processes_are_seen_gone_once() {
        let live = |v: &[Key]| v.iter().copied().collect::<HashSet<Key>>();
        let tracked = [(10, 1), (20, 2)];
        let mut gone = Vec::new();
        note_gone(&tracked, &mut gone, &live(&[(10, 1), (20, 2)]), 100);
        note_gone(&tracked, &mut gone, &live(&[(10, 1)]), 200);
        note_gone(&tracked, &mut gone, &live(&[]), 300);
        note_gone(&tracked, &mut gone, &live(&[]), 400);
        assert_eq!(gone, [(10, 1, 300), (20, 2, 200)]);
        // One that shows up again was not gone; one no longer tracked is forgotten.
        let tracked = [(20, 2), (30, 3)];
        note_gone(&tracked, &mut gone, &live(&[]), 500);
        note_gone(&tracked, &mut gone, &live(&[(30, 3)]), 600);
        assert_eq!(gone, [(20, 2, 200)]);
        // A process that ended while no daemon ran: gone at the first sample after.
        let mut fresh = Vec::new();
        note_gone(&tracked, &mut fresh, &live(&[]), 700);
        assert_eq!(fresh, [(20, 2, 700), (30, 3, 700)]);
    }

    #[test]
    fn cpu_is_kept_when_processes_exit() {
        let mut a = Account::default();
        let ms = 1_000_000;
        // Supervisor 10 with child 20 (reaped by 10 later) and orphan 30 (parent launchd).
        let live = |v: &[(Key, Seen)]| v.iter().copied().collect();
        assert_eq!(
            a.update(live(&[
                ((10, 1), (1, 5 * ms)),
                ((20, 2), (10, 100 * ms)),
                ((30, 3), (1, 50 * ms))
            ])),
            155
        );
        // 20 exited and was reaped: its time shows in 10's children's time.
        assert_eq!(
            a.update(live(&[((10, 1), (1, 110 * ms)), ((30, 3), (1, 60 * ms))])),
            170
        );
        // 20 exited but 10 hasn't reaped it yet: the total may not drop.
        let mut b = Account::default();
        b.update(live(&[((10, 1), (1, 5 * ms)), ((20, 2), (10, 100 * ms))]));
        assert_eq!(b.update(live(&[((10, 1), (1, 5 * ms))])), 105);
        // 30, an orphan, exited: launchd reaped it, so its last value is kept.
        assert_eq!(a.update(live(&[((10, 1), (1, 120 * ms))])), 180);
        assert_eq!(a.update(live(&[])), 180);
    }
}
