//! Running an invocation's processes: one executor per platform behind one trait.
//!
//! On both platforms the command runs under `potluck _supervise` ([`crate::supervise`]), which
//! copies its output into the log through a pipe and writes its exit status to the invocation's
//! status file. When the command exits, anything it left
//! running is killed: an invocation ends when its command does. Over its RAM limit it is ended and
//! reported as over memory.

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;

use crate::types::{Grant, Handle, JobRecord};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Everything an executor needs to start an invocation.
pub struct Launch<'a> {
    pub job: &'a JobRecord,
    pub grant: &'a Grant,
    /// The workspace root: the command's working directory.
    pub cwd: PathBuf,
    /// The final environment (the daemon builds it; executors add nothing).
    pub env: BTreeMap<String, String>,
    /// The supervisor's stdout and stderr (appended); stdin is /dev/null. The supervisor copies
    /// the command's stdout and stderr here through a pipe ([`crate::supervise`]).
    pub log: PathBuf,
    /// Where the supervisor writes the exit status.
    pub status: PathBuf,
    /// The `potluck` binary to run as `_supervise` (the daemon's own executable).
    pub potluck_bin: PathBuf,
    /// A name unique on this machine, e.g. `potluck-studio-42` (the Linux unit name).
    pub name: String,
    pub limits: Limits,
}

/// What an invocation is held to.
pub struct Limits {
    /// Linux MemoryMax; macOS polled footprint.
    pub mem_max_bytes: u64,
    /// Linux CPUWeight.
    pub cpu_weight: u32,
    /// Linux TasksMax (threads count). macOS: the most processes, the supervisor included; the
    /// monitor ends the job past it.
    pub tasks_max: u32,
    /// The grace between SIGTERM and SIGKILL (the daemon's kill_grace_s). Linux: the unit's
    /// TimeoutStopSec, for what is left of the invocation once its command has ended.
    pub kill_grace_ms: u64,
    /// How many bytes of the command's output the log keeps before the supervisor drops the rest
    /// (all but the last 16 MiB, added when the command ends); 0: no limit.
    pub log_max_bytes: u64,
}

/// One sample of a running invocation.
#[derive(Clone, Debug, Default)]
pub struct Sample {
    /// Live pids of the invocation, supervisor included. Empty once everything has exited.
    pub pids: Vec<u32>,
    pub cpu_ms: u64,
    pub mem_bytes: u64,
    /// The most memory the invocation has used since it started, where the platform keeps
    /// count (Linux: the cgroup's peak); 0 when unknown. Samples a second apart miss a quick
    /// climb to the limit, so `mem_bytes` alone can say a job killed at its limit used far less.
    pub peak_mem_bytes: u64,
    /// The kernel (Linux) or the monitor (macOS) ended it for going over its RAM limit.
    pub over_memory: bool,
    /// Linux: the kernel's OOM killer or systemd-oomd ended a process of it while it was under
    /// its own RAM limit, because the machine (or a group of units it runs in) ran short of
    /// memory. Never set together with `over_memory`.
    pub oom_killed: bool,
    /// macOS: the monitor ended it for running more processes than its limit: (how many it
    /// saw, the limit). Linux's TasksMax refuses forks past its limit instead and never sets it.
    pub over_tasks: Option<(u32, u32)>,
}

pub trait Executor: Send + Sync {
    /// Start the invocation and return how to find it again. Must not wait for it.
    fn start(&self, launch: &Launch) -> anyhow::Result<Handle>;
    /// Sample it. macOS also updates the handle's tracked pids and enforces the RAM and process
    /// limits (it ends the invocation and reports `over_memory` or `over_tasks`), so call this
    /// every second or so.
    fn sample(&self, handle: &mut Handle, mem_max_bytes: u64) -> anyhow::Result<Sample>;
    /// Send `sig` to every process of the invocation.
    fn signal(&self, handle: &Handle, sig: i32) -> anyhow::Result<()>;
    /// Stacks of the invocation's processes as text, for a stalled job's log. Best effort.
    fn stacks(&self, handle: &Handle) -> anyhow::Result<String>;
    /// After it is over: SIGKILL whatever of it is still running, drop platform state (reset the
    /// unit) and move its crash reports into `crash_dir`. `Err` when processes of it are left
    /// after the SIGKILL: the daemon lists them as stuck, releases the lease anyway and calls
    /// this again later with the same handle. `since` (milliseconds since the Unix epoch) is when
    /// the job was claimed: a crash before it is an earlier run's (a job, and so its launch name,
    /// can come back to a machine it ran on). macOS matches crash reports to processes it saw
    /// instead.
    fn finish(
        &self,
        handle: &Handle,
        crash_dir: &std::path::Path,
        since: crate::types::Millis,
    ) -> anyhow::Result<()>;
    /// After a daemon restart: the invocation that a `start` with this launch `name` and `status`
    /// path left, when the daemon stopped before it had the handle; `None` when that start
    /// started nothing. Running or ended, it counts. `since` (milliseconds since the Unix epoch)
    /// is when the job was claimed: files from before it are an earlier attempt's (a job can
    /// come back to a machine it ran on). An executor that can't tell says `None`.
    fn find(
        &self,
        name: &str,
        status: &std::path::Path,
        since: crate::types::Millis,
    ) -> anyhow::Result<Option<Handle>> {
        let _ = (name, status, since);
        Ok(None)
    }
}

/// This platform's executor.
pub fn platform() -> Box<dyn Executor> {
    #[cfg(target_os = "linux")]
    return Box::new(linux::SystemdExecutor);
    #[cfg(target_os = "macos")]
    return Box::new(macos::ProcessExecutor::default());
}
