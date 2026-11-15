//! Windows: each invocation runs in a job object of its own.
//!
//! - **Start.** [`JobExecutor::start`] makes a job object named for the run (below), writes that
//!   name beside the status file (`status.job`, for [`find`](JobExecutor::find)), and starts the
//!   supervisor (`potluck _supervise --job NAME STATUS -- CMD...`) in it with `CreateProcessW`
//!   ([`spawn`]): in the job from its first instruction (PROC_THREAD_ATTRIBUTE_JOB_LIST), with a
//!   console of its own that has no window (CREATE_NO_WINDOW), stdin NUL, stdout and stderr the
//!   log (appended), no other handle of the daemon's, the launch's environment as its own and the
//!   workspace as its working directory. The supervisor starts the command on that console, in a
//!   console process group and a nested job object of its own ([`crate::supervise`]).
//! - **Names.** `Local\potluck-JOB-HASH-NONCE`: the launch name, 64 bits of the SHA-256 of the
//!   status file's path (the state directory and the job: a test runs many daemons, and a job id
//!   can be on several of them) and the start's time in microseconds (a job can come back to a
//!   machine it ran on, and an earlier run's job object can still be there, with processes that
//!   wouldn't die; [`find`](JobExecutor::find) reads it back to tell a start since the claim from
//!   an earlier attempt's). `Local\` is the session's namespace: a daemon restarted in the same
//!   session (the service's keeper, or a restart in place) finds it again.
//! - **Who holds it.** A job object lives, and its name can be opened, while a handle to it is
//!   open. The daemon opens it by name for each call and closes it after; the supervisor holds it
//!   from its start until it exits (the daemon gives it a handle before it lets go of its own).
//!   The job has JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: once the supervisor has exited and no call
//!   holds it, whatever is left in it is killed, as systemd stops a unit whose main process has
//!   exited. So no process of a job outlives its supervisor by more than a call, whether or not a
//!   daemon runs.
//! - **Leaving the daemon's job.** A daemon run as a service runs in its keeper's job object
//!   (which kills it when the service stops). The supervisor leaves it
//!   (CREATE_BREAKAWAY_FROM_JOB), so jobs outlive the daemon; when that job doesn't allow it, the
//!   supervisor starts within it, and its jobs end with that job. The job objects potluck makes
//!   allow no process to leave them.
//! - **RAM** is the job's commit limit (JOB_OBJECT_LIMIT_JOB_MEMORY): an allocation past it
//!   fails. The supervisor watches the job's completion port and ends the command and what it
//!   started when one does, and records that in the status file, which `sample` reads
//!   (`over_memory`). The peak is the job's PeakJobMemoryUsed; the memory now is the sum of its
//!   processes' commit (private bytes).
//! - **Processes:** at most `tasks_max` at once, the supervisor included
//!   (JOB_OBJECT_LIMIT_ACTIVE_PROCESS): starting one more fails.
//! - **CPU:** the user and kernel time of every process the job ever held. `--cpu` sets nothing.
//! - **Stop and kill.** [`signal`](JobExecutor::signal) with [`SIGKILL`] ends every process in the
//!   job (TerminateJobObject, exit code 137). Any other signal sets the event `NAME.stop`, on
//!   which the supervisor sends CTRL_BREAK to the command's process group.
//! - **Crashes:** an unhandled exception ends the process at once with the exception code as its
//!   exit code (JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION: no error-reporting dialog holds it).
//!   No crash reports are collected.
//! - **Stacks:** not sampled; `stacks` lists the job's processes with their image, threads and
//!   CPU time.

pub(crate) mod job;
pub(crate) mod spawn;

pub use job::{ProcessInfo, process_info};

use super::{Executor, Launch, SIGKILL, Sample};
use crate::supervise::JobStatus;
use crate::types::{Handle, Millis};
use anyhow::{Context, bail};
use std::collections::HashMap;
use std::ffi::OsString;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use windows_sys::Win32::System::SystemServices::{JOB_OBJECT_QUERY, JOB_OBJECT_TERMINATE};
use windows_sys::Win32::System::Threading::EVENT_MODIFY_STATE;

/// How long `finish` waits for what it killed to go.
const LEFT_WAIT: Duration = Duration::from_secs(2);
/// The longest launch name kept in a job object's name (which may be MAX_PATH long).
const NAME_STEM_MAX: usize = 120;

#[derive(Default)]
pub struct JobExecutor {
    /// Job objects whose processes outlived a `finish`, by name: held open so the name still
    /// finds them when the daemon calls `finish` again.
    stuck: Mutex<HashMap<String, crate::sys::win::Owned>>,
}

impl Executor for JobExecutor {
    fn start(&self, launch: &Launch) -> anyhow::Result<Handle> {
        for (what, path) in [
            ("working directory", &launch.cwd),
            ("log", &launch.log),
            ("status file", &launch.status),
            ("potluck binary", &launch.potluck_bin),
        ] {
            if !path.is_absolute() {
                bail!("the {what} must be an absolute path: {}", path.display());
            }
        }
        if !launch.potluck_bin.is_file() {
            bail!("no potluck binary at {}", launch.potluck_bin.display());
        }
        if launch.job.spec.cmd.is_empty() {
            bail!("job {} has no command", launch.job.id);
        }
        let name = job_name(&launch.name, &launch.status, now_us());
        // First, so a restart that cuts this start short finds what it made.
        write_record(&launch.status, &name)?;
        let job = job::create(&name, &launch.limits)
            .with_context(|| format!("could not make job object {name}"))?;
        let stop = job::create_stop_event(&name)
            .with_context(|| format!("could not make the stop event of job object {name}"))?;
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&launch.log)
            .with_context(|| format!("could not open {}", launch.log.display()))?;
        let nul = std::fs::File::open("NUL").context("could not open NUL")?;
        let mut args: Vec<OsString> = vec!["_supervise".into()];
        if launch.limits.log_max_bytes > 0 {
            args.push("--log-max".into());
            args.push(launch.limits.log_max_bytes.to_string().into());
        }
        args.push("--job".into());
        args.push(name.clone().into());
        args.push(launch.status.clone().into());
        args.push("--".into());
        args.extend(launch.job.spec.cmd.iter().map(OsString::from));
        let child = spawn::spawn(&spawn::Spawn {
            program: &launch.potluck_bin,
            args: &args,
            env: &launch.env,
            cwd: &launch.cwd,
            stdin: &nul,
            stdout: &log,
            job: job.0,
        })
        .with_context(|| format!("could not start {}", launch.potluck_bin.display()))?;
        // The supervisor holds the job and its stop event from here on: when this function lets go
        // of its own handles, they outlive it however soon its supervisor gets to open them.
        let given =
            spawn::give(child.process.0, job.0).and_then(|()| spawn::give(child.process.0, stop.0));
        if let Err(e) = given {
            let _ = job::terminate(job.0);
            return Err(e).context("could not hand the job object to its supervisor");
        }
        Ok(Handle::JobObject {
            name,
            status: launch.status.clone(),
        })
    }

    fn sample(&self, handle: &mut Handle, _mem_max_bytes: u64) -> anyhow::Result<Sample> {
        let (name, status) = parts(handle)?;
        let mut sample = Sample::default();
        // What the supervisor recorded when its command ended: whether it went over its RAM, and
        // the job's last account, which outlives the job object.
        if let Some(end) = read_status(status) {
            sample.over_memory = end.over_memory;
            sample.cpu_ms = end.cpu_ms;
            sample.peak_mem_bytes = end.peak_mem_bytes;
        }
        let Some(job) = open_job(name, JOB_OBJECT_QUERY)? else {
            // Gone: everything in it has ended.
            return Ok(sample);
        };
        let pids = job::live_pids(job.0)?;
        let account = job::accounting(job.0)?;
        sample.cpu_ms = sample.cpu_ms.max(job::cpu_ms(&account));
        sample.peak_mem_bytes = sample
            .peak_mem_bytes
            .max(job::limits(job.0)?.PeakJobMemoryUsed as u64);
        sample.mem_bytes = pids
            .iter()
            .filter_map(|&p| job::process_use(p))
            .map(|u| u.commit)
            .sum();
        sample.pids = pids;
        Ok(sample)
    }

    fn signal(&self, handle: &Handle, sig: i32) -> anyhow::Result<()> {
        let (name, _) = parts(handle)?;
        if sig == SIGKILL {
            if let Some(job) = open_job(name, JOB_OBJECT_TERMINATE | JOB_OBJECT_QUERY)? {
                job::terminate(job.0)
                    .with_context(|| format!("could not end job object {name}"))?;
            }
            return Ok(());
        }
        // No event: its supervisor has exited, and nothing is left to stop.
        if let Some(event) = job::open_stop_event(name, EVENT_MODIFY_STATE)? {
            job::set_event(event.0)
                .with_context(|| format!("could not ask job object {name} to stop"))?;
        }
        Ok(())
    }

    fn stacks(&self, handle: &Handle) -> anyhow::Result<String> {
        let (name, _) = parts(handle)?;
        let pids = match open_job(name, JOB_OBJECT_QUERY)? {
            Some(job) => job::live_pids(job.0)?,
            None => Vec::new(),
        };
        if pids.is_empty() {
            return Ok("no processes left\n".to_string());
        }
        let mut text = String::from("stacks are not sampled on Windows; the job's processes:\n");
        for p in job::process_info(&pids)? {
            let cpu = job::process_use(p.pid).map_or(0, |u| u.cpu_ms);
            text.push_str(&format!(
                "pid {} (parent {}) {}: {} threads, CPU {}.{:03}s\n",
                p.pid,
                p.parent,
                p.image,
                p.threads,
                cpu / 1000,
                cpu % 1000
            ));
        }
        Ok(text)
    }

    /// Ends whatever is left in the job and waits up to [`LEFT_WAIT`] for it to go; processes
    /// still there make it an error, and the job is held open until a later call finds them
    /// gone. Windows keeps no crash reports to move into `crash_dir`.
    fn finish(&self, handle: &Handle, _crash_dir: &Path, _since: Millis) -> anyhow::Result<()> {
        let (name, _) = parts(handle)?;
        let held = self
            .stuck
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name);
        let job = match held {
            Some(job) => job,
            None => match open_job(name, JOB_OBJECT_TERMINATE | JOB_OBJECT_QUERY)? {
                Some(job) => job,
                None => return Ok(()),
            },
        };
        let deadline = Instant::now() + LEFT_WAIT;
        loop {
            let left = job::live_pids(job.0)?;
            if left.is_empty() {
                return Ok(());
            }
            let _ = job::terminate(job.0);
            if Instant::now() >= deadline {
                self.stuck
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(name.to_string(), job);
                bail!("processes {left:?} are left after TerminateJobObject");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The job object a start with this status file named in its record (`status.job`), when that
    /// start began since the claim (the time in the name): while the job object exists, or when
    /// the supervisor recorded in `status` an end since the claim (it ran and ended, and the job
    /// object went with it). Times come from the files' contents, not their modification times:
    /// Windows stamps a file from a clock that advances once per timer tick (up to 15.6 ms), so a
    /// file written just after the claim can look older than it.
    fn find(&self, _name: &str, status: &Path, since: Millis) -> anyhow::Result<Option<Handle>> {
        let record = record_path(status);
        let name = match std::fs::read_to_string(&record) {
            Ok(text) => text.trim().to_string(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading {}", record.display())),
        };
        if started_at(&name).is_none_or(|t| t < since) {
            // An earlier attempt's, or one cut short while it was being written.
            return Ok(None);
        }
        let handle = Handle::JobObject {
            name: name.clone(),
            status: status.to_path_buf(),
        };
        if open_job(&name, JOB_OBJECT_QUERY)?.is_some()
            || read_status(status).is_some_and(|end| end.exit.ended_at >= since)
        {
            return Ok(Some(handle));
        }
        Ok(None)
    }
}

/// The job object `name`, opened with `access`; `None` when it is gone.
fn open_job(name: &str, access: u32) -> anyhow::Result<Option<crate::sys::win::Owned>> {
    job::open(name, access).with_context(|| format!("could not open job object {name}"))
}

fn parts(handle: &Handle) -> anyhow::Result<(&str, &Path)> {
    match handle {
        Handle::JobObject { name, status } => Ok((name, status)),
        other => bail!("not a Windows job object: {other:?}"),
    }
}

/// The job object's name for a run of the launch `launch` with status file `status`, started
/// at `nonce` (microseconds since the Unix epoch).
fn job_name(launch: &str, status: &Path, nonce: u64) -> String {
    let path: Vec<u8> = status
        .as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect();
    let digest = ring::digest::digest(&ring::digest::SHA256, &path);
    let tag: String = digest.as_ref()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    // A name may hold any character but `\`; these keep it readable in tools that list them.
    let stem: String = launch
        .chars()
        .take(NAME_STEM_MAX)
        .map(|c| {
            if c.is_ascii_alphanumeric() || "-_.".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("Local\\{stem}-{tag}-{nonce:x}")
}

/// Where a start records its job object's name: `status.job` beside `status.json`.
fn record_path(status: &Path) -> PathBuf {
    status.with_extension("job")
}

/// Writes the record atomically (a temporary file, then a rename).
fn write_record(status: &Path, name: &str) -> anyhow::Result<()> {
    let record = record_path(status);
    let tmp = status.with_extension("job.tmp");
    std::fs::write(&tmp, format!("{name}\n"))
        .and_then(|()| std::fs::rename(&tmp, &record))
        .with_context(|| format!("writing {}", record.display()))
}

/// What the supervisor wrote to `status`, if it has.
fn read_status(status: &Path) -> Option<JobStatus> {
    serde_json::from_slice(&std::fs::read(status).ok()?).ok()
}

/// When the start that named job object `name` began, in ms since the Unix epoch: its nonce
/// ([`job_name`]); `None` for a name not of that form.
fn started_at(name: &str) -> Option<Millis> {
    let (_, nonce) = name.rsplit_once('-')?;
    let us = u64::from_str_radix(nonce, 16).ok()?;
    Some(us / 1000)
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_names_are_per_state_directory_and_per_run() {
        let a = job_name(
            "potluck-a-1",
            Path::new(r"D:\one\jobs\a-1\status.json"),
            0x10,
        );
        let b = job_name(
            "potluck-a-1",
            Path::new(r"D:\two\jobs\a-1\status.json"),
            0x10,
        );
        let c = job_name(
            "potluck-a-1",
            Path::new(r"D:\one\jobs\a-1\status.json"),
            0x11,
        );
        assert!(a.starts_with("Local\\potluck-a-1-"), "{a}");
        assert!(a.ends_with("-10"), "{a}");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.matches('\\').count(), 1, "{a}");
        let odd = job_name("potluck-ä/b\\c", Path::new(r"D:\x"), 1);
        assert!(odd.starts_with("Local\\potluck-__b_c-"), "{odd}");
    }

    #[test]
    fn a_job_name_says_when_its_start_began() {
        let us = 1_763_200_000_123_456;
        let name = job_name("potluck-a-1", Path::new(r"D:\one\jobs\a-1\status.json"), us);
        assert_eq!(started_at(&name), Some(1_763_200_000_123));
        assert_eq!(started_at("Local\\potluck-a-1-x"), None);
        assert_eq!(started_at(""), None);
    }
}
