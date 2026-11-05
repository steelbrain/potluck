//! Linux: each invocation is a transient systemd user unit.
//!
//! [`SystemdExecutor::start`] runs, in effect:
//!
//! ```text
//! systemd-run --user --unit=NAME.service --service-type=exec --expand-environment=no
//!     -p MemoryMax=RAM -p MemorySwapMax=0 -p CPUWeight=W -p TasksMax=N
//!     -p OOMPolicy=stop -p KillMode=control-group -p TimeoutStopSec=GRACE -p CollectMode=inactive
//!     -p WorkingDirectory=CWD -p StandardInput=null -p StandardOutput=append:LOG
//!     -p StandardError=inherit -p LimitNOFILE=SOFT:HARD
//!     -- /usr/bin/env -i -- POTLUCK _supervise --env-file ENVFILE [--log-max BYTES] STATUS -- CMD...
//! ```
//!
//! and returns once the supervisor has been exec'd. Everything after that works from the
//! [`Handle`] alone (unit name, cgroup path and InvocationID), so a restarted daemon can adopt the
//! unit: the unit belongs to the user manager, not to the daemon's own unit, and outlives it.
//!
//! Choices, each checked on systemd 262:
//! - **MemorySwapMax=0.** With swap available (zram on many desktops) a job over MemoryMax is not
//!   killed: the kernel moves its pages to swap and the job runs on, past its `ram`. `ram` is a
//!   hard limit, so the job gets no swap and the OOM killer acts at MemoryMax.
//! - **OOMPolicy=stop.** When the OOM killer ends a process of the job, systemd stops the whole
//!   unit and records Result=oom-kill. The supervisor survives the SIGTERM of that stop and
//!   records how its command ended.
//! - **Whose shortage.** The OOM killer also acts when the machine, or a slice above the unit,
//!   runs short of memory, and systemd-oomd kills units under memory pressure; systemd records
//!   Result=oom-kill for all of these. `sample` reports `over_memory` only when the unit reached
//!   its own MemoryMax: while the cgroup exists, its memory.events `oom` count (raised for a
//!   limit of this cgroup or below it, not above); once it is gone, the unit's MemoryPeak against
//!   its MemoryMax, as systemd keeps both (systemd 256 and later; before that every oom-kill
//!   counts as over its limit). Any other OOM kill is `oom_killed`.
//! - **The environment.** systemd gives every unit the user manager's environment (a desktop
//!   session's DISPLAY, DBUS_SESSION_BUS_ADDRESS, ...) plus its own (INVOCATION_ID,
//!   MEMORY_PRESSURE_WATCH, ...), and no unit property clears it. `env -i` starts the supervisor
//!   with none, and the supervisor gives the command exactly `Launch.env`, read from ENVFILE
//!   ([`crate::supervise`]). The values never go on a command line: systemd-run's own is
//!   readable by every user in /proc, and the unit keeps its ExecStart. ENVFILE is
//!   `$XDG_RUNTIME_DIR/potluck-env/UNIT` (a tmpfs only this user can read, emptied at reboot),
//!   mode 0600; the supervisor removes it once read, and so do a failed start, [`finish`]
//!   (a supervisor that died before reading it) and later starts (one older than ten minutes,
//!   left by a daemon that died while starting a unit). The unit's description is the job id,
//!   so the journal doesn't log the command line.
//!
//! [`finish`]: SystemdExecutor::finish
//! - **No expansion.** systemd-run expands `$VAR` in the command line by default; potluck passes
//!   arguments through untouched: `--expand-environment=no` on systemd 254+, every `$` doubled on
//!   older ones.
//! - **No `--collect`.** A unit that failed (non-zero exit, a signal, oom-kill) stays loaded with
//!   its Result until [`SystemdExecutor::finish`] resets it, so the daemon can still read how it
//!   ended. A unit that exited 0 is unloaded at once; it has nothing more to tell.
//! - **Output.** LOG is the supervisor's stdout and stderr; the command's go through a pipe that
//!   the supervisor copies into LOG ([`crate::supervise`]), so a job can't truncate its log by
//!   opening `/dev/stdout`.
//! - **Open files.** A unit's limit on open files is the user manager's default (a soft limit of
//!   1024), not its caller's. LimitNOFILE gives the job the daemon's soft and
//!   hard limits, which it raised at start, as a macOS job inherits them. systemd lowers a value
//!   the user manager can't grant to the most it can, instead of failing the unit.
//! - **Leftovers.** When the supervisor exits, systemd stops the unit: SIGTERM to every process
//!   left in its cgroup, SIGKILL after TimeoutStopSec, the daemon's kill grace
//!   ([`Limits::kill_grace_ms`](super::Limits::kill_grace_ms)). The daemon doesn't wait for that:
//!   once it sees the status file (it looks every half second) the command is over, and it
//!   SIGKILLs what is left of the unit at once.
//! - **One run of a name.** A job id, and so its unit name, can come back to a machine (a job
//!   queued again after its machine was lost), and a cleanup that failed is tried again later
//!   with the earlier run's handle. The handle keeps the unit's InvocationID: `signal` and
//!   `finish` leave a unit that runs another invocation alone.
//! - **A stopped supervisor.** A supervisor stopped by a signal (SIGSTOP from outside the job;
//!   the job's own process group is not the supervisor's) can't read the job's output, so the job
//!   would block once the pipe is full, and can't record how the command ended. `sample` continues
//!   it (SIGCONT): the unit's main process, which systemd names once per unit.
//! - **Containers.** What a job asks another service to run lives in that service's cgroups, not
//!   the unit's: a docker container's processes sit in `system.slice/docker-<id>.scope`. They are
//!   neither counted nor killed.
//! - **Leaving the unit.** The user owns every cgroup under `user@UID.service`, so a process of
//!   the job can also move itself out: create a cgroup there and write its pid into that
//!   cgroup's `cgroup.procs` (checked on systemd 262), or ask the user manager for a unit of its
//!   own (`systemd-run --user`). From then on it is neither counted nor killed. Only a cgroup
//!   namespace or a user namespace around the unit would stop that, and either changes what the
//!   job sees of the machine; a job runs as its user, who could start such processes anyway.

mod cgroup;
mod run;
mod stacks;

use super::{Executor, Launch, Sample};
use crate::types::{Handle, Millis};
use anyhow::{Context, bail};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

pub struct SystemdExecutor;

/// The longest a systemctl call may take (the user manager's own D-Bus timeout is 25s).
const SYSTEMCTL_TIMEOUT: Duration = Duration::from_secs(30);
/// systemd-run waits for the unit to start; allow for a busy user manager.
const START_TIMEOUT: Duration = Duration::from_secs(60);
/// How long `stacks` may take in all.
const STACKS_BUDGET: Duration = Duration::from_secs(30);
/// How long `finish` waits for a core dump systemd-coredump is still writing.
const COREDUMP_WAIT: Duration = Duration::from_secs(5);
/// MemoryPeak this close to MemoryMax counts as reaching it: at a limit's OOM the cgroup is
/// within the failed charge (a few pages) of its limit.
const LIMIT_SLACK_BYTES: u64 = 2 << 20;
/// An env file older than this was left behind: a supervisor reads its file within seconds of
/// the start.
const STALE_ENV_FILE: Duration = Duration::from_secs(600);

impl SystemdExecutor {
    /// Where the environment of the unit `unit` (`NAME.service`) waits for its supervisor.
    pub fn env_file(unit: &str) -> std::path::PathBuf {
        env_dir().join(unit)
    }
}

impl Executor for SystemdExecutor {
    fn start(&self, launch: &Launch) -> anyhow::Result<Handle> {
        let unit = unit_name(&launch.name)?;
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

        // A unit of this name may be left failed by a daemon that never finished it; a running one
        // is not ours to touch.
        let state = show(&unit, &["LoadState", "ActiveState"])?;
        match state.get("ActiveState").map(String::as_str) {
            Some("failed") => {
                let _ = systemctl(&["reset-failed", &unit]);
            }
            Some("inactive") | None => {}
            Some(other) => bail!("unit {unit} already exists and is {other}"),
        }

        let expand_flag = systemd_version().is_some_and(|v| v >= 254);
        let mut exec: Vec<OsString> = vec![env_binary()?.into(), "-i".into(), "--".into()];
        if launch.potluck_bin.as_os_str().as_bytes().contains(&b'=') {
            // env would take it for a variable.
            bail!(
                "the potluck binary's path contains '=': {}",
                launch.potluck_bin.display()
            );
        }
        let env_file = Self::env_file(&unit);
        exec.push(launch.potluck_bin.clone().into());
        exec.push("_supervise".into());
        exec.push("--env-file".into());
        exec.push(env_file.clone().into());
        if launch.limits.log_max_bytes > 0 {
            exec.push("--log-max".into());
            exec.push(launch.limits.log_max_bytes.to_string().into());
        }
        exec.push(launch.status.clone().into());
        exec.push("--".into());
        exec.extend(launch.job.spec.cmd.iter().map(OsString::from));
        if !expand_flag {
            exec = exec.iter().map(|a| escape_dollars(a)).collect();
        }

        let limits = &launch.limits;
        let mut props: Vec<OsString> = vec![
            if limits.mem_max_bytes > 0 {
                format!("MemoryMax={}", limits.mem_max_bytes)
            } else {
                "MemoryMax=infinity".into()
            }
            .into(),
            "MemorySwapMax=0".into(),
            format!("CPUWeight={}", limits.cpu_weight.clamp(1, 10_000)).into(),
            if limits.tasks_max > 0 {
                format!("TasksMax={}", limits.tasks_max)
            } else {
                "TasksMax=infinity".into()
            }
            .into(),
            "OOMPolicy=stop".into(),
            "KillMode=control-group".into(),
            // 0 would turn the timeout off.
            format!("TimeoutStopSec={}ms", limits.kill_grace_ms.max(1)).into(),
            "CollectMode=inactive".into(),
            concat("WorkingDirectory=", &launch.cwd),
            "StandardInput=null".into(),
            concat("StandardOutput=append:", &launch.log),
            "StandardError=inherit".into(),
        ];
        props.extend(open_files());

        let mut cmd = user_command("systemd-run");
        cmd.args(["--user", "--quiet", "--service-type=exec"])
            .arg(format!("--unit={unit}"))
            .arg(format!("--description=potluck job {}", launch.job.id));
        if expand_flag {
            cmd.arg("--expand-environment=no");
        }
        for prop in &props {
            cmd.arg("-p").arg(prop);
        }
        cmd.arg("--").args(&exec);
        sweep_env_files();
        forget_supervisor(&unit);
        write_env(&env_file, &launch.env)?;
        let out = match run::run(&mut cmd, START_TIMEOUT) {
            Ok(out) => out,
            Err(e) => {
                let _ = std::fs::remove_file(&env_file);
                return Err(e).context("could not run systemd-run");
            }
        };
        if !out.success() {
            let result = show(&unit, &["Result"])
                .ok()
                .and_then(|mut p| p.remove("Result"))
                .filter(|r| r != "success");
            // The OOM killer can act before systemd has seen the start through (a job that goes
            // over its limit at once while the manager is busy): the start then counts as failed,
            // but the job ran. It ends like any other run that went over, and `finish` clears it.
            if result.as_deref() == Some("oom-kill") {
                let (_, invocation) = current(&unit);
                return Ok(Handle::Systemd {
                    cgroup: None,
                    unit,
                    invocation,
                });
            }
            // Leave nothing behind: a unit that failed to start stays loaded, and one still
            // starting when systemd-run timed out would run unknown to the daemon.
            let _ = systemctl(&["stop", &unit]);
            let _ = systemctl(&["reset-failed", &unit]);
            let _ = std::fs::remove_file(&env_file);
            let result = result.map(|r| format!(" (result {r})")).unwrap_or_default();
            bail!(
                "systemd-run could not start {unit}: {}{result}",
                out.message()
            );
        }
        let (cgroup, invocation) = current(&unit);
        Ok(Handle::Systemd {
            cgroup,
            unit,
            invocation,
        })
    }

    fn sample(&self, handle: &mut Handle, _mem_max_bytes: u64) -> anyhow::Result<Sample> {
        let Handle::Systemd { unit, cgroup, .. } = handle else {
            bail!("not a systemd unit: {handle:?}");
        };
        if let Some(cg) = cgroup.as_deref()
            && let Some(stats) = read_cgroup(cg)?
        {
            continue_supervisor(unit, &stats.pids);
            return Ok(to_sample(stats));
        }
        // The cgroup is gone (the unit has ended) or was never recorded: ask systemd.
        let props = show(
            unit,
            &[
                "LoadState",
                "ActiveState",
                "Result",
                "ControlGroup",
                "CPUUsageNSec",
                "MemoryPeak",
                "MemoryMax",
                "ManagedOOMKills",
            ],
        )?;
        if let Some(cg) = props.get("ControlGroup").filter(|c| !c.is_empty())
            && cgroup.as_deref() != Some(cg.as_str())
        {
            *cgroup = Some(cg.clone());
            if let Some(stats) = read_cgroup(cg)? {
                continue_supervisor(unit, &stats.pids);
                return Ok(to_sample(stats));
            }
        }
        let oom_kill = props.get("Result").map(String::as_str) == Some("oom-kill");
        let own = reached_own_limit(&props);
        let number = |key: &str| props.get(key).and_then(|v| v.parse::<u64>().ok());
        Ok(Sample {
            pids: Vec::new(),
            cpu_ms: number("CPUUsageNSec").map_or(0, |ns| ns / 1_000_000),
            mem_bytes: 0,
            // systemd 256 and later keep it after the cgroup is gone.
            peak_mem_bytes: number("MemoryPeak").unwrap_or(0),
            over_memory: oom_kill && own,
            oom_killed: oom_kill && !own,
            over_tasks: None,
        })
    }

    fn signal(&self, handle: &Handle, sig: i32) -> anyhow::Result<()> {
        let (unit, cg) = parts(handle)?;
        if replaced(handle) {
            return Ok(()); // its run is over; the unit runs a later one
        }
        let Some(cg) = cg.map(str::to_string).or_else(|| current_cgroup(unit)) else {
            return Ok(()); // not running: nothing to signal
        };
        let dir = cgroup::dir(&cg);
        if !alive(&dir) {
            return Ok(());
        }
        if sig == libc::SIGKILL && cgroup::kill(&dir).is_ok() {
            return Ok(());
        }
        // systemd walks the cgroup and its children, and repeats until no new process appears.
        let whom = if systemd_version().is_some_and(|v| v < 252) {
            "--kill-who=all"
        } else {
            "--kill-whom=all"
        };
        let out = systemctl(&["kill", whom, &format!("--signal={sig}"), unit])?;
        if out.success() || !alive(&dir) {
            return Ok(());
        }
        bail!("could not signal {unit}: {}", out.message())
    }

    fn stacks(&self, handle: &Handle) -> anyhow::Result<String> {
        let (unit, cg) = parts(handle)?;
        let mut stats = match cg.map(str::to_string).or_else(|| current_cgroup(unit)) {
            Some(cg) => read_cgroup(&cg)?,
            None => None,
        };
        // The supervisor only waits for its command and copies its output: left out, as on
        // macOS, so no debugger stops it or spends the time on it.
        if let (Some(stats), Some(pid)) = (stats.as_mut(), supervisor(unit)) {
            stats.pids.retain(|&p| p != pid);
        }
        Ok(match stats {
            Some(stats) if !stats.pids.is_empty() => stacks::collect(unit, &stats, STACKS_BUDGET),
            _ => format!("stacks of {unit}: no processes left\n"),
        })
    }

    fn finish(&self, handle: &Handle, crash_dir: &Path, since: Millis) -> anyhow::Result<()> {
        let (unit, cg) = parts(handle)?;
        if replaced(handle) {
            // Its run is over, and the unit, its env file and its core dumps are a later run's.
            return Ok(());
        }
        forget_supervisor(unit);
        // Still there if the supervisor never got to read it.
        let _ = std::fs::remove_file(Self::env_file(unit));
        let dir = cg
            .map(str::to_string)
            .or_else(|| current_cgroup(unit))
            .map(|cg| cgroup::dir(&cg));
        if let Some(dir) = &dir
            && alive(dir)
        {
            let _ = self.signal(handle, libc::SIGKILL);
        }
        let state = show(unit, &["LoadState", "ActiveState"])?;
        if matches!(
            state.get("ActiveState").map(String::as_str),
            Some("active" | "activating" | "deactivating" | "reloading" | "refreshing")
        ) {
            // Waits until the cgroup is empty, or systemd gives up on it.
            let _ = systemctl(&["stop", unit]);
        }
        let _ = save_coredumps(unit, crash_dir, since);
        let _ = systemctl(&["reset-failed", unit]);

        let state = show(unit, &["LoadState", "ActiveState"])?;
        if state.get("LoadState").map(String::as_str) == Some("not-found") {
            return Ok(());
        }
        let left = dir.and_then(|d| cgroup::pids(&d).ok()).unwrap_or_default();
        bail!(
            "unit {unit} is still {} with processes {:?} left",
            state.get("ActiveState").map_or("loaded", String::as_str),
            left
        )
    }

    /// The unit a cut-short start of `name` left: any unit systemd still knows by that name, or
    /// one it unloaded after it exited 0 (its supervisor wrote `status` since the claim).
    fn find(&self, name: &str, status: &Path, since: Millis) -> anyhow::Result<Option<Handle>> {
        let unit = unit_name(name)?;
        let props = show(&unit, &["LoadState", "ControlGroup", "InvocationID"])?;
        if props.get("LoadState").map(String::as_str) == Some("not-found")
            && !written_since(status, since)
        {
            // Never started: an env file written for it goes too.
            let _ = std::fs::remove_file(Self::env_file(&unit));
            return Ok(None);
        }
        let prop = |key: &str| props.get(key).filter(|v| !v.is_empty()).cloned();
        Ok(Some(Handle::Systemd {
            cgroup: prop("ControlGroup"),
            invocation: prop("InvocationID"),
            unit,
        }))
    }
}

/// Each unit's supervisor (its main process; `None` when systemd named none), learned once per
/// unit: asking systemd costs a process, reading /proc doesn't.
fn supervisors() -> std::sync::MutexGuard<'static, BTreeMap<String, Option<u32>>> {
    static PIDS: std::sync::Mutex<BTreeMap<String, Option<u32>>> =
        std::sync::Mutex::new(BTreeMap::new());
    PIDS.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn forget_supervisor(unit: &str) {
    supervisors().remove(unit);
}

/// Continues the unit's supervisor if a signal stopped it (see the module docs). `pids` are the
/// unit's live processes: a supervisor no longer among them has exited.
fn continue_supervisor(unit: &str, pids: &[u32]) {
    if let Some(pid) = supervisor(unit).filter(|p| pids.contains(p))
        && process_state(pid) == Some('T')
    {
        // SAFETY: kill has no memory preconditions; pid is the unit's main process, alive in its
        // cgroup a moment ago.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGCONT) };
    }
}

/// The unit's supervisor, its main process ([`supervisors`]).
fn supervisor(unit: &str) -> Option<u32> {
    if let Some(&pid) = supervisors().get(unit) {
        return pid;
    }
    let pid = show(unit, &["MainPID"])
        .ok()
        .and_then(|p| p.get("MainPID")?.parse::<u32>().ok())
        .filter(|&p| p > 1);
    supervisors().insert(unit.to_string(), pid);
    pid
}

/// A process's state letter from /proc/PID/stat (`T`: stopped by a signal).
fn process_state(pid: u32) -> Option<char> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat[stat.rfind(')')? + 1..].trim_start().chars().next()
}

fn parts(handle: &Handle) -> anyhow::Result<(&str, Option<&str>)> {
    match handle {
        Handle::Systemd { unit, cgroup, .. } => Ok((unit, cgroup.as_deref())),
        other => bail!("not a systemd unit: {other:?}"),
    }
}

fn to_sample(stats: cgroup::Stats) -> Sample {
    let oom_kill = stats.oom_kills > 0;
    let own = stats.limit_ooms > 0;
    Sample {
        pids: stats.pids,
        cpu_ms: stats.cpu_usec / 1000,
        mem_bytes: stats.mem_bytes,
        peak_mem_bytes: stats.peak_bytes,
        over_memory: oom_kill && own,
        oom_killed: oom_kill && !own,
        over_tasks: None,
    }
}

/// `LimitNOFILE=SOFT:HARD` with this process's limits on open files, for the job's unit; `None`
/// if they can't be read (the unit then gets the user manager's).
fn open_files() -> Option<OsString> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit fills `lim`.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return None;
    }
    let value = |v: libc::rlim_t| {
        if v == libc::RLIM_INFINITY {
            "infinity".to_string()
        } else {
            v.to_string()
        }
    };
    Some(
        format!(
            "LimitNOFILE={}:{}",
            value(lim.rlim_cur),
            value(lim.rlim_max)
        )
        .into(),
    )
}

/// Whether a unit whose cgroup is gone reached its own MemoryMax, from what systemd keeps: a
/// MemoryPeak at its MemoryMax, and no kill by systemd-oomd. Without a MemoryPeak (systemd
/// before 256) it can't tell and says yes; with MemoryMax=infinity there was no limit to reach.
fn reached_own_limit(props: &BTreeMap<String, String>) -> bool {
    let number = |key: &str| props.get(key).and_then(|v| v.parse::<u64>().ok());
    if number("ManagedOOMKills").is_some_and(|n| n > 0) {
        return false;
    }
    if props.get("MemoryMax").map(String::as_str) == Some("infinity") {
        return false;
    }
    match (number("MemoryPeak"), number("MemoryMax")) {
        (Some(peak), Some(max)) => peak.saturating_add(LIMIT_SLACK_BYTES) >= max,
        _ => true,
    }
}

fn read_cgroup(cg: &str) -> anyhow::Result<Option<cgroup::Stats>> {
    let dir = cgroup::dir(cg);
    cgroup::read(&dir).with_context(|| format!("could not read cgroup {}", dir.display()))
}

/// Whether any process is left in the cgroup at `dir` (an unreadable one counts as alive).
fn alive(dir: &Path) -> bool {
    match cgroup::pids(dir) {
        Ok(pids) => !pids.is_empty(),
        Err(e) => e.kind() != std::io::ErrorKind::NotFound,
    }
}

/// The unit's cgroup while it has one (it is running or stopping).
fn current_cgroup(unit: &str) -> Option<String> {
    show(unit, &["ControlGroup"])
        .ok()?
        .remove("ControlGroup")
        .filter(|c| !c.is_empty())
}

/// The unit's cgroup (while it has one) and InvocationID (while systemd knows the unit).
fn current(unit: &str) -> (Option<String>, Option<String>) {
    let Ok(mut props) = show(unit, &["ControlGroup", "InvocationID"]) else {
        return (None, None);
    };
    let mut take = |key: &str| props.remove(key).filter(|v| !v.is_empty());
    (take("ControlGroup"), take("InvocationID"))
}

/// Whether the handle's unit now runs another invocation than the handle's: a later run of a job
/// back on this machine, under the same unit name. A handle without an InvocationID (from an
/// older potluck) can't tell and says no.
fn replaced(handle: &Handle) -> bool {
    let Handle::Systemd {
        unit,
        invocation: Some(ours),
        ..
    } = handle
    else {
        return false;
    };
    current(unit).1.is_some_and(|now| now != *ours)
}

/// `NAME.service`, checked against systemd's rules for unit names.
fn unit_name(name: &str) -> anyhow::Result<String> {
    let stem = name.strip_suffix(".service").unwrap_or(name);
    let valid = !stem.is_empty()
        && stem.len() + ".service".len() <= 255
        && stem
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b));
    if !valid {
        bail!("{name:?} can't be a unit name: use letters, digits, '-', '_', '.' and ':'");
    }
    Ok(format!("{stem}.service"))
}

fn concat(prefix: &str, path: &Path) -> OsString {
    let mut s = OsString::from(prefix);
    s.push(path.as_os_str());
    s
}

/// For systemd before 254, which expands `$VAR` and `${VAR}` in a command line: `$$` is a literal
/// `$`.
fn escape_dollars(arg: &OsStr) -> OsString {
    let mut out = Vec::with_capacity(arg.len());
    for &b in arg.as_bytes() {
        out.push(b);
        if b == b'$' {
            out.push(b'$');
        }
    }
    OsString::from_vec(out)
}

/// Whether the file at `path` was last written at or after `since` (ms since the Unix epoch).
fn written_since(path: &Path, since: Millis) -> bool {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .is_some_and(|t| t.as_millis() as Millis >= since)
}

/// `$XDG_RUNTIME_DIR/potluck-env`, the directory of the env files.
fn env_dir() -> std::path::PathBuf {
    runtime_dir().join("potluck-env")
}

/// This user's runtime directory: XDG_RUNTIME_DIR, else /run/user/UID (where the user manager
/// keeps its bus).
fn runtime_dir() -> std::path::PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(dir) if !dir.is_empty() => dir.into(),
        _ => {
            // SAFETY: getuid has no preconditions and cannot fail.
            let uid = unsafe { libc::getuid() };
            format!("/run/user/{uid}").into()
        }
    }
}

/// Writes the unit's env file, in a directory only this user can enter.
fn write_env(path: &Path, env: &BTreeMap<String, String>) -> anyhow::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    let dir = env_dir();
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("making {} private", dir.display()))?;
    crate::supervise::write_env_file(path, env)
        .with_context(|| format!("writing the job's environment to {}", path.display()))
}

/// Removes env files left behind long ago (a daemon that died between writing one and starting
/// its unit).
fn sweep_env_files() {
    let Ok(entries) = std::fs::read_dir(env_dir()) else {
        return;
    };
    for entry in entries.flatten() {
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .is_ok_and(|t| t.elapsed().is_ok_and(|age| age > STALE_ENV_FILE));
        if old {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

fn env_binary() -> anyhow::Result<&'static str> {
    ["/usr/bin/env", "/bin/env"]
        .into_iter()
        .find(|p| Path::new(p).is_file())
        .ok_or_else(|| anyhow::anyhow!("no env binary in /usr/bin or /bin"))
}

/// The systemd version, from `systemd-run --version` (`systemd 262 (262.1-1-arch)`).
fn systemd_version() -> Option<u32> {
    static VERSION: OnceLock<Option<u32>> = OnceLock::new();
    *VERSION.get_or_init(|| {
        let out = run::run(
            user_command("systemd-run").arg("--version"),
            SYSTEMCTL_TIMEOUT,
        )
        .ok()?;
        parse_version(&out.stdout)
    })
}

fn parse_version(text: &str) -> Option<u32> {
    text.lines()
        .next()?
        .strip_prefix("systemd ")?
        .split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

/// A command talking to this user's systemd: it finds the user bus through XDG_RUNTIME_DIR, which
/// a daemon started outside a login session may lack.
fn user_command(program: &str) -> Command {
    let mut cmd = crate::service::user_command(program);
    cmd.env_remove("DEBUGINFOD_URLS");
    cmd
}

/// Whether this user's systemd manager answers, as the daemon reaches it (every job starts
/// through it); why not, if it doesn't.
pub(crate) fn user_manager_problem() -> Option<String> {
    match systemctl(&["show", "--property=Version", "--value"]) {
        Ok(out) if out.success() && !out.stdout.trim().is_empty() => None,
        Ok(out) => Some(out.message()),
        Err(e) => Some(format!("{e:#}")),
    }
}

fn systemctl(args: &[&str]) -> anyhow::Result<run::Output> {
    let mut cmd = user_command("systemctl");
    cmd.args(["--user", "--no-pager", "--no-ask-password"])
        .args(args);
    run::run(&mut cmd, SYSTEMCTL_TIMEOUT).context("could not run systemctl")
}

/// Unit properties; a unit systemd doesn't know has LoadState=not-found.
fn show(unit: &str, props: &[&str]) -> anyhow::Result<BTreeMap<String, String>> {
    let out = systemctl(&["show", &format!("--property={}", props.join(",")), unit])?;
    if !out.success() {
        bail!("systemctl show {unit}: {}", out.message());
    }
    Ok(parse_show(&out.stdout))
}

fn parse_show(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Writes `coredumpctl info` for the unit's core dumps since `since` (ms since the Unix epoch;
/// an earlier run of a unit by that name crashed before it) to `crash_dir/coredumps.txt`. Best
/// effort: systemd-coredump records a dump shortly after the process died, so one still being
/// written is given a few seconds. `crash_dir` is made if need be, but not its parent, the job's
/// directory: once that is gone (the job was deleted before the late collection), nothing is.
fn save_coredumps(unit: &str, crash_dir: &Path, since: Millis) -> anyhow::Result<bool> {
    let matches = coredump_matches(unit);
    // Whole seconds, rounded down: a dump in the claim's second still counts.
    let since = format!("--since=@{}", since / 1000);
    let deadline = Instant::now() + COREDUMP_WAIT;
    loop {
        let out = run::run(
            user_command("coredumpctl")
                .args(["--no-pager", "--no-legend", "list", &since])
                .args(&matches),
            SYSTEMCTL_TIMEOUT,
        )?;
        if out.success() && !out.stdout.trim().is_empty() {
            break;
        }
        if Instant::now() >= deadline || !coredump_in_flight() {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let out = run::run(
        user_command("coredumpctl")
            .args(["--no-pager", "info", &since])
            .args(&matches),
        SYSTEMCTL_TIMEOUT,
    )?;
    match std::fs::create_dir(crash_dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => return Err(e.into()),
        _ => {}
    }
    let mut text = out.stdout;
    if !out.stderr.trim().is_empty() {
        text.push_str(&out.stderr);
    }
    std::fs::write(crash_dir.join("coredumps.txt"), text)?;
    Ok(true)
}

/// The journal matches for the core dumps of this user's processes in `unit`. Another user's
/// manager can run a unit of the same name (a second potluck on the machine), and a user in
/// `wheel` or `adm` reads every user's journal; any user can also send an entry with a core
/// dump's fields, naming any unit and uid. `_UID` is the sender's uid, set by journald:
/// systemd-coredump sends as the crashed process's user, or as its own user when that is a
/// system user. Matches on one field are alternatives; on different fields, all must hold.
fn coredump_matches(unit: &str) -> Vec<String> {
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let mut matches = vec![
        format!("COREDUMP_USER_UNIT={unit}"),
        format!("COREDUMP_UID={uid}"),
        format!("_UID={uid}"),
    ];
    if let Some(coredump) = uid_of("systemd-coredump").filter(|&u| u != uid) {
        matches.push(format!("_UID={coredump}"));
    }
    matches
}

/// The uid of the user `name`, from the password database.
fn uid_of(name: &str) -> Option<libc::uid_t> {
    let name = std::ffi::CString::new(name).ok()?;
    let mut buf = vec![0u8; 16 * 1024];
    // SAFETY: getpwnam_r writes into `pwd` and `buf`, which outlive the call; `result` is set to
    // null or to `pwd`.
    unsafe {
        let mut pwd: libc::passwd = std::mem::zeroed();
        let mut result: *mut libc::passwd = std::ptr::null_mut();
        let r = libc::getpwnam_r(
            name.as_ptr(),
            &mut pwd,
            buf.as_mut_ptr().cast(),
            buf.len(),
            &mut result,
        );
        (r == 0 && !result.is_null()).then_some(pwd.pw_uid)
    }
}

/// Whether systemd-coredump is processing a dump right now (any dump on the machine).
fn coredump_in_flight() -> bool {
    let dir = Path::new(cgroup::ROOT).join("system.slice/system-systemd\\x2dcoredump.slice");
    cgroup::pids(&dir).is_ok_and(|p| !p.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_dumps_are_matched_by_unit_and_this_users_uid() {
        // SAFETY: getuid has no preconditions.
        let uid = unsafe { libc::getuid() };
        let m = coredump_matches("potluck-a-1.service");
        assert_eq!(
            m[..3],
            [
                "COREDUMP_USER_UNIT=potluck-a-1.service".to_string(),
                format!("COREDUMP_UID={uid}"),
                format!("_UID={uid}"),
            ]
        );
        assert!(m[3..].iter().all(|x| x.starts_with("_UID=")), "{m:?}");
        assert_eq!(uid_of("root"), Some(0));
        assert_eq!(uid_of("potluck-no-such-user"), None);
    }

    #[test]
    fn unit_names() {
        assert_eq!(
            unit_name("potluck-studio-42").unwrap(),
            "potluck-studio-42.service"
        );
        assert_eq!(
            unit_name("potluck-a.b:c_d.service").unwrap(),
            "potluck-a.b:c_d.service"
        );
        for bad in [
            "",
            ".service",
            "a b",
            "a/b",
            "a@b",
            "ünit",
            &"x".repeat(250),
        ] {
            assert!(unit_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn dollars_are_doubled() {
        assert_eq!(
            escape_dollars(OsStr::new("$HOME ${X} $$ a")),
            "$$HOME $${X} $$$$ a"
        );
    }

    #[test]
    fn versions() {
        assert_eq!(
            parse_version("systemd 262 (262.1-1-arch)\n+PAM +AUDIT"),
            Some(262)
        );
        assert_eq!(parse_version("systemd 249 (249.11-0ubuntu3)"), Some(249));
        assert_eq!(parse_version("nonsense"), None);
    }

    #[test]
    fn only_the_units_own_limit_is_over_memory() {
        let stats = |oom_kills, limit_ooms| cgroup::Stats {
            oom_kills,
            limit_ooms,
            ..Default::default()
        };
        let s = to_sample(stats(1, 1));
        assert!(s.over_memory && !s.oom_killed);
        // Killed when something above it ran short.
        let s = to_sample(stats(1, 0));
        assert!(!s.over_memory && s.oom_killed);
        // At its limit, but nothing was killed (yet).
        let s = to_sample(stats(0, 1));
        assert!(!s.over_memory && !s.oom_killed);

        let props = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let mib = |n: u64| (n << 20).to_string();
        // As systemd 262 showed them after a unit went over its 64 MiB.
        assert!(reached_own_limit(&props(&[
            ("MemoryPeak", &mib(64)),
            ("MemoryMax", &mib(64)),
            ("ManagedOOMKills", "0"),
        ])));
        assert!(reached_own_limit(&props(&[
            (
                "MemoryPeak",
                &(mib(64).parse::<u64>().unwrap() - 8192).to_string()
            ),
            ("MemoryMax", &mib(64)),
        ])));
        // A slice above it ran short at 100 MiB; its own limit was 1 GiB.
        assert!(!reached_own_limit(&props(&[
            ("MemoryPeak", "105590784"),
            ("MemoryMax", &mib(1024)),
            ("ManagedOOMKills", "0"),
        ])));
        // systemd-oomd killed it.
        assert!(!reached_own_limit(&props(&[
            ("MemoryPeak", &mib(64)),
            ("MemoryMax", &mib(64)),
            ("ManagedOOMKills", "1"),
        ])));
        assert!(!reached_own_limit(&props(&[
            ("MemoryPeak", &mib(64)),
            ("MemoryMax", "infinity"),
        ])));
        // An older systemd keeps no peak: it can't tell.
        assert!(reached_own_limit(&props(&[
            ("MemoryPeak", "[not set]"),
            ("MemoryMax", &mib(64)),
        ])));
        assert!(reached_own_limit(&props(&[("MemoryMax", &mib(64))])));
    }

    #[test]
    fn show_output() {
        let m = parse_show("LoadState=loaded\nResult=oom-kill\nControlGroup=\n");
        assert_eq!(m["LoadState"], "loaded");
        assert_eq!(m["Result"], "oom-kill");
        assert_eq!(m["ControlGroup"], "");
    }
}
