//! The loop: watching invocations, ending them, starting new ones, and the periodic upkeep. Also
//! rebuilding the state after a restart.

use super::clock::Now;
use super::files;
use super::state::{Ending, Machine, Phase, Run, State, run_key};
use super::{Logger, Options, Shared, panic_text};
use crate::cli::fmt_mib;
use crate::config::{NodeConfig, Paths};
use crate::exec::{Executor, Launch, Limits, Sample};
use crate::store::Store;
use crate::types::{
    EXIT_CANCELLED, EXIT_LOST, EXIT_OVER_MEMORY, EXIT_TIMEOUT, ExitStatus, Facts, Grant, Handle,
    JobId, JobRecord, JobState, Lease, MINUTE_MS, Millis, RepoRef, WorkspaceRecord, WorkspaceState,
    fmt_ms, gpu_matches,
};
use crate::workspace::Action;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Sampling failures in a row before the daemon gives up on an invocation as lost.
const MAX_SAMPLE_ERRORS: u32 = 20;
/// A job's task limit: room for big parallel builds and test runs, not for a fork bomb. Linux:
/// the unit's TasksMax (threads count). macOS: processes, at most half of kern.maxprocperuid.
const TASKS_MAX: u32 = 16_384;
/// How long after a crash its crash report may still appear.
const CRASH_REPORT_DELAY_MS: u64 = 5_000;
/// A running job's record is written for its usage at most this often (state changes at once).
const USAGE_SAVE_MS: u64 = 5_000;
/// Finished jobs stay in memory this long; after that they are read from the store.
const KEEP_IN_MEMORY_MS: u64 = 10 * MINUTE_MS;
/// How often retention pruning and workspace size refreshes run.
const UPKEEP_MS: u64 = 10 * MINUTE_MS;
/// How often records whose write to the store failed are written again.
const SAVE_AGAIN_MS: u64 = 5_000;
/// Bookkeeping key prefix of a cleanup ([`Executor::finish`](crate::exec::Executor::finish))
/// that failed: `cleanup/<job>`. It is tried again at each upkeep.
const CLEANUP_PREFIX: &str = "cleanup/";
/// How many times a failed cleanup is tried again before it is given up.
const CLEANUP_TRIES: u32 = 6;
/// How often the executor is asked again whether a start a restart cut short started its
/// invocation, while it can't tell ([`Run::find_again`]).
const FIND_AGAIN_MS: u64 = 2_000;

/// A cleanup to try again.
#[derive(serde::Serialize, serde::Deserialize)]
struct Cleanup {
    handle: Handle,
    tries: u32,
    /// When the job was claimed ([`Executor::finish`]'s `since`).
    #[serde(default)]
    since: Millis,
}

/// Admission capacity: the config's `ram_gib`, else total RAM minus max(4 GiB, 10%).
pub(crate) fn ram_capacity(config: &NodeConfig, facts: &Facts) -> u64 {
    match config.ram_gib {
        Some(g) if g > 0.0 => (g * 1024.0) as u64,
        _ => facts
            .ram_mib
            .saturating_sub((4 * 1024).max(facts.ram_mib / 10)),
    }
}

/// The name an invocation of `job` is launched under (the Linux unit name).
fn launch_name(job: &str) -> String {
    format!("potluck-{job}")
}

/// The state from the store when the daemon starts. `executor` finds the invocations of jobs
/// whose start was cut short (the daemon stopped inside `Executor::start`, before it saved the
/// handle), so they are adopted, not queued again and run twice. `unwritten`: what a daemon
/// before this one in the process held and couldn't write ([`State::unwritten`]), read as if
/// stored, and written from here on.
#[allow(clippy::too_many_arguments)]
pub(crate) fn restore(
    config: &NodeConfig,
    store: Store,
    facts: Facts,
    log: Logger,
    executor: &dyn Executor,
    clock: Arc<dyn super::Clock>,
    paths: &Paths,
    mut unwritten: super::state::Unwritten,
) -> anyhow::Result<State> {
    let at = Now::read(&*clock);
    let boot = clock.boot_id();
    let now = at.wall;
    let node = if facts.node.is_empty() {
        config.name.clone().unwrap_or_else(|| "local".into())
    } else {
        facts.node.clone()
    };
    let mut machines = BTreeMap::new();
    machines.insert(
        node.clone(),
        Machine {
            ram_capacity_mib: ram_capacity(config, &facts),
            facts,
            last_seen: now,
        },
    );
    let mut workspaces = BTreeMap::new();
    // Removed ones too: a removal that failed or that a restart cut short is done again. What
    // the daemon before this one in the process couldn't write goes over what is stored.
    let unwritten_ws: Vec<String> = unwritten.workspaces.iter().map(|w| w.id.clone()).collect();
    for ws in store
        .workspaces()?
        .into_iter()
        .chain(store.removed_workspaces()?)
        .chain(std::mem::take(&mut unwritten.workspaces))
    {
        workspaces.insert(ws.id.clone(), ws);
    }
    let unwritten_jobs: Vec<String> = unwritten.jobs.iter().map(|j| j.id.clone()).collect();
    let mut active = store.active_jobs()?;
    active.retain(|j| !unwritten_jobs.contains(&j.id));
    let mut jobs = BTreeMap::new();
    for job in std::mem::take(&mut unwritten.jobs) {
        if job.state.is_done() {
            jobs.insert(job.id.clone(), job);
        } else {
            active.push(job);
        }
    }
    let mut runs = BTreeMap::new();
    // Written once the state is made, as any write: one that fails (a full disk) is made again
    // later, and doesn't keep the daemon from starting.
    let mut changed: Vec<JobId> = Vec::new();
    let mut runs_gone: Vec<JobId> = Vec::new();
    for mut job in active {
        // In a cluster, this machine's jobs running elsewhere wait for their runners' reports:
        // only the invocations here are restored. Another machine's job that was starting here
        // and can't start again goes back to its owner (the cluster does that).
        let here = job.node.as_deref().is_none_or(|n| n == node);
        let own = super::cluster::peer::owner_of(&job.id) == node;
        // Read once: one that can't be read is said once.
        let stored = match job.state {
            JobState::Starting | JobState::Running if here => match unwritten.runs.get(&job.id) {
                Some(run) => run.clone(),
                None => stored_run(&store, &log, &job.id)?,
            },
            _ => None,
        };
        // The executor was asked to start it and can't tell whether that started it: it is asked
        // again before anything starts it again in its workspace ([`find_again`]).
        let mut find_again = false;
        if job.state == JobState::Starting && here && job.lease.is_some() {
            let since = run_since(&job, stored.as_ref(), at, &boot);
            let status = paths.job_dir(&job.id).join(files::STATUS);
            match executor.find(&launch_name(&job.id), &status, since) {
                Ok(Some(handle)) => {
                    // Adopted below as running; a cancel asked meanwhile still applies.
                    log.log(format!(
                        "{}: its start was cut short by a restart, but it runs; adopted",
                        job.id
                    ));
                    job.state = JobState::Running;
                    job.handle = Some(handle);
                    // It started just after its run record says the start began, when that is
                    // on this boot's clock; its run limit counts from there.
                    let began = stored
                        .as_ref()
                        .filter(|r| same_boot(r, &boot))
                        .and_then(|r| r.starting_at)
                        .map_or(now, |t| at.wall_of(t));
                    job.started_at.get_or_insert(began);
                    if let Some(l) = job.lease.as_mut() {
                        l.deadline = began.saturating_add(job.spec.timeout_ms);
                    }
                    changed.push(job.id.clone());
                }
                Ok(None) => {}
                Err(e) => {
                    let ws_live = job
                        .workspace
                        .as_ref()
                        .and_then(|w| workspaces.get(w))
                        .is_some_and(|w| w.state != WorkspaceState::Removed);
                    find_again =
                        ws_live && stored.as_ref().is_some_and(|r| r.starting_at.is_some());
                    log.log(format!(
                        "{}: could not tell whether its start had started it: {e:#}{}",
                        job.id,
                        if find_again {
                            "; asking again before it starts again"
                        } else {
                            ""
                        }
                    ));
                }
            }
        }
        match job.state {
            JobState::Starting if here => {
                if let Some(mut run) = stored
                    .clone()
                    .filter(|r| r.ending.is_some() || (r.cancel && !find_again))
                {
                    // Cancelled while its workspace was being seeded, or its start had failed:
                    // it ends so ([`adopt`] finishes it), never started again. One cancelled
                    // while the executor was starting it, which can't tell whether it did, is
                    // asked again first, below: it may run.
                    if run.ending.is_none() {
                        log.log(format!("{}: was cancelled while starting", job.id));
                        run.ending = Some(Ending {
                            state: JobState::Cancelled,
                            exit_code: Some(EXIT_CANCELLED),
                            signal: None,
                            message: Some("cancelled before it started".into()),
                            at: now,
                        });
                    }
                    run.phase = Phase::Finishing;
                    runs.insert(job.id.clone(), run);
                    jobs.insert(job.id.clone(), job);
                    continue;
                }
                let live = |w: &String| {
                    workspaces
                        .get(w)
                        .is_some_and(|w: &WorkspaceRecord| w.state != WorkspaceState::Removed)
                };
                if let Some(run) = stored
                    && job.workspace.as_ref().is_some_and(live)
                    && job.lease.is_some()
                {
                    // Its workspace was being seeded or verified: it starts again there, with
                    // the lease it holds ([`adopt`]), so what was dispatched into that workspace
                    // meanwhile still runs in it, after this job. A workspace seeded for it is
                    // seeded afresh.
                    let run = if find_again {
                        // As stored (when its start began, a cancel or a release asked): it may
                        // run, and is adopted with it.
                        Run {
                            phase: Phase::Starting,
                            resumed: true,
                            find_again,
                            ..run
                        }
                    } else {
                        log.log(format!("{}: was starting; starting again", job.id));
                        Run {
                            phase: Phase::Starting,
                            new_workspace: run.new_workspace,
                            resumed: true,
                            ..Default::default()
                        }
                    };
                    runs.insert(job.id.clone(), run);
                    jobs.insert(job.id.clone(), job);
                    continue;
                }
                // No record of how far its start got, or its workspace is gone: start over from
                // the queue (another machine's: its owner's). A workspace seeded for it is removed
                // (it is left in the seeding state).
                if !own {
                    jobs.insert(job.id.clone(), job);
                    continue;
                }
                log.log(format!("{}: was starting; queued again", job.id));
                job.state = JobState::Queued;
                job.node = None;
                job.workspace = None;
                job.lease = None;
                job.handle = None;
                job.started_at = None;
                changed.push(job.id.clone());
                runs_gone.push(job.id.clone());
            }
            JobState::Running if here => {
                let mut run = stored.unwrap_or_default();
                run.adopted = true;
                run.phase = Phase::Running;
                carry_over(&mut run, &job, at, &boot);
                // Progress is measured from the first samples after the restart (the time down
                // isn't a stall). A job already stalled stays so until it makes progress: it
                // isn't sampled into its log again.
                if !job.stalled {
                    job.usage.cpu_progress_at = now;
                    job.usage.output_at = now;
                    run.active_at = Some(at.awake);
                }
                if job.handle.is_none() || job.lease.is_none() {
                    run.ending.get_or_insert(Ending {
                        state: JobState::Lost,
                        exit_code: Some(EXIT_LOST),
                        signal: None,
                        message: Some("no record of its processes".into()),
                        at: now,
                    });
                    run.phase = Phase::Finishing;
                }
                runs.insert(job.id.clone(), run);
            }
            _ => {}
        }
        jobs.insert(job.id.clone(), job);
    }
    // Run records whose job isn't starting or running here any more: left by a daemon killed
    // between saving a job's end and deleting its run record.
    for (key, _) in store.kv_prefixed(super::state::RUN_PREFIX)? {
        let id = &key[super::state::RUN_PREFIX.len()..];
        if !runs.contains_key(id) {
            runs_gone.push(id.to_string());
        }
    }
    let mut st = State {
        node,
        machines,
        jobs,
        runs,
        workspaces,
        reservation: None,
        pending_keys: BTreeSet::new(),
        removing: BTreeSet::new(),
        extra_pending: BTreeMap::new(),
        unsaved: BTreeSet::new(),
        unsaved_kv: BTreeMap::new(),
        store,
        log,
        clock,
    };
    // Workspaces left running by an invocation that is gone are held or idle again.
    let ids: Vec<String> = st.workspaces.keys().cloned().collect();
    for id in ids {
        let busy = st.ws_busy(&id);
        let ws = st.workspaces.get_mut(&id).expect("listed");
        if ws.state == WorkspaceState::Running && !busy {
            ws.state = if ws.held_until > now {
                WorkspaceState::Held
            } else {
                WorkspaceState::Idle
            };
            st.save_ws(&id);
        }
    }
    for id in changed {
        st.save_job(&id);
    }
    runs_gone.dedup();
    for id in runs_gone {
        st.save_run(&id);
    }
    st.take_unwritten(unwritten_jobs, unwritten_ws, unwritten);
    st.refresh_pending();
    Ok(st)
}

/// Job `id`'s run record, None when there is none or it can't be read: then the log says so, and
/// what that loses (a newer potluck may have written it).
fn stored_run(store: &Store, log: &Logger, id: &str) -> anyhow::Result<Option<Run>> {
    let Some(json) = store.kv(&run_key(id))? else {
        return Ok(None);
    };
    match serde_json::from_str(&json) {
        Ok(run) => Ok(Some(run)),
        Err(e) => {
            log.log(format!(
                "{id}: its run record can't be read ({e}); going on without it: a cancel or an \
                 ending under way when the daemon stopped is lost"
            ));
            Ok(None)
        }
    }
}

/// Since when (wall clock, as it reads at `at`) files and crashes of this run of `job` can date,
/// for [`Executor::find`] and [`Executor::finish`]: anything older is an earlier run's. That is
/// when the job was claimed, unless the wall clock was set back after the run's start began: then
/// when the boot clock places that start on the wall clock now.
fn run_since(job: &JobRecord, run: Option<&Run>, at: Now, boot: &str) -> Millis {
    let claimed = job.lease.as_ref().map_or(0, |l| l.granted_at);
    run.filter(|r| same_boot(r, boot))
        .and_then(|r| r.starting_at)
        .map_or(claimed, |t| claimed.min(at.wall_of(t)))
}

/// Whether `run`'s boot-clock readings are on this boot's clock (`boot`).
fn same_boot(run: &Run, boot: &str) -> bool {
    !boot.is_empty() && run.boot.as_deref() == Some(boot)
}

/// Makes a restored run's boot-clock readings this daemon's: kept from a record of the same boot;
/// carried over by their distance from now on the wall clock from a record with wall times
/// (written before the daemon kept boot-clock readings); counted from now from another boot's
/// record (its processes went with that boot). Without a deadline of its own the run limit
/// counts from the start, or ends when the lease says.
fn carry_over(run: &mut Run, job: &JobRecord, now: Now, boot: &str) {
    if !same_boot(run, boot) {
        let wall = run.boot.is_none();
        let carry = |t: u64| if wall { now.boot_of(t) } else { now.boot };
        run.term_at = run.term_at.map(carry);
        run.kill_at = run.kill_at.map(carry);
        run.starting_at = None;
        run.deadline = None;
        run.boot = Some(boot.to_string());
    }
    if run.deadline.is_none() {
        run.deadline = match (run.starting_at, &job.lease) {
            (Some(t), _) => Some(t.saturating_add(job.spec.timeout_ms)),
            (None, Some(l)) => Some(now.boot_of(l.deadline)),
            (None, None) => None,
        };
    }
    run.carry_awake(now);
}

/// After [`restore`]: starts again the jobs whose start a restart cut short, removes workspaces
/// whose seeding was cut short and that no job starts in again, and finishes invocations that
/// can't be adopted.
pub(crate) fn adopt(sh: &Arc<Shared>) {
    let mut removals = Vec::new();
    let mut finishing = Vec::new();
    let mut starting = Vec::new();
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        for (id, run) in &st.runs {
            match run.phase {
                Phase::Finishing => finishing.push(id.clone()),
                Phase::Starting => starting.push(id.clone()),
                Phase::Running => {}
            }
        }
        let seeding: Vec<String> = st
            .workspaces
            .values()
            .filter(|w| w.state == WorkspaceState::Seeding)
            .map(|w| w.id.clone())
            .collect();
        for id in seeding {
            if !st.ws_busy(&id)
                && let Some(ws) = st.workspaces.get_mut(&id)
            {
                ws.state = WorkspaceState::Removed;
                removals.push(ws.clone());
                st.save_ws(&id);
            }
        }
    }
    for id in finishing {
        sh.spawn_worker(&format!("finish-{id}"), move |sh| finish(sh, &id));
    }
    for id in starting {
        sh.spawn_worker(&format!("start-{id}"), move |sh| start_job(sh, &id));
    }
    for rec in removals {
        sh.spawn_worker(&format!("remove-{}", rec.id), move |sh| {
            let _ = remove_workspace(sh, &rec);
        });
    }
    sh.kick();
}

/// The loop: every tick, and whenever kicked.
pub(crate) fn ticker(sh: Arc<Shared>) {
    let mut up = Upkeep::default();
    while !sh.stopping() {
        if let Err(p) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tick(&sh, &mut up)))
        {
            sh.log.log(format!("loop: panicked: {}", panic_text(&p)));
        }
        if sh.stopping() {
            break;
        }
        sh.wait_kick(sh.opts.tick_ms);
    }
}

/// When the loop's periodic work last ran (boot clock); None before the first time.
#[derive(Default)]
struct Upkeep {
    facts_at: Option<u64>,
    upkeep_at: Option<u64>,
    saved_again_at: Option<u64>,
}

/// Whether periodic work last done at `last` is due again at `now` (boot clock).
fn due(last: Option<u64>, now: u64, period: u64) -> bool {
    last.is_none_or(|t| now.saturating_sub(t) >= period)
}

fn tick(sh: &Arc<Shared>, up: &mut Upkeep) {
    let at = Now::read(&*sh.parts.clock);
    let now = at.wall;
    if due(up.saved_again_at, at.boot, SAVE_AGAIN_MS) {
        let mut st = sh.lock();
        if !st.unsaved.is_empty() {
            st.save_again();
        }
        up.saved_again_at = Some(at.boot);
    }
    sh.reap_orphans();
    watch(sh, at);
    // Draining for a restart in place: no job starts, and no periodic work begins; the next
    // daemon does them. A drain beginning while a plan is made waits for it (`planning`).
    let planning = sh.up.planning();
    if sh.up.draining() {
        return;
    }
    plan_and_start(sh, now);
    drop(planning);
    workspace_tick(sh, now);
    if due(up.facts_at, at.boot, sh.opts.facts_refresh_ms) {
        // Not at the first: the daemon detected them as it started (unless it started from a
        // handover, which gave it the facts from before).
        if up.facts_at.is_some() || sh.from_handover {
            refresh_facts(sh);
        }
        up.facts_at = Some(at.boot);
    }
    if due(up.upkeep_at, at.boot, UPKEEP_MS) {
        retention(sh, at);
        refresh_sizes(sh);
        retry_removals(sh);
        retry_cleanups(sh);
        // Not at the first: a member's queue may not have arrived yet.
        if up.upkeep_at.is_some() {
            remove_unsalted(sh);
        }
        up.upkeep_at = Some(at.boot);
    }
}

/// A running invocation to sample, taken from the state.
struct Probe {
    id: JobId,
    handle: Handle,
    mem_max: u64,
    status: PathBuf,
    log: PathBuf,
}

/// What sampling it found.
struct Seen {
    sample: Result<Sample, String>,
    status: Option<ExitStatus>,
    log_len: Option<u64>,
    /// When its processes are gone without a status file: the disk holding that file is full.
    full: Option<String>,
    /// The log's modification time: past its limit the log stops growing, and the supervisor
    /// moves this instead while it drops output.
    log_mtime: Option<std::time::SystemTime>,
}

/// What the loop does after deciding, without the state lock.
enum Act {
    Signal(JobId, Handle, i32),
    /// Sample stacks into the log (slow: on a worker); then end the job if `kill` (kill on stall).
    Stacks {
        id: JobId,
        handle: Handle,
        log: PathBuf,
        header: String,
        kill: Option<Ending>,
    },
    Finish(JobId),
}

/// Samples every running invocation and ends those that are over.
fn watch(sh: &Arc<Shared>, now: Now) {
    let probes: Vec<Probe> = {
        let st = sh.lock();
        st.runs
            .iter()
            .filter(|(_, r)| r.phase == Phase::Running)
            .filter_map(|(id, _)| {
                let j = st.jobs.get(id)?;
                let dir = sh.paths.job_dir(id);
                Some(Probe {
                    id: id.clone(),
                    handle: j.handle.clone()?,
                    mem_max: j.lease.as_ref()?.grant.ram_mib << 20,
                    status: dir.join(files::STATUS),
                    log: dir.join(files::LOG),
                })
            })
            .collect()
    };
    if probes.is_empty() {
        return;
    }
    let mut seen = Vec::new();
    for mut p in probes {
        // Sample first, then read the status file: the supervisor writes it before it exits, so
        // processes gone without a status file really ended without one.
        let sample = sh
            .parts
            .executor
            .sample(&mut p.handle, p.mem_max)
            .map_err(|e| format!("{e:#}"));
        let status = files::read_status(&p.status);
        let meta = std::fs::metadata(&p.log).ok();
        // Gone, or (macOS) its supervisor is: nothing will write the status file any more.
        let gone = sample.as_ref().is_ok_and(|s| {
            s.pids.is_empty()
                || matches!(&p.handle, Handle::Process { pid, .. } if !s.pids.contains(pid))
        });
        let full = (gone && status.is_none())
            .then(|| p.status.parent().and_then(crate::workspace::disk_full))
            .flatten()
            .map(|f| f.to_string());
        seen.push((
            p,
            Seen {
                sample,
                status,
                log_len: meta.as_ref().map(|m| m.len()),
                log_mtime: meta.and_then(|m| m.modified().ok()),
                full,
            },
        ));
    }
    let mut acts = Vec::new();
    {
        let mut guard = sh.lock();
        for (p, s) in seen {
            decide(&mut guard, &sh.opts, now, p, s, &mut acts);
        }
    }
    let mut ended = false;
    for act in acts {
        match act {
            Act::Signal(id, handle, sig) => {
                if let Err(e) = sh.parts.executor.signal(&handle, sig) {
                    sh.log.log(format!("{id}: signal {sig}: {e:#}"));
                }
            }
            Act::Stacks {
                id,
                handle,
                log,
                header,
                kill,
            } => {
                sh.spawn_worker(&format!("stacks-{id}"), move |sh| {
                    stacks(sh, &id, &handle, &log, &header, kill)
                });
            }
            Act::Finish(id) => {
                ended = true;
                sh.spawn_worker(&format!("finish-{id}"), move |sh| finish(sh, &id));
            }
        }
    }
    if ended {
        sh.notify();
    }
}

/// Applies one sample to its job and decides whether and how it ends. Records get wall times;
/// the run limit is measured on the boot clock, the kill grace and stalls on the awake clock.
fn decide(st: &mut State, opts: &Options, at: Now, p: Probe, s: Seen, acts: &mut Vec<Act>) {
    let now = at.wall;
    let id = p.id;
    let Some(mut run) = st.runs.get(&id).cloned() else {
        return;
    };
    if run.phase != Phase::Running {
        return;
    }
    let Some(job) = st.jobs.get_mut(&id) else {
        return;
    };
    let Some(lease) = job.lease.clone() else {
        return;
    };
    let was = (run.ending.clone(), run.term_at, run.kill_at, job.stalled);
    // A macOS handle tracks every process seen, with the time each went (up to hundreds of KB),
    // and changes at nearly every sample of a job that keeps starting processes: it is written
    // with the usage, every USAGE_SAVE_MS, and when the daemon stops. A daemon that dies loses
    // at most that much of it (the next one still finds what is in the job's session or under
    // a process it tracks).
    job.handle = Some(p.handle.clone());
    let mut over_memory = false;
    let mut over_tasks = None;
    let mut oom_killed = false;
    let pids = match &s.sample {
        Ok(sample) => {
            run.sample_errors = 0;
            if run.cpu.progress(at.awake, sample.cpu_ms, opts.stall_ms) {
                job.usage.cpu_progress_at = now;
                run.active_at = Some(at.awake);
            }
            job.usage.cpu_ms = job.usage.cpu_ms.max(sample.cpu_ms);
            job.usage.mem_bytes = sample.mem_bytes;
            job.usage.peak_mem_bytes = job
                .usage
                .peak_mem_bytes
                .max(sample.mem_bytes)
                .max(sample.peak_mem_bytes);
            job.usage.pids = sample.pids.clone();
            over_memory = sample.over_memory;
            over_tasks = sample.over_tasks;
            oom_killed = sample.oom_killed;
            Some(sample.pids.clone())
        }
        Err(e) => {
            run.sample_errors += 1;
            if run.sample_errors == 1 {
                st.log.log(format!("{id}: sampling failed: {e}"));
            }
            None
        }
    };
    if let Some(len) = s.log_len {
        let output = len.saturating_sub(run.daemon_log_bytes);
        if run.last_output.is_some_and(|prev| output > prev) {
            job.usage.output_at = now;
            run.active_at = Some(at.awake);
        }
        run.last_output = Some(output);
        job.usage.log_bytes = len;
    }
    // Any change counts (a clock set back moves it backwards); the daemon's own writes set it
    // as they leave it ([`stacks`]).
    if let Some(t) = s.log_mtime {
        if run.log_mtime.is_some_and(|prev| prev != t) && run.log_mtime_mine != Some(t) {
            job.usage.output_at = now;
            run.active_at = Some(at.awake);
        }
        run.log_mtime = Some(t);
    }
    // When sampling fails, a status file still says the command is over.
    let gone = match &pids {
        Some(p) => p.is_empty(),
        None => s.status.is_some(),
    };
    if run.adopted && pids.as_ref().is_some_and(|p| !p.is_empty()) {
        // This daemon has seen it run: an ending without a status from now on is one it saw.
        run.adopted = false;
    }
    // macOS: the handle's leader is the supervisor. Gone while processes of the job live on, it
    // can no longer read their output or record how the command ends. (On Linux systemd stops
    // the unit when its main process exits.)
    let supervisor_gone = match (&p.handle, &pids) {
        (Handle::Process { pid, .. }, Some(live)) => !live.is_empty() && !live.contains(pid),
        _ => false,
    };
    let grace = opts.kill_grace_ms;
    if run.ending.is_none() {
        let mut ending = None;
        if over_memory {
            ending = Some(Ending {
                state: JobState::OverMemory,
                exit_code: Some(EXIT_OVER_MEMORY),
                signal: s.status.as_ref().and_then(|s| s.signal),
                message: Some(over_memory_message(
                    job.usage.peak_mem_bytes.max(job.usage.mem_bytes),
                    lease.grant.ram_mib,
                )),
                at: now,
            });
            if !gone {
                acts.push(Act::Signal(id.clone(), p.handle.clone(), libc::SIGKILL));
                run.sent_kill(at);
            }
        } else if let Some((seen, limit)) = over_tasks {
            // The monitor's SIGKILL (macOS), whatever then ended the command.
            ending = Some(Ending {
                state: JobState::Failed,
                exit_code: None,
                signal: Some(libc::SIGKILL),
                message: Some(over_tasks_message(seen, limit)),
                at: now,
            });
            if !gone {
                acts.push(Act::Signal(id.clone(), p.handle.clone(), libc::SIGKILL));
                run.sent_kill(at);
            }
        } else if oom_killed {
            // The OOM killer's signal, whatever then ended the command (systemd stops the rest).
            ending = Some(Ending {
                state: JobState::Failed,
                exit_code: None,
                signal: Some(libc::SIGKILL),
                message: Some(oom_killed_message(
                    job.usage.peak_mem_bytes.max(job.usage.mem_bytes),
                    lease.grant.ram_mib,
                )),
                at: now,
            });
            if !gone {
                acts.push(Act::Signal(id.clone(), p.handle.clone(), libc::SIGKILL));
                run.sent_kill(at);
            }
        } else if let Some(status) = &s.status {
            ending = Some(ending_from_status(status));
            if !gone {
                // The command is over; whatever it left behind is killed.
                acts.push(Act::Signal(id.clone(), p.handle.clone(), libc::SIGKILL));
                run.sent_kill(at);
            }
        } else if supervisor_gone {
            let why = match &s.full {
                // It gave up on the status file after a minute.
                Some(full) => format!(
                    "killed: its supervisor could not record how the command ended: {full}; free space there. The processes it left were killed"
                ),
                None => "killed: its supervisor ended without recording an exit status, and the processes it left were killed".into(),
            };
            ending = Some(Ending {
                state: JobState::Failed,
                exit_code: None,
                signal: None,
                message: Some(why),
                at: now,
            });
            acts.push(Act::Signal(id.clone(), p.handle.clone(), libc::SIGKILL));
            run.sent_kill(at);
        } else if gone {
            ending = Some(if let Some(full) = &s.full {
                // Its supervisor could not write the status file (it tries for a minute).
                Ending {
                    state: JobState::Failed,
                    exit_code: None,
                    signal: None,
                    message: Some(format!(
                        "its processes ended, but how could not be recorded: {full}; free space there"
                    )),
                    at: now,
                }
            } else if run.adopted {
                Ending {
                    state: JobState::Lost,
                    exit_code: Some(EXIT_LOST),
                    signal: None,
                    message: Some(
                        "its processes were gone after a daemon restart, and nothing recorded how it ended"
                            .into(),
                    ),
                    at: now,
                }
            } else {
                Ending {
                    state: JobState::Failed,
                    exit_code: None,
                    signal: None,
                    message: Some(
                        "killed: its processes ended without recording an exit status".into(),
                    ),
                    at: now,
                }
            });
        } else if run.sample_errors >= MAX_SAMPLE_ERRORS {
            ending = Some(Ending {
                state: JobState::Lost,
                exit_code: Some(EXIT_LOST),
                signal: None,
                message: Some(format!(
                    "its processes could not be sampled ({})",
                    s.sample.as_ref().err().cloned().unwrap_or_default()
                )),
                at: now,
            });
        } else if run.cancel {
            ending = Some(Ending {
                state: JobState::Cancelled,
                exit_code: Some(EXIT_CANCELLED),
                signal: None,
                message: Some("cancelled".into()),
                at: now,
            });
            acts.push(Act::Signal(id.clone(), p.handle.clone(), libc::SIGTERM));
            run.sent_term(at);
        } else if at.boot
            >= *run
                .deadline
                .get_or_insert_with(|| at.boot_of(lease.deadline))
        {
            ending = Some(Ending {
                state: JobState::TimedOut,
                exit_code: Some(EXIT_TIMEOUT),
                signal: None,
                message: Some(format!(
                    "killed at its run limit ({})",
                    fmt_ms(job.spec.timeout_ms)
                )),
                at: now,
            });
            acts.push(Act::Signal(id.clone(), p.handle.clone(), libc::SIGTERM));
            run.sent_term(at);
        } else if opts.stall_ms > 0 {
            // Awake time: a machine asleep runs none of its jobs.
            let idle = match run.active_at {
                Some(t) => at.awake.saturating_sub(t),
                // Stalled before a restart, with no progress seen since.
                None if job.stalled => u64::MAX,
                None => {
                    run.active_at = Some(at.awake);
                    0
                }
            };
            if idle >= opts.stall_ms {
                if !job.stalled && !run.stacks_busy {
                    job.stalled = true;
                    run.stacks_busy = true;
                    st.log.log(format!("{id}: stalled for {}", fmt_ms(idle)));
                    // Stacks first, then (if the dispatch asked) the end: the worker decides it.
                    let kill = job.spec.kill_on_stall.then(|| Ending {
                        state: JobState::TimedOut,
                        exit_code: Some(EXIT_TIMEOUT),
                        signal: None,
                        message: Some(format!(
                            "stalled: under 1% CPU and no output for {}; ended as its dispatch asked (kill on stall)",
                            fmt_ms(idle)
                        )),
                        at: now,
                    });
                    acts.push(Act::Stacks {
                        id: id.clone(),
                        handle: p.handle.clone(),
                        log: p.log.clone(),
                        header: format!(
                            "--- potluck: under 1% CPU and no output for {}; stacks of its processes ---",
                            fmt_ms(idle)
                        ),
                        kill,
                    });
                }
            } else if job.stalled {
                job.stalled = false;
            }
        }
        run.ending = ending;
    } else if let (Some(status), Some(e)) = (&s.status, run.ending.as_mut())
        && e.signal.is_none()
    {
        // The command ended during an ending potluck started: note the signal that ended it.
        e.signal = status.signal;
    }

    if let Some(ending) = &run.ending {
        let finish = if gone || ending.state == JobState::Lost {
            true
        } else if run.kill_at.is_some() {
            if run.grace_after_kill(at, grace) {
                // Processes that won't die (uninterruptible waits) don't keep the lease.
                job.stuck_pids = pids.clone().unwrap_or_default();
                st.log.log(format!(
                    "{id}: processes {:?} survived SIGKILL; releasing the lease anyway",
                    job.stuck_pids
                ));
                true
            } else {
                false
            }
        } else {
            if run.grace_after_term(at, grace) {
                acts.push(Act::Signal(id.clone(), p.handle.clone(), libc::SIGKILL));
                run.sent_kill(at);
            }
            false
        };
        if finish {
            // It ended when its command did (the status file says when), unless potluck had to
            // signal it or what it left behind: then when the last of it was gone, now.
            let at = match &s.status {
                Some(x) if run.term_at.is_none() && run.kill_at.is_none() => x.ended_at,
                _ => now,
            };
            if let Some(e) = run.ending.as_mut() {
                e.at = at;
            }
            run.phase = Phase::Finishing;
            acts.push(Act::Finish(id.clone()));
        }
    }
    let material = was != (run.ending.clone(), run.term_at, run.kill_at, job.stalled)
        || run.phase != Phase::Running;
    let save_usage = at.boot.saturating_sub(run.saved_at) >= USAGE_SAVE_MS;
    if material || save_usage {
        run.saved_at = at.boot;
    }
    st.runs.insert(id.clone(), run);
    if material || save_usage {
        st.save_job(&id);
    }
    if material {
        st.save_run(&id);
    }
}

/// Samples a stalled invocation's stacks into its log (this can take many seconds), then ends
/// it if its dispatch asked for that and it is still stalled.
fn stacks(
    sh: &Arc<Shared>,
    id: &str,
    handle: &Handle,
    log: &Path,
    header: &str,
    kill: Option<Ending>,
) {
    let text = sh
        .parts
        .executor
        .stacks(handle)
        .unwrap_or_else(|e| format!("(no stacks: {e:#})"));
    let block = format!("\n{header}\n{text}\n--- potluck: end of stacks ---\n");
    let mut end = false;
    {
        let mut st = sh.lock();
        // Progress since the stall (the loop goes on sampling meanwhile) unmarks it.
        let stalled = st.jobs.get(id).is_some_and(|j| j.stalled);
        let Some(run) = st.runs.get_mut(id) else {
            return;
        };
        run.stacks_busy = false;
        if run.phase != Phase::Running {
            return;
        }
        // Counted before it is written, so a sample in between never sees it as output.
        run.daemon_log_bytes += block.len() as u64;
        let _ = std::fs::OpenOptions::new()
            .append(true)
            .open(log)
            .and_then(|mut f| f.write_all(block.as_bytes()));
        // And the time it leaves on the log, which isn't the job's output either.
        run.log_mtime_mine = std::fs::metadata(log).and_then(|m| m.modified()).ok();
        if let Some(ending) = kill
            && run.ending.is_none()
            && stalled
        {
            let now = Now::read(&*sh.parts.clock);
            run.ending = Some(Ending {
                at: now.wall,
                ..ending
            });
            run.sent_term(now);
            end = true;
        }
        st.save_run(id);
    }
    if end {
        if let Err(e) = sh.parts.executor.signal(handle, libc::SIGTERM) {
            sh.log.log(format!("{id}: signal: {e:#}"));
        }
        sh.kick();
    }
}

fn ending_from_status(s: &ExitStatus) -> Ending {
    let (state, message) = match (s.code, s.signal) {
        (Some(0), _) => (JobState::Succeeded, "exited with code 0".to_string()),
        (Some(c), _) => (JobState::Failed, format!("exited with code {c}")),
        (None, Some(sig)) => (
            JobState::Failed,
            format!("killed by signal {sig}{}", signal_name(sig)),
        ),
        (None, None) => (JobState::Failed, "ended without an exit code".to_string()),
    };
    Ending {
        state,
        exit_code: s.code,
        signal: s.signal,
        message: Some(message),
        at: s.ended_at,
    }
}

fn signal_name(sig: i32) -> &'static str {
    match sig {
        libc::SIGHUP => " (SIGHUP)",
        libc::SIGINT => " (SIGINT)",
        libc::SIGQUIT => " (SIGQUIT)",
        libc::SIGILL => " (SIGILL)",
        libc::SIGABRT => " (SIGABRT)",
        libc::SIGFPE => " (SIGFPE)",
        libc::SIGKILL => " (SIGKILL)",
        libc::SIGBUS => " (SIGBUS)",
        libc::SIGSEGV => " (SIGSEGV)",
        libc::SIGPIPE => " (SIGPIPE)",
        libc::SIGALRM => " (SIGALRM)",
        libc::SIGTERM => " (SIGTERM)",
        _ => "",
    }
}

/// What a log line says after a job's state: ": message", without the state again when the
/// message only names it ("cancelled") or starts with it ("over memory: used ...").
pub(crate) fn after_state(state: JobState, message: Option<&str>) -> String {
    let state = state.to_string();
    match message {
        None => String::new(),
        Some(m) if m == state => String::new(),
        Some(m) => match m.strip_prefix(&state).and_then(|r| r.strip_prefix(": ")) {
            Some(rest) => format!(": {rest}"),
            None => format!(": {m}"),
        },
    }
}

/// "over memory: used 4.2 GiB of its 4 GiB", "over memory: used 300 MiB of its 256 MiB": sizes as
/// `why` and `status` write them.
pub(crate) fn over_memory_message(used_bytes: u64, limit_mib: u64) -> String {
    let limit = fmt_mib(limit_mib);
    if used_bytes == 0 {
        format!("over memory: went over its {limit}")
    } else {
        format!(
            "over memory: used {} of its {limit}",
            fmt_mib(mib(used_bytes))
        )
    }
}

/// "too many processes: 5334, over its limit of 5333 (half of kern.maxprocperuid, the most
/// processes one user may have)". On macOS a limit below [`TASKS_MAX`] is that half.
pub(crate) fn over_tasks_message(seen: u32, limit: u32) -> String {
    let why = if limit < TASKS_MAX {
        " (half of kern.maxprocperuid, the most processes one user may have)"
    } else {
        ""
    };
    format!("too many processes: {seen}, over its limit of {limit}{why}")
}

/// "killed because the machine ran short of memory, under its own limit: used 1.2 GiB of its 4 GiB"
pub(crate) fn oom_killed_message(used_bytes: u64, limit_mib: u64) -> String {
    let limit = fmt_mib(limit_mib);
    let used = if used_bytes == 0 {
        format!("its limit was {limit}")
    } else {
        format!("used {} of its {limit}", fmt_mib(mib(used_bytes)))
    };
    format!("killed because the machine ran short of memory, under its own limit: {used}")
}

/// Bytes in whole MiB, rounded to the nearest.
fn mib(bytes: u64) -> u64 {
    bytes.saturating_add(1 << 19) >> 20
}

/// The `[[gpu]]` env of `gpus` (ids).
pub(crate) fn gpu_env(
    config: &NodeConfig,
    facts: &Facts,
    gpus: &[String],
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for g in gpu_configs(config, facts, gpus) {
        env.extend(g.env.clone());
    }
    env
}

fn gpu_configs<'a>(
    config: &'a NodeConfig,
    facts: &'a Facts,
    gpus: &'a [String],
) -> impl Iterator<Item = &'a crate::config::GpuConfig> {
    gpus.iter()
        .filter_map(|id| facts.gpus.iter().find(|g| &g.id == id))
        .flat_map(|gpu| config.gpu.iter().filter(|c| gpu_matches(&c.matches, gpu)))
}

/// Plans with the scheduler and claims what it starts.
fn plan_and_start(sh: &Arc<Shared>, now: Millis) {
    let mut starts = Vec::new();
    let mut unpins = Vec::new();
    let cluster = sh.cluster();
    // Other machines' jobs this plan starts (claimed from their owners), and passes over them.
    let mut claims = Vec::new();
    let mut passed = Vec::new();
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        fail_orphans(st, now, &mut unpins);
        if cluster.is_none() && !st.jobs.values().any(|j| j.state == JobState::Queued) {
            st.reservation = None;
        } else {
            'plan: {
                let mut queue = st.queue();
                if cluster.is_some() {
                    super::cluster::adjust_queue(sh, st, &mut queue, now);
                }
                if queue.is_empty() {
                    st.reservation = None;
                    break 'plan;
                }
                let snaps = st.snaps();
                let Some((_, local)) = snaps.iter().find(|(n, _)| *n == st.node) else {
                    break 'plan;
                };
                let plan = sh.parts.scheduler.plan(now, &local.view(), &queue);
                st.reservation = plan.reservation.clone();
                for id in &plan.passed {
                    // Another machine's job: the pass goes to its owner, even when this machine
                    // has a record by that id (its run of the job before, ended).
                    let own = super::cluster::peer::owner_of(id) == st.node;
                    match st.jobs.get_mut(id) {
                        Some(j) if own => {
                            j.passes = j.passes.saturating_add(1);
                            st.save_job(id);
                        }
                        _ => passed.push(id.clone()),
                    }
                }
                let mut taken_ws = BTreeSet::new();
                for (id, grant) in &plan.start {
                    let remote = cluster
                        .as_ref()
                        .is_some_and(|c| super::cluster::remote::place(c, st, id));
                    if claim(sh, st, id, grant, now, &mut taken_ws, &mut unpins) {
                        if remote {
                            claims.push(id.clone());
                        } else {
                            starts.push(id.clone());
                        }
                    } else if remote {
                        super::cluster::remote::unplace(st, id);
                    }
                }
                st.refresh_pending();
            }
        }
    }
    if !unpins.is_empty() {
        // On a worker: an unpin waits for the mirror's lock, which a sync holds for as long as it
        // fetches (minutes for a big repository), and the loop must go on meanwhile (and a drain
        // or a handover not wait for it). Cut short by a restart, the pins are dropped by the
        // next daemon's sweep ([`sweep_pins`]): their jobs are over.
        sh.spawn_worker("unpin", move |sh| {
            for (repo, name) in unpins {
                unpin_all(sh, &repo, &name);
            }
        });
    }
    if !starts.is_empty() {
        sh.notify();
    }
    for id in starts {
        sh.spawn_worker(&format!("start-{id}"), move |sh| start_job(sh, &id));
    }
    if !claims.is_empty() || !passed.is_empty() {
        super::cluster::remote::after_plan(sh, claims, passed);
    }
}

/// Queued jobs pinned to a workspace here that is gone fail at once (exit 1, as the cluster fails
/// one whose workspace went from a member; 125 is for a machine lost). In a cluster, the cluster
/// fails those pinned to workspaces on other machines.
fn fail_orphans(st: &mut State, now: Millis, unpins: &mut Vec<(RepoRef, String)>) {
    use super::cluster::peer::{node_of_ws, owner_of};
    let orphans: Vec<JobId> = st
        .jobs
        .values()
        .filter(|j| j.state == JobState::Queued && owner_of(&j.id) == st.node)
        .filter(|j| {
            j.spec
                .workspace
                .as_ref()
                .is_some_and(|w| node_of_ws(w) == st.node && st.live_ws(w).is_none())
        })
        .map(|j| j.id.clone())
        .collect();
    for id in orphans {
        if let Some(j) = st.jobs.get_mut(&id) {
            j.state = JobState::Failed;
            j.ended_at = Some(now);
            let why = format!(
                "workspace {} is gone",
                j.spec.workspace.clone().unwrap_or_default()
            );
            st.log.log(format!("{id}: failed: {why}"));
            j.message = Some(why);
            unpins.push((j.spec.repo.clone(), id.clone()));
        }
        st.save_job(&id);
    }
}

/// Claims a planned start: binds its workspace (a new one, or the one it is pinned to), grants
/// the lease and marks it starting. False if it must stay queued (or failed at once).
fn claim(
    sh: &Shared,
    st: &mut State,
    id: &str,
    grant: &Grant,
    now: Millis,
    taken_ws: &mut BTreeSet<String>,
    unpins: &mut Vec<(RepoRef, String)>,
) -> bool {
    let Some(job) = st.jobs.get(id).cloned() else {
        return false;
    };
    if job.state != JobState::Queued {
        return false;
    }
    let node = st.node.clone();
    let (ws_id, new) = match &job.spec.workspace {
        Some(w) => {
            if st.live_ws(w).is_none() {
                fail_orphans(st, now, unpins);
                return false;
            }
            // One invocation at a time in a workspace.
            if st.ws_busy(w) || taken_ws.contains(w) {
                return false;
            }
            (w.clone(), false)
        }
        None => match st.store.next_seq("workspace") {
            Ok(seq) => (format!("{node}-w{seq}"), true),
            Err(e) => {
                st.log.log(format!("{id}: no workspace id: {e:#}"));
                return false;
            }
        },
    };
    taken_ws.insert(ws_id.clone());
    if new {
        st.workspaces.insert(
            ws_id.clone(),
            WorkspaceRecord {
                id: ws_id.clone(),
                node: node.clone(),
                repo_id: job.spec.repo.id.clone(),
                commit: job.spec.repo.commit.clone(),
                branch: job.spec.repo.branch.clone(),
                path: sh.paths.workspace(&ws_id),
                state: WorkspaceState::Seeding,
                created_at: now,
                last_used_at: now,
                held_until: 0,
                pending: 0,
                size_bytes: None,
            },
        );
    } else if let Some(ws) = st.workspaces.get_mut(&ws_id) {
        ws.state = WorkspaceState::Running;
        ws.last_used_at = now;
    }
    let j = st.jobs.get_mut(id).expect("checked");
    j.state = JobState::Starting;
    j.node = Some(node);
    j.workspace = Some(ws_id.clone());
    // A message from a run before ("requeued: ...") is in the log; it doesn't describe this one.
    j.message = None;
    j.lease = Some(Lease {
        grant: grant.clone(),
        granted_at: now,
        deadline: now.saturating_add(job.spec.timeout_ms),
        token: new_token(),
    });
    st.runs.insert(
        id.to_string(),
        Run {
            phase: Phase::Starting,
            new_workspace: new,
            ..Default::default()
        },
    );
    st.save_job(id);
    st.save_ws(&ws_id);
    st.save_run(id);
    true
}

/// 128 random bits, hex: the lease token a job proves itself with.
pub(crate) fn new_token() -> String {
    let mut b = [0u8; 16];
    let ok = std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .is_ok();
    if !ok {
        // Never expected; still unguessable enough for a local secret.
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        b = (t ^ ((std::process::id() as u128) << 64)).to_le_bytes();
    }
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Why an invocation could not start.
enum NotStarted {
    Cancelled,
    Failed(String),
    /// The daemon stopped or handed over meanwhile: nothing is written, and the daemon that
    /// comes next starts the job from what is stored (a restart in place that doesn't go on
    /// starts another daemon in this process, which does so beside this thread).
    Stopped,
}

/// Seeds or verifies the workspace and starts the invocation (on a worker thread).
pub(crate) fn start_job(sh: &Arc<Shared>, id: &str) {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| prepare_and_start(sh, id)))
        .unwrap_or_else(|p| {
            Err(NotStarted::Failed(format!(
                "internal error: {}",
                panic_text(&p)
            )))
        });
    let ending = match r {
        Ok(()) | Err(NotStarted::Stopped) => return,
        Err(NotStarted::Cancelled) => Ending {
            state: JobState::Cancelled,
            exit_code: Some(EXIT_CANCELLED),
            signal: None,
            message: Some("cancelled before it started".into()),
            at: sh.parts.clock.wall_ms(),
        },
        Err(NotStarted::Failed(msg)) => {
            sh.log.log(format!("{id}: did not start: {msg}"));
            Ending {
                state: JobState::Failed,
                exit_code: None,
                signal: None,
                message: Some(msg),
                at: sh.parts.clock.wall_ms(),
            }
        }
    };
    {
        let mut st = sh.lock();
        if let Some(run) = st.runs.get_mut(id) {
            run.ending = Some(ending);
            run.phase = Phase::Finishing;
        }
        st.save_run(id);
    }
    finish(sh, id);
}

/// For a start a restart cut short, whose executor couldn't tell whether it had started the
/// invocation ([`Run::find_again`]): asks it again, every few seconds until it can tell. True
/// when that start had started it: the invocation is then adopted as running.
fn find_again(sh: &Arc<Shared>, id: &str) -> Result<bool, NotStarted> {
    let (job, run) = {
        let st = sh.lock();
        match (st.jobs.get(id), st.runs.get(id)) {
            (Some(j), Some(r)) if r.find_again => (j.clone(), r.clone()),
            _ => return Ok(false),
        }
    };
    let status = sh.paths.job_dir(id).join(files::STATUS);
    let mut tries = 0u32;
    let found = loop {
        let at = Now::read(&*sh.parts.clock);
        let since = run_since(&job, Some(&run), at, &sh.parts.clock.boot_id());
        match sh.parts.executor.find(&launch_name(id), &status, since) {
            Ok(found) => break found,
            // Said once a minute: nothing starts it until it can tell.
            Err(e) if tries.is_multiple_of(30) => sh.log.log(format!(
                "{id}: still can't tell whether its start had started it: {e:#}"
            )),
            Err(_) => {}
        }
        tries += 1;
        let until = std::time::Instant::now() + std::time::Duration::from_millis(FIND_AGAIN_MS);
        while std::time::Instant::now() < until && !sh.stopping() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if sh.stopping() {
            return Err(NotStarted::Stopped);
        }
    };
    let mut guard = sh.lock();
    if sh.stopping() {
        return Err(NotStarted::Stopped);
    }
    let st = &mut *guard;
    let (Some(job), Some(run)) = (st.jobs.get_mut(id), st.runs.get_mut(id)) else {
        return Err(NotStarted::Cancelled);
    };
    run.find_again = false;
    let Some(handle) = found else {
        st.log.log(format!(
            "{id}: its start had started nothing; starting again"
        ));
        return Ok(false);
    };
    // As [`restore`] adopts one: a cancel asked meanwhile still applies, and its run limit
    // counts from when its start began, when that is on this boot's clock.
    let at = Now::read(&*sh.parts.clock);
    let boot = sh.parts.clock.boot_id();
    let began = Some(&*run)
        .filter(|r| same_boot(r, &boot))
        .and_then(|r| r.starting_at)
        .map_or(at.wall, |t| at.wall_of(t));
    job.state = JobState::Running;
    job.handle = Some(handle);
    job.started_at.get_or_insert(began);
    job.usage.cpu_progress_at = at.wall;
    job.usage.output_at = at.wall;
    if let Some(l) = job.lease.as_mut() {
        l.deadline = began.saturating_add(job.spec.timeout_ms);
    }
    run.phase = Phase::Running;
    run.adopted = true;
    carry_over(run, job, at, &boot);
    run.active_at = Some(at.awake);
    st.save_job(id);
    st.save_run(id);
    st.log.log(format!(
        "{id}: its start was cut short by a restart, but it runs; adopted"
    ));
    drop(guard);
    sh.notify();
    sh.kick();
    Ok(true)
}

fn prepare_and_start(sh: &Arc<Shared>, id: &str) -> Result<(), NotStarted> {
    let failed = NotStarted::Failed;
    if find_again(sh, id)? {
        return Ok(());
    }
    let (job, ws, new_ws, resumed, node) = {
        let st = sh.lock();
        let job = st.jobs.get(id).cloned().ok_or(NotStarted::Cancelled)?;
        let run = st.runs.get(id).cloned().unwrap_or_default();
        let ws = job
            .workspace
            .as_ref()
            .and_then(|w| st.workspaces.get(w))
            .cloned()
            .ok_or_else(|| failed("its workspace is gone".into()))?;
        if run.cancel {
            return Err(NotStarted::Cancelled);
        }
        (job, ws, run.new_workspace, run.resumed, st.node.clone())
    };
    let lease = job.lease.clone().ok_or_else(|| failed("no lease".into()))?;
    let wsp = &sh.parts.workspaces;
    if new_ws && resumed {
        // What the seeding a restart cut short left goes first.
        wsp.remove(&ws).map_err(|e| {
            failed(format!(
                "workspace {}: could not clear what its seeding left when a restart cut it short: {e:#}",
                ws.id
            ))
        })?;
        let tmp = sh.paths.workspace_tmp(&ws.id);
        if let Err(e) = std::fs::remove_dir_all(&tmp)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            return Err(failed(format!("could not remove {}: {e}", tmp.display())));
        }
    }
    let root = if new_ws {
        wsp.create(&ws.id, &job.spec.repo)
            .map_err(|e| failed(format!("could not seed workspace {}: {e:#}", ws.id)))?
    } else {
        wsp.verify(&ws.path, &job.spec.repo)
            .map_err(|e| failed(format!("workspace {} failed verification: {e:#}", ws.id)))?;
        ws.path.clone()
    };
    let tmp = sh.paths.workspace_tmp(&ws.id);
    let dir = sh.paths.job_dir(id);
    let io = |what: &str, e: std::io::Error| failed(format!("could not create {what}: {e}"));
    std::fs::create_dir_all(&tmp).map_err(|e| io("the workspace's TMPDIR", e))?;
    std::fs::create_dir_all(dir.join(files::KEEP)).map_err(|e| io("the job directory", e))?;
    std::fs::create_dir_all(dir.join(files::CRASH)).map_err(|e| io("the job directory", e))?;
    let status = dir.join(files::STATUS);
    let _ = std::fs::remove_file(&status);
    let log = dir.join(files::LOG);
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(|e| io("the output log", e))?;
    // What its sources went through, loudly, where its owner reads its output.
    for note in &job.notes {
        writeln!(out, "--- potluck: warning: {note} ---").map_err(|e| io("the output log", e))?;
    }
    drop(out);

    let (facts, login) = {
        let st = sh.lock();
        (st.local().facts.clone(), sh.login_env())
    };
    // The machine's (its login environment, then the env of each `[[gpu]]` entry for a GPU the
    // job holds), then the dispatch's (the committed potluck.toml, then `--env`), then
    // potluck's own variables, which nothing overrides.
    let mut env = login;
    env.extend(gpu_env(&sh.config, &facts, &lease.grant.gpus));
    env.extend(job.spec.env.clone());
    env.insert("POTLUCK_JOB".into(), id.to_string());
    env.insert("POTLUCK_LEASE".into(), lease.token.clone());
    env.insert("POTLUCK_WORKSPACE".into(), ws.id.clone());
    env.insert("POTLUCK_NODE".into(), node);
    // The granted GPU ids, comma-separated (empty without a GPU): a job that asked for any GPU on a
    // machine with several has no other way to know which it got.
    env.insert("POTLUCK_GPUS".into(), lease.grant.gpus.join(","));
    env.insert("TMPDIR".into(), tmp.display().to_string());
    // So a potluck call from inside the job reaches this daemon, wherever its home is.
    env.insert("POTLUCK_HOME".into(), sh.paths.home.display().to_string());
    env.insert(
        "POTLUCK_CONFIG".into(),
        sh.paths.config.display().to_string(),
    );

    {
        let mut st = sh.lock();
        if sh.stopping() {
            return Err(NotStarted::Stopped);
        }
        if st.runs.get(id).is_some_and(|r| r.cancel) {
            return Err(NotStarted::Cancelled);
        }
        if let Some(w) = st.workspaces.get_mut(&ws.id) {
            w.path = root.clone();
            w.state = WorkspaceState::Running;
            w.last_used_at = sh.parts.clock.wall_ms();
        }
        st.save_ws(&ws.id);
        // If the daemon stops inside the start, the next one counts the run limit from here.
        if let Some(r) = st.runs.get_mut(id) {
            r.starting_at = Some(sh.parts.clock.boot_ms());
            r.boot = Some(sh.parts.clock.boot_id());
        }
        st.save_run(id);
    }
    let launch = Launch {
        job: &job,
        grant: &lease.grant,
        cwd: root,
        env,
        log,
        status,
        potluck_bin: sh.helpers.path(),
        name: launch_name(id),
        limits: Limits {
            mem_max_bytes: lease.grant.ram_mib << 20,
            cpu_weight: lease.grant.cpu.max(1).saturating_mul(100).clamp(1, 10_000),
            tasks_max: TASKS_MAX,
            kill_grace_ms: sh.opts.kill_grace_ms,
            log_max_bytes: sh.config.log_max_bytes(),
        },
    };
    let handle = sh
        .parts
        .executor
        .start(&launch)
        .map_err(|e| failed(format!("could not start: {e:#}")))?;
    let at = Now::read(&*sh.parts.clock);
    let now = at.wall;
    {
        let mut st = sh.lock();
        let timeout = st.jobs.get(id).map_or(0, |j| j.spec.timeout_ms);
        if let Some(j) = st.jobs.get_mut(id) {
            j.state = JobState::Running;
            j.handle = Some(handle);
            j.started_at = Some(now);
            j.usage.cpu_progress_at = now;
            j.usage.output_at = now;
            if let Some(l) = j.lease.as_mut() {
                // The run limit counts from the start, not from the claim (seeding can be slow).
                l.deadline = now.saturating_add(j.spec.timeout_ms);
            }
        }
        if let Some(r) = st.runs.get_mut(id) {
            r.phase = Phase::Running;
            r.deadline = Some(at.boot.saturating_add(timeout));
            r.active_at = Some(at.awake);
        }
        st.save_job(id);
        st.save_run(id);
    }
    sh.log.log(format!("{id}: started in {}", ws.id));
    sh.notify();
    sh.kick();
    Ok(())
}

/// Ends an invocation whose ending is decided: copies its keep files, lets the executor clean up
/// (an invocation it can't clean up has stuck processes), then releases its lease, records the
/// job as over and sets its workspace's hold.
pub(crate) fn finish(sh: &Arc<Shared>, id: &str) {
    let (job, run, ws_root) = {
        let st = sh.lock();
        let Some(job) = st.jobs.get(id).cloned() else {
            return;
        };
        let Some(run) = st.runs.get(id).cloned() else {
            return;
        };
        let root = job
            .workspace
            .as_ref()
            .and_then(|w| st.live_ws(w))
            .map(|w| w.path.clone());
        (job, run, root)
    };
    let now = sh.parts.clock.wall_ms();
    let ending = run.ending.clone().unwrap_or(Ending {
        state: JobState::Lost,
        exit_code: Some(EXIT_LOST),
        signal: None,
        message: Some("no record of how it ended".into()),
        at: now,
    });
    let ran = job.handle.is_some();
    let dir = sh.paths.job_dir(id);
    let files::Kept { kept, failed } = match (&ws_root, ran && !job.spec.keep.is_empty()) {
        (Some(root), true) => {
            files::keep(root, &job.spec.keep, &dir.join(files::KEEP), &sh.log, id)
        }
        _ => files::Kept {
            kept: Vec::new(),
            failed: Vec::new(),
        },
    };
    let crash_dir = dir.join(files::CRASH);
    let since = run_since(
        &job,
        Some(&run),
        Now::read(&*sh.parts.clock),
        &sh.parts.clock.boot_id(),
    );
    let mut stuck = job.stuck_pids.clone();
    if let Some(h) = &job.handle
        && let Err(e) = sh.parts.executor.finish(h, &crash_dir, since)
    {
        // It could not go away: what is left of it is stuck. The lease is released anyway, and
        // the cleanup is tried again later (a Linux unit stays loaded until it is reset).
        sh.log.log(format!("{id}: cleaning up: {e:#}"));
        let cleanup = Cleanup {
            handle: h.clone(),
            tries: 0,
            since,
        };
        let mut st = sh.lock();
        match serde_json::to_string(&cleanup) {
            Ok(json) => st.save_kv(&format!("{CLEANUP_PREFIX}{id}"), Some(json)),
            Err(e) => st.log.log(format!("store: {e:#}")),
        }
        drop(st);
        if stuck.is_empty() {
            let mem = job
                .lease
                .as_ref()
                .map(|l| l.grant.ram_mib << 20)
                .unwrap_or(0);
            stuck = sh
                .parts
                .executor
                .sample(&mut h.clone(), mem)
                .map(|s| s.pids)
                .unwrap_or_default();
            if stuck.is_empty() {
                stuck = job.usage.pids.clone();
            }
        }
    }
    let now = sh.parts.clock.wall_ms();
    // Crash reports can be written seconds after the crash: they are collected again below, and
    // until then a job run for another machine holds them back from its owner. On macOS for every
    // job: a child's crash shows only as the command's exit code (a test binary's, under cargo).
    let late_crash = match &job.handle {
        Some(Handle::Process { .. }) => true,
        Some(Handle::Systemd { .. }) => ending.signal.is_some_and(is_crash_signal),
        None => false,
    };
    if late_crash {
        sh.crash_pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_string());
    }
    let removal = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let ws_id = job.workspace.clone();
        if let Some(j) = st.jobs.get_mut(id) {
            j.state = ending.state;
            j.exit_code = ending.exit_code;
            j.signal = ending.signal;
            j.message = ending.message.clone();
            j.ended_at = Some(ending.at.max(j.started_at.unwrap_or(0)));
            j.kept = kept;
            // `why` shows them, here and on its owner.
            for f in failed {
                let note = format!("--keep on {}: could not copy {f}", st.node);
                if !j.notes.contains(&note) {
                    j.notes.push(note);
                }
            }
            j.stuck_pids = stuck;
        }
        // Read now: a release asked while it was ending counts.
        let released = st.runs.get(id).is_some_and(|r| r.released);
        let mut removal = None;
        if let Some(ws_id) = &ws_id {
            if run.new_workspace && !ran {
                // Seeding failed or was cancelled: the half-made workspace goes.
                if let Some(ws) = st.workspaces.get_mut(ws_id) {
                    ws.state = WorkspaceState::Removed;
                    removal = Some(ws.clone());
                }
            } else if let Some(ws) = st.workspaces.get_mut(ws_id) {
                if ran {
                    // Released while it ran: the hold it would give ended with the release.
                    if !released {
                        let held = sh.parts.workspaces.hold_until(
                            ending.at,
                            job.spec.hold_ms,
                            ws.held_until,
                        );
                        // The hold is the branch's whose request sets when it ends.
                        if held > ws.held_until && job.spec.repo.branch.is_some() {
                            ws.branch = job.spec.repo.branch.clone();
                        }
                        ws.held_until = held;
                    }
                    ws.last_used_at = now;
                }
                if ws.state != WorkspaceState::Removed {
                    ws.state = if ws.held_until > now {
                        WorkspaceState::Held
                    } else {
                        WorkspaceState::Idle
                    };
                }
            }
            st.save_ws(ws_id);
        }
        // The ended record first: a daemon killed between the two writes leaves an ended job
        // with a stale run record (dropped at the next start), never a running job without the
        // run record that says how it ends (a cancel would come back as killed by SIGTERM).
        st.save_job(id);
        st.runs.remove(id);
        st.save_run(id);
        st.refresh_pending();
        removal
    };
    sh.log.log(format!(
        "{id}: {}{}",
        ending.state,
        after_state(ending.state, ending.message.as_deref())
    ));
    sh.notify();
    sh.kick();
    // After the end is saved, and after waiters are told: a pin outliving its job briefly costs
    // nothing, and one left by a daemon killed before this line is dropped at the next start
    // (sweep_pins). Unpinning first could drop the pin of a job the store still has running.
    unpin_all(sh, &job.spec.repo, id);
    if let Some(rec) = removal {
        let _ = remove_workspace(sh, &rec);
    } else if let (Some(ws_id), Some(root), true) = (&job.workspace, &ws_root, ran) {
        refresh_size(sh, ws_id, root);
    }
    // Crash reports can be written seconds after the crash: collect again later, unless the job
    // was deleted meanwhile (by retention): its directory is not made again for them.
    if let Some(h) = &job.handle
        && late_crash
    {
        let until =
            std::time::Instant::now() + std::time::Duration::from_millis(CRASH_REPORT_DELAY_MS);
        while std::time::Instant::now() < until && !sh.stopping() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if dir.is_dir() {
            let _ = sh.parts.executor.finish(h, &crash_dir, since);
        }
        sh.crash_pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }
}

fn is_crash_signal(sig: i32) -> bool {
    matches!(
        sig,
        libc::SIGSEGV | libc::SIGBUS | libc::SIGILL | libc::SIGABRT | libc::SIGTRAP | libc::SIGFPE
    )
}

/// Pins the commits of `repo` and its submodules under `name`; on a failure, unpins those done.
pub(crate) fn pin_all(sh: &Shared, repo: &RepoRef, name: &str) -> Result<(), String> {
    let w = &sh.parts.workspaces;
    let mut done: Vec<&str> = Vec::new();
    let all = std::iter::once((repo.id.as_str(), repo.commit.as_str())).chain(
        repo.submodules
            .iter()
            .map(|s| (s.repo_id.as_str(), s.commit.as_str())),
    );
    for (repo_id, commit) in all {
        if let Err(e) = w.pin(repo_id, commit, name) {
            for r in done {
                let _ = w.unpin(r, name);
            }
            return Err(format!("{repo_id} at {commit}: {e:#}"));
        }
        done.push(repo_id);
    }
    Ok(())
}

/// Drops the pins that no job or workspace here holds: left by a daemon killed between pinning
/// a dispatch's commits and recording its job, or between a job's end and its unpinning. Runs
/// when the daemon starts, before anything else can pin. Kept: the pins of jobs not done, of
/// workspaces not removed, and (in a cluster) of commits fetched for other machines' queued jobs.
pub(crate) fn sweep_pins(sh: &Shared) {
    let pins = match sh.parts.workspaces.pins() {
        Ok(p) => p,
        Err(e) => {
            sh.log.log(format!("pins: could not list them: {e:#}"));
            return;
        }
    };
    if pins.is_empty() {
        return;
    }
    let keep: BTreeSet<String> = {
        let st = sh.lock();
        let mut keep: BTreeSet<String> = st
            .jobs
            .values()
            .filter(|j| !j.state.is_done())
            .map(|j| j.id.clone())
            .collect();
        keep.extend(
            st.workspaces
                .values()
                .filter(|w| w.state != WorkspaceState::Removed)
                .map(|w| w.id.clone()),
        );
        if let Some(c) = sh.cluster() {
            keep.extend(c.cs().fetched.keys().cloned());
        }
        keep
    };
    let mut dropped = Vec::new();
    for (repo_id, name) in pins {
        if keep.contains(&name) {
            continue;
        }
        match sh.parts.workspaces.unpin(&repo_id, &name) {
            Ok(()) => dropped.push(format!("{name} in {repo_id}")),
            Err(e) => sh.log.log(format!("unpin {name} in {repo_id}: {e:#}")),
        }
    }
    if !dropped.is_empty() {
        sh.log.log(format!(
            "pins no job or workspace here holds, dropped: {}",
            dropped.join(", ")
        ));
    }
}

/// Releases `name`'s pins of `repo` and its submodules. Best effort: failures are logged.
pub(crate) fn unpin_all(sh: &Shared, repo: &RepoRef, name: &str) {
    let w = &sh.parts.workspaces;
    let ids =
        std::iter::once(repo.id.as_str()).chain(repo.submodules.iter().map(|s| s.repo_id.as_str()));
    for repo_id in ids {
        if let Err(e) = w.unpin(repo_id, name) {
            sh.log.log(format!("unpin {name} in {repo_id}: {e:#}"));
        }
    }
}

/// Removes a workspace already marked removed in the state: its tree and TMPDIR, and (by
/// [`WorkspaceProvider::remove`](super::WorkspaceProvider::remove)) the pins its creation made.
/// Does nothing while another removal of it is under way.
pub(crate) fn remove_workspace(sh: &Shared, rec: &WorkspaceRecord) -> anyhow::Result<()> {
    if !sh.lock().removing.insert(rec.id.clone()) {
        return Ok(());
    }
    let r = remove_workspace_files(sh, rec);
    sh.lock().removing.remove(&rec.id);
    r
}

fn remove_workspace_files(sh: &Shared, rec: &WorkspaceRecord) -> anyhow::Result<()> {
    let r = sh.parts.workspaces.remove(rec);
    if let Err(e) = &r {
        sh.log
            .log(format!("could not remove workspace {}: {e:#}", rec.id));
    }
    let tmp = sh.paths.workspace_tmp(&rec.id);
    if tmp.exists()
        && let Err(e) = std::fs::remove_dir_all(&tmp)
    {
        sh.log
            .log(format!("could not remove {}: {e}", tmp.display()));
    }
    sh.log.log(format!("workspace {} removed", rec.id));
    r
}

/// Removes again the workspaces marked removed whose files are still there: their removal failed,
/// or a restart cut it short (the record is marked first, then the files go).
fn retry_removals(sh: &Arc<Shared>) {
    let removed: Vec<WorkspaceRecord> = {
        let st = sh.lock();
        st.workspaces
            .values()
            .filter(|w| w.state == WorkspaceState::Removed && !st.removing.contains(&w.id))
            .cloned()
            .collect()
    };
    for rec in removed {
        if files_left(sh, &rec) {
            sh.log
                .log(format!("workspace {}: removing what is left of it", rec.id));
            sh.spawn_worker(&format!("remove-{}", rec.id), move |sh| {
                let _ = remove_workspace(sh, &rec);
            });
        }
    }
}

/// Whether a workspace's tree, the tree a removal moved aside ([`crate::workspace::aside`]: its
/// deletion failed, or a restart cut it short), or its TMPDIR is still on disk.
fn files_left(sh: &Shared, rec: &WorkspaceRecord) -> bool {
    [
        Some(rec.path.clone()),
        crate::workspace::aside(&rec.path),
        Some(sh.paths.workspace_tmp(&rec.id)),
    ]
    .iter()
    .flatten()
    .any(|p| !p.as_os_str().is_empty() && p.symlink_metadata().is_ok())
}

/// After an upgrade: removes the mirrors and bases whose repository ids are from before ids were
/// salted with the owner, once no workspace here and no unfinished job here or queued at a member
/// has such an id (on a worker).
pub(crate) fn remove_unsalted(sh: &Arc<Shared>) {
    let unsalted = |r: &RepoRef| crate::types::is_unsalted_repo_id(&r.id);
    let in_use = {
        let st = sh.lock();
        st.workspaces.values().any(|w| {
            w.state != WorkspaceState::Removed && crate::types::is_unsalted_repo_id(&w.repo_id)
        }) || st
            .jobs
            .values()
            .any(|j| !j.state.is_done() && unsalted(&j.spec.repo))
    };
    let queued = sh.cluster().is_some_and(|c| {
        c.cs()
            .reports
            .values()
            .any(|r| r.report.queue.iter().any(|j| unsalted(&j.spec.repo)))
    });
    if in_use || queued {
        return;
    }
    sh.spawn_worker_once("remove-unsalted", |sh| {
        let removed = sh.parts.workspaces.remove_unsalted();
        if !removed.is_empty() {
            sh.log.log(format!(
                "removed the mirrors and base checkouts of {} (repository ids from before ids \
                 were salted with the owner; nothing uses them now)",
                removed.join(", ")
            ));
        }
    });
}

/// Tries again the cleanups of finished invocations that failed (on a worker).
fn retry_cleanups(sh: &Arc<Shared>) {
    let entries = match sh.lock().store.kv_prefixed(CLEANUP_PREFIX) {
        Ok(e) => e,
        Err(e) => {
            sh.log.log(format!("store: {e:#}"));
            return;
        }
    };
    if entries.is_empty() {
        return;
    }
    sh.spawn_worker_once("cleanups", move |sh| {
        for (key, value) in entries {
            let id = &key[CLEANUP_PREFIX.len()..];
            let dir = sh.paths.job_dir(id);
            let cleanup = serde_json::from_str::<Cleanup>(&value)
                .map_err(|e| {
                    sh.log.log(format!(
                        "{id}: its cleanup record can't be read ({e}); not trying it again"
                    ))
                })
                .ok();
            let r = match &cleanup {
                // Its job is gone (pruned): nothing to clean up into.
                Some(c) if dir.is_dir() => {
                    let crash = dir.join(files::CRASH);
                    let _ = std::fs::create_dir_all(&crash);
                    Some(sh.parts.executor.finish(&c.handle, &crash, c.since))
                }
                _ => None,
            };
            let mut st = sh.lock();
            let keep = match (r, cleanup) {
                (Some(Ok(())), _) => {
                    st.log.log(format!("{id}: cleaned up"));
                    None
                }
                (Some(Err(e)), Some(mut c)) => {
                    c.tries += 1;
                    if c.tries >= CLEANUP_TRIES {
                        st.log.log(format!(
                            "{id}: cleaning up: {e:#}; given up after {} tries",
                            c.tries
                        ));
                        None
                    } else {
                        serde_json::to_string(&c).ok()
                    }
                }
                _ => None,
            };
            st.save_kv(&key, keep);
        }
    });
}

/// The lifecycle policy: holds that ran out become idle; workspaces over the budget go.
fn workspace_tick(sh: &Arc<Shared>, now: Millis) {
    let records = sh.lock().records();
    if records.is_empty() {
        return;
    }
    let budget = workspace_budget(sh);
    let actions = sh.parts.workspaces.tick(now, &records, budget);
    if actions.is_empty() {
        return;
    }
    let mut removals = Vec::new();
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        for a in actions {
            match a {
                Action::MakeIdle(w) => {
                    if let Some(ws) = st.workspaces.get_mut(&w)
                        && ws.state == WorkspaceState::Held
                        && ws.held_until <= now
                    {
                        ws.state = WorkspaceState::Idle;
                        st.save_ws(&w);
                    }
                }
                Action::Remove(w) => {
                    // The policy only removes idle workspaces, and decided on a snapshot: one
                    // held since (`potluck hold`) or given a job stays.
                    let busy = st.ws_busy(&w);
                    if let Some(ws) = st.workspaces.get_mut(&w)
                        && ws.pending == 0
                        && !busy
                        && ws.state == WorkspaceState::Idle
                    {
                        ws.state = WorkspaceState::Removed;
                        removals.push(ws.clone());
                        st.save_ws(&w);
                    }
                }
            }
        }
    }
    for rec in removals {
        sh.spawn_worker(&format!("remove-{}", rec.id), move |sh| {
            let _ = remove_workspace(sh, &rec);
        });
    }
}

/// The workspace disk budget for the volume holding the workspaces.
fn workspace_budget(sh: &Shared) -> u64 {
    let dir = sh.paths.home.join("workspaces");
    let _ = std::fs::create_dir_all(&dir);
    match volume_bytes(&dir) {
        Some(total) => sh.config.workspace_budget_bytes(total),
        None => u64::MAX,
    }
}

fn volume_bytes(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: statvfs fills `s`; `c` is a valid C string.
    unsafe {
        let mut s: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut s) != 0 {
            return None;
        }
        #[allow(clippy::useless_conversion)]
        Some(u64::from(s.f_blocks) * u64::from(s.f_frsize))
    }
}

fn refresh_size(sh: &Shared, ws_id: &str, root: &Path) {
    match sh.parts.workspaces.size(root) {
        Ok(n) => {
            let mut st = sh.lock();
            if let Some(ws) = st.workspaces.get_mut(ws_id) {
                ws.size_bytes = Some(n);
            }
            st.save_ws(ws_id);
        }
        Err(e) => sh.log.log(format!("size of {ws_id}: {e:#}")),
    }
}

fn refresh_sizes(sh: &Arc<Shared>) {
    let list: Vec<(String, PathBuf)> = {
        let st = sh.lock();
        st.records()
            .into_iter()
            .filter(|w| matches!(w.state, WorkspaceState::Held | WorkspaceState::Idle))
            .map(|w| (w.id, w.path))
            .collect()
    };
    if list.is_empty() {
        return;
    }
    sh.spawn_worker_once("sizes", move |sh| {
        for (id, root) in list {
            refresh_size(sh, &id, &root);
        }
    });
}

fn refresh_facts(sh: &Arc<Shared>) {
    sh.spawn_worker_once("facts", |sh| match (sh.parts.facts)() {
        Ok(f) => {
            let mut st = sh.lock();
            // The machine keeps the name it started with: its jobs, workspaces and the cluster
            // know it by that one, whatever the hostname turned into since.
            let node = st.node.clone();
            let cap = ram_capacity(&sh.config, &f);
            let m = st.local_mut();
            m.facts = Facts { node, ..f };
            m.ram_capacity_mib = cap;
        }
        Err(e) => sh.log.log(format!("facts: {e:#}")),
    });
}

/// Deletes finished jobs past the retention period with their directories, and old records of
/// removed workspaces whose files are gone (until then [`retry_removals`] goes by them). A job's
/// period counts from when this machine recorded its end, on the wall clock and, within the boot
/// that recorded it, on the boot clock too ([`crate::store::Store::prune_jobs`]).
fn retention(sh: &Arc<Shared>, at: Now) {
    let now = at.wall;
    let before = now.saturating_sub(sh.opts.retention_ms);
    let boot = crate::store::BootNow {
        id: sh.parts.clock.boot_id(),
        now_ms: at.boot,
        within_ms: sh.opts.retention_ms,
    };
    let removed: Vec<WorkspaceRecord> = {
        let st = sh.lock();
        st.workspaces
            .values()
            .filter(|w| w.state == WorkspaceState::Removed && w.last_used_at < before)
            .cloned()
            .collect()
    };
    // Looked at without the lock: only these may go below, unless a removal of one started
    // meanwhile.
    let gone: BTreeSet<String> = removed
        .iter()
        .filter(|w| !files_left(sh, w))
        .map(|w| w.id.clone())
        .collect();
    let pruned = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        st.evict_finished(now, KEEP_IN_MEMORY_MS);
        let jobs = match st.store.prune_jobs(before, &boot) {
            Ok(j) => j,
            Err(e) => {
                st.log.log(format!("store: pruning jobs: {e:#}"));
                Vec::new()
            }
        };
        for id in &jobs {
            st.jobs.remove(id);
        }
        let keep: BTreeSet<String> = st
            .workspaces
            .values()
            .filter(|w| !gone.contains(&w.id) || st.removing.contains(&w.id))
            .map(|w| w.id.clone())
            .collect();
        match st.store.prune_workspaces(before, &keep) {
            Ok(ids) => {
                for id in ids {
                    st.workspaces.remove(&id);
                }
            }
            Err(e) => st.log.log(format!("store: pruning workspaces: {e:#}")),
        }
        jobs
    };
    for id in pruned {
        let dir = sh.paths.job_dir(&id);
        if dir.exists()
            && let Err(e) = std::fs::remove_dir_all(&dir)
        {
            sh.log
                .log(format!("could not remove {}: {e}", dir.display()));
        }
    }
}

/// A queued job as a record, for tests and dispatch.
pub(crate) fn new_record(id: JobId, spec: crate::types::JobSpec, now: Millis) -> JobRecord {
    JobRecord {
        id,
        spec,
        state: JobState::Queued,
        submitted_at: now,
        started_at: None,
        ended_at: None,
        node: None,
        workspace: None,
        lease: None,
        handle: None,
        exit_code: None,
        signal: None,
        message: None,
        stuck_pids: Vec::new(),
        usage: Default::default(),
        passes: 0,
        stalled: false,
        kept: Vec::new(),
        notes: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn over_memory_messages() {
        assert_eq!(
            over_memory_message(4_509_715_660, 4096),
            "over memory: used 4.2 GiB of its 4 GiB"
        );
        assert_eq!(
            over_memory_message(1_200 << 20, 256),
            "over memory: used 1.2 GiB of its 256 MiB"
        );
        // A 64 MiB limit was "0.06 GiB".
        assert_eq!(
            over_memory_message(300 << 20, 64),
            "over memory: used 300 MiB of its 64 MiB"
        );
        assert_eq!(
            over_memory_message(17 << 30, 16 << 10),
            "over memory: used 17 GiB of its 16 GiB"
        );
        assert_eq!(
            over_memory_message(1 << 30, 512),
            "over memory: used 1 GiB of its 512 MiB"
        );
        assert_eq!(
            over_memory_message((64 << 20) - 4096, 64),
            "over memory: used 64 MiB of its 64 MiB"
        );
        assert_eq!(
            over_memory_message(0, 2048),
            "over memory: went over its 2 GiB"
        );
    }

    #[test]
    fn log_lines_say_the_state_once() {
        assert_eq!(after_state(JobState::Cancelled, Some("cancelled")), "");
        assert_eq!(
            after_state(
                JobState::OverMemory,
                Some("over memory: used 300 MiB of its 64 MiB")
            ),
            ": used 300 MiB of its 64 MiB"
        );
        assert_eq!(
            after_state(JobState::Failed, Some("exited with code 3")),
            ": exited with code 3"
        );
        assert_eq!(after_state(JobState::Succeeded, None), "");
    }

    #[test]
    fn oom_killed_messages() {
        assert_eq!(
            oom_killed_message(1_288_490_189, 4096),
            "killed because the machine ran short of memory, under its own limit: used 1.2 GiB of its 4 GiB"
        );
        assert_eq!(
            oom_killed_message(0, 2048),
            "killed because the machine ran short of memory, under its own limit: its limit was 2 GiB"
        );
    }

    #[test]
    fn status_endings() {
        let e = ending_from_status(&ExitStatus {
            code: Some(0),
            signal: None,
            ended_at: 5,
        });
        assert_eq!((e.state, e.at), (JobState::Succeeded, 5));
        let e = ending_from_status(&ExitStatus {
            code: Some(3),
            signal: None,
            ended_at: 5,
        });
        assert_eq!((e.state, e.exit_code), (JobState::Failed, Some(3)));
        let e = ending_from_status(&ExitStatus {
            code: None,
            signal: Some(libc::SIGSEGV),
            ended_at: 5,
        });
        assert_eq!(e.message.as_deref(), Some("killed by signal 11 (SIGSEGV)"));
    }

    #[test]
    fn capacity_leaves_headroom() {
        let facts = Facts {
            ram_mib: 64 * 1024,
            ..Default::default()
        };
        let mut c = NodeConfig::default();
        assert_eq!(ram_capacity(&c, &facts), 64 * 1024 - 64 * 1024 / 10);
        let small = Facts {
            ram_mib: 16 * 1024,
            ..Default::default()
        };
        assert_eq!(ram_capacity(&c, &small), 12 * 1024);
        c.ram_gib = Some(56.0);
        assert_eq!(ram_capacity(&c, &facts), 56 * 1024);
    }

    #[test]
    fn tokens_are_random_hex() {
        let (a, b) = (new_token(), new_token());
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    /// A macOS handle tracks every process seen: one of a job that keeps starting processes
    /// changes at nearly every sample, and can reach hundreds of KB.
    #[test]
    fn a_handle_that_changes_at_every_sample_is_written_with_the_usage() {
        let dir = tempfile::tempdir().unwrap();
        let mut st = State {
            node: "here".into(),
            machines: BTreeMap::new(),
            jobs: BTreeMap::new(),
            runs: BTreeMap::new(),
            workspaces: BTreeMap::new(),
            reservation: None,
            pending_keys: BTreeSet::new(),
            removing: BTreeSet::new(),
            extra_pending: BTreeMap::new(),
            unsaved: BTreeSet::new(),
            unsaved_kv: BTreeMap::new(),
            store: Store::open(&dir.path().join("potluck.db")).unwrap(),
            log: Logger::open(&dir.path().join("daemon.log")),
            clock: Arc::new(super::super::SystemClock),
        };
        let handle = |n: u32| Handle::Process {
            pid: 1,
            start_time: 1,
            tracked: (1..=n).map(|pid| (pid, 1)).collect(),
            gone: Vec::new(),
            lease: String::new(),
            tasks_max: 0,
            boot: String::new(),
        };
        let spec = crate::types::JobSpec {
            key: None,
            labels: BTreeMap::new(),
            priority: 3,
            request: Default::default(),
            on: BTreeMap::new(),
            repo: RepoRef {
                id: "repo-0123abcd".into(),
                source: PathBuf::from("/src/.git"),
                commit: "a".repeat(40),
                branch: None,
                submodules: vec![],
            },
            workspace: None,
            cmd: vec!["true".into()],
            env: BTreeMap::new(),
            timeout_ms: 60_000,
            hold_ms: None,
            keep: vec![],
            rerun_if_lost: false,
            kill_on_stall: false,
            recipe: None,
            submitted_from: "here".into(),
        };
        let mut job = new_record("here-1".into(), spec, 0);
        job.state = JobState::Running;
        job.lease = Some(Lease {
            grant: Default::default(),
            granted_at: 0,
            deadline: u64::MAX,
            token: String::new(),
        });
        job.handle = Some(handle(1));
        st.jobs.insert(job.id.clone(), job);
        let run = Run {
            phase: Phase::Running,
            ..Default::default()
        };
        st.runs.insert("here-1".into(), run);
        st.save_job("here-1");
        let opts = Options {
            potluck_bin: PathBuf::new(),
            tick_ms: 500,
            kill_grace_ms: 1_000,
            stall_ms: 0,
            retention_ms: 0,
            facts_refresh_ms: 0,
            idle_ms: 0,
        };
        // Ten seconds of samples, each with one more process.
        let mut writes = 0;
        for i in 1..=20u32 {
            let p = Probe {
                id: "here-1".into(),
                handle: handle(i + 1),
                mem_max: 0,
                status: dir.path().join("status.json"),
                log: dir.path().join("log"),
            };
            let s = Seen {
                sample: Ok(Sample {
                    pids: vec![1],
                    cpu_ms: 0,
                    mem_bytes: 0,
                    peak_mem_bytes: 0,
                    over_memory: false,
                    oom_killed: false,
                    over_tasks: None,
                }),
                status: None,
                log_len: None,
                log_mtime: None,
                full: None,
            };
            let t = 1_000 + u64::from(i) * 500;
            decide(
                &mut st,
                &opts,
                Now {
                    wall: t,
                    boot: t,
                    awake: t,
                },
                p,
                s,
                &mut Vec::new(),
            );
            let stored = st.store.job("here-1").unwrap().unwrap().handle;
            if stored == Some(handle(i + 1)) {
                writes += 1;
            }
        }
        assert!((1..=3).contains(&writes), "{writes} writes in 10 s");
    }
}
