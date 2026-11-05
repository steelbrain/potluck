//! The daemon's state: machines keyed by node name, the queue (every job not started yet, whichever
//! machine ends up running it), the invocations this machine runs and workspaces.
//! Everything lives behind one mutex and is written through to the store on every change.

use super::Logger;
use crate::proto::NodeView;
use crate::sched::{MachineView, Queued, Reservation, Running};
use crate::store::Store;
use crate::types::{
    Facts, JobId, JobRecord, JobState, Millis, NodeName, WorkspaceId, WorkspaceRecord,
    WorkspaceState,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct State {
    /// This machine.
    pub node: NodeName,
    /// This machine (other members' views live in the cluster's reports).
    pub machines: BTreeMap<NodeName, Machine>,
    /// Jobs not done yet, and jobs that ended recently (older ones are read from the store).
    pub jobs: BTreeMap<JobId, JobRecord>,
    /// The invocations this machine starts, runs or is ending.
    pub runs: BTreeMap<JobId, Run>,
    /// Workspaces, removed ones included until their records are pruned.
    pub workspaces: BTreeMap<WorkspaceId, WorkspaceRecord>,
    /// The first blocked job's reservation from the latest plan.
    pub reservation: Option<Reservation>,
    /// Keys of dispatches in progress, so a second dispatch with the same key waits for the first.
    pub pending_keys: BTreeSet<String>,
    /// Workspaces being removed right now.
    pub removing: BTreeSet<WorkspaceId>,
    /// Invocations queued on other machines that are pinned to workspaces here (cluster): they
    /// count in those workspaces' `pending`.
    pub extra_pending: BTreeMap<WorkspaceId, u32>,
    /// Records whose latest write to the store failed (a full disk, say): [`State::save_again`]
    /// writes them again until it works, and a job among them stays in memory meanwhile, so
    /// nothing reads the store's older copy.
    pub unsaved: BTreeSet<Unsaved>,
    /// For each [`Unsaved::Kv`], what its failed write would have written (None: a removal).
    pub unsaved_kv: BTreeMap<String, Option<String>>,
    pub store: Store,
    pub log: Logger,
    /// The daemon's clocks: a job's first save as done records when this machine saw its end.
    pub clock: std::sync::Arc<dyn super::Clock>,
}

/// A record whose write to the store failed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Unsaved {
    Job(JobId),
    Workspace(WorkspaceId),
    Run(JobId),
    /// A row of the daemon's own bookkeeping (the cluster's, a cleanup's): its value is in
    /// [`State::unsaved_kv`].
    Kv(String),
}

/// What a daemon holds that its store doesn't have: the records whose latest writes failed, as
/// the daemon holds them. A daemon started again in the same process after a restart in place
/// that didn't happen goes on from its store with these over it ([`super::runner::restore`]),
/// and writes them once it can.
#[derive(Default)]
pub(crate) struct Unwritten {
    pub jobs: Vec<JobRecord>,
    pub workspaces: Vec<WorkspaceRecord>,
    /// Run records as they would be stored (None: removed).
    pub runs: BTreeMap<JobId, Option<Run>>,
    /// Rows of the daemon's own bookkeeping (None: removed).
    pub kv: BTreeMap<String, Option<String>>,
}

impl std::fmt::Display for Unsaved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unsaved::Job(id) => write!(f, "job {id}"),
            Unsaved::Workspace(id) => write!(f, "workspace {id}"),
            Unsaved::Run(id) => write!(f, "run of {id}"),
            Unsaved::Kv(key) => f.write_str(key),
        }
    }
}

/// One machine as this daemon knows it.
pub(crate) struct Machine {
    pub facts: Facts,
    pub ram_capacity_mib: u64,
    pub last_seen: Millis,
}

/// An invocation on this machine, beside its job record: how far starting or ending it has got.
/// Stored (key `run/<job>`) so a restarted daemon continues a cancel or a kill where it was.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Run {
    pub phase: Phase,
    /// The workspace was created for this invocation (it is removed if seeding fails).
    pub new_workspace: bool,
    /// A cancel was asked for.
    pub cancel: bool,
    /// Adopted after a daemon restart, and not yet seen running by this daemon: processes gone
    /// without a status then went while no daemon watched (Lost, not Failed).
    pub adopted: bool,
    /// How it ends, once decided.
    pub ending: Option<Ending>,
    /// The boot whose clock `starting_at`, `deadline`, `term_at` and `kill_at` are read on
    /// ([`super::clock`]). None in a record from before the daemon kept boot-clock readings: its
    /// `term_at` and `kill_at` are wall times.
    #[serde(default)]
    pub boot: Option<String>,
    /// Just before the executor was asked to start it (boot clock): the run limit of a start cut
    /// short by a restart counts from here.
    #[serde(default)]
    pub starting_at: Option<u64>,
    /// When its run limit is reached (boot clock). The job's lease has the same in wall time, as
    /// `why` and the scheduler estimate it.
    #[serde(default)]
    pub deadline: Option<u64>,
    /// When potluck sent SIGTERM (boot clock).
    pub term_at: Option<u64>,
    /// When potluck sent SIGKILL (boot clock).
    pub kill_at: Option<u64>,
    /// The same on the awake clock, which the kill grace is measured on: it is the job's time
    /// to shut down, and a machine asleep runs none of its jobs. Not stored: a restarted daemon
    /// counts the time since, on the boot clock, as awake ([`Run::carry_awake`]).
    #[serde(skip)]
    pub term_awake: Option<u64>,
    #[serde(skip)]
    pub kill_awake: Option<u64>,
    /// Bytes the daemon itself appended to the log (stall stacks): not the job's output.
    #[serde(default)]
    pub daemon_log_bytes: u64,
    /// Its workspace was released while it ran (`potluck release`): its end doesn't hold the
    /// workspace again.
    #[serde(default)]
    pub released: bool,
    #[serde(skip)]
    pub sample_errors: u32,
    /// CPU totals sampled since the job last made progress (see [`CpuWindow`]). CPU totals live
    /// in the executor instance (after a restart they start again from the processes still alive,
    /// and a unit that has exited reports 0), so progress is measured between samples of this
    /// daemon, never against the stored total.
    #[serde(skip)]
    pub cpu: CpuWindow,
    /// The job's own output length at the latest sample.
    #[serde(skip)]
    pub last_output: Option<u64>,
    /// The log's modification time at the latest sample. Past the log's limit the log stops
    /// growing and the supervisor moves this instead.
    #[serde(skip)]
    pub log_mtime: Option<std::time::SystemTime>,
    /// The modification time the daemon's own last write (stall stacks) left on the log.
    #[serde(skip)]
    pub log_mtime_mine: Option<std::time::SystemTime>,
    /// When it last made progress (CPU or output), started or was adopted (awake clock: time
    /// asleep is no stall); stalls count from here. None after a restart for a job already
    /// stalled: no progress seen since.
    #[serde(skip)]
    pub active_at: Option<u64>,
    /// When the job record was last written for its usage alone (boot clock).
    #[serde(skip)]
    pub saved_at: u64,
    /// A stack sample is being taken.
    #[serde(skip)]
    pub stacks_busy: bool,
    /// Starting again after a restart cut its start short: a workspace seeded for it is seeded
    /// afresh.
    #[serde(skip)]
    pub resumed: bool,
    /// Its start was cut short by a restart after the executor was asked to start it, and the
    /// executor couldn't tell whether that start started it: it is asked again before anything
    /// is seeded or started.
    #[serde(skip)]
    pub find_again: bool,
}

impl Run {
    /// Potluck sends SIGTERM now.
    pub fn sent_term(&mut self, at: super::clock::Now) {
        self.term_at = Some(at.boot);
        self.term_awake = Some(at.awake);
    }

    /// Potluck sends SIGKILL now.
    pub fn sent_kill(&mut self, at: super::clock::Now) {
        self.kill_at = Some(at.boot);
        self.kill_awake = Some(at.awake);
    }

    /// Whether `grace` has passed, awake, since potluck sent SIGTERM.
    pub fn grace_after_term(&self, at: super::clock::Now, grace: u64) -> bool {
        self.term_awake
            .or(self.term_at.map(|t| awake_of(at, t)))
            .is_some_and(|t| at.awake >= t.saturating_add(grace))
    }

    /// Whether `grace` has passed, awake, since potluck sent SIGKILL.
    pub fn grace_after_kill(&self, at: super::clock::Now, grace: u64) -> bool {
        self.kill_awake
            .or(self.kill_at.map(|t| awake_of(at, t)))
            .is_some_and(|t| at.awake >= t.saturating_add(grace))
    }

    /// After a restart: the signals' boot-clock readings (this boot's) as awake-clock ones, the
    /// time since them counted as awake.
    pub fn carry_awake(&mut self, at: super::clock::Now) {
        self.term_awake = self.term_at.map(|t| awake_of(at, t));
        self.kill_awake = self.kill_at.map(|t| awake_of(at, t));
    }
}

/// Boot-clock reading `t` as an awake-clock one, by its distance from `at`.
fn awake_of(at: super::clock::Now, t: u64) -> u64 {
    at.awake.saturating_sub(at.boot.saturating_sub(t))
}

/// Whether a job is making progress on the CPU: using at least 1% of one core. Less, such as an
/// idle thread that wakes now and then to poll, counts as no CPU for stalls.
///
/// It keeps CPU totals sampled since the job last made progress, thinned out, and only as far
/// back as one stall time: progress is at least 1% of one core since the oldest of them, so over
/// the time since the last progress, or over the last stall time when that is shorter.
#[derive(Clone, Debug, Default)]
pub(crate) struct CpuWindow {
    /// (when by the awake clock, CPU total in ms), oldest first.
    points: std::collections::VecDeque<(u64, u64)>,
}

impl CpuWindow {
    /// Takes the sample `cpu_ms` taken at `now` (awake clock); true when the job made progress.
    /// `window_ms` is the stall time (0 when stall checks are off: then any CPU counts).
    pub fn progress(&mut self, now: u64, cpu_ms: u64, window_ms: u64) -> bool {
        let Some(&(_, last)) = self.points.back() else {
            self.points.push_back((now, cpu_ms));
            return false;
        };
        if cpu_ms < last {
            // The executor counts from a smaller total (processes it counted are gone): start over.
            self.points.clear();
            self.points.push_back((now, cpu_ms));
            return false;
        }
        if window_ms > 0 {
            // The newest point at or before the window's start is the oldest one needed.
            let start = now.saturating_sub(window_ms);
            while self.points.len() >= 2 && self.points[1].0 <= start {
                self.points.pop_front();
            }
        }
        let (since, base) = self.points[0];
        let used = cpu_ms - base;
        let progress = if window_ms == 0 {
            used > 0
        } else {
            used > 0 && used.saturating_mul(100) >= now.saturating_sub(since)
        };
        if progress {
            self.points.clear();
            self.points.push_back((now, cpu_ms));
        } else if window_ms > 0
            && self
                .points
                .back()
                .is_some_and(|(t, _)| now.saturating_sub(*t) >= (window_ms / 32).max(1))
        {
            // Without stall checks only the first point counts: none are kept after it.
            self.points.push_back((now, cpu_ms));
        }
        progress
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    /// Claimed: its workspace is being seeded or verified.
    #[default]
    Starting,
    Running,
    /// Over: keep files are being copied and the lease released.
    Finishing,
}

/// How an invocation ends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Ending {
    pub state: JobState,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub message: Option<String>,
    pub at: Millis,
}

/// What the scheduler gets for one machine, owned so views can borrow it.
pub(crate) struct MachineSnap {
    pub facts: Facts,
    /// Its pool, from its admission ([`crate::mesh::Mesh::pool_of`]); the default pool unless a
    /// cluster with users says otherwise.
    pub pool: String,
    pub ram_capacity_mib: u64,
    pub running: Vec<Running>,
}

impl MachineSnap {
    pub fn view(&self) -> MachineView<'_> {
        MachineView {
            facts: &self.facts,
            pool: &self.pool,
            ram_capacity_mib: self.ram_capacity_mib,
            running: &self.running,
        }
    }
}

/// Bookkeeping key prefix of run records: `run/<job>`.
pub(crate) const RUN_PREFIX: &str = "run/";

pub(crate) fn run_key(job: &str) -> String {
    format!("{RUN_PREFIX}{job}")
}

impl State {
    /// A job from memory, or from the store.
    pub fn job(&self, id: &str) -> Option<JobRecord> {
        if let Some(j) = self.jobs.get(id) {
            return Some(j.clone());
        }
        match self.store.job(id) {
            Ok(j) => j,
            Err(e) => {
                self.log.log(format!("store: reading job {id}: {e:#}"));
                None
            }
        }
    }

    /// The job dispatched with `key`, if any.
    pub fn job_by_key(&self, key: &str) -> Option<JobId> {
        if let Some(j) = self
            .jobs
            .values()
            .find(|j| j.spec.key.as_deref() == Some(key))
        {
            return Some(j.id.clone());
        }
        match self.store.job_by_key(key) {
            Ok(id) => id,
            Err(e) => {
                self.log.log(format!("store: reading key {key:?}: {e:#}"));
                None
            }
        }
    }

    pub fn save_job(&mut self, id: &str) {
        let r = match self.jobs.get(id) {
            Some(j) => {
                // Retention counts from when this machine recorded the end, on its own clocks.
                let ended = j.state.is_done().then(|| crate::store::Ended {
                    wall: self.clock.wall_ms(),
                    boot_ms: self.clock.boot_ms(),
                    boot_id: self.clock.boot_id(),
                });
                self.store.put_job_at(j, ended.as_ref())
            }
            None => Ok(()),
        };
        self.saved(Unsaved::Job(id.to_string()), r);
    }

    pub fn save_ws(&mut self, id: &str) {
        let r = match self.workspaces.get(id) {
            Some(w) => self.store.put_workspace(w),
            None => Ok(()),
        };
        self.saved(Unsaved::Workspace(id.to_string()), r);
    }

    pub fn save_run(&mut self, id: &str) {
        let r = match self.runs.get(id) {
            Some(run) => serde_json::to_string(run)
                .map_err(anyhow::Error::from)
                .and_then(|json| self.store.put_kv(&run_key(id), &json)),
            None => self.store.delete_kv(&run_key(id)),
        };
        self.saved(Unsaved::Run(id.to_string()), r);
    }

    /// Writes a row of the daemon's own bookkeeping (`kv`), or removes it (None). A failed write
    /// is made again later, like a record's.
    pub fn save_kv(&mut self, key: &str, value: Option<String>) {
        let r = match &value {
            Some(v) => self.store.put_kv(key, v),
            None => self.store.delete_kv(key),
        };
        if r.is_ok() {
            self.unsaved_kv.remove(key);
        } else {
            self.unsaved_kv.insert(key.to_string(), value);
        }
        self.saved(Unsaved::Kv(key.to_string()), r);
    }

    /// Notes how a write went: a failed one is made again later, and said once.
    fn saved(&mut self, what: Unsaved, r: anyhow::Result<()>) {
        match r {
            Ok(()) => {
                if self.unsaved.remove(&what) {
                    self.log.log(format!("store: wrote {what} at last"));
                }
            }
            Err(e) => {
                if self.unsaved.insert(what.clone()) {
                    self.log.log(format!(
                        "store: writing {what}: {e:#}; it is written again until that works"
                    ));
                }
            }
        }
    }

    /// Writes again the records whose last write failed, until one fails again (the store
    /// still doesn't work: the rest wait for the next time).
    pub fn save_again(&mut self) {
        let list: Vec<Unsaved> = self.unsaved.iter().cloned().collect();
        for what in list {
            match &what {
                Unsaved::Job(id) => self.save_job(id),
                Unsaved::Workspace(id) => self.save_ws(id),
                Unsaved::Run(id) => self.save_run(id),
                Unsaved::Kv(key) => match self.unsaved_kv.get(key).cloned() {
                    Some(value) => self.save_kv(key, value),
                    None => {
                        self.unsaved.remove(&what);
                    }
                },
            }
            if self.unsaved.contains(&what) {
                return;
            }
        }
    }

    /// The records whose latest writes failed ([`State::unsaved`]), as this daemon holds them.
    pub fn unwritten(&self) -> Unwritten {
        let mut u = Unwritten::default();
        for what in &self.unsaved {
            match what {
                Unsaved::Job(id) => u.jobs.extend(self.jobs.get(id).cloned()),
                Unsaved::Workspace(id) => u.workspaces.extend(self.workspaces.get(id).cloned()),
                Unsaved::Run(id) => {
                    // As the store would have it: what isn't stored isn't carried either.
                    let stored = self
                        .runs
                        .get(id)
                        .and_then(|r| serde_json::to_string(r).ok())
                        .and_then(|j| serde_json::from_str(&j).ok());
                    u.runs.insert(id.clone(), stored);
                }
                Unsaved::Kv(key) => {
                    if let Some(v) = self.unsaved_kv.get(key) {
                        u.kv.insert(key.clone(), v.clone());
                    }
                }
            }
        }
        u
    }

    /// Takes over what another daemon in this process couldn't write ([`State::unwritten`]),
    /// once this one's records were read from the store with them over it: they are written
    /// again from here, as any write that failed.
    pub fn take_unwritten(&mut self, jobs: Vec<JobId>, ws: Vec<WorkspaceId>, u: Unwritten) {
        self.unsaved.extend(jobs.into_iter().map(Unsaved::Job));
        self.unsaved.extend(ws.into_iter().map(Unsaved::Workspace));
        self.unsaved.extend(u.runs.into_keys().map(Unsaved::Run));
        self.unsaved.extend(u.kv.keys().cloned().map(Unsaved::Kv));
        self.unsaved_kv.extend(u.kv);
        if !self.unsaved.is_empty() {
            self.save_again();
        }
    }

    pub fn local(&self) -> &Machine {
        &self.machines[&self.node]
    }

    pub fn local_mut(&mut self) -> &mut Machine {
        self.machines
            .get_mut(&self.node)
            .expect("the local machine")
    }

    /// Jobs holding a lease on `node`, as the scheduler sees them.
    pub fn running_on(&self, node: &str) -> Vec<Running> {
        self.jobs
            .values()
            .filter(|j| !j.state.is_done() && j.node.as_deref() == Some(node))
            .filter_map(|j| {
                let lease = j.lease.as_ref()?;
                Some(Running {
                    job: j.id.clone(),
                    grant: lease.grant.clone(),
                    deadline: lease.deadline,
                })
            })
            .collect()
    }

    /// This machine as the scheduler sees it, in pool `pool` (its cluster's word for it:
    /// [`crate::daemon::cluster::Cluster::pool_here`]).
    pub fn local_snap(&self, pool: &str) -> Option<MachineSnap> {
        let (_, mut snap) = self.snaps().into_iter().find(|(n, _)| *n == self.node)?;
        snap.pool = pool.to_string();
        Some(snap)
    }

    /// Every machine as the scheduler sees it, by name.
    pub fn snaps(&self) -> Vec<(NodeName, MachineSnap)> {
        self.machines
            .iter()
            .map(|(name, m)| {
                (
                    name.clone(),
                    MachineSnap {
                        facts: m.facts.clone(),
                        // This machine's, in a cluster: [`State::local_snap`].
                        pool: crate::mesh::trust::DEFAULT_POOL.into(),
                        ram_capacity_mib: m.ram_capacity_mib,
                        running: self.running_on(name),
                    },
                )
            })
            .collect()
    }

    /// Whether an invocation is starting, running or ending in workspace `ws`.
    pub fn ws_busy(&self, ws: &str) -> bool {
        self.runs.keys().any(|id| {
            self.jobs
                .get(id)
                .is_some_and(|j| j.workspace.as_deref() == Some(ws))
        })
    }

    /// The queue: every job not started yet, as the scheduler sees it.
    pub fn queue(&self) -> Vec<Queued> {
        self.jobs
            .values()
            .filter(|j| j.state == JobState::Queued)
            .map(|j| self.queued(j))
            .collect()
    }

    pub fn queued(&self, j: &JobRecord) -> Queued {
        Queued {
            job: j.id.clone(),
            priority: j.spec.priority,
            passes: j.passes,
            submitted_at: j.submitted_at,
            request: j.spec.request.clone(),
            on: j.spec.on.clone(),
            timeout_ms: j.spec.timeout_ms,
            workspace: j.spec.workspace.as_ref().map(|w| {
                let node = self
                    .workspaces
                    .get(w)
                    .map(|r| r.node.clone())
                    .unwrap_or_else(|| super::cluster::peer::node_of_ws(w).to_string());
                (w.clone(), node, self.ws_busy(w))
            }),
            pools: j.spec.pools.clone(),
        }
    }

    /// Workspaces not removed.
    pub fn records(&self) -> Vec<WorkspaceRecord> {
        self.workspaces
            .values()
            .filter(|w| w.state != WorkspaceState::Removed)
            .cloned()
            .collect()
    }

    /// A workspace that exists (not removed).
    pub fn live_ws(&self, id: &str) -> Option<&WorkspaceRecord> {
        self.workspaces
            .get(id)
            .filter(|w| w.state != WorkspaceState::Removed)
    }

    /// Recounts each workspace's queued and running invocations and saves those that changed.
    pub fn refresh_pending(&mut self) {
        let mut counts: BTreeMap<String, u32> = self.extra_pending.clone();
        for j in self.jobs.values().filter(|j| !j.state.is_done()) {
            let ws = if j.state == JobState::Queued {
                j.spec.workspace.as_ref()
            } else {
                j.workspace.as_ref()
            };
            if let Some(ws) = ws {
                *counts.entry(ws.clone()).or_default() += 1;
            }
        }
        let changed: Vec<WorkspaceId> = self
            .workspaces
            .values()
            .filter(|w| w.pending != counts.get(w.id.as_str()).copied().unwrap_or(0))
            .map(|w| w.id.clone())
            .collect();
        for id in changed {
            let n = counts.get(id.as_str()).copied().unwrap_or(0);
            if let Some(w) = self.workspaces.get_mut(&id) {
                w.pending = n;
            }
            self.save_ws(&id);
        }
    }

    pub fn node_view(&self, name: &str, now: Millis) -> Option<NodeView> {
        let m = self.machines.get(name)?;
        let running = self.running_on(name);
        let mut gpus_busy: Vec<String> = running
            .iter()
            .flat_map(|r| r.grant.gpus.iter().cloned())
            .collect();
        gpus_busy.sort();
        gpus_busy.dedup();
        Some(NodeView {
            facts: m.facts.clone(),
            address: None,
            alive: true,
            updating: false,
            last_seen: Some(if name == self.node { now } else { m.last_seen }),
            ram_capacity_mib: m.ram_capacity_mib,
            ram_reserved_mib: running.iter().map(|r| r.grant.ram_mib).sum(),
            gpus_busy,
            exclusive_job: running
                .iter()
                .find(|r| r.grant.exclusive)
                .map(|r| r.job.clone()),
        })
    }

    /// How long job `id` has left before its run limit, on the boot clock that enforces the
    /// limit (its run's deadline); by its lease's wall-clock end only for a run without one.
    /// None when it holds no lease here.
    pub fn time_left(&self, id: &str, now: super::clock::Now) -> Option<u64> {
        let lease = self.jobs.get(id)?.lease.as_ref()?;
        Some(match self.runs.get(id).and_then(|r| r.deadline) {
            Some(d) => d.saturating_sub(now.boot),
            None => lease.deadline.saturating_sub(now.wall),
        })
    }

    /// Forgets finished jobs that ended more than `age_ms` ago (the store keeps them).
    pub fn evict_finished(&mut self, now: Millis, age_ms: u64) {
        let unsaved = &self.unsaved;
        self.jobs.retain(|id, j| {
            !j.state.is_done()
                || j.ended_at.is_none_or(|t| now.saturating_sub(t) < age_ms)
                || unsaved.contains(&Unsaved::Job(id.clone()))
        });
    }
}

#[cfg(test)]
mod tests {
    use super::CpuWindow;

    /// Samples every `step` ms for `for_ms`, the CPU total rising `per_step` ms each time; returns
    /// the times of progress.
    fn run(
        w: &mut CpuWindow,
        t: &mut u64,
        cpu: &mut u64,
        step: u64,
        per_step: u64,
        for_ms: u64,
    ) -> Vec<u64> {
        let mut out = Vec::new();
        let end = *t + for_ms;
        while *t < end {
            *t += step;
            *cpu += per_step;
            if w.progress(*t, *cpu, 600_000) {
                out.push(*t);
            }
        }
        out
    }

    #[test]
    fn under_one_percent_of_a_core_is_no_progress() {
        let (mut w, mut t, mut cpu) = (CpuWindow::default(), 1_000_000, 0);
        w.progress(t, cpu, 600_000);
        // A thread polling for 1 ms every 500 ms: 0.2%. Its first wake-ups right after a busy
        // spell can count; after that, never.
        let p = run(&mut w, &mut t, &mut cpu, 500, 1, 3_600_000);
        assert!(p.iter().all(|&x| x < 1_000_000 + 2_000), "{p:?}");
        // 0.9% stays under, for an hour.
        let (mut w, mut t) = (CpuWindow::default(), 0);
        w.progress(t, 0, 600_000);
        let mut slow = 0;
        let mut p = Vec::new();
        for _ in 0..7_200 {
            t += 500;
            slow += 9;
            if w.progress(t, slow / 2, 600_000) {
                p.push(t);
            }
        }
        assert!(p.is_empty(), "{p:?}");
        // 2% is progress, at every sample.
        let (mut w, mut t, mut cpu) = (CpuWindow::default(), 0, 0);
        w.progress(t, cpu, 600_000);
        assert_eq!(run(&mut w, &mut t, &mut cpu, 500, 10, 60_000).len(), 120);
    }

    #[test]
    fn after_a_long_quiet_a_busy_job_counts_within_the_window() {
        let (mut w, mut t, mut cpu) = (CpuWindow::default(), 0, 0);
        w.progress(t, cpu, 600_000);
        // Ten hours of nothing, then a core: 1% of the 10-minute window is 6 s of CPU.
        assert!(run(&mut w, &mut t, &mut cpu, 500, 0, 36_000_000).is_empty());
        let p = run(&mut w, &mut t, &mut cpu, 500, 500, 30_000);
        let first = p.first().map(|x| x - 36_000_000);
        assert!(first.is_some_and(|x| (6_000..=7_000).contains(&x)), "{p:?}");
        // Totals that drop (an executor counting afresh) start the window over.
        assert!(!w.progress(t + 500, 10, 600_000));
        assert!(w.progress(t + 1_000, 400, 600_000));
    }

    #[test]
    fn without_stall_checks_any_cpu_counts() {
        let mut w = CpuWindow::default();
        assert!(!w.progress(0, 0, 0));
        assert!(!w.progress(500, 0, 0));
        assert!(w.progress(100_000, 1, 0));
    }

    #[test]
    fn the_window_stays_small_for_a_job_that_sleeps_for_days() {
        for window_ms in [0, 600_000] {
            let mut w = CpuWindow::default();
            // A week of samples every half second, no CPU used.
            for i in 0..1_209_600u64 {
                assert!(!w.progress(i * 500, 7, window_ms));
            }
            assert!(
                w.points.len() <= 40,
                "{window_ms}: {} points",
                w.points.len()
            );
            assert!(w.progress(1_209_600 * 500, 8_000, window_ms));
        }
    }
}
