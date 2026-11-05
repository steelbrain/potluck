//! Which queued jobs a machine starts now: a pure function of the machine's view and the queue, so
//! it can be tested without processes. From phase 2 each machine runs it against the global queue
//! to decide what to claim.
//!
//! - **Admission.** RAM reservations must fit the machine's capacity. Each GPU is exclusive. CPU is a
//!   weight only.
//! - **Exclusive.** An exclusive job starts only on an empty machine, and takes every GPU; its RAM
//!   grant is the RAM it asked for. While it runs nothing else starts there.
//! - **All at once.** A job gets every resource it asks for in one grant, or none. Each GPU selector
//!   gets its own GPU; when several assignments work, the first in the machine's GPU order wins.
//! - **Feasibility.** Facts (`on`) must match, the RAM must fit the capacity, the selectors must fit
//!   the machine's GPUs, and a job pinned to a workspace runs only on that workspace's machine. A job
//!   this machine can never run is skipped: it is neither blocked here nor passed over.
//! - **Workspaces.** One invocation at a time runs in a workspace, and invocations dispatched into
//!   it run first come, first served, whatever their priorities: a later one doesn't start while
//!   an earlier one waits. A pinned job waiting for its own workspace (an invocation runs in it, or
//!   an earlier one goes first) holds no reservation and is not passed over.
//! - **Order.** By effective priority ([`crate::types::effective_priority`]), then submit time, then
//!   job id.
//! - **Aging.** A job is passed over when another job, submitted later, is granted something it
//!   waits for: a GPU one of its selectors matches; RAM, when it didn't fit in the RAM free just
//!   before; anything, when it is exclusive and waits for the machine to empty; or its place, when
//!   it could have had what it asks for just before that start and not after. Passed jobs could
//!   run on this machine and are still waiting; they are returned in [`Plan::passed`] (once per
//!   plan) and the caller adds one to their `passes`.
//! - **Backfill.** The first job in order that can't start gets a reservation: when its resources are
//!   expected free if every holder runs to its deadline. A later job may still start if it will end
//!   (by its run limit) before then, or doesn't touch what the reserved job needs: none of its GPUs,
//!   not its workspace, and only RAM that stays free after the reserved job takes its share at that
//!   time. An exclusive job needs the whole machine, so while it waits only jobs that end before it
//!   is expected to empty start there.
//! - **No preemption.** Nothing here stops a running job.

use crate::types::{Facts, Gpu, Grant, JobId, Millis, NodeName, Request, WorkspaceId};
use crate::types::{effective_priority, gpu_matches};
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod sim;
#[cfg(test)]
pub(crate) mod tests;

/// A job running on this machine.
#[derive(Clone, Debug)]
pub struct Running {
    pub job: JobId,
    pub grant: Grant,
    /// Its run-limit deadline.
    pub deadline: Millis,
}

/// A job in the queue.
#[derive(Clone, Debug)]
pub struct Queued {
    pub job: JobId,
    pub priority: u8,
    pub passes: u32,
    pub submitted_at: Millis,
    pub request: Request,
    pub on: BTreeMap<String, String>,
    pub timeout_ms: u64,
    /// Pinned to a workspace: (id, its machine, whether an invocation runs in it now).
    pub workspace: Option<(WorkspaceId, NodeName, bool)>,
}

/// One machine as the scheduler sees it.
#[derive(Clone, Debug)]
pub struct MachineView<'a> {
    pub facts: &'a Facts,
    pub ram_capacity_mib: u64,
    pub running: &'a [Running],
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    /// Jobs to start now, with what each is granted, in start order.
    pub start: Vec<(JobId, Grant)>,
    /// Waiting jobs these starts passed over: a later-submitted job was granted something each
    /// waits for (each gets one more pass).
    pub passed: Vec<JobId>,
    /// The first blocked job's reservation, if a job is blocked.
    pub reservation: Option<Reservation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reservation {
    pub job: JobId,
    /// When what it needs is expected free.
    pub at: Millis,
}

/// Whether this machine could ever run the job (facts, RAM capacity, GPUs that exist, pinning).
/// `Err` says why not, in plain words.
pub fn feasible(machine: &MachineView, job: &Queued) -> Result<(), String> {
    if let Some((ws, node, _)) = &job.workspace
        && *node != machine.facts.node
    {
        return Err(format!("pinned to workspace {ws} on {node}"));
    }
    if let Some(unmet) = machine.facts.unmet(&job.on) {
        return Err(unmet);
    }
    let r = &job.request;
    if r.ram_mib > machine.ram_capacity_mib {
        return Err(format!(
            "needs {} GiB RAM, its capacity is {} GiB",
            gib(r.ram_mib),
            gib(machine.ram_capacity_mib)
        ));
    }
    let gpus = &machine.facts.gpus;
    if let Some(sel) = r
        .gpus
        .iter()
        .find(|s| !gpus.iter().any(|g| gpu_matches(s, g)))
    {
        return Err(format!("no GPU there matches {sel}"));
    }
    let every: Vec<usize> = (0..gpus.len()).collect();
    if assign(&r.gpus, gpus, &every).is_none() {
        return Err(format!(
            "no {} separate GPUs there match {}",
            r.gpus.len(),
            r.gpus.join(", ")
        ));
    }
    Ok(())
}

/// Indices of `queue` in the order they would start.
pub fn order(queue: &[Queued]) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..queue.len()).collect();
    idx.sort_by(|&a, &b| {
        let (x, y) = (&queue[a], &queue[b]);
        effective_priority(y.priority, y.passes)
            .cmp(&effective_priority(x.priority, x.passes))
            .then(x.submitted_at.cmp(&y.submitted_at))
            .then_with(|| cmp_job_ids(&x.job, &y.job))
    });
    idx
}

/// Job ids compare by node, then by sequence number as a number (`studio-9` before `studio-10`).
fn cmp_job_ids(a: &str, b: &str) -> Ordering {
    fn split(id: &str) -> (&str, Option<u64>) {
        match id.rsplit_once('-') {
            Some((node, seq)) => (node, seq.parse().ok()),
            None => (id, None),
        }
    }
    let ((na, sa), (nb, sb)) = (split(a), split(b));
    na.cmp(nb).then(sa.cmp(&sb)).then(a.cmp(b))
}

/// Which queued jobs to start on this machine now, which waiting jobs that passes over, and the
/// first blocked job's reservation. See the module docs for the rules.
pub fn plan(now: Millis, machine: &MachineView, queue: &[Queued]) -> Plan {
    let mut load = Load::new(now, machine);
    let mut out = Plan::default();
    let mut guard: Option<Guard> = None;
    let heads = workspace_heads(machine, queue);
    // Jobs that could run here and wait for something another job may get (aging candidates).
    let mut waiting: Vec<usize> = Vec::new();
    // Each start: the job, its grant, and the machine's load just before it.
    let mut starts: Vec<(usize, Grant, Vec<Hold>)> = Vec::new();

    for i in order(queue) {
        let q = &queue[i];
        if feasible(machine, q).is_err() {
            continue;
        }
        if let Some((ws, _, busy)) = &q.workspace
            && (*busy || heads.get(ws.as_str()) != Some(&i))
        {
            // Waits for its own workspace: an invocation runs in it, or one dispatched into it
            // earlier goes first. No reservation, and no other start passes it over.
            continue;
        }
        let Some(grant) = load.grant(q) else {
            waiting.push(i);
            if guard.is_none() {
                let g = load.reserve(q);
                out.reservation = Some(Reservation {
                    job: q.job.clone(),
                    at: g.at,
                });
                guard = Some(g);
            }
            continue;
        };
        if let Some(g) = guard.as_mut()
            && !g.admits(now, q, &grant)
        {
            waiting.push(i);
            continue;
        }
        starts.push((i, grant.clone(), load.holds.clone()));
        load.add(&q.job, &grant, now.saturating_add(q.timeout_ms));
        out.start.push((q.job.clone(), grant));
    }

    out.passed = waiting
        .into_iter()
        .filter(|&w| {
            starts.iter().any(|(s, grant, before)| {
                queue[w].submitted_at < queue[*s].submitted_at
                    && passes_over(&load.at(before), &queue[*s], grant, &queue[w])
            })
        })
        .map(|w| queue[w].job.clone())
        .collect();
    out
}

/// For each workspace on this machine, the queued invocation into it that goes next: the one
/// dispatched first (then by job id) among those this machine can run. Invocations into one
/// workspace run first come, first served, whatever their priorities.
pub(crate) fn workspace_heads<'q>(
    machine: &MachineView,
    queue: &'q [Queued],
) -> BTreeMap<&'q str, usize> {
    let mut heads: BTreeMap<&str, usize> = BTreeMap::new();
    for (i, q) in queue.iter().enumerate() {
        let Some((ws, _, _)) = &q.workspace else {
            continue;
        };
        if feasible(machine, q).is_err() {
            continue;
        }
        let earlier = |h: usize| {
            let o = &queue[h];
            q.submitted_at
                .cmp(&o.submitted_at)
                .then_with(|| cmp_job_ids(&q.job, &o.job))
                .is_lt()
        };
        match heads.get(ws.as_str()) {
            Some(&h) if !earlier(h) => {}
            _ => {
                heads.insert(ws, i);
            }
        }
    }
    heads
}

/// Whether start `s`, granted `grant` on a machine loaded as `before`, passes over the waiting job
/// `w`: `s` was granted something `w` needs and lacks.
///
/// - a GPU one of `w`'s selectors matches;
/// - RAM, when `w` didn't fit in the RAM free before `s` started;
/// - anything, when `w` is exclusive (it waits for the machine to empty);
/// - its place: `w` could have been granted what it asks for just before `s`, and not after.
fn passes_over(before: &Load, s: &Queued, grant: &Grant, w: &Queued) -> bool {
    let want = &w.request;
    if want.exclusive {
        return true;
    }
    let gpus = &before.machine.facts.gpus;
    let gpu_wanted = grant.gpus.iter().any(|id| {
        gpus.iter()
            .find(|g| g.id == *id)
            .is_some_and(|g| want.gpus.iter().any(|sel| gpu_matches(sel, g)))
    });
    if gpu_wanted || (grant.ram_mib > 0 && want.ram_mib > before.ram_free()) {
        return true;
    }
    if before.grant(w).is_none() {
        return false;
    }
    let mut after = before.at(&before.holds);
    after.add(&s.job, grant, before.now.saturating_add(s.timeout_ms));
    after.grant(w).is_none()
}

/// A RAM amount in GiB, for [`feasible`]'s reasons: `16`, `0.5`, `1.3`.
pub(crate) fn gib(mib: u64) -> String {
    if mib.is_multiple_of(1024) {
        (mib / 1024).to_string()
    } else {
        format!("{:.1}", mib as f64 / 1024.0)
    }
}

/// Gives each selector its own GPU, trying `candidates` (indices into `gpus`) in the order given:
/// the first selector takes the first candidate that still leaves the rest an assignment, then the
/// second, and so on. `None` when no assignment satisfies every selector. Whether the rest still
/// have one is a bipartite matching ([`complete`]), so this takes polynomial time however many
/// GPUs a job asks for (trying every assignment took 10 s for 11 GPUs, and 11 times longer per
/// GPU after that).
pub(crate) fn assign(sels: &[String], gpus: &[Gpu], candidates: &[usize]) -> Option<Vec<usize>> {
    let mut cands: Vec<usize> = Vec::with_capacity(candidates.len());
    for &g in candidates {
        if !cands.contains(&g) {
            cands.push(g);
        }
    }
    if sels.len() > cands.len() {
        return None;
    }
    // fits[s][c]: selector s matches candidate c (a position in `cands`).
    let fits: Vec<Vec<bool>> = sels
        .iter()
        .map(|sel| cands.iter().map(|&g| gpu_matches(sel, &gpus[g])).collect())
        .collect();
    let mut taken = vec![false; cands.len()];
    let mut picked = Vec::with_capacity(sels.len());
    for s in 0..sels.len() {
        let mut chosen = None;
        for c in 0..cands.len() {
            if taken[c] || !fits[s][c] {
                continue;
            }
            taken[c] = true;
            if complete(&fits[s + 1..], &taken) {
                chosen = Some(c);
                break;
            }
            taken[c] = false;
        }
        picked.push(cands[chosen?]);
    }
    Some(picked)
}

/// Whether every row of `fits` (a selector) can have a column (a candidate GPU) of its own that it
/// fits and that isn't `taken`: a maximum bipartite matching by augmenting paths.
fn complete(fits: &[Vec<bool>], taken: &[bool]) -> bool {
    fn augment(
        row: usize,
        fits: &[Vec<bool>],
        taken: &[bool],
        owner: &mut [Option<usize>],
        seen: &mut [bool],
    ) -> bool {
        for c in 0..taken.len() {
            if taken[c] || seen[c] || !fits[row][c] {
                continue;
            }
            seen[c] = true;
            if owner[c].is_none_or(|o| augment(o, fits, taken, owner, seen)) {
                owner[c] = Some(row);
                return true;
            }
        }
        false
    }
    let mut owner = vec![None; taken.len()];
    (0..fits.len()).all(|row| {
        let mut seen = vec![false; taken.len()];
        augment(row, fits, taken, &mut owner, &mut seen)
    })
}

/// What one running (or just started) job holds, as planning counts it.
#[derive(Clone, Debug)]
pub(crate) struct Hold {
    pub job: JobId,
    pub ram_mib: u64,
    pub gpus: Vec<String>,
    pub exclusive: bool,
    /// When it is expected to let go: its run-limit deadline, or now once that has passed.
    pub end: Millis,
}

/// A machine's load while a plan is made: what runs there plus what the plan has started so far.
pub(crate) struct Load<'m, 'a> {
    pub machine: &'m MachineView<'a>,
    pub now: Millis,
    pub holds: Vec<Hold>,
}

impl<'m, 'a> Load<'m, 'a> {
    pub fn new(now: Millis, machine: &'m MachineView<'a>) -> Self {
        let mut load = Load {
            machine,
            now,
            holds: Vec::new(),
        };
        for r in machine.running {
            load.add(&r.job, &r.grant, r.deadline);
        }
        load
    }

    /// The same machine with these holds.
    pub fn at(&self, holds: &[Hold]) -> Load<'m, 'a> {
        Load {
            machine: self.machine,
            now: self.now,
            holds: holds.to_vec(),
        }
    }

    pub fn add(&mut self, job: &str, grant: &Grant, deadline: Millis) {
        self.holds.push(Hold {
            job: job.to_string(),
            ram_mib: grant.ram_mib,
            gpus: grant.gpus.clone(),
            exclusive: grant.exclusive,
            end: deadline.max(self.now),
        });
    }

    /// RAM not reserved by any holder now.
    pub fn ram_free(&self) -> u64 {
        let held = self
            .holds
            .iter()
            .fold(0u64, |a, h| a.saturating_add(h.ram_mib));
        self.machine.ram_capacity_mib.saturating_sub(held)
    }

    /// RAM expected free at `t` if every holder runs to its deadline.
    pub fn ram_free_at(&self, t: Millis) -> u64 {
        let held = self
            .holds
            .iter()
            .filter(|h| h.end > t)
            .fold(0u64, |a, h| a.saturating_add(h.ram_mib));
        self.machine.ram_capacity_mib.saturating_sub(held)
    }

    /// The earliest time `need` MiB are expected free: holders let go in deadline order.
    pub fn ram_time(&self, need: u64) -> Millis {
        let mut ends: Vec<Millis> = self.holds.iter().map(|h| h.end).collect();
        ends.push(self.now);
        ends.sort_unstable();
        ends.dedup();
        let last = *ends.last().unwrap_or(&self.now);
        ends.into_iter()
            .find(|&t| self.ram_free_at(t) >= need)
            .unwrap_or(last)
    }

    /// The holder of a GPU, if a potluck job holds it.
    pub fn gpu_holder(&self, gpu: &str) -> Option<&Hold> {
        self.holds.iter().find(|h| h.gpus.iter().any(|g| g == gpu))
    }

    /// When a GPU is expected free: now, or its holder's deadline.
    pub fn gpu_free_at(&self, gpu: &str) -> Millis {
        self.holds
            .iter()
            .filter(|h| h.gpus.iter().any(|g| g == gpu))
            .map(|h| h.end)
            .max()
            .unwrap_or(self.now)
    }

    /// When a running exclusive job is expected to end, if one runs.
    pub fn exclusive_end(&self) -> Option<Millis> {
        self.holds
            .iter()
            .filter(|h| h.exclusive)
            .map(|h| h.end)
            .max()
    }

    /// What `q` would be granted now, or `None` if it can't start now. `q` must be feasible here.
    pub fn grant(&self, q: &Queued) -> Option<Grant> {
        let r = &q.request;
        let gpus = &self.machine.facts.gpus;
        if self.holds.iter().any(|h| h.exclusive) || r.ram_mib > self.ram_free() {
            return None;
        }
        if r.exclusive {
            if !self.holds.is_empty() {
                return None;
            }
            return Some(Grant {
                ram_mib: r.ram_mib,
                cpu: r.cpu,
                gpus: gpus.iter().map(|g| g.id.clone()).collect(),
                exclusive: true,
            });
        }
        let free: Vec<usize> = (0..gpus.len())
            .filter(|&i| self.gpu_holder(&gpus[i].id).is_none())
            .collect();
        let picked = assign(&r.gpus, gpus, &free)?;
        Some(Grant {
            ram_mib: r.ram_mib,
            cpu: r.cpu,
            gpus: picked.iter().map(|&i| gpus[i].id.clone()).collect(),
            exclusive: false,
        })
    }

    /// The GPUs (indices) a job with these selectors would wait for, and when they are all
    /// expected free, not before `base`. Among GPUs free by that time, those freed latest are
    /// picked, so GPUs free sooner stay open to jobs that backfill.
    pub fn gpu_wait(&self, sels: &[String], base: Millis) -> (Millis, Vec<usize>) {
        let gpus = &self.machine.facts.gpus;
        let free_at: Vec<Millis> = gpus.iter().map(|g| self.gpu_free_at(&g.id)).collect();
        let mut times: Vec<Millis> = free_at.iter().map(|&t| t.max(base)).collect();
        times.push(base);
        times.sort_unstable();
        times.dedup();
        for &t in &times {
            let mut cand: Vec<usize> = (0..gpus.len()).filter(|&i| free_at[i] <= t).collect();
            cand.sort_by_key(|&i| (Reverse(free_at[i]), i));
            if let Some(picked) = assign(sels, gpus, &cand) {
                return (t, picked);
            }
        }
        // At the last time every GPU is a candidate, so only selectors that fit none of the
        // machine's GPUs (a job it can never run) get here.
        (times.last().copied().unwrap_or(base), Vec::new())
    }

    /// The reservation for `q`, first in order and unable to start: when what it needs is expected
    /// free, and what later jobs must leave alone.
    fn reserve(&self, q: &Queued) -> Guard {
        let r = &q.request;
        let gpus = &self.machine.facts.gpus;
        if r.exclusive {
            let empty = self.holds.iter().map(|h| h.end).max().unwrap_or(self.now);
            return Guard {
                at: empty,
                gpus: gpus.iter().map(|g| g.id.clone()).collect(),
                exclusive: true,
                spare_mib: 0,
                workspace: workspace(q),
            };
        }
        let base = self
            .ram_time(r.ram_mib)
            .max(self.exclusive_end().unwrap_or(self.now));
        let (at, picked) = self.gpu_wait(&r.gpus, base);
        // Free RAM only grows as holders end, so what is spare at the earliest possible start
        // stays spare whenever the reserved job really starts.
        Guard {
            at,
            gpus: picked.iter().map(|&i| gpus[i].id.clone()).collect(),
            exclusive: false,
            spare_mib: self.ram_free_at(at).saturating_sub(r.ram_mib),
            workspace: workspace(q),
        }
    }
}

/// The workspace a job is pinned to, if any.
fn workspace(q: &Queued) -> Option<WorkspaceId> {
    q.workspace.as_ref().map(|(ws, _, _)| ws.clone())
}

/// What the reserved job needs protected from jobs that start after it in order.
struct Guard {
    at: Millis,
    gpus: BTreeSet<String>,
    exclusive: bool,
    /// RAM still free at the reservation time after the reserved job takes its share, less what
    /// jobs started beside it will still hold then.
    spare_mib: u64,
    /// The workspace it is pinned to: one invocation at a time runs there.
    workspace: Option<WorkspaceId>,
}

impl Guard {
    /// Whether `q`, which could start now with `grant`, may start without delaying the reserved
    /// job. A job admitted for the RAM it leaves alone uses up that spare RAM.
    fn admits(&mut self, now: Millis, q: &Queued, grant: &Grant) -> bool {
        if now.saturating_add(q.timeout_ms) <= self.at {
            return true;
        }
        let untouched = !self.exclusive
            && !grant.exclusive
            && grant.gpus.iter().all(|g| !self.gpus.contains(g))
            && grant.ram_mib <= self.spare_mib
            && (self.workspace.is_none() || workspace(q) != self.workspace);
        if untouched {
            self.spare_mib -= grant.ram_mib;
        }
        untouched
    }
}
