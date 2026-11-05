//! `potluck why JOB`: what a job waits on and who holds it, in plain words.
//!
//! For a queued job, each blocker with its holder: "waits on gpu 5080 on msi, held by job studio-41
//! for 14m of its 15m run limit, under 1% CPU for 9m"; "waits on RAM: needs 32 GiB, 20 GiB of 56 GiB
//! free on msi"; "no machine has gpu=amd"; "waits for its workspace msi-w3 on msi (an invocation runs
//! in it)"; "msi can't take it yet: its mirror of proj lacks commit 0123abcd yet: syncing from
//! studio". For a running job: its time against its run
//! limit, its CPU and last output, and whether it is stalled. For a finished one: how it ended,
//! the most memory it used beside what it reserved, and any stuck pids.
//!
//! Times come from the lease records and the daemon's own samples, never from `ps`.
//!
//! A queued job is explained machine by machine: why a machine can never run it, or else what it
//! lacks there now and who holds that, and which job holds that machine's reservation. Each
//! machine's reservation comes from running [`crate::sched::plan`] on the snapshot, the same pure
//! function the daemon runs. A machine that could run it but whose mirrors lack its commits yet
//! shows as a `sync` blocker, one that leaves it a few seconds to a machine with a workspace of its
//! repository as a `locality` blocker, and one that waits while it or the job's owner, back from
//! being away, compares membership records as a `catch_up` blocker ([`explain_waits`]).

use crate::cli::{fmt_bytes, fmt_mib};
use crate::proto::{Blocker, WhyView};
use crate::sched::{self, Hold, HoldsLine, Load, MachineView, Queued, Reservation, Takes};
use crate::types::{
    JobId, JobRecord, JobState, Millis, NodeName, SECOND_MS, effective_priority, fmt_ms, fmt_on,
    fmt_on_all, gpu_matches,
};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests;

pub struct Snapshot<'a> {
    pub now: Millis,
    /// Every machine the caller knows about.
    pub machines: &'a [MachineView<'a>],
    /// Queued, starting and running jobs, and the job asked about.
    pub jobs: &'a [JobRecord],
    pub queue: &'a [Queued],
    /// The caller's last reservation. `explain` doesn't need it: it plans each machine again from
    /// the snapshot, which gives each machine's reservation as of `now`.
    pub reservation: Option<&'a Reservation>,
}

pub fn explain(job: &str, snap: &Snapshot) -> WhyView {
    explain_synced(job, snap, &BTreeMap::new())
}

/// What machines in a cluster wait for before they take a queued job they could otherwise run.
pub struct Waits<'a> {
    /// Machines whose mirrors lack the job's commits yet, with why in plain words ("its mirror of
    /// proj lacks commit 0123abcd yet: syncing from studio"). Each shows as a `sync` blocker.
    pub unsynced: &'a BTreeMap<NodeName, String>,
    /// Machines that could start it now but leave it a few seconds to a member with a workspace
    /// of its repository, with whom and for how long ("msi, which has a workspace of its
    /// repository, for up to 2s more (locality)"). Each shows as a `locality` blocker.
    pub locality: &'a BTreeMap<NodeName, String>,
    /// Machines that could start it now but wait while they, or the job's owner, back from being
    /// away, compare membership records with the other members, with what follows "could start it
    /// now, but" ("is back from being away and compares membership records with the other members
    /// first"). Each shows as a `catch_up` blocker.
    pub catch_up: &'a BTreeMap<NodeName, String>,
    /// The queued jobs each machine leaves out of its plan (it can't take them yet: its mirrors
    /// lack their commits, it leaves them to a warm machine, or it is catching up with their
    /// owner). Each machine's plan is made without them, as the machine makes it.
    pub held_out: &'a BTreeMap<NodeName, BTreeSet<JobId>>,
    /// The one machine that may run the job, when only one may (no user signed it: it was
    /// queued before the cluster had users, and runs only where it was dispatched). The job is
    /// explained on that machine alone.
    pub only: Option<&'a str>,
}

/// [`explain`], knowing which machines can't take a queued job yet because their mirrors lack its
/// commits (see [`Waits::unsynced`]).
pub fn explain_synced(
    job: &str,
    snap: &Snapshot,
    unsynced: &BTreeMap<NodeName, String>,
) -> WhyView {
    explain_waits(
        job,
        snap,
        &Waits {
            unsynced,
            locality: &BTreeMap::new(),
            catch_up: &BTreeMap::new(),
            held_out: &BTreeMap::new(),
            only: None,
        },
    )
}

/// [`explain`], knowing what each machine waits for before it takes the job ([`Waits`]).
pub fn explain_waits(job: &str, snap: &Snapshot, waits: &Waits) -> WhyView {
    let mut why = Why {
        snap,
        unsynced: waits.unsynced,
        locality: waits.locality,
        catch_up: waits.catch_up,
        held_out: waits.held_out,
        only: waits.only,
        lines: Vec::new(),
        blockers: Vec::new(),
    };
    let rec = snap.jobs.iter().find(|j| j.id == job);
    let queued = snap.queue.iter().find(|q| q.job == job);
    let state = match (rec, queued) {
        (Some(r), _) if r.state == JobState::Starting => {
            why.starting(r);
            r.state
        }
        (Some(r), _) if r.state == JobState::Running => {
            why.running(r);
            r.state
        }
        (Some(r), _) if r.state.is_done() => {
            why.finished(r);
            r.state
        }
        (_, Some(q)) => {
            why.queued(q);
            JobState::Queued
        }
        (Some(r), None) => {
            why.queued(&queued_from(r, snap.jobs));
            JobState::Queued
        }
        (None, None) => {
            return WhyView {
                job: job.to_string(),
                state: "unknown".into(),
                lines: vec![format!("no job {job}")],
                blockers: Vec::new(),
            };
        }
    };
    // What its sources went through (a mirror that had diverged or was recloned), loudly.
    let mut lines = why.lines;
    if let Some(r) = rec {
        lines.extend(r.notes.iter().map(|n| format!("warning: {n}")));
    }
    WhyView {
        job: job.to_string(),
        state: state.as_str().into(),
        lines,
        blockers: why.blockers,
    }
}

/// A queued job the caller has a record for but didn't put in the queue: rebuilt from its spec.
/// Its workspace's machine is the runner node in the workspace id (`msi-w3`), and it is busy when
/// a starting or running job is in it.
fn queued_from(r: &JobRecord, jobs: &[JobRecord]) -> Queued {
    let workspace = r.spec.workspace.as_ref().map(|ws| {
        let node = ws.rsplit_once("-w").map_or(ws.as_str(), |(n, _)| n);
        (
            ws.clone(),
            node.to_string(),
            in_workspace(jobs, ws, &r.id).is_some(),
        )
    });
    Queued {
        job: r.id.clone(),
        priority: r.spec.priority,
        passes: r.passes,
        submitted_at: r.submitted_at,
        request: r.spec.request.clone(),
        on: r.spec.on.clone(),
        timeout_ms: r.spec.timeout_ms,
        workspace,
        pools: r.spec.pools.clone(),
        hold_line: r.spec.hold_line,
    }
}

/// The starting or running job in workspace `ws`, other than `except`.
fn in_workspace<'j>(jobs: &'j [JobRecord], ws: &str, except: &str) -> Option<&'j JobRecord> {
    jobs.iter().find(|j| {
        j.id != except
            && j.workspace.as_deref() == Some(ws)
            && matches!(j.state, JobState::Starting | JobState::Running)
    })
}

/// A job holding something another job waits for.
struct Holder {
    job: JobId,
    /// "for 14m of its 15m run limit, under 1% CPU for 9m", or for a job the plan starts now,
    /// "starting now, ahead of it in line, with a 15m run limit".
    clause: String,
    held_for_ms: Option<u64>,
    limit_ms: Option<u64>,
    idle_ms: Option<u64>,
    free_by: Option<Millis>,
}

/// A job a machine's plan starts now: its deadline, and whether it is ahead of the job asked
/// about in line.
type Pending = BTreeMap<JobId, (Millis, bool)>;

struct Why<'s, 'a> {
    snap: &'s Snapshot<'a>,
    unsynced: &'s BTreeMap<NodeName, String>,
    locality: &'s BTreeMap<NodeName, String>,
    catch_up: &'s BTreeMap<NodeName, String>,
    held_out: &'s BTreeMap<NodeName, BTreeSet<JobId>>,
    only: Option<&'s str>,
    lines: Vec<String>,
    blockers: Vec<Blocker>,
}

impl Why<'_, '_> {
    fn line(&mut self, s: impl Into<String>) {
        self.lines.push(s.into());
    }

    fn block(&mut self, resource: impl Into<String>, node: Option<&str>, holder: Option<&Holder>) {
        self.blockers.push(Blocker {
            resource: resource.into(),
            node: node.map(str::to_string),
            holder: holder.map(|h| h.job.clone()),
            held_for_ms: holder.and_then(|h| h.held_for_ms),
            limit_ms: holder.and_then(|h| h.limit_ms),
            holder_idle_ms: holder.and_then(|h| h.idle_ms),
            free_by: holder.and_then(|h| h.free_by),
        });
    }

    fn record(&self, job: &str) -> Option<&JobRecord> {
        self.snap.jobs.iter().find(|j| j.id == job)
    }

    /// How long a running job has held its lease, against its run limit, and its CPU use.
    fn running_holder(&self, job: &str, deadline: Millis) -> Holder {
        let now = self.snap.now;
        let rec = self.record(job);
        // From its start (the run limit counts from there; the deadline is moved then), or from
        // its claim while it is still starting.
        let since = rec.and_then(|r| r.started_at.or(r.lease.as_ref().map(|l| l.granted_at)));
        let limit = match since {
            Some(g) if deadline > g => Some(deadline - g),
            _ => rec.map(|r| r.spec.timeout_ms),
        };
        let held = since.map(|g| now.saturating_sub(g));
        let mut clause = match (held, limit) {
            (Some(h), Some(l)) if h > l => {
                format!("for {}, past its {} run limit", fmt_ms(h), fmt_ms(l))
            }
            (Some(h), Some(l)) => format!("for {} of its {} run limit", fmt_ms(h), fmt_ms(l)),
            _ => format!(
                "until its run limit in {}",
                fmt_ms(deadline.saturating_sub(now))
            ),
        };
        let idle = rec.and_then(|r| cpu_idle(r, now));
        if let Some(cpu) = rec.and_then(|r| cpu_words(r, now)) {
            clause += &format!(", {cpu}");
        }
        if rec.is_some_and(|r| r.stalled) {
            clause += ", stalled";
        }
        Holder {
            job: job.to_string(),
            clause,
            held_for_ms: held,
            limit_ms: limit,
            idle_ms: idle,
            free_by: Some(deadline),
        }
    }

    /// A holder on machine `m`: running there, or started there now by the plan.
    fn holder(&self, m: &MachineView, pending: &Pending, job: &str) -> Holder {
        let now = self.snap.now;
        if let Some(&(deadline, ahead)) = pending.get(job) {
            let limit = deadline.saturating_sub(now);
            let ahead = if ahead { ", ahead of it in line," } else { "" };
            return Holder {
                job: job.to_string(),
                clause: format!("starting now{ahead} with a {} run limit", fmt_ms(limit)),
                held_for_ms: Some(0),
                limit_ms: Some(limit),
                idle_ms: None,
                free_by: Some(deadline),
            };
        }
        let deadline = m
            .running
            .iter()
            .find(|r| r.job == job)
            .map_or(now, |r| r.deadline);
        self.running_holder(job, deadline)
    }

    fn queued(&mut self, q: &Queued) {
        let snap = self.snap;
        let now = snap.now;
        let eff = effective_priority(q.priority, q.passes);
        let waited = fmt_ms(now.saturating_sub(q.submitted_at));
        if eff == q.priority {
            self.line(format!("queued for {waited} at priority {eff}"));
        } else {
            let times = match q.passes {
                1 => "once".to_string(),
                n => format!("{n} times"),
            };
            self.line(format!(
                "queued for {waited} at priority {eff} (dispatched at {}, passed over {times})",
                q.priority
            ));
        }
        self.line(asks(q));

        let mut queue = snap.queue.to_vec();
        if !queue.iter().any(|o| o.job == q.job) {
            queue.push(q.clone());
        }
        let ahead = sched::order(&queue)
            .iter()
            .position(|&i| queue[i].job == q.job)
            .unwrap_or(0);
        match ahead {
            0 => {}
            1 => self.line("1 job is ahead of it in the queue"),
            n => self.line(format!("{n} jobs are ahead of it in the queue")),
        }

        let machines: Vec<&MachineView> = match (self.only, &q.workspace) {
            (Some(only), _) => snap
                .machines
                .iter()
                .filter(|m| m.facts.node == only)
                .collect(),
            (None, Some((_, node, _))) => snap
                .machines
                .iter()
                .filter(|m| m.facts.node == *node)
                .collect(),
            (None, None) => snap.machines.iter().collect(),
        };
        if machines.is_empty() {
            match &q.workspace {
                Some((ws, node, _)) => {
                    self.line(format!(
                        "its workspace {ws} is on {node}, which is not among the machines up"
                    ));
                    self.block("workspace", Some(node), None);
                }
                None => self.line("no machine is up to run it"),
            }
            return;
        }
        let mut cannot = Vec::new();
        let mut behind = 0;
        for m in &machines {
            match sched::feasible(m, q) {
                Ok(()) => match self.unsynced.get(&m.facts.node) {
                    Some(why) => {
                        behind += 1;
                        self.line(format!("{} can't take it yet: {why}", m.facts.node));
                        self.block("sync", Some(&m.facts.node), None);
                    }
                    None => {
                        // The machine's own plan: without the jobs it can't take yet.
                        let mine: Vec<Queued> = match self.held_out.get(&m.facts.node) {
                            Some(out) => queue
                                .iter()
                                .filter(|o| o.job == q.job || !out.contains(&o.job))
                                .cloned()
                                .collect(),
                            None => queue.clone(),
                        };
                        self.on_machine(m, q, &mine)
                    }
                },
                Err(reason) => cannot.push((m.facts.node.as_str(), reason)),
            }
        }
        if behind > 0 && behind + cannot.len() == machines.len() {
            self.line("no machine that could run it has its commits yet (the sync is behind)");
        }
        if cannot.len() == machines.len() && q.workspace.is_none() && self.only.is_none() {
            self.nowhere(q, &machines);
        }
        for (node, reason) in cannot {
            self.line(format!("{node} can't run it: {reason}"));
            self.block("facts", Some(node), None);
        }
    }

    /// What no machine has, when no machine can run the job.
    fn nowhere(&mut self, q: &Queued, machines: &[&MachineView]) {
        for (k, v) in &q.on {
            if !machines.iter().any(|m| m.facts.matches(k, v)) {
                self.line(format!("no machine has {}", fmt_on(k, v)));
            }
        }
        if machines
            .iter()
            .all(|m| q.request.ram_mib > m.ram_capacity_mib)
        {
            self.line(format!(
                "no machine has {} RAM to reserve",
                fmt_mib(q.request.ram_mib)
            ));
        }
        for sel in &q.request.gpus {
            if !machines
                .iter()
                .any(|m| m.facts.gpus.iter().any(|g| gpu_matches(sel, g)))
            {
                self.line(format!("no machine has a GPU matching {sel}"));
            }
        }
    }

    /// Why a job machine `m` could run doesn't start there now.
    fn on_machine(&mut self, m: &MachineView, q: &Queued, queue: &[Queued]) {
        let now = self.snap.now;
        let node = m.facts.node.as_str();
        if let Some((ws, _, true)) = &q.workspace {
            match in_workspace(self.snap.jobs, ws, &q.job) {
                Some(r) => {
                    let deadline = r.lease.as_ref().map_or(now, |l| l.deadline);
                    let h = self.running_holder(&r.id, deadline);
                    self.line(format!(
                        "waits for its workspace {ws} on {node}: job {} runs in it {}",
                        h.job, h.clause
                    ));
                    self.block("workspace", Some(node), Some(&h));
                }
                None => {
                    self.line(format!(
                        "waits for its workspace {ws} on {node} (an invocation runs in it)"
                    ));
                    self.block("workspace", Some(node), None);
                }
            }
            return;
        }

        let plan = sched::plan(now, m, queue);
        if plan.start.iter().any(|(j, _)| *j == q.job) {
            if let Some(wait) = self.catch_up.get(node) {
                self.line(format!("{node} could start it now, but {wait}"));
                self.block("catch_up", Some(node), None);
                return;
            }
            match self.locality.get(node) {
                Some(warm) => {
                    self.line(format!(
                        "{node} could start it now, but leaves it to {warm}"
                    ));
                    self.block("locality", Some(node), None);
                }
                None => self.line(format!(
                    "can start now on {node}; the scheduler takes it on its next pass"
                )),
            }
            return;
        }
        let rank: BTreeMap<&str, usize> = sched::order(queue)
            .into_iter()
            .enumerate()
            .map(|(pos, i)| (queue[i].job.as_str(), pos))
            .collect();
        let mine = rank.get(q.job.as_str()).copied().unwrap_or(usize::MAX);
        let mut load = Load::new(now, m);
        let mut pending = Pending::new();
        for (j, grant) in &plan.start {
            let timeout = queue
                .iter()
                .find(|o| o.job == *j)
                .map_or(0, |o| o.timeout_ms);
            let deadline = now.saturating_add(timeout);
            load.add(j, grant, deadline);
            pending.insert(j.clone(), (deadline, rank.get(j.as_str()) < Some(&mine)));
        }

        if let Some((ws, _, false)) = &q.workspace
            && let Some(&head) = sched::workspace_heads(m, queue).get(ws.as_str())
            && queue[head].job != q.job
        {
            let j = &queue[head].job;
            if pending.contains_key(j) {
                let h = self.holder(m, &pending, j);
                self.line(format!(
                    "waits for its workspace {ws} on {node}: job {j} runs in it first, {}",
                    h.clause
                ));
                self.block("workspace", Some(node), Some(&h));
            } else {
                self.line(format!(
                    "waits for its workspace {ws} on {node}: job {j}, dispatched into it \
                     earlier, goes first"
                ));
                let h = Holder {
                    job: j.clone(),
                    clause: String::new(),
                    held_for_ms: None,
                    limit_ms: None,
                    idle_ms: None,
                    free_by: None,
                };
                self.block("workspace", Some(node), Some(&h));
            }
            return;
        }

        let short = self.lacks(m, q, &load, &pending);
        let first = plan.reservation.as_ref().map(|r| r.job.as_str());
        let when = |at: Millis| match at {
            t if t > now => format!(
                "expected to start within {} (when the jobs holding what it needs reach their run \
                 limits)",
                fmt_ms(t - now)
            ),
            _ => "expected to start once jobs past their run limits are stopped".to_string(),
        };
        let order_block = |job: &str, at: Millis| Holder {
            job: job.to_string(),
            clause: String::new(),
            held_for_ms: None,
            limit_ms: None,
            idle_ms: None,
            free_by: Some(at),
        };
        // Behind a job dispatched with --hold-line: nothing after it in order starts here.
        if let Some(l) = plan.lines.last().filter(|l| {
            l.why == HoldsLine::Asked
                && rank
                    .get(l.reservation.job.as_str())
                    .is_some_and(|&p| p < mine)
        }) {
            let r = &l.reservation;
            // When it is the first in line, whatever else waits there goes after it.
            let expected = if first == Some(r.job.as_str()) {
                format!(
                    " and is expected to start within {}",
                    fmt_ms(r.at.saturating_sub(now))
                )
            } else {
                String::new()
            };
            self.line(format!(
                "waits behind job {}, which holds the line on {node} (dispatched with \
                 --hold-line){expected}",
                r.job
            ));
            if !short {
                self.line(format!(
                    "it fits on {node} now, but nothing behind job {} starts there until it does",
                    r.job
                ));
            }
            self.block("order", Some(node), Some(&order_block(&r.job, r.at)));
            return;
        }
        // Kept back by a job queued for 30 minutes, whose needs it would take.
        if let Some(h) = plan.held.iter().find(|h| h.job == q.job) {
            let what = match &h.takes {
                Takes::Gpu(id) => format!(
                    "waits for the GPU this job would take (gpu {})",
                    gpu_label(m, id)
                ),
                Takes::Ram => "waits for the RAM this job would take".to_string(),
                Takes::Machine => format!("waits for {node} to empty (exclusive)"),
                Takes::Place => "would no longer fit there if this job started".to_string(),
            };
            self.line(format!(
                "waits behind job {} on {node}, which has been queued for {} or more and {what}",
                h.by,
                fmt_ms(sched::HOLD_LINE_AFTER_MS)
            ));
            let at = plan
                .lines
                .iter()
                .find(|l| l.reservation.job == h.by)
                .map_or(now, |l| l.reservation.at);
            self.block("order", Some(node), Some(&order_block(&h.by, at)));
            return;
        }
        if let Some(r) = plan.reservation.as_ref().filter(|r| r.job == q.job) {
            self.line(format!("first in line on {node}: {}", when(r.at)));
        }
        match plan.lines.iter().find(|l| l.reservation.job == q.job) {
            Some(l) if l.why == HoldsLine::Asked => self.line(format!(
                "holds the line on {node} (dispatched with --hold-line): nothing behind it starts \
                 there until it does"
            )),
            Some(_) => self.line(format!(
                "holds the line on {node} (queued for {} or more): no job behind it that would \
                 take what it waits for starts there until it does",
                fmt_ms(sched::HOLD_LINE_AFTER_MS)
            )),
            None => {}
        }
    }

    /// Explains what `q` lacks on `m` given its load; false when it lacks nothing.
    fn lacks(&mut self, m: &MachineView, q: &Queued, load: &Load, pending: &Pending) -> bool {
        let now = self.snap.now;
        let node = m.facts.node.as_str();
        let r = &q.request;

        if let Some(x) = load.holds.iter().find(|h| h.exclusive) {
            let h = self.holder(m, pending, &x.job);
            self.line(format!(
                "waits for job {} to end: it has {node} to itself (exclusive) {}",
                h.job, h.clause
            ));
            self.block("exclusive", Some(node), Some(&h));
            return true;
        }

        if r.exclusive {
            if !load.holds.is_empty() {
                let empty = load.holds.iter().map(|h| h.end).max().unwrap_or(now);
                let n = load.holds.len();
                self.line(format!(
                    "needs {node} to itself (exclusive): {n} {} there, expected to end within {}",
                    if n == 1 { "job runs" } else { "jobs run" },
                    fmt_ms(empty.saturating_sub(now))
                ));
                let mut holds: Vec<&Hold> = load.holds.iter().collect();
                holds.sort_by_key(|h| h.end);
                for x in holds {
                    let h = self.holder(m, pending, &x.job);
                    self.line(format!("job {} runs there {}", h.job, h.clause));
                    self.block("exclusive", Some(node), Some(&h));
                }
            }
            return !load.holds.is_empty();
        }

        let mut short = false;
        if !r.gpus.is_empty() && load.grant(&without_ram(q)).is_none() {
            let (_, picked) = load.gpu_wait(&r.gpus, now);
            for i in picked {
                let id = &m.facts.gpus[i].id;
                if let Some(x) = load.gpu_holder(id) {
                    let h = self.holder(m, pending, &x.job);
                    self.line(format!(
                        "waits on gpu {} on {node}, held by job {} {}",
                        gpu_label(m, id),
                        h.job,
                        h.clause
                    ));
                    self.block(format!("gpu:{id}"), Some(node), Some(&h));
                    short = true;
                }
            }
        }

        let free = load.ram_free();
        if r.ram_mib > free {
            self.line(format!(
                "waits on RAM: needs {}, {} of {} free on {node}",
                fmt_mib(r.ram_mib),
                fmt_mib(free),
                fmt_mib(m.ram_capacity_mib)
            ));
            // The holders whose ends free enough, in deadline order.
            let mut holds: Vec<&Hold> = load.holds.iter().filter(|h| h.ram_mib > 0).collect();
            holds.sort_by_key(|h| (h.end, h.job.clone()));
            let mut freed = free;
            for x in holds {
                if freed >= r.ram_mib {
                    break;
                }
                freed = freed.saturating_add(x.ram_mib);
                let h = self.holder(m, pending, &x.job);
                self.line(format!(
                    "job {} holds {} there {}",
                    h.job,
                    fmt_mib(x.ram_mib),
                    h.clause
                ));
                self.block("ram", Some(node), Some(&h));
            }
            short = true;
        }
        short
    }

    fn starting(&mut self, r: &JobRecord) {
        let node = r.node.as_deref().unwrap_or("its machine");
        let ws = r
            .workspace
            .as_deref()
            .map_or("its workspace".to_string(), |w| format!("workspace {w}"));
        // started_at is set when it runs; until then its claim says since when.
        let since = r
            .started_at
            .or(r.lease.as_ref().map(|l| l.granted_at))
            .map_or(String::new(), |s| {
                format!(", for {} so far", fmt_ms(self.snap.now.saturating_sub(s)))
            });
        self.line(format!(
            "starting on {node}: {ws} is being seeded or checked{since}"
        ));
    }

    fn running(&mut self, r: &JobRecord) {
        let now = self.snap.now;
        let node = r.node.as_deref().unwrap_or("its machine");
        let lease = r.lease.as_ref();
        // The run limit counts from the start (the deadline is moved then), not from the claim.
        let start = r.started_at.or(lease.map(|l| l.granted_at));
        let limit = match (lease, start) {
            (Some(l), Some(s)) if l.deadline > s => l.deadline - s,
            _ => r.spec.timeout_ms,
        };
        let ws = r
            .workspace
            .as_deref()
            .map_or(String::new(), |w| format!(" in workspace {w}"));
        self.line(match start {
            Some(s) if now.saturating_sub(s) > limit => format!(
                "running on {node}{ws} for {}, past its {} run limit: being stopped",
                fmt_ms(now - s),
                fmt_ms(limit)
            ),
            Some(s) => {
                let ran = now.saturating_sub(s);
                format!(
                    "running on {node}{ws} for {} of its {} run limit, {} left",
                    fmt_ms(ran),
                    fmt_ms(limit),
                    fmt_ms(limit - ran)
                )
            }
            None => format!("running on {node}{ws}, run limit {}", fmt_ms(limit)),
        });

        if let Some(l) = lease {
            let machine = self.snap.machines.iter().find(|m| m.facts.node == node);
            let gpus: Vec<String> = l
                .grant
                .gpus
                .iter()
                .map(|g| machine.map_or(g.clone(), |m| gpu_label(m, g)))
                .collect();
            let ram = fmt_mib(l.grant.ram_mib);
            self.line(if l.grant.exclusive {
                format!("holds {node} to itself (exclusive), with {ram} RAM as its limit")
            } else if gpus.is_empty() {
                format!("holds {ram} RAM")
            } else {
                format!("holds {ram} RAM and gpu {}", gpus.join(", gpu "))
            });
        }

        let u = &r.usage;
        let cpu_idle = cpu_idle(r, now);
        self.line(match cpu_words(r, now) {
            Some(cpu) if u.cpu_ms < SECOND_MS => format!("{cpu}, under 1s of CPU time in all"),
            Some(cpu) => format!("{cpu}, {} of CPU time in all", fmt_ms(u.cpu_ms)),
            None => "no CPU sample yet".to_string(),
        });
        // The daemon starts the output clock at the start too: an empty log has had no output.
        let silent = u.log_bytes == 0 && u.output_at <= r.started_at.unwrap_or(0);
        let quiet = (u.output_at > 0).then(|| now.saturating_sub(u.output_at));
        self.line(match quiet {
            Some(q) if !silent => format!("last output {} ago", fmt_ms(q)),
            _ => "no output yet".to_string(),
        });
        if u.peak_mem_bytes > 0 {
            self.line(format!(
                "memory {}, peak {} of its {}",
                fmt_bytes(u.mem_bytes),
                fmt_bytes(u.peak_mem_bytes),
                fmt_mib(r.spec.request.ram_mib)
            ));
        }
        if r.stalled {
            let since_start = start.map(|s| now.saturating_sub(s));
            let still = [cpu_idle.or(since_start), quiet.or(since_start)]
                .into_iter()
                .flatten()
                .min();
            self.line(match still {
                Some(d) => format!("stalled: under 1% CPU and no output for {}", fmt_ms(d)),
                None => "stalled: under 1% CPU and no output".to_string(),
            });
        }
    }

    /// How a finished job ended. The first line of `why` already names its state, so these say
    /// when and where it ran, the daemon's message (unless it only repeats the state), and the
    /// code `potluck wait` exits with (unless the message already gave it).
    fn finished(&mut self, r: &JobRecord) {
        let now = self.snap.now;
        let on = r
            .node
            .as_ref()
            .map_or(String::new(), |n| format!(" on {n}"));
        let ago = |t: Millis| format!("{} ago", fmt_ms(now.saturating_sub(t)));
        match (r.started_at, r.ended_at) {
            (Some(s), Some(e)) => self.line(format!(
                "ran for {}{on}, ended {}",
                fmt_ms(e.saturating_sub(s)),
                ago(e)
            )),
            (None, Some(e)) => self.line(format!("ended {} without starting", ago(e))),
            (Some(s), None) => self.line(format!("started{on} {}", ago(s))),
            (None, None) => {}
        }
        // The most memory it used, as its machine measured it (0: none was), beside what it
        // reserved; not when its message says what it used (ended over memory, or by the OOM
        // killer).
        let peak = r.usage.peak_mem_bytes;
        let said = r.message.as_deref().is_some_and(|m| m.contains(": used "));
        if peak > 0 && !said {
            self.line(format!(
                "peak memory {} of the {} it reserved",
                fmt_bytes(peak),
                fmt_mib(r.spec.request.ram_mib)
            ));
        }
        let code = r.exit_code_for_client();
        let exited = |c: Option<i32>| c.map(|c| format!("exited with code {c}"));
        let how = match (&r.message, r.state) {
            (Some(m), state) if *m == state.to_string() => None,
            (Some(m), _) => Some(m.clone()),
            (None, JobState::Succeeded) => exited(r.exit_code),
            (None, JobState::Failed) => match (r.exit_code, r.signal) {
                (Some(c), _) => exited(Some(c)),
                (None, Some(s)) => Some(format!("killed by signal {s}")),
                _ => None,
            },
            (None, JobState::TimedOut) => Some(format!(
                "killed at its {} run limit",
                fmt_ms(r.spec.timeout_ms)
            )),
            (None, JobState::OverMemory) => Some(format!(
                "went over its {} memory limit",
                fmt_mib(r.spec.request.ram_mib)
            )),
            (None, JobState::Lost) => {
                Some("its machine went away and it was not queued again".to_string())
            }
            (None, _) => None,
        };
        let said_code = how.is_some() && how == exited(Some(code));
        if let Some(how) = how {
            self.line(how);
        }
        if !said_code {
            self.line(format!("potluck wait exits with {code}"));
        }
        if !r.stuck_pids.is_empty() {
            let pids: Vec<String> = r.stuck_pids.iter().map(u32::to_string).collect();
            self.line(format!(
                "processes that would not die: {}; its lease was released anyway",
                pids.join(", ")
            ));
        }
    }
}

/// "asks for 16 GiB RAM, gpu nvidia, a machine with os=linux; run limit 15m".
fn asks(q: &Queued) -> String {
    let r = &q.request;
    let mut parts = vec![format!("{} RAM", fmt_mib(r.ram_mib))];
    parts.extend(r.gpus.iter().map(|s| format!("gpu {s}")));
    if r.exclusive {
        parts.push("the whole machine (exclusive)".into());
    }
    if !q.on.is_empty() {
        parts.push(format!("a machine with {}", fmt_on_all(&q.on)));
    }
    let pinned = match &q.workspace {
        Some((ws, node, _)) => format!("; pinned to workspace {ws} on {node}"),
        None => String::new(),
    };
    format!(
        "asks for {}; run limit {}{pinned}",
        parts.join(", "),
        fmt_ms(q.timeout_ms)
    )
}

/// The job with no RAM, to ask whether its GPUs alone could be granted.
fn without_ram(q: &Queued) -> Queued {
    let mut q = q.clone();
    q.request.ram_mib = 0;
    q
}

/// A GPU as `why` names it: its alias, or its id.
fn gpu_label(m: &MachineView, id: &str) -> String {
    m.facts
        .gpus
        .iter()
        .find(|g| g.id == id)
        .and_then(|g| g.alias.clone())
        .unwrap_or_else(|| id.to_string())
}

/// How long a job has used under 1% of one CPU core (the daemon's measure of progress, from its
/// samples): since it last used more, or since it started (the daemon starts the clock then).
/// `None` before any sample.
fn cpu_idle(r: &JobRecord, now: Millis) -> Option<u64> {
    (r.usage.cpu_progress_at > 0).then(|| now.saturating_sub(r.usage.cpu_progress_at))
}

/// What `why` says of a job's CPU use, from the daemon's samples (every half second where it
/// runs): "using CPU" when it used 1% of a core or more in the last second, else "under 1% CPU
/// for 9m". The daemon starts the clock when the job starts, so a job that hasn't used that much
/// since is under 1% from its start.
fn cpu_words(r: &JobRecord, now: Millis) -> Option<String> {
    let idle = cpu_idle(r, now)?;
    let risen = r.started_at.is_none_or(|s| r.usage.cpu_progress_at > s);
    Some(if risen && idle < SECOND_MS {
        "using CPU".to_string()
    } else {
        format!("under 1% CPU for {}", fmt_ms(idle))
    })
}
