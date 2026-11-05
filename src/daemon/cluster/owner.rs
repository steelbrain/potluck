//! The owner's side: granting claims, taking runners' reports, logs and kept files, queueing jobs
//! again when their runner is lost, and forwarding cancels.

use super::peer::{LogFrame, PeerRequest, PeerResponse, node_of_ws, now, owner_of};
use super::remote::read_line;
use super::{CState, Cluster, KV_CANCELS, KV_ENDINGS, KV_LOG_BASE, capable, save};
use crate::daemon::state::State;
use crate::daemon::{Shared, files, runner};
use crate::proto::{Response, ServerResponse, read_msg};
use crate::types::{
    EXIT_CANCELLED, EXIT_LOST, EXIT_TIMEOUT, JobId, JobRecord, JobState, Lease, Millis, NodeName,
    RepoRef, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufReader, Read, Write};
use std::path::{Component, Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A cancel to forward to the job's runner.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CancelFwd {
    pub runner: String,
    pub token: String,
    /// The runner has it.
    pub acked: bool,
}

/// A job that ended on its runner and whose kept files haven't arrived: its end, kept aside until
/// they do, or until they are given up on (the job then ends without them, with a note).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PendingEnd {
    pub runner: NodeName,
    pub record: JobRecord,
    /// Why the kept files haven't arrived, as the runner last said.
    pub error: String,
    /// When the end first arrived (this machine's clock).
    pub since: Millis,
}

/// The note on a job whose kept files were given up on.
pub(super) fn keep_note(runner: &str, error: &str) -> String {
    format!("kept files from {runner} were not delivered: {error}")
}

/// How long a delivered end is remembered for reconciles that crossed it.
const DELIVERED_MS: u64 = 60_000;
/// A log stream with nothing for this long is taken as dead.
const LOG_IDLE: Duration = Duration::from_secs(30);
/// A queued job pinned to another machine's workspace fails when a report that arrived this long
/// after its dispatch no longer lists the workspace.
const WS_REPORT_MARGIN_MS: u64 = 5_000;

fn token(j: &JobRecord) -> Option<&str> {
    j.lease.as_ref().map(|l| l.token.as_str())
}

/// The owner's active job `id`, if `runner` holds it with lease `tok`.
fn held_by<'a>(st: &'a mut State, id: &str, runner: &str, tok: &str) -> Option<&'a mut JobRecord> {
    let me = st.node.clone();
    st.jobs.get_mut(id).filter(|j| {
        owner_of(id) == me
            && matches!(j.state, JobState::Starting | JobState::Running)
            && j.node.as_deref() == Some(runner)
            && token(j) == Some(tok)
    })
}

/// Whether `from` already holds `id` with the lease `tok`: a claim sent again (see [`claim`]).
pub(super) fn repeated_claim(sh: &Shared, from: &str, id: &str, tok: &str) -> bool {
    let st = sh.lock();
    st.jobs.get(id).is_some_and(|j| {
        j.state == JobState::Starting && j.node.as_deref() == Some(from) && token(j) == Some(tok)
    })
}

/// A runner claims a queued job: granted once, while it is still queued.
pub(super) fn claim(
    sh: &Shared,
    c: &Cluster,
    from: &str,
    id: &str,
    lease: Lease,
    workspace: &WorkspaceId,
) -> PeerResponse {
    let refused = |reason: String| PeerResponse::Refused { reason };
    // The last report made before the grant (reports are numbered with the state locked).
    let reported = || (c.started_at, c.report_seq.load(Ordering::SeqCst));
    // Back from being away, this machine first compares membership records with the others: the
    // claiming machine may have been removed meanwhile.
    let mesh = c.mesh();
    let caught_up = mesh.caught_up_for(from);
    let taker = Taker::of(&mesh, from);
    let (rec, reported) = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        if c.removed() {
            return refused("this machine was removed from the cluster".into());
        }
        if !caught_up {
            return refused(
                "this machine is back from being away and compares membership records with the other members first".into(),
            );
        }
        if owner_of(id) != st.node {
            return refused(format!("{id} is not {}'s job", st.node));
        }
        let Some(j) = st.jobs.get_mut(id) else {
            return refused(format!("no job {id}"));
        };
        match j.state {
            JobState::Queued => {}
            // The same claim again (the mesh resends a call whose connection closed before its
            // answer): granted again, so the runner doesn't drop a job this machine says it has.
            JobState::Starting
                if j.node.as_deref() == Some(from) && token(j) == Some(lease.token.as_str()) =>
            {
                return PeerResponse::Granted {
                    job: Box::new(j.clone()),
                    reported: Some(reported()),
                };
            }
            JobState::Starting | JobState::Running => {
                let on = j.node.clone().unwrap_or_default();
                return refused(format!("{id} was taken by {on}"));
            }
            s => return refused(format!("{id} is {s}")),
        }
        if let Some(w) = &j.spec.workspace
            && node_of_ws(w) != from
        {
            return refused(format!("{id} is pinned to workspace {w}"));
        }
        if let Some(why) = why_not_take(j, &taker) {
            return refused(format!("{id} is not for {from}: {why}"));
        }
        let facts = {
            let cs = c.cs();
            cs.reports
                .get(from)
                .map(|r| r.report.facts.clone())
                .or_else(|| cs.known.get(from).map(|k| k.facts.clone()))
                .filter(|f| !f.os.is_empty())
        };
        if let Some(why) = unmet_on(j, from, facts.as_ref()) {
            return refused(format!("{id} is not for {from}: {why}"));
        }
        j.state = JobState::Starting;
        j.node = Some(from.to_string());
        j.workspace = Some(workspace.clone());
        // Its run limit as this machine counts it ([`tick`]), not as the runner's lease says.
        let granted_at = now();
        j.lease = Some(Lease {
            granted_at,
            deadline: granted_at.saturating_add(j.spec.timeout_ms),
            ..lease
        });
        j.handle = None;
        j.started_at = None;
        // "requeued: ..." was about the run before; the log keeps it.
        j.message = None;
        let rec = j.clone();
        st.save_job(id);
        st.refresh_pending();
        let boot = c.boot();
        let mut cs = c.cs();
        cs.granted_at.insert(id.to_string(), boot);
        cs.run_limits
            .insert(id.to_string(), boot.saturating_add(rec.spec.timeout_ms));
        drop(cs);
        (rec, reported())
    };
    sh.log.log(format!("{id}: claimed by {from}"));
    c.push_soon();
    sh.notify();
    PeerResponse::Granted {
        job: Box::new(rec),
        reported: Some(reported),
    }
}

/// A member that asks for this machine's queued jobs, as this machine knows it: its pool from its
/// admission (None: no member has that name), and whether the cluster has users.
pub(super) struct Taker {
    pub name: NodeName,
    pub pool: Option<String>,
    pub users: bool,
}

impl Taker {
    pub fn of(mesh: &crate::mesh::Mesh, peer: &str) -> Taker {
        Taker {
            name: peer.to_string(),
            pool: mesh.pool_of(peer),
            users: mesh.users_exist(),
        }
    }
}

/// Whether member `t` may take this machine's queued job `j`: it sees the job in this machine's
/// reports, fetches the repositories the job names (before it claims the job, its mirrors sync),
/// and is granted it. See [`why_not_take`].
pub(super) fn may_take(j: &JobRecord, t: &Taker) -> bool {
    why_not_take(j, t).is_none()
}

/// Why member `t` may not take queued job `j`: it isn't in one of the job's pools; or the cluster
/// has users and the job is unsigned (dispatched before the first user: only this machine, its
/// owner, runs it). None when it may.
pub(super) fn why_not_take(j: &JobRecord, t: &Taker) -> Option<String> {
    let Some(pool) = &t.pool else {
        return Some(format!("{} is not a member", t.name));
    };
    if t.users && j.spec.auth.is_none() {
        return Some(
            "it was dispatched unsigned, before the cluster had users: only the machine it was dispatched on runs it"
                .into(),
        );
    }
    if !j.spec.runs_in(pool) {
        return Some(format!(
            "it runs in {}, and {} is in pool {pool}",
            crate::types::fmt_pools(&j.spec.pools),
            t.name
        ));
    }
    None
}

/// Why the job `j` can't run on member `from` by its `--on`, if it can't: `facts` is what `from`
/// last reported (None when no report of it reached this machine yet: nothing is checked). The
/// facts are its own word: this catches mistakes, not a member that lies. `node` is its name in
/// the cluster, whatever it reports.
fn unmet_on(j: &JobRecord, from: &str, facts: Option<&crate::types::Facts>) -> Option<String> {
    let facts = crate::types::Facts {
        node: from.to_string(),
        ..facts?.clone()
    };
    facts
        .unmet(&j.spec.on)
        .map(|u| u.replacen("(this machine: ", &format!("({from}: "), 1))
}

/// A runner passed over these queued jobs: one pass each.
pub(super) fn passed(sh: &Shared, c: &Cluster, jobs: &[JobId]) {
    let mut st = sh.lock();
    let me = st.node.clone();
    for id in jobs {
        if owner_of(id) != me {
            continue;
        }
        if let Some(j) = st.jobs.get_mut(id)
            && j.state == JobState::Queued
        {
            j.passes = j.passes.saturating_add(1);
            st.save_job(id);
        }
    }
    drop(st);
    c.push_soon();
}

/// A runner's record of a job it runs for this machine. An end that comes with `keep_error` waits
/// aside for the job's kept files ([`PendingEnd`]).
pub(super) fn update(
    sh: &Shared,
    c: &Cluster,
    from: &str,
    rec: JobRecord,
    keep_error: Option<String>,
) -> PeerResponse {
    let id = rec.id.clone();
    let Some(tok) = token(&rec).map(str::to_string) else {
        return PeerResponse::Update { stale: true };
    };
    let mut unpin = None;
    let mut changed = None;
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        if held_by(st, &id, from, &tok).is_none() {
            // A repeat of a final record already taken is fine; anything else is stale.
            let taken = st.job(&id).is_some_and(|j| {
                j.state.is_done()
                    && rec.state.is_done()
                    && j.node.as_deref() == Some(from)
                    && token(&j) == Some(tok.as_str())
            });
            return PeerResponse::Update { stale: !taken };
        }
        if rec.state.is_done()
            && let Some(e) = keep_error
        {
            let mut cs = c.cs();
            let first = !cs.endings.contains_key(&id);
            let since = cs.endings.get(&id).map_or_else(now, |p| p.since);
            cs.endings.insert(
                id.clone(),
                PendingEnd {
                    runner: from.to_string(),
                    record: JobRecord {
                        handle: None,
                        ..rec
                    },
                    error: e.clone(),
                    since,
                },
            );
            save(st, KV_ENDINGS, &cs.endings);
            if first {
                st.log.log(format!(
                    "{id}: ended on {from}; its kept files have not arrived ({e}): it waits for them up to {}",
                    crate::types::fmt_ms(c.opts.keep_give_up_ms)
                ));
            }
            return PeerResponse::Update { stale: false };
        }
        let Some(j) = held_by(st, &id, from, &tok) else {
            return PeerResponse::Update { stale: true };
        };
        let before = j.state;
        if rec.started_at.is_some() {
            j.started_at = rec.started_at;
        }
        if rec.workspace.is_some() {
            j.workspace = rec.workspace.clone();
        }
        // The run limit counts from the start, as for a job run here (seeding its workspace
        // doesn't count): from when the runner said it started, on this machine's clocks. The
        // deadline the runner's record says is its own.
        let started = before == JobState::Starting && rec.state == JobState::Running;
        if started && let Some(l) = j.lease.as_mut() {
            l.deadline = now().saturating_add(j.spec.timeout_ms);
            c.cs()
                .run_limits
                .insert(id.clone(), c.boot().saturating_add(j.spec.timeout_ms));
        }
        j.usage = rec.usage.clone();
        j.stalled = rec.stalled;
        for note in &rec.notes {
            if !j.notes.contains(note) {
                j.notes.push(note.clone());
            }
        }
        j.stuck_pids = rec.stuck_pids.clone();
        if rec.state.is_done() {
            j.state = rec.state;
            j.exit_code = rec.exit_code;
            j.signal = rec.signal;
            j.message = rec.message.clone();
            j.ended_at = rec.ended_at.or(Some(now()));
            j.kept = rec.kept.clone();
            unpin = Some(j.spec.repo.clone());
        } else if rec.state == JobState::Running {
            j.state = JobState::Running;
        }
        if j.state != before {
            // The state isn't said twice ("cancelled", "over memory: used ...").
            changed = Some((j.state, runner::after_state(j.state, j.message.as_deref())));
        }
        st.save_job(&id);
        if unpin.is_some() {
            st.refresh_pending();
            let mut cs = c.cs();
            cs.granted_at.remove(&id);
            cs.delivered.insert(id.clone(), (tok.clone(), c.boot()));
            if cs.cancels.remove(&id).is_some() {
                save(st, KV_CANCELS, &cs.cancels);
            }
            if cs.endings.remove(&id).is_some() {
                save(st, KV_ENDINGS, &cs.endings);
            }
        }
    }
    if let Some((state, msg)) = changed {
        sh.log.log(format!("{id}: {state} on {from}{msg}"));
    }
    if let Some(repo) = unpin {
        runner::unpin_all(sh, &repo, &id);
    }
    sh.notify();
    PeerResponse::Update { stale: false }
}

/// Queues a job of this machine's again, keeping its passes. Output from the lost run stays in the
/// log, followed by a line saying why it starts over; the next run's output follows that.
fn requeue(sh: &Shared, st: &mut State, cs: &mut CState, id: &str, why: &str) {
    let path = sh.paths.job_dir(id).join(files::LOG);
    let mut len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    if len > 0 {
        let line = format!("\n--- potluck: {why} ---\n");
        if std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .and_then(|mut f| f.write_all(line.as_bytes()))
            .is_ok()
        {
            len += line.len() as u64;
        }
    }
    cs.log_base.insert(id.to_string(), len);
    save(st, KV_LOG_BASE, &cs.log_base);
    cs.granted_at.remove(id);
    if let Some(j) = st.jobs.get_mut(id) {
        // What its runner said of its own mirrors and kept files was about the run that is over
        // (the log keeps it); the next run, wherever it is, would repeat it at its start.
        if let Some(runner) = &j.node {
            let (sync, keep) = (format!("{runner}: "), format!("--keep on {runner}: "));
            j.notes
                .retain(|n| !n.starts_with(&sync) && !n.starts_with(&keep));
        }
        j.state = JobState::Queued;
        j.node = None;
        j.workspace = None;
        j.lease = None;
        j.handle = None;
        j.started_at = None;
        j.usage = Default::default();
        j.stalled = false;
        j.stuck_pids.clear();
        j.message = Some(why.to_string());
    }
    st.save_job(id);
    st.log.log(format!("{id}: {why}"));
}

/// Ends a job of this machine's that never reached its end on a runner. `why` doesn't repeat the
/// state: the log, `wait` and `why` show it before the message.
fn end(
    st: &mut State,
    id: &str,
    state: JobState,
    code: Option<i32>,
    why: String,
) -> Option<RepoRef> {
    let j = st.jobs.get_mut(id)?;
    j.state = state;
    j.exit_code = code;
    j.signal = None;
    j.ended_at = Some(now());
    j.message = Some(why.clone());
    let repo = j.spec.repo.clone();
    st.save_job(id);
    st.log.log(format!("{id}: {state}: {why}"));
    Some(repo)
}

/// Ends job `id` as its runner said it ended, without the kept files that haven't arrived, with a
/// note saying so; `why` says why the wait for them ends. Nothing if its end isn't kept aside,
/// or the job is no longer that runner's (the end is dropped then).
fn give_up_keep(st: &mut State, cs: &mut CState, id: &str, why: &str) -> Option<RepoRef> {
    let p = cs.endings.remove(id)?;
    save(st, KV_ENDINGS, &cs.endings);
    let tok = token(&p.record)?.to_string();
    let j = held_by(st, id, &p.runner, &tok)?;
    let rec = &p.record;
    j.state = rec.state;
    j.exit_code = rec.exit_code;
    j.signal = rec.signal;
    j.message = rec.message.clone();
    j.ended_at = rec.ended_at.or(Some(now()));
    j.stuck_pids = rec.stuck_pids.clone();
    j.usage = rec.usage.clone();
    j.kept = Vec::new();
    for note in rec
        .notes
        .iter()
        .cloned()
        .chain([keep_note(&p.runner, &p.error)])
    {
        if !j.notes.contains(&note) {
            j.notes.push(note);
        }
    }
    let line = format!(
        "{id}: {} on {}{}; ended without its kept files: {why}",
        j.state,
        p.runner,
        runner::after_state(j.state, j.message.as_deref())
    );
    let repo = j.spec.repo.clone();
    st.save_job(id);
    st.log.log(line);
    cs.granted_at.remove(id);
    if cs.cancels.remove(id).is_some() {
        save(st, KV_CANCELS, &cs.cancels);
    }
    Some(repo)
}

/// A job whose runner lost it: cancelled if a cancel was on its way, lost (exit 125) if its
/// dispatch said `--no-rerun` or pinned a workspace or no machine that is up can run it, otherwise
/// queued again.
fn lose(
    sh: &Shared,
    c: &Cluster,
    st: &mut State,
    cs: &mut CState,
    id: &str,
    (runner, why): (&str, &str),
    unpins: &mut Vec<(RepoRef, JobId)>,
) {
    // It ended there already; only its kept files were on their way.
    if cs.endings.contains_key(id) {
        if let Some(r) = give_up_keep(st, cs, id, &format!("{runner} {why}")) {
            unpins.push((r, id.to_string()));
        }
        return;
    }
    let Some(j) = st.jobs.get(id) else {
        return;
    };
    let pinned = j.spec.workspace.is_some();
    let rerun = j.spec.rerun_if_lost;
    // Queued again only if a machine that is up can run it (the runner, lost, can't).
    let nowhere = {
        let q = st.queued(j);
        match capable::machines(st, Some((c, cs))).verdict(sh, &q) {
            capable::Verdict::Up => None,
            capable::Verdict::Waits | capable::Verdict::Lost(_) => {
                Some(format!("no machine that can run it is up ({runner} {why})"))
            }
        }
    };
    if cs.cancels.remove(id).is_some() {
        if let Some(r) = end(
            st,
            id,
            JobState::Cancelled,
            Some(EXIT_CANCELLED),
            format!("{runner} {why} before it stopped"),
        ) {
            unpins.push((r, id.to_string()));
        }
    } else if !rerun || pinned {
        let because = if pinned {
            "it is pinned to a workspace there"
        } else {
            "its dispatch said --no-rerun"
        };
        if let Some(r) = end(
            st,
            id,
            JobState::Lost,
            Some(EXIT_LOST),
            format!("{runner} {why}; not queued again: {because}"),
        ) {
            unpins.push((r, id.to_string()));
        }
    } else if let Some(nowhere) = nowhere {
        if let Some(r) = end(st, id, JobState::Lost, Some(EXIT_LOST), nowhere) {
            unpins.push((r, id.to_string()));
        }
    } else {
        requeue(sh, st, cs, id, &format!("requeued: {runner} {why}"));
    }
}

/// A machine is gone (down, removed): this machine's jobs on it are queued again or end, and its
/// queued jobs pinned to workspaces there end as lost.
pub(super) fn runner_lost(sh: &Shared, c: &Cluster, node: &str, why: &str) {
    let mut unpins = Vec::new();
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        if node == st.node {
            return;
        }
        let me = st.node.clone();
        let mut cs = c.cs();
        let cs = &mut *cs;
        let on: Vec<JobId> = st
            .jobs
            .values()
            .filter(|j| owner_of(&j.id) == me)
            .filter(|j| matches!(j.state, JobState::Starting | JobState::Running))
            .filter(|j| j.node.as_deref() == Some(node))
            .map(|j| j.id.clone())
            .collect();
        let pinned: Vec<(JobId, String)> = st
            .jobs
            .values()
            .filter(|j| owner_of(&j.id) == me && j.state == JobState::Queued)
            .filter_map(|j| {
                let w = j.spec.workspace.as_ref()?;
                (node_of_ws(w) == node).then(|| (j.id.clone(), w.clone()))
            })
            .collect();
        if on.is_empty() && pinned.is_empty() {
            return;
        }
        // Down, but still a member: a job that ended there waits for its kept files (until it
        // is back, or they are given up on).
        let away = c.is_member(node) && cs.out.is_none();
        for id in &on {
            if away && cs.endings.contains_key(id) {
                continue;
            }
            lose(sh, c, st, cs, id, (node, why), &mut unpins);
        }
        for (id, w) in &pinned {
            if let Some(r) = end(
                st,
                id,
                JobState::Lost,
                Some(EXIT_LOST),
                format!("workspace {w} is gone: its machine {node} {why}"),
            ) {
                unpins.push((r, id.clone()));
            }
        }
        save(st, KV_CANCELS, &cs.cancels);
        st.refresh_pending();
    }
    for (repo, id) in unpins {
        runner::unpin_all(sh, &repo, &id);
    }
    c.push_soon();
    sh.notify();
    sh.kick();
}

/// A runner hands back a job it claimed but won't start.
pub(super) fn give_back(sh: &Shared, c: &Cluster, from: &str, id: &str, tok: &str, message: &str) {
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        if held_by(st, id, from, tok).is_none() {
            return;
        }
        let mut cs = c.cs();
        requeue(sh, st, &mut cs, id, &format!("requeued: {message}"));
        st.refresh_pending();
    }
    c.push_soon();
    sh.notify();
    sh.kick();
}

/// A runner lists this machine's jobs it holds: those no longer its are named back; jobs this
/// machine believes it holds but it doesn't (granted a while ago) are lost there.
pub(super) fn reconcile(
    sh: &Shared,
    c: &Cluster,
    from: &str,
    jobs: &[(JobId, String)],
) -> PeerResponse {
    let boot = c.boot();
    let mut drop = Vec::new();
    let mut unpins = Vec::new();
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let me = st.node.clone();
        let listed: BTreeMap<&str, &str> =
            jobs.iter().map(|(j, t)| (j.as_str(), t.as_str())).collect();
        let mut cs = c.cs();
        let cs = &mut *cs;
        for (id, tok) in &listed {
            // Listed before its end reached this machine: that run is over, and was delivered.
            let delivered = cs.delivered.get(*id).is_some_and(|(t, _)| t == tok);
            if held_by(st, id, from, tok).is_none() && !delivered {
                drop.push(id.to_string());
            }
        }
        let missing: Vec<JobId> = st
            .jobs
            .values()
            .filter(|j| owner_of(&j.id) == me)
            .filter(|j| matches!(j.state, JobState::Starting | JobState::Running))
            .filter(|j| j.node.as_deref() == Some(from) && !listed.contains_key(j.id.as_str()))
            .filter(|j| {
                cs.granted_at
                    .get(&j.id)
                    .is_none_or(|t| boot.saturating_sub(*t) >= c.opts.claim_grace_ms)
            })
            .map(|j| j.id.clone())
            .collect();
        for id in &missing {
            lose(sh, c, st, cs, id, (from, "no longer has it"), &mut unpins);
        }
        if !missing.is_empty() {
            save(st, KV_CANCELS, &cs.cancels);
            st.refresh_pending();
        }
    }
    for (repo, id) in unpins {
        runner::unpin_all(sh, &repo, &id);
    }
    if !drop.is_empty() {
        sh.log.log(format!(
            "cluster: {from} holds jobs that are no longer its: {}",
            drop.join(", ")
        ));
    }
    c.push_soon();
    sh.notify();
    PeerResponse::Reconciled { drop }
}

/// `cancel` of a job of this machine's running elsewhere: forwarded to its runner (until it has
/// it). None when the job isn't running elsewhere (the local cancel handles it).
pub(crate) fn cancel_remote(sh: &Shared, c: &Cluster, id: &str) -> Option<Response> {
    let job = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let me = st.node.clone();
        let j = st.jobs.get(id)?;
        if owner_of(id) != me || !matches!(j.state, JobState::Starting | JobState::Running) {
            return None;
        }
        let runner = j.node.clone()?;
        if runner == me {
            return None;
        }
        let tok = token(j)?.to_string();
        let j = j.clone();
        let mut cs = c.cs();
        // It ended there already and waits for its kept files: it ends now, without them.
        if cs.endings.contains_key(id) {
            let repo = give_up_keep(st, &mut cs, id, "cancelled while they were on their way");
            drop(cs);
            st.refresh_pending();
            let job = st.job(id)?;
            drop(guard);
            if let Some(repo) = repo {
                runner::unpin_all(sh, &repo, id);
            }
            sh.notify();
            return Some(Response::Job { job: Box::new(job) });
        }
        cs.cancels.entry(id.to_string()).or_insert(CancelFwd {
            runner: runner.clone(),
            token: tok,
            acked: false,
        });
        save(st, KV_CANCELS, &cs.cancels);
        st.log
            .log(format!("{id}: cancel asked; forwarded to {runner}"));
        j
    };
    c.wake();
    Some(Response::Job { job: Box::new(job) })
}

/// The owner's periodic work: forwarding cancels; failing queued jobs pinned to a workspace on
/// another machine that is gone; and ending (125) queued jobs whose every capable machine is lost.
/// `now` is the wall time (kept ends' times are, as they are kept across restarts), `boot` the
/// boot clock's.
pub(super) fn tick(sh: &Arc<Shared>, c: &Arc<Cluster>, now: Millis, boot: u64) {
    let mut sends = Vec::new();
    let mut unpins = Vec::new();
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let me = st.node.clone();
        let mut cs = c.cs();
        let cs = &mut *cs;
        // Cancels whose job ended or went elsewhere are dropped.
        let before = cs.cancels.len();
        cs.cancels.retain(|id, f| {
            st.jobs.get(id).is_some_and(|j| {
                matches!(j.state, JobState::Starting | JobState::Running)
                    && j.node.as_deref() == Some(f.runner.as_str())
            })
        });
        if cs.cancels.len() != before {
            save(st, KV_CANCELS, &cs.cancels);
        }
        // Ends waiting for kept files that haven't come for the give-up time end without them.
        let late: Vec<JobId> = cs
            .endings
            .iter()
            .filter(|(_, p)| now.saturating_sub(p.since) >= c.opts.keep_give_up_ms)
            .map(|(id, _)| id.clone())
            .collect();
        let waited = format!(
            "they did not arrive within {}",
            crate::types::fmt_ms(c.opts.keep_give_up_ms)
        );
        for id in late {
            if let Some(r) = give_up_keep(st, cs, &id, &waited) {
                unpins.push((r, id));
            }
        }
        // When a job was granted, and where its run's output starts in its log, matter until it
        // ends (however it ends).
        let active = |id: &JobId| st.jobs.get(id).is_some_and(|j| !j.state.is_done());
        cs.granted_at.retain(|id, _| active(id));
        cs.run_limits.retain(|id, _| active(id));
        // A reconcile sent before a job's end arrived is answered within its call's time.
        cs.delivered
            .retain(|_, (_, at)| boot.saturating_sub(*at) < DELIVERED_MS);
        let before = cs.log_base.len();
        cs.log_base.retain(|id, _| active(id));
        if cs.log_base.len() != before {
            save(st, KV_LOG_BASE, &cs.log_base);
        }
        for (id, f) in &cs.cancels {
            let key = format!("cancel:{id}");
            if !f.acked && c.alive(&f.runner) && cs.sending.insert(key) {
                sends.push((id.clone(), f.clone()));
            }
        }
        let mut gone = Vec::new();
        for j in st.jobs.values() {
            if owner_of(&j.id) != me || j.state != JobState::Queued {
                continue;
            }
            let Some(w) = &j.spec.workspace else {
                continue;
            };
            let node = node_of_ws(w);
            if node == me {
                continue;
            }
            if !c.is_member(node) {
                gone.push((
                    j.id.clone(),
                    true,
                    format!("workspace {w} is gone: {node} is not in the cluster"),
                ));
            } else if let Some(r) = cs.reports.get(node)
                && r.at >= j.submitted_at.saturating_add(WS_REPORT_MARGIN_MS)
                && !r.report.workspaces.iter().any(|x| x.id == *w)
            {
                gone.push((
                    j.id.clone(),
                    false,
                    format!("workspace {w} is gone from {node}"),
                ));
            }
        }
        // Jobs running elsewhere whose runner hasn't ended them by their run limit, as this
        // machine counts it, and the grace after it, end as timed out: the runner's word doesn't
        // keep a job going. The grace counts only while this machine is awake and the runner up
        // (its end, delivered, waits for both: this machine asleep, or the runner unreachable,
        // uses none of it). The runner learns that it is no longer its job at its next update or
        // reconcile, and stops it. One that ended there and waits for its kept files is over.
        let grace = c.opts.run_limit_grace_ms;
        let awake = c.clock.awake_ms();
        let mut over = Vec::new();
        let mut overdue = BTreeMap::new();
        for j in st.jobs.values() {
            if owner_of(&j.id) != me
                || !matches!(j.state, JobState::Starting | JobState::Running)
                || cs.endings.contains_key(&j.id)
            {
                continue;
            }
            let (Some(runner), Some(l)) = (j.node.as_deref().filter(|n| *n != me), &j.lease) else {
                continue;
            };
            // After a restart: what is left of the lease's deadline, counted from now.
            let limit = *cs
                .run_limits
                .entry(j.id.clone())
                .or_insert_with(|| boot.saturating_add(l.deadline.saturating_sub(now)));
            if boot < limit {
                continue;
            }
            let (mut used, at) = cs.overdue.get(&j.id).copied().unwrap_or((0, awake));
            if c.alive(runner) {
                used = used.saturating_add(awake.saturating_sub(at));
            }
            overdue.insert(j.id.clone(), (used, awake));
            if used < grace {
                continue;
            }
            let (what, from) = match j.state {
                JobState::Starting => ("started", "its claim"),
                _ => ("ended", "its start"),
            };
            over.push((
                j.id.clone(),
                format!(
                    "{runner} had not {what} it {} past its run limit ({}), counted on {me} from {from}",
                    crate::types::fmt_ms(grace),
                    crate::types::fmt_ms(j.spec.timeout_ms)
                ),
            ));
        }
        cs.overdue = overdue;
        // Queued jobs (not pinned) that no machine up can run, and whose every capable machine is
        // lost, end: they would wait for machines that may never come back.
        let machines = capable::machines(st, Some((c, &*cs)));
        for j in st.jobs.values() {
            if owner_of(&j.id) != me || j.state != JobState::Queued || j.spec.workspace.is_some() {
                continue;
            }
            if let capable::Verdict::Lost(why) = machines.verdict(sh, &st.queued(j)) {
                gone.push((j.id.clone(), true, why));
            }
        }
        for (id, lost, why) in gone {
            let (state, code) = match lost {
                true => (JobState::Lost, Some(EXIT_LOST)),
                false => (JobState::Failed, None),
            };
            if let Some(r) = end(st, &id, state, code, why) {
                unpins.push((r, id));
            }
        }
        for (id, why) in over {
            if let Some(r) = end(st, &id, JobState::TimedOut, Some(EXIT_TIMEOUT), why) {
                unpins.push((r, id));
            }
        }
        if !unpins.is_empty() {
            st.refresh_pending();
        }
    }
    let ended = !unpins.is_empty();
    for (repo, id) in unpins {
        runner::unpin_all(sh, &repo, &id);
    }
    if ended {
        sh.notify();
    }
    for (id, f) in sends {
        let c2 = c.clone();
        sh.spawn_worker(&format!("cancel-{id}"), move |sh| {
            let r = c2.call(
                &f.runner,
                &PeerRequest::Cancel {
                    job: id.clone(),
                    token: f.token.clone(),
                },
                Duration::from_secs(15),
            );
            let mut guard = sh.lock();
            let st = &mut *guard;
            let mut cs = c2.cs();
            cs.sending.remove(&format!("cancel:{id}"));
            if r.is_ok()
                && let Some(x) = cs.cancels.get_mut(&id)
            {
                x.acked = true;
                save(st, KV_CANCELS, &cs.cancels);
            }
        });
    }
}

fn reply_line(w: &mut impl Write, v: serde_json::Value) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(&v)?;
    line.push(b'\n');
    w.write_all(&line)
}

/// A runner streams a job's output log here: appended to the job's log, after what this machine
/// already has of the run.
pub(super) fn receive_log(
    sh: &Shared,
    c: &Cluster,
    from: &str,
    id: &str,
    tok: &str,
    stream: crate::mesh::Stream,
) {
    let Ok(mut w) = stream.try_clone() else {
        return;
    };
    let ok = {
        let mut st = sh.lock();
        held_by(&mut st, id, from, tok).is_some()
    };
    if !ok {
        let _ = reply_line(
            &mut w,
            serde_json::json!({"error": format!("{id} is not {from}'s job")}),
        );
        return;
    }
    let lock = c.log_lock(id);
    let dir = sh.paths.job_dir(id);
    let path = dir.join(files::LOG);
    let base = c.cs().log_base.get(id).copied().unwrap_or(0);
    let len = {
        let _g = lock.lock();
        if let Err(e) = std::fs::create_dir_all(&dir) {
            let _ = reply_line(&mut w, serde_json::json!({"error": e.to_string()}));
            return;
        }
        std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
    };
    let mut have = len.saturating_sub(base);
    // Where the run's output starts in the log, for the runner to answer `logs` at this log's
    // offsets while this machine can't be reached.
    if reply_line(&mut w, serde_json::json!({"offset": have, "base": base})).is_err() {
        return;
    }
    let _ = stream.set_read_timeout(Some(LOG_IDLE));
    let mut r = BufReader::new(stream);
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let Ok(line) = read_line(&mut r) else {
            return;
        };
        let Ok(f) = serde_json::from_str::<LogFrame>(&line) else {
            return;
        };
        match f {
            LogFrame::Ping => {}
            LogFrame::Data { at, len } => {
                // The run may have ended here, or gone back to the queue, since the stream
                // opened: its output then belongs to no run here (with no output before it,
                // nothing in the log marks the place a lost run's output would take).
                if held_by(&mut sh.lock(), id, from, tok).is_none() {
                    let _ = reply_line(
                        &mut w,
                        serde_json::json!({"error": format!("{id} is no longer {from}'s job")}),
                    );
                    return;
                }
                let _g = lock.lock();
                let now_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                if at != have || now_len.saturating_sub(base) != have {
                    // Another stream wrote meanwhile: this one is stale.
                    let _ = reply_line(
                        &mut w,
                        serde_json::json!({"error": format!("out of step at {at}")}),
                    );
                    return;
                }
                let mut f = match std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                {
                    Ok(f) => f,
                    Err(e) => return cannot_write_log(sh, c, &mut w, id, from, &path, &e),
                };
                let mut left = len;
                while left > 0 {
                    let n = (left as usize).min(buf.len());
                    if r.read_exact(&mut buf[..n]).is_err() {
                        return;
                    }
                    if let Err(e) = f.write_all(&buf[..n]) {
                        // What was written of it counts: the runner goes on from the log's length.
                        return cannot_write_log(sh, c, &mut w, id, from, &path, &e);
                    }
                    left -= n as u64;
                    have += n as u64;
                }
                drop(_g);
                sh.notify();
            }
            LogFrame::End { len } => {
                let _ = if len == have {
                    reply_line(&mut w, serde_json::json!({"len": have}))
                } else {
                    reply_line(
                        &mut w,
                        serde_json::json!({"error": format!("have {have} bytes, not {len}")}),
                    )
                };
                return;
            }
        }
    }
}

/// The job's log can't be written here (a full disk, most likely): said in this daemon's log and
/// to the runner, which tries again later.
fn cannot_write_log(
    sh: &Shared,
    c: &Cluster,
    w: &mut crate::mesh::Stream,
    id: &str,
    from: &str,
    path: &Path,
    e: &std::io::Error,
) {
    let why = match path.parent().and_then(crate::workspace::disk_full) {
        Some(full) => format!("{e}; {full}"),
        None => e.to_string(),
    };
    sh.log.log(format!(
        "{id}: could not write the output log {from} sends to {}: {why}; {from} sends it again later",
        path.display()
    ));
    let _ = reply_line(
        w,
        serde_json::json!({"error": format!("could not write the output log on {}: {why}", c.node)}),
    );
}

/// A relative path from a copy frame, refused if it could leave the destination.
fn safe_rel(p: &str) -> Option<&Path> {
    let path = Path::new(p);
    path.components()
        .all(|c| matches!(c, Component::Normal(_)))
        .then_some(path)
}

/// A runner sends a job's kept files: stored under the job's `keep/`, replacing what was there.
pub(super) fn receive_keep(
    sh: &Shared,
    _c: &Cluster,
    from: &str,
    id: &str,
    tok: &str,
    stream: crate::mesh::Stream,
) {
    let ok = |sh: &Shared| held_by(&mut sh.lock(), id, from, tok).is_some();
    receive_files(sh, from, id, &ok, files::KEEP, stream);
}

/// A runner sends a job's crash reports, after its final record (they can be written seconds
/// after the crash): stored under the job's `crash/`, replacing what was there.
pub(super) fn receive_crash(
    sh: &Shared,
    from: &str,
    id: &str,
    tok: &str,
    stream: crate::mesh::Stream,
) {
    let ok = |sh: &Shared| {
        let mut st = sh.lock();
        let me = st.node.clone();
        held_by(&mut st, id, from, tok).is_some()
            || (owner_of(id) == me
                && st.job(id).is_some_and(|j| {
                    j.state.is_done() && j.node.as_deref() == Some(from) && token(&j) == Some(tok)
                }))
    };
    receive_files(sh, from, id, &ok, files::CRASH, stream);
}

/// Stores the files a runner sends (copy frames) under the job's `sub` directory, replacing what
/// was there; refused unless `ok` (the job ran there with that lease), asked when the stream
/// opens and again when the files are in, before they replace anything.
fn receive_files(
    sh: &Shared,
    from: &str,
    id: &str,
    ok: &dyn Fn(&Shared) -> bool,
    sub: &str,
    stream: crate::mesh::Stream,
) {
    let Ok(mut w) = stream.try_clone() else {
        return;
    };
    let not_ours = format!("{id} is not {from}'s job");
    if !ok(sh) {
        let _ = reply_line(&mut w, serde_json::json!({"error": not_ours}));
        return;
    }
    let dir = sh.paths.job_dir(id);
    // A directory of this stream's own: another run's stream (a lost run's files still on their
    // way while the next run delivers) may be writing beside it. It goes when the stream ends,
    // unless its files became the job's.
    // Those of daemons before this one (stopped while files came in) are left over. That
    // includes this process before an exec (a restart in place keeps the pid), so the name
    // carries a token drawn once per process image, not the pid, and an existing directory is
    // never written into.
    static PARTS: AtomicU64 = AtomicU64::new(0);
    static IMAGE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let image = IMAGE.get_or_init(|| crate::daemon::runner::new_token()[..16].to_string());
    let mine = format!("{sub}.part-{image}-");
    for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!("{sub}.part")) && !name.starts_with(&mine) {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
    let guard = PartDir(dir.join(format!("{mine}{}", PARTS.fetch_add(1, Ordering::Relaxed))));
    let part = guard.0.as_path();
    if let Err(e) = std::fs::create_dir_all(&dir).and_then(|()| std::fs::create_dir(part)) {
        let _ = reply_line(&mut w, serde_json::json!({"error": e.to_string()}));
        return;
    }
    if reply_line(&mut w, serde_json::json!({"ok": true})).is_err() {
        return;
    }
    let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
    let mut r = BufReader::new(stream);
    let mut files_n = 0u64;
    let fail = |w: &mut crate::mesh::Stream, e: String| {
        let _ = reply_line(w, serde_json::json!({"error": e}));
    };
    loop {
        let frame = match read_msg::<_, ServerResponse>(&mut r) {
            Ok(Some(f)) => f.resp,
            Ok(None) => return fail(&mut w, "the copy ended early".into()),
            Err(e) => return fail(&mut w, e.to_string()),
        };
        match frame {
            Response::CopyEntry {
                path,
                mode,
                dir: is_dir,
                len,
            } => {
                let Some(rel) = safe_rel(&path).or((path.is_empty()).then_some(Path::new("")))
                else {
                    return fail(&mut w, format!("refused path {path:?}"));
                };
                let to = part.join(rel);
                if is_dir {
                    if let Err(e) = std::fs::create_dir_all(&to) {
                        return fail(&mut w, e.to_string());
                    }
                    continue;
                }
                if let Some(p) = to.parent() {
                    let _ = std::fs::create_dir_all(p);
                }
                let written = std::fs::File::create(&to).and_then(|mut f| {
                    let n = std::io::copy(&mut (&mut r).take(len), &mut f)?;
                    if n != len {
                        return Err(std::io::ErrorKind::UnexpectedEof.into());
                    }
                    use std::os::unix::fs::PermissionsExt;
                    f.set_permissions(std::fs::Permissions::from_mode(mode & 0o777))
                });
                if let Err(e) = written {
                    return fail(&mut w, e.to_string());
                }
                files_n += 1;
            }
            Response::CopyDone => break,
            Response::Error { message } => return fail(&mut w, message),
            other => return fail(&mut w, format!("unexpected {other:?}")),
        }
    }
    if !ok(sh) {
        return fail(&mut w, not_ours);
    }
    let to = dir.join(sub);
    let _ = std::fs::remove_dir_all(&to);
    if let Err(e) = std::fs::rename(part, &to) {
        return fail(&mut w, e.to_string());
    }
    let _ = reply_line(&mut w, serde_json::json!({"files": files_n}));
}

/// [`receive_files`]' directory for one stream, removed with what is in it when the stream ends
/// (gone already when its files were taken).
struct PartDir(std::path::PathBuf);

impl Drop for PartDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A runner's mirror syncs from this machine: `git upload-pack` of this machine's own mirror of a
/// repository a job of this machine's names ([`fetchable`]). A dispatch syncs that mirror from
/// its source and pins its commits there until the job ends, so the mirror has every commit a
/// queued job needs. The source recorded at the latest dispatch may not: worktrees of one project
/// keep their submodules' git dirs apart, and a submodule commit made in one worktree is only in
/// its own. The source is served only if the mirror is gone.
pub(super) fn upload_pack(
    sh: &Shared,
    c: &Cluster,
    from: &str,
    repo_id: &str,
    stream: crate::mesh::Stream,
) {
    let taker = Taker::of(&c.mesh(), from);
    let source = {
        let st = sh.lock();
        fetchable(&st, &taker, repo_id)
            .then(|| c.cs().sources.get(repo_id).cloned())
            .flatten()
    };
    let Some(source) = source else {
        sh.log.log(format!(
            "cluster: {from} asked for repository {repo_id}, which no job queued here, or run by {from}, names"
        ));
        // git on the runner says `fatal: remote error: <this>`.
        let _ = crate::tunnel::refuse_upload_pack(
            stream,
            &format!(
                "{} has no job queued, or run by {from}, that names repository {repo_id}",
                c.node
            ),
        );
        return;
    };
    let mirror = sh.paths.mirror(repo_id);
    let dir = if mirror.is_dir() { mirror } else { source };
    if let Err(e) = c.opts.tunnel.serve_upload_pack(&dir, stream) {
        sh.log.log(format!(
            "cluster: serving {repo_id} ({}) to {from}: {e:#}",
            dir.display()
        ));
    }
}

/// Whether member `peer` may fetch repository `repo_id` from this machine: a job of this
/// machine's names it (as its repository, a submodule's or a carried one) and is queued, for a
/// member that may take it ([`may_take`]: a runner syncs its mirrors before it claims), or is held
/// by `peer`. Not any repository a dispatch here ever named.
fn fetchable(st: &State, peer: &Taker, repo_id: &str) -> bool {
    st.jobs.values().any(|j| {
        owner_of(&j.id) == st.node
            && match j.state {
                JobState::Queued => may_take(j, peer),
                JobState::Starting | JobState::Running => j.node.as_deref() == Some(&peer.name),
                _ => false,
            }
            && j.spec.repo.commits().any(|(id, _, _)| id == repo_id)
    })
}

/// Records where each repository of a dispatch comes from, for the runners' syncs.
pub(crate) fn dispatched(st: &mut State, c: &Cluster, repo: &RepoRef) {
    let mut cs = c.cs();
    let before = cs.sources.clone();
    for (repo_id, source, _) in repo.commits() {
        cs.sources.insert(repo_id.to_string(), source.to_path_buf());
    }
    if cs.sources != before {
        save(st, super::KV_SOURCES, &cs.sources);
    }
    cs.push_due = true;
    drop(cs);
    c.wake();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Facts;

    fn queued(pools: &[&str], signed: bool, on: &[(&str, &str)]) -> JobRecord {
        let tmp = tempfile::tempdir().unwrap();
        let key = crate::mesh::user::UserKey::load_or_create(&tmp.path().join("user.key"))
            .unwrap()
            .0;
        let mut spec = crate::types::JobSpec {
            key: None,
            labels: Default::default(),
            priority: 3,
            request: Default::default(),
            on: on
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            repo: RepoRef {
                id: "proj-1234abcd".into(),
                source: "/src/proj/.git".into(),
                commit: "0".repeat(40),
                branch: None,
                submodules: Vec::new(),
                carried: Vec::new(),
            },
            workspace: None,
            advance: false,
            cmd: vec!["true".into()],
            env: Default::default(),
            unset_env: Default::default(),
            timeout_ms: 60_000,
            hold_ms: None,
            keep: Vec::new(),
            rerun_if_lost: true,
            kill_on_stall: false,
            recipe: None,
            submitted_from: "a".into(),
            pools: pools.iter().map(|p| p.to_string()).collect(),
            auth: None,
        };
        if signed {
            spec = crate::jobauth::sign(&spec, &key, now());
        }
        crate::daemon::runner::new_record("a-1".into(), spec, now())
    }

    fn taker(pool: Option<&str>, users: bool) -> Taker {
        Taker {
            name: "b".into(),
            pool: pool.map(str::to_string),
            users,
        }
    }

    /// A member takes a queued job (sees it, fetches its repositories, is granted it) only in one
    /// of the job's pools; once the cluster has users, only a signed one.
    #[test]
    fn a_member_takes_only_jobs_for_its_pool_and_signed_once_there_are_users() {
        let cases = [
            (queued(&[], false, &[]), taker(Some("default"), false), None),
            (queued(&[], true, &[]), taker(Some("default"), true), None),
            (
                queued(&[], false, &[]),
                taker(Some("default"), true),
                Some(
                    "it was dispatched unsigned, before the cluster had users: only the machine it was dispatched on runs it",
                ),
            ),
            (
                queued(&[], true, &[]),
                taker(Some("cloud"), true),
                Some("it runs in pool default, and b is in pool cloud"),
            ),
            (
                queued(&["cloud"], true, &[]),
                taker(Some("cloud"), true),
                None,
            ),
            (
                queued(&["cloud", "gpu"], true, &[]),
                taker(Some("default"), true),
                Some("it runs in pools cloud, gpu, and b is in pool default"),
            ),
            (
                queued(&["default", "cloud"], true, &[]),
                taker(Some("default"), true),
                None,
            ),
            (
                queued(&[], true, &[]),
                taker(None, true),
                Some("b is not a member"),
            ),
        ];
        for (i, (j, t, want)) in cases.iter().enumerate() {
            assert_eq!(why_not_take(j, t).as_deref(), *want, "case {i}");
            assert_eq!(may_take(j, t), want.is_none(), "case {i}");
        }
    }

    /// A claim's `--on` is checked against the claimant's last reported facts, under its name in
    /// the cluster; with none reported yet, it isn't.
    #[test]
    fn a_claims_on_is_checked_against_the_claimants_facts() {
        let facts = Facts {
            node: "liar".into(),
            os: "linux".into(),
            ..Default::default()
        };
        let j = queued(&[], true, &[("os", "linux")]);
        assert_eq!(unmet_on(&j, "b", Some(&facts)), None);
        assert_eq!(unmet_on(&j, "b", None), None);
        let j = queued(&[], true, &[("os", "macos")]);
        assert_eq!(
            unmet_on(&j, "b", Some(&facts)).as_deref(),
            Some("needs os=macos (b: linux)")
        );
        let j = queued(&[], true, &[("node", "b")]);
        assert_eq!(unmet_on(&j, "b", Some(&facts)), None);
        let j = queued(&[], true, &[("node", "liar")]);
        assert!(unmet_on(&j, "b", Some(&facts)).is_some());
    }
}
