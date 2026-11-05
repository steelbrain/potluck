//! The runner's side: which of the other machines' queued jobs this machine may start (commits in
//! its mirrors, locality), mirror syncs from the owners, claims, and the courier that reports a
//! job to its owner and delivers its log and kept files.

use super::peer::{LogFrame, PeerRequest, PeerResponse, StreamHeader, node_of_ws, now, owner_of};
use super::views::snap_of;
use super::{
    CState, Cluster, GiveBack, KV_FETCHED, KV_GIVEBACKS, KV_OUTBOX, KV_WS_OWNERS, Outbox, save,
};
use crate::daemon::state::{MachineSnap, State};
use crate::daemon::{Shared, files, runner};
use crate::gitinfo::short;
use crate::sched::{Load, MachineView, Queued, Running};
use crate::types::{
    JobId, JobRecord, JobSpec, JobState, Millis, NodeName, RepoRef, Usage, WorkspaceState,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::Duration;

const CLAIM_TIMEOUT: Duration = Duration::from_secs(15);
const CALL_TIMEOUT: Duration = Duration::from_secs(15);
/// A refused claim isn't tried again before this long.
const REJECTED_MS: u64 = 2_000;
/// A job this machine refused to run is kept from its claims this long at least, though no report
/// lists it queued: from the grant until the owner takes it back and reports it again, it is
/// starting there, in no queue.
const REFUSED_MS: u64 = 60_000;
/// The most output one log frame carries, and one pass of the courier sends.
const LOG_FRAME: u64 = 1 << 20;
const LOG_PASS: u64 = 16 << 20;
/// A quiet log stream sends a ping this often, so the owner knows it is alive.
const LOG_PING_MS: u64 = 5_000;
/// Retries back off up to this.
const MAX_BACKOFF_MS: u64 = 10_000;
/// How often pins of commits fetched for jobs that went elsewhere are dropped.
const FETCHED_CLEANUP_MS: u64 = 10_000;
/// Mirror syncs from one owner at once, at most. Each runs `potluck _tunnel` processes (one at a
/// time), and each of those dials the owner: the owner closes connections from one address beyond
/// 8 still in their handshake, and this daemon's own dials to it take up to 4 of those.
const MAX_SYNCS_PER_OWNER: usize = 3;

/// Every (repository id, commit) a dispatch names ([`RepoRef::commits`]).
fn commits(repo: &RepoRef) -> Vec<(String, String)> {
    repo.commits()
        .map(|(id, _, commit)| (id.to_string(), commit.to_string()))
        .collect()
}

/// A job of another machine's queue as the scheduler sees it here.
pub(super) fn queued_of(st: &State, j: &JobRecord) -> Queued {
    Queued {
        job: j.id.clone(),
        priority: j.spec.priority,
        passes: j.passes,
        submitted_at: j.submitted_at,
        request: j.spec.request.clone(),
        on: j.spec.on.clone(),
        timeout_ms: j.spec.timeout_ms,
        workspace: j.spec.workspace.as_ref().map(|w| {
            let node = node_of_ws(w).to_string();
            let busy = node == st.node && st.ws_busy(w);
            (w.clone(), node, busy)
        }),
        pools: j.spec.pools.clone(),
        hold_line: j.spec.hold_line,
    }
}

/// Whether `view` could start `q` by `now + wait`, if the jobs there end by their run limits.
fn could_start_soon(view: &MachineView, q: &Queued, now: Millis, wait: u64) -> bool {
    let until = now.saturating_add(wait);
    let running: Vec<Running> = view
        .running
        .iter()
        .filter(|r| r.deadline > until)
        .cloned()
        .collect();
    let later = MachineView {
        running: &running,
        ..view.clone()
    };
    crate::sched::feasible(&later, q).is_ok() && Load::new(now, &later).grant(q).is_some()
}

/// Workspaces here that other machines' queued jobs are pinned to are never removed: counted
/// from every member's last report, the ones away too (asleep, updating): its queued jobs still
/// need their workspaces when it is back. A report's queue holds only its machine's own jobs
/// ([`super::views::received`]): a job keeps a workspace only as its owner reports it. Run as
/// each report arrives, so a `release` right after counts the queue that report shows.
pub(super) fn pinned_here(st: &mut State, cs: &CState) {
    let mut pending: BTreeMap<String, u32> = BTreeMap::new();
    for r in cs.reports.values() {
        for j in &r.report.queue {
            if let Some(w) = &j.spec.workspace
                && node_of_ws(w) == st.node
            {
                *pending.entry(w.clone()).or_default() += 1;
            }
        }
    }
    if st.extra_pending != pending {
        st.extra_pending = pending;
        st.refresh_pending();
    }
}

/// Called by the loop while it plans (the state locked): adds the other machines' queued jobs
/// this machine could start (those whose commits its mirrors hold; missing ones start a sync),
/// and leaves out jobs a warm member should take first (locality).
pub(crate) fn adjust_queue(sh: &Shared, st: &State, queue: &mut Vec<Queued>, now: Millis) {
    let Some(c) = sh.cluster() else {
        return;
    };
    let mesh = c.mesh();
    let members = mesh.members();
    let alive: BTreeSet<NodeName> = members
        .iter()
        .filter(|m| c.up(m) && !m.this_machine)
        .map(|m| m.name.clone())
        .collect();
    // Each member's pool, from its admission.
    let pools: BTreeMap<NodeName, String> = members.into_iter().map(|m| (m.name, m.pool)).collect();
    // Owners this machine takes jobs from: back from being away, it first compares membership
    // records with the other members (an owner may have been removed meanwhile).
    let owners: BTreeSet<&NodeName> = alive.iter().filter(|n| mesh.caught_up_for(n)).collect();
    let mut cs = c.cs();
    let cs = &mut *cs;
    cs.candidates.clear();
    cs.locality.clear();
    cs.catching_up = alive
        .iter()
        .filter(|n| !owners.contains(n))
        .cloned()
        .collect();
    if cs.out.is_some() {
        cs.unsynced.clear();
        return;
    }
    let pool_here = c.pool_here();
    let local: MachineSnap = match st.local_snap(&pool_here) {
        Some(s) => s,
        None => return,
    };
    // Only jobs a user this machine recognizes signed are taken from other machines, with users
    // or without (none then): a job queued before the cluster had users runs only on the machine
    // it was dispatched on. A grant is checked whole before the job runs ([`verify_grant`]).
    let view = local.view();
    for s in cs.syncs.values_mut() {
        s.wants.clear();
    }
    cs.unsynced.clear();
    let boot = c.boot();
    cs.rejected.retain(|_, until| *until > boot);

    // Every queued job (own first) with its repository, to look at locality.
    let mut repos: BTreeMap<JobId, String> = queue
        .iter()
        .filter_map(|q| Some((q.job.clone(), st.jobs.get(&q.job)?.spec.repo.id.clone())))
        .collect();
    // A grant is news to every owner's report made after it.
    let reports = &cs.reports;
    cs.granted_after.retain(|id, n| {
        reports
            .get(owner_of(id))
            .is_some_and(|r| !super::views::later_numbers(*n, (r.report.started_at, r.report.seq)))
    });
    let mut added = Vec::new();
    for (owner, r) in &cs.reports {
        if !owners.contains(owner) || r.report.removed {
            continue;
        }
        for rec in &r.report.queue {
            let id = &rec.id;
            // A job this machine ran before and its owner queued again (this machine was lost
            // meanwhile) may come back here, once that run is over and settled with the owner,
            // from a report the owner made after its grant here: the one that run was planned
            // from still lists it as queued.
            let held = st.jobs.get(id).is_some_and(|j| !j.state.is_done())
                || cs.granted_after.contains_key(id);
            if owner_of(id) != owner
                || held
                || cs.outbox.contains_key(id)
                || cs.rejected.contains_key(id)
                || cs.refused.contains_key(id)
            {
                continue;
            }
            let signed_by_user = || {
                rec.spec
                    .auth
                    .as_ref()
                    .is_some_and(|a| mesh.recognized_user(&a.key))
            };
            if !signed_by_user() {
                continue;
            }
            let q = queued_of(st, rec);
            if let Some((w, node, _)) = &q.workspace
                && (*node != st.node || st.live_ws(w).is_none())
            {
                continue;
            }
            if sh.parts.scheduler.feasible(&view, &q).is_err() {
                continue;
            }
            let missing: Vec<(String, String)> = commits(&rec.spec.repo)
                .into_iter()
                .filter(|rc| !cs.present.contains(rc))
                .collect();
            if let Some((repo, commit)) = missing.first() {
                let reason = match cs
                    .syncs
                    .get(&(owner.clone(), repo.clone()))
                    .and_then(|s| s.error.clone())
                {
                    Some(e) => format!(
                        "its mirror of {repo} lacks commit {} and the last sync from {owner} failed: {e}",
                        short(commit)
                    ),
                    None => format!(
                        "its mirror of {repo} lacks commit {} yet: syncing from {owner}",
                        short(commit)
                    ),
                };
                cs.unsynced.insert(id.clone(), reason);
                for (repo, commit) in missing {
                    cs.syncs
                        .entry((owner.clone(), repo))
                        .or_default()
                        .wants
                        .entry(commit)
                        .or_default()
                        .insert(id.clone());
                }
                continue;
            }
            repos.insert(id.clone(), rec.spec.repo.id.clone());
            cs.candidates
                .insert(id.clone(), (owner.clone(), rec.clone()));
            added.push(q);
        }
    }
    queue.extend(added);
    // A job refused here stays refused while its owner queues it, and for REFUSED_MS after the
    // refusal whether or not a report lists it.
    let queued: BTreeSet<&JobId> = cs
        .reports
        .values()
        .flat_map(|r| r.report.queue.iter().map(|j| &j.id))
        .collect();
    cs.refused
        .retain(|id, (_, at)| queued.contains(id) || boot.saturating_sub(*at) < REFUSED_MS);

    // Locality: a machine without a workspace of the repository leaves the job to a live member
    // with one that could start it within the wait, for that long. Not its own job that no user
    // signed: no other member takes that.
    let wait = c.opts.locality_wait_ms;
    let warm_here: BTreeSet<String> = st.records().into_iter().map(|w| w.repo_id).collect();
    let mut seen = BTreeSet::new();
    queue.retain(|q| {
        if q.workspace.is_some() || st.jobs.get(&q.job).is_some_and(|j| j.spec.auth.is_none()) {
            return true;
        }
        let Some(repo) = repos.get(&q.job) else {
            return true;
        };
        if warm_here.contains(repo) {
            return true;
        }
        let warm: Vec<&str> = cs
            .reports
            .iter()
            .filter(|(n, r)| {
                alive.contains(*n)
                    && !r.report.removed
                    && r.report.workspaces.iter().any(|w| w.repo_id == *repo)
                    && pools.get(*n).is_some_and(|pool| {
                        could_start_soon(&snap_of(&r.report, pool).view(), q, now, wait)
                    })
            })
            .map(|(n, _)| n.as_str())
            .collect();
        if warm.is_empty() {
            return true;
        }
        seen.insert(q.job.clone());
        let since = *cs.cold_since.entry(q.job.clone()).or_insert(boot);
        let waited = boot.saturating_sub(since);
        if waited >= wait {
            return true;
        }
        let (whom, has) = match warm.as_slice() {
            [one] => (one.to_string(), "has"),
            many => (many.join(" or "), "have"),
        };
        cs.locality.insert(
            q.job.clone(),
            format!(
                "{whom}, which {has} a workspace of its repository, for up to {} more (locality)",
                crate::types::fmt_ms(wait - waited)
            ),
        );
        false
    });
    cs.cold_since.retain(|j, _| seen.contains(j));
    cs.candidates
        .retain(|j, _| queue.iter().any(|q| q.job == *j));
}

/// Before claiming a start the plan made: puts another machine's job in the state as queued, so
/// the claim can reserve it like an own job. False for own jobs.
pub(crate) fn place(c: &Cluster, st: &mut State, id: &str) -> bool {
    let cs = c.cs();
    let Some((_, rec)) = cs.candidates.get(id) else {
        return false;
    };
    let mut r = rec.clone();
    // The key belongs to the owner's dispatch; here it would collide with own keys.
    r.spec.key = None;
    r.state = JobState::Queued;
    r.node = None;
    r.workspace = None;
    r.lease = None;
    r.handle = None;
    r.started_at = None;
    st.jobs.insert(id.to_string(), r);
    true
}

/// Undoes [`place`] for a start that could not be reserved after all.
pub(crate) fn unplace(st: &mut State, id: &str) {
    if st
        .jobs
        .get(id)
        .is_some_and(|j| !matches!(j.state, JobState::Starting | JobState::Running))
    {
        st.jobs.remove(id);
        if let Err(e) = st.store.delete_job(id) {
            st.log.log(format!("store: {e:#}"));
        }
    }
}

/// After a plan, without the state lock: claims the other machines' jobs it starts and sends the
/// passes to their owners.
pub(crate) fn after_plan(sh: &Arc<Shared>, claims: Vec<JobId>, passed: Vec<JobId>) {
    let Some(c) = sh.cluster() else {
        return;
    };
    let mut by_owner: BTreeMap<String, Vec<JobId>> = BTreeMap::new();
    for id in passed {
        by_owner
            .entry(owner_of(&id).to_string())
            .or_default()
            .push(id);
    }
    // Passes go with the first claim to their owner, so they count before its grant (and before
    // any later claim of a passed job); the others go on the next tick.
    let claims: Vec<(JobId, Vec<JobId>)> = claims
        .into_iter()
        .map(|id| {
            let passed = by_owner.remove(owner_of(&id)).unwrap_or_default();
            (id, passed)
        })
        .collect();
    if !by_owner.is_empty() {
        let mut cs = c.cs();
        for (owner, ids) in by_owner {
            cs.passes.entry(owner).or_default().extend(ids);
        }
        drop(cs);
        c.wake();
    }
    for (id, passed) in claims {
        sh.spawn_worker(&format!("claim-{id}"), move |sh| claim(sh, &id, passed));
    }
}

/// Claims a reserved job from its owner; starts it when granted, undoes the reservation when not.
fn claim(sh: &Arc<Shared>, id: &str, passed: Vec<JobId>) {
    let Some(c) = sh.cluster() else {
        return;
    };
    let owner = owner_of(id).to_string();
    // No claim after all: the passes go on the next tick.
    let keep_passes = |passed: Vec<JobId>| {
        if !passed.is_empty() {
            c.cs()
                .passes
                .entry(owner.clone())
                .or_default()
                .extend(passed);
        }
    };
    // The queue entry claimed, which the reservation was made from.
    let Some((repo, lease, ws, claimed)) = ({
        let st = sh.lock();
        st.jobs.get(id).and_then(|j| {
            Some((
                j.spec.repo.clone(),
                j.lease.clone()?,
                j.workspace.clone()?,
                j.spec.clone(),
            ))
        })
    }) else {
        keep_passes(passed);
        return;
    };
    if let Err(e) = runner::pin_all(sh, &repo, id) {
        sh.log
            .log(format!("{id}: not claimed: could not pin its commits: {e}"));
        // The mirrors may have lost them since (git gc, a reclone): the next plan checks them
        // again and syncs what is missing.
        {
            let mut cs = c.cs();
            for rc in commits(&repo) {
                cs.present.remove(&rc);
            }
        }
        keep_passes(passed);
        undo(sh, &c, id, None);
        return;
    }
    let token = lease.token.clone();
    let r = c.call(
        &owner,
        &PeerRequest::Claim {
            job: id.to_string(),
            lease: Box::new(lease),
            workspace: ws.clone(),
            passed,
        },
        CLAIM_TIMEOUT,
    );
    match r {
        Ok(PeerResponse::Granted { job, reported }) => {
            if let Err(why) = verify_grant(sh, &c, id, &job, &claimed) {
                sh.log.log(format!(
                    "WARNING: {id}: granted by {owner}, but not run here: {why}"
                ));
                c.cs()
                    .refused
                    .insert(id.to_string(), (why.clone(), c.boot()));
                let back = format!("{} did not run it: {why}", c.node);
                undo(sh, &c, id, Some(back));
                return;
            }
            {
                let mut guard = sh.lock();
                let st = &mut *guard;
                let mut cs = c.cs();
                if let Some(j) = st.jobs.get_mut(id) {
                    j.spec = job.spec;
                    j.spec.key = None;
                    j.passes = job.passes;
                    j.submitted_at = job.submitted_at;
                    // The owner's warnings about its sources, then this machine's.
                    j.notes = job.notes;
                    j.notes.extend(cs.sync_notes.remove(id).unwrap_or_default());
                }
                st.save_job(id);
                cs.outbox.insert(
                    id.to_string(),
                    Outbox {
                        owner: owner.clone(),
                        token,
                        log_done: false,
                        keep_done: false,
                        keep_since: None,
                        end_done: false,
                        orphaned: false,
                        log_at: None,
                    },
                );
                save(st, KV_OUTBOX, &cs.outbox);
                cs.ws_owners.insert(ws, owner.clone());
                let live: BTreeSet<String> = st.records().into_iter().map(|w| w.id).collect();
                cs.ws_owners.retain(|w, _| live.contains(w));
                save(st, KV_WS_OWNERS, &cs.ws_owners);
                cs.candidates.remove(id);
                if let Some(n) = reported {
                    cs.granted_after.insert(id.to_string(), n);
                }
            }
            sh.log.log(format!("{id}: claimed from {owner}"));
            c.wake();
            // What an earlier run of the job here left (its log, kept files) is not this run's.
            let dir = sh.paths.job_dir(id);
            if let Err(e) = std::fs::remove_dir_all(&dir)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                sh.log.log(format!("{id}: clearing {}: {e}", dir.display()));
            }
            runner::start_job(sh, id);
        }
        Ok(PeerResponse::Refused { reason }) => {
            sh.log
                .log(format!("{id}: claim refused by {owner}: {reason}"));
            undo(sh, &c, id, None);
        }
        Ok(other) => {
            sh.log.log(format!(
                "{id}: claim: an unexpected answer from {owner}: {other:?}"
            ));
            undo(
                sh,
                &c,
                id,
                Some("its claim got an unexpected answer".into()),
            );
        }
        Err(e) => {
            sh.log.log(format!("{id}: claim: {e}"));
            // The owner may have granted it before the answer was lost: hand it back.
            let definite = e.starts_with(&format!("{owner}: "));
            undo(
                sh,
                &c,
                id,
                (!definite).then(|| "its claim's answer was lost".to_string()),
            );
        }
    }
}

/// What a runner checks of job `id` its owner granted, before it runs it: the spec granted asks
/// for what the queue entry it claimed asked, the resources reserved here (`request`), `on` and
/// `workspace` (`claimed`); a user this machine recognizes signed it, for its owner, and the spec
/// granted is the one signed as its owner holds it ([`crate::jobauth::verify_grant`]: so none is
/// run until the cluster has users); and it runs in this machine's pool, on this machine's facts
/// (`--on`).
fn verify_grant(
    sh: &Shared,
    c: &Cluster,
    id: &str,
    job: &JobRecord,
    claimed: &JobSpec,
) -> Result<(), String> {
    if job.id != id {
        return Err(format!("the grant is of job {}", job.id));
    }
    let granted = &job.spec;
    let differs: Vec<&str> = [
        ("request", granted.request != claimed.request),
        ("on", granted.on != claimed.on),
        ("workspace", granted.workspace != claimed.workspace),
    ]
    .into_iter()
    .filter_map(|(field, differs)| differs.then_some(field))
    .collect();
    if !differs.is_empty() {
        return Err(format!(
            "its {} is not what the queue entry it was claimed from had",
            differs.join(", ")
        ));
    }
    let mesh = c.mesh();
    let signed =
        crate::jobauth::verify_grant(&job.spec, owner_of(id), |k| mesh.recognized_user(k))?;
    let pool = c.pool_here();
    if !signed.runs_in(&pool) {
        return Err(format!(
            "it runs in {}, and this machine is in pool {pool}",
            crate::types::fmt_pools(&signed.pools)
        ));
    }
    let facts = sh.lock().local().facts.clone();
    if let Some(unmet) = facts.unmet(&signed.on) {
        return Err(format!("it {unmet}"));
    }
    Ok(())
}

/// Undoes a reservation whose claim did not go through: the record, its lease, the workspace it
/// was given. With `give_back`, the owner is told to queue the job again.
fn undo(sh: &Arc<Shared>, c: &Cluster, id: &str, give_back: Option<String>) {
    let now = now();
    let repo = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let run = st.runs.remove(id);
        st.save_run(id);
        let Some(j) = st.jobs.remove(id) else {
            return;
        };
        if let Err(e) = st.store.delete_job(id) {
            st.log.log(format!("store: {e:#}"));
        }
        if let Some(w) = &j.workspace
            && let Some(ws) = st.workspaces.get_mut(w)
        {
            if run.as_ref().is_some_and(|r| r.new_workspace) {
                // Never created: only its record goes.
                ws.state = WorkspaceState::Removed;
            } else if ws.state == WorkspaceState::Running {
                ws.state = if ws.held_until > now {
                    WorkspaceState::Held
                } else {
                    WorkspaceState::Idle
                };
            }
            st.save_ws(w);
        }
        st.refresh_pending();
        let mut cs = c.cs();
        cs.rejected.insert(
            id.to_string(),
            c.boot().saturating_add(REJECTED_MS.max(c.opts.pull_ms * 2)),
        );
        if let Some(message) = give_back
            && let Some(l) = &j.lease
        {
            cs.givebacks.push(GiveBack {
                owner: owner_of(id).to_string(),
                job: id.to_string(),
                token: l.token.clone(),
                message,
            });
            save(st, KV_GIVEBACKS, &cs.givebacks);
        }
        // Commits fetched for it stay pinned until it has gone elsewhere.
        (!cs.fetched.contains_key(id)).then_some(j.spec.repo)
    };
    if let Some(repo) = repo {
        runner::unpin_all(sh, &repo, id);
    }
    c.wake();
    sh.kick();
}

/// When the daemon starts: other machines' jobs that were starting here and don't start again go
/// back to their owners: those with no record of how far their start got, or whose workspace is
/// gone, and those claimed but not granted yet (no outbox: what this machine holds of the job is
/// its queue entry, without the command and environment the grant brings).
pub(super) fn give_back_starting(
    st: &mut State,
    cs: &mut CState,
    unpins: &mut Vec<(RepoRef, JobId)>,
) {
    let me = st.node.clone();
    let now = now();
    let ids: Vec<JobId> = st
        .jobs
        .values()
        .filter(|j| owner_of(&j.id) != me && j.state == JobState::Starting)
        .filter(|j| !st.runs.contains_key(&j.id) || !cs.outbox.contains_key(&j.id))
        .map(|j| j.id.clone())
        .collect();
    for id in ids {
        let Some(j) = st.jobs.remove(&id) else {
            continue;
        };
        if let Err(e) = st.store.delete_job(&id) {
            st.log.log(format!("store: {e:#}"));
        }
        if st.runs.remove(&id).is_some() {
            st.log.log(format!(
                "{id}: was claimed from {} and not granted yet",
                owner_of(&id)
            ));
        }
        st.save_run(&id);
        // A workspace of its own was never seeded: removed as a seeding one with no job in it is
        // ([`crate::daemon::runner::adopt`]). One it was pinned to is free again.
        if let Some(w) = &j.workspace
            && !st.ws_busy(w)
            && let Some(ws) = st.workspaces.get_mut(w)
            && ws.state == WorkspaceState::Running
        {
            ws.state = if ws.held_until > now {
                WorkspaceState::Held
            } else {
                WorkspaceState::Idle
            };
            st.save_ws(w);
        }
        if let Some(l) = &j.lease {
            cs.givebacks.push(GiveBack {
                owner: owner_of(&id).to_string(),
                job: id.clone(),
                token: l.token.clone(),
                message: format!("{me}'s daemon restarted while it was starting"),
            });
        }
        cs.outbox.remove(&id);
        st.log.log(format!(
            "{id}: was starting for {}; handed back",
            owner_of(&id)
        ));
        unpins.push((j.spec.repo, id));
    }
    save(st, KV_GIVEBACKS, &cs.givebacks);
    save(st, KV_OUTBOX, &cs.outbox);
}

/// The owner asks to cancel a job running here.
pub(super) fn cancel_here(sh: &Shared, from: &str, job: &str, token: &str) {
    let mut st = sh.lock();
    let ours = st.jobs.get(job).is_some_and(|j| {
        owner_of(job) == from && j.lease.as_ref().is_some_and(|l| l.token == token)
    });
    if !ours {
        return;
    }
    if let Some(run) = st.runs.get_mut(job)
        && !run.cancel
    {
        run.cancel = true;
        st.save_run(job);
        st.log.log(format!("{job}: cancel asked by {from}"));
    }
    drop(st);
    sh.kick();
}

/// Stops a job this machine runs for another, without telling its owner (it isn't ours anymore).
fn stop_run(sh: &Shared, id: &str) {
    let mut st = sh.lock();
    if let Some(run) = st.runs.get_mut(id)
        && !run.cancel
    {
        run.cancel = true;
        st.save_run(id);
        st.log.log(format!(
            "{id}: stopping: its owner {} has it elsewhere now (or is gone)",
            owner_of(id)
        ));
    }
    drop(st);
    sh.kick();
}

fn orphan(sh: &Shared, c: &Cluster, id: &str) {
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let mut cs = c.cs();
        if let Some(ob) = cs.outbox.get_mut(id) {
            ob.orphaned = true;
        }
        save(st, KV_OUTBOX, &cs.outbox);
    }
    stop_run(sh, id);
}

/// An owner left the cluster (or this machine did): the jobs run for it stop.
pub(super) fn owner_gone(sh: &Shared, c: &Cluster, owner: &str) {
    let ids: Vec<JobId> = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let mut cs = c.cs();
        let ids: Vec<JobId> = cs
            .outbox
            .iter()
            .filter(|(_, o)| o.owner == owner && !o.orphaned)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &ids {
            if let Some(o) = cs.outbox.get_mut(id) {
                o.orphaned = true;
            }
        }
        save(st, KV_OUTBOX, &cs.outbox);
        cs.givebacks.retain(|g| g.owner != owner);
        save(st, KV_GIVEBACKS, &cs.givebacks);
        cs.passes.remove(owner);
        cs.syncs.retain(|(o, _), _| o != owner);
        ids
    };
    for id in ids {
        stop_run(sh, &id);
    }
}

/// Something to do on a worker thread, after the locks are let go.
type Work = Box<dyn FnOnce(&Arc<Shared>) + Send>;

/// The runner's periodic work: passes, give-backs, reconciling, couriers, syncs, the workspaces
/// other machines' queued jobs are pinned to, and fetched pins no longer needed; and forgetting
/// what it knew of mirrors, syncs and owners that no job and no member needs any more.
/// The runner's periodic work (`now` on the boot clock).
pub(super) fn tick(sh: &Arc<Shared>, c: &Arc<Cluster>, now: u64) {
    let all = c.mesh().members();
    let members: BTreeSet<NodeName> = all.iter().map(|m| m.name.clone()).collect();
    let alive: BTreeSet<NodeName> = all
        .into_iter()
        .filter(|m| c.up(m) && !m.this_machine)
        .map(|m| m.name)
        .collect();
    let mut work: Vec<(String, Work)> = Vec::new();
    {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let mut cs = c.cs();
        let cs = &mut *cs;

        // Passes, best effort.
        for (owner, jobs) in std::mem::take(&mut cs.passes) {
            if !alive.contains(&owner) {
                continue;
            }
            let c2 = c.clone();
            work.push((
                format!("passed-{owner}"),
                Box::new(move |_| {
                    let _ = c2.call(&owner, &PeerRequest::Passed { jobs }, CALL_TIMEOUT);
                }),
            ));
        }

        // Give-backs, until the owner has them.
        for g in cs.givebacks.clone() {
            let key = format!("giveback:{}", g.job);
            if !alive.contains(&g.owner) || !cs.sending.insert(key.clone()) {
                continue;
            }
            let c2 = c.clone();
            work.push((
                format!("giveback-{}", g.job),
                Box::new(move |sh| {
                    let r = c2.call(
                        &g.owner,
                        &PeerRequest::GiveBack {
                            job: g.job.clone(),
                            token: g.token.clone(),
                            message: g.message.clone(),
                        },
                        CALL_TIMEOUT,
                    );
                    let mut guard = sh.lock();
                    let st = &mut *guard;
                    let mut cs = c2.cs();
                    cs.sending.remove(&key);
                    if r.is_ok() {
                        cs.givebacks
                            .retain(|x| x.job != g.job || x.token != g.token);
                        save(st, KV_GIVEBACKS, &cs.givebacks);
                    }
                }),
            ));
        }

        // Reconcile with each member up: when it comes up, and now and then. An owner learns
        // from it which of its jobs this machine holds; one it granted here a while ago that the
        // list leaves out goes back to its queue, also when this machine holds nothing else of
        // that owner's.
        let mut held: BTreeMap<NodeName, Vec<(JobId, String)>> = BTreeMap::new();
        for (id, o) in &cs.outbox {
            // A job whose end the owner took is the owner's again (only its crash reports are
            // still on their way).
            if !o.orphaned && !o.end_done {
                held.entry(o.owner.clone())
                    .or_default()
                    .push((id.clone(), o.token.clone()));
            }
        }
        for owner in cs.reconcile_due.iter().chain(&alive) {
            held.entry(owner.clone()).or_default();
        }
        for (owner, jobs) in held {
            let due = cs.reconcile_due.contains(&owner)
                || cs
                    .reconciled_at
                    .get(&owner)
                    .is_none_or(|t| now.saturating_sub(*t) >= c.opts.reconcile_ms);
            let key = format!("reconcile:{owner}");
            if !due || !alive.contains(&owner) || !cs.sending.insert(key.clone()) {
                continue;
            }
            cs.reconcile_due.remove(&owner);
            cs.reconciled_at.insert(owner.clone(), now);
            let c2 = c.clone();
            // The owner's word stops only its own jobs, those this list names: an answer naming
            // another machine's job, or one of this machine's own, is ignored.
            let listed: BTreeSet<JobId> = jobs.iter().map(|(id, _)| id.clone()).collect();
            work.push((
                format!("reconcile-{owner}"),
                Box::new(move |sh| {
                    let r = c2.call(&owner, &PeerRequest::Reconcile { jobs }, CALL_TIMEOUT);
                    if let Ok(PeerResponse::Reconciled { drop }) = r {
                        let (drop, not_its): (Vec<JobId>, Vec<JobId>) =
                            drop.into_iter().partition(|id| listed.contains(id));
                        if !not_its.is_empty() {
                            sh.log.log(format!(
                                "cluster: {owner} answered a reconcile naming jobs that are not its own held here: {}; ignored",
                                not_its.join(", ")
                            ));
                        }
                        for id in drop {
                            // Its end reached the owner after this list left: nothing runs here.
                            if c2.cs().outbox.get(&id).is_some_and(|o| o.end_done) {
                                continue;
                            }
                            sh.log.log(format!(
                                "{id}: {owner} says it is no longer this machine's job"
                            ));
                            orphan(sh, &c2, &id);
                        }
                    }
                    // Only now: a reconcile sent before the jobs above are marked orphaned
                    // (one due since the owner came up) would list them again.
                    c2.cs().sending.remove(&key);
                }),
            ));
        }

        // A courier for every job held for an owner.
        for id in cs.outbox.keys() {
            if cs.couriers.insert(id.clone()) {
                let (c2, id) = (c.clone(), id.clone());
                work.push((
                    format!("courier-{id}"),
                    Box::new(move |sh| {
                        courier(sh, &c2, &id);
                        c2.cs().couriers.remove(&id);
                    }),
                ));
            }
        }

        // Mirror syncs the latest plan asked for, a few from one owner at a time.
        let mut syncing: BTreeMap<NodeName, usize> = BTreeMap::new();
        for ((owner, _), s) in &cs.syncs {
            if s.running {
                *syncing.entry(owner.clone()).or_default() += 1;
            }
        }
        for ((owner, repo), s) in cs.syncs.iter_mut() {
            if s.wants.is_empty() || s.running || now < s.next_at || !alive.contains(owner) {
                continue;
            }
            let n = syncing.entry(owner.clone()).or_default();
            if *n >= MAX_SYNCS_PER_OWNER {
                continue;
            }
            *n += 1;
            s.running = true;
            let wants = s.wants.clone();
            let (c2, owner, repo) = (c.clone(), owner.clone(), repo.clone());
            work.push((
                format!("sync-{repo}"),
                Box::new(move |sh| sync(sh, &c2, &owner, &repo, wants)),
            ));
        }

        pinned_here(st, cs);

        // Commits fetched for jobs that went elsewhere: unpinned.
        if now.saturating_sub(cs.fetched_cleanup_at) >= FETCHED_CLEANUP_MS {
            cs.fetched_cleanup_at = now;
            let queued: BTreeSet<&str> = cs
                .reports
                .values()
                .flat_map(|r| r.report.queue.iter().map(|j| j.id.as_str()))
                .collect();
            let gone: Vec<JobId> = cs
                .fetched
                .keys()
                .filter(|id| !queued.contains(id.as_str()) && !st.jobs.contains_key(*id))
                .cloned()
                .collect();
            cs.sync_notes
                .retain(|id, _| queued.contains(id.as_str()) || st.jobs.contains_key(id));
            // The commits known to be in the mirrors that a queued job or one held here names
            // (others are looked up again if a job comes for them), the syncs some job still
            // wants, and members' reconciliations.
            let wanted: BTreeSet<(String, String)> = cs
                .reports
                .values()
                .flat_map(|r| r.report.queue.iter())
                .chain(st.jobs.values().filter(|j| !j.state.is_done()))
                .flat_map(|j| commits(&j.spec.repo))
                .collect();
            cs.present.retain(|rc| wanted.contains(rc));
            cs.syncs.retain(|_, s| s.running || !s.wants.is_empty());
            cs.reconciled_at.retain(|n, _| members.contains(n));
            if !gone.is_empty() {
                let unpins: Vec<(JobId, BTreeSet<String>)> = gone
                    .iter()
                    .filter_map(|id| Some((id.clone(), cs.fetched.remove(id)?)))
                    .collect();
                save(st, KV_FETCHED, &cs.fetched);
                work.push((
                    "unpin-fetched".into(),
                    Box::new(move |sh| {
                        for (id, repos) in unpins {
                            for repo in repos {
                                let _ = sh.parts.workspaces.unpin(&repo, &id);
                            }
                        }
                    }),
                ));
            }
        }
    }
    for (name, f) in work {
        sh.spawn_worker(&name, f);
    }
}

/// Syncs this machine's mirror of `repo` from `owner` through the tunnel, then fetches and pins
/// (under the job's id) any wanted commit that came with no branch.
fn sync(
    sh: &Arc<Shared>,
    c: &Arc<Cluster>,
    owner: &str,
    repo: &str,
    wants: BTreeMap<String, BTreeSet<JobId>>,
) {
    let w = &sh.parts.workspaces;
    let url = c.opts.tunnel.fetch_url(&sh.helpers.path(), owner, repo);
    let mut present = Vec::new();
    let mut missing: Vec<(String, BTreeSet<JobId>)> = Vec::new();
    for (commit, jobs) in &wants {
        if w.has_commit(repo, commit) {
            present.push(commit.clone());
        } else {
            missing.push((commit.clone(), jobs.clone()));
        }
    }
    let mut error = None;
    let mut fetched: Vec<JobId> = Vec::new();
    if !missing.is_empty() {
        let lock = sh.sync_lock(repo);
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        match w.sync_mirror(repo, &url) {
            Ok(warning) => {
                if let Some(text) = warning {
                    sh.log.log(format!("WARNING: {text}"));
                    // For the jobs this sync was for: their log and `why` say so if they run here.
                    let note = format!("{}: {text}", c.node);
                    let mut cs = c.cs();
                    for job in wants.values().flatten() {
                        cs.sync_notes
                            .entry(job.clone())
                            .or_default()
                            .push(note.clone());
                    }
                }
                for (commit, jobs) in std::mem::take(&mut missing) {
                    if w.has_commit(repo, &commit) {
                        present.push(commit);
                        continue;
                    }
                    // A commit on no branch (a submodule's, a detached HEAD's): fetched by its id
                    // and pinned under a job that wants it.
                    let Some(job) = jobs.iter().next() else {
                        continue;
                    };
                    match w.fetch_pin(repo, &url, &commit, job) {
                        Ok(()) => {
                            present.push(commit);
                            fetched.push(job.clone());
                        }
                        Err(e) => {
                            error = Some(format!("commit {} is not there: {e:#}", short(&commit)));
                        }
                    }
                }
            }
            Err(e) => error = Some(format!("{e:#}")),
        }
    }
    let mut guard = sh.lock();
    let st = &mut *guard;
    let mut cs = c.cs();
    let cs = &mut *cs;
    for commit in present {
        cs.present.insert((repo.to_string(), commit));
    }
    if !fetched.is_empty() {
        for job in fetched {
            cs.fetched.entry(job).or_default().insert(repo.to_string());
        }
        save(st, KV_FETCHED, &cs.fetched);
    }
    let s = cs
        .syncs
        .entry((owner.to_string(), repo.to_string()))
        .or_default();
    s.running = false;
    match error {
        Some(e) => {
            s.backoff_ms = (s.backoff_ms * 2).clamp(1_000, 60_000);
            s.next_at = c.boot().saturating_add(s.backoff_ms);
            if s.error.as_deref() != Some(&e) {
                st.log.log(format!(
                    "WARNING: the mirror of {repo} could not sync from {owner}: {e}"
                ));
            }
            s.error = Some(e);
        }
        None => {
            if s.error.take().is_some() {
                st.log
                    .log(format!("the mirror of {repo} synced from {owner} again"));
            }
            s.backoff_ms = 0;
            s.next_at = 0;
        }
    }
    drop(guard);
    sh.kick();
    // A sync from this owner waiting for this one's place starts now.
    c.wake();
}

/// What of a record the owner hears about at once (usage goes now and then).
fn update_digest(j: &JobRecord) -> String {
    serde_json::json!([
        j.state,
        j.workspace,
        j.started_at,
        j.stalled,
        j.lease.as_ref().map(|l| l.deadline),
        j.stuck_pids,
        j.notes.len(),
    ])
    .to_string()
}

/// Sends the record to its owner (an end with `keep_error` while the kept files can't be
/// delivered); `Ok(true)` when the owner says it is no longer ours.
fn send_update(
    c: &Cluster,
    owner: &str,
    j: &JobRecord,
    keep_error: Option<&str>,
) -> Result<bool, String> {
    let rec = JobRecord {
        handle: None,
        ..j.clone()
    };
    match c.call(
        owner,
        &PeerRequest::Update {
            job: Box::new(rec),
            keep_error: keep_error.map(str::to_string),
        },
        CALL_TIMEOUT,
    )? {
        PeerResponse::Update { stale } => Ok(stale),
        other => Err(format!("an unexpected answer from {owner}: {other:?}")),
    }
}

/// The output log on its way to the owner.
struct LogShip {
    stream: crate::mesh::Stream,
    /// How much of this run's output the owner has.
    offset: u64,
    /// Where in this machine's log the owner's copy ends. Behind `offset` once the job has cut its
    /// log short ([`ship_log`]).
    pos: u64,
    /// The last bytes sent from this machine's log, ending at `pos` (at most [`LOG_TAIL`]): when
    /// the log no longer holds them there, it was rewritten.
    tail: Vec<u8>,
    /// When something last went to the owner (boot clock).
    sent_at: u64,
    /// Where this run's output starts in the owner's log, as the owner said.
    base: Option<u64>,
}

/// How many of the last bytes sent [`LogShip`] keeps to notice a rewritten log.
const LOG_TAIL: usize = 64;

/// Whether this machine's log, now `size` bytes, no longer holds what was sent from it: something
/// truncated the file (a job's stdout is a pipe, so only a process writing to the log's path can),
/// and maybe wrote more since.
fn log_rewritten(f: &mut std::fs::File, s: &LogShip, size: u64) -> std::io::Result<bool> {
    if size < s.pos {
        return Ok(true);
    }
    if s.tail.is_empty() {
        return Ok(false);
    }
    let mut now = vec![0u8; s.tail.len()];
    f.seek(SeekFrom::Start(s.pos - s.tail.len() as u64))?;
    f.read_exact(&mut now)?;
    Ok(now != s.tail)
}

/// Reads one line (a JSON answer) from a stream, a byte at a time.
pub(super) fn read_line(s: &mut impl Read) -> std::io::Result<String> {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    loop {
        if s.read(&mut b)? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        if b[0] == b'\n' {
            return String::from_utf8(out)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e));
        }
        out.push(b[0]);
        if out.len() > 64 << 10 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "line too long",
            ));
        }
    }
}

fn answer(s: &mut impl Read) -> Result<serde_json::Value, String> {
    let line = read_line(s).map_err(|e| format!("no answer: {e}"))?;
    let v: serde_json::Value =
        serde_json::from_str(&line).map_err(|e| format!("a bad answer: {e}"))?;
    if let Some(e) = v.get("error").and_then(|e| e.as_str()) {
        return Err(e.to_string());
    }
    Ok(v)
}

fn frame(w: &mut impl Write, f: &LogFrame) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(f)?;
    line.push(b'\n');
    w.write_all(&line)
}

/// Sends what the owner lacks of the output log. With `end` (the job is over), finishes the log
/// once all of it is sent; `Ok(true)` when the owner has confirmed the whole of it.
///
/// The owner's copy only grows. A job can rewrite its log here (truncate it, then write again);
/// the owner then keeps what it has, gets a line saying so, and this machine's log from its start
/// after that. `shift` is how far the owner's copy is ahead of this machine's log (the courier
/// keeps it across reconnections).
fn ship_log(
    sh: &Shared,
    c: &Cluster,
    id: &str,
    ob: &Outbox,
    ship: &mut Option<LogShip>,
    shift: &mut u64,
    end: bool,
) -> Result<bool, String> {
    let path = sh.paths.job_dir(id).join(files::LOG);
    if ship.is_none() {
        let header = serde_json::to_value(StreamHeader::Log {
            job: id.to_string(),
            token: ob.token.clone(),
        })
        .map_err(|e| e.to_string())?;
        let mut stream = c
            .mesh()
            .open_stream(&ob.owner, header)
            .map_err(|e| e.to_string())?;
        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));
        let v = answer(&mut stream)?;
        let offset = v
            .get("offset")
            .and_then(|o| o.as_u64())
            .ok_or("no offset in the owner's answer")?;
        let s = ship.insert(LogShip {
            stream,
            offset,
            pos: offset.saturating_sub(*shift),
            tail: Vec::new(),
            sent_at: c.boot(),
            base: v.get("base").and_then(|b| b.as_u64()),
        });
        log_at(sh, c, id, s);
    }
    let s = ship.as_mut().expect("opened above");
    let io = |e: std::io::Error| e.to_string();
    let mut f = match std::fs::File::open(&path) {
        Ok(f) => Some(f),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(io(e)),
    };
    let size = match &f {
        Some(f) => f.metadata().map_err(io)?.len(),
        None => 0,
    };
    let rewritten = match f.as_mut() {
        Some(f) => log_rewritten(f, s, size).map_err(io)?,
        None => s.pos > 0,
    };
    if rewritten {
        let line = format!(
            "\n--- potluck: the output log on {node} was cut short, to {size} bytes after {pos} \
             had been sent (something wrote to the log file itself); \
             {node}'s log from its start follows ---\n",
            node = c.node,
            pos = s.pos,
        );
        frame(
            &mut s.stream,
            &LogFrame::Data {
                at: s.offset,
                len: line.len() as u64,
            },
        )
        .and_then(|()| s.stream.write_all(line.as_bytes()))
        .map_err(|e| refused(&mut s.stream, e))?;
        s.offset += line.len() as u64;
        s.pos = 0;
        s.tail.clear();
        *shift = s.offset;
        log_at(sh, c, id, s);
        sh.log.log(format!(
            "{id}: its output log was cut short to {size} bytes; {} gets it again from its start",
            ob.owner
        ));
    }
    let mut sent = 0;
    if let Some(f) = f.as_mut()
        && s.pos < size
    {
        f.seek(SeekFrom::Start(s.pos)).map_err(io)?;
        let mut buf = vec![0u8; LOG_FRAME as usize];
        while s.pos < size && sent < LOG_PASS {
            let want = (size - s.pos).min(LOG_FRAME) as usize;
            let n = f.read(&mut buf[..want]).map_err(io)?;
            if n == 0 {
                break;
            }
            frame(
                &mut s.stream,
                &LogFrame::Data {
                    at: s.offset,
                    len: n as u64,
                },
            )
            .and_then(|()| s.stream.write_all(&buf[..n]))
            .map_err(|e| refused(&mut s.stream, e))?;
            s.offset += n as u64;
            s.pos += n as u64;
            sent += n as u64;
            let keep = n.min(LOG_TAIL);
            s.tail.extend_from_slice(&buf[n - keep..n]);
            let extra = s.tail.len().saturating_sub(LOG_TAIL);
            s.tail.drain(..extra);
        }
        s.sent_at = c.boot();
    }
    if end && s.pos >= size {
        frame(&mut s.stream, &LogFrame::End { len: s.offset })
            .map_err(|e| refused(&mut s.stream, e))?;
        let v = answer(&mut s.stream)?;
        let len = v.get("len").and_then(|l| l.as_u64());
        let ok = len == Some(s.offset);
        *ship = None;
        return if ok {
            Ok(true)
        } else {
            Err(format!("{} confirmed {len:?} bytes of the log", ob.owner))
        };
    }
    if sent == 0 && c.boot().saturating_sub(s.sent_at) >= LOG_PING_MS {
        frame(&mut s.stream, &LogFrame::Ping).map_err(|e| refused(&mut s.stream, e))?;
        s.sent_at = c.boot();
    }
    Ok(false)
}

/// Keeps where this machine's log starts in the owner's ([`Outbox::log_at`]), as `s` now places it.
fn log_at(sh: &Shared, c: &Cluster, id: &str, s: &LogShip) {
    let at = s
        .base
        .map(|b| b.saturating_add(s.offset.saturating_sub(s.pos)));
    if c.cs().outbox.get(id).is_some_and(|o| o.log_at != at) {
        set_outbox(sh, c, id, |o| o.log_at = at);
    }
}

/// A write to the owner failed: the owner may have said why before it closed the stream (it
/// can't write the log), which says more than the broken connection.
fn refused(stream: &mut crate::mesh::Stream, e: std::io::Error) -> String {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    match answer(stream) {
        Err(why) if !why.starts_with("no answer") && !why.starts_with("a bad answer") => why,
        _ => e.to_string(),
    }
}

/// Sends the job's kept files (`files::KEEP`) or crash reports (`files::CRASH`) to its owner.
fn ship_files(sh: &Shared, c: &Cluster, id: &str, ob: &Outbox, sub: &str) -> Result<(), String> {
    let (job, token) = (id.to_string(), ob.token.clone());
    let header = if sub == files::CRASH {
        StreamHeader::Crash { job, token }
    } else {
        StreamHeader::Keep { job, token }
    };
    let header = serde_json::to_value(header).map_err(|e| e.to_string())?;
    let mut stream = c
        .mesh()
        .open_stream(&ob.owner, header)
        .map_err(|e| e.to_string())?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(60)));
    answer(&mut stream)?;
    files::send_copy(&mut stream, &[], &sh.paths.job_dir(id), sub).map_err(|e| e.to_string())?;
    let _ = stream.close_write();
    answer(&mut stream).map(|_| ())
}

/// The kept files could not be delivered for the give-up time: they stay here, and the job's end
/// goes to its owner without them, with a note saying so.
fn keep_given_up(sh: &Shared, c: &Cluster, id: &str, ob: &Outbox, e: &str) {
    sh.log.log(format!(
        "{id}: WARNING: its kept files could not be delivered to {} for {}: {e}; they stay in {}",
        ob.owner,
        crate::types::fmt_ms(c.opts.keep_give_up_ms),
        sh.paths.job_dir(id).join(files::KEEP).display()
    ));
    let note = super::owner::keep_note(&c.node, e);
    {
        let mut st = sh.lock();
        if let Some(j) = st.jobs.get_mut(id)
            && !j.notes.contains(&note)
        {
            j.notes.push(note);
            st.save_job(id);
        }
    }
    set_outbox(sh, c, id, |o| o.keep_done = true);
}

fn set_outbox(sh: &Shared, c: &Cluster, id: &str, f: impl FnOnce(&mut Outbox)) {
    let mut guard = sh.lock();
    let st = &mut *guard;
    let mut cs = c.cs();
    if let Some(o) = cs.outbox.get_mut(id) {
        f(o);
    }
    save(st, KV_OUTBOX, &cs.outbox);
}

/// Delivered (or no longer ours): forgotten.
fn forget(sh: &Shared, c: &Cluster, id: &str) {
    let mut guard = sh.lock();
    let st = &mut *guard;
    let mut cs = c.cs();
    cs.outbox.remove(id);
    save(st, KV_OUTBOX, &cs.outbox);
}

/// Reports one job run for an owner until the owner has all of it: its record as it changes, the
/// output log as it grows, then (once it ended) the rest of the log, the kept files and the final
/// record. Retries what fails until the owner is back.
fn courier(sh: &Arc<Shared>, c: &Arc<Cluster>, id: &str) {
    let mut ship: Option<LogShip> = None;
    // How far the owner's copy of the log is ahead of this machine's ([`ship_log`]).
    let mut shift: u64 = 0;
    let mut sent: Option<String> = None;
    let mut sent_usage: Option<Usage> = None;
    // When usage last went to the owner (boot clock).
    let mut usage_at: u64 = 0;
    let mut backoff: u64 = 0;
    let mut pause: u64 = 0;
    loop {
        // Sleep in short steps so a stopping daemon isn't held up.
        let until = c.boot().saturating_add(pause);
        while c.boot() < until && !sh.stopping() && !c.stopping() {
            std::thread::sleep(Duration::from_millis(50));
        }
        if sh.stopping() || c.stopping() {
            return;
        }
        let (rec, ob) = {
            let st = sh.lock();
            let rec = st.job(id);
            (rec, c.cs().outbox.get(id).cloned())
        };
        let Some(ob) = ob else {
            return;
        };
        let Some(rec) = rec else {
            forget(sh, c, id);
            return;
        };
        let mut fail = |e: String, ship: &mut Option<LogShip>| {
            *ship = None;
            backoff = (backoff * 2).clamp(250, MAX_BACKOFF_MS);
            sh.log.log(format!("{id}: reporting to {}: {e}", ob.owner));
            backoff
        };
        if ob.orphaned {
            if rec.state.is_done() {
                if ob.keep_since.is_some() && !ob.keep_done {
                    sh.log.log(format!(
                        "{id}: WARNING: its kept files were not delivered to {}, and it is no longer this machine's job; they stay in {}",
                        ob.owner,
                        sh.paths.job_dir(id).join(files::KEEP).display()
                    ));
                }
                forget(sh, c, id);
                return;
            }
            stop_run(sh, id);
            pause = 500;
            continue;
        }
        if !c.alive(&ob.owner) {
            // Away: the reports wait for it.
            ship = None;
            pause = 500;
            continue;
        }
        if !rec.state.is_done() {
            let digest = update_digest(&rec);
            let usage_due = sent_usage.as_ref() != Some(&rec.usage)
                && c.boot().saturating_sub(usage_at) >= c.opts.usage_report_ms;
            if sent.as_deref() != Some(digest.as_str()) || usage_due {
                match send_update(c, &ob.owner, &rec, None) {
                    Ok(false) => {
                        sent = Some(digest);
                        sent_usage = Some(rec.usage.clone());
                        usage_at = c.boot();
                    }
                    Ok(true) => {
                        sh.log.log(format!(
                            "{id}: {} says it is no longer this machine's job",
                            ob.owner
                        ));
                        orphan(sh, c, id);
                        pause = 0;
                        continue;
                    }
                    Err(e) => {
                        pause = fail(e, &mut ship);
                        continue;
                    }
                }
            }
            if rec.state == JobState::Running
                && let Err(e) = ship_log(sh, c, id, &ob, &mut ship, &mut shift, false)
            {
                pause = fail(e, &mut ship);
                continue;
            }
            backoff = 0;
            pause = 200;
            continue;
        }
        if !ob.log_done {
            match ship_log(sh, c, id, &ob, &mut ship, &mut shift, true) {
                Ok(true) => set_outbox(sh, c, id, |o| o.log_done = true),
                Ok(false) => {
                    pause = 0;
                    continue;
                }
                Err(e) => {
                    pause = fail(e, &mut ship);
                    continue;
                }
            }
        }
        // Kept files given up on (`keep_since` stays) are here, not on the owner.
        let mut kept_here = ob.keep_since.is_some();
        if !ob.keep_done {
            if !rec.kept.is_empty()
                && let Err(e) = ship_files(sh, c, id, &ob, files::KEEP)
            {
                let since = ob.keep_since.unwrap_or_else(now);
                if now().saturating_sub(since) >= c.opts.keep_give_up_ms {
                    keep_given_up(sh, c, id, &ob, &e);
                    pause = 0;
                    continue;
                }
                if ob.keep_since.is_none() {
                    set_outbox(sh, c, id, |o| o.keep_since = Some(since));
                }
                // The owner learns that the job ended and why its kept files are late: it ends
                // the job without them if this machine leaves the cluster meanwhile.
                if let Ok(true) = send_update(c, &ob.owner, &rec, Some(&e)) {
                    orphan(sh, c, id);
                }
                pause = fail(e, &mut ship);
                continue;
            }
            set_outbox(sh, c, id, |o| {
                o.keep_done = true;
                o.keep_since = None;
            });
            kept_here = false;
        }
        if !ob.end_done {
            let rec = match kept_here {
                true => JobRecord {
                    kept: Vec::new(),
                    ..rec.clone()
                },
                false => rec.clone(),
            };
            match send_update(c, &ob.owner, &rec, None) {
                Ok(true) => {
                    sh.log.log(format!(
                        "{id}: {} no longer counts it as this machine's job; its ending was not taken",
                        ob.owner
                    ));
                    forget(sh, c, id);
                    return;
                }
                Ok(false) => {
                    sh.log.log(format!("{id}: delivered to {}", ob.owner));
                    set_outbox(sh, c, id, |o| o.end_done = true);
                }
                Err(e) => {
                    pause = fail(e, &mut ship);
                    continue;
                }
            }
        }
        // Last, the crash reports, once those written late are in too.
        if sh.crash_pending(id) {
            pause = 200;
            continue;
        }
        if has_files(&sh.paths.job_dir(id).join(files::CRASH))
            && let Err(e) = ship_files(sh, c, id, &ob, files::CRASH)
        {
            pause = fail(e, &mut ship);
            continue;
        }
        forget(sh, c, id);
        return;
    }
}

/// Whether directory `dir` holds anything.
fn has_files(dir: &std::path::Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|mut d| d.next().is_some())
}
