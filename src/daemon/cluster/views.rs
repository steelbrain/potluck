//! Reports between members (pulled and pushed), and the cluster-wide `status`, `nodes` and `why`
//! built from them.

use super::peer::{MachineReport, PeerRequest, PeerResponse, node_of_ws, now, owner_of};
use super::remote::queued_of;
use super::{CState, Cluster, Remote};
use crate::daemon::Shared;
use crate::daemon::state::{MachineSnap, State};
use crate::mesh::Member;
use crate::proto::{NodeView, Response, StatusView};
use crate::sched::{MachineView, Queued, Running};
use crate::types::{
    Facts, JobId, JobRecord, JobState, Millis, NodeName, RepoRef, WorkspaceRecord, WorkspaceState,
};
use crate::why::Snapshot;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Finished jobs `status` lists.
const RECENT: usize = 20;
const PULL_TIMEOUT: Duration = Duration::from_secs(5);

/// This machine's report, whole: what goes to a member is [`for_member`] of it.
pub(super) fn own_report(sh: &Shared, c: &Cluster) -> MachineReport {
    let st = sh.lock();
    let cs = c.cs();
    let mut report = build_report(&st, &cs);
    // Numbered while the state is locked, so a higher number never describes an older state.
    report.started_at = c.started_at;
    report.seq = c.report_seq.fetch_add(1, Ordering::SeqCst) + 1;
    report
}

/// Whether `new` from a member is later news than `old` from it: numbered after it, from
/// another start of its daemon, or unnumbered. A pull's reply built before a push that arrived
/// first is not: it would bring back a queue without the jobs dispatched since.
fn later(old: &MachineReport, new: &MachineReport) -> bool {
    later_numbers((old.started_at, old.seq), (new.started_at, new.seq))
}

/// [`later`] by the reports' numbers: (started_at, seq).
pub(super) fn later_numbers(old: (Millis, u64), new: (Millis, u64)) -> bool {
    new.1 == 0 || old.1 == 0 || new.0 != old.0 || new.1 > old.1
}

fn build_report(st: &State, cs: &CState) -> MachineReport {
    let local = st.local();
    let me = st.node.as_str();
    let workspaces = st.records();
    MachineReport {
        node: st.node.clone(),
        facts: local.facts.clone(),
        ram_capacity_mib: local.ram_capacity_mib,
        running: st
            .jobs
            .values()
            .filter(|j| matches!(j.state, JobState::Starting | JobState::Running))
            .filter(|j| j.node.as_deref() == Some(me) && j.lease.is_some())
            .map(|j| JobRecord {
                handle: None,
                ..j.clone()
            })
            .collect(),
        queue: own_queue(st),
        ws_owners: workspaces
            .iter()
            .filter_map(|w| Some((w.id.clone(), cs.ws_owners.get(&w.id)?.clone())))
            .collect(),
        workspaces,
        unsynced: cs.unsynced.clone(),
        locality: cs.locality.clone(),
        catching_up: cs.catching_up.clone(),
        removed: cs.out.is_some(),
        // Numbered by own_report.
        started_at: 0,
        seq: 0,
    }
}

/// What this machine's `report` tells member `to`: what it plans with and `status`, `why` and
/// `nodes` show, and no more. No lease tokens. Of a job running here that `to` doesn't own, no
/// command, environment, kept-file globs, labels, source paths or signed spec; of a queued job,
/// no command, environment, source paths or signed spec (the machine that claims it gets the
/// whole spec with its grant), only who signed it; only the queued jobs `to` may take
/// ([`super::owner::may_take`]); and no workspace paths.
pub(super) fn for_member(mut report: MachineReport, to: &super::owner::Taker) -> MachineReport {
    for j in &mut report.running {
        if let Some(l) = &mut j.lease {
            l.token.clear();
        }
        if owner_of(&j.id) != to.name {
            let spec = &mut j.spec;
            spec.cmd.clear();
            spec.env.clear();
            spec.unset_env.clear();
            spec.keep.clear();
            spec.labels.clear();
            no_sources(&mut spec.repo);
            no_signed_spec(spec);
        }
    }
    report.queue.retain(|j| super::owner::may_take(j, to));
    for j in &mut report.queue {
        let spec = &mut j.spec;
        spec.cmd.clear();
        spec.env.clear();
        spec.unset_env.clear();
        no_sources(&mut spec.repo);
        no_signed_spec(spec);
    }
    for w in &mut report.workspaces {
        w.path = PathBuf::new();
    }
    report
}

/// A spec's signature without the spec it covers (which holds all the spec holds): who signed it
/// stays.
fn no_signed_spec(spec: &mut crate::types::JobSpec) {
    if let Some(a) = &mut spec.auth {
        a.signed_spec.clear();
    }
}

/// A dispatch's repositories without where they are on the dispatching machine.
fn no_sources(repo: &mut RepoRef) {
    repo.source = PathBuf::new();
    for s in &mut repo.submodules {
        s.source = PathBuf::new();
    }
    for c in &mut repo.carried {
        c.source = PathBuf::new();
    }
}

/// What a member asking for one of this machine's workspaces gets ([`PeerRequest::Workspace`]):
/// what a dispatch into it checks, its repository, commit and state; of a removed one only that
/// it is gone. No path, branch, hold, size or times.
pub(super) fn for_dispatch(w: &WorkspaceRecord) -> WorkspaceRecord {
    let live = w.state != WorkspaceState::Removed;
    let kept = |v: &String| if live { v.clone() } else { String::new() };
    WorkspaceRecord {
        id: w.id.clone(),
        node: w.node.clone(),
        repo_id: kept(&w.repo_id),
        commit: kept(&w.commit),
        branch: None,
        path: PathBuf::new(),
        state: w.state,
        created_at: 0,
        last_used_at: 0,
        held_until: 0,
        pending: 0,
        size_bytes: None,
        advanced_by: None,
    }
}

/// This machine's own queued jobs.
fn own_queue(st: &State) -> Vec<JobRecord> {
    st.jobs
        .values()
        .filter(|j| j.state == JobState::Queued && owner_of(&j.id) == st.node)
        .cloned()
        .collect()
}

/// A report a member pushed or this machine pulled.
pub(super) fn received(sh: &Shared, c: &Cluster, from: &str, mut report: MachineReport) {
    if report.node != from {
        return;
    }
    // The digest of what it sent, which its next pull's answer is compared with.
    let digest = report.digest();
    own_entries(from, &mut report);
    let known = super::capable::Known {
        facts: report.facts.clone(),
        ram_capacity_mib: report.ram_capacity_mib,
    };
    let mut guard = sh.lock();
    let mut cs = c.cs();
    if cs
        .reports
        .get(from)
        .is_some_and(|old| !later(&old.report, &report))
    {
        return;
    }
    // What it can run, kept for when it is down (a dispatch says so; queued jobs wait for it).
    if cs.known.get(from) != Some(&known) && !known.facts.os.is_empty() {
        cs.known.insert(from.to_string(), known);
        super::save(&mut guard, super::KV_KNOWN, &cs.known);
    }
    cs.reports.insert(
        from.to_string(),
        Remote {
            report,
            at: now(),
            digest,
        },
    );
    super::remote::pinned_here(&mut guard, &cs);
    drop(cs);
    drop(guard);
    sh.kick();
}

/// What a report from `from` says for its own machine, the rest dropped: its queue holds only its
/// own queued jobs, and what runs there only jobs starting or running there. Anything else (no
/// report of a member's holds it) would keep this machine's workspaces a job is pinned to from
/// removal, or answer a `wait` while the job's owner is away.
fn own_entries(from: &str, report: &mut MachineReport) {
    report
        .queue
        .retain(|j| owner_of(&j.id) == from && j.state == JobState::Queued);
    report.running.retain(|j| {
        j.node.as_deref() == Some(from) && matches!(j.state, JobState::Starting | JobState::Running)
    });
}

/// A pull found the report held from `from` still current: it takes the numbers of the one `from`
/// built now (so one built before, still on its way, is dropped as older news) and counts as
/// arrived now.
fn unchanged(c: &Cluster, from: &str, numbers: (Millis, u64)) {
    let mut cs = c.cs();
    if let Some(r) = cs.reports.get_mut(from)
        && later_numbers((r.report.started_at, r.report.seq), numbers)
    {
        (r.report.started_at, r.report.seq) = numbers;
        r.at = now();
    }
}

/// Pulls the reports that are due and pushes this machine's when its queue changed (`now` on the
/// boot clock).
pub(super) fn pulls_and_pushes(sh: &Arc<Shared>, c: &Arc<Cluster>, now: u64) {
    let members: Vec<(NodeName, bool)> = c
        .mesh()
        .members()
        .into_iter()
        .filter(|m| !m.this_machine)
        .map(|m| (m.name.clone(), c.up(&m)))
        .collect();
    // What the others act on: the queue, what runs here, and the workspaces here.
    let digest = {
        let st = sh.lock();
        let mut d: Vec<String> = own_queue(&st)
            .iter()
            .map(|j| format!("q{}:{}:{}", j.id, j.passes, j.spec.priority))
            .collect();
        d.extend(
            st.jobs
                .values()
                .filter(|j| matches!(j.state, JobState::Starting | JobState::Running))
                .filter(|j| j.node.as_deref() == Some(st.node.as_str()))
                .map(|j| format!("r{}:{:?}", j.id, j.state)),
        );
        d.extend(
            st.records()
                .iter()
                .map(|w| format!("w{}:{:?}:{}", w.id, w.state, w.held_until)),
        );
        d.join(",")
    };
    let (pulls, pushes) = {
        let mut cs = c.cs();
        if cs.out.is_some() {
            return;
        }
        // Reports of machines no longer members go.
        let names: BTreeSet<&NodeName> = members.iter().map(|(n, _)| n).collect();
        cs.reports.retain(|n, _| names.contains(n));
        let mut pulls = Vec::new();
        for (n, alive) in &members {
            // A member not heard from yet is tried too: pulling is how it is first heard.
            let due = cs
                .pulled_at
                .get(n)
                .is_none_or(|t| now.saturating_sub(*t) >= c.opts.pull_ms);
            let known = cs.reports.contains_key(n);
            if due && (*alive || !known) && !cs.pulling.contains(n) {
                cs.pulling.insert(n.clone());
                pulls.push(n.clone());
            }
        }
        let mut pushes = Vec::new();
        if cs.push_due || cs.pushed != digest {
            cs.push_due = false;
            cs.pushed = digest;
            for (n, alive) in &members {
                if *alive && !cs.pushing.contains(n) {
                    cs.pushing.insert(n.clone());
                    pushes.push(n.clone());
                } else if *alive {
                    // One is on its way; push again next time.
                    cs.push_due = true;
                }
            }
        }
        (pulls, pushes)
    };
    for n in pulls {
        sh.spawn_worker(&format!("pull-{n}"), move |sh| pull(sh, &n));
    }
    if !pushes.is_empty() {
        let report = own_report(sh, c);
        let mesh = c.mesh();
        for n in pushes {
            let report = for_member(report.clone(), &super::owner::Taker::of(&mesh, &n));
            sh.spawn_worker(&format!("push-{n}"), move |sh| {
                let Some(c) = sh.cluster() else {
                    return;
                };
                let _ = c.call(
                    &n,
                    &PeerRequest::Push {
                        report: Box::new(report),
                    },
                    PULL_TIMEOUT,
                );
                c.cs().pushing.remove(&n);
            });
        }
    }
}

fn pull(sh: &Arc<Shared>, n: &str) {
    let Some(c) = sh.cluster() else {
        return;
    };
    let have = c.cs().reports.get(n).map(|r| r.digest.clone());
    let r = c.call(n, &PeerRequest::Report { have }, PULL_TIMEOUT);
    {
        let mut cs = c.cs();
        cs.pulling.remove(n);
        cs.pulled_at.insert(n.to_string(), c.boot());
    }
    match r {
        Ok(PeerResponse::Report { report }) => received(sh, &c, n, *report),
        Ok(PeerResponse::Unchanged { started_at, seq }) => unchanged(&c, n, (started_at, seq)),
        _ => {}
    }
}

/// Pulls, now, the reports of members up that this machine has none of yet (for a dispatch to
/// know every machine's facts).
pub(crate) fn refresh_unknown(sh: &Arc<Shared>, c: &Cluster) {
    refresh_unknown_of(sh, c, c.mesh().members());
}

/// [`refresh_unknown`] for `members` (a snapshot the caller goes on to use).
fn refresh_unknown_of(sh: &Arc<Shared>, c: &Cluster, members: Vec<Member>) {
    let unknown: Vec<NodeName> = {
        let cs = c.cs();
        if cs.out.is_some() {
            return;
        }
        members
            .into_iter()
            .filter(|m| !m.this_machine && c.up(m) && !cs.reports.contains_key(&m.name))
            .map(|m| m.name)
            .collect()
    };
    if unknown.is_empty() {
        return;
    }
    std::thread::scope(|s| {
        for n in &unknown {
            s.spawn(move || {
                if let Ok(PeerResponse::Report { report }) = c.call(
                    n,
                    &PeerRequest::Report { have: None },
                    Duration::from_secs(2),
                ) {
                    received(sh, c, n, *report);
                }
            });
        }
    });
}

/// A member's machine as the scheduler sees it, from its report, in pool `pool` (its admission's,
/// [`crate::mesh::Mesh::pool_of`]: not something it reports).
pub(super) fn snap_of(r: &MachineReport, pool: &str) -> MachineSnap {
    MachineSnap {
        facts: r.facts.clone(),
        pool: pool.to_string(),
        ram_capacity_mib: r.ram_capacity_mib,
        running: r
            .running
            .iter()
            .filter_map(|j| {
                let l = j.lease.as_ref()?;
                Some(Running {
                    job: j.id.clone(),
                    grant: l.grant.clone(),
                    deadline: l.deadline,
                })
            })
            .collect(),
    }
}

/// The members up now, other than this machine.
fn alive_members(c: &Cluster) -> BTreeSet<NodeName> {
    c.mesh()
        .members()
        .into_iter()
        .filter(|m| c.up(m) && !m.this_machine)
        .map(|m| m.name)
        .collect()
}

fn view_of(r: &MachineReport) -> NodeView {
    let snap = snap_of(r, crate::mesh::trust::DEFAULT_POOL);
    let mut gpus_busy: Vec<String> = snap
        .running
        .iter()
        .flat_map(|x| x.grant.gpus.iter().cloned())
        .collect();
    gpus_busy.sort();
    gpus_busy.dedup();
    NodeView {
        facts: r.facts.clone(),
        address: None,
        alive: true,
        updating: false,
        last_seen: None,
        ram_capacity_mib: r.ram_capacity_mib,
        // A peer's grants: summed without overflowing, whatever they say.
        ram_reserved_mib: snap
            .running
            .iter()
            .fold(0u64, |a, x| a.saturating_add(x.grant.ram_mib)),
        gpus_busy,
        exclusive_job: snap
            .running
            .iter()
            .find(|x| x.grant.exclusive)
            .map(|x| x.job.clone()),
    }
}

/// Every member of `members`: this machine as it knows itself, the others from their latest
/// reports.
fn node_views(
    st: &State,
    cs: &CState,
    c: &Cluster,
    now: Millis,
    mut members: Vec<Member>,
) -> Vec<NodeView> {
    let mut out = Vec::new();
    members.sort_by(|a, b| a.name.cmp(&b.name));
    if !members.iter().any(|m| m.this_machine) || cs.out.is_some() {
        members.retain(|m| m.this_machine);
        if let Some(mut v) = st.node_view(&st.node, now) {
            v.address = Some(c.mesh().address());
            out.push(v);
        }
        return out;
    }
    for m in members {
        if m.this_machine {
            if let Some(mut v) = st.node_view(&st.node, now) {
                // Where it listens now (the records may name an older port).
                v.address = Some(c.mesh().address());
                out.push(v);
            }
            continue;
        }
        let mut v = match cs.reports.get(&m.name) {
            Some(r) => view_of(&r.report),
            None => NodeView {
                facts: Facts {
                    node: m.name.clone(),
                    version: m.version.clone().unwrap_or_default(),
                    ..Default::default()
                },
                address: None,
                alive: false,
                updating: false,
                last_seen: None,
                ram_capacity_mib: 0,
                ram_reserved_mib: 0,
                gpus_busy: Vec::new(),
                exclusive_job: None,
            },
        };
        v.alive = c.up(&m);
        // Heard from on another version: the mesh counts that as alive for a while.
        v.updating = m.alive && !v.alive;
        v.address = Some(m.address);
        v.last_seen = m.last_seen;
        if let Some(version) = m.version {
            v.facts.version = version;
        }
        out.push(v);
    }
    out
}

/// `potluck nodes`. Members up whose report hasn't arrived yet (one just paired, or back) are
/// asked for it first, so they aren't shown up with no facts.
pub(crate) fn nodes(sh: &Arc<Shared>, c: &Cluster) -> Response {
    // One snapshot of the members for both: one heard from in between is shown as not up yet,
    // not as up with no facts.
    let members = c.mesh().members();
    refresh_unknown_of(sh, c, members.clone());
    let st = sh.lock();
    let cs = c.cs();
    Response::Nodes {
        nodes: node_views(&st, &cs, c, now(), members),
    }
}

/// `potluck status`, with the members' reports as for [`nodes`].
pub(crate) fn status(sh: &Arc<Shared>, c: &Cluster) -> Result<Response, String> {
    let members = c.mesh().members();
    refresh_unknown_of(sh, c, members.clone());
    let alive = alive_members(c);
    // Heard from lately, on any version: what one that is updating runs goes on meanwhile.
    let heard: BTreeSet<NodeName> = members
        .iter()
        .filter(|m| m.alive)
        .map(|m| m.name.clone())
        .collect();
    let listen_error = c.listen_error();
    let st = sh.lock();
    let cs = c.cs();
    let now = now();
    let nodes = node_views(&st, &cs, c, now, members);
    // Running anywhere: this machine's records first (its own jobs wherever they run, and the
    // jobs it runs for others), then what the other members report.
    let mut running: Vec<JobRecord> = st
        .jobs
        .values()
        .filter(|j| matches!(j.state, JobState::Starting | JobState::Running))
        .cloned()
        .collect();
    let mut seen: BTreeSet<String> = running.iter().map(|j| j.id.clone()).collect();
    for (n, r) in &cs.reports {
        // One that is down keeps its last report here: what it ran may run elsewhere now.
        if !heard.contains(n) {
            continue;
        }
        for j in &r.report.running {
            // This machine's own jobs are as its records say (a report may be older).
            if owner_of(&j.id) != st.node && seen.insert(j.id.clone()) {
                running.push(j.clone());
            }
        }
    }
    // The global queue, in start order.
    let mut records: Vec<JobRecord> = own_queue(&st);
    let mut queue: Vec<Queued> = records.iter().map(|j| st.queued(j)).collect();
    for (n, r) in &cs.reports {
        if !alive.contains(n) || r.report.removed {
            continue;
        }
        for j in &r.report.queue {
            queue.push(queued_of(&st, j));
            records.push(j.clone());
        }
    }
    let queued: Vec<JobRecord> = sh
        .parts
        .scheduler
        .order(&queue)
        .into_iter()
        .filter_map(|i| records.get(i).cloned())
        .collect();
    let recent = st
        .store
        .recent_jobs(RECENT)
        .map_err(|e| format!("store: {e:#}"))?;
    let mut workspaces = st.records();
    for r in cs.reports.values() {
        workspaces.extend(r.report.workspaces.iter().cloned());
    }
    let mut notes = Vec::new();
    if let Some(why) = &cs.out {
        notes.push(format!(
            "{why}: it runs only its own jobs; run `potluck pair` here and `potluck admit` on a member to join again"
        ));
    }
    notes.extend(listen_error);
    for ((owner, repo), s) in &cs.syncs {
        if let Some(e) = &s.error {
            notes.push(format!(
                "the mirror of {repo} could not sync from {owner}: {e}"
            ));
        }
    }
    Ok(Response::Status {
        status: Box::new(StatusView {
            nodes,
            running,
            queued,
            recent,
            workspaces,
            notes,
        }),
    })
}

/// `potluck why` for a job this machine owns, over every member up.
pub(crate) fn why(sh: &Shared, c: &Cluster, id: &str) -> Result<Response, String> {
    let alive = alive_members(c);
    // Asked before the state is locked, as the planning loop does.
    let mesh = c.mesh();
    let held_back: BTreeSet<NodeName> = alive
        .iter()
        .filter(|n| !mesh.caught_up_for(n))
        .cloned()
        .collect();
    let pool_here = c.pool_here();
    let st = sh.lock();
    let cs = c.cs();
    let job = st.job(id).ok_or_else(|| format!("no job {id}"))?;
    let mut snaps: Vec<MachineSnap> = st.local_snap(&pool_here).into_iter().collect();
    let mut jobs: Vec<JobRecord> = st
        .jobs
        .values()
        .filter(|j| !j.state.is_done())
        .cloned()
        .collect();
    let mut queue = st.queue();
    let mut unsynced: BTreeMap<NodeName, String> = BTreeMap::new();
    if let Some(why) = cs.unsynced.get(id) {
        unsynced.insert(st.node.clone(), why.clone());
    }
    let mut locality: BTreeMap<NodeName, String> = BTreeMap::new();
    if let Some(why) = cs.locality.get(id) {
        locality.insert(st.node.clone(), why.clone());
    }
    // Members that could take it now but wait: this machine, back from being away, compares
    // membership records with the other members before it grants them a claim, or they, back
    // from being away, do so before they take this machine's jobs.
    let up_to = crate::types::fmt_ms(c.down_after_ms());
    let mut catch_up: BTreeMap<NodeName, String> = held_back
        .iter()
        .map(|n| {
            (
                n.clone(),
                format!(
                    "waits while {}, back from being away, compares membership records with the other members (up to {up_to})",
                    st.node
                ),
            )
        })
        .collect();
    let removed = cs.out.is_some();
    // What each machine leaves out of its plan, from what it last said (this machine: its own
    // state), with the owners it catches up with.
    let mut held_out: BTreeMap<NodeName, (BTreeSet<JobId>, BTreeSet<NodeName>)> = BTreeMap::new();
    held_out.insert(
        st.node.clone(),
        (
            cs.unsynced
                .keys()
                .chain(cs.locality.keys())
                .cloned()
                .collect(),
            cs.catching_up.clone(),
        ),
    );
    for (n, r) in &cs.reports {
        if removed || !alive.contains(n) || r.report.removed {
            continue;
        }
        // How long it waits is its own down time, which this machine doesn't know.
        if r.report.catching_up.contains(&st.node) {
            catch_up.entry(n.clone()).or_insert_with(|| {
                "is back from being away and compares membership records with the other members first".into()
            });
        }
        let pool = mesh
            .pool_of(n)
            .unwrap_or_else(|| crate::mesh::trust::DEFAULT_POOL.to_string());
        let mut snap = snap_of(&r.report, &pool);
        // A claim this machine hasn't granted (it still has the job queued) holds nothing yet:
        // the job would show as waiting on what it reserved itself.
        snap.running.retain(|x| {
            owner_of(&x.job) != st.node
                || st
                    .jobs
                    .get(&x.job)
                    .is_none_or(|j| j.state != JobState::Queued)
        });
        snaps.push(snap);
        let known: BTreeSet<String> = jobs.iter().map(|j| j.id.clone()).collect();
        // This machine's own jobs are as its records say: a report may be older (sent before
        // the job's end arrived here, or before it was queued again).
        jobs.extend(
            r.report
                .running
                .iter()
                .filter(|j| owner_of(&j.id) != st.node && !known.contains(&j.id))
                .cloned(),
        );
        for j in &r.report.queue {
            queue.push(queued_of(&st, j));
            if !known.contains(&j.id) {
                jobs.push(j.clone());
            }
        }
        if let Some(why) = r.report.unsynced.get(id) {
            unsynced.insert(n.clone(), why.clone());
        }
        if let Some(why) = r.report.locality.get(id) {
            locality.insert(n.clone(), why.clone());
        }
        held_out.insert(
            n.clone(),
            (
                r.report
                    .unsynced
                    .keys()
                    .chain(r.report.locality.keys())
                    .cloned()
                    .collect(),
                r.report.catching_up.clone(),
            ),
        );
    }
    // A workspace on another machine is busy while a job runs in it, as this machine's records
    // of its own jobs and the others' reports say: that machine's scheduler waits for it.
    for q in &mut queue {
        if let Some((ws, node, busy)) = &mut q.workspace
            && *node != st.node
        {
            *busy = jobs.iter().any(|j| {
                j.workspace.as_deref() == Some(ws.as_str())
                    && matches!(j.state, JobState::Starting | JobState::Running)
            });
        }
    }
    let held_out: BTreeMap<NodeName, BTreeSet<JobId>> = held_out
        .into_iter()
        .map(|(n, (mut jobs, owners))| {
            jobs.extend(
                queue
                    .iter()
                    .filter(|q| owners.contains(owner_of(&q.job)))
                    .map(|q| q.job.clone()),
            );
            (n, jobs)
        })
        .collect();
    let queued = (job.state == JobState::Queued).then(|| st.queued(&job));
    if job.state.is_done() {
        jobs.push(job);
    }
    let views: Vec<MachineView> = snaps.iter().map(MachineSnap::view).collect();
    let snap = Snapshot {
        now: now(),
        machines: &views,
        jobs: &jobs,
        queue: &queue,
        reservation: None,
    };
    let waits = crate::why::Waits {
        unsynced: &unsynced,
        locality: &locality,
        catch_up: &catch_up,
        held_out: &held_out,
    };
    let mut why = sh.parts.scheduler.explain_waits(id, &snap, &waits);
    if let Some(p) = cs.endings.get(id) {
        why.lines.push(format!(
            "it ended on {} ({}); its kept files have not arrived ({}): it ends without them at {} at the latest",
            p.runner,
            p.record.state,
            p.error,
            super::capable::clock(p.since.saturating_add(c.opts.keep_give_up_ms))
        ));
    }
    // The members that could run it and aren't up, and what then becomes of it.
    if let Some(q) = queued {
        let machines = super::capable::machines(&st, Some((c, &cs)));
        for o in machines.capable_others(sh, &q) {
            if !matches!(o.away, super::capable::Away::Unknown) {
                why.lines
                    .push(format!("{} could run it but {}", o.node, o.why_state()));
            }
        }
        let updating = machines.capable_others(sh, &q).iter().any(|o| o.updating());
        if let super::capable::Verdict::Waits = machines.verdict(sh, &q) {
            why.lines.push(if updating {
                format!(
                    "no machine on this machine's version can run it: it waits for the members to run one version (the older one updates itself), and ends as lost (exit 125) if that takes longer than {}",
                    crate::types::fmt_ms(
                        c.opts.mesh.updating_for.as_millis() as u64 + c.down_after_ms()
                    )
                )
            } else {
                format!(
                    "no machine that is up can run it: it waits for those, and ends as lost (exit 125) once they have been down for {}",
                    crate::types::fmt_ms(c.down_after_ms())
                )
            });
        }
    }
    Ok(Response::Why { why })
}

/// A workspace on `node` as that machine's answer to a relayed hold or release left it: put in
/// this machine's view of `node` at once, so reminders say the same. Then `node`'s report is
/// pulled again: one it built before the change (a pull or push on its way) would otherwise
/// arrive after it and bring the old record back; this one is numbered after those, which are
/// then dropped as older news.
pub(crate) fn workspace_changed(sh: &Shared, c: &Cluster, node: &str, ws: &WorkspaceRecord) {
    {
        let mut cs = c.cs();
        if let Some(r) = cs.reports.get_mut(node)
            && let Some(w) = r.report.workspaces.iter_mut().find(|w| w.id == ws.id)
        {
            // Without its path, as in `node`'s reports.
            *w = WorkspaceRecord {
                path: PathBuf::new(),
                ..ws.clone()
            };
        }
    }
    let full = PeerRequest::Report { have: None };
    if let Ok(PeerResponse::Report { report }) = c.call(node, &full, PULL_TIMEOUT) {
        received(sh, c, node, *report);
    }
}

/// Workspaces on other machines that this machine's jobs used last: its branches' reminders.
pub(crate) fn remote_reminder_records(c: &Cluster) -> Vec<WorkspaceRecord> {
    let cs = c.cs();
    cs.reports
        .values()
        .flat_map(|r| {
            r.report
                .workspaces
                .iter()
                .filter(|w| node_of_ws(&w.id) == r.report.node)
                .filter(|w| r.report.ws_owners.get(&w.id) == Some(&c.node))
                .cloned()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(started_at: Millis, seq: u64) -> MachineReport {
        MachineReport {
            started_at,
            seq,
            ..Default::default()
        }
    }

    #[test]
    fn a_report_built_before_the_one_held_is_dropped() {
        assert!(later(&report(5, 1), &report(5, 2)));
        assert!(
            !later(&report(5, 2), &report(5, 1)),
            "a pull's reply built before a push that came first"
        );
        assert!(
            !later(&report(5, 2), &report(5, 2)),
            "the same report again"
        );
        assert!(later(&report(5, 9), &report(6, 1)), "its daemon restarted");
        assert!(later(&report(5, 9), &report(0, 0)), "unnumbered");
        assert!(later(&report(0, 0), &report(5, 1)));
    }
}
