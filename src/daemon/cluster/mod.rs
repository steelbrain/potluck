//! The daemon in a cluster (phase 2): the mesh, the global queue with claims, jobs run for other
//! machines, remote commands, lost and removed machines, and versions.
//!
//! - **The mesh.** The daemon starts [`Mesh`] with this module's handler when it starts and stops
//!   it first when it stops. `pair`, `pair --finish`, `admit` and `remove` go straight to it. It
//!   listens on its port only while this machine is in a cluster: a machine in none (it never
//!   paired, or it was removed) runs the mesh without a listener ([`Mesh::start_unlistening`]),
//!   which is enough to make a pairing request, admit or finish pairing. The founder starts
//!   listening when it admits the first machine, the new machine when it finishes pairing; a
//!   removed machine stops at once, and a member left alone by removals after a short while (so
//!   the removal reaches the removed machine).
//! - **Reports.** Every member pulls each live member's [`peer::MachineReport`] about once a
//!   second (and at once when it comes up), and a member pushes its own when its queue changes. A
//!   report carries what the others need: facts and load (status, nodes, why, locality), the
//!   member's own queue (the global queue is the union), its workspaces, and the jobs it can't take
//!   yet for want of commits (`sync` in why).
//! - **Claims (late binding).** Each member plans for itself ([`crate::sched::plan`]) against its
//!   own queue plus the other members' queued jobs whose commits its mirrors hold (missing ones
//!   start a sync from the job's owner through the tunnel). A start of another machine's job is
//!   reserved locally (record, workspace, lease), then claimed from the owner, which grants each
//!   job once; a refused claim is undone and the machine plans again. Passes go to the owners.
//!   A machine back from being away claims from an owner, or grants a runner's claim, only once
//!   the mesh has compared its records with the other members ([`Mesh::caught_up_for`]).
//! - **Locality.** A machine with no workspace of a job's repository leaves the job alone for a
//!   few seconds while a live member with one could start it by then; after that it claims it
//!   anyway.
//! - **Running for an owner.** The runner keeps its own record of the job and reports it to the
//!   owner (state changes at once, usage now and then), streams the output log to the owner as it
//!   grows, sends the kept files at the end, and only then the final record: the owner marks the
//!   job done when everything has arrived, so `wait` and `logs` work there as on one machine.
//!   The crash reports follow, once those written late (a few seconds after a crash) are in.
//!   Reports that fail are retried until delivered (an outbox, kept across restarts).
//! - **Remote commands.** A CLI request about a job owned elsewhere, or a workspace on another
//!   machine, is relayed to that machine and answered from there; `cancel` of an owned job running
//!   elsewhere is forwarded to its runner.
//! - **Lost machines.** A runner down for the mesh's down time: its jobs are queued again (output
//!   so far kept, with a line saying so), or end as lost (exit 125) when their dispatch said
//!   `--no-rerun` or pinned a workspace, or when no machine that is up can run them; queued jobs
//!   pinned to its workspaces end as lost too. When it comes back it reconciles with each owner
//!   and stops what is no longer its.
//! - **Up front.** Only machines that are up count: a dispatch none of them can run is refused,
//!   and a queued job whose every capable machine is lost ends (see [`capable`]).
//! - **Removed machines.** Like lost, for good; a machine removed itself stops claiming, stops the
//!   jobs it ran for others and says so in `status`.
//! - **Versions.** A member on a newer version makes this machine update itself to that version
//!   (at most once per version, retried with backoff); `potluck self-update` answers first, then
//!   updates this machine to the latest release, and the others follow.

pub(crate) mod capable;
pub(crate) mod owner;
pub(crate) mod peer;
mod relay;
pub(crate) mod remote;
pub(crate) mod views;

pub(crate) use relay::intercept;
pub(crate) use remote::adjust_queue;

use super::state::State;
use super::{Shared, panic_text};
use crate::config::{NodeConfig, Paths};
use crate::mesh::{
    Event, Handler, Member, Mesh, MeshConfig, Peer, Stream, compare_versions, same_version,
};
use crate::types::{JobId, JobState, Millis, NodeName, WorkspaceId};
use peer::{MachineReport, PeerRequest, PeerResponse, StreamHeader, now};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

/// How a runner's mirror reaches an owner's repository: the git URL it fetches from, and the
/// owner's end of it. The real one is [`GitTunnel`]; tests use fakes.
pub trait Tunnel: Send + Sync {
    /// The URL the mirror of `repo_id` syncs from, through `owner`, with the `potluck` binary
    /// the daemon runs its helpers from.
    fn fetch_url(&self, potluck_bin: &Path, owner: &str, repo_id: &str) -> String;
    /// On the owner: serves `git upload-pack` of `view` on a stream a runner opened.
    fn serve_upload_pack(&self, view: &crate::tunnel::View, stream: Stream) -> anyhow::Result<()>;
}

/// The tunnel of cluster-design.md ([`crate::tunnel`]):
/// `ext::<potluck> _tunnel -- <owner> <repo_id>`, which opens an `upload-pack` stream to the
/// owner, and `git upload-pack` on the owner's side.
pub struct GitTunnel;

impl Tunnel for GitTunnel {
    fn fetch_url(&self, potluck_bin: &Path, owner: &str, repo_id: &str) -> String {
        crate::tunnel::fetch_url(potluck_bin, owner, repo_id)
    }
    fn serve_upload_pack(&self, view: &crate::tunnel::View, stream: Stream) -> anyhow::Result<()> {
        Ok(crate::tunnel::serve_upload_pack(stream, view)?)
    }
}

/// Updates this machine's potluck: to a version (`Some`), as a newer member asks, or to the latest
/// release (`None`, for `potluck self-update`). The real one ([`real_updater`]) replaces the binary
/// and restarts the daemon's service, which ends this daemon; with no service to do that, it says
/// so ([`crate::update::Updated::restart_here`]) and the daemon starts the new binary itself.
pub type Updater =
    Arc<dyn Fn(Option<&str>) -> anyhow::Result<crate::update::Updated> + Send + Sync>;

/// [`crate::update::update_daemon`]: the release binary for this machine, then a restart of the
/// service. The daemon runs this for itself, so it passes its own version; a newer member's
/// version only ever moves this machine up.
pub fn real_updater() -> Updater {
    Arc::new(crate::update::update_daemon)
}

/// The cluster's tunables. [`ClusterOptions::new`] reads them from the node config; tests shorten
/// the times and fake the tunnel and the updater.
#[derive(Clone)]
pub struct ClusterOptions {
    pub mesh: MeshConfig,
    /// The cluster loop's period.
    pub tick_ms: u64,
    /// How often each live member's report is pulled.
    pub pull_ms: u64,
    /// How long a machine without a workspace of a job's repository leaves the job to a member
    /// with one.
    pub locality_wait_ms: u64,
    /// How often a runner reconciles with the owners of the jobs it holds.
    pub reconcile_ms: u64,
    /// A job granted this long ago that its runner says it doesn't hold is lost there: queued
    /// again, or ended (125) when it may not run elsewhere.
    pub claim_grace_ms: u64,
    /// A running job's usage goes to its owner at most this often.
    pub usage_report_ms: u64,
    /// A member left alone by removals keeps listening this long (for the removed machine to
    /// learn it), then closes its port.
    pub linger_ms: u64,
    /// Kept files that can't be delivered for this long are given up on: the owner ends the job
    /// without them, with a note saying so; the runner keeps them.
    pub keep_give_up_ms: u64,
    /// A job of this machine's that another machine runs and hasn't ended this long after its run
    /// limit, as this machine counts it, ends here as timed out: the runner's own end at the limit,
    /// with the job's output and kept files, arrives in this time. It counts while this machine is
    /// awake and the runner up.
    pub run_limit_grace_ms: u64,
    pub tunnel: Arc<dyn Tunnel>,
    pub updater: Updater,
}

impl ClusterOptions {
    pub fn new(paths: &Paths, config: &NodeConfig) -> ClusterOptions {
        ClusterOptions {
            mesh: MeshConfig::new(paths, config),
            tick_ms: 500,
            pull_ms: 1_000,
            locality_wait_ms: 3_000,
            reconcile_ms: 10_000,
            claim_grace_ms: 30_000,
            usage_report_ms: 3_000,
            linger_ms: 30_000,
            keep_give_up_ms: 600_000,
            run_limit_grace_ms: 600_000,
            tunnel: Arc::new(GitTunnel),
            updater: real_updater(),
        }
    }
}

/// A member's latest report, when it arrived or a pull found it still current (this machine's
/// clock), and its digest.
pub(crate) struct Remote {
    pub report: MachineReport,
    pub at: Millis,
    pub digest: String,
}

/// A job this machine runs for another: what is left to deliver to its owner. Kept in the store.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Outbox {
    pub owner: NodeName,
    /// The lease token: this run of the job.
    pub token: String,
    /// The whole output log reached the owner (after the job ended).
    pub log_done: bool,
    /// The kept files reached the owner, or were given up on.
    pub keep_done: bool,
    /// When delivering the kept files first failed (this machine's clock); it stays once they are
    /// given up on, and goes once they are delivered.
    #[serde(default)]
    pub keep_since: Option<Millis>,
    /// The owner took the final record; the crash reports go last.
    #[serde(default)]
    pub end_done: bool,
    /// The owner says the job is no longer this machine's: stop it, deliver nothing.
    pub orphaned: bool,
    /// Where this machine's output log starts in the owner's (after the output of runs lost
    /// before this one), once the owner has said so: `logs` asked here while the owner can't be
    /// reached reads this machine's log at the owner's offsets.
    #[serde(default)]
    pub log_at: Option<u64>,
}

/// A claimed job to hand back to its owner (it won't start here).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct GiveBack {
    pub owner: NodeName,
    pub job: JobId,
    pub token: String,
    pub message: String,
}

/// Syncing one repository's mirror from one owner.
#[derive(Default)]
pub(crate) struct SyncState {
    /// Commits wanted, and the jobs that want each.
    pub wants: BTreeMap<String, BTreeSet<JobId>>,
    pub running: bool,
    /// When to try again after a failure (boot clock).
    pub next_at: u64,
    pub backoff_ms: u64,
    pub error: Option<String>,
}

/// The cluster's own state, beside the daemon's [`State`]. Lock order: the daemon's state first,
/// then this; never the other way round, and no network calls while holding either.
#[derive(Default)]
pub(crate) struct CState {
    pub reports: BTreeMap<NodeName, Remote>,
    /// When each member's report was last pulled (boot clock, as every time the cluster keeps
    /// for its periodic work and waits: [`Cluster::boot`]).
    pub pulled_at: BTreeMap<NodeName, u64>,
    pub pulling: BTreeSet<NodeName>,
    pub pushing: BTreeSet<NodeName>,
    /// What the own queue looked like at the last push, and whether a push is due anyway.
    pub pushed: String,
    pub push_due: bool,

    // As an owner.
    /// Repository id -> its source git dir, from the latest dispatch that named it: the ids runners
    /// may sync from this machine (from its mirror; from the source when there is no mirror).
    pub sources: BTreeMap<String, PathBuf>,
    /// When each job was granted to a runner (boot clock; not kept across restarts).
    pub granted_at: BTreeMap<JobId, u64>,
    /// When each job running elsewhere reaches its run limit as this machine counts it (boot
    /// clock): its timeout from the grant, then from when its runner said it started. After a
    /// restart, from the lease's deadline, which this machine set the same way.
    pub run_limits: BTreeMap<JobId, u64>,
    /// Jobs running elsewhere past those run limits: how much of the grace each has used (this
    /// machine awake and the runner up), and the awake clock's reading when that was counted.
    pub overdue: BTreeMap<JobId, (u64, u64)>,
    /// Where the current run's output starts in the job's log (after a run lost earlier).
    pub log_base: BTreeMap<JobId, u64>,
    /// Cancels to forward to the jobs' runners.
    pub cancels: BTreeMap<JobId, owner::CancelFwd>,
    /// Jobs whose end a runner delivered lately: (lease token, when on the boot clock). A
    /// reconcile the runner sent before that may still arrive; it isn't told these are no longer
    /// its.
    pub delivered: BTreeMap<JobId, (String, u64)>,
    /// Jobs that ended on their runner and wait for their kept files: the end, kept aside (kept
    /// in the store).
    pub endings: BTreeMap<JobId, owner::PendingEnd>,

    // As a member others relay requests to.
    /// Signed requests taken lately: each is taken once.
    pub requests_seen: relay::Seen,

    // As a runner.
    pub outbox: BTreeMap<JobId, Outbox>,
    pub couriers: BTreeSet<JobId>,
    pub givebacks: Vec<GiveBack>,
    /// Claims refused lately: not tried again until this time (boot clock).
    pub rejected: BTreeMap<JobId, u64>,
    /// Jobs granted here that this machine did not run because they don't verify (an unsigned or
    /// forged job, or one not for this machine's pool), with why and when (boot clock): never
    /// claimed again while their owners still queue them, nor in the minute after the refusal,
    /// while no report lists the job (the owner has it as starting until the hand-back).
    pub refused: BTreeMap<JobId, (String, u64)>,
    /// Other machines' jobs the latest plan may start: job -> (owner, record).
    pub candidates: BTreeMap<JobId, (NodeName, crate::types::JobRecord)>,
    /// Other machines' jobs granted here: the numbers of the last report the owner made before
    /// the grant. A report numbered so or lower still lists the job as queued: none is planned
    /// from, for that job, until a later one has come.
    pub granted_after: BTreeMap<JobId, (Millis, u64)>,
    /// When this machine first saw a job it could start but leaves to a warm member (boot clock).
    pub cold_since: BTreeMap<JobId, u64>,
    /// The jobs this machine leaves to a warm member now, with why (for `why`).
    pub locality: BTreeMap<JobId, String>,
    /// Members up whose jobs this machine doesn't take yet: back from being away, it compares
    /// membership records with the other members first (for `why` on those owners).
    pub catching_up: BTreeSet<NodeName>,
    /// (repository id, commit) known to be in this machine's mirrors.
    pub present: BTreeSet<(String, String)>,
    pub syncs: BTreeMap<(NodeName, String), SyncState>,
    /// Jobs this machine could run but whose commits its mirrors lack, with why.
    pub unsynced: BTreeMap<JobId, String>,
    /// Warnings from this machine's syncs (a mirror diverged or recloned), for the jobs each sync
    /// was for: a job claimed here gets them in its notes.
    pub sync_notes: BTreeMap<JobId, Vec<String>>,
    /// Commits fetched and pinned under a job id while syncing: job -> repository ids.
    pub fetched: BTreeMap<JobId, BTreeSet<String>>,
    pub ws_owners: BTreeMap<WorkspaceId, NodeName>,
    /// Passes to send: owner -> jobs (one pass per job per entry).
    pub passes: BTreeMap<NodeName, Vec<JobId>>,
    pub sending: BTreeSet<String>,
    pub reconcile_due: BTreeSet<NodeName>,
    /// When this machine last reconciled with each owner, and last dropped the commits fetched
    /// for jobs that went elsewhere (boot clock).
    pub reconciled_at: BTreeMap<NodeName, u64>,
    pub fetched_cleanup_at: u64,

    // Membership and versions.
    /// Why this machine is out of the cluster ("this machine was removed from the cluster by b"),
    /// while it is.
    pub out: Option<String>,
    /// The key this machine had when it went out: it is back in ([`back_in`]) only under that key,
    /// when what put it out turns out void. One that paired again since, with a new key, joins
    /// at `pair --finish`.
    pub out_key: Option<crate::mesh::PublicKey>,
    /// Each member's machine as it last reported it (kept in the store), and members that left.
    pub known: BTreeMap<NodeName, capable::Known>,
    /// Members down for the down time (the mesh said so) or removed, until they are up again.
    pub lost: BTreeSet<NodeName>,
    /// Members the mesh said are up, until it says they are down. A new mesh (this machine began
    /// or stopped listening) says so again of each: that isn't news.
    pub up: BTreeSet<NodeName>,
    /// The listener error last logged (each is logged once).
    pub listen_logged: Option<String>,
    /// A newer version a member runs, and that member.
    pub newer: Option<(String, NodeName)>,
    pub updated_to: BTreeSet<String>,
    /// When an update that failed is tried again (boot clock).
    pub update_at: u64,
    pub update_backoff_ms: u64,
    pub updating: bool,
}

const KV_SOURCES: &str = "cluster/sources";
const KV_LOG_BASE: &str = "cluster/log-base";
const KV_CANCELS: &str = "cluster/cancels";
const KV_ENDINGS: &str = "cluster/endings";
const KV_OUTBOX: &str = "cluster/outbox";
const KV_GIVEBACKS: &str = "cluster/givebacks";
const KV_FETCHED: &str = "cluster/fetched";
const KV_WS_OWNERS: &str = "cluster/ws-owners";
pub(crate) const KV_KNOWN: &str = "cluster/known";

/// Writes one of the cluster's rows; a failed write is made again later ([`State::save_kv`]).
pub(crate) fn save<T: Serialize>(st: &mut State, key: &str, v: &T) {
    match serde_json::to_string(v) {
        Ok(json) => st.save_kv(key, Some(json)),
        Err(e) => st.log.log(format!("store: writing {key}: {e:#}")),
    }
}

fn load<T: DeserializeOwned + Default>(st: &State, key: &str) -> T {
    // A row not written yet (a daemon before this one in the process couldn't): as it is held.
    let held = st.unsaved_kv.get(key).cloned().map(Ok);
    match held.unwrap_or_else(|| st.store.kv(key)) {
        Ok(Some(json)) => serde_json::from_str(&json).unwrap_or_else(|e| {
            st.log.log(format!("store: reading {key}: {e}"));
            T::default()
        }),
        Ok(None) => T::default(),
        Err(e) => {
            st.log.log(format!("store: reading {key}: {e:#}"));
            T::default()
        }
    }
}

/// Whether the mesh listens, and what came of the last try.
#[derive(Default)]
struct Listening {
    /// When to try again after a failure (boot clock).
    next_try: u64,
    backoff_ms: u64,
    error: Option<String>,
    /// Since when this machine has been the only member left (it keeps listening a while, so a
    /// removal reaches the removed machine; boot clock).
    alone_since: Option<u64>,
    /// The mesh now is the one that dials without a listener ([`Mesh::start_dialing`]), put in
    /// when the port was taken, or while this machine is out of its cluster.
    dialing: bool,
    /// The listener and the members' view an earlier daemon in this process handed over (a
    /// restart in place), for the first mesh that listens.
    inherited: Option<(std::net::TcpListener, Vec<crate::mesh::Seen>)>,
}

/// This machine in its cluster.
pub(crate) struct Cluster {
    /// The mesh now: listening while this machine is in a cluster, not listening otherwise.
    mesh: RwLock<Mesh>,
    handler: Arc<dyn Handler>,
    /// Held while the mesh is replaced.
    listening: Mutex<Listening>,
    /// Each mesh has its own event thread; older ones end.
    events_gen: AtomicU64,
    pub opts: ClusterOptions,
    /// This machine's name.
    pub node: NodeName,
    cs: Mutex<CState>,
    wake: (Mutex<bool>, Condvar),
    threads: Mutex<Vec<JoinHandle<()>>>,
    stopping: AtomicBool,
    /// When this daemon's cluster started (this machine's clock): the first of its reports'
    /// numbers.
    pub started_at: Millis,
    /// When the mesh now running started, on the boot clock: a member it hasn't heard from yet
    /// may only not have been reached yet ([`Cluster::mesh_started_at`]).
    mesh_started_at: AtomicU64,
    /// The daemon's clocks ([`crate::daemon::Parts::clock`]): the cluster's periodic work and
    /// waits are measured on its boot clock, which no step of the wall clock moves.
    clock: Arc<dyn crate::daemon::Clock>,
    /// How many times the mesh was replaced: [`set_listener`] wakes the loop only when it was.
    mesh_swaps: AtomicU64,
    /// The number of the last report this machine built.
    report_seq: AtomicU64,
    /// One writer at a time per job's log on the owner.
    log_locks: Mutex<BTreeMap<JobId, Arc<Mutex<()>>>>,
}

impl Cluster {
    /// The mesh now (it changes when this machine joins or leaves a cluster).
    pub fn mesh(&self) -> Mesh {
        self.mesh
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn cs(&self) -> MutexGuard<'_, CState> {
        self.cs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Wakes the cluster loop now.
    pub fn wake(&self) {
        let (m, cv) = &self.wake;
        *m.lock().unwrap_or_else(PoisonError::into_inner) = true;
        cv.notify_all();
    }

    fn sleep(&self, ms: u64) {
        let (m, cv) = &self.wake;
        let g = m.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut g, _) = cv
            .wait_timeout_while(g, Duration::from_millis(ms), |w| !*w)
            .unwrap_or_else(PoisonError::into_inner);
        *g = false;
    }

    /// Pushes this machine's report to the members soon.
    pub fn push_soon(&self) {
        self.cs().push_due = true;
        self.wake();
    }

    /// This machine's pool, from the admission that made it a member (the default pool in a
    /// cluster without users, or out of one).
    pub fn pool_here(&self) -> String {
        self.mesh()
            .pool_of(&self.node)
            .unwrap_or_else(|| crate::mesh::trust::DEFAULT_POOL.to_string())
    }

    /// Whether a member by that name is in the cluster (this machine included).
    pub fn is_member(&self, node: &str) -> bool {
        self.mesh().member(node).is_some()
    }

    /// Whether a member is up as far as the mesh knows and on this machine's version (this machine
    /// always is). A member the mesh hears on another version is not (see [`Cluster::up`]).
    pub fn alive(&self, node: &str) -> bool {
        self.mesh().member(node).is_some_and(|m| self.up(&m))
    }

    /// Whether a member is up and on this machine's version: members on another version refuse
    /// each other, whatever the heartbeats said last.
    pub fn up(&self, m: &Member) -> bool {
        m.this_machine
            || (m.alive
                && m.version
                    .as_deref()
                    .is_none_or(|v| same_version(v, &self.opts.mesh.version)))
    }

    /// When the mesh now running started, on the boot clock. A new mesh (this machine began or
    /// stopped listening, or tried again to) knows no member as up until it hears from it.
    pub fn mesh_started_at(&self) -> u64 {
        self.mesh_started_at.load(Ordering::SeqCst)
    }

    /// Now on the boot clock ([`crate::daemon::clock`]): what the cluster's periodic work and
    /// waits are measured on. Records sent to members carry wall time.
    pub fn boot(&self) -> u64 {
        self.clock.boot_ms()
    }

    /// The mesh's down time: a member silent this long is lost.
    pub fn down_after_ms(&self) -> Millis {
        self.opts.mesh.down_after.as_millis() as Millis
    }

    pub fn removed(&self) -> bool {
        self.cs().out.is_some()
    }

    /// The lock of job `job`'s log on this machine (its owner): one writer at a time.
    fn log_lock(&self, job: &str) -> LogLock<'_> {
        let lock = self
            .log_locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(job.to_string())
            .or_default()
            .clone();
        LogLock {
            c: self,
            job: job.to_string(),
            lock,
        }
    }

    /// A control message to a member, with its answer.
    pub fn call(
        &self,
        node: &str,
        req: &PeerRequest,
        timeout: Duration,
    ) -> Result<PeerResponse, String> {
        let msg = serde_json::to_value(req).map_err(|e| e.to_string())?;
        let v = self
            .mesh()
            .call_timeout(node, msg, timeout)
            .map_err(|e| e.to_string())?;
        serde_json::from_value(v).map_err(|e| format!("a bad answer from {node}: {e}"))
    }

    /// Stops taking members' connections; they wait in the backlog (a restart in place).
    pub fn stop_accepting(&self) {
        self.mesh().stop_accepting();
    }

    /// Stops the loops and the mesh like [`Cluster::stop`], but returns the mesh's listener,
    /// still open, and its view of the members, for the daemon that comes next in this process.
    pub fn hand_over(&self) -> (Option<std::net::TcpListener>, Vec<crate::mesh::Seen>) {
        if self.stopping.swap(true, Ordering::SeqCst) {
            return (None, Vec::new());
        }
        let handed = {
            let _l = self
                .listening
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.mesh().hand_over()
        };
        self.wake();
        let threads =
            std::mem::take(&mut *self.threads.lock().unwrap_or_else(PoisonError::into_inner));
        for t in threads {
            let _ = t.join();
        }
        handed
    }

    /// Stops the loops and the mesh. Calls from members fail after this.
    pub fn stop(&self) {
        if self.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        {
            // Not while the mesh is being replaced.
            let _l = self
                .listening
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.mesh().stop();
        }
        self.wake();
        let threads =
            std::mem::take(&mut *self.threads.lock().unwrap_or_else(PoisonError::into_inner));
        for t in threads {
            let _ = t.join();
        }
    }
}

/// [`Cluster::log_lock`]: its entry in the cluster's map goes with its last holder.
pub(crate) struct LogLock<'a> {
    c: &'a Cluster,
    job: JobId,
    lock: Arc<Mutex<()>>,
}

impl LogLock<'_> {
    pub fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for LogLock<'_> {
    fn drop(&mut self) {
        let mut locks = self
            .c
            .log_locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Held by the map and this one only: nobody else has it, and nobody can take it while
        // the map is locked.
        if Arc::strong_count(&self.lock) == 2 {
            locks.remove(&self.job);
        }
    }
}

/// Starts the mesh and the cluster's state, before the daemon's loop starts: loads what the store
/// keeps, and hands back jobs of other machines that were starting when the daemon last stopped.
/// `listener` and `seen`: what an earlier daemon in this process handed over (a restart in
/// place), for the mesh once it listens.
pub(crate) fn init(
    sh: &Arc<Shared>,
    mut opts: ClusterOptions,
    listener: Option<std::net::TcpListener>,
    seen: Vec<crate::mesh::Seen>,
) -> anyhow::Result<()> {
    let mut cs = CState::default();
    let mut unpins = Vec::new();
    let node = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        cs.sources = load(st, KV_SOURCES);
        cs.log_base = load(st, KV_LOG_BASE);
        cs.cancels = load(st, KV_CANCELS);
        cs.endings = load(st, KV_ENDINGS);
        cs.outbox = load(st, KV_OUTBOX);
        cs.givebacks = load(st, KV_GIVEBACKS);
        cs.fetched = load(st, KV_FETCHED);
        cs.ws_owners = load(st, KV_WS_OWNERS);
        cs.known = load(st, KV_KNOWN);
        remote::give_back_starting(st, &mut cs, &mut unpins);
        st.node.clone()
    };
    for (repo, id) in unpins {
        super::runner::unpin_all(sh, &repo, &id);
    }
    opts.mesh.name = node.clone();
    let handler: Arc<dyn Handler> = Arc::new(PeerHandler {
        sh: Arc::downgrade(sh),
    });
    // Without a listener until `start` sees whether this machine is in a cluster.
    let mesh = Mesh::start_unlistening(opts.mesh.clone(), handler.clone())
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    if mesh.name() != node {
        let name = mesh.name();
        let in_cluster = mesh.members().len() > 1;
        mesh.stop();
        anyhow::bail!(crate::preflight::renamed(
            &sh.paths, &name, &node, in_cluster
        ));
    }
    if mesh.is_removed() {
        cs.out = Some("this machine was removed from the cluster".into());
        cs.out_key = Some(mesh.key());
    }
    // Members up before a restart in place are not news.
    cs.up = seen
        .iter()
        .filter(|s| s.up == Some(true) && !s.name.is_empty())
        .map(|s| s.name.clone())
        .collect();
    let cluster = Arc::new(Cluster {
        mesh: RwLock::new(mesh),
        handler,
        listening: Mutex::new(Listening {
            inherited: listener.map(|l| (l, seen)),
            ..Listening::default()
        }),
        events_gen: AtomicU64::new(0),
        opts,
        node,
        cs: Mutex::new(cs),
        wake: (Mutex::new(false), Condvar::new()),
        threads: Mutex::new(Vec::new()),
        stopping: AtomicBool::new(false),
        started_at: now(),
        mesh_started_at: AtomicU64::new(sh.parts.clock.boot_ms()),
        clock: sh.parts.clock.clone(),
        mesh_swaps: AtomicU64::new(0),
        report_seq: AtomicU64::new(0),
        log_locks: Mutex::new(BTreeMap::new()),
    });
    if sh.cluster.set(cluster.clone()).is_err() {
        cluster.stop();
        anyhow::bail!("the cluster was started twice");
    }
    Ok(())
}

/// Starts the cluster's threads, once the daemon's loop runs: mesh events and the cluster loop;
/// and the listener, when this machine is in a cluster.
pub(crate) fn start(sh: &Arc<Shared>) -> anyhow::Result<()> {
    let Some(c) = sh.cluster() else {
        return Ok(());
    };
    spawn_events(sh, &c, &c.mesh())?;
    listen_warning(sh, &c, fit_listener(sh, &c, true));
    if c.mesh().local_addr().is_none() && c.listen_error().is_none() {
        sh.log.log(format!(
            "cluster: {} is in no cluster, so it doesn't listen for members (`potluck admit` founds one, `potluck pair` joins one)",
            c.node
        ));
    }
    let (sh2, c2) = (sh.clone(), c.clone());
    let tick = std::thread::Builder::new()
        .name("potluck-cluster".into())
        .spawn(move || tick_loop(&sh2, &c2))?;
    c.threads
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(tick);
    Ok(())
}

/// A thread for `mesh`'s events, ending when another mesh replaces it.
fn spawn_events(sh: &Arc<Shared>, c: &Arc<Cluster>, mesh: &Mesh) -> std::io::Result<()> {
    let generation = c.events_gen.fetch_add(1, Ordering::SeqCst) + 1;
    let events = mesh.subscribe();
    let (sh1, c1) = (sh.clone(), c.clone());
    let t = std::thread::Builder::new()
        .name("potluck-mesh-events".into())
        .spawn(move || event_loop(&sh1, &c1, events, generation))?;
    let mut threads = c.threads.lock().unwrap_or_else(PoisonError::into_inner);
    // Those of the meshes before have ended (or soon will): let them go rather than keep them
    // to join at the end (a bind retried every minute for days would keep thousands).
    threads.retain(|t| !t.is_finished());
    threads.push(t);
    Ok(())
}

impl Cluster {
    /// Why the mesh can't listen although this machine is in a cluster, if it can't.
    pub fn listen_error(&self) -> Option<String> {
        self.listening
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .error
            .clone()
    }
}

/// The mesh listens while this machine is in a cluster with another member, and only then: it is
/// replaced by one that does or doesn't. `now_please` skips the wait after a failure (a command
/// asked) and the linger after a removal. Errors say why it can't listen.
pub(crate) fn fit_listener(
    sh: &Arc<Shared>,
    c: &Arc<Cluster>,
    now_please: bool,
) -> Result<(), String> {
    set_listener(sh, c, None, now_please)
}

/// [`fit_listener`], or listening (`Some(true)`) or not whatever the membership.
fn set_listener(
    sh: &Arc<Shared>,
    c: &Arc<Cluster>,
    want: Option<bool>,
    now_please: bool,
) -> Result<(), String> {
    let swaps = c.mesh_swaps.load(Ordering::SeqCst);
    let mut l = c.listening.lock().unwrap_or_else(PoisonError::into_inner);
    let r = set_listener_locked(sh, c, &mut l, want, now_please);
    drop(l);
    // The loop calls this every tick: waking it when nothing changed would keep it from sleeping.
    if c.mesh_swaps.load(Ordering::SeqCst) != swaps {
        c.wake();
    }
    r
}

/// [`set_listener`] with its lock held (a command that admits holds it throughout, so the
/// cluster loop can't close the port between listening and admitting).
fn set_listener_locked(
    sh: &Arc<Shared>,
    c: &Arc<Cluster>,
    l: &mut Listening,
    want: Option<bool>,
    now_please: bool,
) -> Result<(), String> {
    if c.stopping() {
        return Ok(());
    }
    let old = c.mesh();
    let removed = old.is_removed();
    let want = want.unwrap_or_else(|| !removed && old.members().len() > 1);
    let listening = old.local_addr().is_some();
    // Out of its cluster, it still pings the members it knew, with a mesh that dials: what put it
    // out may turn out void, and it refuses every connection meanwhile.
    let dial = !want && removed && !old.members().is_empty();
    let t = c.boot();
    if !want {
        // A listener handed over is for a machine that listens: not this one any more.
        l.inherited = None;
    }
    if want || removed {
        l.alone_since = None;
    }
    if want == listening && !(dial && !l.dialing) {
        l.error = None;
        return Ok(());
    }
    if !now_please {
        if want && t < l.next_try {
            return Err(l.error.clone().unwrap_or_default());
        }
        if !want && !removed {
            let since = *l.alone_since.get_or_insert(t);
            if t < since.saturating_add(c.opts.linger_ms) {
                return Ok(());
            }
        }
    }
    // Trying the port again: while it is still taken, the mesh that dials stays. A new one would
    // know no member as up until it hears from each again (a dispatch meanwhile would be refused
    // as if they were down).
    if want && l.dialing {
        let listen = c.opts.mesh.listen;
        if let Err(e) = std::net::TcpListener::bind(listen) {
            return Err(listen_failed(
                l,
                t,
                format!("can't listen on {listen}: {e}"),
            ));
        }
    }
    // The mesh started in this one's place reads the records from disk. Records this one merged
    // and couldn't save yet (a full disk) would be lost, this machine's own removal among them:
    // the new one would count it a member, hear of the removal again, and so on. It stays until
    // they are saved (its heartbeat tries again).
    if !old.records_saved() {
        return Ok(());
    }
    old.stop();
    let started = if want {
        match l.inherited.take() {
            Some((listener, seen)) => match fitting(sh, listener, c.opts.mesh.listen) {
                Ok(listener) => {
                    Mesh::start_on(c.opts.mesh.clone(), c.handler.clone(), listener, seen)
                }
                Err(e) => Err(e),
            },
            None => Mesh::start(c.opts.mesh.clone(), c.handler.clone()),
        }
    } else if dial {
        Mesh::start_dialing(c.opts.mesh.clone(), c.handler.clone())
    } else {
        Mesh::start_unlistening(c.opts.mesh.clone(), c.handler.clone())
    };
    let (mesh, result) = match started {
        Ok(m) => {
            l.error = None;
            l.backoff_ms = 0;
            l.dialing = dial;
            (m, Ok(()))
        }
        Err(e) => {
            let e = listen_failed(l, t, e.to_string());
            // It still pings the members, so it knows which are up and which are lost.
            match Mesh::start_dialing(c.opts.mesh.clone(), c.handler.clone()) {
                Ok(m) => {
                    l.dialing = true;
                    (m, Err(e))
                }
                Err(e2) => {
                    l.dialing = false;
                    return Err(format!("{e}; and without a listener: {e2}"));
                }
            }
        }
    };
    *c.mesh.write().unwrap_or_else(PoisonError::into_inner) = mesh.clone();
    c.mesh_started_at.store(c.boot(), Ordering::SeqCst);
    c.mesh_swaps.fetch_add(1, Ordering::SeqCst);
    if let Err(e) = spawn_events(sh, c, &mesh) {
        sh.log.log(format!("cluster: mesh events: {e}"));
    }
    match mesh.local_addr() {
        Some(a) => sh.log.log(format!(
            "cluster: listening on {a} as {} (address {})",
            c.node,
            mesh.address()
        )),
        None if result.is_ok() => sh.log.log(format!(
            "cluster: {} is in no cluster now; it stopped listening for members",
            c.node
        )),
        None => {}
    }
    result
}

/// The listener a daemon before a restart in place handed over, if it listens where node.toml
/// says to (`listen`: its address, and its port unless that is 0); else one bound there, as a
/// plain start would (node.toml changed, and `potluck restart` applies it). Kept, a changed
/// `listen` or `port` was ignored while members were given the new address.
fn fitting(
    sh: &Shared,
    handed: std::net::TcpListener,
    listen: std::net::SocketAddr,
) -> Result<std::net::TcpListener, crate::mesh::MeshError> {
    let had = handed.local_addr().ok();
    if had
        .is_some_and(|a| a.ip() == listen.ip() && (listen.port() == 0 || a.port() == listen.port()))
    {
        return Ok(handed);
    }
    // Let go first: the same port on another address could not be bound beside it.
    crate::mesh::release(handed);
    sh.log.log(format!(
        "cluster: node.toml says to listen on {listen}, not on {} as before the restart: \
         listening there",
        had.map_or_else(|| "?".to_string(), |a| a.to_string())
    ));
    std::net::TcpListener::bind(listen)
        .map_err(|e| crate::mesh::MeshError::Invalid(format!("can't listen on {listen}: {e}")))
}

/// The port couldn't be bound (`why`): tried again later, each time a while longer after.
fn listen_failed(l: &mut Listening, t: u64, why: String) -> String {
    l.backoff_ms = (l.backoff_ms * 2).clamp(1_000, 60_000);
    l.next_try = t.saturating_add(l.backoff_ms);
    let e = format!("this machine is in a cluster but can't listen for members: {why}");
    l.error = Some(e.clone());
    e
}

struct PeerHandler {
    sh: Weak<Shared>,
}

impl PeerHandler {
    fn parts(&self) -> Result<(Arc<Shared>, Arc<Cluster>), String> {
        let sh = self.sh.upgrade().ok_or("the daemon is stopping")?;
        if sh.stopping() {
            return Err("the daemon is stopping".into());
        }
        let c = sh.cluster().ok_or("the daemon is starting")?;
        Ok((sh, c))
    }
}

impl Handler for PeerHandler {
    fn call(&self, from: &Peer, msg: Value) -> Result<Value, String> {
        let (sh, c) = self.parts()?;
        let req: PeerRequest = serde_json::from_value(msg)
            .map_err(|e| format!("a request this machine doesn't understand: {e}"))?;
        let resp = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handle_call(&sh, &c, &from.name, req)
        }))
        .map_err(|p| {
            let msg = panic_text(&p);
            sh.log.log(format!(
                "cluster: a request from {}: panicked: {msg}",
                from.name
            ));
            format!("internal error: {msg}")
        })??;
        serde_json::to_value(resp).map_err(|e| e.to_string())
    }

    fn stream(&self, from: &Peer, header: Value, stream: Stream) {
        let Ok((sh, c)) = self.parts() else {
            return;
        };
        let header: StreamHeader = match serde_json::from_value(header) {
            Ok(h) => h,
            Err(e) => {
                sh.log.log(format!(
                    "cluster: a stream from {} with a bad header: {e}",
                    from.name
                ));
                return;
            }
        };
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match header {
            StreamHeader::Log { job, token } => {
                owner::receive_log(&sh, &c, &from.name, &job, &token, stream)
            }
            StreamHeader::Keep { job, token } => {
                owner::receive_keep(&sh, &c, &from.name, &job, &token, stream)
            }
            StreamHeader::Crash { job, token } => {
                owner::receive_crash(&sh, &from.name, &job, &token, stream)
            }
            StreamHeader::Client { req } => relay::serve(&sh, &c, from, &req, stream),
            StreamHeader::UploadPack { repo_id } => {
                owner::upload_pack(&sh, &c, &from.name, &repo_id, stream)
            }
        }));
        if let Err(p) = r {
            sh.log.log(format!(
                "cluster: a stream from {}: panicked: {}",
                from.name,
                panic_text(&p)
            ));
        }
    }
}

fn handle_call(
    sh: &Arc<Shared>,
    c: &Arc<Cluster>,
    from: &str,
    req: PeerRequest,
) -> Result<PeerResponse, String> {
    Ok(match req {
        PeerRequest::Report { have, runners } => {
            let to = owner::Taker::of(&c.mesh(), from);
            let mut report = views::for_member(views::own_report(sh, c), &to);
            if !runners {
                report.runners = None;
            }
            match have {
                Some(h) if report.digest() == h => PeerResponse::Unchanged {
                    started_at: report.started_at,
                    seq: report.seq,
                },
                _ => PeerResponse::Report {
                    report: Box::new(report),
                },
            }
        }
        PeerRequest::Push { report } => {
            views::received(sh, c, from, *report);
            PeerResponse::Ok
        }
        PeerRequest::Claim {
            job,
            lease,
            workspace,
            passed,
        } => {
            // A claim sent again had its passes counted the first time.
            if !owner::repeated_claim(sh, from, &job, &lease.token) {
                owner::passed(sh, c, &passed);
            }
            owner::claim(sh, c, from, &job, *lease, &workspace)
        }
        PeerRequest::Passed { jobs } => {
            owner::passed(sh, c, &jobs);
            PeerResponse::Ok
        }
        PeerRequest::Update { job, keep_error } => owner::update(sh, c, from, *job, keep_error),
        PeerRequest::GiveBack {
            job,
            token,
            message,
        } => {
            owner::give_back(sh, c, from, &job, &token, &message);
            PeerResponse::Ok
        }
        PeerRequest::Reconcile { jobs } => owner::reconcile(sh, c, from, &jobs),
        PeerRequest::Cancel { job, token } => {
            remote::cancel_here(sh, from, &job, &token);
            PeerResponse::Ok
        }
        // Removed ones too, while their records are kept: the asker tells "gone" from "never
        // there". Only what a dispatch into it checks.
        PeerRequest::Workspace { workspace } => PeerResponse::Workspace {
            workspace: sh
                .lock()
                .workspaces
                .get(&workspace)
                .map(views::for_dispatch)
                .map(Box::new),
        },
    })
}

fn event_loop(sh: &Arc<Shared>, c: &Arc<Cluster>, events: Receiver<Event>, generation: u64) {
    while !c.stopping() && c.events_gen.load(Ordering::SeqCst) == generation {
        let ev = match events.recv_timeout(Duration::from_millis(200)) {
            Ok(ev) => ev,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        if c.stopping() {
            return;
        }
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| on_event(sh, c, ev)));
        if let Err(p) = r {
            sh.log
                .log(format!("cluster: an event: panicked: {}", panic_text(&p)));
        }
    }
}

fn on_event(sh: &Arc<Shared>, c: &Arc<Cluster>, ev: Event) {
    match ev {
        Event::Up { node } => {
            let mut cs = c.cs();
            let new = cs.up.insert(node.clone());
            let back = cs.lost.remove(&node);
            if new || back {
                sh.log.log(format!("cluster: {node} is up"));
            }
            cs.pulled_at.remove(&node);
            cs.reconcile_due.insert(node);
            drop(cs);
            c.wake();
        }
        Event::Down { node, why } => {
            {
                let mut cs = c.cs();
                cs.up.remove(&node);
                if cs.lost.insert(node.clone()) {
                    match why {
                        Some(why) => sh
                            .log
                            .log(format!("cluster: {node} is down; last try: {why}")),
                        None => sh.log.log(format!("cluster: {node} is down")),
                    }
                }
            }
            owner::runner_lost(sh, c, &node, "was lost");
            sh.kick();
        }
        Event::Joined { node, by } => {
            sh.log.log(format!("cluster: {by} admitted {node}"));
            c.wake();
        }
        Event::Removed { node, by, here } => {
            // A removal this machine made (`potluck remove`): the command settled it already.
            if here || by.as_deref() == Some(c.node.as_str()) {
                return;
            }
            match &by {
                Some(by) => sh.log.log(format!("cluster: {by} removed {node}")),
                None => sh.log.log(format!(
                    "cluster: {node} left (the member that admitted it was removed)"
                )),
            }
            {
                let mut cs = c.cs();
                cs.reports.remove(&node);
                cs.up.remove(&node);
                cs.lost.insert(node.clone());
            }
            owner::runner_lost(sh, c, &node, "left the cluster");
            remote::owner_gone(sh, c, &node);
            sh.kick();
        }
        Event::SelfRemoved { by } => {
            left_cluster(
                sh,
                c,
                format!("this machine was removed from the cluster by {by}"),
            );
        }
        Event::Dropped { reason } => {
            left_cluster(
                sh,
                c,
                format!("this machine dropped out of the cluster: {reason}"),
            );
        }
        Event::VersionMismatch { node, ours, theirs } => {
            if compare_versions(&theirs, &ours).is_gt() {
                sh.log.log(format!(
                    "cluster: {node} runs potluck {theirs}, newer than this machine's {ours}"
                ));
                c.cs().newer = Some((theirs, node));
                c.wake();
            } else {
                sh.log.log(format!(
                    "cluster: {node} runs potluck {theirs}, older than this machine's {ours}; it updates itself when it notices"
                ));
            }
        }
        Event::Unreachable { node, why } => {
            // EHOSTUNREACH from a member that reaches this machine: on macOS, most likely Local
            // Network privacy blocking the daemon (a LaunchAgent), not the network.
            let hint = if cfg!(target_os = "macos") && why.contains("(os error 65)") {
                "; on macOS that is likely Local Network privacy: allow potluck in System Settings > Privacy & Security > Local Network"
            } else {
                ""
            };
            sh.log.log(format!(
                "cluster: {node} reaches this machine, but this machine can't reach it: {why}{hint}"
            ));
        }
        Event::Refused { address, reason } => {
            sh.log.log(format!(
                "cluster: refused a connection from {address}: {reason}"
            ));
        }
    }
}

/// This machine is out of the cluster: it stops taking and sending work, and its jobs on other
/// machines and theirs here are settled as with a lost machine.
fn left_cluster(sh: &Arc<Shared>, c: &Arc<Cluster>, why: String) {
    sh.log.log(format!(
        "cluster: WARNING: {why}; it runs only its own jobs now (`potluck pair` joins again)"
    ));
    let key = c.mesh().key();
    let others: BTreeSet<NodeName> = {
        let st = sh.lock();
        let mut cs = c.cs();
        cs.out = Some(why);
        cs.out_key = Some(key);
        // Every member it deals with, also those it has no report of (it never reached them, or
        // its daemon just started): the owners of jobs it runs, the runners of its own jobs.
        let mut others: BTreeSet<NodeName> = cs.reports.keys().cloned().collect();
        others.extend(cs.outbox.values().map(|o| o.owner.clone()));
        others.extend(cs.givebacks.iter().map(|g| g.owner.clone()));
        others.extend(cs.syncs.keys().map(|(o, _)| o.clone()));
        others.extend(
            st.jobs
                .values()
                .filter(|j| peer::owner_of(&j.id) == st.node)
                .filter(|j| matches!(j.state, JobState::Starting | JobState::Running))
                .filter_map(|j| j.node.clone()),
        );
        others.remove(&st.node);
        cs.reports.clear();
        cs.candidates.clear();
        others
    };
    for node in others {
        owner::runner_lost(
            sh,
            c,
            &node,
            "is out of reach: this machine left the cluster",
        );
        remote::owner_gone(sh, c, &node);
    }
    listen_warning(sh, c, fit_listener(sh, c, true));
    sh.kick();
}

/// Logs why the mesh can't listen, once for each new reason (the cluster loop tries again every
/// tick); a success clears it, so the same failure later is logged again.
fn listen_warning(sh: &Shared, c: &Cluster, r: Result<(), String>) {
    let mut cs = c.cs();
    match r {
        Ok(()) => cs.listen_logged = None,
        Err(e) if cs.listen_logged.as_deref() != Some(e.as_str()) => {
            sh.log.log(format!("cluster: WARNING: {e}"));
            cs.listen_logged = Some(e);
        }
        Err(_) => {}
    }
}

fn tick_loop(sh: &Arc<Shared>, c: &Arc<Cluster>) {
    while !c.stopping() && !sh.stopping() {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tick(sh, c)));
        if let Err(p) = r {
            sh.log
                .log(format!("cluster: loop: panicked: {}", panic_text(&p)));
        }
        c.sleep(c.opts.tick_ms);
    }
}

fn tick(sh: &Arc<Shared>, c: &Arc<Cluster>) {
    let boot = c.boot();
    back_in(sh, c);
    listen_warning(sh, c, fit_listener(sh, c, false));
    views::pulls_and_pushes(sh, c, boot);
    owner::tick(sh, c, now(), boot);
    remote::tick(sh, c, boot);
    versions(sh, c, boot);
}

/// Takes work again once the mesh counts this machine as a member of its cluster again: its
/// removal, or the voiding of its admission, turned out void itself (a removal of its signer
/// that counts, signed before that member heard of it), as every member that holds those records
/// sees. A machine that paired again since is alone in its new cluster until `pair --finish`.
fn back_in(sh: &Arc<Shared>, c: &Arc<Cluster>) {
    let why = {
        let mut cs = c.cs();
        if cs.out.is_none() {
            return;
        }
        let mesh = c.mesh();
        if mesh.is_removed() || mesh.members().len() < 2 {
            return;
        }
        // Paired again with a new key: its removal still counts, and `pair --finish` says it
        // joined.
        if cs.out_key.is_some_and(|k| k != mesh.key()) {
            return;
        }
        cs.out_key = None;
        cs.out.take()
    };
    if let Some(why) = why {
        sh.log.log(format!(
            "cluster: this machine is in the cluster again: {why}, but that no longer counts"
        ));
        c.wake();
        sh.kick();
    }
}

/// Updates this machine to a newer version a member runs: once per version, retried with backoff
/// when it fails (`now` on the boot clock).
fn versions(sh: &Arc<Shared>, c: &Arc<Cluster>, now: u64) {
    let (version, node) = {
        let mut cs = c.cs();
        let Some((v, n)) = cs.newer.clone() else {
            return;
        };
        if cs.updating || cs.updated_to.contains(&v) || now < cs.update_at {
            return;
        }
        cs.updating = true;
        (v, n)
    };
    sh.log.log(format!(
        "cluster: UPDATING: {node} runs potluck {version} and this machine {}; members talk only on the same version, so this machine updates itself to {version}",
        c.opts.mesh.version
    ));
    let c2 = c.clone();
    sh.spawn_worker("update", move |sh| {
        let r = (c2.opts.updater)(Some(&version));
        let mut cs = c2.cs();
        cs.updating = false;
        match r {
            Ok(u) => {
                cs.updated_to.insert(version.clone());
                sh.log
                    .log(format!("cluster: updated to {version}: {}", u.text));
                if u.restart_here {
                    drop(cs);
                    sh.restart_here();
                }
            }
            Err(e) => {
                cs.update_backoff_ms = (cs.update_backoff_ms * 2).clamp(60_000, 3_600_000);
                cs.update_at = now.saturating_add(cs.update_backoff_ms);
                sh.log.log(format!(
                    "cluster: WARNING: could not update to {version}: {e:#}; trying again in {}",
                    crate::types::fmt_ms(cs.update_backoff_ms)
                ));
            }
        }
    });
}

/// `potluck self-update`: answers first, then updates this machine to the latest release (which
/// restarts the daemon) and logs how it went; the answer names the log, since the caller sees
/// nothing else of it. The other members follow when they meet its new version. Refused while a
/// restart in place drains: no update starts then, and the daemon after it doesn't know of this
/// one.
pub(crate) fn self_update(sh: &Arc<Shared>, c: &Arc<Cluster>) -> Result<String, String> {
    let c2 = c.clone();
    let started = sh.spawn_worker("self-update", move |sh| {
        // Let the answer reach the caller before a restart cuts the connection.
        std::thread::sleep(Duration::from_millis(300));
        match (c2.opts.updater)(None) {
            Ok(u) => {
                sh.log.log(format!("self-update: {}", u.text));
                if u.restart_here {
                    sh.restart_here();
                }
            }
            Err(e) => sh
                .log
                .log(format!("self-update: WARNING: the update failed: {e:#}")),
        }
    });
    if !started {
        return Err(if sh.up.draining() {
            format!(
                "not updating {} now: its daemon is restarting in place; run `potluck self-update` again once the restart is done",
                c.node
            )
        } else {
            format!(
                "not updating {}: the update could not be started; {} says why",
                c.node,
                sh.paths.daemon_log().display()
            )
        });
    }
    Ok(format!(
        "updating {} to the latest release now; how it went goes to {}. The other members update themselves when they meet its new version (`potluck nodes` shows each member's version)",
        c.node,
        sh.paths.daemon_log().display()
    ))
}

/// `pair`, `pair --finish`, `admit`, `remove`.
pub(crate) fn membership(
    sh: &Arc<Shared>,
    c: &Arc<Cluster>,
    req: &crate::proto::Request,
) -> Result<crate::proto::Response, String> {
    use crate::proto::{Request, Response};
    let text = |t: String| Ok(Response::Text { text: t });
    match req {
        Request::PairRequest => {
            // Not while the mesh is being replaced (as after this machine's removal): asked of
            // the mesh going, a removed machine's new key would be on disk but not in the mesh
            // that serves on, which reads the disk before, and `pair --finish` would refuse the
            // reply as for another machine.
            let _l = c.listening.lock().unwrap_or_else(PoisonError::into_inner);
            match c.mesh().pair_request() {
                Ok(t) => text(t),
                Err(e) => Err(e.to_string()),
            }
        }
        Request::PairFinish { reply } => {
            // Not while the mesh is being replaced, as for PairRequest: finished on the mesh
            // going, the reply would be on disk but not in the mesh that serves on, which would
            // stay alone and not listen.
            let mut l = c.listening.lock().unwrap_or_else(PoisonError::into_inner);
            let members = c
                .mesh()
                .pair_finish(reply.trim())
                .map_err(|e| e.to_string())?;
            let listen = set_listener_locked(sh, c, &mut l, None, true);
            drop(l);
            {
                let mut guard = sh.lock();
                let mut cs = c.cs();
                cs.out = None;
                cs.out_key = None;
                cs.reports.clear();
                cs.lost.clear();
                cs.up.clear();
                // Another cluster: what this machine knew of the old one's members goes.
                cs.known.clear();
                save(&mut guard, KV_KNOWN, &cs.known);
            }
            c.wake();
            let names: Vec<String> = members.iter().map(|m| m.name.clone()).collect();
            sh.log
                .log(format!("cluster: joined; members {}", names.join(", ")));
            match listen {
                Ok(()) => text(format!("joined the cluster: {}", names.join(", "))),
                Err(e) => text(format!(
                    "joined the cluster: {}; but {e} (the daemon tries again; free the port or set `port` in the node config and restart it)",
                    names.join(", ")
                )),
            }
        }
        Request::Admit { request, admission } => {
            // The founder's first admission: it listens before it answers, so the reply says
            // where; if nobody is admitted after all, it stops again.
            let mut l = c.listening.lock().unwrap_or_else(PoisonError::into_inner);
            let listening = match c.mesh().is_removed() {
                true => Ok(()),
                false => set_listener_locked(sh, c, &mut l, Some(true), true),
            };
            let admitted = c
                .mesh()
                .admit_signed(request.trim(), admission.as_deref().cloned());
            let fitted = set_listener_locked(sh, c, &mut l, None, true);
            drop(l);
            let reply = admitted.map_err(|e| e.to_string())?;
            // Removed from its cluster, this machine ran `pair` (a new key, alone in a cluster of
            // its own) and has now admitted a machine there: it is in a cluster again, the one it
            // founded, as after `pair --finish`. (Back in under the key it had when it went out
            // is [`back_in`]'s.)
            let founded = {
                let mut guard = sh.lock();
                let mut cs = c.cs();
                let mesh = c.mesh();
                let anew = cs.out.is_some()
                    && !mesh.is_removed()
                    && cs.out_key.is_some_and(|k| k != mesh.key());
                if anew {
                    cs.out = None;
                    cs.out_key = None;
                    cs.reports.clear();
                    cs.lost.clear();
                    cs.up.clear();
                    // Another cluster: what this machine knew of the old one's members goes.
                    cs.known.clear();
                    save(&mut guard, KV_KNOWN, &cs.known);
                }
                anew.then(|| mesh.members())
            };
            if let Some(members) = founded {
                let names: Vec<String> = members.iter().map(|m| m.name.clone()).collect();
                sh.log.log(format!(
                    "cluster: founded a new cluster; members {}",
                    names.join(", ")
                ));
                sh.kick();
            }
            listening.and(fitted).map_err(|e| {
                format!(
                    "admitted, but {e}; free the port (or set `port` in the node config and restart the daemon), then run `potluck admit` again for the reply"
                )
            })?;
            c.wake();
            text(reply)
        }
        Request::Remove { node, removal } => {
            let elders = c.mesh().admitted_before(node);
            let by_user = c.mesh().users_exist();
            {
                // Not while the mesh is being replaced, as for PairRequest: made on the mesh
                // going, the removal would be on disk but not in the mesh that serves on (whose
                // next save drops it there too), and announced from a mesh stopped.
                let _l = c.listening.lock().unwrap_or_else(PoisonError::into_inner);
                c.mesh()
                    .remove_signed(node, removal.as_deref().cloned())
                    .map_err(|e| e.to_string())?;
            }
            sh.log.log(format!("cluster: removed {node}"));
            {
                let mut cs = c.cs();
                cs.reports.remove(node);
                cs.up.remove(node);
                cs.lost.insert(node.clone());
            }
            owner::runner_lost(sh, c, node, "was removed from the cluster");
            remote::owner_gone(sh, c, node);
            sh.kick();
            // A user's removal is final: the machine removed signs nothing that counts.
            Ok(
                match contestable(&c.node, node, &elders).filter(|_| !by_user) {
                    None => Response::Ok,
                    Some(text) => Response::Text { text },
                },
            )
        }
        Request::Membership => Ok(Response::Membership {
            membership: Box::new(c.mesh().membership()),
        }),
        Request::UserInit {
            genesis,
            admissions,
        } => {
            // Not while the mesh is being replaced, as for PairRequest.
            let _l = c.listening.lock().unwrap_or_else(PoisonError::into_inner);
            let name = genesis.name.clone();
            c.mesh()
                .user_init((**genesis).clone(), admissions.clone())
                .map_err(|e| e.to_string())?;
            sh.log.log(format!(
                "cluster: {name} is the first user; from now on only what users sign admits and removes machines"
            ));
            Ok(Response::Ok)
        }
        Request::UserRecord { record } => {
            let _l = c.listening.lock().unwrap_or_else(PoisonError::into_inner);
            let what = match &**record {
                crate::mesh::Record::User(u) => format!("{} made {} a user", u.by, u.name),
                crate::mesh::Record::UserRemoval(r) => format!("{} removed user {}", r.by, r.name),
                _ => String::new(),
            };
            c.mesh()
                .user_record((**record).clone())
                .map_err(|e| e.to_string())?;
            sh.log.log(format!("cluster: {what}"));
            Ok(Response::Ok)
        }
        _ => Err("not a membership request".into()),
    }
}

/// What a removal of `node` signed by `me` leaves open, given the members admitted before
/// `node` (oldest first): nothing when `me` is one of them (`node` can't undo it), else that
/// `node` can undo it, by removing `me` back.
pub(crate) fn contestable(me: &str, node: &str, elders: &[String]) -> Option<String> {
    if elders.iter().any(|e| e == me) {
        return None;
    }
    Some(match elders {
        [] => format!(
            "{node} founded the cluster, so {node} can undo this removal whenever its key is in use: a removal of the founder is never final"
        ),
        [only] => format!(
            "{node} was admitted before every member that is up, so {node} can undo this removal; run `potluck remove {node}` again when {only} is up to make it final"
        ),
        [rest @ .., last] => format!(
            "{node} was admitted before every member that is up, so {node} can undo this removal; run `potluck remove {node}` again when {} or {last} is up to make it final",
            rest.join(", ")
        ),
    })
}

/// The record of a workspace on another machine, asked of that machine, for `dispatch
/// --workspace`: an error when it or its machine is gone. None while that machine runs another
/// version (one of the two is updating): it can't be asked, and the job waits for it.
pub(crate) fn remote_workspace(
    c: &Cluster,
    w: &str,
) -> Result<Option<crate::types::WorkspaceRecord>, String> {
    let node = peer::node_of_ws(w);
    if c.removed() {
        return Err(format!(
            "workspace {w} is on {node}, and this machine was removed from the cluster"
        ));
    }
    if !c.is_member(node) {
        return Err(format!(
            "workspace {w} is gone: {node} is not in the cluster"
        ));
    }
    if !c.alive(node) {
        // Heard from on another version (updating): its workspace is still there.
        if c.mesh().member(node).is_some_and(|m| m.alive) {
            return Ok(None);
        }
        return Err(format!("workspace {w} is gone: its machine {node} is down"));
    }
    match c.call(
        node,
        &PeerRequest::Workspace {
            workspace: w.to_string(),
        },
        Duration::from_secs(15),
    ) {
        Ok(PeerResponse::Workspace {
            workspace: Some(rec),
        }) if rec.state == crate::types::WorkspaceState::Removed => {
            Err(format!("workspace {w} is gone from {node}"))
        }
        Ok(PeerResponse::Workspace {
            workspace: Some(rec),
        }) => Ok(Some(*rec)),
        Ok(PeerResponse::Workspace { workspace: None }) => {
            Err(format!("no workspace {w} on {node}"))
        }
        Ok(other) => Err(format!("an unexpected answer from {node}: {other:?}")),
        Err(e) => Err(format!("can't reach {node}, where workspace {w} is: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JobRecord;

    struct NoCalls;

    impl Handler for NoCalls {
        fn call(&self, _from: &Peer, _msg: Value) -> Result<Value, String> {
            Err("no calls in this test".into())
        }
    }

    /// A cluster on its own, with a mesh that doesn't listen, in `dir`.
    fn lone_cluster(dir: &Path) -> Cluster {
        let paths = Paths {
            home: dir.join("home"),
            config: dir.join("node.toml"),
        };
        let config = NodeConfig {
            name: Some("lone".into()),
            ..Default::default()
        };
        let opts = ClusterOptions::new(&paths, &config);
        let handler: Arc<dyn Handler> = Arc::new(NoCalls);
        let mesh = Mesh::start_unlistening(opts.mesh.clone(), handler.clone()).unwrap();
        Cluster {
            mesh: RwLock::new(mesh),
            handler,
            listening: Mutex::new(Listening::default()),
            events_gen: AtomicU64::new(0),
            opts,
            node: "lone".into(),
            cs: Mutex::new(CState::default()),
            wake: (Mutex::new(false), Condvar::new()),
            threads: Mutex::new(Vec::new()),
            stopping: AtomicBool::new(false),
            started_at: now(),
            mesh_started_at: AtomicU64::new(crate::daemon::clock::boot_ms()),
            clock: Arc::new(crate::daemon::SystemClock),
            mesh_swaps: AtomicU64::new(0),
            report_seq: AtomicU64::new(0),
            log_locks: Mutex::new(BTreeMap::new()),
        }
    }

    /// A daemon in no cluster with the real executor and workspaces (which these tests don't
    /// reach), on a home in `dir`.
    fn lone_daemon(dir: &Path) -> crate::daemon::Daemon {
        daemon_on_port(dir, 0)
    }

    /// [`lone_daemon`], listening on `port` once in a cluster.
    fn daemon_on_port(dir: &Path, port: u16) -> crate::daemon::Daemon {
        daemon_on_port_with(dir, port, Arc::new(crate::daemon::SystemClock))
    }

    /// The machine's clocks, with the boot clock moved on as a test says, and the wall clock as it
    /// is: time passing (`.0`, the awake clock too) or the machine asleep (`.1`, not the awake
    /// clock).
    #[derive(Default)]
    struct BootStep(AtomicU64, AtomicU64);

    impl crate::daemon::Clock for BootStep {
        fn wall_ms(&self) -> Millis {
            crate::types::now_ms()
        }
        fn boot_ms(&self) -> u64 {
            crate::daemon::clock::boot_ms()
                + self.0.load(Ordering::SeqCst)
                + self.1.load(Ordering::SeqCst)
        }
        fn awake_ms(&self) -> u64 {
            crate::daemon::clock::awake_ms() + self.0.load(Ordering::SeqCst)
        }
        fn boot_id(&self) -> String {
            crate::daemon::SystemClock.boot_id()
        }
    }

    fn daemon_on_port_with(
        dir: &Path,
        port: u16,
        clock: Arc<dyn crate::daemon::Clock>,
    ) -> crate::daemon::Daemon {
        let paths = Paths {
            home: dir.join("home"),
            config: dir.join("node.toml"),
        };
        let config = NodeConfig {
            name: Some("lone".into()),
            listen: Some("127.0.0.1".into()),
            address: Some("127.0.0.1".into()),
            port: Some(port),
            ..Default::default()
        };
        let parts = crate::daemon::Parts {
            executor: Arc::from(crate::exec::platform()),
            workspaces: Arc::new(crate::workspace::Workspaces::new(paths.clone())),
            scheduler: Arc::new(crate::daemon::RealScheduler),
            facts: Box::new(|| {
                Ok(crate::types::Facts {
                    node: "lone".into(),
                    os: std::env::consts::OS.into(),
                    ram_mib: 16 << 10,
                    ..Default::default()
                })
            }),
            login_env: BTreeMap::new(),
            clock,
        };
        let opts = crate::daemon::Options {
            potluck_bin: PathBuf::from("/nonexistent/potluck"),
            tick_ms: 100,
            kill_grace_ms: 600,
            stall_ms: 0,
            retention_ms: 14 * 86_400_000,
            facts_refresh_ms: 60_000,
            idle_ms: 600_000,
        };
        let mut cluster = ClusterOptions::new(&paths, &config);
        cluster.tick_ms = 100;
        crate::daemon::Daemon::start_cluster(&paths, &config, parts, opts, cluster).unwrap()
    }

    /// A job record `id` in `state` on `node`, with lease `token`; no machine but `node` can run
    /// it (`--on node=...`).
    fn job_on(id: &str, state: crate::types::JobState, node: &str, token: &str) -> JobRecord {
        let spec = crate::types::JobSpec {
            key: None,
            labels: BTreeMap::new(),
            priority: 3,
            request: Default::default(),
            on: [("node".to_string(), node.to_string())].into(),
            repo: crate::types::RepoRef {
                id: "proj-1234abcd".into(),
                source: PathBuf::from("/nonexistent"),
                commit: "0".repeat(40),
                branch: None,
                submodules: Vec::new(),
                carried: Vec::new(),
            },
            workspace: None,
            advance: false,
            cmd: vec!["true".into()],
            env: BTreeMap::new(),
            unset_env: Default::default(),
            timeout_ms: 60_000,
            hold_ms: None,
            keep: Vec::new(),
            rerun_if_lost: true,
            kill_on_stall: false,
            recipe: None,
            submitted_from: peer::owner_of(id).into(),
            pools: Vec::new(),
            auth: None,
        };
        let mut j = crate::daemon::runner::new_record(id.into(), spec, now());
        j.state = state;
        j.node = Some(node.into());
        j.lease = Some(crate::types::Lease {
            grant: Default::default(),
            granted_at: now(),
            deadline: now() + 60_000,
            token: token.into(),
        });
        j
    }

    /// A daemon ("lone") in a cluster with one more member, `b`: a bare mesh this test speaks
    /// for (it answers no calls).
    fn daemon_with_b(dir: &Path) -> (crate::daemon::Daemon, Mesh) {
        daemon_with_b_on(dir, Arc::new(crate::daemon::SystemClock))
    }

    fn daemon_with_b_on(
        dir: &Path,
        clock: Arc<dyn crate::daemon::Clock>,
    ) -> (crate::daemon::Daemon, Mesh) {
        // Bound once the daemon is in a cluster, at `admit`: not from the range port 0 and
        // outgoing connections take ports from, where another test's connection can take it
        // first ("can't listen on 127.0.0.1:40025: Address already in use").
        let port = crate::mesh::tests::free_port();
        let d = daemon_on_port_with(dir, port, clock);
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        let mut cfg = MeshConfig::new(
            &Paths {
                home: dir.join("b"),
                config: dir.join("b.toml"),
            },
            &NodeConfig::default(),
        );
        cfg.name = "b".into();
        cfg.listen = "127.0.0.1:0".parse().unwrap();
        cfg.address = Some("127.0.0.1".into());
        let b = Mesh::start(cfg, Arc::new(NoCalls)).unwrap();
        let request = b.pair_request().unwrap();
        let reply = match membership(
            &sh,
            &c,
            &crate::proto::Request::Admit {
                request,
                admission: None,
            },
        ) {
            Ok(crate::proto::Response::Text { text }) => text,
            other => panic!("{other:?}"),
        };
        b.pair_finish(&reply).unwrap();
        let start = std::time::Instant::now();
        while !c.alive("b") {
            assert!(start.elapsed() < Duration::from_secs(30), "b never up");
            std::thread::sleep(Duration::from_millis(50));
        }
        (d, b)
    }

    /// `b` calls the daemon with a peer request.
    fn call_as_b(b: &Mesh, req: &PeerRequest) -> PeerResponse {
        let v = b
            .call_timeout(
                "lone",
                serde_json::to_value(req).unwrap(),
                Duration::from_secs(10),
            )
            .unwrap();
        serde_json::from_value(v).unwrap()
    }

    /// A cluster row (here the cancels this machine forwards to runners) whose write fails, as
    /// on a full disk, is written again once the store takes writes, and until then holds a
    /// restart in place back as a job record would: the next daemon starts from the store.
    #[test]
    fn a_cluster_row_that_cant_be_written_is_written_again_and_holds_a_restart_back() {
        let dir = tempfile::tempdir().unwrap();
        let d = lone_daemon(dir.path());
        let sh = d.shared.clone();
        // Nothing can be written: what the store does once closed.
        sh.lock().store.close();
        save(&mut sh.lock(), KV_CANCELS, &["to forward"]);
        let why = crate::daemon::handover::unsaved(&sh);
        assert!(
            why.as_deref()
                .is_some_and(|w| w.contains("can't be written to the store")),
            "{why:?}"
        );
        // Room again.
        sh.lock().store = crate::store::Store::open(&sh.paths.db()).unwrap();
        assert_eq!(crate::daemon::handover::unsaved(&sh), None);
        assert_eq!(
            sh.lock().store.kv(KV_CANCELS).unwrap().as_deref(),
            Some(r#"["to forward"]"#)
        );
        d.stop();
    }

    /// A cluster row a daemon before this one in the process couldn't write (handed to this one
    /// with the restart in place that didn't go on) is read as it was held, not as the store has
    /// it, while it can't be written yet: the cluster starts from it and writes it over the older
    /// row once it can, rather than writing the older one back.
    #[test]
    fn a_cluster_row_not_written_yet_is_read_as_held() {
        let dir = tempfile::tempdir().unwrap();
        let d = lone_daemon(dir.path());
        let sh = d.shared.clone();
        let mut st = sh.lock();
        save(&mut st, KV_CANCELS, &["stored"]);
        st.unsaved_kv
            .insert(KV_CANCELS.into(), Some(r#"["held"]"#.into()));
        assert_eq!(load::<Vec<String>>(&st, KV_CANCELS), ["held"]);
        st.unsaved_kv.insert(KV_CANCELS.into(), None);
        assert!(load::<Vec<String>>(&st, KV_CANCELS).is_empty());
        st.unsaved_kv.clear();
        assert_eq!(load::<Vec<String>>(&st, KV_CANCELS), ["stored"]);
        drop(st);
        d.stop();
    }

    /// A record whose write fails while the daemon drains for a restart in place (here a
    /// cancel, once the store takes no writes: the disk filled after the restart was asked)
    /// holds the restart back: the drain says why and is undone, and the daemon serves on.
    /// Before, the restart was let through once at its start, and the next daemon started from a
    /// store where the job was still queued: it ran a job its owner was told was cancelled.
    #[test]
    fn a_record_that_cant_be_written_during_the_drain_holds_the_restart_back() {
        use crate::types::JobState;
        let dir = tempfile::tempdir().unwrap();
        let d = lone_daemon(dir.path());
        let sh = d.shared.clone();
        let id = "lone-77".to_string();
        {
            let mut st = sh.lock();
            let mut j = job_on(&id, JobState::Queued, "lone", "t");
            (j.node, j.lease) = (None, None);
            j.spec.on = [("node".to_string(), "elsewhere".to_string())].into();
            st.jobs.insert(id.clone(), j);
            st.save_job(&id);
        }
        // The restart may go on when it is asked; then the disk fills, and the job is cancelled.
        assert_eq!(crate::daemon::handover::unsaved(&sh), None);
        sh.lock().store.close();
        {
            let mut st = sh.lock();
            st.jobs.get_mut(&id).unwrap().state = JobState::Cancelled;
            st.save_job(&id);
        }
        let why = d.drain(std::time::Duration::from_secs(5)).unwrap_err();
        assert!(why.contains("job lone-77"), "{why}");
        assert!(!sh.up.draining());
        // Room again: the drain goes through, and the store has the cancel.
        sh.lock().store = crate::store::Store::open(&sh.paths.db()).unwrap();
        d.drain(std::time::Duration::from_secs(5)).unwrap();
        let _inherited = d.hand_over(std::time::Duration::from_secs(5));
        sh.lock().store.close();
        let store = crate::store::Store::open(&sh.paths.db()).unwrap();
        assert_eq!(
            store.job(&id).unwrap().map(|j| j.state),
            Some(JobState::Cancelled)
        );
    }

    #[test]
    fn a_log_stream_stops_when_its_run_is_no_longer_the_runners() {
        use crate::types::JobState;
        use std::io::{BufRead, Write};
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        {
            let mut st = sh.lock();
            st.jobs.insert(
                "lone-1".into(),
                job_on("lone-1", JobState::Running, "b", "t1"),
            );
            st.save_job("lone-1");
        }
        let mut stream = b
            .open_stream(
                "lone",
                serde_json::json!({"kind": "log", "job": "lone-1", "token": "t1"}),
            )
            .unwrap();
        let mut answers = std::io::BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        answers.read_line(&mut line).unwrap();
        assert_eq!(line, "{\"base\":0,\"offset\":0}\n");
        // The job goes back to the queue before b's run wrote anything (no line marks the
        // place in the log); b's stream stays open and goes on.
        owner::runner_lost(&sh, &c, "b", "was lost");
        assert_eq!(sh.lock().job("lone-1").unwrap().state, JobState::Queued);
        stream
            .write_all(b"{\"t\":\"data\",\"at\":0,\"len\":6}\nstale\n")
            .unwrap();
        line.clear();
        let _ = answers.read_line(&mut line);
        let log = sh.paths.job_dir("lone-1").join("output.log");
        assert_eq!(
            std::fs::read_to_string(&log).unwrap_or_default(),
            "",
            "the lost run's output went into the log of the next"
        );
        assert!(line.contains("error"), "{line}");
        b.stop();
        d.stop();
    }

    /// The claim grace is measured on the daemon's boot clock, which no step of the wall clock
    /// moves (a wall clock set forward past it right after a grant took the job back from a
    /// runner still starting it): here the boot clock moves on and the wall clock doesn't.
    #[test]
    fn the_claim_grace_is_measured_on_the_boot_clock() {
        use crate::types::JobState;
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(BootStep::default());
        let (d, b) = daemon_with_b_on(dir.path(), clock.clone());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        {
            let mut st = sh.lock();
            st.jobs.insert(
                "lone-1".into(),
                job_on("lone-1", JobState::Starting, "b", "t1"),
            );
            st.save_job("lone-1");
            c.cs().granted_at.insert("lone-1".into(), c.boot());
        }
        let reconcile = || call_as_b(&b, &PeerRequest::Reconcile { jobs: Vec::new() });
        // Granted a moment ago: b may not list it yet.
        reconcile();
        assert_eq!(sh.lock().job("lone-1").unwrap().state, JobState::Starting);
        clock.0.fetch_add(c.opts.claim_grace_ms, Ordering::SeqCst);
        reconcile();
        let j = sh.lock().job("lone-1").unwrap();
        assert_eq!(j.state, JobState::Queued, "{j:#?}");
        b.stop();
        d.stop();
    }

    /// The owner counts the run limit of a job another machine runs on its own clocks: from the
    /// grant, then from when the runner said it started, whatever deadline the runner's lease and
    /// records give. A job its runner hasn't ended by then, and the grace after (awake), ends here
    /// as timed out (124), and the runner's next record of it is stale.
    #[test]
    fn the_owner_ends_a_job_its_runner_keeps_past_the_run_limit() {
        use crate::types::{JobState, Lease};
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(BootStep::default());
        let (d, b) = daemon_with_b_on(dir.path(), clock.clone());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        let mut queued = job_on("lone-1", JobState::Queued, "b", "t1");
        queued.node = None;
        queued.lease = None;
        {
            let mut st = sh.lock();
            st.jobs.insert("lone-1".into(), queued);
            st.save_job("lone-1");
        }
        let far = now() + 86_400_000;
        let lease = Lease {
            grant: Default::default(),
            granted_at: now(),
            deadline: far,
            token: "t1".into(),
        };
        let claim = call_as_b(
            &b,
            &PeerRequest::Claim {
                job: "lone-1".into(),
                lease: Box::new(lease),
                workspace: "b-w1".into(),
                passed: Vec::new(),
            },
        );
        assert!(matches!(claim, PeerResponse::Granted { .. }), "{claim:?}");
        let job = || sh.lock().job("lone-1").unwrap();
        let deadline = || job().lease.unwrap().deadline;
        assert!(deadline() <= now() + 60_000, "{:#?}", job());
        // b says it started, with a deadline of its own a day away.
        let mut running = job_on("lone-1", JobState::Running, "b", "t1");
        running.lease.as_mut().unwrap().deadline = far;
        running.started_at = Some(now());
        running.workspace = Some("b-w1".into());
        let update = || {
            call_as_b(
                &b,
                &PeerRequest::Update {
                    job: Box::new(running.clone()),
                    keep_error: None,
                },
            )
        };
        assert!(matches!(update(), PeerResponse::Update { stale: false }));
        assert!(deadline() <= now() + 60_000, "{:#?}", job());
        // Past its run limit, within the grace: still b's. Time this machine sleeps uses none
        // of the grace (b's end waits for it to be awake).
        clock.0.fetch_add(61_000, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(300));
        clock
            .1
            .fetch_add(2 * c.opts.run_limit_grace_ms, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(job().state, JobState::Running);
        clock
            .0
            .fetch_add(c.opts.run_limit_grace_ms, Ordering::SeqCst);
        let start = std::time::Instant::now();
        while !job().state.is_done() {
            assert!(start.elapsed() < Duration::from_secs(10), "{:#?}", job());
            std::thread::sleep(Duration::from_millis(50));
        }
        let j = job();
        assert_eq!(
            (j.state, j.exit_code, j.message.as_deref()),
            (
                JobState::TimedOut,
                Some(crate::types::EXIT_TIMEOUT),
                Some(
                    "b had not ended it 10m past its run limit (1m), counted on lone from its start"
                )
            ),
            "{j:#?}"
        );
        assert!(matches!(update(), PeerResponse::Update { stale: true }));
        b.stop();
        d.stop();
    }

    /// Another machine's job caught starting by a restart (its workspace was being seeded) starts
    /// again here in that workspace, as this machine's own do: handed back, its workspace was
    /// removed, and what its owner had queued into that workspace meanwhile failed there.
    #[test]
    fn another_machines_job_caught_starting_starts_again_where_it_was() {
        use crate::daemon::state::{Phase, Run, run_key};
        use crate::types::{JobState, WorkspaceRecord, WorkspaceState};
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            config: dir.path().join("node.toml"),
        };
        std::fs::create_dir_all(&paths.home).unwrap();
        {
            // What a daemon killed while it seeded b-1's new workspace leaves.
            let mut store = crate::store::Store::open(&paths.db()).unwrap();
            let mut j = job_on("b-1", JobState::Starting, "lone", "t1");
            j.workspace = Some("lone-w1".into());
            store.put_job(&j).unwrap();
            store
                .put_workspace(&WorkspaceRecord {
                    id: "lone-w1".into(),
                    node: "lone".into(),
                    repo_id: "proj-1234abcd".into(),
                    commit: "0".repeat(40),
                    branch: None,
                    path: paths.home.join("workspaces/lone-w1"),
                    state: WorkspaceState::Seeding,
                    created_at: now(),
                    last_used_at: now(),
                    held_until: 0,
                    pending: 0,
                    size_bytes: None,
                    advanced_by: None,
                })
                .unwrap();
            let run = Run {
                phase: Phase::Starting,
                new_workspace: true,
                ..Default::default()
            };
            store
                .put_kv(&run_key("b-1"), &serde_json::to_string(&run).unwrap())
                .unwrap();
            let outbox = serde_json::json!({"b-1": {"owner": "b", "token": "t1",
                "log_done": false, "keep_done": false, "orphaned": false}});
            store.put_kv(KV_OUTBOX, &outbox.to_string()).unwrap();
        }
        let d = lone_daemon(dir.path());
        let sh = d.shared.clone();
        let log = std::fs::read_to_string(sh.paths.daemon_log()).unwrap();
        assert!(log.contains("b-1: was starting; starting again\n"), "{log}");
        assert!(!log.contains("handed back"), "{log}");
        assert!(sh.lock().jobs.contains_key("b-1"), "{log}");
        d.stop();
    }

    /// Another machine's job claimed here, its grant not come yet, when the daemon stopped: the
    /// next daemon hands it back, never starting it from what it holds (the queue's entry, without
    /// the command the grant brings). The workspace it was pinned to is free again.
    #[test]
    fn another_machines_job_claimed_but_not_granted_is_handed_back_at_a_restart() {
        use crate::daemon::state::{Phase, Run, run_key};
        use crate::types::{JobState, WorkspaceRecord, WorkspaceState};
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            config: dir.path().join("node.toml"),
        };
        std::fs::create_dir_all(&paths.home).unwrap();
        {
            // What a daemon killed while its claim of b-1 was on its way leaves: the reservation
            // of the queue's entry (no command), and no outbox.
            let mut store = crate::store::Store::open(&paths.db()).unwrap();
            let mut j = job_on("b-1", JobState::Starting, "lone", "t1");
            j.spec.cmd.clear();
            j.spec.workspace = Some("lone-w1".into());
            j.workspace = Some("lone-w1".into());
            store.put_job(&j).unwrap();
            store
                .put_workspace(&WorkspaceRecord {
                    id: "lone-w1".into(),
                    node: "lone".into(),
                    repo_id: "proj-1234abcd".into(),
                    commit: "0".repeat(40),
                    branch: None,
                    path: paths.home.join("workspaces/lone-w1"),
                    state: WorkspaceState::Running,
                    created_at: now(),
                    last_used_at: now(),
                    held_until: 0,
                    pending: 0,
                    size_bytes: None,
                    advanced_by: None,
                })
                .unwrap();
            let run = Run {
                phase: Phase::Starting,
                ..Default::default()
            };
            store
                .put_kv(&run_key("b-1"), &serde_json::to_string(&run).unwrap())
                .unwrap();
        }
        let d = lone_daemon(dir.path());
        let sh = d.shared.clone();
        let log = std::fs::read_to_string(sh.paths.daemon_log()).unwrap();
        assert!(
            log.contains("b-1: was claimed from b and not granted yet\n"),
            "{log}"
        );
        assert!(
            log.contains("b-1: was starting for b; handed back\n"),
            "{log}"
        );
        assert!(!log.contains("starting again"), "{log}");
        assert!(!log.contains("did not start"), "{log}");
        {
            let st = sh.lock();
            assert!(!st.jobs.contains_key("b-1") && !st.runs.contains_key("b-1"));
            assert_eq!(st.workspaces["lone-w1"].state, WorkspaceState::Idle);
        }
        let back = sh.cluster().unwrap().cs().givebacks.clone();
        assert_eq!(back.len(), 1, "{back:?}");
        assert_eq!(
            (back[0].job.as_str(), back[0].token.as_str()),
            ("b-1", "t1")
        );
        d.stop();
    }

    /// The nonce of a signed dispatch is taken once: a second dispatch with it is refused, after a
    /// restart too, until it is a day old (its signing time is refused by then anyway).
    #[test]
    fn a_dispatchs_nonce_is_taken_once_and_kept_a_day() {
        use crate::daemon::server::{forget_nonces, take_nonce};
        use crate::jobauth::NONCE_KEEP_MS;
        let dir = tempfile::tempdir().unwrap();
        let t = now();
        let d = lone_daemon(dir.path());
        let sh = d.shared.clone();
        take_nonce(&mut sh.lock(), "n0nce-0000000000a", t).unwrap();
        take_nonce(&mut sh.lock(), "n0nce-0000000000b", t + 1).unwrap();
        let e = take_nonce(&mut sh.lock(), "n0nce-0000000000a", t + 2).unwrap_err();
        assert!(e.contains("its nonce was seen"), "{e}");
        d.stop();
        let d = lone_daemon(dir.path());
        let sh = d.shared.clone();
        assert!(take_nonce(&mut sh.lock(), "n0nce-0000000000a", t + 3).is_err());
        forget_nonces(&mut sh.lock(), t + NONCE_KEEP_MS);
        assert!(take_nonce(&mut sh.lock(), "n0nce-0000000000a", t + 4).is_err());
        forget_nonces(&mut sh.lock(), t + NONCE_KEEP_MS + 1);
        take_nonce(&mut sh.lock(), "n0nce-0000000000a", t + 5).unwrap();
        // b, a millisecond younger, is kept.
        assert!(take_nonce(&mut sh.lock(), "n0nce-0000000000b", t + 6).is_err());
        d.stop();
    }

    #[test]
    fn kept_files_stop_counting_when_their_run_is_no_longer_the_runners() {
        use crate::proto::{Response, ServerResponse, write_msg};
        use crate::types::JobState;
        use std::io::{BufRead, Write};
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        {
            let mut st = sh.lock();
            st.jobs.insert(
                "lone-1".into(),
                job_on("lone-1", JobState::Running, "b", "t1"),
            );
            st.save_job("lone-1");
        }
        let mut stream = b
            .open_stream(
                "lone",
                serde_json::json!({"kind": "keep", "job": "lone-1", "token": "t1"}),
            )
            .unwrap();
        let mut answers = std::io::BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        answers.read_line(&mut line).unwrap();
        assert_eq!(line, "{\"ok\":true}\n");
        let frame = |resp| ServerResponse {
            reminders: vec![],
            resp,
        };
        write_msg(
            &mut stream,
            &frame(Response::CopyEntry {
                path: "out.txt".into(),
                mode: 0o644,
                dir: false,
                len: 6,
            }),
        )
        .unwrap();
        stream.write_all(b"stale\n").unwrap();
        // The job goes back to the queue while b's kept files are on their way.
        owner::runner_lost(&sh, &c, "b", "was lost");
        assert_eq!(sh.lock().job("lone-1").unwrap().state, JobState::Queued);
        write_msg(&mut stream, &frame(Response::CopyDone)).unwrap();
        line.clear();
        let _ = answers.read_line(&mut line);
        let kept = sh.paths.job_dir("lone-1").join(crate::daemon::files::KEEP);
        assert!(
            !kept.exists(),
            "the lost run's kept files became the job's: {line}"
        );
        assert!(line.contains("error"), "{line}");
        b.stop();
        d.stop();
    }

    /// Audit repro (round 8): `pair` on a machine just removed, asked while the cluster replaces
    /// its mesh (the listening one by one that only dials), was made on the mesh going: the new
    /// key went to disk, and the mesh installed after it had read the disk kept the removed key.
    /// `pair --finish` then refused the reply ("this reply admits another machine"), while the
    /// cluster had admitted the new key under this machine's name. Asked as soon as the removal
    /// is known, it failed in 231 of 240 runs on the M3.
    /// A bare mesh named `name` with its home in `dir`, listening on a port of its own.
    fn bare_mesh(dir: &Path, name: &str) -> Mesh {
        let mut cfg = MeshConfig::new(
            &Paths {
                home: dir.join(name),
                config: dir.join(format!("{name}.toml")),
            },
            &NodeConfig::default(),
        );
        cfg.name = name.into();
        cfg.listen = "127.0.0.1:0".parse().unwrap();
        cfg.address = Some("127.0.0.1".into());
        Mesh::start(cfg, Arc::new(NoCalls)).unwrap()
    }

    /// Audit repro (round 9): `remove` asked while the cluster replaces its mesh (as when the
    /// port it couldn't listen on comes free, or it stops listening, alone) was made on the mesh
    /// going: the removal went to disk after the mesh installed next had read it, so that mesh
    /// still counted the machine a member, and the announcement went out from a stopped mesh.
    /// A later save of that mesh's records took the removal off the disk too. With a swap made
    /// at the same moment as each removal, 29 to 30 of 30 removals were missing from the mesh
    /// installed, and 15 of 30 still after the next swap read the disk again. One member to
    /// remove at a time: a mesh of 30 holds about a thousand connections.
    #[test]
    fn a_removal_asked_while_the_mesh_is_replaced_holds() {
        use crate::proto::{Request, Response};
        const N: usize = 12;
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        let mut kept = Vec::new();
        for i in 0..N {
            let name = format!("m{i}");
            let m = bare_mesh(dir.path(), &name);
            let request = m.pair_request().unwrap();
            let reply = match membership(
                &sh,
                &c,
                &Request::Admit {
                    request,
                    admission: None,
                },
            ) {
                Ok(Response::Text { text }) => text,
                other => panic!("{other:?}"),
            };
            m.pair_finish(&reply).unwrap();
            let go = Arc::new(std::sync::Barrier::new(2));
            let swap = {
                let (sh, c, go) = (sh.clone(), c.clone(), go.clone());
                std::thread::spawn(move || {
                    go.wait();
                    let _ = set_listener(&sh, &c, Some(false), true);
                })
            };
            go.wait();
            match membership(
                &sh,
                &c,
                &Request::Remove {
                    node: name.clone(),
                    removal: None,
                },
            ) {
                Ok(_) => {}
                other => panic!("{other:?}"),
            }
            swap.join().unwrap();
            if c.mesh().member(&name).is_some() {
                kept.push(name);
            }
            m.stop();
        }
        assert!(kept.is_empty(), "removed, but members still: {kept:?}");
        // And on disk: a mesh started from it doesn't count them either.
        let _ = set_listener(&sh, &c, Some(true), true);
        let members: BTreeSet<String> = c.mesh().members().into_iter().map(|m| m.name).collect();
        assert_eq!(members, ["b".to_string(), "lone".into()].into());
        b.stop();
        d.stop();
    }

    /// Audit repro (round 9): `pair --finish` while the cluster replaces its mesh (here: alone
    /// after a removal, it stops listening once the linger is over) was finished on the mesh
    /// going: the reply went to disk after the mesh installed next had read it, which stayed
    /// alone and didn't listen, while `pair --finish` said "joined the cluster". Racing a swap,
    /// 10 of 10 joins were missing from the mesh installed.
    #[test]
    fn a_pair_finished_while_the_mesh_is_replaced_joins() {
        use crate::proto::{Request, Response};
        const N: usize = 10;
        let mut alone = 0;
        for _ in 0..N {
            let dir = tempfile::tempdir().unwrap();
            let d = daemon_on_port(dir.path(), crate::mesh::tests::free_port());
            let sh = d.shared.clone();
            let c = sh.cluster().unwrap();
            // Alone, still listening (as in the linger after its last member was removed).
            set_listener(&sh, &c, Some(true), true).unwrap();
            let request = match membership(&sh, &c, &Request::PairRequest) {
                Ok(Response::Text { text }) => text,
                other => panic!("{other:?}"),
            };
            let b = bare_mesh(dir.path(), "b");
            let reply = b.admit(&request).unwrap();
            let go = Arc::new(std::sync::Barrier::new(2));
            let swap = {
                let (sh, c, go) = (sh.clone(), c.clone(), go.clone());
                std::thread::spawn(move || {
                    go.wait();
                    let _ = set_listener(&sh, &c, Some(false), true);
                })
            };
            go.wait();
            match membership(&sh, &c, &Request::PairFinish { reply }) {
                Ok(Response::Text { text }) => assert!(text.starts_with("joined"), "{text}"),
                other => panic!("{other:?}"),
            }
            swap.join().unwrap();
            // Whichever went first, the mesh serving on is in b's cluster and listens.
            let _ = fit_listener(&sh, &c, true);
            if c.mesh().member("b").is_none() || c.mesh().local_addr().is_none() {
                alone += 1;
            }
            b.stop();
            d.stop();
        }
        assert_eq!(alone, 0, "of {N} joins");
    }

    #[test]
    fn a_pair_asked_while_the_mesh_is_replaced_after_a_removal_joins() {
        use crate::proto::{Request, Response};
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        b.remove("lone").unwrap();
        let start = std::time::Instant::now();
        while c.cs().out.is_none() {
            assert!(start.elapsed() < Duration::from_secs(30), "never out");
            std::thread::sleep(Duration::from_micros(100));
        }
        let request = match membership(&sh, &c, &Request::PairRequest) {
            Ok(Response::Text { text }) => text,
            other => panic!("{other:?}"),
        };
        let key = crate::mesh::trust::PairRequest::from_text(&request)
            .unwrap()
            .key;
        assert_eq!(c.mesh().key(), key);
        let reply = b.admit(&request).unwrap();
        match membership(&sh, &c, &Request::PairFinish { reply }) {
            Ok(Response::Text { text }) => assert!(text.starts_with("joined"), "{text}"),
            other => panic!("{other:?}"),
        }
        b.stop();
        d.stop();
    }

    #[test]
    fn a_removed_machine_that_pairs_again_is_not_said_to_be_back_in() {
        // Removed for good, this machine pairs again with a new key. Between the mesh taking
        // the reply and `pair --finish` saying it joined, the loop must not take it back in as
        // if its removal had turned out void: that removal still counts.
        use crate::proto::{Request, Response};
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        b.remove("lone").unwrap();
        let start = std::time::Instant::now();
        while c.cs().out.is_none() {
            assert!(start.elapsed() < Duration::from_secs(30), "never out");
            std::thread::sleep(Duration::from_millis(50));
        }
        let request = match membership(&sh, &c, &Request::PairRequest) {
            Ok(Response::Text { text }) => text,
            other => panic!("{other:?}"),
        };
        let reply = b.admit(&request).unwrap();
        // What `pair --finish` does first, then the loop's turn before it says it joined.
        c.mesh().pair_finish(&reply).unwrap();
        back_in(&sh, &c);
        let log = std::fs::read_to_string(sh.paths.daemon_log()).unwrap();
        assert!(!log.contains("in the cluster again"), "{log}");
        assert!(c.cs().out.is_some());
        b.stop();
        d.stop();
    }

    #[test]
    fn a_removed_machine_that_founds_a_cluster_of_its_own_takes_work_again() {
        // Removed for good, this machine runs `pair` (a new key, alone in a cluster of its own),
        // then admits another machine instead of finishing the pairing: it is in a cluster again,
        // the new one, and takes work there.
        use crate::proto::{Request, Response};
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        b.remove("lone").unwrap();
        let start = std::time::Instant::now();
        while c.cs().out.is_none() {
            assert!(start.elapsed() < Duration::from_secs(30), "never out");
            std::thread::sleep(Duration::from_millis(50));
        }
        b.stop();
        match membership(&sh, &c, &Request::PairRequest) {
            Ok(Response::Text { .. }) => {}
            other => panic!("{other:?}"),
        }
        let mut cfg = MeshConfig::new(
            &Paths {
                home: dir.path().join("e"),
                config: dir.path().join("e.toml"),
            },
            &NodeConfig::default(),
        );
        cfg.name = "e".into();
        cfg.listen = "127.0.0.1:0".parse().unwrap();
        cfg.address = Some("127.0.0.1".into());
        let e = Mesh::start(cfg, Arc::new(NoCalls)).unwrap();
        let request = e.pair_request().unwrap();
        let reply = match membership(
            &sh,
            &c,
            &Request::Admit {
                request,
                admission: None,
            },
        ) {
            Ok(Response::Text { text }) => text,
            other => panic!("{other:?}"),
        };
        e.pair_finish(&reply).unwrap();
        let start = std::time::Instant::now();
        while !c.alive("e") {
            assert!(start.elapsed() < Duration::from_secs(30), "e never up");
            std::thread::sleep(Duration::from_millis(50));
        }
        assert_eq!(c.cs().out, None, "still out of a cluster it founded");
        let log = std::fs::read_to_string(sh.paths.daemon_log()).unwrap();
        assert!(
            log.contains("cluster: founded a new cluster; members "),
            "{log}"
        );
        assert!(!log.contains("in the cluster again"), "{log}");
        e.stop();
        d.stop();
    }

    #[test]
    fn kept_files_of_two_runs_of_a_job_dont_mix() {
        // A lost run's kept files still on their way while the job's next run delivers its own:
        // the stale stream must neither delete the new run's files nor add its own to them.
        use crate::proto::{Response, ServerResponse, write_msg};
        use crate::types::JobState;
        use std::io::{BufRead, Write};
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let set_run = |token: &str| {
            let mut st = sh.lock();
            st.jobs.insert(
                "lone-1".into(),
                job_on("lone-1", JobState::Running, "b", token),
            );
            st.save_job("lone-1");
        };
        let open = |token: &str| {
            let stream = b
                .open_stream(
                    "lone",
                    serde_json::json!({"kind": "keep", "job": "lone-1", "token": token}),
                )
                .unwrap();
            let mut answers = std::io::BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            answers.read_line(&mut line).unwrap();
            assert_eq!(line, "{\"ok\":true}\n");
            (stream, answers)
        };
        let send = |stream: &mut Stream, path: &str, body: &[u8]| {
            let entry = Response::CopyEntry {
                path: path.into(),
                mode: 0o644,
                dir: false,
                len: body.len() as u64,
            };
            write_msg(
                stream,
                &ServerResponse {
                    reminders: vec![],
                    resp: entry,
                },
            )
            .unwrap();
            stream.write_all(body).unwrap();
        };
        let done = |stream: &mut Stream, answers: &mut std::io::BufReader<Stream>| {
            let resp = ServerResponse {
                reminders: vec![],
                resp: Response::CopyDone,
            };
            write_msg(stream, &resp).unwrap();
            let mut line = String::new();
            let _ = answers.read_line(&mut line);
            line
        };
        let settle = || std::thread::sleep(Duration::from_millis(300));
        // The first run's kept files start on their way.
        set_run("t1");
        let (mut old, mut old_answers) = open("t1");
        send(&mut old, "old-1.txt", b"old\n");
        settle();
        // Its runner was lost meanwhile; the job ran again (on b again, a new lease), and that
        // run's kept files start on their way too.
        set_run("t2");
        let (mut new, mut new_answers) = open("t2");
        send(&mut new, "new-1.txt", b"new\n");
        settle();
        // More of the first run's, then its end (refused: it is no longer b's run).
        send(&mut old, "old-2.txt", b"old\n");
        settle();
        let old_line = done(&mut old, &mut old_answers);
        assert!(old_line.contains("error"), "{old_line}");
        // The rest of the second run's.
        send(&mut new, "new-2.txt", b"new\n");
        let new_line = done(&mut new, &mut new_answers);
        assert_eq!(new_line, "{\"files\":2}\n");
        let kept = sh.paths.job_dir("lone-1").join(crate::daemon::files::KEEP);
        let mut names: Vec<String> = std::fs::read_dir(&kept)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["new-1.txt", "new-2.txt"]);
        settle();
        let left: Vec<_> = std::fs::read_dir(sh.paths.job_dir("lone-1"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .filter(|n| n.to_string_lossy().contains(".part"))
            .collect();
        assert!(left.is_empty(), "{left:?}");
        b.stop();
        d.stop();
    }

    #[test]
    fn a_release_just_after_an_owner_dropped_a_pinned_job_removes_the_workspace() {
        // b had a job queued pinned to this machine's workspace; its queue no longer has it (the
        // job ran here and ended, say) and b said so. `release` of that workspace right after
        // must not keep it for that job.
        use crate::proto::{ClientRequest, Request};
        use crate::types::{JobState, WorkspaceRecord, WorkspaceState};
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let w = WorkspaceRecord {
            id: "lone-w1".into(),
            node: "lone".into(),
            repo_id: "proj-1234abcd".into(),
            commit: "0".repeat(40),
            branch: Some("agent-1".into()),
            path: dir.path().join("home/workspaces/lone-w1"),
            state: WorkspaceState::Held,
            created_at: now(),
            last_used_at: now(),
            held_until: now() + 3_600_000,
            pending: 0,
            size_bytes: None,
            advanced_by: None,
        };
        {
            let mut st = sh.lock();
            st.workspaces.insert(w.id.clone(), w);
            st.save_ws("lone-w1");
        }
        // Only b could run it (`--on node=b`): this machine never claims it.
        let mut pinned = job_on("b-1", JobState::Queued, "b", "t1");
        (pinned.node, pinned.lease) = (None, None);
        pinned.spec.workspace = Some("lone-w1".into());
        let report = |seq: u64, queue: Vec<JobRecord>| {
            Box::new(peer::MachineReport {
                node: "b".into(),
                queue,
                started_at: 1,
                seq,
                ..Default::default()
            })
        };
        let pending = || sh.lock().workspaces["lone-w1"].pending;
        call_as_b(
            &b,
            &PeerRequest::Push {
                report: report(1, vec![pinned]),
            },
        );
        let start = std::time::Instant::now();
        while pending() != 1 {
            assert!(start.elapsed() < Duration::from_secs(10), "never pending");
            std::thread::sleep(Duration::from_millis(20));
        }
        call_as_b(
            &b,
            &PeerRequest::Push {
                report: report(2, vec![]),
            },
        );
        let req = ClientRequest {
            ctx: Default::default(),
            req: Request::Release {
                workspace: "lone-w1".into(),
            },
            auth: None,
        };
        let mut out = Vec::new();
        crate::daemon::server::handle_relayed(&sh, &req, &mut out).unwrap();
        let state = sh.lock().workspaces["lone-w1"].state;
        assert_eq!(
            state,
            WorkspaceState::Removed,
            "{}",
            String::from_utf8_lossy(&out)
        );
        b.stop();
        d.stop();
    }

    #[test]
    fn a_reconcile_that_crosses_the_final_record_drops_nothing() {
        use crate::types::JobState;
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        {
            let mut st = sh.lock();
            st.jobs.insert(
                "lone-1".into(),
                job_on("lone-1", JobState::Running, "b", "t1"),
            );
            st.save_job("lone-1");
            // Ended here as lost while b ran it: b must still be told to drop it.
            let mut lost = job_on("lone-2", JobState::Lost, "b", "t2");
            lost.ended_at = Some(now());
            st.jobs.insert("lone-2".into(), lost);
            st.save_job("lone-2");
        }
        // b listed lone-1 in a reconcile, then delivered its end, and the reconcile came after.
        let mut end = job_on("lone-1", JobState::Succeeded, "b", "t1");
        end.exit_code = Some(0);
        end.ended_at = Some(now());
        assert!(matches!(
            call_as_b(
                &b,
                &PeerRequest::Update {
                    job: Box::new(end),
                    keep_error: None
                }
            ),
            PeerResponse::Update { stale: false }
        ));
        let r = call_as_b(
            &b,
            &PeerRequest::Reconcile {
                jobs: vec![
                    ("lone-1".into(), "t1".into()),
                    ("lone-2".into(), "t2".into()),
                ],
            },
        );
        match r {
            PeerResponse::Reconciled { drop } => assert_eq!(drop, ["lone-2"]),
            other => panic!("{other:?}"),
        }
        let log = std::fs::read_to_string(sh.paths.daemon_log()).unwrap();
        assert!(
            log.contains("b holds jobs that are no longer its: lone-2\n"),
            "{log}"
        );
        b.stop();
        d.stop();
    }

    #[test]
    fn a_pull_of_a_report_that_says_the_same_is_answered_unchanged() {
        use crate::types::{WorkspaceRecord, WorkspaceState};
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let held = |i: u32| WorkspaceRecord {
            id: format!("lone-w{i}"),
            node: "lone".into(),
            repo_id: "proj-1234abcd".into(),
            commit: "0".repeat(40),
            branch: Some("agent-1".into()),
            path: dir.path().join(format!("home/workspaces/lone-w{i}")),
            state: WorkspaceState::Held,
            created_at: now(),
            last_used_at: now(),
            held_until: now() + 3_600_000,
            pending: 0,
            size_bytes: Some(4096),
            advanced_by: None,
        };
        {
            let mut st = sh.lock();
            for i in 1..=300 {
                let w = held(i);
                st.workspaces.insert(w.id.clone(), w);
            }
        }
        let size = |r: &PeerResponse| serde_json::to_vec(r).unwrap().len();
        let r = call_as_b(
            &b,
            &PeerRequest::Report {
                have: None,
                runners: true,
            },
        );
        // Every workspace in full (its record alone is over 200 bytes, the temp path apart).
        assert!(size(&r) > 300 * 200, "{} bytes", size(&r));
        let PeerResponse::Report { mut report } = r else {
            panic!("{r:?}")
        };
        let digest = report.digest();
        // Asked again with what it holds: nothing changed, so only the new report's numbers come.
        let r = call_as_b(
            &b,
            &PeerRequest::Report {
                have: Some(digest.clone()),
                runners: true,
            },
        );
        assert!(size(&r) < 200, "{} bytes: {r:?}", size(&r));
        match r {
            PeerResponse::Unchanged { started_at, seq } => {
                assert_eq!(started_at, report.started_at);
                assert!(seq > report.seq);
            }
            other => panic!("{other:?}"),
        }
        // Something changed: the whole report comes.
        {
            let mut st = sh.lock();
            let w = held(301);
            st.workspaces.insert(w.id.clone(), w);
        }
        let r = call_as_b(
            &b,
            &PeerRequest::Report {
                have: Some(digest),
                runners: true,
            },
        );
        assert!(
            matches!(&r, PeerResponse::Report { report } if report.workspaces.len() == 301),
            "{r:?}"
        );
        b.stop();
        d.stop();
    }

    #[test]
    fn leaving_the_cluster_settles_members_it_has_no_report_of() {
        // This daemon has no report of x (it just started, or never reached x): its job running
        // on x and x's job running here are settled all the same.
        use crate::types::JobState;
        let dir = tempfile::tempdir().unwrap();
        let d = lone_daemon(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        {
            let mut st = sh.lock();
            st.jobs.insert(
                "lone-1".into(),
                job_on("lone-1", JobState::Running, "x", "t1"),
            );
            st.save_job("lone-1");
            st.jobs
                .insert("x-1".into(), job_on("x-1", JobState::Running, "lone", "t2"));
            let mut cs = c.cs();
            cs.outbox.insert(
                "x-1".into(),
                Outbox {
                    owner: "x".into(),
                    token: "t2".into(),
                    log_done: false,
                    keep_done: false,
                    keep_since: None,
                    end_done: false,
                    orphaned: false,
                    log_at: None,
                },
            );
        }
        left_cluster(
            &sh,
            &c,
            "this machine was removed from the cluster by x".into(),
        );
        assert!(
            c.cs().outbox["x-1"].orphaned,
            "x's job here is still this machine's"
        );
        let j = sh.lock().job("lone-1").unwrap();
        assert_eq!(j.state, JobState::Lost, "{j:#?}");
        d.stop();
    }

    #[test]
    fn what_a_runner_knows_of_mirrors_and_owners_goes_with_the_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let d = lone_daemon(dir.path());
        let c = d.shared.cluster().unwrap();
        {
            // Commits seen in the mirrors, a sync and a reconciliation, for jobs of an owner that
            // are long gone: kept for as long as the daemon ran.
            let mut cs = c.cs();
            for i in 0..100 {
                cs.present
                    .insert(("repo-1234abcd".into(), format!("{i:040}")));
            }
            cs.syncs.insert(
                ("gone".into(), "repo-1234abcd".into()),
                SyncState {
                    error: Some("the tunnel is down".into()),
                    ..Default::default()
                },
            );
            cs.reconciled_at.insert("gone".into(), c.boot());
        }
        let start = std::time::Instant::now();
        loop {
            let cs = c.cs();
            if cs.present.is_empty() && cs.syncs.is_empty() && cs.reconciled_at.is_empty() {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "still kept: {} commits, {} syncs, {} reconciliations",
                cs.present.len(),
                cs.syncs.len(),
                cs.reconciled_at.len()
            );
            drop(cs);
            std::thread::sleep(Duration::from_millis(100));
        }
        d.stop();
    }

    /// A restart in place handed over a listener on 0.0.0.0 and node.toml now says 127.0.0.1, on
    /// the same port: the handed one is let go and the port bound on the new address. A child
    /// process another thread is starting holds a copy of every descriptor until it execs (here
    /// half a second): the handed listener listened on in it, and the port couldn't be bound
    /// ("Address already in use"). (macOS binds the one address beside the other anyway.)
    #[test]
    fn a_handed_listener_let_go_frees_its_port_while_a_child_being_started_holds_it() {
        use std::io::Read;
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;
        let dir = tempfile::tempdir().unwrap();
        let d = daemon_on_port(dir.path(), crate::mesh::tests::free_port());
        let handed = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let port = handed.local_addr().unwrap().port();
        let (forked, tell) = std::os::unix::net::UnixStream::pair().unwrap();
        let starting = std::thread::spawn(move || {
            let tell = tell.as_raw_fd();
            let mut child = std::process::Command::new("true");
            // SAFETY: only write and nanosleep between fork and exec.
            unsafe {
                child.pre_exec(move || {
                    libc::write(tell, b"f".as_ptr().cast(), 1);
                    let half = libc::timespec {
                        tv_sec: 0,
                        tv_nsec: 500_000_000,
                    };
                    libc::nanosleep(&half, std::ptr::null_mut());
                    Ok(())
                });
            }
            child.status().unwrap()
        });
        // Forked: the child holds the handed listener.
        (&forked).read_exact(&mut [0u8; 1]).unwrap();
        let listen: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let l = fitting(&d.shared, handed, listen).unwrap();
        assert_eq!(l.local_addr().unwrap(), listen);
        drop(l);
        assert!(starting.join().unwrap().success());
        d.stop();
    }

    #[test]
    fn a_bind_retried_for_days_leaves_no_threads_behind() {
        // The port is taken: the daemon, in a cluster, tries again (1 s, 2 s, 4 s, ... apart).
        // It keeps the mesh that dials while the port is still taken; each mesh it does replace
        // took a thread for its events, and those end and aren't kept.
        let dir = tempfile::tempdir().unwrap();
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let d = daemon_on_port(dir.path(), taken.local_addr().unwrap().port());
        let c = d.shared.cluster().unwrap();
        let mut cfg = MeshConfig::new(
            &Paths {
                home: dir.path().join("b"),
                config: dir.path().join("b.toml"),
            },
            &NodeConfig::default(),
        );
        cfg.name = "b".into();
        cfg.listen = "127.0.0.1:0".parse().unwrap();
        cfg.address = Some("127.0.0.1".into());
        let b = Mesh::start(cfg, Arc::new(NoCalls)).unwrap();
        let reply = c.mesh().admit(&b.pair_request().unwrap()).unwrap();
        b.pair_finish(&reply).unwrap();
        let swaps = || c.events_gen.load(Ordering::SeqCst);
        let start = std::time::Instant::now();
        while c.listen_error().is_none() || start.elapsed() < Duration::from_secs(16) {
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "never tried to listen"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        let threads = c.threads.lock().unwrap().len();
        assert!(
            threads <= 3,
            "{threads} threads kept after {} swaps",
            swaps()
        );
        b.stop();
        d.stop();
    }

    #[test]
    fn a_jobs_log_lock_goes_with_its_last_holder() {
        let dir = tempfile::tempdir().unwrap();
        let c = lone_cluster(dir.path());
        // Each job whose log a runner streams here took a lock that stayed for good.
        for i in 0..100 {
            let l = c.log_lock(&format!("a-{i}"));
            let _g = l.lock();
        }
        let held = c.log_lock("a-held");
        let again = c.log_lock("a-held");
        drop(held);
        assert_eq!(
            c.log_locks.lock().unwrap().keys().collect::<Vec<_>>(),
            ["a-held"],
            "only the lock still held is kept"
        );
        drop(again);
        assert!(c.log_locks.lock().unwrap().is_empty());
        c.mesh().stop();
    }

    /// b's report, sent while it started this machine's job, lists the job as starting; the job's
    /// end arrives after it, before b's next report. `why` here says how it ended, as `get`,
    /// `wait` and `status` do: b's report is older than this machine's record.
    #[test]
    fn why_says_how_a_job_ended_while_its_runners_last_report_is_older() {
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        let mut starting = job_on("lone-1", JobState::Starting, "b", "t1");
        starting.workspace = Some("b-w1".into());
        {
            let mut st = sh.lock();
            st.jobs.insert("lone-1".into(), starting.clone());
            st.save_job("lone-1");
        }
        call_as_b(
            &b,
            &PeerRequest::Push {
                report: Box::new(MachineReport {
                    node: "b".into(),
                    running: vec![starting],
                    started_at: 1,
                    seq: 1,
                    ..Default::default()
                }),
            },
        );
        let mut end = job_on("lone-1", JobState::Succeeded, "b", "t1");
        end.workspace = Some("b-w1".into());
        end.exit_code = Some(0);
        end.ended_at = Some(now());
        end.notes = vec!["b: the mirror had changed (test)".into()];
        assert!(matches!(
            call_as_b(
                &b,
                &PeerRequest::Update {
                    job: Box::new(end),
                    keep_error: None
                }
            ),
            PeerResponse::Update { stale: false }
        ));
        assert_eq!(sh.lock().jobs["lone-1"].state, JobState::Succeeded);
        let why = match views::why(&sh, &c, "lone-1") {
            Ok(crate::proto::Response::Why { why }) => why,
            other => panic!("{other:?}"),
        };
        assert_eq!(why.state, "succeeded", "{why:?}");
        assert!(
            why.lines
                .contains(&"warning: b: the mirror had changed (test)".to_string()),
            "{why:?}"
        );
        b.stop();
        d.stop();
    }

    /// This machine ran b's job b-1 and delivered its end; then b's report from before its grant,
    /// which still lists b-1 as queued, arrives (b's next one is on its way). b-1 isn't claimed
    /// again from it: its record here stays as the run ended. A report b made after the grant
    /// that lists b-1 brings it back (b queued it again).
    #[test]
    fn a_runner_doesnt_take_again_a_job_it_ran_from_the_report_its_run_was_planned_from() {
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let c = sh.cluster().unwrap();
        let mut ran = job_on("b-1", JobState::Succeeded, "lone", "t1");
        ran.exit_code = Some(0);
        ran.ended_at = Some(now());
        {
            let mut st = sh.lock();
            st.jobs.insert("b-1".into(), ran.clone());
            st.save_job("b-1");
        }
        let mut queued = job_on("b-1", JobState::Queued, "lone", "t1");
        (queued.node, queued.lease) = (None, None);
        let rc = (queued.spec.repo.id.clone(), queued.spec.repo.commit.clone());
        let push = |seq: u64, queue: Vec<JobRecord>| {
            // The commit is in this machine's mirror: b-1 could start here now.
            c.cs().present.insert(rc.clone());
            call_as_b(
                &b,
                &PeerRequest::Push {
                    report: Box::new(MachineReport {
                        node: "b".into(),
                        queue,
                        started_at: 1,
                        seq,
                        ..Default::default()
                    }),
                },
            );
        };
        // When b granted b-1, this machine held a report of b's (here one with nothing queued),
        // and the grant said b's report numbered 2 was the last it made before. A plan drops the
        // grant's numbers only once a later report of b's has come, or none is held (b is no
        // longer a member).
        push(1, Vec::new());
        c.cs().granted_after.insert("b-1".into(), (1, 2));
        let states = |for_ms: u64| {
            let mut seen = Vec::new();
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_millis(for_ms) {
                let s = sh.lock().job("b-1").map(|j| j.state);
                if seen.last() != Some(&s) {
                    seen.push(s);
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            seen
        };
        push(2, vec![queued.clone()]);
        // Planned every 100 ms: claimed (and its record replaced, then deleted when the claim
        // failed) within the first few plans before.
        assert_eq!(
            states(1_500),
            [Some(JobState::Succeeded)],
            "{}",
            std::fs::read_to_string(sh.paths.daemon_log()).unwrap()
        );
        // Made after the grant: b queued b-1 again since, and this machine may take it (here the
        // claim fails: b answers no calls).
        push(3, vec![queued.clone()]);
        let seen = states(1_500);
        assert!(
            seen.iter().any(|s| *s != Some(JobState::Succeeded)),
            "{seen:?}\n{}",
            std::fs::read_to_string(sh.paths.daemon_log()).unwrap()
        );
        assert!(c.cs().granted_after.is_empty());
        b.stop();
        d.stop();
    }

    /// A grant carries the numbers of the last report this machine made before it: every report
    /// numbered so or lower lists the job as queued, every later one doesn't.
    #[test]
    fn a_grant_says_which_reports_were_made_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let (d, b) = daemon_with_b(dir.path());
        let sh = d.shared.clone();
        let mut queued = job_on("lone-1", JobState::Queued, "b", "t1");
        (queued.node, queued.lease) = (None, None);
        {
            let mut st = sh.lock();
            st.jobs.insert("lone-1".into(), queued);
            st.save_job("lone-1");
        }
        let report = || match call_as_b(
            &b,
            &PeerRequest::Report {
                have: None,
                runners: true,
            },
        ) {
            PeerResponse::Report { report } => *report,
            other => panic!("{other:?}"),
        };
        let lists = |r: &MachineReport| r.queue.iter().any(|j| j.id == "lone-1");
        let before = report();
        assert!(lists(&before), "{before:?}");
        let lease = crate::types::Lease {
            grant: Default::default(),
            granted_at: now(),
            deadline: now() + 60_000,
            token: "t2".into(),
        };
        let claim = PeerRequest::Claim {
            job: "lone-1".into(),
            lease: Box::new(lease),
            workspace: "b-w1".into(),
            passed: Vec::new(),
        };
        // This machine may first compare membership records with b (it just admitted it).
        let start = std::time::Instant::now();
        let reported = loop {
            match call_as_b(&b, &claim) {
                PeerResponse::Granted { reported, .. } => break reported.unwrap(),
                PeerResponse::Refused { reason } if reason.contains("compares membership") => {
                    assert!(start.elapsed() < Duration::from_secs(20), "{reason}");
                    std::thread::sleep(Duration::from_millis(50));
                }
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(reported.0, before.started_at);
        assert!(reported.1 >= before.seq, "{reported:?} {}", before.seq);
        let after = report();
        assert!(after.seq > reported.1 && !lists(&after), "{after:?}");
        // The same claim again: granted again, with numbers no lower.
        match call_as_b(&b, &claim) {
            PeerResponse::Granted {
                reported: again, ..
            } => {
                assert!(again.unwrap().1 >= reported.1)
            }
            other => panic!("{other:?}"),
        }
        b.stop();
        d.stop();
    }
}
