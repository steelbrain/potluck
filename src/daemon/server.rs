//! The socket: one thread per connection, any number of requests per connection, one response
//! each (several frames for logs and copies). Every response carries the caller's reminders. A
//! connection idle for [`Options::idle_ms`](super::Options::idle_ms) is closed.

use super::clock::Now;
use super::files::{self, reply};
use super::handover::{self, Asked, Ready};
use super::runner::{self, new_record, pin_all, unpin_all};
use super::{Shared, panic_text};
use crate::gitinfo::short;
use crate::proto::{ClientContext, ClientRequest, Request, Response, StatusView, read_msg};
use crate::sched::{MachineView, Queued};
use crate::types::{
    EXIT_CANCELLED, JobRecord, JobSpec, JobState, MAX_HOLD_MS, Reminder, WorkspaceRecord,
    WorkspaceState, now_ms,
};
use crate::why::Snapshot;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The most output one `Logs` answer carries.
const MAX_LOG_CHUNK: u64 = 4 << 20;
const DEFAULT_LOG_CHUNK: u64 = 64 << 10;
/// Finished jobs `status` lists.
const RECENT: usize = 20;

pub(crate) fn accept_loop(sh: Arc<Shared>, listener: UnixListener) {
    use std::os::fd::AsRawFd;
    // Not blocking in accept: a connection that goes between the wait and the accept would block
    // it. Accepted connections are made blocking again (macOS hands them the listener's flag).
    let waits = listener.set_nonblocking(true).is_ok();
    loop {
        // Stopping, or restarting in place: connections from now on wait in the backlog for
        // the next daemon.
        if sh.stopping() || sh.up.quiescing() {
            break;
        }
        if waits {
            match handover::wait_readable(listener.as_raw_fd(), sh.up.wake_fd(), None) {
                Ready::Fd => {}
                Ready::Wake | Ready::TimedOut => continue,
            }
        }
        match listener.accept() {
            Ok((stream, _)) => {
                if stream.set_nonblocking(false).is_err() {
                    continue;
                }
                spawn_conn(&sh, stream, None, Vec::new());
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => {
                sh.log.log(format!("accept: {e}"));
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Serves a client connection on a thread of its own; with `first`, an answer it is owed first
/// (a `restart` handed over from the daemon before), and `behind`, what the client sent behind
/// that `restart` and the daemon before read without answering.
pub(crate) fn spawn_conn(
    sh: &Arc<Shared>,
    stream: UnixStream,
    first: Option<Response>,
    behind: Vec<u8>,
) {
    let Ok(tracked) = stream.try_clone() else {
        return;
    };
    let sh2 = sh.clone();
    match std::thread::Builder::new()
        .name("potluck-conn".into())
        .spawn(move || serve(sh2, stream, first, behind))
    {
        Ok(h) => {
            let mut conns = sh.conns.lock().unwrap_or_else(|e| e.into_inner());
            conns.retain(|(_, h)| !h.is_finished());
            conns.push((tracked, h));
        }
        Err(e) => sh
            .log
            .log(format!("could not start a connection thread: {e}")),
    }
}

/// What a connection's thread does when it stops serving it.
enum After {
    Close,
    /// Hand it over to the next daemon (restarting in place), with the restart it asked for.
    HandOver(Option<Asked>),
}

/// One client connection, until it closes, sends something bad, or sits idle for
/// [`Options::idle_ms`](super::Options::idle_ms) (no request, or no bytes of an answer taken):
/// then it is closed at once, also for the copy [`spawn_conn`] keeps to close it at shutdown.
/// When the daemon restarts in place, it is handed over between two requests instead.
fn serve(sh: Arc<Shared>, stream: UnixStream, first: Option<Response>, behind: Vec<u8>) {
    let idle = (sh.opts.idle_ms > 0).then(|| Duration::from_millis(sh.opts.idle_ms));
    let mut after = After::Close;
    if stream.set_read_timeout(idle).is_ok()
        && stream.set_write_timeout(idle).is_ok()
        && let Ok(read_half) = stream.try_clone()
    {
        let mut w = &stream;
        if first.is_none_or(|r| reply(&mut w, &[], r).is_ok()) {
            let read = Behind {
                read: std::io::Cursor::new(behind),
                stream: read_half,
            };
            after = serve_requests(&sh, read, &stream, idle);
        }
    }
    match after {
        After::Close => {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
        After::HandOver(asked) => sh.up.hand_over(stream, asked),
    }
}

/// A connection's reading half, after what the daemon before a restart in place read from it
/// and didn't answer.
struct Behind {
    read: std::io::Cursor<Vec<u8>>,
    stream: UnixStream,
}

impl Behind {
    /// What is left of it.
    fn left(&self) -> &[u8] {
        let all = self.read.get_ref();
        &all[(self.read.position() as usize).min(all.len())..]
    }
}

impl Read for Behind {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.left().is_empty() {
            self.stream.read(buf)
        } else {
            self.read.read(buf)
        }
    }
}

fn serve_requests(
    sh: &Arc<Shared>,
    read: Behind,
    stream: &UnixStream,
    idle: Option<Duration>,
) -> After {
    use std::os::fd::AsRawFd;
    let mut reader = BufReader::new(read);
    let mut w = stream;
    loop {
        // Between two requests (nothing of the next one read yet): a restart in place hands the
        // connection over as it is, and the next daemon reads the next request.
        if reader.buffer().is_empty() && reader.get_ref().left().is_empty() {
            if sh.up.quiescing() {
                return After::HandOver(None);
            }
            match handover::wait_readable(stream.as_raw_fd(), sh.up.wake_fd(), idle) {
                Ready::Fd => {}
                // Idle past the limit: let go.
                Ready::TimedOut => return After::Close,
                Ready::Wake if sh.up.quiescing() => return After::HandOver(None),
                Ready::Wake if sh.stopping() => return After::Close,
                // Woken for something else (a drain began): wait again.
                Ready::Wake => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
            }
        }
        let req = match read_msg::<_, ClientRequest>(&mut reader) {
            Ok(Some(r)) => r,
            Ok(None) => return After::Close,
            // Idle past the limit (maybe halfway through a request): let go without an answer.
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return After::Close;
            }
            Err(e) => {
                let _ = reply(
                    &mut w,
                    &[],
                    Response::Error {
                        message: format!("bad request: {e}"),
                    },
                );
                return After::Close;
            }
        };
        if sh.stopping() {
            let _ = reply(
                &mut w,
                &[],
                Response::Error {
                    message: crate::client::STOPPING.into(),
                },
            );
            return After::Close;
        }
        if let Request::Restart { binary } = &req.req {
            // Asked by another potluck than the daemon's, which is unchanged at its path: a
            // restart would run it again as it is, not the binary that asked.
            if let Some(why) = other_binary(sh, binary.as_deref()) {
                if reply(&mut w, &[], Response::Error { message: why }).is_err() {
                    return After::Close;
                }
                continue;
            }
            // Answered by the next daemon once it serves (a plain restart: by this one, right
            // before its exec); at once if there won't be one.
            let asked = sh
                .up
                .ask_and_wait("asked by `potluck restart`", &|| sh.stopping());
            match asked {
                Ok(()) => {
                    // What the client sent behind it (read with it): the next daemon reads it.
                    let mut sent_behind = reader.buffer().to_vec();
                    sent_behind.extend_from_slice(reader.get_ref().left());
                    return After::HandOver(Some(Asked {
                        from_version: crate::VERSION.to_string(),
                        sent_behind,
                    }));
                }
                Err(message) => {
                    if reply(&mut w, &[], Response::Error { message }).is_err() {
                        return After::Close;
                    }
                    continue;
                }
            }
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handle(sh, &req, &mut w))) {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return After::Close,
            Err(p) => {
                let msg = panic_text(&p);
                sh.log
                    .log(format!("request {}: panicked: {msg}", describe(&req.req)));
                let _ = reply(
                    &mut w,
                    &[],
                    Response::Error {
                        message: format!("internal error: {msg}"),
                    },
                );
                return After::Close;
            }
        }
    }
}

/// Why a restart asked by the potluck binary `asking` would not run it: the daemon runs the
/// binary at its path, and while that path still names the file the daemon started from, a
/// restart runs that again, as it is. None when `asking` is that file, when the path names
/// another file now (an update replaced it: the restart runs it), or when unknown.
fn other_binary(sh: &Shared, asking: Option<&std::path::Path>) -> Option<String> {
    let asking = asking?;
    let mine = sh.helpers.binary();
    if crate::service::same_file(asking, mine) || !sh.helpers.path_unchanged() {
        return None;
    }
    let (a, b) = (mine.display(), asking.display());
    Some(format!(
        "this daemon runs {a}, not {b} (the potluck that asked), and {a} hasn't changed since \
         it started: a restart in place would run it again as it is, so the daemon was left as \
         it was. To run {b}: replace {a} with it and run `{a} restart`, or stop this daemon and \
         start `{b} daemon` (`{b} service install` for the service)"
    ))
}

/// A request in a log line: what it asks and the job, workspace or machine it names, never its
/// contents (a dispatch's env may hold secrets).
fn describe(req: &Request) -> String {
    let (op, about): (&str, Option<&str>) = match req {
        Request::Ping => ("ping", None),
        Request::Dispatch { spec } => ("dispatch", spec.key.as_deref()),
        Request::Get { job } => ("get", Some(job)),
        Request::Wait { job, .. } => ("wait", Some(job)),
        Request::Logs { job, .. } => ("logs", Some(job)),
        Request::Cancel { job } => ("cancel", Some(job)),
        Request::Hold { workspace, .. } => ("hold", Some(workspace)),
        Request::Release { workspace } => ("release", Some(workspace)),
        Request::Copy { workspace, .. } => ("copy", Some(workspace)),
        Request::Status => ("status", None),
        Request::Why { job } => ("why", Some(job)),
        Request::Nodes => ("nodes", None),
        Request::LeaseCheck { job, .. } => ("lease_check", Some(job)),
        Request::PairRequest => ("pair_request", None),
        Request::PairFinish { .. } => ("pair_finish", None),
        Request::Admit { .. } => ("admit", None),
        Request::Remove { node } => ("remove", Some(node)),
        Request::SelfUpdate => ("self_update", None),
        Request::Restart { .. } => ("restart", None),
    };
    match (req, about) {
        (Request::Dispatch { .. }, Some(key)) => format!("{op} (key {key})"),
        (_, Some(x)) => format!("{op} {x}"),
        (_, None) => op.to_string(),
    }
}

/// The reminders for the caller's repository and branch: its held workspaces here, and in a
/// cluster those on other machines that this machine's jobs used last. The caller names the
/// repository by its unsalted id; its workspaces have it salted with this machine's name, or
/// unsalted when they were made before ids were salted.
pub(crate) fn reminders(sh: &Shared, ctx: &ClientContext) -> Vec<Reminder> {
    let (Some(repo), Some(branch)) = (&ctx.repo_id, &ctx.branch) else {
        return Vec::new();
    };
    let (mut records, node) = {
        let st = sh.lock();
        (st.records(), st.node.clone())
    };
    if let Some(c) = sh.cluster() {
        records.extend(super::cluster::views::remote_reminder_records(&c));
    }
    let now = now_ms();
    let owned = crate::types::owned_repo_id(&node, repo);
    let mut all = sh.parts.workspaces.reminders(now, &records, &owned, branch);
    all.extend(sh.parts.workspaces.reminders(now, &records, repo, branch));
    all.sort_by(|a, b| (a.remaining_ms, &a.workspace).cmp(&(b.remaining_ms, &b.workspace)));
    all
}

fn handle<W: Write>(sh: &Arc<Shared>, req: &ClientRequest, w: &mut W) -> std::io::Result<()> {
    handle_req(sh, req, w, false)
}

/// A request another member relayed here (it is answered here, never relayed again).
pub(crate) fn handle_relayed<W: Write>(
    sh: &Arc<Shared>,
    req: &ClientRequest,
    w: &mut W,
) -> std::io::Result<()> {
    handle_req(sh, req, w, true)
}

fn handle_req<W: Write>(
    sh: &Arc<Shared>,
    req: &ClientRequest,
    w: &mut W,
    relayed: bool,
) -> std::io::Result<()> {
    let _answering = sh.up.serving();
    if sh.cluster().is_some() && super::cluster::intercept(sh, req, w, relayed)? {
        return Ok(());
    }
    let ctx = &req.ctx;
    let result = match &req.req {
        Request::Logs {
            job,
            offset,
            max,
            wait_ms,
        } => return logs(sh, ctx, job, *offset, 0, *max, *wait_ms, w),
        Request::Copy { workspace, path } => {
            let rem = reminders(sh, ctx);
            let root = sh.lock().live_ws(workspace).map(|w| w.path.clone());
            return match root {
                Some(root) => files::send_copy(w, &rem, &root, path),
                None => reply(
                    w,
                    &rem,
                    Response::Error {
                        message: format!("no workspace {workspace}"),
                    },
                ),
            };
        }
        Request::Ping => Ok(Response::Pong {
            version: crate::VERSION.to_string(),
            node: sh.lock().node.clone(),
        }),
        Request::Dispatch { spec } => dispatch(sh, spec),
        Request::Get { job } => sh
            .lock()
            .job(job)
            .map(|j| Response::Job { job: Box::new(j) })
            .ok_or_else(|| format!("no job {job}")),
        Request::Wait { job, timeout_ms } => wait(sh, job, *timeout_ms),
        Request::Cancel { job } => cancel(sh, job),
        Request::Hold {
            workspace,
            duration_ms,
        } => hold(sh, ctx, workspace, *duration_ms),
        Request::Release { workspace } => release(sh, workspace),
        Request::Status => status(sh),
        Request::Why { job } => why(sh, job),
        Request::Nodes => {
            let st = sh.lock();
            let now = now_ms();
            Ok(Response::Nodes {
                nodes: st
                    .machines
                    .keys()
                    .filter_map(|n| st.node_view(n, now))
                    .collect(),
            })
        }
        Request::LeaseCheck { job, token } => Ok(lease_check(sh, job, token)),
        Request::PairRequest
        | Request::PairFinish { .. }
        | Request::Admit { .. }
        | Request::Remove { .. }
        | Request::SelfUpdate => Err("not available yet".to_string()),
        // Only a client of this machine's own socket restarts it (never a relayed request).
        Request::Restart { .. } => Err("a restart is asked on the machine itself".to_string()),
    };
    let resp = result.unwrap_or_else(|message| Response::Error { message });
    let rem = reminders(sh, ctx);
    reply(w, &rem, resp)
}

fn validate(spec: &JobSpec) -> Result<(), String> {
    if !(1..=5).contains(&spec.priority) {
        return Err(format!("priority must be 1 to 5, not {}", spec.priority));
    }
    if spec.timeout_ms == 0 {
        return Err("a run limit (--timeout) is required".into());
    }
    if spec.cmd.is_empty() || spec.cmd[0].is_empty() {
        return Err("no command to run".into());
    }
    // A committed potluck.toml can say what no process can be given; refused here, not after
    // the job has been queued and its workspace seeded.
    if let Some(a) = spec.cmd.iter().find(|a| a.contains('\0')) {
        return Err(format!(
            "the command's argument {a:?} holds a NUL byte, which no program can be given"
        ));
    }
    for (k, v) in &spec.env {
        if k.is_empty() || k.contains(['=', '\0']) {
            return Err(format!(
                "env {k:?}: a variable's name can't be empty or hold '=' or a NUL byte"
            ));
        }
        if v.contains('\0') {
            return Err(format!(
                "env {k:?}: its value holds a NUL byte, which an environment can't hold"
            ));
        }
    }
    if spec.request.ram_mib == 0 {
        return Err("ram must be more than 0".into());
    }
    if spec.repo.commit.is_empty() || spec.repo.id.is_empty() {
        return Err("the dispatch names no repository commit".into());
    }
    crate::types::check_on(&spec.on).map_err(|e| format!("--on: {e}"))?;
    // A glob that can't match inside the workspace would only be dropped when the job ends.
    for g in &spec.keep {
        files::check_keep(g).map_err(|e| format!("--keep {e}"))?;
    }
    Ok(())
}

/// Queues a job, or returns the job its key already names.
fn dispatch(sh: &Arc<Shared>, spec: &JobSpec) -> Result<Response, String> {
    validate(spec)?;
    if let Some(key) = &spec.key {
        let mut st = sh.lock();
        loop {
            if let Some(id) = st.job_by_key(key) {
                return Ok(Response::Dispatched {
                    job: id,
                    existing: true,
                });
            }
            if !st.pending_keys.contains(key) {
                st.pending_keys.insert(key.clone());
                break;
            }
            if sh.stopping() {
                return Err("the daemon is stopping".into());
            }
            // Another dispatch with this key is syncing; its job is the answer.
            st = sh
                .changed
                .wait_timeout(st, Duration::from_millis(200))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
    // The key is let go even if dispatching panics, so a retry isn't stuck behind it.
    struct KeyGuard<'a>(&'a Shared, Option<&'a String>);
    impl Drop for KeyGuard<'_> {
        fn drop(&mut self) {
            if let Some(key) = self.1 {
                self.0.lock().pending_keys.remove(key);
                self.0.notify();
            }
        }
    }
    let _guard = KeyGuard(sh, spec.key.as_ref());
    dispatch_new(sh, spec)
}

fn dispatch_new(sh: &Arc<Shared>, spec: &JobSpec) -> Result<Response, String> {
    let now = now_ms();
    let cluster = sh.cluster();
    // In a cluster: a workspace on another machine is asked about there (the dispatch fails at
    // once if it or its machine is gone; while that machine runs another version, the job waits
    // for it unchecked), and every member's facts must be known.
    let remote = matches!((&spec.workspace, &cluster),
        (Some(w), Some(c)) if super::cluster::peer::node_of_ws(w) != c.node);
    let remote_ws = match (&spec.workspace, &cluster) {
        (Some(w), Some(c)) if remote => super::cluster::remote_workspace(&c.clone(), w)?,
        _ => None,
    };
    if let Some(c) = &cluster {
        super::cluster::views::refresh_unknown(sh, c);
    }
    let (id, spec) = {
        let mut st = sh.lock();
        let spec = owned_spec(&st, spec, remote_ws.as_ref());
        if let Some(w) = &spec.workspace {
            match &remote_ws {
                Some(rec) => check_ws_record(rec, w, &spec)?,
                None if remote => {}
                None => check_workspace(&st, w, &spec)?,
            }
        }
        feasible_somewhere(sh, &st, &spec, now)?;
        let seq = st
            .store
            .next_seq("job")
            .map_err(|e| format!("store: {e:#}"))?;
        (format!("{}-{seq}", st.node), spec)
    };
    let spec = &spec;
    let pinned = sync_mirrors(sh, spec, &id).and_then(|warnings| {
        pin_all(sh, &spec.repo, &id)
            .map(|()| warnings)
            .map_err(|e| format!("could not pin the commits: {e}"))
    });
    let warnings = match pinned {
        Ok(w) => w,
        Err(e) => {
            // A commit fetched for this dispatch is pinned under its id already.
            unpin_all(sh, &spec.repo, &id);
            return Err(e);
        }
    };
    let mut spec = spec.clone();
    {
        let mut st = sh.lock();
        if spec.submitted_from.is_empty() {
            spec.submitted_from = st.node.clone();
        }
        // The workspace may have gone while the mirrors synced.
        if let Some(w) = &spec.workspace
            && !remote
            && let Err(e) = check_workspace(&st, w, &spec)
        {
            drop(st);
            unpin_all(sh, &spec.repo, &id);
            return Err(e);
        }
        if let Some(c) = &cluster {
            super::cluster::owner::dispatched(&mut st, c, &spec.repo);
        }
        let mut record = new_record(id.clone(), spec, now);
        record.notes = warnings
            .into_iter()
            .map(|w| format!("{}: {w}", st.node))
            .collect();
        st.jobs.insert(id.clone(), record);
        st.save_job(&id);
        st.refresh_pending();
    }
    sh.log.log(format!("{id}: queued"));
    sh.notify();
    sh.kick();
    Ok(Response::Dispatched {
        job: id,
        existing: false,
    })
}

/// Refuses a job no machine could ever run, with each machine's reason.
fn feasible_somewhere(
    sh: &Shared,
    st: &super::state::State,
    spec: &JobSpec,
    now: u64,
) -> Result<(), String> {
    let q = Queued {
        job: String::new(),
        priority: spec.priority,
        passes: 0,
        submitted_at: now,
        request: spec.request.clone(),
        on: spec.on.clone(),
        timeout_ms: spec.timeout_ms,
        workspace: spec.workspace.as_ref().map(|w| {
            let node = st
                .workspaces
                .get(w)
                .map(|r| r.node.clone())
                .unwrap_or_else(|| super::cluster::peer::node_of_ws(w).to_string());
            (w.clone(), node, st.ws_busy(w))
        }),
    };
    // Only machines that are up count: a job none of them can run is refused now, not queued
    // for a machine that may never come back. A member updating counts as up.
    let cluster = sh.cluster();
    let cs = cluster.as_ref().map(|c| c.cs());
    let machines = super::cluster::capable::machines(st, cluster.as_deref().zip(cs.as_deref()));
    let reasons = match machines.up_can_run(sh, &q) {
        Ok(()) => return Ok(()),
        Err(reasons) => reasons,
    };
    let others = machines.capable_others(sh, &q);
    // A member alive on another version (the two are mid-update) is up: the job is queued and
    // starts once they run one version, as a job queued before does.
    if others.iter().any(|o| o.updating()) {
        return Ok(());
    }
    let down: Vec<String> = others
        .iter()
        .map(|o| format!("{}: {}", o.node, o.state()))
        .collect();
    if down.is_empty() {
        return Err(format!(
            "no machine could ever run this job ({})",
            reasons.join("; ")
        ));
    }
    Err(format!(
        "no machine that can run this is up ({})",
        down.into_iter()
            .chain(reasons)
            .collect::<Vec<_>>()
            .join("; ")
    ))
}

/// The spec as this machine owns it: its repository ids salted with this machine's name
/// ([`crate::gitinfo::owned_repo`]), so another owner's repository at the same path never shares
/// a mirror with it on a runner. A dispatch into a workspace made before ids were salted (its
/// record has the CLI's unsalted id) keeps the unsalted ids, which that workspace's mirrors and
/// pins have.
fn owned_spec(
    st: &super::state::State,
    spec: &JobSpec,
    remote_ws: Option<&WorkspaceRecord>,
) -> JobSpec {
    let pinned_to = spec
        .workspace
        .as_ref()
        .and_then(|w| remote_ws.or_else(|| st.workspaces.get(w)));
    if pinned_to.is_some_and(|ws| {
        ws.repo_id == spec.repo.id && crate::types::is_unsalted_repo_id(&ws.repo_id)
    }) {
        return spec.clone();
    }
    JobSpec {
        repo: crate::gitinfo::owned_repo(&st.node, &spec.repo),
        ..spec.clone()
    }
}

/// A dispatch into an existing workspace: it must exist and hold the dispatched commit.
fn check_workspace(st: &super::state::State, w: &str, spec: &JobSpec) -> Result<(), String> {
    let Some(ws) = st.live_ws(w) else {
        // Removed ones are remembered until their records are pruned.
        return Err(if st.workspaces.contains_key(w) {
            format!("workspace {w} is gone")
        } else {
            format!("no workspace {w}")
        });
    };
    check_ws_record(ws, w, spec)
}

/// A workspace's record (here or on another machine) against a dispatch into it.
fn check_ws_record(ws: &WorkspaceRecord, w: &str, spec: &JobSpec) -> Result<(), String> {
    if ws.state == WorkspaceState::Removed {
        return Err(format!("workspace {w} is gone"));
    }
    if ws.repo_id != spec.repo.id {
        return Err(format!(
            "workspace {w} holds repository {}, not {}",
            ws.repo_id, spec.repo.id
        ));
    }
    if ws.commit != spec.repo.commit {
        return Err(format!(
            "workspace {w} is at commit {}, but this dispatch is at {}; dispatch without --workspace to run {}",
            short(&ws.commit),
            short(&spec.repo.commit),
            short(&spec.repo.commit)
        ));
    }
    Ok(())
}

/// Brings this machine's mirrors of the repository and its submodules up to date with their
/// sources and makes sure they hold the dispatched commits, fetching (and pinning under the job
/// id) any commit that came with no branch or tag. Returns the warnings to report loudly (a
/// mirror had diverged from its source, or was recloned), already in the daemon's log.
fn sync_mirrors(sh: &Shared, spec: &JobSpec, job: &str) -> Result<Vec<String>, String> {
    let mut warnings = Vec::new();
    let repo = &spec.repo;
    let all = std::iter::once((&repo.id, &repo.source, &repo.commit)).chain(
        repo.submodules
            .iter()
            .map(|s| (&s.repo_id, &s.source, &s.commit)),
    );
    for (repo_id, source, commit) in all {
        let lock = sh.sync_lock(repo_id);
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let url = source.to_string_lossy();
        let warning = sh
            .parts
            .workspaces
            .sync_mirror(repo_id, &url)
            .map_err(|e| format!("could not sync the mirror of {repo_id} from {url}: {e:#}"))?;
        if let Some(w) = warning {
            sh.log.log(format!("WARNING: {job}: {w}"));
            warnings.push(w);
        }
        if !sh.parts.workspaces.has_commit(repo_id, commit) {
            sh.parts
                .workspaces
                .fetch_pin(repo_id, &url, commit, job)
                .map_err(|e| {
                    format!("commit {commit} is not in {url} (only committed work runs): {e:#}")
                })?;
        }
    }
    Ok(warnings)
}

fn wait(sh: &Shared, id: &str, timeout_ms: Option<u64>) -> Result<Response, String> {
    let deadline = timeout_ms.map(|t| Instant::now() + Duration::from_millis(t));
    let mut st = sh.lock();
    loop {
        let job = st.job(id).ok_or_else(|| format!("no job {id}"))?;
        let left = deadline.map(|d| d.saturating_duration_since(Instant::now()));
        if job.state.is_done() || sh.stopping() || sh.up.quiescing() || left == Some(Duration::ZERO)
        {
            return Ok(Response::Job { job: Box::new(job) });
        }
        let step = left
            .unwrap_or(Duration::from_secs(1))
            .min(Duration::from_secs(1));
        st = sh
            .changed
            .wait_timeout(st, step)
            .unwrap_or_else(|e| e.into_inner())
            .0;
    }
}

/// Up to `max` bytes of the job's output from `offset`; with `wait_ms`, waits that long for some.
/// `at` is where this machine's log starts in the log `offset` is in: 0, or, when this machine
/// runs another's job and answers for an owner it can't reach, past the output of earlier runs
/// elsewhere, which only the owner has (`offset` is at or after it).
#[allow(clippy::too_many_arguments)]
pub(crate) fn logs<W: Write>(
    sh: &Shared,
    ctx: &ClientContext,
    id: &str,
    offset: u64,
    at: u64,
    max: u64,
    wait_ms: u64,
    w: &mut W,
) -> std::io::Result<()> {
    let max = if max == 0 {
        DEFAULT_LOG_CHUNK
    } else {
        max.min(MAX_LOG_CHUNK)
    };
    let path = sh.paths.job_dir(id).join(files::LOG);
    let deadline = Instant::now() + Duration::from_millis(wait_ms);
    let from = offset.saturating_sub(at);
    loop {
        let Some(job) = sh.lock().job(id) else {
            let rem = reminders(sh, ctx);
            return reply(
                w,
                &rem,
                Response::Error {
                    message: format!("no job {id}"),
                },
            );
        };
        // Whether the job was over before the log's size is read: then that size is final.
        let done = job.state.is_done();
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if size > from || done || Instant::now() >= deadline || sh.stopping() || sh.up.quiescing() {
            let mut bytes = Vec::new();
            if size > from {
                let want = (size - from).min(max);
                if let Ok(mut f) = std::fs::File::open(&path)
                    && f.seek(SeekFrom::Start(from)).is_ok()
                {
                    let _ = f.take(want).read_to_end(&mut bytes);
                }
            }
            let len = bytes.len() as u64;
            let rem = reminders(sh, ctx);
            reply(
                w,
                &rem,
                Response::Bytes {
                    offset,
                    len,
                    done: done && from + len >= size,
                },
            )?;
            w.write_all(&bytes)?;
            return w.flush();
        }
        let st = sh.lock();
        let left = deadline.saturating_duration_since(Instant::now());
        let _ = sh
            .changed
            .wait_timeout(st, left.min(Duration::from_millis(100)));
    }
}

fn cancel(sh: &Arc<Shared>, id: &str) -> Result<Response, String> {
    let mut unpin = None;
    let job = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let Some(job) = st.jobs.get_mut(id) else {
            // Done long ago, or unknown.
            return st
                .job(id)
                .map(|j| Response::Job { job: Box::new(j) })
                .ok_or_else(|| format!("no job {id}"));
        };
        match job.state {
            JobState::Queued => {
                job.state = JobState::Cancelled;
                job.ended_at = Some(now_ms());
                job.exit_code = Some(EXIT_CANCELLED);
                job.message = Some("cancelled while queued".into());
                unpin = Some(job.spec.repo.clone());
                let j = job.clone();
                st.save_job(id);
                st.refresh_pending();
                sh.log.log(format!("{id}: cancelled while queued"));
                j
            }
            JobState::Starting | JobState::Running => {
                let j = job.clone();
                if let Some(run) = st.runs.get_mut(id) {
                    run.cancel = true;
                }
                st.save_run(id);
                sh.log.log(format!("{id}: cancel asked"));
                j
            }
            _ => job.clone(),
        }
    };
    if let Some(repo) = unpin {
        unpin_all(sh, &repo, id);
    }
    sh.notify();
    sh.kick();
    Ok(Response::Job { job: Box::new(job) })
}

/// Makes W's hold end `duration_ms` from now (at most 24 hours). The hold is the caller's
/// branch's from now on: its reminders go there.
fn hold(sh: &Shared, ctx: &ClientContext, w: &str, duration_ms: u64) -> Result<Response, String> {
    if duration_ms == 0 {
        return Err("a hold needs a duration".into());
    }
    let now = now_ms();
    let mut st = sh.lock();
    let Some(ws) = st.workspaces.get_mut(w) else {
        return Err(format!("no workspace {w}"));
    };
    if ws.state == WorkspaceState::Removed {
        return Err(format!("workspace {w} is gone"));
    }
    ws.held_until = now.saturating_add(duration_ms.min(MAX_HOLD_MS));
    if let Some(b) = &ctx.branch {
        ws.branch = Some(b.clone());
    }
    if ws.state == WorkspaceState::Idle {
        ws.state = WorkspaceState::Held;
    }
    let rec = ws.clone();
    st.save_ws(w);
    Ok(Response::Workspace {
        workspace: Box::new(rec),
    })
}

/// Ends a workspace's hold and removes it, unless an invocation is queued or running in it (then
/// only the hold ends, and an invocation running in it doesn't hold it again when it ends).
fn release(sh: &Arc<Shared>, w: &str) -> Result<Response, String> {
    let now = now_ms();
    let rec = {
        let mut guard = sh.lock();
        let st = &mut *guard;
        let busy = st.ws_busy(w);
        let Some(ws) = st.workspaces.get_mut(w) else {
            return Err(format!("no workspace {w}"));
        };
        if ws.state == WorkspaceState::Removed {
            return Err(format!("workspace {w} is gone"));
        }
        ws.held_until = ws.held_until.min(now);
        if ws.pending > 0 || busy {
            if ws.state == WorkspaceState::Held {
                ws.state = WorkspaceState::Idle;
            }
            let rec = ws.clone();
            st.save_ws(w);
            // The invocation running (or ending) in it would hold it again at its end.
            let running: Vec<String> = st
                .runs
                .keys()
                .filter(|id| {
                    st.jobs
                        .get(*id)
                        .is_some_and(|j| j.workspace.as_deref() == Some(w))
                })
                .cloned()
                .collect();
            for id in running {
                if let Some(run) = st.runs.get_mut(&id) {
                    run.released = true;
                }
                st.save_run(&id);
            }
            return Ok(Response::Workspace {
                workspace: Box::new(rec),
            });
        }
        ws.state = WorkspaceState::Removed;
        let rec = ws.clone();
        st.save_ws(w);
        rec
    };
    runner::remove_workspace(sh, &rec).map_err(|e| format!("could not remove {w}: {e:#}"))?;
    Ok(Response::Workspace {
        workspace: Box::new(rec),
    })
}

fn status(sh: &Shared) -> Result<Response, String> {
    let st = sh.lock();
    let now = now_ms();
    let queue = st.queue();
    let order = sh.parts.scheduler.order(&queue);
    let queued: Vec<JobRecord> = order
        .iter()
        .filter_map(|&i| queue.get(i))
        .filter_map(|q| st.jobs.get(&q.job).cloned())
        .collect();
    let running: Vec<JobRecord> = st
        .jobs
        .values()
        .filter(|j| matches!(j.state, JobState::Starting | JobState::Running))
        .cloned()
        .collect();
    let recent = st
        .store
        .recent_jobs(RECENT)
        .map_err(|e| format!("store: {e:#}"))?;
    Ok(Response::Status {
        status: Box::new(StatusView {
            nodes: st
                .machines
                .keys()
                .filter_map(|n| st.node_view(n, now))
                .collect(),
            running,
            queued,
            recent,
            workspaces: st.records(),
            notes: Vec::new(),
        }),
    })
}

fn why(sh: &Shared, id: &str) -> Result<Response, String> {
    let st = sh.lock();
    let job = st.job(id).ok_or_else(|| format!("no job {id}"))?;
    let snaps = st.snaps();
    let views: Vec<MachineView> = snaps.iter().map(|(_, s)| s.view()).collect();
    let mut jobs: Vec<JobRecord> = st
        .jobs
        .values()
        .filter(|j| !j.state.is_done())
        .cloned()
        .collect();
    if job.state.is_done() {
        jobs.push(job);
    }
    let queue = st.queue();
    let snap = Snapshot {
        now: now_ms(),
        machines: &views,
        jobs: &jobs,
        queue: &queue,
        // `explain` plans each machine again from the snapshot.
        reservation: None,
    };
    Ok(Response::Why {
        why: sh.parts.scheduler.explain(id, &snap),
    })
}

/// Whether `token` is the live lease of job `id`, and what that lease holds.
fn lease_check(sh: &Shared, id: &str, token: &str) -> Response {
    let st = sh.lock();
    let lease = st
        .jobs
        .get(id)
        .filter(|j| matches!(j.state, JobState::Starting | JobState::Running))
        .and_then(|j| j.lease.as_ref())
        .filter(|l| same_secret(&l.token, token));
    Response::Lease {
        live: lease.is_some(),
        grant: lease.map(|l| l.grant.clone()),
        // On the boot clock, as the run limit is enforced: a step of the wall clock moves
        // neither.
        remaining_ms: lease.and_then(|_| st.time_left(id, Now::read(&*sh.parts.clock))),
    }
}

/// Compares secrets without stopping at the first difference.
fn same_secret(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}
