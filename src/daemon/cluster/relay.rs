//! CLI requests in a cluster. The CLI only talks to its own daemon; a request about a job owned by
//! another machine, a workspace on another machine, or another machine's login environment
//! (`potluck env NODE`), is relayed there over a `client` stream and answered from there, frame by
//! frame (with this machine's reminders). Status, nodes and why cover every member; pairing,
//! admission, removal and self-update go to the mesh. In a cluster without users, a removal goes
//! to the oldest member up that was admitted before the machine removed, which makes it final; in
//! one with users, a user signs it and no member signs one for another.
//!
//! In a cluster with users, a member answers a relayed request only when a user signed it
//! ([`RequestAuth`]): a recognized user's key, signed within [`RequestAuth::WINDOW_MS`] of the
//! member's clock, and not answered before. Whoever signs it, a member answers about a job only
//! when it owns it or runs it now; a cancel of a job it runs for another owner goes to that owner;
//! and it answers about a workspace only when the workspace is its own.

use super::peer::{StreamHeader, node_of_ws, owner_of};
use super::{Cluster, membership, owner, self_update, views};
use crate::daemon::Shared;
use crate::daemon::files::reply;
use crate::daemon::server::{self, describe, handle_relayed, reminders};
use crate::mesh::{Peer, PublicKey};
use crate::proto::{
    ClientContext, ClientRequest, NEEDS_USER_KEY, Request, RequestAuth, Response, ServerResponse,
    TooLong, read_msg_within,
};
use crate::types::Millis;
use std::collections::BTreeMap;
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A relayed answer longer than this between frames is given up on (long polls ask for less).
const RELAY_IDLE: Duration = Duration::from_secs(90);

/// The longest message of a relayed answer this machine reads, its newline not counted: as long
/// as a control message between members may be ([`crate::mesh::MAX_FRAME`]). The longest an
/// honest member answers with are a job's record (`get`, `wait`, `cancel`, `resize`), which
/// crossed the mesh in a control message already if the job ran on another member, and a login
/// environment (`env NODE`), no more than one process can be given (a few MiB). A `why`, a
/// workspace's record (`hold`, `release`), and the head of a `logs` answer's output or of each
/// file of a `cp` are kilobytes. The output and the files follow their heads as raw bytes, copied
/// through as they come: a `logs` answer's no more than the request asked for and the answering
/// daemon gives ([`most_output`]; the command line holds one answer's output with `--json`), a
/// file of a `cp` as long as the file is (the command line writes it to disk as it comes).
const MAX_ANSWER: u64 = crate::mesh::MAX_FRAME;

/// The most output a `Bytes` frame of an answer to `req` carries: what a `logs` request asks
/// for, as the answering daemon caps it; none for any other request.
fn most_output(req: &Request) -> u64 {
    match req {
        Request::Logs { max, .. } => server::log_chunk(*max),
        _ => 0,
    }
}

enum RelayError {
    /// Nothing was answered: the caller may answer otherwise.
    NotStarted(String),
    /// The member's answer had a message longer than [`MAX_ANSWER`]: it was dropped, and the
    /// connection closed. Why, for the command line.
    TooLong(String),
    /// The answer broke off midway: the connection can only be closed.
    Broken(std::io::Error),
}

/// Answers `req` if the cluster has something to do with it; false to let the daemon answer it as
/// on one machine. `relayed` requests came from another member and are never relayed again.
pub(crate) fn intercept<W: Write>(
    sh: &Arc<Shared>,
    req: &ClientRequest,
    w: &mut W,
    relayed: bool,
) -> std::io::Result<bool> {
    let Some(c) = sh.cluster() else {
        return Ok(false);
    };
    let me = c.node.clone();
    let answer = |w: &mut W, r: Result<Response, String>| {
        let rem = reminders(sh, &req.ctx);
        reply(
            w,
            &rem,
            r.unwrap_or_else(|message| Response::Error { message }),
        )
        .map(|()| true)
    };
    match &req.req {
        Request::Get { job }
        | Request::Wait { job, .. }
        | Request::Logs { job, .. }
        | Request::Why { job }
        | Request::Cancel { job }
        | Request::Resize { job, .. } => {
            let owner = owner_of(job);
            if owner != me {
                if relayed || !c.is_member(owner) || c.removed() {
                    return Ok(false);
                }
                if let Some(why) = unsigned(&c, req, owner) {
                    return answer(w, Err(why));
                }
                let began = Instant::now();
                loop {
                    let now = for_now(req, began.elapsed());
                    let e = match relay(sh, &c, owner, &now, w) {
                        Ok(()) => return Ok(true),
                        Err(RelayError::Broken(e)) => return Err(e),
                        Err(RelayError::TooLong(why)) => return answer(w, Err(why)),
                        Err(RelayError::NotStarted(e)) => e,
                    };
                    // The machine running the job answers from its own record and log, the log
                    // at the owner's offsets. Not one that ran it before: it may have run
                    // elsewhere since, and its record ended when it was taken away.
                    match (runs(&c, job), &now.req) {
                        (
                            Some(Some(at)),
                            Request::Logs {
                                offset,
                                max,
                                wait_ms,
                                ..
                            },
                        ) if *offset >= at => {
                            let ctx = &req.ctx;
                            server::logs(sh, ctx, job, *offset, at, *max, *wait_ms, w)?;
                            return Ok(true);
                        }
                        // Output from before this run (only the owner has it), or this run's
                        // place in the owner's log isn't known here yet.
                        (_, Request::Logs { .. }) | (None, _) => {}
                        (Some(_), _) => return Ok(false),
                    }
                    // Restarting, or on another version while one of the two updates.
                    let away = c.mesh().member(owner).is_some_and(|m| m.alive);
                    if away && waits(sh, &req.req, began.elapsed()) {
                        std::thread::sleep(AWAY_RETRY);
                        continue;
                    }
                    let why = format!("can't reach {owner}, the machine job {job} belongs to: {e}");
                    return match no_news(&c, &req.req, &why).filter(|_| away) {
                        Some(r) => answer(w, Ok(r)),
                        None => answer(w, Err(why)),
                    };
                }
            }
            match &req.req {
                Request::Why { job } => answer(w, views::why(sh, &c, job)),
                Request::Cancel { job } => match owner::cancel_remote(sh, &c, job) {
                    Some(r) => answer(w, Ok(r)),
                    None => Ok(false),
                },
                _ => Ok(false),
            }
        }
        Request::Copy { workspace, .. }
        | Request::Hold { workspace, .. }
        | Request::Release { workspace } => {
            let node = node_of_ws(workspace);
            if node == me || relayed {
                return Ok(false);
            }
            if !c.is_member(node) {
                return answer(
                    w,
                    Err(format!(
                        "workspace {workspace} is on {node}, which is not in the cluster"
                    )),
                );
            }
            if let Some(why) = unsigned(&c, req, node) {
                return answer(w, Err(why));
            }
            match relay(sh, &c, node, req, w) {
                Ok(()) => Ok(true),
                Err(RelayError::Broken(e)) => Err(e),
                Err(RelayError::TooLong(why)) => answer(w, Err(why)),
                Err(RelayError::NotStarted(e)) => answer(
                    w,
                    Err(format!(
                        "can't reach {node}, where workspace {workspace} is: {e}"
                    )),
                ),
            }
        }
        Request::Env { node: Some(node) } if *node != me && !relayed => {
            if c.removed() {
                return answer(
                    w,
                    Err(format!(
                        "{node} can't be asked: this machine was removed from the cluster; run \
                         `potluck pair` to join again"
                    )),
                );
            }
            let Some(m) = c.mesh().member(node) else {
                return answer(
                    w,
                    Err(format!(
                        "{node} is not a member of the cluster (`potluck nodes` lists them)"
                    )),
                );
            };
            if !c.up(&m) {
                let why = match m.version.as_deref().filter(|_| m.alive) {
                    Some(theirs) => format!(
                        "{node} runs potluck {theirs}, not this machine's {}, and members on \
                         two versions don't answer each other's requests",
                        c.opts.mesh.version
                    ),
                    None => format!("{node} is down, and only its own daemon has its environment"),
                };
                return answer(w, Err(why));
            }
            if let Some(why) = unsigned(&c, req, node) {
                return answer(w, Err(why));
            }
            match relay(sh, &c, node, req, w) {
                Ok(()) => Ok(true),
                Err(RelayError::Broken(e)) => Err(e),
                Err(RelayError::TooLong(why)) => answer(w, Err(why)),
                Err(RelayError::NotStarted(e)) => {
                    answer(w, Err(format!("can't reach {node}: {e}")))
                }
            }
        }
        Request::Status => answer(w, views::status(sh, &c)),
        Request::Nodes => answer(w, Ok(views::nodes(sh, &c))),
        Request::Remove { node, .. } if !relayed && !c.removed() && !c.mesh().users_exist() => {
            // Signed by the oldest member up that was admitted before `node`, the removal is
            // final: `node` can't undo it by removing that member back. This machine signs it
            // when none is up, or none that is signs it (saying the removal holds only until
            // `node` contests it, and why they didn't).
            let mut refused: Vec<(String, String)> = Vec::new();
            for elder in c.mesh().admitted_before(node) {
                if elder == me {
                    break;
                }
                if !c.alive(&elder) {
                    continue;
                }
                match ask(sh, &c, &elder, req) {
                    Ok(Response::Error { message }) => refused.push((elder, message)),
                    Ok(resp) => return answer(w, Ok(resp)),
                    Err(_) => continue,
                }
            }
            let done = membership(sh, &c, &req.req);
            answer(
                w,
                match done {
                    Ok(Response::Text { .. }) if !refused.is_empty() => Ok(Response::Text {
                        text: not_signed_by(node, &refused),
                    }),
                    other => other,
                },
            )
        }
        Request::PairRequest
        | Request::PairFinish { .. }
        | Request::Admit { .. }
        | Request::Remove { .. }
        | Request::Membership
        | Request::UserInit { .. }
        | Request::UserRecord { .. } => answer(w, membership(sh, &c, &req.req)),
        Request::SelfUpdate => answer(w, self_update(sh, &c).map(|text| Response::Text { text })),
        _ => Ok(false),
    }
}

/// Why `req` from this machine's command line isn't relayed to `node`, which would refuse it: in
/// a cluster with users, it is unsigned, or signed by a key that is no user's. None when it goes
/// (`node` checks the signature).
fn unsigned(c: &Cluster, req: &ClientRequest, node: &str) -> Option<String> {
    let mesh = c.mesh();
    if !mesh.users_exist() {
        return None;
    }
    let what = describe(&req.req);
    match &req.auth {
        None => Some(format!(
            "this request ({what}) {NEEDS_USER_KEY}: {node} answers it, and in a cluster with \
             users a machine answers another machine's requests only when a user signed them"
        )),
        Some(a) if !mesh.recognized_user(&a.key) => Some(format!(
            "this request ({what}) is signed with the user key {key}, which is no user of this \
             cluster, and {node} answers only what a user signed; a user adds the key with \
             `potluck user add NAME {key}`",
            key = a.key
        )),
        Some(_) => None,
    }
}

/// Where this machine's log of `job` starts in its owner's ([`super::Outbox::log_at`]), when this
/// machine runs the job for its owner (a run the owner hasn't taken away); None when it doesn't.
fn runs(c: &Cluster, job: &str) -> Option<Option<u64>> {
    c.cs()
        .outbox
        .get(job)
        .filter(|o| !o.orphaned)
        .map(|o| o.log_at)
}

/// How long a request about a job waits for its owner when the owner can't be reached but the
/// mesh still counts it as alive: it is restarting, or it runs another version while one of the
/// two updates (members talk only on the same version). A long poll waits no longer than it
/// asked to. Below the CLI's read timeout.
const AWAY_WAIT: Duration = Duration::from_secs(30);
/// How often the owner is tried again meanwhile.
const AWAY_RETRY: Duration = Duration::from_millis(250);

/// Whether a request about a job whose owner couldn't be reached but still counts as alive
/// (heard from within the down time, or on another version while one of the two updates) waits
/// and tries again: its time isn't up (`waited` so far), and this daemon isn't handing over
/// (then long polls answer at once).
fn waits(sh: &Shared, req: &Request, waited: Duration) -> bool {
    let limit = match req {
        Request::Wait {
            timeout_ms: Some(ms),
            ..
        } => Duration::from_millis(*ms).min(AWAY_WAIT),
        Request::Logs { wait_ms, .. } if *wait_ms > 0 => {
            Duration::from_millis(*wait_ms).min(AWAY_WAIT)
        }
        _ => AWAY_WAIT,
    };
    waited + AWAY_RETRY <= limit && !sh.stopping() && !sh.up.quiescing()
}

/// `req` to relay after waiting `waited` for its owner: a long poll waits that much less.
fn for_now(req: &ClientRequest, waited: Duration) -> ClientRequest {
    let mut req = req.clone();
    let less = |ms: u64| ms.saturating_sub(waited.as_millis() as u64);
    match &mut req.req {
        Request::Wait {
            timeout_ms: Some(ms),
            ..
        } => *ms = less(*ms),
        Request::Logs { wait_ms, .. } if *wait_ms > 0 => *wait_ms = less(*wait_ms).max(1),
        _ => {}
    }
    req
}

/// The answer to a long poll about a job whose owner, still alive, couldn't be reached in its
/// time, as to one that heard no news: no new output for `logs` (the client asks again), and
/// for `wait` the job as its runner or owner last reported it, if the report of a member still
/// alive lists it (a member that is down keeps its last report here, and the job may have ended
/// since: its owner ends it, or queues it again, once the runner is lost), or else that the
/// owner is away (`why`): nothing reachable knows how the job is, and the client asks again.
/// A report lists a job only as its owner (queued) or its runner (starting, running) says
/// ([`views::received`]): no other member's word ends a `wait`. None for anything else.
fn no_news(c: &Cluster, req: &Request, why: &str) -> Option<Response> {
    match req {
        Request::Logs {
            offset, wait_ms, ..
        } if *wait_ms > 0 => Some(Response::Bytes {
            offset: *offset,
            len: 0,
            done: false,
        }),
        Request::Wait { job, .. } => {
            let alive: Vec<String> = c
                .mesh()
                .members()
                .into_iter()
                .filter(|m| m.alive)
                .map(|m| m.name)
                .collect();
            let cs = c.cs();
            let r = cs
                .reports
                .iter()
                .filter(|(node, _)| alive.contains(node))
                .flat_map(|(_, r)| r.report.running.iter().chain(&r.report.queue))
                .find(|r| &r.id == job);
            Some(match r {
                Some(r) => Response::Job {
                    job: Box::new(r.clone()),
                },
                // One that ended is in no report.
                None => Response::OwnerAway {
                    message: why.to_string(),
                },
            })
        }
        _ => None,
    }
}

/// `req` as it is relayed to another member: its context only as far as that member uses it, the
/// caller's branch for a hold (the workspace keeps it, for its reminders). Not the job and lease
/// token of a caller inside a job, nor the caller's repository: the answer's reminders are this
/// machine's.
fn as_relayed(req: &ClientRequest) -> ClientRequest {
    let branch = match req.req {
        Request::Hold { .. } => req.ctx.branch.clone(),
        _ => None,
    };
    ClientRequest {
        ctx: ClientContext {
            branch,
            ..ClientContext::default()
        },
        req: req.req.clone(),
        auth: req.auth.clone(),
    }
}

/// Relays a CLI request to `node` and copies its answer to `w`, with this machine's reminders.
fn relay<W: Write>(
    sh: &Arc<Shared>,
    c: &Cluster,
    node: &str,
    req: &ClientRequest,
    w: &mut W,
) -> Result<(), RelayError> {
    // A restart in place waits for relays that began before it drains; those that begin during
    // it wait for news only briefly ([`crate::daemon::handover::cap_long_poll`]).
    let draining = sh.up.draining();
    let _relaying = (!draining).then(|| sh.up.relaying());
    let mut req = req.clone();
    if draining {
        crate::daemon::handover::cap_long_poll(&mut req.req);
    }
    let req = &req;
    let header = serde_json::to_value(StreamHeader::Client {
        req: Box::new(as_relayed(req)),
    })
    .map_err(|e| RelayError::NotStarted(e.to_string()))?;
    let stream = c
        .mesh()
        .open_stream(node, header)
        .map_err(|e| RelayError::NotStarted(e.to_string()))?;
    let _ = stream.set_read_timeout(Some(RELAY_IDLE));
    // Made once the first frame is in: a hold or release answers with the workspace as it left
    // it, which this machine's view of `node` (its last report) doesn't have yet.
    let mut rem = None;
    let mut r = BufReader::new(stream);
    let mut any = false;
    loop {
        let frame = match read_msg_within::<_, ServerResponse>(&mut r, MAX_ANSWER) {
            Ok(Some(f)) => f,
            Ok(None) if any => break,
            Ok(None) => {
                return Err(RelayError::NotStarted(format!(
                    "{node} closed the connection without answering"
                )));
            }
            Err(e) if TooLong::is(&e) => {
                drop(r);
                let why = format!("it sent {e}, longer than any answer a member gives");
                return Err(RelayError::TooLong(dropped(sh, node, req, &why)));
            }
            Err(e) if !any => return Err(RelayError::NotStarted(e.to_string())),
            Err(e) => return Err(RelayError::Broken(e)),
        };
        any = true;
        if let Response::Workspace { workspace } = &frame.resp {
            views::workspace_changed(sh, c, node, workspace);
        }
        let rem = rem.get_or_insert_with(|| reminders(sh, &req.ctx));
        let len = match &frame.resp {
            Response::Bytes { len, .. } if *len > most_output(&req.req) => {
                drop(r);
                let why = format!(
                    "it sent {} of output, and an answer to it carries at most {}",
                    crate::cli::fmt_bytes(*len),
                    crate::cli::fmt_bytes(most_output(&req.req))
                );
                return Err(RelayError::TooLong(dropped(sh, node, req, &why)));
            }
            Response::Bytes { len, .. } | Response::CopyEntry { len, .. } => *len,
            _ => 0,
        };
        reply(w, rem, frame.resp).map_err(RelayError::Broken)?;
        if len > 0 {
            let n = std::io::copy(&mut (&mut r).take(len), w).map_err(RelayError::Broken)?;
            if n != len {
                return Err(RelayError::Broken(std::io::ErrorKind::UnexpectedEof.into()));
            }
        }
    }
    w.flush().map_err(RelayError::Broken)
}

/// `node`'s answer to `req` is dropped, its connection closed, for `why` (it was too long): what
/// the command line is told, also logged as a warning.
fn dropped(sh: &Shared, node: &str, req: &ClientRequest, why: &str) -> String {
    let what = format!(
        "{node}'s answer to this request ({}) was dropped: {why}",
        describe(&req.req)
    );
    sh.log.log(format!(
        "cluster: WARNING: {what}; the connection to {node} was closed"
    ));
    what
}

/// A removal asked of `elder` ([`intercept`]): its answer, one frame; an error when none came.
fn ask(
    sh: &Arc<Shared>,
    c: &Cluster,
    elder: &str,
    req: &ClientRequest,
) -> Result<Response, String> {
    // As [`relay`]: a restart in place waits for the answer.
    let _relaying = (!sh.up.draining()).then(|| sh.up.relaying());
    let header = serde_json::to_value(StreamHeader::Client {
        req: Box::new(as_relayed(req)),
    })
    .map_err(|e| e.to_string())?;
    let stream = c
        .mesh()
        .open_stream(elder, header)
        .map_err(|e| e.to_string())?;
    let _ = stream.set_read_timeout(Some(RELAY_IDLE));
    let answered = read_msg_within::<_, ServerResponse>(&mut BufReader::new(stream), MAX_ANSWER);
    match answered {
        Ok(Some(frame)) => Ok(frame.resp),
        Ok(None) => Err(format!("{elder} closed the connection without answering")),
        Err(e) if TooLong::is(&e) => {
            let why = format!("it sent {e}, longer than any answer a member gives");
            Err(dropped(sh, elder, req, &why))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// What a removal of `node` signed here leaves open when the members admitted before it that
/// are up didn't sign it (`refused`: each one and why): `node` can undo it.
fn not_signed_by(node: &str, refused: &[(String, String)]) -> String {
    let why: Vec<String> = refused
        .iter()
        .map(|(elder, message)| format!("{elder} did not sign it: {message}"))
        .collect();
    let elders: Vec<&str> = refused.iter().map(|(elder, _)| elder.as_str()).collect();
    format!(
        "{}; so {node} can undo this removal; run `potluck remove {node}` again when {} can sign it to make it final",
        why.join("; "),
        elders.join(" or ")
    )
}

/// A removal of `node` another member (`requester`) relayed here, as [`intercept`] does: signed
/// only when this machine was admitted before `node`, so that `node` can't undo it (never the
/// founder's, which no member was admitted before, nor this machine's own).
fn serve_removal<W: Write>(
    sh: &Arc<Shared>,
    req: &ClientRequest,
    node: &str,
    requester: &str,
    w: &mut W,
) -> std::io::Result<()> {
    let _answering = sh.up.serving();
    let result = match sh.cluster() {
        None => Err("this machine is in no cluster".to_string()),
        // A user signs every removal there: no member signs one for another.
        Some(c) if c.mesh().users_exist() => Err(format!(
            "this cluster has users, so {} signs no removal: a user's signed removal removes {node}",
            c.node
        )),
        Some(c) if c.mesh().admitted_before(node).is_empty() && c.mesh().member(node).is_none() => {
            Err(format!("{} does not know {node} (yet)", c.node))
        }
        Some(c) if !c.mesh().admitted_before(node).contains(&c.node) => Err(format!(
            "{} was not admitted before {node}, so it signs no removal of {node} that {requester} relays",
            c.node
        )),
        Some(c) => {
            let r = membership(sh, &c, &req.req);
            if r.is_ok() {
                sh.log
                    .log(format!("cluster: removal of {node} signed for {requester}"));
            }
            r
        }
    };
    let rem = reminders(sh, &req.ctx);
    reply(
        w,
        &rem,
        result.unwrap_or_else(|message| Response::Error { message }),
    )
}

/// A request another member (`from`) relayed here: answered on the stream as on the socket, when
/// [`may_answer`] lets it be. Only the requests [`intercept`] relays are answered (about a job or
/// a workspace, this machine's login environment, and a removal, which it relays to a member
/// admitted before the machine removed); anything else, such as an admission, is refused rather
/// than done here on another member's word.
pub(super) fn serve(
    sh: &Arc<Shared>,
    c: &Cluster,
    from: &Peer,
    req: &ClientRequest,
    stream: crate::mesh::Stream,
) {
    let mut s = stream;
    if let Request::Remove { node, .. } = &req.req {
        let answered = serve_removal(sh, req, node, &from.name, &mut s);
        if let Err(e) = answered {
            sh.log
                .log(format!("cluster: answering a relayed request: {e}"));
        }
        let _ = s.close_write();
        return;
    }
    let relayable = matches!(
        req.req,
        Request::Get { .. }
            | Request::Wait { .. }
            | Request::Logs { .. }
            | Request::Why { .. }
            | Request::Cancel { .. }
            | Request::Resize { .. }
            | Request::Copy { .. }
            | Request::Hold { .. }
            | Request::Release { .. }
            | Request::Env { .. }
    );
    let answered = if relayable {
        match may_answer(c, &from.name, req) {
            Ok(Answer::Here) => handle_relayed(sh, req, &mut s),
            Ok(Answer::ToOwner(owner)) => to_owner(sh, c, &from.name, &owner, req, &mut s),
            Err(message) => {
                sh.log.log(format!(
                    "cluster: refused a relayed request ({}) from {}: {message}",
                    describe(&req.req),
                    from.name
                ));
                reply(&mut s, &[], Response::Error { message })
            }
        }
    } else {
        let op = serde_json::to_value(&req.req)
            .ok()
            .and_then(|v| v.get("op").and_then(|o| o.as_str()).map(str::to_string))
            .unwrap_or_default();
        sh.log.log(format!(
            "cluster: refused a relayed {op} request: only requests about a job, a workspace, \
             a machine's environment or a removal are relayed"
        ));
        reply(
            &mut s,
            &[],
            Response::Error {
                message: format!(
                    "{op} requests are not relayed between members: run it on the machine itself"
                ),
            },
        )
    };
    if let Err(e) = answered {
        sh.log
            .log(format!("cluster: answering a relayed request: {e}"));
    }
    let _ = s.close_write();
}

/// Where a relayed request [`may_answer`] lets in is answered.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// Here, as on the socket.
    Here,
    /// By the job's owner: a cancel of a job this machine runs for it.
    ToOwner(String),
}

/// Whether this machine answers `req`, which member `from` relayed (a kind [`serve`] answers),
/// and where; the reason, for the command line, when it doesn't. In a cluster with users, a user
/// must have signed it ([`signed`]). Whoever signed it: about a job, only when this machine owns
/// it or runs it now, a cancel of one it runs for another owner going to that owner; about a
/// workspace, only one of this machine's.
fn may_answer(c: &Cluster, from: &str, req: &ClientRequest) -> Result<Answer, String> {
    if c.mesh().users_exist() {
        signed(c, from, req)?;
    }
    let me = c.node.as_str();
    match &req.req {
        Request::Get { job }
        | Request::Wait { job, .. }
        | Request::Logs { job, .. }
        | Request::Why { job }
        | Request::Cancel { job }
        | Request::Resize { job, .. } => {
            let owner = owner_of(job);
            if owner == me {
                return Ok(Answer::Here);
            }
            match (runs(c, job), &req.req) {
                (Some(_), Request::Cancel { .. }) => Ok(Answer::ToOwner(owner.to_string())),
                (Some(_), _) => Ok(Answer::Here),
                (None, _) => Err(format!(
                    "{me} neither owns nor runs job {job}, so it doesn't answer for it; {owner} does"
                )),
            }
        }
        Request::Copy { workspace, .. }
        | Request::Hold { workspace, .. }
        | Request::Release { workspace }
            if node_of_ws(workspace) != me =>
        {
            Err(format!(
                "workspace {workspace} is on {}, not {me}",
                node_of_ws(workspace)
            ))
        }
        _ => Ok(Answer::Here),
    }
}

/// Whether a user signed `req`, relayed by `from`: a signature by a recognized user's key that
/// verifies, made within [`RequestAuth::WINDOW_MS`] of this machine's clock, and on a request
/// this machine hasn't taken before ([`Seen`]). The reason when not.
fn signed(c: &Cluster, from: &str, req: &ClientRequest) -> Result<(), String> {
    let me = c.node.as_str();
    let what = describe(&req.req);
    let Some(auth) = &req.auth else {
        return Err(format!(
            "this request ({what}) {NEEDS_USER_KEY}: in a cluster with users, {me} answers \
             another machine's requests only when a user signed them, and {from} relayed it \
             unsigned"
        ));
    };
    if !c.mesh().recognized_user(&auth.key) {
        return Err(format!(
            "{me} refuses this request ({what}): the key that signed it, {}, is no user of this \
             cluster",
            auth.key
        ));
    }
    if !auth.verifies(&req.req) {
        return Err(format!(
            "{me} refuses this request ({what}): its signature is not {}'s on it",
            auth.key
        ));
    }
    let now = c.clock.wall_ms();
    c.cs()
        .requests_seen
        .take(auth, now)
        .map_err(|why| format!("{me} refuses this request ({what}): {why}"))
}

/// The signed requests this machine took lately ([`signed`]), by key and nonce, with when each
/// was signed. One is kept until it is too old to be taken anyway: [`RequestAuth::WINDOW_MS`]
/// past its time. In memory only: a daemon that starts again takes a request signed before once
/// more, within that window.
#[derive(Default)]
pub(crate) struct Seen(BTreeMap<(PublicKey, String), Millis>);

impl Seen {
    /// Takes a request `auth` signed, at wall time `now`: refused when it was signed too far
    /// from `now`, either way, or was taken before.
    fn take(&mut self, auth: &RequestAuth, now: Millis) -> Result<(), String> {
        let window = RequestAuth::WINDOW_MS;
        if auth.issued_at.abs_diff(now) > window {
            let (by, way) = match auth.issued_at > now {
                true => (auth.issued_at - now, "after"),
                false => (now - auth.issued_at, "before"),
            };
            return Err(format!(
                "it was signed {}s {way} now by this machine's clock, and a signed request is \
                 taken only within {} minutes of it (one signed long ago, or a clock that is off)",
                by / 1000,
                window / 60_000
            ));
        }
        self.0.retain(|_, at| at.saturating_add(window) >= now);
        match self.0.entry((auth.key, auth.nonce.clone())) {
            std::collections::btree_map::Entry::Occupied(_) => Err(
                "it was taken once already, and a signed request is taken once (this is a replay \
                 of it)"
                    .to_string(),
            ),
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(auth.issued_at);
                Ok(())
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// A cancel, relayed by `from`, of a job this machine runs for `owner`: sent on to `owner`, which
/// cancels it here as it cancels any of its jobs running elsewhere, its answer copied back.
fn to_owner<W: Write>(
    sh: &Arc<Shared>,
    c: &Cluster,
    from: &str,
    owner: &str,
    req: &ClientRequest,
    w: &mut W,
) -> std::io::Result<()> {
    let _answering = sh.up.serving();
    let what = describe(&req.req);
    match relay(sh, c, owner, req, w) {
        Ok(()) => {
            sh.log.log(format!(
                "cluster: a relayed request ({what}) from {from}, of a job {owner} owns, went to {owner}"
            ));
            Ok(())
        }
        Err(RelayError::Broken(e)) => Err(e),
        Err(RelayError::TooLong(message)) => {
            reply(w, &reminders(sh, &req.ctx), Response::Error { message })
        }
        Err(RelayError::NotStarted(e)) => {
            let rem = reminders(sh, &req.ctx);
            let message = format!(
                "{} runs this job for {owner}, which cancels it, and {owner} can't be reached: {e}",
                c.node
            );
            reply(w, &rem, Response::Error { message })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::user::UserKey;

    /// A relayed request carries of its context only the branch of a hold: never the caller's
    /// job and lease token, nor its repository. Its signature goes along.
    #[test]
    fn a_relayed_request_carries_only_the_context_its_member_uses() {
        let tmp = tempfile::tempdir().unwrap();
        let k = UserKey::load_or_create(&tmp.path().join("u.key"))
            .unwrap()
            .0;
        let ctx = ClientContext {
            repo_id: Some("app-0011aabb".into()),
            branch: Some("feature".into()),
            job: Some("a-7".into()),
            lease_token: Some("secret".into()),
        };
        let hold = Request::Hold {
            workspace: "b-w1".into(),
            duration_ms: 60_000,
        };
        for req in [
            Request::Get { job: "b-1".into() },
            Request::Cancel { job: "b-1".into() },
            Request::Release {
                workspace: "b-w1".into(),
            },
            Request::Env {
                node: Some("b".into()),
            },
            hold.clone(),
        ] {
            let auth = RequestAuth::sign(&k, &req);
            let sent = as_relayed(&ClientRequest {
                ctx: ctx.clone(),
                req: req.clone(),
                auth: Some(auth.clone()),
            });
            let is_hold = matches!(req, Request::Hold { .. });
            assert_eq!(
                (sent.ctx.repo_id, sent.ctx.job, sent.ctx.lease_token),
                (None, None, None),
                "{req:?}"
            );
            let branch = is_hold.then(|| "feature".to_string());
            assert_eq!(sent.ctx.branch, branch, "{req:?}");
            assert_eq!(sent.auth.as_ref(), Some(&auth));
            assert!(sent.auth.unwrap().verifies(&sent.req));
            let line = serde_json::to_string(&StreamHeader::Client {
                req: Box::new(as_relayed(&ClientRequest {
                    ctx: ctx.clone(),
                    req,
                    auth: None,
                })),
            })
            .unwrap();
            assert!(!line.contains("secret") && !line.contains("a-7"), "{line}");
        }
    }

    #[test]
    fn a_signed_request_is_taken_once_and_only_near_its_time() {
        let tmp = tempfile::tempdir().unwrap();
        let k = UserKey::load_or_create(&tmp.path().join("u.key"))
            .unwrap()
            .0;
        let other = UserKey::load_or_create(&tmp.path().join("v.key"))
            .unwrap()
            .0;
        let req = Request::Get { job: "a-1".into() };
        let now: Millis = 1_000_000_000;
        let w = RequestAuth::WINDOW_MS;
        let at = |t: Millis, nonce: &str| RequestAuth::sign_at(&k, &req, nonce.into(), t);
        let mut seen = Seen::default();
        seen.take(&at(now, "n1"), now).unwrap();
        // Again: a replay.
        let e = seen.take(&at(now, "n1"), now + 1_000).unwrap_err();
        assert!(e.contains("replay"), "{e}");
        // The same nonce from another key is another request.
        seen.take(&RequestAuth::sign_at(&other, &req, "n1".into(), now), now)
            .unwrap();
        // Up to 5 minutes either way.
        seen.take(&at(now - w, "n2"), now).unwrap();
        seen.take(&at(now + w, "n3"), now).unwrap();
        let e = seen.take(&at(now - w - 1_000, "n4"), now).unwrap_err();
        assert!(e.contains("signed 301s before now"), "{e}");
        let e = seen.take(&at(now + w + 61_000, "n5"), now).unwrap_err();
        assert!(e.contains("signed 361s after now"), "{e}");
        assert_eq!(seen.len(), 4);
        // Kept until too old to be taken anyway: then forgotten.
        let e = seen.take(&at(now + w, "n3"), now + 2 * w).unwrap_err();
        assert!(e.contains("replay"), "{e}");
        seen.take(&at(now + 2 * w, "n6"), now + 2 * w).unwrap();
        assert_eq!(
            seen.len(),
            2,
            "only n3 (signed at now + 5m) and n6 are left"
        );
        // n1, forgotten, is too old to count.
        assert!(seen.take(&at(now, "n1"), now + 2 * w).is_err());
    }
}
