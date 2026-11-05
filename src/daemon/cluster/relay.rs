//! CLI requests in a cluster. The CLI only talks to its own daemon; a request about a job owned by
//! another machine, a workspace on another machine, or another machine's login environment
//! (`potluck env NODE`), is relayed there over a `client` stream and answered from there, frame by
//! frame (with this machine's reminders). Status, nodes and why cover every member; pairing,
//! admission, removal and self-update go to the mesh. A removal goes to the oldest member up that
//! was admitted before the machine removed, which makes it final.

use super::peer::{StreamHeader, node_of_ws, owner_of};
use super::{Cluster, membership, owner, self_update, views};
use crate::daemon::Shared;
use crate::daemon::files::reply;
use crate::daemon::server::{self, handle_relayed, reminders};
use crate::proto::{ClientRequest, Request, Response, ServerResponse, read_msg};
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A relayed answer longer than this between frames is given up on (long polls ask for less).
const RELAY_IDLE: Duration = Duration::from_secs(90);

enum RelayError {
    /// Nothing was answered: the caller may answer otherwise.
    NotStarted(String),
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
        | Request::Cancel { job } => {
            let owner = owner_of(job);
            if owner != me {
                if relayed || !c.is_member(owner) || c.removed() {
                    return Ok(false);
                }
                let began = Instant::now();
                loop {
                    let now = for_now(req, began.elapsed());
                    let e = match relay(sh, &c, owner, &now, w) {
                        Ok(()) => return Ok(true),
                        Err(RelayError::Broken(e)) => return Err(e),
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
            match relay(sh, &c, node, req, w) {
                Ok(()) => Ok(true),
                Err(RelayError::Broken(e)) => Err(e),
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
            match relay(sh, &c, node, req, w) {
                Ok(()) => Ok(true),
                Err(RelayError::Broken(e)) => Err(e),
                Err(RelayError::NotStarted(e)) => {
                    answer(w, Err(format!("can't reach {node}: {e}")))
                }
            }
        }
        Request::Status => answer(w, views::status(sh, &c)),
        Request::Nodes => answer(w, Ok(views::nodes(sh, &c))),
        Request::Remove { node } if !relayed && !c.removed() => {
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
        | Request::Remove { .. } => answer(w, membership(sh, &c, &req.req)),
        Request::SelfUpdate => answer(w, self_update(sh, &c).map(|text| Response::Text { text })),
        _ => Ok(false),
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
/// None for anything else.
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
        req: Box::new(req.clone()),
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
        let frame = match read_msg::<_, ServerResponse>(&mut r) {
            Ok(Some(f)) => f,
            Ok(None) if any => break,
            Ok(None) => {
                return Err(RelayError::NotStarted(format!(
                    "{node} closed the connection without answering"
                )));
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
        req: Box::new(req.clone()),
    })
    .map_err(|e| e.to_string())?;
    let stream = c
        .mesh()
        .open_stream(elder, header)
        .map_err(|e| e.to_string())?;
    let _ = stream.set_read_timeout(Some(RELAY_IDLE));
    match read_msg::<_, ServerResponse>(&mut BufReader::new(stream)) {
        Ok(Some(frame)) => Ok(frame.resp),
        Ok(None) => Err(format!("{elder} closed the connection without answering")),
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

/// A request another member relayed here: answered on the stream as on the socket. Only the
/// requests [`intercept`] relays are answered (about a job or a workspace, this machine's login
/// environment, and a removal, which it relays to a member admitted before the machine removed);
/// anything else, such as an admission, is refused rather than done here on another member's
/// word.
pub(super) fn serve(sh: &Arc<Shared>, req: &ClientRequest, stream: crate::mesh::Stream) {
    let mut s = stream;
    if let Request::Remove { node } = &req.req {
        let requester = s.peer().name.clone();
        let answered = serve_removal(sh, req, node, &requester, &mut s);
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
            | Request::Copy { .. }
            | Request::Hold { .. }
            | Request::Release { .. }
            | Request::Env { .. }
    );
    let answered = if relayable {
        handle_relayed(sh, req, &mut s)
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
