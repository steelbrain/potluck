//! The protocol between the CLI and its local daemon, over a Unix socket.
//!
//! Each message is one line of JSON. A [`Response::Bytes`] line is followed by exactly `len` raw
//! bytes. Every response carries the reminders for the caller's branch (its held workspaces), so
//! an agent always sees what it holds and how long is left.
//!
//! The CLI only ever talks to its own machine's daemon; from phase 2 the daemon forwards requests
//! about jobs and workspaces on other machines over the mesh.

use crate::mesh::identity::Signer;
use crate::mesh::trust::{Admission, Record, Removal, UserAdmission};
use crate::mesh::{MembershipView, PublicKey, Signature};
use crate::types::{
    Facts, Grant, JobId, JobRecord, JobSpec, Millis, NodeName, Reminder, WorkspaceId,
    WorkspaceRecord,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Who is asking: used for reminders and to find nested calls.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClientContext {
    /// The repository id and branch of the caller's working directory, if it is in one.
    pub repo_id: Option<String>,
    pub branch: Option<String>,
    /// POTLUCK_JOB and POTLUCK_LEASE when the caller runs inside a potluck job.
    pub job: Option<JobId>,
    pub lease_token: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientRequest {
    pub ctx: ClientContext,
    pub req: Request,
    /// A user's signature on `req`: the command line signs every request when it has a user
    /// key. Its own daemon answers without one, and carries it along when it relays the request
    /// to another member, which in a cluster with users answers only what a user signed. None
    /// from a command line without a user key, or from before signatures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<RequestAuth>,
}

/// What a refusal says, in a cluster with users, of an unsigned request another member would
/// answer. The command line adds where it looked for its user key.
pub const NEEDS_USER_KEY: &str = "needs this cluster's user key";

/// A user's signature on a request ([`ClientRequest::auth`]): by `key`, over
/// `("potluck request v1", request, nonce, issued_at)` as JSON, where the request is
/// [`Request`] with its long-poll waits left out (`wait`'s `timeout_ms` as null, `logs`'
/// `wait_ms` as 0): a daemon relaying a long poll shortens them after waiting for an owner that
/// was away, or while it restarts, and how long a poll waits carries no authority. The request's
/// context (the caller's repository and branch) is not signed; a daemon relaying the request
/// passes on only a hold's branch of it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestAuth {
    pub key: PublicKey,
    /// Random, new for each request: a member answers a signed request once.
    pub nonce: String,
    /// When it was signed, Unix ms by the signing machine's clock: a member answers it only
    /// within [`RequestAuth::WINDOW_MS`] of its own.
    pub issued_at: Millis,
    pub sig: Signature,
}

impl RequestAuth {
    /// How far a request's `issued_at` may be from the answering member's clock, either way.
    pub const WINDOW_MS: Millis = 5 * 60_000;

    /// `req` signed by `key` now, with a new nonce.
    pub fn sign(key: &dyn Signer, req: &Request) -> RequestAuth {
        RequestAuth::sign_at(
            key,
            req,
            crate::mesh::trust::nonce(),
            crate::types::now_ms(),
        )
    }

    /// `req` signed by `key` with this nonce and time.
    pub fn sign_at(key: &dyn Signer, req: &Request, nonce: String, issued_at: Millis) -> Self {
        let sig = key.sign(&signed_bytes(req, &nonce, issued_at));
        RequestAuth {
            key: key.public(),
            nonce,
            issued_at,
            sig,
        }
    }

    /// Whether this is `key`'s signature on `req`, with this nonce and time.
    pub fn verifies(&self, req: &Request) -> bool {
        self.key
            .verify(&signed_bytes(req, &self.nonce, self.issued_at), &self.sig)
    }
}

/// What a [`RequestAuth`] signs.
fn signed_bytes(req: &Request, nonce: &str, issued_at: Millis) -> Vec<u8> {
    let mut req = req.clone();
    match &mut req {
        Request::Wait { timeout_ms, .. } => *timeout_ms = None,
        Request::Logs { wait_ms, .. } => *wait_ms = 0,
        _ => {}
    }
    serde_json::to_vec(&("potluck request v1", &req, nonce, issued_at)).unwrap_or_default()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Ping,
    /// Queue a job. With a key that already exists, returns that job instead.
    Dispatch {
        spec: Box<JobSpec>,
    },
    Get {
        job: JobId,
    },
    /// Returns when the job is done, or at the timeout with the job as it stands.
    Wait {
        job: JobId,
        timeout_ms: Option<u64>,
    },
    /// Up to `max` bytes of output from `offset`. With `wait_ms`, waits that long for new output
    /// when there is none yet (long poll, for `wait` and `logs --follow`).
    Logs {
        job: JobId,
        offset: u64,
        max: u64,
        wait_ms: u64,
    },
    Cancel {
        job: JobId,
    },
    /// Set a workspace's hold to end `duration_ms` from now (at most 24 h).
    Hold {
        workspace: WorkspaceId,
        duration_ms: u64,
    },
    /// End a workspace's hold now and remove it (unless an invocation is queued or running in it).
    Release {
        workspace: WorkspaceId,
    },
    /// Copy a file or directory out of a workspace. Answered by a stream of
    /// [`Response::CopyEntry`] frames, each followed by its bytes, then [`Response::CopyDone`].
    Copy {
        workspace: WorkspaceId,
        path: String,
    },
    Status,
    Why {
        job: JobId,
    },
    Nodes,
    /// The login environment `node`'s daemon holds (None: this machine's), which every job there
    /// starts from. Asked of `node` over the mesh when it is another member.
    Env {
        node: Option<NodeName>,
    },
    /// Whether the lease a job's environment names is live, and what it holds.
    LeaseCheck {
        job: JobId,
        token: String,
    },
    /// Pairing: this machine's request, to paste into `admit` on a member.
    PairRequest,
    /// Pairing: store the reply `admit` printed and join.
    PairFinish {
        reply: String,
    },
    /// Admit a machine from its pairing request; answers with the reply to paste back. In a
    /// cluster with users, `admission` is a user's signed admission of that machine (the CLI
    /// signs it); without users, None (this machine signs one).
    Admit {
        request: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        admission: Option<Box<Admission>>,
    },
    /// Remove a machine. In a cluster with users, `removal` is a user's signed removal of it;
    /// without users, None (a member signs one).
    Remove {
        node: NodeName,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        removal: Option<Box<Removal>>,
    },
    /// What the CLI signs membership records from: this machine, the members, the users.
    Membership,
    /// The first user: its admission of its own key, signed by it (this machine co-signs it),
    /// and its admission of every member, taken together.
    UserInit {
        genesis: Box<UserAdmission>,
        admissions: Vec<Admission>,
    },
    /// A user's admission or removal (`user add`, `user remove`), signed by a user.
    UserRecord {
        record: Box<Record>,
    },
    /// This machine's own jobs that haven't ended and that no user signed (dispatched before the
    /// cluster had users), for `potluck user init` to sign: answered with [`Response::Jobs`].
    /// Asked on the machine itself, never relayed.
    UnsignedJobs,
    /// A user's signatures of this machine's unsigned jobs ([`Request::UnsignedJobs`]), each over
    /// the job's spec as it stands, for this machine, with a nonce of its own. The daemon keeps
    /// each one that a user it recognizes made and that covers the job's spec, so other members
    /// may run the job: answered with [`Response::JobsSigned`]. Asked on the machine itself,
    /// never relayed.
    SignJobs {
        jobs: Vec<JobSignature>,
    },
    /// Update this machine to the latest release (the other members follow when they meet it).
    SelfUpdate,
    /// Start the potluck binary at the daemon's path in the daemon's own process, handing over
    /// its sockets, connections and state (an upgrade in place). Answered once the new binary
    /// serves, with what it did, or at once with why it can't.
    Restart {
        /// The potluck binary that asks (`potluck restart`, `self-update --here`). When it is
        /// another file than the one at the daemon's path, and that one is still the binary the
        /// daemon runs, a restart would run it again as it is: refused, naming both. None (a
        /// client from before this field): not checked. Daemons from before it ignore it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binary: Option<std::path::PathBuf>,
    },
}

/// A user's signature of one job ([`Request::SignJobs`]).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobSignature {
    pub job: JobId,
    pub auth: crate::jobauth::JobAuth,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerResponse {
    pub reminders: Vec<Reminder>,
    pub resp: Response,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Response {
    Pong {
        version: String,
        node: NodeName,
    },
    Dispatched {
        job: JobId,
        existing: bool,
    },
    Job {
        job: Box<JobRecord>,
    },
    /// Followed by `len` raw bytes.
    Bytes {
        offset: u64,
        len: u64,
        /// No more output will come (the job is done and all of it was sent).
        done: bool,
    },
    Workspace {
        workspace: Box<WorkspaceRecord>,
    },
    /// One file of a copy, followed by `len` bytes. Directories have `len` 0 and `dir` true.
    CopyEntry {
        /// Path relative to the copied path.
        path: String,
        mode: u32,
        dir: bool,
        len: u64,
    },
    CopyDone,
    Status {
        status: Box<StatusView>,
    },
    Why {
        why: WhyView,
    },
    Nodes {
        nodes: Vec<NodeView>,
    },
    /// A machine's login environment ([`Request::Env`]).
    Env {
        node: NodeName,
        env: BTreeMap<String, String>,
    },
    Lease {
        live: bool,
        grant: Option<Grant>,
        /// How long the live lease has left before its run limit, in ms, by the daemon's own
        /// clock: a dispatch run in place must end within it.
        #[serde(default)]
        remaining_ms: Option<u64>,
    },
    Ok,
    /// What [`Request::Membership`] asks for.
    Membership {
        membership: Box<MembershipView>,
    },
    /// Jobs, as [`Request::UnsignedJobs`] asks.
    Jobs {
        jobs: Vec<JobRecord>,
    },
    /// What [`Request::SignJobs`] did: the jobs whose signatures it kept, and why it kept none
    /// of the others.
    JobsSigned {
        signed: Vec<JobId>,
        refused: BTreeMap<JobId, String>,
    },
    /// Text for the user, e.g. a pairing request or reply to paste.
    Text {
        text: String,
    },
    Error {
        message: String,
    },
    /// The answer to a `wait` whose job's owner can't be reached now but is still a member up
    /// (restarting, or on another version while one of the two updates), when nothing this
    /// machine can reach knows how the job is: wait more and ask again. `message` says why, for a
    /// caller whose time is up.
    OwnerAway {
        message: String,
    },
}

/// One machine as `nodes` and `status` show it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeView {
    pub facts: Facts,
    pub address: Option<String>,
    /// Its pool, from the admission that made it a member ([`crate::mesh::Mesh::pool_of`]):
    /// `default` in a cluster without users, and on a machine in no cluster.
    #[serde(default = "default_pool")]
    pub pool: String,
    pub alive: bool,
    /// Heard from lately, but on another potluck version (`facts.version`): one of the two is
    /// updating. Not `alive`: members talk only on the same version.
    #[serde(default)]
    pub updating: bool,
    pub last_seen: Option<Millis>,
    pub ram_capacity_mib: u64,
    pub ram_reserved_mib: u64,
    /// GPU ids leased now.
    pub gpus_busy: Vec<String>,
    pub exclusive_job: Option<JobId>,
}

fn default_pool() -> String {
    crate::mesh::trust::DEFAULT_POOL.to_string()
}

/// `potluck status`: machines, running jobs, the queue, workspaces.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusView {
    pub nodes: Vec<NodeView>,
    pub running: Vec<JobRecord>,
    /// In the order the scheduler would start them.
    pub queued: Vec<JobRecord>,
    pub recent: Vec<JobRecord>,
    pub workspaces: Vec<WorkspaceRecord>,
    /// Things about the cluster to say first, in plain words: "this machine was removed from the
    /// cluster by studio; run `potluck pair` to join again".
    #[serde(default)]
    pub notes: Vec<String>,
}

/// `potluck why JOB`: the wait-for chain in plain words.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WhyView {
    pub job: JobId,
    pub state: String,
    /// One sentence a line, e.g. "waits on gpu 5080 on msi, held by job studio-41 for 14m of its
    /// 15m run limit, under 1% CPU for 9m".
    pub lines: Vec<String>,
    pub blockers: Vec<Blocker>,
}

/// One thing a queued job waits on.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Blocker {
    /// `gpu:ID`, `ram`, `exclusive`, `order`, `facts`, `workspace`, `sync`, `locality`, `catch_up`.
    pub resource: String,
    pub node: Option<NodeName>,
    pub holder: Option<JobId>,
    pub held_for_ms: Option<u64>,
    pub limit_ms: Option<u64>,
    /// How long the holder has used under 1% of a CPU core.
    pub holder_idle_ms: Option<u64>,
    /// When the scheduler expects it free (the holders' run-limit deadlines).
    pub free_by: Option<Millis>,
}

/// Writes one message as a JSON line.
pub fn write_msg<W: std::io::Write, T: Serialize>(w: &mut W, msg: &T) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(msg)?;
    line.push(b'\n');
    w.write_all(&line)?;
    w.flush()
}

/// Reads one JSON-line message; `None` at end of stream.
pub fn read_msg<R: std::io::BufRead, T: serde::de::DeserializeOwned>(
    r: &mut R,
) -> std::io::Result<Option<T>> {
    let mut line = String::new();
    if r.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    serde_json::from_str(&line)
        .map(Some)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::user::UserKey;

    fn key(dir: &std::path::Path, name: &str) -> UserKey {
        UserKey::load_or_create(&dir.join(name)).unwrap().0
    }

    #[test]
    fn a_signed_request_verifies_and_a_changed_one_does_not() {
        let tmp = tempfile::tempdir().unwrap();
        let (k, other) = (key(tmp.path(), "a.key"), key(tmp.path(), "b.key"));
        let get = |job: &str| Request::Get { job: job.into() };
        let auth = RequestAuth::sign(&k, &get("a-1"));
        assert_eq!(auth.key, k.public());
        assert!(auth.verifies(&get("a-1")));
        assert!(auth.issued_at.abs_diff(crate::types::now_ms()) < 60_000);
        // Another request, another kind of request with the same field, another nonce, time or
        // key: no.
        assert!(!auth.verifies(&get("a-2")));
        assert!(!auth.verifies(&Request::Cancel { job: "a-1".into() }));
        let changed = |f: &dyn Fn(&mut RequestAuth)| {
            let mut a = auth.clone();
            f(&mut a);
            a.verifies(&get("a-1"))
        };
        assert!(!changed(&|a| a.nonce.push('x')));
        assert!(!changed(&|a| a.issued_at += 1));
        assert!(!changed(&|a| a.key = other.public()));
        assert!(!changed(&|a| a.sig.0[0] ^= 1));
        // Each signature has a nonce of its own.
        assert_ne!(RequestAuth::sign(&k, &get("a-1")).nonce, auth.nonce);
        // As JSON, and in a request, it is the same.
        let req = ClientRequest {
            ctx: ClientContext::default(),
            req: get("a-1"),
            auth: Some(auth.clone()),
        };
        let line = serde_json::to_string(&req).unwrap();
        let back: ClientRequest = serde_json::from_str(&line).unwrap();
        assert_eq!(back.auth.as_ref(), Some(&auth));
        assert!(back.auth.unwrap().verifies(&back.req));
    }

    #[test]
    fn a_long_polls_wait_is_not_signed() {
        let tmp = tempfile::tempdir().unwrap();
        let k = key(tmp.path(), "a.key");
        let wait = |t: Option<u64>| Request::Wait {
            job: "a-1".into(),
            timeout_ms: t,
        };
        let auth = RequestAuth::sign_at(&k, &wait(Some(20_000)), "n".into(), 5);
        assert!(auth.verifies(&wait(Some(1_200))));
        assert!(auth.verifies(&wait(None)));
        let logs = |offset: u64, wait_ms: u64| Request::Logs {
            job: "a-1".into(),
            offset,
            max: 1 << 20,
            wait_ms,
        };
        let auth = RequestAuth::sign_at(&k, &logs(10, 20_000), "n".into(), 5);
        assert!(auth.verifies(&logs(10, 1)));
        // The rest of it is.
        assert!(!auth.verifies(&logs(0, 20_000)));
    }

    #[test]
    fn a_request_without_a_signature_parses_and_is_written_as_before() {
        let old = r#"{"ctx":{"repo_id":null,"branch":null,"job":null,"lease_token":null},"req":{"op":"get","job":"a-1"}}"#;
        let req: ClientRequest = serde_json::from_str(old).unwrap();
        assert!(req.auth.is_none());
        assert_eq!(serde_json::to_string(&req).unwrap(), old);
    }
}
