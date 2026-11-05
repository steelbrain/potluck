//! The protocol between the CLI and its local daemon, over a Unix socket.
//!
//! Each message is one line of JSON. A [`Response::Bytes`] line is followed by exactly `len` raw
//! bytes. Every response carries the reminders for the caller's branch (its held workspaces), so
//! an agent always sees what it holds and how long is left.
//!
//! The CLI only ever talks to its own machine's daemon; from phase 2 the daemon forwards requests
//! about jobs and workspaces on other machines over the mesh.

use crate::types::{
    Facts, Grant, JobId, JobRecord, JobSpec, Millis, NodeName, Reminder, WorkspaceId,
    WorkspaceRecord,
};
use serde::{Deserialize, Serialize};

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
    /// Admit a machine from its pairing request; answers with the reply to paste back.
    Admit {
        request: String,
    },
    Remove {
        node: NodeName,
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
    Lease {
        live: bool,
        grant: Option<Grant>,
        /// How long the live lease has left before its run limit, in ms, by the daemon's own
        /// clock: a dispatch run in place must end within it.
        #[serde(default)]
        remaining_ms: Option<u64>,
    },
    Ok,
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
