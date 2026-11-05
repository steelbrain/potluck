//! The daemon-to-daemon protocol, over the mesh: control messages ([`PeerRequest`] answered by
//! [`PeerResponse`], JSON through [`Mesh::call`]) and streams ([`StreamHeader`], then bytes,
//! through [`Mesh::open_stream`]).
//!
//! Roles: the **owner** of a job is the machine it was dispatched on (its id is
//! `<owner>-<seq>`); the **runner** is the machine that claimed it. A machine is often both.
//!
//! Control messages:
//! - `report`: a member's [`MachineReport`]: its facts, load, own queue, workspaces and the jobs it
//!   can't take yet for want of commits. Pulled about once a second from every live member, and
//!   pushed (`push`) by a member whose queue or load changed. A pull names the digest of the
//!   report the asker holds (`have`); one that says the same is answered `unchanged`, with the
//!   numbers of the report built now, not resent (a member's workspaces alone can run to
//!   hundreds of kilobytes).
//! - `claim` (runner to owner): the runner can start a queued job now. The owner grants a job once:
//!   while it is still queued it becomes Starting on that runner and the answer is `granted` with
//!   the job's record; otherwise `refused`. The lease's token identifies this run of the job in
//!   every later message about it.
//! - `passed` (runner to owner): queued jobs the runner's plan passed over (aging). When that plan
//!   claims a job of the same owner, they ride on the `claim` instead.
//! - `update` (runner to owner): the runner's record of a job: state changes at once, usage now
//!   and then, the end last (after the log and kept files arrived). `stale: true` means the job
//!   is no longer this runner's (requeued elsewhere, cancelled, ended): the runner stops it.
//!   While the kept files can't be delivered, the end comes with `keep_error` (why): the owner
//!   keeps it aside and waits for them, and ends the job without them, with a note, once they
//!   have failed for the give-up time (10 minutes) or the runner leaves the cluster. The runner
//!   gives up after the same time, keeps the files and sends the end with the note.
//! - `give_back` (runner to owner): a claimed job the runner won't start after all (its claim's
//!   answer was lost, or its daemon restarted while seeding): the owner queues it again.
//! - `reconcile` (runner to owner, when the owner comes back and now and then): the owner's jobs
//!   the runner holds. The owner answers with those that are no longer the runner's (`drop`), and
//!   queues again its jobs it believed were on that runner but the runner doesn't hold.
//! - `cancel` (owner to runner): stop a job running there.
//! - `workspace` (any member to a workspace's machine): its record, for `dispatch --workspace`.
//!
//! Streams:
//! - `log` (runner to owner): the owner answers `{"offset":N}` (how much of this run's output it
//!   has), then the runner sends [`LogFrame`]s: `data` with the output from there as it grows,
//!   `ping` while there is none, and `end` with the log's length when the job is over, which the
//!   owner answers with `{"len":N}` (how much it has).
//! - `keep` (runner to owner): the owner answers `{"ok":true}`, the runner sends the job's kept
//!   files as copy frames ([`crate::proto::Response::CopyEntry`] / `CopyDone`), the owner stores
//!   them under the job's `keep/` and answers `{"files":N}`.
//! - `crash` (runner to owner, after the final `update`): the job's crash reports (macOS crash
//!   reports, Linux `coredumpctl info`), sent once the runner has collected those written late
//!   (a few seconds after a crash), the same way as `keep`; the owner stores them under the
//!   job's `crash/`.
//! - `client` (any member to a job's owner, a workspace's machine, or the machine `potluck env`
//!   names): a CLI request made on another machine, answered exactly as on the socket (frames, raw
//!   bytes after `Bytes` and `CopyEntry`).
//! - `upload-pack` (a runner's `potluck _tunnel` to an owner): `git upload-pack` of the owner's own
//!   mirror of a repository a dispatch there named (it holds every commit a job there needs).

use crate::proto::ClientRequest;
use crate::types::{
    Facts, JobId, JobRecord, Lease, Millis, NodeName, WorkspaceId, WorkspaceRecord,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum PeerRequest {
    Report {
        /// The digest ([`MachineReport::digest`]) of the report the asker holds from this member.
        #[serde(default)]
        have: Option<String>,
    },
    Push {
        report: Box<MachineReport>,
    },
    Claim {
        job: JobId,
        lease: Box<Lease>,
        workspace: WorkspaceId,
        /// The owner's queued jobs the plan that started this one passed over: counted before
        /// the grant, so they are counted while still queued.
        #[serde(default)]
        passed: Vec<JobId>,
    },
    Passed {
        jobs: Vec<JobId>,
    },
    Update {
        job: Box<JobRecord>,
        /// The job ended, but its kept files haven't reached the owner: why, as the runner last
        /// tried.
        #[serde(default)]
        keep_error: Option<String>,
    },
    GiveBack {
        job: JobId,
        token: String,
        message: String,
    },
    Reconcile {
        /// (job, lease token) of every job of the owner's this runner holds.
        jobs: Vec<(JobId, String)>,
    },
    Cancel {
        job: JobId,
        token: String,
    },
    Workspace {
        workspace: WorkspaceId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum PeerResponse {
    Report {
        report: Box<MachineReport>,
    },
    /// The report the asker holds says what one built now would: that one's numbers.
    Unchanged {
        started_at: Millis,
        seq: u64,
    },
    Ok,
    Granted {
        job: Box<JobRecord>,
        /// The numbers (started_at, seq) of the last report the owner made before it granted
        /// the job (None from an owner of an older version).
        #[serde(default)]
        reported: Option<(Millis, u64)>,
    },
    Refused {
        reason: String,
    },
    Update {
        stale: bool,
    },
    Reconciled {
        drop: Vec<JobId>,
    },
    Workspace {
        workspace: Option<Box<WorkspaceRecord>>,
    },
}

/// What a member tells the others about itself.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct MachineReport {
    pub node: NodeName,
    pub facts: Facts,
    pub ram_capacity_mib: u64,
    /// Jobs holding a lease here (any owner's), as this machine records them.
    pub running: Vec<JobRecord>,
    /// This machine's own queued jobs.
    pub queue: Vec<JobRecord>,
    /// Workspaces here that exist (not removed).
    pub workspaces: Vec<WorkspaceRecord>,
    /// The owner of the job that last used each workspace: it reminds its branch.
    pub ws_owners: BTreeMap<WorkspaceId, NodeName>,
    /// Queued jobs this machine could run but can't take yet, with why (its mirrors lack their
    /// commits).
    pub unsynced: BTreeMap<JobId, String>,
    /// Queued jobs this machine could start now but leaves a few seconds to a member with a
    /// workspace of their repository (locality), with why.
    #[serde(default)]
    pub locality: BTreeMap<JobId, String>,
    /// Owners whose jobs this machine doesn't take yet: back from being away, it compares
    /// membership records with the other members first.
    #[serde(default)]
    pub catching_up: BTreeSet<NodeName>,
    /// This machine was removed from the cluster.
    pub removed: bool,
    /// When the sending daemon's cluster started, and the number of this report among those it
    /// built since (0: unnumbered). A report that arrives after a later one is dropped.
    #[serde(default)]
    pub started_at: Millis,
    #[serde(default)]
    pub seq: u64,
}

impl MachineReport {
    /// What the report says, apart from its numbers: equal for two reports that say the same.
    pub fn digest(&mut self) -> String {
        let numbers = (self.started_at, self.seq);
        (self.started_at, self.seq) = (0, 0);
        let json = serde_json::to_vec(&*self).unwrap_or_default();
        (self.started_at, self.seq) = numbers;
        let d = ring::digest::digest(&ring::digest::SHA256, &json);
        d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// The first JSON value on a stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub(crate) enum StreamHeader {
    Log { job: JobId, token: String },
    Keep { job: JobId, token: String },
    Crash { job: JobId, token: String },
    Client { req: Box<ClientRequest> },
    UploadPack { repo_id: String },
}

/// A frame of a `log` stream, runner to owner: a JSON line, followed by `len` bytes for `data`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub(crate) enum LogFrame {
    /// Output from offset `at` of this run's log.
    Data { at: u64, len: u64 },
    /// Nothing new; the stream is alive.
    Ping,
    /// The job is over and its log is `len` bytes.
    End { len: u64 },
}

/// The machine that owns a job: its id is `<owner>-<seq>` (node names may contain `-`).
pub(crate) fn owner_of(job: &str) -> &str {
    job.rsplit_once('-').map_or(job, |(n, _)| n)
}

/// The machine a workspace is on: its id is `<node>-w<seq>`.
pub(crate) fn node_of_ws(ws: &str) -> &str {
    ws.rsplit_once("-w").map_or(ws, |(n, _)| n)
}

/// The current time on this machine, for the cluster's own bookkeeping.
pub(crate) fn now() -> Millis {
    crate::types::now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_name_their_machines() {
        assert_eq!(owner_of("studio-42"), "studio");
        assert_eq!(owner_of("msi-laptop-7"), "msi-laptop");
        assert_eq!(node_of_ws("msi-laptop-w3"), "msi-laptop");
        assert_eq!(node_of_ws("a-w1"), "a");
        assert_eq!(node_of_ws("my-web-w12"), "my-web");
    }

    #[test]
    fn messages_are_tagged_json() {
        let v = serde_json::to_value(PeerRequest::Passed {
            jobs: vec!["a-1".into()],
        })
        .unwrap();
        assert_eq!(v["op"], "passed");
        let h = serde_json::to_value(StreamHeader::UploadPack {
            repo_id: "r-1".into(),
        })
        .unwrap();
        assert_eq!(
            h,
            serde_json::json!({"kind": "upload-pack", "repo_id": "r-1"})
        );
        let back: StreamHeader = serde_json::from_value(h).unwrap();
        assert!(matches!(back, StreamHeader::UploadPack { .. }));
    }
}
