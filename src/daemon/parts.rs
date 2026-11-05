//! The parts the daemon's core runs with, injected so the core can be tested with fakes: the
//! executor ([`crate::exec::Executor`]), workspaces, the scheduler, facts, the login
//! environment and the clocks. [`super::run`] wires in the real ones.

use crate::exec::Executor;
use crate::proto::WhyView;
use crate::sched::{MachineView, Plan, Queued};
use crate::types::{Facts, Millis, Reminder, RepoRef, WorkspaceId, WorkspaceRecord};
use crate::why::Snapshot;
use crate::workspace::{Action, Workspaces};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Everything the daemon's core needs from outside itself.
pub struct Parts {
    pub executor: Arc<dyn Executor>,
    pub workspaces: Arc<dyn WorkspaceProvider>,
    pub scheduler: Arc<dyn Scheduler>,
    /// Detects this machine's facts; called at start and now and then after.
    pub facts: Box<dyn Fn() -> anyhow::Result<Facts> + Send + Sync>,
    /// The runner user's login environment, the base of every job's environment.
    pub login_env: BTreeMap<String, String>,
    /// The wall clock and the boot clock ([`super::clock`]).
    pub clock: Arc<dyn super::Clock>,
}

/// Mirrors, workspaces and their lifecycle policy: [`crate::workspace`] behind a trait. The
/// policy functions default to the real ones.
pub trait WorkspaceProvider: Send + Sync {
    /// Syncs the mirror; `Ok(Some(text))` is a warning to report loudly (the mirror was recloned,
    /// or something other than potluck had changed its branches and tags).
    fn sync_mirror(&self, repo_id: &str, url: &str) -> anyhow::Result<Option<String>>;
    fn has_commit(&self, repo_id: &str, commit: &str) -> bool;
    fn pin(&self, repo_id: &str, commit: &str, name: &str) -> anyhow::Result<()>;
    /// Pins `commit`, first fetching it from `url` when the mirror lacks it (a commit on no branch
    /// or tag, such as a submodule's detached HEAD, never comes with a sync).
    fn fetch_pin(&self, repo_id: &str, url: &str, commit: &str, name: &str) -> anyhow::Result<()> {
        let _ = url;
        self.pin(repo_id, commit, name)
    }
    fn unpin(&self, repo_id: &str, name: &str) -> anyhow::Result<()>;
    /// Every pin in every mirror, as (repository id, pin name). A provider that can't list them
    /// says none.
    fn pins(&self) -> anyhow::Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
    fn create(&self, id: &WorkspaceId, repo: &RepoRef) -> anyhow::Result<PathBuf>;
    fn verify(&self, root: &Path, repo: &RepoRef) -> anyhow::Result<()>;
    /// `dispatch --advance`: checks workspace `id` at `root`, at commit `from` as its record says,
    /// out at `repo`'s commits, keeping its untracked and ignored files, and moves its pins to
    /// them ([`Workspaces::advance`]). A provider that can't says so.
    fn advance(
        &self,
        id: &WorkspaceId,
        root: &Path,
        from: &str,
        repo: &RepoRef,
    ) -> anyhow::Result<()> {
        let _ = (id, root, from, repo);
        anyhow::bail!("this machine's workspaces can't be advanced")
    }
    fn remove(&self, record: &WorkspaceRecord) -> anyhow::Result<()>;
    fn size(&self, root: &Path) -> anyhow::Result<u64>;
    /// Removes the mirrors and bases with repository ids from before ids were salted with the
    /// owner, once nothing uses them; returns their ids.
    fn remove_unsalted(&self) -> Vec<String> {
        Vec::new()
    }

    fn tick(&self, now: Millis, records: &[WorkspaceRecord], budget_bytes: u64) -> Vec<Action> {
        crate::workspace::tick(now, records, budget_bytes)
    }
    fn hold_until(&self, ended_at: Millis, requested_ms: Option<u64>, current: Millis) -> Millis {
        crate::workspace::hold_until(ended_at, requested_ms, current)
    }
    fn reminders(
        &self,
        now: Millis,
        records: &[WorkspaceRecord],
        repo_id: &str,
        branch: &str,
    ) -> Vec<Reminder> {
        crate::workspace::reminders(now, records, repo_id, branch)
    }
}

impl WorkspaceProvider for Workspaces {
    fn sync_mirror(&self, repo_id: &str, url: &str) -> anyhow::Result<Option<String>> {
        Workspaces::sync_mirror(self, repo_id, url).map(|r| r.warning(repo_id))
    }
    fn fetch_pin(&self, repo_id: &str, url: &str, commit: &str, name: &str) -> anyhow::Result<()> {
        Workspaces::fetch_pin(self, repo_id, url, commit, name)
    }
    fn has_commit(&self, repo_id: &str, commit: &str) -> bool {
        Workspaces::has_commit(self, repo_id, commit)
    }
    fn pin(&self, repo_id: &str, commit: &str, name: &str) -> anyhow::Result<()> {
        Workspaces::pin(self, repo_id, commit, name)
    }
    fn unpin(&self, repo_id: &str, name: &str) -> anyhow::Result<()> {
        Workspaces::unpin(self, repo_id, name)
    }
    fn pins(&self) -> anyhow::Result<Vec<(String, String)>> {
        Workspaces::pins(self)
    }
    fn create(&self, id: &WorkspaceId, repo: &RepoRef) -> anyhow::Result<PathBuf> {
        Workspaces::create(self, id, repo)
    }
    fn verify(&self, root: &Path, repo: &RepoRef) -> anyhow::Result<()> {
        Workspaces::verify(self, root, repo)
    }
    fn advance(
        &self,
        id: &WorkspaceId,
        root: &Path,
        from: &str,
        repo: &RepoRef,
    ) -> anyhow::Result<()> {
        Workspaces::advance(self, id, root, from, repo)
    }
    fn remove(&self, record: &WorkspaceRecord) -> anyhow::Result<()> {
        Workspaces::remove(self, record)
    }
    fn size(&self, root: &Path) -> anyhow::Result<u64> {
        Workspaces::size(self, root)
    }
    fn remove_unsalted(&self) -> Vec<String> {
        Workspaces::remove_unsalted(self)
    }
}

/// Scheduling and `why`: [`crate::sched`] and [`crate::why`] behind a trait. Everything defaults
/// to the real functions.
pub trait Scheduler: Send + Sync {
    fn feasible(&self, machine: &MachineView, job: &Queued) -> Result<(), String> {
        crate::sched::feasible(machine, job)
    }
    fn order(&self, queue: &[Queued]) -> Vec<usize> {
        crate::sched::order(queue)
    }
    fn plan(&self, now: Millis, machine: &MachineView, queue: &[Queued]) -> Plan {
        crate::sched::plan(now, machine, queue)
    }
    fn explain(&self, job: &str, snap: &Snapshot) -> WhyView {
        crate::why::explain(job, snap)
    }
    /// `explain`, with the machines whose mirrors lack the job's commits yet, and those that
    /// leave it to a warm member for now (locality).
    fn explain_waits(&self, job: &str, snap: &Snapshot, waits: &crate::why::Waits) -> WhyView {
        crate::why::explain_waits(job, snap, waits)
    }
}

/// The real scheduler.
pub struct RealScheduler;

impl Scheduler for RealScheduler {}
