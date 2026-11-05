//! Mirrors and workspaces on one machine.
//!
//! **Mirrors.** A bare mirror per repository (submodule repositories included) under
//! `mirrors/<repo_id>.git`. [`Workspaces::sync_mirror`] makes its branches and tags an exact copy of
//! the source's (force-updates and deletions included), from a git URL: a local path on the
//! submitting machine, or a tunnel over the mesh from phase 2. Refs under `refs/potluck/` are
//! potluck's own and survive syncing: a dispatch pins its commits under `refs/potluck/pins/<name>`
//! until its workspace is removed ([`Workspaces::pin`], [`Workspaces::fetch_pin`] for a commit on
//! no branch, such as a detached HEAD's), and `refs/potluck/base` holds the base checkout's commit.
//!
//! - The first sync clones the source (`git clone --bare`; from a local path git hardlinks the
//!   objects, so it is fast and takes little space). Later syncs fetch only what the mirror lacks.
//! - Divergence: only syncs write a mirror's branches and tags, and each sync records them
//!   (`potluck-synced` in the mirror). Before the next fetch they are compared with that record:
//!   any that changed (a job that ran `git commit` on a branch, `git branch` or `git tag` in its
//!   workspace writes the mirror's refs, which worktrees share) are listed in
//!   [`SyncReport::diverged`] for the daemon to report loudly, and the fetch resets them to the
//!   source's. The source's own force-updates and deletions are not divergence. A sync that fails
//!   keeps what it found (`potluck-diverged`) for the next one that succeeds to report.
//! - Checks, cheap enough for every sync: git's own (index-pack verifies the hash of every object
//!   received, and fetch checks the new tips are connected), then every branch, tag and potluck
//!   ref is read back: its object, and for branches and potluck refs the commit's root tree.
//! - A fetch that fails while the source can be read is tried once more, after the lock files a
//!   git killed while it updated refs leaves behind (`HEAD.lock`, `packed-refs.lock`,
//!   `refs/**/*.lock`, each failing every fetch after it) are removed once over a minute old: a
//!   transfer cut short, or a dead git's locks, is not damage.
//! - A full `git fsck --full` (too slow for every sync of a repository that vendors something like
//!   qemu) runs only when the fetch fails twice or the cheap check fails, or when a seeding from
//!   the mirror failed (it leaves a `potluck-suspect` marker in the mirror).
//! - Damage it finds is reported loudly in [`SyncReport::recloned`], with fsck's findings, and
//!   the mirror is recloned from the source without hardlinks (`--no-local`), keeping its
//!   worktrees (the base checkout and every workspace) and its potluck refs: their commits are
//!   fetched again from the source, and those the source lost too are listed in
//!   [`SyncReport::lost_pins`]. When fsck finds nothing the mirror is kept: a fetch that still
//!   fails is an `Err` (the next sync tries again), as are a source that can't be read and a full
//!   disk. A reclone cut short (the daemon stopped) is finished or undone by the next sync, which
//!   also removes the clones it left beside the mirror.
//! - Mirror config, set when a mirror is made: `core.checkStat=minimal` and
//!   `core.trustctime=false` (clones get new inodes and ctimes; see seeding), no hooks
//!   (`core.hooksPath=/dev/null`), no fsmonitor, untracked cache or split index, parallel checkout
//!   (`checkout.workers=0`), gc in the foreground (`gc.autoDetach=false`, so no git process
//!   outlives a sync), file names as the commit has them (`core.precomposeUnicode=false`: macOS's
//!   default would show a decomposed name as untracked), and `/.potluck/` in `info/exclude` (from
//!   when potluck kept files in a tree; its logs, status files and TMPDIR are outside trees now).
//!
//! **Workspaces.** One commit's tree on one machine, under `workspaces/<id>`:
//! 1. seeding: `git worktree add --detach --no-checkout` from the mirror (no new `.git`); the files
//!    are CoW clones of the machine's base checkout of the repository (`bases/<repo_id>`), along
//!    with a copy of the base's index; then a two-tree `git read-tree -m -u <base> <target>` writes
//!    only the files that differ (and removes those the target lacks). Each submodule is a worktree
//!    of its own mirror at its path, seeded the same way from its own base. Clones keep mode and
//!    mtime but get a new inode and ctime; with the mirror's `core.checkStat=minimal` and
//!    `core.trustctime=false` git compares only whole-second mtime and size, so it neither
//!    rewrites nor rereads the cloned files.
//! 2. the base: a worktree of the mirror (detached, with a valid index), created at the first
//!    workspace's commit (a full checkout, the cold path). When a workspace's commit is at least as
//!    new as the base's (committer time), the base first moves to it (the same two-tree
//!    `read-tree -m -u`, then HEAD), writing only the files that differ; the workspace is then
//!    cloned from it and git writes nothing more. An older commit leaves the base where it is and
//!    the workspace takes the difference. So the base follows the newest work, and seeding costs the clone plus the
//!    difference once. Cloning holds a shared lock on the base (`bases/<repo_id>.lock`), moving it
//!    an exclusive one. A base that fails its checks (missing, no HEAD, a leftover `index.lock`
//!    from a crash, a failed move) is removed and made again; a seeding whose tree comes out wrong
//!    discards the bases it used, so the next one starts fresh.
//! 3. verification: HEAD and every submodule's HEAD equal the dispatched commits, and right after
//!    seeding the tree is clean (`git status`, per repository); otherwise creating fails, loudly.
//!    Before a later invocation in the same workspace only the HEADs are checked (earlier
//!    invocations may have written files): [`Workspaces::verify`].
//!    A file git calls modified only because the commit's blob isn't what its own attributes or
//!    filters would store (a CRLF file committed before `*.txt text` was added: every checkout of
//!    that commit shows it modified once git rereads it) counts as clean when it holds exactly
//!    what a fresh checkout of it writes and its blob is intact (`filters.rs`).
//!    A tree that isn't clean only because the commit has paths that differ only in case
//!    (`README` and `readme`) or Unicode normalisation (a composed and a decomposed `é`) on a
//!    case-insensitive filesystem (APFS by default) fails saying so;
//!    that is the repository's doing, so the mirror isn't marked suspect and the base is kept. A
//!    base never moves onto such a commit; one just made at it (the cold path) is dropped.
//!    So is a seeding that fails because the commit has a file name the filesystem refuses (APFS
//!    takes only UTF-8 of the Unicode version it knows: U+A7DD is "Illegal byte sequence"; no
//!    name over 255 bytes anywhere): a base never moves onto a commit that adds one.
//!    Likewise a seeding that fails while the disk holding the workspaces has less than 256 MiB
//!    free fails saying the disk is full, and blames neither the base nor the mirror (a reclone
//!    would need room for a second copy). A base that couldn't be made or moved, for whatever
//!    reason, is removed at once, freeing what it took.
//! 4. removal: the tree (with every submodule worktree in it) moves aside to `<id>.removing`
//!    ([`aside`]) and is deleted with the TMPDIR before any mirror is locked, so a sync never waits
//!    for a big tree's deletion; then `git worktree prune` and the pins named after the workspace
//!    go. A tree left aside (its deletion failed) is deleted by the next removal. Removal
//!    waits only for the mirrors the workspace uses (the top repository and the submodules its
//!    commit's gitlinks name); other mirrors are swept for leftovers only when free, so a sync or a
//!    full fsck of another repository never holds it up.
//!
//! **Git LFS** content is not potluck's business: every git command potluck runs on mirrors, bases
//! and workspaces skips LFS smudging (`GIT_LFS_SKIP_SMUDGE=1`, and the `lfs` filter switched off
//! on its command line), since mirrors carry no LFS objects. LFS files stay the pointer files the
//! commit holds, which still counts as clean; a project that needs their content fetches it in its
//! job.
//!
//! Each workspace also has a TMPDIR beside its tree ([`Paths::workspace_tmp`]), created with it and
//! removed with it. [`Workspaces::create`] pins every commit it seeds under
//! `refs/potluck/pins/<workspace id>`; [`Workspaces::remove`] drops those pins.
//!
//! **Lifecycle policy** ([`tick`]): held until `held_until` (at least 5 minutes after each
//! invocation, extended by holds up to 24 hours from each request), then idle. On each machine the
//! newest idle workspace of each repository is kept; older idle ones are removed oldest first while
//! workspaces exceed the disk budget. A workspace with `pending` invocations is never removed.
//! [`reminders`] lists a branch's workspaces whose hold lasts (held ones, and ones a job runs in
//! that are held beyond now; not idle ones) for every response.
//!
//! Locks, so several threads or processes can use one state directory: `mirrors/<repo_id>.lock` is
//! taken exclusively while a mirror is fetched or recloned and shared by everything else that uses
//! the mirror; `mirrors/<repo_id>.worktrees.lock` serializes `git worktree` add, remove, prune and
//! list in a mirror (concurrent ones trip over each other's half-made admin dirs); the base lock
//! is described above.

mod case;
pub(crate) mod filters;
mod git;
mod lifecycle;
mod lock;
mod mirror;
mod seed;

pub use lifecycle::{Action, hold_until, reminders, tick};
pub use mirror::SyncReport;
pub use seed::aside;
pub(crate) use seed::disk_full;

use crate::config::Paths;
use anyhow::bail;

pub struct Workspaces {
    pub paths: Paths,
}

impl Workspaces {
    /// A relative state directory is made absolute here, against the current directory: every git
    /// potluck runs changes into a repository first (`git -C`), where a relative path handed to git
    /// would resolve against the wrong directory.
    pub fn new(mut paths: Paths) -> Workspaces {
        if let Ok(home) = std::path::absolute(&paths.home) {
            paths.home = home;
        }
        Workspaces { paths }
    }

    fn mirror_lock(&self, repo_id: &str) -> std::path::PathBuf {
        self.paths
            .home
            .join("mirrors")
            .join(format!("{repo_id}.lock"))
    }

    /// Held exclusively around every `git worktree` add, remove, prune and list in a mirror: git
    /// reads every worktree's admin dir for these, and one that another `worktree add` is still
    /// creating makes it fail ("failed to read worktrees/<name>/commondir").
    fn worktrees_lock(&self, repo_id: &str) -> std::path::PathBuf {
        self.paths
            .home
            .join("mirrors")
            .join(format!("{repo_id}.worktrees.lock"))
    }

    fn base_lock(&self, repo_id: &str) -> std::path::PathBuf {
        self.paths
            .home
            .join("bases")
            .join(format!("{repo_id}.lock"))
    }
}

/// Refuses ids that can't be a single path component and ref name: repository and workspace ids
/// become directory names, worktree names and pin names.
fn check_id(kind: &str, id: &str) -> anyhow::Result<()> {
    let ok = !id.is_empty()
        && !id.starts_with('.')
        && !id.ends_with(".lock")
        && !id.contains("..")
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    if !ok {
        bail!("bad {kind} id {id:?}: use letters, digits, '-', '_' and '.'");
    }
    Ok(())
}

/// Refuses pin names git would reject (or that could escape `refs/potluck/pins/`).
fn check_pin_name(name: &str) -> anyhow::Result<()> {
    let ok = !name.is_empty()
        && !name.starts_with('/')
        && !name.ends_with('/')
        && !name.ends_with('.')
        && !name.contains("//")
        && name.split('/').all(|c| {
            !c.starts_with('.')
                && !c.ends_with(".lock")
                && !c.contains("..")
                && c.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        });
    if !ok {
        bail!("bad pin name {name:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_pin_names() {
        for good in ["msi-w7", "potluck-0a1b2c3d", "a.b_c"] {
            check_id("workspace", good).unwrap();
        }
        for bad in ["", ".x", "a/b", "a..b", "x.lock", "a b"] {
            assert!(check_id("workspace", bad).is_err(), "{bad:?}");
        }
        for good in ["msi-w7", "job/studio-42"] {
            check_pin_name(good).unwrap();
        }
        for bad in [
            "", "/x", "x/", "a//b", "a/.b", "a..b", "x.lock", "a b", "x.",
        ] {
            assert!(check_pin_name(bad).is_err(), "{bad:?}");
        }
    }
}
