//! git for potluck's own repositories: mirrors, base checkouts and workspaces.
//!
//! Git LFS is never smudged there: mirrors carry no LFS objects (and no LFS remote), so a
//! required LFS filter from the user's global config would fail every checkout ("Smudge error ...
//! missing protocol"). Every command sets `GIT_LFS_SKIP_SMUDGE=1` and switches the `lfs` filter
//! off (`filter.lfs.required=false`, empty `smudge` and `process`), so LFS files stay the pointer
//! files the commit holds, which is also what the index records: the tree counts as clean. Getting
//! LFS content into a workspace is the project's business. The caller's own repository
//! (`gitinfo::inspect`) keeps its filters: its helpers are `gitinfo::{git, git_bytes, git_text}`,
//! and these are named `tree_*` and `mirror_*`, so a `use` of the wrong set can't stand in for
//! these under the same names and smudge.
//!
//! Nor does git recurse into submodules there (`submodule.recurse=false`): a job's `git submodule
//! init` in its workspace marks the submodule active in the mirror's config, which every worktree
//! of the mirror shares, and with `submodule.recurse` in the user's global config the base's
//! `reset --hard` and a move's `read-tree -m -u` would then recurse into a submodule that has no
//! git dir there, and fail. Each submodule is a worktree of its own mirror, seeded on its own.
//!
//! A mirror is named to git explicitly (`GIT_DIR`), never found by git's own search from its
//! directory: git uses a bare repository it finds only while `safe.bareRepository` is `all`
//! (`explicit` is git's advice for a global config, and the default from Git 3.0), and a mirror
//! directory that isn't a repository any more (damaged, half deleted) would send that search on
//! up, into whatever repository holds the state directory. Bases and workspaces are found from
//! their `.git` files; potluck checks a `.git` is there before it runs git in one that may have
//! lost it.

use crate::gitinfo;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;

const NO_LFS: &[&str] = &[
    "-c",
    "filter.lfs.required=false",
    "-c",
    "filter.lfs.smudge=",
    "-c",
    "filter.lfs.process=",
];

const NO_RECURSE: &[&str] = &["-c", "submodule.recurse=false"];

/// A `git -C <dir>` command for a base, a workspace, or a directory that holds no repository.
pub(super) fn tree_git(dir: &Path) -> Command {
    let mut c = gitinfo::git(dir);
    c.args(NO_LFS);
    c.args(NO_RECURSE);
    c.env("GIT_LFS_SKIP_SMUDGE", "1");
    c
}

/// A git command for the bare mirror at `mirror`, named with `GIT_DIR`.
pub(super) fn mirror_git(mirror: &Path) -> Command {
    let mut c = tree_git(mirror);
    // Absolute: git reads GIT_DIR after `-C`.
    c.env(
        "GIT_DIR",
        std::path::absolute(mirror).unwrap_or_else(|_| mirror.to_path_buf()),
    );
    c
}

/// Runs git in `dir` and returns its stdout. A failure carries git's stderr.
pub(super) fn tree_bytes<I, S>(dir: &Path, args: I) -> anyhow::Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    stdout(tree_git(dir), args)
}

/// Like [`tree_bytes`], as trimmed text.
pub(super) fn tree_text<I, S>(dir: &Path, args: I) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Ok(text(tree_bytes(dir, args)?))
}

/// [`tree_bytes`] for the mirror at `mirror`.
pub(super) fn mirror_bytes<I, S>(mirror: &Path, args: I) -> anyhow::Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    stdout(mirror_git(mirror), args)
}

/// [`tree_text`] for the mirror at `mirror`.
pub(super) fn mirror_text<I, S>(mirror: &Path, args: I) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Ok(text(mirror_bytes(mirror, args)?))
}

fn stdout<I, S>(mut cmd: Command, args: I) -> anyhow::Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    cmd.args(args);
    Ok(gitinfo::output(&mut cmd)?.stdout)
}

fn text(out: Vec<u8>) -> String {
    String::from_utf8_lossy(&out).trim().to_string()
}
