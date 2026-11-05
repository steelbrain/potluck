//! Reading the caller's repository on the submitting machine, for `dispatch`.
//!
//! - Refuses a tree with uncommitted changes (tracked changes or untracked files that aren't
//!   ignored), in submodules too: only committed work runs. The message names the files. A file
//!   git calls modified only because its own attributes or filters would store the committed
//!   blob differently, and that holds exactly the blob's bytes or what a checkout of it writes,
//!   is committed work (`workspace::filters`).
//! - The commit is HEAD; the branch is HEAD's branch, if any.
//! - The repository id is `<name>-<8 hex>` ([`crate::types::repo_id`]): the name of the main
//!   worktree's directory and the 32-bit FNV-1a hash of the common git dir's canonical path, so
//!   every worktree of one repository gets the same id. The daemon salts it, and the submodules'
//!   ids, with its node name when it takes the dispatch ([`owned_repo`]).
//! - Submodules come from the commit's tree (gitlinks), recursively: a nested submodule is listed
//!   with its full path from the top (`vendor/qemu/roms/seabios`). Each has its own git dir (the
//!   superproject's `.git/modules/...`, or `.git/worktrees/<worktree>/modules/...` in a linked
//!   worktree) and must be checked out at the recorded commit. Its repository id comes from the
//!   top repository's id and its path ([`submodule_repo_id`]), not from its git dir, so every
//!   worktree of the superproject shares one mirror per submodule on each machine.
//! - A dispatch can leave submodules out ([`inspect_without`]): patterns on the full path from
//!   the top. One left out, and everything nested in it, is neither checked nor listed, so the
//!   job's workspace doesn't have it, and its uncommitted changes don't count.
//!
//! Git is driven through the `git` command line. Every git potluck runs drops the environment
//! variables that would point it at another repository, index or object store
//! (`git rev-parse --local-env-vars`), so a potluck started from a git hook still reads the
//! repository it was pointed at.

use crate::types::{RepoRef, SubmoduleRef};
use anyhow::{Context, bail};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

/// `git rev-parse --local-env-vars`.
const GIT_LOCAL_ENV: &[&str] = &[
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CONFIG",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_COUNT",
    "GIT_OBJECT_DIRECTORY",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_IMPLICIT_WORK_TREE",
    "GIT_GRAFT_FILE",
    "GIT_INDEX_FILE",
    "GIT_NO_REPLACE_OBJECTS",
    "GIT_REPLACE_REF_BASE",
    "GIT_PREFIX",
    "GIT_SHALLOW_FILE",
    "GIT_COMMON_DIR",
];

/// The most uncommitted paths a refusal lists.
const MAX_LISTED: usize = 30;

/// A `git -C <dir>` command that never prompts and reads no stdin.
pub(crate) fn git(dir: &Path) -> Command {
    let mut c = Command::new("git");
    c.arg("-C").arg(dir);
    for v in GIT_LOCAL_ENV {
        c.env_remove(v);
    }
    c.env("GIT_TERMINAL_PROMPT", "0");
    c.stdin(Stdio::null());
    c
}

/// Runs `git -C dir args...` and returns its stdout. A failure carries git's stderr.
pub(crate) fn git_bytes<I, S>(dir: &Path, args: I) -> anyhow::Result<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd = git(dir);
    cmd.args(args);
    let out = output(&mut cmd)?;
    Ok(out.stdout)
}

/// Like [`git_bytes`], as trimmed text.
pub(crate) fn git_text<I, S>(dir: &Path, args: I) -> anyhow::Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let out = git_bytes(dir, args)?;
    Ok(String::from_utf8_lossy(&out).trim().to_string())
}

/// Runs `cmd` capturing its output; `Err` unless it exits 0, with its stderr.
pub(crate) fn output(cmd: &mut Command) -> anyhow::Result<Output> {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let out = cmd
        .output()
        .with_context(|| format!("can't run {}", describe(cmd)))?;
    if !out.status.success() {
        bail!(
            "{} failed ({}): {}",
            describe(cmd),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(out)
}

/// `git -C dir a b c`, for error messages.
pub(crate) fn describe(cmd: &Command) -> String {
    let mut s = cmd.get_program().to_string_lossy().into_owned();
    for a in cmd.get_args() {
        s.push(' ');
        s.push_str(&a.to_string_lossy());
    }
    s
}

/// What a dispatch reads of the caller's repository ([`inspect_without`]).
#[derive(Clone, Debug)]
pub struct Inspected {
    pub repo: RepoRef,
    /// The submodules left out, by their full paths from the top, parents before what is nested
    /// in them.
    pub left_out: Vec<String>,
}

/// Inspects the repository containing `cwd`.
pub fn inspect(cwd: &Path) -> anyhow::Result<RepoRef> {
    inspect_without(cwd, &mut |_| Ok(Vec::new())).map(|i| i.repo)
}

/// [`inspect`], leaving out the submodules that `without` names for the commit (called once,
/// with HEAD's commit, before any submodule is read): patterns ([`glob::Pattern`], where `*`
/// doesn't cross a `/`) on the full path from the top, such as `vendor/qemu` or
/// `vendor/qemu/roms/*`. A pattern that matches no submodule of the commit is refused, unless it
/// names one inside a submodule left out or not looked into.
pub fn inspect_without(
    cwd: &Path,
    without: &mut dyn FnMut(&str) -> anyhow::Result<Vec<String>>,
) -> anyhow::Result<Inspected> {
    let out = git_text(
        cwd,
        [
            "rev-parse",
            "--path-format=absolute",
            "--show-toplevel",
            "--git-common-dir",
        ],
    )
    .with_context(|| {
        format!(
            "{} is not inside a git work tree; run dispatch in the repository whose commit should run",
            cwd.display()
        )
    })?;
    let mut lines = out.lines();
    let (Some(top), Some(common)) = (lines.next(), lines.next()) else {
        bail!("unexpected output from git rev-parse in {}", cwd.display());
    };
    let top = canonical(Path::new(top))?;
    let common = canonical(Path::new(common))?;

    let commit = git_text(&top, ["rev-parse", "--verify", "--quiet", "HEAD^{commit}"])
        .map_err(|_| anyhow::anyhow!("{}: HEAD has no commit yet", top.display()))?;
    // The full ref name: `--short` would give `heads/x` when a tag is also called `x`.
    let branch = git_text(&top, ["symbolic-ref", "--quiet", "HEAD"])
        .ok()
        .and_then(|r| r.strip_prefix("refs/heads/").map(str::to_string))
        .filter(|b| !b.is_empty());

    let mut skip = Skip::new(&without(&commit)?)?;
    let mut problems = Vec::new();
    dirty_paths(&top, "", &mut problems)?;
    let mut submodules = Vec::new();
    let id = repo_id(&common);
    collect_submodules(
        &top,
        &commit,
        &id,
        "",
        &mut submodules,
        &mut problems,
        &mut skip,
    )?;
    // What is left out doesn't count: a submodule left out at another commit than recorded
    // shows as modified in its parent's status.
    problems.retain(|p| p.get(3..).is_none_or(|path| !skip.covers(path)));
    if !problems.is_empty() {
        let shown = problems.len().min(MAX_LISTED);
        let mut msg = format!(
            "refusing to dispatch from {}: only committed work runs, and this isn't committed:\n",
            top.display()
        );
        for p in &problems[..shown] {
            msg.push_str("  ");
            msg.push_str(p);
            msg.push('\n');
        }
        if problems.len() > shown {
            msg.push_str(&format!("  ... and {} more\n", problems.len() - shown));
        }
        msg.push_str(
            "Commit it (or add untracked files to .gitignore), check submodules out at the \
             commits recorded, and dispatch again.",
        );
        bail!(msg);
    }
    skip.all_matched(&commit)?;
    Ok(Inspected {
        repo: RepoRef {
            id,
            source: common,
            commit,
            branch,
            submodules,
        },
        left_out: skip.left_out,
    })
}

/// The submodules a dispatch leaves out, and what [`collect_submodules`] saw of them.
struct Skip {
    /// Each pattern as given, parsed, and whether a submodule matched it.
    patterns: Vec<(String, glob::Pattern, bool)>,
    left_out: Vec<String>,
    /// Submodules not looked into (not checked out, or at another commit): what is nested in
    /// them can't be matched.
    unread: Vec<String>,
    seen: Vec<String>,
}

impl Skip {
    fn new(patterns: &[String]) -> anyhow::Result<Skip> {
        let mut parsed = Vec::new();
        for raw in patterns {
            let p = raw.trim_end_matches('/');
            if p.is_empty() {
                bail!("leaving out submodules: {raw:?} names no path");
            }
            let pattern = glob::Pattern::new(p)
                .map_err(|e| anyhow::anyhow!("leaving out submodules: {raw:?}: {e}"))?;
            parsed.push((p.to_string(), pattern, false));
        }
        Ok(Skip {
            patterns: parsed,
            left_out: Vec::new(),
            unread: Vec::new(),
            seen: Vec::new(),
        })
    }

    /// Whether the submodule at `full` is left out (noting which patterns matched it).
    fn leaves_out(&mut self, full: &str) -> bool {
        self.seen.push(full.to_string());
        let opts = glob::MatchOptions {
            require_literal_separator: true,
            ..Default::default()
        };
        let mut hit = false;
        for (_, pattern, matched) in &mut self.patterns {
            if pattern.matches_with(full, opts) {
                *matched = true;
                hit = true;
            }
        }
        if hit {
            self.left_out.push(full.to_string());
        }
        hit
    }

    /// Whether `path` is a submodule left out or inside one.
    fn covers(&self, path: &str) -> bool {
        self.left_out.iter().any(|l| {
            path == l
                || path
                    .strip_prefix(l.as_str())
                    .is_some_and(|r| r.starts_with('/'))
        })
    }

    /// Refuses a pattern that matched no submodule: a typo would ship what it meant to leave out.
    fn all_matched(&self, commit: &str) -> anyhow::Result<()> {
        let inside = |p: &str, dirs: &[String]| {
            dirs.iter().any(|d| {
                p.strip_prefix(d.as_str())
                    .is_some_and(|r| r.starts_with('/'))
            })
        };
        let unmatched: Vec<&str> = self
            .patterns
            .iter()
            .filter(|(p, _, matched)| {
                !matched && !inside(p, &self.left_out) && !inside(p, &self.unread)
            })
            .map(|(p, _, _)| p.as_str())
            .collect();
        if unmatched.is_empty() {
            return Ok(());
        }
        let known = if self.seen.is_empty() {
            "it has none".to_string()
        } else {
            let shown = self.seen.len().min(MAX_LISTED);
            let mut s = format!("it has {}", self.seen[..shown].join(", "));
            if self.seen.len() > shown {
                s.push_str(&format!(" and {} more", self.seen.len() - shown));
            }
            s
        };
        bail!(
            "leaving out submodules: {} {} no submodule of commit {} ({known})",
            unmatched
                .iter()
                .map(|p| format!("{p:?}"))
                .collect::<Vec<_>>()
                .join(", "),
            if unmatched.len() == 1 {
                "matches"
            } else {
                "match"
            },
            short(commit)
        )
    }
}

/// The repository id for a common git dir: [`crate::types::repo_id`], shared with the client's
/// context so reminders match.
pub fn repo_id(common_dir: &Path) -> String {
    crate::types::repo_id(common_dir)
}

pub(crate) fn canonical(p: &Path) -> anyhow::Result<PathBuf> {
    std::fs::canonicalize(p).with_context(|| format!("can't resolve {}", p.display()))
}

/// Appends the uncommitted paths of the repository at `dir` (status lines, paths prefixed with
/// `prefix`). Changes inside submodules are left to their own pass; a submodule at another commit
/// than the index or HEAD records shows here.
fn dirty_paths(dir: &Path, prefix: &str, out: &mut Vec<String>) -> anyhow::Result<()> {
    let raw = git_bytes(
        dir,
        [
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=normal",
            "--ignore-submodules=dirty",
        ],
    )?;
    let mut lines = Vec::new();
    let mut fields = raw.split(|b| *b == 0).filter(|f| !f.is_empty());
    while let Some(f) = fields.next() {
        if f.len() < 4 {
            continue;
        }
        let line = String::from_utf8_lossy(f).into_owned();
        // A rename or copy is followed by its source path.
        if line[..2].contains('R') || line[..2].contains('C') {
            fields.next();
        }
        lines.push(line);
    }
    // A file git calls modified only because its own filters would store the committed blob
    // differently (a CRLF file committed before `*.txt text` was added) holds the committed work:
    // its bytes as committed, or as a checkout of them writes them.
    if !lines.is_empty() {
        lines = crate::workspace::filters::unexplained(dir, lines.clone(), true).unwrap_or(lines);
    }
    for line in lines {
        out.push(format!("{} {prefix}{}", &line[..2], line[3..].trim_end()));
    }
    Ok(())
}

/// The gitlinks (path, commit) in `commit`'s tree in the repository at `dir`.
pub(crate) fn gitlinks(dir: &Path, commit: &str) -> anyhow::Result<Vec<(String, String)>> {
    let raw = git_bytes(dir, gitlinks_args(commit))?;
    parse_gitlinks(&raw, dir)
}

/// The git arguments that list `commit`'s tree for [`parse_gitlinks`].
pub(crate) fn gitlinks_args(commit: &str) -> [&str; 5] {
    ["ls-tree", "-r", "-z", "--full-tree", commit]
}

/// The gitlinks in the output of git with [`gitlinks_args`], run in the repository at `dir`.
pub(crate) fn parse_gitlinks(raw: &[u8], dir: &Path) -> anyhow::Result<Vec<(String, String)>> {
    let mut links = Vec::new();
    for rec in raw.split(|b| *b == 0).filter(|r| !r.is_empty()) {
        // "<mode> SP <type> SP <object> TAB <path>"
        let Some(tab) = rec.iter().position(|b| *b == b'\t') else {
            continue;
        };
        let meta = String::from_utf8_lossy(&rec[..tab]);
        let mut parts = meta.split(' ');
        if parts.next() != Some("160000") {
            continue;
        }
        let sha = parts.nth(1).unwrap_or_default().to_string();
        let path = std::str::from_utf8(&rec[tab + 1..])
            .map_err(|_| anyhow::anyhow!("a submodule path in {} isn't UTF-8", dir.display()))?;
        links.push((path.to_string(), sha));
    }
    Ok(links)
}

fn collect_submodules(
    dir: &Path,
    commit: &str,
    top_id: &str,
    prefix: &str,
    subs: &mut Vec<SubmoduleRef>,
    problems: &mut Vec<String>,
    skip: &mut Skip,
) -> anyhow::Result<()> {
    for (path, sha) in gitlinks(dir, commit)? {
        let full = format!("{prefix}{path}");
        if skip.leaves_out(&full) {
            continue;
        }
        let sub_dir = dir.join(&path);
        let not_checked_out = || {
            // Escaped: the flag takes a glob, and a path may hold `*`, `?` or `[`.
            format!(
                "submodule {full}: not checked out (git submodule update --init --recursive, or leave it out with --without-submodule {})",
                glob::Pattern::escape(&full)
            )
        };
        if sub_dir.join(".git").symlink_metadata().is_err() {
            problems.push(not_checked_out());
            skip.unread.push(full.clone());
            continue;
        }
        let out = git_text(
            &sub_dir,
            [
                "rev-parse",
                "--path-format=absolute",
                "--show-toplevel",
                "--git-common-dir",
                "HEAD",
            ],
        );
        let Ok(out) = out else {
            problems.push(not_checked_out());
            skip.unread.push(full.clone());
            continue;
        };
        let lines: Vec<&str> = out.lines().collect();
        if lines.len() < 3 || canonical(Path::new(lines[0]))? != canonical(&sub_dir)? {
            problems.push(not_checked_out());
            skip.unread.push(full.clone());
            continue;
        }
        let common = canonical(Path::new(lines[1]))?;
        let head = lines[2];
        if head != sha {
            problems.push(format!(
                "submodule {full}: checked out at {}, but the commit records {} (commit the \
                 submodule change, or git submodule update)",
                short(head),
                short(&sha)
            ));
            skip.unread.push(full.clone());
            continue;
        }
        let sub_prefix = format!("{full}/");
        dirty_paths(&sub_dir, &sub_prefix, problems)?;
        subs.push(SubmoduleRef {
            repo_id: submodule_repo_id(top_id, &full),
            path: full,
            source: common,
            commit: sha.clone(),
        });
        collect_submodules(&sub_dir, &sha, top_id, &sub_prefix, subs, problems, skip)?;
    }
    Ok(())
}

/// A submodule's repository id: the last component of its path from the top, made an
/// [`crate::types::id_name`], and the FNV-1a hash of the top repository's id and that path.
/// Worktrees of one superproject keep their submodules' git dirs apart, but they are clones of
/// the same repositories: one mirror each. Under a salted top id
/// ([`crate::types::owned_repo_id`]) it is `<name>-<16 hex>` (64-bit hash), so it is salted with
/// the owner too; under an id from before salting, `<name>-<8 hex>` (32-bit) as it was then.
pub fn submodule_repo_id(top_id: &str, path: &str) -> String {
    let name = crate::types::id_name(path.rsplit('/').next().unwrap_or_default(), "sub");
    let bytes = top_id.bytes().chain(*b"/").chain(path.bytes());
    if crate::types::is_owned_repo_id(top_id) {
        return format!("{name}-{:016x}", crate::types::fnv1a64(bytes));
    }
    let mut hash: u32 = 0x811c_9dc5;
    for b in bytes {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{name}-{hash:08x}")
}

/// `repo` with its ids salted with `owner` ([`crate::types::owned_repo_id`]) and its submodules'
/// ids derived from the salted top id.
pub fn owned_repo(owner: &str, repo: &RepoRef) -> RepoRef {
    let id = crate::types::owned_repo_id(owner, &repo.id);
    let submodules = repo
        .submodules
        .iter()
        .map(|s| SubmoduleRef {
            repo_id: submodule_repo_id(&id, &s.path),
            ..s.clone()
        })
        .collect();
    RepoRef {
        id,
        submodules,
        ..repo.clone()
    }
}

/// The first 12 characters of a commit id, for messages. Ids from peers or raw requests aren't
/// checked yet where they are shown, so this never slices inside a character.
pub(crate) fn short(sha: &str) -> &str {
    sha.get(..12).unwrap_or(sha)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A commit from a peer's queue or a raw dispatch isn't checked before it is shown: one with a
    /// character across byte 12 is shown whole, not a panic.
    #[test]
    fn short_commits() {
        assert_eq!(short("0123456789abcdef"), "0123456789ab");
        assert_eq!(short("abc"), "abc");
        assert_eq!(short("aaaaaaaaaaa\u{e9}"), "aaaaaaaaaaa\u{e9}");
    }

    #[test]
    fn ids() {
        let a = repo_id(Path::new("/home/u/src/potluck/.git"));
        assert!(
            a.starts_with("potluck-") && a.len() == "potluck-".len() + 8,
            "{a}"
        );
        assert_eq!(a, repo_id(Path::new("/home/u/src/potluck/.git")));
        assert_ne!(a, repo_id(Path::new("/home/u/other/potluck/.git")));
        assert!(repo_id(Path::new("/srv/git/tools.git")).starts_with("tools-"));
        assert!(repo_id(Path::new("/r/.git/modules/vendor/qemu")).starts_with("qemu-"));
        assert!(repo_id(Path::new("/r/my repo/.git")).starts_with("my-repo-"));
        let q = submodule_repo_id("app-1234abcd", "vendor/qemu");
        assert!(q.starts_with("qemu-"));
        assert_eq!(q, submodule_repo_id("app-1234abcd", "vendor/qemu"));
        assert_ne!(q, submodule_repo_id("app-1234abcd", "third_party/qemu"));
        assert_ne!(q, submodule_repo_id("other-1234abcd", "vendor/qemu"));
        // Under a salted top id, salted (16 hex) too.
        let top = crate::types::owned_repo_id("studio", "app-1234abcd");
        let s = submodule_repo_id(&top, "vendor/qemu");
        assert!(
            s.starts_with("qemu-") && s.len() == "qemu-".len() + 16,
            "{s}"
        );
        assert_ne!(
            s,
            submodule_repo_id(
                &crate::types::owned_repo_id("msi", "app-1234abcd"),
                "vendor/qemu"
            )
        );
    }

    #[test]
    fn owned_repos_salt_every_id() {
        let repo = RepoRef {
            id: "app-1234abcd".into(),
            source: PathBuf::from("/src/app/.git"),
            commit: "c".repeat(40),
            branch: Some("main".into()),
            submodules: vec![SubmoduleRef {
                path: "vendor/qemu".into(),
                repo_id: submodule_repo_id("app-1234abcd", "vendor/qemu"),
                source: PathBuf::from("/src/app/.git/modules/vendor/qemu"),
                commit: "d".repeat(40),
            }],
        };
        let a = owned_repo("studio", &repo);
        let b = owned_repo("msi", &repo);
        assert_eq!(a.id, crate::types::owned_repo_id("studio", "app-1234abcd"));
        assert_eq!(
            a.submodules[0].repo_id,
            submodule_repo_id(&a.id, "vendor/qemu")
        );
        assert_ne!(a.id, b.id);
        assert_ne!(a.submodules[0].repo_id, b.submodules[0].repo_id);
        assert_ne!(a.submodules[0].repo_id, repo.submodules[0].repo_id);
        // Everything else as it was.
        assert_eq!(
            (a.commit, a.source, a.branch),
            (repo.commit, repo.source, repo.branch)
        );
        assert_eq!(a.submodules[0].commit, repo.submodules[0].commit);
    }
}
