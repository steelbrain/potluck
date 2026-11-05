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
//! - A dispatch can carry other repositories' commits along (`--carry DIR[@REV]`, [`carry_all`]),
//!   which go into the mirrors like a submodule's but are never checked out. A carried
//!   repository's id is [`repo_id`] of its common git dir, the id a dispatch from that repository
//!   gets, so the daemon's salting makes it the id of the mirror its own jobs use.
//!
//! Git is driven through the `git` command line. Every git potluck runs drops the environment
//! variables that would point it at another repository, index or object store
//! (`git rev-parse --local-env-vars`), so a potluck started from a git hook still reads the
//! repository it was pointed at.

use crate::types::{CarriedRef, RepoRef, SubmoduleRef};
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
            carried: Vec::new(),
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

/// `repo` with its ids salted with `owner` ([`crate::types::owned_repo_id`]): its submodules'
/// ids derived from the salted top id, and its carried repositories' ids salted as a dispatch
/// from each of them would be ([`owned_carried`]).
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
        carried: owned_carried(owner, &repo.carried),
        ..repo.clone()
    }
}

/// Carried repositories with their ids salted with `owner`, each as a dispatch from that
/// repository itself is ([`crate::types::owned_repo_id`] of the CLI's [`repo_id`]), so their
/// commits go into the mirrors that repository's own jobs use. Salted also for a dispatch into a
/// workspace from before salting, which keeps its own unsalted ids: a carried repository's
/// mirror isn't that workspace's.
pub fn owned_carried(owner: &str, carried: &[CarriedRef]) -> Vec<CarriedRef> {
    carried
        .iter()
        .map(|c| CarriedRef {
            repo_id: crate::types::owned_repo_id(owner, &c.repo_id),
            ..c.clone()
        })
        .collect()
}

/// What `--carry DIR[@REV]` names, relative to `cwd`: the directory and the revision. The whole
/// argument is the directory when it is one, with REV HEAD. Otherwise it splits at the last `@`
/// whose left side is an existing directory, so a revision may hold `@` (`dep@HEAD@{1}`) and a
/// directory too (`dep@2@main`); without an `@`, REV is HEAD.
pub fn split_carry(arg: &str, cwd: &Path) -> anyhow::Result<(PathBuf, String)> {
    if arg.is_empty() {
        bail!("--carry \"\": names no directory (DIR[@REV])");
    }
    let whole = cwd.join(arg);
    if whole.is_dir() {
        return Ok((whole, "HEAD".to_string()));
    }
    let splits: Vec<usize> = arg.match_indices('@').map(|(i, _)| i).collect();
    for &at in splits.iter().rev() {
        let (dir, rev) = (&arg[..at], &arg[at + 1..]);
        let path = cwd.join(dir);
        if dir.is_empty() || !path.is_dir() {
            continue;
        }
        if rev.is_empty() {
            bail!("--carry {arg}: names no revision after the @ (DIR[@REV])");
        }
        return Ok((path, rev.to_string()));
    }
    let dir = splits.last().map_or(arg, |&at| &arg[..at]);
    if dir.is_empty() {
        bail!("--carry {arg}: names no directory before the @ (DIR[@REV])");
    }
    bail!(
        "--carry {arg}: there is no directory {}",
        cwd.join(dir).display()
    )
}

/// The repository and commit `--carry DIR[@REV]` names ([`split_carry`]): DIR is any directory
/// in a git repository, a bare one too, and REV (HEAD by default) is resolved there to a commit
/// (`rev-parse --verify REV^{commit}`), HEAD being that worktree's. Its id is [`repo_id`] of its
/// common git dir; its name is the directory name of its worktree's top (of a bare repository,
/// its own name without `.git`), with every character but ASCII letters, digits and `_` made
/// `_`.
pub fn carried(arg: &str, cwd: &Path) -> anyhow::Result<CarriedRef> {
    let (dir, rev) = split_carry(arg, cwd)?;
    let out = git_text(
        &dir,
        [
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
            "--is-inside-work-tree",
        ],
    )
    .map_err(|_| {
        anyhow::anyhow!(
            "--carry {arg}: {} is not in a git repository",
            dir.display()
        )
    })?;
    let mut lines = out.lines();
    let (Some(common), Some(inside)) = (lines.next(), lines.next()) else {
        bail!("unexpected output from git rev-parse in {}", dir.display());
    };
    let common = canonical(Path::new(common))?;
    let top = if inside == "true" {
        let top = git_text(
            &dir,
            ["rev-parse", "--path-format=absolute", "--show-toplevel"],
        )?;
        Some(canonical(Path::new(&top))?)
    } else {
        None
    };
    let commit = git_text(
        &dir,
        [
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            &format!("{rev}^{{commit}}"),
        ],
    )
    .map_err(|_| {
        anyhow::anyhow!(
            "--carry {arg}: {rev} names no commit in {}",
            top.as_deref().unwrap_or(&common).display()
        )
    })?;
    let raw = match &top {
        Some(top) => top.file_name().map(|n| n.to_string_lossy().into_owned()),
        None => match common.file_name().map(|n| n.to_string_lossy().into_owned()) {
            Some(n) if n == ".git" => common
                .parent()
                .and_then(Path::file_name)
                .map(|n| n.to_string_lossy().into_owned()),
            Some(n) => Some(n.strip_suffix(".git").unwrap_or(&n).to_string()),
            None => None,
        },
    };
    Ok(CarriedRef {
        name: carried_name(&raw.unwrap_or_default()),
        repo_id: repo_id(&common),
        source: common,
        commit,
    })
}

/// `raw` as the name in `POTLUCK_MIRROR_<name>`: every character but ASCII letters, digits and
/// `_` made `_`; `repo` when empty.
pub fn carried_name(raw: &str) -> String {
    if raw.is_empty() {
        return "repo".to_string();
    }
    raw.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// The repositories a dispatch of `repo` carries along, one for each `--carry` ([`carried`]).
/// Refused: `repo` itself (its workspace holds its commit), and two that are one repository (a
/// job's mirror holds one pinned commit of each) or have one name.
pub fn carry_all(args: &[String], cwd: &Path, repo: &RepoRef) -> anyhow::Result<Vec<CarriedRef>> {
    let mut all: Vec<(&str, CarriedRef)> = Vec::new();
    for arg in args {
        let c = carried(arg, cwd)?;
        if c.repo_id == repo.id {
            bail!(
                "--carry {arg}: that is the job's own repository ({}), whose commit its workspace holds; --carry is for other repositories",
                c.source.display()
            );
        }
        if let Some((other, _)) = all.iter().find(|(_, o)| o.repo_id == c.repo_id) {
            bail!(
                "--carry {other} and --carry {arg} are one repository ({}): a dispatch carries one commit of each repository",
                c.source.display()
            );
        }
        if let Some((other, _)) = all.iter().find(|(_, o)| o.name == c.name) {
            bail!(
                "--carry {other} and --carry {arg} would both be POTLUCK_MIRROR_{}: carried repositories need directories with different names",
                c.name
            );
        }
        all.push((arg, c));
    }
    Ok(all.into_iter().map(|(_, c)| c).collect())
}

/// Whether `ancestor` is `commit` or an ancestor of it, in the repository whose (common) git dir is
/// `git_dir` (`git merge-base --is-ancestor`). The git dir is named with `GIT_DIR`, so a bare one
/// is read whatever `safe.bareRepository` says. `Err` when git can't tell: a commit it doesn't
/// have, or a git dir that isn't one.
pub(crate) fn is_ancestor(git_dir: &Path, ancestor: &str, commit: &str) -> anyhow::Result<bool> {
    let mut cmd = git(git_dir);
    cmd.env("GIT_DIR", git_dir);
    is_ancestor_with(cmd, ancestor, commit)
}

/// [`is_ancestor`] with `cmd`, a git command for the repository to ask (a mirror's, say).
pub(crate) fn is_ancestor_with(
    mut cmd: Command,
    ancestor: &str,
    commit: &str,
) -> anyhow::Result<bool> {
    for c in [ancestor, commit] {
        if c.is_empty() || !c.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("{c:?} is not a commit id");
        }
    }
    cmd.args(["merge-base", "--is-ancestor", ancestor, commit])
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let out = cmd
        .output()
        .with_context(|| format!("can't run {}", describe(&cmd)))?;
    match out.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => bail!(
            "{} failed ({}): {}",
            describe(&cmd),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ),
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
    /// Ancestry in a repository named by its git dir: a commit is its own ancestor, a child isn't
    /// its parent's, and a commit git doesn't have can't be told.
    #[test]
    fn ancestry() {
        let dir = tempfile::tempdir().unwrap();
        let run = |args: &[&str]| {
            let out = Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(["-c", "user.name=t", "-c", "user.email=t@example.invalid"])
                .args(["-c", "commit.gpgsign=false"])
                .args(args)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .output()
                .unwrap();
            assert!(out.status.success(), "{args:?}");
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        run(&["init", "-q"]);
        run(&["commit", "-q", "--allow-empty", "-m", "one"]);
        let one = run(&["rev-parse", "HEAD"]);
        run(&["commit", "-q", "--allow-empty", "-m", "two"]);
        let two = run(&["rev-parse", "HEAD"]);
        let git_dir = dir.path().join(".git");
        assert!(is_ancestor(&git_dir, &one, &two).unwrap());
        assert!(is_ancestor(&git_dir, &two, &two).unwrap());
        assert!(!is_ancestor(&git_dir, &two, &one).unwrap());
        assert!(is_ancestor(&git_dir, &"0".repeat(40), &two).is_err());
        assert!(is_ancestor(&git_dir, "--all", &two).is_err());
    }

    #[test]
    fn short_commits() {
        assert_eq!(short("0123456789abcdef"), "0123456789ab");
        assert_eq!(short("abc"), "abc");
        assert_eq!(short("aaaaaaaaaaa\u{e9}"), "aaaaaaaaaaa\u{e9}");
    }

    /// git in `dir` for a test, with no global or system config.
    fn test_git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args([
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "init.defaultBranch=main",
            ])
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// A repository at `dir` with two commits.
    fn two_commits(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        test_git(dir, &["init", "-q"]);
        for n in ["one", "two"] {
            std::fs::write(dir.join("f"), n).unwrap();
            test_git(dir, &["add", "f"]);
            test_git(dir, &["commit", "-qm", n]);
        }
    }

    fn err_of<T: std::fmt::Debug>(r: anyhow::Result<T>) -> String {
        format!("{:#}", r.unwrap_err())
    }

    /// `--carry DIR[@REV]`: a whole argument that is a directory has no REV; otherwise it splits
    /// at the last `@` with a directory before it.
    #[test]
    fn carry_arguments_split_into_a_directory_and_a_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let cwd = tmp.path();
        for d in ["dep", "v@2", "v@2@x"] {
            std::fs::create_dir_all(cwd.join(d)).unwrap();
        }
        let split = |a: &str| split_carry(a, cwd).unwrap();
        let at = |d: &str, r: &str| (cwd.join(d), r.to_string());
        assert_eq!(split("dep"), at("dep", "HEAD"));
        assert_eq!(split("dep/"), at("dep/", "HEAD"));
        assert_eq!(split("dep@b01aec5cbe"), at("dep", "b01aec5cbe"));
        assert_eq!(split("dep@HEAD@{1}"), at("dep", "HEAD@{1}"));
        assert_eq!(split("dep@main@{upstream}"), at("dep", "main@{upstream}"));
        // A directory whose name holds an @.
        assert_eq!(split("v@2"), at("v@2", "HEAD"));
        assert_eq!(split("v@2@x"), at("v@2@x", "HEAD"));
        assert_eq!(split("v@2@main"), at("v@2", "main"));
        assert_eq!(split("v@2@x@main"), at("v@2@x", "main"));
        // Absolute, and relative to the working directory.
        let abs = cwd.join("dep");
        assert_eq!(
            split_carry(&format!("{}@v1", abs.display()), Path::new("/")).unwrap(),
            (abs.clone(), "v1".to_string())
        );
        assert_eq!(
            split_carry("../dep@v1", &cwd.join("v@2")).unwrap(),
            (cwd.join("v@2/../dep"), "v1".to_string())
        );

        let e = err_of(split_carry("nope", cwd));
        assert!(
            e.contains("--carry nope: there is no directory") && e.contains("nope"),
            "{e}"
        );
        let e = err_of(split_carry("nope@abc", cwd));
        assert!(
            e.ends_with(&format!("{}", cwd.join("nope").display())),
            "{e}"
        );
        let e = err_of(split_carry("dep@", cwd));
        assert!(e.contains("names no revision after the @"), "{e}");
        let e = err_of(split_carry("@abc", cwd));
        assert!(e.contains("names no directory before the @"), "{e}");
        let e = err_of(split_carry("", cwd));
        assert!(e.contains("names no directory"), "{e}");
    }

    /// A carried repository: its commit at REV, its name from its directory, and the id a
    /// dispatch from it gets.
    #[test]
    fn carried_repositories_are_read_like_a_dispatch_from_them() {
        let tmp = tempfile::tempdir().unwrap();
        let dep = tmp.path().join("my-dep.rs");
        two_commits(&dep);
        let head = test_git(&dep, &["rev-parse", "HEAD"]);
        let first = test_git(&dep, &["rev-parse", "HEAD~1"]);
        let c = carried("my-dep.rs", tmp.path()).unwrap();
        assert_eq!(c.name, "my_dep_rs");
        assert_eq!(c.commit, head);
        assert_eq!(c.source, canonical(&dep.join(".git")).unwrap());
        // The id a dispatch from the repository itself sends.
        assert_eq!(c.repo_id, inspect(&dep).unwrap().id);
        assert_eq!(
            carried("my-dep.rs@HEAD~1", tmp.path()).unwrap().commit,
            first
        );
        // A subdirectory, and a full or short sha.
        std::fs::create_dir_all(dep.join("sub")).unwrap();
        let sub = carried(
            &format!("{}@{}", dep.join("sub").display(), &first[..10]),
            tmp.path(),
        )
        .unwrap();
        assert_eq!(
            (sub.commit.as_str(), sub.name.as_str()),
            (first.as_str(), "my_dep_rs")
        );
        assert_eq!(sub.repo_id, c.repo_id);

        // A commit on no branch: only committed work, but not only branches.
        test_git(&dep, &["checkout", "-q", "--detach"]);
        std::fs::write(dep.join("f"), "detached").unwrap();
        test_git(&dep, &["commit", "-qam", "detached"]);
        let detached = test_git(&dep, &["rev-parse", "HEAD"]);
        test_git(&dep, &["checkout", "-q", "main"]);
        let d = carried(&format!("my-dep.rs@{detached}"), tmp.path()).unwrap();
        assert_eq!(d.commit, detached);

        // A linked worktree: its own HEAD and directory name, the repository's id.
        let wt = tmp.path().join("dep-wt");
        test_git(
            &dep,
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                wt.to_str().unwrap(),
                &first,
            ],
        );
        let w = carried("dep-wt", tmp.path()).unwrap();
        assert_eq!(
            (w.commit.as_str(), w.name.as_str()),
            (first.as_str(), "dep_wt")
        );
        assert_eq!((&w.repo_id, &w.source), (&c.repo_id, &c.source));

        // A bare repository is named without its .git, and so is a .git directory by its
        // parent.
        test_git(
            tmp.path(),
            &["clone", "-q", "--bare", dep.to_str().unwrap(), "tools.git"],
        );
        let b = carried("tools.git", tmp.path()).unwrap();
        assert_eq!(
            (b.name.as_str(), b.commit.as_str()),
            ("tools", head.as_str())
        );
        assert!(b.repo_id.starts_with("tools-"), "{}", b.repo_id);
        let g = carried("my-dep.rs/.git", tmp.path()).unwrap();
        assert_eq!((g.name.as_str(), &g.repo_id), ("my_dep_rs", &c.repo_id));

        // Refused: not a repository, and a revision that names no commit.
        let plain = tmp.path().join("plain");
        std::fs::create_dir_all(&plain).unwrap();
        let e = err_of(carried("plain", tmp.path()));
        assert!(
            e.contains("--carry plain:") && e.contains("is not in a git repository"),
            "{e}"
        );
        let e = err_of(carried("my-dep.rs@nope", tmp.path()));
        assert!(
            e.contains("--carry my-dep.rs@nope: nope names no commit in"),
            "{e}"
        );
        let tree = test_git(&dep, &["rev-parse", "HEAD^{tree}"]);
        let e = err_of(carried(&format!("my-dep.rs@{tree}"), tmp.path()));
        assert!(e.contains("names no commit"), "{e}");
        let e = err_of(carried("my-dep.rs@--output=x", tmp.path()));
        assert!(e.contains("names no commit"), "{e}");
    }

    #[test]
    fn carried_names() {
        assert_eq!(carried_name("dep"), "dep");
        assert_eq!(carried_name("my-dep.rs"), "my_dep_rs");
        assert_eq!(carried_name("Dep_2"), "Dep_2");
        assert_eq!(carried_name("caf\u{e9}"), "caf_");
        assert_eq!(carried_name(""), "repo");
    }

    /// Refused: the job's own repository, one repository twice, and two with one name.
    #[test]
    fn a_dispatch_carries_other_repositories_once_each_under_distinct_names() {
        let tmp = tempfile::tempdir().unwrap();
        let app = tmp.path().join("app");
        let dep = tmp.path().join("dep");
        let other = tmp.path().join("other/dep");
        let util = tmp.path().join("util");
        for r in [&app, &dep, &other, &util] {
            two_commits(r);
        }
        let repo = inspect(&app).unwrap();
        let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let all = carry_all(&args(&["dep", "util@HEAD~1"]), tmp.path(), &repo).unwrap();
        assert_eq!(
            all.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["dep", "util"]
        );
        assert!(carry_all(&[], tmp.path(), &repo).unwrap().is_empty());

        let e = err_of(carry_all(&args(&["app"]), tmp.path(), &repo));
        assert!(
            e.contains("--carry app: that is the job's own repository"),
            "{e}"
        );
        // From a subdirectory of the job's own repository too.
        std::fs::create_dir_all(app.join("src")).unwrap();
        let e = err_of(carry_all(&args(&["src"]), &app, &repo));
        assert!(e.contains("the job's own repository"), "{e}");

        let e = err_of(carry_all(&args(&["dep", "dep@HEAD~1"]), tmp.path(), &repo));
        assert!(
            e.contains("--carry dep and --carry dep@HEAD~1 are one repository"),
            "{e}"
        );
        let e = err_of(carry_all(&args(&["dep", "other/dep"]), tmp.path(), &repo));
        assert!(
            e.contains("--carry dep and --carry other/dep would both be POTLUCK_MIRROR_dep"),
            "{e}"
        );
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
            carried: vec![CarriedRef {
                name: "dep".into(),
                repo_id: "dep-89abcdef".into(),
                source: PathBuf::from("/src/dep/.git"),
                commit: "e".repeat(40),
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
        // A carried repository gets the id a dispatch from it gets here.
        assert_eq!(
            a.carried[0].repo_id,
            crate::types::owned_repo_id("studio", "dep-89abcdef")
        );
        assert_eq!(
            a.carried[0].repo_id,
            owned_repo(
                "studio",
                &RepoRef {
                    id: "dep-89abcdef".into(),
                    submodules: Vec::new(),
                    carried: Vec::new(),
                    ..repo.clone()
                }
            )
            .id
        );
        assert_ne!(a.carried[0].repo_id, b.carried[0].repo_id);
        assert_eq!(
            (
                &a.carried[0].name,
                &a.carried[0].commit,
                &a.carried[0].source
            ),
            (
                &repo.carried[0].name,
                &repo.carried[0].commit,
                &repo.carried[0].source
            )
        );
        // Everything else as it was.
        assert_eq!(
            (a.commit, a.source, a.branch),
            (repo.commit, repo.source, repo.branch)
        );
        assert_eq!(a.submodules[0].commit, repo.submodules[0].commit);
    }
}
