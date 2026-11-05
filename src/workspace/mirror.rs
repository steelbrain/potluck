//! Mirrors: syncing, checking, recloning, pins. See the module docs in `mod.rs`.

use super::git::{mirror_git, mirror_text, tree_git};
use super::lock::Lock;
use super::{Workspaces, check_id, check_pin_name};
use crate::gitinfo::{output, short};
use anyhow::{Context, bail};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;

/// Left in a mirror when a seeding from it failed: the next sync runs a full `git fsck`.
const SUSPECT: &str = "potluck-suspect";

/// The branches and tags the last sync left in a mirror (`<sha> <refname>` lines), so the next
/// sync can tell whether something else changed them since. Removed while a sync runs, so a sync
/// cut short leaves none and the next one reports nothing it can't be sure of.
const SYNCED: &str = "potluck-synced";

/// What a sync that failed found had diverged (one line each), for the next sync that succeeds to
/// report: the failed fetch may have reset those refs already.
const PENDING: &str = "potluck-diverged";

/// The most diverged refs a report lists.
const MAX_DIVERGED: usize = 10;

/// Set in every mirror. Worktrees (the base and the workspaces) share it.
const MIRROR_CONFIG: &[(&str, &str)] = &[
    // Clones get new inodes and ctimes: compare only whole-second mtime and size.
    ("core.checkStat", "minimal"),
    ("core.trustctime", "false"),
    ("core.fsmonitor", "false"),
    ("core.untrackedCache", "false"),
    // A workspace gets a copy of the base's index file: a split index would point at a shared
    // index file in the base's own git dir.
    ("core.splitIndex", "false"),
    // No hooks from the user's global config (or anywhere) run in potluck's trees.
    ("core.hooksPath", "/dev/null"),
    ("core.logAllRefUpdates", "false"),
    // File names exactly as the commit has them. On by default on macOS, this makes git read names
    // back from the filesystem composed (NFC): a commit's decomposed (NFD) name, which APFS keeps
    // as written, would show as an untracked file and the tree as not clean.
    ("core.precomposeUnicode", "false"),
    // As many checkout workers as cores (base checkouts, large differences).
    ("checkout.workers", "0"),
    // gc runs inside the git command that triggers it, so nothing outlives a sync.
    ("gc.autoDetach", "false"),
    ("maintenance.autoDetach", "false"),
];

/// What a sync did, for the daemon to surface.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    /// This sync created the mirror.
    pub created: bool,
    /// The mirror couldn't be updated or was found corrupt, and was recloned from the source:
    /// what went wrong, with `git fsck`'s findings. Report it loudly (daemon log, `status`).
    pub recloned: Option<String>,
    /// potluck refs (`refs/potluck/...`, with their commits) the reclone couldn't bring back
    /// because the source lacks those commits too: workspaces at them can't be verified or seeded.
    pub lost_pins: Vec<String>,
    /// Branches and tags that changed in the mirror since the last sync, though only syncs write
    /// them: something else wrote to it, such as a job running git in a workspace (worktrees
    /// share the mirror's refs). The sync reset them to the source's; this says what they were
    /// ("refs/heads/main was at 0123abcd5678 here, 89abcdef0123 at the last sync").
    pub diverged: Vec<String>,
}

impl SyncReport {
    /// The warning to report for the mirror of `repo_id`, if any.
    pub fn warning(&self, repo_id: &str) -> Option<String> {
        let mut parts = Vec::new();
        if !self.diverged.is_empty() {
            let shown = self.diverged.len().min(MAX_DIVERGED);
            let mut s = format!(
                "the mirror of {repo_id} had changed since its last sync, so something other \
                 than potluck wrote to it (a job running git in its workspace, for one), and was \
                 reset to its source's branches and tags: {}",
                self.diverged[..shown].join("; ")
            );
            if self.diverged.len() > shown {
                s.push_str(&format!("; and {} more", self.diverged.len() - shown));
            }
            parts.push(s);
        }
        if let Some(why) = &self.recloned {
            let mut s = format!("the mirror of {repo_id} was recloned from its source: {why}");
            if !self.lost_pins.is_empty() {
                s.push_str(&format!(
                    "\nlost (the source lacks them too): {}",
                    self.lost_pins.join(", ")
                ));
            }
            parts.push(s);
        }
        (!parts.is_empty()).then(|| parts.join("\n"))
    }
}

impl Workspaces {
    /// Makes the mirror of `repo_id` an exact copy of `url`'s branches and tags (creating it on the
    /// first sync). Refs under `refs/potluck/` are kept. A mirror that git fsck finds damaged is
    /// recloned, and the report says why. `Err` when the source can't be read, the disk is full
    /// (less than 256 MiB free: no reclone is tried), the fetch fails twice though git fsck finds
    /// nothing wrong with the mirror (it is kept), or the reclone fails.
    pub fn sync_mirror(&self, repo_id: &str, url: &str) -> anyhow::Result<SyncReport> {
        check_id("repository", repo_id)?;
        check_url(url)?;
        let mirror = self.paths.mirror(repo_id);
        let lock = Lock::exclusive(&self.mirror_lock(repo_id))?;
        recover(&mirror);
        if !mirror.exists() {
            let tmp = sibling(&mirror, "new");
            make_mirror(url, &tmp, true)?;
            std::fs::rename(&tmp, &mirror)
                .with_context(|| format!("can't move {} into place", tmp.display()))?;
            record_synced(&mirror);
            return Ok(SyncReport {
                created: true,
                ..Default::default()
            });
        }
        let had_record = mirror.join(SYNCED).is_file();
        let diverged = diverged(&mirror);
        let _ = std::fs::remove_file(mirror.join(SYNCED));
        // A sync that fails keeps what had diverged for the next one to report, and takes the
        // refs as its fetch left them as potluck's own.
        let keep_divergence = || {
            if had_record {
                defer_divergence(&mirror, &diverged);
                record_synced(&mirror);
            }
        };
        let mut failed = None;
        let mut problem = match fetch(&mirror, url) {
            Ok(()) => cheap_check(&mirror)
                .err()
                .map(|e| format!("its check failed: {e:#}")),
            Err(e) => {
                // A source that can't be read is not the mirror's fault: nothing to reclone from.
                let mut ls = git_from(&self.paths.home, url);
                ls.args(["ls-remote", "--heads", url]);
                if let Err(src) = output(&mut ls) {
                    keep_divergence();
                    return Err(src.context(format!("can't read the source of {repo_id}")));
                }
                // Nor is a full disk: a reclone would need room for a second copy (fetched over
                // the network on a runner) and fail the same way.
                if let Some(full) = super::seed::disk_full(&mirror) {
                    keep_divergence();
                    return Err(e.context(format!(
                        "can't update the mirror of {repo_id}: {full}; free space there"
                    )));
                }
                // Nor, most often, anything else: a transfer cut short (a connection dropped
                // mid-pack) is worth a second try, and a git killed while it updated refs leaves
                // lock files that fail every fetch after it until they go.
                clear_stale_locks(&mirror);
                match fetch(&mirror, url) {
                    Ok(()) => cheap_check(&mirror)
                        .err()
                        .map(|e| format!("its check failed: {e:#}")),
                    Err(e) => {
                        let why = format!("it couldn't be updated: {e:#}");
                        failed = Some(e);
                        Some(why)
                    }
                }
            }
        };
        let suspect = std::fs::read_to_string(mirror.join(SUSPECT)).ok();
        if problem.is_none() && suspect.is_none() {
            record_synced(&mirror);
            return Ok(SyncReport {
                diverged: reported(&mirror, diverged),
                ..Default::default()
            });
        }
        // Only damage that git fsck finds is recloned for: a reclone of a sound mirror fetches
        // everything again (over the network on a runner) and would fail where the fetch did.
        match (&mut problem, full_fsck(&mirror)) {
            (_, Fsck::CantRun(e)) => {
                keep_divergence();
                bail!("can't check the mirror of {repo_id}: {e}");
            }
            (None, Fsck::Clean) => {
                let _ = std::fs::remove_file(mirror.join(SUSPECT));
                record_synced(&mirror);
                return Ok(SyncReport {
                    diverged: reported(&mirror, diverged),
                    ..Default::default()
                });
            }
            (Some(_), Fsck::Clean) => {
                let Some(e) = failed else {
                    // The cheap check failed where git fsck finds nothing: it is sound.
                    let _ = std::fs::remove_file(mirror.join(SUSPECT));
                    record_synced(&mirror);
                    return Ok(SyncReport {
                        diverged: reported(&mirror, diverged),
                        ..Default::default()
                    });
                };
                let _ = std::fs::remove_file(mirror.join(SUSPECT));
                keep_divergence();
                return Err(e.context(format!(
                    "can't update the mirror of {repo_id} (git fsck --full finds nothing wrong \
                     with it, so it is kept; the next sync tries again)"
                )));
            }
            (None, Fsck::Found(found)) => {
                problem = Some(format!(
                    "a seeding from it failed ({}) and git fsck --full found:\n{found}",
                    suspect.unwrap_or_default().trim()
                ))
            }
            (Some(p), Fsck::Found(found)) => {
                p.push_str(&format!("\ngit fsck --full found:\n{found}"))
            }
        }
        // Taken first: the old mirror, with what failed syncs kept, goes with the reclone.
        let diverged = reported(&mirror, diverged);
        let (lost_pins, old) = match reclone(&mirror, url) {
            Ok(done) => done,
            Err(e) => {
                if had_record {
                    defer_divergence(&mirror, &diverged);
                    record_synced(&mirror);
                }
                return Err(e);
            }
        };
        record_synced(&mirror);
        // The old mirror is deleted after the lock is let go: every seeding and dispatch of the
        // repository waits for it. (A daemon stopped first leaves it to the next sync.)
        drop(lock);
        let _ = remove_all(&old);
        Ok(SyncReport {
            created: false,
            recloned: problem,
            lost_pins,
            diverged,
        })
    }

    /// Whether the mirror of `repo_id` has `commit`.
    pub fn has_commit(&self, repo_id: &str, commit: &str) -> bool {
        check_id("repository", repo_id).is_ok()
            && is_sha(commit)
            && mirror_has(&self.paths.mirror(repo_id), commit)
    }

    /// Pins `commit` in the mirror under `refs/potluck/pins/<name>`. The mirror must have it.
    pub fn pin(&self, repo_id: &str, commit: &str, name: &str) -> anyhow::Result<()> {
        check_id("repository", repo_id)?;
        check_pin_name(name)?;
        check_sha(commit)?;
        let mirror = self.paths.mirror(repo_id);
        let _lock = Lock::shared(&self.mirror_lock(repo_id))?;
        if !self.has_commit(repo_id, commit) {
            bail!(
                "the mirror of {repo_id} doesn't have commit {}",
                short(commit)
            );
        }
        mirror_text(
            &mirror,
            ["update-ref", &format!("refs/potluck/pins/{name}"), commit],
        )?;
        Ok(())
    }

    /// Pins `commit` like [`Workspaces::pin`], first fetching it from `url` when the mirror lacks
    /// it: a commit on no branch or tag (a detached HEAD's) never comes with a sync. The mirror must
    /// exist (sync it first).
    pub fn fetch_pin(
        &self,
        repo_id: &str,
        url: &str,
        commit: &str,
        name: &str,
    ) -> anyhow::Result<()> {
        check_id("repository", repo_id)?;
        check_pin_name(name)?;
        check_sha(commit)?;
        check_url(url)?;
        let mirror = self.paths.mirror(repo_id);
        if !mirror.exists() {
            bail!("there is no mirror of {repo_id} yet");
        }
        let _lock = Lock::exclusive(&self.mirror_lock(repo_id))?;
        let refname = format!("refs/potluck/pins/{name}");
        if mirror_has(&mirror, commit) {
            mirror_text(&mirror, ["update-ref", &refname, commit])?;
        } else {
            fetch_sha(&mirror, url, commit, &refname)
                .with_context(|| format!("can't fetch {} from {url}", short(commit)))?;
        }
        Ok(())
    }

    /// Every pin in every mirror, as (repository id, pin name): for a sweep of pins that no job or
    /// workspace holds any more.
    pub fn pins(&self) -> anyhow::Result<Vec<(String, String)>> {
        let dir = self.paths.home.join("mirrors");
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e).with_context(|| format!("can't list {}", dir.display())),
        };
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.with_context(|| format!("can't list {}", dir.display()))?;
            let name = entry.file_name();
            let Some(repo_id) = name.to_str().and_then(|n| n.strip_suffix(".git")) else {
                continue;
            };
            if check_id("repository", repo_id).is_err() || !entry.path().is_dir() {
                continue;
            }
            let mirror = self.paths.mirror(repo_id);
            let _lock = Lock::shared(&self.mirror_lock(repo_id))?;
            let refs = mirror_text(
                &mirror,
                ["for-each-ref", "--format=%(refname)", "refs/potluck/pins/"],
            )?;
            for r in refs.lines() {
                if let Some(pin) = r.strip_prefix("refs/potluck/pins/") {
                    out.push((repo_id.to_string(), pin.to_string()));
                }
            }
        }
        out.sort();
        Ok(out)
    }

    pub fn unpin(&self, repo_id: &str, name: &str) -> anyhow::Result<()> {
        check_id("repository", repo_id)?;
        check_pin_name(name)?;
        let mirror = self.paths.mirror(repo_id);
        if !mirror.exists() {
            return Ok(());
        }
        let _lock = Lock::shared(&self.mirror_lock(repo_id))?;
        mirror_text(
            &mirror,
            ["update-ref", "-d", &format!("refs/potluck/pins/{name}")],
        )?;
        Ok(())
    }

    /// Removes the mirrors and base checkouts whose repository ids are from before ids were salted
    /// with the owner ([`crate::types::is_unsalted_repo_id`]): an older version left them, and only
    /// the workspaces and jobs made then use them. The caller makes sure none of those is left. A
    /// mirror or base in use right now (a sync, a seeding) is left for the next call. Their lock
    /// files stay (a lock file removed while another process waits on it would let two holders
    /// in). Returns the ids removed.
    pub fn remove_unsalted(&self) -> Vec<String> {
        let mut ids: std::collections::BTreeSet<String> =
            self.mirrors().into_iter().map(|(id, _)| id).collect();
        if let Ok(rd) = std::fs::read_dir(self.paths.home.join("bases")) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if e.file_type().is_ok_and(|t| t.is_dir()) && check_id("repository", &name).is_ok()
                {
                    ids.insert(name);
                }
            }
        }
        let mut removed = Vec::new();
        for id in ids
            .into_iter()
            .filter(|id| crate::types::is_unsalted_repo_id(id))
        {
            let Ok(Some(_mirror)) = Lock::try_exclusive(&self.mirror_lock(&id)) else {
                continue;
            };
            let Ok(Some(_base)) = Lock::try_exclusive(&self.base_lock(&id)) else {
                continue;
            };
            // The base is a worktree of the mirror: both go, so git needn't be told.
            if remove_all(&self.paths.base(&id)).is_ok()
                && remove_all(&self.paths.mirror(&id)).is_ok()
            {
                removed.push(id);
            }
        }
        removed
    }

    /// Every mirror on this machine: (repository id, path).
    pub(super) fn mirrors(&self) -> Vec<(String, PathBuf)> {
        let Ok(dir) = std::fs::read_dir(self.paths.home.join("mirrors")) else {
            return Vec::new();
        };
        let mut out: Vec<(String, PathBuf)> = dir
            .flatten()
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                let id = name.strip_suffix(".git")?.to_string();
                check_id("repository", &id).ok()?;
                Some((id, e.path()))
            })
            .collect();
        out.sort();
        out
    }
}

/// Marks a mirror for a full `git fsck` at its next sync.
pub(super) fn mark_suspect(mirror: &Path, why: &str) {
    if mirror.is_dir() {
        let _ = std::fs::write(mirror.join(SUSPECT), why);
    }
}

pub(super) fn mirror_has(mirror: &Path, commit: &str) -> bool {
    mirror.is_dir()
        && mirror_git(mirror)
            .args(["cat-file", "-e", &format!("{commit}^{{commit}}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
}

pub(super) fn is_sha(s: &str) -> bool {
    (s.len() == 40 || s.len() == 64) && s.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(super) fn check_sha(s: &str) -> anyhow::Result<()> {
    if !is_sha(s) {
        bail!("bad commit {s:?}: a full sha is needed");
    }
    Ok(())
}

fn check_url(url: &str) -> anyhow::Result<()> {
    if url.is_empty() || url.starts_with('-') {
        bail!("bad source URL {url:?}");
    }
    Ok(())
}

/// `git -C dir`, for a command that reads from `url` and needs no repository (clone, ls-remote):
/// a tunnel URL (git over the mesh) needs git's ext:: transport allowed, for that command only.
fn git_from(dir: &Path, url: &str) -> std::process::Command {
    let mut cmd = tree_git(dir);
    cmd.args(crate::tunnel::git_args(url));
    cmd
}

/// [`git_from`] for a command in the mirror at `mirror`.
fn mirror_from(mirror: &Path, url: &str) -> std::process::Command {
    let mut cmd = mirror_git(mirror);
    cmd.args(crate::tunnel::git_args(url));
    cmd
}

/// `<mirror>.<suffix>`, beside the mirror.
fn sibling(mirror: &Path, suffix: &str) -> PathBuf {
    let mut s = mirror.as_os_str().to_owned();
    s.push(format!(".{suffix}"));
    PathBuf::from(s)
}

/// Clones `url` into a new bare mirror at `dst` and configures it. `local` lets git hardlink the
/// objects of a source on this machine; that copy walks the source's object directory, which a
/// `git gc` running there at the same time can pull files out of, so a failed local clone is
/// retried through the transport (`--no-local`, a consistent snapshot).
fn make_mirror(url: &str, dst: &Path, local: bool) -> anyhow::Result<()> {
    let parent = dst
        .parent()
        .ok_or_else(|| anyhow::anyhow!("bad mirror path {}", dst.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("can't create {}", parent.display()))?;
    let clone = |local: bool| -> anyhow::Result<()> {
        remove_all(dst)?;
        let mut cmd = git_from(parent, url);
        cmd.args(["clone", "--bare", "--quiet"]);
        if !local {
            cmd.arg("--no-local");
        }
        cmd.arg("--").arg(url).arg(dst);
        let result = output(&mut cmd).map(|_| ()).and_then(|()| configure(dst));
        if result.is_err() {
            let _ = remove_all(dst);
        }
        result
    };
    match clone(local) {
        Err(e) if local => clone(false).map_err(|e2| e2.context(format!("{e:#}"))),
        result => result,
    }
}

fn configure(mirror: &Path) -> anyhow::Result<()> {
    for (k, v) in MIRROR_CONFIG {
        mirror_text(mirror, ["config", k, v])?;
    }
    // Syncs name their source each time; a stale origin would only mislead.
    let _ = mirror_text(mirror, ["config", "--remove-section", "remote.origin"]);
    let info = mirror.join("info");
    std::fs::create_dir_all(&info)?;
    std::fs::write(
        info.join("exclude"),
        "# potluck's files in a workspace (logs, status) are never part of the tree\n/.potluck/\n",
    )?;
    Ok(())
}

/// Fetches `url`'s branches and tags into the mirror, force-updating and deleting to match.
fn fetch(mirror: &Path, url: &str) -> anyhow::Result<()> {
    let mut cmd = mirror_from(mirror, url);
    cmd.args([
        "fetch",
        "--quiet",
        "--prune",
        "--no-tags",
        "--update-head-ok",
        "--no-write-fetch-head",
        "--no-recurse-submodules",
        url,
        "+refs/heads/*:refs/heads/*",
        "+refs/tags/*:refs/tags/*",
    ]);
    output(&mut cmd)?;
    Ok(())
}

/// The mirror's branches and tags: refname -> sha. `None` if git can't list them.
fn branches_and_tags(mirror: &Path) -> Option<BTreeMap<String, String>> {
    let out = mirror_text(
        mirror,
        [
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            "refs/heads/",
            "refs/tags/",
        ],
    )
    .ok()?;
    Some(
        out.lines()
            .filter_map(|l| l.split_once(' '))
            .map(|(sha, name)| (name.to_string(), sha.to_string()))
            .collect(),
    )
}

/// Records the mirror's branches and tags as this sync leaves them ([`SYNCED`]).
fn record_synced(mirror: &Path) {
    let Some(refs) = branches_and_tags(mirror) else {
        return;
    };
    let text: String = refs
        .iter()
        .map(|(name, sha)| format!("{sha} {name}\n"))
        .collect();
    let tmp = mirror.join(format!("{SYNCED}.new"));
    if std::fs::write(&tmp, text).is_ok() {
        let _ = std::fs::rename(&tmp, mirror.join(SYNCED));
    }
}

/// How the mirror's branches and tags differ from what the last sync left ([`SYNCED`]); empty
/// when there is no record (a mirror from before records, or a sync cut short).
fn diverged(mirror: &Path) -> Vec<String> {
    let Ok(text) = std::fs::read_to_string(mirror.join(SYNCED)) else {
        return Vec::new();
    };
    let Some(now) = branches_and_tags(mirror) else {
        return Vec::new();
    };
    let was: BTreeMap<String, String> = text
        .lines()
        .filter_map(|l| l.split_once(' '))
        .map(|(sha, name)| (name.to_string(), sha.to_string()))
        .collect();
    let names: std::collections::BTreeSet<&String> = was.keys().chain(now.keys()).collect();
    let mut out = Vec::new();
    for name in names {
        match (was.get(name), now.get(name)) {
            (Some(w), Some(n)) if w != n => out.push(format!(
                "{name} was at {} here, {} at the last sync",
                short(n),
                short(w)
            )),
            (None, Some(n)) => out.push(format!("{name} was made here, at {}", short(n))),
            (Some(w), None) => {
                out.push(format!("{name} was deleted here (it was at {})", short(w)))
            }
            _ => {}
        }
    }
    out
}

/// Keeps `diverged` ([`PENDING`]) for the next sync that succeeds to report.
fn defer_divergence(mirror: &Path, diverged: &[String]) {
    if diverged.is_empty() {
        return;
    }
    let mut all = pending(mirror);
    all.extend(diverged.iter().cloned());
    let text: String = all.iter().map(|l| format!("{l}\n")).collect();
    let tmp = mirror.join(format!("{PENDING}.new"));
    if std::fs::write(&tmp, text).is_ok() {
        let _ = std::fs::rename(&tmp, mirror.join(PENDING));
    }
}

/// What failed syncs kept ([`PENDING`]), oldest first, without repeats.
fn pending(mirror: &Path) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in std::fs::read_to_string(mirror.join(PENDING))
        .unwrap_or_default()
        .lines()
    {
        if !line.is_empty() && !out.iter().any(|l| l == line) {
            out.push(line.to_string());
        }
    }
    out
}

/// The divergence a sync that succeeded reports: what failed syncs before it kept, then its own.
fn reported(mirror: &Path, diverged: Vec<String>) -> Vec<String> {
    let mut all = pending(mirror);
    let _ = std::fs::remove_file(mirror.join(PENDING));
    for d in diverged {
        if !all.contains(&d) {
            all.push(d);
        }
    }
    all
}

/// Fetches one commit by its sha into `refname`.
fn fetch_sha(mirror: &Path, url: &str, sha: &str, refname: &str) -> anyhow::Result<()> {
    let mut cmd = mirror_from(mirror, url);
    cmd.args([
        "fetch",
        "--quiet",
        "--no-tags",
        "--no-write-fetch-head",
        "--no-recurse-submodules",
        url,
        &format!("+{sha}:{refname}"),
    ]);
    output(&mut cmd)?;
    if !mirror_has(mirror, sha) {
        bail!("{url} sent no commit {}", short(sha));
    }
    Ok(())
}

/// The per-sync check: every ref's object, and for branches and potluck refs the commit's root
/// tree, read back in full (`git cat-file --batch`).
fn cheap_check(mirror: &Path) -> anyhow::Result<()> {
    let refs = mirror_text(
        mirror,
        ["for-each-ref", "--format=%(objectname) %(refname)"],
    )?;
    let mut input = String::new();
    for line in refs.lines() {
        let Some((sha, name)) = line.split_once(' ') else {
            continue;
        };
        input.push_str(sha);
        input.push('\n');
        if name.starts_with("refs/heads/") || name.starts_with("refs/potluck/") {
            input.push_str(&format!("{sha}^{{tree}}\n"));
        }
    }
    let mut child = mirror_git(mirror)
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("can't run git cat-file")?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(input.as_bytes());
    });
    let mut out = Vec::new();
    child
        .stdout
        .take()
        .expect("piped stdout")
        .read_to_end(&mut out)?;
    let mut err = String::new();
    child
        .stderr
        .take()
        .expect("piped stderr")
        .read_to_string(&mut err)?;
    let _ = writer.join();
    let status = child.wait()?;
    if !status.success() {
        bail!("git cat-file --batch: {}", err.trim());
    }
    let mut problems = Vec::new();
    let mut i = 0;
    while i < out.len() {
        let Some(nl) = out[i..].iter().position(|b| *b == b'\n') else {
            break;
        };
        let header = String::from_utf8_lossy(&out[i..i + nl]).into_owned();
        i += nl + 1;
        let parts: Vec<&str> = header.split(' ').collect();
        match parts.as_slice() {
            [_, _, size] if size.parse::<usize>().is_ok() => {
                i += size.parse::<usize>().unwrap_or(0) + 1;
            }
            _ => problems.push(header),
        }
    }
    if !problems.is_empty() {
        bail!("unreadable refs: {}", problems.join("; "));
    }
    Ok(())
}

/// What [`full_fsck`] says of a mirror.
enum Fsck {
    Clean,
    /// Its findings (the first lines).
    Found(String),
    /// git fsck couldn't be run (a fault of this machine's, not the mirror's).
    CantRun(String),
}

/// `git fsck --full`.
fn full_fsck(mirror: &Path) -> Fsck {
    let out = mirror_git(mirror)
        .args(["fsck", "--full", "--no-dangling", "--no-progress"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();
    match out {
        Ok(o) if o.status.success() => Fsck::Clean,
        Ok(o) => {
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            );
            let lines: Vec<&str> = text.lines().collect();
            let mut s = lines[..lines.len().min(20)].join("\n");
            if lines.len() > 20 {
                s.push_str(&format!("\n... and {} more lines", lines.len() - 20));
            }
            Fsck::Found(s)
        }
        Err(e) => Fsck::CantRun(format!("can't run git fsck: {e}")),
    }
}

/// A lock file older than this in a mirror was left by a git that died (killed outright, or its
/// machine lost power): git holds a ref lock for a moment and gives up on a held one within a
/// second, and potluck syncs under the mirror's own lock.
const STALE_LOCK: std::time::Duration = std::time::Duration::from_secs(60);

/// Removes the lock files that can fail a fetch (`HEAD.lock`, `packed-refs.lock`, `refs/**/*.lock`)
/// once they are older than [`STALE_LOCK`]; a younger one may be held by a job's git in a
/// workspace (worktrees share the mirror's refs).
fn clear_stale_locks(mirror: &Path) {
    let stale = |p: &Path| {
        p.symlink_metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > STALE_LOCK)
    };
    let mut locks = vec![mirror.join("HEAD.lock"), mirror.join("packed-refs.lock")];
    let mut dirs = vec![mirror.join("refs")];
    while let Some(dir) = dirs.pop() {
        for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let p = e.path();
            if e.file_type().is_ok_and(|t| t.is_dir()) {
                dirs.push(p);
            } else if p.extension().is_some_and(|x| x == "lock") {
                locks.push(p);
            }
        }
    }
    for lock in locks {
        if stale(&lock) {
            let _ = std::fs::remove_file(&lock);
        }
    }
}

/// potluck's refs in a mirror that may be damaged: through git, else from the files.
fn potluck_refs(mirror: &Path) -> Vec<(String, String)> {
    if let Ok(out) = mirror_text(
        mirror,
        [
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            "refs/potluck/",
        ],
    ) {
        return out
            .lines()
            .filter_map(|l| l.split_once(' '))
            .map(|(sha, name)| (name.to_string(), sha.to_string()))
            .collect();
    }
    let mut refs = Vec::new();
    if let Ok(packed) = std::fs::read_to_string(mirror.join("packed-refs")) {
        for line in packed.lines() {
            if let Some((sha, name)) = line.split_once(' ')
                && name.starts_with("refs/potluck/")
                && is_sha(sha)
            {
                refs.push((name.to_string(), sha.to_string()));
            }
        }
    }
    fn walk(dir: &Path, name: &str, refs: &mut Vec<(String, String)>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let child = format!("{name}/{}", e.file_name().to_string_lossy());
            if e.path().is_dir() {
                walk(&e.path(), &child, refs);
            } else if let Ok(sha) = std::fs::read_to_string(e.path()) {
                let sha = sha.trim();
                if is_sha(sha) {
                    refs.retain(|(n, _)| *n != child);
                    refs.push((child, sha.to_string()));
                }
            }
        }
    }
    walk(&mirror.join("refs/potluck"), "refs/potluck", &mut refs);
    refs
}

/// Finishes or undoes a [`reclone`] that was cut short (the daemon stopped), under the mirror's
/// exclusive lock: a complete new clone found where the mirror should be, with the old one moved
/// aside, is put in place; worktrees already moved into a new clone go back to the mirror. Every
/// old mirror moved aside is removed, and so is any other new clone once the mirror is there (a
/// first clone cut short is replaced when the mirror is made).
fn recover(mirror: &Path) {
    let new = sibling(mirror, "new");
    let Some(name) = mirror.file_name().map(|n| n.to_string_lossy().into_owned()) else {
        return;
    };
    let prefix = format!("{name}.broken-");
    let aside: Vec<PathBuf> = mirror
        .parent()
        .and_then(|dir| std::fs::read_dir(dir).ok())
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
        .map(|e| e.path())
        .collect();
    // `info/exclude` is the last file a new clone gets: it was made in full.
    if !mirror.exists() && !aside.is_empty() && new.join("info/exclude").is_file() {
        let _ = std::fs::rename(&new, mirror);
    }
    let moved = new.join("worktrees");
    if mirror.is_dir() && moved.is_dir() {
        let worktrees = mirror.join("worktrees");
        let _ = std::fs::create_dir_all(&worktrees);
        for e in std::fs::read_dir(&moved).into_iter().flatten().flatten() {
            let to = worktrees.join(e.file_name());
            if to.symlink_metadata().is_err() {
                let _ = std::fs::rename(e.path(), to);
            }
        }
    }
    // An old mirror moved aside is never needed again: the new clone took its pins and worktrees.
    if mirror.exists() {
        let _ = remove_all(&new);
    }
    for p in aside {
        let _ = remove_all(&p);
    }
}

/// Replaces the mirror with a fresh clone of `url` (no hardlinks: a damaged shared file would
/// come back), keeping its worktrees and potluck refs. Returns the refs whose commits are lost,
/// and where the old mirror was moved aside, for the caller to delete. A reclone cut short is
/// finished or undone by [`recover`] at the next sync.
fn reclone(mirror: &Path, url: &str) -> anyhow::Result<(Vec<String>, PathBuf)> {
    let new = sibling(mirror, "new");
    make_mirror(url, &new, false)?;
    let mut lost = Vec::new();
    for (name, sha) in potluck_refs(mirror) {
        let kept = if mirror_has(&new, &sha) {
            mirror_text(&new, ["update-ref", &name, &sha]).is_ok()
        } else {
            fetch_sha(&new, url, &sha, &name).is_ok()
        };
        if !kept {
            lost.push(format!("{name} ({})", short(&sha)));
        }
    }
    let worktrees = mirror.join("worktrees");
    if worktrees.is_dir() {
        std::fs::rename(&worktrees, new.join("worktrees"))
            .context("can't move the mirror's worktrees to its new clone")?;
    }
    let old = sibling(mirror, &format!("broken-{}", crate::types::now_ms()));
    std::fs::rename(mirror, &old)
        .with_context(|| format!("can't move {} aside", mirror.display()))?;
    std::fs::rename(&new, mirror)
        .with_context(|| format!("can't move {} into place", new.display()))?;
    Ok((lost, old))
}

/// Removes a file or tree, making read-only directories writable when that's in the way. Gone
/// already is fine.
pub(super) fn remove_all(path: &Path) -> anyhow::Result<()> {
    let meta = match path.symlink_metadata() {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    if !meta.is_dir() {
        return std::fs::remove_file(path)
            .with_context(|| format!("can't remove {}", path.display()));
    }
    if std::fs::remove_dir_all(path).is_ok() || !path.exists() {
        return Ok(());
    }
    make_dirs_writable(path);
    std::fs::remove_dir_all(path).with_context(|| format!("can't remove {}", path.display()))
}

fn make_dirs_writable(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = dir.symlink_metadata()
        && meta.is_dir()
    {
        let mode = meta.permissions().mode();
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode | 0o700));
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    make_dirs_writable(&e.path());
                }
            }
        }
    }
}
