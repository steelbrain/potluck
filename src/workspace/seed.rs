//! Seeding, verifying, removing and measuring workspaces, and the base checkouts they are cloned
//! from. See the module docs in `mod.rs`.

use super::git::{mirror_bytes, mirror_git, mirror_text, tree_bytes, tree_git, tree_text};
use super::lock::Lock;
use super::mirror::{check_sha, mark_suspect, remove_all};
use super::{Workspaces, case, check_id};
use crate::cow;
use crate::gitinfo::{self, canonical, short};
use crate::types::{RepoRef, WorkspaceId, WorkspaceRecord};
use anyhow::{Context, bail};
use std::collections::{BTreeSet, HashSet};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

/// One repository of a workspace: the top or a submodule.
struct Part<'a> {
    repo_id: &'a str,
    commit: &'a str,
    dir: PathBuf,
    /// "" for the top, else the submodule's path.
    path: &'a str,
}

impl Part<'_> {
    fn label(&self) -> String {
        if self.path.is_empty() {
            "the workspace".to_string()
        } else {
            format!("submodule {}", self.path)
        }
    }
}

/// The top first, then submodules, parents before the submodules nested in them.
fn parts<'a>(repo: &'a RepoRef, root: &Path) -> anyhow::Result<Vec<Part<'a>>> {
    let mut parts = vec![Part {
        repo_id: &repo.id,
        commit: &repo.commit,
        dir: root.to_path_buf(),
        path: "",
    }];
    let mut subs: Vec<_> = repo.submodules.iter().collect();
    subs.sort_by_key(|s| Path::new(&s.path).components().count());
    for s in subs {
        let p = Path::new(&s.path);
        if s.path.is_empty() || !p.components().all(|c| matches!(c, Component::Normal(_))) {
            bail!("bad submodule path {:?}", s.path);
        }
        parts.push(Part {
            repo_id: &s.repo_id,
            commit: &s.commit,
            dir: root.join(p),
            path: &s.path,
        });
    }
    Ok(parts)
}

impl Workspaces {
    /// Creates workspace `id` at `repo`'s commit (and its submodules) and its TMPDIR, and checks the
    /// new tree is clean and at those commits. Every mirror must already have its commit. Pins each
    /// commit under `refs/potluck/pins/<id>` until [`Workspaces::remove`]. Returns its root. On
    /// failure nothing is left behind.
    pub fn create(&self, id: &WorkspaceId, repo: &RepoRef) -> anyhow::Result<PathBuf> {
        check_id("workspace", id)?;
        let root = self.paths.workspace(id);
        let tmp = self.paths.workspace_tmp(id);
        for p in [&root, &tmp] {
            if p.symlink_metadata().is_ok() {
                bail!("workspace {id} already exists ({})", p.display());
            }
        }
        let parts = parts(repo, &root)?;
        for p in &parts {
            check_id("repository", p.repo_id)?;
            check_sha(p.commit)?;
            if !self.has_commit(p.repo_id, p.commit) {
                bail!(
                    "the mirror of {} doesn't have commit {} ({})",
                    p.repo_id,
                    short(p.commit),
                    p.label()
                );
            }
        }
        let dir = root.parent().expect("workspaces have a parent");
        std::fs::create_dir_all(dir).with_context(|| format!("can't create {}", dir.display()))?;
        match self.create_parts(id, repo, &root, &tmp, &parts) {
            Ok(()) => Ok(root),
            Err(e) => {
                let related: Vec<String> = parts.iter().map(|p| p.repo_id.to_string()).collect();
                let _ = self.remove_paths(id, &root, &tmp, &related);
                Err(e.context(format!(
                    "can't create workspace {id} at {}",
                    short(&repo.commit)
                )))
            }
        }
    }

    fn create_parts(
        &self,
        id: &str,
        repo: &RepoRef,
        root: &Path,
        tmp: &Path,
        parts: &[Part],
    ) -> anyhow::Result<()> {
        for p in parts {
            self.pin(p.repo_id, p.commit, id)?;
        }
        for p in parts {
            if let Err(e) = self.seed(p.repo_id, p.commit, &p.dir) {
                // A full disk is not the base's fault, nor the mirror's (a reclone would need
                // room for a second copy of it).
                let full = e.downcast_ref::<DiskFull>().is_some();
                let e = e.context(format!("seeding {}", p.label()));
                if full {
                    return Err(e);
                }
                if let Some(full) = disk_full(root) {
                    return Err(e.context(full));
                }
                // Not the base's fault, nor the mirror's: the base never moves to such a commit,
                // and one made at it was removed half made.
                if let Some(refused) = root.parent().and_then(|d| self.refused_names(p, d)) {
                    return Err(e.context(refused));
                }
                let why = format!("{e:#}");
                self.discard_base(p.repo_id);
                mark_suspect(&self.paths.mirror(p.repo_id), &why);
                return Err(e);
            }
        }
        std::fs::create_dir_all(tmp).with_context(|| format!("can't create {}", tmp.display()))?;
        // Set when the commit itself can't be checked out here (paths that differ only in case or
        // normalisation).
        let mut colliding: Option<Vec<(&str, &str)>> = None;
        let checked = self.verify(root, repo).and_then(|()| {
            let mut dirty = Vec::new();
            let mut dirty_parts = Vec::new();
            for p in parts {
                let prefix = if p.path.is_empty() {
                    String::new()
                } else {
                    format!("{}/", p.path)
                };
                let lines = status(&p.dir)?;
                // Files git calls modified only because its own filters would store the commit's
                // blob differently hold what any checkout of the commit writes: not damage.
                let lines = if lines.is_empty() {
                    lines
                } else {
                    super::filters::unexplained(&p.dir, lines.clone(), false).unwrap_or(lines)
                };
                for line in &lines {
                    dirty.push(format!("{} {prefix}{}", &line[..2], &line[3..]));
                }
                if !lines.is_empty() {
                    dirty_parts.push((p, lines.iter().map(|l| l[3..].to_string()).collect()));
                }
            }
            if dirty.is_empty() {
                return Ok(());
            }
            if let Some((msg, parts)) = self.case_collisions(&dirty_parts) {
                colliding = Some(parts);
                bail!(msg);
            }
            let shown = dirty.len().min(20);
            bail!(
                "the new tree isn't clean:\n  {}{}",
                dirty[..shown].join("\n  "),
                if dirty.len() > shown {
                    format!("\n  ... and {} more", dirty.len() - shown)
                } else {
                    String::new()
                }
            );
        });
        if let Err(e) = checked {
            if let Some(parts) = colliding {
                // Not the base's fault, nor the mirror's. Only a base made at the colliding commit
                // (the cold path: a base never moves to such a commit) holds a tree it can't.
                for (repo_id, commit) in parts {
                    self.discard_base_at(repo_id, commit);
                }
                return Err(e);
            }
            if let Some(full) = disk_full(root) {
                return Err(e.context(full));
            }
            // The bases it came from are suspect: start the next seeding from fresh ones.
            let why = format!("{e:#}");
            let mut seen = HashSet::new();
            for p in parts {
                if seen.insert(p.repo_id) {
                    self.discard_base(p.repo_id);
                    mark_suspect(&self.paths.mirror(p.repo_id), &why);
                }
            }
            return Err(e);
        }
        Ok(())
    }

    /// When every dirty path of every part (part, its dirty paths) is a case collision among that
    /// commit's own paths on a case-insensitive filesystem: the message, and the (repository,
    /// commit) of each part concerned.
    fn case_collisions<'a>(
        &self,
        dirty_parts: &[(&'a Part<'a>, Vec<String>)],
    ) -> Option<(String, Vec<(&'a str, &'a str)>)> {
        let probe = dirty_parts.first()?.0.dir.parent()?;
        if !case::case_insensitive(probe) {
            return None;
        }
        let mut msgs = Vec::new();
        let mut parts = Vec::new();
        for (p, dirty) in dirty_parts {
            let paths = commit_paths(&self.paths.mirror(p.repo_id), p.commit).ok()?;
            let groups = case::explain(dirty, &paths)?;
            let whose = if p.path.is_empty() {
                String::new()
            } else {
                format!("submodule {}: ", p.path)
            };
            msgs.push(format!(
                "{whose}the commit has paths that differ only in case or Unicode normalisation: \
                 {}; this filesystem can't hold both",
                case::describe(&groups)
            ));
            parts.push((p.repo_id, p.commit));
        }
        Some((msgs.join("\n"), parts))
    }

    /// When part `p`'s commit holds file names the filesystem at `dir` refuses: the message.
    fn refused_names(&self, p: &Part, dir: &Path) -> Option<String> {
        let paths = commit_paths_raw(&self.paths.mirror(p.repo_id), p.commit).ok()?;
        let refused = case::refused(dir, &paths);
        if refused.is_empty() {
            return None;
        }
        let whose = if p.path.is_empty() {
            String::new()
        } else {
            format!("submodule {}: ", p.path)
        };
        Some(format!(
            "{whose}the commit has file names this filesystem refuses: {}",
            case::describe_refused(&refused)
        ))
    }

    /// Seeds one repository's worktree at `dir` (missing or empty) at `commit`.
    fn seed(&self, repo_id: &str, commit: &str, dir: &Path) -> anyhow::Result<()> {
        let mirror = self.paths.mirror(repo_id);
        let base = self.paths.base(repo_id);
        let _mirror_lock = Lock::shared(&self.mirror_lock(repo_id))?;
        self.add_worktree(repo_id, dir, commit)?;
        let admin = PathBuf::from(tree_text(dir, ["rev-parse", "--absolute-git-dir"])?);

        let base_lock = Lock::shared(&self.base_lock(repo_id))?;
        let ready = match base_head(&base) {
            Some((b, admin)) if !should_advance(&mirror, &base, &b, commit)? => Some((b, admin)),
            _ => None,
        };
        let (b, base_admin) = match ready {
            Some(ready) => ready,
            None => {
                base_lock.convert(true)?;
                if let Err(e) = self.prepare_base(repo_id, commit) {
                    // Told before the half-made base goes and frees what it took.
                    let e = match disk_full(&base) {
                        Some(full) => e.context(full),
                        None => e,
                    };
                    // Half made or half moved: never cloned from.
                    self.remove_base(repo_id);
                    return Err(e);
                }
                base_lock.convert(false)?;
                // Someone may have moved it between the two conversions: read it again.
                base_head(&base).ok_or_else(|| {
                    anyhow::anyhow!("the base checkout {} went away", base.display())
                })?
            }
        };
        cow::clone_dir_contents(&base, dir, &[".git"])?;
        let index = std::fs::read(base_admin.join("index"))
            .with_context(|| format!("can't read the index of {}", base.display()))?;
        std::fs::write(admin.join("index"), index)
            .with_context(|| format!("can't write the index of {}", dir.display()))?;
        drop(base_lock);

        if b != commit {
            move_tree(dir, &b, commit)?;
        }
        Ok(())
    }

    /// Removes the base checkout of `repo_id`; the next seeding makes a new one.
    fn discard_base(&self, repo_id: &str) {
        let Ok(_mirror_lock) = Lock::shared(&self.mirror_lock(repo_id)) else {
            return;
        };
        if let Ok(_lock) = Lock::exclusive(&self.base_lock(repo_id)) {
            self.remove_base(repo_id);
        }
    }

    /// Removes the base checkout of `repo_id` if it is at `commit`.
    fn discard_base_at(&self, repo_id: &str, commit: &str) {
        let Ok(_mirror_lock) = Lock::shared(&self.mirror_lock(repo_id)) else {
            return;
        };
        if let Ok(_lock) = Lock::exclusive(&self.base_lock(repo_id))
            && base_head(&self.paths.base(repo_id)).is_some_and(|(b, _)| b == commit)
        {
            self.remove_base(repo_id);
        }
    }

    /// Checks workspace `id` at `root`, at commit `from` (its record's), out at `repo`'s commits
    /// (`dispatch --advance`). `repo`'s commit must be `from` or descend from it (in the mirror),
    /// and its submodules must be the ones the workspace has checked out; otherwise nothing is
    /// changed. Then the top and each submodule, parents first, are checked out detached at their
    /// commits with `git checkout --force`: tracked files become the commit's, changes to them are
    /// discarded, and untracked files in the way of the commit's files are removed; other
    /// untracked and ignored files (build output) stay. Files whose converting attributes differ
    /// under the two commits are written again (`switch_tree`). Last, the workspace's pins
    /// (`refs/potluck/pins/<id>`) move to the new commits, so the old ones are no longer pinned.
    /// Every mirror must already have its commit.
    pub fn advance(
        &self,
        id: &WorkspaceId,
        root: &Path,
        from: &str,
        repo: &RepoRef,
    ) -> anyhow::Result<()> {
        check_id("workspace", id)?;
        check_sha(from)?;
        let parts = parts(repo, root)?;
        for p in &parts {
            check_id("repository", p.repo_id)?;
            check_sha(p.commit)?;
            if !self.has_commit(p.repo_id, p.commit) {
                bail!(
                    "the mirror of {} doesn't have commit {} ({})",
                    p.repo_id,
                    short(p.commit),
                    p.label()
                );
            }
        }
        if from != repo.commit {
            let _lock = Lock::shared(&self.mirror_lock(&repo.id))?;
            let mirror = self.paths.mirror(&repo.id);
            if !gitinfo::is_ancestor_with(mirror_git(&mirror), from, &repo.commit)? {
                bail!(
                    "commit {} doesn't descend from {}, the workspace's commit",
                    short(&repo.commit),
                    short(from)
                );
            }
        }
        let have = self.checked_out_submodules(&repo.id, from, root)?;
        let want: BTreeSet<String> = repo.submodules.iter().map(|s| s.path.clone()).collect();
        if have != want {
            let list =
                |s: Vec<&String>| s.iter().map(|p| p.as_str()).collect::<Vec<_>>().join(", ");
            let mut why = Vec::new();
            let added: Vec<&String> = want.difference(&have).collect();
            if !added.is_empty() {
                why.push(format!(
                    "this dispatch has {}, which the workspace doesn't",
                    list(added)
                ));
            }
            let dropped: Vec<&String> = have.difference(&want).collect();
            if !dropped.is_empty() {
                why.push(format!(
                    "the workspace has {}, which this dispatch doesn't (its commit has no such submodule, or it is left out)",
                    list(dropped)
                ));
            }
            bail!("its submodules aren't the workspace's: {}", why.join("; "));
        }
        for p in &parts {
            // Before git runs there: without one, git would find the repository above it.
            if p.dir.join(".git").symlink_metadata().is_err() {
                bail!("{} ({}) is not a checkout", p.label(), p.dir.display());
            }
            let _lock = Lock::shared(&self.mirror_lock(p.repo_id))?;
            force_checkout(&p.dir, p.commit)
                .with_context(|| format!("checking {} out at {}", p.label(), short(p.commit)))?;
        }
        for p in &parts {
            self.pin(p.repo_id, p.commit, id)?;
        }
        Ok(())
    }

    /// The submodules checked out in the workspace at `root`, by path from the top: each gitlink
    /// of commit `from`'s tree (in the mirror of `top_id`) whose directory there is a checkout of
    /// its own, then those of each submodule checked out, at the commit its parent's tree names. A
    /// submodule its first dispatch left out, and what is nested in it, has no checkout.
    fn checked_out_submodules(
        &self,
        top_id: &str,
        from: &str,
        root: &Path,
    ) -> anyhow::Result<BTreeSet<String>> {
        let mut out = BTreeSet::new();
        let mut stack = vec![(top_id.to_string(), from.to_string(), String::new())];
        while let Some((rid, sha, prefix)) = stack.pop() {
            check_id("repository", &rid)?;
            let mirror = self.paths.mirror(&rid);
            let raw = mirror_bytes(&mirror, gitinfo::gitlinks_args(&sha)).with_context(|| {
                format!("can't read the submodules of {} in {rid}", short(&sha))
            })?;
            for (path, sub_sha) in gitinfo::parse_gitlinks(&raw, &mirror)? {
                let full = format!("{prefix}{path}");
                if root.join(&full).join(".git").symlink_metadata().is_err() {
                    continue;
                }
                let sub_id = gitinfo::submodule_repo_id(top_id, &full);
                stack.push((sub_id, sub_sha, format!("{full}/")));
                out.insert(full);
            }
        }
        Ok(out)
    }

    /// Checks that HEAD and every submodule's HEAD at `root` are `repo`'s commits.
    pub fn verify(&self, root: &Path, repo: &RepoRef) -> anyhow::Result<()> {
        check_head(root, &repo.commit, "the workspace")?;
        for s in &repo.submodules {
            check_head(
                &root.join(&s.path),
                &s.commit,
                &format!("submodule {}", s.path),
            )?;
        }
        Ok(())
    }

    /// Removes the workspace's tree (every worktree in it), its TMPDIR, the worktree entries left
    /// pointing nowhere and the pins named after it, in every mirror.
    pub fn remove(&self, record: &WorkspaceRecord) -> anyhow::Result<()> {
        check_id("workspace", &record.id)?;
        let root = if record.path.as_os_str().is_empty() {
            self.paths.workspace(&record.id)
        } else {
            record.path.clone()
        };
        let related = self.related_repo_ids(&record.repo_id, &record.commit);
        self.remove_paths(
            &record.id,
            &root,
            &self.paths.workspace_tmp(&record.id),
            &related,
        )
    }

    /// The repositories a workspace of `repo_id` at `commit` uses: the top and every submodule,
    /// from the commit's gitlinks in the mirrors (pinned, so still there).
    fn related_repo_ids(&self, repo_id: &str, commit: &str) -> Vec<String> {
        let mut ids = vec![repo_id.to_string()];
        let mut stack = vec![(repo_id.to_string(), commit.to_string(), String::new())];
        while let Some((rid, sha, prefix)) = stack.pop() {
            let mirror = self.paths.mirror(&rid);
            let links = mirror_bytes(&mirror, gitinfo::gitlinks_args(&sha))
                .and_then(|raw| gitinfo::parse_gitlinks(&raw, &mirror));
            let Ok(links) = links else {
                continue;
            };
            for (path, sub_sha) in links {
                let full = format!("{prefix}{path}");
                let sub_id = gitinfo::submodule_repo_id(repo_id, &full);
                ids.push(sub_id.clone());
                stack.push((sub_id, sub_sha, format!("{full}/")));
            }
        }
        ids
    }

    /// Removes a workspace's worktrees, files and pins. Its tree is moved aside (one rename) and
    /// deleted before any mirror is locked: deleting a big tree takes long, and a mirror lock held
    /// meanwhile holds up every sync of that repository (any dispatch of it). Then each mirror
    /// drops the worktree entries left pointing nowhere and the pins named after the workspace:
    /// the mirrors of `related` (the repositories it uses) are waited for; every other mirror is
    /// swept only when no one holds it exclusively right now, so a sync or a full fsck of one
    /// repository never holds up removing another's workspaces.
    fn remove_paths(
        &self,
        id: &str,
        root: &Path,
        tmp: &Path,
        related: &[String],
    ) -> anyhow::Result<()> {
        let mut errors = Vec::new();
        // Where it can't be moved, it is deleted in place (still holding no lock). A tree moved
        // aside by a removal that a restart cut short is deleted now.
        let aside = aside(root);
        if let Some(aside) = &aside {
            let _ = std::fs::rename(root, aside);
        }
        for p in aside.iter().map(PathBuf::as_path).chain([root, tmp]) {
            if let Err(e) = remove_all(p) {
                errors.push(format!("{e:#}"));
            }
        }
        for (repo_id, mirror) in self.mirrors() {
            let _lock = if related.contains(&repo_id) {
                Lock::shared(&self.mirror_lock(&repo_id))?
            } else {
                match Lock::try_shared(&self.mirror_lock(&repo_id))? {
                    Some(lock) => lock,
                    None => continue,
                }
            };
            let admin = Lock::exclusive(&self.worktrees_lock(&repo_id))?;
            if let Err(e) = mirror_text(&mirror, ["worktree", "prune"]) {
                errors.push(format!("{e:#}"));
            }
            drop(admin);
            if let Err(e) = mirror_text(
                &mirror,
                ["update-ref", "-d", &format!("refs/potluck/pins/{id}")],
            ) {
                errors.push(format!("{e:#}"));
            }
        }
        if !errors.is_empty() {
            bail!("removing workspace {id}: {}", errors.join("; "));
        }
        Ok(())
    }

    /// Bytes the workspace uses on disk: blocks allocated (st_blocks * 512) to its tree and to its
    /// TMPDIR beside it (`<root>.tmp`), hardlinked files counted once. Blocks a clone still shares
    /// with the base checkout are counted in full, so this overstates what removing it frees.
    pub fn size(&self, root: &Path) -> anyhow::Result<u64> {
        let mut seen = HashSet::new();
        let mut total = 0;
        if root.symlink_metadata().is_err() {
            bail!("{} doesn't exist", root.display());
        }
        add_size(root, &mut seen, &mut total)?;
        let mut tmp = root.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        if tmp.symlink_metadata().is_ok() {
            add_size(&tmp, &mut seen, &mut total)?;
        }
        Ok(total)
    }
}

/// Where a removal moves the workspace tree at `root` before deleting it: `<root>.removing`. A tree
/// left there (its deletion failed, or a restart cut it short) is deleted by the next removal of
/// the workspace.
pub fn aside(root: &Path) -> Option<PathBuf> {
    root.file_name()
        .map(|name| root.with_file_name(format!("{}.removing", name.to_string_lossy())))
}

fn add_size(path: &Path, seen: &mut HashSet<(u64, u64)>, total: &mut u64) -> anyhow::Result<()> {
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        let meta = match p.symlink_metadata() {
            Ok(m) => m,
            // Removed while we walk: a job may still be writing.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format!("can't stat {}", p.display())),
        };
        if meta.nlink() > 1 && !meta.is_dir() && !seen.insert((meta.dev(), meta.ino())) {
            continue;
        }
        *total += meta.blocks() * 512;
        if meta.is_dir()
            && let Ok(rd) = std::fs::read_dir(&p)
        {
            stack.extend(rd.flatten().map(|e| e.path()));
        }
    }
    Ok(())
}

/// Free space under which a failed seeding is put down to a full disk.
const DISK_FULL_BYTES: u64 = 256 << 20;

/// A seeding failed while the disk was full: "the disk holding <dir> is full (12 MiB free)".
#[derive(Debug)]
pub(crate) struct DiskFull(String);

impl std::fmt::Display for DiskFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a write in `dir` (a seeding, an exit status) failed, if the disk holding it is full (less
/// than [`DISK_FULL_BYTES`] free for this user).
pub(crate) fn disk_full(dir: &Path) -> Option<DiskFull> {
    let probe = dir.ancestors().find(|p| p.exists())?;
    let free = free_bytes(probe)?;
    (free < DISK_FULL_BYTES).then(|| {
        DiskFull(format!(
            "the disk holding {} is full ({} MiB free)",
            probe.display(),
            free >> 20
        ))
    })
}

/// Bytes free for an unprivileged user on the filesystem holding `path` (statvfs).
fn free_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: statvfs fills `s`; `c` is a valid C string.
    unsafe {
        let mut s: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut s) != 0 {
            return None;
        }
        #[allow(clippy::useless_conversion)]
        Some(u64::from(s.f_bavail).saturating_mul(u64::from(s.f_frsize)))
    }
}

/// The base checkout's commit and git dir, if it is usable: a worktree at a readable commit with
/// an index and no leftover `index.lock`.
fn base_head(base: &Path) -> Option<(String, PathBuf)> {
    if !base.join(".git").is_file() {
        return None;
    }
    let out = tree_text(
        base,
        [
            "rev-parse",
            "--path-format=absolute",
            "--show-toplevel",
            "--absolute-git-dir",
            "HEAD^{commit}",
        ],
    )
    .ok()?;
    let lines: Vec<&str> = out.lines().collect();
    let [top, admin, sha] = lines.as_slice() else {
        return None;
    };
    if canonical(Path::new(top)).ok()? != canonical(base).ok()? {
        return None;
    }
    let admin = PathBuf::from(admin);
    if admin.join("index.lock").exists() || !admin.join("index").is_file() {
        return None;
    }
    Some((sha.to_string(), admin))
}

/// Whether the base at `base_commit` should move to `target` before seeding: when the target is
/// at least as new (committer time). Never onto a commit with paths that differ only in case (or
/// normalisation) when
/// the base's filesystem can't tell them apart: the base would hold a tree that isn't the commit's.
/// Nor onto one that adds a name the filesystem refuses: the move would fail part way, and a base
/// half moved is made anew from nothing.
fn should_advance(
    mirror: &Path,
    base: &Path,
    base_commit: &str,
    target: &str,
) -> anyhow::Result<bool> {
    if base_commit == target {
        return Ok(false);
    }
    let out = mirror_text(
        mirror,
        [
            "log",
            "--no-walk=unsorted",
            "--format=%ct",
            base_commit,
            target,
        ],
    )?;
    let times: Vec<u64> = out.lines().filter_map(|l| l.trim().parse().ok()).collect();
    let newer = match times.as_slice() {
        [b, t] => t >= b,
        _ => true,
    };
    if !newer {
        return Ok(false);
    }
    let insensitive = base.parent().is_some_and(case::case_insensitive);
    if insensitive && !case::collisions(&commit_paths(mirror, target)?).is_empty() {
        return Ok(false);
    }
    let added = mirror_bytes(
        mirror,
        [
            "diff-tree",
            "-r",
            "-z",
            "--name-only",
            "--no-renames",
            "--diff-filter=A",
            base_commit,
            target,
        ],
    )?;
    let added: Vec<Vec<u8>> = added
        .split(|b| *b == 0)
        .filter(|p| !p.is_empty())
        .map(<[u8]>::to_vec)
        .collect();
    Ok(base
        .parent()
        .is_none_or(|dir| case::refused(dir, &added).is_empty()))
}

/// Moves the checkout at `dir` from commit `from` to `to` with a two-tree `read-tree -m -u`, which
/// writes only the files whose blobs differ, and fails on local changes in its way
/// ([`switch_tree`]).
fn move_tree(dir: &Path, from: &str, to: &str) -> anyhow::Result<()> {
    switch_tree(dir, from, to, &["read-tree", "-m", "-u", from, to])
}

/// Checks the worktree at `dir` out at `to`, detached, with `git checkout --force`: tracked files
/// become `to`'s (changes to them are discarded, files `to` lacks are removed) and untracked files
/// in the way of its files are removed; other untracked and ignored files stay. Only the files
/// that differ from the index are written ([`switch_tree`], from the commit HEAD was at).
fn force_checkout(dir: &Path, to: &str) -> anyhow::Result<()> {
    let from = tree_text(dir, ["rev-parse", "--verify", "--quiet", "HEAD^{commit}"])
        .with_context(|| format!("{} has no HEAD commit", dir.display()))?;
    switch_tree(
        dir,
        &from,
        to,
        &["checkout", "--force", "--detach", "--quiet", to],
    )
}

/// Writes commit `to`'s files at `dir`, whose files are commit `from`'s, by running git with
/// `switch` (which writes only the files whose blobs differ). When the commits' `.gitattributes`
/// differ, the files whose converting attributes differ under them are written again: a blob the
/// commits share would otherwise keep what `from`'s attributes made of it (an `eol=crlf`, `ident`
/// or filter added or dropped), unlike a fresh checkout of `to`, and git would still call it
/// clean. A file whose attributes are the same under both is written the same way by either: it
/// stays as it is (and keeps sharing its blocks with the base it was cloned from).
fn switch_tree(dir: &Path, from: &str, to: &str, switch: &[&str]) -> anyhow::Result<()> {
    let same = tree_git(dir)
        .args(["diff", "--quiet", "--no-ext-diff", from, to, "--"])
        .arg(":(glob)**/.gitattributes")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    // `to`'s files, and what `from`'s attributes say of them, read while the tree still has them.
    let before = if same {
        None
    } else {
        let files = commit_files(dir, to)?;
        let attrs = super::filters::attributes(dir, files.iter().map(String::as_str))?;
        Some((files, attrs))
    };
    tree_text(dir, switch)?;
    let Some((files, before)) = before else {
        return Ok(());
    };
    let after = super::filters::attributes(dir, files.iter().map(String::as_str))?;
    let paths: Vec<&String> = files
        .iter()
        .filter(|p| before.get(*p) != after.get(*p))
        .collect();
    if paths.is_empty() {
        return Ok(());
    }
    // checkout-index leaves a file whose stat the index matches as it is, even with --force.
    let mut input = Vec::new();
    for p in paths {
        remove_all(&dir.join(p))?;
        input.extend_from_slice(p.as_bytes());
        input.push(0);
    }
    let mut cmd = tree_git(dir);
    cmd.args(["checkout-index", "--index", "-z", "--stdin"]);
    super::filters::run_with_input(cmd, input)?;
    Ok(())
}

/// The regular files (modes 100644 and 100755) in `commit`'s tree, read in the repository at `dir`.
fn commit_files(dir: &Path, commit: &str) -> anyhow::Result<Vec<String>> {
    let raw = tree_bytes(dir, ["ls-tree", "-r", "-z", "--full-tree", commit])?;
    let mut out = Vec::new();
    for rec in raw.split(|b| *b == 0).filter(|r| !r.is_empty()) {
        // "<mode> SP <type> SP <object> TAB <path>"
        let Some(tab) = rec.iter().position(|b| *b == b'\t') else {
            continue;
        };
        if !(rec.starts_with(b"100644 ") || rec.starts_with(b"100755 ")) {
            continue;
        }
        if let Ok(path) = std::str::from_utf8(&rec[tab + 1..]) {
            out.push(path.to_string());
        }
    }
    Ok(out)
}

/// Every path in `commit`'s tree.
fn commit_paths(mirror: &Path, commit: &str) -> anyhow::Result<Vec<String>> {
    Ok(commit_paths_raw(mirror, commit)?
        .iter()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .collect())
}

/// [`commit_paths`] as git gives them (a name need not be UTF-8).
fn commit_paths_raw(mirror: &Path, commit: &str) -> anyhow::Result<Vec<Vec<u8>>> {
    let raw = mirror_bytes(
        mirror,
        ["ls-tree", "-r", "-z", "--name-only", "--full-tree", commit],
    )?;
    Ok(raw
        .split(|b| *b == 0)
        .filter(|p| !p.is_empty())
        .map(<[u8]>::to_vec)
        .collect())
}

impl Workspaces {
    /// `git worktree add --detach --no-checkout <dir> <commit>` in the mirror of `repo_id`.
    fn add_worktree(&self, repo_id: &str, dir: &Path, commit: &str) -> anyhow::Result<()> {
        let _admin = Lock::exclusive(&self.worktrees_lock(repo_id))?;
        mirror_text(
            &self.paths.mirror(repo_id),
            [
                "worktree".as_ref(),
                "add".as_ref(),
                "--detach".as_ref(),
                "--no-checkout".as_ref(),
                "--quiet".as_ref(),
                dir.as_os_str(),
                commit.as_ref(),
            ],
        )?;
        Ok(())
    }

    /// Under the base's exclusive lock: makes the base of `repo_id` usable at `target` or newer.
    /// Moves a usable base forward to `target`, leaves one already as new, and makes a broken or
    /// missing one anew (a full checkout).
    fn prepare_base(&self, repo_id: &str, target: &str) -> anyhow::Result<()> {
        let mirror = self.paths.mirror(repo_id);
        let base = self.paths.base(repo_id);
        if let Some((b, _)) = base_head(&base) {
            if !should_advance(&mirror, &base, &b, target)? {
                return Ok(());
            }
            // The same two-tree merge a workspace gets: only the files that differ are written. A
            // base with local changes in its way fails here and is made anew below.
            let moved = move_tree(&base, &b, target)
                .and_then(|_| tree_text(&base, ["update-ref", "--no-deref", "HEAD", target]));
            if moved.is_ok() && base_head(&base).is_some_and(|(h, _)| h == target) {
                mirror_text(&mirror, ["update-ref", "refs/potluck/base", target])?;
                return Ok(());
            }
        }
        self.remove_base(repo_id);
        if let Some(parent) = base.parent() {
            std::fs::create_dir_all(parent)?;
        }
        self.add_worktree(repo_id, &base, target)
            .context("can't make the base checkout")?;
        // The full checkout, outside the worktrees lock.
        tree_text(&base, ["reset", "--hard", "--quiet"])
            .context("can't check out the base checkout")?;
        mirror_text(&mirror, ["update-ref", "refs/potluck/base", target])?;
        Ok(())
    }

    /// Removes the base checkout of `repo_id` and its worktree entry. It moves aside first (one
    /// rename) and is deleted once the worktrees lock is let go: a big checkout takes a while to
    /// delete, and every workspace of the repository being made or removed waits for that lock.
    fn remove_base(&self, repo_id: &str) {
        let mirror = self.paths.mirror(repo_id);
        let base = self.paths.base(repo_id);
        // Hidden, so never taken for a base (ids can't start with '.'). One left by a removal cut
        // short goes now.
        let aside = base.with_file_name(format!(".{repo_id}.removing"));
        let _ = remove_all(&aside);
        if std::fs::rename(&base, &aside).is_ok() {
            if let Ok(_admin) = Lock::exclusive(&self.worktrees_lock(repo_id)) {
                let _ = mirror_text(&mirror, ["worktree", "prune"]);
            }
            let _ = remove_all(&aside);
            return;
        }
        // Not there (or not movable): as git sees it.
        if let Ok(_admin) = Lock::exclusive(&self.worktrees_lock(repo_id)) {
            let _ = mirror_text(
                &mirror,
                [
                    "worktree".as_ref(),
                    "remove".as_ref(),
                    "--force".as_ref(),
                    "--force".as_ref(),
                    base.as_os_str(),
                ],
            );
        }
        let _ = remove_all(&base);
        if let Ok(_admin) = Lock::exclusive(&self.worktrees_lock(repo_id)) {
            let _ = mirror_text(&mirror, ["worktree", "prune"]);
        }
    }
}

/// `git status` lines (`XY path`) of one repository's own files (submodules are checked on their
/// own).
pub(super) fn status(dir: &Path) -> anyhow::Result<Vec<String>> {
    let raw = tree_bytes(
        dir,
        [
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=normal",
            "--ignore-submodules=all",
        ],
    )?;
    let mut lines = Vec::new();
    // A rename or copy is followed by its source path, as a field of its own (of any length).
    let mut fields = raw.split(|b| *b == 0).filter(|f| !f.is_empty());
    while let Some(f) = fields.next() {
        if f.len() < 4 {
            continue;
        }
        let line = String::from_utf8_lossy(f).into_owned();
        if line[..2].contains('R') || line[..2].contains('C') {
            fields.next();
        }
        lines.push(line);
    }
    Ok(lines)
}

/// Checks that `dir` is its own checkout with HEAD at `want`.
fn check_head(dir: &Path, want: &str, what: &str) -> anyhow::Result<()> {
    if dir.join(".git").symlink_metadata().is_err() {
        bail!("{what} ({}) is not a checkout", dir.display());
    }
    let out = tree_text(
        dir,
        [
            "rev-parse",
            "--path-format=absolute",
            "--show-toplevel",
            "HEAD",
        ],
    )
    .with_context(|| format!("{what} ({}) is not a usable checkout", dir.display()))?;
    let lines: Vec<&str> = out.lines().collect();
    let [top, head] = lines.as_slice() else {
        bail!("{what} ({}) is not a usable checkout", dir.display());
    };
    if canonical(Path::new(top))? != canonical(dir)? {
        bail!("{what} ({}) is not a checkout of its own", dir.display());
    }
    if *head != want {
        bail!(
            "{what} is at {}, but the job needs {}",
            short(head),
            short(want)
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::Command;

    fn git(dir: &Path, args: &[&str]) {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["-c", "user.name=t", "-c", "user.email=t@example.invalid"])
            .args([
                "-c",
                "commit.gpgsign=false",
                "-c",
                "init.defaultBranch=main",
            ])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[test]
    fn status_lists_every_entry_after_a_rename_from_a_short_path() {
        let dir = tempfile::tempdir().unwrap();
        let r = dir.path();
        git(r, &["init", "-q"]);
        std::fs::write(r.join("ab"), "renamed content\n").unwrap();
        std::fs::write(r.join("zz-other"), "one\n").unwrap();
        git(r, &["add", "-A"]);
        git(r, &["commit", "-q", "-m", "one"]);
        git(r, &["mv", "ab", "renamed-ab"]);
        std::fs::write(r.join("zz-other"), "two\n").unwrap();
        // -z: "R  renamed-ab\0ab\0 M zz-other\0": the rename's source is a field of its own.
        assert_eq!(super::status(r).unwrap(), ["R  renamed-ab", " M zz-other"]);
    }
}
