//! Real git repositories in temp dirs: inspecting, mirrors, seeding, verifying, removing, clones.
//!
//! Temp dirs live under Cargo's target tmp dir (`CARGO_TARGET_TMPDIR`), which must be on a
//! copy-on-write filesystem: the system temp dir often isn't (tmpfs on many Linux machines).

use potluck::config::Paths;
use potluck::types::{RepoRef, WorkspaceRecord, WorkspaceState};
use potluck::workspace::Workspaces;
use potluck::{cow, gitinfo};
use std::collections::BTreeMap;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

fn tmp() -> tempfile::TempDir {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    std::fs::create_dir_all(&root).unwrap();
    tempfile::Builder::new()
        .prefix("potluck-test-")
        .tempdir_in(root)
        .unwrap()
}

/// Runs git hermetically (no user or system config) with fixed dates, in `dir`.
fn git_at(dir: &Path, time: u64, args: &[&str]) -> String {
    let date = format!("@{time} +0000");
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args([
            "-c",
            "protocol.file.allow=always",
            "-c",
            "init.defaultBranch=main",
            "-c",
            "user.name=potluck test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
            "-c",
            "core.compression=1",
            // No auto gc: a detached gc would outlive the test and race with it.
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
        ])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?} in {}: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn git(dir: &Path, args: &[&str]) -> String {
    git_at(dir, 1_700_000_000, args)
}

fn write(dir: &Path, rel: &str, content: &str) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, content).unwrap();
}

fn init(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "-q"]);
}

/// Commits everything at Unix time `time`; returns the sha.
fn commit(dir: &Path, msg: &str, time: u64) -> String {
    git_at(dir, time, &["add", "-A"]);
    git_at(dir, time, &["commit", "-q", "--allow-empty", "-m", msg]);
    git(dir, &["rev-parse", "HEAD"])
}

struct Env {
    dir: tempfile::TempDir,
    ws: Workspaces,
}

fn env() -> Env {
    let dir = tmp();
    let ws = Workspaces::new(Paths {
        home: dir.path().join("state"),
        config: dir.path().join("node.toml"),
    });
    Env { dir, ws }
}

impl Env {
    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    fn mirror(&self, repo_id: &str) -> PathBuf {
        self.ws.paths.mirror(repo_id)
    }

    /// What dispatch does: syncs the mirrors of `repo` and its submodules, then pins each commit
    /// (`job/<sha>`), fetching it when it is on no branch (a submodule's commit usually isn't on
    /// any branch of the submodule's own clone). Asserts nothing went wrong.
    fn sync(&self, repo: &RepoRef) {
        let mut all = vec![(&repo.id, &repo.source, &repo.commit)];
        all.extend(
            repo.submodules
                .iter()
                .map(|s| (&s.repo_id, &s.source, &s.commit)),
        );
        for (id, source, commit) in all {
            let url = source.to_str().unwrap();
            let report = self.ws.sync_mirror(id, url).unwrap();
            assert!(report.recloned.is_none(), "{report:?}");
            self.ws
                .fetch_pin(id, url, commit, &format!("job/{}", &commit[..8]))
                .unwrap();
        }
    }

    /// Pins named after workspaces (not dispatch's `job/...` pins) in a mirror.
    fn workspace_pins(&self, repo_id: &str) -> Vec<String> {
        refs(&self.mirror(repo_id), &["refs/potluck/pins"])
            .into_keys()
            .filter(|r| !r.starts_with("refs/potluck/pins/job/"))
            .collect()
    }

    fn record(&self, id: &str, repo: &RepoRef, path: &Path) -> WorkspaceRecord {
        WorkspaceRecord {
            id: id.into(),
            node: "test".into(),
            repo_id: repo.id.clone(),
            commit: repo.commit.clone(),
            branch: repo.branch.clone(),
            path: path.to_path_buf(),
            state: WorkspaceState::Idle,
            created_at: 0,
            last_used_at: 0,
            held_until: 0,
            pending: 0,
            size_bytes: None,
        }
    }
}

/// Refs of a repository, as `sha refname` lines, limited to `prefixes`.
fn refs(dir: &Path, prefixes: &[&str]) -> BTreeMap<String, String> {
    let mut args = vec!["for-each-ref", "--format=%(refname) %(objectname)"];
    args.extend_from_slice(prefixes);
    git(dir, &args)
        .lines()
        .filter_map(|l| l.split_once(' '))
        .map(|(r, s)| (r.to_string(), s.to_string()))
        .collect()
}

/// Paths of the worktrees git lists for a mirror, besides the bare repository itself.
fn worktrees(mirror: &Path) -> Vec<String> {
    git(mirror, &["worktree", "list", "--porcelain"])
        .lines()
        .filter_map(|l| l.strip_prefix("worktree "))
        .map(String::from)
        .filter(|p| !p.ends_with(".git"))
        .collect()
}

// --- inspect -------------------------------------------------------------------------------

#[test]
fn inspect_refuses_uncommitted_work() {
    let t = tmp();
    let r = t.path().join("proj");
    init(&r);
    write(&r, "src/main.rs", "fn main() {}\n");
    write(&r, ".gitignore", "target/\n");
    commit(&r, "one", 1_700_000_000);

    // Ignored files are fine.
    write(&r, "target/debug/x", "built");
    let repo = gitinfo::inspect(&r.join("src")).unwrap();
    assert_eq!(repo.branch.as_deref(), Some("main"));
    assert_eq!(repo.commit, git(&r, &["rev-parse", "HEAD"]));
    assert!(repo.submodules.is_empty());

    write(&r, "src/main.rs", "fn main() { changed() }\n");
    let e = format!("{:#}", gitinfo::inspect(&r).unwrap_err());
    assert!(
        e.contains("only committed work runs") && e.contains("src/main.rs"),
        "{e}"
    );
    git(&r, &["checkout", "--", "src/main.rs"]);

    write(&r, "notes.txt", "untracked");
    let e = format!("{:#}", gitinfo::inspect(&r).unwrap_err());
    assert!(e.contains("?? notes.txt"), "{e}");
    git(&r, &["add", "notes.txt"]);
    let e = format!("{:#}", gitinfo::inspect(&r).unwrap_err());
    assert!(e.contains("A  notes.txt"), "{e}");
    git(&r, &["rm", "-q", "--cached", "notes.txt"]);
    std::fs::remove_file(r.join("notes.txt")).unwrap();
    gitinfo::inspect(&r).unwrap();

    // Not a repository: in the system temp dir, since `t` sits in the target dir, inside this
    // checkout (if the system one does too, there's nothing to check).
    let plain_dir = tempfile::tempdir().unwrap();
    let plain = plain_dir.path().to_path_buf();
    let outside = !Command::new("git")
        .arg("-C")
        .arg(&plain)
        .args(["rev-parse", "--git-dir"])
        .output()
        .unwrap()
        .status
        .success();
    if outside {
        let e = format!("{:#}", gitinfo::inspect(&plain).unwrap_err());
        assert!(
            e.contains("is not inside a git work tree; run dispatch in the repository"),
            "{e}"
        );
    }
}

#[test]
fn inspect_ids_stable_across_worktrees() {
    let t = tmp();
    let r = t.path().join("proj");
    init(&r);
    write(&r, "a", "a");
    let c1 = commit(&r, "one", 1_700_000_000);
    let wt = t.path().join("proj-agent");
    git(
        &r,
        &["worktree", "add", "-q", "-b", "agent", wt.to_str().unwrap()],
    );
    write(&wt, "b", "b");
    let c2 = commit(&wt, "two", 1_700_000_100);

    let main = gitinfo::inspect(&r).unwrap();
    let agent = gitinfo::inspect(&wt).unwrap();
    assert_eq!(main.id, agent.id);
    assert!(main.id.starts_with("proj-"), "{}", main.id);
    assert_eq!(main.source, agent.source);
    assert_eq!(main.source, std::fs::canonicalize(r.join(".git")).unwrap());
    assert_eq!(
        (main.commit.as_str(), main.branch.as_deref()),
        (c1.as_str(), Some("main"))
    );
    assert_eq!(
        (agent.commit.as_str(), agent.branch.as_deref()),
        (c2.as_str(), Some("agent"))
    );

    // A tag with the branch's name leaves the branch's name alone (holds and reminders match it
    // against the client's context, which reads the full ref name).
    git(&r, &["tag", "agent"]);
    assert_eq!(
        gitinfo::inspect(&wt).unwrap().branch.as_deref(),
        Some("agent")
    );

    git(&wt, &["checkout", "-q", "--detach"]);
    let detached = gitinfo::inspect(&wt).unwrap();
    assert_eq!(
        (detached.commit.as_str(), detached.branch),
        (c2.as_str(), None)
    );

    // A different repository with the same directory name gets a different id.
    let other = t.path().join("elsewhere/proj");
    init(&other);
    commit(&other, "x", 1_700_000_000);
    let o = gitinfo::inspect(&other).unwrap();
    assert!(o.id.starts_with("proj-") && o.id != main.id);
}

/// app (vendor/lib -> lib (inner -> inner)). Returns (app, lib, inner) source dirs.
fn submodule_fixture(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let inner = root.join("inner");
    init(&inner);
    write(&inner, "inner.txt", "inner 1\n");
    commit(&inner, "inner 1", 1_700_000_000);

    let lib = root.join("lib");
    init(&lib);
    write(&lib, "lib.c", "int lib = 1;\n");
    git(
        &lib,
        &["submodule", "add", "-q", inner.to_str().unwrap(), "inner"],
    );
    commit(&lib, "lib 1", 1_700_000_000);

    let app = root.join("app");
    init(&app);
    write(&app, "src/app.c", "int main() {}\n");
    git(
        &app,
        &[
            "submodule",
            "add",
            "-q",
            lib.to_str().unwrap(),
            "vendor/lib",
        ],
    );
    git(
        &app,
        &["submodule", "update", "-q", "--init", "--recursive"],
    );
    commit(&app, "app 1", 1_700_000_000);
    (app, lib, inner)
}

#[test]
fn submodule_ids_shared_across_worktrees() {
    let t = tmp();
    let (app, _lib, _inner) = submodule_fixture(t.path());
    let wt = t.path().join("app-agent");
    git(
        &app,
        &["worktree", "add", "-q", "-b", "agent", wt.to_str().unwrap()],
    );
    git(&wt, &["submodule", "update", "-q", "--init", "--recursive"]);
    let main = gitinfo::inspect(&app).unwrap();
    let agent = gitinfo::inspect(&wt).unwrap();
    // The linked worktree keeps its own submodule git dirs...
    assert_ne!(main.submodules[0].source, agent.submodules[0].source);
    // ...but they are the same repositories: one mirror each on every machine.
    let ids = |r: &potluck::types::RepoRef| -> Vec<String> {
        r.submodules.iter().map(|s| s.repo_id.clone()).collect()
    };
    assert_eq!(ids(&main), ids(&agent));
    assert_eq!(main.id, agent.id);
}

#[test]
fn inspect_submodules_nested() {
    let t = tmp();
    let (app, _lib, _inner) = submodule_fixture(t.path());
    let repo = gitinfo::inspect(&app).unwrap();
    let subs: Vec<(&str, &str)> = repo
        .submodules
        .iter()
        .map(|s| (s.path.as_str(), s.commit.as_str()))
        .collect();
    let lib_head = git(&app.join("vendor/lib"), &["rev-parse", "HEAD"]);
    let inner_head = git(&app.join("vendor/lib/inner"), &["rev-parse", "HEAD"]);
    assert_eq!(
        subs,
        vec![
            ("vendor/lib", lib_head.as_str()),
            ("vendor/lib/inner", inner_head.as_str())
        ]
    );
    let lib_ref = &repo.submodules[0];
    assert!(lib_ref.repo_id.starts_with("lib-"), "{}", lib_ref.repo_id);
    assert_eq!(
        lib_ref.source,
        std::fs::canonicalize(app.join(".git/modules/vendor/lib")).unwrap()
    );
    assert!(repo.submodules[1].repo_id.starts_with("inner-"));
    assert_ne!(repo.submodules[0].repo_id, repo.id);

    // Dirty files in a nested submodule are named with their full path.
    write(&app, "vendor/lib/inner/inner.txt", "changed\n");
    write(&app, "vendor/lib/new.h", "x\n");
    let e = format!("{:#}", gitinfo::inspect(&app).unwrap_err());
    assert!(
        e.contains(" M vendor/lib/inner/inner.txt") && e.contains("?? vendor/lib/new.h"),
        "{e}"
    );
    git(
        &app.join("vendor/lib/inner"),
        &["checkout", "--", "inner.txt"],
    );
    std::fs::remove_file(app.join("vendor/lib/new.h")).unwrap();
    gitinfo::inspect(&app).unwrap();

    // A submodule moved to another commit without committing that in the superproject.
    let lib_dir = app.join("vendor/lib");
    write(&lib_dir, "lib.c", "int lib = 2;\n");
    commit(&lib_dir, "lib 2", 1_700_000_100);
    let e = format!("{:#}", gitinfo::inspect(&app).unwrap_err());
    assert!(
        e.contains("submodule vendor/lib: checked out at") && e.contains("M vendor/lib"),
        "{e}"
    );
    git(&lib_dir, &["checkout", "-q", "--detach", &lib_head]);
    gitinfo::inspect(&app).unwrap();

    // A submodule that isn't checked out.
    git(&app, &["submodule", "deinit", "-q", "-f", "vendor/lib"]);
    let e = format!("{:#}", gitinfo::inspect(&app).unwrap_err());
    assert!(e.contains("submodule vendor/lib: not checked out"), "{e}");
}

// --- mirrors -------------------------------------------------------------------------------

#[test]
fn mirror_is_an_exact_copy_and_keeps_pins() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1");
    let c1 = commit(&src, "one", 1_700_000_000);
    git(&src, &["branch", "feature"]);
    git(&src, &["branch", "doomed"]);
    git(&src, &["tag", "v1"]);
    git(&src, &["tag", "-a", "-m", "annotated", "v2"]);
    // Refs that are neither branches nor tags stay out of mirrors.
    git(&src, &["update-ref", "refs/remotes/origin/x", &c1]);
    write(&src, "a", "stash me");
    git(&src, &["stash", "-q"]);
    let url = src.join(".git");
    let url = url.to_str().unwrap();
    let id = "src-00000001";

    let report = e.ws.sync_mirror(id, url).unwrap();
    assert!(report.created && report.recloned.is_none());
    let mirror = e.mirror(id);
    let ours = |m: &Path| refs(m, &["refs/heads", "refs/tags"]);
    assert_eq!(ours(&mirror), refs(&src, &["refs/heads", "refs/tags"]));
    assert!(refs(&mirror, &["refs/remotes", "refs/stash"]).is_empty());
    assert!(e.ws.has_commit(id, &c1));

    // Pin feature's commit, then amend it away (a force-push), delete a branch and a tag.
    assert!(e.ws.pins().unwrap().is_empty());
    e.ws.pin(id, &c1, "ws-1").unwrap();
    e.ws.pin(id, &c1, "studio-7").unwrap();
    // A clone left beside the mirror by a reclone cut short isn't a mirror.
    std::fs::create_dir_all(e.path("state/mirrors").join(format!("{id}.git.new"))).unwrap();
    assert_eq!(
        e.ws.pins().unwrap(),
        [
            (id.to_string(), "studio-7".to_string()),
            (id.to_string(), "ws-1".to_string())
        ]
    );
    std::fs::remove_dir(e.path("state/mirrors").join(format!("{id}.git.new"))).unwrap();
    e.ws.unpin(id, "studio-7").unwrap();
    git(&src, &["checkout", "-q", "feature"]);
    write(&src, "b", "2");
    git_at(&src, 1_700_000_100, &["add", "-A"]);
    git_at(
        &src,
        1_700_000_100,
        &["commit", "-q", "--amend", "-m", "amended"],
    );
    let amended = git(&src, &["rev-parse", "HEAD"]);
    git(&src, &["branch", "-D", "doomed"]);
    git(&src, &["tag", "-d", "v1"]);
    git(&src, &["branch", "fresh"]);

    let report = e.ws.sync_mirror(id, url).unwrap();
    assert_eq!(report, Default::default());
    assert_eq!(ours(&mirror), refs(&src, &["refs/heads", "refs/tags"]));
    let m = refs(&mirror, &[]);
    assert_eq!(m.get("refs/heads/feature"), Some(&amended));
    assert!(!m.contains_key("refs/heads/doomed") && !m.contains_key("refs/tags/v1"));
    assert_eq!(m.get("refs/potluck/pins/ws-1"), Some(&c1));
    assert!(e.ws.has_commit(id, &c1) && e.ws.has_commit(id, &amended));

    e.ws.unpin(id, "ws-1").unwrap();
    assert!(!refs(&mirror, &["refs/potluck"]).contains_key("refs/potluck/pins/ws-1"));
    e.ws.unpin(id, "ws-1").unwrap();

    // Mirror config that seeding relies on.
    assert_eq!(git(&mirror, &["config", "core.checkStat"]), "minimal");
    assert_eq!(git(&mirror, &["config", "core.hooksPath"]), "/dev/null");
}

#[test]
fn fetch_pin_brings_commits_on_no_branch() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1");
    commit(&src, "one", 1_700_000_000);
    let url = src.join(".git");
    let url = url.to_str().unwrap();
    let id = "src-00000002";
    assert!(e.ws.fetch_pin(id, url, &"a".repeat(40), "x").is_err());
    e.ws.sync_mirror(id, url).unwrap();

    git(&src, &["checkout", "-q", "--detach"]);
    write(&src, "a", "detached work");
    let detached = commit(&src, "detached", 1_700_000_100);
    git(&src, &["checkout", "-q", "main"]);
    e.ws.sync_mirror(id, url).unwrap();
    assert!(!e.ws.has_commit(id, &detached));
    let err = format!("{:#}", e.ws.pin(id, &detached, "job").unwrap_err());
    assert!(err.contains("doesn't have commit"), "{err}");

    e.ws.fetch_pin(id, url, &detached, "job/studio-1").unwrap();
    assert!(e.ws.has_commit(id, &detached));
    assert_eq!(
        refs(&e.mirror(id), &["refs/potluck"]).get("refs/potluck/pins/job/studio-1"),
        Some(&detached)
    );
    // A commit the source doesn't have.
    assert!(e.ws.fetch_pin(id, url, &"b".repeat(40), "y").is_err());
    assert!(e.ws.pin(id, &detached, "../escape").is_err());
    // A repository id that isn't one path component is refused before anything is made for it.
    let outside = e.path("escape.lock");
    assert!(e.ws.pin("../../escape", &detached, "z").is_err());
    assert!(!outside.exists(), "{} was created", outside.display());
}

#[test]
fn corrupt_mirror_is_reported_and_recloned_keeping_worktrees_and_pins() {
    let e = env();
    let src = e.path("src");
    init(&src);
    for i in 0..20 {
        write(&src, &format!("d/f{i}"), &format!("file {i}\n"));
    }
    commit(&src, "one", 1_700_000_000);
    let repo0 = gitinfo::inspect(&src).unwrap();
    e.sync(&repo0);

    // A commit on no branch, pinned; and a commit only the mirror has, pinned.
    git(&src, &["checkout", "-q", "--detach"]);
    write(&src, "d/f1", "detached\n");
    let detached = commit(&src, "detached", 1_700_000_050);
    git(&src, &["checkout", "-q", "main"]);
    let url = repo0.source.to_str().unwrap();
    e.ws.fetch_pin(&repo0.id, url, &detached, "detached")
        .unwrap();
    let mirror = e.mirror(&repo0.id);
    let tree = git(&mirror, &["rev-parse", &format!("{detached}^{{tree}}")]);
    let orphan = git(&mirror, &["commit-tree", &tree, "-m", "only in the mirror"]);
    e.ws.pin(&repo0.id, &orphan, "orphan").unwrap();

    // A new commit arrives by fetch, so its objects are the mirror's own loose files.
    write(&src, "d/f2", "two\n");
    let c2 = commit(&src, "two", 1_700_000_100);
    let repo = gitinfo::inspect(&src).unwrap();
    e.sync(&repo);
    let root = e.ws.create(&"t-w1".to_string(), &repo).unwrap();

    let obj = mirror.join(format!("objects/{}/{}", &c2[..2], &c2[2..]));
    let meta = std::fs::metadata(&obj).expect("the fetched commit is a loose object");
    assert_eq!(meta.nlink(), 1, "never write into a file the source shares");
    std::fs::set_permissions(&obj, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::write(&obj, b"garbage, not zlib").unwrap();

    let report = e.ws.sync_mirror(&repo.id, url).unwrap();
    let why = report.recloned.clone().expect("corruption is reported");
    assert!(why.contains("git fsck --full"), "{why}");
    let warning = report.warning(&repo.id).unwrap();
    assert!(
        warning.contains("recloned") && warning.contains("orphan"),
        "{warning}"
    );
    assert_eq!(report.lost_pins.len(), 1, "{:?}", report.lost_pins);
    assert!(report.lost_pins[0].contains("refs/potluck/pins/orphan"));

    // The new mirror is whole, matches the source, and kept pins, base and workspace.
    git(&mirror, &["fsck", "--full", "--no-dangling"]);
    assert_eq!(
        refs(&mirror, &["refs/heads", "refs/tags"]),
        refs(&src, &["refs/heads", "refs/tags"])
    );
    let potluck_refs = refs(&mirror, &["refs/potluck"]);
    assert_eq!(
        potluck_refs.get("refs/potluck/pins/detached"),
        Some(&detached)
    );
    assert_eq!(potluck_refs.get("refs/potluck/pins/t-w1"), Some(&c2));
    assert!(potluck_refs.contains_key("refs/potluck/base"));
    e.ws.verify(&root, &repo).unwrap();
    assert_eq!(git(&root, &["status", "--porcelain"]), "");
    assert_eq!(std::fs::read_to_string(root.join("d/f2")).unwrap(), "two\n");
    let siblings: Vec<String> = std::fs::read_dir(mirror.parent().unwrap())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        siblings
            .iter()
            .all(|n| !n.contains(".new") && !n.contains("broken")),
        "{siblings:?}"
    );

    // A sync after that is quiet; a workspace can still be made and removed.
    assert_eq!(e.ws.sync_mirror(&repo.id, url).unwrap(), Default::default());
    let root2 = e.ws.create(&"t-w2".to_string(), &repo).unwrap();
    e.ws.remove(&e.record("t-w2", &repo, &root2)).unwrap();
    e.ws.remove(&e.record("t-w1", &repo, &root)).unwrap();
}

/// Moves `path`'s modification time back by `secs`.
fn age(path: &Path, secs: u64) {
    let then = std::time::SystemTime::now() - Duration::from_secs(secs);
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(then)
        .unwrap();
}

/// The files beside a mirror (a reclone's `.new` and `.broken-*`).
fn mirror_siblings(mirror: &Path) -> Vec<String> {
    let name = mirror.file_name().unwrap().to_string_lossy().into_owned();
    std::fs::read_dir(mirror.parent().unwrap())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&name) && *n != name)
        .collect()
}

/// A git killed while it updated a mirror's refs (the daemon killed outright, a power cut) leaves
/// lock files that fail every fetch after it. Over a minute old they are a dead git's: they go and
/// the sync goes on. A younger one may be a job's git at work in a workspace (worktrees share the
/// mirror's refs): the sync fails and leaves it. Neither is damage: nothing is recloned.
#[test]
fn locks_left_by_a_killed_git_go_and_nothing_is_recloned() {
    // (A HEAD.lock fails a fetch only where HEAD keeps a reflog: mirrors keep none.)
    for lock in ["refs/heads/main.lock", "packed-refs.lock"] {
        let e = env();
        let src = e.path("src");
        init(&src);
        write(&src, "a", "1");
        commit(&src, "one", 1_700_000_000);
        git(&src, &["branch", "gone"]);
        let url = src.join(".git");
        let url = url.to_str().unwrap();
        let id = "src-00000003";
        let mirror = e.mirror(id);
        e.ws.sync_mirror(id, url).unwrap();
        let before = refs(&mirror, &["refs/heads"]);
        // A new commit on main and a branch deleted: the fetch updates one ref and prunes one.
        write(&src, "a", "2");
        commit(&src, "two", 1_700_000_100);
        git(&src, &["branch", "-D", "gone"]);
        std::fs::write(mirror.join(lock), "").unwrap();

        let err = match e.ws.sync_mirror(id, url) {
            Ok(r) => panic!("{lock}: the sync went through: {r:?}"),
            Err(err) => format!("{err:#}"),
        };
        assert!(
            err.contains(&format!("can't update the mirror of {id}"))
                && err.contains("finds nothing wrong")
                && err.contains(".lock"),
            "{lock}: {err}"
        );
        assert!(mirror.join(lock).exists(), "{lock}: a young lock is left");
        // What the lock guards wasn't changed: main, or (packed-refs) the deletion of gone.
        let held = if lock == "packed-refs.lock" {
            "refs/heads/gone"
        } else {
            "refs/heads/main"
        };
        assert_eq!(
            refs(&mirror, &["refs/heads"]).get(held),
            before.get(held),
            "{lock}"
        );
        assert!(mirror_siblings(&mirror).is_empty(), "{lock}");

        age(&mirror.join(lock), 120);
        let report = e.ws.sync_mirror(id, url).unwrap();
        assert_eq!(report, Default::default(), "{lock}");
        assert!(!mirror.join(lock).exists(), "{lock}");
        assert_eq!(
            refs(&mirror, &["refs/heads"]),
            refs(&src, &["refs/heads"]),
            "{lock}"
        );

        // A source that can't be read is an error, not a reclone.
        let gone = e.path("missing/.git");
        assert!(e.ws.sync_mirror(id, gone.to_str().unwrap()).is_err());
        assert!(mirror.is_dir() && mirror_siblings(&mirror).is_empty());
    }
}

/// A runner's fetch over the mesh whose connection drops mid-pack is tried again; the mirror is
/// neither checked in full nor recloned (all of a repository again, over the network).
#[test]
fn a_transfer_cut_short_is_tried_again_not_recloned() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1");
    commit(&src, "one", 1_700_000_000);
    let id = "src-0123456789abcdef";
    let mirror = e.mirror(id);
    e.ws.sync_mirror(id, src.join(".git").to_str().unwrap())
        .unwrap();
    // 256 KB that doesn't compress, so the pack is cut halfway.
    let mut data = vec![0u8; 256 << 10];
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for b in data.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    std::fs::write(src.join("blob.bin"), &data).unwrap();
    let c2 = commit(&src, "two", 1_700_000_100);

    // Stands in for `potluck _tunnel`: serves the source, cutting the first connection short
    // (`dd bs=1` passes each byte on at once; `head` would hold back git's first lines).
    let calls = e.path("calls");
    let tunnel = e.path("tunnel");
    std::fs::write(
        &tunnel,
        format!(
            "#!/bin/sh\nn=$(cat {calls} 2>/dev/null || echo 0); n=$((n+1)); echo $n > {calls}\n\
             if [ $n = 1 ]; then git upload-pack {src} | dd bs=1 count=100000 2>/dev/null; exit 1; fi\n\
             exec git upload-pack {src}\n",
            calls = calls.display(),
            src = src.join(".git").display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&tunnel, std::fs::Permissions::from_mode(0o755)).unwrap();
    let url = format!("ext::{} _tunnel -- studio {id}", tunnel.display());
    assert!(potluck::tunnel::is_tunnel_url(&url), "{url}");

    let report = e.ws.sync_mirror(id, &url).unwrap();
    assert_eq!(report, Default::default());
    assert_eq!(refs(&mirror, &["refs/heads"])["refs/heads/main"], c2);
    // The fetch, the source checked, the fetch again: no reclone.
    assert_eq!(std::fs::read_to_string(&calls).unwrap().trim(), "3");
    assert!(mirror_siblings(&mirror).is_empty());
}

/// A sync whose fetch fails while git fsck finds the mirror sound keeps what had diverged before
/// it for the next sync to report, and takes what its fetch did update as its own.
#[test]
fn a_divergence_outlives_a_fetch_that_fails() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1");
    let c1 = commit(&src, "one", 1_700_000_000);
    git(&src, &["branch", "side"]);
    let url = src.join(".git");
    let url = url.to_str().unwrap();
    let id = "src-00000004";
    let mirror = e.mirror(id);
    e.ws.sync_mirror(id, url).unwrap();
    // What a job running `git branch scratch` in its workspace does to the mirror.
    git(&mirror, &["branch", "scratch", &c1]);
    // The source moves both branches; a lock held on main fails that one ref.
    git(&src, &["checkout", "-q", "side"]);
    write(&src, "a", "side");
    let side = commit(&src, "side", 1_700_000_100);
    git(&src, &["checkout", "-q", "main"]);
    write(&src, "a", "2");
    commit(&src, "two", 1_700_000_200);
    std::fs::write(mirror.join("refs/heads/main.lock"), "").unwrap();
    assert!(e.ws.sync_mirror(id, url).is_err());
    // The fetch updated side before it failed on main.
    assert_eq!(refs(&mirror, &["refs/heads"])["refs/heads/side"], side);
    std::fs::remove_file(mirror.join("refs/heads/main.lock")).unwrap();
    let report = e.ws.sync_mirror(id, url).unwrap();
    assert_eq!(
        report.diverged,
        [format!(
            "refs/heads/scratch was made here, at {}",
            &c1[..12]
        )]
    );
    assert_eq!(refs(&mirror, &["refs/heads"]), refs(&src, &["refs/heads"]));
}

/// A reclone builds the new clone beside the mirror (`<mirror>.new`), moves the mirror's worktrees
/// into it, moves the old mirror aside (`<mirror>.broken-<ms>`), moves the new one in and removes
/// the old. A daemon stopped between any two of those steps leaves the next sync to finish or undo
/// it: the workspaces and pins survive, and no clone is left behind.
#[test]
fn a_reclone_cut_short_is_finished_or_undone_at_the_next_sync() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1\n");
    commit(&src, "one", 1_700_000_000);
    git(&src, &["checkout", "-q", "--detach"]);
    write(&src, "a", "detached\n");
    let detached = commit(&src, "detached", 1_700_000_050);
    git(&src, &["checkout", "-q", "main"]);
    let repo = gitinfo::inspect(&src).unwrap();
    e.sync(&repo);
    let url = repo.source.to_str().unwrap();
    e.ws.fetch_pin(&repo.id, url, &detached, "detached")
        .unwrap();
    let root = e.ws.create(&"t-w1".to_string(), &repo).unwrap();
    let mirror = e.mirror(&repo.id);
    let aside = |n: &str| PathBuf::from(format!("{}.{n}", mirror.display()));
    let siblings = || -> Vec<String> {
        let name = mirror.file_name().unwrap().to_string_lossy().into_owned();
        let mut v: Vec<String> = std::fs::read_dir(mirror.parent().unwrap())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(&name) && *n != name)
            .collect();
        v.sort();
        v
    };
    let check = |what: &str| {
        e.ws.sync_mirror(&repo.id, url)
            .unwrap_or_else(|err| panic!("{what}: {err:#}"));
        assert!(siblings().is_empty(), "{what}: {:?}", siblings());
        e.ws.verify(&root, &repo)
            .unwrap_or_else(|err| panic!("{what}: {err:#}"));
        assert_eq!(git(&root, &["status", "--porcelain"]), "", "{what}");
        let pins = refs(&mirror, &["refs/potluck"]);
        assert_eq!(
            pins.get("refs/potluck/pins/detached"),
            Some(&detached),
            "{what}"
        );
        assert!(pins.contains_key("refs/potluck/pins/t-w1"), "{what}");
    };

    // Stopped after the worktrees moved into the new clone, before the swap.
    cow::clone_tree(&mirror, &aside("new")).unwrap();
    std::fs::remove_dir_all(aside("new").join("worktrees")).unwrap();
    std::fs::rename(mirror.join("worktrees"), aside("new").join("worktrees")).unwrap();
    check("worktrees moved");

    // Stopped with the old mirror moved aside and the new one not yet in its place.
    cow::clone_tree(&mirror, &aside("new")).unwrap();
    std::fs::rename(&mirror, aside("broken-1")).unwrap();
    std::fs::remove_dir_all(aside("broken-1").join("worktrees")).unwrap();
    check("old mirror moved aside");

    // Stopped while making the new clone, or while removing the old mirror.
    cow::clone_tree(&mirror, &aside("new")).unwrap();
    std::fs::remove_dir_all(aside("new").join("worktrees")).unwrap();
    cow::clone_tree(&mirror, &aside("broken-2")).unwrap();
    check("leftover clones");

    // A first clone cut short beside an old mirror moved aside (the mirror was then removed by
    // hand): the mirror is made anew.
    std::fs::rename(&mirror, aside("broken-3")).unwrap();
    std::fs::create_dir(aside("new")).unwrap();
    std::fs::write(aside("new").join("HEAD"), "ref: refs/heads/main\n").unwrap();
    let report = e.ws.sync_mirror(&repo.id, url).unwrap();
    assert!(report.created, "{report:?}");
    assert!(siblings().is_empty(), "{:?}", siblings());
    assert_eq!(refs(&mirror, &["refs/heads"]), refs(&src, &["refs/heads"]));
}

/// Mirrors are bare repositories, which git finds by itself only while `safe.bareRepository` is
/// `all`: `explicit` is git's advice for a global config, and the default from Git 3.0.
#[test]
fn mirrors_work_with_safe_bare_repository_explicit() {
    // GIT_CONFIG_GLOBAL is process-wide: run this test again, alone, in a child that has it.
    const CHILD: &str = "POTLUCK_TEST_BARE_EXPLICIT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let t = tmp();
        let cfg = t.path().join("gitconfig");
        std::fs::write(&cfg, "[safe]\n\tbareRepository = explicit\n").unwrap();
        let out = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "mirrors_work_with_safe_bare_repository_explicit",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env("GIT_CONFIG_GLOBAL", &cfg)
            .output()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.status.success() && text.contains("1 passed"), "{text}");
        return;
    }
    let e = env();
    let (app, _lib, _inner) = submodule_fixture(&e.path("src"));
    let repo1 = gitinfo::inspect(&app).unwrap();
    e.sync(&repo1);
    write(&app, "src/app.c", "int main() { return 1; }\n");
    commit(&app, "app 2", 1_700_000_100);
    let repo = gitinfo::inspect(&app).unwrap();
    e.sync(&repo);
    let root = e.ws.create(&"t-w1".to_string(), &repo).unwrap();
    e.ws.verify(&root, &repo).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("vendor/lib/inner/inner.txt")).unwrap(),
        "inner 1\n"
    );
    e.ws.remove(&e.record("t-w1", &repo, &root)).unwrap();
    assert!(!root.exists());
    for id in std::iter::once(&repo.id).chain(repo.submodules.iter().map(|s| &s.repo_id)) {
        assert!(e.workspace_pins(id).is_empty(), "{id}");
    }
}

/// A mirror directory that git no longer takes for a repository (here: its HEAD is gone) is
/// recloned, and git never looks for a repository above it: the state directory may sit inside
/// one (a home directory kept in git, or these tests' temp dirs inside potluck's own checkout).
#[test]
fn a_broken_mirror_never_touches_a_repository_around_the_state_directory() {
    let e = env();
    let outer = e.dir.path();
    init(outer);
    write(outer, "outer.txt", "outer\n");
    commit(outer, "outer", 1_700_000_000);
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1\n");
    commit(&src, "one", 1_700_000_100);
    git(&src, &["branch", "other"]);
    let repo = gitinfo::inspect(&src).unwrap();
    e.sync(&repo);
    let before = refs(outer, &[]);

    let mirror = e.mirror(&repo.id);
    std::fs::remove_file(mirror.join("HEAD")).unwrap();
    let report =
        e.ws.sync_mirror(&repo.id, repo.source.to_str().unwrap())
            .unwrap();
    assert_eq!(refs(outer, &[]), before, "the repository around it changed");
    assert!(report.recloned.is_some(), "{report:?}");
    assert_eq!(refs(&mirror, &["refs/heads"]), refs(&src, &["refs/heads"]));
    assert!(e.ws.has_commit(&repo.id, &repo.commit));
}

// --- seeding ---------------------------------------------------------------------------------

/// (inode, mtime ns) per file under `dir`, skipping `.git`.
fn stamps(dir: &Path) -> BTreeMap<String, (u64, i128)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if e.file_name() == ".git" {
                continue;
            }
            let m = p.symlink_metadata().unwrap();
            if m.is_dir() {
                stack.push(p);
            } else {
                let rel = p.strip_prefix(dir).unwrap().to_string_lossy().into_owned();
                let mtime = m.mtime() as i128 * 1_000_000_000 + m.mtime_nsec() as i128;
                out.insert(rel, (m.ino(), mtime));
            }
        }
    }
    out
}

#[test]
fn seeding_writes_only_what_differs_from_the_base() {
    let e = env();
    let src = e.path("src");
    init(&src);
    for i in 0..40 {
        write(
            &src,
            &format!("dir{}/file{i}.txt", i % 4),
            &format!("content {i}\n"),
        );
    }
    write(&src, "run.sh", "#!/bin/sh\necho hi\n");
    std::fs::set_permissions(src.join("run.sh"), std::fs::Permissions::from_mode(0o755)).unwrap();
    std::os::unix::fs::symlink("dir0/file0.txt", src.join("link")).unwrap();
    let c1 = commit(&src, "one", 1_700_000_000);
    write(&src, "dir1/file1.txt", "changed 1\n");
    write(&src, "dir2/file2.txt", "changed 2\n");
    std::fs::remove_file(src.join("dir3/file3.txt")).unwrap();
    write(&src, "dir0/added.txt", "added\n");
    let c2 = commit(&src, "two", 1_700_000_100);
    let changed = ["dir1/file1.txt", "dir2/file2.txt", "dir0/added.txt"];

    let repo2 = gitinfo::inspect(&src).unwrap();
    e.sync(&repo2);
    let repo1 = RepoRef {
        commit: c1.clone(),
        ..repo2.clone()
    };
    let base = e.ws.paths.base(&repo1.id);

    // Cold: the base is made at c1, the workspace cloned from it.
    let t = Instant::now();
    let w1 = e.ws.create(&"t-w1".to_string(), &repo1).unwrap();
    eprintln!("cold seeding: {:?}", t.elapsed());
    assert_eq!(w1, e.ws.paths.workspace("t-w1"));
    assert!(e.ws.paths.workspace_tmp("t-w1").is_dir());
    assert_eq!(git(&w1, &["rev-parse", "HEAD"]), c1);
    assert_eq!(git(&w1, &["status", "--porcelain"]), "");
    assert_eq!(
        std::fs::read_to_string(w1.join("dir3/file3.txt")).unwrap(),
        "content 3\n"
    );
    assert!(!w1.join("dir0/added.txt").exists());
    // potluck's own files in a tree never show as untracked.
    write(&w1, ".potluck/inv-1/log", "output");
    assert_eq!(git(&w1, &["status", "--porcelain"]), "");
    std::fs::remove_dir_all(w1.join(".potluck")).unwrap();
    let mode = std::fs::metadata(w1.join("run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o755);
    assert_eq!(
        std::fs::read_link(w1.join("link")).unwrap(),
        Path::new("dir0/file0.txt")
    );
    let base_at_c1 = stamps(&base);
    let w1_stamps = stamps(&w1);
    for (f, (ino, mtime)) in &w1_stamps {
        assert_eq!(base_at_c1[f].1, *mtime, "{f}: cloned with the base's mtime");
        assert_ne!(base_at_c1[f].0, *ino, "{f}: a clone, not the base's file");
    }

    // A newer commit: the base moves to it, rewriting only the files that differ; the workspace
    // is then cloned and git writes nothing in it.
    let w2 = e.ws.create(&"t-w2".to_string(), &repo2).unwrap();
    assert_eq!(git(&w2, &["rev-parse", "HEAD"]), c2);
    assert_eq!(git(&w2, &["status", "--porcelain"]), "");
    let base_at_c2 = stamps(&base);
    for (f, (ino, mtime)) in &base_at_c2 {
        let unchanged = !changed.contains(&f.as_str());
        if unchanged {
            assert_eq!(base_at_c1[f], (*ino, *mtime), "{f}: left alone in the base");
        } else if let Some(before) = base_at_c1.get(f) {
            assert_ne!(*before, (*ino, *mtime), "{f}: rewritten in the base");
        }
    }
    assert!(!base_at_c2.contains_key("dir3/file3.txt"));
    for (f, (_, mtime)) in stamps(&w2) {
        assert_eq!(base_at_c2[&f].1, mtime, "{f}: cloned, not written");
    }
    assert_eq!(
        std::fs::read_to_string(w2.join("dir1/file1.txt")).unwrap(),
        "changed 1\n"
    );
    assert!(!w2.join("dir3/file3.txt").exists());

    // An older commit: the base stays at c2 and git writes only the difference.
    let w3 = e.ws.create(&"t-w3".to_string(), &repo1).unwrap();
    assert_eq!(git(&base, &["rev-parse", "HEAD"]), c2);
    assert_eq!(git(&w3, &["rev-parse", "HEAD"]), c1);
    assert_eq!(git(&w3, &["status", "--porcelain"]), "");
    let w3_stamps = stamps(&w3);
    let mut written: Vec<&str> = w3_stamps
        .iter()
        .filter(|(f, (_, mtime))| base_at_c2.get(*f).map(|b| b.1) != Some(*mtime))
        .map(|(f, _)| f.as_str())
        .collect();
    written.sort();
    assert_eq!(
        written,
        ["dir1/file1.txt", "dir2/file2.txt", "dir3/file3.txt"]
    );
    assert!(!w3.join("dir0/added.txt").exists());
    assert_eq!(
        std::fs::read_to_string(w3.join("dir2/file2.txt")).unwrap(),
        "content 2\n"
    );

    // Workspaces are independent: a change in one doesn't reach the base or another.
    write(&w2, "dir0/file0.txt", "job output");
    assert_eq!(
        std::fs::read_to_string(base.join("dir0/file0.txt")).unwrap(),
        "content 0\n"
    );
    assert_eq!(
        std::fs::read_to_string(w3.join("dir0/file0.txt")).unwrap(),
        "content 0\n"
    );

    // The size counts the tree and the TMPDIR.
    let before = e.ws.size(&w1).unwrap();
    assert!(before > 0);
    std::fs::write(
        e.ws.paths.workspace_tmp("t-w1").join("big"),
        vec![7u8; 1 << 20],
    )
    .unwrap();
    assert!(e.ws.size(&w1).unwrap() >= before + (1 << 20));

    // An existing workspace id is refused.
    assert!(e.ws.create(&"t-w1".to_string(), &repo1).is_err());

    for (id, root, repo) in [
        ("t-w1", &w1, &repo1),
        ("t-w2", &w2, &repo2),
        ("t-w3", &w3, &repo1),
    ] {
        e.ws.remove(&e.record(id, repo, root)).unwrap();
        assert!(!root.exists() && !e.ws.paths.workspace_tmp(id).exists());
    }
    let mirror = e.mirror(&repo1.id);
    assert_eq!(
        worktrees(&mirror),
        vec![
            std::fs::canonicalize(&base)
                .unwrap()
                .to_string_lossy()
                .into_owned()
        ]
    );
    assert!(e.workspace_pins(&repo1.id).is_empty());
    assert!(refs(&mirror, &["refs/potluck"]).contains_key("refs/potluck/base"));
}

#[test]
fn seeding_with_nested_submodules() {
    let e = env();
    let (app, lib, _inner) = submodule_fixture(&e.path("src"));
    let repo1 = gitinfo::inspect(&app).unwrap();
    e.sync(&repo1);
    let w1 = e.ws.create(&"t-w1".to_string(), &repo1).unwrap();
    e.ws.verify(&w1, &repo1).unwrap();
    assert_eq!(
        git(&w1, &["status", "--porcelain", "--ignore-submodules=none"]),
        ""
    );
    assert_eq!(
        std::fs::read_to_string(w1.join("vendor/lib/lib.c")).unwrap(),
        "int lib = 1;\n"
    );
    assert_eq!(
        std::fs::read_to_string(w1.join("vendor/lib/inner/inner.txt")).unwrap(),
        "inner 1\n"
    );
    // Each submodule is a worktree of its own mirror: no .git directory anywhere.
    for d in ["", "vendor/lib", "vendor/lib/inner"] {
        assert!(w1.join(d).join(".git").is_file(), "{d}");
    }
    let lib_mirror = e.mirror(&repo1.submodules[0].repo_id);
    assert!(
        worktrees(&lib_mirror)
            .iter()
            .any(|w| w.ends_with("t-w1/vendor/lib"))
    );

    // A new lib commit, taken into the app.
    write(&lib, "lib.c", "int lib = 2;\n");
    commit(&lib, "lib 2", 1_700_000_100);
    let app_lib = app.join("vendor/lib");
    git(&app_lib, &["fetch", "-q", "origin"]);
    git(&app_lib, &["checkout", "-q", "--detach", "origin/main"]);
    commit(&app, "app 2", 1_700_000_100);
    let repo2 = gitinfo::inspect(&app).unwrap();
    assert_ne!(repo2.submodules[0].commit, repo1.submodules[0].commit);
    e.sync(&repo2);
    let w2 = e.ws.create(&"t-w2".to_string(), &repo2).unwrap();
    e.ws.verify(&w2, &repo2).unwrap();
    assert_eq!(
        std::fs::read_to_string(w2.join("vendor/lib/lib.c")).unwrap(),
        "int lib = 2;\n"
    );
    assert_eq!(
        git(&w2, &["status", "--porcelain", "--ignore-submodules=none"]),
        ""
    );
    // The first workspace is untouched.
    e.ws.verify(&w1, &repo1).unwrap();
    assert!(e.ws.verify(&w1, &repo2).is_err());

    // verify catches a wrong HEAD, in the top and in a submodule.
    git(
        &w2.join("vendor/lib"),
        &["checkout", "-q", "--detach", "HEAD~1"],
    );
    let err = format!("{:#}", e.ws.verify(&w2, &repo2).unwrap_err());
    assert!(err.contains("submodule vendor/lib is at"), "{err}");
    git(
        &w2.join("vendor/lib"),
        &["checkout", "-q", "--detach", &repo2.submodules[0].commit],
    );
    e.ws.verify(&w2, &repo2).unwrap();
    git(&w2, &["checkout", "-q", "--detach", "HEAD~1"]);
    let err = format!("{:#}", e.ws.verify(&w2, &repo2).unwrap_err());
    assert!(err.contains("the workspace is at"), "{err}");

    // Removal leaves nothing: no trees, TMPDIRs, worktree entries or pins.
    // (A job removed one .git file: the tree still goes, and prune drops the stale entry.)
    std::fs::remove_file(w1.join("vendor/lib/inner/.git")).unwrap();
    e.ws.remove(&e.record("t-w1", &repo1, &w1)).unwrap();
    e.ws.remove(&e.record("t-w2", &repo2, &w2)).unwrap();
    for id in ["t-w1", "t-w2"] {
        assert!(!e.ws.paths.workspace(id).exists() && !e.ws.paths.workspace_tmp(id).exists());
    }
    let mut ids = vec![repo1.id.clone()];
    ids.extend(repo1.submodules.iter().map(|s| s.repo_id.clone()));
    for id in ids {
        let m = e.mirror(&id);
        let base = std::fs::canonicalize(e.ws.paths.base(&id)).unwrap();
        assert_eq!(
            worktrees(&m),
            vec![base.to_string_lossy().into_owned()],
            "{id}"
        );
        let wt_dirs: Vec<_> = std::fs::read_dir(m.join("worktrees")).unwrap().collect();
        assert_eq!(wt_dirs.len(), 1, "{id}");
        assert!(e.workspace_pins(&id).is_empty(), "{id}");
    }
    // Removing again is fine.
    e.ws.remove(&e.record("t-w1", &repo1, &w1)).unwrap();
}

/// Repository ids come from directory names: one that starts with a dot or has two dots in a row
/// still makes an id that mirrors, bases and workspaces accept.
#[test]
fn repositories_in_dot_directories() {
    let e = env();
    let top = e.path(".dotfiles");
    let sub = e.path("sub");
    init(&sub);
    write(&sub, "s", "s\n");
    commit(&sub, "sub", 1_700_000_000);
    init(&top);
    write(&top, "a", "1\n");
    git(
        &top,
        &[
            "submodule",
            "add",
            "-q",
            sub.to_str().unwrap(),
            "vendor/..hidden",
        ],
    );
    commit(&top, "one", 1_700_000_000);
    let repo = gitinfo::inspect(&top).unwrap();
    e.sync(&repo);
    let root = e.ws.create(&"t-w1".to_string(), &repo).unwrap();
    e.ws.verify(&root, &repo).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("vendor/..hidden/s")).unwrap(),
        "s\n"
    );
    e.ws.remove(&e.record("t-w1", &repo, &root)).unwrap();
}

#[test]
fn create_refuses_commits_the_mirror_lacks_and_leaves_nothing() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1");
    commit(&src, "one", 1_700_000_000);
    let repo = gitinfo::inspect(&src).unwrap();
    let err = format!("{:#}", e.ws.create(&"t-w1".to_string(), &repo).unwrap_err());
    assert!(
        err.contains(&format!(
            "the mirror of {} doesn't have commit {} (the workspace)",
            repo.id,
            &repo.commit[..12]
        )),
        "{err}"
    );
    e.sync(&repo);
    let bad = RepoRef {
        commit: "c".repeat(40),
        ..repo.clone()
    };
    assert!(e.ws.create(&"t-w1".to_string(), &bad).is_err());
    assert!(!e.ws.paths.workspace("t-w1").exists());
    assert!(e.ws.create(&"../x".to_string(), &repo).is_err());
}

/// POTLUCK_HOME may be a relative path. Every git potluck runs changes into a repository first
/// (`git -C`), so a relative path handed to git would resolve against the wrong directory.
#[test]
fn relative_state_directory() {
    let t = tmp();
    let cwd = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
    let dir = std::fs::canonicalize(t.path()).unwrap();
    let (a, b): (Vec<_>, Vec<_>) = (cwd.components().collect(), dir.components().collect());
    let common = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
    // Up and back into the current directory first, so the path always has a `..` in it.
    let mut rel = PathBuf::from("..").join(cwd.file_name().unwrap());
    for _ in common..a.len() {
        rel.push("..");
    }
    rel.extend(&b[common..]);
    let ws = Workspaces::new(Paths {
        home: rel.join("state"),
        config: rel.join("node.toml"),
    });

    let src = dir.join("src");
    init(&src);
    write(&src, "a", "1\n");
    commit(&src, "one", 1_700_000_000);
    let repo = gitinfo::inspect(&src).unwrap();
    let url = repo.source.to_str().unwrap();
    let report = ws.sync_mirror(&repo.id, url).unwrap();
    assert!(report.created, "{report:?}");
    let root = ws.create(&"t-w1".to_string(), &repo).unwrap();
    assert!(root.is_absolute(), "{}", root.display());
    let root = std::fs::canonicalize(root).unwrap();
    assert_eq!(root, dir.join("state/workspaces/t-w1"));
    ws.verify(&root, &repo).unwrap();
    assert_eq!(std::fs::read_to_string(root.join("a")).unwrap(), "1\n");
    let mirror = dir.join(format!("state/mirrors/{}.git", repo.id));
    let base = dir.join(format!("state/bases/{}", repo.id));
    assert_eq!(
        worktrees(&mirror),
        vec![
            base.to_string_lossy().into_owned(),
            root.to_string_lossy().into_owned()
        ]
    );
    let record = WorkspaceRecord {
        id: "t-w1".into(),
        node: "test".into(),
        repo_id: repo.id.clone(),
        commit: repo.commit.clone(),
        branch: repo.branch.clone(),
        path: root.clone(),
        state: WorkspaceState::Idle,
        created_at: 0,
        last_used_at: 0,
        held_until: 0,
        pending: 0,
        size_bytes: None,
    };
    ws.remove(&record).unwrap();
    assert!(!root.exists());
    assert_eq!(
        worktrees(&mirror),
        vec![base.to_string_lossy().into_owned()]
    );
}

#[test]
fn concurrent_creates_share_one_base() {
    let e = env();
    let src = e.path("src");
    init(&src);
    let mut commits = Vec::new();
    for i in 0..4 {
        write(&src, &format!("f{i}"), &format!("{i}\n"));
        write(&src, "common", &format!("round {i}\n"));
        commits.push(commit(
            &src,
            &format!("c{i}"),
            1_700_000_000 + i as u64 * 10,
        ));
    }
    let head = gitinfo::inspect(&src).unwrap();
    e.sync(&head);
    let repos: Vec<RepoRef> = (0..8)
        .map(|i| RepoRef {
            commit: commits[i % 4].clone(),
            ..head.clone()
        })
        .collect();
    std::thread::scope(|s| {
        for (i, repo) in repos.iter().enumerate() {
            let ws = &e.ws;
            s.spawn(move || {
                let root = ws
                    .create(&format!("t-c{i}"), repo)
                    .unwrap_or_else(|e| panic!("{e:#}"));
                ws.verify(&root, repo).unwrap();
                assert_eq!(git(&root, &["status", "--porcelain"]), "");
                assert_eq!(
                    std::fs::read_to_string(root.join("common")).unwrap(),
                    format!("round {}\n", i % 4)
                );
            });
        }
    });
    for (i, repo) in repos.iter().enumerate() {
        let id = format!("t-c{i}");
        let root = e.ws.paths.workspace(&id);
        e.ws.remove(&e.record(&id, repo, &root)).unwrap();
    }
    assert_eq!(worktrees(&e.mirror(&head.id)).len(), 1);
}

/// Every git potluck runs reads the user's global git config. With `core.splitIndex` there, the
/// base checkout's index would be split, and a workspace's copy of it would point at a shared index
/// file in the base's git dir that the workspace can't reach.
#[test]
fn seeding_ignores_a_global_split_index() {
    // GIT_CONFIG_GLOBAL is process-wide: run this test again, alone, in a child that has it.
    const CHILD: &str = "POTLUCK_TEST_SPLIT_INDEX_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let t = tmp();
        let cfg = t.path().join("gitconfig");
        std::fs::write(&cfg, "[core]\n\tsplitIndex = true\n").unwrap();
        let out = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "seeding_ignores_a_global_split_index",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env("GIT_CONFIG_GLOBAL", &cfg)
            .output()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.status.success() && text.contains("1 passed"), "{text}");
        return;
    }
    let e = env();
    let src = e.path("src");
    init(&src);
    for i in 0..10 {
        write(&src, &format!("f{i}"), &format!("{i}\n"));
    }
    let c1 = commit(&src, "one", 1_700_000_000);
    write(&src, "f1", "changed\n");
    commit(&src, "two", 1_700_000_100);
    let repo2 = gitinfo::inspect(&src).unwrap();
    e.sync(&repo2);
    let repo1 = RepoRef {
        commit: c1,
        ..repo2.clone()
    };
    // Cold base, base moved forward, base ahead of the workspace.
    for (id, repo) in [("t-w1", &repo1), ("t-w2", &repo2), ("t-w3", &repo1)] {
        let root =
            e.ws.create(&id.to_string(), repo)
                .unwrap_or_else(|err| panic!("{id}: {err:#}"));
        e.ws.verify(&root, repo).unwrap();
        assert_eq!(git(&root, &["status", "--porcelain"]), "", "{id}");
    }
}

/// A job's `git submodule init` in its workspace writes `submodule.<name>.active` into the
/// mirror's config, which every worktree of the mirror shares. With `submodule.recurse` in the
/// user's global config, git would then recurse into that submodule in potluck's own checkouts
/// (the base's `reset --hard`, the `read-tree -m -u` that moves a base or a workspace), where it
/// has no git dir of its own: every later seeding of the repository failed.
#[test]
fn a_global_submodule_recurse_and_a_job_s_submodule_init_break_no_seeding() {
    // GIT_CONFIG_GLOBAL is process-wide: run this test again, alone, in a child that has it.
    const CHILD: &str = "POTLUCK_TEST_SUBMODULE_RECURSE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let t = tmp();
        let cfg = t.path().join("gitconfig");
        std::fs::write(&cfg, "[submodule]\n\trecurse = true\n").unwrap();
        let out = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_global_submodule_recurse_and_a_job_s_submodule_init_break_no_seeding",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env("GIT_CONFIG_GLOBAL", &cfg)
            .output()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.status.success() && text.contains("1 passed"), "{text}");
        return;
    }
    let e = env();
    let (app, lib, _inner) = submodule_fixture(&e.path("src"));
    let repo1 = gitinfo::inspect(&app).unwrap();
    // A second app commit that moves the gitlink.
    write(&lib, "lib.c", "int lib = 2;\n");
    commit(&lib, "lib 2", 1_700_000_100);
    let app_lib = app.join("vendor/lib");
    git(&app_lib, &["fetch", "-q", "origin"]);
    git(&app_lib, &["checkout", "-q", "--detach", "origin/main"]);
    commit(&app, "app 2", 1_700_000_100);
    let repo2 = gitinfo::inspect(&app).unwrap();
    e.sync(&repo1);
    e.sync(&repo2);
    let w1 = e.ws.create(&"t-w1".to_string(), &repo1).unwrap();
    // A job's build step, with the user's global config.
    let out = Command::new("git")
        .arg("-C")
        .arg(&w1)
        .args(["submodule", "init"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let mirror = e.mirror(&repo1.id);
    assert!(
        std::fs::read_to_string(mirror.join("config"))
            .unwrap()
            .contains("active = true"),
        "the job's submodule init didn't reach the mirror's config"
    );
    // The base moves forward across the gitlink change; a workspace moves back across it; a base
    // made anew (a full checkout).
    for (id, repo, cold) in [
        ("t-w2", &repo2, false),
        ("t-w3", &repo1, false),
        ("t-w4", &repo2, true),
    ] {
        if cold {
            std::fs::remove_dir_all(e.ws.paths.base(&repo.id)).unwrap();
        }
        let root =
            e.ws.create(&id.to_string(), repo)
                .unwrap_or_else(|err| panic!("{id}: {err:#}"));
        e.ws.verify(&root, repo).unwrap();
        assert_eq!(
            git(
                &root,
                &["status", "--porcelain", "--ignore-submodules=none"]
            ),
            "",
            "{id}"
        );
        assert!(!mirror.join("potluck-suspect").exists(), "{id}");
    }
}

/// A Git LFS pointer file's text.
fn lfs_pointer(oid_byte: char, size: u64) -> String {
    format!(
        "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize {size}\n",
        oid_byte.to_string().repeat(64)
    )
}

#[test]
fn lfs_files_stay_pointers_and_count_as_clean() {
    // GIT_CONFIG_GLOBAL is process-wide: run this test again, alone, in a child that has a global
    // config routing files through a required lfs filter, as the owner's machines do. With
    // git-lfs installed it is the real filter (its smudge fails without an LFS remote); without
    // it, a filter that always fails stands in.
    const CHILD: &str = "POTLUCK_TEST_LFS_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let t = tmp();
        let cfg = t.path().join("gitconfig");
        let real = Command::new("git-lfs")
            .arg("version")
            .output()
            .is_ok_and(|o| o.status.success());
        let filter = if real {
            "[filter \"lfs\"]\n\tclean = git-lfs clean -- %f\n\tsmudge = git-lfs smudge -- %f\n\
             \tprocess = git-lfs filter-process\n\trequired = true\n"
        } else {
            eprintln!("git-lfs isn't installed: a failing stand-in filter is used");
            "[filter \"lfs\"]\n\tsmudge = false\n\tprocess = false\n\trequired = true\n"
        };
        std::fs::write(&cfg, filter).unwrap();
        let out = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lfs_files_stay_pointers_and_count_as_clean",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("GIT_CONFIG_GLOBAL", &cfg)
            .output()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.status.success() && text.contains("1 passed"), "{text}");
        return;
    }
    let e = env();
    let src = e.path("src");
    init(&src);
    write(
        &src,
        ".gitattributes",
        "*.bin filter=lfs diff=lfs merge=lfs -text\n",
    );
    write(&src, "data/big.bin", &lfs_pointer('a', 1 << 20));
    write(&src, "src/main.c", "int main() {}\n");
    // The test's git has no global config, so these blobs are the pointers, as git-lfs makes them.
    let c1 = commit(&src, "one", 1_700_000_000);
    write(&src, "data/big.bin", &lfs_pointer('b', 2 << 20));
    write(&src, "data/more.bin", &lfs_pointer('c', 3 << 20));
    commit(&src, "two", 1_700_000_100);
    let repo2 = gitinfo::inspect(&src).unwrap();
    e.sync(&repo2);
    let repo1 = RepoRef {
        commit: c1,
        ..repo2.clone()
    };

    // The filter bites: plain git with this global config can't check the commit out.
    let probe = e.path("probe");
    let plain = Command::new("git")
        .arg("-C")
        .arg(e.mirror(&repo2.id))
        .args(["worktree", "add", "--detach", "--quiet"])
        .arg(&probe)
        .arg(&repo2.commit)
        .output()
        .unwrap();
    assert!(!plain.status.success(), "the lfs filter should fail here");
    let _ = std::fs::remove_dir_all(&probe);
    git(&e.mirror(&repo2.id), &["worktree", "prune"]);

    // Cold base, base moved forward, base ahead of the workspace.
    for (id, repo, pointer) in [
        ("t-w1", &repo1, lfs_pointer('a', 1 << 20)),
        ("t-w2", &repo2, lfs_pointer('b', 2 << 20)),
        ("t-w3", &repo1, lfs_pointer('a', 1 << 20)),
    ] {
        let root =
            e.ws.create(&id.to_string(), repo)
                .unwrap_or_else(|err| panic!("{id}: {err:#}"));
        assert_eq!(
            std::fs::read_to_string(root.join("data/big.bin")).unwrap(),
            pointer,
            "{id}"
        );
        assert_eq!(git(&root, &["status", "--porcelain"]), "", "{id}");
        // Also clean to git with the lfs filter, as a job's git would see it.
        let with_filter = Command::new("git")
            .arg("-C")
            .arg(&root)
            .args(["status", "--porcelain"])
            .output()
            .unwrap();
        assert!(with_filter.status.success(), "{id}");
        assert_eq!(String::from_utf8_lossy(&with_filter.stdout), "", "{id}");
    }
    assert!(!e.mirror(&repo2.id).join("potluck-suspect").exists());
}

/// File names in decomposed Unicode (NFD), as a repository made on Linux may hold them. APFS keeps
/// such a name as written, but git on macOS (`core.precomposeUnicode`, on by default there) reads
/// names back composed (NFC), so it would take a checkout for an untracked file.
#[test]
fn decomposed_unicode_file_names_seed_clean() {
    let e = env();
    let src = e.path("src");
    init(&src);
    // Git takes the names exactly as given (as git on Linux always does).
    git(&src, &["config", "core.precomposeUnicode", "false"]);
    let names = ["cafe\u{301}.txt", "re\u{301}sume\u{301}/notes.txt"];
    for n in names {
        write(&src, n, "decomposed\n");
    }
    write(&src, "plain.txt", "1\n");
    let c1 = commit(&src, "one", 1_700_000_000);
    let listed = git(&src, &["-c", "core.quotePath=false", "ls-files"]);
    for n in names {
        assert!(listed.lines().any(|l| l == n), "{n:?} in {listed:?}");
    }
    write(&src, "plain.txt", "2\n");
    commit(&src, "two", 1_700_000_100);
    let repo2 = gitinfo::inspect(&src).unwrap();
    e.sync(&repo2);
    let repo1 = RepoRef {
        commit: c1,
        ..repo2.clone()
    };
    // Cold base, base moved forward, base ahead of the workspace.
    for (id, repo) in [("t-w1", &repo1), ("t-w2", &repo2), ("t-w3", &repo1)] {
        let root =
            e.ws.create(&id.to_string(), repo)
                .unwrap_or_else(|err| panic!("{id}: {err:#}"));
        assert_eq!(git(&root, &["status", "--porcelain"]), "", "{id}");
        assert_eq!(
            std::fs::read_to_string(root.join(names[0])).unwrap(),
            "decomposed\n"
        );
    }
    assert!(!e.mirror(&repo2.id).join("potluck-suspect").exists());
}

/// Whether the filesystem under `dir` looks names up without regard to case.
fn case_insensitive(dir: &Path) -> bool {
    let probe = dir.join("case-probe");
    std::fs::write(&probe, "").unwrap();
    let found = dir.join("CASE-PROBE").exists();
    std::fs::remove_file(&probe).unwrap();
    found
}

/// A repository whose first commit is ordinary and whose second, newer one adds `readme` beside
/// `README` (made through the index: a case-insensitive filesystem can't hold both). Returns the
/// RepoRefs of the two commits.
fn case_fixture(e: &Env) -> (RepoRef, RepoRef) {
    let src = e.path("src");
    init(&src);
    write(&src, "README", "upper\n");
    write(&src, "src/a.txt", "a\n");
    commit(&src, "one", 1_700_000_000);
    let repo1 = gitinfo::inspect(&src).unwrap();
    let blob = git(&src, &["hash-object", "-w", "src/a.txt"]);
    git(
        &src,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("100644,{blob},readme"),
        ],
    );
    git_at(&src, 1_700_000_100, &["commit", "-q", "-m", "two"]);
    let repo2 = RepoRef {
        commit: git(&src, &["rev-parse", "HEAD"]),
        ..repo1.clone()
    };
    e.sync(&repo2);
    (repo1, repo2)
}

#[test]
fn paths_that_differ_only_in_case_are_the_repository_s_doing() {
    let e = env();
    let (repo1, repo2) = case_fixture(&e);
    std::fs::create_dir_all(e.ws.paths.home.join("workspaces")).unwrap();
    let insensitive = case_insensitive(&e.ws.paths.home.join("workspaces"));
    let mirror = e.mirror(&repo1.id);
    let base = e.ws.paths.base(&repo1.id);
    let w1 = e.ws.create(&"t-w1".to_string(), &repo1).unwrap();
    let base_ino = std::fs::metadata(base.join("src/a.txt")).unwrap().ino();

    let w2 = e.ws.create(&"t-w2".to_string(), &repo2);
    if insensitive {
        let err = format!("{:#}", w2.unwrap_err());
        assert!(
            err.contains(
                "the commit has paths that differ only in case or Unicode normalisation: \
                 README, readme; this \
                 filesystem can't hold both"
            ),
            "{err}"
        );
        assert!(!mirror.join("potluck-suspect").exists());
        // The base stayed where it was, untouched.
        assert_eq!(git(&base, &["rev-parse", "HEAD"]), repo1.commit);
        assert_eq!(
            std::fs::metadata(base.join("src/a.txt")).unwrap().ino(),
            base_ino
        );
        assert!(!e.ws.paths.workspace("t-w2").exists());
    } else {
        let w2 = w2.unwrap();
        assert_eq!(git(&w2, &["status", "--porcelain"]), "");
        e.ws.remove(&e.record("t-w2", &repo2, &w2)).unwrap();
    }
    let w3 = e.ws.create(&"t-w3".to_string(), &repo1).unwrap();
    for (id, root) in [("t-w1", &w1), ("t-w3", &w3)] {
        e.ws.remove(&e.record(id, &repo1, root)).unwrap();
    }

    // Cold: the first base is made at the colliding commit. It can't hold that tree, so it goes,
    // still without blaming the mirror; the next seeding makes a good one.
    let e = env();
    let (repo1, repo2) = case_fixture(&e);
    let cold = e.ws.create(&"t-c1".to_string(), &repo2);
    let mirror = e.mirror(&repo1.id);
    if insensitive {
        let err = format!("{:#}", cold.unwrap_err());
        assert!(
            err.contains("differ only in case or Unicode normalisation: README, readme"),
            "{err}"
        );
        assert!(!mirror.join("potluck-suspect").exists());
        assert!(!e.ws.paths.base(&repo1.id).exists());
        let w = e.ws.create(&"t-c2".to_string(), &repo1).unwrap();
        assert_eq!(git(&w, &["status", "--porcelain"]), "");
    } else {
        cold.unwrap();
    }
}

/// Every file of a tree (not `.git`) and its bytes.
fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    stamps(dir)
        .into_keys()
        .map(|f| {
            let data = std::fs::read(dir.join(&f)).unwrap();
            (f, data)
        })
        .collect()
}

/// A repository whose files are committed first without attributes (`one`), then the attributes
/// that would store them differently (`two`), then a change elsewhere (`three`, newer). Filter
/// drivers (and `core.autocrlf` if asked) are set in the mirror's config, as the user's global
/// config would set them. Returns the RepoRefs of `two` and `three`.
fn normalised_fixture(e: &Env, autocrlf: bool) -> (RepoRef, RepoRef) {
    let src = e.path("src");
    init(&src);
    let files: &[(&str, &[u8])] = &[
        ("crlf.txt", b"a\r\nb\r\n"),
        ("lf.md", b"one\r\ntwo\r\n"),
        ("mixed.bat", b"x\r\ny\nz\r\n"),
        ("id.c", b"/* $Id: 0123456789abcdef $ */\n"),
        ("up.up", b"lower case\n"),
        ("trail.ws", b"trailing   \n"),
        ("plain.txt", b"plain\n"),
        ("sub/notes", b"n1\nn2\n"),
    ];
    for (name, data) in files {
        std::fs::create_dir_all(src.join(name).parent().unwrap()).unwrap();
        std::fs::write(src.join(name), data).unwrap();
    }
    commit(&src, "one", 1_700_000_000);
    write(
        &src,
        ".gitattributes",
        "*.txt text\n*.md eol=lf\n*.bat eol=crlf\n*.c ident\n*.up filter=upper\n*.ws filter=strip\n",
    );
    // Only the attributes: `git add -A` would store the files anew through them.
    git_at(&src, 1_700_000_100, &["add", ".gitattributes"]);
    git_at(&src, 1_700_000_100, &["commit", "-q", "-m", "two"]);
    let two = git(&src, &["rev-parse", "HEAD"]);
    write(&src, "plain.txt", "plain, three\n");
    git_at(&src, 1_700_000_200, &["add", "plain.txt"]);
    git_at(&src, 1_700_000_200, &["commit", "-q", "-m", "three"]);
    // By hand: the source's own tree shows those files as modified too.
    let repo3 = RepoRef {
        id: "norm-0123456789abcdef".into(),
        source: src.join(".git"),
        commit: git(&src, &["rev-parse", "HEAD"]),
        branch: Some("main".into()),
        submodules: vec![],
    };
    e.sync(&repo3);
    let mirror = e.mirror(&repo3.id);
    git(&mirror, &["config", "filter.upper.smudge", "tr a-z A-Z"]);
    git(&mirror, &["config", "filter.upper.clean", "cat"]);
    git(
        &mirror,
        &["config", "filter.strip.clean", "sed -e 's/ *$//'"],
    );
    if autocrlf {
        git(&mirror, &["config", "core.autocrlf", "true"]);
    }
    let repo2 = RepoRef {
        commit: two,
        ..repo3.clone()
    };
    (repo2, repo3)
}

/// A commit can hold a file that its own attributes or filters would store differently (a CRLF
/// file committed before `*.txt text` was added): git calls it modified in every fresh checkout of
/// that commit. That's the commit's doing, not damage: the workspace holds what a checkout writes,
/// counts as clean, and neither the base nor the mirror is blamed.
#[test]
fn files_git_itself_normalises_seed_clean_and_blame_nothing() {
    for autocrlf in [false, true] {
        let e = env();
        let (repo2, repo3) = normalised_fixture(&e, autocrlf);
        // What a plain checkout of each commit writes, with the same config.
        let fresh = |repo: &RepoRef| -> BTreeMap<String, Vec<u8>> {
            let dir = e.path(&format!("fresh-{}", &repo.commit[..8]));
            if !dir.exists() {
                let mut args = vec!["clone", "-q", "--no-checkout"];
                let cfg = [
                    "filter.upper.smudge=tr a-z A-Z",
                    "filter.upper.clean=cat",
                    "filter.strip.clean=sed -e 's/ *$//'",
                    "core.autocrlf=true",
                ];
                for c in &cfg[..if autocrlf { 4 } else { 3 }] {
                    args.extend(["-c", c]);
                }
                let url = repo.source.to_str().unwrap();
                let name = dir.file_name().unwrap().to_str().unwrap();
                args.extend([url, name]);
                git(e.dir.path(), &args);
                git(&dir, &["checkout", "-q", "--detach", &repo.commit]);
            }
            files(&dir)
        };
        let base = e.ws.paths.base(&repo2.id);
        // Cold base; base moved forward; base ahead of the workspace.
        for (id, repo) in [("t-w1", &repo2), ("t-w2", &repo3), ("t-w3", &repo2)] {
            let what = format!("{id}, autocrlf {autocrlf}");
            let root =
                e.ws.create(&id.to_string(), repo)
                    .unwrap_or_else(|err| panic!("{what}: {err:#}"));
            e.ws.verify(&root, repo).unwrap();
            let fresh = fresh(repo);
            assert_eq!(fresh.len(), 9, "{what}");
            assert_eq!(files(&root), fresh, "{what}");
            assert!(
                !e.mirror(&repo2.id).join("potluck-suspect").exists(),
                "{what}"
            );
            // The check of the files git calls modified writes nothing: not into the workspace's
            // TMPDIR, nor beside the trees.
            assert_eq!(
                std::fs::read_dir(e.ws.paths.workspace_tmp(id))
                    .unwrap()
                    .count(),
                0,
                "{what}"
            );
            let mut beside: Vec<String> = std::fs::read_dir(root.parent().unwrap())
                .unwrap()
                .map(|d| d.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            beside.sort();
            let made = ["t-w1", "t-w2", "t-w3"]
                .into_iter()
                .take_while(|w| *w <= id)
                .flat_map(|w| [w.to_string(), format!("{w}.tmp")])
                .collect::<Vec<_>>();
            assert_eq!(beside, made, "{what}");
            if id == "t-w1" {
                // Git itself calls them modified once it rereads them.
                let then = std::time::SystemTime::now() - Duration::from_secs(60);
                std::fs::File::options()
                    .write(true)
                    .open(root.join("mixed.bat"))
                    .unwrap()
                    .set_modified(then)
                    .unwrap();
                assert_eq!(
                    git(&root, &["status", "--porcelain", "mixed.bat"]),
                    "M mixed.bat"
                );
            }
        }
        // The base is the one made first, moved forward once.
        assert_eq!(git(&base, &["rev-parse", "HEAD"]), repo3.commit);
    }
}

/// Dispatch refuses only uncommitted work: a file git calls modified because its own filters would
/// store the committed blob differently holds the committed work.
#[test]
fn inspect_accepts_files_git_itself_normalises() {
    let e = env();
    let (_, repo3) = normalised_fixture(&e, false);
    let src = e.path("src");
    // Git rereads every file whose mtime changed.
    let then = std::time::SystemTime::now() - Duration::from_secs(60);
    for f in stamps(&src).into_keys() {
        std::fs::File::options()
            .write(true)
            .open(src.join(&f))
            .unwrap()
            .set_modified(then)
            .unwrap();
    }
    let status = git(&src, &["status", "--porcelain"]);
    assert!(
        status.contains("M crlf.txt") && status.contains("M mixed.bat"),
        "{status}"
    );
    let repo = gitinfo::inspect(&src).unwrap_or_else(|err| panic!("{err:#}"));
    assert_eq!(repo.commit, repo3.commit);
    // Work that isn't committed still is refused, and only it is named.
    write(&src, "crlf.txt", "a\r\nchanged\r\n");
    write(&src, "sub/notes", "changed\n");
    let err = format!("{:#}", gitinfo::inspect(&src).unwrap_err());
    assert!(
        err.contains(" M crlf.txt") && err.contains(" M sub/notes"),
        "{err}"
    );
    assert!(!err.contains("mixed.bat") && !err.contains("id.c"), "{err}");
}

/// A commit that changes `.gitattributes` changes how files it shares with the commit before are
/// checked out (an `eol=crlf`, an `ident`, a filter). A workspace holds what a fresh checkout of
/// its commit writes, whichever commit its base was at.
#[test]
fn a_workspace_follows_its_commit_s_attributes_whatever_its_base() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "run.bat", "echo one\necho two\n");
    write(&src, "id.c", "/* $Id$ */\n");
    write(&src, "up.up", "lower case\n");
    write(&src, "plain", "plain\n");
    let c1 = commit(&src, "one", 1_700_000_000);
    write(
        &src,
        ".gitattributes",
        "*.bat eol=crlf\n*.c ident\n*.up filter=upper\n",
    );
    commit(&src, "two", 1_700_000_100);
    let repo2 = RepoRef {
        id: "attrs-0123456789abcdef".into(),
        source: src.join(".git"),
        commit: git(&src, &["rev-parse", "HEAD"]),
        branch: Some("main".into()),
        submodules: vec![],
    };
    e.sync(&repo2);
    git(
        &e.mirror(&repo2.id),
        &["config", "filter.upper.smudge", "tr a-z A-Z"],
    );
    git(
        &e.mirror(&repo2.id),
        &["config", "filter.upper.clean", "tr A-Z a-z"],
    );
    let repo1 = RepoRef {
        commit: c1,
        ..repo2.clone()
    };
    let fresh = |repo: &RepoRef| -> BTreeMap<String, Vec<u8>> {
        let dir = e.path(&format!("fresh-{}", &repo.commit[..8]));
        if !dir.exists() {
            let name = dir.file_name().unwrap().to_str().unwrap();
            git(
                e.dir.path(),
                &[
                    "-c",
                    "filter.upper.smudge=tr a-z A-Z",
                    "-c",
                    "filter.upper.clean=tr A-Z a-z",
                    "clone",
                    "-q",
                    "--no-checkout",
                    repo.source.to_str().unwrap(),
                    name,
                ],
            );
            git(&dir, &["config", "filter.upper.smudge", "tr a-z A-Z"]);
            git(&dir, &["config", "filter.upper.clean", "tr A-Z a-z"]);
            git(&dir, &["checkout", "-q", "--detach", &repo.commit]);
        }
        files(&dir)
    };
    // Base at one, moved forward to two; then a workspace at one from the base at two.
    for (id, repo) in [("t-w1", &repo1), ("t-w2", &repo2), ("t-w3", &repo1)] {
        let root = e.ws.create(&id.to_string(), repo).unwrap();
        let fresh = fresh(repo);
        let ours = files(&root);
        for (f, data) in &fresh {
            assert_eq!(
                String::from_utf8_lossy(&ours[f]),
                String::from_utf8_lossy(data),
                "{id}: {f}"
            );
        }
        assert_eq!(git(&root, &["status", "--porcelain"]), "", "{id}");
    }
}

/// Under `* text=auto` or `core.autocrlf` git converts every file, but a `.gitattributes` change
/// rewrites only the files whose attributes it changes: the others are written the same way under
/// either commit, so a base moved across it, and a workspace cloned from that base and moved back,
/// keep them as they are (a move rewrote the whole tree, and the workspace shared no block with
/// its base).
#[test]
fn a_gitattributes_change_rewrites_only_the_files_it_concerns() {
    for autocrlf in [false, true] {
        let e = env();
        let src = e.path("src");
        init(&src);
        for i in 0..50 {
            write(&src, &format!("d/f{i}.txt"), "one\ntwo\n");
        }
        write(&src, "run.bat", "echo one\necho two\n");
        let attrs = if autocrlf {
            "*.png binary\n"
        } else {
            "* text=auto\n"
        };
        write(&src, ".gitattributes", attrs);
        let c1 = commit(&src, "one", 1_700_000_000);
        write(&src, ".gitattributes", &format!("{attrs}*.bat eol=crlf\n"));
        commit(&src, "two", 1_700_000_100);
        let repo2 = RepoRef {
            id: "attrs-0123456789abcdef".into(),
            source: src.join(".git"),
            commit: git(&src, &["rev-parse", "HEAD"]),
            branch: Some("main".into()),
            submodules: vec![],
        };
        e.sync(&repo2);
        if autocrlf {
            git(&e.mirror(&repo2.id), &["config", "core.autocrlf", "true"]);
        }
        let repo1 = RepoRef {
            commit: c1,
            ..repo2.clone()
        };
        let what = format!("autocrlf {autocrlf}");
        let eol = if autocrlf { "\r\n" } else { "\n" };
        let base = e.ws.paths.base(&repo1.id);
        let w1 = e.ws.create(&"t-w1".to_string(), &repo1).unwrap();
        let inodes = || -> Vec<u64> {
            (0..50)
                .map(|i| {
                    std::fs::metadata(base.join(format!("d/f{i}.txt")))
                        .unwrap()
                        .ino()
                })
                .collect()
        };
        let before = inodes();
        let bat = std::fs::metadata(base.join("run.bat")).unwrap().ino();
        // The base moves forward to two: only run.bat gets new attributes.
        let w2 = e.ws.create(&"t-w2".to_string(), &repo2).unwrap();
        assert_eq!(inodes(), before, "{what}: the base's other files stay");
        assert_ne!(
            std::fs::metadata(base.join("run.bat")).unwrap().ino(),
            bat,
            "{what}"
        );
        // A workspace at one, cloned from the base at two and moved back.
        let w3 = e.ws.create(&"t-w3".to_string(), &repo1).unwrap();
        for i in 0..50 {
            let f = format!("d/f{i}.txt");
            let mtime = |root: &Path| {
                std::fs::metadata(root.join(&f))
                    .unwrap()
                    .modified()
                    .unwrap()
            };
            assert_eq!(mtime(&w3), mtime(&base), "{what}: {f} is the base's clone");
        }
        for (root, bat) in [
            (&w1, format!("echo one{eol}echo two{eol}")),
            (&w2, "echo one\r\necho two\r\n".to_string()),
            (&w3, format!("echo one{eol}echo two{eol}")),
        ] {
            assert_eq!(
                std::fs::read_to_string(root.join("run.bat")).unwrap(),
                bat,
                "{what}: {}",
                root.display()
            );
            assert_eq!(
                std::fs::read_to_string(root.join("d/f0.txt")).unwrap(),
                format!("one{eol}two{eol}"),
                "{what}"
            );
            assert_eq!(git(root, &["status", "--porcelain"]), "", "{what}");
        }
    }
}

/// A file in the base that no longer holds what a checkout writes is damage, also when its
/// attributes convert it: the seeding fails, the base goes and the mirror gets a full check.
#[test]
fn a_damaged_base_is_still_blamed() {
    for (file, damaged) in [
        ("crlf.txt", &b"a\r\nB!\r\n"[..]),
        ("mixed.bat", &b"x\r\ny\nz\r\n"[..]),
        ("plain.txt", &b"plain, damaged\n"[..]),
    ] {
        let e = env();
        let (repo2, _) = normalised_fixture(&e, false);
        let base = e.ws.paths.base(&repo2.id);
        let mirror = e.mirror(&repo2.id);
        let w1 = e.ws.create(&"t-w1".to_string(), &repo2).unwrap();
        std::fs::write(base.join(file), damaged).unwrap();
        let err = format!(
            "{:#}",
            e.ws.create(&"t-w2".to_string(), &repo2).unwrap_err()
        );
        assert!(
            err.contains("the new tree isn't clean") && err.contains(&format!(" M {file}")),
            "{file}: {err}"
        );
        assert!(mirror.join("potluck-suspect").exists(), "{file}");
        assert!(!base.exists(), "{file}: the base is discarded");
        assert!(!e.ws.paths.workspace("t-w2").exists());
        // The next seeding starts from a fresh base.
        let w3 = e.ws.create(&"t-w3".to_string(), &repo2).unwrap();
        e.ws.remove(&e.record("t-w3", &repo2, &w3)).unwrap();
        e.ws.remove(&e.record("t-w1", &repo2, &w1)).unwrap();
    }
}

/// APFS also takes canonically equivalent names (`café` composed and decomposed) for one: a
/// commit holding both is the repository's doing too, like names that differ only in case.
#[test]
fn names_that_differ_only_in_unicode_normalisation_are_the_repository_s_doing() {
    let e = env();
    let src = e.path("src");
    init(&src);
    git(&src, &["config", "core.precomposeUnicode", "false"]);
    write(&src, "a.txt", "a\n");
    commit(&src, "one", 1_700_000_000);
    let repo1 = gitinfo::inspect(&src).unwrap();
    for (name, data) in [("caf\u{e9}", "composed\n"), ("cafe\u{301}", "decomposed\n")] {
        let tmp = e.path("blob");
        std::fs::write(&tmp, data).unwrap();
        let blob = git(&src, &["hash-object", "-w", tmp.to_str().unwrap()]);
        git(
            &src,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("100644,{blob},{name}"),
            ],
        );
    }
    git_at(&src, 1_700_000_100, &["commit", "-q", "-m", "two"]);
    let repo2 = RepoRef {
        commit: git(&src, &["rev-parse", "HEAD"]),
        ..repo1.clone()
    };
    e.sync(&repo2);
    std::fs::create_dir_all(e.ws.paths.home.join("workspaces")).unwrap();
    let dir = e.ws.paths.home.join("workspaces");
    std::fs::write(dir.join("caf\u{e9}"), "").unwrap();
    let insensitive = dir.join("cafe\u{301}").exists();
    std::fs::remove_file(dir.join("caf\u{e9}")).unwrap();
    e.ws.create(&"t-w1".to_string(), &repo1).unwrap();
    let w2 = e.ws.create(&"t-w2".to_string(), &repo2);
    let mirror = e.mirror(&repo1.id);
    if insensitive {
        let err = format!("{:#}", w2.unwrap_err());
        assert!(
            err.contains("this filesystem can't hold both")
                && err.contains("caf\u{e9}")
                && err.contains("cafe\u{301}"),
            "{err}"
        );
        assert!(!mirror.join("potluck-suspect").exists());
        assert_eq!(
            git(&e.ws.paths.base(&repo1.id), &["rev-parse", "HEAD"]),
            repo1.commit
        );
    } else {
        let w2 = w2.unwrap();
        assert_eq!(git(&w2, &["status", "--porcelain"]), "");
    }
}

/// APFS folds case as Unicode's case folding does, not only to lower case: `ß` and `ss`, a final
/// `ς` and `σ`, a micro sign and `μ` are one name there too.
#[test]
fn names_that_differ_only_in_unicode_case_folding_are_the_repository_s_doing() {
    for (one, other) in [
        ("stra\u{df}e", "strasse"),
        ("\u{3c3}\u{3c2}", "\u{3c3}\u{3c3}"),
        ("\u{b5}s", "\u{3bc}s"),
    ] {
        let e = env();
        let src = e.path("src");
        init(&src);
        git(&src, &["config", "core.precomposeUnicode", "false"]);
        write(&src, "a.txt", "a\n");
        commit(&src, "one", 1_700_000_000);
        let repo1 = gitinfo::inspect(&src).unwrap();
        for (name, data) in [(one, "one\n"), (other, "other\n")] {
            let tmp = e.path("blob");
            std::fs::write(&tmp, data).unwrap();
            let blob = git(&src, &["hash-object", "-w", tmp.to_str().unwrap()]);
            git(
                &src,
                &[
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    &format!("100644,{blob},{name}"),
                ],
            );
        }
        git_at(&src, 1_700_000_100, &["commit", "-q", "-m", "two"]);
        let repo2 = RepoRef {
            commit: git(&src, &["rev-parse", "HEAD"]),
            ..repo1.clone()
        };
        e.sync(&repo2);
        let dir = e.ws.paths.home.join("workspaces");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(one), "").unwrap();
        let insensitive = dir.join(other).exists();
        std::fs::remove_file(dir.join(one)).unwrap();
        e.ws.create(&"t-w1".to_string(), &repo1).unwrap();
        let w2 = e.ws.create(&"t-w2".to_string(), &repo2);
        let mirror = e.mirror(&repo1.id);
        if insensitive {
            let err = format!("{:#}", w2.unwrap_err());
            assert!(
                err.contains("this filesystem can't hold both")
                    && err.contains(one)
                    && err.contains(other),
                "{one:?}, {other:?}: {err}"
            );
            assert!(!mirror.join("potluck-suspect").exists(), "{one:?}");
            assert_eq!(
                git(&e.ws.paths.base(&repo1.id), &["rev-parse", "HEAD"]),
                repo1.commit,
                "{one:?}"
            );
        } else {
            let w2 = w2.unwrap();
            assert_eq!(git(&w2, &["status", "--porcelain"]), "", "{one:?}");
        }
    }
}

/// A commit may hold a file name the filesystem refuses: APFS takes only UTF-8 of the Unicode
/// version it knows (U+A7DD, unassigned there, is "Illegal byte sequence"), and no filesystem here
/// takes a name over 255 bytes. That is the repository's doing, like names that differ only in
/// case: seeding fails saying so, the mirror isn't blamed (no full fsck at its next sync), and a
/// base checkout never moves to such a commit (it would be lost half moved). Where the filesystem
/// takes the name, the workspace is seeded clean.
#[test]
fn names_the_filesystem_refuses_are_the_repository_s_doing() {
    let long = "n".repeat(256);
    for name in ["x\u{a7dd}", long.as_str()] {
        let e = env();
        let src = e.path("src");
        init(&src);
        write(&src, "a.txt", "a\n");
        commit(&src, "one", 1_700_000_000);
        let repo1 = gitinfo::inspect(&src).unwrap();
        let blob = git(&src, &["hash-object", "-w", "a.txt"]);
        git(
            &src,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("100644,{blob},d/{name}"),
            ],
        );
        git_at(&src, 1_700_000_100, &["commit", "-q", "-m", "two"]);
        let repo2 = RepoRef {
            commit: git(&src, &["rev-parse", "HEAD"]),
            ..repo1.clone()
        };
        e.sync(&repo2);
        let dir = e.ws.paths.home.join("workspaces");
        std::fs::create_dir_all(&dir).unwrap();
        let refused = std::fs::write(dir.join(name), "").is_err();
        let _ = std::fs::remove_file(dir.join(name));
        let mirror = e.mirror(&repo1.id);
        let base = e.ws.paths.base(&repo1.id);
        let shown: String = name.chars().take(20).collect();
        // Warm: the base is at the first commit.
        e.ws.create(&"t-w1".to_string(), &repo1).unwrap();
        let base_ino = std::fs::metadata(base.join("a.txt")).unwrap().ino();
        let w2 = e.ws.create(&"t-w2".to_string(), &repo2);
        if refused {
            let err = format!("{:#}", w2.unwrap_err());
            assert!(
                err.contains("the commit has file names this filesystem refuses: d/")
                    && err.contains(&shown),
                "{shown:?}: {err}"
            );
            assert!(!mirror.join("potluck-suspect").exists(), "{shown:?}: {err}");
            assert_eq!(
                git(&base, &["rev-parse", "HEAD"]),
                repo1.commit,
                "{shown:?}"
            );
            assert_eq!(
                std::fs::metadata(base.join("a.txt")).unwrap().ino(),
                base_ino,
                "{shown:?}: the base was made anew"
            );
            assert!(!e.ws.paths.workspace("t-w2").exists());
        } else {
            let w2 = w2.unwrap();
            assert_eq!(git(&w2, &["status", "--porcelain"]), "", "{shown:?}");
        }
        // Cold: the first base would be made at that commit.
        let e2 = env();
        e2.sync(&repo2);
        let cold = e2.ws.create(&"t-c1".to_string(), &repo2);
        if refused {
            let err = format!("{:#}", cold.unwrap_err());
            assert!(
                err.contains("the commit has file names this filesystem refuses: d/"),
                "{shown:?}: {err}"
            );
            assert!(
                !e2.mirror(&repo1.id).join("potluck-suspect").exists(),
                "{shown:?}: {err}"
            );
            let w = e2.ws.create(&"t-c2".to_string(), &repo1).unwrap();
            assert_eq!(git(&w, &["status", "--porcelain"]), "", "{shown:?}");
        } else {
            cold.unwrap();
        }
    }
}

/// A removal moves the tree aside to `<id>.removing` and deletes it there. When a file in it can't
/// be deleted (here an immutable one; elsewhere root-owned files a job made through a container),
/// the tree stays under that name, with the tree's own path and the TMPDIR gone: the daemon must
/// look there to know the removal isn't done. The next removal deletes it.
#[cfg(target_os = "macos")]
#[test]
fn a_tree_whose_deletion_fails_stays_aside_until_the_next_removal() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "f", "one\n");
    commit(&src, "one", 1_700_000_000);
    let repo = gitinfo::inspect(&src).unwrap();
    e.sync(&repo);
    let root = e.ws.create(&"t-w1".to_string(), &repo).unwrap();
    let stuck = root.join("build/out.o");
    write(&root, "build/out.o", "object\n");
    let chflags = |flag: &str, path: &Path| {
        let st = Command::new("chflags")
            .arg(flag)
            .arg(path)
            .status()
            .unwrap();
        assert!(st.success(), "chflags {flag} {}", path.display());
    };
    chflags("uchg", &stuck);
    // Cleared whatever happens, so the temp dir can go.
    struct Unlock(PathBuf);
    impl Drop for Unlock {
        fn drop(&mut self) {
            let _ = Command::new("chflags").arg("nouchg").arg(&self.0).status();
        }
    }
    let aside = root.with_file_name("t-w1.removing");
    let _unlock = Unlock(aside.join("build/out.o"));
    let rec = e.record("t-w1", &repo, &root);
    let err = format!("{:#}", e.ws.remove(&rec).unwrap_err());
    assert!(err.contains("t-w1.removing"), "{err}");
    assert!(
        !root.exists() && !e.ws.paths.workspace_tmp("t-w1").exists(),
        "{err}"
    );
    assert!(aside.join("build/out.o").is_file());
    chflags("nouchg", &aside.join("build/out.o"));
    e.ws.remove(&rec).unwrap();
    assert!(!aside.exists());
}

/// Holds `path` with flock(2), as potluck's own locks do.
fn hold_lock(path: &Path, exclusive: bool) -> std::fs::File {
    use std::os::unix::io::AsRawFd;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    let how = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    assert_eq!(unsafe { libc::flock(f.as_raw_fd(), how) }, 0);
    f
}

/// Runs `f` on a thread; panics if it doesn't finish within `secs`.
fn within<T: Send + 'static>(secs: u64, what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(std::time::Duration::from_secs(secs))
        .unwrap_or_else(|_| panic!("{what} didn't finish within {secs}s"))
}

#[test]
fn a_busy_mirror_holds_up_only_its_own_workspaces() {
    let e = env();
    let mut repos = Vec::new();
    for name in ["alpha", "beta"] {
        let src = e.path(name);
        init(&src);
        write(&src, "f", name);
        commit(&src, "one", 1_700_000_000);
        let repo = gitinfo::inspect(&src).unwrap();
        e.sync(&repo);
        repos.push(repo);
    }
    let (alpha, beta) = (repos[0].clone(), repos[1].clone());
    let wa = e.ws.create(&"t-a".to_string(), &alpha).unwrap();
    let wb = e.ws.create(&"t-b".to_string(), &beta).unwrap();
    let lock = |id: &str| e.ws.paths.home.join("mirrors").join(format!("{id}.lock"));

    // While beta's mirror is held exclusively (its sync and full fsck hold it so), alpha's
    // workspace is removed all the same.
    let held = hold_lock(&lock(&beta.id), true);
    let ws = Workspaces::new(e.ws.paths.clone());
    let record = e.record("t-a", &alpha, &wa);
    within(20, "removing alpha's workspace", move || {
        ws.remove(&record).unwrap()
    });
    assert!(!wa.exists());
    drop(held);

    // A full fsck of beta (an earlier seeding failed there) holds only beta's lock: it runs while
    // alpha's mirror is held exclusively by someone else.
    std::fs::write(e.mirror(&beta.id).join("potluck-suspect"), "test").unwrap();
    let held = hold_lock(&lock(&alpha.id), true);
    let ws = Workspaces::new(e.ws.paths.clone());
    let (id, url) = (beta.id.clone(), beta.source.to_str().unwrap().to_string());
    let report = within(20, "syncing beta with a full fsck", move || {
        ws.sync_mirror(&id, &url).unwrap()
    });
    assert_eq!(report, Default::default());
    assert!(!e.mirror(&beta.id).join("potluck-suspect").exists());
    drop(held);
    e.ws.remove(&e.record("t-b", &beta, &wb)).unwrap();
    assert!(e.workspace_pins(&alpha.id).is_empty() && e.workspace_pins(&beta.id).is_empty());
}

#[test]
fn deleting_a_big_workspace_holds_up_no_sync() {
    let e = env();
    let mut repos = Vec::new();
    for name in ["alpha", "beta"] {
        let src = e.path(name);
        init(&src);
        write(&src, "f", name);
        commit(&src, "one", 1_700_000_000);
        let repo = gitinfo::inspect(&src).unwrap();
        e.sync(&repo);
        repos.push(repo);
    }
    let (alpha, beta) = (repos[0].clone(), repos[1].clone());
    let wa = e.ws.create(&"t-a".to_string(), &alpha).unwrap();
    // What a build leaves in its workspace: a big tree, slow to delete.
    let out = wa.join("out");
    for d in 0..100 {
        let dir = out.join(format!("d{d:03}"));
        std::fs::create_dir_all(&dir).unwrap();
        for f in 0..300 {
            std::fs::write(dir.join(format!("f{f:03}")), "x").unwrap();
        }
    }
    let ws = Workspaces::new(e.ws.paths.clone());
    let record = e.record("t-a", &alpha, &wa);
    let removing = std::thread::spawn(move || ws.remove(&record).unwrap());
    // Once the tree is going (or gone from its place), beta's mirror syncs, and alpha's: neither
    // waits for the rest of the deletion.
    let start = Instant::now();
    while out.exists() && std::fs::read_dir(&out).unwrap().count() == 100 {
        assert!(start.elapsed() < Duration::from_secs(60), "never started");
        std::thread::sleep(Duration::from_millis(1));
    }
    let deleting = Instant::now();
    for repo in [&beta, &alpha] {
        e.ws.sync_mirror(&repo.id, repo.source.to_str().unwrap())
            .unwrap();
    }
    let synced = deleting.elapsed();
    removing.join().unwrap();
    let deleted = deleting.elapsed();
    assert!(
        synced * 4 < deleted,
        "the syncs took {synced:?}, the rest of the removal {deleted:?}"
    );
    assert!(!wa.exists() && worktrees(&e.mirror(&alpha.id)).len() == 1);
    assert!(e.workspace_pins(&alpha.id).is_empty());
}

/// Writes `n` small files under `dir` (100 to a directory), each with its own content.
fn many_files(dir: &Path, n: usize) {
    for i in 0..n {
        let d = dir.join(format!("d{:03}", i / 100));
        if i % 100 == 0 {
            std::fs::create_dir_all(&d).unwrap();
        }
        std::fs::write(d.join(format!("f{i:05}")), format!("{i}\n")).unwrap();
    }
}

/// Polls `cond` every millisecond for up to a minute.
fn until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < Duration::from_secs(60), "never: {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Whether `path` (a potluck lock file) can be locked right now, shared or exclusively; the lock
/// is let go at once.
fn lock_free(path: &Path, exclusive: bool) -> bool {
    use std::os::unix::io::AsRawFd;
    let Ok(f) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    else {
        return false;
    };
    let how = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    unsafe { libc::flock(f.as_raw_fd(), how | libc::LOCK_NB) == 0 }
}

/// While `deleting()` holds, polls whether `lock` is free; true once it was.
fn free_while(lock: &Path, exclusive: bool, mut deleting: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while deleting() {
        if lock_free(lock, exclusive) {
            return true;
        }
        assert!(start.elapsed() < Duration::from_secs(60), "never deleted");
    }
    false
}

/// A base checkout that has to be made anew moves aside and is deleted outside the mirror's
/// worktrees lock, which every workspace of the repository being made or removed waits for.
#[test]
fn a_base_is_deleted_outside_the_worktrees_lock() {
    let e = env();
    let src = e.path("src");
    init(&src);
    many_files(&src, 5_000);
    commit(&src, "big", 1_700_000_000);
    let repo = gitinfo::inspect(&src).unwrap();
    e.sync(&repo);
    let w1 = e.ws.create(&"t-1".to_string(), &repo).unwrap();
    e.ws.remove(&e.record("t-1", &repo, &w1)).unwrap();
    // A crash left the base's index locked: the next seeding makes it anew.
    let base = e.ws.paths.base(&repo.id);
    let admin = PathBuf::from(git(&base, &["rev-parse", "--absolute-git-dir"]));
    std::fs::write(admin.join("index.lock"), "").unwrap();
    let ws = Workspaces::new(e.ws.paths.clone());
    let r = repo.clone();
    let seeding = std::thread::spawn(move || ws.create(&"t-2".to_string(), &r));
    let aside = base.with_file_name(format!(".{}.removing", repo.id));
    let (first, last) = (Path::new("d000/f00000"), Path::new("d049/f04999"));
    // The old base's files go (moved aside, or deleted in place) ...
    until("the old base goes", || !base.join(first).exists());
    // ... and while they are being deleted, the worktrees lock is free.
    let lock =
        e.ws.paths
            .home
            .join("mirrors")
            .join(format!("{}.worktrees.lock", repo.id));
    let free = free_while(&lock, true, || {
        aside.join(last).exists() || (base.join(last).exists() && !base.join(".git").exists())
    });
    let w2 = seeding.join().unwrap().unwrap();
    assert!(
        free,
        "the worktrees lock was held while the base was deleted"
    );
    assert!(!aside.exists());
    assert_eq!(git(&w2, &["status", "--porcelain"]), "");
}

/// A recloned mirror's old copy is deleted after the mirror's lock is let go: every seeding, pin
/// and dispatch of the repository waits for that lock.
#[test]
fn an_old_mirror_is_deleted_outside_its_lock() {
    let e = env();
    let src = e.path("src");
    init(&src);
    many_files(&src, 5_000);
    commit(&src, "big", 1_700_000_000);
    let repo0 = gitinfo::inspect(&src).unwrap();
    e.sync(&repo0);
    // Kept loose (as with gc.auto=0 in the user's config), so the old mirror is slow to delete.
    git(&e.mirror(&repo0.id), &["config", "gc.auto", "0"]);
    // A commit that arrives by fetch, as the mirror's own loose objects; then damaged.
    write(&src, "d000/f00000", "changed\n");
    let c2 = commit(&src, "two", 1_700_000_100);
    let repo = gitinfo::inspect(&src).unwrap();
    e.sync(&repo);
    let mirror = e.mirror(&repo.id);
    let obj = mirror.join(format!("objects/{}/{}", &c2[..2], &c2[2..]));
    assert_eq!(std::fs::metadata(&obj).unwrap().nlink(), 1);
    std::fs::set_permissions(&obj, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::write(&obj, b"garbage, not zlib").unwrap();

    let ws = Workspaces::new(e.ws.paths.clone());
    let (id, url) = (repo.id.clone(), repo.source.to_str().unwrap().to_string());
    let syncing = std::thread::spawn(move || ws.sync_mirror(&id, &url));
    let name = mirror.file_name().unwrap().to_string_lossy().into_owned();
    let old = || {
        std::fs::read_dir(mirror.parent().unwrap())
            .unwrap()
            .flatten()
            .find(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(&format!("{name}.broken-"))
            })
            .map(|e| e.path())
    };
    until("the old mirror moves aside", || old().is_some());
    let lock =
        e.ws.paths
            .home
            .join("mirrors")
            .join(format!("{}.lock", repo.id));
    let free = free_while(&lock, false, || {
        old().is_some_and(|p| p.join("objects").exists())
    });
    let report = syncing.join().unwrap().unwrap();
    assert!(report.recloned.is_some(), "{report:?}");
    assert!(
        free,
        "the mirror's lock was held while its old copy was deleted"
    );
    e.ws.pin(&repo.id, &c2, "after").unwrap();
}

// --- clones ----------------------------------------------------------------------------------

#[test]
fn clone_tree_keeps_contents_modes_links_and_mtimes() {
    let t = tmp();
    let src = t.path().join("src");
    write(&src, "a/b/c.txt", "deep\n");
    write(&src, "a/exec", "#!/bin/sh\n");
    write(&src, "ro", "read only\n");
    write(&src, "empty", "");
    std::fs::write(src.join("big"), vec![42u8; 300_000]).unwrap();
    std::os::unix::fs::symlink("a/b/c.txt", src.join("link")).unwrap();
    std::os::unix::fs::symlink("nowhere", src.join("dangling")).unwrap();
    std::fs::create_dir_all(src.join("emptydir")).unwrap();
    std::fs::set_permissions(src.join("a/exec"), std::fs::Permissions::from_mode(0o750)).unwrap();
    std::fs::set_permissions(src.join("ro"), std::fs::Permissions::from_mode(0o444)).unwrap();
    std::fs::set_permissions(src.join("a/b"), std::fs::Permissions::from_mode(0o555)).unwrap();
    let dst = t.path().join("dst");
    // Read-only directories would keep the temp dir from going away, even after a failure.
    struct Writable(Vec<PathBuf>);
    impl Drop for Writable {
        fn drop(&mut self) {
            for d in &self.0 {
                let _ = std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o755));
            }
        }
    }
    let _writable = Writable(vec![src.join("a/b"), dst.join("a/b")]);

    cow::clone_tree(&src, &dst).unwrap();
    assert!(cow::clone_tree(&src, &dst).is_err(), "dst must not exist");
    for rel in [
        "a/b/c.txt",
        "a/exec",
        "ro",
        "empty",
        "big",
        "link",
        "dangling",
        "emptydir",
        "a/b",
        "a",
    ] {
        let (s, d) = (src.join(rel), dst.join(rel));
        let (sm, dm) = (s.symlink_metadata().unwrap(), d.symlink_metadata().unwrap());
        assert_eq!(sm.mode(), dm.mode(), "{rel}");
        // clonefile gives a directory with entries the time of the clone.
        let keeps_mtime = cfg!(target_os = "linux") || !sm.is_dir() || rel == "emptydir";
        if keeps_mtime {
            assert_eq!(
                (sm.mtime(), sm.mtime_nsec()),
                (dm.mtime(), dm.mtime_nsec()),
                "{rel}"
            );
        }
        assert_ne!(sm.ino(), dm.ino(), "{rel}");
        if sm.is_file() {
            assert_eq!(
                std::fs::read(&s).unwrap(),
                std::fs::read(&d).unwrap(),
                "{rel}"
            );
        }
        if sm.file_type().is_symlink() {
            assert_eq!(
                std::fs::read_link(&s).unwrap(),
                std::fs::read_link(&d).unwrap()
            );
        }
    }
    std::fs::write(dst.join("big"), b"changed").unwrap();
    assert_eq!(std::fs::read(src.join("big")).unwrap().len(), 300_000);

    // clone_dir_contents skips top-level names.
    let into = t.path().join("into");
    std::fs::create_dir_all(&into).unwrap();
    cow::clone_dir_contents(&src, &into, &["a", "big"]).unwrap();
    assert!(into.join("ro").is_file() && !into.join("a").exists() && !into.join("big").exists());
}

#[test]
fn cow_check_passes_here() {
    let t = tmp();
    cow::check(t.path()).unwrap();
    assert_eq!(
        std::fs::read_dir(t.path()).unwrap().count(),
        0,
        "test files removed"
    );
}

/// A temp dir on a filesystem without clones, or None when this machine has none handy.
#[cfg(target_os = "linux")]
fn non_cow_dir() -> Option<(tempfile::TempDir, Box<dyn FnOnce()>)> {
    let shm = Path::new("/dev/shm");
    if !shm.is_dir() {
        return None;
    }
    Some((tempfile::tempdir_in(shm).unwrap(), Box::new(|| {})))
}

/// An HFS+ disk image attached at a temp mount point, detached afterwards.
#[cfg(target_os = "macos")]
fn non_cow_dir() -> Option<(tempfile::TempDir, Box<dyn FnOnce()>)> {
    let work = tmp();
    let image = work.path().join("hfs.dmg");
    let made = Command::new("hdiutil")
        .args([
            "create",
            "-quiet",
            "-size",
            "4m",
            "-fs",
            "HFS+",
            "-volname",
            "potlucktest",
        ])
        .arg(&image)
        .status()
        .ok()?;
    if !made.success() {
        return None;
    }
    let mnt = tmp();
    let attached = Command::new("hdiutil")
        .args(["attach", "-quiet", "-nobrowse", "-noverify", "-mountpoint"])
        .arg(mnt.path())
        .arg(&image)
        .status()
        .ok()?;
    if !attached.success() {
        return None;
    }
    let mount = mnt.path().to_path_buf();
    let detach = Box::new(move || {
        let _ = Command::new("hdiutil")
            .args(["detach", "-quiet", "-force"])
            .arg(&mount)
            .status();
        drop(work);
    });
    Some((mnt, detach))
}

#[test]
fn cow_check_names_a_filesystem_without_clones() {
    let Some((dir, cleanup)) = non_cow_dir() else {
        eprintln!("no filesystem without clones at hand: skipped");
        return;
    };
    let result = cow::check(dir.path());
    let name = cow::fs_name(dir.path());
    let leftovers = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".potluck-cow-check")
        })
        .count();
    // A file that exists: the clone fails for want of clones, not of a source.
    std::fs::write(dir.path().join("x"), "x").unwrap();
    let clone_err = cow::clone_tree(&dir.path().join("x"), &dir.path().join("y"))
        .map_err(|e| format!("{:#}", e.root_cause()));
    cleanup();
    let err = format!("{:#}", result.expect_err("no clones there"));
    eprintln!("{err}");
    assert!(err.contains(&name) && err.contains("POTLUCK_HOME"), "{err}");
    #[cfg(target_os = "linux")]
    assert_eq!(name, "tmpfs");
    #[cfg(target_os = "macos")]
    assert_eq!(name, "hfs");
    assert_eq!(leftovers, 0);
    let clone_err = clone_err.expect_err("a clone there");
    assert!(!clone_err.contains("No such file"), "{clone_err}");
}

// --- timing ----------------------------------------------------------------------------------

/// Seeding time for a synthetic repository (default ~20k files, ~200 MB):
/// `cargo test --release --test workspace -- --ignored --nocapture seed_timing`.
/// POTLUCK_TIMING_FILES and POTLUCK_TIMING_MB change the size.
#[test]
#[ignore]
fn seed_timing() {
    let files: usize = std::env::var("POTLUCK_TIMING_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let mb: usize = std::env::var("POTLUCK_TIMING_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let e = env();
    let src = e.path("src");
    init(&src);
    let avg = mb * 1024 * 1024 / files;
    let mut rng: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut total = 0usize;
    for i in 0..files {
        let size = (next() as usize) % (2 * avg + 1);
        let mut buf = Vec::with_capacity(size + 8);
        while buf.len() < size {
            buf.extend_from_slice(&next().to_le_bytes());
        }
        buf.truncate(size);
        total += size;
        let p = src.join(format!("d{:03}/s{:02}/f{i}.bin", i % 200, (i / 200) % 10));
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, buf).unwrap();
    }
    let t = Instant::now();
    git_at(
        &src,
        1_700_000_000,
        &["-c", "core.compression=0", "add", "-A"],
    );
    git_at(&src, 1_700_000_000, &["commit", "-q", "-m", "one"]);
    let c1 = git(&src, &["rev-parse", "HEAD"]);
    eprintln!(
        "generated {files} files, {} MB; committed in {:?}",
        total >> 20,
        t.elapsed()
    );
    for i in 0..10 {
        write(
            &src,
            &format!("d{i:03}/s00/f{i}.bin"),
            &format!("changed {i}\n"),
        );
    }
    commit(&src, "two", 1_700_000_100);
    let repo2 = gitinfo::inspect(&src).unwrap();
    let repo1 = RepoRef {
        commit: c1,
        ..repo2.clone()
    };

    let mut rows = Vec::new();
    let mut time = |what: &str, f: &mut dyn FnMut()| {
        let t = Instant::now();
        f();
        let d = t.elapsed();
        rows.push(format!("{what:<58} {:>8.2} s", d.as_secs_f64()));
    };
    time("mirror: first sync (local clone)", &mut || e.sync(&repo2));
    time(
        "cold: base checkout at c1, then clone + verify",
        &mut || {
            e.ws.create(&"t-cold".to_string(), &repo1).unwrap();
        },
    );
    time(
        "warm: base one commit behind (moves to c2), clone + verify",
        &mut || {
            e.ws.create(&"t-warm".to_string(), &repo2).unwrap();
        },
    );
    time(
        "warm: base one commit ahead (stays), clone + read-tree + verify",
        &mut || {
            e.ws.create(&"t-older".to_string(), &repo1).unwrap();
        },
    );
    time("  of which: clone of the base alone", &mut || {
        cow::clone_tree(&e.ws.paths.base(&repo1.id), &e.path("bare-clone")).unwrap();
    });
    let warm = e.ws.paths.workspace("t-warm");
    time("  of which: git status of a fresh workspace", &mut || {
        git(&warm, &["status", "--porcelain"]);
    });
    time("size of a workspace", &mut || {
        e.ws.size(&warm).unwrap();
    });
    time("remove a workspace", &mut || {
        e.ws.remove(&e.record("t-warm", &repo2, &warm)).unwrap();
    });
    eprintln!(
        "seeding {files} files, {} MB on {}:",
        total >> 20,
        cow::fs_name(e.dir.path())
    );
    for r in rows {
        eprintln!("  {r}");
    }
}

/// After an upgrade, the mirrors and bases with repository ids from before ids were salted with
/// the owner go once nothing uses them: salted ones stay, and one in use right now is left for
/// the next time.
#[test]
fn mirrors_and_bases_from_before_salted_ids_are_removed() {
    let e = env();
    let r = e.path("proj");
    init(&r);
    write(&r, "a.txt", "a\n");
    let c = commit(&r, "one", 1_700_000_000);
    let url = r.join(".git");
    let url = url.to_str().unwrap();
    let old = "proj-0123abcd";
    let salted = potluck::types::owned_repo_id("studio", old);
    let busy = "busy-89abcdef";
    for id in [old, salted.as_str(), busy] {
        e.ws.sync_mirror(id, url).unwrap();
        let repo = RepoRef {
            id: id.into(),
            source: r.join(".git"),
            commit: c.clone(),
            branch: None,
            submodules: vec![],
        };
        let w = format!("w-{id}");
        let root = e.ws.create(&w, &repo).unwrap();
        e.ws.remove(&e.record(&w, &repo, &root)).unwrap();
        assert!(e.ws.paths.base(id).is_dir(), "{id}");
    }
    // A sync of `busy` holds its mirror's lock.
    let lock = std::fs::File::open(e.ws.paths.home.join(format!("mirrors/{busy}.lock"))).unwrap();
    use std::os::unix::io::AsRawFd;
    // SAFETY: flock on an open file.
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) }, 0);

    // A lock just let go can stay held a moment longer: a process another test's thread is
    // starting holds a copy of its descriptor until it runs its program. A mirror found busy is
    // left for the next call, so call again.
    let removed_until = |want: &str| {
        let start = Instant::now();
        loop {
            let got = e.ws.remove_unsalted();
            if got == [want] {
                return;
            }
            assert!(got.is_empty(), "{got:?}");
            assert!(
                start.elapsed() < std::time::Duration::from_secs(10),
                "{want} never removed"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    };
    removed_until(old);
    assert!(!e.mirror(old).exists() && !e.ws.paths.base(old).exists());
    assert!(e.mirror(&salted).is_dir() && e.ws.paths.base(&salted).is_dir());
    assert!(e.mirror(busy).is_dir() && e.ws.paths.base(busy).is_dir());
    drop(lock);
    removed_until(busy);
    assert!(!e.mirror(busy).exists() && !e.ws.paths.base(busy).exists());
    assert!(e.ws.has_commit(&salted, &c));
    assert!(e.ws.remove_unsalted().is_empty());
}

/// A divergence found by a sync whose source couldn't be read (its machine went away) is still
/// reported by the next sync that reaches the source.
#[test]
fn a_divergence_outlives_a_sync_that_cant_read_the_source() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1");
    let c1 = commit(&src, "one", 1_700_000_000);
    let url = src.join(".git");
    let url = url.to_str().unwrap();
    let id = potluck::types::owned_repo_id("studio", "src-00000002");
    let id = id.as_str();
    e.ws.sync_mirror(id, url).unwrap();
    // What a job running `git branch scratch` in its workspace does to the mirror.
    git(&e.mirror(id), &["branch", "scratch", &c1]);
    let away = e.path("src-away");
    std::fs::rename(&src, &away).unwrap();
    assert!(e.ws.sync_mirror(id, url).is_err());
    std::fs::rename(&away, &src).unwrap();
    let report = e.ws.sync_mirror(id, url).unwrap();
    assert_eq!(
        report.diverged,
        [format!(
            "refs/heads/scratch was made here, at {}",
            &c1[..12]
        )]
    );
}

/// A job that runs git in its workspace writes the mirror's branches and tags (worktrees share
/// them). The next sync resets them to the source's, and reports what they were, loudly; the
/// source's own force-updates and deletions are no divergence.
#[test]
fn a_mirror_changed_outside_a_sync_is_reported_and_reset() {
    let e = env();
    let src = e.path("src");
    init(&src);
    write(&src, "a", "1");
    let c1 = commit(&src, "one", 1_700_000_000);
    git(&src, &["tag", "v1"]);
    let url = src.join(".git");
    let url = url.to_str().unwrap();
    let id = potluck::types::owned_repo_id("studio", "src-00000001");
    let id = id.as_str();
    e.ws.sync_mirror(id, url).unwrap();
    let repo = RepoRef {
        id: id.into(),
        source: src.join(".git"),
        commit: c1.clone(),
        branch: None,
        submodules: vec![],
    };
    let root = e.ws.create(&"w1".to_string(), &repo).unwrap();

    // The job: a branch of its own with a commit, a tag, and main moved onto that commit.
    git(&root, &["checkout", "-q", "-b", "scratch"]);
    write(&root, "b", "2");
    let local = commit(&root, "local", 1_700_000_100);
    git(&root, &["tag", "localtag"]);
    git(&root, &["branch", "-f", "main", "HEAD"]);
    git(&root, &["tag", "-d", "v1"]);

    let report = e.ws.sync_mirror(id, url).unwrap();
    let (l, c) = (&local[..12], &c1[..12]);
    assert_eq!(
        report.diverged,
        [
            format!("refs/heads/main was at {l} here, {c} at the last sync"),
            format!("refs/heads/scratch was made here, at {l}"),
            format!("refs/tags/localtag was made here, at {l}"),
            format!("refs/tags/v1 was deleted here (it was at {c})"),
        ]
    );
    assert!(report.recloned.is_none());
    let warning = report.warning(id).unwrap();
    assert!(
        warning.contains("had changed since its last sync") && warning.contains(id),
        "{warning}"
    );
    let ours = |m: &Path| refs(m, &["refs/heads", "refs/tags"]);
    assert_eq!(ours(&e.mirror(id)), ours(&src));

    // Synced again: nothing changed meanwhile, nothing to report.
    assert_eq!(e.ws.sync_mirror(id, url).unwrap(), Default::default());
    // The source amends main and deletes its tag: an exact copy follows, quietly.
    write(&src, "a", "3");
    git_at(&src, 1_700_000_200, &["add", "-A"]);
    git_at(
        &src,
        1_700_000_200,
        &["commit", "-q", "--amend", "-m", "amended"],
    );
    git(&src, &["tag", "-d", "v1"]);
    assert_eq!(e.ws.sync_mirror(id, url).unwrap(), Default::default());
    assert_eq!(ours(&e.mirror(id)), ours(&src));
}

/// A small APFS disk image attached at a temp mount point (copy-on-write, so seeding works on
/// it), detached afterwards.
#[cfg(target_os = "macos")]
fn small_cow_dir(mb: u32) -> Option<(tempfile::TempDir, Box<dyn FnOnce()>)> {
    let work = tmp();
    let image = work.path().join("apfs.dmg");
    let made = Command::new("hdiutil")
        .args([
            "create",
            "-quiet",
            "-size",
            &format!("{mb}m"),
            "-fs",
            "APFS",
        ])
        .args(["-volname", "potlucktest"])
        .arg(&image)
        .status()
        .ok()?;
    if !made.success() {
        return None;
    }
    let mnt = tmp();
    let attached = Command::new("hdiutil")
        .args(["attach", "-quiet", "-nobrowse", "-noverify", "-mountpoint"])
        .arg(mnt.path())
        .arg(&image)
        .status()
        .ok()?;
    if !attached.success() {
        return None;
    }
    let mount = mnt.path().to_path_buf();
    let detach = Box::new(move || {
        let _ = Command::new("hdiutil")
            .args(["detach", "-quiet", "-force"])
            .arg(&mount)
            .status();
        drop(work);
    });
    Some((mnt, detach))
}

#[cfg(not(target_os = "macos"))]
fn small_cow_dir(_mb: u32) -> Option<(tempfile::TempDir, Box<dyn FnOnce()>)> {
    // A small btrfs or XFS needs root to mount.
    None
}

/// A seeding that fails because the disk filled up is the disk's doing: the error says the disk
/// is full, and the mirror isn't marked for a full fsck (and a reclone, which would need room
/// for a second copy).
#[test]
fn a_seeding_that_fills_the_disk_blames_neither_base_nor_mirror() {
    let Some((vol, cleanup)) = small_cow_dir(64) else {
        eprintln!("no small copy-on-write volume at hand: skipped");
        return;
    };
    let result = std::panic::catch_unwind(|| {
        let src_dir = tmp();
        let src = src_dir.path().join("big");
        init(&src);
        // 36 MB that doesn't compress: the mirror holds it once and the base checkout again,
        // more than the 64 MB volume has.
        let mut data = vec![0u8; 36 << 20];
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for b in data.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        std::fs::write(src.join("blob.bin"), &data).unwrap();
        let c = commit(&src, "big", 1_700_000_000);
        let ws = Workspaces::new(Paths {
            home: vol.path().join("state"),
            config: vol.path().join("node.toml"),
        });
        let id = "big-0123456789abcdef";
        let url = src.join(".git");
        ws.sync_mirror(id, url.to_str().unwrap()).unwrap();
        let repo = RepoRef {
            id: id.into(),
            source: url.clone(),
            commit: c,
            branch: None,
            submodules: vec![],
        };
        let err = format!("{:#}", ws.create(&"w1".to_string(), &repo).unwrap_err());
        let mirror = ws.paths.mirror(id);
        let left = (
            mirror.join("potluck-suspect").exists(),
            ws.paths.base(id).exists(),
            ws.paths.workspace("w1").exists(),
        );
        (err, left)
    });
    cleanup();
    let (err, (suspect, base, workspace)) = result.unwrap();
    eprintln!("{err}");
    assert!(
        err.contains("is full (") && err.contains("MiB free)"),
        "{err}"
    );
    assert!(!suspect, "the mirror was marked suspect: {err}");
    // The half-made base and the workspace are gone, freeing what they took.
    assert!(!base && !workspace);
}

/// A sync whose fetch fails because the disk is full is the disk's doing: the error says the disk
/// is full, and no full fsck or reclone (a second copy, over the network for a runner) is tried.
#[test]
fn a_sync_on_a_full_disk_says_so_and_tries_no_reclone() {
    let Some((vol, cleanup)) = small_cow_dir(64) else {
        eprintln!("no small copy-on-write volume at hand: skipped");
        return;
    };
    let result = std::panic::catch_unwind(|| {
        let src_dir = tmp();
        let src = src_dir.path().join("repo");
        init(&src);
        write(&src, "a", "1");
        commit(&src, "one", 1_700_000_000);
        let ws = Workspaces::new(Paths {
            home: vol.path().join("state"),
            config: vol.path().join("node.toml"),
        });
        let id = "repo-0123456789abcdef";
        let url = src.join(".git");
        ws.sync_mirror(id, url.to_str().unwrap()).unwrap();
        // 4 MB that doesn't compress, for the next fetch; then the disk fills up.
        let mut data = vec![0u8; 4 << 20];
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for b in data.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        std::fs::write(src.join("blob.bin"), &data).unwrap();
        commit(&src, "big", 1_700_000_100);
        let mut fill = std::fs::File::create(vol.path().join("fill")).unwrap();
        for size in [1 << 20, 4 << 10, 512] {
            let block = vec![0u8; size];
            while std::io::Write::write_all(&mut fill, &block).is_ok() {}
        }
        drop(fill);
        let err = format!(
            "{:#}",
            ws.sync_mirror(id, url.to_str().unwrap()).unwrap_err()
        );
        let mirror = ws.paths.mirror(id);
        let new = mirror.with_extension("git.new");
        (err, mirror.join("HEAD").is_file(), new.exists())
    });
    cleanup();
    let (err, kept, new) = result.unwrap();
    eprintln!("{err}");
    assert!(
        err.contains("is full (") && err.contains("MiB free)"),
        "{err}"
    );
    assert!(!err.contains("clone"), "a reclone was tried: {err}");
    assert!(kept && !new);
}
