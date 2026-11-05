//! The command line against a fake daemon on a temporary Unix socket: the real binary where the
//! process matters (exit codes, streaming, stdio of in-place runs), `cli::run_args` in-process
//! where a test needs a fake repository reader.

mod common;

use common::{FakeDaemon, job, potluck, reminder, run};
use potluck::cli::{Env, run_args};
use potluck::proto::{Blocker, ClientRequest, NodeView, Request, Response, StatusView, WhyView};
use potluck::types::{
    Facts, Gpu, Grant, JobState, RepoRef, WorkspaceRecord, WorkspaceState, now_ms,
};
use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args([
            "-c",
            "user.email=t@example.com",
            "-c",
            "user.name=t",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A git repository on branch `feature` with `potluck.toml` committed.
fn repo_with(toml: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-q", "-b", "feature"]);
    std::fs::write(dir.path().join("potluck.toml"), toml).unwrap();
    git(dir.path(), &["add", "potluck.toml"]);
    git(dir.path(), &["commit", "-q", "-m", "recipes"]);
    dir
}

/// Stands in for gitinfo::inspect (another part's work): HEAD of the repository, no checks.
fn fake_inspect(
    cwd: &Path,
    without: &mut dyn FnMut(&str) -> anyhow::Result<Vec<String>>,
) -> anyhow::Result<potluck::gitinfo::Inspected> {
    let common = PathBuf::from(git(cwd, &["rev-parse", "--git-common-dir"]));
    let common = if common.is_absolute() {
        common
    } else {
        cwd.join(common)
    };
    let commit = git(cwd, &["rev-parse", "HEAD"]);
    // Here every pattern names one submodule, left out (gitinfo's own tests match them).
    let left_out = without(&commit)?;
    Ok(potluck::gitinfo::Inspected {
        repo: RepoRef {
            id: potluck::types::repo_id(&common),
            source: common,
            commit,
            branch: Some(git(cwd, &["symbolic-ref", "--short", "HEAD"])),
            submodules: vec![],
        },
        left_out,
    })
}

fn env_for(fake: &FakeDaemon, cwd: &Path) -> Env {
    Env {
        paths: fake.paths(),
        cwd: cwd.to_path_buf(),
        job: None,
        lease: None,
        workspace: None,
        inspect: fake_inspect,
    }
}

struct Out {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run_in(env: &Env, args: &[&str]) -> Out {
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let mut argv = vec!["potluck"];
    argv.extend_from_slice(args);
    let code = run_args(argv, env, &mut out, &mut err);
    Out {
        code,
        stdout: String::from_utf8(out).unwrap(),
        stderr: String::from_utf8(err).unwrap(),
    }
}

fn one_json(s: &str) -> Value {
    assert_eq!(s.lines().count(), 1, "one JSON document on one line: {s:?}");
    serde_json::from_str(s).unwrap()
}

const RECIPES: &str = r#"
[env]
A = "file"
B = "file"
C = "file"

[recipe.gpu-tests]
cmd = ["cargo", "nextest", "run"]
timeout = "15m"
priority = 4
gpu = ["nvidia"]
ram = "16G"
cpu = 4
on = { os = "linux" }
hold = "30m"
keep = ["target/junit.xml"]
env = { B = "recipe", C = "recipe" }

[recipe.quick]
cmd = ["true"]
"#;

fn dispatch_daemon() -> FakeDaemon {
    FakeDaemon::start(
        vec![reminder("msi-w7", 3_600_000)],
        |req, conn| match &req.req {
            Request::Dispatch { spec } => conn.send(Response::Dispatched {
                job: "studio-5".into(),
                existing: spec.key.as_deref() == Some("again"),
            }),
            Request::Get { job: id } => {
                let mut j = job(id, JobState::Queued, None);
                j.workspace = None;
                conn.send(Response::Job { job: Box::new(j) })
            }
            Request::Ping => conn.send(Response::Pong {
                version: "0".into(),
                node: "studio".into(),
            }),
            _ => conn.send(Response::Error {
                message: "unexpected".into(),
            }),
        },
    )
}

fn dispatched_spec(fake: &FakeDaemon) -> potluck::types::JobSpec {
    let reqs: Vec<ClientRequest> = fake.requests();
    reqs.iter()
        .find_map(|r| match &r.req {
            Request::Dispatch { spec } => Some((**spec).clone()),
            _ => None,
        })
        .expect("a dispatch request")
}

/// The daemon answers a dispatch once the mirrors are synced (a first clone, or waiting for a
/// workspace being seeded from the mirror), and a release once the workspace is deleted: both can
/// take longer than the client's usual wait for an answer. The CLI waits for them. It gave up
/// after 60 seconds and exited 2 while the daemon went on and queued the job, so a dispatch tried
/// again ran twice.
#[test]
fn dispatch_and_release_wait_for_a_slow_answer() {
    let slow = potluck::client::READ_TIMEOUT + Duration::from_secs(3);
    let fake = FakeDaemon::start(vec![], move |req, conn| match &req.req {
        Request::Dispatch { .. } => {
            std::thread::sleep(slow);
            conn.send(Response::Dispatched {
                job: "studio-5".into(),
                existing: false,
            })
        }
        Request::Release { .. } => {
            std::thread::sleep(slow);
            conn.send(Response::Ok)
        }
        Request::Get { job: id } => conn.send(Response::Job {
            job: Box::new(job(id, JobState::Queued, None)),
        }),
        _ => conn.send(Response::Error {
            message: "unexpected".into(),
        }),
    });
    let repo = repo_with(RECIPES);
    let dispatch = potluck(fake.home())
        .args(["dispatch", "--timeout", "1m", "--", "true"])
        .current_dir(repo.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let release = run(potluck(fake.home()).args(["release", "msi-w7"]));
    let dispatch = dispatch.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&dispatch.stderr);
    assert_eq!(
        (dispatch.status.code(), release.code),
        (Some(0), 0),
        "dispatch: {stderr}release: {}",
        release.stderr
    );
    assert!(
        String::from_utf8_lossy(&dispatch.stdout).starts_with("job studio-5\n"),
        "{stderr}"
    );
}

/// git's own variables (a hook exports GIT_DIR) don't change which repository's committed
/// potluck.toml a dispatch reads: the one it dispatches from.
#[test]
fn dispatch_reads_the_potluck_toml_of_its_own_repository() {
    let fake = dispatch_daemon();
    let repo = repo_with(RECIPES);
    let other = tempfile::tempdir().unwrap();
    git(other.path(), &["init", "-q"]);
    git(
        other.path(),
        &["commit", "-q", "--allow-empty", "-m", "other"],
    );
    let out = Command::new(env!("CARGO_BIN_EXE_potluck"))
        .args(["dispatch", "--timeout", "5m", "--", "true"])
        .current_dir(repo.path())
        .env("POTLUCK_HOME", fake.home())
        .env("POTLUCK_CONFIG", fake.home().join("node.toml"))
        .env("GIT_DIR", other.path().join(".git"))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let spec = dispatched_spec(&fake);
    assert_eq!(
        spec.env.get("A").map(String::as_str),
        Some("file"),
        "{spec:#?}"
    );
    // The reminders asked for are those of the repository dispatched from, too.
    for req in fake.requests() {
        assert_eq!(
            req.ctx.repo_id.as_deref(),
            Some(spec.repo.id.as_str()),
            "{req:?}"
        );
        assert_eq!(req.ctx.branch.as_deref(), Some("feature"), "{req:?}");
    }
}

/// From a subdirectory, a dispatch reads the potluck.toml committed at the top of the repository,
/// not one under the directory it runs in.
#[test]
fn dispatch_from_a_subdirectory_reads_the_top_level_potluck_toml() {
    let repo = repo_with(RECIPES);
    let sub = repo.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    std::fs::write(sub.join("potluck.toml"), "[env]\nA = \"sub\"\n").unwrap();
    git(repo.path(), &["add", "sub/potluck.toml"]);
    git(repo.path(), &["commit", "-q", "-m", "sub"]);

    let fake = dispatch_daemon();
    let out = run_in(
        &env_for(&fake, &sub),
        &["dispatch", "--timeout", "5m", "--", "true"],
    );
    assert_eq!(out.code, 0, "{}", out.stderr);
    let spec = dispatched_spec(&fake);
    assert_eq!(
        spec.env.get("A").map(String::as_str),
        Some("file"),
        "{spec:#?}"
    );

    std::fs::remove_file(sub.join("potluck.toml")).unwrap();
    git(repo.path(), &["rm", "-q", "--cached", "sub/potluck.toml"]);
    git(repo.path(), &["commit", "-q", "-m", "no sub"]);
    let fake = dispatch_daemon();
    let out = run_in(
        &env_for(&fake, &sub),
        &["dispatch", "quick", "--timeout", "5m"],
    );
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(dispatched_spec(&fake).cmd, ["true"]);
}

/// The submodules to leave out come from the recipe of the commit, or from the flags, which
/// replace them; dispatch says which were left out, and so does its JSON.
#[test]
fn dispatch_leaves_out_the_submodules_the_recipe_or_the_flags_name() {
    let fake = dispatch_daemon();
    let repo = repo_with(
        "[recipe.boot]\ncmd = [\"true\"]\ntimeout = \"5m\"\nwithout_submodules = [\"vendor/qemu/roms/*\"]\n",
    );
    let env = env_for(&fake, repo.path());
    let out = run_in(&env, &["dispatch", "boot"]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert!(
        out.stderr
            .contains("potluck: left out 1 submodule and what is nested in it: vendor/qemu/roms/*"),
        "{}",
        out.stderr
    );
    let out = run_in(
        &env,
        &[
            "--json",
            "dispatch",
            "boot",
            "--without-submodule",
            "vendor/qemu",
            "--without-submodule",
            "third_party/x",
        ],
    );
    assert_eq!(out.code, 0, "{}", out.stderr);
    let v: serde_json::Value = serde_json::from_str(&out.stdout).unwrap();
    assert_eq!(
        v["without_submodules"],
        serde_json::json!(["vendor/qemu", "third_party/x"])
    );
    // None left out: nothing said.
    let out = run_in(&env, &["dispatch", "--timeout", "1m", "--", "true"]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert!(!out.stderr.contains("left out"), "{}", out.stderr);
}

#[test]
fn dispatch_resolves_the_committed_recipe_under_the_flags() {
    let fake = dispatch_daemon();
    let repo = repo_with(RECIPES);
    // The working tree's edit must not count: only the committed file does.
    std::fs::write(
        repo.path().join("potluck.toml"),
        "[recipe.gpu-tests]\ncmd = [\"rm\", \"-rf\", \"/\"]\n",
    )
    .unwrap();
    let env = env_for(&fake, repo.path());
    let out = run_in(
        &env,
        &[
            "dispatch",
            "gpu-tests",
            "--timeout",
            "20m",
            "--env",
            "C=flag",
            "--on",
            "arch=x86_64",
            "--label",
            "who=me",
            "--key",
            "k1",
        ],
    );
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(out.stdout, "job studio-5\n");
    assert!(
        out.stderr
            .contains("workspace msi-w7 on msi is held for this branch for 1h more")
    );

    let spec = dispatched_spec(&fake);
    assert_eq!(spec.cmd, ["cargo", "nextest", "run"]);
    let env: Vec<(&str, &str)> = spec
        .env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert_eq!(env, [("A", "file"), ("B", "recipe"), ("C", "flag")]);
    assert_eq!(spec.timeout_ms, 20 * 60_000);
    assert_eq!(spec.priority, 4);
    assert_eq!(
        (
            spec.request.ram_mib,
            spec.request.cpu,
            spec.request.gpus.clone(),
            spec.request.exclusive
        ),
        (16 * 1024, 4, vec!["nvidia".to_string()], false)
    );
    assert_eq!(spec.on.get("os").map(String::as_str), Some("linux"));
    assert_eq!(spec.on.get("arch").map(String::as_str), Some("x86_64"));
    assert_eq!(spec.hold_ms, Some(30 * 60_000));
    assert_eq!(spec.keep, ["target/junit.xml"]);
    assert_eq!(spec.labels.get("who").map(String::as_str), Some("me"));
    assert_eq!(spec.key.as_deref(), Some("k1"));
    assert_eq!(spec.recipe.as_deref(), Some("gpu-tests"));
    assert!(spec.rerun_if_lost && !spec.kill_on_stall);
    assert_eq!(spec.repo.commit, git(repo.path(), &["rev-parse", "HEAD"]));
    assert_eq!(spec.repo.branch.as_deref(), Some("feature"));

    // The request's context names the caller's repository and branch, for its reminders.
    let ctx = &fake.requests()[0].ctx;
    assert_eq!(ctx.repo_id.as_deref(), Some(spec.repo.id.as_str()));
    assert_eq!(ctx.branch.as_deref(), Some("feature"));
}

#[test]
fn dispatch_flags_override_and_json() {
    let fake = dispatch_daemon();
    let repo = repo_with(RECIPES);
    let env = env_for(&fake, repo.path());
    let out = run_in(
        &env,
        &[
            "--json",
            "dispatch",
            "gpu-tests",
            "--ram",
            "512M",
            "--gpu",
            "amd",
            "--gpu",
            "any",
            "--exclusive",
            "--priority",
            "5",
            "--cpu",
            "2",
            "--keep",
            "a",
            "--keep",
            "b",
            "--workspace",
            "msi-w7",
            "--hold",
            "1m",
            "--kill-on-stall",
            "--key",
            "again",
            "--",
            "make",
            "check",
        ],
    );
    assert_eq!(out.code, 0, "{}", out.stderr);
    let doc = one_json(&out.stdout);
    assert_eq!(doc["job"], "studio-5");
    assert_eq!(doc["existing"], true);
    assert_eq!(doc["workspace"], "msi-w7");
    assert_eq!(doc["state"], "queued");
    assert_eq!(doc["reminders"][0]["workspace"], "msi-w7");
    assert_eq!(doc["reminders"][0]["release"], "potluck release msi-w7");
    assert!(
        out.stderr.is_empty(),
        "no reminders on stderr with --json: {}",
        out.stderr
    );

    let spec = dispatched_spec(&fake);
    assert_eq!(spec.cmd, ["make", "check"]);
    assert_eq!(spec.request.ram_mib, 512);
    assert_eq!(spec.request.gpus, ["amd", "any"]);
    assert!(spec.request.exclusive && spec.kill_on_stall);
    assert_eq!((spec.priority, spec.request.cpu), (5, 2));
    assert_eq!(spec.keep, ["a", "b"]);
    assert_eq!(spec.timeout_ms, 15 * 60_000, "the recipe's timeout");
    assert_eq!(spec.hold_ms, Some(5 * 60_000), "holds are at least 5m");
    assert_eq!(spec.workspace.as_deref(), Some("msi-w7"));
    assert!(
        !spec.rerun_if_lost,
        "a pinned workspace is never rerun elsewhere"
    );

    // Without a recipe: defaults, the file's env only.
    let fake = dispatch_daemon();
    let env = env_for(&fake, repo.path());
    let out = run_in(
        &env,
        &["dispatch", "--timeout", "90s", "--no-rerun", "--", "true"],
    );
    assert_eq!(out.code, 0, "{}", out.stderr);
    let spec = dispatched_spec(&fake);
    assert_eq!(
        (
            spec.timeout_ms,
            spec.priority,
            spec.request.ram_mib,
            spec.request.cpu
        ),
        (90_000, 3, 4096, 1)
    );
    assert_eq!(spec.env.len(), 3);
    assert!(!spec.rerun_if_lost && spec.recipe.is_none());
}

#[test]
fn dispatch_with_a_taken_key_names_that_jobs_state_in_words() {
    let fake = FakeDaemon::start(vec![], |req, conn| match &req.req {
        Request::Dispatch { .. } => conn.send(Response::Dispatched {
            job: "studio-5".into(),
            existing: true,
        }),
        Request::Get { job: id } => conn.send(Response::Job {
            job: Box::new(job(id, JobState::TimedOut, Some(124))),
        }),
        _ => conn.send(Response::Error {
            message: "unexpected".into(),
        }),
    });
    let repo = repo_with(RECIPES);
    let env = env_for(&fake, repo.path());
    let args = ["dispatch", "--key", "k", "--timeout", "5m", "--", "true"];
    let out = run_in(&env, &args);
    assert_eq!(
        out.stdout,
        "job studio-5\nworkspace msi-w7\n(a job with key k already existed; this is that job, timed out)\n"
    );
    let out = run_in(&env, &[&["--json"][..], &args].concat());
    assert_eq!(one_json(&out.stdout)["state"], "timed_out");
}

#[test]
fn dispatch_sends_os_shorthands_as_given() {
    let repo = repo_with(RECIPES);
    let t = ["dispatch", "--timeout", "5m"];
    for (args, want) in [
        (
            [&t[..], &["--on", "macos=27", "--", "true"]].concat(),
            &[("macos", "27")][..],
        ),
        (
            [
                &t[..],
                &["--on", "linux", "--on", "gpu=nvidia", "--", "true"],
            ]
            .concat(),
            &[("gpu", "nvidia"), ("linux", "")],
        ),
        (
            [&t[..], &["--on", "os_version=ubuntu-24", "--", "true"]].concat(),
            &[("os_version", "ubuntu-24")],
        ),
        // An --on naming the OS replaces the recipe's os = "linux".
        (
            vec!["dispatch", "gpu-tests", "--on", "macos"],
            &[("macos", "")],
        ),
        (
            vec!["dispatch", "gpu-tests", "--on", "linux=7.2"],
            &[("linux", "7.2")],
        ),
    ] {
        let fake = dispatch_daemon();
        let out = run_in(&env_for(&fake, repo.path()), &args);
        assert_eq!(out.code, 0, "{args:?}: {}", out.stderr);
        let spec = dispatched_spec(&fake);
        let on: Vec<(&str, &str)> = spec
            .on
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(on, want, "{args:?}");
    }
}

#[test]
fn dispatch_refuses_bad_options_before_sending_anything() {
    let fake = dispatch_daemon();
    let repo = repo_with(RECIPES);
    let env = env_for(&fake, repo.path());
    for (args, want) in [
        (
            &["dispatch", "--timeout", "600", "--", "true"][..],
            "use units",
        ),
        (&["dispatch", "--", "true"][..], "--timeout is required"),
        (
            &[
                "dispatch",
                "--timeout",
                "5m",
                "--priority",
                "9",
                "--",
                "true",
            ][..],
            "out of range",
        ),
        (
            &["dispatch", "nope", "--timeout", "5m"][..],
            "no recipe \"nope\" in the committed potluck.toml (it has: gpu-tests, quick)",
        ),
        (&["dispatch", "--timeout", "5m"][..], "no command"),
        (
            &[
                "dispatch",
                "--timeout",
                "5m",
                "--on",
                "colour=red",
                "--",
                "true",
            ][..],
            "unknown fact",
        ),
        (
            &[
                "dispatch",
                "--timeout",
                "5m",
                "--on",
                "macos",
                "--on",
                "linux",
                "--",
                "true",
            ][..],
            "--on: macos and linux contradict each other: a machine runs one OS",
        ),
        (
            &[
                "dispatch",
                "gpu-tests",
                "--on",
                "linux=7.2",
                "--on",
                "kernel=6",
            ][..],
            "--on: linux=7.2 and kernel=6 contradict each other: no kernel starts with both",
        ),
        (
            &["dispatch", "--timeout", "5m", "--on", "gpu", "--", "true"][..],
            "--on: gpu needs a value, gpu=VALUE (only macos and linux stand alone)",
        ),
        (
            &["dispatch", "--timeout", "5m", "--env", "NOEQ", "--", "true"][..],
            "expected KEY=VALUE",
        ),
        (
            &["dispatch", "--timeout", "5m", "--hold", "25h", "--", "true"][..],
            "at most 24h",
        ),
        (
            &["dispatch", "--timeout", "5m", "--ram", "lots", "--", "true"][..],
            "bad RAM amount",
        ),
        (
            &["dispatch", "--timeout", "5m", "--cpu", "0", "--", "true"][..],
            "cpu must be at least 1",
        ),
    ] {
        let out = run_in(&env, args);
        assert_eq!(out.code, 2, "{args:?}");
        assert!(out.stderr.contains(want), "{args:?}: {}", out.stderr);
        // Reminders follow errors too.
        assert!(
            out.stderr.contains("potluck release msi-w7"),
            "{args:?}: {}",
            out.stderr
        );
    }
    let out = run_in(
        &env,
        &["--json", "dispatch", "--timeout", "10", "--", "true"],
    );
    assert_eq!(out.code, 2);
    let doc = one_json(&out.stdout);
    assert!(doc["error"].as_str().unwrap().contains("use units"));
    assert_eq!(doc["reminders"][0]["workspace"], "msi-w7");
    assert!(
        !fake.ops().contains(&"dispatch".to_string()),
        "{:?}",
        fake.ops()
    );

    // A committed file with an unknown field, and a commit without the file.
    let bad = repo_with("[recipe.x]\ncmd = [\"a\"]\ntimout = \"5m\"\n");
    let out = run_in(
        &env_for(&fake, bad.path()),
        &["dispatch", "x", "--timeout", "5m"],
    );
    assert_eq!(out.code, 2);
    assert!(
        out.stderr.contains("unknown field `timout`"),
        "{}",
        out.stderr
    );
    let none = repo_with("");
    git(none.path(), &["rm", "-q", "potluck.toml"]);
    git(none.path(), &["commit", "-q", "-m", "no recipes"]);
    let out = run_in(
        &env_for(&fake, none.path()),
        &["dispatch", "x", "--timeout", "5m"],
    );
    assert_eq!(out.code, 2);
    assert!(out.stderr.contains("has no potluck.toml"), "{}", out.stderr);
}

/// A committed potluck.toml that is a directory, a symbolic link or a submodule is named as such,
/// not parsed: git shows a directory's listing or a link's target as text, and TOML said "key
/// with no value".
#[test]
fn a_potluck_toml_that_is_not_a_file_is_named_as_such() {
    let fake = dispatch_daemon();
    let tree = repo_with("");
    git(tree.path(), &["rm", "-q", "potluck.toml"]);
    std::fs::create_dir(tree.path().join("potluck.toml")).unwrap();
    std::fs::write(tree.path().join("potluck.toml/a"), "x\n").unwrap();
    git(tree.path(), &["add", "potluck.toml"]);
    git(tree.path(), &["commit", "-q", "-m", "a directory"]);
    let link = repo_with("");
    git(link.path(), &["rm", "-q", "potluck.toml"]);
    std::fs::write(link.path().join("real.toml"), RECIPES).unwrap();
    std::os::unix::fs::symlink("real.toml", link.path().join("potluck.toml")).unwrap();
    git(link.path(), &["add", "real.toml", "potluck.toml"]);
    git(link.path(), &["commit", "-q", "-m", "a link"]);
    for (repo, what) in [(&tree, "a directory"), (&link, "a symbolic link")] {
        for args in [
            &["dispatch", "quick", "--timeout", "5m"][..],
            &["dispatch", "--timeout", "5m", "--", "true"],
        ] {
            let out = run_in(&env_for(&fake, repo.path()), args);
            assert_eq!(out.code, 2, "{args:?}: {}", out.stderr);
            assert!(
                out.stderr.contains("potluck.toml at commit ")
                    && out.stderr.contains(&format!(" is {what}, not a file")),
                "{args:?}: {}",
                out.stderr
            );
        }
    }
    assert!(!fake.ops().contains(&"dispatch".to_string()));
}

#[test]
fn dispatch_leaves_the_owner_to_the_daemon_whatever_node_toml_says() {
    // The daemon numbers the job and owns it; a node.toml it refused, or one edited since it
    // started, must not put another machine's name in the record.
    let fake = dispatch_daemon();
    std::fs::write(fake.home().join("node.toml"), "name = \"box\"\nbogus = 1\n").unwrap();
    let repo = repo_with(RECIPES);
    let out = run_in(
        &env_for(&fake, repo.path()),
        &["dispatch", "--timeout", "5m", "--", "true"],
    );
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert_eq!(dispatched_spec(&fake).submitted_from, "");
}

#[test]
fn nested_dispatch_refuses_a_node_toml_it_cannot_read() {
    let grant = Grant {
        ram_mib: 8192,
        cpu: 1,
        gpus: vec![],
        exclusive: false,
    };
    let fake = lease_daemon(true, grant);
    std::fs::write(fake.home().join("node.toml"), "name = \"box\"\nbogus = 1\n").unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1m",
        "--on",
        "node=box",
        "--",
        "true",
    ]));
    assert_eq!(r.code, 2, "{}", r.stderr);
    assert!(
        r.stderr.contains("node.toml") && r.stderr.contains("bogus"),
        "names the config and its problem, not a fact it misread: {}",
        r.stderr
    );
}

#[test]
fn usage_errors_exit_2_and_json_errors_are_one_document() {
    let home = tempfile::tempdir().unwrap();
    let r = run(potluck(home.path()).args(["hold", "msi-w7", "600"]));
    assert_eq!(r.code, 2);
    assert!(r.stderr.contains("use units"), "{}", r.stderr);
    let r = run(potluck(home.path()).args(["--json", "wait", "j", "--max-wait", "10"]));
    assert_eq!(r.code, 2);
    let doc = one_json(&r.stdout);
    assert!(
        doc["error"].as_str().unwrap().contains("--max-wait"),
        "{doc}"
    );
    assert_eq!(doc["reminders"], Value::Array(vec![]));
    let r = run(potluck(home.path()).args(["dispatch", "--bogus"]));
    assert_eq!(r.code, 2);
    assert!(r.stderr.contains("--bogus"));
    let r = run(potluck(home.path()).args(["dispatch", "--json", "--bogus"]));
    assert_eq!(r.code, 2);
    assert!(
        one_json(&r.stdout)["error"]
            .as_str()
            .unwrap()
            .contains("--bogus")
    );
    let r = run(potluck(home.path()).args(["status"]));
    assert_eq!(r.code, 2);
    assert!(
        r.stderr.contains("the potluck daemon is not running"),
        "{}",
        r.stderr
    );
    assert!(r.stderr.contains("potluck service install"), "{}", r.stderr);
    // A socket someone listens on but this user may not use: not "not running".
    let sock = home.path().join("potluck.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
    std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o000)).unwrap();
    let r = run(potluck(home.path()).args(["status"]));
    assert_eq!(r.code, 2);
    assert!(
        r.stderr.contains(&format!(
            "can't connect to the potluck daemon: {}: Permission denied",
            sock.display()
        )) && !r.stderr.contains("not running"),
        "{}",
        r.stderr
    );
    let r = run(potluck(home.path()).args(["--help"]));
    assert_eq!(r.code, 0);
    assert!(r.stdout.contains("dispatch"));
}

fn wait_daemon(end: JobState, code: Option<i32>) -> FakeDaemon {
    FakeDaemon::start(
        vec![reminder("msi-w7", 125 * 60_000)],
        move |req, conn| match &req.req {
            Request::Logs { offset: 0, .. } => conn.send_with(
                Response::Bytes {
                    offset: 0,
                    len: 6,
                    done: false,
                },
                b"hello\n",
            ),
            Request::Logs { offset: 6, .. } => conn.send_with(
                Response::Bytes {
                    offset: 6,
                    len: 6,
                    done: true,
                },
                b"world\n",
            ),
            Request::Wait { job: id, .. } => {
                let mut j = job(id, end, code);
                if end == JobState::TimedOut {
                    j.message = Some("killed at its run limit (10m)".into());
                }
                conn.send(Response::Job { job: Box::new(j) })
            }
            _ => conn.send(Response::Error {
                message: "unexpected".into(),
            }),
        },
    )
}

#[test]
fn wait_streams_the_output_and_exits_with_the_jobs_code() {
    let fake = wait_daemon(JobState::Failed, Some(7));
    let r = run(potluck(fake.home()).args(["wait", "studio-1"]));
    assert_eq!(r.code, 7, "{}", r.stderr);
    assert_eq!(r.stdout, "hello\nworld\n");
    assert!(
        r.stderr
            .contains("potluck: job studio-1 failed, exit code 7"),
        "{}",
        r.stderr
    );
    assert!(
        r.stderr.contains("potluck: workspace msi-w7 on msi is held for this branch for 2h5m more (commit 0123456789ab, 3.0 GiB); `potluck release msi-w7` ends it"),
        "{}",
        r.stderr
    );
    assert_eq!(fake.ops(), ["logs", "logs", "wait"]);
    let logs: Vec<(u64, u64)> = fake
        .requests()
        .iter()
        .filter_map(|r| match r.req {
            Request::Logs {
                offset, wait_ms, ..
            } => Some((offset, wait_ms)),
            _ => None,
        })
        .collect();
    assert!(
        logs[0].0 == 0 && logs[0].1 > 0,
        "long poll from offset 0: {logs:?}"
    );

    // --json: the output goes to stderr, stdout holds one document.
    let r = run(potluck(fake.home()).args(["wait", "--json", "studio-1"]));
    assert_eq!(r.code, 7);
    assert_eq!(r.stderr, "hello\nworld\n");
    let doc = one_json(&r.stdout);
    assert_eq!(
        (doc["exit_code"].as_i64(), doc["ended"].as_bool()),
        (Some(7), Some(true))
    );
    assert_eq!(doc["state"], "failed");
    assert_eq!(doc["reminders"][0]["remaining_ms"], 125 * 60_000);

    // -q: no output, same code; endings potluck causes have fixed codes.
    let fake = wait_daemon(JobState::TimedOut, None);
    let r = run(potluck(fake.home()).args(["wait", "-q", "studio-2"]));
    assert_eq!(r.code, 124);
    assert_eq!(r.stdout, "");
    assert!(
        r.stderr.contains(
            "potluck: job studio-2 timed out: killed at its run limit (10m), exit code 124"
        ),
        "{}",
        r.stderr
    );
    assert_eq!(fake.ops(), ["wait"]);
    let fake = wait_daemon(JobState::Succeeded, Some(0));
    assert_eq!(run(potluck(fake.home()).args(["wait", "studio-3"])).code, 0);
}

/// The line `wait` ends with says how the job ended once, in words, with the code it exits with.
#[test]
fn wait_says_how_a_job_ended_in_one_plain_line() {
    let ended = |state: JobState, code: Option<i32>, message: &str| {
        let mut j = job("studio-1", state, code);
        j.message = Some(message.to_string());
        j
    };
    let cases = [
        (
            ended(JobState::Succeeded, Some(0), "exited with code 0"),
            "job studio-1 succeeded, exit code 0",
        ),
        (
            ended(JobState::Failed, Some(3), "exited with code 3"),
            "job studio-1 failed, exit code 3",
        ),
        (
            ended(JobState::Failed, None, "killed by signal 9 (SIGKILL)"),
            "job studio-1 failed: killed by signal 9 (SIGKILL), exit code 1",
        ),
        (
            ended(JobState::Cancelled, Some(130), "cancelled"),
            "job studio-1 cancelled, exit code 130",
        ),
        (
            ended(JobState::Cancelled, Some(130), "cancelled while queued"),
            "job studio-1 cancelled while queued, exit code 130",
        ),
        (
            ended(
                JobState::OverMemory,
                Some(137),
                "over memory: used 300 MiB of its 64 MiB",
            ),
            "job studio-1 over memory: used 300 MiB of its 64 MiB, exit code 137",
        ),
        (
            ended(
                JobState::Lost,
                Some(125),
                "no machine that can run it is up (mini was lost)",
            ),
            "job studio-1 lost: no machine that can run it is up (mini was lost), exit code 125",
        ),
    ];
    for (record, want) in cases {
        let fake = FakeDaemon::start(vec![], move |req, conn| match &req.req {
            Request::Wait { .. } => conn.send(Response::Job {
                job: Box::new(record.clone()),
            }),
            _ => conn.send(Response::Error {
                message: "unexpected".into(),
            }),
        });
        let r = run(potluck(fake.home()).args(["wait", "-q", "studio-1"]));
        assert_eq!(r.stderr, format!("potluck: {want}\n"));
    }
}

/// A daemon restart (after an update) closes the connection; jobs survive it, and so does
/// `wait`: it connects again and goes on from the last byte it wrote.
#[test]
fn wait_and_logs_go_on_after_the_daemon_hangs_up() {
    let hung_up = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fake = {
        let hung_up = hung_up.clone();
        FakeDaemon::start(vec![], move |req, conn| match &req.req {
            // Half of the first chunk, then the connection breaks.
            Request::Logs { offset: 0, .. } => {
                conn.send_with(
                    Response::Bytes {
                        offset: 0,
                        len: 6,
                        done: false,
                    },
                    b"hel",
                );
                conn.hang_up();
            }
            Request::Logs { offset: 3, .. } => conn.send_with(
                Response::Bytes {
                    offset: 3,
                    len: 7,
                    done: true,
                },
                b"lo\nend\n",
            ),
            Request::Wait { job: id, .. } => {
                if !hung_up.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    conn.hang_up();
                } else {
                    conn.send(Response::Job {
                        job: Box::new(job(id, JobState::Succeeded, Some(0))),
                    })
                }
            }
            _ => conn.send(Response::Error {
                message: "unexpected".into(),
            }),
        })
    };
    let r = run(potluck(fake.home()).args(["wait", "studio-1"]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.stdout, "hello\nend\n");
    assert_eq!(fake.ops(), ["logs", "logs", "wait", "wait"]);
}

/// A daemon that stops for a restart (a plain one: the new binary can't take a handover, or the
/// service is restarted) answers a request that arrives meanwhile with "the daemon is stopping"
/// and closes the connection: `wait` and `logs -f` connect again and go on, as after a hang-up.
/// They exited 2 with that answer (seen in a cluster-wide update to a binary that can't take
/// over: 1 of 13 clients).
#[test]
fn wait_and_logs_go_on_after_the_daemon_says_it_is_stopping() {
    let stopping = |conn: &mut common::Conn| {
        conn.send(Response::Error {
            message: "the daemon is stopping".into(),
        });
        conn.hang_up();
    };
    let said = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let fake = {
        let said = said.clone();
        FakeDaemon::start(vec![], move |req, conn| {
            let first = |n: u32| said.fetch_or(n, std::sync::atomic::Ordering::SeqCst) & n == 0;
            match &req.req {
                Request::Logs { offset: 0, .. } if first(1) => stopping(conn),
                Request::Logs { offset: 0, .. } => conn.send_with(
                    Response::Bytes {
                        offset: 0,
                        len: 6,
                        done: true,
                    },
                    b"hello\n",
                ),
                Request::Wait { .. } if first(2) => stopping(conn),
                Request::Wait { job: id, .. } => conn.send(Response::Job {
                    job: Box::new(job(id, JobState::Failed, Some(3))),
                }),
                _ => conn.send(Response::Error {
                    message: "unexpected".into(),
                }),
            }
        })
    };
    let r = run(potluck(fake.home()).args(["wait", "studio-1"]));
    assert_eq!((r.code, r.stdout.as_str()), (3, "hello\n"), "{}", r.stderr);
    assert_eq!(fake.ops(), ["logs", "logs", "wait", "wait"]);
    said.store(0, std::sync::atomic::Ordering::SeqCst);
    let r = run(potluck(fake.home()).args(["logs", "-f", "studio-1"]));
    assert_eq!((r.code, r.stdout.as_str()), (0, "hello\n"), "{}", r.stderr);
}

#[test]
fn wait_gives_up_at_max_wait_with_exit_3() {
    let fake = FakeDaemon::start(vec![], |req, conn| match &req.req {
        Request::Logs {
            offset, wait_ms, ..
        } => {
            std::thread::sleep(std::time::Duration::from_millis((*wait_ms).min(300)));
            conn.send(Response::Bytes {
                offset: *offset,
                len: 0,
                done: false,
            })
        }
        Request::Wait {
            job: id,
            timeout_ms,
        } => {
            std::thread::sleep(std::time::Duration::from_millis(
                timeout_ms.unwrap_or(0).min(300),
            ));
            conn.send(Response::Job {
                job: Box::new(job(id, JobState::Running, None)),
            })
        }
        _ => conn.send(Response::Error {
            message: "unexpected".into(),
        }),
    });
    let started = std::time::Instant::now();
    let r = run(potluck(fake.home()).args(["wait", "studio-1", "--max-wait", "1s"]));
    assert_eq!(r.code, 3, "{}", r.stderr);
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert!(
        r.stderr
            .contains("job studio-1 is still running after 1s; it keeps running"),
        "{}",
        r.stderr
    );
    let r =
        run(potluck(fake.home()).args(["--json", "wait", "-q", "studio-1", "--max-wait", "1s"]));
    assert_eq!(r.code, 3);
    assert_eq!(one_json(&r.stdout)["ended"], false);
}

#[test]
fn logs_prints_what_there_is_or_follows_to_the_end() {
    let fake = FakeDaemon::start(vec![], |req, conn| match &req.req {
        Request::Logs { offset: 0, .. } => conn.send_with(
            Response::Bytes {
                offset: 0,
                len: 2,
                done: false,
            },
            b"a\n",
        ),
        Request::Logs {
            offset: 2,
            wait_ms: 0,
            ..
        } => conn.send(Response::Bytes {
            offset: 2,
            len: 0,
            done: false,
        }),
        Request::Logs { offset: 2, .. } => conn.send_with(
            Response::Bytes {
                offset: 2,
                len: 2,
                done: true,
            },
            b"b\n",
        ),
        _ => conn.send(Response::Error {
            message: "unexpected".into(),
        }),
    });
    let r = run(potluck(fake.home()).args(["logs", "studio-1"]));
    assert_eq!((r.code, r.stdout.as_str()), (0, "a\n"), "{}", r.stderr);
    let r = run(potluck(fake.home()).args(["logs", "-f", "studio-1"]));
    assert_eq!((r.code, r.stdout.as_str()), (0, "a\nb\n"), "{}", r.stderr);
    let r = run(potluck(fake.home()).args(["logs", "--follow", "--json", "studio-1"]));
    let doc = one_json(&r.stdout);
    assert_eq!(
        (
            doc["output"].as_str(),
            doc["done"].as_bool(),
            doc["bytes"].as_u64()
        ),
        (Some("a\nb\n"), Some(true), Some(4))
    );
}

/// A fake daemon serving a log of `len` bytes, `byte_at(i)` each, at most `piece` bytes per
/// answer (fewer than asked, so pieces are cut anywhere), done at the end; at offset `fail_at`,
/// an error. The bytes are made as they are asked for: the test process holds none of a large
/// log.
fn log_daemon(
    len: usize,
    byte_at: impl Fn(usize) -> u8 + Send + Sync + 'static,
    piece: usize,
    fail_at: Option<u64>,
) -> FakeDaemon {
    FakeDaemon::start(vec![reminder("studio-w1", 60_000)], move |req, conn| {
        let Request::Logs { offset, max, .. } = &req.req else {
            return conn.send(Response::Error {
                message: "unexpected".into(),
            });
        };
        if fail_at == Some(*offset) {
            return conn.send(Response::Error {
                message: "the log went away".into(),
            });
        }
        let at = (*offset as usize).min(len);
        let end = (at + piece.min(*max as usize)).min(len);
        let bytes: Vec<u8> = (at..end).map(&byte_at).collect();
        conn.send_with(
            Response::Bytes {
                offset: at as u64,
                len: (end - at) as u64,
                done: end == len,
            },
            &bytes,
        );
    })
}

/// [`log_daemon`] for a log held in memory.
fn log_daemon_of(log: Vec<u8>, piece: usize, fail_at: Option<u64>) -> FakeDaemon {
    let len = log.len();
    log_daemon(len, move |i| log[i], piece, fail_at)
}

/// `logs --json` writes its document a piece at a time: the text is what decoding the whole log
/// at once gives (sequences cut between pieces put back together, invalid bytes as U+FFFD,
/// control characters and quotes escaped), and the document carries the reminders.
#[test]
fn logs_json_is_the_whole_log_decoded_at_once() {
    let mut log = Vec::new();
    for i in 0..200 {
        log.extend_from_slice(format!("line {i}: é→✓ 𝄞 \"q\" \\ \t\x01").as_bytes());
        log.extend_from_slice(&[0xff, b'x', 0xe2, 0x82, b'\n']);
    }
    // Ends inside a sequence.
    log.extend_from_slice(&[0xf0, 0x9d]);
    let fake = log_daemon_of(log.clone(), 7, None);
    let r = run(potluck(fake.home()).args(["logs", "--json", "studio-1"]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    let doc = one_json(&r.stdout);
    assert_eq!(
        doc["output"].as_str().unwrap(),
        String::from_utf8_lossy(&log)
    );
    assert_eq!(doc["bytes"].as_u64(), Some(log.len() as u64));
    assert_eq!(doc["done"], true);
    assert_eq!(doc["job"], "studio-1");
    assert_eq!(doc["reminders"][0]["workspace"], "studio-w1", "{doc}");
}

/// An error once the document has begun ends it with an `error` field: still one JSON document,
/// with the output so far, and exit code 2.
#[test]
fn logs_json_ends_its_document_on_an_error() {
    let fake = log_daemon_of(b"0123456789".to_vec(), 4, Some(8));
    let r = run(potluck(fake.home()).args(["logs", "--json", "-f", "studio-1"]));
    assert_eq!(r.code, 2, "{}", r.stderr);
    let doc = one_json(&r.stdout);
    assert_eq!(doc["output"], "01234567");
    assert_eq!(doc["bytes"], 8);
    assert_eq!(doc["done"], false);
    assert_eq!(doc["error"], "the log went away");
    // An error before any output: the usual error document.
    let fake = log_daemon_of(b"0123".to_vec(), 4, Some(0));
    let r = run(potluck(fake.home()).args(["logs", "--json", "studio-1"]));
    assert_eq!(r.code, 2);
    let doc = one_json(&r.stdout);
    assert_eq!(doc["error"], "the log went away");
    assert!(doc.get("output").is_none(), "{doc}");
}

/// `logs --json` of a large log holds a piece of it at a time, not all of it (a 300 MB log
/// used to take 606 MB).
#[test]
fn logs_json_of_a_large_log_takes_little_memory() {
    const SIZE: usize = 96 << 20;
    let fake = log_daemon(SIZE, |_| b'a', 1 << 20, None);
    let dir = tempfile::tempdir().unwrap();
    let out = std::fs::File::create(dir.path().join("out.json")).unwrap();
    let mut child = potluck(fake.home())
        .args(["logs", "--json", "studio-1"])
        .stdout(out)
        .spawn()
        .unwrap();
    let peak = peak_memory(&mut child);
    assert!(
        peak < (SIZE as u64) / 2,
        "{} MiB for a {} MiB log",
        peak >> 20,
        SIZE >> 20
    );
    let written = std::fs::read(dir.path().join("out.json")).unwrap();
    let head = b"{\"job\":\"studio-1\",\"output\":\"aaa";
    assert!(written.starts_with(head));
    let tail = format!("aaa\",\"bytes\":{SIZE},\"done\":true,\"reminders\":");
    let at = written.len() - 300;
    assert!(
        String::from_utf8_lossy(&written[at..]).contains(&tail),
        "{}",
        String::from_utf8_lossy(&written[at..])
    );
}

/// Waits for `child` to exit 0 and returns the most memory it held. macOS: its peak resident
/// size from wait4. Linux: its VmHWM, read until it exits, because the kernel's ru_maxrss of a
/// child includes its parent's peak at the fork (this test process).
fn peak_memory(child: &mut std::process::Child) -> u64 {
    #[cfg(target_os = "macos")]
    {
        let mut status = 0;
        // SAFETY: rusage is plain data; wait4 reaps our own child and fills it in.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        let pid = unsafe { libc::wait4(child.id() as i32, &mut status, 0, &mut usage) };
        assert_eq!(pid, child.id() as i32);
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
        usage.ru_maxrss as u64
    }
    #[cfg(target_os = "linux")]
    {
        let mut peak = 0u64;
        loop {
            let status = std::fs::read_to_string(format!("/proc/{}/status", child.id()));
            let hwm = status.ok().and_then(|s| {
                s.lines()
                    .find_map(|l| l.strip_prefix("VmHWM:"))
                    .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
            });
            if let Some(kib) = hwm {
                peak = peak.max(kib * 1024);
            }
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                return peak;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

fn copy_daemon(evil: &'static str) -> FakeDaemon {
    FakeDaemon::start(vec![], move |req, conn| {
        let Request::Copy { path, .. } = &req.req else {
            return conn.send(Response::Error {
                message: "unexpected".into(),
            });
        };
        let entry = |p: &str, mode: u32, dir: bool, len: u64| Response::CopyEntry {
            path: p.into(),
            mode,
            dir,
            len,
        };
        if path == "one.txt" {
            conn.send_with(entry("", 0o600, false, 3), b"one");
            return conn.send(Response::CopyDone);
        }
        if path == "special" {
            conn.send(entry("", 0o3775, true, 0));
            conn.send_with(entry("suid", 0o4755, false, 1), b"x");
            conn.send_with(entry("sgid", 0o2755, false, 1), b"y");
            return conn.send(Response::CopyDone);
        }
        conn.send(entry("", 0o755, true, 0));
        conn.send(entry("bin", 0o700, true, 0));
        conn.send_with(entry("bin/app", 0o755, false, 4), b"\x7fELF");
        conn.send_with(entry("README", 0o444, false, 5), b"hello");
        if !evil.is_empty() {
            conn.send_with(entry(evil, 0o644, false, 4), b"evil");
        }
        conn.send_with(entry("z/late.txt", 0o644, false, 4), b"late");
        conn.send(Response::CopyDone);
    })
}

fn mode(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o7777
}

/// A file that can't be written (here: past a file size limit) is named in the error, not
/// left to a bare "File too large (os error 27)".
#[test]
fn cp_names_the_file_it_could_not_write() {
    let fake = FakeDaemon::start(vec![], |req, conn| {
        if let Request::Copy { .. } = &req.req {
            let big = vec![b'x'; 200_000];
            conn.send_with(
                Response::CopyEntry {
                    path: "".into(),
                    mode: 0o644,
                    dir: false,
                    len: big.len() as u64,
                },
                &big,
            );
            conn.send(Response::CopyDone);
        }
    });
    let dest = tempfile::tempdir().unwrap();
    let target = dest.path().join("big");
    let r = run(Command::new("sh")
        .args([
            "-c",
            "trap '' XFSZ; ulimit -f 8; exec \"$0\" cp msi-w7:big \"$1\"",
            env!("CARGO_BIN_EXE_potluck"),
            target.to_str().unwrap(),
        ])
        .env("POTLUCK_HOME", fake.home())
        .env("POTLUCK_CONFIG", fake.home().join("node.toml"))
        .stdin(std::process::Stdio::null()));
    assert_eq!(r.code, 2, "{}", r.stderr);
    assert!(
        r.stderr
            .contains(&format!("potluck: {}: File too large", target.display())),
        "{}",
        r.stderr
    );
}

/// A command run from a directory that was removed says so, not a bare "No such file or
/// directory (os error 2)".
#[test]
fn a_removed_working_directory_is_named() {
    let home = tempfile::tempdir().unwrap();
    let gone = home.path().join("gone");
    std::fs::create_dir(&gone).unwrap();
    let r = run(Command::new("sh")
        .args([
            "-c",
            "cd \"$1\" && rmdir \"$1\" && exec \"$0\" status",
            env!("CARGO_BIN_EXE_potluck"),
            gone.to_str().unwrap(),
        ])
        .env("POTLUCK_HOME", home.path())
        .env("POTLUCK_CONFIG", home.path().join("node.toml"))
        .stdin(std::process::Stdio::null()));
    assert_eq!(r.code, 2, "{}", r.stderr);
    assert!(
        r.stderr
            .starts_with("potluck: can't read the current directory (was it removed?): "),
        "{}",
        r.stderr
    );
}

/// A log of 2 MiB in 256 KiB pieces, then the job ended with code 3.
fn long_log_daemon() -> FakeDaemon {
    FakeDaemon::start(vec![], |req, conn| match &req.req {
        Request::Logs { offset, .. } => {
            let piece = if *offset < 2 << 20 { 256 << 10 } else { 0 };
            conn.send_with(
                Response::Bytes {
                    offset: *offset,
                    len: piece as u64,
                    done: piece == 0,
                },
                &vec![b'x'; piece],
            );
        }
        Request::Wait { job: id, .. } => conn.send(Response::Job {
            job: Box::new(job(id, JobState::Failed, Some(3))),
        }),
        _ => conn.send(Response::Error {
            message: "unexpected".into(),
        }),
    })
}

/// Output read only in part (`potluck logs J | head`) isn't an error: `logs` stops quietly, with
/// `--json` too, and `wait` goes on waiting and exits with the job's code, as when its output is
/// read in full.
#[test]
fn output_read_in_part_is_not_an_error() {
    use std::io::Read;
    let fake = long_log_daemon();
    for (args, want) in [
        (&["logs", "studio-1"][..], 0),
        (&["--json", "logs", "studio-1"], 0),
        (&["wait", "studio-1"], 3),
    ] {
        let mut child = potluck(fake.home())
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut first = [0u8; 10];
        stdout.read_exact(&mut first).unwrap();
        drop(stdout);
        let out = child.wait_with_output().unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(want), "{args:?}: {stderr}");
        assert!(!stderr.contains("Broken pipe"), "{args:?}: {stderr}");
    }
}

/// Output that takes no bytes: `Full` fails every write as a full disk does (`> /dev/full`),
/// `Gone` as a pipe nobody reads.
struct Unwritable(std::io::ErrorKind);

impl std::io::Write for Unwritable {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        Err(match self.0 {
            std::io::ErrorKind::BrokenPipe => std::io::ErrorKind::BrokenPipe.into(),
            _ => std::io::Error::from_raw_os_error(libc::ENOSPC),
        })
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Output that can't be written (a full disk) is an error said on stderr, exit 2, for text and
/// --json alike, also when the output is itself an error document. It used to exit 0 (or 2)
/// with nothing written and nothing said. A pipe nobody reads any more stays quiet.
#[test]
fn output_that_cannot_be_written_is_an_error() {
    let fake = FakeDaemon::start(vec![], |req, conn| match &req.req {
        Request::Nodes => conn.send(Response::Nodes { nodes: vec![] }),
        Request::Logs { offset, .. } => conn.send_with(
            Response::Bytes {
                offset: *offset,
                len: if *offset == 0 { 3 } else { 0 },
                done: true,
            },
            if *offset == 0 { b"abc" } else { b"" },
        ),
        _ => conn.send(Response::Error {
            message: "no job x-1".into(),
        }),
    });
    let cwd = tempfile::tempdir().unwrap();
    let env = env_for(&fake, cwd.path());
    let run_into = |kind, args: &[&str]| {
        let (mut out, mut err) = (Unwritable(kind), Vec::new());
        let mut argv = vec!["potluck"];
        argv.extend_from_slice(args);
        let code = run_args(argv, &env, &mut out, &mut err);
        (code, String::from_utf8(err).unwrap())
    };
    let full = std::io::ErrorKind::StorageFull;
    for args in [
        &["nodes"][..],
        &["--json", "nodes"],
        &["logs", "studio-1"],
        &["--json", "logs", "studio-1"],
    ] {
        let (code, stderr) = run_into(full, args);
        assert_eq!(code, 2, "{args:?}: {stderr}");
        assert!(
            (stderr.contains("can't write the output")
                || stderr.contains("writing the job's output"))
                && stderr.contains("os error 28"),
            "{args:?}: {stderr}"
        );
        let (code, stderr) = run_into(std::io::ErrorKind::BrokenPipe, args);
        assert_eq!((code, stderr.as_str()), (0, ""), "{args:?}");
    }
    // The error document itself.
    let (code, stderr) = run_into(full, &["--json", "why", "x-1"]);
    assert_eq!(code, 2, "{stderr}");
    assert!(stderr.contains("no job x-1"), "{stderr}");
}

/// A usage error with --json goes to stdout as a JSON document; when nobody reads stdout any more
/// it is said on stderr, not a panic ("failed printing to stdout", exit 101).
#[test]
fn a_usage_error_nobody_reads_is_not_a_panic() {
    let home = tempfile::tempdir().unwrap();
    for _ in 0..3 {
        // Its stdout is a pipe already closed for reading, so the write fails however soon it
        // comes.
        let (reader, writer) = std::io::pipe().unwrap();
        drop(reader);
        let out = potluck(home.path())
            .args(["--json", "no-such-command"])
            .stdout(writer)
            .stderr(std::process::Stdio::piped())
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{stderr}");
        assert!(!stderr.contains("panicked"), "{stderr}");
        assert!(stderr.contains("no-such-command"), "{stderr}");
    }
}

/// `--help` and `--version` are output like any command's: into a full disk they are an error said
/// on stderr, exit 2 (they used to exit 0 with nothing written and nothing said); into a pipe
/// nobody reads any more they stay quiet, exit 0.
#[test]
fn help_that_cannot_be_written_is_an_error() {
    let home = tempfile::tempdir().unwrap();
    let cases: [&[&str]; 4] = [
        &["--help"],
        &["--version"],
        &["help"],
        &["dispatch", "--help"],
    ];
    for args in cases {
        let (reader, writer) = std::io::pipe().unwrap();
        drop(reader);
        let out = potluck(home.path())
            .args(args)
            .stdout(writer)
            .stderr(std::process::Stdio::piped())
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!((out.status.code(), &*stderr), (Some(0), ""), "{args:?}");
        // Linux has a full disk at hand.
        let Ok(full) = std::fs::OpenOptions::new().write(true).open("/dev/full") else {
            continue;
        };
        let out = potluck(home.path())
            .args(args)
            .stdout(full)
            .stderr(std::process::Stdio::piped())
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {stderr}");
        assert!(
            stderr.contains("can't write the output") && stderr.contains("os error 28"),
            "{args:?}: {stderr}"
        );
    }
}

#[test]
fn cp_writes_directories_files_and_modes() {
    let fake = copy_daemon("");
    let dest = tempfile::tempdir().unwrap();
    // DEST does not exist yet: the copy becomes it.
    let r =
        run(potluck(fake.home())
            .current_dir(dest.path())
            .args(["cp", "msi-w7:target/out", "out"]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    let out = dest.path().join("out");
    assert_eq!(std::fs::read(out.join("bin/app")).unwrap(), b"\x7fELF");
    assert_eq!(
        std::fs::read_to_string(out.join("README")).unwrap(),
        "hello"
    );
    assert_eq!(
        std::fs::read_to_string(out.join("z/late.txt")).unwrap(),
        "late"
    );
    assert_eq!(
        (
            mode(&out),
            mode(&out.join("bin")),
            mode(&out.join("bin/app")),
            mode(&out.join("README"))
        ),
        (0o755, 0o700, 0o755, 0o444)
    );
    assert!(r.stdout.contains("copied 3 files"), "{}", r.stdout);
    // Again into the now existing directory: it lands inside, under the source's name; a
    // read-only file from before is replaced.
    let r = run(potluck(fake.home()).current_dir(dest.path()).args([
        "--json",
        "cp",
        "msi-w7:target/out",
        "out",
    ]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    let doc = one_json(&r.stdout);
    assert_eq!(
        (
            doc["files"].as_u64(),
            doc["dirs"].as_u64(),
            doc["bytes"].as_u64()
        ),
        (Some(3), Some(2), Some(13))
    );
    assert!(out.join("out/bin/app").is_file());
    // A single file into a directory.
    let r = run(potluck(fake.home())
        .args(["cp", "msi-w7:one.txt"])
        .arg(dest.path()));
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(
        std::fs::read_to_string(dest.path().join("one.txt")).unwrap(),
        "one"
    );
    assert_eq!(mode(&dest.path().join("one.txt")), 0o600);
}

/// As for kept files: setuid, setgid and sticky bits from a workspace are not kept.
#[test]
fn cp_drops_setuid_setgid_and_sticky_bits() {
    let fake = copy_daemon("");
    let dest = tempfile::tempdir().unwrap();
    let r =
        run(potluck(fake.home())
            .current_dir(dest.path())
            .args(["cp", "msi-w7:special", "out"]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    let out = dest.path().join("out");
    assert_eq!(
        (mode(&out), mode(&out.join("suid")), mode(&out.join("sgid"))),
        (0o775, 0o755, 0o755)
    );
}

#[test]
fn cp_names_where_the_copy_landed_without_dot_components() {
    let fake = copy_daemon("");
    let dest = tempfile::tempdir().unwrap();
    std::fs::create_dir(dest.path().join("sub")).unwrap();
    for (to, want) in [(".", "one.txt"), ("./sub/.", "sub/one.txt")] {
        let r =
            run(potluck(fake.home())
                .current_dir(dest.path())
                .args(["cp", "msi-w7:one.txt", to]));
        assert_eq!(r.code, 0, "{}", r.stderr);
        assert!(dest.path().join(want).is_file());
        assert!(
            r.stdout.ends_with(&format!("/{want}\n")) && !r.stdout.contains("/./"),
            "{to}: {}",
            r.stdout
        );
    }
    // DEST/ names a directory, as for cp(1): it must exist.
    let r =
        run(potluck(fake.home())
            .current_dir(dest.path())
            .args(["cp", "msi-w7:one.txt", "nodir/"]));
    assert_eq!(r.code, 2);
    assert!(
        r.stderr.contains("nodir/ is not a directory"),
        "{}",
        r.stderr
    );
    assert!(!dest.path().join("nodir").exists());
}

#[test]
fn cp_refuses_paths_that_leave_the_copy() {
    for evil in [
        "../escaped.txt",
        "bin/../../escaped.txt",
        "/tmp/escaped.txt",
    ] {
        let fake = copy_daemon(evil);
        let dest = tempfile::tempdir().unwrap();
        let r = run(potluck(fake.home()).current_dir(dest.path()).args([
            "cp",
            "msi-w7:target/out",
            "sub/out",
        ]));
        assert_eq!(r.code, 2, "{evil}: {}", r.stderr);
        assert!(r.stderr.contains("refusing"), "{evil}: {}", r.stderr);
        assert!(
            !dest.path().join("escaped.txt").exists()
                && !dest.path().join("sub/escaped.txt").exists()
        );
        assert!(
            !dest.path().join("sub/out/z/late.txt").exists(),
            "stops at the bad entry"
        );
    }
    let home = tempfile::tempdir().unwrap();
    for bad in ["msi-w7:../x", "msi-w7:/etc", "msi-w7", ":x"] {
        let r = run(potluck(home.path()).args(["cp", bad, "dest"]));
        assert_eq!(r.code, 2, "{bad}");
        assert!(
            !r.stderr.contains("daemon is not running"),
            "refused before connecting: {bad}"
        );
    }
}

fn lease_daemon(live: bool, grant: Grant) -> FakeDaemon {
    lease_daemon_until(live, grant, None)
}

/// A daemon whose lease check says the job has `remaining_ms` left before its run limit.
fn lease_daemon_until(live: bool, grant: Grant, remaining_ms: Option<u64>) -> FakeDaemon {
    FakeDaemon::start(vec![], move |req, conn| match &req.req {
        Request::LeaseCheck { .. } => conn.send(Response::Lease {
            live,
            grant: live.then(|| grant.clone()),
            remaining_ms,
        }),
        _ => conn.send(Response::Error {
            message: "unexpected".into(),
        }),
    })
}

fn inside_job(fake: &FakeDaemon, cwd: &Path) -> Command {
    let mut c = potluck(fake.home());
    c.current_dir(cwd)
        .env("POTLUCK_JOB", "studio-9")
        .env("POTLUCK_LEASE", "secret")
        .env("POTLUCK_WORKSPACE", "msi-w3");
    c
}

/// A dispatch run in place from a subdirectory of the repository runs at the repository's top, as
/// one from outside runs at its workspace's root (a recipe's `./ci/test.sh` works from anywhere).
#[test]
fn nested_dispatch_runs_at_the_top_of_the_repository() {
    let fake = lease_daemon(
        true,
        Grant {
            ram_mib: 4096,
            cpu: 1,
            gpus: vec![],
            exclusive: false,
        },
    );
    let repo = tempfile::tempdir().unwrap();
    let ok = |args: &[&str]| {
        let s = std::process::Command::new("git")
            .args(args)
            .current_dir(repo.path())
            .status()
            .unwrap();
        assert!(s.success(), "git {args:?}");
    };
    ok(&["init", "-q"]);
    let sub = repo.path().join("crates/x");
    std::fs::create_dir_all(&sub).unwrap();
    let r = run(inside_job(&fake, &sub).args(["dispatch", "--timeout", "1m", "--", "pwd"]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    let top = std::fs::canonicalize(repo.path()).unwrap();
    assert_eq!(r.stdout.trim(), top.display().to_string());
}

/// From inside a job, waiting for (or following) that same job would wait for itself until its run
/// limit: refused at once. A dispatch run in place reports the job it ran in, so a script that
/// waits on what dispatch printed lands here.
#[test]
fn a_job_cant_wait_for_itself() {
    let fake = lease_daemon(true, Grant::default());
    let cwd = tempfile::tempdir().unwrap();
    for args in [
        &["wait", "studio-9"][..],
        &["wait", "-q", "studio-9"],
        &["logs", "-f", "studio-9"],
    ] {
        let r = run(inside_job(&fake, cwd.path()).args(args));
        assert_eq!(r.code, 2, "{args:?}: {}", r.stderr);
        assert!(
            r.stderr.contains("studio-9 is the job this runs in"),
            "{args:?}: {}",
            r.stderr
        );
    }
}

#[test]
fn nested_dispatch_runs_in_place_when_it_fits_the_lease() {
    let grant = Grant {
        ram_mib: 8192,
        cpu: 2,
        gpus: vec![],
        exclusive: false,
    };
    let fake = lease_daemon(true, grant);
    let cwd = tempfile::tempdir().unwrap();
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1m",
        "--ram",
        "2G",
        "--cpu",
        "64",
        "--env",
        "X=from-flag",
        "--",
        "sh",
        "-c",
        "echo inside $X $(pwd); exit 5",
    ]));
    assert_eq!(r.code, 5, "{}", r.stderr);
    let here = std::fs::canonicalize(cwd.path()).unwrap();
    assert_eq!(
        r.stdout.trim(),
        format!("inside from-flag {}", here.display())
    );
    assert!(
        r.stderr.contains("inside job studio-9: running in place"),
        "{}",
        r.stderr
    );
    assert_eq!(
        fake.ops(),
        ["lease_check"],
        "never dispatched, never waited"
    );
    let req = &fake.requests()[0];
    assert!(
        matches!(&req.req, Request::LeaseCheck { job, token } if job == "studio-9" && token == "secret")
    );
    assert_eq!(req.ctx.job.as_deref(), Some("studio-9"));
    assert_eq!(req.ctx.lease_token.as_deref(), Some("secret"));

    // --json keeps stdout for the one document: the command's output goes to stderr.
    let r = run(inside_job(&fake, cwd.path()).args([
        "--json",
        "dispatch",
        "--timeout",
        "1m",
        "--",
        "sh",
        "-c",
        "echo out",
    ]));
    assert_eq!(r.code, 0);
    let doc = one_json(&r.stdout);
    assert_eq!(
        (doc["in_place"].as_bool(), doc["exit_code"].as_i64()),
        (Some(true), Some(0))
    );
    assert!(r.stderr.contains("out"));

    // The run limit holds in place too.
    let started = std::time::Instant::now();
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1s",
        "--",
        "sleep",
        "30",
    ]));
    assert_eq!(r.code, 124);
    assert!(started.elapsed() < std::time::Duration::from_secs(15));
    // ... for every process the command started, not only the first: a shell's child that
    // outlived it would keep running, and keep the caller's pipes open.
    let marker = cwd.path().join("late");
    let script = format!("sleep 4; touch {}", marker.display());
    let started = std::time::Instant::now();
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1s",
        "--",
        "sh",
        "-c",
        &script,
    ]));
    assert_eq!(r.code, 124);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "the pipes closed at the run limit: {:?}",
        started.elapsed()
    );
    std::thread::sleep(std::time::Duration::from_secs(5));
    assert!(!marker.exists(), "the shell's child was stopped too");

    // Same workspace named explicitly: fine.
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1m",
        "--workspace",
        "msi-w3",
        "--",
        "true",
    ]));
    assert_eq!(r.code, 0, "{}", r.stderr);
}

/// In place, potluck's own variables stay the job's: `--env` and the committed potluck.toml
/// can't change them (a nested dispatch inside would otherwise present another lease).
#[test]
fn nested_dispatch_keeps_the_jobs_own_variables() {
    let grant = Grant {
        ram_mib: 8192,
        cpu: 1,
        gpus: vec![],
        exclusive: false,
    };
    let fake = lease_daemon(true, grant);
    let cwd = tempfile::tempdir().unwrap();
    let r = run(inside_job(&fake, cwd.path())
        .env("TMPDIR", "/the-jobs-tmp")
        .args([
            "dispatch",
            "--timeout",
            "1m",
            "--env",
            "POTLUCK_JOB=other-1",
            "--env",
            "POTLUCK_LEASE=forged",
            "--env",
            "TMPDIR=/elsewhere",
            "--env",
            "X=from-flag",
            "--",
            "sh",
            "-c",
            "echo $POTLUCK_JOB $POTLUCK_LEASE $TMPDIR $X",
        ]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.stdout.trim(), "studio-9 secret /the-jobs-tmp from-flag");
}

/// In place, what ignores the SIGTERM at the run limit gets SIGKILL after the machine's
/// `kill_grace_s`, as a job does.
#[test]
fn nested_dispatch_kills_after_the_configured_grace() {
    let grant = Grant {
        ram_mib: 8192,
        cpu: 1,
        gpus: vec![],
        exclusive: false,
    };
    let fake = lease_daemon(true, grant);
    std::fs::write(fake.home().join("node.toml"), "kill_grace_s = 1\n").unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let started = std::time::Instant::now();
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1s",
        "--",
        "sh",
        "-c",
        "trap '' TERM; sleep 30",
    ]));
    assert_eq!(r.code, 124, "{}", r.stderr);
    let took = started.elapsed();
    assert!(
        took < std::time::Duration::from_secs(6),
        "SIGKILL came {took:?} after the start, with a 1 s limit and a 1 s grace"
    );
}

/// The grace at the run limit is for every process the command started, not only the first:
/// a shell that dies at once on SIGTERM doesn't cut short its child's cleanup.
#[test]
fn nested_dispatch_gives_the_whole_group_its_grace() {
    let grant = Grant {
        ram_mib: 8192,
        cpu: 1,
        gpus: vec![],
        exclusive: false,
    };
    let fake = lease_daemon(true, grant);
    std::fs::write(fake.home().join("node.toml"), "kill_grace_s = 5\n").unwrap();
    let cwd = tempfile::tempdir().unwrap();
    let marker = cwd.path().join("cleaned");
    let script = format!(
        "(trap 'sleep 1; touch {}; exit 0' TERM; while :; do sleep 0.1; done) & wait",
        marker.display()
    );
    let started = std::time::Instant::now();
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1s",
        "--",
        "sh",
        "-c",
        &script,
    ]));
    assert_eq!(r.code, 124, "{}", r.stderr);
    assert!(marker.exists(), "the child's cleanup ran to its end");
    let took = started.elapsed();
    assert!(
        took < std::time::Duration::from_secs(5),
        "it ended when the group did, not at the end of the grace: {took:?}"
    );
}

/// The job's run limit ends everything in it: a dispatch whose own run limit doesn't fit in the
/// time the job has left fails at once, as one that asks for more RAM does.
#[test]
fn nested_dispatch_must_end_before_the_job_does() {
    let grant = Grant {
        ram_mib: 4096,
        cpu: 1,
        gpus: vec![],
        exclusive: false,
    };
    let fake = lease_daemon_until(true, grant, Some(10 * 60_000));
    let cwd = tempfile::tempdir().unwrap();
    let r = run(inside_job(&fake, cwd.path()).args(["dispatch", "--timeout", "1h", "--", "true"]));
    assert_eq!(r.code, 2, "{}", r.stderr);
    assert!(
        r.stderr.contains(
            "can't run inside job studio-9: it asks for a 1h run limit and the job has 10m left of its own"
        ),
        "{}",
        r.stderr
    );
    let r = run(inside_job(&fake, cwd.path()).args(["dispatch", "--timeout", "5m", "--", "true"]));
    assert_eq!(r.code, 0, "{}", r.stderr);
}

/// Options that only mean something for a job of its own (files copied back, a hold, a key, labels,
/// what happens when the machine is lost or the job stalls) are refused in place rather than
/// dropped without a word; a recipe's keep and hold are named as not applying.
#[test]
fn nested_dispatch_refuses_options_it_cannot_honour() {
    let grant = Grant {
        ram_mib: 4096,
        cpu: 1,
        gpus: vec![],
        exclusive: false,
    };
    let fake = lease_daemon(true, grant);
    let cwd = tempfile::tempdir().unwrap();
    let marker = cwd.path().join("ran");
    let touch = format!("touch {}", marker.display());
    for (extra, want) in [
        (
            &["--keep", "out/*"][..],
            "only job studio-9's own --keep copies files back",
        ),
        (&["--hold", "1h"][..], "its workspace is job studio-9's"),
        (
            &["--key", "k1"][..],
            "leaves no job behind for a key to name",
        ),
        (&["--label", "a=b"][..], "leaves no job behind to label"),
        (&["--no-rerun"][..], "job studio-9's own dispatch decides"),
        (
            &["--kill-on-stall"][..],
            "job studio-9's own dispatch decides",
        ),
        (
            &["--without-submodule", "vendor/x"][..],
            "with the submodules that job's dispatch shipped",
        ),
    ] {
        let mut args = vec!["dispatch", "--timeout", "1m"];
        args.extend_from_slice(extra);
        args.extend_from_slice(&["--", "sh", "-c", &touch]);
        let r = run(inside_job(&fake, cwd.path()).args(&args));
        assert_eq!(r.code, 2, "{extra:?}: {}", r.stderr);
        assert!(
            r.stderr.contains(&format!(
                "potluck: {} doesn't apply inside job studio-9: a dispatch from inside a job runs in place, not as a job of its own,",
                extra[0]
            )) && r.stderr.contains(want),
            "{extra:?}: {}",
            r.stderr
        );
        assert!(!marker.exists(), "{extra:?}");
    }
    // --priority and --cpu need nothing in place: it starts at once, sharing the job's CPU.
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1m",
        "--priority",
        "5",
        "--cpu",
        "4",
        "--",
        "true",
    ]));
    assert_eq!(r.code, 0, "{}", r.stderr);

    // A recipe's keep and hold: it runs, and says they don't apply.
    let repo = repo_with(
        "[recipe.t]\ncmd = [\"true\"]\ntimeout = \"1m\"\nkeep = [\"out/*\"]\nhold = \"1h\"\n",
    );
    let r = run(inside_job(&fake, repo.path()).args(["dispatch", "t"]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert!(
        r.stderr.contains(
            "potluck: inside job studio-9: recipe t's keep and hold don't apply in place; job studio-9's own do"
        ),
        "{}",
        r.stderr
    );
    let r = run(inside_job(&fake, repo.path()).args(["--json", "dispatch", "t"]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    let doc = one_json(&r.stdout);
    assert_eq!(doc["ignored"], serde_json::json!(["keep", "hold"]));
}

#[test]
fn nested_dispatch_fails_at_once_when_it_does_not_fit() {
    let grant = Grant {
        ram_mib: 4096,
        cpu: 1,
        gpus: vec!["10de:2c19@0000:01:00.0".into()],
        exclusive: false,
    };
    let fake = lease_daemon(true, grant);
    let cwd = tempfile::tempdir().unwrap();
    let marker = cwd.path().join("ran");
    let touch = format!("touch {}", marker.display());
    for (extra, want) in [
        (
            &["--ram", "16G"][..],
            "it asks for 16384 MiB of RAM and the job holds 4096 MiB",
        ),
        (
            &["--gpu", "amd"][..],
            "it asks for GPU amd and the job holds 10de:2c19@0000:01:00.0",
        ),
        (
            &["--gpu", "nvidia", "--gpu", "any"][..],
            "it asks for GPU nvidia, any",
        ),
        (&["--exclusive"][..], "the job's lease is not exclusive"),
        (
            &["--workspace", "msi-w1"][..],
            "it names workspace msi-w1 and the job runs in msi-w3",
        ),
    ] {
        let mut args = vec!["dispatch", "--timeout", "1m"];
        args.extend_from_slice(extra);
        args.extend_from_slice(&["--", "sh", "-c", &touch]);
        let started = std::time::Instant::now();
        let r = run(inside_job(&fake, cwd.path()).args(&args));
        assert_eq!(r.code, 2, "{extra:?}");
        assert!(
            r.stderr.contains("can't run inside job studio-9") && r.stderr.contains(want),
            "{extra:?}: {}",
            r.stderr
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "fails at once"
        );
        assert!(!marker.exists());
    }
    // The GPU it holds fits.
    let r = run(inside_job(&fake, cwd.path()).args([
        "dispatch",
        "--timeout",
        "1m",
        "--gpu",
        "10de:2c19",
        "--",
        "sh",
        "-c",
        &touch,
    ]));
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert!(marker.exists());

    let dead = lease_daemon(false, Grant::default());
    let r = run(inside_job(&dead, cwd.path()).args(["dispatch", "--timeout", "1m", "--", "true"]));
    assert_eq!(r.code, 2);
    assert!(r.stderr.contains("its lease is not live"), "{}", r.stderr);
    assert_eq!(dead.ops(), ["lease_check"]);
}

fn facts(node: &str, gpus: Vec<Gpu>) -> Facts {
    Facts {
        node: node.into(),
        os: "linux".into(),
        os_version: "CachyOS".into(),
        arch: "x86_64".into(),
        kernel: "7.2.8-2-cachyos".into(),
        cpu_model: "Intel(R) Core(TM) Ultra 9 275HX".into(),
        cpus: 24,
        ram_mib: 96201,
        gpus,
        display: true,
        version: "0.1.0".into(),
    }
}

fn status_daemon() -> FakeDaemon {
    FakeDaemon::start(vec![reminder("msi-w7", 30 * 60_000)], |req, conn| {
        let now = now_ms();
        let gpu = Gpu {
            id: "10de:2c19@0000:01:00.0".into(),
            vendor: "10de".into(),
            device: "2c19".into(),
            name: "NVIDIA GeForce RTX 5080 Laptop GPU".into(),
            driver: Some("nvidia".into()),
            alias: Some("5080".into()),
            render_node: Some("/dev/dri/renderD128".into()),
        };
        let node = NodeView {
            facts: facts("msi", vec![gpu.clone()]),
            address: Some("10.0.0.2:7465".into()),
            alive: true,
            updating: false,
            last_seen: Some(now),
            ram_capacity_mib: 88 * 1024,
            ram_reserved_mib: 16 * 1024,
            gpus_busy: vec![gpu.id.clone()],
            exclusive_job: None,
        };
        match &req.req {
            Request::Status => {
                let mut running = job("studio-41", JobState::Running, None);
                running.started_at = Some(now - 14 * 60_000);
                running.lease = Some(potluck::types::Lease {
                    grant: Grant {
                        ram_mib: 4096,
                        cpu: 1,
                        gpus: vec![gpu.id.clone()],
                        exclusive: false,
                    },
                    granted_at: now,
                    deadline: now,
                    token: "t".into(),
                });
                let mut queued = job("studio-42", JobState::Queued, None);
                queued.submitted_at = now - 5 * 60_000;
                queued.passes = 1;
                queued.spec.request.gpus = vec!["5080".into()];
                queued.spec.on = [("gpu", "nvidia"), ("linux", "")]
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .into();
                let mut done = job("studio-40", JobState::Succeeded, Some(0));
                done.ended_at = Some(now - 3 * 60_000);
                let mut stuck = job("studio-39", JobState::TimedOut, Some(124));
                stuck.ended_at = Some(now - 4 * 60_000);
                stuck.stuck_pids = vec![4242, 4243];
                let ws = WorkspaceRecord {
                    id: "msi-w7".into(),
                    node: "msi".into(),
                    repo_id: "app-0011aabb".into(),
                    commit: "0123456789abcdef".into(),
                    branch: Some("feature".into()),
                    path: PathBuf::from("/w/msi-w7"),
                    state: WorkspaceState::Held,
                    created_at: now,
                    last_used_at: now,
                    held_until: now + 30 * 60_000 + 999,
                    pending: 0,
                    size_bytes: Some(3 << 30),
                };
                conn.send(Response::Status {
                    status: Box::new(StatusView {
                        nodes: vec![node],
                        running: vec![running],
                        queued: vec![queued],
                        recent: vec![done, stuck],
                        workspaces: vec![ws],
                        notes: vec!["this machine was removed from the cluster".into()],
                    }),
                })
            }
            Request::Nodes => conn.send(Response::Nodes { nodes: vec![node] }),
            Request::Why { job } if job == "studio-42" => conn.send(Response::Why {
                why: WhyView {
                    job: job.clone(),
                    state: "queued".into(),
                    lines: vec!["waits on gpu 5080 on msi, held by job studio-41 for 14m of its 15m run limit, under 1% CPU for 9m".into()],
                    blockers: vec![Blocker {
                        resource: "gpu:10de:2c19@0000:01:00.0".into(),
                        node: Some("msi".into()),
                        holder: Some("studio-41".into()),
                        held_for_ms: Some(14 * 60_000),
                        limit_ms: Some(15 * 60_000),
                        holder_idle_ms: Some(9 * 60_000),
                        free_by: None,
                    }],
                },
            }),
            Request::Why { job } => conn.send(Response::Error { message: format!("no job {job}") }),
            Request::Cancel { job: id } => conn.send(Response::Job { job: Box::new(job(id, JobState::Cancelled, None)) }),
            Request::Hold { workspace, duration_ms } => {
                let mut ws_rec = WorkspaceRecord {
                    id: workspace.clone(),
                    node: "msi".into(),
                    repo_id: "app".into(),
                    commit: "c".into(),
                    branch: None,
                    path: PathBuf::new(),
                    state: WorkspaceState::Held,
                    created_at: 0,
                    last_used_at: 0,
                    held_until: 0,
                    pending: 0,
                    size_bytes: None,
                };
                ws_rec.held_until = now + duration_ms;
                conn.send(Response::Workspace { workspace: Box::new(ws_rec) })
            }
            Request::Release { .. } | Request::Remove { .. } => conn.send(Response::Ok),
            Request::PairRequest | Request::Admit { .. } => conn.send(Response::Text { text: "PASTE-ME".into() }),
            Request::PairFinish { .. } | Request::SelfUpdate => conn.send(Response::Ok),
            _ => conn.send(Response::Error { message: "unexpected".into() }),
        }
    })
}

#[test]
fn status_why_and_nodes_print_plain_tables() {
    let fake = status_daemon();
    let r = run(potluck(fake.home()).arg("status"));
    assert_eq!(r.code, 0, "{}", r.stderr);
    let s = &r.stdout;
    for want in [
        "machines\nNODE  UP   RAM RESERVED      GPUS  LEASED  EXCLUSIVE JOB\nmsi   yes  16 GiB of 88 GiB  1     5080    -\n",
        "\nrunning\nJOB        STATE    NODE  WORKSPACE  TIME        RAM           GPUS  COMMAND\nstudio-41  running  msi   msi-w7     14m of 10m  0 B of 4 GiB  5080  cargo test\n",
        "\nqueued, in start order\nJOB        PRIORITY    WAITING  NEEDS            ON                 COMMAND\nstudio-42  4 (from 3)  5m       4 GiB, gpu 5080  linux, gpu=nvidia  cargo test\n",
        "\nrecent\nJOB        STATE      EXIT  ENDED   NODE  COMMAND\nstudio-40  succeeded  0     3m ago  msi   cargo test\nstudio-39  timed out  124   4m ago  msi   cargo test\nstudio-39 left processes that would not die on msi: 4242, 4243\n",
        "\nworkspaces\nWORKSPACE  NODE  STATE  HELD  SIZE     PENDING  BRANCH   COMMIT        REPO\nmsi-w7     msi   held   30m   3.0 GiB  0        feature  0123456789ab  app-0011aabb\n",
    ] {
        assert!(s.contains(want), "missing\n{want}\nin\n{s}");
    }
    assert!(!s.contains('|') && !s.contains("--"), "no decoration:\n{s}");
    assert!(
        s.starts_with("this machine was removed from the cluster\n\nmachines\n"),
        "notes come first:\n{s}"
    );
    let r = run(potluck(fake.home()).args(["status", "--json"]));
    let doc = one_json(&r.stdout);
    assert_eq!(doc["notes"][0], "this machine was removed from the cluster");
    assert_eq!(doc["running"][0]["id"], "studio-41");
    assert_eq!(doc["queued"][0]["passes"], 1);
    assert_eq!(doc["reminders"][0]["workspace"], "msi-w7");

    let r = run(potluck(fake.home()).args(["why", "studio-42"]));
    assert_eq!(
        r.stdout,
        "job studio-42 is queued\nwaits on gpu 5080 on msi, held by job studio-41 for 14m of its 15m run limit, under 1% CPU for 9m\n"
    );
    let doc = one_json(&run(potluck(fake.home()).args(["--json", "why", "studio-42"])).stdout);
    assert_eq!(doc["blockers"][0]["holder"], "studio-41");
    let r = run(potluck(fake.home()).args(["why", "nope"]));
    assert_eq!(r.code, 2);
    assert!(
        r.stderr.starts_with("potluck: no job nope\n"),
        "{}",
        r.stderr
    );
    assert!(
        r.stderr.contains("potluck release msi-w7"),
        "reminders after errors too: {}",
        r.stderr
    );
    let doc = one_json(&run(potluck(fake.home()).args(["--json", "why", "nope"])).stdout);
    assert_eq!(
        (
            doc["error"].as_str(),
            doc["reminders"][0]["workspace"].as_str()
        ),
        (Some("no job nope"), Some("msi-w7"))
    );

    let r = run(potluck(fake.home()).arg("nodes"));
    assert_eq!(r.code, 0);
    assert!(r.stdout.starts_with("NODE  UP   OS             KERNEL           ARCH    CPUS  RAM       DISPLAY  VERSION  ADDRESS        CPU\nmsi   yes  linux CachyOS  7.2.8-2-cachyos  x86_64  24    93.9 GiB  yes      0.1.0    10.0.0.2:7465  Intel(R) Core(TM) Ultra 9 275HX\n"), "{}", r.stdout);
    assert!(r.stdout.contains("\nNODE  GPU                     ALIAS  NAME                                DRIVER  STATE\nmsi   10de:2c19@0000:01:00.0  5080   NVIDIA GeForce RTX 5080 Laptop GPU  nvidia  leased\n"), "{}", r.stdout);
    let doc = one_json(&run(potluck(fake.home()).args(["nodes", "--json"])).stdout);
    assert_eq!(doc["nodes"][0]["facts"]["gpus"][0]["alias"], "5080");
}

#[test]
fn small_commands_print_what_they_did_and_the_reminders() {
    let fake = status_daemon();
    let cases: [(&[&str], &str); 7] = [
        (&["cancel", "studio-1"], "cancelled job studio-1\n"),
        (
            &["hold", "msi-w7", "2h"],
            "workspace msi-w7 is held for 2h from now (`potluck release msi-w7` ends it sooner)\n",
        ),
        (&["release", "msi-w7"], "released workspace msi-w7\n"),
        (&["remove", "old-box"], "removed old-box from the cluster\n"),
        (&["pair"], "PASTE-ME\n"),
        (&["admit", "REQ"], "PASTE-ME\n"),
        (&["pair", "--finish", "REPLY"], "pair --finish: done\n"),
    ];
    for (args, want) in cases {
        let r = run(potluck(fake.home()).args(args));
        assert_eq!(r.code, 0, "{args:?}: {}", r.stderr);
        assert!(
            r.stdout.starts_with(want.trim_end()),
            "{args:?}: {}",
            r.stdout
        );
        assert!(
            r.stderr.contains("`potluck release msi-w7` ends it"),
            "{args:?}: {}",
            r.stderr
        );
    }
    // Pairing's lines go to another machine: stdout is the line alone, stderr says where.
    for (args, next) in [
        (&["pair"][..], "into `potluck admit` on a member"),
        (
            &["admit", "REQ"][..],
            "into `potluck pair --finish` on the new machine",
        ),
    ] {
        let r = run(potluck(fake.home()).args(args));
        assert_eq!(r.stdout, "PASTE-ME\n", "{args:?}");
        assert!(r.stderr.contains(next), "{args:?}: {}", r.stderr);
    }
    let r = run(potluck(fake.home()).args(["hold", "msi-w7", "25h"]));
    assert_eq!(r.code, 2);
    assert!(r.stderr.contains("at most 24h"));
    let doc = one_json(&run(potluck(fake.home()).args(["--json", "cancel", "studio-1"])).stdout);
    assert_eq!(doc["record"]["state"], "cancelled");
    assert!(fake.requests().iter().any(|r| matches!(
        r.req,
        Request::Hold {
            duration_ms: 7_200_000,
            ..
        }
    )));
}

#[test]
fn context_names_the_repository_and_branch_cheaply() {
    let repo = repo_with("");
    let ctx = potluck::client::context(repo.path(), Some("j".into()), None);
    let common = std::fs::canonicalize(repo.path().join(".git")).unwrap();
    assert_eq!(ctx.repo_id, Some(potluck::types::repo_id(&common)));
    assert_eq!(ctx.branch.as_deref(), Some("feature"));
    assert_eq!(ctx.job.as_deref(), Some("j"));
    // A linked worktree of the same repository has the same id; a detached HEAD has no branch.
    let wt = repo.path().join("wt");
    git(
        repo.path(),
        &["worktree", "add", "-q", "--detach", wt.to_str().unwrap()],
    );
    let sub = wt.join("deeper");
    std::fs::create_dir(&sub).unwrap();
    let ctx2 = potluck::client::context(&sub, None, None);
    assert_eq!(ctx2.repo_id, ctx.repo_id);
    assert_eq!(ctx2.branch, None);
    // Outside a repository: nothing, and no error.
    let plain = tempfile::tempdir().unwrap();
    let ctx3 = potluck::client::context(plain.path(), None, None);
    assert_eq!((ctx3.repo_id, ctx3.branch), (None, None));
}

/// Installs and removes a real service under a test name (`potluck-test-...`), then checks the
/// service manager saw both. Opt-in: it talks to the user's systemd or launchd.
#[test]
#[ignore = "installs a real user service named potluck-test-*; run with --ignored"]
fn service_install_and_uninstall_for_real() {
    let home = tempfile::tempdir().unwrap();
    let name = format!("potluck-test-svc-{}", std::process::id());
    let user_home = PathBuf::from(std::env::var("HOME").unwrap());
    let (dir, file, check): (PathBuf, PathBuf, Vec<String>) = if cfg!(target_os = "macos") {
        let dir = home.path().join("agents");
        let uid = unsafe { libc::getuid() };
        (
            dir.clone(),
            dir.join(format!("{name}.plist")),
            vec![
                "launchctl".into(),
                "print".into(),
                format!("gui/{uid}/{name}"),
            ],
        )
    } else {
        let dir = user_home.join(".config/systemd/user");
        (
            dir.clone(),
            dir.join(format!("{name}.service")),
            vec![
                "systemctl".into(),
                "--user".into(),
                "is-enabled".into(),
                format!("{name}.service"),
            ],
        )
    };
    // Run potluck through a symlink: the service must name the real file, the one self-update
    // replaces.
    let link = home.path().join("bin/potluck");
    std::fs::create_dir_all(link.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_potluck"), &link).unwrap();
    let real = std::fs::canonicalize(env!("CARGO_BIN_EXE_potluck")).unwrap();
    let svc = |action: &str| {
        let mut c = common::potluck_at(&link, home.path());
        run(c
            .env("POTLUCK_SERVICE_DIR", &dir)
            .env("POTLUCK_SERVICE_NAME", &name)
            .args(["--json", "service", action]))
    };
    let checked = || {
        Command::new(&check[0])
            .args(&check[1..])
            .output()
            .unwrap()
            .status
            .success()
    };
    // launchd can take about 10 s to settle; allow 15.
    let eventually = |want: bool| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while checked() != want && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
        checked() == want
    };
    let r = svc("install");
    let cleanup = svc_guard(&dir, &name);
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
    assert!(file.is_file());
    assert!(eventually(true), "the service manager knows {name}");
    let text = std::fs::read_to_string(&file).unwrap();
    assert!(text.contains(real.to_str().unwrap()), "{text}");
    assert!(!text.contains(link.to_str().unwrap()), "{text}");
    // Installing again replaces the loaded service.
    let r = svc("install");
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
    assert!(eventually(true), "{name} is loaded again");
    let r = svc("uninstall");
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
    assert!(!file.exists());
    assert!(eventually(false), "{name} is gone");
    drop(cleanup);
}

/// `service install` refuses what the daemon would refuse, before it writes or starts anything:
/// otherwise the service manager starts a daemon that exits at once, again and again, while the
/// command says it started it.
#[test]
fn service_install_refuses_a_config_the_daemon_would_refuse() {
    let home = tempfile::tempdir().unwrap();
    let dir = home.path().join("services");
    let name = format!("potluck-test-refused-{}", std::process::id());
    std::fs::write(home.path().join("node.toml"), "bogus = 1\n").unwrap();
    let _cleanup = svc_guard(&dir, &name);
    let r = run(potluck(home.path())
        .env("POTLUCK_SERVICE_DIR", &dir)
        .env("POTLUCK_SERVICE_NAME", &name)
        .args(["service", "install"]));
    assert_eq!(r.code, 2, "{} {}", r.stdout, r.stderr);
    assert!(
        r.stderr.contains("node.toml") && r.stderr.contains("bogus"),
        "{}",
        r.stderr
    );
    assert!(!dir.exists(), "nothing written");
}

/// A node.toml that parses but whose `listen` isn't an IP address: the daemon exits at start on
/// it, so `service install` refuses it too rather than start a service that fails again and again.
#[test]
fn service_install_refuses_a_listen_address_the_daemon_would_refuse() {
    let home = tempfile::tempdir().unwrap();
    let dir = home.path().join("services");
    let name = format!("potluck-test-listen-{}", std::process::id());
    std::fs::write(home.path().join("node.toml"), "listen = \"localhost\"\n").unwrap();
    let _cleanup = svc_guard(&dir, &name);
    let r = run(potluck(home.path())
        .env("POTLUCK_SERVICE_DIR", &dir)
        .env("POTLUCK_SERVICE_NAME", &name)
        .args(["service", "install"]));
    assert_eq!(r.code, 2, "{} {}", r.stdout, r.stderr);
    assert!(
        r.stderr
            .contains("listen = \"localhost\": not an IP address"),
        "{}",
        r.stderr
    );
    assert!(!dir.exists(), "nothing written");
}

/// A node name starting with `-` makes ids (`-box-1`, `-box-w1`) that every command reads as
/// options: the daemon refuses to start under it, and `service install` refuses it up front.
#[test]
fn a_node_name_starting_with_a_dash_is_refused() {
    let home = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    std::fs::write(home.path().join("node.toml"), "name = \"-box\"\n").unwrap();
    let dir = home.path().join("services");
    let name = format!("potluck-test-dash-{}", std::process::id());
    let _cleanup = svc_guard(&dir, &name);
    let r = run(potluck(home.path())
        .env("POTLUCK_SERVICE_DIR", &dir)
        .env("POTLUCK_SERVICE_NAME", &name)
        .args(["service", "install"]));
    assert_eq!(r.code, 2, "{} {}", r.stdout, r.stderr);
    assert!(r.stderr.contains("bad node name \"-box\""), "{}", r.stderr);
    assert!(!dir.exists(), "nothing written");

    let mut daemon = potluck(home.path())
        .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
        .arg("daemon")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let status = loop {
        if let Some(s) = daemon.try_wait().unwrap() {
            break Some(s);
        }
        if std::time::Instant::now() > deadline {
            let _ = daemon.kill();
            let _ = daemon.wait();
            break None;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let out = daemon.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(status.and_then(|s| s.code()), Some(2), "{stderr}");
    assert!(stderr.contains("bad node name \"-box\""), "{stderr}");
}

/// A machine keeps the name its daemon first ran under. A `name` set in node.toml after that (a
/// new user's next step after `service install`) used to pass `service install`, whose daemon
/// then failed again and again under the service manager, with a message about "its cluster" on
/// a machine in none. Both now refuse it and say how to go on.
#[test]
fn a_name_set_after_the_first_start_is_refused_with_the_way_on() {
    let home = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let id = std::process::id();
    let config = home.path().join("node.toml");
    std::fs::write(&config, format!("name = \"first-{id}\"\n")).unwrap();
    let daemon = || {
        potluck(home.path())
            .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
            .arg("daemon")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    };
    // The first start writes this machine's records under its name.
    let mut first = daemon();
    let records = home.path().join("keys/members.json");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !records.exists() && std::time::Instant::now() < deadline {
        assert!(first.try_wait().unwrap().is_none(), "the daemon exited");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let _ = first.kill();
    let _ = first.wait();
    assert!(records.exists(), "the daemon wrote no records in 60 s");

    std::fs::write(&config, format!("name = \"second-{id}\"\n")).unwrap();
    let dir = home.path().join("services");
    let name = format!("potluck-test-renamed-{id}");
    let _cleanup = svc_guard(&dir, &name);
    let r = run(potluck(home.path())
        .env("POTLUCK_SERVICE_DIR", &dir)
        .env("POTLUCK_SERVICE_NAME", &name)
        .args(["service", "install"]));
    assert_eq!(r.code, 2, "{} {}", r.stdout, r.stderr);
    let says_how = |text: &str| {
        text.contains(&format!("has been first-{id} since its daemon first ran"))
            && text.contains(&format!("now names it second-{id}"))
            && text.contains(&format!(
                "Set `name = \"first-{id}\"` in {}",
                config.display()
            ))
            && text.contains("since it is in no cluster yet")
            && text.contains(&format!("remove {}", home.path().display()))
            && !text.contains("in its cluster")
    };
    assert!(says_how(&r.stderr), "{}", r.stderr);
    assert!(!dir.exists(), "nothing written");

    let mut second = daemon();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let status = loop {
        if let Some(s) = second.try_wait().unwrap() {
            break Some(s);
        }
        if std::time::Instant::now() > deadline {
            let _ = second.kill();
            break None;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let out = second.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(status.and_then(|s| s.code()), Some(2), "{stderr}");
    assert!(says_how(&stderr), "{stderr}");
}

/// git before 2.31 doesn't know `git rev-parse --path-format=absolute`: it prints the option back
/// as the first line, which potluck reads as the top of the tree, so every dispatch and every
/// seeding on that machine failed. The daemon refuses to start with such a git, and `service
/// install` refuses it up front, naming the git it found and its version.
#[test]
fn a_git_older_than_2_31_is_refused() {
    let home = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let id = std::process::id();
    std::fs::write(
        home.path().join("node.toml"),
        format!("name = \"oldgit-{id}\"\n"),
    )
    .unwrap();
    let (bin, _calls) = fake_service_tools(home.path());
    // Says it is Debian 11's git, and is the real one for anything else.
    let real = String::from_utf8(
        Command::new("sh")
            .args(["-c", "command -v git"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    let git = bin.join("git");
    std::fs::write(
        &git,
        format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'git version 2.30.2'; exit 0; fi\n\
             exec '{}' \"$@\"\n",
            real.trim()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let says_why = |text: &str| {
        text.contains("potluck needs git 2.31 or newer")
            && text.contains(&format!("{} is git 2.30.2", git.display()))
    };
    let dir = home.path().join("services");
    let name = format!("potluck-test-oldgit-{id}");
    let _cleanup = svc_guard(&dir, &name);
    let r = run(potluck(home.path())
        .env("PATH", &path)
        .env("POTLUCK_SERVICE_DIR", &dir)
        .env("POTLUCK_SERVICE_NAME", &name)
        .args(["service", "install"]));
    assert_eq!(r.code, 2, "{} {}", r.stdout, r.stderr);
    assert!(says_why(&r.stderr), "{}", r.stderr);
    assert!(!dir.exists(), "nothing written");

    let mut daemon = potluck(home.path())
        .env("PATH", &path)
        .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
        .arg("daemon")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let status = loop {
        if let Some(s) = daemon.try_wait().unwrap() {
            break Some(s);
        }
        if std::time::Instant::now() > deadline {
            let _ = daemon.kill();
            break None;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let out = daemon.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(status.and_then(|s| s.code()), Some(2), "{stderr}");
    assert!(says_why(&stderr), "{stderr}");
}

/// With `potluck daemon` running in a terminal (the README's other way to run it), `service
/// install` used to say it installed and started the service, whose daemon then exited at once
/// ("another potluck daemon is running") and was started again every 10 s (seen with a test
/// LaunchAgent on the Mac). It now refuses, and a second daemon says what to do.
#[test]
fn service_install_refuses_while_a_daemon_runs_outside_it() {
    let home = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let id = std::process::id();
    std::fs::write(
        home.path().join("node.toml"),
        format!("name = \"fg-{id}\"\n"),
    )
    .unwrap();
    let daemon = || {
        potluck(home.path())
            .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
            .arg("daemon")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut fg = daemon();
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while run(potluck(home.path()).args(["--json", "status"])).code != 0 {
        assert!(fg.try_wait().unwrap().is_none(), "the daemon exited");
        assert!(std::time::Instant::now() < deadline, "no daemon in 60 s");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let _fg = Kill(fg);

    let dir = home.path().join("services");
    let name = format!("potluck-test-fg-{id}");
    let _cleanup = svc_guard(&dir, &name);
    let r = run(potluck(home.path())
        .env("POTLUCK_SERVICE_DIR", &dir)
        .env("POTLUCK_SERVICE_NAME", &name)
        .args(["service", "install"]));
    assert_eq!(r.code, 2, "{} {}", r.stdout, r.stderr);
    assert!(
        r.stderr.contains(&format!(
            "a potluck daemon already runs for {} outside the service",
            home.path().display()
        )) && r
            .stderr
            .contains("stop that daemon, then run `potluck service install` again"),
        "{}",
        r.stderr
    );
    assert!(!dir.exists(), "nothing written");

    let out = daemon().wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(
        stderr.contains("another potluck daemon is running for")
            && stderr.contains("`potluck status` talks to it"),
        "{stderr}"
    );
}

/// Stand-ins for `systemctl`, `loginctl` and `launchctl`, in `dir/bin` (put it first on PATH), so
/// `service install` runs as with a service manager without touching the real one. Each call is
/// a line in `dir/calls`. `systemctl --user` fails as the real one does when it can't find the
/// user manager's bus (no XDG_RUNTIME_DIR and no DBUS_SESSION_BUS_ADDRESS). A service counts as
/// running from `enable --now` or `bootstrap` until `disable --now` or `bootout`. Lingering is on.
fn fake_service_tools(dir: &Path) -> (PathBuf, PathBuf) {
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let calls = dir.join("calls");
    let running = dir.join("running");
    let script = |body: &str| {
        format!(
            "#!/bin/sh\nCALLS='{}'\nRUNNING='{}'\n{body}",
            calls.display(),
            running.display()
        )
    };
    let tools = [
        (
            "systemctl",
            script(
                r#"if [ -z "${XDG_RUNTIME_DIR:-}" ] && [ -z "${DBUS_SESSION_BUS_ADDRESS:-}" ]; then
    echo 'Failed to connect to user scope bus via local transport: $DBUS_SESSION_BUS_ADDRESS and $XDG_RUNTIME_DIR not defined' >&2
    exit 1
fi
echo "systemctl $*" >>"$CALLS"
case "$*" in
    *--property=Version*) echo 262 ;;
    *is-active*) [ -f "$RUNNING" ] || exit 3 ;;
    *"enable --now"*) touch "$RUNNING" ;;
    *"disable --now"*) rm -f "$RUNNING" ;;
esac
exit 0
"#,
            ),
        ),
        (
            "loginctl",
            script("echo \"loginctl $*\" >>\"$CALLS\"\necho yes\n"),
        ),
        (
            "launchctl",
            script(
                r#"echo "launchctl $*" >>"$CALLS"
case "$1" in
    print) [ -f "$RUNNING" ] || exit 113 ;;
    bootstrap) touch "$RUNNING" ;;
    bootout) rm -f "$RUNNING" ;;
esac
exit 0
"#,
            ),
        ),
    ];
    for (name, text) in tools {
        let path = bin.join(name);
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    (bin, calls)
}

/// `service install` from a shell without XDG_RUNTIME_DIR (`su`, cron): the daemon and preflight
/// reach the user manager at /run/user/UID then, and so must the install. It used to pass
/// preflight and then fail at `systemctl --user daemon-reload` ("Failed to connect to user scope
/// bus").
#[cfg(target_os = "linux")]
#[test]
fn service_install_finds_the_user_manager_without_xdg_runtime_dir() {
    let home = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let id = std::process::id();
    std::fs::write(
        home.path().join("node.toml"),
        format!("name = \"xdg-{id}\"\n"),
    )
    .unwrap();
    let (bin, calls) = fake_service_tools(home.path());
    let dir = home.path().join("services");
    let name = format!("potluck-test-xdg-{id}");
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let svc = |action: &str| {
        run(potluck(home.path())
            .env("PATH", &path)
            .env_remove("XDG_RUNTIME_DIR")
            .env_remove("DBUS_SESSION_BUS_ADDRESS")
            .env("POTLUCK_SERVICE_DIR", &dir)
            .env("POTLUCK_SERVICE_NAME", &name)
            .args(["service", action]))
    };
    let r = svc("install");
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
    let unit = format!("{name}.service");
    let log = std::fs::read_to_string(&calls).unwrap();
    for want in [
        "systemctl --user daemon-reload".to_string(),
        format!("systemctl --user enable --now {unit}"),
    ] {
        assert!(log.lines().any(|l| l == want), "{want:?} in\n{log}");
    }
    let r = svc("uninstall");
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
    let log = std::fs::read_to_string(&calls).unwrap();
    assert!(
        log.lines()
            .any(|l| l == format!("systemctl --user disable --now {unit}")),
        "{log}"
    );
}

/// `service install` again with nothing to change leaves the running daemon alone and says it
/// changed nothing, on both systems; a service file that differs is rewritten and the daemon
/// restarted. macOS used to restart the daemon every time, and `changed` was always true.
#[test]
fn service_install_restarts_the_daemon_only_when_its_service_changed() {
    let home = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let id = std::process::id();
    std::fs::write(
        home.path().join("node.toml"),
        format!("name = \"again-{id}\"\n"),
    )
    .unwrap();
    let (bin, calls) = fake_service_tools(home.path());
    let dir = home.path().join("services");
    let name = format!("potluck-test-again-{id}");
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let install = || {
        let _ = std::fs::remove_file(&calls);
        let r = run(potluck(home.path())
            .env("PATH", &path)
            .env("POTLUCK_SERVICE_DIR", &dir)
            .env("POTLUCK_SERVICE_NAME", &name)
            .args(["--json", "service", "install"]));
        assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
        let done: Value = serde_json::from_str(&r.stdout).unwrap();
        let log = std::fs::read_to_string(&calls).unwrap_or_default();
        (done, log)
    };
    let (file, restarts): (PathBuf, &[&str]) = if cfg!(target_os = "macos") {
        (dir.join(format!("{name}.plist")), &["bootout", "bootstrap"])
    } else {
        (dir.join(format!("{name}.service")), &["restart"])
    };
    let restarted = |log: &str| {
        restarts
            .iter()
            .any(|verb| log.lines().any(|l| l.split(' ').any(|w| w == *verb)))
    };

    let (done, _) = install();
    assert_eq!(done["changed"], true, "{done}");
    let (done, log) = install();
    assert_eq!(done["changed"], false, "{done}");
    assert!(!restarted(&log), "nothing to change, yet:\n{log}");
    assert!(
        done["message"].as_str().unwrap().contains("already"),
        "{done}"
    );

    let text = std::fs::read_to_string(&file).unwrap();
    std::fs::write(&file, text.replace("daemon", "daemon-old")).unwrap();
    let (done, log) = install();
    assert_eq!(done["changed"], true, "{done}");
    assert!(restarted(&log), "the service changed, yet:\n{log}");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), text);
}

/// Removes the test service if an assertion failed halfway.
fn svc_guard(dir: &Path, name: &str) -> impl Drop {
    struct G(PathBuf, String);
    impl Drop for G {
        fn drop(&mut self) {
            if cfg!(target_os = "macos") {
                let uid = unsafe { libc::getuid() };
                let _ = Command::new("launchctl")
                    .args(["bootout", &format!("gui/{uid}/{}", self.1)])
                    .output();
                let _ = std::fs::remove_file(self.0.join(format!("{}.plist", self.1)));
            } else {
                let unit = format!("{}.service", self.1);
                let _ = Command::new("systemctl")
                    .args(["--user", "disable", "--now", &unit])
                    .output();
                let _ = std::fs::remove_file(self.0.join(&unit));
                let _ = Command::new("systemctl")
                    .args(["--user", "daemon-reload"])
                    .output();
                let _ = Command::new("systemctl")
                    .args(["--user", "reset-failed", &unit])
                    .output();
            }
        }
    }
    G(dir.to_path_buf(), name.to_string())
}

#[test]
fn self_update_here_runs_the_update_module_and_reports_its_failure() {
    // A closed port: the update fails at its first download, before touching any file.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let home = tempfile::tempdir().unwrap();
    let before = std::fs::metadata(env!("CARGO_BIN_EXE_potluck"))
        .unwrap()
        .modified()
        .unwrap();
    let r = run(potluck(home.path())
        .env("POTLUCK_RELEASES_URL", format!("http://127.0.0.1:{port}/"))
        .args(["--json", "self-update", "--here"]));
    assert_eq!(r.code, 2, "{}", r.stderr);
    let doc = one_json(&r.stdout);
    let e = doc["error"].as_str().unwrap();
    assert!(
        e.starts_with(&format!("downloading http://127.0.0.1:{port}/SHA256SUMS")),
        "{e}"
    );
    let after = std::fs::metadata(env!("CARGO_BIN_EXE_potluck"))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(before, after, "the binary is untouched");
}

/// Every command takes `--json`: with no daemon to answer, each prints one JSON document saying
/// why, and nothing on stderr (spec: every command accepts `--json`; none prompts).
#[test]
fn every_command_answers_in_json_without_a_daemon() {
    let home = tempfile::tempdir().unwrap();
    let dest = home.path().join("dest");
    let dest = dest.to_str().unwrap();
    let commands: &[&[&str]] = &[
        &["dispatch", "--timeout", "1m", "--", "true"],
        &["wait", "j-1"],
        &["logs", "j-1"],
        &["logs", "j-1", "--follow"],
        &["cancel", "j-1"],
        &["hold", "n-w1", "5m"],
        &["release", "n-w1"],
        &["cp", "n-w1:x", dest],
        &["status"],
        &["why", "j-1"],
        &["nodes"],
        &["pair"],
        &["pair", "--finish", "x"],
        &["admit", "x"],
        &["remove", "n"],
        &["self-update"],
    ];
    for args in commands {
        let r = run(potluck(home.path())
            .current_dir(home.path())
            // Never the real releases, should a command reach for them.
            .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
            .arg("--json")
            .args(*args));
        assert_eq!(r.code, 2, "{args:?}: {} {}", r.stdout, r.stderr);
        let doc = one_json(&r.stdout);
        assert!(
            doc["error"].as_str().is_some_and(|e| !e.is_empty()),
            "{args:?}: {doc}"
        );
        assert_eq!(r.stderr, "", "{args:?}");
    }
}
