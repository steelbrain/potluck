//! Git over the mesh: real repositories in temp dirs, two meshes on 127.0.0.1 with free ports, and
//! the potluck binary as git's `ext::` command (`potluck _tunnel`).
//!
//! The source side runs a mesh whose handler serves `git upload-pack` for the repositories it
//! knows. The runner side is a state dir paired with it (its daemon's mesh runs, as it would), and
//! fetches with the runner's POTLUCK_HOME and POTLUCK_CONFIG. The source advertises the address of
//! a counting proxy in front of it, so a test can see how many bytes a fetch moved.

use potluck::config::Paths;
use potluck::mesh::{Handler, Mesh, MeshConfig, Peer, Stream};
use potluck::tunnel;
use potluck::workspace::{SyncReport, Workspaces};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_potluck");
/// The longest any one git command or wait may take here.
const LIMIT: Duration = Duration::from_secs(60);

fn tmp() -> tempfile::TempDir {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    std::fs::create_dir_all(&root).unwrap();
    tempfile::Builder::new()
        .prefix("potluck-test-")
        .tempdir_in(root)
        .unwrap()
}

/// A repository id unique to this test run, so leftover processes can be found by it.
fn unique_id(dir: &Path) -> String {
    let tail: String = dir
        .file_name()
        .unwrap()
        .to_string_lossy()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    format!("app-{}", tail.to_lowercase())
}

/// Runs `cmd` with `input` on its stdin, killing it after `limit`.
fn run(cmd: &mut Command, input: &[u8], limit: Duration) -> Output {
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let input = input.to_vec();
    let writer = thread::spawn(move || {
        let _ = stdin.write_all(&input);
    });
    let read_all = |mut r: Box<dyn Read + Send>| {
        thread::spawn(move || {
            let mut v = Vec::new();
            let _ = r.read_to_end(&mut v);
            v
        })
    };
    let out = read_all(Box::new(child.stdout.take().unwrap()));
    let err = read_all(Box::new(child.stderr.take().unwrap()));
    let start = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        if start.elapsed() > limit {
            let _ = child.kill();
            let _ = child.wait();
            panic!("timed out after {limit:?}: {cmd:?}");
        }
        thread::sleep(Duration::from_millis(10));
    };
    let _ = writer.join();
    Output {
        status,
        stdout: out.join().unwrap(),
        stderr: err.join().unwrap(),
    }
}

/// Runs `f` on a thread of its own, failing the test if it takes longer than [`LIMIT`].
fn within<T: Send + 'static>(what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(LIMIT)
        .unwrap_or_else(|_| panic!("{what} took longer than {LIMIT:?}"))
}

/// Runs git hermetically (no user or system config), with fixed dates, in `dir`.
fn git(dir: &Path, args: &[&str]) -> String {
    let date = "@1700000000 +0000";
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(dir)
        .args([
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
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
            "-c",
            "core.compression=1",
        ])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_DATE", date)
        .env("GIT_COMMITTER_DATE", date);
    let out = run(&mut cmd, b"", LIMIT);
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn commit_file(repo: &Path, name: &str, content: &[u8], message: &str) -> String {
    std::fs::write(repo.join(name), content).unwrap();
    git(repo, &["add", name]);
    git(repo, &["commit", "-q", "-m", message]);
    git(repo, &["rev-parse", "HEAD"])
}

/// Bytes that don't compress, so a pack carrying them is at least this big.
fn noise(len: usize) -> Vec<u8> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut v = Vec::with_capacity(len + 8);
    while v.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(len);
    v
}

/// `sha refname` for every branch and tag.
fn refs(repo: &Path) -> String {
    git(
        repo,
        &[
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            "refs/heads",
            "refs/tags",
        ],
    )
}

/// Forwards connections to the source's real port and counts the bytes coming back.
struct Proxy {
    addr: SocketAddr,
    target: Arc<OnceLock<SocketAddr>>,
    down: Arc<AtomicU64>,
}

impl Proxy {
    fn start() -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let target = Arc::new(OnceLock::<SocketAddr>::new());
        let down = Arc::new(AtomicU64::new(0));
        let (t, d) = (target.clone(), down.clone());
        thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { return };
                let Some(target) = t.get() else { continue };
                let Ok(server) = TcpStream::connect(target) else {
                    continue;
                };
                let pipe =
                    |mut from: TcpStream, mut to: TcpStream, count: Option<Arc<AtomicU64>>| {
                        thread::spawn(move || {
                            let mut buf = [0u8; 32 * 1024];
                            loop {
                                match from.read(&mut buf) {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => {
                                        if to.write_all(&buf[..n]).is_err() {
                                            break;
                                        }
                                        if let Some(c) = &count {
                                            c.fetch_add(n as u64, Ordering::SeqCst);
                                        }
                                    }
                                }
                            }
                            let _ = to.shutdown(std::net::Shutdown::Write);
                        });
                    };
                pipe(
                    client.try_clone().unwrap(),
                    server.try_clone().unwrap(),
                    None,
                );
                pipe(server, client, Some(d.clone()));
            }
        });
        Proxy { addr, target, down }
    }

    fn bytes(&self) -> u64 {
        self.down.load(Ordering::SeqCst)
    }
}

/// What the source serves: repository id to git dir, and each serve's outcome.
struct Source {
    repos: Mutex<HashMap<String, PathBuf>>,
    served: Mutex<Sender<(String, Result<(), String>)>>,
}

impl Handler for Source {
    fn call(&self, _from: &Peer, _msg: Value) -> Result<Value, String> {
        Err("no calls here".into())
    }

    fn stream(&self, _from: &Peer, header: Value, stream: Stream) {
        let Some(id) = tunnel::upload_pack_repo(&header) else {
            let _ = tunnel::refuse_upload_pack(stream, "not an upload-pack request");
            return;
        };
        let dir = self.repos.lock().unwrap().get(&id).cloned();
        let Some(dir) = dir else {
            let _ =
                tunnel::refuse_upload_pack(stream, &format!("no repository {id} is served here"));
            return;
        };
        let result = tunnel::serve_upload_pack(stream, &dir).map_err(|e| e.to_string());
        let _ = self.served.lock().unwrap().send((id, result));
    }
}

/// Members' streams are refused; nothing else is asked of a runner here.
struct Quiet;

impl Handler for Quiet {
    fn call(&self, _from: &Peer, _msg: Value) -> Result<Value, String> {
        Err("no calls here".into())
    }
}

fn mesh_config(name: &str, home: &Path, address: String) -> MeshConfig {
    MeshConfig {
        name: name.to_string(),
        keys_dir: home.join("keys"),
        listen: "127.0.0.1:0".parse().unwrap(),
        address: Some(address),
        version: potluck::VERSION.to_string(),
        heartbeat: Duration::from_millis(200),
        down_after: Duration::from_secs(5),
        updating_for: Duration::from_secs(600),
        connect_timeout: Duration::from_secs(2),
        call_timeout: Duration::from_secs(10),
    }
}

/// A source with one repository and a runner paired with it.
struct Cluster {
    root: tempfile::TempDir,
    repo: PathBuf,
    repo_id: String,
    proxy: Proxy,
    source: Mesh,
    handler: Arc<Source>,
    served: Receiver<(String, Result<(), String>)>,
    runner: Mesh,
    runner_paths: Paths,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.runner.stop();
        self.source.stop();
    }
}

fn cluster() -> Cluster {
    let root = tmp();
    let repo = root.path().join("source repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q"]);
    let repo_id = unique_id(root.path());

    let proxy = Proxy::start();
    let (tx, served) = mpsc::channel();
    let handler = Arc::new(Source {
        repos: Mutex::new(HashMap::from([(repo_id.clone(), repo.join(".git"))])),
        served: Mutex::new(tx),
    });
    let source = Mesh::start(
        mesh_config(
            "source",
            &root.path().join("source"),
            proxy.addr.to_string(),
        ),
        handler.clone(),
    )
    .unwrap();
    proxy.target.set(source.local_addr().unwrap()).unwrap();

    let runner_home = root.path().join("runner");
    let runner = Mesh::start(
        mesh_config("runner", &runner_home, "127.0.0.1".into()),
        Arc::new(Quiet),
    )
    .unwrap();
    // The runner's config, as its daemon has it: what `_tunnel` loads.
    let config = runner_home.join("node.toml");
    std::fs::write(
        &config,
        format!(
            "name = \"runner\"\nport = {}\naddress = \"127.0.0.1\"\n",
            runner.local_addr().unwrap().port()
        ),
    )
    .unwrap();
    let request = runner.pair_request().unwrap();
    let reply = source.admit(&request).unwrap();
    runner.pair_finish(&reply).unwrap();

    Cluster {
        root,
        repo,
        repo_id,
        proxy,
        source,
        handler,
        served,
        runner,
        runner_paths: Paths {
            home: runner_home,
            config,
        },
    }
}

impl Cluster {
    fn url(&self) -> String {
        tunnel::fetch_url(Path::new(BIN), "source", &self.repo_id)
    }

    /// `potluck _tunnel` as git would run it, as the runner.
    fn tunnel_cmd(&self, paths: &Paths, repo_id: &str) -> Command {
        let mut cmd = Command::new(BIN);
        cmd.args(["_tunnel", "source", repo_id])
            .env("POTLUCK_HOME", &paths.home)
            .env("POTLUCK_CONFIG", &paths.config)
            .env_remove("GIT_EXT_SERVICE");
        cmd
    }

    /// A plain `git fetch` of every branch into the bare repository `into`, as the runner.
    fn fetch(&self, into: &Path, url: &str) -> Output {
        let mut cmd = Command::new("git");
        cmd.arg("-C")
            .arg(into)
            .args(tunnel::git_args(url))
            .args(["fetch", "--quiet", url, "+refs/heads/*:refs/heads/*"])
            .env("POTLUCK_HOME", &self.runner_paths.home)
            .env("POTLUCK_CONFIG", &self.runner_paths.config);
        run(&mut cmd, b"", LIMIT)
    }

    fn bare(&self, name: &str) -> PathBuf {
        let dir = self.root.path().join(name);
        git(
            self.root.path(),
            &["init", "-q", "--bare", dir.to_str().unwrap()],
        );
        dir
    }

    /// The outcomes of the next `n` serves.
    fn served(&self, n: usize) -> Vec<Result<(), String>> {
        (0..n)
            .map(|_| {
                let (id, r) = self
                    .served
                    .recv_timeout(LIMIT)
                    .expect("the source served nothing");
                assert_eq!(id, self.repo_id);
                r
            })
            .collect()
    }

    fn assert_no_processes(&self) {
        assert_no_processes(&[
            self.repo.to_str().unwrap(),
            &self.repo_id,
            self.root.path().to_str().unwrap(),
        ]);
    }
}

/// The processes whose command line mentions any of `needles`.
fn processes(needles: &[&str]) -> Vec<String> {
    let out = Command::new("ps")
        .args(["-A", "-ww", "-o", "pid=", "-o", "args="])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| needles.iter().any(|n| l.contains(n)))
        .map(str::to_string)
        .collect()
}

/// The process group of the `git upload-pack` serving `repo` (it leads a group of its own).
fn upload_pack_group(repo: &Path) -> String {
    let out = Command::new("ps")
        .args(["-A", "-ww", "-o", "pgid=", "-o", "args="])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let line = text
        .lines()
        .find(|l| l.contains(repo.to_str().unwrap()) && l.contains("upload-pack"))
        .unwrap_or_else(|| panic!("no upload-pack for {}", repo.display()));
    line.split_whitespace().next().unwrap().to_string()
}

/// The processes in process group `pgid`.
fn group(pgid: &str) -> Vec<String> {
    let out = Command::new("ps")
        .args(["-A", "-ww", "-o", "pgid=", "-o", "pid=", "-o", "args="])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.split_whitespace().next() == Some(pgid))
        .map(str::to_string)
        .collect()
}

fn pkt(line: &str) -> String {
    format!("{:04x}{line}", line.len() + 4)
}

/// Waits a little for processes to finish exiting, then fails if any mention `needles`.
fn assert_no_processes(needles: &[&str]) {
    let start = Instant::now();
    loop {
        let left = processes(needles);
        if left.is_empty() {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "processes left over: {left:#?}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// Splits git protocol output into pkt-lines (`None` for a flush), failing on anything else.
fn pkt_lines(mut bytes: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut lines = Vec::new();
    while !bytes.is_empty() {
        assert!(bytes.len() >= 4, "a stray tail: {bytes:?}");
        let len = std::str::from_utf8(&bytes[..4])
            .ok()
            .and_then(|h| usize::from_str_radix(h, 16).ok())
            .unwrap_or_else(|| panic!("not a pkt-line: {:?}", String::from_utf8_lossy(bytes)));
        if len == 0 {
            lines.push(None);
            bytes = &bytes[4..];
            continue;
        }
        assert!(len >= 4 && len <= bytes.len(), "bad pkt-line length {len}");
        lines.push(Some(bytes[4..len].to_vec()));
        bytes = &bytes[len..];
    }
    lines
}

/// The runner's mirror follows the source: first sync, an incremental one, a force-pushed and a
/// deleted branch, and a commit on no branch. Only this test sets the process environment (the
/// mirror's git inherits it, and runs `_tunnel` with it, as the daemon's would).
#[test]
fn a_mirror_syncs_over_the_mesh() {
    let c = cluster();
    // SAFETY: no other test in this binary reads these variables or sets the environment; every
    // other command here sets them explicitly.
    unsafe {
        std::env::set_var("POTLUCK_HOME", &c.runner_paths.home);
        std::env::set_var("POTLUCK_CONFIG", &c.runner_paths.config);
    }
    let repo = &c.repo;
    commit_file(repo, "big.bin", &noise(2 << 20), "a big file");
    commit_file(repo, "a.txt", b"one\n", "first");
    git(repo, &["branch", "feature"]);
    git(repo, &["branch", "doomed"]);
    git(repo, &["tag", "v1"]);
    git(repo, &["tag", "-a", "-m", "notes", "v1-notes"]);
    git(repo, &["tag", "old"]);
    git(repo, &["checkout", "-q", "feature"]);
    let feature_old = commit_file(repo, "f.txt", b"feature\n", "feature work");
    git(repo, &["checkout", "-q", "main"]);

    let sync = |c: &Cluster| -> SyncReport {
        let (paths, id, url) = (c.runner_paths.clone(), c.repo_id.clone(), c.url());
        within("sync_mirror", move || {
            Workspaces::new(paths).sync_mirror(&id, &url)
        })
        .unwrap()
    };
    let mirror = c.runner_paths.mirror(&c.repo_id);

    // The first sync: every branch and tag.
    let before = c.proxy.bytes();
    let report = sync(&c);
    let first = c.proxy.bytes() - before;
    assert!(report.created, "{report:?}");
    assert_eq!(refs(&mirror), refs(repo));
    assert!(refs(&mirror).contains("refs/tags/v1-notes"));
    assert!(first > 2 << 20, "the first sync moved only {first} bytes");
    assert_eq!(c.served(1), [Ok(())]);

    // One new commit: only a small pack travels.
    let next = commit_file(repo, "a.txt", b"two\n", "second");
    let before = c.proxy.bytes();
    let report = sync(&c);
    let second = c.proxy.bytes() - before;
    assert_eq!(report, SyncReport::default());
    assert_eq!(refs(&mirror), refs(repo));
    assert_eq!(git(&mirror, &["rev-parse", "refs/heads/main"]), next);
    eprintln!("first sync: {first} bytes from the source; incremental: {second}");
    assert!(
        second < 64 << 10,
        "an incremental sync moved {second} bytes (the first {first})"
    );
    assert_eq!(c.served(1), [Ok(())]);

    // A force-pushed branch, a deleted branch and a deleted tag.
    git(repo, &["checkout", "-q", "-B", "feature", "main~1"]);
    let feature_new = commit_file(repo, "g.txt", b"rewritten\n", "rewritten feature");
    git(repo, &["checkout", "-q", "main"]);
    git(repo, &["branch", "-D", "doomed"]);
    git(repo, &["tag", "-d", "old"]);
    let report = sync(&c);
    assert_eq!(report, SyncReport::default());
    assert_eq!(refs(&mirror), refs(repo));
    assert_eq!(
        git(&mirror, &["rev-parse", "refs/heads/feature"]),
        feature_new
    );
    assert_ne!(feature_new, feature_old);
    let mirrored = refs(&mirror);
    assert!(!mirrored.contains("refs/heads/doomed") && !mirrored.contains("refs/tags/old"));
    assert_eq!(c.served(1), [Ok(())]);

    // A commit on no branch or tag (a job's detached HEAD, since moved on): fetch_pin gets it.
    let tree = git(repo, &["rev-parse", "HEAD^{tree}"]);
    let loose = git(
        repo,
        &["commit-tree", &tree, "-p", "HEAD", "-m", "detached"],
    );
    let (paths, id, url, sha) = (
        c.runner_paths.clone(),
        c.repo_id.clone(),
        c.url(),
        loose.clone(),
    );
    within("fetch_pin", move || {
        Workspaces::new(paths).fetch_pin(&id, &url, &sha, "job-1")
    })
    .unwrap();
    assert_eq!(
        git(&mirror, &["rev-parse", "refs/potluck/pins/job-1"]),
        loose
    );
    assert_eq!(c.served(1), [Ok(())]);

    // The mirror's git ran with ext:: allowed for its own commands only.
    let allowed = Command::new("git")
        .arg("-C")
        .arg(&mirror)
        .args(["config", "--get", "protocol.ext.allow"])
        .output()
        .unwrap();
    assert!(
        allowed.stdout.is_empty(),
        "ext:: was allowed in the mirror's config"
    );
    c.assert_no_processes();
}

#[test]
fn an_unknown_repository_is_refused_clearly() {
    let c = cluster();
    commit_file(&c.repo, "a.txt", b"one\n", "first");
    let into = c.bare("into.git");
    let url = tunnel::fetch_url(Path::new(BIN), "source", "nope-00000000");
    let out = c.fetch(&into, &url);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(
        err.contains("remote error: no repository nope-00000000 is served here"),
        "{err}"
    );

    // Run by hand, `_tunnel` prints exactly git's error packet.
    let out = run(
        &mut c.tunnel_cmd(&c.runner_paths, "nope-00000000"),
        b"",
        LIMIT,
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        pkt_lines(&out.stdout),
        [Some(
            b"ERR no repository nope-00000000 is served here\n".to_vec()
        )]
    );

    // A known id whose git dir is gone: git upload-pack's own error comes back.
    let gone = c.root.path().join("gone");
    c.handler
        .repos
        .lock()
        .unwrap()
        .insert("gone-00000000".into(), gone.clone());
    let url = tunnel::fetch_url(Path::new(BIN), "source", "gone-00000000");
    let out = c.fetch(&into, &url);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(
        err.contains("remote error: git upload-pack") && err.contains(gone.to_str().unwrap()),
        "{err}"
    );
    let (id, result) = c.served.recv_timeout(LIMIT).unwrap();
    assert_eq!(id, "gone-00000000");
    assert!(result.is_err());
    c.assert_no_processes();
}

#[test]
fn a_non_member_is_refused() {
    let c = cluster();
    commit_file(&c.repo, "a.txt", b"one\n", "first");

    // A machine with its own key and a copy of the runner's records: it knows where the source
    // is, but its key was never admitted.
    let outsider = c.root.path().join("outsider");
    Mesh::start(
        mesh_config("outsider", &outsider, "127.0.0.1".into()),
        Arc::new(Quiet),
    )
    .unwrap()
    .stop();
    for e in std::fs::read_dir(c.runner_paths.keys_dir()).unwrap() {
        let e = e.unwrap();
        if e.file_name() != "node.key" && e.file_type().unwrap().is_file() {
            std::fs::copy(e.path(), outsider.join("keys").join(e.file_name())).unwrap();
        }
    }
    let paths = Paths {
        home: outsider.clone(),
        config: outsider.join("node.toml"),
    };
    let out = run(&mut c.tunnel_cmd(&paths, &c.repo_id), b"0000", LIMIT);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(out.stdout.is_empty(), "{:?}", out.stdout);
    assert!(
        err.starts_with("potluck _tunnel: can't fetch") && err.contains("source"),
        "{err}"
    );
    // Its own records admit no key of its own, so it doesn't even dial: it knows it isn't in the
    // cluster. (A stranger's key reaching a member is refused in the TLS handshake; the mesh's
    // tests cover that.)
    assert!(err.contains("no longer in the cluster"), "{err}");

    // A machine that never paired: the source isn't a member of its cluster at all.
    let stranger = c.root.path().join("stranger");
    Mesh::start(
        mesh_config("stranger", &stranger, "127.0.0.1".into()),
        Arc::new(Quiet),
    )
    .unwrap()
    .stop();
    let paths = Paths {
        home: stranger.clone(),
        config: stranger.join("node.toml"),
    };
    let out = run(&mut c.tunnel_cmd(&paths, &c.repo_id), b"", LIMIT);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success() && out.stdout.is_empty());
    assert!(
        err.contains("source is not a member of this cluster"),
        "{err}"
    );

    // Through git, the outsider's fetch fails with the tunnel's reason.
    let into = c.bare("into.git");
    let url = c.url();
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(&into)
        .args(tunnel::git_args(&url))
        .args(["fetch", "--quiet", &url, "+refs/heads/*:refs/heads/*"])
        .env("POTLUCK_HOME", &outsider)
        .env("POTLUCK_CONFIG", outsider.join("node.toml"));
    let out = run(&mut cmd, b"", LIMIT);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(err.contains("potluck _tunnel: can't fetch"), "{err}");
    c.assert_no_processes();
}

#[test]
fn only_git_protocol_goes_to_stdout() {
    let c = cluster();
    let head = commit_file(&c.repo, "a.txt", b"one\n", "first");
    // The client wants nothing: a flush. upload-pack sends its refs and ends.
    let out = run(
        &mut c.tunnel_cmd(&c.runner_paths, &c.repo_id),
        b"0000",
        LIMIT,
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = pkt_lines(&out.stdout);
    let first = String::from_utf8_lossy(lines[0].as_ref().unwrap()).into_owned();
    assert!(first.starts_with(&format!("{head} HEAD\0")), "{first:?}");
    assert!(lines[1..lines.len() - 1].iter().all(Option::is_some));
    assert_eq!(lines.last(), Some(&None));
    assert_eq!(c.served(1), [Ok(())]);

    // Pushing is refused before anything is sent.
    let into = c.bare("into.git");
    let url = c.url();
    let out = c.fetch(&into, &url);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(&into)
        .args(tunnel::git_args(&url))
        .args(["push", "--quiet", &url, "main:refs/heads/other"])
        .env("POTLUCK_HOME", &c.runner_paths.home)
        .env("POTLUCK_CONFIG", &c.runner_paths.config);
    let out = run(&mut cmd, b"", LIMIT);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(err.contains("only fetching works over the mesh"), "{err}");
    assert_eq!(c.served(1), [Ok(())]);
    c.assert_no_processes();
}

#[test]
fn a_program_path_with_spaces_and_percents() {
    let c = cluster();
    let head = commit_file(&c.repo, "a.txt", b"one\n", "first");
    let dir = c.root.path().join("my tools 100% %s");
    std::fs::create_dir_all(&dir).unwrap();
    let bin = dir.join("pot luck");
    std::os::unix::fs::symlink(BIN, &bin).unwrap();
    let url = tunnel::fetch_url(&bin, "source", &c.repo_id);
    assert!(url.contains("my% tools% 100%%% %%s/pot% luck"), "{url}");
    let into = c.bare("into.git");
    let out = c.fetch(&into, &url);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(git(&into, &["rev-parse", "refs/heads/main"]), head);
    assert_eq!(c.served(1), [Ok(())]);

    // Without the per-command config, git refuses the ext:: transport.
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(&into)
        .args(["-c", "protocol.ext.allow=never"])
        .args(["fetch", "--quiet", &url]);
    let out = run(&mut cmd, b"", LIMIT);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("transport 'ext' not allowed"));
    c.assert_no_processes();
}

/// A repository in a directory whose name starts with '.' has an id that starts with '-' (as may
/// a node name): `_tunnel` must take them as its arguments, not as options.
#[test]
fn a_repository_id_that_starts_with_a_hyphen() {
    let c = cluster();
    let head = commit_file(&c.repo, "a.txt", b"one\n", "first");
    let id = potluck::types::repo_id(&c.root.path().join(".dotfiles/.git"));
    assert!(id.starts_with('-'), "{id}");
    c.handler
        .repos
        .lock()
        .unwrap()
        .insert(id.clone(), c.repo.join(".git"));
    let into = c.bare("into.git");
    let out = c.fetch(&into, &tunnel::fetch_url(Path::new(BIN), "source", &id));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(git(&into, &["rev-parse", "refs/heads/main"]), head);
    let (served, result) = c.served.recv_timeout(LIMIT).unwrap();
    assert_eq!((served.as_str(), result), (id.as_str(), Ok(())));
    c.assert_no_processes();
}

/// A `_tunnel` started by hand, and its stdout, which the test holds open without reading.
struct HalfFetch {
    child: std::process::Child,
    _stdout: std::process::ChildStdout,
}

impl std::ops::Deref for HalfFetch {
    type Target = std::process::Child;
    fn deref(&self) -> &std::process::Child {
        &self.child
    }
}

impl std::ops::DerefMut for HalfFetch {
    fn deref_mut(&mut self) -> &mut std::process::Child {
        &mut self.child
    }
}

/// A daemon listening on a port the system picked (`port = 0`, as tests do): the tunnel, which
/// doesn't listen, names no address in its hello, so the source keeps reaching the runner where
/// it listens.
#[test]
fn a_tunnel_from_a_daemon_on_port_0_leaves_its_address_alone() {
    let c = cluster();
    let head = commit_file(&c.repo, "a.txt", b"one\n", "first");
    let address = c.source.member("runner").unwrap().address;
    assert!(!address.ends_with(":0"), "{address}");
    std::fs::write(
        &c.runner_paths.config,
        "name = \"runner\"\nlisten = \"127.0.0.1\"\nport = 0\naddress = \"127.0.0.1\"\n",
    )
    .unwrap();
    let into = c.bare("into.git");
    let out = c.fetch(&into, &c.url());
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(git(&into, &["rev-parse", "refs/heads/main"]), head);
    assert_eq!(c.served(1), [Ok(())]);
    assert_eq!(c.source.member("runner").unwrap().address, address);
}

/// Starts `_tunnel` by hand and reads the source's refs, leaving `git upload-pack` waiting for
/// the client's wants.
fn half_fetch(c: &Cluster) -> HalfFetch {
    let mut child = c
        .tunnel_cmd(&c.runner_paths, &c.repo_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut got = Vec::new();
        let mut buf = [0u8; 4096];
        // Until the flush that ends the refs.
        while !got.ends_with(b"0000") {
            match stdout.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
        let _ = tx.send((got, stdout));
    });
    let (refs, stdout) = rx.recv_timeout(LIMIT).expect("no refs from the tunnel");
    assert!(
        refs.ends_with(b"0000"),
        "{:?}",
        String::from_utf8_lossy(&refs)
    );
    // The git dir's upload-pack is running.
    assert!(!processes(&[c.repo.to_str().unwrap()]).is_empty());
    HalfFetch {
        child,
        _stdout: stdout,
    }
}

#[test]
fn upload_pack_dies_when_the_runner_goes_away() {
    let c = cluster();
    commit_file(&c.repo, "a.txt", b"one\n", "first");
    let mut tunnel = half_fetch(&c);
    tunnel.kill().unwrap();
    tunnel.wait().unwrap();
    let served = c.served(1);
    assert!(served[0].is_err(), "{served:?}");
    c.assert_no_processes();
}

/// The runner dies while the source is still packing and sending: `git upload-pack` and the
/// `git pack-objects` it started are killed.
#[test]
fn upload_pack_and_its_children_die_when_the_runner_goes_away_mid_pack() {
    let c = cluster();
    // Far more than every socket and pipe buffer on the way can hold.
    let head = commit_file(&c.repo, "big.bin", &noise(64 << 20), "big");
    let mut tunnel = half_fetch(&c);
    let pgid = upload_pack_group(&c.repo);
    let wants = format!(
        "{}0000{}",
        pkt(&format!("want {head} side-band-64k ofs-delta\n")),
        pkt("done\n")
    );
    // Nobody reads the tunnel's stdout from here on: the transfer stalls.
    tunnel
        .stdin
        .as_mut()
        .unwrap()
        .write_all(wants.as_bytes())
        .unwrap();
    let start = Instant::now();
    while group(&pgid).len() < 2 {
        assert!(
            start.elapsed() < LIMIT,
            "upload-pack started no pack-objects"
        );
        thread::sleep(Duration::from_millis(20));
    }
    tunnel.kill().unwrap();
    tunnel.wait().unwrap();
    let served = c.served(1);
    assert!(served[0].is_err(), "{served:?}");
    let start = Instant::now();
    while !group(&pgid).is_empty() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "left in upload-pack's group: {:#?}",
            group(&pgid)
        );
        thread::sleep(Duration::from_millis(20));
    }
    c.assert_no_processes();
}

#[test]
fn upload_pack_dies_and_the_tunnel_fails_when_the_source_goes_away() {
    let c = cluster();
    commit_file(&c.repo, "a.txt", b"one\n", "first");
    let mut tunnel = half_fetch(&c);
    c.source.stop();
    let served = c.served(1);
    assert!(served[0].is_err(), "{served:?}");
    let start = Instant::now();
    let status = loop {
        if let Some(s) = tunnel.try_wait().unwrap() {
            break s;
        }
        if start.elapsed() > LIMIT {
            let _ = tunnel.kill();
            panic!("the tunnel didn't end when the source went away");
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut err = String::new();
    tunnel
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert!(!status.success());
    assert!(err.contains("the stream from source"), "{err}");
    drop(tunnel);
    c.assert_no_processes();
}
