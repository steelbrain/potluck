//! Updates against a fake release served over plain HTTP on 127.0.0.1, and install.sh against the
//! same. The fake binaries are shell scripts that answer `--version` like potluck does. No test
//! restarts a service, except the ignored ones at the end, which use services named for tests.

use super::*;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// A release directory's files: name and contents.
type Files = Vec<(String, Vec<u8>)>;
/// Redirects: path, and the location it redirects to.
type Redirects = Vec<(String, String)>;

/// Files and redirects served over plain HTTP on `127.0.0.1:<port>`: a release directory at
/// `/rel/`, and GitHub-style releases of [`GITHUB_REPO`] under `/<repo>/releases/`.
struct Server {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Server {
    /// A release directory at `/rel/`.
    fn start(files: Files) -> Server {
        let files = files
            .into_iter()
            .map(|(name, data)| (format!("/rel/{name}"), data))
            .collect();
        Server::serving(files, Vec::new())
    }

    /// GitHub-style releases: each `(version, files)` at `/<repo>/releases/download/v<version>/`,
    /// and `/<repo>/releases/latest` redirecting to the tag of `latest`.
    fn github(releases: Vec<(&str, Files)>, latest: Option<&str>) -> Server {
        let root = format!("/{GITHUB_REPO}/releases");
        let mut files = Vec::new();
        for (version, release) in releases {
            for (name, data) in release {
                files.push((format!("{root}/download/v{version}/{name}"), data));
            }
        }
        let redirects = latest
            .map(|v| (format!("{root}/latest"), format!("{{base}}{root}/tag/v{v}")))
            .into_iter()
            .collect();
        Server::serving(files, redirects)
    }

    /// `files` and `redirects` by path; `{base}` in a redirect's location is this server's URL.
    fn serving(files: Files, redirects: Redirects) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let files: HashMap<String, Vec<u8>> = files.into_iter().collect();
        let redirects: HashMap<String, String> = redirects
            .into_iter()
            .map(|(path, to)| (path, to.replace("{base}", &format!("http://{addr}"))))
            .collect();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread = {
            let (stop, requests) = (stop.clone(), requests.clone());
            std::thread::spawn(move || {
                for conn in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Ok(conn) = conn {
                        serve(conn, &files, &redirects, &requests);
                    }
                }
            })
        };
        Server {
            addr,
            stop,
            thread: Some(thread),
            requests,
        }
    }

    fn url(&self) -> String {
        format!("http://{}/rel/", self.addr)
    }

    /// The URL to set as `POTLUCK_GITHUB_URL`.
    fn github_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The GitHub-style source this server holds.
    fn releases(&self) -> Source {
        Source::from_env(None, Some(&self.github_url()))
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve(
    mut conn: TcpStream,
    files: &HashMap<String, Vec<u8>>,
    redirects: &HashMap<String, String>,
    requests: &Mutex<Vec<String>>,
) {
    let _ = conn.set_read_timeout(Some(Duration::from_secs(10)));
    let Ok(clone) = conn.try_clone() else { return };
    let mut reader = BufReader::new(clone);
    let mut request = String::new();
    if reader.read_line(&mut request).is_err() {
        return;
    }
    loop {
        let mut header = String::new();
        match reader.read_line(&mut header) {
            Ok(0) | Err(_) => break,
            Ok(_) if header.trim().is_empty() => break,
            Ok(_) => {}
        }
    }
    let path = request.split_whitespace().nth(1).unwrap_or("").to_string();
    requests.lock().unwrap().push(path.clone());
    let (status, body, location) = match (files.get(&path), redirects.get(&path)) {
        (_, Some(to)) => (
            "302 Found",
            &b"redirect\n"[..],
            format!("Location: {to}\r\n"),
        ),
        (Some(data), None) => ("200 OK", data.as_slice(), String::new()),
        (None, None) => ("404 Not Found", &b"not found\n"[..], String::new()),
    };
    let head = format!(
        "HTTP/1.1 {status}\r\n{location}Content-Length: {}\r\n\
         Content-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = conn.write_all(head.as_bytes());
    let _ = conn.write_all(body);
}

/// A stand-in for a potluck binary: a script that answers `--version`.
fn fake_binary(version: &str) -> Vec<u8> {
    format!("#!/bin/sh\necho 'potluck {version}'\n").into_bytes()
}

fn sums_line(name: &str, data: &[u8]) -> String {
    format!("{}  {name}\n", hex(&sha256(data)))
}

/// A release holding `binary` as this platform's asset, a decoy asset for another platform, and
/// a SHA256SUMS listing both.
fn release(binary: &[u8]) -> Files {
    let asset = asset_name().unwrap();
    let other = b"another platform".to_vec();
    let sums =
        sums_line("potluck-riscv64gc-unknown-linux-musl", &other) + &sums_line(asset, binary);
    vec![
        (asset.to_string(), binary.to_vec()),
        ("potluck-riscv64gc-unknown-linux-musl".to_string(), other),
        (SUMS_FILE.to_string(), sums.into_bytes()),
    ]
}

fn with_file(mut files: Files, name: &str, data: Vec<u8>) -> Files {
    files.retain(|(n, _)| n != name);
    files.push((name.to_string(), data));
    files
}

fn without_file(mut files: Files, name: &str) -> Files {
    files.retain(|(n, _)| n != name);
    files
}

/// Updates `target` from the release directory at `url`, without a restart.
fn update_dir(url: &str, target: &Path, version: Option<&str>) -> Result<String> {
    let source = Source::Directory(url.to_string());
    update(&plan(&source, target, version), false)
}

/// A plan with no lower versions allowed and no daemon.
fn plan<'a>(source: &'a Source, target: &'a Path, version: Option<&'a str>) -> Plan<'a> {
    Plan {
        source,
        target,
        version,
        allow_lower: false,
        daemon: None,
    }
}

/// A restart that only counts its calls.
struct FakeRestart(std::cell::Cell<u32>);

impl FakeRestart {
    fn new() -> FakeRestart {
        FakeRestart(std::cell::Cell::new(0))
    }
    fn calls(&self) -> u32 {
        self.0.get()
    }
    fn run(&self) -> Result<Restart> {
        self.0.set(self.0.get() + 1);
        Ok(Restart::Restarted("a fake restart".into()))
    }
}

/// A temp dir with an executable fake potluck at `bin/potluck`.
fn installed(version: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let target = bin.join("potluck");
    fs::write(&target, fake_binary(version)).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();
    (dir, target)
}

fn names_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// The target's bytes and identity, to show it wasn't touched.
fn snapshot(path: &Path) -> (Vec<u8>, u64, i64, i64) {
    let meta = fs::metadata(path).unwrap();
    (
        fs::read(path).unwrap(),
        meta.ino(),
        meta.mtime(),
        meta.mtime_nsec(),
    )
}

#[cfg(target_os = "macos")]
fn has_quarantine(path: &Path) -> bool {
    Command::new("xattr")
        .args(["-p", "com.apple.quarantine"])
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

#[test]
fn updates_from_a_local_release() {
    let (_dir, target) = installed("0.0.1");
    let new = fake_binary("9.9.9");
    let server = Server::start(release(&new));
    let msg = update_dir(&server.url(), &target, None).unwrap();
    assert_eq!(
        msg,
        format!(
            "Updated {} from 0.0.1 to 9.9.9. The daemon was not restarted.",
            target.canonicalize().unwrap().display()
        )
    );
    assert_eq!(fs::read(&target).unwrap(), new);
    assert_eq!(fs::metadata(&target).unwrap().mode() & 0o7777, 0o755);
    assert_eq!(names_in(target.parent().unwrap()), ["potluck"]);
    #[cfg(target_os = "macos")]
    assert!(!has_quarantine(&target));
    let asset = asset_name().unwrap();
    assert_eq!(
        server.requests(),
        [format!("/rel/{SUMS_FILE}"), format!("/rel/{asset}")]
    );
}

#[test]
fn takes_the_requested_version() {
    let (_dir, target) = installed("0.0.1");
    let server = Server::start(release(&fake_binary("9.9.9")));
    let msg = update_dir(server.url().trim_end_matches('/'), &target, Some("9.9.9")).unwrap();
    assert!(msg.contains("to 9.9.9"), "{msg}");
    assert_eq!(fs::read(&target).unwrap(), fake_binary("9.9.9"));
}

#[test]
fn replaces_the_real_file_behind_a_symlink() {
    let (dir, real) = installed("0.0.1");
    let links = dir.path().join("links");
    fs::create_dir(&links).unwrap();
    let link = links.join("potluck");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let server = Server::start(release(&fake_binary("9.9.9")));
    update_dir(&server.url(), &link, None).unwrap();
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read(&real).unwrap(), fake_binary("9.9.9"));
    assert_eq!(names_in(&links), ["potluck"]);
    assert_eq!(names_in(real.parent().unwrap()), ["potluck"]);
}

#[test]
fn refuses_a_bad_checksum_and_changes_nothing() {
    let (_dir, target) = installed("0.0.1");
    let before = snapshot(&target);
    let asset = asset_name().unwrap();
    // SHA256SUMS describes a different binary than the one served.
    let files = with_file(
        release(&fake_binary("9.9.9")),
        SUMS_FILE,
        sums_line(asset, &fake_binary("6.6.6")).into_bytes(),
    );
    let server = Server::start(files);
    let err = update_dir(&server.url(), &target, None).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains(&format!("{asset} has SHA-256 ")), "{msg}");
    assert!(msg.contains("refusing it, nothing was changed"), "{msg}");
    assert_eq!(snapshot(&target), before);
    assert_eq!(names_in(target.parent().unwrap()), ["potluck"]);
}

#[test]
fn missing_asset_fails_clearly() {
    let (_dir, target) = installed("0.0.1");
    let before = snapshot(&target);
    let asset = asset_name().unwrap();
    let server = Server::start(without_file(release(&fake_binary("9.9.9")), asset));
    let err = format!(
        "{:#}",
        update_dir(&server.url(), &target, None).unwrap_err()
    );
    assert_eq!(
        err,
        format!("downloading {}{asset}: HTTP 404", server.url())
    );
    assert_eq!(snapshot(&target), before);
    assert_eq!(names_in(target.parent().unwrap()), ["potluck"]);
}

#[test]
fn missing_sums_file_fails_clearly() {
    let (_dir, target) = installed("0.0.1");
    let before = snapshot(&target);
    let server = Server::start(without_file(release(&fake_binary("9.9.9")), SUMS_FILE));
    let err = format!(
        "{:#}",
        update_dir(&server.url(), &target, None).unwrap_err()
    );
    assert_eq!(
        err,
        format!("downloading {}{SUMS_FILE}: HTTP 404", server.url())
    );
    assert_eq!(snapshot(&target), before);
}

#[test]
fn missing_sums_line_fails_clearly() {
    let (_dir, target) = installed("0.0.1");
    let before = snapshot(&target);
    let asset = asset_name().unwrap();
    let files = with_file(
        release(&fake_binary("9.9.9")),
        SUMS_FILE,
        sums_line("potluck-riscv64gc-unknown-linux-musl", b"x").into_bytes(),
    );
    let server = Server::start(files);
    let err = format!(
        "{:#}",
        update_dir(&server.url(), &target, None).unwrap_err()
    );
    assert_eq!(
        err,
        format!(
            "{}{SUMS_FILE}: {SUMS_FILE} has no line for {asset}",
            server.url()
        )
    );
    assert_eq!(snapshot(&target), before);
    // It stops before downloading the binary.
    assert_eq!(server.requests(), [format!("/rel/{SUMS_FILE}")]);
}

#[test]
fn refuses_another_version_than_requested() {
    let (_dir, target) = installed("0.0.1");
    let before = snapshot(&target);
    let server = Server::start(release(&fake_binary("9.9.9")));
    let err = format!(
        "{:#}",
        update_dir(&server.url(), &target, Some("1.2.3")).unwrap_err()
    );
    assert!(
        err.contains("is potluck 9.9.9, not the requested 1.2.3"),
        "{err}"
    );
    assert_eq!(snapshot(&target), before);
    assert_eq!(names_in(target.parent().unwrap()), ["potluck"]);
}

#[test]
fn refuses_a_binary_that_does_not_run() {
    let (_dir, target) = installed("0.0.1");
    let before = snapshot(&target);
    let server = Server::start(release(b"\x7fELF this is not a program"));
    let err = format!(
        "{:#}",
        update_dir(&server.url(), &target, None).unwrap_err()
    );
    assert!(err.contains("doesn't run on this machine"), "{err}");
    assert!(err.ends_with("nothing was changed"), "{err}");
    assert_eq!(snapshot(&target), before);
    assert_eq!(names_in(target.parent().unwrap()), ["potluck"]);
}

#[test]
fn an_identical_binary_is_left_alone() {
    let (_dir, target) = installed("9.9.9");
    let before = snapshot(&target);
    let server = Server::start(release(&fake_binary("9.9.9")));
    let msg = update_dir(&server.url(), &target, None).unwrap();
    assert!(msg.ends_with("is already the release binary (potluck 9.9.9); nothing was changed."));
    assert_eq!(snapshot(&target), before);
    // A lower version, asked for without allowing it: refused before downloading anything.
    let asked = server.requests().len();
    let err = update_dir(&server.url(), &target, Some("1.0.0")).unwrap_err();
    assert_eq!(
        format!("{err:#}"),
        "potluck 1.0.0 is lower than the potluck 9.9.9 here; a lower version is installed only \
         when asked for directly; nothing was changed"
    );
    assert_eq!(server.requests().len(), asked);
    // Allowed, it gets as far as finding the release is 9.9.9.
    let source = Source::Directory(server.url());
    let lower = Plan {
        allow_lower: true,
        ..plan(&source, &target, Some("1.0.0"))
    };
    let err = update(&lower, false).unwrap_err();
    assert!(
        format!("{err:#}").contains("not the requested 1.0.0"),
        "{err:#}"
    );
    assert_eq!(snapshot(&target), before);
}

/// A daemon asked to update whose binary file is already the release (written by install.sh with
/// POTLUCK_NO_RESTART=1, or by an update whose restart failed) still runs its old version: it must
/// restart, or it never runs the release it was asked for.
#[test]
fn an_identical_binary_restarts_a_daemon_running_another_version() {
    // The daemon calling for itself passes its own version; the file and the release are 9.9.9.
    assert_ne!(crate::VERSION, "9.9.9");
    let restart = FakeRestart::new();
    let run = || restart.run();
    let (_dir, target) = installed("9.9.9");
    let before = snapshot(&target);
    let server = Server::start(release(&fake_binary("9.9.9")));
    let source = Source::Directory(server.url());
    let daemon = Plan {
        daemon: Some(crate::VERSION),
        ..plan(&source, &target, Some("9.9.9"))
    };
    let msg = update_with(&daemon, Some(&run)).unwrap();
    assert_eq!(restart.calls(), 1, "{msg}");
    assert!(
        msg.ends_with(&format!(
            "nothing was changed. The daemon runs potluck {}. Restarted the daemon (`a fake \
             restart`).",
            crate::VERSION
        )),
        "{msg}"
    );
    assert_eq!(snapshot(&target), before);
    // A file at the version the daemon runs: nothing to restart.
    let (_dir, current) = installed(crate::VERSION);
    let server = Server::start(release(&fake_binary(crate::VERSION)));
    let source = Source::Directory(server.url());
    let same = Plan {
        daemon: Some(crate::VERSION),
        ..plan(&source, &current, None)
    };
    let msg = update_with(&same, Some(&run)).unwrap();
    assert_eq!(restart.calls(), 1, "{msg}");
    assert!(msg.ends_with("nothing was changed."), "{msg}");
}

/// `potluck self-update --here` runs from the new binary while the daemon may still run the old
/// one: the CLI passes the version the daemon reported, and that decides the restart.
#[test]
fn an_identical_binary_restarts_only_an_older_daemon() {
    let restart = FakeRestart::new();
    let run = || restart.run();
    let (_dir, target) = installed("9.9.9");
    let server = Server::start(release(&fake_binary("9.9.9")));
    let source = Source::Directory(server.url());
    let with_daemon = |daemon| Plan {
        daemon,
        ..plan(&source, &target, None)
    };
    // No daemon answered: nothing to restart.
    let msg = update_with(&with_daemon(None), Some(&run)).unwrap();
    assert_eq!(restart.calls(), 0, "{msg}");
    assert!(msg.ends_with("nothing was changed."), "{msg}");
    // The daemon runs the file's version already.
    let msg = update_with(&with_daemon(Some("9.9.9")), Some(&run)).unwrap();
    assert_eq!(restart.calls(), 0, "{msg}");
    // An older daemon is restarted.
    let msg = update_with(&with_daemon(Some("0.0.1-old")), Some(&run)).unwrap();
    assert_eq!(restart.calls(), 1, "{msg}");
    assert!(
        msg.ends_with(
            "The daemon runs potluck 0.0.1-old. Restarted the daemon (`a fake restart`)."
        ),
        "{msg}"
    );
    // A newer daemon is left alone: restarting it would take it down to the file's version.
    let msg = update_with(&with_daemon(Some("10.0.0")), Some(&run)).unwrap();
    assert_eq!(restart.calls(), 1, "{msg}");
    assert!(
        msg.ends_with("The daemon runs potluck 10.0.0, which is not older; it was left alone."),
        "{msg}"
    );
}

/// Toward the latest release, an update never goes below the version on disk, this process's, or
/// the daemon's: a fix released for an older version line must not take machines down.
#[test]
fn latest_never_goes_lower() {
    // GitHub: the tag says 1.0.0, so nothing is downloaded.
    let (_dir, target) = installed("9.9.9");
    let before = snapshot(&target);
    let server = Server::github(
        vec![("1.0.0", release(&fake_binary("1.0.0")))],
        Some("1.0.0"),
    );
    let msg = update(&plan(&server.releases(), &target, None), false).unwrap();
    assert_eq!(
        msg,
        "The latest release is potluck 1.0.0, lower than the potluck 9.9.9 here; nothing was \
         changed."
    );
    assert_eq!(
        server.requests(),
        [format!("/{GITHUB_REPO}/releases/latest")]
    );
    assert_eq!(snapshot(&target), before);
    // A directory: the downloaded binary says 1.0.0, so it is dropped before the rename.
    let server = Server::start(release(&fake_binary("1.0.0")));
    let msg = update_dir(&server.url(), &target, None).unwrap();
    assert!(
        msg.starts_with("The latest release is potluck 1.0.0, lower"),
        "{msg}"
    );
    assert_eq!(snapshot(&target), before);
    assert_eq!(names_in(target.parent().unwrap()), ["potluck"]);
    // The daemon's version counts too: here the file is old, but the daemon runs 10.0.0.
    let (_dir, old) = installed("0.0.1");
    let server = Server::github(
        vec![("9.9.9", release(&fake_binary("9.9.9")))],
        Some("9.9.9"),
    );
    let source = server.releases();
    let newer_daemon = Plan {
        daemon: Some("10.0.0"),
        ..plan(&source, &old, None)
    };
    let msg = update(&newer_daemon, false).unwrap();
    assert!(
        msg.ends_with("lower than the potluck 10.0.0 here; nothing was changed."),
        "{msg}"
    );
    assert_eq!(fs::read(&old).unwrap(), fake_binary("0.0.1"));
}

/// A requested lower version is refused unless allowed: the mesh's version gating only moves up,
/// a user asking directly may go down.
#[test]
fn a_requested_lower_version_needs_allow_lower() {
    let (_dir, target) = installed("9.9.9");
    let server = Server::github(
        vec![("1.0.0", release(&fake_binary("1.0.0")))],
        Some("1.0.0"),
    );
    let source = server.releases();
    let err = update(&plan(&source, &target, Some("1.0.0")), false).unwrap_err();
    assert!(format!("{err:#}").contains("a lower version is installed only when asked"));
    assert!(server.requests().is_empty());
    assert_eq!(fs::read(&target).unwrap(), fake_binary("9.9.9"));
    let down = Plan {
        allow_lower: true,
        ..plan(&source, &target, Some("1.0.0"))
    };
    let msg = update(&down, false).unwrap();
    assert!(msg.contains("from 9.9.9 to 1.0.0"), "{msg}");
    assert_eq!(fs::read(&target).unwrap(), fake_binary("1.0.0"));
}

/// "Latest" is resolved to its tag first, so SHA256SUMS and the binary come from one release even
/// if another is published between the two downloads.
#[test]
fn latest_is_resolved_to_its_tag_before_downloading() {
    let (_dir, target) = installed("0.0.1");
    let asset = asset_name().unwrap();
    // `latest/download/...` would serve a newer binary beside the older SHA256SUMS: never asked.
    let mixed = with_file(release(&fake_binary("9.9.8")), asset, fake_binary("9.9.9"));
    let mut files: Files = Vec::new();
    for (name, data) in mixed {
        files.push((
            format!("/{GITHUB_REPO}/releases/latest/download/{name}"),
            data,
        ));
    }
    for (name, data) in release(&fake_binary("9.9.8")) {
        files.push((
            format!("/{GITHUB_REPO}/releases/download/v9.9.8/{name}"),
            data,
        ));
    }
    let latest = format!("/{GITHUB_REPO}/releases/latest");
    for location in [
        format!("{{base}}/{GITHUB_REPO}/releases/tag/v9.9.8"),
        format!("/{GITHUB_REPO}/releases/tag/v9.9.8?from=test"),
    ] {
        fs::write(&target, fake_binary("0.0.1")).unwrap();
        let server = Server::serving(files.clone(), vec![(latest.clone(), location)]);
        let msg = update(&plan(&server.releases(), &target, None), false).unwrap();
        assert!(msg.contains("from 0.0.1 to 9.9.8"), "{msg}");
        let root = format!("/{GITHUB_REPO}/releases");
        assert_eq!(
            server.requests(),
            [
                format!("{root}/latest"),
                format!("{root}/download/v9.9.8/{SUMS_FILE}"),
                format!("{root}/download/v9.9.8/{asset}"),
            ]
        );
    }
}

#[test]
fn a_release_whose_binary_is_not_its_tag_is_refused() {
    let (_dir, target) = installed("0.0.1");
    let server = Server::github(
        vec![("9.9.9", release(&fake_binary("9.9.7")))],
        Some("9.9.9"),
    );
    let err = update(&plan(&server.releases(), &target, None), false).unwrap_err();
    assert!(
        format!("{err:#}").contains("is potluck 9.9.7, not 9.9.9 as its tag says"),
        "{err:#}"
    );
    assert_eq!(fs::read(&target).unwrap(), fake_binary("0.0.1"));
}

/// Build metadata (`+...`) doesn't make another version: a binary reporting `9.9.9+abc` is the
/// release tagged 9.9.9, and the 9.9.9 asked for, both when it is downloaded and when it is
/// already the file here.
#[test]
fn a_binary_with_build_metadata_is_its_version() {
    let (_dir, target) = installed("0.0.1");
    let new = fake_binary("9.9.9+abc");
    let server = Server::github(vec![("9.9.9", release(&new))], Some("9.9.9"));
    let msg = update(&plan(&server.releases(), &target, None), false).unwrap();
    assert!(msg.contains("to 9.9.9+abc"), "{msg}");
    assert_eq!(fs::read(&target).unwrap(), new);
    let msg = update(&plan(&server.releases(), &target, Some("9.9.9")), false).unwrap();
    assert!(msg.contains("is already the release binary"), "{msg}");

    let (_dir, target) = installed("0.0.1");
    let server = Server::start(release(&new));
    let msg = update_dir(&server.url(), &target, Some("9.9.9")).unwrap();
    assert!(msg.contains("to 9.9.9+abc"), "{msg}");
}

#[test]
fn no_latest_release_fails_clearly() {
    let (_dir, target) = installed("0.0.1");
    // No redirect at all: GitHub answers 404 for a repository without releases.
    let server = Server::github(vec![], None);
    let err = format!(
        "{:#}",
        update(&plan(&server.releases(), &target, None), false).unwrap_err()
    );
    assert!(
        err.starts_with("finding the latest release: ") && err.contains("HTTP 404"),
        "{err}"
    );
    // A redirect that isn't to a tag.
    let latest = format!("/{GITHUB_REPO}/releases/latest");
    let server = Server::serving(vec![], vec![(latest, "/elsewhere".into())]);
    let err = format!(
        "{:#}",
        update(&plan(&server.releases(), &target, None), false).unwrap_err()
    );
    assert!(err.contains("not a redirect to a release tag"), "{err}");
    assert_eq!(fs::read(&target).unwrap(), fake_binary("0.0.1"));
}

/// Without a tag (a release directory), a checksum mismatch may be a release published between the
/// two downloads: the error says so. From a tag it can't be, and the error doesn't.
#[test]
fn a_checksum_mismatch_names_the_race_only_without_a_tag() {
    let (_dir, target) = installed("0.0.1");
    let asset = asset_name().unwrap();
    let bad = with_file(
        release(&fake_binary("9.9.9")),
        SUMS_FILE,
        sums_line(asset, b"another binary").into_bytes(),
    );
    let race = "(if a release was published there between the two downloads, run it again)";
    let server = Server::start(bad.clone());
    let err = format!(
        "{:#}",
        update_dir(&server.url(), &target, None).unwrap_err()
    );
    assert!(err.ends_with(race), "{err}");
    let server = Server::github(vec![("9.9.9", bad)], Some("9.9.9"));
    let err = format!(
        "{:#}",
        update(&plan(&server.releases(), &target, None), false).unwrap_err()
    );
    assert!(err.contains("refusing it, nothing was changed"), "{err}");
    assert!(!err.contains(race), "{err}");
}

#[test]
fn compares_versions_like_semver() {
    use std::cmp::Ordering::*;
    let cases = [
        ("0.1.0", "0.1.0", Equal),
        ("0.10.0", "0.9.0", Greater),
        ("v1.2.3", "1.2.3", Equal),
        ("1.2", "1.2.0", Equal),
        ("1.2.3-rc.1", "1.2.3", Less),
        ("1.2.3-rc.2", "1.2.3-rc.10", Less),
        ("1.2.3-alpha", "1.2.3-1", Greater),
        ("1.2.3-rc", "1.2.3-rc.1", Less),
        ("1.2.3+b5", "1.2.3", Equal),
        ("0.1.1-e2e", "0.1.0", Greater),
    ];
    for (a, b, want) in cases {
        assert_eq!(compare_versions(a, b), Some(want), "{a} vs {b}");
        assert_eq!(compare_versions(b, a), Some(want.reverse()), "{b} vs {a}");
    }
    assert_eq!(compare_versions("latest", "1.0.0"), None);
    assert_eq!(compare_versions("1..0", "1.0.0"), None);
    assert_eq!(
        highest([Some("0.2.0"), None, Some("junk"), Some("0.10.0")]),
        Some("0.10.0")
    );
    assert_eq!(highest([None, Some("junk")]), None);
}

/// The release workflow marks a release latest only when no published release has a higher
/// version, so a fix released for an older version line never becomes latest.
#[test]
fn the_workflow_marks_only_the_highest_release_latest() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let is_latest = |tag: &str, published: &[&str]| -> String {
        let mut child = Command::new("sh")
            .arg(root.join(".github/is-latest.sh"))
            .arg(tag)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = published.join("\n");
        input.push('\n');
        // The script may answer without reading its input (a prerelease is never latest), so the
        // write can meet a closed pipe.
        let _ = child.stdin.take().unwrap().write_all(input.as_bytes());
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    let cases: [(&str, &[&str], &str); 10] = [
        ("v0.3.0", &["v0.2.0", "v0.1.0"], "true"),
        ("v0.2.1", &["v0.3.0", "v0.2.0"], "false"),
        ("v0.10.0", &["v0.9.0"], "true"),
        ("v0.9.1", &["v0.10.0"], "false"),
        ("v1.0.0", &[], "true"),
        ("v1.0.0", &[""], "true"),
        ("v1.0.0", &["v1.0.0"], "true"),
        // Prereleases are never latest, and published prereleases or other tags don't count.
        ("v1.0.0-rc.1", &[], "false"),
        ("v1.0.0", &["v2.0.0-rc.1", "nightly", "v1.0.0+old"], "true"),
        ("v1.0.0", &["v1.0.1+b2"], "false"),
    ];
    for (tag, published, want) in cases {
        assert_eq!(is_latest(tag, published), want, "{tag} after {published:?}");
    }
    // The publish step uses it for --latest.
    let workflow = fs::read_to_string(root.join(".github/workflows/release.yml")).unwrap();
    assert!(
        workflow.contains(
            r#"latest=$(printf '%s\n' "$published" | sh ../.github/is-latest.sh "$TAG")"#
        )
    );
    assert!(workflow.contains(r#"gh release edit "$TAG" --draft=false --latest="$latest""#));
}

/// Under Rosetta an x86_64 potluck takes the arm64 binary, as install.sh does.
#[test]
fn rosetta_takes_the_arm64_binary() {
    let asset = |os, arch, translated| asset_for(os, host_arch(os, arch, translated));
    assert_eq!(
        asset("macos", "x86_64", true),
        Some("potluck-aarch64-apple-darwin")
    );
    assert_eq!(
        asset("macos", "x86_64", false),
        Some("potluck-x86_64-apple-darwin")
    );
    assert_eq!(
        asset("macos", "aarch64", false),
        Some("potluck-aarch64-apple-darwin")
    );
    assert_eq!(
        asset("linux", "x86_64", true),
        Some("potluck-x86_64-unknown-linux-musl")
    );
    // This test binary runs natively (it isn't built for x86_64 macOS here).
    assert!(!rosetta_translated());
    let native = asset_for(std::env::consts::OS, std::env::consts::ARCH);
    assert_eq!(asset_name().ok(), native);
}

#[test]
fn an_unwritable_directory_fails_and_changes_nothing() {
    if unsafe { libc::geteuid() } == 0 {
        return; // root writes anywhere
    }
    let (_dir, target) = installed("0.0.1");
    let bin = target.parent().unwrap().to_path_buf();
    let before = snapshot(&target);
    fs::set_permissions(&bin, fs::Permissions::from_mode(0o555)).unwrap();
    let server = Server::start(release(&fake_binary("9.9.9")));
    let result = update_dir(&server.url(), &target, None);
    fs::set_permissions(&bin, fs::Permissions::from_mode(0o755)).unwrap();
    let err = format!("{:#}", result.unwrap_err());
    assert!(err.contains("can't write the new binary beside"), "{err}");
    assert_eq!(snapshot(&target), before);
    assert_eq!(names_in(&bin), ["potluck"]);
}

#[test]
fn a_missing_target_fails_before_downloading() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(release(&fake_binary("9.9.9")));
    let err = update_dir(&server.url(), &dir.path().join("nope"), None).unwrap_err();
    assert!(format!("{err:#}").contains("finding the binary to replace"));
    assert!(server.requests().is_empty());
}

#[test]
fn an_unreachable_server_fails_clearly() {
    let (_dir, target) = installed("0.0.1");
    // A port nothing listens on: bind one, then close it.
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let err = format!("{:#}", update_dir(&url, &target, None).unwrap_err());
    assert!(
        err.starts_with(&format!("downloading {url}/{SUMS_FILE}: ")),
        "{err}"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn clears_the_quarantine_attribute() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("potluck");
    fs::write(&file, b"x").unwrap();
    let set = Command::new("xattr")
        .args(["-w", "com.apple.quarantine", "0081;00000000;test;"])
        .arg(&file)
        .status()
        .unwrap();
    assert!(set.success());
    assert!(has_quarantine(&file));
    clear_quarantine(&file).unwrap();
    assert!(!has_quarantine(&file));
    // Clearing an attribute that isn't there is fine.
    clear_quarantine(&file).unwrap();
}

#[test]
fn reads_sha256sum_output() {
    let a = "a".repeat(64);
    let b = "B".repeat(64);
    let sums = format!("{a}  potluck-x\r\n{b} *potluck-y\n\nnot a line\n");
    assert_eq!(expected_sha256(&sums, "potluck-x").unwrap(), [0xaa; 32]);
    assert_eq!(expected_sha256(&sums, "potluck-y").unwrap(), [0xbb; 32]);
    let err = expected_sha256(&sums, "potluck").unwrap_err().to_string();
    assert_eq!(err, "SHA256SUMS has no line for potluck");
    let twice = format!("{a}  potluck-x\n{b}  potluck-x\n");
    assert!(expected_sha256(&twice, "potluck-x").is_err());
    let same_twice = format!("{a}  potluck-x\n{a}  potluck-x\n");
    assert!(expected_sha256(&same_twice, "potluck-x").is_ok());
    let signed = format!("+{}  potluck-x\n", "a".repeat(63));
    assert!(expected_sha256(&signed, "potluck-x").is_err());
    let short = "abc  potluck-x\n";
    assert!(
        expected_sha256(short, "potluck-x")
            .unwrap_err()
            .to_string()
            .contains("malformed hash")
    );
}

#[test]
fn names_the_assets() {
    assert_eq!(
        asset_for("macos", "aarch64"),
        Some("potluck-aarch64-apple-darwin")
    );
    assert_eq!(
        asset_for("macos", "x86_64"),
        Some("potluck-x86_64-apple-darwin")
    );
    assert_eq!(
        asset_for("linux", "x86_64"),
        Some("potluck-x86_64-unknown-linux-musl")
    );
    assert_eq!(
        asset_for("linux", "aarch64"),
        Some("potluck-aarch64-unknown-linux-musl")
    );
    assert_eq!(asset_for("freebsd", "x86_64"), None);
    assert_eq!(asset_for("linux", "riscv64"), None);
    assert!(asset_name().is_ok());
}

#[test]
fn builds_release_urls() {
    let gh = format!("https://github.com/{GITHUB_REPO}/releases");
    assert_eq!(Source::from_env(None, None), Source::Releases(gh.clone()));
    assert_eq!(Source::from_env(Some(" "), Some("")), Source::Releases(gh));
    assert_eq!(
        Source::from_env(None, Some("http://127.0.0.1:8000/")),
        Source::Releases(format!("http://127.0.0.1:8000/{GITHUB_REPO}/releases"))
    );
    assert_eq!(
        Source::from_env(Some("http://127.0.0.1:8000/rel/"), Some("http://x")),
        Source::Directory("http://127.0.0.1:8000/rel".into())
    );
    assert_eq!(percent_decode("v1.0.0%2Bb5"), "v1.0.0+b5");
    assert_eq!(percent_decode("a%zz%2"), "a%zz%2");
    assert_eq!(normalize_version("v0.2.0").unwrap(), "0.2.0");
    assert_eq!(normalize_version("0.2.0-rc.1+b5").unwrap(), "0.2.0-rc.1+b5");
    assert!(normalize_version("latest").is_err());
    assert!(normalize_version("0.2/../x").is_err());
    assert!(normalize_version("").is_err());
}

// install.sh

fn install_sh() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("install.sh")
}

/// The POSIX shells to run install.sh with: `sh`, and `dash` when present (stricter than bash).
fn shells() -> Vec<&'static str> {
    let mut shells = vec!["sh"];
    let has_dash = Command::new("sh")
        .args(["-c", "command -v dash"])
        .stdout(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if has_dash {
        shells.push("dash");
    }
    shells
}

struct Install {
    ok: bool,
    stdout: String,
    stderr: String,
}

fn run_install(shell: &str, url: &str, dir: &Path, args: &[&str]) -> Install {
    run_install_with(shell, url, dir, args, None)
}

/// `path`, when given, replaces PATH.
fn run_install_with(
    shell: &str,
    url: &str,
    dir: &Path,
    args: &[&str],
    path: Option<&str>,
) -> Install {
    let mut env = vec![("POTLUCK_RELEASES_URL", url.to_string())];
    if let Some(path) = path {
        env.push(("PATH", path.to_string()));
    }
    run_install_env(shell, dir, args, &env)
}

/// install.sh into `dir`, with `env` on top of this process's environment.
fn run_install_env(shell: &str, dir: &Path, args: &[&str], env: &[(&str, String)]) -> Install {
    let out = Command::new(shell)
        .arg(install_sh())
        .args(args)
        .env("POTLUCK_INSTALL_DIR", dir)
        .env("POTLUCK_NO_RESTART", "1")
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::null())
        .output()
        .unwrap();
    Install {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

#[test]
fn install_sh_installs_from_a_local_release() {
    let server = Server::start(release(&fake_binary("9.9.9")));
    for shell in shells() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("prefix/bin");
        let run = run_install(shell, &server.url(), &dir, &[]);
        assert!(run.ok, "{shell}: {}{}", run.stdout, run.stderr);
        let bin = dir.join("potluck");
        assert_eq!(fs::read(&bin).unwrap(), fake_binary("9.9.9"), "{shell}");
        assert_eq!(fs::metadata(&bin).unwrap().mode() & 0o7777, 0o755);
        assert_eq!(names_in(&dir), ["potluck"], "{shell}");
        assert!(
            run.stdout
                .contains(&format!("Installed potluck 9.9.9 to {}", bin.display())),
            "{shell}: {}",
            run.stdout
        );
        #[cfg(target_os = "macos")]
        assert!(!has_quarantine(&bin));
        // Again with a version, over the installed one.
        fs::write(&bin, b"old").unwrap();
        let run = run_install(shell, &server.url(), &dir, &["v9.9.9"]);
        assert!(run.ok, "{shell}: {}{}", run.stdout, run.stderr);
        assert_eq!(fs::read(&bin).unwrap(), fake_binary("9.9.9"), "{shell}");
        assert_eq!(names_in(&dir), ["potluck"], "{shell}");
    }
}

/// As for self-update, a binary reporting `9.9.9+abc` is the 9.9.9 asked for.
#[test]
fn install_sh_takes_a_binary_with_build_metadata_for_its_version() {
    let server = Server::start(release(&fake_binary("9.9.9+abc")));
    for shell in shells() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("bin");
        let run = run_install(shell, &server.url(), &dir, &["9.9.9"]);
        assert!(run.ok, "{shell}: {}{}", run.stdout, run.stderr);
        assert!(
            run.stdout.contains("Installed potluck 9.9.9+abc"),
            "{shell}"
        );
        // Another version is still refused.
        let run = run_install(shell, &server.url(), &dir, &["9.9.8"]);
        assert!(!run.ok, "{shell}");
        assert!(
            run.stderr.contains("not the requested 9.9.8"),
            "{shell}: {}",
            run.stderr
        );
    }
}

/// A `potluck` in the install directory that is a symlink (to a copy kept elsewhere) keeps being
/// one: install.sh replaces the file it points to, as `potluck self-update` does. It used to
/// replace the link with a file, leaving the target, which a service runs, at the old version.
#[test]
fn install_sh_replaces_a_symlinked_binary_at_its_target() {
    let server = Server::start(release(&fake_binary("9.9.9")));
    for shell in shells() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("bin");
        let real = tmp.path().join("opt/potluck/potluck");
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(real.parent().unwrap()).unwrap();
        fs::write(&real, b"old").unwrap();
        // Relative, and through a second link, as package managers make them.
        std::os::unix::fs::symlink("potluck/potluck", tmp.path().join("opt/current")).unwrap();
        std::os::unix::fs::symlink("../opt/current", dir.join("potluck")).unwrap();
        let run = run_install(shell, &server.url(), &dir, &[]);
        assert!(run.ok, "{shell}: {}{}", run.stdout, run.stderr);
        assert!(
            fs::symlink_metadata(dir.join("potluck"))
                .unwrap()
                .file_type()
                .is_symlink(),
            "{shell}: the link stays"
        );
        assert_eq!(fs::read(&real).unwrap(), fake_binary("9.9.9"), "{shell}");
        assert_eq!(names_in(&dir), ["potluck"], "{shell}");
        assert_eq!(names_in(real.parent().unwrap()), ["potluck"], "{shell}");
        assert!(
            run.stdout.contains(&format!(
                "Installed potluck 9.9.9 to {} ({}/potluck links to it).",
                real.canonicalize().unwrap().display(),
                dir.display()
            )),
            "{shell}: {}",
            run.stdout
        );
    }
}

/// install.sh from a shell without XDG_RUNTIME_DIR (`su`, cron) still finds the running service
/// through the user manager's own /run/user/UID, as potluck does. It used to see no service and
/// print the first-install steps. `systemctl` is a stand-in here that fails like the real one
/// without a bus.
#[cfg(target_os = "linux")]
#[test]
fn install_sh_finds_the_service_without_xdg_runtime_dir() {
    use std::os::unix::fs::PermissionsExt;
    let server = Server::start(release(&fake_binary("9.9.9")));
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("prefix/bin");
    let bin = tmp.path().join("fake");
    fs::create_dir_all(&bin).unwrap();
    let unit = format!("potluck-test-xdg-{}.service", std::process::id());
    let systemctl = format!(
        r#"#!/bin/sh
if [ -z "${{XDG_RUNTIME_DIR:-}}" ] && [ -z "${{DBUS_SESSION_BUS_ADDRESS:-}}" ]; then
    echo 'Failed to connect to user scope bus via local transport' >&2
    exit 1
fi
case "$*" in
    *LoadState*) echo loaded ;;
    *ExecStart*) echo '{{ path={dir}/potluck ; argv[]={dir}/potluck daemon ; ignore_errors=no }}' ;;
esac
exit 0
"#,
        dir = dir.display()
    );
    fs::write(bin.join("systemctl"), systemctl).unwrap();
    fs::set_permissions(bin.join("systemctl"), fs::Permissions::from_mode(0o755)).unwrap();
    let out = Command::new("sh")
        .arg(install_sh())
        .env("POTLUCK_INSTALL_DIR", &dir)
        .env("POTLUCK_NO_RESTART", "1")
        .env("POTLUCK_RELEASES_URL", server.url())
        .env("POTLUCK_SERVICE_NAME", &unit)
        .env(
            "PATH",
            format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
        )
        .env_remove("XDG_RUNTIME_DIR")
        .env_remove("DBUS_SESSION_BUS_ADDRESS")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains(&format!(
            "The potluck service still runs the old binary; restart it with: {}/potluck restart",
            dir.display()
        )),
        "{stdout}"
    );
    assert!(!stdout.contains("Next steps"), "{stdout}");
}

#[test]
fn install_sh_refuses_bad_releases() {
    let asset = asset_name().unwrap();
    let good = release(&fake_binary("9.9.9"));
    let cases: Vec<(&str, Files, Vec<&str>, &str)> = vec![
        (
            "bad checksum",
            with_file(
                good.clone(),
                SUMS_FILE,
                sums_line(asset, b"something else").into_bytes(),
            ),
            vec![],
            "refusing it, nothing was installed (if a release was published there between the \
             two downloads, run it again)",
        ),
        (
            "no sums line",
            with_file(good.clone(), SUMS_FILE, b"\n".to_vec()),
            vec![],
            "has no line for",
        ),
        (
            "no asset",
            without_file(good.clone(), asset),
            vec![],
            "couldn't download",
        ),
        (
            "no sums file",
            without_file(good.clone(), SUMS_FILE),
            vec![],
            "couldn't download",
        ),
        (
            "other version",
            good.clone(),
            vec!["1.2.3"],
            "not the requested 1.2.3",
        ),
        (
            "not a version",
            good.clone(),
            vec!["--help"],
            "usage: install.sh [VERSION]",
        ),
        (
            "not a program",
            release(b"\x7fELF this is not a program"),
            vec![],
            "doesn't run on this machine; nothing was installed",
        ),
    ];
    for (what, files, args, expect) in cases {
        let server = Server::start(files);
        for shell in shells() {
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path().join("bin");
            fs::create_dir(&dir).unwrap();
            fs::write(dir.join("potluck"), b"old").unwrap();
            let run = run_install(shell, &server.url(), &dir, &args);
            assert!(!run.ok, "{what} ({shell}): {}", run.stdout);
            assert!(
                run.stderr.contains(expect),
                "{what} ({shell}): wanted {expect:?} in {}",
                run.stderr
            );
            assert_eq!(fs::read(dir.join("potluck")).unwrap(), b"old", "{what}");
            assert_eq!(names_in(&dir), ["potluck"], "{what} ({shell})");
        }
    }
}

/// GNU sha256sum and shasum print `\<hash>` for a file name with a backslash in it.
#[test]
fn install_sh_checks_a_download_in_a_temporary_directory_with_a_backslash() {
    let server = Server::start(release(&fake_binary("9.9.9")));
    for shell in shells() {
        let tmp = tempfile::tempdir().unwrap();
        let tmpdir = tmp.path().join("back\\slash");
        fs::create_dir(&tmpdir).unwrap();
        let dir = tmp.path().join("bin");
        let out = Command::new(shell)
            .arg(install_sh())
            .env("TMPDIR", &tmpdir)
            .env("POTLUCK_RELEASES_URL", server.url())
            .env("POTLUCK_INSTALL_DIR", &dir)
            .env("POTLUCK_NO_RESTART", "1")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{shell}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(fs::read(dir.join("potluck")).unwrap(), fake_binary("9.9.9"));
        assert!(names_in(&tmpdir).is_empty(), "{shell}");
    }
}

/// A PATH holding everything this process's PATH does, except curl: install.sh then uses wget.
fn path_without_curl(farm: &Path) -> String {
    fs::create_dir_all(farm).unwrap();
    for dir in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let link = farm.join(entry.file_name());
            if entry.file_name() != "curl" && !link.exists() {
                let _ = std::os::unix::fs::symlink(entry.path(), link);
            }
        }
    }
    farm.display().to_string()
}

fn has_wget() -> bool {
    Command::new("sh")
        .args(["-c", "command -v wget"])
        .stdout(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// From GitHub, install.sh resolves "latest" to its tag first (with curl, and with wget), so
/// SHA256SUMS and the binary come from one release.
#[test]
fn install_sh_resolves_latest_to_its_tag() {
    let asset = asset_name().unwrap();
    let root = format!("/{GITHUB_REPO}/releases");
    let mut files: Files = Vec::new();
    // `latest/download/...` would mix a newer binary with the older SHA256SUMS: never asked.
    let mixed = with_file(release(&fake_binary("9.9.8")), asset, fake_binary("9.9.9"));
    for (name, data) in mixed {
        files.push((format!("{root}/latest/download/{name}"), data));
    }
    for (name, data) in release(&fake_binary("9.9.8")) {
        files.push((format!("{root}/download/v9.9.8/{name}"), data));
    }
    let latest = (
        format!("{root}/latest"),
        format!("{{base}}{root}/tag/v9.9.8"),
    );
    let tmp = tempfile::tempdir().unwrap();
    let mut runs: Vec<(String, Vec<(&str, String)>)> = shells()
        .into_iter()
        .map(|sh| (format!("{sh} with curl"), vec![]))
        .collect();
    if has_wget() {
        let path = path_without_curl(&tmp.path().join("no-curl"));
        runs.push(("sh with wget".into(), vec![("PATH", path)]));
    }
    for (what, mut env) in runs {
        let server = Server::serving(files.clone(), vec![latest.clone()]);
        env.push(("POTLUCK_GITHUB_URL", server.github_url()));
        let dir = tmp.path().join(what.replace(' ', "-"));
        let run = run_install_env(what.split(' ').next().unwrap(), &dir, &[], &env);
        assert!(run.ok, "{what}: {}{}", run.stdout, run.stderr);
        assert_eq!(
            fs::read(dir.join("potluck")).unwrap(),
            fake_binary("9.9.8"),
            "{what}"
        );
        assert_eq!(
            server.requests(),
            [
                format!("{root}/latest"),
                format!("{root}/download/v9.9.8/{SUMS_FILE}"),
                format!("{root}/download/v9.9.8/{asset}"),
            ],
            "{what}"
        );
    }
}

#[test]
fn install_sh_from_github_checks_the_tag_and_names_failures() {
    let asset = asset_name().unwrap();
    let root = format!("/{GITHUB_REPO}/releases");
    let tagged = |version: &str, binary: &[u8]| -> Files {
        release(binary)
            .into_iter()
            .map(|(name, data)| (format!("{root}/download/v{version}/{name}"), data))
            .collect()
    };
    let latest = |v: &str| vec![(format!("{root}/latest"), format!("{{base}}{root}/tag/v{v}"))];
    let bad_sums = vec![(
        format!("{root}/download/v9.9.9/{SUMS_FILE}"),
        sums_line(asset, b"another binary").into_bytes(),
    )];
    // What, the files and redirects served, install.sh's arguments, and the failure expected.
    type Case<'a> = (&'a str, Files, Redirects, Vec<&'a str>, Option<&'a str>);
    let cases: Vec<Case> = vec![
        // A requested version comes from its own tag, with no redirect.
        (
            "requested",
            tagged("9.9.7", &fake_binary("9.9.7")),
            vec![],
            vec!["9.9.7"],
            None,
        ),
        (
            "binary not its tag",
            tagged("9.9.9", &fake_binary("9.9.7")),
            latest("9.9.9"),
            vec![],
            Some("has potluck 9.9.7, not 9.9.9 as its tag says"),
        ),
        (
            "no release",
            vec![],
            vec![],
            vec![],
            Some("couldn't find the latest release"),
        ),
        (
            "not a tag",
            vec![],
            vec![(format!("{root}/latest"), format!("{{base}}{root}"))],
            vec![],
            Some("not a release tag"),
        ),
        (
            "bad checksum, no race from a tag",
            [tagged("9.9.9", &fake_binary("9.9.9")), bad_sums].concat(),
            latest("9.9.9"),
            vec![],
            Some("refusing it, nothing was installed\n"),
        ),
    ];
    for (what, files, redirects, args, failure) in cases {
        let server = Server::serving(files, redirects);
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("bin");
        let env = [("POTLUCK_GITHUB_URL", server.github_url())];
        let run = run_install_env("sh", &dir, &args, &env);
        match failure {
            None => {
                assert!(run.ok, "{what}: {}", run.stderr);
                assert_eq!(fs::read(dir.join("potluck")).unwrap(), fake_binary("9.9.7"));
            }
            Some(expect) => {
                assert!(!run.ok, "{what}: {}", run.stdout);
                assert!(run.stderr.contains(expect), "{what}: {}", run.stderr);
                assert!(!dir.join("potluck").exists(), "{what}");
            }
        }
    }
}

/// Writes an executable script at `path` through a child process, so this (multithreaded) test
/// process never holds it open for writing. If it did, another test thread forking meanwhile would
/// leave a copy of that descriptor in its child until the child execs, and running the script
/// in that time fails with ETXTBSY ("Text file busy").
fn write_script(path: &Path, text: &str) {
    let mut child = Command::new("sh")
        .args(["-c", "cat > \"$1\" && chmod 755 \"$1\"", "sh"])
        .arg(path)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(text.as_bytes())
        .unwrap();
    assert!(
        child.wait().unwrap().success(),
        "writing {}",
        path.display()
    );
}

#[test]
fn install_sh_refuses_unsupported_platforms() {
    let server = Server::start(release(&fake_binary("9.9.9")));
    let tmp = tempfile::tempdir().unwrap();
    let fake_bin = tmp.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    let uname = fake_bin.join("uname");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let dir = tmp.path().join("bin");
    for (os, arch) in [
        ("FreeBSD", "amd64"),
        ("Linux", "riscv64"),
        ("Linux", "armv7l"),
    ] {
        write_script(
            &uname,
            &format!("#!/bin/sh\ncase $1 in -s) echo {os} ;; -m) echo {arch} ;; esac\n"),
        );
        let run = run_install_with("sh", &server.url(), &dir, &[], Some(&path));
        assert!(!run.ok, "{os} {arch}: {}", run.stdout);
        assert!(
            run.stderr
                .contains(&format!("not for {os} on {arch}; build it from source")),
            "{os} {arch}: {}",
            run.stderr
        );
        assert!(!dir.exists());
    }
    assert!(server.requests().is_empty());
}

// The restart helpers, against services named for tests. They start and stop real services, so
// they're ignored by default: `cargo test -- --ignored restarts_`.

#[cfg(target_os = "linux")]
#[test]
#[ignore = "starts a potluck-test systemd user unit"]
fn restarts_a_running_systemd_unit() {
    let unit = format!("potluck-test-restart-{}.service", std::process::id());
    let main_pid = || {
        let out = Command::new("systemctl")
            .args(["--user", "show", "--property=MainPID", "--value", &unit])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    let started = Command::new("systemd-run")
        .args(["--user", "--quiet", "--unit", &unit, "sleep", "300"])
        .status()
        .unwrap();
    assert!(started.success());
    let before = main_pid();
    let result = restart::restart_systemd_unit(&unit);
    let after = main_pid();
    let _ = Command::new("systemctl")
        .args(["--user", "stop", &unit])
        .status();
    let _ = Command::new("systemctl")
        .args(["--user", "reset-failed", &unit])
        .stderr(Stdio::null())
        .status();
    assert_eq!(
        result.unwrap(),
        Restart::Restarted(format!("systemctl --user restart {unit}"))
    );
    assert_ne!(before, "0");
    assert_ne!(after, "0");
    assert_ne!(before, after);
    assert_eq!(
        restart::restart_systemd_unit("potluck-test-no-such-unit.service").unwrap(),
        Restart::NoService
    );
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "loads a potluck.test LaunchAgent"]
fn restarts_a_loaded_launch_agent() {
    let label = format!("potluck.test.restart.{}", std::process::id());
    let dir = tempfile::tempdir().unwrap();
    let plist = dir.path().join(format!("{label}.plist"));
    fs::write(
        &plist,
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>{label}</string>
<key>ProgramArguments</key><array><string>/bin/sleep</string><string>300</string></array>
<key>RunAtLoad</key><true/>
</dict></plist>
"#
        ),
    )
    .unwrap();
    let target = restart::launchd_target(&label);
    let pid = || {
        let out = Command::new("launchctl")
            .args(["print", &target])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .find_map(|l| l.trim().strip_prefix("pid = ").map(str::to_string))
    };
    let domain = target.rsplit_once('/').unwrap().0.to_string();
    let loaded = Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&plist)
        .status()
        .unwrap();
    assert!(loaded.success());
    let wait_pid = |not: Option<String>| {
        for _ in 0..100 {
            if let Some(p) = pid()
                && Some(&p) != not.as_ref()
            {
                return Some(p);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        None
    };
    let before = wait_pid(None);
    let result = restart::restart_launch_agent(&label, &plist);
    let after = wait_pid(before.clone());
    let _ = Command::new("launchctl")
        .args(["bootout", &target])
        .status();
    assert_eq!(
        result.unwrap(),
        Restart::Restarted(format!("launchctl kickstart -k {target}"))
    );
    assert!(before.is_some());
    assert!(after.is_some());
    // Unloaded with its plist still there: installed, not running.
    assert_eq!(
        restart::restart_launch_agent(&label, &plist).unwrap(),
        Restart::NotRunning(label.clone())
    );
    assert_eq!(
        restart::restart_launch_agent(&label, &dir.path().join("none.plist")).unwrap(),
        Restart::NoService
    );
}
