//! `potluck self-update --here` through the real binary: a copy of it is both the installed file
//! and the release, served over plain HTTP on 127.0.0.1, with a fake daemon reporting its version.
//! POTLUCK_NO_RESTART=1 keeps any real service untouched; the output says what would restart.

mod common;

use common::{FakeDaemon, potluck_at, run};
use potluck::proto::{Request, Response};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;

/// Serves `files` (name, contents) at `http://127.0.0.1:<port>/` until the test process ends.
fn serve(files: Vec<(String, Vec<u8>)>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            answer(conn, &files);
        }
    });
    url
}

fn answer(mut conn: TcpStream, files: &[(String, Vec<u8>)]) {
    let mut reader = BufReader::new(conn.try_clone().unwrap());
    let mut request = String::new();
    let _ = reader.read_line(&mut request);
    let mut header = String::new();
    while reader.read_line(&mut header).is_ok_and(|n| n > 0) && header.trim() != "" {
        header.clear();
    }
    let path = request.split_whitespace().nth(1).unwrap_or("");
    let found = files.iter().find(|(name, _)| path == format!("/{name}"));
    let (status, body) = match found {
        Some((_, data)) => ("200 OK", data.as_slice()),
        None => ("404 Not Found", &b"not found\n"[..]),
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = conn.write_all(head.as_bytes());
    let _ = conn.write_all(body);
}

fn daemon_at(version: &'static str) -> FakeDaemon {
    FakeDaemon::start(vec![], move |req, conn| match &req.req {
        Request::Ping => conn.send(Response::Pong {
            version: version.into(),
            node: "studio".into(),
        }),
        _ => conn.send(Response::Error {
            message: "unexpected".into(),
        }),
    })
}

fn self_update(bin: &Path, home: &Path, releases: &str) -> common::Run {
    run(potluck_at(bin, home)
        .env("POTLUCK_RELEASES_URL", releases)
        .env("POTLUCK_NO_RESTART", "1")
        // A service that isn't installed: the real one is never looked at.
        .env(
            "POTLUCK_SERVICE_NAME",
            format!("potluck-test-none-{}", std::process::id()),
        )
        .env("POTLUCK_SERVICE_DIR", home.join("no-services"))
        .env_remove("POTLUCK_GITHUB_URL")
        .args(["self-update", "--here"]))
}

/// The file on disk already is the release, but the daemon may still run an older binary (one
/// replaced by install.sh with POTLUCK_NO_RESTART=1): the CLI asks the daemon its version, and an
/// older daemon is restarted.
#[test]
fn self_update_here_restarts_a_daemon_older_than_the_binary() {
    let tmp = tempfile::tempdir().unwrap();
    let bin = tmp.path().join("potluck");
    common::copy_binary(Path::new(env!("CARGO_BIN_EXE_potluck")), &bin);
    let data = std::fs::read(&bin).unwrap();
    let asset = potluck::update::asset_name().unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &data);
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let releases = serve(vec![
        (asset.to_string(), data),
        (
            "SHA256SUMS".to_string(),
            format!("{hex}  {asset}\n").into_bytes(),
        ),
    ]);
    let version = potluck::VERSION;
    let unchanged = format!(
        "{} is already the release binary (potluck {version}); nothing was changed.",
        bin.canonicalize().unwrap().display()
    );

    let older = daemon_at("0.0.1-old");
    let r = self_update(&bin, older.home(), &releases);
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert!(
        r.stdout.starts_with(&format!(
            "{unchanged} The daemon runs potluck 0.0.1-old. POTLUCK_NO_RESTART=1, so the daemon \
             was not restarted"
        )),
        "{}",
        r.stdout
    );
    // No service is installed: there is no service command to name.
    assert!(
        r.stdout.contains("no potluck service is installed here")
            && !r.stdout.contains("systemctl")
            && !r.stdout.contains("launchctl"),
        "{}",
        r.stdout
    );

    let current = daemon_at(version);
    let r = self_update(&bin, current.home(), &releases);
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.stdout.trim(), unchanged);

    let no_daemon = tempfile::tempdir().unwrap();
    let r = self_update(&bin, no_daemon.path(), &releases);
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.stdout.trim(), unchanged);
}

/// `self-update --here` asks the daemon to restart in place on the binary (`potluck restart`),
/// and prints its answer. A daemon from before `restart` doesn't know the request: then its
/// service is restarted as before (none is installed here, and the output says so).
#[test]
fn self_update_here_asks_the_daemon_to_restart_in_place() {
    let tmp = tempfile::tempdir().unwrap();
    let bin = tmp.path().join("potluck");
    common::copy_binary(Path::new(env!("CARGO_BIN_EXE_potluck")), &bin);
    let data = std::fs::read(&bin).unwrap();
    let asset = potluck::update::asset_name().unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &data);
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let releases = serve(vec![
        (asset.to_string(), data),
        (
            "SHA256SUMS".to_string(),
            format!("{hex}  {asset}\n").into_bytes(),
        ),
    ]);
    let daemon = |restart: Response| {
        let bin = bin.clone();
        FakeDaemon::start(vec![], move |req, conn| match &req.req {
            Request::Ping => conn.send(Response::Pong {
                version: "0.0.1-old".into(),
                node: "studio".into(),
            }),
            // Asked to restart on the binary it updated.
            Request::Restart { binary: Some(b) } if potluck::service::same_file(b, &bin) => {
                conn.send(restart.clone())
            }
            _ => conn.send(Response::Error {
                message: "unexpected".into(),
            }),
        })
    };
    let update = |home: &Path| {
        run(potluck_at(&bin, home)
            .env("POTLUCK_RELEASES_URL", &releases)
            .env_remove("POTLUCK_NO_RESTART")
            .env(
                "POTLUCK_SERVICE_NAME",
                format!("potluck-test-none-{}", std::process::id()),
            )
            .env("POTLUCK_SERVICE_DIR", home.join("no-services"))
            .env_remove("POTLUCK_GITHUB_URL")
            .args(["self-update", "--here"]))
    };

    let current = daemon(Response::Text {
        text: "Restarted in place: (the test daemon)".into(),
    });
    let r = update(current.home());
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert!(
        r.stdout
            .trim_end()
            .ends_with("The daemon runs potluck 0.0.1-old. Restarted in place: (the test daemon)"),
        "{}",
        r.stdout
    );
    assert!(current.ops().contains(&"restart".to_string()));

    // What a daemon from before `restart` answers a request it doesn't know.
    let old = daemon(Response::Error {
        message: "bad request: unknown variant `restart`, expected one of `ping`, `dispatch`"
            .into(),
    });
    let r = update(old.home());
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert!(
        r.stdout.contains("No potluck service is installed here"),
        "{}",
        r.stdout
    );
}

/// install.sh restarts the service only when the service runs the file it just replaced. A service
/// installed from another potluck would run that one again, so it is left alone, and the output
/// says how to point it at the new one. Installs and removes a real user service named
/// `potluck-test-*`.
#[test]
#[ignore = "installs a real user service named potluck-test-*; run with --ignored"]
fn install_sh_restarts_only_a_service_that_runs_the_installed_file() {
    let tmp = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let (a, b) = (tmp.path().join("a"), tmp.path().join("b"));
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    let bin_a = a.join("potluck");
    common::copy_binary(Path::new(env!("CARGO_BIN_EXE_potluck")), &bin_a);
    let data = std::fs::read(&bin_a).unwrap();
    let asset = potluck::update::asset_name().unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &data);
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let releases = serve(vec![
        (asset.to_string(), data),
        (
            "SHA256SUMS".to_string(),
            format!("{hex}  {asset}\n").into_bytes(),
        ),
    ]);
    let name = format!("potluck-test-install-{}", std::process::id());
    let services = if cfg!(target_os = "macos") {
        tmp.path().join("agents")
    } else {
        Path::new(&std::env::var("HOME").unwrap()).join(".config/systemd/user")
    };
    let home = tmp.path().join("home");
    let with_service = |c: &mut std::process::Command| {
        c.env("POTLUCK_HOME", &home)
            .env("POTLUCK_CONFIG", home.join("node.toml"))
            .env("POTLUCK_SERVICE_DIR", &services)
            .env("POTLUCK_SERVICE_NAME", &name)
            .env_remove("POTLUCK_NO_RESTART");
    };
    let service = |action: &str| {
        let mut c = std::process::Command::new(&bin_a);
        with_service(&mut c);
        run(c.args(["service", action]))
    };
    let install_into = |dir: &Path| {
        let mut c = std::process::Command::new("sh");
        with_service(&mut c);
        run(c
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("install.sh"))
            .env("POTLUCK_INSTALL_DIR", dir)
            .env("POTLUCK_RELEASES_URL", &releases)
            .stdin(std::process::Stdio::null()))
    };
    struct Uninstall<'a>(&'a dyn Fn(&str) -> common::Run);
    impl Drop for Uninstall<'_> {
        fn drop(&mut self) {
            let _ = (self.0)("uninstall");
        }
    }
    let r = service("install");
    let _uninstall = Uninstall(&service);
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);

    let r = install_into(&b);
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
    let want = format!(
        "The potluck service runs {}, not {}/potluck, so it was not restarted. To make it run {}/potluck: {}/potluck service install",
        bin_a.canonicalize().unwrap().display(),
        b.display(),
        b.display(),
        b.display()
    );
    assert!(r.stdout.contains(&want), "{}", r.stdout);
    assert!(!r.stdout.contains("Restarted"), "{}", r.stdout);

    let r = install_into(&a);
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
    assert!(
        r.stdout.contains("Restarted the potluck service"),
        "{}",
        r.stdout
    );
    let r = service("uninstall");
    assert_eq!(r.code, 0, "{} {}", r.stdout, r.stderr);
}

/// A daemon run in the foreground (no service) that updates itself restarts in place on the new
/// binary, with the same arguments and a handover; before, it ran the old version on, which no
/// member on the new one talks to. The release here is a script standing in for the new potluck:
/// it answers `--version` and `_handover-check`, and otherwise writes its arguments (and whether
/// it was given a handover) to a file. The service name is a test one that doesn't exist, so no
/// real service is looked at.
#[test]
fn a_foreground_daemon_starts_the_new_binary_after_updating_itself() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};
    let tmp = tempfile::Builder::new()
        .prefix("reexec")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let bin = tmp.path().join("bin/potluck");
    std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
    common::copy_binary(Path::new(env!("CARGO_BIN_EXE_potluck")), &bin);
    let script = b"#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'potluck 9.9.9'; exit 0; fi\nif [ \"$1\" = _handover-check ]; then exit 0; fi\nprintf '%s handover=%s\\n' \"$*\" \"${POTLUCK_HANDOVER:+yes}\" > \"$REEXEC_MARKER\"\n".to_vec();
    let asset = potluck::update::asset_name().unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &script);
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let releases = serve(vec![
        (asset.to_string(), script),
        (
            "SHA256SUMS".to_string(),
            format!("{hex}  {asset}\n").into_bytes(),
        ),
    ]);
    let home = tmp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let pid = std::process::id();
    std::fs::write(
        home.join("node.toml"),
        format!("name = \"test-reexec-{pid}\"\n"),
    )
    .unwrap();
    // A login shell that sets nothing (the daemon captures the login environment).
    let shell = tmp.path().join("login.sh");
    std::fs::write(&shell, "#!/bin/sh\nshift 2\nexec /bin/sh -c \"$1\"\n").unwrap();
    std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
    let marker = tmp.path().join("marker");
    let out = tmp.path().join("daemon.out");
    let log = std::fs::File::create(&out).unwrap();
    let mut daemon = potluck_at(&bin, &home)
        .arg("daemon")
        .env("HOME", tmp.path())
        .env("SHELL", &shell)
        .env("POTLUCK_SERVICE_NAME", format!("potluck-test-reexec-{pid}"))
        .env("POTLUCK_RELEASES_URL", &releases)
        .env("REEXEC_MARKER", &marker)
        .env_remove("POTLUCK_NO_RESTART")
        .env_remove("POTLUCK_GITHUB_URL")
        .env_remove("POTLUCK_SERVICE_DIR")
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let start = Instant::now();
    let daemon_out = || std::fs::read_to_string(&out).unwrap_or_default();
    loop {
        let r = run(common::potluck(&home).arg("status"));
        if r.code == 0 {
            break;
        }
        if let Some(status) = daemon.try_wait().unwrap() {
            panic!("the daemon exited ({status}): {}", daemon_out());
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "the daemon never answered: {}",
            daemon_out()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let r = run(common::potluck(&home).arg("self-update"));
    assert_eq!(r.code, 0, "{}{}", r.stdout, r.stderr);
    let start = Instant::now();
    let status = loop {
        if let Some(status) = daemon.try_wait().unwrap() {
            break status;
        }
        if start.elapsed() > Duration::from_secs(60) {
            let _ = daemon.kill();
            panic!("the daemon never restarted: {}", daemon_out());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(status.success(), "{status}: {}", daemon_out());
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap_or_default(),
        "daemon handover=yes\n",
        "{}",
        daemon_out()
    );
    assert!(
        daemon_out().contains("restarting in place (the potluck binary was updated)"),
        "{}",
        daemon_out()
    );
}

/// An update's restart looks at the service `POTLUCK_SERVICE_NAME` and `POTLUCK_SERVICE_DIR`
/// name, as `potluck service install` does, not at the real one. Here a LaunchAgent plist (never
/// loaded) says the service runs another program, so the update says so instead of restarting.
#[cfg(target_os = "macos")]
#[test]
fn an_update_restarts_the_service_the_service_variables_name() {
    let tmp = tempfile::tempdir().unwrap();
    let bin = tmp.path().join("potluck");
    common::copy_binary(Path::new(env!("CARGO_BIN_EXE_potluck")), &bin);
    let script = b"#!/bin/sh\necho 'potluck 9.9.9'\n".to_vec();
    let asset = potluck::update::asset_name().unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &script);
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let releases = serve(vec![
        (asset.to_string(), script),
        (
            "SHA256SUMS".to_string(),
            format!("{hex}  {asset}\n").into_bytes(),
        ),
    ]);
    let agents = tmp.path().join("agents");
    std::fs::create_dir_all(&agents).unwrap();
    let name = format!("potluck-test-svc-{}", std::process::id());
    std::fs::write(
        agents.join(format!("{name}.plist")),
        "<plist><dict><key>ProgramArguments</key><array><string>/usr/bin/true</string>\
         <string>daemon</string></array></dict></plist>\n",
    )
    .unwrap();
    let no_daemon = tempfile::tempdir().unwrap();
    let r = run(potluck_at(&bin, no_daemon.path())
        .env("HOME", tmp.path())
        .env("POTLUCK_RELEASES_URL", &releases)
        .env("POTLUCK_SERVICE_NAME", &name)
        .env("POTLUCK_SERVICE_DIR", &agents)
        .env_remove("POTLUCK_NO_RESTART")
        .env_remove("POTLUCK_GITHUB_URL")
        .args(["self-update", "--here"]));
    assert_eq!(r.code, 0, "{}{}", r.stdout, r.stderr);
    assert!(
        r.stdout
            .contains("The potluck service runs /usr/bin/true, not "),
        "{}",
        r.stdout
    );
}

/// `self-update` asks the daemon; with none running it says how to update without one.
#[test]
fn self_update_without_a_daemon_names_the_way_without_one() {
    let home = tempfile::tempdir().unwrap();
    let r = run(common::potluck(home.path())
        .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
        .arg("self-update"));
    assert_eq!(r.code, 2, "{}{}", r.stdout, r.stderr);
    assert!(
        r.stderr.contains("the potluck daemon is not running")
            && r.stderr.contains("`potluck self-update --here`"),
        "{}",
        r.stderr
    );
}

/// install.sh with a daemon running here without a service (`potluck daemon` in the foreground)
/// asks it to restart in place on the binary it installed, as `self-update --here` does. It used
/// to leave the daemon running the old binary and print only the next steps for a new machine
/// (`potluck service install`, `potluck pair`). The release is the real potluck, installed over a
/// copy of itself; the service names are test ones that don't exist (on macOS `launchctl` is a
/// stub that knows no service).
#[test]
fn install_sh_restarts_a_daemon_running_without_a_service_in_place() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};
    let tmp = tempfile::Builder::new()
        .prefix("install-fg")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let bin = tmp.path().join("bin/potluck");
    std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
    common::copy_binary(Path::new(env!("CARGO_BIN_EXE_potluck")), &bin);
    let data = std::fs::read(env!("CARGO_BIN_EXE_potluck")).unwrap();
    let asset = potluck::update::asset_name().unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &data);
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let releases = serve(vec![
        (asset.to_string(), data),
        (
            "SHA256SUMS".to_string(),
            format!("{hex}  {asset}\n").into_bytes(),
        ),
    ]);
    let home = tmp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let pid = std::process::id();
    std::fs::write(
        home.join("node.toml"),
        format!("name = \"test-install-fg-{pid}\"\n"),
    )
    .unwrap();
    let script = |name: &str, text: &str| {
        let path = tmp.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    };
    // A login shell that sets nothing (the daemon captures the login environment).
    let shell = script("login.sh", "#!/bin/sh\nshift 2\nexec /bin/sh -c \"$1\"\n");
    script(
        "stub/launchctl",
        "#!/bin/sh\necho 'Could not find service' >&2\nexit 113\n",
    );
    let service = format!("potluck-test-none-{pid}");
    let services = tmp.path().join("no-services");
    let out = tmp.path().join("daemon.out");
    let log = std::fs::File::create(&out).unwrap();
    let mut daemon = potluck_at(&bin, &home)
        .arg("daemon")
        .env("HOME", tmp.path())
        .env("SHELL", &shell)
        .env("POTLUCK_SERVICE_NAME", &service)
        .env("POTLUCK_SERVICE_DIR", &services)
        .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
        .env_remove("POTLUCK_NO_RESTART")
        .env_remove("POTLUCK_GITHUB_URL")
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    struct Stop<'a>(&'a mut std::process::Child);
    impl Drop for Stop<'_> {
        fn drop(&mut self) {
            unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) };
            let _ = self.0.wait();
        }
    }
    let daemon_pid = daemon.id();
    let daemon = Stop(&mut daemon);
    let daemon_out = || std::fs::read_to_string(&out).unwrap_or_default();
    let start = Instant::now();
    while run(common::potluck(&home).arg("status")).code != 0 {
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "the daemon exited: {}",
            daemon_out()
        );
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "the daemon never answered: {}",
            daemon_out()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let path = format!(
        "{}:{}",
        tmp.path().join("stub").display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let r = run(std::process::Command::new("sh")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("install.sh"))
        .env("PATH", path)
        .env("POTLUCK_HOME", &home)
        .env("POTLUCK_CONFIG", home.join("node.toml"))
        .env("POTLUCK_INSTALL_DIR", bin.parent().unwrap())
        .env("POTLUCK_RELEASES_URL", &releases)
        .env("POTLUCK_SERVICE_NAME", &service)
        .env("POTLUCK_SERVICE_DIR", &services)
        .env_remove("POTLUCK_NO_RESTART")
        .env_remove("POTLUCK_GITHUB_URL")
        .stdin(std::process::Stdio::null()));
    assert_eq!(r.code, 0, "{}{}", r.stdout, r.stderr);
    let v = potluck::VERSION;
    assert!(
        r.stdout.contains(&format!(
            "Restarted in place: potluck {v} became potluck {v} in the same process (pid {daemon_pid})"
        )),
        "{}{}\n{}",
        r.stdout,
        r.stderr,
        daemon_out()
    );
    assert!(daemon.0.try_wait().unwrap().is_none());
    assert_eq!(run(common::potluck(&home).arg("status")).code, 0);
}

/// install.sh runs without HOME when POTLUCK_INSTALL_DIR says where to install (it says so when
/// neither is set). potluck finds no daemon without HOME, so there is none to restart: it installs
/// and exits 0. Since b5c5336 it ran `potluck restart` for a daemon in the foreground, which
/// exited 2 ("HOME is not set"), and install.sh exited 1, saying "the potluck daemon running here
/// did not restart in place" with no daemon running.
#[test]
fn install_sh_without_home_installs_and_asks_no_daemon() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::Builder::new()
        .prefix("install-nohome")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let data = std::fs::read(env!("CARGO_BIN_EXE_potluck")).unwrap();
    let asset = potluck::update::asset_name().unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &data);
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let releases = serve(vec![
        (asset.to_string(), data),
        (
            "SHA256SUMS".to_string(),
            format!("{hex}  {asset}\n").into_bytes(),
        ),
    ]);
    // On macOS `launchctl` is a stub that knows no service.
    let stub = tmp.path().join("stub");
    std::fs::create_dir_all(&stub).unwrap();
    std::fs::write(
        stub.join("launchctl"),
        "#!/bin/sh\necho 'Could not find service' >&2\nexit 113\n",
    )
    .unwrap();
    std::fs::set_permissions(
        stub.join("launchctl"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    let bin = tmp.path().join("bin");
    let pid = std::process::id();
    let r = run(std::process::Command::new("sh")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("install.sh"))
        .env_clear()
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", stub.display()),
        )
        .env("POTLUCK_INSTALL_DIR", &bin)
        .env("POTLUCK_HOME", tmp.path().join("home"))
        .env("POTLUCK_RELEASES_URL", &releases)
        .env("POTLUCK_SERVICE_NAME", format!("potluck-test-nohome-{pid}"))
        .env("POTLUCK_SERVICE_DIR", tmp.path().join("no-services"))
        .stdin(std::process::Stdio::null()));
    assert_eq!(r.code, 0, "{}{}", r.stdout, r.stderr);
    assert!(
        r.stdout.contains(&format!(
            "Installed potluck {} to {}/potluck.",
            potluck::VERSION,
            bin.display()
        )),
        "{}",
        r.stdout
    );
    assert!(bin.join("potluck").exists());
}

/// install.sh into one directory, with a daemon running in the foreground from a potluck in
/// another (both the real binary): the daemon restarts in place on the binary at its own path,
/// so a restart would run that one again, unchanged. It used to do so and install.sh exited 0
/// with "Restarted in place: potluck V became potluck V", as if the daemon now ran what was
/// installed. Now the daemon refuses, naming both binaries and what to do, serves on as it was,
/// and install.sh fails with that. `potluck restart` from the daemon's own binary still works.
#[test]
fn install_sh_leaves_a_daemon_running_another_binary_and_says_so() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};
    let tmp = tempfile::Builder::new()
        .prefix("install-other")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let other = tmp.path().join("other/potluck");
    std::fs::create_dir_all(other.parent().unwrap()).unwrap();
    std::fs::copy(env!("CARGO_BIN_EXE_potluck"), &other).unwrap();
    let data = std::fs::read(env!("CARGO_BIN_EXE_potluck")).unwrap();
    let asset = potluck::update::asset_name().unwrap();
    let digest = ring::digest::digest(&ring::digest::SHA256, &data);
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let releases = serve(vec![
        (asset.to_string(), data),
        (
            "SHA256SUMS".to_string(),
            format!("{hex}  {asset}\n").into_bytes(),
        ),
    ]);
    let home = tmp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let pid = std::process::id();
    std::fs::write(
        home.join("node.toml"),
        format!("name = \"test-install-other-{pid}\"\n"),
    )
    .unwrap();
    let script = |name: &str, text: &str| {
        let path = tmp.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    };
    // A login shell that sets nothing (the daemon captures the login environment).
    let shell = script("login.sh", "#!/bin/sh\nshift 2\nexec /bin/sh -c \"$1\"\n");
    script(
        "stub/launchctl",
        "#!/bin/sh\necho 'Could not find service' >&2\nexit 113\n",
    );
    let service = format!("potluck-test-none-{pid}");
    let services = tmp.path().join("no-services");
    let out = tmp.path().join("daemon.out");
    let log = std::fs::File::create(&out).unwrap();
    let mut daemon = potluck_at(&other, &home)
        .arg("daemon")
        .env("HOME", tmp.path())
        .env("SHELL", &shell)
        .env("POTLUCK_SERVICE_NAME", &service)
        .env("POTLUCK_SERVICE_DIR", &services)
        .env("POTLUCK_RELEASES_URL", "http://127.0.0.1:9/")
        .env_remove("POTLUCK_NO_RESTART")
        .env_remove("POTLUCK_GITHUB_URL")
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    struct Stop<'a>(&'a mut std::process::Child);
    impl Drop for Stop<'_> {
        fn drop(&mut self) {
            unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) };
            let _ = self.0.wait();
        }
    }
    let daemon = Stop(&mut daemon);
    let daemon_out = || std::fs::read_to_string(&out).unwrap_or_default();
    let start = Instant::now();
    while run(common::potluck(&home).arg("status")).code != 0 {
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "the daemon exited: {}",
            daemon_out()
        );
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "the daemon never answered: {}",
            daemon_out()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let new_dir = tmp.path().join("new");
    let path = format!(
        "{}:{}",
        tmp.path().join("stub").display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let r = run(std::process::Command::new("sh")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("install.sh"))
        .env("PATH", path)
        .env("HOME", tmp.path())
        .env("POTLUCK_HOME", &home)
        .env("POTLUCK_CONFIG", home.join("node.toml"))
        .env("POTLUCK_INSTALL_DIR", &new_dir)
        .env("POTLUCK_RELEASES_URL", &releases)
        .env("POTLUCK_SERVICE_NAME", &service)
        .env("POTLUCK_SERVICE_DIR", &services)
        .env_remove("POTLUCK_NO_RESTART")
        .env_remove("POTLUCK_GITHUB_URL")
        .stdin(std::process::Stdio::null()));
    let new = new_dir.join("potluck");
    assert!(new.exists());
    assert_eq!(r.code, 1, "{}{}\n{}", r.stdout, r.stderr, daemon_out());
    assert!(!r.stdout.contains("Restarted"), "{}", r.stdout);
    let (a, b) = (other.canonicalize().unwrap(), new.canonicalize().unwrap());
    assert!(
        r.stderr.contains(&format!(
            "this daemon runs {}, not {} (the potluck that asked)",
            a.display(),
            b.display()
        )) && r.stderr.contains(&format!(
            "To run {}: replace {} with it and run `{} restart`",
            b.display(),
            a.display(),
            a.display()
        )),
        "{}",
        r.stderr
    );
    // It serves on as it was, and its own binary restarts it.
    assert!(daemon.0.try_wait().unwrap().is_none());
    assert!(
        !daemon_out().contains("restarting in place"),
        "{}",
        daemon_out()
    );
    let r = run(potluck_at(&other, &home).arg("restart"));
    assert_eq!(r.code, 0, "{}{}", r.stdout, r.stderr);
    assert!(r.stdout.contains("Restarted in place"), "{}", r.stdout);
}
