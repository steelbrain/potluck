//! Git over the mesh: a runner's mirror fetches from the submitting machine through a mesh stream,
//! with no ssh or GitHub access between them.
//!
//! - **The URL.** [`fetch_url`] builds `ext::<potluck> _tunnel -- <node> <repo_id>` (the `--`
//!   because a node name or repository id may start with `-`). Git's `ext::`
//!   transport runs that command and speaks git's protocol (v0) on its stdin and stdout. Git
//!   refuses `ext::` unless allowed: a command fetching from such a URL runs with
//!   `git -c protocol.ext.allow=always` ([`git_args`]), for that command only and never globally,
//!   and only for URLs of exactly this shape, so any other `ext::` command stays refused.
//! - **The client.** `potluck _tunnel NODE REPO_ID` ([`run_tunnel`]) loads this machine's keys
//!   and records as the daemon does (`POTLUCK_HOME`, `POTLUCK_CONFIG`), dials NODE with
//!   [`Mesh::connect_only`], opens a stream with the header [`upload_pack_header`] and pipes its
//!   stdin and stdout through it. Nothing but git's protocol goes to stdout; errors go to stderr
//!   (git shows them) with a non-zero exit. Only fetching: git names the service it wants in
//!   `GIT_EXT_SERVICE`, and anything but `git-upload-pack` is refused.
//! - **The server.** The owner's daemon reads the header's repository id (its stream header
//!   `upload-pack`, parsed as the cluster's `StreamHeader`), serves its own mirror of that
//!   repository if a dispatch there named it, and calls [`serve_upload_pack`] through
//!   [`crate::daemon::GitTunnel`]; an unknown id gets [`refuse_upload_pack`], which git shows
//!   as `fatal: remote error: <message>`. `git upload-pack` runs in a process group of its own and
//!   is killed with its children if either side goes away; it is always reaped before the call
//!   returns.
//!
//! Members can fetch any commit the source has, as with git's protocol v2: a job's commit may be
//! on no branch any more by the time a runner asks for it.

use crate::config::{NodeConfig, Paths};
use crate::mesh::{Mesh, MeshConfig, Stream};
use anyhow::{Context, bail};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, ChildStdin, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// The stream kind in the header.
pub const KIND: &str = "upload-pack";

/// The git config a command reading from a tunnel URL needs, for that command only:
/// `git -c protocol.ext.allow=always ...`. [`git_args`] adds it for tunnel URLs.
pub const GIT_CONFIG: (&str, &str) = ("protocol.ext.allow", "always");

/// The service git asks the `ext::` command for when it fetches.
const FETCH_SERVICE: &str = "git-upload-pack";

/// How often the server's reading side wakes to see whether the transfer is over.
const POLL: Duration = Duration::from_millis(200);
/// After `git upload-pack` is done, how long the server waits for the client to close its side
/// (reading whatever it still sends, so closing never resets a connection the client is reading).
const LINGER: Duration = Duration::from_secs(60);
/// A client that takes no bytes for this long is gone.
const WRITE_TIMEOUT: Duration = Duration::from_secs(300);
/// `git upload-pack --timeout`: no progress for this long ends it.
const UPLOAD_PACK_TIMEOUT_S: u64 = 300;
/// How long `git upload-pack` gets to exit once its output has ended.
const EXIT_WAIT: Duration = Duration::from_secs(10);
/// The most of `git upload-pack`'s stderr kept for an error.
const MAX_STDERR: usize = 16 * 1024;

/// The stream header asking `node` to serve `git upload-pack` for `repo_id`.
pub fn upload_pack_header(repo_id: &str) -> Value {
    json!({ "kind": KIND, "repo_id": repo_id })
}

/// The repository id of a valid upload-pack header: exactly `{"kind":"upload-pack","repo_id":ID}`
/// with ID a valid repository id (letters, digits, `-`, `_` and `.`; no `..`, no leading `.`).
/// `None` for anything else.
pub fn upload_pack_repo(header: &Value) -> Option<String> {
    let obj = header.as_object()?;
    if obj.len() != 2 || obj.get("kind")?.as_str()? != KIND {
        return None;
    }
    let id = obj.get("repo_id")?.as_str()?;
    valid_repo_id(id).then(|| id.to_string())
}

/// The same rule as the workspace module's repository ids.
fn valid_repo_id(id: &str) -> bool {
    !id.is_empty()
        && !id.starts_with('.')
        && !id.ends_with(".lock")
        && !id.contains("..")
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// The URL a git command fetches `repo_id` from `node` with, through `potluck_bin _tunnel --`
/// (node names and repository ids may start with `-`): `ext::` and the command's arguments,
/// separated by spaces, with `%` written `%%` and a space inside an argument `% ` (git's `ext::`
/// syntax). A path that isn't UTF-8 can't be written in a
/// URL; its invalid bytes become U+FFFD and git will fail to run it.
pub fn fetch_url(potluck_bin: &Path, node: &str, repo_id: &str) -> String {
    let bin = potluck_bin.to_string_lossy();
    let args = [bin.as_ref(), "_tunnel", "--", node, repo_id];
    let escaped: Vec<String> = args
        .iter()
        .map(|a| a.replace('%', "%%").replace(' ', "% "))
        .collect();
    format!("ext::{}", escaped.join(" "))
}

/// The arguments of an `ext::` URL as git's `ext::` transport splits them. `None` if it isn't
/// one, or uses a placeholder (`%s`, `%S`, `%G`, `%V`).
fn ext_args(url: &str) -> Option<Vec<String>> {
    let rest = url.strip_prefix("ext::")?;
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        match c {
            '%' => match chars.next()? {
                ' ' => cur.push(' '),
                '%' => cur.push('%'),
                _ => return None,
            },
            ' ' => args.push(std::mem::take(&mut cur)),
            c => cur.push(c),
        }
    }
    args.push(cur);
    Some(args)
}

/// Whether `url` is a URL [`fetch_url`] made: `ext::<program> _tunnel -- <node> <repo_id>`.
pub fn is_tunnel_url(url: &str) -> bool {
    ext_args(url).is_some_and(|a| {
        a.len() == 5
            && !a[0].is_empty()
            && a[1] == "_tunnel"
            && a[2] == "--"
            && !a[3].is_empty()
            && valid_repo_id(&a[4])
    })
}

/// The `git -c` arguments (to go before the subcommand) a git command reading from `url`
/// needs: [`GIT_CONFIG`] for a tunnel URL, nothing for any other URL.
pub fn git_args(url: &str) -> Vec<String> {
    if is_tunnel_url(url) {
        vec!["-c".into(), format!("{}={}", GIT_CONFIG.0, GIT_CONFIG.1)]
    } else {
        Vec::new()
    }
}

/// Answers an upload-pack stream with git's error packet (`ERR <message>`) and closes it: the
/// fetching git stops with `fatal: remote error: <message>`.
pub fn refuse_upload_pack(mut stream: Stream, message: &str) -> io::Result<()> {
    stream.write_all(&err_pkt(message))?;
    stream.close_write()
}

/// A pkt-line `ERR <message>\n`, the message on one line and cut to a sane length.
fn err_pkt(message: &str) -> Vec<u8> {
    let mut msg: String = message
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if msg.len() > 1000 {
        let mut cut = 1000;
        while !msg.is_char_boundary(cut) {
            cut -= 1;
        }
        msg.truncate(cut);
    }
    let body = format!("ERR {}\n", msg.trim());
    let mut pkt = format!("{:04x}", body.len() + 4).into_bytes();
    pkt.extend_from_slice(body.as_bytes());
    pkt
}

/// Runs `git upload-pack` on `git_dir` with its stdin and stdout piped through `stream`, until
/// the fetch is done or either side goes away, for the daemon's [`crate::mesh::Handler::stream`].
///
/// The process (and anything it started) is killed if the stream breaks or stops taking bytes
/// for five minutes, and is reaped before this returns. If `git upload-pack` fails before it
/// sends anything, the client gets its error as a git error packet. `Err` when the stream broke,
/// or `git upload-pack` couldn't run or failed (with its stderr).
pub fn serve_upload_pack(mut stream: Stream, git_dir: &Path) -> io::Result<()> {
    // Before anything is started: these fail on a socket the other side has reset (macOS
    // refuses setsockopt then), and returning after the spawn would leave the child unreaped.
    let reader = stream.try_clone()?;
    reader.set_read_timeout(Some(POLL))?;
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
    let mut cmd = crate::gitinfo::git(git_dir);
    cmd.args([
        "-c",
        "uploadpack.allowAnySHA1InWant=true",
        "upload-pack",
        &format!("--timeout={UPLOAD_PACK_TIMEOUT_S}"),
    ])
    .arg(git_dir)
    // Protocol v0, whatever the daemon's environment says: the header carries no version.
    .env_remove("GIT_PROTOCOL")
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .process_group(0);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            let _ = refuse_upload_pack(stream, &format!("can't run git upload-pack: {e}"));
            return Err(e);
        }
    };
    let stdin = child.stdin.take().expect("piped stdin");
    let mut stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let proc = Arc::new(Proc::new(child));

    let (err_tx, err_rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = err_tx.send(read_capped(stderr, MAX_STDERR));
    });

    let state = Arc::new(AtomicU8::new(RUNNING));
    let inbound = {
        let (proc, state) = (proc.clone(), state.clone());
        thread::spawn(move || pump_in(reader, stdin, &proc, &state))
    };

    let mut sent = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    let out: io::Result<()> = loop {
        let n = match stdout.read(&mut buf) {
            Ok(0) => break Ok(()),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            // Our own pipe: only a broken process gets here.
            Err(e) => break Err(e),
        };
        if let Err(e) = stream.write_all(&buf[..n]) {
            break Err(io::Error::new(
                e.kind(),
                format!("the stream broke after {sent} bytes: {e}"),
            ));
        }
        sent += n as u64;
    };
    drop(stdout);
    if out.is_err() {
        proc.kill();
        state.store(ABORT, Ordering::SeqCst);
    }
    let status = proc.wait(EXIT_WAIT);
    let stderr = err_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or_default();
    let failed = match &status {
        Ok(s) if s.success() => None,
        Ok(s) => Some(format!("git upload-pack {}: {}", s, stderr.trim())),
        Err(e) => Some(format!("git upload-pack: {e}")),
    };
    if out.is_ok() {
        if sent == 0
            && let Some(f) = &failed
        {
            let _ = stream.write_all(&err_pkt(f));
        }
        let _ = stream.close_write();
        state.store(DONE, Ordering::SeqCst);
    }
    let inbound = inbound
        .join()
        .unwrap_or_else(|_| Err(io::Error::other("the reading thread panicked")));
    out?;
    if let Some(f) = failed {
        return Err(io::Error::other(f));
    }
    inbound
}

const RUNNING: u8 = 0;
/// `git upload-pack` is done: the reading side drains the client until it closes or [`LINGER`].
const DONE: u8 = 1;
/// The transfer failed: the reading side stops.
const ABORT: u8 = 2;

/// Copies what the client sends to `git upload-pack`'s stdin, and closes that when the client
/// closes. If the client goes away, kills the process.
fn pump_in(mut reader: Stream, stdin: ChildStdin, proc: &Proc, state: &AtomicU8) -> io::Result<()> {
    let mut stdin = Some(stdin);
    let mut buf = vec![0u8; 64 * 1024];
    let mut done_at: Option<Instant> = None;
    loop {
        match state.load(Ordering::SeqCst) {
            ABORT => return Ok(()),
            DONE => {
                stdin = None;
                let since = *done_at.get_or_insert_with(Instant::now);
                if since.elapsed() >= LINGER {
                    return Ok(());
                }
            }
            _ => {}
        }
        match reader.read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                if let Some(s) = stdin.as_mut()
                    && s.write_all(&buf[..n]).is_err()
                {
                    // It stopped reading (it exited, or is sending its pack): drop the rest.
                    stdin = None;
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => {
                if state.load(Ordering::SeqCst) == RUNNING {
                    proc.kill();
                    return Err(io::Error::new(
                        e.kind(),
                        format!("the client went away: {e}"),
                    ));
                }
                return Ok(());
            }
        }
    }
}

/// A child process in a process group of its own, killed (group and all) only while it hasn't
/// been reaped, so a recycled pid is never signalled.
struct Proc {
    child: Mutex<Child>,
    pgid: libc::pid_t,
}

impl Proc {
    fn new(child: Child) -> Proc {
        let pgid = child.id() as libc::pid_t;
        Proc {
            child: Mutex::new(child),
            pgid,
        }
    }

    fn kill(&self) {
        let mut child = self.child.lock().unwrap_or_else(|e| e.into_inner());
        if let Ok(None) = child.try_wait() {
            // SAFETY: the group's leader is our unreaped child, so the id names its group.
            unsafe { libc::kill(-self.pgid, libc::SIGKILL) };
        }
    }

    /// Waits up to `limit` for the process to exit, then kills it, and reaps it.
    fn wait(&self, limit: Duration) -> io::Result<std::process::ExitStatus> {
        let start = Instant::now();
        loop {
            {
                let mut child = self.child.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(status) = child.try_wait()? {
                    return Ok(status);
                }
                if start.elapsed() >= limit {
                    // SAFETY: as in kill: not reaped yet.
                    unsafe { libc::kill(-self.pgid, libc::SIGKILL) };
                    return child.wait();
                }
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Reads `r` to its end, keeping the first `max` bytes as text.
fn read_capped(mut r: impl Read, max: usize) -> String {
    let mut kept = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match r.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let room = max.saturating_sub(kept.len());
                kept.extend_from_slice(&buf[..n.min(room)]);
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&kept).into_owned()
}

/// `potluck _tunnel NODE REPO_ID`: returns the process exit code. Errors go to stderr.
pub fn run_tunnel(node: &str, repo_id: &str) -> i32 {
    match tunnel(node, repo_id) {
        Ok(()) => 0,
        Err(e) => {
            let _ = writeln!(io::stderr(), "potluck _tunnel: {e:#}");
            1
        }
    }
}

fn tunnel(node: &str, repo_id: &str) -> anyhow::Result<()> {
    if let Some(service) = std::env::var_os("GIT_EXT_SERVICE")
        && service != FETCH_SERVICE
    {
        bail!(
            "only fetching works over the mesh (git asked for {})",
            service.to_string_lossy()
        );
    }
    if !valid_repo_id(repo_id) {
        bail!("bad repository id {repo_id:?}");
    }
    let paths = Paths::from_env()?;
    let config = NodeConfig::load(&paths)?;
    let keys = || {
        format!(
            "can't use this machine's cluster keys in {}",
            paths.keys_dir().display()
        )
    };
    // A mesh that doesn't listen names no address in its hello, so the owner keeps reaching this
    // machine where its daemon listens (also when the config says port 0).
    let mesh = Mesh::connect_only(MeshConfig::new(&paths, &config)).with_context(keys)?;
    let stream = mesh
        .open_stream(node, upload_pack_header(repo_id))
        .with_context(|| format!("can't fetch {repo_id} from {node}"))?;
    pipe(stream, node, repo_id)
}

/// Pipes stdin to the stream and the stream to stdout. Done when the other side has sent
/// everything and closed: what git still sends after that can't matter, so this doesn't wait
/// for stdin to end.
fn pipe(stream: Stream, node: &str, repo_id: &str) -> anyhow::Result<()> {
    // Unbuffered: git's protocol, as it arrives.
    let mut out = File::from(io::stdout().as_fd().try_clone_to_owned()?);
    let mut writer = stream.try_clone()?;
    thread::spawn(move || {
        let mut stdin = io::stdin().lock();
        if io::copy(&mut stdin, &mut writer).is_ok() {
            let _ = writer.close_write();
        }
    });
    let mut reader = stream;
    let mut received = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    let result: anyhow::Result<()> = loop {
        match reader.read(&mut buf) {
            Ok(0) => break Ok(()),
            Ok(n) => {
                received += n as u64;
                if let Err(e) = out.write_all(&buf[..n]) {
                    // git stopped reading: it has what it wanted, or it failed and says why.
                    break Err(anyhow::anyhow!("git closed the connection: {e}"));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => break Err(e.into()),
        }
    };
    drop(out);
    let _ = reader.close_write();
    match result {
        Ok(()) if received > 0 => Ok(()),
        Ok(()) => bail!("{node} closed the stream for {repo_id} without answering"),
        Err(e) if received == 0 => Err(e.context(format!(
            "{node} closed the stream for {repo_id} without answering"
        ))),
        Err(e) => Err(e.context(format!(
            "the stream from {node} for {repo_id} broke after {received} bytes"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_escape_spaces_and_percents() {
        let url = fetch_url(
            Path::new("/opt/my tools/100%/potluck"),
            "studio",
            "app-1a2b3c4d",
        );
        assert_eq!(
            url,
            "ext::/opt/my% tools/100%%/potluck _tunnel -- studio app-1a2b3c4d"
        );
        assert_eq!(
            ext_args(&url).unwrap(),
            [
                "/opt/my tools/100%/potluck",
                "_tunnel",
                "--",
                "studio",
                "app-1a2b3c4d"
            ]
        );
        // A path that looks like a placeholder stays literal.
        let url = fetch_url(Path::new("/x/%s %G/potluck"), "a", "r");
        assert_eq!(ext_args(&url).unwrap()[0], "/x/%s %G/potluck");
        assert!(is_tunnel_url(&url));
    }

    #[test]
    fn only_tunnel_urls_get_ext_allowed() {
        let url = fetch_url(Path::new("/usr/bin/potluck"), "studio", "app-1a2b3c4d");
        assert_eq!(git_args(&url), ["-c", "protocol.ext.allow=always"]);
        // A repository in `.dotfiles`, or a node named so: arguments, not options.
        let url = fetch_url(
            Path::new("/usr/bin/potluck"),
            "-studio",
            "-dotfiles-1a2b3c4d",
        );
        assert_eq!(git_args(&url), ["-c", "protocol.ext.allow=always"]);
        for other in [
            "ext::/usr/bin/potluck _tunnel studio app-1a2b3c4d",
            "https://example.com/r.git",
            "/srv/r.git",
            "ext::sh -c touch% /tmp/x",
            "ext::/usr/bin/potluck _tunnel -- studio",
            "ext::/usr/bin/potluck _tunnel -- studio app extra",
            "ext::/usr/bin/potluck _tunnel -- studio %s",
            "ext::/usr/bin/potluck _tunnel -- studio ../x",
            "ext::/usr/bin/potluck _tunnel -- studio app%",
            "ext::/usr/bin/potluck _tunnel -x studio app",
            "ext::/usr/bin/potluck _tunnel --  app",
        ] {
            assert!(git_args(other).is_empty(), "{other}");
        }
    }

    #[test]
    fn headers() {
        assert_eq!(
            upload_pack_repo(&upload_pack_header("app-1a2b3c4d")).as_deref(),
            Some("app-1a2b3c4d")
        );
        for bad in [
            json!({ "kind": "upload-pack" }),
            json!({ "kind": "receive-pack", "repo_id": "app" }),
            json!({ "kind": "upload-pack", "repo_id": 5 }),
            json!({ "kind": "upload-pack", "repo_id": "../etc" }),
            json!({ "kind": "upload-pack", "repo_id": "a/b" }),
            json!({ "kind": "upload-pack", "repo_id": "" }),
            json!({ "kind": "upload-pack", "repo_id": "app", "path": "/etc" }),
            json!("upload-pack"),
        ] {
            assert_eq!(upload_pack_repo(&bad), None, "{bad}");
        }
    }

    #[test]
    fn error_packets() {
        assert_eq!(err_pkt("no such repo"), b"0015ERR no such repo\n");
        assert_eq!(err_pkt("two\nlines"), b"0012ERR two lines\n");
        let long = err_pkt(&"é".repeat(2000));
        let len = usize::from_str_radix(std::str::from_utf8(&long[..4]).unwrap(), 16).unwrap();
        assert_eq!(len, long.len());
        assert!(len < 1100);
    }
}
