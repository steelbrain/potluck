//! The CLI's connection to its local daemon. If the daemon isn't running, commands fail with how to
//! start it; the CLI never starts a daemon itself.

use crate::config::Paths;
use crate::proto::{
    ClientContext, ClientRequest, Request, Response, ServerResponse, read_msg, write_msg,
};
use crate::types::Reminder;
use std::io::{BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

/// How long the CLI waits for any one answer from its daemon before giving up. Long polls ask the
/// daemon to answer well within this.
pub const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// The caller's context: the repository and branch of `cwd` when it is inside a git repository
/// (one `git rev-parse`, never an error outside one), and `job`/`lease` from POTLUCK_JOB and
/// POTLUCK_LEASE.
pub fn context(cwd: &Path, job: Option<String>, lease: Option<String>) -> ClientContext {
    let (repo_id, branch) = match repo_of(cwd) {
        Some((id, branch)) => (Some(id), branch),
        None => (None, None),
    };
    ClientContext {
        repo_id,
        branch,
        job,
        lease_token: lease,
    }
}

/// The repository id ([`crate::types::repo_id`]) and HEAD's branch of the repository containing
/// `cwd`, if any.
pub fn repo_of(cwd: &Path) -> Option<(String, Option<String>)> {
    // gitinfo's command: git's own variables (GIT_DIR from a hook) must not name another
    // repository than the one dispatch reads.
    let out = crate::gitinfo::git(cwd)
        .args([
            "rev-parse",
            "--git-common-dir",
            "--symbolic-full-name",
            "HEAD",
        ])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    // An unborn branch prints the common dir and then fails on HEAD: the dir is still good.
    let text = String::from_utf8(out.stdout).ok()?;
    let mut lines = text.lines();
    let common = lines.next().filter(|l| !l.is_empty())?;
    let common = PathBuf::from(common);
    let common = if common.is_absolute() {
        common
    } else {
        cwd.join(common)
    };
    let branch = lines
        .next()
        .and_then(|l| l.strip_prefix("refs/heads/"))
        .map(str::to_string);
    Some((crate::types::repo_id(&common), branch))
}

/// The connection to the daemon failed: it closed, broke, or timed out. A daemon restarting (after
/// an update) causes it; the daemon's own error answers are plain errors, not this.
#[derive(Debug)]
pub struct Disconnected(pub String);

impl std::fmt::Display for Disconnected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Disconnected {}

/// What a daemon that is stopping (to restart, or for good) answers a request before it closes
/// the connection. The client takes it as the connection's end ([`Disconnected`]), so `wait` and
/// `logs` connect again as after a hang-up.
pub const STOPPING: &str = "the daemon is stopping";

/// Whether `e` is a failed connection ([`Disconnected`]) rather than an answer.
pub fn is_disconnected(e: &anyhow::Error) -> bool {
    e.is::<Disconnected>()
}

/// Whether `e` is a daemon's answer that it didn't understand the request (a daemon older than
/// this CLI, which doesn't know it).
pub fn not_understood(e: &anyhow::Error) -> bool {
    !is_disconnected(e) && {
        let m = e.to_string();
        m.starts_with("bad request:") && m.contains("unknown variant")
    }
}

/// Connects to the daemon's socket. A connection refused, or a socket not there yet, while a
/// daemon holds the home's `daemon.lock` is tried again, for up to [`READ_TIMEOUT`]: the daemon is
/// there but takes no connections for now. A restart in place leaves them in the socket's
/// backlog, and the backlog is full (macOS refuses connections past its 128,
/// `kern.ipc.somaxconn`, where Linux makes them wait); a daemon starting (a plain restart, a
/// binary that can't take a handover) holds the lock while it captures the login environment and
/// detects facts, and binds its socket after. Nothing holding the lock: no daemon, refused at
/// once.
fn connect_waiting(paths: &Paths, sock: &Path) -> std::io::Result<UnixStream> {
    let give_up = std::time::Instant::now() + READ_TIMEOUT;
    loop {
        match UnixStream::connect(sock) {
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::NotFound
                ) && std::time::Instant::now() < give_up
                    && crate::daemon::running_here(paths) =>
            {
                std::thread::sleep(Duration::from_millis(20));
            }
            other => return other,
        }
    }
}

pub struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    pub ctx: ClientContext,
    /// Reminders from the latest response.
    pub reminders: Vec<Reminder>,
}

impl Client {
    pub fn connect(paths: &Paths, ctx: ClientContext) -> anyhow::Result<Client> {
        let sock = paths.socket();
        let writer = connect_waiting(paths, &sock).map_err(|e| match e.kind() {
            // No socket, or nothing listening on it.
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused => anyhow::anyhow!(
                "the potluck daemon is not running ({}: {e}); start it with `potluck service install`, or `potluck daemon` in the foreground",
                sock.display()
            ),
            _ => anyhow::anyhow!("can't connect to the potluck daemon: {}: {e}", sock.display()),
        })?;
        writer.set_read_timeout(Some(READ_TIMEOUT))?;
        let reader = BufReader::new(writer.try_clone()?);
        Ok(Client {
            reader,
            writer,
            ctx,
            reminders: Vec::new(),
        })
    }

    /// Sends a request and reads one response. An `Error` response becomes an `Err`.
    pub fn call(&mut self, req: Request) -> anyhow::Result<Response> {
        write_msg(
            &mut self.writer,
            &ClientRequest {
                ctx: self.ctx.clone(),
                req,
            },
        )
        .map_err(|e| Disconnected(format!("writing to the daemon: {e}")))?;
        self.next_frame()
    }

    /// Reads the next response frame (for streamed answers such as copies).
    pub fn next_frame(&mut self) -> anyhow::Result<Response> {
        let frame = read_msg::<_, ServerResponse>(&mut self.reader).map_err(|e| {
            Disconnected(
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) {
                    format!("the daemon did not answer in time ({e})")
                } else {
                    format!("reading the daemon's answer: {e}")
                },
            )
        })?;
        match frame {
            Some(ServerResponse { reminders, resp }) => {
                self.reminders = reminders;
                match resp {
                    Response::Error { message } if message == STOPPING => {
                        Err(Disconnected(message).into())
                    }
                    Response::Error { message } => Err(anyhow::anyhow!(message)),
                    r => Ok(r),
                }
            }
            None => Err(Disconnected("the daemon closed the connection".into()).into()),
        }
    }

    /// Reads the raw bytes that follow a `Bytes` or `CopyEntry` frame.
    pub fn read_bytes(&mut self, len: u64) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0; len as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Copies the `len` raw bytes that follow a `Bytes` or `CopyEntry` frame into `w`, without
    /// holding them all in memory.
    pub fn copy_bytes(&mut self, len: u64, w: &mut dyn Write) -> anyhow::Result<()> {
        let mut copied = 0;
        self.copy_bytes_counted(len, w, &mut copied)
    }

    /// [`Client::copy_bytes`], adding to `copied` each byte written to `w` as it goes, so a
    /// caller can resume after a failed connection.
    pub fn copy_bytes_counted(
        &mut self,
        len: u64,
        w: &mut dyn Write,
        copied: &mut u64,
    ) -> anyhow::Result<()> {
        let mut buf = [0u8; 64 * 1024];
        let mut left = len;
        while left > 0 {
            let want = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
            let n = match self.reader.read(&mut buf[..want]) {
                Ok(0) => {
                    return Err(Disconnected(format!(
                        "the daemon closed the connection after {} of {len} bytes",
                        len - left
                    ))
                    .into());
                }
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Disconnected(format!("reading from the daemon: {e}")).into()),
            };
            w.write_all(&buf[..n])?;
            *copied += n as u64;
            left -= n as u64;
        }
        Ok(())
    }

    /// How long to wait for an answer (default [`READ_TIMEOUT`]).
    pub fn set_read_timeout(&mut self, timeout: Duration) -> anyhow::Result<()> {
        self.writer.set_read_timeout(Some(timeout))?;
        Ok(())
    }
}
