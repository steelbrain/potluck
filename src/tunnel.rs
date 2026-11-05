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
//!   repository if one of its jobs that the asker may run or runs names it, and calls
//!   [`serve_upload_pack`] through [`crate::daemon::GitTunnel`]; an unknown id gets
//!   [`refuse_upload_pack`], which git shows as `fatal: remote error: <message>`. `git
//!   upload-pack` runs in a process group of its own and is killed with its children if either
//!   side goes away; it is always reaped before the call returns.
//! - **What is served.** `git upload-pack` speaks protocol v0, whatever the client asks for, of a
//!   [`View`] of the mirror made for that one fetch, whose refs are those jobs' commits by id and
//!   the mirror's branches and tags that they reach. A fetch can want only those refs' tips
//!   (`allowReachableSHA1InWant` would also take any tree or blob by its id, and v2 any object at
//!   all). The view reads the mirror's objects, all of them, so what the client sends goes through
//!   a [`Filter`] first: a `have` or `shallow` line naming another object would tell the client
//!   whether the mirror has it, and a thin pack could come as deltas against it.

use crate::config::{NodeConfig, Paths};
use crate::mesh::{Mesh, MeshConfig, Stream};
use anyhow::{Context, bail};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
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

/// The refs of a [`View`] that name its commits: `refs/potluck/commits/<sha>`.
const VIEW_REFS: &str = "refs/potluck/commits";

/// An object id, as bytes: SHA-1's 20, or SHA-256's 32.
type Oid = [u8; 32];

/// The object id that `hex` starts with, `len` hex digits in either case, as git reads one
/// (`get_oid_hex`: whatever follows is left alone). `None` if it doesn't start with one.
fn parse_oid(hex: &[u8], len: usize) -> Option<Oid> {
    let digits = hex.get(..len)?;
    let mut oid = [0u8; 32];
    for (byte, pair) in oid.iter_mut().zip(digits.chunks(2)) {
        let hi = char::from(pair[0]).to_digit(16)?;
        let lo = char::from(pair[1]).to_digit(16)?;
        *byte = (hi << 4 | lo) as u8;
    }
    Some(oid)
}

/// `oid` as `len` lowercase hex digits.
fn oid_hex(oid: &Oid, len: usize) -> String {
    oid[..len / 2].iter().map(|b| format!("{b:02x}")).collect()
}

/// A git dir that serves some commits of a repository and what they reach, for
/// [`serve_upload_pack`]. Its refs are those commits (`refs/potluck/commits/<sha>`) and the
/// repository's branches and tags whose tips they reach (an annotated tag's own object is its
/// ref's tip), and it reads the repository's objects through `objects/info/alternates`, so nothing
/// is copied and nothing is written into the repository. A fetch from it gets those refs and what
/// they reach, and no other branch, tag or object of the repository. Removed with everything in
/// it when dropped.
#[derive(Debug)]
pub struct View {
    dir: PathBuf,
    /// The objects its refs name: all that a fetch may want.
    tips: HashSet<Oid>,
    /// Hex digits in an object id: 40 (SHA-1) or 64 (SHA-256).
    hex_len: usize,
}

impl View {
    /// Makes a view at `dir` (which must not exist) of `commits` of the repository at `git_dir`.
    /// Only full shas of commits the repository has count.
    pub fn create(
        dir: PathBuf,
        git_dir: &Path,
        commits: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> io::Result<View> {
        let objects = git_dir.join("objects").canonicalize().map_err(|e| {
            io::Error::new(e.kind(), format!("can't read {}: {e}", git_dir.display()))
        })?;
        let alternate = objects
            .to_str()
            .filter(|s| !s.contains('\n'))
            .ok_or_else(|| {
                io::Error::other(format!(
                    "can't serve {}: a path git can't read",
                    objects.display()
                ))
            })?;
        let asked: BTreeSet<String> = commits
            .into_iter()
            .map(|c| c.as_ref().to_ascii_lowercase())
            .filter(|c| {
                (c.len() == 40 || c.len() == 64) && c.bytes().all(|b| b.is_ascii_hexdigit())
            })
            .collect();
        let commits = existing_commits(git_dir, &asked)?;
        let mut refs: Vec<(String, String)> = commits
            .iter()
            .map(|c| (format!("{VIEW_REFS}/{c}"), c.clone()))
            .collect();
        if !commits.is_empty() {
            refs.extend(reached_refs(git_dir, &commits)?);
        }
        // `packed-refs` must be sorted by name, as git compares them (bytes).
        refs.sort();
        let sha256 = commits.first().is_some_and(|c| c.len() == 64);
        let hex_len = if sha256 { 64 } else { 40 };
        let tips = refs
            .iter()
            .filter_map(|(_, oid)| parse_oid(oid.as_bytes(), hex_len))
            .collect();
        // The repository's own default branch, if it names one.
        let head = std::fs::read_to_string(git_dir.join("HEAD"))
            .ok()
            .filter(|h| h.starts_with("ref: refs/heads/") && h.lines().count() == 1)
            .unwrap_or_else(|| "ref: refs/heads/main\n".into());

        if let Some(parent) = dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::create_dir(&dir)?;
        let view = View { dir, tips, hex_len };
        let d = &view.dir;
        std::fs::create_dir_all(d.join("objects/info"))?;
        std::fs::create_dir_all(d.join("refs"))?;
        std::fs::write(d.join("HEAD"), head)?;
        let config = if sha256 {
            "[core]\n\trepositoryformatversion = 1\n\tbare = true\n[extensions]\n\tobjectformat = sha256\n"
        } else {
            "[core]\n\trepositoryformatversion = 0\n\tbare = true\n"
        };
        std::fs::write(d.join("config"), config)?;
        std::fs::write(d.join("objects/info/alternates"), format!("{alternate}\n"))?;
        // A shallow repository's boundary, so that git doesn't look for the parents it lacks.
        if let Ok(shallow) = std::fs::read(git_dir.join("shallow")) {
            std::fs::write(d.join("shallow"), shallow)?;
        }
        // One file, whatever the names: loose refs that differ only in case would be one file on
        // a filesystem that ignores case.
        let mut packed = String::from("# pack-refs with: sorted \n");
        for (name, oid) in &refs {
            packed.push_str(&format!("{oid} {name}\n"));
        }
        std::fs::write(d.join("packed-refs"), packed)?;
        Ok(view)
    }

    /// The view's git dir.
    pub fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for View {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Those of `commits` that are commits in the repository at `git_dir` (`git cat-file
/// --batch-check`).
fn existing_commits(git_dir: &Path, commits: &BTreeSet<String>) -> io::Result<Vec<String>> {
    if commits.is_empty() {
        return Ok(Vec::new());
    }
    let mut child = crate::gitinfo::git(git_dir)
        .args(["cat-file", "--batch-check=%(objectname) %(objecttype)"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let input: String = commits.iter().map(|c| format!("{c}\n")).collect();
    let writer = thread::spawn(move || stdin.write_all(input.as_bytes()));
    let out = child.wait_with_output()?;
    let _ = writer.join();
    if !out.status.success() {
        return Err(io::Error::other(format!(
            "git cat-file in {}: {}",
            git_dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.strip_suffix(" commit"))
        .filter(|c| commits.contains(*c))
        .map(str::to_string)
        .collect())
}

/// The branches and tags of the repository at `git_dir` whose tips one of `commits` reaches (a
/// tag's peeled), as (name, object): `git for-each-ref --merged` of each, which counts a ref
/// any one of them reaches.
fn reached_refs(git_dir: &Path, commits: &[String]) -> io::Result<Vec<(String, String)>> {
    let mut cmd = crate::gitinfo::git(git_dir);
    cmd.args(["for-each-ref", "--format=%(objectname) %(refname)"]);
    cmd.args(commits.iter().map(|c| format!("--merged={c}")));
    cmd.args(["refs/heads/", "refs/tags/"]);
    let out = crate::gitinfo::output(&mut cmd).map_err(|e| io::Error::other(format!("{e:#}")))?;
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.split_once(' '))
        .map(|(oid, name)| (name.to_string(), oid.to_string()))
        .collect())
}

/// The longest pkt-line git sends (`LARGE_PACKET_MAX`), its 4-digit length included.
const MAX_PKT: usize = 65520;

/// What a client may tell `git upload-pack` serving a [`View`], read as git's pkt-lines
/// (protocol v0) and passed on line by line. A line that names an object passes only if the
/// client may fetch that object, so nothing upload-pack answers, and no pack it sends, depends on
/// an object outside the view, nor on whether the repository has one:
/// - `want <id>`: only an object one of the view's refs names. Any other refuses the fetch with
///   git's own error, `upload-pack: not our ref <id>` (upload-pack would refuse it too, but sooner
///   for an object the repository lacks than for one it has).
/// - `have <id>` and `shallow <id>`: only a commit the view's refs reach (`git rev-list --all` in
///   the view, the first time a line needs it); any other line is left out, so upload-pack never
///   sees it, whether the repository has the object or not. It answers as for a client that
///   doesn't have that object, which it can't then use as a delta base either.
/// - Any other line names no object (`deepen-not` names a ref, of the view's) and passes as it is.
///   `filter sparse:oid=<blob>` would read a blob: upload-pack runs with that filter banned.
///
/// A line that isn't a pkt-line, or a `want`, `have` or `shallow` without an object id after it,
/// refuses the fetch.
struct Filter {
    view: PathBuf,
    tips: HashSet<Oid>,
    hex_len: usize,
    /// The commits the view's refs reach, once a line needs them.
    reached: Option<HashSet<Oid>>,
    /// What came of a line that hasn't all come yet.
    pending: Vec<u8>,
}

/// What [`Filter`] does with a line.
enum Pass {
    /// Passes as it came.
    Same,
    /// Passes as this line, the object id written as upload-pack reads it.
    As(String),
    /// Is left out.
    Drop,
}

impl Filter {
    fn new(view: &View) -> Filter {
        Filter {
            view: view.dir.clone(),
            tips: view.tips.clone(),
            hex_len: view.hex_len,
            reached: None,
            pending: Vec::new(),
        }
    }

    /// Takes `bytes` from the client: what passes, as pkt-lines, or why the fetch is refused.
    fn feed(&mut self, bytes: &[u8]) -> Result<Vec<u8>, String> {
        let mut pending = std::mem::take(&mut self.pending);
        pending.extend_from_slice(bytes);
        let mut out = Vec::new();
        let mut at = 0;
        while let Some(head) = pending.get(at..at + 4) {
            let len = std::str::from_utf8(head)
                .ok()
                .filter(|h| h.bytes().all(|b| b.is_ascii_hexdigit()))
                .and_then(|h| usize::from_str_radix(h, 16).ok())
                .ok_or_else(|| {
                    format!(
                        "protocol error: bad pkt-line length {:?}",
                        String::from_utf8_lossy(head)
                    )
                })?;
            // A flush, a delimiter or a response end: no line.
            if len <= 2 {
                out.extend_from_slice(head);
                at += 4;
                continue;
            }
            if !(4..=MAX_PKT).contains(&len) {
                return Err(format!("protocol error: bad pkt-line length {len}"));
            }
            let Some(packet) = pending.get(at..at + len) else {
                break;
            };
            match self.line(&packet[4..])? {
                Pass::Same => out.extend_from_slice(packet),
                Pass::As(line) => {
                    out.extend_from_slice(format!("{:04x}{line}", line.len() + 4).as_bytes())
                }
                Pass::Drop => {}
            }
            at += len;
        }
        pending.drain(..at);
        self.pending = pending;
        Ok(out)
    }

    fn line(&mut self, line: &[u8]) -> Result<Pass, String> {
        let (kind, rest) = if let Some(rest) = line.strip_prefix(b"want ") {
            ("want", rest)
        } else if let Some(rest) = line.strip_prefix(b"have ") {
            ("have", rest)
        } else if let Some(rest) = line.strip_prefix(b"shallow ") {
            ("shallow", rest)
        } else {
            return Ok(Pass::Same);
        };
        let oid = parse_oid(rest, self.hex_len)
            .ok_or_else(|| format!("protocol error: no object id in a {kind} line"))?;
        if kind == "want" {
            return match self.tips.contains(&oid) {
                // upload-pack reads the same id from it, and the capabilities after it.
                true => Ok(Pass::Same),
                false => Err(format!(
                    "upload-pack: not our ref {}",
                    oid_hex(&oid, self.hex_len)
                )),
            };
        }
        Ok(match self.reached()?.contains(&oid) {
            true => Pass::As(format!("{kind} {}\n", oid_hex(&oid, self.hex_len))),
            false => Pass::Drop,
        })
    }

    /// The commits the view's refs reach.
    fn reached(&mut self) -> Result<&HashSet<Oid>, String> {
        if self.reached.is_none() {
            let commits = rev_list_all(&self.view, self.hex_len).map_err(|e| {
                format!("can't list the commits {} serves: {e}", self.view.display())
            })?;
            self.reached = Some(commits);
        }
        Ok(self.reached.as_ref().expect("set above"))
    }
}

/// `git rev-list --all` in the git dir at `dir`: every commit its refs reach.
fn rev_list_all(dir: &Path, hex_len: usize) -> io::Result<HashSet<Oid>> {
    let mut child = crate::gitinfo::git(dir)
        .args(["rev-list", "--all"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stderr = child.stderr.take().expect("piped stderr");
    let err = thread::spawn(move || read_capped(stderr, MAX_STDERR));
    let mut commits = HashSet::new();
    let mut bad = None;
    // Read to the end, or until reading fails, before the wait: the pipe is closed by then.
    let read: io::Result<()> = BufReader::new(child.stdout.take().expect("piped stdout"))
        .split(b'\n')
        .try_for_each(|line| {
            let line = line?;
            match parse_oid(&line, hex_len).filter(|_| line.len() == hex_len) {
                Some(oid) => {
                    commits.insert(oid);
                }
                None => bad = Some(String::from_utf8_lossy(&line).into_owned()),
            }
            Ok(())
        });
    let status = child.wait()?;
    let err = err.join().unwrap_or_default();
    if !status.success() {
        return Err(io::Error::other(format!(
            "git rev-list {status}: {}",
            err.trim()
        )));
    }
    read?;
    if let Some(line) = bad {
        return Err(io::Error::other(format!("git rev-list printed {line:?}")));
    }
    Ok(commits)
}

/// Runs `git upload-pack` on `view` with its stdin and stdout piped through `stream`, what comes
/// in through a [`Filter`], until the fetch is done or either side goes away, for the daemon's
/// [`crate::mesh::Handler::stream`]. The client can want only the tips of the view's refs: every
/// `uploadpack.allow*SHA1InWant` is off, on the command line, after every config file, so no
/// config can let more through.
///
/// The process (and anything it started) is killed if the stream breaks or stops taking bytes
/// for five minutes, or the filter refuses the fetch, and is reaped before this returns. If `git
/// upload-pack` fails before it sends anything, the client gets its error as a git error packet,
/// as it gets the filter's refusal. `Err` when the stream broke, the filter refused, or `git
/// upload-pack` couldn't run or failed (with its stderr).
pub fn serve_upload_pack(mut stream: Stream, view: &View) -> io::Result<()> {
    // Before anything is started: these fail on a socket the other side has reset (macOS
    // refuses setsockopt then), and returning after the spawn would leave the child unreaped.
    let reader = stream.try_clone()?;
    reader.set_read_timeout(Some(POLL))?;
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
    let git_dir = view.path();
    let filter = Filter::new(view);
    let mut cmd = crate::gitinfo::git(git_dir);
    cmd.args([
        "-c",
        "uploadpack.allowAnySHA1InWant=false",
        "-c",
        "uploadpack.allowReachableSHA1InWant=false",
        "-c",
        "uploadpack.allowTipSHA1InWant=false",
        // A `sparse:oid` filter reads the blob it names, any blob of the repository.
        "-c",
        "uploadpackfilter.sparse:oid.allow=false",
    ]);
    cmd.args(["upload-pack", &format!("--timeout={UPLOAD_PACK_TIMEOUT_S}")])
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
    let refused = Arc::new(Mutex::new(None));
    let inbound = {
        let (proc, state, refused) = (proc.clone(), state.clone(), refused.clone());
        thread::spawn(move || pump_in(reader, stdin, filter, &proc, &state, &refused))
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
    // Set before the filter killed the process, so before its output ended.
    let refusal = refused.lock().unwrap_or_else(|e| e.into_inner()).take();
    let failed = match (&refusal, &status) {
        (Some(r), _) => Some(format!("refused the fetch: {r}")),
        (None, Ok(s)) if s.success() => None,
        (None, Ok(s)) => Some(format!("git upload-pack {}: {}", s, stderr.trim())),
        (None, Err(e)) => Some(format!("git upload-pack: {e}")),
    };
    if out.is_ok() {
        if let Some(r) = &refusal {
            let _ = stream.write_all(&err_pkt(r));
        } else if sent == 0
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

/// Copies what the client sends to `git upload-pack`'s stdin, through `filter`, and closes that
/// when the client closes. If the client goes away, kills the process; if the filter refuses the
/// fetch, says why in `refused` and kills the process, and takes in the rest unread.
fn pump_in(
    mut reader: Stream,
    stdin: ChildStdin,
    mut filter: Filter,
    proc: &Proc,
    state: &AtomicU8,
    refused: &Mutex<Option<String>>,
) -> io::Result<()> {
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
                if let Some(s) = stdin.as_mut() {
                    match filter.feed(&buf[..n]) {
                        Ok(pass) => {
                            // It stopped reading (it exited, or is sending its pack): drop the
                            // rest.
                            if s.write_all(&pass).is_err() {
                                stdin = None;
                            }
                        }
                        Err(why) => {
                            *refused.lock().unwrap_or_else(|e| e.into_inner()) = Some(why);
                            proc.kill();
                            stdin = None;
                        }
                    }
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
    fn views() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("r");
        let git = |args: &[&str]| -> String {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(["-c", "user.name=t", "-c", "user.email=t@example.invalid"])
                .args(args)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .output()
                .unwrap();
            assert!(out.status.success(), "{args:?}: {out:?}");
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        std::fs::create_dir_all(&repo).unwrap();
        git(&["init", "-q", "-b", "trunk"]);
        git(&["commit", "-q", "--allow-empty", "-m", "base"]);
        let base = git(&["rev-parse", "HEAD"]);
        git(&["tag", "-a", "-m", "on base", "v0"]);
        git(&["checkout", "-q", "-b", "x"]);
        git(&["commit", "-q", "--allow-empty", "-m", "a"]);
        let a = git(&["rev-parse", "HEAD"]);
        git(&["checkout", "-q", "-b", "y", "trunk"]);
        git(&["commit", "-q", "--allow-empty", "-m", "b"]);
        let b = git(&["rev-parse", "HEAD"]);
        let tree = git(&["rev-parse", "HEAD^{tree}"]);
        let v0 = git(&["rev-parse", "v0"]);
        let git_dir = repo.join(".git");
        let read = |view: &View, f: &str| std::fs::read_to_string(view.path().join(f)).unwrap();

        // Only commits it has count, by their full ids, whatever their case.
        let at = tmp.path().join("views/1.git");
        let missing = "0".repeat(40);
        let asked = [
            a.to_uppercase(),
            missing,
            tree,
            "HEAD".into(),
            a[..39].into(),
        ];
        let view = View::create(at.clone(), &git_dir, &asked).unwrap();
        // With the branches and tags a reaches.
        assert_eq!(
            read(&view, "packed-refs"),
            format!(
                "# pack-refs with: sorted \n{base} refs/heads/trunk\n{a} refs/heads/x\n\
                 {a} {VIEW_REFS}/{a}\n{v0} refs/tags/v0\n"
            )
        );
        assert!(read(&view, "config").contains("repositoryformatversion = 0"));
        let objects = git_dir.join("objects").canonicalize().unwrap();
        assert_eq!(
            read(&view, "objects/info/alternates"),
            format!("{}\n", objects.display())
        );
        assert_eq!(read(&view, "HEAD"), "ref: refs/heads/y\n");
        // What a fetch may want: the objects its refs name, the tag's own included.
        let oids = |ids: &[&String]| -> HashSet<Oid> {
            ids.iter()
                .map(|id| parse_oid(id.as_bytes(), 40).unwrap())
                .collect()
        };
        assert_eq!(view.tips, oids(&[&base, &a, &v0]));
        // And the commits those reach.
        assert_eq!(rev_list_all(view.path(), 40).unwrap(), oids(&[&base, &a]));
        // Never made over another.
        assert!(View::create(at.clone(), &git_dir, [&a]).is_err());
        drop(view);
        assert!(!at.exists());

        // Each ref any of them reaches; none, and no ref but its own, for none.
        let view = View::create(tmp.path().join("views/2.git"), &git_dir, [&a, &b]).unwrap();
        let names: Vec<String> = read(&view, "packed-refs")
            .lines()
            .skip(1)
            .map(|l| l.split(' ').nth(1).unwrap().to_string())
            .collect();
        let mut want = vec![
            "refs/heads/trunk".to_string(),
            "refs/heads/x".into(),
            "refs/heads/y".into(),
            format!("{VIEW_REFS}/{a}"),
            format!("{VIEW_REFS}/{b}"),
            "refs/tags/v0".into(),
        ];
        want.sort();
        assert_eq!(names, want);
        let view = View::create(tmp.path().join("views/3.git"), &git_dir, ["nope"]).unwrap();
        assert_eq!(read(&view, "packed-refs"), "# pack-refs with: sorted \n");

        let gone = View::create(
            tmp.path().join("views/4.git"),
            &tmp.path().join("gone"),
            [&a],
        );
        assert!(gone.unwrap_err().to_string().contains("can't read"));
        assert!(!tmp.path().join("views/4.git").exists());
    }

    #[test]
    fn the_filter_passes_only_lines_that_name_what_the_view_serves() {
        let id = |c: char| c.to_string().repeat(40);
        let (tip, reached, other) = (id('a'), id('b'), id('c'));
        let oid = |h: &str| parse_oid(h.as_bytes(), 40).unwrap();
        let filter = || Filter {
            view: PathBuf::from("/nonexistent"),
            tips: HashSet::from([oid(&tip)]),
            hex_len: 40,
            reached: Some(HashSet::from([oid(&tip), oid(&reached)])),
            pending: Vec::new(),
        };
        let pkt = |l: &str| format!("{:04x}{l}", l.len() + 4);
        let passed = |f: &mut Filter, input: &str| {
            String::from_utf8(f.feed(input.as_bytes()).unwrap()).unwrap()
        };

        // A want of a tip, capabilities and all, as it came, in pieces as they come.
        let mut f = filter();
        let input = format!(
            "{}0000",
            pkt(&format!("want {} thin-pack\n", tip.to_uppercase()))
        );
        let (one, two) = input.split_at(7);
        assert_eq!(passed(&mut f, one), "");
        assert_eq!(passed(&mut f, two), input);
        // A have or shallow of a commit it reaches, its id as git reads it; of any other, nothing.
        let input = [
            pkt(&format!("have {}{}\n", reached.to_uppercase(), id('d'))),
            pkt(&format!("have {other}\n")),
            pkt(&format!("shallow {other}\n")),
            pkt(&format!("shallow {tip}")),
            "0000".into(),
            pkt("done\n"),
        ]
        .concat();
        let want = [
            pkt(&format!("have {reached}\n")),
            pkt(&format!("shallow {tip}\n")),
            "0000".into(),
            pkt("done\n"),
        ]
        .concat();
        assert_eq!(passed(&mut f, &input), want);
        // Lines that name no object pass as they are.
        let input = [
            pkt("deepen 1\n"),
            pkt("deepen-not refs/heads/main\n"),
            pkt("filter blob:none\n"),
            "0001".into(),
            pkt(&"x".repeat(MAX_PKT - 4)),
        ]
        .concat();
        assert_eq!(passed(&mut f, &input), input);

        // Anything else refuses the fetch.
        for (input, why) in [
            (
                pkt(&format!("want {reached}\n")),
                format!("upload-pack: not our ref {reached}"),
            ),
            (
                pkt(&format!("want {other} thin-pack\n")),
                format!("upload-pack: not our ref {other}"),
            ),
            (
                pkt(&format!("have {}\n", &reached[..39])),
                "protocol error: no object id in a have line".into(),
            ),
            (
                pkt("shallow \n"),
                "protocol error: no object id in a shallow line".into(),
            ),
            (
                "zzzz".into(),
                "protocol error: bad pkt-line length \"zzzz\"".into(),
            ),
            (
                "0003".into(),
                "protocol error: bad pkt-line length 3".into(),
            ),
            (
                "fff1".into(),
                "protocol error: bad pkt-line length 65521".into(),
            ),
        ] {
            assert_eq!(filter().feed(input.as_bytes()), Err(why), "{input}");
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
