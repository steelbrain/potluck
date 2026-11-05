//! Files that git itself calls modified in a tree a checkout has just written.
//!
//! A checkout writes each blob through the smudge side of git's filters: end-of-line conversion
//! (the `text`, `eol` and `crlf` attributes, `core.autocrlf`, `core.eol`), `ident`,
//! `working-tree-encoding` and filter drivers. `git status` compares a file through the clean side.
//! When a commit holds a blob that its own attributes would store differently (a CRLF file
//! committed before `* text` or `*.txt eol=lf` was added, say), the round trip doesn't come back
//! to the blob, and every checkout of that commit, on any machine, shows the file as modified once
//! git rereads it (as it does for a file written in the same second as the index). That is the
//! commit's doing, not damage: the tree holds what a checkout of the commit writes.
//!
//! So a seeded tree that `git status` calls modified counts as clean when every modified file
//! holds exactly what a fresh checkout of its index entry writes (the blob through the same
//! filters, from `git cat-file --filters`, compared a piece at a time: nothing is written anywhere,
//! and a big file costs no more memory than a small one), and the blob it came from is intact (its
//! content hashes to its id: a damaged object would be written the same way both times). Anything
//! else (a file that differs from that checkout, one deleted or added, a symbolic link, a name
//! that isn't UTF-8) still counts against the tree, and the base and mirror are blamed. Only files git converts are compared (a converting attribute
//! set, or `core.autocrlf` on): any other file git calls modified is.
//!
//! Dispatch uses the same rule for the caller's tree, where a file may also hold its blob's bytes
//! exactly, as written and committed before the attributes came: that is committed work too.

use super::git::{tree_bytes, tree_git, tree_text};
use anyhow::{Context, bail};
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::Stdio;

/// Attributes under which a checkout may write a file other than its blob, or `git status` read
/// one back differently. A file with none of them set (and `core.autocrlf` off) is written as its
/// blob and read back as it is: git calls it modified only when it is.
const CONVERTING: &[&str] = &[
    "text",
    "eol",
    "crlf",
    "ident",
    "filter",
    "working-tree-encoding",
];

/// Of the `git status` lines (`XY path`) of the repository at `dir`, those a fresh checkout of its
/// index doesn't explain. With `as_committed`, a file that holds its blob's bytes exactly (as a
/// caller's own tree may: written and committed, never checked out through the filters since)
/// is explained too.
pub(crate) fn unexplained(
    dir: &Path,
    lines: Vec<String>,
    as_committed: bool,
) -> anyhow::Result<Vec<String>> {
    let mut left = Vec::new();
    let mut candidates = Vec::new();
    for line in lines {
        // Modified in the tree only, with the path as git gave it (no replacement characters
        // from a name that isn't UTF-8).
        if &line[..2] == " M" && !line[3..].contains('\u{fffd}') {
            candidates.push(line);
        } else {
            left.push(line);
        }
    }
    if candidates.is_empty() {
        return Ok(left);
    }
    let converted = converted(dir, candidates.iter().map(|l| &l[3..]))?;
    let (candidates, plain): (Vec<String>, Vec<String>) = candidates
        .into_iter()
        .partition(|l| converted.contains(&l[3..]));
    left.extend(plain);
    if candidates.is_empty() {
        return Ok(left);
    }
    let index = regular_files(dir)?;
    let mut entries = Vec::new();
    for line in candidates {
        match index.get(&line[3..]) {
            Some(oid) => {
                let oid = oid.clone();
                entries.push((line, oid));
            }
            None => left.push(line),
        }
    }
    let blobs = read_blobs(dir, &entries)?;
    let mut to_check = Vec::new();
    for ((line, oid), blob) in entries.into_iter().zip(blobs) {
        if !blob.intact {
            left.push(line);
        } else if !(as_committed && blob.in_tree) {
            to_check.push((line, oid));
        }
    }
    if to_check.is_empty() {
        return Ok(left);
    }
    let same = as_checked_out(dir, &to_check)?;
    for ((line, _), same) in to_check.into_iter().zip(same) {
        if !same {
            left.push(line);
        }
    }
    Ok(left)
}

/// Of `paths`, those git converts on checkout or status: any of [`CONVERTING`] set for them
/// (`git check-attr`), or all of them when `core.autocrlf` is on.
pub(super) fn converted<'a>(
    dir: &Path,
    paths: impl Iterator<Item = &'a str>,
) -> anyhow::Result<HashSet<String>> {
    let paths: Vec<&str> = paths.collect();
    // Exits 1 when it isn't set.
    let autocrlf = tree_text(dir, ["config", "--get", "core.autocrlf"]).unwrap_or_default();
    if matches!(
        autocrlf.to_ascii_lowercase().as_str(),
        "true" | "yes" | "on" | "1" | "input"
    ) {
        return Ok(paths.iter().map(|p| p.to_string()).collect());
    }
    Ok(attributes(dir, paths.into_iter())?
        .into_iter()
        .filter(|(_, attrs)| attrs.iter().any(|(_, value)| value != "unset"))
        .map(|(path, _)| path)
        .collect())
}

/// The [`CONVERTING`] attributes that the tree at `dir` sets or unsets for each of `paths`
/// (`git check-attr`), as (attribute, value) pairs; a path with none of them is left out.
pub(super) fn attributes<'a>(
    dir: &Path,
    paths: impl Iterator<Item = &'a str>,
) -> anyhow::Result<HashMap<String, Vec<(String, String)>>> {
    let mut input = Vec::new();
    for p in paths {
        input.extend_from_slice(p.as_bytes());
        input.push(0);
    }
    if input.is_empty() {
        return Ok(HashMap::new());
    }
    let mut cmd = tree_git(dir);
    cmd.args(["check-attr", "-z", "--stdin"]).args(CONVERTING);
    let out = run_with_input(cmd, input)?;
    // "<path> NUL <attribute> NUL <value> NUL", for each path and attribute.
    let fields: Vec<&[u8]> = out.split(|b| *b == 0).collect();
    let mut set: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for [path, attribute, value] in fields.as_chunks::<3>().0 {
        if *value != b"unspecified" {
            set.entry(String::from_utf8_lossy(path).into_owned())
                .or_default()
                .push((
                    String::from_utf8_lossy(attribute).into_owned(),
                    String::from_utf8_lossy(value).into_owned(),
                ));
        }
    }
    Ok(set)
}

/// Runs `cmd` with `input` on its stdin; its stdout, or `Err` with its stderr.
pub(super) fn run_with_input(
    mut cmd: std::process::Command,
    input: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("can't run {}", crate::gitinfo::describe(&cmd)))?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(&input);
    });
    let mut stderr = child.stderr.take().expect("piped stderr");
    let errs = std::thread::spawn(move || {
        let mut err = Vec::new();
        let _ = stderr.read_to_end(&mut err);
        err
    });
    let mut out = Vec::new();
    child
        .stdout
        .take()
        .expect("piped stdout")
        .read_to_end(&mut out)?;
    let _ = writer.join();
    let err = errs.join().unwrap_or_default();
    let status = child.wait()?;
    if !status.success() {
        bail!(
            "{} failed ({status}): {}",
            crate::gitinfo::describe(&cmd),
            String::from_utf8_lossy(&err).trim()
        );
    }
    Ok(out)
}

/// The index's regular files (modes 100644 and 100755): path -> blob id.
pub(super) fn regular_files(dir: &Path) -> anyhow::Result<HashMap<String, String>> {
    let raw = tree_bytes(dir, ["ls-files", "--stage", "-z"])?;
    let mut out = HashMap::new();
    for rec in raw.split(|b| *b == 0).filter(|r| !r.is_empty()) {
        // "<mode> SP <object> SP <stage> TAB <path>"
        let Some(tab) = rec.iter().position(|b| *b == b'\t') else {
            continue;
        };
        let meta = String::from_utf8_lossy(&rec[..tab]);
        let mut parts = meta.split(' ');
        let (Some(mode), Some(oid), Some("0")) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        if mode != "100644" && mode != "100755" {
            continue;
        }
        if let Ok(path) = std::str::from_utf8(&rec[tab + 1..]) {
            out.insert(path.to_string(), oid.to_string());
        }
    }
    Ok(out)
}

/// What [`read_blobs`] found of one blob.
struct Blob {
    /// The object's content hashes to its id (git doesn't check that when it reads an object).
    intact: bool,
    /// The tree's file holds exactly the blob's bytes.
    in_tree: bool,
}

/// How much of a blob or file is read at a time: a big file costs no more memory than this.
const PIECE: usize = 64 << 10;

/// Reads each entry's (status line, blob id) blob (`git cat-file --batch`) and compares it with
/// its id and with the file in the tree at `dir`, a piece at a time.
fn read_blobs(dir: &Path, entries: &[(String, String)]) -> anyhow::Result<Vec<Blob>> {
    let input: String = entries.iter().map(|(_, oid)| format!("{oid}\n")).collect();
    let mut blobs = Vec::with_capacity(entries.len());
    let mut k = 0;
    cat_file_batch(
        dir,
        &[],
        input,
        entries.len(),
        |out| {
            let (line, oid) = &entries[k];
            k += 1;
            read_blob(out, oid, &dir.join(&line[3..]))
        },
        |blob| blobs.push(blob),
    )?;
    Ok(blobs)
}

/// Whether each entry's (status line, blob id) file in the tree at `dir` holds exactly what a
/// checkout of the blob writes there: the blob through the smudge side of the path's attributes
/// and filters, as `git cat-file --filters` gives it, compared a piece at a time.
fn as_checked_out(dir: &Path, entries: &[(String, String)]) -> anyhow::Result<Vec<bool>> {
    let mut same = vec![false; entries.len()];
    // In its batch input git takes the path after the object id and the blanks that follow it, up
    // to a line feed (and a carriage return before it): a path that starts with a blank, holds a
    // line feed or ends with a carriage return is asked for on its own.
    let (batch, single): (Vec<usize>, Vec<usize>) = (0..entries.len()).partition(|&i| {
        let path = &entries[i].0[3..];
        !path.starts_with([' ', '\t']) && !path.contains('\n') && !path.ends_with('\r')
    });
    if !batch.is_empty() {
        // `git cat-file --batch --filters` heads each blob it converts with the size it has as
        // stored, not as converted: so each is followed by an object that can't exist (an id
        // drawn at random), whose "missing" line marks where the converted content ends.
        let marker = random_hex(entries[batch[0]].1.len())?;
        let end = format!("{marker} missing\n");
        let input: String = batch
            .iter()
            .map(|&i| format!("{} {}\n{marker} -\n", entries[i].1, &entries[i].0[3..]))
            .collect();
        let mut k = 0;
        cat_file_batch(
            dir,
            &["--filters"],
            input,
            batch.len(),
            |out| {
                let i = batch[k];
                k += 1;
                let file = SameAs::new(&dir.join(&entries[i].0[3..]), None);
                Ok((i, read_converted(out, end.as_bytes(), file)?))
            },
            |(i, s)| same[i] = s,
        )?;
    }
    for i in single {
        let (line, oid) = &entries[i];
        let mut path = OsString::from("--path=");
        path.push(&line[3..]);
        let mut child = tree_git(dir)
            .args([
                OsStr::new("cat-file"),
                OsStr::new("--filters"),
                &path,
                OsStr::new(oid),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("can't run git cat-file")?;
        let mut file = SameAs::new(&dir.join(&line[3..]), None);
        let mut out = child.stdout.take().expect("piped stdout");
        let mut piece = vec![0u8; PIECE];
        let read = loop {
            match out.read(&mut piece) {
                Ok(0) => break Ok(()),
                Ok(n) => file.feed(&piece[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => break Err(e),
            }
        };
        drop(out);
        let status = child.wait()?;
        read.context("reading git cat-file --filters")?;
        if !status.success() {
            bail!("git cat-file --filters failed ({status})");
        }
        same[i] = file.same();
    }
    Ok(same)
}

/// Compares bytes that arrive a piece at a time with a file's content.
struct SameAs {
    /// The file, while everything fed so far matched it.
    file: Option<std::fs::File>,
    buf: Vec<u8>,
}

impl SameAs {
    /// For the regular file at `path`; `len`, when known, is how many bytes will be fed (a file of
    /// another size is told apart without reading it).
    fn new(path: &Path, len: Option<u64>) -> SameAs {
        use std::os::unix::fs::OpenOptionsExt;
        // Opened without waiting: git calls a FIFO in a tracked file's place modified, and a
        // blocking open of it waits for a writer that never comes. Only a regular file is read
        // (O_NONBLOCK changes nothing for one).
        let file = std::fs::File::options()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .ok()
            .filter(|f| {
                f.metadata()
                    .is_ok_and(|m| m.is_file() && len.is_none_or(|len| m.len() == len))
            });
        SameAs {
            file,
            buf: Vec::new(),
        }
    }

    /// The next piece of the bytes.
    fn feed(&mut self, data: &[u8]) {
        let Some(f) = &mut self.file else {
            return;
        };
        self.buf.resize(data.len(), 0);
        if f.read_exact(&mut self.buf).is_err() || self.buf != data {
            self.file = None;
        }
    }

    /// Whether the file held exactly the bytes fed, and nothing more.
    fn same(mut self) -> bool {
        let Some(f) = &mut self.file else {
            return false;
        };
        let mut more = [0u8; 1];
        matches!(f.read(&mut more), Ok(0))
    }
}

/// `len` random hex digits.
fn random_hex(len: usize) -> anyhow::Result<String> {
    use ring::rand::SecureRandom;
    let mut bytes = vec![0u8; len.div_ceil(2)];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow::anyhow!("no random numbers from the system"))?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(hex[..len].to_string())
}

/// One answer of `git cat-file --batch --filters` followed by the `end` line of the missing marker
/// object: whether `file` holds exactly the converted blob (false for a missing object).
fn read_converted(out: &mut impl BufRead, end: &[u8], mut file: SameAs) -> anyhow::Result<bool> {
    let mut header = Vec::new();
    if out.read_until(b'\n', &mut header)? == 0 {
        bail!("git cat-file ended early");
    }
    if header.ends_with(b" missing\n") {
        // The marker's line follows at once.
        let mut line = Vec::new();
        loop {
            line.clear();
            if out.read_until(b'\n', &mut line)? == 0 {
                bail!("git cat-file ended early");
            }
            if line == end {
                return Ok(false);
            }
        }
    }
    // The content, then the line feed git writes after each blob, then the marker's line.
    let mut term = Vec::with_capacity(end.len() + 1);
    term.push(b'\n');
    term.extend_from_slice(end);
    read_through(out, &term, &mut |piece| file.feed(piece))?;
    Ok(file.same())
}

/// Reads `out` through the first `term`, handing what comes before it to `sink` a piece at a
/// time. Nothing after `term` is read.
fn read_through(
    out: &mut impl BufRead,
    term: &[u8],
    sink: &mut dyn FnMut(&[u8]),
) -> anyhow::Result<()> {
    // The end of what came so far that may be the start of `term`, and that with the next piece.
    let mut held: Vec<u8> = Vec::new();
    let mut window: Vec<u8> = Vec::new();
    loop {
        let piece = out.fill_buf()?;
        if piece.is_empty() {
            bail!("git cat-file ended early");
        }
        let n = piece.len();
        window.clear();
        window.extend_from_slice(&held);
        window.extend_from_slice(piece);
        if let Some(at) = find(&window, term) {
            sink(&window[..at]);
            out.consume(at + term.len() - held.len());
            return Ok(());
        }
        let keep = window.len().min(term.len() - 1);
        let cut = window.len() - keep;
        sink(&window[..cut]);
        held.clear();
        held.extend_from_slice(&window[cut..]);
        out.consume(n);
    }
}

/// Where `needle` (not empty) first occurs in `hay`.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    let mut from = 0;
    while let Some(i) = hay[from..].iter().position(|b| *b == needle[0]) {
        let at = from + i;
        if hay[at..].starts_with(needle) {
            return Some(at);
        }
        from = at + 1;
    }
    None
}

/// Runs `git cat-file --batch` (with `args`) in the repository at `dir` on `input` and hands each
/// of its `count` answers, as `read` takes them from its output, to `each` in order.
fn cat_file_batch<R>(
    dir: &Path,
    args: &[&str],
    input: String,
    count: usize,
    mut read: impl FnMut(&mut BufReader<std::process::ChildStdout>) -> anyhow::Result<R>,
    mut each: impl FnMut(R),
) -> anyhow::Result<()> {
    let mut child = tree_git(dir)
        .args(["cat-file", "--batch"])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("can't run git cat-file")?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(input.as_bytes());
    });
    let mut out = BufReader::with_capacity(PIECE, child.stdout.take().expect("piped stdout"));
    let mut failed = None;
    for _ in 0..count {
        match read(&mut out) {
            Ok(answer) => each(answer),
            Err(e) => {
                failed = Some(e);
                break;
            }
        }
    }
    drop(out);
    let _ = writer.join();
    let status = child.wait()?;
    if let Some(e) = failed {
        return Err(e.context("reading blobs with git cat-file --batch"));
    }
    if !status.success() {
        bail!("git cat-file --batch failed ({status})");
    }
    Ok(())
}

/// One answer of `git cat-file --batch`, read a piece at a time: whether it is a blob whose
/// content hashes to `oid`, and whether the file at `path` holds exactly that content. A missing
/// object, or one that isn't a blob, is neither.
fn read_blob(out: &mut impl BufRead, oid: &str, path: &Path) -> anyhow::Result<Blob> {
    let mut header = String::new();
    if out.read_line(&mut header)? == 0 {
        bail!("git cat-file ended early");
    }
    let parts: Vec<&str> = header.trim_end().split(' ').collect();
    let [_, kind, size] = parts.as_slice() else {
        return Ok(Blob {
            intact: false,
            in_tree: false,
        });
    };
    let size: u64 = size
        .parse()
        .with_context(|| format!("bad git cat-file header {header:?}"))?;
    let mut hash = BlobHash::new(oid, size);
    let mut file = SameAs::new(path, Some(size));
    let mut left = size;
    while left > 0 {
        let piece = out.fill_buf()?;
        if piece.is_empty() {
            bail!("git cat-file ended early");
        }
        let n = piece.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        hash.update(&piece[..n]);
        file.feed(&piece[..n]);
        out.consume(n);
        left -= n as u64;
    }
    let mut nl = [0u8; 1];
    out.read_exact(&mut nl)?;
    let blob = *kind == "blob";
    Ok(Blob {
        intact: blob && hash.is(oid),
        in_tree: blob && file.same(),
    })
}

/// A blob's id, computed from its content as it arrives: SHA-1, or SHA-256 for a repository in
/// that format (by the length of the id it is checked against).
struct BlobHash(Option<ring::digest::Context>);

impl BlobHash {
    /// For a blob of `len` bytes, to be checked against `oid`.
    fn new(oid: &str, len: u64) -> BlobHash {
        let algorithm = match oid.len() {
            40 => &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
            64 => &ring::digest::SHA256,
            _ => return BlobHash(None),
        };
        let mut ctx = ring::digest::Context::new(algorithm);
        ctx.update(format!("blob {len}\0").as_bytes());
        BlobHash(Some(ctx))
    }

    fn update(&mut self, data: &[u8]) {
        if let Some(ctx) = &mut self.0 {
            ctx.update(data);
        }
    }

    /// Whether the content hashes to `oid`.
    fn is(self, oid: &str) -> bool {
        let Some(ctx) = self.0 else {
            return false;
        };
        let hex: String = ctx
            .finish()
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        hex == oid.to_ascii_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::process::Command;

    fn git(dir: &Path, args: &[&str]) -> String {
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
            .args(["-c", "gc.auto=0"])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// `git status` lines, as seeding reads them.
    fn status(dir: &Path) -> Vec<String> {
        crate::workspace::seed::status(dir).unwrap()
    }

    /// Moves every file's mtime back a minute, so that git rereads them all (as it does for files
    /// written in the same second as the index).
    fn reread(dir: &Path) {
        let then = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).unwrap().flatten() {
                let p = e.path();
                if e.file_name() == ".git" {
                    continue;
                }
                if p.is_dir() {
                    stack.push(p);
                } else {
                    std::fs::File::options()
                        .write(true)
                        .open(&p)
                        .unwrap()
                        .set_modified(then)
                        .unwrap();
                }
            }
        }
    }

    /// A commit holding files its own attributes would store differently: each committed first
    /// without the attribute, which the next commit adds. Filter drivers and `core.autocrlf` are
    /// set in the repository's config, as the user's global config would set them. Returns a
    /// fresh checkout of it (a clone) after git has reread every file.
    fn fixture(root: &Path, autocrlf: bool) -> PathBuf {
        let src = root.join("src");
        std::fs::create_dir_all(&src).unwrap();
        git(&src, &["init", "-q"]);
        let files: &[(&str, &[u8])] = &[
            ("crlf.txt", b"a\r\nb\r\n"),
            ("lf.md", b"one\r\ntwo\r\n"),
            ("mixed.bat", b"x\r\ny\nz\r\n"),
            ("id.c", b"/* $Id: 0123456789abcdef $ */\n"),
            ("up.up", b"lower case\n"),
            ("trail.ws", b"trailing   \n"),
            ("plain.txt", b"plain\n"),
            ("notes", b"n1\nn2\n"),
        ];
        for (name, data) in files {
            std::fs::write(src.join(name), data).unwrap();
        }
        git(&src, &["add", "-A"]);
        git(&src, &["commit", "-q", "-m", "one"]);
        std::fs::write(
            src.join(".gitattributes"),
            "*.txt text\n*.md eol=lf\n*.bat eol=crlf\n*.c ident\n*.up filter=upper\n\
             *.ws filter=strip\n",
        )
        .unwrap();
        git(&src, &["add", ".gitattributes"]);
        git(&src, &["commit", "-q", "-m", "attributes"]);
        let co = root.join("co");
        git(
            root,
            &["clone", "-q", "--no-checkout", src.to_str().unwrap(), "co"],
        );
        git(&co, &["config", "filter.upper.smudge", "tr a-z A-Z"]);
        git(&co, &["config", "filter.upper.clean", "cat"]);
        git(&co, &["config", "filter.strip.clean", "sed -e 's/ *$//'"]);
        if autocrlf {
            git(&co, &["config", "core.autocrlf", "true"]);
        }
        git(&co, &["checkout", "-q", "main"]);
        reread(&co);
        co
    }

    #[test]
    fn files_git_itself_normalises_are_explained() {
        for autocrlf in [false, true] {
            let t = tempfile::tempdir().unwrap();
            let co = fixture(t.path(), autocrlf);
            let dirty = status(&co);
            let mut names: Vec<&str> = dirty.iter().map(|l| &l[3..]).collect();
            names.sort();
            // Git itself calls these modified in a checkout it has just written. (With autocrlf
            // the file the strip filter cleans ends its line with "\r\n", so "*$" finds no spaces.)
            let mut want = vec!["crlf.txt", "id.c", "lf.md", "mixed.bat", "up.up"];
            if !autocrlf {
                want.insert(4, "trail.ws");
            }
            assert_eq!(names, want, "autocrlf {autocrlf}: {dirty:?}");
            assert_eq!(
                unexplained(&co, dirty, false).unwrap(),
                Vec::<String>::new(),
                "autocrlf {autocrlf}"
            );
        }
    }

    #[test]
    fn anything_else_is_not() {
        let t = tempfile::tempdir().unwrap();
        let co = fixture(t.path(), false);
        // Same line endings, other content; a converted file changed; one gone; one new.
        std::fs::write(co.join("crlf.txt"), b"a\r\nB\r\n").unwrap();
        std::fs::write(co.join("mixed.bat"), b"x\r\ny\r\nZ\r\n").unwrap();
        std::fs::write(co.join("plain.txt"), b"plain, changed\n").unwrap();
        std::fs::remove_file(co.join("notes")).unwrap();
        std::fs::write(co.join("new"), b"new\n").unwrap();
        let left = unexplained(&co, status(&co), false).unwrap();
        let mut left: Vec<&str> = left.iter().map(String::as_str).collect();
        left.sort();
        assert_eq!(
            left,
            [
                " D notes",
                " M crlf.txt",
                " M mixed.bat",
                " M plain.txt",
                "?? new"
            ]
        );
    }

    /// A seeded tree must hold what a checkout writes; a caller's own tree may also hold a blob's
    /// bytes as they were committed.
    #[test]
    fn the_bytes_as_committed_count_only_when_asked() {
        let t = tempfile::tempdir().unwrap();
        let co = fixture(t.path(), false);
        std::fs::write(co.join("mixed.bat"), b"x\r\ny\nz\r\n").unwrap();
        std::fs::write(co.join("id.c"), b"/* $Id: 0123456789abcdef $ */\n").unwrap();
        let mut left = unexplained(&co, status(&co), false).unwrap();
        left.sort();
        assert_eq!(left, [" M id.c", " M mixed.bat"]);
        assert_eq!(
            unexplained(&co, status(&co), true).unwrap(),
            Vec::<String>::new()
        );
        // Other bytes are other work.
        std::fs::write(co.join("mixed.bat"), b"x\r\ny\nZ\r\n").unwrap();
        assert_eq!(
            unexplained(&co, status(&co), true).unwrap(),
            [" M mixed.bat"]
        );
    }

    /// Files without any converting attribute read back as they are: never checked out to compare.
    #[test]
    fn only_converted_files_are_compared() {
        let t = tempfile::tempdir().unwrap();
        let co = fixture(t.path(), false);
        let all = ["crlf.txt", "notes", "up.up", "plain.txt", "none.bin"];
        let mut got: Vec<String> = converted(&co, all.into_iter())
            .unwrap()
            .into_iter()
            .collect();
        got.sort();
        assert_eq!(got, ["crlf.txt", "plain.txt", "up.up"]);
        git(&co, &["config", "core.autocrlf", "input"]);
        assert_eq!(converted(&co, all.into_iter()).unwrap().len(), all.len());
    }

    /// A damaged object a checkout reads back without complaint (git doesn't check the hash of
    /// what it reads) is converted the same way for the comparison: its hash gives it away.
    #[test]
    fn a_damaged_blob_is_not() {
        let t = tempfile::tempdir().unwrap();
        let co = fixture(t.path(), false);
        let oid = git(&co, &["rev-parse", ":crlf.txt"]);
        // An object of the same type and size, stored under crlf.txt's id.
        let other = git(&co, &["hash-object", "-w", "--no-filters", "lf.md"]);
        assert_eq!(std::fs::metadata(co.join("lf.md")).unwrap().len(), 10);
        let bad = std::fs::read(co.join("lf.md")).unwrap();
        // A local clone keeps the source's loose objects (hardlinked).
        let objects = co.join(".git/objects");
        let path = |o: &str| objects.join(&o[..2]).join(&o[2..]);
        let loose = path(&oid);
        assert!(loose.is_file(), "{} is loose", loose.display());
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        std::fs::copy(path(&other), &loose).unwrap();
        assert_eq!(
            git(&co, &["cat-file", "blob", &oid]).as_bytes(),
            b"one\r\ntwo"
        );
        // The file as a checkout from the damaged object writes it.
        std::fs::write(co.join("crlf.txt"), &bad).unwrap();
        let left = unexplained(&co, status(&co), false).unwrap();
        assert_eq!(left, [" M crlf.txt"]);
    }

    /// Names git's batch input can't carry as they are (a leading blank, a line feed, a carriage
    /// return at the end) are compared too, each on its own.
    #[test]
    fn awkward_names_are_compared_too() {
        let t = tempfile::tempdir().unwrap();
        let src = t.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        git(&src, &["init", "-q"]);
        let names = [
            " lead.txt",
            "\tlead.txt",
            "line\nfeed.txt",
            "cr\r.txt",
            "two  spaces.txt",
        ];
        for n in names {
            std::fs::write(src.join(n), b"a\r\nb\r\n").unwrap();
        }
        git(&src, &["add", "-A"]);
        git(&src, &["commit", "-q", "-m", "one"]);
        std::fs::write(src.join(".gitattributes"), "*.txt text\n").unwrap();
        git(&src, &["add", ".gitattributes"]);
        git(&src, &["commit", "-q", "-m", "attributes"]);
        reread(&src);
        let dirty = status(&src);
        assert_eq!(dirty.len(), names.len(), "{dirty:?}");
        assert_eq!(
            unexplained(&src, dirty, false).unwrap(),
            Vec::<String>::new()
        );
        // Each still tells other bytes apart.
        for n in names {
            std::fs::write(src.join(n), b"a\r\nB\r\n").unwrap();
            assert_eq!(
                unexplained(&src, status(&src), false).unwrap(),
                [format!(" M {n}")],
                "{n:?}"
            );
            std::fs::write(src.join(n), b"a\r\nb\r\n").unwrap();
        }
    }

    #[test]
    fn hashes() {
        // `printf 'hello\n' | git hash-object --stdin`, and with --object-format=sha256.
        assert!(hashes_to(
            "ce013625030ba8dba906f756967f9e9ca394464a",
            b"hello\n"
        ));
        assert!(hashes_to(
            "2cf8d83d9ee29543b34a87727421fdecb7e3f3a183d337639025de576db9ebb4",
            b"hello\n"
        ));
        assert!(!hashes_to(
            "ce013625030ba8dba906f756967f9e9ca394464a",
            b"hello"
        ));
        assert!(!hashes_to("ce01", b"hello\n"));
        // Fed in pieces, it is the same id.
        let mut h = BlobHash::new("ce013625030ba8dba906f756967f9e9ca394464a", 6);
        for piece in [&b"hel"[..], b"", b"lo\n"] {
            h.update(piece);
        }
        assert!(h.is("ce013625030ba8dba906f756967f9e9ca394464a"));
    }

    fn hashes_to(oid: &str, data: &[u8]) -> bool {
        let mut h = BlobHash::new(oid, data.len() as u64);
        h.update(data);
        h.is(oid)
    }

    /// The content before the end marker comes out whole and nothing after it is read, wherever
    /// the reader's pieces split the content, the marker or a start of it that isn't one.
    #[test]
    fn reads_through_the_marker_and_no_further() {
        let term = b"\nabc missing\n";
        let tricky: &[u8] = b"x\nab\nabc\nabc missin\n\nabc missinG\nxabc missing";
        for content in [&b""[..], b"\n", b"plain", tricky, &[b'\n'; 40][..]] {
            for cap in [1, 2, 3, 5, 13, 64] {
                let mut stream = content.to_vec();
                stream.extend_from_slice(term);
                stream.extend_from_slice(b"next\n");
                let mut out = std::io::BufReader::with_capacity(cap, &stream[..]);
                let mut got = Vec::new();
                read_through(&mut out, term, &mut |p| got.extend_from_slice(p)).unwrap();
                assert_eq!(got, content, "cap {cap}");
                let mut rest = String::new();
                out.read_to_string(&mut rest).unwrap();
                assert_eq!(rest, "next\n", "cap {cap}");
            }
        }
        let mut out = std::io::BufReader::with_capacity(4, &b"no end here"[..]);
        assert!(read_through(&mut out, term, &mut |_| {}).is_err());
    }

    /// A FIFO in a tracked file's place (git calls it modified) is not that file, and looking at
    /// it doesn't wait for a writer that never comes.
    #[test]
    fn a_fifo_in_a_files_place_is_not_waited_on() {
        use std::os::unix::ffi::OsStringExt;
        let t = tempfile::tempdir().unwrap();
        let src = t.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        git(&src, &["init", "-q"]);
        std::fs::write(src.join(".gitattributes"), "* text=auto\n").unwrap();
        std::fs::write(src.join("f.txt"), b"abc\n").unwrap();
        std::fs::write(src.join("empty.txt"), b"").unwrap();
        git(&src, &["add", "-A"]);
        git(&src, &["commit", "-q", "-m", "one"]);
        for name in ["f.txt", "empty.txt"] {
            std::fs::remove_file(src.join(name)).unwrap();
            let path = std::ffi::CString::new(src.join(name).into_os_string().into_vec()).unwrap();
            // SAFETY: a valid NUL-terminated path.
            assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        }
        let dirty = status(&src);
        assert_eq!(dirty.len(), 2, "{dirty:?}");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(unexplained(&src, dirty, true).map(|mut l| {
                l.sort();
                l
            }));
        });
        let left = rx
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("still waiting on a FIFO after 20 s")
            .unwrap();
        assert_eq!(left, [" M empty.txt", " M f.txt"]);
    }

    /// A file is the same as what is fed only when it holds exactly that: not a byte more or less.
    #[test]
    fn compares_a_file_a_piece_at_a_time() {
        let t = tempfile::tempdir().unwrap();
        let f = t.path().join("f");
        std::fs::write(&f, b"hello world").unwrap();
        let same = |len: Option<u64>, pieces: &[&[u8]]| {
            let mut s = SameAs::new(&f, len);
            for p in pieces {
                s.feed(p);
            }
            s.same()
        };
        assert!(same(None, &[b"hello", b" ", b"world"]));
        assert!(same(Some(11), &[b"hello world"]));
        assert!(!same(Some(12), &[b"hello world"]));
        assert!(!same(None, &[b"hello"]));
        assert!(!same(None, &[b"hello world!"]));
        assert!(!same(None, &[b"hello", b"World"]));
        assert!(!SameAs::new(t.path(), None).same());
        assert!(!SameAs::new(&t.path().join("none"), None).same());
    }
}
