//! Files the daemon reads and copies: status files, `--keep` globs into the job's directory, and
//! `potluck cp` streams out of a workspace.
//!
//! Paths into a workspace never leave it:
//! - a path with `..` or an absolute path is refused;
//! - the path asked for may go through symlinks only if it resolves inside the workspace; so may
//!   a `--keep` glob's components before its first wildcard;
//! - inside a copied directory, and where a glob's wildcards match, symlinks are not followed as
//!   directories (no loops, no escapes): a symlink to a regular file inside the workspace is
//!   copied as that file; symlinks to directories, to anything outside the workspace, and
//!   dangling ones are skipped;
//! - only directories and regular files are copied (sockets, FIFOs and devices are skipped), and
//!   so are names that aren't UTF-8.
//!
//! A copy stream is a [`Response::CopyEntry`] per entry, in pre-order (a directory before its
//! contents, siblings by name), then [`Response::CopyDone`]. Entry paths are relative to the path
//! asked for, so its own entry has the empty path. A file whose size changes during the copy is
//! sent with the length it had when its entry was written: cut short, or padded with zeros.

use super::Logger;
use crate::proto::{Response, ServerResponse, write_msg};
use crate::types::{ExitStatus, Reminder};
use std::fs::{File, Metadata};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

/// In a job's directory: stdout and stderr.
pub(crate) const LOG: &str = "output.log";
/// In a job's directory: written by the supervisor when the command exits.
pub(crate) const STATUS: &str = "status.json";
/// In a job's directory: files matched by `--keep`, by their path in the workspace.
pub(crate) const KEEP: &str = "keep";
/// In a job's directory: crash reports of its processes.
pub(crate) const CRASH: &str = "crash";

pub(crate) fn read_status(path: &Path) -> Option<ExitStatus> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

/// Writes one answer to a client (or to a member that relayed its request), with `rem`. Job
/// records go without their handle's process lists ([`without_process_lists`]).
pub(crate) fn reply<W: Write>(w: &mut W, rem: &[Reminder], resp: Response) -> std::io::Result<()> {
    let mut resp = resp;
    match &mut resp {
        Response::Job { job } => without_process_lists(job),
        Response::Status { status } => status
            .running
            .iter_mut()
            .chain(status.queued.iter_mut())
            .chain(status.recent.iter_mut())
            .for_each(without_process_lists),
        _ => {}
    }
    write_msg(
        w,
        &ServerResponse {
            reminders: rem.to_vec(),
            resp,
        },
    )
}

/// A macOS handle tracks every process seen in the job, with when each went (up to 10,000,
/// about 670 KB of JSON for a running job): the daemon needs them, no command does. The handle
/// keeps the rest (the supervisor's pid and start time).
fn without_process_lists(job: &mut crate::types::JobRecord) {
    if let Some(crate::types::Handle::Process { tracked, gone, .. }) = &mut job.handle {
        tracked.clear();
        gone.clear();
    }
}

/// Checks a path relative to a workspace root without touching the disk: no absolute paths, no
/// `..`.
fn check_relative(rel: &str) -> Result<&Path, String> {
    let p = Path::new(rel);
    if p.is_absolute() {
        return Err(format!("{rel}: give a path relative to the workspace root"));
    }
    if p.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!("{rel}: paths with .. are refused"));
    }
    Ok(p)
}

/// Whether `--keep` glob `g` can match inside a workspace: relative, without `..`, and a valid
/// pattern. Checked at dispatch; [`keep`] skips what fails it.
pub(crate) fn check_keep(g: &str) -> Result<(), String> {
    check_relative(g)?;
    glob::Pattern::new(g).map_err(|e| format!("{g}: bad pattern: {e}"))?;
    Ok(())
}

/// Resolves `rel` inside `root`, refusing anything that leads outside it.
pub(crate) fn resolve(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let p = check_relative(rel)?;
    let root = root
        .canonicalize()
        .map_err(|e| format!("{}: {e}", root.display()))?;
    let real = root.join(p).canonicalize().map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => {
            format!("{rel}: no such file or directory in the workspace")
        }
        _ => format!("{rel}: {e}"),
    })?;
    if !real.starts_with(&root) {
        return Err(format!("{rel}: leads outside the workspace"));
    }
    Ok(real)
}

/// Answers a copy request: entries, then `CopyDone`, or one `Error`.
pub(crate) fn send_copy<W: Write>(
    w: &mut W,
    rem: &[Reminder],
    root: &Path,
    rel: &str,
) -> std::io::Result<()> {
    let (root, real) = match resolve(root, rel).and_then(|real| {
        root.canonicalize()
            .map(|r| (r, real))
            .map_err(|e| e.to_string())
    }) {
        Ok(x) => x,
        Err(message) => return reply(w, rem, Response::Error { message }),
    };
    let meta = match std::fs::metadata(&real) {
        Ok(m) => m,
        Err(e) => {
            return reply(
                w,
                rem,
                Response::Error {
                    message: format!("{rel}: {e}"),
                },
            );
        }
    };
    if !meta.is_dir() && !meta.is_file() {
        return reply(
            w,
            rem,
            Response::Error {
                message: format!("{rel}: not a file or directory"),
            },
        );
    }
    walk(w, rem, &root, &real, "", &meta)?;
    reply(w, rem, Response::CopyDone)
}

/// What [`walk`] has still to send: a file or directory at its path, or a symlink's target file.
enum Pending {
    Entry(PathBuf, String, Metadata),
    Link(PathBuf, String),
}

/// Sends `path` (at `rel`) and, for a directory, everything in it, in pre-order. Depth first with
/// a list of its own rather than a call per level: a tree can be as deep as paths allow.
fn walk<W: Write>(
    w: &mut W,
    rem: &[Reminder],
    root: &Path,
    path: &Path,
    rel: &str,
    meta: &Metadata,
) -> std::io::Result<()> {
    let mut todo = vec![Pending::Entry(
        path.to_path_buf(),
        rel.to_string(),
        meta.clone(),
    )];
    while let Some(next) = todo.pop() {
        let (path, rel, meta) = match next {
            Pending::Link(target, rel) => {
                send_file(w, rem, &target, &rel)?;
                continue;
            }
            Pending::Entry(path, rel, meta) => (path, rel, meta),
        };
        if meta.is_file() {
            send_file(w, rem, &path, &rel)?;
            continue;
        }
        if !meta.is_dir() {
            continue;
        }
        reply(
            w,
            rem,
            Response::CopyEntry {
                path: rel.clone(),
                mode: meta.permissions().mode() & 0o7777,
                dir: true,
                len: 0,
            },
        )?;
        let Ok(rd) = std::fs::read_dir(&path) else {
            continue;
        };
        let mut entries: Vec<(String, PathBuf)> = rd
            .filter_map(Result::ok)
            .filter_map(|e| Some((e.file_name().to_str()?.to_string(), e.path())))
            .collect();
        entries.sort();
        let mut children = Vec::new();
        for (name, child) in entries {
            let child_rel = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            let Ok(lmeta) = std::fs::symlink_metadata(&child) else {
                continue;
            };
            if lmeta.file_type().is_symlink() {
                if let Some(target) = file_link_inside(root, &child) {
                    children.push(Pending::Link(target, child_rel));
                }
                continue;
            }
            children.push(Pending::Entry(child, child_rel, lmeta));
        }
        // Taken from the end: the first by name goes next.
        todo.extend(children.into_iter().rev());
    }
    Ok(())
}

/// The target of symlink `link` if it is a regular file inside `root`.
fn file_link_inside(root: &Path, link: &Path) -> Option<PathBuf> {
    let target = link.canonicalize().ok()?;
    (target.starts_with(root) && std::fs::metadata(&target).ok()?.is_file()).then_some(target)
}

fn send_file<W: Write>(w: &mut W, rem: &[Reminder], path: &Path, rel: &str) -> std::io::Result<()> {
    // Unreadable files are skipped rather than ending the copy.
    let Ok(mut f) = File::open(path) else {
        return Ok(());
    };
    let Ok(meta) = f.metadata() else {
        return Ok(());
    };
    let len = meta.len();
    reply(
        w,
        rem,
        Response::CopyEntry {
            path: rel.to_string(),
            mode: meta.permissions().mode() & 0o7777,
            dir: false,
            len,
        },
    )?;
    let sent = std::io::copy(&mut (&mut f).take(len), w)?;
    if sent < len {
        let zeros = [0u8; 8192];
        let mut left = len - sent;
        while left > 0 {
            let n = left.min(zeros.len() as u64) as usize;
            w.write_all(&zeros[..n])?;
            left -= n as u64;
        }
    }
    w.flush()
}

/// What [`keep`] copied, and what it could not.
pub(crate) struct Kept {
    /// The copied files, as paths relative to the job's directory (`keep/...`).
    pub kept: Vec<String>,
    /// Matches that could not be copied, each with why: "a/b.txt: No space left on device".
    pub failed: Vec<String>,
}

/// Copies what `globs` match in the workspace at `root` into `dest` (the job's `keep/`), by their
/// paths in the workspace. Problems are logged, never fatal; a file that could not be copied
/// (a full disk) leaves nothing behind in `dest`.
pub(crate) fn keep(root: &Path, globs: &[String], dest: &Path, log: &Logger, job: &str) -> Kept {
    let Ok(root) = root.canonicalize() else {
        log.log(format!("{job}: keep: workspace {} is gone", root.display()));
        return Kept {
            kept: Vec::new(),
            failed: Vec::new(),
        };
    };
    let mut kept = Vec::new();
    let mut failed = Vec::new();
    // Each file is copied once, whatever matches it again: a glob's directory and one inside it
    // (`out/**`), or another glob.
    let mut copied = std::collections::HashSet::new();
    for g in globs {
        if let Err(e) = check_relative(g) {
            log.log(format!("{job}: keep: {e}"));
            continue;
        }
        let matches = match glob_matches(&root, g) {
            Ok(m) => m,
            Err(e) => {
                log.log(format!("{job}: keep: bad pattern {g:?}: {e}"));
                continue;
            }
        };
        let mut any = false;
        for rel in matches {
            any |= copy_into(
                &root,
                &root.join(&rel),
                &rel,
                dest,
                &mut kept,
                &mut failed,
                &mut copied,
            );
        }
        if !any {
            log.log(format!("{job}: keep: {g:?} matched no files"));
        }
    }
    kept.sort();
    kept.dedup();
    failed.sort();
    failed.dedup();
    for f in &failed {
        log.log(format!("{job}: keep: could not copy {f}"));
    }
    Kept { kept, failed }
}

/// What glob `g` (relative, without `..`) matches in the workspace at `root` (canonical), as
/// paths relative to it, matched as `glob::glob` does but for symlinks. The components before the
/// first wildcard may go through symlinks that resolve inside the workspace; past them, the walk
/// goes into real directories only (`glob::glob` followed links to directories: two links to `.`
/// made `**` walk until ELOOP at every level, and a link to a mount walked all of it). `**`
/// stands for any number of directories, and at the end for each directory below; a trailing `/`
/// matches directories only.
fn glob_matches(root: &Path, g: &str) -> Result<Vec<PathBuf>, glob::PatternError> {
    let want_dir = g.ends_with('/');
    let comps: Vec<&str> = Path::new(g)
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();
    let lit = comps
        .iter()
        .take_while(|c| !c.contains(['*', '?', '[']))
        .count();
    let mut pats: Vec<glob::Pattern> = Vec::new();
    for c in &comps[lit..] {
        let p = glob::Pattern::new(c)?;
        if !(p.as_str() == "**" && pats.last().is_some_and(|l| l.as_str() == "**")) {
            pats.push(p);
        }
    }
    let start = comps[..lit].join("/");
    let Ok(base) = resolve(root, &start) else {
        return Ok(Vec::new());
    };
    if pats.is_empty() {
        return Ok((!want_dir || base.is_dir())
            .then(|| PathBuf::from(start))
            .into_iter()
            .collect());
    }
    let mut found = Vec::new();
    // Directories to look in, each with the index of the pattern its entries are matched with.
    let mut todo = vec![(base, PathBuf::from(start), 0)];
    while let Some((dir, rel, i)) = todo.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in rd.filter_map(Result::ok) {
            let Some(name) = e.file_name().to_str().map(str::to_string) else {
                continue;
            };
            // The entry itself: a symlink is not a directory here.
            let is_dir = e.file_type().is_ok_and(|t| t.is_dir());
            let (path, rel) = (e.path(), rel.join(&name));
            let mut j = i;
            if pats[i].as_str() == "**" {
                if is_dir {
                    todo.push((path.clone(), rel.clone(), i));
                }
                if i + 1 == pats.len() {
                    if is_dir {
                        found.push(rel);
                    }
                    continue;
                }
                j = i + 1;
            }
            if !pats[j].matches(&name) {
                continue;
            }
            if j + 1 == pats.len() {
                if !want_dir || is_dir {
                    found.push(rel);
                }
            } else if is_dir {
                todo.push((path, rel, j + 1));
            }
        }
    }
    Ok(found)
}

/// Copies `src` (a match, at `rel` in the workspace) into `dest`; false if it is nothing to keep
/// (no file in it, nor a directory that couldn't be made). A file whose path is in `copied` was
/// copied (or failed) already; the others are added. Depth first with a list of its own rather
/// than a call per level: a tree can be as deep as paths allow.
fn copy_into(
    root: &Path,
    src: &Path,
    rel: &Path,
    dest: &Path,
    kept: &mut Vec<String>,
    failed: &mut Vec<String>,
    copied: &mut std::collections::HashSet<PathBuf>,
) -> bool {
    let mut any = false;
    let mut todo = vec![(src.to_path_buf(), rel.to_path_buf())];
    while let Some((src, rel)) = todo.pop() {
        let Ok(lmeta) = std::fs::symlink_metadata(&src) else {
            continue;
        };
        let src = if lmeta.file_type().is_symlink() {
            match file_link_inside(root, &src) {
                Some(t) => t,
                None => continue,
            }
        } else {
            src
        };
        let Ok(meta) = std::fs::metadata(&src) else {
            continue;
        };
        let to = dest.join(&rel);
        if meta.is_dir() {
            if let Err(e) = std::fs::create_dir_all(&to) {
                failed.push(format!("{}: {e}", rel.display()));
                any = true;
                continue;
            }
            let Ok(rd) = std::fs::read_dir(&src) else {
                continue;
            };
            for e in rd.filter_map(Result::ok) {
                todo.push((e.path(), rel.join(e.file_name())));
            }
        } else if meta.is_file() {
            any = true;
            if !copied.insert(rel.clone()) {
                continue;
            }
            if let Some(parent) = to.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            // A copy left by an earlier keep of this job (a restart cut its end short) goes
            // first: copying onto it fails when it is read-only.
            let _ = std::fs::remove_file(&to);
            match std::fs::copy(&src, &to) {
                Ok(_) => kept.push(Path::new(KEEP).join(&rel).to_string_lossy().into_owned()),
                Err(e) => {
                    // A copy cut short must not pass for the file (it would be shipped as kept).
                    let _ = std::fs::remove_file(&to);
                    failed.push(format!("{}: {e}", rel.display()));
                }
            }
        }
    }
    any
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::read_msg;
    use std::io::BufRead;

    fn tree() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("ws");
        std::fs::create_dir_all(root.join("a/b")).unwrap();
        std::fs::write(root.join("a/one.txt"), b"one").unwrap();
        std::fs::write(root.join("a/b/two.txt"), b"two").unwrap();
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret"), b"outside").unwrap();
        std::os::unix::fs::symlink(outside.join("secret"), root.join("a/out")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("a/outdir")).unwrap();
        std::os::unix::fs::symlink(root.join("a/one.txt"), root.join("a/in")).unwrap();
        std::os::unix::fs::symlink("b", root.join("a/loop")).unwrap();
        (dir, root)
    }

    fn entries(buf: &[u8]) -> Vec<(String, bool, Vec<u8>)> {
        let mut r = std::io::BufReader::new(buf);
        let mut out = Vec::new();
        loop {
            let m: ServerResponse = read_msg(&mut r).unwrap().unwrap();
            match m.resp {
                Response::CopyEntry { path, dir, len, .. } => {
                    let mut b = vec![0; len as usize];
                    r.read_exact(&mut b).unwrap();
                    out.push((path, dir, b));
                }
                Response::CopyDone => break,
                other => panic!("{other:?}"),
            }
        }
        assert!(r.fill_buf().unwrap().is_empty());
        out
    }

    #[test]
    fn copies_a_tree_without_leaving_it() {
        let (_d, root) = tree();
        let mut buf = Vec::new();
        send_copy(&mut buf, &[], &root, "a").unwrap();
        let got: Vec<(String, bool, Vec<u8>)> = entries(&buf);
        let names: Vec<&str> = got.iter().map(|(p, _, _)| p.as_str()).collect();
        assert_eq!(names, ["", "b", "b/two.txt", "in", "one.txt"]);
        assert!(got[0].1 && got[1].1);
        assert_eq!(
            got[3].2, b"one",
            "a symlink to a file inside is copied as the file"
        );

        let mut buf = Vec::new();
        send_copy(&mut buf, &[], &root, "a/b/two.txt").unwrap();
        assert_eq!(entries(&buf), [("".to_string(), false, b"two".to_vec())]);

        for bad in [
            "../secret",
            "/etc/hosts",
            "a/out",
            "a/outdir/secret",
            "nope",
        ] {
            let mut buf = Vec::new();
            send_copy(&mut buf, &[], &root, bad).unwrap();
            let m: ServerResponse = read_msg(&mut std::io::BufReader::new(&buf[..]))
                .unwrap()
                .unwrap();
            assert!(matches!(m.resp, Response::Error { .. }), "{bad}: {m:?}");
        }
        // Through a symlink that stays inside: fine.
        let mut buf = Vec::new();
        send_copy(&mut buf, &[], &root, "a/loop/two.txt").unwrap();
        assert_eq!(entries(&buf)[0].2, b"two");
    }

    #[test]
    fn keep_copies_matches_inside_only() {
        let (d, root) = tree();
        let dest = d.path().join("job/keep");
        let log = Logger::open(&d.path().join("log"));
        let Kept { kept, failed } = keep(
            &root,
            &[
                "a/*.txt".into(),
                "a/b".into(),
                "a/out".into(),
                "a/outdir/*".into(),
                "../outside/secret".into(),
                "a/in".into(),
            ],
            &dest,
            &log,
            "j",
        );
        assert_eq!(kept, ["keep/a/b/two.txt", "keep/a/in", "keep/a/one.txt"]);
        assert!(failed.is_empty(), "{failed:?}");
        assert_eq!(std::fs::read(dest.join("a/in")).unwrap(), b"one");
        assert!(!dest.join("a/out").exists() && !dest.join("a/outdir").exists());
        let log = std::fs::read_to_string(d.path().join("log")).unwrap();
        assert!(log.contains("paths with .. are refused"), "{log}");
    }

    /// A tree as deep as paths allow (PATH_MAX: about 2,000 levels on Linux, 500 on macOS) is
    /// copied by `cp` and kept by `--keep` in a thread with a small stack: neither walk recurses
    /// once per level (a debug build of the daemon overflowed its 2 MiB stack on Linux).
    #[test]
    fn deep_trees_are_copied_and_kept_on_a_small_stack() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().join("ws");
        let dest = d.path().join("kp");
        // Room for the deepest file's name and the longer of the two prefixes.
        let room = libc::PATH_MAX as usize - root.as_os_str().len() - 16;
        let levels = room / 2;
        let mut rel = PathBuf::from("t");
        for _ in 1..levels {
            rel.push("d");
        }
        std::fs::create_dir_all(root.join(&rel)).unwrap();
        std::fs::write(root.join(&rel).join("f"), b"deep").unwrap();
        let deepest = format!("{}/f", rel.display());
        // Dropped before `d`, also when an assertion fails.
        let chains = Chains {
            tops: vec![root.clone(), dest.clone()],
            rel: rel.clone(),
        };

        let (cp, kept) = std::thread::Builder::new()
            .stack_size(128 << 10)
            .spawn({
                let (root, dest, d) = (root.clone(), dest.clone(), d.path().to_path_buf());
                move || {
                    let mut buf = Vec::new();
                    send_copy(&mut buf, &[], &root, "t").unwrap();
                    let log = Logger::open(&d.join("log"));
                    (buf, keep(&root, &["t".into()], &dest, &log, "j"))
                }
            })
            .unwrap()
            .join()
            .unwrap();
        let got = entries(&cp);
        assert_eq!(got.len(), levels + 1, "every level and the file");
        let file = got.last().unwrap();
        assert_eq!(
            (format!("t/{}", file.0), file.1, file.2.as_slice()),
            (deepest.clone(), false, &b"deep"[..])
        );
        assert_eq!(kept.kept, [format!("{KEEP}/{deepest}")]);
        assert!(kept.failed.is_empty(), "{:?}", kept.failed);
        assert_eq!(std::fs::read(dest.join(&deepest)).unwrap(), b"deep");
        drop(chains);
        assert!(!root.join("t").exists() && !dest.join("t").exists());
    }

    /// Removes the chains `top/rel` (a directory for each level of `rel`, the file `f` at the
    /// bottom) from the bottom up, one path at a time. Left to the tempdir, they were removed by
    /// std's `remove_dir_all`, which holds a descriptor for each level: about 2,030 for the tree
    /// above on Linux, about all a test process may have open there (msi: 2048) with the other
    /// tests' few hundred. The removal failed, leaving both trees in /tmp, and a test opening a
    /// file meanwhile could fail with "Too many open files".
    struct Chains {
        tops: Vec<PathBuf>,
        rel: PathBuf,
    }

    impl Drop for Chains {
        fn drop(&mut self) {
            for top in &self.tops {
                let mut p = top.join(&self.rel);
                let _ = std::fs::remove_file(p.join("f"));
                while p != *top {
                    let _ = std::fs::remove_dir(&p);
                    p.pop();
                }
            }
        }
    }

    /// A match that can't be copied (here into a directory that can't be written; on a full disk
    /// the same) is reported as such, not as matching nothing, and leaves nothing behind.
    #[test]
    fn keep_reports_what_it_could_not_copy() {
        let (d, root) = tree();
        let dest = d.path().join("job/keep");
        std::fs::create_dir_all(dest.join("a")).unwrap();
        std::fs::set_permissions(dest.join("a"), std::fs::Permissions::from_mode(0o500)).unwrap();
        let log = Logger::open(&d.path().join("log"));
        let Kept { kept, failed } = keep(&root, &["a/one.txt".into()], &dest, &log, "j");
        std::fs::set_permissions(dest.join("a"), std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(kept.is_empty(), "{kept:?}");
        assert_eq!(failed.len(), 1, "{failed:?}");
        assert!(failed[0].starts_with("a/one.txt: "), "{failed:?}");
        assert!(!dest.join("a/one.txt").exists());
        let log = std::fs::read_to_string(d.path().join("log")).unwrap();
        assert!(log.contains("j: keep: could not copy a/one.txt: "), "{log}");
        assert!(!log.contains("matched no files"), "{log}");
    }

    /// Runs `keep` on a thread and gives it 10 seconds.
    fn keep_in_time(root: &Path, globs: &[&str], dest: &Path) -> Kept {
        let (root, dest) = (root.to_path_buf(), dest.to_path_buf());
        let globs: Vec<String> = globs.iter().map(|g| g.to_string()).collect();
        let (tx, rx) = std::sync::mpsc::channel();
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::thread::spawn(move || {
            let log = Logger::open(&dest.with_file_name("log"));
            let _ = tx.send(keep(&root, &globs, &dest, &log, "j"));
        });
        rx.recv_timeout(std::time::Duration::from_secs(10))
            .expect("keep did not finish within 10 seconds")
    }

    /// A glob's wildcards don't go through symlinks to directories, as `cp` doesn't inside a
    /// directory: two links to `.` made `**` walk 2^40 paths (until ELOOP), and one made it copy
    /// each file once per level. The glob's start may go through a link that stays inside.
    #[test]
    fn keep_globs_dont_follow_symlinked_directories() {
        let (d, root) = tree();
        std::os::unix::fs::symlink(".", root.join("a/l1")).unwrap();
        std::os::unix::fs::symlink(".", root.join("a/l2")).unwrap();
        let dest = d.path().join("job/keep");
        let Kept { kept, failed } = keep_in_time(&root, &["**/*.txt", "a/*/two.txt"], &dest);
        assert_eq!(kept, ["keep/a/b/two.txt", "keep/a/one.txt"]);
        assert!(failed.is_empty(), "{failed:?}");

        let dest = d.path().join("job2/keep");
        let Kept { kept, failed } = keep_in_time(&root, &["a/loop/*", "a/outdir/*"], &dest);
        assert_eq!(kept, ["keep/a/loop/two.txt"]);
        assert!(failed.is_empty(), "{failed:?}");
    }

    /// Without symlinks, a glob matches what `glob::glob` matched.
    #[test]
    fn keep_globs_match_as_glob_does() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().canonicalize().unwrap().join("ws");
        for dir in ["a/b/c", "a/bb", "x/b", ".h/b"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        for file in [
            "a/one.txt",
            "a/b/two.txt",
            "a/b/c/b",
            "a/bb/x.rs",
            "x/b/y",
            ".h/z",
            "t",
        ] {
            std::fs::write(root.join(file), b"").unwrap();
        }
        let prefix = glob::Pattern::escape(&root.to_string_lossy());
        for g in [
            "*",
            "**",
            "**/*",
            "a/**",
            "a/**/*.txt",
            "**/b",
            "*/b/*",
            "a/*/",
            "**/",
            "a/b/",
            "a/[ab]*/*",
            "?/*",
            "**/.h",
            "a/**/b/**",
            "**/**/b",
            "a/./b/*",
            "./a/*",
            "a/b",
            "a//b/*",
            "nope/*",
            "a/one.txt/*",
            "t/",
        ] {
            let ours: std::collections::BTreeSet<PathBuf> =
                glob_matches(&root, g).unwrap().into_iter().collect();
            let theirs: std::collections::BTreeSet<PathBuf> =
                glob::glob(&format!("{prefix}/{}", g.trim_start_matches("./")))
                    .unwrap()
                    .flatten()
                    .map(|m| m.strip_prefix(&root).unwrap().to_path_buf())
                    .collect();
            assert_eq!(ours, theirs, "{g}");
        }
    }

    /// A file matched more than once (by nested or overlapping globs, or by a second `keep` after
    /// a restart cut the first short) is copied once and stays kept, also when it is read-only:
    /// a second copy onto the first failed, and the failure removed the first.
    #[test]
    fn keep_copies_a_file_matched_twice_once() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().join("ws");
        std::fs::create_dir_all(root.join("out/sub/deeper")).unwrap();
        let file = root.join("out/sub/deeper/r.txt");
        std::fs::write(&file, b"r").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();
        let dest = d.path().join("job/keep");
        for globs in [
            &["out/**"][..],
            &["out/**", "out/sub/deeper/*.txt", "out/sub/deeper/r.txt"],
        ] {
            let Kept { kept, failed } = keep_in_time(&root, globs, &dest);
            assert!(failed.is_empty(), "{globs:?}: {failed:?}");
            assert_eq!(kept, ["keep/out/sub/deeper/r.txt"], "{globs:?}");
            assert_eq!(
                std::fs::read(dest.join("out/sub/deeper/r.txt")).unwrap(),
                b"r",
                "{globs:?}"
            );
        }
    }
}
