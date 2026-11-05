//! The potluck binary the daemon runs its helpers from: `_supervise`, the first process of every
//! job, and `_tunnel`, which git runs to fetch from another member.
//!
//! An update renames a new binary over the path the daemon started from, then restarts the
//! daemon; with `POTLUCK_NO_RESTART=1`, or when something else replaced the binary, nothing
//! restarts it. Until the daemon restarts, a helper run from that path is the new version, which
//! may read its arguments, env file or status file differently, or speak another mesh version.
//! So the daemon keeps the file it started from open. While the path still names that file,
//! helpers run from the path; once it names another file (or none), the daemon copies the file
//! it kept to `bin/potluck` in its state directory, once, and runs helpers from the copy until it
//! restarts. A starting daemon removes the copy an earlier one left (helpers started from it run
//! on without their file).

use super::Logger;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

/// The binary this process started from, opened by [`remember_own_binary`]: its path and the file.
static STARTED_FROM: OnceLock<(PathBuf, File)> = OnceLock::new();

/// Opens the binary this process runs, before an update can replace it: on Linux the running
/// image itself (`/proc/self/exe`, whatever its path names now), elsewhere the file at its path,
/// read as soon as `potluck daemon` starts. A daemon whose helper binary is that path keeps this
/// file ([`Helpers::new`]).
pub(crate) fn remember_own_binary() {
    let Ok(path) = crate::update::running_binary() else {
        return;
    };
    #[cfg(target_os = "linux")]
    let file = File::open("/proc/self/exe");
    #[cfg(not(target_os = "linux"))]
    let file = File::open(&path);
    if let Ok(file) = file {
        let _ = STARTED_FROM.set((path, file));
    }
}

/// Where the daemon's helpers run from ([`Helpers::path`]).
pub(crate) struct Helpers {
    /// The binary's path (`Options::potluck_bin`).
    path: PathBuf,
    /// The file at `path` when the daemon started; None if it couldn't be opened.
    kept: Option<File>,
    /// Where a copy goes.
    dir: PathBuf,
    copy: Mutex<Copy>,
    log: Logger,
}

#[derive(Default)]
struct Copy {
    path: Option<PathBuf>,
    /// A copy failed and was logged; the next failure isn't.
    failed: bool,
}

impl Helpers {
    /// For a daemon whose helpers run from `path`, with copies in `dir` (removed now: an earlier
    /// daemon's).
    pub fn new(path: &Path, dir: PathBuf, log: Logger) -> Helpers {
        if let Err(e) = std::fs::remove_dir_all(&dir)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            log.log(format!("can't remove {}: {e}", dir.display()));
        }
        let kept = match STARTED_FROM.get() {
            Some((p, f)) if p == path => f.try_clone(),
            _ => File::open(path),
        };
        Helpers {
            path: path.to_path_buf(),
            kept: kept.ok(),
            dir,
            copy: Mutex::new(Copy::default()),
            log,
        }
    }

    /// The binary to run a helper from now: the path while it names the file the daemon started
    /// from, else a copy of that file. The path if a copy can't be made (said in the log once).
    pub fn path(&self) -> PathBuf {
        let Some(kept) = &self.kept else {
            return self.path.clone();
        };
        let mut copy = self.copy.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(p) = &copy.path {
            return p.clone();
        }
        if same_file(kept, &self.path) {
            return self.path.clone();
        }
        match make_copy(kept, &self.dir) {
            Ok(p) => {
                self.log.log(format!(
                    "{} is no longer the potluck binary this daemon started from (updated?): jobs \
                     and git fetches run a copy of that one, {}, until the daemon restarts",
                    self.path.display(),
                    p.display()
                ));
                copy.path = Some(p.clone());
                p
            }
            Err(e) => {
                if !std::mem::replace(&mut copy.failed, true) {
                    self.log.log(format!(
                        "{} is no longer the potluck binary this daemon started from, and a copy \
                         of that one could not be made in {}: {e}; jobs and git fetches run the \
                         new one until the daemon restarts",
                        self.path.display(),
                        self.dir.display()
                    ));
                }
                self.path.clone()
            }
        }
    }
}

impl Helpers {
    /// Whether the binary's path still names the file the daemon started from (false when that
    /// isn't known): a restart in place would run the same binary again.
    pub fn path_unchanged(&self) -> bool {
        self.kept.as_ref().is_some_and(|k| same_file(k, &self.path))
    }

    /// The binary's path (`Options::potluck_bin`).
    pub fn binary(&self) -> &Path {
        &self.path
    }
}

/// Whether `path` names the file `f` is open on.
fn same_file(f: &File, path: &Path) -> bool {
    match (f.metadata(), std::fs::metadata(path)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

/// Copies `kept` to `<dir>/potluck` (a new file renamed into place), executable by this user.
fn make_copy(kept: &File, dir: &Path) -> std::io::Result<PathBuf> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    let dst = dir.join("potluck");
    let tmp = dir.join(format!(".potluck.{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let written = (|| {
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o700)
            .open(&tmp)?;
        let mut src = kept.try_clone()?;
        src.seek(SeekFrom::Start(0))?;
        std::io::copy(&mut src, &mut out)?;
        out.sync_all()
    })();
    if written.is_ok() {
        wait_unwritten(&tmp);
    }
    if let Err(e) = written.and_then(|()| std::fs::rename(&tmp, &dst)) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(dst)
}

/// Waits, up to [`UNWRITTEN_WAIT`], until no process holds the file at `path` open for writing.
/// Linux refuses to run a file that is (ETXTBSY), and a child another thread of the daemon forked
/// while the copy was being written holds a copy of its descriptor until that child execs (a
/// moment, or longer on a loaded machine): a job started from the copy meanwhile would fail to
/// start. No other process opens the copy, so once none holds it, none will. A read lease can be
/// taken only on a file nobody has open for writing: that is the test. A file system without
/// leases can't tell; then this doesn't wait.
#[cfg(target_os = "linux")]
fn wait_unwritten(path: &Path) {
    use std::os::fd::AsRawFd;
    let Ok(f) = File::open(path) else {
        return;
    };
    let until = std::time::Instant::now() + UNWRITTEN_WAIT;
    loop {
        // SAFETY: fcntl on a descriptor this function owns; the lease taken is let go at once.
        let r = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_SETLEASE, libc::F_RDLCK) };
        if r == 0 {
            // SAFETY: as above.
            unsafe { libc::fcntl(f.as_raw_fd(), libc::F_SETLEASE, libc::F_UNLCK) };
            return;
        }
        let busy = std::io::Error::last_os_error().raw_os_error() == Some(libc::EAGAIN);
        if !busy || std::time::Instant::now() >= until {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// macOS runs a file open for writing.
#[cfg(not(target_os = "linux"))]
fn wait_unwritten(_path: &Path) {}

/// How long [`wait_unwritten`] waits at most.
#[cfg(target_os = "linux")]
const UNWRITTEN_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_run_from_the_path_until_it_names_another_file() {
        let d = tempfile::tempdir().unwrap();
        let bin = d.path().join("potluck");
        std::fs::write(&bin, b"the binary the daemon started from").unwrap();
        let dir = d.path().join("home/bin");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("potluck"), b"an earlier daemon's copy").unwrap();
        let log = Logger::open(&d.path().join("daemon.log"));
        let h = Helpers::new(&bin, dir.clone(), log);
        // An earlier daemon's copy is gone.
        assert!(!dir.exists());
        assert_eq!(h.path(), bin);
        // Replaced as an update does: a new file renamed over it.
        let new = d.path().join("potluck.new");
        std::fs::write(&new, b"the new one").unwrap();
        std::fs::rename(&new, &bin).unwrap();
        let p = h.path();
        assert_eq!(p, dir.join("potluck"));
        assert_eq!(
            std::fs::read(&p).unwrap(),
            b"the binary the daemon started from"
        );
        let mode = std::fs::metadata(&p).unwrap().mode();
        assert_eq!(mode & 0o777, 0o700, "{mode:o}");
        // Made once.
        assert_eq!(h.path(), p);
        let log = std::fs::read_to_string(d.path().join("daemon.log")).unwrap();
        assert_eq!(
            log.matches("is no longer the potluck binary").count(),
            1,
            "{log}"
        );
        // Gone altogether: the copy still runs.
        std::fs::remove_file(&bin).unwrap();
        assert_eq!(h.path(), p);
    }

    /// Linux refuses to run a file that is open for writing (ETXTBSY), and a child another thread
    /// of the daemon forks while the copy is being written holds the copy's descriptor until it
    /// execs: a job started from the copy then fails to start. Here children paused between fork
    /// and exec stand in for them (a fork-heavy daemon on a loaded machine). The copy is handed
    /// out once nothing holds it open for writing.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_copy_is_handed_out_once_nothing_holds_it_open_for_writing() {
        use std::os::unix::process::CommandExt;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let d = tempfile::tempdir().unwrap();
        let bin = d.path().join("potluck");
        // A script of 64 MiB (the rest a comment), so the copy takes a while.
        let mut text = b"#!/bin/sh\nexit 0\n#".to_vec();
        text.resize(64 << 20, b'#');
        std::fs::write(&bin, &text).unwrap();
        let h = Helpers::new(
            &bin,
            d.path().join("bin"),
            Logger::open(&d.path().join("daemon.log")),
        );
        let new = d.path().join("potluck.new");
        std::fs::write(&new, b"the new one").unwrap();
        std::fs::rename(&new, &bin).unwrap();
        // Threads forking children that wait 300 ms before their exec (spawn returns at the exec).
        let stop = Arc::new(AtomicBool::new(false));
        let forks: Vec<_> = (0..64)
            .map(|i| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    // Spread out: a fork every 5 ms or so.
                    std::thread::sleep(std::time::Duration::from_millis(5 * i));
                    while !stop.load(Ordering::SeqCst) {
                        let mut cmd = std::process::Command::new("true");
                        // SAFETY: only a sleep between fork and exec.
                        unsafe {
                            cmd.pre_exec(|| {
                                std::thread::sleep(std::time::Duration::from_millis(300));
                                Ok(())
                            });
                        }
                        if let Ok(mut c) = cmd.spawn() {
                            let _ = c.wait();
                        }
                    }
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(400));
        let t = std::time::Instant::now();
        let p = h.path();
        let took = t.elapsed();
        let ran = std::process::Command::new(&p).status();
        stop.store(true, Ordering::SeqCst);
        for f in forks {
            f.join().unwrap();
        }
        assert_eq!(p, d.path().join("bin/potluck"));
        assert!(
            ran.as_ref().is_ok_and(|s| s.success()),
            "{ran:?} (copied in {took:?})"
        );
    }

    #[test]
    fn a_binary_removed_before_the_first_helper_is_copied_too() {
        let d = tempfile::tempdir().unwrap();
        let bin = d.path().join("potluck");
        std::fs::write(&bin, b"started from").unwrap();
        let h = Helpers::new(
            &bin,
            d.path().join("bin"),
            Logger::open(&d.path().join("daemon.log")),
        );
        std::fs::remove_file(&bin).unwrap();
        assert_eq!(std::fs::read(h.path()).unwrap(), b"started from");
    }
}
