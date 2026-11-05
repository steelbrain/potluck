//! Advisory locks between threads and processes sharing one state directory: `flock(2)` on a lock
//! file. Each [`Lock`] opens the file anew, so two threads of one process exclude each other too.
//! Dropping the lock releases it.

use anyhow::Context;
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::Path;

pub(super) struct Lock {
    file: File,
}

impl Lock {
    pub fn shared(path: &Path) -> anyhow::Result<Lock> {
        Lock::take(path, libc::LOCK_SH)
    }

    pub fn exclusive(path: &Path) -> anyhow::Result<Lock> {
        Lock::take(path, libc::LOCK_EX)
    }

    /// A shared lock if no one holds the lock exclusively right now, else `None`.
    pub fn try_shared(path: &Path) -> anyhow::Result<Option<Lock>> {
        match Lock::take(path, libc::LOCK_SH | libc::LOCK_NB) {
            Ok(lock) => Ok(Some(lock)),
            Err(e)
                if e.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::WouldBlock) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// An exclusive lock if no one holds the lock right now, else `None`.
    pub fn try_exclusive(path: &Path) -> anyhow::Result<Option<Lock>> {
        match Lock::take(path, libc::LOCK_EX | libc::LOCK_NB) {
            Ok(lock) => Ok(Some(lock)),
            Err(e)
                if e.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::WouldBlock) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn take(path: &Path, how: libc::c_int) -> anyhow::Result<Lock> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("can't create {}", dir.display()))?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("can't open lock file {}", path.display()))?;
        let lock = Lock { file };
        lock.set(how)
            .with_context(|| format!("can't lock {}", path.display()))?;
        Ok(lock)
    }

    /// Turns a shared lock exclusive or back. Not atomic: another holder may get in between, so
    /// re-read whatever the lock protects afterwards.
    pub fn convert(&self, exclusive: bool) -> anyhow::Result<()> {
        self.set(if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        })
    }

    fn set(&self, how: libc::c_int) -> anyhow::Result<()> {
        loop {
            // SAFETY: flock on an open descriptor.
            if unsafe { libc::flock(self.file.as_raw_fd(), how) } == 0 {
                return Ok(());
            }
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::Interrupted {
                return Err(e.into());
            }
        }
    }
}
