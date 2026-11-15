//! Advisory locks between threads and processes sharing one state directory: `flock(2)` on a lock
//! file (on Windows `LockFileEx` over the whole file, through std's `File::lock`). Each [`Lock`]
//! opens the file anew, so two threads of one process exclude each other too. Dropping the lock
//! releases it.

use anyhow::Context;
use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::path::Path;

pub(super) struct Lock {
    file: File,
}

/// How a lock is taken: shared or exclusive, waiting or not.
#[derive(Clone, Copy)]
struct How {
    exclusive: bool,
    wait: bool,
}

impl Lock {
    pub fn shared(path: &Path) -> anyhow::Result<Lock> {
        Lock::take(
            path,
            How {
                exclusive: false,
                wait: true,
            },
        )
    }

    pub fn exclusive(path: &Path) -> anyhow::Result<Lock> {
        Lock::take(
            path,
            How {
                exclusive: true,
                wait: true,
            },
        )
    }

    /// A shared lock if no one holds the lock exclusively right now, else `None`.
    pub fn try_shared(path: &Path) -> anyhow::Result<Option<Lock>> {
        Lock::try_take(
            path,
            How {
                exclusive: false,
                wait: false,
            },
        )
    }

    /// An exclusive lock if no one holds the lock right now, else `None`.
    pub fn try_exclusive(path: &Path) -> anyhow::Result<Option<Lock>> {
        Lock::try_take(
            path,
            How {
                exclusive: true,
                wait: false,
            },
        )
    }

    fn try_take(path: &Path, how: How) -> anyhow::Result<Option<Lock>> {
        match Lock::take(path, how) {
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

    fn take(path: &Path, how: How) -> anyhow::Result<Lock> {
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
        // Windows doesn't convert a lock: it is released, then taken again, as flock(2) does
        // without saying so.
        #[cfg(windows)]
        self.file.unlock()?;
        self.set(How {
            exclusive,
            wait: true,
        })
    }

    #[cfg(unix)]
    fn set(&self, how: How) -> anyhow::Result<()> {
        let mut op = if how.exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        };
        if !how.wait {
            op |= libc::LOCK_NB;
        }
        loop {
            // SAFETY: flock on an open descriptor.
            if unsafe { libc::flock(self.file.as_raw_fd(), op) } == 0 {
                return Ok(());
            }
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::Interrupted {
                return Err(e.into());
            }
        }
    }

    #[cfg(windows)]
    fn set(&self, how: How) -> anyhow::Result<()> {
        use std::fs::TryLockError;
        let tried = match (how.exclusive, how.wait) {
            (true, true) => return Ok(self.file.lock()?),
            (false, true) => return Ok(self.file.lock_shared()?),
            (true, false) => self.file.try_lock(),
            (false, false) => self.file.try_lock_shared(),
        };
        match tried {
            Ok(()) => Ok(()),
            Err(TryLockError::WouldBlock) => {
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock).into())
            }
            Err(TryLockError::Error(e)) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `take` until it gives a lock: a released lock can stay held a moment longer, by a child
    /// another test thread forks before its exec closes the descriptor (flock follows the open
    /// file into the child).
    fn soon(take: impl Fn() -> anyhow::Result<Option<Lock>>) -> Lock {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Some(lock) = take().unwrap() {
                return lock;
            }
            assert!(std::time::Instant::now() < deadline, "never free");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Shared locks share, an exclusive one excludes, and a conversion waits for the others.
    #[test]
    fn shared_and_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub/x.lock");
        let a = Lock::shared(&path).unwrap();
        let b = soon(|| Lock::try_shared(&path));
        assert!(Lock::try_exclusive(&path).unwrap().is_none());
        drop(b);
        a.convert(true).unwrap();
        assert!(Lock::try_shared(&path).unwrap().is_none());
        a.convert(false).unwrap();
        drop(soon(|| Lock::try_shared(&path)));
        drop(a);
        let x = soon(|| Lock::try_exclusive(&path));
        let (tx, rx) = std::sync::mpsc::channel();
        let p = path.clone();
        let waiter = std::thread::spawn(move || {
            let _l = Lock::exclusive(&p).unwrap();
            tx.send(()).unwrap();
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(200))
                .is_err()
        );
        drop(x);
        rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        waiter.join().unwrap();
    }
}
