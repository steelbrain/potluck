//! Copy-on-write clones: `clonefile(2)` on macOS (a whole directory in one call), the FICLONE
//! ioctl per file on Linux (btrfs, XFS with reflink). No fallback to copying: potluck requires a
//! CoW filesystem and refuses to run without one.
//!
//! A clone keeps each file's mode and modification time (and on macOS its other attributes), so
//! git's stat check (with `core.checkStat=minimal`, as potluck's mirrors set it) sees cloned files
//! as unchanged. On Linux, directories are recreated with their modes and mtimes, and symlinks with
//! their targets and mtimes; files are cloned by several threads at once. Other file types
//! (sockets, fifos, devices) are refused. On macOS, clonefile gives a directory that has entries
//! the time of the clone as its mtime (git ignores directory mtimes).

use anyhow::{Context, bail};
use std::path::Path;

/// Clones `src` (a file or directory tree) to `dst`, which must not exist.
pub fn clone_tree(src: &Path, dst: &Path) -> anyhow::Result<()> {
    if dst.symlink_metadata().is_ok() {
        bail!("can't clone to {}: it already exists", dst.display());
    }
    imp::clone_tree(src, dst)
        .with_context(|| format!("can't clone {} to {}", src.display(), dst.display()))
}

/// Clones every entry of directory `src` into the existing directory `dst`, except the top-level
/// names in `skip`. Entries must not exist in `dst` yet.
pub fn clone_dir_contents(src: &Path, dst: &Path, skip: &[&str]) -> anyhow::Result<()> {
    imp::clone_dir_contents(src, dst, skip)
        .with_context(|| format!("can't clone {} into {}", src.display(), dst.display()))
}

/// Whether `dir` supports clones: clones a small test file there. `Err` names the filesystem.
pub fn check(dir: &Path) -> anyhow::Result<()> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let name = format!(".potluck-cow-check-{}-{nanos}", std::process::id());
    let src = dir.join(&name);
    let dst = dir.join(format!("{name}.clone"));
    std::fs::write(&src, vec![b'p'; 8192])
        .with_context(|| format!("can't write a test file in {}", dir.display()))?;
    let result = imp::clone_tree(&src, &dst);
    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
    result.map_err(|e| {
        anyhow::anyhow!(
            "{} is on {}, which can't make copy-on-write clones ({e:#}). potluck needs its state \
             directory on APFS, btrfs, or XFS with reflink: set POTLUCK_HOME to a directory on one",
            dir.display(),
            fs_name(dir)
        )
    })
}

/// The filesystem `path` is on, in words.
pub fn fs_name(path: &Path) -> String {
    imp::fs_name(path)
}

fn c_path(p: &Path) -> anyhow::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(p.as_os_str().as_bytes())
        .map_err(|_| anyhow::anyhow!("{} contains a NUL byte", p.display()))
}

#[cfg(target_os = "macos")]
mod imp {
    use super::c_path;
    use std::path::Path;

    /// From <sys/clonefile.h>: don't follow a symlink at `src`.
    const CLONE_NOFOLLOW: u32 = 0x0001;

    pub fn clone_tree(src: &Path, dst: &Path) -> anyhow::Result<()> {
        let (s, d) = (c_path(src)?, c_path(dst)?);
        // SAFETY: both are valid NUL-terminated paths.
        let r = unsafe { libc::clonefile(s.as_ptr(), d.as_ptr(), CLONE_NOFOLLOW) };
        if r != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    pub fn clone_dir_contents(src: &Path, dst: &Path, skip: &[&str]) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let name = entry.file_name();
            if skip.iter().any(|s| name == **s) {
                continue;
            }
            clone_tree(&entry.path(), &dst.join(&name))?;
        }
        Ok(())
    }

    pub fn fs_name(path: &Path) -> String {
        let Ok(p) = c_path(path) else {
            return "an unknown filesystem".into();
        };
        // SAFETY: statfs fills the zeroed struct; the path is NUL-terminated.
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(p.as_ptr(), &mut st) } != 0 {
            return "an unknown filesystem".into();
        }
        // SAFETY: f_fstypename is a NUL-terminated C string within the struct.
        let name = unsafe { std::ffi::CStr::from_ptr(st.f_fstypename.as_ptr()) };
        name.to_string_lossy().into_owned()
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use super::c_path;
    use anyhow::bail;
    use std::fs::{self, File, Metadata, OpenOptions};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::os::unix::io::AsRawFd;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex};

    pub fn clone_tree(src: &Path, dst: &Path) -> anyhow::Result<()> {
        let meta = fs::symlink_metadata(src)?;
        let ft = meta.file_type();
        if ft.is_dir() {
            fs::create_dir(dst)?;
            fs::set_permissions(dst, fs::Permissions::from_mode(0o700))?;
            clone_dir_contents(src, dst, &[])?;
            finish_dir(dst, &meta)
        } else {
            clone_entry(src, dst, &meta)?;
            Ok(())
        }
    }

    struct Queue {
        dirs: Vec<(PathBuf, PathBuf)>,
        active: usize,
    }

    pub fn clone_dir_contents(src: &Path, dst: &Path, skip: &[&str]) -> anyhow::Result<()> {
        let queue = Mutex::new(Queue {
            dirs: Vec::new(),
            active: 0,
        });
        let wake = Condvar::new();
        let failed = AtomicBool::new(false);
        let error: Mutex<Option<anyhow::Error>> = Mutex::new(None);
        let finished_dirs: Mutex<Vec<(PathBuf, Metadata)>> = Mutex::new(Vec::new());

        // The top level runs here, so `skip` applies only to it; subdirectories go to the workers.
        let fail = |e: anyhow::Error| {
            failed.store(true, Ordering::Relaxed);
            error.lock().unwrap().get_or_insert(e);
        };
        clone_one_dir(src, dst, skip, &queue, &wake, &finished_dirs)?;
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(1, 8);
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    loop {
                        let job = {
                            let mut q = queue.lock().unwrap();
                            loop {
                                if failed.load(Ordering::Relaxed) {
                                    break None;
                                }
                                if let Some(job) = q.dirs.pop() {
                                    q.active += 1;
                                    break Some(job);
                                }
                                if q.active == 0 {
                                    break None;
                                }
                                q = wake.wait(q).unwrap();
                            }
                        };
                        let Some((s, d)) = job else {
                            wake.notify_all();
                            return;
                        };
                        if let Err(e) = clone_one_dir(&s, &d, &[], &queue, &wake, &finished_dirs) {
                            fail(e);
                        }
                        let mut q = queue.lock().unwrap();
                        q.active -= 1;
                        if q.active == 0 && q.dirs.is_empty() {
                            wake.notify_all();
                        }
                    }
                });
            }
        });
        if let Some(e) = error.into_inner().unwrap() {
            return Err(e);
        }
        // Directory modes and mtimes last: creating entries changes a directory's mtime, and a
        // read-only directory couldn't take its entries.
        for (dir, meta) in finished_dirs.into_inner().unwrap() {
            finish_dir(&dir, &meta)?;
        }
        Ok(())
    }

    /// Clones the entries of `src` into `dst`; subdirectories are created and queued.
    fn clone_one_dir(
        src: &Path,
        dst: &Path,
        skip: &[&str],
        queue: &Mutex<Queue>,
        wake: &Condvar,
        finished_dirs: &Mutex<Vec<(PathBuf, Metadata)>>,
    ) -> anyhow::Result<()> {
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let name = entry.file_name();
            if skip.iter().any(|s| name == **s) {
                continue;
            }
            let (s, d) = (entry.path(), dst.join(&name));
            let meta = fs::symlink_metadata(&s)?;
            if meta.is_dir() {
                fs::create_dir(&d)?;
                fs::set_permissions(&d, fs::Permissions::from_mode(0o700))?;
                finished_dirs.lock().unwrap().push((d.clone(), meta));
                queue.lock().unwrap().dirs.push((s, d));
                wake.notify_one();
            } else {
                clone_entry(&s, &d, &meta)?;
            }
        }
        Ok(())
    }

    fn clone_entry(src: &Path, dst: &Path, meta: &Metadata) -> anyhow::Result<()> {
        let ft = meta.file_type();
        if ft.is_symlink() {
            let target = fs::read_link(src)?;
            std::os::unix::fs::symlink(&target, dst)?;
            set_times(dst, meta, true)
        } else if ft.is_file() {
            let s = File::open(src)?;
            let d = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(dst)?;
            if meta.len() > 0 {
                // SAFETY: both descriptors are open; FICLONE takes the source fd as its argument.
                let r = unsafe { libc::ioctl(d.as_raw_fd(), libc::FICLONE, s.as_raw_fd()) };
                if r != 0 {
                    let e = std::io::Error::last_os_error();
                    drop(d);
                    let _ = fs::remove_file(dst);
                    bail!("can't clone {}: {e}", src.display());
                }
            }
            d.set_permissions(fs::Permissions::from_mode(meta.mode() & 0o7777))?;
            drop(d);
            set_times(dst, meta, false)
        } else {
            bail!(
                "{} is not a file, directory or symlink: refusing to clone it",
                src.display()
            )
        }
    }

    fn finish_dir(dir: &Path, meta: &Metadata) -> anyhow::Result<()> {
        fs::set_permissions(dir, fs::Permissions::from_mode(meta.mode() & 0o7777))?;
        set_times(dir, meta, false)
    }

    /// Gives `path` the access and modification times in `meta`, to the nanosecond.
    fn set_times(path: &Path, meta: &Metadata, nofollow: bool) -> anyhow::Result<()> {
        let p = c_path(path)?;
        let times = [
            libc::timespec {
                tv_sec: meta.atime() as libc::time_t,
                tv_nsec: meta.atime_nsec() as _,
            },
            libc::timespec {
                tv_sec: meta.mtime() as libc::time_t,
                tv_nsec: meta.mtime_nsec() as _,
            },
        ];
        let flags = if nofollow {
            libc::AT_SYMLINK_NOFOLLOW
        } else {
            0
        };
        // SAFETY: a valid path and two timespecs.
        let r = unsafe { libc::utimensat(libc::AT_FDCWD, p.as_ptr(), times.as_ptr(), flags) };
        if r != 0 {
            bail!(
                "can't set the times of {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            );
        }
        Ok(())
    }

    #[allow(clippy::unnecessary_cast)]
    pub fn fs_name(path: &Path) -> String {
        let Ok(p) = c_path(path) else {
            return "an unknown filesystem".into();
        };
        // SAFETY: statfs fills the zeroed struct; the path is NUL-terminated.
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statfs(p.as_ptr(), &mut st) } != 0 {
            return "an unknown filesystem".into();
        }
        let magic = st.f_type as u64 & 0xffff_ffff;
        let name = match magic {
            0xEF53 => "ext4",
            0x5846_5342 => "XFS without reflink (it needs mkfs.xfs -m reflink=1)",
            0x9123_683E => "btrfs",
            0x0102_1994 => "tmpfs",
            0x8584_58F6 => "ramfs",
            0x794C_7630 => "overlayfs",
            0x2FC1_2FC1 => "ZFS",
            0x6969 => "NFS",
            0xFF53_4D42 | 0xFE53_4D42 => "SMB",
            0xF2F5_2010 => "F2FS",
            0xCA45_1A4E => "bcachefs",
            0x6573_5546 => "a FUSE filesystem",
            0xF15F => "eCryptfs",
            0x4D44 => "FAT",
            0x2011_BAB0 => "exFAT",
            0x7366_746E | 0x5346_544E => "NTFS",
            _ => return format!("a filesystem of type 0x{magic:x}"),
        };
        name.to_string()
    }
}
