//! Copy-on-write clones: `clonefile(2)` on macOS (a whole directory in one call), the FICLONE
//! ioctl per file on Linux (btrfs, XFS with reflink), ReFS block cloning per file on Windows
//! (`FSCTL_DUPLICATE_EXTENTS_TO_FILE`; a Dev Drive is ReFS). No fallback to copying: potluck
//! requires a CoW filesystem and refuses to run without one.
//!
//! A clone keeps each file's mode and modification time (and on macOS its other attributes), so
//! git's stat check (with `core.checkStat=minimal`, as potluck's mirrors set it) sees cloned files
//! as unchanged. On Linux, directories are recreated with their modes and mtimes, and symlinks with
//! their targets and mtimes; files are cloned by several threads at once. Other file types
//! (sockets, fifos, devices) are refused. On macOS, clonefile gives a directory that has entries
//! the time of the clone as its mtime (git ignores directory mtimes).
//!
//! On Windows, as on Linux, directories are recreated and files cloned by several threads at once,
//! each keeping its creation, access and modification times and its read-only, hidden, system,
//! archive and not-indexed attributes; directories keep theirs too. A file is created, made sparse
//! and given the source's integrity-stream setting when the source has them (ReFS clones only
//! between files that agree on both), sized, then cloned in cluster-aligned pieces under 4 GiB
//! each (ReFS's limits for one call). Symbolic links are recreated as file or directory links
//! with their targets and times; Windows lets a user create them only with Developer Mode on or
//! the Create symbolic links privilege, and a clone that meets one without either fails saying so.

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
            "{} is on {}, which can't make copy-on-write clones ({e:#}). {NEEDS}",
            dir.display(),
            fs_name(dir)
        )
    })
}

/// What potluck needs of its state directory's filesystem, and how to give it that.
#[cfg(unix)]
const NEEDS: &str = "potluck needs its state directory on APFS, btrfs, or XFS with reflink: set \
                     POTLUCK_HOME to a directory on one";
#[cfg(windows)]
const NEEDS: &str = "potluck needs its state directory on a Dev Drive or another ReFS volume: set \
                     POTLUCK_HOME, a user environment variable, to a directory on one";

/// The filesystem `path` is on, in words.
pub fn fs_name(path: &Path) -> String {
    imp::fs_name(path)
}

#[cfg(unix)]
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

/// Cloning a tree entry by entry with several threads (Linux and Windows): directories are
/// recreated, every other entry is cloned by the platform's [`imp::clone_entry`], and each
/// directory gets its own mode or attributes and times last ([`imp::finish_dir`]), once nothing
/// more is made in it.
#[cfg(any(target_os = "linux", windows))]
mod tree {
    use super::imp::{clone_entry, finish_dir, new_dir};
    use std::fs::{self, Metadata};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex};

    pub fn clone_tree(src: &Path, dst: &Path) -> anyhow::Result<()> {
        let meta = fs::symlink_metadata(src)?;
        let ft = meta.file_type();
        if ft.is_dir() {
            new_dir(dst)?;
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
                new_dir(&d)?;
                finished_dirs.lock().unwrap().push((d.clone(), meta));
                queue.lock().unwrap().dirs.push((s, d));
                wake.notify_one();
            } else {
                clone_entry(&s, &d, &meta)?;
            }
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use super::c_path;
    pub use super::tree::{clone_dir_contents, clone_tree};
    use anyhow::bail;
    use std::fs::{self, File, Metadata, OpenOptions};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::os::unix::io::AsRawFd;
    use std::path::Path;

    /// A new directory of the clone, private until [`finish_dir`] gives it its mode.
    pub fn new_dir(dir: &Path) -> anyhow::Result<()> {
        fs::create_dir(dir)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
        Ok(())
    }

    pub fn clone_entry(src: &Path, dst: &Path, meta: &Metadata) -> anyhow::Result<()> {
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

    pub fn finish_dir(dir: &Path, meta: &Metadata) -> anyhow::Result<()> {
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

#[cfg(windows)]
mod imp {
    pub use super::tree::{clone_dir_contents, clone_tree};
    use crate::sys::win::wide;
    use anyhow::{Context, bail};
    use std::fs::{self, File, Metadata, OpenOptions};
    use std::os::windows::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;
    use windows_sys::Win32::Foundation::{ERROR_PRIVILEGE_NOT_HELD, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_ARCHIVE, FILE_ATTRIBUTE_HIDDEN, FILE_ATTRIBUTE_NORMAL,
        FILE_ATTRIBUTE_NOT_CONTENT_INDEXED, FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_SPARSE_FILE,
        FILE_ATTRIBUTE_SYSTEM, FILE_BASIC_INFO, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES, FileBasicInfo,
        GetVolumeInformationByHandleW, SetFileAttributesW, SetFileInformationByHandle,
    };
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::{
        DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE, FSCTL_GET_INTEGRITY_INFORMATION,
        FSCTL_GET_INTEGRITY_INFORMATION_BUFFER, FSCTL_SET_INTEGRITY_INFORMATION,
        FSCTL_SET_INTEGRITY_INFORMATION_BUFFER, FSCTL_SET_SPARSE,
    };

    /// The attributes a clone keeps.
    const KEPT: u32 = FILE_ATTRIBUTE_READONLY
        | FILE_ATTRIBUTE_HIDDEN
        | FILE_ATTRIBUTE_SYSTEM
        | FILE_ATTRIBUTE_ARCHIVE
        | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED;

    /// A new directory of the clone; its attributes and times come last ([`finish_dir`]).
    pub fn new_dir(dir: &Path) -> anyhow::Result<()> {
        fs::create_dir(dir)?;
        Ok(())
    }

    pub fn clone_entry(src: &Path, dst: &Path, meta: &Metadata) -> anyhow::Result<()> {
        let ft = meta.file_type();
        if ft.is_symlink() {
            let target = fs::read_link(src)?;
            // std creates it with SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE (Developer Mode).
            let made = if ft.is_symlink_dir() {
                std::os::windows::fs::symlink_dir(&target, dst)
            } else {
                std::os::windows::fs::symlink_file(&target, dst)
            };
            if let Err(e) = made {
                if e.raw_os_error() == Some(ERROR_PRIVILEGE_NOT_HELD as i32) {
                    bail!(
                        "can't recreate the symbolic link {}: Windows lets this user create \
                         symbolic links only with Developer Mode on (Settings, System, For \
                         developers) or the Create symbolic links privilege",
                        src.display()
                    );
                }
                return Err(e)
                    .with_context(|| format!("can't create the symbolic link {}", dst.display()));
            }
            let link = OpenOptions::new()
                .access_mode(FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS)
                .open(dst)?;
            set_times(&link, meta, dst)
        } else if ft.is_file() {
            let d = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(dst)?;
            if let Err(e) = clone_file(src, &d, meta) {
                drop(d);
                let _ = fs::remove_file(dst);
                return Err(e.context(format!("can't clone {}", src.display())));
            }
            drop(d);
            set_attributes(dst, meta, FILE_ATTRIBUTE_ARCHIVE)
        } else {
            bail!(
                "{} is not a file, directory or symlink: refusing to clone it",
                src.display()
            )
        }
    }

    /// Clones the file `src` (with `meta`) into the new, empty file `d`, and gives it `src`'s
    /// times.
    fn clone_file(src: &Path, d: &File, meta: &Metadata) -> anyhow::Result<()> {
        let s = File::open(src)?;
        if meta.file_attributes() & FILE_ATTRIBUTE_SPARSE_FILE != 0 {
            ioctl(d, FSCTL_SET_SPARSE, &[] as &[u8], &mut [] as &mut [u8])
                .context("can't make the clone sparse")?;
        }
        let (from, cluster) = integrity(&s).context("can't read the integrity-stream setting")?;
        let (to, _) = integrity(d).context("can't read the integrity-stream setting")?;
        if (from.ChecksumAlgorithm, from.Flags) != (to.ChecksumAlgorithm, to.Flags) {
            let set = FSCTL_SET_INTEGRITY_INFORMATION_BUFFER {
                ChecksumAlgorithm: from.ChecksumAlgorithm,
                Reserved: 0,
                Flags: from.Flags,
            };
            ioctl(
                d,
                FSCTL_SET_INTEGRITY_INFORMATION,
                as_bytes(&set),
                &mut [] as &mut [u8],
            )
            .context("can't give the clone the source's integrity-stream setting")?;
        }
        let len = meta.len();
        if len > 0 {
            d.set_len(len)?;
            // Whole clusters up to the end of file (the last one past it, which ReFS takes as
            // the end), in pieces under 4 GiB that are whole clusters too.
            let total = len.div_ceil(cluster) * cluster;
            let piece = (1u64 << 32) - cluster;
            let mut at = 0u64;
            while at < total {
                let n = piece.min(total - at);
                let dup = DUPLICATE_EXTENTS_DATA {
                    FileHandle: s.as_raw_handle() as HANDLE,
                    SourceFileOffset: at as i64,
                    TargetFileOffset: at as i64,
                    ByteCount: n as i64,
                };
                ioctl(
                    d,
                    FSCTL_DUPLICATE_EXTENTS_TO_FILE,
                    as_bytes(&dup),
                    &mut [] as &mut [u8],
                )?;
                at += n;
            }
        }
        set_times(d, meta, src)
    }

    /// The integrity-stream setting of `f` (as FSCTL_SET_INTEGRITY_INFORMATION takes it) and its
    /// volume's cluster size. Fails on a filesystem without integrity streams, which can't
    /// clone either (NTFS).
    fn integrity(f: &File) -> anyhow::Result<(FSCTL_GET_INTEGRITY_INFORMATION_BUFFER, u64)> {
        let mut info = FSCTL_GET_INTEGRITY_INFORMATION_BUFFER {
            ChecksumAlgorithm: 0,
            Reserved: 0,
            Flags: 0,
            ChecksumChunkSizeInBytes: 0,
            ClusterSizeInBytes: 0,
        };
        // SAFETY: info is plain data; the call fills it and nothing past it.
        let out = unsafe {
            std::slice::from_raw_parts_mut(
                (&mut info as *mut FSCTL_GET_INTEGRITY_INFORMATION_BUFFER).cast::<u8>(),
                std::mem::size_of::<FSCTL_GET_INTEGRITY_INFORMATION_BUFFER>(),
            )
        };
        ioctl(f, FSCTL_GET_INTEGRITY_INFORMATION, &[] as &[u8], out)?;
        let cluster = u64::from(info.ClusterSizeInBytes);
        if cluster == 0 || !cluster.is_power_of_two() {
            bail!("the volume gives no cluster size ({cluster})");
        }
        Ok((info, cluster))
    }

    /// `value`'s bytes, for an ioctl's input.
    fn as_bytes<T>(value: &T) -> &[u8] {
        // SAFETY: a plain-data struct of the Windows API, read as the bytes it is.
        unsafe {
            std::slice::from_raw_parts((value as *const T).cast::<u8>(), std::mem::size_of::<T>())
        }
    }

    /// DeviceIoControl on `f` with `input`, filling `output`.
    pub(super) fn ioctl(
        f: &File,
        code: u32,
        input: &[u8],
        output: &mut [u8],
    ) -> std::io::Result<()> {
        let mut returned = 0u32;
        let ptr = |len: usize, p: *const u8| if len == 0 { std::ptr::null() } else { p };
        // SAFETY: a valid handle; each buffer is valid for the length given (null when empty),
        // and the call is synchronous (no OVERLAPPED).
        let ok = unsafe {
            DeviceIoControl(
                f.as_raw_handle() as HANDLE,
                code,
                ptr(input.len(), input.as_ptr()).cast(),
                input.len() as u32,
                ptr(output.len(), output.as_ptr()).cast_mut().cast(),
                output.len() as u32,
                &mut returned,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Gives the open file `f` (`path`, for messages) the creation, access and modification
    /// times in `meta`, leaving its attributes as they are.
    fn set_times(f: &File, meta: &Metadata, path: &Path) -> anyhow::Result<()> {
        let info = FILE_BASIC_INFO {
            CreationTime: meta.creation_time() as i64,
            LastAccessTime: meta.last_access_time() as i64,
            LastWriteTime: meta.last_write_time() as i64,
            // 0: unchanged.
            ChangeTime: 0,
            FileAttributes: 0,
        };
        // SAFETY: a valid handle with write-attributes access, and a FILE_BASIC_INFO of the size
        // given.
        let ok = unsafe {
            SetFileInformationByHandle(
                f.as_raw_handle() as HANDLE,
                FileBasicInfo,
                (&info as *const FILE_BASIC_INFO).cast(),
                std::mem::size_of::<FILE_BASIC_INFO>() as u32,
            )
        };
        if ok == 0 {
            bail!(
                "can't set the times of {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            );
        }
        Ok(())
    }

    /// Gives `path` the attributes of `meta` that a clone keeps, unless it has them already as
    /// made (`made`: what Windows gives a new one).
    fn set_attributes(path: &Path, meta: &Metadata, made: u32) -> anyhow::Result<()> {
        let want = meta.file_attributes() & KEPT;
        if want == made {
            return Ok(());
        }
        let p = wide(path);
        let value = if want == 0 {
            FILE_ATTRIBUTE_NORMAL
        } else {
            want
        };
        // SAFETY: p is NUL-terminated.
        if unsafe { SetFileAttributesW(p.as_ptr(), value) } == 0 {
            bail!(
                "can't set the attributes of {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            );
        }
        Ok(())
    }

    pub fn finish_dir(dir: &Path, meta: &Metadata) -> anyhow::Result<()> {
        // A new directory has no attribute a clone keeps.
        set_attributes(dir, meta, 0)?;
        let d = OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(dir)?;
        set_times(&d, meta, dir)
    }

    pub fn fs_name(path: &Path) -> String {
        let unknown = || "an unknown filesystem".to_string();
        let Ok(f) = OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)
        else {
            return unknown();
        };
        let mut name = [0u16; 64];
        // SAFETY: a valid handle; name has room for name.len() units, the other outputs are null.
        let ok = unsafe {
            GetVolumeInformationByHandleW(
                f.as_raw_handle() as HANDLE,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                name.as_mut_ptr(),
                name.len() as u32,
            )
        };
        if ok == 0 {
            return unknown();
        }
        let name = crate::sys::win::from_wide(&name);
        if name.is_empty() { unknown() } else { name }
    }
}

/// Windows clones. They need a directory on ReFS (a Dev Drive): the target directory, as CI and
/// the test machines have it.
#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::fs::{self, File, FileTimes};
    use std::os::windows::fs::{FileTimesExt, MetadataExt};
    use std::time::{Duration, SystemTime};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_HIDDEN, FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_SPARSE_FILE,
    };

    /// A new directory beside the test binary, in the target directory.
    fn refs_dir() -> tempfile::TempDir {
        let exe = std::env::current_exe().unwrap();
        let dir = tempfile::tempdir_in(exe.parent().unwrap()).unwrap();
        assert_eq!(
            fs_name(dir.path()),
            "ReFS",
            "these tests need the target directory on ReFS (a Dev Drive): {}",
            dir.path().display()
        );
        dir
    }

    fn set_times(path: &Path, secs: u64) {
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_nanos(1234500);
        let times = FileTimes::new()
            .set_accessed(t)
            .set_modified(t + Duration::from_secs(60))
            .set_created(t - Duration::from_secs(60));
        let f = fs::OpenOptions::new()
            .access_mode(windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES)
            .custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)
            .unwrap();
        f.set_times(times).unwrap();
    }

    use std::os::windows::fs::OpenOptionsExt;

    fn set_attrs(path: &Path, attrs: u32) {
        let p = crate::sys::win::wide(path);
        // SAFETY: p is NUL-terminated.
        assert_ne!(
            unsafe {
                windows_sys::Win32::Storage::FileSystem::SetFileAttributesW(p.as_ptr(), attrs)
            },
            0
        );
    }

    /// Whether this user may create symbolic links (Developer Mode or the privilege).
    fn can_symlink(dir: &Path) -> bool {
        let probe = dir.join("probe-link");
        let ok = std::os::windows::fs::symlink_file("nowhere", &probe).is_ok();
        let _ = fs::remove_file(&probe);
        ok
    }

    #[test]
    fn a_tree_clones_with_its_contents_times_and_attributes() {
        let dir = refs_dir();
        let src = dir.path().join("src");
        fs::create_dir_all(src.join("sub/deeper")).unwrap();
        let big: Vec<u8> = (0..1_000_003u32).map(|i| (i * 7 % 251) as u8).collect();
        fs::write(src.join("big.bin"), &big).unwrap();
        fs::write(src.join("empty"), b"").unwrap();
        fs::write(src.join("small.txt"), b"hello\r\n").unwrap();
        fs::write(src.join("sub/readonly.txt"), b"keep me").unwrap();
        fs::write(src.join("sub/deeper/hidden.txt"), b"shh").unwrap();
        let links = can_symlink(dir.path());
        if links {
            std::os::windows::fs::symlink_file("big.bin", src.join("link-to-file")).unwrap();
            std::os::windows::fs::symlink_dir("sub", src.join("link-to-dir")).unwrap();
        }
        for (i, p) in [
            "big.bin",
            "empty",
            "small.txt",
            "sub/readonly.txt",
            "sub/deeper/hidden.txt",
            "sub/deeper",
            "sub",
        ]
        .iter()
        .enumerate()
        {
            set_times(&src.join(p), 1_000_000_000 + i as u64 * 3600);
        }
        set_attrs(&src.join("sub/readonly.txt"), FILE_ATTRIBUTE_READONLY);
        set_attrs(&src.join("sub/deeper/hidden.txt"), FILE_ATTRIBUTE_HIDDEN);
        set_attrs(&src.join("sub"), FILE_ATTRIBUTE_HIDDEN);

        let dst = dir.path().join("dst");
        clone_tree(&src, &dst).unwrap();

        assert_eq!(fs::read(dst.join("big.bin")).unwrap(), big);
        assert_eq!(fs::read(dst.join("empty")).unwrap(), b"");
        assert_eq!(fs::read(dst.join("small.txt")).unwrap(), b"hello\r\n");
        assert_eq!(fs::read(dst.join("sub/readonly.txt")).unwrap(), b"keep me");
        let kept = |m: &fs::Metadata| {
            (
                m.last_write_time(),
                m.creation_time(),
                m.file_attributes() & (FILE_ATTRIBUTE_READONLY | FILE_ATTRIBUTE_HIDDEN),
                m.len(),
            )
        };
        for p in [
            "big.bin",
            "empty",
            "small.txt",
            "sub/readonly.txt",
            "sub/deeper/hidden.txt",
            "sub/deeper",
            "sub",
        ] {
            let (a, b) = (
                fs::symlink_metadata(src.join(p)).unwrap(),
                fs::symlink_metadata(dst.join(p)).unwrap(),
            );
            if b.is_file() {
                assert_eq!(kept(&a), kept(&b), "{p}");
            } else {
                assert_eq!((kept(&a).0, kept(&a).2), (kept(&b).0, kept(&b).2), "{p}");
            }
        }
        assert!(
            fs::metadata(dst.join("sub/readonly.txt"))
                .unwrap()
                .permissions()
                .readonly()
        );
        if links {
            assert_eq!(
                fs::read_link(dst.join("link-to-file")).unwrap(),
                Path::new("big.bin")
            );
            assert_eq!(
                fs::read_link(dst.join("link-to-dir")).unwrap(),
                Path::new("sub")
            );
            use std::os::windows::fs::FileTypeExt;
            let ft = |p: &str| fs::symlink_metadata(dst.join(p)).unwrap().file_type();
            assert!(ft("link-to-file").is_symlink_file());
            assert!(ft("link-to-dir").is_symlink_dir());
            assert_eq!(fs::read(dst.join("link-to-file")).unwrap(), big);
        }
        // A write to the clone leaves the source as it was.
        fs::write(dst.join("small.txt"), b"changed").unwrap();
        assert_eq!(fs::read(src.join("small.txt")).unwrap(), b"hello\r\n");
        // Into an existing directory, skipping a top-level name.
        let into = dir.path().join("into");
        fs::create_dir(&into).unwrap();
        clone_dir_contents(&src, &into, &["big.bin"]).unwrap();
        assert!(!into.join("big.bin").exists());
        assert_eq!(
            fs::read(into.join("sub/deeper/hidden.txt")).unwrap(),
            b"shh"
        );
        assert!(clone_tree(&src, &dst).is_err(), "the destination exists");
        // Clean up what is read-only.
        for root in [&src, &dst, &into] {
            set_attrs(&root.join("sub/readonly.txt"), 0x80);
        }
    }

    /// A sparse file over 4 GiB clones in pieces, and stays sparse.
    #[test]
    fn a_sparse_file_over_4_gib_clones_in_pieces() {
        let dir = refs_dir();
        let src = dir.path().join("sparse");
        let f = File::create(&src).unwrap();
        imp::ioctl(
            &f,
            windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE,
            &[],
            &mut [],
        )
        .unwrap();
        let len = (9u64 << 30) / 2 + 12345;
        f.set_len(len).unwrap();
        use std::os::windows::fs::FileExt;
        f.seek_write(b"start", 0).unwrap();
        f.seek_write(b"middle", 1 << 32).unwrap();
        f.seek_write(b"end", len - 3).unwrap();
        drop(f);
        let dst = dir.path().join("sparse.clone");
        clone_tree(&src, &dst).unwrap();
        let m = fs::metadata(&dst).unwrap();
        assert_eq!(m.len(), len);
        assert_ne!(m.file_attributes() & FILE_ATTRIBUTE_SPARSE_FILE, 0);
        let d = File::open(&dst).unwrap();
        let mut buf = [0u8; 6];
        d.seek_read(&mut buf[..5], 0).unwrap();
        assert_eq!(&buf[..5], b"start");
        d.seek_read(&mut buf, 1 << 32).unwrap();
        assert_eq!(&buf, b"middle");
        d.seek_read(&mut buf[..3], len - 3).unwrap();
        assert_eq!(&buf[..3], b"end");
    }

    /// The state directory's check passes on ReFS and, on NTFS (the system drive), says what
    /// to do.
    #[test]
    fn check_says_why_not_on_ntfs() {
        let dir = refs_dir();
        check(dir.path()).unwrap();
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0, "left behind");
        let ntfs = tempfile::tempdir().unwrap();
        if fs_name(ntfs.path()) != "NTFS" {
            return;
        }
        let e = format!("{:#}", check(ntfs.path()).unwrap_err());
        assert!(
            e.contains("is on NTFS, which can't make copy-on-write clones")
                && e.contains("a Dev Drive or another ReFS volume")
                && e.contains("set POTLUCK_HOME, a user environment variable"),
            "{e}"
        );
        assert_eq!(fs::read_dir(ntfs.path()).unwrap().count(), 0, "left behind");
    }
}
