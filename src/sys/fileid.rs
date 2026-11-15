//! Which file an open file or a path is: its device and inode on Unix, its volume's serial number
//! and its 128-bit file id on Windows (the 64-bit index isn't unique on ReFS).

use std::fs::File;
use std::io;
use std::path::Path;

/// A file's identity: equal for two handles or paths on the same file, and only then.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileId {
    volume: u64,
    file: u128,
}

impl FileId {
    /// The file `f` is open on.
    pub fn of(f: &File) -> io::Result<FileId> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let m = f.metadata()?;
            Ok(FileId {
                volume: m.dev(),
                file: u128::from(m.ino()),
            })
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsHandle;
            FileId::of_handle(f.as_handle())
        }
    }

    /// The file at `path` (symlinks followed).
    pub fn of_path(path: &Path) -> io::Result<FileId> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let m = std::fs::metadata(path)?;
            Ok(FileId {
                volume: m.dev(),
                file: u128::from(m.ino()),
            })
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
            };
            // No access asked: reading its id needs none, and a file open elsewhere (a running
            // binary) or a directory opens all the same.
            let f = std::fs::OpenOptions::new()
                .access_mode(0)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(path)?;
            FileId::of(&f)
        }
    }

    /// The file a handle is open on (a standard handle, say).
    #[cfg(windows)]
    pub fn of_handle(h: std::os::windows::io::BorrowedHandle<'_>) -> io::Result<FileId> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
        };
        // SAFETY: plain data, filled by the call.
        let mut info: FILE_ID_INFO = unsafe { std::mem::zeroed() };
        // SAFETY: an open handle and a buffer of the size given.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                h.as_raw_handle(),
                FileIdInfo,
                (&mut info as *mut FILE_ID_INFO).cast(),
                std::mem::size_of::<FILE_ID_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(FileId {
            volume: info.VolumeSerialNumber,
            file: u128::from_le_bytes(info.FileId.Identifier),
        })
    }
}

/// `path` made absolute with every symlink resolved, as `canonicalize` does. On Windows without
/// the `\\?\` prefix `canonicalize` puts before it when the path can be written without one (a
/// drive letter's, or a share's as `\\server\share`), so that programs that don't take such
/// paths (git) read it.
pub fn canonical(path: &Path) -> io::Result<std::path::PathBuf> {
    let real = std::fs::canonicalize(path)?;
    #[cfg(windows)]
    {
        if let Some(text) = real.to_str() {
            if let Some(unc) = text.strip_prefix(r"\\?\UNC\") {
                return Ok(format!(r"\\{unc}").into());
            }
            if let Some(rest) = text.strip_prefix(r"\\?\")
                && rest.as_bytes().get(1) == Some(&b':')
                && rest.len() < 260
            {
                return Ok(rest.into());
            }
        }
    }
    Ok(real)
}

/// Whether `a` and `b` are one file (symlinks followed); false if either can't be read.
pub fn same_file(a: &Path, b: &Path) -> bool {
    matches!((FileId::of_path(a), FileId::of_path(b)), (Ok(x), Ok(y)) if x == y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_canonical_path_is_absolute_and_plain() {
        let d = tempfile::tempdir().unwrap();
        let c = canonical(d.path()).unwrap();
        assert!(c.is_absolute(), "{}", c.display());
        assert!(!c.to_string_lossy().starts_with(r"\\?\"), "{}", c.display());
        assert!(same_file(&c, d.path()));
    }

    #[test]
    fn a_file_is_itself_and_no_other() {
        let d = tempfile::tempdir().unwrap();
        let (a, b) = (d.path().join("a"), d.path().join("b"));
        std::fs::write(&a, "a").unwrap();
        std::fs::write(&b, "b").unwrap();
        let f = File::open(&a).unwrap();
        assert_eq!(FileId::of(&f).unwrap(), FileId::of_path(&a).unwrap());
        assert!(same_file(&a, &a));
        assert!(!same_file(&a, &b));
        assert!(!same_file(&a, &d.path().join("none")));
        assert_eq!(
            FileId::of_path(d.path()).unwrap(),
            FileId::of_path(d.path()).unwrap()
        );
        // Renamed over: the path names another file now; the open one is still the first.
        std::fs::rename(&b, &a).unwrap();
        assert_ne!(FileId::of(&f).unwrap(), FileId::of_path(&a).unwrap());
    }
}
