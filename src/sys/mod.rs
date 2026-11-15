//! What differs between the operating systems potluck runs on (Linux, macOS and Windows), for the
//! code that is otherwise the same everywhere: private files and directories, processes and the
//! processes they start, the host's name, the user, local time and free disk space.
//!
//! - **Private files.** On Unix a mode: 0600 for a file only this user reads, 0700 for a
//!   directory. Windows has no modes: a mode without group or other bits (`mode & 0o077 == 0`)
//!   gives the file or directory a protected access list that lets only this user in (no
//!   inherited entries, not even Administrators or SYSTEM), and any other mode leaves it to inherit
//!   its directory's. A new file always inherits its directory's on Windows, so a private file
//!   belongs in a private directory, as each of potluck's does.
//! - **Process groups.** A [`Group`] is a child and everything it starts: on Unix the child leads
//!   a process group of its own, on Windows it runs in a job object of its own from its first
//!   instruction (it starts suspended until it is in the job).
//! - **Which file.** [`FileId`]: device and inode on Unix, volume and file id on Windows.
//! - **The local socket.** [`local`]: the daemon's Unix domain socket, std's on Unix and AF_UNIX
//!   through Winsock on Windows.

mod fileid;
mod host;
pub mod local;
mod path;
mod perm;
mod private;
mod proc;
#[cfg(windows)]
pub(crate) mod registry;

pub use fileid::{FileId, same_file};
pub use host::{LocalTime, free_bytes, home_dir, home_var, hostname, local_time, user_name};
pub use path::canonicalize;
#[cfg(windows)]
pub use perm::user_sid;
pub use perm::{create_dir, create_dir_all, file_mode, set_mode};
pub use private::is_private;
#[cfg(windows)]
pub(crate) use proc::resume_threads;
pub use proc::{Group, kill_pid, pid_alive};

/// `path` as UTF-8 text, or an error naming it. Paths potluck keeps are UTF-8 (the state
/// directory and config paths are checked at start, [`crate::preflight`]); on Unix a path is
/// bytes and on Windows UTF-16, so code that needs a path's bytes takes them from its text.
pub fn path_str(path: &std::path::Path) -> std::io::Result<&str> {
    path.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} is not UTF-8", path.display()),
        )
    })
}

/// The executable suffix of this platform: `.exe` on Windows, nothing elsewhere.
pub const EXE_SUFFIX: &str = std::env::consts::EXE_SUFFIX;

/// `s`, a copy of a std socket (`try_clone` of a TCP listener or stream), made so that no child
/// process inherits it. On Unix std's copy is close-on-exec already. On Windows it is
/// inheritable whatever flag std asks for (WSASocketW from WSADuplicateSocketW's description), so
/// every process started with inherited handles while it was open (`std::process::Command`
/// starts each so) held the socket: a copy of the mesh's listener kept its port taken after the
/// daemon closed it, as long as such a process ran. This clears the flag; a process another
/// thread starts between the copy and this still inherits it.
#[cfg(windows)]
pub fn not_inherited<S: std::os::windows::io::AsRawSocket>(s: S) -> S {
    use windows_sys::Win32::Foundation::{HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation};
    // SAFETY: a socket is a handle, open while `s` lives; clearing its inherit flag only keeps it
    // from child processes. Best effort: on failure it stays as std made it.
    unsafe { SetHandleInformation(s.as_raw_socket() as HANDLE, HANDLE_FLAG_INHERIT, 0) };
    s
}

/// `s`, a copy of a std socket made so that no child process inherits it: on Unix, as std makes
/// it (close-on-exec).
#[cfg(unix)]
pub fn not_inherited<S>(s: S) -> S {
    s
}

#[cfg(windows)]
pub(crate) mod win {
    //! Small helpers for the Windows API.

    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};

    /// `s` as a NUL-terminated UTF-16 string.
    pub fn wide(s: impl AsRef<OsStr>) -> Vec<u16> {
        s.as_ref().encode_wide().chain(std::iter::once(0)).collect()
    }

    /// A UTF-16 buffer up to its first NUL, as text.
    pub fn from_wide(buf: &[u16]) -> String {
        let end = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
        String::from_utf16_lossy(&buf[..end])
    }

    /// A handle closed on drop. Null and INVALID_HANDLE_VALUE are never closed.
    pub struct Owned(pub HANDLE);

    impl Owned {
        /// `None` for a null or invalid handle (the call failed; `GetLastError` says why, and
        /// still does after this: no `Owned` is made, so none is dropped and closed).
        pub fn new(h: HANDLE) -> Option<Owned> {
            if h.is_null() || h == INVALID_HANDLE_VALUE {
                None
            } else {
                Some(Owned(h))
            }
        }
    }

    impl Drop for Owned {
        fn drop(&mut self) {
            // SAFETY: we own the handle, and it is valid (checked in `new`).
            unsafe { CloseHandle(self.0) };
        }
    }

    // SAFETY: a kernel handle may be used and closed from any thread.
    unsafe impl Send for Owned {}
    // SAFETY: as above; the handle is only read.
    unsafe impl Sync for Owned {}
}
