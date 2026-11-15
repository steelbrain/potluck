//! Private files and directories: a mode on Unix, an access list on Windows (see [`super`]).

use std::fs::OpenOptions;
use std::io;
use std::path::Path;

/// Whether `mode` keeps everyone but the user out.
#[cfg_attr(unix, allow(dead_code))]
fn private(mode: u32) -> bool {
    mode & 0o077 == 0
}

/// Gives a file that `opts` creates `mode` on Unix. On Windows it changes nothing: the file
/// inherits its directory's access list, so a private file belongs in a private directory
/// ([`create_dir`] with a private mode), as every private file of potluck's does.
pub fn file_mode(opts: &mut OpenOptions, mode: u32) -> &mut OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(mode)
    }
    #[cfg(windows)]
    {
        let _ = mode;
        opts
    }
}

/// Creates the directory `path` (its parent must exist) with `mode`, as
/// `DirBuilder::new().mode(mode).create(path)` does on Unix.
pub fn create_dir(path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(mode).create(path)
    }
    #[cfg(windows)]
    {
        if private(mode) {
            win::create_private_dir(path)
        } else {
            std::fs::create_dir(path)
        }
    }
}

/// Creates `path` and its missing parents, each with `mode`, as
/// `DirBuilder::new().recursive(true).mode(mode).create(path)` does on Unix. A directory that
/// exists already is left as it is.
pub fn create_dir_all(path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(mode)
            .create(path)
    }
    #[cfg(windows)]
    {
        if path.is_dir() {
            return Ok(());
        }
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            create_dir_all(parent, mode)?;
        }
        match create_dir(path, mode) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
            other => other,
        }
    }
}

/// Sets `path`'s mode, as `set_permissions(path, Permissions::from_mode(mode))` does on Unix. On
/// Windows a private mode replaces its access list with one that lets only this user in; any other
/// mode changes nothing.
pub fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
    }
    #[cfg(windows)]
    {
        if private(mode) {
            win::make_private(path)
        } else {
            Ok(())
        }
    }
}

#[cfg(windows)]
mod win {
    use super::super::win::{Owned, wide};
    use std::io;
    use std::path::Path;
    use std::sync::OnceLock;
    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        SDDL_REVISION_1, SE_FILE_OBJECT, SetNamedSecurityInfoW,
    };
    use windows_sys::Win32::Security::{
        ACL, DACL_SECURITY_INFORMATION, GetSecurityDescriptorDacl, GetTokenInformation,
        PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES,
        TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::CreateDirectoryW;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    /// The security descriptor that lets only this process's user in, with full access, for
    /// files and (inherited by what is made in them) directories. Made once and kept for the
    /// life of the process.
    struct Private {
        sd: PSECURITY_DESCRIPTOR,
        sa: SECURITY_ATTRIBUTES,
    }

    // SAFETY: the descriptor is never written after it is made, and lives as long as the process.
    unsafe impl Send for Private {}
    // SAFETY: as above.
    unsafe impl Sync for Private {}

    fn private() -> Option<&'static Private> {
        static PRIVATE: OnceLock<Option<Private>> = OnceLock::new();
        PRIVATE
            .get_or_init(|| {
                let sid = user_sid().ok()?;
                // P: protected (nothing inherited from above); OICI: what is made inside a
                // directory inherits the entry; FA: full access; for this user's SID alone.
                let sddl = wide(format!("D:P(A;OICI;FA;;;{sid})"));
                let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
                // SAFETY: sddl is NUL-terminated; sd receives a LocalAlloc'd descriptor, which is
                // kept for the life of the process.
                let ok = unsafe {
                    ConvertStringSecurityDescriptorToSecurityDescriptorW(
                        sddl.as_ptr(),
                        SDDL_REVISION_1,
                        &mut sd,
                        std::ptr::null_mut(),
                    )
                };
                if ok == 0 {
                    return None;
                }
                Some(Private {
                    sd,
                    sa: SECURITY_ATTRIBUTES {
                        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
                        lpSecurityDescriptor: sd,
                        bInheritHandle: 0,
                    },
                })
            })
            .as_ref()
    }

    /// Security attributes for a new directory only this user can open (what is made in it
    /// inherits that); `None` if they couldn't be made (it then inherits its parent's access list).
    pub fn private_attributes() -> Option<*mut SECURITY_ATTRIBUTES> {
        private().map(|p| &p.sa as *const SECURITY_ATTRIBUTES as *mut SECURITY_ATTRIBUTES)
    }

    pub fn create_private_dir(path: &Path) -> io::Result<()> {
        let p = wide(path);
        let sa = private_attributes().unwrap_or(std::ptr::null_mut());
        // SAFETY: p is NUL-terminated; sa is null or points to attributes kept for the process.
        if unsafe { CreateDirectoryW(p.as_ptr(), sa) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn make_private(path: &Path) -> io::Result<()> {
        let private = private().ok_or_else(|| io::Error::other("can't make a private ACL"))?;
        let mut present = 0;
        let mut defaulted = 0;
        let mut dacl: *mut ACL = std::ptr::null_mut();
        // SAFETY: the descriptor is valid; dacl points into it.
        if unsafe { GetSecurityDescriptorDacl(private.sd, &mut present, &mut dacl, &mut defaulted) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        let p = wide(path);
        // SAFETY: p is NUL-terminated; dacl is a valid ACL within a descriptor kept for the
        // process. PROTECTED drops the entries inherited from the parent.
        let rc = unsafe {
            SetNamedSecurityInfoW(
                p.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dacl,
                std::ptr::null(),
            )
        };
        if rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(rc as i32));
        }
        Ok(())
    }

    /// This process's user's SID as text (`S-1-5-21-...`).
    pub fn user_sid() -> io::Result<String> {
        let mut token = std::ptr::null_mut();
        // SAFETY: the pseudo handle of this process; token receives a handle we close.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let token = Owned(token);
        let mut len = 0u32;
        // SAFETY: a size query: no buffer, len receives the size needed.
        unsafe { GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &mut len) };
        // u64s keep the buffer aligned for TOKEN_USER.
        let mut buf = vec![0u64; (len as usize).div_ceil(8).max(1)];
        // SAFETY: buf has at least len bytes.
        if unsafe {
            GetTokenInformation(token.0, TokenUser, buf.as_mut_ptr().cast(), len, &mut len)
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: GetTokenInformation filled a TOKEN_USER at the start of the aligned buffer.
        let user = unsafe { &*(buf.as_ptr() as *const TOKEN_USER) };
        let mut text: windows_sys::core::PWSTR = std::ptr::null_mut();
        // SAFETY: the SID points into buf, alive here; text receives a LocalAlloc'd string.
        if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut text) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: text is a NUL-terminated string that we free after copying it.
        let s = unsafe {
            let len = (0..).take_while(|&i| *text.add(i) != 0).count();
            let s = String::from_utf16_lossy(std::slice::from_raw_parts(text, len));
            LocalFree(text.cast());
            s
        };
        Ok(s)
    }
}

/// This process's user's SID as text (`S-1-5-21-...`).
#[cfg(windows)]
pub fn user_sid() -> io::Result<String> {
    win::user_sid()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_files_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("a/b/c");
        create_dir_all(&d, 0o700).unwrap();
        create_dir_all(&d, 0o700).unwrap();
        let f = d.join("f");
        file_mode(OpenOptions::new().write(true).create_new(true), 0o600)
            .open(&f)
            .unwrap();
        set_mode(&f, 0o600).unwrap();
        create_dir(&dir.path().join("e"), 0o700).unwrap();
        assert!(create_dir(&dir.path().join("e"), 0o700).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&d), 0o700);
            assert_eq!(mode(&dir.path().join("a")), 0o700);
            assert_eq!(mode(&f), 0o600);
        }
    }
}
