//! Whether a file or directory is private already, as [`super::set_mode`] with a private mode
//! leaves it: on Unix no group or other bits; on Windows an access list with one entry, full
//! access for this user, which for a directory is protected (nothing inherited from above) and
//! passed on to what is made inside, and for a file may be inherited (from a private directory).
//! Setting a directory's access list on Windows reaches everything in it (the system walks the
//! tree to pass the list on), so a caller that makes a large directory private at every start
//! looks here first.

use std::io;
use std::path::Path;

/// Whether `path` lets only this user in.
pub fn is_private(path: &Path) -> io::Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        Ok(std::fs::metadata(path)?.permissions().mode() & 0o077 == 0)
    }
    #[cfg(windows)]
    {
        let dir = std::fs::metadata(path)?.is_dir();
        let sddl = win::dacl_sddl(path)?;
        Ok(win::private_sddl(&sddl, &super::user_sid()?, dir))
    }
}

#[cfg(windows)]
mod win {
    use super::super::win::wide;
    use std::io;
    use std::path::Path;
    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSecurityDescriptorToStringSecurityDescriptorW, GetNamedSecurityInfoW,
        SDDL_REVISION_1, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR};

    /// The access list of `path` in SDDL (`D:P(A;OICI;FA;;;S-1-5-21-...)`).
    pub fn dacl_sddl(path: &Path) -> io::Result<String> {
        let p = wide(path);
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: p is NUL-terminated; sd receives a LocalAlloc'd descriptor freed below; the
        // other outputs may be null.
        let rc = unsafe {
            GetNamedSecurityInfoW(
                p.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        if rc != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(rc as i32));
        }
        let mut text: windows_sys::core::PWSTR = std::ptr::null_mut();
        // SAFETY: sd is a valid descriptor; text receives a LocalAlloc'd string freed below.
        let ok = unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                sd,
                SDDL_REVISION_1,
                DACL_SECURITY_INFORMATION,
                &mut text,
                std::ptr::null_mut(),
            )
        };
        let e = io::Error::last_os_error();
        // SAFETY: sd was allocated by GetNamedSecurityInfoW and isn't used after.
        unsafe { LocalFree(sd) };
        if ok == 0 {
            return Err(e);
        }
        // SAFETY: text is a NUL-terminated string, freed after it is copied.
        let s = unsafe {
            let len = (0..).take_while(|&i| *text.add(i) != 0).count();
            let s = String::from_utf16_lossy(std::slice::from_raw_parts(text, len));
            LocalFree(text.cast());
            s
        };
        Ok(s)
    }

    /// Whether `sddl` is a list whose one entry gives `sid` full access; for a directory (`dir`)
    /// also protected, and passed on to what is made inside: what [`crate::sys::set_mode`] and
    /// [`crate::sys::create_dir`] give.
    pub fn private_sddl(sddl: &str, sid: &str, dir: bool) -> bool {
        let Some(rest) = sddl.strip_prefix("D:") else {
            return false;
        };
        let (flags, aces) = rest.split_at(rest.find('(').unwrap_or(rest.len()));
        let Some(ace) = aces.strip_prefix('(').and_then(|a| a.strip_suffix(')')) else {
            return false;
        };
        if ace.contains('(') || ace.contains(')') {
            return false;
        }
        let parts: Vec<&str> = ace.split(';').collect();
        let [kind, inherit, rights, "", "", who] = parts[..] else {
            return false;
        };
        let passed_on = inherit.contains("OI") && inherit.contains("CI");
        kind == "A" && rights == "FA" && who == sid && (!dir || (flags.contains('P') && passed_on))
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn only_a_list_for_this_user_alone_is_private() {
            let sid = "S-1-5-21-1-2-3-1001";
            let private = |sddl: &str, dir: bool| super::private_sddl(sddl, sid, dir);
            assert!(private("D:P(A;OICI;FA;;;S-1-5-21-1-2-3-1001)", true));
            assert!(private("D:PAI(A;OICI;FA;;;S-1-5-21-1-2-3-1001)", true));
            // A directory's own list, inherited from above.
            assert!(!private("D:AI(A;OICIID;FA;;;S-1-5-21-1-2-3-1001)", true));
            // A file's, inherited from a private directory.
            assert!(private("D:AI(A;ID;FA;;;S-1-5-21-1-2-3-1001)", false));
            // Someone else too.
            assert!(!private(
                "D:P(A;OICI;FA;;;S-1-5-21-1-2-3-1001)(A;OICI;0x1301bf;;;AU)",
                true
            ));
            assert!(!private(
                "D:AI(A;ID;FA;;;S-1-5-21-1-2-3-1001)(A;ID;FR;;;BU)",
                false
            ));
            assert!(!private("D:P(A;OICI;FA;;;S-1-5-21-9)", true));
            assert!(!private("D:P(D;OICI;FA;;;S-1-5-21-1-2-3-1001)", true));
            assert!(!private("D:P", true));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_private_directory_is_private_and_another_is_not() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("private");
        super::super::create_dir(&p, 0o700).unwrap();
        assert!(is_private(&p).unwrap());
        let open = d.path().join("open");
        std::fs::create_dir(&open).unwrap();
        #[cfg(unix)]
        super::super::set_mode(&open, 0o755).unwrap();
        assert!(!is_private(&open).unwrap());
        super::super::set_mode(&open, 0o700).unwrap();
        assert!(is_private(&open).unwrap());
    }
}
