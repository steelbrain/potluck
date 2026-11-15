//! The host's name, the user, local time and free disk space.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// The machine's host name as the OS reports it (on Unix `gethostname`, on Windows the DNS host
/// name), or `localhost` if it can't be read.
pub fn hostname() -> String {
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        // SAFETY: gethostname writes at most buf.len() bytes into buf.
        let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
        if rc != 0 {
            return "localhost".into();
        }
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        String::from_utf8_lossy(&buf[..end]).into_owned()
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::SystemInformation::{
            ComputerNameDnsHostname, GetComputerNameExW,
        };
        let mut buf = [0u16; 256];
        let mut len = buf.len() as u32;
        // SAFETY: buf has room for len UTF-16 units; len receives the length written.
        if unsafe { GetComputerNameExW(ComputerNameDnsHostname, buf.as_mut_ptr(), &mut len) } == 0 {
            return "localhost".into();
        }
        String::from_utf16_lossy(&buf[..len as usize])
    }
}

/// The name of the user this process runs as, if the OS knows one.
pub fn user_name() -> Option<String> {
    #[cfg(unix)]
    {
        let mut buf = vec![0u8; 16384];
        // SAFETY: passwd is plain data that getpwuid_r fills; its strings point into buf.
        let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
        let mut result = std::ptr::null_mut();
        // SAFETY: pwd and buf outlive the call; result is set to &pwd or null.
        let r = unsafe {
            libc::getpwuid_r(
                libc::getuid(),
                &mut pwd,
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut result,
            )
        };
        if r != 0 || result.is_null() || pwd.pw_name.is_null() {
            return None;
        }
        // SAFETY: pw_name is a NUL-terminated string in buf, alive here.
        let name = unsafe { std::ffi::CStr::from_ptr(pwd.pw_name) };
        Some(name.to_string_lossy().into_owned())
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::WindowsProgramming::GetUserNameW;
        let mut buf = [0u16; 257];
        let mut len = buf.len() as u32;
        // SAFETY: buf has room for len UTF-16 units; len receives the length with the NUL.
        if unsafe { GetUserNameW(buf.as_mut_ptr(), &mut len) } == 0 {
            return None;
        }
        Some(super::win::from_wide(&buf))
    }
}

/// The variable naming the user's home directory: `HOME` on Unix, `USERPROFILE` on Windows.
pub fn home_var() -> &'static str {
    if cfg!(windows) { "USERPROFILE" } else { "HOME" }
}

/// The user's home directory, from [`home_var`], if it is set and not empty.
pub fn home_dir() -> Option<PathBuf> {
    std::env::var_os(home_var())
        .filter(|h: &OsString| !h.is_empty())
        .map(PathBuf::from)
}

/// A moment as this machine's local time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalTime {
    pub year: i32,
    /// 1 to 12.
    pub month: u32,
    /// 1 to 31.
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
}

impl LocalTime {
    /// Whether `self` and `other` fall on the same local date.
    pub fn same_day(&self, other: &LocalTime) -> bool {
        (self.year, self.month, self.day) == (other.year, other.month, other.day)
    }
}

/// `secs` since the Unix epoch as local time, in the time zone (and its daylight saving rule)
/// in effect then; `None` if the OS can't convert it.
pub fn local_time(secs: i64) -> Option<LocalTime> {
    #[cfg(unix)]
    {
        let secs = secs as libc::time_t;
        // SAFETY: tm is plain data; localtime_r fills it from `secs` and touches nothing else.
        let mut tm: libc::tm = unsafe { std::mem::zeroed() };
        if unsafe { libc::localtime_r(&secs, &mut tm) }.is_null() {
            return None;
        }
        Some(LocalTime {
            year: tm.tm_year + 1900,
            month: (tm.tm_mon + 1) as u32,
            day: tm.tm_mday as u32,
            hour: tm.tm_hour as u32,
            minute: tm.tm_min as u32,
            second: tm.tm_sec as u32,
        })
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{FILETIME, SYSTEMTIME};
        use windows_sys::Win32::System::Time::{
            FileTimeToSystemTime, SystemTimeToTzSpecificLocalTime,
        };
        // FILETIME counts 100 ns intervals since 1601-01-01.
        let ft = u64::try_from(secs)
            .ok()?
            .checked_add(11_644_473_600)?
            .checked_mul(10_000_000)?;
        let ft = FILETIME {
            dwLowDateTime: ft as u32,
            dwHighDateTime: (ft >> 32) as u32,
        };
        // SAFETY: plain data, filled by the calls below.
        let mut utc: SYSTEMTIME = unsafe { std::mem::zeroed() };
        let mut local: SYSTEMTIME = unsafe { std::mem::zeroed() };
        // SAFETY: all pointers are to locals; a null time zone means the current one.
        let ok = unsafe {
            FileTimeToSystemTime(&ft, &mut utc) != 0
                && SystemTimeToTzSpecificLocalTime(std::ptr::null(), &utc, &mut local) != 0
        };
        ok.then_some(LocalTime {
            year: i32::from(local.wYear),
            month: u32::from(local.wMonth),
            day: u32::from(local.wDay),
            hour: u32::from(local.wHour),
            minute: u32::from(local.wMinute),
            second: u32::from(local.wSecond),
        })
    }
}

/// Bytes free for this user on the filesystem holding `path` (Unix `statvfs`'s `f_bavail`,
/// Windows `GetDiskFreeSpaceExW`'s bytes available to the caller), if it can be read.
pub fn free_bytes(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
        // SAFETY: statvfs fills `s`; `c` is a valid C string.
        unsafe {
            let mut s: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(c.as_ptr(), &mut s) != 0 {
                return None;
            }
            #[allow(clippy::useless_conversion)]
            Some(u64::from(s.f_bavail).saturating_mul(u64::from(s.f_frsize)))
        }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
        let p = super::win::wide(path);
        let mut avail = 0u64;
        // SAFETY: p is NUL-terminated; the other outputs may be null.
        let ok = unsafe {
            GetDiskFreeSpaceExW(
                p.as_ptr(),
                &mut avail,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        (ok != 0).then_some(avail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_facts() {
        assert!(!hostname().is_empty());
        assert!(user_name().is_some_and(|u| !u.is_empty()));
        assert!(free_bytes(&std::env::temp_dir()).is_some_and(|b| b > 0));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let t = local_time(now).unwrap();
        assert!(t.year >= 2024 && (1..=12).contains(&t.month) && (1..=31).contains(&t.day));
        assert!(t.same_day(&local_time(now).unwrap()));
        assert!(!t.same_day(&local_time(now - 3 * 86_400).unwrap()));
    }
}
