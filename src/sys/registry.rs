//! Reading values from the Windows registry's `HKEY_LOCAL_MACHINE`: the Windows version, the CPU
//! model and the boot's id.

use super::win::{from_wide, wide};
use windows_sys::Win32::Foundation::ERROR_SUCCESS;
use windows_sys::Win32::System::Registry::{
    HKEY_LOCAL_MACHINE, RRF_RT_REG_DWORD, RRF_RT_REG_SZ, RegGetValueW,
};

/// The text value `name` of `HKEY_LOCAL_MACHINE\<key>`, trimmed; `None` when it is missing, not
/// text, or empty.
pub fn hklm_string(key: &str, name: &str) -> Option<String> {
    let (k, n) = (wide(key), wide(name));
    let mut len = 0u32;
    // SAFETY: a size query: no buffer, len receives the bytes needed (with the NUL).
    let rc = unsafe {
        RegGetValueW(
            HKEY_LOCAL_MACHINE,
            k.as_ptr(),
            n.as_ptr(),
            RRF_RT_REG_SZ,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut len,
        )
    };
    if rc != ERROR_SUCCESS || len == 0 {
        return None;
    }
    // A value may grow between the two calls: room for a little more, and a retry would only
    // repeat the race.
    let mut buf = vec![0u16; (len as usize).div_ceil(2) + 64];
    let mut bytes = (buf.len() * 2) as u32;
    // SAFETY: buf has `bytes` bytes of room; RegGetValueW NUL-terminates what it writes.
    let rc = unsafe {
        RegGetValueW(
            HKEY_LOCAL_MACHINE,
            k.as_ptr(),
            n.as_ptr(),
            RRF_RT_REG_SZ,
            std::ptr::null_mut(),
            buf.as_mut_ptr().cast(),
            &mut bytes,
        )
    };
    if rc != ERROR_SUCCESS {
        return None;
    }
    let s = from_wide(&buf).trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// The DWORD value `name` of `HKEY_LOCAL_MACHINE\<key>`; `None` when it is missing or no DWORD.
pub fn hklm_dword(key: &str, name: &str) -> Option<u32> {
    let (k, n) = (wide(key), wide(name));
    let mut value = 0u32;
    let mut len = std::mem::size_of::<u32>() as u32;
    // SAFETY: value has room for the DWORD that RRF_RT_REG_DWORD restricts the read to.
    let rc = unsafe {
        RegGetValueW(
            HKEY_LOCAL_MACHINE,
            k.as_ptr(),
            n.as_ptr(),
            RRF_RT_REG_DWORD,
            std::ptr::null_mut(),
            (&mut value as *mut u32).cast(),
            &mut len,
        )
    };
    (rc == ERROR_SUCCESS).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_windows_version() {
        let key = r"SOFTWARE\Microsoft\Windows NT\CurrentVersion";
        let build = hklm_string(key, "CurrentBuildNumber").unwrap();
        assert!(build.parse::<u32>().unwrap() >= 17763, "{build}");
        assert!(hklm_dword(key, "CurrentMajorVersionNumber").is_some_and(|v| v >= 10));
        assert_eq!(hklm_string(key, "NoSuchValuePotluck"), None);
        assert_eq!(
            hklm_dword(key, "CurrentBuildNumber"),
            None,
            "text, not a DWORD"
        );
    }
}
