//! Paths: a canonical path as the rest of the system writes it.

use std::io;
use std::path::{Path, PathBuf};

/// `path` with symlinks resolved, absolute, as `std::fs::canonicalize` gives it. On Windows that
/// is a verbatim path (`\\?\C:\...`), which other programs (Task Scheduler, cmd, PowerShell)
/// don't all take and which never equals the same path written plainly: the prefix is dropped
/// when the plain path means the same file (a drive or UNC path under 260 characters, with no
/// component Windows would read differently without it).
pub fn canonicalize(path: &Path) -> io::Result<PathBuf> {
    let real = std::fs::canonicalize(path)?;
    #[cfg(windows)]
    {
        Ok(plain(&real).unwrap_or(real))
    }
    #[cfg(not(windows))]
    {
        Ok(real)
    }
}

/// `\\?\C:\a` as `C:\a`, `\\?\UNC\host\share\a` as `\\host\share\a`, when the plain form names
/// the same file; None otherwise.
#[cfg_attr(not(windows), allow(dead_code))]
fn plain(path: &Path) -> Option<PathBuf> {
    let s = path.to_str()?;
    let plain = if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{rest}")
    } else {
        let rest = s.strip_prefix(r"\\?\")?;
        let b = rest.as_bytes();
        if b.len() < 3 || !b[0].is_ascii_alphabetic() || b[1] != b':' || b[2] != b'\\' {
            return None;
        }
        rest.to_string()
    };
    // MAX_PATH counts the terminating NUL.
    if plain.len() >= 260 {
        return None;
    }
    let reserved = |name: &str| {
        let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
        matches!(
            stem.as_str(),
            "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
        ) || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.as_bytes()[3].is_ascii_digit())
    };
    let skip = if plain.starts_with(r"\\") { 2 } else { 1 };
    for part in plain.split('\\').skip(skip).filter(|p| !p.is_empty()) {
        if part.ends_with(['.', ' ']) || reserved(part) || part.contains(['/', ':']) {
            return None;
        }
    }
    Some(PathBuf::from(plain))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verbatim_paths_become_plain_where_they_mean_the_same() {
        let p = |s: &str| plain(Path::new(s)).map(|p| p.to_string_lossy().into_owned());
        assert_eq!(
            p(r"\\?\C:\Users\a b\.local\bin\potluck.exe").as_deref(),
            Some(r"C:\Users\a b\.local\bin\potluck.exe")
        );
        assert_eq!(
            p(r"\\?\UNC\host\share\x").as_deref(),
            Some(r"\\host\share\x")
        );
        assert_eq!(p(r"C:\plain"), None);
        assert_eq!(p(r"\\?\C:\dir\nul.txt"), None);
        assert_eq!(p(r"\\?\C:\dir\COM1"), None);
        assert_eq!(p(r"\\?\C:\trailing.\x"), None);
        assert_eq!(p(r"\\?\Volume{1234}\x"), None);
        assert_eq!(p(&format!(r"\\?\C:\{}", "a".repeat(300))), None);
        let dir = tempfile::tempdir().unwrap();
        let c = canonicalize(dir.path()).unwrap();
        assert!(c.is_absolute());
        assert!(!c.to_string_lossy().starts_with(r"\\?\"), "{}", c.display());
    }
}
