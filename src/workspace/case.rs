//! Paths that differ only in case (`README` and `readme`, or `Docs/` and `docs/`; `Straße` and
//! `STRASSE`), or in Unicode normalisation (`café` with a composed `é` and with `e` and a combining
//! accent). A commit may hold both, but a case-insensitive filesystem (APFS by default, which also
//! takes canonically equivalent names for one) holds one file for them, so a tree seeded there
//! can't come out clean.
//! That is the repository's doing, not a fault of the base checkout or the mirror: seeding fails
//! saying so, and neither is discarded or checked.
//!
//! So is a name the filesystem refuses ([`refused`]): APFS takes only UTF-8 of the Unicode version
//! it knows (an unassigned code point such as U+A7DD, or bytes that aren't UTF-8, are "Illegal
//! byte sequence"), and the filesystems potluck runs on take no name over 255 bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Whether `dir` looks names up without regard to case: probes with a file of its own.
pub(super) fn case_insensitive(dir: &Path) -> bool {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let name = format!(".potluck-case-probe-{}-{nanos}", std::process::id());
    let lower = dir.join(&name);
    if std::fs::write(&lower, b"").is_err() {
        return false;
    }
    let found = dir.join(name.to_uppercase()).symlink_metadata().is_ok();
    let _ = std::fs::remove_file(&lower);
    found
}

/// The name a case- and normalisation-insensitive filesystem looks `s` up by: Unicode's canonical
/// caseless form, NFD(case fold(NFD(s))). APFS folds case in full, not just to lower case: `ß` is
/// `ss` there, a final `ς` is `σ`, a micro sign `μ` and `ﬁ` `fi`.
fn fold(s: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    let nfd: String = s.nfd().collect();
    unicase::UniCase::new(nfd).to_folded_case().nfd().collect()
}

/// `a`, `a/b` and `a/b/c` for `a/b/c`: each directory of a path, then the path itself.
fn prefixes(p: &str) -> impl Iterator<Item = &str> {
    p.match_indices('/')
        .map(|(i, _)| i)
        .filter(|i| *i > 0)
        .chain([p.len()])
        .map(|end| &p[..end])
}

/// The commit's paths and their directories that differ only in case or normalisation, as groups
/// of the spellings
/// the commit uses, keyed by their folded form.
pub(super) fn collisions(paths: &[String]) -> BTreeMap<String, Vec<String>> {
    let mut by_fold: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for p in paths {
        for prefix in prefixes(p) {
            let spellings = by_fold.entry(fold(prefix)).or_default();
            if !spellings.iter().any(|s| s == prefix) {
                spellings.push(prefix.to_string());
            }
        }
    }
    by_fold.retain(|_, spellings| spellings.len() > 1);
    for spellings in by_fold.values_mut() {
        spellings.sort();
    }
    by_fold
}

/// When every one of the `dirty` paths (or a directory of it) is one of the commit's case
/// collisions, those collisions; `None` when something else is dirty too.
pub(super) fn explain(dirty: &[String], commit_paths: &[String]) -> Option<Vec<Vec<String>>> {
    let groups = collisions(commit_paths);
    if dirty.is_empty() || groups.is_empty() {
        return None;
    }
    let mut used: BTreeMap<&str, &Vec<String>> = BTreeMap::new();
    for d in dirty {
        let (key, spellings) = prefixes(d.trim_end_matches('/'))
            .find_map(|prefix| groups.get_key_value(&fold(prefix)))?;
        used.insert(key, spellings);
    }
    Some(used.into_values().cloned().collect())
}

/// "README, readme; Docs, docs", at most a few groups.
pub(super) fn describe(groups: &[Vec<String>]) -> String {
    let mut parts: Vec<String> = groups.iter().take(5).map(|g| g.join(", ")).collect();
    if groups.len() > 5 {
        parts.push(format!("and {} more", groups.len() - 5));
    }
    parts.join("; ")
}

/// Of `paths` (as git lists them: bytes, `/` between names), those holding a name the
/// filesystem at `dir` refuses, each with why ("Illegal byte sequence", "File name too long").
/// Only names that aren't plain ASCII, or are over 255 bytes, are tried: each is created in a
/// scratch directory under `dir`, removed again at once.
pub(super) fn refused(dir: &Path, paths: &[Vec<u8>]) -> Vec<(String, String)> {
    use std::os::unix::ffi::OsStrExt;
    let names: BTreeSet<&[u8]> = paths
        .iter()
        .flat_map(|p| p.split(|b| *b == b'/'))
        .filter(|n| !n.is_ascii() || n.len() > 255)
        .collect();
    if names.is_empty() {
        return Vec::new();
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let scratch = dir.join(format!(
        ".potluck-name-probe-{}-{nanos}",
        std::process::id()
    ));
    if std::fs::create_dir(&scratch).is_err() {
        return Vec::new();
    }
    let mut why: BTreeMap<&[u8], String> = BTreeMap::new();
    for name in names {
        let probe = scratch.join(std::ffi::OsStr::from_bytes(name));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)
        {
            Ok(_) => {
                let _ = std::fs::remove_file(&probe);
            }
            Err(e) if matches!(e.raw_os_error(), Some(libc::EILSEQ | libc::ENAMETOOLONG)) => {
                let text = e.to_string();
                let text = text.split(" (os error").next().unwrap_or(&text);
                why.insert(name, text.to_string());
            }
            Err(_) => {}
        }
    }
    let _ = std::fs::remove_dir_all(&scratch);
    paths
        .iter()
        .filter_map(|p| {
            let why = p.split(|b| *b == b'/').find_map(|n| why.get(n))?;
            Some((String::from_utf8_lossy(p).into_owned(), why.clone()))
        })
        .collect()
}

/// "d/x (Illegal byte sequence)", at most a few paths.
pub(super) fn describe_refused(refused: &[(String, String)]) -> String {
    let mut parts: Vec<String> = refused
        .iter()
        .take(5)
        .map(|(path, why)| format!("{path} ({why})"))
        .collect();
    if refused.len() > 5 {
        parts.push(format!("and {} more", refused.len() - 5));
    }
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn finds_collisions_of_files_and_directories() {
        let c = collisions(&s(&[
            "README", "readme", "src/a.rs", "Docs/x", "docs/y", "ok/Z",
        ]));
        assert_eq!(
            c.into_values().collect::<Vec<_>>(),
            vec![s(&["Docs", "docs"]), s(&["README", "readme"])]
        );
        assert!(collisions(&s(&["a/b", "a/c", "A1"])).is_empty());
        assert_eq!(
            collisions(&s(&["caf\u{e9}", "Cafe\u{301}/x", "cafe"]))
                .into_values()
                .collect::<Vec<_>>(),
            vec![s(&["Cafe\u{301}", "caf\u{e9}"])]
        );
        // Names APFS takes for one (each pair checked there): case folded in full.
        for (a, b) in [
            ("Stra\u{df}e", "STRASSE"),
            ("\u{1e9e}", "\u{df}"),
            ("\u{3c3}", "\u{3c2}"),
            ("\u{b5}", "\u{3bc}"),
            ("\u{17f}", "s"),
            ("\u{fb01}", "FI"),
            ("\u{345}", "\u{3b9}"),
            ("\u{212a}", "k"),
            ("\u{2126}", "\u{3c9}"),
            ("\u{130}", "i\u{307}"),
            ("\u{1f88}", "\u{1f00}\u{3b9}"),
            ("\u{ac00}", "\u{1100}\u{1161}"),
        ] {
            assert_eq!(collisions(&s(&[a, b])).len(), 1, "{a:?} {b:?}");
        }
        // And names it keeps apart.
        for (a, b) in [
            ("\u{130}", "i"),
            ("\u{131}", "I"),
            ("a\u{200b}b", "ab"),
            ("\u{2460}", "1"),
            ("\u{2168}", "ix"),
        ] {
            assert!(collisions(&s(&[a, b])).is_empty(), "{a:?} {b:?}");
        }
    }

    /// A name over 255 bytes is refused everywhere; U+A7DD where the filesystem doesn't know it
    /// (APFS); plain names never. Nothing is left behind.
    #[test]
    fn finds_names_the_filesystem_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let long = format!("d/{}", "n".repeat(256));
        let paths: Vec<Vec<u8>> = ["a/b", long.as_str(), "x\u{a7dd}/y", "caf\u{e9}"]
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect();
        let apfs = std::fs::write(dir.path().join("\u{a7dd}"), "").is_err();
        let _ = std::fs::remove_file(dir.path().join("\u{a7dd}"));
        let found: Vec<String> = refused(dir.path(), &paths)
            .into_iter()
            .map(|(p, why)| format!("{p} ({why})"))
            .collect();
        let mut want = vec![format!("{long} (File name too long)")];
        if apfs {
            want.push("x\u{a7dd}/y (Illegal byte sequence)".into());
        }
        assert_eq!(found, want);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn explains_only_when_every_dirty_path_collides() {
        let paths = s(&["README", "readme", "Docs/x", "docs/y", "src/main.rs"]);
        assert_eq!(
            explain(&s(&["README"]), &paths),
            Some(vec![s(&["README", "readme"])])
        );
        assert_eq!(
            explain(&s(&["readme", "Docs/y", "docs/"]), &paths),
            Some(vec![s(&["Docs", "docs"]), s(&["README", "readme"])])
        );
        assert_eq!(explain(&s(&["README", "src/main.rs"]), &paths), None);
        assert_eq!(explain(&s(&["src/main.rs"]), &s(&["src/main.rs"])), None);
        assert_eq!(explain(&[], &paths), None);
        assert_eq!(
            describe(&[s(&["README", "readme"]), s(&["Docs", "docs"])]),
            "README, readme; Docs, docs"
        );
    }

    #[test]
    fn probes_this_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let insensitive = case_insensitive(dir.path());
        std::fs::write(dir.path().join("x"), "").unwrap();
        assert_eq!(insensitive, dir.path().join("X").exists());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
