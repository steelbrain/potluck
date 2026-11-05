//! `potluck cp WORKSPACE:PATH DEST`: writes the daemon's stream of [`Response::CopyEntry`] frames
//! to disk, with directories and their permission bits (setuid, setgid and sticky bits are
//! dropped, as for kept files). Paths in the stream must be relative and stay inside the copy: an
//! absolute path or a `..` ends the copy with an error.

use crate::client::Client;
use crate::proto::{Request, Response};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

/// The mode bits a copy keeps: read, write and execute for owner, group and others.
const PERMISSIONS: u32 = 0o777;

pub struct CopyStats {
    /// Where the copied path landed.
    pub root: PathBuf,
    pub files: u64,
    pub dirs: u64,
    pub bytes: u64,
}

/// Splits `WORKSPACE:PATH` and checks PATH is relative and has no `..`.
pub fn parse_source(from: &str) -> anyhow::Result<(String, String)> {
    let (ws, path) = from
        .split_once(':')
        .filter(|(w, p)| !w.is_empty() && !p.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "cp: expected WORKSPACE:PATH (for example msi-w7:target/release/app), got {from:?}"
            )
        })?;
    relative(path).map_err(|e| anyhow::anyhow!("cp: {e}"))?;
    Ok((ws.to_string(), path.to_string()))
}

/// `p` as a relative path inside the copy; empty or `.` is the copy's root.
pub fn relative(p: &str) -> anyhow::Result<PathBuf> {
    let mut out = PathBuf::new();
    for c in Path::new(p).components() {
        match c {
            Component::Normal(n) => out.push(n),
            Component::CurDir => {}
            Component::ParentDir => anyhow::bail!("refusing path {p:?}: it contains `..`"),
            Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("refusing absolute path {p:?}: paths are relative to the workspace")
            }
        }
    }
    Ok(out)
}

/// Copies `path` out of `workspace` into `dest`. When `dest` is an existing directory the copy
/// goes inside it under the source's name; otherwise it is created as `dest`.
pub fn copy_out(
    client: &mut Client,
    workspace: &str,
    path: &str,
    dest: &Path,
) -> anyhow::Result<CopyStats> {
    let name = relative(path)?
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_else(|| workspace.into());
    let root = if dest.is_dir() {
        dest.join(name)
    } else {
        dest.to_path_buf()
    };
    let mut stats = CopyStats {
        root: root.clone(),
        files: 0,
        dirs: 0,
        bytes: 0,
    };
    // Directory modes are set last, deepest first, so a read-only directory can still be filled.
    let mut dir_modes: Vec<(PathBuf, u32)> = Vec::new();
    let mut frame = client.call(Request::Copy {
        workspace: workspace.to_string(),
        path: path.to_string(),
    })?;
    loop {
        match frame {
            Response::CopyEntry {
                path: rel,
                mode,
                dir,
                len,
            } => {
                let rel = relative(&rel)?;
                let target = if rel.as_os_str().is_empty() {
                    root.clone()
                } else {
                    root.join(rel)
                };
                if dir {
                    fs::create_dir_all(&target)
                        .map_err(|e| anyhow::anyhow!("{}: {e}", target.display()))?;
                    dir_modes.push((target, mode));
                    stats.dirs += 1;
                    if len > 0 {
                        client.copy_bytes(len, &mut std::io::sink())?;
                    }
                } else {
                    write_file(client, &target, mode, len)?;
                    stats.files += 1;
                    stats.bytes += len;
                }
            }
            Response::CopyDone => break,
            other => anyhow::bail!(
                "unexpected answer from the daemon during a copy: {}",
                serde_json::to_value(&other)
                    .ok()
                    .and_then(|v| v.get("kind").and_then(|k| k.as_str()).map(str::to_string))
                    .unwrap_or_default()
            ),
        }
        frame = client.next_frame()?;
    }
    dir_modes.sort_by_key(|(p, _)| std::cmp::Reverse(p.components().count()));
    for (dir, mode) in dir_modes {
        fs::set_permissions(&dir, fs::Permissions::from_mode(mode & PERMISSIONS))
            .map_err(|e| anyhow::anyhow!("{}: {e}", dir.display()))?;
    }
    Ok(stats)
}

fn write_file(client: &mut Client, target: &Path, mode: u32, len: u64) -> anyhow::Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|e| anyhow::anyhow!("{}: {e}", parent.display()))?;
    }
    // Replace what is there (a read-only file from an earlier copy, or a symlink) rather than
    // writing through it.
    if let Ok(meta) = fs::symlink_metadata(target)
        && !meta.is_dir()
    {
        fs::remove_file(target).map_err(|e| anyhow::anyhow!("{}: {e}", target.display()))?;
    }
    let mut file =
        fs::File::create(target).map_err(|e| anyhow::anyhow!("{}: {e}", target.display()))?;
    client.copy_bytes(len, &mut file).map_err(|e| {
        if crate::client::is_disconnected(&e) {
            e
        } else {
            anyhow::anyhow!("{}: {e:#}", target.display())
        }
    })?;
    file.set_permissions(fs::Permissions::from_mode(mode & PERMISSIONS))
        .map_err(|e| anyhow::anyhow!("{}: {e}", target.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sources_and_relative_paths() {
        assert_eq!(
            parse_source("msi-w7:target/release/app").unwrap(),
            ("msi-w7".to_string(), "target/release/app".to_string())
        );
        assert!(parse_source("msi-w7").is_err());
        assert!(parse_source(":x").is_err());
        assert!(parse_source("msi-w7:").is_err());
        assert!(parse_source("msi-w7:/etc/passwd").is_err());
        assert!(parse_source("msi-w7:a/../../b").is_err());
        assert_eq!(relative("").unwrap(), PathBuf::new());
        assert_eq!(relative("./a/./b").unwrap(), PathBuf::from("a/b"));
        assert!(relative("../x").is_err());
        assert!(relative("/x").is_err());
    }
}
