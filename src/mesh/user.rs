//! A user's key pair: ed25519 in PKCS#8, as a machine's ([`super::identity`]), held by the command
//! line, never by the daemon. It signs what carries authority in a cluster with users: who joins
//! and leaves, and who else is a user (see [`super::trust`]).
//!
//! The file is `$POTLUCK_USER_KEY`, else `user.key` beside the command line's node.toml, mode
//! 0600.

use super::MeshError;
use super::identity::{PublicKey, Signature, Signer, sign_with, tighten, write_private};
use crate::config::Paths;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use std::fs;
use std::path::{Path, PathBuf};

/// The key file beside node.toml when `$POTLUCK_USER_KEY` isn't set.
pub const USER_KEY_FILE: &str = "user.key";

/// Where this command line's user key is: `$POTLUCK_USER_KEY`, else `user.key` in the directory of
/// `paths.config`.
pub fn key_path(paths: &Paths) -> PathBuf {
    match std::env::var_os("POTLUCK_USER_KEY").filter(|v| !v.is_empty()) {
        Some(p) => std::path::absolute(PathBuf::from(&p)).unwrap_or_else(|_| PathBuf::from(p)),
        None => paths
            .config
            .parent()
            .unwrap_or(Path::new("."))
            .join(USER_KEY_FILE),
    }
}

/// A user's key pair.
pub struct UserKey {
    pair: Ed25519KeyPair,
    public: PublicKey,
    path: PathBuf,
}

impl std::fmt::Debug for UserKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserKey({})", self.public)
    }
}

impl UserKey {
    /// The key at `path`; None when there is no file there.
    pub fn load(path: &Path) -> Result<Option<UserKey>, MeshError> {
        match fs::read(path) {
            Ok(der) => {
                tighten(path, 0o600)?;
                UserKey::from_pkcs8(&der, path).map(Some)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(MeshError::Identity(format!("{}: {e}", path.display()))),
        }
    }

    /// The key at `path`, made there first (mode 0600, its directory 0700 if missing) when
    /// there is none. Returns whether it was made now.
    pub fn load_or_create(path: &Path) -> Result<(UserKey, bool), MeshError> {
        if let Some(k) = UserKey::load(path)? {
            return Ok((k, false));
        }
        let doc = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .map_err(|_| MeshError::Identity("could not generate a key pair".into()))?;
        let (dir, name) = match (path.parent(), path.file_name()) {
            (Some(d), Some(n)) => (d, n.to_string_lossy()),
            _ => {
                return Err(MeshError::Identity(format!(
                    "{}: not a file path",
                    path.display()
                )));
            }
        };
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        write_private(dir, &name, doc.as_ref())?;
        Ok((UserKey::from_pkcs8(doc.as_ref(), path)?, true))
    }

    fn from_pkcs8(der: &[u8], path: &Path) -> Result<UserKey, MeshError> {
        let bad = |e: String| MeshError::Identity(format!("{}: {e}", path.display()));
        let pair = Ed25519KeyPair::from_pkcs8_maybe_unchecked(der)
            .map_err(|e| bad(format!("not an ed25519 PKCS#8 key: {e}")))?;
        let public = PublicKey(
            pair.public_key()
                .as_ref()
                .try_into()
                .map_err(|_| bad("bad public key length".into()))?,
        );
        Ok(UserKey {
            pair,
            public,
            path: path.to_path_buf(),
        })
    }

    pub fn public(&self) -> PublicKey {
        self.public
    }

    pub fn sign(&self, msg: &[u8]) -> Signature {
        sign_with(&self.pair, msg)
    }

    /// The file it was read from.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Signer for UserKey {
    fn public(&self) -> PublicKey {
        self.public
    }
    fn sign(&self, msg: &[u8]) -> Signature {
        UserKey::sign(self, msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn a_user_key_is_made_once_with_mode_0600_and_signs() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("conf").join(USER_KEY_FILE);
        assert!(UserKey::load(&path).unwrap().is_none());
        let (a, made) = UserKey::load_or_create(&path).unwrap();
        assert!(made);
        let mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let (b, made) = UserKey::load_or_create(&path).unwrap();
        assert!(!made);
        assert_eq!(a.public(), b.public());
        let sig = a.sign(b"hello");
        assert!(b.public().verify(b"hello", &sig));
        assert!(!b.public().verify(b"hellO", &sig));
        // A key file others can read is tightened when loaded.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        UserKey::load(&path).unwrap().unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        // A machine's key file is the same format.
        let id = super::super::identity::Identity::load_or_create(tmp.path()).unwrap();
        let der = fs::read(tmp.path().join(super::super::identity::KEY_FILE)).unwrap();
        let other = tmp.path().join("other.key");
        fs::write(&other, der).unwrap();
        assert_eq!(
            UserKey::load(&other).unwrap().unwrap().public(),
            id.public()
        );
    }

    #[test]
    fn the_key_lives_beside_node_toml_unless_the_variable_says() {
        let paths = Paths {
            home: PathBuf::from("/s/home"),
            config: PathBuf::from("/c/potluck/node.toml"),
        };
        if std::env::var_os("POTLUCK_USER_KEY").is_none() {
            assert_eq!(key_path(&paths), PathBuf::from("/c/potluck/user.key"));
        }
    }
}
