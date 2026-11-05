//! This machine's key pair: ed25519, one per machine, in `keys/node.key` (PKCS#8, mode 0600).
//!
//! The same key signs admission and removal records (in a cluster without users; see
//! [`super::trust`]) and authenticates TLS, through a self-signed certificate made from it at
//! start. Peers pin the key, never the certificate.

use super::MeshError;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ring::rand::SystemRandom;
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// The key file in the keys directory.
pub const KEY_FILE: &str = "node.key";

/// An ed25519 public key: how members know each other. Written as 43 characters of unpadded
/// base64url.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PublicKey(pub [u8; 32]);

impl PublicKey {
    pub fn to_text(&self) -> String {
        B64.encode(self.0)
    }

    pub fn from_text(s: &str) -> Result<PublicKey, MeshError> {
        let bytes = B64
            .decode(s.trim())
            .map_err(|_| MeshError::Protocol(format!("bad public key {s:?}")))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| MeshError::Protocol(format!("bad public key {s:?}")))?;
        Ok(PublicKey(arr))
    }

    /// Whether `sig` is this key's signature over `msg`.
    pub fn verify(&self, msg: &[u8], sig: &Signature) -> bool {
        UnparsedPublicKey::new(&ED25519, &self.0)
            .verify(msg, &sig.0)
            .is_ok()
    }

    /// The key from an ed25519 certificate's SubjectPublicKeyInfo, if it is one.
    pub fn from_certificate(cert: &CertificateDer<'_>) -> Option<PublicKey> {
        // SEQUENCE { SEQUENCE { OID 1.3.101.112 } BIT STRING (0 unused bits) key }
        const ED25519_SPKI_PREFIX: [u8; 12] = [
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
        ];
        let parsed = rustls::server::ParsedCertificate::try_from(cert).ok()?;
        let spki = parsed.subject_public_key_info();
        let key = spki.as_ref().strip_prefix(&ED25519_SPKI_PREFIX[..])?;
        Some(PublicKey(key.try_into().ok()?))
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_text())
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey({})", self.to_text())
    }
}

impl Serialize for PublicKey {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_text())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        PublicKey::from_text(&s).map_err(serde::de::Error::custom)
    }
}

/// An ed25519 signature, written as unpadded base64url.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Signature(pub [u8; 64]);

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signature({})", B64.encode(self.0))
    }
}

impl Serialize for Signature {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&B64.encode(self.0))
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let bytes = B64
            .decode(s.trim())
            .map_err(|_| serde::de::Error::custom("bad signature"))?;
        let arr: [u8; 64] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("bad signature length"))?;
        Ok(Signature(arr))
    }
}

/// A key that signs records: a machine's [`Identity`], or a user's key
/// ([`super::user::UserKey`]).
pub trait Signer {
    fn public(&self) -> PublicKey;
    fn sign(&self, msg: &[u8]) -> Signature;
}

impl Signer for Identity {
    fn public(&self) -> PublicKey {
        self.public
    }
    fn sign(&self, msg: &[u8]) -> Signature {
        Identity::sign(self, msg)
    }
}

/// An ed25519 signature over `msg` by `pair`.
pub(super) fn sign_with(pair: &Ed25519KeyPair, msg: &[u8]) -> Signature {
    let sig = pair.sign(msg);
    let mut out = [0u8; 64];
    out.copy_from_slice(sig.as_ref());
    Signature(out)
}

/// This machine's key pair and the certificate made from it.
pub struct Identity {
    pkcs8: Vec<u8>,
    pair: Ed25519KeyPair,
    public: PublicKey,
    cert: CertificateDer<'static>,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Identity({})", self.public)
    }
}

impl Identity {
    /// Loads the key pair from `dir`, creating it (and the directory, mode 0700) the first time.
    pub fn load_or_create(dir: &Path) -> Result<Identity, MeshError> {
        let path = dir.join(KEY_FILE);
        match fs::read(&path) {
            Ok(der) => {
                tighten(&path, 0o600)?;
                Identity::from_pkcs8(der)
                    .map_err(|e| MeshError::Identity(format!("{}: {e}", path.display())))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Identity::create(dir),
            Err(e) => Err(MeshError::Identity(format!("{}: {e}", path.display()))),
        }
    }

    /// Loads the key pair from `dir`; an error if there is none.
    pub fn load(dir: &Path) -> Result<Identity, MeshError> {
        let path = dir.join(KEY_FILE);
        let der = fs::read(&path).map_err(|e| {
            MeshError::Identity(format!(
                "{}: {e} (has the daemon run here?)",
                path.display()
            ))
        })?;
        Identity::from_pkcs8(der)
            .map_err(|e| MeshError::Identity(format!("{}: {e}", path.display())))
    }

    /// Makes a new key pair in `dir`, replacing any old one.
    pub fn create(dir: &Path) -> Result<Identity, MeshError> {
        let rng = SystemRandom::new();
        let doc = Ed25519KeyPair::generate_pkcs8(&rng)
            .map_err(|_| MeshError::Identity("could not generate a key pair".into()))?;
        let der = doc.as_ref().to_vec();
        write_private(dir, KEY_FILE, &der)?;
        Identity::from_pkcs8(der).map_err(MeshError::Identity)
    }

    fn from_pkcs8(der: Vec<u8>) -> Result<Identity, String> {
        let pair = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der)
            .map_err(|e| format!("not an ed25519 PKCS#8 key: {e}"))?;
        let public = PublicKey(
            pair.public_key()
                .as_ref()
                .try_into()
                .map_err(|_| "bad public key length".to_string())?,
        );
        let cert = self_signed(&der).map_err(|e| format!("certificate: {e}"))?;
        Ok(Identity {
            pkcs8: der,
            pair,
            public,
            cert,
        })
    }

    pub fn public(&self) -> PublicKey {
        self.public
    }

    pub fn sign(&self, msg: &[u8]) -> Signature {
        sign_with(&self.pair, msg)
    }

    pub fn cert(&self) -> CertificateDer<'static> {
        self.cert.clone()
    }

    pub fn private_key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.pkcs8.clone()))
    }
}

/// A self-signed certificate for the key. Its contents don't matter: peers pin the key.
fn self_signed(pkcs8: &[u8]) -> Result<CertificateDer<'static>, rcgen::Error> {
    let key = rcgen::KeyPair::try_from(pkcs8)?;
    let mut params = rcgen::CertificateParams::new(vec!["potluck".to_string()])?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "potluck");
    let cert = params.self_signed(&key)?;
    Ok(cert.der().clone())
}

/// Creates `dir` with mode 0700 if missing.
pub fn ensure_dir(dir: &Path) -> Result<(), MeshError> {
    if !dir.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|e| MeshError::Identity(format!("{}: {e}", dir.display())))?;
    }
    Ok(())
}

/// Writes `bytes` to `dir/name` with mode 0600, atomically (a new file renamed over the old).
pub fn write_private(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), MeshError> {
    ensure_dir(dir)?;
    let path = dir.join(name);
    let tmp: PathBuf = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    let err = |e: std::io::Error| MeshError::Identity(format!("{}: {e}", path.display()));
    let _ = fs::remove_file(&tmp);
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(err)?;
    let written = f
        .write_all(bytes)
        .and_then(|()| f.sync_all())
        .and_then(|()| {
            drop(f);
            fs::rename(&tmp, &path)
        });
    if written.is_err() {
        // Named for this process: a later one would never remove it.
        let _ = fs::remove_file(&tmp);
    }
    written.map_err(err)
}

/// Clears group and other permission bits on `path`, if any are set.
pub(super) fn tighten(path: &Path, mode: u32) -> Result<(), MeshError> {
    let meta = fs::metadata(path).map_err(|e| MeshError::Identity(format!("{e}")))?;
    if meta.permissions().mode() & 0o077 != 0 {
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .map_err(|e| MeshError::Identity(format!("{}: {e}", path.display())))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_created_once_with_mode_0600_and_reloaded() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("keys");
        let a = Identity::load_or_create(&dir).unwrap();
        let mode = fs::metadata(dir.join(KEY_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        assert_eq!(
            fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let b = Identity::load_or_create(&dir).unwrap();
        assert_eq!(a.public(), b.public());
        assert_eq!(PublicKey::from_certificate(&b.cert()), Some(b.public()));
        let sig = a.sign(b"hello");
        assert!(b.public().verify(b"hello", &sig));
        assert!(!b.public().verify(b"hellO", &sig));
        let text = a.public().to_text();
        assert_eq!(text.len(), 43);
        assert_eq!(PublicKey::from_text(&text).unwrap(), a.public());
    }

    /// A write that fails (here the rename, over a directory) leaves no temp file behind: it is
    /// named for the process, so a later daemon would never remove it.
    #[test]
    fn a_failed_write_leaves_nothing_behind() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        fs::create_dir(dir.join("members.json")).unwrap();
        fs::write(dir.join("members.json").join("x"), "").unwrap();
        assert!(write_private(dir, "members.json", b"{}").is_err());
        let names: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["members.json"]);
    }
}
