//! The cluster (phase 2): members on one TLS port each, pinned keys, a chain of trust, heartbeats.
//!
//! - **Keys.** Each machine has an ed25519 key pair in `keys/node.key` (mode 0600); its TLS
//!   certificate is self-signed with it at start. Connections are TLS 1.3, mutually authenticated
//!   by pinned public keys, never by a CA: the dialing side accepts only the key of the member it
//!   dials, the listening side only members' keys (and removed members' keys, to tell them).
//! - **Pairing.** [`Mesh::pair_request`] on a new machine makes a request (its name, key, address
//!   and a nonce, as one line of base64url); [`Mesh::admit`] on any member signs an admission and
//!   returns a reply (the admission among all the cluster's records, and the members' addresses);
//!   [`Mesh::pair_finish`] on the new machine stores it. Then they connect directly. In a cluster
//!   with users ([`trust`]), a user's key signs the admission instead ([`Mesh::admit_signed`]),
//!   and removals ([`Mesh::remove_signed`]); [`Mesh::user_init`] makes the first user.
//! - **Chain of trust.** The admitting member announces the admission to every member; every
//!   member accepts a key a member signed. Members compare a digest of their records on every
//!   heartbeat and exchange them when they differ, so a member that was offline catches up when it
//!   is back. Every member passes on every record it holds, those that count nowhere for it
//!   included, so members that reach each other only through a third agree too. A member keeps
//!   its own records that are void, and until it restarts those it took before they turned
//!   void, which the others turn away, so two members' digests may stay different: they
//!   exchange again only once either side's records change. [`Mesh::remove`]
//!   is announced the same way and the key is refused everywhere after.
//!   A member back from being away (its mesh started, or it heard from no member for the down
//!   time) compares records with every other member, waiting up to the down time for those it
//!   doesn't hear from, before [`Mesh::caught_up_for`] lets the daemon deal with one: a member
//!   removed meanwhile can't be told from an honest one by its own records.
//!   Which records count (a removed member's, removals that conflict, names taken twice) is a
//!   function of the records alone, with no wall-clock time in it: see [`trust`].
//! - **Versions.** The hello after the handshake carries each side's version; members talk only to
//!   members on the same version ([`same_version`]: build metadata doesn't count). A mismatch is refused with [`MeshError::VersionMismatch`] on the
//!   dialing side and [`Event::VersionMismatch`] on the other, both carrying both versions, so the
//!   daemon can have the older side update itself. A member answering on another version counts
//!   as alive for [`MeshConfig::updating_for`] (10 minutes) from its first such answer, so a
//!   cluster-wide update doesn't make members down; after that it is down as if silent.
//! - **Heartbeats** every few seconds; a member silent for 30 seconds is down ([`Event::Down`]).
//! - **One port** carries control messages ([`Mesh::call`], JSON values both ways, one at a time
//!   per connection, connections pooled) and streams ([`Mesh::open_stream`], each on a connection
//!   of its own: a JSON header, then raw bytes both ways until either side closes).
//!
//! - **Unauthenticated connections** (still in their handshake or hello) are capped: 64 in all,
//!   8 from one address; more are closed at once. [`Event::Refused`] is rate-limited.
//!
//! Threads only, no async runtime: one per connection, one for the listener, one for heartbeats.
//!
//! Wire format, after the TLS handshake: the dialing side sends one JSON line
//! `{"hello":{..},"open":{"kind":"control"}}` (or `{"kind":"stream","header":..}`); the
//! listening side answers `{"ok":{..its hello..}}` or `{"refused":{..}}`; or first `"records"`,
//! when it holds the dialing side removed and their records differ, which then sends
//! `{"t":"sync","records":[..]}` (the records that count for it) before the answer. A control
//! connection then carries request and reply lines; a stream carries the caller's bytes.

pub mod identity;
mod node;
mod tls;
pub mod trust;
pub mod user;

#[cfg(test)]
pub(crate) mod tests;

pub use identity::{PublicKey, Signature};
pub(crate) use node::release;
pub use node::{Mesh, Stream};
pub use trust::{Admission, Record, RecordId, Removal, UserAdmission, UserRemoval};

use crate::config::{NodeConfig, Paths};
use crate::types::{Millis, NodeName};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// How a mesh runs. [`MeshConfig::new`] fills it from the node config; tests shorten the times.
#[derive(Clone, Debug)]
pub struct MeshConfig {
    /// This machine's name when it founds a cluster or asks to join one. Once a member, the name
    /// it was admitted under is used.
    pub name: NodeName,
    /// The key pair and the trust records (`Paths::keys_dir`).
    pub keys_dir: PathBuf,
    /// Where to listen. Port 0 picks a free port.
    pub listen: SocketAddr,
    /// How other members reach this machine: `host:port`, or a bare host to use the listening
    /// port. Default: the hostname and the listening port.
    pub address: Option<String>,
    /// This build's version; members talk only to members on the same version.
    pub version: String,
    pub heartbeat: Duration,
    /// A member silent this long is down.
    pub down_after: Duration,
    /// A member answering on another version counts as heard from for this long (one of the two
    /// is updating), then no more: it is down once silent otherwise for the down time.
    pub updating_for: Duration,
    pub connect_timeout: Duration,
    /// How long [`Mesh::call`] waits for a reply.
    pub call_timeout: Duration,
}

impl MeshConfig {
    pub fn new(paths: &Paths, config: &NodeConfig) -> MeshConfig {
        MeshConfig {
            name: config.name.clone().unwrap_or_else(short_hostname),
            keys_dir: paths.keys_dir(),
            // `listen` from the node config, else every IPv4 interface. A bad `listen` is reported
            // when the daemon starts (`NodeConfig::listen_ip`); here it falls back to the default.
            listen: SocketAddr::new(
                config
                    .listen_ip()
                    .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
                config.port(),
            ),
            address: config.address.clone(),
            version: crate::VERSION.to_string(),
            heartbeat: Duration::from_secs(3),
            down_after: Duration::from_secs(30),
            updating_for: Duration::from_secs(600),
            connect_timeout: Duration::from_secs(5),
            call_timeout: Duration::from_secs(60),
        }
    }
}

/// What the listening side runs for each message and stream from a member. Each connection has
/// a thread of its own, so calls from several members (or several calls from one) run at once.
pub trait Handler: Send + Sync + 'static {
    /// A control message; the returned value (or error text) goes back to the caller.
    fn call(&self, from: &Peer, msg: Value) -> Result<Value, String>;

    /// A stream a member opened, with the header it sent. The connection is the handler's until
    /// it drops the stream. By default the stream is closed at once.
    fn stream(&self, from: &Peer, header: Value, stream: Stream) {
        let _ = (from, header);
        drop(stream);
    }
}

/// The member on the other end of a call or stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub name: NodeName,
    pub key: PublicKey,
    /// The address it gave in its hello.
    pub address: String,
    pub version: String,
    /// Its pool, from the admission that made it a member ([`trust::DEFAULT_POOL`] in a cluster
    /// without users).
    #[serde(default = "default_pool")]
    pub pool: String,
}

fn default_pool() -> String {
    trust::DEFAULT_POOL.to_string()
}

/// A member as this machine sees it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub name: NodeName,
    pub key: PublicKey,
    /// Where to reach it: the latest address it gave, or the one it joined with.
    pub address: String,
    /// The member that admitted it (the founder admitted itself), or in a cluster with users the
    /// user.
    pub admitted_by: NodeName,
    pub admitted_at: Millis,
    /// Its pool, from the admission that made it a member ([`trust::DEFAULT_POOL`] in a cluster
    /// without users).
    #[serde(default = "default_pool")]
    pub pool: String,
    pub this_machine: bool,
    /// Heard from within the down time (always true for this machine).
    pub alive: bool,
    pub last_seen: Option<Millis>,
    /// Its version, as of its last hello.
    pub version: Option<String>,
}

/// What the command line signs membership records from ([`Mesh::membership`]): this machine, the
/// clock for a new record, the members and the users.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipView {
    /// This machine's name and machine key.
    pub node: NodeName,
    pub key: PublicKey,
    /// The cluster's id (its founder's machine key).
    pub cluster: PublicKey,
    /// This machine is in its cluster: not removed, and its admission counts.
    pub member: bool,
    /// The logical clock for a record signed now ([`trust::Trust::next_clock`]).
    pub next_clock: u64,
    /// Users sign what counts ([`trust::Trust::users_exist`]).
    pub users_exist: bool,
    pub members: Vec<MemberEntry>,
    /// The users that count (none in a cluster without users).
    pub users: Vec<UserEntry>,
    /// Removed machines, by name.
    pub removed: Vec<MemberEntry>,
    /// Without users: the members admitted before this machine, oldest first. Each can co-sign
    /// a first user's admission that counts instead of one this machine co-signs.
    pub elders: Vec<NodeName>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberEntry {
    pub name: NodeName,
    pub key: PublicKey,
    pub address: String,
    pub pool: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserEntry {
    pub name: String,
    pub key: PublicKey,
    /// The user that admitted it (the first user itself).
    pub by: String,
    pub at: Millis,
    /// The ids of the records it signed that count: what a removal of it keeps.
    pub signed: Vec<RecordId>,
}

/// A member as a mesh last heard it: what [`Mesh::hand_over`] passes to [`Mesh::start_on`]
/// across a restart in place, so the next mesh neither starts the member's down time afresh nor
/// takes it for down. Times are ages at the handover (the clocks they were measured on go on).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    pub key: PublicKey,
    pub name: NodeName,
    pub known_ago_ms: u64,
    pub up: Option<bool>,
    pub seen_ago_ms: Option<u64>,
    pub last_seen_ms: Option<Millis>,
    pub version: Option<String>,
    pub mismatch: Option<String>,
    pub mismatch_ago_ms: Option<u64>,
}

/// Something the daemon may act on. See [`Mesh::subscribe`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// A member was heard from, after being down or not heard from since start.
    Up { node: NodeName },
    /// A member has been silent for the down time (or never answered since start). `why`: how
    /// the last heartbeat to it failed (`can't reach b: 192.168.1.9:7465: No route to host (os
    /// error 65)`), if one did.
    Down { node: NodeName, why: Option<String> },
    /// A machine was admitted to the cluster.
    Joined { node: NodeName, by: NodeName },
    /// A machine left the cluster: removed by `by` (a member, or in a cluster with users the
    /// user), or (`by` None) dropped: the member that admitted it was removed by a member that
    /// had not heard of the admission, or another machine was admitted under its name first.
    /// `here`: the removal was made on this machine ([`Mesh::remove`]).
    Removed {
        node: NodeName,
        by: Option<NodeName>,
        #[serde(default)]
        here: bool,
    },
    /// This machine was removed from the cluster. It must pair again (with a new key) to rejoin,
    /// unless the removal turns out void (see [`Mesh::is_removed`]).
    SelfRemoved { by: NodeName },
    /// This machine dropped out of the cluster without being removed: its admission no longer
    /// counts (see [`Event::Removed`] for why that happens). It must pair again to rejoin, unless
    /// that turns out void (see [`Mesh::is_removed`]).
    Dropped { reason: String },
    /// A member runs another version. Members talk only on the same version.
    VersionMismatch {
        node: NodeName,
        ours: String,
        theirs: String,
    },
    /// Heartbeats to a member that is up fail to reach it while it reaches this machine (three in
    /// a row, with a connection or request from it between each and the next): this machine
    /// can't reach it. Said at most every 10 minutes of each member while that lasts.
    Unreachable { node: NodeName, why: String },
    /// An incoming connection was refused: an unknown key, a failed handshake, a bad hello, or
    /// too many connections not authenticated yet. At most 10 per 10 seconds; the next one
    /// after a pause says how many were not reported.
    Refused { address: String, reason: String },
}

#[derive(Debug)]
pub enum MeshError {
    /// No member by that name.
    NotMember(String),
    /// This machine was removed from the cluster; it must pair again to rejoin.
    Removed { by: NodeName },
    /// This machine dropped out of the cluster without being removed (see [`Event::Dropped`]);
    /// it must pair again to rejoin.
    Dropped { reason: String },
    /// The other side runs another version. The older side should update itself.
    VersionMismatch {
        node: NodeName,
        ours: String,
        theirs: String,
    },
    /// The TLS handshake failed: the other side's key was not the pinned one, or it refused ours.
    Handshake { node: NodeName, message: String },
    /// The member refused this machine after the handshake.
    Refused { node: NodeName, reason: String },
    /// The member could not be reached, or the connection broke.
    Unreachable { node: NodeName, message: String },
    /// No reply in time.
    Timeout { node: NodeName },
    /// The member's handler answered with an error.
    Remote { node: NodeName, message: String },
    /// A pairing request or reply that can't be used, or an admission that can't be made.
    Pairing(String),
    /// The key pair or the trust records could not be read or written.
    Identity(String),
    /// A request that makes no sense here (removing this machine, for example).
    Invalid(String),
    /// A malformed message.
    Protocol(String),
    /// The mesh was stopped.
    Stopped,
}

impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeshError::NotMember(n) => write!(f, "{n} is not a member of this cluster"),
            MeshError::Removed { by } => write!(
                f,
                "this machine was removed from the cluster (by {by}); run `potluck pair` to join again"
            ),
            MeshError::Dropped { reason } => write!(
                f,
                "this machine is no longer in the cluster: {reason}; run `potluck pair` to join again"
            ),
            MeshError::VersionMismatch { node, ours, theirs } => write!(
                f,
                "{node} runs potluck {theirs} and this machine {ours}; members talk only on the same version"
            ),
            MeshError::Handshake { node, message } => {
                write!(f, "TLS handshake with {node} failed: {message}")
            }
            MeshError::Refused { node, reason } => write!(f, "{node} refused: {reason}"),
            MeshError::Unreachable { node, message } => {
                write!(f, "can't reach {node}: {message}")
            }
            MeshError::Timeout { node } => write!(f, "{node} did not answer in time"),
            MeshError::Remote { node, message } => write!(f, "{node}: {message}"),
            MeshError::Pairing(m) | MeshError::Invalid(m) => f.write_str(m),
            MeshError::Identity(m) => write!(f, "keys: {m}"),
            MeshError::Protocol(m) => write!(f, "protocol error: {m}"),
            MeshError::Stopped => f.write_str("the mesh is stopped"),
        }
    }
}

impl std::error::Error for MeshError {}

/// Whether two members' versions are the same: the same string, or the same version by
/// [`compare_versions`] (equal but for build metadata, `+...`, which semver says doesn't make
/// another version). Such builds can't update to each other (neither is newer, and a release is
/// fetched by its version), so they talk.
pub fn same_version(a: &str, b: &str) -> bool {
    a == b || crate::update::compare_versions(a, b) == Some(Ordering::Equal)
}

/// Which of two versions is newer, for deciding which side of a [`MeshError::VersionMismatch`]
/// updates: [`crate::update::compare_versions`], the order the updater itself goes by (parts
/// compared as numbers, so `0.1.10` after `0.1.9`; a missing part counts as 0; a prerelease before
/// its release; build metadata ignored). `Equal` when either is not a version: neither then
/// updates to the other.
pub fn compare_versions(a: &str, b: &str) -> Ordering {
    crate::update::compare_versions(a, b).unwrap_or(Ordering::Equal)
}

/// This machine's host name, as the kernel reports it.
pub fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes at most buf.len() bytes into buf.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc != 0 {
        return "localhost".to_string();
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

/// The host name up to its first dot: the default node name.
pub fn short_hostname() -> String {
    let h = hostname();
    h.split('.').next().unwrap_or(&h).to_string()
}

/// The name this machine's records give it, which it keeps whatever its node config says later,
/// and whether those records name any other member. None when no daemon has run here yet, when
/// the key file was replaced (the daemon starts over under the configured name), or once this
/// machine was removed. Reads `keys_dir` only; nothing needs to be running.
pub fn recorded_name(keys_dir: &std::path::Path) -> Option<(NodeName, bool)> {
    let me = identity::Identity::load(keys_dir).ok()?.public();
    let trust = trust::Trust::load_read_only(keys_dir).ok()?;
    let name = trust.view.members.get(&me)?.node.clone();
    Some((name, trust.view.members.len() > 1))
}

/// The address to advertise: `configured` if it has a port, a configured bare host with `port`,
/// or the hostname with `port`.
pub fn advertised_address(configured: Option<&str>, port: u16) -> String {
    let host = match configured.map(str::trim) {
        Some(a) if a.starts_with('[') && a.contains("]:") => return a.to_string(),
        Some(a) if a.matches(':').count() == 1 => return a.to_string(),
        Some(a) if a.matches(':').count() > 1 && !a.starts_with('[') => format!("[{a}]"),
        Some(a) if !a.is_empty() => a.to_string(),
        _ => hostname(),
    };
    format!("{host}:{port}")
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn versions_compare_numerically() {
        assert_eq!(compare_versions("0.1.10", "0.1.9"), Ordering::Greater);
        assert_eq!(compare_versions("0.2.0", "0.10.0"), Ordering::Less);
        assert_eq!(compare_versions("1.0.0", "1.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("1.0.0.1", "1.0.0"), Ordering::Greater);
    }

    #[test]
    fn a_prerelease_is_older_than_its_release() {
        // The release workflow publishes tags like v0.2.0-rc.1 as prereleases.
        assert_eq!(compare_versions("0.2.0-rc.1", "0.2.0"), Ordering::Less);
        assert_eq!(compare_versions("0.2.0", "0.2.0-rc.1"), Ordering::Greater);
        assert_eq!(
            compare_versions("0.2.0-rc.2", "0.2.0-rc.10"),
            Ordering::Less
        );
        assert_eq!(compare_versions("0.2.0-rc.1", "0.1.9"), Ordering::Greater);
        assert_eq!(
            compare_versions("0.2.0-alpha", "0.2.0-alpha.1"),
            Ordering::Less
        );
        assert_eq!(compare_versions("0.2.0-1", "0.2.0-alpha"), Ordering::Less);
        // Build metadata doesn't count.
        assert_eq!(compare_versions("0.2.0+abc", "0.2.0"), Ordering::Equal);
    }

    /// The mesh decides which side of a version mismatch updates, and the updater whether a
    /// version is newer than the one here: they must agree, or a member would update to a
    /// version the updater calls no newer (again and again). Versions that are the same by the
    /// comparison are the same for the mesh too, so such members talk.
    #[test]
    fn the_mesh_and_the_updater_order_versions_alike() {
        let pairs = [
            ("0.1.10", "0.1.9"),
            ("0.2.0", "0.10.0"),
            ("1.0.0", "1.0.0"),
            ("1.0.0.1", "1.0.0"),
            ("1.0.0", "1.0.0.0"),
            ("1.0", "1.0.0"),
            ("0.2.0-rc.1", "0.2.0"),
            ("0.2.0-rc.2", "0.2.0-rc.10"),
            ("0.2.0-rc.1", "0.1.9"),
            ("0.2.0-alpha", "0.2.0-alpha.1"),
            ("0.2.0-1", "0.2.0-alpha"),
            ("0.2.0+abc", "0.2.0"),
            ("0.2.0+abc", "0.2.0+def"),
        ];
        for (a, b) in pairs {
            let updater = crate::update::compare_versions(a, b);
            assert_eq!(Some(compare_versions(a, b)), updater, "{a} vs {b}");
            assert_eq!(Some(compare_versions(b, a)), updater.map(Ordering::reverse));
            assert_eq!(
                same_version(a, b),
                updater == Some(Ordering::Equal),
                "{a} vs {b}"
            );
        }
        // Not versions: neither is newer, so nobody updates to the other; they don't talk.
        assert_eq!(compare_versions("latest", "1.0.0"), Ordering::Equal);
        assert!(!same_version("latest", "1.0.0"));
        assert!(same_version("dev", "dev"));
    }

    #[test]
    fn addresses() {
        assert_eq!(advertised_address(Some("a.lan:9"), 7465), "a.lan:9");
        assert_eq!(advertised_address(Some("a.lan"), 7465), "a.lan:7465");
        assert_eq!(advertised_address(Some("[::1]:9"), 7465), "[::1]:9");
        assert_eq!(advertised_address(Some("fe80::1"), 7465), "[fe80::1]:7465");
        assert!(advertised_address(None, 7465).ends_with(":7465"));
    }
}
