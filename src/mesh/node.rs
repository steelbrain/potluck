//! The running mesh: the listener, connections, hellos, calls, streams, record exchange and
//! heartbeats.

use super::identity::{Identity, PublicKey, ensure_dir};
use super::tls::{self, Duplex, DuplexIo};
use super::trust::{
    self, Admission, Changes, PairReply, PairRequest, Pending, Record, Removal, Trust,
    UserAdmission, View,
};
use super::{
    Event, Handler, Member, MemberEntry, MembershipView, MeshConfig, MeshError, Peer, Seen,
    UserEntry, advertised_address,
};
use crate::types::{Millis, NodeName, now_ms};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::IpAddr;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// How long a handshake and hello may take. Each read and write waits at most this long, and a
/// connection still in them after this is closed at the next heartbeat (a peer that trickles
/// bytes would otherwise hold its thread and socket for good).
pub(super) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// A control connection idle this long is closed by the listening side.
const IDLE_TIMEOUT: Duration = Duration::from_secs(600);
/// Pooled connections idle longer than this are not reused.
const POOL_IDLE: Duration = Duration::from_secs(60);
/// Idle connections kept per member.
const POOL_MAX: usize = 8;
pub(super) const MAX_HELLO: u64 = 64 * 1024;
/// A listening side's answer to a hello may carry its records.
const MAX_SERVER_HELLO: u64 = 16 << 20;
/// Connections not authenticated yet (in their handshake or hello): at most this many in all,
/// and from one address. More are closed at once.
pub(super) const MAX_UNAUTHENTICATED: usize = 64;
pub(super) const MAX_UNAUTHENTICATED_PER_IP: usize = 8;
/// At most this many `Refused` events per [`REFUSED_WINDOW`]; the rest are counted and the
/// count reported with the next one.
const REFUSED_PER_WINDOW: u32 = 10;
const REFUSED_WINDOW: Duration = Duration::from_secs(10);
/// [`Event::Unreachable`] is said of a member at most this often.
const UNREACHABLE_EVERY: Duration = Duration::from_secs(600);
/// Handshakes this machine runs at once with one member, so that it stays well under the
/// member's cap on unauthenticated connections; further callers wait for one to finish (or for
/// a pooled connection).
const MAX_DIALS_PER_PEER: usize = 4;
/// The longest line a control connection takes, its newline included: a request or its reply.
pub const MAX_FRAME: u64 = 64 << 20;
/// Events kept for the first subscriber, from before it subscribed; past this, the oldest go.
const EARLY_EVENTS: usize = 256;
/// The accept loop waits this long at most for a connection before it looks whether the mesh
/// is stopping.
const ACCEPT_WAIT: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Hello {
    cluster: PublicKey,
    node: NodeName,
    version: String,
    key: PublicKey,
    /// Where the sender listens; empty when it doesn't ([`Mesh::connect_only`],
    /// [`Mesh::start_unlistening`]): the other side keeps the address it has.
    address: String,
    /// Of the sender's records: when they differ, the dialing side starts an exchange.
    digest: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Open {
    Control,
    Stream { header: Value },
}

#[derive(Debug, Serialize, Deserialize)]
struct ClientHello {
    hello: Hello,
    open: Open,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ServerHello {
    Ok(Hello),
    Refused(Refusal),
    /// The dialing side was removed here, and its records differ: before it is refused, the
    /// listening side takes the records that count for it (a [`Frame::Sync`] line), which may
    /// void its removal, and answers again ([`Inner::take_from_removed`]).
    Records,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
enum Refusal {
    /// Another version: the listening side's hello, so the dialing side knows which.
    Version {
        hello: Hello,
    },
    /// The dialing side (or the listening side itself) was removed: the proof.
    Removed {
        removal: Removal,
    },
    Cluster,
    /// The dialing side's key is known (some record admits it) but it is not a member: the
    /// listening side's records, so it can see why.
    NotMember {
        records: Vec<Record>,
    },
    /// The listening side itself dropped out of the cluster.
    Dropped {
        why: String,
    },
    BadHello {
        message: String,
    },
}

/// A request on a control connection.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum Frame {
    Call {
        body: Value,
    },
    Ping {
        digest: String,
    },
    /// The sender's records; the answer carries the receiver's.
    Sync {
        records: Vec<Record>,
    },
    Announce {
        records: Vec<Record>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum Reply {
    Ok { body: Value },
    Err { message: String },
    Pong { digest: String },
    Sync { records: Vec<Record> },
    Done,
}

/// A member to dial.
#[derive(Clone, Debug)]
struct Target {
    name: NodeName,
    key: PublicKey,
    address: String,
}

#[derive(Debug)]
struct PeerState {
    known_since: Instant,
    /// None until first heard from or declared down.
    up: Option<bool>,
    last_seen: Option<Instant>,
    last_seen_ms: Option<Millis>,
    version: Option<String>,
    /// The other version last reported in an event, so it is reported once.
    mismatch: Option<String>,
    /// Since when it has answered on another version.
    mismatch_since: Option<Instant>,
    /// Why the last heartbeat to it failed, until one gets through: said when it goes down.
    last_error: Option<String>,
    /// When [`Event::Unreachable`] was last said of it.
    unreachable_said: Option<Instant>,
    /// When it last reached this machine: a connection or a request from it.
    reached_us: Option<Instant>,
    /// When the last heartbeat to it failed to reach it ([`MeshError::Unreachable`]), while
    /// they fail so.
    unreachable_at: Option<Instant>,
    /// Heartbeats in a row that failed to reach it, each after it reached this machine since
    /// the one before.
    one_way: u32,
}

impl PeerState {
    fn new() -> PeerState {
        PeerState {
            known_since: Instant::now(),
            up: None,
            last_seen: None,
            last_seen_ms: None,
            version: None,
            mismatch: None,
            mismatch_since: None,
            last_error: None,
            unreachable_said: None,
            reached_us: None,
            unreachable_at: None,
            one_way: 0,
        }
    }
}

/// Whom this machine compared its records with since it came back: see [`Mesh::caught_up_for`].
struct CatchUp {
    /// When it came back: its mesh started, or it heard from a member after hearing from none for
    /// the down time.
    since: Instant,
    /// The members whose records it compared with its own since then: their digest was the same,
    /// or the two exchanged records.
    compared: HashSet<PublicKey>,
    /// When it last heard from any member, on the boot clock
    /// ([`crate::daemon::clock::boot_ms`]): it counts time asleep, which a sleeping Mac's
    /// monotonic clock doesn't, and no step of the wall clock moves it. When its mesh started,
    /// until it hears from one, so a mesh that hears from nobody for the down time after it
    /// starts is silent too.
    last_contact: Option<u64>,
}

impl CatchUp {
    /// Whether no member has been heard from for `down_after`, at `now` on the boot clock.
    fn silent(&self, down_after: Duration, now: u64) -> bool {
        self.last_contact
            .is_some_and(|at| now.saturating_sub(at) >= down_after.as_millis() as u64)
    }
}

/// Who receives events, and the events kept until the first one subscribes.
struct Subscribers {
    senders: Vec<Sender<Event>>,
    early: Option<VecDeque<Event>>,
}

/// Keeps a socket in the mesh's list of open connections (so `stop` and removals can cut it)
/// until dropped.
struct Registration {
    id: u64,
    inner: Weak<Inner>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            lock(&inner.open).remove(&self.id);
        }
    }
}

/// An open connection, in the mesh's list.
struct OpenConn {
    /// The member on the other end, once known.
    key: Option<PublicKey>,
    sock: TcpStream,
    /// Until its handshake and hello are done, when they must be done by.
    hello_by: Option<Instant>,
}

/// An idle control connection.
struct Pooled {
    duplex: Arc<Duplex>,
    reader: BufReader<DuplexIo>,
    _reg: Registration,
    idle_since: Instant,
}

enum RoundTrip {
    /// The connection was closed before the request got an answer: the peer closed it while
    /// idle, so a new connection may retry.
    Stale,
    Failed(io::Error),
}

impl Pooled {
    fn roundtrip(&mut self, line: &[u8], timeout: Duration) -> Result<Reply, RoundTrip> {
        let _ = self.duplex.set_read_timeout(Some(timeout));
        if let Err(e) = write_all(&self.duplex, line) {
            return Err(if is_closed(&e) {
                RoundTrip::Stale
            } else {
                RoundTrip::Failed(e)
            });
        }
        let mut buf = Vec::new();
        match (&mut self.reader)
            .take(MAX_FRAME)
            .read_until(b'\n', &mut buf)
        {
            Ok(0) => Err(RoundTrip::Stale),
            Ok(_) if buf.last() == Some(&b'\n') => serde_json::from_slice(&buf)
                .map_err(|e| RoundTrip::Failed(io::Error::new(io::ErrorKind::InvalidData, e))),
            Ok(_) => Err(RoundTrip::Failed(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed mid-reply",
            ))),
            Err(e) if buf.is_empty() && is_closed(&e) => Err(RoundTrip::Stale),
            Err(e) => Err(RoundTrip::Failed(e)),
        }
    }
}

fn is_closed(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}

/// Counts the connections not authenticated yet, in all and per source address.
#[derive(Debug, Default)]
pub(super) struct PreAuth {
    total: usize,
    per_ip: HashMap<IpAddr, usize>,
}

impl PreAuth {
    /// Takes a slot for a connection from `ip`, if under both caps.
    pub(super) fn try_take(&mut self, ip: IpAddr, total_cap: usize, per_ip_cap: usize) -> bool {
        let n = self.per_ip.get(&ip).copied().unwrap_or(0);
        if self.total >= total_cap || n >= per_ip_cap {
            return false;
        }
        self.total += 1;
        self.per_ip.insert(ip, n + 1);
        true
    }

    pub(super) fn release(&mut self, ip: IpAddr) {
        self.total = self.total.saturating_sub(1);
        if let Some(n) = self.per_ip.get_mut(&ip) {
            *n -= 1;
            if *n == 0 {
                self.per_ip.remove(&ip);
            }
        }
    }
}

/// A connection's slot among those not authenticated yet: given back once its hello is done,
/// or when it ends.
struct PreAuthSlot {
    inner: Weak<Inner>,
    ip: IpAddr,
    held: bool,
}

impl PreAuthSlot {
    fn release(&mut self) {
        if std::mem::take(&mut self.held)
            && let Some(inner) = self.inner.upgrade()
        {
            lock(&inner.preauth).release(self.ip);
        }
    }
}

impl Drop for PreAuthSlot {
    fn drop(&mut self) {
        self.release();
    }
}

/// One of [`MAX_DIALS_PER_PEER`] handshakes with a member, given back when dropped.
struct DialSlot<'a> {
    inner: &'a Inner,
    key: PublicKey,
}

impl Drop for DialSlot<'_> {
    fn drop(&mut self) {
        let mut dials = lock(&self.inner.dials);
        if let Some(n) = dials.get_mut(&self.key) {
            *n -= 1;
            if *n == 0 {
                dials.remove(&self.key);
            }
        }
        self.inner.dials_cv.notify_all();
    }
}

/// Rate limit for `Refused` events.
#[derive(Debug)]
struct RefusedLog {
    window: Instant,
    sent: u32,
    suppressed: u64,
}

/// How this machine left its cluster, for events.
enum SelfChange {
    Removed(Removal),
    Dropped(String),
}

struct NoHandler;

impl Handler for NoHandler {
    fn call(&self, _from: &Peer, _msg: Value) -> Result<Value, String> {
        Err("this machine only dials out".into())
    }
}

struct Inner {
    cfg: MeshConfig,
    /// The address this machine advertises.
    address: String,
    /// None for a mesh that doesn't listen.
    local_addr: Option<SocketAddr>,
    /// A [`Mesh::connect_only`] mesh: the records are read-only, so no pairing.
    dial_only: bool,
    handler: Arc<dyn Handler>,
    identity: RwLock<Arc<Identity>>,
    trust: Arc<Mutex<Trust>>,
    peers: Mutex<HashMap<PublicKey, PeerState>>,
    pool: Mutex<HashMap<PublicKey, Vec<Pooled>>>,
    open: Mutex<HashMap<u64, OpenConn>>,
    next_id: AtomicU64,
    subscribers: Mutex<Subscribers>,
    stopping: AtomicBool,
    /// Set to make the heartbeat run now (and on stop).
    wake: (Mutex<bool>, Condvar),
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// Members with a heartbeat ping in flight.
    pinging: Mutex<HashSet<PublicKey>>,
    catchup: Mutex<CatchUp>,
    preauth: Mutex<PreAuth>,
    refused_log: Mutex<RefusedLog>,
    /// Handshakes in flight per member, and a wakeup when one ends or a connection is pooled.
    dials: Mutex<HashMap<PublicKey, usize>>,
    dials_cv: Condvar,
    /// Per member, the digests (this machine's, the member's) of the last record exchange with it
    /// that changed neither side's records. Each member keeps its own records that are void, and
    /// those it took before they turned void, which the other turns away, so their digests may
    /// differ for good: exchanging again at the same two digests would change nothing either,
    /// and is skipped.
    in_vain: Mutex<HashMap<PublicKey, (String, String)>>,
    /// Per removed key, the digest it gave when this machine last took its records before
    /// refusing it, and when ([`Inner::take_from_removed`]).
    removed_taken: Mutex<HashMap<PublicKey, (String, Instant)>>,
    /// The trust records changed (records merged from a member, an address heard) and couldn't
    /// be saved: the heartbeat tries again.
    unsaved: AtomicBool,
    /// When this machine, out of its cluster, last pinged the members it knows
    /// ([`Inner::former_members`]): once per down time.
    former_pinged: Mutex<Option<Instant>>,
    /// False once it stops taking connections ([`Mesh::stop_accepting`]).
    accepting: AtomicBool,
    /// Wakes the accept loop: written to, never read.
    accept_wake: Option<(
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
    )>,
    /// A copy of the listener, for [`Mesh::hand_over`].
    kept: Mutex<Option<TcpListener>>,
    /// Record exchanges this machine started (for tests).
    #[cfg(test)]
    exchanges: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A running member: listens on its port, answers members through its [`Handler`], dials them
/// for [`Mesh::call`] and [`Mesh::open_stream`], keeps the trust records and sends heartbeats.
///
/// Cloning gives another handle on the same mesh. [`Mesh::stop`] ends it.
#[derive(Clone)]
pub struct Mesh {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Mesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Mesh({}, {})", self.name(), self.inner.address)
    }
}

impl Mesh {
    /// Loads (or creates) the key pair and the trust records, listens, and starts heartbeats.
    /// A machine with no records founds a cluster of its own, ready to admit others or to pair
    /// into a cluster.
    pub fn start(cfg: MeshConfig, handler: Arc<dyn Handler>) -> Result<Mesh, MeshError> {
        Mesh::open(cfg, handler, true, true)
    }

    /// [`Mesh::start`] without the listener and the heartbeats, for a machine in no cluster: it
    /// makes pairing requests, admits and finishes pairing (advertising the configured address and
    /// port), and nobody can reach it. Once it is in a cluster, the daemon stops this mesh and
    /// starts it again with [`Mesh::start`].
    pub fn start_unlistening(
        cfg: MeshConfig,
        handler: Arc<dyn Handler>,
    ) -> Result<Mesh, MeshError> {
        Mesh::open(cfg, handler, false, false)
    }

    /// [`Mesh::start_unlistening`] with heartbeats, for a machine in a cluster that can't listen
    /// right now (its port is taken): nobody can reach it, but it pings the members, so it still
    /// learns which are up and which are down.
    pub fn start_dialing(cfg: MeshConfig, handler: Arc<dyn Handler>) -> Result<Mesh, MeshError> {
        Mesh::open(cfg, handler, false, true)
    }

    /// [`Mesh::start`] on a listener an earlier mesh of this machine handed over
    /// ([`Mesh::hand_over`], across a restart in place), with the members as it saw them:
    /// nothing is bound, members up stay up and members down stay down. Records are compared
    /// with the members as after any start.
    pub fn start_on(
        cfg: MeshConfig,
        handler: Arc<dyn Handler>,
        listener: TcpListener,
        seen: Vec<Seen>,
    ) -> Result<Mesh, MeshError> {
        Mesh::open_with(cfg, handler, true, true, Some((listener, seen)))
    }

    fn open(
        cfg: MeshConfig,
        handler: Arc<dyn Handler>,
        listen: bool,
        heartbeats: bool,
    ) -> Result<Mesh, MeshError> {
        Mesh::open_with(cfg, handler, listen, heartbeats, None)
    }

    fn open_with(
        cfg: MeshConfig,
        handler: Arc<dyn Handler>,
        listen: bool,
        heartbeats: bool,
        inherited: Option<(TcpListener, Vec<Seen>)>,
    ) -> Result<Mesh, MeshError> {
        if !trust::valid_name(&cfg.name) {
            return Err(MeshError::Invalid(format!(
                "bad node name {:?}: use letters, digits, '.', '_' and '-', not starting with '-' (set `name` in the node config)",
                cfg.name
            )));
        }
        ensure_dir(&cfg.keys_dir)?;
        let identity = Identity::load_or_create(&cfg.keys_dir)?;
        let (inherited, seen) = match inherited {
            Some((l, seen)) => (Some(l), seen),
            None => (None, Vec::new()),
        };
        let (listener, local) = if listen {
            let listener = match inherited {
                Some(l) => l,
                None => TcpListener::bind(cfg.listen).map_err(|e| {
                    MeshError::Invalid(format!("can't listen on {}: {e}", cfg.listen))
                })?,
            };
            let local = listener
                .local_addr()
                .map_err(|e| MeshError::Invalid(format!("listening socket: {e}")))?;
            (Some(listener), Some(local))
        } else {
            (None, None)
        };
        let port = local.map_or(cfg.listen.port(), |l| l.port());
        let address = advertised_address(cfg.address.as_deref(), port);
        let mut trust = Trust::load_or_found(&cfg.keys_dir, &identity, &cfg.name, &address)?;
        let me = identity.public();
        if !trust
            .records()
            .iter()
            .any(|r| matches!(r, Record::Admission(a) if a.key == me))
        {
            // The records belong to another key (the key file was replaced): start over alone.
            trust.refound(&identity, &cfg.name, &address)?;
        }
        let mut inner = Inner::new(cfg, address, local, handler, identity, trust);
        inner.see(seen);
        if let Some(l) = &listener {
            // Kept for a handover; the accept loop has the listener itself.
            *inner.kept.get_mut().unwrap_or_else(|e| e.into_inner()) = l.try_clone().ok();
        }
        let inner = Arc::new(inner);
        if let Some(listener) = listener {
            let inner2 = inner.clone();
            let l = thread::Builder::new()
                .name("mesh-listen".into())
                .spawn(move || inner2.accept_loop(listener))
                .map_err(|e| MeshError::Invalid(format!("thread: {e}")))?;
            lock(&inner.threads).push(l);
        }
        if heartbeats {
            let inner2 = inner.clone();
            let h = thread::Builder::new()
                .name("mesh-heartbeat".into())
                .spawn(move || inner2.heartbeat_loop())
                .map_err(|e| MeshError::Invalid(format!("thread: {e}")))?;
            lock(&inner.threads).push(h);
        }
        Ok(Mesh { inner })
    }

    /// A mesh that only dials out, for a short-lived process beside the daemon (such as the
    /// `_tunnel` that git runs): the daemon's key and records, read and never written; no
    /// listener and no heartbeats.
    pub fn connect_only(cfg: MeshConfig) -> Result<Mesh, MeshError> {
        let identity = Identity::load(&cfg.keys_dir)?;
        let trust = Trust::load_read_only(&cfg.keys_dir)?;
        let address = advertised_address(cfg.address.as_deref(), cfg.listen.port());
        let mut inner = Inner::new(cfg, address, None, Arc::new(NoHandler), identity, trust);
        inner.dial_only = true;
        Ok(Mesh {
            inner: Arc::new(inner),
        })
    }

    /// Stops listening and heartbeats and closes every connection. Calls fail with
    /// [`MeshError::Stopped`] after.
    pub fn stop(&self) {
        let inner = &self.inner;
        if inner.stopping.swap(true, Ordering::SeqCst) {
            return;
        }
        inner.kick();
        inner.wake_accept();
        // Without a wake-up pipe, a connection of its own wakes the accept loop sooner (it looks
        // at the flag every ACCEPT_WAIT anyway). Never once it stopped accepting for a handover:
        // the connection would wait in the backlog for the next mesh.
        if inner.accept_wake.is_none()
            && inner.accepting.load(Ordering::SeqCst)
            && let Some(addr) = inner.local_addr
        {
            let ip = match addr.ip() {
                std::net::IpAddr::V4(v) if v.is_unspecified() => {
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                }
                std::net::IpAddr::V6(v) if v.is_unspecified() => {
                    std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
                }
                ip => ip,
            };
            let _ =
                TcpStream::connect_timeout(&SocketAddr::new(ip, addr.port()), HANDSHAKE_TIMEOUT);
        }
        inner.cut_all();
        let threads = std::mem::take(&mut *lock(&inner.threads));
        for t in threads {
            let _ = t.join();
        }
        inner.cut_all();
        // The copy kept for a handover ([`Mesh::hand_over`] takes it before it stops) goes too:
        // the port is free once this returns, though the Mesh lives on.
        if let Some(listener) = lock(&inner.kept).take() {
            release(listener);
        }
        // No events come after this: receivers see so at once.
        lock(&inner.subscribers).senders.clear();
    }

    /// Stops taking connections off the listener, which stays open: members' connections wait
    /// in the backlog (a restart in place hands the listener over next, [`Mesh::hand_over`]).
    pub fn stop_accepting(&self) {
        self.inner.accepting.store(false, Ordering::SeqCst);
        self.inner.wake_accept();
    }

    /// Stops ([`Mesh::stop`]) and returns the listener, still open, with the members as this mesh
    /// saw them, for [`Mesh::start_on`] in the daemon that comes next.
    pub fn hand_over(&self) -> (Option<TcpListener>, Vec<Seen>) {
        self.stop_accepting();
        let seen = self.inner.seen();
        let listener = lock(&self.inner.kept).take();
        self.stop();
        (listener, seen)
    }

    /// The name this machine has in its cluster.
    pub fn name(&self) -> NodeName {
        self.inner.my_name()
    }

    pub fn key(&self) -> PublicKey {
        self.inner.key()
    }

    /// The cluster's id: its founder's key.
    pub fn cluster(&self) -> PublicKey {
        lock(&self.inner.trust).cluster
    }

    /// The address this machine advertises to members.
    pub fn address(&self) -> String {
        self.inner.address.clone()
    }

    /// Where it listens (None for [`Mesh::connect_only`], [`Mesh::start_unlistening`] and
    /// [`Mesh::start_dialing`]).
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.inner.local_addr
    }

    /// Whether this machine is out of its cluster: removed, or dropped (its admission no longer
    /// counts). It must pair again, unless what put it out turns out void: a removal of the
    /// machine that removed it (or of the one whose removal dropped it) counts, signed before that
    /// member heard of it. It is then a member again, with no event; it keeps asking the members
    /// it knows meanwhile.
    pub fn is_removed(&self) -> bool {
        let me = self.key();
        !lock(&self.inner.trust).view.members.contains_key(&me)
    }

    /// Whether the trust records on disk are the ones this mesh holds. Records merged from a
    /// member (or an address heard) that couldn't be saved are saved now if they can be. A mesh
    /// started in this one's place reads the records from disk, so what this one couldn't save
    /// would be lost.
    pub fn records_saved(&self) -> bool {
        let inner = &self.inner;
        if inner.unsaved.load(Ordering::Relaxed) {
            inner.save_or_retry(&lock(&inner.trust));
        }
        !inner.unsaved.load(Ordering::Relaxed)
    }

    /// How many record exchanges this machine started.
    #[cfg(test)]
    pub(super) fn exchanges(&self) -> u64 {
        self.inner.exchanges.load(Ordering::SeqCst)
    }

    /// Every member, this machine included, with what is known of its liveness.
    pub fn members(&self) -> Vec<Member> {
        self.inner.members()
    }

    pub fn member(&self, name: &str) -> Option<Member> {
        self.members().into_iter().find(|m| m.name == name)
    }

    /// The members admitted before `node` (a member or a removed machine), oldest first: the
    /// founder, then by admission. A removal one of them signs is final; one a younger member
    /// signs holds until `node` removes that member back (removals that void each other go to
    /// the member admitted first). Empty for the founder, or a name no record admits.
    pub fn admitted_before(&self, node: &str) -> Vec<String> {
        let t = lock(&self.inner.trust);
        let key = match t.view.by_name(node) {
            Some(a) => a.key,
            None => match t.view.removed.values().find(|r| r.node == node) {
                Some(r) => r.key,
                None => return Vec::new(),
            },
        };
        let ranks = t.ranks();
        let Some(&rank) = ranks.get(&key) else {
            return Vec::new();
        };
        let mut before: Vec<((u64, trust::RecordId), String)> = t
            .view
            .members
            .values()
            .filter_map(|a| ranks.get(&a.key).map(|&r| (r, a.node.clone())))
            .filter(|(r, _)| *r < rank)
            .collect();
        before.sort();
        before.into_iter().map(|(_, n)| n).collect()
    }

    /// Whether this machine may take work from member `node` or hand work to it. Once back (its
    /// mesh started, or it heard from a member after hearing from none for the down time), it
    /// first compares its records with every other member, waiting up to the down time for those
    /// it doesn't hear from. So a member removed while this machine was away is known as removed
    /// before this machine deals with it, whatever records that member shows, provided a member
    /// that knows of the removal answers in that time. False while no member has been heard from
    /// for the down time, and for a machine that is not a member.
    pub fn caught_up_for(&self, node: &str) -> bool {
        let inner = &self.inner;
        let me = inner.key();
        let others: Vec<PublicKey> = {
            let t = lock(&inner.trust);
            let Some(peer) = t.view.by_name(node) else {
                return false;
            };
            if !t.view.members.contains_key(&me) || peer.key == me {
                return false;
            }
            let peer = peer.key;
            t.view
                .members
                .keys()
                .filter(|k| **k != me && **k != peer)
                .copied()
                .collect()
        };
        let down_after = inner.cfg.down_after;
        let cu = lock(&inner.catchup);
        !cu.silent(down_after, crate::daemon::clock::boot_ms())
            && (cu.since.elapsed() >= down_after || others.iter().all(|k| cu.compared.contains(k)))
    }

    /// A new receiver of every [`Event`] from now on. The first receiver also gets the events
    /// since the mesh started (the last 256), so none is missed between [`Mesh::start`] and
    /// subscribing: a member may connect, or this machine learn it was removed, at once.
    pub fn subscribe(&self) -> Receiver<Event> {
        let (tx, rx) = mpsc::channel();
        let mut s = lock(&self.inner.subscribers);
        for e in s.early.take().into_iter().flatten() {
            let _ = tx.send(e);
        }
        s.senders.push(tx);
        rx
    }

    /// Sends `msg` to `node`'s handler and returns its answer, waiting at most the configured
    /// call timeout. Connections are reused; if one turns out to have been closed by the other
    /// side while idle, the message is sent again on a new one (handlers should tolerate the rare
    /// repeat).
    pub fn call(&self, node: &str, msg: Value) -> Result<Value, MeshError> {
        self.call_timeout(node, msg, self.inner.cfg.call_timeout)
    }

    pub fn call_timeout(
        &self,
        node: &str,
        msg: Value,
        timeout: Duration,
    ) -> Result<Value, MeshError> {
        let t = self.inner.target(node)?;
        match self
            .inner
            .request(&t, &Frame::Call { body: msg }, timeout)?
        {
            Reply::Ok { body } => Ok(body),
            Reply::Err { message } => Err(MeshError::Remote {
                node: t.name,
                message,
            }),
            other => Err(MeshError::Protocol(format!("unexpected reply {other:?}"))),
        }
    }

    /// Opens a stream to `node`: a connection of its own, handed to its handler's
    /// [`Handler::stream`] with `header`. Then raw bytes both ways. No timeouts are set on it.
    pub fn open_stream(&self, node: &str, header: Value) -> Result<Stream, MeshError> {
        let t = self.inner.target(node)?;
        let (duplex, hello, reg) = self.inner.dial_limited(&t, Open::Stream { header })?;
        let _ = duplex.set_read_timeout(None);
        let _ = duplex.set_write_timeout(None);
        let pool = lock(&self.inner.trust)
            .pool_of(&hello.key)
            .unwrap_or(trust::DEFAULT_POOL)
            .to_string();
        let peer = Peer {
            name: hello.node,
            key: hello.key,
            address: hello.address,
            version: hello.version,
            pool,
        };
        Ok(Stream::new(duplex, peer, reg))
    }

    /// Pairing, on the new machine: a request to paste into [`Mesh::admit`] on a member. Asking
    /// again gives the same request until a reply is used. A machine removed from its cluster
    /// makes a new key first (the old one is refused everywhere).
    pub fn pair_request(&self) -> Result<String, MeshError> {
        let inner = &self.inner;
        if inner.dial_only {
            return Err(MeshError::Invalid("pairing needs the daemon".into()));
        }
        let mut t = lock(&inner.trust);
        let mut id = inner.identity();
        let me = id.public();
        let mut rotated = false;
        if t.view.removed.contains_key(&me) {
            let new_id = Arc::new(Identity::create(&inner.cfg.keys_dir)?);
            let before = t.clone();
            if let Err(e) = t.refound(&new_id, &inner.cfg.name, &inner.address) {
                // Still the removed machine, with its old key: the next `pair` makes another.
                *t = before;
                return Err(e);
            }
            *inner.identity.write().unwrap_or_else(|e| e.into_inner()) = new_id.clone();
            id = new_id;
            rotated = true;
        } else if t.view.members.contains_key(&me) && t.view.members.len() > 1 {
            let mine = t.view.members[&me].node.clone();
            let others: Vec<&str> = t
                .view
                .members
                .values()
                .filter(|a| a.key != me)
                .map(|a| a.node.as_str())
                .collect();
            return Err(MeshError::Invalid(format!(
                "this machine is already in a cluster (with {}); to move it, run `potluck remove {mine}` on a member first",
                others.join(", ")
            )));
        }
        let nonce = match &t.pending {
            Some(p) => p.nonce.clone(),
            None => {
                let n = trust::nonce();
                t.pending = Some(Pending {
                    nonce: n.clone(),
                    at: now_ms(),
                });
                if let Err(e) = t.save() {
                    t.pending = None;
                    return Err(e);
                }
                n
            }
        };
        let name = t
            .view
            .members
            .get(&id.public())
            .map(|a| a.node.clone())
            .unwrap_or_else(|| inner.cfg.name.clone());
        drop(t);
        if rotated {
            inner.cut_all();
            lock(&inner.peers).clear();
        }
        Ok(PairRequest {
            v: 1,
            name,
            key: id.public(),
            address: inner.address.clone(),
            version: inner.cfg.version.clone(),
            nonce,
        }
        .to_text())
    }

    /// Pairing, on any member: admits the machine that made `request` and returns the reply to
    /// paste into [`Mesh::pair_finish`] there. The admission is announced to every member. Asking
    /// again for a machine already admitted gives a reply again. In a cluster without users:
    /// this machine signs the admission ([`Mesh::admit_signed`] in one with users).
    pub fn admit(&self, request: &str) -> Result<String, MeshError> {
        self.admit_signed(request, None)
    }

    /// [`Mesh::admit`] with the admission a user signed of the machine that made `request` (v3,
    /// its pool in it): what admits a machine in a cluster with users, where this machine signs
    /// none. Without users, `admission` must be None: it would count for nothing.
    pub fn admit_signed(
        &self,
        request: &str,
        admission: Option<Admission>,
    ) -> Result<String, MeshError> {
        let inner = &self.inner;
        let req = PairRequest::from_text(request)?;
        if req.v != 1 {
            return Err(MeshError::Pairing(
                "the request comes from an incompatible potluck; update both machines".into(),
            ));
        }
        if !trust::valid_name(&req.name) {
            return Err(MeshError::Pairing(format!(
                "the new machine's name {:?} is not usable: set `name` in its node.toml to letters, digits, '.', '_' and '-', not starting with '-'",
                req.name
            )));
        }
        if !super::same_version(&req.version, &inner.cfg.version) {
            return Err(MeshError::VersionMismatch {
                node: req.name,
                ours: inner.cfg.version.clone(),
                theirs: req.version,
            });
        }
        let id = inner.identity();
        let me = id.public();
        let (reply, changes, announce) = {
            let mut t = lock(&inner.trust);
            let my_name = standing(&t, &me)?;
            match (&admission, t.users_exist()) {
                (None, true) => {
                    return Err(MeshError::Pairing(
                        "this cluster has users: only a user's signed admission admits a machine (`potluck admit` where a user's key is)".into(),
                    ));
                }
                (Some(_), false) => {
                    return Err(MeshError::Pairing(
                        "this cluster has no users yet, so an admission a user signed would count for nothing; run `potluck user init` first".into(),
                    ));
                }
                (Some(a), true) => {
                    let matches = a.by_user()
                        && a.key == req.key
                        && a.node == req.name
                        && a.address == req.address
                        && a.nonce == req.nonce;
                    if !matches {
                        return Err(MeshError::Pairing(
                            "the signed admission is of another request".into(),
                        ));
                    }
                    if !trust::valid_pool(a.pool_name()) {
                        return Err(MeshError::Pairing(format!(
                            "bad pool name {:?}: use letters, digits, '.', '_' and '-', not starting with '-'",
                            a.pool_name()
                        )));
                    }
                    if !a.verify() {
                        return Err(MeshError::Pairing(
                            "the admission's signature is not good".into(),
                        ));
                    }
                }
                (None, false) => {}
            }
            if req.key == me {
                return Err(MeshError::Pairing(
                    "that request was made on this machine: run `potluck pair` on the new one"
                        .into(),
                ));
            }
            if t.view.removed.contains_key(&req.key) {
                return Err(MeshError::Pairing(format!(
                    "{}'s key was removed from the cluster; run `potluck pair` there again once it knows (it makes a new key)",
                    req.name
                )));
            }
            let mut changes = Changes::default();
            let mut announce = Vec::new();
            match t.view.by_name(&req.name) {
                Some(a) if a.key != req.key => {
                    return Err(MeshError::Pairing(format!(
                        "there is already a member named {}: give the new machine another name (`name` in its node.toml)",
                        req.name
                    )));
                }
                Some(_) => {}
                None if t.view.members.len() >= t.max_members() => {
                    return Err(MeshError::Pairing(format!(
                        "the cluster has {} members, the most it can have: remove one (`potluck remove`) to admit {}",
                        t.view.members.len(),
                        req.name
                    )));
                }
                None => {
                    let adm = match admission {
                        Some(a) => a,
                        None => Admission::sign(
                            &id,
                            &my_name,
                            &req.name,
                            req.key,
                            &req.address,
                            &req.nonce,
                            t.next_clock(),
                        ),
                    };
                    let before = t.clone();
                    changes = t.merge(vec![Record::Admission(adm.clone())]);
                    if !t.view.members.contains_key(&req.key) {
                        let why = t.why_not_member(&req.key);
                        *t = before;
                        return Err(MeshError::Pairing(format!(
                            "the admission of {} did not take: {why}",
                            req.name
                        )));
                    }
                    save_or_undo(&mut t, before)?;
                    announce.push(Record::Admission(adm));
                }
            }
            let reply = PairReply {
                v: 1,
                cluster: t.cluster,
                key: req.key,
                nonce: req.nonce.clone(),
                // What makes the members members; the new member gets the rest when it first
                // compares records with a member.
                records: t.members_proof(),
                addresses: t
                    .view
                    .members
                    .keys()
                    .filter_map(|k| match *k == me && inner.local_addr.is_some() {
                        // Where this machine listens now (its records may name an older port).
                        true => Some((*k, inner.address.clone())),
                        false => t.address_of(k).map(|a| (*k, a)),
                    })
                    .collect(),
            };
            (reply, changes, announce)
        };
        inner.apply_changes(changes, None);
        if !announce.is_empty() {
            let targets = inner.targets_except(&[me, req.key]);
            inner.announce(announce, targets);
        }
        Ok(reply.to_text())
    }

    /// Pairing, on the new machine: stores the reply [`Mesh::admit`] gave and joins. Returns the
    /// members.
    pub fn pair_finish(&self, reply: &str) -> Result<Vec<Member>, MeshError> {
        let inner = &self.inner;
        let reply = PairReply::from_text(reply)?;
        if reply.v != 1 {
            return Err(MeshError::Pairing(
                "the reply comes from an incompatible potluck; update both machines".into(),
            ));
        }
        let me = inner.key();
        let changes = 'pair: {
            let mut t = lock(&inner.trust);
            if reply.key != me {
                return Err(MeshError::Pairing(
                    "this reply admits another machine (or this machine's key changed since `potluck pair`); run `potluck pair` again".into(),
                ));
            }
            if t.pending.is_none() && t.cluster == reply.cluster && t.view.members.contains_key(&me)
            {
                // Already joined with this reply.
                break 'pair Changes::default();
            }
            let pending = t.pending.clone().ok_or_else(|| {
                MeshError::Pairing(
                    "no pairing request is pending on this machine; run `potluck pair` first"
                        .into(),
                )
            })?;
            if reply.nonce != pending.nonce {
                return Err(MeshError::Pairing(
                    "this reply answers an older pairing request: admit the one `potluck pair` prints now".into(),
                ));
            }
            if !View::compute(&reply.cluster, &reply.records)
                .members
                .contains_key(&me)
            {
                return Err(MeshError::Pairing(
                    "the reply does not admit this machine".into(),
                ));
            }
            if t.view.members.contains_key(&me) && t.view.members.len() > 1 {
                return Err(MeshError::Invalid(
                    "this machine is already in a cluster; remove it there first".into(),
                ));
            }
            let before = t.clone();
            let changes = t.adopt(reply);
            save_or_undo(&mut t, before)?;
            drop(t);
            inner.cut_all();
            lock(&inner.peers).clear();
            changes
        };
        inner.apply_changes(changes, None);
        inner.kick();
        Ok(self.members())
    }

    /// Removes `node` from the cluster for good, and announces it to every member (and to the
    /// removed machine, so it knows). Removing a machine already removed does nothing, unless
    /// only a removal it can undo removed it and this machine was admitted before it: then this
    /// machine signs one it can't (see [`Mesh::admitted_before`]).
    pub fn remove(&self, node: &str) -> Result<(), MeshError> {
        self.remove_signed(node, None)
    }

    /// [`Mesh::remove`] with the removal a user signed of `node` (v3): what removes a machine in a
    /// cluster with users, where this machine signs none. Without users, `removal` must be None.
    pub fn remove_signed(&self, node: &str, removal: Option<Removal>) -> Result<(), MeshError> {
        let inner = &self.inner;
        let id = inner.identity();
        let me = id.public();
        let (rec, changes, mut targets, notify) = {
            let mut t = lock(&inner.trust);
            let my_name = standing(&t, &me)?;
            match (&removal, t.users_exist()) {
                (None, true) => {
                    return Err(MeshError::Invalid(
                        "this cluster has users: only a user's signed removal removes a machine (`potluck remove` where a user's key is)".into(),
                    ));
                }
                (Some(_), false) => {
                    return Err(MeshError::Invalid(
                        "this cluster has no users yet, so a removal a user signed would count for nothing; run `potluck user init` first".into(),
                    ));
                }
                _ => {}
            }
            if let Some(rm) = &removal {
                if t.view.removed.values().any(|r| r.node == node) && t.view.by_name(node).is_none()
                {
                    // Removed already: by a user, so for good.
                    return Ok(());
                }
                let target = t
                    .view
                    .by_name(node)
                    .cloned()
                    .ok_or_else(|| MeshError::NotMember(node.to_string()))?;
                if target.key == me {
                    return Err(MeshError::Invalid(
                        "a machine can't remove itself: run `potluck remove` on another member, with a user's key".into(),
                    ));
                }
                if !rm.by_user || rm.key != target.key || rm.node != target.node {
                    return Err(MeshError::Invalid(format!(
                        "the signed removal is not of {} as this machine knows it",
                        target.node
                    )));
                }
                if !rm.verify() {
                    return Err(MeshError::Invalid(
                        "the removal's signature is not good".into(),
                    ));
                }
                let before = t.clone();
                let changes = t.merge(vec![Record::Removal(rm.clone())]);
                if !t.view.valid.contains(&rm.id()) {
                    let why = match t.recognized_user(&rm.by_key) {
                        true => "its signer's removal voids it".to_string(),
                        false => format!("{} is no user here", rm.by),
                    };
                    *t = before;
                    return Err(MeshError::Invalid(format!(
                        "the removal of {} did not take ({why}); nothing was changed",
                        target.node
                    )));
                }
                save_or_undo(&mut t, before)?;
                let notify = Target {
                    name: target.node.clone(),
                    key: target.key,
                    address: t.address_of(&target.key).unwrap_or(target.address),
                };
                (rm.clone(), changes, Vec::new(), notify)
            } else {
                let target = match t.view.by_name(node) {
                    Some(a) => a.clone(),
                    None => {
                        let Some(gone) = t.view.removed.values().find(|r| r.node == node).cloned()
                        else {
                            return Err(MeshError::NotMember(node.to_string()));
                        };
                        // Removed already: by a removal it can't undo, or by a younger member's,
                        // which this machine makes final if it ranks before it.
                        let ranks = t.ranks();
                        let elder = ranks
                            .get(&me)
                            .is_some_and(|m| ranks.get(&gone.key).is_some_and(|g| m < g));
                        if !elder || t.removed_for_good(&gone.key) {
                            return Ok(());
                        }
                        t.records()
                            .iter()
                            .find_map(|r| match r {
                                Record::Admission(a) if a.key == gone.key => Some(a.clone()),
                                _ => None,
                            })
                            .ok_or_else(|| MeshError::NotMember(node.to_string()))?
                    }
                };
                if target.key == me {
                    return Err(MeshError::Invalid(
                        "a machine can't remove itself: run `potluck remove` on another member"
                            .into(),
                    ));
                }
                // Every record of the removed machine's that counts here stays valid; any other it
                // signed is void (see `Trust::signed_by`).
                let kept = t.signed_by(&target.key);
                let clock = t.next_clock();
                let rec = Removal::sign(&id, &my_name, &target.node, target.key, kept, clock);
                let before = t.clone();
                let changes = t.merge(vec![Record::Removal(rec.clone())]);
                if !t.view.valid.contains(&rec.id()) {
                    *t = before;
                    return Err(MeshError::Invalid(format!(
                        "the removal of {} did not take; nothing was changed",
                        target.node
                    )));
                }
                save_or_undo(&mut t, before)?;
                let notify = Target {
                    name: target.node.clone(),
                    key: target.key,
                    address: t.address_of(&target.key).unwrap_or(target.address),
                };
                (rec, changes, Vec::new(), notify)
            }
        };
        inner.apply_changes_removing(changes, None, Some(notify.key));
        targets.extend(inner.targets_except(&[me]));
        targets.push(notify);
        inner.announce(vec![Record::Removal(rec)], targets);
        Ok(())
    }

    /// Whether users sign what counts in this machine's cluster: from then on only a user's
    /// signed admission or removal admits or removes a machine, and records members sign count
    /// for nothing.
    pub fn users_exist(&self) -> bool {
        lock(&self.inner.trust).users_exist()
    }

    /// Whether `key` is the key of a user that counts here (admitted, not removed).
    pub fn recognized_user(&self, key: &PublicKey) -> bool {
        lock(&self.inner.trust).recognized_user(key)
    }

    /// Member `node`'s pool, from the admission that made it a member
    /// ([`trust::DEFAULT_POOL`] in a cluster without users); None for a name no member has.
    pub fn pool_of(&self, node: &str) -> Option<String> {
        let t = lock(&self.inner.trust);
        t.view.by_name(node).map(|a| a.pool_name().to_string())
    }

    /// What the command line signs membership records from.
    pub fn membership(&self) -> MembershipView {
        let inner = &self.inner;
        let me = inner.key();
        let t = lock(&inner.trust);
        let entry = |a: &Admission| MemberEntry {
            name: a.node.clone(),
            key: a.key,
            address: match a.key == me && inner.local_addr.is_some() {
                true => inner.address.clone(),
                false => t.address_of(&a.key).unwrap_or_else(|| a.address.clone()),
            },
            pool: a.pool_name().to_string(),
        };
        let mut members: Vec<MemberEntry> = t.view.members.values().map(entry).collect();
        members.sort_by(|a, b| a.name.cmp(&b.name));
        let removed = t
            .view
            .removed
            .values()
            .map(|r| MemberEntry {
                name: r.node.clone(),
                key: r.key,
                address: t.address_of(&r.key).unwrap_or_default(),
                pool: String::new(),
            })
            .collect::<Vec<_>>();
        let mut users: Vec<UserEntry> = t
            .view
            .users
            .values()
            .map(|u| UserEntry {
                name: u.name.clone(),
                key: u.key,
                by: u.by.clone(),
                at: u.at,
                signed: t.signed_by(&u.key),
            })
            .collect();
        users.sort_by(|a, b| a.name.cmp(&b.name));
        let member = t.view.members.contains_key(&me);
        let node = t
            .view
            .members
            .get(&me)
            .map(|a| a.node.clone())
            .unwrap_or_else(|| inner.cfg.name.clone());
        let users_exist = t.users_exist();
        let next_clock = t.next_clock();
        let cluster = t.cluster;
        drop(t);
        MembershipView {
            elders: match users_exist || !member {
                true => Vec::new(),
                false => self.admitted_before(&node),
            },
            node,
            key: me,
            cluster,
            member,
            next_clock,
            users_exist,
            members,
            users,
            removed,
        }
    }

    /// The first user: `genesis`, the admission of its own key the command line signed, naming
    /// this machine, which co-signs it here; with `admissions`, a v3 admission that user signed
    /// of each member (as [`Mesh::membership`] lists them). Taken and announced together, so no
    /// member drops out: from then on only users' records admit and remove machines. Refused
    /// when the cluster has users already, or the admissions aren't of the members as they are.
    pub fn user_init(
        &self,
        mut genesis: UserAdmission,
        admissions: Vec<Admission>,
    ) -> Result<(), MeshError> {
        let inner = &self.inner;
        if inner.dial_only {
            return Err(MeshError::Invalid("a first user needs the daemon".into()));
        }
        let id = inner.identity();
        let me = id.public();
        let (records, changes) = {
            let mut t = lock(&inner.trust);
            let my_name = standing(&t, &me)?;
            if t.users_exist() {
                let names: Vec<&str> = t.view.users.values().map(|u| u.name.as_str()).collect();
                return Err(MeshError::Invalid(format!(
                    "this cluster has users already ({}); `potluck user add` adds another",
                    names.join(", ")
                )));
            }
            let ours = genesis
                .machine
                .as_ref()
                .is_some_and(|m| m.node == my_name && m.key == me);
            if !genesis.is_genesis() || !ours {
                return Err(MeshError::Invalid(
                    "not a first user's admission for this machine to co-sign".into(),
                ));
            }
            if !valid_user_name(&genesis.name) {
                return Err(MeshError::Invalid(format!(
                    "bad user name {:?}: use letters, digits, '.', '_' and '-', not starting with '-'",
                    genesis.name
                )));
            }
            if !genesis.by_key.verify(&genesis.signed_bytes(), &genesis.sig) {
                return Err(MeshError::Invalid(
                    "the first user's signature is not good".into(),
                ));
            }
            if let Some(m) = machine_named(&t, &genesis.key) {
                return Err(MeshError::Invalid(format!(
                    "that key is {m}'s machine key, which a daemon holds; a user's key is one `potluck user key` makes"
                )));
            }
            genesis.co_sign(&id).map_err(MeshError::Invalid)?;
            let members: HashMap<PublicKey, String> = t
                .view
                .members
                .values()
                .map(|a| (a.key, a.node.clone()))
                .collect();
            let mut covered = HashSet::new();
            for a in &admissions {
                if !a.by_user() || a.by_key != genesis.key || !a.verify() {
                    return Err(MeshError::Invalid(format!(
                        "the admission of {} is not one the first user signed",
                        a.node
                    )));
                }
                if members.get(&a.key) != Some(&a.node) || !covered.insert(a.key) {
                    return Err(MeshError::Invalid(format!(
                        "the members changed since they were read ({} is not a member as admitted); run `potluck user init` again",
                        a.node
                    )));
                }
            }
            if let Some((_, name)) = members.iter().find(|(k, _)| !covered.contains(*k)) {
                return Err(MeshError::Invalid(format!(
                    "the members changed since they were read ({name} has no admission); run `potluck user init` again"
                )));
            }
            let keys: Vec<PublicKey> = members.keys().copied().collect();
            let mut batch = vec![Record::User(genesis.clone())];
            batch.extend(admissions.into_iter().map(Record::Admission));
            let before = t.clone();
            let changes = t.merge(batch.clone());
            let all_in = keys.iter().all(|k| t.view.members.contains_key(k));
            if t.view.genesis != Some(genesis.id()) || !all_in {
                let why = match t.view.genesis {
                    Some(g) if g != genesis.id() => {
                        "another first user's admission counts instead".to_string()
                    }
                    Some(_) => keys
                        .iter()
                        .find(|k| !t.view.members.contains_key(*k))
                        .map(|k| {
                            format!(
                                "{} would not be a member: {}",
                                members[k],
                                t.why_not_member(k)
                            )
                        })
                        .unwrap_or_default(),
                    None => "the admission does not count".into(),
                };
                *t = before;
                return Err(MeshError::Invalid(format!(
                    "the first user did not take: {why}; nothing was changed"
                )));
            }
            save_or_undo(&mut t, before)?;
            (batch, changes)
        };
        inner.apply_changes(changes, None);
        let targets = inner.targets_except(&[me]);
        inner.announce(records, targets);
        Ok(())
    }

    /// A user's admission or removal a user signed (`potluck user add`, `user remove`): taken if
    /// it counts, and announced. Refused when the cluster has no users, when it doesn't count
    /// (its signer is no user here, a name is taken), and for the removal of the last user.
    pub fn user_record(&self, record: Record) -> Result<(), MeshError> {
        let inner = &self.inner;
        let me = inner.key();
        let changes = {
            let mut t = lock(&inner.trust);
            standing(&t, &me)?;
            if !t.users_exist() {
                return Err(MeshError::Invalid(
                    "this cluster has no users yet: `potluck user init` makes the first".into(),
                ));
            }
            if !record.verify() {
                return Err(MeshError::Invalid(
                    "the record's signature is not good".into(),
                ));
            }
            let id = record.id();
            let before = t.clone();
            let changes = match &record {
                Record::User(u) if !u.is_genesis() => {
                    if !valid_user_name(&u.name) {
                        return Err(MeshError::Invalid(format!(
                            "bad user name {:?}: use letters, digits, '.', '_' and '-', not starting with '-'",
                            u.name
                        )));
                    }
                    if let Some(other) = t.view.users.values().find(|o| o.name == u.name)
                        && other.key != u.key
                    {
                        return Err(MeshError::Invalid(format!(
                            "there is a user named {} already",
                            u.name
                        )));
                    }
                    if let Some(m) = machine_named(&t, &u.key) {
                        return Err(MeshError::Invalid(format!(
                            "that key is {m}'s machine key, which a daemon holds; a user's key is one `potluck user key` prints"
                        )));
                    }
                    if t.view.users_removed.contains_key(&u.key) {
                        return Err(MeshError::Invalid(format!(
                            "that key was removed as user {}; a removed key is never a user again",
                            t.view.users_removed[&u.key].name
                        )));
                    }
                    let changes = t.merge(vec![record.clone()]);
                    if !t.view.valid.contains(&id) || !t.view.users.contains_key(&u.key) {
                        let signer = t.recognized_user(&u.by_key);
                        *t = before;
                        return Err(MeshError::Invalid(match signer {
                            false => format!(
                                "the admission of user {} did not take: {} is no user here",
                                u.name, u.by
                            ),
                            true => format!("the admission of user {} did not take", u.name),
                        }));
                    }
                    changes
                }
                Record::UserRemoval(r) => {
                    let changes = t.merge(vec![record.clone()]);
                    if !t.view.valid.contains(&id) || t.view.users.contains_key(&r.key) {
                        let signer = t.recognized_user(&r.by_key);
                        *t = before;
                        return Err(MeshError::Invalid(match signer {
                            false => format!(
                                "the removal of user {} did not take: {} is no user here",
                                r.name, r.by
                            ),
                            true => format!("the removal of user {} did not take", r.name),
                        }));
                    }
                    if t.view.users.is_empty() {
                        *t = before;
                        return Err(MeshError::Invalid(format!(
                            "{} is the last user, and a cluster with users keeps one",
                            r.name
                        )));
                    }
                    changes
                }
                _ => {
                    return Err(MeshError::Invalid(
                        "not a user's admission or removal".into(),
                    ));
                }
            };
            save_or_undo(&mut t, before)?;
            changes
        };
        inner.apply_changes(changes, None);
        let targets = inner.targets_except(&[me]);
        inner.announce(vec![record], targets);
        Ok(())
    }
}

/// The name of the machine whose key `key` is, if a record admits it as a machine.
fn machine_named(t: &Trust, key: &PublicKey) -> Option<NodeName> {
    t.records().iter().find_map(|r| match r {
        Record::Admission(a) if a.key == *key => Some(a.node.clone()),
        _ => None,
    })
}

/// User names: as node names ([`trust::valid_name`]).
pub fn valid_user_name(name: &str) -> bool {
    trust::valid_name(name)
}

impl Inner {
    fn new(
        cfg: MeshConfig,
        address: String,
        local_addr: Option<SocketAddr>,
        handler: Arc<dyn Handler>,
        identity: Identity,
        trust: Trust,
    ) -> Inner {
        Inner {
            cfg,
            address,
            local_addr,
            dial_only: false,
            handler,
            identity: RwLock::new(Arc::new(identity)),
            trust: Arc::new(Mutex::new(trust)),
            peers: Mutex::new(HashMap::new()),
            pool: Mutex::new(HashMap::new()),
            open: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            subscribers: Mutex::new(Subscribers {
                senders: Vec::new(),
                early: Some(VecDeque::new()),
            }),
            stopping: AtomicBool::new(false),
            wake: (Mutex::new(false), Condvar::new()),
            threads: Mutex::new(Vec::new()),
            pinging: Mutex::new(HashSet::new()),
            catchup: Mutex::new(CatchUp {
                since: Instant::now(),
                compared: HashSet::new(),
                last_contact: Some(crate::daemon::clock::boot_ms()),
            }),
            preauth: Mutex::new(PreAuth::default()),
            dials: Mutex::new(HashMap::new()),
            dials_cv: Condvar::new(),
            in_vain: Mutex::new(HashMap::new()),
            removed_taken: Mutex::new(HashMap::new()),
            unsaved: AtomicBool::new(false),
            former_pinged: Mutex::new(None),
            accepting: AtomicBool::new(true),
            accept_wake: std::os::unix::net::UnixStream::pair().ok(),
            kept: Mutex::new(None),
            refused_log: Mutex::new(RefusedLog {
                window: Instant::now(),
                sent: 0,
                suppressed: 0,
            }),
            #[cfg(test)]
            exchanges: AtomicU64::new(0),
        }
    }

    /// Wakes the accept loop to look at the flags.
    fn wake_accept(&self) {
        if let Some((_, w)) = &self.accept_wake {
            let _ = (&*w).write(&[1]);
        }
    }

    /// The members as this mesh sees them, for the next one ([`Mesh::hand_over`]).
    fn seen(&self) -> Vec<Seen> {
        let now = Instant::now();
        let ago = |t: Instant| now.saturating_duration_since(t).as_millis() as u64;
        let names: HashMap<PublicKey, NodeName> = lock(&self.trust)
            .view
            .members
            .values()
            .map(|a| (a.key, a.node.clone()))
            .collect();
        lock(&self.peers)
            .iter()
            .map(|(k, p)| Seen {
                key: *k,
                name: names.get(k).cloned().unwrap_or_default(),
                known_ago_ms: ago(p.known_since),
                up: p.up,
                seen_ago_ms: p.last_seen.map(ago),
                last_seen_ms: p.last_seen_ms,
                version: p.version.clone(),
                mismatch: p.mismatch.clone(),
                mismatch_ago_ms: p.mismatch_since.map(ago),
            })
            .collect()
    }

    /// Takes the members as an earlier mesh saw them ([`Inner::seen`]).
    fn see(&mut self, seen: Vec<Seen>) {
        let now = Instant::now();
        let at = |ago: u64| now.checked_sub(Duration::from_millis(ago)).unwrap_or(now);
        let peers = self.peers.get_mut().unwrap_or_else(|e| e.into_inner());
        for s in seen {
            peers.insert(
                s.key,
                PeerState {
                    known_since: at(s.known_ago_ms),
                    up: s.up,
                    last_seen: s.seen_ago_ms.map(at),
                    last_seen_ms: s.last_seen_ms,
                    version: s.version,
                    mismatch: s.mismatch,
                    mismatch_since: s.mismatch_ago_ms.map(at),
                    // Not handed over: the next heartbeat says it again.
                    last_error: None,
                    unreachable_said: None,
                    reached_us: None,
                    unreachable_at: None,
                    one_way: 0,
                },
            );
        }
    }

    /// Counts a record exchange this machine starts (for tests).
    fn count_exchange(&self) {
        #[cfg(test)]
        self.exchanges.fetch_add(1, Ordering::SeqCst);
    }

    fn identity(&self) -> Arc<Identity> {
        self.identity
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn key(&self) -> PublicKey {
        self.identity().public()
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn my_name(&self) -> NodeName {
        let me = self.key();
        lock(&self.trust)
            .view
            .members
            .get(&me)
            .map(|a| a.node.clone())
            .unwrap_or_else(|| self.cfg.name.clone())
    }

    fn emit(&self, e: Event) {
        let mut s = lock(&self.subscribers);
        if let Some(early) = &mut s.early {
            if early.len() == EARLY_EVENTS {
                early.pop_front();
            }
            early.push_back(e);
            return;
        }
        s.senders.retain(|tx| tx.send(e.clone()).is_ok());
    }

    /// A `Refused` event, at most [`REFUSED_PER_WINDOW`] per [`REFUSED_WINDOW`]: anyone who can
    /// reach the port can cause them.
    fn refused_event(&self, address: String, mut reason: String) {
        if self.stopping() {
            return;
        }
        {
            let mut log = lock(&self.refused_log);
            if log.window.elapsed() >= REFUSED_WINDOW {
                if log.suppressed > 0 {
                    reason = format!(
                        "{reason} ({} more refused before this, not reported)",
                        log.suppressed
                    );
                }
                *log = RefusedLog {
                    window: Instant::now(),
                    sent: 0,
                    suppressed: 0,
                };
            }
            if log.sent >= REFUSED_PER_WINDOW {
                log.suppressed += 1;
                return;
            }
            log.sent += 1;
        }
        self.emit(Event::Refused { address, reason });
    }

    /// Makes the heartbeat run now.
    fn kick(&self) {
        let (m, cv) = &self.wake;
        *lock(m) = true;
        cv.notify_all();
    }

    /// Sleeps for `d` or until kicked. Returns whether the mesh is stopping.
    fn sleep(&self, d: Duration) -> bool {
        let (m, cv) = &self.wake;
        let g = lock(m);
        let (mut g, _) = cv
            .wait_timeout_while(g, d, |kicked| !*kicked)
            .unwrap_or_else(|e| e.into_inner());
        *g = false;
        self.stopping()
    }

    fn hello(&self) -> Hello {
        let t = lock(&self.trust);
        self.hello_with(&t)
    }

    fn hello_with(&self, t: &Trust) -> Hello {
        let key = self.key();
        Hello {
            cluster: t.cluster,
            node: t
                .view
                .members
                .get(&key)
                .map(|a| a.node.clone())
                .unwrap_or_else(|| self.cfg.name.clone()),
            version: self.cfg.version.clone(),
            key,
            address: if self.local_addr.is_some() {
                self.address.clone()
            } else {
                String::new()
            },
            digest: t.digest(),
        }
    }

    fn register(self: &Arc<Self>, key: Option<PublicKey>, sock: &TcpStream) -> Registration {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        if let Ok(clone) = sock.try_clone() {
            let conn = OpenConn {
                key,
                sock: clone,
                hello_by: Some(Instant::now() + HANDSHAKE_TIMEOUT),
            };
            lock(&self.open).insert(id, conn);
        }
        if self.stopping() {
            let _ = sock.shutdown(Shutdown::Both);
        }
        Registration {
            id,
            inner: Arc::downgrade(self),
        }
    }

    fn set_registered_key(&self, reg: &Registration, key: PublicKey) {
        if let Some(conn) = lock(&self.open).get_mut(&reg.id) {
            conn.key = Some(key);
        }
    }

    /// Closes every connection with `key`.
    fn cut(&self, key: &PublicKey) {
        lock(&self.pool).remove(key);
        for conn in lock(&self.open).values() {
            if conn.key.as_ref() == Some(key) {
                let _ = conn.sock.shutdown(Shutdown::Both);
            }
        }
    }

    fn cut_all(&self) {
        lock(&self.pool).clear();
        for conn in lock(&self.open).values() {
            let _ = conn.sock.shutdown(Shutdown::Both);
        }
    }

    /// Marks a connection's handshake and hello as done.
    fn hello_done(&self, reg: &Registration) {
        if let Some(conn) = lock(&self.open).get_mut(&reg.id) {
            conn.hello_by = None;
        }
    }

    /// Closes connections still in their handshake or hello past [`HANDSHAKE_TIMEOUT`].
    fn cut_slow_hellos(&self) {
        let now = Instant::now();
        for conn in lock(&self.open).values() {
            if conn.hello_by.is_some_and(|t| t <= now) {
                let _ = conn.sock.shutdown(Shutdown::Both);
            }
        }
    }

    fn is_member(&self, key: &PublicKey) -> bool {
        let me = self.key();
        let t = lock(&self.trust);
        t.view.members.contains_key(key) && t.view.members.contains_key(&me)
    }

    fn target(&self, node: &str) -> Result<Target, MeshError> {
        if self.stopping() {
            return Err(MeshError::Stopped);
        }
        let me = self.key();
        let t = lock(&self.trust);
        standing(&t, &me)?;
        let a = t
            .view
            .by_name(node)
            .ok_or_else(|| MeshError::NotMember(node.to_string()))?;
        Ok(Target {
            name: a.node.clone(),
            key: a.key,
            address: t.address_of(&a.key).unwrap_or_else(|| a.address.clone()),
        })
    }

    /// Every member but the given keys, as dial targets. None if this machine is not a member.
    fn targets_except(&self, skip: &[PublicKey]) -> Vec<Target> {
        let me = self.key();
        let t = lock(&self.trust);
        if !t.view.members.contains_key(&me) {
            return Vec::new();
        }
        t.view
            .members
            .values()
            .filter(|a| a.key != me && !skip.contains(&a.key))
            .map(|a| Target {
                name: a.node.clone(),
                key: a.key,
                address: t.address_of(&a.key).unwrap_or_else(|| a.address.clone()),
            })
            .collect()
    }

    /// While this machine is out of its cluster (removed, or its admission void), the members
    /// it knows, as dial targets, once per down time; none while it is a member. Its removal may
    /// be void where they are: a removal of its remover counts there, signed before that member
    /// heard of this one. Meanwhile it refuses every connection, so only its own pings can bring
    /// it the records that say so; a member that knows of a removal that counts refuses it, with
    /// the proof.
    fn former_members(&self) -> Vec<Target> {
        let me = self.key();
        let t = lock(&self.trust);
        if t.view.members.contains_key(&me) {
            return Vec::new();
        }
        {
            let mut at = lock(&self.former_pinged);
            if at.is_some_and(|at| at.elapsed() < self.cfg.down_after) {
                return Vec::new();
            }
            *at = Some(Instant::now());
        }
        t.view
            .members
            .values()
            .filter(|a| a.key != me)
            .map(|a| Target {
                name: a.node.clone(),
                key: a.key,
                address: t.address_of(&a.key).unwrap_or_else(|| a.address.clone()),
            })
            .collect()
    }

    fn members(&self) -> Vec<Member> {
        let me = self.key();
        let t = lock(&self.trust);
        let peers = lock(&self.peers);
        t.view
            .members
            .values()
            .map(|a| {
                let this = a.key == me;
                let p = peers.get(&a.key);
                Member {
                    name: a.node.clone(),
                    key: a.key,
                    address: t.address_of(&a.key).unwrap_or_else(|| a.address.clone()),
                    admitted_by: a.by.clone(),
                    admitted_at: a.at,
                    pool: a.pool_name().to_string(),
                    this_machine: this,
                    alive: this || p.is_some_and(|p| p.up == Some(true)),
                    last_seen: if this {
                        Some(now_ms())
                    } else {
                        p.and_then(|p| p.last_seen_ms)
                    },
                    version: if this {
                        Some(self.cfg.version.clone())
                    } else {
                        p.and_then(|p| p.version.clone())
                    },
                }
            })
            .collect()
    }

    /// Notes that a member was heard from.
    fn mark_seen(&self, key: PublicKey, name: &str, version: Option<&str>) {
        if key == self.key() || !self.is_member(&key) {
            return;
        }
        self.contact();
        let mut peers = lock(&self.peers);
        let p = peers.entry(key).or_insert_with(PeerState::new);
        p.last_seen = Some(Instant::now());
        p.last_seen_ms = Some(now_ms());
        if let Some(v) = version {
            p.version = Some(v.to_string());
            p.mismatch = None;
            p.mismatch_since = None;
        }
        if p.up != Some(true) {
            p.up = Some(true);
            drop(peers);
            self.emit(Event::Up {
                node: name.to_string(),
            });
        }
    }

    /// A member was heard from: after hearing from none for the down time, this machine is back,
    /// and compares its records with the members again before it deals with them.
    fn contact(&self) {
        let mut cu = lock(&self.catchup);
        let now = crate::daemon::clock::boot_ms();
        if cu.silent(self.cfg.down_after, now) {
            cu.since = Instant::now();
            cu.compared.clear();
        }
        cu.last_contact = Some(now);
    }

    /// This machine's records and member `key`'s are the same, or the two just exchanged them.
    fn compared(&self, key: PublicKey) {
        lock(&self.catchup).compared.insert(key);
    }

    /// Whether this machine's records (digest `mine`) and member `key`'s (`theirs`) are as they
    /// were at an exchange that changed neither: another would change nothing either.
    fn exchanged_in_vain(&self, key: &PublicKey, mine: &str, theirs: &str) -> bool {
        lock(&self.in_vain)
            .get(key)
            .is_some_and(|(m, t)| m == mine && t == theirs)
    }

    /// Takes the records member `key` answered an exchange with: `mine` is the digest of the
    /// records this machine sent, `theirs` the member's digest before the exchange.
    fn exchanged(&self, key: PublicKey, mine: &str, theirs: &str, records: Vec<Record>) {
        let cluster = lock(&self.trust).cluster;
        let theirs_now = trust::digest_of(&cluster, &records);
        self.merge(records);
        let mine_now = lock(&self.trust).digest();
        let mut in_vain = lock(&self.in_vain);
        if mine_now == mine && theirs_now == theirs {
            in_vain.insert(key, (mine_now, theirs_now));
        } else {
            in_vain.remove(&key);
        }
    }

    /// A member answered on another version. For [`MeshConfig::updating_for`] from its first
    /// such answer, that counts as hearing from it (one of the two is updating); after that it
    /// doesn't, and the member goes down once silent otherwise for the down time.
    ///
    /// The version is reported only for a machine the records admit (a member, or while this
    /// machine is out of the cluster, a former member it follows), not for one removed: the
    /// remover dials that one to tell it, and the daemon updates to a newer version reported.
    fn note_mismatch(&self, key: PublicKey, name: &str, theirs: &str) {
        let member = key != self.key() && self.is_member(&key);
        let admitted = key != self.key() && lock(&self.trust).view.members.contains_key(&key);
        let mut peers = lock(&self.peers);
        let p = peers.entry(key).or_insert_with(PeerState::new);
        p.version = Some(theirs.to_string());
        let since = *p.mismatch_since.get_or_insert_with(Instant::now);
        let mut events = Vec::new();
        let updating = member && since.elapsed() < self.cfg.updating_for;
        if updating {
            p.last_seen = Some(Instant::now());
            p.last_seen_ms = Some(now_ms());
            if p.up != Some(true) {
                p.up = Some(true);
                events.push(Event::Up {
                    node: name.to_string(),
                });
            }
        }
        if admitted && p.mismatch.as_deref() != Some(theirs) {
            p.mismatch = Some(theirs.to_string());
            events.push(Event::VersionMismatch {
                node: name.to_string(),
                ours: self.cfg.version.clone(),
                theirs: theirs.to_string(),
            });
        }
        drop(peers);
        if updating {
            self.contact();
        }
        for e in events {
            self.emit(e);
        }
    }

    /// After a hello from a member: its address and liveness.
    fn on_hello(&self, key: PublicKey, hello: &Hello) {
        self.note_address(key, hello);
        self.mark_seen(key, &hello.node, Some(&hello.version));
    }

    /// Notes that member `key` reached this machine: a connection or a request from it.
    fn reached_us(&self, key: PublicKey) {
        if let Some(p) = lock(&self.peers).get_mut(&key) {
            p.reached_us = Some(Instant::now());
        }
    }

    /// The address a member gave in its hello (also when its version differs: the newer side
    /// must still be able to reach the older one).
    fn note_address(&self, key: PublicKey, hello: &Hello) {
        if hello.address.is_empty() {
            return;
        }
        let mut t = lock(&self.trust);
        if t.view.members.contains_key(&key) && t.set_address(key, &hello.address) {
            self.save_or_retry(&t);
        }
    }

    /// Merges records from a member and acts on what changed. Their ids and signatures are
    /// checked without the trust store's lock: a member sending records in bulk (64 MiB of junk
    /// is several seconds of signatures) would otherwise hold up every handshake, hello and
    /// request meanwhile.
    fn merge(&self, records: Vec<Record>) {
        self.merge_reinstating(records, None);
    }

    /// [`Inner::merge`], or with `reinstated`, only if with the records that key is a member
    /// again: what a removed machine sends before it is refused ([`Inner::take_from_removed`])
    /// is taken only if it voids its removal (a removal of its remover by a member ranked before
    /// it, which this machine hadn't heard of), so it can't make this machine hold anything else.
    fn merge_reinstating(&self, records: Vec<Record>, reinstated: Option<PublicKey>) {
        let me = self.key();
        let incoming = trust::with_ids(records);
        let screened = lock(&self.trust).screen(incoming);
        if screened.is_empty() {
            return;
        }
        let checked = trust::Checked::new(screened);
        let (changes, self_change) = {
            let mut t = lock(&self.trust);
            let was_member = t.view.members.contains_key(&me);
            let changes = match reinstated {
                None => t.merge_checked(checked),
                Some(key) => {
                    let mut trial = t.clone();
                    let changes = trial.merge_checked(checked);
                    if !trial.view.members.contains_key(&key) {
                        return;
                    }
                    *t = trial;
                    changes
                }
            };
            if changes.stored {
                self.save_or_retry(&t);
            }
            if changes.is_empty() {
                return;
            }
            let self_change = if was_member && !t.view.members.contains_key(&me) {
                Some(match t.view.removed.get(&me) {
                    Some(r) => SelfChange::Removed(r.clone()),
                    None => SelfChange::Dropped(t.why_not_member(&me)),
                })
            } else {
                None
            };
            (changes, self_change)
        };
        self.apply_changes(changes, self_change);
    }

    fn apply_changes(&self, changes: Changes, self_change: Option<SelfChange>) {
        self.apply_changes_removing(changes, self_change, None);
    }

    /// [`Inner::apply_changes`] after this machine removed `removed` ([`Mesh::remove`]).
    fn apply_changes_removing(
        &self,
        changes: Changes,
        self_change: Option<SelfChange>,
        removed: Option<PublicKey>,
    ) {
        let me = self.key();
        for a in &changes.joined {
            if a.key != me {
                lock(&self.peers)
                    .entry(a.key)
                    .or_insert_with(PeerState::new);
                self.emit(Event::Joined {
                    node: a.node.clone(),
                    by: a.by.clone(),
                });
            }
        }
        for a in &changes.left {
            if a.key == me {
                continue;
            }
            let by = lock(&self.trust)
                .view
                .removed
                .get(&a.key)
                .map(|r| r.by.clone());
            self.cut(&a.key);
            lock(&self.peers).remove(&a.key);
            lock(&self.in_vain).remove(&a.key);
            self.emit(Event::Removed {
                node: a.node.clone(),
                by,
                here: removed == Some(a.key),
            });
        }
        if let Some(change) = self_change {
            self.cut_all();
            lock(&self.peers).clear();
            self.emit(match change {
                SelfChange::Removed(r) => Event::SelfRemoved { by: r.by },
                SelfChange::Dropped(reason) => Event::Dropped { reason },
            });
        }
    }

    /// Sends `records` to each target in the background, best effort: members that miss it get
    /// it from the next record exchange.
    fn announce(self: &Arc<Self>, records: Vec<Record>, targets: Vec<Target>) {
        let inner = self.clone();
        let _ = thread::Builder::new()
            .name("mesh-announce".into())
            .spawn(move || {
                thread::scope(|s| {
                    for t in &targets {
                        let inner = &inner;
                        let frame = Frame::Announce {
                            records: records.clone(),
                        };
                        s.spawn(move || {
                            let _ = inner.request(t, &frame, inner.cfg.call_timeout);
                        });
                    }
                });
            });
    }

    fn io_error(&self, t: &Target, e: io::Error) -> MeshError {
        if let Some(tls) = tls::tls_error(&e) {
            MeshError::Handshake {
                node: t.name.clone(),
                message: tls.to_string(),
            }
        } else if e.kind() == io::ErrorKind::TimedOut {
            MeshError::Timeout {
                node: t.name.clone(),
            }
        } else {
            MeshError::Unreachable {
                node: t.name.clone(),
                message: e.to_string(),
            }
        }
    }

    /// Connects to a member: TCP, the TLS handshake pinned to its key, and the hello.
    fn dial(
        self: &Arc<Self>,
        t: &Target,
        open: Open,
    ) -> Result<(Arc<Duplex>, Hello, Registration), MeshError> {
        if self.stopping() {
            return Err(MeshError::Stopped);
        }
        let unreachable = |message: String| MeshError::Unreachable {
            node: t.name.clone(),
            message,
        };
        let addrs: Vec<SocketAddr> = t
            .address
            .to_socket_addrs()
            .map_err(|e| unreachable(format!("{}: {e}", t.address)))?
            .collect();
        let mut sock = None;
        let mut last = format!("{}: no address", t.address);
        for a in addrs {
            match TcpStream::connect_timeout(&a, self.cfg.connect_timeout) {
                Ok(s) => {
                    sock = Some(s);
                    break;
                }
                Err(e) => last = format!("{}: {e}", t.address),
            }
        }
        let sock = sock.ok_or_else(|| unreachable(last))?;
        let _ = sock.set_nodelay(true);
        tls::keepalive(&sock);
        let _ = sock.set_read_timeout(Some(HANDSHAKE_TIMEOUT));
        let _ = sock.set_write_timeout(Some(HANDSHAKE_TIMEOUT));
        let reg = self.register(Some(t.key), &sock);
        let id = self.identity();
        let duplex = Arc::new(Duplex::connect(sock, &id, t.key).map_err(|e| self.io_error(t, e))?);
        write_line(
            &duplex,
            &ClientHello {
                hello: self.hello(),
                open,
            },
        )
        .map_err(|e| self.io_error(t, e))?;
        let read_answer = || -> Result<ServerHello, MeshError> {
            let line = read_line_raw(&duplex, MAX_SERVER_HELLO)
                .map_err(|e| self.io_error(t, e))?
                .ok_or_else(|| unreachable("closed during the hello".into()))?;
            serde_json::from_slice(&line)
                .map_err(|e| MeshError::Protocol(format!("hello from {}: {e}", t.name)))
        };
        let mut answer = read_answer()?;
        if matches!(answer, ServerHello::Records) {
            // It holds this machine removed: the records that count here may say otherwise.
            let records = lock(&self.trust).counting();
            let line = frame_line(&Frame::Sync { records })?;
            write_all(&duplex, &line).map_err(|e| self.io_error(t, e))?;
            answer = read_answer()?;
        }
        match answer {
            ServerHello::Ok(h) => {
                if h.key != t.key || h.node != t.name {
                    return Err(MeshError::Refused {
                        node: t.name.clone(),
                        reason: format!("it says it is {} with another key", h.node),
                    });
                }
                if !super::same_version(&h.version, &self.cfg.version) {
                    self.note_address(t.key, &h);
                    self.note_mismatch(t.key, &t.name, &h.version);
                    return Err(MeshError::VersionMismatch {
                        node: t.name.clone(),
                        ours: self.cfg.version.clone(),
                        theirs: h.version,
                    });
                }
                self.on_hello(t.key, &h);
                self.hello_done(&reg);
                Ok((duplex, h, reg))
            }
            ServerHello::Refused(r) => Err(self.refused(t, r)),
            ServerHello::Records => Err(MeshError::Protocol(format!(
                "{} asked for this machine's records twice",
                t.name
            ))),
        }
    }

    fn refused(&self, t: &Target, r: Refusal) -> MeshError {
        let node = t.name.clone();
        match r {
            Refusal::Version { hello } => {
                if hello.key == t.key {
                    self.note_address(t.key, &hello);
                }
                self.note_mismatch(t.key, &t.name, &hello.version);
                MeshError::VersionMismatch {
                    node,
                    ours: self.cfg.version.clone(),
                    theirs: hello.version,
                }
            }
            Refusal::Removed { removal } => {
                let me = self.key();
                let (by, subject) = (removal.by.clone(), removal.key);
                self.merge(vec![Record::Removal(removal)]);
                let removed = lock(&self.trust).view.removed.contains_key(&subject);
                if subject == me && removed {
                    MeshError::Removed { by }
                } else if subject == t.key && removed {
                    MeshError::NotMember(node)
                } else {
                    MeshError::Refused {
                        node,
                        reason: "it says a machine was removed, without proof".into(),
                    }
                }
            }
            Refusal::Cluster => MeshError::Refused {
                node,
                reason: "it belongs to another cluster".into(),
            },
            Refusal::NotMember { records } => {
                self.merge(records);
                let me = self.key();
                match standing(&lock(&self.trust), &me) {
                    Err(e) => e,
                    Ok(_) => MeshError::Refused {
                        node,
                        reason: "it does not know this machine (yet)".into(),
                    },
                }
            }
            Refusal::Dropped { why } => MeshError::Refused {
                node,
                reason: format!("it is no longer in the cluster: {why}"),
            },
            Refusal::BadHello { message } => MeshError::Refused {
                node,
                reason: message,
            },
        }
    }

    /// A new control connection; exchanges records first if the digests differ.
    fn open_control(self: &Arc<Self>, t: &Target) -> Result<Pooled, MeshError> {
        let (duplex, hello, reg) = self.dial(t, Open::Control)?;
        let _ = duplex.set_write_timeout(Some(self.cfg.call_timeout));
        let mut pc = Pooled {
            reader: BufReader::new(DuplexIo(duplex.clone())),
            duplex,
            _reg: reg,
            idle_since: Instant::now(),
        };
        let digest = lock(&self.trust).digest();
        if hello.digest == digest || self.exchanged_in_vain(&t.key, &digest, &hello.digest) {
            self.compared(t.key);
        } else {
            self.count_exchange();
            let (digest, records) = {
                let t = lock(&self.trust);
                (t.digest(), t.records().to_vec())
            };
            let line = frame_line(&Frame::Sync { records })?;
            match pc.roundtrip(&line, self.cfg.call_timeout) {
                Ok(Reply::Sync { records }) => {
                    self.exchanged(t.key, &digest, &hello.digest, records);
                    self.compared(t.key);
                }
                Ok(other) => {
                    return Err(MeshError::Protocol(format!("unexpected reply {other:?}")));
                }
                Err(RoundTrip::Stale) => {
                    return Err(MeshError::Unreachable {
                        node: t.name.clone(),
                        message: "connection closed".into(),
                    });
                }
                Err(RoundTrip::Failed(e)) => return Err(self.io_error(t, e)),
            }
            if !self.is_member(&t.key) {
                return Err(MeshError::NotMember(t.name.clone()));
            }
        }
        Ok(pc)
    }

    fn take_pooled(&self, key: &PublicKey) -> Option<Pooled> {
        let mut pool = lock(&self.pool);
        let list = pool.get_mut(key)?;
        while let Some(pc) = list.pop() {
            if pc.idle_since.elapsed() < POOL_IDLE {
                return Some(pc);
            }
        }
        None
    }

    fn put_pooled(&self, key: PublicKey, mut pc: Pooled) {
        if self.stopping() || !self.is_member(&key) {
            return;
        }
        pc.idle_since = Instant::now();
        {
            let mut pool = lock(&self.pool);
            let list = pool.entry(key).or_default();
            if list.len() < POOL_MAX {
                list.push(pc);
            }
        }
        self.dials_cv.notify_all();
    }

    fn try_dial_slot(&self, key: PublicKey) -> Option<DialSlot<'_>> {
        let mut dials = lock(&self.dials);
        let n = dials.entry(key).or_default();
        if *n >= MAX_DIALS_PER_PEER {
            return None;
        }
        *n += 1;
        Some(DialSlot { inner: self, key })
    }

    /// Waits a little for a dial slot or a pooled connection to come free.
    fn wait_dial(&self) -> Result<(), MeshError> {
        let dials = lock(&self.dials);
        let _ = self
            .dials_cv
            .wait_timeout(dials, Duration::from_millis(50))
            .unwrap_or_else(|e| e.into_inner());
        if self.stopping() {
            return Err(MeshError::Stopped);
        }
        Ok(())
    }

    /// Dials `t` once a dial slot is free.
    fn dial_limited(
        self: &Arc<Self>,
        t: &Target,
        open: Open,
    ) -> Result<(Arc<Duplex>, Hello, Registration), MeshError> {
        loop {
            if let Some(_slot) = self.try_dial_slot(t.key) {
                return self.dial(t, open);
            }
            self.wait_dial()?;
        }
    }

    /// One request and its reply on a pooled or new control connection.
    fn request(
        self: &Arc<Self>,
        t: &Target,
        frame: &Frame,
        timeout: Duration,
    ) -> Result<Reply, MeshError> {
        let line = frame_line(frame)?;
        let mut pc = loop {
            while let Some(mut pc) = self.take_pooled(&t.key) {
                match pc.roundtrip(&line, timeout) {
                    Ok(reply) => {
                        self.mark_seen(t.key, &t.name, None);
                        self.put_pooled(t.key, pc);
                        return Ok(reply);
                    }
                    Err(RoundTrip::Stale) => continue,
                    Err(RoundTrip::Failed(e)) => return Err(self.io_error(t, e)),
                }
            }
            if let Some(_slot) = self.try_dial_slot(t.key) {
                break self.open_control(t)?;
            }
            self.wait_dial()?;
        };
        match pc.roundtrip(&line, timeout) {
            Ok(reply) => {
                self.mark_seen(t.key, &t.name, None);
                self.put_pooled(t.key, pc);
                Ok(reply)
            }
            Err(RoundTrip::Stale) => Err(MeshError::Unreachable {
                node: t.name.clone(),
                message: "connection closed".into(),
            }),
            Err(RoundTrip::Failed(e)) => Err(self.io_error(t, e)),
        }
    }

    fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        // `stop` wakes this loop by connecting to its port, but that connection can land
        // elsewhere (another process listening on 127.0.0.1 and this port, beside this one on
        // every interface; or the address listened on gone): so the loop also waits for a
        // connection a while at most and looks at the flag again. Not blocking in `accept`
        // either: a connection reset between the wait and the accept would block it.
        let waits = listener.set_nonblocking(true).is_ok();
        let wake = self.accept_wake.as_ref().map(|(r, _)| r.as_raw_fd());
        loop {
            // Not accepting any more (a restart in place): connections wait in the backlog for
            // the next mesh, on this listener handed over.
            if self.stopping() || !self.accepting.load(Ordering::SeqCst) {
                break;
            }
            if waits && !incoming(&listener, wake, ACCEPT_WAIT) {
                continue;
            }
            if !self.accepting.load(Ordering::SeqCst) {
                break;
            }
            let sock = match listener.accept() {
                Ok((sock, _)) => sock,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                // Reset before it was taken: nothing to say.
                Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => continue,
                Err(e) => {
                    // Out of file descriptors, say: rate-limited like refused connections.
                    self.refused_event("an unknown address".into(), format!("accept failed: {e}"));
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
            };
            if self.stopping() {
                break;
            }
            // Accepted sockets take the listener's non-blocking flag on macOS.
            if sock.set_nonblocking(false).is_err() {
                continue;
            }
            let Ok(peer) = sock.peer_addr() else {
                continue;
            };
            let taken = lock(&self.preauth).try_take(
                peer.ip(),
                MAX_UNAUTHENTICATED,
                MAX_UNAUTHENTICATED_PER_IP,
            );
            if !taken {
                let _ = sock.shutdown(Shutdown::Both);
                drop(sock);
                self.refused_event(
                    peer.to_string(),
                    "too many connections not authenticated yet".into(),
                );
                continue;
            }
            let slot = PreAuthSlot {
                inner: Arc::downgrade(&self),
                ip: peer.ip(),
                held: true,
            };
            let inner = self.clone();
            let _ = thread::Builder::new()
                .name("mesh-conn".into())
                .spawn(move || inner.serve(sock, slot));
        }
    }

    /// One incoming connection: handshake, hello, then control requests or a stream.
    fn serve(self: Arc<Self>, sock: TcpStream, mut slot: PreAuthSlot) {
        let address = sock.peer_addr().map(|a| a.to_string()).unwrap_or_default();
        let _ = sock.set_nodelay(true);
        tls::keepalive(&sock);
        let _ = sock.set_read_timeout(Some(HANDSHAKE_TIMEOUT));
        let _ = sock.set_write_timeout(Some(HANDSHAKE_TIMEOUT));
        let reg = self.register(None, &sock);
        if self.stopping() {
            return;
        }
        let trust = self.trust.clone();
        let allow = Arc::new(move |k: &PublicKey| {
            let t = lock(&trust);
            // Members, and keys some record admits (removed or dropped ones): those are told
            // why they are refused, after the handshake.
            t.view.known.contains(k)
        });
        let refused = |reason: String| self.refused_event(address.clone(), reason);
        let cfg = match tls::server_config(&self.identity(), allow) {
            Ok(c) => c,
            Err(e) => return refused(format!("TLS setup: {e}")),
        };
        let duplex = match Duplex::accept(sock, cfg) {
            Ok(d) => Arc::new(d),
            Err(e) => return refused(format!("TLS handshake: {e}")),
        };
        let key = duplex.peer_key();
        self.set_registered_key(&reg, key);
        let ch: ClientHello = match read_line_raw(&duplex, MAX_HELLO) {
            Ok(Some(line)) => match serde_json::from_slice(&line) {
                Ok(h) => h,
                Err(e) => return refused(format!("bad hello: {e}")),
            },
            Ok(None) => return,
            Err(e) => return refused(format!("hello: {e}")),
        };
        let (mut answer, mut peer) = self.check_hello(&ch.hello, key);
        if self.take_from_removed(&ch.hello, key, &answer) {
            if write_line(&duplex, &ServerHello::Records).is_err() {
                return;
            }
            let records = match read_line_raw(&duplex, MAX_SERVER_HELLO) {
                Ok(Some(line)) => match serde_json::from_slice(&line) {
                    Ok(Frame::Sync { records }) => records,
                    _ => return refused("bad records from a removed machine".into()),
                },
                Ok(None) => return,
                Err(e) => return refused(format!("records from a removed machine: {e}")),
            };
            self.merge_reinstating(records, Some(key));
            (answer, peer) = self.check_hello(&ch.hello, key);
        }
        if write_line(&duplex, &answer).is_err() {
            return;
        }
        let Some(peer) = peer else {
            let _ = duplex.close_write();
            return;
        };
        self.hello_done(&reg);
        slot.release();
        self.on_hello(key, &ch.hello);
        self.reached_us(key);
        if ch.hello.digest == lock(&self.trust).digest() {
            self.compared(key);
        }
        match ch.open {
            Open::Control => {
                self.serve_control(&duplex, &peer);
                let _ = duplex.close_write();
            }
            Open::Stream { header } => {
                let _ = duplex.set_read_timeout(None);
                let _ = duplex.set_write_timeout(None);
                let stream = Stream::new(duplex, peer.clone(), reg);
                self.handler.stream(&peer, header, stream);
            }
        }
    }

    /// Whether to take the records of `key`, about to be refused as removed (`answer`), before
    /// refusing it: they may void its removal. A member can remove another while that one, not
    /// knowing, removes it; the removal by the one admitted first counts (see [`trust`]), but a
    /// member that heard of the other first refuses the winner as removed, and so may every
    /// member, and none would ever hear of the removal that counts. A removed machine can make
    /// this one check its records only when they differ from those it last offered, and once per
    /// down time: they are checked (their signatures too) but taken only if they void its
    /// removal ([`Inner::merge_reinstating`]).
    fn take_from_removed(&self, hello: &Hello, key: PublicKey, answer: &ServerHello) -> bool {
        let ServerHello::Refused(Refusal::Removed { removal }) = answer else {
            return false;
        };
        if removal.key != key
            || key == self.key()
            || !super::same_version(&hello.version, &self.cfg.version)
            || hello.digest == lock(&self.trust).digest()
        {
            return false;
        }
        let mut taken = lock(&self.removed_taken);
        if taken.get(&key).is_some_and(|(digest, at)| {
            *digest == hello.digest || at.elapsed() < self.cfg.down_after
        }) {
            return false;
        }
        taken.insert(key, (hello.digest.clone(), Instant::now()));
        true
    }

    fn check_hello(&self, hello: &Hello, key: PublicKey) -> (ServerHello, Option<Peer>) {
        let me = self.key();
        let t = lock(&self.trust);
        let refuse = |r: Refusal| (ServerHello::Refused(r), None);
        if let Some(r) = t.view.removed.get(&me) {
            return refuse(Refusal::Removed { removal: r.clone() });
        }
        if !t.view.members.contains_key(&me) {
            return refuse(Refusal::Dropped {
                why: t.why_not_member(&me),
            });
        }
        if hello.key != key {
            return refuse(Refusal::BadHello {
                message: "the hello names another key than the handshake".into(),
            });
        }
        if hello.cluster != t.cluster {
            return refuse(Refusal::Cluster);
        }
        if let Some(r) = t.view.removed.get(&key) {
            return refuse(Refusal::Removed { removal: r.clone() });
        }
        let Some(a) = t.view.members.get(&key) else {
            return refuse(Refusal::NotMember {
                records: t.counting(),
            });
        };
        if a.node != hello.node {
            return refuse(Refusal::BadHello {
                message: format!("that key was admitted as {}, not {}", a.node, hello.node),
            });
        }
        let pool = a.pool_name().to_string();
        let mine = self.hello_with(&t);
        drop(t);
        if !super::same_version(&hello.version, &self.cfg.version) {
            self.note_address(key, hello);
            self.note_mismatch(key, &hello.node, &hello.version);
            return refuse(Refusal::Version { hello: mine });
        }
        let peer = Peer {
            name: hello.node.clone(),
            key,
            address: hello.address.clone(),
            version: hello.version.clone(),
            pool,
        };
        (ServerHello::Ok(mine), Some(peer))
    }

    fn serve_control(&self, duplex: &Arc<Duplex>, peer: &Peer) {
        let _ = duplex.set_read_timeout(Some(IDLE_TIMEOUT));
        let _ = duplex.set_write_timeout(Some(self.cfg.call_timeout));
        let mut reader = BufReader::new(DuplexIo(duplex.clone()));
        loop {
            let mut buf = Vec::new();
            match (&mut reader).take(MAX_FRAME).read_until(b'\n', &mut buf) {
                Ok(n) if n > 0 && buf.last() == Some(&b'\n') => {}
                _ => return,
            }
            let Ok(frame) = serde_json::from_slice::<Frame>(&buf) else {
                return;
            };
            if self.stopping() || !self.is_member(&peer.key) {
                return;
            }
            self.mark_seen(peer.key, &peer.name, None);
            self.reached_us(peer.key);
            let reply = match frame {
                Frame::Call { body } => match self.handler.call(peer, body) {
                    Ok(body) => Reply::Ok { body },
                    Err(message) => Reply::Err { message },
                },
                Frame::Ping { digest } => {
                    let ours = lock(&self.trust).digest();
                    if digest == ours {
                        self.compared(peer.key);
                    }
                    Reply::Pong { digest: ours }
                }
                Frame::Sync { records } => {
                    self.merge(records);
                    self.compared(peer.key);
                    Reply::Sync {
                        records: lock(&self.trust).records().to_vec(),
                    }
                }
                Frame::Announce { records } => {
                    self.merge(records);
                    Reply::Done
                }
            };
            let Ok(line) = frame_line(&reply) else {
                return;
            };
            if write_all(duplex, &line).is_err() {
                return;
            }
        }
    }

    /// Saves the trust records; if that fails (a full disk), the heartbeat tries again until it
    /// works. What was merged stays in memory meanwhile, but a restart would forget it.
    fn save_or_retry(&self, t: &Trust) {
        self.unsaved.store(t.save().is_err(), Ordering::Relaxed);
    }

    fn heartbeat_loop(self: Arc<Self>) {
        loop {
            if self.sleep(self.cfg.heartbeat) {
                return;
            }
            if self.unsaved.load(Ordering::Relaxed) {
                self.save_or_retry(&lock(&self.trust));
            }
            self.cut_slow_hellos();
            let targets = self.targets_except(&[]);
            let former = self.former_members();
            let digest = lock(&self.trust).digest();
            let timeout = self.cfg.heartbeat.max(Duration::from_secs(2));
            // One ping per member, not waited for: a member that doesn't answer (its connect
            // timing out) doesn't hold up the others. A member still being pinged is skipped.
            for t in targets.iter().chain(&former) {
                if !lock(&self.pinging).insert(t.key) {
                    continue;
                }
                let key = t.key;
                let (inner, t, digest) = (self.clone(), t.clone(), digest.clone());
                let spawned = thread::Builder::new()
                    .name("mesh-ping".into())
                    .spawn(move || {
                        inner.ping(&t, &digest, timeout);
                        lock(&inner.pinging).remove(&t.key);
                    });
                if spawned.is_err() {
                    lock(&self.pinging).remove(&key);
                }
            }
            self.check_down(&targets);
        }
    }

    fn ping(self: &Arc<Self>, t: &Target, digest: &str, timeout: Duration) {
        let ping = Frame::Ping {
            digest: digest.to_string(),
        };
        let pong = self.request(t, &ping, timeout);
        // Members only: a former member pinged has no state to keep, nor gets one.
        let mut unreachable = None;
        if let Some(p) = lock(&self.peers).get_mut(&t.key) {
            p.last_error = pong.as_ref().err().map(|e| e.to_string());
            if matches!(pong, Err(MeshError::Unreachable { .. })) {
                let reached = p
                    .unreachable_at
                    .zip(p.reached_us)
                    .is_some_and(|(failed, reached)| reached > failed);
                p.one_way = if reached {
                    p.one_way.saturating_add(1)
                } else {
                    0
                };
                p.unreachable_at = Some(Instant::now());
            } else {
                p.unreachable_at = None;
                p.one_way = 0;
            }
            // It keeps reaching this machine, but this machine can't reach it: nothing else
            // shows that (on macOS, Local Network privacy can block the daemon's own
            // connections and nothing else). Not a member that stopped, up until the down time
            // (a request it sent as it stopped can come in after a heartbeat failed), nor one
            // that answers on another version, nor one back from sleep as a heartbeat to it
            // fails.
            if let Some(why) = &p.last_error
                && p.up == Some(true)
                && p.one_way >= 2
                && p.unreachable_said
                    .is_none_or(|at| at.elapsed() >= UNREACHABLE_EVERY)
            {
                p.unreachable_said = Some(Instant::now());
                unreachable = Some(why.clone());
            }
        }
        if let Some(why) = unreachable {
            self.emit(Event::Unreachable {
                node: t.name.clone(),
                why,
            });
        }
        let Ok(Reply::Pong { digest: theirs }) = pong else {
            return;
        };
        let mine = lock(&self.trust).digest();
        if theirs == digest || theirs == mine || self.exchanged_in_vain(&t.key, &mine, &theirs) {
            self.compared(t.key);
            return;
        }
        // Records differ: exchange them.
        self.count_exchange();
        let (mine, records) = {
            let t = lock(&self.trust);
            (t.digest(), t.records().to_vec())
        };
        if let Ok(Reply::Sync { records }) =
            self.request(t, &Frame::Sync { records }, self.cfg.call_timeout)
        {
            self.exchanged(t.key, &mine, &theirs, records);
            self.compared(t.key);
        }
    }

    fn check_down(&self, targets: &[Target]) {
        let now = Instant::now();
        let mut down = Vec::new();
        {
            let mut peers = lock(&self.peers);
            for t in targets {
                let p = peers.entry(t.key).or_insert_with(PeerState::new);
                let since = p.last_seen.unwrap_or(p.known_since);
                if now.duration_since(since) >= self.cfg.down_after && p.up != Some(false) {
                    p.up = Some(false);
                    down.push((t.name.clone(), p.last_error.clone()));
                }
            }
        }
        for (node, why) in down {
            self.emit(Event::Down { node, why });
        }
    }
}

/// A stream to or from a member: raw bytes both ways on a connection of its own.
///
/// One thread may read while another writes: use [`Stream::try_clone`] for the second handle
/// (to pipe a process's stdin and stdout through it, for example). [`Stream::close_write`] tells
/// the other side no more bytes are coming (it reads end-of-file) while reading goes on. The
/// connection closes when every handle is dropped.
pub struct Stream {
    duplex: Arc<Duplex>,
    peer: Peer,
    _reg: Arc<Registration>,
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stream({})", self.peer.name)
    }
}

impl Stream {
    fn new(duplex: Arc<Duplex>, peer: Peer, reg: Registration) -> Stream {
        Stream {
            duplex,
            peer,
            _reg: Arc::new(reg),
        }
    }

    /// The member on the other end.
    pub fn peer(&self) -> &Peer {
        &self.peer
    }

    /// Another handle on the same connection.
    pub fn try_clone(&self) -> io::Result<Stream> {
        Ok(Stream {
            duplex: self.duplex.clone(),
            peer: self.peer.clone(),
            _reg: self._reg.clone(),
        })
    }

    /// Ends this side's writing; the other side reads end-of-file. Reading goes on.
    pub fn close_write(&self) -> io::Result<()> {
        self.duplex.close_write()
    }

    pub fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.duplex.set_read_timeout(d)
    }

    pub fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.duplex.set_write_timeout(d)
    }

    /// Pipes a pair of byte streams through this stream until both directions end: `input` is
    /// sent (then this side's writing is closed) while what arrives is copied to `output`, which
    /// is dropped as soon as the other side closes (so a child process reading it sees the end).
    /// For example a process's stdout and stdin on one side, and stdin and stdout on the other.
    /// Returns the bytes sent and received.
    pub fn splice<R: Read + Send, W: Write>(
        self,
        mut input: R,
        output: W,
    ) -> io::Result<(u64, u64)> {
        let mut writer = self.try_clone()?;
        let mut reader = self;
        thread::scope(|s| {
            let up = s.spawn(move || -> io::Result<u64> {
                let n = io::copy(&mut input, &mut writer)?;
                writer.close_write()?;
                Ok(n)
            });
            let mut output = output;
            let down = io::copy(&mut reader, &mut output).and_then(|n| output.flush().map(|()| n));
            drop(output);
            let up = up
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("copy thread panicked")));
            Ok((up?, down?))
        })
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.duplex.read(buf)
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.duplex.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// This machine's name in its cluster, or why it is no longer in it.
/// Saves `t` after a change asked for here (an admission, a removal, joining); if that fails, puts
/// `before` back, so a change the user is told failed has not happened, nor is it announced.
fn save_or_undo(t: &mut Trust, before: Trust) -> Result<(), MeshError> {
    match t.save() {
        Ok(()) => Ok(()),
        Err(e) => {
            *t = before;
            Err(e)
        }
    }
}

fn standing(t: &Trust, me: &PublicKey) -> Result<NodeName, MeshError> {
    if let Some(r) = t.view.removed.get(me) {
        return Err(MeshError::Removed { by: r.by.clone() });
    }
    match t.view.members.get(me) {
        Some(a) => Ok(a.node.clone()),
        None => Err(MeshError::Dropped {
            reason: t.why_not_member(me),
        }),
    }
}

/// Closes the last copy of a listener that this process knows of (a stopped mesh's, one handed
/// over that is let go), so that its port can be bound again once this returns. Closing it is
/// not enough: a child process another thread is starting holds a copy of every descriptor until
/// it execs, and the socket listens on while a copy is open. Linux stops it listening, copies or
/// not, once it is shut down. macOS refuses to shut a listener down, so there this waits,
/// [`RELEASE_WAIT`] at most, until the address can be bound.
pub(crate) fn release(listener: TcpListener) {
    #[cfg(target_os = "linux")]
    // SAFETY: shutdown on the listener's own descriptor, open until the drop below.
    unsafe {
        libc::shutdown(listener.as_raw_fd(), libc::SHUT_RDWR);
    }
    #[cfg(not(target_os = "linux"))]
    let addr = listener.local_addr().ok();
    drop(listener);
    #[cfg(not(target_os = "linux"))]
    if let Some(addr) = addr {
        let until = Instant::now() + RELEASE_WAIT;
        while TcpListener::bind(addr).is_err() && Instant::now() < until {
            thread::sleep(Duration::from_millis(2));
        }
    }
}

/// How long [`release`] waits at most on macOS for a child process that holds a copy of the
/// listener to exec (a few milliseconds, more under load).
#[cfg(not(target_os = "linux"))]
const RELEASE_WAIT: Duration = Duration::from_secs(2);

/// Whether a connection waits on `listener`, waiting `wait` at most, or less once `wake` is
/// readable (also true on an error, so the caller's accept reports it).
fn incoming(listener: &TcpListener, wake: Option<std::os::fd::RawFd>, wait: Duration) -> bool {
    let mut fds = [
        libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: wake.unwrap_or(-1),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    // SAFETY: pollfds we own, for descriptors that stay open during the call (poll skips -1).
    let n = unsafe { libc::poll(fds.as_mut_ptr(), 2, wait.as_millis() as libc::c_int) };
    n < 0 || fds[0].revents != 0
}

fn frame_line<T: Serialize>(msg: &T) -> Result<Vec<u8>, MeshError> {
    let mut line = serde_json::to_vec(msg).map_err(|e| MeshError::Protocol(e.to_string()))?;
    line.push(b'\n');
    Ok(line)
}

fn write_all(d: &Duplex, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let n = d.write(buf)?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        buf = &buf[n..];
    }
    Ok(())
}

fn write_line<T: Serialize>(d: &Duplex, msg: &T) -> io::Result<()> {
    let mut line = serde_json::to_vec(msg)?;
    line.push(b'\n');
    write_all(d, &line)
}

/// Reads one line a byte at a time, so nothing after it is consumed (a stream's bytes may
/// follow). `None` at end of stream before any byte.
pub(super) fn read_line_raw(d: &Duplex, max: u64) -> io::Result<Option<Vec<u8>>> {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    loop {
        if d.read(&mut b)? == 0 {
            return if out.is_empty() {
                Ok(None)
            } else {
                Err(io::ErrorKind::UnexpectedEof.into())
            };
        }
        if b[0] == b'\n' {
            return Ok(Some(out));
        }
        out.push(b[0]);
        if out.len() as u64 > max {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "line too long"));
        }
    }
}

#[cfg(test)]
impl Mesh {
    /// Dials `address` expecting `key`, as if it were member `name`: for tests of refusals.
    pub(super) fn dial_raw(
        &self,
        name: &str,
        key: PublicKey,
        address: &str,
    ) -> Result<(), MeshError> {
        let t = Target {
            name: name.to_string(),
            key,
            address: address.to_string(),
        };
        self.inner.dial(&t, Open::Control).map(|_| ())
    }
}

#[cfg(test)]
mod catchup_tests {
    use super::*;

    /// Silence is judged on the boot clock alone: what the wall clock says (a step of it) plays no
    /// part, and time asleep counts.
    #[test]
    fn silence_is_measured_on_the_boot_clock() {
        let cu = CatchUp {
            since: Instant::now(),
            compared: HashSet::new(),
            last_contact: Some(1_000_000),
        };
        let down = Duration::from_secs(30);
        assert!(!cu.silent(down, 1_000_000));
        assert!(!cu.silent(down, 1_029_999));
        assert!(cu.silent(down, 1_030_000));
        // A reading before the contact (never on one boot clock) isn't silence.
        assert!(!cu.silent(down, 0));
        let never = CatchUp {
            last_contact: None,
            ..cu
        };
        assert!(!never.silent(down, u64::MAX));
    }
}
