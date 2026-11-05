//! TLS 1.3 with pinned ed25519 keys over blocking sockets, and a connection that can be read on
//! one thread while another writes (needed to pipe a process through a stream).
//!
//! No CA and no host names: the dialing side accepts only the key of the member it dials, the
//! listening side only keys its verifier approves (members, and removed members so they can be
//! told). Session resumption is off, so every connection runs the verifiers.

use super::identity::{Identity, PublicKey};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    CertificateError, ClientConfig, ClientConnection, Connection, DigitallySignedStruct,
    DistinguishedName, Error as TlsError, ServerConfig, ServerConnection, SignatureScheme,
};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn algorithms() -> WebPkiSupportedAlgorithms {
    rustls::crypto::ring::default_provider().signature_verification_algorithms
}

fn key_of(cert: &CertificateDer<'_>) -> Result<PublicKey, TlsError> {
    PublicKey::from_certificate(cert)
        .ok_or(TlsError::InvalidCertificate(CertificateError::BadEncoding))
}

/// The dialing side: accepts exactly one key.
#[derive(Debug)]
struct PinnedServer {
    expected: PublicKey,
    algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinnedServer {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        if key_of(end_entity)? == self.expected {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(TlsError::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Err(TlsError::General("TLS 1.2 is not supported".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

/// The listening side: accepts the keys `allow` approves.
struct PinnedClients {
    allow: Arc<dyn Fn(&PublicKey) -> bool + Send + Sync>,
    algs: WebPkiSupportedAlgorithms,
}

impl std::fmt::Debug for PinnedClients {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PinnedClients")
    }
}

impl ClientCertVerifier for PinnedClients {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, TlsError> {
        if (self.allow)(&key_of(end_entity)?) {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(TlsError::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Err(TlsError::General("TLS 1.2 is not supported".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

pub fn client_config(id: &Identity, expected: PublicKey) -> Result<Arc<ClientConfig>, TlsError> {
    let mut cfg = ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedServer {
            expected,
            algs: algorithms(),
        }))
        .with_client_auth_cert(vec![id.cert()], id.private_key())?;
    cfg.resumption = rustls::client::Resumption::disabled();
    cfg.enable_sni = false;
    Ok(Arc::new(cfg))
}

pub fn server_config(
    id: &Identity,
    allow: Arc<dyn Fn(&PublicKey) -> bool + Send + Sync>,
) -> Result<Arc<ServerConfig>, TlsError> {
    let mut cfg = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(Arc::new(PinnedClients {
            allow,
            algs: algorithms(),
        }))
        .with_single_cert(vec![id.cert()], id.private_key())?;
    cfg.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    cfg.send_tls13_tickets = 0;
    Ok(Arc::new(cfg))
}

/// The name the dialing side puts in its handshake. Never checked: keys are pinned.
fn server_name() -> ServerName<'static> {
    ServerName::try_from("potluck").expect("valid name")
}

/// Wraps a rustls error as an io error (kind InvalidData), so it can be found again.
fn tls_io(e: TlsError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// The TLS error inside an io error, if there is one.
pub fn tls_error(e: &io::Error) -> Option<&TlsError> {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<TlsError>())
}

/// An established TLS connection that one thread can read while another writes.
///
/// The TLS state sits behind a mutex that is never held while blocking on the socket: a reader
/// waits for bytes without it, then feeds them in; a writer encrypts under it and sends after.
#[derive(Debug)]
pub struct Duplex {
    sock: TcpStream,
    conn: Mutex<Connection>,
    /// Serializes socket writes, so records go out in the order they were made.
    wlock: Mutex<()>,
    /// Bytes read from the socket that rustls hasn't taken yet. Only the reader uses it.
    rx: Mutex<RxState>,
    peer: PublicKey,
    /// Buffers zeroed for socket reads.
    #[cfg(test)]
    buffers: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Default)]
struct RxState {
    pending: Vec<u8>,
    eof: bool,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Duplex {
    /// Dials: runs the handshake on `sock`, accepting only `expected`'s key.
    pub fn connect(sock: TcpStream, id: &Identity, expected: PublicKey) -> io::Result<Duplex> {
        let cfg = client_config(id, expected).map_err(tls_io)?;
        let conn = ClientConnection::new(cfg, server_name()).map_err(tls_io)?;
        Duplex::handshake(sock, Connection::Client(conn))
    }

    /// Answers: runs the handshake on an accepted socket.
    pub fn accept(sock: TcpStream, cfg: Arc<ServerConfig>) -> io::Result<Duplex> {
        let conn = ServerConnection::new(cfg).map_err(tls_io)?;
        Duplex::handshake(sock, Connection::Server(conn))
    }

    fn handshake(mut sock: TcpStream, mut conn: Connection) -> io::Result<Duplex> {
        while conn.is_handshaking() {
            conn.complete_io(&mut sock)?;
        }
        // A client sends its certificate in its last flight: push it out now, so a listener that
        // refuses the key does so here rather than at the first read.
        while conn.wants_write() {
            conn.complete_io(&mut sock)?;
        }
        let peer = conn
            .peer_certificates()
            .and_then(|c| c.first())
            .and_then(PublicKey::from_certificate)
            .ok_or_else(|| tls_io(TlsError::NoCertificatesPresented))?;
        Ok(Duplex {
            sock,
            conn: Mutex::new(conn),
            wlock: Mutex::new(()),
            rx: Mutex::new(RxState::default()),
            peer,
            #[cfg(test)]
            buffers: Default::default(),
        })
    }

    /// The peer's key, as the handshake proved it.
    pub fn peer_key(&self) -> PublicKey {
        self.peer
    }

    /// How many buffers its reads have zeroed for reading the socket into.
    #[cfg(test)]
    pub fn buffers(&self) -> u64 {
        self.buffers.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.sock.set_read_timeout(d)
    }

    pub fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.sock.set_write_timeout(d)
    }

    /// Reads plaintext. `Ok(0)` once the peer closed its side cleanly.
    pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut rx = lock(&self.rx);
        // Made only to read the socket: a read rustls answers from what it has (a line read a
        // byte at a time) zeroes nothing.
        let mut chunk = Vec::new();
        loop {
            {
                let mut c = lock(&self.conn);
                match c.reader().read(buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
                if !rx.pending.is_empty() || rx.eof {
                    let mut input: &[u8] = &rx.pending;
                    let before = input.len();
                    c.read_tls(&mut input)?;
                    let used = before - input.len();
                    if used == 0 && !rx.eof {
                        return Err(io::Error::other("TLS input stalled"));
                    }
                    rx.pending.drain(..used);
                    let processed = c.process_new_packets();
                    drop(c);
                    if let Err(e) = processed {
                        let _ = self.flush_tls(true);
                        return Err(tls_io(e));
                    }
                    self.flush_tls(false)?;
                    if rx.eof && rx.pending.is_empty() {
                        // rustls has seen the end; its reader now says how it ended.
                        let mut c = lock(&self.conn);
                        return match c.reader().read(buf) {
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "connection closed",
                            )),
                            other => other,
                        };
                    }
                    continue;
                }
            }
            if chunk.is_empty() {
                chunk = vec![0u8; 16 * 1024];
                #[cfg(test)]
                self.buffers
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            match (&self.sock).read(&mut chunk) {
                Ok(0) => rx.eof = true,
                Ok(n) => rx.pending.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Writes plaintext; it is encrypted and sent before this returns.
    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        let _w = lock(&self.wlock);
        let mut out = Vec::new();
        let n = {
            let mut c = lock(&self.conn);
            let n = c.writer().write(buf)?;
            while c.wants_write() {
                c.write_tls(&mut out)?;
            }
            n
        };
        write_sock(&self.sock, &out)?;
        Ok(n)
    }

    /// Sends anything rustls queued on its own (alerts, key updates). Without `wait`, gives up if
    /// a writer is busy: that writer sends it with its next write.
    fn flush_tls(&self, wait: bool) -> io::Result<()> {
        let _w = if wait {
            lock(&self.wlock)
        } else {
            match self.wlock.try_lock() {
                Ok(g) => g,
                Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
            }
        };
        let mut out = Vec::new();
        {
            let mut c = lock(&self.conn);
            while c.wants_write() {
                c.write_tls(&mut out)?;
            }
        }
        write_sock(&self.sock, &out)
    }

    /// Ends this side's writing: a TLS close_notify, then a TCP half-close. Reading goes on.
    pub fn close_write(&self) -> io::Result<()> {
        lock(&self.conn).send_close_notify();
        self.flush_tls(true)?;
        match self.sock.shutdown(Shutdown::Write) {
            Err(e) if e.kind() != io::ErrorKind::NotConnected => Err(e),
            _ => Ok(()),
        }
    }
}

fn write_sock(mut sock: &TcpStream, bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    sock.write_all(bytes).map_err(|e| {
        if e.kind() == io::ErrorKind::WouldBlock {
            io::Error::new(io::ErrorKind::TimedOut, "write timed out")
        } else {
            e
        }
    })
}

/// `Read` and `Write` on a shared [`Duplex`].
#[derive(Clone, Debug)]
pub struct DuplexIo(pub Arc<Duplex>);

impl Read for DuplexIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for DuplexIo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Turns on TCP keepalive, so a peer that vanished mid-stream is noticed (idle 30 s, then a
/// probe every 10 s, 3 probes).
pub fn keepalive(sock: &TcpStream) {
    use std::os::fd::AsRawFd;
    let fd = sock.as_raw_fd();
    let set = |level: libc::c_int, opt: libc::c_int, val: libc::c_int| {
        // SAFETY: setsockopt on a socket we own, with a pointer to a live c_int of the given size.
        unsafe {
            libc::setsockopt(
                fd,
                level,
                opt,
                &val as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    };
    set(libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1);
    #[cfg(target_os = "linux")]
    set(libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, 30);
    #[cfg(target_os = "macos")]
    set(libc::IPPROTO_TCP, libc::TCP_KEEPALIVE, 30);
    set(libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, 10);
    set(libc::IPPROTO_TCP, libc::TCP_KEEPCNT, 3);
}
