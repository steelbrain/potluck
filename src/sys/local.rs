//! The daemon's local socket: a Unix domain stream socket, `potluck.sock` in the state directory,
//! that the command line connects to.
//!
//! On Unix these are std's [`UnixListener`] and [`UnixStream`]. On Windows (10 1803 and later)
//! they are AF_UNIX stream sockets through Winsock, with the same calls the daemon and the
//! command line use: connect, bind, accept, read and write, read and write timeouts, `try_clone`,
//! `shutdown` and non-blocking mode. Who may connect is decided by the socket file's access list,
//! which it inherits from its directory: the private state directory. A socket's path is at most
//! [`MAX_PATH_BYTES`] bytes of UTF-8 there.
//!
//! [`AsRawSock`] names a socket for a wait on it, the same way on both.

#[cfg(unix)]
pub use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(windows)]
pub use win::{SocketAddr, UnixListener, UnixStream};

/// A socket as a wait takes it: its descriptor on Unix, its `SOCKET` on Windows.
#[cfg(unix)]
pub type RawSock = std::os::fd::RawFd;
/// A socket as a wait takes it: its descriptor on Unix, its `SOCKET` on Windows.
#[cfg(windows)]
pub type RawSock = windows_sys::Win32::Networking::WinSock::SOCKET;

/// A socket's [`RawSock`]: its descriptor on Unix, its `SOCKET` on Windows (std's TCP sockets
/// and this module's local sockets alike).
pub trait AsRawSock {
    fn as_raw_sock(&self) -> RawSock;
}

#[cfg(unix)]
impl<T: std::os::fd::AsRawFd> AsRawSock for T {
    fn as_raw_sock(&self) -> RawSock {
        self.as_raw_fd()
    }
}

#[cfg(windows)]
impl<T: std::os::windows::io::AsRawSocket> AsRawSock for T {
    fn as_raw_sock(&self) -> RawSock {
        self.as_raw_socket() as RawSock
    }
}

/// The longest socket path Windows takes, in bytes of UTF-8: `sockaddr_un`'s 108 with its
/// terminating NUL.
#[cfg(windows)]
pub const MAX_PATH_BYTES: usize = 107;

/// Whether a socket file is at `path` (one a daemon left behind, or one listening). On Windows a
/// socket file is a reparse point that `Path::exists` can't follow, so it is looked at itself.
pub fn socket_file_exists(path: &std::path::Path) -> bool {
    #[cfg(unix)]
    {
        path.exists()
    }
    #[cfg(windows)]
    {
        std::fs::symlink_metadata(path).is_ok()
    }
}

/// Waits until one of `socks` is readable (data, a connection to accept, the other side closed,
/// or an error), at most `timeout` (`None`: no limit). Returns which are; all false when the
/// time is up. An error from the wait itself is returned as every socket being readable, so the
/// caller's read or accept reports it.
#[cfg(windows)]
pub fn wait_readable(socks: &[RawSock], timeout: Option<std::time::Duration>) -> Vec<bool> {
    use windows_sys::Win32::Networking::WinSock::{POLLRDNORM, WSAPOLLFD, WSAPoll};
    let mut fds: Vec<WSAPOLLFD> = socks
        .iter()
        .map(|&fd| WSAPOLLFD {
            fd,
            events: POLLRDNORM,
            revents: 0,
        })
        .collect();
    let ms = match timeout {
        None => -1,
        // Rounded up: a wait of 0 would spin.
        Some(t) => t.as_millis().clamp(1, i32::MAX as u128) as i32,
    };
    // SAFETY: the array holds fds.len() entries, for sockets that stay open during the call.
    let n = unsafe { WSAPoll(fds.as_mut_ptr(), fds.len() as u32, ms) };
    if n < 0 {
        return vec![true; socks.len()];
    }
    fds.iter().map(|f| f.revents != 0).collect()
}

#[cfg(windows)]
mod win {
    use std::io::{self, Read, Write};
    use std::net::Shutdown;
    use std::os::windows::io::{AsRawSocket, FromRawSocket, IntoRawSocket, RawSocket};
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use windows_sys::Win32::Foundation::{HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation};
    use windows_sys::Win32::Networking::WinSock::{
        AF_UNIX, FIONBIO, FROM_PROTOCOL_INFO, INVALID_SOCKET, SD_BOTH, SD_RECEIVE, SD_SEND,
        SO_RCVTIMEO, SO_SNDTIMEO, SOCK_STREAM, SOCKADDR, SOCKADDR_UN, SOCKET, SOCKET_ERROR,
        SOL_SOCKET, WSA_FLAG_NO_HANDLE_INHERIT, WSA_FLAG_OVERLAPPED, WSADATA, WSADuplicateSocketW,
        WSAESHUTDOWN, WSAGetLastError, WSAPROTOCOL_INFOW, WSASocketW, WSAStartup, accept, bind,
        closesocket, connect, getsockopt, ioctlsocket, listen, recv, send, setsockopt, shutdown,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcessId;

    /// The backlog of a listening socket, as std gives a Unix one.
    const BACKLOG: i32 = 128;

    /// Winsock started for this process (once; never cleaned up).
    fn startup() -> io::Result<()> {
        static STARTED: OnceLock<i32> = OnceLock::new();
        let rc = *STARTED.get_or_init(|| {
            // SAFETY: plain data, filled by the call.
            let mut data: WSADATA = unsafe { std::mem::zeroed() };
            // SAFETY: version 2.2 and a buffer for what it says about itself.
            unsafe { WSAStartup(0x0202, &mut data) }
        });
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }

    /// The last Winsock error.
    fn last_error() -> io::Error {
        // SAFETY: plain call.
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    /// A socket, closed on drop.
    #[derive(Debug)]
    struct Socket(SOCKET);

    impl Drop for Socket {
        fn drop(&mut self) {
            // SAFETY: we own the socket.
            unsafe { closesocket(self.0) };
        }
    }

    impl Socket {
        /// A new AF_UNIX stream socket, not inherited by child processes.
        fn new() -> io::Result<Socket> {
            startup()?;
            // SAFETY: plain call; the socket returned is ours.
            let s = unsafe {
                WSASocketW(
                    i32::from(AF_UNIX),
                    SOCK_STREAM,
                    0,
                    std::ptr::null(),
                    0,
                    WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
                )
            };
            if s == INVALID_SOCKET {
                return Err(last_error());
            }
            Ok(Socket(s))
        }

        /// One accepted or otherwise handed to us, made non-inheritable.
        fn owned(s: SOCKET) -> Socket {
            // SAFETY: a socket is a handle; clearing its inherit flag only keeps it from child
            // processes. Best effort.
            unsafe { SetHandleInformation(s as HANDLE, HANDLE_FLAG_INHERIT, 0) };
            Socket(s)
        }

        /// Another socket for the same connection or listener, as std's `try_clone` makes one.
        fn duplicate(&self) -> io::Result<Socket> {
            // SAFETY: plain data, filled by the call.
            let mut info: WSAPROTOCOL_INFOW = unsafe { std::mem::zeroed() };
            // SAFETY: a socket we own, duplicated for this process; info receives its
            // description.
            if unsafe { WSADuplicateSocketW(self.0, GetCurrentProcessId(), &mut info) } != 0 {
                return Err(last_error());
            }
            // SAFETY: info describes the socket to duplicate; the new one is ours.
            let s = unsafe {
                WSASocketW(
                    FROM_PROTOCOL_INFO,
                    FROM_PROTOCOL_INFO,
                    FROM_PROTOCOL_INFO,
                    &info,
                    0,
                    WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
                )
            };
            if s == INVALID_SOCKET {
                return Err(last_error());
            }
            // WSASocketW ignores WSA_FLAG_NO_HANDLE_INHERIT for a socket made from another's
            // description: the copy would be inheritable ([`crate::sys::not_inherited`]).
            Ok(Socket::owned(s))
        }

        fn set_timeout(&self, d: Option<Duration>, opt: i32) -> io::Result<()> {
            let ms: u32 = match d {
                // As std: a zero timeout would mean none.
                Some(d) if d.is_zero() => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cannot set a 0 duration timeout",
                    ));
                }
                // Rounded up to a millisecond; 0 means no timeout.
                Some(d) => d.as_millis().clamp(1, u128::from(u32::MAX - 1)) as u32,
                None => 0,
            };
            // SAFETY: a socket we own; a DWORD option and its size.
            let rc = unsafe {
                setsockopt(
                    self.0,
                    SOL_SOCKET,
                    opt,
                    (&ms as *const u32).cast(),
                    std::mem::size_of::<u32>() as i32,
                )
            };
            if rc == SOCKET_ERROR {
                return Err(last_error());
            }
            Ok(())
        }

        fn timeout(&self, opt: i32) -> io::Result<Option<Duration>> {
            let mut ms: u32 = 0;
            let mut len = std::mem::size_of::<u32>() as i32;
            // SAFETY: a socket we own; a DWORD to fill and its size.
            let rc = unsafe {
                getsockopt(
                    self.0,
                    SOL_SOCKET,
                    opt,
                    (&mut ms as *mut u32).cast(),
                    &mut len,
                )
            };
            if rc == SOCKET_ERROR {
                return Err(last_error());
            }
            Ok((ms != 0).then(|| Duration::from_millis(u64::from(ms))))
        }

        fn set_nonblocking(&self, on: bool) -> io::Result<()> {
            let mut arg = u32::from(on);
            // SAFETY: a socket we own; FIONBIO reads the u32.
            if unsafe { ioctlsocket(self.0, FIONBIO, &mut arg) } == SOCKET_ERROR {
                return Err(last_error());
            }
            Ok(())
        }

        fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
            let len = buf.len().min(i32::MAX as usize) as i32;
            // SAFETY: buf has room for len bytes.
            let n = unsafe { recv(self.0, buf.as_mut_ptr(), len, 0) };
            if n == SOCKET_ERROR {
                // SAFETY: plain call.
                let code = unsafe { WSAGetLastError() };
                // Its reading side shut down here: the end, as on Unix.
                if code == WSAESHUTDOWN {
                    return Ok(0);
                }
                return Err(io::Error::from_raw_os_error(code));
            }
            Ok(n as usize)
        }

        fn send(&self, buf: &[u8]) -> io::Result<usize> {
            let len = buf.len().min(i32::MAX as usize) as i32;
            // SAFETY: buf holds len bytes.
            let n = unsafe { send(self.0, buf.as_ptr(), len, 0) };
            if n == SOCKET_ERROR {
                return Err(last_error());
            }
            Ok(n as usize)
        }

        fn shutdown(&self, how: Shutdown) -> io::Result<()> {
            let how = match how {
                Shutdown::Read => SD_RECEIVE,
                Shutdown::Write => SD_SEND,
                Shutdown::Both => SD_BOTH,
            };
            // SAFETY: a socket we own.
            if unsafe { shutdown(self.0, how) } == SOCKET_ERROR {
                return Err(last_error());
            }
            Ok(())
        }
    }

    /// `path` as a `sockaddr_un` and its length: UTF-8, at most [`super::MAX_PATH_BYTES`].
    fn sockaddr(path: &Path) -> io::Result<(SOCKADDR_UN, i32)> {
        let text = crate::sys::path_str(path)?;
        let bytes = text.as_bytes();
        let mut addr = SOCKADDR_UN {
            sun_family: AF_UNIX,
            ..Default::default()
        };
        if bytes.len() > super::MAX_PATH_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{}: the socket's path is {} bytes; a Unix domain socket's path is at most {} \
                     (108 with its terminating NUL)",
                    path.display(),
                    bytes.len(),
                    super::MAX_PATH_BYTES
                ),
            ));
        }
        if bytes.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{}: the socket's path holds a NUL", path.display()),
            ));
        }
        for (d, &b) in addr.sun_path.iter_mut().zip(bytes) {
            *d = b as i8;
        }
        let len = std::mem::offset_of!(SOCKADDR_UN, sun_path) + bytes.len() + 1;
        Ok((addr, len as i32))
    }

    /// Tells connections apart: the same for a stream and its clones ([`UnixStream::id`]).
    fn next_id() -> u64 {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        NEXT.fetch_add(1, Ordering::Relaxed)
    }

    /// The address of a socket: the path it is bound to, if any. An accepted connection's peer
    /// has none (clients don't bind).
    #[derive(Clone, Debug, Default)]
    pub struct SocketAddr {
        path: Option<PathBuf>,
    }

    impl SocketAddr {
        pub fn as_pathname(&self) -> Option<&Path> {
            self.path.as_deref()
        }

        pub fn is_unnamed(&self) -> bool {
            self.path.is_none()
        }
    }

    /// A connected AF_UNIX stream socket.
    #[derive(Debug)]
    pub struct UnixStream {
        sock: Socket,
        id: u64,
    }

    impl UnixStream {
        /// Connects to the socket at `path`. No file there: `NotFound`; a file nothing listens
        /// on (a daemon's, left behind): `ConnectionRefused`.
        pub fn connect<P: AsRef<Path>>(path: P) -> io::Result<UnixStream> {
            let path = path.as_ref();
            let (addr, len) = sockaddr(path)?;
            let sock = Socket::new()?;
            // SAFETY: addr is a sockaddr_un of len bytes.
            let rc = unsafe { connect(sock.0, (&addr as *const SOCKADDR_UN).cast(), len) };
            if rc == SOCKET_ERROR {
                let e = last_error();
                if std::fs::symlink_metadata(path)
                    .is_err_and(|m| m.kind() == io::ErrorKind::NotFound)
                {
                    return Err(io::Error::new(io::ErrorKind::NotFound, e));
                }
                return Err(e);
            }
            Ok(UnixStream {
                sock,
                id: next_id(),
            })
        }

        /// Another handle on the same connection (reads and writes on either are the
        /// connection's; timeouts and the blocking mode are shared).
        pub fn try_clone(&self) -> io::Result<UnixStream> {
            Ok(UnixStream {
                sock: self.sock.duplicate()?,
                id: self.id,
            })
        }

        /// The same for this stream and every clone of it, and for no other connection.
        pub fn id(&self) -> u64 {
            self.id
        }

        pub fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
            self.sock.set_timeout(d, SO_RCVTIMEO)
        }

        pub fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> {
            self.sock.set_timeout(d, SO_SNDTIMEO)
        }

        pub fn read_timeout(&self) -> io::Result<Option<Duration>> {
            self.sock.timeout(SO_RCVTIMEO)
        }

        pub fn write_timeout(&self) -> io::Result<Option<Duration>> {
            self.sock.timeout(SO_SNDTIMEO)
        }

        pub fn set_nonblocking(&self, on: bool) -> io::Result<()> {
            self.sock.set_nonblocking(on)
        }

        pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
            self.sock.shutdown(how)
        }
    }

    impl Read for UnixStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.sock.recv(buf)
        }
    }

    impl Read for &UnixStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.sock.recv(buf)
        }
    }

    impl Write for UnixStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.sock.send(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Write for &UnixStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.sock.send(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl AsRawSocket for UnixStream {
        fn as_raw_socket(&self) -> RawSocket {
            self.sock.0 as RawSocket
        }
    }

    impl IntoRawSocket for UnixStream {
        fn into_raw_socket(self) -> RawSocket {
            let s = self.sock.0;
            std::mem::forget(self);
            s as RawSocket
        }
    }

    impl FromRawSocket for UnixStream {
        /// # Safety
        /// `sock` is an open AF_UNIX stream socket that nothing else owns.
        unsafe fn from_raw_socket(sock: RawSocket) -> UnixStream {
            UnixStream {
                sock: Socket(sock as SOCKET),
                id: next_id(),
            }
        }
    }

    /// A listening AF_UNIX stream socket.
    #[derive(Debug)]
    pub struct UnixListener {
        sock: Socket,
        path: PathBuf,
    }

    impl UnixListener {
        /// Binds a new socket file at `path` and listens on it. A file there already (a socket
        /// left behind) fails with `AddrInUse`: remove it first.
        pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<UnixListener> {
            let path = path.as_ref();
            let (addr, len) = sockaddr(path)?;
            let sock = Socket::new()?;
            // SAFETY: addr is a sockaddr_un of len bytes.
            if unsafe { bind(sock.0, (&addr as *const SOCKADDR_UN).cast(), len) } == SOCKET_ERROR {
                return Err(last_error());
            }
            // SAFETY: a bound socket we own.
            if unsafe { listen(sock.0, BACKLOG) } == SOCKET_ERROR {
                return Err(last_error());
            }
            Ok(UnixListener {
                sock,
                path: path.to_path_buf(),
            })
        }

        /// The next connection; in non-blocking mode `WouldBlock` when none waits. The
        /// connection takes the listener's blocking mode.
        pub fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
            // SAFETY: plain data, filled by the call.
            let mut addr: SOCKADDR_UN = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of::<SOCKADDR_UN>() as i32;
            // SAFETY: addr has room for len bytes; the socket returned is ours.
            let s = unsafe {
                accept(
                    self.sock.0,
                    (&mut addr as *mut SOCKADDR_UN).cast::<SOCKADDR>(),
                    &mut len,
                )
            };
            if s == INVALID_SOCKET {
                return Err(last_error());
            }
            Ok((
                UnixStream {
                    sock: Socket::owned(s),
                    id: next_id(),
                },
                SocketAddr::default(),
            ))
        }

        pub fn incoming(&self) -> Incoming<'_> {
            Incoming { listener: self }
        }

        pub fn try_clone(&self) -> io::Result<UnixListener> {
            Ok(UnixListener {
                sock: self.sock.duplicate()?,
                path: self.path.clone(),
            })
        }

        pub fn set_nonblocking(&self, on: bool) -> io::Result<()> {
            self.sock.set_nonblocking(on)
        }

        pub fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr {
                path: Some(self.path.clone()),
            })
        }
    }

    impl AsRawSocket for UnixListener {
        fn as_raw_socket(&self) -> RawSocket {
            self.sock.0 as RawSocket
        }
    }

    /// The connections a listener accepts, one at a time ([`UnixListener::incoming`]).
    pub struct Incoming<'a> {
        listener: &'a UnixListener,
    }

    impl Iterator for Incoming<'_> {
        type Item = io::Result<UnixStream>;
        fn next(&mut self) -> Option<io::Result<UnixStream>> {
            Some(self.listener.accept().map(|(s, _)| s))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::time::{Duration, Instant};

    /// A short directory for sockets (their paths are limited), removed with the guard.
    fn sock_dir() -> tempfile::TempDir {
        tempfile::Builder::new().prefix("pl").tempdir().unwrap()
    }

    #[test]
    fn a_connection_carries_bytes_both_ways_and_ends_with_a_shutdown() {
        let d = sock_dir();
        let path = d.path().join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        assert!(socket_file_exists(&path));
        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let mut buf = [0u8; 5];
            conn.read_exact(&mut buf).unwrap();
            conn.write_all(&buf).unwrap();
            // A clone writes on the same connection.
            let mut clone = conn.try_clone().unwrap();
            clone.write_all(b"!").unwrap();
            conn.shutdown(std::net::Shutdown::Both).unwrap();
        });
        let c = UnixStream::connect(&path).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        (&c).write_all(b"hello").unwrap();
        let mut got = Vec::new();
        let mut reader = c.try_clone().unwrap();
        reader.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"hello!");
        server.join().unwrap();
    }

    #[test]
    fn a_read_with_nothing_to_read_times_out() {
        let d = sock_dir();
        let path = d.path().join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let c = UnixStream::connect(&path).unwrap();
        let (_conn, _) = listener.accept().unwrap();
        c.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        assert_eq!(c.read_timeout().unwrap(), Some(Duration::from_millis(100)));
        assert!(c.set_read_timeout(Some(Duration::ZERO)).is_err());
        let began = Instant::now();
        let e = (&c).read(&mut [0u8; 8]).unwrap_err();
        assert!(
            matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            "{e:?}"
        );
        assert!(
            began.elapsed() >= Duration::from_millis(80),
            "{:?}",
            began.elapsed()
        );
    }

    /// No socket file: not found. One left behind with nothing listening: refused, and it must be
    /// removed before another listener binds there.
    #[test]
    fn a_connect_while_nothing_listens_is_refused_and_a_stale_file_is_removed() {
        let d = sock_dir();
        let path = d.path().join("s.sock");
        let e = UnixStream::connect(&path).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::NotFound, "{e:?}");
        drop(UnixListener::bind(&path).unwrap());
        assert!(socket_file_exists(&path));
        // A child another test's thread is starting holds a copy of the listener until it execs
        // (on Unix), and the socket listens on meanwhile.
        let give_up = Instant::now() + Duration::from_secs(5);
        let e = loop {
            match UnixStream::connect(&path) {
                Ok(_) if Instant::now() < give_up => std::thread::sleep(Duration::from_millis(10)),
                Ok(c) => panic!("still listening: {c:?}"),
                Err(e) => break e,
            }
        };
        assert_eq!(e.kind(), std::io::ErrorKind::ConnectionRefused, "{e:?}");
        assert!(UnixListener::bind(&path).is_err());
        std::fs::remove_file(&path).unwrap();
        assert!(!socket_file_exists(&path));
        let listener = UnixListener::bind(&path).unwrap();
        let _c = UnixStream::connect(&path).unwrap();
        listener.accept().unwrap();
    }

    #[test]
    fn a_non_blocking_accept_says_when_no_connection_waits() {
        let d = sock_dir();
        let path = d.path().join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let e = listener.accept().unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock, "{e:?}");
        let _c = UnixStream::connect(&path).unwrap();
        let give_up = Instant::now() + Duration::from_secs(10);
        let (conn, _) = loop {
            match listener.accept() {
                Ok(c) => break c,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < give_up, "no connection to accept");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => panic!("{e}"),
            }
        };
        conn.set_nonblocking(false).unwrap();
        let clone = listener.try_clone().unwrap();
        drop(clone);
    }

    /// No child process gets a copy of a connection or of its clone: once this process lets go
    /// of it, the other end reads its end, though a child started meanwhile runs on.
    #[test]
    fn a_child_process_holds_no_copy_of_a_connection() {
        let d = sock_dir();
        let path = d.path().join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let c = UnixStream::connect(&path).unwrap();
        // Before the other end closes: macOS refuses setsockopt on a socket whose peer is gone.
        c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let (conn, _) = listener.accept().unwrap();
        let clone = conn.try_clone().unwrap();
        // Started with every inheritable handle of this process, as std starts a child.
        let mut child = if cfg!(windows) {
            std::process::Command::new("ping")
                .args(["-n", "30", "127.0.0.1"])
                .stdout(std::process::Stdio::null())
                .spawn()
        } else {
            std::process::Command::new("sleep").arg("30").spawn()
        }
        .unwrap();
        drop((conn, clone));
        let read = (&c).read(&mut [0u8; 8]);
        let _ = child.kill();
        let _ = child.wait();
        assert_eq!(read.unwrap(), 0);
    }

    #[cfg(windows)]
    #[test]
    fn a_path_too_long_for_a_socket_says_the_limit() {
        let d = sock_dir();
        let path = d.path().join(format!("{}.sock", "x".repeat(120)));
        let e = UnixListener::bind(&path).unwrap_err().to_string();
        assert!(e.contains("at most 107"), "{e}");
        let e = UnixStream::connect(&path).unwrap_err().to_string();
        assert!(e.contains("at most 107"), "{e}");
    }

    #[cfg(windows)]
    #[test]
    fn a_wait_sees_a_connection_and_data() {
        let d = sock_dir();
        let path = d.path().join("s.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let t = Some(Duration::from_millis(50));
        assert_eq!(wait_readable(&[listener.as_raw_sock()], t), [false]);
        let mut c = UnixStream::connect(&path).unwrap();
        assert_eq!(
            wait_readable(&[listener.as_raw_sock()], Some(Duration::from_secs(10))),
            [true]
        );
        let (conn, _) = listener.accept().unwrap();
        assert_eq!(wait_readable(&[conn.as_raw_sock()], t), [false]);
        c.write_all(b"x").unwrap();
        assert_eq!(
            wait_readable(&[conn.as_raw_sock()], Some(Duration::from_secs(10))),
            [true]
        );
        assert_eq!(c.id(), c.try_clone().unwrap().id());
        assert_ne!(c.id(), conn.id());
    }
}
