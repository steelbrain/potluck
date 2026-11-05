//! A fake potluck daemon on a temporary Unix socket, answering with a test's handler, and helpers
//! to build the records it answers with.

#![allow(dead_code)]

use potluck::config::Paths;
use potluck::proto::{ClientRequest, Response, ServerResponse, read_msg, write_msg};
use potluck::types::{
    JobRecord, JobSpec, JobState, Reminder, RepoRef, Request, Usage, WorkspaceState,
};
use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// One client connection, for the handler to answer on.
pub struct Conn<'a> {
    stream: &'a mut UnixStream,
    pub reminders: Vec<Reminder>,
}

impl Conn<'_> {
    pub fn send(&mut self, resp: Response) {
        write_msg(
            self.stream,
            &ServerResponse {
                reminders: self.reminders.clone(),
                resp,
            },
        )
        .unwrap();
    }

    /// A frame followed by raw bytes (`Bytes`, `CopyEntry`).
    pub fn send_with(&mut self, resp: Response, bytes: &[u8]) {
        self.send(resp);
        self.stream.write_all(bytes).unwrap();
        self.stream.flush().unwrap();
    }

    /// Closes the connection, as a daemon that stops does.
    pub fn hang_up(&mut self) {
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}

pub type Handler = dyn Fn(&ClientRequest, &mut Conn) + Send + Sync;

pub struct FakeDaemon {
    pub dir: tempfile::TempDir,
    pub requests: Arc<Mutex<Vec<ClientRequest>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl FakeDaemon {
    /// Listens on `<temp POTLUCK_HOME>/potluck.sock`; every response carries `reminders`.
    pub fn start(
        reminders: Vec<Reminder>,
        handler: impl Fn(&ClientRequest, &mut Conn) + Send + Sync + 'static,
    ) -> FakeDaemon {
        let dir = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(dir.path().join("potluck.sock")).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let handler: Arc<Handler> = Arc::new(handler);
        let thread = {
            let (requests, stop) = (requests.clone(), stop.clone());
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(mut stream) = stream else { continue };
                    let (requests, handler, reminders) =
                        (requests.clone(), handler.clone(), reminders.clone());
                    std::thread::spawn(move || {
                        let mut reader = BufReader::new(stream.try_clone().unwrap());
                        while let Ok(Some(req)) = read_msg::<_, ClientRequest>(&mut reader) {
                            requests.lock().unwrap().push(req.clone());
                            let mut conn = Conn {
                                stream: &mut stream,
                                reminders: reminders.clone(),
                            };
                            handler(&req, &mut conn);
                        }
                    });
                }
            })
        };
        FakeDaemon {
            dir,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    pub fn home(&self) -> &Path {
        self.dir.path()
    }

    pub fn paths(&self) -> Paths {
        Paths {
            home: self.home().to_path_buf(),
            config: self.home().join("node.toml"),
        }
    }

    pub fn requests(&self) -> Vec<ClientRequest> {
        self.requests.lock().unwrap().clone()
    }

    /// The `op` of each request received, in order.
    pub fn ops(&self) -> Vec<String> {
        self.requests()
            .iter()
            .map(|r| {
                serde_json::to_value(&r.req).unwrap()["op"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }
}

impl Drop for FakeDaemon {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = UnixStream::connect(self.home().join("potluck.sock"));
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

pub fn reminder(ws: &str, left_ms: u64) -> Reminder {
    Reminder {
        workspace: ws.into(),
        node: "msi".into(),
        commit: "0123456789abcdef0123456789abcdef01234567".into(),
        state: WorkspaceState::Held,
        remaining_ms: left_ms,
        size_bytes: Some(3 << 30),
    }
}

pub fn spec(cmd: &[&str]) -> JobSpec {
    JobSpec {
        key: None,
        labels: BTreeMap::new(),
        priority: 3,
        request: Request::default(),
        on: BTreeMap::new(),
        repo: RepoRef {
            id: "app-0011aabb".into(),
            source: PathBuf::from("/src/app/.git"),
            commit: "0123456789abcdef0123456789abcdef01234567".into(),
            branch: Some("feature".into()),
            submodules: vec![],
            carried: vec![],
        },
        workspace: None,
        advance: false,
        cmd: cmd.iter().map(|s| s.to_string()).collect(),
        env: BTreeMap::new(),
        unset_env: Default::default(),
        timeout_ms: 600_000,
        hold_ms: None,
        keep: vec![],
        rerun_if_lost: true,
        kill_on_stall: false,
        recipe: None,
        submitted_from: "studio".into(),
        pools: Vec::new(),
        auth: None,
    }
}

pub fn job(id: &str, state: JobState, exit_code: Option<i32>) -> JobRecord {
    JobRecord {
        id: id.into(),
        spec: spec(&["cargo", "test"]),
        state,
        submitted_at: 1_000,
        started_at: Some(2_000),
        ended_at: state.is_done().then_some(3_000),
        node: Some("msi".into()),
        workspace: Some("msi-w7".into()),
        lease: None,
        handle: None,
        exit_code,
        signal: None,
        message: None,
        stuck_pids: vec![],
        usage: Usage::default(),
        passes: 0,
        stalled: false,
        kept: vec![],
        notes: Vec::new(),
    }
}

/// The potluck binary under test, with POTLUCK_HOME set and no job variables from outside.
pub fn potluck(home: &Path) -> std::process::Command {
    potluck_at(Path::new(env!("CARGO_BIN_EXE_potluck")), home)
}

/// [`potluck`], started through another path to the binary (a symlink).
pub fn potluck_at(program: &Path, home: &Path) -> std::process::Command {
    let mut c = std::process::Command::new(program);
    c.env("POTLUCK_HOME", home)
        .env("POTLUCK_CONFIG", home.join("node.toml"))
        .env_remove("POTLUCK_JOB")
        .env_remove("POTLUCK_LEASE")
        .env_remove("POTLUCK_WORKSPACE")
        .stdin(std::process::Stdio::null());
    c
}

/// Copies the executable at `from` to `to` through a child process (`cp`), so this
/// (multithreaded) test process never holds `to` open for writing. If it did, another test
/// thread forking meanwhile would leave a copy of that descriptor in its child until the child
/// execs, and running `to` in that time fails with ETXTBSY ("Text file busy") on Linux.
pub fn copy_binary(from: &Path, to: &Path) {
    let st = std::process::Command::new("cp")
        .arg(from)
        .arg(to)
        .status()
        .unwrap();
    assert!(st.success(), "cp {} {}: {st}", from.display(), to.display());
}

pub struct Run {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

pub fn run(cmd: &mut std::process::Command) -> Run {
    let out = cmd.output().unwrap();
    Run {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// A port on 127.0.0.1, free when asked; the daemon binds it once in a cluster, maybe seconds
/// later, and again after a restart. It is below the ranges the system hands out to listeners on
/// port 0 and to outgoing connections (32768-60999 on Linux, 49152-65535 on macOS): a port from
/// those can meanwhile go to any test's connection ("can't listen on 127.0.0.1:43731: Address
/// already in use" at `admit`). Each is handed out once in this process.
///
/// Other test processes pick from the same range. Each port handed out is claimed for this
/// process until it ends, by binding the UDP port of the same number (TCP listeners don't mind
/// it): a process skips a port whose UDP twin it can't bind, so no two pick the same one.
pub fn free_port() -> u16 {
    use std::hash::{BuildHasher, Hasher};
    static NEXT: Mutex<u16> = Mutex::new(0);
    static CLAIMS: Mutex<Vec<std::net::UdpSocket>> = Mutex::new(Vec::new());
    let mut next = NEXT.lock().unwrap();
    if *next == 0 {
        // Each RandomState has keys of its own: a random start in 20000..30000.
        let r = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        *next = 20_000 + (r % 10_000) as u16;
    }
    for _ in 0..2_000 {
        let port = *next;
        *next = if port >= 31_999 { 20_000 } else { port + 1 };
        let Ok(claim) = std::net::UdpSocket::bind(("127.0.0.1", port)) else {
            continue;
        };
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            CLAIMS.lock().unwrap().push(claim);
            return port;
        }
    }
    panic!("no free port on 127.0.0.1 in 20000..32000");
}

/// The process groups that hold a `potluck _supervise` started in a group of its own (`pid`) and
/// its command, which leads a group of its own: `pid`'s, then each child's. Signalling these
/// reaches every process of the invocation that stayed in its group.
pub fn supervisor_groups(pid: u32) -> Vec<u32> {
    let mut groups = vec![pid];
    groups.extend(children(pid));
    groups
}

#[cfg(target_os = "linux")]
fn children(pid: u32) -> Vec<u32> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
        let Some(p) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{p}/stat")) else {
            continue;
        };
        // "PID (COMM) STATE PPID ..."
        let ppid = stat
            .rfind(')')
            .and_then(|i| stat[i + 1..].split_whitespace().nth(1))
            .and_then(|v| v.parse::<u32>().ok());
        if ppid == Some(pid) {
            out.push(p);
        }
    }
    out
}

#[cfg(target_os = "macos")]
fn children(pid: u32) -> Vec<u32> {
    let mut buf = vec![0i32; 256];
    // SAFETY: the buffer holds 256 pids; the size is in bytes; it returns how many it wrote.
    let n = unsafe {
        libc::proc_listchildpids(
            pid as i32,
            buf.as_mut_ptr().cast(),
            (buf.len() * std::mem::size_of::<i32>()) as i32,
        )
    };
    buf.truncate(n.max(0) as usize);
    buf.into_iter()
        .filter(|&p| p > 1)
        .map(|p| p as u32)
        .collect()
}
