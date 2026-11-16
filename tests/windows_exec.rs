//! The Windows executor against real processes: job objects, `potluck _supervise` from this
//! build, and cmd.exe, PowerShell and Git for Windows' sh (first on PATH, tests/common) as
//! jobs. Every test ends what it started (TerminateJobObject through the executor) and checks
//! that nothing is left.
#![cfg(windows)]

mod common;

use potluck::exec::windows::{JobExecutor, process_info};
use potluck::exec::{Executor, Launch, Limits, SIGKILL, SIGTERM, Sample};
use potluck::supervise::JobStatus;
use potluck::types::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MIB: u64 = 1024 * 1024;
const POTLUCK: &str = env!("CARGO_BIN_EXE_potluck");
/// A test machine may be a small VM: be patient.
const PATIENCE: Duration = Duration::from_secs(60);

fn job_record(cmd: &[&str]) -> JobRecord {
    JobRecord {
        id: "test-1".into(),
        spec: JobSpec {
            key: None,
            labels: BTreeMap::new(),
            priority: DEFAULT_PRIORITY,
            request: Request::default(),
            on: BTreeMap::new(),
            repo: RepoRef {
                id: "repo-00000000".into(),
                source: PathBuf::from("C:\\nonexistent"),
                commit: "0".repeat(40),
                branch: None,
                submodules: vec![],
                carried: vec![],
            },
            workspace: None,
            advance: false,
            cmd: cmd.iter().map(|s| s.to_string()).collect(),
            env: BTreeMap::new(),
            unset_env: Default::default(),
            timeout_ms: 60_000,
            hold_ms: None,
            keep: vec![],
            rerun_if_lost: false,
            kill_on_stall: false,
            hold_line: false,
            recipe: None,
            submitted_from: "test".into(),
            pools: Vec::new(),
            auth: None,
        },
        state: JobState::Running,
        submitted_at: 0,
        started_at: None,
        ended_at: None,
        node: None,
        workspace: None,
        lease: None,
        handle: None,
        exit_code: None,
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

/// The test's own environment (a Windows process wants SystemRoot, PATH and the like), as a
/// job's environment is all the daemon gives it.
fn base_env() -> BTreeMap<String, String> {
    std::env::vars().collect()
}

fn limits() -> Limits {
    Limits {
        mem_max_bytes: 0,
        cpu_weight: 100,
        tasks_max: 1000,
        kill_grace_ms: 10_000,
        log_max_bytes: 0,
    }
}

/// A started invocation. Dropping it kills whatever is left and checks that nothing remains.
struct Inv {
    dir: tempfile::TempDir,
    handle: Handle,
}

impl Inv {
    fn start(cmd: &[&str]) -> Inv {
        Self::start_full(&JobExecutor::default(), cmd, base_env(), limits())
    }

    fn start_full(
        exec: &JobExecutor,
        cmd: &[&str],
        env: BTreeMap<String, String>,
        limits: Limits,
    ) -> Inv {
        let dir = tempfile::tempdir().unwrap();
        let handle = start_in(exec, dir.path(), cmd, env, limits);
        Inv { dir, handle }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.path("out.log")).unwrap_or_default()
    }

    fn status(&self, timeout: Duration) -> Option<JobStatus> {
        let path = self.path("status.json");
        wait_for(timeout, || {
            std::fs::read(&path)
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
        })
    }

    fn sample(&mut self, exec: &JobExecutor) -> Sample {
        exec.sample(&mut self.handle, 0).unwrap()
    }

    /// Waits until no process of the job is left.
    fn assert_all_gone(&mut self, exec: &JobExecutor) {
        let gone = wait_for(Duration::from_secs(10), || {
            self.sample(exec).pids.is_empty().then_some(())
        });
        assert!(gone.is_some(), "left: {:?}", self.sample(exec).pids);
    }
}

impl Drop for Inv {
    fn drop(&mut self) {
        let exec = JobExecutor::default();
        let _ = exec.signal(&self.handle, SIGKILL);
        let finished = exec.finish(&self.handle, &self.path("crash"), 0);
        if !std::thread::panicking() {
            finished.unwrap();
            self.assert_all_gone(&exec);
        }
    }
}

/// Starts `cmd` with the executor, its files in `dir` (a `work` directory is its cwd).
fn start_in(
    exec: &JobExecutor,
    dir: &Path,
    cmd: &[&str],
    env: BTreeMap<String, String>,
    limits: Limits,
) -> Handle {
    let work = dir.join("work");
    let _ = std::fs::create_dir(&work);
    let job = job_record(cmd);
    let grant = Grant::default();
    let launch = Launch {
        job: &job,
        grant: &grant,
        cwd: work,
        env,
        log: dir.join("out.log"),
        status: dir.join("status.json"),
        potluck_bin: PathBuf::from(POTLUCK),
        name: "potluck-test-1".into(),
        limits,
    };
    exec.start(&launch).unwrap()
}

fn wait_for<T>(timeout: Duration, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The image names of the processes among `pids` that still run, in lower case. Beside the
/// supervisor and the command, a job holds the console host of the supervisor's console
/// (conhost.exe).
fn images(pids: &[u32]) -> Vec<String> {
    process_info(pids)
        .unwrap()
        .into_iter()
        .map(|p| p.image.to_lowercase())
        .collect()
}

/// `ping` as a sleep that needs no console input (timeout.exe refuses a redirected stdin).
fn sleep_cmd(secs: u32) -> String {
    format!("ping -n {} 127.0.0.1 >NUL", secs + 1)
}

#[test]
fn exit_code_and_output_come_through() {
    let mut inv = Inv::start(&["cmd", "/d", "/c", "echo one& echo two 1>&2& exit /b 7"]);
    let st = inv.status(PATIENCE).expect("status file");
    assert_eq!((st.exit.code, st.exit.signal), (Some(7), None));
    assert!(!st.over_memory);
    assert_eq!(inv.log().replace("\r\n", "\n"), "one\ntwo \n");
    inv.assert_all_gone(&JobExecutor::default());
}

#[test]
fn a_command_that_is_not_found_ends_127() {
    let inv = Inv::start(&["potluck-no-such-command"]);
    let st = inv.status(PATIENCE).expect("status file");
    assert_eq!(st.exit.code, Some(127));
    assert!(inv.log().contains("could not start"), "{}", inv.log());
}

/// The job gets exactly the given environment, values with spaces and `=` too, and runs in the
/// given working directory.
#[test]
fn environment_and_working_directory() {
    let mut env = base_env();
    env.insert("POTLUCK_TEST_VALUE".into(), "a b=c".into());
    env.retain(|k, _| !k.eq_ignore_ascii_case("POTLUCK_TEST_ABSENT"));
    let inv = Inv::start_full(
        &JobExecutor::default(),
        &[
            "cmd",
            "/d",
            "/c",
            "echo [%POTLUCK_TEST_VALUE%]& echo [%POTLUCK_TEST_ABSENT%]& cd",
        ],
        env,
        limits(),
    );
    inv.status(PATIENCE).expect("status file");
    let log = inv.log().replace("\r\n", "\n");
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(lines[..2], ["[a b=c]", "[%POTLUCK_TEST_ABSENT%]"], "{log}");
    let cwd = PathBuf::from(lines[2]);
    assert_eq!(
        cwd.canonicalize().unwrap(),
        inv.path("work").canonicalize().unwrap()
    );
}

/// Arguments reach the command as they were given: through the supervisor's command line and
/// the one it gives the command, both quoted as the C runtime reads them. The command is git,
/// which reads its command line as the C runtime does and prints its arguments back quoted
/// (`rev-parse --sq-quote`); Git for Windows' sh would not do: an MSYS program reads a command
/// line its own way (`q\"uote` there opens a quote).
#[test]
fn arguments_arrive_unchanged() {
    let args = [
        "a b",
        "q\"uote",
        "back\\slash\\",
        "",
        "x",
        "x\\\"y",
        "sp ace\\",
        "tab\there",
    ];
    let mut cmd = vec!["git", "rev-parse", "--sq-quote"];
    cmd.extend(args);
    let inv = Inv::start(&cmd);
    let st = inv.status(PATIENCE).expect("status file");
    assert_eq!(st.exit.code, Some(0), "{}", inv.log());
    let want: String = args.iter().map(|a| format!(" '{a}'")).collect();
    assert_eq!(inv.log().replace("\r\n", "\n"), format!("{want}\n"));
}

/// A stop request reaches the command as CTRL_BREAK; the supervisor survives it and records how
/// the command ended. The held tail of dropped output is written at once.
#[test]
fn a_stop_is_ctrl_break_and_the_supervisor_records_the_end() {
    let exec = JobExecutor::default();
    let mut l = limits();
    l.log_max_bytes = 100;
    // sleep's own stderr goes nowhere: MSYS's exec starts it as a new process, and a CTRL_BREAK
    // while that one starts up can end it with errors of its own (couldn't create signal pipe).
    let script = "i=1000; while [ $i -lt 1100 ]; do echo line $i; i=$((i+1)); done; \
                  echo started > started; exec sleep 60 2>/dev/null";
    let mut inv = Inv::start_full(&exec, &["sh", "-c", script], base_env(), l);
    // Stopped once sleep runs.
    wait_for(PATIENCE, || {
        (inv.path("work/started").exists()
            && images(&inv.sample(&exec).pids).contains(&"sleep.exe".to_string()))
        .then_some(())
    })
    .expect("the command to start");
    let stopped = Instant::now();
    exec.signal(&inv.handle, SIGTERM).unwrap();
    let st = inv
        .status(PATIENCE)
        .expect("status file: the supervisor survived");
    assert!(stopped.elapsed() < Duration::from_secs(30));
    assert_ne!(st.exit.code, Some(0));
    let log = inv.log();
    assert!(log.ends_with("line 1098\nline 1099\n"), "{log}");
    inv.assert_all_gone(&exec);
}

/// SIGKILL ends every process of the job at once, the supervisor too.
#[test]
fn kill_ends_every_process() {
    let exec = JobExecutor::default();
    let cmd = format!(
        "start /b {s} & start /b {s} & echo started> started& {s}",
        s = sleep_cmd(60)
    );
    let mut inv = Inv::start(&["cmd", "/d", "/c", &cmd]);
    let many = wait_for(PATIENCE, || {
        let s = inv.sample(&exec);
        (inv.path("work/started").exists() && s.pids.len() >= 4).then_some(s)
    });
    assert!(many.is_some(), "{:?}", inv.sample(&exec));
    exec.signal(&inv.handle, SIGKILL).unwrap();
    inv.assert_all_gone(&exec);
    assert!(
        !inv.path("status.json").exists(),
        "the supervisor outlived the kill"
    );
    exec.finish(&inv.handle, &inv.path("crash"), 0).unwrap();
}

/// An allocation past the job's RAM fails; the supervisor ends the command and records it, and
/// `sample` says over memory, with the job's peak. That peak (PeakJobMemoryUsed) counts the
/// allocation that failed: here about 1 GiB, past the limit.
#[test]
fn over_its_ram_the_job_ends_over_memory() {
    let exec = JobExecutor::default();
    let mut l = limits();
    l.mem_max_bytes = 256 * MIB;
    let mut inv = Inv::start_full(
        &exec,
        &[
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "$a = New-Object byte[] 1GB; Start-Sleep 60",
        ],
        base_env(),
        l,
    );
    let over = wait_for(PATIENCE, || {
        let s = inv.sample(&exec);
        s.over_memory.then_some(s)
    })
    .unwrap_or_else(|| panic!("never over memory: {:?}\n{}", inv.sample(&exec), inv.log()));
    assert!(over.peak_mem_bytes > 0, "{over:?}");
    let st = inv.status(PATIENCE).expect("status file");
    assert!(st.over_memory, "{st:?}");
    assert!(st.peak_mem_bytes > 0, "{st:?}");
    inv.assert_all_gone(&exec);
}

/// At most `tasks_max` processes at once, the supervisor included: starting one more fails. The
/// console host of the supervisor's console (conhost.exe) runs in the job too, but Windows
/// doesn't count it against the job's limit, so it isn't counted here.
#[test]
fn the_process_limit_holds() {
    let exec = JobExecutor::default();
    let mut l = limits();
    l.tasks_max = 3;
    let s = sleep_cmd(5);
    let cmd = format!(
        "start /b {s} & start /b {s} & start /b {s} & start /b {s} & {}",
        sleep_cmd(3)
    );
    let mut inv = Inv::start_full(&exec, &["cmd", "/d", "/c", &cmd], base_env(), l);
    let mut most = Vec::new();
    let st = wait_for(PATIENCE, || {
        let counted: Vec<String> = images(&inv.sample(&exec).pids)
            .into_iter()
            .filter(|i| i != "conhost.exe")
            .collect();
        if counted.len() > most.len() {
            most = counted;
        }
        inv.status(Duration::ZERO)
    });
    assert!(st.is_some(), "no status file: {}", inv.log());
    assert!(most.len() <= 3, "{most:?}");
}

/// After a daemon restart (a fresh executor), `find` gives the running job back from its
/// status path alone; the job is sampled, stopped and finished through it.
#[test]
fn find_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let since = now_ms();
    let started = start_in(
        &JobExecutor::default(),
        dir.path(),
        &["cmd", "/d", "/c", &sleep_cmd(60)],
        base_env(),
        limits(),
    );
    let status = dir.path().join("status.json");
    let exec = JobExecutor::default();
    let mut found = exec
        .find("potluck-test-1", &status, since)
        .unwrap()
        .expect("found");
    assert_eq!(found, started);
    assert!(!exec.sample(&mut found, 0).unwrap().pids.is_empty());
    // A record from before the claim is an earlier attempt's.
    assert_eq!(
        exec.find("potluck-test-1", &status, now_ms() + 60_000)
            .unwrap(),
        None
    );
    // No record: nothing was started.
    let other = dir.path().join("other.json");
    assert_eq!(exec.find("potluck-test-2", &other, since).unwrap(), None);
    exec.signal(&found, SIGKILL).unwrap();
    let gone = wait_for(Duration::from_secs(10), || {
        exec.sample(&mut found, 0)
            .unwrap()
            .pids
            .is_empty()
            .then_some(())
    });
    assert!(gone.is_some());
    exec.finish(&found, &dir.path().join("crash"), since)
        .unwrap();
    // Killed before its supervisor recorded anything: once the job object is gone, a start that
    // left nothing running and no status counts as having started nothing. The job object goes
    // a moment after its processes have ended, when Windows has let go of the last of them.
    let none = wait_for(Duration::from_secs(10), || {
        exec.find("potluck-test-1", &status, since)
            .unwrap()
            .is_none()
            .then_some(())
    });
    assert!(none.is_some(), "the job object outlived its processes");
}

/// A job that ended while no daemon ran is still found (its status was written since the
/// claim), and its sample says it is over.
///
/// What counts is the time the start's files say, not when Windows says they were written: it
/// stamps a file from a clock that advances once per timer tick (up to 15.6 ms), so one written
/// just after the claim can look older than it. Here they look a minute older.
#[test]
fn find_of_a_job_that_ended() {
    let dir = tempfile::tempdir().unwrap();
    let since = now_ms();
    let started = start_in(
        &JobExecutor::default(),
        dir.path(),
        &["cmd", "/d", "/c", "exit /b 4"],
        base_env(),
        limits(),
    );
    let status = dir.path().join("status.json");
    wait_for(PATIENCE, || status.exists().then_some(())).expect("status file");
    let Handle::JobObject { name, .. } = &started else {
        panic!("{started:?}")
    };
    let gone = wait_for(Duration::from_secs(10), || {
        (!job_object_exists(name)).then_some(())
    });
    assert!(gone.is_some(), "job object {name} outlived its processes");
    let past = std::time::SystemTime::now() - Duration::from_secs(60);
    for file in [status.with_extension("job"), status.clone()] {
        let f = std::fs::File::options().write(true).open(&file).unwrap();
        f.set_modified(past).unwrap();
    }
    let exec = JobExecutor::default();
    let mut found = exec
        .find("potluck-test-1", &status, since)
        .unwrap()
        .expect("found");
    assert_eq!(found, started);
    let gone = wait_for(Duration::from_secs(10), || {
        exec.sample(&mut found, 0)
            .unwrap()
            .pids
            .is_empty()
            .then_some(())
    });
    assert!(gone.is_some());
    exec.finish(&found, &dir.path().join("crash"), since)
        .unwrap();
}

/// The supervisor holds its job: a job outlives a supervisor's daemon, and ends with its
/// supervisor (KILL_ON_JOB_CLOSE, once no daemon holds the job either).
#[test]
fn the_job_ends_with_its_supervisor() {
    let exec = JobExecutor::default();
    let cmd = format!("start /b {s} & {s}", s = sleep_cmd(60));
    let mut inv = Inv::start(&["cmd", "/d", "/c", &cmd]);
    let pids = wait_for(PATIENCE, || {
        let s = inv.sample(&exec);
        let pings = images(&s.pids).iter().filter(|i| *i == "ping.exe").count();
        (pings == 2).then_some(s.pids)
    })
    .expect("the job's processes");
    let sup = process_info(&pids)
        .unwrap()
        .into_iter()
        .find(|p| p.image.eq_ignore_ascii_case("potluck.exe"))
        .expect("the supervisor");
    potluck::sys::kill_pid(sup.pid).unwrap();
    inv.assert_all_gone(&exec);
    assert!(!inv.path("status.json").exists());
}

/// What a stalled job's log gets: no stacks, the job's processes.
#[test]
fn stacks_list_the_processes() {
    let exec = JobExecutor::default();
    let mut inv = Inv::start(&["cmd", "/d", "/c", &sleep_cmd(60)]);
    wait_for(PATIENCE, || {
        images(&inv.sample(&exec).pids)
            .contains(&"ping.exe".to_string())
            .then_some(())
    })
    .expect("the job's processes");
    let text = exec.stacks(&inv.handle).unwrap();
    assert!(
        text.starts_with("stacks are not sampled on Windows"),
        "{text}"
    );
    for image in ["potluck.exe", "cmd.exe", "PING.EXE"] {
        assert!(
            text.to_lowercase().contains(&image.to_lowercase()),
            "{text}"
        );
    }
}

/// `--log-max` through the executor: the first bytes, then the last 16 MiB.
#[test]
fn the_log_limit_applies() {
    let mut l = limits();
    l.log_max_bytes = MIB;
    let inv = Inv::start_full(
        &JobExecutor::default(),
        &[
            "sh",
            "-c",
            "head -c 20971520 /dev/zero | tr '\\0' x; echo; echo the end",
        ],
        base_env(),
        l,
    );
    let st = inv.status(PATIENCE).expect("status file");
    assert_eq!(st.exit.code, Some(0));
    let log = std::fs::read(inv.path("out.log")).unwrap();
    let tail = 16 << 20;
    assert!(log[..1 << 20].iter().all(|&b| b == b'x'));
    assert!(log.ends_with(b"xxx\nthe end\n"));
    assert!(log.len() > tail + (1 << 20) && log.len() < tail + (1 << 20) + 1000);
}

/// Run as a helper process by [`the_job_outlives_the_daemons_job`]: joins the job object
/// `$POTLUCK_TEST_KEEPER` (as a daemon runs in its keeper's), starts a job, writes its handle to
/// `$POTLUCK_TEST_HELPER_DIR/handle.json` and exits. Does nothing otherwise.
#[test]
fn helper_starts_a_job() {
    use windows_sys::Win32::System::JobObjects::{AssignProcessToJobObject, OpenJobObjectW};
    use windows_sys::Win32::System::SystemServices::JOB_OBJECT_ASSIGN_PROCESS;
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let (Some(dir), Some(keeper)) = (
        std::env::var_os("POTLUCK_TEST_HELPER_DIR"),
        std::env::var("POTLUCK_TEST_KEEPER").ok(),
    ) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let name: Vec<u16> = keeper.encode_utf16().chain([0]).collect();
    // SAFETY: a NUL-terminated name; the handle is closed by Job, before this process starts
    // the job, so the test holds the keeper's only handle.
    let keeper = Job(unsafe { OpenJobObjectW(JOB_OBJECT_ASSIGN_PROCESS, 0, name.as_ptr()) });
    assert!(!keeper.0.is_null(), "{}", std::io::Error::last_os_error());
    // SAFETY: a valid job handle and this process's pseudo handle.
    let ok = unsafe { AssignProcessToJobObject(keeper.0, GetCurrentProcess()) };
    assert_ne!(ok, 0, "{}", std::io::Error::last_os_error());
    drop(keeper);
    let handle = start_in(
        &JobExecutor::default(),
        &dir,
        &["cmd", "/d", "/c", &sleep_cmd(60)],
        base_env(),
        limits(),
    );
    std::fs::write(
        dir.join("handle.json"),
        serde_json::to_vec(&handle).unwrap(),
    )
    .unwrap();
}

/// A daemon run as a service runs in its keeper's job object, which kills what is in it when the
/// service stops. The supervisor leaves that job when it allows it, so the job runs on after
/// the keeper's job is gone; when it doesn't, the start still works, within it, and the job ends
/// with it.
#[test]
fn the_job_outlives_the_daemons_job() {
    for breakaway in [true, false] {
        if breakaway && !may_leave_own_job() {
            eprintln!("skipped with breakaway: this test runs in a job that keeps its processes");
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let name = format!(
            "Local\\potluck-test-keeper-{}-{breakaway}",
            std::process::id()
        );
        let keeper = keeper_job(&name, breakaway);
        let out = std::fs::File::create(dir.path().join("helper.log")).unwrap();
        let helper = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "helper_starts_a_job", "--nocapture"])
            .env("POTLUCK_TEST_HELPER_DIR", dir.path())
            .env("POTLUCK_TEST_KEEPER", &name)
            .stdin(std::process::Stdio::null())
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .status()
            .unwrap();
        let bytes = std::fs::read(dir.path().join("handle.json")).unwrap_or_else(|_| {
            panic!(
                "no handle ({helper}): {}",
                std::fs::read_to_string(dir.path().join("helper.log")).unwrap_or_default()
            )
        });
        let mut handle: Handle = serde_json::from_slice(&bytes).unwrap();
        // The service stops: its keeper's job, and what is in it, go.
        drop(keeper);
        std::thread::sleep(Duration::from_millis(500));
        let exec = JobExecutor::default();
        let pids = exec.sample(&mut handle, 0).unwrap().pids;
        assert_eq!(
            !pids.is_empty(),
            breakaway,
            "breakaway {breakaway}: {pids:?}"
        );
        exec.signal(&handle, SIGKILL).unwrap();
        exec.finish(&handle, &dir.path().join("crash"), 0).unwrap();
    }
}

/// Whether a job object named `name` exists (a handle to it is open somewhere).
fn job_object_exists(name: &str) -> bool {
    use windows_sys::Win32::System::JobObjects::OpenJobObjectW;
    use windows_sys::Win32::System::SystemServices::JOB_OBJECT_QUERY;
    let wide: Vec<u16> = name.encode_utf16().chain([0]).collect();
    // SAFETY: a NUL-terminated name.
    let h = unsafe { OpenJobObjectW(JOB_OBJECT_QUERY, 0, wide.as_ptr()) };
    if h.is_null() {
        return false;
    }
    drop(Job(h));
    true
}

/// A job object handle, closed on drop.
struct Job(windows_sys::Win32::Foundation::HANDLE);

impl Drop for Job {
    fn drop(&mut self) {
        // SAFETY: a handle this test made and closes once.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

/// A job object named `name` as a service's keeper makes one: it kills what it holds when its
/// last handle closes, and lets processes leave it when `breakaway`.
fn keeper_job(name: &str, breakaway: bool) -> Job {
    use windows_sys::Win32::System::JobObjects::{
        CreateJobObjectW, JOB_OBJECT_LIMIT_BREAKAWAY_OK, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };
    let name: Vec<u16> = name.encode_utf16().chain([0]).collect();
    // SAFETY: a NUL-terminated name and default security; closed by Job.
    let h = unsafe { CreateJobObjectW(std::ptr::null(), name.as_ptr()) };
    assert!(!h.is_null(), "{}", std::io::Error::last_os_error());
    let job = Job(h);
    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        | if breakaway {
            JOB_OBJECT_LIMIT_BREAKAWAY_OK
        } else {
            0
        };
    // SAFETY: a valid job handle and a structure of the size given.
    let ok = unsafe {
        SetInformationJobObject(
            job.0,
            JobObjectExtendedLimitInformation,
            (&raw const info).cast(),
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    assert_ne!(ok, 0, "{}", std::io::Error::last_os_error());
    job
}

/// Whether a process this one starts may leave this one's job: it runs in none, or in one that
/// allows it.
fn may_leave_own_job() -> bool {
    use windows_sys::Win32::System::JobObjects::{
        IsProcessInJob, JOB_OBJECT_LIMIT_BREAKAWAY_OK, JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        QueryInformationJobObject,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let mut inside = 0;
    // SAFETY: this process's pseudo handle; a null job asks about any.
    unsafe { IsProcessInJob(GetCurrentProcess(), std::ptr::null_mut(), &mut inside) };
    if inside == 0 {
        return true;
    }
    let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    // SAFETY: a null job is this process's own; a structure of the size given.
    let ok = unsafe {
        QueryInformationJobObject(
            std::ptr::null_mut(),
            JobObjectExtendedLimitInformation,
            (&raw mut info).cast(),
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    ok != 0
        && info.BasicLimitInformation.LimitFlags
            & (JOB_OBJECT_LIMIT_BREAKAWAY_OK | JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK)
            != 0
}
