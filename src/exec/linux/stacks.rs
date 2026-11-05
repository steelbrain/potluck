//! Stacks of a stalled invocation, for its log. Best effort and time-bounded.
//!
//! Per process: name, command line, state, wait channel and threads from /proc; the kernel stack
//! when /proc/PID/stack is readable (it needs CAP_SYS_ADMIN, so normally not); user stacks from
//! eu-stack or gdb when installed and allowed to attach. Under Yama's ptrace_scope=1 (the default
//! on most distributions) only a process's ancestors may attach, and the daemon is not one (the
//! user manager forks every unit), so the user stack section then says why it is missing.
//!
//! The debuggers run with DEBUGINFOD_URLS removed: they must not fetch symbols over the network.

use super::cgroup::Stats;
use super::run;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// At most this many processes get a section; the rest are listed by pid.
const MAX_PROCESSES: usize = 64;
/// Threads listed per process.
const MAX_THREADS: usize = 32;
/// Frames per thread in user stacks.
const MAX_FRAMES: &str = "64";
/// The longest one debugger run may take.
const TOOL_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) fn collect(unit: &str, stats: &Stats, budget: Duration) -> String {
    let deadline = Instant::now() + budget;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "stacks of {unit}: {} process{}, cpu {:.1}s, memory {} MiB",
        stats.pids.len(),
        if stats.pids.len() == 1 { "" } else { "es" },
        stats.cpu_usec as f64 / 1e6,
        stats.mem_bytes >> 20
    );
    let mut kernel = KernelStacks::Unknown;
    let mut user = UserStacks::find();
    for (i, &pid) in stats.pids.iter().enumerate() {
        if i == MAX_PROCESSES {
            let rest: Vec<String> = stats.pids[i..].iter().map(u32::to_string).collect();
            let _ = writeln!(out, "and {} more: {}", rest.len(), rest.join(" "));
            break;
        }
        process(&mut out, pid, &mut kernel, &mut user, deadline);
    }
    if let KernelStacks::Unreadable(why) = &kernel {
        let _ = writeln!(out, "kernel stacks: not readable ({why})");
    }
    if let Some(note) = user.note() {
        let _ = writeln!(out, "user stacks: {note}");
    }
    out
}

fn process(
    out: &mut String,
    pid: u32,
    kernel: &mut KernelStacks,
    user: &mut UserStacks,
    deadline: Instant,
) {
    let proc_dir = PathBuf::from(format!("/proc/{pid}"));
    let Some(task) = Task::read(&proc_dir) else {
        let _ = writeln!(out, "pid {pid}: gone");
        return;
    };
    let threads = std::fs::read_dir(proc_dir.join("task"))
        .map(|d| {
            let mut tids: Vec<u32> = d
                .flatten()
                .filter_map(|e| e.file_name().to_str()?.parse().ok())
                .collect();
            tids.sort_unstable();
            tids
        })
        .unwrap_or_default();
    let _ = writeln!(
        out,
        "pid {pid} {}: {}, {} thread{}",
        task.comm,
        task.describe(),
        threads.len().max(1),
        if threads.len() > 1 { "s" } else { "" }
    );
    let _ = writeln!(out, "  cmdline: {}", cmdline(&proc_dir));
    if threads.len() > 1 {
        for &tid in threads.iter().filter(|&&t| t != pid).take(MAX_THREADS) {
            if let Some(t) = Task::read(&proc_dir.join("task").join(tid.to_string())) {
                let _ = writeln!(out, "  thread {tid} {}: {}", t.comm, t.describe());
            }
        }
        if threads.len() > MAX_THREADS + 1 {
            let _ = writeln!(out, "  ({} more threads)", threads.len() - MAX_THREADS - 1);
        }
    }
    kernel.add(out, &proc_dir);
    // A debugger's attach queues a SIGSTOP that a thread in uninterruptible sleep takes only when
    // it wakes, so the debugger would wait as long and be killed at its time limit. Leave such a
    // process alone.
    let uninterruptible = task.uninterruptible()
        || threads.iter().any(|&tid| {
            Task::read(&proc_dir.join("task").join(tid.to_string()))
                .is_some_and(|t| t.uninterruptible())
        });
    if !uninterruptible {
        user.add(out, pid, deadline);
    } else if matches!(user, UserStacks::Tool { .. }) {
        let _ = writeln!(
            out,
            "  user stack: not taken, a thread is in uninterruptible sleep (a debugger would wait \
             until it wakes)"
        );
    }
}

/// What /proc says about a process or thread.
struct Task {
    comm: String,
    /// `S (sleeping)`, `D (disk sleep)`, ...
    state: String,
    /// The kernel function it waits in, if any.
    wchan: Option<String>,
}

impl Task {
    fn read(dir: &Path) -> Option<Task> {
        let status = std::fs::read_to_string(dir.join("status")).ok()?;
        let field = |name: &str| {
            status
                .lines()
                .find_map(|l| l.strip_prefix(name))
                .map(|v| v.trim().to_string())
                .unwrap_or_default()
        };
        let wchan = std::fs::read_to_string(dir.join("wchan"))
            .ok()
            .map(|w| w.trim().to_string())
            .filter(|w| !w.is_empty() && w != "0");
        Some(Task {
            comm: field("Name:"),
            state: field("State:"),
            wchan,
        })
    }

    /// `D`: waiting in the kernel where signals don't wake it.
    fn uninterruptible(&self) -> bool {
        self.state.starts_with('D')
    }

    fn describe(&self) -> String {
        match &self.wchan {
            Some(w) => format!("{} in {w}", self.state),
            None => self.state.clone(),
        }
    }
}

/// The command line, shell-quoted where needed, cut at 500 bytes.
fn cmdline(proc_dir: &Path) -> String {
    let Ok(raw) = std::fs::read(proc_dir.join("cmdline")) else {
        return "?".into();
    };
    let args: Vec<String> = raw
        .split(|&b| b == 0)
        .filter(|a| !a.is_empty())
        .map(|a| quote(&String::from_utf8_lossy(a)))
        .collect();
    let mut line = args.join(" ");
    if line.is_empty() {
        return "(none)".into();
    }
    if line.len() > 500 {
        let mut cut = 500;
        while !line.is_char_boundary(cut) {
            cut -= 1;
        }
        line.truncate(cut);
        line.push_str(" ...");
    }
    line
}

fn quote(arg: &str) -> String {
    let plain = arg
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-_./=:,+@%".contains(&b));
    if plain {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', r"'\''"))
    }
}

enum KernelStacks {
    Unknown,
    Readable,
    Unreadable(String),
}

impl KernelStacks {
    fn add(&mut self, out: &mut String, proc_dir: &Path) {
        if matches!(self, KernelStacks::Unreadable(_)) {
            return;
        }
        match std::fs::read_to_string(proc_dir.join("stack")) {
            Ok(stack) => {
                *self = KernelStacks::Readable;
                let _ = writeln!(out, "  kernel stack:");
                for line in stack.lines() {
                    let _ = writeln!(out, "    {line}");
                }
            }
            // Gone in between, or no stack to show: nothing to say for this one.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                if matches!(self, KernelStacks::Unknown) {
                    *self = KernelStacks::Unreadable(format!("/proc/PID/stack: {e}"));
                }
            }
        }
    }
}

enum UserStacks {
    Tool {
        program: PathBuf,
        kind: Debugger,
    },
    /// No more attempts; the reason goes at the end of the output.
    Stopped(String),
}

#[derive(Clone, Copy)]
enum Debugger {
    EuStack,
    Gdb,
}

impl UserStacks {
    fn find() -> UserStacks {
        if let Some(program) = find_program("eu-stack") {
            UserStacks::Tool {
                program,
                kind: Debugger::EuStack,
            }
        } else if let Some(program) = find_program("gdb") {
            UserStacks::Tool {
                program,
                kind: Debugger::Gdb,
            }
        } else {
            UserStacks::Stopped("neither eu-stack (elfutils) nor gdb is installed".into())
        }
    }

    fn note(&self) -> Option<&str> {
        match self {
            UserStacks::Stopped(why) => Some(why),
            _ => None,
        }
    }

    fn add(&mut self, out: &mut String, pid: u32, deadline: Instant) {
        let UserStacks::Tool { program, kind } = self else {
            return;
        };
        let (program, kind) = (program.clone(), *kind);
        let left = deadline.saturating_duration_since(Instant::now());
        if left < Duration::from_millis(500) {
            *self = UserStacks::Stopped("out of time for more stacks".into());
            return;
        }
        let mut cmd = Command::new(&program);
        let name = match kind {
            Debugger::EuStack => {
                cmd.args(["-n", MAX_FRAMES, "-p", &pid.to_string()]);
                "eu-stack"
            }
            Debugger::Gdb => {
                cmd.args(["-nx", "-batch", "-q"])
                    .args(["-iex", "set debuginfod enabled off"])
                    .args(["-ex", &format!("thread apply all bt {MAX_FRAMES}")])
                    .args(["-p", &pid.to_string()]);
                "gdb"
            }
        };
        cmd.env_remove("DEBUGINFOD_URLS");
        let stopped_before = stopped_or_stop_queued(pid);
        let output = match run::run(&mut cmd, left.min(TOOL_TIMEOUT)) {
            Ok(o) => o,
            Err(e) => {
                *self = UserStacks::Stopped(format!("could not run {name}: {e}"));
                return;
            }
        };
        // Killed at its time limit, the debugger may leave behind the SIGSTOP its attach queued:
        // still queued (a thread in uninterruptible sleep takes it only when it wakes) or about to
        // take effect (the debugger died before it took the stop). Either would stop the job for
        // good. A process that was stopped before is left as it was.
        let continued = output.status.is_none() && !stopped_before && undo_attach_stop(pid);
        let text = format!("{}{}", output.stdout, output.stderr);
        if text.contains("Operation not permitted") {
            *self = UserStacks::Stopped(format!(
                "{name} may not attach ({}): only a process's ancestors may ptrace it here, and \
                 the user manager, not the daemon, started the job",
                ptrace_scope()
            ));
            return;
        }
        if output.status.is_none() {
            let _ = writeln!(
                out,
                "  user stack ({name}): timed out{}",
                if continued {
                    "; it had left the process stopped, continued it (SIGCONT)"
                } else {
                    ""
                }
            );
            return;
        }
        let _ = writeln!(out, "  user stack ({name}):");
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let _ = writeln!(out, "    {line}");
        }
    }
}

/// Whether a thread of `pid` is stopped (state T) or has a SIGSTOP queued.
fn stopped_or_stop_queued(pid: u32) -> bool {
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return false;
    };
    let sigstop = 1u64 << (libc::SIGSTOP - 1);
    tasks.flatten().any(|task| {
        let Ok(status) = std::fs::read_to_string(task.path().join("status")) else {
            return false;
        };
        status.lines().any(|line| {
            if let Some(state) = line.strip_prefix("State:") {
                return state.trim_start().starts_with('T');
            }
            ["SigPnd:", "ShdPnd:"].iter().any(|field| {
                line.strip_prefix(field)
                    .and_then(|mask| u64::from_str_radix(mask.trim(), 16).ok())
                    .is_some_and(|mask| mask & sigstop != 0)
            })
        })
    })
}

/// Continues `pid` if it is stopped or has a SIGSTOP queued, watching for a second: a stop the
/// killed debugger left in flight takes effect only once the process runs again. SIGCONT drops
/// queued stop signals and wakes a stopped process. True if it sent one.
fn undo_attach_stop(pid: u32) -> bool {
    let until = Instant::now() + Duration::from_secs(1);
    let mut continued = false;
    loop {
        if stopped_or_stop_queued(pid) {
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGCONT) };
            continued = true;
        }
        if Instant::now() >= until {
            return continued;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn ptrace_scope() -> String {
    match std::fs::read_to_string("/proc/sys/kernel/yama/ptrace_scope") {
        Ok(v) => format!("kernel.yama.ptrace_scope = {}", v.trim()),
        Err(_) => "ptrace not permitted".into(),
    }
}

fn find_program(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_else(|| "/usr/local/bin:/usr/bin:/bin".into());
    std::env::split_paths(&path)
        .map(|dir| dir.join(name))
        .find(|p| is_executable(p))
}

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_only_what_needs_it() {
        assert_eq!(quote("sleep"), "sleep");
        assert_eq!(quote("/usr/bin/python3"), "/usr/bin/python3");
        assert_eq!(quote("a b"), "'a b'");
        assert_eq!(quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn describes_a_process() {
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        // Until it reaches its nanosleep it runs (the loader); under load that can take a while.
        wait_state(child.id(), "asleep", |s| s.starts_with('S'));
        let stats = Stats {
            pids: vec![child.id(), u32::MAX],
            ..Default::default()
        };
        let text = collect("x.service", &stats, Duration::from_secs(20));
        // Our own child, not yet reaped: its pid is still ours.
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            text.starts_with("stacks of x.service: 2 processes,"),
            "{text}"
        );
        assert!(
            text.contains(&format!("pid {} sleep: S (sleeping)", child.id())),
            "{text}"
        );
        assert!(text.contains("cmdline: sleep 30"), "{text}");
        assert!(text.contains(&format!("pid {}: gone", u32::MAX)), "{text}");
    }

    fn state(pid: u32) -> String {
        Task::read(Path::new(&format!("/proc/{pid}"))).map_or_else(String::new, |t| t.state)
    }

    fn wait_state(pid: u32, what: &str, ok: impl Fn(&str) -> bool) -> String {
        let started = Instant::now();
        loop {
            let s = state(pid);
            if ok(&s) {
                return s;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "pid {pid} never {what}: {s}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn sees_a_stopped_process() {
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        wait_state(child.id(), "slept", |s| s.starts_with('S'));
        assert!(!stopped_or_stop_queued(child.id()));
        unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGSTOP) };
        wait_state(child.id(), "stopped", |s| s.starts_with('T'));
        assert!(stopped_or_stop_queued(child.id()));
        assert!(undo_attach_stop(child.id()));
        wait_state(child.id(), "continued", |s| s.starts_with('S'));
        // Our own child, not yet reaped: its pid is still ours.
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// A process in uninterruptible sleep (state D) until its stdin closes: a vfork parent waits
    /// so until its child exits, and the child reads stdin to the end. The test decides when it
    /// wakes, so a slow machine can't end the sleep early. The child writes a byte first: a fresh
    /// process can also be in D for a moment while it starts, and only the vfork wait counts.
    /// PR_SET_PTRACER_ANY lets a debugger attach under Yama's ptrace_scope=1 too. None (and why
    /// on stderr) when the test can't run.
    fn in_uninterruptible_sleep() -> Option<(std::process::Child, tempfile::TempDir)> {
        if UserStacks::find().note().is_some() {
            eprintln!("skipped: neither eu-stack nor gdb is installed");
            return None;
        }
        let Some(cc) = find_program("cc") else {
            eprintln!("skipped: no C compiler");
            return None;
        };
        let dir = tempfile::tempdir().unwrap();
        let source = r#"
            #include <sys/prctl.h>
            #include <unistd.h>
            int main(void) {
                prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
                if (vfork() == 0) {
                    char c = 'v';
                    if (write(1, &c, 1) != 1) _exit(1);
                    while (read(0, &c, 1) > 0) {}
                    _exit(0);
                }
                for (;;) sleep(1);
            }
        "#;
        std::fs::write(dir.path().join("vfork.c"), source).unwrap();
        let program = dir.path().join("vfork");
        let built = Command::new(cc)
            .arg("-o")
            .arg(&program)
            .arg(dir.path().join("vfork.c"))
            .status()
            .unwrap();
        assert!(built.success());
        let mut child = Command::new(&program)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut vforked = [0u8];
        std::io::Read::read_exact(child.stdout.as_mut().unwrap(), &mut vforked).unwrap();
        wait_state(child.id(), "waited for its vfork child", |s| {
            s.starts_with('D')
        });
        Some((child, dir))
    }

    /// Wakes the process from `in_uninterruptible_sleep`, gives a stop left pending time to take
    /// effect, and returns its state then.
    fn wake(child: &mut std::process::Child) -> String {
        drop(child.stdin.take());
        wait_state(child.id(), "woke up", |s| !s.starts_with('D'));
        std::thread::sleep(Duration::from_millis(500));
        let after = state(child.id());
        // Our own child, not yet reaped: its pid is still ours.
        child.kill().unwrap();
        child.wait().unwrap();
        after
    }

    /// PTRACE_ATTACH queues a SIGSTOP that a process in uninterruptible sleep takes only when it
    /// wakes, so a debugger would only wait for it until its time limit: such a process gets no
    /// user stack.
    #[test]
    fn a_process_in_uninterruptible_sleep_is_not_left_stopped() {
        let Some((mut child, _dir)) = in_uninterruptible_sleep() else {
            return;
        };
        let stats = Stats {
            pids: vec![child.id()],
            ..Default::default()
        };
        let text = collect("x.service", &stats, Duration::from_secs(2));
        println!("{text}");
        let after = wake(&mut child);
        assert!(!after.starts_with('T'), "left stopped: {after}\n{text}");
        assert!(text.contains("uninterruptible"), "{text}");
    }

    /// A debugger killed at its time limit before it took the SIGSTOP its attach queued leaves
    /// that SIGSTOP behind, and the process would stop for good when it took it. Here the
    /// debugger attaches to a process in uninterruptible sleep, which a process can also enter
    /// just after `collect` looked at it.
    #[test]
    fn a_debugger_that_times_out_does_not_leave_the_process_stopped() {
        let Some((mut child, _dir)) = in_uninterruptible_sleep() else {
            return;
        };
        let mut user = UserStacks::find();
        let mut text = String::new();
        user.add(
            &mut text,
            child.id(),
            Instant::now() + Duration::from_secs(2),
        );
        println!("{text}");
        let after = wake(&mut child);
        assert!(!after.starts_with('T'), "left stopped: {after}\n{text}");
        assert!(
            text.contains("timed out; it had left the process stopped"),
            "{text}"
        );
    }
}
