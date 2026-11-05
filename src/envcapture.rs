//! The runner user's login environment. The daemon runs as a service with a bare environment, so it
//! captures `$SHELL -l -c 'env -0'` (falling back to /bin/sh) when it starts. A job's environment is
//! that, then the env of each `[[gpu]]` entry for a GPU it holds, then the spec's env (committed
//! potluck.toml and `--env`), then potluck's own variables, which nothing overrides (POTLUCK_JOB,
//! POTLUCK_LEASE, POTLUCK_WORKSPACE, POTLUCK_NODE, POTLUCK_GPUS, TMPDIR beside the workspace's tree
//! and removed with it, POTLUCK_HOME and POTLUCK_CONFIG). Nothing comes from the caller's shell.
//!
//! - The shell is `$SHELL`, else the user's shell from the password database, else /bin/sh. If it
//!   fails (a shell that refuses `-l -c`, a profile that exits early), /bin/sh is tried.
//! - The shell starts from a bare environment (HOME, USER, LOGNAME, SHELL, a default PATH, and the
//!   session variables a login would have: LANG, XDG_RUNTIME_DIR, DBUS_SESSION_BUS_ADDRESS), so
//!   whatever started the daemon doesn't leak into jobs.
//! - A profile that prints something is harmless: the variables are read after a marker line.
//! - A profile that hangs is killed (its whole process group) after a timeout.
//! - A process the profile leaves running in the background doesn't hold the capture up: the
//!   shell's output goes to unlinked files (mode 0600, under random names), not pipes that stay
//!   open while it runs.
//! - The shell's own bookkeeping (PWD, OLDPWD, SHLVL, `_`) is dropped.
//! - A variable whose name or value isn't UTF-8 can't be in a job's environment (it holds text):
//!   it is left out, and named in what [`login_env`] returns, for the daemon's log.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// How long a login shell may take to print its environment.
pub const TIMEOUT: Duration = Duration::from_secs(20);

/// Printed before the environment, so anything a profile prints before it is skipped.
const MARKER: &str = "__POTLUCK_LOGIN_ENV__";

/// Variables a shell sets for itself; they mean nothing to a job.
const DROPPED: &[&str] = &["PWD", "OLDPWD", "SHLVL", "_"];

/// Variables passed from the daemon's environment to the login shell: what a login session has
/// before any profile runs.
const SESSION: &[&str] = &[
    "HOME",
    "USER",
    "LOGNAME",
    "LANG",
    "XDG_RUNTIME_DIR",
    "DBUS_SESSION_BUS_ADDRESS",
];

const DEFAULT_PATH: &str = "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin";

/// The login environment, and the variables left out of it because they aren't UTF-8 (each said
/// in words).
pub fn login_env() -> anyhow::Result<(BTreeMap<String, String>, Vec<String>)> {
    let shell = user_shell();
    match capture(&shell, TIMEOUT) {
        Ok(env) => Ok(env),
        Err(e) if shell != Path::new("/bin/sh") => {
            capture(Path::new("/bin/sh"), TIMEOUT).map_err(|e2| {
                anyhow::anyhow!(
                    "could not capture the login environment: {}: {e:#}; /bin/sh: {e2:#}",
                    shell.display()
                )
            })
        }
        Err(e) => Err(e.context("could not capture the login environment")),
    }
}

/// `$SHELL`, else the password database's shell for this user, else /bin/sh.
pub fn user_shell() -> PathBuf {
    if let Some(s) = std::env::var_os("SHELL").filter(|s| !s.is_empty()) {
        return PathBuf::from(s);
    }
    passwd_shell().unwrap_or_else(|| PathBuf::from("/bin/sh"))
}

fn passwd_shell() -> Option<PathBuf> {
    let mut buf = vec![0u8; 16 * 1024];
    // SAFETY: getpwuid_r writes into `pwd` and `buf`, which outlive the call; `result` is set to
    // null or to `pwd`.
    unsafe {
        let mut pwd: libc::passwd = std::mem::zeroed();
        let mut result: *mut libc::passwd = std::ptr::null_mut();
        let r = libc::getpwuid_r(
            libc::getuid(),
            &mut pwd,
            buf.as_mut_ptr().cast(),
            buf.len(),
            &mut result,
        );
        if r != 0 || result.is_null() || pwd.pw_shell.is_null() {
            return None;
        }
        let shell = std::ffi::CStr::from_ptr(pwd.pw_shell).to_str().ok()?;
        (!shell.is_empty()).then(|| PathBuf::from(shell))
    }
}

/// Runs `shell -l -c` to print the environment, from a bare environment, killing it after
/// `timeout`. Also returns the variables left out because they aren't UTF-8.
pub fn capture(
    shell: &Path,
    timeout: Duration,
) -> anyhow::Result<(BTreeMap<String, String>, Vec<String>)> {
    let mut cmd = Command::new(shell);
    cmd.arg("-l")
        .arg("-c")
        .arg(format!("echo {MARKER}; exec env -0"))
        .env_clear()
        .env("SHELL", shell)
        .env("PATH", DEFAULT_PATH)
        .stdin(Stdio::null())
        // Its own process group, so a timeout kills whatever the profile started too.
        .process_group(0);
    // Its output goes to files, not pipes: a process the profile leaves running in the
    // background keeps the shell's stdout and stderr open, and a pipe would not end until that
    // process does.
    let mut out = scratch_file()?;
    let mut err = scratch_file()?;
    cmd.stdout(out.try_clone()?).stderr(err.try_clone()?);
    for k in SESSION {
        if let Some(v) = std::env::var_os(k) {
            cmd.env(k, v);
        }
    }
    if std::env::var_os("HOME").is_none()
        && let Some(home) = passwd_home()
    {
        cmd.env("HOME", home);
    }
    if let Some(dir) = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|d| d.is_dir())
    {
        cmd.current_dir(dir);
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("could not start {}: {e}", shell.display()))?;
    let pgid = child.id() as libc::pid_t;
    let start = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if start.elapsed() >= timeout {
            // SAFETY: signalling the process group we created; its leader is our unreaped child,
            // so the group id can't have been reused.
            unsafe { libc::kill(-pgid, libc::SIGKILL) };
            let _ = child.wait();
            anyhow::bail!(
                "{} -l did not finish within {}s",
                shell.display(),
                timeout.as_secs()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let read_all = |f: &mut File| -> Vec<u8> {
        let mut v = Vec::new();
        if f.seek(SeekFrom::Start(0)).is_ok() {
            let _ = f.read_to_end(&mut v);
        }
        v
    };
    let (out, err) = (read_all(&mut out), read_all(&mut err));
    if !status.success() {
        anyhow::bail!(
            "{} -l exited with {status}: {}",
            shell.display(),
            String::from_utf8_lossy(&err).trim()
        );
    }
    parse(&out).ok_or_else(|| {
        anyhow::anyhow!(
            "{} -l printed no environment: {}",
            shell.display(),
            String::from_utf8_lossy(&err).trim()
        )
    })
}

/// A new, empty file only this user can read, already unlinked (the environment may hold secrets).
fn scratch_file() -> anyhow::Result<File> {
    scratch_file_in(&std::env::temp_dir())
}

/// [`scratch_file`] in `dir`, under a random name: on Linux the temp directory is usually /tmp,
/// which every user shares, and a name another user can guess (the pid's) is one they can take
/// first.
fn scratch_file_in(dir: &Path) -> anyhow::Result<File> {
    use ring::rand::SecureRandom;
    let random = ring::rand::SystemRandom::new();
    for _ in 0..16 {
        let mut bytes = [0u8; 16];
        random
            .fill(&mut bytes)
            .map_err(|_| anyhow::anyhow!("no random numbers from the system"))?;
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let path = dir.join(format!("potluck-login-env-{hex}"));
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(f) => {
                let _ = std::fs::remove_file(&path);
                return Ok(f);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => anyhow::bail!("could not create a file in {}: {e}", dir.display()),
        }
    }
    anyhow::bail!(
        "could not create a file in {}: every random name tried was taken",
        dir.display()
    )
}

fn passwd_home() -> Option<PathBuf> {
    let mut buf = vec![0u8; 16 * 1024];
    // SAFETY: as in passwd_shell.
    unsafe {
        let mut pwd: libc::passwd = std::mem::zeroed();
        let mut result: *mut libc::passwd = std::ptr::null_mut();
        let r = libc::getpwuid_r(
            libc::getuid(),
            &mut pwd,
            buf.as_mut_ptr().cast(),
            buf.len(),
            &mut result,
        );
        if r != 0 || result.is_null() || pwd.pw_dir.is_null() {
            return None;
        }
        let dir = std::ffi::CStr::from_ptr(pwd.pw_dir).to_str().ok()?;
        (!dir.is_empty()).then(|| PathBuf::from(dir))
    }
}

/// The variables after the marker line, without the shell's bookkeeping, and those left out
/// because they aren't UTF-8. `None` if the marker is missing.
fn parse(out: &[u8]) -> Option<(BTreeMap<String, String>, Vec<String>)> {
    let marker = format!("{MARKER}\n");
    let at = out
        .windows(marker.len())
        .position(|w| w == marker.as_bytes())?;
    let body = &out[at + marker.len()..];
    let mut env = BTreeMap::new();
    let mut dropped = Vec::new();
    for entry in body.split(|&b| b == 0) {
        let Ok(entry) = std::str::from_utf8(entry) else {
            let name = entry.split(|&b| b == b'=').next().unwrap_or_default();
            match std::str::from_utf8(name) {
                Ok(name) => dropped.push(format!("{name}: its value is not UTF-8")),
                Err(_) => dropped.push(format!(
                    "{}: its name is not UTF-8",
                    String::from_utf8_lossy(name)
                )),
            }
            continue;
        };
        let Some((k, v)) = entry.split_once('=') else {
            continue;
        };
        if k.is_empty() || DROPPED.contains(&k) {
            continue;
        }
        env.insert(k.to_string(), v.to_string());
    }
    Some((env, dropped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// The temp directory is shared on Linux (/tmp, unless TMPDIR is set; a systemd user service
    /// has none): names another user took there first don't stop the capture, and so don't stop
    /// the daemon from starting.
    #[test]
    fn names_taken_in_the_temp_dir_do_not_stop_the_capture() {
        let dir = tempfile::tempdir().unwrap();
        for n in 0..=1000 {
            let name = format!("potluck-login-env-{}-{n}", std::process::id());
            File::create(dir.path().join(name)).unwrap();
        }
        let mut f = scratch_file_in(dir.path()).unwrap();
        std::io::Write::write_all(&mut f, b"secret").unwrap();
        // Unlinked: only the 1001 files put there before are left.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1001);
    }

    /// A fake login shell: a script that runs `profile`, then the command it was given.
    fn fake_shell(dir: &Path, profile: &str) -> PathBuf {
        let path = dir.join("fakesh");
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\n# called as: fakesh -l -c CMD\n[ \"$1\" = -l ] || exit 64\n{profile}\nshift 2\nexec /bin/sh -c \"$1\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// `capture` of a script the test just wrote. On Linux, exec can fail with ETXTBSY while
    /// another test thread's fork still holds the script's write fd; that is retried.
    fn capture_new(shell: &Path, timeout: Duration) -> anyhow::Result<BTreeMap<String, String>> {
        for _ in 0..50 {
            match capture(shell, timeout) {
                Err(e) if e.to_string().contains("Text file busy") => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                r => return r.map(|(env, _)| env),
            }
        }
        capture(shell, timeout).map(|(env, _)| env)
    }

    #[test]
    fn captures_profile_exports_and_drops_bookkeeping() {
        let dir = tempfile::tempdir().unwrap();
        let shell = fake_shell(
            dir.path(),
            "echo 'Welcome, a profile that talks'\nexport FROM_PROFILE='a b=c'\nexport PATH=\"/opt/x/bin:$PATH\"\ncd /\n",
        );
        let env = capture_new(&shell, Duration::from_secs(10)).unwrap();
        assert_eq!(env.get("FROM_PROFILE").map(String::as_str), Some("a b=c"));
        assert!(env["PATH"].starts_with("/opt/x/bin:"), "{}", env["PATH"]);
        assert_eq!(env.get("SHELL").map(PathBuf::from), Some(shell.clone()));
        for k in DROPPED {
            assert!(!env.contains_key(*k), "{k} kept");
        }
        assert!(
            !env.keys().any(|k| k.contains("Welcome")),
            "profile output before the marker is skipped"
        );
    }

    #[test]
    fn nothing_leaks_from_the_daemons_environment() {
        // CARGO_* variables are set in every test process; a login shell must not see them.
        assert!(std::env::vars().any(|(k, _)| k.starts_with("CARGO")));
        let dir = tempfile::tempdir().unwrap();
        let env = capture_new(&fake_shell(dir.path(), ""), Duration::from_secs(10)).unwrap();
        assert!(
            !env.keys()
                .any(|k| k.starts_with("CARGO") || k == "RUSTFLAGS"),
            "{env:?}"
        );
        if let Some(home) = std::env::var_os("HOME") {
            assert_eq!(env.get("HOME").map(Into::into), Some(home));
        }
    }

    #[test]
    fn a_hanging_profile_is_killed() {
        let dir = tempfile::tempdir().unwrap();
        let shell = fake_shell(dir.path(), "sleep 30");
        let start = Instant::now();
        let err = capture_new(&shell, Duration::from_millis(300))
            .unwrap_err()
            .to_string();
        assert!(err.contains("did not finish"), "{err}");
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn a_profile_that_leaves_a_process_running_does_not_hold_it_up() {
        // The background process keeps the shell's stdout and stderr open after the shell exits.
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("bg.pid");
        let shell = fake_shell(
            dir.path(),
            &format!(
                "sleep 30 &\necho $! > '{}'\nexport AFTER_BG=yes\n",
                pidfile.display()
            ),
        );
        let start = Instant::now();
        let env = capture_new(&shell, Duration::from_secs(20));
        let took = start.elapsed();
        if let Ok(pid) = std::fs::read_to_string(&pidfile) {
            // SAFETY: signals the sleep this test started (its parent, init or launchd, reaps it).
            unsafe { libc::kill(pid.trim().parse().unwrap(), libc::SIGKILL) };
        }
        assert!(took < Duration::from_secs(10), "{took:?}");
        assert_eq!(
            env.unwrap().get("AFTER_BG").map(String::as_str),
            Some("yes")
        );
    }

    #[test]
    fn failing_shells_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let err = capture_new(&fake_shell(dir.path(), "exit 3"), Duration::from_secs(10))
            .unwrap_err()
            .to_string();
        assert!(err.contains("exited"), "{err}");
        let err = capture(&dir.path().join("nope"), Duration::from_secs(10))
            .unwrap_err()
            .to_string();
        assert!(err.contains("could not start"), "{err}");
    }

    #[test]
    fn the_real_login_shell_works() {
        // This machine's own login shell and /bin/sh both give a usable environment.
        for shell in [user_shell(), PathBuf::from("/bin/sh")] {
            let (env, _) = capture(&shell, TIMEOUT).unwrap();
            assert!(env.contains_key("PATH"), "{}: {env:?}", shell.display());
            assert!(!env.contains_key("PWD") && !env.contains_key("SHLVL"));
        }
        assert!(login_env().unwrap().0.contains_key("HOME"));
    }

    #[test]
    fn parse_needs_the_marker() {
        assert!(parse(b"A=1\0").is_none());
        let (env, dropped) =
            parse(format!("noise\n{MARKER}\nA=1\0B==2\0bad\0_=/usr/bin/env\0").as_bytes()).unwrap();
        assert_eq!(env.len(), 2);
        assert_eq!(env["B"], "=2");
        assert!(dropped.is_empty());
    }

    /// A job's environment holds text: a variable that isn't UTF-8 is left out, and named.
    #[test]
    fn variables_that_are_not_utf8_are_left_out_and_named() {
        let mut out = format!("{MARKER}\nA=1\0LATIN=caf").into_bytes();
        out.extend_from_slice(b"\xe9\0N\xffX=1\0B=2\0");
        let (env, dropped) = parse(&out).unwrap();
        assert_eq!(env.keys().collect::<Vec<_>>(), ["A", "B"]);
        assert_eq!(
            dropped,
            [
                "LATIN: its value is not UTF-8",
                "N\u{fffd}X: its name is not UTF-8"
            ]
        );
    }
}
