//! `potluck service install|uninstall`: the daemon as a systemd user service (Linux; lingering must
//! be on, see [`crate::preflight`]) or a LaunchAgent with ProcessType=Interactive (macOS; Standard or
//! unspecified throttles CPU and I/O).
//!
//! - Linux: `~/.config/systemd/user/potluck.service` running `<this executable> daemon` with
//!   `Restart=on-failure`, then `systemctl --user daemon-reload` and `enable --now`. Refused while
//!   lingering is off: without it systemd stops the user's services, and so the daemon and its jobs,
//!   when the user's last session ends.
//! - macOS: `~/Library/LaunchAgents/potluck.daemon.plist` (RunAtLoad, KeepAlive,
//!   ProcessType=Interactive, AbandonProcessGroup so stopping the daemon never takes jobs with it,
//!   stdout and stderr to the daemon log), then `launchctl bootstrap gui/<uid>`.
//!
//! The directory and the name can be overridden (`POTLUCK_SERVICE_DIR`, `POTLUCK_SERVICE_NAME`), for
//! tests that must not touch the real service; an update's restart of the daemon
//! ([`crate::update::restart_daemon`]) uses the same.

use crate::config::{LAUNCHD_LABEL, Paths, SYSTEMD_UNIT};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Which service manager a spec is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Manager {
    Systemd,
    Launchd,
}

/// Everything that decides the installed service.
#[derive(Clone, Debug)]
pub struct ServiceSpec {
    pub manager: Manager,
    /// Where the unit file or plist goes.
    pub dir: PathBuf,
    /// The unit name (`potluck.service`) or the LaunchAgent label (`potluck.daemon`).
    pub name: String,
    /// The potluck executable the service runs as `<exe> daemon`.
    pub exe: PathBuf,
    /// The daemon log (macOS: launchd sends stdout and stderr there).
    pub log: PathBuf,
    /// Variables the daemon needs to find the same state as this client (POTLUCK_HOME and
    /// POTLUCK_CONFIG when set).
    pub env: BTreeMap<String, String>,
    /// Run systemctl or launchctl. Off only in tests that look at the files.
    pub manage: bool,
}

/// What install or uninstall did.
#[derive(Clone, Debug, Serialize)]
pub struct ServiceDone {
    pub manager: Manager,
    pub name: String,
    pub file: PathBuf,
    /// Installed and started, or stopped and removed.
    pub changed: bool,
    pub message: String,
}

/// The running executable's canonical path (symlinks resolved): the file `self-update` replaces,
/// so a restarted service runs the new binary rather than an old copy behind a link.
pub fn this_executable() -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe()
        .map_err(|e| anyhow::anyhow!("finding the running potluck executable: {e}"))?;
    // Linux names a replaced, still running binary "<path> (deleted)"; the service should run the
    // file now at <path>.
    let exe = match exe.to_str().and_then(|s| s.strip_suffix(" (deleted)")) {
        Some(stripped) if !exe.exists() => PathBuf::from(stripped),
        _ => exe,
    };
    std::fs::canonicalize(&exe)
        .map_err(|e| anyhow::anyhow!("resolving the potluck executable {}: {e}", exe.display()))
}

/// A variable's value as the service file writes it: refused unless it is UTF-8 (the file would
/// name another path).
fn env_value(name: &str, v: &std::ffi::OsStr) -> anyhow::Result<String> {
    v.to_str().map(str::to_string).ok_or_else(|| {
        anyhow::anyhow!(
            "{name} {v:?} is not UTF-8, so the service file can't name it: use a UTF-8 path"
        )
    })
}

/// Refuses what a unit file or plist can't hold as it is: text that isn't UTF-8, or that holds a
/// control character (a line break would end a unit file's line and start another).
fn writable(what: &str, v: &std::ffi::OsStr) -> anyhow::Result<()> {
    match v.to_str() {
        None => anyhow::bail!(
            "{what} {v:?} is not UTF-8, so the service file can't name it: use a UTF-8 path"
        ),
        Some(s) if s.chars().any(char::is_control) => anyhow::bail!(
            "{what} {s:?} holds a control character, which the service file can't hold: use a \
             path without one"
        ),
        Some(_) => Ok(()),
    }
}

/// Checks everything the service file will name ([`writable`]).
fn check_writable(spec: &ServiceSpec) -> anyhow::Result<()> {
    writable("the potluck executable", spec.exe.as_os_str())?;
    // systemd refuses a unit whose program path holds a quote or a backslash, however quoted
    // ("Executable path contains special characters").
    if spec.manager == Manager::Systemd
        && let Some(path) = spec.exe.to_str()
        && path.contains(['"', '\'', '\\'])
    {
        anyhow::bail!(
            "the potluck executable {path} has a quote or a backslash in its path, which systemd \
             refuses in the program a unit runs: move potluck to a path without one"
        );
    }
    writable("the daemon log", spec.log.as_os_str())?;
    for (k, v) in &spec.env {
        writable(k, std::ffi::OsStr::new(v))?;
    }
    Ok(())
}

/// This machine's service manager.
pub fn manager() -> Manager {
    if cfg!(target_os = "macos") {
        Manager::Launchd
    } else {
        Manager::Systemd
    }
}

/// The daemon's service name: the systemd unit (`potluck.service`) or the LaunchAgent's label
/// (`potluck.daemon`), or `POTLUCK_SERVICE_NAME`.
pub fn service_name() -> String {
    let var = std::env::var("POTLUCK_SERVICE_NAME")
        .ok()
        .filter(|v| !v.is_empty());
    match (var, manager()) {
        (Some(n), Manager::Systemd) if !n.ends_with(".service") => format!("{n}.service"),
        (Some(n), _) => n,
        (None, Manager::Systemd) => SYSTEMD_UNIT.to_string(),
        (None, Manager::Launchd) => LAUNCHD_LABEL.to_string(),
    }
}

/// Where the daemon's service file goes: `~/.config/systemd/user` or `~/Library/LaunchAgents`, or
/// `POTLUCK_SERVICE_DIR`. None without HOME.
pub fn service_dir() -> Option<PathBuf> {
    if let Some(d) = std::env::var_os("POTLUCK_SERVICE_DIR").filter(|d| !d.is_empty()) {
        return Some(PathBuf::from(d));
    }
    let home = PathBuf::from(std::env::var_os("HOME")?);
    Some(match manager() {
        Manager::Systemd => home.join(".config/systemd/user"),
        Manager::Launchd => home.join("Library/LaunchAgents"),
    })
}

impl ServiceSpec {
    /// The spec for this machine and executable, honouring `POTLUCK_SERVICE_DIR` and
    /// `POTLUCK_SERVICE_NAME`.
    pub fn for_this_machine(paths: &Paths) -> anyhow::Result<ServiceSpec> {
        let manager = manager();
        let dir = service_dir().ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
        let name = service_name();
        let exe = this_executable()?;
        let mut env = BTreeMap::new();
        for k in ["POTLUCK_HOME", "POTLUCK_CONFIG"] {
            if let Some(v) = std::env::var_os(k) {
                let v = PathBuf::from(v);
                let v = std::path::absolute(&v).unwrap_or(v);
                env.insert(k.to_string(), env_value(k, v.as_os_str())?);
            }
        }
        Ok(ServiceSpec {
            manager,
            dir,
            name,
            exe,
            log: paths.daemon_log(),
            env,
            manage: true,
        })
    }

    /// The unit file or plist.
    pub fn file(&self) -> PathBuf {
        match self.manager {
            Manager::Systemd => self.dir.join(&self.name),
            Manager::Launchd => self.dir.join(format!("{}.plist", self.name)),
        }
    }

    pub fn render(&self) -> String {
        match self.manager {
            Manager::Systemd => render_systemd_unit(self),
            Manager::Launchd => render_launchd_plist(self),
        }
    }
}

/// Quotes a word for a systemd unit file: `%` is escaped (specifiers), and words with spaces,
/// quotes or backslashes go in double quotes. `$` stays as it is: systemd expands variables only
/// in a command's arguments, not in its program path (a `$$` there names a file with `$$` in it)
/// nor in `Environment=`.
fn systemd_quote(s: &str) -> String {
    let s = s.replace('%', "%%");
    if s.chars()
        .any(|c| c.is_whitespace() || matches!(c, '"' | '\'' | '\\' | ';'))
    {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        s
    }
}

pub fn render_systemd_unit(spec: &ServiceSpec) -> String {
    let mut s = String::from(
        "[Unit]\nDescription=potluck daemon (build and test cluster)\n\n[Service]\nType=simple\n",
    );
    s.push_str(&format!(
        "ExecStart={} daemon\n",
        systemd_quote(&spec.exe.to_string_lossy())
    ));
    s.push_str("Restart=on-failure\nRestartSec=5\n");
    for (k, v) in &spec.env {
        s.push_str(&format!(
            "Environment={}\n",
            systemd_quote(&format!("{k}={v}"))
        ));
    }
    s.push_str("\n[Install]\nWantedBy=default.target\n");
    s
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn render_launchd_plist(spec: &ServiceSpec) -> String {
    let string = |v: &str| format!("<string>{}</string>", xml_escape(v));
    let mut s = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n<dict>\n",
    );
    s.push_str(&format!("\t<key>Label</key>\n\t{}\n", string(&spec.name)));
    s.push_str(&format!(
        "\t<key>ProgramArguments</key>\n\t<array>\n\t\t{}\n\t\t{}\n\t</array>\n",
        string(&spec.exe.to_string_lossy()),
        string("daemon")
    ));
    s.push_str("\t<key>RunAtLoad</key>\n\t<true/>\n");
    s.push_str("\t<key>KeepAlive</key>\n\t<true/>\n");
    s.push_str("\t<key>ProcessType</key>\n\t<string>Interactive</string>\n");
    s.push_str("\t<key>AbandonProcessGroup</key>\n\t<true/>\n");
    let log = spec.log.to_string_lossy();
    s.push_str(&format!(
        "\t<key>StandardOutPath</key>\n\t{}\n",
        string(&log)
    ));
    s.push_str(&format!(
        "\t<key>StandardErrorPath</key>\n\t{}\n",
        string(&log)
    ));
    if !spec.env.is_empty() {
        s.push_str("\t<key>EnvironmentVariables</key>\n\t<dict>\n");
        for (k, v) in &spec.env {
            s.push_str(&format!(
                "\t\t<key>{}</key>\n\t\t{}\n",
                xml_escape(k),
                string(v)
            ));
        }
        s.push_str("\t</dict>\n");
    }
    s.push_str("</dict>\n</plist>\n");
    s
}

/// A command for one of this user's service manager tools (`systemctl --user`, `systemd-run
/// --user`, `coredumpctl`, `loginctl`, `launchctl`), as the daemon, the CLI and updates all run
/// them. Linux: when XDG_RUNTIME_DIR isn't set (a shell with no login session, such as `su` or
/// cron), it is the user manager's own `/run/user/<uid>`, where systemd's tools find its bus;
/// no pager, no colours.
pub(crate) fn user_command(program: &str) -> Command {
    let mut cmd = Command::new(program);
    if cfg!(target_os = "linux") && std::env::var_os("XDG_RUNTIME_DIR").is_none() {
        // SAFETY: getuid has no preconditions and cannot fail.
        let uid = unsafe { libc::getuid() };
        cmd.env("XDG_RUNTIME_DIR", format!("/run/user/{uid}"));
    }
    cmd.env("SYSTEMD_PAGER", "").env("SYSTEMD_COLORS", "0");
    cmd
}

fn run(prog: &str, args: &[&str]) -> anyhow::Result<std::process::Output> {
    user_command(prog)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| anyhow::anyhow!("could not run {prog}: {e}"))
}

fn run_ok(prog: &str, args: &[&str]) -> anyhow::Result<()> {
    let out = run(prog, args)?;
    if !out.status.success() {
        anyhow::bail!(
            "{prog} {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// The program the installed daemon service runs (systemd's `ExecStart`, the LaunchAgent's first
/// argument), if a service is installed and says. `service install` writes the executable it ran
/// from; an update elsewhere leaves the service running that one.
pub fn installed_program() -> Option<PathBuf> {
    let name = service_name();
    if cfg!(target_os = "macos") {
        let plist = service_dir()?.join(format!("{name}.plist"));
        plist_program(&std::fs::read_to_string(plist).ok()?)
    } else {
        let out = run(
            "systemctl",
            &["--user", "show", "--property=ExecStart", "--value", &name],
        )
        .ok()
        .filter(|o| o.status.success())?;
        exec_start_program(&String::from_utf8_lossy(&out.stdout))
    }
}

/// The program in `systemctl show --property=ExecStart --value`'s
/// `{ path=/x/potluck ; argv[]=/x/potluck daemon ; ... }`.
pub fn exec_start_program(shown: &str) -> Option<PathBuf> {
    let rest = shown.trim().strip_prefix("{ path=")?;
    let (path, _) = rest.split_once(" ; argv[]=")?;
    (!path.is_empty()).then(|| PathBuf::from(path))
}

/// The first `ProgramArguments` string of a plist as [`render_launchd_plist`] writes it.
pub fn plist_program(plist: &str) -> Option<PathBuf> {
    let after = plist.split_once("<key>ProgramArguments</key>")?.1;
    let start = after.find("<string>")? + "<string>".len();
    let end = start + after[start..].find("</string>")?;
    let text = after[start..end]
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&amp;", "&");
    (!text.is_empty()).then(|| PathBuf::from(text))
}

/// Whether `a` and `b` are one file (symlinks followed).
pub fn same_file(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(x), Ok(y)) => x.dev() == y.dev() && x.ino() == y.ino(),
        _ => false,
    }
}

/// The user's login name.
pub fn user_name() -> anyhow::Result<String> {
    // SAFETY: getpwuid returns a pointer into static storage or null; we copy the name out at once.
    let name = unsafe {
        let pw = libc::getpwuid(libc::getuid());
        if pw.is_null() || (*pw).pw_name.is_null() {
            None
        } else {
            Some(
                std::ffi::CStr::from_ptr((*pw).pw_name)
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    };
    name.or_else(|| std::env::var("USER").ok())
        .ok_or_else(|| anyhow::anyhow!("can't tell this user's name"))
}

/// Whether systemd keeps `user`'s services running without a session (loginctl enable-linger).
pub fn linger_enabled(user: &str) -> bool {
    if let Ok(out) = run(
        "loginctl",
        &["show-user", user, "--property=Linger", "--value"],
    ) && out.status.success()
    {
        return String::from_utf8_lossy(&out.stdout).trim() == "yes";
    }
    Path::new("/var/lib/systemd/linger").join(user).exists()
}

/// Writes `text` to `path` through a temporary file and a rename. Returns whether it changed.
fn write_file(path: &Path, text: &str) -> anyhow::Result<bool> {
    if std::fs::read_to_string(path).is_ok_and(|old| old == text) {
        return Ok(false);
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent directory", path.display()))?;
    std::fs::create_dir_all(parent).map_err(|e| anyhow::anyhow!("{}: {e}", parent.display()))?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text).map_err(|e| anyhow::anyhow!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    Ok(true)
}

/// Whether the service is running (systemd: active) or loaded (launchd).
pub fn active(spec: &ServiceSpec) -> bool {
    match spec.manager {
        Manager::Systemd => run("systemctl", &["--user", "is-active", "--quiet", &spec.name])
            .is_ok_and(|o| o.status.success()),
        Manager::Launchd => launchd_loaded(&spec.name),
    }
}

/// Writes the service file and (with `manage`) starts the service. Refused, with nothing
/// written, when a path or value the file names isn't UTF-8 or holds a control character.
pub fn install(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    check_writable(spec)?;
    match spec.manager {
        Manager::Systemd => install_systemd(spec),
        Manager::Launchd => install_launchd(spec),
    }
}

pub fn uninstall(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    match spec.manager {
        Manager::Systemd => uninstall_systemd(spec),
        Manager::Launchd => uninstall_launchd(spec),
    }
}

fn install_systemd(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    if spec.manage {
        let user = user_name()?;
        if !linger_enabled(&user) {
            anyhow::bail!(
                "lingering is off for {user}: without it systemd stops the daemon and its jobs when your last session ends. Turn it on with `sudo loginctl enable-linger {user}`, then run `potluck service install` again"
            );
        }
    }
    let file = spec.file();
    let existed = file.exists();
    let mut changed = write_file(&file, &render_systemd_unit(spec))?;
    if spec.manage {
        let was_running = active(spec);
        run_ok("systemctl", &["--user", "daemon-reload"])?;
        run_ok("systemctl", &["--user", "enable", "--now", &spec.name])?;
        if existed && changed {
            // A running daemon keeps the old ExecStart until restarted; jobs survive a restart.
            run_ok("systemctl", &["--user", "restart", &spec.name])?;
        }
        changed |= !was_running;
    }
    let logs = format!("journalctl --user -u {}", spec.name);
    Ok(installed(spec, &file, changed, &logs))
}

/// What `install` did: wrote the service file and started the service, or found both as they
/// should be and changed nothing.
fn installed(spec: &ServiceSpec, file: &Path, changed: bool, logs: &str) -> ServiceDone {
    let what = match spec.manager {
        Manager::Systemd => spec.name.clone(),
        Manager::Launchd => format!("LaunchAgent {}", spec.name),
    };
    let message = if !changed {
        format!(
            "{what} ({}) was already installed{}; nothing changed; logs: {logs}",
            file.display(),
            if spec.manage { " and running" } else { "" }
        )
    } else {
        format!(
            "installed {what} ({}){}; logs: {logs}",
            file.display(),
            if spec.manage { " and started it" } else { "" }
        )
    };
    ServiceDone {
        manager: spec.manager,
        name: spec.name.clone(),
        file: file.to_path_buf(),
        changed,
        message,
    }
}

fn uninstall_systemd(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    let file = spec.file();
    let existed = file.exists();
    if spec.manage && existed {
        run_ok("systemctl", &["--user", "disable", "--now", &spec.name])?;
    }
    if existed {
        std::fs::remove_file(&file).map_err(|e| anyhow::anyhow!("{}: {e}", file.display()))?;
    }
    if spec.manage {
        run_ok("systemctl", &["--user", "daemon-reload"])?;
        // Forget a failed state; there is nothing to reset when it ended cleanly.
        let _ = run("systemctl", &["--user", "reset-failed", &spec.name]);
    }
    Ok(ServiceDone {
        manager: spec.manager,
        name: spec.name.clone(),
        file: file.clone(),
        changed: existed,
        message: if existed {
            format!("stopped and removed {} ({})", spec.name, file.display())
        } else {
            format!(
                "{} is not installed ({} does not exist)",
                spec.name,
                file.display()
            )
        },
    })
}

fn gui_domain() -> String {
    // SAFETY: getuid has no preconditions.
    format!("gui/{}", unsafe { libc::getuid() })
}

fn launchd_loaded(label: &str) -> bool {
    run(
        "launchctl",
        &["print", &format!("{}/{label}", gui_domain())],
    )
    .is_ok_and(|o| o.status.success())
}

fn install_launchd(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    if let Some(dir) = spec.log.parent() {
        std::fs::create_dir_all(dir).map_err(|e| anyhow::anyhow!("{}: {e}", dir.display()))?;
    }
    let file = spec.file();
    let mut changed = write_file(&file, &render_launchd_plist(spec))?;
    let loaded = spec.manage && launchd_loaded(&spec.name);
    if spec.manage && (changed || !loaded) {
        let domain = gui_domain();
        let target = format!("{domain}/{}", spec.name);
        if loaded {
            // launchd reads the plist when it loads it. AbandonProcessGroup: the daemon's jobs
            // keep running while it restarts.
            run_ok("launchctl", &["bootout", &target])?;
        }
        let _ = run("launchctl", &["enable", &target]);
        let path = file.to_string_lossy().into_owned();
        // bootout returns before launchd has let go of the label, and launchd can take about 10 s
        // to settle (as after `kickstart -k`); keep trying for 15 s.
        let mut last = None;
        for _ in 0..60 {
            let out = run("launchctl", &["bootstrap", &domain, &path])?;
            if out.status.success() {
                last = None;
                break;
            }
            last = Some(String::from_utf8_lossy(&out.stderr).trim().to_string());
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
        if let Some(e) = last {
            anyhow::bail!("launchctl bootstrap {domain} {path} failed: {e}");
        }
        changed = true;
    }
    Ok(installed(
        spec,
        &file,
        changed,
        &spec.log.display().to_string(),
    ))
}

fn uninstall_launchd(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    let file = spec.file();
    let loaded = spec.manage && launchd_loaded(&spec.name);
    if loaded {
        run_ok(
            "launchctl",
            &["bootout", &format!("{}/{}", gui_domain(), spec.name)],
        )?;
    }
    let existed = file.exists();
    if existed {
        std::fs::remove_file(&file).map_err(|e| anyhow::anyhow!("{}: {e}", file.display()))?;
    }
    let changed = existed || loaded;
    Ok(ServiceDone {
        manager: spec.manager,
        name: spec.name.clone(),
        file: file.clone(),
        changed,
        message: if changed {
            format!(
                "stopped and removed LaunchAgent {} ({})",
                spec.name,
                file.display()
            )
        } else {
            format!(
                "{} is not installed ({} does not exist)",
                spec.name,
                file.display()
            )
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(manager: Manager, dir: &Path) -> ServiceSpec {
        let mut env = BTreeMap::new();
        env.insert("POTLUCK_HOME".to_string(), "/tmp/pl home/state".to_string());
        ServiceSpec {
            manager,
            dir: dir.to_path_buf(),
            name: match manager {
                Manager::Systemd => "potluck-test-unit.service".into(),
                Manager::Launchd => "potluck-test.label".into(),
            },
            exe: PathBuf::from("/opt/my tools/potluck"),
            log: PathBuf::from("/tmp/pl home/state/daemon.log"),
            env,
            manage: false,
        }
    }

    #[test]
    fn systemd_unit() {
        let dir = tempfile::tempdir().unwrap();
        let s = spec(Manager::Systemd, dir.path());
        let unit = render_systemd_unit(&s);
        assert!(
            unit.contains("\nExecStart=\"/opt/my tools/potluck\" daemon\n"),
            "{unit}"
        );
        assert!(unit.contains("\nRestart=on-failure\n"));
        assert!(unit.contains("\nEnvironment=\"POTLUCK_HOME=/tmp/pl home/state\"\n"));
        assert!(unit.contains("[Install]\nWantedBy=default.target\n"));
        assert_eq!(systemd_quote("/a/100%/$x"), "/a/100%%/$x");
        assert_eq!(systemd_quote("A=$x"), "A=$x");
        assert_eq!(systemd_quote(r#"a "b"#), r#""a \"b""#);

        let done = install(&s).unwrap();
        assert_eq!(done.file, dir.path().join("potluck-test-unit.service"));
        assert_eq!(std::fs::read_to_string(&done.file).unwrap(), unit);
        let done = uninstall(&s).unwrap();
        assert!(done.changed && !done.file.exists());
        assert!(!uninstall(&s).unwrap().changed);
    }

    #[test]
    fn launchd_plist() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = spec(Manager::Launchd, dir.path());
        s.log = dir.path().join("state/daemon.log");
        s.exe = PathBuf::from("/opt/a&b/potluck");
        let plist = render_launchd_plist(&s);
        for want in [
            "<key>Label</key>\n\t<string>potluck-test.label</string>",
            "<key>ProgramArguments</key>\n\t<array>\n\t\t<string>/opt/a&amp;b/potluck</string>\n\t\t<string>daemon</string>\n\t</array>",
            "<key>RunAtLoad</key>\n\t<true/>",
            "<key>KeepAlive</key>\n\t<true/>",
            "<key>ProcessType</key>\n\t<string>Interactive</string>",
            "<key>AbandonProcessGroup</key>\n\t<true/>",
            "<key>StandardErrorPath</key>",
            "<key>POTLUCK_HOME</key>\n\t\t<string>/tmp/pl home/state</string>",
        ] {
            assert!(plist.contains(want), "missing {want:?} in\n{plist}");
        }
        let done = install(&s).unwrap();
        assert_eq!(done.file, dir.path().join("potluck-test.label.plist"));
        assert!(dir.path().join("state").is_dir(), "log directory made");
        assert!(uninstall(&s).unwrap().changed);
        assert!(!done.file.exists());
    }

    /// systemd finds the program the unit names, with `$`, `%` and spaces in its path: a `$`
    /// stays one `$` (written `$$`, systemd looked for a file with `$$` in its name, and the
    /// service failed at every start with status 203/EXEC while install said it started it).
    #[cfg(target_os = "linux")]
    #[test]
    fn systemd_finds_the_program_the_unit_names() {
        use std::os::unix::fs::PermissionsExt;
        let Ok(probe) = Command::new("systemd-analyze").arg("--version").output() else {
            eprintln!("no systemd-analyze here: skipped");
            return;
        };
        assert!(probe.status.success());
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("a$x 100%y");
        std::fs::create_dir(&bin).unwrap();
        let mut s = spec(Manager::Systemd, dir.path());
        s.exe = bin.join("potluck");
        std::fs::write(&s.exe, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&s.exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let unit = render_systemd_unit(&s);
        let file = dir.path().join("potluck-test-verify.service");
        std::fs::write(&file, &unit).unwrap();
        let out = Command::new("systemd-analyze")
            .args(["--user", "verify"])
            .arg(&file)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "{stderr}\n{unit}");
        assert!(!stderr.contains("potluck"), "{stderr}\n{unit}");

        // A quote or backslash in the program path: systemd refuses the unit outright, however
        // it is quoted (which is why install refuses such a path, below).
        let bin = dir.path().join("it's");
        std::fs::create_dir(&bin).unwrap();
        s.exe = bin.join("potluck");
        std::fs::write(&s.exe, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&s.exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(&file, render_systemd_unit(&s)).unwrap();
        let out = Command::new("systemd-analyze")
            .args(["--user", "verify"])
            .arg(&file)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("special characters"), "{stderr}");
    }

    /// systemd refuses a unit whose program path holds a quote or a backslash ("Executable path
    /// contains special characters"): install used to write it, enable it, fail to start it, and
    /// leave a unit that `service uninstall` couldn't stop ("not loaded"), so it failed too. Now
    /// it is refused with nothing written. A LaunchAgent can hold them.
    #[test]
    fn a_program_path_systemd_refuses_is_refused() {
        for bad in [
            "/opt/it's/potluck",
            "/opt/a\"b/potluck",
            "/opt/a\\b/potluck",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut s = spec(Manager::Systemd, dir.path());
            s.exe = PathBuf::from(bad);
            let err = format!("{:#}", install(&s).unwrap_err());
            assert!(err.contains(bad) && err.contains("systemd"), "{err}");
            assert!(!s.file().exists(), "nothing written: {err}");
            let mut s = spec(Manager::Launchd, dir.path());
            s.exe = PathBuf::from(bad);
            s.log = dir.path().join("state/daemon.log");
            install(&s).unwrap();
            assert_eq!(
                plist_program(&std::fs::read_to_string(s.file()).unwrap()),
                Some(PathBuf::from(bad))
            );
        }
    }

    /// A line break in the executable's path would end ExecStart's line and start another unit
    /// line; a path that isn't UTF-8 would be written as another path. Install refuses both, and
    /// writes nothing.
    #[test]
    fn paths_a_service_file_cant_hold_are_refused() {
        use std::os::unix::ffi::OsStrExt;
        for manager in [Manager::Systemd, Manager::Launchd] {
            let dir = tempfile::tempdir().unwrap();
            let fresh = || spec(manager, dir.path());
            let mut bad = Vec::new();
            let mut s = fresh();
            s.exe = PathBuf::from("/opt/x\nExecStartPre=/bin/evil\n/potluck");
            bad.push((s, "ExecStartPre"));
            let mut s = fresh();
            s.exe = PathBuf::from(std::ffi::OsStr::from_bytes(b"/opt/\xff/potluck"));
            bad.push((s, "UTF-8"));
            let mut s = fresh();
            s.log = PathBuf::from("/tmp/a\rb/daemon.log");
            bad.push((s, "daemon.log"));
            let mut s = fresh();
            s.env
                .insert("POTLUCK_HOME".into(), "/tmp/a\nEnvironment=X=1".into());
            bad.push((s, "POTLUCK_HOME"));
            for (s, names) in bad {
                let err = format!("{:#}", install(&s).unwrap_err());
                assert!(err.contains(names), "{manager:?}: {err}");
                assert!(!s.file().exists(), "{manager:?}: nothing written: {err}");
            }
        }
        // A non-UTF-8 POTLUCK_HOME is refused when the spec is made, not written lossily.
        assert!(env_value("POTLUCK_HOME", std::ffi::OsStr::from_bytes(b"/h/\xff")).is_err());
        assert_eq!(
            env_value("POTLUCK_HOME", std::ffi::OsStr::new("/h/pl home")).unwrap(),
            "/h/pl home"
        );
    }

    #[test]
    fn the_program_an_installed_service_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = spec(Manager::Launchd, dir.path());
        s.exe = PathBuf::from("/opt/a&b <c>/potluck");
        assert_eq!(
            plist_program(&render_launchd_plist(&s)),
            Some(PathBuf::from("/opt/a&b <c>/potluck"))
        );
        assert_eq!(plist_program("<plist></plist>"), None);
        assert_eq!(
            exec_start_program(
                "{ path=/opt/my tools/potluck ; argv[]=/opt/my tools/potluck daemon ; ignore_errors=no ; start_time=[n/a] }\n"
            ),
            Some(PathBuf::from("/opt/my tools/potluck"))
        );
        assert_eq!(exec_start_program(""), None);
        let a = dir.path().join("a");
        std::fs::write(&a, "x").unwrap();
        std::os::unix::fs::symlink(&a, dir.path().join("link")).unwrap();
        std::fs::write(dir.path().join("b"), "x").unwrap();
        assert!(same_file(&a, &dir.path().join("link")));
        assert!(!same_file(&a, &dir.path().join("b")));
        assert!(!same_file(&a, &dir.path().join("missing")));
    }

    #[test]
    fn overrides_from_the_environment_shape_the_spec() {
        // Only checks the defaults' shape: the variables are process-wide, so they are exercised
        // by the CLI tests in their own processes.
        let paths = Paths {
            home: PathBuf::from("/tmp/x"),
            config: PathBuf::from("/tmp/x/node.toml"),
        };
        let s = ServiceSpec::for_this_machine(&paths).unwrap();
        assert!(s.exe.is_absolute());
        let canonical = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        assert_eq!(s.exe, canonical, "the canonical path self-update replaces");
        assert_eq!(s.log, PathBuf::from("/tmp/x/daemon.log"));
        if std::env::var_os("POTLUCK_SERVICE_NAME").is_none() {
            assert!(s.name == SYSTEMD_UNIT || s.name == LAUNCHD_LABEL);
        }
    }
}
