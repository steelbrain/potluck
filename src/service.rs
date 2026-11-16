//! `potluck service install|uninstall`: the daemon as a systemd user service (Linux; lingering must
//! be on, see [`crate::preflight`]), a LaunchAgent with ProcessType=Interactive (macOS; Standard or
//! unspecified throttles CPU and I/O), or a Task Scheduler task (Windows).
//!
//! - Linux: `~/.config/systemd/user/potluck.service` running `<this executable> daemon` with
//!   `Restart=on-failure`, then `systemctl --user daemon-reload` and `enable --now`. Refused while
//!   lingering is off: without it systemd stops the user's services, and so the daemon and its jobs,
//!   when the user's last session ends.
//! - macOS: `~/Library/LaunchAgents/potluck.daemon.plist` (RunAtLoad, KeepAlive,
//!   ProcessType=Interactive, AbandonProcessGroup so stopping the daemon never takes jobs with it,
//!   stdout and stderr to the daemon log), then `launchctl bootstrap gui/<uid>`.
//! - Windows: the task `potluck`, registered with `schtasks /create /xml`, which runs `<this
//!   executable> _service-daemon` at the user's sign-in, in the user's session (InteractiveToken:
//!   no stored password, no elevation), with no run time limit, at normal priority and on battery
//!   too. `_service-daemon` ([`start_service_daemon`]) sets POTLUCK_HOME and POTLUCK_CONFIG from
//!   its arguments (a task's action carries no environment of its own), lets go of the console
//!   window Windows opens for it, and runs `potluck daemon`, whose keeper starts the daemon again
//!   5 seconds after it fails, as systemd's `Restart=on-failure` does (Task Scheduler's own
//!   restart on failure doesn't count a program's exit code). The task's definition
//!   is kept as `~/.config/potluck/potluck.task.xml`. Like a LaunchAgent it runs while the user is
//!   signed in, so `service install` starts it at once only when the user has a session.
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
    /// Windows' Task Scheduler.
    TaskScheduler,
}

/// Everything that decides the installed service.
#[derive(Clone, Debug)]
pub struct ServiceSpec {
    pub manager: Manager,
    /// Where the unit file, plist or task definition goes.
    pub dir: PathBuf,
    /// The unit name (`potluck.service`), the LaunchAgent label (`potluck.daemon`) or the task's
    /// name (`potluck`).
    pub name: String,
    /// The potluck executable the service runs as `<exe> daemon` (Windows: `<exe>
    /// _service-daemon`).
    pub exe: PathBuf,
    /// The daemon log (macOS: launchd sends stdout and stderr there; Windows: the task runs in its
    /// directory, the state directory).
    pub log: PathBuf,
    /// Variables the daemon needs to find the same state as this client (POTLUCK_HOME and
    /// POTLUCK_CONFIG when set).
    pub env: BTreeMap<String, String>,
    /// Run systemctl, launchctl or schtasks. Off only in tests that look at the files.
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
    /// Installed, but members won't be reachable from it: [`signature_problem`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// On macOS, why the daemon run from `exe` can't reach members: `exe` has no code signature
/// (one that no longer matches the binary isn't caught: `codesign -d` doesn't check it). Local
/// Network privacy asks the user about a signed LaunchAgent's first connection to a machine on
/// the local network, but blocks an unsigned one's without asking, and System Settings lists
/// nothing to allow. Apple's linker signs arm64 binaries (ad hoc), not x86_64 ones, so a potluck
/// built for an Intel Mac has none until `codesign` gives it one. None when it has one, off
/// macOS, or when `codesign` can't run.
pub fn signature_problem(exe: &Path) -> Option<String> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    // `-d`, not `-v`: it reads the signature without hashing the whole binary (10 ms, not 130,
    // for a debug build), and fails just the same for a binary that has none.
    let out = Command::new("/usr/bin/codesign")
        .arg("-d")
        .arg(exe)
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if out.status.success() {
        return None;
    }
    let why = String::from_utf8_lossy(&out.stderr);
    let exe = exe.display();
    Some(format!(
        "{}; macOS's Local Network privacy blocks an unsigned daemon's connections to members without asking, so this machine can't reach them. Sign a copy and move it into place: `cp {exe} {exe}.new && codesign -s - -i potluck {exe}.new && mv {exe}.new {exe}`, then `potluck restart`",
        why.trim()
    ))
}

/// The running executable's canonical path (symlinks resolved): the file `self-update` replaces,
/// so a restarted service runs the new binary rather than an old copy behind a link.
/// (On Windows a running binary renamed aside by an update keeps reporting the path it started
/// from, where the new binary now is.)
pub fn this_executable() -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe()
        .map_err(|e| anyhow::anyhow!("finding the running potluck executable: {e}"))?;
    // Linux names a replaced, still running binary "<path> (deleted)"; the service should run the
    // file now at <path>.
    let exe = match exe.to_str().and_then(|s| s.strip_suffix(" (deleted)")) {
        Some(stripped) if !exe.exists() => PathBuf::from(stripped),
        _ => exe,
    };
    crate::sys::canonicalize(&exe)
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
    if spec.manager == Manager::TaskScheduler {
        // Task Scheduler puts a variable's value in place of `%NAME%` in a task's program,
        // arguments and working directory, and has no way to write a `%` that it leaves alone.
        let mut named = vec![
            (
                "the potluck executable",
                spec.exe.to_string_lossy().into_owned(),
            ),
            ("the daemon log", spec.log.to_string_lossy().into_owned()),
        ];
        named.extend(spec.env.iter().map(|(k, v)| (k.as_str(), v.clone())));
        for (what, v) in named {
            if v.matches('%').count() >= 2 {
                anyhow::bail!(
                    "{what} {v} holds two `%`, and Task Scheduler reads the text between them as \
                     an environment variable to put in its place: use a path without them"
                );
            }
        }
        if let Some(path) = spec.exe.to_str()
            && path.contains('"')
        {
            anyhow::bail!(
                "the potluck executable {path} has a quote in its path, which a task's program \
                 can't hold: move potluck to a path without one"
            );
        }
    }
    Ok(())
}

/// This machine's service manager.
pub fn manager() -> Manager {
    if cfg!(target_os = "macos") {
        Manager::Launchd
    } else if cfg!(windows) {
        Manager::TaskScheduler
    } else {
        Manager::Systemd
    }
}

/// The name of the daemon's Task Scheduler task (Windows), in the root folder.
pub const TASK_NAME: &str = "potluck";

/// The daemon's service name: the systemd unit (`potluck.service`), the LaunchAgent's label
/// (`potluck.daemon`) or the task's name (`potluck`), or `POTLUCK_SERVICE_NAME`.
pub fn service_name() -> String {
    let var = std::env::var("POTLUCK_SERVICE_NAME")
        .ok()
        .filter(|v| !v.is_empty());
    match (var, manager()) {
        (Some(n), Manager::Systemd) if !n.ends_with(".service") => format!("{n}.service"),
        (Some(n), _) => n,
        (None, Manager::Systemd) => SYSTEMD_UNIT.to_string(),
        (None, Manager::Launchd) => LAUNCHD_LABEL.to_string(),
        (None, Manager::TaskScheduler) => TASK_NAME.to_string(),
    }
}

/// Where the daemon's service file goes: `~/.config/systemd/user` or `~/Library/LaunchAgents`, or
/// on Windows `%USERPROFILE%\.config\potluck` (the copy of the task's definition), or
/// `POTLUCK_SERVICE_DIR`. None without HOME (USERPROFILE).
pub fn service_dir() -> Option<PathBuf> {
    if let Some(d) = std::env::var_os("POTLUCK_SERVICE_DIR").filter(|d| !d.is_empty()) {
        return Some(PathBuf::from(d));
    }
    if manager() == Manager::TaskScheduler {
        return Some(crate::sys::home_dir()?.join(".config").join("potluck"));
    }
    let home = PathBuf::from(std::env::var_os("HOME")?);
    Some(match manager() {
        Manager::Systemd => home.join(".config/systemd/user"),
        Manager::Launchd => home.join("Library/LaunchAgents"),
        Manager::TaskScheduler => unreachable!("handled above"),
    })
}

impl ServiceSpec {
    /// The spec for this machine and executable, honouring `POTLUCK_SERVICE_DIR` and
    /// `POTLUCK_SERVICE_NAME`.
    pub fn for_this_machine(paths: &Paths) -> anyhow::Result<ServiceSpec> {
        let manager = manager();
        let dir = service_dir()
            .ok_or_else(|| anyhow::anyhow!("{} is not set", crate::sys::home_var()))?;
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

    /// The unit file, plist, or copy of the task's definition.
    pub fn file(&self) -> PathBuf {
        match self.manager {
            Manager::Systemd => self.dir.join(&self.name),
            Manager::Launchd => self.dir.join(format!("{}.plist", self.name)),
            Manager::TaskScheduler => self.dir.join(format!("{}.task.xml", self.name)),
        }
    }

    pub fn render(&self) -> String {
        match self.manager {
            Manager::Systemd => render_systemd_unit(self),
            Manager::Launchd => render_launchd_plist(self),
            Manager::TaskScheduler => render_task_xml(self, &task_user().unwrap_or_default()),
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

/// Quotes one argument for a Windows command line as programs built with the Microsoft C runtime
/// (and Rust's `std::env::args`) split it: words with spaces, tabs or quotes go in double quotes,
/// a quote is escaped with a backslash, and backslashes before a quote (or the closing quote) are
/// doubled.
pub fn windows_quote(arg: &str) -> String {
    if !arg.is_empty() && !arg.contains([' ', '\t', '\n', '\x0b', '"']) {
        return arg.to_string();
    }
    let mut out = String::from('"');
    let mut backslashes = 0;
    for c in arg.chars() {
        match c {
            '\\' => backslashes += 1,
            '"' => {
                out.extend(std::iter::repeat_n('\\', 2 * backslashes + 1));
                out.push('"');
                backslashes = 0;
            }
            c => {
                out.extend(std::iter::repeat_n('\\', backslashes));
                out.push(c);
                backslashes = 0;
            }
        }
    }
    out.extend(std::iter::repeat_n('\\', 2 * backslashes));
    out.push('"');
    out
}

/// The hidden command the task runs ([`start_service_daemon`]).
pub const SERVICE_DAEMON_COMMAND: &str = "_service-daemon";

/// The task's command line after the program: `_service-daemon`, and `--env NAME=VALUE` for each
/// variable the daemon needs.
fn task_arguments(spec: &ServiceSpec) -> String {
    let mut args = vec![SERVICE_DAEMON_COMMAND.to_string()];
    for (k, v) in &spec.env {
        args.push("--env".into());
        args.push(format!("{k}={v}"));
    }
    args.iter()
        .map(|a| windows_quote(a))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The Task Scheduler task (schema 1.2) for `spec`, run as `user` (a SID): at that user's sign-in,
/// in their session (InteractiveToken: no stored password; LeastPrivilege: no elevation).
/// IgnoreNew: a start while it runs does nothing. No run time limit (`PT0S`); started again a
/// minute after a failure; not held back or stopped on battery; priority 5, which runs it at
/// normal priority (Task Scheduler's default, 7, is below normal); hidden in Task Scheduler's
/// list unless it shows hidden tasks. Its working directory is the executable's, there whenever
/// the task can run: Task Scheduler doesn't start a task whose working directory is missing, and
/// the state directory may be on a volume attached after sign-in.
pub fn render_task_xml(spec: &ServiceSpec, user: &str) -> String {
    let exe = xml_escape(&format!("\"{}\"", spec.exe.to_string_lossy()));
    let args = xml_escape(&task_arguments(spec));
    // The executable's directory, read as Windows does (on any system, for tests).
    let program = spec.exe.to_string_lossy();
    let workdir = match program.rsplit_once(['\\', '/']) {
        Some((d, _)) if d.ends_with(':') => format!(
            "\n      <WorkingDirectory>{}\\</WorkingDirectory>",
            xml_escape(d)
        ),
        Some((d, _)) if !d.is_empty() => {
            format!(
                "\n      <WorkingDirectory>{}</WorkingDirectory>",
                xml_escape(d)
            )
        }
        _ => String::new(),
    };
    let user = xml_escape(user);
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>potluck daemon (build and test cluster)</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>{user}</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{user}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>false</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>true</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>5</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{exe}</Command>
      <Arguments>{args}</Arguments>{workdir}
    </Exec>
  </Actions>
</Task>
"#
    )
}

/// The program of a task definition as [`render_task_xml`] writes it (its `<Command>`, unquoted).
pub fn task_program(xml: &str) -> Option<PathBuf> {
    let start = xml.find("<Command>")? + "<Command>".len();
    let end = start + xml[start..].find("</Command>")?;
    let text = xml_unescape(&xml[start..end]);
    let text = text.trim();
    let text = text
        .strip_prefix('"')
        .and_then(|t| t.strip_suffix('"'))
        .unwrap_or(text);
    (!text.is_empty()).then(|| PathBuf::from(text))
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// `text` as a file Task Scheduler reads as its XML says (`encoding="UTF-16"`): UTF-16
/// little-endian with a byte order mark, as `schtasks /query /xml` writes a task.
pub fn utf16_file(text: &str) -> Vec<u8> {
    let mut out = vec![0xff, 0xfe];
    for unit in text.encode_utf16() {
        out.extend_from_slice(&unit.to_le_bytes());
    }
    out
}

/// A file [`utf16_file`] wrote (or UTF-8 text) as text.
pub fn read_utf16_file(bytes: &[u8]) -> Option<String> {
    match bytes.strip_prefix(&[0xff, 0xfe]) {
        Some(rest) if rest.len() % 2 == 0 => {
            let units: Vec<u16> = rest
                .as_chunks::<2>()
                .0
                .iter()
                .map(|c| u16::from_le_bytes(*c))
                .collect();
            String::from_utf16(&units).ok()
        }
        Some(_) => None,
        None => String::from_utf8(bytes.to_vec()).ok(),
    }
}

/// The user a task runs as: this process's user's SID.
fn task_user() -> anyhow::Result<String> {
    #[cfg(windows)]
    {
        crate::sys::user_sid().map_err(|e| anyhow::anyhow!("reading this user's SID: {e}"))
    }
    #[cfg(not(windows))]
    {
        anyhow::bail!("Task Scheduler tasks are for Windows")
    }
}

/// A command for one of this user's service manager tools (`systemctl --user`, `systemd-run
/// --user`, `coredumpctl`, `loginctl`, `launchctl`), as the daemon, the CLI and updates all run
/// them. Linux: when XDG_RUNTIME_DIR isn't set (a shell with no login session, such as `su` or
/// cron), it is the user manager's own `/run/user/<uid>`, where systemd's tools find its bus;
/// no pager, no colours.
pub(crate) fn user_command(program: &str) -> Command {
    let mut cmd = Command::new(program);
    #[cfg(target_os = "linux")]
    if std::env::var_os("XDG_RUNTIME_DIR").is_none() {
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

#[cfg(unix)]
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
/// argument, the task's program), if a service is installed and says. `service install` writes the
/// executable it ran from; an update elsewhere leaves the service running that one.
pub fn installed_program() -> Option<PathBuf> {
    let name = service_name();
    match manager() {
        Manager::Launchd => {
            let plist = service_dir()?.join(format!("{name}.plist"));
            plist_program(&std::fs::read_to_string(plist).ok()?)
        }
        Manager::Systemd => {
            let out = run(
                "systemctl",
                &["--user", "show", "--property=ExecStart", "--value", &name],
            )
            .ok()
            .filter(|o| o.status.success())?;
            exec_start_program(&String::from_utf8_lossy(&out.stdout))
        }
        // From the copy of the definition `service install` registered, while the task exists.
        // (schtasks prints a task's XML in the console's code page, which can't hold every path.)
        Manager::TaskScheduler => {
            #[cfg(windows)]
            task::state(&name)?;
            let file = service_dir()?.join(format!("{name}.task.xml"));
            task_program(&read_utf16_file(&std::fs::read(file).ok()?)?)
        }
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

/// Whether `a` and `b` are one file (symlinks followed). On Windows: whether their canonical paths
/// are the same (a file's index, which `GetFileInformationByHandle` gives, isn't unique on ReFS).
pub fn same_file(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (std::fs::metadata(a), std::fs::metadata(b)) {
            (Ok(x), Ok(y)) => x.dev() == y.dev() && x.ino() == y.ino(),
            _ => false,
        }
    }
    #[cfg(windows)]
    {
        match (crate::sys::canonicalize(a), crate::sys::canonicalize(b)) {
            (Ok(x), Ok(y)) => x == y,
            _ => false,
        }
    }
}

/// The user's login name.
#[cfg(unix)]
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
#[cfg(unix)]
fn write_file(path: &Path, text: &str) -> anyhow::Result<bool> {
    write_bytes(path, text.as_bytes())
}

/// Writes `bytes` to `path` through a temporary file and a rename. Returns whether it changed.
fn write_bytes(path: &Path, text: &[u8]) -> anyhow::Result<bool> {
    if std::fs::read(path).is_ok_and(|old| old == text) {
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

/// Whether the service is running (systemd: active; Task Scheduler: an instance of the task runs)
/// or loaded (launchd).
pub fn active(spec: &ServiceSpec) -> bool {
    match spec.manager {
        Manager::Systemd => run("systemctl", &["--user", "is-active", "--quiet", &spec.name])
            .is_ok_and(|o| o.status.success()),
        #[cfg(unix)]
        Manager::Launchd => launchd_loaded(&spec.name),
        #[cfg(windows)]
        Manager::TaskScheduler => task::state(&spec.name) == Some(true),
        _ => false,
    }
}

/// The error for a service manager this system doesn't have.
fn foreign(manager: Manager) -> anyhow::Error {
    anyhow::anyhow!("{manager:?} is not this system's service manager")
}

/// Writes the service file and (with `manage`) starts the service. Refused, with nothing
/// written, when a path or value the file names isn't UTF-8 or holds a control character (or,
/// for a task, two `%`).
pub fn install(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    check_writable(spec)?;
    match spec.manager {
        #[cfg(unix)]
        Manager::Systemd => install_systemd(spec),
        #[cfg(unix)]
        Manager::Launchd => install_launchd(spec),
        #[cfg(windows)]
        Manager::TaskScheduler => install_task(spec),
        other => Err(foreign(other)),
    }
}

pub fn uninstall(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    match spec.manager {
        #[cfg(unix)]
        Manager::Systemd => uninstall_systemd(spec),
        #[cfg(unix)]
        Manager::Launchd => uninstall_launchd(spec),
        #[cfg(windows)]
        Manager::TaskScheduler => uninstall_task(spec),
        other => Err(foreign(other)),
    }
}

#[cfg(unix)]
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
        Manager::TaskScheduler => format!("the scheduled task {}", spec.name),
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
    let warning = match spec.manager {
        Manager::Launchd => signature_problem(&spec.exe),
        Manager::Systemd | Manager::TaskScheduler => None,
    };
    let message = match &warning {
        Some(w) => format!("{message}\nwarning: {w}"),
        None => message,
    };
    ServiceDone {
        manager: spec.manager,
        name: spec.name.clone(),
        file: file.to_path_buf(),
        changed,
        message,
        warning,
    }
}

#[cfg(unix)]
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
        warning: None,
    })
}

#[cfg(unix)]
fn gui_domain() -> String {
    // SAFETY: getuid has no preconditions.
    format!("gui/{}", unsafe { libc::getuid() })
}

#[cfg(unix)]
fn launchd_loaded(label: &str) -> bool {
    run(
        "launchctl",
        &["print", &format!("{}/{label}", gui_domain())],
    )
    .is_ok_and(|o| o.status.success())
}

#[cfg(unix)]
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

#[cfg(unix)]
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
        warning: None,
    })
}

/// The fields of one line of CSV as `schtasks /fo csv` writes it: each in double quotes, a quote
/// inside one doubled.
pub fn csv_fields(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = line.trim_end_matches(['\r', '\n']).chars().peekable();
    while let Some(c) = chars.next() {
        match (c, quoted) {
            ('"', true) if chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            ('"', _) => quoted = !quoted,
            (',', false) => fields.push(std::mem::take(&mut field)),
            (c, _) => field.push(c),
        }
    }
    fields.push(field);
    fields
}

/// `SCHED_S_TASK_RUNNING` (0x41301): a task's last result while an instance of it runs.
const TASK_RUNNING_RESULT: &str = "267009";

/// Whether `schtasks /query /tn NAME /v /fo csv /nh` says an instance of the task runs: its last
/// result (the seventh column, a number in every language) is `SCHED_S_TASK_RUNNING`. Its status
/// column says so too, but in the system's language.
pub fn task_running(verbose_csv: &str) -> bool {
    verbose_csv
        .lines()
        .filter(|l| !l.trim().is_empty())
        .any(|l| csv_fields(l).get(6).map(|f| f.trim()) == Some(TASK_RUNNING_RESULT))
}

/// Task Scheduler through `schtasks.exe`, and the user's sessions (Windows).
#[cfg(windows)]
pub(crate) mod task {
    use std::process::{Command, Output, Stdio};
    use std::time::{Duration, Instant};

    /// Runs `schtasks` with `args`. Its own console has no window, so none opens even when this
    /// process has no console.
    pub fn schtasks(args: &[&str]) -> anyhow::Result<Output> {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;
        Command::new("schtasks")
            .args(args)
            .stdin(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .output()
            .map_err(|e| anyhow::anyhow!("could not run schtasks: {e}"))
    }

    /// `schtasks` with `args`, failing with what it said unless it succeeds.
    pub fn schtasks_ok(args: &[&str]) -> anyhow::Result<()> {
        let out = schtasks(args)?;
        if !out.status.success() {
            let said = [&out.stderr[..], &out.stdout[..]]
                .iter()
                .map(|b| String::from_utf8_lossy(b).trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(" ");
            anyhow::bail!(
                "schtasks {} failed ({}): {said}",
                args.join(" "),
                out.status
            );
        }
        Ok(())
    }

    /// None when there is no task named `name`; otherwise whether an instance of it runs.
    pub fn state(name: &str) -> Option<bool> {
        let out = schtasks(&["/query", "/tn", name, "/v", "/fo", "csv", "/nh"]).ok()?;
        if !out.status.success() {
            return None;
        }
        Some(super::task_running(&String::from_utf8_lossy(&out.stdout)))
    }

    /// Registers (or replaces) the task `name` from the definition in `file`.
    pub fn register(name: &str, file: &std::path::Path) -> anyhow::Result<()> {
        let file = file.to_string_lossy();
        schtasks_ok(&["/create", "/xml", &file, "/tn", name, "/f"])
    }

    /// Starts the task.
    pub fn run(name: &str) -> anyhow::Result<()> {
        schtasks_ok(&["/run", "/tn", name])
    }

    /// Ends the running instance of the task, and waits up to 30 s for it to be gone (a start
    /// before then would be ignored: IgnoreNew).
    pub fn end(name: &str) -> anyhow::Result<()> {
        schtasks_ok(&["/end", "/tn", name])?;
        let give_up = Instant::now() + Duration::from_secs(30);
        while state(name) == Some(true) {
            if Instant::now() > give_up {
                anyhow::bail!("the task {name} still runs 30 s after `schtasks /end /tn {name}`");
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Ok(())
    }

    /// Deletes the task.
    pub fn delete(name: &str) -> anyhow::Result<()> {
        schtasks_ok(&["/delete", "/tn", name, "/f"])
    }

    /// Whether this user is signed in: a session that is active or disconnected (signed in, not
    /// shown) has this user's name and domain. A task that runs with the user's interactive token
    /// runs only then. True when it can't be told: then starting the task is tried.
    pub fn user_signed_in() -> bool {
        use windows_sys::Win32::System::RemoteDesktop::{
            WTS_CURRENT_SERVER_HANDLE, WTS_SESSION_INFOW, WTSActive, WTSDisconnected,
            WTSDomainName, WTSEnumerateSessionsW, WTSFreeMemory, WTSUserName,
        };
        let Some((domain, user)) = account() else {
            return true;
        };
        let mut info: *mut WTS_SESSION_INFOW = std::ptr::null_mut();
        let mut count = 0u32;
        // SAFETY: the local server; info receives an array of count sessions, freed below.
        if unsafe { WTSEnumerateSessionsW(WTS_CURRENT_SERVER_HANDLE, 0, 1, &mut info, &mut count) }
            == 0
        {
            return true;
        }
        // SAFETY: WTSEnumerateSessionsW filled info with count entries.
        let sessions = unsafe { std::slice::from_raw_parts(info, count as usize) };
        let same = |a: Option<String>, b: &str| a.is_some_and(|a| a.to_lowercase() == b);
        let found = sessions.iter().any(|s| {
            (s.State == WTSActive || s.State == WTSDisconnected)
                && same(session_text(s.SessionId, WTSUserName), &user)
                && same(session_text(s.SessionId, WTSDomainName), &domain)
        });
        // SAFETY: info came from WTSEnumerateSessionsW and isn't used after this.
        unsafe { WTSFreeMemory(info.cast()) };
        found
    }

    /// A text field of a session.
    fn session_text(
        session: u32,
        class: windows_sys::Win32::System::RemoteDesktop::WTS_INFO_CLASS,
    ) -> Option<String> {
        use windows_sys::Win32::System::RemoteDesktop::{
            WTS_CURRENT_SERVER_HANDLE, WTSFreeMemory, WTSQuerySessionInformationW,
        };
        let mut buf: windows_sys::core::PWSTR = std::ptr::null_mut();
        let mut bytes = 0u32;
        // SAFETY: the local server; buf receives a NUL-terminated string, freed below.
        if unsafe {
            WTSQuerySessionInformationW(
                WTS_CURRENT_SERVER_HANDLE,
                session,
                class,
                &mut buf,
                &mut bytes,
            )
        } == 0
        {
            return None;
        }
        // SAFETY: buf is a NUL-terminated UTF-16 string from the call above.
        let text = unsafe {
            let len = (0..).take_while(|&i| *buf.add(i) != 0).count();
            String::from_utf16_lossy(std::slice::from_raw_parts(buf, len))
        };
        // SAFETY: buf came from WTSQuerySessionInformationW and isn't used after this.
        unsafe { WTSFreeMemory(buf.cast()) };
        Some(text)
    }

    /// This process's user's domain (the machine's name for a local account) and name, lower
    /// case.
    fn account() -> Option<(String, String)> {
        use crate::sys::win::wide;
        use windows_sys::Win32::Foundation::LocalFree;
        use windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;
        use windows_sys::Win32::Security::{LookupAccountSidW, PSID, SID_NAME_USE};
        let text = wide(crate::sys::user_sid().ok()?);
        let mut sid: PSID = std::ptr::null_mut();
        // SAFETY: text is NUL-terminated; sid receives a LocalAlloc'd SID, freed below.
        if unsafe { ConvertStringSidToSidW(text.as_ptr(), &mut sid) } == 0 {
            return None;
        }
        let mut name = [0u16; 512];
        let mut domain = [0u16; 512];
        let (mut n, mut d) = (name.len() as u32, domain.len() as u32);
        let mut kind: SID_NAME_USE = 0;
        // SAFETY: the buffers hold n and d UTF-16 units; sid is valid until freed below.
        let ok = unsafe {
            LookupAccountSidW(
                std::ptr::null(),
                sid,
                name.as_mut_ptr(),
                &mut n,
                domain.as_mut_ptr(),
                &mut d,
                &mut kind,
            )
        } != 0;
        // SAFETY: sid came from ConvertStringSidToSidW and isn't used after this.
        unsafe { LocalFree(sid) };
        ok.then(|| {
            (
                crate::sys::win::from_wide(&domain).to_lowercase(),
                crate::sys::win::from_wide(&name).to_lowercase(),
            )
        })
    }
}

/// Writes the task's definition and (with `manage`) registers the task and starts it when the user
/// has a session; a task that runs and was changed is ended and started again (running jobs go
/// on).
#[cfg(windows)]
fn install_task(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    let user = task_user()?;
    // The task runs in the state directory.
    if let Some(dir) = spec.log.parent() {
        std::fs::create_dir_all(dir).map_err(|e| anyhow::anyhow!("{}: {e}", dir.display()))?;
    }
    let file = spec.file();
    let mut changed = write_bytes(&file, &utf16_file(&render_task_xml(spec, &user)))?;
    let mut waits = false;
    if spec.manage {
        let state = task::state(&spec.name);
        if changed || state.is_none() {
            task::register(&spec.name, &file)?;
        }
        let running = state == Some(true);
        if running && changed {
            // It runs what it was started with until it starts again.
            task::end(&spec.name)?;
            task::run(&spec.name)?;
        } else if !running {
            if task::user_signed_in() {
                task::run(&spec.name)?;
            } else {
                waits = true;
            }
        }
        changed |= !running;
    }
    let mut done = installed(spec, &file, changed, &spec.log.display().to_string());
    if waits {
        let user = crate::sys::user_name().unwrap_or_else(|| "this user".into());
        done.message = done.message.replacen(
            " and started it;",
            &format!(
                "; it starts when {user} signs in (a task runs in its user's session, and {user} \
                 has none now);"
            ),
            1,
        );
    }
    Ok(done)
}

/// Ends and deletes the task, and removes the copy of its definition.
#[cfg(windows)]
fn uninstall_task(spec: &ServiceSpec) -> anyhow::Result<ServiceDone> {
    let file = spec.file();
    let state = if spec.manage {
        task::state(&spec.name)
    } else {
        None
    };
    if state == Some(true) {
        task::end(&spec.name)?;
    }
    if state.is_some() {
        task::delete(&spec.name)?;
    }
    let existed = file.exists();
    if existed {
        std::fs::remove_file(&file).map_err(|e| anyhow::anyhow!("{}: {e}", file.display()))?;
    }
    let changed = existed || state.is_some();
    Ok(ServiceDone {
        manager: spec.manager,
        name: spec.name.clone(),
        file: file.clone(),
        changed,
        message: if changed {
            format!(
                "stopped and removed the scheduled task {} ({})",
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
        warning: None,
    })
}

/// The scheduled task's entry, `potluck _service-daemon [--env NAME=VALUE]...` (Windows), before
/// it runs `potluck daemon`. Sets POTLUCK_HOME and POTLUCK_CONFIG as `service install` found them
/// (a task's action carries no environment of its own). Then, when no other process shares its
/// console (Task Scheduler starts a console program in a console of its own, shown in the user's
/// session; a `potluck _service-daemon` typed in a terminal keeps its terminal), lets go of that
/// console and sends stdout and stderr to the daemon log, as launchd does on macOS: an error
/// before the daemon's log opens is said there, and the daemon the keeper starts inherits them
/// (the console's handles end with the console). The daemon proper runs these same arguments
/// ([`crate::daemon::keeper`]) and keeps the console without a window its keeper gave it: a
/// console program started by a process without a console gets a console window of its own, and
/// the daemon starts git and other programs throughout. Call it before any other thread starts.
#[cfg(windows)]
pub fn start_service_daemon(env: &[String]) -> anyhow::Result<()> {
    use windows_sys::Win32::System::Console::{FreeConsole, GetConsoleProcessList};
    for kv in env {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--env {kv:?} is not NAME=VALUE"))?;
        if !matches!(k, "POTLUCK_HOME" | "POTLUCK_CONFIG") {
            anyhow::bail!("--env {k}: only POTLUCK_HOME and POTLUCK_CONFIG are taken");
        }
        // SAFETY: the caller runs this before any other thread starts, so nothing else reads or
        // writes the environment meanwhile.
        unsafe { std::env::set_var(k, v) };
    }
    if std::env::var_os(crate::daemon::keeper::MARKER).is_some() {
        return Ok(());
    }
    let mut pids = [0u32; 2];
    // SAFETY: pids has room for the 2 entries given.
    if unsafe { GetConsoleProcessList(pids.as_mut_ptr(), pids.len() as u32) } <= 1 {
        // SAFETY: plain call; no other process uses the console, if there is one.
        unsafe { FreeConsole() };
        std_to_log();
    }
    Ok(())
}

/// Points stdin at NUL, and stdout and stderr at the daemon log (appended to), or at NUL when
/// it can't be opened.
#[cfg(windows)]
fn std_to_log() {
    use std::os::windows::io::IntoRawHandle;
    use windows_sys::Win32::System::Console::{
        STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, SetStdHandle,
    };
    let nul = |write: bool| {
        std::fs::OpenOptions::new()
            .read(!write)
            .write(write)
            .open("NUL")
            .ok()
    };
    let log = crate::config::Paths::from_env().ok().and_then(|paths| {
        crate::sys::create_dir_all(&paths.home, 0o700).ok()?;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(paths.daemon_log())
            .ok()
    });
    let out = log.or_else(|| nul(true));
    let err = out.as_ref().and_then(|f| f.try_clone().ok());
    for (which, file) in [
        (STD_INPUT_HANDLE, nul(false)),
        (STD_OUTPUT_HANDLE, out),
        (STD_ERROR_HANDLE, err),
    ] {
        if let Some(f) = file {
            // SAFETY: the handle is valid and stays open for the life of the process (it is
            // never closed: into_raw_handle gives it up).
            unsafe { SetStdHandle(which, f.into_raw_handle()) };
        }
    }
}

#[cfg(test)]
mod tests {
    /// A copy of this test binary, signed ad hoc, then with its signature removed.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_unsigned_binary_is_told() {
        let tmp = tempfile::tempdir().unwrap();
        let copy = tmp.path().join("potluck");
        std::fs::copy(std::env::current_exe().unwrap(), &copy).unwrap();
        let codesign = |args: &[&str]| {
            let out = Command::new("/usr/bin/codesign")
                .args(args)
                .arg(&copy)
                .output()
                .unwrap();
            assert!(out.status.success(), "{out:?}");
        };
        codesign(&["--force", "-s", "-", "-i", "potluck"]);
        assert_eq!(super::signature_problem(&copy), None);
        codesign(&["--remove-signature"]);
        let why = super::signature_problem(&copy).unwrap();
        assert!(why.contains("not signed at all"), "{why}");
        assert!(why.contains("codesign -s - -i potluck"), "{why}");
    }

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
                Manager::TaskScheduler => "potluck-test-task".into(),
            },
            exe: PathBuf::from("/opt/my tools/potluck"),
            log: PathBuf::from("/tmp/pl home/state/daemon.log"),
            env,
            manage: false,
        }
    }

    #[cfg(unix)]
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

    #[cfg(unix)]
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
    #[cfg(unix)]
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
    #[cfg(unix)]
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

    #[cfg(unix)]
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
        let canonical = crate::sys::canonicalize(&std::env::current_exe().unwrap()).unwrap();
        assert_eq!(s.exe, canonical, "the canonical path self-update replaces");
        assert_eq!(s.log, PathBuf::from("/tmp/x").join("daemon.log"));
        if std::env::var_os("POTLUCK_SERVICE_NAME").is_none() {
            assert!([SYSTEMD_UNIT, LAUNCHD_LABEL, TASK_NAME].contains(&s.name.as_str()));
        }
    }

    fn task_spec(dir: &Path) -> ServiceSpec {
        let mut s = spec(Manager::TaskScheduler, dir);
        s.exe = PathBuf::from(r"C:\Users\Ann Lee\.local\bin\potluck.exe");
        s.log = PathBuf::from(r"D:\pl & co\daemon.log");
        s.env = BTreeMap::from([
            ("POTLUCK_HOME".to_string(), r"D:\pl & co".to_string()),
            ("POTLUCK_CONFIG".to_string(), r"D:\cfg\".to_string()),
        ]);
        s
    }

    /// The task runs `potluck.exe _service-daemon` with the variables as arguments, at the user's
    /// sign-in, in their session, and comes back after a failure.
    #[test]
    fn task_definition() {
        let dir = tempfile::tempdir().unwrap();
        let s = task_spec(dir.path());
        let xml = render_task_xml(&s, "S-1-5-21-1-2-3-1001");
        for want in [
            r#"<?xml version="1.0" encoding="UTF-16"?>"#,
            r#"<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">"#,
            "<LogonTrigger>\n      <Enabled>true</Enabled>\n      <UserId>S-1-5-21-1-2-3-1001</UserId>",
            "<UserId>S-1-5-21-1-2-3-1001</UserId>\n      <LogonType>InteractiveToken</LogonType>\n      <RunLevel>LeastPrivilege</RunLevel>",
            "<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
            "<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
            "<StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
            "<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
            "<Priority>5</Priority>",
            "<RestartOnFailure>\n      <Interval>PT1M</Interval>",
            "<Hidden>true</Hidden>",
            "<AllowHardTerminate>true</AllowHardTerminate>",
            r#"<Command>&quot;C:\Users\Ann Lee\.local\bin\potluck.exe&quot;</Command>"#,
            r#"<Arguments>_service-daemon --env POTLUCK_CONFIG=D:\cfg\ --env &quot;POTLUCK_HOME=D:\pl &amp; co&quot;</Arguments>"#,
            r"<WorkingDirectory>C:\Users\Ann Lee\.local\bin</WorkingDirectory>",
        ] {
            assert!(xml.contains(want), "missing {want:?} in\n{xml}");
        }
        assert_eq!(task_program(&xml), Some(s.exe.clone()));
        assert_eq!(task_program("<Task></Task>"), None);
        let file = utf16_file(&xml);
        assert_eq!(&file[..4], &[0xff, 0xfe, b'<', 0]);
        assert_eq!(read_utf16_file(&file).as_deref(), Some(xml.as_str()));
        assert_eq!(read_utf16_file(b"plain").as_deref(), Some("plain"));
        assert_eq!(read_utf16_file(&[0xff, 0xfe, 0x41]), None);
    }

    /// Arguments come back as they were from a command line Windows programs split.
    #[test]
    fn windows_command_line_quoting() {
        for (arg, quoted) in [
            ("plain", "plain"),
            ("", r#""""#),
            ("a b", r#""a b""#),
            (r"D:\pl home\", r#""D:\pl home\\""#),
            (r"D:\x\", r"D:\x\"),
            (r#"say "hi""#, r#""say \"hi\"""#),
            (r#"a\"b"#, r#""a\\\"b""#),
        ] {
            assert_eq!(windows_quote(arg), quoted, "{arg:?}");
        }
    }

    /// What Task Scheduler would change in a task (text between two `%`) or a task's program
    /// can't hold (a quote) is refused before anything is written.
    #[test]
    fn a_task_refuses_paths_task_scheduler_would_change() {
        let dir = tempfile::tempdir().unwrap();
        let mut bad = Vec::new();
        let mut s = task_spec(dir.path());
        s.exe = PathBuf::from(r"C:\%TEMP%\potluck.exe");
        bad.push((s, "the potluck executable"));
        let mut s = task_spec(dir.path());
        s.env
            .insert("POTLUCK_HOME".into(), r"D:\%USERNAME%\pl".into());
        bad.push((s, "POTLUCK_HOME"));
        let mut s = task_spec(dir.path());
        s.log = PathBuf::from(r"D:\a%b%\daemon.log");
        bad.push((s, "the daemon log"));
        let mut s = task_spec(dir.path());
        s.exe = PathBuf::from(r#"C:\a"b\potluck.exe"#);
        bad.push((s, "quote"));
        for (s, names) in bad {
            let err = format!("{:#}", install(&s).unwrap_err());
            assert!(err.contains(names), "{err}");
            assert!(!s.file().exists(), "nothing written: {err}");
        }
        let mut s = task_spec(dir.path());
        s.exe = PathBuf::from(r"C:\100%\potluck.exe");
        check_writable(&s).unwrap();
    }

    /// The running state comes from the last result column, a number in every language.
    #[test]
    fn a_running_task_is_told_by_its_last_result() {
        let running = r#""HOST","\potluck","N/A","Running","Interactive only","11/15/2026 9:00:00 AM","267009","HOST\ann","""C:\x\potluck.exe"" _service-daemon","D:\pl","N/A""#;
        let german = r#""HOST","\potluck","Nicht zutreffend","Wird ausgefuehrt","Nur interaktiv","15.11.2026 09:00:00","267009","HOST\ann""#;
        let ready = r#""HOST","\potluck","N/A","Ready","Interactive only","11/15/2026 9:00:00 AM","0","HOST\ann""#;
        assert!(task_running(&format!("\r\n{running}\r\n")));
        assert!(task_running(german));
        assert!(!task_running(ready));
        assert!(!task_running(""));
        assert_eq!(
            csv_fields(r#""a","b ""c"", d","""#),
            ["a", r#"b "c", d"#, ""]
        );
        assert_eq!(
            csv_fields(running)[8],
            r#""C:\x\potluck.exe" _service-daemon"#
        );
    }

    /// Without managing the task, install writes its definition (UTF-16, as Task Scheduler reads
    /// it) and uninstall removes it.
    #[cfg(windows)]
    #[test]
    fn task_definition_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = task_spec(dir.path());
        s.log = dir.path().join("state").join("daemon.log");
        s.env.clear();
        let done = install(&s).unwrap();
        assert_eq!(done.file, dir.path().join("potluck-test-task.task.xml"));
        assert!(done.changed, "{}", done.message);
        assert!(
            dir.path().join("state").is_dir(),
            "the task's directory made"
        );
        let xml = read_utf16_file(&std::fs::read(&done.file).unwrap()).unwrap();
        assert!(
            xml.contains("<Arguments>_service-daemon</Arguments>"),
            "{xml}"
        );
        let sid = crate::sys::user_sid().unwrap();
        assert!(xml.contains(&format!("<UserId>{sid}</UserId>")), "{xml}");
        assert!(!install(&s).unwrap().changed);
        assert!(uninstall(&s).unwrap().changed);
        assert!(!done.file.exists());
        assert!(!uninstall(&s).unwrap().changed);
    }
}
