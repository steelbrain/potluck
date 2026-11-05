//! Restarting the daemon's service after an update: `systemctl --user restart potluck.service` on
//! Linux, `launchctl kickstart -k gui/<uid>/potluck.daemon` on macOS. A service that is installed
//! but stopped (a systemd unit that isn't active, a LaunchAgent that isn't loaded) is left stopped.

use anyhow::{Result, bail};
use std::fmt;
use std::process::{Command, Output, Stdio};

/// What happened to the daemon's service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Restart {
    /// It was running and was restarted with this command.
    Restarted(String),
    /// The daemon restarted in place on the new binary, and said so (`potluck restart`).
    InPlace(String),
    /// The daemon updating itself restarts in place on the new binary next.
    InPlaceNow,
    /// It is installed but not running, so it was left alone.
    NotRunning(String),
    /// No service is installed.
    NoService,
    /// Not tried: `POTLUCK_NO_RESTART=1`. Whether a service is installed.
    Skipped { service: bool },
    /// Not tried: the service runs another potluck than the one updated, and would run that one
    /// again.
    OtherBinary {
        service: std::path::PathBuf,
        updated: std::path::PathBuf,
    },
}

impl fmt::Display for Restart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Restart::Restarted(cmd) => write!(f, "Restarted the daemon (`{cmd}`)."),
            Restart::InPlace(said) => f.write_str(said),
            Restart::InPlaceNow => write!(
                f,
                "The daemon restarts in place on the new binary now; running jobs and \
                 connections are kept."
            ),
            Restart::NotRunning(name) => write!(
                f,
                "The daemon's service ({name}) is installed but not running; it will run the new \
                 binary when it starts."
            ),
            Restart::NoService => write!(
                f,
                "No potluck service is installed here; if a `potluck daemon` is running, restart \
                 it to use the new binary."
            ),
            Restart::Skipped { service: true } => write!(
                f,
                "POTLUCK_NO_RESTART=1, so the daemon was not restarted; restart it to run the new \
                 binary (`{}`).",
                restart_command()
            ),
            Restart::Skipped { service: false } => write!(
                f,
                "POTLUCK_NO_RESTART=1, so the daemon was not restarted; no potluck service is \
                 installed here, so if a `potluck daemon` is running, restart it to run the new \
                 binary."
            ),
            Restart::OtherBinary { service, updated } => write!(
                f,
                "The potluck service runs {}, not {}, so it was not restarted. To make it run {}: \
                 {} service install",
                service.display(),
                updated.display(),
                updated.display(),
                updated.display()
            ),
        }
    }
}

/// Restarts the daemon's service if it is installed and running (`POTLUCK_SERVICE_NAME` and
/// `POTLUCK_SERVICE_DIR` name another, as for `potluck service install`).
#[cfg(target_os = "linux")]
pub fn restart_daemon() -> Result<Restart> {
    restart_systemd_unit(&crate::service::service_name())
}

/// Restarts the daemon's service if it is installed and running (`POTLUCK_SERVICE_NAME` and
/// `POTLUCK_SERVICE_DIR` name another, as for `potluck service install`).
#[cfg(target_os = "macos")]
pub fn restart_daemon() -> Result<Restart> {
    let label = crate::service::service_name();
    let plist = crate::service::service_dir()
        .unwrap_or_default()
        .join(format!("{label}.plist"));
    restart_launch_agent(&label, &plist)
}

/// potluck has no service on other systems.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn restart_daemon() -> Result<Restart> {
    Ok(Restart::NoService)
}

/// Whether the daemon's service is installed (loaded in systemd, or a LaunchAgent plist or
/// label), running or not.
pub(crate) fn service_installed() -> bool {
    let name = crate::service::service_name();
    if cfg!(target_os = "macos") {
        crate::service::service_dir().is_some_and(|d| d.join(format!("{name}.plist")).exists())
            || run(Command::new("launchctl").args(["print", &launchd_target(&name)]))
                .is_ok_and(|o| o.status.success())
    } else {
        run(crate::service::user_command("systemctl").args([
            "--user",
            "show",
            "--property=LoadState",
            "--value",
            &name,
        ]))
        .is_ok_and(|o| {
            let load = String::from_utf8_lossy(&o.stdout);
            o.status.success() && !load.trim().is_empty() && load.trim() != "not-found"
        })
    }
}

/// The command that restarts the daemon's service by hand.
pub(crate) fn restart_command() -> String {
    if cfg!(target_os = "linux") {
        format!(
            "systemctl --user restart {}",
            crate::service::service_name()
        )
    } else if cfg!(target_os = "macos") {
        format!(
            "launchctl kickstart -k {}",
            launchd_target(&crate::service::service_name())
        )
    } else {
        "potluck daemon".to_string()
    }
}

pub(crate) fn launchd_target(label: &str) -> String {
    // SAFETY: getuid has no preconditions and can't fail.
    format!("gui/{}/{label}", unsafe { libc::getuid() })
}

fn run(cmd: &mut Command) -> Result<Output> {
    let shown = format!("{cmd:?}").replace('"', "");
    match cmd.stdin(Stdio::null()).output() {
        Ok(out) => Ok(out),
        Err(e) => bail!("running {shown}: {e}"),
    }
}

fn failure(what: &str, out: &Output) -> anyhow::Error {
    let stderr = String::from_utf8_lossy(&out.stderr);
    anyhow::anyhow!("`{what}` failed ({}): {}", out.status, stderr.trim())
}

/// Restarts a systemd user unit if it is active; reports it as not running if it is loaded but
/// inactive, and as no service if systemd doesn't know it.
#[cfg(target_os = "linux")]
pub(crate) fn restart_systemd_unit(unit: &str) -> Result<Restart> {
    let show = format!("systemctl --user show --property=LoadState,ActiveState {unit}");
    let out = match crate::service::user_command("systemctl")
        .args(["--user", "show", "--property=LoadState,ActiveState", unit])
        .stdin(Stdio::null())
        .output()
    {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Restart::NoService),
        other => other.map_err(|e| anyhow::anyhow!("running {show}: {e}"))?,
    };
    if !out.status.success() {
        return Err(failure(&show, &out));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let prop = |key: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(key)?.strip_prefix('='))
            .unwrap_or("")
            .to_string()
    };
    let (load, active) = (prop("LoadState"), prop("ActiveState"));
    if load == "not-found" {
        return Ok(Restart::NoService);
    }
    if !matches!(active.as_str(), "active" | "activating" | "reloading") {
        return Ok(Restart::NotRunning(unit.to_string()));
    }
    let restart = format!("systemctl --user restart {unit}");
    let out = run(crate::service::user_command("systemctl").args(["--user", "restart", unit]))?;
    if !out.status.success() {
        return Err(failure(&restart, &out));
    }
    Ok(Restart::Restarted(restart))
}

/// Restarts a LaunchAgent if launchd has it loaded; reports it as not running if only its plist
/// exists, and as no service otherwise.
#[cfg(target_os = "macos")]
pub(crate) fn restart_launch_agent(label: &str, plist: &std::path::Path) -> Result<Restart> {
    let target = launchd_target(label);
    let loaded = run(Command::new("launchctl").args(["print", &target]))?;
    if !loaded.status.success() {
        return Ok(if plist.exists() {
            Restart::NotRunning(label.to_string())
        } else {
            Restart::NoService
        });
    }
    let kickstart = format!("launchctl kickstart -k {target}");
    let out = run(Command::new("launchctl").args(["kickstart", "-k", &target]))?;
    if !out.status.success() {
        return Err(failure(&kickstart, &out));
    }
    Ok(Restart::Restarted(kickstart))
}
