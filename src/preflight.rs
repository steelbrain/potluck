//! The hard requirements the daemon checks before it runs, refusing with a plain message otherwise:
//! lingering enabled for the user, the systemd user manager answering, and the unified cgroup
//! hierarchy with the memory and pids controllers in the user manager's cgroup (Linux), git 2.31
//! or newer on PATH, and potluck's state directory on a copy-on-write filesystem
//! ([`crate::cow::check`]), and the state directory and config paths in UTF-8 (jobs get them as
//! text) without line breaks (git's output is read line by line). The state directory is created
//! if missing (mode 0700).
//! Every problem found is reported at once, each with its fix.

use crate::config::Paths;
use anyhow::bail;

pub fn check(paths: &Paths) -> anyhow::Result<()> {
    let mut problems = Vec::new();
    #[cfg(target_os = "linux")]
    {
        problems.extend(linger_problem());
        // SAFETY: getuid has no preconditions and cannot fail.
        let uid = unsafe { libc::getuid() };
        problems.extend(cgroup_problem(std::path::Path::new("/sys/fs/cgroup"), uid));
    }
    problems.extend(git_problem());
    // Jobs get these paths as text (POTLUCK_HOME, POTLUCK_CONFIG, and TMPDIR in the state
    // directory), and the store keeps workspace paths as text; git's output and the service
    // file are read line by line.
    let text = |path: &std::path::Path, what: &str, var: &str| {
        let why = match path.to_str() {
            None => "is not valid UTF-8, and jobs get it as text",
            Some(p) if p.contains(['\n', '\r']) => {
                "has a line break, and git's output naming paths in it is read line by line"
            }
            Some(_) => return None,
        };
        Some(format!(
            "{what} {:?} {why}. Set {var} to another path",
            path.display().to_string()
        ))
    };
    problems.extend(text(
        &paths.config,
        "potluck's config path",
        "POTLUCK_CONFIG",
    ));
    if let Some(p) = text(&paths.home, "potluck's state directory", "POTLUCK_HOME") {
        problems.push(p);
    } else {
        match create_home(&paths.home) {
            Ok(()) => {
                if let Err(e) = crate::cow::check(&paths.home) {
                    problems.push(format!("{e:#}"));
                }
            }
            Err(e) => problems.push(format!(
                "can't create potluck's state directory {}: {e}. Set POTLUCK_HOME to a directory \
                 potluck may write to",
                paths.home.display()
            )),
        }
    }
    if !problems.is_empty() {
        bail!(
            "this machine can't run potluck yet:\n- {}",
            problems.join("\n- ")
        );
    }
    Ok(())
}

/// The oldest git potluck works with: `git rev-parse --path-format` is new in 2.31. An older git
/// prints that option back as the first line of its answer, where potluck reads a path.
const MIN_GIT: (u32, u32) = (2, 31);

/// What is wrong with the git the daemon runs (the first `git` on PATH), if anything: none there,
/// or one older than [`MIN_GIT`].
fn git_problem() -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let need = format!("potluck needs git {}.{} or newer", MIN_GIT.0, MIN_GIT.1);
    let path = std::env::var_os("PATH").unwrap_or_default();
    let Some(git) = std::env::split_paths(&path)
        .map(|d| d.join("git"))
        .find(|p| {
            p.metadata()
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
    else {
        return Some(format!(
            "{need}, and there is no git on PATH ({})",
            path.to_string_lossy()
        ));
    };
    let out = match std::process::Command::new(&git)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .output()
    {
        Ok(out) if out.status.success() => out,
        Ok(out) => {
            return Some(format!(
                "{need}, and `{} --version` failed ({}): {}",
                git.display(),
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Err(e) => return Some(format!("{need}, and {} can't run: {e}", git.display())),
    };
    let version = String::from_utf8_lossy(&out.stdout);
    let version = version.trim().trim_start_matches("git version ");
    too_old(version).then(|| {
        format!(
            "{need} (`git rev-parse --path-format` is new in {}.{}), and {} is git {version}. \
             Put a newer git in its place, or in a directory before it on PATH",
            MIN_GIT.0,
            MIN_GIT.1,
            git.display()
        )
    })
}

/// Whether git `version` ("2.30.2", "2.39.5 (Apple Git-154)") is older than [`MIN_GIT`]. One
/// that doesn't read as a version isn't refused.
fn too_old(version: &str) -> bool {
    let mut nums = version
        .split(|c: char| !c.is_ascii_digit())
        .map(str::parse::<u32>);
    match (nums.next(), nums.next()) {
        (Some(Ok(major)), Some(Ok(minor))) => (major, minor) < MIN_GIT,
        _ => false,
    }
}

/// Refuses node name `name` when this machine's records already give it another name: the daemon
/// refuses to start under it ([`renamed`] says why and what to do).
pub fn check_name(paths: &Paths, name: &str) -> anyhow::Result<()> {
    match crate::mesh::recorded_name(&paths.keys_dir()) {
        Some((had, in_cluster)) if had != name => bail!(renamed(paths, &had, name, in_cluster)),
        _ => Ok(()),
    }
}

/// Why the daemon won't run as `name` on a machine whose records named it `had` when it first
/// ran, and how to go on.
pub fn renamed(paths: &Paths, had: &str, name: &str, in_cluster: bool) -> String {
    let config = paths.config.display();
    let keep = format!(
        "this machine has been {had} since its daemon first ran, and {config} now names it \
         {name}; a machine keeps the name it started with. Set `name = \"{had}\"` in {config}"
    );
    if in_cluster {
        return format!("{keep}: the cluster knows it by that name");
    }
    format!(
        "{keep}, or, since it is in no cluster yet, stop its daemon, remove {} (its jobs, logs and \
         workspaces go with it) and start it again to begin as {name}",
        paths.home.display()
    )
}

/// Creates the state directory (and its parents), readable by this user only: it holds job specs
/// with their `--env` values, logs and sources.
fn create_home(home: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    if let Some(parent) = home.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::DirBuilder::new().mode(0o700).create(home) {
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && home.is_dir() => Ok(()),
        other => other,
    }
}

/// What is wrong with the systemd user manager's lingering for this user, if anything.
#[cfg(target_os = "linux")]
fn linger_problem() -> Option<String> {
    if !std::path::Path::new("/run/systemd/system").is_dir() {
        return Some(
            "systemd is not running: potluck needs the systemd user manager on Linux".into(),
        );
    }
    let user = match crate::service::user_name() {
        Ok(u) => u,
        Err(e) => return Some(format!("{e:#}")),
    };
    if !crate::service::linger_enabled(&user) {
        return Some(format!(
            "lingering is off for user {user}, so systemd stops potluck (and its jobs) when you \
             log out. Fix: sudo loginctl enable-linger {user}"
        ));
    }
    // Every job starts through the user manager: it must answer, not only be lingering.
    let why = crate::exec::linux::user_manager_problem()?;
    // SAFETY: getuid has no preconditions and cannot fail.
    let uid = unsafe { libc::getuid() };
    let runtime = match std::env::var("XDG_RUNTIME_DIR") {
        Ok(dir) => format!("XDG_RUNTIME_DIR is {dir}"),
        Err(_) => "XDG_RUNTIME_DIR is not set".to_string(),
    };
    Some(format!(
        "lingering is on for user {user}, but the systemd user manager doesn't answer \
         (`systemctl --user`: {why}); potluck starts every job through it. Check that \
         `systemctl --user status` works in the daemon's environment ({runtime}; the user \
         manager's is /run/user/{uid})"
    ))
}

/// What in the cgroup tree at `root` keeps this machine from running jobs, if anything. A job's
/// unit runs in the cgroup of user `uid`'s systemd manager, and potluck finds its processes and
/// memory under `root` by the path systemd gives, so the unified (v2) hierarchy must be mounted
/// there. systemd enforces a job's MemoryMax and TasksMax only with the memory and pids
/// controllers in the user manager's cgroup. A user manager that isn't running (no cgroup) is
/// for linger_problem to report.
#[cfg(any(target_os = "linux", test))]
fn cgroup_problem(root: &std::path::Path, uid: u32) -> Option<String> {
    let controllers =
        |dir: &std::path::Path| std::fs::read_to_string(dir.join("cgroup.controllers"));
    let Ok(kernel) = controllers(root) else {
        return Some(format!(
            "{} is not the unified cgroup hierarchy (cgroup v2; it has no cgroup.controllers): \
             this machine runs cgroup v1 or systemd's hybrid layout, where potluck can't find a \
             job's processes and ends every job as soon as it starts. Fix: add \
             systemd.unified_cgroup_hierarchy=1 to the kernel command line and reboot",
            root.display()
        ));
    };
    let manager = root.join(format!("user.slice/user-{uid}.slice/user@{uid}.service"));
    let delegated = controllers(&manager).ok()?;
    let has = |list: &str, c: &str| list.split_whitespace().any(|x| x == c);
    let missing: Vec<&str> = ["memory", "pids"]
        .into_iter()
        .filter(|c| !has(&delegated, c))
        .collect();
    if missing.is_empty() {
        return None;
    }
    let (names, it) = match missing[..] {
        [one] => (one.to_string(), "it"),
        _ => (missing.join(" or "), "them"),
    };
    let limits = missing
        .iter()
        .map(|c| match *c {
            "memory" => "RAM limit (MemoryMax)",
            _ => "task limit (TasksMax)",
        })
        .collect::<Vec<_>>()
        .join(" and ");
    let fix = if missing.iter().all(|c| has(&kernel, c)) {
        format!(
            "delegate {it} to the user manager: put `[Service]` and `Delegate=cpu memory pids` in \
             /etc/systemd/system/user@.service.d/delegate.conf and reboot"
        )
    } else {
        format!(
            "the kernel has {it} off ({} lacks {it} too); turn {it} on at boot, for example by \
             removing cgroup_disable={} from the kernel command line, and reboot",
            root.join("cgroup.controllers").display(),
            missing.join(",")
        )
    };
    Some(format!(
        "the systemd user manager's cgroup {} has no {names} controller, so jobs run without \
         their {limits}. Fix: {fix}",
        manager.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_the_state_directory() {
        // The system temp dir may not be CoW (tmpfs on many Linux machines): only the directory's
        // creation is checked here; the CoW checks have their own tests.
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("a/b/state");
        let paths = Paths {
            home: home.clone(),
            config: dir.path().join("node.toml"),
        };
        let result = check(&paths);
        assert!(home.is_dir());
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&home).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "{mode:o}");
        if let Err(e) = result {
            let msg = format!("{e:#}");
            // The CLI prints it after "potluck: ".
            assert!(
                msg.starts_with("this machine can't run potluck yet:"),
                "{msg}"
            );
            assert!(
                msg.contains("copy-on-write") || msg.contains("linger"),
                "{msg}"
            );
        }
    }

    /// A cgroup tree as [`cgroup_problem`] reads it for user 1000: the kernel's controllers in
    /// the root's cgroup.controllers, and the user manager's in its cgroup's.
    fn cgroup_tree(kernel: &str, manager: Option<&str>) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cgroup.controllers"), kernel).unwrap();
        if let Some(manager) = manager {
            let cg = dir
                .path()
                .join("user.slice/user-1000.slice/user@1000.service");
            std::fs::create_dir_all(&cg).unwrap();
            std::fs::write(cg.join("cgroup.controllers"), manager).unwrap();
        }
        dir
    }

    /// On a cgroup v1 or hybrid host every job ended about half a second after it started,
    /// "killed: its processes ended without recording an exit status": the unit's cgroup isn't
    /// under /sys/fs/cgroup there, so potluck found no processes in it. The daemon started.
    #[test]
    fn a_host_without_the_unified_cgroup_hierarchy_is_refused() {
        // The hybrid layout: a tmpfs holding one hierarchy per v1 controller, and cgroup v2
        // without controllers at unified/.
        let dir = tempfile::tempdir().unwrap();
        for sub in ["memory", "pids", "systemd", "unified"] {
            std::fs::create_dir(dir.path().join(sub)).unwrap();
        }
        std::fs::write(dir.path().join("unified/cgroup.controllers"), "\n").unwrap();
        let msg = cgroup_problem(dir.path(), 1000).expect("not cgroup v2");
        assert!(
            msg.contains("is not the unified cgroup hierarchy")
                && msg.contains("systemd.unified_cgroup_hierarchy=1"),
            "{msg}"
        );
    }

    #[test]
    fn the_user_managers_cgroup_needs_the_memory_and_pids_controllers() {
        let all = "cpuset cpu io memory hugetlb pids rdma misc\n";
        let tree = cgroup_tree(all, Some("cpuset cpu io memory pids\n"));
        assert_eq!(cgroup_problem(tree.path(), 1000), None);
        // No user manager: linger_problem says so.
        let tree = cgroup_tree(all, None);
        assert_eq!(cgroup_problem(tree.path(), 1000), None);

        // Not delegated to the user manager.
        let tree = cgroup_tree(all, Some("cpu io\n"));
        let msg = cgroup_problem(tree.path(), 1000).expect("not delegated");
        assert!(
            msg.contains("user@1000.service has no memory or pids controller")
                && msg.contains("RAM limit (MemoryMax) and task limit (TasksMax)")
                && msg.contains("delegate them to the user manager")
                && msg.contains("Delegate=cpu memory pids"),
            "{msg}"
        );

        // Off in the kernel (cgroup_disable=memory).
        let tree = cgroup_tree("cpuset cpu io pids\n", Some("cpu io pids\n"));
        let msg = cgroup_problem(tree.path(), 1000).expect("off in the kernel");
        assert!(
            msg.contains(
                "has no memory controller, so jobs run without their RAM limit (MemoryMax)."
            ) && msg.contains("the kernel has it off")
                && msg.contains("cgroup_disable=memory "),
            "{msg}"
        );
    }

    #[test]
    fn git_before_2_31_is_too_old() {
        for old in ["2.30.2", "2.25.1", "1.9.5", "2.30.0.windows.1"] {
            assert!(too_old(old), "{old}");
        }
        for new in [
            "2.31.0",
            "2.54.0 (Apple Git-157)",
            "2.56.0",
            "3.0.0",
            "2.43.GIT",
            "",
            "x",
        ] {
            assert!(!too_old(new), "{new}");
        }
    }

    /// A state directory or config path that isn't UTF-8 is refused before anything is created:
    /// jobs would get a mangled TMPDIR and POTLUCK_HOME, and the store can't keep workspace paths.
    #[test]
    fn paths_that_are_not_utf8_are_refused() {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        let bad = std::ffi::OsStr::from_bytes(b"home-\xff");
        let paths = Paths {
            home: dir.path().join(bad),
            config: dir.path().join(bad).join("node.toml"),
        };
        let msg = format!("{:#}", check(&paths).expect_err("not UTF-8"));
        assert!(
            msg.contains("potluck's state directory") && msg.contains("Set POTLUCK_HOME"),
            "{msg}"
        );
        assert!(
            msg.contains("potluck's config path") && msg.contains("Set POTLUCK_CONFIG"),
            "{msg}"
        );
        assert!(!paths.home.exists());

        // A line break: every seeding failed ("the base checkout ... went away").
        let paths = Paths {
            home: dir.path().join("home\nx"),
            config: dir.path().join("node.toml"),
        };
        let msg = format!("{:#}", check(&paths).expect_err("a line break"));
        assert!(msg.contains("has a line break"), "{msg}");
        assert!(!paths.home.exists());
    }
}
