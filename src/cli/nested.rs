//! `potluck dispatch` from inside a job (POTLUCK_JOB and POTLUCK_LEASE set). If the request fits
//! within the job's own lease, the command runs here, in place, synchronously; otherwise the
//! dispatch fails at once. It never waits on the job's own lease, so a job can't deadlock on itself.

use crate::cli::DispatchArgs;
use crate::config::{NodeConfig, Paths};
use crate::recipe::{ProjectFile, Resolved};
use crate::types::{Gpu, Grant, Request, fmt_ms};
use std::os::fd::AsFd;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

/// Whether `req` fits within `grant`: no more RAM than granted, each GPU selector matched by a
/// distinct granted GPU (`gpus` are the granted GPUs as this machine knows them), and
/// `--exclusive` only inside an exclusive lease. CPU is a weight and always fits. `Err` says what
/// doesn't fit.
pub fn fits(req: &Request, grant: &Grant, gpus: &[Gpu]) -> Result<(), String> {
    if req.exclusive && !grant.exclusive {
        return Err("it asks for --exclusive and the job's lease is not exclusive".into());
    }
    if req.ram_mib > grant.ram_mib {
        return Err(format!(
            "it asks for {} MiB of RAM and the job holds {} MiB",
            req.ram_mib, grant.ram_mib
        ));
    }
    if !req.gpus.is_empty() {
        let every: Vec<usize> = (0..gpus.len()).collect();
        if crate::sched::assign(&req.gpus, gpus, &every).is_none() {
            let held: Vec<String> = gpus
                .iter()
                .map(|g| match &g.alias {
                    Some(a) => format!("{} ({a})", g.id),
                    None => g.id.clone(),
                })
                .collect();
            return Err(format!(
                "it asks for GPU {} and the job holds {}",
                req.gpus.join(", "),
                if held.is_empty() {
                    "no GPU".to_string()
                } else {
                    held.join(", ")
                }
            ));
        }
    }
    Ok(())
}

/// A GPU known only by its id: `vendor:device@slot` (Linux) or `macN` (macOS).
pub fn gpu_from_id(id: &str) -> Gpu {
    let mut g = Gpu {
        id: id.to_string(),
        ..Default::default()
    };
    if id.starts_with("mac") {
        g.vendor = "apple".into();
    } else if let Some((vd, _slot)) = id.split_once('@')
        && let Some((v, d)) = vd.split_once(':')
    {
        g.vendor = v.to_ascii_lowercase();
        g.device = d.to_ascii_lowercase();
    }
    g
}

/// The granted GPUs, with names and aliases from this machine's facts where it has them.
fn granted_gpus(grant: &Grant, paths: &Paths, config: &NodeConfig) -> Vec<Gpu> {
    let detected = crate::facts::detect(paths, config)
        .map(|f| f.gpus)
        .unwrap_or_default();
    grant
        .gpus
        .iter()
        .map(|id| {
            detected
                .iter()
                .find(|g| &g.id == id)
                .cloned()
                .unwrap_or_else(|| gpu_from_id(id))
        })
        .collect()
}

/// Everything a nested dispatch must satisfy to run in place: the lease (RAM, GPUs, exclusive,
/// and its run limit within the `time_left` of the job's, when the daemon says), this machine's
/// facts for `--on`, and the workspace when one is named.
pub fn check(
    r: &Resolved,
    grant: &Grant,
    time_left: Option<u64>,
    paths: &Paths,
    config: &NodeConfig,
    want_workspace: Option<&str>,
    here_workspace: Option<&str>,
) -> Result<(), String> {
    let gpus = if r.request.gpus.is_empty() {
        Vec::new()
    } else {
        granted_gpus(grant, paths, config)
    };
    fits(&r.request, grant, &gpus)?;
    if let Some(left) = time_left
        && r.timeout_ms > left
    {
        return Err(format!(
            "it asks for a {} run limit and the job has {} left of its own",
            fmt_ms(r.timeout_ms),
            fmt_ms(left)
        ));
    }
    if !r.on.is_empty() {
        let facts = crate::facts::detect(paths, config)
            .map_err(|e| format!("reading this machine's facts: {e:#}"))?;
        if let Some(unmet) = facts.unmet(&r.on) {
            return Err(format!("it {unmet}"));
        }
    }
    if let Some(w) = want_workspace
        && here_workspace != Some(w)
    {
        return Err(format!(
            "it names workspace {w} and the job runs in {}",
            here_workspace.unwrap_or("another workspace")
        ));
    }
    Ok(())
}

/// The first option on the command line that only means something for a job of its own, and so
/// can't apply to a dispatch run in place inside job `job`: (the option, why, in plain words).
/// `--priority` and `--cpu` aren't among them: in place a command starts at once and shares the
/// job's CPU.
pub fn refused_option(args: &DispatchArgs, job: &str) -> Option<(&'static str, String)> {
    let checks: [(bool, &'static str, String); 10] = [
        (
            !args.keep.is_empty(),
            "--keep",
            format!("only job {job}'s own --keep copies files back"),
        ),
        (
            args.hold.is_some(),
            "--hold",
            format!("its workspace is job {job}'s, held as job {job}'s dispatch asked"),
        ),
        (
            args.key.is_some(),
            "--key",
            "it leaves no job behind for a key to name".into(),
        ),
        (
            !args.labels.is_empty(),
            "--label",
            "it leaves no job behind to label".into(),
        ),
        (
            args.no_rerun,
            "--no-rerun",
            format!("job {job}'s own dispatch decides what happens if this machine is lost"),
        ),
        (
            args.kill_on_stall,
            "--kill-on-stall",
            format!("job {job}'s own dispatch decides whether a stall ends it"),
        ),
        (
            !args.without_submodule.is_empty(),
            "--without-submodule",
            format!(
                "it runs in job {job}'s workspace, with the submodules that job's dispatch shipped"
            ),
        ),
        (
            !args.carry.is_empty(),
            "--carry",
            format!(
                "it runs with the mirrors job {job}'s dispatch carried commits into (its POTLUCK_MIRROR_ variables)"
            ),
        ),
        (
            args.advance,
            "--advance",
            format!("it runs in job {job}'s workspace, which stays at the commit job {job} runs"),
        ),
        (
            !args.pools.is_empty(),
            "--pool",
            format!("it runs on job {job}'s machine, in the pools job {job}'s dispatch named"),
        ),
    ];
    checks
        .into_iter()
        .find(|(given, _, _)| *given)
        .map(|(_, flag, why)| (flag, why))
}

/// What of the recipe's settings a dispatch run in place doesn't use: its `keep`, `hold`,
/// `without_submodules` and `pools`.
pub fn recipe_ignored(file: &ProjectFile, recipe: Option<&str>) -> Vec<&'static str> {
    let Some(r) = recipe.and_then(|name| file.recipe.get(name)) else {
        return Vec::new();
    };
    let mut ignored = Vec::new();
    if !r.keep.is_empty() {
        ignored.push("keep");
    }
    if r.hold.is_some() {
        ignored.push("hold");
    }
    if !r.without_submodules.is_empty() {
        ignored.push("without_submodules");
    }
    if !r.pools.is_empty() {
        ignored.push("pools");
    }
    ignored
}

/// The committed potluck.toml at HEAD of the repository the job runs in. Only an error when a
/// recipe is named and there is none.
pub fn project_file_here(cwd: &Path, need: bool) -> anyhow::Result<ProjectFile> {
    let head = crate::gitinfo::git(cwd)
        .args(["rev-parse", "--verify", "-q", "HEAD"])
        .stderr(Stdio::null())
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
    let file = match head {
        Some(commit) => crate::recipe::load_at_commit(cwd, &commit)?,
        None => None,
    };
    match file {
        Some(f) => Ok(f),
        None if need => anyhow::bail!(
            "no committed {} here to take the recipe from",
            crate::recipe::FILE_NAME
        ),
        None => Ok(ProjectFile::default()),
    }
}

/// The top directory of the git work tree containing `cwd` (where a dispatch from outside runs
/// its command: the workspace's root), or `cwd` itself outside one.
pub fn top_of(cwd: &Path) -> std::path::PathBuf {
    crate::gitinfo::git(cwd)
        .args(["rev-parse", "--show-toplevel"])
        .stderr(Stdio::null())
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| std::path::PathBuf::from(s.trim_end_matches('\n')))
        .filter(|p| p.is_dir())
        .unwrap_or_else(|| cwd.to_path_buf())
}

/// Runs the command here: in `cwd`, with this process's environment plus the resolved env
/// (except potluck's own variables, [`crate::daemon::is_job_variable`], which stay the job's) and
/// without the variables it unsets (never potluck's own: [`crate::recipe::resolve`] refuses
/// those), stdin and stderr inherited, stdout inherited too unless `stdout_to_stderr` (so `--json` keeps
/// stdout for its one document). It runs in a process group of its own, which is ended at its
/// run limit like a job: SIGTERM to every process in it, SIGKILL to what is left `grace` later
/// (the machine's `kill_grace_s`), exit 124. Returns its exit code (128 + signal when killed).
pub fn run_in_place(
    r: &Resolved,
    cwd: &Path,
    stdout_to_stderr: bool,
    grace: Duration,
) -> anyhow::Result<i32> {
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    let (prog, args) = r
        .cmd
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("no command to run"))?;
    let mut cmd = Command::new(prog);
    let env = r
        .env
        .iter()
        .filter(|(k, _)| !crate::daemon::is_job_variable(k));
    cmd.args(args).current_dir(cwd).envs(env).process_group(0);
    for k in &r.unset_env {
        cmd.env_remove(k);
    }
    if stdout_to_stderr {
        let err = std::io::stderr().as_fd().try_clone_to_owned()?;
        cmd.stdout(Stdio::from(err));
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("could not start {prog}: {e}"))?;
    let pid = child.id() as libc::pid_t;
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait());
    });
    let status = match rx.recv_timeout(Duration::from_millis(r.timeout_ms)) {
        Ok(status) => status?,
        Err(_) => {
            // SAFETY: kill(2) on the child's process group (its pid): the group lasts while any
            // process in it does, and the child is not reaped until the waiter returns.
            unsafe { libc::kill(-pid, libc::SIGTERM) };
            // The grace is the whole group's: a child that ends at once (a shell) doesn't cut
            // short the cleanup of what it started.
            let until = std::time::Instant::now() + grace;
            let ended = rx.recv_timeout(grace).is_ok();
            while ended && group_alive(pid) && std::time::Instant::now() < until {
                std::thread::sleep(Duration::from_millis(20));
            }
            // What ignored SIGTERM, the child or processes it started, gets SIGKILL. Once the
            // child is reaped, only while the group has members (its id can't be reused then).
            if !ended || group_alive(pid) {
                // SAFETY: as above.
                unsafe { libc::kill(-pid, libc::SIGKILL) };
            }
            if !ended {
                let _ = rx.recv();
            }
            return Ok(crate::types::EXIT_TIMEOUT);
        }
    };
    Ok(status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(0)))
}

/// Whether process group `pgid` has a process left.
fn group_alive(pgid: libc::pid_t) -> bool {
    // SAFETY: kill(2) with signal 0 only checks that the group exists and may be signalled.
    let rc = unsafe { libc::kill(-pgid, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gpu(id: &str, alias: Option<&str>) -> Gpu {
        let mut g = gpu_from_id(id);
        g.alias = alias.map(str::to_string);
        g
    }

    #[test]
    fn requests_fit_the_grant_or_say_why_not() {
        let nv = gpu("10de:2c19@0000:01:00.0", Some("5080"));
        let amd = gpu("1002:744c@0000:03:00.0", None);
        let grant = Grant {
            ram_mib: 8192,
            cpu: 4,
            gpus: vec![nv.id.clone(), amd.id.clone()],
            exclusive: false,
        };
        let gpus = [nv.clone(), amd.clone()];
        let req = |ram: u64, sels: &[&str], excl: bool| Request {
            ram_mib: ram,
            cpu: 16,
            gpus: sels.iter().map(|s| s.to_string()).collect(),
            exclusive: excl,
        };
        assert_eq!(fits(&req(8192, &[], false), &grant, &gpus), Ok(()));
        assert_eq!(fits(&req(4096, &["5080"], false), &grant, &gpus), Ok(()));
        assert_eq!(
            fits(&req(4096, &["any", "nvidia"], false), &grant, &gpus),
            Ok(())
        );
        assert_eq!(
            fits(&req(4096, &["amd", "10de:2c19"], false), &grant, &gpus),
            Ok(())
        );
        let e = fits(&req(16384, &[], false), &grant, &gpus).unwrap_err();
        assert!(e.contains("16384 MiB") && e.contains("8192 MiB"), "{e}");
        let e = fits(&req(1024, &["nvidia", "nvidia"], false), &grant, &gpus).unwrap_err();
        assert!(e.contains("GPU nvidia, nvidia"), "{e}");
        assert!(fits(&req(1024, &["any", "any", "any"], false), &grant, &gpus).is_err());
        assert!(fits(&req(1024, &["intel"], false), &grant, &gpus).is_err());
        let e = fits(&req(1024, &[], true), &grant, &gpus).unwrap_err();
        assert!(e.contains("exclusive"), "{e}");
        let excl = Grant {
            exclusive: true,
            ..grant.clone()
        };
        assert_eq!(fits(&req(1024, &["any"], true), &excl, &gpus), Ok(()));
        let none = Grant {
            gpus: vec![],
            ..grant
        };
        let e = fits(&req(1024, &["any"], false), &none, &[]).unwrap_err();
        assert!(e.contains("no GPU"), "{e}");
    }

    /// The scheduler's polynomial assignment: a job holding many GPUs doesn't wait minutes
    /// for a request that can't fit (backtracking tried every order of 12 GPUs).
    #[test]
    fn many_gpus_are_checked_quickly() {
        let mut gpus: Vec<Gpu> = (0..11)
            .map(|i| gpu(&format!("10de:2c19@0000:{i:02x}:00.0"), None))
            .collect();
        gpus.push(gpu("1002:744c@0000:20:00.0", None));
        let grant = Grant {
            ram_mib: 1024,
            cpu: 1,
            gpus: gpus.iter().map(|g| g.id.clone()).collect(),
            exclusive: false,
        };
        let mut sels = vec!["any".to_string(); 10];
        sels.extend(["amd".to_string(), "amd".to_string()]);
        let req = Request {
            ram_mib: 1024,
            cpu: 1,
            gpus: sels,
            exclusive: false,
        };
        let started = std::time::Instant::now();
        assert!(fits(&req, &grant, &gpus).is_err());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn gpu_ids() {
        let g = gpu_from_id("10DE:2C19@0000:01:00.0");
        assert_eq!((g.vendor.as_str(), g.device.as_str()), ("10de", "2c19"));
        assert_eq!(gpu_from_id("mac0").vendor, "apple");
    }
}
