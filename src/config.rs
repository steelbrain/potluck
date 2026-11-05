//! Where potluck keeps its state, and the node config.
//!
//! State: `$POTLUCK_HOME`, default `~/.local/state/potluck` on Linux and macOS. It must be on a
//! copy-on-write filesystem. Layout:
//! - `potluck.sock`, `potluck.db`, `daemon.log`, `daemon.lock` (one daemon per home)
//! - `keys/` (this machine's key pair and the trust records, phase 2)
//! - `mirrors/<repo_id>.git` (bare mirrors), `bases/<repo_id>` (base checkouts to clone from)
//! - `workspaces/<workspace_id>` (one commit's tree) and `workspaces/<workspace_id>.tmp` (its
//!   TMPDIR, beside the tree)
//! - `jobs/<job_id>/`: `output.log`, `status.json`, `keep/` and `crash/` (14 days)
//!
//! Config: `$POTLUCK_CONFIG`, default `~/.config/potluck/node.toml`. Optional. Example:
//!
//! ```toml
//! name = "studio"                 # default: the short hostname
//! port = 7465                     # the one port members reach this machine on, while in a cluster
//! listen = "127.0.0.1"            # the address to listen on; default every interface (0.0.0.0)
//! address = "studio.lan:7465"     # how members reach it; default: the hostname and the port
//! ram_gib = 56                    # admission capacity; default total RAM minus max(4 GiB, 10%);
//!                                 # more than 0, at most 1048576 (1 PiB)
//! workspace_budget_percent = 10   # workspaces may use this much of the disk, 0 to 100 (idle
//!                                 # ones removed),
//! workspace_budget_min_gib = 50   # but never less than this
//! kill_grace_s = 10               # SIGTERM to SIGKILL; at most 86400 (a day)
//! stall_minutes = 10              # under 1% of a core and no output this long: sample stacks,
//!                                 # mark stalled; 0: never; at most 10080 (a week)
//! retention_days = 14             # job records, logs and kept files
//! log_max_mib = 1024              # a job's log keeps this much of its output, then drops the
//!                                 # rest but its last 16 MiB, added at the end; 0: no limit
//!
//! [[gpu]]
//! match = "nvidia"                # a GPU selector naming one detected GPU
//! alias = "5080"
//! env = { SOME_PROJECT_GPU = "5080" }  # set in jobs holding this GPU
//! ```

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const DEFAULT_PORT: u16 = 7465;
/// The longest `kill_grace_s`: a day.
const MAX_KILL_GRACE_S: u64 = 86_400;
/// The longest `stall_minutes`: a week.
const MAX_STALL_MINUTES: u64 = 7 * 24 * 60;
/// The most `ram_gib`: 1 PiB (a job's limit in bytes must fit in 64 bits).
const MAX_RAM_GIB: f64 = 1_048_576.0;

/// The daemon's systemd user unit (Linux).
pub const SYSTEMD_UNIT: &str = "potluck.service";
/// The daemon's LaunchAgent label (macOS), installed as `~/Library/LaunchAgents/<label>.plist`.
pub const LAUNCHD_LABEL: &str = "potluck.daemon";

#[derive(Clone, Debug)]
pub struct Paths {
    pub home: PathBuf,
    pub config: PathBuf,
}

impl Paths {
    /// From `$POTLUCK_HOME` and `$POTLUCK_CONFIG`, or the defaults, made absolute (the daemon
    /// hands them to jobs, which run elsewhere).
    pub fn from_env() -> anyhow::Result<Paths> {
        Paths::from_vars(
            std::env::var_os("HOME"),
            std::env::var_os("POTLUCK_HOME"),
            std::env::var_os("POTLUCK_CONFIG"),
        )
    }

    fn from_vars(
        user_home: Option<std::ffi::OsString>,
        home: Option<std::ffi::OsString>,
        config: Option<std::ffi::OsString>,
    ) -> anyhow::Result<Paths> {
        let user_home = user_home
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
        let absolute = |p: PathBuf, var: &str| {
            std::path::absolute(&p)
                .map_err(|e| anyhow::anyhow!("{var} {:?} can't be made absolute: {e}", p.display()))
        };
        let home = home
            .map(PathBuf::from)
            .unwrap_or_else(|| user_home.join(".local/state/potluck"));
        let config = config
            .map(PathBuf::from)
            .unwrap_or_else(|| user_home.join(".config/potluck/node.toml"));
        Ok(Paths {
            home: absolute(home, "POTLUCK_HOME")?,
            config: absolute(config, "POTLUCK_CONFIG")?,
        })
    }

    pub fn socket(&self) -> PathBuf {
        self.home.join("potluck.sock")
    }
    pub fn db(&self) -> PathBuf {
        self.home.join("potluck.db")
    }
    pub fn daemon_log(&self) -> PathBuf {
        self.home.join("daemon.log")
    }
    pub fn keys_dir(&self) -> PathBuf {
        self.home.join("keys")
    }
    pub fn mirror(&self, repo_id: &str) -> PathBuf {
        self.home.join("mirrors").join(format!("{repo_id}.git"))
    }
    /// Where the views of mirrors served to other members ([`crate::tunnel::View`]) are made, one
    /// for each fetch; emptied when the daemon starts.
    pub fn upload_pack_views(&self) -> PathBuf {
        self.home.join("upload-pack")
    }
    pub fn base(&self, repo_id: &str) -> PathBuf {
        self.home.join("bases").join(repo_id)
    }
    pub fn workspace(&self, id: &str) -> PathBuf {
        self.home.join("workspaces").join(id)
    }
    /// A workspace's TMPDIR: beside its tree (so it never dirties it), on the same volume, created
    /// with the workspace and removed with it.
    pub fn workspace_tmp(&self, id: &str) -> PathBuf {
        self.home.join("workspaces").join(format!("{id}.tmp"))
    }
    /// A job's directory on the submitting machine: output log and kept files.
    pub fn job_dir(&self, id: &str) -> PathBuf {
        self.home.join("jobs").join(id)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub name: Option<String>,
    pub port: Option<u16>,
    /// The IP address the daemon listens on for members (`127.0.0.1`, `::`, a LAN address).
    /// Default: every IPv4 interface. Loopback suits tests and setups that reach members through
    /// ssh port forwards.
    pub listen: Option<String>,
    /// How other members reach this machine, `host:port` (or a bare host, given the port). Default:
    /// the hostname and the port. Pairing hands it to the cluster; members learn changes on connect.
    pub address: Option<String>,
    pub ram_gib: Option<f64>,
    pub workspace_budget_percent: Option<f64>,
    pub workspace_budget_min_gib: Option<u64>,
    pub kill_grace_s: Option<u64>,
    pub stall_minutes: Option<u64>,
    pub retention_days: Option<u32>,
    /// How much of a job's output its log keeps, in MiB; 0: no limit.
    pub log_max_mib: Option<u64>,
    #[serde(default)]
    pub gpu: Vec<GpuConfig>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GpuConfig {
    /// A GPU selector (see `types::gpu_matches`) that must name exactly one detected GPU.
    #[serde(rename = "match")]
    pub matches: String,
    pub alias: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl NodeConfig {
    /// Reads the config; a missing file is an empty config.
    pub fn load(paths: &Paths) -> anyhow::Result<NodeConfig> {
        let config: NodeConfig = match std::fs::read_to_string(&paths.config) {
            Ok(text) => toml::from_str(&text).map_err(|e| {
                anyhow::anyhow!("{}: {}", paths.config.display(), parse_error(&text, &e))
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(NodeConfig::default()),
            Err(e) => return Err(anyhow::anyhow!("{}: {e}", paths.config.display())),
        };
        config
            .check_limits()
            .map_err(|e| anyhow::anyhow!("{}: {e}", paths.config.display()))?;
        Ok(config)
    }

    /// Refuses durations too long to mean anything (and too long to count in milliseconds), and
    /// amounts that can't be meant (a RAM capacity of 0, NaN or inf; a percentage outside 0-100),
    /// rather than putting a default or a bound in their place.
    fn check_limits(&self) -> anyhow::Result<()> {
        if let Some(g) = self.ram_gib
            && !(g > 0.0 && g <= MAX_RAM_GIB)
        {
            anyhow::bail!(
                "ram_gib = {g}: give the GiB jobs may reserve here, more than 0 and at most \
                 {MAX_RAM_GIB} (1 PiB), or leave it out for total RAM minus max(4 GiB, 10%)"
            );
        }
        if let Some(p) = self.workspace_budget_percent
            && !(0.0..=100.0).contains(&p)
        {
            anyhow::bail!("workspace_budget_percent = {p}: a percentage of the disk, 0 to 100");
        }
        for (key, value, most, what) in [
            ("kill_grace_s", self.kill_grace_s, MAX_KILL_GRACE_S, "a day"),
            (
                "stall_minutes",
                self.stall_minutes,
                MAX_STALL_MINUTES,
                "a week",
            ),
        ] {
            if let Some(v) = value.filter(|v| *v > most) {
                anyhow::bail!("{key} = {v}: at most {most} ({what})");
            }
        }
        Ok(())
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }
    /// The address to listen on: `listen`, or every IPv4 interface.
    pub fn listen_ip(&self) -> anyhow::Result<std::net::IpAddr> {
        match self.listen.as_deref().map(str::trim) {
            None | Some("") => Ok(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
            Some(s) => s
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse()
                .map_err(|_| {
                    anyhow::anyhow!(
                        "listen = {s:?}: not an IP address (for example 127.0.0.1 or ::)"
                    )
                }),
        }
    }
    pub fn kill_grace_ms(&self) -> u64 {
        self.kill_grace_s.unwrap_or(10).saturating_mul(1000)
    }
    pub fn stall_ms(&self) -> u64 {
        self.stall_minutes.unwrap_or(10).saturating_mul(60_000)
    }
    pub fn retention_ms(&self) -> u64 {
        self.retention_days.unwrap_or(14) as u64 * 86_400_000
    }
    /// How many bytes of a job's output its log keeps before it drops the rest (all but the last
    /// 16 MiB); 0: no limit. Default 1 GiB.
    pub fn log_max_bytes(&self) -> u64 {
        self.log_max_mib.unwrap_or(1024).saturating_mul(1 << 20)
    }
    /// The workspace disk budget for a volume of `volume_bytes`.
    pub fn workspace_budget_bytes(&self, volume_bytes: u64) -> u64 {
        let pct = self
            .workspace_budget_percent
            .unwrap_or(10.0)
            .clamp(0.0, 100.0);
        let min = self
            .workspace_budget_min_gib
            .unwrap_or(50)
            .saturating_mul(1 << 30);
        ((volume_bytes as f64 * pct / 100.0) as u64).max(min)
    }
}

/// Why the config didn't parse. A `[[gpu]]` entry that still has `legacy_locks` gets its own
/// message: the bare error would only call the key unknown.
fn parse_error(text: &str, e: &toml::de::Error) -> String {
    let legacy_locks = toml::from_str::<toml::Table>(text).is_ok_and(|t| {
        t.get("gpu")
            .and_then(|g| g.as_array())
            .is_some_and(|gpus| gpus.iter().any(|g| g.get("legacy_locks").is_some()))
    });
    if legacy_locks {
        return "`legacy_locks` in [[gpu]] was removed: potluck no longer holds lock files while \
                a GPU is leased. Delete the key; [[gpu]] takes match, alias and env"
            .into();
    }
    e.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A relative POTLUCK_HOME or POTLUCK_CONFIG is taken from the current directory: jobs run in
    /// their workspaces, where a relative path would name something else (on macOS every job's
    /// exit status was lost). An empty one is refused.
    #[test]
    fn paths_are_absolute() {
        let cwd = std::env::current_dir().unwrap();
        let p = Paths::from_vars(
            Some("/home/u".into()),
            Some("state".into()),
            Some("cfg/node.toml".into()),
        )
        .unwrap();
        assert_eq!(p.home, cwd.join("state"));
        assert_eq!(p.config, cwd.join("cfg/node.toml"));
        let p = Paths::from_vars(Some("/home/u".into()), None, None).unwrap();
        assert_eq!(p.home, PathBuf::from("/home/u/.local/state/potluck"));
        assert_eq!(p.config, PathBuf::from("/home/u/.config/potluck/node.toml"));
        let e = Paths::from_vars(Some("/home/u".into()), Some("".into()), None).unwrap_err();
        assert!(
            format!("{e}").starts_with("POTLUCK_HOME \"\" can't be made absolute"),
            "{e}"
        );
    }

    #[test]
    fn listen_addresses() {
        let mut c = NodeConfig::default();
        assert_eq!(c.listen_ip().unwrap().to_string(), "0.0.0.0");
        for (given, want) in [("127.0.0.1", "127.0.0.1"), ("::1", "::1"), ("[::]", "::")] {
            c.listen = Some(given.into());
            assert_eq!(c.listen_ip().unwrap().to_string(), want);
        }
        c.listen = Some("localhost".into());
        assert!(c.listen_ip().is_err());
        let c: NodeConfig = toml::from_str("listen = \"127.0.0.1\"\nport = 0").unwrap();
        assert_eq!(c.listen_ip().unwrap().to_string(), "127.0.0.1");
    }

    #[test]
    fn legacy_locks_is_refused_as_removed() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            config: dir.path().join("node.toml"),
        };
        let load = |text: &str| {
            std::fs::write(&paths.config, text).unwrap();
            NodeConfig::load(&paths)
        };
        for text in [
            "[[gpu]]\nmatch = \"nvidia\"\nlegacy_locks = [\"/tmp/gpu.lock\"]\n",
            "gpu = [{ match = \"nvidia\", legacy_locks = [] }]\n",
        ] {
            let e = load(text).unwrap_err().to_string();
            assert!(
                e.starts_with(&format!("{}: ", paths.config.display()))
                    && e.contains("`legacy_locks` in [[gpu]] was removed"),
                "{e}"
            );
        }
        let c =
            load("[[gpu]]\nmatch = \"nvidia\"\nalias = \"5080\"\nenv = { A = \"1\" }\n").unwrap();
        assert_eq!(c.gpu[0].env["A"], "1");
        // Any other unknown key keeps the parser's own message.
        let e = load("[[gpu]]\nmatch = \"nvidia\"\nlocks = []\n")
            .unwrap_err()
            .to_string();
        assert!(e.contains("unknown field `locks`"), "{e}");
    }

    #[test]
    fn values_that_cant_be_meant_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            config: dir.path().join("node.toml"),
        };
        let load = |text: &str| {
            std::fs::write(&paths.config, text).unwrap();
            NodeConfig::load(&paths)
        };
        for (text, want) in [
            (
                "kill_grace_s = 18446744073709551615",
                "kill_grace_s = 18446744073709551615: at most 86400 (a day)",
            ),
            (
                "kill_grace_s = 86401",
                "kill_grace_s = 86401: at most 86400 (a day)",
            ),
            (
                "stall_minutes = 307445734561826",
                "stall_minutes = 307445734561826: at most 10080 (a week)",
            ),
        ] {
            let e = load(text).unwrap_err().to_string();
            assert_eq!(e, format!("{}: {want}", paths.config.display()));
        }
        for (text, want) in [
            ("ram_gib = 0", "ram_gib = 0: give the GiB"),
            ("ram_gib = -8", "ram_gib = -8: give the GiB"),
            ("ram_gib = nan", "ram_gib = NaN: give the GiB"),
            ("ram_gib = inf", "ram_gib = inf: give the GiB"),
            ("ram_gib = 2e6", "ram_gib = 2000000: give the GiB"),
            (
                "workspace_budget_percent = 150",
                "workspace_budget_percent = 150: a percentage of the disk, 0 to 100",
            ),
            (
                "workspace_budget_percent = -1",
                "workspace_budget_percent = -1: a percentage of the disk, 0 to 100",
            ),
            (
                "workspace_budget_percent = nan",
                "workspace_budget_percent = NaN: a percentage of the disk, 0 to 100",
            ),
        ] {
            let e = load(text).unwrap_err().to_string();
            let want = format!("{}: {want}", paths.config.display());
            assert!(e.starts_with(&want), "{e}");
        }
        let c = load("ram_gib = 0.5\nworkspace_budget_percent = 0\n").unwrap();
        assert_eq!(c.workspace_budget_bytes(1 << 40), 50 << 30);
        let c = load("kill_grace_s = 86400\nstall_minutes = 10080\n").unwrap();
        assert_eq!((c.kill_grace_ms(), c.stall_ms()), (86_400_000, 604_800_000));
        let c =
            load("stall_minutes = 0\nworkspace_budget_min_gib = 18446744073709551615\n").unwrap();
        assert_eq!(c.stall_ms(), 0);
        assert_eq!(c.workspace_budget_bytes(1 << 40), u64::MAX);
    }
}
