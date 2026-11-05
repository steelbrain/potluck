//! The records every part of potluck shares: jobs, requests, grants, leases, workspaces and
//! machine facts. Changing a type here changes the contract between modules: keep it deliberate.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unix time in milliseconds.
pub type Millis = u64;
/// A machine's name in the cluster.
pub type NodeName = String;
/// A job's id: `<submitting node>-<seq>`, for example `studio-42`.
pub type JobId = String;
/// A workspace's id: `<runner node>-w<seq>`, for example `msi-w7`.
pub type WorkspaceId = String;

pub const SECOND_MS: u64 = 1_000;
pub const MINUTE_MS: u64 = 60 * SECOND_MS;
pub const HOUR_MS: u64 = 60 * MINUTE_MS;

/// Every workspace is held at least this long after each invocation in it.
pub const MIN_HOLD_MS: u64 = 5 * MINUTE_MS;
/// The longest hold one request may ask for.
pub const MAX_HOLD_MS: u64 = 24 * HOUR_MS;

/// Exit codes for endings potluck causes. Otherwise `wait` exits with the job's own code.
pub const EXIT_TIMEOUT: i32 = 124;
pub const EXIT_LOST: i32 = 125;
pub const EXIT_CANCELLED: i32 = 130;
pub const EXIT_OVER_MEMORY: i32 = 137;

/// The current Unix time in milliseconds.
pub fn now_ms() -> Millis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as Millis)
        .unwrap_or(0)
}

/// Formats a duration the way `status`, `why` and reminders print it, to the second and in the
/// form `--timeout` and `hold` take: `45s`, `14m5s`, `2h5m`, `0s` under a second.
pub fn fmt_ms(ms: u64) -> String {
    let s = ms / 1000;
    let (h, m, s) = (s / 3600, (s % 3600) / 60, s % 60);
    let mut out = String::new();
    if h > 0 {
        out.push_str(&format!("{h}h"));
    }
    if m > 0 {
        out.push_str(&format!("{m}m"));
    }
    if s > 0 || out.is_empty() {
        out.push_str(&format!("{s}s"));
    }
    out
}

/// A Unix time in milliseconds as this machine's local time, to the millisecond, as daemon.log
/// lines start: `2026-11-04 03:12:05.123`. The Unix time itself if the C library can't convert it.
pub fn fmt_local_time(ms: Millis) -> String {
    let secs = (ms / 1000) as libc::time_t;
    // SAFETY: tm is plain data; localtime_r fills it from `secs` and touches nothing else.
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    if unsafe { libc::localtime_r(&secs, &mut tm) }.is_null() {
        return ms.to_string();
    }
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        ms % 1000
    )
}

/// Parses a duration with units: `600s`, `5m`, `2h`, `2h5m10s`. A bare number is refused, so
/// nobody has to guess the unit.
pub fn parse_duration_ms(s: &str) -> anyhow::Result<u64> {
    let t = s.trim();
    let bad = || anyhow::anyhow!("bad duration {s:?}: use units, for example 600s, 5m, 2h5m10s");
    if t.is_empty() {
        return Err(bad());
    }
    let mut total: u64 = 0;
    let mut num = String::new();
    let mut last_rank = 0;
    for c in t.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let (rank, mult) = match c {
            'h' => (1, HOUR_MS),
            'm' => (2, MINUTE_MS),
            's' => (3, SECOND_MS),
            _ => return Err(bad()),
        };
        if num.is_empty() || rank <= last_rank {
            return Err(bad());
        }
        let n: u64 = num.parse().map_err(|_| bad())?;
        total = total
            .checked_add(n.checked_mul(mult).ok_or_else(bad)?)
            .ok_or_else(bad)?;
        num.clear();
        last_rank = rank;
    }
    if !num.is_empty() {
        return Err(bad());
    }
    if total == 0 {
        anyhow::bail!("bad duration {s:?}: it must be longer than 0");
    }
    Ok(total)
}

/// Parses a RAM amount: `4` and `4G` are GiB, `512M` is MiB. Returns MiB.
pub fn parse_ram_mib(s: &str) -> anyhow::Result<u64> {
    let t = s.trim().to_ascii_lowercase();
    let t = t
        .strip_suffix("ib")
        .or_else(|| t.strip_suffix('b'))
        .unwrap_or(&t);
    let (num, mult) = if let Some(n) = t.strip_suffix('g') {
        (n, 1024.0)
    } else if let Some(n) = t.strip_suffix('m') {
        (n, 1.0)
    } else {
        (t, 1024.0)
    };
    let v: f64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("bad RAM amount {s:?} (examples: 4, 4G, 512M)"))?;
    if !(v.is_finite() && v > 0.0) {
        anyhow::bail!("bad RAM amount {s:?}: must be positive");
    }
    Ok((v * mult).ceil() as u64)
}

/// A job's priority after aging. Priority 5 stays 5. Anything lower rises by one for each time
/// the job was passed over, up to 4, so priority-5 jobs still run ahead of everything.
pub fn effective_priority(priority: u8, passes: u32) -> u8 {
    if priority >= 5 {
        5
    } else {
        (priority as u32).saturating_add(passes).min(4) as u8
    }
}

pub const DEFAULT_PRIORITY: u8 = 3;

/// A repository's id on the machine it lives on, `<name>-<8 hex>`, from its common git dir (`git
/// rev-parse --git-common-dir`). The name is the main worktree's directory: the common dir's
/// parent when the common dir is called `.git`, else the common dir's own name without a `.git`
/// suffix, made an [`id_name`]. The hex is the 32-bit FNV-1a hash of the common dir's canonical
/// absolute path. Every worktree of one repository gets the same id, and gitinfo and the client's
/// context compute it with this one function. The daemon salts it with its node name for the
/// job ([`owned_repo_id`]).
pub fn repo_id(common_dir: &Path) -> String {
    let path = std::fs::canonicalize(common_dir).unwrap_or_else(|_| common_dir.to_path_buf());
    let own = path.file_name().map(|n| n.to_string_lossy().into_owned());
    let name = match own.as_deref() {
        Some(".git") => path
            .parent()
            .and_then(Path::file_name)
            .map(|n| n.to_string_lossy().into_owned()),
        Some(n) => Some(n.strip_suffix(".git").unwrap_or(n).to_string()),
        None => None,
    };
    let name = id_name(&name.unwrap_or_default(), "repo");
    let mut hash: u32 = 0x811c_9dc5;
    for b in path.as_os_str().as_encoded_bytes() {
        hash ^= u32::from(*b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{name}-{hash:08x}")
}

/// The repository id a job and its mirrors, bases and workspaces use: the CLI's [`repo_id`]
/// salted with the owner's node name, by the owner's daemon at dispatch. `<name>-<16 hex>`: the
/// name of `local_id` (without its hex) and the 64-bit FNV-1a hash of `owner`, a NUL and
/// `local_id`. Two owners whose repositories have the same path get different ids, so a runner
/// never mixes their branches, commits and pins in one mirror. Submodule ids derive from it
/// (`gitinfo::submodule_repo_id`).
///
/// Ids from before salting (`<name>-<8 hex>`, [`is_unsalted_repo_id`]) stay in use for the
/// workspaces and jobs that have them.
pub fn owned_repo_id(owner: &str, local_id: &str) -> String {
    let name = match local_id.rsplit_once('-') {
        Some((name, hex)) if is_hex_of(hex, 8) || is_hex_of(hex, 16) => name,
        _ => local_id,
    };
    let name = if name.is_empty() { "repo" } else { name };
    let hash = fnv1a64(owner.bytes().chain([0]).chain(local_id.bytes()));
    format!("{name}-{hash:016x}")
}

/// Whether `id` has the form ids had before they were salted with the owner: `<name>-<8 hex>`
/// ([`repo_id`], and submodule ids derived from such an id). Salted ids end in 16 hex.
pub fn is_unsalted_repo_id(id: &str) -> bool {
    id.rsplit_once('-')
        .is_some_and(|(_, hex)| is_hex_of(hex, 8))
}

/// Whether `id` is a salted repository id ([`owned_repo_id`], and submodule ids derived from
/// one): it ends in `-` and 16 hex digits.
pub fn is_owned_repo_id(id: &str) -> bool {
    id.rsplit_once('-')
        .is_some_and(|(_, hex)| is_hex_of(hex, 16))
}

fn is_hex_of(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The 64-bit FNV-1a hash of `bytes`.
pub fn fnv1a64(bytes: impl IntoIterator<Item = u8>) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// `raw` as the name part of an id: characters other than ASCII letters, digits, `.`, `_` and `-`
/// become `-`, and so does a `.` at the start or right after another `.` (ids become directory
/// names and ref name components, where those are refused). Empty becomes `fallback`.
pub fn id_name(raw: &str, fallback: &str) -> String {
    let mut name = String::with_capacity(raw.len());
    for c in raw.chars() {
        let dot_ok = !name.is_empty() && !name.ends_with('.');
        let keep = c.is_ascii_alphanumeric() || matches!(c, '_' | '-') || (c == '.' && dot_ok);
        name.push(if keep { c } else { '-' });
    }
    if name.is_empty() {
        fallback.to_string()
    } else {
        name
    }
}

/// The resources a job asks for, all at once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    /// RAM in MiB: what the job reserves (the main admission filter) and its hard limit.
    pub ram_mib: u64,
    /// CPU weight in cores. A weight only: it never blocks admission.
    pub cpu: u32,
    /// GPU selectors, each taking one distinct GPU exclusively. See [`gpu_matches`].
    pub gpus: Vec<String>,
    /// Nothing else runs on the machine beside this job, and it holds every GPU there.
    pub exclusive: bool,
}

impl Default for Request {
    fn default() -> Self {
        Request {
            ram_mib: 4 * 1024,
            cpu: 1,
            gpus: Vec::new(),
            exclusive: false,
        }
    }
}

/// What a lease holds: the concrete resources granted for a request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub ram_mib: u64,
    pub cpu: u32,
    /// GPU ids (not selectors).
    pub gpus: Vec<String>,
    pub exclusive: bool,
}

/// A repository commit as a dispatch names it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoRef {
    /// Stable id of the repository. The CLI sends [`repo_id`] (`<name>-<8 hex>`, from its common
    /// git dir's path on the submitting machine); the owner's daemon salts it with its node name
    /// at dispatch ([`owned_repo_id`], `<name>-<16 hex>`) and derives the submodules' ids from
    /// that. Mirrors, bases and workspaces are keyed by it. A job dispatched into a workspace
    /// from before salting keeps that workspace's unsalted ids.
    pub id: String,
    /// The repository's common git dir on the submitting machine (the source of truth).
    pub source: PathBuf,
    /// Full commit sha.
    pub commit: String,
    /// The dispatching agent's branch, if HEAD is on one. Holds belong to it.
    pub branch: Option<String>,
    pub submodules: Vec<SubmoduleRef>,
    /// Other repositories' commits the dispatch carries along (`dispatch --carry`): synced into
    /// the mirrors and pinned like the submodules', never checked out. None in records from
    /// before `--carry`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub carried: Vec<CarriedRef>,
}

impl RepoRef {
    /// Every repository commit the dispatch names, as (repository id, source, commit): the
    /// repository's own, each submodule's, then each carried repository's. A machine runs the
    /// job only once its mirrors hold them all, and pins each of them for it.
    pub fn commits(&self) -> impl Iterator<Item = (&str, &Path, &str)> {
        let subs = self
            .submodules
            .iter()
            .map(|s| (s.repo_id.as_str(), s.source.as_path(), s.commit.as_str()));
        let carried = self
            .carried
            .iter()
            .map(|c| (c.repo_id.as_str(), c.source.as_path(), c.commit.as_str()));
        std::iter::once((
            self.id.as_str(),
            self.source.as_path(),
            self.commit.as_str(),
        ))
        .chain(subs)
        .chain(carried)
    }
}

/// A commit of another repository that a dispatch carries along (`dispatch --carry DIR[@REV]`),
/// for the job to fetch from the mirror that holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CarriedRef {
    /// The repository's directory name with every character but ASCII letters, digits and `_`
    /// made `_`: the job finds the mirror in `POTLUCK_MIRROR_<name>`.
    pub name: String,
    /// Its repository id: the CLI sends [`repo_id`] of its common git dir, and the owner's daemon
    /// salts it ([`owned_repo_id`]), as for a dispatch from that repository itself, so the commit
    /// goes into the mirror that repository's own jobs use.
    pub repo_id: String,
    /// Its common git dir on the submitting machine.
    pub source: PathBuf,
    /// Full commit sha.
    pub commit: String,
}

/// A submodule commit that a dispatched commit pins.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmoduleRef {
    /// Its path in the tree, e.g. `vendor/qemu`.
    pub path: String,
    /// Its own repository id (mirrors are per repository).
    pub repo_id: String,
    /// Its git dir on the submitting machine.
    pub source: PathBuf,
    pub commit: String,
}

/// What a caller dispatches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    /// Chosen by the caller. Dispatching a key that already exists returns that job (a no-op).
    pub key: Option<String>,
    pub labels: BTreeMap<String, String>,
    /// 1 (lowest) to 5 (highest).
    pub priority: u8,
    pub request: Request,
    /// Facts the machine must have (`--on KEY=VALUE`), see [`Facts::matches`].
    pub on: BTreeMap<String, String>,
    pub repo: RepoRef,
    /// Run inside this existing workspace, on its machine only.
    pub workspace: Option<WorkspaceId>,
    /// `--advance` (only with `workspace`): the workspace may be at an ancestor of the commit, and
    /// is checked out at it when the job starts, its untracked and ignored files kept. False in
    /// records from before it.
    #[serde(default)]
    pub advance: bool,
    /// argv, run in the workspace's root. Not run through a shell.
    pub cmd: Vec<String>,
    /// From the committed potluck.toml and `--env`, already merged by the client.
    pub env: BTreeMap<String, String>,
    /// Variables the job's environment goes without (`--unset-env`, a recipe's `unset_env`):
    /// removed from what the login environment, the `[[gpu]]` env and `env` set. None of
    /// potluck's own ([`unset_env_refused`]), and none that `env` sets. Empty in records from
    /// before it.
    #[serde(default)]
    pub unset_env: BTreeSet<String>,
    /// The run limit. Required.
    pub timeout_ms: u64,
    /// Requested hold after the invocation; at least [`MIN_HOLD_MS`], at most [`MAX_HOLD_MS`].
    pub hold_ms: Option<u64>,
    /// Globs relative to the workspace root, copied to the submitting machine at the end.
    pub keep: Vec<String>,
    /// Queue again elsewhere if the running machine is lost (not for pinned workspaces).
    pub rerun_if_lost: bool,
    /// End the job early when it stalls (otherwise it is only marked stalled).
    pub kill_on_stall: bool,
    pub recipe: Option<String>,
    /// The machine that dispatched it: the one that owns it. In a signed spec, this machine's
    /// name as its daemon told the command line, so the signature binds the owner.
    pub submitted_from: NodeName,
    /// The pools of the machines that may run it (`--pool`, a recipe's `pools`): a machine runs
    /// it only when its admission put it in one of them. Empty: the default pool
    /// ([`crate::mesh::trust::DEFAULT_POOL`]), which every machine is in in a cluster without
    /// users. Empty in records from before pools.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pools: Vec<String>,
    /// The user's signature over the spec as the command line sent it ([`crate::jobauth`]).
    /// None for an unsigned dispatch, and in records from before signed jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<crate::jobauth::JobAuth>,
}

impl JobSpec {
    /// Whether a machine in pool `pool` may run it: `pool` is one of its [`JobSpec::pools`], or
    /// the default pool when it names none.
    pub fn runs_in(&self, pool: &str) -> bool {
        runs_in(&self.pools, pool)
    }
}

/// Whether a job for `pools` ([`JobSpec::pools`]) may run on a machine in pool `pool`.
pub fn runs_in(pools: &[String], pool: &str) -> bool {
    match pools {
        [] => pool == crate::mesh::trust::DEFAULT_POOL,
        some => some.iter().any(|p| p == pool),
    }
}

/// [`JobSpec::pools`] in words: "pool default", "pools cloud, gpu".
pub fn fmt_pools(pools: &[String]) -> String {
    match pools {
        [] => format!("pool {}", crate::mesh::trust::DEFAULT_POOL),
        [one] => format!("pool {one}"),
        many => format!("pools {}", many.join(", ")),
    }
}

/// A job's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    /// Claimed; its workspace is being seeded or verified.
    Starting,
    Running,
    Succeeded,
    /// Non-zero exit, or a signal not sent by potluck; also killed past its process limit or by
    /// the OOM killer, a workspace that couldn't be seeded or verified, a start that failed, a
    /// pinned workspace that is gone, or processes that ended without recording an exit status
    /// while a daemon watched.
    Failed,
    Cancelled,
    /// Killed at its run limit, or stalled with `kill_on_stall`.
    TimedOut,
    OverMemory,
    /// Its machine was lost and it was not queued again, or every machine that could run it was
    /// lost; or its daemon lost track of its processes (gone while no daemon ran, with no record
    /// of how it ended, or not sampled 20 times in a row).
    Lost,
}

impl JobState {
    pub fn is_done(self) -> bool {
        !matches!(
            self,
            JobState::Queued | JobState::Starting | JobState::Running
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Starting => "starting",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
            JobState::TimedOut => "timed_out",
            JobState::OverMemory => "over_memory",
            JobState::Lost => "lost",
        }
    }
}

/// The state in words, as human output prints it: `timed out`, `over memory`. JSON and the store
/// use [`JobState::as_str`].
impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            JobState::TimedOut => "timed out",
            JobState::OverMemory => "over memory",
            other => other.as_str(),
        })
    }
}

/// A granted lease. The daemon holds it, never a file descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub grant: Grant,
    pub granted_at: Millis,
    /// When its run limit is reached: granted_at + timeout, then start + timeout once it starts
    /// (seeding its workspace doesn't count).
    pub deadline: Millis,
    /// Secret carried in the job's environment (POTLUCK_LEASE), so a potluck call from inside the
    /// job can prove it runs within this lease.
    pub token: String,
}

/// How the daemon finds a running invocation's processes again, including after a restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Handle {
    /// Linux: a transient systemd user unit.
    Systemd {
        unit: String,
        /// The unit's cgroup path relative to the cgroup root, once known.
        cgroup: Option<String>,
        /// The unit's InvocationID, systemd's id for this run of it: a later run under the same
        /// unit name (a job back on a machine it ran on) has another. None in older handles.
        #[serde(default)]
        invocation: Option<String>,
    },
    /// macOS: the supervisor's session and process group, and every process seen in it.
    Process {
        /// The supervisor: session and process-group leader.
        pid: u32,
        /// Its start time, to tell it from a reused pid.
        start_time: u64,
        /// (pid, start time) of every process seen in the job, including ones that left its
        /// session; followed from pid-tree snapshots.
        tracked: Vec<(u32, u64)>,
        /// (pid, start time, when a sample first saw it gone, in microseconds since the Unix
        /// epoch) of tracked processes that are gone: a crash report of a child launched after
        /// that is another process's, whoever has the pid now. Kept in the handle so a daemon
        /// restart doesn't lose it.
        #[serde(default)]
        gone: Vec<(u32, u64, u64)>,
        /// This run's `POTLUCK_LEASE`, which its processes carry (new for every run, unlike
        /// `POTLUCK_JOB`: a job can run again on a machine it ran on): a process of the user
        /// reparented to launchd that started after the supervisor and was started with it is
        /// adopted into the job. Empty: none is.
        #[serde(default)]
        lease: String,
        /// The most processes the job may have, the supervisor included; past it the monitor
        /// ends the job. 0: no limit.
        #[serde(default)]
        tasks_max: u32,
        /// `kern.bootsessionuuid` when the supervisor started: once no process has the
        /// supervisor's pid, its session is the job's only in the same boot. Empty: unknown
        /// (the boot time decides instead).
        #[serde(default)]
        boot: String,
    },
}

/// The latest resource sample of a running invocation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub cpu_ms: u64,
    pub mem_bytes: u64,
    pub peak_mem_bytes: u64,
    pub pids: Vec<u32>,
    /// When it last used at least 1% of one core (a stall is under that and no output).
    pub cpu_progress_at: Millis,
    /// When the log last grew.
    pub output_at: Millis,
    pub log_bytes: u64,
}

/// A job as potluck records it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: JobId,
    pub spec: JobSpec,
    pub state: JobState,
    pub submitted_at: Millis,
    pub started_at: Option<Millis>,
    pub ended_at: Option<Millis>,
    /// The machine running or that ran it.
    pub node: Option<NodeName>,
    pub workspace: Option<WorkspaceId>,
    pub lease: Option<Lease>,
    pub handle: Option<Handle>,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    /// Why it ended, in plain words: "killed at its run limit (15m)".
    pub message: Option<String>,
    /// Processes that would not die. The lease was released anyway.
    pub stuck_pids: Vec<u32>,
    pub usage: Usage,
    /// Times a job submitted after this one was granted something this one was waiting for
    /// (aging).
    pub passes: u32,
    pub stalled: bool,
    /// Files copied back to the submitting machine (paths relative to the job's directory there).
    pub kept: Vec<String>,
    /// Warnings about what the job's sources went through, each naming its machine: a mirror its
    /// dispatch or claim synced had diverged from its source, or was recloned (its log and `why`
    /// show them); and `--keep` matches that could not be copied when it ended (`why` shows them).
    #[serde(default)]
    pub notes: Vec<String>,
}

impl JobRecord {
    /// The exit code `potluck wait` exits with.
    pub fn exit_code_for_client(&self) -> i32 {
        match self.state {
            JobState::Succeeded => 0,
            JobState::TimedOut => EXIT_TIMEOUT,
            JobState::Cancelled => EXIT_CANCELLED,
            JobState::OverMemory => EXIT_OVER_MEMORY,
            JobState::Lost => EXIT_LOST,
            _ => match (self.exit_code, self.signal) {
                (Some(c), _) => c,
                (None, Some(s)) => 128i32.saturating_add(s),
                _ => 1,
            },
        }
    }
}

/// What the supervisor writes to status.json when the job's command exits. The daemon after a
/// restart in place or an update reads files an older binary's supervisor wrote, so a change must
/// stay readable across one version step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitStatus {
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub ended_at: Millis,
}

/// A workspace's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceState {
    Seeding,
    /// An invocation runs in it.
    Running,
    /// After an invocation, until `held_until`.
    Held,
    /// The hold is over; kept as an incremental donor until garbage collection.
    Idle,
    Removed,
}

impl WorkspaceState {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkspaceState::Seeding => "seeding",
            WorkspaceState::Running => "running",
            WorkspaceState::Held => "held",
            WorkspaceState::Idle => "idle",
            WorkspaceState::Removed => "removed",
        }
    }
}

/// One commit's tree on one machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRecord {
    pub id: WorkspaceId,
    pub node: NodeName,
    pub repo_id: String,
    pub commit: String,
    /// The branch its hold belongs to, and reminders go to: that of the job that created it,
    /// then of each job or `potluck hold` request that set when the hold ends.
    pub branch: Option<String>,
    pub path: PathBuf,
    pub state: WorkspaceState,
    pub created_at: Millis,
    pub last_used_at: Millis,
    pub held_until: Millis,
    /// Invocations queued or running in it; it is never removed while this is non-zero.
    pub pending: u32,
    /// Disk use, refreshed by the daemon now and then.
    pub size_bytes: Option<u64>,
    /// The job that last moved it to another commit (`dispatch --advance`), if one did: a job
    /// queued into it at its commit before then fails at start, saying so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advanced_by: Option<JobId>,
}

/// A held workspace, as every response reminds its owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reminder {
    pub workspace: WorkspaceId,
    pub node: NodeName,
    pub commit: String,
    pub state: WorkspaceState,
    /// Time left on the hold (only held workspaces get reminders).
    pub remaining_ms: u64,
    pub size_bytes: Option<u64>,
}

/// One GPU of a machine.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gpu {
    /// Linux: `vendor:device@pci-slot`, lowercase hex, e.g. `1002:744c@0000:03:00.0`.
    /// macOS: `mac0`, `mac1`, ...
    pub id: String,
    /// PCI vendor id in lowercase hex without 0x (`1002`, `10de`, `8086`), or `apple`.
    pub vendor: String,
    /// PCI device id in lowercase hex without 0x; empty on Apple silicon.
    pub device: String,
    /// e.g. "NVIDIA GeForce RTX 5080 Laptop GPU" or "Apple M3 Max".
    pub name: String,
    /// Kernel driver (amdgpu, i915, xe, nvidia), if known.
    pub driver: Option<String>,
    /// Short name from the node config.
    pub alias: Option<String>,
    /// Linux render node, e.g. `/dev/dri/renderD128`.
    pub render_node: Option<String>,
}

/// The PCI vendor id behind a vendor name a user may type.
pub fn vendor_id(name: &str) -> Option<&'static str> {
    match name {
        "nvidia" => Some("10de"),
        "amd" | "ati" => Some("1002"),
        "intel" => Some("8086"),
        "apple" => Some("apple"),
        _ => None,
    }
}

/// Whether GPU selector `sel` names `gpu`. A selector is one of: `any`; a GPU's id or alias; a
/// `vendor:device` pair; a vendor id; a vendor name (`nvidia`, `amd`, `intel`, `apple`). Hex is
/// matched case-insensitively and may carry `0x`.
pub fn gpu_matches(sel: &str, gpu: &Gpu) -> bool {
    let raw = sel.trim().to_ascii_lowercase();
    if raw == "any" || raw == gpu.id.to_ascii_lowercase() {
        return true;
    }
    if gpu.alias.as_deref().map(str::to_ascii_lowercase) == Some(raw.clone()) {
        return true;
    }
    let vendor = gpu.vendor.to_ascii_lowercase();
    if vendor_id(&raw) == Some(vendor.as_str()) {
        return true;
    }
    let sel = raw
        .split(':')
        .map(|p| p.trim_start_matches("0x").to_string())
        .collect::<Vec<_>>()
        .join(":");
    if !gpu.device.is_empty() && sel == format!("{vendor}:{}", gpu.device.to_ascii_lowercase()) {
        return true;
    }
    sel == vendor
}

/// What a machine reports about itself. Facts are not leased; jobs can require them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Facts {
    pub node: NodeName,
    /// `linux` or `macos`.
    pub os: String,
    /// Distribution and version, or the macOS version.
    pub os_version: String,
    /// `x86_64` or `aarch64`.
    pub arch: String,
    pub kernel: String,
    pub cpu_model: String,
    pub cpus: u32,
    pub ram_mib: u64,
    pub gpus: Vec<Gpu>,
    pub display: bool,
    /// potluck's version.
    pub version: String,
}

/// The parts of a version or name that `--on` compares: split on `.`, `-`, `_`, `+` and
/// whitespace, lowercased, empty parts dropped. `7.2.8-2-cachyos` has 7, 2, 8, 2, cachyos;
/// `Ubuntu 24.04` has ubuntu, 24, 04.
pub fn version_parts(s: &str) -> Vec<String> {
    s.split(|c: char| matches!(c, '.' | '-' | '_' | '+') || c.is_whitespace())
        .filter(|p| !p.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// Whether `want`'s parts equal the leading parts of `have` (see [`version_parts`]): `7.2`
/// matches `7.2.8-2-cachyos`, `27` matches `27.0.1` but not `270.1` or `26.4`, and `ubuntu-24`
/// matches `Ubuntu 24.04`. A `want` with no parts matches nothing.
pub fn version_matches(want: &str, have: &str) -> bool {
    let (want, have) = (version_parts(want), version_parts(have));
    !want.is_empty() && want.len() <= have.len() && want.iter().zip(&have).all(|(w, h)| w == h)
}

/// Whether `key` is an OS shorthand: `macos` (any macOS) or `macos=V` (V matched against the
/// macOS version, `os_version`), `linux` or `linux=V` (V matched against the kernel).
pub fn is_os_shorthand(key: &str) -> bool {
    matches!(key, "macos" | "linux")
}

/// A requirement as `--on` writes it: `macos` for any macOS (an OS shorthand with no version),
/// else `KEY=VALUE`.
pub fn fmt_on(key: &str, want: &str) -> String {
    if want.is_empty() && is_os_shorthand(key) {
        key.to_string()
    } else {
        format!("{key}={want}")
    }
}

/// `--on` requirements as one line: `macos=27, gpu=apple`, the OS first.
pub fn fmt_on_all(on: &BTreeMap<String, String>) -> String {
    on_in_order(on)
        .map(|(k, v)| fmt_on(k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether `have` and `want` name one CPU architecture: case ignored, `arm64` is `aarch64` (what
/// `uname -m` says on a Mac and on Linux) and `amd64` is `x86_64`.
pub fn same_arch(have: &str, want: &str) -> bool {
    let norm = |a: &str| match a.trim().to_ascii_lowercase().as_str() {
        "arm64" => "aarch64".to_string(),
        "amd64" => "x86_64".to_string(),
        other => other.to_string(),
    };
    norm(have) == norm(want)
}

/// The keys that name an OS. A machine runs one OS, so these must agree, and a dispatch's
/// `--on` for any of them replaces its recipe's for all of them.
pub const OS_KEYS: [&str; 3] = ["macos", "linux", "os"];

/// The requirements: those naming the OS first (`macos`, `linux`, `os`), then the rest by key.
fn on_in_order(on: &BTreeMap<String, String>) -> impl Iterator<Item = (&str, &str)> {
    let os = OS_KEYS.iter().filter_map(|k| on.get_key_value(*k));
    let rest = on.iter().filter(|(k, _)| !OS_KEYS.contains(&k.as_str()));
    os.chain(rest).map(|(k, v)| (k.as_str(), v.as_str()))
}

/// Why `name` can't be removed from a job's environment (`--unset-env`): it isn't a variable's
/// name (letters, digits and `_`, not starting with a digit), or it is one of potluck's own
/// variables ([`crate::daemon::is_job_variable`]), which every job gets.
pub fn unset_env_refused(name: &str) -> Option<String> {
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        return Some(format!(
            "{name:?} is not a variable name: use letters, digits and _, not starting with a digit"
        ));
    }
    crate::daemon::is_job_variable(name).then(|| {
        format!(
            "{name} is one of potluck's own variables, which every job gets ({} and {}<name>)",
            crate::daemon::JOB_VARIABLES.join(", "),
            crate::daemon::MIRROR_VARIABLE_PREFIX
        )
    })
}

/// Refuses `--on` requirements that no machine could meet together, or that give a version
/// with nothing in it to compare: `macos` with `linux` (or with `os=linux`), `macos=26` with
/// `os_version=27`, `linux=7.2` with `kernel=6`, `kernel=.`. The error names both requirements.
pub fn check_on(on: &BTreeMap<String, String>) -> Result<(), String> {
    for (k, v) in on {
        let version =
            matches!(k.as_str(), "os_version" | "kernel") || (is_os_shorthand(k) && !v.is_empty());
        if version && version_parts(v).is_empty() {
            return Err(format!(
                "{} gives no version: versions are compared by their parts, such as 27 or 7.2",
                fmt_on(k, v)
            ));
        }
    }
    let os: Vec<(String, String)> = on_in_order(on)
        .filter(|(k, _)| OS_KEYS.contains(k))
        .map(|(k, v)| {
            let os = if k == "os" { v } else { k };
            (fmt_on(k, v), os.to_ascii_lowercase())
        })
        .collect();
    if let Some((a, first)) = os.first()
        && let Some((b, _)) = os.iter().find(|(_, o)| o != first)
    {
        return Err(format!(
            "{a} and {b} contradict each other: a machine runs one OS"
        ));
    }
    for (short, fact) in [("macos", "os_version"), ("linux", "kernel")] {
        let (Some(s), Some(f)) = (on.get(short), on.get(fact)) else {
            continue;
        };
        let (sp, fp) = (version_parts(s), version_parts(f));
        if !s.is_empty() && !sp.iter().zip(&fp).all(|(a, b)| a == b) {
            return Err(format!(
                "{} and {} contradict each other: no {fact} starts with both",
                fmt_on(short, s),
                fmt_on(fact, f)
            ));
        }
    }
    Ok(())
}

impl Facts {
    /// Whether this machine has fact `key` = `want`. `gpu` matches when any GPU matches the
    /// selector, and `gpu_driver` when any GPU's kernel driver has that name (case ignored);
    /// `ram_gib` when the machine has at least that much (`32`, `15.5`); `os_version` and `kernel`
    /// when `want`'s parts are the leading parts of the machine's ([`version_matches`]);
    /// `macos` and `linux` when the machine runs that OS and, unless `want` is empty, its macOS
    /// version (`os_version`) or its kernel matches `want` that way; `os` and `arch` ignoring
    /// case, with `arm64` the same arch as `aarch64` and `amd64` as `x86_64` ([`same_arch`]).
    /// Others compare as text.
    pub fn matches(&self, key: &str, want: &str) -> bool {
        match key {
            "gpu" => self.gpus.iter().any(|g| gpu_matches(want, g)),
            "gpu_driver" => self.gpus.iter().any(|g| {
                g.driver
                    .as_deref()
                    .is_some_and(|d| d.eq_ignore_ascii_case(want.trim()))
            }),
            "ram_gib" => want
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|g| g.is_finite() && *g >= 0.0)
                .is_some_and(|g| self.ram_mib as f64 >= g * 1024.0),
            "node" => self.node == want,
            "os" => self.os.eq_ignore_ascii_case(want.trim()),
            "macos" | "linux" => {
                self.os == key
                    && (want.is_empty() || version_matches(want, self.shorthand_version()))
            }
            "os_version" => version_matches(want, &self.os_version),
            "arch" => same_arch(&self.arch, want),
            "kernel" => version_matches(want, &self.kernel),
            "cpu_model" => self.cpu_model == want,
            "cpus" => self.cpus.to_string() == want,
            "display" => self.display.to_string() == want,
            _ => false,
        }
    }

    /// The version `macos=` and `linux=` compare against on this machine: the macOS version
    /// on a Mac, the kernel on Linux.
    fn shorthand_version(&self) -> &str {
        match self.os.as_str() {
            "linux" => &self.kernel,
            _ => &self.os_version,
        }
    }

    /// This machine's OS as the shorthands name it: `macOS 27.0.1`, `linux 7.2.8-2-cachyos`.
    fn os_label(&self) -> String {
        let name = match self.os.as_str() {
            "macos" => "macOS",
            "" => "an unknown OS",
            other => other,
        };
        let version = self.shorthand_version();
        if version.is_empty() || self.os.is_empty() {
            name.to_string()
        } else {
            format!("{name} {version}")
        }
    }

    /// What this machine has for fact `key`, as a requirement it doesn't meet names it.
    fn describe(&self, key: &str) -> String {
        let text = |name: &str, v: &str| {
            if v.is_empty() {
                format!("no {name}")
            } else {
                format!("{name}={v}")
            }
        };
        match key {
            "os" | "macos" | "linux" => self.os_label(),
            "gpu" if self.gpus.is_empty() => "no GPU".into(),
            "gpu" => self
                .gpus
                .iter()
                .map(|g| if g.name.is_empty() { &g.id } else { &g.name }.clone())
                .collect::<Vec<_>>()
                .join(", "),
            "gpu_driver" => {
                let drivers: Vec<&str> = self
                    .gpus
                    .iter()
                    .filter_map(|g| g.driver.as_deref())
                    .collect();
                if !drivers.is_empty() {
                    format!("gpu_driver={}", drivers.join(", "))
                } else if self.gpus.is_empty() {
                    "no GPU".into()
                } else if self.os == "macos" {
                    // Its GPUs have no driver fact: nothing there can ever match.
                    "macOS, which names no GPU driver; gpu_driver matches on Linux only".into()
                } else {
                    let ids: Vec<&str> = self.gpus.iter().map(|g| g.id.as_str()).collect();
                    format!("no driver bound to {}", ids.join(", "))
                }
            }
            "ram_gib" => format!("{} RAM", crate::cli::fmt_mib(self.ram_mib)),
            "node" => text("node", &self.node),
            "os_version" => text("os_version", &self.os_version),
            "arch" => text("arch", &self.arch),
            "kernel" => text("kernel", &self.kernel),
            "cpu_model" => text("cpu_model", &self.cpu_model),
            "cpus" => format!("cpus={}", self.cpus),
            "display" => format!("display={}", self.display),
            other => format!("no fact {other}"),
        }
    }

    /// The first requirement this machine does not meet, in plain words with what the machine
    /// has: "needs macos=26 (this machine: macOS 27.0.1)". Those naming the OS come first.
    pub fn unmet(&self, on: &BTreeMap<String, String>) -> Option<String> {
        on_in_order(on)
            .find(|(k, v)| !self.matches(k, v))
            .map(|(k, v)| {
                format!(
                    "needs {} (this machine: {})",
                    fmt_on(k, v),
                    self.describe(k)
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Job records persist (the store, a restart's handover): one written before `unset_env`
    /// loads with nothing unset.
    #[test]
    fn a_spec_from_before_unset_env_loads() {
        let spec: JobSpec = serde_json::from_value(serde_json::json!({
            "key": null,
            "labels": {},
            "priority": 3,
            "request": { "ram_mib": 4096, "cpu": 1, "gpus": [], "exclusive": false },
            "on": {},
            "repo": {
                "id": "app-0011aabb",
                "source": "/src/app/.git",
                "commit": "0123456789abcdef0123456789abcdef01234567",
                "branch": "main",
                "submodules": []
            },
            "workspace": null,
            "cmd": ["true"],
            "env": { "A": "1" },
            "timeout_ms": 60000,
            "hold_ms": null,
            "keep": [],
            "rerun_if_lost": true,
            "kill_on_stall": false,
            "recipe": null,
            "submitted_from": "studio"
        }))
        .unwrap();
        assert!(spec.unset_env.is_empty());
        assert_eq!(spec.env["A"], "1");
        // Nor pools nor a signature: the default pool, unsigned.
        assert!(spec.pools.is_empty() && spec.auth.is_none());
        assert!(spec.runs_in("default") && !spec.runs_in("cloud"));
    }

    #[test]
    fn what_can_be_unset() {
        for ok in ["DEBUG_LAYER", "_x", "a1", "POTLUCK_OTHER", "TMP"] {
            assert_eq!(unset_env_refused(ok), None, "{ok}");
        }
        for bad in ["", "1A", "A=B", "A B", "A-B", "\u{e9}t\u{e9}"] {
            let why = unset_env_refused(bad).unwrap();
            assert!(
                why.ends_with(
                    "is not a variable name: use letters, digits and _, not starting with a digit"
                ),
                "{bad}: {why}"
            );
        }
        for own in crate::daemon::JOB_VARIABLES
            .into_iter()
            .chain(["POTLUCK_MIRROR_dep"])
        {
            let why = unset_env_refused(own).unwrap();
            assert!(
                why.starts_with(&format!(
                    "{own} is one of potluck's own variables, which every job gets (POTLUCK_JOB, "
                )),
                "{why}"
            );
        }
    }

    /// A count of passes from a peer or the store can be anything: no overflow (a panic in the
    /// scheduler's sort on every loop pass in a debug build, a wrong order in a release one).
    #[test]
    fn aging_doesnt_overflow() {
        assert_eq!(effective_priority(3, u32::MAX), 4);
        assert_eq!(effective_priority(1, 2), 3);
        assert_eq!(effective_priority(5, u32::MAX), 5);
    }

    /// Records written before `--advance` (0.1.0, in the store or a restart's handover) load: the
    /// spec doesn't advance and the workspace was moved by no job; and a workspace no job moved
    /// is written as before.
    #[test]
    fn records_from_before_advance_load() {
        let spec: JobSpec = serde_json::from_str(
            r#"{"key":null,"labels":{},"priority":3,
                "request":{"ram_mib":4096,"cpu":1,"gpus":[],"exclusive":false},"on":{},
                "repo":{"id":"app-0011aabb","source":"/src/app/.git",
                        "commit":"0123456789abcdef0123456789abcdef01234567","branch":null,
                        "submodules":[]},
                "workspace":"studio-w5","cmd":["true"],"env":{},"timeout_ms":60000,
                "hold_ms":null,"keep":[],"rerun_if_lost":false,"kill_on_stall":false,
                "recipe":null,"submitted_from":"studio"}"#,
        )
        .unwrap();
        assert!(!spec.advance);
        let old = r#"{"id":"studio-w5","node":"studio","repo_id":"app-0011aabb",
            "commit":"0123456789abcdef0123456789abcdef01234567","branch":null,"path":"/w",
            "state":"held","created_at":1,"last_used_at":1,"held_until":2,"pending":0,
            "size_bytes":null}"#;
        let ws: WorkspaceRecord = serde_json::from_str(old).unwrap();
        assert_eq!(ws.advanced_by, None);
        let again = serde_json::to_value(&ws).unwrap();
        assert!(again.get("advanced_by").is_none(), "{again}");
        let moved = WorkspaceRecord {
            advanced_by: Some("studio-8".into()),
            ..ws
        };
        let back: WorkspaceRecord =
            serde_json::from_str(&serde_json::to_string(&moved).unwrap()).unwrap();
        assert_eq!(back, moved);
    }

    #[test]
    fn durations_need_units() {
        assert_eq!(parse_duration_ms("600s").unwrap(), 600 * SECOND_MS);
        assert_eq!(parse_duration_ms("5m").unwrap(), 5 * MINUTE_MS);
        assert_eq!(
            parse_duration_ms("2h5m10s").unwrap(),
            2 * HOUR_MS + 5 * MINUTE_MS + 10 * SECOND_MS
        );
        assert_eq!(parse_duration_ms("1h30m").unwrap(), 90 * MINUTE_MS);
        for bad in ["600", "", "5x", "m", "5m2h", "0s", "1.5h", "5m5m", "-1s"] {
            assert!(parse_duration_ms(bad).is_err(), "{bad:?}");
        }
        assert_eq!(fmt_ms(45_000), "45s");
        assert_eq!(fmt_ms(14 * MINUTE_MS + 5_000), "14m5s");
        assert_eq!(fmt_ms(125 * MINUTE_MS), "2h5m");
    }

    /// A zero duration has units: the error says what is wrong with it.
    #[test]
    fn a_zero_duration_is_refused_for_being_zero() {
        for zero in ["0s", "0m", "0h0m0s"] {
            let e = parse_duration_ms(zero).unwrap_err().to_string();
            assert_eq!(
                e,
                format!("bad duration {zero:?}: it must be longer than 0")
            );
        }
    }

    /// Every duration potluck prints reads back as the same duration (to the second): a run limit
    /// of 90s is shown as 1m30s, not 1m, and 2h5m without a space the parser would refuse.
    #[test]
    fn printed_durations_parse_back() {
        assert_eq!(fmt_ms(0), "0s");
        assert_eq!(fmt_ms(999), "0s");
        assert_eq!(fmt_ms(90_000), "1m30s");
        assert_eq!(fmt_ms(HOUR_MS + 1_000), "1h1s");
        assert_eq!(fmt_ms(MAX_HOLD_MS), "24h");
        for ms in [
            1_000,
            59_000,
            60_000,
            90_000,
            3_599_000,
            HOUR_MS,
            2 * HOUR_MS + 5 * MINUTE_MS + 10_000,
            MAX_HOLD_MS,
            100 * HOUR_MS + 1_000,
        ] {
            assert_eq!(
                parse_duration_ms(&fmt_ms(ms)).unwrap(),
                ms,
                "{}",
                fmt_ms(ms)
            );
        }
    }

    #[test]
    fn local_times_read_as_dates() {
        // 2026-11-04 12:00:00.042 UTC: whatever the zone, the same day give or take one.
        let t = fmt_local_time(1_793_793_600_042);
        let b = t.as_bytes();
        assert_eq!(t.len(), "2026-11-04 03:12:05.123".len(), "{t}");
        for (i, c) in b.iter().enumerate() {
            match i {
                4 | 7 => assert_eq!(*c, b'-', "{t}"),
                10 => assert_eq!(*c, b' ', "{t}"),
                13 | 16 => assert_eq!(*c, b':', "{t}"),
                19 => assert_eq!(*c, b'.', "{t}"),
                _ => assert!(c.is_ascii_digit(), "{t}"),
            }
        }
        assert!(t.starts_with("2026-11-0") && t.ends_with(".042"), "{t}");
    }

    /// A record from before `--carry` loads with no carried repositories, and one without any
    /// is written as before (no `carried` field), so an older binary reads it too.
    #[test]
    fn repo_refs_from_before_carry_load_and_are_written_as_before() {
        let old = r#"{"id":"app-0123abcd","source":"/src/app/.git","commit":"c","branch":null,"submodules":[]}"#;
        let r: RepoRef = serde_json::from_str(old).unwrap();
        assert!(r.carried.is_empty());
        assert_eq!(serde_json::to_string(&r).unwrap(), old);
        let carrying = RepoRef {
            carried: vec![CarriedRef {
                name: "dep".into(),
                repo_id: "dep-89abcdef".into(),
                source: PathBuf::from("/src/dep/.git"),
                commit: "d".into(),
            }],
            ..r.clone()
        };
        let text = serde_json::to_string(&carrying).unwrap();
        assert_eq!(serde_json::from_str::<RepoRef>(&text).unwrap(), carrying);
        assert_eq!(
            carrying.commits().collect::<Vec<_>>(),
            [
                ("app-0123abcd", Path::new("/src/app/.git"), "c"),
                ("dep-89abcdef", Path::new("/src/dep/.git"), "d"),
            ]
        );
    }

    #[test]
    fn repo_ids() {
        let dir = tempfile::tempdir().unwrap();
        let main = dir.path().join("my repo");
        std::fs::create_dir_all(main.join(".git")).unwrap();
        let id = repo_id(&main.join(".git"));
        assert!(
            id.starts_with("my-repo-") && id.len() == "my-repo-".len() + 8,
            "{id}"
        );
        assert_eq!(id, repo_id(&main.join(".git")));
        let bare = dir.path().join("tools.git");
        std::fs::create_dir_all(&bare).unwrap();
        assert!(repo_id(&bare).starts_with("tools-"));
        assert_ne!(repo_id(&bare)[6..], id[8..]);
        // Salted with the owner: the same name, 16 hex, different for each owner.
        let a = owned_repo_id("studio", &id);
        assert!(
            a.starts_with("my-repo-") && a.len() == "my-repo-".len() + 16,
            "{a}"
        );
        assert_eq!(a, owned_repo_id("studio", &id));
        assert_ne!(a, owned_repo_id("msi", &id));
        assert_ne!(a, owned_repo_id("studio", &repo_id(&bare)));
        assert!(is_owned_repo_id(&a) && !is_unsalted_repo_id(&a));
        assert!(is_unsalted_repo_id(&id) && !is_owned_repo_id(&id));
        // The owner's name and the id don't run into each other.
        assert_ne!(
            owned_repo_id("ab", "c-0123abcd"),
            owned_repo_id("a", "bc-0123abcd")
        );
        // Odd ids keep a usable name.
        assert!(owned_repo_id("n", "app").starts_with("app-"));
        assert!(owned_repo_id("n", "-0123abcd").starts_with("repo-"));
        assert!(owned_repo_id("n", "x-y-0123abcd").starts_with("x-y-"));
        assert_eq!(id_name(".dotfiles", "repo"), "-dotfiles");
        assert_eq!(id_name("a..b...c", "repo"), "a.-b.-.c");
        assert_eq!(id_name("my repo.v2", "repo"), "my-repo.v2");
        assert_eq!(id_name("", "repo"), "repo");
    }

    #[test]
    fn ram() {
        assert_eq!(parse_ram_mib("4").unwrap(), 4096);
        assert_eq!(parse_ram_mib("4G").unwrap(), 4096);
        assert_eq!(parse_ram_mib("4GiB").unwrap(), 4096);
        assert_eq!(parse_ram_mib("512M").unwrap(), 512);
        assert!(parse_ram_mib("-1").is_err());
    }

    #[test]
    fn states_print_in_words_and_serialize_in_snake_case() {
        assert_eq!(JobState::TimedOut.to_string(), "timed out");
        assert_eq!(JobState::OverMemory.to_string(), "over memory");
        assert_eq!(JobState::Succeeded.to_string(), "succeeded");
        assert_eq!(JobState::TimedOut.as_str(), "timed_out");
        assert_eq!(
            serde_json::to_value(JobState::OverMemory).unwrap(),
            "over_memory"
        );
    }

    #[test]
    fn aging() {
        assert_eq!(effective_priority(3, 0), 3);
        assert_eq!(effective_priority(1, 2), 3);
        assert_eq!(effective_priority(3, 9), 4);
        assert_eq!(effective_priority(4, 1), 4);
        assert_eq!(effective_priority(5, 0), 5);
    }

    #[test]
    fn gpu_selectors() {
        let xtx = Gpu {
            id: "1002:744c@0000:03:00.0".into(),
            vendor: "1002".into(),
            device: "744c".into(),
            alias: Some("xtx".into()),
            ..Default::default()
        };
        for sel in [
            "any",
            "1002:744c@0000:03:00.0",
            "xtx",
            "1002:744c",
            "0x1002:0x744C",
            "1002",
            "amd",
        ] {
            assert!(gpu_matches(sel, &xtx), "{sel}");
        }
        for sel in ["10de", "nvidia", "1002:15bf", "mac0"] {
            assert!(!gpu_matches(sel, &xtx), "{sel}");
        }
        let mac = Gpu {
            id: "mac0".into(),
            vendor: "apple".into(),
            ..Default::default()
        };
        assert!(gpu_matches("mac0", &mac) && gpu_matches("apple", &mac));
    }

    #[test]
    fn facts() {
        let f = Facts {
            os: "linux".into(),
            ram_mib: 64 * 1024,
            gpus: vec![Gpu {
                id: "10de:2c19@0000:01:00.0".into(),
                vendor: "10de".into(),
                device: "2c19".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            f.matches("gpu", "nvidia") && f.matches("ram_gib", "32") && f.matches("os", "linux")
        );
        assert!(!f.matches("gpu", "amd") && !f.matches("ram_gib", "128"));
        // 2^54 + 1 GiB: times 1024 it wraps around to 1024 MiB in integers.
        assert!(!f.matches("ram_gib", "18014398509481985"));
        // Fractions count; nonsense matches nothing.
        assert!(f.matches("ram_gib", "63.5") && f.matches("ram_gib", "64"));
        for want in ["64.5", "-1", "NaN", "inf", "lots", ""] {
            assert!(!f.matches("ram_gib", want), "{want:?}");
        }
        // The GPU's kernel driver.
        assert!(!f.matches("gpu_driver", "nvidia"), "no driver known");
        let mut g = f.clone();
        g.gpus[0].driver = Some("nvidia".into());
        assert!(g.matches("gpu_driver", "nvidia") && g.matches("gpu_driver", "NVIDIA"));
        assert!(!g.matches("gpu_driver", "nouveau"));
        let mut on = BTreeMap::new();
        on.insert("gpu_driver".to_string(), "amdgpu".to_string());
        assert_eq!(
            g.unmet(&on).as_deref(),
            Some("needs gpu_driver=amdgpu (this machine: gpu_driver=nvidia)")
        );
        on.insert("gpu_driver".to_string(), "nvidia".to_string());
        assert_eq!(g.unmet(&on), None);
        let mut on = BTreeMap::new();
        on.insert("gpu".to_string(), "amd".to_string());
        assert_eq!(
            f.unmet(&on).as_deref(),
            Some("needs gpu=amd (this machine: 10de:2c19@0000:01:00.0)")
        );
    }

    fn mac() -> Facts {
        Facts {
            node: "studio".into(),
            os: "macos".into(),
            os_version: "27.0.1".into(),
            kernel: "27.0.0".into(),
            arch: "aarch64".into(),
            ..Default::default()
        }
    }

    fn msi() -> Facts {
        Facts {
            node: "msi".into(),
            os: "linux".into(),
            os_version: "CachyOS".into(),
            kernel: "7.2.8-2-cachyos".into(),
            arch: "x86_64".into(),
            ..Default::default()
        }
    }

    fn on(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn versions_match_by_leading_parts() {
        assert_eq!(
            version_parts("7.2.8-2-cachyos"),
            ["7", "2", "8", "2", "cachyos"]
        );
        assert_eq!(version_parts("Ubuntu 24.04"), ["ubuntu", "24", "04"]);
        assert_eq!(version_parts(" 6.12_rc1+x  "), ["6", "12", "rc1", "x"]);
        let kernel = "7.2.8-2-cachyos";
        for want in [
            "7",
            "7.2",
            "7.2.8",
            "7-2",
            "7.2.8-2-cachyos",
            "7.2.8-2-CachyOS",
        ] {
            assert!(version_matches(want, kernel), "{want}");
        }
        for want in [
            "7.20",
            "72",
            "7.2.9",
            "6",
            "7.2.8-2-cachyos-lts",
            "",
            ".",
            "-",
        ] {
            assert!(!version_matches(want, kernel), "{want:?}");
        }
        assert!(version_matches("27", "27.0.1") && version_matches("27.0", "27.0.1"));
        for have in ["270.1", "26.4", "2.7"] {
            assert!(!version_matches("27", have), "{have}");
        }
        for want in ["ubuntu", "Ubuntu", "ubuntu-24", "ubuntu 24.04", "UBUNTU_24"] {
            assert!(version_matches(want, "Ubuntu 24.04"), "{want}");
        }
        for want in ["ubuntu-25", "ubuntu-24.04.1", "ubu", "debian"] {
            assert!(!version_matches(want, "Ubuntu 24.04"), "{want}");
        }
    }

    #[test]
    fn version_facts_and_os_shorthands() {
        let (mac, msi) = (mac(), msi());
        // os_version and kernel compare by leading parts on both OSes.
        assert!(mac.matches("os_version", "27") && !mac.matches("os_version", "26"));
        assert!(msi.matches("kernel", "7.2") && !msi.matches("kernel", "7.20"));
        assert!(msi.matches("os_version", "cachyos") && !msi.matches("os_version", "ubuntu"));
        assert!(mac.matches("kernel", "27") && !mac.matches("kernel", "7"));
        // macos: any macOS, or its product version.
        for want in ["", "27", "27.0", "27.0.1"] {
            assert!(mac.matches("macos", want), "{want:?}");
            assert!(!msi.matches("macos", want), "{want:?}");
        }
        for want in ["26", "270", "27.1", "."] {
            assert!(!mac.matches("macos", want), "{want:?}");
        }
        // linux: any Linux, or its kernel (not the distribution).
        for want in ["", "7", "7.2", "7.2.8-2-cachyos"] {
            assert!(msi.matches("linux", want), "{want:?}");
            assert!(!mac.matches("linux", want), "{want:?}");
        }
        for want in ["6.12", "7.20", "cachyos", "27"] {
            assert!(!msi.matches("linux", want), "{want:?}");
        }
        // The other facts keep comparing as text.
        assert!(msi.matches("arch", "x86_64") && !msi.matches("arch", "x86"));
        assert!(msi.matches("os", "linux") && !msi.matches("os", "lin"));
        assert!(!msi.matches("node", "ms"));
    }

    #[test]
    fn unmet_says_what_the_machine_has() {
        let (mac, msi) = (mac(), msi());
        let cases = [
            (
                &mac,
                on(&[("macos", "26")]),
                "needs macos=26 (this machine: macOS 27.0.1)",
            ),
            (
                &mac,
                on(&[("linux", "")]),
                "needs linux (this machine: macOS 27.0.1)",
            ),
            (
                &msi,
                on(&[("linux", "6.12")]),
                "needs linux=6.12 (this machine: linux 7.2.8-2-cachyos)",
            ),
            (
                &msi,
                on(&[("macos", "")]),
                "needs macos (this machine: linux 7.2.8-2-cachyos)",
            ),
            (
                &msi,
                on(&[("os", "macos")]),
                "needs os=macos (this machine: linux 7.2.8-2-cachyos)",
            ),
            (
                &msi,
                on(&[("kernel", "6")]),
                "needs kernel=6 (this machine: kernel=7.2.8-2-cachyos)",
            ),
            (
                &msi,
                on(&[("os_version", "ubuntu-24")]),
                "needs os_version=ubuntu-24 (this machine: os_version=CachyOS)",
            ),
            (
                &msi,
                on(&[("gpu", "nvidia")]),
                "needs gpu=nvidia (this machine: no GPU)",
            ),
            (
                &msi,
                on(&[("ram_gib", "8")]),
                "needs ram_gib=8 (this machine: 0 MiB RAM)",
            ),
            (
                &msi,
                on(&[("display", "true")]),
                "needs display=true (this machine: display=false)",
            ),
            (
                &msi,
                on(&[("cpu_model", "x")]),
                "needs cpu_model=x (this machine: no cpu_model)",
            ),
            // The OS is named first, whatever the key order.
            (
                &mac,
                on(&[("gpu", "nvidia"), ("linux", "")]),
                "needs linux (this machine: macOS 27.0.1)",
            ),
        ];
        for (facts, on, want) in cases {
            assert_eq!(facts.unmet(&on).as_deref(), Some(want), "{on:?}");
        }
        // gpu_driver says why the machine has none to match: no GPU, a Mac (macOS names no
        // drivers), or a GPU with no driver bound.
        assert_eq!(
            msi.unmet(&on(&[("gpu_driver", "nvidia")])).as_deref(),
            Some("needs gpu_driver=nvidia (this machine: no GPU)")
        );
        let mut mac_gpu = mac.clone();
        mac_gpu.gpus.push(Gpu {
            id: "mac0".into(),
            vendor: "apple".into(),
            ..Default::default()
        });
        assert_eq!(
            mac_gpu.unmet(&on(&[("gpu_driver", "nvidia")])).as_deref(),
            Some(
                "needs gpu_driver=nvidia (this machine: macOS, which names no GPU driver; \
                 gpu_driver matches on Linux only)"
            )
        );
        let mut unbound = msi.clone();
        unbound.gpus.push(Gpu {
            id: "10de:2c19@0000:01:00.0".into(),
            vendor: "10de".into(),
            device: "2c19".into(),
            ..Default::default()
        });
        assert_eq!(
            unbound.unmet(&on(&[("gpu_driver", "nvidia")])).as_deref(),
            Some(
                "needs gpu_driver=nvidia (this machine: no driver bound to 10de:2c19@0000:01:00.0)"
            )
        );
        assert_eq!(
            mac.unmet(&on(&[("macos", "27"), ("arch", "aarch64")])),
            None
        );
        assert_eq!(
            msi.unmet(&on(&[("linux", "7.2"), ("os_version", "cachyos")])),
            None
        );
        assert_eq!(
            fmt_on_all(&on(&[
                ("gpu", "apple"),
                ("macos", "27"),
                ("arch", "aarch64")
            ])),
            "macos=27, arch=aarch64, gpu=apple"
        );
        assert_eq!(fmt_on("linux", ""), "linux");
        assert_eq!(fmt_on("kernel", ""), "kernel=");
    }

    /// `uname -m` says arm64 on a Mac and people write macOS: `--on arch=arm64` and `os=macOS`
    /// match the Mac (facts say aarch64 and macos), and `os=macOS` agrees with `macos`.
    #[test]
    fn os_and_arch_match_as_people_write_them() {
        let (mac, msi) = (mac(), msi());
        for want in ["arm64", "ARM64", "aarch64", "AArch64"] {
            assert!(mac.matches("arch", want), "{want}");
            assert!(!msi.matches("arch", want), "{want}");
        }
        for want in ["x86_64", "X86_64", "amd64"] {
            assert!(msi.matches("arch", want), "{want}");
        }
        assert!(!msi.matches("arch", "x86"));
        for want in ["macos", "macOS", "MACOS"] {
            assert!(mac.matches("os", want), "{want}");
            assert!(!msi.matches("os", want), "{want}");
        }
        assert!(msi.matches("os", "Linux"));
        assert_eq!(check_on(&on(&[("macos", "27"), ("os", "macOS")])), Ok(()));
        assert_eq!(check_on(&on(&[("linux", ""), ("os", "Linux")])), Ok(()));
        assert_eq!(
            check_on(&on(&[("macos", ""), ("os", "Linux")])),
            Err("macos and os=Linux contradict each other: a machine runs one OS".into())
        );
    }

    #[test]
    fn contradictions_are_refused() {
        let ok = [
            on(&[("macos", "")]),
            on(&[("macos", "27"), ("os", "macos")]),
            on(&[("macos", "27"), ("os_version", "27.0")]),
            on(&[("macos", "27.0"), ("os_version", "27")]),
            on(&[("macos", "27"), ("kernel", "27")]),
            on(&[("linux", "7.2"), ("kernel", "7.2.8")]),
            on(&[("linux", "7"), ("os_version", "ubuntu")]),
            on(&[("linux", ""), ("kernel", "6")]),
            on(&[("os", "plan9")]),
        ];
        for on in ok {
            assert_eq!(check_on(&on), Ok(()), "{on:?}");
        }
        let bad = [
            (
                on(&[("macos", ""), ("linux", "")]),
                "macos and linux contradict each other: a machine runs one OS",
            ),
            (
                on(&[("macos", "27"), ("os", "linux")]),
                "macos=27 and os=linux contradict each other: a machine runs one OS",
            ),
            (
                on(&[("linux", "7.2"), ("os", "macos")]),
                "linux=7.2 and os=macos contradict each other: a machine runs one OS",
            ),
            (
                on(&[("macos", "26"), ("os_version", "27")]),
                "macos=26 and os_version=27 contradict each other: no os_version starts with both",
            ),
            (
                on(&[("linux", "7.2"), ("kernel", "6")]),
                "linux=7.2 and kernel=6 contradict each other: no kernel starts with both",
            ),
            (
                on(&[("linux", "7.2"), ("kernel", "7.20")]),
                "linux=7.2 and kernel=7.20 contradict each other: no kernel starts with both",
            ),
            (
                on(&[("kernel", ".")]),
                "kernel=. gives no version: versions are compared by their parts, such as 27 or 7.2",
            ),
            (
                on(&[("os_version", "")]),
                "os_version= gives no version: versions are compared by their parts, such as 27 or 7.2",
            ),
            (
                on(&[("macos", "-")]),
                "macos=- gives no version: versions are compared by their parts, such as 27 or 7.2",
            ),
        ];
        for (on, want) in bad {
            assert_eq!(check_on(&on).unwrap_err(), want, "{on:?}");
        }
    }
}
