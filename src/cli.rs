//! The command line. Every command takes `--json` and none prompts. Human output goes to stdout;
//! reminders of held workspaces go to stderr on every command (and into `reminders` with `--json`).

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

/// A build and test cluster for a handful of Linux and macOS machines.
///
/// Every command takes --json and none prompts. A job keeps running when its client exits.
///
/// potluck's own errors exit with 2, `wait --max-wait` with 3 when the job hasn't ended, and
/// `restart` with 4 when no daemon here can restart in place. Otherwise `wait` exits with the
/// job's own exit code.
#[derive(Parser)]
#[command(name = "potluck", version)]
pub struct Cli {
    /// Print machine-readable JSON instead of text.
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

// Parsed once per process; boxing the dispatch arguments buys nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
pub enum Command {
    /// Queue a job at the current commit; print its id, and its workspace's once it has started.
    Dispatch(DispatchArgs),
    /// Stream a job's output until it ends, and exit with its exit code.
    Wait {
        /// The job's id, as dispatch printed it.
        job: String,
        /// Stop waiting after this long (the job keeps running); exits 3 if it hasn't ended.
        #[arg(long, value_name = "DUR")]
        max_wait: Option<String>,
        /// Don't stream the output, only wait.
        #[arg(long, short)]
        quiet: bool,
    },
    /// Print a job's output so far.
    ///
    /// With --json the output goes into the document's `output` as it arrives, so a log of any
    /// size takes little memory.
    Logs {
        /// The job's id.
        job: String,
        /// Keep printing what the job writes until it ends.
        #[arg(long, short)]
        follow: bool,
    },
    /// Cancel a job: drop it from the queue, or stop it (SIGTERM, then SIGKILL).
    Cancel {
        /// The job's id.
        job: String,
    },
    /// Lower a queued job's RAM request; it keeps its place in the queue.
    ///
    /// The machine the job was dispatched on changes it, only while the job is queued, and only
    /// to less than it asks for: the same amount changes nothing, and more is refused. In a
    /// cluster with users, the job with its new request is signed with this command line's user
    /// key.
    Resize {
        /// The job's id.
        job: String,
        /// The RAM it reserves and its hard limit from now on, as `dispatch --ram` takes it: 2,
        /// 2G, 512M.
        #[arg(long, value_name = "N")]
        ram: String,
    },
    /// Hold a workspace for DURATION from now (at most 24h).
    Hold {
        /// The workspace's id, such as studio-w5.
        workspace: String,
        /// With units: 30m, 2h.
        duration: String,
    },
    /// End a workspace's hold and remove it; while a job is queued or running in it, only the
    /// hold ends.
    Release {
        /// The workspace's id.
        workspace: String,
    },
    /// Copy a file or directory out of a workspace: potluck cp WORKSPACE:PATH DEST.
    Cp {
        /// WORKSPACE:PATH, the path relative to the workspace's root.
        from: String,
        /// Where to put it; into it when it is an existing directory.
        dest: PathBuf,
    },
    /// Machines, running jobs, the queue, recent jobs and workspaces.
    Status,
    /// What a queued job waits on and who holds that, or how a job runs or ended.
    Why {
        /// The job's id.
        job: String,
    },
    /// The machines and their facts.
    Nodes,
    /// The login environment a job on NODE starts from.
    ///
    /// NODE's daemon captured it when it started, and captures it again on `potluck restart`. A
    /// job gets the `[[gpu]]` env of the GPUs it holds, its dispatch's env and potluck's own
    /// variables on top, without what its dispatch unsets.
    Env {
        /// A machine's name, as `potluck nodes` lists it. Default: this machine.
        node: Option<String>,
    },
    /// Pair this machine with a cluster: prints a request to paste into `potluck admit` on a member.
    Pair {
        /// Finish pairing with the reply `potluck admit` printed.
        #[arg(long, value_name = "REPLY")]
        finish: Option<String>,
    },
    /// Admit a machine: takes the request `potluck pair` printed, prints the reply to paste back.
    ///
    /// In a cluster with users (`potluck user init`), the admission is signed with this command
    /// line's user key.
    Admit {
        /// The request `potluck pair` printed on the new machine.
        request: String,
        /// The machine's pool (a cluster with users only). Default: default.
        #[arg(long, value_name = "POOL")]
        pool: Option<String>,
    },
    /// Remove a machine from the cluster.
    ///
    /// In a cluster with users, the removal is signed with this command line's user key.
    Remove {
        /// The machine's name, as `potluck nodes` lists it.
        node: String,
    },
    /// The cluster's users: keys that sign who joins and leaves it, and who else is a user.
    User {
        #[command(subcommand)]
        action: UserAction,
    },
    /// Update this machine's daemon to the latest release; the other members update themselves
    /// when they meet its new version.
    SelfUpdate {
        /// Update from this process instead of asking the daemon (which need not be running):
        /// replace the potluck you ran, then have the daemon running here restart in place on it
        /// (a daemon from before `potluck restart` gets its service restarted).
        #[arg(long)]
        here: bool,
    },
    /// Restart the daemon in place: it starts the potluck binary at its path in its own process
    /// and hands over its sockets, open connections, running jobs and the cluster's view.
    ///
    /// No job starts until work already going (a workspace being seeded, a mirror sync) has
    /// finished, for at most a minute. Open connections, `wait` and `logs -f` go on. Exits 0 once
    /// the new binary answers, also when it can't take over (an older potluck): the daemon then
    /// restarts plainly on it, open connections close, and it says so. Exits 2 when the daemon
    /// doesn't restart, saying why: the binary at its path doesn't run or wouldn't start with
    /// node.toml (the daemon serves on), the state directory can't be written (the daemon tries
    /// again every 10 seconds), or the potluck you ran is another binary than the daemon's and
    /// that one hasn't changed (a restart would run it again as it is; both are named). Exits 4
    /// when no daemon here can restart in place (none answers, or it runs a potluck from before
    /// `restart`): restart its service instead.
    Restart,
    /// Run the daemon in the foreground.
    Daemon,
    /// Install or remove the daemon as a systemd user service or LaunchAgent.
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    #[command(name = "_supervise", hide = true)]
    Supervise {
        /// The command's environment, read from this file (see [`crate::supervise`]), which is
        /// removed once read. Without it the command inherits the supervisor's environment.
        #[arg(long)]
        env_file: Option<PathBuf>,
        /// Keep this many bytes of the command's output in the log, then drop the rest but its
        /// last 16 MiB (see [`crate::supervise`]). Without it, the log keeps everything.
        #[arg(long, value_name = "BYTES")]
        log_max: Option<u64>,
        status: PathBuf,
        #[arg(last = true, required = true)]
        cmd: Vec<String>,
    },
    #[command(name = "_tunnel", hide = true)]
    Tunnel { node: String, repo_id: String },
    /// Exits 0 if this binary can start from the daemon handover file FILE (a restart in
    /// place); 5 if it wouldn't start here at all (node.toml); otherwise says why and exits 2.
    #[command(name = "_handover-check", hide = true)]
    HandoverCheck { file: PathBuf },
}

#[derive(Subcommand)]
pub enum UserAction {
    /// Make this command line's user key (if there is none) the cluster's first user, and sign
    /// every member in with it. From then on only a user's signed admission or removal admits or
    /// removes a machine. On a machine in no cluster, founds one.
    Init {
        /// The user's name. Default: $USER.
        #[arg(long)]
        name: Option<String>,
    },
    /// Make KEY (another command line's `potluck user key`) a user, as NAME.
    Add {
        /// Letters, digits, '.', '_' and '-', not starting with '-'.
        name: String,
        /// The user's public key. One in 64 starts with '-', and is still taken as the key.
        #[arg(allow_hyphen_values = true)]
        key: String,
    },
    /// Remove user NAME: its key signs nothing that counts after. The last user stays.
    Remove {
        /// The user's name, as `potluck user list` lists it.
        name: String,
    },
    /// The users.
    List,
    /// Print this command line's public user key, making the key first if there is none.
    ///
    /// The key file is $POTLUCK_USER_KEY, else user.key beside node.toml (mode 0600).
    Key,
}

#[derive(Subcommand)]
pub enum ServiceAction {
    /// Write the service for this executable and start it (Linux: lingering must be on).
    Install,
    /// Stop the service and remove its file.
    Uninstall,
}

#[derive(Args)]
pub struct DispatchArgs {
    /// A recipe from the repository's potluck.toml. Options below override it.
    pub recipe: Option<String>,
    /// The run limit, with units: 600s, 5m, 2h5m10s. Required (here or in the recipe).
    #[arg(long, value_name = "DUR")]
    pub timeout: Option<String>,
    /// 1 (lowest) to 5 (highest). Default 3.
    #[arg(long, value_name = "1-5")]
    pub priority: Option<u8>,
    /// RAM the job reserves and its hard limit, in GiB without a unit: 4, 4G, 512M. Default 4. A
    /// machine starts jobs only while their reservations fit its ram_gib; a job that uses more
    /// than its limit is ended.
    #[arg(long)]
    pub ram: Option<String>,
    /// CPU weight in cores, default 1; it never keeps a job from starting. On Linux it is the
    /// job's CPUWeight, 100 per core; on macOS it has no effect.
    #[arg(long, value_name = "N")]
    pub cpu: Option<u32>,
    /// A GPU, held exclusively: any, nvidia, amd, intel, apple, an id or alias, vendor:device.
    /// Repeatable, one GPU each.
    #[arg(long = "gpu", value_name = "SEL")]
    pub gpus: Vec<String>,
    /// Nothing else runs on the machine beside this job, and it holds every GPU there.
    #[arg(long)]
    pub exclusive: bool,
    /// A fact the machine must have, KEY=VALUE: gpu=nvidia, gpu_driver=amdgpu (Linux only),
    /// ram_gib=32, kernel=6.12. macos and linux stand alone for any version, or take one:
    /// macos=27 (macOS 27.x), linux=7.2 (kernel 7.2.x). Keys: os, os_version, kernel, arch, gpu,
    /// gpu_driver, ram_gib, cpus, cpu_model, node, display (`potluck nodes` shows each
    /// machine's). os_version and kernel match by leading parts. os and arch ignore case, and
    /// arch=arm64 is aarch64, arch=amd64 is x86_64. Repeatable; all must hold.
    #[arg(long = "on", value_name = "KEY[=VALUE]")]
    pub on: Vec<String>,
    /// An environment variable for the job, K=V. Repeatable.
    #[arg(long = "env", value_name = "K=V")]
    pub env: Vec<String>,
    /// Leave variable K out of the job's environment, wherever it comes from: the machine's
    /// login environment, its `[[gpu]]` env, or the committed potluck.toml. Repeatable.
    /// potluck's own variables (POTLUCK_JOB, TMPDIR, ...) can't be left out.
    #[arg(long = "unset-env", value_name = "K")]
    pub unset_env: Vec<String>,
    /// Run only on machines in pool POOL (`potluck admit --pool`). Repeatable: any of them.
    /// Default: the default pool. Replaces the recipe's `pools`.
    #[arg(long = "pool", value_name = "POOL")]
    pub pools: Vec<String>,
    /// Run inside this existing workspace (on its machine).
    #[arg(long)]
    pub workspace: Option<String>,
    /// With --workspace: the workspace may be at an older commit that this one descends from.
    /// When the job starts, the workspace is checked out at this commit, and its untracked and
    /// ignored files (build output) are kept.
    #[arg(long, requires = "workspace")]
    pub advance: bool,
    /// Hold the workspace this long after the job (at least 5m, at most 24h).
    #[arg(long, value_name = "DUR")]
    pub hold: Option<String>,
    /// Files to copy back here when the job ends, as globs relative to the workspace. Repeatable.
    #[arg(long = "keep", value_name = "GLOB")]
    pub keep: Vec<String>,
    /// Leave out the submodules matching PATTERN, a path from the top or a glob
    /// (`vendor/qemu/roms/*`), and everything nested in them: not checked here, not in the job's
    /// workspace. Repeatable; replaces the recipe's `without_submodules`.
    #[arg(long = "without-submodule", value_name = "PATTERN")]
    pub without_submodule: Vec<String>,
    /// Carry a commit of another repository here into the mirrors of the machine that runs the
    /// job, for the job to fetch: DIR is a directory in that repository, REV a commit there
    /// (default HEAD). The job finds the mirror in POTLUCK_MIRROR_<name>, <name> being the
    /// repository's directory name with characters other than letters, digits and _ made _.
    /// Repeatable.
    #[arg(long = "carry", value_name = "DIR[@REV]")]
    pub carry: Vec<String>,
    /// Makes the dispatch idempotent: a key that exists returns that job.
    #[arg(long)]
    pub key: Option<String>,
    /// A label, K=V. Repeatable.
    #[arg(long = "label", value_name = "K=V")]
    pub labels: Vec<String>,
    /// Don't queue it again elsewhere if its machine is lost.
    #[arg(long)]
    pub no_rerun: bool,
    /// End the job if it stalls (under 1% of a CPU core and no output for the machine's stall
    /// time).
    #[arg(long)]
    pub kill_on_stall: bool,
    /// The command, after --.
    #[arg(last = true)]
    pub cmd: Vec<String>,
}

mod copy;
mod nested;
mod output;

pub use nested::fits;
pub use output::{JsonText, Report, fmt_bytes, fmt_mib, table};

use crate::client::Client;
use crate::config::Paths;
use crate::mesh::user::UserKey;
use crate::proto::{Request as Req, Response};
use crate::types::{JobRecord, now_ms, parse_duration_ms};
use serde_json::{Value, json};
use std::ffi::OsString;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

/// `potluck wait --max-wait` exits with this when the job hasn't ended.
pub const EXIT_STILL_RUNNING: i32 = 3;
/// Usage errors and potluck's own errors.
pub const EXIT_ERROR: i32 = 2;
/// `potluck restart` exits with this when no daemon here can restart in place: none answers on
/// the socket, or the one that does runs a potluck from before `restart` (install.sh then
/// restarts the service instead).
pub const EXIT_NO_RESTART_HERE: i32 = 4;

/// Bytes asked for per `Logs` request.
const LOG_CHUNK: u64 = 1 << 20;
/// The longest a long poll asks the daemon to wait (well inside the client's read timeout).
const POLL_MS: u64 = 20_000;
/// How long `wait` and `logs` keep trying to reach a daemon whose connection failed (a restart
/// after an update takes a few seconds).
const RECONNECT: Duration = Duration::from_secs(60);
/// How long `potluck restart` waits for its answer: the check of the new binary, the drain and
/// the handover ([`crate::daemon::handover`]).
const RESTART_WAIT: Duration = Duration::from_secs(180);
/// How long `dispatch` and `release` wait for their answer. The daemon answers a dispatch once
/// its mirrors of the repository are up to date (a first clone, or a wait for a workspace being
/// seeded from the mirror), and a release once the workspace is deleted: minutes for a large one.
const SLOW_ANSWER: Duration = Duration::from_secs(30 * 60);

/// What a command needs from its process: where state lives, the working directory, the
/// variables a job sets, and how to read a repository (a parameter so tests can fake it).
pub struct Env {
    pub paths: Paths,
    pub cwd: PathBuf,
    /// POTLUCK_JOB, POTLUCK_LEASE and POTLUCK_WORKSPACE: set when running inside a job.
    pub job: Option<String>,
    pub lease: Option<String>,
    pub workspace: Option<String>,
    /// [`crate::gitinfo::inspect_without`], or a stand-in in tests.
    pub inspect: Inspect,
}

/// Reads the caller's repository for a dispatch: the second argument gets the commit and returns
/// the patterns of submodules to leave out.
pub type Inspect = fn(
    &Path,
    &mut dyn FnMut(&str) -> anyhow::Result<Vec<String>>,
) -> anyhow::Result<crate::gitinfo::Inspected>;

impl Env {
    pub fn from_process() -> anyhow::Result<Env> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        Ok(Env {
            paths: Paths::from_env()?,
            cwd: std::env::current_dir().map_err(|e| {
                anyhow::anyhow!("can't read the current directory (was it removed?): {e}")
            })?,
            job: var("POTLUCK_JOB"),
            lease: var("POTLUCK_LEASE"),
            workspace: var("POTLUCK_WORKSPACE"),
            inspect: crate::gitinfo::inspect_without,
        })
    }
}

/// Runs the command line and returns the process exit code.
pub fn main() -> i32 {
    let args: Vec<OsString> = std::env::args_os().collect();
    let cli = match Cli::try_parse_from(&args) {
        Ok(cli) => cli,
        Err(e) => return usage_error(&e, wants_json(&args)),
    };
    let json = cli.json;
    let direct: Option<anyhow::Result<i32>> = match &cli.command {
        Command::Supervise {
            status,
            env_file,
            log_max,
            cmd,
        } => Some(crate::supervise::run(
            status,
            env_file.as_deref(),
            *log_max,
            cmd,
        )),
        Command::Daemon => Some(Paths::from_env().and_then(|paths| {
            let config = crate::config::NodeConfig::load(&paths)?;
            crate::daemon::run(&paths, &config).map(|()| 0)
        })),
        // git's ext:: transport runs it: git's protocol on stdout, errors on stderr only.
        Command::Tunnel { node, repo_id } => Some(Ok(crate::tunnel::run_tunnel(node, repo_id))),
        Command::HandoverCheck { file } => match crate::daemon::handover::check(file) {
            Ok(()) => Some(Ok(0)),
            Err(crate::daemon::handover::Refused::File(e)) => Some(Err(e)),
            Err(crate::daemon::handover::Refused::WontStart(e)) => {
                print_error(
                    &mut std::io::stdout().lock(),
                    &mut std::io::stderr().lock(),
                    false,
                    &e,
                    &[],
                );
                return crate::daemon::handover::EXIT_WONT_START;
            }
        },
        _ => None,
    };
    let stdout = std::io::stdout();
    let stderr = std::io::stderr();
    let (mut out, mut err) = (stdout.lock(), stderr.lock());
    match direct {
        Some(Ok(code)) => code,
        Some(Err(e)) => {
            print_error(&mut out, &mut err, json, &e, &[]);
            EXIT_ERROR
        }
        None => match Env::from_process() {
            Ok(env) => run(cli, &env, &mut out, &mut err),
            Err(e) => {
                print_error(&mut out, &mut err, json, &e, &[]);
                EXIT_ERROR
            }
        },
    }
}

/// Parses `args` (the first is the program name) and runs the command against `env`. For tests
/// and embedding; [`main`] is the process entry point.
pub fn run_args<I, T>(args: I, env: &Env, out: &mut dyn Write, err: &mut dyn Write) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    match Cli::try_parse_from(&args) {
        Ok(cli) => run(cli, env, out, err),
        Err(e) => {
            let json = wants_json(&args);
            if json {
                let _ = writeln!(out, "{}", error_json(&clap_message(&e), &[]));
            } else {
                let _ = write!(err, "{}", e.render());
            }
            if e.use_stderr() { EXIT_ERROR } else { 0 }
        }
    }
}

/// Whether `--json` appears among the options (before a `--`), for errors clap reports before
/// anything is parsed.
fn wants_json(args: &[OsString]) -> bool {
    args.iter()
        .skip(1)
        .take_while(|a| a.as_os_str() != "--")
        .any(|a| a.as_os_str() == "--json")
}

fn clap_message(e: &clap::Error) -> String {
    e.render().to_string().trim().to_string()
}

fn usage_error(e: &clap::Error, json: bool) -> i32 {
    if !e.use_stderr() {
        // --help and --version: the output asked for. One that can't be written is an error, as
        // any command's output is; a pipe nobody reads any more (`| head`) is not.
        return match e.print().and_then(|()| std::io::stdout().flush()) {
            Err(w) if w.kind() != std::io::ErrorKind::BrokenPipe => {
                let _ = writeln!(std::io::stderr(), "potluck: can't write the output: {w}");
                EXIT_ERROR
            }
            _ => 0,
        };
    }
    let mut out = std::io::stdout().lock();
    let doc = error_json(&clap_message(e), &[]);
    // The JSON document goes to stdout; when stdout can't take it (nobody reads it, a full
    // disk), the error is said on stderr rather than lost.
    if !json || writeln!(out, "{doc}").and_then(|()| out.flush()).is_err() {
        let _ = e.print();
    }
    EXIT_ERROR
}

fn error_json(message: &str, reminders: &[crate::types::Reminder]) -> Value {
    json!({ "error": message, "reminders": output::reminders_json(reminders) })
}

fn print_error(
    out: &mut dyn Write,
    err: &mut dyn Write,
    json: bool,
    e: &anyhow::Error,
    reminders: &[crate::types::Reminder],
) {
    // With --json the error document goes to stdout; when stdout can't take it (a full disk,
    // nobody reads it), the error is said on stderr rather than lost.
    let doc = || error_json(&format!("{e:#}"), reminders);
    if !json
        || writeln!(out, "{}", doc())
            .and_then(|()| out.flush())
            .is_err()
    {
        let _ = writeln!(err, "potluck: {e:#}");
        let _ = write!(err, "{}", output::reminders_human(reminders));
    }
}

/// A report's text on stdout, ending with a line break, flushed before anything goes to stderr.
fn write_human(out: &mut dyn Write, human: &str) -> std::io::Result<()> {
    if !human.is_empty() {
        write!(out, "{human}")?;
        if !human.ends_with('\n') {
            writeln!(out)?;
        }
    }
    out.flush()
}

/// Runs one parsed command: its output, then the caller's reminders, and returns the exit code.
pub fn run(cli: Cli, env: &Env, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let json = cli.json;
    let mut cx = Cx {
        env,
        json,
        out,
        err,
        client: None,
    };
    let result = cx.command(cli.command);
    let reminders = cx.reminders();
    let Cx { out, err, .. } = cx;
    match result {
        Ok(report) => {
            let written = if json && report.streamed {
                // Already written, reminders included.
                Ok(())
            } else if json {
                let mut doc = report.json;
                if let Value::Object(map) = &mut doc {
                    map.insert("reminders".into(), output::reminders_json(&reminders));
                }
                writeln!(out, "{doc}")
            } else {
                let written = write_human(out, &report.human);
                if !report.note.is_empty() {
                    let _ = writeln!(err, "potluck: {}", report.note.trim_end());
                }
                let _ = write!(err, "{}", output::reminders_human(&reminders));
                written
            };
            match written.and_then(|()| out.flush()) {
                // Nobody reads it any more (`| head`): not an error.
                Err(e) if e.kind() != std::io::ErrorKind::BrokenPipe => {
                    let _ = writeln!(err, "potluck: can't write the output: {e}");
                    EXIT_ERROR
                }
                _ => report.code,
            }
        }
        Err(e) => {
            print_error(out, err, json, &e, &reminders);
            let _ = out.flush();
            EXIT_ERROR
        }
    }
}

/// One command's run: the environment, the output streams and the daemon connection.
struct Cx<'a> {
    env: &'a Env,
    json: bool,
    out: &'a mut dyn Write,
    err: &'a mut dyn Write,
    client: Option<Client>,
}

impl Cx<'_> {
    fn connect(&mut self) -> anyhow::Result<&mut Client> {
        if self.client.is_none() {
            let ctx =
                crate::client::context(&self.env.cwd, self.env.job.clone(), self.env.lease.clone());
            self.client = Some(Client::connect(&self.env.paths, ctx)?);
        }
        Ok(self.client.as_mut().expect("connected above"))
    }

    fn call(&mut self, req: Req) -> anyhow::Result<Response> {
        self.connect()?.call(req)
    }

    /// [`Cx::call`], waiting up to [`SLOW_ANSWER`] for the answer.
    fn call_slow(&mut self, req: Req) -> anyhow::Result<Response> {
        let c = self.connect()?;
        c.set_read_timeout(SLOW_ANSWER)?;
        let answer = c.call(req);
        let _ = c.set_read_timeout(crate::client::READ_TIMEOUT);
        answer
    }

    /// The reminders the daemon sent with its latest answer. A command that never talked to the
    /// daemon asks for them with a ping, if a daemon is there to answer.
    fn reminders(&mut self) -> Vec<crate::types::Reminder> {
        if let Some(c) = &self.client {
            return c.reminders.clone();
        }
        let ctx =
            crate::client::context(&self.env.cwd, self.env.job.clone(), self.env.lease.clone());
        let Ok(mut c) = Client::connect(&self.env.paths, ctx) else {
            return Vec::new();
        };
        if c.set_read_timeout(Duration::from_secs(2)).is_err() {
            return Vec::new();
        }
        match c.call(Req::Ping) {
            Ok(_) => c.reminders,
            Err(_) => Vec::new(),
        }
    }

    /// The version the local daemon runs, if one answers.
    fn daemon_version(&mut self) -> Option<String> {
        let ctx =
            crate::client::context(&self.env.cwd, self.env.job.clone(), self.env.lease.clone());
        let mut c = Client::connect(&self.env.paths, ctx).ok()?;
        c.set_read_timeout(Duration::from_secs(2)).ok()?;
        match c.call(Req::Ping).ok()? {
            Response::Pong { version, .. } => Some(version),
            _ => None,
        }
    }

    fn command(&mut self, command: Command) -> anyhow::Result<Report> {
        match command {
            Command::Dispatch(args) => self.dispatch(args),
            Command::Wait {
                job,
                max_wait,
                quiet,
            } => self.wait(&job, max_wait.as_deref(), quiet),
            Command::Logs { job, follow } => self.logs(&job, follow),
            Command::Cancel { job } => self.cancel(&job),
            Command::Resize { job, ram } => self.resize(&job, &ram),
            Command::Hold {
                workspace,
                duration,
            } => self.hold(&workspace, &duration),
            Command::Release { workspace } => self.release(&workspace),
            Command::Cp { from, dest } => self.cp(&from, &dest),
            Command::Status => self.status(),
            Command::Why { job } => self.why(&job),
            Command::Nodes => self.nodes(),
            Command::Env { node } => self.env(node),
            // Pairing hands a line from one machine to the other: stdout is that line alone, and
            // stderr says where it goes next.
            Command::Pair { finish } => match finish {
                None => self.text_request(Req::PairRequest, "pair").map(|r| Report {
                    note: "paste this line into `potluck admit` on a member of the cluster (a \
                           machine in no cluster founds one with it), then the reply into \
                           `potluck pair --finish` here"
                        .into(),
                    ..r
                }),
                Some(reply) => self.text_request(Req::PairFinish { reply }, "pair --finish"),
            },
            Command::Admit { request, pool } => self.admit(request, pool),
            Command::User { action } => self.user(action),
            Command::Remove { node } => {
                let removal = self.signed_removal(&node)?;
                // Text: the removal holds until the machine removed contests it, and why.
                let warning = match self.call(Req::Remove {
                    node: node.clone(),
                    removal,
                })? {
                    Response::Ok => None,
                    Response::Text { text } => Some(text),
                    other => return Err(unexpected("remove", &other)),
                };
                Ok(Report {
                    note: warning.clone().unwrap_or_default(),
                    ..Report::new(
                        json!({ "removed": node, "final": warning.is_none(), "warning": warning }),
                        format!("removed {node} from the cluster"),
                    )
                })
            }
            Command::SelfUpdate { here } => {
                if here {
                    let daemon = self.daemon_version();
                    let text = crate::update::self_update_here(daemon.as_deref())?;
                    Ok(Report::new(json!({ "text": text }), text))
                } else {
                    self.text_request(Req::SelfUpdate, "self-update")
                        .map_err(|e| match e.to_string() {
                            m if m.starts_with("the potluck daemon is not running") => anyhow::anyhow!(
                                "{m}; or update this machine without it: `potluck self-update --here`"
                            ),
                            _ => e,
                        })
                }
            }
            Command::Service { action } => self.service(action),
            Command::Restart => self.restart(),
            Command::Daemon
            | Command::Supervise { .. }
            | Command::Tunnel { .. }
            | Command::HandoverCheck { .. } => {
                anyhow::bail!("this command runs on its own, not through the client")
            }
        }
    }

    fn dispatch(&mut self, args: DispatchArgs) -> anyhow::Result<Report> {
        if let (Some(job), Some(lease)) = (self.env.job.clone(), self.env.lease.clone()) {
            return self.dispatch_nested(&args, &job, &lease);
        }
        // The recipe comes from the commit, and says which submodules to leave out before they
        // are read.
        let (inspect, cwd) = (self.env.inspect, self.env.cwd.clone());
        let mut resolved = None;
        let inspected = inspect(&cwd, &mut |commit| {
            let file = match crate::recipe::load_at_commit(&cwd, commit)? {
                Some(f) => f,
                None if args.recipe.is_some() => anyhow::bail!(
                    "commit {} has no {}: recipes come from the committed file",
                    crate::gitinfo::short(commit),
                    crate::recipe::FILE_NAME
                ),
                None => Default::default(),
            };
            let r = crate::recipe::resolve(&file, &args)?;
            let without = r.without_submodules.clone();
            resolved = Some(r);
            Ok(without)
        })?;
        let r = resolved.ok_or_else(|| anyhow::anyhow!("the dispatch's recipe wasn't read"))?;
        let (mut repo, left_out) = (inspected.repo, inspected.left_out);
        repo.carried = crate::gitinfo::carry_all(&args.carry, &cwd, &repo)?;
        let spec = crate::types::JobSpec {
            key: args.key.clone(),
            labels: r.labels,
            priority: r.priority,
            request: r.request,
            on: r.on,
            repo,
            workspace: args.workspace.clone(),
            advance: args.advance,
            cmd: r.cmd,
            env: r.env,
            unset_env: r.unset_env,
            timeout_ms: r.timeout_ms,
            hold_ms: r.hold_ms,
            keep: r.keep,
            rerun_if_lost: !args.no_rerun && args.workspace.is_none(),
            kill_on_stall: args.kill_on_stall,
            recipe: r.recipe,
            // The daemon names it: it numbers the job and owns it, with the node.toml it read
            // when it started (this process may read another, or one edited since). A signed
            // dispatch names it as the daemon says, so the signature binds the owner.
            submitted_from: String::new(),
            pools: r.pools,
            auth: None,
        };
        let spec = self.signed(spec)?;
        let (job, existing) = match self.call_slow(Req::Dispatch {
            spec: Box::new(spec.clone()),
        })? {
            Response::Dispatched { job, existing } => (job, existing),
            other => return Err(unexpected("dispatch", &other)),
        };
        // The record says where it runs once it has started (an existing key may have).
        let record = match self.call(Req::Get { job: job.clone() }) {
            Ok(Response::Job { job }) => Some(*job),
            _ => None,
        };
        let workspace = record
            .as_ref()
            .and_then(|r| r.workspace.clone())
            .or(spec.workspace.clone());
        let state = record.as_ref().map(|r| r.state);
        let mut human = format!("job {job}\n");
        if let Some(w) = &workspace {
            human.push_str(&format!("workspace {w}\n"));
        }
        if existing {
            human.push_str(&format!(
                "(a job with key {} already existed; this is that job{})\n",
                spec.key.as_deref().unwrap_or("?"),
                state.map(|s| format!(", {s}")).unwrap_or_default()
            ));
        }
        let mut notes = Vec::new();
        if !left_out.is_empty() {
            notes.push(format!(
                "left out {} and what is nested in {}: {}",
                if left_out.len() == 1 {
                    "1 submodule".to_string()
                } else {
                    format!("{} submodules", left_out.len())
                },
                if left_out.len() == 1 { "it" } else { "them" },
                left_out.join(", ")
            ));
        }
        if !spec.repo.carried.is_empty() {
            let carried: Vec<String> = spec
                .repo
                .carried
                .iter()
                .map(|c| {
                    format!(
                        "{} at {} (POTLUCK_MIRROR_{})",
                        c.name,
                        crate::gitinfo::short(&c.commit),
                        c.name
                    )
                })
                .collect();
            notes.push(format!("carried {}", carried.join(", ")));
        }
        // One `potluck: ` line each.
        let note = notes.join("\npotluck: ");
        let carried: Vec<Value> = spec
            .repo
            .carried
            .iter()
            .map(|c| json!({ "name": c.name, "commit": c.commit, "source": c.source }))
            .collect();
        Ok(Report {
            note,
            ..Report::new(
                json!({
                    "job": job,
                    "existing": existing,
                    "workspace": workspace,
                    "state": state,
                    "commit": spec.repo.commit,
                    "branch": spec.repo.branch,
                    "without_submodules": left_out,
                    "carried": carried,
                }),
                human,
            )
        })
    }

    /// The dispatch `spec`, signed with this command line's user key when it has one, for the
    /// local daemon to own (its name, asked first, is the spec's `submitted_from`). Without a
    /// key it goes unsigned, unless the cluster has users: then it is refused here.
    fn signed(&mut self, mut spec: crate::types::JobSpec) -> anyhow::Result<crate::types::JobSpec> {
        let path = crate::mesh::user::key_path(&self.env.paths);
        let Some(key) = UserKey::load(&path)? else {
            if self
                .membership_for_signing()?
                .is_some_and(|m| m.users_exist)
            {
                anyhow::bail!(
                    "this cluster has users, so a dispatch is signed with a user's key, and there is none at {} (dispatch from the machine where a user's key is; or make one here with `potluck user key` and have a user run `potluck user add NAME KEY`)",
                    path.display()
                );
            }
            return Ok(spec);
        };
        spec.submitted_from = match self.call(Req::Ping)? {
            Response::Pong { node, .. } => node,
            other => return Err(unexpected("ping", &other)),
        };
        Ok(crate::jobauth::sign(&spec, &key, crate::types::now_ms()))
    }

    fn dispatch_nested(
        &mut self,
        args: &DispatchArgs,
        job: &str,
        lease: &str,
    ) -> anyhow::Result<Report> {
        if let Some((flag, why)) = nested::refused_option(args, job) {
            anyhow::bail!(
                "{flag} doesn't apply inside job {job}: a dispatch from inside a job runs in place, not as a job of its own, and {why}"
            );
        }
        let file = nested::project_file_here(&self.env.cwd, args.recipe.is_some())?;
        let r = crate::recipe::resolve(&file, args)?;
        let ignored = nested::recipe_ignored(&file, args.recipe.as_deref());
        let (grant, time_left) = match self.call(Req::LeaseCheck {
            job: job.to_string(),
            token: lease.to_string(),
        })? {
            Response::Lease {
                live: true,
                grant: Some(grant),
                remaining_ms,
            } => (grant, remaining_ms),
            Response::Lease { .. } => anyhow::bail!(
                "this runs inside job {job} (POTLUCK_JOB), but its lease is not live; a dispatch from inside a job runs only within the job's own lease"
            ),
            other => return Err(unexpected("lease check", &other)),
        };
        // Aliases and the node name come from it; a config that doesn't parse would make the
        // checks below compare against the wrong names.
        let config = crate::config::NodeConfig::load(&self.env.paths)
            .map_err(|e| anyhow::anyhow!("reading this machine's config: {e:#}"))?;
        nested::check(
            &r,
            &grant,
            time_left,
            &self.env.paths,
            &config,
            args.workspace.as_deref(),
            self.env.workspace.as_deref(),
        )
        .map_err(|why| {
            anyhow::anyhow!(
                "can't run inside job {job}: {why}. A dispatch from inside a job runs in place only when it fits the job's own lease, and never waits for it"
            )
        })?;
        if !self.json {
            let _ = writeln!(
                self.err,
                "potluck: inside job {job}: running in place, within its lease (not dispatched)"
            );
            if !ignored.is_empty() {
                let _ = writeln!(
                    self.err,
                    "potluck: inside job {job}: recipe {}'s {} don't apply in place; job {job}'s own do",
                    r.recipe.as_deref().unwrap_or_default(),
                    match ignored.split_last() {
                        Some((last, [])) => last.to_string(),
                        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
                        None => String::new(),
                    }
                );
            }
        }
        let _ = self.out.flush();
        let grace = Duration::from_millis(config.kill_grace_ms());
        // Where a dispatch from outside would run it: the top of the repository (its workspace's
        // root), not the subdirectory the job's script is in.
        let dir = nested::top_of(&self.env.cwd);
        let code = nested::run_in_place(&r, &dir, self.json, grace)?;
        Ok(Report {
            code,
            json: json!({
                "job": job,
                "in_place": true,
                "cmd": r.cmd,
                "exit_code": code,
                "ignored": ignored,
            }),
            human: String::new(),
            note: String::new(),
            streamed: false,
        })
    }

    /// After the connection to the daemon failed (it restarts after an update; jobs survive
    /// that), connects again, trying for up to [`RECONNECT`]. `lost` is the failure, for the
    /// error when the daemon doesn't come back.
    fn reconnect(&mut self, job: &str, lost: anyhow::Error) -> anyhow::Result<()> {
        self.client = None;
        let give_up = Instant::now() + RECONNECT;
        loop {
            match self.connect() {
                Ok(_) => return Ok(()),
                Err(_) if Instant::now() < give_up => {
                    std::thread::sleep(Duration::from_millis(250))
                }
                Err(e) => anyhow::bail!(
                    "{lost:#}; the daemon did not answer again within {}s ({e:#}). The job keeps running: `potluck wait {job}` waits again",
                    RECONNECT.as_secs()
                ),
            }
        }
    }

    /// From inside job `job` (POTLUCK_JOB), waiting for it would wait for itself until its run
    /// limit (a dispatch run in place reports the job it ran in, so a script may get here).
    fn not_itself(&self, job: &str, what: &str) -> anyhow::Result<()> {
        if self.env.job.as_deref() == Some(job) {
            anyhow::bail!(
                "{job} is the job this runs in: {what} it from inside would wait for itself until its run limit"
            );
        }
        Ok(())
    }

    fn wait(&mut self, job: &str, max_wait: Option<&str>, quiet: bool) -> anyhow::Result<Report> {
        self.not_itself(job, "waiting for")?;
        let max_ms = max_wait
            .map(|m| parse_duration_ms(m).map_err(|e| anyhow::anyhow!("--max-wait: {e}")))
            .transpose()?;
        let deadline = max_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
        let left =
            || deadline.map(|d| d.saturating_duration_since(Instant::now()).as_millis() as u64);
        let json = self.json;
        if !quiet {
            let mut offset = 0;
            loop {
                let wait_ms = left().unwrap_or(POLL_MS).min(POLL_MS);
                if deadline.is_some() && wait_ms == 0 {
                    break;
                }
                self.connect()?;
                let client = self.client.as_mut().expect("connected");
                let sink: &mut dyn Write = if json { &mut *self.err } else { &mut *self.out };
                match log_chunk(client, job, &mut offset, wait_ms, sink) {
                    Ok((_, true)) => break,
                    Ok(_) => {}
                    Err(e) if crate::client::is_disconnected(&e) => self.reconnect(job, e)?,
                    // Nobody reads the output any more (`| head`): wait without it, for the
                    // job's code, on a new connection (this one is midway through a piece).
                    Err(e) if broken_pipe(&e) => {
                        self.client = None;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        loop {
            let timeout_ms = left().unwrap_or(POLL_MS).min(POLL_MS);
            let answer = self.call(Req::Wait {
                job: job.to_string(),
                timeout_ms: Some(timeout_ms),
            });
            let record = match answer {
                Ok(Response::Job { job }) => *job,
                // Its owner can't be reached now but is still up (restarting, or one of the two
                // updating), and nothing else knows how the job is: wait for it, within
                // --max-wait.
                Ok(Response::OwnerAway { message }) => {
                    if deadline.is_some() && left() == Some(0) {
                        return Err(anyhow::anyhow!(message));
                    }
                    continue;
                }
                Ok(other) => return Err(unexpected("wait", &other)),
                Err(e) if crate::client::is_disconnected(&e) => {
                    self.reconnect(job, e)?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            if record.state.is_done() {
                return Ok(ended_report(&record));
            }
            if deadline.is_some() && left() == Some(0) {
                let max = max_wait.unwrap_or_default();
                return Ok(Report {
                    code: EXIT_STILL_RUNNING,
                    json: json!({
                        "job": record.id,
                        "ended": false,
                        "state": record.state,
                        "record": record,
                    }),
                    human: String::new(),
                    note: format!(
                        "job {job} is still {} after {max}; it keeps running (`potluck wait {job}` waits again)",
                        record.state
                    ),
                    streamed: false,
                });
            }
        }
    }

    fn logs(&mut self, job: &str, follow: bool) -> anyhow::Result<Report> {
        if follow {
            self.not_itself(job, "following")?;
        }
        if self.json {
            return match self.logs_json(job, follow) {
                // Nobody reads the document any more (`| head`): done, as without --json.
                Err(e) if broken_pipe(&e) => Ok(Report {
                    streamed: true,
                    ..Report::new(Value::Null, String::new())
                }),
                // Its own errors are text; an I/O error is the document's write to stdout.
                Err(e) if e.is::<std::io::Error>() => Err(e.context("writing the job's output")),
                other => other,
            };
        }
        let mut offset = 0;
        loop {
            self.connect()?;
            let client = self.client.as_mut().expect("connected");
            let wait_ms = if follow { POLL_MS } else { 0 };
            match log_chunk(client, job, &mut offset, wait_ms, &mut *self.out) {
                Ok((len, done)) if done || (!follow && len == 0) => break,
                Ok(_) => {}
                Err(e) if crate::client::is_disconnected(&e) => self.reconnect(job, e)?,
                // Nobody reads the output any more (`| head`): done.
                Err(e) if broken_pipe(&e) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(Report::new(Value::Null, String::new()))
    }

    /// `logs --json`: the document goes to stdout as the output arrives, its `output` a piece at
    /// a time, so a log of any size takes no more memory than one piece (it used to be held
    /// twice: the bytes, then the string). An error once the document has begun ends it with an
    /// `error` field, so it stays one JSON document.
    fn logs_json(&mut self, job: &str, follow: bool) -> anyhow::Result<Report> {
        let mut text = JsonText::default();
        let mut started = false;
        let mut offset = 0;
        let mut piece = Vec::new();
        let ended: anyhow::Result<bool> = loop {
            if let Err(e) = self.connect() {
                break Err(e);
            }
            let client = self.client.as_mut().expect("connected");
            let wait_ms = if follow { POLL_MS } else { 0 };
            piece.clear();
            let got = log_chunk(client, job, &mut offset, wait_ms, &mut piece);
            if got.is_ok() || !piece.is_empty() {
                if !started {
                    write!(self.out, "{{\"job\":{},\"output\":\"", Value::from(job))?;
                    started = true;
                }
                text.write(&mut *self.out, &piece)?;
            }
            match got {
                Ok((len, done)) if done || (!follow && len == 0) => break Ok(done),
                Ok(_) => {}
                Err(e) if crate::client::is_disconnected(&e) => {
                    if let Err(e) = self.reconnect(job, e) {
                        break Err(e);
                    }
                }
                Err(e) => break Err(e),
            }
        };
        let ended = match ended {
            // Nothing written yet: the usual error document.
            Err(e) if !started => return Err(e),
            other => other,
        };
        if !started {
            write!(self.out, "{{\"job\":{},\"output\":\"", Value::from(job))?;
        }
        text.finish(&mut *self.out)?;
        let (done, error) = match &ended {
            Ok(done) => (*done, None),
            Err(e) => (false, Some(format!("{e:#}"))),
        };
        write!(self.out, "\",\"bytes\":{offset},\"done\":{done}")?;
        if let Some(e) = &error {
            write!(self.out, ",\"error\":{}", Value::from(e.as_str()))?;
        }
        let reminders = output::reminders_json(&self.reminders());
        writeln!(self.out, ",\"reminders\":{reminders}}}")?;
        self.out.flush()?;
        Ok(Report {
            code: if error.is_some() { EXIT_ERROR } else { 0 },
            json: Value::Null,
            human: String::new(),
            note: String::new(),
            streamed: true,
        })
    }

    fn cancel(&mut self, job: &str) -> anyhow::Result<Report> {
        let record = match self.call(Req::Cancel {
            job: job.to_string(),
        })? {
            Response::Job { job } => Some(*job),
            Response::Ok => None,
            other => return Err(unexpected("cancel", &other)),
        };
        let human = match &record {
            Some(r) if r.state.is_done() && r.state != crate::types::JobState::Cancelled => {
                format!("job {job} had already ended ({})", r.state)
            }
            _ => format!("cancelled job {job}"),
        };
        Ok(Report::new(json!({ "job": job, "record": record }), human))
    }

    /// `potluck resize JOB --ram N`: the job's RAM request lowered by its owner, the job keeping
    /// its place in line.
    fn resize(&mut self, job: &str, ram: &str) -> anyhow::Result<Report> {
        let ram_mib =
            crate::types::parse_ram_mib(ram).map_err(|e| anyhow::anyhow!("--ram: {e}"))?;
        let auth = self.signed_resize(job, ram_mib)?;
        let (record, from) = match self.call(Req::Resize {
            job: job.to_string(),
            ram_mib,
            auth,
        }) {
            Ok(Response::Resized { job, from_ram_mib }) => (*job, from_ram_mib),
            Ok(other) => return Err(unexpected("resize", &other)),
            Err(e) if crate::client::not_understood(&e) => anyhow::bail!(
                "the daemon here runs a potluck from before `potluck resize`; `potluck restart` \
                 restarts it on the binary at its path"
            ),
            Err(e) => return Err(e),
        };
        let now = record.spec.request.ram_mib;
        let changed = now != from;
        let human = match changed {
            true => format!(
                "job {job} now asks for {} of RAM, not {}; it keeps its place in the queue",
                fmt_mib(now),
                fmt_mib(from)
            ),
            false => format!(
                "job {job} asks for {} of RAM already; nothing changed",
                fmt_mib(now)
            ),
        };
        Ok(Report::new(
            json!({
                "job": job,
                "changed": changed,
                "ram_mib": now,
                "from_ram_mib": from,
                "record": record,
            }),
            human,
        ))
    }

    /// For `resize`: the job's spec as its owner holds it, with `ram_mib` as its RAM request,
    /// signed with this command line's user key when it has one (for its owner, its ids salted
    /// already: [`crate::jobauth::salted_already`]). Without a key it goes unsigned, unless the
    /// cluster has users: then it is refused here.
    fn signed_resize(
        &mut self,
        job: &str,
        ram_mib: u64,
    ) -> anyhow::Result<Option<Box<crate::jobauth::JobAuth>>> {
        let path = crate::mesh::user::key_path(&self.env.paths);
        let Some(key) = UserKey::load(&path)? else {
            if self
                .membership_for_signing()?
                .is_some_and(|m| m.users_exist)
            {
                anyhow::bail!(
                    "this cluster has users, so a resize is signed with a user's key, and there is none at {} (resize from the machine where a user's key is; or make one here with `potluck user key` and have a user run `potluck user add NAME KEY`)",
                    path.display()
                );
            }
            return Ok(None);
        };
        let held = match self.call(Req::Get {
            job: job.to_string(),
        })? {
            Response::Job { job } => job.spec,
            other => return Err(unexpected("get", &other)),
        };
        let spec = crate::types::JobSpec {
            request: crate::types::Request {
                ram_mib,
                ..held.request.clone()
            },
            auth: None,
            ..held
        };
        Ok(crate::jobauth::sign(&spec, &key, now_ms())
            .auth
            .map(Box::new))
    }

    fn hold(&mut self, workspace: &str, duration: &str) -> anyhow::Result<Report> {
        let duration_ms = crate::recipe::parse_hold_ms(duration)?;
        let record = match self.call(Req::Hold {
            workspace: workspace.to_string(),
            duration_ms,
        })? {
            Response::Workspace { workspace } => Some(*workspace),
            Response::Ok => None,
            other => return Err(unexpected("hold", &other)),
        };
        let held_until = record.as_ref().map(|w| w.held_until);
        Ok(Report::new(
            json!({
                "workspace": workspace,
                "duration_ms": duration_ms,
                "held_until": held_until,
                "record": record,
            }),
            format!(
                "workspace {workspace} is held for {} from now (`potluck release {workspace}` ends it sooner)",
                crate::types::fmt_ms(duration_ms)
            ),
        ))
    }

    fn release(&mut self, workspace: &str) -> anyhow::Result<Report> {
        let record = match self.call_slow(Req::Release {
            workspace: workspace.to_string(),
        })? {
            Response::Workspace { workspace } => Some(*workspace),
            Response::Ok => None,
            other => return Err(unexpected("release", &other)),
        };
        let human = released(workspace, record.as_ref());
        Ok(Report::new(
            json!({ "workspace": workspace, "record": record }),
            human,
        ))
    }

    fn cp(&mut self, from: &str, dest: &Path) -> anyhow::Result<Report> {
        let (workspace, path) = copy::parse_source(from)?;
        // Without `.` components, so the report names the path as people would write it
        // (`/src/app/Cargo.lock`, not `/src/app/./Cargo.lock`). `..` stays: it can cross a link.
        let dest_dir = dest.as_os_str().as_encoded_bytes().ends_with(b"/");
        let dest: PathBuf = self.env.cwd.join(dest).components().collect();
        if dest_dir && !dest.is_dir() {
            anyhow::bail!("cp: {}/ is not a directory", dest.display());
        }
        let client = self.connect()?;
        let stats = copy::copy_out(client, &workspace, &path, &dest)?;
        Ok(Report::new(
            json!({
                "workspace": workspace,
                "path": path,
                "dest": stats.root,
                "files": stats.files,
                "dirs": stats.dirs,
                "bytes": stats.bytes,
            }),
            format!(
                "copied {} file{} ({}) from {workspace}:{path} to {}",
                stats.files,
                if stats.files == 1 { "" } else { "s" },
                output::fmt_bytes(stats.bytes),
                stats.root.display()
            ),
        ))
    }

    fn status(&mut self) -> anyhow::Result<Report> {
        let status = match self.call(Req::Status)? {
            Response::Status { status } => *status,
            other => return Err(unexpected("status", &other)),
        };
        let human = output::status_human(&status, now_ms());
        Ok(Report::new(serde_json::to_value(&status)?, human))
    }

    fn why(&mut self, job: &str) -> anyhow::Result<Report> {
        let why = match self.call(Req::Why {
            job: job.to_string(),
        })? {
            Response::Why { why } => why,
            other => return Err(unexpected("why", &other)),
        };
        let human = output::why_human(&why);
        Ok(Report::new(serde_json::to_value(&why)?, human))
    }

    fn nodes(&mut self) -> anyhow::Result<Report> {
        let nodes = match self.call(Req::Nodes)? {
            Response::Nodes { nodes } => nodes,
            other => return Err(unexpected("nodes", &other)),
        };
        let human = output::nodes_human(&nodes, now_ms());
        Ok(Report::new(json!({ "nodes": nodes }), human))
    }

    /// `potluck env [NODE]`: the login environment NODE's daemon holds, which every job there
    /// starts from.
    fn env(&mut self, node: Option<String>) -> anyhow::Result<Report> {
        let (node, env) = match self.call(Req::Env { node }) {
            Ok(Response::Env { node, env }) => (node, env),
            Ok(other) => return Err(unexpected("env", &other)),
            Err(e) if crate::client::not_understood(&e) => anyhow::bail!(
                "the daemon here runs a potluck from before `potluck env`; `potluck restart` \
                 restarts it on the binary at its path"
            ),
            Err(e) => return Err(e),
        };
        Ok(Report {
            note: format!(
                "this is {node}'s login environment, as its daemon captured it; a job on {node} \
                 gets the `[[gpu]]` env of the GPUs it holds, its dispatch's env and potluck's own \
                 variables on top, without what the dispatch unsets"
            ),
            ..Report::new(json!({ "node": node, "env": env }), output::env_human(&env))
        })
    }

    /// `potluck restart`: the daemon restarts in place, and the new binary answers.
    fn restart(&mut self) -> anyhow::Result<Report> {
        let log = self.env.paths.daemon_log();
        let cant = |message: String| Report {
            code: EXIT_NO_RESTART_HERE,
            json: json!({ "restarted": false, "error": message }),
            human: String::new(),
            note: message,
            streamed: false,
        };
        let client = match self.connect() {
            Ok(c) => c,
            Err(e) => return Ok(cant(format!("{e:#}"))),
        };
        client.set_read_timeout(RESTART_WAIT)?;
        // The daemon refuses when it runs another binary that hasn't changed: nothing would.
        let binary = crate::update::running_binary().ok();
        match client.call(Req::Restart { binary }) {
            Ok(Response::Text { text }) => {
                // Done once the daemon answers again: at once on this connection after a restart
                // in place, on a new one after a plain restart (which closed this one).
                let give_up = Instant::now() + RECONNECT;
                while self.call(Req::Ping).is_err() {
                    self.client = None;
                    if Instant::now() >= give_up {
                        anyhow::bail!(
                            "{text}\nThe daemon did not answer again within {}s; whether it runs is \
                             in {}",
                            RECONNECT.as_secs(),
                            log.display()
                        );
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Ok(Report::new(
                    json!({ "restarted": true, "text": text }),
                    text,
                ))
            }
            Ok(other) => Err(unexpected("restart", &other)),
            Err(e) if crate::client::not_understood(&e) => Ok(cant(format!(
                "the daemon runs a potluck from before `potluck restart`, so it can't restart \
                 in place; restart its service instead: {}",
                crate::update::restart_command()
            ))),
            Err(e) if crate::client::is_disconnected(&e) => Err(anyhow::anyhow!(
                "{e:#}; whether the daemon restarted is in {}",
                log.display()
            )),
            Err(e) => Err(e),
        }
    }

    /// A request answered with text to show (pairing), or just Ok.
    /// What the daemon says membership records are signed from.
    fn membership(&mut self) -> anyhow::Result<crate::mesh::MembershipView> {
        self.membership_if_known()?.ok_or_else(|| {
            anyhow::anyhow!(
                "the daemon here runs a potluck from before users; `potluck restart` restarts it \
                 on the binary at its path"
            )
        })
    }

    /// [`Cx::membership`]; None from a daemon from before users (its cluster has none).
    fn membership_if_known(&mut self) -> anyhow::Result<Option<crate::mesh::MembershipView>> {
        match self.call(Req::Membership) {
            Ok(Response::Membership { membership }) => Ok(Some(*membership)),
            Ok(other) => Err(unexpected("membership", &other)),
            Err(e) if crate::client::not_understood(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// For `admit` and `remove`, whether to sign: [`Cx::membership`] if the daemon says, else
    /// None. The daemon decides whether a request needs a user's signature, and says so when one
    /// that has none does.
    fn membership_for_signing(&mut self) -> anyhow::Result<Option<crate::mesh::MembershipView>> {
        match self.membership_if_known() {
            Err(e) if !crate::client::is_disconnected(&e) => Ok(None),
            other => other,
        }
    }

    /// This command line's user key, and its user's name in the cluster `m` describes, to sign
    /// `what` with.
    fn signing_user(
        &self,
        m: &crate::mesh::MembershipView,
        what: &str,
    ) -> anyhow::Result<(UserKey, String)> {
        let path = crate::mesh::user::key_path(&self.env.paths);
        let Some(key) = UserKey::load(&path)? else {
            anyhow::bail!(
                "this cluster has users, so {what} is signed with a user's key, and there is none at {} (on the machine where a user's key is, run it there; or make one here with `potluck user key` and have a user run `potluck user add NAME KEY`)",
                path.display()
            );
        };
        match m.users.iter().find(|u| u.key == key.public()) {
            Some(u) => Ok((key, u.name.clone())),
            None => anyhow::bail!(
                "the user key at {} is no user of this cluster; a user adds it with `potluck user add NAME {}`",
                path.display(),
                key.public()
            ),
        }
    }

    /// `potluck admit [--pool P] REQUEST`: in a cluster with users, the admission signed here.
    fn admit(&mut self, request: String, pool: Option<String>) -> anyhow::Result<Report> {
        use crate::mesh::trust::{self, DEFAULT_POOL};
        let m = self.membership_for_signing()?;
        let pool = pool.unwrap_or_else(|| DEFAULT_POOL.to_string());
        if !trust::valid_pool(&pool) {
            anyhow::bail!(
                "bad pool name {pool:?}: use letters, digits, '.', '_' and '-', not starting with '-'"
            );
        }
        let admission = if let Some(m) = m.filter(|m| m.users_exist) {
            let req = trust::PairRequest::from_text(&request)?;
            let (key, by) = self.signing_user(&m, "an admission")?;
            Some(Box::new(crate::mesh::Admission::sign_by_user(
                &key,
                &by,
                &req.name,
                req.key,
                &req.address,
                &pool,
                &req.nonce,
                m.next_clock,
            )))
        } else if pool != DEFAULT_POOL {
            anyhow::bail!(
                "this cluster has no users, and pools come with them: every machine is in pool {DEFAULT_POOL} until `potluck user init`"
            );
        } else {
            None
        };
        let by_user = admission.is_some();
        let reply = self.text_request(Req::Admit { request, admission }, "admit")?;
        let mut json = reply.json;
        if let Value::Object(map) = &mut json {
            map.insert("pool".into(), json!(pool));
            map.insert("signed_by_user".into(), json!(by_user));
        }
        Ok(Report {
            json,
            note: "paste this line into `potluck pair --finish` on the new machine".into(),
            ..reply
        })
    }

    /// In a cluster with users, the removal of `node` signed with this command line's user key;
    /// None without users (a member signs it).
    fn signed_removal(&mut self, node: &str) -> anyhow::Result<Option<Box<crate::mesh::Removal>>> {
        let Some(m) = self.membership_for_signing()?.filter(|m| m.users_exist) else {
            return Ok(None);
        };
        let Some(target) = m.members.iter().chain(&m.removed).find(|e| e.name == node) else {
            anyhow::bail!("{node} is not a member of this cluster");
        };
        let (key, by) = self.signing_user(&m, "a removal")?;
        Ok(Some(Box::new(crate::mesh::Removal::sign_by_user(
            &key,
            &by,
            node,
            target.key,
            m.next_clock,
        ))))
    }

    fn user(&mut self, action: UserAction) -> anyhow::Result<Report> {
        use crate::mesh::{Record, UserAdmission, UserRemoval};
        let path = crate::mesh::user::key_path(&self.env.paths);
        match action {
            UserAction::Key => {
                let (key, made) = UserKey::load_or_create(&path)?;
                Ok(Report {
                    note: match made {
                        true => format!(
                            "made a user key at {}; a user makes it a user with `potluck user add NAME {}`",
                            path.display(),
                            key.public()
                        ),
                        false => String::new(),
                    },
                    ..Report::new(
                        json!({ "key": key.public(), "file": path, "made": made }),
                        key.public().to_text(),
                    )
                })
            }
            UserAction::List => {
                let m = self.membership()?;
                let mine = UserKey::load(&path).ok().flatten().map(|k| k.public());
                let rows: Vec<Vec<String>> = m
                    .users
                    .iter()
                    .map(|u| {
                        vec![
                            u.name.clone(),
                            u.key.to_text(),
                            u.by.clone(),
                            if Some(u.key) == mine { "yes" } else { "" }.to_string(),
                        ]
                    })
                    .collect();
                let users: Vec<Value> = m
                    .users
                    .iter()
                    .map(|u| {
                        json!({
                            "name": u.name, "key": u.key, "by": u.by, "at": u.at,
                            "this_cli": Some(u.key) == mine,
                        })
                    })
                    .collect();
                let human = match m.users_exist {
                    false => "this cluster has no users: its members sign who joins and leaves it (`potluck user init` makes the first user)\n".to_string(),
                    true => table(&["NAME", "KEY", "ADDED BY", "THIS CLI"], &rows),
                };
                Ok(Report::new(
                    json!({ "users_exist": m.users_exist, "users": users }),
                    human,
                ))
            }
            UserAction::Init { name } => {
                let m = self.membership()?;
                if m.users_exist {
                    let names: Vec<&str> = m.users.iter().map(|u| u.name.as_str()).collect();
                    anyhow::bail!(
                        "this cluster has users already ({}); `potluck user add NAME KEY` adds another",
                        names.join(", ")
                    );
                }
                if !m.member {
                    anyhow::bail!(
                        "{} is not in its cluster (`potluck status` says why); run `potluck pair` to join one",
                        m.node
                    );
                }
                let name = name
                    .or_else(|| std::env::var("USER").ok().filter(|u| !u.is_empty()))
                    .unwrap_or_else(|| "user".to_string());
                if !crate::mesh::trust::valid_name(&name) {
                    anyhow::bail!(
                        "bad user name {name:?}: use letters, digits, '.', '_' and '-', not starting with '-' (--name)"
                    );
                }
                let (key, made) = UserKey::load_or_create(&path)?;
                let genesis = UserAdmission::genesis(&key, &name, &m.node, m.key, m.next_clock);
                let admissions: Vec<crate::mesh::Admission> = m
                    .members
                    .iter()
                    .map(|e| {
                        crate::mesh::Admission::sign_by_user(
                            &key,
                            &name,
                            &e.name,
                            e.key,
                            &e.address,
                            crate::mesh::trust::DEFAULT_POOL,
                            "",
                            m.next_clock + 1,
                        )
                    })
                    .collect();
                match self.call(Req::UserInit {
                    genesis: Box::new(genesis),
                    admissions,
                })? {
                    Response::Ok => {}
                    other => return Err(unexpected("user init", &other)),
                }
                let members: Vec<&str> = m.members.iter().map(|e| e.name.as_str()).collect();
                let warning = match m.elders.as_slice() {
                    [] => None,
                    elders => Some(format!(
                        "{} {} admitted before {}, and with {} machine key {} can co-sign a first user's admission that counts instead of this one",
                        elders.join(", "),
                        if elders.len() == 1 { "was" } else { "were" },
                        m.node,
                        if elders.len() == 1 { "its" } else { "its own" },
                        if elders.len() == 1 { "it" } else { "each" },
                    )),
                };
                let mut notes = Vec::new();
                if made {
                    notes.push(format!("made a user key at {}", path.display()));
                }
                notes.extend(warning.clone());
                Ok(Report {
                    note: notes.join("\npotluck: "),
                    ..Report::new(
                        json!({
                            "user": name,
                            "key": key.public(),
                            "file": path,
                            "made": made,
                            "members": members,
                            "warning": warning,
                        }),
                        format!(
                            "{name} is the cluster's first user; signed in {}: {}",
                            if members.len() == 1 {
                                "its member"
                            } else {
                                "its members"
                            },
                            members.join(", ")
                        ),
                    )
                })
            }
            UserAction::Add { name, key } => {
                let m = self.membership()?;
                if !m.users_exist {
                    anyhow::bail!(
                        "this cluster has no users yet: `potluck user init` makes the first"
                    );
                }
                if !crate::mesh::trust::valid_name(&name) {
                    anyhow::bail!(
                        "bad user name {name:?}: use letters, digits, '.', '_' and '-', not starting with '-'"
                    );
                }
                let new = crate::mesh::PublicKey::from_text(&key).map_err(|_| {
                    anyhow::anyhow!(
                        "{key:?} is not a user key: `potluck user key` prints one, 43 characters"
                    )
                })?;
                let (signer, by) = self.signing_user(&m, "a user's admission")?;
                let rec = UserAdmission::sign(&signer, &by, &name, new, m.next_clock);
                match self.call(Req::UserRecord {
                    record: Box::new(Record::User(rec)),
                })? {
                    Response::Ok => {}
                    other => return Err(unexpected("user add", &other)),
                }
                Ok(Report::new(
                    json!({ "added": name, "key": new, "by": by }),
                    format!("{name} is a user of the cluster"),
                ))
            }
            UserAction::Remove { name } => {
                let m = self.membership()?;
                if !m.users_exist {
                    anyhow::bail!("this cluster has no users");
                }
                let Some(target) = m.users.iter().find(|u| u.name == name) else {
                    anyhow::bail!("no user is named {name} (`potluck user list` lists them)");
                };
                if m.users.len() == 1 {
                    anyhow::bail!("{name} is the last user, and a cluster with users keeps one");
                }
                let (signer, by) = self.signing_user(&m, "a user's removal")?;
                let rec = UserRemoval::sign(
                    &signer,
                    &by,
                    &name,
                    target.key,
                    target.signed.clone(),
                    m.next_clock,
                );
                match self.call(Req::UserRecord {
                    record: Box::new(Record::UserRemoval(rec)),
                })? {
                    Response::Ok => {}
                    other => return Err(unexpected("user remove", &other)),
                }
                Ok(Report::new(
                    json!({ "removed": name, "by": by }),
                    format!("{name} is no user of the cluster any more"),
                ))
            }
        }
    }

    fn text_request(&mut self, req: Req, what: &str) -> anyhow::Result<Report> {
        match self.call(req)? {
            Response::Text { text } => Ok(Report::new(json!({ "text": text }), text)),
            Response::Ok => Ok(Report::new(json!({ "ok": true }), format!("{what}: done"))),
            other => Err(unexpected(what, &other)),
        }
    }

    fn service(&mut self, action: ServiceAction) -> anyhow::Result<Report> {
        let spec = crate::service::ServiceSpec::for_this_machine(&self.env.paths)?;
        let done = match action {
            ServiceAction::Install => {
                // What the daemon refuses to start without, checked before the service manager
                // starts it (and restarts it, again and again) behind our back.
                let config = crate::config::NodeConfig::load(&self.env.paths)?;
                let name = crate::facts::node_name(&self.env.paths, &config);
                if !crate::mesh::trust::valid_name(&name) {
                    anyhow::bail!(
                        "bad node name {name:?}: use letters, digits, '.', '_' and '-', not starting with '-' (set `name` in {})",
                        self.env.paths.config.display()
                    );
                }
                crate::daemon::handover::starts_here()?;
                crate::preflight::check(&self.env.paths)?;
                // A daemon already running outside the service would keep the service's daemon
                // from starting, and the service manager would start it again and again.
                if crate::daemon::running_here(&self.env.paths) && !crate::service::active(&spec) {
                    anyhow::bail!(
                        "a potluck daemon already runs for {} outside the service (a `potluck daemon` in a terminal?), and the service's daemon would fail beside it: stop that daemon, then run `potluck service install` again",
                        self.env.paths.home.display()
                    );
                }
                crate::service::install(&spec)?
            }
            ServiceAction::Uninstall => crate::service::uninstall(&spec)?,
        };
        Ok(Report::new(
            serde_json::to_value(&done)?,
            done.message.clone(),
        ))
    }
}

/// The report of `wait` for a job that has ended: its exit code, and a line saying how it ended.
fn ended_report(record: &JobRecord) -> Report {
    let code = record.exit_code_for_client();
    Report {
        code,
        json: json!({
            "job": record.id,
            "ended": true,
            "state": record.state,
            "exit_code": code,
            "message": record.message,
            "workspace": record.workspace,
            "record": record,
        }),
        human: String::new(),
        note: format!(
            "job {} {}, exit code {code}",
            record.id,
            how_it_ended(record)
        ),
        streamed: false,
    }
}

/// How a job ended, saying each thing once: its state, then the daemon's message unless that only
/// repeats the state or the exit code ("cancelled", "exited with code 3"). A message that starts
/// with the state ("over memory: used ...") stands for both.
fn how_it_ended(record: &JobRecord) -> String {
    let state = record.state.to_string();
    match record.message.as_deref() {
        None => state,
        Some(m)
            if record
                .exit_code
                .is_some_and(|c| m == format!("exited with code {c}")) =>
        {
            state
        }
        Some(m) if m.starts_with(&state) => m.to_string(),
        Some(m) => format!("{state}: {m}"),
    }
}

/// Asks for the job's output from `offset` and copies what comes to `sink`. `offset` moves past
/// each byte written, also when the connection fails partway, so the caller can resume there.
/// Returns how many bytes the daemon sent and whether the log is complete.
fn log_chunk(
    client: &mut Client,
    job: &str,
    offset: &mut u64,
    wait_ms: u64,
    sink: &mut dyn Write,
) -> anyhow::Result<(u64, bool)> {
    match client.call(Req::Logs {
        job: job.to_string(),
        offset: *offset,
        max: LOG_CHUNK,
        wait_ms,
    })? {
        Response::Bytes {
            offset: at,
            len,
            done,
        } => {
            let mut copied = 0;
            let result = client
                .copy_bytes_counted(len, sink, &mut copied)
                .and_then(|()| sink.flush().map_err(Into::into));
            *offset = at + copied;
            result.map_err(|e| {
                if crate::client::is_disconnected(&e) {
                    e
                } else {
                    e.context("writing the job's output")
                }
            })?;
            Ok((len, done))
        }
        other => Err(unexpected("logs", &other)),
    }
}

/// Whether `e` is a write to a pipe nobody reads any more (`potluck logs J | head`).
fn broken_pipe(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
    })
}

fn unexpected(what: &str, resp: &Response) -> anyhow::Error {
    let kind = serde_json::to_value(resp)
        .ok()
        .and_then(|v| v.get("kind").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| "?".into());
    anyhow::anyhow!("unexpected answer from the daemon to {what}: {kind}")
}

/// What `release` did: removed the workspace, or (a job queued or running in it) only ended its
/// hold.
fn released(workspace: &str, record: Option<&crate::types::WorkspaceRecord>) -> String {
    match record {
        Some(w) if w.state != crate::types::WorkspaceState::Removed => format!(
            "released workspace {workspace}: its hold ended, but a job is queued or running in it, so it is not removed"
        ),
        _ => format!("released workspace {workspace}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{WorkspaceRecord, WorkspaceState};

    #[test]
    fn release_says_when_the_workspace_stays() {
        let mut w = WorkspaceRecord {
            id: "studio-w5".into(),
            node: "studio".into(),
            repo_id: "app-0011aabb".into(),
            commit: "0123456789abcdef".into(),
            branch: Some("main".into()),
            path: "/tmp/w".into(),
            state: WorkspaceState::Removed,
            created_at: 0,
            last_used_at: 0,
            held_until: 0,
            pending: 0,
            size_bytes: None,
            advanced_by: None,
        };
        assert_eq!(
            released("studio-w5", Some(&w)),
            "released workspace studio-w5"
        );
        assert_eq!(released("studio-w5", None), "released workspace studio-w5");
        w.state = WorkspaceState::Running;
        assert_eq!(
            released("studio-w5", Some(&w)),
            "released workspace studio-w5: its hold ended, but a job is queued or running in it, so it is not removed"
        );
    }
}
