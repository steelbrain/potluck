# potluck

potluck runs build and test jobs on a handful of Linux and macOS machines owned by one person or
team. Every machine runs the same daemon, and the daemons talk to each other directly. You
dispatch a command from a git repository with the resources it needs. A machine that has those
resources free runs it at the dispatched commit, and the output comes back to the machine you
dispatched from.

It is written to be driven by scripts and coding agents. Every command takes `--json` and none
prompts. A dispatch with `--key` is idempotent. Jobs keep running when the client that dispatched
them exits, and `potluck why` says what a queued job is waiting for and who holds it.

potluck is at version 0.1.0 and has no published releases yet.

## What it leaves to projects

potluck queues jobs, runs them within their RAM and run limits, keeps their output, and keeps a
mirror of each repository on every machine. The rest belongs to the projects that use it:

- Build steps, artifacts and pipelines. A project scripts them around `dispatch`, `wait`, `--keep`
  and `cp`.
- Toolchains. A job runs in the dispatched commit's tree, so `rust-toolchain.toml`,
  `.tool-versions`, `mise.toml` or the `toolchain` line of `go.mod` apply through the machine's own
  rustup, mise or go. potluck reads none of them.
- Build caches. potluck never sets `RUSTC_WRAPPER`, `RUSTFLAGS`, `CARGO_TARGET_DIR`, other `CARGO_*`
  variables or `HOME`. Everything a job starts is killed when the job ends, so a cache daemon
  should run as its own service on each machine.
- Git LFS content. Workspaces hold the pointer files the commit has; a job that needs the content
  fetches it.
- Networking. Members must reach each other's port, over a LAN or a VPN such as Tailscale. potluck
  doesn't configure firewalls, addresses or tunnels. Which machine dispatches, and whether it
  sleeps, is up to you.

## Requirements

- Linux: the systemd user manager, with lingering enabled for your user
  (`sudo loginctl enable-linger $USER`). Without lingering, systemd stops the daemon and its jobs
  when your last session ends, so the daemon refuses to start without it. It also refuses to
  start when the user manager doesn't answer it (`systemctl --user` fails in its environment, for
  example with a wrong `XDG_RUNTIME_DIR`): every job starts through it.
- Linux: cgroup v2, the unified hierarchy, at `/sys/fs/cgroup`, with the memory and pids
  controllers in the user manager's cgroup
  (`/sys/fs/cgroup/user.slice/user-$UID.slice/user@$UID.service/cgroup.controllers` lists them).
  potluck finds a job's processes in its unit's cgroup there, and systemd enforces the job's RAM
  and task limits with those controllers; the daemon refuses to start without them. Machines on
  cgroup v1 or systemd's hybrid layout (the default on Ubuntu 20.04 and Debian 10) need
  `systemd.unified_cgroup_hierarchy=1` on the kernel command line.
- A state directory on a filesystem that can clone files: APFS on macOS, btrfs or XFS with reflink
  on Linux. Workspaces are copy-on-write clones; the daemon clones a test file there and refuses to
  start if that fails. The state directory is `~/.local/state/potluck`, or `$POTLUCK_HOME`.
- `git` 2.31 or newer. The daemon refuses to start when the first `git` on its PATH is older, or
  when there is none.
- In a cluster: one TCP port per machine that the other members can reach (7465 by default), and
  the same potluck version on every member.

## Install

Once a release is published, this installs the latest one, or a given version:

```sh
curl -fsSL https://github.com/steelbrain/potluck/releases/latest/download/install.sh | sh
curl -fsSL https://github.com/steelbrain/potluck/releases/latest/download/install.sh | sh -s -- 0.2.0
```

`install.sh` picks the binary for the OS and architecture: macOS arm64 or x86_64, or Linux x86_64
or arm64 (static, musl). On Apple silicon it installs the arm64 binary even from a shell running
under Rosetta. It refuses a binary whose SHA-256 doesn't match the release's `SHA256SUMS`, removes
the `com.apple.quarantine` attribute on macOS, checks that the binary runs and reports the version
asked for or the latest release's tag (from a `POTLUCK_RELEASES_URL` directory with no version
asked for, any version), and renames it over `~/.local/bin/potluck` (or
`$POTLUCK_INSTALL_DIR/potluck`; when that is a symlink, over the file it leads to). A potluck
daemon running here, as the service or in the foreground, restarts in place on the new binary
(`potluck restart`, see [Versions and updates](#versions-and-updates)); the service's daemon from
before `potluck restart` gets a service restart instead. A daemon running in the foreground from a
potluck at another path is left as it is, and install.sh fails, naming both binaries. With
`POTLUCK_NO_RESTART=1` set it leaves the daemon alone and prints the `potluck restart` command for
the service. When the service runs a potluck at another path, it leaves the service alone and
prints the `potluck service install` command that points it at the new one. The script never
uses sudo and never prompts. When no service is installed yet, it prints the next steps: the
`loginctl enable-linger` command on Linux if lingering is off, `potluck service install` and
`potluck pair`. `potluck self-update` replaces the binary and restarts the daemon in the same way
(see [Versions and updates](#versions-and-updates)), but doesn't print the next steps.
`POTLUCK_RELEASES_URL` makes either download from a directory holding one release's binaries and
`SHA256SUMS` instead of GitHub.

Until then, build from source. Stable Rust works:

```sh
cargo install --locked --git https://github.com/steelbrain/potluck
```

In a clone, `cargo build --release` uses the nightly pinned in `rust-toolchain.toml`, with
`-Zthreads=8` from `.cargo/config.toml`; `RUSTFLAGS= cargo +stable build --release` uses stable.
The crate isn't on crates.io yet (`publish = false` in `Cargo.toml`).

A machine keeps the name its daemon first runs under: its short hostname, unless `name` is set in
`~/.config/potluck/node.toml` (see [Configuration](#configuration)). Names must be unique in a
cluster, so set `name` before `potluck service install` if another machine has the same short
hostname. The daemon refuses to start under a name it didn't start with, and says how to go on.

Then run the daemon as a service with `potluck service install`. On Linux it writes and starts the
systemd user unit `~/.config/systemd/user/potluck.service`; on macOS the LaunchAgent
`~/Library/LaunchAgents/potluck.daemon.plist`, with `AbandonProcessGroup` so that stopping the
daemon doesn't stop its jobs. The service runs the executable you ran `service install` with,
symlinks resolved, and gets `POTLUCK_HOME` and `POTLUCK_CONFIG` if they are set. Before it writes
anything, `service install` checks what the daemon would refuse to start without: a node config that
parses with values in bounds and a `listen` that is an IP address, a usable node name that matches
the one the machine started with, lingering and a user manager that answers on Linux, and a state
directory that can clone files. It also refuses while a daemon runs for that state directory outside
the service, such as a `potluck daemon` in a terminal. The daemon logs to `daemon.log` in the state
directory, each line starting with the local time, and on Linux also to the journal
(`journalctl --user -u potluck`). Past 16 MiB, `daemon.log` moves to `daemon.log.1` (replacing the
one before) and a new one starts. `potluck service uninstall` removes the service, and
`potluck daemon` runs the daemon in the foreground.

## Quick start

On one machine, in a git repository with everything committed:

```console
$ potluck dispatch --timeout 10m -- cargo test
job studio-1
workspace studio-w1
$ potluck wait studio-1
   Compiling app v0.1.0 (/Users/me/.local/state/potluck/workspaces/studio-w1)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.20s
[...]
running 2 tests
test tests::adds ... ok
test tests::adds_zero ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
[...]
potluck: job studio-1 succeeded, exit code 0
potluck: workspace studio-w1 on studio is held for this branch for 4m59s more (commit f988fe3f3570); `potluck release studio-w1` ends it
```

`dispatch` queues the job at the commit HEAD points to and prints its id, and its workspace once
it has started. `wait` streams the output from the start and exits with the job's exit code.
`potluck logs studio-1` prints the output so far, and `--follow` follows it. The last line above
is a reminder, which every potluck command run on this branch prints on stderr while one of its
workspaces is held (see [Workspaces](#workspaces)).

A queued job says what it waits for:

```console
$ potluck dispatch --timeout 30m --ram 40 -- sleep 900
job studio-2
workspace studio-w2
$ potluck dispatch --timeout 10m --ram 30 -- cargo test --release
job studio-3
$ potluck why studio-3
job studio-3 is queued
queued for 1s at priority 3
asks for 30 GiB RAM; run limit 10m
waits on RAM: needs 30 GiB, 17.6 GiB of 57.6 GiB free on studio
job studio-2 holds 40 GiB there for 1s of its 30m run limit, under 1% CPU for 1s
first in line on studio: expected to start within 29m58s (when the jobs holding what it needs reach their run limits)
$ potluck status
machines
NODE    UP   RAM RESERVED        GPUS  LEASED  EXCLUSIVE JOB
studio  yes  40 GiB of 57.6 GiB  1     -       -

running
JOB       STATE    NODE    WORKSPACE  TIME       RAM                GPUS  COMMAND
studio-2  running  studio  studio-w2  1s of 30m  2.9 MiB of 40 GiB  -     sleep 900

queued, in start order
JOB       PRIORITY  WAITING  NEEDS   ON  COMMAND
studio-3  3         1s       30 GiB  -   cargo test --release

recent
JOB       STATE      EXIT  ENDED   NODE    COMMAND
studio-1  succeeded  0     1s ago  studio  cargo test

workspaces
WORKSPACE  NODE    STATE    HELD   SIZE     PENDING  BRANCH  COMMIT        REPO
studio-w1  studio  held     4m58s  1.6 MiB  0        main    f988fe3f3570  app-7c27480694ae7212
studio-w2  studio  running  -      -        1        main    f988fe3f3570  app-7c27480694ae7212
```

These examples, and the ones below, ran on a Mac named `studio`. Cargo's output is shortened,
most reminder lines are left out, and paths are shown as if the repository were
`/Users/me/src/app` and the state directory the default `/Users/me/.local/state/potluck`.

In a script, `--json` prints one JSON document on stdout:

```sh
job=$(potluck dispatch --json --timeout 10m -- cargo test | jq -r .job)
potluck wait "$job"
```

## Jobs

### Dispatching

A job runs a command, given after `--` or by a [recipe](#recipes), at the commit HEAD points to.
The command runs in the workspace's root as an argument list, not through a shell; use
`-- sh -c '...'` for shell syntax. Only committed work runs:

```console
$ potluck dispatch --timeout 10m -- cargo test
potluck: refusing to dispatch from /Users/me/src/app: only committed work runs, and this isn't committed:
   M src/lib.rs
Commit it (or add untracked files to .gitignore), check submodules out at the commits recorded, and dispatch again.
```

A file that `git status` shows as modified only because the repository's attributes or filters
would store it differently (a CRLF file committed before `*.txt text` was added, for one) is not
refused when it holds the committed bytes or what a checkout writes for them. Its workspaces
hold what a checkout writes, and `git status` there shows it modified too.

A dispatch that no machine could run is refused at once, with each machine's reason, and nothing
is queued. Only machines that are up count; capable machines that aren't up are named in the
refusal. A member on another potluck version that answers counts as up: one of the two is
updating, and a job only it can run is queued (see [Lost machines](#lost-machines)).

```console
$ potluck dispatch --timeout 10m --ram 100 -- cargo test
potluck: no machine could ever run this job (studio: needs 100 GiB RAM, its capacity is 57.6 GiB)
```

### Run limit

`--timeout` is required, on the command line or in the recipe. Durations need units: `600s`, `5m`,
`2h5m10s`; a bare `600` is refused. At the limit every process of the job gets SIGTERM, then
SIGKILL after `kill_grace_s` (10 seconds) of the machine being awake: time asleep gives the job no
time to shut down. Processes that can't die don't keep the job's resources: those are released
anyway, and `status` and `why` list the stuck pids.

A job that uses under 1% of one CPU core and writes no output for `stall_minutes` (10) is marked
stalled, and the stacks of its processes go into its log. `--kill-on-stall` ends it then. A thread
that wakes now and then to poll doesn't keep a job from stalling. Time the machine spends asleep
doesn't count toward a stall; it does count toward the run limit.

### Resources

A job gets everything it asks for at once, or waits, so jobs can't deadlock on each other.

- `--ram G`: the job's reservation and its hard limit, in GiB (`4`, `4G`, `512M`; default 4).
  Reservations on a machine never add up to more than its `ram_gib`. Nothing keeps other programs
  on the machine from using the RAM a job reserved: `ram_gib` is what leaves room for them. (On
  Linux, `MemoryLow` or `MemoryMin` on the job's unit would protect nothing: the kernel honours
  them only as far as every slice above the unit has them too, and systemd sets none on
  `user.slice` or a user's manager unless root configures it.) On Linux the limit is the job's
  systemd unit's `MemoryMax`, with `MemorySwapMax=0`; on macOS potluck sums the memory
  footprint of the job's processes and kills them over the limit. The job then ends as
  "over memory: used X of its Y" (`used 300 MiB of its 256 MiB`, `used 4.2 GiB of its 4 GiB`). On
  Linux, a job whose process the kernel's OOM killer or systemd-oomd ends while the machine runs
  short of memory, with the job under its own limit, fails as "killed because the machine ran
  short of memory, under its own limit: used X of its Y".
- `--cpu N`: a weight in cores (default 1) that never keeps a job from starting. On Linux it sets
  the unit's `CPUWeight` to 100 per core; on macOS it has no effect.
- `--gpu SEL`, repeatable: one GPU each, held only by this job until it ends. A selector is `any`,
  a vendor (`nvidia`, `amd`, `intel`, `apple`, or a PCI vendor id such as `10de`), `vendor:device`
  in hex (`10de:2c19`), a GPU id from `potluck nodes` (`10de:2c19@0000:01:00.0` on Linux, `mac0`
  on macOS), or an alias from the machine's config. The job finds its GPU ids in `POTLUCK_GPUS`.
- `--exclusive`: the job starts only when nothing else runs on the machine, nothing starts beside
  it, and it holds every GPU there. Nothing is paused or killed to make room. While it is the first
  job in line waiting on that machine, the machine starts only jobs that will end, by their run
  limits, before it expects to be empty.

A job may have at most 16384 processes. On Linux that is its unit's `TasksMax`, which counts
threads too, and a fork past it fails. On macOS it counts processes, potluck's supervisor
included, and is at most half of `kern.maxprocperuid`, the most processes one user may have
(`sysctl kern.maxprocperuid`). potluck counts them every half second and ends a job that has more
as failed, killed by SIGKILL (exit code 137): "too many processes: 5334, over its limit of 5333
(half of kern.maxprocperuid, the most processes one user may have)". Between counts, the job's
RLIMIT_NPROC holds it back: it starts at the user's process count plus the job's limit. macOS
counts that limit per user, not per job, so other processes of the same user, other jobs
included, take from the job's room while it runs: a job can be refused forks (EAGAIN) below its
own limit when the user's other processes grow.

A job's limits on open files are the daemon's. At start the daemon raises its soft limit to its
hard limit (on macOS at most `kern.maxfilesperproc`), and a job gets both: on macOS by
inheriting them, on Linux as its unit's `LimitNOFILE`, in place of the user manager's default
soft limit of 1024.

Processes a job hands to another service, such as containers run by `dockerd`, are outside the
job: they are neither counted nor killed. On Linux a process of the job can also leave the job's
systemd unit, by moving itself into a cgroup of its own under the user's systemd or by starting a
unit of its own with `systemd-run --user`, and is then neither counted nor killed either. On macOS
potluck finds a job's processes in snapshots taken every half second. A process that leaves the
job's session and loses its parent between two snapshots, as a daemon started by a double fork
does, still counts as the job's if it was started with the job's `POTLUCK_LEASE` in its
environment, a token potluck makes for each run of a job. That is best effort: macOS doesn't show
the environment of its own programs, such as those in `/bin` and `/usr/bin`, so a daemon that is
one of those programs, or one started with a cleared environment (`env -i`), is neither counted
nor killed.

### Where it runs

`--on`, repeatable, names a fact the machine must have, and every one must hold. Facts aren't
reserved.

- `--on macos` or `--on linux`: that OS, any version. Only these two stand without a value.
- `--on macos=27`: macOS 27.x. `--on linux=7.2`: Linux with kernel 7.2.x.
- `--on KEY=VALUE`, for these facts:

| Key | Matches |
| --- | --- |
| `os` | `linux` or `macos`, case ignored (`macOS`) |
| `os_version` | the macOS version (`27.0.1`), or the Linux distribution as os-release names it (`Ubuntu 24.04`) |
| `kernel` | `uname -r` on Linux (`7.2.8-2-cachyos`), the Darwin version on macOS |
| `arch` | `x86_64` or `aarch64`, case ignored; `arm64` is `aarch64` and `amd64` is `x86_64` |
| `gpu` | a GPU selector, as for `--gpu`, without leasing the GPU |
| `gpu_driver` | a GPU whose kernel driver has that name (`nvidia`, `amdgpu`, `i915`; Linux only) |
| `ram_gib` | at least that much RAM (`32`, `15.5`) |
| `cpus`, `cpu_model`, `node` | exactly |
| `display` | `true` when a graphical session is up for the user |

Versions match by leading parts. A value is split into parts at `.`, `-`, `_`, `+` and spaces, case
is ignored, and a requirement matches when its parts are the first parts of the machine's value:
`7.2` matches `7.2.8-2-cachyos` and not `7.20.1`, and `ubuntu-24` matches `Ubuntu 24.04`.
Requirements that can't hold together, such as `--on macos --on linux`, are refused before anything
is queued, and so is a key given two values (`--on gpu=nvidia --on gpu=amd`). A refusal names what
each machine has, and `potluck nodes` lists every machine's facts:

```console
$ potluck dispatch --timeout 10m --on linux=7.2 -- cargo test
potluck: no machine could ever run this job (studio: needs linux=7.2 (this machine: macOS 27.0.1))
```

### Order

Jobs start by priority, `--priority 1` to `5` (5 highest, default 3), then by submit time. Each
time a job is passed over, meaning a job submitted after it is granted something it was waiting
for, its priority rises by one, up to 4, so priority-5 jobs still go first. While the first job in
line waits, later jobs may start if they will end, by their run limits, before its resources are
expected free, or if they don't touch those resources. Running jobs are never stopped for
priority.

### Keys, kept files and labels

- `--key K`: if the dispatching machine already has a job with this key, the dispatch returns that
  job, whatever its state, instead of queuing another. A key stays taken as long as its job's
  record is kept (`retention_days`, 14).
- `--keep GLOB`, repeatable: files matching the glob, relative to the workspace, are copied to the
  dispatching machine when the job ends, into `jobs/<job>/keep/` in its state directory. A glob
  that is absolute, holds `..` or doesn't parse is refused at dispatch. Symlinks
  and file names follow the rules of `potluck cp` (below). The output log is always kept there
  too, as `jobs/<job>/output.log`. Both are deleted after `retention_days`.
- `--label K=V`, repeatable: stored with the job.

Crash reports of a job's processes go to `jobs/<job>/crash/` in the dispatching machine's state
directory, and are deleted with the job's other files after `retention_days`. On macOS these are
the `.ips` files that ReportCrash writes to `~/Library/Logs/DiagnosticReports`, moved from there
when the crashed process, or its parent, was one of the job's. On Linux, when systemd-coredump
recorded core dumps of the job's unit, the output of `coredumpctl info` for them is saved there as
`coredumps.txt`; the dumps themselves stay where systemd-coredump keeps them.

### Output

A job's stdout and stderr go through one pipe into its output log, `jobs/<job>/output.log` in
the state directory of the machine that runs it, which streams it to the dispatching machine as
it grows; stdin is `/dev/null`. `> /dev/stdout`, `2>/dev/stdout` and `tee /dev/stdout` in a job
write to the pipe and never cut the log short. When the command exits, potluck reads
what is left in the pipe before it records the exit status, so `wait` and `logs` end with all of
it. Processes the command left running are read for at most one more second, while they keep
writing; then potluck records the status and ends them.

A log keeps the first `log_max_mib` MiB of a job's output (1024 unless the config of the machine
running the job says otherwise). Then a line says so, and the rest is dropped except its last
16 MiB, which come at the end after a line saying how many bytes were dropped. The job runs on,
and what it writes still counts as output for stall checks. potluck holds those 16 MiB in memory,
counted in the job's RAM, and writes them out early when the job gets SIGTERM (at its run limit,
or on `cancel`), so they outlast a SIGKILL after it. A job killed outright with SIGKILL, as one
over its RAM limit can be, loses them.

`logs --json` writes its document as the output arrives, so a log of any size takes little
memory.

### How a job ends

`potluck wait` exits with the job's own exit code, or 128 + N when signal N killed it. Endings
potluck causes have fixed codes:

| Code | Ending |
| --- | --- |
| 124 | killed at its run limit, or stalled with `--kill-on-stall` |
| 125 | its machine was lost and it wasn't queued again, or every machine that could run it was lost (see [Lost machines](#lost-machines)); or the daemon of the machine running it lost track of its processes: after a daemon restart nothing recorded them or how the job ended, or 20 samples of them in a row failed |
| 130 | cancelled with `potluck cancel` |
| 137 | over its RAM limit; also, as 128 + SIGKILL, a job killed on Linux because the machine ran short of memory, or on macOS for having more processes than its limit |
| 127, 126 | the command wasn't found, or couldn't be started |
| 1 | potluck couldn't prepare the workspace or start the job, the workspace a queued job was pinned to is gone, or the job's processes ended without recording an exit status; `why` says which |

```console
$ potluck dispatch --timeout 1m --ram 64M -- python3 -c 'b = b"x" * (300 << 20); import time; time.sleep(30)'
job studio-4
workspace studio-w3
$ potluck wait studio-4
potluck: job studio-4 over memory: used 310 MiB of its 64 MiB, exit code 137
```

### Environment

A job's environment is built in this order, later entries winning:

1. the user's login environment, which the daemon captures from a login shell (`$SHELL -l`) when it
   starts. A login shell doesn't read `.zshrc` or `.bashrc`, so what jobs need belongs in a login
   profile, and the daemon sees a change after a restart. A variable whose name or value isn't
   UTF-8 is left out, and `daemon.log` names it;
2. the `env` of each `[[gpu]]` entry of the machine's config whose GPU the job holds;
3. `[env]` of the `potluck.toml` committed at the dispatched commit, then the recipe's `env`;
4. `--env K=V`;
5. `POTLUCK_JOB`, `POTLUCK_WORKSPACE`, `POTLUCK_NODE`, `POTLUCK_GPUS`, `POTLUCK_LEASE`,
   `POTLUCK_HOME`, `POTLUCK_CONFIG`, and `TMPDIR`, a directory beside the workspace that is removed
   with it.

Nothing comes from the shell that ran `dispatch`.

### Dispatching from inside a job

`potluck dispatch` inside a job doesn't queue anything. If the request fits within the job's own
lease (no more RAM, each `--gpu` matched by a different GPU the job holds, `--exclusive` only in an
exclusive job, `--on` facts this machine has, `--workspace` only the job's own, and a `--timeout` no
longer than the time left before the job's own run limit), the command runs in place, in the
foreground, at the top of the repository (where a dispatch from outside would run it), and
`dispatch` exits with its code. It runs in a process group of its own. At its `--timeout`, every
process in that group gets SIGTERM, then SIGKILL after `kill_grace_s`, and `dispatch` exits
with 124. A process that left the group (with `setsid`, for example) is not stopped then; it ends
with the job, as do processes the command leaves running when it exits. Its environment is the job's
with the request's env on top, except potluck's own variables, which stay the job's. A request that
doesn't fit fails at once with exit code 2, and so does one with `--keep`, `--hold`, `--key`,
`--label`, `--no-rerun` or `--kill-on-stall`, which only mean something for a job of its own; a
recipe's `keep` and `hold` are left out, and `dispatch` says so. It never waits for the job's own
resources, so a job can't deadlock on itself; for the same reason `potluck wait` and
`potluck logs -f` of the job's own id fail at once inside it, with exit code 2. The job runs the
`potluck` its `PATH` finds, and that `PATH` comes from the login environment, so potluck's directory
must be on the `PATH` a login shell sets; otherwise the job's shell says
`potluck: command not found` and exits 127. From a job holding 4 GiB:

```console
$ potluck dispatch --timeout 1m -- echo inner
potluck: inside job studio-5: running in place, within its lease (not dispatched)
inner
$ potluck dispatch --timeout 1m --ram 16 -- echo inner
potluck: can't run inside job studio-5: it asks for 16384 MiB of RAM and the job holds 4096 MiB. A dispatch from inside a job runs in place only when it fits the job's own lease, and never waits for it
```

## Workspaces

A workspace is one commit's tree on one machine, with an id like `studio-w5`. Each job gets a new
one when it starts, unless it names one with `--workspace`.

- **Seeding.** A workspace is a detached `git worktree` of the machine's mirror of the repository,
  with each submodule a worktree of its own mirror. Its files are copy-on-write clones of a base
  checkout, and git writes only the files that differ. Before each job, HEAD and each submodule's
  HEAD are checked against the dispatched commits.
- **Reuse.** `--workspace W` runs the job in W, on W's machine, after the jobs already running or
  queued there. W must be at the same commit. If W or its machine is gone, the dispatch fails at
  once.
- **Held.** After each job a workspace is held for at least 5 minutes. `--hold DUR` holds it DUR
  after the job ends, up to 24 hours. `potluck hold W DUR` makes the hold end DUR from now, at most
  24 hours, and can be repeated. A hold belongs to the branch whose job or `potluck hold` set when
  it ends. While a workspace's hold lasts, also while a job runs in it, every potluck command run
  in its repository on that branch prints a reminder on stderr (`reminders` with `--json`).
- **Idle.** After the hold the reminders stop, also while jobs are queued into the workspace. The
  workspace stays on disk, where a build cache may reuse its build output, and `--workspace` still
  works while it exists.
- **Removed.** On each machine the newest idle workspace of each repository is always kept. Older
  idle ones are removed, oldest first, while all workspaces together exceed the disk budget:
  `workspace_budget_percent` of the disk (10), but at least `workspace_budget_min_gib` (50).
  A workspace that is held, or has a job queued or running, is never removed.

`potluck release W` ends the hold and removes the workspace at once; if a job is queued or running
in it, only the hold ends, and the job running in it then doesn't hold it again when it ends.
`potluck cp W:PATH DEST` copies a file or directory out of a workspace, from any member, with
permission bits (setuid, setgid and sticky bits are dropped). A symlink to a file inside the
workspace is copied as that file; other symlinks are skipped, and so are files whose names aren't
valid UTF-8. A build, then tests in the same tree:

```console
$ potluck dispatch --timeout 10m --hold 1h -- cargo build --tests
job studio-6
workspace studio-w5
$ potluck wait -q studio-6
potluck: job studio-6 succeeded, exit code 0
potluck: workspace studio-w5 on studio is held for this branch for 59m59s more (commit f988fe3f3570); `potluck release studio-w5` ends it
$ potluck dispatch --timeout 5m --workspace studio-w5 -- cargo test
job studio-7
workspace studio-w5
$ potluck wait -q studio-7
potluck: job studio-7 succeeded, exit code 0
$ potluck cp studio-w5:Cargo.lock /tmp/
copied 1 file (147 B) from studio-w5:Cargo.lock to /tmp/Cargo.lock
$ potluck hold studio-w5 3h
workspace studio-w5 is held for 3h from now (`potluck release studio-w5` ends it sooner)
$ potluck release studio-w5
released workspace studio-w5
```

## Recipes

A `potluck.toml` at the repository root names commands with their options. potluck reads it from
the dispatched commit, never from the working tree.

```toml
[env]
RUST_BACKTRACE = "1"

[recipe.gpu-tests]
cmd = ["cargo", "test", "--features", "gpu"]
timeout = "20m"
priority = 4
ram = "16G"
cpu = 4
gpu = ["nvidia"]
on = { os = "linux" }
hold = "30m"
keep = ["target/gpu-report.json"]
env = { GPU_TESTS = "1" }
```

```console
$ potluck dispatch gpu-tests
$ potluck dispatch gpu-tests --timeout 1h -- cargo test --features gpu -- render
```

A recipe's fields are `cmd` (required), `timeout`, `priority`, `ram`, `cpu`, `gpu`, `exclusive`,
`on`, `hold`, `keep` and `env`. Unknown fields are refused, so a misspelled option can't be dropped
silently. `on` is a table, as above, or a list in `--on`'s form: `on = ["macos=27", "gpu=apple"]`.
Options on the command line replace the recipe's; `on` and `env` merge key by key, the command line
last, with `os`, `macos` and `linux` counting as one key, so `--on macos` replaces a recipe's
`os = "linux"`. A recipe's `exclusive = true` can't be turned off from the command line. `[env]`
applies to every job dispatched from the repository.

## A cluster

### Pairing

A machine joins once, with the daemon running on it and on any member, and a shell on both. On the
new machine:

```console
mini$ potluck pair
potluck-pair-request.eyJ2IjoxLCJuYW1lIjoibWluaSIsImtleSI6ImNZMnc0S2NWemNNR0xrRkVEOG1xdXJiamwzai0xcjhMOVQw...
potluck: paste this line into `potluck admit` on a member of the cluster (a machine in no cluster founds one with it), then the reply into `potluck pair --finish` here
```

On any member, `admit` takes the request and prints a reply:

```console
studio$ potluck admit 'potluck-pair-request.eyJ2IjoxLCJuYW1lIjoibWluaSIs...'
potluck-pair-reply.eyJ2IjoxLCJjbHVzdGVyIjoic3MzU2w4cHN1MHdlYmJtZF9VdmVZSXlUREpyejZoM0lBZWtWa3ZpdXNxMCIs...
potluck: paste this line into `potluck pair --finish` on the new machine
```

Back on the new machine:

```console
mini$ potluck pair --finish 'potluck-pair-reply.eyJ2IjoxLCJjbHVzdGVyIjoi...'
joined the cluster: mini, studio
```

Each text is one line, shortened here. The request holds the new machine's name, public key,
address, potluck version and a nonce; `admit` refuses a request from another version. The reply
holds the member's signed admission of that key, the membership records that make the members
members, and the members' addresses; the new machine takes the rest of the records from the
members. A machine's name is the one its daemon first ran under (see
[Install](#install)). Names must be unique, and are letters, digits, `.`, `_` and `-`, not starting
with `-`. A machine that isn't in a cluster founds one with its first `admit`. A daemon listens on
its port only while its machine is in a cluster with other members: from its first `admit` or from
`pair --finish`, until it is removed or its admission is voided, or 30 seconds after the removal of
every other member.

The admitting member announces the admission, and every member accepts a key that a member
signed, so joining one member joins the cluster. Members compare their records on every heartbeat
and exchange them when they differ, so a member that was offline catches up when it returns. A
member back from being away (its daemon restarted, or it heard from no member for 30 seconds)
takes jobs from another member, or hands jobs to one, only once it has compared records with every
other member, waiting up to 30 seconds for those it doesn't hear from; so it learns of a removal
made while it was away before it deals with the removed machine. Meanwhile `why` says so for a job
that machine could start (a `catch_up` blocker).

`potluck remove NODE`, on any other member, removes a machine. The removal is announced the same
way, and the machine's key is refused everywhere after. The removed machine runs only its own jobs
from then on and says so in `status`; `potluck pair` there makes a new key to join again.

Wherever you run it, the removal is signed by the oldest member that is up and was admitted before
NODE, the founder first, and such a removal is for good. When removals cross, the one signed by the
member admitted earlier counts (see [Security](#security)), so a removal signed by a member
admitted after NODE holds only until NODE removes that member in turn. When no member admitted
before NODE is up, or none that is up can sign it (it can't save its records, say), the machine
you ran it on signs the removal and says that NODE can undo it, and why those members didn't sign
it (with `--json`, `"final": false` and a `warning`); run it again once such a member can sign it
to make it final.

No member was admitted before the founder, so a removal of the founder is never final: the
founder's key can undo it. If the founder's machine is lost for good, its removal holds. If its key
may be used again (the machine stolen, or its disk copied), start a new cluster: on each machine,
stop the daemon, delete `keys/` in the state directory and start the daemon again, which gives the
machine a new key and a cluster of its own; then pair the machines with one of them.
A removal doesn't count when its signer was itself removed by a member that hadn't heard of it; a
machine that heard of such a removal first keeps asking the members it knew, and is a member again,
taking work, once it hears of the other.

A cluster has at most 2048 members: `admit` refuses another until one is removed. When two members
admit a machine each into the last place at once, one admission counts on every member once they
exchange records (the lower logical clock, then the lower record id), and the other machine is not
a member. Members keep every record that counts for good, removed machines' included. They also
keep and pass on records that count nowhere for now, such as the admission that lost the last
place, so that members that reach each other only through another member end with the same
records. They turn away the records a removal voids, and drop those they took before when the
daemon next starts.

### Sources

The dispatching machine's repository is the source of truth: its commits may exist nowhere else, and
the other members need no access to its remotes. A member keeps a bare mirror of a repository and of
each of its submodule repositories from the first time a job from it is queued that the member could
run (the dispatching machine, at dispatch). Mirrors are fed from the dispatching machine over the
cluster's connections with git's own protocol, so only the objects a mirror lacks travel. Mirrors
are exact copies of the source's branches and tags, force-updates and deletions included. A job that
runs git in its workspace writes the mirror's branches and tags (a worktree shares them): a branch
or tag that changed since the last sync is reset to the source's at the next sync. A mirror that
changed that way, or that fails its checks and is cloned again from the source, is reported in the
daemon's log and in the log and `why` of the job the sync was for:

```console
--- potluck: warning: mini: the mirror of app-7c27480694ae7212 had changed since its last sync, so something other than potluck wrote to it (a job running git in its workspace, for one), and was reset to its source's branches and tags: refs/heads/scratch was made here, at f988fe3f3570 ---
```

Repository ids are salted with the dispatching machine's name, so two machines whose repositories
have the same path never share a mirror on a third.

A dispatch pins its commits with refs under `refs/potluck/` until its workspace is removed, so
amending or deleting a branch can't let `git gc` drop them. A machine takes a job only once its
mirrors hold every commit the job names, and nothing is fetched at run time. A mirror still
syncing shows in `why` as a `sync` blocker.

### Queue and claims

Each machine keeps the queue of the jobs dispatched on it. Members exchange queues about once a
second and whenever one changes, so every member sees every queue. Each machine plans for itself
and claims a job from its dispatching machine only when it can start it now. The dispatching
machine grants each job once; a machine whose claim is refused plans again. A machine with no
workspace of the job's repository leaves the job for 3 seconds when a member with one could start
it by then; `why` says so meanwhile.

The machine running a job streams its output to the dispatching machine and sends the kept files
at the end, so `wait` and `logs` work there as on one machine. Kept files that can't be delivered
for 10 minutes are given up on, and at once if their machine leaves the cluster: the job ends with
its own exit code and a note saying so (`why` shows it), and the files stay in the job's directory
on the machine that ran it. `cancel`, `cp`, `hold` and `release` are forwarded to the machine
concerned, and `status`, `nodes` and `why` show every member.

### Lost machines

Members exchange heartbeats every 3 seconds, and a member silent for 30 seconds, or removed, is
lost. A job running there is queued again, in a new workspace, if a machine that is up can run it;
its output so far is kept, with a line saying so. It ends as lost, exit code 125, if no machine that
is up can run it, if it was dispatched with `--no-rerun`, or if it was pinned with `--workspace`. A
queued job whose every capable machine is lost also ends with 125, and so do queued jobs pinned to
the lost machine's workspaces. A queued job keeps waiting while a capable member has been silent for
less than 30 seconds. A member on another potluck version doesn't count as up, but while it answers
it isn't lost either, for up to 10 minutes from its first answer on that version (one of the two is
updating): `nodes` and `status` show it as updating, its jobs keep running there, and jobs only it
can run, queued before or dispatched meanwhile (into one of its workspaces too), wait for the
members to run one version, as `why` says. After that it is lost like a silent one. When a lost
machine returns, it stops the jobs that are no longer its. If the dispatching machine is the one
away because its daemon stopped or the machine slept, its jobs elsewhere keep running and their
reports wait for it. A dispatching machine that stays up but hears from no member for 30 seconds
(its network down, say) counts the members lost: its jobs on them are queued again or end with 125
as above, and those members stop them once they hear from it again.

Here `mini` stopped just before the first dispatch, while `studio` still counted it as up:

```console
$ potluck dispatch --timeout 10m --on node=mini -- cargo test
job studio-10
$ potluck wait studio-10
potluck: job studio-10 lost: no machine that can run it is up (mini was lost), exit code 125
$ potluck dispatch --timeout 10m --on node=mini -- cargo test
potluck: no machine that can run this is up (mini: down since 15:01; studio: needs node=mini (this machine: node=studio))
```

### Versions and updates

Members talk only to members running the same version. A member that meets a newer one updates
itself to that version from the GitHub releases, at most once per version, retrying with backoff
from 1 minute up to an hour if that fails; versions only go up. `potluck self-update` has this
machine's daemon update itself to the latest release and restart in place, and the other members
follow when they meet the new version. Until they all run it, a member on the other version shows
as updating, not down: its running jobs go on, a job only it can run is queued for it, and `wait`
and `logs` of a job it owns wait for it. The command returns before the update starts; how it went
goes to `daemon.log` in the state directory, which the command names. During a restart in place
the command fails, saying to run it again once the restart is done. `potluck self-update --here`
does the update in the command's own process, without asking the daemon, so it works while no
daemon runs: it replaces the potluck you ran and asks the daemon to restart in place (a daemon from
before `potluck restart` gets a service restart), and the other members follow in the same way.
An update checks the release's SHA-256 and replaces the binary it runs from (the daemon's, or with
`--here` the one you ran), even one built from source. Running jobs survive any daemon restart:
the new daemon finds them again, and after a plain restart `wait` and `logs` connect to it again.
A daemon still running after its binary was replaced (with `POTLUCK_NO_RESTART=1`, or by a package
manager) starts each job's supervisor and git's tunnel from a copy of the binary it started from,
`bin/potluck` in the state directory, not from the new one.

A restart in place (`potluck restart`, or the daemon after an update) runs the potluck binary at
the daemon's path in the daemon's own process, so its pid stays the same and systemd or launchd
sees no restart. The daemon first checks that the new binary runs and can take over. Then no job
starts until work already going (a workspace being seeded, a mirror sync) has finished, for at most
a minute; a job dispatched meanwhile is queued and starts after the restart. Then new connections
wait in the kernel's backlog instead of being refused, and the daemon hands the new binary its
socket, its cluster port, its open connections and its view of the members. Running jobs keep
running, `wait` and `logs -f` go on over the same connection without losing or repeating output,
and the other members don't see the machine go down. `potluck restart` prints the new binary's
answer, which says how long new connections waited. The new binary reads node.toml as a plain
start does, so `potluck restart` applies changes to it. A new binary that doesn't run, or that
wouldn't start with node.toml as it now is (a key given twice, another name for the machine),
leaves the daemon serving on the old one, and `potluck restart` says why. One that runs but can't
take over (a potluck from before `restart`) gets a plain restart: open connections close, running
jobs keep running, commands run while the new daemon starts wait for it, and `potluck restart` says
it restarted plainly and why, and exits 0 once the daemon answers again. While the state directory
can't be written (a full disk), the restart waits, and the daemon tries again every 10 seconds. A
restart asked by another potluck binary than the daemon's, while the one at the daemon's path is
still the binary it runs, would run that one again unchanged: the daemon refuses it, names both
binaries, and serves on as it was (`potluck restart` exits 2, and install.sh and `self-update
--here` fail with that).

### Security

- Membership is full trust: any member can run commands on any other, as the user who runs potluck
  there, and read its workspaces.
- Each machine has an ed25519 key pair, in `keys/node.key` in the state directory (mode 0600).
  Members connect over TLS 1.3, both sides authenticated by the other's pinned public key, with no
  certificate authorities or host names involved. Control messages, git objects, logs and file
  copies all travel over these connections, encrypted and authenticated.
- The pairing request and reply are neither secret nor encrypted. Pairing relies on you carrying
  them between your own shells: a key you `admit` becomes a member, and the reply you give
  `pair --finish` decides which cluster the new machine joins.
- A removal lists the removed key's records that counted for the remover when it signed it. Every
  other record signed by that key is void, wherever it turns up and whatever date or logical
  clock it claims, so backdating doesn't help: a removed machine can't sign its way back in or
  admit others, and members turn its later records away. A record the remover hadn't seen is lost
  even if honest, so a machine that the removed one admitted just before its removal may need
  admitting again.
- Members keep and pass on every record a member signs, also one that counts nowhere, so a member
  can fill the others' disks with records while it is a member, as it can with jobs. Once it is
  removed, its records that its removal doesn't list, and the records of machines whose only
  admission was one of those, are turned away; a member that took them before it heard of the
  removal drops them when its daemon next starts.
- When removals cross (A removes B while B removes A), the removal by the member admitted earlier
  counts, so the founder's removal wins any such race. A member that heard of the other removal
  first learns of it when the winner next connects: before it refuses a machine as removed, it
  takes the records that count for that machine, and refuses it only if it is still removed with
  them. This settles races between members acting in good faith; it is no defence against a
  hostile member, which can run `potluck remove` on any other member through a job there.
- At most 64 connections, and 8 from one address, may be unauthenticated at once.
- The command line talks to its daemon over a Unix socket, `potluck.sock` in the state directory,
  with mode 0600. The daemon keeps the state directory at mode 0700: it holds job specs with their
  `--env` values, logs, mirrors and workspaces.
- Workspaces, mirrors, logs and kept files are plain files on disk. Release downloads are checked
  against the release's `SHA256SUMS`, which catches a corrupted download but not a tampered release.

## Configuration

Each machine reads `~/.config/potluck/node.toml`, or `$POTLUCK_CONFIG`; the daemon reads it when it
starts. The file is optional, and unknown keys are an error.

| Key | Default | Meaning |
| --- | --- | --- |
| `name` | the short hostname | this machine's name in the cluster, kept from its daemon's first start |
| `port` | `7465` | the port members reach this machine on |
| `listen` | every IPv4 interface | the IP address to listen on, such as `127.0.0.1` or `::` |
| `address` | the hostname and `port` | how members reach this machine: `host:port`, or a host to use with `port` |
| `ram_gib` | total RAM minus 4 GiB or 10%, whichever is more | what RAM reservations may add up to, more than 0 and at most 1048576 |
| `workspace_budget_percent` | `10` | the share of the disk workspaces may use, 0 to 100 |
| `workspace_budget_min_gib` | `50` | the smallest workspace budget, in GiB |
| `kill_grace_s` | `10` | seconds from SIGTERM to SIGKILL, at most 86400 |
| `stall_minutes` | `10` | under 1% of a CPU core and no output this long is a stall, at most 10080; `0` turns stall checks off |
| `retention_days` | `14` | how long job records, output logs and kept files are kept after the job ends, counted from when this machine learned of the end, on its own clock |
| `log_max_mib` | `1024` | how much of a job's output its log keeps before it drops the rest but the last 16 MiB (see [Output](#output)); `0`: no limit |

Each `[[gpu]]` entry names one of the machine's GPUs:

```toml
[[gpu]]
match = "nvidia"              # a GPU selector that names exactly one GPU here
alias = "5080"                # a name for it in --gpu and --on gpu=
env = { MY_GPU = "5080" }     # set in jobs that hold it
```

## Commands

| Command | What it does |
| --- | --- |
| `dispatch [RECIPE] --timeout DUR [OPTIONS] [-- CMD...]` | Queue a job at HEAD; print its id, and its workspace once started. |
| `wait JOB [--max-wait DUR] [-q]` | Stream the output and exit with the job's code; `-q` only waits. |
| `logs JOB [-f]` | Print the output so far, or follow it. |
| `cancel JOB` | Drop a queued job, or stop a running one (SIGTERM, then SIGKILL). |
| `hold W DUR` | Hold workspace W for DUR from now, at most 24h. |
| `release W` | End W's hold and remove it; while a job is queued or running in it, only the hold ends. |
| `cp W:PATH DEST` | Copy a file or directory out of a workspace. |
| `status` | Machines, running jobs, the queue in start order, recent jobs, workspaces. |
| `why JOB` | What a queued job waits on and who holds it, or how a job ended. |
| `nodes` | Every machine with its facts, GPUs, version and address. |
| `pair`, `pair --finish REPLY` | Ask to join a cluster, and finish joining. |
| `admit REQUEST` | Admit a machine and print the reply. |
| `remove NODE` | Remove a machine from the cluster. |
| `self-update [--here]` | Update this machine to the latest release, through the daemon or (`--here`) from this process; the other members follow. |
| `restart` | Restart the daemon in place on the binary at its path, keeping running jobs and open connections. |
| `service install`, `service uninstall` | Install or remove the daemon's service. |
| `daemon` | Run the daemon in the foreground. |

Without `--json`, results go to stdout, and errors and reminders to stderr. With `--json`, every
command prints one JSON document on stdout, errors included
(`{"error": "...", "reminders": [...]}`), and `wait --json` sends the job's output to stderr.
`logs --json` writes its document as the output arrives; an error after it has begun ends the
document with an `"error"` field and exit code 2. Output that can't be written (a full disk) is an
error said on stderr, with exit code 2, also when it is a JSON error document; a pipe closed
early (`potluck logs J | head`) is not an error.
potluck's own errors, refused dispatches included, exit with 2. `wait --max-wait DUR` exits with 3
if the job hasn't ended by then; the job keeps running, and `"ended": false` in the JSON tells this
apart from a job that exited with 3. `potluck restart` exits with 4 when no daemon here can restart
in place: none answers, or it runs a potluck from before `restart` (install.sh then restarts the
service). `restart` never exits with a job's code, so its 4 is always its own.

## Development

[docs/development.md](docs/development.md) describes how to build potluck and run its checks.

## License

MIT, Anees Iqbal. See [LICENSE](LICENSE).
