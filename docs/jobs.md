# Jobs

## Dispatching

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

`--without-submodule PATTERN`, repeatable, leaves out the submodules a job doesn't need. PATTERN
is a submodule's path from the top of the repository, or a glob on such paths in which `*`
doesn't cross a `/`. A submodule left out, and every submodule nested in it, is neither checked
at dispatch nor copied to the machine that runs the job: its directory in the workspace is
empty, and its uncommitted changes, or its being at another commit than recorded, don't count.
`dispatch` names what it left out on stderr (with `--json`, `without_submodules`). A pattern that
matches no submodule of the commit is refused, so a typo can't ship what it meant to leave out;
one that names a submodule inside another left out counts as matched.

```console
$ potluck dispatch --timeout 30m --without-submodule vendor/qemu -- cargo test
job studio-9
potluck: left out 1 submodule and what is nested in it: vendor/qemu
```

A dispatch that no machine could run is refused at once, with each machine's reason, and nothing
is queued. Only machines that are up count; capable machines that aren't up are named in the
refusal. A member on another potluck version that answers counts as up: one of the two is
updating, and a job only it can run is queued (see [Lost machines](cluster.md#lost-machines)).

```console
$ potluck dispatch --timeout 10m --ram 100 -- cargo test
potluck: no machine could ever run this job (studio: needs 100 GiB RAM, its capacity is 57.6 GiB)
```

## Run limit

`--timeout` is required, on the command line or in the recipe. Durations need units: `600s`, `5m`,
`2h5m10s`; a bare `600` is refused. At the limit every process of the job gets SIGTERM, then
SIGKILL after `kill_grace_s` (10 seconds) of the machine being awake: time asleep gives the job no
time to shut down. Processes that can't die don't keep the job's resources: those are released
anyway, and `status` and `why` list the stuck pids.

A job that uses under 1% of one CPU core and writes no output for `stall_minutes` (10) is marked
stalled, and the stacks of its processes go into its log. `--kill-on-stall` ends it then. A thread
that wakes now and then to poll doesn't keep a job from stalling. Time the machine spends asleep
doesn't count toward a stall; it does count toward the run limit.

## Resources

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

## Where it runs

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

## Order

Jobs start by priority, `--priority 1` to `5` (5 highest, default 3), then by submit time. Each
time a job is passed over, meaning a job submitted after it is granted something it was waiting
for, its priority rises by one, up to 4, so priority-5 jobs still go first. While the first job in
line waits, later jobs may start if they will end, by their run limits, before its resources are
expected free, or if they don't touch those resources. Running jobs are never stopped for
priority.

## Keys, kept files and labels

- `--key K`: if the dispatching machine already has a job with this key, the dispatch returns that
  job, whatever its state, instead of queuing another. A key stays taken as long as its job's
  record is kept (`retention_days`, 14).
- `--keep GLOB`, repeatable: files matching the glob, relative to the workspace, are copied to the
  dispatching machine when the job ends, into `jobs/<job>/keep/` in its state directory. A glob that
  is absolute, holds `..` or doesn't parse is refused at dispatch. Symlinks and file names follow
  the rules of `potluck cp` (see [Workspaces](workspaces.md)). The output log is always kept there
  too, as `jobs/<job>/output.log`. Both are deleted after `retention_days`.
- `--label K=V`, repeatable: stored with the job.

Crash reports of a job's processes go to `jobs/<job>/crash/` in the dispatching machine's state
directory, and are deleted with the job's other files after `retention_days`. On macOS these are
the `.ips` files that ReportCrash writes to `~/Library/Logs/DiagnosticReports`, moved from there
when the crashed process, or its parent, was one of the job's. On Linux, when systemd-coredump
recorded core dumps of the job's unit, the output of `coredumpctl info` for them is saved there as
`coredumps.txt`; the dumps themselves stay where systemd-coredump keeps them.

## Output

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

## How a job ends

`potluck wait` exits with the job's own exit code, or 128 + N when signal N killed it. Endings
potluck causes have fixed codes:

| Code | Ending |
| --- | --- |
| 124 | killed at its run limit, or stalled with `--kill-on-stall` |
| 125 | its machine was lost and it wasn't queued again, or every machine that could run it was lost (see [Lost machines](cluster.md#lost-machines)); or the daemon of the machine running it lost track of its processes: after a daemon restart nothing recorded them or how the job ended, or 20 samples of them in a row failed |
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

## Environment

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

## Dispatching from inside a job

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
`--label`, `--no-rerun`, `--kill-on-stall` or `--without-submodule`, which only mean something for
a job of its own; a recipe's `keep`, `hold` and `without_submodules` are left out, and `dispatch`
says so. It never waits for the job's own resources, so a job can't deadlock on itself; for the
same reason `potluck wait` and `potluck logs -f` of the job's own id fail at once inside it, with
exit code 2. The job runs the
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
`on`, `hold`, `keep`, `without_submodules` (a list of `--without-submodule` patterns) and `env`. Unknown fields are refused, so a misspelled option can't be dropped
silently. `on` is a table, as above, or a list in `--on`'s form: `on = ["macos=27", "gpu=apple"]`.
Options on the command line replace the recipe's; `on` and `env` merge key by key, the command line
last, with `os`, `macos` and `linux` counting as one key, so `--on macos` replaces a recipe's
`os = "linux"`. A recipe's `exclusive = true` can't be turned off from the command line. `[env]`
applies to every job dispatched from the repository.
