# Changelog

## Unreleased

### Added

- Users. `potluck user init` makes a user key for the command line (`user.key` beside node.toml,
  or `$POTLUCK_USER_KEY`) the cluster's first user and signs every member in with it. From then on
  only a user's signed admission or removal admits or removes a machine: `admit` and `remove` sign
  with the user key, a member's daemon signs neither, and records machine keys sign count for
  nothing. `admit --pool POOL` puts a machine in a pool. `user add`, `user remove`, `user list`
  and `user key` manage the users. See [A cluster](docs/cluster.md#users).
- `dispatch --pool POOL`, repeatable, and a recipe's `pools`: the job runs only on machines admitted
  into one of those pools, `default` when none is given. See [Jobs](docs/jobs.md#where-it-runs).
- Signed jobs. The command line signs every dispatch with its user key and refuses to dispatch
  without one; a machine runs another machine's job only when a user it knows signed it for that
  machine and the job it was granted is the one signed. See [A cluster](docs/cluster.md#users).
- A member fetching a repository from the dispatching machine gets only the commits of the queued
  jobs it may take and of the jobs it runs, with what they reach, and the branches and tags whose
  tips those commits reach; other branches, tags and commits don't come along, and naming one while
  fetching gets the answer an object that doesn't exist gets.
- `potluck nodes` shows each machine's pool, in its POOL column and as `pool` with `--json`.
- In a cluster with users, a request another member answers (`wait`, `logs`, `why` or `cancel` of
  a job dispatched elsewhere, `cp`, `hold` or `release` of a workspace elsewhere, `env NODE`) is
  signed with the command line's user key; without one, it fails saying so.
- `potluck resize JOB --ram N` lowers a queued job's RAM request, and the job keeps its place in the
  queue: its priority, passes and submit time stay. The machine that dispatched the job changes it,
  only while the job is queued, and only to less; in a cluster with users, the command line signs
  the job with its new request, unless the job was queued before the cluster had users. See
  [Jobs](docs/jobs.md#resources).
- `potluck why` of a job that has ended says the most memory it used beside what it reserved:
  `peak memory 1.2 GiB of the 16 GiB it reserved`.

### Changed

- `dispatch`, `admit` and `remove` need a user: until `potluck user init` has made the cluster's
  first user, they refuse and say so (exit 2), on a new machine and in a cluster paired with 0.1.1
  alike. A member's daemon signs no admission or removal with its machine key and takes no
  unsigned dispatch. A removal is a user's and final: `remove --json` always has `"final": true`,
  and no `warning`.
- A machine runs another machine's job only when a user signed it. In a cluster paired with 0.1.1,
  the jobs queued before `user init` stay unsigned: each stays queued on the machine it was
  dispatched on and runs only there, and `potluck why` says so.
- Scheduling: a queued job that fits in what is free on a machine starts there, even while a job
  ahead of it in order waits on that machine. In 0.1.1 nothing behind the first waiting job started
  there. Each job that starts behind a waiting one passes it over, and the waiting job's priority
  rises by one, up to 4. See [Order](docs/jobs.md#order).
- `dispatch --hold-line`, and a recipe's `hold_line = true`, keep the 0.1.1 rule for one job: while
  it waits on a machine, no job behind it starts there. A job queued for 30 minutes holds the line
  the same way without it, so a plain `--exclusive` job waits for at most 30 minutes of jobs
  starting ahead of it, then for the jobs running then to end. `potluck why` of a job waiting
  behind it names the job holding the line and why: "waits behind job studio-7, which holds the
  line on studio (dispatched with --hold-line)".

### Fixed

- `potluck user add NAME KEY` takes a key that starts with `-` (one in 64), not as an option.
- A machine reads at most 64 MiB of each message of a member's answer to a request it relayed
  there (`wait`, `logs`, `why`, `cancel`, `cp`, `hold`, `release`, `env NODE`), and no more of a
  `logs` answer's output than it asked for; the machine that dispatched a job reads at most 64 MiB
  of each message the runner sends its kept files or crash reports with. A member could send a
  message without end and fill the machine's memory, or the command line's. A longer answer is
  dropped and its connection closed: the command line's error names the member, and the daemon's
  log has a warning.
- A member's report counts only the workspaces on that member. Workspaces it listed on other
  machines showed in `status` and could hold a job back for the 3-second locality wait, as if the
  member had a workspace of the job's repository.
- `status` shows a job dispatched on another machine running where that machine says it granted
  it, while that machine is up. A member that said it ran such a job, granted elsewhere or still
  queued, was shown running it.
- Linux with systemd older than 256 (Ubuntu 24.04 has 255): a job that the kernel's OOM killer
  ended at its own `--ram` limit was reported as `failed`, "killed because the machine ran short of
  memory, under its own limit", when potluck learned of the kill only after the job's unit had
  ended. That systemd shows a stale memory peak for an ended unit; potluck now ignores it there,
  and the job ends as `over_memory`.
- Linux with systemd 255: a job that ended at once while the user manager was busy could be
  reported as "systemd-run could not start" instead of with its own exit code.

## 0.1.1 (2026-11-04)

### Added

- `dispatch --carry DIR[@REV]`, repeatable: carries a commit of another repository on the
  dispatching machine into the running machine's mirror of that repository, the one that
  repository's own jobs use there. The job finds the mirror's path in `POTLUCK_MIRROR_<name>` and
  can `git fetch` the commit from it. Nothing checks it out. See [Jobs](docs/jobs.md).
- `dispatch --workspace W --advance`: runs the job in W at a commit that descends from W's. When
  the job starts, W and its submodules are checked out at the new commit: tracked files are reset
  to it, and untracked and ignored files (build output) are kept. A job queued into W at the old
  commit, or one whose set of submodules differs from W's, fails at start and says why. See
  [Workspaces](docs/workspaces.md).
- `dispatch --unset-env K`, repeatable, and a recipe's `unset_env`: K is left out of the job's
  environment, whether the login environment, a `[[gpu]]` entry or the dispatch's env set it.
  potluck's own variables can't be unset.
- `potluck env [NODE]`: prints the login environment that a job on NODE (default: this machine)
  starts from, asked of NODE's daemon in a cluster.
- `dispatch --without-submodule PATTERN`, repeatable, and a recipe's `without_submodules`: leaves
  the matching submodules, and what is nested in them, out of a job. They are not checked on the
  dispatching machine and not shipped to the job's workspace. A pattern that matches no submodule
  is refused.
- macOS: `service install` and the daemon at start warn about a potluck binary with no code
  signature, whose connections to other members Local Network privacy blocks without asking.

### Changed

- Scheduling: while the first job in order on a machine waits, nothing behind it starts there.
  A job goes ahead of another only by priority, and a job passed over that way rises by one
  priority, up to 4. In 0.1.0 a later job could start while the first one waited, if its run limit
  had it ending before the first could start, or if it needed none of what the first waited for.
- The macOS release binaries, arm64 and x86_64, are signed ad hoc.
- Cluster messages: a member listed as down says how its last heartbeat failed, failed incoming
  connections are logged, and a member that reaches this machine while this machine can't reach
  it is said in `daemon.log`, with a Local Network hint on macOS.

### Fixed

- `--keep 'dir/**'` kept only the directories below `dir/`, none of the files directly in it. A
  trailing `**` now keeps everything below. docs/jobs.md says where kept files land: in
  `jobs/<job>/keep/` in the dispatching machine's state directory.

## 0.1.0 (2026-11-04)

The first release: one daemon on each Linux or macOS machine, and a CLI that talks to it.

- `dispatch` queues a command at the repository's HEAD commit, with a run limit (`--timeout`),
  RAM (`--ram`), a CPU weight, GPUs, `--exclusive` and requirements on the machine (`--on`).
  `wait`, `logs`, `cancel`, `status`, `why` and `nodes` follow it. Recipes come from the
  committed `potluck.toml`.
- Jobs run in workspaces: detached git worktrees of a mirror on each machine, with copy-on-write
  files. `--workspace`, `--hold`, `hold`, `release`, `cp` and `--keep` reuse them and get files
  out of them.
- On Linux each job is a systemd user unit with `MemoryMax`, `MemorySwapMax=0` and `TasksMax`. On
  macOS potluck finds the job's processes and kills them over the job's RAM limit.
- A cluster of up to 2048 machines that connect directly over TLS 1.3, each pinning the others'
  keys (`pair`, `admit`, `remove`). The dispatching machine's repository is the source of the
  commits, and members' mirrors are fed from it over the cluster's connections.
- install.sh, `self-update`, `service install`, and `restart`, which restarts the daemon in place
  with running jobs and open connections kept.
- Every command takes `--json`, none prompts, and `--key` makes a dispatch idempotent.
