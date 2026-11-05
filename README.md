# potluck

potluck runs build and test jobs on a handful of Linux and macOS machines owned by one person or
team. Every machine runs the same daemon, and the daemons talk to each other directly. You
dispatch a command from a git repository with the resources it needs. A machine that has those
resources free runs it at the dispatched commit, and the output comes back to the machine you
dispatched from.

It is written to be driven by scripts and coding agents. Every command takes `--json` and none
prompts. A dispatch with `--key` is idempotent. Jobs keep running when the client that dispatched
them exits, and `potluck why` says what a queued job is waiting for and who holds it.

potluck is at version 0.1.1.

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
  (`sudo loginctl enable-linger $USER`), and cgroup v2 with the memory and pids controllers.
- A state directory on a filesystem that can clone files: APFS on macOS, btrfs or XFS with reflink
  on Linux.
- `git` 2.31 or newer.
- In a cluster: one TCP port per machine that the other members can reach (7465 by default), and
  the same potluck version on every member. On a Mac, that means a code-signed binary (the release
  binaries are; one you build for an Intel Mac isn't, until `codesign` signs it), Local Network
  access for it, and on a Mac with the application firewall on a rule for it, the last two
  possibly again for a new binary ([docs/install.md](docs/install.md#requirements) says how).

The daemon refuses to start on a machine that lacks one of the first three.
[docs/install.md](docs/install.md#requirements) says what each is for.

## Install

This installs the latest release, or a given version:

```sh
curl -fsSL https://github.com/steelbrain/potluck/releases/latest/download/install.sh | sh
curl -fsSL https://github.com/steelbrain/potluck/releases/latest/download/install.sh | sh -s -- 0.1.1
```

`install.sh` checks the binary against the release's `SHA256SUMS`, installs it as
`~/.local/bin/potluck`, and restarts a potluck daemon running here in place on the new binary. It
never uses sudo and never prompts.

Until then, build from source. Stable Rust works:

```sh
cargo install --locked --git https://github.com/steelbrain/potluck
```

Then run the daemon as a service, a systemd user unit on Linux or a LaunchAgent on macOS:

```sh
potluck service install
```

A machine is named after its short hostname unless `name` is set in `~/.config/potluck/node.toml`,
and keeps the name its daemon first ran under. Names must be unique in a cluster, so set `name`
before `potluck service install` if two machines share a short hostname.
[docs/install.md](docs/install.md) covers the rest: what install.sh does in each case, building in
a clone, the service and the daemon's log.

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
workspaces is held (see [Workspaces](docs/workspaces.md)).

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

These examples, and the ones in [docs/](docs/), ran on a Mac named `studio`. Cargo's output is
shortened, most reminder lines are left out, and paths are shown as if the repository were
`/Users/me/src/app` and the state directory the default `/Users/me/.local/state/potluck`.

In a script, `--json` prints one JSON document on stdout:

```sh
job=$(potluck dispatch --json --timeout 10m -- cargo test | jq -r .job)
potluck wait "$job"
```

## A cluster

A machine joins once, with the daemon running on it and on a member:

1. On the new machine, `potluck pair` prints a request line.
2. On any member, `potluck admit '<request>'` prints a reply line. A machine that isn't in a
   cluster founds one with its first `admit`.
3. On the new machine, `potluck pair --finish '<reply>'` joins the cluster.

From then on a job dispatched on any member runs on whichever member can run it, and its output
comes back to the machine it was dispatched from. Members reach each other's port over a LAN or a
VPN. [docs/cluster.md](docs/cluster.md) covers pairing, removing machines, how jobs move between
members and what happens when one is lost.

## Documentation

- [Jobs](docs/jobs.md): dispatching, run limits, resources, where a job runs, output, how a job
  ends, its environment, dispatching from inside a job, recipes.
- [Workspaces](docs/workspaces.md): seeding, reuse, holds, removal, `cp`.
- [A cluster](docs/cluster.md): pairing, removal, sources and mirrors, the queue, lost machines.
- [Versions and updates](docs/updates.md): updates across the cluster, `self-update`, restart in
  place.
- [Security](docs/security.md): keys, TLS, membership records, the local socket.
- [Configuration](docs/configuration.md): `node.toml` and GPU entries.
- [Scripting](docs/scripting.md): `--json`, where output goes, potluck's exit codes.
- [Installing](docs/install.md): requirements, install.sh, building from source, the service.
- [Development](docs/development.md): building potluck and running its checks.

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

[docs/scripting.md](docs/scripting.md) says where each command's output goes and what its exit
codes mean.

## License

MIT, Anees Iqbal. See [LICENSE](LICENSE).
