# Installing potluck

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
- In a cluster, on a Mac with the application firewall on
  (`/usr/libexec/ApplicationFirewall/socketfilterfw --getglobalstate` says `enabled`): an allow
  rule for the potluck binary. potluck's binaries are ad-hoc signed, so the firewall's automatic
  allowing of signed software doesn't cover them. Without the rule, members' connections to the
  Mac's port open but the daemon never receives them, and the members list the Mac as down.
  `socketfilterfw --getappblocked` may still say the binary is permitted. Add the rule with:

  ```sh
  sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add ~/.local/bin/potluck
  sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp ~/.local/bin/potluck
  ```

  The firewall knows an ad-hoc-signed binary by its code hash (`codesign -d -r- ~/.local/bin/potluck`
  prints `designated => cdhash H"..."`), and every new binary has a new one: run both commands
  again after install.sh, `potluck self-update` or a rebuild replaces the binary.
- In a cluster, on macOS: Local Network access for the daemon. The service is a LaunchAgent, and
  macOS's Local Network privacy can block an agent's connections to machines on the local network
  until it is allowed in System Settings > Privacy & Security > Local Network (Apple's
  [TN3179](https://developer.apple.com/documentation/technotes/tn3179-understanding-local-network-privacy)).
  It blocked them on macOS 26.7.1. The members still list the Mac as up, because their connections
  to it get through, but it can't reach them: its `daemon.log` says
  `cluster: NODE reaches this machine, but this machine can't reach it: can't reach NODE: ADDRESS: No route to host (os error 65)`,
  and the kernel log has lines for potluck ending in `reason: NECP`
  (`log show --last 10m --predicate 'eventMessage CONTAINS "potluck"' | grep NECP`). A shell,
  local or over ssh, isn't subject to it, so `nc` from one doesn't show the block. macOS tracks the
  permission by the binary's code signature, so a new binary may need allowing again.

## install.sh

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
(`potluck restart`, see [Versions and updates](updates.md)); the service's daemon from
before `potluck restart` gets a service restart instead. A daemon running in the foreground from a
potluck at another path is left as it is, and install.sh fails, naming both binaries. With
`POTLUCK_NO_RESTART=1` set it leaves the daemon alone and prints the `potluck restart` command for
the service. When the service runs a potluck at another path, it leaves the service alone and
prints the `potluck service install` command that points it at the new one. The script never
uses sudo and never prompts. When no service is installed yet, it prints the next steps: the
`loginctl enable-linger` command on Linux if lingering is off, `potluck service install` and
`potluck pair`. `potluck self-update` replaces the binary and restarts the daemon in the same way
(see [Versions and updates](updates.md)), but doesn't print the next steps.
`POTLUCK_RELEASES_URL` makes either download from a directory holding one release's binaries and
`SHA256SUMS` instead of GitHub.

## Building from source

Until a release is published, build from source. Stable Rust works:

```sh
cargo install --locked --git https://github.com/steelbrain/potluck
```

In a clone, `cargo build --release` uses the nightly pinned in `rust-toolchain.toml`, with
`-Zthreads=8` from `.cargo/config.toml`; `RUSTFLAGS= cargo +stable build --release` uses stable.
The crate isn't on crates.io yet (`publish = false` in `Cargo.toml`).

## The machine's name

A machine keeps the name its daemon first runs under: its short hostname, unless `name` is set in
`~/.config/potluck/node.toml` (see [Configuration](configuration.md)). Names must be unique in a
cluster, so set `name` before `potluck service install` if another machine has the same short
hostname. The daemon refuses to start under a name it didn't start with, and says how to go on.

## Running the daemon

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
