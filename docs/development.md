# Development

## Toolchain

potluck builds with the nightly that `rust-toolchain.toml` pins, with rustfmt and clippy.
`.cargo/config.toml` passes `-Zthreads=8` to rustc, which only a nightly accepts. Run
`rustup toolchain install` in the repository to install the pinned toolchain and its components.

The code must also build on stable Rust, because crates.io users build it with their own
toolchain. The last of the checks below covers that.

## Checks

A change must pass these four. CI runs them on every push and pull request.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
RUSTFLAGS= cargo +stable check --all-targets
```

The empty `RUSTFLAGS` replaces the `-Zthreads=8` from `.cargo/config.toml`, which stable rustc
rejects. CI also passes `--locked` to the cargo commands, so a `Cargo.lock` that doesn't match
`Cargo.toml` fails there.

## What the tests need

The tests run real processes, git repositories and sockets on 127.0.0.1, and on Linux real
systemd units. The machine that runs them needs what a potluck machine needs:

- The target directory on a copy-on-write filesystem: APFS on macOS; btrfs, or XFS with reflink,
  on Linux. The workspace, tunnel and end-to-end tests (`tests/workspace.rs`, `tests/tunnel.rs`,
  `tests/e2e.rs`) keep their temp dirs in `target/tmp` (Cargo's `CARGO_TARGET_TMPDIR`), because
  the system temp dir is often tmpfs. If the checkout is on another filesystem, set
  `CARGO_TARGET_DIR` to a directory on one. The other tests use the system temp dir.
- On Linux, the systemd user manager, with lingering enabled for your user
  (`sudo loginctl enable-linger $USER`). potluck runs jobs as `systemd-run --user` units. The
  tests name their units `potluck-test-*` and remove them when they finish.
- On Linux, `systemctl --user` must answer. Outside a login session (a system service, a CI
  runner) set `XDG_RUNTIME_DIR=/run/user/$(id -u)` and
  `DBUS_SESSION_BUS_ADDRESS=unix:path=$XDG_RUNTIME_DIR/bus`. Where no user manager answers, the
  tests in `tests/linux_systemd.rs` print `skipped: no systemd user manager answers` and pass
  without testing anything.

A few tests skip themselves, with a note on stderr, when the machine lacks something:

- A test of stack sampling (`src/exec/linux/stacks.rs`) needs `eu-stack` (elfutils) or `gdb`,
  and a C compiler.
- The end-to-end GPU tests need a GPU. They lease it only for jobs that run `sleep`.
- The test of the copy-on-write check's error needs a filesystem without clones: `/dev/shm` on
  Linux, or a small HFS+ disk image that it makes with `hdiutil` on macOS.

## Opt-in tests

`cargo test` leaves out the tests marked `#[ignore]`. Run them by name with `-- --ignored`:

- `service_install_and_uninstall_for_real` (`tests/cli.rs`) installs and removes a real user
  service named `potluck-test-*`.
- `install_sh_restarts_only_a_service_that_runs_the_installed_file` (`tests/self_update.rs`)
  does the same, and runs `install.sh` against it from a local release.
- `restarts_a_running_systemd_unit` and `restarts_a_loaded_launch_agent` (`src/update/tests.rs`)
  start a real systemd unit or LaunchAgent: `cargo test -- --ignored restarts_`.
- `core_dumps_go_to_the_crash_dir` and `core_dumps_of_an_earlier_run_of_the_unit_stay_out`
  (`tests/linux_systemd.rs`) each leave a core file in `/var/lib/systemd/coredump`:
  `cargo test --test linux_systemd -- --ignored core_dumps`.
- `no_start_waits_on_a_process_spawned_meanwhile` (`tests/macos_exec.rs`) churns processes for
  25 seconds to check that no start waits on a process spawned at the same moment:
  `cargo test --test macos_exec -- --ignored no_start_waits`.
- `seed_timing` (`tests/workspace.rs`) times seeding a large synthetic repository:
  `cargo test --release --test workspace -- --ignored --nocapture seed_timing`.
- `smoke_between_machines` (`src/mesh/tests.rs`) is a manual test between two machines, driven
  by `SMOKE_*` environment variables.

## CI

`.github/workflows/ci.yml` runs the four checks on macOS arm64 (`macos-15`), Linux x86_64
(`ubuntu-24.04`) and Linux arm64 (`ubuntu-24.04-arm`). On Linux it first runs
`.github/ci-linux-setup.sh`, which mounts a btrfs filesystem (a loopback file) over `./target`,
enables lingering, and starts the user manager and its bus. That script uses sudo and changes
the machine, so don't run it on a machine you use.

`.github/workflows/release.yml` builds the binaries for a pushed `v*` tag and publishes them as
that tag's GitHub release, with `install.sh` and `SHA256SUMS`. Before a binary is handed on, it
must run and print `potluck` and the tag's version for `--version`, and a Linux one must be
statically linked; the x86_64 macOS binary runs under Rosetta on the Apple silicon runner, which
installs Rosetta first if the image lacks it. The jobs that build have a read-only token; they
pass the binaries as workflow artifacts to the job that publishes them.

Every `uses:` in the workflows names a full commit SHA, with the version in a comment. To move an
action to a new version, take that tag's commit from
`git ls-remote --tags https://github.com/OWNER/ACTION` (for an annotated tag, the line ending in
`^{}`). `.github/dependabot.yml` has Dependabot open weekly pull requests that update the pinned
actions and the crates in `Cargo.lock`, proposing a version once it has been out for 7 days.

After changing a workflow or `.github/dependabot.yml`, check them with zizmor and actionlint,
from the repository root; both must report nothing:

```sh
zizmor --offline .
actionlint
```

With shellcheck on `PATH`, actionlint also checks the `run:` scripts. The workflows were last
checked with zizmor 1.32.0, actionlint 1.7.12 and shellcheck 0.11.0.
