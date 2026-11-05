# AGENTS.md

potluck is one Rust binary: a daemon on each machine and the command line that talks to it. Members
of a cluster connect over TLS (port 7465) and run each other's queued jobs. A job is any command;
keep features language-agnostic, with nothing that assumes a job is a cargo build.

## Layout

- `src/cli.rs`, `src/cli/`: the commands and their output. `src/client.rs`: the command line's
  connection to its daemon's Unix socket; it signs each request with the user key.
- `src/proto.rs`: messages between the command line and its daemon. `src/types.rs`: job specs,
  records, requests and grants.
- `src/daemon/`: `server.rs` answers the command line, `runner.rs` runs jobs on this machine,
  `handover.rs` restarts the daemon in place, `state.rs` holds the daemon's state, written through
  to its store.
- `src/daemon/cluster/`: `owner.rs` is the dispatching machine's side (grants, runners' reports,
  requeues), `remote.rs` the runner's side (claims, mirror syncs, reporting back), `relay.rs`
  requests forwarded to another member, `views.rs` the reports between members and the
  cluster-wide `status`, `nodes` and `why`.
- `src/mesh/`: machine keys and TLS (`identity.rs`, `tls.rs`), the signed membership records
  (`trust.rs`), user keys (`user.rs`).
- `src/sched.rs`: which queued jobs a machine starts. Its module doc states the rules; change the
  doc and the code together.
- `src/jobauth.rs`: signing and checking jobs. `src/tunnel.rs`: git upload-pack between members.
- `src/workspace/`: mirrors and workspaces. `src/exec/`: running a job on Linux (systemd user
  units) and macOS. `src/update.rs`, `src/update/`: self-update. `src/why.rs`: `potluck why`.
- `docs/`: the user documentation. `docs/development.md`: the toolchain, what the tests need, CI.

## Checks

A change passes these four, as CI runs them on macOS and Linux:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
RUSTFLAGS= cargo +stable check --all-targets
```

The repository builds with the nightly `rust-toolchain.toml` pins; the last check keeps it
building on stable. CI passes `--locked`, so commit `Cargo.lock` with any dependency change. A
`[[patch.unused]]` entry in `Cargo.lock` comes from a `[patch]` in a parent directory's
`.cargo/config.toml`, not from this repository: never commit one.

## Tests

- Tests run real processes, git repositories, sockets on 127.0.0.1 and, on Linux, systemd user
  units named `potluck-test-*`. Keep them hermetic: temporary directories, `POTLUCK_HOME` and
  `POTLUCK_CONFIG` pointed into them, port 0, and clean up what a test starts. Nothing a test
  runs touches a real install's state or service. Tests that install a real service are
  `#[ignore]`.
- Every `tests/*.rs` has `mod common;`. `tests/common` clears `$POTLUCK_USER_KEY` before main, so
  no test signs with the developer's key; a test in `tests/cli.rs` checks that every test binary
  has the module.
- Dispatching, admitting and removing need a user, so tests make one with a real
  `potluck user init` (`common::user_init`; in `tests/cluster.rs`, each node gets one unless
  built with `no_user_key()`), never a bypass in the production code.
- Multi-machine tests go in `tests/cluster.rs`. Its `Node` starts a real daemon; `Node::impostor`
  replaces a node with a bare mesh member that answers as a test scripts it, for testing what a
  hostile member can do.
- macOS limits a Unix socket path to 104 bytes, and the tests put sockets under `target/tmp`, so
  keep the checkout's path short. Don't move a checkout that has been built: test binaries keep
  the old `CARGO_MANIFEST_DIR`. Build again from a fresh one.
- A test checks what the feature guarantees, not timing it doesn't promise. For example, two jobs
  granted in the same plan may start in either order.

## Rules the design keeps

- Users sign, machines carry out. A user key, held only by the command line, signs admissions,
  removals, users, every job and every request forwarded to another member. A machine key signs
  nothing that counts. A machine runs another machine's job only when a user signed it. Any new
  request that crosses machines checks who signed it. `docs/security.md` lists what a hostile
  member can still do; keep that list true.
- Bounds: keep small permanent history (removals, machines that are gone) forever, and limit only
  live things (at most 2048 members, `trust::MAX_MEMBERS`), with an error that names the limit.
  No garbage collection with exceptions.
- Saved state must load after an update: a new field in a saved or sent struct gets
  `#[serde(default)]`, as the existing ones do.

## Docs and history

- `README.md` is the front page: add to it only what a new user needs to start. Details go in
  `docs/`. Write plain sentences that state checkable facts, and leave out caveats about
  internals.
- Every user-visible change gets a line in `CHANGELOG.md` under `## Unreleased` (`### Added`,
  `### Changed` or `### Fixed`).
- Commit messages are one line: `area: what changed`, then why, after a colon.
- Work lands on `development`. `master` is the published branch, and a release is a `vX.Y.Z` tag
  on it. Keep history linear: rebase onto the target and fast-forward it, with no merge commits.
