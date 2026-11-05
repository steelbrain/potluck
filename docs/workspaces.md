# Workspaces

A workspace is one commit's tree on one machine, with an id like `studio-w5`. Each job gets a new
one when it starts, unless it names one with `--workspace`.

- **Seeding.** A workspace is a detached `git worktree` of the machine's mirror of the repository,
  with each submodule a worktree of its own mirror. Its files are copy-on-write clones of a base
  checkout, and git writes only the files that differ. Before each job, HEAD and each submodule's
  HEAD are checked against the dispatched commits.
- **Reuse.** `--workspace W` runs the job in W, on W's machine, after the jobs already running or
  queued there. W must be at the same commit; when the dispatch's commit descends from W's, the
  refusal says `--advance` would take it. If W or its machine is gone, the dispatch fails at once.
- **Advance.** With `--advance`, `--workspace W` also takes a commit that descends from W's, in
  the repository it is dispatched from. When the job starts, W and its submodules are checked out
  at the job's commits with `git checkout --force`: tracked files become the commit's, and changes
  earlier jobs made to them are discarded, even when W was at that commit already. Untracked and
  ignored files, such as build output in `target/`, are kept, unless the commit has a file at the
  same path. W is then at the new commit, and its holds, reminders and `status` say so;
  `status --json` names the job that moved it (`advanced_by`). The job fails at start, leaving W
  as it was, if W's submodules aren't the job's (the commit adds or removes one, or
  `--without-submodule` leaves out other ones than W's first dispatch did), or if an `--advance`
  queued before it moved W to a commit this one doesn't descend from. A job queued into W at the
  old commit without `--advance` fails at start, naming the job that moved W. `--advance` without
  `--workspace` is refused.
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
valid UTF-8. A build, then tests in the same tree, and the tests again after a fix is committed,
with the build output kept:

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
potluck: job studio-7 failed, exit code 101
$ git commit -qam 'Fix the parser test'
$ potluck dispatch --timeout 5m --workspace studio-w5 --advance -- cargo test
job studio-8
workspace studio-w5
$ potluck wait -q studio-8
potluck: job studio-8 succeeded, exit code 0
potluck: workspace studio-w5 on studio is held for this branch for 54m10s more (commit 0c7a2d91e4b3); `potluck release studio-w5` ends it
$ potluck cp studio-w5:Cargo.lock /tmp/
copied 1 file (147 B) from studio-w5:Cargo.lock to /tmp/Cargo.lock
$ potluck hold studio-w5 3h
workspace studio-w5 is held for 3h from now (`potluck release studio-w5` ends it sooner)
$ potluck release studio-w5
released workspace studio-w5
```
