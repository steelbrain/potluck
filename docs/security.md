# Security

## Users and machines

- potluck has two kinds of keys, both ed25519. Each machine's daemon has a machine key,
  `keys/node.key` in the state directory (mode 0600), which authenticates its connections. A user
  key is held by the command line, never by the daemon: `user.key` beside `node.toml`, or
  `$POTLUCK_USER_KEY` (mode 0600).
- In a cluster with users (after `potluck user init`, see [Users](cluster.md#users)), only what a
  user key signs has authority:
  - admitting and removing machines and users;
  - each job, for the machine it was dispatched on: its command, env, commits, limits and pools;
  - each request one machine forwards to another (`wait`, `logs`, `why`, `cancel`, `resize`,
    `cp`, `hold`, `release`, `env NODE`).
  
  Machines carry out signed work: they queue, claim and run signed jobs, send their output and
  kept files back, and pass records on. A machine key signs nothing that counts.
- Without users, membership is full trust: any member can admit or remove machines, run commands
  on any other member as the user who runs potluck there, and read its workspaces.
- A machine that holds a user key is as trusted as the user. Any process running as that user
  there, jobs included, can read the key file and sign with it. Keep user keys on the machines you
  dispatch from.
- When first users compete, the one co-signed by the member admitted earliest counts. A member
  admitted before the machine you run `user init` on can make a first user that counts instead,
  with its machine key, at any time. `user init` names such members; run it on the founder.

## What a machine without a user key can still do

In a cluster with users, a machine whose root is hostile can still:

- read the source, command and env of each job granted to it, and fetch the repositories of every
  job its pools may run: any object of the dispatching machine's mirror of them, every branch and
  tag included, not only the job's commits;
- see the queue entries of jobs its pools may run (resources, priority, repository ids and
  commits, the signing user's key) and every member's facts, address and version;
- report any result, output and kept files for the jobs it runs, or none, and hold a job it
  claimed for up to about twice its run limit and 10 minutes more: the dispatching machine counts
  the limit from the grant and again from the runner's report that the job started, and the 10
  minutes only while it is awake and hears from the runner;
- report false facts and workspaces of its own (facts are self-reported, so `--on` catches
  mistakes, not lies), and so be granted a job it can't run, or hold a job back from machines
  without a workspace of its repository for up to 3 seconds, the wait for a warm one; workspaces
  it reports on other machines count for nothing;
- say that it runs a job another machine dispatched: other members show the job running there
  only while the dispatching machine is away, and the claim ends no `wait`;
- withhold or delay the records and reports it passes on; it can't forge records other keys
  signed;
- send a signed request it answered on to another machine within 5 minutes: a request names its
  job, workspace or machine, so elsewhere it gets nothing new;
- make the other members update, by running a newer version: they download that release from
  GitHub and check it against its `SHA256SUMS`, so it can't choose the binary.

It can't make another machine run anything a user didn't sign, add or remove machines or users,
or read other jobs' logs, workspaces or env through another machine.

## A tailnet ACL

potluck enforces these limits itself, on its own port. A tailnet ACL that lets a machine reach only
the members' potluck port (7465) changes nothing about what it can do through potluck, but it stops
the machine from reaching anything else on the members: ssh and other services listening on their
tailnet addresses. For a machine whose root you don't trust, keep that ACL.

## Connections and records

- Members connect over TLS 1.3, both sides authenticated by the other's pinned machine key, with no
  certificate authorities or host names involved. Control messages, git objects, logs and file
  copies all travel over these connections, encrypted and authenticated.
- Each member's reports carry its queued jobs and the jobs running on it to every other member,
  without their command, env (`--env`, the committed `potluck.toml`'s, `--unset-env`) or the
  paths of their repositories on the dispatching machine, and a running job also without its
  labels and kept-file globs; the job's dispatching machine gets all of a job running elsewhere.
  With users, a member gets the queue entries only of jobs its pools may run. Reports carry no
  lease tokens and no workspace paths. A job's whole spec, env included, goes to the machine that
  claims it, with its grant, and `potluck env NODE` brings NODE's whole login environment to the
  user who asks.
- The pairing request and reply are neither secret nor encrypted. Pairing relies on you carrying
  them between your own shells: a key you `admit` becomes a member, and the reply you give
  `pair --finish` decides which cluster the new machine joins.
- A removal lists the removed key's records that counted for the remover when it signed it. Every
  other record signed by that key is void, wherever it turns up and whatever date or logical
  clock it claims, so backdating doesn't help: a removed machine or user can't sign its way back
  in or admit others, and members turn its later records away. A record the remover hadn't seen
  is lost even if honest, so a machine admitted just before its admitter's removal may need
  admitting again.
- Members keep and pass on every record signed by a member's or user's key, also one that counts
  nowhere, so a key holder can fill the others' disks with records while it counts. With users,
  records signed by the machine keys of machines admitted after `user init` aren't kept. Once a key is removed, its records
  that its removal doesn't list, and the records of machines whose only admission was one of
  those, are turned away; a member that took them before it heard of the removal drops them when
  its daemon next starts.
- Without users, when removals cross (A removes B while B removes A), the removal by the member
  admitted earlier counts, so the founder's removal wins any such race. This settles races between
  members acting in good faith; it is no defence against a hostile member, which can run
  `potluck remove` on any other member through a job there. With users, a removal is final.
- At most 64 connections, and 8 from one address, may be unauthenticated at once.

## On each machine

- The command line talks to its daemon over a Unix socket, `potluck.sock` in the state directory,
  with mode 0600. The daemon answers its own command line without a user's signature: the socket
  is the user's. It keeps the state directory at mode 0700: it holds job specs with their `--env`
  values, logs, mirrors and workspaces.
- Workspaces, mirrors, logs and kept files are plain files on disk. Release downloads are checked
  against the release's `SHA256SUMS`, which catches a corrupted download but not a tampered release.
