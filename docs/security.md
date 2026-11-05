# Security

- Membership is full trust: any member can run commands on any other, as the user who runs potluck
  there, and read its workspaces.
- Each machine has an ed25519 key pair, in `keys/node.key` in the state directory (mode 0600).
  Members connect over TLS 1.3, both sides authenticated by the other's pinned public key, with no
  certificate authorities or host names involved. Control messages, git objects, logs and file
  copies all travel over these connections, encrypted and authenticated.
- Each member's reports carry its queued jobs and the jobs running on it to every other member,
  without their command, env (`--env`, the committed `potluck.toml`'s, `--unset-env`) or the
  paths of their repositories on the dispatching machine, and a running job also without its
  labels and kept-file globs; the job's dispatching machine gets all of a job running elsewhere.
  Reports carry no lease tokens and no workspace paths. A job's whole spec, env included, goes to
  the machine that claims it, with its grant, and `potluck env NODE` brings NODE's whole login
  environment to the member that asks. A secret in either reaches that machine, over the
  connections above only. A member can read it anyway, through a job it runs on that machine.
- A member fetches a repository from the machine that dispatched a job naming it (its own
  repository, a submodule's or a carried one) while that job is queued, or runs on the member
  that asks; and then any object of that machine's mirror of it, every branch and tag of the
  source included, not only the job's commits.
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
