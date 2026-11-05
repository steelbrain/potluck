# A cluster

## Pairing

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
holds the signed admission of that key, the membership records that make the members members,
and the members' addresses; the new machine takes the rest of the records from the members. In a
cluster with [users](#users), `admit` signs the admission with the command line's user key and
refuses without one, and `--pool POOL` puts the machine in pool POOL (default `default`); without
users, the member's machine key signs it.
A machine's name is the one its daemon first ran under (see
[Installing](install.md#the-machines-name)). Names must be unique, and are letters, digits, `.`, `_`
and `-`, not starting with `-`. A machine that isn't in a cluster founds one with its first `admit`.
A daemon listens on its port only while its machine is in a cluster with other members: from its
first `admit` or from `pair --finish`, until it is removed or its admission is voided, or 30 seconds
after the removal of every other member.

The admitting member announces the admission, and every member accepts a key that a member
signed (in a cluster with users, a user), so joining one member joins the cluster. Members compare
their records on every heartbeat and exchange them when they differ, so a member that was offline
catches up when it returns. A
member back from being away (its daemon restarted, or it heard from no member for 30 seconds)
takes jobs from another member, or hands jobs to one, only once it has compared records with every
other member, waiting up to 30 seconds for those it doesn't hear from; so it learns of a removal
made while it was away before it deals with the removed machine. Meanwhile `why` says so for a job
that machine could start (a `catch_up` blocker).

`potluck remove NODE`, on any other member, removes a machine. The removal is announced the same
way, and the machine's key is refused everywhere after. The removed machine runs only its own jobs
from then on and says so in `status`; `potluck pair` there makes a new key to join again.

In a cluster with users, the command line's user key signs the removal, and `remove` refuses
without one. Such a removal is final: the machine removed signs nothing that counts, so it can't
undo it. The next two paragraphs are about a cluster without users.

Wherever you run it, the removal is signed by the oldest member that is up and was admitted before
NODE, the founder first, and such a removal is for good. When removals cross, the one signed by the
member admitted earlier counts (see [Security](security.md)), so a removal signed by a member
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

## Users

Without users, every member's machine key signs admissions and removals, so any member can admit a
machine or remove one. `potluck user init` makes a user key for this command line, if there is
none, and makes it the cluster's first user:

```console
studio$ potluck user init --name anees
anees is the cluster's first user; signed in its members: mini, studio; signed 12 jobs studio dispatched before it that haven't ended
potluck: made a user key at /Users/anees/.config/potluck/user.key
```

The user key is an ed25519 key in the file `$POTLUCK_USER_KEY`, else `user.key` beside the command
line's `node.toml`, with mode 0600. Only the command line reads it; the daemon never does. The
first user's admission is signed by the user key and co-signed by this machine's machine key, and
the user key signs every member in again, in one batch with it, so no member drops out. On a
machine in no cluster, `user init` founds one with that machine as its only member. Once the
cluster has users, `user init` with a user's key makes no records and signs the jobs of the
machine it runs on that a first run left unsigned (its signing cut short: the daemon restarted,
the connection dropped); with any other key it refuses.

From then on:

- Only a user's signed record admits or removes a machine. Admissions and removals signed by
  machine keys count for nothing, and a member's daemon signs neither.
- A machine's pool is the one its admission names: `potluck admit --pool POOL`, default `default`.
  `potluck nodes` shows each machine's pool (`pool` with `--json`). The members `user init` signs
  in are in `default`. Pool names are letters, digits, `.`, `_` and `-`, not starting with `-`. A
  job runs only on machines in its pools (`dispatch --pool`, default `default`; see
  [Queue and claims](#queue-and-claims)).
- The command line signs every dispatch with its user key, for the machine it dispatches on, and
  refuses to dispatch without one. The daemon there takes a dispatch only when a user signed it,
  for that machine, within 10 minutes of the daemon's clock, and with a nonce it hasn't seen in the
  day before (see [Dispatching](jobs.md#dispatching)). `resize` signs the job with its lowered
  request the same way, as that machine holds it, and the machine takes it only when the job
  signed is the one it holds, field by field, but for the request.
- A machine claims another machine's job only when a user it knows signed it, and runs it only
  after checking the grant: a user it knows signed the job for the machine that granted it, and
  the job granted is the one signed, field by field (repository ids aside, which the dispatching
  machine salts with its name, the same way on every machine), for this machine's pool. A job that
  fails the check isn't run: the machine hands it back, logs a `WARNING` line saying why, and
  doesn't claim it again while it stays queued. A job queued again after its machine is lost is the
  same signed job, and is checked again where it runs next.
- `user init` signs, with the user key, the jobs the machine it runs on dispatched before it that
  haven't ended (queued, or starting or running on any member), each as that machine holds it, so
  they run on any member as before; it says how many (with `--json`, `signed_jobs` lists them).
  Jobs other machines dispatched before it stay unsigned: they still run, but only on the machine
  they were dispatched on.
- Only a user's signed record adds or removes a user. `potluck user key` prints this command line's
  public key, making the key first if there is none. A user adds that key with `potluck user add
  NAME KEY` and removes it with `potluck user remove NAME`; `user remove` refuses to remove the
  last user. A removed user's key signs nothing that counts after; what it signed that the machine
  you removed it on knew of stays. `potluck user list` lists the users.
- The command line signs each request it sends with its user key. A machine answers a request
  that another machine forwards (`wait`, `logs`, `why`, `cancel` and `resize` of a job dispatched
  elsewhere, `cp`, `hold` and `release` of a workspace elsewhere, `env NODE`) only if a user's key
  signed it at most 5 minutes before or after the answering machine's clock, and it answers each
  signed request once. A command line without a user key gets an error saying the request needs
  this cluster's user key, and where it looked for one. What the local daemon answers itself needs
  no key: `status`, `nodes`, and requests about this machine's own jobs, workspaces and
  environment.

When first users compete (`user init` on two members at once), the one co-signed by the member
admitted earliest counts, the founder first. A member admitted before the one you run `user init`
on can therefore make a first user that counts instead of yours, with its machine key, at any time;
`user init` names such members. Run it on the founder to rule that out.

## Sources

The dispatching machine's repository is the source of truth: its commits may exist nowhere else, and
the other members need no access to its remotes. A member keeps a bare mirror of a repository and of
each of its submodule repositories from the first time a job from it is queued that the member could
run (the dispatching machine, at dispatch). Mirrors are fed from the dispatching machine over the
cluster's connections with git's own protocol, so only the objects a mirror lacks travel. The
dispatching machine serves a member only the commits of its jobs that are queued, for a member that
may take them, or that run on that member, each by its id with the history and files it reaches: no
other branch, tag or commit of the repository. The dispatching machine's mirrors are exact copies of
the source's branches and tags, force-updates and deletions included; another member's mirrors have
no branches or tags. A job that runs git in its workspace writes the mirror's branches and tags (a
worktree shares them): a branch or tag that changed since the last sync is reset to the source's at
the next sync. A mirror that changed that way, or that fails its checks and is cloned again from the
source, is reported in the daemon's log and in the log and `why` of the job the sync was for:

```console
--- potluck: warning: mini: the mirror of app-7c27480694ae7212 had changed since its last sync, so something other than potluck wrote to it (a job running git in its workspace, for one), and was reset to its source's branches and tags: refs/heads/scratch was made here, at f988fe3f3570 ---
```

Repository ids are salted with the dispatching machine's name, so two machines whose repositories
have the same path never share a mirror on a third.

A repository a dispatch carries (`--carry`, see [Dispatching](jobs.md#dispatching)) travels like
a submodule: a member's mirror of it is fed from the dispatching machine, and its commit is pinned
and must be there before the member takes the job, but it is not checked out in the workspace. Its
id is the one a dispatch from that repository on the same machine gets, so the commit goes into
the mirror that repository's own jobs use.

A dispatch pins its commits with refs under `refs/potluck/` until its workspace is removed, so
amending or deleting a branch can't let `git gc` drop them. A machine takes a job only once its
mirrors hold every commit the job names, and nothing is fetched at run time. A mirror still
syncing shows in `why` as a `sync` blocker.

## Queue and claims

Each machine keeps the queue of the jobs dispatched on it. Members exchange queues about once a
second and whenever one changes, so every member sees every queued job for its pool. A job runs
only on machines in one of its pools (`dispatch --pool`, default `default`; a machine's pool is its
admission's, see [Users](#users)): the dispatching machine tells a member of a queued job, serves
it the job's repositories and grants it the job only when the member is in one of those pools, and
in a cluster with users only when the job is signed. It also refuses a claim when the claiming
machine's facts, as that machine last reported them, don't meet the job's `--on`. What members tell
each other of a job leaves out its command, env, the paths of its repositories and the spec its
signature covers (and, while it runs, its labels and `--keep` globs), except to the job's
dispatching machine; the machine that claims a job gets all of it with the grant. So `status` on a machine that neither dispatched nor runs a job shows it
without its command (`-`), and other members' workspaces without their paths. Each machine plans for
itself and claims a job from its dispatching machine only when it can start it now. The dispatching
machine grants each job once; a machine whose claim is refused plans again. A machine runs a job
granted to it only when the grant asks for the resources, `--on` and workspace the queue entry it
claimed asked for, which it reserved; otherwise it hands the job back, logs a `WARNING` line, and
doesn't claim it again while it stays queued. A claim that reserved more RAM than the job asks for
since a `resize` is refused, and the machine claims the job again from its new request. A machine
with no workspace of the job's repository leaves the job for 3 seconds when a member with one could
start it by then; `why` says so meanwhile.

The machine running a job streams its output to the dispatching machine and sends the kept files
at the end, so `wait` and `logs` work there as on one machine. Kept files that can't be delivered
for 10 minutes are given up on, and at once if their machine leaves the cluster: the job ends with
its own exit code and a note saying so (`why` shows it), and the files stay in the job's directory
on the machine that ran it. `cancel`, `resize`, `cp`, `hold`, `release` and `env NODE` are
forwarded to the machine concerned, and `status`, `nodes` and `why` show every member. A machine
answers a forwarded request about a job only if it dispatched the job or runs it (until the job's
end and output have reached the dispatching machine), and one about a workspace only if the
workspace is its own. A `cancel` that reaches the machine running a job goes on to the job's
dispatching machine, which cancels it. In a cluster with [users](#users), a forwarded request
needs a user's signature.

The dispatching machine counts the run limit of a job running elsewhere on its own clocks: from
its grant, then from when the machine running it says it started. A job that machine hasn't ended
10 minutes past that limit ends as timed out (exit 124), and that machine stops it when it next
reports. The 10 minutes count while the dispatching machine is awake and the other machine up.

## Lost machines

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
