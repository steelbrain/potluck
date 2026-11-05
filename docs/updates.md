# Versions and updates

Members talk only to members running the same version. A member that meets a newer one updates
itself to that version from the GitHub releases, at most once per version, retrying with backoff
from 1 minute up to an hour if that fails; versions only go up. `potluck self-update` has this
machine's daemon update itself to the latest release and restart in place, and the other members
follow when they meet the new version. Until they all run it, a member on the other version shows
as updating, not down: its running jobs go on, a job only it can run is queued for it, and `wait`
and `logs` of a job it owns wait for it. The command returns before the update starts; how it went
goes to `daemon.log` in the state directory, which the command names. During a restart in place
the command fails, saying to run it again once the restart is done. `potluck self-update --here`
does the update in the command's own process, without asking the daemon, so it works while no
daemon runs: it replaces the potluck you ran and asks the daemon to restart in place (a daemon from
before `potluck restart` gets a service restart), and the other members follow in the same way.
An update checks the release's SHA-256 and replaces the binary it runs from (the daemon's, or with
`--here` the one you ran), even one built from source. Running jobs survive any daemon restart:
the new daemon finds them again, and after a plain restart `wait` and `logs` connect to it again.
A daemon still running after its binary was replaced (with `POTLUCK_NO_RESTART=1`, or by a package
manager) starts each job's supervisor and git's tunnel from a copy of the binary it started from,
`bin/potluck` in the state directory, not from the new one.

## Restart in place

A restart in place (`potluck restart`, or the daemon after an update) runs the potluck binary at
the daemon's path in the daemon's own process, so its pid stays the same and systemd or launchd
sees no restart. The daemon first checks that the new binary runs and can take over. Then no job
starts until work already going (a workspace being seeded, a mirror sync) has finished, for at most
a minute; a job dispatched meanwhile is queued and starts after the restart. Then new connections
wait in the kernel's backlog instead of being refused, and the daemon hands the new binary its
socket, its cluster port, its open connections and its view of the members. Running jobs keep
running, `wait` and `logs -f` go on over the same connection without losing or repeating output,
and the other members don't see the machine go down. `potluck restart` prints the new binary's
answer, which says how long new connections waited. The new binary reads node.toml as a plain
start does, so `potluck restart` applies changes to it. A new binary that doesn't run, or that
wouldn't start with node.toml as it now is (a key given twice, another name for the machine),
leaves the daemon serving on the old one, and `potluck restart` says why. One that runs but can't
take over (a potluck from before `restart`) gets a plain restart: open connections close, running
jobs keep running, commands run while the new daemon starts wait for it, and `potluck restart` says
it restarted plainly and why, and exits 0 once the daemon answers again. While the state directory
can't be written (a full disk), the restart waits, and the daemon tries again every 10 seconds. A
restart asked by another potluck binary than the daemon's, while the one at the daemon's path is
still the binary it runs, would run that one again unchanged: the daemon refuses it, names both
binaries, and serves on as it was (`potluck restart` exits 2, and install.sh and `self-update
--here` fail with that).
