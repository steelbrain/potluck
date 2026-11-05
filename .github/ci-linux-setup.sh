#!/bin/bash
# Prepares a GitHub-hosted Ubuntu runner for potluck's tests. ci.yml runs it from the checkout,
# before anything is built. It uses sudo and changes the machine (packages, a mount over ./target,
# lingering), so it is for throwaway CI runners only, not for a development machine.
#
# - ./target becomes a btrfs filesystem in a loopback file. The workspace, tunnel and e2e tests
#   keep their temp dirs under the target directory (CARGO_TARGET_TMPDIR is target/tmp), and
#   these must be on a copy-on-write filesystem; the runner's own disk is ext4.
# - The systemd user manager runs for this user, kept by lingering: potluck runs jobs as
#   `systemd-run --user` units, and its daemon refuses to start without lingering. The job's
#   steps run outside a login session, so nothing sets XDG_RUNTIME_DIR or
#   DBUS_SESSION_BUS_ADDRESS for them; this script writes both to $GITHUB_ENV for the steps after
#   it. Without them `systemctl --user` doesn't answer, and tests/linux_systemd.rs skips its tests.
set -euo pipefail

: "${GITHUB_ENV:?not running in GitHub Actions}"
user=$(id -un)
uid=$(id -u)
gid=$(id -g)
runtime=/run/user/$uid
bus=unix:path=$runtime/bus
target=$PWD/target
image=${RUNNER_TEMP:?}/potluck-target.btrfs

# btrfs-progs for mkfs.btrfs; dbus-user-session for the user bus; elfutils for eu-stack, which a
# test in src/exec/linux/stacks.rs needs (it is skipped without eu-stack or gdb).
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends \
    btrfs-progs dbus-user-session elfutils

# The target directory on btrfs. The file is sparse: it takes only the space written to it.
truncate -s 20G "$image"
mkfs.btrfs -q "$image"
mkdir -p "$target"
sudo mount -o loop "$image" "$target"
sudo chown "$uid:$gid" "$target"
fstype=$(findmnt -no FSTYPE --target "$target")
if [ "$fstype" != btrfs ]; then
    echo "$target is on $fstype, not btrfs" >&2
    exit 1
fi
# Files clone there, as potluck's preflight check needs.
probe=$(mktemp "$target/.reflink-probe.XXXXXX")
echo potluck >"$probe"
cp --reflink=always "$probe" "$probe.clone"
rm -f "$probe" "$probe.clone"
df -h "$target"

# The systemd user manager, kept by lingering. Enabling lingering starts it; starting
# user@UID.service as well waits until it is up.
sudo loginctl enable-linger "$user"
sudo systemctl start "user@$uid.service"
export XDG_RUNTIME_DIR=$runtime DBUS_SESSION_BUS_ADDRESS=$bus
# dbus-user-session may have been installed after the user manager started: load its units and
# start the user bus.
systemctl --user daemon-reload
systemctl --user start dbus.socket

linger=$(loginctl show-user "$user" --property=Linger --value)
if [ "$linger" != yes ]; then
    echo "lingering is '$linger' for $user, not yes" >&2
    exit 1
fi
systemctl --user show-environment >/dev/null
busctl --user status >/dev/null
# potluck sets MemoryMax on its units: the user manager needs the memory controller.
controllers=$(cat "/sys/fs/cgroup/user.slice/user-$uid.slice/user@$uid.service/cgroup.controllers")
echo "controllers delegated to the user manager: $controllers"
case " $controllers " in
    *" memory "*) ;;
    *)
        echo "the memory controller is not delegated to user@$uid.service" >&2
        exit 1
        ;;
esac
# A transient unit runs, as potluck starts one.
systemd-run --user --unit="potluck-test-ci-setup-$$" --wait --quiet --collect -p MemoryMax=64M -- true
systemd-run --version | sed -n 1p
# "running", or "degraded" when a unit of the user manager failed; for the log only.
systemctl --user is-system-running || true

{
    echo "XDG_RUNTIME_DIR=$runtime"
    echo "DBUS_SESSION_BUS_ADDRESS=$bus"
} >>"$GITHUB_ENV"
