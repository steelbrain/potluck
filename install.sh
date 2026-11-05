#!/bin/sh
# Installs potluck from a GitHub release: picks the binary for this OS and architecture, checks its
# SHA-256 against the release's SHA256SUMS, and installs it as $POTLUCK_INSTALL_DIR/potluck
# (default ~/.local/bin/potluck). It never uses sudo and never prompts.
#
#   curl -fsSL https://github.com/steelbrain/potluck/releases/latest/download/install.sh | sh
#       installs the latest release
#   curl -fsSL https://github.com/steelbrain/potluck/releases/latest/download/install.sh | sh -s -- 0.2.0
#       installs release v0.2.0 (the version may also be given as v0.2.0)
#   sh install.sh [VERSION]  the same, from a downloaded copy
#
# Environment:
#   POTLUCK_INSTALL_DIR   the directory to install into (default: $HOME/.local/bin)
#   POTLUCK_RELEASES_URL  a directory URL holding one release's binaries and SHA256SUMS, used
#                         instead of GitHub (plain http is allowed); a requested version is then
#                         checked against the version the downloaded binary reports
#   POTLUCK_GITHUB_URL    replaces https://github.com (for tests)
#   POTLUCK_NO_RESTART=1  leave a running potluck daemon alone (by default it restarts in place on
#                         the new binary, keeping running jobs and connections, whether it runs as
#                         the service or not; the service is restarted when its daemon is too old
#                         to do that)
#   POTLUCK_SERVICE_NAME, POTLUCK_SERVICE_DIR
#                         the service's name and LaunchAgent directory, as for `potluck service
#                         install` (for tests)

set -eu

# The GitHub repository the releases come from; keep in step with GITHUB_REPO in src/update.rs.
REPO=steelbrain/potluck

die() {
    printf 'install.sh: %s\n' "$*" >&2
    exit 1
}
say() { printf '%s\n' "$*"; }

version=
if [ $# -gt 1 ]; then
    die "usage: install.sh [VERSION]   (for example: install.sh 0.2.0)"
elif [ $# -eq 1 ]; then
    version=${1#v}
    case $version in
        [0-9]*) ;;
        *) die "usage: install.sh [VERSION]   (for example: install.sh 0.2.0)" ;;
    esac
    case $version in
        *[!0-9A-Za-z.+-]*) die "not a version: $1" ;;
    esac
fi

os=$(uname -s)
arch=$(uname -m)
target=
case $os in
    Darwin)
        # On Apple silicon a shell running under Rosetta reports x86_64; install the native binary.
        if [ "$(sysctl -n hw.optional.arm64 2>/dev/null || true)" = 1 ]; then
            arch=arm64
        fi
        case $arch in
            arm64 | aarch64) target=aarch64-apple-darwin ;;
            x86_64) target=x86_64-apple-darwin ;;
        esac
        ;;
    Linux)
        case $arch in
            x86_64 | amd64) target=x86_64-unknown-linux-musl ;;
            aarch64 | arm64) target=aarch64-unknown-linux-musl ;;
        esac
        ;;
esac
if [ -z "$target" ]; then
    die "potluck publishes binaries for macOS (arm64, x86_64) and Linux (x86_64, arm64), not for $os on $arch; build it from source with cargo instead"
fi
asset=potluck-$target

if [ -n "${POTLUCK_INSTALL_DIR:-}" ]; then
    dir=${POTLUCK_INSTALL_DIR%/}
elif [ -n "${HOME:-}" ]; then
    dir=$HOME/.local/bin
else
    die "HOME is not set; set POTLUCK_INSTALL_DIR to the directory to install into"
fi

# fetch URL FILE downloads; redirect_of URL prints where URL redirects to, without following it.
if command -v curl >/dev/null 2>&1; then
    fetch() { curl -fsSL --retry 3 -o "$2" "$1"; }
    redirect_of() { curl -fsS --retry 3 -o /dev/null -w '%{redirect_url}' "$1"; }
elif command -v wget >/dev/null 2>&1; then
    fetch() { wget -q -O "$2" "$1"; }
    redirect_of() {
        wget --max-redirect=0 -S -O /dev/null "$1" 2>&1 | sed -n 's/^ *Location: *\([^ ]*\).*/\1/p' | tail -n 1
    }
else
    die "needs curl or wget to download the release"
fi
# The file on stdin: given a name with a backslash, both print the hash as `\<hash>`.
if command -v sha256sum >/dev/null 2>&1; then
    sha256() { sha256sum <"$1" | cut -d ' ' -f 1; }
elif command -v shasum >/dev/null 2>&1; then
    sha256() { shasum -a 256 <"$1" | cut -d ' ' -f 1; }
else
    die "needs sha256sum or shasum to check the download"
fi

# The version the binary must report: the requested one, or the latest release's.
expect=$version
tagged=
if [ -n "${POTLUCK_RELEASES_URL:-}" ]; then
    base=${POTLUCK_RELEASES_URL%/}
else
    github=${POTLUCK_GITHUB_URL:-https://github.com}
    root=${github%/}/$REPO/releases
    if [ -n "$version" ]; then
        tag=v$version
    else
        # Resolve "latest" to its tag first, so SHA256SUMS and the binary come from one release
        # even if another is published meanwhile.
        location=$(redirect_of "$root/latest" 2>/dev/null || true)
        tag=${location##*/tag/}
        tag=${tag%%[?#]*}
        case $location in
            */tag/v[0-9]*) ;;
            *) die "couldn't find the latest release: $root/latest leads to '$location', not a release tag" ;;
        esac
        case ${tag#v} in
            *[!0-9A-Za-z.+-]*) die "couldn't find the latest release: unexpected tag '$tag' at $location" ;;
        esac
        expect=${tag#v}
    fi
    base=$root/download/$tag
    tagged=1
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/potluck-install.XXXXXX") || die "can't create a temporary directory"
staged=
trap 'rm -rf "$tmp"; if [ -n "$staged" ]; then rm -f "$staged"; fi' EXIT
trap 'exit 1' HUP INT TERM

fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS" || die "couldn't download $base/SHA256SUMS"
expected=$(tr -d '\r' <"$tmp/SHA256SUMS" | awk -v f="$asset" '$2 == f || $2 == "*" f { print $1; exit }' | tr 'A-F' 'a-f')
[ -n "$expected" ] || die "$base/SHA256SUMS has no line for $asset"

fetch "$base/$asset" "$tmp/$asset" || die "couldn't download $base/$asset"
actual=$(sha256 "$tmp/$asset")
if [ "$actual" != "$expected" ]; then
    race=
    if [ -z "$tagged" ]; then
        race=" (if a release was published there between the two downloads, run it again)"
    fi
    die "$base/$asset has SHA-256 $actual, but SHA256SUMS lists $expected; refusing it, nothing was installed$race"
fi

# Write the new binary beside the old one, check that it runs there, and rename it over, so a
# running potluck keeps its file. (Checking it in the install directory, not the temporary one,
# also works where the temporary directory is mounted noexec.)
mkdir -p "$dir" || die "can't create $dir"
[ ! -d "$dir/potluck" ] || die "$dir/potluck is a directory"
# A symlink stays one: the file it leads to is replaced, as `potluck self-update` does.
dest=$dir/potluck
links=0
while [ -L "$dest" ]; do
    links=$((links + 1))
    [ "$links" -le 40 ] || die "$dir/potluck: too many levels of symbolic links"
    link=$(readlink "$dest") || die "can't read the symbolic link $dest"
    case $link in
        /*) dest=$link ;;
        *) dest=$(dirname "$dest")/$link ;;
    esac
done
if [ "$links" -gt 0 ]; then
    real_dir=$(cd "$(dirname "$dest")" 2>/dev/null && pwd -P) || die "$dir/potluck links to $dest, whose directory doesn't exist"
    dest=$real_dir/$(basename "$dest")
fi
staged=$(dirname "$dest")/.potluck.install-$$
cp "$tmp/$asset" "$staged" || die "can't write to $(dirname "$dest")"
chmod 755 "$staged"
if [ "$os" = Darwin ]; then
    xattr -d com.apple.quarantine "$staged" 2>/dev/null || true
fi
reported=$("$staged" --version 2>/dev/null) || die "the downloaded $asset doesn't run on this machine; nothing was installed"
case $reported in
    "potluck "*) ;;
    *) die "the downloaded $asset printed '$reported' for --version; nothing was installed" ;;
esac
new_version=${reported#potluck }
new_version=${new_version%%[!0-9A-Za-z.+-]*}
# Build metadata (+...) doesn't make another version.
if [ -n "$version" ] && [ "${new_version%%+*}" != "${version%%+*}" ]; then
    die "$base has potluck $new_version, not the requested $version; nothing was installed"
elif [ -n "$expect" ] && [ "${new_version%%+*}" != "${expect%%+*}" ]; then
    die "$base has potluck $new_version, not $expect as its tag says; nothing was installed"
fi
mv -f "$staged" "$dest" || die "can't replace $dest"
staged=
if [ "$dest" = "$dir/potluck" ]; then
    say "Installed potluck $new_version to $dir/potluck."
else
    say "Installed potluck $new_version to $dest ($dir/potluck links to it)."
fi

# The daemon's service: systemd user unit potluck.service, LaunchAgent potluck.daemon, and the
# program it runs (`potluck service install` writes the executable it ran from, symlinks resolved).
service=none
restart=
program=
case $os in
    Linux)
        # systemctl --user finds the user manager through XDG_RUNTIME_DIR, which a shell with no
        # login session (su, cron) lacks: then it is the user manager's own, as for potluck.
        XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}
        export XDG_RUNTIME_DIR
        unit=${POTLUCK_SERVICE_NAME:-potluck.service}
        case $unit in *.service) ;; *) unit=$unit.service ;; esac
        if command -v systemctl >/dev/null 2>&1; then
            load=$(systemctl --user show --property=LoadState --value "$unit" 2>/dev/null || true)
            if [ -n "$load" ] && [ "$load" != not-found ]; then
                service=stopped
                program=$(systemctl --user show --property=ExecStart --value "$unit" 2>/dev/null |
                    sed -n 's/^{ path=\(.*\) ; argv\[\]=.*/\1/p' | head -n 1 || true)
                if systemctl --user is-active --quiet "$unit" 2>/dev/null; then
                    service=running
                    restart="systemctl --user restart $unit"
                fi
            fi
        fi
        ;;
    Darwin)
        uid=$(id -u)
        label=${POTLUCK_SERVICE_NAME:-potluck.daemon}
        plist=${POTLUCK_SERVICE_DIR:-${HOME:-}/Library/LaunchAgents}/$label.plist
        if [ -f "$plist" ]; then
            program=$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments:0' "$plist" 2>/dev/null || true)
        fi
        if launchctl print "gui/$uid/$label" >/dev/null 2>&1; then
            service=running
            restart="launchctl kickstart -k gui/$uid/$label"
        elif [ -f "$plist" ]; then
            service=stopped
        fi
        ;;
esac
# A service installed from another potluck keeps running that one: restarting it would not run
# this version.
if [ "$service" != none ] && [ -n "$program" ] && ! [ "$program" -ef "$dir/potluck" ]; then
    say "The potluck service runs $program, not $dir/potluck, so it was not restarted. To make it run $dir/potluck: $dir/potluck service install"
    service=other
fi
case $service in
    running)
        if [ "${POTLUCK_NO_RESTART:-}" = 1 ]; then
            say "The potluck service still runs the old binary; restart it with: $dir/potluck restart"
        else
            # The daemon restarts in place on the new binary (`potluck restart`). Exit 4: no
            # daemon here can (none answers, or it predates `restart`): the service is restarted.
            set +e
            said=$("$dir/potluck" restart 2>&1)
            code=$?
            set -e
            if [ "$code" = 0 ]; then
                say "$said"
            elif [ "$code" != 4 ]; then
                die "installed, but the daemon did not restart in place: $said"
            elif $restart; then
                say "Restarted the potluck service ($restart)."
            else
                die "installed, but restarting the potluck service failed; restart it with: $restart"
            fi
        fi
        ;;
    stopped)
        say "The potluck service is installed but not running; it will run the new binary when it starts."
        ;;
esac
# A daemon running here without the service (`potluck daemon` in the foreground) restarts in place
# on the new binary too, as with `potluck self-update --here`. Exit 4: no daemon answers here (or
# one from before `potluck restart`, which only a restart by hand can update). Without HOME (the
# install directory given by POTLUCK_INSTALL_DIR) potluck finds no daemon: none is asked.
if { [ "$service" = none ] || [ "$service" = stopped ]; } && [ "${POTLUCK_NO_RESTART:-}" != 1 ] &&
    [ -n "${HOME:-}" ]; then
    set +e
    said=$("$dir/potluck" restart 2>&1)
    code=$?
    set -e
    if [ "$code" = 0 ]; then
        say "$said"
    elif [ "$code" != 4 ]; then
        die "installed, but the potluck daemon running here did not restart in place: $said"
    fi
fi

found=$(command -v potluck 2>/dev/null || true)
case :$PATH: in
    *":$dir:"*)
        if [ -n "$found" ] && [ "$found" != "$dir/potluck" ]; then
            say "Note: $found comes before $dir/potluck on your PATH."
        fi
        ;;
    *) say "Note: $dir is not on your PATH; add it in your shell's login profile: export PATH=\"$dir:\$PATH\"" ;;
esac

if [ "$service" = none ]; then
    say ""
    say "Next steps:"
    if [ "$os" = Linux ]; then
        user=${USER:-$(id -un)}
        linger=$(loginctl show-user "$user" --property=Linger --value 2>/dev/null || true)
        if [ "$linger" != yes ]; then
            say "  sudo loginctl enable-linger $user    (the daemon is a systemd user service; it needs lingering)"
        fi
    fi
    say "  potluck service install    (runs the daemon as a service)"
    say "  potluck pair               (on a machine joining a cluster: prints a line for \`potluck admit\` on a member)"
fi
