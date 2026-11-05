#!/bin/sh
# Usage: is-latest.sh TAG < published-tags
#
# Prints "true" if release TAG (v1.2.3) should be marked the latest: it isn't a prerelease (no
# suffix after a "-"), and its version is the highest among it and the published release tags on
# stdin, one per line. Prints "false" otherwise. Tags on stdin that aren't plain versions
# (prereleases, other names) are ignored. A fix released for an older version line must not become
# "latest": machines updating to the latest release would go down to it.
set -eu
tag=${1:?usage: is-latest.sh TAG < published-tags}
case $tag in
    v[0-9]*) ;;
    *) echo false; exit 0 ;;
esac
case $tag in
    *-*) echo false; exit 0 ;;
esac
awk -v tag="$tag" '
    function norm(v) { sub(/^v/, "", v); sub(/\+.*/, "", v); return v }
    # -1, 0 or 1 as version a is lower than, equal to or higher than b.
    function cmp(a, b,    pa, pb, na, nb, n, i, x, y) {
        na = split(norm(a), pa, "."); nb = split(norm(b), pb, ".")
        n = na > nb ? na : nb
        for (i = 1; i <= n; i++) {
            x = (i <= na) ? pa[i] + 0 : 0; y = (i <= nb) ? pb[i] + 0 : 0
            if (x != y) return x < y ? -1 : 1
        }
        return 0
    }
    $0 ~ /^v[0-9]+(\.[0-9]+)*(\+.*)?$/ && cmp($0, tag) > 0 { higher = 1 }
    END { print (higher ? "false" : "true") }
'
