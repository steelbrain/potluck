#!/bin/sh
# Prints each failed test in `cargo test` output (the file $1) as a GitHub Actions error
# annotation: its name as the title, and its output from the failure block without the stack
# backtrace, at most 30 lines. A run's annotations can be read without signing in to GitHub, and
# its log can't.
awk '
function esc(s) { gsub(/%/, "%25", s); gsub(/\r/, "", s); return s }
function flush(  t, m) {
  if (name != "") {
    t = esc(name); gsub(/:/, "%3A", t); gsub(/,/, "%2C", t)
    m = esc(msg); gsub(/\n/, "%0A", m)
    printf "::error title=%s::%s\n", t, m
  }
  name = ""; msg = ""; n = 0; skip = 0
}
/^---- .* stdout ----\r?$/ { flush(); name = $2; next }
/^failures:\r?$/ || /^test result:/ { flush(); next }
name == "" { next }
/^stack backtrace:/ { skip = 1; next }
skip && /^ *([0-9]+: |at )/ { next }
skip && /^note: / { skip = 0 }
n < 30 { msg = msg $0 "\n"; n++ }
END { flush() }
' "$1"
