#!/bin/sh
# Prints each failed test in `cargo test` output (the file $1) as a GitHub Actions error
# annotation: its name as the title, and its output from the failure block without the stack
# backtrace: its first 8 lines and its last 22 (where an assertion's message ends). A run's annotations can be read without signing in to GitHub, and
# its log can't.
awk '
function esc(s) { gsub(/%/, "%25", s); gsub(/\r/, "", s); return s }
function flush(  t, m, i) {
  if (name != "") {
    t = esc(name); gsub(/:/, "%3A", t); gsub(/,/, "%2C", t)
    msg = ""
    for (i = 1; i <= n; i++) {
      if (i <= 8 || i > n - 22) msg = msg line[i] "\n"
      else if (i == 9) msg = msg "[... " (n - 30) " lines ...]\n"
    }
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
{ line[++n] = $0 }
END { flush() }
' "$1"
