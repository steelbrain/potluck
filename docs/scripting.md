# Scripting

In a script, `--json` prints one JSON document on stdout:

```sh
job=$(potluck dispatch --json --timeout 10m -- cargo test | jq -r .job)
potluck wait "$job"
```

`dispatch --json` has the job's id (`job`), whether `--key` named a job that already existed
(`existing`), its `state`, its `workspace` once it has one, the dispatched `commit` and
`branch`, the submodules left out (`without_submodules`) and the repositories carried
(`carried`). Each entry of `carried` has the `name` of its `POTLUCK_MIRROR_<name>` variable, the
full `commit` and the `source`, the repository's git directory on the dispatching machine:

```console
$ potluck dispatch --json --timeout 10m --carry ../dep -- cargo test | jq -c .carried
[{"commit":"b01aec5cbe2b49c1f0f7a3c6e9d1e4b2a8c7f610","name":"dep","source":"/Users/me/src/dep/.git"}]
```

`resize --json` has the job's id (`job`), its RAM request in MiB now and before (`ram_mib`,
`from_ram_mib`), whether it changed (`changed`, false when the job asked for that amount already)
and the job's `record`:

```console
$ potluck resize --json studio-41 --ram 2 | jq -c '[.changed, .ram_mib, .from_ram_mib]'
[true,2048,16384]
```

## Output and exit codes

Without `--json`, results go to stdout, and errors and reminders to stderr. With `--json`, every
command prints one JSON document on stdout, errors included
(`{"error": "...", "reminders": [...]}`), and `wait --json` sends the job's output to stderr.
`logs --json` writes its document as the output arrives; an error after it has begun ends the
document with an `"error"` field and exit code 2. Output that can't be written (a full disk) is an
error said on stderr, with exit code 2, also when it is a JSON error document; a pipe closed
early (`potluck logs J | head`) is not an error.
potluck's own errors, refused dispatches and resizes included, exit with 2. `wait --max-wait DUR`
exits with 3 if the job hasn't ended by then; the job keeps running, and `"ended": false` in the
JSON tells this apart from a job that exited with 3. `potluck restart` exits with 4 when no daemon
here can restart in place: none answers, or it runs a potluck from before `restart` (install.sh
then restarts the service). `restart` never exits with a job's code, so its 4 is always its own.

`potluck env --json` prints the machine's name and its login environment, with the values as they
are (the text output writes control characters in a name or value as escapes, such as `\n`, so
each variable stays on one line):

```json
{"env": {"HOME": "/Users/me", "PATH": "/opt/homebrew/bin:/usr/bin:/bin"}, "node": "studio", "reminders": []}
```

A job's own exit codes, and the codes potluck gives the endings it causes, are in
[How a job ends](jobs.md#how-a-job-ends).
