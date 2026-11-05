# Scripting

In a script, `--json` prints one JSON document on stdout:

```sh
job=$(potluck dispatch --json --timeout 10m -- cargo test | jq -r .job)
potluck wait "$job"
```

## Output and exit codes

Without `--json`, results go to stdout, and errors and reminders to stderr. With `--json`, every
command prints one JSON document on stdout, errors included
(`{"error": "...", "reminders": [...]}`), and `wait --json` sends the job's output to stderr.
`logs --json` writes its document as the output arrives; an error after it has begun ends the
document with an `"error"` field and exit code 2. Output that can't be written (a full disk) is an
error said on stderr, with exit code 2, also when it is a JSON error document; a pipe closed
early (`potluck logs J | head`) is not an error.
potluck's own errors, refused dispatches included, exit with 2. `wait --max-wait DUR` exits with 3
if the job hasn't ended by then; the job keeps running, and `"ended": false` in the JSON tells this
apart from a job that exited with 3. `potluck restart` exits with 4 when no daemon here can restart
in place: none answers, or it runs a potluck from before `restart` (install.sh then restarts the
service). `restart` never exits with a job's code, so its 4 is always its own.

`potluck env --json` prints the machine's name and its login environment, with the values as they
are (the text output writes control characters in a name or value as escapes, such as `\n`, so
each variable stays on one line):

```json
{"env": {"HOME": "/Users/me", "PATH": "/opt/homebrew/bin:/usr/bin:/bin"}, "node": "studio", "reminders": []}
```

A job's own exit codes, and the codes potluck gives the endings it causes, are in
[How a job ends](jobs.md#how-a-job-ends).
