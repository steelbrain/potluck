//! Human output: plain left-aligned tables, durations, sizes and the reminder lines.

use crate::proto::{NodeView, StatusView, WhyView};
use crate::types::{JobState, Millis, Reminder, WorkspaceState, effective_priority, fmt_ms};
use serde_json::Value;

/// What a command produced: its exit code, its JSON document (reminders are added to it), the
/// text for stdout, and a line for stderr (human mode only).
pub struct Report {
    pub code: i32,
    pub json: Value,
    pub human: String,
    pub note: String,
    /// With `--json`, the command has already written its document to stdout, a piece at a time
    /// (`logs --json`): nothing more is printed.
    pub streamed: bool,
}

impl Report {
    pub fn new(json: Value, human: impl Into<String>) -> Report {
        Report {
            code: 0,
            json,
            human: human.into(),
            note: String::new(),
            streamed: false,
        }
    }
}

/// Writes bytes as the inside of a JSON string, a piece at a time, decoded as UTF-8 the way
/// `String::from_utf8_lossy` decodes them all at once (each invalid sequence becomes U+FFFD; one
/// cut between two pieces is put back together).
#[derive(Default)]
pub struct JsonText {
    /// The start of a sequence the last piece cut short.
    carry: Vec<u8>,
}

impl JsonText {
    pub fn write(&mut self, out: &mut dyn std::io::Write, bytes: &[u8]) -> std::io::Result<()> {
        let mut data = std::mem::take(&mut self.carry);
        data.extend_from_slice(bytes);
        let mut text = String::with_capacity(data.len());
        let mut rest = &data[..];
        loop {
            match std::str::from_utf8(rest) {
                Ok(s) => {
                    text.push_str(s);
                    break;
                }
                Err(e) => {
                    let (good, bad) = rest.split_at(e.valid_up_to());
                    text.push_str(std::str::from_utf8(good).unwrap_or_default());
                    match e.error_len() {
                        Some(n) => {
                            text.push(char::REPLACEMENT_CHARACTER);
                            rest = &bad[n..];
                        }
                        None => {
                            self.carry = bad.to_vec();
                            break;
                        }
                    }
                }
            }
        }
        write_json_inside(out, &text)
    }

    /// Ends the text: a sequence still cut short becomes U+FFFD.
    pub fn finish(&mut self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        if self.carry.is_empty() {
            return Ok(());
        }
        self.carry.clear();
        write_json_inside(out, "\u{FFFD}")
    }
}

/// `s` as a JSON string, without its quotes.
fn write_json_inside(out: &mut dyn std::io::Write, s: &str) -> std::io::Result<()> {
    let quoted = serde_json::to_string(s)?;
    out.write_all(&quoted.as_bytes()[1..quoted.len() - 1])
}

/// A plain table: a header row and left-aligned columns two spaces apart, no other decoration.
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    // A row stays one line: control characters in a cell are written as escapes.
    let escaped: Vec<Vec<String>>;
    let rows = if rows.iter().flatten().any(|c| c.contains(char::is_control)) {
        escaped = rows
            .iter()
            .map(|row| row.iter().map(|c| escape_controls(c)).collect())
            .collect();
        &escaped[..]
    } else {
        rows
    };
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
    }
    let mut s = String::new();
    let mut line = |cells: Vec<&str>| {
        let mut l = String::new();
        for (i, c) in cells.iter().enumerate() {
            if i > 0 {
                l.push_str("  ");
            }
            l.push_str(c);
            let pad = widths[i].saturating_sub(c.chars().count());
            l.extend(std::iter::repeat_n(' ', pad));
        }
        s.push_str(l.trim_end());
        s.push('\n');
    };
    line(headers.to_vec());
    for row in rows {
        line(row.iter().map(String::as_str).collect());
    }
    s
}

/// `potluck env`: one `K=V` line a variable, control characters in it written as escapes, so a
/// value can't pass for more lines (`--json` has the values as they are).
pub fn env_human(env: &std::collections::BTreeMap<String, String>) -> String {
    env.iter()
        .map(|(k, v)| format!("{}={}\n", escape_controls(k), escape_controls(v)))
        .collect()
}

/// `s` with each control character written as an escape: `\n`, `\t`, `\r`, `\x7f`, `\u{85}`.
fn escape_controls(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if c.is_control() && (c as u32) < 0x80 => {
                out.push_str(&format!("\\x{:02x}", c as u32))
            }
            c if c.is_control() => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Bytes in binary units: `512 B`, `12.0 KiB`, `3.4 MiB`, `2.1 GiB`.
pub fn fmt_bytes(b: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if b < 1024 {
        return format!("{b} B");
    }
    let mut v = b as f64 / 1024.0;
    let mut unit = 0;
    while v >= 1024.0 && unit + 1 < UNITS.len() {
        v /= 1024.0;
        unit += 1;
    }
    format!("{v:.1} {}", UNITS[unit])
}

/// MiB as GiB when whole or large, else MiB: `4 GiB`, `1.5 GiB`, `512 MiB`.
pub fn fmt_mib(mib: u64) -> String {
    if mib >= 1024 {
        let g = mib as f64 / 1024.0;
        if mib.is_multiple_of(1024) {
            format!("{} GiB", mib / 1024)
        } else {
            format!("{g:.1} GiB")
        }
    } else {
        format!("{mib} MiB")
    }
}

/// argv as one line a shell would read back, cut to `max` characters. An argument with control
/// characters (a script of several lines) is written in `$'...'` with escapes, so the line stays
/// one line.
pub fn shell_line(cmd: &[String], max: usize) -> String {
    let line = cmd
        .iter()
        .map(|a| {
            if a.chars().any(char::is_control) {
                let mut q = String::from("$'");
                for c in a.chars() {
                    match c {
                        '\n' => q.push_str("\\n"),
                        '\t' => q.push_str("\\t"),
                        '\r' => q.push_str("\\r"),
                        '\\' => q.push_str("\\\\"),
                        '\'' => q.push_str("\\'"),
                        c if c.is_control() && (c as u32) < 0x80 => {
                            q.push_str(&format!("\\x{:02x}", c as u32))
                        }
                        c if c.is_control() => q.push_str(&format!("\\u{:04x}", c as u32)),
                        c => q.push(c),
                    }
                }
                q.push('\'');
                return q;
            }
            let plain = !a.is_empty()
                && a.chars().all(|c| {
                    c.is_ascii_alphanumeric()
                        || matches!(c, '-' | '_' | '.' | '/' | ':' | '=' | ',' | '@' | '+')
                });
            if plain {
                a.clone()
            } else {
                format!("'{}'", a.replace('\'', r"'\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    if line.chars().count() > max {
        let cut: String = line.chars().take(max.saturating_sub(3)).collect();
        format!("{cut}...")
    } else {
        line
    }
}

fn or_dash(s: Option<String>) -> String {
    s.filter(|s| !s.is_empty()).unwrap_or_else(|| "-".into())
}

fn ago(now: Millis, t: Option<Millis>) -> String {
    match t {
        Some(t) => format!("{} ago", fmt_ms(now.saturating_sub(t))),
        None => "-".into(),
    }
}

/// The reminders as JSON: each with the command that releases it.
pub fn reminders_json(reminders: &[Reminder]) -> Value {
    Value::Array(
        reminders
            .iter()
            .map(|r| {
                let mut v = serde_json::to_value(r).unwrap_or(Value::Null);
                if let Value::Object(m) = &mut v {
                    m.insert(
                        "release".into(),
                        Value::String(format!("potluck release {}", r.workspace)),
                    );
                }
                v
            })
            .collect(),
    )
}

/// One line per held workspace, for stderr.
pub fn reminders_human(reminders: &[Reminder]) -> String {
    let mut s = String::new();
    for r in reminders {
        let size = r.size_bytes.map(|b| format!(", {}", fmt_bytes(b)));
        let what = match r.state {
            WorkspaceState::Held => {
                format!("held for this branch for {} more", fmt_ms(r.remaining_ms))
            }
            other => format!(
                "{}, held for this branch for {} more",
                other.as_str(),
                fmt_ms(r.remaining_ms)
            ),
        };
        s.push_str(&format!(
            "potluck: workspace {} on {} is {what} (commit {}{}); `potluck release {}` ends it\n",
            r.workspace,
            r.node,
            crate::gitinfo::short(&r.commit),
            size.unwrap_or_default(),
            r.workspace,
        ));
    }
    s
}

/// A GPU id as people know it: its alias if a node config gave one.
fn gpu_label(id: &str, nodes: &[NodeView]) -> String {
    nodes
        .iter()
        .flat_map(|n| n.facts.gpus.iter())
        .find(|g| g.id == id)
        .and_then(|g| g.alias.clone())
        .unwrap_or_else(|| id.to_string())
}

fn section(s: &mut String, title: &str, headers: &[&str], rows: &[Vec<String>]) {
    if !s.is_empty() {
        s.push('\n');
    }
    if rows.is_empty() {
        s.push_str(&format!("{title}: none\n"));
    } else {
        s.push_str(title);
        s.push('\n');
        s.push_str(&table(headers, rows));
    }
}

/// A machine's UP column: a member heard from on another version is updating (its VERSION
/// column says which).
fn up_word(n: &NodeView) -> &'static str {
    match (n.alive, n.updating) {
        (true, _) => "yes",
        (false, true) => "updating",
        (false, false) => "no",
    }
}

/// `potluck status` as text.
pub fn status_human(st: &StatusView, now: Millis) -> String {
    let mut s = String::new();
    // The first section starts with a blank line when something precedes it.
    for note in &st.notes {
        s.push_str(note);
        s.push('\n');
    }
    let machines: Vec<Vec<String>> = st
        .nodes
        .iter()
        .map(|n| {
            let busy: Vec<String> = n
                .gpus_busy
                .iter()
                .map(|g| gpu_label(g, &st.nodes))
                .collect();
            vec![
                n.facts.node.clone(),
                up_word(n).into(),
                format!(
                    "{} of {}",
                    fmt_mib(n.ram_reserved_mib),
                    fmt_mib(n.ram_capacity_mib)
                ),
                n.facts.gpus.len().to_string(),
                or_dash(Some(busy.join(","))),
                or_dash(n.exclusive_job.clone()),
            ]
        })
        .collect();
    section(
        &mut s,
        "machines",
        &[
            "NODE",
            "UP",
            "RAM RESERVED",
            "GPUS",
            "LEASED",
            "EXCLUSIVE JOB",
        ],
        &machines,
    );

    let running: Vec<Vec<String>> = st
        .running
        .iter()
        .map(|j| {
            let lease = j.lease.as_ref();
            let gpus = lease
                .map(|l| {
                    l.grant
                        .gpus
                        .iter()
                        .map(|g| gpu_label(g, &st.nodes))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default();
            let mut state = j.state.to_string();
            if j.stalled {
                state.push_str(", stalled");
            }
            vec![
                j.id.clone(),
                state,
                or_dash(j.node.clone()),
                or_dash(j.workspace.clone()),
                format!(
                    "{} of {}",
                    j.started_at
                        .map(|t| fmt_ms(now.saturating_sub(t)))
                        .unwrap_or_else(|| "-".into()),
                    fmt_ms(j.spec.timeout_ms)
                ),
                format!(
                    "{} of {}",
                    fmt_bytes(j.usage.mem_bytes),
                    fmt_mib(j.spec.request.ram_mib)
                ),
                or_dash(Some(gpus)),
                // Another machine's job: only its owner and its runner are told its command.
                or_dash(Some(shell_line(&j.spec.cmd, 50))),
            ]
        })
        .collect();
    section(
        &mut s,
        "running",
        &[
            "JOB",
            "STATE",
            "NODE",
            "WORKSPACE",
            "TIME",
            "RAM",
            "GPUS",
            "COMMAND",
        ],
        &running,
    );

    let queued: Vec<Vec<String>> = st
        .queued
        .iter()
        .map(|j| {
            let eff = effective_priority(j.spec.priority, j.passes);
            let prio = if eff == j.spec.priority {
                eff.to_string()
            } else {
                format!("{eff} (from {})", j.spec.priority)
            };
            let on = crate::types::fmt_on_all(&j.spec.on);
            let mut needs = vec![fmt_mib(j.spec.request.ram_mib)];
            if !j.spec.request.gpus.is_empty() {
                needs.push(format!("gpu {}", j.spec.request.gpus.join(",")));
            }
            if j.spec.request.exclusive {
                needs.push("exclusive".into());
            }
            vec![
                j.id.clone(),
                prio,
                fmt_ms(now.saturating_sub(j.submitted_at)),
                needs.join(", "),
                or_dash(Some(on)),
                or_dash(Some(shell_line(&j.spec.cmd, 50))),
            ]
        })
        .collect();
    section(
        &mut s,
        "queued, in start order",
        &["JOB", "PRIORITY", "WAITING", "NEEDS", "ON", "COMMAND"],
        &queued,
    );

    let recent: Vec<Vec<String>> = st
        .recent
        .iter()
        .map(|j| {
            vec![
                j.id.clone(),
                j.state.to_string(),
                if j.state.is_done() {
                    j.exit_code_for_client().to_string()
                } else {
                    "-".into()
                },
                ago(now, j.ended_at),
                or_dash(j.node.clone()),
                shell_line(&j.spec.cmd, 50),
            ]
        })
        .collect();
    section(
        &mut s,
        "recent",
        &["JOB", "STATE", "EXIT", "ENDED", "NODE", "COMMAND"],
        &recent,
    );
    for j in st.recent.iter().filter(|j| !j.stuck_pids.is_empty()) {
        let pids: Vec<String> = j.stuck_pids.iter().map(u32::to_string).collect();
        s.push_str(&format!(
            "{} left processes that would not die on {}: {}\n",
            j.id,
            j.node.as_deref().unwrap_or("its machine"),
            pids.join(", ")
        ));
    }

    let workspaces: Vec<Vec<String>> = st
        .workspaces
        .iter()
        .map(|w| {
            let held = if w.state == WorkspaceState::Held {
                fmt_ms(w.held_until.saturating_sub(now))
            } else {
                "-".into()
            };
            vec![
                w.id.clone(),
                w.node.clone(),
                w.state.as_str().into(),
                held,
                w.size_bytes.map(fmt_bytes).unwrap_or_else(|| "-".into()),
                w.pending.to_string(),
                or_dash(w.branch.clone()),
                crate::gitinfo::short(&w.commit).into(),
                w.repo_id.clone(),
            ]
        })
        .collect();
    section(
        &mut s,
        "workspaces",
        &[
            "WORKSPACE",
            "NODE",
            "STATE",
            "HELD",
            "SIZE",
            "PENDING",
            "BRANCH",
            "COMMIT",
            "REPO",
        ],
        &workspaces,
    );
    s
}

/// `potluck why JOB` as text: "job studio-3 is queued", "job studio-2 timed out", then the lines.
pub fn why_human(why: &WhyView) -> String {
    let state = serde_json::from_value::<JobState>(Value::String(why.state.clone())).ok();
    let what = match state {
        Some(JobState::Succeeded) => "succeeded".to_string(),
        Some(JobState::Failed) => "failed".to_string(),
        Some(JobState::Cancelled) => "was cancelled".to_string(),
        Some(JobState::TimedOut) => "timed out".to_string(),
        Some(JobState::OverMemory) => "went over its memory limit".to_string(),
        Some(JobState::Lost) => "was lost".to_string(),
        Some(other) => format!("is {other}"),
        None => format!("is {}", why.state),
    };
    let mut s = format!("job {} {what}\n", why.job);
    for line in &why.lines {
        s.push_str(line);
        s.push('\n');
    }
    s
}

/// `potluck nodes` as text: the machines, then their GPUs.
pub fn nodes_human(nodes: &[NodeView], now: Millis) -> String {
    let machines: Vec<Vec<String>> = nodes
        .iter()
        .map(|n| {
            let f = &n.facts;
            vec![
                f.node.clone(),
                if n.alive || n.updating {
                    up_word(n).to_string()
                } else {
                    format!("no, seen {}", ago(now, n.last_seen))
                },
                n.pool.clone(),
                format!("{} {}", f.os, f.os_version),
                or_dash(Some(f.kernel.clone())),
                f.arch.clone(),
                f.cpus.to_string(),
                fmt_mib(f.ram_mib),
                if f.display { "yes" } else { "no" }.into(),
                f.version.clone(),
                or_dash(n.address.clone()),
                f.cpu_model.clone(),
            ]
        })
        .collect();
    let mut s = table(
        &[
            "NODE", "UP", "POOL", "OS", "KERNEL", "ARCH", "CPUS", "RAM", "DISPLAY", "VERSION",
            "ADDRESS", "CPU",
        ],
        &machines,
    );
    let gpus: Vec<Vec<String>> = nodes
        .iter()
        .flat_map(|n| {
            n.facts.gpus.iter().map(move |g| {
                let busy = if n.gpus_busy.contains(&g.id) {
                    "leased".to_string()
                } else {
                    "free".to_string()
                };
                vec![
                    n.facts.node.clone(),
                    g.id.clone(),
                    or_dash(g.alias.clone()),
                    g.name.clone(),
                    or_dash(g.driver.clone()),
                    busy,
                ]
            })
        })
        .collect();
    s.push('\n');
    if gpus.is_empty() {
        s.push_str("gpus: none\n");
    } else {
        s.push_str(&table(
            &["NODE", "GPU", "ALIAS", "NAME", "DRIVER", "STATE"],
            &gpus,
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_are_plain_and_left_aligned() {
        let t = table(
            &["JOB", "STATE"],
            &[
                vec!["studio-1".into(), "running".into()],
                vec!["s-2".into(), "queued".into()],
            ],
        );
        assert_eq!(t, "JOB       STATE\nstudio-1  running\ns-2       queued\n");
    }

    /// Every row stays one line: a cell's control characters (an `--on` value or GPU selector
    /// given with a line break or tab, which matching ignores as whitespace) are written as
    /// escapes, as a command's are.
    #[test]
    fn a_cell_with_a_line_break_stays_on_its_row() {
        let t = table(
            &["JOB", "ON", "NEEDS"],
            &[vec![
                "studio-6".into(),
                "macos=27\n\t".into(),
                "4 GiB, gpu apple\u{7f}\u{85}".into(),
            ]],
        );
        assert_eq!(
            t,
            "JOB       ON            NEEDS\nstudio-6  macos=27\\n\\t  4 GiB, gpu apple\\x7f\\u{85}\n"
        );
    }

    #[test]
    fn a_reminder_says_the_time_left_whatever_the_state() {
        let r = |state| Reminder {
            workspace: "studio-w5".into(),
            node: "studio".into(),
            commit: "6ad66fa4fb0123".into(),
            state,
            remaining_ms: 30 * 60_000,
            size_bytes: None,
        };
        assert_eq!(
            reminders_human(&[r(WorkspaceState::Held), r(WorkspaceState::Running)]),
            "potluck: workspace studio-w5 on studio is held for this branch for 30m more \
             (commit 6ad66fa4fb01); `potluck release studio-w5` ends it\n\
             potluck: workspace studio-w5 on studio is running, held for this branch for 30m \
             more (commit 6ad66fa4fb01); `potluck release studio-w5` ends it\n"
        );
    }

    #[test]
    fn why_names_the_state_in_words() {
        let why = |state: &str| {
            why_human(&WhyView {
                job: "studio-2".into(),
                state: state.into(),
                lines: vec![],
                blockers: vec![],
            })
        };
        assert_eq!(why("queued"), "job studio-2 is queued\n");
        assert_eq!(why("succeeded"), "job studio-2 succeeded\n");
        assert_eq!(why("timed_out"), "job studio-2 timed out\n");
        assert_eq!(
            why("over_memory"),
            "job studio-2 went over its memory limit\n"
        );
        assert_eq!(why("cancelled"), "job studio-2 was cancelled\n");
        assert_eq!(why("unknown"), "job studio-2 is unknown\n");
    }

    #[test]
    fn sizes_and_commands() {
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(2 * 1024 * 1024 + 300 * 1024), "2.3 MiB");
        assert_eq!(fmt_mib(4096), "4 GiB");
        assert_eq!(fmt_mib(1536), "1.5 GiB");
        assert_eq!(fmt_mib(512), "512 MiB");
        let cmd = vec!["sh".to_string(), "-c".into(), "echo 'hi' there".into()];
        assert_eq!(shell_line(&cmd, 80), r"sh -c 'echo '\''hi'\'' there'");
        assert_eq!(shell_line(&cmd, 8), "sh -c...");
    }

    /// A script of several lines stays on one line of a table: control characters are written
    /// as escapes in `$'...'`, which bash and zsh read back.
    #[test]
    fn a_command_with_line_breaks_stays_on_one_line() {
        let cmd = vec![
            "sh".to_string(),
            "-c".into(),
            "cargo build\ncargo test\t'x'\\\u{1b}".into(),
        ];
        let line = shell_line(&cmd, 200);
        assert_eq!(line, r"sh -c $'cargo build\ncargo test\t\'x\'\\\x1b'");
        assert!(!line.chars().any(char::is_control), "{line:?}");
    }
}
