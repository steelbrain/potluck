//! The workspace lifecycle policy: pure functions over workspace records.

use crate::types::{
    MAX_HOLD_MS, MIN_HOLD_MS, Millis, Reminder, WorkspaceId, WorkspaceRecord, WorkspaceState,
};
use std::collections::HashMap;

/// What the lifecycle policy decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// The hold is over.
    MakeIdle(WorkspaceId),
    Remove(WorkspaceId),
}

/// The lifecycle policy: pure, over this machine's workspace records.
///
/// - A held workspace whose hold is over (`held_until <= now`) becomes idle, also with work
///   queued into it (which keeps it from being removed).
/// - The newest idle workspace (by `last_used_at`) of each repository is kept, counting those
///   becoming idle now. Not for a repository id from before ids were salted with the owner
///   ([`crate::types::is_unsalted_repo_id`]): its workspaces get no newer ones (new dispatches
///   use the salted id), so they are removed like any older idle workspace.
/// - While the live workspaces (every state but removed; unknown sizes count 0) exceed
///   `budget_bytes`, older idle ones are removed, oldest first. Never one with `pending > 0`, and
///   never one that is seeding, running or held at the start of this tick.
pub fn tick(now: Millis, records: &[WorkspaceRecord], budget_bytes: u64) -> Vec<Action> {
    let mut actions = Vec::new();
    let mut idle: Vec<&WorkspaceRecord> = Vec::new();
    for r in records {
        match r.state {
            WorkspaceState::Held if r.held_until <= now => {
                actions.push(Action::MakeIdle(r.id.clone()));
                idle.push(r);
            }
            WorkspaceState::Idle => idle.push(r),
            _ => {}
        }
    }
    let mut total: u64 = records
        .iter()
        .filter(|r| r.state != WorkspaceState::Removed)
        .map(|r| r.size_bytes.unwrap_or(0))
        .sum();
    if total <= budget_bytes {
        return actions;
    }
    let age = |r: &WorkspaceRecord| (r.last_used_at, r.created_at, r.id.clone());
    let mut newest: HashMap<&str, &WorkspaceRecord> = HashMap::new();
    for r in &idle {
        let e = newest.entry(r.repo_id.as_str()).or_insert(r);
        if age(r) > age(e) {
            *e = r;
        }
    }
    let mut candidates: Vec<&WorkspaceRecord> = idle
        .iter()
        .copied()
        .filter(|r| {
            r.state == WorkspaceState::Idle
                && r.pending == 0
                && (crate::types::is_unsalted_repo_id(&r.repo_id)
                    || newest.get(r.repo_id.as_str()).map(|n| n.id != r.id) == Some(true))
        })
        .collect();
    candidates.sort_by_key(|r| age(r));
    for r in candidates {
        if total <= budget_bytes {
            break;
        }
        actions.push(Action::Remove(r.id.clone()));
        total = total.saturating_sub(r.size_bytes.unwrap_or(0));
    }
    actions
}

/// When a workspace's hold should end after an invocation that ended at `ended_at`, given the hold
/// it asked for and the hold already in place: `max(current, ended_at + max(5 min, min(requested,
/// 24 h)))`. A hold never shrinks.
pub fn hold_until(ended_at: Millis, requested_ms: Option<u64>, current: Millis) -> Millis {
    let hold = requested_ms.unwrap_or(0).clamp(MIN_HOLD_MS, MAX_HOLD_MS);
    current.max(ended_at.saturating_add(hold))
}

/// The reminders for `branch` (of `repo_id`) among `records`: its workspaces with a hold that
/// isn't over, with the time left, soonest to end first. That is every held one, and one a job
/// runs in that `potluck hold` (or the hold after the job before) holds beyond now. A workspace
/// whose hold is over gets none, even before the loop makes it idle.
pub fn reminders(
    now: Millis,
    records: &[WorkspaceRecord],
    repo_id: &str,
    branch: &str,
) -> Vec<Reminder> {
    let mut held: Vec<&WorkspaceRecord> = records
        .iter()
        .filter(|r| {
            r.state != WorkspaceState::Removed
                && r.held_until > now
                && r.repo_id == repo_id
                && r.branch.as_deref() == Some(branch)
        })
        .collect();
    held.sort_by(|a, b| a.held_until.cmp(&b.held_until).then(a.id.cmp(&b.id)));
    held.into_iter()
        .map(|r| Reminder {
            workspace: r.id.clone(),
            node: r.node.clone(),
            commit: r.commit.clone(),
            state: r.state,
            remaining_ms: r.held_until.saturating_sub(now),
            size_bytes: r.size_bytes,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{HOUR_MS, MINUTE_MS};
    use std::path::PathBuf;

    const GB: u64 = 1 << 30;

    fn ws(
        id: &str,
        repo: &str,
        state: WorkspaceState,
        last_used_at: Millis,
        size_gb: u64,
    ) -> WorkspaceRecord {
        WorkspaceRecord {
            id: id.into(),
            node: "m".into(),
            repo_id: repo.into(),
            commit: "c".repeat(40),
            branch: Some("agent-a".into()),
            path: PathBuf::from(format!("/w/{id}")),
            state,
            created_at: last_used_at,
            last_used_at,
            held_until: 0,
            pending: 0,
            size_bytes: Some(size_gb * GB),
            advanced_by: None,
        }
    }

    #[test]
    fn hold_rule() {
        let t = 1_000 * HOUR_MS;
        // (requested, current, expected)
        let cases = [
            (None, 0, t + 5 * MINUTE_MS),
            (Some(MINUTE_MS), 0, t + 5 * MINUTE_MS),
            (Some(2 * HOUR_MS), 0, t + 2 * HOUR_MS),
            (Some(48 * HOUR_MS), 0, t + 24 * HOUR_MS),
            (None, t + HOUR_MS, t + HOUR_MS),
            (Some(30 * MINUTE_MS), t + HOUR_MS, t + HOUR_MS),
            (Some(3 * HOUR_MS), t + HOUR_MS, t + 3 * HOUR_MS),
        ];
        for (requested, current, want) in cases {
            assert_eq!(
                hold_until(t, requested, current),
                want,
                "{requested:?} {current}"
            );
        }
        assert_eq!(hold_until(u64::MAX - 1, Some(HOUR_MS), 0), u64::MAX);
    }

    #[test]
    fn holds_end() {
        use WorkspaceState::*;
        let now = 100 * HOUR_MS;
        let mut over = ws("w1", "r", Held, 1, 1);
        over.held_until = now;
        let mut later = ws("w2", "r", Held, 1, 1);
        later.held_until = now + 1;
        // Work queued into it doesn't keep it held: the hold is over (it isn't removed while
        // anything is queued into it).
        let mut queued = ws("w3", "r", Held, 1, 1);
        queued.held_until = now - 1;
        queued.pending = 1;
        let running = ws("w4", "r", Running, 1, 1);
        let actions = tick(now, &[over, later, queued, running], 100 * GB);
        assert_eq!(
            actions,
            vec![Action::MakeIdle("w1".into()), Action::MakeIdle("w3".into())]
        );
    }

    #[test]
    fn removal_table() {
        use WorkspaceState::*;
        let now = 100 * HOUR_MS;
        struct Case {
            name: &'static str,
            records: Vec<WorkspaceRecord>,
            budget_gb: u64,
            want: Vec<Action>,
        }
        let rm = |id: &str| Action::Remove(id.into());
        let idle = |id: &str| Action::MakeIdle(id.into());
        let pending = |mut r: WorkspaceRecord| {
            r.pending = 1;
            r
        };
        let held_over = |mut r: WorkspaceRecord| {
            r.held_until = now - 1;
            r
        };
        let held_on = |mut r: WorkspaceRecord| {
            r.held_until = now + HOUR_MS;
            r
        };
        let cases = vec![
            Case {
                name: "under budget: nothing removed",
                records: vec![ws("a", "r", Idle, 1, 1), ws("b", "r", Idle, 2, 1)],
                budget_gb: 2,
                want: vec![],
            },
            Case {
                name: "newest idle per repo kept, older removed oldest first",
                records: vec![
                    ws("a", "r", Idle, 3, 10),
                    ws("b", "r", Idle, 1, 10),
                    ws("c", "r", Idle, 2, 10),
                    ws("d", "r", Idle, 9, 10),
                ],
                budget_gb: 15,
                want: vec![rm("b"), rm("c"), rm("a")],
            },
            Case {
                name: "stops once under budget",
                records: vec![
                    ws("a", "r", Idle, 3, 10),
                    ws("b", "r", Idle, 1, 10),
                    ws("c", "r", Idle, 2, 10),
                    ws("d", "r", Idle, 9, 10),
                ],
                budget_gb: 30,
                want: vec![rm("b")],
            },
            Case {
                name: "each repository keeps its newest",
                records: vec![
                    ws("a1", "a", Idle, 1, 10),
                    ws("b1", "b", Idle, 2, 10),
                    ws("a2", "a", Idle, 3, 10),
                ],
                budget_gb: 0,
                want: vec![rm("a1")],
            },
            Case {
                name: "never pending, running, seeding or held; their size still counts",
                records: vec![
                    pending(ws("p", "r", Idle, 1, 10)),
                    ws("run", "r", Running, 2, 10),
                    ws("seed", "r", Seeding, 2, 10),
                    held_on(ws("h", "r", Held, 2, 10)),
                    ws("old", "r", Idle, 3, 10),
                    ws("new", "r", Idle, 4, 10),
                ],
                budget_gb: 0,
                want: vec![rm("old")],
            },
            Case {
                name: "a pending idle one can be the newest that is kept",
                records: vec![
                    pending(ws("p", "r", Idle, 5, 10)),
                    ws("old", "r", Idle, 3, 10),
                ],
                budget_gb: 0,
                want: vec![rm("old")],
            },
            Case {
                name: "one becoming idle now counts as the newest but isn't removed this tick",
                records: vec![
                    held_over(ws("h", "r", Held, 5, 10)),
                    ws("old", "r", Idle, 3, 10),
                    ws("older", "r", Idle, 1, 10),
                ],
                budget_gb: 0,
                want: vec![idle("h"), rm("older"), rm("old")],
            },
            Case {
                name: "a repository id from before salting keeps no newest workspace",
                records: vec![
                    ws("old", "app-0123abcd", Idle, 1, 10),
                    ws("new", "app-0123abcd", Idle, 2, 10),
                    ws("salted", "app-0123456789abcdef", Idle, 3, 10),
                ],
                budget_gb: 0,
                want: vec![rm("old"), rm("new")],
            },
            Case {
                name: "removed records don't count",
                records: vec![
                    ws("gone", "r", Removed, 1, 100),
                    ws("a", "r", Idle, 2, 1),
                    ws("b", "r", Idle, 3, 1),
                ],
                budget_gb: 2,
                want: vec![],
            },
            Case {
                name: "unknown sizes count zero",
                records: vec![
                    WorkspaceRecord {
                        size_bytes: None,
                        ..ws("a", "r", Idle, 1, 0)
                    },
                    ws("b", "r", Idle, 3, 1),
                ],
                budget_gb: 0,
                want: vec![rm("a")],
            },
        ];
        for c in cases {
            assert_eq!(
                tick(now, &c.records, c.budget_gb * GB),
                c.want,
                "{}",
                c.name
            );
        }
    }

    #[test]
    fn reminders_list_held_only() {
        use WorkspaceState::*;
        let now = 100 * HOUR_MS;
        let mut h1 = ws("h1", "r", Held, 1, 2);
        h1.held_until = now + 10 * MINUTE_MS;
        let mut h2 = ws("h2", "r", Held, 1, 2);
        h2.held_until = now + 2 * MINUTE_MS;
        let mut other_branch = ws("o", "r", Held, 1, 2);
        other_branch.branch = Some("agent-b".into());
        other_branch.held_until = now + MINUTE_MS;
        let mut other_repo = ws("x", "s", Held, 1, 2);
        other_repo.held_until = now + MINUTE_MS;
        let idle = ws("i", "r", Idle, 1, 2);
        let running = ws("run", "r", Running, 1, 2);
        // Held by `potluck hold` while a job runs in it: it is held, and says so.
        let mut running_held = ws("rh", "r", Running, 1, 2);
        running_held.held_until = now + 30 * MINUTE_MS;
        // A hold that is over, with work queued into it: no hold to remind of.
        let mut over = ws("ov", "r", Held, 1, 2);
        over.held_until = now;
        over.pending = 1;
        let got = reminders(
            now,
            &[
                h1,
                h2,
                other_branch,
                other_repo,
                idle,
                running,
                running_held,
                over,
            ],
            "r",
            "agent-a",
        );
        let ids: Vec<(&str, u64)> = got
            .iter()
            .map(|r| (r.workspace.as_str(), r.remaining_ms))
            .collect();
        assert_eq!(
            ids,
            vec![
                ("h2", 2 * MINUTE_MS),
                ("h1", 10 * MINUTE_MS),
                ("rh", 30 * MINUTE_MS)
            ]
        );
        assert_eq!(got[0].size_bytes, Some(2 * GB));
        assert_eq!(got[0].state, Held);
        assert_eq!(got[2].state, Running);
    }
}
