//! Which machines can run a job now. potluck is up front about jobs it can't run:
//!
//! - A dispatch counts only machines that are up: this one, and the members the mesh sees alive,
//!   on this machine's version or another (one of the two is updating: the job is queued and
//!   starts once they run one version). When none of them can run the job, it is refused, naming
//!   the members that could run it and are not up ("no machine that can run this is up (msi:
//!   down since 01:12)").
//! - A queued job whose every capable machine is lost (down for the mesh's down time, or removed)
//!   ends as lost, exit 125 ("no machine that can run it is up (msi was lost)"). While a capable
//!   member is down for less than that, or up on another version (one of the two updates), the job
//!   waits.
//! - A running job whose runner is lost is queued again only when some machine that is up can run
//!   it; otherwise it ends the same way.
//!
//! A member's machine is known from its latest report, or, after a restart of this daemon, from
//! the facts it last reported (kept in the store). A member up whose facts aren't known yet counts
//! as able to run anything.

use super::views::snap_of;
use super::{CState, Cluster};
use crate::daemon::Shared;
use crate::daemon::state::{MachineSnap, State};
use crate::sched::Queued;
use crate::types::{Facts, Millis, NodeName};
use serde::{Deserialize, Serialize};

/// A member's machine as it last reported it, kept across restarts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Known {
    pub facts: Facts,
    pub ram_capacity_mib: u64,
}

impl Known {
    fn snap(&self, pool: &str) -> MachineSnap {
        MachineSnap {
            facts: self.facts.clone(),
            pool: pool.to_string(),
            ram_capacity_mib: self.ram_capacity_mib,
            running: Vec::new(),
        }
    }
}

/// Why a member isn't up.
pub(crate) enum Away {
    /// Silent since `since` (this machine's clock, if it was ever heard from); `lost` once silent
    /// for the down time.
    Down { since: Option<Millis>, lost: bool },
    /// Alive, on another version (`theirs`, `ours`): the two refuse each other until one of them
    /// updates.
    Version { theirs: String, ours: String },
    /// Up, but its facts haven't reached this machine yet.
    Unknown,
    /// No longer a member.
    Left,
}

/// A member that isn't up, or whose machine isn't known.
pub(crate) struct Other {
    pub node: NodeName,
    /// Its machine as last known (None: it never reported to this machine).
    pub snap: Option<MachineSnap>,
    pub away: Away,
}

impl Other {
    pub fn lost(&self) -> bool {
        matches!(self.away, Away::Down { lost: true, .. } | Away::Left)
    }

    /// Alive on another version: one of the two is updating.
    pub fn updating(&self) -> bool {
        matches!(self.away, Away::Version { .. })
    }

    /// "down since 01:12", "runs potluck 0.1.1", ...
    pub fn state(&self) -> String {
        match &self.away {
            Away::Down { since: Some(t), .. } => format!("down since {}", clock(*t)),
            Away::Down { since: None, .. } => "down".into(),
            Away::Version { theirs, ours } => {
                format!("runs potluck {theirs}, not this machine's {ours}")
            }
            Away::Unknown => "up, its facts not known here yet".into(),
            Away::Left => "left the cluster".into(),
        }
    }

    /// For `why`: "is down since 01:12", "left the cluster", ...
    pub fn why_state(&self) -> String {
        match self.away {
            Away::Down { .. } => format!("is {}", self.state()),
            _ => self.state(),
        }
    }

    /// What became of it, for a job that ends: "msi was lost", "msi left the cluster".
    fn fate(&self) -> String {
        match self.away {
            Away::Left => format!("{} left the cluster", self.node),
            _ => format!("{} was lost", self.node),
        }
    }
}

/// The machines that are up (this one first) and the members that aren't.
pub(crate) struct Machines {
    pub up: Vec<(NodeName, MachineSnap)>,
    pub others: Vec<Other>,
    /// This machine was removed from its cluster: it is alone.
    pub out: bool,
}

/// What can become of a job, by the machines that could run it.
pub(crate) enum Verdict {
    /// A machine that is up can run it (or might: its facts aren't known yet).
    Up,
    /// None that is up can; some capable member isn't lost yet.
    Waits,
    /// No machine that can run it is up, nor will be: why, for the job's message.
    Lost(String),
}

/// The machines as this machine sees them now. `cluster` is None on one machine.
pub(crate) fn machines(st: &State, cluster: Option<(&Cluster, &CState)>) -> Machines {
    let pool_here = match cluster {
        Some((c, _)) => c.pool_here(),
        None => crate::mesh::trust::DEFAULT_POOL.to_string(),
    };
    let mut up: Vec<(NodeName, MachineSnap)> = st
        .local_snap(&pool_here)
        .map(|s| (st.node.clone(), s))
        .into_iter()
        .collect();
    let mut others = Vec::new();
    let Some((c, cs)) = cluster else {
        return Machines {
            up,
            others,
            out: false,
        };
    };
    if cs.out.is_some() {
        return Machines {
            up,
            others,
            out: true,
        };
    }
    // A member the mesh now running hasn't heard from is lost only once that mesh has run for the
    // down time: a mesh started a moment ago (the port came free, say) hasn't reached anyone yet.
    let warm = c.boot().saturating_sub(c.mesh_started_at()) >= c.down_after_ms();
    let members = c.mesh().members();
    for m in &members {
        if m.this_machine {
            continue;
        }
        let snap = cs
            .reports
            .get(&m.name)
            .map(|r| snap_of(&r.report, &m.pool))
            .or_else(|| cs.known.get(&m.name).map(|k| k.snap(&m.pool)));
        let away = if c.up(m) {
            match snap {
                Some(s) => {
                    up.push((m.name.clone(), s));
                    continue;
                }
                None => Away::Unknown,
            }
        } else if m.alive {
            Away::Version {
                theirs: m.version.clone().unwrap_or_default(),
                ours: c.opts.mesh.version.clone(),
            }
        } else {
            Away::Down {
                since: m.last_seen,
                // Down for the down time: the mesh said so, or nothing was heard from it since
                // the mesh now running started that long ago.
                lost: cs.lost.contains(&m.name) || warm,
            }
        };
        others.push(Other {
            node: m.name.clone(),
            snap,
            away,
        });
    }
    for (n, k) in &cs.known {
        if *n != st.node && !members.iter().any(|m| m.name == *n) {
            others.push(Other {
                node: n.clone(),
                // Its pool went with its membership.
                snap: Some(k.snap(crate::mesh::trust::DEFAULT_POOL)),
                away: Away::Left,
            });
        }
    }
    Machines {
        up,
        others,
        out: false,
    }
}

impl Machines {
    /// Whether some machine that is up can run `q`; else each up machine's reason.
    pub fn up_can_run(&self, sh: &Shared, q: &Queued) -> Result<(), Vec<String>> {
        let mut reasons = Vec::new();
        for (name, snap) in &self.up {
            match sh.parts.scheduler.feasible(&snap.view(), q) {
                Ok(()) => return Ok(()),
                Err(r) => reasons.push(format!("{name}: {r}")),
            }
        }
        if self.others.iter().any(|o| matches!(o.away, Away::Unknown)) {
            return Ok(());
        }
        Err(reasons)
    }

    /// The members not up that could run `q` (those never heard from might).
    pub fn capable_others<'m>(&'m self, sh: &Shared, q: &Queued) -> Vec<&'m Other> {
        self.others
            .iter()
            .filter(|o| {
                o.snap
                    .as_ref()
                    .is_none_or(|s| sh.parts.scheduler.feasible(&s.view(), q).is_ok())
            })
            .collect()
    }

    /// What can become of `q`.
    pub fn verdict(&self, sh: &Shared, q: &Queued) -> Verdict {
        if self.up_can_run(sh, q).is_ok() {
            return Verdict::Up;
        }
        let capable = self.capable_others(sh, q);
        if capable.iter().any(|o| !o.lost()) {
            return Verdict::Waits;
        }
        let mut why: Vec<String> = capable.iter().map(|o| o.fate()).collect();
        if self.out {
            why.push("this machine was removed from the cluster".into());
        }
        Verdict::Lost(if why.is_empty() {
            "no machine that can run it is up".into()
        } else {
            format!("no machine that can run it is up ({})", why.join("; "))
        })
    }
}

/// A time of day on this machine's clock, `HH:MM`, with the date when not today.
pub(crate) fn clock(ms: Millis) -> String {
    let secs = (ms / 1000) as libc::time_t;
    let now = (super::peer::now() / 1000) as libc::time_t;
    // SAFETY: localtime_r writes into the tm we own; a zeroed tm is a valid value for it.
    let (t, today) = unsafe {
        let mut t: libc::tm = std::mem::zeroed();
        let mut n: libc::tm = std::mem::zeroed();
        libc::localtime_r(&secs, &mut t);
        libc::localtime_r(&now, &mut n);
        (t, t.tm_year == n.tm_year && t.tm_yday == n.tm_yday)
    };
    if today {
        format!("{:02}:{:02}", t.tm_hour, t.tm_min)
    } else {
        format!(
            "{}-{:02}-{:02} {:02}:{:02}",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_reads_hours_and_minutes() {
        let now = super::super::peer::now();
        let c = clock(now);
        assert_eq!(c.len(), 5, "{c}");
        assert_eq!(&c[2..3], ":");
        let old = clock(now - 3 * 86_400_000);
        assert_eq!(old.len(), 16, "{old}");
    }
}
