//! `explain` for each blocker kind, and for starting, running and finished jobs.

use super::*;
use crate::sched::Running;
use crate::sched::tests::{AMD, Machine, NOW, NV, Q, amd, job, m, nv};
use crate::types::{Grant, JobSpec, Lease, MINUTE_MS, RepoRef, Request, Usage};

fn record(id: &str, state: JobState) -> JobRecord {
    JobRecord {
        id: id.into(),
        spec: JobSpec {
            key: None,
            labels: BTreeMap::new(),
            priority: 3,
            request: Request::default(),
            on: BTreeMap::new(),
            repo: RepoRef {
                id: "proj-0123abcd".into(),
                source: "/src/proj/.git".into(),
                commit: "a".repeat(40),
                branch: Some("agent-1".into()),
                submodules: Vec::new(),
            },
            workspace: None,
            cmd: vec!["true".into()],
            env: BTreeMap::new(),
            unset_env: Default::default(),
            timeout_ms: 15 * MINUTE_MS,
            hold_ms: None,
            keep: Vec::new(),
            rerun_if_lost: true,
            kill_on_stall: false,
            recipe: None,
            submitted_from: "studio".into(),
        },
        state,
        submitted_at: m(-30),
        started_at: None,
        ended_at: None,
        node: None,
        workspace: None,
        lease: None,
        handle: None,
        exit_code: None,
        signal: None,
        message: None,
        stuck_pids: Vec::new(),
        usage: Usage::default(),
        passes: 0,
        stalled: false,
        kept: Vec::new(),
        notes: Vec::new(),
    }
}

struct World {
    machines: Vec<Machine>,
    jobs: Vec<JobRecord>,
    queue: Vec<Queued>,
}

impl World {
    fn new(machines: Vec<Machine>) -> Self {
        World {
            machines,
            jobs: Vec::new(),
            queue: Vec::new(),
        }
    }

    /// A job running on machine `i` since `since` minutes, with a `limit`-minute run limit, last
    /// using CPU `idle` minutes ago.
    #[allow(clippy::too_many_arguments)]
    fn runs(
        &mut self,
        i: usize,
        id: &str,
        gib: u64,
        gpus: &[&str],
        exclusive: bool,
        since: i64,
        limit: u64,
        idle: i64,
    ) -> &mut JobRecord {
        let grant = Grant {
            ram_mib: gib * 1024,
            cpu: 1,
            gpus: gpus.iter().map(|g| g.to_string()).collect(),
            exclusive,
        };
        let deadline = m(since) + limit * MINUTE_MS;
        self.machines[i].running.push(Running {
            job: id.into(),
            grant: grant.clone(),
            deadline,
        });
        let mut r = record(id, JobState::Running);
        r.spec.request.ram_mib = gib * 1024;
        r.spec.timeout_ms = limit * MINUTE_MS;
        r.started_at = Some(m(since));
        r.node = Some(self.machines[i].facts.node.clone());
        r.lease = Some(Lease {
            grant,
            granted_at: m(since),
            deadline,
            token: "t".into(),
        });
        r.usage.cpu_progress_at = m(-idle);
        self.jobs.push(r);
        self.jobs.last_mut().unwrap()
    }

    fn queue(&mut self, q: Queued) {
        self.jobs.push(record(&q.job, JobState::Queued));
        self.queue.push(q);
    }

    fn why(&self, job: &str) -> WhyView {
        let views: Vec<MachineView> = self.machines.iter().map(|m| m.view()).collect();
        explain(
            job,
            &Snapshot {
                now: NOW,
                machines: &views,
                jobs: &self.jobs,
                queue: &self.queue,
                reservation: None,
            },
        )
    }
}

fn has(w: &WhyView, line: &str) {
    assert!(
        w.lines.iter().any(|l| l == line),
        "missing line {line:?} in:\n{}",
        w.lines.join("\n")
    );
}

fn kinds(w: &WhyView) -> Vec<&str> {
    w.blockers.iter().map(|b| b.resource.as_str()).collect()
}

#[test]
fn gpu_held_by_a_job() {
    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()])]);
    w.runs(0, "studio-41", 4, &[NV], false, -14, 15, 9);
    w.queue(job("studio-42").gpu("nvidia").at(-3));
    let why = w.why("studio-42");
    assert_eq!(why.state, "queued");
    has(&why, "queued for 3m at priority 3");
    has(&why, "asks for 4 GiB RAM, gpu nvidia; run limit 10m");
    has(
        &why,
        "waits on gpu 5080 on msi, held by job studio-41 for 14m of its 15m run limit, under 1% CPU for 9m",
    );
    has(
        &why,
        "first in line on msi: expected to start within 1m (when the jobs holding what it needs \
         reach their run limits)",
    );
    assert_eq!(kinds(&why), [format!("gpu:{NV}")]);
    let b = &why.blockers[0];
    assert_eq!(b.node.as_deref(), Some("msi"));
    assert_eq!(b.holder.as_deref(), Some("studio-41"));
    assert_eq!(b.held_for_ms, Some(14 * MINUTE_MS));
    assert_eq!(b.limit_ms, Some(15 * MINUTE_MS));
    assert_eq!(b.holder_idle_ms, Some(9 * MINUTE_MS));
    assert_eq!(b.free_by, Some(m(1)));
}

#[test]
fn ram() {
    let mut w = World::new(vec![Machine::new("msi", 56, vec![])]);
    w.runs(0, "a", 24, &[], false, -4, 15, 0);
    w.runs(0, "b", 12, &[], false, -1, 30, 3);
    w.queue(job("q").ram(32));
    let why = w.why("q");
    has(
        &why,
        "waits on RAM: needs 32 GiB, 20 GiB of 56 GiB free on msi",
    );
    // a ends first and frees enough; b isn't needed.
    has(
        &why,
        "job a holds 24 GiB there for 4m of its 15m run limit, using CPU",
    );
    assert!(!why.lines.iter().any(|l| l.starts_with("job b")));
    assert_eq!(kinds(&why), ["ram"]);
    assert_eq!(why.blockers[0].free_by, Some(m(11)));
    assert_eq!(why.blockers[0].holder_idle_ms, Some(0));
}

#[test]
fn exclusive() {
    // Waits for an exclusive job to end.
    let mut w = World::new(vec![Machine::new("ms01", 64, vec![nv(), amd()])]);
    w.runs(0, "x", 8, &[NV, AMD], true, -4, 15, 0);
    w.queue(job("q").ram(1));
    let why = w.why("q");
    has(
        &why,
        "waits for job x to end: it has ms01 to itself (exclusive) for 4m of its 15m run limit, \
         using CPU",
    );
    assert_eq!(kinds(&why), ["exclusive"]);

    // An exclusive job waits for the machine to empty.
    let mut w = World::new(vec![Machine::new("ms01", 64, vec![nv(), amd()])]);
    w.runs(0, "a", 4, &[], false, -5, 15, 2).stalled = true;
    w.runs(0, "b", 4, &[NV], false, -10, 30, 0);
    w.queue(job("e").excl());
    let why = w.why("e");
    has(
        &why,
        "asks for 4 GiB RAM, the whole machine (exclusive); run limit 10m",
    );
    has(
        &why,
        "needs ms01 to itself (exclusive): 2 jobs run there, expected to end within 20m",
    );
    has(
        &why,
        "job a runs there for 5m of its 15m run limit, under 1% CPU for 2m, stalled",
    );
    has(
        &why,
        "job b runs there for 10m of its 30m run limit, using CPU",
    );
    has(
        &why,
        "first in line on ms01: expected to start within 20m (when the jobs holding what it \
         needs reach their run limits)",
    );
    assert_eq!(kinds(&why), ["exclusive", "exclusive"]);
    assert_eq!(why.blockers[0].holder.as_deref(), Some("a"));
}

#[test]
fn order_behind_the_reservation() {
    let mut w = World::new(vec![Machine::new("msi", 32, vec![nv()])]);
    w.runs(0, "r", 8, &[NV], false, -30, 90, 0);
    w.runs(0, "r2", 8, &[], false, -10, 180, 0);
    w.queue(job("g").gpu("nvidia").ram(16).at(-20));
    w.queue(job("long").ram(12).mins(120).at(-5));
    let why = w.why("long");
    has(&why, "1 job is ahead of it in the queue");
    has(
        &why,
        "waits behind job g, first in line on msi, expected to start within 1h",
    );
    has(&why, "it fits on msi now, but waits its turn behind job g");
    assert_eq!(kinds(&why), ["order"]);
    assert_eq!(why.blockers[0].holder.as_deref(), Some("g"));
    assert_eq!(why.blockers[0].free_by, Some(m(60)));
}

#[test]
fn facts() {
    let mut mac = Machine::new("studio", 96, vec![]);
    mac.facts.os = "macos".into();
    mac.facts.arch = "aarch64".into();
    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()]), mac]);
    w.queue(
        job("q")
            .on("gpu", "amd")
            .on("os", "linux")
            .gpu("amd")
            .ram(128),
    );
    let why = w.why("q");
    has(&why, "no machine has gpu=amd");
    has(&why, "no machine has 128 GiB RAM to reserve");
    has(&why, "no machine has a GPU matching amd");
    has(
        &why,
        "msi can't run it: needs gpu=amd (this machine: NVIDIA GeForce RTX 5080 Laptop GPU)",
    );
    // The OS is named first.
    has(
        &why,
        "studio can't run it: needs os=linux (this machine: macOS)",
    );
    // os=linux alone is met somewhere, so it isn't listed as missing everywhere.
    assert!(!why.lines.iter().any(|l| l == "no machine has os=linux"));
    assert_eq!(kinds(&why), ["facts", "facts"]);

    // A machine that can't run it beside one that can.
    let mut w = World::new(vec![
        Machine::new("msi", 56, vec![nv()]),
        Machine::new("um780", 16, vec![]),
    ]);
    w.queue(job("q").gpu("nvidia"));
    let why = w.why("q");
    has(
        &why,
        "can start now on msi; the scheduler takes it on its next pass",
    );
    has(&why, "um780 can't run it: no GPU there matches nvidia");
    assert!(!why.lines.iter().any(|l| l.starts_with("no machine")));
}

#[test]
fn os_shorthands() {
    let w = |job: Queued| {
        let mut msi = Machine::new("msi", 56, vec![nv()]);
        msi.facts.kernel = "7.2.8-2-cachyos".into();
        msi.facts.os_version = "CachyOS".into();
        let mut mac = Machine::new("studio", 96, vec![]);
        mac.facts.os = "macos".into();
        mac.facts.os_version = "27.0.1".into();
        mac.facts.kernel = "27.0.0".into();
        let mut w = World::new(vec![msi, mac]);
        w.queue(job);
        w.why("q")
    };

    let why = w(job("q").on("macos", "26"));
    has(
        &why,
        "asks for 4 GiB RAM, a machine with macos=26; run limit 10m",
    );
    has(&why, "no machine has macos=26");
    has(
        &why,
        "msi can't run it: needs macos=26 (this machine: linux 7.2.8-2-cachyos)",
    );
    has(
        &why,
        "studio can't run it: needs macos=26 (this machine: macOS 27.0.1)",
    );
    assert_eq!(kinds(&why), ["facts", "facts"]);

    let why = w(job("q").on("linux", "6.12").on("gpu", "nvidia"));
    has(
        &why,
        "asks for 4 GiB RAM, a machine with linux=6.12, gpu=nvidia; run limit 10m",
    );
    has(&why, "no machine has linux=6.12");
    has(
        &why,
        "msi can't run it: needs linux=6.12 (this machine: linux 7.2.8-2-cachyos)",
    );

    let why = w(job("q").on("linux", ""));
    has(
        &why,
        "asks for 4 GiB RAM, a machine with linux; run limit 10m",
    );
    has(
        &why,
        "can start now on msi; the scheduler takes it on its next pass",
    );
    has(
        &why,
        "studio can't run it: needs linux (this machine: macOS 27.0.1)",
    );

    let why = w(job("q").on("macos", "27"));
    has(
        &why,
        "can start now on studio; the scheduler takes it on its next pass",
    );
    has(
        &why,
        "msi can't run it: needs macos=27 (this machine: linux 7.2.8-2-cachyos)",
    );
}

#[test]
fn workspace() {
    let mut w = World::new(vec![Machine::new("msi", 56, vec![])]);
    w.runs(0, "studio-40", 4, &[], false, -4, 15, 0).workspace = Some("msi-w3".into());
    w.queue(job("q").ws("msi-w3", "msi", true));
    let why = w.why("q");
    has(
        &why,
        "asks for 4 GiB RAM; run limit 10m; pinned to workspace msi-w3 on msi",
    );
    has(
        &why,
        "waits for its workspace msi-w3 on msi: job studio-40 runs in it for 4m of its 15m run \
         limit, using CPU",
    );
    assert_eq!(kinds(&why), ["workspace"]);
    assert_eq!(why.blockers[0].holder.as_deref(), Some("studio-40"));

    // Busy, but the caller has no record of what runs in it.
    let mut w = World::new(vec![Machine::new("msi", 56, vec![])]);
    w.queue(job("q").ws("msi-w3", "msi", true));
    has(
        &w.why("q"),
        "waits for its workspace msi-w3 on msi (an invocation runs in it)",
    );

    // Another invocation pinned to the same workspace goes first.
    let mut w = World::new(vec![Machine::new("msi", 56, vec![])]);
    w.queue(job("first").ws("msi-w3", "msi", false).prio(4));
    w.queue(job("q").ws("msi-w3", "msi", false));
    has(
        &w.why("q"),
        "waits for its workspace msi-w3 on msi: job first runs in it first, starting now, ahead \
         of it in line, with a 10m run limit",
    );

    // An earlier invocation into it, still waiting for the GPU, goes first.
    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()])]);
    w.runs(0, "x", 4, &[NV], false, -5, 15, 0);
    w.queue(job("first").ws("msi-w3", "msi", false).gpu("nvidia").at(-5));
    w.queue(job("q").ws("msi-w3", "msi", false).prio(5));
    let why = w.why("q");
    has(
        &why,
        "waits for its workspace msi-w3 on msi: job first, dispatched into it earlier, goes first",
    );
    assert_eq!(kinds(&why), ["workspace"]);
    assert_eq!(why.blockers[0].holder.as_deref(), Some("first"));

    // Its machine is not up.
    let mut w = World::new(vec![Machine::new("msi", 56, vec![])]);
    w.queue(job("q").ws("studio-w1", "studio", false));
    let why = w.why("q");
    has(
        &why,
        "its workspace studio-w1 is on studio, which is not among the machines up",
    );
    assert_eq!(why.blockers[0].node.as_deref(), Some("studio"));
}

#[test]
fn held_by_a_job_starting_now() {
    // Aged from 3 to 4 and older than p, q goes first; p waits on the GPU q takes now.
    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()])]);
    w.queue(job("p").gpu("any").prio(4));
    w.queue(job("q").gpu("any").at(-5).passes(1));
    let why = w.why("q");
    has(
        &why,
        "queued for 5m at priority 4 (dispatched at 3, passed over once)",
    );
    has(
        &why,
        "can start now on msi; the scheduler takes it on its next pass",
    );
    has(
        &w.why("p"),
        "waits on gpu 5080 on msi, held by job q starting now, ahead of it in line, with a 10m \
         run limit",
    );

    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()])]);
    w.queue(job("p").gpu("any").prio(4));
    w.queue(job("q").gpu("any").at(-5));
    let why = w.why("q");
    has(
        &why,
        "waits on gpu 5080 on msi, held by job p starting now, ahead of it in line, with a 10m \
         run limit",
    );
    assert_eq!(why.blockers[0].held_for_ms, Some(0));
    assert_eq!(why.blockers[0].free_by, Some(m(10)));
}

#[test]
fn queued_without_a_queue_entry() {
    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()])]);
    w.runs(0, "studio-41", 4, &[NV], false, -14, 15, 9);
    let mut r = record("studio-42", JobState::Queued);
    r.spec.request.gpus = vec!["nvidia".into()];
    r.submitted_at = m(-3);
    w.jobs.push(r);
    let why = w.why("studio-42");
    assert_eq!(why.state, "queued");
    has(
        &why,
        "waits on gpu 5080 on msi, held by job studio-41 for 14m of its 15m run limit, under 1% CPU for 9m",
    );
}

/// The run limit counts from the start, not from the claim: seeding before it doesn't count, and
/// a starting job says how long it has been starting since its claim.
#[test]
fn run_time_counts_from_the_start_not_the_claim() {
    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()])]);
    let r = w.runs(0, "studio-41", 4, &[NV], false, -4, 15, 0);
    // Claimed 3 minutes before it started: its workspace was being seeded.
    r.lease.as_mut().unwrap().granted_at = m(-7);
    let why = w.why("studio-41");
    assert_eq!(
        why.lines[0],
        "running on msi for 4m of its 15m run limit, 11m left"
    );
    let mut r = record("studio-42", JobState::Queued);
    r.spec.request.gpus = vec!["nvidia".into()];
    w.jobs.push(r);
    has(
        &w.why("studio-42"),
        "waits on gpu 5080 on msi, held by job studio-41 for 4m of its 15m run limit, using CPU",
    );

    // Starting: started_at is set only when it runs; the claim says since when.
    let mut r = record("s", JobState::Starting);
    r.node = Some("msi".into());
    r.workspace = Some("msi-w4".into());
    r.lease = Some(Lease {
        grant: Grant::default(),
        granted_at: m(-2),
        deadline: m(13),
        token: "t".into(),
    });
    let mut w = World::new(vec![Machine::new("msi", 56, vec![])]);
    w.jobs.push(r);
    assert_eq!(
        w.why("s").lines[0],
        "starting on msi: workspace msi-w4 is being seeded or checked, for 2m so far"
    );
}

#[test]
fn running() {
    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()])]);
    let r = w.runs(0, "studio-41", 16, &[NV], false, -14, 15, 9);
    r.workspace = Some("msi-w3".into());
    r.usage.cpu_ms = 4 * MINUTE_MS;
    r.usage.output_at = m(-10);
    r.usage.mem_bytes = 3 << 30;
    r.usage.peak_mem_bytes = 5 << 30;
    r.stalled = true;
    let why = w.why("studio-41");
    assert_eq!(why.state, "running");
    assert_eq!(
        why.lines,
        [
            "running on msi in workspace msi-w3 for 14m of its 15m run limit, 1m left",
            "holds 16 GiB RAM and gpu 5080",
            "under 1% CPU for 9m, 4m of CPU time in all",
            "last output 10m ago",
            "memory 3.0 GiB, peak 5.0 GiB of its 16 GiB",
            "stalled: under 1% CPU and no output for 9m",
        ]
    );
    assert!(why.blockers.is_empty());

    // Past its run limit, exclusive, no samples yet.
    let mut w = World::new(vec![Machine::new("msi", 56, vec![nv()])]);
    let r = w.runs(0, "x", 8, &[NV], true, -16, 15, 0);
    r.usage = Usage::default();
    let why = w.why("x");
    assert_eq!(
        why.lines,
        [
            "running on msi for 16m, past its 15m run limit: being stopped",
            "holds msi to itself (exclusive), with 8 GiB RAM as its limit",
            "no CPU sample yet",
            "no output yet",
        ]
    );

    let mut r = record("s", JobState::Starting);
    r.node = Some("msi".into());
    r.workspace = Some("msi-w4".into());
    r.started_at = Some(m(-1));
    let mut w = World::new(vec![Machine::new("msi", 56, vec![])]);
    w.jobs.push(r);
    let why = w.why("s");
    assert_eq!(why.state, "starting");
    assert_eq!(
        why.lines,
        ["starting on msi: workspace msi-w4 is being seeded or checked, for 1m so far"]
    );
}

#[test]
fn finished() {
    let ended = |state: JobState| {
        let mut r = record("j", state);
        r.node = Some("msi".into());
        r.started_at = Some(m(-20));
        r.ended_at = Some(m(-5));
        r
    };
    let why_of = |r: JobRecord| {
        let mut w = World::new(vec![]);
        w.jobs.push(r);
        w.why("j")
    };

    // The first line of `why` names the state ("job j timed out"); these lines don't repeat it.
    let mut r = ended(JobState::TimedOut);
    r.message = Some("killed at its run limit (15m)".into());
    r.stuck_pids = vec![4242, 4243];
    let why = why_of(r);
    assert_eq!(why.state, "timed_out");
    assert_eq!(
        why.lines,
        [
            "ran for 15m on msi, ended 5m ago",
            "killed at its run limit (15m)",
            "potluck wait exits with 124",
            "processes that would not die: 4242, 4243; its lease was released anyway",
        ]
    );
    // Ended on a stall, as its dispatch asked: the daemon's message says so.
    let mut r = ended(JobState::TimedOut);
    r.message = Some(
        "stalled: under 1% CPU and no output for 10m; ended as its dispatch asked (kill on stall)"
            .into(),
    );
    assert_eq!(
        why_of(r).lines[1],
        "stalled: under 1% CPU and no output for 10m; ended as its dispatch asked (kill on stall)"
    );

    let mut r = ended(JobState::Failed);
    r.exit_code = Some(2);
    r.message = Some("exited with code 2".into());
    assert_eq!(
        why_of(r).lines,
        ["ran for 15m on msi, ended 5m ago", "exited with code 2"]
    );
    let mut r = ended(JobState::Failed);
    r.exit_code = Some(2);
    assert_eq!(why_of(r).lines[1..], ["exited with code 2"]);
    let mut r = ended(JobState::Failed);
    r.signal = Some(11);
    assert_eq!(
        why_of(r).lines[1..],
        ["killed by signal 11", "potluck wait exits with 139"]
    );
    let mut r = ended(JobState::Succeeded);
    r.exit_code = Some(0);
    r.message = Some("exited with code 0".into());
    assert_eq!(
        why_of(r).lines,
        ["ran for 15m on msi, ended 5m ago", "exited with code 0"]
    );

    let mut r = ended(JobState::OverMemory);
    r.spec.request.ram_mib = 8 * 1024;
    r.message = Some("over memory: used 8.2 GiB of its 8 GiB".into());
    assert_eq!(
        why_of(r).lines,
        [
            "ran for 15m on msi, ended 5m ago",
            "over memory: used 8.2 GiB of its 8 GiB",
            "potluck wait exits with 137"
        ]
    );
    // A limit under 1 GiB is given in MiB, not rounded to 0.1 GiB.
    let mut r = ended(JobState::OverMemory);
    r.spec.request.ram_mib = 64;
    assert_eq!(why_of(r).lines[1], "went over its 64 MiB memory limit");

    let mut r = ended(JobState::Cancelled);
    r.message = Some("cancelled".into());
    assert_eq!(
        why_of(r).lines,
        [
            "ran for 15m on msi, ended 5m ago",
            "potluck wait exits with 130"
        ]
    );
    let mut r = ended(JobState::Cancelled);
    r.started_at = None;
    r.node = None;
    r.message = Some("cancelled while queued".into());
    assert_eq!(
        why_of(r).lines,
        [
            "ended 5m ago without starting",
            "cancelled while queued",
            "potluck wait exits with 130"
        ]
    );
    assert_eq!(
        why_of(ended(JobState::Lost)).lines[1],
        "its machine went away and it was not queued again"
    );
}

/// A job that sleeps uses no CPU and writes nothing: `why` must not say it is using CPU or name
/// an output time. The daemon starts both clocks when the job starts.
#[test]
fn a_sleeping_job_is_under_1_percent_cpu_with_no_output() {
    let mut w = World::new(vec![Machine::new("studio", 56, vec![])]);
    let r = w.runs(0, "a", 40, &[], false, 0, 30, 0);
    let start = NOW - 2_000;
    r.started_at = Some(start);
    // The run limit counts from the start: the deadline moves there.
    let lease = r.lease.as_mut().unwrap();
    lease.granted_at = start;
    lease.deadline = start + 30 * 60_000;
    r.usage.cpu_progress_at = start;
    r.usage.output_at = start;
    r.usage.cpu_ms = 3;
    r.usage.mem_bytes = 1 << 20;
    r.usage.peak_mem_bytes = 3 << 20;
    w.machines[0].running[0].deadline = start + 30 * 60_000;
    let why = w.why("a");
    assert_eq!(
        why.lines[2..],
        [
            "under 1% CPU for 2s, under 1s of CPU time in all",
            "no output yet",
            "memory 1.0 MiB, peak 3.0 MiB of its 40 GiB",
        ]
    );
    w.queue(job("q").ram(30));
    has(
        &w.why("q"),
        "job a holds 40 GiB there for 2s of its 30m run limit, under 1% CPU for 2s",
    );
    // Starting up took CPU; nothing since.
    let r = w.jobs.iter_mut().find(|j| j.id == "a").unwrap();
    r.usage.cpu_progress_at = start + 500;
    assert_eq!(
        w.why("a").lines[2],
        "under 1% CPU for 1s, under 1s of CPU time in all"
    );
    // Once its CPU time rises it is using CPU; once its log grows it has output.
    let r = w.jobs.iter_mut().find(|j| j.id == "a").unwrap();
    r.usage.cpu_progress_at = NOW - 500;
    r.usage.output_at = NOW - 1_000;
    r.usage.log_bytes = 6;
    let why = w.why("a");
    assert_eq!(
        why.lines[2..4],
        [
            "using CPU, under 1s of CPU time in all",
            "last output 1s ago"
        ]
    );
}

#[test]
fn ram_under_a_gib_is_in_mib() {
    let mut w = World::new(vec![Machine::new("msi", 56, vec![])]);
    let mut q = job("q");
    q.request.ram_mib = 512;
    w.queue(q);
    has(&w.why("q"), "asks for 512 MiB RAM; run limit 10m");
}

#[test]
fn unknown_job() {
    let w = World::new(vec![]);
    let why = w.why("nope-1");
    assert_eq!(why.state, "unknown");
    assert_eq!(why.lines, ["no job nope-1"]);
}

#[test]
fn no_machine_up() {
    let mut w = World::new(vec![]);
    w.queue(job("q"));
    has(&w.why("q"), "no machine is up to run it");
}

#[test]
fn sync_behind() {
    let mut w = World::new(vec![
        Machine::new("msi", 56, vec![nv()]),
        Machine::new("studio", 56, vec![]),
    ]);
    w.queue(job("q").gpu("nvidia").at(-2));
    let views: Vec<MachineView> = w.machines.iter().map(|m| m.view()).collect();
    let snap = Snapshot {
        now: NOW,
        machines: &views,
        jobs: &w.jobs,
        queue: &w.queue,
        reservation: None,
    };
    let unsynced: BTreeMap<String, String> = [(
        "msi".to_string(),
        "its mirror of proj lacks commit aaaaaaaaaaaa yet: syncing from studio".to_string(),
    )]
    .into();
    let why = explain_synced("q", &snap, &unsynced);
    has(
        &why,
        "msi can't take it yet: its mirror of proj lacks commit aaaaaaaaaaaa yet: syncing from studio",
    );
    has(
        &why,
        "no machine that could run it has its commits yet (the sync is behind)",
    );
    assert_eq!(kinds(&why), ["sync", "facts"]);
    assert_eq!(why.blockers[0].node.as_deref(), Some("msi"));
    // Synced: the usual explanation.
    let why = explain("q", &snap);
    has(
        &why,
        "can start now on msi; the scheduler takes it on its next pass",
    );
    // msi could start it, but waits a few seconds for a machine with a workspace of its
    // repository (locality): why says so, not that it starts now.
    let locality: BTreeMap<String, String> = [(
        "msi".to_string(),
        "studio, which has a workspace of its repository, for up to 2s more (locality)".to_string(),
    )]
    .into();
    let why = explain_waits(
        "q",
        &snap,
        &Waits {
            unsynced: &BTreeMap::new(),
            locality: &locality,
            catch_up: &BTreeMap::new(),
            held_out: &BTreeMap::new(),
        },
    );
    has(
        &why,
        "msi could start it now, but leaves it to studio, which has a workspace of its repository, for up to 2s more (locality)",
    );
    assert!(
        !why.lines.iter().any(|l| l.contains("can start now")),
        "{:?}",
        why.lines
    );
    assert_eq!(kinds(&why), ["locality", "facts"]);
    // msi, back from being away, compares membership records before it takes the job: that wait
    // comes first, locality or not.
    let catch_up: BTreeMap<String, String> = [(
        "msi".to_string(),
        "is back from being away and compares membership records with the other members first"
            .to_string(),
    )]
    .into();
    let why = explain_waits(
        "q",
        &snap,
        &Waits {
            unsynced: &BTreeMap::new(),
            locality: &locality,
            catch_up: &catch_up,
            held_out: &BTreeMap::new(),
        },
    );
    has(
        &why,
        "msi could start it now, but is back from being away and compares membership records with the other members first",
    );
    assert!(
        !why.lines.iter().any(|l| l.contains("can start now")),
        "{:?}",
        why.lines
    );
    assert_eq!(kinds(&why), ["catch_up", "facts"]);
}

/// Each machine plans with the jobs it can take: one whose commits its mirrors lack, one it
/// leaves to a warm machine (locality), or one of an owner it is catching up with is left out of
/// its plan. `why` plans each machine the same way, so a job the machine can't take yet doesn't
/// show as holding what the job asked about needs.
#[test]
fn a_machine_plans_without_the_jobs_it_cannot_take_yet() {
    let mut w = World::new(vec![Machine::new("studio", 48, vec![])]);
    w.runs(0, "a", 32, &[], false, -1, 30, 0);
    // Ahead in line, but studio's mirrors lack its commits.
    w.queue(job("big").ram(40).prio(5));
    // Fits in the 16 GiB free now, but runs past a's end, when big would need its RAM.
    w.queue(job("q").ram(10).mins(60));
    let views: Vec<MachineView> = w.machines.iter().map(|m| m.view()).collect();
    let snap = Snapshot {
        now: NOW,
        machines: &views,
        jobs: &w.jobs,
        queue: &w.queue,
        reservation: None,
    };
    // Planned with every job, big holds studio's reservation and q waits behind it.
    let why = explain("q", &snap);
    assert!(
        why.lines.iter().any(|l| l.contains("big")),
        "{:?}",
        why.lines
    );
    // studio leaves big out of its plan: q starts there.
    let held_out: BTreeMap<String, BTreeSet<String>> =
        [("studio".to_string(), ["big".to_string()].into())].into();
    let why = explain_waits(
        "q",
        &snap,
        &Waits {
            unsynced: &BTreeMap::new(),
            locality: &BTreeMap::new(),
            catch_up: &BTreeMap::new(),
            held_out: &held_out,
        },
    );
    has(
        &why,
        "can start now on studio; the scheduler takes it on its next pass",
    );
    assert!(
        !why.lines.iter().any(|l| l.contains("big")),
        "{:?}",
        why.lines
    );
}

/// A signal from a peer's record or the store can be anything: the exit code `wait` and `why`
/// give saturates instead of overflowing.
#[test]
fn a_huge_signal_does_not_overflow_the_exit_code() {
    let mut r = record("studio-1", JobState::Failed);
    r.exit_code = None;
    r.signal = Some(9);
    assert_eq!(r.exit_code_for_client(), 137);
    r.signal = Some(i32::MAX);
    assert_eq!(r.exit_code_for_client(), i32::MAX);
}
