//! Table-driven tests for every scheduling rule. Times are minutes after `NOW`.

use super::*;
use crate::types::{Facts, Gpu, Grant, MINUTE_MS, Request};

pub(crate) const NOW: Millis = 1_000 * MINUTE_MS;
pub(crate) const NV: &str = "10de:2c19@0000:01:00.0";
pub(crate) const AMD: &str = "1002:744c@0000:03:00.0";

/// `n` minutes after `NOW` (negative: before).
pub(crate) fn m(n: i64) -> Millis {
    (NOW as i64 + n * MINUTE_MS as i64) as Millis
}

pub(crate) fn nv() -> Gpu {
    Gpu {
        id: NV.into(),
        vendor: "10de".into(),
        device: "2c19".into(),
        name: "NVIDIA GeForce RTX 5080 Laptop GPU".into(),
        driver: Some("nvidia".into()),
        alias: Some("5080".into()),
        render_node: None,
    }
}

pub(crate) fn amd() -> Gpu {
    Gpu {
        id: AMD.into(),
        vendor: "1002".into(),
        device: "744c".into(),
        name: "AMD Radeon RX 7900 XTX".into(),
        driver: Some("amdgpu".into()),
        alias: Some("xtx".into()),
        render_node: None,
    }
}

pub(crate) fn facts(node: &str, gpus: Vec<Gpu>) -> Facts {
    Facts {
        node: node.into(),
        os: "linux".into(),
        arch: "x86_64".into(),
        cpus: 24,
        ram_mib: 64 * 1024,
        gpus,
        ..Default::default()
    }
}

/// A queued job: priority 3, 4 GiB, no GPU, 10m run limit, submitted at `NOW`.
pub(crate) fn job(id: &str) -> Queued {
    Queued {
        job: id.into(),
        priority: 3,
        passes: 0,
        submitted_at: NOW,
        request: Request::default(),
        on: BTreeMap::new(),
        timeout_ms: 10 * MINUTE_MS,
        workspace: None,
    }
}

/// Builders for test jobs.
pub(crate) trait Q: Sized {
    fn q(&mut self) -> &mut Queued;
    fn prio(mut self, p: u8) -> Self {
        self.q().priority = p;
        self
    }
    fn passes(mut self, n: u32) -> Self {
        self.q().passes = n;
        self
    }
    /// Submitted `t` minutes after `NOW`.
    fn at(mut self, t: i64) -> Self {
        self.q().submitted_at = m(t);
        self
    }
    fn ram(mut self, gib: u64) -> Self {
        self.q().request.ram_mib = gib * 1024;
        self
    }
    fn cpu(mut self, n: u32) -> Self {
        self.q().request.cpu = n;
        self
    }
    fn gpu(mut self, sel: &str) -> Self {
        self.q().request.gpus.push(sel.into());
        self
    }
    fn excl(mut self) -> Self {
        self.q().request.exclusive = true;
        self
    }
    fn mins(mut self, n: u64) -> Self {
        self.q().timeout_ms = n * MINUTE_MS;
        self
    }
    fn on(mut self, k: &str, v: &str) -> Self {
        self.q().on.insert(k.into(), v.into());
        self
    }
    fn ws(mut self, ws: &str, node: &str, busy: bool) -> Self {
        self.q().workspace = Some((ws.into(), node.into(), busy));
        self
    }
}

impl Q for Queued {
    fn q(&mut self) -> &mut Queued {
        self
    }
}

/// A running job holding `gib` GiB and these GPUs until `until` minutes after `NOW`.
pub(crate) fn run(id: &str, gib: u64, gpus: &[&str], until: i64) -> Running {
    Running {
        job: id.into(),
        grant: Grant {
            ram_mib: gib * 1024,
            cpu: 1,
            gpus: gpus.iter().map(|g| g.to_string()).collect(),
            exclusive: false,
        },
        deadline: m(until),
    }
}

/// A running exclusive job.
pub(crate) fn run_excl(id: &str, gib: u64, gpus: &[&str], until: i64) -> Running {
    let mut r = run(id, gib, gpus, until);
    r.grant.exclusive = true;
    r
}

/// A machine fixture that lends out a [`MachineView`].
pub(crate) struct Machine {
    pub facts: Facts,
    pub cap_gib: u64,
    pub running: Vec<Running>,
}

impl Machine {
    pub fn new(node: &str, cap_gib: u64, gpus: Vec<Gpu>) -> Self {
        Machine {
            facts: facts(node, gpus),
            cap_gib,
            running: Vec::new(),
        }
    }

    pub fn view(&self) -> MachineView<'_> {
        MachineView {
            facts: &self.facts,
            ram_capacity_mib: self.cap_gib * 1024,
            running: &self.running,
        }
    }
}

#[test]
fn feasibility() {
    let msi = Machine::new("msi", 56, vec![nv()]);
    let two = Machine::new("ms01", 56, vec![nv(), amd()]);
    let cases: Vec<(&str, &Machine, Queued, Result<(), &str>)> = vec![
        ("plain job", &msi, job("a"), Ok(())),
        ("facts met", &msi, job("a").on("os", "linux"), Ok(())),
        (
            "facts unmet",
            &msi,
            job("a").on("gpu", "amd"),
            Err("needs gpu=amd (this machine: NVIDIA GeForce RTX 5080 Laptop GPU)"),
        ),
        (
            "an OS shorthand unmet",
            &msi,
            job("a").on("macos", "27").on("gpu", "nvidia"),
            Err("needs macos=27 (this machine: linux)"),
        ),
        (
            "an OS shorthand met",
            &msi,
            job("a").on("linux", ""),
            Ok(()),
        ),
        (
            "RAM above capacity",
            &msi,
            job("a").ram(64),
            Err("needs 64 GiB RAM, its capacity is 56 GiB"),
        ),
        ("RAM at capacity", &msi, job("a").ram(56), Ok(())),
        ("a GPU that exists", &msi, job("a").gpu("5080"), Ok(())),
        (
            "no GPU matches",
            &msi,
            job("a").gpu("amd"),
            Err("no GPU there matches amd"),
        ),
        (
            "two selectors, one GPU",
            &msi,
            job("a").gpu("any").gpu("nvidia"),
            Err("no 2 separate GPUs there match any, nvidia"),
        ),
        (
            "two selectors, two GPUs",
            &two,
            job("a").gpu("any").gpu("nvidia"),
            Ok(()),
        ),
        (
            "busy GPUs still feasible",
            &msi,
            job("a").gpu("nvidia"),
            Ok(()),
        ),
        ("exclusive", &msi, job("a").excl().gpu("any"), Ok(())),
        (
            "pinned to this machine, workspace busy",
            &msi,
            job("a").ws("msi-w3", "msi", true),
            Ok(()),
        ),
        (
            "pinned to another machine",
            &msi,
            job("a").ws("studio-w1", "studio", false),
            Err("pinned to workspace studio-w1 on studio"),
        ),
    ];
    for (name, machine, q, want) in cases {
        let got = feasible(&machine.view(), &q);
        assert_eq!(got, want.map_err(str::to_string), "{name}");
    }
    // A GPU held by a job doesn't make a job infeasible.
    let mut busy = Machine::new("msi", 56, vec![nv()]);
    busy.running.push(run("x", 4, &[NV], 5));
    assert_eq!(feasible(&busy.view(), &job("a").gpu("nvidia")), Ok(()));
}

#[test]
fn ordering() {
    let cases: Vec<(&str, Vec<Queued>, Vec<&str>)> = vec![
        (
            "higher priority first",
            vec![job("s-1").prio(1), job("s-2").prio(4), job("s-3")],
            vec!["s-2", "s-3", "s-1"],
        ),
        (
            "then submit time",
            vec![job("s-1").at(5), job("s-2").at(1), job("s-3").at(3)],
            vec!["s-2", "s-3", "s-1"],
        ),
        (
            "then job id, by sequence number",
            vec![job("s-10"), job("s-9"), job("r-11")],
            vec!["r-11", "s-9", "s-10"],
        ),
        (
            "aging raises priority",
            vec![job("s-1").at(1), job("s-2").prio(1).passes(2)],
            vec!["s-2", "s-1"],
        ),
        (
            "aging stops at 4",
            vec![job("s-1").prio(1).passes(9), job("s-2").prio(4).at(-1)],
            vec!["s-2", "s-1"],
        ),
        (
            "priority 5 ahead of aged jobs",
            vec![
                job("s-1").prio(1).passes(9).at(-60),
                job("s-2").prio(4).at(-30),
                job("s-3").prio(5).at(10),
            ],
            vec!["s-3", "s-1", "s-2"],
        ),
    ];
    for (name, queue, want) in cases {
        let got: Vec<&str> = order(&queue)
            .into_iter()
            .map(|i| queue[i].job.as_str())
            .collect();
        assert_eq!(got, want, "{name}");
    }
}

/// One plan case: the machine, the queue, and what the plan must say.
struct Case {
    name: &'static str,
    machine: Machine,
    queue: Vec<Queued>,
    start: &'static [&'static str],
    passed: &'static [&'static str],
    reserved: Option<(&'static str, Millis)>,
}

fn check(cases: Vec<Case>) {
    for c in cases {
        let p = plan(NOW, &c.machine.view(), &c.queue);
        let start: Vec<&str> = p.start.iter().map(|(j, _)| j.as_str()).collect();
        assert_eq!(start, c.start, "{}: start", c.name);
        assert_eq!(p.passed, c.passed, "{}: passed", c.name);
        let reserved = p.reservation.as_ref().map(|r| (r.job.as_str(), r.at));
        assert_eq!(reserved, c.reserved, "{}: reservation", c.name);
    }
}

fn with(mut machine: Machine, running: Vec<Running>) -> Machine {
    machine.running = running;
    machine
}

#[test]
fn admission() {
    check(vec![
        Case {
            name: "RAM fits",
            machine: with(Machine::new("msi", 32, vec![]), vec![run("x", 16, &[], 30)]),
            queue: vec![job("a").ram(16)],
            start: &["a"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "RAM short",
            machine: with(Machine::new("msi", 32, vec![]), vec![run("x", 28, &[], 30)]),
            queue: vec![job("a").ram(8)],
            start: &[],
            passed: &[],
            reserved: Some(("a", m(30))),
        },
        Case {
            name: "CPU is a weight only",
            machine: Machine::new("msi", 32, vec![]),
            queue: vec![job("a").cpu(64), job("b").cpu(64)],
            start: &["a", "b"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "free GPU",
            machine: Machine::new("msi", 32, vec![nv()]),
            queue: vec![job("a").gpu("nvidia")],
            start: &["a"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "GPU held by a job",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("x", 4, &[NV], 20)],
            ),
            queue: vec![job("a").gpu("nvidia")],
            start: &[],
            passed: &[],
            reserved: Some(("a", m(20))),
        },
        Case {
            name: "all or nothing: one of two GPUs free",
            machine: with(
                Machine::new("ms01", 32, vec![nv(), amd()]),
                vec![run("x", 4, &[AMD], 15)],
            ),
            queue: vec![job("a").gpu("nvidia").gpu("amd")],
            start: &[],
            passed: &[],
            reserved: Some(("a", m(15))),
        },
        Case {
            name: "each GPU to one job",
            machine: Machine::new("msi", 32, vec![nv()]),
            queue: vec![job("a").gpu("any"), job("b").gpu("any").at(1)],
            start: &["a"],
            passed: &[],
            reserved: Some(("b", m(10))),
        },
    ]);
}

#[test]
fn gpu_assignment() {
    let both = Machine::new("ms01", 32, vec![nv(), amd()]);
    let cases: Vec<(&str, Queued, Vec<&str>)> = vec![
        ("any takes the first GPU", job("a").gpu("any"), vec![NV]),
        ("by vendor", job("a").gpu("amd"), vec![AMD]),
        ("by alias", job("a").gpu("xtx"), vec![AMD]),
        ("by vendor:device", job("a").gpu("10de:2c19"), vec![NV]),
        (
            "backtracks so every selector gets a GPU",
            job("a").gpu("any").gpu("nvidia"),
            vec![AMD, NV],
        ),
        ("two GPUs", job("a").gpu("any").gpu("any"), vec![NV, AMD]),
    ];
    for (name, q, want) in cases {
        let p = plan(NOW, &both.view(), std::slice::from_ref(&q));
        assert_eq!(p.start.len(), 1, "{name}");
        assert_eq!(p.start[0].1.gpus, want, "{name}");
        assert_eq!(p.start[0].1.ram_mib, q.request.ram_mib, "{name}");
        assert!(!p.start[0].1.exclusive, "{name}");
    }
    // With the NVIDIA GPU busy, `any` goes to the AMD one.
    let busy = with(
        Machine::new("ms01", 32, vec![nv(), amd()]),
        vec![run("x", 4, &[NV], 30)],
    );
    let p = plan(NOW, &busy.view(), &[job("a").gpu("any")]);
    assert_eq!(p.start[0].1.gpus, vec![AMD]);
}

#[test]
fn exclusive() {
    // On an empty machine it takes every GPU, and its RAM grant is the RAM it asked for.
    let empty = Machine::new("ms01", 64, vec![nv(), amd()]);
    let p = plan(NOW, &empty.view(), &[job("e").excl().ram(8).cpu(4)]);
    assert_eq!(
        p.start,
        vec![(
            "e".to_string(),
            Grant {
                ram_mib: 8 * 1024,
                cpu: 4,
                gpus: vec![NV.into(), AMD.into()],
                exclusive: true,
            }
        )]
    );

    check(vec![
        Case {
            name: "waits for the machine to empty",
            machine: with(
                Machine::new("ms01", 64, vec![nv(), amd()]),
                vec![run("x", 4, &[], 10), run("y", 4, &[], 40)],
            ),
            queue: vec![job("e").excl()],
            start: &[],
            passed: &[],
            reserved: Some(("e", m(40))),
        },
        Case {
            name: "nothing starts beside a running exclusive job",
            machine: with(
                Machine::new("ms01", 64, vec![nv(), amd()]),
                vec![run_excl("x", 4, &[NV, AMD], 30)],
            ),
            queue: vec![job("a").ram(1), job("b").gpu("any").at(1)],
            start: &[],
            passed: &[],
            reserved: Some(("a", m(30))),
        },
        Case {
            name: "an exclusive job started now blocks the rest",
            machine: Machine::new("ms01", 64, vec![nv(), amd()]),
            queue: vec![job("e").excl().prio(4).mins(20), job("a").ram(1)],
            start: &["e"],
            passed: &[],
            reserved: Some(("a", m(20))),
        },
    ]);
}

#[test]
fn infeasible_and_pinned() {
    check(vec![
        Case {
            name: "infeasible job is skipped, not passed, not reserved",
            machine: Machine::new("msi", 32, vec![nv()]),
            queue: vec![
                job("amd").on("gpu", "amd").prio(5).at(-10),
                job("big").ram(64).prio(5).at(-10),
                job("a"),
            ],
            start: &["a"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "reservation goes to the first feasible blocked job",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("x", 4, &[NV], 30)],
            ),
            queue: vec![job("amd").gpu("amd").prio(5), job("a").gpu("nvidia")],
            start: &[],
            passed: &[],
            reserved: Some(("a", m(30))),
        },
        Case {
            name: "pinned to a busy workspace: waits without a reservation or aging",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("x", 4, &[], 30)],
            ),
            queue: vec![
                job("p").ws("msi-w3", "msi", true).prio(5).at(-10),
                job("a").gpu("nvidia"),
            ],
            start: &["a"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "pinned to a free workspace: starts",
            machine: Machine::new("msi", 32, vec![]),
            queue: vec![job("p").ws("msi-w3", "msi", false)],
            start: &["p"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "pinned to another machine: skipped",
            machine: Machine::new("msi", 32, vec![]),
            queue: vec![job("p").ws("studio-w1", "studio", false).at(-5), job("a")],
            start: &["a"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "invocations into one workspace run first come, first served",
            machine: Machine::new("msi", 32, vec![]),
            queue: vec![
                job("low").ws("msi-w3", "msi", false).prio(2).at(-5),
                job("high").ws("msi-w3", "msi", false).prio(5),
            ],
            start: &["low"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "a later invocation doesn't start while an earlier one into its workspace waits",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("x", 4, &[NV], 30)],
            ),
            queue: vec![
                job("first")
                    .ws("msi-w3", "msi", false)
                    .gpu("nvidia")
                    .prio(2)
                    .at(-5),
                job("next").ws("msi-w3", "msi", false).prio(4).mins(1),
                job("elsewhere").ws("msi-w4", "msi", false),
            ],
            start: &["elsewhere"],
            passed: &[],
            reserved: Some(("first", m(30))),
        },
        Case {
            name: "an earlier invocation this machine can't run doesn't hold the workspace",
            machine: Machine::new("msi", 32, vec![nv()]),
            queue: vec![
                job("never")
                    .ws("msi-w3", "msi", false)
                    .on("gpu", "amd")
                    .at(-5),
                job("next").ws("msi-w3", "msi", false),
            ],
            start: &["next"],
            passed: &[],
            reserved: None,
        },
    ]);
}

#[test]
fn aging() {
    check(vec![
        Case {
            name: "CPU-only starts don't pass a job waiting on a GPU",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("x", 4, &[NV], 30)],
            ),
            queue: vec![
                job("gpu-2").gpu("nvidia").prio(2).at(-20),
                job("gpu-3").gpu("nvidia").at(-15),
                job("s1").at(-5),
                job("s2").at(-4),
            ],
            start: &["s1", "s2"],
            passed: &[],
            reserved: Some(("gpu-3", m(30))),
        },
        Case {
            name: "a GPU its selector matches: passed, once per plan",
            machine: Machine::new("ms01", 64, vec![nv(), amd()]),
            queue: vec![
                job("w").gpu("any").prio(1).at(-10),
                job("s1").gpu("any"),
                job("s2").gpu("any").at(1),
                job("late").gpu("any").prio(1).at(2),
            ],
            start: &["s1", "s2"],
            passed: &["w"],
            reserved: Some(("w", m(10))),
        },
        Case {
            name: "a GPU its selectors don't match: not passed",
            machine: with(
                Machine::new("ms01", 64, vec![nv(), amd()]),
                vec![run("x", 4, &[NV], 30)],
            ),
            queue: vec![job("w").gpu("nvidia").at(-10), job("s").gpu("amd")],
            start: &["s"],
            passed: &[],
            reserved: Some(("w", m(30))),
        },
        Case {
            name: "RAM when it didn't fit before the start: passed",
            machine: with(Machine::new("msi", 32, vec![]), vec![run("x", 24, &[], 30)]),
            queue: vec![job("w").ram(16).at(-10), job("s").ram(4)],
            start: &["s"],
            passed: &["w"],
            reserved: Some(("w", m(30))),
        },
        Case {
            name: "its place: a later job takes the RAM it would have had",
            machine: with(Machine::new("msi", 32, vec![]), vec![run("x", 16, &[], 30)]),
            queue: vec![job("w").ram(16).prio(1).at(-10), job("s").ram(16)],
            start: &["s"],
            passed: &["w"],
            // s's 16 GiB come back at its 10m run limit.
            reserved: Some(("w", m(10))),
        },
        Case {
            name: "held back for the reservation, still fitting after the start: not passed",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("x", 8, &[NV], 30)],
            ),
            queue: vec![
                job("g").gpu("nvidia").ram(16).at(-30),
                // 24 GiB free; 16 spare at 30m after g's 16; 22 free after s.
                job("w").ram(20).mins(60).at(-20),
                job("s").ram(2).mins(5).at(-10),
            ],
            start: &["s"],
            passed: &[],
            reserved: Some(("g", m(30))),
        },
        Case {
            name: "exclusive and waiting for the machine to empty: any start passes it",
            machine: with(Machine::new("msi", 64, vec![]), vec![run("x", 4, &[], 30)]),
            queue: vec![job("e").excl().at(-10), job("s").ram(1).mins(5)],
            start: &["s"],
            passed: &["e"],
            reserved: Some(("e", m(30))),
        },
        Case {
            name: "waiting only for its own workspace: not passed",
            machine: with(
                Machine::new("msi", 64, vec![nv()]),
                vec![run("x", 4, &[], 30)],
            ),
            queue: vec![
                job("busy-ws")
                    .ws("msi-w3", "msi", true)
                    .gpu("nvidia")
                    .at(-10),
                job("first").ws("msi-w4", "msi", false).gpu("nvidia").at(-9),
                job("behind")
                    .ws("msi-w4", "msi", false)
                    .gpu("nvidia")
                    .at(-8),
            ],
            start: &["first"],
            passed: &[],
            reserved: None,
        },
        Case {
            name: "jobs submitted at the same time are not passed",
            machine: Machine::new("msi", 32, vec![nv()]),
            queue: vec![job("a").gpu("any"), job("b").gpu("any").prio(1)],
            start: &["a"],
            passed: &[],
            reserved: Some(("b", m(10))),
        },
        Case {
            name: "held back for the reservation: passed when a later job takes its RAM",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("x", 8, &[NV], 30)],
            ),
            queue: vec![
                job("g").gpu("nvidia").ram(16).at(-30),
                // Fits now (24 GiB free) but not in the 16 GiB left at 30m after g's 16, and not
                // in the 20 GiB left once short starts.
                job("held").ram(22).mins(60).at(-20),
                job("short").ram(4).mins(5).at(-10),
            ],
            // g waits on the GPU and fits in RAM either way.
            start: &["short"],
            passed: &["held"],
            reserved: Some(("g", m(30))),
        },
    ]);
}

#[test]
fn reservation_times() {
    check(vec![
        Case {
            name: "RAM freed in deadline order",
            machine: with(
                Machine::new("msi", 32, vec![]),
                vec![
                    run("a", 8, &[], 50),
                    run("b", 8, &[], 10),
                    run("c", 12, &[], 30),
                ],
            ),
            queue: vec![job("q").ram(16)],
            start: &[],
            passed: &[],
            reserved: Some(("q", m(30))),
        },
        Case {
            name: "the GPU expected free first",
            machine: with(
                Machine::new("ms01", 32, vec![nv(), amd()]),
                vec![run("a", 4, &[NV], 40), run("b", 4, &[AMD], 15)],
            ),
            queue: vec![job("q").gpu("any")],
            start: &[],
            passed: &[],
            reserved: Some(("q", m(15))),
        },
        Case {
            name: "GPU and RAM: the later of the two",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("a", 4, &[NV], 15), run("b", 24, &[], 45)],
            ),
            queue: vec![job("q").gpu("nvidia").ram(16)],
            start: &[],
            passed: &[],
            reserved: Some(("q", m(45))),
        },
        Case {
            name: "a holder past its deadline is expected free now",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("a", 4, &[NV], -2)],
            ),
            queue: vec![job("q").gpu("nvidia")],
            start: &[],
            passed: &[],
            reserved: Some(("q", NOW)),
        },
    ]);
}

#[test]
fn backfill() {
    check(vec![
        Case {
            name: "a short job ends before the reservation",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("r", 8, &[NV], 60)],
            ),
            queue: vec![
                job("g").gpu("nvidia").ram(16).at(-1),
                job("short").ram(16).mins(60),
            ],
            start: &["short"],
            passed: &[],
            reserved: Some(("g", m(60))),
        },
        Case {
            name: "a long job that needs the reserved RAM waits",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("r", 8, &[NV], 60), run("r2", 16, &[], 120)],
            ),
            queue: vec![
                job("g").gpu("nvidia").ram(16).at(-1),
                job("long").ram(8).mins(120),
                job("short").ram(8).mins(30).at(1),
            ],
            start: &["short"],
            passed: &["g", "long"],
            reserved: Some(("g", m(60))),
        },
        Case {
            name: "long jobs fit in RAM that stays free, until it is used up",
            machine: with(
                Machine::new("msi", 64, vec![nv()]),
                vec![run("r", 8, &[NV], 60)],
            ),
            queue: vec![
                job("g").gpu("nvidia").ram(16).at(-1),
                job("l1").ram(8).mins(120),
                job("l2").ram(40).mins(120).at(1),
                job("l3").ram(36).mins(120).at(2),
            ],
            // At 60m: 64 - 16 for g = 48 spare; l1 takes 8, l2's 40 fits the 40 left, l3 can't.
            start: &["l1", "l2"],
            passed: &[],
            reserved: Some(("g", m(60))),
        },
        Case {
            name: "a long job on another GPU starts",
            machine: with(
                Machine::new("ms01", 64, vec![nv(), amd()]),
                vec![run("r", 4, &[NV], 60)],
            ),
            queue: vec![job("g").gpu("nvidia").at(-1), job("l").gpu("amd").mins(120)],
            start: &["l"],
            passed: &[],
            reserved: Some(("g", m(60))),
        },
        Case {
            name: "a long job on the reserved GPU waits, a short one doesn't",
            machine: with(
                Machine::new("ms01", 64, vec![nv(), amd()]),
                vec![run("r", 32, &[], 30)],
            ),
            queue: vec![
                // Blocked on RAM; it will get the NVIDIA GPU at 30m.
                job("g").gpu("nvidia").ram(48).at(-1),
                job("long-nv").gpu("nvidia").mins(120),
                job("long-amd").gpu("amd").mins(120).at(1),
                job("short-nv").gpu("nvidia").mins(20).at(2),
            ],
            start: &["long-amd", "short-nv"],
            passed: &["g", "long-nv"],
            reserved: Some(("g", m(30))),
        },
        Case {
            name: "exclusive waiting: only jobs that end before the machine empties",
            machine: with(
                Machine::new("ms01", 64, vec![nv(), amd()]),
                vec![run("r", 4, &[], 40)],
            ),
            queue: vec![
                job("e").excl().at(-1),
                job("long").ram(1).mins(120),
                job("short").ram(1).mins(30).at(1),
                job("gpu-short").gpu("amd").mins(40).at(2),
            ],
            start: &["short", "gpu-short"],
            passed: &["e"],
            reserved: Some(("e", m(40))),
        },
        Case {
            name: "later jobs in the reserved job's workspace wait, short or long",
            machine: with(
                Machine::new("msi", 32, vec![nv()]),
                vec![run("r", 4, &[NV], 30)],
            ),
            queue: vec![
                // Blocked on the GPU; it will have it at 30m, and needs its workspace then.
                job("g").gpu("nvidia").ws("msi-w3", "msi", false).at(-2),
                job("long").ws("msi-w3", "msi", false).mins(60).at(-1),
                job("short").ws("msi-w3", "msi", false).mins(5).at(-1),
                job("other-ws").ws("msi-w4", "msi", false).mins(60),
            ],
            start: &["other-ws"],
            passed: &[],
            reserved: Some(("g", m(30))),
        },
        Case {
            name: "only the first blocked job is protected",
            machine: with(
                Machine::new("ms01", 64, vec![nv(), amd()]),
                vec![run("r", 4, &[NV], 60)],
            ),
            queue: vec![
                job("g").gpu("nvidia").at(-2),
                // Blocked too, behind g, but holds no reservation: c may take the AMD GPU.
                job("both").gpu("nvidia").gpu("amd").at(-1),
                job("c").gpu("amd").mins(120),
            ],
            // c takes the AMD GPU both asks for, not g's NVIDIA one.
            start: &["c"],
            passed: &["both"],
            reserved: Some(("g", m(60))),
        },
    ]);
}

#[test]
fn no_preemption() {
    // A priority-5 job waits for a priority-1 job's GPU: nothing is stopped, it is reserved.
    check(vec![Case {
        name: "priority 5 waits for a running priority-1 job",
        machine: with(
            Machine::new("msi", 32, vec![nv()]),
            vec![run("low", 4, &[NV], 120)],
        ),
        queue: vec![job("urgent").gpu("nvidia").prio(5)],
        start: &[],
        passed: &[],
        reserved: Some(("urgent", m(120))),
    }]);
}

#[test]
fn ram_amounts() {
    assert_eq!(gib(16 * 1024), "16");
    assert_eq!(gib(512), "0.5");
    assert_eq!(gib(1300), "1.3");
}

/// Grants in a peer's report can be anything: what they hold adds up without overflowing.
#[test]
fn huge_grants_dont_overflow() {
    let f = facts("b", vec![]);
    let mut big = run("x", 0, &[], 10);
    big.grant.ram_mib = u64::MAX;
    let running = vec![
        big.clone(),
        Running {
            job: "y".into(),
            ..big
        },
    ];
    let view = MachineView {
        facts: &f,
        ram_capacity_mib: 1024,
        running: &running,
    };
    let load = Load::new(NOW, &view);
    assert_eq!(load.ram_free(), 0);
    assert_eq!(load.ram_free_at(NOW), 0);
}

/// `n - 1` NVIDIA GPUs and one AMD; `n - 2` selectors `any`, then `amd` twice: no assignment
/// exists, and only the last selector finds out.
fn no_assignment_case(n: usize) -> (Vec<String>, Vec<Gpu>) {
    let mut gpus: Vec<Gpu> = (0..n - 1)
        .map(|i| Gpu {
            id: format!("10de:2c19@0000:{i:02x}:00.0"),
            ..nv()
        })
        .collect();
    gpus.push(amd());
    let mut sels = vec!["any".to_string(); n - 2];
    sels.extend(["amd".to_string(), "amd".to_string()]);
    (sels, gpus)
}

/// A job can ask for as many GPUs as a machine has: assigning them takes polynomial time, also
/// when no assignment exists and only the last selector finds out (backtracking took 10 s for 11
/// GPUs in a release build, and about 11 times longer per GPU after that, under the daemon's
/// state lock at dispatch).
#[test]
fn assigning_many_gpus_is_quick() {
    for n in [12, 16, 24] {
        let (sels, gpus) = no_assignment_case(n);
        let every: Vec<usize> = (0..gpus.len()).collect();
        let t = std::time::Instant::now();
        assert_eq!(assign(&sels, &gpus, &every), None, "{n}");
        // One amd: then there is one, the AMD GPU last.
        let one = &sels[..n - 1];
        let got = assign(one, &gpus, &every).unwrap();
        assert_eq!(got.last(), Some(&(n - 1)), "{n}");
        assert!(
            t.elapsed() < std::time::Duration::from_secs(1),
            "{n}: {:?}",
            t.elapsed()
        );
    }
}

/// The backtracking [`assign`] replaced: the reference for which assignment is chosen.
fn assign_by_backtracking(
    sels: &[String],
    gpus: &[Gpu],
    candidates: &[usize],
) -> Option<Vec<usize>> {
    fn fill(sels: &[String], gpus: &[Gpu], candidates: &[usize], used: &mut Vec<usize>) -> bool {
        let Some(sel) = sels.get(used.len()) else {
            return true;
        };
        for &g in candidates {
            if !used.contains(&g) && gpu_matches(sel, &gpus[g]) {
                used.push(g);
                if fill(sels, gpus, candidates, used) {
                    return true;
                }
                used.pop();
            }
        }
        false
    }
    if sels.len() > candidates.len() {
        return None;
    }
    let mut used = Vec::with_capacity(sels.len());
    fill(sels, gpus, candidates, &mut used).then_some(used)
}

/// The same GPUs as before for every request: the first selector takes the first candidate that
/// still leaves the rest an assignment, and so on.
#[test]
fn assign_picks_what_backtracking_picked() {
    let kinds = [
        nv(),
        amd(),
        Gpu {
            id: "8086:56a0@0000:05:00.0".into(),
            vendor: "8086".into(),
            device: "56a0".into(),
            alias: None,
            ..nv()
        },
        Gpu {
            id: "10de:2684@0000:07:00.0".into(),
            device: "2684".into(),
            alias: Some("4090".into()),
            ..nv()
        },
    ];
    let selectors = [
        "any",
        "nvidia",
        "amd",
        "intel",
        "5080",
        "xtx",
        "4090",
        "10de:2c19",
        "10de",
        NV,
    ];
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = |n: usize| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((seed >> 33) % n as u64) as usize
    };
    for _ in 0..20_000 {
        let gpus: Vec<Gpu> = (0..1 + next(7))
            .map(|i| Gpu {
                id: format!("{}#{i}", kinds[i % kinds.len()].id),
                ..kinds[next(kinds.len())].clone()
            })
            .collect();
        let sels: Vec<String> = (0..next(7))
            .map(|_| selectors[next(selectors.len())].to_string())
            .collect();
        // Some of the GPUs, in any order, now and then one twice.
        let mut cand: Vec<usize> = (0..gpus.len()).filter(|_| next(4) > 0).collect();
        for k in (1..cand.len()).rev() {
            cand.swap(k, next(k + 1));
        }
        if !cand.is_empty() && next(5) == 0 {
            cand.push(cand[next(cand.len())]);
        }
        assert_eq!(
            assign(&sels, &gpus, &cand),
            assign_by_backtracking(&sels, &gpus, &cand),
            "{sels:?} over {cand:?} of {:?}",
            gpus.iter().map(|g| &g.id).collect::<Vec<_>>()
        );
    }
}
