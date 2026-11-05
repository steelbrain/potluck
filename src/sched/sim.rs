//! `plan` stepped over simulated time, the way the daemon calls it: jobs arrive, run for a while
//! (never past their run limit) and end; at every event the machine plans until nothing more
//! starts, and each passed job gets one more pass.

use super::tests::{Machine, NOW, NV, Q, amd, job, m, nv, run};
use super::*;
use crate::types::MINUTE_MS;

struct Sim {
    machine: Machine,
    now: Millis,
    /// Jobs not yet submitted, with how long each really runs.
    arrivals: Vec<(Queued, u64)>,
    queue: Vec<Queued>,
    run_ms: BTreeMap<JobId, u64>,
    /// When each running job really ends.
    ends: BTreeMap<JobId, Millis>,
    /// Job -> (start time, passes when it started).
    started: BTreeMap<JobId, (Millis, u32)>,
    /// Every start in order.
    log: Vec<(Millis, JobId)>,
    /// Every reservation a plan made, with the time.
    reservations: Vec<(Millis, Reservation)>,
}

impl Sim {
    /// `running`: jobs already running at `NOW`, each with when it really ends (minutes).
    /// `jobs`: (job, minutes it really runs), submitted at their `submitted_at`.
    fn new(mut machine: Machine, running: Vec<(Running, i64)>, jobs: Vec<(Queued, u64)>) -> Sim {
        let mut ends = BTreeMap::new();
        for (r, end) in running {
            ends.insert(r.job.clone(), m(end));
            machine.running.push(r);
        }
        let mut arrivals: Vec<(Queued, u64)> = jobs
            .into_iter()
            .map(|(q, mins)| (q, mins * MINUTE_MS))
            .collect();
        arrivals.sort_by_key(|(q, _)| q.submitted_at);
        Sim {
            machine,
            now: NOW,
            arrivals,
            queue: Vec::new(),
            run_ms: BTreeMap::new(),
            ends,
            started: BTreeMap::new(),
            log: Vec::new(),
            reservations: Vec::new(),
        }
    }

    fn step(&mut self) {
        let now = self.now;
        while self
            .arrivals
            .first()
            .is_some_and(|(q, _)| q.submitted_at <= now)
        {
            let (q, run_ms) = self.arrivals.remove(0);
            self.run_ms.insert(q.job.clone(), run_ms);
            self.queue.push(q);
        }
        let ends = &self.ends;
        self.machine.running.retain(|r| ends[&r.job] > now);
        loop {
            let p = plan(now, &self.machine.view(), &self.queue);
            if let Some(r) = &p.reservation {
                self.reservations.push((now, r.clone()));
            }
            for id in &p.passed {
                let q = self.queue.iter_mut().find(|q| &q.job == id).unwrap();
                q.passes += 1;
            }
            if p.start.is_empty() {
                break;
            }
            for (id, grant) in p.start {
                let at = self.queue.iter().position(|q| q.job == id).unwrap();
                let q = self.queue.remove(at);
                let deadline = now + q.timeout_ms;
                self.ends
                    .insert(id.clone(), now + self.run_ms[&id].min(q.timeout_ms));
                self.machine.running.push(Running {
                    job: id.clone(),
                    grant,
                    deadline,
                });
                self.started.insert(id.clone(), (now, q.passes));
                self.log.push((now, id));
            }
        }
    }

    /// Steps from event to event until `end` minutes after `NOW`.
    fn run_until(&mut self, end: i64) -> &mut Self {
        loop {
            self.step();
            let next_arrival = self.arrivals.first().map(|(q, _)| q.submitted_at);
            let next_end = self.machine.running.iter().map(|r| self.ends[&r.job]).min();
            match [next_arrival, next_end].into_iter().flatten().min() {
                Some(t) if t <= m(end) => self.now = t,
                _ => return self,
            }
        }
    }

    fn start_of(&self, id: &str) -> Millis {
        self.started
            .get(id)
            .unwrap_or_else(|| panic!("{id} never started"))
            .0
    }
}

/// (a) A priority-1 job is not starved by a steady stream of priority-3 jobs: each start it
/// waits through raises it one step, and at 3 it is the oldest job in line.
#[test]
fn aging_prevents_starvation() {
    let mut jobs = vec![(job("old").prio(1).gpu("nvidia").at(0), 10)];
    for i in 0..40 {
        jobs.push((job(&format!("s-{i}")).gpu("nvidia").at(1 + 5 * i), 10));
    }
    let mut sim = Sim::new(
        Machine::new("msi", 32, vec![nv()]),
        vec![(run("x", 4, &[NV], 5), 5)],
        jobs,
    );
    sim.run_until(300);

    // s-0 starts at 5 and s-1 at 15, each passing "old" over; at 25 it is first in line.
    assert_eq!(sim.started["old"], (m(25), 2));
    // It did not wait for the stream to dry up: three priority-3 jobs were still waiting.
    let waiting_then = (0..40)
        .filter(|&i| m(1 + 5 * i) <= m(25) && sim.start_of(&format!("s-{i}")) > m(25))
        .count();
    assert_eq!(waiting_then, 3);
}

/// (b) A priority-5 job starts ahead of everything waiting for the same resource, aged jobs
/// included, at the first moment the resource frees.
#[test]
fn priority_five_starts_first() {
    let mut jobs = vec![
        (job("p4").prio(4).gpu("nvidia").at(0), 10),
        (job("aged").prio(1).passes(5).gpu("nvidia").at(-10), 10),
        (job("urgent").prio(5).gpu("nvidia").at(20), 10),
        (job("urgent2").prio(5).gpu("nvidia").at(35), 10),
    ];
    for i in 0..10 {
        jobs.push((job(&format!("s-{i}")).gpu("nvidia").at(1 + 5 * i), 10));
    }
    let mut sim = Sim::new(
        Machine::new("msi", 32, vec![nv()]),
        vec![(run("x", 4, &[NV], 30), 30)],
        jobs,
    );
    sim.run_until(300);
    assert_eq!(sim.start_of("urgent"), m(30));
    assert_eq!(sim.start_of("urgent2"), m(40));
    assert_eq!(sim.start_of("aged"), m(50));
    assert_eq!(sim.start_of("p4"), m(60));

    // A longer, mixed stream: whenever a job below priority 5 starts on the GPU, no
    // priority-5 job is waiting for it.
    let mut seed: u64 = 0x5eed;
    let mut next = |n: u64| {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) % n
    };
    let mut jobs = Vec::new();
    for i in 0..120 {
        let prio = [1, 1, 2, 3, 3, 3, 4, 5][next(8) as usize];
        let mins = 2 + next(8);
        let q = job(&format!("j-{i}"))
            .prio(prio)
            .gpu("nvidia")
            .mins(mins + next(3))
            .at(3 * i as i64);
        jobs.push((q, mins));
    }
    let prio: BTreeMap<JobId, (u8, Millis)> = jobs
        .iter()
        .map(|(q, _)| (q.job.clone(), (q.priority, q.submitted_at)))
        .collect();
    let mut sim = Sim::new(Machine::new("msi", 32, vec![nv()]), vec![], jobs);
    sim.run_until(24 * 60);
    assert_eq!(sim.started.len(), 120, "every job ran");
    assert!(prio.values().filter(|(p, _)| *p == 5).count() >= 10);
    assert!(sim.started.values().any(|(_, passes)| *passes >= 3));
    for (k, (t, id)) in sim.log.iter().enumerate() {
        if prio[id].0 == 5 {
            continue;
        }
        for (other, (p, submitted)) in &prio {
            let waiting = *p == 5
                && *submitted <= *t
                && sim.log.iter().position(|(_, j)| j == other).unwrap() > k;
            assert!(!waiting, "{id} started at {t} while {other} waited");
        }
    }
}

/// (c) Jobs that fit start while the first job waits on the GPU, until it has been queued for 30
/// minutes: then it holds the line, and a job that would fit waits for it.
#[test]
fn jobs_fill_in_until_the_first_has_waited_30_minutes() {
    let running = vec![(run("r", 8, &[NV], 60), 60), (run("r2", 16, &[], 120), 120)];
    let jobs = vec![
        (job("gpu").gpu("nvidia").ram(16).mins(120).at(0), 30),
        (job("short").ram(8).mins(30).at(1), 30),
        (job("long").ram(8).mins(120).at(2), 120),
    ];
    let mut sim = Sim::new(Machine::new("msi", 32, vec![nv()]), running, jobs);
    sim.run_until(600);
    // short takes the 8 GiB free; long waits for RAM.
    assert_eq!(sim.start_of("short"), m(1));
    // short's 8 GiB come back at 31, when gpu has waited 31 minutes: long waits behind it.
    assert_eq!(sim.start_of("gpu"), m(60));
    // gpu's 16 GiB come back when it ends at 90.
    assert_eq!(sim.start_of("long"), m(90));
    // Filled in ahead of once: passed over once.
    assert_eq!(sim.started["gpu"].1, 1);

    // With --hold-line, nothing starts behind it from the first.
    let running = vec![(run("r", 8, &[NV], 60), 60), (run("r2", 16, &[], 120), 120)];
    let jobs = vec![
        (
            job("gpu").gpu("nvidia").ram(16).mins(120).at(0).hold_line(),
            30,
        ),
        (job("short").ram(8).mins(30).at(1), 30),
        (job("long").ram(8).mins(120).at(2), 120),
    ];
    let mut sim = Sim::new(Machine::new("msi", 32, vec![nv()]), running, jobs);
    sim.run_until(600);
    assert_eq!(sim.started["gpu"], (m(60), 0));
    assert_eq!(sim.start_of("short"), m(90));
    assert_eq!(sim.start_of("long"), m(90));
}

/// (d) An exclusive job with --hold-line waits only for what already runs: jobs arriving
/// meanwhile, short or long, wait for it, and it starts when it would have alone. Without it, jobs
/// that fit start ahead of it while small jobs keep arriving, until it has been queued for 30
/// minutes; then it waits for what runs then to end.
#[test]
fn an_exclusive_job_waits_at_most_30_minutes_of_filling() {
    let machine = || Machine::new("ms01", 64, vec![nv(), amd()]);
    let running = || vec![(run("r1", 4, &[], 30), 30), (run("r2", 4, &[NV], 20), 20)];
    let stream = || {
        (0..30)
            .map(|i| (job(&format!("s-{i}")).ram(4).mins(20).at(2 + 5 * i), 20))
            .collect::<Vec<_>>()
    };
    let excl = || job("excl").excl().ram(16).mins(30).at(1);
    let mut alone = Sim::new(machine(), running(), vec![(excl(), 20)]);
    alone.run_until(600);
    assert_eq!(alone.start_of("excl"), m(30));

    let mut held = Sim::new(machine(), running(), stream());
    held.arrivals.push((excl().hold_line(), 20 * MINUTE_MS));
    held.arrivals.sort_by_key(|(q, _)| q.submitted_at);
    held.run_until(600);
    assert_eq!(held.start_of("excl"), m(30));
    // Everything that arrived meanwhile starts when it ends, after its 20 minutes.
    for i in 0..10 {
        assert_eq!(held.start_of(&format!("s-{i}")), m(50), "s-{i}");
    }

    let mut busy = Sim::new(machine(), running(), stream());
    busy.arrivals.push((excl(), 20 * MINUTE_MS));
    busy.arrivals.sort_by_key(|(q, _)| q.submitted_at);
    busy.run_until(600);
    // s-0 to s-5 (2 to 27) fill in at once; from 31 excl holds the line, and s-6 (32) waits.
    for i in 0..6 {
        assert_eq!(busy.start_of(&format!("s-{i}")), m(2 + 5 * i), "s-{i}");
    }
    // s-5, started at 27, ends last of what ran at 31.
    assert_eq!(busy.start_of("excl"), m(47));
    // Passed over by each job that filled in ahead of it, its priority 4 since.
    assert_eq!(busy.started["excl"].1, 6);
    // s-6 to s-12 arrived meanwhile: they start when it ends, after its 20 minutes.
    for i in 6..13 {
        assert_eq!(busy.start_of(&format!("s-{i}")), m(67), "s-{i}");
    }
    // Then the stream fills in as it arrives again.
    for i in 13..30 {
        assert_eq!(busy.start_of(&format!("s-{i}")), m(2 + 5 * i), "s-{i}");
    }
}

/// A job waiting on a busy GPU is passed over by every job that starts after it in order, CPU-only
/// ones too: its priority rises by one each time, and stops at 4. A priority-5 job dispatched later
/// still goes first. Queued for 30 minutes, it keeps back no CPU-only job: they take nothing it
/// waits for.
#[test]
fn filling_in_ages_the_waiting_job_up_to_4() {
    let mut jobs = vec![
        (job("gpu").prio(1).gpu("nvidia").at(0), 10),
        (job("urgent").prio(5).gpu("nvidia").at(25), 10),
    ];
    for i in 0..12 {
        jobs.push((
            job(&format!("c-{i}")).prio(1).ram(1).mins(5).at(1 + 5 * i),
            2,
        ));
    }
    let mut sim = Sim::new(
        Machine::new("msi", 64, vec![nv()]),
        vec![(run("x", 4, &[NV], 60), 60)],
        jobs,
    );
    // c-0 to c-5 (1 to 26) each start at once, after gpu in order.
    for (t, passes, eff) in [(1, 1, 2), (6, 2, 3), (11, 3, 4), (16, 4, 4), (29, 6, 4)] {
        sim.run_until(t);
        let q = sim.queue.iter().find(|q| q.job == "gpu").unwrap();
        let got = (q.passes, effective_priority(q.priority, q.passes));
        assert_eq!(got, (passes, eff), "at {t}");
    }
    sim.run_until(600);
    // From 30 gpu holds the line, for the GPU: c-6 (31) to c-11 (56) start as they arrive too.
    for i in 0..12 {
        assert_eq!(sim.start_of(&format!("c-{i}")), m(1 + 5 * i), "c-{i}");
    }
    // urgent gets the GPU first, passing gpu over once more: 12 + 1 passes.
    assert_eq!(sim.start_of("urgent"), m(60));
    assert_eq!(sim.started["gpu"], (m(70), 13));
}

/// A job that needs both GPUs at once waits while single-GPU jobs keep filling in, one GPU or the
/// other always busy. Queued for 30 minutes, it keeps back the jobs that would take a GPU, and gets
/// both once those running end; CPU-only jobs, which take nothing it waits for, start as they
/// arrive all along.
#[test]
fn a_job_queued_for_30_minutes_keeps_back_only_what_takes_what_it_waits_for() {
    let mut jobs = vec![(job("both").gpu("any").gpu("any").at(0), 10)];
    for i in 0..20 {
        jobs.push((job(&format!("g-{i}")).gpu("any").mins(5).at(1 + 3 * i), 5));
        jobs.push((job(&format!("c-{i}")).ram(1).mins(5).at(2 + 3 * i), 5));
    }
    let mut sim = Sim::new(
        Machine::new("ms01", 64, vec![nv(), amd()]),
        vec![(run("x", 4, &[NV], 2), 2)],
        jobs,
    );
    sim.run_until(600);
    // g-0 to g-9 (1 to 28) fill in as they arrive.
    for i in 0..10 {
        assert_eq!(sim.start_of(&format!("g-{i}")), m(1 + 3 * i), "g-{i}");
    }
    // From 30 g-10 (31) waits: both starts when g-9 ends, at 33, and g-10 when it ends.
    assert_eq!(sim.start_of("both"), m(33));
    assert_eq!(sim.start_of("g-10"), m(43));
    for i in 0..20 {
        assert_eq!(sim.start_of(&format!("c-{i}")), m(2 + 3 * i), "c-{i}");
    }
}

/// CPU-only jobs of a higher priority that start while two jobs wait on a busy GPU don't pass them
/// over: when the GPU frees, the priority-3 job gets it ahead of the priority-2 one.
#[test]
fn cpu_starts_do_not_age_gpu_waiters() {
    let mut sim = Sim::new(
        Machine::new("msi", 64, vec![nv()]),
        vec![(run("x", 4, &[NV], 30), 30)],
        vec![
            (job("gpu-2").gpu("nvidia").prio(2).at(0), 10),
            (job("gpu-3").gpu("nvidia").at(1), 10),
            (job("cpu-1").prio(4).at(5), 10),
            (job("cpu-2").prio(4).at(10), 10),
        ],
    );
    sim.run_until(120);
    assert_eq!(sim.start_of("cpu-1"), m(5));
    assert_eq!(sim.start_of("cpu-2"), m(10));
    assert_eq!(sim.started["gpu-3"], (m(30), 0));
    assert_eq!(sim.started["gpu-2"], (m(40), 1));
}

/// (a), with RAM only: a priority-1 job is not starved by a steady stream of priority-3 jobs the
/// same size, each taking the place it would have had.
#[test]
fn aging_prevents_starvation_on_ram() {
    let mut jobs = vec![(job("old").prio(1).ram(16).at(0), 10)];
    for i in 0..100 {
        jobs.push((job(&format!("s-{i}")).ram(16).at(1 + 3 * i), 10));
    }
    let mut sim = Sim::new(
        Machine::new("msi", 48, vec![]),
        vec![
            (run("x1", 16, &[], 4), 4),
            (run("x2", 16, &[], 8), 8),
            (run("x3", 16, &[], 12), 12),
        ],
        jobs,
    );
    sim.run_until(400);
    // s-0 takes the slot freed at 4 and s-1 the one at 8, each passing "old" over; at 12 it is
    // first in line, with s-2 and s-3 still waiting.
    assert_eq!(sim.started["old"], (m(12), 2));
    for i in [2, 3] {
        assert!(sim.start_of(&format!("s-{i}")) > m(12));
    }
}
