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

/// (c) Nothing starts behind a job blocked on the GPU, even jobs that would fit and end long
/// before it: they start once it has, as RAM frees.
#[test]
fn nothing_jumps_a_blocked_job() {
    let running = vec![(run("r", 8, &[NV], 60), 60), (run("r2", 16, &[], 120), 120)];
    let jobs = vec![
        (job("gpu").gpu("nvidia").ram(16).mins(120).at(0), 30),
        (job("short").ram(8).mins(30).at(1), 30),
        (job("long").ram(8).mins(120).at(2), 120),
    ];
    let mut sim = Sim::new(Machine::new("msi", 32, vec![nv()]), running, jobs);
    sim.run_until(600);
    assert_eq!(sim.start_of("gpu"), m(60));
    // The GPU job's 16 GiB come back when it ends at 90.
    assert_eq!(sim.start_of("short"), m(90));
    assert_eq!(sim.start_of("long"), m(90));
}

/// (d) An exclusive job waits only for what already runs: jobs arriving meanwhile, short or long,
/// wait for it, and it starts when it would have alone.
#[test]
fn exclusive_waits_only_for_what_runs() {
    let machine = || Machine::new("ms01", 64, vec![nv(), amd()]);
    let running = || vec![(run("r1", 4, &[], 30), 30), (run("r2", 4, &[NV], 50), 50)];
    let excl = || (job("excl").excl().ram(16).mins(30).at(1), 20);
    let mut jobs = vec![excl(), (job("short").ram(4).gpu("amd").mins(10).at(10), 10)];
    for i in 0..10 {
        jobs.push((
            job(&format!("long-{i}")).ram(4).mins(120).at(2 + 5 * i),
            120,
        ));
    }
    let mut busy = Sim::new(machine(), running(), jobs);
    busy.run_until(600);
    let mut alone = Sim::new(machine(), running(), vec![excl()]);
    alone.run_until(600);

    assert_eq!(alone.start_of("excl"), m(50));
    assert_eq!(busy.start_of("excl"), m(50));
    // Everything that arrived meanwhile starts when it ends, after its 20 minutes.
    assert_eq!(busy.start_of("short"), m(70));
    for i in 0..10 {
        assert_eq!(busy.start_of(&format!("long-{i}")), m(70), "long-{i}");
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
