//! The daemon's clocks.
//!
//! Records carry wall time (Unix milliseconds): when a job was submitted, started and ended, its
//! run limit's end as `why` and the scheduler estimate it, a workspace's hold. What the daemon
//! enforces over a span of time is measured on the boot clock instead, which no step of the wall
//! clock moves (an NTP correction, someone setting the date): a job's run limit and the loop's
//! periodic work. Stall windows and the grace between SIGTERM and SIGKILL are measured on the
//! awake clock, which no step moves either and which stops while the machine sleeps: a machine
//! asleep runs none of its jobs, so the time it slept is no stall, and no time a job had to shut
//! down.
//!
//! The boot clock counts milliseconds since the machine booted, time asleep included, as the wall
//! clock does (Linux `CLOCK_BOOTTIME`; macOS `CLOCK_MONOTONIC`, which counts while asleep as
//! `mach_continuous_time` does). Its readings are stored with the boot's id (Linux's `boot_id`,
//! macOS's `kern.bootsessionuuid`), so a daemon restarted in the same boot goes on from them. After
//! a reboot they mean nothing, and nothing of the jobs they timed is left running. The awake clock
//! (Linux `CLOCK_MONOTONIC`, macOS `CLOCK_UPTIME_RAW`, as Rust's `Instant`) is never stored: a
//! restarted daemon measures stalls from the first samples it takes, and counts the time since a
//! SIGTERM or SIGKILL sent before the restart on the boot clock.
//!
//! Holds stay on wall time: a hold is a promise in wall-clock terms (`hold W 2h`, the time left in
//! every reminder), it outlives a reboot, and its record travels to the owner's machine, where
//! only wall time means the same. A step of the wall clock moves a hold's end by the step.
//!
//! Retention (finished jobs are deleted after 14 days) counts from when this machine recorded each
//! end, on its own clocks: for a job another machine ran, when that machine's report of the end
//! arrived, whatever the runner's clock said. It outlives a reboot, so it goes by the wall clock;
//! for an end recorded in this boot, the boot clock must agree too, so a step of the wall clock
//! doesn't delete jobs that ended since the machine booted. Ends from earlier boots go by the
//! wall clock alone.

use crate::types::Millis;
use std::sync::OnceLock;

/// The clocks the daemon reads: [`SystemClock`], or a test's.
pub trait Clock: Send + Sync {
    /// Unix time in milliseconds.
    fn wall_ms(&self) -> Millis;
    /// Milliseconds since this machine booted, time asleep included. No step of the wall clock
    /// moves it.
    fn boot_ms(&self) -> u64;
    /// Milliseconds this machine has been awake since it booted: it stops while the machine
    /// sleeps, and no step of the wall clock moves it.
    fn awake_ms(&self) -> u64;
    /// This boot's id: boot-clock readings compare only within one boot. Empty when unknown.
    fn boot_id(&self) -> String;
}

/// The machine's clocks.
pub struct SystemClock;

impl Clock for SystemClock {
    fn wall_ms(&self) -> Millis {
        crate::types::now_ms()
    }

    fn boot_ms(&self) -> u64 {
        boot_ms()
    }

    fn awake_ms(&self) -> u64 {
        awake_ms()
    }

    fn boot_id(&self) -> String {
        static ID: OnceLock<String> = OnceLock::new();
        ID.get_or_init(read_boot_id).clone()
    }
}

/// The machine's boot clock ([`Clock::boot_ms`]), for code with no clock injected.
pub fn boot_ms() -> u64 {
    #[cfg(target_os = "linux")]
    let id = libc::CLOCK_BOOTTIME;
    // macOS: counts while asleep, unlike CLOCK_UPTIME_RAW (and Rust's Instant).
    #[cfg(not(target_os = "linux"))]
    let id = libc::CLOCK_MONOTONIC;
    clock_ms(id)
}

/// The machine's awake clock ([`Clock::awake_ms`]).
pub fn awake_ms() -> u64 {
    // Neither counts while asleep.
    #[cfg(target_os = "linux")]
    let id = libc::CLOCK_MONOTONIC;
    #[cfg(not(target_os = "linux"))]
    let id = libc::CLOCK_UPTIME_RAW;
    clock_ms(id)
}

/// Clock `id`'s reading in milliseconds; 0 if it can't be read.
fn clock_ms(id: libc::clockid_t) -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime fills `ts`; the clock id is valid on this platform.
    if unsafe { libc::clock_gettime(id, &mut ts) } != 0 {
        return 0;
    }
    (ts.tv_sec.max(0) as u64)
        .saturating_mul(1000)
        .saturating_add(ts.tv_nsec.max(0) as u64 / 1_000_000)
}

#[cfg(target_os = "linux")]
fn read_boot_id() -> String {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

#[cfg(not(target_os = "linux"))]
fn read_boot_id() -> String {
    let mut buf = [0u8; 64];
    let mut len = buf.len();
    // SAFETY: the buffer has `len` bytes of room; sysctlbyname writes at most that many.
    let r = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            buf.as_mut_ptr().cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if r != 0 {
        return String::new();
    }
    let text = buf[..len.min(buf.len())]
        .split(|&b| b == 0)
        .next()
        .unwrap_or_default();
    std::str::from_utf8(text)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// One moment on the three clocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Now {
    pub wall: Millis,
    pub boot: u64,
    pub awake: u64,
}

impl Now {
    pub fn read(clock: &dyn Clock) -> Now {
        Now {
            wall: clock.wall_ms(),
            boot: clock.boot_ms(),
            awake: clock.awake_ms(),
        }
    }

    /// Wall time `t` as a boot-clock reading, by its distance from this moment.
    pub fn boot_of(self, t: Millis) -> u64 {
        if t >= self.wall {
            self.boot.saturating_add(t - self.wall)
        } else {
            self.boot.saturating_sub(self.wall - t)
        }
    }

    /// Boot-clock reading `t` as a wall time, by its distance from this moment.
    pub fn wall_of(self, t: u64) -> Millis {
        if t >= self.boot {
            self.wall.saturating_add(t - self.boot)
        } else {
            self.wall.saturating_sub(self.boot - t)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_boot_clock_runs_and_names_its_boot() {
        let c = SystemClock;
        let a = c.boot_ms();
        std::thread::sleep(std::time::Duration::from_millis(30));
        let b = c.boot_ms();
        assert!(a > 0 && b >= a + 25 && b < a + 5_000, "{a} {b}");
        // Since boot: well under the Unix time.
        assert!(b < c.wall_ms() / 2, "{b}");
        let id = c.boot_id();
        assert!(id.len() >= 32, "{id:?}");
        assert_eq!(id, c.boot_id());
        // The awake clock runs too. It may be ahead of the boot clock: on macOS the two drift
        // apart (7 s after 13 hours awake on the M3).
        let a = c.awake_ms();
        std::thread::sleep(std::time::Duration::from_millis(30));
        let a2 = c.awake_ms();
        assert!(a > 0 && a2 >= a + 25 && a2 < a + 5_000, "{a} {a2}");
    }

    #[test]
    fn readings_carry_over_between_the_clocks() {
        let now = Now {
            wall: 1_000_000,
            boot: 5_000,
            awake: 4_000,
        };
        assert_eq!(now.boot_of(1_000_500), 5_500);
        assert_eq!(now.boot_of(999_000), 4_000);
        // Before the boot: the boot's start.
        assert_eq!(now.boot_of(10), 0);
        assert_eq!(now.wall_of(5_500), 1_000_500);
        assert_eq!(now.wall_of(4_000), 999_000);
        assert_eq!(now.wall_of(now.boot_of(1_234_567)), 1_234_567);
    }
}
