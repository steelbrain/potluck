//! potluck: a language-generic build and test cluster for a handful of Linux and macOS machines.
//!
//! Every machine runs the same daemon, and the daemons talk to each other directly. One machine
//! submits work and is the source of truth for its repositories; jobs sit in one global queue and
//! whichever machine can meet a job's requirements takes it. A job runs in a workspace (one
//! commit's tree on one machine) that outlives the job, so a build and a later test run can share
//! it. Everything is driven by scripts and agents: `--json` everywhere, no prompts.

pub mod cli;
pub mod client;
pub mod config;
pub mod cow;
pub mod daemon;
pub mod envcapture;
pub mod exec;
pub mod facts;
pub mod gitinfo;
pub mod jobauth;
pub mod mesh;
pub mod preflight;
pub mod proto;
pub mod recipe;
pub mod sched;
pub mod service;
pub mod store;
pub mod supervise;
pub mod tunnel;
pub mod types;
pub mod update;
pub mod why;
pub mod workspace;

/// The developer's `$POTLUCK_USER_KEY` is no unit test's: it is cleared before main, so nothing
/// a test runs reads, signs with or makes that key (tests/common does the same for every test
/// binary).
#[cfg(test)]
#[used]
#[cfg_attr(target_os = "macos", unsafe(link_section = "__DATA,__mod_init_func"))]
#[cfg_attr(target_os = "linux", unsafe(link_section = ".init_array"))]
static NO_USER_KEY: extern "C" fn() = {
    extern "C" fn clear() {
        // SAFETY: this runs before main, while the test process has one thread, so nothing else
        // reads or writes the environment meanwhile.
        unsafe { std::env::remove_var("POTLUCK_USER_KEY") };
    }
    clear
};

/// This build's version; members talk only to members on the same version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
