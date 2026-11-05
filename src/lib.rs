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

/// This build's version; members talk only to members on the same version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
