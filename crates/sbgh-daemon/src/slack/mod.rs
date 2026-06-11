//! Slack ad-hoc profiling connector (item `0002`, iteration v5).
//!
//! Slack is a new **trigger source** + **reporting surface** for the *ad-hoc,
//! no-commit* case ("profile this tx/block from yesterday"). The code under
//! test is a constant (`[slack].default_repository`/`default_rev`); the
//! workload is the variable (`--txid`/`--block`/…), resolved from an
//! `@BenchBot` mention.
//!
//! This module is built in slices (v5 phases): [`workload`] is the pure
//! resolve-then-validate seam, [`connector`] the mention→job orchestration,
//! [`target`] the startup repo resolution, [`api_client`] the Web API client,
//! [`socket`] the Socket Mode receive loop, and [`timeline`] the live
//! `plan`-card reporting surface — all wired into `main` behind
//! `[slack].enabled`.

pub mod api_client;
pub mod client;
pub mod connector;
pub mod socket;
pub mod target;
pub mod timeline;
pub mod workload;
