//! Slack ad-hoc profiling connector (item `0002`, iteration v5).
//!
//! Slack is a **trigger source** + **reporting surface** for the *ad-hoc,
//! no-commit* case ("profile this tx/block from yesterday"). The code under
//! test is a constant (`[slack].default_repository`/`default_rev`); the
//! workload is the variable, resolved from an `@BenchBot` mention through the
//! shared workload/LLM seams.
//!
//! This module owns Slack-specific adaptation: [`connector`] is the
//! mention→job orchestration, [`target`] the startup repo resolution,
//! [`api_client`] the Web API client, [`socket`] the Socket Mode receive loop,
//! and [`timeline`] the live `plan`-card reporting surface — all wired into
//! `main` behind `[slack].enabled`.

pub mod api_client;
pub mod card;
pub mod client;
pub mod connector;
pub mod socket;
pub mod stream;
pub mod target;
pub mod timeline;
