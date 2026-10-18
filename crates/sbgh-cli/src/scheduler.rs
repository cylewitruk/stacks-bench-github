//! Scheduler controls with presentation separated from durable state changes.

use std::time::Duration;

use anyhow::{Context, ensure};
use clap::{Args, Subcommand};
use sbgh_api::{Client, SchedulerChangeRequest, SchedulerPolicyView, SchedulerStatusView};
use tokio::time::{self, Instant};
use uuid::Uuid;

use crate::output::{self, OutputArgs};

/// Explicit fleet admission operations.
#[derive(Debug, Subcommand)]
pub enum Action {
    /// Read admission policy and remaining work without changing anything.
    Status(OutputArgs),
    /// Stop new preparation, offers, and acceptance; let active work finish.
    Pause(ChangeArgs),
    /// Restore admission without changing worker drain policy.
    Resume(ChangeArgs),
    /// Wait for paused admission and zero attempt/cleanup/projection/artifact debt.
    Wait {
        /// Require this pause generation; otherwise pin the first observed one.
        #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
        generation: Option<i64>,
        /// Maximum total wait, including API requests; timeout exits unsuccessfully.
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout_seconds: u64,
        /// Presentation of the final quiescent snapshot.
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// Audited change arguments shared by pause and resume.
#[derive(Debug, Args)]
pub struct ChangeArgs {
    /// Explanation retained in the audit; include your operator name if useful.
    #[arg(long)]
    reason: String,
    /// Reuse this UUID to retry an ambiguous request, not to issue a new change.
    #[arg(long)]
    request_id: Option<Uuid>,
    /// Presentation of the policy returned for this request.
    #[command(flatten)]
    output: OutputArgs,
}

/// Execute a scheduler operation using only the authenticated daemon API.
pub async fn run(client: &Client, action: Action) -> anyhow::Result<()> {
    match action {
        Action::Status(output) => print_status(
            &client
                .scheduler_status()
                .await?,
            &output,
        ),
        Action::Pause(args) => change(client, true, args).await,
        Action::Resume(args) => change(client, false, args).await,
        Action::Wait {
            generation,
            timeout_seconds,
            output,
        } => {
            let status =
                time::timeout(Duration::from_secs(timeout_seconds), wait(client, generation))
                    .await
                    .context("scheduler wait timed out; policy was not changed")??;
            print_status(&status, &output)
        }
    }
}

/// Retain the request identity on stderr even when a response is lost.
async fn change(client: &Client, paused: bool, args: ChangeArgs) -> anyhow::Result<()> {
    ensure!(
        !args.reason.trim().is_empty() && args.reason.chars().count() <= 1024,
        "reason must contain 1–1024 characters and not be blank"
    );
    let request_id = args
        .request_id
        .unwrap_or_else(Uuid::new_v4)
        .to_string();
    eprintln!("scheduler request_id={request_id}");
    let policy = client
        .change_scheduler(
            paused,
            &SchedulerChangeRequest {
                request_id,
                reason: args.reason,
            },
        )
        .await?;
    output::print(&policy, &policy_line(&policy), &args.output)
}

/// Pin one pause generation and fail if another operator resumes or repauses it.
async fn wait(client: &Client, mut generation: Option<i64>) -> anyhow::Result<SchedulerStatusView> {
    let mut last_progress = Instant::now() - Duration::from_secs(5);
    loop {
        let status = client
            .scheduler_status()
            .await?;
        ensure!(status.policy.paused, "scheduler is running; pause it explicitly before waiting");
        let expected = *generation.get_or_insert(status.policy.generation);
        ensure!(
            status.policy.generation == expected,
            "scheduler generation changed while waiting; inspect status before continuing"
        );
        if status.quiescent() {
            return Ok(status);
        }
        if last_progress.elapsed() >= Duration::from_secs(5) {
            eprint!("{}", status_line(&status));
            last_progress = Instant::now();
        }
        time::sleep(Duration::from_secs(1)).await;
    }
}

/// Render one snapshot using the shared command-local output policy.
fn print_status(status: &SchedulerStatusView, output: &OutputArgs) -> anyhow::Result<()> {
    output::print(status, &status_line(status), output)
}

/// Human-readable policy, with external text sanitized for terminals.
fn policy_line(policy: &SchedulerPolicyView) -> String {
    format!(
        "scheduler={} generation={} actor={} reason={}\n",
        if policy.paused { "paused" } else { "running" },
        policy.generation,
        output::text(&policy.actor),
        output::text(&policy.reason)
    )
}

/// Human-readable quiescence counters; zero alone never implies paused admission.
fn status_line(status: &SchedulerStatusView) -> String {
    format!(
        "{}quiescent={} attempts={} cleanup={} projection={} artifacts={} garbage_artifacts={}\n",
        policy_line(&status.policy),
        status.quiescent(),
        status.active_attempts,
        status.pending_cleanup,
        status.pending_projection,
        status.pending_artifacts,
        status.garbage_artifacts
    )
}
