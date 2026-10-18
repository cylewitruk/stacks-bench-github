//! Audited fleet cancellation with an explicit blast radius and pause generation.

use anyhow::ensure;
use clap::{Args, ValueEnum};
use sbgh_api::{Client, FleetAbortRequest, FleetAbortScope};
use uuid::Uuid;

use crate::output::{self, OutputArgs};

/// Explicit CLI selection, independent from JSON spelling.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Scope {
    /// Executing work, including expired attempts awaiting cleanup.
    Active,
    /// All currently queued, claimed and executing work.
    AllPending,
}

/// No implicit pause, scope, retry target expansion or worker policy changes.
#[derive(Debug, Args)]
pub struct AbortArgs {
    /// Required cancellation blast radius.
    #[arg(long)]
    scope: Scope,
    /// Exact paused scheduler generation, obtained from scheduler status/pause.
    #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
    generation: i64,
    /// Explanation retained with the frozen job selection.
    #[arg(long)]
    reason: String,
    /// Reuse only when retrying the same uncertain request.
    #[arg(long)]
    request_id: Option<Uuid>,
    /// Structured receipt on stdout; diagnostics remain on stderr.
    #[command(flatten)]
    output: OutputArgs,
}

/// Send one attributable cancellation request, never wait implicitly for teardown.
pub async fn run(client: &Client, args: AbortArgs) -> anyhow::Result<()> {
    ensure!(
        !args.reason.trim().is_empty() && args.reason.chars().count() <= 1024,
        "reason must contain 1–1024 characters and not be blank"
    );
    let request_id = args
        .request_id
        .unwrap_or_else(Uuid::new_v4);
    ensure!(!request_id.is_nil(), "request_id must not be nil");
    eprintln!("fleet abort request_id={request_id}");
    let receipt = client
        .abort_fleet(&FleetAbortRequest {
            request_id: request_id.to_string(),
            scope: match args.scope {
                Scope::Active => FleetAbortScope::Active,
                Scope::AllPending => FleetAbortScope::AllPending,
            },
            scheduler_generation: args.generation,
            reason: args.reason,
        })
        .await?;
    output::print(
        &receipt,
        &format!(
            "abort request_id={} generation={} selected={} accepted={} cleanup=not_confirmed\n",
            output::text(&receipt.request_id),
            receipt.scheduler_generation,
            receipt.selected_job_ids.len(),
            receipt.accepted_job_ids.len()
        ),
        &args.output,
    )
}
