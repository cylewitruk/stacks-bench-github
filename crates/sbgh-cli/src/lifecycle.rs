//! Explicit worker maintenance commands with immutable receipts.

use std::time::Duration;

use anyhow::{Context, ensure};
use clap::{Args, ValueEnum};
use sbgh_api::{
    Client, CordonSemantic, WorkerLifecycleAction, WorkerLifecycleRequest, WorkerMaintenanceView,
};
use tokio::time::{self, Instant};
use uuid::Uuid;

use crate::output::{self, OutputArgs};

/// Read-only snapshot selection and presentation.
#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Worker registry UUID.
    #[arg(long)]
    worker_id: Uuid,
    /// Structured snapshot output.
    #[command(flatten)]
    output: OutputArgs,
}

/// A database-observable maintenance milestone, not a host service-state check.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum WaitUntil {
    /// No active attempts or pending cleanup; a resident worker stays online.
    Quiescent,
    /// Quiescent and the pinned session has acknowledged deregistration.
    Deregistered,
    /// No worker-wide admission blockers; matching job placement is still required.
    Schedulable,
}

/// Bound a read-only wait to one policy generation and session identity.
#[derive(Debug, Args)]
pub struct WaitArgs {
    /// Worker and output selection.
    #[command(flatten)]
    target: StatusArgs,
    /// Required milestone; neither condition changes policy or service state.
    #[arg(long, value_enum, default_value = "quiescent")]
    until: WaitUntil,
    /// Pin a receipt's generation; otherwise pin the first observed generation.
    #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
    generation: Option<i64>,
    /// Pin the expected session; otherwise pin the first observed session.
    #[arg(long)]
    session_id: Option<Uuid>,
    /// Total deadline, including slow or stalled API requests.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
    timeout_seconds: u64,
}

/// Print a read-only maintenance snapshot.
pub async fn status(client: &Client, args: StatusArgs) -> anyhow::Result<()> {
    let snapshot = client
        .worker_maintenance(&args.worker_id.to_string())
        .await?;
    output::print(&snapshot, &status_line(&snapshot), &args.output)
}

/// Wait without silently following a replacement session or another operator's policy.
pub async fn wait(client: &Client, args: WaitArgs) -> anyhow::Result<()> {
    let snapshot =
        time::timeout(Duration::from_secs(args.timeout_seconds), wait_inner(client, &args))
            .await
            .context("worker wait timed out; policy was not changed")??;
    output::print(&snapshot, &status_line(&snapshot), &args.target.output)
}

/// Enforce explicit scheduling intent and preserve the initially observed session.
async fn wait_inner(client: &Client, args: &WaitArgs) -> anyhow::Result<WorkerMaintenanceView> {
    let mut generation = args.generation;
    let mut session = args
        .session_id
        .map(|id| Some(id.to_string()));
    let mut last_progress = Instant::now() - Duration::from_secs(5);
    loop {
        let snapshot = client
            .worker_maintenance(
                &args
                    .target
                    .worker_id
                    .to_string(),
            )
            .await?;
        if matches!(args.until, WaitUntil::Schedulable) {
            ensure!(
                !snapshot.cordoned,
                "worker is cordoned; uncordon it explicitly before waiting for admission"
            );
        } else {
            ensure!(
                snapshot.cordoned,
                "worker is not cordoned; cordon it explicitly before maintenance"
            );
        }
        let expected_generation = *generation.get_or_insert(snapshot.generation);
        ensure!(
            snapshot.generation == expected_generation,
            "worker lifecycle generation changed; inspect status before continuing"
        );
        let expected_session = session.get_or_insert_with(|| snapshot.session_id.clone());
        ensure!(
            snapshot.session_id == *expected_session,
            "worker session changed; inspect its replacement before continuing"
        );
        let reached = match args.until {
            WaitUntil::Schedulable => {
                let admission = snapshot.admission.as_ref().context("daemon does not report admission blockers; upgrade it before using this wait")?;
                admission.schedulable && admission.blockers.is_empty()
            }
            WaitUntil::Quiescent => snapshot.quiescent(),
            WaitUntil::Deregistered => {
                ensure!(
                    snapshot.session_id.is_some(),
                    "worker has no session to await deregistration"
                );
                snapshot.quiescent()
                    && !snapshot.session_live
                    && snapshot
                        .session_status
                        .as_deref()
                        == Some("deregistered")
            }
        };
        if reached {
            return Ok(snapshot);
        }
        if last_progress.elapsed() >= Duration::from_secs(5) {
            eprint!("{}", status_line(&snapshot));
            last_progress = Instant::now();
        }
        time::sleep(Duration::from_secs(1)).await;
    }
}

/// Concise state and debt, keeping terminal escape sequences out of human output.
fn status_line(snapshot: &WorkerMaintenanceView) -> String {
    let semantic = match snapshot.cordon_semantic {
        CordonSemantic::Resident => "resident",
        CordonSemantic::ExitAfterIdle => "exit_after_idle",
        CordonSemantic::Unknown => "unknown",
    };
    let admission = snapshot
        .admission
        .as_ref()
        .map(|state| {
            if state.schedulable && state.blockers.is_empty() {
                "schedulable".to_string()
            } else {
                format!("blocked({})", state.blockers.join(","))
            }
        })
        .unwrap_or_else(|| "unknown".into());
    let preflight = snapshot.preflight.as_ref().map(|report| format!(
        "reported(revision={},completed_at_ms={},executable_sha256={},configuration_sha256={})",
        report.revision, report.completed_at_ms, report.executable_sha256, report.configuration_sha256
    )).unwrap_or_else(|| "unreported".into());
    format!(
        "worker={} enabled={} generation={} cordoned={} session={} state={} live={} attempts={} cleanup={} cordon_semantic={} protocol={} features={} admission={}\n",
        output::text(&snapshot.worker_id),
        snapshot.enabled,
        snapshot.generation,
        snapshot.cordoned,
        output::text(
            snapshot
                .session_id
                .as_deref()
                .unwrap_or("-")
        ),
        output::text(
            snapshot
                .session_status
                .as_deref()
                .unwrap_or("unknown")
        ),
        snapshot.session_live,
        snapshot.active_attempts,
        snapshot.pending_cleanup,
        semantic,
        snapshot
            .protocol_revision
            .map(|revision| revision.to_string())
            .unwrap_or_else(|| "unknown".into()),
        output::text(
            &snapshot
                .protocol_features
                .join(",")
        ),
        output::text(&admission)
    ) + &format!("preflight={}\n", output::text(&preflight))
}

/// Shared target, audit and presentation arguments for worker maintenance.
#[derive(Debug, Args)]
pub struct ChangeArgs {
    /// Worker registry UUID.
    #[arg(long)]
    worker_id: Uuid,
    /// Explanation retained in the administrative audit.
    #[arg(long)]
    reason: String,
    /// Reuse only for the same request after an uncertain response.
    #[arg(long)]
    request_id: Option<Uuid>,
    /// Structured receipt output.
    #[command(flatten)]
    output: OutputArgs,
}

/// Apply only the selected action; never implicitly start/stop a service or uncordon.
pub async fn change(
    client: &Client,
    action: WorkerLifecycleAction,
    args: ChangeArgs,
) -> anyhow::Result<()> {
    ensure!(
        !args.reason.trim().is_empty() && args.reason.chars().count() <= 1024,
        "reason must be nonempty and at most 1024 characters"
    );
    let request_id = args
        .request_id
        .unwrap_or_else(Uuid::new_v4)
        .to_string();
    eprintln!("request_id={request_id}");
    let receipt = client
        .change_worker_lifecycle(
            &args.worker_id.to_string(),
            &WorkerLifecycleRequest {
                request_id,
                action,
                reason: args.reason,
            },
        )
        .await?;
    let action = match receipt.action {
        WorkerLifecycleAction::Cordon => "cordon",
        WorkerLifecycleAction::Uncordon => "uncordon",
        WorkerLifecycleAction::ShutdownAfterIdle => "shutdown-after-idle",
    };
    output::print(
        &receipt,
        &format!(
            "{} action={} generation={} target_session={}\n",
            receipt.worker_id,
            action,
            receipt.generation,
            receipt
                .target_session_id
                .as_deref()
                .unwrap_or("-")
        ),
        &args.output,
    )
}
