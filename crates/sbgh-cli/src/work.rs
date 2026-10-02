//! Operator browsing and observation through the daemon API.

use std::fmt::Write as _;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use sbgh_api::{Client, ClientError, JobInspectionView, WorkSnapshotView};
use tokio::time;

use crate::output::{self, OutputArgs};

/// Common ID and presentation options for inspecting work.
#[derive(Debug, Args)]
pub struct InspectionArgs {
    /// Job/submission UUID or unique prefix (minimum eight characters).
    pub id: String,
    /// Response presentation.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Request browsing filters.
#[derive(Debug, Args)]
pub struct ListArgs {
    /// Filter by aggregate state.
    #[arg(long)]
    pub status: Option<String>,
    /// Show only queued or running requests.
    #[arg(long, conflicts_with = "status")]
    pub active: bool,
    /// Maximum rows (server default 50).
    #[arg(long)]
    pub limit: Option<u32>,
    /// Response presentation.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Options for following a pinned request or execution.
#[derive(Debug, Args)]
pub struct WatchArgs {
    /// Selected ID and output format.
    #[command(flatten)]
    pub inspection: InspectionArgs,
    /// Poll interval in seconds; stopping watch never cancels execution.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub interval: u64,
}

/// Submission-oriented commands, sharing the top-level inspection handlers.
#[derive(Debug, Subcommand)]
pub enum SubmissionsAction {
    /// List recent requests and their aggregate state.
    List(ListArgs),
    /// Explain a request or job in readable text.
    Show(InspectionArgs),
    /// Follow progress through completion.
    Watch(WatchArgs),
    /// Show recorded task results.
    Results(InspectionArgs),
}

/// Run a submission command without coupling its output to persistence.
pub async fn run_submissions(client: &Client, action: SubmissionsAction) -> anyhow::Result<()> {
    match action {
        SubmissionsAction::List(args) => list(client, args).await,
        SubmissionsAction::Show(args) => show(client, args).await,
        SubmissionsAction::Watch(args) => watch(client, args).await,
        SubmissionsAction::Results(args) => results(client, args).await,
    }
}

/// Browse request summaries, retaining typed data for JSON rendering.
async fn list(client: &Client, args: ListArgs) -> anyhow::Result<()> {
    let rows = client
        .list_submissions(args.status.as_deref(), args.active, args.limit)
        .await
        .context("list submissions")?;
    let mut human = String::new();
    if rows.is_empty() {
        human.push_str("(no submissions)\n");
    }
    for row in &rows {
        writeln!(
            human,
            "{}  {:<10} {}/{} jobs  {}  {}  {}",
            row.id,
            row.state,
            row.completed_jobs,
            row.total_jobs,
            output::text(&row.repository),
            output::text(&row.reference),
            row.created_at
        )?;
    }
    output::print(&rows, &human, &args.output)
}

/// Display request context and the explicitly selected job(s).
pub async fn show(client: &Client, args: InspectionArgs) -> anyhow::Result<()> {
    let snapshot = client
        .inspect_work(&args.id)
        .await
        .context("inspect work")?;
    output::print(&snapshot, &render(&snapshot, Utc::now(), true), &args.output)
}

/// Display task-specific outputs for each selected execution.
pub async fn results(client: &Client, args: InspectionArgs) -> anyhow::Result<()> {
    let snapshot = client
        .inspect_work(&args.id)
        .await
        .context("load results")?;
    output::print(&snapshot, &render_results(&snapshot), &args.output)
}

/// Select precisely one job or every materialized job in a request.
fn selected_jobs(snapshot: &WorkSnapshotView) -> impl Iterator<Item = &JobInspectionView> {
    snapshot
        .jobs
        .iter()
        .filter(|job| {
            snapshot
                .target
                .job_id
                .as_ref()
                .is_none_or(|id| id == &job.id)
        })
}

/// State of the selected scope, rather than whichever job the report last referenced.
fn state(snapshot: &WorkSnapshotView) -> &str {
    match snapshot
        .target
        .job_id
        .as_ref()
    {
        Some(id) => snapshot
            .jobs
            .iter()
            .find(|job| &job.id == id)
            .map(|job| job.status.as_str())
            .unwrap_or("unknown"),
        None => {
            &snapshot
                .report
                .lifecycle
                .state
        }
    }
}

/// Whether observation can finish without affecting execution.
fn terminal(state: &str) -> bool {
    matches!(state, "completed" | "failed" | "cancelled")
}

/// Format age independently for worker heartbeat and execution progress.
fn age(timestamp: &str, now: DateTime<Utc>) -> String {
    DateTime::parse_from_rfc3339(timestamp)
        .map(|timestamp| {
            format!(
                "{} ago",
                output::duration(
                    now.signed_duration_since(timestamp)
                        .num_seconds()
                )
            )
        })
        .unwrap_or_else(|_| "unknown".into())
}

/// Build a human observation without writing or fetching anything.
fn render(snapshot: &WorkSnapshotView, now: DateTime<Utc>, detailed: bool) -> String {
    let mut human = format!(
        "{} · {}\nSubmission {} · {}/{} jobs complete\n",
        output::text(
            &snapshot
                .report
                .identity
                .repository
        ),
        state(snapshot),
        snapshot.target.submission_id,
        snapshot
            .report
            .lifecycle
            .completed_jobs,
        snapshot
            .report
            .lifecycle
            .total_jobs
    );
    for job in selected_jobs(snapshot) {
        let _ = writeln!(
            human,
            "Job {} · {} · {}{}",
            job.id,
            job.status,
            output::text(&job.task_kind),
            job.phase
                .as_deref()
                .map(|phase| format!(" · {}", output::text(phase)))
                .unwrap_or_default()
        );
        let _ = writeln!(
            human,
            "  {} @ {} · spec {} / run {}",
            output::text(&job.reference),
            job.commit
                .as_deref()
                .unwrap_or("unresolved"),
            job.spec_index + 1,
            job.run_index + 1
        );
        if let Some(attempt) = &job.attempt {
            let elapsed = attempt
                .started_at
                .as_deref()
                .and_then(|time| DateTime::parse_from_rfc3339(time).ok())
                .map(|start| {
                    let end = attempt
                        .finished_at
                        .as_deref()
                        .and_then(|time| DateTime::parse_from_rfc3339(time).ok())
                        .map(|time| time.with_timezone(&Utc))
                        .unwrap_or(now);
                    output::duration(
                        end.signed_duration_since(start)
                            .num_seconds(),
                    )
                })
                .unwrap_or_else(|| "not started".into());
            let _ = writeln!(
                human,
                "  Worker {} · elapsed {} · heartbeat {}",
                output::text(&attempt.worker_name),
                elapsed,
                age(&attempt.heartbeat_at, now)
            );
            if detailed {
                let _ = writeln!(human, "  Attempt {} · {}", attempt.id, attempt.status);
            }
        } else if job.status == "queued" {
            let _ =
                writeln!(human, "  Waiting for a worker · queued {}", age(&job.created_at, now));
        } else {
            human.push_str("  No worker attempt recorded\n");
        }
        if !terminal(&job.status) {
            if let Some(progress) = &job.progress {
                let count = match progress
                    .total
                    .filter(|total| *total > 0)
                {
                    Some(total) => format!(
                        "{}/{} ({:.1}%)",
                        progress.current,
                        total,
                        progress.current.min(total) as f64 * 100.0 / total as f64
                    ),
                    None => progress.current.to_string(),
                };
                let _ = writeln!(
                    human,
                    "  {} {} · progress {}",
                    output::text(&progress.phase),
                    count,
                    age(&progress.received_at, now)
                );
                if let Some(message) = &progress.message {
                    let _ = writeln!(human, "  {}", output::text(message));
                }
            } else {
                human.push_str("  No progress reported in this phase yet\n");
            }
        }
        if let Some(origin) = &job.chainstate_origin {
            let _ = writeln!(human, "  Chainstate {}", output::text(origin));
        }
        if detailed && !job.effective_args.is_empty() {
            let _ = writeln!(
                human,
                "  Arguments {}",
                job.effective_args
                    .iter()
                    .map(|arg| output::text(arg))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        if let Some(failure) = &job.failure {
            let _ = writeln!(human, "  Failure: {}", output::text(failure));
        }
    }
    if snapshot
        .target
        .job_id
        .is_none()
    {
        let pending = snapshot
            .report
            .lifecycle
            .total_jobs
            .saturating_sub(snapshot.jobs.len() as u32);
        if pending > 0 {
            let _ = writeln!(human, "{pending} planned executions have not been queued yet");
        }
    }
    human
}

/// Render promoted measurements and explicit absence of results.
fn render_results(snapshot: &WorkSnapshotView) -> String {
    let mut human = format!(
        "{} · {}\n",
        output::text(
            &snapshot
                .report
                .identity
                .repository
        ),
        state(snapshot)
    );
    for job in selected_jobs(snapshot) {
        let _ = writeln!(
            human,
            "Job {} @ {}",
            job.id,
            job.commit
                .as_deref()
                .unwrap_or("unresolved")
        );
        if let Some(origin) = &job.chainstate_origin {
            let _ = writeln!(human, "  Chainstate {}", output::text(origin));
        }
        if let Some(metrics) = &job.metrics {
            let _ = writeln!(
                human,
                "  Blocks measured {} · warmup {} · transactions {}",
                metrics.measured_blocks, metrics.warmup_blocks, metrics.transactions
            );
            let _ = writeln!(
                human,
                "  Run {} · replay {}",
                output::duration(metrics.envelope_duration_us / 1_000_000),
                output::duration(metrics.replay_duration_us / 1_000_000)
            );
            let _ = writeln!(
                human,
                "  Setup {} µs · execution {} µs · commit {} µs (totals)",
                metrics.setup_duration_us,
                metrics.execution_duration_us,
                metrics.commit_duration_us
            );
            let _ = writeln!(
                human,
                "  Clarity runtime {} · read {} / write {}",
                metrics.clarity_runtime,
                output::bytes(metrics.read_length),
                output::bytes(metrics.write_length)
            );
        } else if job.task_kind == "block_validation"
            && let Some(result) = &job.result
        {
            let verdict = match result
                .get("valid")
                .and_then(serde_json::Value::as_bool)
            {
                Some(true) => "valid",
                Some(false) => "invalid",
                None => "unknown",
            };
            let _ = writeln!(
                human,
                "  Verdict {} · checked blocks {}",
                verdict,
                result
                    .get("checked_blocks")
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".into())
            );
        } else if job.task_kind == "build_only" && job.status == "completed" {
            human.push_str("  Build completed\n");
        } else {
            let _ = writeln!(human, "  {} · no promoted measurements yet", job.status);
        }
        if let Some(failure) = &job.failure {
            let _ = writeln!(human, "  Failure: {}", output::text(failure));
        }
        let _ = writeln!(human, "  Artifacts: sbgh-cli artifacts ls {}", job.id);
    }
    human
}

/// Follow a pinned scope; Ctrl-C only stops local observation.
pub async fn watch(client: &Client, args: WatchArgs) -> anyhow::Result<()> {
    let mut snapshot = client
        .inspect_work(&args.inspection.id)
        .await
        .context("inspect work")?;
    let pinned = match snapshot
        .target
        .job_id
        .as_ref()
    {
        Some(id) => format!("job:{id}"),
        None => format!("submission:{}", snapshot.target.submission_id),
    };
    let tty = io::stdout().is_terminal()
        && std::env::var("TERM").as_deref() != Ok("dumb")
        && !args.inspection.output.json;
    let mut previous = None;
    let mut drew_frame = false;
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    loop {
        let changed = previous.as_ref() != Some(&snapshot);
        if changed || tty {
            let finished = terminal(state(&snapshot));
            let mut stdout = io::stdout().lock();
            if args.inspection.output.json {
                serde_json::to_writer(&mut stdout, &snapshot)?;
                writeln!(stdout)?;
            } else {
                if tty {
                    // Restoring the starting cursor also handles wrapped lines.
                    if drew_frame {
                        write!(stdout, "\x1b[u\x1b[J")?;
                    } else {
                        write!(stdout, "\x1b[s")?;
                        drew_frame = true;
                    }
                }
                if !tty {
                    writeln!(stdout, "[{}]", Utc::now().to_rfc3339())?;
                }
                let mut human = render(&snapshot, Utc::now(), false);
                if finished {
                    human.push_str(&render_results(&snapshot));
                }
                write!(stdout, "{human}")?;
            }
            stdout.flush()?;
        }
        if terminal(state(&snapshot)) {
            anyhow::ensure!(state(&snapshot) == "completed", "watched work {}", state(&snapshot));
            return Ok(());
        }
        previous = Some(snapshot.clone());
        tokio::select! {
            result = &mut interrupted => { result?; return Ok(()); }
            () = time::sleep(Duration::from_secs(args.interval)) => {}
        }
        tokio::select! {
            result = &mut interrupted => { result?; return Ok(()); }
            response = client.inspect_work(&pinned) => match response {
                Ok(update) => snapshot = update,
                Err(error @ ClientError::Api { status: 400..=499, .. }) => return Err(error.into()),
                Err(error) => {
                    eprintln!("watch: could not refresh ({error}); retrying");
                }
            }
        }
    }
}

/// Resolve an artifact selection without silently choosing among jobs.
pub async fn artifact_job(client: &Client, id: &str, name: Option<&str>) -> anyhow::Result<String> {
    let snapshot = client
        .inspect_work(id)
        .await
        .context("resolve artifact work")?;
    if let Some(id) = snapshot.target.job_id {
        return Ok(id);
    }
    if snapshot.jobs.len() == 1 {
        return Ok(snapshot.jobs[0].id.clone());
    }
    if let Some(name) = name {
        let mut matches = Vec::new();
        for job in &snapshot.jobs {
            if client
                .list_job_artifacts(&job.id)
                .await?
                .iter()
                .any(|artifact| artifact.name == name)
            {
                matches.push(job.id.clone());
            }
        }
        if matches.len() == 1 {
            return Ok(matches.remove(0));
        }
        anyhow::bail!(
            "artifact {name} matches {} jobs; provide a job ID (jobs: {})",
            matches.len(),
            snapshot
                .jobs
                .iter()
                .map(|job| job.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    anyhow::bail!(
        "submission has {} jobs; provide a job ID or list via `show {id}`",
        snapshot.jobs.len()
    )
}

/// Artifacts grouped by their owning job, including multi-job requests.
#[derive(Debug, serde::Serialize)]
pub struct ArtifactListing {
    /// Execution identity.
    pub job_id: String,
    /// Recorded artifact metadata.
    pub artifacts: Vec<sbgh_api::JobArtifactView>,
}

/// List outputs of all selected jobs without flattening ambiguous names.
pub async fn list_artifacts(client: &Client, id: &str, output: OutputArgs) -> anyhow::Result<()> {
    let snapshot = client
        .inspect_work(id)
        .await
        .context("resolve artifact work")?;
    let mut listings = Vec::new();
    let mut human = String::new();
    for job in selected_jobs(&snapshot) {
        let artifacts = client
            .list_job_artifacts(&job.id)
            .await?;
        if snapshot
            .target
            .job_id
            .is_none()
            && snapshot.jobs.len() > 1
        {
            writeln!(human, "Job {}", job.id)?;
        }
        if artifacts.is_empty() {
            writeln!(human, "(no artifacts; job {})", job.status)?;
        }
        for artifact in &artifacts {
            writeln!(human, "{:>12}  {}", artifact.size, output::text(&artifact.name))?;
        }
        listings.push(ArtifactListing {
            job_id: job.id.clone(),
            artifacts,
        });
    }
    output::print(&listings, &human, &output)
}

/// Download receipt, separate from the artifact bytes.
#[derive(Debug, serde::Serialize)]
pub struct DownloadReceipt {
    /// Resolved execution identity.
    pub job_id: String,
    /// Job-relative artifact name.
    pub name: String,
    /// Destination path.
    pub output: PathBuf,
    /// Downloaded bytes.
    pub bytes: u64,
}

/// Download through a sibling temporary file; errors preserve any existing destination.
pub async fn download(
    client: &Client,
    id: &str,
    name: &str,
    destination: PathBuf,
    output: OutputArgs,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !destination.try_exists()?,
        "destination {} already exists",
        destination.display()
    );
    let job_id = artifact_job(client, id, Some(name)).await?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temporary =
        tempfile::NamedTempFile::new_in(parent).context("create download temporary file")?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(temporary.path())
        .await?;
    client
        .copy_job_artifact(&job_id, name, &mut file)
        .await
        .context("download artifact")?;
    file.sync_all().await?;
    let bytes = file.metadata().await?.len();
    drop(file);
    temporary
        .persist_noclobber(&destination)
        .context("save artifact (destination must not already exist)")?;
    let receipt = DownloadReceipt {
        job_id,
        name: name.to_owned(),
        output: destination,
        bytes,
    };
    output::print(
        &receipt,
        &format!("saved {} bytes to {}\n", bytes, receipt.output.display()),
        &output,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sbgh_api::{AttemptInspectionView, ProgressInspectionView};

    /// Minimal typed observation with differing job and submission outcomes.
    fn snapshot() -> WorkSnapshotView {
        serde_json::from_value(serde_json::json!({
            "target": {"submission_id":"submission", "job_id":"selected"},
            "report": {
                "identity": {"submission_id":"submission", "current_job_id":"other", "current_attempt_id":null,
                    "task_kind":"benchmark", "source":"cli", "repository":"o/r", "commit":"abc"},
                "lifecycle": {"state":"failed", "phase":null, "completed_jobs":1, "total_jobs":2, "failure":"other failed"},
                "task": {"kind":"benchmark", "detail":{"requested_runs":2,"completed_runs":1}},
                "artifacts":[], "forensics":null
            },
            "jobs": [
                {"id":"selected", "spec_index":0, "run_index":0, "status":"completed", "task_kind":"benchmark",
                 "reference":"branch", "commit":"abc", "created_at":"2026-10-02T00:00:00Z", "effective_args":[],
                 "chainstate_origin":"vg/base", "phase":"done", "failure":null, "attempt":null, "progress":null,
                 "metrics":null, "result":null},
                {"id":"other", "spec_index":0, "run_index":1, "status":"failed", "task_kind":"benchmark",
                 "reference":"branch", "commit":"abc", "created_at":"2026-10-02T00:00:00Z", "effective_args":[],
                 "chainstate_origin":null, "phase":"error", "failure":"other failed", "attempt":null, "progress":null,
                 "metrics":null, "result":null}
            ]
        })).unwrap()
    }

    #[test]
    fn observation_respects_selected_job_instead_of_aggregate_failure() {
        let mut snapshot = snapshot();
        assert_eq!(state(&snapshot), "completed");
        assert!(terminal(state(&snapshot)));
        assert_eq!(selected_jobs(&snapshot).count(), 1);
        let rendered = render(&snapshot, Utc::now(), true);
        assert!(rendered.contains("Job selected"));
        assert!(!rendered.contains("Job other"));
        assert!(!render_results(&snapshot).contains("other failed"));
        snapshot.target.job_id = None;
        assert_eq!(state(&snapshot), "failed");
        assert_eq!(selected_jobs(&snapshot).count(), 2);
    }

    #[test]
    fn progress_and_heartbeat_have_independent_freshness() {
        let mut snapshot = snapshot();
        let job = &mut snapshot.jobs[0];
        job.status = "running".into();
        job.attempt = Some(AttemptInspectionView {
            id: "attempt".into(),
            worker_id: "worker".into(),
            worker_name: "worker\u{1b}[2J".into(),
            status: "running".into(),
            started_at: Some("2026-10-02T00:00:00Z".into()),
            finished_at: None,
            heartbeat_at: "2026-10-02T00:01:59Z".into(),
        });
        job.progress = Some(ProgressInspectionView {
            sequence: 1,
            phase: "replay".into(),
            current: 42,
            total: Some(100),
            message: Some("message\nline".into()),
            received_at: "2026-10-02T00:01:00Z".into(),
        });
        let now = DateTime::parse_from_rfc3339("2026-10-02T00:02:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let rendered = render(&snapshot, now, true);
        assert!(rendered.contains("heartbeat 1s ago"));
        assert!(rendered.contains("progress 1m 0s ago"));
        assert!(rendered.contains("42/100 (42.0%)"));
        assert!(rendered.contains("message line"));
        assert!(!rendered.contains('\u{1b}'));
        snapshot.jobs[0].status = "completed".into();
        assert!(!render(&snapshot, now, true).contains("progress 1m 0s ago"));
    }
}
