//! Authenticated operator inspection, independent of provider reporting surfaces.

use axum::Json;
use axum::extract::{Path, Query, State};
use sbgh_api::{
    AttemptInspectionView, BenchmarkMetricsView, JobInspectionView, ProgressInspectionView,
    SubmissionSummaryView, WorkSnapshotView, WorkTargetView,
};
use sbgh_core::inspection::WorkTarget;
use serde::Deserialize;

use super::conv::enum_str;
use super::error::ApiErr;
use super::jobs;
use super::state::ApiState;

/// Filters for request browsing.
#[derive(Debug, Deserialize)]
pub struct ListParams {
    /// Aggregate state filter.
    status: Option<String>,
    /// Only unfinished requests.
    #[serde(default)]
    active: bool,
    /// Maximum rows.
    limit: Option<i64>,
}

/// List requests without requiring database access from the CLI.
pub async fn list(
    State(state): State<ApiState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<SubmissionSummaryView>>, ApiErr> {
    if params
        .status
        .as_deref()
        .is_some_and(|status| {
            !matches!(status, "queued" | "running" | "completed" | "failed" | "cancelled")
        })
    {
        return Err(ApiErr::bad_request("unknown submission status"));
    }
    let rows = sbgh_postgres::application::list_submissions(
        &state.pool,
        params.status.as_deref(),
        params.active,
        params
            .limit
            .unwrap_or(50)
            .clamp(1, 500),
    )
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| SubmissionSummaryView {
                id: row.id.to_string(),
                repository: row.repository,
                source: row.source,
                state: enum_str(&row.state),
                created_at: row.created_at.to_rfc3339(),
                completed_jobs: row.completed_jobs,
                total_jobs: row.total_jobs,
                reference: row.reference,
            })
            .collect(),
    ))
}

/// Observe a job or submission selected by a UUID or unique prefix.
pub async fn show(
    State(state): State<ApiState>,
    Path(selector): Path<String>,
) -> Result<Json<WorkSnapshotView>, ApiErr> {
    inspect(state, selector).await
}

/// URL-encoded selector used by the typed client.
#[derive(Debug, Deserialize)]
pub struct InspectParams {
    /// Job or submission selector.
    id: String,
}

/// Inspect a selector supplied through a query parameter.
pub async fn show_query(
    State(state): State<ApiState>,
    Query(params): Query<InspectParams>,
) -> Result<Json<WorkSnapshotView>, ApiErr> {
    inspect(state, params.id).await
}

/// Build a typed response after resolving its identity.
async fn inspect(state: ApiState, selector: String) -> Result<Json<WorkSnapshotView>, ApiErr> {
    let target = resolve(&state, &selector).await?;
    let snapshot = sbgh_postgres::application::inspect_work(&state.pool, target)
        .await?
        .ok_or_else(|| ApiErr::not_found(format!("work {selector} not found")))?;
    Ok(Json(WorkSnapshotView {
        target: WorkTargetView {
            submission_id: snapshot
                .target
                .submission_id
                .to_string(),
            job_id: snapshot
                .target
                .job_id
                .map(|id| id.to_string()),
        },
        report: jobs::report_view(snapshot.report),
        jobs: snapshot
            .jobs
            .into_iter()
            .map(|job| JobInspectionView {
                id: job.id.to_string(),
                spec_index: job.spec_index,
                run_index: job.run_index,
                status: job.status,
                task_kind: job.task_kind,
                reference: job.reference,
                commit: job.commit,
                created_at: job.created_at.to_rfc3339(),
                effective_args: job.effective_args,
                chainstate_origin: job.chainstate_origin,
                phase: job.phase,
                failure: job.failure,
                attempt: job
                    .attempt
                    .map(|attempt| AttemptInspectionView {
                        id: attempt.id.to_string(),
                        worker_id: attempt.worker_id.to_string(),
                        worker_name: attempt.worker_name,
                        status: attempt.status,
                        started_at: attempt
                            .started_at
                            .map(|time| time.to_rfc3339()),
                        finished_at: attempt
                            .finished_at
                            .map(|time| time.to_rfc3339()),
                        heartbeat_at: attempt
                            .heartbeat_at
                            .to_rfc3339(),
                    }),
                progress: job
                    .progress
                    .map(|progress| ProgressInspectionView {
                        sequence: progress.sequence,
                        phase: progress.phase,
                        current: progress.current,
                        total: progress.total,
                        message: progress.message,
                        received_at: progress
                            .received_at
                            .to_rfc3339(),
                    }),
                metrics: job
                    .metrics
                    .map(|metric| BenchmarkMetricsView {
                        envelope_duration_us: metric.envelope_duration_us,
                        replay_duration_us: metric.replay_duration_us,
                        measured_blocks: metric.measured_blocks,
                        warmup_blocks: metric.warmup_blocks,
                        transactions: metric.transactions,
                        setup_duration_us: metric.setup_duration_us,
                        execution_duration_us: metric.execution_duration_us,
                        commit_duration_us: metric.commit_duration_us,
                        clarity_runtime: metric.clarity_runtime,
                        read_length: metric.read_length,
                        write_length: metric.write_length,
                    }),
                result: job.result,
            })
            .collect(),
    }))
}

/// Resolve an operator selector without silently choosing among matches.
pub async fn resolve(state: &ApiState, selector: &str) -> Result<WorkTarget, ApiErr> {
    let (kind, prefix) = match selector.split_once(':') {
        Some((kind @ ("job" | "submission"), prefix)) => (Some(kind), prefix),
        Some(_) => return Err(ApiErr::bad_request("ID qualifier must be job: or submission:")),
        None => (None, selector),
    };
    if !(8..=36).contains(&prefix.len())
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        || (prefix.len() == 36 && uuid::Uuid::parse_str(prefix).is_err())
    {
        return Err(ApiErr::bad_request(
            "provide a UUID or a hexadecimal UUID prefix of at least 8 characters",
        ));
    }
    let matches =
        sbgh_postgres::application::resolve_work(&state.pool, &prefix.to_ascii_lowercase(), kind)
            .await?;
    match matches.as_slice() {
        [] => Err(ApiErr::not_found(format!("no job or submission matches {selector}"))),
        [target] => Ok(target.clone()),
        _ => Err(ApiErr::conflict(format!(
            "ambiguous ID {selector}; use a longer ID or a qualifier: {}",
            matches
                .iter()
                .map(|target| match target.job_id {
                    Some(id) => format!("job:{id}"),
                    None => format!("submission:{}", target.submission_id),
                })
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}
