//! `/api/jobs` — benchmark run visibility.

use axum::Json;
use axum::extract::{Path, Query, State};
use sbgh_api::{
    BenchmarkReportDetail, BlockValidationReportDetail, BuildOnlyReportDetail,
    EnqueueBlockValidationRequest, EnqueueJobResponse, InvalidBlockDetail, JobView,
    ReportArtifactView, ReportIdentityView, ReportLifecycleView, ReportRange, SubmissionReportView,
    TaskReportView,
};
use sbgh_core::models::{
    BuildTarget, GitRefKind, Job, JobIntent, JobSource, QueuedEventDetail, TaskKind,
};
use sbgh_core::submission::{
    BlockValidationPlan, ProducerKey, ResolvedTaskSource, SchedulingConstraints, SubmissionActor,
    SubmissionCommand, SubmissionDisposition, SubmissionProvenance, TaskPlan,
};
use sbgh_fleet::{BlockValidationPayload, InclusiveRange, TaskPayload, Validate, ValidationEpoch};
use serde::Deserialize;

use crate::api::conv::enum_str;
use crate::api::error::ApiErr;
use crate::api::state::ApiState;

/// `job_status` enum values — validated before binding so a bad value is a
/// clean 400, not a Postgres cast error.
const JOB_STATUSES: &[&str] = &["queued", "claimed", "running", "completed", "failed", "cancelled"];

#[derive(Debug, Deserialize)]
pub struct ListParams {
    status: Option<String>,
    limit: Option<i64>,
}

fn view(r: Job) -> JobView {
    JobView {
        id: r.id.to_string(),
        install_id: r.github_installation_id,
        repo_id: r.github_repo_id,
        status: enum_str(&r.status),
        source: enum_str(&r.source),
        intent: enum_str(&r.intent),
        task_kind: enum_str(&r.task_kind),
        build_target: enum_str(&r.build_target),
        git_ref_kind: enum_str(&r.git_ref_kind),
        git_ref_display: r.git_ref_display,
        commit: r.git_commit_hash,
        created_at: r.created_at.to_rfc3339(),
    }
}

pub async fn list(
    State(s): State<ApiState>,
    Query(p): Query<ListParams>,
) -> Result<Json<Vec<JobView>>, ApiErr> {
    if let Some(st) = &p.status {
        if !JOB_STATUSES.contains(&st.as_str()) {
            return Err(ApiErr::bad_request(format!("unknown status {st:?}")));
        }
    }
    let limit = p
        .limit
        .unwrap_or(50)
        .clamp(1, 500);

    let rows = sbgh_postgres::application::list_jobs(&s.pool, p.status.as_deref(), limit).await?;
    Ok(Json(
        rows.into_iter()
            .map(view)
            .collect(),
    ))
}

pub async fn report(
    State(state): State<ApiState>,
    Path(submission_id): Path<uuid::Uuid>,
) -> Result<Json<SubmissionReportView>, ApiErr> {
    let report = sbgh_postgres::application::submission_report(&state.pool, submission_id)
        .await?
        .ok_or_else(|| ApiErr::not_found(format!("submission {submission_id} not found")))?;
    Ok(Json(report_view(report)))
}

fn report_view(report: sbgh_core::reporting::SubmissionReportView) -> SubmissionReportView {
    use sbgh_core::reporting::{
        BlockValidationVerdict, ReportIdentity, ReportLifecycle, TaskReport,
    };

    let sbgh_core::reporting::SubmissionReportView {
        identity,
        lifecycle,
        task,
        artifacts,
    } = report;
    let ReportIdentity {
        submission_id,
        current_job_id,
        current_attempt_id,
        task_kind,
        source,
        repository,
        commit,
    } = identity;
    let ReportLifecycle {
        state,
        phase,
        completed_jobs,
        total_jobs,
        failure,
    } = lifecycle;
    let task = match task {
        TaskReport::Benchmark(detail) => TaskReportView::Benchmark(BenchmarkReportDetail {
            requested_runs: detail.requested_runs,
            completed_runs: detail.completed_runs,
        }),
        TaskReport::BuildOnly(detail) => TaskReportView::BuildOnly(BuildOnlyReportDetail {
            cache_outcome: detail.cache_outcome,
        }),
        TaskReport::BlockValidation(detail) => {
            TaskReportView::BlockValidation(BlockValidationReportDetail {
                requested_range: detail
                    .requested_range
                    .map(|range| ReportRange {
                        start: range.start,
                        end: range.end,
                    }),
                observed_range: detail
                    .observed_range
                    .map(|range| ReportRange {
                        start: range.start,
                        end: range.end,
                    }),
                verdict: detail
                    .verdict
                    .map(|verdict| match verdict {
                        BlockValidationVerdict::Valid => "valid".into(),
                        BlockValidationVerdict::Invalid => "invalid".into(),
                    }),
                checked_blocks: detail.checked_blocks,
                chainstate_origin: detail.chainstate_origin,
                invalid_blocks: detail
                    .invalid_blocks
                    .into_iter()
                    .map(|invalid| InvalidBlockDetail {
                        shard: invalid.shard,
                        block: invalid.block,
                        reason: invalid.reason,
                    })
                    .collect(),
            })
        }
    };
    SubmissionReportView {
        identity: ReportIdentityView {
            submission_id: submission_id.to_string(),
            current_job_id: current_job_id.map(|id| id.to_string()),
            current_attempt_id: current_attempt_id.map(|id| id.to_string()),
            task_kind: enum_str(&task_kind),
            source: enum_str(&source),
            repository,
            commit,
        },
        lifecycle: ReportLifecycleView {
            state: enum_str(&state),
            phase,
            completed_jobs,
            total_jobs,
            failure,
        },
        task,
        artifacts: artifacts
            .into_iter()
            .map(|artifact| ReportArtifactView {
                name: artifact.name,
                key: artifact.key,
            })
            .collect(),
    }
}

pub async fn enqueue_block_validation(
    State(state): State<ApiState>,
    Json(request): Json<EnqueueBlockValidationRequest>,
) -> Result<Json<EnqueueJobResponse>, ApiErr> {
    let epoch = match request.epoch.as_str() {
        "pre_nakamoto" => ValidationEpoch::PreNakamoto,
        "nakamoto" => ValidationEpoch::Nakamoto,
        other => return Err(ApiErr::bad_request(format!("unknown validation epoch {other:?}"))),
    };
    if !matches!(request.commit.len(), 40 | 64)
        || !request
            .commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ApiErr::bad_request(
            "commit must be a 40- or 64-character hexadecimal object ID",
        ));
    }
    let required_worker_id = request
        .worker_id
        .as_deref()
        .map(str::parse::<uuid::Uuid>)
        .transpose()
        .map_err(|_| ApiErr::bad_request("worker_id must be a UUID"))?;
    let payload = TaskPayload::BlockValidation(BlockValidationPayload {
        epoch,
        range: InclusiveRange {
            start: request.range_start,
            end: request.range_end,
        },
        requested_shards: request.requested_shards,
        max_concurrency: request.max_concurrency,
        timeout_secs: request.timeout_secs,
    });
    payload
        .validate()
        .map_err(|error| ApiErr::bad_request(error.to_string()))?;
    let detail = serde_json::to_value(QueuedEventDetail::BlockValidation {
        range_start: request.range_start,
        range_end: request.range_end,
        requested_shards: request.requested_shards,
        max_concurrency: request.max_concurrency,
    })
    .map_err(|error| {
        tracing::error!(%error, "serializing block-validation provenance failed");
        ApiErr::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "could not enqueue block validation",
        )
    })?;
    let source = ResolvedTaskSource {
        github_installation_id: request.install_id,
        github_repo_id: request.repo_id,
        source: JobSource::Cli,
        intent: JobIntent::BlockValidation,
        task_kind: TaskKind::BlockValidation,
        build_target: BuildTarget::StacksInspect,
        git_ref_kind: GitRefKind::Commit,
        git_ref_display: request.commit.clone(),
        commit: request.commit,
        committed_at: None,
        workload_key: None,
    };
    let TaskPayload::BlockValidation(validation) = payload else {
        unreachable!("constructed as block validation")
    };
    let store = sbgh_postgres::PostgresJobStore::new(state.pool);
    let receipt = crate::submission::submit(
        &store,
        SubmissionCommand {
            actor: SubmissionActor::System,
            producer_key: ProducerKey {
                namespace: "admin_block_validation".into(),
                key: request.idempotency_key,
            },
            constraints: SchedulingConstraints {
                required_worker_id,
                required_measurement_profile: None,
            },
            task: TaskPlan::BlockValidation(BlockValidationPlan { source, payload: validation }),
            provenance: SubmissionProvenance {
                queued_event_detail: detail,
                github: None,
                slack: None,
            },
        },
    )
    .await?;
    Ok(Json(EnqueueJobResponse {
        submission_id: receipt
            .submission_id
            .to_string(),
        disposition: match receipt.disposition {
            SubmissionDisposition::Created => "created",
            SubmissionDisposition::AlreadySubmitted => "already_submitted",
        }
        .into(),
        initial_job_ids: receipt
            .initial_job_ids
            .into_iter()
            .map(|id| id.to_string())
            .collect(),
    }))
}
