//! `/api/jobs` — benchmark run visibility.

use axum::Json;
use axum::extract::{Path, Query, State};
use sbgh_api::{
    BenchmarkReportDetail, BlockValidationReportDetail, BlockValidationSelectionDetail,
    BlockValidationSelectionRequest, BuildOnlyReportDetail, EnqueueBlockValidationRequest,
    EnqueueJobResponse, InvalidBlockDetail, JobView, ObservedValidationIndexDetail,
    ReportArtifactView, ReportIdentityView, ReportLifecycleView, ReportRange, SubmissionReportView,
    TaskReportView, ValidationEpochSegmentDetail,
};
use sbgh_core::models::{BuildTarget, GitRefKind, Job, JobIntent, JobSource, TaskKind};
use sbgh_core::submission::{
    ProducerKey, ResolvedTaskSource, SchedulingConstraints, SubmissionActor, SubmissionDisposition,
    SubmissionProvenance,
};
use sbgh_intent::ValidationSelection;
use serde::Deserialize;

use crate::api::conv::enum_str;
use crate::api::error::ApiErr;
use crate::api::state::ApiState;
use crate::block_validation_submission::BlockValidationSubmission;

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
                requested: detail
                    .requested
                    .map(|selection| match selection {
                        sbgh_core::reporting::BlockValidationSelectionReport::Recent {
                            block_count,
                        } => BlockValidationSelectionDetail::Recent { block_count },
                        sbgh_core::reporting::BlockValidationSelectionReport::Full => {
                            BlockValidationSelectionDetail::Full
                        }
                        sbgh_core::reporting::BlockValidationSelectionReport::Range { range } => {
                            BlockValidationSelectionDetail::Range {
                                range: ReportRange {
                                    start: range.start,
                                    end: range.end,
                                },
                            }
                        }
                    }),
                observed: detail
                    .observed
                    .map(|observed| ObservedValidationIndexDetail {
                        pre_nakamoto_count: observed.pre_nakamoto_count,
                        nakamoto_count: observed.nakamoto_count,
                    }),
                resolved_range: detail
                    .resolved_range
                    .map(|range| ReportRange {
                        start: range.start,
                        end: range.end,
                    }),
                segments: detail
                    .segments
                    .into_iter()
                    .map(|segment| ValidationEpochSegmentDetail {
                        epoch: enum_str(&segment.epoch),
                        global_range: ReportRange {
                            start: segment.global_range.start,
                            end: segment.global_range.end,
                        },
                        local_range: ReportRange {
                            start: segment.local_range.start,
                            end: segment.local_range.end,
                        },
                    })
                    .collect(),
                shard_count: detail.shard_count,
                max_concurrency: detail.max_concurrency,
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
    let service = state
        .block_validation
        .as_ref()
        .ok_or_else(|| ApiErr::bad_request("block validation is not configured"))?;
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
    let requested_selection = match request.selection {
        BlockValidationSelectionRequest::Recent { block_count } => {
            ValidationSelection::Recent { block_count }
        }
        BlockValidationSelectionRequest::Full => ValidationSelection::Full,
        BlockValidationSelectionRequest::Range { start, end } => {
            ValidationSelection::Range { start, end }
        }
    };
    let selection = service
        .resolve_user_selection(&requested_selection)
        .map_err(ApiErr::from)?;
    let detail = service
        .queued_detail(&selection)
        .map_err(ApiErr::from)?;
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
    let receipt = service
        .submit(BlockValidationSubmission {
            source,
            selection,
            constraints: SchedulingConstraints {
                required_worker_id,
                required_measurement_profile: None,
            },
            actor: SubmissionActor::System,
            producer_key: ProducerKey {
                namespace: "admin_block_validation".into(),
                key: request.idempotency_key,
            },
            provenance: SubmissionProvenance {
                queued_event_detail: detail,
                github: None,
                slack: None,
            },
        })
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
