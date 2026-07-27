//! `/api/jobs` — benchmark run visibility.

use axum::Json;
use axum::extract::{Query, State};
use sbgh_api::{EnqueueBlockValidationRequest, EnqueueJobResponse, JobView};
use sbgh_core::db::fleet::PreparedExecution;
use sbgh_core::models::{
    BuildTarget, GitRefKind, Job, JobAxes, JobIntent, JobSource, NewJob, QueuedEventDetail,
    TaskKind,
};
use sbgh_proto::{BlockValidationPayload, InclusiveRange, TaskPayload, Validate, ValidationEpoch};
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
    let worker_id = request
        .worker_id
        .parse::<uuid::Uuid>()
        .map_err(|_| ApiErr::bad_request("worker_id must be a UUID"))?;
    let fleet = sbgh_postgres::PostgresFleetStore::new(state.pool);
    let dataset = fleet
        .current_dataset(worker_id, &request.dataset_network)
        .await?
        .ok_or_else(|| {
            ApiErr::bad_request(format!(
                "worker {worker_id} has no current configured {} dataset",
                request.dataset_network
            ))
        })?;
    let payload = TaskPayload::BlockValidation(BlockValidationPayload {
        dataset: dataset.clone(),
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
    let job_id = uuid::Uuid::new_v4();
    let detail = serde_json::to_value(QueuedEventDetail::BlockValidation {
        dataset_generation: dataset.generation,
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
    let new_job = NewJob {
        github_installation_id: request.install_id,
        github_repo_id: request.repo_id,
        axes: JobAxes {
            source: JobSource::Cli,
            intent: JobIntent::BlockValidation,
            task_kind: TaskKind::BlockValidation,
            build_target: BuildTarget::StacksInspect,
        },
        git_ref_kind: GitRefKind::Commit,
        git_ref_display: request.commit.clone(),
        git_commit_hash: Some(request.commit.clone()),
        git_committed_at: None,
        workload_key: None,
    };
    let payload_hash = sbgh_proto::payload_digest(&payload).map_err(|error| {
        tracing::error!(%error, "hashing block-validation payload failed");
        ApiErr::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "could not enqueue block validation",
        )
    })?;
    fleet
        .enqueue_prepared_job(
            job_id,
            &new_job,
            &detail,
            &PreparedExecution {
                job_id,
                commit: request.commit,
                payload,
                payload_hash,
                worker_id: Some(worker_id),
            },
            &sbgh_postgres::PreparedJobProvenance::default(),
        )
        .await?;
    Ok(Json(EnqueueJobResponse { job_id: job_id.to_string() }))
}
