use sbgh_core::models::{GithubInstallation, Job};
use sbgh_core::models::{JobSource, TaskKind};
use sbgh_core::reporting::{
    BenchmarkReportView, BlockValidationReportView, BuildOnlyReportView, ReportArtifact,
    ReportForensics, ReportIdentity, ReportLifecycle, ReportLifecycleState, SubmissionReportView,
    TaskReport,
};
use sbgh_fleet::{ArtifactDescriptor, TaskPayload, Validate};
use sqlx::Row as _;
use uuid::Uuid;

use crate::mapping::{Db, IntoDomain};
use crate::{Pool, Result};

pub async fn list_installations(pool: &Pool) -> Result<Vec<GithubInstallation>> {
    let rows = sqlx::query_as::<_, Db<GithubInstallation>>(
        "SELECT id, github_account_id, account_login, account_type, suspended_at, deleted_at, \
         created_at, updated_at FROM github_installation ORDER BY created_at DESC",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    Ok(rows.into_domain())
}

pub async fn list_jobs(pool: &Pool, status: Option<&str>, limit: i64) -> Result<Vec<Job>> {
    let rows = sqlx::query_as::<_, Db<Job>>(
        r#"
        SELECT * FROM job
         WHERE ($1::text IS NULL OR status = $1::job_status)
         ORDER BY created_at DESC
         LIMIT $2
        "#,
    )
    .bind(status)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    Ok(rows.into_domain())
}

/// Build the provider-neutral submission report snapshot from durable state.
pub async fn submission_report(
    pool: &Pool,
    submission_id: Uuid,
) -> Result<Option<SubmissionReportView>> {
    let header = sqlx::query(
        r#"
        SELECT submission.source::text AS source,
               repo.owner || '/' || repo.name AS repository,
               COUNT(DISTINCT spec.task_kind)::bigint AS task_kind_count,
               MIN(spec.task_kind::text) AS task_kind
          FROM task_submission submission
          JOIN github_repo repo ON repo.id = submission.github_repo_id
          JOIN task_spec spec ON spec.task_submission_id = submission.id
         WHERE submission.id = $1
      GROUP BY submission.source, repo.owner, repo.name
        "#,
    )
    .bind(submission_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    let Some(header) = header else {
        return Ok(None);
    };
    let task_kind_count: i64 = header
        .try_get("task_kind_count")
        .map_err(other)?;
    if task_kind_count != 1 {
        return Err(sbgh_core::Error::Other(anyhow::anyhow!(
            "submission {submission_id} contains {task_kind_count} task kinds"
        )));
    }
    let source = parse_source(
        header
            .try_get("source")
            .map_err(other)?,
    )?;
    let task_kind = parse_task_kind(
        header
            .try_get("task_kind")
            .map_err(other)?,
    )?;
    let repository: String = header
        .try_get("repository")
        .map_err(other)?;

    let jobs = sqlx::query(
        r#"
        SELECT job.id, job.status::text AS status, job.git_commit_hash,
               job.execution_payload
          FROM job
          JOIN task_spec spec ON spec.id = job.task_spec_id
         WHERE job.task_submission_id = $1
      ORDER BY spec.spec_index, job.task_run_index, job.created_at, job.id
        "#,
    )
    .bind(submission_id)
    .fetch_all(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    if jobs.is_empty() {
        return Err(sbgh_core::Error::Other(anyhow::anyhow!(
            "submission {submission_id} has no jobs"
        )));
    }
    let current = jobs
        .last()
        .expect("non-empty checked above");
    let current_job_id: Uuid = current
        .try_get("id")
        .map_err(other)?;
    let commit = current
        .try_get::<Option<String>, _>("git_commit_hash")
        .map_err(other)?
        .unwrap_or_default();
    let statuses = jobs
        .iter()
        .map(|row| {
            row.try_get::<String, _>("status")
                .map_err(other)
        })
        .collect::<Result<Vec<_>>>()?;
    let completed_jobs = statuses
        .iter()
        .filter(|status| status.as_str() == "completed")
        .count() as u32;
    let requested_runs: i64 = sqlx::query_scalar(
        r#"
        SELECT COALESCE(SUM(
            CASE WHEN task_kind = 'build_only' THEN 0
                 ELSE GREATEST(requested_run_count, 1)
            END
        ), 0)::bigint
          FROM task_spec
         WHERE task_submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_one(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    let total_jobs = (requested_runs as u32).max(jobs.len() as u32);
    let state = aggregate_state(&statuses, completed_jobs, total_jobs);

    let current_attempt_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        SELECT attempt_id
          FROM worker_attempt
         WHERE job_id = $1
      ORDER BY created_at DESC, attempt_id DESC
         LIMIT 1
        "#,
    )
    .bind(current_job_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    let phase: Option<String> = sqlx::query_scalar(
        r#"
        SELECT event.payload->>'label'
          FROM worker_attempt attempt
          JOIN worker_attempt_event event ON event.attempt_id = attempt.attempt_id
         WHERE attempt.job_id = $1
           AND event.payload_kind = 'phase'
           AND event.projected_at IS NOT NULL
      ORDER BY attempt.created_at DESC, event.reliable_seq DESC
         LIMIT 1
        "#,
    )
    .bind(current_job_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?
    .flatten();
    let failure: Option<String> = sqlx::query_scalar(
        r#"
        SELECT event.remark
          FROM job
          JOIN job_event event ON event.job_id = job.id
         WHERE job.task_submission_id = $1
           AND event.event_kind IN ('failed', 'cancelled')
           AND event.remark IS NOT NULL
      ORDER BY event.occurred_at DESC, event.id DESC
         LIMIT 1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?
    .flatten();
    let terminal_artifacts = terminal_report_artifacts(pool, current_job_id).await?;
    let forensics = terminal_report_forensics(pool, current_job_id).await?;

    let (task, task_artifacts) = match task_kind {
        TaskKind::Benchmark => (
            TaskReport::Benchmark(BenchmarkReportView {
                requested_runs: requested_runs.max(0) as u32,
                completed_runs: completed_jobs,
            }),
            Vec::new(),
        ),
        TaskKind::BuildOnly => {
            (TaskReport::BuildOnly(BuildOnlyReportView { cache_outcome: None }), Vec::new())
        }
        TaskKind::BlockValidation => {
            let requested_payload = current
                .try_get::<Option<serde_json::Value>, _>("execution_payload")
                .map_err(other)?
                .map(parse_validation_request)
                .transpose()?;
            let result = sqlx::query(
                r#"
                SELECT valid, checked_blocks, chainstate_origin,
                       pre_nakamoto_count, nakamoto_count,
                       resolved_start, resolved_end, epoch_segments,
                       shard_count, max_concurrency, invalid_blocks, artifact_manifest
                  FROM block_validation_result
                 WHERE job_id = $1
                "#,
            )
            .bind(current_job_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
            match result {
                Some(row) => {
                    let invalid: Vec<sbgh_fleet::InvalidBlock> = serde_json::from_value(
                        row.try_get("invalid_blocks")
                            .map_err(other)?,
                    )
                    .map_err(other)?;
                    let manifest: Vec<ArtifactDescriptor> = serde_json::from_value(
                        row.try_get("artifact_manifest")
                            .map_err(other)?,
                    )
                    .map_err(other)?;
                    let valid: bool = row
                        .try_get("valid")
                        .map_err(other)?;
                    let checked_blocks = nonnegative_u64(
                        row.try_get("checked_blocks")
                            .map_err(other)?,
                        "checked_blocks",
                    )?;
                    let chainstate_origin: String = row
                        .try_get("chainstate_origin")
                        .map_err(other)?;
                    let segments = serde_json::from_value(
                        row.try_get::<serde_json::Value, _>("epoch_segments")
                            .map_err(other)?,
                    )
                    .map_err(other)?;
                    let result = sbgh_fleet::BlockValidationResult {
                        valid,
                        checked_blocks,
                        invalid_blocks: invalid,
                        chainstate_origin,
                        observed: sbgh_fleet::ObservedValidationIndex {
                            pre_nakamoto_count: nonnegative_u64(
                                row.try_get("pre_nakamoto_count")
                                    .map_err(other)?,
                                "pre_nakamoto_count",
                            )?,
                            nakamoto_count: nonnegative_u64(
                                row.try_get("nakamoto_count")
                                    .map_err(other)?,
                                "nakamoto_count",
                            )?,
                        },
                        resolved_range: sbgh_fleet::InclusiveRange {
                            start: nonnegative_u64(
                                row.try_get("resolved_start")
                                    .map_err(other)?,
                                "resolved_start",
                            )?,
                            end: nonnegative_u64(
                                row.try_get("resolved_end")
                                    .map_err(other)?,
                                "resolved_end",
                            )?,
                        },
                        segments,
                        shard_count: u32::try_from(
                            row.try_get::<i32, _>("shard_count")
                                .map_err(other)?,
                        )
                        .map_err(other)?,
                        max_concurrency: u32::try_from(
                            row.try_get::<i32, _>("max_concurrency")
                                .map_err(other)?,
                        )
                        .map_err(other)?,
                    };
                    let detail =
                        BlockValidationReportView::from_result(requested_payload.as_ref(), &result);
                    (
                        TaskReport::BlockValidation(detail),
                        manifest
                            .into_iter()
                            .map(|artifact| ReportArtifact {
                                name: artifact.logical_key,
                                key: artifact.key,
                            })
                            .collect(),
                    )
                }
                None => (
                    TaskReport::BlockValidation(BlockValidationReportView {
                        ..BlockValidationReportView::from_request(requested_payload.as_ref())
                    }),
                    Vec::new(),
                ),
            }
        }
    };
    let artifacts = if terminal_artifacts.is_empty() { task_artifacts } else { terminal_artifacts };

    Ok(Some(SubmissionReportView {
        identity: ReportIdentity {
            submission_id,
            current_job_id: Some(current_job_id),
            current_attempt_id,
            task_kind,
            source,
            repository,
            commit,
        },
        lifecycle: ReportLifecycle {
            state,
            phase,
            completed_jobs,
            total_jobs,
            failure,
        },
        task,
        artifacts,
        forensics,
    }))
}

async fn terminal_report_forensics(pool: &Pool, job_id: Uuid) -> Result<Option<ReportForensics>> {
    let summary: Option<serde_json::Value> = sqlx::query_scalar(
        r#"
        SELECT CASE
                 WHEN jsonb_typeof(detail->'summary') = 'object'
                   THEN detail->'summary'
                 ELSE detail
               END
          FROM job_event
         WHERE job_id = $1
           AND event_kind IN ('completed', 'failed')
           AND detail IS NOT NULL
      ORDER BY occurred_at DESC, id DESC
         LIMIT 1
        "#,
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?
    .flatten();
    let Some(summary) = summary else {
        return Ok(None);
    };
    let forensics = ReportForensics {
        last_phase: summary
            .get("last_phase")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        console_tail: summary
            .get("console_tail")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        console_size_bytes: summary
            .get("console_size_bytes")
            .and_then(serde_json::Value::as_u64),
        console_artifact_key: summary
            .get("console_archived_path")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
    };
    if forensics.last_phase.is_none()
        && forensics
            .console_tail
            .is_none()
        && forensics
            .console_size_bytes
            .is_none()
        && forensics
            .console_artifact_key
            .is_none()
    {
        Ok(None)
    } else {
        Ok(Some(forensics))
    }
}

/// Artifacts belong to the task-neutral attempt lifecycle. Read them from the
/// latest terminal event so failed benchmark, build-only, and block-validation
/// attempts expose the same operator forensics. The block-validation result's
/// manifest remains a compatibility fallback for rows written before terminal
/// event projection carried artifact descriptors.
async fn terminal_report_artifacts(pool: &Pool, job_id: Uuid) -> Result<Vec<ReportArtifact>> {
    let manifest: Option<serde_json::Value> = sqlx::query_scalar(
        r#"
        SELECT detail->'artifacts'
          FROM job_event
         WHERE job_id = $1
           AND event_kind IN ('completed', 'failed')
           AND jsonb_typeof(detail->'artifacts') = 'array'
      ORDER BY occurred_at DESC, id DESC
         LIMIT 1
        "#,
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?
    .flatten();
    manifest
        .map(serde_json::from_value::<Vec<ArtifactDescriptor>>)
        .transpose()
        .map_err(other)
        .map(|artifacts| {
            artifacts
                .unwrap_or_default()
                .into_iter()
                .map(|artifact| ReportArtifact {
                    name: artifact.logical_key,
                    key: artifact.key,
                })
                .collect()
        })
}

fn parse_source(value: &str) -> Result<JobSource> {
    match value {
        "github_webhook" => Ok(JobSource::GithubWebhook),
        "github_comment" => Ok(JobSource::GithubComment),
        "slack" => Ok(JobSource::Slack),
        "cli" => Ok(JobSource::Cli),
        "scheduler" => Ok(JobSource::Scheduler),
        "daemon" => Ok(JobSource::Daemon),
        _ => Err(sbgh_core::Error::Other(anyhow::anyhow!("unknown job source {value:?}"))),
    }
}

fn parse_task_kind(value: &str) -> Result<TaskKind> {
    match value {
        "benchmark" => Ok(TaskKind::Benchmark),
        "block_validation" => Ok(TaskKind::BlockValidation),
        "build_only" => Ok(TaskKind::BuildOnly),
        _ => Err(sbgh_core::Error::Other(anyhow::anyhow!("unknown task kind {value:?}"))),
    }
}

fn aggregate_state(
    statuses: &[String],
    completed_jobs: u32,
    total_jobs: u32,
) -> ReportLifecycleState {
    if statuses
        .iter()
        .any(|status| matches!(status.as_str(), "claimed" | "running"))
    {
        ReportLifecycleState::Running
    } else if statuses
        .iter()
        .any(|status| status == "failed")
    {
        ReportLifecycleState::Failed
    } else if statuses
        .iter()
        .any(|status| status == "cancelled")
    {
        ReportLifecycleState::Cancelled
    } else if completed_jobs >= total_jobs && total_jobs > 0 {
        ReportLifecycleState::Completed
    } else {
        ReportLifecycleState::Queued
    }
}

fn parse_validation_request(
    value: serde_json::Value,
) -> Result<sbgh_fleet::BlockValidationPayload> {
    let payload: TaskPayload = serde_json::from_value(value).map_err(other)?;
    payload
        .validate()
        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    let TaskPayload::BlockValidation(payload) = payload else {
        return Err(sbgh_core::Error::Other(anyhow::anyhow!(
            "block-validation job has a non-validation execution payload"
        )));
    };
    Ok(payload)
}

fn nonnegative_u64(value: i64, field: &str) -> Result<u64> {
    value
        .try_into()
        .map_err(|_| sbgh_core::Error::Other(anyhow::anyhow!("{field} contains a negative value")))
}

fn other(error: impl std::error::Error + Send + Sync + 'static) -> sbgh_core::Error {
    sbgh_core::Error::Other(anyhow::Error::new(error))
}
