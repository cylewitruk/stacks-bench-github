//! Read-only operator queries; presentation and authentication stay outside persistence.

use sbgh_core::inspection::{
    AttemptInspection, JobInspection, ProgressInspection, SubmissionSummary, WorkSnapshot,
    WorkTarget,
};
use sbgh_fleet::ProgressUpdate;
use sqlx::Row;

use super::other;
use crate::{Pool, Result};

/// Resolve a validated UUID prefix, optionally constrained to one identity kind.
pub async fn resolve_work(
    pool: &Pool,
    prefix: &str,
    kind: Option<&str>,
) -> Result<Vec<WorkTarget>> {
    let rows = sqlx::query(
        r#"
        SELECT id AS submission_id, NULL::uuid AS job_id FROM task_submission
         WHERE id::text LIKE $1 AND ($2::text IS NULL OR $2 = 'submission')
        UNION ALL
        SELECT task_submission_id, id FROM job
         WHERE id::text LIKE $1 AND ($2::text IS NULL OR $2 = 'job')
        ORDER BY submission_id, job_id NULLS FIRST
        LIMIT 10
        "#,
    )
    .bind(format!("{prefix}%"))
    .bind(kind)
    .fetch_all(pool)
    .await
    .map_err(other)?;
    rows.into_iter()
        .map(|row| {
            Ok(WorkTarget {
                submission_id: row
                    .try_get("submission_id")
                    .map_err(other)?,
                job_id: row
                    .try_get("job_id")
                    .map_err(other)?,
            })
        })
        .collect()
}

/// List request summaries with filtering applied before the row limit.
pub async fn list_submissions(
    pool: &Pool,
    status: Option<&str>,
    active: bool,
    limit: i64,
) -> Result<Vec<SubmissionSummary>> {
    let rows = sqlx::query(
        r#"
        WITH summaries AS (
            SELECT submission.id, repo.owner || '/' || repo.name AS repository,
                   submission.source::text AS source, submission.created_at,
                   ARRAY_AGG(job.status::text) AS statuses,
                   COUNT(*) FILTER (WHERE job.status = 'completed') AS completed_jobs,
                   GREATEST(COUNT(*), (
                       SELECT SUM(CASE WHEN task_kind = 'build_only' THEN 0
                                  ELSE GREATEST(requested_run_count, 1) END)
                         FROM task_spec WHERE task_submission_id = submission.id
                   )) AS total_jobs,
                   (ARRAY_AGG(job.git_ref_display ORDER BY spec.spec_index, job.task_run_index))[1]
                       AS reference
              FROM task_submission submission
              JOIN github_repo repo ON repo.id = submission.github_repo_id
              JOIN job ON job.task_submission_id = submission.id
              JOIN task_spec spec ON spec.id = job.task_spec_id
          GROUP BY submission.id, repo.owner, repo.name
        ), states AS (
            SELECT *, CASE
                WHEN statuses && ARRAY['claimed', 'running'] THEN 'running'
                WHEN 'failed' = ANY(statuses) THEN 'failed'
                WHEN 'cancelled' = ANY(statuses) THEN 'cancelled'
                WHEN completed_jobs >= total_jobs AND total_jobs > 0 THEN 'completed'
                ELSE 'queued' END AS state
              FROM summaries
        )
        SELECT * FROM states
         WHERE ($1::text IS NULL OR state = $1)
           AND (NOT $2 OR state IN ('queued', 'running'))
      ORDER BY created_at DESC, id DESC LIMIT $3
        "#,
    )
    .bind(status)
    .bind(active)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(other)?;
    rows.into_iter()
        .map(|row| {
            Ok(SubmissionSummary {
                id: row
                    .try_get("id")
                    .map_err(other)?,
                repository: row
                    .try_get("repository")
                    .map_err(other)?,
                source: row
                    .try_get("source")
                    .map_err(other)?,
                state: serde_json::from_value(serde_json::Value::String(
                    row.try_get("state")
                        .map_err(other)?,
                ))
                .map_err(other)?,
                created_at: row
                    .try_get("created_at")
                    .map_err(other)?,
                completed_jobs: u32::try_from(
                    row.try_get::<i64, _>("completed_jobs")
                        .map_err(other)?,
                )
                .map_err(other)?,
                total_jobs: u32::try_from(
                    row.try_get::<i64, _>("total_jobs")
                        .map_err(other)?,
                )
                .map_err(other)?,
                reference: row
                    .try_get("reference")
                    .map_err(other)?,
            })
        })
        .collect()
}

/// Observe all jobs and the latest attempt of each, retaining the selected scope.
pub async fn inspect_work(pool: &Pool, target: WorkTarget) -> Result<Option<WorkSnapshot>> {
    let Some(mut report) = super::submission_report(pool, target.submission_id).await? else {
        return Ok(None);
    };
    // Console tails are available through report/artifacts, not every watch poll.
    report.forensics = None;
    let rows = sqlx::query(
        r#"
        SELECT job.id, spec.spec_index, job.task_run_index AS run_index,
               job.status::text AS status, job.task_kind::text AS task_kind,
               job.git_ref_display AS reference, job.git_commit_hash AS commit,
               job.created_at,
               COALESCE(job.execution_payload->'effective_args',
                        queued.detail->'effective_args') AS effective_args,
               COALESCE(terminal.detail #>> '{summary,chainstate_origin}',
                        terminal.detail->>'chainstate_origin', validation.chainstate_origin)
                   AS chainstate_origin,
               terminal.remark AS failure,
               attempt.attempt_id, attempt.worker_id, registry.display_name AS worker_name,
               attempt.status::text AS attempt_status, attempt.accepted_at,
               attempt.terminal_at, session.last_heartbeat_at,
               CASE WHEN job.status = 'queued' THEN NULL ELSE phase.payload->>'label' END AS phase,
               progress.progress_seq, progress.update AS progress,
               progress.received_at AS progress_received_at,
               to_jsonb(metric) AS metrics,
               COALESCE(result.run_json, to_jsonb(validation) - 'artifact_manifest') AS result
          FROM job JOIN task_spec spec ON spec.id = job.task_spec_id
          LEFT JOIN LATERAL (
              SELECT * FROM worker_attempt WHERE job_id = job.id
               ORDER BY fencing_generation DESC, created_at DESC, attempt_id DESC LIMIT 1
          ) attempt ON TRUE
          LEFT JOIN worker_registry registry ON registry.worker_id = attempt.worker_id
          LEFT JOIN worker_session session ON session.worker_session_id = attempt.worker_session_id
          LEFT JOIN LATERAL (
              SELECT payload, received_at FROM worker_attempt_event
               WHERE attempt_id = attempt.attempt_id AND payload_kind = 'phase'
                 AND projection_discarded_reason IS NULL
               ORDER BY reliable_seq DESC LIMIT 1
          ) phase ON TRUE
          LEFT JOIN LATERAL (
              SELECT * FROM worker_attempt_progress
               WHERE attempt_id = attempt.attempt_id AND projection_discarded_reason IS NULL
                 AND attempt.status IN ('running', 'cancel_requested')
                 AND (phase.received_at IS NULL OR received_at >= phase.received_at)
               ORDER BY progress_seq DESC LIMIT 1
          ) progress ON TRUE
          LEFT JOIN LATERAL (
              SELECT detail FROM job_event WHERE job_id = job.id AND event_kind = 'queued'
               ORDER BY id DESC LIMIT 1
          ) queued ON TRUE
          LEFT JOIN LATERAL (
              SELECT detail, remark FROM job_event
               WHERE job_id = job.id AND event_kind IN ('completed', 'failed', 'cancelled')
               ORDER BY occurred_at DESC, id DESC LIMIT 1
          ) terminal ON TRUE
          LEFT JOIN job_metric metric ON metric.job_id = job.id
          LEFT JOIN job_result result ON result.job_id = job.id
          LEFT JOIN block_validation_result validation ON validation.job_id = job.id
         WHERE job.task_submission_id = $1
      ORDER BY spec.spec_index, job.task_run_index, job.created_at, job.id
        "#,
    )
    .bind(target.submission_id)
    .fetch_all(pool)
    .await
    .map_err(other)?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let attempt = match row
            .try_get("attempt_id")
            .map_err(other)?
        {
            Some(id) => Some(AttemptInspection {
                id,
                worker_id: row
                    .try_get("worker_id")
                    .map_err(other)?,
                worker_name: row
                    .try_get("worker_name")
                    .map_err(other)?,
                status: row
                    .try_get("attempt_status")
                    .map_err(other)?,
                started_at: row
                    .try_get("accepted_at")
                    .map_err(other)?,
                finished_at: row
                    .try_get("terminal_at")
                    .map_err(other)?,
                heartbeat_at: row
                    .try_get("last_heartbeat_at")
                    .map_err(other)?,
            }),
            None => None,
        };
        let progress = row
            .try_get::<Option<serde_json::Value>, _>("progress")
            .map_err(other)?
            .map(|value| -> Result<ProgressInspection> {
                let update: ProgressUpdate = serde_json::from_value(value).map_err(other)?;
                Ok(ProgressInspection {
                    sequence: row
                        .try_get("progress_seq")
                        .map_err(other)?,
                    phase: update.phase,
                    current: update.progress,
                    total: update.total,
                    message: update.message,
                    received_at: row
                        .try_get("progress_received_at")
                        .map_err(other)?,
                })
            })
            .transpose()?;
        jobs.push(JobInspection {
            id: row
                .try_get("id")
                .map_err(other)?,
            spec_index: row
                .try_get("spec_index")
                .map_err(other)?,
            run_index: row
                .try_get("run_index")
                .map_err(other)?,
            status: row
                .try_get("status")
                .map_err(other)?,
            task_kind: row
                .try_get("task_kind")
                .map_err(other)?,
            reference: row
                .try_get("reference")
                .map_err(other)?,
            commit: row
                .try_get("commit")
                .map_err(other)?,
            created_at: row
                .try_get("created_at")
                .map_err(other)?,
            effective_args: row
                .try_get::<Option<serde_json::Value>, _>("effective_args")
                .map_err(other)?
                .map(serde_json::from_value)
                .transpose()
                .map_err(other)?
                .unwrap_or_default(),
            chainstate_origin: row
                .try_get("chainstate_origin")
                .map_err(other)?,
            phase: row
                .try_get("phase")
                .map_err(other)?,
            failure: row
                .try_get("failure")
                .map_err(other)?,
            attempt,
            progress,
            metrics: row
                .try_get::<Option<serde_json::Value>, _>("metrics")
                .map_err(other)?
                .map(serde_json::from_value)
                .transpose()
                .map_err(other)?,
            result: row
                .try_get("result")
                .map_err(other)?,
        });
    }
    if target
        .job_id
        .is_some_and(|id| {
            !jobs
                .iter()
                .any(|job| job.id == id)
        })
    {
        return Ok(None);
    }
    // Derive state from the same observation as progress and results, rather
    // than the earlier report read while a terminal projection may be advancing.
    let statuses = jobs
        .iter()
        .map(|job| job.status.clone())
        .collect::<Vec<_>>();
    report
        .lifecycle
        .completed_jobs = jobs
        .iter()
        .filter(|job| job.status == "completed")
        .count() as u32;
    report.lifecycle.state = super::aggregate_state(
        &statuses,
        report
            .lifecycle
            .completed_jobs,
        report.lifecycle.total_jobs,
    );
    Ok(Some(WorkSnapshot { target, report, jobs }))
}
