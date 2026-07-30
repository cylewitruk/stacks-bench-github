//! Atomic persistence adapter for the task-submission kernel.

use sbgh_core::db::SubmissionStore;
use sbgh_core::models::{JobEventKind, JobEventStatus, SubmissionStepKind};
use sbgh_core::submission::{
    PreparedSubmission, SubmissionActor, SubmissionDisposition, SubmissionError, SubmissionReceipt,
    TaskPlan,
};
use sbgh_fleet::TaskPayload;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::jobs::PostgresJobStore;
use crate::Db;

type SubmitResult<T> = Result<T, SubmissionError>;
type IdempotencyRow = (Uuid, Option<i32>, Option<String>, Vec<Uuid>);

fn persistence(error: impl std::error::Error + Send + Sync + 'static) -> SubmissionError {
    SubmissionError::Persistence(anyhow::Error::new(error))
}

async fn existing_receipt(
    store: &PostgresJobStore,
    prepared: &PreparedSubmission,
) -> SubmitResult<Option<SubmissionReceipt>> {
    let row: Option<IdempotencyRow> = sqlx::query_as(
        r#"
        SELECT task_submission_id, contract_version, request_digest, initial_job_ids
          FROM task_submission_idempotency
         WHERE producer_namespace = $1 AND producer_key = $2
        "#,
    )
    .bind(
        &prepared
            .command
            .producer_key
            .namespace,
    )
    .bind(
        &prepared
            .command
            .producer_key
            .key,
    )
    .fetch_optional(&store.pool)
    .await
    .map_err(persistence)?;
    let Some((submission_id, version, digest, initial_job_ids)) = row else {
        return Ok(None);
    };
    if let (Some(version), Some(digest)) = (version, digest)
        && (version != prepared.contract_version || digest != prepared.request_digest)
    {
        return Err(SubmissionError::Conflict {
            namespace: prepared
                .command
                .producer_key
                .namespace
                .clone(),
            key: prepared
                .command
                .producer_key
                .key
                .clone(),
        });
    }
    Ok(Some(SubmissionReceipt {
        submission_id,
        disposition: SubmissionDisposition::AlreadySubmitted,
        initial_job_ids,
    }))
}

async fn insert_step(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
    step_index: i32,
    step_kind: SubmissionStepKind,
    spec_id: Uuid,
) -> SubmitResult<()> {
    sqlx::query(
        r#"
        INSERT INTO task_workflow_step
            (task_submission_id, step_index, step_kind, task_spec_id)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(submission_id)
    .bind(step_index)
    .bind(Db(step_kind))
    .bind(spec_id)
    .execute(&mut **tx)
    .await
    .map_err(persistence)?;
    Ok(())
}

#[async_trait::async_trait]
impl SubmissionStore for PostgresJobStore {
    async fn persist_submission(
        &self,
        prepared: &PreparedSubmission,
    ) -> SubmitResult<SubmissionReceipt> {
        if let Some(receipt) = existing_receipt(self, prepared).await? {
            return Ok(receipt);
        }

        let submission_id = Uuid::new_v4();
        let initial_job_id = Uuid::new_v4();
        let source = prepared
            .command
            .task
            .first_source()
            .ok_or_else(|| {
                SubmissionError::Invalid("a task plan must contain at least one source".into())
            })?;
        let capability = prepared
            .command
            .task
            .required_capability();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(persistence)?;

        sqlx::query(
            r#"
            INSERT INTO task_submission
                (id, github_installation_id, github_repo_id, source, intent,
                 artifact_prefix, contract_version, request_digest,
                 required_worker_id, required_measurement_profile)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(submission_id)
        .bind(source.github_installation_id)
        .bind(source.github_repo_id)
        .bind(Db(source.source))
        .bind(Db(source.intent))
        .bind(submission_id.to_string())
        .bind(prepared.contract_version)
        .bind(&prepared.request_digest)
        .bind(
            prepared
                .command
                .constraints
                .required_worker_id,
        )
        .bind(
            &prepared
                .command
                .constraints
                .required_measurement_profile,
        )
        .execute(&mut *tx)
        .await
        .map_err(persistence)?;

        let variants = match &prepared.command.task {
            TaskPlan::Benchmark(plan) => plan
                .variants
                .iter()
                .map(|variant| {
                    (
                        &variant.source,
                        variant.requested_run_count,
                        variant.baseline_calibration_id,
                        true,
                    )
                })
                .collect::<Vec<_>>(),
            TaskPlan::BuildOnly(plan) => vec![(&plan.source, 1, None, false)],
            TaskPlan::BlockValidation(plan) => vec![(&plan.source, 1, None, true)],
        };

        let mut spec_ids = Vec::with_capacity(variants.len());
        let mut next_step = 0_i32;
        let measured_runs = variants
            .iter()
            .map(|(source, requested, _, _)| {
                sbgh_core::models::measured_run_count(source.task_kind, *requested)
            })
            .sum::<i32>();
        for (index, (variant, requested_runs, baseline_calibration_id, has_run)) in
            variants.iter().enumerate()
        {
            let spec_id = Uuid::new_v4();
            spec_ids.push(spec_id);
            sqlx::query(
                r#"
                INSERT INTO task_spec
                    (id, task_submission_id, spec_index, requested_run_count,
                     baseline_calibration_id, github_repo_id, task_kind, build_target,
                     git_ref_kind, git_ref_display, git_commit_hash, git_committed_at,
                     workload_key)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                "#,
            )
            .bind(spec_id)
            .bind(submission_id)
            .bind(index as i32)
            .bind(requested_runs)
            .bind(baseline_calibration_id)
            .bind(variant.github_repo_id)
            .bind(Db(variant.task_kind))
            .bind(Db(variant.build_target))
            .bind(Db(variant.git_ref_kind))
            .bind(&variant.git_ref_display)
            .bind(&variant.commit)
            .bind(variant.committed_at)
            .bind(&variant.workload_key)
            .execute(&mut *tx)
            .await
            .map_err(persistence)?;

            insert_step(&mut tx, submission_id, next_step, SubmissionStepKind::Build, spec_id)
                .await?;
            next_step += 1;
            if sbgh_core::models::uses_shared_calibration(
                variant.task_kind,
                variant.build_target,
                measured_runs,
            ) {
                insert_step(
                    &mut tx,
                    submission_id,
                    next_step,
                    SubmissionStepKind::Calibrate,
                    spec_id,
                )
                .await?;
                next_step += 1;
            }
            if *has_run {
                insert_step(&mut tx, submission_id, next_step, SubmissionStepKind::Run, spec_id)
                    .await?;
                next_step += 1;
            }
        }

        let (execution_version, execution_payload, execution_hash) = match &prepared.command.task {
            TaskPlan::BlockValidation(plan) => {
                let payload = TaskPayload::BlockValidation(plan.payload.clone());
                let hash = sbgh_fleet::payload_digest(&payload).map_err(persistence)?;
                (
                    Some(i32::from(sbgh_fleet::PROTOCOL_VERSION)),
                    Some(serde_json::to_value(payload).map_err(persistence)?),
                    Some(hash),
                )
            }
            TaskPlan::Benchmark(_) | TaskPlan::BuildOnly(_) => (None, None, None),
        };
        let first_spec_id = spec_ids[0];
        sqlx::query(
            r#"
            INSERT INTO job
                (id, task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind,
                 git_ref_display, git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target, required_capability,
                 execution_payload_version, execution_payload, execution_payload_hash)
            VALUES (
                $1, $2, $3, 0, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18
            )
            "#,
        )
        .bind(initial_job_id)
        .bind(submission_id)
        .bind(first_spec_id)
        .bind(source.github_installation_id)
        .bind(source.github_repo_id)
        .bind(Db(source.git_ref_kind))
        .bind(&source.git_ref_display)
        .bind(&source.commit)
        .bind(source.committed_at)
        .bind(&source.workload_key)
        .bind(Db(source.source))
        .bind(Db(source.intent))
        .bind(Db(source.task_kind))
        .bind(Db(source.build_target))
        .bind(Db(capability))
        .bind(execution_version)
        .bind(execution_payload)
        .bind(execution_hash)
        .execute(&mut *tx)
        .await
        .map_err(persistence)?;

        let queued_detail = &prepared
            .command
            .provenance
            .queued_event_detail;
        sqlx::query(
            r#"
            INSERT INTO job_event (job_id, event_kind, event_status, detail)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(initial_job_id)
        .bind(Db(JobEventKind::Queued))
        .bind(Db(JobEventStatus::Success))
        .bind(queued_detail)
        .execute(&mut *tx)
        .await
        .map_err(persistence)?;

        let (actor_kind, actor_identity) = match &prepared.command.actor {
            SubmissionActor::System => ("system", None),
            SubmissionActor::GithubUser { user_id } => ("github_user", Some(user_id.to_string())),
            SubmissionActor::SlackUser { team_id, user_id } => {
                ("slack_user", Some(format!("{team_id}:{user_id}")))
            }
        };
        sqlx::query(
            r#"
            INSERT INTO task_submission_provenance
                (task_submission_id, actor_kind, actor_identity, queued_event_detail)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(submission_id)
        .bind(actor_kind)
        .bind(actor_identity)
        .bind(queued_detail)
        .execute(&mut *tx)
        .await
        .map_err(persistence)?;

        if let Some(github) = &prepared
            .command
            .provenance
            .github
        {
            sqlx::query(
                r#"
                INSERT INTO task_submission_github
                    (task_submission_id, github_webhook_id, triggering_user_id,
                     github_pull_request_id, triggering_comment_id)
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(submission_id)
            .bind(github.webhook_id)
            .bind(github.triggering_user_id)
            .bind(github.pull_request_id)
            .bind(github.triggering_comment_id)
            .execute(&mut *tx)
            .await
            .map_err(persistence)?;
        }
        if let Some(slack) = &prepared
            .command
            .provenance
            .slack
        {
            sqlx::query(
                r#"
                INSERT INTO task_submission_slack
                    (task_submission_id, team_id, channel_id, request_message_ts,
                     reporting_identity, report_message_ts)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(submission_id)
            .bind(&slack.team_id)
            .bind(&slack.channel_id)
            .bind(&slack.request_message_ts)
            .bind(&slack.reporting_identity)
            .bind(&slack.report_message_ts)
            .execute(&mut *tx)
            .await
            .map_err(persistence)?;
            if let Some(message_ts) = &slack.report_message_ts {
                sqlx::query(
                    r#"
                    INSERT INTO job_event (job_id, event_kind, event_status, detail)
                    VALUES ($1, $2, $3, $4)
                    "#,
                )
                .bind(initial_job_id)
                .bind(Db(JobEventKind::PlanMessageSent))
                .bind(Db(JobEventStatus::Success))
                .bind(serde_json::json!({ "plan_message_ts": message_ts }))
                .execute(&mut *tx)
                .await
                .map_err(persistence)?;
            }
        }

        let elected = sqlx::query(
            r#"
            INSERT INTO task_submission_idempotency
                (producer_namespace, producer_key, task_submission_id,
                 contract_version, request_digest, initial_job_ids)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (producer_namespace, producer_key) DO NOTHING
            "#,
        )
        .bind(
            &prepared
                .command
                .producer_key
                .namespace,
        )
        .bind(
            &prepared
                .command
                .producer_key
                .key,
        )
        .bind(submission_id)
        .bind(prepared.contract_version)
        .bind(&prepared.request_digest)
        .bind(vec![initial_job_id])
        .execute(&mut *tx)
        .await
        .map_err(persistence)?;
        if elected.rows_affected() == 0 {
            tx.rollback()
                .await
                .map_err(persistence)?;
            return existing_receipt(self, prepared)
                .await?
                .ok_or_else(|| {
                    SubmissionError::Persistence(anyhow::anyhow!("idempotency winner disappeared"))
                });
        }

        tx.commit()
            .await
            .map_err(persistence)?;
        Ok(SubmissionReceipt {
            submission_id,
            disposition: SubmissionDisposition::Created,
            initial_job_ids: vec![initial_job_id],
        })
    }
}
