//! PostgreSQL implementation of the worker-fleet state machine.

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sbgh_core::db::fleet::{
    ArtifactGrantRecord, AttemptHeartbeat, AuthorizedSession, CleanupObligation, EventIngest,
    FleetCompletion, FleetFailure, FleetSnapshot, FleetStore, FleetTerminalSubmission,
    FleetTerminalWrite, OfferedAssignment, PendingArtifactPromotion, PreparedExecution,
    ProjectedReportMutation, ReportProjectionSeed, ResolvedSpecSource, StoredAttemptEvent,
    StoredProgress, SubmissionRecovery, TerminalAcceptance, WorkerAuthorization,
    WorkerIdentityRecord, WorkerPolicyPatch, WorkerRegistration, WorkerRegistryEntry,
    WorkerRegistryMutation, WorkerRegistryStore,
};
use sbgh_core::models::{JobEventKind, JobEventStatus};
#[cfg(feature = "testing")]
use sbgh_core::models::{NewJob, NewPullRequestLink};
use sbgh_fleet::{
    ArtifactDescriptor, AttemptIdentity, BlockValidationResult, DesiredState, ProgressRequest,
    RegisterSessionRequest, ReliableEventEnvelope, ReliableEventPayload, TaskPayload,
    TerminalOutcome, WorkOffer, WorkerCapability,
};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use crate::{Db, IntoCoreResult, Pool};

#[derive(Clone)]
pub struct PostgresFleetStore {
    pool: Pool,
}

#[derive(Debug, Clone, Default)]
#[cfg(feature = "testing")]
pub struct PreparedJobProvenance {
    pub github_webhook_id: Option<i64>,
    pub triggering_user_id: Option<i64>,
    pub pull_request_link: Option<NewPullRequestLink>,
}

impl PostgresFleetStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    #[cfg(feature = "testing")]
    pub async fn seed_worker(&self, registration: &WorkerRegistration) -> sbgh_core::Result<()> {
        let capabilities = capability_names(&registration.allowed_capabilities);
        sqlx::query(
            r#"
            INSERT INTO worker_registry
                (worker_id, display_name, allowed_capabilities,
                 measurement_profile, enabled, draining)
            VALUES (
                $1, $2,
                ARRAY(SELECT value::worker_capability FROM unnest($3::text[]) value),
                $4, $5, $6
            )
            ON CONFLICT (worker_id) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    allowed_capabilities = EXCLUDED.allowed_capabilities,
                    measurement_profile = EXCLUDED.measurement_profile,
                    enabled = EXCLUDED.enabled,
                    draining = EXCLUDED.draining,
                    updated_at = NOW()
            "#,
        )
        .bind(registration.worker_id)
        .bind(&registration.display_name)
        .bind(capabilities)
        .bind(&registration.measurement_profile)
        .bind(registration.enabled)
        .bind(registration.draining)
        .execute(&self.pool)
        .await
        .core()?;
        sqlx::query(
            "INSERT INTO worker_identity_key (identity_key_sha256, worker_id)
             VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
        )
        .bind(test_identity(registration.worker_id).as_slice())
        .bind(registration.worker_id)
        .execute(&self.pool)
        .await
        .core()?;
        Ok(())
    }

    #[cfg(feature = "testing")]
    pub async fn register_session(
        &self,
        worker_id: Uuid,
        request: &RegisterSessionRequest,
        session_ttl: Duration,
    ) -> sbgh_core::Result<AuthorizedSession> {
        <Self as FleetStore>::register_session(
            self,
            worker_id,
            test_identity(worker_id),
            request,
            session_ttl,
        )
        .await
    }

    /// Atomically create and prepare a singleton fleet task.
    ///
    /// A worker can never observe the job between queue insertion and
    /// immutable-payload persistence.
    #[cfg(feature = "testing")]
    pub async fn enqueue_prepared_job(
        &self,
        job_id: Uuid,
        new: &NewJob,
        queued_detail: &serde_json::Value,
        prepared: &PreparedExecution,
        provenance: &PreparedJobProvenance,
    ) -> sbgh_core::Result<()> {
        if prepared.job_id != job_id {
            return Err(sbgh_core::Error::Config(
                "prepared execution does not belong to the new job".into(),
            ));
        }
        let payload = serde_json::to_value(&prepared.payload)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let submission_id = Uuid::new_v4();
        let spec_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO task_submission
                (id, github_installation_id, github_repo_id, source, intent,
                 artifact_prefix)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(submission_id)
        .bind(new.github_installation_id)
        .bind(new.github_repo_id)
        .bind(Db(new.axes.source))
        .bind(Db(new.axes.intent))
        .bind(submission_id.to_string())
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            INSERT INTO task_spec
                (id, task_submission_id, spec_index, requested_run_count,
                 github_repo_id, task_kind, build_target, git_ref_kind,
                 git_ref_display, git_commit_hash, git_committed_at, workload_key)
            VALUES ($1, $2, 0, 1, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(spec_id)
        .bind(submission_id)
        .bind(new.github_repo_id)
        .bind(Db(new.axes.task_kind))
        .bind(Db(new.axes.build_target))
        .bind(Db(new.git_ref_kind))
        .bind(&new.git_ref_display)
        .bind(&new.git_commit_hash)
        .bind(new.git_committed_at)
        .bind(&new.workload_key)
        .execute(&mut *tx)
        .await
        .core()?;
        for (step_kind, step_index) in [("build", 0_i32), ("run", 1_i32)] {
            sqlx::query(
                r#"
                INSERT INTO task_workflow_step
                    (task_submission_id, step_index, step_kind, task_spec_id)
                VALUES ($1, $2, $3::text::task_step_kind, $4)
                "#,
            )
            .bind(submission_id)
            .bind(step_index)
            .bind(step_kind)
            .bind(spec_id)
            .execute(&mut *tx)
            .await
            .core()?;
        }
        sqlx::query(
            r#"
            INSERT INTO job
                (id, task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind,
                 git_ref_display, git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target,
                 required_capability, execution_payload_version,
                 execution_payload, execution_payload_hash)
            VALUES (
                $1, $2, $3, 0, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18
            )
            "#,
        )
        .bind(job_id)
        .bind(submission_id)
        .bind(spec_id)
        .bind(new.github_installation_id)
        .bind(new.github_repo_id)
        .bind(Db(new.git_ref_kind))
        .bind(&new.git_ref_display)
        .bind(&prepared.commit)
        .bind(new.git_committed_at)
        .bind(&new.workload_key)
        .bind(Db(new.axes.source))
        .bind(Db(new.axes.intent))
        .bind(Db(new.axes.task_kind))
        .bind(Db(new.axes.build_target))
        .bind(Db(new
            .axes
            .task_kind
            .required_capability()))
        .bind(i32::from(sbgh_fleet::PROTOCOL_VERSION))
        .bind(payload)
        .bind(&prepared.payload_hash)
        .execute(&mut *tx)
        .await
        .core()?;
        if let Some(webhook_id) = provenance.github_webhook_id {
            let linked = sqlx::query(
                r#"
                INSERT INTO github_webhook_job (github_webhook_id, job_id)
                VALUES ($1, $2)
                ON CONFLICT (github_webhook_id) DO NOTHING
                RETURNING github_webhook_id
                "#,
            )
            .bind(webhook_id)
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await
            .core()?;
            if linked.is_none() {
                tx.rollback().await.core()?;
                return Ok(());
            }
        }
        if let Some(user_id) = provenance.triggering_user_id {
            sqlx::query("INSERT INTO github_user_job (github_user_id, job_id) VALUES ($1, $2)")
                .bind(user_id)
                .bind(job_id)
                .execute(&mut *tx)
                .await
                .core()?;
        }
        if let Some(link) = &provenance.pull_request_link {
            sqlx::query(
                r#"
                INSERT INTO github_pull_request_job
                    (job_id, github_pull_request_id, triggering_comment_id)
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(job_id)
            .bind(link.github_pull_request_id)
            .bind(link.triggering_comment_id)
            .execute(&mut *tx)
            .await
            .core()?;
        }
        insert_job_event(
            &mut tx,
            job_id,
            JobEventKind::Queued,
            JobEventStatus::Success,
            None,
            Some(queued_detail),
        )
        .await?;
        tx.commit().await.core()?;
        Ok(())
    }
}

#[cfg(feature = "testing")]
fn test_identity(worker_id: Uuid) -> [u8; 32] {
    let mut digest = [0_u8; 32];
    digest[..16].copy_from_slice(worker_id.as_bytes());
    digest[16..].copy_from_slice(worker_id.as_bytes());
    digest
}

fn capability_name(capability: WorkerCapability) -> &'static str {
    match capability {
        WorkerCapability::Benchmark => "benchmark",
        WorkerCapability::BuildOnly => "build_only",
        WorkerCapability::BlockValidation => "block_validation",
    }
}

fn capability(value: &str) -> sbgh_core::Result<WorkerCapability> {
    match value {
        "benchmark" => Ok(WorkerCapability::Benchmark),
        "build_only" => Ok(WorkerCapability::BuildOnly),
        "block_validation" => Ok(WorkerCapability::BlockValidation),
        other => Err(sbgh_core::Error::Config(format!(
            "unknown worker capability from database: {other}"
        ))),
    }
}

fn capability_names<'a>(values: impl IntoIterator<Item = &'a WorkerCapability>) -> Vec<String> {
    let mut names = values
        .into_iter()
        .map(|value| capability_name(*value).to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn millis(timestamp: DateTime<Utc>) -> i64 {
    timestamp.timestamp_millis()
}

fn duration_seconds(duration: Duration) -> f64 {
    duration
        .num_milliseconds()
        .max(1) as f64
        / 1_000.0
}

fn parse_payload(value: serde_json::Value) -> sbgh_core::Result<TaskPayload> {
    serde_json::from_value(value)
        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
}

fn offer_from_row(row: &sqlx::postgres::PgRow) -> sbgh_core::Result<OfferedAssignment> {
    let worker_session_id: Uuid = row
        .try_get("worker_session_id")
        .core()?;
    let attempt_id: Uuid = row
        .try_get("attempt_id")
        .core()?;
    let fencing_generation: i64 = row
        .try_get("fencing_generation")
        .core()?;
    let lease_token: String = row
        .try_get("lease_token")
        .core()?;
    let capability_text: String = row
        .try_get("capability")
        .core()?;
    let offer_expires_at: DateTime<Utc> = row
        .try_get("offer_expires_at")
        .core()?;
    let payload = parse_payload(
        row.try_get("execution_payload")
            .core()?,
    )?;
    Ok(OfferedAssignment {
        offer: WorkOffer {
            identity: AttemptIdentity {
                worker_session_id,
                attempt_id,
                fencing_generation: fencing_generation as u64,
                lease_token: sbgh_fleet::LeaseToken(lease_token),
            },
            job_id: row.try_get("job_id").core()?,
            trace_id: row
                .try_get("trace_id")
                .core()?,
            capability: capability(&capability_text)?,
            requirements: sbgh_fleet::OfferRequirements::from(&payload),
            payload_hash: row
                .try_get("payload_hash")
                .core()?,
            offer_expires_at_ms: millis(offer_expires_at),
        },
        context_repository: row
            .try_get("repository")
            .core()?,
        context_commit: row
            .try_get("git_commit_hash")
            .core()?,
        installation_id: row
            .try_get("github_installation_id")
            .core()?,
        github_repo_id: row
            .try_get("github_repo_id")
            .core()?,
        payload,
        vcpu_cpuset: None,
        measurement_profile: row
            .try_get("measurement_profile")
            .core()?,
    })
}

const OFFER_SELECT: &str = r#"
    SELECT a.attempt_id,
           a.job_id,
           a.worker_session_id,
           a.fencing_generation,
           ''::text AS lease_token,
           a.trace_id,
           a.capability::text AS capability,
           a.payload_hash,
           a.offer_expires_at,
           j.execution_payload,
           j.git_commit_hash,
           j.github_installation_id,
           j.github_repo_id,
           repo.owner || '/' || repo.name AS repository,
           submission.assigned_measurement_profile AS measurement_profile
      FROM worker_attempt a
      JOIN job j ON j.id = a.job_id
      JOIN github_repo repo ON repo.id = j.github_repo_id
      JOIN task_submission submission ON submission.id = a.task_submission_id
"#;

async fn insert_job_event(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    kind: JobEventKind,
    status: JobEventStatus,
    remark: Option<&str>,
    detail: Option<&serde_json::Value>,
) -> sbgh_core::Result<()> {
    sqlx::query(
        "INSERT INTO job_event
             (job_id, event_kind, event_status, remark, detail)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(job_id)
    .bind(Db(kind))
    .bind(Db(status))
    .bind(remark)
    .bind(detail)
    .execute(&mut **tx)
    .await
    .core()?;
    Ok(())
}

async fn expire_worker_leases(
    tx: &mut Transaction<'_, Postgres>,
    worker_id: Uuid,
    identity_key_sha256: Option<[u8; 32]>,
) -> sbgh_core::Result<()> {
    sqlx::query(
        r#"
        UPDATE worker_attempt attempt
           SET offer_expires_at = LEAST(offer_expires_at, NOW()),
               lease_expires_at = LEAST(lease_expires_at, NOW()),
               updated_at = NOW()
          FROM worker_session session
         WHERE attempt.worker_session_id = session.worker_session_id
           AND attempt.worker_id = $1
           AND ($2::bytea IS NULL OR session.identity_key_sha256 = $2)
           AND attempt.status IN ('offered', 'running', 'cancel_requested')
        "#,
    )
    .bind(worker_id)
    .bind(identity_key_sha256.map(|value| value.to_vec()))
    .execute(&mut **tx)
    .await
    .core()?;
    sqlx::query(
        r#"
        UPDATE worker_session
           SET expires_at = LEAST(expires_at, NOW()),
               status = CASE
                   WHEN status IN ('registered', 'idle', 'draining') THEN 'draining'
                   ELSE status
               END
         WHERE worker_id = $1
           AND ($2::bytea IS NULL OR identity_key_sha256 = $2)
           AND status IN ('registered', 'idle', 'offered', 'running', 'draining')
        "#,
    )
    .bind(worker_id)
    .bind(identity_key_sha256.map(|value| value.to_vec()))
    .execute(&mut **tx)
    .await
    .core()?;
    Ok(())
}

#[async_trait]
impl WorkerRegistryStore for PostgresFleetStore {
    async fn create_worker(
        &self,
        registration: &WorkerRegistration,
    ) -> sbgh_core::Result<WorkerRegistryMutation> {
        let capabilities = capability_names(&registration.allowed_capabilities);
        let expected_capabilities = capabilities.clone();
        let result = sqlx::query(
            r#"
            INSERT INTO worker_registry
                (worker_id, display_name, allowed_capabilities,
                 measurement_profile, enabled, draining)
            VALUES (
                $1,
                $2,
                ARRAY(
                    SELECT value::worker_capability
                      FROM unnest($3::text[]) AS value
                ),
                $4, $5, $6
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(registration.worker_id)
        .bind(&registration.display_name)
        .bind(capabilities)
        .bind(&registration.measurement_profile)
        .bind(registration.enabled)
        .bind(registration.draining)
        .execute(&self.pool)
        .await
        .core()?;
        if result.rows_affected() == 1 {
            return Ok(WorkerRegistryMutation::Applied);
        }
        let existing = sqlx::query(
            r#"
            SELECT display_name,
                   ARRAY(
                       SELECT capability::text
                         FROM unnest(allowed_capabilities) AS capability
                   ) AS allowed_capabilities,
                   measurement_profile, enabled, draining
              FROM worker_registry
             WHERE worker_id = $1
            "#,
        )
        .bind(registration.worker_id)
        .fetch_one(&self.pool)
        .await
        .core()?;
        let exact_retry = existing
            .try_get::<String, _>("display_name")
            .core()?
            == registration.display_name
            && existing
                .try_get::<Vec<String>, _>("allowed_capabilities")
                .core()?
                == expected_capabilities
            && existing
                .try_get::<Option<String>, _>("measurement_profile")
                .core()?
                == registration.measurement_profile
            && existing
                .try_get::<bool, _>("enabled")
                .core()?
                == registration.enabled
            && existing
                .try_get::<bool, _>("draining")
                .core()?
                == registration.draining;
        Ok(if exact_retry {
            WorkerRegistryMutation::Unchanged
        } else {
            WorkerRegistryMutation::Conflict
        })
    }

    async fn update_worker(
        &self,
        worker_id: Uuid,
        patch: &WorkerPolicyPatch,
    ) -> sbgh_core::Result<WorkerRegistryMutation> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let current = sqlx::query(
            r#"
            SELECT display_name,
                   ARRAY(
                       SELECT capability::text
                         FROM unnest(allowed_capabilities) AS capability
                   ) AS allowed_capabilities,
                   measurement_profile, enabled, draining
              FROM worker_registry
             WHERE worker_id = $1
             FOR UPDATE
            "#,
        )
        .bind(worker_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(current) = current else {
            return Ok(WorkerRegistryMutation::NotFound);
        };

        let current_capabilities: Vec<String> = current
            .try_get("allowed_capabilities")
            .core()?;
        let next_capabilities = patch
            .allowed_capabilities
            .as_ref()
            .map(capability_names)
            .unwrap_or_else(|| current_capabilities.clone());
        let current_profile: Option<String> = current
            .try_get("measurement_profile")
            .core()?;
        let next_profile = patch
            .measurement_profile
            .clone()
            .unwrap_or_else(|| current_profile.clone());
        if next_capabilities
            .iter()
            .any(|capability| capability == "benchmark")
            && !next_profile
                .as_deref()
                .is_some_and(|profile| !profile.is_empty())
        {
            return Ok(WorkerRegistryMutation::Conflict);
        }
        let current_enabled: bool = current
            .try_get("enabled")
            .core()?;
        let next_enabled = patch
            .enabled
            .unwrap_or(current_enabled);
        let sensitive = next_capabilities != current_capabilities
            || next_profile != current_profile
            || next_enabled != current_enabled;

        if sensitive {
            let draining: bool = current
                .try_get("draining")
                .core()?;
            let busy: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1 FROM worker_attempt
                     WHERE worker_id = $1
                       AND status IN ('offered', 'running', 'cancel_requested')
                ) OR EXISTS (
                    SELECT 1 FROM worker_cleanup_obligation
                     WHERE worker_id = $1 AND status = 'pending'
                )
                "#,
            )
            .bind(worker_id)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            if !draining || busy {
                return Ok(WorkerRegistryMutation::Busy);
            }
        }

        if next_enabled {
            let has_identity: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1 FROM worker_identity_key
                     WHERE worker_id = $1 AND revoked_at IS NULL
                 )",
            )
            .bind(worker_id)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            if !has_identity {
                return Ok(WorkerRegistryMutation::MissingIdentity);
            }
        }

        let display_name: String = current
            .try_get("display_name")
            .core()?;
        let next_display_name = patch
            .display_name
            .as_deref()
            .unwrap_or(&display_name);
        let result = sqlx::query(
            r#"
            UPDATE worker_registry
               SET display_name = $2,
                   allowed_capabilities = ARRAY(
                       SELECT value::worker_capability
                         FROM unnest($3::text[]) AS value
                   ),
                   measurement_profile = $4,
                   enabled = $5,
                   updated_at = NOW()
             WHERE worker_id = $1
            "#,
        )
        .bind(worker_id)
        .bind(next_display_name)
        .bind(next_capabilities)
        .bind(next_profile)
        .bind(next_enabled)
        .execute(&mut *tx)
        .await
        .core()?;
        if sensitive {
            sqlx::query(
                r#"
                UPDATE worker_session
                   SET status = 'offline', ended_at = NOW()
                 WHERE worker_id = $1
                   AND status IN ('registered', 'idle', 'draining')
                "#,
            )
            .bind(worker_id)
            .execute(&mut *tx)
            .await
            .core()?;
        }
        tx.commit().await.core()?;
        Ok(if result.rows_affected() == 1 {
            WorkerRegistryMutation::Applied
        } else {
            WorkerRegistryMutation::NotFound
        })
    }

    async fn authorize_identity(
        &self,
        worker_id: Uuid,
        identity_key_sha256: [u8; 32],
    ) -> sbgh_core::Result<WorkerRegistryMutation> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let worker_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM worker_registry WHERE worker_id = $1)",
        )
        .bind(worker_id)
        .fetch_one(&mut *tx)
        .await
        .core()?;
        if !worker_exists {
            return Ok(WorkerRegistryMutation::NotFound);
        }
        let result = sqlx::query(
            r#"
            INSERT INTO worker_identity_key (identity_key_sha256, worker_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(identity_key_sha256.as_slice())
        .bind(worker_id)
        .execute(&mut *tx)
        .await
        .core()?;
        if result.rows_affected() == 1 {
            tx.commit().await.core()?;
            return Ok(WorkerRegistryMutation::Applied);
        }
        let existing = sqlx::query(
            "SELECT worker_id, revoked_at IS NOT NULL AS revoked
               FROM worker_identity_key
              WHERE identity_key_sha256 = $1",
        )
        .bind(identity_key_sha256.as_slice())
        .fetch_one(&mut *tx)
        .await
        .core()?;
        let same_active = existing
            .try_get::<Uuid, _>("worker_id")
            .core()?
            == worker_id
            && !existing
                .try_get::<bool, _>("revoked")
                .core()?;
        tx.commit().await.core()?;
        Ok(if same_active {
            WorkerRegistryMutation::Unchanged
        } else {
            WorkerRegistryMutation::Conflict
        })
    }

    async fn revoke_identity(
        &self,
        worker_id: Uuid,
        identity_key_sha256: [u8; 32],
    ) -> sbgh_core::Result<WorkerRegistryMutation> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let identity = sqlx::query(
            r#"
            SELECT identity_key_sha256, revoked_at, registry.enabled, registry.draining
              FROM worker_identity_key identity
              JOIN worker_registry registry USING (worker_id)
             WHERE identity.worker_id = $1
               AND identity.identity_key_sha256 = $2
             FOR UPDATE OF identity, registry
            "#,
        )
        .bind(worker_id)
        .bind(identity_key_sha256.as_slice())
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(identity) = identity else {
            return Ok(WorkerRegistryMutation::NotFound);
        };
        if identity
            .try_get::<Option<DateTime<Utc>>, _>("revoked_at")
            .core()?
            .is_some()
        {
            return Ok(WorkerRegistryMutation::Unchanged);
        }
        let used_by_live_session: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM worker_session
                 WHERE worker_id = $1
                   AND identity_key_sha256 = $2
                   AND status IN ('registered', 'idle', 'offered', 'running', 'draining')
            )
            "#,
        )
        .bind(worker_id)
        .bind(identity_key_sha256.as_slice())
        .fetch_one(&mut *tx)
        .await
        .core()?;
        let active_identity_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM worker_identity_key
              WHERE worker_id = $1 AND revoked_at IS NULL",
        )
        .bind(worker_id)
        .fetch_one(&mut *tx)
        .await
        .core()?;
        let final_identity_for_enabled_worker = active_identity_count == 1
            && identity
                .try_get::<bool, _>("enabled")
                .core()?;
        if used_by_live_session || final_identity_for_enabled_worker {
            let draining: bool = identity
                .try_get("draining")
                .core()?;
            let busy: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1 FROM worker_attempt
                     WHERE worker_id = $1
                       AND status IN ('offered', 'running', 'cancel_requested')
                ) OR EXISTS (
                    SELECT 1 FROM worker_cleanup_obligation
                     WHERE worker_id = $1 AND status = 'pending'
                )
                "#,
            )
            .bind(worker_id)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            if !draining || busy {
                return Ok(WorkerRegistryMutation::Busy);
            }
            if used_by_live_session {
                sqlx::query(
                    r#"
                    UPDATE worker_session
                       SET status = 'offline', ended_at = NOW()
                     WHERE worker_id = $1
                       AND identity_key_sha256 = $2
                       AND status IN ('registered', 'idle', 'draining')
                    "#,
                )
                .bind(worker_id)
                .bind(identity_key_sha256.as_slice())
                .execute(&mut *tx)
                .await
                .core()?;
            }
        }
        sqlx::query(
            "UPDATE worker_identity_key
                SET revoked_at = NOW()
              WHERE identity_key_sha256 = $1",
        )
        .bind(identity_key_sha256.as_slice())
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(WorkerRegistryMutation::Applied)
    }

    async fn emergency_disable_worker(
        &self,
        worker_id: Uuid,
    ) -> sbgh_core::Result<WorkerRegistryMutation> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let result = sqlx::query(
            "UPDATE worker_registry
                SET enabled = FALSE, draining = TRUE, updated_at = NOW()
              WHERE worker_id = $1",
        )
        .bind(worker_id)
        .execute(&mut *tx)
        .await
        .core()?;
        if result.rows_affected() == 0 {
            return Ok(WorkerRegistryMutation::NotFound);
        }
        expire_worker_leases(&mut tx, worker_id, None).await?;
        tx.commit().await.core()?;
        Ok(WorkerRegistryMutation::Applied)
    }

    async fn emergency_revoke_identity(
        &self,
        worker_id: Uuid,
        identity_key_sha256: [u8; 32],
    ) -> sbgh_core::Result<WorkerRegistryMutation> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let result = sqlx::query(
            "UPDATE worker_identity_key
                SET revoked_at = NOW()
              WHERE worker_id = $1
                AND identity_key_sha256 = $2
                AND revoked_at IS NULL",
        )
        .bind(worker_id)
        .bind(identity_key_sha256.as_slice())
        .execute(&mut *tx)
        .await
        .core()?;
        if result.rows_affected() == 0 {
            let existing: Option<bool> = sqlx::query_scalar(
                "SELECT revoked_at IS NOT NULL
                   FROM worker_identity_key
                  WHERE worker_id = $1 AND identity_key_sha256 = $2",
            )
            .bind(worker_id)
            .bind(identity_key_sha256.as_slice())
            .fetch_optional(&mut *tx)
            .await
            .core()?;
            return Ok(match existing {
                Some(true) => WorkerRegistryMutation::Unchanged,
                _ => WorkerRegistryMutation::NotFound,
            });
        }
        expire_worker_leases(&mut tx, worker_id, Some(identity_key_sha256)).await?;
        tx.commit().await.core()?;
        Ok(WorkerRegistryMutation::Applied)
    }

    async fn worker_identities(
        &self,
        worker_id: Uuid,
    ) -> sbgh_core::Result<Vec<WorkerIdentityRecord>> {
        let rows = sqlx::query(
            "SELECT identity_key_sha256, created_at, revoked_at
               FROM worker_identity_key
              WHERE worker_id = $1
              ORDER BY created_at, identity_key_sha256",
        )
        .bind(worker_id)
        .fetch_all(&self.pool)
        .await
        .core()?;
        rows.into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row
                    .try_get("identity_key_sha256")
                    .core()?;
                let identity_key_sha256 = bytes
                    .try_into()
                    .map_err(|_| {
                        sbgh_core::Error::Config("worker identity digest is not 32 bytes".into())
                    })?;
                Ok(WorkerIdentityRecord {
                    identity_key_sha256,
                    created_at: row
                        .try_get("created_at")
                        .core()?,
                    revoked_at: row
                        .try_get("revoked_at")
                        .core()?,
                })
            })
            .collect()
    }

    async fn workers(
        &self,
        worker_id: Option<Uuid>,
    ) -> sbgh_core::Result<Vec<WorkerRegistryEntry>> {
        let rows = sqlx::query(
            r#"
            SELECT registry.worker_id,
                   registry.display_name,
                   registry.enabled,
                   registry.draining,
                   ARRAY(
                       SELECT capability::text
                         FROM unnest(registry.allowed_capabilities) capability
                   ) AS capabilities,
                   registry.measurement_profile,
                   session.worker_session_id,
                   session.status::text AS session_status,
                   session.software_version,
                   session.last_heartbeat_at,
                   session.expires_at,
                   session.resource_facts,
                   attempt.attempt_id,
                   attempt.job_id,
                   attempt.trace_id
              FROM worker_registry registry
              LEFT JOIN LATERAL (
                  SELECT *
                    FROM worker_session
                   WHERE worker_id = registry.worker_id
                   ORDER BY started_at DESC
                   LIMIT 1
              ) session ON TRUE
              LEFT JOIN LATERAL (
                  SELECT attempt_id, job_id, trace_id
                    FROM worker_attempt
                   WHERE worker_id = registry.worker_id
                     AND status IN ('offered', 'running', 'cancel_requested')
                   ORDER BY created_at DESC
                   LIMIT 1
              ) attempt ON TRUE
             WHERE ($1::uuid IS NULL OR registry.worker_id = $1)
             ORDER BY registry.display_name, registry.worker_id
            "#,
        )
        .bind(worker_id)
        .fetch_all(&self.pool)
        .await
        .core()?;
        rows.into_iter()
            .map(|row| {
                let capabilities: Vec<String> = row
                    .try_get("capabilities")
                    .core()?;
                Ok(WorkerRegistryEntry {
                    worker_id: row
                        .try_get("worker_id")
                        .core()?,
                    display_name: row
                        .try_get("display_name")
                        .core()?,
                    enabled: row
                        .try_get("enabled")
                        .core()?,
                    draining: row
                        .try_get("draining")
                        .core()?,
                    allowed_capabilities: capabilities,
                    measurement_profile: row
                        .try_get("measurement_profile")
                        .core()?,
                    worker_session_id: row
                        .try_get("worker_session_id")
                        .core()?,
                    session_status: row
                        .try_get("session_status")
                        .core()?,
                    software_version: row
                        .try_get("software_version")
                        .core()?,
                    last_heartbeat_at: row
                        .try_get("last_heartbeat_at")
                        .core()?,
                    session_expires_at: row
                        .try_get("expires_at")
                        .core()?,
                    resource_facts: row
                        .try_get("resource_facts")
                        .core()?,
                    attempt_id: row
                        .try_get("attempt_id")
                        .core()?,
                    job_id: row.try_get("job_id").core()?,
                    trace_id: row
                        .try_get("trace_id")
                        .core()?,
                })
            })
            .collect()
    }

    async fn authorize_worker(
        &self,
        identity_key_sha256: [u8; 32],
    ) -> sbgh_core::Result<Option<WorkerAuthorization>> {
        let row = sqlx::query(
            r#"
            SELECT registry.worker_id,
                   ARRAY(
                       SELECT capability::text
                         FROM unnest(registry.allowed_capabilities) AS capability
                   ) AS allowed_capabilities,
                   registry.measurement_profile,
                   registry.draining
              FROM worker_registry registry
              JOIN worker_identity_key identity
                ON identity.worker_id = registry.worker_id
               AND identity.identity_key_sha256 = $1
               AND identity.revoked_at IS NULL
             WHERE registry.enabled
            "#,
        )
        .bind(identity_key_sha256.as_slice())
        .fetch_optional(&self.pool)
        .await
        .core()?;
        row.map(|row| {
            let capabilities: Vec<String> = row
                .try_get("allowed_capabilities")
                .core()?;
            Ok(WorkerAuthorization {
                worker_id: row
                    .try_get("worker_id")
                    .core()?,
                allowed_capabilities: capabilities
                    .iter()
                    .map(|value| capability(value))
                    .collect::<sbgh_core::Result<Vec<_>>>()?,
                measurement_profile: row
                    .try_get("measurement_profile")
                    .core()?,
                draining: row
                    .try_get("draining")
                    .core()?,
            })
        })
        .transpose()
    }

    async fn set_worker_draining(
        &self,
        worker_id: Uuid,
        draining: bool,
    ) -> sbgh_core::Result<WorkerRegistryMutation> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let result = sqlx::query(
            "UPDATE worker_registry
                SET draining = $2, updated_at = NOW()
              WHERE worker_id = $1",
        )
        .bind(worker_id)
        .bind(draining)
        .execute(&mut *tx)
        .await
        .core()?;
        if result.rows_affected() == 0 {
            return Ok(WorkerRegistryMutation::NotFound);
        }
        if !draining {
            sqlx::query(
                "UPDATE worker_session
                    SET status = 'idle'
                  WHERE worker_id = $1
                    AND status = 'draining'
                    AND expires_at > NOW()",
            )
            .bind(worker_id)
            .execute(&mut *tx)
            .await
            .core()?;
        }
        tx.commit().await.core()?;
        Ok(WorkerRegistryMutation::Applied)
    }
}

#[async_trait]
impl FleetStore for PostgresFleetStore {
    async fn register_session(
        &self,
        worker_id: Uuid,
        identity_key_sha256: [u8; 32],
        request: &RegisterSessionRequest,
        session_ttl: Duration,
    ) -> sbgh_core::Result<AuthorizedSession> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let policy = sqlx::query(
            r#"
            SELECT ARRAY(
                       SELECT capability::text
                         FROM unnest(allowed_capabilities) AS capability
                   ) AS allowed_capabilities,
                   measurement_profile,
                   enabled,
                   draining
              FROM worker_registry registry
              JOIN worker_identity_key identity
                ON identity.worker_id = registry.worker_id
               AND identity.identity_key_sha256 = $2
               AND identity.revoked_at IS NULL
             WHERE registry.worker_id = $1
             FOR UPDATE
            "#,
        )
        .bind(worker_id)
        .bind(identity_key_sha256.as_slice())
        .fetch_optional(&mut *tx)
        .await
        .core()?
        .ok_or_else(|| sbgh_core::Error::Config("worker is not registered".into()))?;
        let enabled: bool = policy
            .try_get("enabled")
            .core()?;
        if !enabled {
            return Err(sbgh_core::Error::Config("worker identity is disabled".into()));
        }
        let allowed: Vec<String> = policy
            .try_get("allowed_capabilities")
            .core()?;
        let advertised = capability_names(
            request
                .advertised_capabilities
                .iter(),
        );
        let effective: Vec<String> = advertised
            .into_iter()
            .filter(|item| allowed.contains(item))
            .collect();
        if effective.is_empty() {
            return Err(sbgh_core::Error::Config(
                "worker advertises no server-authorized capability".into(),
            ));
        }

        let advertised = capability_names(
            request
                .advertised_capabilities
                .iter(),
        );
        let resources = serde_json::to_value(&request.resources)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let expires_at = Utc::now() + session_ttl;
        let draining: bool = policy
            .try_get("draining")
            .core()?;
        if let Some(existing) = sqlx::query(
            r#"
            SELECT ARRAY(
                       SELECT capability::text
                         FROM unnest(advertised_capabilities) AS capability
                   ) AS advertised_capabilities,
                   protocol_version,
                   software_version,
                   resource_facts,
                   identity_key_sha256
              FROM worker_session
             WHERE worker_id = $1
               AND worker_session_id = $2
               AND status IN ('registered', 'idle', 'offered', 'running', 'draining')
             FOR UPDATE
            "#,
        )
        .bind(worker_id)
        .bind(request.worker_session_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?
        {
            let same = existing
                .try_get::<i32, _>("protocol_version")
                .core()?
                == i32::from(request.protocol_version)
                && existing
                    .try_get::<String, _>("software_version")
                    .core()?
                    == request.software_version
                && existing
                    .try_get::<Vec<String>, _>("advertised_capabilities")
                    .core()?
                    == advertised
                && existing
                    .try_get::<serde_json::Value, _>("resource_facts")
                    .core()?
                    == resources
                && existing
                    .try_get::<Option<Vec<u8>>, _>("identity_key_sha256")
                    .core()?
                    .as_deref()
                    == Some(identity_key_sha256.as_slice());
            if !same {
                return Err(sbgh_core::Error::Config(
                    "a worker session UUID cannot be reused with different registration facts"
                        .into(),
                ));
            }
            sqlx::query(
                r#"
                UPDATE worker_session
                   SET expires_at = $2,
                       last_heartbeat_at = NOW(),
                       effective_capabilities = ARRAY(
                           SELECT value::worker_capability
                             FROM unnest($4::text[]) AS value
                       ),
                       status = CASE
                           WHEN $3 THEN 'draining'::worker_session_status
                           ELSE status
                       END
                 WHERE worker_session_id = $1
                "#,
            )
            .bind(request.worker_session_id)
            .bind(expires_at)
            .bind(draining)
            .bind(&effective)
            .execute(&mut *tx)
            .await
            .core()?;
            tx.commit().await.core()?;
            return Ok(AuthorizedSession {
                worker_id,
                worker_session_id: request.worker_session_id,
                effective_capabilities: effective
                    .iter()
                    .map(|item| capability(item))
                    .collect::<sbgh_core::Result<Vec<_>>>()?,
                measurement_profile: policy
                    .try_get("measurement_profile")
                    .core()?,
                draining,
                expires_at,
            });
        }

        sqlx::query(
            r#"
            UPDATE worker_session
               SET status = 'offline',
                   ended_at = NOW()
             WHERE worker_id = $1
               AND status IN ('registered', 'idle', 'offered', 'running', 'draining')
               AND expires_at <= NOW()
            "#,
        )
        .bind(worker_id)
        .execute(&mut *tx)
        .await
        .core()?;
        let predecessor_attempts = sqlx::query(
            r#"
            SELECT attempt_id, job_id, worker_session_id, status::text AS status
              FROM worker_attempt
             WHERE worker_id = $1
               AND worker_session_id <> $2
               AND status IN ('offered', 'running', 'cancel_requested')
             FOR UPDATE
            "#,
        )
        .bind(worker_id)
        .bind(request.worker_session_id)
        .fetch_all(&mut *tx)
        .await
        .core()?;
        for attempt in predecessor_attempts {
            let attempt_id: Uuid = attempt
                .try_get("attempt_id")
                .core()?;
            let job_id: Uuid = attempt
                .try_get("job_id")
                .core()?;
            let predecessor_session_id: Uuid = attempt
                .try_get("worker_session_id")
                .core()?;
            let status: String = attempt
                .try_get("status")
                .core()?;
            discard_attempt_projection(
                &mut tx,
                attempt_id,
                "predecessor worker session was fenced",
            )
            .await?;
            sqlx::query(
                "UPDATE worker_attempt SET status = 'fenced', updated_at = NOW()
                  WHERE attempt_id = $1",
            )
            .bind(attempt_id)
            .execute(&mut *tx)
            .await
            .core()?;
            if status == "offered" {
                sqlx::query(
                    r#"
                    UPDATE job
                       SET status = 'queued', claim_token = NULL,
                           claimed_at = NULL, updated_at = NOW()
                     WHERE id = $1 AND status = 'claimed'
                    "#,
                )
                .bind(job_id)
                .execute(&mut *tx)
                .await
                .core()?;
            } else {
                sqlx::query(
                    r#"
                    INSERT INTO worker_cleanup_obligation
                        (worker_id, worker_session_id, attempt_id, job_id,
                         requeue_job, reason)
                    VALUES (
                        $1, $2, $3, $4, $5,
                        'worker process registered a successor session'
                    )
                    ON CONFLICT (attempt_id) DO NOTHING
                    "#,
                )
                .bind(worker_id)
                .bind(predecessor_session_id)
                .bind(attempt_id)
                .bind(job_id)
                .bind(status == "running")
                .execute(&mut *tx)
                .await
                .core()?;
            }
        }
        sqlx::query(
            r#"
            UPDATE worker_session
               SET status = 'offline', ended_at = NOW()
             WHERE worker_id = $1
               AND worker_session_id <> $2
               AND status IN ('registered', 'idle', 'offered', 'running', 'draining')
            "#,
        )
        .bind(worker_id)
        .bind(request.worker_session_id)
        .execute(&mut *tx)
        .await
        .core()?;

        sqlx::query(
            r#"
            INSERT INTO worker_session
                (worker_session_id, worker_id, status, protocol_version,
                 software_version, advertised_capabilities, effective_capabilities,
                 resource_facts, expires_at, identity_key_sha256)
            VALUES (
                $1, $2, $3::text::worker_session_status, $4, $5,
                ARRAY(SELECT value::worker_capability FROM unnest($6::text[]) AS value),
                ARRAY(SELECT value::worker_capability FROM unnest($7::text[]) AS value),
                $8, $9, $10
            )
            "#,
        )
        .bind(request.worker_session_id)
        .bind(worker_id)
        .bind(if draining { "draining" } else { "registered" })
        .bind(i32::from(request.protocol_version))
        .bind(&request.software_version)
        .bind(advertised)
        .bind(&effective)
        .bind(resources)
        .bind(expires_at)
        .bind(identity_key_sha256.as_slice())
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;

        Ok(AuthorizedSession {
            worker_id,
            worker_session_id: request.worker_session_id,
            effective_capabilities: effective
                .iter()
                .map(|item| capability(item))
                .collect::<sbgh_core::Result<Vec<_>>>()?,
            measurement_profile: policy
                .try_get("measurement_profile")
                .core()?,
            draining,
            expires_at,
        })
    }

    async fn deregister_session(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
    ) -> sbgh_core::Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE worker_session session
               SET status = 'deregistered',
                   ended_at = NOW()
             WHERE session.worker_id = $1
               AND session.worker_session_id = $2
               AND session.status IN ('registered', 'idle', 'draining')
               AND NOT EXISTS (
                   SELECT 1
                     FROM worker_attempt attempt
                    WHERE attempt.worker_session_id = session.worker_session_id
                      AND attempt.status IN ('offered', 'running', 'cancel_requested')
               )
            "#,
        )
        .bind(worker_id)
        .bind(worker_session_id)
        .execute(&self.pool)
        .await
        .core()?;
        Ok(result.rows_affected() == 1)
    }

    async fn freeze_submission_sources(
        &self,
        submission_id: Uuid,
        sources: &[ResolvedSpecSource],
    ) -> sbgh_core::Result<bool> {
        let mut resolved = BTreeMap::new();
        for source in sources {
            if !matches!(source.commit.len(), 40 | 64)
                || !source
                    .commit
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                || resolved
                    .insert(source.spec_id, source.commit.as_str())
                    .is_some()
            {
                return Err(sbgh_core::Error::Config(
                    "group source resolutions must contain unique specs and immutable commits"
                        .into(),
                ));
            }
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let rows = sqlx::query(
            r#"
            SELECT id, git_commit_hash
              FROM task_spec
             WHERE task_submission_id = $1
             ORDER BY spec_index
             FOR UPDATE
            "#,
        )
        .bind(submission_id)
        .fetch_all(&mut *tx)
        .await
        .core()?;
        if rows.is_empty() || rows.len() != resolved.len() {
            tx.rollback().await.core()?;
            return Ok(false);
        }
        for row in rows {
            let spec_id: Uuid = row.try_get("id").core()?;
            let Some(commit) = resolved
                .get(&spec_id)
                .copied()
            else {
                tx.rollback().await.core()?;
                return Ok(false);
            };
            let existing: Option<String> = row
                .try_get("git_commit_hash")
                .core()?;
            if existing
                .as_deref()
                .is_some_and(|existing| !existing.eq_ignore_ascii_case(commit))
            {
                return Err(sbgh_core::Error::Config(format!(
                    "benchmark spec {spec_id} is already frozen to a different commit"
                )));
            }
            let conflicting_job: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                      FROM job
                     WHERE task_spec_id = $1
                       AND git_commit_hash IS NOT NULL
                       AND lower(git_commit_hash) <> lower($2)
                )
                "#,
            )
            .bind(spec_id)
            .bind(commit)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            if conflicting_job {
                return Err(sbgh_core::Error::Config(format!(
                    "benchmark spec {spec_id} has a job at a different commit"
                )));
            }
            sqlx::query(
                r#"
                UPDATE task_spec
                   SET git_commit_hash = $2, updated_at = NOW()
                 WHERE id = $1
                "#,
            )
            .bind(spec_id)
            .bind(commit)
            .execute(&mut *tx)
            .await
            .core()?;
            sqlx::query(
                r#"
                UPDATE job
                   SET git_commit_hash = $2, updated_at = NOW()
                 WHERE task_spec_id = $1
                   AND status = 'queued'
                   AND git_commit_hash IS NULL
                "#,
            )
            .bind(spec_id)
            .bind(commit)
            .execute(&mut *tx)
            .await
            .core()?;
        }
        tx.commit().await.core()?;
        Ok(true)
    }

    async fn prepare_execution(&self, prepared: &PreparedExecution) -> sbgh_core::Result<bool> {
        let payload = serde_json::to_value(&prepared.payload)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let result = sqlx::query(
            r#"
            UPDATE job
               SET git_commit_hash = $2,
                   execution_payload_version = $3,
                   execution_payload = $4,
                   execution_payload_hash = $5,
                   updated_at = NOW()
             WHERE id = $1
               AND status = 'queued'
               AND execution_payload IS NULL
            "#,
        )
        .bind(prepared.job_id)
        .bind(&prepared.commit)
        .bind(i32::from(sbgh_fleet::PROTOCOL_VERSION))
        .bind(payload)
        .bind(&prepared.payload_hash)
        .execute(&self.pool)
        .await
        .core()?;
        Ok(result.rows_affected() == 1)
    }

    async fn poll_offer(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
        offer_ttl: Duration,
        lease_ttl: Duration,
    ) -> sbgh_core::Result<Option<OfferedAssignment>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let session = sqlx::query(
            r#"
            SELECT ARRAY(
                       SELECT capability::text
                         FROM unnest(session.advertised_capabilities) AS capability
                        WHERE capability = ANY(registry.allowed_capabilities)
                   ) AS capabilities,
                   registry.measurement_profile,
                   registry.draining,
                   session.status::text AS status
              FROM worker_session session
              JOIN worker_registry registry ON registry.worker_id = session.worker_id
             WHERE session.worker_id = $1
               AND session.worker_session_id = $2
               AND registry.enabled
               AND session.advertised_capabilities && registry.allowed_capabilities
               AND session.expires_at > NOW()
               AND session.status IN ('registered', 'idle', 'offered')
             FOR UPDATE OF session, registry
            "#,
        )
        .bind(worker_id)
        .bind(worker_session_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(session) = session else {
            tx.rollback().await.core()?;
            return Ok(None);
        };
        sqlx::query(
            r#"
            UPDATE worker_session
               SET last_heartbeat_at = NOW(),
                   expires_at = NOW() + make_interval(secs => $2)
             WHERE worker_session_id = $1
            "#,
        )
        .bind(worker_session_id)
        .bind(duration_seconds(lease_ttl * 2))
        .execute(&mut *tx)
        .await
        .core()?;
        let draining: bool = session
            .try_get("draining")
            .core()?;
        if draining {
            sqlx::query(
                "UPDATE worker_session SET status = 'draining'
                  WHERE worker_session_id = $1",
            )
            .bind(worker_session_id)
            .execute(&mut *tx)
            .await
            .core()?;
            tx.commit().await.core()?;
            return Ok(None);
        }

        let existing_sql = format!(
            "{OFFER_SELECT}
             WHERE a.worker_id = $1
               AND a.worker_session_id = $2
               AND a.status = 'offered'
               AND a.offer_expires_at > NOW()
             ORDER BY a.created_at
             LIMIT 1"
        );
        // `existing_sql` is composed only from the static `OFFER_SELECT`
        // constant and a static suffix; all values remain bind parameters.
        if let Some(row) = sqlx::query(sqlx::AssertSqlSafe(existing_sql))
            .bind(worker_id)
            .bind(worker_session_id)
            .fetch_optional(&mut *tx)
            .await
            .core()?
        {
            tx.commit().await.core()?;
            return Ok(Some(offer_from_row(&row)?));
        }

        let capabilities: Vec<String> = session
            .try_get("capabilities")
            .core()?;
        let measurement_profile: Option<String> = session
            .try_get("measurement_profile")
            .core()?;
        let row = sqlx::query(
            r#"
            SELECT j.id AS job_id,
                   j.task_submission_id,
                   j.github_installation_id,
                   j.github_repo_id,
                   j.git_commit_hash,
                   j.execution_payload,
                   j.execution_payload_hash,
                   j.required_capability::text AS capability,
                   repo.owner || '/' || repo.name AS repository,
                   submission.assigned_worker_id AS placed_worker_id,
                   submission.assigned_measurement_profile AS placed_measurement_profile,
                   submission.execution_generation,
                   submission.fencing_generation
              FROM job j
              JOIN task_submission submission ON submission.id = j.task_submission_id
              JOIN github_repo repo ON repo.id = j.github_repo_id
             WHERE j.status = 'queued'
               AND j.execution_payload IS NOT NULL
               AND j.git_commit_hash IS NOT NULL
               AND j.required_capability::text = ANY($1::text[])
               AND (
                   submission.required_worker_id IS NULL
                   OR submission.required_worker_id = $2
               )
               AND (
                   submission.assigned_worker_id IS NULL
                   OR submission.assigned_worker_id = $2
               )
               AND (
                   j.task_kind <> 'benchmark'
                   OR submission.required_measurement_profile IS NULL
                   OR submission.required_measurement_profile IS NOT DISTINCT FROM $3
               )
               AND (
                   j.task_kind <> 'benchmark'
                   OR submission.assigned_measurement_profile IS NULL
                   OR submission.assigned_measurement_profile IS NOT DISTINCT FROM $3
               )
               AND NOT EXISTS (
                   SELECT 1
                     FROM worker_attempt active
                    WHERE active.task_submission_id = submission.id
                      AND active.status IN ('offered', 'running', 'cancel_requested')
               )
             ORDER BY j.created_at, j.id
             FOR UPDATE OF j, submission SKIP LOCKED
             LIMIT 1
            "#,
        )
        .bind(&capabilities)
        .bind(worker_id)
        .bind(&measurement_profile)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(row) = row else {
            sqlx::query(
                "UPDATE worker_session
                    SET status = 'idle', last_heartbeat_at = NOW()
                  WHERE worker_session_id = $1",
            )
            .bind(worker_session_id)
            .execute(&mut *tx)
            .await
            .core()?;
            tx.commit().await.core()?;
            return Ok(None);
        };

        let job_id: Uuid = row.try_get("job_id").core()?;
        let task_submission_id: Uuid = row
            .try_get("task_submission_id")
            .core()?;
        let capability_text: String = row
            .try_get("capability")
            .core()?;
        let task_capability = capability(&capability_text)?;
        let execution_generation: i64 = row
            .try_get("execution_generation")
            .core()?;
        let prior_fence: i64 = row
            .try_get("fencing_generation")
            .core()?;
        let fence = prior_fence + 1;
        let claim_token = Uuid::new_v4();
        let attempt_id = Uuid::new_v4();
        let trace_id = Uuid::new_v4();
        let offer_expires_at = Utc::now() + offer_ttl;
        let lease_expires_at = Utc::now() + lease_ttl;
        let profile_for_submission = if task_capability == WorkerCapability::Benchmark {
            measurement_profile.clone()
        } else {
            None
        };

        let placement = sqlx::query(
            r#"
            UPDATE task_submission
               SET assigned_worker_id = COALESCE(assigned_worker_id, $2),
                   assigned_measurement_profile = CASE
                       WHEN assigned_measurement_profile IS NULL THEN $3
                       ELSE assigned_measurement_profile
                   END,
                   fencing_generation = $4,
                   updated_at = NOW()
             WHERE id = $1
               AND (assigned_worker_id IS NULL OR assigned_worker_id = $2)
               AND (required_worker_id IS NULL OR required_worker_id = $2)
            "#,
        )
        .bind(task_submission_id)
        .bind(worker_id)
        .bind(&profile_for_submission)
        .bind(fence)
        .execute(&mut *tx)
        .await
        .core()?;
        if placement.rows_affected() != 1 {
            tx.rollback().await.core()?;
            return Ok(None);
        }
        let claimed = sqlx::query(
            r#"
            UPDATE job
               SET status = 'claimed',
                   claim_token = $2,
                   claimed_at = NOW(),
                   updated_at = NOW()
             WHERE id = $1
               AND status = 'queued'
            "#,
        )
        .bind(job_id)
        .bind(claim_token)
        .execute(&mut *tx)
        .await
        .core()?;
        if claimed.rows_affected() != 1 {
            tx.rollback().await.core()?;
            return Ok(None);
        }
        sqlx::query(
            r#"
            INSERT INTO worker_attempt
                (attempt_id, job_id, task_submission_id, worker_id,
                 worker_session_id, status, fencing_generation,
                 execution_generation, claim_token, trace_id, capability,
                 payload_hash, offer_expires_at, lease_expires_at)
            VALUES (
                $1, $2, $3, $4, $5, 'offered', $6, $7, $8, $9,
                $10::text::worker_capability, $11, $12, $13
            )
            "#,
        )
        .bind(attempt_id)
        .bind(job_id)
        .bind(task_submission_id)
        .bind(worker_id)
        .bind(worker_session_id)
        .bind(fence)
        .bind(execution_generation)
        .bind(claim_token)
        .bind(trace_id)
        .bind(&capability_text)
        .bind(
            row.try_get::<String, _>("execution_payload_hash")
                .core()?,
        )
        .bind(offer_expires_at)
        .bind(lease_expires_at)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            "UPDATE worker_session
                SET status = 'offered', last_heartbeat_at = NOW()
              WHERE worker_session_id = $1",
        )
        .bind(worker_session_id)
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;

        let payload = parse_payload(
            row.try_get("execution_payload")
                .core()?,
        )?;
        Ok(Some(OfferedAssignment {
            offer: WorkOffer {
                identity: AttemptIdentity {
                    worker_session_id,
                    attempt_id,
                    fencing_generation: fence as u64,
                    lease_token: sbgh_fleet::LeaseToken(String::new()),
                },
                job_id,
                trace_id,
                capability: task_capability,
                requirements: sbgh_fleet::OfferRequirements::from(&payload),
                payload_hash: row
                    .try_get("execution_payload_hash")
                    .core()?,
                offer_expires_at_ms: millis(offer_expires_at),
            },
            context_repository: row
                .try_get("repository")
                .core()?,
            context_commit: row
                .try_get("git_commit_hash")
                .core()?,
            installation_id: row
                .try_get("github_installation_id")
                .core()?,
            github_repo_id: row
                .try_get("github_repo_id")
                .core()?,
            payload,
            vcpu_cpuset: None,
            measurement_profile: profile_for_submission,
        }))
    }

    async fn session_is_draining(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
    ) -> sbgh_core::Result<bool> {
        sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM worker_session session
                  JOIN worker_registry registry
                    ON registry.worker_id = session.worker_id
                 WHERE session.worker_id = $1
                   AND session.worker_session_id = $2
                   AND registry.enabled
                   AND session.advertised_capabilities && registry.allowed_capabilities
                   AND registry.draining
                   AND session.status = 'draining'
                   AND session.expires_at > NOW()
            )
            "#,
        )
        .bind(worker_id)
        .bind(worker_session_id)
        .fetch_one(&self.pool)
        .await
        .core()
    }

    async fn session_is_active(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
    ) -> sbgh_core::Result<bool> {
        sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM worker_session session
                  JOIN worker_registry registry
                    ON registry.worker_id = session.worker_id
                 WHERE session.worker_id = $1
                   AND session.worker_session_id = $2
                   AND registry.enabled
                   AND session.advertised_capabilities && registry.allowed_capabilities
                   AND session.status IN (
                       'registered', 'idle', 'offered', 'running', 'draining'
                   )
                   AND session.expires_at > NOW()
            )
            "#,
        )
        .bind(worker_id)
        .bind(worker_session_id)
        .fetch_one(&self.pool)
        .await
        .core()
    }

    async fn offered_assignment(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
    ) -> sbgh_core::Result<Option<OfferedAssignment>> {
        let sql = format!(
            "{OFFER_SELECT}
             WHERE a.worker_id = $1
               AND a.worker_session_id = $2
               AND a.attempt_id = $3
               AND a.fencing_generation = $4
               AND a.status IN ('offered', 'running', 'cancel_requested')
               AND a.lease_expires_at > NOW()
               AND EXISTS (
                   SELECT 1 FROM worker_registry registry
                    WHERE registry.worker_id = a.worker_id
                      AND registry.enabled
                      AND a.capability = ANY(registry.allowed_capabilities)
               )
               AND EXISTS (
                   SELECT 1 FROM worker_session session
                    WHERE session.worker_session_id = a.worker_session_id
                      AND session.expires_at > NOW()
                      AND session.status NOT IN ('offline', 'deregistered')
               )"
        );
        // `sql` is composed only from the static `OFFER_SELECT` constant and a
        // static suffix; all values remain bind parameters.
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(worker_id)
            .bind(identity.worker_session_id)
            .bind(identity.attempt_id)
            .bind(identity.fencing_generation as i64)
            .fetch_optional(&self.pool)
            .await
            .core()?;
        row.map(|row| offer_from_row(&row))
            .transpose()
    }

    async fn completion_assignment(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
    ) -> sbgh_core::Result<Option<OfferedAssignment>> {
        let sql = format!(
            "{OFFER_SELECT}
             WHERE a.worker_id = $1
               AND a.worker_session_id = $2
               AND a.attempt_id = $3
               AND a.fencing_generation = $4
               AND a.status IN (
                   'running', 'cancel_requested', 'completed', 'failed', 'cancelled'
               )
               AND EXISTS (
                   SELECT 1 FROM worker_registry registry
                    WHERE registry.worker_id = a.worker_id
                      AND registry.enabled
                      AND a.capability = ANY(registry.allowed_capabilities)
               )"
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(worker_id)
            .bind(identity.worker_session_id)
            .bind(identity.attempt_id)
            .bind(identity.fencing_generation as i64)
            .fetch_optional(&self.pool)
            .await
            .core()?;
        row.map(|row| offer_from_row(&row))
            .transpose()
    }

    async fn accept_offer(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
        lease_ttl: Duration,
    ) -> sbgh_core::Result<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let attempt = sqlx::query(
            r#"
            SELECT job_id, claim_token, status::text AS status
              FROM worker_attempt
             WHERE attempt_id = $1
               AND worker_id = $2
               AND worker_session_id = $3
               AND fencing_generation = $4
             FOR UPDATE
            "#,
        )
        .bind(identity.attempt_id)
        .bind(worker_id)
        .bind(identity.worker_session_id)
        .bind(identity.fencing_generation as i64)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(attempt) = attempt else {
            tx.rollback().await.core()?;
            return Ok(false);
        };
        let status: String = attempt
            .try_get("status")
            .core()?;
        if status == "running" || status == "cancel_requested" {
            tx.commit().await.core()?;
            return Ok(true);
        }
        if status != "offered" {
            tx.rollback().await.core()?;
            return Ok(false);
        }
        let job_id: Uuid = attempt
            .try_get("job_id")
            .core()?;
        let claim_token: Uuid = attempt
            .try_get("claim_token")
            .core()?;
        let running = sqlx::query(
            r#"
            UPDATE job
               SET status = 'running', updated_at = NOW()
             WHERE id = $1
               AND claim_token = $2
               AND status = 'claimed'
            "#,
        )
        .bind(job_id)
        .bind(claim_token)
        .execute(&mut *tx)
        .await
        .core()?;
        if running.rows_affected() != 1 {
            tx.rollback().await.core()?;
            return Ok(false);
        }
        let updated = sqlx::query(
            r#"
            UPDATE worker_attempt
               SET status = 'running',
                   accepted_at = NOW(),
                   lease_expires_at = NOW() + make_interval(secs => $5),
                   updated_at = NOW()
             WHERE attempt_id = $1
               AND worker_id = $2
               AND worker_session_id = $3
               AND fencing_generation = $4
               AND status = 'offered'
               AND offer_expires_at > NOW()
            "#,
        )
        .bind(identity.attempt_id)
        .bind(worker_id)
        .bind(identity.worker_session_id)
        .bind(identity.fencing_generation as i64)
        .bind(duration_seconds(lease_ttl))
        .execute(&mut *tx)
        .await
        .core()?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.core()?;
            return Ok(false);
        }
        sqlx::query(
            "UPDATE worker_session
                SET status = 'running', last_heartbeat_at = NOW()
              WHERE worker_session_id = $1",
        )
        .bind(identity.worker_session_id)
        .execute(&mut *tx)
        .await
        .core()?;
        insert_job_event(
            &mut tx,
            job_id,
            JobEventKind::Claimed,
            JobEventStatus::Success,
            None,
            Some(&serde_json::json!({
                "attempt_id": identity.attempt_id,
                "fencing_generation": identity.fencing_generation,
                "worker_id": worker_id,
            })),
        )
        .await?;
        tx.commit().await.core()?;
        Ok(true)
    }

    async fn heartbeat_attempt(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
        lease_ttl: Duration,
        reliable_buffer_len: Option<u32>,
    ) -> sbgh_core::Result<Option<AttemptHeartbeat>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let row = sqlx::query(
            r#"
            UPDATE worker_attempt attempt
               SET lease_expires_at = NOW() + make_interval(secs => $5),
                   updated_at = NOW()
              FROM worker_registry registry
             WHERE attempt.attempt_id = $1
               AND attempt.worker_id = $2
               AND attempt.worker_session_id = $3
               AND attempt.fencing_generation = $4
               AND attempt.worker_id = registry.worker_id
               AND registry.enabled
               AND attempt.capability = ANY(registry.allowed_capabilities)
               AND attempt.status IN ('running', 'cancel_requested')
               AND attempt.lease_expires_at > NOW()
            RETURNING attempt.status::text AS status,
                      attempt.lease_expires_at,
                      attempt.highest_contiguous_reliable_seq,
                      registry.draining
            "#,
        )
        .bind(identity.attempt_id)
        .bind(worker_id)
        .bind(identity.worker_session_id)
        .bind(identity.fencing_generation as i64)
        .bind(duration_seconds(lease_ttl))
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(row) = row else {
            tx.rollback().await.core()?;
            return Ok(None);
        };
        sqlx::query(
            r#"
            UPDATE worker_session
               SET last_heartbeat_at = NOW(),
                   expires_at = NOW() + make_interval(secs => $2),
                   reliable_buffer_len = COALESCE($3, reliable_buffer_len)
             WHERE worker_session_id = $1
            "#,
        )
        .bind(identity.worker_session_id)
        .bind(duration_seconds(lease_ttl * 2))
        .bind(reliable_buffer_len.map(i64::from))
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        let status: String = row.try_get("status").core()?;
        let draining: bool = row
            .try_get("draining")
            .core()?;
        let desired_state = if status == "cancel_requested" {
            DesiredState::Cancel
        } else if draining {
            DesiredState::Drain
        } else {
            DesiredState::Continue
        };
        Ok(Some(AttemptHeartbeat {
            desired_state,
            lease_expires_at: row
                .try_get("lease_expires_at")
                .core()?,
            highest_contiguous_reliable_seq: row
                .try_get::<i64, _>("highest_contiguous_reliable_seq")
                .core()? as u64,
        }))
    }

    async fn request_cancel(&self, job_id: Uuid) -> sbgh_core::Result<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        // Lock in the same attempt→job order as accept/terminal transitions.
        let attempt = sqlx::query(
            "SELECT attempt_id, worker_session_id, status::text AS status
               FROM worker_attempt
              WHERE job_id = $1
                AND status IN ('offered', 'running', 'cancel_requested')
              FOR UPDATE",
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let job_status: Option<String> =
            sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1 FOR UPDATE")
                .bind(job_id)
                .fetch_optional(&mut *tx)
                .await
                .core()?;
        let Some(job_status) = job_status else {
            tx.rollback().await.core()?;
            return Ok(false);
        };
        if job_status == "cancelled" {
            tx.commit().await.core()?;
            return Ok(true);
        }
        if let Some(attempt) = attempt {
            let attempt_id: Uuid = attempt
                .try_get("attempt_id")
                .core()?;
            let session_id: Uuid = attempt
                .try_get("worker_session_id")
                .core()?;
            let status: String = attempt
                .try_get("status")
                .core()?;
            if matches!(status.as_str(), "running" | "cancel_requested") {
                sqlx::query(
                    "UPDATE worker_attempt
                        SET status = 'cancel_requested', updated_at = NOW()
                      WHERE attempt_id = $1",
                )
                .bind(attempt_id)
                .execute(&mut *tx)
                .await
                .core()?;
                tx.commit().await.core()?;
                return Ok(true);
            }

            // An offer has not acquired local resources. Fence it and
            // terminal-cancel the job; a concurrent accept serializes on the
            // same attempt row, so exactly one transition wins.
            discard_attempt_projection(&mut tx, attempt_id, "offered work was cancelled").await?;
            sqlx::query(
                "UPDATE worker_attempt
                    SET status = 'fenced', updated_at = NOW()
                  WHERE attempt_id = $1 AND status = 'offered'",
            )
            .bind(attempt_id)
            .execute(&mut *tx)
            .await
            .core()?;
            sqlx::query(
                "UPDATE worker_session
                    SET status = 'idle', last_heartbeat_at = NOW()
                  WHERE worker_session_id = $1 AND status = 'offered'",
            )
            .bind(session_id)
            .execute(&mut *tx)
            .await
            .core()?;
        }
        let cancelled = sqlx::query(
            "UPDATE job
                SET status = 'cancelled', claim_token = NULL,
                    claimed_at = NULL, updated_at = NOW()
              WHERE id = $1 AND status IN ('queued', 'claimed')",
        )
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .core()?;
        if cancelled.rows_affected() == 1 {
            insert_job_event(
                &mut tx,
                job_id,
                JobEventKind::Cancelled,
                JobEventStatus::Success,
                Some("cancelled before worker execution started"),
                None,
            )
            .await?;
        }
        tx.commit().await.core()?;
        Ok(cancelled.rows_affected() == 1)
    }

    async fn set_all_workers_draining(&self, draining: bool) -> sbgh_core::Result<u64> {
        let result = sqlx::query(
            "UPDATE worker_registry
                SET draining = $1, updated_at = NOW()
              WHERE enabled AND draining IS DISTINCT FROM $1",
        )
        .bind(draining)
        .execute(&self.pool)
        .await
        .core()?;
        Ok(result.rows_affected())
    }

    async fn request_cancel_all(&self) -> sbgh_core::Result<u64> {
        let job_ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id
               FROM job
              WHERE status IN ('queued', 'claimed', 'running')
              ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .core()?;
        let mut cancelled = 0_u64;
        for job_id in job_ids {
            cancelled += u64::from(
                self.request_cancel(job_id)
                    .await?,
            );
        }
        Ok(cancelled)
    }

    async fn recover_submission(
        &self,
        submission_id: Uuid,
        target_worker_id: Option<Uuid>,
        reason: &str,
    ) -> sbgh_core::Result<SubmissionRecovery> {
        if reason.trim().is_empty() || reason.len() > 1_024 {
            return Err(sbgh_core::Error::Config(
                "submission recovery requires a concise operator reason".into(),
            ));
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        if let Some(worker_id) = target_worker_id {
            let compatible: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                      FROM worker_registry
                     WHERE worker_id = $1
                       AND enabled
                       AND NOT draining
                       AND 'benchmark'::worker_capability = ANY(allowed_capabilities)
                )
                "#,
            )
            .bind(worker_id)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            if !compatible {
                return Err(sbgh_core::Error::Config(
                    "recovery target is not an enabled, non-draining benchmark worker".into(),
                ));
            }
        }
        let prior = sqlx::query(
            r#"
            SELECT execution_generation
              FROM task_submission
             WHERE id = $1
               AND EXISTS (
                   SELECT 1 FROM task_spec
                    WHERE task_submission_id = $1
               )
               AND NOT EXISTS (
                   SELECT 1 FROM task_spec
                    WHERE task_submission_id = $1
                      AND task_kind <> 'benchmark'
               )
             FOR UPDATE
            "#,
        )
        .bind(submission_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?
        .ok_or_else(|| {
            sbgh_core::Error::Config(
                "submission does not exist or is not a benchmark comparison generation".into(),
            )
        })?;
        let active: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM worker_attempt
                 WHERE task_submission_id = $1
                   AND status IN ('offered', 'running', 'cancel_requested')
            )
            "#,
        )
        .bind(submission_id)
        .fetch_one(&mut *tx)
        .await
        .core()?;
        if active {
            return Err(sbgh_core::Error::Config(
                "benchmark submission still has active work".into(),
            ));
        }
        let abandoned = sqlx::query(
            r#"
            UPDATE worker_cleanup_obligation obligation
               SET status = 'abandoned', completed_at = NOW()
              FROM job
             WHERE job.id = obligation.job_id
               AND job.task_submission_id = $1
               AND obligation.status = 'pending'
            RETURNING obligation.job_id
            "#,
        )
        .bind(submission_id)
        .fetch_all(&mut *tx)
        .await
        .core()?;
        for row in abandoned {
            let job_id: Uuid = row.try_get("job_id").core()?;
            let cancelled = sqlx::query(
                "UPDATE job SET status = 'cancelled', updated_at = NOW()
                  WHERE id = $1 AND status = 'running'",
            )
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .core()?;
            if cancelled.rows_affected() == 1 {
                insert_job_event(
                    &mut tx,
                    job_id,
                    JobEventKind::Cancelled,
                    JobEventStatus::Success,
                    Some(reason),
                    Some(&serde_json::json!({
                        "recovery": "operator_abandoned_remote_cleanup",
                        "prior_submission_id": submission_id,
                    })),
                )
                .await?;
            }
        }
        let execution_generation = prior
            .try_get::<i64, _>("execution_generation")
            .core()?
            + 1;
        let new_submission_id = Uuid::new_v4();
        let inserted = sqlx::query(
            r#"
            INSERT INTO task_submission
                (id, github_installation_id, github_repo_id, source, intent,
                 artifact_prefix, recovery_of_submission_id,
                 execution_generation, fencing_generation, required_worker_id,
                 required_measurement_profile, contract_version, request_digest)
            SELECT $2, github_installation_id, github_repo_id, source, intent,
                   $2::text, COALESCE(recovery_of_submission_id, id),
                   $3, fencing_generation + 1, $4,
                   required_measurement_profile, contract_version, request_digest
              FROM task_submission
             WHERE id = $1
            "#,
        )
        .bind(submission_id)
        .bind(new_submission_id)
        .bind(execution_generation)
        .bind(target_worker_id)
        .execute(&mut *tx)
        .await
        .core()?;
        if inserted.rows_affected() != 1 {
            return Err(sbgh_core::Error::Config(
                "benchmark submission disappeared during recovery".into(),
            ));
        }
        sqlx::query(
            r#"
            INSERT INTO task_spec
                (task_submission_id, spec_index, requested_run_count,
                 baseline_calibration_id, github_repo_id, task_kind, build_target,
                 git_ref_kind, git_ref_display, git_commit_hash, git_committed_at,
                 workload_key)
            SELECT $2, spec_index, requested_run_count, baseline_calibration_id,
                   github_repo_id, task_kind, build_target, git_ref_kind,
                   git_ref_display, git_commit_hash, git_committed_at, workload_key
              FROM task_spec
             WHERE task_submission_id = $1
             ORDER BY spec_index
            "#,
        )
        .bind(submission_id)
        .bind(new_submission_id)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            INSERT INTO task_workflow_step
                (task_submission_id, step_index, step_kind, task_spec_id)
            SELECT $2, step.step_index, step.step_kind, new_spec.id
              FROM task_workflow_step step
              LEFT JOIN task_spec old_spec
                ON old_spec.id = step.task_spec_id
              LEFT JOIN task_spec new_spec
                ON new_spec.task_submission_id = $2
               AND new_spec.spec_index = old_spec.spec_index
             WHERE step.task_submission_id = $1
             ORDER BY step.step_index
            "#,
        )
        .bind(submission_id)
        .bind(new_submission_id)
        .execute(&mut *tx)
        .await
        .core()?;
        let first_job_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO job
                (task_submission_id, task_spec_id, task_run_index,
                 github_installation_id, github_repo_id, git_ref_kind,
                 git_ref_display, git_commit_hash, git_committed_at, workload_key,
                 source, intent, task_kind, build_target,
                 required_capability, execution_payload_version,
                 execution_payload, execution_payload_hash)
            SELECT $2, new_spec.id, 0, old_job.github_installation_id,
                   old_job.github_repo_id, old_job.git_ref_kind,
                   old_job.git_ref_display, old_job.git_commit_hash,
                   old_job.git_committed_at, old_job.workload_key,
                   old_job.source, old_job.intent, old_job.task_kind,
                   old_job.build_target, old_job.required_capability,
                   old_job.execution_payload_version,
                   old_job.execution_payload, old_job.execution_payload_hash
              FROM job old_job
              JOIN task_spec old_spec
                ON old_spec.id = old_job.task_spec_id
               AND old_spec.task_submission_id = $1
               AND old_spec.spec_index = 0
              JOIN task_spec new_spec
                ON new_spec.task_submission_id = $2
               AND new_spec.spec_index = 0
             WHERE old_job.task_run_index = 0
             ORDER BY old_job.created_at, old_job.id
             LIMIT 1
            RETURNING id
            "#,
        )
        .bind(submission_id)
        .bind(new_submission_id)
        .fetch_one(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            INSERT INTO job_event
                (job_id, event_kind, event_status, remark, detail)
            SELECT $2, event_kind, event_status,
                   CASE WHEN event_kind = 'queued' THEN $3 ELSE remark END,
                   detail
              FROM job_event
             WHERE job_id = (
                 SELECT old_job.id
                   FROM job old_job
                   JOIN task_spec old_spec
                     ON old_spec.id = old_job.task_spec_id
                  WHERE old_spec.task_submission_id = $1
                    AND old_spec.spec_index = 0
                    AND old_job.task_run_index = 0
                  ORDER BY old_job.created_at, old_job.id
                  LIMIT 1
             )
               AND event_kind IN ('queued', 'plan_message_sent')
             ORDER BY occurred_at DESC, id DESC
            "#,
        )
        .bind(submission_id)
        .bind(first_job_id)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            INSERT INTO task_submission_provenance
                (task_submission_id, actor_kind, actor_identity, queued_event_detail)
            SELECT $2, actor_kind, actor_identity, queued_event_detail
              FROM task_submission_provenance
             WHERE task_submission_id = $1
            "#,
        )
        .bind(submission_id)
        .bind(new_submission_id)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            INSERT INTO task_submission_github
                (task_submission_id, github_webhook_id, triggering_user_id,
                 github_pull_request_id, triggering_comment_id)
            SELECT $2, NULL, triggering_user_id,
                   github_pull_request_id, triggering_comment_id
              FROM task_submission_github
             WHERE task_submission_id = $1
            "#,
        )
        .bind(submission_id)
        .bind(new_submission_id)
        .execute(&mut *tx)
        .await
        .core()?;
        // One canonical Slack reporting identity follows the active recovery
        // generation. Prior job events retain the historical message audit.
        sqlx::query(
            "UPDATE task_submission_slack
                SET task_submission_id = $2
              WHERE task_submission_id = $1",
        )
        .bind(submission_id)
        .bind(new_submission_id)
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(SubmissionRecovery {
            prior_submission_id: submission_id,
            new_submission_id,
            first_job_id,
            execution_generation: execution_generation as u64,
        })
    }

    async fn ingest_reliable_event(
        &self,
        worker_id: Uuid,
        event: &ReliableEventEnvelope,
    ) -> sbgh_core::Result<EventIngest> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let attempt = sqlx::query(
            r#"
            SELECT attempt.highest_contiguous_reliable_seq
              FROM worker_attempt attempt
              JOIN worker_registry registry
                ON registry.worker_id = attempt.worker_id
               AND registry.enabled
               AND attempt.capability = ANY(registry.allowed_capabilities)
             WHERE attempt.attempt_id = $1
               AND attempt.worker_id = $2
               AND attempt.worker_session_id = $3
               AND attempt.fencing_generation = $4
               AND attempt.trace_id = $5
               AND attempt.status IN ('running', 'cancel_requested')
               AND attempt.lease_expires_at > NOW()
             FOR UPDATE
            "#,
        )
        .bind(event.identity.attempt_id)
        .bind(worker_id)
        .bind(
            event
                .identity
                .worker_session_id,
        )
        .bind(
            event
                .identity
                .fencing_generation as i64,
        )
        .bind(event.trace_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(attempt) = attempt else {
            tx.rollback().await.core()?;
            return Ok(EventIngest::Stale);
        };
        let payload = serde_json::to_value(&event.payload)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let payload_kind = match event.payload {
            ReliableEventPayload::Phase { .. } => "phase",
            ReliableEventPayload::Terminal { .. } => "terminal",
        };
        let inserted = sqlx::query(
            r#"
            INSERT INTO worker_attempt_event
                (attempt_id, reliable_seq, trace_id, payload_kind, payload,
                 payload_digest, worker_timestamp)
            VALUES (
                $1, $2, $3, $4, $5, $6,
                to_timestamp($7::double precision / 1000.0)
            )
            ON CONFLICT (attempt_id, reliable_seq) DO NOTHING
            "#,
        )
        .bind(event.identity.attempt_id)
        .bind(event.reliable_seq as i64)
        .bind(event.trace_id)
        .bind(payload_kind)
        .bind(&payload)
        .bind(&event.payload_digest)
        .bind(event.worker_timestamp_ms as f64)
        .execute(&mut *tx)
        .await
        .core()?;
        let outcome = if inserted.rows_affected() == 1 {
            EventIngest::Inserted
        } else {
            let existing = sqlx::query(
                r#"
                SELECT payload, payload_digest, trace_id
                  FROM worker_attempt_event
                 WHERE attempt_id = $1 AND reliable_seq = $2
                "#,
            )
            .bind(event.identity.attempt_id)
            .bind(event.reliable_seq as i64)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            let existing_payload: serde_json::Value = existing
                .try_get("payload")
                .core()?;
            let existing_digest: String = existing
                .try_get("payload_digest")
                .core()?;
            let existing_trace: Uuid = existing
                .try_get("trace_id")
                .core()?;
            if existing_payload != payload
                || existing_digest != event.payload_digest
                || existing_trace != event.trace_id
            {
                tx.rollback().await.core()?;
                return Ok(EventIngest::Conflict);
            }
            EventIngest::Duplicate
        };

        let mut contiguous: i64 = attempt
            .try_get("highest_contiguous_reliable_seq")
            .core()?;
        loop {
            let next_exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                      FROM worker_attempt_event
                     WHERE attempt_id = $1 AND reliable_seq = $2
                )
                "#,
            )
            .bind(event.identity.attempt_id)
            .bind(contiguous + 1)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            if !next_exists {
                break;
            }
            contiguous += 1;
        }
        sqlx::query(
            r#"
            UPDATE worker_attempt
               SET highest_contiguous_reliable_seq = $2,
                   updated_at = NOW()
             WHERE attempt_id = $1
            "#,
        )
        .bind(event.identity.attempt_id)
        .bind(contiguous)
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(outcome)
    }

    async fn unprojected_events(&self, limit: u32) -> sbgh_core::Result<Vec<StoredAttemptEvent>> {
        let rows = sqlx::query(
            r#"
            SELECT event.attempt_id,
                   attempt.job_id,
                   event.reliable_seq,
                   event.payload,
                   event.payload_digest
              FROM worker_attempt_event event
              JOIN worker_attempt attempt ON attempt.attempt_id = event.attempt_id
             WHERE event.projected_at IS NULL
               AND event.reliable_seq = attempt.projected_reliable_seq + 1
               AND event.reliable_seq <= attempt.highest_contiguous_reliable_seq
               AND (
                    attempt.status IN ('completed', 'failed', 'cancelled')
                    OR (
                        attempt.status IN ('running', 'cancel_requested')
                        AND attempt.lease_expires_at > NOW()
                    )
               )
             ORDER BY event.received_at, event.attempt_id, event.reliable_seq
             LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .core()?;
        rows.into_iter()
            .map(|row| {
                let payload: serde_json::Value = row
                    .try_get("payload")
                    .core()?;
                Ok(StoredAttemptEvent {
                    attempt_id: row
                        .try_get("attempt_id")
                        .core()?,
                    job_id: row.try_get("job_id").core()?,
                    reliable_seq: row
                        .try_get::<i64, _>("reliable_seq")
                        .core()? as u64,
                    payload: serde_json::from_value(payload)
                        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?,
                    payload_digest: row
                        .try_get("payload_digest")
                        .core()?,
                })
            })
            .collect()
    }

    async fn attempt_projection_is_authoritative(
        &self,
        attempt_id: Uuid,
    ) -> sbgh_core::Result<bool> {
        sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                  FROM worker_attempt
                 WHERE attempt_id = $1
                   AND (
                       status IN ('completed', 'failed', 'cancelled')
                       OR (
                           status IN ('running', 'cancel_requested')
                           AND lease_expires_at > NOW()
                       )
                   )
            )
            "#,
        )
        .bind(attempt_id)
        .fetch_one(&self.pool)
        .await
        .core()
    }

    async fn mark_event_projected(
        &self,
        attempt_id: Uuid,
        reliable_seq: u64,
    ) -> sbgh_core::Result<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let event = sqlx::query(
            r#"
            SELECT attempt.job_id, event.payload
              FROM worker_attempt attempt
              JOIN worker_attempt_event event
                ON event.attempt_id = attempt.attempt_id
               AND event.reliable_seq = $2
             WHERE attempt.attempt_id = $1
               AND attempt.projected_reliable_seq + 1 = $2
               AND attempt.highest_contiguous_reliable_seq >= $2
             FOR UPDATE OF attempt, event
            "#,
        )
        .bind(attempt_id)
        .bind(reliable_seq as i64)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(event) = event else {
            tx.rollback().await.core()?;
            return Ok(false);
        };
        let job_id: Uuid = event
            .try_get("job_id")
            .core()?;
        let payload: serde_json::Value = event
            .try_get("payload")
            .core()?;
        let payload: ReliableEventPayload = serde_json::from_value(payload)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        if let ReliableEventPayload::Phase { label, elapsed_ms } = payload {
            insert_job_event(
                &mut tx,
                job_id,
                JobEventKind::PhaseFinished,
                JobEventStatus::Success,
                Some(&label),
                Some(&serde_json::json!({
                    "attempt_id": attempt_id,
                    "reliable_seq": reliable_seq,
                    "elapsed_ms": elapsed_ms,
                })),
            )
            .await?;
        }
        sqlx::query(
            r#"
            UPDATE worker_attempt
               SET projected_reliable_seq = $2,
                   updated_at = NOW()
             WHERE attempt_id = $1
            "#,
        )
        .bind(attempt_id)
        .bind(reliable_seq as i64)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            UPDATE worker_attempt_event
               SET projected_at = NOW()
             WHERE attempt_id = $1 AND reliable_seq = $2
            "#,
        )
        .bind(attempt_id)
        .bind(reliable_seq as i64)
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(true)
    }

    async fn ingest_progress(
        &self,
        worker_id: Uuid,
        progress: &ProgressRequest,
    ) -> sbgh_core::Result<bool> {
        let update = serde_json::to_value(&progress.update)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let inserted = sqlx::query(
            r#"
            INSERT INTO worker_attempt_progress
                (attempt_id, progress_seq, trace_id, update)
            SELECT attempt_id, $6, trace_id, $7
              FROM worker_attempt
             WHERE attempt_id = $1
               AND worker_id = $2
               AND worker_session_id = $3
               AND fencing_generation = $4
               AND trace_id = $5
               AND status IN ('running', 'cancel_requested')
               AND lease_expires_at > NOW()
               AND EXISTS (
                   SELECT 1
                     FROM worker_registry registry
                    WHERE registry.worker_id = worker_attempt.worker_id
                      AND registry.enabled
                      AND worker_attempt.capability
                          = ANY(registry.allowed_capabilities)
               )
            ON CONFLICT (attempt_id, progress_seq) DO NOTHING
            "#,
        )
        .bind(progress.identity.attempt_id)
        .bind(worker_id)
        .bind(
            progress
                .identity
                .worker_session_id,
        )
        .bind(
            progress
                .identity
                .fencing_generation as i64,
        )
        .bind(progress.trace_id)
        .bind(progress.progress_seq as i64)
        .bind(&update)
        .execute(&self.pool)
        .await
        .core()?;
        if inserted.rows_affected() == 1 {
            return Ok(true);
        }
        let existing: Option<(Uuid, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT trace_id, update
              FROM worker_attempt_progress
             WHERE attempt_id = $1 AND progress_seq = $2
            "#,
        )
        .bind(progress.identity.attempt_id)
        .bind(progress.progress_seq as i64)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        match existing {
            Some((trace_id, existing_update))
                if trace_id == progress.trace_id && existing_update == update =>
            {
                Ok(false)
            }
            Some(_) => Err(sbgh_core::Error::Config(
                "conflicting progress update reused an existing sequence".into(),
            )),
            None => Err(sbgh_core::Error::Config("stale or unauthorized progress update".into())),
        }
    }

    async fn unprojected_progress(&self, limit: u32) -> sbgh_core::Result<Vec<StoredProgress>> {
        let rows = sqlx::query(
            r#"
            SELECT progress.attempt_id, attempt.job_id, progress.progress_seq,
                   progress.update
             FROM worker_attempt_progress progress
              JOIN worker_attempt attempt ON attempt.attempt_id = progress.attempt_id
             WHERE progress.projected_at IS NULL
               AND (
                    attempt.status IN ('completed', 'failed', 'cancelled')
                    OR (
                        attempt.status IN ('running', 'cancel_requested')
                        AND attempt.lease_expires_at > NOW()
                    )
               )
             ORDER BY progress.received_at, progress.attempt_id, progress.progress_seq
             LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .core()?;
        rows.into_iter()
            .map(|row| {
                let value: serde_json::Value = row.try_get("update").core()?;
                Ok(StoredProgress {
                    attempt_id: row
                        .try_get("attempt_id")
                        .core()?,
                    job_id: row.try_get("job_id").core()?,
                    progress_seq: row
                        .try_get::<i64, _>("progress_seq")
                        .core()? as u64,
                    update: serde_json::from_value(value)
                        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?,
                })
            })
            .collect()
    }

    async fn mark_progress_projected(
        &self,
        attempt_id: Uuid,
        progress_seq: u64,
    ) -> sbgh_core::Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE worker_attempt_progress
               SET projected_at = NOW()
             WHERE attempt_id = $1
               AND progress_seq = $2
               AND projected_at IS NULL
            "#,
        )
        .bind(attempt_id)
        .bind(progress_seq as i64)
        .execute(&self.pool)
        .await
        .core()?;
        Ok(result.rows_affected() == 1)
    }

    async fn report_projection_seed(
        &self,
        attempt_id: Uuid,
    ) -> sbgh_core::Result<Option<ReportProjectionSeed>> {
        let row = sqlx::query(
            r#"
            WITH projected AS (
                SELECT event.projected_at,
                       event.reliable_seq AS sequence,
                       'phase'::text AS kind,
                       event.payload
                  FROM worker_attempt_event event
                 WHERE event.attempt_id = $1
                   AND event.projected_at IS NOT NULL
                   AND event.projection_discarded_reason IS NULL
                   AND event.payload_kind = 'phase'
                UNION ALL
                SELECT progress.projected_at,
                       progress.progress_seq AS sequence,
                       'progress'::text AS kind,
                       progress.update AS payload
                  FROM worker_attempt_progress progress
                 WHERE progress.attempt_id = $1
                   AND progress.projected_at IS NOT NULL
                   AND progress.projection_discarded_reason IS NULL
            )
            SELECT kind, payload, COUNT(*) OVER () AS mutation_count
              FROM projected
             ORDER BY projected_at DESC, sequence DESC, kind DESC
             LIMIT 1
            "#,
        )
        .bind(attempt_id)
        .fetch_optional(&self.pool)
        .await
        .core()?;
        row.map(|row| {
            let kind: String = row.try_get("kind").core()?;
            let payload: serde_json::Value = row
                .try_get("payload")
                .core()?;
            let latest = match kind.as_str() {
                "phase" => ProjectedReportMutation::Phase(
                    serde_json::from_value(payload)
                        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?,
                ),
                "progress" => ProjectedReportMutation::Progress(
                    serde_json::from_value(payload)
                        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?,
                ),
                _ => {
                    return Err(sbgh_core::Error::Config(format!(
                        "unknown projected report mutation kind {kind}"
                    )));
                }
            };
            Ok(ReportProjectionSeed {
                mutation_count: row
                    .try_get::<i64, _>("mutation_count")
                    .core()? as u64,
                latest,
            })
        })
        .transpose()
    }

    async fn attempt_terminal_outcome(
        &self,
        attempt_id: Uuid,
    ) -> sbgh_core::Result<Option<TerminalOutcome>> {
        let value: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT terminal_outcome
               FROM worker_attempt
              WHERE attempt_id = $1
                AND status IN ('completed', 'failed', 'cancelled')
                AND NOT EXISTS (
                    SELECT 1
                     FROM worker_artifact_staging staging
                     WHERE staging.attempt_id = worker_attempt.attempt_id
                       AND staging.status = 'verified'
                )",
        )
        .bind(attempt_id)
        .fetch_optional(&self.pool)
        .await
        .core()?
        .flatten();
        value
            .map(|value| {
                serde_json::from_value(value)
                    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
            })
            .transpose()
    }

    async fn accepted_terminal_manifest(
        &self,
        attempt_id: Uuid,
    ) -> sbgh_core::Result<Option<Vec<ArtifactDescriptor>>> {
        let value: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT terminal_artifacts
               FROM worker_attempt
              WHERE attempt_id = $1
                AND status IN ('completed', 'failed', 'cancelled')",
        )
        .bind(attempt_id)
        .fetch_optional(&self.pool)
        .await
        .core()?
        .flatten();
        value
            .map(|value| {
                serde_json::from_value(value)
                    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
            })
            .transpose()
    }

    async fn record_artifact_grant(&self, grant: &ArtifactGrantRecord) -> sbgh_core::Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO worker_artifact_staging
                (attempt_id, object_key, logical_key, status, size_bytes, sha256, grant_expires_at)
            SELECT $1, $2, $3, 'granted', $4, $5, $6
             WHERE EXISTS (
                 SELECT 1 FROM worker_attempt
                  WHERE attempt_id = $1
                    AND status IN ('running', 'cancel_requested')
                    AND lease_expires_at > NOW()
             )
            ON CONFLICT (attempt_id, logical_key) DO UPDATE
                SET grant_expires_at = EXCLUDED.grant_expires_at,
                    updated_at = NOW()
              WHERE worker_artifact_staging.object_key = EXCLUDED.object_key
                AND worker_artifact_staging.size_bytes IS NOT DISTINCT FROM EXCLUDED.size_bytes
                AND worker_artifact_staging.sha256 IS NOT DISTINCT FROM EXCLUDED.sha256
                AND worker_artifact_staging.status = 'granted'
            "#,
        )
        .bind(grant.attempt_id)
        .bind(&grant.object_key)
        .bind(&grant.logical_key)
        .bind(
            grant
                .size
                .map(|value| value as i64),
        )
        .bind(&grant.sha256)
        .bind(grant.expires_at)
        .execute(&self.pool)
        .await
        .core()?;
        Ok(result.rows_affected() == 1)
    }

    async fn verify_artifact(
        &self,
        identity: &AttemptIdentity,
        artifact: &ArtifactDescriptor,
    ) -> sbgh_core::Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE worker_artifact_staging staging
               SET status = CASE
                       WHEN staging.status = 'promoted' THEN staging.status
                       ELSE 'verified'::artifact_staging_status
                   END,
                   size_bytes = $4,
                   sha256 = $5,
                   verified_at = NOW(),
                   updated_at = NOW()
              FROM worker_attempt attempt
             WHERE staging.attempt_id = $1
               AND staging.object_key = $2
               AND staging.attempt_id = attempt.attempt_id
               AND attempt.worker_session_id = $3
               AND attempt.fencing_generation = $6
               AND attempt.status IN (
                   'running', 'cancel_requested', 'completed', 'failed', 'cancelled'
               )
               AND (
                   attempt.status IN ('completed', 'failed', 'cancelled')
                   OR staging.grant_expires_at > NOW()
               )
               AND (staging.size_bytes IS NULL OR staging.size_bytes = $4)
               AND (staging.sha256 IS NULL OR staging.sha256 = $5)
               AND staging.logical_key = $7
            "#,
        )
        .bind(identity.attempt_id)
        .bind(&artifact.key)
        .bind(identity.worker_session_id)
        .bind(artifact.size as i64)
        .bind(&artifact.sha256)
        .bind(identity.fencing_generation as i64)
        .bind(&artifact.logical_key)
        .execute(&self.pool)
        .await
        .core()?;
        Ok(result.rows_affected() == 1)
    }

    async fn accept_terminal(
        &self,
        worker_id: Uuid,
        identity: &AttemptIdentity,
        submission: &FleetTerminalSubmission<'_>,
    ) -> sbgh_core::Result<TerminalAcceptance> {
        let terminal_reliable_seq = submission.reliable_seq;
        let terminal_payload_digest = submission.payload_digest;
        let outcome = submission.outcome;
        let artifacts = submission.artifacts;
        let write = submission.write;
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let attempt = sqlx::query(
            r#"
            SELECT job_id, claim_token, status::text AS status,
                   lease_expires_at > NOW() AS lease_current,
                   highest_contiguous_reliable_seq,
                   terminal_reliable_seq,
                   terminal_payload_digest,
                   terminal_outcome,
                   terminal_artifacts,
                   EXISTS (
                       SELECT 1
                         FROM worker_registry registry
                        WHERE registry.worker_id = attempt.worker_id
                          AND registry.enabled
                          AND attempt.capability
                              = ANY(registry.allowed_capabilities)
                   ) AS authorized
              FROM worker_attempt attempt
             WHERE attempt.attempt_id = $1
               AND attempt.worker_id = $2
               AND attempt.worker_session_id = $3
               AND attempt.fencing_generation = $4
             FOR UPDATE
            "#,
        )
        .bind(identity.attempt_id)
        .bind(worker_id)
        .bind(identity.worker_session_id)
        .bind(identity.fencing_generation as i64)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(attempt) = attempt else {
            tx.rollback().await.core()?;
            return Ok(TerminalAcceptance::Stale);
        };
        let status: String = attempt
            .try_get("status")
            .core()?;
        let outcome_json = serde_json::to_value(outcome)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let artifacts_json = serde_json::to_value(artifacts)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        if matches!(status.as_str(), "completed" | "failed" | "cancelled") {
            let same_seq = attempt
                .try_get::<Option<i64>, _>("terminal_reliable_seq")
                .core()?
                == Some(terminal_reliable_seq as i64);
            let same_digest = attempt
                .try_get::<Option<String>, _>("terminal_payload_digest")
                .core()?
                .as_deref()
                == Some(terminal_payload_digest);
            let same_outcome = attempt
                .try_get::<Option<serde_json::Value>, _>("terminal_outcome")
                .core()?
                == Some(outcome_json);
            let same_artifacts = attempt
                .try_get::<Option<serde_json::Value>, _>("terminal_artifacts")
                .core()?
                == Some(artifacts_json);
            tx.rollback().await.core()?;
            return Ok(if same_seq && same_digest && same_outcome && same_artifacts {
                TerminalAcceptance::Duplicate
            } else {
                TerminalAcceptance::Stale
            });
        }
        if !matches!(status.as_str(), "running" | "cancel_requested") {
            tx.rollback().await.core()?;
            return Ok(TerminalAcceptance::Stale);
        }
        if !attempt
            .try_get::<bool, _>("authorized")
            .core()?
        {
            tx.rollback().await.core()?;
            return Ok(TerminalAcceptance::Stale);
        }
        if !attempt
            .try_get::<bool, _>("lease_current")
            .core()?
        {
            tx.rollback().await.core()?;
            return Ok(TerminalAcceptance::Stale);
        }
        if status == "cancel_requested" && !matches!(outcome, TerminalOutcome::Cancelled { .. }) {
            tx.rollback().await.core()?;
            return Ok(TerminalAcceptance::Stale);
        }
        let contiguous: i64 = attempt
            .try_get("highest_contiguous_reliable_seq")
            .core()?;
        if contiguous < terminal_reliable_seq as i64 {
            return Err(sbgh_core::Error::Config(
                "terminal reliable event is not in the contiguous durable prefix".into(),
            ));
        }
        let terminal_event = sqlx::query(
            r#"
            SELECT payload, payload_digest
              FROM worker_attempt_event
             WHERE attempt_id = $1
               AND reliable_seq = $2
               AND payload_kind = 'terminal'
            "#,
        )
        .bind(identity.attempt_id)
        .bind(terminal_reliable_seq as i64)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(terminal_event) = terminal_event else {
            return Err(sbgh_core::Error::Config(
                "terminal event is missing from durable event ledger".into(),
            ));
        };
        let stored_digest: String = terminal_event
            .try_get("payload_digest")
            .core()?;
        if stored_digest != terminal_payload_digest {
            return Err(sbgh_core::Error::Config(
                "terminal event digest does not match completion".into(),
            ));
        }
        let stored_payload: serde_json::Value = terminal_event
            .try_get("payload")
            .core()?;
        let stored_payload: ReliableEventPayload = serde_json::from_value(stored_payload)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let ReliableEventPayload::Terminal { outcome_digest } = stored_payload else {
            return Err(sbgh_core::Error::Config(
                "terminal event has a non-terminal payload".into(),
            ));
        };
        let submitted_outcome_digest = sbgh_fleet::payload_digest(outcome)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        if outcome_digest != submitted_outcome_digest {
            return Err(sbgh_core::Error::Config(
                "terminal outcome does not match its committed reliable event".into(),
            ));
        }
        for artifact in artifacts {
            let verified: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                      FROM worker_artifact_staging
                     WHERE attempt_id = $1
                       AND object_key = $2
                       AND status = 'verified'
                       AND size_bytes = $3
                       AND sha256 = $4
                       AND logical_key = $5
                )
                "#,
            )
            .bind(identity.attempt_id)
            .bind(&artifact.key)
            .bind(artifact.size as i64)
            .bind(&artifact.sha256)
            .bind(&artifact.logical_key)
            .fetch_one(&mut *tx)
            .await
            .core()?;
            if !verified {
                return Err(sbgh_core::Error::Config(format!(
                    "artifact {} is not verified for this attempt",
                    artifact.key
                )));
            }
        }
        let verified_artifact_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
              FROM worker_artifact_staging
             WHERE attempt_id = $1
               AND status = 'verified'
            "#,
        )
        .bind(identity.attempt_id)
        .fetch_one(&mut *tx)
        .await
        .core()?;
        if verified_artifact_count != artifacts.len() as i64 {
            return Err(sbgh_core::Error::Config(
                "terminal manifest omits a verified attempt artifact".into(),
            ));
        }

        let job_id: Uuid = attempt
            .try_get("job_id")
            .core()?;
        let claim_token: Uuid = attempt
            .try_get("claim_token")
            .core()?;
        let (attempt_status, job_status) = match write {
            FleetTerminalWrite::Completed(_) => ("completed", "completed"),
            FleetTerminalWrite::Failed(_) => ("failed", "failed"),
            FleetTerminalWrite::Cancelled { .. } => ("cancelled", "cancelled"),
        };
        let outcome_matches = matches!(
            (outcome, write),
            (TerminalOutcome::Completed { .. }, FleetTerminalWrite::Completed(_))
                | (TerminalOutcome::Failed { .. }, FleetTerminalWrite::Failed(_))
                | (TerminalOutcome::Cancelled { .. }, FleetTerminalWrite::Cancelled { .. })
        );
        if !outcome_matches {
            return Err(sbgh_core::Error::Config(
                "terminal persistence write does not match protocol outcome".into(),
            ));
        }
        let updated = sqlx::query(
            r#"
            UPDATE job
               SET status = $3::text::job_status, updated_at = NOW()
             WHERE id = $1
               AND claim_token = $2
               AND status = 'running'
            "#,
        )
        .bind(job_id)
        .bind(claim_token)
        .bind(job_status)
        .execute(&mut *tx)
        .await
        .core()?;
        if updated.rows_affected() != 1 {
            tx.rollback().await.core()?;
            return Ok(TerminalAcceptance::Stale);
        }

        match write {
            FleetTerminalWrite::Completed(completion) => {
                persist_completion(&mut tx, job_id, identity.attempt_id, completion).await?;
            }
            FleetTerminalWrite::Failed(failure) => {
                persist_failure(&mut tx, job_id, failure).await?;
            }
            FleetTerminalWrite::Cancelled { remark } => {
                insert_job_event(
                    &mut tx,
                    job_id,
                    JobEventKind::Cancelled,
                    JobEventStatus::Success,
                    Some(remark),
                    None,
                )
                .await?;
            }
        }
        sqlx::query(
            r#"
            UPDATE worker_attempt
               SET status = $2::text::worker_attempt_status,
                   terminal_reliable_seq = $3,
                   terminal_payload_digest = $4,
                   terminal_outcome = $5,
                   terminal_artifacts = $6,
                   terminal_at = NOW(),
                   updated_at = NOW()
             WHERE attempt_id = $1
            "#,
        )
        .bind(identity.attempt_id)
        .bind(attempt_status)
        .bind(terminal_reliable_seq as i64)
        .bind(terminal_payload_digest)
        .bind(outcome_json)
        .bind(artifacts_json)
        .execute(&mut *tx)
        .await
        .core()?;
        sqlx::query(
            r#"
            UPDATE worker_session
               SET status = CASE
                   WHEN EXISTS (
                       SELECT 1 FROM worker_registry
                        WHERE worker_id = $2 AND draining
                   ) THEN 'draining'::worker_session_status
                   ELSE 'idle'::worker_session_status
               END,
               last_heartbeat_at = NOW()
             WHERE worker_session_id = $1
            "#,
        )
        .bind(identity.worker_session_id)
        .bind(worker_id)
        .execute(&mut *tx)
        .await
        .core()?;
        tx.commit().await.core()?;
        Ok(TerminalAcceptance::Accepted)
    }

    async fn pending_artifact_promotions(
        &self,
        limit: u32,
    ) -> sbgh_core::Result<Vec<PendingArtifactPromotion>> {
        let rows = sqlx::query(
            r#"
            SELECT attempt_id, terminal_artifacts
              FROM worker_attempt
             WHERE status IN ('completed', 'failed', 'cancelled')
               AND terminal_artifacts IS NOT NULL
               AND EXISTS (
                   SELECT 1
                     FROM worker_artifact_staging staging
                    WHERE staging.attempt_id = worker_attempt.attempt_id
                      AND staging.status = 'verified'
               )
             ORDER BY terminal_at, attempt_id
             LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .core()?;
        rows.into_iter()
            .map(|row| {
                let artifacts: serde_json::Value = row
                    .try_get("terminal_artifacts")
                    .core()?;
                Ok(PendingArtifactPromotion {
                    attempt_id: row
                        .try_get("attempt_id")
                        .core()?,
                    artifacts: serde_json::from_value(artifacts)
                        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?,
                })
            })
            .collect()
    }

    async fn mark_artifacts_promoted(
        &self,
        attempt_id: Uuid,
        artifacts: &[ArtifactDescriptor],
    ) -> sbgh_core::Result<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let stored: Option<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT terminal_artifacts
              FROM worker_attempt
             WHERE attempt_id = $1
               AND status IN ('completed', 'failed', 'cancelled')
             FOR UPDATE
            "#,
        )
        .bind(attempt_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let submitted = serde_json::to_value(artifacts)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        if stored.as_ref() != Some(&submitted) {
            tx.rollback().await.core()?;
            return Ok(false);
        }
        for artifact in artifacts {
            let result = sqlx::query(
                r#"
                UPDATE worker_artifact_staging
                   SET status = 'promoted',
                       promoted_at = COALESCE(promoted_at, NOW()),
                       updated_at = NOW()
                 WHERE attempt_id = $1
                   AND object_key = $2
                   AND logical_key = $3
                   AND size_bytes = $4
                   AND sha256 = $5
                   AND status IN ('verified', 'promoted')
                "#,
            )
            .bind(attempt_id)
            .bind(&artifact.key)
            .bind(&artifact.logical_key)
            .bind(artifact.size as i64)
            .bind(&artifact.sha256)
            .execute(&mut *tx)
            .await
            .core()?;
            if result.rows_affected() != 1 {
                tx.rollback().await.core()?;
                return Ok(false);
            }
        }
        tx.commit().await.core()?;
        Ok(true)
    }

    async fn expire_stale_attempts(&self) -> sbgh_core::Result<u64> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let rows = sqlx::query(
            r#"
            SELECT attempt_id, job_id, worker_id, worker_session_id,
                   status::text AS status
              FROM worker_attempt
             WHERE status IN ('offered', 'running', 'cancel_requested')
               AND CASE
                   WHEN status = 'offered' THEN offer_expires_at
                   ELSE lease_expires_at
               END <= NOW()
             FOR UPDATE SKIP LOCKED
            "#,
        )
        .fetch_all(&mut *tx)
        .await
        .core()?;
        for row in &rows {
            let attempt_id: Uuid = row
                .try_get("attempt_id")
                .core()?;
            let job_id: Uuid = row.try_get("job_id").core()?;
            let worker_id: Uuid = row
                .try_get("worker_id")
                .core()?;
            let worker_session_id: Uuid = row
                .try_get("worker_session_id")
                .core()?;
            let status: String = row.try_get("status").core()?;
            discard_attempt_projection(
                &mut tx,
                attempt_id,
                "attempt lease expired before report projection",
            )
            .await?;
            sqlx::query(
                "UPDATE worker_attempt
                    SET status = 'expired', updated_at = NOW()
                  WHERE attempt_id = $1",
            )
            .bind(attempt_id)
            .execute(&mut *tx)
            .await
            .core()?;
            if status == "offered" {
                sqlx::query(
                    r#"
                    UPDATE job
                       SET status = 'queued',
                           claim_token = NULL,
                           claimed_at = NULL,
                           updated_at = NOW()
                     WHERE id = $1 AND status = 'claimed'
                    "#,
                )
                .bind(job_id)
                .execute(&mut *tx)
                .await
                .core()?;
            } else {
                sqlx::query(
                    r#"
                    INSERT INTO worker_cleanup_obligation
                        (worker_id, worker_session_id, attempt_id, job_id,
                         requeue_job, reason)
                    VALUES (
                        $1, $2, $3, $4, $5,
                        'lease expired while local execution may exist'
                    )
                    ON CONFLICT (attempt_id) DO NOTHING
                    "#,
                )
                .bind(worker_id)
                .bind(worker_session_id)
                .bind(attempt_id)
                .bind(job_id)
                .bind(status == "running")
                .execute(&mut *tx)
                .await
                .core()?;
            }
            sqlx::query(
                r#"
                UPDATE worker_session
                   SET status = 'offline', ended_at = NOW()
                 WHERE worker_session_id = $1
                   AND status NOT IN ('offline', 'deregistered')
                "#,
            )
            .bind(worker_session_id)
            .execute(&mut *tx)
            .await
            .core()?;
        }
        tx.commit().await.core()?;
        Ok(rows.len() as u64)
    }

    async fn cleanup_obligations(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
    ) -> sbgh_core::Result<Vec<CleanupObligation>> {
        let rows = sqlx::query(
            r#"
            SELECT obligation.id, obligation.worker_id,
                   obligation.worker_session_id, obligation.attempt_id,
                   obligation.job_id, obligation.requeue_job, obligation.reason
              FROM worker_cleanup_obligation obligation
             WHERE obligation.worker_id = $1
               AND obligation.status = 'pending'
               AND EXISTS (
                   SELECT 1
                     FROM worker_session current_session
                    WHERE current_session.worker_id = $1
                      AND current_session.worker_session_id = $2
                      AND current_session.status IN (
                          'registered', 'idle', 'offered', 'running', 'draining'
                      )
                      AND current_session.expires_at > NOW()
               )
             ORDER BY created_at
            "#,
        )
        .bind(worker_id)
        .bind(worker_session_id)
        .fetch_all(&self.pool)
        .await
        .core()?;
        rows.into_iter()
            .map(|row| {
                Ok(CleanupObligation {
                    id: row.try_get("id").core()?,
                    worker_id: row
                        .try_get("worker_id")
                        .core()?,
                    worker_session_id: row
                        .try_get("worker_session_id")
                        .core()?,
                    attempt_id: row
                        .try_get("attempt_id")
                        .core()?,
                    job_id: row.try_get("job_id").core()?,
                    requeue_job: row
                        .try_get("requeue_job")
                        .core()?,
                    reason: row.try_get("reason").core()?,
                })
            })
            .collect()
    }

    async fn complete_cleanup(
        &self,
        worker_id: Uuid,
        worker_session_id: Uuid,
        obligation_id: i64,
    ) -> sbgh_core::Result<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .core()?;
        let row = sqlx::query(
            r#"
            UPDATE worker_cleanup_obligation
               SET status = 'completed', completed_at = NOW()
             WHERE id = $1
               AND worker_id = $2
               AND status = 'pending'
               AND EXISTS (
                   SELECT 1
                     FROM worker_session current_session
                    WHERE current_session.worker_id = $2
                      AND current_session.worker_session_id = $3
                      AND current_session.status IN (
                          'registered', 'idle', 'offered', 'running', 'draining'
                      )
                      AND current_session.expires_at > NOW()
               )
            RETURNING job_id, requeue_job
            "#,
        )
        .bind(obligation_id)
        .bind(worker_id)
        .bind(worker_session_id)
        .fetch_optional(&mut *tx)
        .await
        .core()?;
        let Some(row) = row else {
            tx.rollback().await.core()?;
            return Ok(false);
        };
        let job_id: Uuid = row.try_get("job_id").core()?;
        let requeue_job: bool = row
            .try_get("requeue_job")
            .core()?;
        if requeue_job {
            sqlx::query(
                r#"
                UPDATE job
                   SET status = 'queued',
                       claim_token = NULL,
                       claimed_at = NULL,
                       updated_at = NOW()
                 WHERE id = $1 AND status = 'running'
                "#,
            )
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .core()?;
        } else {
            let cancelled = sqlx::query(
                "UPDATE job SET status = 'cancelled', updated_at = NOW()
                  WHERE id = $1 AND status = 'running'",
            )
            .bind(job_id)
            .execute(&mut *tx)
            .await
            .core()?;
            if cancelled.rows_affected() == 1 {
                insert_job_event(
                    &mut tx,
                    job_id,
                    JobEventKind::Cancelled,
                    JobEventStatus::Success,
                    Some("cancelled attempt was fenced after its lease expired"),
                    None,
                )
                .await?;
            }
        }
        tx.commit().await.core()?;
        Ok(true)
    }

    async fn staged_artifacts_due_for_reap(
        &self,
        older_than: DateTime<Utc>,
    ) -> sbgh_core::Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT staging.object_key
              FROM worker_artifact_staging staging
              JOIN worker_attempt attempt ON attempt.attempt_id = staging.attempt_id
             WHERE staging.attempt_id = attempt.attempt_id
               AND staging.status IN ('granted', 'uploaded', 'verified', 'rejected')
               AND staging.created_at < $1
               AND (
                   attempt.status IN ('expired', 'fenced')
                   OR (
                       attempt.status IN ('completed', 'failed', 'cancelled')
                       AND NOT EXISTS (
                           SELECT 1
                             FROM jsonb_array_elements(
                                      COALESCE(attempt.terminal_artifacts, '[]'::JSONB)
                                  ) terminal_artifact
                            WHERE terminal_artifact->>'key' = staging.object_key
                       )
                   )
               )
             ORDER BY staging.created_at, staging.object_key
             LIMIT 128
            "#,
        )
        .bind(older_than)
        .fetch_all(&self.pool)
        .await
        .core()?;
        rows.into_iter()
            .map(|row| {
                let key: String = row
                    .try_get("object_key")
                    .core()?;
                Ok(key)
            })
            .collect()
    }

    async fn mark_staged_artifact_reaped(&self, object_key: &str) -> sbgh_core::Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE worker_artifact_staging staging
               SET status = 'reaped', updated_at = NOW()
              FROM worker_attempt attempt
             WHERE staging.attempt_id = attempt.attempt_id
               AND staging.object_key = $1
               AND staging.status IN ('granted', 'uploaded', 'verified', 'rejected')
               AND (
                   attempt.status IN ('expired', 'fenced')
                   OR (
                       attempt.status IN ('completed', 'failed', 'cancelled')
                       AND NOT EXISTS (
                           SELECT 1
                             FROM jsonb_array_elements(
                                      COALESCE(attempt.terminal_artifacts, '[]'::JSONB)
                                  ) terminal_artifact
                            WHERE terminal_artifact->>'key' = staging.object_key
                       )
                   )
               )
            "#,
        )
        .bind(object_key)
        .execute(&self.pool)
        .await
        .core()?;
        Ok(result.rows_affected() == 1)
    }

    async fn fleet_snapshot(&self) -> sbgh_core::Result<FleetSnapshot> {
        let row = sqlx::query(
            r#"
            SELECT
                (SELECT count(*) FROM worker_registry WHERE enabled) AS registered_workers,
                (SELECT count(*) FROM worker_session
                  WHERE status IN ('registered', 'idle', 'offered', 'running', 'draining')
                    AND expires_at > NOW()) AS online_workers,
                (SELECT count(*) FROM worker_registry WHERE enabled AND draining)
                    AS draining_workers,
                (SELECT count(*) FROM worker_attempt
                  WHERE status IN ('offered', 'running', 'cancel_requested')) AS active_attempts,
                (SELECT count(*) FROM worker_cleanup_obligation WHERE status = 'pending')
                    AS pending_cleanup,
                (SELECT count(*) FROM worker_attempt
                  WHERE highest_contiguous_reliable_seq > projected_reliable_seq)
                    AS reliable_event_gap_attempts,
                COALESCE((SELECT sum(size_bytes) FROM worker_artifact_staging
                  WHERE status IN ('granted', 'uploaded', 'verified')), 0)::bigint
                    AS staged_artifact_bytes
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .core()?;
        Ok(FleetSnapshot {
            registered_workers: row
                .try_get::<i64, _>("registered_workers")
                .core()? as u64,
            online_workers: row
                .try_get::<i64, _>("online_workers")
                .core()? as u64,
            draining_workers: row
                .try_get::<i64, _>("draining_workers")
                .core()? as u64,
            active_attempts: row
                .try_get::<i64, _>("active_attempts")
                .core()? as u64,
            pending_cleanup: row
                .try_get::<i64, _>("pending_cleanup")
                .core()? as u64,
            reliable_event_gap_attempts: row
                .try_get::<i64, _>("reliable_event_gap_attempts")
                .core()? as u64,
            staged_artifact_bytes: row
                .try_get::<i64, _>("staged_artifact_bytes")
                .core()? as u64,
        })
    }
}

/// Retain stale attempt events for audit while making it explicit that they
/// were intentionally not sent to an external reporting surface.
async fn discard_attempt_projection(
    tx: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
    reason: &str,
) -> sbgh_core::Result<()> {
    sqlx::query(
        r#"
        UPDATE worker_attempt_event
           SET projected_at = COALESCE(projected_at, NOW()),
               projection_discarded_reason = CASE
                   WHEN projected_at IS NULL THEN $2
                   ELSE projection_discarded_reason
               END
         WHERE attempt_id = $1
           AND projected_at IS NULL
        "#,
    )
    .bind(attempt_id)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .core()?;
    sqlx::query(
        r#"
        UPDATE worker_attempt_progress
           SET projected_at = COALESCE(projected_at, NOW()),
               projection_discarded_reason = CASE
                   WHEN projected_at IS NULL THEN $2
                   ELSE projection_discarded_reason
               END
         WHERE attempt_id = $1
           AND projected_at IS NULL
        "#,
    )
    .bind(attempt_id)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .core()?;
    sqlx::query(
        r#"
        UPDATE worker_attempt
           SET projected_reliable_seq = highest_contiguous_reliable_seq,
               updated_at = NOW()
         WHERE attempt_id = $1
        "#,
    )
    .bind(attempt_id)
    .execute(&mut **tx)
    .await
    .core()?;
    Ok(())
}

async fn persist_completion(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    attempt_id: Uuid,
    completion: &FleetCompletion,
) -> sbgh_core::Result<()> {
    let result = &completion.result;
    sqlx::query(
        "INSERT INTO job_result (job_id, run_json, archive_dir)
         VALUES ($1, $2, $3)",
    )
    .bind(job_id)
    .bind(&result.run_json)
    .bind(&result.archive_dir)
    .execute(&mut **tx)
    .await
    .core()?;
    if let Some(metric) = &completion.metric {
        sqlx::query(
            r#"
            INSERT INTO job_metric
                (job_id, envelope_duration_us, replay_duration_us, total_duration_us,
                 setup_duration_us, execution_duration_us, commit_duration_us,
                 clarity_runtime, transactions, read_length, write_length,
                 measured_blocks, warmup_blocks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(job_id)
        .bind(metric.envelope_duration_us)
        .bind(metric.replay_duration_us)
        .bind(metric.total_duration_us)
        .bind(metric.setup_duration_us)
        .bind(metric.execution_duration_us)
        .bind(metric.commit_duration_us)
        .bind(metric.clarity_runtime)
        .bind(metric.transactions)
        .bind(metric.read_length)
        .bind(metric.write_length)
        .bind(metric.measured_blocks)
        .bind(metric.warmup_blocks)
        .execute(&mut **tx)
        .await
        .core()?;
    }
    if let Some(calibration_id) = completion.baseline_calibration_id {
        sqlx::query(
            r#"
            UPDATE task_spec spec
               SET baseline_calibration_id = $2, updated_at = NOW()
              FROM job
             WHERE job.id = $1 AND spec.id = job.task_spec_id
            "#,
        )
        .bind(job_id)
        .bind(calibration_id)
        .execute(&mut **tx)
        .await
        .core()?;
    }
    if let Some(result) = &completion.block_validation {
        persist_block_result(tx, job_id, attempt_id, result, &completion.artifact_manifest).await?;
    }
    insert_job_event(
        tx,
        job_id,
        JobEventKind::Completed,
        JobEventStatus::Success,
        None,
        completion
            .event_detail
            .as_ref(),
    )
    .await
}

async fn persist_failure(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    failure: &FleetFailure,
) -> sbgh_core::Result<()> {
    if let Some(result) = &failure.result {
        sqlx::query(
            "INSERT INTO job_result (job_id, run_json, archive_dir)
             VALUES ($1, $2, $3)",
        )
        .bind(job_id)
        .bind(&result.run_json)
        .bind(&result.archive_dir)
        .execute(&mut **tx)
        .await
        .core()?;
    }
    insert_job_event(
        tx,
        job_id,
        JobEventKind::Failed,
        JobEventStatus::Fail,
        Some(&failure.remark),
        failure.event_detail.as_ref(),
    )
    .await
}

async fn persist_block_result(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    attempt_id: Uuid,
    result: &BlockValidationResult,
    artifacts: &[ArtifactDescriptor],
) -> sbgh_core::Result<()> {
    let invalid_blocks = serde_json::to_value(&result.invalid_blocks)
        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    let artifact_manifest = serde_json::to_value(artifacts)
        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
    sqlx::query(
        r#"
        INSERT INTO block_validation_result
            (job_id, attempt_id, chainstate_origin, observed_start, observed_end,
             valid, checked_blocks, invalid_blocks, artifact_manifest)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(job_id)
    .bind(attempt_id)
    .bind(&result.chainstate_origin)
    .bind(result.observed_range.start as i64)
    .bind(result.observed_range.end as i64)
    .bind(result.valid)
    .bind(result.checked_blocks as i64)
    .bind(invalid_blocks)
    .bind(artifact_manifest)
    .execute(&mut **tx)
    .await
    .core()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_mapping_is_exhaustive_and_round_trips() {
        let values = [
            WorkerCapability::Benchmark,
            WorkerCapability::BuildOnly,
            WorkerCapability::BlockValidation,
        ];
        for value in values {
            assert_eq!(capability(capability_name(value)).unwrap(), value);
        }
    }

    #[test]
    fn effective_capabilities_are_an_intersection() {
        let allowed = ["benchmark".to_owned(), "build_only".to_owned()];
        let advertised =
            BTreeSet::from([WorkerCapability::Benchmark, WorkerCapability::BlockValidation]);
        let effective: Vec<_> = capability_names(advertised.iter())
            .into_iter()
            .filter(|item| allowed.contains(item))
            .collect();
        assert_eq!(effective, vec!["benchmark"]);
    }
}
