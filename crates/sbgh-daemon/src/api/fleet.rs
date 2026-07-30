//! Authenticated operator visibility and deliberate fleet recovery controls.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::header;
use sbgh_api::{
    FleetCancellationResponse, FleetOverview, FleetRecoveryRequest, FleetRecoveryResponse,
    FleetSummaryView, FleetWorkerView, WorkerCertificateRequest, WorkerCertificateView,
    WorkerCreateRequest, WorkerDrainRequest, WorkerPolicyView, WorkerUpdateRequest,
};
use sbgh_core::db::fleet::{
    FleetStore, WorkerPolicyPatch, WorkerRegistration, WorkerRegistryEntry, WorkerRegistryMutation,
    WorkerRegistryStore,
};
use sbgh_fleet::WorkerCapability;
use sqlx::Row;
use uuid::Uuid;

use crate::api::error::ApiErr;
use crate::api::state::ApiState;
use crate::fleet::validate_worker_certificate;

pub async fn overview(State(state): State<ApiState>) -> Result<Json<FleetOverview>, ApiErr> {
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    let summary = store.fleet_snapshot().await?;
    let workers = store
        .workers(None)
        .await?
        .into_iter()
        .map(worker_view)
        .collect();
    Ok(Json(FleetOverview {
        summary: FleetSummaryView {
            registered_workers: summary.registered_workers,
            online_workers: summary.online_workers,
            draining_workers: summary.draining_workers,
            active_attempts: summary.active_attempts,
            pending_cleanup: summary.pending_cleanup,
            reliable_event_gap_attempts: summary.reliable_event_gap_attempts,
            staged_artifact_bytes: summary.staged_artifact_bytes,
        },
        workers,
    }))
}

pub async fn create_worker(
    State(state): State<ApiState>,
    Json(request): Json<WorkerCreateRequest>,
) -> Result<Json<WorkerPolicyView>, ApiErr> {
    let worker_id = request
        .worker_id
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| ApiErr::bad_request("worker_id must be a UUID"))?
        .unwrap_or_else(Uuid::new_v4);
    let display_name = validated_display_name(&request.display_name)?;
    let capabilities = parse_capabilities(&request.capabilities)?;
    let measurement_profile = normalize_measurement_profile(request.measurement_profile)?;
    validate_profile(&capabilities, measurement_profile.as_deref())?;
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    mutation_response(
        store
            .create_worker(&WorkerRegistration {
                worker_id,
                display_name,
                allowed_capabilities: capabilities,
                measurement_profile,
                enabled: false,
                draining: true,
            })
            .await?,
        "worker already exists",
    )?;
    worker_detail(&state, worker_id).await
}

pub async fn show_worker(
    State(state): State<ApiState>,
    Path(worker_id): Path<Uuid>,
) -> Result<Json<WorkerPolicyView>, ApiErr> {
    worker_detail(&state, worker_id).await
}

pub async fn update_worker(
    State(state): State<ApiState>,
    Path(worker_id): Path<Uuid>,
    Json(request): Json<WorkerUpdateRequest>,
) -> Result<Json<WorkerPolicyView>, ApiErr> {
    let display_name = request
        .display_name
        .as_deref()
        .map(validated_display_name)
        .transpose()?;
    let capabilities = request
        .capabilities
        .as_deref()
        .map(parse_capabilities)
        .transpose()?;
    let measurement_profile = request
        .measurement_profile
        .map(normalize_measurement_profile)
        .transpose()?;
    if let Some(capabilities) = capabilities.as_ref() {
        validate_profile(
            capabilities,
            measurement_profile
                .as_ref()
                .and_then(|profile| profile.as_deref()),
        )?;
    }
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    mutation_response(
        store
            .update_worker(
                worker_id,
                &WorkerPolicyPatch {
                    display_name,
                    allowed_capabilities: capabilities,
                    measurement_profile,
                    enabled: request.enabled,
                },
            )
            .await?,
        "worker policy conflicts with current state",
    )?;
    worker_detail(&state, worker_id).await
}

pub async fn authorize_certificate(
    State(state): State<ApiState>,
    Path(worker_id): Path<Uuid>,
    Json(request): Json<WorkerCertificateRequest>,
) -> Result<Json<WorkerPolicyView>, ApiErr> {
    let certificate_sha256 = validate_worker_certificate(
        request
            .certificate_pem
            .as_bytes(),
        worker_id,
        &state.worker_ca_certificate,
    )
    .map_err(|error| ApiErr::bad_request(error.to_string()))?;
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    mutation_response(
        store
            .authorize_certificate(worker_id, certificate_sha256)
            .await?,
        "certificate is already bound or revoked",
    )?;
    tracing::info!(
        %worker_id,
        certificate_sha256 = %hex::encode(certificate_sha256),
        "worker certificate authorized"
    );
    worker_detail(&state, worker_id).await
}

pub async fn revoke_certificate(
    State(state): State<ApiState>,
    Path((worker_id, fingerprint)): Path<(Uuid, String)>,
) -> Result<Json<WorkerPolicyView>, ApiErr> {
    let certificate_sha256 = parse_fingerprint(&fingerprint)?;
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    mutation_response(
        store
            .revoke_certificate(worker_id, certificate_sha256)
            .await?,
        "certificate cannot be revoked in the current worker state",
    )?;
    tracing::info!(
        %worker_id,
        certificate_sha256 = %hex::encode(certificate_sha256),
        "worker certificate revoked"
    );
    worker_detail(&state, worker_id).await
}

pub async fn emergency_disable_worker(
    State(state): State<ApiState>,
    Path(worker_id): Path<Uuid>,
) -> Result<Json<WorkerPolicyView>, ApiErr> {
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    mutation_response(
        store
            .emergency_disable_worker(worker_id)
            .await?,
        "worker cannot be disabled",
    )?;
    tracing::warn!(%worker_id, "worker authorization withdrawn and leases expired");
    worker_detail(&state, worker_id).await
}

pub async fn emergency_revoke_certificate(
    State(state): State<ApiState>,
    Path((worker_id, fingerprint)): Path<(Uuid, String)>,
) -> Result<Json<WorkerPolicyView>, ApiErr> {
    let certificate_sha256 = parse_fingerprint(&fingerprint)?;
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    mutation_response(
        store
            .emergency_revoke_certificate(worker_id, certificate_sha256)
            .await?,
        "certificate cannot be revoked",
    )?;
    tracing::warn!(
        %worker_id,
        certificate_sha256 = %hex::encode(certificate_sha256),
        "worker certificate revoked and authenticated leases expired"
    );
    worker_detail(&state, worker_id).await
}

async fn worker_detail(
    state: &ApiState,
    worker_id: Uuid,
) -> Result<Json<WorkerPolicyView>, ApiErr> {
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    let worker = store
        .workers(Some(worker_id))
        .await?
        .into_iter()
        .next()
        .map(worker_view)
        .ok_or_else(|| ApiErr::not_found("worker not found"))?;
    let certificates = store
        .worker_certificates(worker_id)
        .await?
        .into_iter()
        .map(|record| WorkerCertificateView {
            certificate_sha256: hex::encode(record.certificate_sha256),
            created_at: record.created_at.to_rfc3339(),
            revoked_at: record
                .revoked_at
                .map(|value| value.to_rfc3339()),
        })
        .collect();
    Ok(Json(WorkerPolicyView { worker, certificates }))
}

fn worker_view(record: WorkerRegistryEntry) -> FleetWorkerView {
    FleetWorkerView {
        worker_id: record.worker_id.to_string(),
        display_name: record.display_name,
        enabled: record.enabled,
        draining: record.draining,
        capabilities: record.allowed_capabilities,
        measurement_profile: record.measurement_profile,
        worker_session_id: record
            .worker_session_id
            .map(|id| id.to_string()),
        session_status: record.session_status,
        software_version: record.software_version,
        last_heartbeat_at: record
            .last_heartbeat_at
            .map(|value| value.to_rfc3339()),
        session_expires_at: record
            .session_expires_at
            .map(|value| value.to_rfc3339()),
        resource_facts: record.resource_facts,
        attempt_id: record
            .attempt_id
            .map(|id| id.to_string()),
        job_id: record
            .job_id
            .map(|id| id.to_string()),
        trace_id: record
            .trace_id
            .map(|id| id.to_string()),
    }
}

fn mutation_response(
    mutation: WorkerRegistryMutation,
    conflict: &'static str,
) -> Result<(), ApiErr> {
    match mutation {
        WorkerRegistryMutation::Applied | WorkerRegistryMutation::Unchanged => Ok(()),
        WorkerRegistryMutation::NotFound => {
            Err(ApiErr::not_found("worker or certificate not found"))
        }
        WorkerRegistryMutation::Busy => {
            Err(ApiErr::conflict("worker must be drained and quiescent"))
        }
        WorkerRegistryMutation::MissingCertificate => {
            Err(ApiErr::conflict("worker has no active certificate"))
        }
        WorkerRegistryMutation::Conflict => Err(ApiErr::conflict(conflict)),
    }
}

fn parse_capabilities(values: &[String]) -> Result<Vec<WorkerCapability>, ApiErr> {
    if values.is_empty() {
        return Err(ApiErr::bad_request("at least one capability is required"));
    }
    let mut capabilities = values
        .iter()
        .map(|value| match value.as_str() {
            "benchmark" => Ok(WorkerCapability::Benchmark),
            "build_only" => Ok(WorkerCapability::BuildOnly),
            "block_validation" => Ok(WorkerCapability::BlockValidation),
            _ => Err(ApiErr::bad_request(format!("unknown capability `{value}`"))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    capabilities.sort();
    capabilities.dedup();
    Ok(capabilities)
}

fn validate_profile(
    capabilities: &[WorkerCapability],
    measurement_profile: Option<&str>,
) -> Result<(), ApiErr> {
    if capabilities.contains(&WorkerCapability::Benchmark)
        && !measurement_profile.is_some_and(|profile| !profile.trim().is_empty())
    {
        return Err(ApiErr::bad_request("benchmark capability requires measurement_profile"));
    }
    if measurement_profile.is_some_and(|profile| profile.is_empty() || profile.len() > 128) {
        return Err(ApiErr::bad_request("measurement_profile must contain 1..=128 bytes"));
    }
    Ok(())
}

fn normalize_measurement_profile(value: Option<String>) -> Result<Option<String>, ApiErr> {
    value
        .map(|profile| {
            let profile = profile.trim();
            if profile.is_empty() || profile.len() > 128 {
                return Err(ApiErr::bad_request("measurement_profile must contain 1..=128 bytes"));
            }
            Ok(profile.to_owned())
        })
        .transpose()
}

fn validated_display_name(value: &str) -> Result<String, ApiErr> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        return Err(ApiErr::bad_request("display_name must contain 1..=128 bytes"));
    }
    Ok(value.to_owned())
}

fn parse_fingerprint(value: &str) -> Result<[u8; 32], ApiErr> {
    let bytes = hex::decode(value).map_err(|_| {
        ApiErr::bad_request("certificate fingerprint must be lowercase SHA-256 hex")
    })?;
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| byte.is_ascii_uppercase())
        || bytes.len() != 32
    {
        return Err(ApiErr::bad_request("certificate fingerprint must be lowercase SHA-256 hex"));
    }
    bytes
        .try_into()
        .map_err(|_| ApiErr::bad_request("certificate fingerprint must be 32 bytes"))
}

/// Prometheus text exposition for the operational signals pinned by v25.
/// This remains behind the read-scoped operator API authentication.
pub async fn metrics(
    State(state): State<ApiState>,
) -> Result<([(header::HeaderName, &'static str); 1], String), ApiErr> {
    use std::fmt::Write as _;

    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    let summary = store.fleet_snapshot().await?;
    let aggregate = sqlx::query(
        r#"
        SELECT
            COALESCE((
                SELECT EXTRACT(EPOCH FROM MAX(NOW() - created_at))::double precision
                  FROM job
                 WHERE status = 'queued' AND execution_payload IS NOT NULL
            ), 0)
                AS max_scheduling_wait_seconds,
            COALESCE((
                SELECT EXTRACT(EPOCH FROM MAX(NOW() - created_at))::double precision
                  FROM job
                 WHERE status = 'queued' AND execution_payload IS NULL
            ), 0)
                AS max_preparation_wait_seconds,
            COALESCE((
                SELECT MAX(highest_contiguous_reliable_seq - projected_reliable_seq)
                  FROM worker_attempt
            ), 0)
                AS max_reliable_ack_lag,
            COALESCE((
                SELECT EXTRACT(EPOCH FROM MAX(NOW() - created_at))::double precision
                  FROM worker_artifact_staging
                 WHERE status IN ('granted', 'uploaded', 'verified')
            ), 0)
                AS max_staging_age_seconds
        "#,
    )
    .fetch_one(&state.pool)
    .await?;
    let workers = sqlx::query(
        r#"
        SELECT registry.worker_id, registry.display_name,
               EXTRACT(EPOCH FROM NOW() - session.last_heartbeat_at)::double precision
                   AS heartbeat_age_seconds,
               EXTRACT(EPOCH FROM session.expires_at - NOW())::double precision
                   AS session_remaining_seconds,
               session.reliable_buffer_len,
               EXTRACT(EPOCH FROM attempt.lease_expires_at - NOW())::double precision
                   AS attempt_lease_remaining_seconds
          FROM worker_registry registry
          LEFT JOIN LATERAL (
              SELECT last_heartbeat_at, expires_at, reliable_buffer_len
                FROM worker_session
               WHERE worker_id = registry.worker_id
               ORDER BY started_at DESC
               LIMIT 1
          ) session ON TRUE
          LEFT JOIN LATERAL (
              SELECT lease_expires_at
                FROM worker_attempt
               WHERE worker_id = registry.worker_id
                 AND status IN ('offered', 'running', 'cancel_requested')
               ORDER BY created_at DESC
               LIMIT 1
          ) attempt ON TRUE
         WHERE registry.enabled
         ORDER BY registry.worker_id
        "#,
    )
    .fetch_all(&state.pool)
    .await?;

    let mut body = String::new();
    for (name, value) in [
        ("sbgh_fleet_registered_workers", summary.registered_workers),
        ("sbgh_fleet_online_workers", summary.online_workers),
        ("sbgh_fleet_draining_workers", summary.draining_workers),
        ("sbgh_fleet_active_attempts", summary.active_attempts),
        ("sbgh_fleet_pending_cleanup", summary.pending_cleanup),
        ("sbgh_fleet_reliable_event_gap_attempts", summary.reliable_event_gap_attempts),
        ("sbgh_fleet_staged_artifact_bytes", summary.staged_artifact_bytes),
    ] {
        writeln!(body, "# TYPE {name} gauge").unwrap();
        writeln!(body, "{name} {value}").unwrap();
    }
    for (name, value) in [
        (
            "sbgh_fleet_max_scheduling_wait_seconds",
            aggregate.get::<f64, _>("max_scheduling_wait_seconds"),
        ),
        (
            "sbgh_fleet_max_preparation_wait_seconds",
            aggregate.get::<f64, _>("max_preparation_wait_seconds"),
        ),
        ("sbgh_fleet_max_reliable_ack_lag", aggregate.get::<i64, _>("max_reliable_ack_lag") as f64),
        ("sbgh_fleet_max_staging_age_seconds", aggregate.get::<f64, _>("max_staging_age_seconds")),
    ] {
        writeln!(body, "# TYPE {name} gauge").unwrap();
        writeln!(body, "{name} {value}").unwrap();
    }
    for worker in workers {
        let worker_id = worker.get::<Uuid, _>("worker_id");
        let display = prometheus_label(&worker.get::<String, _>("display_name"));
        let labels = format!("worker_id=\"{worker_id}\",worker=\"{display}\"");
        for (name, value) in [
            (
                "sbgh_worker_heartbeat_age_seconds",
                worker
                    .get::<Option<f64>, _>("heartbeat_age_seconds")
                    .unwrap_or(f64::NAN),
            ),
            (
                "sbgh_worker_session_remaining_seconds",
                worker
                    .get::<Option<f64>, _>("session_remaining_seconds")
                    .unwrap_or(f64::NAN),
            ),
            (
                "sbgh_worker_attempt_lease_remaining_seconds",
                worker
                    .get::<Option<f64>, _>("attempt_lease_remaining_seconds")
                    .unwrap_or(f64::NAN),
            ),
            (
                "sbgh_worker_reliable_resend_buffer",
                worker
                    .get::<Option<i32>, _>("reliable_buffer_len")
                    .map(f64::from)
                    .unwrap_or(0.0),
            ),
        ] {
            writeln!(body, "{name}{{{labels}}} {value}").unwrap();
        }
    }
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body))
}

fn prometheus_label(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\n")
}

pub async fn set_drain(
    State(state): State<ApiState>,
    Path(worker_id): Path<Uuid>,
    Json(request): Json<WorkerDrainRequest>,
) -> Result<Json<FleetWorkerView>, ApiErr> {
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    mutation_response(
        store
            .set_worker_draining(worker_id, request.draining)
            .await?,
        "worker drain state conflicts",
    )?;
    let detail = worker_detail(&state, worker_id)
        .await?
        .0;
    Ok(Json(detail.worker))
}

pub async fn recover_submission(
    State(state): State<ApiState>,
    Path(submission_id): Path<Uuid>,
    Json(request): Json<FleetRecoveryRequest>,
) -> Result<Json<FleetRecoveryResponse>, ApiErr> {
    let target_worker_id = request
        .worker_id
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| ApiErr::bad_request("worker_id must be a UUID"))?;
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool);
    let recovery = store
        .recover_submission(submission_id, target_worker_id, request.reason.trim())
        .await?;
    Ok(Json(FleetRecoveryResponse {
        prior_submission_id: recovery
            .prior_submission_id
            .to_string(),
        new_submission_id: recovery
            .new_submission_id
            .to_string(),
        first_job_id: recovery
            .first_job_id
            .to_string(),
        execution_generation: recovery.execution_generation,
    }))
}

pub async fn cancel_job(
    State(state): State<ApiState>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<FleetCancellationResponse>, ApiErr> {
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool);
    let cancel_requested = store
        .request_cancel(job_id)
        .await?;
    Ok(Json(FleetCancellationResponse {
        job_id: job_id.to_string(),
        cancel_requested,
    }))
}
