//! Authenticated operator visibility and deliberate fleet recovery controls.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::header;
use sbgh_api::{
    FleetCancellationResponse, FleetOverview, FleetRecoveryRequest, FleetRecoveryResponse,
    FleetSummaryView, FleetWorkerView, WorkerDrainRequest,
};
use sbgh_core::db::fleet::FleetStore;
use sqlx::Row;
use uuid::Uuid;

use crate::api::error::ApiErr;
use crate::api::state::ApiState;

pub async fn overview(State(state): State<ApiState>) -> Result<Json<FleetOverview>, ApiErr> {
    let store = sbgh_postgres::PostgresFleetStore::new(state.pool.clone());
    let summary = store.fleet_snapshot().await?;
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
         ORDER BY registry.display_name, registry.worker_id
        "#,
    )
    .fetch_all(&state.pool)
    .await?;
    let workers = rows
        .into_iter()
        .map(|row| FleetWorkerView {
            worker_id: row
                .get::<Uuid, _>("worker_id")
                .to_string(),
            display_name: row.get("display_name"),
            enabled: row.get("enabled"),
            draining: row.get("draining"),
            capabilities: row.get("capabilities"),
            measurement_profile: row.get("measurement_profile"),
            worker_session_id: row
                .get::<Option<Uuid>, _>("worker_session_id")
                .map(|id| id.to_string()),
            session_status: row.get("session_status"),
            software_version: row.get("software_version"),
            last_heartbeat_at: row
                .get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_heartbeat_at")
                .map(|value| value.to_rfc3339()),
            session_expires_at: row
                .get::<Option<chrono::DateTime<chrono::Utc>>, _>("expires_at")
                .map(|value| value.to_rfc3339()),
            resource_facts: row.get("resource_facts"),
            attempt_id: row
                .get::<Option<Uuid>, _>("attempt_id")
                .map(|id| id.to_string()),
            job_id: row
                .get::<Option<Uuid>, _>("job_id")
                .map(|id| id.to_string()),
            trace_id: row
                .get::<Option<Uuid>, _>("trace_id")
                .map(|id| id.to_string()),
        })
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
    let mut tx = state.pool.begin().await?;
    let changed = sqlx::query(
        "UPDATE worker_registry
            SET draining = $2, updated_at = NOW()
          WHERE worker_id = $1
        RETURNING display_name",
    )
    .bind(worker_id)
    .bind(request.draining)
    .fetch_optional(&mut *tx)
    .await?;
    if changed.is_none() {
        return Err(ApiErr::not_found("worker not found"));
    }
    if !request.draining {
        sqlx::query(
            "UPDATE worker_session
                SET status = 'idle'
              WHERE worker_id = $1
                AND status = 'draining'
                AND expires_at > NOW()",
        )
        .bind(worker_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    let overview = overview(State(state))
        .await?
        .0;
    let worker = overview
        .workers
        .into_iter()
        .find(|worker| worker.worker_id == worker_id.to_string())
        .ok_or_else(|| ApiErr::not_found("worker not found"))?;
    Ok(Json(worker))
}

pub async fn recover_group(
    State(state): State<ApiState>,
    Path(group_id): Path<Uuid>,
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
        .recover_group(group_id, target_worker_id, request.reason.trim())
        .await?;
    Ok(Json(FleetRecoveryResponse {
        prior_group_id: recovery
            .prior_group_id
            .to_string(),
        new_group_id: recovery
            .new_group_id
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
