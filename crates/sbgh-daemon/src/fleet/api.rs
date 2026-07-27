use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration as StdDuration;

use anyhow::Context;
use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{Duration, Utc};
use sbgh_core::db::fleet::{
    ArtifactGrantRecord, EventIngest, FleetCompletion, FleetFailure, FleetStore,
    FleetTerminalSubmission, FleetTerminalWrite, TerminalAcceptance, WorkerRegistration,
};
use sbgh_core::models::JobResult;
use sbgh_github::InstallationTokenCache;
use sbgh_proto::{
    AcceptOfferRequest, AcceptOfferResponse, ApiError, ArtifactGrantRequest, ArtifactGrantResponse,
    ArtifactOperation, Assignment, AssignmentContext, CleanupCompleteRequest, CleanupItem,
    CleanupListRequest, CompleteAttemptRequest, CompleteAttemptResponse, DeregisterSessionRequest,
    HeartbeatRequest, HeartbeatResponse, MAX_REQUEST_BYTES, PROTOCOL_VERSION, PollRequest,
    PollResponse, ProgressRequest, RegisterSessionRequest, RegisterSessionResponse,
    ReliableEventAck, ReliableEventEnvelope, RepositoryCredentialRequest,
    RepositoryCredentialResponse, RepositoryToken, Validate,
};
use tokio_util::sync::CancellationToken;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use uuid::Uuid;

use super::config::FleetConfig;
use super::lease::LeaseSigner;
use super::tls::{AuthenticatedPeer, MtlsListener};
use crate::artifact_store::ArtifactStore;

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;
const MIN_CONCURRENT_REQUESTS: usize = 64;
const MAX_CONCURRENT_REQUESTS: usize = 1_024;

#[derive(Default)]
struct ActivePolls {
    sessions: StdMutex<HashSet<(Uuid, Uuid)>>,
}

impl ActivePolls {
    fn enter(self: &Arc<Self>, worker_id: Uuid, session_id: Uuid) -> Option<ActivePollGuard> {
        let identity = (worker_id, session_id);
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(identity)
            .then(|| ActivePollGuard { active: self.clone(), identity })
    }
}

struct ActivePollGuard {
    active: Arc<ActivePolls>,
    identity: (Uuid, Uuid),
}

impl Drop for ActivePollGuard {
    fn drop(&mut self) {
        self.active
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.identity);
    }
}

#[derive(Clone)]
struct ApiState {
    store: Arc<dyn FleetStore>,
    artifacts: Arc<dyn ArtifactStore>,
    github_tokens: InstallationTokenCache,
    signer: LeaseSigner,
    config: Arc<FleetConfig>,
    active_polls: Arc<ActivePolls>,
}

pub struct FleetRuntime {
    state: ApiState,
}

impl FleetRuntime {
    pub async fn build(
        config: FleetConfig,
        store: Arc<dyn FleetStore>,
        artifacts: Arc<dyn ArtifactStore>,
        github_tokens: InstallationTokenCache,
    ) -> anyhow::Result<Self> {
        let signer = LeaseSigner::load(&config.lease_hmac_key)?;
        for worker in &config.workers {
            store
                .upsert_worker(&WorkerRegistration {
                    worker_id: worker.id,
                    display_name: worker.display_name.clone(),
                    allowed_capabilities: worker
                        .capabilities
                        .iter()
                        .copied()
                        .collect(),
                    measurement_profile: worker
                        .measurement_profile
                        .clone(),
                    enabled: worker.enabled,
                    draining: worker.draining,
                })
                .await?;
        }
        store
            .disable_workers_except(
                &config
                    .workers
                    .iter()
                    .map(|worker| worker.id)
                    .collect::<Vec<_>>(),
            )
            .await?;
        for dataset in &config.datasets {
            store
                .record_dataset(dataset.worker_id, &dataset.identity, Utc::now(), dataset.current)
                .await?;
        }
        Ok(Self {
            state: ApiState {
                store,
                artifacts,
                github_tokens,
                signer,
                config: Arc::new(config),
                active_polls: Arc::new(ActivePolls::default()),
            },
        })
    }
}

pub async fn run(runtime: FleetRuntime, shutdown: CancellationToken) -> anyhow::Result<()> {
    let config = runtime.state.config.clone();
    let request_limit = config
        .workers
        .len()
        .saturating_mul(2)
        .clamp(MIN_CONCURRENT_REQUESTS, MAX_CONCURRENT_REQUESTS);
    let listener = MtlsListener::bind(
        config.listen,
        &config.server_certificate,
        &config.server_private_key,
        &config.client_ca_certificate,
    )
    .await?;
    let router = Router::new()
        .route("/v1/register", post(register))
        .route("/v1/poll", post(poll))
        .route("/v1/accept", post(accept))
        .route("/v1/repository-credential", post(repository_credential))
        .route("/v1/heartbeat", post(heartbeat))
        .route("/v1/events", post(event))
        .route("/v1/progress", post(progress))
        .route("/v1/artifacts/grant", post(artifact_grant))
        .route("/v1/complete", post(complete))
        .route("/v1/cleanup", get(cleanup).post(cleanup_complete))
        .route("/v1/deregister", post(deregister))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(GlobalConcurrencyLimitLayer::new(request_limit))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            config.request_timeout(),
        ))
        .with_state(runtime.state.clone());

    let maintenance_state = runtime.state.clone();
    let maintenance_shutdown = shutdown.clone();
    let maintenance = tokio::spawn(async move {
        maintenance_loop(maintenance_state, maintenance_shutdown).await;
    });
    tracing::info!(listen = %config.listen, "worker fleet mTLS listener started");
    let serving =
        axum::serve(listener, router.into_make_service_with_connect_info::<AuthenticatedPeer>())
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await;
    maintenance.abort();
    serving.map_err(anyhow::Error::from)
}

async fn register(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<RegisterSessionRequest>,
) -> ApiResult<RegisterSessionResponse> {
    if request.worker_id != peer.worker_id {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "worker_identity_mismatch",
            "certificate identity does not match the requested worker",
            false,
        ));
    }
    let configured = authorized_worker(&state, &peer)?;
    validate(&request)?;
    if request
        .advertised_capabilities
        .is_disjoint(&configured.capabilities)
    {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "capability_not_authorized",
            "worker advertises no server-authorized capability",
            false,
        ));
    }
    match &request.resources.dataset {
        Some(dataset)
            if !configured
                .capabilities
                .contains(&sbgh_proto::WorkerCapability::BlockValidation)
                || !state
                    .config
                    .datasets
                    .iter()
                    .any(|registered| {
                        registered.worker_id == peer.worker_id && registered.identity == *dataset
                    }) =>
        {
            return Err(api_error(
                StatusCode::FORBIDDEN,
                "dataset_not_authorized",
                "dataset telemetry does not exactly match server-owned registry policy",
                false,
            ));
        }
        None if request
            .advertised_capabilities
            .contains(&sbgh_proto::WorkerCapability::BlockValidation) =>
        {
            return Err(api_error(
                StatusCode::FORBIDDEN,
                "dataset_required",
                "block_validation registration requires a configured dataset identity",
                false,
            ));
        }
        _ => {}
    }
    let session = state
        .store
        .register_session(peer.worker_id, &request, chrono_duration(state.config.session_ttl())?)
        .await
        .map_err(internal)?;
    tracing::info!(
        worker_id = %peer.worker_id,
        session_id = %session.worker_session_id,
        peer = %peer.socket_addr,
        capabilities = ?session.effective_capabilities,
        "worker session registered"
    );
    Ok(Json(RegisterSessionResponse {
        protocol_version: PROTOCOL_VERSION,
        heartbeat_interval_ms: state
            .config
            .heartbeat_interval()
            .as_millis() as u64,
        lease_ttl_ms: state
            .config
            .lease_ttl()
            .as_millis() as u64,
        server_time_ms: Utc::now().timestamp_millis(),
    }))
}

async fn poll(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<PollRequest>,
) -> ApiResult<PollResponse> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    if !state
        .store
        .session_is_active(peer.worker_id, request.worker_session_id)
        .await
        .map_err(internal)?
    {
        return Err(api_error(
            StatusCode::CONFLICT,
            "stale_session",
            "worker session is not active",
            false,
        ));
    }
    let _poll_guard = state
        .active_polls
        .enter(peer.worker_id, request.worker_session_id)
        .ok_or_else(|| {
            api_error(
                StatusCode::CONFLICT,
                "poll_already_active",
                "this worker session already has an active long poll",
                true,
            )
        })?;
    let deadline = tokio::time::Instant::now()
        + state
            .config
            .long_poll_timeout();
    loop {
        if let Some(mut offered) = state
            .store
            .poll_offer(
                peer.worker_id,
                request.worker_session_id,
                chrono_duration(state.config.offer_ttl())?,
                chrono_duration(state.config.lease_ttl())?,
            )
            .await
            .map_err(internal)?
        {
            offered
                .offer
                .identity
                .lease_token = state.signer.issue(
                peer.worker_id,
                offered
                    .offer
                    .identity
                    .worker_session_id,
                offered
                    .offer
                    .identity
                    .attempt_id,
                offered
                    .offer
                    .identity
                    .fencing_generation,
            );
            tracing::info!(
                worker_id = %peer.worker_id,
                attempt_id = %offered.offer.identity.attempt_id,
                job_id = %offered.offer.job_id,
                trace_id = %offered.offer.trace_id,
                "work offered"
            );
            return Ok(Json(PollResponse::Offer { offer: offered.offer }));
        }
        if state
            .store
            .session_is_draining(peer.worker_id, request.worker_session_id)
            .await
            .map_err(internal)?
        {
            return Ok(Json(PollResponse::Drain));
        }
        if !state
            .store
            .session_is_active(peer.worker_id, request.worker_session_id)
            .await
            .map_err(internal)?
        {
            return Err(api_error(
                StatusCode::FORBIDDEN,
                "worker_session_inactive",
                "worker session is expired, disabled, or superseded",
                false,
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(Json(PollResponse::NoWork { retry_after_ms: 1_000 }));
        }
        tokio::time::sleep(StdDuration::from_millis(250)).await;
    }
}

async fn accept(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<AcceptOfferRequest>,
) -> ApiResult<AcceptOfferResponse> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    authorize_attempt(&state, peer.worker_id, &request.identity)?;
    let offered = state
        .store
        .offered_assignment(peer.worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    let accepted = state
        .store
        .accept_offer(peer.worker_id, &request.identity, chrono_duration(state.config.lease_ttl())?)
        .await
        .map_err(internal)?;
    if !accepted {
        return Err(stale_attempt());
    }
    let assignment = Assignment {
        identity: request.identity,
        trace_id: offered.offer.trace_id,
        context: AssignmentContext {
            job_id: offered.offer.job_id,
            repository: offered.context_repository,
            commit: offered.context_commit,
        },
        payload: offered.payload,
        payload_hash: offered.offer.payload_hash,
        vcpu_cpuset: offered.vcpu_cpuset,
    };
    assignment
        .validate()
        .map_err(protocol_error)?;
    Ok(Json(AcceptOfferResponse { assignment }))
}

async fn repository_credential(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<RepositoryCredentialRequest>,
) -> ApiResult<RepositoryCredentialResponse> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    authorize_attempt(&state, peer.worker_id, &request.identity)?;
    let heartbeat = state
        .store
        .heartbeat_attempt(
            peer.worker_id,
            &request.identity,
            chrono_duration(state.config.lease_ttl())?,
            None,
        )
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    if heartbeat.desired_state == sbgh_proto::DesiredState::Cancel {
        return Err(api_error(
            StatusCode::CONFLICT,
            "attempt_cancelled",
            "repository credentials are unavailable after cancellation is committed",
            false,
        ));
    }
    let offered = state
        .store
        .offered_assignment(peer.worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    let token = state
        .github_tokens
        .mint_repository_read_token(offered.installation_id, offered.github_repo_id)
        .await
        .map_err(|error| {
            tracing::warn!(
                attempt_id = %request.identity.attempt_id,
                %error,
                "repository token mint failed"
            );
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "repository_token_unavailable",
                "repository credential is temporarily unavailable",
                true,
            )
        })?;
    Ok(Json(RepositoryCredentialResponse {
        token: RepositoryToken(token.token),
        expires_at_ms: token
            .expires_at
            .timestamp_millis(),
    }))
}

async fn heartbeat(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<HeartbeatRequest>,
) -> ApiResult<HeartbeatResponse> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    authorize_attempt(&state, peer.worker_id, &request.identity)?;
    let heartbeat = state
        .store
        .heartbeat_attempt(
            peer.worker_id,
            &request.identity,
            chrono_duration(state.config.lease_ttl())?,
            Some(request.reliable_buffer_len),
        )
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    Ok(Json(HeartbeatResponse {
        desired_state: heartbeat.desired_state,
        lease_expires_at_ms: heartbeat
            .lease_expires_at
            .timestamp_millis(),
        highest_contiguous_reliable_seq: heartbeat.highest_contiguous_reliable_seq,
    }))
}

async fn event(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<ReliableEventEnvelope>,
) -> ApiResult<ReliableEventAck> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    authorize_attempt(&state, peer.worker_id, &request.identity)?;
    let ingest = state
        .store
        .ingest_reliable_event(peer.worker_id, &request)
        .await
        .map_err(internal)?;
    accept_event_ingest(ingest)?;
    let heartbeat = state
        .store
        .heartbeat_attempt(
            peer.worker_id,
            &request.identity,
            chrono_duration(state.config.lease_ttl())?,
            None,
        )
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    Ok(Json(ReliableEventAck {
        highest_contiguous_reliable_seq: heartbeat.highest_contiguous_reliable_seq,
    }))
}

fn accept_event_ingest(ingest: EventIngest) -> Result<(), (StatusCode, Json<ApiError>)> {
    match ingest {
        EventIngest::Inserted | EventIngest::Duplicate => {}
        EventIngest::Stale => return Err(stale_attempt()),
        EventIngest::Conflict => {
            return Err(api_error(
                StatusCode::CONFLICT,
                "event_sequence_conflict",
                "reliable event sequence was reused with different content",
                false,
            ));
        }
    }
    Ok(())
}

async fn progress(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<ProgressRequest>,
) -> ApiResult<serde_json::Value> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    authorize_attempt(&state, peer.worker_id, &request.identity)?;
    let offered = state
        .store
        .offered_assignment(peer.worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    if offered.offer.trace_id != request.trace_id {
        return Err(api_error(
            StatusCode::CONFLICT,
            "trace_mismatch",
            "progress trace does not match the active attempt",
            false,
        ));
    }
    let accepted = state
        .store
        .ingest_progress(peer.worker_id, &request)
        .await
        .map_err(internal)?;
    if !accepted {
        return Err(stale_attempt());
    }
    tracing::info!(
        worker_id = %peer.worker_id,
        attempt_id = %request.identity.attempt_id,
        trace_id = %request.trace_id,
        progress_seq = request.progress_seq,
        step = %request.update.workflow_step,
        phase = %request.update.phase,
        progress = request.update.progress,
        total = ?request.update.total,
        message = ?request.update.message,
        "worker progress"
    );
    Ok(Json(serde_json::json!({ "accepted": true })))
}

fn validate_terminal_for_assignment(
    payload: &sbgh_proto::TaskPayload,
    outcome: &sbgh_proto::TerminalOutcome,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let sbgh_proto::TerminalOutcome::Completed { block_validation, .. } = outcome else {
        return Ok(());
    };
    match (payload, block_validation) {
        (sbgh_proto::TaskPayload::BlockValidation(payload), Some(result))
            if result.dataset == payload.dataset
                && result
                    .invalid_blocks
                    .iter()
                    .all(|invalid| invalid.shard < payload.requested_shards)
                && payload
                    .range
                    .end
                    .checked_sub(payload.range.start)
                    .and_then(|distance| distance.checked_add(1))
                    == Some(result.checked_blocks) =>
        {
            Ok(())
        }
        (sbgh_proto::TaskPayload::BlockValidation(_), _) => Err(api_error(
            StatusCode::CONFLICT,
            "block_validation_result_mismatch",
            "completed block-validation result does not match its pinned assignment",
            false,
        )),
        (_, None) => Ok(()),
        (_, Some(_)) => Err(api_error(
            StatusCode::CONFLICT,
            "unexpected_block_validation_result",
            "a non-block-validation assignment cannot submit a block-validation result",
            false,
        )),
    }
}

async fn artifact_grant(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<ArtifactGrantRequest>,
) -> ApiResult<ArtifactGrantResponse> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    authorize_attempt(&state, peer.worker_id, &request.identity)?;
    if request.operation == ArtifactOperation::Get {
        let offered = state
            .store
            .offered_assignment(peer.worker_id, &request.identity)
            .await
            .map_err(internal)?
            .ok_or_else(stale_attempt)?;
        let allowed = match offered.payload {
            sbgh_proto::TaskPayload::Benchmark(payload) => {
                payload
                    .sqlite_seed_key
                    .as_deref()
                    == Some(request.key.as_str())
            }
            _ => false,
        };
        if !allowed {
            return Err(api_error(
                StatusCode::FORBIDDEN,
                "artifact_read_forbidden",
                "artifact is not an input of this assignment",
                false,
            ));
        }
        let grant = state
            .artifacts
            .fleet_get_grant(
                &request.key,
                state
                    .config
                    .upload_grant_ttl(),
            )
            .map_err(|error| {
                tracing::warn!(%error, "fleet artifact read grant unavailable");
                api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "artifact_store_unavailable",
                    "fleet mode requires a reachable S3-compatible artifact store",
                    true,
                )
            })?;
        return Ok(Json(grant));
    }
    let offered = state
        .store
        .offered_assignment(peer.worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    if !request
        .key
        .starts_with(&format!("{}/", offered.offer.job_id))
    {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "artifact_key_forbidden",
            "output artifacts must stay inside the assigned job namespace",
            false,
        ));
    }
    let size = request
        .size
        .context("artifact size is required")
        .map_err(bad_request)?;
    let digest = request
        .sha256
        .as_deref()
        .context("artifact SHA-256 is required")
        .map_err(bad_request)?;
    if size
        > state
            .config
            .max_artifact_bytes
    {
        return Err(api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "artifact_too_large",
            "artifact exceeds the configured object limit",
            false,
        ));
    }
    let key = artifact_staging_key(request.identity.attempt_id, &request.key, size, digest)
        .map_err(protocol_error)?;
    let grant = state
        .artifacts
        .fleet_put_grant(
            &key,
            size,
            digest,
            state
                .config
                .upload_grant_ttl(),
        )
        .map_err(|error| {
            tracing::warn!(%error, "fleet artifact grant unavailable");
            api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "artifact_store_unavailable",
                "fleet mode requires a reachable S3-compatible artifact store",
                true,
            )
        })?;
    let recorded = state
        .store
        .record_artifact_grant(&ArtifactGrantRecord {
            attempt_id: request.identity.attempt_id,
            object_key: key,
            logical_key: request.key,
            size: Some(size),
            sha256: Some(digest.into()),
            expires_at: Utc::now()
                + chrono_duration(
                    state
                        .config
                        .upload_grant_ttl(),
                )?,
        })
        .await
        .map_err(internal)?;
    if !recorded {
        return Err(stale_attempt());
    }
    Ok(Json(grant))
}

async fn complete(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<CompleteAttemptRequest>,
) -> ApiResult<CompleteAttemptResponse> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    authorize_attempt(&state, peer.worker_id, &request.identity)?;
    let offered = state
        .store
        .completion_assignment(peer.worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    if offered.offer.trace_id != request.trace_id {
        return Err(api_error(
            StatusCode::CONFLICT,
            "trace_mismatch",
            "terminal trace does not match the active attempt",
            false,
        ));
    }
    validate_terminal_for_assignment(&offered.payload, &request.outcome)?;
    let total = request
        .artifacts
        .iter()
        .try_fold(0_u64, |sum, artifact| sum.checked_add(artifact.size))
        .context("artifact size sum overflow")
        .map_err(bad_request)?;
    if total
        > state
            .config
            .max_attempt_artifact_bytes
    {
        return Err(api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "attempt_artifacts_too_large",
            "attempt artifacts exceed the configured aggregate limit",
            false,
        ));
    }
    let accepted_manifest = state
        .store
        .accepted_terminal_manifest(request.identity.attempt_id)
        .await
        .map_err(internal)?;
    if let Some(manifest) = &accepted_manifest
        && manifest != &request.artifacts
    {
        return Err(stale_attempt());
    }
    if accepted_manifest.is_none() {
        for artifact in &request.artifacts {
            if !artifact
                .key
                .starts_with(&format!("staging/{}/", request.identity.attempt_id))
                || !state
                    .artifacts
                    .verify_fleet_upload(artifact)
                    .await
                || !state
                    .store
                    .verify_artifact(&request.identity, artifact)
                    .await
                    .map_err(internal)?
            {
                return Err(api_error(
                    StatusCode::CONFLICT,
                    "artifact_verification_failed",
                    "an artifact is absent or does not match its signed upload metadata",
                    true,
                ));
            }
        }
    }
    let write = terminal_write(
        offered.offer.job_id,
        request.identity.attempt_id,
        &request.outcome,
        &request.artifacts,
        state.artifacts.as_ref(),
    )
    .await;
    let acceptance = state
        .store
        .accept_terminal(
            peer.worker_id,
            &request.identity,
            &FleetTerminalSubmission {
                reliable_seq: request.terminal_reliable_seq,
                payload_digest: &request.terminal_payload_digest,
                outcome: &request.outcome,
                artifacts: &request.artifacts,
                write: &write,
            },
        )
        .await
        .map_err(internal)?;
    match acceptance {
        TerminalAcceptance::Accepted | TerminalAcceptance::Duplicate => {
            if !promote_artifacts(
                state.artifacts.as_ref(),
                state.store.as_ref(),
                request.identity.attempt_id,
                &request.artifacts,
            )
            .await
            {
                return Err(api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "artifact_promotion_failed",
                    "terminal is accepted but artifact promotion is incomplete; retry",
                    true,
                ));
            }
            Ok(Json(CompleteAttemptResponse { accepted: true }))
        }
        TerminalAcceptance::Stale => Err(stale_attempt()),
    }
}

async fn cleanup(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    axum::extract::Query(request): axum::extract::Query<CleanupListRequest>,
) -> ApiResult<Vec<CleanupItem>> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    let obligations = state
        .store
        .cleanup_obligations(peer.worker_id, request.worker_session_id)
        .await
        .map_err(internal)?;
    Ok(Json(
        obligations
            .into_iter()
            .map(|item| CleanupItem {
                id: item.id,
                attempt_id: item.attempt_id,
                job_id: item.job_id,
                reason: item.reason,
            })
            .collect(),
    ))
}

async fn cleanup_complete(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<CleanupCompleteRequest>,
) -> ApiResult<serde_json::Value> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    let completed = state
        .store
        .complete_cleanup(peer.worker_id, request.worker_session_id, request.obligation_id)
        .await
        .map_err(internal)?;
    if !completed {
        return Err(stale_attempt());
    }
    tracing::info!(
        worker_id = %peer.worker_id,
        session_id = %request.worker_session_id,
        obligation_id = request.obligation_id,
        "worker cleanup obligation completed"
    );
    Ok(Json(serde_json::json!({ "completed": true })))
}

async fn deregister(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<AuthenticatedPeer>,
    Json(request): Json<DeregisterSessionRequest>,
) -> ApiResult<serde_json::Value> {
    authorized_worker(&state, &peer)?;
    validate(&request)?;
    let deregistered = state
        .store
        .deregister_session(peer.worker_id, request.worker_session_id)
        .await
        .map_err(internal)?;
    Ok(Json(serde_json::json!({ "deregistered": deregistered })))
}

async fn maintenance_loop(state: ApiState, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(StdDuration::from_secs(2));
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = interval.tick() => {
                if let Err(error) = maintenance_tick(&state).await {
                    tracing::error!(%error, "fleet maintenance tick failed");
                }
            }
        }
    }
}

async fn maintenance_tick(state: &ApiState) -> sbgh_core::Result<()> {
    for pending in state
        .store
        .pending_artifact_promotions(32)
        .await?
    {
        if !promote_artifacts(
            state.artifacts.as_ref(),
            state.store.as_ref(),
            pending.attempt_id,
            &pending.artifacts,
        )
        .await
        {
            tracing::warn!(
                attempt_id = %pending.attempt_id,
                "accepted fleet artifact promotion remains pending"
            );
        }
    }
    let keys = state
        .store
        .staged_artifacts_due_for_reap(
            Utc::now()
                - Duration::from_std(
                    state
                        .config
                        .staging_gc_grace(),
                )
                .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?,
        )
        .await?;
    for key in keys {
        if state
            .artifacts
            .delete_fleet_staging(&key)
            .await
        {
            if !state
                .store
                .mark_staged_artifact_reaped(&key)
                .await?
            {
                tracing::warn!(%key, "deleted staging object was no longer reap-eligible");
            }
        } else {
            tracing::warn!(%key, "failed to delete expired fleet staging object");
        }
    }
    Ok(())
}

async fn promote_artifacts(
    artifacts: &dyn ArtifactStore,
    store: &dyn FleetStore,
    attempt_id: Uuid,
    manifest: &[sbgh_proto::ArtifactDescriptor],
) -> bool {
    for artifact in manifest {
        if !artifacts
            .promote_fleet_upload(artifact)
            .await
        {
            return false;
        }
    }
    match store
        .mark_artifacts_promoted(attempt_id, manifest)
        .await
    {
        Ok(promoted) => promoted,
        Err(error) => {
            tracing::warn!(%attempt_id, %error, "recording fleet artifact promotion failed");
            false
        }
    }
}

async fn terminal_write(
    job_id: Uuid,
    attempt_id: Uuid,
    outcome: &sbgh_proto::TerminalOutcome,
    artifacts: &[sbgh_proto::ArtifactDescriptor],
    store: &dyn ArtifactStore,
) -> FleetTerminalWrite {
    let logical_artifacts = logical_artifacts(artifacts);
    match outcome {
        sbgh_proto::TerminalOutcome::Completed { summary, block_validation } => {
            let (result, metric) = if block_validation.is_some() {
                (
                    JobResult {
                        job_id,
                        run_json: Some(summary.clone()),
                        archive_dir: format!("fleet:{attempt_id}"),
                        created_at: Utc::now(),
                    },
                    None,
                )
            } else {
                let staging_summary = staging_summary(summary, artifacts);
                crate::job_source::extract_outcome(job_id, &staging_summary, store).await
            };
            FleetTerminalWrite::Completed(Box::new(FleetCompletion {
                result,
                metric,
                baseline_calibration_id: crate::job_source::baseline_calibration_id(summary),
                event_detail: Some(serde_json::json!({
                    "attempt_id": attempt_id,
                    "artifacts": logical_artifacts,
                    "block_validation": block_validation,
                    "summary": summary,
                })),
                block_validation: block_validation.clone(),
                artifact_manifest: logical_artifacts,
            }))
        }
        sbgh_proto::TerminalOutcome::Failed { error, summary, .. } => {
            let result = match summary {
                Some(summary) => {
                    let staging_summary = staging_summary(summary, artifacts);
                    Some(
                        crate::job_source::extract_outcome(job_id, &staging_summary, store)
                            .await
                            .0,
                    )
                }
                None => None,
            };
            FleetTerminalWrite::Failed(FleetFailure {
                result,
                remark: error.clone(),
                event_detail: Some(serde_json::json!({
                    "attempt_id": attempt_id,
                    "artifacts": logical_artifacts,
                })),
            })
        }
        sbgh_proto::TerminalOutcome::Cancelled { reason } => {
            FleetTerminalWrite::Cancelled { remark: reason.clone() }
        }
    }
}

fn logical_artifacts(
    artifacts: &[sbgh_proto::ArtifactDescriptor],
) -> Vec<sbgh_proto::ArtifactDescriptor> {
    artifacts
        .iter()
        .map(|artifact| sbgh_proto::ArtifactDescriptor {
            key: artifact.logical_key.clone(),
            logical_key: artifact.logical_key.clone(),
            size: artifact.size,
            sha256: artifact.sha256.clone(),
        })
        .collect()
}

fn artifact_staging_key(
    attempt_id: Uuid,
    logical_key: &str,
    size: u64,
    sha256: &str,
) -> Result<String, sbgh_proto::ProtocolError> {
    let identity = sbgh_proto::payload_digest(&serde_json::json!({
        "attempt_id": attempt_id,
        "logical_key": logical_key,
        "size": size,
        "sha256": sha256,
    }))?;
    Ok(format!("staging/{attempt_id}/{identity}/{logical_key}"))
}

fn staging_summary(
    summary: &serde_json::Value,
    artifacts: &[sbgh_proto::ArtifactDescriptor],
) -> serde_json::Value {
    let mut summary = summary.clone();
    let Some(logical_key) = summary
        .get("run_json_archived_path")
        .and_then(serde_json::Value::as_str)
    else {
        return summary;
    };
    let Some(staging_key) = artifacts
        .iter()
        .find(|artifact| artifact.logical_key == logical_key)
        .map(|artifact| artifact.key.clone())
    else {
        return summary;
    };
    if let Some(object) = summary.as_object_mut() {
        object.insert("run_json_archived_path".into(), serde_json::Value::String(staging_key));
    }
    summary
}

fn authorize_attempt(
    state: &ApiState,
    worker_id: Uuid,
    identity: &sbgh_proto::AttemptIdentity,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    if state
        .signer
        .verify(worker_id, identity)
    {
        Ok(())
    } else {
        Err(api_error(
            StatusCode::FORBIDDEN,
            "invalid_lease_token",
            "attempt authorization is invalid",
            false,
        ))
    }
}

fn authorized_worker<'a>(
    state: &'a ApiState,
    peer: &AuthenticatedPeer,
) -> Result<&'a super::config::ConfiguredWorker, (StatusCode, Json<ApiError>)> {
    let worker = state
        .config
        .workers
        .iter()
        .find(|worker| worker.id == peer.worker_id)
        .ok_or_else(|| {
            api_error(
                StatusCode::FORBIDDEN,
                "worker_not_registered",
                "certificate identity is not present in the worker registry policy",
                false,
            )
        })?;
    if !worker.enabled {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "worker_disabled",
            "worker identity is disabled",
            false,
        ));
    }
    if !worker
        .certificate_sha256
        .contains(&peer.certificate_sha256)
    {
        return Err(api_error(
            StatusCode::FORBIDDEN,
            "worker_certificate_revoked",
            "worker certificate is not authorized for this identity",
            false,
        ));
    }
    Ok(worker)
}

fn validate<T: Validate>(value: &T) -> Result<(), (StatusCode, Json<ApiError>)> {
    value
        .validate()
        .map_err(protocol_error)
}

fn chrono_duration(value: StdDuration) -> Result<Duration, (StatusCode, Json<ApiError>)> {
    Duration::from_std(value).map_err(|error| internal(anyhow::Error::new(error)))
}

fn protocol_error(error: sbgh_proto::ProtocolError) -> (StatusCode, Json<ApiError>) {
    api_error(StatusCode::BAD_REQUEST, "invalid_protocol_message", &error.to_string(), false)
}

fn bad_request(error: anyhow::Error) -> (StatusCode, Json<ApiError>) {
    api_error(StatusCode::BAD_REQUEST, "invalid_request", &error.to_string(), false)
}

fn stale_attempt() -> (StatusCode, Json<ApiError>) {
    api_error(
        StatusCode::CONFLICT,
        "stale_attempt",
        "attempt is expired, fenced, or no longer current",
        false,
    )
}

fn internal(error: impl std::fmt::Display) -> (StatusCode, Json<ApiError>) {
    tracing::error!(%error, "worker API operation failed");
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "worker API operation failed",
        true,
    )
}

fn api_error(
    status: StatusCode,
    code: &str,
    message: &str,
    retryable: bool,
) -> (StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError {
            code: code.into(),
            message: message.into(),
            retryable,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sbgh_proto::{
        ArtifactDescriptor, BlockValidationPayload, BlockValidationResult, DatasetIdentity,
        InclusiveRange, TaskPayload, TerminalOutcome, ValidationEpoch,
    };

    use super::{
        ActivePolls, accept_event_ingest, artifact_staging_key, logical_artifacts,
        validate_terminal_for_assignment,
    };
    use sbgh_core::db::fleet::EventIngest;

    fn dataset() -> DatasetIdentity {
        DatasetIdentity {
            generation: "mainnet-1".into(),
            network: "mainnet".into(),
            format_version: "v1".into(),
            covered_start: 10,
            covered_end: 19,
            manifest_sha256: "00".repeat(32),
        }
    }

    #[test]
    fn one_worker_session_cannot_hold_multiple_long_polls() {
        let active = Arc::new(ActivePolls::default());
        let worker_id = uuid::Uuid::new_v4();
        let session_id = uuid::Uuid::new_v4();
        let first = active
            .enter(worker_id, session_id)
            .expect("first poll enters");
        assert!(
            active
                .enter(worker_id, session_id)
                .is_none()
        );
        assert!(
            active
                .enter(uuid::Uuid::new_v4(), session_id)
                .is_some(),
            "another authenticated worker cannot occupy this worker's guard"
        );
        drop(first);
        assert!(
            active
                .enter(worker_id, session_id)
                .is_some()
        );
    }

    #[test]
    fn artifact_grant_retries_reuse_the_exact_staging_identity() {
        let attempt_id = uuid::Uuid::new_v4();
        let digest = "ab".repeat(32);
        let first = artifact_staging_key(attempt_id, "job/run.json", 17, &digest).unwrap();
        let retry = artifact_staging_key(attempt_id, "job/run.json", 17, &digest).unwrap();
        assert_eq!(first, retry);
        assert_ne!(first, artifact_staging_key(attempt_id, "job/run.json", 18, &digest).unwrap());
    }

    #[test]
    fn stale_and_conflicting_events_are_typed_non_retryable_conflicts() {
        for (ingest, expected_code) in [
            (EventIngest::Stale, "stale_attempt"),
            (EventIngest::Conflict, "event_sequence_conflict"),
        ] {
            let (status, axum::Json(error)) = accept_event_ingest(ingest).unwrap_err();
            assert_eq!(status, axum::http::StatusCode::CONFLICT);
            assert_eq!(error.code, expected_code);
            assert!(!error.retryable);
        }
        assert!(accept_event_ingest(EventIngest::Inserted).is_ok());
        assert!(accept_event_ingest(EventIngest::Duplicate).is_ok());
    }

    #[test]
    fn block_terminal_must_match_pinned_dataset_range_and_task_kind() {
        let payload = TaskPayload::BlockValidation(BlockValidationPayload {
            dataset: dataset(),
            epoch: ValidationEpoch::Nakamoto,
            range: InclusiveRange { start: 10, end: 19 },
            requested_shards: 2,
            max_concurrency: 2,
            timeout_secs: 60,
        });
        let outcome = TerminalOutcome::Completed {
            summary: serde_json::json!({}),
            block_validation: Some(BlockValidationResult {
                valid: true,
                checked_blocks: 10,
                invalid_blocks: Vec::new(),
                dataset: dataset(),
            }),
        };
        assert!(validate_terminal_for_assignment(&payload, &outcome).is_ok());

        let mut wrong_count = outcome.clone();
        if let TerminalOutcome::Completed {
            block_validation: Some(result), ..
        } = &mut wrong_count
        {
            result.checked_blocks = 9;
        }
        assert!(validate_terminal_for_assignment(&payload, &wrong_count).is_err());
        assert!(validate_terminal_for_assignment(&TaskPayload::BuildOnly, &outcome).is_err());
    }

    #[test]
    fn durable_result_artifacts_never_expose_attempt_staging_keys() {
        let logical = "job/run.json";
        let projected = logical_artifacts(&[ArtifactDescriptor {
            key: "staging/attempt/random/job/run.json".into(),
            logical_key: logical.into(),
            size: 17,
            sha256: "ab".repeat(32),
        }]);
        assert_eq!(projected[0].key, logical);
        assert_eq!(projected[0].logical_key, logical);
        assert!(
            !serde_json::to_string(&projected)
                .unwrap()
                .contains("staging/")
        );
    }
}
