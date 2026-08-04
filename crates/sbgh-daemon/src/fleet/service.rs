use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration as StdDuration;

use anyhow::Context;
use chrono::{Duration, Utc};
use sbgh_core::config::FleetConfig;
use sbgh_core::db::fleet::{
    ArtifactGrantRecord, EventIngest, FleetCompletion, FleetFailure, FleetStore,
    FleetTerminalSubmission, FleetTerminalWrite, TerminalAcceptance, WorkerAuthorization,
    WorkerRegistryStore,
};
use sbgh_core::models::JobResult;
use sbgh_fleet::{
    AcceptOfferRequest, AcceptOfferResponse, ArtifactGrantRequest, ArtifactGrantResponse,
    ArtifactOperation, Assignment, AssignmentContext, CleanupCompleteRequest, CleanupItem,
    CleanupListRequest, CompleteAttemptRequest, CompleteAttemptResponse, DeregisterSessionRequest,
    HeartbeatRequest, HeartbeatResponse, PROTOCOL_VERSION, PollRequest, PollResponse,
    ProgressRequest, RegisterSessionRequest, RegisterSessionResponse, RegistrationCheckRequest,
    RegistrationCheckResponse, ReliableEventAck, ReliableEventEnvelope,
    RepositoryCredentialRequest, RepositoryCredentialResponse, RepositoryToken, Validate,
};
use sbgh_github::InstallationTokenCache;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::lease::LeaseSigner;
use super::tls::AuthenticatedPeer;
use crate::artifact_store::ArtifactStore;

pub(super) type ServiceResult<T> = Result<T, ServiceError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ServiceCode {
    InvalidArgument,
    PermissionDenied,
    FailedPrecondition,
    ResourceExhausted,
    Unavailable,
    Internal,
}

#[derive(Debug)]
pub(super) struct ServiceError {
    pub code: ServiceCode,
    pub stable_code: &'static str,
    pub message: String,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
}

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
pub(super) struct FleetService {
    store: Arc<dyn FleetStore>,
    registry: Arc<dyn WorkerRegistryStore>,
    artifacts: Arc<dyn ArtifactStore>,
    github_tokens: InstallationTokenCache,
    signer: LeaseSigner,
    config: Arc<FleetConfig>,
    active_polls: Arc<ActivePolls>,
}

pub struct FleetRuntime {
    pub(super) service: FleetService,
}

impl FleetRuntime {
    pub async fn build(
        config: FleetConfig,
        store: Arc<dyn FleetStore>,
        registry: Arc<dyn WorkerRegistryStore>,
        artifacts: Arc<dyn ArtifactStore>,
        github_tokens: InstallationTokenCache,
    ) -> anyhow::Result<Self> {
        let signer = LeaseSigner::load(&config.lease_hmac_key)?;
        Ok(Self {
            service: FleetService {
                store,
                registry,
                artifacts,
                github_tokens,
                signer,
                config: Arc::new(config),
                active_polls: Arc::new(ActivePolls::default()),
            },
        })
    }
}

impl FleetService {
    pub(super) fn config(&self) -> &Arc<FleetConfig> {
        &self.config
    }
}

pub(super) async fn register(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: RegisterSessionRequest,
) -> ServiceResult<RegisterSessionResponse> {
    let configured = authorized_worker(state, peer).await?;
    let worker_id = configured.worker_id;
    validate(&request)?;
    effective_capabilities(&configured, &request.advertised_capabilities)?;
    let session = state
        .store
        .register_session(
            worker_id,
            peer.identity_key_sha256,
            &request,
            chrono_duration(state.config.session_ttl())?,
        )
        .await
        .map_err(internal)?;
    tracing::info!(
        worker_id = %worker_id,
        session_id = %session.worker_session_id,
        peer = %peer.socket_addr,
        capabilities = ?session.effective_capabilities,
        "worker session registered"
    );
    Ok(RegisterSessionResponse {
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
    })
}

/// Validate everything required for registration without creating a session.
/// This endpoint is safe to call while the same worker identity is active.
pub(super) async fn check_registration(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: RegistrationCheckRequest,
) -> ServiceResult<RegistrationCheckResponse> {
    registration_readiness(state.registry.as_ref(), state.config.as_ref(), peer, request).await
}

async fn registration_readiness(
    registry: &dyn WorkerRegistryStore,
    config: &FleetConfig,
    peer: &AuthenticatedPeer,
    request: RegistrationCheckRequest,
) -> ServiceResult<RegistrationCheckResponse> {
    let configured = authorize_peer(registry, peer).await?;
    validate(&request)?;
    let effective_capabilities =
        effective_capabilities(&configured, &request.advertised_capabilities)?;
    tracing::info!(
        worker_id = %configured.worker_id,
        peer = %peer.socket_addr,
        effective_capabilities = ?effective_capabilities,
        measurement_profile = ?configured.measurement_profile,
        draining = configured.draining,
        "worker registration readiness checked"
    );
    Ok(RegistrationCheckResponse {
        protocol_version: PROTOCOL_VERSION,
        worker_id: configured.worker_id,
        effective_capabilities,
        measurement_profile: configured.measurement_profile,
        draining: configured.draining,
        heartbeat_interval_ms: config
            .heartbeat_interval()
            .as_millis() as u64,
        lease_ttl_ms: config.lease_ttl().as_millis() as u64,
        server_time_ms: Utc::now().timestamp_millis(),
    })
}

fn effective_capabilities(
    configured: &WorkerAuthorization,
    advertised: &BTreeSet<sbgh_fleet::WorkerCapability>,
) -> ServiceResult<BTreeSet<sbgh_fleet::WorkerCapability>> {
    let effective = advertised
        .iter()
        .filter(|capability| {
            configured
                .allowed_capabilities
                .contains(capability)
        })
        .copied()
        .collect::<BTreeSet<_>>();
    if effective.is_empty() {
        return Err(service_error(
            ServiceCode::PermissionDenied,
            "capability_not_authorized",
            "worker advertises no server-authorized capability",
            false,
        ));
    }
    Ok(effective)
}

pub(super) async fn poll(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: PollRequest,
) -> ServiceResult<PollResponse> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    if !state
        .store
        .session_is_active(worker_id, request.worker_session_id)
        .await
        .map_err(internal)?
    {
        return Err(service_error(
            ServiceCode::FailedPrecondition,
            "stale_session",
            "worker session is not active",
            false,
        ));
    }
    let _poll_guard = state
        .active_polls
        .enter(worker_id, request.worker_session_id)
        .ok_or_else(|| {
            service_error(
                ServiceCode::FailedPrecondition,
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
                worker_id,
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
                worker_id,
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
                worker_id = %worker_id,
                attempt_id = %offered.offer.identity.attempt_id,
                job_id = %offered.offer.job_id,
                trace_id = %offered.offer.trace_id,
                "work offered"
            );
            return Ok(PollResponse::Offer { offer: Box::new(offered.offer) });
        }
        if state
            .store
            .session_is_draining(worker_id, request.worker_session_id)
            .await
            .map_err(internal)?
        {
            return Ok(PollResponse::Drain);
        }
        if !state
            .store
            .session_is_active(worker_id, request.worker_session_id)
            .await
            .map_err(internal)?
        {
            return Err(service_error(
                ServiceCode::PermissionDenied,
                "worker_session_inactive",
                "worker session is expired, disabled, or superseded",
                false,
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(PollResponse::NoWork { retry_after_ms: 1_000 });
        }
        tokio::time::sleep(StdDuration::from_millis(250)).await;
    }
}

pub(super) async fn accept(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: AcceptOfferRequest,
) -> ServiceResult<AcceptOfferResponse> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    authorize_attempt(state, worker_id, &request.identity)?;
    let offered = state
        .store
        .offered_assignment(worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    let accepted = state
        .store
        .accept_offer(worker_id, &request.identity, chrono_duration(state.config.lease_ttl())?)
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
    Ok(AcceptOfferResponse { assignment })
}

pub(super) async fn repository_credential(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: RepositoryCredentialRequest,
) -> ServiceResult<RepositoryCredentialResponse> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    authorize_attempt(state, worker_id, &request.identity)?;
    let heartbeat = state
        .store
        .heartbeat_attempt(
            worker_id,
            &request.identity,
            chrono_duration(state.config.lease_ttl())?,
            None,
        )
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    if heartbeat.desired_state == sbgh_fleet::DesiredState::Cancel {
        return Err(service_error(
            ServiceCode::FailedPrecondition,
            "attempt_cancelled",
            "repository credentials are unavailable after cancellation is committed",
            false,
        ));
    }
    let offered = state
        .store
        .offered_assignment(worker_id, &request.identity)
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
            service_error(
                ServiceCode::Unavailable,
                "repository_token_unavailable",
                "repository credential is temporarily unavailable",
                true,
            )
        })?;
    Ok(RepositoryCredentialResponse {
        token: RepositoryToken(token.token),
        expires_at_ms: token
            .expires_at
            .timestamp_millis(),
    })
}

pub(super) async fn heartbeat(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: HeartbeatRequest,
) -> ServiceResult<HeartbeatResponse> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    authorize_attempt(state, worker_id, &request.identity)?;
    let heartbeat = state
        .store
        .heartbeat_attempt(
            worker_id,
            &request.identity,
            chrono_duration(state.config.lease_ttl())?,
            Some(request.reliable_buffer_len),
        )
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    Ok(HeartbeatResponse {
        desired_state: heartbeat.desired_state,
        lease_expires_at_ms: heartbeat
            .lease_expires_at
            .timestamp_millis(),
        highest_contiguous_reliable_seq: heartbeat.highest_contiguous_reliable_seq,
    })
}

pub(super) async fn event(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: ReliableEventEnvelope,
) -> ServiceResult<ReliableEventAck> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    authorize_attempt(state, worker_id, &request.identity)?;
    let ingest = state
        .store
        .ingest_reliable_event(worker_id, &request)
        .await
        .map_err(internal)?;
    accept_event_ingest(ingest)?;
    let heartbeat = state
        .store
        .heartbeat_attempt(
            worker_id,
            &request.identity,
            chrono_duration(state.config.lease_ttl())?,
            None,
        )
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    Ok(ReliableEventAck {
        highest_contiguous_reliable_seq: heartbeat.highest_contiguous_reliable_seq,
    })
}

fn accept_event_ingest(ingest: EventIngest) -> ServiceResult<()> {
    match ingest {
        EventIngest::Inserted | EventIngest::Duplicate => {}
        EventIngest::Stale => return Err(stale_attempt()),
        EventIngest::Conflict => {
            return Err(service_error(
                ServiceCode::FailedPrecondition,
                "event_sequence_conflict",
                "reliable event sequence was reused with different content",
                false,
            ));
        }
    }
    Ok(())
}

pub(super) async fn progress(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: ProgressRequest,
) -> ServiceResult<bool> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    authorize_attempt(state, worker_id, &request.identity)?;
    let offered = state
        .store
        .offered_assignment(worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    if offered.offer.trace_id != request.trace_id {
        return Err(service_error(
            ServiceCode::FailedPrecondition,
            "trace_mismatch",
            "progress trace does not match the active attempt",
            false,
        ));
    }
    let accepted = state
        .store
        .ingest_progress(worker_id, &request)
        .await
        .map_err(internal)?;
    if !accepted {
        return Err(stale_attempt());
    }
    tracing::info!(
        worker_id = %worker_id,
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
    Ok(true)
}

fn validate_terminal_for_assignment(
    payload: &sbgh_fleet::TaskPayload,
    outcome: &sbgh_fleet::TerminalOutcome,
) -> ServiceResult<()> {
    let sbgh_fleet::TerminalOutcome::Completed { block_validation, .. } = outcome else {
        return Ok(());
    };
    match (payload, block_validation) {
        (sbgh_fleet::TaskPayload::BlockValidation(payload), Some(result)) => {
            sbgh_fleet::validate_block_validation_result(payload, result).map_err(|_| {
                service_error(
                    ServiceCode::FailedPrecondition,
                    "block_validation_result_mismatch",
                    "completed block-validation result does not match its pinned assignment",
                    false,
                )
            })
        }
        (sbgh_fleet::TaskPayload::BlockValidation(_), _) => Err(service_error(
            ServiceCode::FailedPrecondition,
            "block_validation_result_mismatch",
            "completed block-validation result does not match its pinned assignment",
            false,
        )),
        (_, None) => Ok(()),
        (_, Some(_)) => Err(service_error(
            ServiceCode::FailedPrecondition,
            "unexpected_block_validation_result",
            "a non-block-validation assignment cannot submit a block-validation result",
            false,
        )),
    }
}

pub(super) async fn artifact_grant(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: ArtifactGrantRequest,
) -> ServiceResult<ArtifactGrantResponse> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    authorize_attempt(state, worker_id, &request.identity)?;
    if request.operation == ArtifactOperation::Get {
        let offered = state
            .store
            .offered_assignment(worker_id, &request.identity)
            .await
            .map_err(internal)?
            .ok_or_else(stale_attempt)?;
        let allowed = match offered.payload {
            sbgh_fleet::TaskPayload::Benchmark(payload) => {
                payload
                    .sqlite_seed_key
                    .as_deref()
                    == Some(request.key.as_str())
            }
            _ => false,
        };
        if !allowed {
            return Err(service_error(
                ServiceCode::PermissionDenied,
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
                service_error(
                    ServiceCode::Unavailable,
                    "artifact_store_unavailable",
                    "fleet mode requires a reachable S3-compatible artifact store",
                    true,
                )
            })?;
        return Ok(grant);
    }
    let offered = state
        .store
        .offered_assignment(worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    if !request
        .key
        .starts_with(&format!("{}/", offered.offer.job_id))
    {
        return Err(service_error(
            ServiceCode::PermissionDenied,
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
        return Err(service_error(
            ServiceCode::ResourceExhausted,
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
            service_error(
                ServiceCode::Unavailable,
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
    Ok(grant)
}

pub(super) async fn complete(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: CompleteAttemptRequest,
) -> ServiceResult<CompleteAttemptResponse> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    authorize_attempt(state, worker_id, &request.identity)?;
    let offered = state
        .store
        .completion_assignment(worker_id, &request.identity)
        .await
        .map_err(internal)?
        .ok_or_else(stale_attempt)?;
    if offered.offer.trace_id != request.trace_id {
        return Err(service_error(
            ServiceCode::FailedPrecondition,
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
        return Err(service_error(
            ServiceCode::ResourceExhausted,
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
                return Err(service_error(
                    ServiceCode::FailedPrecondition,
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
            worker_id,
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
                return Err(service_error(
                    ServiceCode::Unavailable,
                    "artifact_promotion_failed",
                    "terminal is accepted but artifact promotion is incomplete; retry",
                    true,
                ));
            }
            Ok(CompleteAttemptResponse { accepted: true })
        }
        TerminalAcceptance::Stale => Err(stale_attempt()),
    }
}

pub(super) async fn cleanup(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: CleanupListRequest,
) -> ServiceResult<Vec<CleanupItem>> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    let obligations = state
        .store
        .cleanup_obligations(worker_id, request.worker_session_id)
        .await
        .map_err(internal)?;
    Ok(obligations
        .into_iter()
        .map(|item| CleanupItem {
            id: item.id,
            attempt_id: item.attempt_id,
            job_id: item.job_id,
            reason: item.reason,
        })
        .collect())
}

pub(super) async fn cleanup_complete(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: CleanupCompleteRequest,
) -> ServiceResult<bool> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    let completed = state
        .store
        .complete_cleanup(worker_id, request.worker_session_id, request.obligation_id)
        .await
        .map_err(internal)?;
    if !completed {
        return Err(stale_attempt());
    }
    tracing::info!(
        worker_id = %worker_id,
        session_id = %request.worker_session_id,
        obligation_id = request.obligation_id,
        "worker cleanup obligation completed"
    );
    Ok(true)
}

pub(super) async fn deregister(
    state: &FleetService,
    peer: &AuthenticatedPeer,
    request: DeregisterSessionRequest,
) -> ServiceResult<bool> {
    let worker_id = authorized_worker(state, peer)
        .await?
        .worker_id;
    validate(&request)?;
    let deregistered = state
        .store
        .deregister_session(worker_id, request.worker_session_id)
        .await
        .map_err(internal)?;
    Ok(deregistered)
}

pub(super) async fn maintenance_loop(state: FleetService, shutdown: CancellationToken) {
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

async fn maintenance_tick(state: &FleetService) -> sbgh_core::Result<()> {
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
    manifest: &[sbgh_fleet::ArtifactDescriptor],
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
    outcome: &sbgh_fleet::TerminalOutcome,
    artifacts: &[sbgh_fleet::ArtifactDescriptor],
    store: &dyn ArtifactStore,
) -> FleetTerminalWrite {
    let logical_artifacts = logical_artifacts(artifacts);
    match outcome {
        sbgh_fleet::TerminalOutcome::Completed { summary, block_validation } => {
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
        sbgh_fleet::TerminalOutcome::Failed { error, summary, .. } => {
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
        sbgh_fleet::TerminalOutcome::Cancelled { reason } => {
            FleetTerminalWrite::Cancelled { remark: reason.clone() }
        }
    }
}

fn logical_artifacts(
    artifacts: &[sbgh_fleet::ArtifactDescriptor],
) -> Vec<sbgh_fleet::ArtifactDescriptor> {
    artifacts
        .iter()
        .map(|artifact| sbgh_fleet::ArtifactDescriptor {
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
) -> Result<String, sbgh_fleet::ProtocolError> {
    let identity = sbgh_fleet::payload_digest(&serde_json::json!({
        "attempt_id": attempt_id,
        "logical_key": logical_key,
        "size": size,
        "sha256": sha256,
    }))?;
    Ok(format!("staging/{attempt_id}/{identity}/{logical_key}"))
}

fn staging_summary(
    summary: &serde_json::Value,
    artifacts: &[sbgh_fleet::ArtifactDescriptor],
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
    state: &FleetService,
    worker_id: Uuid,
    identity: &sbgh_fleet::AttemptIdentity,
) -> ServiceResult<()> {
    if state
        .signer
        .verify(worker_id, identity)
    {
        Ok(())
    } else {
        Err(service_error(
            ServiceCode::PermissionDenied,
            "invalid_lease_token",
            "attempt authorization is invalid",
            false,
        ))
    }
}

async fn authorized_worker(
    state: &FleetService,
    peer: &AuthenticatedPeer,
) -> ServiceResult<WorkerAuthorization> {
    authorize_peer(state.registry.as_ref(), peer).await
}

async fn authorize_peer(
    registry: &dyn WorkerRegistryStore,
    peer: &AuthenticatedPeer,
) -> ServiceResult<WorkerAuthorization> {
    let worker = registry
        .authorize_worker(peer.identity_key_sha256)
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            service_error(
                ServiceCode::PermissionDenied,
                "worker_not_registered",
                "worker identity or enabled state is invalid",
                false,
            )
        })?;
    Ok(worker)
}

fn validate<T: Validate>(value: &T) -> ServiceResult<()> {
    value
        .validate()
        .map_err(protocol_error)
}

fn chrono_duration(value: StdDuration) -> ServiceResult<Duration> {
    Duration::from_std(value).map_err(|error| internal(anyhow::Error::new(error)))
}

fn protocol_error(error: sbgh_fleet::ProtocolError) -> ServiceError {
    service_error(
        ServiceCode::InvalidArgument,
        "invalid_protocol_message",
        &error.to_string(),
        false,
    )
}

fn bad_request(error: anyhow::Error) -> ServiceError {
    service_error(ServiceCode::InvalidArgument, "invalid_request", &error.to_string(), false)
}

fn stale_attempt() -> ServiceError {
    service_error(
        ServiceCode::FailedPrecondition,
        "stale_attempt",
        "attempt is expired, fenced, or no longer current",
        false,
    )
}

fn internal(error: impl std::fmt::Display) -> ServiceError {
    tracing::error!(%error, "worker fleet operation failed");
    service_error(ServiceCode::Internal, "internal_error", "worker fleet operation failed", true)
}

fn service_error(
    status: ServiceCode,
    code: &'static str,
    message: &str,
    retryable: bool,
) -> ServiceError {
    ServiceError {
        code: status,
        stable_code: code,
        message: message.into(),
        retryable,
        retry_after_ms: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use sbgh_core::db::fleet::{
        WorkerAuthorization, WorkerIdentityRecord, WorkerPolicyPatch, WorkerRegistration,
        WorkerRegistryEntry, WorkerRegistryMutation, WorkerRegistryStore,
    };
    use sbgh_fleet::{
        ArtifactDescriptor, BlockValidationPayload, BlockValidationResult, InclusiveRange,
        PROTOCOL_VERSION, RegistrationCheckRequest, ResourceFacts, TaskPayload, TerminalOutcome,
        ValidationEpoch, WorkerCapability,
    };

    use super::{
        ActivePolls, ServiceCode, accept_event_ingest, artifact_staging_key, authorize_peer,
        effective_capabilities, logical_artifacts, registration_readiness,
        validate_terminal_for_assignment,
    };
    use sbgh_core::db::fleet::EventIngest;

    #[derive(Default)]
    struct ToggleRegistry {
        authorized: AtomicBool,
    }

    #[async_trait]
    impl WorkerRegistryStore for ToggleRegistry {
        async fn create_worker(
            &self,
            _: &WorkerRegistration,
        ) -> sbgh_core::Result<WorkerRegistryMutation> {
            Ok(WorkerRegistryMutation::NotFound)
        }

        async fn update_worker(
            &self,
            _: uuid::Uuid,
            _: &WorkerPolicyPatch,
        ) -> sbgh_core::Result<WorkerRegistryMutation> {
            Ok(WorkerRegistryMutation::NotFound)
        }

        async fn authorize_identity(
            &self,
            _: uuid::Uuid,
            _: [u8; 32],
        ) -> sbgh_core::Result<WorkerRegistryMutation> {
            Ok(WorkerRegistryMutation::NotFound)
        }

        async fn revoke_identity(
            &self,
            _: uuid::Uuid,
            _: [u8; 32],
        ) -> sbgh_core::Result<WorkerRegistryMutation> {
            Ok(WorkerRegistryMutation::NotFound)
        }

        async fn emergency_disable_worker(
            &self,
            _: uuid::Uuid,
        ) -> sbgh_core::Result<WorkerRegistryMutation> {
            Ok(WorkerRegistryMutation::NotFound)
        }

        async fn emergency_revoke_identity(
            &self,
            _: uuid::Uuid,
            _: [u8; 32],
        ) -> sbgh_core::Result<WorkerRegistryMutation> {
            Ok(WorkerRegistryMutation::NotFound)
        }

        async fn worker_identities(
            &self,
            _: uuid::Uuid,
        ) -> sbgh_core::Result<Vec<WorkerIdentityRecord>> {
            Ok(Vec::new())
        }

        async fn workers(
            &self,
            _: Option<uuid::Uuid>,
        ) -> sbgh_core::Result<Vec<WorkerRegistryEntry>> {
            Ok(Vec::new())
        }

        async fn authorize_worker(
            &self,
            _: [u8; 32],
        ) -> sbgh_core::Result<Option<WorkerAuthorization>> {
            Ok(self
                .authorized
                .load(Ordering::SeqCst)
                .then_some(WorkerAuthorization {
                    worker_id: uuid::Uuid::from_u128(1),
                    allowed_capabilities: vec![sbgh_fleet::WorkerCapability::BuildOnly],
                    measurement_profile: None,
                    draining: false,
                }))
        }

        async fn set_worker_draining(
            &self,
            _: uuid::Uuid,
            _: bool,
        ) -> sbgh_core::Result<WorkerRegistryMutation> {
            Ok(WorkerRegistryMutation::NotFound)
        }
    }

    #[tokio::test]
    async fn same_authenticated_connection_peer_is_rechecked_after_revocation() {
        let registry = ToggleRegistry::default();
        registry
            .authorized
            .store(true, Ordering::SeqCst);
        let peer = crate::fleet::tls::AuthenticatedPeer {
            identity_key_sha256: [0x77; 32],
            socket_addr: "127.0.0.1:1234"
                .parse()
                .unwrap(),
        };
        assert!(
            authorize_peer(&registry, &peer)
                .await
                .is_ok()
        );
        registry
            .authorized
            .store(false, Ordering::SeqCst);
        let error = authorize_peer(&registry, &peer)
            .await
            .unwrap_err();
        assert_eq!(error.code, ServiceCode::PermissionDenied);
        assert_eq!(error.stable_code, "worker_not_registered");
    }

    #[tokio::test]
    async fn registration_readiness_returns_registry_policy_without_a_session_store() {
        let registry = ToggleRegistry::default();
        registry
            .authorized
            .store(true, Ordering::SeqCst);
        let peer = crate::fleet::tls::AuthenticatedPeer {
            identity_key_sha256: [0x55; 32],
            socket_addr: "127.0.0.1:1234"
                .parse()
                .unwrap(),
        };
        let response = registration_readiness(
            &registry,
            &sbgh_core::config::FleetConfig::default(),
            &peer,
            RegistrationCheckRequest {
                protocol_version: PROTOCOL_VERSION,
                software_version: "test".into(),
                advertised_capabilities: std::collections::BTreeSet::from([
                    WorkerCapability::Benchmark,
                    WorkerCapability::BuildOnly,
                ]),
                resources: ResourceFacts {
                    logical_cpus: 8,
                    memory_bytes: 32 * 1024 * 1024 * 1024,
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(response.worker_id, uuid::Uuid::from_u128(1));
        assert_eq!(
            response.effective_capabilities,
            std::collections::BTreeSet::from([WorkerCapability::BuildOnly])
        );
        assert!(!response.draining);
        assert_eq!(response.protocol_version, PROTOCOL_VERSION);
    }

    #[test]
    fn registration_admission_returns_only_server_authorized_capabilities() {
        let authorization = WorkerAuthorization {
            worker_id: uuid::Uuid::from_u128(1),
            allowed_capabilities: vec![
                sbgh_fleet::WorkerCapability::BuildOnly,
                sbgh_fleet::WorkerCapability::BlockValidation,
            ],
            measurement_profile: Some("zen4".into()),
            draining: false,
        };
        let advertised = std::collections::BTreeSet::from([
            sbgh_fleet::WorkerCapability::Benchmark,
            sbgh_fleet::WorkerCapability::BuildOnly,
        ]);
        assert_eq!(
            effective_capabilities(&authorization, &advertised).unwrap(),
            std::collections::BTreeSet::from([sbgh_fleet::WorkerCapability::BuildOnly])
        );
        assert!(
            effective_capabilities(
                &authorization,
                &std::collections::BTreeSet::from([sbgh_fleet::WorkerCapability::Benchmark]),
            )
            .is_err()
        );
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
            let error = accept_event_ingest(ingest).unwrap_err();
            assert_eq!(error.code, ServiceCode::FailedPrecondition);
            assert_eq!(error.stable_code, expected_code);
            assert!(!error.retryable);
        }
        assert!(accept_event_ingest(EventIngest::Inserted).is_ok());
        assert!(accept_event_ingest(EventIngest::Duplicate).is_ok());
    }

    #[test]
    fn block_terminal_must_match_observed_range_and_task_kind() {
        let payload = TaskPayload::BlockValidation(BlockValidationPayload {
            selection: sbgh_fleet::BlockValidationSelection::Range {
                range: InclusiveRange { start: 10, end: 19 },
            },
            timeout_secs: 60,
        });
        let outcome = TerminalOutcome::Completed {
            summary: serde_json::json!({}),
            block_validation: Some(BlockValidationResult {
                valid: true,
                checked_blocks: 10,
                invalid_blocks: Vec::new(),
                chainstate_origin: "vg/mainnet-2026-07-28".into(),
                observed: sbgh_fleet::ObservedValidationIndex {
                    pre_nakamoto_count: 10,
                    nakamoto_count: 21,
                },
                resolved_range: InclusiveRange { start: 10, end: 19 },
                segments: vec![sbgh_fleet::ValidationEpochSegment {
                    epoch: ValidationEpoch::Nakamoto,
                    global_range: InclusiveRange { start: 10, end: 19 },
                    local_range: InclusiveRange { start: 0, end: 9 },
                }],
                shard_count: 2,
                max_concurrency: 2,
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
        let mut missing_coverage = outcome.clone();
        if let TerminalOutcome::Completed {
            block_validation: Some(result), ..
        } = &mut missing_coverage
        {
            result.resolved_range.end = 18;
        }
        assert!(validate_terminal_for_assignment(&payload, &missing_coverage).is_err());
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
