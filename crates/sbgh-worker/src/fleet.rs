use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use sbgh_driver::{
    ArtifactSink, BenchmarkRunContext, BenchmarkTask, BlockValidationSelection,
    BlockValidationTaskSpec, ExecutionContext, ExecutionPlacement, ExecutionRequest, ExecutionTask,
    InclusiveRange, RepositoryCredential, Terminal, ValidationEpoch, WorkerEvent,
};
use sbgh_fleet::{
    AcceptOfferRequest, CompleteAttemptRequest, DesiredState, HeartbeatRequest, PROTOCOL_VERSION,
    PollRequest, PollResponse, ProgressRequest, ProgressUpdate, RegisterSessionRequest,
    RegistrationCheckRequest, RegistrationCheckResponse, ReliableEventEnvelope,
    ReliableEventPayload, RepositoryCredentialRequest, RepositoryToken, ResourceFacts, TaskPayload,
    TerminalOutcome, Validate,
};
use sbgh_libvirt::SystemShell;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::remote_artifacts::RemoteArtifactSink;
use crate::transport::{FleetClient, FleetClientError};
use crate::{HostResources, WorkerRuntime, build_binary_cache};

const EVENT_BUFFER_CAPACITY: usize = 256;
const MAX_CLOCK_SKEW_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetCheckReport {
    pub registration: RegistrationCheckResponse,
    pub clock_skew_ms: u64,
}

/// Validate every advertised local sandbox recipe before registering a worker
/// session.
pub async fn preflight_local_execution(config: &WorkerConfig) -> anyhow::Result<()> {
    let driver = preflight_driver(config)?;
    let capabilities = config.advertised_capabilities();
    if capabilities.contains(&sbgh_fleet::WorkerCapability::Benchmark) {
        driver
            .preflight_benchmark()
            .await
            .context("validating benchmark sandbox and immutable origin")?;
    } else if capabilities.contains(&sbgh_fleet::WorkerCapability::BuildOnly) {
        driver
            .preflight_build()
            .await
            .context("validating build-only sandbox")?;
    }
    if capabilities.contains(&sbgh_fleet::WorkerCapability::BlockValidation) {
        preflight_block_validation(&driver).await?;
    }
    Ok(())
}

fn preflight_driver(config: &WorkerConfig) -> anyhow::Result<sbgh_libvirt::LibvirtDriver> {
    let libvirt = config.libvirt_config();
    Ok(sbgh_libvirt::LibvirtDriver::new(
        libvirt.clone(),
        Arc::new(SystemShell::new(&libvirt.paths.sudo_binary)),
        Arc::new(CleanupArtifactSink),
        config
            .binary_cache
            .as_ref()
            .and_then(build_binary_cache)
            .map(|cache| cache as Arc<dyn sbgh_driver::BinaryCacheStore>),
    ))
}

async fn preflight_block_validation(driver: &sbgh_libvirt::LibvirtDriver) -> anyhow::Result<()> {
    driver
        .preflight_block_validation()
        .await
        .context("validating block-validation sandbox and local read-only origin")
}

fn driver_block_result_to_wire(
    result: sbgh_driver::BlockValidationOutput,
) -> sbgh_fleet::BlockValidationResult {
    sbgh_fleet::BlockValidationResult {
        valid: result.valid,
        checked_blocks: result.checked_blocks,
        invalid_blocks: result
            .invalid_blocks
            .into_iter()
            .map(|invalid| sbgh_fleet::InvalidBlock {
                shard: invalid.shard,
                block: invalid.block,
                reason: invalid.reason,
            })
            .collect(),
        chainstate_origin: result.chainstate_origin,
        observed: sbgh_fleet::ObservedValidationIndex {
            pre_nakamoto_count: result
                .observed
                .pre_nakamoto_count,
            nakamoto_count: result.observed.nakamoto_count,
        },
        resolved_range: sbgh_fleet::InclusiveRange {
            start: result.resolved_range.start,
            end: result.resolved_range.end,
        },
        segments: result
            .segments
            .into_iter()
            .map(|segment| sbgh_fleet::ValidationEpochSegment {
                epoch: match segment.epoch {
                    ValidationEpoch::PreNakamoto => sbgh_fleet::ValidationEpoch::PreNakamoto,
                    ValidationEpoch::Nakamoto => sbgh_fleet::ValidationEpoch::Nakamoto,
                },
                global_range: sbgh_fleet::InclusiveRange {
                    start: segment.global_range.start,
                    end: segment.global_range.end,
                },
                local_range: sbgh_fleet::InclusiveRange {
                    start: segment.local_range.start,
                    end: segment.local_range.end,
                },
            })
            .collect(),
        shard_count: result.shard_count,
        max_concurrency: result.max_concurrency,
    }
}

pub async fn run(
    config: WorkerConfig,
    resources: HostResources,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    config
        .validate_host_resources(&resources)
        .context("validating execution profiles against discovered host resources")?;
    preflight_local_execution(&config).await?;
    let session_id = Uuid::new_v4();
    let client = FleetClient::build(&config.orchestrator_url, &config.identity_private_key)?;
    let request_started_ms = now_millis();
    let registration = client
        .register(&registration_request(&config, resources.facts().clone(), session_id))
        .await
        .context("registering worker session")?;
    let request_finished_ms = now_millis();
    let clock_skew_ms =
        validate_registration(&registration, request_started_ms, request_finished_ms)?;
    tracing::info!(
        session_id = %session_id,
        protocol_version = registration.protocol_version,
        clock_skew_ms,
        "worker session timing validated"
    );
    cleanup_obligations(&config, &client, session_id).await?;
    let mut backoff = Duration::from_millis(250);
    let mut draining = false;
    while !shutdown.is_cancelled() && !draining {
        let poll = client
            .poll(&PollRequest { worker_session_id: session_id })
            .await;
        let response = match poll {
            Ok(response) => {
                backoff = Duration::from_millis(250);
                response
            }
            Err(error) => {
                if is_non_retryable(&error) {
                    return Err(error).context("worker poll rejected permanently");
                }
                tracing::warn!(%error, "worker poll failed; reconnecting");
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(retry_delay(&error, backoff)) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(15));
                continue;
            }
        };
        match response {
            PollResponse::NoWork { retry_after_ms } => {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(Duration::from_millis(retry_after_ms.min(30_000))) => {}
                }
            }
            PollResponse::Drain => {
                draining = true;
            }
            PollResponse::Offer { offer } => {
                admit_offer(&config, &offer).await?;
                let accepted = match client
                    .accept(&AcceptOfferRequest {
                        identity: offer.identity.clone(),
                    })
                    .await
                {
                    Ok(accepted) => accepted,
                    Err(error) if has_fleet_code(&error, "stale_attempt") => {
                        tracing::info!(
                            attempt_id = %offer.identity.attempt_id,
                            "work offer became stale before acceptance; resuming polling"
                        );
                        continue;
                    }
                    Err(error) => return Err(error).context("accepting work offer"),
                };
                accepted
                    .assignment
                    .validate()?;
                ensure!(
                    accepted
                        .assignment
                        .payload_hash
                        == offer.payload_hash
                        && accepted.assignment.trace_id == offer.trace_id
                        && sbgh_fleet::OfferRequirements::from(&accepted.assignment.payload)
                            == offer.requirements,
                    "accepted assignment does not match its offer"
                );
                draining = execute_assignment(
                    &config,
                    &client,
                    accepted.assignment,
                    Duration::from_millis(registration.heartbeat_interval_ms),
                    Duration::from_millis(registration.lease_ttl_ms),
                    &shutdown,
                )
                .await?;
            }
        }
    }
    if let Err(error) = client
        .deregister(session_id)
        .await
    {
        tracing::warn!(%error, "worker deregistration failed");
    }
    Ok(())
}

/// Verify the production TLS 1.3 connector and receive a standard gRPC health
/// response without requiring registry enrollment or creating a session.
pub async fn check_connectivity(config: &WorkerConfig) -> anyhow::Result<()> {
    FleetClient::build(&config.orchestrator_url, &config.identity_private_key)?
        .check_health()
        .await
}

/// Evaluate registration authorization and policy without creating, replacing,
/// or deregistering a worker session.
pub async fn check_registration(
    config: &WorkerConfig,
    resources: HostResources,
) -> anyhow::Result<FleetCheckReport> {
    config
        .validate_host_resources(&resources)
        .context("validating execution profiles against discovered host resources")?;
    let client = FleetClient::build(&config.orchestrator_url, &config.identity_private_key)?;
    let request = registration_check_request(config, resources.facts().clone());
    let request_started_ms = now_millis();
    let registration = client
        .check_registration(&request)
        .await
        .context("checking worker registration authorization")?;
    let request_finished_ms = now_millis();
    let clock_skew_ms = validate_registration_check(
        &request,
        &registration,
        request_started_ms,
        request_finished_ms,
    )?;
    Ok(FleetCheckReport { registration, clock_skew_ms })
}

fn registration_request(
    config: &WorkerConfig,
    resources: ResourceFacts,
    worker_session_id: Uuid,
) -> RegisterSessionRequest {
    let facts = registration_check_request(config, resources);
    RegisterSessionRequest {
        protocol_version: facts.protocol_version,
        worker_session_id,
        software_version: facts.software_version,
        advertised_capabilities: facts.advertised_capabilities,
        resources: facts.resources,
    }
}

fn registration_check_request(
    config: &WorkerConfig,
    resources: ResourceFacts,
) -> RegistrationCheckRequest {
    RegistrationCheckRequest {
        protocol_version: PROTOCOL_VERSION,
        software_version: env!("CARGO_PKG_VERSION").into(),
        advertised_capabilities: config.advertised_capabilities(),
        resources,
    }
}

fn validate_registration(
    registration: &sbgh_fleet::RegisterSessionResponse,
    request_started_ms: i64,
    request_finished_ms: i64,
) -> anyhow::Result<u64> {
    validate_server_timing(
        registration.protocol_version,
        registration.heartbeat_interval_ms,
        registration.lease_ttl_ms,
        registration.server_time_ms,
        request_started_ms,
        request_finished_ms,
    )
}

fn validate_registration_check(
    request: &RegistrationCheckRequest,
    registration: &RegistrationCheckResponse,
    request_started_ms: i64,
    request_finished_ms: i64,
) -> anyhow::Result<u64> {
    ensure!(
        !registration
            .worker_id
            .is_nil(),
        "orchestrator returned a nil worker identity"
    );
    ensure!(
        !registration
            .effective_capabilities
            .is_empty()
            && registration
                .effective_capabilities
                .is_subset(&request.advertised_capabilities),
        "orchestrator returned invalid effective capabilities"
    );
    validate_server_timing(
        registration.protocol_version,
        registration.heartbeat_interval_ms,
        registration.lease_ttl_ms,
        registration.server_time_ms,
        request_started_ms,
        request_finished_ms,
    )
}

fn validate_server_timing(
    protocol_version: u16,
    heartbeat_interval_ms: u64,
    lease_ttl_ms: u64,
    server_time_ms: i64,
    request_started_ms: i64,
    request_finished_ms: i64,
) -> anyhow::Result<u64> {
    ensure!(
        protocol_version == PROTOCOL_VERSION,
        "orchestrator returned a mismatched protocol version"
    );
    ensure!(
        heartbeat_interval_ms > 0 && lease_ttl_ms > heartbeat_interval_ms,
        "orchestrator returned invalid fleet timing"
    );
    ensure!(server_time_ms > 0, "orchestrator returned an invalid server time");
    ensure!(
        request_finished_ms >= request_started_ms,
        "local system clock moved backwards during registration"
    );
    let local_midpoint_ms =
        i128::from(request_started_ms) + i128::from(request_finished_ms - request_started_ms) / 2;
    let clock_skew_ms = local_midpoint_ms.abs_diff(i128::from(server_time_ms));
    let clock_skew_ms = u64::try_from(clock_skew_ms).unwrap_or(u64::MAX);
    ensure!(
        clock_skew_ms <= MAX_CLOCK_SKEW_MS,
        "orchestrator clock differs from the worker by {clock_skew_ms}ms (maximum {MAX_CLOCK_SKEW_MS}ms)"
    );
    Ok(clock_skew_ms)
}

async fn admit_offer(config: &WorkerConfig, offer: &sbgh_fleet::WorkOffer) -> anyhow::Result<()> {
    anyhow::ensure!(
        config
            .advertised_capabilities()
            .contains(&offer.capability),
        "orchestrator offered an unadvertised capability"
    );
    match &offer.requirements {
        sbgh_fleet::OfferRequirements::Benchmark => {
            anyhow::ensure!(
                offer.capability == sbgh_fleet::WorkerCapability::Benchmark,
                "offer capability/requirements mismatch"
            );
        }
        sbgh_fleet::OfferRequirements::BuildOnly => {
            anyhow::ensure!(
                offer.capability == sbgh_fleet::WorkerCapability::BuildOnly,
                "offer capability/requirements mismatch"
            );
        }
        sbgh_fleet::OfferRequirements::BlockValidation => {
            anyhow::ensure!(
                offer.capability == sbgh_fleet::WorkerCapability::BlockValidation,
                "offer capability/requirements mismatch"
            );
            config
                .block_validation
                .as_ref()
                .context("block-validation offer has no local sandbox profile")?;
            // Re-check the immutable local origin and current pool health
            // immediately before acceptance;
            // registration-time health is not a durable lease.
            let driver = preflight_driver(config)?;
            preflight_block_validation(&driver).await?;
        }
    }
    Ok(())
}

async fn execute_assignment(
    config: &WorkerConfig,
    client: &FleetClient,
    assignment: sbgh_fleet::Assignment,
    heartbeat_interval: Duration,
    lease_ttl: Duration,
    shutdown: &CancellationToken,
) -> anyhow::Result<bool> {
    let attempt_cancel = shutdown.child_token();
    let local_artifact_root = config
        .workspace
        .results_archive_dir
        .join("fleet");
    let artifacts =
        RemoteArtifactSink::new(client.clone(), assignment.identity.clone(), local_artifact_root);
    let lease_lost = Arc::new(AtomicBool::new(false));
    let reliable = Arc::new(ReliableSender::new(
        client.clone(),
        assignment.identity.clone(),
        assignment.trace_id,
        lease_lost.clone(),
    ));
    let drain_requested = Arc::new(AtomicBool::new(false));
    let heartbeat = HeartbeatSupervisor::spawn(
        client.clone(),
        HeartbeatContext {
            identity: assignment.identity.clone(),
            heartbeat_interval,
            lease_ttl,
            cancel: attempt_cancel.clone(),
            reliable: reliable.clone(),
            drain_requested: drain_requested.clone(),
            lease_lost,
        },
    );
    let started = Instant::now();
    reliable
        .phase("accepted", started.elapsed())
        .await?;
    let credential = fetch_repository_credential(client, &assignment, &attempt_cancel).await?;

    let terminal = match credential {
        CredentialFetch::Cancelled => TerminalOutcome::Cancelled {
            reason: "orchestrator requested cancellation".into(),
        },
        CredentialFetch::Ready(repository_token) => {
            execute_driver(
                config,
                &assignment,
                &repository_token,
                artifacts.clone(),
                client,
                &attempt_cancel,
                &reliable,
            )
            .await?
        }
    };
    let outcome_digest = sbgh_fleet::payload_digest(&terminal)?;
    let terminal_event = reliable
        .send(ReliableEventPayload::Terminal { outcome_digest })
        .await?;
    let manifest = artifacts.manifest().await;
    let completion = CompleteAttemptRequest {
        identity: assignment.identity.clone(),
        trace_id: assignment.trace_id,
        terminal_reliable_seq: terminal_event.reliable_seq,
        terminal_payload_digest: terminal_event.payload_digest,
        outcome: terminal,
        artifacts: manifest,
    };
    completion.validate()?;
    let response = retry_terminal(client, &completion, shutdown).await?;
    ensure!(response.accepted, "orchestrator rejected terminal outcome");
    heartbeat.stop().await?;
    Ok(drain_requested.load(Ordering::Acquire))
}

enum CredentialFetch {
    Ready(RepositoryToken),
    Cancelled,
}

async fn fetch_repository_credential(
    client: &FleetClient,
    assignment: &sbgh_fleet::Assignment,
    cancel: &CancellationToken,
) -> anyhow::Result<CredentialFetch> {
    let request = RepositoryCredentialRequest {
        identity: assignment.identity.clone(),
    };
    let mut backoff = Duration::from_millis(250);
    loop {
        let credential = client.repository_credential(&request);
        tokio::pin!(credential);
        let result = tokio::select! {
            result = &mut credential => result,
            () = cancel.cancelled() => return Ok(CredentialFetch::Cancelled),
        };
        match result {
            Ok(response) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context("system clock before Unix epoch")?
                    .as_millis() as i64;
                ensure!(
                    response.expires_at_ms > now,
                    "orchestrator returned an expired repository credential"
                );
                return Ok(CredentialFetch::Ready(response.token));
            }
            Err(error) if is_non_retryable(&error) => {
                return Err(error).context("repository credential request rejected permanently");
            }
            Err(error) => {
                tracing::warn!(
                    attempt_id = %assignment.identity.attempt_id,
                    %error,
                    "repository credential unavailable; retrying"
                );
                tokio::select! {
                    () = cancel.cancelled() => return Ok(CredentialFetch::Cancelled),
                    () = tokio::time::sleep(retry_delay(
                        &error,
                        backoff,
                    )) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
}

struct HeartbeatContext {
    identity: sbgh_fleet::AttemptIdentity,
    heartbeat_interval: Duration,
    lease_ttl: Duration,
    cancel: CancellationToken,
    reliable: Arc<ReliableSender>,
    drain_requested: Arc<AtomicBool>,
    lease_lost: Arc<AtomicBool>,
}

async fn heartbeat_loop(client: FleetClient, context: HeartbeatContext) {
    let mut interval = tokio::time::interval(context.heartbeat_interval);
    let mut lease_deadline = Instant::now() + context.lease_ttl;
    loop {
        tokio::select! {
            () = context.cancel.cancelled() => return,
            _ = interval.tick() => {
                match client.heartbeat(&HeartbeatRequest {
                    identity: context.identity.clone(),
                    reliable_buffer_len: context.reliable.unacknowledged_len(),
                }).await {
                    Ok(heartbeat) => {
                        lease_deadline = Instant::now() + context.lease_ttl;
                        match heartbeat.desired_state {
                            DesiredState::Continue => {}
                            DesiredState::Cancel => context.cancel.cancel(),
                            DesiredState::Drain => {
                                context.drain_requested.store(true, Ordering::Release);
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            attempt_id = %context.identity.attempt_id,
                            %error,
                            "attempt heartbeat failed"
                        );
                        if Instant::now() >= lease_deadline {
                            tracing::error!(
                                attempt_id = %context.identity.attempt_id,
                                "lease confirmation expired; cancelling local execution"
                            );
                            context.lease_lost.store(true, Ordering::Release);
                            context.cancel.cancel();
                            return;
                        }
                    }
                }
            }
        }
    }
}

struct HeartbeatSupervisor {
    cancel: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl HeartbeatSupervisor {
    fn spawn(client: FleetClient, context: HeartbeatContext) -> Self {
        let cancel = context.cancel.clone();
        let handle = tokio::spawn(heartbeat_loop(client, context));
        Self { cancel, handle: Some(handle) }
    }

    async fn stop(mut self) -> anyhow::Result<()> {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .context("joining attempt heartbeat supervisor")?;
        }
        Ok(())
    }
}

impl Drop for HeartbeatSupervisor {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

async fn execute_driver(
    config: &WorkerConfig,
    assignment: &sbgh_fleet::Assignment,
    repository_token: &RepositoryToken,
    artifacts: Arc<RemoteArtifactSink>,
    client: &FleetClient,
    cancel: &CancellationToken,
    reliable: &ReliableSender,
) -> anyhow::Result<TerminalOutcome> {
    let libvirt = config.libvirt_config();
    let cache = config
        .binary_cache
        .as_ref()
        .and_then(build_binary_cache);
    let shell = Arc::new(SystemShell::new(&libvirt.paths.sudo_binary));
    let built = WorkerRuntime::libvirt(libvirt, shell, artifacts, cache);
    let request = execution_request(config, assignment, repository_token)?;
    let (events_tx, mut events_rx) = mpsc::channel(64);
    let runtime = built.runtime;
    let execution_runtime = runtime.clone();
    let execution_cancel = cancel.clone();
    let execution = tokio::spawn(async move {
        execution_runtime
            .run(request, events_tx, execution_cancel)
            .await;
    });
    let mut progress_seq = 0_u64;
    loop {
        tokio::select! {
            event = events_rx.recv() => match event {
                Some(WorkerEvent::Phase { label, elapsed }) => {
                    reliable.phase(&label.to_string(), elapsed).await?;
                }
                Some(WorkerEvent::Heartbeat { label, elapsed }) => {
                    tracing::debug!(
                        attempt_id = %assignment.identity.attempt_id,
                        phase = %label,
                        elapsed_ms = elapsed.as_millis(),
                        "execution heartbeat"
                    );
                }
                Some(WorkerEvent::Progress(progress)) => {
                    progress_seq = progress_seq.saturating_add(1);
                    let _ = client.progress(&ProgressRequest {
                        identity: assignment.identity.clone(),
                        trace_id: assignment.trace_id,
                        progress_seq,
                        update: ProgressUpdate {
                            workflow_step: progress.workflow_step.to_string(),
                            run_index: progress.run_index,
                            requested_run_count: progress.requested_run_count,
                            phase: progress.phase,
                            progress: progress.progress,
                            total: progress.total,
                            message: progress.message,
                        },
                    }).await;
                }
                Some(WorkerEvent::Finished(terminal)) => {
                    execution.await.context("joining local execution task")?;
                    if !runtime
                        .cleanup_attempt(
                            &assignment.context.job_id.to_string(),
                            &assignment.identity.attempt_id.to_string(),
                        )
                        .await
                    {
                        anyhow::bail!(
                            "attempt cleanup could not be verified; withholding terminal outcome \
                             so lease recovery can retry"
                        );
                    }
                    let outcome = match terminal {
                        Terminal::Completed { summary, block_validation } => TerminalOutcome::Completed {
                            summary,
                            block_validation: block_validation.map(driver_block_result_to_wire),
                        },
                        Terminal::Failed { error, summary } => TerminalOutcome::Failed {
                            error,
                            summary: Some(summary),
                            retryable: true,
                        },
                        Terminal::SetupError { error } => TerminalOutcome::Failed {
                            error,
                            summary: None,
                            retryable: true,
                        },
                        Terminal::Aborted => TerminalOutcome::Cancelled {
                            reason: "execution cancelled".into(),
                        },
                    };
                    return Ok(outcome);
                }
                None => anyhow::bail!("execution event channel closed before terminal"),
            }
        }
    }
}

fn execution_request(
    config: &WorkerConfig,
    assignment: &sbgh_fleet::Assignment,
    repository_token: &RepositoryToken,
) -> anyhow::Result<ExecutionRequest> {
    let vcpu_cpuset = local_vcpu_cpuset(
        config,
        &assignment.payload,
        assignment
            .vcpu_cpuset
            .as_deref(),
    )?;
    let task = match &assignment.payload {
        TaskPayload::Benchmark(payload) => ExecutionTask::Benchmark(BenchmarkTask {
            args: payload.effective_args.clone(),
            sqlite_seed_key: payload
                .sqlite_seed_key
                .clone(),
            shared_baseline_calibration: payload.shared_baseline_calibration,
            baseline_calibration_id: payload.baseline_calibration_id,
            run: BenchmarkRunContext {
                run_index: payload.run_index,
                requested_run_count: payload.requested_run_count,
            },
        }),
        TaskPayload::BuildOnly => ExecutionTask::BuildOnly,
        TaskPayload::BlockValidation(payload) => {
            let selection = match &payload.selection {
                sbgh_fleet::BlockValidationSelection::Recent { block_count } => {
                    BlockValidationSelection::Recent { block_count: *block_count }
                }
                sbgh_fleet::BlockValidationSelection::Full => BlockValidationSelection::Full,
                sbgh_fleet::BlockValidationSelection::Range { range } => {
                    BlockValidationSelection::Range {
                        range: InclusiveRange {
                            start: range.start,
                            end: range.end,
                        },
                    }
                }
            };
            ExecutionTask::BlockValidation(BlockValidationTaskSpec {
                selection,
                timeout_secs: payload.timeout_secs,
            })
        }
    };
    Ok(ExecutionRequest {
        context: ExecutionContext {
            job_id: assignment.context.job_id,
            attempt_id: assignment.identity.attempt_id,
            fencing_generation: assignment
                .identity
                .fencing_generation,
            repository: assignment
                .context
                .repository
                .clone(),
            commit: assignment
                .context
                .commit
                .clone(),
            repository_credential: Some(RepositoryCredential::new(repository_token.0.clone())),
        },
        task,
        placement: ExecutionPlacement { vcpu_cpuset },
    })
}

fn local_vcpu_cpuset(
    config: &WorkerConfig,
    payload: &TaskPayload,
    orchestrator_cpuset: Option<&str>,
) -> anyhow::Result<Option<String>> {
    ensure!(
        orchestrator_cpuset.is_none(),
        "orchestrator-supplied CPU placement is forbidden; VM placement is worker-owned"
    );
    Ok(match payload {
        TaskPayload::Benchmark(_) | TaskPayload::BuildOnly => Some(
            config
                .benchmark
                .as_ref()
                .context("benchmark assignment has no local worker profile")?
                .cpu_set
                .clone(),
        ),
        TaskPayload::BlockValidation(_) => config
            .block_validation
            .as_ref()
            .context("block-validation assignment has no local worker profile")?
            .cpu_set
            .clone(),
    })
}

struct ReliableSender {
    client: FleetClient,
    identity: sbgh_fleet::AttemptIdentity,
    trace_id: Uuid,
    state: Mutex<ReliableState>,
    buffer_len: AtomicU32,
    lease_lost: Arc<AtomicBool>,
}

struct ReliableState {
    next_seq: u64,
    unacknowledged: VecDeque<ReliableEventEnvelope>,
}

impl ReliableSender {
    fn new(
        client: FleetClient,
        identity: sbgh_fleet::AttemptIdentity,
        trace_id: Uuid,
        lease_lost: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client,
            identity,
            trace_id,
            state: Mutex::new(ReliableState {
                next_seq: 1,
                unacknowledged: VecDeque::new(),
            }),
            buffer_len: AtomicU32::new(0),
            lease_lost,
        }
    }

    fn unacknowledged_len(&self) -> u32 {
        self.buffer_len
            .load(Ordering::Relaxed)
    }

    async fn phase(&self, label: &str, elapsed: Duration) -> anyhow::Result<()> {
        self.send(ReliableEventPayload::Phase {
            label: label.into(),
            elapsed_ms: elapsed.as_millis() as u64,
        })
        .await?;
        Ok(())
    }

    async fn send(&self, payload: ReliableEventPayload) -> anyhow::Result<ReliableEventEnvelope> {
        let mut state = self.state.lock().await;
        ensure!(
            state.unacknowledged.len() < EVENT_BUFFER_CAPACITY,
            "reliable event resend buffer is full"
        );
        let envelope = ReliableEventEnvelope {
            identity: self.identity.clone(),
            trace_id: self.trace_id,
            reliable_seq: state.next_seq,
            payload_digest: sbgh_fleet::payload_digest(&payload)?,
            payload,
            worker_timestamp_ms: now_millis(),
        };
        state.next_seq += 1;
        state
            .unacknowledged
            .push_back(envelope.clone());
        self.buffer_len
            .store(state.unacknowledged.len() as u32, Ordering::Relaxed);
        let mut backoff = Duration::from_millis(100);
        loop {
            ensure!(
                !self
                    .lease_lost
                    .load(Ordering::Acquire),
                "lease confirmation expired while delivering reliable event"
            );
            let Some(next) = state.unacknowledged.front() else {
                break;
            };
            match self.client.event(next).await {
                Ok(ack) => {
                    while state
                        .unacknowledged
                        .front()
                        .is_some_and(|event| {
                            event.reliable_seq <= ack.highest_contiguous_reliable_seq
                        })
                    {
                        state
                            .unacknowledged
                            .pop_front();
                        self.buffer_len
                            .store(state.unacknowledged.len() as u32, Ordering::Relaxed);
                    }
                }
                Err(error) => {
                    if is_non_retryable(&error) {
                        return Err(error).context("reliable event rejected permanently");
                    }
                    tracing::warn!(
                        attempt_id = %self.identity.attempt_id,
                        %error,
                        "reliable event delivery failed; retrying same session"
                    );
                    tokio::time::sleep(retry_delay(&error, backoff)).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
        Ok(envelope)
    }
}

async fn retry_terminal(
    client: &FleetClient,
    completion: &CompleteAttemptRequest,
    shutdown: &CancellationToken,
) -> anyhow::Result<sbgh_fleet::CompleteAttemptResponse> {
    let mut backoff = Duration::from_millis(100);
    loop {
        match client
            .complete(completion)
            .await
        {
            Ok(response) => return Ok(response),
            Err(error) => {
                if is_non_retryable(&error) {
                    return Err(error).context("terminal submission rejected permanently");
                }
                tracing::warn!(
                    attempt_id = %completion.identity.attempt_id,
                    %error,
                    "terminal submission failed; retrying"
                );
                tokio::select! {
                    () = shutdown.cancelled() => return Err(error),
                    () = tokio::time::sleep(retry_delay(&error, backoff)) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
}

async fn cleanup_obligations(
    config: &WorkerConfig,
    client: &FleetClient,
    session_id: Uuid,
) -> anyhow::Result<()> {
    for obligation in client
        .cleanup(session_id)
        .await?
    {
        tracing::info!(
            obligation_id = obligation.id,
            attempt_id = %obligation.attempt_id,
            job_id = %obligation.job_id,
            reason = %obligation.reason,
            "cleaning orphaned worker resources"
        );
        let mut cleaned = true;
        if config.benchmark.is_some()
            || config
                .block_validation
                .is_some()
        {
            let libvirt = config.libvirt_config();
            let runtime = WorkerRuntime::libvirt(
                libvirt.clone(),
                Arc::new(SystemShell::new(&libvirt.paths.sudo_binary)),
                Arc::new(CleanupArtifactSink),
                config
                    .binary_cache
                    .as_ref()
                    .and_then(build_binary_cache),
            )
            .runtime;
            cleaned &= runtime
                .cleanup_attempt(
                    &obligation.job_id.to_string(),
                    &obligation
                        .attempt_id
                        .to_string(),
                )
                .await;
        }
        if cleaned {
            client
                .complete_cleanup(session_id, obligation.id)
                .await?;
        } else {
            anyhow::bail!("cleanup obligation {} could not be satisfied", obligation.id);
        }
    }
    Ok(())
}

struct CleanupArtifactSink;

#[async_trait::async_trait]
impl ArtifactSink for CleanupArtifactSink {
    async fn put(&self, _key: &str, _src: &Path) -> Option<u64> {
        None
    }

    async fn get(&self, _key: &str) -> std::io::Result<PathBuf> {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "cleanup has no artifact access"))
    }

    fn job_dir(&self, job_id: &str) -> PathBuf {
        PathBuf::from("/tmp/sbgh-cleanup").join(job_id)
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn jitter(duration: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let percent = 80 + nanos % 41;
    duration.mul_f64(f64::from(percent) / 100.0)
}

fn is_non_retryable(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<FleetClientError>())
        .is_some_and(|error| !error.retryable)
}

fn has_fleet_code(error: &anyhow::Error, code: &str) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<FleetClientError>())
        .is_some_and(|error| error.code == code)
}

fn retry_delay(error: &anyhow::Error, fallback: Duration) -> Duration {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<FleetClientError>())
        .and_then(|error| error.retry_after)
        .unwrap_or_else(|| jitter(fallback))
        .min(Duration::from_secs(30))
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_CLOCK_SKEW_MS, admit_offer, has_fleet_code, is_non_retryable, local_vcpu_cpuset,
        retry_delay, validate_server_timing,
    };
    use crate::WorkerConfig;
    use crate::transport::FleetClientError;
    use sbgh_fleet::{
        AttemptIdentity, BlockValidationPayload, BlockValidationSelection, LeaseToken,
        OfferRequirements, TaskPayload, WorkOffer, WorkerCapability,
    };
    use std::path::Path;
    use std::time::Duration;
    use uuid::Uuid;

    fn client_error(code: &str, retryable: bool, retry_after: Option<Duration>) -> anyhow::Error {
        anyhow::Error::new(FleetClientError {
            path: "PublishReliableEvent".into(),
            code: code.into(),
            message: "test response".into(),
            retryable,
            retry_after,
        })
        .context("reliable event delivery")
    }

    #[test]
    fn typed_stale_and_sequence_conflict_errors_stop_reliable_retries() {
        for code in ["stale_attempt", "event_sequence_conflict"] {
            let error = client_error(code, false, None);
            assert!(
                is_non_retryable(&error),
                "{code} must remain non-retryable through anyhow context"
            );
            assert!(has_fleet_code(&error, code));
        }
        assert!(!is_non_retryable(&client_error("temporary", true, None)));
    }

    #[test]
    fn server_retry_delay_is_bounded() {
        assert_eq!(
            retry_delay(
                &client_error("busy", true, Some(Duration::from_secs(300))),
                Duration::from_millis(10),
            ),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn registration_timing_checks_midpoint_clock_skew_and_lease_ordering() {
        assert_eq!(validate_server_timing(1, 1_000, 5_000, 10_050, 10_000, 10_100).unwrap(), 0);
        assert!(
            validate_server_timing(
                1,
                1_000,
                5_000,
                10_050 + MAX_CLOCK_SKEW_MS as i64 + 1,
                10_000,
                10_100,
            )
            .is_err()
        );
        assert!(validate_server_timing(1, 5_000, 5_000, 10_050, 10_000, 10_100).is_err());
    }

    #[test]
    fn execution_uses_worker_owned_cpu_placement_and_rejects_orchestrator_placement() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = WorkerConfig::load(&root.join("config.example.worker-combined.toml")).unwrap();

        assert_eq!(
            local_vcpu_cpuset(&config, &TaskPayload::BuildOnly, None).unwrap(),
            Some("0-3".into())
        );
        assert_eq!(
            local_vcpu_cpuset(
                &config,
                &TaskPayload::BlockValidation(BlockValidationPayload {
                    selection: BlockValidationSelection::Recent { block_count: 1 },
                    timeout_secs: 60,
                }),
                None,
            )
            .unwrap(),
            Some("0-47".into())
        );
        assert!(
            local_vcpu_cpuset(&config, &TaskPayload::BuildOnly, Some("48-49"))
                .unwrap_err()
                .to_string()
                .contains("worker-owned")
        );
    }

    #[tokio::test]
    async fn block_offer_rejects_a_capability_requirements_mismatch_before_preflight() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config =
            WorkerConfig::load(&root.join("config.example.worker-block-validation.toml")).unwrap();
        let offer = WorkOffer {
            identity: AttemptIdentity {
                worker_session_id: Uuid::new_v4(),
                attempt_id: Uuid::new_v4(),
                fencing_generation: 1,
                lease_token: LeaseToken("a".repeat(64)),
            },
            job_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            capability: WorkerCapability::BlockValidation,
            requirements: OfferRequirements::Benchmark,
            payload_hash: "ab".repeat(32),
            offer_expires_at_ms: i64::MAX,
        };
        let error = admit_offer(&config, &offer)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("capability/requirements mismatch")
        );
    }
}
