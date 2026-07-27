use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use sbgh_driver::{
    ArtifactSink, BenchmarkRunContext, BenchmarkTask, BlockValidationTaskSpec, DatasetIdentity,
    ExecutionContext, ExecutionPlacement, ExecutionRequest, ExecutionTask, InclusiveRange,
    RepositoryCredential, Terminal, ValidationEpoch, WorkerEvent,
};
use sbgh_libvirt::SystemShell;
use sbgh_proto::{
    AcceptOfferRequest, CompleteAttemptRequest, DesiredState, HeartbeatRequest, PROTOCOL_VERSION,
    PollRequest, PollResponse, ProgressRequest, ProgressUpdate, RegisterSessionRequest,
    ReliableEventEnvelope, ReliableEventPayload, RepositoryCredentialRequest, RepositoryToken,
    TaskPayload, TerminalOutcome, Validate,
};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::remote_artifacts::RemoteArtifactSink;
use crate::transport::{FleetApiError, FleetClient};
use crate::{WorkerRuntime, build_binary_cache};

const EVENT_BUFFER_CAPACITY: usize = 256;

/// Validate the local block-validation sandbox, sealed dataset, and current
/// thin-pool admission without registering a worker session.
pub async fn preflight_local_execution(config: &WorkerConfig) -> anyhow::Result<()> {
    if !config
        .capabilities
        .contains(&sbgh_proto::WorkerCapability::BlockValidation)
    {
        return Ok(());
    }
    let libvirt = config
        .libvirt
        .clone()
        .context("block_validation capability requires [libvirt]")?;
    let dataset = config
        .resources
        .dataset
        .as_ref()
        .context("block_validation capability requires resources.dataset")?;
    let driver = sbgh_libvirt::LibvirtDriver::new(
        libvirt.clone(),
        Arc::new(SystemShell::new(&libvirt.paths.sudo_binary)),
        Arc::new(CleanupArtifactSink),
        config
            .binary_cache
            .as_ref()
            .and_then(build_binary_cache)
            .map(|cache| cache as Arc<dyn sbgh_driver::BinaryCacheStore>),
    );
    driver
        .preflight_block_validation(&driver_dataset_identity(dataset))
        .await
        .context("validating libvirt block-validation profile and sealed dataset")
}

fn driver_dataset_identity(dataset: &sbgh_proto::DatasetIdentity) -> DatasetIdentity {
    DatasetIdentity {
        generation: dataset.generation.clone(),
        network: dataset.network.clone(),
        format_version: dataset.format_version.clone(),
        covered_start: dataset.covered_start,
        covered_end: dataset.covered_end,
        manifest_sha256: dataset
            .manifest_sha256
            .clone(),
    }
}

fn driver_block_result_to_wire(
    result: sbgh_driver::BlockValidationOutput,
) -> sbgh_proto::BlockValidationResult {
    sbgh_proto::BlockValidationResult {
        valid: result.valid,
        checked_blocks: result.checked_blocks,
        invalid_blocks: result
            .invalid_blocks
            .into_iter()
            .map(|invalid| sbgh_proto::InvalidBlock {
                shard: invalid.shard,
                block: invalid.block,
                reason: invalid.reason,
            })
            .collect(),
        dataset: sbgh_proto::DatasetIdentity {
            generation: result.dataset.generation,
            network: result.dataset.network,
            format_version: result.dataset.format_version,
            covered_start: result.dataset.covered_start,
            covered_end: result.dataset.covered_end,
            manifest_sha256: result.dataset.manifest_sha256,
        },
    }
}

pub async fn run(config: WorkerConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    preflight_local_execution(&config).await?;
    let client = FleetClient::build(
        &config.orchestrator_url,
        &config.client_certificate,
        &config.client_private_key,
        &config.server_ca_certificate,
    )?;
    let session_id = Uuid::new_v4();
    let registration = client
        .register(&RegisterSessionRequest {
            protocol_version: PROTOCOL_VERSION,
            worker_id: config.worker_id,
            worker_session_id: session_id,
            software_version: env!("CARGO_PKG_VERSION").into(),
            advertised_capabilities: config.capabilities.clone(),
            resources: config.resources.clone(),
        })
        .await
        .context("registering worker session")?;
    ensure!(
        registration.protocol_version == PROTOCOL_VERSION,
        "orchestrator returned a mismatched protocol version"
    );
    cleanup_obligations(&config, &client, session_id).await?;
    let mut backoff = Duration::from_millis(250);
    let mut draining = false;
    while !shutdown.is_cancelled() && !draining {
        let poll = client
            .poll(&PollRequest {
                protocol_version: PROTOCOL_VERSION,
                worker_session_id: session_id,
            })
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
                        protocol_version: PROTOCOL_VERSION,
                        identity: offer.identity.clone(),
                    })
                    .await
                {
                    Ok(accepted) => accepted,
                    Err(error) if has_api_code(&error, "stale_attempt") => {
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
                        && sbgh_proto::OfferRequirements::from(&accepted.assignment.payload)
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

async fn admit_offer(config: &WorkerConfig, offer: &sbgh_proto::WorkOffer) -> anyhow::Result<()> {
    anyhow::ensure!(
        config
            .capabilities
            .contains(&offer.capability),
        "orchestrator offered an unadvertised capability"
    );
    match &offer.requirements {
        sbgh_proto::OfferRequirements::Benchmark => {
            anyhow::ensure!(
                offer.capability == sbgh_proto::WorkerCapability::Benchmark,
                "offer capability/requirements mismatch"
            );
        }
        sbgh_proto::OfferRequirements::BuildOnly => {
            anyhow::ensure!(
                offer.capability == sbgh_proto::WorkerCapability::BuildOnly,
                "offer capability/requirements mismatch"
            );
        }
        sbgh_proto::OfferRequirements::BlockValidation {
            dataset,
            requested_shards,
            max_concurrency,
        } => {
            anyhow::ensure!(
                offer.capability == sbgh_proto::WorkerCapability::BlockValidation,
                "offer capability/requirements mismatch"
            );
            let profile = config
                .libvirt
                .as_ref()
                .and_then(|libvirt| {
                    libvirt
                        .block_validation
                        .as_ref()
                })
                .context("block-validation offer has no local sandbox profile")?;
            anyhow::ensure!(
                config
                    .resources
                    .dataset
                    .as_ref()
                    == Some(dataset),
                "block-validation offer requests a different dataset generation"
            );
            anyhow::ensure!(
                *requested_shards > 0 && *requested_shards <= profile.max_shards,
                "block-validation offer exceeds local shard limit"
            );
            anyhow::ensure!(
                *max_concurrency > 0 && *max_concurrency <= profile.max_concurrency,
                "block-validation offer exceeds local concurrency limit"
            );
            // Re-check sealed origin and current Data%/Meta% immediately before
            // acceptance; registration-time health is not a durable lease.
            preflight_local_execution(config).await?;
        }
    }
    Ok(())
}

async fn execute_assignment(
    config: &WorkerConfig,
    client: &FleetClient,
    assignment: sbgh_proto::Assignment,
    heartbeat_interval: Duration,
    lease_ttl: Duration,
    shutdown: &CancellationToken,
) -> anyhow::Result<bool> {
    let attempt_cancel = shutdown.child_token();
    let local_artifact_root = config
        .libvirt
        .as_ref()
        .context("sandboxed assignment received without local libvirt config")?
        .paths
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
    let outcome_digest = sbgh_proto::payload_digest(&terminal)?;
    let terminal_event = reliable
        .send(ReliableEventPayload::Terminal { outcome_digest })
        .await?;
    let manifest = artifacts.manifest().await;
    let completion = CompleteAttemptRequest {
        protocol_version: PROTOCOL_VERSION,
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
    assignment: &sbgh_proto::Assignment,
    cancel: &CancellationToken,
) -> anyhow::Result<CredentialFetch> {
    let request = RepositoryCredentialRequest {
        protocol_version: PROTOCOL_VERSION,
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
    identity: sbgh_proto::AttemptIdentity,
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
                    protocol_version: PROTOCOL_VERSION,
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
    assignment: &sbgh_proto::Assignment,
    repository_token: &RepositoryToken,
    artifacts: Arc<RemoteArtifactSink>,
    client: &FleetClient,
    cancel: &CancellationToken,
    reliable: &ReliableSender,
) -> anyhow::Result<TerminalOutcome> {
    let libvirt = config
        .libvirt
        .clone()
        .context("sandboxed assignment received without [libvirt]")?;
    let cache = config
        .binary_cache
        .as_ref()
        .and_then(build_binary_cache);
    let shell = Arc::new(SystemShell::new(&libvirt.paths.sudo_binary));
    let built = WorkerRuntime::libvirt(libvirt, shell, artifacts, cache);
    let request = execution_request(assignment, repository_token)?;
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
                        protocol_version: PROTOCOL_VERSION,
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
    assignment: &sbgh_proto::Assignment,
    repository_token: &RepositoryToken,
) -> anyhow::Result<ExecutionRequest> {
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
            ExecutionTask::BlockValidation(BlockValidationTaskSpec {
                dataset: driver_dataset_identity(&payload.dataset),
                epoch: match payload.epoch {
                    sbgh_proto::ValidationEpoch::PreNakamoto => ValidationEpoch::PreNakamoto,
                    sbgh_proto::ValidationEpoch::Nakamoto => ValidationEpoch::Nakamoto,
                },
                range: InclusiveRange {
                    start: payload.range.start,
                    end: payload.range.end,
                },
                requested_shards: payload.requested_shards,
                max_concurrency: payload.max_concurrency,
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
        placement: ExecutionPlacement {
            vcpu_cpuset: assignment.vcpu_cpuset.clone(),
        },
    })
}

struct ReliableSender {
    client: FleetClient,
    identity: sbgh_proto::AttemptIdentity,
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
        identity: sbgh_proto::AttemptIdentity,
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
            protocol_version: PROTOCOL_VERSION,
            identity: self.identity.clone(),
            trace_id: self.trace_id,
            reliable_seq: state.next_seq,
            payload_digest: sbgh_proto::payload_digest(&payload)?,
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
) -> anyhow::Result<sbgh_proto::CompleteAttemptResponse> {
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
        if let Some(libvirt) = &config.libvirt {
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
        .find_map(|cause| cause.downcast_ref::<FleetApiError>())
        .is_some_and(|error| !error.retryable)
}

fn has_api_code(error: &anyhow::Error, code: &str) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<FleetApiError>())
        .is_some_and(|error| error.code == code)
}

fn retry_delay(error: &anyhow::Error, fallback: Duration) -> Duration {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<FleetApiError>())
        .and_then(|error| error.retry_after)
        .unwrap_or_else(|| jitter(fallback))
        .min(Duration::from_secs(30))
}

#[cfg(test)]
mod tests {
    use super::{admit_offer, has_api_code, is_non_retryable, retry_delay};
    use crate::WorkerConfig;
    use crate::transport::FleetApiError;
    use sbgh_proto::{AttemptIdentity, LeaseToken, OfferRequirements, WorkOffer, WorkerCapability};
    use std::path::Path;
    use std::time::Duration;
    use uuid::Uuid;

    fn api_error(code: &str, retryable: bool, retry_after: Option<Duration>) -> anyhow::Error {
        anyhow::Error::new(FleetApiError {
            path: "/v1/event".into(),
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
            let error = api_error(code, false, None);
            assert!(
                is_non_retryable(&error),
                "{code} must remain non-retryable through anyhow context"
            );
            assert!(has_api_code(&error, code));
        }
        assert!(!is_non_retryable(&api_error("temporary", true, None)));
    }

    #[test]
    fn server_retry_delay_is_bounded() {
        assert_eq!(
            retry_delay(
                &api_error("busy", true, Some(Duration::from_secs(300))),
                Duration::from_millis(10),
            ),
            Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn oversized_block_offer_is_rejected_before_sandbox_preflight_or_acceptance() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config =
            WorkerConfig::load(&root.join("config.example.worker-block-validation.toml")).unwrap();
        let dataset = config
            .resources
            .dataset
            .clone()
            .unwrap();
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
            requirements: OfferRequirements::BlockValidation {
                dataset,
                requested_shards: 49,
                max_concurrency: 48,
            },
            payload_hash: "ab".repeat(32),
            offer_expires_at_ms: i64::MAX,
        };
        let error = admit_offer(&config, &offer)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("shard limit")
        );
    }
}
