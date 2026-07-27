use std::collections::VecDeque;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, ensure};
use sbgh_driver::{
    ArtifactSink, BenchmarkRunContext, BenchmarkTask, ExecutionContext, ExecutionPlacement,
    ExecutionRequest, ExecutionTask, RepositoryCredential, Terminal, WorkerEvent,
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

use crate::block_validation;
use crate::config::WorkerConfig;
use crate::remote_artifacts::RemoteArtifactSink;
use crate::transport::{FleetApiError, FleetClient};
use crate::{WorkerRuntime, build_binary_cache};

const EVENT_BUFFER_CAPACITY: usize = 256;

pub async fn run(config: WorkerConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    if config
        .capabilities
        .contains(&sbgh_proto::WorkerCapability::BlockValidation)
    {
        let block = config
            .block_validation
            .as_ref()
            .context("validated block-validation config is absent")?;
        let dataset = config
            .resources
            .dataset
            .as_ref()
            .context("validated block-validation dataset is absent")?;
        block_validation::verify_host(block, dataset)
            .await
            .context("proving block-validation dataset and CoW isolation")?;
    }
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
                        && accepted.assignment.trace_id == offer.trace_id,
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

async fn execute_assignment(
    config: &WorkerConfig,
    client: &FleetClient,
    assignment: sbgh_proto::Assignment,
    heartbeat_interval: Duration,
    lease_ttl: Duration,
    shutdown: &CancellationToken,
) -> anyhow::Result<bool> {
    let attempt_cancel = shutdown.child_token();
    let local_artifact_root = match &assignment.payload {
        TaskPayload::Benchmark(_) | TaskPayload::BuildOnly => config
            .libvirt
            .as_ref()
            .context("benchmark/build assignment received without local libvirt config")?
            .paths
            .results_archive_dir
            .join("fleet"),
        TaskPayload::BlockValidation(_) => config
            .block_validation
            .as_ref()
            .context("block-validation assignment received without local config")?
            .workspace_root
            .join("artifacts"),
    };
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
        CredentialFetch::Ready(repository_token) => match &assignment.payload {
            TaskPayload::Benchmark(_) | TaskPayload::BuildOnly => {
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
            TaskPayload::BlockValidation(payload) => {
                reliable
                    .phase("dataset_verified", started.elapsed())
                    .await?;
                ensure!(
                    config
                        .resources
                        .dataset
                        .as_ref()
                        == Some(&payload.dataset),
                    "assignment dataset identity differs from registered worker dataset"
                );
                let block_config = config
                    .block_validation
                    .as_ref()
                    .context("block-validation assignment received without local config")?;
                reliable
                    .phase("probe", started.elapsed())
                    .await?;
                let (block_progress_tx, block_progress_rx) = mpsc::unbounded_channel();
                let block_request = block_validation::BlockExecutionRequest {
                    repository: &assignment.context.repository,
                    commit: &assignment.context.commit,
                    repository_token: Some(repository_token.0.as_str()),
                    payload,
                    job_id: assignment.context.job_id,
                };
                let execution = block_validation::execute(
                    block_config,
                    &block_request,
                    &attempt_cancel,
                    block_progress_tx,
                );
                let result = drive_block_execution(
                    client,
                    &assignment,
                    &attempt_cancel,
                    block_progress_rx,
                    execution,
                )
                .await?;
                match result {
                    Driven::Finished(Ok(execution)) => {
                        let archive = archive_block_artifacts(
                            assignment.context.job_id,
                            artifacts.as_ref(),
                            &execution.artifacts,
                        )
                        .await;
                        match archive {
                            Ok(()) => {
                                let result = execution.result;
                                reliable
                                    .phase("reduced", started.elapsed())
                                    .await?;
                                TerminalOutcome::Completed {
                                    summary: serde_json::json!({
                                        "task": "block_validation",
                                        "valid": result.valid,
                                        "checked_blocks": result.checked_blocks,
                                        "invalid_block_count": result.invalid_blocks.len(),
                                    }),
                                    block_validation: Some(result),
                                }
                            }
                            Err(error) => TerminalOutcome::Failed {
                                error: format!("{error:#}"),
                                summary: None,
                                retryable: true,
                            },
                        }
                    }
                    Driven::Finished(Err(error)) => {
                        let mut message = format!("{error:#}");
                        let diagnostics =
                            block_diagnostic_paths(block_config, assignment.context.job_id).await;
                        if let Err(archive_error) = archive_block_artifacts(
                            assignment.context.job_id,
                            artifacts.as_ref(),
                            &diagnostics,
                        )
                        .await
                        {
                            message.push_str(&format!(
                                "; diagnostic upload also failed: {archive_error:#}"
                            ));
                        }
                        TerminalOutcome::Failed {
                            error: message,
                            summary: None,
                            retryable: true,
                        }
                    }
                    Driven::Cancelled => TerminalOutcome::Cancelled {
                        reason: "orchestrator requested cancellation".into(),
                    },
                }
            }
        },
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
    if matches!(assignment.payload, TaskPayload::BlockValidation(_)) {
        let block = config
            .block_validation
            .as_ref()
            .context("block-validation assignment lost its local config")?;
        if !block_validation::cleanup(block, assignment.context.job_id).await {
            tracing::warn!(
                job_id = %assignment.context.job_id,
                attempt_id = %assignment.identity.attempt_id,
                "accepted block-validation attempt left local diagnostic files for operator cleanup"
            );
        }
    }
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

async fn archive_block_artifacts(
    job_id: Uuid,
    sink: &dyn ArtifactSink,
    paths: &[PathBuf],
) -> anyhow::Result<()> {
    for path in paths {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .context("block-validation artifact has no UTF-8 filename")?;
        let key = format!("{job_id}/block-validation/{name}");
        sink.put(&key, path)
            .await
            .with_context(|| format!("uploading block-validation artifact {name}"))?;
    }
    Ok(())
}

async fn block_diagnostic_paths(
    config: &crate::config::BlockValidationConfig,
    job_id: Uuid,
) -> Vec<PathBuf> {
    let root = config
        .workspace_root
        .join(job_id.to_string());
    let Ok(mut entries) = tokio::fs::read_dir(root).await else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("shard-") && name.ends_with(".log"))
        {
            paths.push(path);
        }
    }
    paths.sort();
    paths
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
        .context("benchmark/build assignment received without [libvirt]")?;
    let cache = config
        .binary_cache
        .as_ref()
        .and_then(build_binary_cache);
    let shell = Arc::new(SystemShell::new(&libvirt.paths.sudo_binary));
    let built = WorkerRuntime::libvirt(libvirt, shell, artifacts, cache);
    let request = execution_request(assignment, repository_token)?;
    let (events_tx, mut events_rx) = mpsc::channel(64);
    let runtime = built.runtime;
    let execution_cancel = cancel.clone();
    let execution = tokio::spawn(async move {
        runtime
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
                    let outcome = match terminal {
                        Terminal::Completed { summary } => TerminalOutcome::Completed {
                            summary,
                            block_validation: None,
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
        TaskPayload::BlockValidation(_) => anyhow::bail!("block validation is not a driver task"),
    };
    Ok(ExecutionRequest {
        context: ExecutionContext {
            job_id: assignment.context.job_id,
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

enum Driven<T> {
    Finished(T),
    Cancelled,
}

async fn drive_block_execution<F, T>(
    client: &FleetClient,
    assignment: &sbgh_proto::Assignment,
    cancel: &CancellationToken,
    mut progress: mpsc::UnboundedReceiver<block_validation::BlockProgress>,
    future: F,
) -> anyhow::Result<Driven<T>>
where
    F: Future<Output = T>,
{
    tokio::pin!(future);
    let mut cancelling = false;
    let mut progress_seq = 0_u64;
    loop {
        tokio::select! {
            result = &mut future => {
                return Ok(if cancelling || cancel.is_cancelled() {
                    Driven::Cancelled
                } else {
                    Driven::Finished(result)
                });
            }
            () = cancel.cancelled(), if !cancelling => {
                cancelling = true;
            }
            Some(update) = progress.recv() => {
                progress_seq = progress_seq.saturating_add(1);
                let _ = client.progress(&ProgressRequest {
                    protocol_version: PROTOCOL_VERSION,
                    identity: assignment.identity.clone(),
                    trace_id: assignment.trace_id,
                    progress_seq,
                    update: ProgressUpdate {
                        workflow_step: "run".into(),
                        run_index: 0,
                        requested_run_count: 1,
                        phase: "validate".into(),
                        progress: update.completed_shards,
                        total: Some(update.total_shards),
                        message: Some(format!("{} blocks checked", update.checked_blocks)),
                    },
                }).await;
            }
        }
    }
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
                .cleanup_by_job_id(&obligation.job_id.to_string())
                .await;
        }
        if let Some(block) = &config.block_validation {
            cleaned &= block_validation::cleanup(block, obligation.job_id).await;
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
    use super::{has_api_code, is_non_retryable, retry_delay};
    use crate::transport::FleetApiError;
    use std::time::Duration;

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
}
