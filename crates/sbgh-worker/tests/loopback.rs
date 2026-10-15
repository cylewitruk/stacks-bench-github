#![cfg(unix)]

use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sbgh_fleet::{
    AttemptIdentity, LeaseToken, PollResponse, RegisterSessionResponse, RegistrationCheckResponse,
    WorkOffer, WorkerCapability,
};
use sbgh_proto::Wire;
use sbgh_proto::fleet::v1::worker_fleet_service_server::{
    WorkerFleetService, WorkerFleetServiceServer,
};
use sbgh_proto::fleet::v1::{
    AcceptRequest, AcceptResponse, CheckRegistrationRequest, CheckRegistrationResponse,
    CompleteAttemptRequest, CompleteAttemptResponse, CompleteCleanupRequest,
    CompleteCleanupResponse, DeregisterRequest, DeregisterResponse,
    FetchRepositoryCredentialRequest, FetchRepositoryCredentialResponse, GrantArtifactRequest,
    GrantArtifactResponse, HeartbeatRequest, HeartbeatResponse, ListCleanupRequest,
    ListCleanupResponse, PollRequest, PollResponse as WirePollResponse, PublishProgressRequest,
    PublishProgressResponse, PublishReliableEventRequest, PublishReliableEventResponse,
    RegisterRequest, RegisterResponse as WireRegisterResponse,
};
use sbgh_proto::{FleetRpcError, FleetServiceMux};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

struct PkiFixture {
    _directory: tempfile::TempDir,
    client_key: PathBuf,
    ca_certificate: PathBuf,
    server_certificate: PathBuf,
    server_key: PathBuf,
}

fn pki_fixture() -> PkiFixture {
    let directory = tempfile::tempdir().unwrap();
    let mut ca_parameters = CertificateParams::default();
    ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_parameters.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().unwrap();
    let ca = CertifiedIssuer::self_signed(ca_parameters, ca_key).unwrap();

    let mut server_parameters = CertificateParams::new(vec!["localhost".into()]).unwrap();
    server_parameters
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_key = KeyPair::generate().unwrap();
    let server_certificate = server_parameters
        .signed_by(&server_key, &ca)
        .unwrap();

    let client_key = KeyPair::generate().unwrap();
    let client_key_path = directory
        .path()
        .join("worker.key");
    let ca_certificate_path = directory
        .path()
        .join("ca.crt");
    let server_certificate_path = directory
        .path()
        .join("server.crt");
    let server_key_path = directory
        .path()
        .join("server.key");
    std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();
    std::fs::write(&ca_certificate_path, ca.pem()).unwrap();
    std::fs::write(&server_certificate_path, server_certificate.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
    for key in [&client_key_path, &server_key_path] {
        std::fs::set_permissions(key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    PkiFixture {
        _directory: directory,
        client_key: client_key_path,
        ca_certificate: ca_certificate_path,
        server_certificate: server_certificate_path,
        server_key: server_key_path,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Drain,
    RejectFirstOffer,
    Recover,
    OldDaemon,
    Superseded,
    PeriodicCleanup,
    CleanupRecovery,
    LeaseLoss,
}

#[derive(Clone)]
struct MockFleet {
    worker_id: Uuid,
    mode: Mode,
    session_id: Arc<std::sync::Mutex<Option<Uuid>>>,
    polls: Arc<AtomicUsize>,
    deregistrations: Arc<AtomicUsize>,
    registrations: Arc<AtomicUsize>,
    cleanup_calls: Arc<AtomicUsize>,
    cleanup_acks: Arc<AtomicUsize>,
    heartbeats: Arc<AtomicUsize>,
    attempt_id: Uuid,
    job_id: Uuid,
    trace_id: Uuid,
}

impl MockFleet {
    /// Return a typed non-retryable server lifecycle response.
    fn lifecycle_error(code: &str) -> Status {
        FleetRpcError {
            status: Code::FailedPrecondition,
            code: code.into(),
            message: code.into(),
            retryable: false,
            retry_after_ms: None,
        }
        .into_status()
    }
    fn unimplemented<T>() -> Result<Response<T>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }
}

#[tonic::async_trait]
impl WorkerFleetService for MockFleet {
    async fn check_registration(
        &self,
        request: Request<CheckRegistrationRequest>,
    ) -> Result<Response<CheckRegistrationResponse>, Status> {
        let request = request
            .into_inner()
            .into_domain()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        assert_eq!(
            request.advertised_capabilities,
            BTreeSet::from([WorkerCapability::Benchmark, WorkerCapability::BuildOnly])
        );
        assert_eq!(
            request.resources,
            worker_resources()
                .facts()
                .clone()
        );
        Ok(Response::new(CheckRegistrationResponse::from_domain(RegistrationCheckResponse {
            protocol_version: sbgh_fleet::PROTOCOL_VERSION,
            worker_id: self.worker_id,
            effective_capabilities: request.advertised_capabilities,
            measurement_profile: Some("loopback".into()),
            draining: false,
            heartbeat_interval_ms: 1_000,
            lease_ttl_ms: 5_000,
            server_time_ms: now_millis(),
        })))
    }

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<WireRegisterResponse>, Status> {
        let registration = request
            .into_inner()
            .into_domain()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        assert_eq!(
            registration.advertised_capabilities,
            BTreeSet::from([WorkerCapability::Benchmark, WorkerCapability::BuildOnly])
        );
        assert_eq!(
            registration.resources,
            worker_resources()
                .facts()
                .clone()
        );
        let previous = self
            .registrations
            .fetch_add(1, Ordering::SeqCst);
        let mut session = self
            .session_id
            .lock()
            .unwrap();
        if let Some(id) = *session {
            assert_eq!(id, registration.worker_session_id, "recovery must preserve UUID");
        }
        if self.mode == Mode::Superseded && previous > 0 {
            return Err(Self::lifecycle_error("session_superseded"));
        }
        *session = Some(registration.worker_session_id);
        Ok(Response::new(WireRegisterResponse::from_domain(RegisterSessionResponse {
            protocol_version: sbgh_fleet::PROTOCOL_VERSION,
            heartbeat_interval_ms: 1_000,
            lease_ttl_ms: 5_000,
            server_time_ms: now_millis(),
        })))
    }

    async fn poll(
        &self,
        request: Request<PollRequest>,
    ) -> Result<Response<WirePollResponse>, Status> {
        let poll = request
            .into_inner()
            .into_domain()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        assert_eq!(
            Some(poll.worker_session_id),
            *self
                .session_id
                .lock()
                .unwrap()
        );
        let index = self
            .polls
            .fetch_add(1, Ordering::SeqCst);
        let response = match (self.mode, index) {
            (Mode::Recover | Mode::Superseded, 0) => {
                return Err(Self::lifecycle_error("session_recovery_required"));
            }
            (Mode::OldDaemon, 0) => return Err(Self::lifecycle_error("stale_session")),
            (Mode::CleanupRecovery, _) => {
                assert_eq!(
                    self.cleanup_acks
                        .load(Ordering::SeqCst),
                    3,
                    "acknowledgement must precede polling"
                );
                PollResponse::Drain
            }
            (Mode::PeriodicCleanup, 0) => PollResponse::NoWork { retry_after_ms: 0 },
            (Mode::PeriodicCleanup, _) => {
                assert_eq!(
                    self.cleanup_acks
                        .load(Ordering::SeqCst),
                    1,
                    "cleanup must precede next poll"
                );
                PollResponse::Drain
            }
            (Mode::RejectFirstOffer | Mode::LeaseLoss, 0) => PollResponse::Offer {
                offer: Box::new(WorkOffer {
                    identity: AttemptIdentity {
                        worker_session_id: poll.worker_session_id,
                        attempt_id: self.attempt_id,
                        fencing_generation: 1,
                        lease_token: LeaseToken("a".repeat(64)),
                    },
                    job_id: self.job_id,
                    trace_id: self.trace_id,
                    capability: WorkerCapability::BuildOnly,
                    requirements: sbgh_fleet::OfferRequirements::BuildOnly,
                    payload_hash: sbgh_fleet::payload_digest(&sbgh_fleet::TaskPayload::BuildOnly)
                        .unwrap(),
                    offer_expires_at_ms: i64::MAX,
                }),
            },
            _ => PollResponse::Drain,
        };
        Ok(Response::new(WirePollResponse::from_domain(response)))
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        if self.mode == Mode::LeaseLoss {
            let accepted = request
                .into_inner()
                .into_domain()
                .unwrap();
            let payload = sbgh_fleet::TaskPayload::BuildOnly;
            return Ok(Response::new(AcceptResponse::from_domain(
                sbgh_fleet::AcceptOfferResponse {
                    assignment: sbgh_fleet::Assignment {
                        identity: accepted.identity,
                        trace_id: self.trace_id,
                        context: sbgh_fleet::AssignmentContext {
                            job_id: self.job_id,
                            repository: "test/repo".into(),
                            commit: "1".repeat(40),
                        },
                        payload_hash: sbgh_fleet::payload_digest(&payload).unwrap(),
                        payload,
                        vcpu_cpuset: None,
                    },
                },
            )));
        }
        Err(FleetRpcError {
            status: Code::FailedPrecondition,
            code: "stale_attempt".into(),
            message: "attempt is stale, expired, fenced, or unauthorized".into(),
            retryable: false,
            retry_after_ms: None,
        }
        .into_status())
    }

    async fn list_cleanup(
        &self,
        request: Request<ListCleanupRequest>,
    ) -> Result<Response<ListCleanupResponse>, Status> {
        let cleanup = request
            .into_inner()
            .into_domain()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        assert_eq!(
            Some(cleanup.worker_session_id),
            *self
                .session_id
                .lock()
                .unwrap()
        );
        let count = self
            .cleanup_calls
            .fetch_add(1, Ordering::SeqCst);
        if self.mode == Mode::CleanupRecovery && count > 0 {
            assert_eq!(
                self.cleanup_acks
                    .load(Ordering::SeqCst),
                3,
                "retry the verified teardown acknowledgement before listing or repeating cleanup"
            );
        }
        let obligations = if (self.mode == Mode::PeriodicCleanup && count == 1)
            || (self.mode == Mode::CleanupRecovery && count == 0)
        {
            vec![sbgh_fleet::CleanupItem {
                id: 1,
                attempt_id: Uuid::new_v4(),
                job_id: Uuid::new_v4(),
                reason: "inserted after startup".into(),
            }]
        } else {
            Vec::new()
        };
        Ok(Response::new(ListCleanupResponse::from_domain(obligations)))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        let request = request
            .into_inner()
            .into_domain()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        assert_eq!(
            Some(request.worker_session_id),
            *self
                .session_id
                .lock()
                .unwrap()
        );
        self.deregistrations
            .fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(DeregisterResponse::from_domain(true)))
    }

    async fn fetch_repository_credential(
        &self,
        _request: Request<FetchRepositoryCredentialRequest>,
    ) -> Result<Response<FetchRepositoryCredentialResponse>, Status> {
        if self.mode == Mode::LeaseLoss {
            return std::future::pending().await;
        }
        Self::unimplemented()
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        if self.mode == Mode::LeaseLoss {
            if self
                .heartbeats
                .fetch_add(1, Ordering::SeqCst)
                > 0
            {
                return std::future::pending().await;
            }
            return Ok(Response::new(HeartbeatResponse::from_domain(
                sbgh_fleet::HeartbeatResponse {
                    desired_state: sbgh_fleet::DesiredState::Continue,
                    lease_expires_at_ms: now_millis() + 5_000,
                    highest_contiguous_reliable_seq: 0,
                },
            )));
        }
        Self::unimplemented()
    }

    async fn publish_reliable_event(
        &self,
        request: Request<PublishReliableEventRequest>,
    ) -> Result<Response<PublishReliableEventResponse>, Status> {
        if self.mode == Mode::LeaseLoss {
            let event = request
                .into_inner()
                .into_domain()
                .unwrap();
            std::assert_matches!(event.payload, sbgh_fleet::ReliableEventPayload::Phase { .. });
            return Ok(Response::new(PublishReliableEventResponse::from_domain(
                sbgh_fleet::ReliableEventAck {
                    highest_contiguous_reliable_seq: event.reliable_seq,
                },
            )));
        }
        Self::unimplemented()
    }

    async fn publish_progress(
        &self,
        _request: Request<PublishProgressRequest>,
    ) -> Result<Response<PublishProgressResponse>, Status> {
        Self::unimplemented()
    }

    async fn grant_artifact(
        &self,
        _request: Request<GrantArtifactRequest>,
    ) -> Result<Response<GrantArtifactResponse>, Status> {
        Self::unimplemented()
    }

    async fn complete_attempt(
        &self,
        _request: Request<CompleteAttemptRequest>,
    ) -> Result<Response<CompleteAttemptResponse>, Status> {
        Self::unimplemented()
    }

    async fn complete_cleanup(
        &self,
        request: Request<CompleteCleanupRequest>,
    ) -> Result<Response<CompleteCleanupResponse>, Status> {
        let request = request
            .into_inner()
            .into_domain()
            .unwrap();
        assert_eq!(
            Some(request.worker_session_id),
            *self
                .session_id
                .lock()
                .unwrap()
        );
        assert_eq!(request.obligation_id, 1);
        let previous = self
            .cleanup_acks
            .fetch_add(1, Ordering::SeqCst);
        if self.mode == Mode::CleanupRecovery && previous < 2 {
            return Err(Self::lifecycle_error("session_recovery_required"));
        }
        Ok(Response::new(CompleteCleanupResponse::from_domain(true)))
    }
}

async fn serve_mock(
    listener: TcpListener,
    server_certificate: Vec<u8>,
    server_key: Vec<u8>,
    _client_ca: Vec<u8>,
    fleet: MockFleet,
    shutdown: CancellationToken,
) {
    let identity = Identity::from_pem(server_certificate, server_key);
    let (health_reporter, health) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<WorkerFleetServiceServer<MockFleet>>()
        .await;
    Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))
        .unwrap()
        .serve_with_incoming_shutdown(
            FleetServiceMux::new(WorkerFleetServiceServer::new(fleet), health),
            TcpListenerStream::new(listener),
            shutdown.cancelled_owned(),
        )
        .await
        .unwrap();
}

fn worker_config(
    _worker_id: Uuid,
    address: std::net::SocketAddr,
    pki: &PkiFixture,
) -> sbgh_worker::WorkerConfig {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.worker-benchmark.toml");
    let mut config = sbgh_worker::WorkerConfig::load(&path).unwrap();
    config.orchestrator_url = format!("https://localhost:{}", address.port());
    config.identity_private_key = pki.client_key.clone();
    config.block_validation = None;
    configure_sandbox_preflight_fixture(&mut config, pki._directory.path());
    config.validate().unwrap();
    config
}

fn worker_resources() -> sbgh_worker::HostResources {
    sbgh_worker::HostResources::new((0..8).collect(), 32 * 1024 * 1024 * 1024).unwrap()
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn configure_sandbox_preflight_fixture(config: &mut sbgh_worker::WorkerConfig, directory: &Path) {
    let golden = directory.join("golden.qcow2");
    let host_tool = directory.join("host-tool");
    let sudo = directory.join("sudo");
    std::fs::write(&golden, b"qcow2 fixture").unwrap();
    std::fs::write(&host_tool, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::write(
        &sudo,
        concat!(
            "#!/bin/sh\n",
            "case \"$*\" in\n",
            "  *\"lv_name,lv_attr\"*) printf 'mainnet-test|Vri---tz-k\\n' ;;\n",
            "  *\"data_percent,metadata_percent\"*) printf '10.00|10.00\\n' ;;\n",
            "  *) exit 0 ;;\n",
            "esac\n",
        ),
    )
    .unwrap();
    for path in [&host_tool, &sudo] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    config.sandbox.golden_image = golden;
    config.workspace.jobs_dir = directory.join("jobs");
    config.workspace.git_mirror = directory.join("git/mirror.git");
    config
        .workspace
        .results_tmpfs_root = directory.join("results-tmpfs");
    config
        .workspace
        .results_archive_dir = directory.join("results-archive");
    config.commands.sudo = sudo;
    config.commands.virsh = host_tool.clone();
    config.commands.qemu_img = host_tool.clone();
    config.commands.cloud_localds = host_tool.clone();
    config.commands.git = host_tool;
}

async fn run_case(mode: Mode) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let worker_id = Uuid::new_v4();
    let pki = pki_fixture();
    // rustls-native-certs honors SSL_CERT_FILE on Unix; this keeps the
    // integration fixture on the production Web-PKI validation path.
    unsafe {
        std::env::set_var("SSL_CERT_FILE", &pki.ca_certificate);
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let fleet = MockFleet {
        worker_id,
        mode,
        session_id: Arc::new(std::sync::Mutex::new(None)),
        polls: Arc::new(AtomicUsize::new(0)),
        deregistrations: Arc::new(AtomicUsize::new(0)),
        registrations: Arc::new(AtomicUsize::new(0)),
        cleanup_calls: Arc::new(AtomicUsize::new(0)),
        cleanup_acks: Arc::new(AtomicUsize::new(0)),
        heartbeats: Arc::new(AtomicUsize::new(0)),
        attempt_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        trace_id: Uuid::new_v4(),
    };
    let server_shutdown = CancellationToken::new();
    let server = tokio::spawn(serve_mock(
        listener,
        std::fs::read(&pki.server_certificate).unwrap(),
        std::fs::read(&pki.server_key).unwrap(),
        std::fs::read(&pki.ca_certificate).unwrap(),
        fleet.clone(),
        server_shutdown.clone(),
    ));

    let config = worker_config(worker_id, address, &pki);
    sbgh_worker::check_connectivity(&config)
        .await
        .unwrap();
    let registration = sbgh_worker::check_registration(&config, worker_resources())
        .await
        .unwrap();
    assert_eq!(
        registration
            .registration
            .worker_id,
        worker_id
    );
    assert_eq!(
        registration
            .registration
            .protocol_version,
        sbgh_fleet::PROTOCOL_VERSION
    );
    assert!(
        fleet
            .session_id
            .lock()
            .unwrap()
            .is_none(),
        "registration diagnostics must not create or replace a worker session"
    );
    assert_eq!(
        fleet
            .deregistrations
            .load(Ordering::SeqCst),
        0
    );

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        sbgh_worker::run_fleet(config, worker_resources(), CancellationToken::new()),
    )
    .await
    .unwrap();
    let rejected = matches!(mode, Mode::OldDaemon | Mode::Superseded);
    if rejected {
        assert!(result.is_err());
    } else {
        result.unwrap();
    }
    server_shutdown.cancel();
    server.await.unwrap();

    assert!(
        fleet
            .session_id
            .lock()
            .unwrap()
            .is_some()
    );
    assert!(
        fleet
            .polls
            .load(Ordering::SeqCst)
            >= 1
    );
    assert_eq!(
        fleet
            .deregistrations
            .load(Ordering::SeqCst),
        usize::from(!rejected)
    );
    assert_eq!(
        fleet
            .registrations
            .load(Ordering::SeqCst),
        match mode {
            Mode::Recover | Mode::Superseded => 2,
            Mode::CleanupRecovery => 3,
            _ => 1,
        }
    );
}

#[tokio::test]
async fn real_worker_registers_polls_drain_and_deregisters_over_mtls_grpc() {
    run_case(Mode::Drain).await;
}

#[tokio::test]
async fn cancellation_winning_before_accept_never_starts_execution() {
    run_case(Mode::RejectFirstOffer).await;
}

#[tokio::test]
async fn resident_worker_recovers_same_session_over_grpc() {
    run_case(Mode::Recover).await;
}

#[tokio::test]
async fn previous_daemon_stale_session_still_fails_closed() {
    run_case(Mode::OldDaemon).await;
}

#[tokio::test]
async fn superseded_worker_exits_without_minting_another_session() {
    run_case(Mode::Superseded).await;
}

#[tokio::test]
async fn cleanup_inserted_after_startup_is_acknowledged_before_polling_again() {
    run_case(Mode::PeriodicCleanup).await;
}

#[tokio::test]
async fn expired_cleanup_session_recovers_and_retries_ack_without_repeating_teardown() {
    run_case(Mode::CleanupRecovery).await;
}

#[tokio::test]
async fn stalled_heartbeat_cancels_credential_wait_and_worker_remains_resident() {
    run_case(Mode::LeaseLoss).await;
}
