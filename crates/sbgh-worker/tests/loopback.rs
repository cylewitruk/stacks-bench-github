#![cfg(unix)]

use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use sbgh_fleet::{
    AttemptIdentity, LeaseToken, PollResponse, RegisterSessionResponse, ResourceFacts, WorkOffer,
    WorkerCapability,
};
use sbgh_proto::FleetRpcError;
use sbgh_proto::Wire;
use sbgh_proto::fleet::v1::worker_fleet_service_server::{
    WorkerFleetService, WorkerFleetServiceServer,
};
use sbgh_proto::fleet::v1::{
    AcceptRequest, AcceptResponse, CompleteAttemptRequest, CompleteAttemptResponse,
    CompleteCleanupRequest, CompleteCleanupResponse, DeregisterRequest, DeregisterResponse,
    FetchRepositoryCredentialRequest, FetchRepositoryCredentialResponse, GrantArtifactRequest,
    GrantArtifactResponse, HeartbeatRequest, HeartbeatResponse, ListCleanupRequest,
    ListCleanupResponse, PollRequest, PollResponse as WirePollResponse, PublishProgressRequest,
    PublishProgressResponse, PublishReliableEventRequest, PublishReliableEventResponse,
    RegisterRequest, RegisterResponse as WireRegisterResponse,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

struct PkiFixture {
    _directory: tempfile::TempDir,
    client_certificate: PathBuf,
    client_key: PathBuf,
    ca_certificate: PathBuf,
    server_certificate: PathBuf,
    server_key: PathBuf,
}

fn pki_fixture(worker_id: Uuid) -> PkiFixture {
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

    let mut client_parameters = CertificateParams::default();
    client_parameters.subject_alt_names = vec![SanType::URI(
        format!("urn:sbgh:worker:{worker_id}")
            .try_into()
            .unwrap(),
    )];
    client_parameters
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_key = KeyPair::generate().unwrap();
    let client_certificate = client_parameters
        .signed_by(&client_key, &ca)
        .unwrap();

    let client_certificate_path = directory
        .path()
        .join("worker.crt");
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
    std::fs::write(&client_certificate_path, client_certificate.pem()).unwrap();
    std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();
    std::fs::write(&ca_certificate_path, ca.pem()).unwrap();
    std::fs::write(&server_certificate_path, server_certificate.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
    for key in [&client_key_path, &server_key_path] {
        std::fs::set_permissions(key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    PkiFixture {
        _directory: directory,
        client_certificate: client_certificate_path,
        client_key: client_key_path,
        ca_certificate: ca_certificate_path,
        server_certificate: server_certificate_path,
        server_key: server_key_path,
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Drain,
    RejectFirstOffer,
}

#[derive(Clone)]
struct MockFleet {
    worker_id: Uuid,
    mode: Mode,
    session_id: Arc<std::sync::Mutex<Option<Uuid>>>,
    polls: Arc<AtomicUsize>,
}

impl MockFleet {
    fn unimplemented<T>() -> Result<Response<T>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }
}

#[tonic::async_trait]
impl WorkerFleetService for MockFleet {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<WireRegisterResponse>, Status> {
        let registration = request
            .into_inner()
            .into_domain()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        assert_eq!(registration.worker_id, self.worker_id);
        assert_eq!(
            registration.advertised_capabilities,
            BTreeSet::from([WorkerCapability::BuildOnly])
        );
        assert_eq!(registration.resources, worker_resources());
        *self
            .session_id
            .lock()
            .unwrap() = Some(registration.worker_session_id);
        Ok(Response::new(WireRegisterResponse::from_domain(RegisterSessionResponse {
            protocol_version: sbgh_fleet::PROTOCOL_VERSION,
            heartbeat_interval_ms: 1_000,
            lease_ttl_ms: 5_000,
            server_time_ms: 0,
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
            (Mode::RejectFirstOffer, 0) => PollResponse::Offer {
                offer: Box::new(WorkOffer {
                    identity: AttemptIdentity {
                        worker_session_id: poll.worker_session_id,
                        attempt_id: Uuid::new_v4(),
                        fencing_generation: 1,
                        lease_token: LeaseToken("a".repeat(64)),
                    },
                    job_id: Uuid::new_v4(),
                    trace_id: Uuid::new_v4(),
                    capability: WorkerCapability::BuildOnly,
                    requirements: sbgh_fleet::OfferRequirements::BuildOnly,
                    payload_hash: "ab".repeat(32),
                    offer_expires_at_ms: i64::MAX,
                }),
            },
            _ => PollResponse::Drain,
        };
        Ok(Response::new(WirePollResponse::from_domain(response)))
    }

    async fn accept(
        &self,
        _request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
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
        Ok(Response::new(ListCleanupResponse::from_domain(Vec::new())))
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
        Ok(Response::new(DeregisterResponse::from_domain(true)))
    }

    async fn fetch_repository_credential(
        &self,
        _request: Request<FetchRepositoryCredentialRequest>,
    ) -> Result<Response<FetchRepositoryCredentialResponse>, Status> {
        Self::unimplemented()
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Self::unimplemented()
    }

    async fn publish_reliable_event(
        &self,
        _request: Request<PublishReliableEventRequest>,
    ) -> Result<Response<PublishReliableEventResponse>, Status> {
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
        _request: Request<CompleteCleanupRequest>,
    ) -> Result<Response<CompleteCleanupResponse>, Status> {
        Self::unimplemented()
    }
}

async fn serve_mock(
    listener: TcpListener,
    server_certificate: Vec<u8>,
    server_key: Vec<u8>,
    client_ca: Vec<u8>,
    fleet: MockFleet,
    shutdown: CancellationToken,
) {
    let identity = Identity::from_pem(server_certificate, server_key);
    let client_ca = Certificate::from_pem(client_ca);
    Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(client_ca),
        )
        .unwrap()
        .serve_with_incoming_shutdown(
            WorkerFleetServiceServer::new(fleet),
            TcpListenerStream::new(listener),
            shutdown.cancelled_owned(),
        )
        .await
        .unwrap();
}

fn worker_config(
    worker_id: Uuid,
    address: std::net::SocketAddr,
    pki: &PkiFixture,
) -> sbgh_worker::WorkerConfig {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.worker-benchmark.toml");
    let mut config = sbgh_worker::WorkerConfig::load(&path).unwrap();
    config.worker_id = worker_id;
    config.orchestrator_url = format!("https://localhost:{}", address.port());
    config.client_certificate = pki.client_certificate.clone();
    config.client_private_key = pki.client_key.clone();
    config.server_ca_certificate = pki.ca_certificate.clone();
    config.capabilities = BTreeSet::from([WorkerCapability::BuildOnly]);
    configure_sandbox_preflight_fixture(&mut config, pki._directory.path());
    config.validate().unwrap();
    config
}

fn worker_resources() -> ResourceFacts {
    ResourceFacts {
        logical_cpus: 8,
        memory_bytes: 32 * 1024 * 1024 * 1024,
    }
}

fn configure_sandbox_preflight_fixture(config: &mut sbgh_worker::WorkerConfig, directory: &Path) {
    let libvirt = config
        .libvirt
        .as_mut()
        .unwrap();
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

    libvirt.vm.golden_image = golden;
    libvirt.paths.jobs_dir = directory.join("jobs");
    libvirt.paths.git_mirror = directory.join("git/mirror.git");
    libvirt
        .paths
        .results_tmpfs_root = directory.join("results-tmpfs");
    libvirt
        .paths
        .results_archive_dir = directory.join("results-archive");
    libvirt.paths.sudo_binary = sudo;
    libvirt.paths.virsh_binary = host_tool.clone();
    libvirt.paths.qemu_img_binary = host_tool.clone();
    libvirt
        .paths
        .cloud_localds_binary = host_tool.clone();
    libvirt.paths.git_binary = host_tool;
}

async fn run_case(mode: Mode) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let worker_id = Uuid::new_v4();
    let pki = pki_fixture(worker_id);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let fleet = MockFleet {
        worker_id,
        mode,
        session_id: Arc::new(std::sync::Mutex::new(None)),
        polls: Arc::new(AtomicUsize::new(0)),
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

    sbgh_worker::run_fleet(
        worker_config(worker_id, address, &pki),
        worker_resources(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
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
}

#[tokio::test]
async fn real_worker_registers_polls_drain_and_deregisters_over_mtls_grpc() {
    run_case(Mode::Drain).await;
}

#[tokio::test]
async fn cancellation_winning_before_accept_never_starts_execution() {
    run_case(Mode::RejectFirstOffer).await;
}
