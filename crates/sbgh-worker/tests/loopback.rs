#![cfg(unix)]

use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::server::WebPkiClientVerifier;
use sbgh_proto::{
    AcceptOfferRequest, ApiError, AttemptIdentity, DeregisterSessionRequest, LeaseToken,
    PROTOCOL_VERSION, PollRequest, PollResponse, RegisterSessionRequest, ResourceFacts, WorkOffer,
    WorkerCapability,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct PkiFixture {
    _directory: tempfile::TempDir,
    client_certificate: PathBuf,
    client_key: PathBuf,
    ca_certificate: PathBuf,
    server: rustls::ServerConfig,
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
    std::fs::write(&client_certificate_path, client_certificate.pem()).unwrap();
    std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();
    std::fs::write(&ca_certificate_path, ca.pem()).unwrap();
    std::fs::set_permissions(&client_key_path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let mut roots = RootCertStore::empty();
    roots
        .add(ca.der().clone())
        .unwrap();
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();
    let mut server =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![
                    server_certificate
                        .der()
                        .clone(),
                ],
                PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            )
            .unwrap();
    server.alpn_protocols = vec![b"http/1.1".to_vec()];

    PkiFixture {
        _directory: directory,
        client_certificate: client_certificate_path,
        client_key: client_key_path,
        ca_certificate: ca_certificate_path,
        server,
    }
}

struct Request {
    path: String,
    body: Vec<u8>,
}

async fn read_request(
    stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> Request {
    const HEADER_LIMIT: usize = 32 * 1024;
    let mut bytes = Vec::new();
    let header_end = loop {
        assert!(bytes.len() < HEADER_LIMIT, "request headers exceeded test limit");
        let mut buffer = [0_u8; 4096];
        let read = stream
            .read(&mut buffer)
            .await
            .unwrap();
        assert!(read > 0, "connection closed before request headers");
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(offset) = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
        {
            break offset + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
    let request_line = headers
        .lines()
        .next()
        .unwrap();
    let mut request_parts = request_line.split_whitespace();
    let _method = request_parts.next().unwrap();
    let path = request_parts
        .next()
        .unwrap()
        .to_owned();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| {
                    value
                        .trim()
                        .parse::<usize>()
                        .unwrap()
                })
        })
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let mut buffer = [0_u8; 4096];
        let read = stream
            .read(&mut buffer)
            .await
            .unwrap();
        assert!(read > 0, "connection closed before request body");
        bytes.extend_from_slice(&buffer[..read]);
    }
    Request {
        path,
        body: bytes[header_end..header_end + content_length].to_vec(),
    }
}

async fn respond(stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>, body: &str) {
    respond_with_status(stream, "200 OK", body).await;
}

async fn respond_with_status(
    stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    status: &str,
    body: &str,
) {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .unwrap();
    stream
        .shutdown()
        .await
        .unwrap();
}

async fn accept_request(
    listener: &TcpListener,
    acceptor: &TlsAcceptor,
) -> (tokio_rustls::server::TlsStream<tokio::net::TcpStream>, Request) {
    let (tcp, _) = listener
        .accept()
        .await
        .unwrap();
    let mut stream = acceptor
        .accept(tcp)
        .await
        .unwrap();
    let request = read_request(&mut stream).await;
    (stream, request)
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

#[tokio::test]
async fn real_worker_registers_polls_drain_and_deregisters_over_mtls_loopback() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let worker_id = Uuid::new_v4();
    let pki = pki_fixture(worker_id);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(pki.server.clone()));
    let server = tokio::spawn(async move {
        let mut session_id = None;
        for response in [
            format!(
                r#"{{"protocol_version":{PROTOCOL_VERSION},"heartbeat_interval_ms":1000,"lease_ttl_ms":5000,"server_time_ms":0}}"#
            ),
            "[]".into(),
            r#"{"kind":"drain"}"#.into(),
            "{}".into(),
        ] {
            let (tcp, _) = listener
                .accept()
                .await
                .unwrap();
            let mut stream = acceptor
                .accept(tcp)
                .await
                .unwrap();
            let request = read_request(&mut stream).await;
            if request.path == "/v1/register" {
                let registration: RegisterSessionRequest =
                    serde_json::from_slice(&request.body).unwrap();
                assert_eq!(registration.protocol_version, PROTOCOL_VERSION);
                assert_eq!(registration.worker_id, worker_id);
                assert_eq!(
                    registration.advertised_capabilities,
                    BTreeSet::from([WorkerCapability::BuildOnly])
                );
                assert_eq!(registration.resources, worker_resources());
                session_id = Some(registration.worker_session_id);
            } else if request
                .path
                .starts_with("/v1/cleanup?")
            {
                let query = request
                    .path
                    .split_once('?')
                    .unwrap()
                    .1;
                let parameters = query
                    .split('&')
                    .filter_map(|pair| pair.split_once('='))
                    .collect::<std::collections::BTreeMap<_, _>>();
                assert_eq!(
                    *parameters
                        .get("protocol_version")
                        .unwrap(),
                    PROTOCOL_VERSION.to_string()
                );
                assert_eq!(
                    *parameters
                        .get("worker_session_id")
                        .unwrap(),
                    session_id
                        .unwrap()
                        .to_string()
                );
            } else if request.path == "/v1/poll" {
                let poll: PollRequest = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(poll.protocol_version, PROTOCOL_VERSION);
                assert_eq!(Some(poll.worker_session_id), session_id);
            } else if request.path == "/v1/deregister" {
                let deregister: DeregisterSessionRequest =
                    serde_json::from_slice(&request.body).unwrap();
                assert_eq!(deregister.protocol_version, PROTOCOL_VERSION);
                assert_eq!(Some(deregister.worker_session_id), session_id);
            } else {
                panic!("unexpected worker request {}", request.path);
            }
            respond(&mut stream, &response).await;
        }
        assert!(session_id.is_some());
    });

    sbgh_worker::run_fleet(
        worker_config(worker_id, address, &pki),
        worker_resources(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn cancellation_winning_before_accept_never_starts_execution() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let worker_id = Uuid::new_v4();
    let pki = pki_fixture(worker_id);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(pki.server.clone()));
    let server = tokio::spawn(async move {
        let (mut stream, request) = accept_request(&listener, &acceptor).await;
        assert_eq!(request.path, "/v1/register");
        let registration: RegisterSessionRequest = serde_json::from_slice(&request.body).unwrap();
        let session_id = registration.worker_session_id;
        respond(
            &mut stream,
            &format!(
                r#"{{"protocol_version":{PROTOCOL_VERSION},"heartbeat_interval_ms":1000,"lease_ttl_ms":5000,"server_time_ms":0}}"#
            ),
        )
        .await;

        let (mut stream, request) = accept_request(&listener, &acceptor).await;
        assert!(
            request
                .path
                .starts_with("/v1/cleanup?")
        );
        respond(&mut stream, "[]").await;

        let identity = AttemptIdentity {
            worker_session_id: session_id,
            attempt_id: Uuid::new_v4(),
            fencing_generation: 1,
            lease_token: LeaseToken("a".repeat(64)),
        };
        let offer = PollResponse::Offer {
            offer: Box::new(WorkOffer {
                identity: identity.clone(),
                job_id: Uuid::new_v4(),
                trace_id: Uuid::new_v4(),
                capability: WorkerCapability::BuildOnly,
                requirements: sbgh_proto::OfferRequirements::BuildOnly,
                payload_hash: "ab".repeat(32),
                offer_expires_at_ms: i64::MAX,
            }),
        };
        let (mut stream, request) = accept_request(&listener, &acceptor).await;
        assert_eq!(request.path, "/v1/poll");
        respond(&mut stream, &serde_json::to_string(&offer).unwrap()).await;

        let (mut stream, request) = accept_request(&listener, &acceptor).await;
        assert_eq!(request.path, "/v1/accept");
        let accepted: AcceptOfferRequest = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(accepted.identity, identity);
        respond_with_status(
            &mut stream,
            "409 Conflict",
            &serde_json::to_string(&ApiError {
                code: "stale_attempt".into(),
                message: "attempt is stale, expired, fenced, or unauthorized".into(),
                retryable: false,
            })
            .unwrap(),
        )
        .await;

        let (mut stream, request) = accept_request(&listener, &acceptor).await;
        assert_eq!(request.path, "/v1/poll");
        respond(&mut stream, &serde_json::to_string(&PollResponse::Drain).unwrap()).await;

        let (mut stream, request) = accept_request(&listener, &acceptor).await;
        assert_eq!(request.path, "/v1/deregister");
        let deregister: DeregisterSessionRequest = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(deregister.worker_session_id, session_id);
        respond(&mut stream, "{}").await;
    });

    sbgh_worker::run_fleet(
        worker_config(worker_id, address, &pki),
        worker_resources(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    server.await.unwrap();
}
