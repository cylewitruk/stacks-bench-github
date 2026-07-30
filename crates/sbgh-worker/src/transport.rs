use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::{Context, ensure};
use hyper_util::rt::TokioIo;
use reqwest::header::{HeaderName, HeaderValue};
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use sbgh_fleet::{
    AcceptOfferRequest, AcceptOfferResponse, ArtifactGrantRequest, ArtifactGrantResponse,
    CleanupCompleteRequest, CleanupItem, CleanupListRequest, CompleteAttemptRequest,
    CompleteAttemptResponse, DeregisterSessionRequest, HeartbeatRequest, HeartbeatResponse,
    MAX_LONG_POLL_SECS, PollRequest, PollResponse, ProgressRequest, RegisterSessionRequest,
    RegisterSessionResponse, ReliableEventAck, ReliableEventEnvelope, RepositoryCredentialRequest,
    RepositoryCredentialResponse,
};
use sbgh_proto::Wire;
use sbgh_proto::fleet::v1 as wire;
use sbgh_proto::fleet::v1::worker_fleet_service_client::WorkerFleetServiceClient;
use sbgh_proto::status_detail;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tonic::Code;
use tonic::codegen::Service;
use tonic::codegen::http::Uri;
use tonic::transport::{Channel, Endpoint};

#[derive(Clone)]
pub struct FleetClient {
    control: WorkerFleetServiceClient<Channel>,
    artifact_http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
#[error("fleet RPC {path} failed: {code}: {message}")]
pub struct FleetClientError {
    pub path: String,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after: Option<Duration>,
}

#[derive(Clone)]
struct Tls13Connector {
    host: Arc<str>,
    port: u16,
    server_name: ServerName<'static>,
    tls: tokio_rustls::TlsConnector,
}

impl Service<Uri> for Tls13Connector {
    type Response = TokioIo<TlsStream<TcpStream>>;
    type Error = io::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _context: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Uri) -> Self::Future {
        let host = self.host.clone();
        let port = self.port;
        let server_name = self.server_name.clone();
        let tls = self.tls.clone();
        Box::pin(async move {
            let tcp = TcpStream::connect((host.as_ref(), port)).await?;
            tcp.set_nodelay(true)?;
            tls.connect(server_name, tcp)
                .await
                .map_err(io::Error::other)
                .map(TokioIo::new)
        })
    }
}

impl FleetClient {
    pub fn build(base_url: &str, identity_private_key: &Path) -> anyhow::Result<Self> {
        crate::identity::require_private_permissions(identity_private_key)?;
        let private_key_pem = std::fs::read_to_string(identity_private_key).with_context(|| {
            format!("reading worker identity key {}", identity_private_key.display())
        })?;
        let origin = reqwest::Url::parse(base_url).context("parsing fleet gRPC endpoint")?;
        ensure!(
            origin.scheme() == "https"
                && origin.username().is_empty()
                && origin.password().is_none()
                && origin.query().is_none()
                && origin.fragment().is_none()
                && matches!(origin.path(), "" | "/"),
            "fleet gRPC endpoint must be an HTTPS origin"
        );
        let host = origin
            .host_str()
            .context("fleet gRPC endpoint has no host")?
            .to_owned();
        let port = origin
            .port_or_known_default()
            .context("fleet gRPC endpoint has no port")?;
        let public_uri: Uri = base_url
            .trim_end_matches('/')
            .parse()
            .context("parsing fleet gRPC endpoint URI")?;
        let authority = public_uri
            .authority()
            .context("fleet gRPC endpoint has no authority")?
            .as_str();
        let endpoint = Endpoint::from_shared(format!("http://{authority}"))
            .context("parsing fleet gRPC endpoint")?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(MAX_LONG_POLL_SECS + 15));
        let connector = tls13_connector(&host, port, &private_key_pem)?;
        // The endpoint URI is deliberately `http` only to keep Tonic from
        // applying its TLS-1.2-capable convenience connector. Every socket is
        // wrapped by this TLS-1.3-only connector before HTTP/2 sees it.
        let control =
            WorkerFleetServiceClient::new(endpoint.connect_with_connector_lazy(connector))
                .max_decoding_message_size(sbgh_fleet::MAX_REQUEST_BYTES)
                .max_encoding_message_size(sbgh_fleet::MAX_REQUEST_BYTES);
        let artifact_http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("building delegated artifact client")?;
        Ok(Self { control, artifact_http })
    }

    pub async fn register(
        &self,
        request: &RegisterSessionRequest,
    ) -> anyhow::Result<RegisterSessionResponse> {
        let mut client = self.control.clone();
        let response = client
            .register(wire::RegisterRequest::from_domain(request.clone()))
            .await
            .map_err(|status| fleet_error("Register", status))?
            .into_inner();
        response
            .into_domain()
            .context("decoding Register response")
    }

    pub async fn poll(&self, request: &PollRequest) -> anyhow::Result<PollResponse> {
        let mut client = self.control.clone();
        let response = client
            .poll(wire::PollRequest::from_domain(request.clone()))
            .await
            .map_err(|status| fleet_error("Poll", status))?
            .into_inner();
        response
            .into_domain()
            .context("decoding Poll response")
    }

    pub async fn accept(
        &self,
        request: &AcceptOfferRequest,
    ) -> anyhow::Result<AcceptOfferResponse> {
        let mut client = self.control.clone();
        client
            .accept(wire::AcceptRequest::from_domain(request.clone()))
            .await
            .map_err(|status| fleet_error("Accept", status))?
            .into_inner()
            .into_domain()
            .context("decoding Accept response")
    }

    pub async fn heartbeat(&self, request: &HeartbeatRequest) -> anyhow::Result<HeartbeatResponse> {
        let mut client = self.control.clone();
        client
            .heartbeat(wire::HeartbeatRequest::from_domain(request.clone()))
            .await
            .map_err(|status| fleet_error("Heartbeat", status))?
            .into_inner()
            .into_domain()
            .context("decoding Heartbeat response")
    }

    pub async fn repository_credential(
        &self,
        request: &RepositoryCredentialRequest,
    ) -> anyhow::Result<RepositoryCredentialResponse> {
        let mut client = self.control.clone();
        client
            .fetch_repository_credential(wire::FetchRepositoryCredentialRequest::from_domain(
                request.clone(),
            ))
            .await
            .map_err(|status| fleet_error("FetchRepositoryCredential", status))?
            .into_inner()
            .into_domain()
            .context("decoding FetchRepositoryCredential response")
    }

    pub async fn event(&self, request: &ReliableEventEnvelope) -> anyhow::Result<ReliableEventAck> {
        let mut client = self.control.clone();
        client
            .publish_reliable_event(wire::PublishReliableEventRequest::from_domain(request.clone()))
            .await
            .map_err(|status| fleet_error("PublishReliableEvent", status))?
            .into_inner()
            .into_domain()
            .context("decoding PublishReliableEvent response")
    }

    pub async fn progress(&self, request: &ProgressRequest) -> anyhow::Result<()> {
        let mut client = self.control.clone();
        let accepted = client
            .publish_progress(wire::PublishProgressRequest::from_domain(request.clone()))
            .await
            .map_err(|status| fleet_error("PublishProgress", status))?
            .into_inner()
            .into_domain()
            .context("decoding PublishProgress response")?;
        ensure!(accepted, "fleet rejected progress without an RPC error");
        Ok(())
    }

    pub async fn artifact_grant(
        &self,
        request: &ArtifactGrantRequest,
    ) -> anyhow::Result<ArtifactGrantResponse> {
        let mut client = self.control.clone();
        client
            .grant_artifact(wire::GrantArtifactRequest::from_domain(request.clone()))
            .await
            .map_err(|status| fleet_error("GrantArtifact", status))?
            .into_inner()
            .into_domain()
            .context("decoding GrantArtifact response")
    }

    pub async fn complete(
        &self,
        request: &CompleteAttemptRequest,
    ) -> anyhow::Result<CompleteAttemptResponse> {
        let mut client = self.control.clone();
        client
            .complete_attempt(wire::CompleteAttemptRequest::from_domain(request.clone()))
            .await
            .map_err(|status| fleet_error("CompleteAttempt", status))?
            .into_inner()
            .into_domain()
            .context("decoding CompleteAttempt response")
    }

    pub async fn cleanup(&self, session_id: uuid::Uuid) -> anyhow::Result<Vec<CleanupItem>> {
        let mut client = self.control.clone();
        client
            .list_cleanup(wire::ListCleanupRequest::from_domain(CleanupListRequest {
                worker_session_id: session_id,
            }))
            .await
            .map_err(|status| fleet_error("ListCleanup", status))?
            .into_inner()
            .into_domain()
            .context("decoding ListCleanup response")
    }

    pub async fn complete_cleanup(
        &self,
        session_id: uuid::Uuid,
        obligation_id: i64,
    ) -> anyhow::Result<()> {
        let mut client = self.control.clone();
        let completed = client
            .complete_cleanup(wire::CompleteCleanupRequest::from_domain(CleanupCompleteRequest {
                worker_session_id: session_id,
                obligation_id,
            }))
            .await
            .map_err(|status| fleet_error("CompleteCleanup", status))?
            .into_inner()
            .into_domain()
            .context("decoding CompleteCleanup response")?;
        ensure!(completed, "fleet rejected cleanup without an RPC error");
        Ok(())
    }

    pub async fn deregister(&self, session_id: uuid::Uuid) -> anyhow::Result<()> {
        let mut client = self.control.clone();
        client
            .deregister(wire::DeregisterRequest::from_domain(DeregisterSessionRequest {
                worker_session_id: session_id,
            }))
            .await
            .map_err(|status| fleet_error("Deregister", status))?;
        Ok(())
    }

    pub async fn upload(&self, grant: &ArtifactGrantResponse, path: &Path) -> anyhow::Result<()> {
        ensure!(grant.method == "PUT", "artifact grant is not a PUT");
        let url = delegated_artifact_url(&grant.url)?;
        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("opening artifact {}", path.display()))?;
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let mut request = self.artifact_http.put(url);
        for header in &grant.headers {
            request = request.header(
                HeaderName::from_bytes(header.name.as_bytes())
                    .context("invalid grant header name")?,
                HeaderValue::from_str(&header.value).context("invalid grant header value")?,
            );
        }
        let response = request
            .body(body)
            .send()
            .await
            .context("uploading artifact")?;
        ensure!(response.status().is_success(), "artifact PUT returned HTTP {}", response.status());
        Ok(())
    }

    pub async fn download(
        &self,
        grant: &ArtifactGrantResponse,
        destination: &Path,
    ) -> anyhow::Result<()> {
        ensure!(grant.method == "GET", "artifact grant is not a GET");
        let url = delegated_artifact_url(&grant.url)?;
        let response = self
            .artifact_http
            .get(url)
            .send()
            .await
            .context("downloading artifact")?;
        ensure!(response.status().is_success(), "artifact GET returned HTTP {}", response.status());
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let part = destination.with_extension(format!("{}.part", uuid::Uuid::new_v4()));
        let mut file = tokio::fs::File::create(&part).await?;
        use futures::StreamExt;
        use tokio::io::AsyncWriteExt;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk?)
                .await?;
        }
        file.flush().await?;
        tokio::fs::rename(part, destination).await?;
        Ok(())
    }
}

fn tls13_connector(
    host: &str,
    port: u16,
    identity_private_key_pem: &str,
) -> anyhow::Result<Tls13Connector> {
    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for error in native.errors {
        tracing::warn!(%error, "skipping an unreadable platform trust anchor");
    }
    for authority in native.certs {
        roots
            .add(authority)
            .context("adding native server trust anchor")?;
    }
    ensure!(!roots.is_empty(), "platform trust store contains no certificates");

    let key = rcgen::KeyPair::from_pem(identity_private_key_pem)
        .context("parsing P-256 worker identity private key")?;
    ensure!(
        key.algorithm() == &rcgen::PKCS_ECDSA_P256_SHA256,
        "worker identity key must use P-256"
    );
    let mut params = rcgen::CertificateParams::default();
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::minutes(1);
    params.not_after = now + time::Duration::hours(1);
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let wrapper = params
        .self_signed(&key)
        .context("creating ephemeral worker TLS wrapper")?;
    let certificate_chain = vec![wrapper.der().clone()];
    let private_key = PrivatePkcs8KeyDer::from(key.serialize_der()).into();
    let mut config = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_client_auth_cert(certificate_chain, private_key)
        .context("building TLS 1.3 worker client configuration")?;
    config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(Tls13Connector {
        host: Arc::from(host),
        port,
        server_name: ServerName::try_from(host.to_owned())
            .context("fleet server name is invalid")?,
        tls: tokio_rustls::TlsConnector::from(Arc::new(config)),
    })
}

fn delegated_artifact_url(raw: &str) -> anyhow::Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).context("artifact grant contains an invalid URL")?;
    ensure!(
        url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
        "artifact grant URL must not contain user info or a fragment"
    );
    let loopback_http = url.scheme() == "http"
        && url
            .host_str()
            .is_some_and(|host| {
                host.eq_ignore_ascii_case("localhost")
                    || host
                        .trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            });
    ensure!(
        url.scheme() == "https" || loopback_http,
        "artifact grant URL must use HTTPS (HTTP is accepted only on loopback)"
    );
    Ok(url)
}

fn fleet_error(path: &str, status: tonic::Status) -> anyhow::Error {
    let detail = status_detail(&status);
    let retryable = detail.as_ref().map_or_else(
        || matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded),
        |detail| detail.retryable,
    );
    let code = detail
        .as_ref()
        .map_or_else(|| format!("{:?}", status.code()), |detail| detail.code.clone());
    let retry_after = detail
        .and_then(|detail| detail.retry_after_ms)
        .map(Duration::from_millis);
    anyhow::Error::new(FleetClientError {
        path: path.into(),
        code,
        message: status.message().into(),
        retryable,
        retry_after,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use sbgh_fleet::{
        ArtifactDescriptor, Assignment, AssignmentContext, AttemptIdentity, DesiredState,
        HeaderValue as DomainHeaderValue, LeaseToken, PROTOCOL_VERSION, ReliableEventPayload,
        RepositoryToken, ResourceFacts, TaskPayload, TerminalOutcome, WorkerCapability,
    };
    use sbgh_proto::FleetRpcError;
    use sbgh_proto::Wire;
    use sbgh_proto::fleet::v1::worker_fleet_service_server::{
        WorkerFleetService, WorkerFleetServiceServer,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};
    use uuid::Uuid;

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingFleet {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl RecordingFleet {
        fn record(&self, name: &'static str) {
            self.calls
                .lock()
                .unwrap()
                .push(name);
        }
    }

    fn identity() -> AttemptIdentity {
        AttemptIdentity {
            worker_session_id: Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
            attempt_id: Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
            fencing_generation: 7,
            lease_token: LeaseToken("a".repeat(64)),
        }
    }

    fn assignment() -> Assignment {
        let payload = TaskPayload::BuildOnly;
        Assignment {
            identity: identity(),
            trace_id: Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap(),
            context: AssignmentContext {
                job_id: Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap(),
                repository: "stacks-network/stacks-core".into(),
                commit: "1".repeat(40),
            },
            payload_hash: sbgh_fleet::payload_digest(&payload).unwrap(),
            payload,
            vcpu_cpuset: None,
        }
    }

    #[tonic::async_trait]
    impl WorkerFleetService for RecordingFleet {
        async fn register(
            &self,
            request: Request<wire::RegisterRequest>,
        ) -> Result<Response<wire::RegisterResponse>, Status> {
            self.record("Register");
            let request = request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            assert_eq!(request.protocol_version, PROTOCOL_VERSION);
            Ok(Response::new(wire::RegisterResponse::from_domain(RegisterSessionResponse {
                protocol_version: PROTOCOL_VERSION,
                heartbeat_interval_ms: 1_000,
                lease_ttl_ms: 5_000,
                server_time_ms: 1_700_000_000_000,
            })))
        }

        async fn poll(
            &self,
            request: Request<wire::PollRequest>,
        ) -> Result<Response<wire::PollResponse>, Status> {
            self.record("Poll");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::PollResponse::from_domain(PollResponse::NoWork {
                retry_after_ms: 1_000,
            })))
        }

        async fn accept(
            &self,
            request: Request<wire::AcceptRequest>,
        ) -> Result<Response<wire::AcceptResponse>, Status> {
            self.record("Accept");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::AcceptResponse::from_domain(AcceptOfferResponse {
                assignment: assignment(),
            })))
        }

        async fn fetch_repository_credential(
            &self,
            request: Request<wire::FetchRepositoryCredentialRequest>,
        ) -> Result<Response<wire::FetchRepositoryCredentialResponse>, Status> {
            self.record("FetchRepositoryCredential");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::FetchRepositoryCredentialResponse::from_domain(
                RepositoryCredentialResponse {
                    token: RepositoryToken("secret".into()),
                    expires_at_ms: 1_700_000_030_000,
                },
            )))
        }

        async fn heartbeat(
            &self,
            request: Request<wire::HeartbeatRequest>,
        ) -> Result<Response<wire::HeartbeatResponse>, Status> {
            self.record("Heartbeat");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::HeartbeatResponse::from_domain(HeartbeatResponse {
                desired_state: DesiredState::Continue,
                lease_expires_at_ms: 1_700_000_030_000,
                highest_contiguous_reliable_seq: 2,
            })))
        }

        async fn publish_reliable_event(
            &self,
            request: Request<wire::PublishReliableEventRequest>,
        ) -> Result<Response<wire::PublishReliableEventResponse>, Status> {
            self.record("PublishReliableEvent");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::PublishReliableEventResponse::from_domain(ReliableEventAck {
                highest_contiguous_reliable_seq: 2,
            })))
        }

        async fn publish_progress(
            &self,
            request: Request<wire::PublishProgressRequest>,
        ) -> Result<Response<wire::PublishProgressResponse>, Status> {
            self.record("PublishProgress");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::PublishProgressResponse::from_domain(true)))
        }

        async fn grant_artifact(
            &self,
            request: Request<wire::GrantArtifactRequest>,
        ) -> Result<Response<wire::GrantArtifactResponse>, Status> {
            self.record("GrantArtifact");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::GrantArtifactResponse::from_domain(ArtifactGrantResponse {
                method: "PUT".into(),
                key: "attempt/result.json".into(),
                url: "https://objects.example/result".into(),
                headers: vec![DomainHeaderValue {
                    name: "content-type".into(),
                    value: "application/json".into(),
                }],
                expires_at_ms: 1_700_000_030_000,
            })))
        }

        async fn complete_attempt(
            &self,
            request: Request<wire::CompleteAttemptRequest>,
        ) -> Result<Response<wire::CompleteAttemptResponse>, Status> {
            self.record("CompleteAttempt");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::CompleteAttemptResponse::from_domain(CompleteAttemptResponse {
                accepted: true,
            })))
        }

        async fn list_cleanup(
            &self,
            request: Request<wire::ListCleanupRequest>,
        ) -> Result<Response<wire::ListCleanupResponse>, Status> {
            self.record("ListCleanup");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::ListCleanupResponse::from_domain(vec![CleanupItem {
                id: 9,
                attempt_id: identity().attempt_id,
                job_id: assignment().context.job_id,
                reason: "restart".into(),
            }])))
        }

        async fn complete_cleanup(
            &self,
            request: Request<wire::CompleteCleanupRequest>,
        ) -> Result<Response<wire::CompleteCleanupResponse>, Status> {
            self.record("CompleteCleanup");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::CompleteCleanupResponse::from_domain(true)))
        }

        async fn deregister(
            &self,
            request: Request<wire::DeregisterRequest>,
        ) -> Result<Response<wire::DeregisterResponse>, Status> {
            self.record("Deregister");
            request
                .into_inner()
                .into_domain()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(Response::new(wire::DeregisterResponse::from_domain(true)))
        }
    }

    #[test]
    fn delegated_artifact_urls_are_tls_only_except_for_loopback_tests() {
        assert!(delegated_artifact_url("https://objects.example/key?signature=x").is_ok());
        assert!(delegated_artifact_url("http://127.0.0.1:9100/key?signature=x").is_ok());
        assert!(delegated_artifact_url("http://[::1]:9100/key?signature=x").is_ok());
        assert!(delegated_artifact_url("http://objects.example/key?signature=x").is_err());
        assert!(delegated_artifact_url("https://user@objects.example/key").is_err());
        assert!(delegated_artifact_url("https://objects.example/key#fragment").is_err());
    }

    #[test]
    fn structured_grpc_status_preserves_stable_retry_contract() {
        let error = fleet_error(
            "Heartbeat",
            FleetRpcError {
                status: Code::ResourceExhausted,
                code: "worker_busy".into(),
                message: "diagnostic only".into(),
                retryable: true,
                retry_after_ms: Some(2_500),
            }
            .into_status(),
        );
        let error = error
            .downcast_ref::<FleetClientError>()
            .unwrap();
        assert_eq!(error.path, "Heartbeat");
        assert_eq!(error.code, "worker_busy");
        assert_eq!(error.message, "diagnostic only");
        assert!(error.retryable);
        assert_eq!(error.retry_after, Some(Duration::from_millis(2_500)));
    }

    #[test]
    fn unstructured_transport_status_uses_retryable_grpc_classification() {
        let error = fleet_error("Poll", Status::unavailable("connection lost"));
        let error = error
            .downcast_ref::<FleetClientError>()
            .unwrap();
        assert_eq!(error.code, "Unavailable");
        assert!(error.retryable);
        assert_eq!(error.retry_after, None);
    }

    #[tokio::test]
    async fn generated_client_and_server_cover_every_control_rpc() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let fleet = RecordingFleet::default();
        let server_fleet = fleet.clone();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming_shutdown(
                    WorkerFleetServiceServer::new(server_fleet),
                    TcpListenerStream::new(listener),
                    server_shutdown.cancelled_owned(),
                )
                .await
                .unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = FleetClient {
            control: WorkerFleetServiceClient::new(channel),
            artifact_http: reqwest::Client::builder()
                .build()
                .unwrap(),
        };
        let session_id = identity().worker_session_id;
        let trace_id = assignment().trace_id;

        client
            .register(&RegisterSessionRequest {
                protocol_version: PROTOCOL_VERSION,
                worker_session_id: session_id,
                software_version: "test".into(),
                advertised_capabilities: std::collections::BTreeSet::from([
                    WorkerCapability::BuildOnly,
                ]),
                resources: ResourceFacts {
                    logical_cpus: 8,
                    memory_bytes: 32 * 1024 * 1024 * 1024,
                },
            })
            .await
            .unwrap();
        client
            .poll(&PollRequest { worker_session_id: session_id })
            .await
            .unwrap();
        client
            .accept(&AcceptOfferRequest { identity: identity() })
            .await
            .unwrap();
        client
            .repository_credential(&RepositoryCredentialRequest { identity: identity() })
            .await
            .unwrap();
        client
            .heartbeat(&HeartbeatRequest {
                identity: identity(),
                reliable_buffer_len: 0,
            })
            .await
            .unwrap();
        client
            .event(&ReliableEventEnvelope {
                identity: identity(),
                trace_id,
                reliable_seq: 2,
                payload: ReliableEventPayload::Phase {
                    label: "building".into(),
                    elapsed_ms: 10,
                },
                payload_digest: "b".repeat(64),
                worker_timestamp_ms: 1_700_000_000_000,
            })
            .await
            .unwrap();
        client
            .progress(&ProgressRequest {
                identity: identity(),
                trace_id,
                progress_seq: 1,
                update: sbgh_fleet::ProgressUpdate {
                    workflow_step: "build".into(),
                    run_index: 0,
                    requested_run_count: 1,
                    phase: "compiling".into(),
                    progress: 1,
                    total: Some(2),
                    message: None,
                },
            })
            .await
            .unwrap();
        client
            .artifact_grant(&ArtifactGrantRequest {
                identity: identity(),
                operation: sbgh_fleet::ArtifactOperation::Put,
                key: "result.json".into(),
                size: Some(2),
                sha256: Some("c".repeat(64)),
            })
            .await
            .unwrap();
        client
            .complete(&CompleteAttemptRequest {
                identity: identity(),
                trace_id,
                terminal_reliable_seq: 2,
                terminal_payload_digest: "d".repeat(64),
                outcome: TerminalOutcome::Cancelled { reason: "test".into() },
                artifacts: vec![ArtifactDescriptor {
                    key: "attempt/result.json".into(),
                    logical_key: "result.json".into(),
                    size: 2,
                    sha256: "c".repeat(64),
                }],
            })
            .await
            .unwrap();
        client
            .cleanup(session_id)
            .await
            .unwrap();
        client
            .complete_cleanup(session_id, 9)
            .await
            .unwrap();
        client
            .deregister(session_id)
            .await
            .unwrap();

        assert_eq!(
            *fleet.calls.lock().unwrap(),
            [
                "Register",
                "Poll",
                "Accept",
                "FetchRepositoryCredential",
                "Heartbeat",
                "PublishReliableEvent",
                "PublishProgress",
                "GrantArtifact",
                "CompleteAttempt",
                "ListCleanup",
                "CompleteCleanup",
                "Deregister",
            ]
        );
        shutdown.cancel();
        server.await.unwrap();
    }
}
