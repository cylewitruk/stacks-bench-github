use std::path::Path;
use std::time::Duration;

use anyhow::{Context, ensure};
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{Certificate, Identity, StatusCode};
use sbgh_proto::{
    AcceptOfferRequest, AcceptOfferResponse, ApiError, ArtifactGrantRequest, ArtifactGrantResponse,
    CleanupCompleteRequest, CleanupItem, CleanupListRequest, CompleteAttemptRequest,
    CompleteAttemptResponse, DeregisterSessionRequest, HeartbeatRequest, HeartbeatResponse,
    MAX_LONG_POLL_SECS, PROTOCOL_VERSION, PollRequest, PollResponse, ProgressRequest,
    RegisterSessionRequest, RegisterSessionResponse, ReliableEventAck, ReliableEventEnvelope,
    RepositoryCredentialRequest, RepositoryCredentialResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Clone)]
pub struct FleetClient {
    base_url: String,
    control_http: reqwest::Client,
    artifact_http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
#[error("fleet API {path} failed: {code}: {message}")]
pub struct FleetApiError {
    pub path: String,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after: Option<Duration>,
}

impl FleetClient {
    pub fn build(
        base_url: &str,
        client_certificate: &Path,
        client_private_key: &Path,
        server_ca_certificate: &Path,
    ) -> anyhow::Result<Self> {
        require_private_permissions(client_private_key)?;
        let mut identity_pem = std::fs::read(client_certificate).with_context(|| {
            format!("reading worker certificate {}", client_certificate.display())
        })?;
        identity_pem.extend_from_slice(&std::fs::read(client_private_key).with_context(|| {
            format!("reading worker private key {}", client_private_key.display())
        })?);
        let identity = Identity::from_pem(&identity_pem)
            .context("parsing worker certificate/private key identity")?;
        let ca =
            Certificate::from_pem(&std::fs::read(server_ca_certificate).with_context(|| {
                format!("reading server CA {}", server_ca_certificate.display())
            })?)
            .context("parsing server CA certificate")?;
        let control_http = reqwest::Client::builder()
            .identity(identity)
            .tls_certs_only([ca])
            .https_only(true)
            .min_tls_version(reqwest::tls::Version::TLS_1_3)
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(MAX_LONG_POLL_SECS + 15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("building mTLS fleet client")?;
        let artifact_http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("building delegated artifact client")?;
        Ok(Self {
            base_url: base_url
                .trim_end_matches('/')
                .into(),
            control_http,
            artifact_http,
        })
    }

    pub async fn register(
        &self,
        request: &RegisterSessionRequest,
    ) -> anyhow::Result<RegisterSessionResponse> {
        self.post("/v1/register", request)
            .await
    }

    pub async fn poll(&self, request: &PollRequest) -> anyhow::Result<PollResponse> {
        self.post("/v1/poll", request)
            .await
    }

    pub async fn accept(
        &self,
        request: &AcceptOfferRequest,
    ) -> anyhow::Result<AcceptOfferResponse> {
        self.post("/v1/accept", request)
            .await
    }

    pub async fn heartbeat(&self, request: &HeartbeatRequest) -> anyhow::Result<HeartbeatResponse> {
        self.post("/v1/heartbeat", request)
            .await
    }

    pub async fn repository_credential(
        &self,
        request: &RepositoryCredentialRequest,
    ) -> anyhow::Result<RepositoryCredentialResponse> {
        self.post("/v1/repository-credential", request)
            .await
    }

    pub async fn event(&self, request: &ReliableEventEnvelope) -> anyhow::Result<ReliableEventAck> {
        self.post("/v1/events", request)
            .await
    }

    pub async fn progress(&self, request: &ProgressRequest) -> anyhow::Result<()> {
        let _: serde_json::Value = self
            .post("/v1/progress", request)
            .await?;
        Ok(())
    }

    pub async fn artifact_grant(
        &self,
        request: &ArtifactGrantRequest,
    ) -> anyhow::Result<ArtifactGrantResponse> {
        self.post("/v1/artifacts/grant", request)
            .await
    }

    pub async fn complete(
        &self,
        request: &CompleteAttemptRequest,
    ) -> anyhow::Result<CompleteAttemptResponse> {
        self.post("/v1/complete", request)
            .await
    }

    pub async fn cleanup(&self, session_id: uuid::Uuid) -> anyhow::Result<Vec<CleanupItem>> {
        self.get(
            "/v1/cleanup",
            &CleanupListRequest {
                protocol_version: PROTOCOL_VERSION,
                worker_session_id: session_id,
            },
        )
        .await
    }

    pub async fn complete_cleanup(
        &self,
        session_id: uuid::Uuid,
        obligation_id: i64,
    ) -> anyhow::Result<()> {
        let _: serde_json::Value = self
            .post(
                "/v1/cleanup",
                &CleanupCompleteRequest {
                    protocol_version: PROTOCOL_VERSION,
                    worker_session_id: session_id,
                    obligation_id,
                },
            )
            .await?;
        Ok(())
    }

    pub async fn deregister(&self, session_id: uuid::Uuid) -> anyhow::Result<()> {
        let _: serde_json::Value = self
            .post(
                "/v1/deregister",
                &DeregisterSessionRequest {
                    protocol_version: PROTOCOL_VERSION,
                    worker_session_id: session_id,
                },
            )
            .await?;
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

    async fn post<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
    ) -> anyhow::Result<R> {
        let response = self
            .control_http
            .post(format!("{}{path}", self.base_url))
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        decode(path, response).await
    }

    async fn get<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        path: &str,
        query: &T,
    ) -> anyhow::Result<R> {
        let response = self
            .control_http
            .get(format!("{}{path}", self.base_url))
            .query(query)
            .send()
            .await
            .with_context(|| format!("GET {path}"))?;
        decode(path, response).await
    }
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

fn require_private_permissions(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .with_context(|| format!("stat worker private key {}", path.display()))?
            .permissions()
            .mode();
        ensure!(
            mode & 0o077 == 0,
            "worker private key {} must not be accessible by group/other",
            path.display()
        );
    }
    Ok(())
}

async fn decode<R: DeserializeOwned>(path: &str, response: reqwest::Response) -> anyhow::Result<R> {
    let status = response.status();
    if status.is_success() {
        return response
            .json()
            .await
            .with_context(|| format!("decoding successful response from {path}"));
    }
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs);
    let error = response
        .json::<ApiError>()
        .await
        .unwrap_or(ApiError {
            code: "invalid_error_response".into(),
            message: format!("HTTP {status}"),
            retryable: status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
        });
    Err(anyhow::Error::new(FleetApiError {
        path: path.into(),
        code: error.code,
        message: error.message,
        retryable: error.retryable,
        retry_after,
    }))
}

#[cfg(test)]
mod tests {
    use super::delegated_artifact_url;

    #[test]
    fn delegated_artifact_urls_are_tls_only_except_for_loopback_tests() {
        assert!(delegated_artifact_url("https://objects.example/key?signature=x").is_ok());
        assert!(delegated_artifact_url("http://127.0.0.1:9100/key?signature=x").is_ok());
        assert!(delegated_artifact_url("http://[::1]:9100/key?signature=x").is_ok());
        assert!(delegated_artifact_url("http://objects.example/key?signature=x").is_err());
        assert!(delegated_artifact_url("https://user@objects.example/key").is_err());
        assert!(delegated_artifact_url("https://objects.example/key#fragment").is_err());
    }
}
