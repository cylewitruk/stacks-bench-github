use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, ensure};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use uuid::Uuid;
use x509_parser::extensions::GeneralName;
use x509_parser::parse_x509_certificate;

const WORKER_URI_PREFIX: &str = "urn:sbgh:worker:";
const MAX_PENDING_HANDSHAKES: usize = 128;
const ACCEPTED_CONNECTION_BUFFER: usize = 128;

#[derive(Debug, Clone)]
pub struct AuthenticatedPeer {
    pub worker_id: Uuid,
    pub certificate_sha256: String,
    pub socket_addr: SocketAddr,
}

pub struct MtlsListener {
    local_addr: SocketAddr,
    accepted: mpsc::Receiver<AuthenticatedStream>,
    accept_task: JoinHandle<()>,
}

pub struct AuthenticatedStream {
    inner: TlsStream<TcpStream>,
    peer: AuthenticatedPeer,
}

impl tonic::transport::server::Connected for AuthenticatedStream {
    type ConnectInfo = AuthenticatedPeer;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.peer.clone()
    }
}

impl AsyncRead for AuthenticatedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for AuthenticatedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

impl MtlsListener {
    pub async fn bind(
        address: SocketAddr,
        server_certificate: &Path,
        server_private_key: &Path,
        client_ca_certificate: &Path,
    ) -> anyhow::Result<Self> {
        let config = server_config(server_certificate, server_private_key, client_ca_certificate)?;
        let tcp = TcpListener::bind(address)
            .await
            .with_context(|| format!("binding worker listener on {address}"))?;
        let local_addr = tcp
            .local_addr()
            .context("reading worker listener address")?;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let (accepted_tx, accepted) = mpsc::channel(ACCEPTED_CONNECTION_BUFFER);
        let accept_task = tokio::spawn(run_accept_loop(tcp, acceptor, accepted_tx));
        Ok(Self {
            local_addr,
            accepted,
            accept_task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for MtlsListener {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn run_accept_loop(
    tcp: TcpListener,
    acceptor: TlsAcceptor,
    accepted: mpsc::Sender<AuthenticatedStream>,
) {
    let handshakes = Arc::new(Semaphore::new(MAX_PENDING_HANDSHAKES));
    loop {
        let (stream, socket_addr) = match tcp.accept().await {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%error, "worker listener accept failed");
                continue;
            }
        };
        let Ok(permit) = handshakes
            .clone()
            .try_acquire_owned()
        else {
            tracing::warn!(%socket_addr, "worker mTLS handshake capacity exhausted");
            continue;
        };
        let acceptor = acceptor.clone();
        let accepted = accepted.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let tls = match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                acceptor.accept(stream),
            )
            .await
            {
                Ok(Ok(tls)) => tls,
                Ok(Err(error)) => {
                    tracing::warn!(%socket_addr, %error, "worker mTLS handshake rejected");
                    return;
                }
                Err(_) => {
                    tracing::warn!(%socket_addr, "worker mTLS handshake timed out");
                    return;
                }
            };
            let worker_id = match peer_worker_id(&tls) {
                Ok(worker_id) => worker_id,
                Err(error) => {
                    tracing::warn!(%socket_addr, %error, "worker certificate identity rejected");
                    return;
                }
            };
            let certificate_sha256 = match peer_certificate_sha256(&tls) {
                Ok(fingerprint) => fingerprint,
                Err(error) => {
                    tracing::warn!(
                        %socket_addr,
                        %error,
                        "worker certificate fingerprint could not be derived"
                    );
                    return;
                }
            };
            let _ = accepted
                .send(AuthenticatedStream {
                    inner: tls,
                    peer: AuthenticatedPeer {
                        worker_id,
                        certificate_sha256,
                        socket_addr,
                    },
                })
                .await;
        });
    }
}

impl tokio_stream::Stream for MtlsListener {
    type Item = io::Result<AuthenticatedStream>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.accepted
            .poll_recv(context)
            .map(|value| value.map(Ok))
    }
}

fn server_config(
    certificate_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> anyhow::Result<ServerConfig> {
    let server_certificates = certificates(certificate_path)?;
    ensure!(!server_certificates.is_empty(), "server certificate chain is empty");
    let key = private_key(key_path)?;
    let mut roots = RootCertStore::empty();
    for certificate in certificates(ca_path)? {
        roots
            .add(certificate)
            .context("adding worker CA certificate to trust store")?;
    }
    ensure!(!roots.is_empty(), "worker CA certificate file is empty");
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .context("building mandatory client-certificate verifier")?;
    let mut config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_client_cert_verifier(verifier)
        .with_single_cert(server_certificates, key)
        .context("building TLS 1.3 worker server configuration")?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(config)
}

fn certificates(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let bytes =
        std::fs::read(path).with_context(|| format!("reading certificate {}", path.display()))?;
    rustls_pemfile::certs(&mut BufReader::new(bytes.as_slice()))
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parsing certificate {}", path.display()))
}

fn private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .with_context(|| format!("stat private key {}", path.display()))?
            .permissions()
            .mode();
        ensure!(
            mode & 0o077 == 0,
            "private key {} must not be accessible by group/other",
            path.display()
        );
    }
    let bytes =
        std::fs::read(path).with_context(|| format!("reading private key {}", path.display()))?;
    rustls_pemfile::private_key(&mut BufReader::new(bytes.as_slice()))
        .with_context(|| format!("parsing private key {}", path.display()))?
        .context("private key PEM contains no supported key")
}

fn peer_worker_id(stream: &TlsStream<TcpStream>) -> anyhow::Result<Uuid> {
    let certificates = stream
        .get_ref()
        .1
        .peer_certificates()
        .context("authenticated worker presented no certificate")?;
    ensure!(!certificates.is_empty(), "authenticated worker certificate chain is empty");
    worker_id_from_certificate(certificates[0].as_ref())
}

fn peer_certificate_sha256(stream: &TlsStream<TcpStream>) -> anyhow::Result<String> {
    let leaf = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .context("authenticated worker presented no leaf certificate")?;
    Ok(hex::encode(Sha256::digest(leaf.as_ref())))
}

fn worker_id_from_certificate(der: &[u8]) -> anyhow::Result<Uuid> {
    let (_, certificate) = parse_x509_certificate(der)
        .map_err(|error| anyhow::anyhow!("parsing worker X.509: {error}"))?;
    let san = certificate
        .subject_alternative_name()
        .context("reading worker URI SAN")?
        .context("worker certificate has no subjectAltName")?;
    let uri_names = san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        })
        .collect::<Vec<_>>();
    ensure!(uri_names.len() == 1, "worker certificate must contain exactly one URI SAN");
    let identity = uri_names[0]
        .strip_prefix(WORKER_URI_PREFIX)
        .context("worker URI SAN is not an sbgh worker identity")?;
    Uuid::parse_str(identity).context("worker URI SAN contains an invalid UUID")
}

#[cfg(test)]
mod tests {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use rustls::ClientConfig;
    use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
    use tokio_rustls::TlsConnector;
    use tokio_stream::StreamExt;

    use super::*;

    fn certificate_with_uris(uris: &[String]) -> Vec<u8> {
        let mut parameters = CertificateParams::default();
        parameters.subject_alt_names = uris
            .iter()
            .map(|uri| {
                SanType::URI(
                    uri.as_str()
                        .try_into()
                        .unwrap(),
                )
            })
            .collect();
        parameters
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ClientAuth);
        let key = KeyPair::generate().unwrap();
        parameters
            .self_signed(&key)
            .unwrap()
            .der()
            .to_vec()
    }

    #[test]
    fn worker_identity_comes_only_from_one_exact_uri_san() {
        let worker_id = Uuid::new_v4();
        let valid = certificate_with_uris(&[format!("{WORKER_URI_PREFIX}{worker_id}")]);
        assert_eq!(worker_id_from_certificate(&valid).unwrap(), worker_id);

        let foreign = certificate_with_uris(&[format!("urn:other:{worker_id}")]);
        assert!(worker_id_from_certificate(&foreign).is_err());

        let ambiguous = certificate_with_uris(&[
            format!("{WORKER_URI_PREFIX}{worker_id}"),
            "urn:other:identity".into(),
        ]);
        assert!(worker_id_from_certificate(&ambiguous).is_err());

        let cn_only = certificate_with_uris(&[]);
        assert!(worker_id_from_certificate(&cn_only).is_err());
    }

    struct PkiFixture {
        _directory: tempfile::TempDir,
        server_certificate: std::path::PathBuf,
        server_key: std::path::PathBuf,
        ca_certificate: std::path::PathBuf,
        client_certificate: CertificateDer<'static>,
        client_key: PrivateKeyDer<'static>,
    }

    fn pki_fixture(worker_id: Uuid) -> PkiFixture {
        let directory = tempfile::tempdir().unwrap();
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_key = KeyPair::generate().unwrap();
        let ca = CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

        let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        server_params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ServerAuth);
        let server_key = KeyPair::generate().unwrap();
        let server = server_params
            .signed_by(&server_key, &ca)
            .unwrap();

        let mut client_params = CertificateParams::default();
        client_params.subject_alt_names = vec![SanType::URI(
            format!("{WORKER_URI_PREFIX}{worker_id}")
                .try_into()
                .unwrap(),
        )];
        client_params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ClientAuth);
        let client_key = KeyPair::generate().unwrap();
        let client = client_params
            .signed_by(&client_key, &ca)
            .unwrap();

        let server_certificate = directory
            .path()
            .join("server.crt");
        let server_key_path = directory
            .path()
            .join("server.key");
        let ca_certificate = directory
            .path()
            .join("ca.crt");
        std::fs::write(&server_certificate, server.pem()).unwrap();
        std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
        std::fs::write(&ca_certificate, ca.pem()).unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(
            &server_key_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap();

        PkiFixture {
            _directory: directory,
            server_certificate,
            server_key: server_key_path,
            ca_certificate,
            client_certificate: client.der().clone(),
            client_key: PrivatePkcs8KeyDer::from(client_key.serialize_der()).into(),
        }
    }

    #[tokio::test]
    async fn tls13_handshake_authenticates_the_worker_uri_san() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let worker_id = Uuid::new_v4();
        let fixture = pki_fixture(worker_id);
        let expected_fingerprint = hex::encode(Sha256::digest(
            fixture
                .client_certificate
                .as_ref(),
        ));
        let server = server_config(
            &fixture.server_certificate,
            &fixture.server_key,
            &fixture.ca_certificate,
        )
        .unwrap();
        let tcp = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = tcp.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = tcp.accept().await.unwrap();
            TlsAcceptor::from(Arc::new(server))
                .accept(stream)
                .await
        });

        let mut roots = RootCertStore::empty();
        roots
            .add(
                certificates(&fixture.ca_certificate)
                    .unwrap()
                    .remove(0),
            )
            .unwrap();
        let mut client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![fixture.client_certificate], fixture.client_key)
            .unwrap();
        client.alpn_protocols = vec![b"h2".to_vec()];
        let stream = TcpStream::connect(address)
            .await
            .unwrap();
        let tls = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("localhost").unwrap(), stream)
            .await
            .unwrap();
        let server_stream = accept.await.unwrap().unwrap();
        assert_eq!(peer_worker_id(&server_stream).unwrap(), worker_id);
        assert_eq!(peer_certificate_sha256(&server_stream).unwrap(), expected_fingerprint);
        assert_eq!(
            tls.get_ref()
                .1
                .protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3)
        );
        assert_eq!(
            tls.get_ref()
                .1
                .alpn_protocol(),
            Some(b"h2".as_slice())
        );
    }

    #[tokio::test]
    async fn tls12_clients_are_rejected() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let fixture = pki_fixture(Uuid::new_v4());
        let server = server_config(
            &fixture.server_certificate,
            &fixture.server_key,
            &fixture.ca_certificate,
        )
        .unwrap();
        let tcp = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = tcp.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = tcp.accept().await.unwrap();
            TlsAcceptor::from(Arc::new(server))
                .accept(stream)
                .await
        });

        let mut roots = RootCertStore::empty();
        roots
            .add(
                certificates(&fixture.ca_certificate)
                    .unwrap()
                    .remove(0),
            )
            .unwrap();
        let client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![fixture.client_certificate], fixture.client_key)
            .unwrap();
        let stream = TcpStream::connect(address)
            .await
            .unwrap();
        let client_result = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("localhost").unwrap(), stream)
            .await;
        let server_result = accept.await.unwrap();
        assert!(
            client_result.is_err() || server_result.is_err(),
            "TLS 1.2 unexpectedly reached the fleet listener"
        );
    }

    #[tokio::test]
    async fn plaintext_http_cannot_reach_the_authenticated_stream() {
        use tokio::io::AsyncWriteExt;

        let _ = rustls::crypto::ring::default_provider().install_default();
        let fixture = pki_fixture(Uuid::new_v4());
        let mut listener = MtlsListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            &fixture.server_certificate,
            &fixture.server_key,
            &fixture.ca_certificate,
        )
        .await
        .unwrap();
        let mut plaintext = TcpStream::connect(listener.local_addr())
            .await
            .unwrap();
        plaintext
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        plaintext
            .shutdown()
            .await
            .unwrap();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(250), listener.next())
                .await
                .is_err(),
            "plaintext HTTP unexpectedly produced an authenticated fleet stream"
        );
    }

    #[tokio::test]
    async fn stalled_client_hello_does_not_block_an_authenticated_worker() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let worker_id = Uuid::new_v4();
        let fixture = pki_fixture(worker_id);
        let mut listener = MtlsListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            &fixture.server_certificate,
            &fixture.server_key,
            &fixture.ca_certificate,
        )
        .await
        .unwrap();
        let address = listener.local_addr();
        let _stalled = TcpStream::connect(address)
            .await
            .unwrap();

        let mut roots = RootCertStore::empty();
        roots
            .add(
                certificates(&fixture.ca_certificate)
                    .unwrap()
                    .remove(0),
            )
            .unwrap();
        let client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![fixture.client_certificate], fixture.client_key)
            .unwrap();
        let connect = async {
            let stream = TcpStream::connect(address)
                .await
                .unwrap();
            TlsConnector::from(Arc::new(client))
                .connect(ServerName::try_from("localhost").unwrap(), stream)
                .await
                .unwrap()
        };
        let (_, accepted) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(connect, listener.next())
        })
        .await
        .expect("valid worker handshake must bypass the stalled client");
        let accepted = accepted
            .expect("listener remains open")
            .expect("authenticated stream");
        assert_eq!(accepted.peer.worker_id, worker_id);
    }

    #[tokio::test]
    async fn dropping_listener_releases_the_bound_socket() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let fixture = pki_fixture(Uuid::new_v4());
        let listener = MtlsListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            &fixture.server_certificate,
            &fixture.server_key,
            &fixture.ca_certificate,
        )
        .await
        .unwrap();
        let address = listener.local_addr;
        drop(listener);
        tokio::task::yield_now().await;
        TcpListener::bind(address)
            .await
            .expect("listener drop must abort its background accept task");
    }

    #[tokio::test]
    async fn client_certificate_from_an_untrusted_ca_is_rejected() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let trusted = pki_fixture(Uuid::new_v4());
        let untrusted = pki_fixture(Uuid::new_v4());
        let server = server_config(
            &trusted.server_certificate,
            &trusted.server_key,
            &trusted.ca_certificate,
        )
        .unwrap();
        let tcp = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = tcp.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = tcp.accept().await.unwrap();
            TlsAcceptor::from(Arc::new(server))
                .accept(stream)
                .await
        });

        let mut roots = RootCertStore::empty();
        roots
            .add(
                certificates(&trusted.ca_certificate)
                    .unwrap()
                    .remove(0),
            )
            .unwrap();
        let client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![untrusted.client_certificate], untrusted.client_key)
            .unwrap();
        let stream = TcpStream::connect(address)
            .await
            .unwrap();
        let client_result = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("localhost").unwrap(), stream)
            .await;
        let server_result = accept.await.unwrap();
        assert!(
            client_result.is_err() || server_result.is_err(),
            "untrusted client certificate unexpectedly completed mTLS"
        );
    }
}
