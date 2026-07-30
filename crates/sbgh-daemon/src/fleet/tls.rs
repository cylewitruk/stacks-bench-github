use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use anyhow::{Context, ensure};
use p256::pkcs8::{DecodePublicKey, EncodePublicKey};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error as TlsError, ServerConfig,
    SignatureScheme,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use x509_parser::parse_x509_certificate;

const MAX_IDENTITY_BYTES: usize = 16 * 1024;
const MAX_PENDING_HANDSHAKES: usize = 128;
const ACCEPTED_CONNECTION_BUFFER: usize = 128;

#[derive(Debug, Clone)]
pub struct AuthenticatedPeer {
    pub identity_key_sha256: [u8; 32],
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
    ) -> anyhow::Result<Self> {
        let config = server_config(server_certificate, server_private_key)?;
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
            tracing::warn!(%socket_addr, "worker TLS handshake capacity exhausted");
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
                    tracing::warn!(%socket_addr, %error, "worker TLS handshake rejected");
                    return;
                }
                Err(_) => {
                    tracing::warn!(%socket_addr, "worker TLS handshake timed out");
                    return;
                }
            };
            let identity_key_sha256 = match peer_identity_key_sha256(&tls) {
                Ok(digest) => digest,
                Err(error) => {
                    tracing::warn!(%socket_addr, %error, "worker public identity rejected");
                    return;
                }
            };
            let _ = accepted
                .send(AuthenticatedStream {
                    inner: tls,
                    peer: AuthenticatedPeer {
                        identity_key_sha256,
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

fn server_config(certificate_path: &Path, key_path: &Path) -> anyhow::Result<ServerConfig> {
    let certificates = certificates(certificate_path)?;
    ensure!(!certificates.is_empty(), "server certificate chain is empty");
    let mut config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_client_cert_verifier(Arc::new(PossessionVerifier))
        .with_single_cert(certificates, private_key(key_path)?)
        .context("building TLS 1.3 worker server configuration")?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(config)
}

#[derive(Debug)]
struct PossessionVerifier;

impl ClientCertVerifier for PossessionVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, TlsError> {
        if !intermediates.is_empty()
            || canonical_spki_from_certificate(end_entity.as_ref()).is_err()
        {
            return Err(TlsError::InvalidCertificate(CertificateError::BadEncoding));
        }
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Err(TlsError::PeerIncompatible(rustls::PeerIncompatible::Tls12NotOffered))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        if dss.scheme != SignatureScheme::ECDSA_NISTP256_SHA256 {
            return Err(TlsError::PeerMisbehaved(
                rustls::PeerMisbehaved::SignedHandshakeWithUnadvertisedSigScheme,
            ));
        }
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ECDSA_NISTP256_SHA256]
    }
}

fn certificates(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let bytes =
        std::fs::read(path).with_context(|| format!("reading certificate {}", path.display()))?;
    rustls_pemfile::certs(&mut BufReader::new(bytes.as_slice()))
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parsing certificate {}", path.display()))
}

pub(crate) fn validate_worker_identity_key(pem: &[u8]) -> anyhow::Result<[u8; 32]> {
    ensure!(pem.len() <= MAX_IDENTITY_BYTES, "worker public key exceeds 16 KiB");
    let mut items = rustls_pemfile::read_all(&mut BufReader::new(pem))
        .collect::<Result<Vec<_>, _>>()
        .context("parsing worker public key PEM")?;
    ensure!(items.len() == 1, "expected exactly one public identity key");
    let spki = match items.remove(0) {
        rustls_pemfile::Item::SubjectPublicKeyInfo(spki) => spki,
        _ => anyhow::bail!("worker enrollment accepts a PUBLIC KEY only"),
    };
    canonical_spki_digest(spki.as_ref())
}

fn canonical_spki_digest(spki: &[u8]) -> anyhow::Result<[u8; 32]> {
    let key =
        p256::PublicKey::from_public_key_der(spki).context("identity key is not P-256 SPKI")?;
    let canonical = key
        .to_public_key_der()
        .context("canonicalizing P-256 identity key")?;
    ensure!(canonical.as_bytes() == spki, "identity key SPKI is not canonical DER");
    Ok(Sha256::digest(canonical.as_bytes()).into())
}

fn canonical_spki_from_certificate(der: &[u8]) -> anyhow::Result<&[u8]> {
    ensure!(der.len() <= MAX_IDENTITY_BYTES, "worker TLS wrapper exceeds 16 KiB");
    let (remainder, certificate) = parse_x509_certificate(der)
        .map_err(|error| anyhow::anyhow!("parsing worker X.509: {error}"))?;
    ensure!(remainder.is_empty(), "worker TLS wrapper has trailing bytes");
    ensure!(
        certificate
            .validity()
            .is_valid(),
        "worker TLS wrapper is not currently valid"
    );
    ensure!(
        certificate
            .validity()
            .not_after
            .timestamp()
            - certificate
                .validity()
                .not_before
                .timestamp()
            <= 24 * 60 * 60,
        "worker TLS wrapper lifetime exceeds 24 hours"
    );
    let usage = certificate
        .extended_key_usage()
        .context("reading worker TLS wrapper extended key usage")?
        .context("worker TLS wrapper lacks extended key usage")?;
    ensure!(
        usage.value.client_auth && !usage.value.any && !usage.value.server_auth,
        "worker TLS wrapper is not client-auth-only"
    );
    let spki = certificate
        .tbs_certificate
        .subject_pki
        .raw;
    canonical_spki_digest(spki)?;
    Ok(spki)
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

fn peer_identity_key_sha256(stream: &TlsStream<TcpStream>) -> anyhow::Result<[u8; 32]> {
    let leaf = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .context("authenticated worker presented no TLS wrapper")?;
    canonical_spki_digest(canonical_spki_from_certificate(leaf.as_ref())?)
}

#[cfg(test)]
mod tests {
    use p256::ecdsa::signature::Signer;
    use p256::ecdsa::{Signature, SigningKey};
    use p256::elliptic_curve::rand_core::OsRng;
    use p256::pkcs8::{EncodePrivateKey, LineEnding};
    use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair};
    use rustls::ClientConfig;
    use rustls::internal::msgs::codec::{Codec, Reader};
    use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    use super::*;

    #[test]
    fn enrollment_accepts_only_canonical_p256_public_keys() {
        let secret = p256::SecretKey::random(&mut OsRng);
        let public = secret
            .public_key()
            .to_public_key_pem(LineEnding::LF)
            .unwrap();
        assert_eq!(
            validate_worker_identity_key(public.as_bytes()).unwrap(),
            Sha256::digest(
                secret
                    .public_key()
                    .to_public_key_der()
                    .unwrap()
                    .as_bytes()
            )
            .as_slice()
        );
        let private = secret
            .to_pkcs8_pem(LineEnding::LF)
            .unwrap();
        assert!(validate_worker_identity_key(private.as_bytes()).is_err());
        let wrapper_key = KeyPair::generate().unwrap();
        let wrapper = CertificateParams::default()
            .self_signed(&wrapper_key)
            .unwrap();
        assert!(validate_worker_identity_key(wrapper.pem().as_bytes()).is_err());
        assert!(validate_worker_identity_key(b"not a key").is_err());
    }

    #[tokio::test]
    async fn tls_handshake_accepts_a_matching_identity_key() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server_key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server = server_params
            .self_signed(&server_key)
            .unwrap();

        let client_key = KeyPair::generate().unwrap();
        let mut client_params = CertificateParams::default();
        let now = time::OffsetDateTime::now_utc();
        client_params.not_before = now - time::Duration::minutes(1);
        client_params.not_after = now + time::Duration::hours(1);
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client = client_params
            .self_signed(&client_key)
            .unwrap();

        let server_config =
            ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(Arc::new(PossessionVerifier))
                .with_single_cert(
                    vec![server.der().clone()],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
                )
                .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(server.der().clone())
            .unwrap();
        let client_config =
            ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_root_certificates(roots)
                .with_client_auth_cert(
                    vec![client.der().clone()],
                    PrivatePkcs8KeyDer::from(client_key.serialize_der()).into(),
                )
                .unwrap();

        let (client_io, server_io) = tokio::io::duplex(16 * 1024);
        let accept = TlsAcceptor::from(Arc::new(server_config)).accept(server_io);
        let connect = TlsConnector::from(Arc::new(client_config))
            .connect(ServerName::try_from("localhost").unwrap(), client_io);
        let (accepted, connected) = tokio::join!(accept, connect);
        accepted.unwrap();
        connected.unwrap();
    }

    #[test]
    fn possession_verifier_rejects_a_signature_from_an_unrelated_key() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let victim_key = KeyPair::generate().unwrap();
        let mut victim_params = CertificateParams::default();
        let now = time::OffsetDateTime::now_utc();
        victim_params.not_before = now - time::Duration::minutes(1);
        victim_params.not_after = now + time::Duration::hours(1);
        victim_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let victim = victim_params
            .self_signed(&victim_key)
            .unwrap();

        let message = b"TLS 1.3 CertificateVerify adversarial fixture";
        let attacker = SigningKey::random(&mut OsRng);
        let forged: Signature = attacker.sign(message);
        let forged_der = forged.to_der();

        // `DigitallySignedStruct::new` is crate-private in rustls. Decode its
        // normal TLS wire representation through rustls's explicitly
        // test-oriented internal codec.
        let mut encoded = Vec::new();
        SignatureScheme::ECDSA_NISTP256_SHA256.encode(&mut encoded);
        encoded.extend_from_slice(
            &u16::try_from(forged_der.as_bytes().len())
                .unwrap()
                .to_be_bytes(),
        );
        encoded.extend_from_slice(forged_der.as_bytes());
        let mut reader = Reader::init(&encoded);
        let forged_dss = DigitallySignedStruct::read(&mut reader).unwrap();
        reader
            .expect_empty("forged DigitallySignedStruct")
            .unwrap();

        let result = PossessionVerifier.verify_tls13_signature(message, victim.der(), &forged_dss);
        assert!(
            result.is_err(),
            "daemon verifier accepted CertificateVerify from an unrelated private key"
        );
    }
}
