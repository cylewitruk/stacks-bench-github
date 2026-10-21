//! Long-lived connector tests with real TLS and a controlled wall clock.

use std::assert_matches;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, PublicKeyData};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::time_provider::TimeProvider;
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error, RootCertStore, ServerConfig,
    SignatureScheme,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use x509_parser::parse_x509_certificate;

use super::super::Tls13Connector;

/// Deterministic issuance and verification time; no process-wide clock changes.
#[derive(Debug)]
struct Clock(AtomicU64);

impl TimeProvider for Clock {
    fn current_time(&self) -> Option<UnixTime> {
        Some(UnixTime::since_unix_epoch(Duration::from_secs(self.0.load(Ordering::SeqCst))))
    }
}

/// Test peer requires the enrolled key, current validity and a real TLS signature.
#[derive(Debug)]
struct VerifyIdentity(Vec<u8>);

impl ClientCertVerifier for VerifyIdentity {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        assert!(intermediates.is_empty());
        let (_, cert) = parse_x509_certificate(leaf.as_ref()).unwrap();
        assert_eq!(cert.public_key().raw, self.0);
        let validity = cert.validity();
        let now = i64::try_from(now.as_secs()).unwrap();
        if now
            < validity
                .not_before
                .timestamp()
        {
            return Err(Error::InvalidCertificate(CertificateError::NotValidYet));
        }
        if now > validity.not_after.timestamp() {
            return Err(Error::InvalidCertificate(CertificateError::Expired));
        }
        assert_eq!(validity.not_after.timestamp() - now, 3600);
        let usage = cert
            .extended_key_usage()
            .unwrap()
            .unwrap();
        assert!(usage.value.client_auth && !usage.value.server_auth && !usage.value.any);
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        panic!("TLS 1.2 must not be negotiated");
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            signature,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ECDSA_NISTP256_SHA256]
    }
}

/// Connector and TLS server share a test clock, but use different signing keys.
fn fixture(port: u16) -> (Tls13Connector, TlsAcceptor, Arc<Clock>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let clock = Arc::new(Clock(AtomicU64::new(1_800_000_000)));
    let now = time::OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params.not_before = now - time::Duration::days(1);
    params.not_after = now + time::Duration::days(30);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = params
        .self_signed(&server_key)
        .unwrap();
    let identity = Arc::new(KeyPair::generate().unwrap());
    let mut server = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_client_cert_verifier(Arc::new(VerifyIdentity(identity.subject_public_key_info())))
        .with_single_cert(
            vec![server_cert.der().clone()],
            PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        )
        .unwrap();
    server.alpn_protocols = vec![b"h2".to_vec()];
    server.time_provider = clock.clone();
    let mut roots = RootCertStore::empty();
    roots
        .add(server_cert.der().clone())
        .unwrap();
    (
        Tls13Connector {
            host: Arc::from("127.0.0.1"),
            port,
            server_name: ServerName::try_from("localhost").unwrap(),
            roots: Arc::new(roots),
            identity,
            clock: clock.clone(),
        },
        TlsAcceptor::from(Arc::new(server)),
        clock,
    )
}

/// A listener restart after the initial wrapper lifetime must not replace identity.
#[tokio::test]
async fn aged_connector_reconnects_after_tls_listener_restart() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let (connector, acceptor, clock) = fixture(address.port());
        let original_config = Arc::new(
            connector
                .client_config()
                .unwrap(),
        );
        let original_cert = exchange(&connector, &acceptor, &listener).await;
        drop(listener);

        // Two hours later the old wrapper is expired, but the resident connector
        // and its already-loaded enrolled key are still the same objects.
        clock
            .0
            .fetch_add(7200, Ordering::SeqCst);
        let listener = TcpListener::bind(address)
            .await
            .unwrap();
        let stale_client = tokio_rustls::TlsConnector::from(original_config);
        let stale_connect = async {
            let tcp = tokio::net::TcpStream::connect(address)
                .await
                .unwrap();
            stale_client
                .connect(ServerName::try_from("localhost").unwrap(), tcp)
                .await
        };
        let stale_accept = async {
            let (tcp, _) = listener
                .accept()
                .await
                .unwrap();
            acceptor.accept(tcp).await
        };
        let (_, rejected) = tokio::join!(stale_connect, stale_accept);
        let error = rejected.unwrap_err();
        assert_matches!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<Error>()),
            Some(Error::InvalidCertificate(CertificateError::Expired))
        );

        let renewed_cert = exchange(&connector.clone(), &acceptor, &listener).await;
        assert_ne!(original_cert, renewed_cert);
        // A further aged reconnect must renew too, not just the first recovery.
        clock
            .0
            .fetch_add(7200, Ordering::SeqCst);
        let third_cert = exchange(&connector, &acceptor, &listener).await;
        assert_ne!(renewed_cert, third_cert);
    })
    .await
    .unwrap();
}

/// Complete both sides of TLS and exchange bytes before returning the peer wrapper.
async fn exchange(
    connector: &Tls13Connector,
    acceptor: &TlsAcceptor,
    listener: &TcpListener,
) -> Vec<u8> {
    let server = async {
        let (tcp, _) = listener
            .accept()
            .await
            .unwrap();
        let mut tls = acceptor
            .accept(tcp)
            .await
            .unwrap();
        assert_eq!(
            tls.get_ref()
                .1
                .alpn_protocol(),
            Some(b"h2".as_slice())
        );
        let cert = tls
            .get_ref()
            .1
            .peer_certificates()
            .unwrap()[0]
            .to_vec();
        assert_eq!(tls.read_u8().await.unwrap(), 7);
        tls.write_u8(9).await.unwrap();
        tls.shutdown().await.unwrap();
        cert
    };
    let client = async {
        let mut tls = connector
            .connect()
            .await
            .unwrap();
        tls.write_u8(7).await.unwrap();
        assert_eq!(tls.read_u8().await.unwrap(), 9);
        tls.shutdown().await.unwrap();
    };
    tokio::join!(server, client).0
}

/// Failure to obtain time must not create an unbounded or unverifiable wrapper.
#[test]
fn unavailable_clock_fails_closed() {
    #[derive(Debug)]
    struct NoTime;
    impl TimeProvider for NoTime {
        fn current_time(&self) -> Option<UnixTime> {
            None
        }
    }
    let (mut connector, _, _) = fixture(0);
    connector.clock = Arc::new(NoTime);
    assert!(
        connector
            .client_config()
            .unwrap_err()
            .to_string()
            .contains("clock is unavailable")
    );
}
