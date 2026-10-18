//! Pinned worker libraries talking to the production TLS/RPC/store composition.

use std::fs;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use sbgh_core::config::FleetConfig;
use sbgh_core::db::fleet::{WorkerPolicyPatch, WorkerRegistration, WorkerRegistryStore};
use sbgh_core::db::protocol::{
    ProtocolPolicyChange, ProtocolPolicyChangeOutcome, ProtocolPolicyStore,
};
use sbgh_core::db::scheduler::{SchedulerChange, SchedulerStore};
use sbgh_core::db::worker_lifecycle::{
    WorkerLifecycleAction, WorkerLifecycleChange, WorkerLifecycleOutcome, WorkerLifecycleStore,
};
use sbgh_fleet::WorkerCapability;
use sbgh_github::{AppCredentials, InstallationTokenCache};
use sbgh_postgres::{PostgresFleetStore, setup_pg_db};
use sqlx::Row;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{FleetRuntime, MtlsListener, serve};
use crate::artifact_store::LocalFsStore;
use crate::fleet::tls::validate_worker_identity_key;

/// One disposable instance of the production fleet listener and service.
struct RunningDaemon {
    /// Bound address retained for a real listener restart.
    address: SocketAddr,
    /// Graceful listener shutdown.
    shutdown: CancellationToken,
    /// Owned serving task, joined before a restart.
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl RunningDaemon {
    /// Rebuild all in-memory service state over the same durable store.
    async fn start(
        config: &FleetConfig,
        store: Arc<PostgresFleetStore>,
        tokens: InstallationTokenCache,
        root: &Path,
    ) -> anyhow::Result<Self> {
        let listener = MtlsListener::bind(
            config.listen,
            &config.server_certificate,
            &config.server_private_key,
        )
        .await?;
        let address = listener.local_addr();
        let runtime = FleetRuntime::build(
            config.clone(),
            store.clone(),
            store,
            Arc::new(LocalFsStore::new(root.join("artifacts"))),
            tokens,
            CancellationToken::new(),
        )
        .await?;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(serve(runtime, listener, shutdown.clone()));
        Ok(Self {
            address,
            shutdown,
            task: Some(task),
        })
    }

    /// Bound graceful shutdown and join the serving task even on failure.
    async fn stop(&mut self) -> anyhow::Result<()> {
        self.shutdown.cancel();
        if let Some(mut task) = self.task.take() {
            match time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(result) => result??,
                Err(error) => {
                    task.abort();
                    let _ = task.await;
                    return Err(error).context("daemon listener did not shut down gracefully");
                }
            }
        }
        Ok(())
    }
}

impl Drop for RunningDaemon {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Write one private fixture file without weakening production permission checks.
fn private_file(path: &Path, bytes: impl AsRef<[u8]>) -> anyhow::Result<()> {
    fs::write(path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// Poll a durable condition with a bounded deadline, checking child exit eagerly.
async fn session_ready(
    pool: &sbgh_postgres::Pool,
    worker: Uuid,
    child: &mut Child,
    after: Option<DateTime<Utc>>,
) -> anyhow::Result<Uuid> {
    time::timeout(Duration::from_secs(15), async {
        loop {
            ensure!(child.try_wait()?.is_none(), "worker exited before its session became ready");
            let session: Option<Uuid> = sqlx::query_scalar("SELECT worker_session_id FROM worker_session WHERE worker_id = $1 AND ended_at IS NULL AND expires_at > NOW() AND status IN ('registered','idle','draining') AND ($2::timestamptz IS NULL OR last_heartbeat_at > $2)")
                .bind(worker).bind(after).fetch_optional(pool).await?;
            if let Some(session) = session { return Ok(session); }
            time::sleep(Duration::from_millis(25)).await;
        }
    }).await.context("worker did not register or recover before deadline")?
}

/// Build disposable host-command fixtures; no real libvirt or LVM commands run.
fn config(root: &Path, endpoint: &str) -> anyhow::Result<std::path::PathBuf> {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.worker-benchmark.toml");
    let mut document: toml::Value = toml::from_str(&fs::read_to_string(source)?)?;
    document["orchestrator_url"] = endpoint.into();
    document["identity_private_key"] = root
        .join("worker.key")
        .to_string_lossy()
        .into_owned()
        .into();
    document
        .as_table_mut()
        .unwrap()
        .remove("binary_cache");
    fs::write(root.join("golden.qcow2"), b"fixture")?;
    document["sandbox"]["golden_image"] = root
        .join("golden.qcow2")
        .to_string_lossy()
        .into_owned()
        .into();
    for key in ["jobs_dir", "git_mirror", "results_tmpfs_root", "results_archive_dir"] {
        document["workspace"][key] = root
            .join(key)
            .to_string_lossy()
            .into_owned()
            .into();
    }
    let command = root.join("host-command");
    fs::write(
        &command,
        "#!/bin/sh\ncase \"$*\" in\n *\"lv_name,lv_attr\"*) printf 'mainnet-test|Vri---tz-k\\n' ;;\n *\"data_percent,metadata_percent\"*) printf '10.00|10.00\\n' ;;\n *) exit 0 ;;\nesac\n",
    )?;
    fs::set_permissions(&command, fs::Permissions::from_mode(0o700))?;
    for key in ["sudo", "virsh", "qemu_img", "cloud_localds", "git"] {
        document["commands"][key] = command
            .to_string_lossy()
            .into_owned()
            .into();
    }
    let path = root.join("worker.toml");
    fs::write(&path, toml::to_string(&document)?)?;
    Ok(path)
}

/// Run one source-pinned worker through registration, same-process recovery and legacy drain.
async fn qualify(binary: &Path, revision: u16) -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (_db, pool) = setup_pg_db().await;
    let root = tempfile::tempdir()?;
    let root = root.path();
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate()?)?;
    fs::write(root.join("ca.crt"), ca.pem())?;
    let mut server_params = CertificateParams::new(vec!["localhost".into()])?;
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let key = KeyPair::generate()?;
    fs::write(
        root.join("server.crt"),
        server_params
            .signed_by(&key, &ca)?
            .pem(),
    )?;
    private_file(&root.join("server.key"), key.serialize_pem())?;
    private_file(&root.join("hmac.key"), [0x55; 32])?;
    let public = sbgh_worker::identity::generate(&root.join("worker.key"))?;
    let identity = validate_worker_identity_key(public.as_bytes())?;
    let store = Arc::new(PostgresFleetStore::new(pool.clone()));
    let worker = Uuid::new_v4();
    store
        .seed_worker(&WorkerRegistration {
            worker_id: worker,
            display_name: "compatibility".into(),
            allowed_capabilities: vec![WorkerCapability::BuildOnly],
            measurement_profile: None,
            enabled: true,
            draining: false,
        })
        .await?;
    store
        .authorize_identity(worker, identity)
        .await?;
    let rsa = root.join("github.key");
    let generated = Command::new("openssl")
        .args(["genrsa", "-out"])
        .arg(&rsa)
        .arg("2048")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .status()
        .await?;
    ensure!(generated.success(), "generating disposable GitHub signing fixture");
    fs::set_permissions(&rsa, fs::Permissions::from_mode(0o600))?;
    let tokens = InstallationTokenCache::new(
        AppCredentials::from_pem_file("test", &rsa)?,
        "http://127.0.0.1:1",
    );
    let mut fleet = FleetConfig {
        listen: "127.0.0.1:0".parse()?,
        server_certificate: root.join("server.crt"),
        server_private_key: root.join("server.key"),
        lease_hmac_key: root.join("hmac.key"),
        heartbeat_seconds: 1,
        lease_seconds: 4,
        session_seconds: 8,
        long_poll_seconds: 1,
        ..FleetConfig::default()
    };
    let mut server = RunningDaemon::start(&fleet, store.clone(), tokens.clone(), root).await?;
    fleet.listen = server.address;
    let endpoint = format!("https://localhost:{}", server.address.port());
    let config = config(root, &endpoint)?;
    let log_path = root.join("worker.stderr");
    let mut child = Command::new(binary)
        .arg(&config)
        .env("SSL_CERT_FILE", root.join("ca.crt"))
        .stdout(Stdio::null())
        .stderr(fs::File::create(&log_path)?)
        .kill_on_drop(true)
        .spawn()?;
    let pid = child.id();
    let result: anyhow::Result<()> = async {
        let session = session_ready(&pool, worker, &mut child, None).await?;
        let row = sqlx::query("SELECT protocol_version, protocol_offer, protocol_contract, preflight_report FROM worker_session WHERE worker_session_id=$1")
            .bind(session).fetch_one(&pool).await?;
        ensure!(row.try_get::<i32,_>("protocol_version")? == i32::from(revision), "wrong worker advertisement");
        ensure!(row.try_get::<serde_json::Value,_>("protocol_contract")?["revision"] == revision, "wrong selected contract");
        ensure!(row.try_get::<Option<serde_json::Value>,_>("protocol_offer")?.is_some() == (revision == 2), "unexpected explicit offer");
        let preflight: Option<serde_json::Value> = row.try_get("preflight_report")?;
        ensure!(preflight.is_some() == (revision == 2), "wrong preflight reporting cohort");
        for expire_session in [false, true] {
            server.stop().await?;
            if expire_session {
                time::sleep(Duration::from_secs(fleet.session_seconds + 1)).await;
            }
            let expired: bool = sqlx::query_scalar("SELECT expires_at <= clock_timestamp() FROM worker_session WHERE worker_session_id=$1")
                .bind(session).fetch_one(&pool).await?;
            ensure!(expired == expire_session, "outage did not exercise the intended expiry boundary");
            let checkpoint: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(&pool).await?;
            server = RunningDaemon::start(&fleet, Arc::new(PostgresFleetStore::new(pool.clone())), tokens.clone(), root).await?;
            let recovered = session_ready(&pool, worker, &mut child, Some(checkpoint)).await?;
            ensure!(recovered == session && child.id() == pid, "restart recovery replaced process or session");
            let after: Option<serde_json::Value> = sqlx::query_scalar("SELECT preflight_report FROM worker_session WHERE worker_session_id=$1")
                .bind(session).fetch_one(&pool).await?;
            ensure!(after == preflight, "recovery changed successful startup evidence");
        }
        store.set_worker_draining(worker, true).await?;
        if revision == 2 {
            let checkpoint: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(&pool).await?;
            ensure!(session_ready(&pool, worker, &mut child, Some(checkpoint)).await? == session && child.id() == pid,
                "cordon replaced current worker process/session");
            store.set_worker_draining(worker, false).await?;
            let checkpoint: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()").fetch_one(&pool).await?;
            ensure!(session_ready(&pool, worker, &mut child, Some(checkpoint)).await? == session && child.id() == pid,
                "uncordon replaced current worker process/session");
            let outcome = store.change_worker_lifecycle(&WorkerLifecycleChange {
                request_id: Uuid::new_v4(), worker_id: worker, action: WorkerLifecycleAction::ShutdownAfterIdle,
                actor: "test".into(), reason: "explicit idle process shutdown".into(),
            }).await?;
            ensure!(matches!(outcome, WorkerLifecycleOutcome::Applied(_)), "shutdown request failed");
        }
        let status = time::timeout(Duration::from_secs(15), child.wait()).await??;
        ensure!(status.success(), "worker drain failed: {status}");
        let status: String = sqlx::query_scalar("SELECT status::text FROM worker_session WHERE worker_session_id=$1")
            .bind(session).fetch_one(&pool).await?;
        ensure!(status == "deregistered", "legacy drain did not deregister: {status}");
        if revision == 1 {
            store.update_worker(worker, &WorkerPolicyPatch { enabled: Some(false), ..WorkerPolicyPatch::default() }).await?;
            store.change_scheduler(&SchedulerChange { request_id:Uuid::new_v4(), paused:true,
                actor:"test".into(), reason:"compatibility qualification".into() }).await?;
            let advanced = store.change_protocol_policy(&ProtocolPolicyChange {
                request_id:Uuid::new_v4(), minimum_revision:2, actor:"test".into(),
                reason:"retire previous worker".into(), rollback_evidence:"isolated runtime fixture".into(),
            }).await?;
            ensure!(matches!(advanced, ProtocolPolicyChangeOutcome::Applied(_)), "floor advancement failed");
            store.update_worker(worker, &WorkerPolicyPatch { enabled: Some(true), ..WorkerPolicyPatch::default() }).await?;
            child = Command::new(binary).arg(&config).env("SSL_CERT_FILE", root.join("ca.crt"))
                .stdout(Stdio::null()).stderr(fs::File::create(&log_path)?).kill_on_drop(true).spawn()?;
            let rejected = time::timeout(Duration::from_secs(15), child.wait()).await??;
            ensure!(!rejected.success(), "retired worker was accepted");
            ensure!(fs::read_to_string(&log_path)?.contains("protocol_incompatible"), "rejection lost stable compatibility code");
        }
        let sessions: i64 = sqlx::query_scalar("SELECT count(*) FROM worker_session WHERE worker_id=$1")
            .bind(worker).fetch_one(&pool).await?;
        ensure!(sessions == 1, "recovery or rejection minted another session");
        Ok(())
    }.await;
    if child.try_wait()?.is_none() {
        child.kill().await?;
    }
    server.stop().await?;
    result.with_context(|| {
        format!(
            "revision {revision} worker stderr: {}",
            fs::read_to_string(log_path).unwrap_or_default()
        )
    })
}

#[tokio::test]
#[ignore = "run just qualify-protocol-runtime to build both source-pinned worker probes"]
async fn pinned_worker_runtime_registers_recovers_and_preserves_drain_semantics() {
    for (variable, revision) in
        [("SBGH_PREVIOUS_WORKER_PROBE", 1), ("SBGH_CURRENT_WORKER_PROBE", 2)]
    {
        let binary = std::env::var_os(variable)
            .expect("worker probe path is required; use just qualify-protocol-runtime");
        qualify(Path::new(&binary), revision)
            .await
            .unwrap();
    }
}
