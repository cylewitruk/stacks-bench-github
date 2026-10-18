//! Full daemon processes receiving real Unix signals over disposable local infrastructure.

use std::fs;
use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use rcgen::{CertificateParams, KeyPair};
use sbgh_core::db::fleet::{
    ArtifactGrantRecord, FleetStore, FleetTerminalSubmission, FleetTerminalWrite,
    TerminalAcceptance, WorkerRegistration,
};
use sbgh_core::db::scheduler::{SchedulerChange, SchedulerStore};
use sbgh_daemon::{ArtifactStore, S3Store, S3StoreConfig};
use sbgh_fleet::{
    ArtifactDescriptor, ReliableEventEnvelope, ReliableEventPayload, TaskPayload, TerminalOutcome,
    WorkerCapability, payload_digest,
};
use sbgh_postgres::db::Pool;
use sbgh_postgres::{PostgresFleetStore, setup_pg_db};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time;
use uuid::Uuid;

#[path = "support/fleet.rs"]
mod fleet;
#[path = "support/s3.rs"]
mod s3;

/// Isolated files and listener address reused by successive daemon processes.
struct Fixture {
    /// Own all disposable keys, configuration and diagnostic output.
    root: TempDir,
    /// Loopback-only operator API; fleet also binds loopback on an ephemeral port.
    address: SocketAddr,
}

impl Fixture {
    /// Prepare a real daemon configuration without inheriting deployment secrets.
    async fn new(pool: &Pool, heartbeat: u64) -> anyhow::Result<Self> {
        s3::ensure_bucket().await;
        let root = TempDir::new()?;
        let key = root.path().join("github.key");
        let generated = time::timeout(
            Duration::from_secs(15),
            Command::new("openssl")
                .args(["genrsa", "-out"])
                .arg(&key)
                .arg("2048")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .status(),
        )
        .await??;
        ensure!(generated.success(), "generating disposable GitHub key");
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600))?;
        let tls_key = KeyPair::generate()?;
        let cert = CertificateParams::new(vec!["localhost".into()])?.self_signed(&tls_key)?;
        fs::write(root.path().join("server.crt"), cert.pem())?;
        private_file(&root.path().join("server.key"), tls_key.serialize_pem())?;
        private_file(&root.path().join("hmac.key"), [0x33; 32])?;

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let database: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(pool)
            .await?;
        let options = pool.connect_options();
        let dsn = format!(
            "postgres://postgres:postgres@{}:{}/{database}",
            options.get_host(),
            options.get_port()
        );
        let mut config: toml::Value =
            toml::from_str(include_str!("../../../config.example.daemon.toml"))?;
        config["server"]["database_url"] = dsn.into();
        config["github"]["api_base_url"] = "http://127.0.0.1:1".into();
        for (section, name, file) in [
            ("github", "private_key_path", "github.key"),
            ("fleet", "server_certificate", "server.crt"),
            ("fleet", "server_private_key", "server.key"),
            ("fleet", "lease_hmac_key", "hmac.key"),
            ("api", "cookie_path", "cookie"),
            ("paths", "results_archive_dir", "results"),
        ] {
            config[section][name] = root
                .path()
                .join(file)
                .to_string_lossy()
                .into_owned()
                .into();
        }
        config["api"]["listen"] = vec![address.to_string()].into();
        config["fleet"]["listen"] = "127.0.0.1:0".into();
        config["fleet"]["heartbeat_seconds"] = (heartbeat as i64).into();
        let s3 = s3::s3_config();
        config["artifacts"]["endpoint"] = s3.endpoint.into();
        config["artifacts"]["bucket"] = s3.bucket.into();
        config["artifacts"]["region"] = s3.region.into();
        fs::write(
            root.path()
                .join("daemon.toml"),
            toml::to_string(&config)?,
        )?;
        drop(listener);
        Ok(Self { root, address })
    }

    /// Read the daemon-generated admin cookie for this boot only.
    fn cookie(&self) -> anyhow::Result<String> {
        Ok(fs::read_to_string(
            self.root
                .path()
                .join("cookie"),
        )?
        .trim()
        .to_owned())
    }

    /// Launch the shipped binary, retaining logs and killing/reaping failed startups.
    async fn start(&self) -> anyhow::Result<DaemonProcess> {
        let log_path = self
            .root
            .path()
            .join(format!("daemon-{}.log", Uuid::new_v4()));
        let log = fs::File::create(&log_path)?;
        let s3 = s3::s3_config();
        let child = Command::new(env!("CARGO_BIN_EXE_sbgh-daemon"))
            .current_dir(self.root.path())
            .env_clear()
            .env(
                "SBGH_CONFIG",
                self.root
                    .path()
                    .join("daemon.toml"),
            )
            .env("SBGH_ARTIFACTS_S3_ACCESS_KEY_ID", s3.access_key_id)
            .env("SBGH_ARTIFACTS_S3_SECRET_ACCESS_KEY", s3.secret_access_key)
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true)
            .spawn()?;
        let mut process = DaemonProcess { child, log_path };
        let ready = time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(status) = process.child.try_wait()? {
                    bail!("daemon exited {status}: {}", fs::read_to_string(&process.log_path)?);
                }
                if s3::http()
                    .get(format!("http://{}/api/health", self.address))
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    return Ok::<(), anyhow::Error>(());
                }
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("daemon readiness timeout")
        .and_then(|result| result);
        if let Err(error) = ready {
            process.cleanup().await;
            return Err(error);
        }
        Ok(process)
    }
}

/// Owned subprocess; every normal and error path explicitly reaps it.
struct DaemonProcess {
    /// Live binary under test, never an unrelated host service.
    child: Child,
    /// Captured process-local signal and startup observations.
    log_path: PathBuf,
}

impl DaemonProcess {
    /// Send a real Unix signal to this owned, unreaped child only.
    async fn signal(&mut self, signal: &str) -> anyhow::Result<()> {
        ensure!(
            self.child
                .try_wait()?
                .is_none(),
            "daemon exited before signal"
        );
        let pid = self
            .child
            .id()
            .context("missing daemon PID")?;
        let status = Command::new("kill")
            .arg(signal)
            .arg(pid.to_string())
            .kill_on_drop(true)
            .status()
            .await?;
        ensure!(status.success(), "sending {signal} to daemon child");
        Ok(())
    }

    /// Bound shutdown and retain the process log on any unsuccessful exit.
    async fn wait(&mut self, limit: Duration) -> anyhow::Result<()> {
        let status = time::timeout(limit, self.child.wait())
            .await
            .context("daemon exit timeout")??;
        ensure!(
            status.success(),
            "daemon exited {status}: {}",
            fs::read_to_string(&self.log_path)?
        );
        Ok(())
    }

    /// Kill and reap a failed test child before dropping its database and files.
    async fn cleanup(&mut self) {
        if self
            .child
            .try_wait()
            .ok()
            .flatten()
            .is_none()
        {
            let _ = self.child.kill().await;
        }
        let _ = self.child.wait().await;
    }
}

/// Write disposable key material with production-compatible file permissions.
fn private_file(path: &Path, bytes: impl AsRef<[u8]>) -> anyhow::Result<()> {
    fs::write(path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// Observe durable worker/attempt/admission facts without modifying them.
async fn snapshot(pool: &Pool, worker: Uuid, attempt: Uuid) -> anyhow::Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
           'worker', (SELECT to_jsonb(w) FROM worker_registry w WHERE worker_id = $1),
           'attempt', (SELECT to_jsonb(a) FROM worker_attempt a WHERE attempt_id = $2),
           'scheduler', (SELECT to_jsonb(s) FROM scheduler_policy s),
           'cleanup', (SELECT COUNT(*) FROM worker_cleanup_obligation))",
    )
    .bind(worker)
    .bind(attempt)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn daemon_sigterm_restart_preserves_active_attempt_and_scheduler_policy() -> anyhow::Result<()>
{
    qualify_signal("-TERM", false).await
}

#[tokio::test]
async fn daemon_sigint_restart_preserves_active_attempt_and_scheduler_policy() -> anyhow::Result<()>
{
    qualify_signal("-INT", false).await
}

#[tokio::test]
async fn daemon_second_sigint_forces_local_exit_without_fleet_mutation() -> anyhow::Result<()> {
    qualify_signal("-INT", true).await
}

/// Exercise signal handling in the production binary, then boot it again over the same DB.
async fn qualify_signal(signal: &str, force: bool) -> anyhow::Result<()> {
    let (_db, pool) = setup_pg_db().await;
    let fixture = Fixture::new(&pool, if force { 10 } else { 1 }).await?;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&WorkerRegistration {
            worker_id: worker,
            display_name: "daemon-process-qualification".into(),
            allowed_capabilities: vec![WorkerCapability::BuildOnly],
            measurement_profile: None,
            enabled: true,
            draining: false,
        })
        .await?;
    let offer = fleet::seed_active_attempt(&pool, &store, worker, TaskPayload::BuildOnly).await;
    store
        .change_scheduler(&SchedulerChange {
            request_id: Uuid::new_v4(),
            paused: true,
            actor: "test:operator".into(),
            reason: "process restart qualification".into(),
        })
        .await?;
    let before = snapshot(&pool, worker, offer.identity.attempt_id).await?;
    let mut process = fixture.start().await?;
    let result = async {
        let cookie = fixture.cookie()?;
        let mut held_request = None;
        if force {
            let mut connection = TcpStream::connect(fixture.address).await?;
            connection.write_all(format!(
                "POST /api/fleet/scheduler/pause HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {cookie}\r\nContent-Type: application/json\r\nContent-Length: 1024\r\nExpect: 100-continue\r\n\r\n",
            ).as_bytes()).await?;
            let mut response = [0_u8; 25];
            time::timeout(Duration::from_secs(5), connection.read_exact(&mut response)).await??;
            ensure!(&response == b"HTTP/1.1 100 Continue\r\n\r\n", "request must be waiting for its body");
            held_request = Some(connection);
        }
        process.signal(signal).await?;
        if force {
            time::timeout(Duration::from_secs(3), async {
                loop {
                    if fs::read_to_string(&process.log_path)?.contains("daemon shutdown signal received") {
                        return Ok::<(), anyhow::Error>(());
                    }
                    time::sleep(Duration::from_millis(10)).await;
                }
            }).await??;
            ensure!(process.child.try_wait()?.is_none(), "in-flight request must hold graceful shutdown");
            process.signal("-INT").await?;
        }
        process.wait(Duration::from_secs(5)).await?;
        let log = fs::read_to_string(&process.log_path)?;
        ensure!(log.contains("daemon shutdown signal received"), "signal must be observable");
        if force { ensure!(log.contains("forced=true"), "second interrupt must force local exit"); }
        drop(held_request);
        ensure!(snapshot(&pool, worker, offer.identity.attempt_id).await? == before, "signal changed durable fleet state");

        process = fixture.start().await?;
        let new_cookie = fixture.cookie()?;
        ensure!(new_cookie != cookie, "restart must rotate the operator cookie");
        let endpoint = format!("http://{}/api/fleet/scheduler", fixture.address);
        ensure!(s3::http().get(&endpoint).bearer_auth(&cookie).send().await?.status().as_u16() == 401, "old boot cookie still authorized");
        ensure!(s3::http().get(&endpoint).bearer_auth(&new_cookie).send().await?.status().is_success(), "new boot cookie not authorized");
        ensure!(snapshot(&pool, worker, offer.identity.attempt_id).await? == before, "restart changed durable fleet state");
        process.signal("-TERM").await?;
        process.wait(Duration::from_secs(5)).await?;
        Ok::<(), anyhow::Error>(())
    }.await;
    process.cleanup().await;
    result
}

#[tokio::test]
async fn daemon_restart_recovers_copied_artifact_without_promotion_ack() -> anyhow::Result<()> {
    let (_db, pool) = setup_pg_db().await;
    let fixture = Fixture::new(&pool, 1).await?;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&WorkerRegistration {
            worker_id: worker,
            display_name: "daemon-artifact-qualification".into(),
            allowed_capabilities: vec![WorkerCapability::BuildOnly],
            measurement_profile: None,
            enabled: true,
            draining: false,
        })
        .await?;
    let offer = fleet::seed_active_attempt(&pool, &store, worker, TaskPayload::BuildOnly).await;
    let artifacts = S3Store::new(
        fixture
            .root
            .path()
            .join("verifier"),
        &s3::s3_config(),
        s3::http(),
    )?;
    let bytes = b"daemon promotion restart qualification";
    let descriptor = ArtifactDescriptor {
        key: format!(
            "staging/{}/{}/{}/console.log",
            offer.identity.attempt_id,
            Uuid::new_v4(),
            offer.job_id
        ),
        logical_key: format!("{}/console.log", offer.job_id),
        size: bytes.len() as u64,
        sha256: hex::encode(Sha256::digest(bytes)),
    };
    ensure!(
        store
            .record_artifact_grant(&ArtifactGrantRecord {
                attempt_id: offer.identity.attempt_id,
                object_key: descriptor.key.clone(),
                logical_key: descriptor.logical_key.clone(),
                size: Some(descriptor.size),
                sha256: Some(descriptor.sha256.clone()),
                expires_at: chrono::Utc::now() + chrono::Duration::minutes(5),
            })
            .await?,
        "grant not persisted"
    );
    let grant = artifacts.fleet_put_grant(
        &descriptor.key,
        descriptor.size,
        &descriptor.sha256,
        Duration::from_secs(300),
    )?;
    let mut upload = s3::http().put(grant.url);
    for header in grant.headers {
        upload = upload.header(header.name, header.value);
    }
    ensure!(
        upload
            .body(bytes.to_vec())
            .send()
            .await?
            .status()
            .is_success(),
        "fixture upload failed"
    );
    ensure!(
        artifacts
            .verify_fleet_upload(&descriptor)
            .await,
        "staged upload failed verification"
    );
    ensure!(
        store
            .verify_artifact(&offer.identity, &descriptor)
            .await?,
        "verified manifest not persisted"
    );

    let outcome = TerminalOutcome::Cancelled {
        reason: "daemon artifact qualification".into(),
    };
    let payload = ReliableEventPayload::Terminal {
        outcome_digest: payload_digest(&outcome)?,
    };
    let event = ReliableEventEnvelope {
        identity: offer.identity.clone(),
        trace_id: offer.trace_id,
        reliable_seq: 1,
        payload_digest: payload_digest(&payload)?,
        payload,
        worker_timestamp_ms: chrono::Utc::now().timestamp_millis(),
    };
    store
        .ingest_reliable_event(worker, &event)
        .await?;
    let write = FleetTerminalWrite::Cancelled {
        remark: "daemon artifact qualification".into(),
    };
    ensure!(
        store
            .accept_terminal(
                worker,
                &offer.identity,
                &FleetTerminalSubmission {
                    reliable_seq: 1,
                    payload_digest: &event.payload_digest,
                    outcome: &outcome,
                    artifacts: std::slice::from_ref(&descriptor),
                    write: &write,
                }
            )
            .await?
            == TerminalAcceptance::Accepted,
        "terminal not accepted"
    );
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION reject_promotion_ack() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.status = 'promoted' THEN RAISE EXCEPTION 'injected promotion ack loss'; END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER reject_promotion_ack BEFORE UPDATE ON worker_artifact_staging
            FOR EACH ROW EXECUTE FUNCTION reject_promotion_ack();
    "#,
    )
    .execute(&pool)
    .await?;

    let mut process = fixture.start().await?;
    let result = async {
        eventually(|| async {
            let copied = artifacts.exists(&descriptor.logical_key).await
                && !artifacts.exists(&descriptor.key).await;
            let ack_failed = fs::read_to_string(&process.log_path)?
                .contains("injected promotion ack loss");
            Ok(copied && ack_failed)
        }).await.context("first daemon did not copy the artifact and fail its promotion acknowledgement")?;
        ensure!(store.attempt_terminal_outcome(offer.identity.attempt_id).await?.is_none(), "report escaped before promotion acknowledgement");
        let projected: i64 = sqlx::query_scalar("SELECT projected_reliable_seq FROM worker_attempt WHERE attempt_id = $1")
            .bind(offer.identity.attempt_id).fetch_one(&pool).await?;
        ensure!(projected == 0, "terminal projected before artifact durability was acknowledged");
        process.signal("-TERM").await?;
        process.wait(Duration::from_secs(5)).await?;
        sqlx::query("DROP TRIGGER reject_promotion_ack ON worker_artifact_staging").execute(&pool).await?;

        process = fixture.start().await?;
        eventually(|| async {
            let projected: bool = sqlx::query_scalar(
                "SELECT projected_reliable_seq = 1 AND status = 'cancelled' FROM worker_attempt WHERE attempt_id = $1",
            ).bind(offer.identity.attempt_id).fetch_one(&pool).await?;
            Ok(projected && store.pending_artifact_promotions(10).await?.is_empty())
        }).await.context("restarted daemon did not finish promotion and terminal projection")?;
        ensure!(store.attempt_terminal_outcome(offer.identity.attempt_id).await? == Some(outcome.clone()), "terminal changed across restart");
        let reader = S3Store::new(fixture.root.path().join("fresh-reader"), &s3::s3_config(), s3::http())?;
        let downloaded = reader.get(&descriptor.logical_key).await.context("missing promoted object")?;
        ensure!(tokio::fs::read(downloaded).await? == bytes, "promoted bytes changed");
        let attempts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_attempt WHERE job_id = $1")
            .bind(offer.job_id).fetch_one(&pool).await?;
        ensure!(attempts == 1, "restart created another execution");
        process.signal("-TERM").await?;
        process.wait(Duration::from_secs(5)).await?;
        Ok::<(), anyhow::Error>(())
    }.await;
    process.cleanup().await;
    result
}

/// Wait for a durable observable condition without advancing either process's clock.
async fn eventually<F, T>(mut check: F) -> anyhow::Result<()>
where
    F: FnMut() -> T,
    T: Future<Output = anyhow::Result<bool>>,
{
    time::timeout(Duration::from_secs(15), async {
        loop {
            if check().await? {
                return Ok::<(), anyhow::Error>(());
            }
            time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("condition did not converge")?
}
