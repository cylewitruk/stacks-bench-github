//! Durable terminal-to-S3 promotion recovery with fresh persistence/storage adapters.

use std::assert_matches;
use std::time::Duration;

use chrono::Utc;
use sbgh_core::db::fleet::{
    ArtifactGrantRecord, FleetStore, FleetTerminalSubmission, FleetTerminalWrite,
    TerminalAcceptance, WorkerRegistration,
};
use sbgh_fleet::{
    ArtifactDescriptor, ReliableEventEnvelope, ReliableEventPayload, TaskPayload, TerminalOutcome,
    WorkerCapability, payload_digest,
};
use sbgh_postgres::{PostgresFleetStore, setup_pg_db};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

use crate::artifact_store::{ArtifactStore, S3Store, S3StoreConfig};
use crate::fleet::service::promote_artifacts;
use crate::fleet::test_support::seed_active_attempt;

#[path = "../../../../tests/support/s3.rs"]
mod s3;

/// Interruption points around the independently durable object and database writes.
#[derive(Clone, Copy)]
enum Interruption {
    BeforePromotion,
    AfterFirstObject,
    BeforeDatabaseAck,
}

#[tokio::test]
async fn artifact_recovery_after_terminal_acceptance_promotes_before_reporting() {
    recover_promotion(Interruption::BeforePromotion).await;
}

#[tokio::test]
async fn artifact_recovery_after_partial_batch_reuses_the_first_logical_object() {
    recover_promotion(Interruption::AfterFirstObject).await;
}

#[tokio::test]
async fn artifact_recovery_after_lost_database_ack_verifies_existing_logical_objects() {
    recover_promotion(Interruption::BeforeDatabaseAck).await;
}

/// Build an empty local mirror so restart checks cannot rely on cached object bytes.
fn store(directory: &TempDir, name: &str) -> S3Store {
    S3Store::new(directory.path().join(name), &s3::s3_config(), s3::http()).unwrap()
}

/// Upload exactly the checksum-bound bytes through the worker's signed PUT path.
async fn upload(store: &S3Store, descriptor: &ArtifactDescriptor, bytes: &[u8]) {
    let grant = store
        .fleet_put_grant(
            &descriptor.key,
            descriptor.size,
            &descriptor.sha256,
            Duration::from_secs(300),
        )
        .unwrap();
    let mut request = s3::http().put(&grant.url);
    for header in grant.headers {
        request = request.header(header.name, header.value);
    }
    let response = request
        .body(bytes.to_vec())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "signed fixture upload: {}", response.status());
    assert!(
        store
            .verify_fleet_upload(descriptor)
            .await
    );
}

/// Exercise the production promoter using accepted manifests reloaded after reconstruction.
async fn recover_promotion(interruption: Interruption) {
    s3::ensure_bucket().await;
    let (_db, pool) = setup_pg_db().await;
    let directory = TempDir::new().unwrap();
    let fleet = PostgresFleetStore::new(pool.clone());
    let original = store(&directory, "original");
    let worker = Uuid::new_v4();
    fleet
        .seed_worker(&WorkerRegistration {
            worker_id: worker,
            display_name: "artifact-recovery".into(),
            allowed_capabilities: vec![WorkerCapability::BuildOnly],
            measurement_profile: None,
            enabled: true,
            draining: false,
        })
        .await
        .unwrap();
    let offer = seed_active_attempt(&pool, &fleet, worker, TaskPayload::BuildOnly).await;
    let bytes: [&[u8]; 2] = [b"retained qualification artifact", b""];
    let mut manifest = Vec::new();
    for (index, bytes) in bytes.iter().enumerate() {
        let descriptor = ArtifactDescriptor {
            key: format!(
                "staging/{}/{}/{}/part-{index}",
                offer.identity.attempt_id,
                Uuid::new_v4(),
                offer.job_id
            ),
            logical_key: format!("{}/part-{index}", offer.job_id),
            size: bytes.len() as u64,
            sha256: hex::encode(Sha256::digest(bytes)),
        };
        assert!(
            fleet
                .record_artifact_grant(&ArtifactGrantRecord {
                    attempt_id: offer.identity.attempt_id,
                    object_key: descriptor.key.clone(),
                    logical_key: descriptor.logical_key.clone(),
                    size: Some(descriptor.size),
                    sha256: Some(descriptor.sha256.clone()),
                    expires_at: Utc::now() + chrono::Duration::minutes(5),
                })
                .await
                .unwrap()
        );
        upload(&original, &descriptor, bytes).await;
        assert!(
            fleet
                .verify_artifact(&offer.identity, &descriptor)
                .await
                .unwrap()
        );
        manifest.push(descriptor);
    }
    let outcome = TerminalOutcome::Cancelled {
        reason: "artifact recovery qualification".into(),
    };
    let payload = ReliableEventPayload::Terminal {
        outcome_digest: payload_digest(&outcome).unwrap(),
    };
    let event = ReliableEventEnvelope {
        identity: offer.identity.clone(),
        trace_id: offer.trace_id,
        reliable_seq: 1,
        payload_digest: payload_digest(&payload).unwrap(),
        payload,
        worker_timestamp_ms: Utc::now().timestamp_millis(),
    };
    fleet
        .ingest_reliable_event(worker, &event)
        .await
        .unwrap();
    let write = FleetTerminalWrite::Cancelled {
        remark: "artifact recovery qualification".into(),
    };
    let submission = FleetTerminalSubmission {
        reliable_seq: 1,
        payload_digest: &event.payload_digest,
        outcome: &outcome,
        artifacts: &manifest,
        write: &write,
    };
    assert_matches!(
        fleet
            .accept_terminal(worker, &offer.identity, &submission)
            .await
            .unwrap(),
        TerminalAcceptance::Accepted
    );
    assert!(
        fleet
            .attempt_terminal_outcome(offer.identity.attempt_id)
            .await
            .unwrap()
            .is_none()
    );

    match interruption {
        Interruption::BeforePromotion => {}
        Interruption::AfterFirstObject => {
            assert!(
                original
                    .delete_fleet_staging(&manifest[1].key)
                    .await
            );
            assert!(
                !promote_artifacts(&original, &fleet, offer.identity.attempt_id, &manifest).await
            );
            assert!(
                !original
                    .exists(&manifest[0].key)
                    .await
            );
            assert!(
                original
                    .exists(&manifest[0].logical_key)
                    .await
            );
            assert!(
                !original
                    .exists(&manifest[1].logical_key)
                    .await
            );
        }
        Interruption::BeforeDatabaseAck => {
            sqlx::raw_sql(r#"
                CREATE FUNCTION reject_promotion_ack() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.status = 'promoted' THEN RAISE EXCEPTION 'injected promotion ack loss'; END IF;
                    RETURN NEW;
                END $$;
                CREATE TRIGGER reject_promotion_ack BEFORE UPDATE ON worker_artifact_staging
                    FOR EACH ROW EXECUTE FUNCTION reject_promotion_ack();
            "#).execute(&pool).await.unwrap();
            assert!(
                !promote_artifacts(&original, &fleet, offer.identity.attempt_id, &manifest).await
            );
            for descriptor in &manifest {
                assert!(
                    !original
                        .exists(&descriptor.key)
                        .await
                );
                assert!(
                    original
                        .exists(&descriptor.logical_key)
                        .await
                );
            }
            sqlx::query("DROP TRIGGER reject_promotion_ack ON worker_artifact_staging")
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    assert!(
        fleet
            .attempt_terminal_outcome(offer.identity.attempt_id)
            .await
            .unwrap()
            .is_none()
    );
    drop(original);
    drop(fleet);

    let restarted = store(&directory, "restarted");
    let fleet = PostgresFleetStore::new(pool.clone());
    let pending = fleet
        .pending_artifact_promotions(10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].attempt_id, offer.identity.attempt_id);
    assert_eq!(pending[0].artifacts, manifest);
    if matches!(interruption, Interruption::AfterFirstObject) {
        upload(&restarted, &manifest[1], bytes[1]).await;
    }
    assert!(
        promote_artifacts(&restarted, &fleet, pending[0].attempt_id, &pending[0].artifacts).await
    );
    assert!(
        fleet
            .pending_artifact_promotions(10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fleet
            .attempt_terminal_outcome(offer.identity.attempt_id)
            .await
            .unwrap(),
        Some(outcome.clone())
    );
    assert_matches!(
        fleet
            .accept_terminal(worker, &offer.identity, &submission)
            .await
            .unwrap(),
        TerminalAcceptance::Duplicate
    );

    let reader = store(&directory, "fresh-reader");
    for (descriptor, bytes) in manifest.iter().zip(bytes) {
        assert!(
            !reader
                .exists(&descriptor.key)
                .await
        );
        let downloaded = reader
            .get(&descriptor.logical_key)
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(downloaded)
                .await
                .unwrap(),
            bytes
        );
    }
    let promoted: (i64, bool) = sqlx::query_as(
        "SELECT COUNT(*), bool_and(status = 'promoted') FROM worker_artifact_staging WHERE attempt_id = $1",
    ).bind(offer.identity.attempt_id).fetch_one(&pool).await.unwrap();
    assert_eq!(promoted, (2, true), "zero-byte objects also require promotion acknowledgement");
    let attempt_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM worker_attempt WHERE job_id = $1")
            .bind(offer.job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempt_count, 1);
}
