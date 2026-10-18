//! Registration negotiation and semantic admission against production persistence.

use sbgh_core::db::protocol::{
    ProtocolPolicyChange, ProtocolPolicyChangeOutcome, ProtocolPolicyStore,
};
use sbgh_core::db::scheduler::{SchedulerChange, SchedulerChangeOutcome, SchedulerStore};
use sbgh_fleet::{NEGOTIATED_PROTOCOL_REVISION, ProtocolFeature, ProtocolOffer};

use super::*;

/// A retry-safe floor change with an explicit qualification reference.
fn floor_change(minimum_revision: u16) -> ProtocolPolicyChange {
    ProtocolPolicyChange {
        request_id: Uuid::new_v4(),
        minimum_revision,
        actor: "api:admin".into(),
        reason: "qualified rolling upgrade".into(),
        rollback_evidence: "test qualification record".into(),
    }
}

/// Pause or resume admission without changing worker policy.
async fn pause(store: &PostgresFleetStore, paused: bool) {
    std::assert_matches!(
        store
            .change_scheduler(&SchedulerChange {
                request_id: Uuid::new_v4(),
                paused,
                actor: "api:admin".into(),
                reason: "floor maintenance".into(),
            })
            .await
            .unwrap(),
        SchedulerChangeOutcome::Applied(_)
    );
}

#[tokio::test]
async fn protocol_floor_requires_convergence_and_preserves_audited_rollback_receipts() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let change = floor_change(2);
    assert_eq!(
        store
            .change_protocol_policy(&change)
            .await
            .unwrap(),
        ProtocolPolicyChangeOutcome::MaintenanceRequired
    );
    pause(&store, true).await;
    assert_eq!(
        store
            .change_protocol_policy(&change)
            .await
            .unwrap(),
        ProtocolPolicyChangeOutcome::WorkersNotConverged(vec![worker])
    );
    let legacy = session(worker, Uuid::new_v4());
    store
        .register_session(worker, &legacy, Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(
        store
            .change_protocol_policy(&change)
            .await
            .unwrap(),
        ProtocolPolicyChangeOutcome::WorkersNotConverged(vec![worker])
    );
    let request = current(worker, Uuid::new_v4());
    store
        .register_session(worker, &request, Duration::seconds(90))
        .await
        .unwrap();
    sqlx::query("UPDATE worker_session SET expires_at = NOW() - INTERVAL '1 second' WHERE worker_session_id = $1")
        .bind(request.worker_session_id).execute(&pool).await.unwrap();
    assert_eq!(
        store
            .change_protocol_policy(&change)
            .await
            .unwrap(),
        ProtocolPolicyChangeOutcome::WorkersNotConverged(vec![worker])
    );
    store
        .register_session(worker, &request, Duration::seconds(90))
        .await
        .unwrap();
    let receipt = store
        .change_protocol_policy(&change)
        .await
        .unwrap();
    let ProtocolPolicyChangeOutcome::Applied(policy) = &receipt else {
        panic!("floor not advanced: {receipt:?}");
    };
    assert_eq!(policy.minimum_revision, 2);
    assert_eq!(policy.generation, 1);
    // Neither a new legacy session nor a replay of its old UUID may supersede the current worker.
    for rejected in [legacy, session(worker, Uuid::new_v4())] {
        std::assert_matches!(
            store
                .register_session(worker, &rejected, Duration::seconds(90))
                .await
                .unwrap_err(),
            CoreError::SessionRegistration(SessionRegistrationError::ProtocolIncompatible)
        );
    }
    assert!(
        store
            .session_is_active(worker, request.worker_session_id)
            .await
            .unwrap()
    );
    pause(&store, false).await;
    let reopened = PostgresFleetStore::new(pool.clone());
    assert_eq!(
        reopened
            .protocol_policy()
            .await
            .unwrap(),
        *policy
    );
    assert_eq!(
        reopened
            .change_protocol_policy(&change)
            .await
            .unwrap(),
        receipt
    );
    let mut conflicting = change.clone();
    conflicting
        .rollback_evidence
        .push_str(" changed");
    assert_eq!(
        store
            .change_protocol_policy(&conflicting)
            .await
            .unwrap(),
        ProtocolPolicyChangeOutcome::Conflict
    );
    let rollback = floor_change(1);
    assert_eq!(
        store
            .change_protocol_policy(&rollback)
            .await
            .unwrap(),
        ProtocolPolicyChangeOutcome::MaintenanceRequired
    );
    pause(&store, true).await;
    let ProtocolPolicyChangeOutcome::Applied(rolled_back) = store
        .change_protocol_policy(&rollback)
        .await
        .unwrap()
    else {
        panic!("rollback rejected");
    };
    assert_eq!(rolled_back.generation, 2);
    assert_eq!(rolled_back.minimum_revision, 1);
    assert_eq!(
        store
            .change_protocol_policy(&change)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .protocol_policy()
            .await
            .unwrap(),
        rolled_back
    );
    store
        .register_session(worker, &session(worker, Uuid::new_v4()), Duration::seconds(90))
        .await
        .unwrap();
    for sql in [
        "UPDATE fleet_protocol_administration_audit SET actor = 'tamper'",
        "DELETE FROM fleet_protocol_administration_audit",
        "TRUNCATE fleet_protocol_administration_audit",
    ] {
        assert!(
            sqlx::query(sql)
                .execute(&pool)
                .await
                .is_err()
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM fleet_protocol_administration_audit")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn protocol_floor_cannot_advance_over_running_work() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool);
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let request = current(worker, Uuid::new_v4());
    store
        .register_session(worker, &request, Duration::seconds(90))
        .await
        .unwrap();
    enqueue_build(&store, Uuid::new_v4()).await;
    let assignment = store
        .poll_offer(worker, request.worker_session_id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .accept_offer(worker, &assignment.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    );
    pause(&store, true).await;
    assert_eq!(
        store
            .change_protocol_policy(&floor_change(2))
            .await
            .unwrap(),
        ProtocolPolicyChangeOutcome::MaintenanceRequired
    );
    assert_eq!(
        store
            .protocol_policy()
            .await
            .unwrap()
            .minimum_revision,
        1
    );
}

/// Convergence observes registrations that already hold the shared policy lock.
#[tokio::test]
async fn protocol_floor_waits_for_registration_before_checking_convergence() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    store
        .register_session(worker, &current(worker, Uuid::new_v4()), Duration::seconds(90))
        .await
        .unwrap();
    pause(&store, true).await;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT worker_id FROM worker_registry WHERE worker_id = $1 FOR UPDATE")
        .bind(worker)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let registration_store = store.clone();
    let register = tokio::spawn(async move {
        registration_store
            .register_session(worker, &session(worker, Uuid::new_v4()), Duration::seconds(90))
            .await
            .unwrap()
    });
    wait_for_lock(
        &pool,
        "%FROM worker_registry registry%identity.identity_key_sha256%FOR UPDATE%",
        1,
    )
    .await;
    let policy_store = store.clone();
    let advance = tokio::spawn(async move {
        policy_store
            .change_protocol_policy(&floor_change(2))
            .await
            .unwrap()
    });
    wait_for_lock(&pool, "SELECT * FROM fleet_protocol_policy WHERE singleton FOR UPDATE", 1).await;
    tx.commit().await.unwrap();
    assert_eq!(
        register
            .await
            .unwrap()
            .protocol_contract
            .revision,
        1
    );
    assert_eq!(
        advance.await.unwrap(),
        ProtocolPolicyChangeOutcome::WorkersNotConverged(vec![worker])
    );
    assert_eq!(
        store
            .protocol_policy()
            .await
            .unwrap()
            .minimum_revision,
        1
    );
}

/// A worker implementing revision 2 plus the deployed predecessor contract.
fn current(worker: Uuid, session_id: Uuid) -> RegisterSessionRequest {
    RegisterSessionRequest {
        protocol_version: NEGOTIATED_PROTOCOL_REVISION,
        protocol_offer: Some(ProtocolOffer::current()),
        ..session(worker, session_id)
    }
}

#[tokio::test]
async fn negotiation_contract_survives_recovery_and_rejects_changed_advertisements() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let request = current(worker, Uuid::new_v4());
    let registered = store
        .register_session(worker, &request, Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(
        registered
            .protocol_contract
            .revision,
        2
    );
    assert!(
        registered
            .protocol_contract
            .supports_payload(&TaskPayload::BuildOnly)
    );
    sqlx::query("UPDATE worker_session SET status = 'offline', ended_at = NOW(), expires_at = NOW() - INTERVAL '1 second' WHERE worker_session_id = $1")
        .bind(request.worker_session_id).execute(&pool).await.unwrap();
    let reopened = PostgresFleetStore::new(pool.clone());
    let recovered = reopened
        .register_session(worker, &request, Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(recovered.protocol_contract, registered.protocol_contract);
    assert_eq!(recovered.worker_session_id, registered.worker_session_id);
    let visible = reopened
        .workers(Some(worker))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(visible.protocol_offer, request.protocol_offer);
    assert_eq!(visible.protocol_contract, Some(registered.protocol_contract));
    let mut changed = request.clone();
    changed
        .protocol_offer
        .as_mut()
        .unwrap()
        .features
        .insert("unknown_future_feature".into());
    std::assert_matches!(
        reopened
            .register_session(worker, &changed, Duration::seconds(90))
            .await
            .unwrap_err(),
        CoreError::SessionRegistration(SessionRegistrationError::FactsChanged)
    );
    let persisted: serde_json::Value = sqlx::query_scalar(
        "SELECT protocol_offer FROM worker_session WHERE worker_session_id = $1",
    )
    .bind(request.worker_session_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        persisted,
        serde_json::to_value(
            request
                .protocol_offer
                .unwrap()
        )
        .unwrap()
    );
}

#[tokio::test]
async fn negotiation_matches_task_semantics_independently_of_authorized_capability() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let legacy_worker = Uuid::new_v4();
    let legacy = session(legacy_worker, Uuid::new_v4());
    store
        .seed_worker(&registration(legacy_worker))
        .await
        .unwrap();
    let unregistered = store
        .workers(Some(legacy_worker))
        .await
        .unwrap()
        .remove(0);
    assert!(
        unregistered
            .protocol_offer
            .is_none()
    );
    assert!(
        unregistered
            .protocol_contract
            .is_none()
    );
    let previous = store
        .register_session(legacy_worker, &legacy, Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(
        previous
            .protocol_contract
            .revision,
        1
    );
    let visible = store
        .workers(Some(legacy_worker))
        .await
        .unwrap()
        .remove(0);
    assert!(
        visible
            .protocol_offer
            .is_none()
    );
    assert_eq!(visible.protocol_contract, Some(previous.protocol_contract));
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let mut request = current(worker, Uuid::new_v4());
    request
        .protocol_offer
        .as_mut()
        .unwrap()
        .features = ["benchmark_v1".into()].into();
    let registered = store
        .register_session(worker, &request, Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(registered.effective_capabilities, vec![WorkerCapability::BuildOnly]);
    assert_eq!(
        registered
            .protocol_contract
            .features,
        [ProtocolFeature::BenchmarkV1].into()
    );
    let job = Uuid::new_v4();
    enqueue_build(&store, job).await;
    assert!(
        store
            .poll_offer(
                worker,
                request.worker_session_id,
                Duration::seconds(30),
                Duration::seconds(45)
            )
            .await
            .unwrap()
            .is_none()
    );
    let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM worker_attempt WHERE job_id = $1")
        .bind(job)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(attempts, 0);
    let offered = store
        .poll_offer(
            legacy_worker,
            legacy.worker_session_id,
            Duration::seconds(30),
            Duration::seconds(45),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(offered.offer.job_id, job);
    assert!(
        store
            .accept_offer(legacy_worker, &offered.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    );
    let replacement = current(worker, Uuid::new_v4());
    store
        .register_session(worker, &replacement, Duration::seconds(90))
        .await
        .unwrap();
    let next = Uuid::new_v4();
    enqueue_build(&store, next).await;
    let offered = store
        .poll_offer(
            worker,
            replacement.worker_session_id,
            Duration::seconds(30),
            Duration::seconds(45),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(offered.offer.job_id, next);
    assert!(
        store
            .accept_offer(worker, &offered.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn negotiation_rejection_cannot_replace_an_existing_session() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let legacy = session(worker, Uuid::new_v4());
    store
        .register_session(worker, &legacy, Duration::seconds(90))
        .await
        .unwrap();
    let mut incompatible = current(worker, Uuid::new_v4());
    incompatible.protocol_version = 3;
    let offer = incompatible
        .protocol_offer
        .as_mut()
        .unwrap();
    offer.minimum_revision = 3;
    offer.maximum_revision = 3;
    std::assert_matches!(
        store
            .register_session(worker, &incompatible, Duration::seconds(90))
            .await
            .unwrap_err(),
        CoreError::SessionRegistration(SessionRegistrationError::ProtocolIncompatible)
    );
    assert!(
        store
            .session_is_active(worker, legacy.worker_session_id)
            .await
            .unwrap()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM worker_session WHERE worker_id = $1")
        .bind(worker)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}
