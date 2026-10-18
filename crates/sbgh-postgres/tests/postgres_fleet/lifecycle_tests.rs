//! Lifecycle policy, admission fencing and cleanup against production stores.

use sbgh_core::db::worker_lifecycle::{
    WorkerAdmissionBlocker, WorkerLifecycleAction, WorkerLifecycleChange, WorkerLifecycleOutcome,
    WorkerLifecycleStore,
};
use sbgh_fleet::{DesiredState, NEGOTIATED_PROTOCOL_REVISION, PreflightReport, ProtocolOffer};

use super::*;

/// A successor cannot bypass its predecessor's teardown by requesting another job.
#[tokio::test]
async fn lifecycle_pending_cleanup_blocks_new_work_across_sessions() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let old = resident(worker);
    store
        .register_session(worker, &old, Duration::minutes(5))
        .await
        .unwrap();
    let job = Uuid::new_v4();
    enqueue_build(&store, job).await;
    running_attempt(&store, worker, old.worker_session_id, job).await;
    let successor = resident(worker);
    store
        .register_session(worker, &successor, Duration::minutes(5))
        .await
        .unwrap();
    let queued = Uuid::new_v4();
    enqueue_build(&store, queued).await;
    let snapshot = store
        .worker_maintenance_snapshot(worker)
        .await
        .unwrap()
        .unwrap();
    assert!(
        snapshot
            .admission_blockers()
            .contains(&WorkerAdmissionBlocker::PendingCleanup)
    );
    assert!(
        store
            .poll_offer(
                worker,
                successor.worker_session_id,
                Duration::seconds(30),
                Duration::seconds(45)
            )
            .await
            .unwrap()
            .is_none(),
        "another queued job must not bypass predecessor teardown"
    );
    let obligations = store
        .cleanup_obligations(worker, successor.worker_session_id)
        .await
        .unwrap();
    assert_eq!(obligations.len(), 1);
    assert!(
        store
            .complete_cleanup(worker, successor.worker_session_id, obligations[0].id)
            .await
            .unwrap()
    );
    assert!(
        store
            .poll_offer(
                worker,
                successor.worker_session_id,
                Duration::seconds(30),
                Duration::seconds(45)
            )
            .await
            .unwrap()
            .is_some()
    );
}

/// Newly committed teardown debt gates both re-delivery and acceptance of an offer.
#[tokio::test]
async fn lifecycle_pending_cleanup_blocks_retained_offer_and_acceptance() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let facts = resident(worker);
    store
        .register_session(worker, &facts, Duration::minutes(5))
        .await
        .unwrap();
    let job = Uuid::new_v4();
    enqueue_build(&store, job).await;
    let offer = store
        .poll_offer(worker, facts.worker_session_id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    let identity = offer.offer.identity;

    // Seed independent teardown debt after offer delivery. Admission must enforce
    // this invariant even when a client retains an earlier offer.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT worker_id FROM worker_registry WHERE worker_id=$1 FOR UPDATE")
        .bind(worker)
        .execute(&mut *tx)
        .await
        .unwrap();
    let obligation: i64 = sqlx::query_scalar(
        "INSERT INTO worker_cleanup_obligation
        (worker_id,worker_session_id,attempt_id,job_id,requeue_job,reason)
        VALUES ($1,$2,$3,$4,false,'admission regression') RETURNING id",
    )
    .bind(worker)
    .bind(facts.worker_session_id)
    .bind(identity.attempt_id)
    .bind(job)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let poll_store = store.clone();
    let poll = tokio::spawn(async move {
        poll_store
            .poll_offer(
                worker,
                facts.worker_session_id,
                Duration::seconds(30),
                Duration::seconds(45),
            )
            .await
            .unwrap()
    });
    let accept_store = store.clone();
    let accept_identity = identity.clone();
    let accept = tokio::spawn(async move {
        accept_store
            .accept_offer(worker, &accept_identity, Duration::seconds(45))
            .await
            .unwrap()
    });
    wait_for_lock(&pool, "%FROM worker_registry WHERE worker_id=$1 FOR UPDATE", 2).await;
    tx.commit().await.unwrap();
    assert!(poll.await.unwrap().is_none());
    assert!(!accept.await.unwrap());
    assert!(
        store
            .complete_cleanup(worker, facts.worker_session_id, obligation)
            .await
            .unwrap()
    );
    let retried = store
        .poll_offer(worker, facts.worker_session_id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retried
            .offer
            .identity
            .attempt_id,
        identity.attempt_id
    );
    assert!(
        store
            .accept_offer(worker, &identity, Duration::seconds(45))
            .await
            .unwrap()
    );
    assert!(
        store
            .accept_offer(worker, &identity, Duration::seconds(45))
            .await
            .unwrap(),
        "acceptance retries stay idempotent"
    );
}

/// Recovery must retain the original successful check, never silently rewrite it.
#[tokio::test]
async fn lifecycle_preflight_provenance_is_immutable_and_legacy_is_unreported() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool);
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let mut facts = resident(worker);
    let report = PreflightReport {
        revision: 1,
        completed_at_ms: 1_792_213_533_000,
        executable_sha256: "ab".repeat(32),
        configuration_sha256: "cd".repeat(32),
    };
    facts.preflight = Some(report.clone());
    for _ in 0..2 {
        store
            .register_session(worker, &facts, Duration::seconds(90))
            .await
            .unwrap();
        assert_eq!(
            store
                .worker_maintenance_snapshot(worker)
                .await
                .unwrap()
                .unwrap()
                .preflight,
            Some(report.clone())
        );
    }
    for changed in [
        None,
        Some(PreflightReport { revision: 2, ..report.clone() }),
        Some(PreflightReport {
            configuration_sha256: "ef".repeat(32),
            ..report.clone()
        }),
        Some(PreflightReport {
            executable_sha256: "ef".repeat(32),
            ..report.clone()
        }),
        Some(PreflightReport {
            completed_at_ms: report.completed_at_ms + 1,
            ..report.clone()
        }),
    ] {
        let mut retry = facts.clone();
        retry.preflight = changed;
        std::assert_matches!(
            store
                .register_session(worker, &retry, Duration::seconds(90))
                .await,
            Err(sbgh_core::Error::SessionRegistration(SessionRegistrationError::FactsChanged))
        );
    }
    store
        .deregister_session(worker, facts.worker_session_id)
        .await
        .unwrap();
    let legacy = session(worker, Uuid::new_v4());
    store
        .register_session(worker, &legacy, Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(
        store
            .worker_maintenance_snapshot(worker)
            .await
            .unwrap()
            .unwrap()
            .preflight,
        None
    );
}

/// Admission observations reflect database policy, identity and session facts.
#[tokio::test]
async fn lifecycle_admission_snapshot_explains_policy_session_and_identity_blockers() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let absent = store
        .worker_maintenance_snapshot(worker)
        .await
        .unwrap()
        .unwrap();
    assert!(
        absent
            .admission_blockers()
            .contains(&WorkerAdmissionBlocker::SessionInactive)
    );
    let facts = resident(worker);
    store
        .register_session(worker, &facts, Duration::seconds(90))
        .await
        .unwrap();
    let ready = store
        .worker_maintenance_snapshot(worker)
        .await
        .unwrap()
        .unwrap();
    assert!(
        ready
            .admission_blockers()
            .is_empty(),
        "{:?}",
        ready.admission_blockers()
    );
    assert_eq!(ready.effective_capabilities, vec!["build_only"]);
    for (mutation, restoration, expected) in [
        (
            "UPDATE worker_registry SET enabled=false WHERE worker_id=$1",
            "UPDATE worker_registry SET enabled=true WHERE worker_id=$1",
            WorkerAdmissionBlocker::Disabled,
        ),
        (
            "UPDATE worker_registry SET draining=true WHERE worker_id=$1",
            "UPDATE worker_registry SET draining=false WHERE worker_id=$1",
            WorkerAdmissionBlocker::Cordoned,
        ),
        (
            "UPDATE scheduler_policy SET paused=true WHERE EXISTS (SELECT 1 FROM worker_registry WHERE worker_id=$1)",
            "UPDATE scheduler_policy SET paused=false WHERE EXISTS (SELECT 1 FROM worker_registry WHERE worker_id=$1)",
            WorkerAdmissionBlocker::SchedulerPaused,
        ),
        (
            "UPDATE worker_session SET expires_at=NOW()-INTERVAL '1 second' WHERE worker_id=$1",
            "UPDATE worker_session SET expires_at=NOW()+INTERVAL '90 seconds' WHERE worker_id=$1",
            WorkerAdmissionBlocker::SessionInactive,
        ),
        (
            "UPDATE fleet_protocol_policy SET minimum_revision=3 WHERE EXISTS (SELECT 1 FROM worker_registry WHERE worker_id=$1)",
            "UPDATE fleet_protocol_policy SET minimum_revision=1 WHERE EXISTS (SELECT 1 FROM worker_registry WHERE worker_id=$1)",
            WorkerAdmissionBlocker::ProtocolIncompatible,
        ),
        (
            "UPDATE worker_session SET status='running' WHERE worker_id=$1",
            "UPDATE worker_session SET status='idle' WHERE worker_id=$1",
            WorkerAdmissionBlocker::SessionNotIdle,
        ),
    ] {
        // Inject observations only; administrative mutation semantics have separate tests.
        sqlx::query(sqlx::AssertSqlSafe(mutation))
            .bind(worker)
            .execute(&pool)
            .await
            .unwrap();
        let snapshot = store
            .worker_maintenance_snapshot(worker)
            .await
            .unwrap()
            .unwrap();
        assert!(
            snapshot
                .admission_blockers()
                .contains(&expected),
            "{:?}",
            snapshot.admission_blockers()
        );
        sqlx::query(sqlx::AssertSqlSafe(restoration))
            .bind(worker)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            store
                .worker_maintenance_snapshot(worker)
                .await
                .unwrap()
                .unwrap()
                .admission_blockers()
                .is_empty()
        );
    }
    apply(&store, &change(worker, WorkerLifecycleAction::ShutdownAfterIdle)).await;
    assert!(
        store
            .worker_maintenance_snapshot(worker)
            .await
            .unwrap()
            .unwrap()
            .admission_blockers()
            .contains(&WorkerAdmissionBlocker::ShutdownRequested)
    );
    assert!(
        store
            .deregister_session(worker, facts.worker_session_id)
            .await
            .unwrap()
    );
    let mut next = resident(worker);
    next.protocol_offer
        .as_mut()
        .unwrap()
        .features = ["resident_cordon".to_owned()].into();
    store
        .register_session(worker, &next, Duration::seconds(90))
        .await
        .unwrap();
    let unsupported = store
        .worker_maintenance_snapshot(worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        unsupported.admission_blockers(),
        vec![WorkerAdmissionBlocker::NoEffectiveCapabilities]
    );
    sqlx::query("UPDATE worker_identity_key SET revoked_at=NOW() WHERE worker_id=$1")
        .bind(worker)
        .execute(&pool)
        .await
        .unwrap();
    let revoked = store
        .worker_maintenance_snapshot(worker)
        .await
        .unwrap()
        .unwrap();
    assert!(revoked.session_live, "revocation must be visible even before liveness expires");
    assert!(
        revoked
            .admission_blockers()
            .contains(&WorkerAdmissionBlocker::IdentityUnauthorized)
    );
}

/// Session advertising the implemented lifecycle features.
fn resident(worker: Uuid) -> RegisterSessionRequest {
    RegisterSessionRequest {
        protocol_version: NEGOTIATED_PROTOCOL_REVISION,
        protocol_offer: Some(ProtocolOffer::current()),
        ..session(worker, Uuid::new_v4())
    }
}

/// Fresh attributable operator request.
fn change(worker: Uuid, action: WorkerLifecycleAction) -> WorkerLifecycleChange {
    WorkerLifecycleChange {
        request_id: Uuid::new_v4(),
        worker_id: worker,
        action,
        actor: "api:admin".into(),
        reason: "lifecycle qualification".into(),
    }
}

/// Require an applied command while retaining its immutable receipt for retries.
async fn apply(
    store: &PostgresFleetStore,
    change: &WorkerLifecycleChange,
) -> WorkerLifecycleOutcome {
    let outcome = store
        .change_worker_lifecycle(change)
        .await
        .unwrap();
    std::assert_matches!(outcome, WorkerLifecycleOutcome::Applied(_));
    outcome
}

#[tokio::test]
async fn lifecycle_cordon_is_resident_audited_and_withdraws_unaccepted_offers() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let facts = resident(worker);
    let id = facts.worker_session_id;
    store
        .register_session(worker, &facts, Duration::seconds(90))
        .await
        .unwrap();
    let job = Uuid::new_v4();
    enqueue_build(&store, job).await;
    let offer = store
        .poll_offer(worker, id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    let cordon = change(worker, WorkerLifecycleAction::Cordon);
    let receipt = apply(&store, &cordon).await;
    assert!(
        !store
            .accept_offer(worker, &offer.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    );
    assert!(
        store
            .poll_offer(worker, id, Duration::seconds(30), Duration::seconds(45))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .idle_worker_control(worker, id)
            .await
            .unwrap(),
        IdleWorkerControl::Poll
    );
    sqlx::query(
        "UPDATE worker_session SET expires_at=NOW()+INTERVAL '1 second' WHERE worker_session_id=$1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        store
            .poll_offer(worker, id, Duration::seconds(30), Duration::seconds(45))
            .await
            .unwrap()
            .is_none()
    );
    let renewed: bool = sqlx::query_scalar("SELECT expires_at > NOW()+INTERVAL '60 seconds' FROM worker_session WHERE worker_session_id=$1")
        .bind(id).fetch_one(&pool).await.unwrap();
    assert!(renewed, "cordoned polling must renew the resident session");
    apply(&store, &change(worker, WorkerLifecycleAction::Uncordon)).await;
    assert_eq!(
        store
            .change_worker_lifecycle(&cordon)
            .await
            .unwrap(),
        receipt
    );
    let mut conflicting = cordon.clone();
    conflicting.reason = "different intent".into();
    assert_eq!(
        store
            .change_worker_lifecycle(&conflicting)
            .await
            .unwrap(),
        WorkerLifecycleOutcome::Conflict
    );
    let next = store
        .poll_offer(worker, id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.offer.job_id, job);
    assert_ne!(
        next.offer.identity.attempt_id,
        offer
            .offer
            .identity
            .attempt_id
    );
    assert!(
        store
            .accept_offer(worker, &next.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    );
    for query in [
        "DELETE FROM worker_lifecycle_audit",
        "UPDATE worker_lifecycle_audit SET reason='edited'",
        "TRUNCATE worker_lifecycle_audit",
    ] {
        assert!(
            sqlx::query(sqlx::AssertSqlSafe(query))
                .execute(&pool)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn lifecycle_shutdown_preserves_active_work_waits_for_cleanup_and_never_targets_successor() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let facts = resident(worker);
    let id = facts.worker_session_id;
    store
        .register_session(worker, &facts, Duration::seconds(90))
        .await
        .unwrap();
    enqueue_build(&store, Uuid::new_v4()).await;
    let offer = store
        .poll_offer(worker, id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .accept_offer(worker, &offer.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    );
    apply(&store, &change(worker, WorkerLifecycleAction::Cordon)).await;
    assert_eq!(
        store
            .heartbeat_attempt(worker, &offer.offer.identity, Duration::seconds(45), None)
            .await
            .unwrap()
            .unwrap()
            .desired_state,
        DesiredState::Continue
    );
    apply(&store, &change(worker, WorkerLifecycleAction::Uncordon)).await;
    let shutdown = change(worker, WorkerLifecycleAction::ShutdownAfterIdle);
    let receipt = apply(&store, &shutdown).await;
    let snapshot = store
        .worker_maintenance_snapshot(worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.session_id, Some(id));
    assert_eq!(snapshot.shutdown_session_id, Some(id));
    assert!(snapshot.session_live);
    assert!(!snapshot.cordoned);
    assert_eq!(snapshot.active_attempts, 1);
    assert!(
        snapshot
            .admission_blockers()
            .contains(&WorkerAdmissionBlocker::ActiveAttempt)
    );
    assert_eq!(snapshot.pending_cleanup, 0);
    assert!(
        !store
            .workers(Some(worker))
            .await
            .unwrap()[0]
            .draining,
        "shutdown must not mutate cordon policy"
    );
    assert_eq!(
        store
            .heartbeat_attempt(worker, &offer.offer.identity, Duration::seconds(45), None)
            .await
            .unwrap()
            .unwrap()
            .desired_state,
        DesiredState::Continue
    );
    assert_eq!(
        store
            .idle_worker_control(worker, id)
            .await
            .unwrap(),
        IdleWorkerControl::Poll
    );
    assert!(
        !store
            .deregister_session(worker, id)
            .await
            .unwrap()
    );
    sqlx::query(
        "UPDATE worker_attempt SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE attempt_id=$1",
    )
    .bind(
        offer
            .offer
            .identity
            .attempt_id,
    )
    .execute(&pool)
    .await
    .unwrap();
    store
        .expire_stale_attempts()
        .await
        .unwrap();
    store
        .register_session(worker, &facts, Duration::seconds(90))
        .await
        .unwrap();
    let obligations = store
        .cleanup_obligations(worker, id)
        .await
        .unwrap();
    assert_eq!(obligations.len(), 1);
    let snapshot = store
        .worker_maintenance_snapshot(worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.active_attempts, 0);
    assert_eq!(snapshot.pending_cleanup, 1);
    assert!(
        snapshot
            .admission_blockers()
            .contains(&WorkerAdmissionBlocker::PendingCleanup)
    );
    assert_eq!(
        store
            .idle_worker_control(worker, id)
            .await
            .unwrap(),
        IdleWorkerControl::Poll
    );
    assert!(
        !store
            .deregister_session(worker, id)
            .await
            .unwrap()
    );
    assert!(
        store
            .complete_cleanup(worker, id, obligations[0].id)
            .await
            .unwrap()
    );
    assert!(
        store
            .poll_offer(worker, id, Duration::seconds(30), Duration::seconds(45))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .idle_worker_control(worker, id)
            .await
            .unwrap(),
        IdleWorkerControl::ShutdownAfterIdle
    );
    assert!(
        store
            .deregister_session(worker, id)
            .await
            .unwrap()
    );
    assert!(
        store
            .deregister_session(worker, id)
            .await
            .unwrap(),
        "lost acknowledgement must be retryable"
    );
    let snapshot = store
        .worker_maintenance_snapshot(worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.active_attempts, 0);
    assert_eq!(snapshot.pending_cleanup, 0);
    assert!(!snapshot.session_live);
    assert_eq!(
        snapshot
            .session_status
            .as_deref(),
        Some("deregistered")
    );
    let successor = resident(worker);
    store
        .register_session(worker, &successor, Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(
        store
            .change_worker_lifecycle(&shutdown)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .idle_worker_control(worker, successor.worker_session_id)
            .await
            .unwrap(),
        IdleWorkerControl::Poll
    );
    assert!(
        store
            .poll_offer(
                worker,
                successor.worker_session_id,
                Duration::seconds(30),
                Duration::seconds(45)
            )
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn lifecycle_legacy_cordon_keeps_exit_semantics_and_rejects_explicit_shutdown() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool);
    let worker = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    let shutdown = change(worker, WorkerLifecycleAction::ShutdownAfterIdle);
    assert_eq!(
        store
            .change_worker_lifecycle(&shutdown)
            .await
            .unwrap(),
        WorkerLifecycleOutcome::UnsupportedSession
    );
    let facts = session(worker, Uuid::new_v4());
    store
        .register_session(worker, &facts, Duration::seconds(90))
        .await
        .unwrap();
    assert_eq!(
        store
            .change_worker_lifecycle(&shutdown)
            .await
            .unwrap(),
        WorkerLifecycleOutcome::UnsupportedSession
    );
    apply(&store, &change(worker, WorkerLifecycleAction::Cordon)).await;
    assert_eq!(
        store
            .idle_worker_control(worker, facts.worker_session_id)
            .await
            .unwrap(),
        IdleWorkerControl::LegacyDrain
    );
}

#[tokio::test]
async fn lifecycle_cordon_and_accept_serialize_at_the_registry_lock_in_both_orders() {
    for accept_first in [false, true] {
        let (_db, pool) = setup_pg_db().await;
        seed_install_repo(&pool, 100, 10).await;
        let store = PostgresFleetStore::new(pool.clone());
        let worker = Uuid::new_v4();
        store
            .seed_worker(&registration(worker))
            .await
            .unwrap();
        let facts = resident(worker);
        store
            .register_session(worker, &facts, Duration::seconds(90))
            .await
            .unwrap();
        enqueue_build(&store, Uuid::new_v4()).await;
        let offer = store
            .poll_offer(
                worker,
                facts.worker_session_id,
                Duration::seconds(30),
                Duration::seconds(45),
            )
            .await
            .unwrap()
            .unwrap();
        let identity = offer.offer.identity;
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("SELECT worker_id FROM worker_registry WHERE worker_id=$1 FOR UPDATE")
            .bind(worker)
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let accept = || {
            let store = store.clone();
            let identity = identity.clone();
            tokio::spawn(async move {
                store
                    .accept_offer(worker, &identity, Duration::seconds(45))
                    .await
                    .unwrap()
            })
        };
        let cordon = || {
            let store = store.clone();
            tokio::spawn(async move {
                apply(&store, &change(worker, WorkerLifecycleAction::Cordon)).await
            })
        };
        let (accept_task, cordon_task) = if accept_first {
            let accepted = accept();
            wait_for_lock(
                &pool,
                "SELECT enabled, draining, shutdown_session_id FROM worker_registry%",
                1,
            )
            .await;
            let cordoned = cordon();
            wait_for_lock(
                &pool,
                "SELECT draining, shutdown_session_id, lifecycle_generation FROM worker_registry%",
                1,
            )
            .await;
            (accepted, cordoned)
        } else {
            let cordoned = cordon();
            wait_for_lock(
                &pool,
                "SELECT draining, shutdown_session_id, lifecycle_generation FROM worker_registry%",
                1,
            )
            .await;
            let accepted = accept();
            wait_for_lock(
                &pool,
                "SELECT enabled, draining, shutdown_session_id FROM worker_registry%",
                1,
            )
            .await;
            (accepted, cordoned)
        };
        blocker
            .commit()
            .await
            .unwrap();
        let (accepted, cordoned) = time::timeout(StdDuration::from_secs(5), async {
            tokio::join!(accept_task, cordon_task)
        })
        .await
        .unwrap();
        assert_eq!(accepted.unwrap(), accept_first);
        std::assert_matches!(cordoned.unwrap(), WorkerLifecycleOutcome::Applied(_));
        if accept_first {
            assert_eq!(
                store
                    .heartbeat_attempt(worker, &identity, Duration::seconds(45), None)
                    .await
                    .unwrap()
                    .unwrap()
                    .desired_state,
                DesiredState::Continue
            );
        }
    }
}
