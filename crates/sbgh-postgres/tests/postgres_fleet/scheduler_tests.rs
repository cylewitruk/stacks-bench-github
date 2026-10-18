//! Scheduler maintenance against the production store and migrated schema.

use sbgh_core::db::protocol::{
    ProtocolPolicyChange, ProtocolPolicyChangeOutcome, ProtocolPolicyStore,
};
use sbgh_core::db::scheduler::{SchedulerChange, SchedulerChangeOutcome, SchedulerStore};
use sbgh_fleet::{NEGOTIATED_PROTOCOL_REVISION, ProtocolOffer};

use super::*;

/// A retry-safe administrative request.
fn change(paused: bool) -> SchedulerChange {
    SchedulerChange {
        request_id: Uuid::new_v4(),
        paused,
        actor: "api:admin".into(),
        reason: "planned maintenance".into(),
    }
}

/// Apply a fresh change and return its generation.
async fn set_paused(store: &PostgresFleetStore, paused: bool) -> i64 {
    let SchedulerChangeOutcome::Applied(policy) = store
        .change_scheduler(&change(paused))
        .await
        .unwrap()
    else {
        panic!("fresh request conflicted");
    };
    policy.generation
}

/// Initialize one authorized, resident worker with a queued build.
async fn ready(pool: &Pool) -> (PostgresFleetStore, Uuid, Uuid, Uuid) {
    seed_install_repo(pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let job = Uuid::new_v4();
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    store
        .register_session(
            worker,
            &RegisterSessionRequest {
                protocol_version: NEGOTIATED_PROTOCOL_REVISION,
                protocol_offer: Some(ProtocolOffer::current()),
                ..session(worker, session_id)
            },
            Duration::seconds(90),
        )
        .await
        .unwrap();
    enqueue_build(&store, job).await;
    (store, worker, session_id, job)
}

#[tokio::test]
async fn scheduler_audit_is_idempotent_immutable_and_survives_store_recreation() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresFleetStore::new(pool.clone());
    assert_eq!(
        store
            .scheduler_policy()
            .await
            .unwrap()
            .generation,
        0
    );
    let pause = change(true);
    let original = store
        .change_scheduler(&pause)
        .await
        .unwrap();
    assert_eq!(
        original,
        store
            .change_scheduler(&pause)
            .await
            .unwrap()
    );
    assert_eq!(
        set_paused(&store, true).await,
        1,
        "a no-op records audit without changing generation"
    );
    assert_eq!(set_paused(&store, false).await, 2);
    let recreated = PostgresFleetStore::new(pool.clone());
    assert_eq!(
        original,
        recreated
            .change_scheduler(&pause)
            .await
            .unwrap(),
        "replay returns original receipt, never reapplies an old pause"
    );
    assert!(
        !recreated
            .scheduler_policy()
            .await
            .unwrap()
            .paused
    );
    let mut conflicting = pause.clone();
    conflicting.reason = "different explanation".into();
    assert_eq!(
        recreated
            .change_scheduler(&conflicting)
            .await
            .unwrap(),
        SchedulerChangeOutcome::Conflict
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM scheduler_administration_audit")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 3);
    for sql in [
        "DELETE FROM scheduler_administration_audit",
        "UPDATE scheduler_administration_audit SET reason = 'rewritten'",
        "TRUNCATE scheduler_administration_audit",
    ] {
        assert!(
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .execute(&pool)
                .await
                .is_err()
        );
    }
    let mut invalid = change(true);
    invalid.reason = " ".into();
    assert!(
        recreated
            .change_scheduler(&invalid)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn scheduler_pause_keeps_worker_resident_and_resume_restores_offers() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, job) = ready(&pool).await;
    let before: (bool, String) = sqlx::query_as("SELECT r.draining, s.status::text FROM worker_registry r JOIN worker_session s USING(worker_id) WHERE worker_id = $1").bind(worker).fetch_one(&pool).await.unwrap();
    set_paused(&store, true).await;
    assert!(
        store
            .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
            .await
            .unwrap()
            .is_none()
    );
    let after: (bool, String) = sqlx::query_as("SELECT r.draining, s.status::text FROM worker_registry r JOIN worker_session s USING(worker_id) WHERE worker_id = $1").bind(worker).fetch_one(&pool).await.unwrap();
    assert_eq!(before, after);
    assert!(
        store
            .session_is_active(worker, session_id)
            .await
            .unwrap()
    );
    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(job)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "queued");
    set_paused(&store, false).await;
    assert!(
        store
            .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn scheduler_pause_fences_old_offers_but_keeps_accepted_work_live() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, _) = ready(&pool).await;
    let offer = store
        .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    set_paused(&store, true).await;
    assert!(
        !store
            .accept_offer(worker, &offer.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    );
    set_paused(&store, false).await;
    assert!(
        !store
            .accept_offer(worker, &offer.offer.identity, Duration::seconds(45))
            .await
            .unwrap(),
        "resume cannot revive an old offer"
    );
    let new = store
        .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .accept_offer(worker, &new.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    );
    set_paused(&store, true).await;
    assert!(
        store
            .accept_offer(worker, &new.offer.identity, Duration::seconds(45))
            .await
            .unwrap(),
        "lost acceptance acknowledgement remains idempotent"
    );
    assert!(
        store
            .heartbeat_attempt(worker, &new.offer.identity, Duration::seconds(45), None)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .scheduler_snapshot()
            .await
            .unwrap()
            .active_attempts,
        1
    );
    let outcome = TerminalOutcome::Completed {
        summary: serde_json::json!({}),
        block_validation: None,
    };
    let terminal = event(
        &new.offer.identity,
        new.offer.trace_id,
        1,
        ReliableEventPayload::Terminal {
            outcome_digest: sbgh_fleet::payload_digest(&outcome).unwrap(),
        },
    );
    store
        .ingest_reliable_event(worker, &terminal)
        .await
        .unwrap();
    let write = FleetTerminalWrite::Completed(Box::new(FleetCompletion {
        result: JobResult {
            job_id: new.offer.job_id,
            run_json: None,
            archive_dir: "job".into(),
            created_at: Utc::now(),
        },
        metric: None,
        baseline_calibration_id: None,
        event_detail: None,
        block_validation: None,
        artifact_manifest: Vec::new(),
    }));
    assert_eq!(
        store
            .accept_terminal(
                worker,
                &new.offer.identity,
                &FleetTerminalSubmission {
                    reliable_seq: 1,
                    payload_digest: &terminal.payload_digest,
                    outcome: &outcome,
                    artifacts: &[],
                    write: &write,
                }
            )
            .await
            .unwrap(),
        TerminalAcceptance::Accepted
    );
    let snapshot = store
        .scheduler_snapshot()
        .await
        .unwrap();
    assert!(snapshot.policy.paused);
    assert_eq!(snapshot.active_attempts, 0);
    assert_eq!(
        snapshot.pending_projection, 1,
        "terminal projection remains visible to maintenance wait"
    );
}

#[tokio::test]
async fn scheduler_snapshot_excludes_fenced_artifact_garbage_but_retains_cleanup_debt() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, job) = ready(&pool).await;
    let (identity, _) = running_attempt(&store, worker, session_id, job).await;
    let object_key = format!("staging/{}/nonce/{job}/empty.log", identity.attempt_id);
    assert!(
        store
            .record_artifact_grant(&ArtifactGrantRecord {
                attempt_id: identity.attempt_id,
                object_key: object_key.clone(),
                logical_key: format!("{job}/empty.log"),
                size: Some(0),
                sha256: Some(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into()
                ),
                expires_at: Utc::now() + Duration::minutes(5),
            })
            .await
            .unwrap()
    );
    set_paused(&store, true).await;
    assert_eq!(
        store
            .scheduler_snapshot()
            .await
            .unwrap()
            .pending_artifacts,
        1,
        "zero bytes still blocks while the attempt is active"
    );
    let successor = Uuid::new_v4();
    store
        .register_session(worker, &session(worker, successor), Duration::minutes(5))
        .await
        .unwrap();
    let snapshot = store
        .scheduler_snapshot()
        .await
        .unwrap();
    assert!(snapshot.policy.paused);
    assert_eq!(snapshot.active_attempts, 0);
    assert_eq!(snapshot.pending_cleanup, 1);
    assert_eq!(snapshot.pending_projection, 0);
    assert_eq!(snapshot.pending_artifacts, 0);
    assert_eq!(snapshot.garbage_artifacts, 1);

    let obligations = store
        .cleanup_obligations(worker, successor)
        .await
        .unwrap();
    assert_eq!(obligations.len(), 1);
    assert!(
        store
            .complete_cleanup(worker, successor, obligations[0].id)
            .await
            .unwrap()
    );
    let snapshot = store
        .scheduler_snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.pending_cleanup, 0);
    assert_eq!(snapshot.pending_artifacts, 0);
    assert_eq!(snapshot.garbage_artifacts, 1);
    assert!(
        store
            .staged_artifacts_due_for_reap(Utc::now() - Duration::days(1))
            .await
            .unwrap()
            .is_empty(),
        "maintenance does not bypass staging retention"
    );
    let snapshot = store
        .scheduler_snapshot()
        .await
        .unwrap();
    assert!(snapshot.policy.paused);
    assert_eq!(snapshot.active_attempts, 0);
    assert_eq!(snapshot.pending_cleanup, 0);
    assert_eq!(snapshot.pending_projection, 0);
    assert_eq!(snapshot.pending_artifacts, 0);
}

/// Try a real floor change so its admission predicate is checked alongside status.
async fn advance_floor(store: &PostgresFleetStore) -> ProtocolPolicyChangeOutcome {
    store
        .change_protocol_policy(&ProtocolPolicyChange {
            request_id: Uuid::new_v4(),
            minimum_revision: 2,
            actor: "api:admin".into(),
            reason: "quiescence regression".into(),
            rollback_evidence: "isolated test".into(),
        })
        .await
        .unwrap()
}

/// Grant one empty staging object, retaining its descriptor for terminal acceptance.
async fn grant_empty(
    store: &PostgresFleetStore,
    identity: &AttemptIdentity,
    job: Uuid,
    name: &str,
) -> ArtifactDescriptor {
    let artifact = ArtifactDescriptor {
        key: format!("staging/{}/nonce/{job}/{name}", identity.attempt_id),
        logical_key: format!("{job}/{name}"),
        size: 0,
        sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
    };
    assert!(
        store
            .record_artifact_grant(&ArtifactGrantRecord {
                attempt_id: identity.attempt_id,
                object_key: artifact.key.clone(),
                logical_key: artifact.logical_key.clone(),
                size: Some(artifact.size),
                sha256: Some(artifact.sha256.clone()),
                expires_at: Utc::now() + Duration::minutes(5),
            })
            .await
            .unwrap()
    );
    artifact
}

/// Expiry and acknowledged teardown unblock maintenance without forcing staging GC.
#[tokio::test]
async fn scheduler_and_floor_ignore_expired_uploads_after_real_cleanup() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, job) = ready(&pool).await;
    let (identity, _) = running_attempt(&store, worker, session_id, job).await;
    grant_empty(&store, &identity, job, "unused.log").await;
    set_paused(&store, true).await;
    sqlx::query(
        "UPDATE worker_attempt SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE attempt_id=$1",
    )
    .bind(identity.attempt_id)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        store
            .expire_stale_attempts()
            .await
            .unwrap(),
        1
    );
    assert_eq!(advance_floor(&store).await, ProtocolPolicyChangeOutcome::MaintenanceRequired);
    store
        .register_session(
            worker,
            &RegisterSessionRequest {
                protocol_version: NEGOTIATED_PROTOCOL_REVISION,
                protocol_offer: Some(ProtocolOffer::current()),
                ..session(worker, session_id)
            },
            Duration::minutes(5),
        )
        .await
        .unwrap();
    let obligations = store
        .cleanup_obligations(worker, session_id)
        .await
        .unwrap();
    assert_eq!(obligations.len(), 1);
    assert!(
        store
            .complete_cleanup(worker, session_id, obligations[0].id)
            .await
            .unwrap()
    );
    let snapshot = store
        .scheduler_snapshot()
        .await
        .unwrap();
    assert_eq!(
        (
            snapshot.active_attempts,
            snapshot.pending_cleanup,
            snapshot.pending_projection,
            snapshot.pending_artifacts
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(snapshot.garbage_artifacts, 1);
    assert!(
        store
            .staged_artifacts_due_for_reap(Utc::now() - Duration::days(1))
            .await
            .unwrap()
            .is_empty()
    );
    std::assert_matches!(advance_floor(&store).await, ProtocolPolicyChangeOutcome::Applied(_));
}

/// Only accepted manifest membership retains promotion debt after terminal acceptance.
#[tokio::test]
async fn scheduler_and_floor_keep_manifest_promotion_debt_not_verified_garbage() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, job) = ready(&pool).await;
    let (identity, trace_id) = running_attempt(&store, worker, session_id, job).await;
    let accepted = grant_empty(&store, &identity, job, "accepted.log").await;
    let unused = grant_empty(&store, &identity, job, "unused.log").await;
    assert!(
        store
            .verify_artifact(&identity, &accepted)
            .await
            .unwrap()
    );
    set_paused(&store, true).await;
    let outcome = TerminalOutcome::Cancelled { reason: "test".into() };
    let terminal = event(
        &identity,
        trace_id,
        1,
        ReliableEventPayload::Terminal {
            outcome_digest: sbgh_fleet::payload_digest(&outcome).unwrap(),
        },
    );
    store
        .ingest_reliable_event(worker, &terminal)
        .await
        .unwrap();
    assert_eq!(
        store
            .accept_terminal(
                worker,
                &identity,
                &FleetTerminalSubmission {
                    reliable_seq: 1,
                    payload_digest: &terminal.payload_digest,
                    outcome: &outcome,
                    artifacts: std::slice::from_ref(&accepted),
                    write: &FleetTerminalWrite::Cancelled { remark: "test".into() },
                }
            )
            .await
            .unwrap(),
        TerminalAcceptance::Accepted
    );
    assert!(
        store
            .mark_event_projected(identity.attempt_id, 1)
            .await
            .unwrap()
    );
    let snapshot = store
        .scheduler_snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.pending_projection, 0);
    assert_eq!(snapshot.pending_artifacts, 1, "accepted zero-byte artifact still needs promotion");
    assert_eq!(snapshot.garbage_artifacts, 1, "unused grant is not promotion debt");
    assert_eq!(advance_floor(&store).await, ProtocolPolicyChangeOutcome::MaintenanceRequired);
    // A late verification must not expand the already accepted terminal manifest.
    assert!(
        store
            .verify_artifact(&identity, &unused)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .scheduler_snapshot()
            .await
            .unwrap()
            .pending_artifacts,
        1
    );
    assert!(
        store
            .mark_artifacts_promoted(identity.attempt_id, std::slice::from_ref(&accepted))
            .await
            .unwrap()
    );
    let snapshot = store
        .scheduler_snapshot()
        .await
        .unwrap();
    assert_eq!((snapshot.pending_artifacts, snapshot.garbage_artifacts), (0, 1));
    assert!(
        store
            .staged_artifacts_due_for_reap(Utc::now() - Duration::days(1))
            .await
            .unwrap()
            .is_empty()
    );
    std::assert_matches!(advance_floor(&store).await, ProtocolPolicyChangeOutcome::Applied(_));
}

/// Withdrawing an offer leaves its resident session eligible for a clean stop.
#[tokio::test]
async fn scheduler_pause_repairs_withdrawn_session_before_deregistration() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, _) = ready(&pool).await;
    assert!(
        store
            .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
            .await
            .unwrap()
            .is_some()
    );
    set_paused(&store, true).await;
    let status: String =
        sqlx::query_scalar("SELECT status::text FROM worker_session WHERE worker_session_id=$1")
            .bind(session_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "idle");
    assert!(
        store
            .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .deregister_session(worker, session_id)
            .await
            .unwrap()
    );
}

/// Pause coexists with expiry locks and preserves the sweep's offline session state.
#[tokio::test]
async fn scheduler_pause_waits_for_expiry_without_reviving_its_session() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, _) = ready(&pool).await;
    let offered = store
        .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    sqlx::query(
        "UPDATE worker_attempt SET offer_expires_at=NOW()-INTERVAL '1 second' WHERE attempt_id=$1",
    )
    .bind(
        offered
            .offer
            .identity
            .attempt_id,
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "SELECT worker_session_id FROM worker_session WHERE worker_session_id=$1 FOR UPDATE",
    )
    .bind(session_id)
    .execute(&mut *tx)
    .await
    .unwrap();
    let expiry_store = store.clone();
    let expiry = tokio::spawn(async move {
        expiry_store
            .expire_stale_attempts()
            .await
            .unwrap()
    });
    // The attempt update checks its session foreign key before the explicit
    // session update; the expiry transaction already owns the attempt lock.
    wait_for_lock(&pool, "%UPDATE worker_attempt%SET status = 'expired'%", 1).await;
    let pause_store = store.clone();
    let pause = tokio::spawn(async move { set_paused(&pause_store, true).await });
    // Pause's registry lock must coexist with expiry's foreign-key key-share lock.
    wait_for_lock(
        &pool,
        "%SELECT attempt_id FROM worker_attempt WHERE status='offered'%FOR UPDATE",
        1,
    )
    .await;
    tx.commit().await.unwrap();
    assert_eq!(expiry.await.unwrap(), 1);
    assert_eq!(pause.await.unwrap(), 1);
    let status: String =
        sqlx::query_scalar("SELECT status::text FROM worker_session WHERE worker_session_id=$1")
            .bind(session_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "offline", "pause must not revive an expired session");
}

#[tokio::test]
async fn scheduler_generation_fences_preparation_across_pause_and_resume() {
    let (_db, pool) = setup_pg_db().await;
    let (store, _, _, job) = ready(&pool).await;
    sqlx::query("UPDATE job SET execution_payload = NULL, execution_payload_hash = NULL, execution_payload_version = NULL WHERE id = $1")
        .bind(job).execute(&pool).await.unwrap();
    let mut prepared = PreparedExecution {
        scheduler_generation: 0,
        job_id: job,
        commit: "1".repeat(40),
        payload: TaskPayload::BuildOnly,
        payload_hash: sbgh_fleet::payload_digest(&TaskPayload::BuildOnly).unwrap(),
    };
    set_paused(&store, true).await;
    assert!(
        !store
            .prepare_execution(&prepared)
            .await
            .unwrap()
    );
    prepared.scheduler_generation = set_paused(&store, false).await;
    let mut stale = prepared.clone();
    stale.scheduler_generation = 0;
    assert!(
        !store
            .prepare_execution(&stale)
            .await
            .unwrap()
    );
    assert!(
        store
            .prepare_execution(&prepared)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn scheduler_pause_commit_fences_waiting_prepare_poll_and_accept() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, _) = ready(&pool).await;
    let offer = store
        .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    let job = Uuid::new_v4();
    enqueue_build(&store, job).await;
    sqlx::query("UPDATE job SET execution_payload = NULL, execution_payload_hash = NULL, execution_payload_version = NULL WHERE id = $1")
        .bind(job).execute(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT singleton FROM scheduler_policy FOR UPDATE")
        .execute(&mut *tx)
        .await
        .unwrap();
    let poll_store = store.clone();
    let accept_store = store.clone();
    let prepare_store = store.clone();
    let poll = tokio::spawn(async move {
        poll_store
            .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
            .await
            .unwrap()
    });
    let accept = tokio::spawn(async move {
        accept_store
            .accept_offer(worker, &offer.offer.identity, Duration::seconds(45))
            .await
            .unwrap()
    });
    let prepare = tokio::spawn(async move {
        prepare_store
            .prepare_execution(&PreparedExecution {
                scheduler_generation: 0,
                job_id: job,
                commit: "1".repeat(40),
                payload: TaskPayload::BuildOnly,
                payload_hash: sbgh_fleet::payload_digest(&TaskPayload::BuildOnly).unwrap(),
            })
            .await
            .unwrap()
    });
    wait_for_lock(&pool, "SELECT * FROM scheduler_policy WHERE singleton FOR SHARE", 3).await;
    sqlx::query("UPDATE scheduler_policy SET paused = TRUE, generation = generation + 1")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(poll.await.unwrap().is_none());
    assert!(!accept.await.unwrap());
    assert!(!prepare.await.unwrap());
}

#[tokio::test]
async fn scheduler_pause_waits_for_inflight_acceptance_without_cancelling_it() {
    let (_db, pool) = setup_pg_db().await;
    let (store, worker, session_id, _) = ready(&pool).await;
    let offer = store
        .poll_offer(worker, session_id, Duration::seconds(30), Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    let identity = offer.offer.identity;
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT attempt_id FROM worker_attempt WHERE attempt_id = $1 FOR UPDATE")
        .bind(identity.attempt_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let accept_store = store.clone();
    let accepted_identity = identity.clone();
    let accept = tokio::spawn(async move {
        accept_store
            .accept_offer(worker, &accepted_identity, Duration::seconds(45))
            .await
            .unwrap()
    });
    wait_for_lock(
        &pool,
        "%SELECT job_id, claim_token, status::text AS status, scheduler_generation%",
        1,
    )
    .await;
    let pause_store = store.clone();
    let pause = tokio::spawn(async move { set_paused(&pause_store, true).await });
    wait_for_lock(&pool, "SELECT * FROM scheduler_policy WHERE singleton FOR UPDATE", 1).await;
    tx.commit().await.unwrap();
    assert!(accept.await.unwrap());
    assert_eq!(pause.await.unwrap(), 1);
    assert!(
        store
            .heartbeat_attempt(worker, &identity, Duration::seconds(45), None)
            .await
            .unwrap()
            .is_some()
    );
    let snapshot = store
        .scheduler_snapshot()
        .await
        .unwrap();
    assert!(snapshot.policy.paused);
    assert_eq!(snapshot.active_attempts, 1);
}
