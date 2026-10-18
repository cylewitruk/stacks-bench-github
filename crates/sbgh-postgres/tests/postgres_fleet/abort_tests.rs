//! Atomic abort intent, frozen replay selection and teardown-gated cancellation.

use sbgh_core::db::fleet_abort::{
    FleetAbortOutcome, FleetAbortRequest, FleetAbortScope, FleetAbortStore,
};
use sbgh_core::db::scheduler::{SchedulerChange, SchedulerChangeOutcome, SchedulerStore};
use sbgh_fleet::DesiredState;

use super::*;

/// Fresh audited request at the caller's observed pause generation.
fn request(generation: i64, scope: FleetAbortScope) -> FleetAbortRequest {
    FleetAbortRequest {
        request_id: Uuid::new_v4(),
        scope,
        scheduler_generation: generation,
        actor: "api:admin".into(),
        reason: "abort qualification".into(),
    }
}

/// Commit a distinct pause/resume transition and return its observed generation.
async fn pause(store: &PostgresFleetStore, paused: bool) -> i64 {
    let SchedulerChangeOutcome::Applied(policy) = store
        .change_scheduler(&SchedulerChange {
            request_id: Uuid::new_v4(),
            paused,
            actor: "api:admin".into(),
            reason: "abort qualification".into(),
        })
        .await
        .unwrap()
    else {
        panic!("new request conflicted")
    };
    policy.generation
}

/// Read durable job state independently of the abort receipt.
async fn status(pool: &Pool, job: Uuid) -> String {
    sqlx::query_scalar("SELECT status::text FROM job WHERE id=$1")
        .bind(job)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Enroll and start a real store-backed attempt without simulating a VM.
async fn running(
    store: &PostgresFleetStore,
) -> (Uuid, RegisterSessionRequest, Uuid, AttemptIdentity) {
    let worker = Uuid::new_v4();
    let facts = session(worker, Uuid::new_v4());
    store
        .seed_worker(&registration(worker))
        .await
        .unwrap();
    store
        .register_session(worker, &facts, Duration::minutes(5))
        .await
        .unwrap();
    let job = Uuid::new_v4();
    enqueue_build(store, job).await;
    let (identity, _) = running_attempt(store, worker, facts.worker_session_id, job).await;
    (worker, facts, job, identity)
}

#[tokio::test]
async fn fleet_abort_requires_explicit_pause_and_replays_frozen_selection() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let job = Uuid::new_v4();
    enqueue_build(&store, job).await;
    assert_eq!(
        store
            .abort_fleet(&request(0, FleetAbortScope::AllPending))
            .await
            .unwrap(),
        FleetAbortOutcome::MaintenanceRequired
    );
    let generation = pause(&store, true).await;
    assert_eq!(
        store
            .abort_fleet(&request(generation + 1, FleetAbortScope::AllPending))
            .await
            .unwrap(),
        FleetAbortOutcome::MaintenanceRequired
    );
    let active = store
        .abort_fleet(&request(generation, FleetAbortScope::Active))
        .await
        .unwrap();
    std::assert_matches!(active, FleetAbortOutcome::Applied(ref receipt) if receipt.selected_job_ids.is_empty());
    assert_eq!(status(&pool, job).await, "queued");
    let command = request(generation, FleetAbortScope::AllPending);
    let original = store
        .abort_fleet(&command)
        .await
        .unwrap();
    std::assert_matches!(&original, FleetAbortOutcome::Applied(receipt) if receipt.selected_job_ids == vec![job] && receipt.accepted_job_ids == vec![job]);
    assert_eq!(status(&pool, job).await, "cancelled");
    let later = Uuid::new_v4();
    enqueue_build(&store, later).await;
    pause(&store, false).await;
    let recreated = PostgresFleetStore::new(pool.clone());
    assert_eq!(
        recreated
            .abort_fleet(&command)
            .await
            .unwrap(),
        original
    );
    assert_eq!(status(&pool, later).await, "queued", "replay cannot expand the selected workload");
    for changed in [
        FleetAbortRequest {
            reason: "different".into(),
            ..command.clone()
        },
        FleetAbortRequest {
            actor: "other".into(),
            ..command.clone()
        },
        FleetAbortRequest {
            scheduler_generation: generation + 1,
            ..command.clone()
        },
        FleetAbortRequest {
            scope: FleetAbortScope::Active,
            ..command.clone()
        },
    ] {
        assert_eq!(
            store
                .abort_fleet(&changed)
                .await
                .unwrap(),
            FleetAbortOutcome::Conflict
        );
    }
    for sql in [
        "DELETE FROM fleet_abort_audit",
        "UPDATE fleet_abort_audit SET receipt='{}'",
        "TRUNCATE fleet_abort_audit",
    ] {
        assert!(
            sqlx::query(sql)
                .execute(&pool)
                .await
                .is_err(),
            "audit must be append-only"
        );
    }
}

#[tokio::test]
async fn fleet_abort_cancels_multiple_active_workers_without_changing_their_policy() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let first = running(&store).await;
    let second = running(&store).await;
    let queued = Uuid::new_v4();
    enqueue_build(&store, queued).await;
    let generation = pause(&store, true).await;
    let FleetAbortOutcome::Applied(receipt) = store
        .abort_fleet(&request(generation, FleetAbortScope::Active))
        .await
        .unwrap()
    else {
        panic!("abort rejected")
    };
    assert_eq!(receipt.accepted_job_ids.len(), 2);
    for (worker, facts, job, identity) in [first, second] {
        assert!(
            receipt
                .accepted_job_ids
                .contains(&job)
        );
        assert_eq!(status(&pool, job).await, "running", "receipt is not teardown completion");
        assert_eq!(
            store
                .heartbeat_attempt(worker, &identity, Duration::seconds(45), Some(0))
                .await
                .unwrap()
                .unwrap()
                .desired_state,
            DesiredState::Cancel
        );
        assert!(
            store
                .session_is_active(worker, facts.worker_session_id)
                .await
                .unwrap()
        );
        let draining: bool =
            sqlx::query_scalar("SELECT draining FROM worker_registry WHERE worker_id=$1")
                .bind(worker)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!draining);
    }
    assert_eq!(status(&pool, queued).await, "queued");
    assert!(
        store
            .scheduler_snapshot()
            .await
            .unwrap()
            .policy
            .paused
    );
}

#[tokio::test]
async fn fleet_abort_expired_execution_cancels_after_cleanup_instead_of_requeueing() {
    for fleet_wide in [false, true] {
        let (_db, pool) = setup_pg_db().await;
        seed_install_repo(&pool, 100, 10).await;
        let store = PostgresFleetStore::new(pool.clone());
        let (worker, facts, job, identity) = running(&store).await;
        sqlx::query("UPDATE worker_attempt SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE attempt_id=$1")
            .bind(identity.attempt_id).execute(&pool).await.unwrap();
        store
            .expire_stale_attempts()
            .await
            .unwrap();
        let generation = pause(&store, true).await;
        if fleet_wide {
            std::assert_matches!(store.abort_fleet(&request(generation, FleetAbortScope::Active)).await.unwrap(),
                FleetAbortOutcome::Applied(receipt) if receipt.accepted_job_ids == vec![job]);
        } else {
            assert!(
                store
                    .request_cancel(job)
                    .await
                    .unwrap()
            );
        }
        assert_eq!(status(&pool, job).await, "running");
        store
            .register_session(worker, &facts, Duration::seconds(90))
            .await
            .unwrap();
        let obligations = store
            .cleanup_obligations(worker, facts.worker_session_id)
            .await
            .unwrap();
        assert_eq!(obligations.len(), 1);
        assert!(!obligations[0].requeue_job);
        assert!(
            store
                .complete_cleanup(worker, facts.worker_session_id, obligations[0].id)
                .await
                .unwrap()
        );
        assert!(
            store
                .complete_cleanup(worker, facts.worker_session_id, obligations[0].id)
                .await
                .unwrap()
        );
        assert_eq!(status(&pool, job).await, "cancelled");
    }
}

#[tokio::test]
async fn fleet_abort_audit_failure_rolls_back_all_cancellation_changes() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresFleetStore::new(pool.clone());
    let job = Uuid::new_v4();
    enqueue_build(&store, job).await;
    let generation = pause(&store, true).await;
    sqlx::raw_sql("CREATE FUNCTION fail_abort_audit() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END; $$;
        CREATE TRIGGER fail_abort_audit BEFORE INSERT ON fleet_abort_audit FOR EACH ROW EXECUTE FUNCTION fail_abort_audit();")
        .execute(&pool).await.unwrap();
    assert!(
        store
            .abort_fleet(&request(generation, FleetAbortScope::AllPending))
            .await
            .is_err()
    );
    assert_eq!(status(&pool, job).await, "queued");
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM job_event WHERE job_id=$1 AND event_kind='cancelled'",
    )
    .bind(job)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(events, 0);
}

#[tokio::test]
async fn fleet_abort_is_serialized_with_resume_in_both_lock_orders() {
    for abort_first in [false, true] {
        let (_db, pool) = setup_pg_db().await;
        seed_install_repo(&pool, 100, 10).await;
        let store = PostgresFleetStore::new(pool.clone());
        let job = Uuid::new_v4();
        enqueue_build(&store, job).await;
        let generation = pause(&store, true).await;
        let command = request(generation, FleetAbortScope::AllPending);
        let mut tx = pool.begin().await.unwrap();
        if abort_first {
            sqlx::query("SELECT id FROM job WHERE id=$1 FOR UPDATE")
                .bind(job)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        } else {
            sqlx::query("SELECT * FROM scheduler_policy WHERE singleton FOR UPDATE")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        }
        let abort_store = store.clone();
        let abort = tokio::spawn(async move {
            abort_store
                .abort_fleet(&command)
                .await
                .unwrap()
        });
        if abort_first {
            wait_for_lock(&pool, "SELECT status::text FROM job WHERE id = $1 FOR UPDATE", 1).await;
            let resume_store = store.clone();
            let resume = tokio::spawn(async move { pause(&resume_store, false).await });
            wait_for_lock(&pool, "SELECT * FROM scheduler_policy WHERE singleton FOR UPDATE", 1)
                .await;
            tx.commit().await.unwrap();
            std::assert_matches!(abort.await.unwrap(), FleetAbortOutcome::Applied(_));
            assert_eq!(resume.await.unwrap(), generation + 1);
            assert_eq!(status(&pool, job).await, "cancelled");
        } else {
            wait_for_lock(
                &pool,
                "SELECT paused, generation FROM scheduler_policy WHERE singleton FOR UPDATE",
                1,
            )
            .await;
            sqlx::query("UPDATE scheduler_policy SET paused=false, generation=generation+1")
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            assert_eq!(abort.await.unwrap(), FleetAbortOutcome::MaintenanceRequired);
            assert_eq!(status(&pool, job).await, "queued");
        }
    }
}

#[tokio::test]
async fn fleet_abort_and_cleanup_acknowledgement_serialize_without_resurrecting_work() {
    for abort_first in [false, true] {
        let (_db, pool) = setup_pg_db().await;
        seed_install_repo(&pool, 100, 10).await;
        let store = PostgresFleetStore::new(pool.clone());
        let (worker, facts, job, identity) = running(&store).await;
        sqlx::query("UPDATE worker_attempt SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE attempt_id=$1")
            .bind(identity.attempt_id).execute(&pool).await.unwrap();
        store
            .expire_stale_attempts()
            .await
            .unwrap();
        store
            .register_session(worker, &facts, Duration::seconds(90))
            .await
            .unwrap();
        let obligation = store
            .cleanup_obligations(worker, facts.worker_session_id)
            .await
            .unwrap()[0]
            .id;
        let generation = pause(&store, true).await;
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM job WHERE id=$1 FOR UPDATE")
            .bind(job)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let abort_store = store.clone();
        let abort = async move {
            abort_store
                .abort_fleet(&request(generation, FleetAbortScope::AllPending))
                .await
                .unwrap()
        };
        let cleanup_store = store.clone();
        let cleanup = async move {
            cleanup_store
                .complete_cleanup(worker, facts.worker_session_id, obligation)
                .await
                .unwrap()
        };
        let (abort_task, cleanup_task) = if abort_first {
            let abort_task = tokio::spawn(abort);
            wait_for_lock(&pool, "SELECT status::text FROM job WHERE id = $1 FOR UPDATE", 1).await;
            let cleanup_task = tokio::spawn(cleanup);
            wait_for_lock(&pool, "%UPDATE worker_cleanup_obligation%SET status = 'completed'%", 1)
                .await;
            (abort_task, cleanup_task)
        } else {
            let cleanup_task = tokio::spawn(cleanup);
            wait_for_lock(&pool, "%UPDATE job%WHERE id = $1 AND status = 'running'%", 1).await;
            let abort_task = tokio::spawn(abort);
            wait_for_lock(&pool, "%SELECT id FROM worker_cleanup_obligation%FOR UPDATE%", 1).await;
            (abort_task, cleanup_task)
        };
        tx.commit().await.unwrap();
        std::assert_matches!(abort_task.await.unwrap(), FleetAbortOutcome::Applied(receipt) if receipt.accepted_job_ids == vec![job]);
        assert!(cleanup_task.await.unwrap());
        assert_eq!(status(&pool, job).await, "cancelled");
    }
}
