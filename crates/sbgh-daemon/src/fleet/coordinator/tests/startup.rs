//! Startup reconciliation with the running coordinator and database-time leases.

use std::future::{self, Future};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sbgh_core::db::fleet::{FleetStore, WorkerRegistration};
use sbgh_fleet::{DesiredState, TaskPayload, WorkerCapability};
use sbgh_github::test_support::FakeGitHub;
use sbgh_postgres::PostgresFleetStore;
use sbgh_postgres::db::{Pool, setup_pg_db};
use tempfile::TempDir;
use tokio::time;
use uuid::Uuid;

use super::{seed_active_attempt, test_coordinator};
use crate::shutdown::Shutdown;

#[tokio::test]
async fn startup_grace_does_not_renew_an_expired_lease_and_expires_it_once() {
    reconcile_startup(false).await;
}

#[tokio::test]
async fn startup_grace_preserves_a_heartbeat_renewed_attempt() {
    reconcile_startup(true).await;
}

/// Observe the actual coordinator sweep while retaining PostgreSQL as the lease clock.
async fn reconcile_startup(renew: bool) {
    let (_db, pool) = setup_pg_db().await;
    let directory = TempDir::new().unwrap();
    let fleet = PostgresFleetStore::new(pool.clone());
    let worker = Uuid::new_v4();
    fleet
        .seed_worker(&WorkerRegistration {
            worker_id: worker,
            display_name: "startup-reconciliation".into(),
            allowed_capabilities: vec![WorkerCapability::BuildOnly],
            measurement_profile: None,
            enabled: true,
            draining: false,
        })
        .await
        .unwrap();
    let offer = seed_active_attempt(&pool, &fleet, worker, TaskPayload::BuildOnly).await;
    let mut coordinator = test_coordinator(&pool, &directory, Arc::new(FakeGitHub::new()));
    Arc::get_mut(&mut coordinator.config)
        .unwrap()
        .fleet
        .heartbeat_seconds = 3;
    let grace = Duration::from_secs(6);

    let original_deadline: DateTime<Utc> = sqlx::query_scalar(
        "UPDATE worker_attempt SET lease_expires_at = clock_timestamp() + make_interval(secs => $2)
         WHERE attempt_id = $1 RETURNING lease_expires_at",
    )
    .bind(offer.identity.attempt_id)
    .bind(if renew { 3.0_f64 } else { -1.0_f64 })
    .fetch_one(&pool)
    .await
    .unwrap();
    let shutdown = Shutdown::new();
    // Disable intake only; projection and expiry must keep running.
    shutdown.draining.cancel();
    let _exit_on_failure = shutdown
        .exit
        .clone()
        .drop_guard();
    let mut runner = Box::pin(coordinator.run(shutdown.clone()));
    let started = time::Instant::now();
    future::poll_fn(|context| {
        assert!(
            runner
                .as_mut()
                .poll(context)
                .is_pending()
        );
        Poll::Ready(())
    })
    .await;
    let task = tokio::spawn(runner);

    let heartbeat = fleet
        .heartbeat_attempt(worker, &offer.identity, chrono::Duration::seconds(45), None)
        .await
        .unwrap();
    assert!(started.elapsed() < grace, "heartbeat must be observed during startup grace");
    let expected_deadline = if renew {
        let heartbeat = heartbeat.expect("a still-live lease may be renewed during grace");
        assert_eq!(heartbeat.desired_state, DesiredState::Continue);
        assert!(heartbeat.lease_expires_at > original_deadline);
        heartbeat.lease_expires_at
    } else {
        assert!(heartbeat.is_none(), "startup grace must not resurrect an expired lease");
        original_deadline
    };
    let before_sweep: (String, DateTime<Utc>, i64) = sqlx::query_as(
        "SELECT status::text, lease_expires_at,
            (SELECT COUNT(*) FROM worker_cleanup_obligation WHERE attempt_id = $1)
         FROM worker_attempt WHERE attempt_id = $1",
    )
    .bind(offer.identity.attempt_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(started.elapsed() < grace, "pre-sweep observation must precede startup grace");
    assert_eq!(before_sweep, ("running".into(), expected_deadline, 0));

    if renew {
        time::sleep_until(started + grace + Duration::from_millis(250)).await;
        let (status, deadline): (String, DateTime<Utc>) = sqlx::query_as(
            "SELECT status::text, lease_expires_at FROM worker_attempt WHERE attempt_id = $1",
        )
        .bind(offer.identity.attempt_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "running");
        assert_eq!(deadline, expected_deadline);
        let old_deadline_passed: bool = sqlx::query_scalar("SELECT clock_timestamp() > $1")
            .bind(original_deadline)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            old_deadline_passed,
            "the renewed lease, not the original lease, preserves authority"
        );
        sqlx::query("UPDATE worker_attempt SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE attempt_id = $1")
            .bind(offer.identity.attempt_id).execute(&pool).await.unwrap();
    }
    wait_for_expiry(&pool, offer.identity.attempt_id).await;
    assert!(started.elapsed() >= grace, "expiry sweep must respect startup grace");
    shutdown.exit.cancel();
    time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        fleet
            .expire_stale_attempts()
            .await
            .unwrap(),
        0,
        "repeat sweep cannot expire twice"
    );
    assert!(
        fleet
            .heartbeat_attempt(worker, &offer.identity, chrono::Duration::seconds(45), None)
            .await
            .unwrap()
            .is_none()
    );

    let cleanup: (i64, bool, bool) = sqlx::query_as(
        "SELECT COUNT(*), bool_and(status = 'pending'), bool_and(requeue_job)
         FROM worker_cleanup_obligation WHERE attempt_id = $1",
    )
    .bind(offer.identity.attempt_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cleanup, (1, true, true));
    let job_status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(offer.job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        job_status, "running",
        "expiry must not requeue before physical cleanup acknowledgement"
    );
    let policy: (bool, bool) =
        sqlx::query_as("SELECT enabled, draining FROM worker_registry WHERE worker_id = $1")
            .bind(worker)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(policy, (true, false));
}

/// Bound a failed sweep test without changing database time or calling expiry directly.
async fn wait_for_expiry(pool: &Pool, attempt: Uuid) {
    time::timeout(Duration::from_secs(12), async {
        loop {
            let expired: bool = sqlx::query_scalar(
                "SELECT status = 'expired' FROM worker_attempt WHERE attempt_id = $1",
            )
            .bind(attempt)
            .fetch_one(pool)
            .await
            .unwrap();
            if expired {
                return;
            }
            time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("coordinator must sweep the expired lease within the test bound");
}
