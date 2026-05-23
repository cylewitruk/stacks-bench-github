//! Integration tests for `PostgresJobStore` against a real Postgres engine,
//! booted per-test via `testcontainers`.
//!
//! These tests run by default but skip gracefully when Docker isn't reachable
//! (CI without a daemon, sandboxed laptops, etc.) — they print a "skipping"
//! line and pass instead of failing the whole suite.
//!
//! Behaviours pinned here that an in-memory fake can't validate:
//!   - The partial unique index on `github_delivery_id` plus the matching `ON
//!     CONFLICT (...) WHERE ...` predicate (i.e. deduplication actually happens
//!     at the SQL layer, not just in the fake).
//!   - Multiple NULL delivery ids are legal (partial index semantics).
//!   - `SELECT ... FOR UPDATE SKIP LOCKED` makes `claim_next` safe under
//!     concurrent claimants.
//!   - End-to-end column round-trip through `FromRow` for `Job`.

use std::sync::Arc;

use sbgh_core::db::{self, JobStore, Pool, PostgresJobStore};
use sbgh_core::models::{JobStatus, NewJob};
use sqlx::types::Json;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

/// Read the columns that terminal-transition operations write — used by tests
/// that need to assert *what* `complete` / `fail` actually stored, beyond just
/// "the job left the queue".
async fn read_terminal_state(
    pool: &Pool,
    id: Uuid,
) -> (JobStatus, Option<String>, Option<Json<serde_json::Value>>) {
    sqlx::query_as::<_, (JobStatus, Option<String>, Option<Json<serde_json::Value>>)>(
        "SELECT status, error, result FROM jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read jobs row")
}

/// Start a fresh Postgres container, connect, run migrations.
///
/// Returns `None` (with a printed notice) **only** when the container itself
/// fails to start — that's the "no Docker daemon" case we want to skip on.
/// Once the container is up, everything else (port lookup, pool connect,
/// migrations) MUST succeed — those failures indicate a real regression and
/// we let them panic so the test reports as failed, not skipped.
async fn setup() -> Option<(ContainerAsync<Postgres>, Pool)> {
    // - `with_tag("18-trixie")`: the module defaults to `postgres:11-alpine`, but
    //   our `gen_random_uuid()` default on `jobs.id` needs PG 13+. We pin the same
    //   version as the prod docker-compose so tests catch any behaviour that
    //   differs from what we deploy.
    // - `with_mapped_port(0, Tcp(5432))`: the Postgres image doesn't override
    //   `Image::expose_ports`, so without an explicit mapping `get_host_port_ipv4`
    //   returns `PortNotExposed`. `host_port = 0` asks Docker for a free port so
    //   parallel tests don't collide.
    let container = match Postgres::default()
        .with_tag("18-trixie")
        .with_mapped_port(0, ContainerPort::Tcp(5432))
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: failed to start postgres container ({e}); Docker not reachable?");
            return None;
        }
    };
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres container started but host port unavailable");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = db::connect(&url)
        .await
        .expect("connect to ephemeral postgres failed");
    db::migrate(&pool)
        .await
        .expect("migrations failed against ephemeral postgres");
    Some((container, pool))
}

fn new_job(delivery: Option<&str>) -> NewJob {
    NewJob {
        repository: "acme/widgets".into(),
        pr_number: 42,
        head_sha: "abc123".into(),
        requested_by: "alice".into(),
        command: "run".into(),
        args: serde_json::json!({ "args": ["--iters=1"] }),
        installation_id: 7,
        github_delivery_id: delivery.map(str::to_string),
    }
}

#[tokio::test]
async fn duplicate_delivery_id_is_deduped_via_partial_index() {
    // Regression test for two related fixes:
    //   1. the partial unique index added in
    //      migrations/20260522000001_jobs_github_delivery_id.sql
    //   2. the matching `ON CONFLICT (github_delivery_id) WHERE github_delivery_id
    //      IS NOT NULL DO NOTHING` predicate in PostgresJobStore::enqueue — without
    //      the predicate Postgres raises "no unique or exclusion constraint
    //      matching".
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool);
    let first = store
        .enqueue(&new_job(Some("dup-key")))
        .await
        .unwrap();
    let second = store
        .enqueue(&new_job(Some("dup-key")))
        .await
        .unwrap();
    assert!(first.is_some(), "first enqueue should succeed");
    assert!(
        second.is_none(),
        "second enqueue with same delivery id must be deduped (got {second:?})"
    );
}

#[tokio::test]
async fn null_delivery_id_does_not_collide() {
    // The unique index is partial (WHERE github_delivery_id IS NOT NULL),
    // so multiple rows with NULL delivery ids must all succeed.
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool);
    for _ in 0..3 {
        let result = store
            .enqueue(&new_job(None))
            .await
            .unwrap();
        assert!(result.is_some(), "NULL delivery id should never dedup");
    }
}

#[tokio::test]
async fn claim_next_flips_status_and_sets_started_at() {
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool);
    let id = store
        .enqueue(&new_job(Some("c1")))
        .await
        .unwrap()
        .unwrap();

    let claimed = store
        .claim_next()
        .await
        .unwrap()
        .expect("a job to claim");
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.status, JobStatus::Running);
    assert!(claimed.started_at.is_some());

    // No more queued jobs → next claim is None.
    assert!(
        store
            .claim_next()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn concurrent_claims_pick_different_jobs() {
    // Two jobs queued. Two concurrent `claim_next` tasks (each with its own
    // pool connection, so they hold separate transactions) must end up with
    // distinct job ids — this is `SELECT ... FOR UPDATE SKIP LOCKED` doing
    // its job. With a plain `SELECT ... LIMIT 1` the second would block
    // and then re-read the same row, deadlocking or double-claiming.
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = Arc::new(PostgresJobStore::new(pool));

    let id_a = store
        .enqueue(&new_job(Some("c-a")))
        .await
        .unwrap()
        .unwrap();
    let id_b = store
        .enqueue(&new_job(Some("c-b")))
        .await
        .unwrap()
        .unwrap();

    let s1 = store.clone();
    let s2 = store.clone();
    let (r1, r2) = tokio::join!(async move { s1.claim_next().await.unwrap() }, async move {
        s2.claim_next().await.unwrap()
    },);

    let claimed_a = r1.expect("first task should claim a job");
    let claimed_b = r2.expect("second task should claim a job");
    assert_ne!(claimed_a.id, claimed_b.id, "concurrent claimants must NOT both pick the same row");
    let claimed_set: std::collections::HashSet<_> = [claimed_a.id, claimed_b.id]
        .into_iter()
        .collect();
    let expected: std::collections::HashSet<_> = [id_a, id_b]
        .into_iter()
        .collect();
    assert_eq!(claimed_set, expected);
}

#[tokio::test]
async fn complete_writes_result_and_removes_from_queue() {
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool.clone());
    let id = store
        .enqueue(&new_job(Some("done1")))
        .await
        .unwrap()
        .unwrap();
    let claimed = store
        .claim_next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, id);

    let summary = serde_json::json!({ "ok": true, "metric": 42 });
    store
        .complete(id, summary.clone())
        .await
        .unwrap();

    // Direct read: status, error, and the stored result JSON must match.
    let (status, error, result) = read_terminal_state(&pool, id).await;
    assert_eq!(status, JobStatus::Completed);
    assert!(error.is_none(), "complete must not set error");
    let stored = result
        .expect("complete must write a result blob")
        .0;
    assert_eq!(stored, summary);

    // Queue-side: gone, nothing claimable.
    assert!(
        store
            .claim_next()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn fail_with_summary_stores_both_error_and_forensics() {
    // Verifies the COALESCE($3, result) behaviour added when JobStore::fail
    // grew the optional summary param.
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool.clone());
    let id = store
        .enqueue(&new_job(Some("fail1")))
        .await
        .unwrap()
        .unwrap();
    let _ = store
        .claim_next()
        .await
        .unwrap();

    let forensics = serde_json::json!({
        "finish_reason": "phase_error",
        "last_phase": "running",
        "console_tail": "panic at 0x...",
    });
    let err_msg = "VM reported phase=error";
    store
        .fail(id, err_msg, Some(forensics.clone()))
        .await
        .unwrap();

    // Direct read: status flipped, error message stored, forensics blob landed.
    let (status, error, result) = read_terminal_state(&pool, id).await;
    assert_eq!(status, JobStatus::Failed);
    assert_eq!(error.as_deref(), Some(err_msg));
    let stored = result
        .expect("fail-with-summary must write a result blob")
        .0;
    assert_eq!(stored, forensics);

    // Queue-side: gone.
    assert!(
        store
            .claim_next()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn fail_without_summary_leaves_null_result_null() {
    // Sanity case: nothing in `result` before, `fail(_, _, None)` doesn't
    // invent a value out of thin air.
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool.clone());
    let id = store
        .enqueue(&new_job(Some("fail-null-stays-null")))
        .await
        .unwrap()
        .unwrap();
    let _ = store
        .claim_next()
        .await
        .unwrap();

    store
        .fail(id, "setup error", None)
        .await
        .unwrap();

    let (status, error, result) = read_terminal_state(&pool, id).await;
    assert_eq!(status, JobStatus::Failed);
    assert_eq!(error.as_deref(), Some("setup error"));
    assert!(result.is_none(), "no summary passed → result stays NULL");
}

#[tokio::test]
async fn fail_without_summary_preserves_existing_result() {
    // The actual `COALESCE($3, result)` preservation branch: an earlier
    // writer (e.g. a partial forensics dump) put something in `result`, and
    // a later `fail(_, _, None)` must NOT clobber it. Without COALESCE, the
    // raw `result = $3` would write NULL and we'd silently lose the data.
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool.clone());
    let id = store
        .enqueue(&new_job(Some("fail-preserves-prior")))
        .await
        .unwrap()
        .unwrap();
    let _ = store
        .claim_next()
        .await
        .unwrap();

    // Pre-seed `result` directly so we don't rely on `complete()` (which
    // would also flip status to Completed and conflict with the later fail).
    let prior = serde_json::json!({ "partial": true, "captured_at": "early" });
    sqlx::query("UPDATE jobs SET result = $2 WHERE id = $1")
        .bind(id)
        .bind(Json(&prior))
        .execute(&pool)
        .await
        .expect("pre-seed result column");

    store
        .fail(id, "VM crashed", None)
        .await
        .unwrap();

    let (status, error, result) = read_terminal_state(&pool, id).await;
    assert_eq!(status, JobStatus::Failed);
    assert_eq!(error.as_deref(), Some("VM crashed"));
    let stored = result
        .expect("prior result must survive fail-with-None")
        .0;
    assert_eq!(stored, prior, "COALESCE must preserve pre-existing result");
}

#[tokio::test]
async fn set_comment_id_persists_through_subsequent_reads() {
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool);
    let id = store
        .enqueue(&new_job(Some("comment-id-1")))
        .await
        .unwrap()
        .unwrap();
    store
        .set_comment_id(id, 4242)
        .await
        .unwrap();

    // claim_next returns the full Job row; assert comment_id round-tripped
    // through FromRow.
    let claimed = store
        .claim_next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.comment_id, Some(4242));
    assert_eq!(
        claimed
            .github_delivery_id
            .as_deref(),
        Some("comment-id-1")
    );
}

#[tokio::test]
async fn set_head_sha_updates_the_column() {
    // Handler enqueues with head_sha = "" (it doesn't hold App credentials
    // and can't call the PR API). The orchestrator resolves on pickup and
    // writes it back via set_head_sha. Make sure that round-trips.
    let Some((_c, pool)) = setup().await else {
        return;
    };
    let store = PostgresJobStore::new(pool);
    let id = store
        .enqueue(&NewJob {
            head_sha: String::new(),
            ..new_job(Some("head-sha-1"))
        })
        .await
        .unwrap()
        .unwrap();

    store
        .set_head_sha(id, "deadbeefcafef00d")
        .await
        .unwrap();

    let claimed = store
        .claim_next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.head_sha, "deadbeefcafef00d");
}
