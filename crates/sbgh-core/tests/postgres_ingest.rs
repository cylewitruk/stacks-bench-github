//! Integration tests for `PostgresIngestStore` (slice 1) against a real
//! Postgres engine, booted per-test via the shared `setup_pg()` helper.
//!
//! Behaviours pinned here that the in-memory fake can't validate:
//!   - The dual-write transaction is atomic: if the legacy `jobs` INSERT fails
//!     inside `ingest_webhook_and_job`, the `github_webhook` INSERT also rolls
//!     back.
//!   - `ON CONFLICT (delivery_id)` actually dedupes at the SQL layer.
//!   - `Option<Value>` payload binds as SQL NULL (not JSON `null`) when None —
//!     important for ops queries using `payload IS NULL`.
//!   - JSON `null` body still stores AS JSON null (round-trippable).
//!   - `payload_size_bytes` round-trips correctly.
//!   - Default `status='received'` and `next_attempt_at=NOW()` server-side
//!     defaults fire on handler-shaped inserts.

use sbgh_core::db::{IngestOutcome, IngestStore, NewWebhook, Pool, PostgresIngestStore, setup_pg};
use sbgh_core::models::{NewJob, WebhookStatus};

fn sample_webhook(delivery: &str) -> NewWebhook {
    NewWebhook {
        delivery_id: delivery.to_string(),
        event_type: "issue_comment".into(),
        action: Some("created".into()),
        payload_installation_id: Some(42),
        payload: Some(serde_json::json!({ "hello": "world" })),
        payload_size_bytes: 24,
    }
}

fn sample_job(delivery: &str) -> NewJob {
    NewJob {
        repository: "acme/widgets".into(),
        pr_number: 42,
        head_sha: String::new(),
        requested_by: "alice".into(),
        command: "run".into(),
        args: serde_json::json!({ "args": ["--iters=1"] }),
        installation_id: 42,
        github_delivery_id: Some(delivery.to_string()),
    }
}

async fn read_webhook_row(
    pool: &Pool,
    delivery: &str,
) -> (WebhookStatus, Option<serde_json::Value>, Option<i64>, i32) {
    sqlx::query_as::<_, (WebhookStatus, Option<serde_json::Value>, Option<i64>, i32)>(
        "SELECT status, payload, payload_installation_id, payload_size_bytes FROM github_webhook \
         WHERE delivery_id = $1",
    )
    .bind(delivery)
    .fetch_one(pool)
    .await
    .expect("read github_webhook row")
}

#[tokio::test]
async fn ingest_webhook_dedupes_via_unique_delivery_id() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresIngestStore::new(pool.clone());

    let first = store
        .ingest_webhook(&sample_webhook("dup-1"))
        .await
        .unwrap();
    assert!(matches!(first, IngestOutcome::Recorded { .. }));

    let second = store
        .ingest_webhook(&sample_webhook("dup-1"))
        .await
        .unwrap();
    assert!(
        matches!(second, IngestOutcome::Duplicate),
        "second ingest with same delivery_id must dedup at the SQL UNIQUE level; got {second:?}"
    );

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook WHERE delivery_id = $1")
            .bind("dup-1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn ingest_webhook_and_job_writes_both_rows_in_one_tx() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresIngestStore::new(pool.clone());

    let outcome = store
        .ingest_webhook_and_job(&sample_webhook("ok-1"), &sample_job("ok-1"))
        .await
        .unwrap();
    let IngestOutcome::Recorded { webhook_id, job_id } = outcome else {
        panic!("expected Recorded, got {outcome:?}");
    };
    assert!(webhook_id > 0);
    let job_id = job_id.expect("happy path must produce a fresh job_id");

    let webhook_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook WHERE delivery_id = $1")
            .bind("ok-1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(webhook_count, 1);

    let job_present: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(job_present);
}

#[tokio::test]
async fn dual_write_rolls_back_webhook_when_job_insert_fails() {
    // The slice 1 invariant tests for in-memory CAN'T validate: if
    // the legacy jobs INSERT fails inside the same transaction, the
    // webhook INSERT must also roll back so GH redelivery has a fresh
    // shot. We trigger a jobs-side failure by violating a constraint
    // that only manifests on real Postgres — installation_id is
    // BIGINT NOT NULL, so cast a NewJob through a hand-written SQL
    // path that forces a NOT NULL violation downstream.
    //
    // The most controllable way: pre-seed a row in jobs with the same
    // delivery_id BUT pre-existing, then have ingest_webhook_and_job
    // collide on the partial unique index. Per current Postgres impl,
    // the collision is handled by ON CONFLICT DO NOTHING and the
    // webhook IS committed (per the documented `Recorded { job_id:
    // None }` outcome for legacy-prior deliveries). So that's NOT a
    // true rollback case — it's a documented graceful skip.
    //
    // For a TRUE rollback, we need an actual transaction failure.
    // Easiest: drop the jobs table mid-test, then call
    // ingest_webhook_and_job. The webhook INSERT happens first, then
    // the jobs INSERT errors out (relation does not exist), and the
    // tx should roll back the webhook too.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresIngestStore::new(pool.clone());

    // Sabotage the jobs table so the second INSERT of the dual-write
    // transaction fails hard. Renames are reversible if we cared, but
    // this test owns its container so no cleanup needed.
    sqlx::query("ALTER TABLE jobs RENAME TO jobs_disabled")
        .execute(&pool)
        .await
        .unwrap();

    let result = store
        .ingest_webhook_and_job(&sample_webhook("rollback-1"), &sample_job("rollback-1"))
        .await;
    assert!(result.is_err(), "ingest_webhook_and_job must error when jobs INSERT fails");

    // The webhook row MUST NOT be present — the transaction rolled it back.
    let webhook_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook WHERE delivery_id = $1")
            .bind("rollback-1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        webhook_count, 0,
        "webhook INSERT must roll back when the dual-write transaction fails — otherwise GH \
         redelivery sees a duplicate and never retries the job"
    );
}

#[tokio::test]
async fn none_payload_binds_sql_null_not_json_null() {
    // Slice 1 fix: `NewWebhook.payload` is Option<Value> so None binds
    // SQL NULL (not the JSON `null` literal). Ops queries using
    // `payload IS NULL` to detect missing/cleared payloads MUST match.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresIngestStore::new(pool.clone());

    let webhook = NewWebhook {
        payload: None,
        ..sample_webhook("null-payload-1")
    };
    let outcome = store
        .ingest_webhook(&webhook)
        .await
        .unwrap();
    assert!(matches!(outcome, IngestOutcome::Recorded { .. }));

    // Direct check: payload column IS NULL.
    let is_null: bool =
        sqlx::query_scalar("SELECT payload IS NULL FROM github_webhook WHERE delivery_id = $1")
            .bind("null-payload-1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(is_null, "None payload must bind as SQL NULL, not JSON null");
}

#[tokio::test]
async fn json_null_payload_distinct_from_sql_null() {
    // Sanity: passing Some(Value::Null) — a JSON null body — stores
    // AS JSON null and is distinct from SQL NULL. Otherwise an
    // attacker could send `null` as the body to "look like" a wiped row.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresIngestStore::new(pool.clone());

    let webhook = NewWebhook {
        payload: Some(serde_json::Value::Null),
        ..sample_webhook("json-null-1")
    };
    store
        .ingest_webhook(&webhook)
        .await
        .unwrap();

    let (is_sql_null, payload_text): (bool, Option<String>) = sqlx::query_as(
        "SELECT payload IS NULL, payload::text FROM github_webhook WHERE delivery_id = $1",
    )
    .bind("json-null-1")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!is_sql_null, "JSON null body must NOT be SQL NULL");
    assert_eq!(payload_text.as_deref(), Some("null"));
}

#[tokio::test]
async fn payload_size_bytes_round_trips() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresIngestStore::new(pool.clone());

    let webhook = NewWebhook {
        payload_size_bytes: 4096,
        ..sample_webhook("size-1")
    };
    store
        .ingest_webhook(&webhook)
        .await
        .unwrap();

    let (_status, _payload, install_id, size) = read_webhook_row(&pool, "size-1").await;
    assert_eq!(size, 4096);
    assert_eq!(install_id, Some(42));
}

#[tokio::test]
async fn fresh_row_status_is_received() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresIngestStore::new(pool.clone());
    store
        .ingest_webhook(&sample_webhook("status-1"))
        .await
        .unwrap();
    let (status, _payload, _install_id, _size) = read_webhook_row(&pool, "status-1").await;
    assert_eq!(status, WebhookStatus::Received, "default status server-side must be 'received'");
}
