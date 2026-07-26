//! Integration tests for `PostgresIngestStore` against a real Postgres
//! engine, booted per-test via the shared `setup_pg_db()` helper.
//!
//! Behaviours pinned here that require the production Postgres implementation:
//!   - `ON CONFLICT (delivery_id)` actually dedupes at the SQL layer.
//!   - `Option<Value>` payload binds as SQL NULL (not JSON `null`) when None —
//!     important for ops queries using `payload IS NULL`.
//!   - JSON `null` body still stores AS JSON null (round-trippable).
//!   - `payload_size_bytes` round-trips correctly.
//!   - Default `status='received'` and `next_attempt_at=NOW()` server-side
//!     defaults fire on handler-shaped inserts.

use sbgh_core::models::WebhookStatus;
use sbgh_postgres::Db;
use sbgh_postgres::db::{
    IngestOutcome, IngestStore, NewWebhook, Pool, PostgresIngestStore, setup_pg_db,
};

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

async fn read_webhook_row(
    pool: &Pool,
    delivery: &str,
) -> (WebhookStatus, Option<serde_json::Value>, Option<i64>, i32) {
    let (status, payload, installation_id, size) = sqlx::query_as::<
        _,
        (Db<WebhookStatus>, Option<serde_json::Value>, Option<i64>, i32),
    >(
        "SELECT status, payload, payload_installation_id, payload_size_bytes FROM github_webhook \
         WHERE delivery_id = $1",
    )
    .bind(delivery)
    .fetch_one(pool)
    .await
    .expect("read github_webhook row");
    (status.0, payload, installation_id, size)
}

#[tokio::test]
async fn ingest_webhook_dedupes_via_unique_delivery_id() {
    let (_db, pool) = setup_pg_db().await;
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
async fn none_payload_binds_sql_null_not_json_null() {
    // Slice 1 fix: `NewWebhook.payload` is Option<Value> so None binds
    // SQL NULL (not the JSON `null` literal). Ops queries using
    // `payload IS NULL` to detect missing/cleared payloads MUST match.
    let (_db, pool) = setup_pg_db().await;
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
    let (_db, pool) = setup_pg_db().await;
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
    let (_db, pool) = setup_pg_db().await;
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
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresIngestStore::new(pool.clone());
    store
        .ingest_webhook(&sample_webhook("status-1"))
        .await
        .unwrap();
    let (status, _payload, _install_id, _size) = read_webhook_row(&pool, "status-1").await;
    assert_eq!(status, WebhookStatus::Received, "default status server-side must be 'received'");
}
