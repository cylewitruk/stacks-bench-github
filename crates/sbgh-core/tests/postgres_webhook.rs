//! Integration tests for `PostgresWebhookInbox` (slices 2a + 2b) against
//! a real Postgres engine. The behaviors pinned here can't be validated
//! by the in-memory `InMemoryWebhookInbox` because they exercise actual
//! Postgres SQL semantics:
//!
//!   - `FOR UPDATE SKIP LOCKED` returns disjoint rows under concurrent claimers
//!     (the in-memory test uses a Mutex which makes this trivially true; here
//!     we prove it under real lock semantics).
//!   - The `event_type = ANY($N)` filter actually leaves non-matching rows in
//!     `received`.
//!   - `claim_token`-guarded conditional updates are no-ops when the token
//!     doesn't match (sweep raced ahead).
//!   - `sweep_stuck_claims` correctly parameterizes the lease via
//!     `make_interval(secs => $1)` and recovers stuck rows.

use std::sync::Arc;

use sbgh_core::db::{
    IngestStore, NewWebhook, Pool, PostgresIngestStore, PostgresWebhookInbox, WebhookInbox,
    setup_pg,
};
use sbgh_core::models::{WebhookOutcome, WebhookStatus};
use uuid::Uuid;

/// Helper: seed a webhook row via `IngestStore` (the production
/// insertion path) so tests exercise the real handler-side write
/// shape, not synthetic SQL.
async fn seed_webhook(pool: &Pool, delivery: &str, event_type: &str) {
    let store = PostgresIngestStore::new(pool.clone());
    let webhook = NewWebhook {
        delivery_id: delivery.into(),
        event_type: event_type.into(),
        action: Some("created".into()),
        payload_installation_id: Some(42),
        payload: Some(serde_json::json!({})),
        payload_size_bytes: 2,
    };
    store
        .ingest_webhook(&webhook)
        .await
        .expect("seed webhook");
}

async fn read_row_status(pool: &Pool, delivery: &str) -> (WebhookStatus, Option<WebhookOutcome>) {
    sqlx::query_as("SELECT status, outcome FROM github_webhook WHERE delivery_id = $1")
        .bind(delivery)
        .fetch_one(pool)
        .await
        .expect("read row")
}

#[tokio::test]
async fn claim_next_returns_none_when_empty() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let inbox = PostgresWebhookInbox::new(pool);
    let claimed = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap();
    assert!(claimed.is_none());
}

#[tokio::test]
async fn claim_next_filters_by_event_type() {
    // Slice 2b invariant: rows for event types not in the filter
    // STAY in `received` for a future-slice processor to consume.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "issue-1", "issue_comment").await;
    seed_webhook(&pool, "install-1", "installation").await;

    let inbox = PostgresWebhookInbox::new(pool.clone());
    let claimed = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap()
        .expect("issue_comment row is claimable");
    assert_eq!(claimed.delivery_id, "issue-1");

    // Second claim with same filter finds nothing — the installation
    // row is NOT eligible.
    let none = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap();
    assert!(none.is_none());

    // installation row STILL `received`, not terminalized.
    let (status, outcome) = read_row_status(&pool, "install-1").await;
    assert_eq!(
        status,
        WebhookStatus::Received,
        "filter must leave non-matching event types in `received` for future-slice processors"
    );
    assert!(outcome.is_none());
}

#[tokio::test]
async fn claim_next_with_empty_filter_returns_none() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "any-1", "issue_comment").await;
    let inbox = PostgresWebhookInbox::new(pool);
    let claimed = inbox
        .claim_next(&[])
        .await
        .unwrap();
    assert!(claimed.is_none(), "empty filter must claim nothing");
}

#[tokio::test]
async fn concurrent_claims_pick_disjoint_rows() {
    // Real `FOR UPDATE SKIP LOCKED` semantics. Two concurrent claimants
    // on a pool of 2 rows must end up with distinct rows. With a plain
    // SELECT ... LIMIT 1 the second would block then re-read the same
    // row, double-claiming.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "a", "issue_comment").await;
    seed_webhook(&pool, "b", "issue_comment").await;

    let inbox = Arc::new(PostgresWebhookInbox::new(pool));
    let i1 = inbox.clone();
    let i2 = inbox.clone();
    let (r1, r2) = tokio::join!(
        async move {
            i1.claim_next(&["issue_comment"])
                .await
                .unwrap()
        },
        async move {
            i2.claim_next(&["issue_comment"])
                .await
                .unwrap()
        },
    );

    let a = r1.expect("first claimer must get a row");
    let b = r2.expect("second claimer must get a row");
    assert_ne!(
        a.delivery_id, b.delivery_id,
        "FOR UPDATE SKIP LOCKED must hand out distinct rows under concurrent claim"
    );
    let claimed: std::collections::HashSet<_> = [a.delivery_id, b.delivery_id]
        .into_iter()
        .collect();
    let expected: std::collections::HashSet<_> = ["a".to_string(), "b".to_string()]
        .into_iter()
        .collect();
    assert_eq!(claimed, expected);
}

#[tokio::test]
async fn complete_transitions_row_to_terminal_status() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "term-1", "issue_comment").await;
    let inbox = PostgresWebhookInbox::new(pool.clone());

    let claimed = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap()
        .unwrap();
    inbox
        .complete(claimed.id, claimed.claim_token, WebhookOutcome::IgnoredNoCommand)
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "term-1").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredNoCommand));
}

#[tokio::test]
async fn complete_with_stale_claim_token_is_noop() {
    // The slice 2a stale-claim invariant pinned against real Postgres:
    // if the sweeper has reset the row mid-flight and re-claimed it
    // under a new token, the original processor's late `complete` MUST
    // be a no-op. With the conditional WHERE clause on claim_token,
    // it should leave the row's new state untouched.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "stale-1", "issue_comment").await;
    let inbox = PostgresWebhookInbox::new(pool.clone());

    let claimed = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap()
        .unwrap();

    // Sweep raced ahead: force the row back to `received` with a
    // different token (or NULL). We simulate this directly via SQL.
    sqlx::query(
        "UPDATE github_webhook SET status = 'received', claim_token = NULL, claimed_at = NULL \
         WHERE id = $1",
    )
    .bind(claimed.id)
    .execute(&pool)
    .await
    .unwrap();

    // Stale processor's late complete: must NOT transition the row.
    inbox
        .complete(claimed.id, claimed.claim_token, WebhookOutcome::IgnoredAction)
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "stale-1").await;
    assert_eq!(
        status,
        WebhookStatus::Received,
        "stale claim_token write MUST be a no-op (sweep already reset the row)"
    );
    assert!(outcome.is_none(), "stale write MUST NOT set an outcome");
}

#[tokio::test]
async fn sweep_recovers_stuck_processing_rows() {
    // Pin the make_interval(secs => $1) shape against real Postgres.
    // Seed a row, force it into `processing` with an old claimed_at,
    // call sweep with a lease shorter than the age — must recover.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "stuck-1", "issue_comment").await;
    let inbox = PostgresWebhookInbox::new(pool.clone());

    // Make the row look stuck: status=processing, claimed_at 60 seconds ago.
    sqlx::query(
        "UPDATE github_webhook SET status = 'processing', claim_token = $1, claimed_at = NOW() - \
         INTERVAL '60 seconds' WHERE delivery_id = $2",
    )
    .bind(Uuid::new_v4())
    .bind("stuck-1")
    .execute(&pool)
    .await
    .unwrap();

    let recovered = inbox
        .sweep_stuck_claims(chrono::Duration::seconds(10))
        .await
        .unwrap();
    assert_eq!(recovered, 1);

    let (status, _) = read_row_status(&pool, "stuck-1").await;
    assert_eq!(
        status,
        WebhookStatus::RetryableError,
        "sweep must reset stuck-processing rows to retryable_error"
    );
}

#[tokio::test]
async fn sweep_leaves_fresh_processing_rows_alone() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "fresh-1", "issue_comment").await;
    let inbox = PostgresWebhookInbox::new(pool.clone());

    let claimed = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap()
        .unwrap();
    assert!(claimed.delivery_id == "fresh-1");

    // Sweep with a long lease should not touch a just-claimed row.
    let recovered = inbox
        .sweep_stuck_claims(chrono::Duration::minutes(10))
        .await
        .unwrap();
    assert_eq!(recovered, 0);

    let (status, _) = read_row_status(&pool, "fresh-1").await;
    assert_eq!(status, WebhookStatus::Processing);
}

#[tokio::test]
async fn permanent_failure_increments_attempts_in_db() {
    // Pin the Postgres-side `attempts = attempts + 1` from
    // record_permanent_failure (fixed mid-slice 2a per Codex review).
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "perm-1", "issue_comment").await;
    let inbox = PostgresWebhookInbox::new(pool.clone());

    let claimed = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap()
        .unwrap();
    inbox
        .record_permanent_failure(claimed.id, claimed.claim_token, "fatal")
        .await
        .unwrap();

    let (attempts, status, outcome): (i32, WebhookStatus, Option<WebhookOutcome>) = sqlx::query_as(
        "SELECT attempts, status, outcome FROM github_webhook WHERE delivery_id = $1",
    )
    .bind("perm-1")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(attempts, 1, "permanent failure must increment attempts");
    assert_eq!(status, WebhookStatus::Failed);
    assert_eq!(outcome, Some(WebhookOutcome::Error));
}

#[tokio::test]
async fn complete_clears_last_error() {
    // Pin the slice 2a fix: a row that transient-errored once and
    // then succeeded must have last_error cleared.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_webhook(&pool, "clear-1", "issue_comment").await;
    let inbox = PostgresWebhookInbox::new(pool.clone());

    // First attempt: record transient error.
    let claimed = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap()
        .unwrap();
    let next = chrono::Utc::now() - chrono::Duration::seconds(1);
    inbox
        .record_retryable_error(claimed.id, claimed.claim_token, "transient", next)
        .await
        .unwrap();

    // Second attempt: succeed.
    let claimed2 = inbox
        .claim_next(&["issue_comment"])
        .await
        .unwrap()
        .unwrap();
    inbox
        .complete(claimed2.id, claimed2.claim_token, WebhookOutcome::IgnoredAction)
        .await
        .unwrap();

    let last_error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM github_webhook WHERE delivery_id = $1")
            .bind("clear-1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        last_error.is_none(),
        "complete must clear last_error from prior retries; got {last_error:?}"
    );
}
