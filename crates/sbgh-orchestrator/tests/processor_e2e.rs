//! End-to-end integration test for the slice 1 + 2a + 2b seam:
//! handler-shaped `IngestStore::ingest_webhook` insert → real
//! `WebhookProcessor::process_one` → terminal status/outcome in DB.
//!
//! This is the highest-confidence "the inbox pipeline actually works"
//! test we can write before slice 9's job creation lands. It exercises
//! the production Postgres impls (no in-memory stand-ins), the real
//! BasicClassifier, and the real WebhookProcessor — all wired together
//! against a fresh testcontainers Postgres.

use std::sync::Arc;

use sbgh_core::db::{
    IngestStore, NewWebhook, Pool, PostgresIngestStore, PostgresWebhookInbox, setup_pg,
};
use sbgh_core::models::{WebhookOutcome, WebhookStatus};

// Pull in the orchestrator's webhook_processor module via path include
// (orchestrator is a bin-only crate so its modules aren't normally
// reachable from tests). Same pattern the handler integration tests
// use for routes/mod.rs.
//
// allow(dead_code): the e2e tests exercise process_one + BasicClassifier
// but not run() or NoopClassifier; from this test binary's POV they're
// dead, but they're real production code in the main binary.
#[path = "../src/webhook_processor.rs"]
#[allow(dead_code)]
mod webhook_processor;

use webhook_processor::{BasicClassifier, ProcessorConfig, WebhookProcessor};

async fn read_row_status(pool: &Pool, delivery: &str) -> (WebhookStatus, Option<WebhookOutcome>) {
    sqlx::query_as("SELECT status, outcome FROM github_webhook WHERE delivery_id = $1")
        .bind(delivery)
        .fetch_one(pool)
        .await
        .expect("read row")
}

fn issue_comment_webhook(delivery: &str, body: &str, is_pr: bool) -> NewWebhook {
    let pull_request = if is_pr {
        serde_json::json!({ "url": "https://api.github.test/repos/o/r/pulls/1" })
    } else {
        serde_json::Value::Null
    };
    let payload = serde_json::json!({
        "action": "created",
        "comment": {
            "id": 1,
            "body": body,
            "user": { "login": "alice" },
            "author_association": "MEMBER",
        },
        "issue": {
            "number": 1,
            "pull_request": pull_request,
        },
        "repository": { "full_name": "o/r" },
        "sender": { "login": "alice" },
        "installation": { "id": 42 },
    });
    let size = serde_json::to_vec(&payload)
        .unwrap()
        .len() as i32;
    NewWebhook {
        delivery_id: delivery.into(),
        event_type: "issue_comment".into(),
        action: Some("created".into()),
        payload_installation_id: Some(42),
        payload: Some(payload),
        payload_size_bytes: size,
    }
}

#[tokio::test]
async fn pipeline_classifies_pr_no_command_as_ignored_no_command() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };

    // Insert via the production IngestStore — same path the handler uses.
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&issue_comment_webhook("e2e-1", "looks great", true))
        .await
        .unwrap();

    // Run the processor through one iteration.
    let inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
    let processor =
        WebhookProcessor::new(inbox, Arc::new(BasicClassifier), ProcessorConfig::default());
    assert!(
        processor
            .process_one()
            .await
            .unwrap(),
        "processor must claim and classify the seeded row"
    );

    // Verify terminal state in DB — no in-memory stand-ins.
    let (status, outcome) = read_row_status(&pool, "e2e-1").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredNoCommand));
}

#[tokio::test]
async fn pipeline_classifies_pr_benchmark_as_ignored_action_in_phase1() {
    // Slice 9 will change this to `enqueued_job` + create a `job` row.
    // Pinning the Phase 1 behavior here means the slice-9 assertion
    // change is the visible record of intent.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&issue_comment_webhook("e2e-bench", "/benchmark run", true))
        .await
        .unwrap();

    let inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
    let processor =
        WebhookProcessor::new(inbox, Arc::new(BasicClassifier), ProcessorConfig::default());
    processor
        .process_one()
        .await
        .unwrap();

    let (status, outcome) = read_row_status(&pool, "e2e-bench").await;
    assert_eq!(status, WebhookStatus::Ignored);
    assert_eq!(outcome, Some(WebhookOutcome::IgnoredAction));
}

#[tokio::test]
async fn pipeline_leaves_installation_events_in_received() {
    // Slice 2b high-finding-fix invariant, proven end-to-end against
    // real Postgres: an installation row stays `received` for slice 3
    // to consume.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let ingest = PostgresIngestStore::new(pool.clone());
    ingest
        .ingest_webhook(&NewWebhook {
            delivery_id: "e2e-install".into(),
            event_type: "installation".into(),
            action: Some("created".into()),
            payload_installation_id: Some(42),
            payload: Some(serde_json::json!({ "action": "created" })),
            payload_size_bytes: 24,
        })
        .await
        .unwrap();

    let inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
    let processor =
        WebhookProcessor::new(inbox, Arc::new(BasicClassifier), ProcessorConfig::default());
    // BasicClassifier doesn't support `installation`, so the processor
    // finds nothing claimable.
    assert!(
        !processor
            .process_one()
            .await
            .unwrap(),
        "processor must NOT claim the installation row in slice 2b"
    );

    let (status, outcome) = read_row_status(&pool, "e2e-install").await;
    assert_eq!(
        status,
        WebhookStatus::Received,
        "installation row must remain `received` for slice 3's processor"
    );
    assert!(outcome.is_none());
}

#[tokio::test]
async fn pipeline_processes_multiple_rows_in_a_loop() {
    // Sanity: process_one is called repeatedly until the inbox is empty.
    // Mimics the run() loop without the indefinite blocking.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let ingest = PostgresIngestStore::new(pool.clone());
    for i in 0..3 {
        ingest
            .ingest_webhook(&issue_comment_webhook(&format!("loop-{i}"), "nothing special", true))
            .await
            .unwrap();
    }

    let inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
    let processor =
        WebhookProcessor::new(inbox, Arc::new(BasicClassifier), ProcessorConfig::default());

    let mut processed = 0;
    while processor
        .process_one()
        .await
        .unwrap()
    {
        processed += 1;
    }
    assert_eq!(processed, 3);

    // All three should be terminal `ignored` / `ignored_no_command`.
    let terminal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM github_webhook WHERE delivery_id LIKE 'loop-%' AND status = \
         'ignored' AND outcome = 'ignored_no_command'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(terminal_count, 3);
}
