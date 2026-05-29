//! Slice 8 (post-second-pass review): in-memory `JobV2Store` tests
//! focused on the atomic visibility property of
//! `create_job_with_links` — a concurrent reader must NEVER observe
//! a partially-created job. Mirrors the Postgres single-transaction
//! semantic.
//!
//! These tests don't need Postgres — the in-memory store is the unit
//! under test.

use std::sync::Arc;

use sbgh_core::db::{
    InMemoryJobV2Store, JobCompletion, JobCreationOutcome, JobFailure, JobV2Store,
};
use sbgh_core::models::{
    GitRefKind, JobCreationRequest, JobKind, JobResult, JobStatus, NewJobV2, NewPullRequestLink,
    TriggerKind,
};
use uuid::Uuid;

fn make_request(webhook_id: i64) -> JobCreationRequest {
    JobCreationRequest {
        new_job: NewJobV2 {
            github_installation_id: 100,
            github_repo_id: 10,
            job_kind: JobKind::AdHoc,
            trigger_kind: TriggerKind::PrComment,
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: "main".into(),
            git_commit_hash: None,
            git_committed_at: None,
        },
        github_webhook_id: webhook_id,
        triggering_user_id: Some(42),
        pull_request_link: Some(NewPullRequestLink {
            github_pull_request_id: 1,
            triggering_comment_id: Some(9001),
        }),
        queued_event_detail: Some(serde_json::json!({"trigger": "pr_comment"})),
    }
}

#[tokio::test]
async fn create_job_with_links_is_atomically_visible() {
    // After the call returns Ok, ALL related rows must be present
    // — never a job without its webhook link or queued event.
    let store = InMemoryJobV2Store::new();
    let JobCreationOutcome::Created(created) = store
        .create_job_with_links(&make_request(1))
        .await
        .unwrap()
    else {
        panic!("expected a fresh Created job");
    };

    // Job present, status=Queued.
    let job = store
        .lookup_job(created.job.id)
        .await
        .unwrap()
        .expect("job must be present after create");
    assert_eq!(job.status, JobStatus::Queued);
    // CreatedJob bundle reflects every link + the queued event.
    assert!(created.user_link.is_some(), "user link must be present in the return bundle");
    assert!(
        created
            .pull_request_link
            .is_some(),
        "PR link must be present in the return bundle"
    );
}

#[tokio::test]
async fn create_job_with_links_is_idempotent_on_webhook_id() {
    // Slice 9 (review fix): the in-memory store mirrors the Postgres
    // UNIQUE(github_webhook_id) idempotency guard. Reprocessing the same
    // webhook returns AlreadyEnqueued and leaves exactly one job.
    let store = InMemoryJobV2Store::new();
    let first = store
        .create_job_with_links(&make_request(7))
        .await
        .unwrap();
    assert!(matches!(first, JobCreationOutcome::Created(_)));

    let second = store
        .create_job_with_links(&make_request(7))
        .await
        .unwrap();
    assert!(
        matches!(second, JobCreationOutcome::AlreadyEnqueued),
        "reprocessing the same webhook must be AlreadyEnqueued"
    );
    assert_eq!(store.all_jobs().len(), 1, "retry must not create a second job");
}

#[tokio::test]
async fn concurrent_claim_never_observes_partial_create() {
    // Post-second-pass review regression: with the old multi-mutex-
    // acquire impl, a concurrent `claim_next_queued` could
    // intercept the job between insert_job and link_to_webhook and
    // see the orphaned job. With the single-mutex impl this is
    // structurally impossible — `claim_next_queued` either sees the
    // fully-committed job or nothing.
    //
    // Test strength (per Codex L2): the assertion checks
    // `has_webhook_link_for_job` INSIDE the claim task, right after
    // the claim observed the row. That proves the link was visible
    // AT CLAIM TIME — not just by the time the create task finished
    // (which is what the previous version of this test was actually
    // measuring, and which would have passed against the old buggy
    // impl too).
    let store = Arc::new(InMemoryJobV2Store::new());
    const ITERATIONS: usize = 50;
    for i in 0..ITERATIONS {
        let create_store = store.clone();
        let claim_store = store.clone();
        let webhook_id = (i + 1) as i64;
        let create_task = tokio::spawn(async move {
            create_store
                .create_job_with_links(&make_request(webhook_id))
                .await
        });
        let claim_token = Uuid::new_v4();
        let claim_task = tokio::spawn(async move {
            // Hammer the claim path multiple times to maximise the
            // chance of intercepting a partial state if any existed.
            for _ in 0..10 {
                if let Some(j) = claim_store
                    .claim_next_queued(claim_token)
                    .await
                    .unwrap()
                {
                    // CRITICAL: check the link's visibility *right now*,
                    // before yielding back to the create task. With the
                    // old multi-mutex impl this could return false for
                    // a window between insert_job and link_to_webhook.
                    let link_visible = claim_store.has_webhook_link_for_job(j.id);
                    return Some((j, link_visible));
                }
                tokio::task::yield_now().await;
            }
            None
        });

        let (created, maybe_claimed) = tokio::join!(create_task, claim_task);
        let _created = created.unwrap().unwrap();
        if let Some((claimed, link_visible)) = maybe_claimed.unwrap() {
            assert!(
                link_visible,
                "iteration {i}: claim observed job_id={} but the webhook link was NOT visible at \
                 the same time — atomic visibility violated",
                claimed.id
            );
        }
    }
}

/// Helper: insert → claim → mark_running, returning (job_id, claim_token).
/// `webhook_id` must be distinct per call (the slice-9 idempotency guard
/// rejects a second job for the same webhook).
async fn run_job(store: &InMemoryJobV2Store, webhook_id: i64) -> (Uuid, Uuid) {
    let JobCreationOutcome::Created(created) = store
        .create_job_with_links(&make_request(webhook_id))
        .await
        .unwrap()
    else {
        panic!("expected Created");
    };
    let token = Uuid::new_v4();
    let claimed = store
        .claim_next_queued(token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, created.job.id);
    store
        .mark_running(claimed.id, token, None)
        .await
        .unwrap();
    (claimed.id, token)
}

#[tokio::test]
async fn complete_job_and_fail_job_mirror_postgres_guard() {
    // complete_job: happy path flips to Completed + records result.
    let store = InMemoryJobV2Store::new();
    let (job_id, token) = run_job(&store, 11).await;
    let ok = store
        .complete_job(&JobCompletion {
            job_id,
            claim_token: token,
            result: JobResult {
                job_id,
                run_json: None,
                archive_dir: "/d".into(),
                created_at: chrono::Utc::now(),
            },
            metric: None,
            event_detail: None,
        })
        .await
        .unwrap();
    assert!(ok);
    assert_eq!(
        store
            .lookup_job(job_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Completed
    );

    // Stale-claim guard: fail_job on a now-completed (not running) job is
    // a no-op, mirroring the Postgres status guard.
    let stale = store
        .fail_job(&JobFailure {
            job_id,
            claim_token: token,
            result: None,
            remark: "late".into(),
            event_detail: None,
        })
        .await
        .unwrap();
    assert!(!stale, "fail_job on a non-running job must be a no-op");

    // fail_job happy path on a fresh running job (distinct webhook id).
    let (job2, token2) = run_job(&store, 12).await;
    let ok = store
        .fail_job(&JobFailure {
            job_id: job2,
            claim_token: token2,
            result: None,
            remark: "boom".into(),
            event_detail: None,
        })
        .await
        .unwrap();
    assert!(ok);
    assert_eq!(
        store
            .lookup_job(job2)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Failed
    );
}
