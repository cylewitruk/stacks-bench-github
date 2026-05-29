//! Slice 8 (post-second-pass review): in-memory `JobV2Store` tests
//! focused on the atomic visibility property of
//! `create_job_with_links` — a concurrent reader must NEVER observe
//! a partially-created job. Mirrors the Postgres single-transaction
//! semantic.
//!
//! These tests don't need Postgres — the in-memory store is the unit
//! under test.

use std::sync::Arc;

use sbgh_core::db::{InMemoryJobV2Store, JobV2Store};
use sbgh_core::models::{
    GitRefKind, JobCreationRequest, JobKind, JobStatus, NewJobV2, NewPullRequestLink, TriggerKind,
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
    let created = store
        .create_job_with_links(&make_request(1))
        .await
        .unwrap();

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
