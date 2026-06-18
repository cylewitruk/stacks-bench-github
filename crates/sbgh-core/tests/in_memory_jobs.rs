//! Slice 8 (post-second-pass review): in-memory `JobStore` tests
//! focused on the atomic visibility property of
//! `create_job_with_links` — a concurrent reader must NEVER observe
//! a partially-created job. Mirrors the Postgres single-transaction
//! semantic.
//!
//! These tests don't need Postgres — the in-memory store is the unit
//! under test.

use std::sync::Arc;

use sbgh_core::db::{
    BaselineSelection, InMemoryJobStore, JobCompletion, JobCreationOutcome, JobFailure, JobStore,
};
use sbgh_core::models::{
    GitRefKind, JobAxes, JobCreationRequest, JobKind, JobMetric, JobResult, JobSource, JobStatus,
    NewJob, NewPullRequestLink, QueuedEventDetail, TriggerKind,
};
use uuid::Uuid;

fn make_request(webhook_id: i64) -> JobCreationRequest {
    JobCreationRequest {
        new_job: NewJob {
            github_installation_id: 100,
            github_repo_id: 10,
            axes: JobAxes::from_legacy(TriggerKind::PrComment, JobKind::AdHoc),
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: "main".into(),
            git_commit_hash: None,
            git_committed_at: None,
            workload_key: None,
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

/// v5 (item 0002): in-memory parity for `create_unlinked_job` (the store the
/// Slack connector tests use) — a webhook-less queued job whose queued event
/// carries the `SlackAdhoc` provenance, with no webhook link.
#[tokio::test]
async fn create_unlinked_job_is_webhook_less_and_preserves_detail() {
    let store = InMemoryJobStore::new();
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        bench_args: vec!["--block".into(), "184231".into()],
        clean_repetitions: 1,
    })
    .unwrap();
    let new_job = NewJob {
        github_installation_id: 100,
        github_repo_id: 10,
        axes: JobAxes::from_legacy(TriggerKind::SlackAdhoc, JobKind::AdHoc),
        git_ref_kind: GitRefKind::Branch,
        git_ref_display: "develop".into(),
        git_commit_hash: None,
        git_committed_at: None,
        workload_key: None,
    };

    let job = store
        .create_unlinked_job(uuid::Uuid::new_v4(), &new_job, &detail, None)
        .await
        .unwrap();
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.source, JobSource::Slack);
    assert!(job.claim_token.is_none() && job.claimed_at.is_none());

    // No webhook link — same boundary as the Postgres path.
    assert!(!store.has_webhook_link_for_job(job.id), "an ad-hoc job must have no webhook link");

    // The queued event preserves the SlackAdhoc detail verbatim.
    let queued = store
        .queued_event(job.id)
        .await
        .unwrap()
        .expect("ad-hoc job has a queued event");
    assert_eq!(
        queued
            .detail
            .expect("queued event carries detail")
            .0,
        detail
    );
}

#[tokio::test]
async fn in_memory_repeat_planner_appends_and_resumes_next_run() {
    let store = InMemoryJobStore::new();
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "1".into()],
        clean_repetitions: 2,
    })
    .unwrap();
    let first = store
        .create_unlinked_job(
            uuid::Uuid::new_v4(),
            &NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                axes: JobAxes::from_legacy(TriggerKind::SlackAdhoc, JobKind::AdHoc),
                git_ref_kind: GitRefKind::Branch,
                git_ref_display: "develop".into(),
                git_commit_hash: Some("abc".into()),
                git_committed_at: None,
                workload_key: Some("wk".into()),
            },
            &detail,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .spec(first.benchmark_spec_id)
            .expect("spec exists")
            .requested_run_count,
        2
    );
    store
        .record_plan_message_ts(first.id, "1700000000.000200")
        .await
        .unwrap();

    let token = Uuid::new_v4();
    let claimed = store
        .claim_next_queued(token)
        .await
        .unwrap()
        .expect("run 0 claimed");
    store
        .mark_running(claimed.id, token, None)
        .await
        .unwrap();
    store
        .complete_job(&JobCompletion {
            job_id: claimed.id,
            claim_token: token,
            result: JobResult {
                job_id: claimed.id,
                run_json: None,
                archive_dir: "/tmp".into(),
                created_at: chrono::Utc::now(),
            },
            metric: None,
            baseline_calibration_id: None,
            event_detail: None,
        })
        .await
        .unwrap();

    let resumed = store
        .resume_pending_benchmark_runs()
        .await
        .unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].benchmark_spec_id, first.benchmark_spec_id);
    assert_eq!(resumed[0].benchmark_run_index, 1);
    assert_eq!(resumed[0].status, JobStatus::Queued);
    assert_eq!(
        store
            .latest_plan_message_ts(resumed[0].id)
            .await
            .unwrap()
            .as_deref(),
        Some("1700000000.000200"),
        "repeat run reuses the group's Slack card",
    );
    assert!(
        store
            .resume_pending_benchmark_runs()
            .await
            .unwrap()
            .is_empty(),
        "active run 1 blocks duplicate resume"
    );
}

#[tokio::test]
async fn create_job_with_links_is_atomically_visible() {
    // After the call returns Ok, ALL related rows must be present
    // — never a job without its webhook link or queued event.
    let store = InMemoryJobStore::new();
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
    let store = InMemoryJobStore::new();
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
    let store = Arc::new(InMemoryJobStore::new());
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
async fn run_job(store: &InMemoryJobStore, webhook_id: i64) -> (Uuid, Uuid) {
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
    let store = InMemoryJobStore::new();
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
            baseline_calibration_id: Some(42),
            event_detail: None,
        })
        .await
        .unwrap();
    assert!(ok);
    let completed = store
        .lookup_job(job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .spec(completed.benchmark_spec_id)
            .unwrap()
            .baseline_calibration_id,
        Some(42)
    );
    assert_eq!(completed.status, JobStatus::Completed);

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

// ─── roadmap-v7: find_baseline_for parity with Postgres ──────────────────

fn ts(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs, 0).unwrap()
}

/// Drive a baseline job to `completed` with a metric (insert → claim → run →
/// complete). Each call fully completes, so the next call's insert is the only
/// `queued` row and `claim_next_queued` picks it deterministically.
async fn seed_completed_baseline(
    store: &InMemoryJobStore,
    sha: &str,
    ref_display: &str,
    committed_at: chrono::DateTime<chrono::Utc>,
    workload_key: &str,
    exec_us: i64,
) {
    store
        .insert_job(&NewJob {
            github_installation_id: 100,
            github_repo_id: 10,
            axes: JobAxes::from_legacy(TriggerKind::BranchPush, JobKind::Baseline),
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: ref_display.into(),
            git_commit_hash: Some(sha.into()),
            git_committed_at: Some(committed_at),
            workload_key: Some(workload_key.into()),
        })
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let claimed = store
        .claim_next_queued(token)
        .await
        .unwrap()
        .unwrap();
    store
        .mark_running(claimed.id, token, None)
        .await
        .unwrap();
    store
        .complete_job(&JobCompletion {
            job_id: claimed.id,
            claim_token: token,
            result: JobResult {
                job_id: claimed.id,
                run_json: None,
                archive_dir: "/tmp".into(),
                created_at: chrono::Utc::now(),
            },
            metric: Some(JobMetric {
                job_id: claimed.id,
                envelope_duration_us: 0,
                replay_duration_us: 0,
                total_duration_us: 0,
                setup_duration_us: 0,
                execution_duration_us: exec_us,
                commit_duration_us: 0,
                clarity_runtime: 0,
                transactions: 0,
                read_length: 0,
                write_length: 0,
                measured_blocks: 5000,
                warmup_blocks: 1000,
                created_at: chrono::Utc::now(),
            }),
            baseline_calibration_id: None,
            event_detail: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn find_baseline_for_mirrors_postgres() {
    let store = InMemoryJobStore::new();
    seed_completed_baseline(&store, "old1", "develop", ts(1_000), "wk1", 100).await;
    seed_completed_baseline(&store, "abc", "develop", ts(2_000), "wk1", 222).await;

    // Exact hit at the merge-base SHA.
    let exact = store
        .find_baseline_for("abc", "develop", Some(ts(2_500)), "wk1")
        .await
        .unwrap()
        .expect("exact hit");
    assert_eq!(exact.anchor.commit, "abc");
    assert_eq!(exact.anchor.selection, BaselineSelection::Exact);
    assert_eq!(
        exact
            .metric
            .execution_duration_us,
        222
    );

    // Nearest-before when the fork-point SHA wasn't benchmarked → newest ≤ 2500.
    let near = store
        .find_baseline_for("forkpoint", "develop", Some(ts(2_500)), "wk1")
        .await
        .unwrap()
        .expect("nearest-before");
    assert_eq!(near.anchor.commit, "abc");
    assert_eq!(near.anchor.selection, BaselineSelection::NearestBefore);

    // Workload mismatch and a missing fork-point timestamp both → None.
    assert!(
        store
            .find_baseline_for("abc", "develop", Some(ts(2_500)), "wk2")
            .await
            .unwrap()
            .is_none(),
    );
    assert!(
        store
            .find_baseline_for("nope", "develop", None, "wk1")
            .await
            .unwrap()
            .is_none(),
    );
}
