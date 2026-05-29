//! Slice 8 integration tests for `PostgresJobV2Store` against real
//! Postgres. Covers:
//!
//!   - The queued-state invariant (insert leaves claim_token + claimed_at NULL)
//!   - The full lifecycle: queued → claimed → running → terminal
//!   - Claim-token-guarded conditional writes (mark_running / mark_terminal
//!     reject stale tokens)
//!   - Stuck-claim sweep recovery
//!   - FK enforcement on the multi-table relations
//!   - Concurrent FOR UPDATE SKIP LOCKED hands out disjoint rows
//!   - Job event timeline + write-once metric/result behaviour

use std::sync::Arc;

use chrono::Duration;
use sbgh_core::db::{
    CreatedJob, JobCompletion, JobCreationOutcome, JobFailure, JobV2Store, NewInstallation, Pool,
    PostgresJobV2Store, setup_pg,
};
use sbgh_core::models::{
    GitRefKind, GithubAccountType, JobCreationRequest, JobEventKind, JobEventStatus, JobKind,
    JobMetric, JobResult, JobStatus, NewJobEvent, NewJobV2, NewPullRequestLink, ResolvedCommit,
    TerminalJobStatus, TriggerKind,
};
use uuid::Uuid;

/// Unwrap a `JobCreationOutcome` expected to be a fresh creation.
fn expect_created(outcome: JobCreationOutcome) -> CreatedJob {
    match outcome {
        JobCreationOutcome::Created(c) => *c,
        JobCreationOutcome::AlreadyEnqueued => {
            panic!("expected a fresh Created job, got AlreadyEnqueued")
        }
    }
}

/// Seed install + repo + membership so `job`'s composite FK to
/// `github_installation_repo` is satisfiable.
async fn seed_install_repo(pool: &Pool, install_id: i64, repo_id: i64) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         ($1, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    use sbgh_core::db::{InstallationStore, PostgresInstallationStore};
    let install_store = PostgresInstallationStore::new(pool.clone());
    install_store
        .upsert_installation(&NewInstallation {
            id: install_id,
            github_account_id: install_id,
            account_login: "octo".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES ($1, 'o', $2) ON CONFLICT DO NOTHING",
    )
    .bind(repo_id)
    .bind(format!("r{repo_id}"))
    .execute(pool)
    .await
    .unwrap();
    install_store
        .add_or_restore_membership(install_id, repo_id)
        .await
        .unwrap();
}

fn make_new_job(install_id: i64, repo_id: i64) -> NewJobV2 {
    NewJobV2 {
        github_installation_id: install_id,
        github_repo_id: repo_id,
        job_kind: JobKind::AdHoc,
        trigger_kind: TriggerKind::PrComment,
        git_ref_kind: GitRefKind::Branch,
        git_ref_display: "main".into(),
        git_commit_hash: None,
        git_committed_at: None,
    }
}

#[tokio::test]
async fn insert_job_leaves_claim_columns_null_in_queued_state() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());

    let job = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    assert_eq!(job.status, JobStatus::Queued);
    assert!(job.claim_token.is_none(), "queued-state invariant: claim_token MUST be NULL");
    assert!(job.claimed_at.is_none(), "queued-state invariant: claimed_at MUST be NULL");
}

#[tokio::test]
async fn full_lifecycle_queued_to_claimed_to_running_to_completed() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let queued = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();

    let claim_token = Uuid::new_v4();
    let claimed = store
        .claim_next_queued(claim_token)
        .await
        .unwrap()
        .expect("claim returns the queued row");
    assert_eq!(claimed.id, queued.id);
    assert_eq!(claimed.status, JobStatus::Claimed);
    assert_eq!(claimed.claim_token, Some(claim_token));
    assert!(claimed.claimed_at.is_some());

    let ran = store
        .mark_running(claimed.id, claim_token, None)
        .await
        .unwrap();
    assert!(ran, "mark_running with matching claim_token must succeed");
    let after_run = store
        .lookup_job(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_run.status, JobStatus::Running);
    // Audit invariant: running PRESERVES claim_token + claimed_at.
    assert_eq!(after_run.claim_token, Some(claim_token));
    assert!(after_run.claimed_at.is_some());

    let done = store
        .mark_terminal(claimed.id, claim_token, TerminalJobStatus::Completed)
        .await
        .unwrap();
    assert!(done);
    let after_done = store
        .lookup_job(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_done.status, JobStatus::Completed);
    // Audit invariant: terminal PRESERVES claim_token + claimed_at.
    assert_eq!(after_done.claim_token, Some(claim_token));
    assert!(
        after_done
            .claimed_at
            .is_some()
    );
}

#[tokio::test]
async fn mark_running_rejects_stale_claim_token() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let queued = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();

    let real_token = Uuid::new_v4();
    let _ = store
        .claim_next_queued(real_token)
        .await
        .unwrap()
        .unwrap();

    let stale_token = Uuid::new_v4();
    let result = store
        .mark_running(queued.id, stale_token, None)
        .await
        .unwrap();
    assert!(!result, "stale claim_token MUST NOT transition the row");
    // Row stayed at claimed.
    let row = store
        .lookup_job(queued.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Claimed);
}

#[tokio::test]
async fn mark_terminal_rejects_transitions_skipping_running() {
    // The mark_terminal predicate requires status='running' so a
    // caller can't shortcut claimed → completed (would lose the
    // execution-started signal).
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let queued = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let _ = store
        .claim_next_queued(token)
        .await
        .unwrap();
    let direct = store
        .mark_terminal(queued.id, token, TerminalJobStatus::Completed)
        .await
        .unwrap();
    assert!(!direct, "mark_terminal MUST require status='running'");
}

#[tokio::test]
async fn sweep_stuck_claims_recovers_old_claimed_rows() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let queued = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let _ = store
        .claim_next_queued(token)
        .await
        .unwrap();
    // Backdate claimed_at well past any reasonable lease.
    sqlx::query("UPDATE job SET claimed_at = NOW() - INTERVAL '1 hour' WHERE id = $1")
        .bind(queued.id)
        .execute(&pool)
        .await
        .unwrap();

    let recovered = store
        .sweep_stuck_claims(Duration::minutes(5))
        .await
        .unwrap();
    assert_eq!(recovered, 1);
    let row = store
        .lookup_job(queued.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert!(row.claim_token.is_none(), "sweep clears claim_token");
    assert!(row.claimed_at.is_none(), "sweep clears claimed_at");
}

#[tokio::test]
async fn sweep_leaves_fresh_claimed_rows_alone() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let _ = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let _ = store
        .claim_next_queued(token)
        .await
        .unwrap();

    let recovered = store
        .sweep_stuck_claims(Duration::minutes(5))
        .await
        .unwrap();
    assert_eq!(recovered, 0, "fresh claim is within the lease window");
}

#[tokio::test]
async fn concurrent_claims_pick_disjoint_rows() {
    // Real FOR UPDATE SKIP LOCKED semantics — two parallel claimants
    // against a 2-row queue must come away with distinct rows.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobV2Store::new(pool.clone()));
    let _a = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let _b = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();

    let token_a = Uuid::new_v4();
    let token_b = Uuid::new_v4();
    let s1 = store.clone();
    let s2 = store.clone();
    let (r1, r2) = tokio::join!(
        async move {
            s1.claim_next_queued(token_a)
                .await
                .unwrap()
        },
        async move {
            s2.claim_next_queued(token_b)
                .await
                .unwrap()
        },
    );
    let a = r1.expect("first claim succeeds");
    let b = r2.expect("second claim succeeds");
    assert_ne!(a.id, b.id, "FOR UPDATE SKIP LOCKED must hand out distinct rows");
}

#[tokio::test]
async fn claim_next_returns_none_when_queue_empty() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresJobV2Store::new(pool.clone());
    let none = store
        .claim_next_queued(Uuid::new_v4())
        .await
        .unwrap();
    assert!(none.is_none());
}

#[tokio::test]
async fn insert_job_rejects_unknown_installation_repo_pair() {
    // Composite FK enforces (install, repo) ∈ github_installation_repo.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let store = PostgresJobV2Store::new(pool.clone());
    let err = store
        .insert_job(&make_new_job(999, 999))
        .await;
    assert!(err.is_err(), "FK violation must surface as an error");
}

#[tokio::test]
async fn job_event_insert_round_trips() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let job = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let event = store
        .insert_event(&NewJobEvent {
            job_id: job.id,
            event_kind: JobEventKind::Queued,
            event_status: JobEventStatus::Success,
            github_comment_id: None,
            remark: Some("slice 9 will populate this from the processor".into()),
            detail: Some(serde_json::json!({ "trigger": "pr_comment" })),
        })
        .await
        .unwrap();
    assert_eq!(event.job_id, job.id);
    assert_eq!(event.event_kind, JobEventKind::Queued);
    assert!(event.detail.is_some());
}

#[tokio::test]
async fn job_metric_insert_round_trips_and_pk_collision_errors() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let job = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let metric = JobMetric {
        job_id: job.id,
        envelope_duration_us: 1,
        replay_duration_us: 2,
        total_duration_us: 3,
        setup_duration_us: 4,
        execution_duration_us: 5,
        commit_duration_us: 6,
        clarity_runtime: 7,
        transactions: 8,
        read_length: 9,
        write_length: 10,
        measured_blocks: 11,
        warmup_blocks: 12,
        created_at: chrono::Utc::now(),
    };
    store
        .record_metric(&metric)
        .await
        .unwrap();
    // Write-once: second insert collides on PK.
    let second = store
        .record_metric(&metric)
        .await;
    assert!(second.is_err(), "job_metric is write-once (PK conflict on re-insert)");
}

#[tokio::test]
async fn job_result_insert_round_trips_with_optional_run_json() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let job = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    store
        .record_result(&JobResult {
            job_id: job.id,
            run_json: Some(sqlx::types::Json(serde_json::json!({"ok": true}))),
            archive_dir: "/var/runs/abc".into(),
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    // Insert with NULL run_json (failure case) on a different job.
    let job2 = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    store
        .record_result(&JobResult {
            job_id: job2.id,
            run_json: None,
            archive_dir: "/var/runs/failed".into(),
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn subject_relation_link_tables_round_trip_with_fk_enforcement() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let job = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();

    // Seed webhook + user + PR rows so the link FKs resolve.
    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('slice8-1', 'issue_comment', 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES (42, 'alice', 'user')")
        .execute(&pool)
        .await
        .unwrap();
    let pr_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_pull_request (target_github_repo_id, source_github_repo_id, \
         pr_number, title, author_github_user_id) VALUES (10, 10, 1, 't', 42) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    store
        .link_to_webhook(webhook_id, job.id)
        .await
        .unwrap();
    store
        .link_to_user(42, job.id)
        .await
        .unwrap();
    store
        .link_to_pull_request(pr_id, job.id, Some(9001))
        .await
        .unwrap();

    // UNIQUE on github_webhook_job.job_id: second link for the SAME
    // job MUST fail.
    let second = store
        .link_to_webhook(webhook_id, job.id)
        .await;
    assert!(second.is_err(), "github_webhook_job.job_id UNIQUE blocks duplicate links");
}

// ─── Post-review fixes ────────────────────────────────────────────────

#[tokio::test]
async fn create_job_with_links_inserts_job_links_and_queued_event_atomically() {
    // Slice 8 (post-review M1 fix): the transactional creation
    // boundary. Asserts that all five rows land together.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());

    // Seed FK targets.
    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('create-with-links-1', 'issue_comment', 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES (42, 'alice', 'user')")
        .execute(&pool)
        .await
        .unwrap();
    let pr_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_pull_request (target_github_repo_id, source_github_repo_id, \
         pr_number, title, author_github_user_id) VALUES (10, 10, 1, 't', 42) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let created = expect_created(
        store
            .create_job_with_links(&JobCreationRequest {
                new_job: make_new_job(100, 10),
                github_webhook_id: webhook_id,
                triggering_user_id: Some(42),
                pull_request_link: Some(NewPullRequestLink {
                    github_pull_request_id: pr_id,
                    triggering_comment_id: Some(9001),
                }),
                queued_event_detail: Some(serde_json::json!({"trigger": "pr_comment"})),
            })
            .await
            .unwrap(),
    );

    assert_eq!(created.job.status, JobStatus::Queued);
    assert_eq!(
        created
            .webhook_link
            .github_webhook_id,
        webhook_id
    );
    assert_eq!(
        created
            .user_link
            .as_ref()
            .unwrap()
            .github_user_id,
        42
    );
    assert_eq!(
        created
            .pull_request_link
            .as_ref()
            .unwrap()
            .github_pull_request_id,
        pr_id
    );
    assert_eq!(
        created
            .queued_event
            .event_kind,
        JobEventKind::Queued
    );
    assert_eq!(
        created
            .queued_event
            .event_status,
        JobEventStatus::Success
    );
}

#[tokio::test]
async fn create_job_with_links_rolls_back_on_fk_violation() {
    // Slice 8 (post-review M1 fix): a FK failure on any link MUST
    // roll back the job row insertion too — no partial state.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    // No webhook seeded → FK on github_webhook_id fails.
    let result = store
        .create_job_with_links(&JobCreationRequest {
            new_job: make_new_job(100, 10),
            github_webhook_id: 9999, // unknown webhook id
            triggering_user_id: None,
            pull_request_link: None,
            queued_event_detail: None,
        })
        .await;
    assert!(result.is_err(), "FK violation on webhook link must surface");

    // CRITICAL: no orphaned job row.
    let job_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_count, 0, "transaction rollback MUST clean up the job row");
}

#[tokio::test]
async fn create_job_with_links_optional_user_and_pr_links_are_skipped_when_none() {
    // Slice 8 (post-review M1): triggers without a responsible user
    // (branch_push, tag_created, scheduled) skip the user link;
    // non-PR jobs skip the PR link. Verify both omissions produce a
    // valid job with only the webhook link present.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('create-with-links-2', 'push', 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let created = expect_created(
        store
            .create_job_with_links(&JobCreationRequest {
                new_job: NewJobV2 {
                    trigger_kind: TriggerKind::BranchPush,
                    ..make_new_job(100, 10)
                },
                github_webhook_id: webhook_id,
                triggering_user_id: None,
                pull_request_link: None,
                queued_event_detail: None,
            })
            .await
            .unwrap(),
    );
    assert!(created.user_link.is_none());
    assert!(
        created
            .pull_request_link
            .is_none()
    );
}

#[tokio::test]
async fn create_job_with_links_is_idempotent_on_webhook_id() {
    // Slice 9 (review fix): job creation is the first non-idempotent
    // classify side effect, and the inbox is at-least-once. A webhook
    // reprocessed after a failed complete() / swept lease MUST NOT mint
    // a second job. The UNIQUE(github_webhook_id) guard +
    // ON CONFLICT DO NOTHING makes the retry a no-op returning
    // AlreadyEnqueued, leaving exactly one job.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('idempotent-1', 'push', 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let request = JobCreationRequest {
        new_job: NewJobV2 {
            trigger_kind: TriggerKind::BranchPush,
            ..make_new_job(100, 10)
        },
        github_webhook_id: webhook_id,
        triggering_user_id: None,
        pull_request_link: None,
        queued_event_detail: None,
    };

    // First create succeeds.
    let first = store
        .create_job_with_links(&request)
        .await
        .unwrap();
    assert!(matches!(first, JobCreationOutcome::Created(_)));

    // Reprocess the SAME webhook → idempotent no-op.
    let second = store
        .create_job_with_links(&request)
        .await
        .unwrap();
    assert!(
        matches!(second, JobCreationOutcome::AlreadyEnqueued),
        "second create for the same webhook must be AlreadyEnqueued, not a duplicate"
    );

    // Exactly one job + one link, no orphan from the rolled-back retry.
    let job_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_count, 1, "retry must not create a second job");
    let link_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook_job WHERE github_webhook_id = $1")
            .bind(webhook_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(link_count, 1);
}

#[tokio::test]
async fn mark_running_with_resolved_commit_writes_metadata_atomically() {
    // Slice 8 (post-review M2 fix): the orchestrator resolves a
    // branch tip during the claim phase and passes the resolved
    // values to mark_running. Both commit metadata and status
    // transition land under the same claim_token guard.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let _ = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let claimed = store
        .claim_next_queued(token)
        .await
        .unwrap()
        .unwrap();
    assert!(
        claimed
            .git_commit_hash
            .is_none(),
        "queue-time commit was unresolved"
    );

    let now = chrono::Utc::now();
    let ran = store
        .mark_running(
            claimed.id,
            token,
            Some(ResolvedCommit {
                hash: "deadbeef".into(),
                committed_at: Some(now),
            }),
        )
        .await
        .unwrap();
    assert!(ran);
    let row = store
        .lookup_job(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Running);
    assert_eq!(row.git_commit_hash.as_deref(), Some("deadbeef"));
    assert!(row.git_committed_at.is_some());
}

#[tokio::test]
async fn mark_running_without_resolved_commit_leaves_existing_metadata_untouched() {
    // Mirror case: if the job was queued with a concrete commit (push
    // trigger), mark_running(None) must leave the existing commit
    // columns untouched.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let preset = NewJobV2 {
        git_commit_hash: Some("preset-sha".into()),
        git_committed_at: Some(chrono::Utc::now()),
        ..make_new_job(100, 10)
    };
    let _ = store
        .insert_job(&preset)
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let claimed = store
        .claim_next_queued(token)
        .await
        .unwrap()
        .unwrap();
    let _ = store
        .mark_running(claimed.id, token, None)
        .await
        .unwrap();
    let row = store
        .lookup_job(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.git_commit_hash.as_deref(), Some("preset-sha"));
}

// ─── Slice 10: transactional finish ────────────────────────────────────

/// Claim + run a freshly-inserted job, returning `(job_id, claim_token)`
/// in the `running` state ready for `complete_job` / `fail_job`.
async fn claim_and_run(store: &PostgresJobV2Store, install: i64, repo: i64) -> (Uuid, Uuid) {
    store
        .insert_job(&make_new_job(install, repo))
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
    (claimed.id, token)
}

fn sample_metric(job_id: Uuid) -> JobMetric {
    JobMetric {
        job_id,
        envelope_duration_us: 100,
        replay_duration_us: 90,
        total_duration_us: 80,
        setup_duration_us: 5,
        execution_duration_us: 60,
        commit_duration_us: 15,
        clarity_runtime: 42,
        transactions: 7,
        read_length: 3,
        write_length: 2,
        measured_blocks: 4,
        warmup_blocks: 1,
        created_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn complete_job_writes_status_result_metric_and_event_atomically() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let (job_id, token) = claim_and_run(&store, 100, 10).await;

    let ok = store
        .complete_job(&JobCompletion {
            job_id,
            claim_token: token,
            result: JobResult {
                job_id,
                run_json: Some(sqlx::types::Json(serde_json::json!({"ok": true}))),
                archive_dir: "/var/runs/done".into(),
                created_at: chrono::Utc::now(),
            },
            metric: Some(sample_metric(job_id)),
            event_detail: Some(serde_json::json!({"finish_reason": "phase_done"})),
        })
        .await
        .unwrap();
    assert!(ok);

    let row = store
        .lookup_job(job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Completed);
    // Audit columns preserved.
    assert_eq!(row.claim_token, Some(token));

    let metric_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_metric WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(metric_count, 1);
    let result_dir: String =
        sqlx::query_scalar("SELECT archive_dir FROM job_result WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(result_dir, "/var/runs/done");
    let event_kind: String = sqlx::query_scalar(
        "SELECT event_kind::text FROM job_event WHERE job_id = $1 AND event_kind = 'completed'",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_kind, "completed");
}

#[tokio::test]
async fn complete_job_with_stale_claim_is_noop() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let (job_id, _token) = claim_and_run(&store, 100, 10).await;

    let stale = Uuid::new_v4();
    let ok = store
        .complete_job(&JobCompletion {
            job_id,
            claim_token: stale,
            result: JobResult {
                job_id,
                run_json: None,
                archive_dir: "/x".into(),
                created_at: chrono::Utc::now(),
            },
            metric: None,
            event_detail: None,
        })
        .await
        .unwrap();
    assert!(!ok, "stale claim_token must be a no-op");
    // Nothing written, job still running.
    let row = store
        .lookup_job(job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Running);
    let result_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_result WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(result_count, 0, "rollback leaves no result row");
}

#[tokio::test]
async fn fail_job_writes_status_result_and_event() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobV2Store::new(pool.clone());
    let (job_id, token) = claim_and_run(&store, 100, 10).await;

    let ok = store
        .fail_job(&JobFailure {
            job_id,
            claim_token: token,
            result: Some(JobResult {
                job_id,
                run_json: None,
                archive_dir: "/var/runs/failed".into(),
                created_at: chrono::Utc::now(),
            }),
            remark: "VM powered off before phase=done".into(),
            event_detail: Some(serde_json::json!({"finish_reason": "shut_off"})),
        })
        .await
        .unwrap();
    assert!(ok);

    let row = store
        .lookup_job(job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    let (event_kind, remark): (String, Option<String>) = sqlx::query_as(
        "SELECT event_kind::text, remark FROM job_event WHERE job_id = $1 AND event_kind = \
         'failed'",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_kind, "failed");
    assert_eq!(remark.as_deref(), Some("VM powered off before phase=done"));
    // Forensics result row recorded even on failure.
    let result_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_result WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(result_count, 1);
}
