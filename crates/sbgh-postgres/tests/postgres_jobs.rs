//! Slice 8 integration tests for `PostgresJobStore` against real
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
use sbgh_core::models::{
    BuildTarget, GitRefKind, GithubAccountType, JobAxes, JobCreationRequest, JobEventKind,
    JobEventStatus, JobIntent, JobKind, JobMetric, JobResult, JobSource, JobStatus, NewJob,
    NewJobEvent, NewPullRequestLink, QueuedEventDetail, ResolvedCommit, TaskKind,
    TerminalJobStatus, TriggerKind,
};
use sbgh_postgres::Db;
use sbgh_postgres::db::{
    CreatedJob, JobCompletion, JobCreationOutcome, JobFailure, JobStore, NewInstallation, Pool,
    PostgresJobStore, setup_pg_db,
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
    use sbgh_postgres::db::{InstallationStore, PostgresInstallationStore};
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

fn make_new_job(install_id: i64, repo_id: i64) -> NewJob {
    NewJob {
        github_installation_id: install_id,
        github_repo_id: repo_id,
        // v10 (0005): a PR-comment ad-hoc, expressed via the axes. Helpers keep
        // taking the legacy `(trigger_kind, job_kind)` shape and convert through
        // `from_legacy` so call sites read unchanged.
        axes: JobAxes::from_legacy(TriggerKind::PrComment, JobKind::AdHoc),
        git_ref_kind: GitRefKind::Branch,
        git_ref_display: "main".into(),
        git_commit_hash: None,
        git_committed_at: None,
        workload_key: None,
    }
}

/// v5 (item 0002): `TriggerKind::SlackAdhoc` — added by the 20260609 migration
/// — round-trips through the `trigger_kind` Postgres enum, proving the
/// migration applied and the sqlx mapping matches the DB literal
/// `'slack_adhoc'`.
#[tokio::test]
async fn trigger_kind_slack_adhoc_round_trips() {
    let (_db, pool) = setup_pg_db().await;
    // Decode the DB literal → enum.
    let decoded: Db<TriggerKind> = sqlx::query_scalar("SELECT 'slack_adhoc'::trigger_kind")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(decoded.0, TriggerKind::SlackAdhoc);
    // Encode the enum → DB → enum.
    let round: Db<TriggerKind> = sqlx::query_scalar("SELECT $1::trigger_kind")
        .bind(Db(TriggerKind::SlackAdhoc))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(round.0, TriggerKind::SlackAdhoc);
}

/// v5 (item 0002): `create_unlinked_job` inserts a webhook-less queued job +
/// its queued `job_event` (carrying the `SlackAdhoc` provenance) in one
/// transaction, with **no** `github_webhook_job` link — the non-webhook trigger
/// entry path, kept separate from the GitHub webhook flow.
#[tokio::test]
async fn create_unlinked_job_inserts_queued_job_and_event_without_webhook() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        reporting_identity: None,
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
    assert!(
        job.claim_token.is_none() && job.claimed_at.is_none(),
        "queued-state invariant holds for an ad-hoc job"
    );

    // No webhook link — the ad-hoc path doesn't bend the GitHub webhook flow.
    let webhook_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook_job WHERE job_id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(webhook_count, 0, "an ad-hoc job must have no webhook link");

    // The queued event carries the SlackAdhoc provenance the claim path reads.
    let queued = store
        .queued_event(job.id)
        .await
        .unwrap()
        .expect("ad-hoc job has a queued event");
    let parsed: QueuedEventDetail = serde_json::from_value(
        queued
            .detail
            .expect("queued event carries detail"),
    )
    .unwrap();
    assert!(
        matches!(parsed, QueuedEventDetail::SlackAdhoc { channel, .. } if channel == "C123"),
        "queued event carries the SlackAdhoc detail"
    );
}

/// v11 (item 0031): `create_unlinked_job` is the webhook-less path **warming**
/// rides — a build-only job with daemon axes (`source=daemon`,
/// `intent=cache_warm`, `task_kind=build_only`, `build_target=stacks_bench`)
/// and a `CacheWarm` provenance detail, with no webhook/PR/owner links.
#[tokio::test]
async fn create_unlinked_job_writes_build_only_cache_warm() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    let detail = serde_json::to_value(QueuedEventDetail::CacheWarm {
        trigger_id: 7,
        git_ref: "release/3.2".into(),
        commit: "abc123".into(),
        build_target: BuildTarget::StacksBench,
    })
    .unwrap();
    let new_job = NewJob {
        github_installation_id: 100,
        github_repo_id: 10,
        axes: JobAxes {
            source: JobSource::Daemon,
            intent: JobIntent::CacheWarm,
            task_kind: TaskKind::BuildOnly,
            build_target: BuildTarget::StacksBench,
        },
        git_ref_kind: GitRefKind::Branch,
        git_ref_display: "release/3.2".into(),
        git_commit_hash: Some("abc123".into()),
        git_committed_at: None,
        workload_key: None,
    };

    let job = store
        .create_unlinked_job(uuid::Uuid::new_v4(), &new_job, &detail, None)
        .await
        .unwrap();

    assert_eq!(job.source, JobSource::Daemon);
    assert_eq!(job.intent, JobIntent::CacheWarm);
    assert_eq!(job.task_kind, TaskKind::BuildOnly);
    assert_eq!(job.build_target, BuildTarget::StacksBench);
    assert_eq!(job.status, JobStatus::Queued);

    // No webhook/PR/owner links — daemon warming has no GitHub subject.
    let webhook_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM github_webhook_job WHERE job_id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(webhook_count, 0, "a warming job must have no webhook link");

    // The CacheWarm provenance round-trips for the audit trail.
    let queued = store
        .queued_event(job.id)
        .await
        .unwrap()
        .expect("warming job has a queued event");
    let parsed: QueuedEventDetail = serde_json::from_value(
        queued
            .detail
            .expect("queued event carries detail"),
    )
    .unwrap();
    assert!(
        matches!(
            parsed,
            QueuedEventDetail::CacheWarm {
                trigger_id: 7,
                build_target: BuildTarget::StacksBench,
                ..
            }
        ),
        "queued event carries the CacheWarm provenance",
    );
}

#[tokio::test]
async fn insert_job_leaves_claim_columns_null_in_queued_state() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
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
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresJobStore::new(pool.clone());
    let none = store
        .claim_next_queued(Uuid::new_v4())
        .await
        .unwrap();
    assert!(none.is_none());
}

#[tokio::test]
async fn insert_job_rejects_unknown_installation_repo_pair() {
    // Composite FK enforces (install, repo) ∈ github_installation_repo.
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresJobStore::new(pool.clone());
    let err = store
        .insert_job(&make_new_job(999, 999))
        .await;
    assert!(err.is_err(), "FK violation must surface as an error");
}

#[tokio::test]
async fn job_event_insert_round_trips() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
            github_check_run_id: None,
            github_check_run_url: None,
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    let job = store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    store
        .record_result(&JobResult {
            job_id: job.id,
            run_json: Some(serde_json::json!({"ok": true})),
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
                new_job: NewJob {
                    axes: JobAxes::from_legacy(TriggerKind::BranchPush, JobKind::AdHoc),
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('idempotent-1', 'push', 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let request = JobCreationRequest {
        new_job: NewJob {
            axes: JobAxes::from_legacy(TriggerKind::BranchPush, JobKind::AdHoc),
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
    // Slice 8 (post-review M2 fix): the daemon resolves a
    // branch tip during the claim phase and passes the resolved
    // values to mark_running. Both commit metadata and status
    // transition land under the same claim_token guard.
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    let preset = NewJob {
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
async fn claim_and_run(store: &PostgresJobStore, install: i64, repo: i64) -> (Uuid, Uuid) {
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    let (job_id, token) = claim_and_run(&store, 100, 10).await;

    let ok = store
        .complete_job(&JobCompletion {
            job_id,
            claim_token: token,
            result: JobResult {
                job_id,
                run_json: Some(serde_json::json!({"ok": true})),
                archive_dir: "/var/runs/done".into(),
                created_at: chrono::Utc::now(),
            },
            metric: Some(sample_metric(job_id)),
            baseline_calibration_id: Some(42),
            event_detail: Some(serde_json::json!({"finish_reason": "phase_done"})),
        })
        .await
        .unwrap();
    assert!(ok);

    let calibration_id: Option<i64> = sqlx::query_scalar(
        "SELECT s.baseline_calibration_id
           FROM task_spec s
           JOIN job j ON j.task_spec_id = s.id
          WHERE j.id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(calibration_id, Some(42));

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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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
            baseline_calibration_id: None,
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
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
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

#[tokio::test]
async fn fail_job_terminalizes_a_claimed_job_not_yet_running() {
    // Slice 11 review fix (High): a preflight failure (PR-head-SHA / tag
    // resolution / comment posting) happens while the job is still
    // `claimed`. fail_job must terminalize it — otherwise the stuck-claim
    // sweep would requeue it and the failure would loop forever.
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let claimed = store
        .claim_next_queued(token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.status, JobStatus::Claimed);

    // No mark_running — fail straight from `claimed`.
    let ok = store
        .fail_job(&JobFailure {
            job_id: claimed.id,
            claim_token: token,
            result: None,
            remark: "preflight: PR head SHA resolution failed".into(),
            event_detail: None,
        })
        .await
        .unwrap();
    assert!(ok, "fail_job must terminalize a claimed job");
    let row = store
        .lookup_job(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Failed);
}

// ─── roadmap-v5 Phase 4B-2: orphan / stuck-`running` recovery ───────────

#[tokio::test]
async fn running_job_ids_lists_only_running_jobs() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    // One `running`, one left `queued`, one `claimed` (not yet running).
    let (running_id, _t) = claim_and_run(&store, 100, 10).await;
    store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap(); // stays queued
    store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    store
        .claim_next_queued(Uuid::new_v4())
        .await
        .unwrap()
        .unwrap(); // claimed-not-running

    let ids = store
        .running_job_ids()
        .await
        .unwrap();
    assert_eq!(ids, vec![running_id], "only the `running` row is an orphan candidate");
}

#[tokio::test]
async fn cancel_orphan_terminalizes_running_without_a_claim_token_and_is_idempotent() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    // The orphan still carries its (now-dead) claimer's token; cancel_orphan
    // must transition it WITHOUT being given that token.
    let (job_id, _dead_token) = claim_and_run(&store, 100, 10).await;

    let recovered = store
        .cancel_orphan(job_id, "recovered: orphaned by restart")
        .await
        .unwrap();
    assert!(recovered, "a `running` orphan transitions to cancelled with no claim token");

    let row = store
        .lookup_job(job_id)
        .await
        .unwrap()
        .unwrap();
    // A crash-orphan is cancelled (re-triggerable), NOT failed (Phase 4C).
    assert_eq!(row.status, JobStatus::Cancelled);
    let (kind, remark): (String, Option<String>) = sqlx::query_as(
        "SELECT event_kind::text, remark FROM job_event WHERE job_id = $1 AND event_kind = \
         'cancelled'",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kind, "cancelled");
    assert_eq!(remark.as_deref(), Some("recovered: orphaned by restart"));

    // Idempotent: a second pass (a crash mid-recovery re-listed the row) is a
    // no-op — the guard requires `status = 'running'`.
    let again = store
        .cancel_orphan(job_id, "recovered: orphaned by restart")
        .await
        .unwrap();
    assert!(!again, "a row already off `running` must not be re-cancelled");
}

#[tokio::test]
async fn cancel_orphan_ignores_a_claimed_not_running_job() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let claimed = store
        .claim_next_queued(Uuid::new_v4())
        .await
        .unwrap()
        .unwrap();

    // Orphan recovery only touches `running`; a `claimed` row is the
    // stuck-claim sweep's job, not cancel_orphan's.
    let r = store
        .cancel_orphan(claimed.id, "should not apply")
        .await
        .unwrap();
    assert!(!r, "cancel_orphan must leave a `claimed` job alone");
    let row = store
        .lookup_job(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Claimed);
}

// ─── roadmap-v5 Phase 4C: cancellation (operator abort) ─────────────────

#[tokio::test]
async fn cancel_job_transitions_running_to_cancelled_with_event() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    let (job_id, token) = claim_and_run(&store, 100, 10).await;

    let ok = store
        .cancel_job(job_id, token, "aborted by shutdown")
        .await
        .unwrap();
    assert!(ok, "a running job under our token cancels");

    let row = store
        .lookup_job(job_id)
        .await
        .unwrap()
        .unwrap();
    // Cancelled, NOT failed — a deliberately-stopped run (Phase 4C).
    assert_eq!(row.status, JobStatus::Cancelled);
    let (kind, remark): (String, Option<String>) = sqlx::query_as(
        "SELECT event_kind::text, remark FROM job_event WHERE job_id = $1 AND event_kind = \
         'cancelled'",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kind, "cancelled");
    assert_eq!(remark.as_deref(), Some("aborted by shutdown"));
    // No forensics result row for a cancelled run.
    let result_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_result WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(result_count, 0);
}

#[tokio::test]
async fn cancel_job_rejects_a_stale_claim_token() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    let (job_id, _token) = claim_and_run(&store, 100, 10).await;

    // A writer whose lease was reclaimed by the sweep must not cancel the row.
    let ok = store
        .cancel_job(job_id, Uuid::new_v4(), "aborted")
        .await
        .unwrap();
    assert!(!ok, "stale claim_token must not cancel");
    let row = store
        .lookup_job(job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Running);
}

#[tokio::test]
async fn cancel_job_terminalizes_a_claimed_job_not_yet_running() {
    // Like fail_job, cancel accepts `claimed` (an abort can land before the
    // run started) — terminalize cleanly rather than looping via the sweep.
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    store
        .insert_job(&make_new_job(100, 10))
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let claimed = store
        .claim_next_queued(token)
        .await
        .unwrap()
        .unwrap();

    let ok = store
        .cancel_job(claimed.id, token, "aborted before run")
        .await
        .unwrap();
    assert!(ok, "cancel_job must terminalize a claimed job");
    let row = store
        .lookup_job(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, JobStatus::Cancelled);
}

// ─── roadmap-v5 Phase 5: /benchmark dedup (find_active_job) ──────────────

#[tokio::test]
async fn find_active_job_matches_repo_commit_kind_and_excludes_terminal() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    let job = store
        .insert_job(&NewJob {
            github_installation_id: 100,
            github_repo_id: 10,
            axes: JobAxes::from_legacy(TriggerKind::PrComment, JobKind::AdHoc),
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: "feat".into(),
            git_commit_hash: Some("abc".into()),
            git_committed_at: None,
            workload_key: Some("wk1".into()),
        })
        .await
        .unwrap();

    // Exact (repo, commit, kind, workload) match.
    assert_eq!(
        store
            .find_active_job(10, "abc", JobSource::GithubComment, "wk1")
            .await
            .unwrap(),
        Some(job.id),
    );
    // A different commit / repo / source does not match.
    assert!(
        store
            .find_active_job(10, "xyz", JobSource::GithubComment, "wk1")
            .await
            .unwrap()
            .is_none(),
    );
    assert!(
        store
            .find_active_job(99, "abc", JobSource::GithubComment, "wk1")
            .await
            .unwrap()
            .is_none(),
    );
    assert!(
        store
            .find_active_job(10, "abc", JobSource::GithubWebhook, "wk1")
            .await
            .unwrap()
            .is_none(),
    );
    // roadmap-v7: a DIFFERENT workload on the same commit is not deduped — it's
    // a genuinely different benchmark, so it must enqueue.
    assert!(
        store
            .find_active_job(10, "abc", JobSource::GithubComment, "wk2")
            .await
            .unwrap()
            .is_none(),
        "a different workload_key must not match (it's a distinct benchmark)",
    );

    // `claimed` still counts as active (the job is in flight, not terminal).
    let token = Uuid::new_v4();
    store
        .claim_next_queued(token)
        .await
        .unwrap();
    assert_eq!(
        store
            .find_active_job(10, "abc", JobSource::GithubComment, "wk1")
            .await
            .unwrap(),
        Some(job.id),
        "a claimed job is still active",
    );

    // A terminal job is excluded → a re-`/benchmark` after it finishes runs.
    store
        .mark_running(job.id, token, None)
        .await
        .unwrap();
    store
        .cancel_job(job.id, token, "stopped")
        .await
        .unwrap();
    assert!(
        store
            .find_active_job(10, "abc", JobSource::GithubComment, "wk1")
            .await
            .unwrap()
            .is_none(),
        "a terminal (cancelled/completed/failed) job no longer blocks a re-run",
    );
}

// ─── roadmap-v7: baseline comparison lookup (find_baseline_for) ───────────

/// Insert a **completed baseline** with a recorded metric, set up so
/// `find_baseline_for` can match it. Returns the job id.
#[allow(clippy::too_many_arguments)]
async fn seed_completed_baseline(
    pool: &Pool,
    install_id: i64,
    repo_id: i64,
    sha: &str,
    ref_display: &str,
    committed_at: chrono::DateTime<chrono::Utc>,
    workload_key: Option<&str>,
    exec_us: i64,
) -> Uuid {
    let store = PostgresJobStore::new(pool.clone());
    let job = store
        .insert_job(&NewJob {
            github_installation_id: install_id,
            github_repo_id: repo_id,
            axes: JobAxes::from_legacy(TriggerKind::BranchPush, JobKind::Baseline),
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: ref_display.into(),
            git_commit_hash: Some(sha.into()),
            git_committed_at: Some(committed_at),
            workload_key: workload_key.map(Into::into),
        })
        .await
        .unwrap();
    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(job.id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_submission
            SET assigned_measurement_profile = 'test-profile'
          WHERE id = (SELECT task_submission_id FROM job WHERE id = $1)",
    )
    .bind(job.id)
    .execute(pool)
    .await
    .unwrap();
    store
        .record_metric(&JobMetric {
            job_id: job.id,
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
        })
        .await
        .unwrap();
    job.id
}

fn ts(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs, 0).unwrap()
}

#[tokio::test]
async fn find_baseline_for_exact_hit_is_repo_agnostic_and_workload_scoped() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await; // the PR's base repo
    seed_install_repo(&pool, 200, 20).await; // a *different* repo (e.g. upstream)
    let store = PostgresJobStore::new(pool.clone());

    // Baseline lives in repo 20 (not the base repo 10) — exact lookup is
    // repo-agnostic, so a fork PR finds it by SHA.
    let id = seed_completed_baseline(&pool, 200, 20, "abc", "develop", ts(1_000), Some("wk1"), 555)
        .await;

    let m = store
        .find_baseline_for(id, "abc", "develop", Some(ts(2_000)), "wk1")
        .await
        .unwrap()
        .expect("exact baseline at the merge-base SHA");
    assert_eq!(m.anchor.job_id, id);
    assert_eq!(m.anchor.github_repo_id, 20, "found in the upstream repo, not the base");
    assert_eq!(m.anchor.commit, "abc");
    assert_eq!(m.anchor.selection, sbgh_postgres::db::BaselineSelection::Exact);
    assert_eq!(m.metric.execution_duration_us, 555);

    // Different workload → no match (even at the same SHA).
    assert!(
        store
            .find_baseline_for(id, "abc", "develop", Some(ts(2_000)), "wk2")
            .await
            .unwrap()
            .is_none(),
        "a different workload_key must not match",
    );
    // No exact hit + no fork-point timestamp → no nearest-before → None.
    assert!(
        store
            .find_baseline_for(id, "zzz", "develop", None, "wk1")
            .await
            .unwrap()
            .is_none(),
    );
}

#[tokio::test]
async fn find_baseline_for_requires_the_subject_measurement_profile() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());
    let baseline =
        seed_completed_baseline(&pool, 100, 10, "base", "develop", ts(1_000), Some("wk1"), 111)
            .await;
    let subject =
        seed_completed_baseline(&pool, 100, 10, "subject", "feature", ts(1_500), Some("wk1"), 222)
            .await;
    sqlx::query(
        "UPDATE task_submission
            SET assigned_measurement_profile = 'different-profile'
          WHERE id = (SELECT task_submission_id FROM job WHERE id = $1)",
    )
    .bind(subject)
    .execute(&pool)
    .await
    .unwrap();

    assert!(
        store
            .find_baseline_for(subject, "base", "develop", Some(ts(2_000)), "wk1")
            .await
            .unwrap()
            .is_none(),
        "a different measurement environment must disable comparison"
    );

    sqlx::query(
        "UPDATE task_submission
            SET assigned_measurement_profile = 'test-profile'
          WHERE id = (SELECT task_submission_id FROM job WHERE id = $1)",
    )
    .bind(subject)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        store
            .find_baseline_for(subject, "base", "develop", Some(ts(2_000)), "wk1")
            .await
            .unwrap()
            .unwrap()
            .anchor
            .job_id,
        baseline
    );

    sqlx::query(
        "UPDATE task_submission
            SET assigned_measurement_profile = NULL
          WHERE id = (SELECT task_submission_id FROM job WHERE id = $1)",
    )
    .bind(subject)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        store
            .find_baseline_for(subject, "base", "develop", Some(ts(2_000)), "wk1")
            .await
            .unwrap()
            .is_none(),
        "an unstamped subject must remain absolute-only"
    );
}

#[tokio::test]
async fn find_baseline_for_nearest_before_picks_newest_at_or_before_forkpoint() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    // develop timeline: old1 @1000, old2 @2000 (both ≤ fork-point 2500),
    // future @3000 (after). A divergent develop2 @2400 must not leak in.
    let subject =
        seed_completed_baseline(&pool, 100, 10, "old1", "develop", ts(1_000), Some("wk1"), 100)
            .await;
    seed_completed_baseline(&pool, 100, 10, "old2", "develop", ts(2_000), Some("wk1"), 222).await;
    seed_completed_baseline(&pool, 100, 10, "future", "develop", ts(3_000), Some("wk1"), 999).await;
    seed_completed_baseline(&pool, 100, 10, "div", "develop2", ts(2_400), Some("wk1"), 888).await;

    // Fork-point SHA wasn't benchmarked → nearest-before on `develop` ≤ 2500.
    let m = store
        .find_baseline_for(subject, "forkpoint-sha", "develop", Some(ts(2_500)), "wk1")
        .await
        .unwrap()
        .expect("nearest-before baseline");
    assert_eq!(m.anchor.commit, "old2", "newest baseline at/just-before the fork-point");
    assert_eq!(m.anchor.selection, sbgh_postgres::db::BaselineSelection::NearestBefore);
    assert_eq!(m.metric.execution_duration_us, 222);

    // A different ref has no nearest-before here.
    assert!(
        store
            .find_baseline_for(subject, "forkpoint-sha", "release", Some(ts(2_500)), "wk1")
            .await
            .unwrap()
            .is_none(),
    );
}

#[tokio::test]
async fn find_baseline_for_nearest_before_tiebreak_is_deterministic() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    // Two baselines on develop at the SAME commit timestamp → a nearest-before
    // tie. The deterministic ORDER BY must break it on the freshest measurement.
    let a = seed_completed_baseline(&pool, 100, 10, "tieA", "develop", ts(2_000), Some("wk1"), 100)
        .await;
    let b = seed_completed_baseline(&pool, 100, 10, "tieB", "develop", ts(2_000), Some("wk1"), 200)
        .await;
    // Force a known measurement order (record_metric uses DEFAULT NOW()): tieB
    // measured later, so `m.created_at DESC` must pick it.
    sqlx::query("UPDATE job_metric SET created_at = $2 WHERE job_id = $1")
        .bind(a)
        .bind(ts(5_000))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE job_metric SET created_at = $2 WHERE job_id = $1")
        .bind(b)
        .bind(ts(6_000))
        .execute(&pool)
        .await
        .unwrap();

    let m = store
        .find_baseline_for(a, "forkpoint", "develop", Some(ts(2_500)), "wk1")
        .await
        .unwrap()
        .expect("nearest-before with a timestamp tie");
    assert_eq!(m.anchor.job_id, b, "a shared timestamp breaks to the freshest measurement");
    assert_eq!(m.anchor.commit, "tieB");
}

#[tokio::test]
async fn find_baseline_for_ignores_null_workload_key() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    // A pre-v7 baseline with a NULL workload_key must never match.
    let subject =
        seed_completed_baseline(&pool, 100, 10, "abc", "develop", ts(1_000), None, 111).await;

    assert!(
        store
            .find_baseline_for(subject, "abc", "develop", Some(ts(2_000)), "wk1")
            .await
            .unwrap()
            .is_none(),
        "a NULL-workload_key baseline can't serve as a comparison anchor",
    );
}
