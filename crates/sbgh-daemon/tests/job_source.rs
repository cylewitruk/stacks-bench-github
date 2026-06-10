//! Slice 10 integration test: the new-schema `JobSource` drives a job
//! end-to-end against real Postgres — `claim → start_running → complete`
//! (and `fail`), persisting the durable `job_event` / `job_metric` /
//! `job_result` the slice 11 cutover depends on. This is the highest-
//! confidence proof that the new execution backend works before it
//! becomes the production source.

use std::sync::Arc;

use sbgh_core::db::{
    JobCreationOutcome, JobStore, Pool, PostgresJobStore, PostgresPullRequestStore,
    PostgresRepoStore, setup_pg_db,
};
use sbgh_core::models::{
    GitRefKind, JobCreationRequest, JobKind, NewJob, NewPullRequestLink, QueuedEventDetail,
    TriggerKind,
};

// Daemon is a bin-only crate; pull in the modules under test via
// path include (same pattern as processor_e2e). `job_source` references
// `crate::bench_summary` + `crate::artifact_store`, so all must be declared at
// the test root.
// `artifact_store.rs` carries its own `#![allow(dead_code)]`, so no outer
// attribute here (a second one trips `clippy::duplicated_attributes`).
#[path = "../src/artifact_store.rs"]
mod artifact_store;
#[path = "../src/bench_summary.rs"]
#[allow(dead_code)]
mod bench_summary;
#[path = "../src/comparison.rs"]
#[allow(dead_code)]
mod comparison;
#[path = "../src/job_source.rs"]
#[allow(dead_code)]
mod job_source;

use job_source::{JobSource, ProgressTarget, RunnableJobStore};

/// Seed install + repo + membership so a `job` row's composite FK and
/// the `RepoStore` owner/name lookup both resolve. Returns the seeded
/// webhook id (FK target for `create_job_with_links`).
async fn seed(pool: &Pool, install: i64, repo: i64) -> i64 {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         ($1, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES ($1, $1, 'octo', 'organization')",
    )
    .bind(install)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES ($1, 'octo', 'core')")
        .bind(repo)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         ($1, $2)",
    )
    .bind(install)
    .bind(repo)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('v2-source-1', 'push', 0) RETURNING id",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Create a queued `branch_push` baseline job (+ webhook link + queued
/// event carrying bench_args provenance) via the slice-9 atomic path.
async fn enqueue_branch_push(store: &PostgresJobStore, webhook_id: i64) {
    let detail = serde_json::to_value(QueuedEventDetail::BranchPush {
        branch: "develop".into(),
        trigger_id: 1,
        bench_args: Some("--iters=5".into()),
    })
    .unwrap();
    let outcome = store
        .create_job_with_links(&JobCreationRequest {
            new_job: NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                job_kind: JobKind::Baseline,
                trigger_kind: TriggerKind::BranchPush,
                git_ref_kind: GitRefKind::Branch,
                git_ref_display: "develop".into(),
                git_commit_hash: Some("pushsha".into()),
                git_committed_at: None,
                workload_key: None,
            },
            github_webhook_id: webhook_id,
            triggering_user_id: None,
            pull_request_link: None,
            queued_event_detail: Some(detail),
        })
        .await
        .unwrap();
    assert!(matches!(outcome, JobCreationOutcome::Created(_)));
}

/// Create a queued `slack_adhoc` ad-hoc job carrying a `SlackAdhoc` queued
/// event (channel/message_ts reporting provenance + the resolved workload).
async fn enqueue_slack_adhoc(store: &PostgresJobStore, webhook_id: i64) {
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "5".into()],
    })
    .unwrap();
    let outcome = store
        .create_job_with_links(&JobCreationRequest {
            new_job: NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                job_kind: JobKind::AdHoc,
                trigger_kind: TriggerKind::SlackAdhoc,
                git_ref_kind: GitRefKind::Branch,
                git_ref_display: "develop".into(),
                git_commit_hash: Some("revsha".into()),
                git_committed_at: None,
                workload_key: None,
            },
            github_webhook_id: webhook_id,
            triggering_user_id: None,
            pull_request_link: None,
            queued_event_detail: Some(detail),
        })
        .await
        .unwrap();
    assert!(matches!(outcome, JobCreationOutcome::Created(_)));
}

/// Codex acceptance point (v5): a `slack_adhoc` job MUST assemble as
/// `ProgressTarget::Slack` (channel/message_ts from its `SlackAdhoc` queued
/// detail), never fall through to a commit check, and carry the resolved
/// workload as `bench_args`.
#[tokio::test]
async fn slack_adhoc_job_assembles_as_slack_progress() {
    let (_db, pool) = setup_pg_db().await;
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    enqueue_slack_adhoc(&store, webhook_id).await;

    let archive_root = tempfile::tempdir().unwrap();
    let source = JobSource::new(
        store.clone(),
        Arc::new(PostgresRepoStore::new(pool.clone())),
        Arc::new(PostgresPullRequestStore::new(pool.clone())),
        Arc::new(artifact_store::LocalFsStore::new(
            archive_root
                .path()
                .to_path_buf(),
        )),
    );

    let job = source
        .claim_next()
        .await
        .unwrap()
        .expect("one queued slack job");
    match job.progress {
        ProgressTarget::Slack { channel, message_ts } => {
            assert_eq!(channel, "C123");
            assert_eq!(message_ts, "1700000000.000100");
        }
        other => panic!("slack_adhoc must assemble as Slack, got {other:?}"),
    }
    assert_eq!(job.bench_args, vec!["--block", "184231", "--repetitions", "5"]);
}

#[tokio::test]
async fn v2_source_claims_assembles_and_completes_with_metric_and_result() {
    let (_db, pool) = setup_pg_db().await;
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    enqueue_branch_push(&store, webhook_id).await;

    let archive_root = tempfile::tempdir().unwrap();
    let source = JobSource::new(
        store.clone(),
        Arc::new(PostgresRepoStore::new(pool.clone())),
        Arc::new(PostgresPullRequestStore::new(pool.clone())),
        Arc::new(artifact_store::LocalFsStore::new(
            archive_root
                .path()
                .to_path_buf(),
        )),
    );

    // Claim assembles the execution view from across the schema.
    let job = source
        .claim_next()
        .await
        .unwrap()
        .expect("one queued job");
    assert_eq!(job.repository, "octo/core", "owner/name resolved via RepoStore");
    assert_eq!(job.commit, "pushsha");
    assert_eq!(job.git_ref_display, "develop");
    assert_eq!(job.bench_args, vec!["--iters=5"], "bench_args resolved from queued event detail");
    assert!(
        matches!(job.progress, ProgressTarget::CommitCheck { check_run_id: None }),
        "a baseline (push) job reports on a commit check"
    );
    assert!(job.claim_token.is_some());

    // Transition to running (commit already resolved → None).
    source
        .start_running(&job, None)
        .await
        .unwrap();
    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "running");

    // Write a run.json the summary points at, so complete() promotes a
    // full job_metric AND stores the raw run_json.
    // Place the run.json under the store root at the key the summary references
    // (Decision 0002 — `complete` resolves it via `ArtifactStore::get`).
    let run_json_key = "job/run.json";
    let run_json_path = archive_root
        .path()
        .join(run_json_key);
    std::fs::create_dir_all(
        run_json_path
            .parent()
            .unwrap(),
    )
    .unwrap();
    std::fs::write(
        &run_json_path,
        r#"{"success":true,"duration_secs":2200.0,"data":{"measured_blocks":4000,
        "warmup_blocks":1000,"duration_secs":1900.0,"summary":{"total_duration_us":100000000,
        "setup_duration_us":9000000,"execution_duration_us":72000000,"commit_duration_us":18000000,
        "transactions":12345,"clarity_runtime":9876543,"write_length":89000000,
        "read_length":245000000}}}"#,
    )
    .unwrap();
    let summary = serde_json::json!({
        "archive_dir": "/var/lib/sbgh/results/job",
        "run_json_archived_path": run_json_key,
        "finish_reason": "phase_done",
    });

    source
        .complete(&job, &summary)
        .await
        .unwrap();

    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "completed");

    // job_result: archive dir + the raw run_json content.
    let (archive_dir, run_json_present): (String, bool) = sqlx::query_as(
        "SELECT archive_dir, run_json IS NOT NULL FROM job_result WHERE job_id = $1",
    )
    .bind(job.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(archive_dir, "/var/lib/sbgh/results/job");
    assert!(run_json_present, "raw run.json content stored");

    // job_metric: fully populated → promoted. envelope = 2200s, replay = 1900s.
    let (txns, envelope_us, replay_us): (i64, i64, i64) = sqlx::query_as(
        "SELECT transactions, envelope_duration_us, replay_duration_us FROM job_metric WHERE \
         job_id = $1",
    )
    .bind(job.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(txns, 12345);
    assert_eq!(envelope_us, 2_200_000_000);
    assert_eq!(replay_us, 1_900_000_000);

    // Terminal timeline event.
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM job_event WHERE job_id = $1 AND event_kind = 'completed'",
    )
    .bind(job.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 1);
}

/// 4C-2: `load_runnable` assembles the same execution view as `claim_next`
/// but **read-only** — no claim taken (`claim_token = None`) and the row's
/// status is untouched. Used by orphan recovery to conclude a stuck check.
#[tokio::test]
async fn v2_source_load_runnable_assembles_view_without_claiming() {
    let (_db, pool) = setup_pg_db().await;
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    enqueue_branch_push(&store, webhook_id).await;
    let source = JobSource::new(
        store.clone(),
        Arc::new(PostgresRepoStore::new(pool.clone())),
        Arc::new(PostgresPullRequestStore::new(pool.clone())),
        Arc::new(artifact_store::LocalFsStore::new(std::path::PathBuf::from(
            "/var/lib/sbgh/results",
        ))),
    );

    // Claim + run so there's a `running` row to load (the orphan case).
    let claimed = source
        .claim_next()
        .await
        .unwrap()
        .expect("one queued job");
    source
        .start_running(&claimed, None)
        .await
        .unwrap();

    let loaded = source
        .load_runnable(claimed.id)
        .await
        .unwrap()
        .expect("the running row loads");
    // Same assembled view…
    assert_eq!(loaded.id, claimed.id);
    assert_eq!(loaded.repository, "octo/core");
    assert_eq!(loaded.commit, "pushsha");
    assert!(matches!(loaded.progress, ProgressTarget::CommitCheck { check_run_id: None }));
    // …but read-only: no claim, and the status stayed `running`.
    assert!(loaded.claim_token.is_none(), "load_runnable takes no claim");
    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(claimed.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "running", "load_runnable must not transition the row");

    // A missing id loads nothing.
    assert!(
        source
            .load_runnable(uuid::Uuid::new_v4())
            .await
            .unwrap()
            .is_none()
    );
}

/// 4C-2 (Codex test-gap): the product value of `load_runnable` is surfacing an
/// **existing** Check Run id + url so orphan recovery can conclude the stuck
/// check. A PR job with a recorded `check_run_created` event, left `running`
/// (the orphan state), loads with `check_run_id`/`check_run_url` populated.
#[tokio::test]
async fn v2_source_load_runnable_surfaces_existing_check_for_conclusion() {
    let (_db, pool) = setup_pg_db().await;
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));

    // Author + PR row (the PR link's FK target).
    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES (42, 'alice', 'user')")
        .execute(&pool)
        .await
        .unwrap();
    let pr_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_pull_request (target_github_repo_id, source_github_repo_id, \
         pr_number, title, author_github_user_id) VALUES (10, 10, 7, 't', 42) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let created = store
        .create_job_with_links(&JobCreationRequest {
            new_job: NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                job_kind: JobKind::AdHoc,
                trigger_kind: TriggerKind::PrComment,
                git_ref_kind: GitRefKind::Branch,
                git_ref_display: "feat".into(),
                git_commit_hash: Some("headsha".into()),
                git_committed_at: None,
                workload_key: None,
            },
            github_webhook_id: webhook_id,
            triggering_user_id: Some(42),
            pull_request_link: Some(NewPullRequestLink {
                github_pull_request_id: pr_id,
                triggering_comment_id: Some(9001),
            }),
            queued_event_detail: None,
        })
        .await
        .unwrap();
    let JobCreationOutcome::Created(created) = created else {
        panic!("expected Created");
    };

    let source = JobSource::new(
        store.clone(),
        Arc::new(PostgresRepoStore::new(pool.clone())),
        Arc::new(PostgresPullRequestStore::new(pool.clone())),
        Arc::new(artifact_store::LocalFsStore::new(std::path::PathBuf::from(
            "/var/lib/sbgh/results",
        ))),
    );

    // Claim, record a created check (id + url), and mark running — the orphan
    // state.
    let job = source
        .claim_next()
        .await
        .unwrap()
        .unwrap();
    source
        .set_check_run(&job, 4242, Some("https://github.test/checks/4242"))
        .await
        .unwrap();
    source
        .start_running(&job, None)
        .await
        .unwrap();

    // load_runnable surfaces the existing check id + url for conclusion.
    let loaded = source
        .load_runnable(created.job.id)
        .await
        .unwrap()
        .expect("the running row loads");
    match loaded.progress {
        ProgressTarget::PullRequest {
            check_run_id, check_run_url, ..
        } => {
            assert_eq!(check_run_id, Some(4242), "existing check id surfaced for conclusion");
            assert_eq!(
                check_run_url.as_deref(),
                Some("https://github.test/checks/4242"),
                "existing check url surfaced",
            );
        }
        other => panic!("expected PullRequest progress, got {other:?}"),
    }
    assert!(loaded.claim_token.is_none(), "still read-only");
}

/// Phase 5: `list_queued` returns every `queued` job in **claim order**
/// (`created_at, id`), as read-only views (no claim taken, rows stay queued) —
/// the input to the coordinator's queue-position reporting.
#[tokio::test]
async fn v2_source_list_queued_returns_queued_in_claim_order_readonly() {
    let (_db, pool) = setup_pg_db().await;
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    enqueue_branch_push(&store, webhook_id).await;
    // A second queued job via a distinct webhook.
    let wh2: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('v2-source-2', 'push', 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    enqueue_branch_push(&store, wh2).await;

    let source = JobSource::new(
        store.clone(),
        Arc::new(PostgresRepoStore::new(pool.clone())),
        Arc::new(PostgresPullRequestStore::new(pool.clone())),
        Arc::new(artifact_store::LocalFsStore::new(std::path::PathBuf::from(
            "/var/lib/sbgh/results",
        ))),
    );

    // Force a deterministic claim order: make one row strictly older.
    let ids: Vec<uuid::Uuid> =
        sqlx::query_scalar("SELECT id FROM job WHERE status = 'queued' ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(ids.len(), 2);
    sqlx::query("UPDATE job SET created_at = NOW() - INTERVAL '1 hour' WHERE id = $1")
        .bind(ids[0])
        .execute(&pool)
        .await
        .unwrap();

    let queued = source
        .list_queued()
        .await
        .unwrap();
    assert_eq!(queued.len(), 2, "both queued jobs listed");
    assert_eq!(queued[0].id, ids[0], "older job first (matches claim order)");
    assert_eq!(queued[1].id, ids[1]);
    assert!(
        queued
            .iter()
            .all(|j| j.claim_token.is_none()),
        "read-only — no claim taken"
    );

    // Read-only: both rows are still queued.
    let still_queued: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job WHERE status = 'queued'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(still_queued, 2, "list_queued must not transition any row");
}

#[tokio::test]
async fn v2_source_fail_records_event_and_forensics_result() {
    let (_db, pool) = setup_pg_db().await;
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    enqueue_branch_push(&store, webhook_id).await;
    let source = JobSource::new(
        store.clone(),
        Arc::new(PostgresRepoStore::new(pool.clone())),
        Arc::new(PostgresPullRequestStore::new(pool.clone())),
        Arc::new(artifact_store::LocalFsStore::new(std::path::PathBuf::from(
            "/var/lib/sbgh/results",
        ))),
    );

    let job = source
        .claim_next()
        .await
        .unwrap()
        .unwrap();
    source
        .start_running(&job, None)
        .await
        .unwrap();

    let summary = serde_json::json!({
        "archive_dir": "/var/lib/sbgh/results/failed-job",
        "finish_reason": "shut_off",
    });
    source
        .fail(&job, "VM powered off before phase=done", Some(&summary))
        .await
        .unwrap();

    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "failed");
    let remark: Option<String> = sqlx::query_scalar(
        "SELECT remark FROM job_event WHERE job_id = $1 AND event_kind = 'failed'",
    )
    .bind(job.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(remark.as_deref(), Some("VM powered off before phase=done"));
    // Forensics result row (archive dir, no run_json) recorded on failure.
    let archive_dir: String =
        sqlx::query_scalar("SELECT archive_dir FROM job_result WHERE job_id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(archive_dir, "/var/lib/sbgh/results/failed-job");
}

#[tokio::test]
async fn v2_source_sweeps_stuck_claimed_jobs() {
    // Review fix (High): a job stranded in `claimed` (daemon
    // crashed / preflight errored between claim and start_running) must
    // be recoverable. The runner calls `sweep_stuck_claims` each loop;
    // here we exercise the JobSource passthrough directly.
    let (_db, pool) = setup_pg_db().await;
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    enqueue_branch_push(&store, webhook_id).await;
    let source = JobSource::new(
        store.clone(),
        Arc::new(PostgresRepoStore::new(pool.clone())),
        Arc::new(PostgresPullRequestStore::new(pool.clone())),
        Arc::new(artifact_store::LocalFsStore::new(std::path::PathBuf::from(
            "/var/lib/sbgh/results",
        ))),
    );

    // Claim → claimed, then simulate a crash by NOT calling start_running
    // and backdating claimed_at past the lease.
    let job = source
        .claim_next()
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE job SET claimed_at = NOW() - INTERVAL '1 hour' WHERE id = $1")
        .bind(job.id)
        .execute(&pool)
        .await
        .unwrap();

    let recovered = source
        .sweep_stuck_claims(chrono::Duration::minutes(5))
        .await
        .unwrap();
    assert_eq!(recovered, 1, "the stuck claimed job is recovered");

    let status: String = sqlx::query_scalar("SELECT status::text FROM job WHERE id = $1")
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "queued", "swept back to queued for re-claim");
}

#[tokio::test]
async fn v2_source_pr_comment_job_assembles_pr_progress_and_records_comment_id() {
    // A `pr_comment` job carries a PR link, so claim_next must assemble
    // `PullRequest { pr_number, .. }` (resolved via PullRequestStore) — not a
    // `CommitCheck`. set_comment_id records a `comment_posted` event; a
    // re-claim reads that id back so the job edits the existing comment instead
    // of double-posting.
    let (_db, pool) = setup_pg_db().await;
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));

    // Author + PR row (the PR link's FK target).
    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES (42, 'alice', 'user')")
        .execute(&pool)
        .await
        .unwrap();
    let pr_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_pull_request (target_github_repo_id, source_github_repo_id, \
         pr_number, title, author_github_user_id) VALUES (10, 10, 7, 't', 42) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // A pr_comment job linked to that PR.
    let created = store
        .create_job_with_links(&JobCreationRequest {
            new_job: NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                job_kind: JobKind::AdHoc,
                trigger_kind: TriggerKind::PrComment,
                git_ref_kind: GitRefKind::Branch,
                git_ref_display: "feat".into(),
                git_commit_hash: Some("headsha".into()),
                git_committed_at: None,
                workload_key: None,
            },
            github_webhook_id: webhook_id,
            triggering_user_id: Some(42),
            pull_request_link: Some(NewPullRequestLink {
                github_pull_request_id: pr_id,
                triggering_comment_id: Some(9001),
            }),
            queued_event_detail: None,
        })
        .await
        .unwrap();
    let JobCreationOutcome::Created(created) = created else {
        panic!("expected Created");
    };
    let job_id = created.job.id;

    let source = JobSource::new(
        store.clone(),
        Arc::new(PostgresRepoStore::new(pool.clone())),
        Arc::new(PostgresPullRequestStore::new(pool.clone())),
        Arc::new(artifact_store::LocalFsStore::new(std::path::PathBuf::from(
            "/var/lib/sbgh/results",
        ))),
    );

    // Claim assembles PR progress (pr_number=7, no comment yet).
    let job = source
        .claim_next()
        .await
        .unwrap()
        .unwrap();
    match job.progress {
        ProgressTarget::PullRequest {
            pr_number,
            comment_id,
            check_run_id,
            ..
        } => {
            assert_eq!(pr_number, 7, "pr_number resolved from the PR link");
            assert!(comment_id.is_none(), "no comment posted yet on first claim");
            assert!(check_run_id.is_none(), "no check created yet on first claim");
        }
        other => panic!("pr_comment job must assemble PullRequest progress, got {other:?}"),
    }

    // Record the posted comment id → a comment_posted event lands.
    source
        .set_comment_id(&job, 555)
        .await
        .unwrap();
    let recorded: i64 = sqlx::query_scalar(
        "SELECT github_comment_id FROM job_event WHERE job_id = $1 AND event_kind = \
         'comment_posted'",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(recorded, 555);

    // Simulate a re-claim: sweep the (still-claimed) job back to queued,
    // then claim again — the recorded comment id must be read back so the
    // job edits rather than re-posts.
    sqlx::query(
        "UPDATE job SET status = 'queued', claim_token = NULL, claimed_at = NULL WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .unwrap();
    let reclaimed = source
        .claim_next()
        .await
        .unwrap()
        .unwrap();
    match reclaimed.progress {
        ProgressTarget::PullRequest { comment_id, .. } => {
            assert_eq!(comment_id, Some(555), "re-claim reads back the posted comment id");
        }
        other => panic!("expected PullRequest on re-claim, got {other:?}"),
    }
}
