//! Slice 10 integration test: the new-schema `JobV2Source` drives a job
//! end-to-end against real Postgres — `claim → start_running → complete`
//! (and `fail`), persisting the durable `job_event` / `job_metric` /
//! `job_result` the slice 11 cutover depends on. This is the highest-
//! confidence proof that the new execution backend works before it
//! becomes the production source.

use std::io::Write;
use std::sync::Arc;

use sbgh_core::db::{
    JobCreationOutcome, JobV2Store, Pool, PostgresJobV2Store, PostgresRepoStore, setup_pg,
};
use sbgh_core::models::{
    GitRefKind, JobCreationRequest, JobKind, NewJobV2, QueuedEventDetail, TriggerKind,
};

// Orchestrator is a bin-only crate; pull in the modules under test via
// path include (same pattern as processor_e2e). `job_source` references
// `crate::bench_summary`, so both must be declared at the test root.
#[path = "../src/bench_summary.rs"]
#[allow(dead_code)]
mod bench_summary;
#[path = "../src/job_source.rs"]
#[allow(dead_code)]
mod job_source;

use job_source::{JobV2Source, ProgressTarget, RunnableJobStore};

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
async fn enqueue_branch_push(store: &PostgresJobV2Store, webhook_id: i64) {
    let detail = serde_json::to_value(QueuedEventDetail::BranchPush {
        branch: "develop".into(),
        trigger_id: 1,
        bench_args: Some("--iters=5".into()),
    })
    .unwrap();
    let outcome = store
        .create_job_with_links(&JobCreationRequest {
            new_job: NewJobV2 {
                github_installation_id: 100,
                github_repo_id: 10,
                job_kind: JobKind::Baseline,
                trigger_kind: TriggerKind::BranchPush,
                git_ref_kind: GitRefKind::Branch,
                git_ref_display: "develop".into(),
                git_commit_hash: Some("pushsha".into()),
                git_committed_at: None,
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

#[tokio::test]
async fn v2_source_claims_assembles_and_completes_with_metric_and_result() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobV2Store::new(pool.clone()));
    enqueue_branch_push(&store, webhook_id).await;

    let source = JobV2Source::new(store.clone(), Arc::new(PostgresRepoStore::new(pool.clone())));

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
    assert!(matches!(job.progress, ProgressTarget::LogOnly), "new-schema progress is log-only");
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
    let mut run_json = tempfile::NamedTempFile::new().unwrap();
    write!(
        run_json,
        r#"{{"success":true,"duration_secs":2200.0,"data":{{"measured_blocks":4000,
        "warmup_blocks":1000,"duration_secs":1900.0,"summary":{{"total_duration_us":100000000,
        "setup_duration_us":9000000,"execution_duration_us":72000000,"commit_duration_us":18000000,
        "transactions":12345,"clarity_runtime":9876543,"write_length":89000000,
        "read_length":245000000}}}}}}"#
    )
    .unwrap();
    let summary = serde_json::json!({
        "archive_dir": "/var/lib/sbgh/results/job",
        "run_json_archived_path": run_json.path().to_str().unwrap(),
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

#[tokio::test]
async fn v2_source_fail_records_event_and_forensics_result() {
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobV2Store::new(pool.clone()));
    enqueue_branch_push(&store, webhook_id).await;
    let source = JobV2Source::new(store.clone(), Arc::new(PostgresRepoStore::new(pool.clone())));

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
    // Review fix (High): a job stranded in `claimed` (orchestrator
    // crashed / preflight errored between claim and start_running) must
    // be recoverable. The runner calls `sweep_stuck_claims` each loop;
    // here we exercise the JobV2Source passthrough directly.
    let Some((_c, pool)) = setup_pg().await else {
        return;
    };
    let webhook_id = seed(&pool, 100, 10).await;
    let store = Arc::new(PostgresJobV2Store::new(pool.clone()));
    enqueue_branch_push(&store, webhook_id).await;
    let source = JobV2Source::new(store.clone(), Arc::new(PostgresRepoStore::new(pool.clone())));

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
