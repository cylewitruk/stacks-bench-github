use std::sync::Arc;

use sbgh_core::db::SubmissionStore;
use sbgh_core::models::{
    BuildTarget, GitRefKind, JobIntent, JobSource, QueuedEventDetail, TaskKind,
};
use sbgh_core::submission::{
    BenchmarkPlan, BenchmarkVariant, GithubSubmissionProvenance, PreparedSubmission, ProducerKey,
    ResolvedTaskSource, SchedulingConstraints, SubmissionActor, SubmissionCommand,
    SubmissionDisposition, SubmissionError, SubmissionProvenance, TaskPlan,
};
use sbgh_postgres::db::{Pool, PostgresJobStore, setup_pg_db};
use uuid::Uuid;

async fn seed_install_repo(pool: &Pool) {
    sqlx::query(
        "INSERT INTO allowed_installer
            (github_account_id, account_login, account_type)
         VALUES (100, 'octo', 'organization')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation
            (id, github_account_id, account_login, account_type)
         VALUES (100, 100, 'octo', 'organization')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r')")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo
            (github_installation_id, github_repo_id)
         VALUES (100, 10)",
    )
    .execute(pool)
    .await
    .unwrap();
}

fn source(commit: char) -> ResolvedTaskSource {
    ResolvedTaskSource {
        github_installation_id: 100,
        github_repo_id: 10,
        source: JobSource::Daemon,
        intent: JobIntent::BaselineBenchmark,
        task_kind: TaskKind::Benchmark,
        build_target: BuildTarget::StacksBench,
        git_ref_kind: GitRefKind::Commit,
        git_ref_display: commit.to_string().repeat(40),
        commit: commit.to_string().repeat(40),
        committed_at: None,
        workload_key: Some("workload".into()),
    }
}

fn prepared(key: &str) -> PreparedSubmission {
    let effective_args = vec!["run".into(), "--count".into(), "5000".into()];
    let mut detail = serde_json::to_value(QueuedEventDetail::BranchPush {
        branch: "main".into(),
        trigger_id: 7,
        bench_args: Some("run --count 5000".into()),
    })
    .unwrap();
    detail
        .as_object_mut()
        .unwrap()
        .insert("effective_args".into(), serde_json::json!(effective_args));
    PreparedSubmission {
        command: SubmissionCommand {
            actor: SubmissionActor::System,
            producer_key: ProducerKey {
                namespace: "test".into(),
                key: key.into(),
            },
            constraints: SchedulingConstraints::default(),
            task: TaskPlan::Benchmark(BenchmarkPlan {
                variants: vec![
                    BenchmarkVariant {
                        source: source('a'),
                        requested_run_count: 2,
                        baseline_calibration_id: None,
                    },
                    BenchmarkVariant {
                        source: source('b'),
                        requested_run_count: 2,
                        baseline_calibration_id: None,
                    },
                ],
                effective_args,
            }),
            provenance: SubmissionProvenance {
                queued_event_detail: detail,
                github: None,
                slack: None,
            },
        },
        contract_version: 1,
        request_digest: "1".repeat(64),
    }
}

#[tokio::test]
async fn benchmark_submission_persists_complete_plan_and_one_ready_job() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());

    let receipt = store
        .persist_submission(&prepared("benchmark"))
        .await
        .unwrap();
    assert_eq!(receipt.disposition, SubmissionDisposition::Created);
    assert_eq!(receipt.initial_job_ids.len(), 1);

    let specs: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_spec WHERE task_submission_id = $1")
            .bind(receipt.submission_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job WHERE task_submission_id = $1")
        .bind(receipt.submission_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let steps: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_workflow_step WHERE task_submission_id = $1")
            .bind(receipt.submission_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let capability: String =
        sqlx::query_scalar("SELECT required_capability::text FROM job WHERE id = $1")
            .bind(receipt.initial_job_ids[0])
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((specs, jobs, steps), (2, 1, 6));
    assert_eq!(capability, "benchmark");
}

#[tokio::test]
async fn replay_and_concurrent_first_write_return_one_canonical_submission() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = Arc::new(PostgresJobStore::new(pool.clone()));
    let left = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .persist_submission(&prepared("race"))
                .await
                .unwrap()
        })
    };
    let right = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .persist_submission(&prepared("race"))
                .await
                .unwrap()
        })
    };
    let (left, right) = tokio::join!(left, right);
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.submission_id, right.submission_id);
    assert_eq!(left.initial_job_ids, right.initial_job_ids);
    assert_ne!(left.disposition, right.disposition);

    sqlx::query("UPDATE job SET status = 'failed' WHERE id = $1")
        .bind(left.initial_job_ids[0])
        .execute(&pool)
        .await
        .unwrap();
    let replay = store
        .persist_submission(&prepared("race"))
        .await
        .unwrap();
    assert_eq!(replay.submission_id, left.submission_id);
    assert_eq!(replay.disposition, SubmissionDisposition::AlreadySubmitted);

    let mut conflict = prepared("race");
    conflict.request_digest = "2".repeat(64);
    assert!(matches!(
        store
            .persist_submission(&conflict)
            .await,
        Err(SubmissionError::Conflict { .. })
    ));
}

#[tokio::test]
async fn failed_transaction_leaves_no_partial_aggregate_or_key() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let mut invalid = prepared("rollback");
    let TaskPlan::Benchmark(plan) = &mut invalid.command.task else { unreachable!() };
    plan.variants[1]
        .source
        .github_repo_id = 999_999;

    assert!(
        store
            .persist_submission(&invalid)
            .await
            .is_err()
    );
    let submissions: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_submission_idempotency
          WHERE producer_namespace = 'test' AND producer_key = 'rollback'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(submissions, 0);
}

#[tokio::test]
async fn every_common_transaction_stage_rolls_back_the_whole_aggregate() {
    const STAGES: &[(&str, &str)] = &[
        (
            "task_submission",
            "CREATE TRIGGER reject_submission BEFORE INSERT ON task_submission
             FOR EACH ROW EXECUTE FUNCTION reject_kernel_stage()",
        ),
        (
            "task_spec",
            "CREATE TRIGGER reject_spec BEFORE INSERT ON task_spec
             FOR EACH ROW EXECUTE FUNCTION reject_kernel_stage()",
        ),
        (
            "task_workflow_step",
            "CREATE TRIGGER reject_step BEFORE INSERT ON task_workflow_step
             FOR EACH ROW EXECUTE FUNCTION reject_kernel_stage()",
        ),
        (
            "job",
            "CREATE TRIGGER reject_job BEFORE INSERT ON job
             FOR EACH ROW EXECUTE FUNCTION reject_kernel_stage()",
        ),
        (
            "job_event",
            "CREATE TRIGGER reject_event BEFORE INSERT ON job_event
             FOR EACH ROW EXECUTE FUNCTION reject_kernel_stage()",
        ),
        (
            "task_submission_provenance",
            "CREATE TRIGGER reject_provenance BEFORE INSERT ON task_submission_provenance
             FOR EACH ROW EXECUTE FUNCTION reject_kernel_stage()",
        ),
        (
            "task_submission_idempotency",
            "CREATE TRIGGER reject_idempotency BEFORE INSERT ON task_submission_idempotency
             FOR EACH ROW EXECUTE FUNCTION reject_kernel_stage()",
        ),
    ];

    for (table, trigger_sql) in STAGES {
        let (_db, pool) = setup_pg_db().await;
        seed_install_repo(&pool).await;
        sqlx::query(
            "CREATE FUNCTION reject_kernel_stage() RETURNS TRIGGER
             LANGUAGE plpgsql AS $$
             BEGIN
                 RAISE EXCEPTION 'injected kernel-stage failure';
             END
             $$",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(*trigger_sql)
            .execute(&pool)
            .await
            .unwrap();

        let store = PostgresJobStore::new(pool.clone());
        assert!(
            store
                .persist_submission(&prepared(table))
                .await
                .is_err(),
            "{table} failure must escape the transaction"
        );
        let persisted: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_submission WHERE contract_version = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(persisted, 0, "{table} failure left a partial aggregate");
    }
}

#[tokio::test]
async fn late_provenance_failure_rolls_back_plan_job_and_events() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let mut invalid = prepared("late-rollback");
    invalid
        .command
        .provenance
        .github = Some(GithubSubmissionProvenance {
        webhook_id: 999_999,
        triggering_user_id: None,
        pull_request_id: None,
        triggering_comment_id: None,
    });

    assert!(
        store
            .persist_submission(&invalid)
            .await
            .is_err()
    );
    let aggregates: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_submission_idempotency
          WHERE producer_namespace = 'test' AND producer_key = 'late-rollback'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let orphan_jobs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM job
          WHERE git_commit_hash IN ($1, $2)",
    )
    .bind("a".repeat(40))
    .bind("b".repeat(40))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((aggregates, orphan_jobs), (0, 0));
}

#[tokio::test]
async fn valid_worker_constraint_persists_without_an_assignment() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let worker_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO worker_registry
            (worker_id, identity_uri, display_name, allowed_capabilities)
         VALUES ($1, $2, 'offline', ARRAY['benchmark']::worker_capability[])",
    )
    .bind(worker_id)
    .bind(format!("urn:sbgh:worker:{worker_id}"))
    .execute(&pool)
    .await
    .unwrap();
    let store = PostgresJobStore::new(pool.clone());
    let mut command = prepared("constrained");
    command
        .command
        .constraints
        .required_worker_id = Some(worker_id);
    let receipt = store
        .persist_submission(&command)
        .await
        .unwrap();

    let placement: (Option<Uuid>, Option<Uuid>) = sqlx::query_as(
        "SELECT required_worker_id, assigned_worker_id
           FROM task_submission WHERE id = $1",
    )
    .bind(receipt.submission_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(placement, (Some(worker_id), None));
}
