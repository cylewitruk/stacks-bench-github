//! 0037: every current job is a singleton `benchmark_group -> benchmark_spec
//! -> job(run)` without changing creation behavior.

use sbgh_core::db::{JobCreationOutcome, JobStore, Pool, PostgresJobStore, setup_pg_db};
use sbgh_core::models::{
    BenchmarkStepKind, BuildTarget, GitRefKind, JobAxes, JobCreationRequest, JobIntent, JobKind,
    JobSource, NewJob, QueuedEventDetail, TaskKind, TriggerKind,
};
use uuid::Uuid;

fn new_job(trigger_kind: TriggerKind, job_kind: JobKind) -> NewJob {
    NewJob {
        github_installation_id: 100,
        github_repo_id: 10,
        axes: JobAxes::from_legacy(trigger_kind, job_kind),
        git_ref_kind: GitRefKind::Branch,
        git_ref_display: "develop".into(),
        git_commit_hash: Some("0123456789abcdef0123456789abcdef01234567".into()),
        git_committed_at: None,
        workload_key: Some("workload-key".into()),
    }
}

async fn seed_install_repo(pool: &Pool) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         (100, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES (100, 100, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES (10, 'o', 'r') ON CONFLICT DO NOTHING",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         (100, 10) ON CONFLICT DO NOTHING",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn assert_singleton_model(
    pool: &Pool,
    job_id: Uuid,
    group_id: Uuid,
    spec_id: Uuid,
    expected_axes: JobAxes,
) {
    let (source, intent, artifact_prefix, host_key): (
        JobSource,
        JobIntent,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT source, intent, artifact_prefix, host_key FROM benchmark_group WHERE id = $1",
    )
    .bind(group_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(source, expected_axes.source);
    assert_eq!(intent, expected_axes.intent);
    assert_eq!(artifact_prefix, group_id.to_string());
    assert_eq!(host_key, None);

    let (
        spec_group_id,
        spec_index,
        task_kind,
        build_target,
        git_ref_kind,
        git_ref_display,
        commit,
        workload_key,
    ): (Uuid, i32, TaskKind, BuildTarget, GitRefKind, String, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT benchmark_group_id, spec_index, task_kind, build_target, git_ref_kind, \
             git_ref_display, git_commit_hash, workload_key FROM benchmark_spec WHERE id = $1",
        )
        .bind(spec_id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(spec_group_id, group_id);
    assert_eq!(spec_index, 0);
    assert_eq!(task_kind, expected_axes.task_kind);
    assert_eq!(build_target, expected_axes.build_target);
    assert_eq!(git_ref_kind, GitRefKind::Branch);
    assert_eq!(git_ref_display, "develop");
    assert_eq!(commit.as_deref(), Some("0123456789abcdef0123456789abcdef01234567"));
    assert_eq!(workload_key.as_deref(), Some("workload-key"));

    let (run_group_id, run_spec_id, run_index): (Uuid, Uuid, i32) = sqlx::query_as(
        "SELECT benchmark_group_id, benchmark_spec_id, benchmark_run_index FROM job WHERE id = $1",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(run_group_id, group_id);
    assert_eq!(run_spec_id, spec_id);
    assert_eq!(run_index, 0);

    let steps: Vec<(i32, BenchmarkStepKind)> = sqlx::query_as(
        "SELECT step_index, step_kind FROM benchmark_workflow_step WHERE benchmark_group_id = $1 \
         ORDER BY step_index",
    )
    .bind(group_id)
    .fetch_all(pool)
    .await
    .unwrap();
    if expected_axes.task_kind == TaskKind::BuildOnly {
        assert_eq!(steps, vec![(0, BenchmarkStepKind::Build)]);
    } else {
        assert_eq!(steps, vec![(0, BenchmarkStepKind::Build), (1, BenchmarkStepKind::Run)]);
    }
}

#[tokio::test]
async fn insert_job_creates_singleton_group_spec_and_run() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let new = new_job(TriggerKind::BranchPush, JobKind::Baseline);

    let job = store
        .insert_job(&new)
        .await
        .unwrap();

    assert_singleton_model(&pool, job.id, job.benchmark_group_id, job.benchmark_spec_id, new.axes)
        .await;
}

#[tokio::test]
async fn create_job_with_links_creates_singleton_group_spec_and_run() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('group-links-1', 'issue_comment', 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let new = new_job(TriggerKind::PrComment, JobKind::AdHoc);

    let outcome = store
        .create_job_with_links(&JobCreationRequest {
            new_job: new.clone(),
            github_webhook_id: webhook_id,
            triggering_user_id: None,
            pull_request_link: None,
            queued_event_detail: None,
        })
        .await
        .unwrap();
    let JobCreationOutcome::Created(created) = outcome else {
        panic!("expected a fresh job");
    };

    assert_singleton_model(
        &pool,
        created.job.id,
        created.job.benchmark_group_id,
        created.job.benchmark_spec_id,
        new.axes,
    )
    .await;
}

#[tokio::test]
async fn create_unlinked_build_only_job_creates_build_only_singleton_group() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let detail = serde_json::to_value(QueuedEventDetail::CacheWarm {
        trigger_id: 42,
        git_ref: "sb-integration/3.4.0.0.3".into(),
        commit: "0123456789abcdef0123456789abcdef01234567".into(),
        build_target: BuildTarget::StacksBench,
    })
    .unwrap();
    let new = NewJob {
        github_installation_id: 100,
        github_repo_id: 10,
        axes: JobAxes {
            source: JobSource::Daemon,
            intent: JobIntent::CacheWarm,
            task_kind: TaskKind::BuildOnly,
            build_target: BuildTarget::StacksBench,
        },
        git_ref_kind: GitRefKind::Branch,
        git_ref_display: "develop".into(),
        git_commit_hash: Some("0123456789abcdef0123456789abcdef01234567".into()),
        git_committed_at: None,
        workload_key: Some("workload-key".into()),
    };

    let job = store
        .create_unlinked_job(&new, &detail)
        .await
        .unwrap();

    assert_singleton_model(&pool, job.id, job.benchmark_group_id, job.benchmark_spec_id, new.axes)
        .await;
}
