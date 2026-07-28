//! Every current job belongs to a `task_submission -> task_spec -> job(run)`
//! aggregate without changing creation behavior.

use sbgh_core::models::{
    BuildTarget, GitRefKind, JobAxes, JobCreationRequest, JobIntent, JobKind, JobSource, JobStatus,
    NewJob, QueuedEventDetail, SubmissionStepKind, TaskKind, TriggerKind,
};
use sbgh_postgres::Db;
use sbgh_postgres::db::{
    JobCreationOutcome, JobStore, NewBenchmarkSpec, Pool, PostgresJobStore, setup_pg_db,
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
    submission_id: Uuid,
    spec_id: Uuid,
    expected_axes: JobAxes,
) {
    type PersistedSpec = (
        Uuid,
        i32,
        i32,
        Db<TaskKind>,
        Db<BuildTarget>,
        Db<GitRefKind>,
        String,
        Option<String>,
        Option<String>,
    );

    let (source, intent, artifact_prefix, host_key): (
        Db<JobSource>,
        Db<JobIntent>,
        String,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT source, intent, artifact_prefix, host_key FROM task_submission WHERE id = $1",
    )
    .bind(submission_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(source.0, expected_axes.source);
    assert_eq!(intent.0, expected_axes.intent);
    assert_eq!(artifact_prefix, submission_id.to_string());
    assert_eq!(host_key, None);

    let (
        spec_submission_id,
        spec_index,
        requested_run_count,
        task_kind,
        build_target,
        git_ref_kind,
        git_ref_display,
        commit,
        workload_key,
    ): PersistedSpec = sqlx::query_as(
        "SELECT task_submission_id, spec_index, requested_run_count, task_kind, build_target, \
         git_ref_kind, git_ref_display, git_commit_hash, workload_key FROM task_spec WHERE \
         id = $1",
    )
    .bind(spec_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(spec_submission_id, submission_id);
    assert_eq!(spec_index, 0);
    assert_eq!(requested_run_count, 1);
    assert_eq!(task_kind.0, expected_axes.task_kind);
    assert_eq!(build_target.0, expected_axes.build_target);
    assert_eq!(git_ref_kind.0, GitRefKind::Branch);
    assert_eq!(git_ref_display, "develop");
    assert_eq!(commit.as_deref(), Some("0123456789abcdef0123456789abcdef01234567"));
    assert_eq!(workload_key.as_deref(), Some("workload-key"));

    let (run_submission_id, run_spec_id, run_index): (Uuid, Uuid, i32) = sqlx::query_as(
        "SELECT task_submission_id, task_spec_id, task_run_index FROM job WHERE id = $1",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(run_submission_id, submission_id);
    assert_eq!(run_spec_id, spec_id);
    assert_eq!(run_index, 0);

    let steps: Vec<(i32, Db<SubmissionStepKind>)> = sqlx::query_as(
        "SELECT step_index, step_kind FROM task_workflow_step WHERE task_submission_id = $1 \
         ORDER BY step_index",
    )
    .bind(submission_id)
    .fetch_all(pool)
    .await
    .unwrap();
    if expected_axes.task_kind == TaskKind::BuildOnly {
        assert_eq!(steps, vec![(0, Db(SubmissionStepKind::Build))]);
    } else {
        assert_eq!(
            steps,
            vec![(0, Db(SubmissionStepKind::Build)), (1, Db(SubmissionStepKind::Run))]
        );
    }
}

#[tokio::test]
async fn slack_unlinked_job_persists_requested_clean_repetitions_on_spec() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        reporting_identity: None,
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "1".into()],
        clean_repetitions: 4,
    })
    .unwrap();
    let new = new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc);

    let job = store
        .create_unlinked_job(uuid::Uuid::new_v4(), &new, &detail, None)
        .await
        .unwrap();

    let requested: i32 =
        sqlx::query_scalar("SELECT requested_run_count FROM task_spec WHERE id = $1")
            .bind(job.task_spec_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(requested, 4);

    let steps: Vec<(i32, Db<SubmissionStepKind>)> = sqlx::query_as(
        "SELECT step_index, step_kind FROM task_workflow_step WHERE task_submission_id = $1 \
         ORDER BY step_index",
    )
    .bind(job.task_submission_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        steps,
        vec![
            (0, Db(SubmissionStepKind::Build)),
            (1, Db(SubmissionStepKind::Calibrate)),
            (2, Db(SubmissionStepKind::Run)),
        ]
    );
}

#[tokio::test]
async fn append_next_benchmark_run_is_ordered_and_blocks_on_active_sibling() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        reporting_identity: None,
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "1".into()],
        clean_repetitions: 3,
    })
    .unwrap();
    let first = store
        .create_unlinked_job(
            uuid::Uuid::new_v4(),
            &new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc),
            &detail,
            None,
        )
        .await
        .unwrap();
    store
        .record_plan_message_ts(first.id, "1700000000.000200")
        .await
        .unwrap();
    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .unwrap();

    let second = store
        .append_next_benchmark_run(first.id)
        .await
        .unwrap()
        .expect("run 1 is queued");
    assert_eq!(second.task_submission_id, first.task_submission_id);
    assert_eq!(second.task_spec_id, first.task_spec_id);
    assert_eq!(second.task_run_index, 1);
    assert_eq!(second.status, JobStatus::Queued);
    assert_eq!(
        store
            .latest_plan_message_ts(second.id)
            .await
            .unwrap()
            .as_deref(),
        Some("1700000000.000200"),
        "repeat run reuses the submission's Slack message",
    );
    assert!(
        store
            .append_next_benchmark_run(first.id)
            .await
            .unwrap()
            .is_none(),
        "active run 1 prevents a duplicate run 1"
    );

    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(second.id)
        .execute(&pool)
        .await
        .unwrap();
    let third = store
        .append_next_benchmark_run(second.id)
        .await
        .unwrap()
        .expect("run 2 is queued");
    assert_eq!(third.task_run_index, 2);

    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(third.id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store
            .append_next_benchmark_run(third.id)
            .await
            .unwrap()
            .is_none(),
        "requested run count is satisfied"
    );
}

#[tokio::test]
async fn resume_pending_benchmark_runs_derives_next_run_from_db_state() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        reporting_identity: None,
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "1".into()],
        clean_repetitions: 2,
    })
    .unwrap();
    let first = store
        .create_unlinked_job(
            uuid::Uuid::new_v4(),
            &new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc),
            &detail,
            None,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .unwrap();

    let resumed = store
        .resume_pending_benchmark_runs()
        .await
        .unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].task_spec_id, first.task_spec_id);
    assert_eq!(resumed[0].task_run_index, 1);

    assert!(
        store
            .resume_pending_benchmark_runs()
            .await
            .unwrap()
            .is_empty(),
        "the active resumed run prevents duplicate startup enqueue"
    );
}

#[tokio::test]
async fn create_unlinked_benchmark_submission_persists_ordered_specs_and_first_run() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        reporting_identity: None,
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "1".into()],
        clean_repetitions: 1,
    })
    .unwrap();
    let mut baseline = new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc);
    baseline.git_ref_display = "release/3.4.0.0.2".into();
    let mut candidate = new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc);
    candidate.git_ref_display = "release/3.4.0.0.3".into();

    let first = store
        .create_unlinked_benchmark_submission(
            Uuid::new_v4(),
            &[
                NewBenchmarkSpec::singleton(baseline, 1),
                NewBenchmarkSpec {
                    new_job: candidate,
                    requested_run_count: 1,
                    baseline_calibration_id: Some(42),
                },
            ],
            &detail,
            Some("1700000000.000200"),
        )
        .await
        .unwrap();

    assert_eq!(first.task_run_index, 0);
    assert_eq!(first.git_ref_display, "release/3.4.0.0.2");
    assert_eq!(
        store
            .latest_plan_message_ts(first.id)
            .await
            .unwrap()
            .as_deref(),
        Some("1700000000.000200"),
    );

    let specs: Vec<(Uuid, i32, String, Option<i64>)> = sqlx::query_as(
        "SELECT id, spec_index, git_ref_display, baseline_calibration_id
           FROM task_spec
          WHERE task_submission_id = $1
       ORDER BY spec_index",
    )
    .bind(first.task_submission_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].1, 0);
    assert_eq!(specs[0].2, "release/3.4.0.0.2");
    assert_eq!(specs[0].3, None);
    assert_eq!(specs[1].1, 1);
    assert_eq!(specs[1].2, "release/3.4.0.0.3");
    assert_eq!(specs[1].3, Some(42));

    let jobs: Vec<(Uuid, Uuid, i32)> = sqlx::query_as(
        "SELECT id, task_spec_id, task_run_index
           FROM job
          WHERE task_submission_id = $1
       ORDER BY created_at, id",
    )
    .bind(first.task_submission_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(jobs, vec![(first.id, specs[0].0, 0)]);

    let steps: Vec<(i32, Db<SubmissionStepKind>, Option<Uuid>)> = sqlx::query_as(
        "SELECT step_index, step_kind, task_spec_id
           FROM task_workflow_step
          WHERE task_submission_id = $1
       ORDER BY step_index",
    )
    .bind(first.task_submission_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        steps,
        vec![
            (0, Db(SubmissionStepKind::Build), Some(specs[0].0)),
            (1, Db(SubmissionStepKind::Calibrate), Some(specs[0].0)),
            (2, Db(SubmissionStepKind::Run), Some(specs[0].0)),
            (3, Db(SubmissionStepKind::Build), Some(specs[1].0)),
            (4, Db(SubmissionStepKind::Calibrate), Some(specs[1].0)),
            (5, Db(SubmissionStepKind::Run), Some(specs[1].0)),
        ],
    );
}

#[tokio::test]
async fn create_unlinked_benchmark_submission_rolls_back_on_invalid_spec_collection() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        reporting_identity: None,
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "1".into()],
        clean_repetitions: 1,
    })
    .unwrap();
    let slack = new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc);
    let github = new_job(TriggerKind::BranchPush, JobKind::Baseline);

    let err = store
        .create_unlinked_benchmark_submission(
            Uuid::new_v4(),
            &[NewBenchmarkSpec::singleton(slack, 1), NewBenchmarkSpec::singleton(github, 1)],
            &detail,
            Some("1700000000.000200"),
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("must share installation, source, and intent"),
        "{err}",
    );

    let submissions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_submission")
        .fetch_one(&pool)
        .await
        .unwrap();
    let specs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_spec")
        .fetch_one(&pool)
        .await
        .unwrap();
    let steps: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_workflow_step")
        .fetch_one(&pool)
        .await
        .unwrap();
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((submissions, specs, steps, jobs), (0, 0, 0, 0));
}

#[tokio::test]
async fn append_next_benchmark_run_moves_from_final_run_to_next_spec() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        reporting_identity: None,
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "1".into()],
        clean_repetitions: 1,
    })
    .unwrap();
    let mut baseline = new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc);
    baseline.git_ref_display = "baseline".into();
    let mut candidate = new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc);
    candidate.git_ref_display = "candidate".into();
    let first = store
        .create_unlinked_benchmark_submission(
            Uuid::new_v4(),
            &[NewBenchmarkSpec::singleton(baseline, 1), NewBenchmarkSpec::singleton(candidate, 1)],
            &detail,
            Some("1700000000.000200"),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .unwrap();

    let second = store
        .append_next_benchmark_run(first.id)
        .await
        .unwrap()
        .expect("candidate spec run 0 is queued");
    assert_eq!(second.task_submission_id, first.task_submission_id);
    assert_ne!(second.task_spec_id, first.task_spec_id);
    assert_eq!(second.task_run_index, 0);
    assert_eq!(second.git_ref_display, "candidate");
    assert_eq!(second.status, JobStatus::Queued);
    assert_eq!(
        store
            .latest_plan_message_ts(second.id)
            .await
            .unwrap()
            .as_deref(),
        Some("1700000000.000200"),
    );
    assert!(
        store
            .append_next_benchmark_run(first.id)
            .await
            .unwrap()
            .is_none(),
        "active candidate run prevents duplicate spec transition",
    );

    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(second.id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store
            .append_next_benchmark_run(second.id)
            .await
            .unwrap()
            .is_none(),
        "final spec completion terminates the submission",
    );
}

#[tokio::test]
async fn resume_pending_benchmark_runs_continues_across_specs() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
        channel: "C123".into(),
        message_ts: "1700000000.000100".into(),
        reporting_identity: None,
        bench_args: vec!["--block".into(), "184231".into(), "--repetitions".into(), "1".into()],
        clean_repetitions: 1,
    })
    .unwrap();
    let mut baseline = new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc);
    baseline.git_ref_display = "baseline".into();
    let mut candidate = new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc);
    candidate.git_ref_display = "candidate".into();
    let first = store
        .create_unlinked_benchmark_submission(
            Uuid::new_v4(),
            &[NewBenchmarkSpec::singleton(baseline, 1), NewBenchmarkSpec::singleton(candidate, 1)],
            &detail,
            None,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE job SET status = 'completed' WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .unwrap();

    let resumed = store
        .resume_pending_benchmark_runs()
        .await
        .unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].git_ref_display, "candidate");
    assert_eq!(resumed[0].task_run_index, 0);
    assert_ne!(resumed[0].task_spec_id, first.task_spec_id);
}

#[tokio::test]
async fn insert_job_creates_singleton_submission_spec_and_run() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let new = new_job(TriggerKind::BranchPush, JobKind::Baseline);

    let job = store
        .insert_job(&new)
        .await
        .unwrap();

    assert_singleton_model(&pool, job.id, job.task_submission_id, job.task_spec_id, new.axes).await;
}

#[tokio::test]
async fn create_job_with_links_creates_singleton_submission_spec_and_run() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool).await;
    let store = PostgresJobStore::new(pool.clone());
    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('submission-links-1', 'issue_comment', 0) RETURNING id",
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
        created.job.task_submission_id,
        created.job.task_spec_id,
        new.axes,
    )
    .await;
}

#[tokio::test]
async fn create_unlinked_build_only_job_creates_build_only_singleton_submission() {
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
        .create_unlinked_job(uuid::Uuid::new_v4(), &new, &detail, None)
        .await
        .unwrap();

    assert_singleton_model(&pool, job.id, job.task_submission_id, job.task_spec_id, new.axes).await;
}
