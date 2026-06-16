//! v10 (item 0005): each job-creation path writes the four job-model axes
//! `(source, intent, task_kind, build_target)` natively. These exercise the two
//! transactional INSERTs + `insert_job` per-path — where a bind-order /
//! omitted-column slip would hide — and assert the round-tripped axes match
//! `JobAxes::from_legacy` for the handler's `(trigger_kind, job_kind)` shape
//! (the unit-tested mapping source of truth).

use sbgh_core::db::{JobCreationOutcome, JobStore, Pool, PostgresJobStore, setup_pg_db};
use sbgh_core::models::{
    BuildTarget, GitRefKind, JobAxes, JobCreationRequest, JobIntent, JobKind, JobSource, NewJob,
    TaskKind, TriggerKind,
};
use uuid::Uuid;

/// A minimal `NewJob` whose axes are derived from a `(trigger_kind, job_kind)`
/// handler shape, on (install 100, repo 10).
fn new_job(trigger_kind: TriggerKind, job_kind: JobKind) -> NewJob {
    NewJob {
        github_installation_id: 100,
        github_repo_id: 10,
        axes: JobAxes::from_legacy(trigger_kind, job_kind),
        git_ref_kind: GitRefKind::Branch,
        git_ref_display: "develop".into(),
        git_commit_hash: None,
        git_committed_at: None,
        workload_key: None,
    }
}

/// Read a job's axes back from the DB.
async fn read_axes(pool: &Pool, id: Uuid) -> JobAxes {
    let (source, intent, task_kind, build_target): (JobSource, JobIntent, TaskKind, BuildTarget) =
        sqlx::query_as("SELECT source, intent, task_kind, build_target FROM job WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap();
    JobAxes {
        source,
        intent,
        task_kind,
        build_target,
    }
}

async fn seed_install_repo(pool: &Pool, install_id: i64, repo_id: i64) {
    sqlx::query(
        "INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES \
         ($1, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_installation (id, github_account_id, account_login, account_type) \
         VALUES ($1, $1, 'octo', 'organization') ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO github_repo (id, owner, name) VALUES ($1, 'o', 'r') ON CONFLICT DO NOTHING",
    )
    .bind(repo_id)
    .execute(pool)
    .await
    .unwrap();
    // `job` has a composite FK to the membership row.
    sqlx::query(
        "INSERT INTO github_installation_repo (github_installation_id, github_repo_id) VALUES \
         ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(install_id)
    .bind(repo_id)
    .execute(pool)
    .await
    .unwrap();
}

/// `insert_job` — a branch-push baseline (the PushHandler shape).
#[tokio::test]
async fn v10_insert_job_writes_derived_axes() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    let job = store
        .insert_job(&new_job(TriggerKind::BranchPush, JobKind::Baseline))
        .await
        .unwrap();

    assert_eq!(
        read_axes(&pool, job.id).await,
        JobAxes::from_legacy(TriggerKind::BranchPush, JobKind::Baseline),
    );
}

/// `create_job_with_links` (transactional) — a PR-comment ad-hoc (the
/// IssueCommentHandler shape).
#[tokio::test]
async fn v10_create_job_with_links_writes_derived_axes() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    let webhook_id: i64 = sqlx::query_scalar(
        "INSERT INTO github_webhook (delivery_id, event_type, payload_size_bytes) VALUES \
         ('axes-links-1', 'issue_comment', 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let outcome = store
        .create_job_with_links(&JobCreationRequest {
            new_job: new_job(TriggerKind::PrComment, JobKind::AdHoc),
            github_webhook_id: webhook_id,
            triggering_user_id: None,
            pull_request_link: None,
            queued_event_detail: None,
        })
        .await
        .unwrap();
    let id = match outcome {
        JobCreationOutcome::Created(created) => created.job.id,
        JobCreationOutcome::AlreadyEnqueued => panic!("expected Created"),
    };

    assert_eq!(
        read_axes(&pool, id).await,
        JobAxes::from_legacy(TriggerKind::PrComment, JobKind::AdHoc),
    );
}

/// `create_unlinked_job` (transactional) — a Slack ad-hoc (the SlackConnector
/// shape).
#[tokio::test]
async fn v10_create_unlinked_job_writes_derived_axes() {
    let (_db, pool) = setup_pg_db().await;
    seed_install_repo(&pool, 100, 10).await;
    let store = PostgresJobStore::new(pool.clone());

    let detail = serde_json::json!({
        "trigger": "slack_adhoc",
        "channel": "C1",
        "message_ts": "1",
        "bench_args": [],
        "clean_repetitions": 1
    });
    let job = store
        .create_unlinked_job(
            uuid::Uuid::new_v4(),
            &new_job(TriggerKind::SlackAdhoc, JobKind::AdHoc),
            &detail,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        read_axes(&pool, job.id).await,
        JobAxes::from_legacy(TriggerKind::SlackAdhoc, JobKind::AdHoc),
    );
}
