//! Shared production-store fixtures for unit and process lifecycle tests.

use sbgh_core::db::fleet::{FleetStore, PreparedExecution};
use sbgh_core::models::{
    BuildTarget, GitRefKind, GithubAccountType, JobAxes, JobIntent, JobSource as ModelJobSource,
    NewJob, TaskKind,
};
use sbgh_fleet::{
    PROTOCOL_VERSION, RegisterSessionRequest, ResourceFacts, TaskPayload, WorkOffer, payload_digest,
};
use sbgh_postgres::db::{InstallationStore, NewInstallation, Pool, PostgresInstallationStore};
use sbgh_postgres::{PostgresFleetStore, PreparedJobProvenance};
use uuid::Uuid;

/// Create an accepted attempt through production stores for lifecycle qualification.
#[cfg(test)]
pub async fn seed_active_attempt(
    pool: &Pool,
    fleet: &PostgresFleetStore,
    worker: Uuid,
    payload: TaskPayload,
) -> WorkOffer {
    sqlx::query("INSERT INTO allowed_installer (github_account_id, account_login, account_type) VALUES (100, 'test', 'organization')")
        .execute(pool).await.unwrap();
    let installations = PostgresInstallationStore::new(pool.clone());
    installations
        .upsert_installation(&NewInstallation {
            id: 100,
            github_account_id: 100,
            account_login: "test".into(),
            account_type: GithubAccountType::Organization,
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES (10, 'test', 'repo')")
        .execute(pool)
        .await
        .unwrap();
    installations
        .add_or_restore_membership(100, 10)
        .await
        .unwrap();
    let session = Uuid::new_v4();
    fleet
        .register_session(
            worker,
            &RegisterSessionRequest {
                preflight: None,
                protocol_version: PROTOCOL_VERSION,
                protocol_offer: None,
                worker_session_id: session,
                software_version: "test".into(),
                advertised_capabilities: [payload.capability()].into(),
                resources: ResourceFacts {
                    logical_cpus: 2,
                    memory_bytes: 8 << 30,
                },
            },
            chrono::Duration::seconds(90),
        )
        .await
        .unwrap();
    let job_id = Uuid::new_v4();
    let commit = "1111111111111111111111111111111111111111";
    let (intent, task_kind, build_target) = match &payload {
        TaskPayload::BuildOnly => {
            (JobIntent::CacheWarm, TaskKind::BuildOnly, BuildTarget::StacksBench)
        }
        TaskPayload::Benchmark(_) => {
            (JobIntent::BaselineBenchmark, TaskKind::Benchmark, BuildTarget::StacksBench)
        }
        TaskPayload::BlockValidation(_) => {
            unreachable!("fixture only seeds build and benchmark attempts")
        }
    };
    fleet
        .enqueue_prepared_job(
            job_id,
            &NewJob {
                github_installation_id: 100,
                github_repo_id: 10,
                axes: JobAxes {
                    source: ModelJobSource::Daemon,
                    intent,
                    task_kind,
                    build_target,
                },
                git_ref_kind: GitRefKind::Commit,
                git_ref_display: commit.into(),
                git_commit_hash: Some(commit.into()),
                git_committed_at: None,
                workload_key: None,
            },
            &serde_json::json!({}),
            &PreparedExecution {
                scheduler_generation: 0,
                job_id,
                commit: commit.into(),
                payload_hash: payload_digest(&payload).unwrap(),
                payload,
            },
            &PreparedJobProvenance::default(),
        )
        .await
        .unwrap();
    let offer = fleet
        .poll_offer(worker, session, chrono::Duration::seconds(30), chrono::Duration::seconds(45))
        .await
        .unwrap()
        .unwrap();
    assert!(
        fleet
            .accept_offer(worker, &offer.offer.identity, chrono::Duration::seconds(45))
            .await
            .unwrap()
    );
    offer.offer
}
