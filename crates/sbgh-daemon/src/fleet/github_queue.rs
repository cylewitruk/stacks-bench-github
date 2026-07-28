use sbgh_core::db::fleet::PreparedExecution;
use sbgh_core::models::{
    BuildTarget, GitRefKind, JobAxes, JobIntent, JobSource, NewJob, NewPullRequestLink,
    QueuedEventDetail, TaskKind,
};
use sbgh_postgres::{PostgresFleetStore, PreparedJobProvenance};
use sbgh_proto::{BlockValidationPayload, TaskPayload};
use uuid::Uuid;

use super::config::GitHubBlockValidationConfig;
use crate::webhook_processor::{BlockValidationJobRequest, BlockValidationQueue};

pub struct PostgresBlockValidationQueue {
    store: PostgresFleetStore,
    config: GitHubBlockValidationConfig,
}

impl PostgresBlockValidationQueue {
    pub fn new(store: PostgresFleetStore, config: GitHubBlockValidationConfig) -> Self {
        Self { store, config }
    }
}

#[async_trait::async_trait]
impl BlockValidationQueue for PostgresBlockValidationQueue {
    async fn enqueue(&self, request: BlockValidationJobRequest) -> sbgh_core::Result<()> {
        let payload = TaskPayload::BlockValidation(BlockValidationPayload {
            epoch: request.epoch,
            range: request.range.clone(),
            requested_shards: self.config.requested_shards,
            max_concurrency: self.config.max_concurrency,
            timeout_secs: self.config.timeout_secs,
        });
        sbgh_proto::Validate::validate(&payload)
            .map_err(|error| sbgh_core::Error::Config(error.to_string()))?;
        let payload_hash = sbgh_proto::payload_digest(&payload)
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let job_id = Uuid::new_v4();
        let new = NewJob {
            github_installation_id: request.github_installation_id,
            github_repo_id: request.github_repo_id,
            axes: JobAxes {
                source: JobSource::GithubComment,
                intent: JobIntent::BlockValidation,
                task_kind: TaskKind::BlockValidation,
                build_target: BuildTarget::StacksInspect,
            },
            git_ref_kind: GitRefKind::Commit,
            git_ref_display: request.commit.clone(),
            git_commit_hash: Some(request.commit.clone()),
            git_committed_at: None,
            workload_key: None,
        };
        let detail = serde_json::to_value(QueuedEventDetail::BlockValidation {
            range_start: request.range.start,
            range_end: request.range.end,
            requested_shards: self.config.requested_shards,
            max_concurrency: self.config.max_concurrency,
        })
        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        self.store
            .enqueue_prepared_job(
                job_id,
                &new,
                &detail,
                &PreparedExecution {
                    job_id,
                    commit: request.commit,
                    payload,
                    payload_hash,
                    worker_id: Some(self.config.worker_id),
                },
                &PreparedJobProvenance {
                    github_webhook_id: Some(request.webhook_id),
                    triggering_user_id: Some(request.triggering_user_id),
                    pull_request_link: Some(NewPullRequestLink {
                        github_pull_request_id: request.github_pull_request_id,
                        triggering_comment_id: Some(request.triggering_comment_id),
                    }),
                },
            )
            .await
    }
}
