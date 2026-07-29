use sbgh_core::models::{
    BuildTarget, GitRefKind, JobIntent, JobSource, QueuedEventDetail, TaskKind,
};
use sbgh_core::submission::{
    BlockValidationPlan, GithubSubmissionProvenance, ProducerKey, ResolvedTaskSource,
    SchedulingConstraints, SubmissionActor, SubmissionCommand, SubmissionProvenance, TaskPlan,
};
use sbgh_postgres::PostgresJobStore;
use sbgh_proto::{BlockValidationPayload, TaskPayload};

use super::config::GitHubBlockValidationConfig;
use crate::webhook_processor::{BlockValidationJobRequest, BlockValidationQueue};

pub struct PostgresBlockValidationQueue {
    store: PostgresJobStore,
    config: GitHubBlockValidationConfig,
}

impl PostgresBlockValidationQueue {
    pub fn new(store: PostgresJobStore, config: GitHubBlockValidationConfig) -> Self {
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
        let TaskPayload::BlockValidation(payload) = payload else {
            unreachable!("constructed as block validation")
        };
        let detail = serde_json::to_value(QueuedEventDetail::BlockValidation {
            range_start: request.range.start,
            range_end: request.range.end,
            requested_shards: self.config.requested_shards,
            max_concurrency: self.config.max_concurrency,
        })
        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
        let command = SubmissionCommand {
            actor: SubmissionActor::GithubUser {
                user_id: request.triggering_user_id,
            },
            producer_key: ProducerKey {
                namespace: "github_webhook".into(),
                key: request.webhook_id.to_string(),
            },
            constraints: SchedulingConstraints::default(),
            task: TaskPlan::BlockValidation(BlockValidationPlan {
                source: ResolvedTaskSource {
                    github_installation_id: request.github_installation_id,
                    github_repo_id: request.github_repo_id,
                    source: JobSource::GithubComment,
                    intent: JobIntent::BlockValidation,
                    task_kind: TaskKind::BlockValidation,
                    build_target: BuildTarget::StacksInspect,
                    git_ref_kind: GitRefKind::Commit,
                    git_ref_display: request.commit.clone(),
                    commit: request.commit,
                    committed_at: None,
                    workload_key: None,
                },
                payload,
            }),
            provenance: SubmissionProvenance {
                queued_event_detail: detail,
                github: Some(GithubSubmissionProvenance {
                    webhook_id: request.webhook_id,
                    triggering_user_id: Some(request.triggering_user_id),
                    pull_request_id: Some(request.github_pull_request_id),
                    triggering_comment_id: Some(request.triggering_comment_id),
                }),
                slack: None,
            },
        };
        crate::submission::submit(&self.store, command)
            .await
            .map(|_| ())
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
    }
}
