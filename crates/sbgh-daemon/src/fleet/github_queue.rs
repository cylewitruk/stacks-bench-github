use std::sync::Arc;

use sbgh_core::models::{BuildTarget, GitRefKind, JobIntent, JobSource, TaskKind};
use sbgh_core::submission::{
    GithubSubmissionProvenance, ProducerKey, ResolvedTaskSource, SubmissionActor,
    SubmissionProvenance,
};

use crate::block_validation_submission::{
    BlockValidationSubmission, BlockValidationSubmissionService,
};
use crate::webhook_processor::{BlockValidationJobRequest, BlockValidationQueue};

pub struct PostgresBlockValidationQueue {
    service: Arc<BlockValidationSubmissionService>,
}

impl PostgresBlockValidationQueue {
    pub fn new(service: Arc<BlockValidationSubmissionService>) -> Self {
        Self { service }
    }
}

#[async_trait::async_trait]
impl BlockValidationQueue for PostgresBlockValidationQueue {
    async fn enqueue(&self, request: BlockValidationJobRequest) -> sbgh_core::Result<()> {
        let detail = self
            .service
            .queued_detail(&request.range)?;
        self.service
            .submit(BlockValidationSubmission {
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
                epoch: request.epoch,
                range: request.range,
                actor: SubmissionActor::GithubUser {
                    user_id: request.triggering_user_id,
                },
                producer_key: ProducerKey {
                    namespace: "github_webhook".into(),
                    key: request.webhook_id.to_string(),
                },
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
            })
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use sbgh_core::config::BlockValidationTaskConfig;
    use sbgh_core::db::SubmissionStore;
    use sbgh_core::submission::{
        PreparedSubmission, SubmissionDisposition, SubmissionError, SubmissionReceipt, TaskPlan,
    };
    use sbgh_fleet::{InclusiveRange, ValidationEpoch};
    use uuid::Uuid;

    use super::*;

    #[derive(Default)]
    struct RecordingStore(Mutex<Vec<PreparedSubmission>>);

    #[async_trait]
    impl SubmissionStore for RecordingStore {
        async fn persist_submission(
            &self,
            prepared: &PreparedSubmission,
        ) -> Result<SubmissionReceipt, SubmissionError> {
            self.0
                .lock()
                .unwrap()
                .push(prepared.clone());
            Ok(SubmissionReceipt {
                submission_id: Uuid::new_v4(),
                disposition: SubmissionDisposition::Created,
                initial_job_ids: vec![Uuid::new_v4()],
            })
        }
    }

    #[tokio::test]
    async fn enqueue_maps_github_identity_and_validation_payload() {
        let store = Arc::new(RecordingStore::default());
        let service = Arc::new(BlockValidationSubmissionService::new(
            store.clone(),
            BlockValidationTaskConfig {
                default_epoch: ValidationEpoch::Nakamoto,
                default_range_start: 100,
                default_range_end: 200,
                requested_shards: 8,
                max_concurrency: 4,
                timeout_secs: 300,
                allow_range_override: true,
            },
        ));
        let queue = PostgresBlockValidationQueue::new(service);

        queue
            .enqueue(BlockValidationJobRequest {
                github_installation_id: 11,
                github_repo_id: 12,
                commit: "a".repeat(40),
                webhook_id: 13,
                triggering_user_id: 14,
                github_pull_request_id: 15,
                triggering_comment_id: 16,
                epoch: ValidationEpoch::Nakamoto,
                range: InclusiveRange { start: 17, end: 18 },
            })
            .await
            .unwrap();

        let records = store.0.lock().unwrap();
        let prepared = records
            .first()
            .expect("one submission");
        assert_eq!(prepared.command.actor, SubmissionActor::GithubUser { user_id: 14 });
        assert_eq!(
            prepared.command.producer_key,
            ProducerKey {
                namespace: "github_webhook".into(),
                key: "13".into(),
            }
        );
        let TaskPlan::BlockValidation(plan) = &prepared.command.task else {
            panic!("expected block-validation plan");
        };
        assert_eq!(plan.source.source, JobSource::GithubComment);
        assert_eq!(plan.source.build_target, BuildTarget::StacksInspect);
        assert_eq!(
            plan.source
                .github_installation_id,
            11
        );
        assert_eq!(plan.source.github_repo_id, 12);
        assert_eq!(plan.source.commit, "a".repeat(40));
        assert_eq!(plan.payload.epoch, ValidationEpoch::Nakamoto);
        assert_eq!(plan.payload.range, InclusiveRange { start: 17, end: 18 });
        assert_eq!(plan.payload.requested_shards, 8);
        assert_eq!(plan.payload.max_concurrency, 4);
        assert_eq!(plan.payload.timeout_secs, 300);
        assert_eq!(
            prepared
                .command
                .provenance
                .github
                .as_ref()
                .expect("GitHub provenance"),
            &GithubSubmissionProvenance {
                webhook_id: 13,
                triggering_user_id: Some(14),
                pull_request_id: Some(15),
                triggering_comment_id: Some(16),
            }
        );
        assert!(
            prepared
                .command
                .provenance
                .slack
                .is_none()
        );
    }
}
