//! Task-owned block-validation submission policy and application service.

use std::sync::Arc;

use sbgh_core::config::BlockValidationTaskConfig;
use sbgh_core::db::SubmissionStore;
use sbgh_core::models::{QueuedEventDetail, TaskKind};
use sbgh_core::submission::{
    BlockValidationPlan, ProducerKey, ResolvedTaskSource, SchedulingConstraints, SubmissionActor,
    SubmissionCommand, SubmissionProvenance, SubmissionReceipt, TaskPlan,
};
use sbgh_fleet::{BlockValidationPayload, InclusiveRange, TaskPayload, Validate, ValidationEpoch};
use sbgh_intent::{ValidationEpochIntent, ValidationSelection};

pub struct BlockValidationSubmissionService {
    store: Arc<dyn SubmissionStore>,
    policy: BlockValidationTaskConfig,
}

pub struct BlockValidationSubmission {
    pub source: ResolvedTaskSource,
    pub epoch: ValidationEpoch,
    pub range: InclusiveRange,
    pub actor: SubmissionActor,
    pub producer_key: ProducerKey,
    pub provenance: SubmissionProvenance,
}

impl BlockValidationSubmissionService {
    pub fn new(store: Arc<dyn SubmissionStore>, policy: BlockValidationTaskConfig) -> Self {
        Self { store, policy }
    }

    pub fn resolve_user_selection(
        &self,
        selection: &ValidationSelection,
    ) -> sbgh_core::Result<(ValidationEpoch, InclusiveRange)> {
        resolve_user_selection(&self.policy, selection)
    }

    pub async fn submit(
        &self,
        request: BlockValidationSubmission,
    ) -> sbgh_core::Result<SubmissionReceipt> {
        if request.source.task_kind != TaskKind::BlockValidation {
            return Err(sbgh_core::Error::Config(
                "block-validation submission received a non-validation source".into(),
            ));
        }
        let payload = TaskPayload::BlockValidation(BlockValidationPayload {
            epoch: request.epoch,
            range: request.range.clone(),
            requested_shards: self.policy.requested_shards,
            max_concurrency: self.policy.max_concurrency,
            timeout_secs: self.policy.timeout_secs,
        });
        payload
            .validate()
            .map_err(|error| sbgh_core::Error::Config(error.to_string()))?;
        let TaskPayload::BlockValidation(payload) = payload else {
            unreachable!("constructed as block validation")
        };
        let command = SubmissionCommand {
            actor: request.actor,
            producer_key: request.producer_key,
            constraints: SchedulingConstraints::default(),
            task: TaskPlan::BlockValidation(BlockValidationPlan {
                source: request.source,
                payload,
            }),
            provenance: request.provenance,
        };
        crate::submission::submit(self.store.as_ref(), command)
            .await
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
    }

    pub fn queued_detail(&self, range: &InclusiveRange) -> sbgh_core::Result<serde_json::Value> {
        serde_json::to_value(QueuedEventDetail::BlockValidation {
            range_start: range.start,
            range_end: range.end,
            requested_shards: self.policy.requested_shards,
            max_concurrency: self.policy.max_concurrency,
        })
        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
    }
}

fn resolve_user_selection(
    policy: &BlockValidationTaskConfig,
    selection: &ValidationSelection,
) -> sbgh_core::Result<(ValidationEpoch, InclusiveRange)> {
    match selection {
        ValidationSelection::DefaultPlan => Ok((
            policy.default_epoch,
            InclusiveRange {
                start: policy.default_range_start,
                end: policy.default_range_end,
            },
        )),
        ValidationSelection::Range { epoch, start, end } => {
            if !policy.allow_range_override {
                return Err(sbgh_core::Error::Config(
                    "block-validation range overrides are disabled".into(),
                ));
            }
            Ok((
                match epoch {
                    ValidationEpochIntent::PreNakamoto => ValidationEpoch::PreNakamoto,
                    ValidationEpochIntent::Nakamoto => ValidationEpoch::Nakamoto,
                },
                InclusiveRange { start: *start, end: *end },
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use async_trait::async_trait;
    use sbgh_core::models::{BuildTarget, GitRefKind, JobIntent, JobSource};
    use sbgh_core::submission::{
        GithubSubmissionProvenance, PreparedSubmission, SlackSubmissionProvenance,
        SubmissionDisposition, SubmissionError,
    };
    use uuid::Uuid;

    fn policy(allow_range_override: bool) -> BlockValidationTaskConfig {
        BlockValidationTaskConfig {
            default_epoch: ValidationEpoch::Nakamoto,
            default_range_start: 100,
            default_range_end: 200,
            requested_shards: 8,
            max_concurrency: 4,
            timeout_secs: 300,
            allow_range_override,
        }
    }

    #[test]
    fn default_and_allowed_override_are_resolved_from_server_policy() {
        assert_eq!(
            resolve_user_selection(&policy(false), &ValidationSelection::DefaultPlan).unwrap(),
            (ValidationEpoch::Nakamoto, InclusiveRange { start: 100, end: 200 })
        );
        assert_eq!(
            resolve_user_selection(
                &policy(true),
                &ValidationSelection::Range {
                    epoch: ValidationEpochIntent::PreNakamoto,
                    start: 10,
                    end: 20,
                }
            )
            .unwrap(),
            (ValidationEpoch::PreNakamoto, InclusiveRange { start: 10, end: 20 })
        );
    }

    #[test]
    fn disabled_override_fails_before_submission() {
        assert!(
            resolve_user_selection(
                &policy(false),
                &ValidationSelection::Range {
                    epoch: ValidationEpochIntent::Nakamoto,
                    start: 10,
                    end: 20,
                }
            )
            .is_err()
        );
    }

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

    fn source(source: JobSource) -> ResolvedTaskSource {
        ResolvedTaskSource {
            github_installation_id: 1,
            github_repo_id: 2,
            source,
            intent: JobIntent::BlockValidation,
            task_kind: TaskKind::BlockValidation,
            build_target: BuildTarget::StacksInspect,
            git_ref_kind: GitRefKind::Commit,
            git_ref_display: "candidate".into(),
            commit: "a".repeat(40),
            committed_at: None,
            workload_key: None,
        }
    }

    #[tokio::test]
    async fn github_and_slack_share_one_payload_planner() {
        let store = Arc::new(RecordingStore::default());
        let service = BlockValidationSubmissionService::new(store.clone(), policy(true));
        let range = InclusiveRange { start: 10, end: 20 };
        let detail = service
            .queued_detail(&range)
            .unwrap();
        service
            .submit(BlockValidationSubmission {
                source: source(JobSource::GithubComment),
                epoch: ValidationEpoch::Nakamoto,
                range: range.clone(),
                actor: SubmissionActor::GithubUser { user_id: 7 },
                producer_key: ProducerKey {
                    namespace: "github_webhook".into(),
                    key: "1".into(),
                },
                provenance: SubmissionProvenance {
                    queued_event_detail: detail.clone(),
                    github: Some(GithubSubmissionProvenance {
                        webhook_id: 1,
                        triggering_user_id: Some(7),
                        pull_request_id: Some(8),
                        triggering_comment_id: Some(9),
                    }),
                    slack: None,
                },
            })
            .await
            .unwrap();
        service
            .submit(BlockValidationSubmission {
                source: source(JobSource::Slack),
                epoch: ValidationEpoch::Nakamoto,
                range,
                actor: SubmissionActor::SlackUser {
                    team_id: "T1".into(),
                    user_id: "U1".into(),
                },
                producer_key: ProducerKey {
                    namespace: "slack_request".into(),
                    key: "opaque".into(),
                },
                provenance: SubmissionProvenance {
                    queued_event_detail: detail,
                    github: None,
                    slack: Some(SlackSubmissionProvenance {
                        team_id: "T1".into(),
                        channel_id: "C1".into(),
                        request_message_ts: "1.2".into(),
                        reporting_identity: "opaque".into(),
                        report_message_ts: Some("1.3".into()),
                    }),
                },
            })
            .await
            .unwrap();

        let records = store.0.lock().unwrap();
        let payloads: Vec<_> = records
            .iter()
            .map(|prepared| match &prepared.command.task {
                TaskPlan::BlockValidation(plan) => plan.payload.clone(),
                _ => panic!("expected block-validation plan"),
            })
            .collect();
        assert_eq!(payloads[0], payloads[1]);
        assert_eq!(payloads[0].requested_shards, 8);
        assert_eq!(payloads[0].max_concurrency, 4);
        assert_eq!(payloads[0].timeout_secs, 300);
    }
}
