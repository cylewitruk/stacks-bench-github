//! Task-owned block-validation submission policy and application service.

use std::sync::Arc;

use sbgh_core::config::BlockValidationTaskConfig;
use sbgh_core::db::SubmissionStore;
use sbgh_core::models::{QueuedEventDetail, TaskKind};
use sbgh_core::submission::{
    BlockValidationPlan, ProducerKey, ResolvedTaskSource, SchedulingConstraints, SubmissionActor,
    SubmissionCommand, SubmissionError, SubmissionProvenance, SubmissionReceipt, TaskPlan,
};
use sbgh_fleet::{
    BlockValidationPayload, BlockValidationSelection, InclusiveRange, TaskPayload, Validate,
};
use sbgh_intent::ValidationSelection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockValidationSelectionError {
    RecentCountOutOfBounds { max: u64 },
    FullDisabled,
    RangeDisabled,
    ReversedRange,
    RangeOutOfBounds,
}

impl std::fmt::Display for BlockValidationSelectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RecentCountOutOfBounds { max } => {
                write!(formatter, "requested recent block count must be within 1..={max}")
            }
            Self::FullDisabled => formatter.write_str("full block validation is disabled"),
            Self::RangeDisabled => {
                formatter.write_str("block-validation range overrides are disabled")
            }
            Self::ReversedRange => formatter.write_str("block-validation range is reversed"),
            Self::RangeOutOfBounds => {
                formatter.write_str("block-validation range exceeds the durable integer range")
            }
        }
    }
}

impl std::error::Error for BlockValidationSelectionError {}

pub struct BlockValidationSubmissionService {
    store: Arc<dyn SubmissionStore>,
    policy: BlockValidationTaskConfig,
}

pub struct BlockValidationSubmission {
    pub source: ResolvedTaskSource,
    pub selection: BlockValidationSelection,
    pub constraints: SchedulingConstraints,
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
    ) -> Result<BlockValidationSelection, BlockValidationSelectionError> {
        resolve_user_selection(&self.policy, selection)
    }

    pub async fn submit(
        &self,
        request: BlockValidationSubmission,
    ) -> Result<SubmissionReceipt, SubmissionError> {
        if request.source.task_kind != TaskKind::BlockValidation {
            return Err(SubmissionError::Invalid(
                "block-validation submission received a non-validation source".into(),
            ));
        }
        let payload = TaskPayload::BlockValidation(BlockValidationPayload {
            selection: request.selection,
            timeout_secs: self.policy.timeout_secs,
        });
        payload
            .validate()
            .map_err(|error| SubmissionError::Invalid(error.to_string()))?;
        let TaskPayload::BlockValidation(payload) = payload else {
            unreachable!("constructed as block validation")
        };
        let command = SubmissionCommand {
            actor: request.actor,
            producer_key: request.producer_key,
            constraints: request.constraints,
            task: TaskPlan::BlockValidation(BlockValidationPlan {
                source: request.source,
                payload,
            }),
            provenance: request.provenance,
        };
        crate::submission::submit(self.store.as_ref(), command).await
    }

    pub fn queued_detail(
        &self,
        selection: &BlockValidationSelection,
    ) -> sbgh_core::Result<serde_json::Value> {
        serde_json::to_value(QueuedEventDetail::BlockValidation { selection: selection.clone() })
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
    }
}

fn resolve_user_selection(
    policy: &BlockValidationTaskConfig,
    selection: &ValidationSelection,
) -> Result<BlockValidationSelection, BlockValidationSelectionError> {
    match selection {
        ValidationSelection::Recent { block_count } => {
            let block_count = block_count.unwrap_or(policy.default_recent_blocks);
            if block_count == 0 || block_count > policy.max_recent_blocks {
                return Err(BlockValidationSelectionError::RecentCountOutOfBounds {
                    max: policy.max_recent_blocks,
                });
            }
            Ok(BlockValidationSelection::Recent { block_count })
        }
        ValidationSelection::Full => {
            if !policy.allow_full_validation {
                return Err(BlockValidationSelectionError::FullDisabled);
            }
            Ok(BlockValidationSelection::Full)
        }
        ValidationSelection::Range { start, end } => {
            if !policy.allow_range_override {
                return Err(BlockValidationSelectionError::RangeDisabled);
            }
            if start > end {
                return Err(BlockValidationSelectionError::ReversedRange);
            }
            if *end >= i64::MAX as u64 {
                return Err(BlockValidationSelectionError::RangeOutOfBounds);
            }
            Ok(BlockValidationSelection::Range {
                range: InclusiveRange { start: *start, end: *end },
            })
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
            default_recent_blocks: 100,
            max_recent_blocks: 200,
            timeout_secs: 300,
            allow_full_validation: true,
            allow_range_override,
        }
    }

    #[test]
    fn recent_full_and_allowed_range_are_resolved_from_server_policy() {
        assert_eq!(
            resolve_user_selection(
                &policy(false),
                &ValidationSelection::Recent { block_count: None }
            )
            .unwrap(),
            BlockValidationSelection::Recent { block_count: 100 }
        );
        assert_eq!(
            resolve_user_selection(
                &policy(true),
                &ValidationSelection::Range { start: 10, end: 20 }
            )
            .unwrap(),
            BlockValidationSelection::Range {
                range: InclusiveRange { start: 10, end: 20 }
            }
        );
        assert_eq!(
            resolve_user_selection(&policy(true), &ValidationSelection::Full).unwrap(),
            BlockValidationSelection::Full
        );
    }

    #[test]
    fn disabled_override_fails_before_submission() {
        assert!(
            resolve_user_selection(
                &policy(false),
                &ValidationSelection::Range { start: 10, end: 20 }
            )
            .is_err()
        );
        assert_eq!(
            resolve_user_selection(
                &policy(true),
                &ValidationSelection::Range { start: 0, end: i64::MAX as u64 },
            ),
            Err(BlockValidationSelectionError::RangeOutOfBounds)
        );
    }

    #[test]
    fn recent_count_and_full_mode_are_bounded_by_server_policy() {
        assert_eq!(
            resolve_user_selection(
                &policy(false),
                &ValidationSelection::Recent { block_count: Some(200) }
            )
            .unwrap(),
            BlockValidationSelection::Recent { block_count: 200 }
        );
        for block_count in [0, 201] {
            assert!(
                resolve_user_selection(
                    &policy(false),
                    &ValidationSelection::Recent { block_count: Some(block_count) }
                )
                .is_err()
            );
        }
        let mut no_full = policy(false);
        no_full.allow_full_validation = false;
        assert!(resolve_user_selection(&no_full, &ValidationSelection::Full).is_err());
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
        let selection = BlockValidationSelection::Range {
            range: InclusiveRange { start: 10, end: 20 },
        };
        let detail = service
            .queued_detail(&selection)
            .unwrap();
        service
            .submit(BlockValidationSubmission {
                source: source(JobSource::GithubComment),
                selection: selection.clone(),
                constraints: SchedulingConstraints::default(),
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
                selection,
                constraints: SchedulingConstraints::default(),
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
        assert_eq!(
            payloads[0].selection,
            BlockValidationSelection::Range {
                range: InclusiveRange { start: 10, end: 20 }
            }
        );
        assert_eq!(payloads[0].timeout_secs, 300);
    }
}
