//! Daemon composition adapter for Slack's task-aware submission port.

use std::sync::Arc;

use async_trait::async_trait;
use sbgh_core::db::SubmissionStore;
use sbgh_core::models::{
    BuildTarget, GitRefKind, JobIntent, JobSource, QueuedEventDetail, TaskKind,
};
use sbgh_core::submission::{
    BenchmarkPlan, BenchmarkVariant, ProducerKey, ResolvedTaskSource, SchedulingConstraints,
    SlackSubmissionProvenance, SubmissionActor, SubmissionCommand, SubmissionProvenance,
    SubmissionReceipt, TaskPlan,
};
use sbgh_github::GitHubApi;
use sbgh_slack::{SlackSubmissionActor, TaskSubmissionPort, TaskSubmissionRequest};

use crate::block_validation_submission::{
    BlockValidationSubmission, BlockValidationSubmissionService,
};
use crate::slack_target::SlackRepositoryResolver;

pub struct SlackTaskSubmissionAdapter {
    jobs: Arc<dyn SubmissionStore>,
    github: Arc<dyn GitHubApi>,
    validation: Option<Arc<BlockValidationSubmissionService>>,
    repositories: Arc<dyn SlackRepositoryResolver>,
    default_repository: String,
    default_rev: String,
}

impl SlackTaskSubmissionAdapter {
    pub fn new(
        jobs: Arc<dyn SubmissionStore>,
        github: Arc<dyn GitHubApi>,
        validation: Option<Arc<BlockValidationSubmissionService>>,
        repositories: Arc<dyn SlackRepositoryResolver>,
        default_repository: impl Into<String>,
        default_rev: impl Into<String>,
    ) -> Self {
        Self {
            jobs,
            github,
            validation,
            repositories,
            default_repository: default_repository.into(),
            default_rev: default_rev.into(),
        }
    }

    fn slack_provenance(
        actor: SlackSubmissionActor<'_>,
        queued_event_detail: serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> SubmissionProvenance {
        SubmissionProvenance {
            queued_event_detail,
            github: None,
            slack: Some(SlackSubmissionProvenance {
                team_id: actor.team_id.into(),
                channel_id: actor.channel_id.into(),
                request_message_ts: actor
                    .request_message_ts
                    .into(),
                reporting_identity: actor
                    .reporting_identity
                    .into(),
                report_message_ts: plan_message_ts.map(Into::into),
            }),
        }
    }

    fn producer_key(actor: SlackSubmissionActor<'_>) -> ProducerKey {
        ProducerKey {
            namespace: "slack_request".into(),
            key: actor
                .reporting_identity
                .into(),
        }
    }
}

#[async_trait]
impl TaskSubmissionPort for SlackTaskSubmissionAdapter {
    async fn submit(
        &self,
        request: TaskSubmissionRequest,
        plan_message_ts: Option<&str>,
        actor: SlackSubmissionActor<'_>,
    ) -> sbgh_core::Result<SubmissionReceipt> {
        match request {
            TaskSubmissionRequest::Benchmark {
                repository,
                variants: requested_variants,
                effective_args,
                clean_repetitions,
            } => {
                let repository = repository
                    .as_deref()
                    .unwrap_or(&self.default_repository);
                let target = self
                    .repositories
                    .resolve(repository)
                    .await?;
                let mut variants = Vec::with_capacity(requested_variants.len());
                for variant in &requested_variants {
                    let resolved = self
                        .github
                        .resolve_commit(target.installation_id, repository, &variant.rev)
                        .await
                        .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
                    variants.push(BenchmarkVariant {
                        source: ResolvedTaskSource {
                            github_installation_id: target.installation_id,
                            github_repo_id: target.repo_id,
                            source: JobSource::Slack,
                            intent: JobIntent::AdhocBenchmark,
                            task_kind: TaskKind::Benchmark,
                            build_target: BuildTarget::StacksBench,
                            git_ref_kind: GitRefKind::Branch,
                            git_ref_display: variant.rev.clone(),
                            commit: resolved.hash,
                            committed_at: resolved.committed_at,
                            workload_key: Some(variant.workload_key.clone()),
                        },
                        requested_run_count: variant.requested_run_count,
                        baseline_calibration_id: None,
                    });
                }
                let mut detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
                    channel: actor.channel_id.into(),
                    message_ts: actor
                        .request_message_ts
                        .into(),
                    reporting_identity: Some(
                        actor
                            .reporting_identity
                            .into(),
                    ),
                    bench_args: effective_args.clone(),
                    clean_repetitions,
                })
                .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
                detail["effective_args"] = serde_json::json!(&effective_args);
                let command = SubmissionCommand {
                    actor: SubmissionActor::SlackUser {
                        team_id: actor.team_id.into(),
                        user_id: actor.user_id.into(),
                    },
                    producer_key: Self::producer_key(actor),
                    constraints: SchedulingConstraints::default(),
                    task: TaskPlan::Benchmark(BenchmarkPlan { variants, effective_args }),
                    provenance: Self::slack_provenance(actor, detail, plan_message_ts),
                };
                crate::submission::submit(self.jobs.as_ref(), command)
                    .await
                    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
            }
            TaskSubmissionRequest::BlockValidation(request) => {
                let validation = self
                    .validation
                    .as_ref()
                    .ok_or_else(|| {
                        sbgh_core::Error::Config(
                            "block-validation submission is not configured".into(),
                        )
                    })?;
                let repository = request
                    .source
                    .repository
                    .as_deref()
                    .unwrap_or(&self.default_repository);
                let target = self
                    .repositories
                    .resolve(repository)
                    .await?;
                let revision = request
                    .source
                    .revision
                    .unwrap_or_else(|| self.default_rev.clone());
                let selection = validation
                    .resolve_user_selection(&request.selection)
                    .map_err(|error| sbgh_core::Error::Config(error.to_string()))?;
                let resolved = self
                    .github
                    .resolve_commit(target.installation_id, repository, &revision)
                    .await
                    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
                let detail = validation.queued_detail(&selection)?;
                validation
                    .submit(BlockValidationSubmission {
                        source: ResolvedTaskSource {
                            github_installation_id: target.installation_id,
                            github_repo_id: target.repo_id,
                            source: JobSource::Slack,
                            intent: JobIntent::BlockValidation,
                            task_kind: TaskKind::BlockValidation,
                            build_target: BuildTarget::StacksInspect,
                            git_ref_kind: GitRefKind::Branch,
                            git_ref_display: revision,
                            commit: resolved.hash,
                            committed_at: resolved.committed_at,
                            workload_key: None,
                        },
                        selection,
                        constraints: SchedulingConstraints::default(),
                        actor: SubmissionActor::SlackUser {
                            team_id: actor.team_id.into(),
                            user_id: actor.user_id.into(),
                        },
                        producer_key: Self::producer_key(actor),
                        provenance: Self::slack_provenance(actor, detail, plan_message_ts),
                    })
                    .await
                    .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use sbgh_core::config::BlockValidationTaskConfig;
    use sbgh_core::submission::{
        PreparedSubmission, SubmissionDisposition, SubmissionError, TaskPlan,
    };
    use sbgh_fleet::BlockValidationSelection;
    use sbgh_github::test_support::FakeGitHub;
    use sbgh_intent::{BlockValidationIntent, RequestedSource, ValidationSelection};
    use sbgh_slack::{BenchmarkVariantRequest, SlackJobTarget};
    use uuid::Uuid;

    use super::*;

    #[derive(Default)]
    struct RecordingStore(Mutex<Vec<PreparedSubmission>>);

    struct FakeRepositories;

    #[async_trait]
    impl SlackRepositoryResolver for FakeRepositories {
        async fn resolve(&self, repository: &str) -> sbgh_core::Result<SlackJobTarget> {
            match repository {
                "stacks-network/stacks-core" => {
                    Ok(SlackJobTarget { installation_id: 1, repo_id: 2 })
                }
                "cylewitruk/stacks-core" => Ok(SlackJobTarget { installation_id: 3, repo_id: 4 }),
                _ => Err(sbgh_core::Error::Unauthorized("repository not allowed".into())),
            }
        }
    }

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
    async fn validation_source_is_frozen_and_policy_is_server_owned() {
        let store = Arc::new(RecordingStore::default());
        let github = Arc::new(FakeGitHub::new());
        github.set_commit("stacks-network/stacks-core", "candidate", &"a".repeat(40), None);
        github.set_commit("cylewitruk/stacks-core", "candidate", &"b".repeat(40), None);
        let validation = Arc::new(BlockValidationSubmissionService::new(
            store.clone(),
            BlockValidationTaskConfig {
                default_recent_blocks: 100,
                max_recent_blocks: 200,
                timeout_secs: 300,
                allow_full_validation: true,
                allow_range_override: false,
            },
        ));
        let adapter = SlackTaskSubmissionAdapter::new(
            store.clone(),
            github,
            Some(validation),
            Arc::new(FakeRepositories),
            "stacks-network/stacks-core",
            "develop",
        );
        adapter
            .submit(
                TaskSubmissionRequest::BlockValidation(BlockValidationIntent {
                    source: RequestedSource {
                        repository: None,
                        revision: Some("candidate".into()),
                    },
                    selection: ValidationSelection::Recent { block_count: None },
                }),
                Some("1.3"),
                SlackSubmissionActor {
                    team_id: "T1",
                    user_id: "U1",
                    channel_id: "C1",
                    request_message_ts: "1.2",
                    reporting_identity: "opaque",
                },
            )
            .await
            .unwrap();

        {
            let records = store.0.lock().unwrap();
            let TaskPlan::BlockValidation(plan) = &records[0].command.task else {
                panic!("expected validation plan");
            };
            assert_eq!(plan.source.commit, "a".repeat(40));
            assert_eq!(plan.source.git_ref_display, "candidate");
            assert_eq!(
                plan.payload.selection,
                BlockValidationSelection::Recent { block_count: 100 }
            );
        }

        adapter
            .submit(
                TaskSubmissionRequest::BlockValidation(BlockValidationIntent {
                    source: RequestedSource {
                        repository: Some("cylewitruk/stacks-core".into()),
                        revision: Some("candidate".into()),
                    },
                    selection: ValidationSelection::Recent { block_count: None },
                }),
                None,
                SlackSubmissionActor {
                    team_id: "T1",
                    user_id: "U1",
                    channel_id: "C1",
                    request_message_ts: "1.4",
                    reporting_identity: "fork",
                },
            )
            .await
            .unwrap();
        {
            let records = store.0.lock().unwrap();
            let TaskPlan::BlockValidation(plan) = &records[1].command.task else {
                panic!("expected validation plan");
            };
            assert_eq!(
                plan.source
                    .github_installation_id,
                3
            );
            assert_eq!(plan.source.github_repo_id, 4);
            assert_eq!(plan.source.commit, "b".repeat(40));
        }

        let error = adapter
            .submit(
                TaskSubmissionRequest::BlockValidation(BlockValidationIntent {
                    source: RequestedSource {
                        repository: Some("other/repository".into()),
                        revision: Some("candidate".into()),
                    },
                    selection: ValidationSelection::Recent { block_count: None },
                }),
                None,
                SlackSubmissionActor {
                    team_id: "T1",
                    user_id: "U1",
                    channel_id: "C1",
                    request_message_ts: "2.2",
                    reporting_identity: "other",
                },
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("repository not allowed")
        );
        assert_eq!(store.0.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn benchmark_uses_explicit_repository_and_default_only_when_omitted() {
        let store = Arc::new(RecordingStore::default());
        let github = Arc::new(FakeGitHub::new());
        github.set_commit("stacks-network/stacks-core", "develop", &"a".repeat(40), None);
        github.set_commit("cylewitruk/stacks-core", "candidate", &"b".repeat(40), None);
        let adapter = SlackTaskSubmissionAdapter::new(
            store.clone(),
            github,
            None,
            Arc::new(FakeRepositories),
            "stacks-network/stacks-core",
            "develop",
        );
        let actor = SlackSubmissionActor {
            team_id: "T1",
            user_id: "U1",
            channel_id: "C1",
            request_message_ts: "1.2",
            reporting_identity: "opaque",
        };
        for (repository, rev) in
            [(None, "develop"), (Some("cylewitruk/stacks-core".into()), "candidate")]
        {
            let effective_args = vec!["run".into(), "--count".into(), "1".into()];
            adapter
                .submit(
                    TaskSubmissionRequest::Benchmark {
                        repository,
                        variants: vec![BenchmarkVariantRequest {
                            rev: rev.into(),
                            workload_key: sbgh_core::bench_args::workload_key(&effective_args),
                            requested_run_count: 1,
                        }],
                        effective_args,
                        clean_repetitions: 1,
                    },
                    None,
                    actor,
                )
                .await
                .unwrap();
        }
        let records = store.0.lock().unwrap();
        for (record, installation_id, repo_id, commit) in
            [(&records[0], 1, 2, "a"), (&records[1], 3, 4, "b")]
        {
            let TaskPlan::Benchmark(plan) = &record.command.task else {
                panic!("expected benchmark plan");
            };
            let source = &plan.variants[0].source;
            assert_eq!(source.github_installation_id, installation_id);
            assert_eq!(source.github_repo_id, repo_id);
            assert_eq!(source.commit, commit.repeat(40));
        }
    }
}
