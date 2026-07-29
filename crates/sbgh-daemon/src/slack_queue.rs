//! Daemon composition adapter for Slack's narrow submission port.

use std::sync::Arc;

use async_trait::async_trait;
use sbgh_core::db::JobStore;
use sbgh_core::models::{BuildTarget, GitRefKind, JobIntent, JobSource, TaskKind};
use sbgh_core::submission::{
    BenchmarkPlan, BenchmarkVariant, ProducerKey, ResolvedTaskSource, SchedulingConstraints,
    SlackSubmissionProvenance, SubmissionActor, SubmissionCommand, SubmissionProvenance,
    SubmissionReceipt, TaskPlan,
};
use sbgh_github::GitHubApi;
use sbgh_slack::{BenchmarkQueue, BenchmarkVariantRequest, SlackJobTarget, SlackSubmissionActor};

pub struct SlackBenchmarkQueue {
    jobs: Arc<dyn JobStore>,
    github: Arc<dyn GitHubApi>,
    target: SlackJobTarget,
    repository: String,
}

impl SlackBenchmarkQueue {
    pub fn new(
        jobs: Arc<dyn JobStore>,
        github: Arc<dyn GitHubApi>,
        target: SlackJobTarget,
        repository: impl Into<String>,
    ) -> Self {
        Self {
            jobs,
            github,
            target,
            repository: repository.into(),
        }
    }
}

#[async_trait]
impl BenchmarkQueue for SlackBenchmarkQueue {
    async fn submit_benchmark(
        &self,
        requested_variants: &[BenchmarkVariantRequest],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
        actor: SlackSubmissionActor<'_>,
    ) -> sbgh_core::Result<SubmissionReceipt> {
        let effective_args = queued_event_detail
            .get("effective_args")
            .and_then(|value| serde_json::from_value::<Vec<String>>(value.clone()).ok())
            .ok_or_else(|| {
                sbgh_core::Error::Config(
                    "Slack benchmark submission is missing effective_args".into(),
                )
            })?;
        let mut variants = Vec::with_capacity(requested_variants.len());
        for variant in requested_variants {
            let resolved = self
                .github
                .resolve_commit(self.target.installation_id, &self.repository, &variant.rev)
                .await
                .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))?;
            variants.push(BenchmarkVariant {
                source: ResolvedTaskSource {
                    github_installation_id: self.target.installation_id,
                    github_repo_id: self.target.repo_id,
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
        let command = SubmissionCommand {
            actor: SubmissionActor::SlackUser {
                team_id: actor.team_id.into(),
                user_id: actor.user_id.into(),
            },
            producer_key: ProducerKey {
                namespace: "slack_request".into(),
                key: actor
                    .reporting_identity
                    .into(),
            },
            constraints: SchedulingConstraints::default(),
            task: TaskPlan::Benchmark(BenchmarkPlan { variants, effective_args }),
            provenance: SubmissionProvenance {
                queued_event_detail: queued_event_detail.clone(),
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
            },
        };
        crate::submission::submit(self.jobs.as_ref(), command)
            .await
            .map_err(|error| sbgh_core::Error::Other(anyhow::Error::new(error)))
    }
}
