//! Task-submission application boundary.
//!
//! Surface adapters resolve mutable external identity and construct typed
//! plans. This module validates and fingerprints immutable demand before the
//! persistence adapter commits it atomically.

use sbgh_core::db::SubmissionStore;
use sbgh_core::models::{BuildTarget, QueuedEventDetail, TaskKind};
use sbgh_core::submission::{
    BenchmarkPlan, BenchmarkVariant, BlockValidationPlan, BuildOnlyPlan, PreparedSubmission,
    ResolvedTaskSource, SUBMISSION_CONTRACT_VERSION, SubmissionActor, SubmissionCommand,
    SubmissionError, SubmissionReceipt, TaskPlan,
};
use sbgh_proto::Validate;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Serialize)]
struct DigestMaterial<'a> {
    contract_version: i32,
    constraints: &'a sbgh_core::submission::SchedulingConstraints,
    task: DigestTask<'a>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum DigestTask<'a> {
    Benchmark { variants: Vec<DigestVariant<'a>>, effective_args: &'a [String] },
    BuildOnly { source: DigestSource<'a> },
    BlockValidation { source: DigestSource<'a>, payload: &'a sbgh_proto::BlockValidationPayload },
}

#[derive(Serialize)]
struct DigestVariant<'a> {
    source: DigestSource<'a>,
    requested_run_count: i32,
    baseline_calibration_id: Option<i64>,
}

#[derive(Serialize)]
struct DigestSource<'a> {
    github_installation_id: i64,
    github_repo_id: i64,
    task_kind: TaskKind,
    build_target: BuildTarget,
    commit: &'a str,
    workload_key: Option<&'a str>,
}

impl<'a> From<&'a ResolvedTaskSource> for DigestSource<'a> {
    fn from(source: &'a ResolvedTaskSource) -> Self {
        Self {
            github_installation_id: source.github_installation_id,
            github_repo_id: source.github_repo_id,
            task_kind: source.task_kind,
            build_target: source.build_target,
            commit: &source.commit,
            workload_key: source.workload_key.as_deref(),
        }
    }
}

impl<'a> From<&'a BenchmarkVariant> for DigestVariant<'a> {
    fn from(variant: &'a BenchmarkVariant) -> Self {
        Self {
            source: (&variant.source).into(),
            requested_run_count: variant.requested_run_count,
            baseline_calibration_id: variant.baseline_calibration_id,
        }
    }
}

impl<'a> From<&'a TaskPlan> for DigestTask<'a> {
    fn from(task: &'a TaskPlan) -> Self {
        match task {
            TaskPlan::Benchmark(BenchmarkPlan { variants, effective_args }) => Self::Benchmark {
                variants: variants
                    .iter()
                    .map(Into::into)
                    .collect(),
                effective_args,
            },
            TaskPlan::BuildOnly(BuildOnlyPlan { source }) => {
                Self::BuildOnly { source: source.into() }
            }
            TaskPlan::BlockValidation(BlockValidationPlan { source, payload }) => {
                Self::BlockValidation { source: source.into(), payload }
            }
        }
    }
}

fn valid_key(value: &str, max: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= max
        && !value
            .chars()
            .any(char::is_control)
}

fn validate_source(
    source: &sbgh_core::submission::ResolvedTaskSource,
) -> Result<(), SubmissionError> {
    if source.github_installation_id <= 0 || source.github_repo_id <= 0 {
        return Err(SubmissionError::Invalid(
            "GitHub installation and repository ids must be positive".into(),
        ));
    }
    if source
        .commit
        .trim()
        .is_empty()
    {
        return Err(SubmissionError::Invalid(
            "task source must contain an immutable commit".into(),
        ));
    }
    if source
        .git_ref_display
        .trim()
        .is_empty()
    {
        return Err(SubmissionError::Invalid("task source must contain a display ref".into()));
    }
    Ok(())
}

fn validate(command: &SubmissionCommand) -> Result<(), SubmissionError> {
    if !valid_key(&command.producer_key.namespace, 64) || !valid_key(&command.producer_key.key, 512)
    {
        return Err(SubmissionError::Invalid(
            "producer namespace/key is empty, oversized, or contains control characters".into(),
        ));
    }
    if command
        .constraints
        .required_measurement_profile
        .as_deref()
        .is_some_and(|profile| !valid_key(profile, 128))
    {
        return Err(SubmissionError::Invalid("required measurement profile is invalid".into()));
    }
    if !matches!(&command.task, TaskPlan::Benchmark(_))
        && command
            .constraints
            .required_measurement_profile
            .is_some()
    {
        return Err(SubmissionError::Invalid(
            "measurement-profile constraints apply only to benchmark submissions".into(),
        ));
    }

    let queued: QueuedEventDetail = serde_json::from_value(
        command
            .provenance
            .queued_event_detail
            .clone(),
    )
    .map_err(|error| {
        SubmissionError::Invalid(format!("queued-event provenance is invalid: {error}"))
    })?;
    match &command.task {
        TaskPlan::Benchmark(plan) => {
            if matches!(
                queued,
                QueuedEventDetail::CacheWarm { .. } | QueuedEventDetail::BlockValidation { .. }
            ) {
                return Err(SubmissionError::Invalid(
                    "benchmark plan has non-benchmark queued provenance".into(),
                ));
            }
            if plan.variants.is_empty() {
                return Err(SubmissionError::Invalid(
                    "benchmark plan must contain at least one variant".into(),
                ));
            }
            let first = &plan.variants[0].source;
            let expected_workload_key = sbgh_core::bench_args::workload_key(&plan.effective_args);
            let queued_args = command
                .provenance
                .queued_event_detail
                .get("effective_args")
                .and_then(|value| serde_json::from_value::<Vec<String>>(value.clone()).ok())
                .ok_or_else(|| {
                    SubmissionError::Invalid(
                        "benchmark queued provenance must contain effective_args".into(),
                    )
                })?;
            if queued_args != plan.effective_args {
                return Err(SubmissionError::Invalid(
                    "queued effective_args differ from the immutable benchmark plan".into(),
                ));
            }
            for variant in &plan.variants {
                validate_source(&variant.source)?;
                if variant.source.task_kind != TaskKind::Benchmark
                    || variant.source.build_target != BuildTarget::StacksBench
                {
                    return Err(SubmissionError::Invalid(
                        "benchmark variant has an unsupported task/build target".into(),
                    ));
                }
                if variant
                    .source
                    .github_installation_id
                    != first.github_installation_id
                    || variant.source.source != first.source
                    || variant.source.intent != first.intent
                {
                    return Err(SubmissionError::Invalid(
                        "benchmark variants must share installation, source, and intent".into(),
                    ));
                }
                if variant
                    .source
                    .workload_key
                    .as_deref()
                    != Some(&expected_workload_key)
                {
                    return Err(SubmissionError::Invalid(
                        "benchmark workload key differs from its frozen effective arguments".into(),
                    ));
                }
                if variant.requested_run_count <= 0 {
                    return Err(SubmissionError::Invalid(
                        "benchmark run count must be positive".into(),
                    ));
                }
            }
        }
        TaskPlan::BuildOnly(plan) => {
            if !matches!(queued, QueuedEventDetail::CacheWarm { .. }) {
                return Err(SubmissionError::Invalid(
                    "build-only plan has non-build queued provenance".into(),
                ));
            }
            validate_source(&plan.source)?;
            if plan.source.task_kind != TaskKind::BuildOnly
                || plan.source.build_target != BuildTarget::StacksBench
            {
                return Err(SubmissionError::Invalid(
                    "build-only plan has an unsupported task/build target".into(),
                ));
            }
        }
        TaskPlan::BlockValidation(plan) => {
            if !matches!(queued, QueuedEventDetail::BlockValidation { .. }) {
                return Err(SubmissionError::Invalid(
                    "validation plan has non-validation queued provenance".into(),
                ));
            }
            validate_source(&plan.source)?;
            if plan.source.task_kind != TaskKind::BlockValidation
                || plan.source.build_target != BuildTarget::StacksInspect
            {
                return Err(SubmissionError::Invalid(
                    "validation plan has an unsupported task/build target".into(),
                ));
            }
            sbgh_proto::TaskPayload::BlockValidation(plan.payload.clone())
                .validate()
                .map_err(|error| SubmissionError::Invalid(error.to_string()))?;
        }
    }

    match (&command.actor, &command.provenance.github, &command.provenance.slack) {
        (SubmissionActor::GithubUser { user_id }, Some(github), None)
            if github.triggering_user_id == Some(*user_id) => {}
        (SubmissionActor::SlackUser { team_id, user_id }, None, Some(slack))
            if &slack.team_id == team_id && !user_id.trim().is_empty() => {}
        (SubmissionActor::System, _, None) => {}
        _ => {
            return Err(SubmissionError::Invalid("actor and source provenance disagree".into()));
        }
    }

    // Deserialization above is the audit-shape validation. Keep the binding so
    // adding a new provenance variant cannot silently bypass this boundary.
    let _ = queued;
    Ok(())
}

pub fn prepare(command: SubmissionCommand) -> Result<PreparedSubmission, SubmissionError> {
    validate(&command)?;
    let material = DigestMaterial {
        contract_version: SUBMISSION_CONTRACT_VERSION,
        constraints: &command.constraints,
        task: (&command.task).into(),
    };
    let encoded = serde_json::to_vec(&material)
        .map_err(|error| SubmissionError::Persistence(anyhow::Error::new(error)))?;
    let request_digest = hex::encode(Sha256::digest(encoded));
    Ok(PreparedSubmission {
        command,
        contract_version: SUBMISSION_CONTRACT_VERSION,
        request_digest,
    })
}

pub async fn submit<S: SubmissionStore + ?Sized>(
    store: &S,
    command: SubmissionCommand,
) -> Result<SubmissionReceipt, SubmissionError> {
    let prepared = prepare(command)?;
    store
        .persist_submission(&prepared)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sbgh_core::models::{
        BuildTarget, GitRefKind, JobIntent, JobSource, QueuedEventDetail, TaskKind,
    };
    use sbgh_core::submission::{
        BenchmarkPlan, BenchmarkVariant, ProducerKey, ResolvedTaskSource, SchedulingConstraints,
        SubmissionProvenance,
    };

    fn command() -> SubmissionCommand {
        let effective_args = vec!["run".into(), "--count".into(), "1".into()];
        let workload_key = sbgh_core::bench_args::workload_key(&effective_args);
        let mut detail = serde_json::to_value(QueuedEventDetail::BranchPush {
            branch: "main".into(),
            trigger_id: 1,
            bench_args: Some("run --count 1".into()),
        })
        .unwrap();
        detail
            .as_object_mut()
            .unwrap()
            .insert("effective_args".into(), serde_json::json!(effective_args));
        SubmissionCommand {
            actor: SubmissionActor::System,
            producer_key: ProducerKey {
                namespace: "test".into(),
                key: "one".into(),
            },
            constraints: SchedulingConstraints::default(),
            task: TaskPlan::Benchmark(BenchmarkPlan {
                variants: vec![BenchmarkVariant {
                    source: ResolvedTaskSource {
                        github_installation_id: 1,
                        github_repo_id: 2,
                        source: JobSource::Daemon,
                        intent: JobIntent::BaselineBenchmark,
                        task_kind: TaskKind::Benchmark,
                        build_target: BuildTarget::StacksBench,
                        git_ref_kind: GitRefKind::Commit,
                        git_ref_display: "main".into(),
                        commit: "a".repeat(40),
                        committed_at: None,
                        workload_key: Some(workload_key),
                    },
                    requested_run_count: 1,
                    baseline_calibration_id: None,
                }],
                effective_args,
            }),
            provenance: SubmissionProvenance {
                queued_event_detail: detail,
                github: None,
                slack: None,
            },
        }
    }

    #[test]
    fn digest_is_stable_and_covers_executable_demand() {
        let first = prepare(command()).unwrap();
        let second = prepare(command()).unwrap();
        assert_eq!(first.request_digest, second.request_digest);

        let mut changed = command();
        let TaskPlan::Benchmark(plan) = &mut changed.task else { unreachable!() };
        plan.variants[0].requested_run_count = 2;
        assert_ne!(
            first.request_digest,
            prepare(changed)
                .unwrap()
                .request_digest
        );
    }

    #[test]
    fn effective_arguments_must_match_audit_provenance() {
        let mut invalid = command();
        invalid
            .provenance
            .queued_event_detail["effective_args"] = serde_json::json!(["different"]);
        assert!(matches!(
            prepare(invalid),
            Err(SubmissionError::Invalid(message)) if message.contains("effective_args")
        ));
    }

    #[test]
    fn measurement_profile_constraints_are_benchmark_only() {
        let mut invalid = command();
        let source = invalid
            .task
            .first_source()
            .unwrap()
            .clone();
        invalid.task = TaskPlan::BuildOnly(BuildOnlyPlan {
            source: ResolvedTaskSource {
                intent: JobIntent::CacheWarm,
                task_kind: TaskKind::BuildOnly,
                ..source
            },
        });
        invalid
            .provenance
            .queued_event_detail = serde_json::to_value(QueuedEventDetail::CacheWarm {
            trigger_id: 1,
            git_ref: "main".into(),
            commit: "a".repeat(40),
            build_target: BuildTarget::StacksBench,
        })
        .unwrap();
        invalid
            .constraints
            .required_measurement_profile = Some("bench-a".into());

        assert!(matches!(
            prepare(invalid),
            Err(SubmissionError::Invalid(message)) if message.contains("benchmark submissions")
        ));
    }

    #[test]
    fn provenance_and_display_metadata_do_not_change_executable_digest() {
        let first = prepare(command()).unwrap();
        let mut same_demand = command();
        let TaskPlan::Benchmark(plan) = &mut same_demand.task else { unreachable!() };
        plan.variants[0].source.source = JobSource::Cli;
        plan.variants[0]
            .source
            .git_ref_display = "a human-friendly alias".into();
        same_demand.producer_key.key = "another delivery".into();
        assert_eq!(
            first.request_digest,
            prepare(same_demand)
                .unwrap()
                .request_digest
        );
    }
}
