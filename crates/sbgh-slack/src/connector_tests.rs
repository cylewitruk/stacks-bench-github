use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use sbgh_core::models::{JobSource, QueuedEventDetail, TaskKind};
use sbgh_core::workload::{BlockSelector, WorkloadSpec, WorkloadTarget};
use sbgh_intent::{
    IntentOutcome, IntentProviderError, IntentResolver, RequestedSource, ValidationSelection,
};

use super::*;
use crate::test_support::{FakeSlackClient, RecordingTaskSubmissionPort};

fn config() -> SlackConnectorConfig {
    SlackConnectorConfig::new("develop", vec!["T_OK".into()], vec!["U_OK".into()], None)
}

fn event(text: &str) -> MentionEvent {
    MentionEvent {
        team_id: "T_OK".into(),
        user: "U_OK".into(),
        channel: "C1".into(),
        message_ts: "1700000000.000100".into(),
        text: text.into(),
    }
}

fn connector(
    queue: Arc<RecordingTaskSubmissionPort>,
    slack: Arc<FakeSlackClient>,
) -> SlackConnector {
    SlackConnector::new(config(), queue, slack)
        .with_intent_resolver(Arc::new(FakeIntentResolver::resolved(natural_language_spec())), 60)
}

struct FakeIntentResolver {
    calls: AtomicUsize,
    outcome: Result<IntentOutcome, String>,
}

impl FakeIntentResolver {
    fn benchmark(request: BenchmarkRequest) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcome: Ok(IntentOutcome::Resolved(UserIntent::Create(
                TaskCreationIntent::Benchmark(request),
            ))),
        }
    }

    fn resolved(spec: WorkloadSpec) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcome: Ok(IntentOutcome::Resolved(UserIntent::Create(
                TaskCreationIntent::Benchmark(BenchmarkRequest::Single(spec)),
            ))),
        }
    }

    fn error(message: &str) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcome: Err(message.into()),
        }
    }

    fn invalid(message: &str) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcome: Ok(IntentOutcome::Invalid(sbgh_intent::IntentInvalid::new(message))),
        }
    }

    fn block_validation(revision: &str) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcome: Ok(IntentOutcome::Resolved(UserIntent::Create(
                TaskCreationIntent::BlockValidation(BlockValidationIntent {
                    source: RequestedSource {
                        repository: None,
                        revision: Some(revision.into()),
                    },
                    selection: ValidationSelection::Recent { block_count: None },
                }),
            ))),
        }
    }

    fn calls(&self) -> usize {
        self.calls
            .load(Ordering::SeqCst)
    }
}

#[async_trait]
impl IntentResolver for FakeIntentResolver {
    async fn resolve(&self, _text: &str) -> Result<IntentOutcome, IntentProviderError> {
        self.calls
            .fetch_add(1, Ordering::SeqCst);
        self.outcome
            .clone()
            .map_err(IntentProviderError::Message)
    }
}

fn natural_language_spec() -> WorkloadSpec {
    WorkloadSpec {
        target: WorkloadTarget::Blocks(vec![BlockSelector::Height(10)]),
        clean_repetitions: 1,
        warmup: Some(0),
        rev: Some("feature/natural-language".into()),
    }
}

#[tokio::test]
async fn authorized_request_posts_one_snapshot_and_enqueues() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    connector(queue.clone(), slack.clone())
        .handle_mention(event("<@BOT> benchmark block 184231"))
        .await;

    let jobs = queue.jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].source, JobSource::Slack);
    assert_eq!(slack.posts().len(), 1);
    assert_eq!(
        queue
            .plan_message_ts(jobs[0].id)
            .as_deref(),
        Some(slack.posts()[0].ts.as_str())
    );
    let detail: QueuedEventDetail = serde_json::from_value(
        queue
            .queued_event_detail(jobs[0].id)
            .unwrap(),
    )
    .unwrap();
    let QueuedEventDetail::SlackAdhoc { reporting_identity, .. } = detail else {
        panic!("expected Slack provenance");
    };
    assert_eq!(
        reporting_identity.as_deref(),
        Some(
            slack.posts()[0]
                .identity
                .as_str()
        )
    );
    assert_eq!(
        slack.reaction_calls(),
        vec![
            ("add".into(), ACK_REACTION.into()),
            ("remove".into(), ACK_REACTION.into()),
            ("add".into(), QUEUED_REACTION.into()),
        ]
    );
    assert_eq!(slack.reactions().len(), 1);
    assert_eq!(slack.reactions()[0].2, QUEUED_REACTION);
}

#[tokio::test]
async fn redelivery_reconciles_by_stable_request_identity() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    let connector = connector(queue.clone(), slack.clone());
    connector
        .handle_mention(event("<@BOT> benchmark block 184231"))
        .await;
    connector
        .handle_mention(event("<@BOT> benchmark block 184231"))
        .await;
    assert_eq!(slack.posts().len(), 1, "the canonical message is adopted");
    assert_eq!(queue.jobs().len(), 1, "stable producer identity deduplicates work");
}

#[tokio::test]
async fn lost_post_response_enqueues_without_timestamp_for_claim_time_reconciliation() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    slack.lose_next_post_response();
    connector(queue.clone(), slack.clone())
        .handle_mention(event("<@BOT> benchmark block 184231"))
        .await;
    let job = queue
        .jobs()
        .pop()
        .expect("job still enqueued");
    assert!(
        queue
            .plan_message_ts(job.id)
            .is_none()
    );
    assert_eq!(slack.posts().len(), 1, "Slack accepted the post");
}

#[tokio::test]
async fn unauthorized_request_neither_posts_nor_enqueues() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    let mut mention = event("<@BOT> benchmark block 184231");
    mention.user = "U_DENIED".into();
    connector(queue.clone(), slack.clone())
        .handle_mention(mention)
        .await;
    assert!(queue.jobs().is_empty());
    assert!(slack.posts().is_empty());
    assert_eq!(slack.ephemeral().len(), 1);
}

#[tokio::test]
async fn enqueue_failure_marks_the_single_snapshot_failed() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    queue.fail_create();
    let slack = Arc::new(FakeSlackClient::default());
    connector(queue.clone(), slack.clone())
        .handle_mention(event("<@BOT> benchmark block 184231"))
        .await;
    assert!(queue.jobs().is_empty());
    assert_eq!(slack.posts().len(), 1);
    assert_eq!(slack.updates().len(), 1);
    assert!(
        slack.updates()[0]
            .text
            .contains("Couldn't enqueue")
    );
}

#[tokio::test]
async fn enqueue_failure_redelivery_never_restores_queued_snapshot() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    queue.fail_create();
    let slack = Arc::new(FakeSlackClient::default());
    let connector = connector(queue, slack.clone());

    connector
        .handle_mention(event("<@BOT> benchmark block 184231"))
        .await;
    connector
        .handle_mention(event("<@BOT> benchmark block 184231"))
        .await;

    assert_eq!(slack.posts().len(), 1);
    assert_eq!(slack.updates().len(), 1);
    assert!(
        slack.posts()[0]
            .text
            .contains("Couldn't enqueue")
    );
    assert_eq!(slack.posts()[0].snapshot_version, 1);
}

#[tokio::test]
async fn invalid_provider_result_and_repetition_cap_reject_without_enqueue() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    connector(queue.clone(), slack.clone())
        .with_intent_resolver(Arc::new(FakeIntentResolver::invalid("only one target mode")), 5)
        .with_max_clean_repetitions(2)
        .handle_mention(event("<@BOT> benchmark incompatible targets"))
        .await;
    let mut too_many = natural_language_spec();
    too_many.clean_repetitions = 3;
    connector(queue.clone(), slack.clone())
        .with_intent_resolver(Arc::new(FakeIntentResolver::resolved(too_many)), 5)
        .with_max_clean_repetitions(2)
        .handle_mention(event("<@BOT> benchmark this three times"))
        .await;

    assert!(queue.jobs().is_empty());
    assert!(slack.reactions().is_empty());
    assert_eq!(slack.ephemeral().len(), 2);
    assert!(
        slack.ephemeral()[0]
            .2
            .contains("only one target mode")
    );
    assert!(
        slack.ephemeral()[1]
            .2
            .contains("too many clean repetitions")
    );
}

#[tokio::test]
async fn clean_repetitions_require_cache_and_preserve_requested_count_when_enabled() {
    let mut repeated = natural_language_spec();
    repeated.clean_repetitions = 2;
    let disabled_queue = Arc::new(RecordingTaskSubmissionPort::default());
    let disabled_slack = Arc::new(FakeSlackClient::default());
    connector(disabled_queue.clone(), disabled_slack.clone())
        .with_intent_resolver(Arc::new(FakeIntentResolver::resolved(repeated.clone())), 5)
        .with_max_clean_repetitions(5)
        .handle_mention(event("<@BOT> benchmark this twice"))
        .await;
    assert!(
        disabled_queue
            .jobs()
            .is_empty()
    );
    assert!(
        disabled_slack.ephemeral()[0]
            .2
            .contains("binary cache")
    );

    let enabled_queue = Arc::new(RecordingTaskSubmissionPort::default());
    let enabled_slack = Arc::new(FakeSlackClient::default());
    connector(enabled_queue.clone(), enabled_slack)
        .with_intent_resolver(Arc::new(FakeIntentResolver::resolved(repeated)), 5)
        .with_max_clean_repetitions(5)
        .with_binary_cache_enabled(true)
        .handle_mention(event("<@BOT> benchmark this twice"))
        .await;
    let job = &enabled_queue.jobs()[0];
    let detail: QueuedEventDetail = serde_json::from_value(
        enabled_queue
            .queued_event_detail(job.id)
            .unwrap(),
    )
    .unwrap();
    let QueuedEventDetail::SlackAdhoc { clean_repetitions, .. } = detail else {
        panic!("expected Slack ad-hoc detail");
    };
    assert_eq!(clean_repetitions, 2);
}

#[tokio::test]
async fn provider_comparison_enqueues_ordered_variants() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    let comparison = sbgh_core::workload::ComparisonRequest {
        workload: WorkloadSpec {
            target: WorkloadTarget::BlockRange { start: 100, end: 102 },
            clean_repetitions: 1,
            warmup: None,
            rev: None,
        },
        variants: vec![
            sbgh_core::workload::ComparisonVariant { rev: "baseline".into() },
            sbgh_core::workload::ComparisonVariant { rev: "candidate".into() },
        ],
    };
    connector(queue.clone(), slack)
        .with_intent_resolver(
            Arc::new(FakeIntentResolver::benchmark(BenchmarkRequest::Comparison(comparison))),
            5,
        )
        .with_binary_cache_enabled(true)
        .handle_mention(event("<@BOT> compare baseline and candidate over blocks 100 to 102"))
        .await;

    let job = &queue.jobs()[0];
    let variants = queue.requested_variants_for_submission(job.task_submission_id);
    assert_eq!(
        variants
            .iter()
            .map(|variant| variant.rev.as_str())
            .collect::<Vec<_>>(),
        vec!["baseline", "candidate"]
    );
    let keys = variants
        .iter()
        .map(|variant| Some(variant.workload_key.as_str()))
        .collect::<Vec<_>>();
    assert!(keys[0].is_some(), "Slack enqueue must snapshot a workload key");
    assert_eq!(keys[0], keys[1], "comparison variants share one workload");
}

#[tokio::test]
async fn natural_language_resolution_and_rate_limit_are_preserved() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    let resolver = Arc::new(FakeIntentResolver::resolved(natural_language_spec()));
    let connector =
        connector(queue.clone(), slack.clone()).with_intent_resolver(resolver.clone(), 1);

    connector
        .handle_mention(event("<@BOT> benchmark block ten naturally"))
        .await;
    connector
        .handle_mention(event("<@BOT> benchmark block eleven naturally"))
        .await;

    assert_eq!(resolver.calls(), 1);
    assert_eq!(queue.jobs().len(), 1);
    assert_eq!(queue.jobs()[0].git_ref_display, "feature/natural-language");
    assert!(
        slack.ephemeral()[0]
            .2
            .contains("too many task requests")
    );
}

#[tokio::test]
async fn validation_requests_are_always_provider_resolved() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    let resolver = Arc::new(FakeIntentResolver::block_validation("natural-validation"));
    let connector =
        connector(queue.clone(), slack.clone()).with_intent_resolver(resolver.clone(), 5);

    connector
        .handle_mention(event("<@BOT> validate --rev explicit-validation"))
        .await;
    assert_eq!(resolver.calls(), 0, "flag-like input is rejected before the provider");
    assert!(queue.jobs().is_empty());
    assert!(
        slack.ephemeral()[0]
            .2
            .contains("natural language")
    );

    let mut natural = event("<@BOT> please validate the Nakamoto blocks on my revision");
    natural.message_ts = "1700000000.000200".into();
    connector
        .handle_mention(natural)
        .await;
    let mut another = event("<@BOT> validate the recent chainstate on my revision");
    another.message_ts = "1700000000.000300".into();
    connector
        .handle_mention(another)
        .await;

    assert_eq!(resolver.calls(), 2);
    let jobs = queue.jobs();
    assert_eq!(jobs.len(), 2);
    assert!(
        jobs.iter()
            .all(|job| job.task_kind == TaskKind::BlockValidation)
    );
    assert!(
        jobs.iter()
            .all(|job| job.git_ref_display == "natural-validation")
    );
    assert!(
        slack
            .posts()
            .iter()
            .all(|post| post
                .text
                .contains("Block validation"))
    );
}

#[tokio::test]
async fn task_specific_authorization_is_checked_after_resolution() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    let resolver = Arc::new(FakeIntentResolver::block_validation("candidate"));
    let cfg = SlackConnectorConfig::new(
        "develop",
        vec!["T_OK".into()],
        vec!["U_OK".into()],
        Some(Vec::new()),
    );
    SlackConnector::new(cfg, queue.clone(), slack.clone())
        .with_intent_resolver(resolver.clone(), 5)
        .handle_mention(event("<@BOT> run validation on candidate"))
        .await;

    assert_eq!(resolver.calls(), 1, "partially entitled user may resolve once");
    assert!(queue.jobs().is_empty());
    assert!(slack.posts().is_empty(), "no public snapshot follows task denial");
    assert!(
        slack.ephemeral()[0]
            .2
            .contains("not authorized")
    );
}

#[tokio::test]
async fn validation_only_user_is_admitted_but_cannot_submit_benchmark() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    let cfg = SlackConnectorConfig::new(
        "develop",
        vec!["T_OK".into()],
        Vec::new(),
        Some(vec!["U_OK".into()]),
    );
    SlackConnector::new(cfg, queue.clone(), slack.clone())
        .with_intent_resolver(Arc::new(FakeIntentResolver::resolved(natural_language_spec())), 5)
        .handle_mention(event("<@BOT> benchmark block 184231"))
        .await;

    assert!(queue.jobs().is_empty());
    assert!(slack.posts().is_empty());
    assert!(
        slack.ephemeral()[0]
            .2
            .contains("not authorized")
    );
}

#[tokio::test]
async fn provider_failure_is_safe_and_authz_precedes_provider_calls() {
    let queue = Arc::new(RecordingTaskSubmissionPort::default());
    let slack = Arc::new(FakeSlackClient::default());
    let resolver = Arc::new(FakeIntentResolver::error("provider secret detail"));
    let connector =
        connector(queue.clone(), slack.clone()).with_intent_resolver(resolver.clone(), 5);

    connector
        .handle_mention(event("<@BOT> benchmark block ten naturally"))
        .await;
    let mut denied = event("<@BOT> benchmark block eleven naturally");
    denied.user = "U_DENIED".into();
    connector
        .handle_mention(denied)
        .await;

    assert_eq!(resolver.calls(), 1);
    assert!(queue.jobs().is_empty());
    assert!(
        slack.ephemeral()[0]
            .2
            .contains("couldn't resolve")
    );
    assert!(
        !slack.ephemeral()[0]
            .2
            .contains("provider secret detail")
    );
    assert!(
        slack.ephemeral()[1]
            .2
            .contains("not authorized")
    );
}
