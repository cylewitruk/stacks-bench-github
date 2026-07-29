use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use sbgh_core::models::{JobSource, QueuedEventDetail};
use sbgh_core::workload::{BlockSelector, WorkloadSpec, WorkloadTarget};
use sbgh_intent::{IntentOutcome, IntentProviderError, IntentResolver};

use super::*;
use crate::test_support::{FakeSlackClient, RecordingBenchmarkQueue};

fn config() -> SlackConnectorConfig {
    SlackConnectorConfig::new("develop", vec!["T_OK".into()], vec!["U_OK".into()])
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

fn connector(queue: Arc<RecordingBenchmarkQueue>, slack: Arc<FakeSlackClient>) -> SlackConnector {
    SlackConnector::new(config(), queue, slack)
}

struct FakeIntentResolver {
    calls: AtomicUsize,
    outcome: Result<IntentOutcome, String>,
}

impl FakeIntentResolver {
    fn resolved(spec: WorkloadSpec) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcome: Ok(IntentOutcome::Resolved(BenchmarkRequest::Single(spec))),
        }
    }

    fn error(message: &str) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcome: Err(message.into()),
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
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    let slack = Arc::new(FakeSlackClient::default());
    connector(queue.clone(), slack.clone())
        .handle_mention(event("<@BOT> bench --block 184231"))
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
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    let slack = Arc::new(FakeSlackClient::default());
    let connector = connector(queue.clone(), slack.clone());
    connector
        .handle_mention(event("<@BOT> bench --block 184231"))
        .await;
    connector
        .handle_mention(event("<@BOT> bench --block 184231"))
        .await;
    assert_eq!(slack.posts().len(), 1, "the canonical message is adopted");
    assert_eq!(queue.jobs().len(), 2, "enqueue idempotency remains outside this iteration");
}

#[tokio::test]
async fn lost_post_response_enqueues_without_timestamp_for_claim_time_reconciliation() {
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    let slack = Arc::new(FakeSlackClient::default());
    slack.lose_next_post_response();
    connector(queue.clone(), slack.clone())
        .handle_mention(event("<@BOT> bench --block 184231"))
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
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    let slack = Arc::new(FakeSlackClient::default());
    let mut mention = event("<@BOT> bench --block 184231");
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
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    queue.fail_create();
    let slack = Arc::new(FakeSlackClient::default());
    connector(queue.clone(), slack.clone())
        .handle_mention(event("<@BOT> bench --block 184231"))
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
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    queue.fail_create();
    let slack = Arc::new(FakeSlackClient::default());
    let connector = connector(queue, slack.clone());

    connector
        .handle_mention(event("<@BOT> bench --block 184231"))
        .await;
    connector
        .handle_mention(event("<@BOT> bench --block 184231"))
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
async fn malformed_workload_and_repetition_cap_reject_without_enqueue() {
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    let slack = Arc::new(FakeSlackClient::default());
    let connector = connector(queue.clone(), slack.clone()).with_max_clean_repetitions(2);
    let txid = format!("0x{}", "1".repeat(64));

    connector
        .handle_mention(event(&format!("<@BOT> bench --block 1 --txid {txid}")))
        .await;
    connector
        .handle_mention(event("<@BOT> bench --block 1 --repetitions 3"))
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
    let disabled_queue = Arc::new(RecordingBenchmarkQueue::default());
    let disabled_slack = Arc::new(FakeSlackClient::default());
    connector(disabled_queue.clone(), disabled_slack.clone())
        .with_max_clean_repetitions(5)
        .handle_mention(event("<@BOT> bench --block 1 --repetitions 2"))
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

    let enabled_queue = Arc::new(RecordingBenchmarkQueue::default());
    let enabled_slack = Arc::new(FakeSlackClient::default());
    connector(enabled_queue.clone(), enabled_slack)
        .with_max_clean_repetitions(5)
        .with_binary_cache_enabled(true)
        .handle_mention(event("<@BOT> bench --block 1 --repetitions 2"))
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
async fn deterministic_comparison_enqueues_ordered_variants() {
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    let slack = Arc::new(FakeSlackClient::default());
    connector(queue.clone(), slack)
        .with_binary_cache_enabled(true)
        .handle_mention(event(
            "<@BOT> bench --start-at 100 --count 3 --rev baseline \
             --compare-rev candidate --repetitions 1",
        ))
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
    let queue = Arc::new(RecordingBenchmarkQueue::default());
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
            .contains("too many benchmark requests")
    );
}

#[tokio::test]
async fn provider_failure_is_safe_and_authz_precedes_provider_calls() {
    let queue = Arc::new(RecordingBenchmarkQueue::default());
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
