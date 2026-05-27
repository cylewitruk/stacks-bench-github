//! Webhook processor (slices 2a + 2b).
//!
//! Pulls webhook rows from `github_webhook` via [`WebhookInbox`], hands
//! each to a pluggable [`Classifier`], and writes the resulting
//! outcome back. Implements the queue state machine — claim, terminate,
//! retry-with-backoff, permanent-failure-on-attempts-exhausted,
//! stuck-claim sweep — and runs concurrently with the legacy job
//! `Runner` in production via `tokio::try_join!` from `main`.
//!
//! Slice 2a built the scaffold + lifecycle tests; slice 2b plugged in
//! [`BasicClassifier`] (the Phase 1 production classifier) and started
//! the loop. Later slices (3–7) replace individual branches of
//! `BasicClassifier` with richer logic (installer gate, lineage,
//! policies, user roles, PR materialization); slice 9 changes the
//! `issue_comment` `/benchmark` branch to actually create `job` rows.
//!
//! The scaffold is structured so that every state transition is
//! testable in isolation via [`WebhookProcessor::process_one`], and
//! the long-running [`WebhookProcessor::run`] just composes
//! `process_one` with periodic sweeps and idle backoff.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use sbgh_core::Result;
use sbgh_core::db::{ClaimedWebhook, WebhookInbox};
use sbgh_core::github::{IssueCommentEvent, parse_command};
use sbgh_core::models::WebhookOutcome;

/// What a [`Classifier`] decides to do with a claimed webhook.
#[derive(Debug, Clone)]
pub enum ClassifyOutcome {
    /// Final decision; outcome's terminal status is set immediately.
    Terminal(WebhookOutcome),
    /// Transient failure. The processor records the error, increments
    /// attempts, schedules a backoff retry, or — if attempts have run
    /// out — promotes to a permanent failure.
    ///
    /// BasicClassifier (slice 2b) never emits this — its classifications
    /// are all terminal. Later slices that hit GitHub APIs will use it
    /// for transient network/rate-limit failures.
    #[allow(dead_code)]
    Retryable(String),
}

#[async_trait]
pub trait Classifier: Send + Sync + 'static {
    /// Event types this classifier can produce a terminal outcome
    /// for. The processor passes this list to `WebhookInbox::claim_next`
    /// so rows for event types not on the list are LEFT in `received`
    /// status — waiting for a future slice that adds the relevant
    /// classifier branch. This is what prevents slice 2b from
    /// terminalizing rows that slices 3-7 will need to consume
    /// (e.g., `installation.created`, `push`, etc.).
    fn supported_event_types(&self) -> &'static [&'static str];

    async fn classify(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome;
}

/// Test-only no-op classifier: everything terminates as
/// `ignored_action`. Used by lifecycle tests that don't care about
/// classification semantics. Production uses [`BasicClassifier`].
#[cfg(test)]
pub struct NoopClassifier;

#[cfg(test)]
#[async_trait]
impl Classifier for NoopClassifier {
    fn supported_event_types(&self) -> &'static [&'static str] {
        // Wide list for lifecycle tests: NoopClassifier handles whatever
        // gets seeded, so tests don't have to remember to set
        // event_type='issue_comment'.
        &[
            "issue_comment",
            "push",
            "pull_request",
            "create",
            "installation",
            "installation_repositories",
        ]
    }

    async fn classify(&self, _: &ClaimedWebhook) -> ClassifyOutcome {
        ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)
    }
}

/// Slice 2b classifier. ONLY claims and classifies `issue_comment`
/// rows. Other supported event types (`push`, `pull_request`, `create`,
/// `installation`, `installation_repositories`) are deliberately NOT
/// in the supported list — those rows accumulate in `received` until
/// a future slice (3-7) adds a classifier branch for them. This
/// preserves the "supporting state accumulates across Phase 1" rollout
/// pattern: terminalizing rows now would prevent later slices from
/// processing the events they need (e.g., an `installation.created`
/// terminally-ignored in slice 2b would never create its
/// `github_installation` row in slice 3).
///
/// For the `issue_comment` rows it DOES claim:
/// - non-`created` actions → `ignored_action`
/// - `created` on a non-PR issue → `ignored_action`
/// - `created` on a PR with no `/benchmark` → `ignored_no_command`
/// - `created` on a PR with malformed `/benchmark` → `ignored_no_command`
///   (schema has no distinct "malformed command" outcome; bucketed with
///   no-command for now)
/// - `created` on a PR with valid `/benchmark` → `ignored_action` in Phase 1.
///   Slice 9 changes this branch to `enqueued_job` + creates the new `job` row.
///   The legacy handler→`jobs` path continues to actually run the bench in the
///   meantime.
/// - NULL / unparseable payload → `error` (can't classify; better terminal than
///   infinite retry)
pub struct BasicClassifier;

#[async_trait]
impl Classifier for BasicClassifier {
    fn supported_event_types(&self) -> &'static [&'static str] {
        &["issue_comment"]
    }

    async fn classify(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
        match webhook.event_type.as_str() {
            "issue_comment" => classify_issue_comment(webhook),
            other => {
                // Shouldn't happen — the claim filter restricts to
                // `issue_comment` — but if it does, fail terminally
                // rather than loop on retries.
                tracing::error!(
                    event_type = other,
                    "BasicClassifier received an unsupported event type; check claim filter"
                );
                ClassifyOutcome::Terminal(WebhookOutcome::Error)
            }
        }
    }
}

fn classify_issue_comment(webhook: &ClaimedWebhook) -> ClassifyOutcome {
    // Only the `created` action carries a comment body we'd act on.
    if webhook.action.as_deref() != Some("created") {
        return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
    }

    let Some(payload) = webhook.payload.as_ref() else {
        // Handler stored only metadata (parse failed at HTTP-receipt).
        // Without the body we can't extract the comment; record
        // terminally instead of retrying.
        return ClassifyOutcome::Terminal(WebhookOutcome::Error);
    };

    let event: IssueCommentEvent = match serde_json::from_value(payload.clone()) {
        Ok(e) => e,
        Err(_) => {
            return ClassifyOutcome::Terminal(WebhookOutcome::Error);
        }
    };

    if event
        .issue
        .pull_request
        .is_none()
    {
        // Non-PR issue comment. Nothing we'd ever act on.
        return ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction);
    }

    match parse_command(&event.comment.body) {
        Ok(Some(_)) => {
            // PR comment with a recognized command. Slice 9 turns
            // this into `enqueued_job` + creates the new job row; in
            // Phase 1 the legacy handler→`jobs` path already runs
            // the actual bench, so the processor classifies the
            // inbox row as ignored_action.
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)
        }
        // Schema has no distinct "command was malformed" outcome;
        // bucket malformed/missing both under `ignored_no_command`.
        Ok(None) | Err(_) => ClassifyOutcome::Terminal(WebhookOutcome::IgnoredNoCommand),
    }
}

/// Tunables. Reasonable defaults are picked to play nicely with
/// GitHub's redelivery cadence and a single-orchestrator deployment.
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    /// Permanent-failure threshold: when `attempts >= max_attempts`
    /// after a transient failure, the row goes to `failed` instead of
    /// `retryable_error`.
    pub max_attempts: i32,
    /// First retry waits this long; subsequent retries double until
    /// `backoff_max`.
    pub backoff_base: chrono::Duration,
    pub backoff_max: chrono::Duration,
    /// A `processing` row whose `claimed_at` exceeds this is presumed
    /// abandoned and reset to `retryable_error` by the next sweep.
    pub claim_lease: chrono::Duration,
    /// Sleep when no rows are claimable.
    pub idle_sleep: std::time::Duration,
    /// How often `sweep_stuck_claims` runs from inside the main loop.
    pub sweep_interval: std::time::Duration,
    /// Run loop bails after this many consecutive iteration errors
    /// (claim / sweep / row processing). Forces a systemd restart so
    /// persistent infrastructure problems (DB down, grants revoked,
    /// schema drift) are surfaced as a crash rather than spin-logged
    /// silently. Per-row classification errors don't count — those
    /// land as terminal `error` rows.
    pub max_consecutive_errors: u32,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            backoff_base: chrono::Duration::seconds(30),
            backoff_max: chrono::Duration::minutes(15),
            claim_lease: chrono::Duration::minutes(5),
            idle_sleep: std::time::Duration::from_secs(2),
            sweep_interval: std::time::Duration::from_secs(60),
            max_consecutive_errors: 10,
        }
    }
}

pub struct WebhookProcessor {
    inbox: Arc<dyn WebhookInbox>,
    classifier: Arc<dyn Classifier>,
    config: ProcessorConfig,
}

impl WebhookProcessor {
    pub fn new(
        inbox: Arc<dyn WebhookInbox>,
        classifier: Arc<dyn Classifier>,
        config: ProcessorConfig,
    ) -> Self {
        Self { inbox, classifier, config }
    }

    /// Claim + classify + write outcome for a single row. Returns
    /// `Ok(true)` if a row was processed, `Ok(false)` if the inbox was
    /// empty (idle). The claim is restricted to event types the
    /// classifier knows how to handle; rows for other types stay
    /// `received` for a future processor with a wider filter.
    pub async fn process_one(&self) -> Result<bool> {
        let supported = self
            .classifier
            .supported_event_types();
        let Some(claimed) = self
            .inbox
            .claim_next(supported)
            .await?
        else {
            return Ok(false);
        };
        let id = claimed.id;
        let token = claimed.claim_token;
        match self
            .classifier
            .classify(&claimed)
            .await
        {
            ClassifyOutcome::Terminal(outcome) => {
                self.inbox
                    .complete(id, token, outcome)
                    .await?;
            }
            ClassifyOutcome::Retryable(err) => {
                // attempts is the value BEFORE this run; the DB
                // increments it inside record_retryable_error. We
                // compare against max_attempts using the value the
                // increment will produce (claimed.attempts + 1).
                let next_attempts = claimed
                    .attempts
                    .saturating_add(1);
                if next_attempts >= self.config.max_attempts {
                    self.inbox
                        .record_permanent_failure(id, token, &err)
                        .await?;
                } else {
                    let delay = backoff_delay(
                        next_attempts,
                        self.config.backoff_base,
                        self.config.backoff_max,
                    );
                    let next_at = Utc::now() + delay;
                    self.inbox
                        .record_retryable_error(id, token, &err, next_at)
                        .await?;
                }
            }
        }
        Ok(true)
    }

    /// Long-running loop: alternates `process_one` with periodic
    /// stuck-claim sweeps and idle backoff. Per-iteration errors
    /// (claim / sweep / row processing) are logged and the loop
    /// continues so a transient hiccup doesn't crash the processor.
    /// BUT: after `max_consecutive_errors` consecutive failures of
    /// EITHER category — process_one or sweep — the loop bails,
    /// forcing a systemd restart rather than silently spin-logging
    /// through a persistent DB or grant problem.
    ///
    /// The two counters are tracked independently so a persistent
    /// sweep failure (e.g., a grant revoked on a column the sweep
    /// updates) can't be masked by an otherwise-healthy process loop —
    /// without the split, a successful `process_one` after a failed
    /// sweep would reset the shared counter and the sweep could stay
    /// broken indefinitely.
    pub async fn run(&self) -> Result<()> {
        let mut last_sweep = Instant::now();
        let mut consecutive_sweep_errors: u32 = 0;
        let mut consecutive_process_errors: u32 = 0;
        loop {
            if last_sweep.elapsed() >= self.config.sweep_interval {
                match self
                    .inbox
                    .sweep_stuck_claims(self.config.claim_lease)
                    .await
                {
                    Ok(n) if n > 0 => {
                        tracing::warn!(recovered = n, "stuck-claim sweep recovered rows");
                        consecutive_sweep_errors = 0;
                    }
                    Ok(_) => {
                        consecutive_sweep_errors = 0;
                    }
                    Err(e) => {
                        tracing::error!(error = ?e, "stuck-claim sweep failed");
                        consecutive_sweep_errors += 1;
                    }
                }
                last_sweep = Instant::now();
            }

            match self.process_one().await {
                Ok(true) => {
                    consecutive_process_errors = 0;
                }
                Ok(false) => {
                    consecutive_process_errors = 0;
                    tokio::time::sleep(self.config.idle_sleep).await;
                }
                Err(e) => {
                    tracing::error!(error = ?e, "webhook processor iteration failed");
                    consecutive_process_errors += 1;
                    tokio::time::sleep(self.config.idle_sleep).await;
                }
            }

            // Either category persistently broken → bail. Common root
            // causes: DB pool exhausted, role grants revoked, schema
            // drift mid-deploy.
            let max = self
                .config
                .max_consecutive_errors;
            if consecutive_process_errors >= max || consecutive_sweep_errors >= max {
                return Err(sbgh_core::Error::Other(anyhow::anyhow!(
                    "webhook processor bailing for systemd restart (consecutive process errors: \
                     {consecutive_process_errors}, consecutive sweep errors: \
                     {consecutive_sweep_errors})"
                )));
            }
        }
    }
}

/// Exponential backoff: `base * 2^(attempt-1)`, capped at `max`.
/// `attempt` is the 1-indexed attempt number (1 for the first retry).
fn backoff_delay(attempt: i32, base: chrono::Duration, max: chrono::Duration) -> chrono::Duration {
    let exp = attempt
        .saturating_sub(1)
        .clamp(0, 30) as u32;
    let factor = 1i64
        .checked_shl(exp)
        .unwrap_or(i64::MAX);
    let scaled_secs = base
        .num_seconds()
        .saturating_mul(factor);
    let raw = chrono::Duration::seconds(scaled_secs);
    if raw > max { max } else { raw }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use sbgh_core::db::{InMemoryWebhookInbox, SeedWebhook};
    use sbgh_core::models::{WebhookOutcome, WebhookStatus};

    use super::*;

    /// Wide event-type filter for tests that exercise the inbox
    /// lifecycle directly (i.e. not through a Classifier).
    const ALL_EVENT_TYPES: &[&str] = &[
        "issue_comment",
        "push",
        "pull_request",
        "create",
        "installation",
        "installation_repositories",
    ];

    fn fast_config() -> ProcessorConfig {
        // Tight defaults so backoff/sweep tests don't sit on
        // wall-clock. backoff_base of 1s and claim_lease of 100ms keep
        // tests deterministic without time mocking.
        ProcessorConfig {
            max_attempts: 3,
            backoff_base: chrono::Duration::seconds(1),
            backoff_max: chrono::Duration::seconds(10),
            claim_lease: chrono::Duration::milliseconds(100),
            idle_sleep: std::time::Duration::from_millis(10),
            sweep_interval: std::time::Duration::from_millis(50),
            max_consecutive_errors: 10,
        }
    }

    fn seed(inbox: &InMemoryWebhookInbox, delivery: &str, event: &str) -> i64 {
        inbox.seed(SeedWebhook {
            delivery_id: delivery.into(),
            event_type: event.into(),
            payload_size_bytes: 42,
            ..Default::default()
        })
    }

    /// Test classifier that pops a programmed outcome per call.
    /// Records every classified id for assertions.
    struct ScriptedClassifier {
        script: Mutex<Vec<ClassifyOutcome>>,
        seen: Mutex<Vec<i64>>,
    }

    impl ScriptedClassifier {
        fn new(script: Vec<ClassifyOutcome>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                seen: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl Classifier for ScriptedClassifier {
        fn supported_event_types(&self) -> &'static [&'static str] {
            ALL_EVENT_TYPES
        }

        async fn classify(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome {
            self.seen
                .lock()
                .unwrap()
                .push(webhook.id);
            self.script
                .lock()
                .unwrap()
                .remove(0)
        }
    }

    #[tokio::test]
    async fn process_one_terminates_with_outcome() {
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-1", "push");
        let classifier =
            ScriptedClassifier::new(vec![ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, fast_config());

        assert!(
            proc.process_one()
                .await
                .unwrap()
        );
        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::Ignored);
        assert_eq!(row.outcome, Some(WebhookOutcome::IgnoredAction));
        assert!(row.processed_at.is_some());
        assert!(row.claim_token.is_none(), "claim cleared on terminal");
    }

    #[tokio::test]
    async fn process_one_returns_false_when_empty() {
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let proc = WebhookProcessor::new(inbox, Arc::new(NoopClassifier), fast_config());
        assert!(
            !proc
                .process_one()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn retryable_increments_attempts_and_sets_backoff() {
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-2", "push");
        let classifier =
            ScriptedClassifier::new(vec![ClassifyOutcome::Retryable("transient".into())]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, fast_config());

        let before = Utc::now();
        assert!(
            proc.process_one()
                .await
                .unwrap()
        );
        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::RetryableError);
        assert_eq!(row.attempts, 1);
        assert_eq!(row.last_error.as_deref(), Some("transient"));
        // first retry waits ~1s with fast_config's base.
        assert!(
            row.next_attempt_at >= before + chrono::Duration::milliseconds(900),
            "backoff respected: next_attempt_at={}, before={}",
            row.next_attempt_at,
            before
        );
        assert!(row.claim_token.is_none());
        assert!(row.processed_at.is_none(), "not terminal");
    }

    #[tokio::test]
    async fn attempts_exhausted_promotes_to_permanent_failure() {
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-3", "push");
        let mut config = fast_config();
        config.max_attempts = 2;
        // Three retryable classifications scheduled, but only the
        // first two should run; the second hits max_attempts and
        // becomes permanent.
        let classifier = ScriptedClassifier::new(vec![
            ClassifyOutcome::Retryable("first".into()),
            ClassifyOutcome::Retryable("second".into()),
        ]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, config);

        // First retry → retryable_error, next_attempt_at = future.
        proc.process_one()
            .await
            .unwrap();
        // Make the row immediately claimable again.
        inbox.set_next_attempt_at(id, Utc::now());
        // Second attempt → next_attempts (2) >= max_attempts (2) →
        // permanent failure.
        proc.process_one()
            .await
            .unwrap();

        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::Failed);
        assert_eq!(row.outcome, Some(WebhookOutcome::Error));
        assert_eq!(row.last_error.as_deref(), Some("second"));
        assert!(row.processed_at.is_some());
        // Both transient + permanent failure paths increment attempts,
        // so after max_attempts=2 worth of failures the row reflects 2,
        // not 1.
        assert_eq!(
            row.attempts, 2,
            "permanent failure must also increment attempts so the count is accurate"
        );
    }

    #[tokio::test]
    async fn sweep_resets_stuck_processing_rows() {
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-4", "push");
        // Simulate a crashed processor: claim normally, then backdate
        // claimed_at past the lease window.
        let _ = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .expect("seeded row must be claimable");
        inbox.set_claimed_at(id, Utc::now() - chrono::Duration::seconds(60));

        let recovered = inbox
            .sweep_stuck_claims(chrono::Duration::seconds(10))
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::RetryableError);
        assert!(row.claim_token.is_none());
        assert!(row.claimed_at.is_none());
        assert!(
            row.last_error
                .as_deref()
                .unwrap_or("")
                .contains("stuck-claim sweep")
        );
    }

    #[tokio::test]
    async fn concurrent_claims_pick_disjoint_rows() {
        // Both calls run sequentially in this test (Mutex on the
        // in-memory state), but the semantic we verify is that each
        // claim_next returns a different row id — which is the
        // guarantee FOR UPDATE SKIP LOCKED gives in Postgres.
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id_a = seed(&inbox, "d-a", "push");
        let id_b = seed(&inbox, "d-b", "push");

        let claim1 = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .unwrap();
        let claim2 = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(claim1.id, claim2.id);
        // Both should be in our seeded set.
        assert!([id_a, id_b].contains(&claim1.id));
        assert!([id_a, id_b].contains(&claim2.id));

        // A third claim returns nothing — both are now `processing`.
        assert!(
            inbox
                .claim_next(ALL_EVENT_TYPES)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn stale_claim_writes_are_no_ops() {
        // Processor A claims; sweeper resets the row; processor A
        // tries to complete with its stale token. Must be a no-op:
        // the row stays in retryable_error.
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-5", "push");

        let claimed = inbox
            .claim_next(ALL_EVENT_TYPES)
            .await
            .unwrap()
            .unwrap();
        // Force the claim to look ancient and sweep it.
        inbox.set_claimed_at(id, Utc::now() - chrono::Duration::seconds(60));
        let recovered = inbox
            .sweep_stuck_claims(chrono::Duration::seconds(10))
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        // Stale processor's late complete: must be a no-op.
        inbox
            .complete(id, claimed.claim_token, WebhookOutcome::IgnoredAction)
            .await
            .unwrap();
        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::RetryableError);
        assert!(row.outcome.is_none(), "stale write must not set outcome");
    }

    #[tokio::test]
    async fn complete_clears_last_error_from_prior_retries() {
        // A row that transient-failed once and then succeeded must
        // not leave a stale last_error string visible to ops queries.
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let id = seed(&inbox, "d-6", "push");
        // Sequence: retryable → reset for re-claim → terminal success.
        let classifier = ScriptedClassifier::new(vec![
            ClassifyOutcome::Retryable("transient blip".into()),
            ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction),
        ]);
        let proc = WebhookProcessor::new(inbox.clone(), classifier, fast_config());

        proc.process_one()
            .await
            .unwrap();
        assert_eq!(
            inbox
                .row(id)
                .unwrap()
                .last_error
                .as_deref(),
            Some("transient blip")
        );

        inbox.set_next_attempt_at(id, Utc::now());
        proc.process_one()
            .await
            .unwrap();

        let row = inbox.row(id).unwrap();
        assert_eq!(row.status, WebhookStatus::Ignored);
        assert!(
            row.last_error.is_none(),
            "complete() must clear last_error from prior retry attempts; got {:?}",
            row.last_error
        );
    }

    // ─── BasicClassifier ────────────────────────────────────────────────

    fn issue_comment_payload(action: &str, body: &str, is_pr: bool) -> serde_json::Value {
        let pull_request = if is_pr {
            serde_json::json!({ "url": "https://api.github.test/repos/o/r/pulls/1" })
        } else {
            serde_json::Value::Null
        };
        serde_json::json!({
            "action": action,
            "comment": {
                "id": 1,
                "body": body,
                "user": { "login": "alice" },
                "author_association": "MEMBER",
            },
            "issue": {
                "number": 1,
                "pull_request": pull_request,
            },
            "repository": { "full_name": "o/r" },
            "sender": { "login": "alice" },
            "installation": { "id": 1 },
        })
    }

    fn make_claimed(
        event_type: &str,
        action: Option<&str>,
        payload: Option<serde_json::Value>,
    ) -> ClaimedWebhook {
        ClaimedWebhook {
            id: 1,
            claim_token: uuid::Uuid::new_v4(),
            delivery_id: "d".into(),
            event_type: event_type.into(),
            action: action.map(str::to_string),
            payload_installation_id: None,
            payload,
            payload_size_bytes: 0,
            attempts: 0,
            received_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn basic_issue_comment_non_created_is_ignored_action() {
        let webhook = make_claimed(
            "issue_comment",
            Some("deleted"),
            Some(issue_comment_payload("deleted", "anything", true)),
        );
        let outcome = BasicClassifier
            .classify(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn basic_issue_comment_on_non_pr_is_ignored_action() {
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark", false)),
        );
        let outcome = BasicClassifier
            .classify(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn basic_issue_comment_pr_no_command_is_ignored_no_command() {
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "looks great", true)),
        );
        let outcome = BasicClassifier
            .classify(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredNoCommand)));
    }

    #[tokio::test]
    async fn basic_issue_comment_pr_with_benchmark_is_ignored_action_in_phase1() {
        // Slice 9 will change this to enqueued_job; pinning the Phase 1
        // behavior here keeps the legacy `/benchmark` path intact.
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(issue_comment_payload("created", "/benchmark run", true)),
        );
        let outcome = BasicClassifier
            .classify(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)));
    }

    #[tokio::test]
    async fn basic_issue_comment_null_payload_is_error() {
        let webhook = make_claimed("issue_comment", Some("created"), None);
        let outcome = BasicClassifier
            .classify(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[tokio::test]
    async fn basic_issue_comment_bad_typed_shape_is_error() {
        let webhook = make_claimed(
            "issue_comment",
            Some("created"),
            Some(serde_json::json!({ "action": "created", "not": "an issue_comment" })),
        );
        let outcome = BasicClassifier
            .classify(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[test]
    fn basic_classifier_supported_types_is_issue_comment_only() {
        // Slice 2b contract: only `issue_comment`. Slices 3-7 widen
        // this list as they add real per-event classification. If
        // someone adds an event here without also adding a real branch
        // in classify(), it would terminalize rows that the later
        // slice needs — which is exactly what Codex caught.
        assert_eq!(BasicClassifier.supported_event_types(), &["issue_comment"]);
    }

    #[tokio::test]
    async fn basic_classifier_leaves_future_slice_events_in_received() {
        // Codex high finding: BasicClassifier must NOT terminalize
        // events that slices 3-7 will need to consume. Seed a mix of
        // issue_comment + installation rows; run the processor; assert
        // the installation row stays in `received` while the
        // issue_comment row becomes terminal.
        let inbox = Arc::new(InMemoryWebhookInbox::new());
        let issue_comment_id = inbox.seed(SeedWebhook {
            delivery_id: "d-ic".into(),
            event_type: "issue_comment".into(),
            action: Some("created".into()),
            payload: Some(issue_comment_payload("created", "looks good", true)),
            payload_size_bytes: 0,
            ..Default::default()
        });
        let installation_id = inbox.seed(SeedWebhook {
            delivery_id: "d-inst".into(),
            event_type: "installation".into(),
            action: Some("created".into()),
            payload_size_bytes: 0,
            ..Default::default()
        });
        let proc = WebhookProcessor::new(inbox.clone(), Arc::new(BasicClassifier), fast_config());

        // First process_one claims the issue_comment row.
        assert!(
            proc.process_one()
                .await
                .unwrap()
        );
        // Second process_one finds nothing claimable (the installation
        // row's event_type isn't in BasicClassifier's supported list).
        assert!(
            !proc
                .process_one()
                .await
                .unwrap()
        );

        // issue_comment row went terminal; installation row UNCHANGED.
        let ic = inbox
            .row(issue_comment_id)
            .unwrap();
        assert!(matches!(ic.status, WebhookStatus::Ignored));

        let inst = inbox
            .row(installation_id)
            .unwrap();
        assert_eq!(
            inst.status,
            WebhookStatus::Received,
            "future-slice event types must NOT be terminalized by BasicClassifier — slice 3 needs \
             to consume them"
        );
        assert!(inst.claim_token.is_none(), "installation row must not be claimed");
        assert!(inst.outcome.is_none(), "installation row must not have a terminal outcome");
    }

    #[tokio::test]
    async fn basic_unsupported_event_is_error() {
        // Handler's allowlist should prevent this from reaching the
        // inbox, but if a misconfiguration lets one slip through, the
        // processor records it terminally instead of looping.
        let webhook = make_claimed("star", None, None);
        let outcome = BasicClassifier
            .classify(&webhook)
            .await;
        assert!(matches!(outcome, ClassifyOutcome::Terminal(WebhookOutcome::Error)));
    }

    #[test]
    fn backoff_doubles_until_cap() {
        let base = chrono::Duration::seconds(2);
        let cap = chrono::Duration::seconds(16);
        assert_eq!(backoff_delay(1, base, cap), chrono::Duration::seconds(2));
        assert_eq!(backoff_delay(2, base, cap), chrono::Duration::seconds(4));
        assert_eq!(backoff_delay(3, base, cap), chrono::Duration::seconds(8));
        assert_eq!(backoff_delay(4, base, cap), chrono::Duration::seconds(16));
        // capped after that
        assert_eq!(backoff_delay(5, base, cap), chrono::Duration::seconds(16));
        assert_eq!(backoff_delay(99, base, cap), chrono::Duration::seconds(16));
    }
}
