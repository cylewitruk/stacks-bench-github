//! Webhook processor scaffold (slice 2a).
//!
//! Pulls webhook rows from `github_webhook` via [`WebhookInbox`], hands
//! each to a pluggable [`Classifier`], and writes the resulting
//! outcome back. Implements the queue state machine — claim, terminate,
//! retry-with-backoff, permanent-failure-on-attempts-exhausted,
//! stuck-claim sweep — but is intentionally unwired from `main.rs` in
//! slice 2a. Slice 2b plugs in a real [`Classifier`] and starts the
//! loop in production.
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
use sbgh_core::models::WebhookOutcome;

/// What a [`Classifier`] decides to do with a claimed webhook.
#[derive(Debug, Clone)]
pub enum ClassifyOutcome {
    /// Final decision; outcome's terminal status is set immediately.
    Terminal(WebhookOutcome),
    /// Transient failure. The processor records the error, increments
    /// attempts, schedules a backoff retry, or — if attempts have run
    /// out — promotes to a permanent failure.
    Retryable(String),
}

#[async_trait]
pub trait Classifier: Send + Sync + 'static {
    async fn classify(&self, webhook: &ClaimedWebhook) -> ClassifyOutcome;
}

/// Slice 2a default classifier: everything terminates as
/// `ignored_action`. Production-safe because it doesn't drive any
/// downstream effects, and tests can substitute richer behavior.
/// Slice 2b replaces it with real classification logic.
pub struct NoopClassifier;

#[async_trait]
impl Classifier for NoopClassifier {
    async fn classify(&self, _: &ClaimedWebhook) -> ClassifyOutcome {
        ClassifyOutcome::Terminal(WebhookOutcome::IgnoredAction)
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
    /// empty (idle).
    pub async fn process_one(&self) -> Result<bool> {
        let Some(claimed) = self
            .inbox
            .claim_next()
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
    /// stuck-claim sweeps and idle backoff. Errors from `process_one`
    /// are logged and swallowed so a single bad row doesn't crash the
    /// processor.
    pub async fn run(&self) -> Result<()> {
        let mut last_sweep = Instant::now();
        loop {
            if last_sweep.elapsed() >= self.config.sweep_interval {
                match self
                    .inbox
                    .sweep_stuck_claims(self.config.claim_lease)
                    .await
                {
                    Ok(n) if n > 0 => {
                        tracing::warn!(recovered = n, "stuck-claim sweep recovered rows")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = ?e, "stuck-claim sweep failed"),
                }
                last_sweep = Instant::now();
            }

            match self.process_one().await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(self.config.idle_sleep).await,
                Err(e) => {
                    tracing::error!(error = ?e, "webhook processor iteration failed");
                    tokio::time::sleep(self.config.idle_sleep).await;
                }
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
        fn seen(&self) -> Vec<i64> {
            self.seen
                .lock()
                .unwrap()
                .clone()
        }
    }

    #[async_trait]
    impl Classifier for ScriptedClassifier {
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
            .claim_next()
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
            .claim_next()
            .await
            .unwrap()
            .unwrap();
        let claim2 = inbox
            .claim_next()
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
                .claim_next()
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
            .claim_next()
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
