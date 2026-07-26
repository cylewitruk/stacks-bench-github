//! Slack mention-to-benchmark orchestration.
//!
//! Flow for one `@BenchBot bench …` mention, in this order:
//!   1. **authz** (team + user allowlist) — *before* anything else;
//!   2. **resolve** the workload (deterministic parser fast-path, then optional
//!      LLM intent resolver);
//!   3. **post/reconcile** the queued snapshot in-thread, then **create** the
//!      job (default repo/rev, no webhook), recording the message `ts` in the
//!      same transaction — so the job is never claimable without its
//!      message identity and the reporter resumes the same snapshot on claim;
//!   4. **react** ⏳ on the request.
//!
//! The message is posted before the job exists so its `ts` can be persisted
//! atomically with job creation; if creation then fails, the message is turned
//! into a visible failure.
//!
//! A rejection at step 1 or 2 is an **ephemeral** reply (invoker-only) with
//! **no enqueue and no reaction**, so a denied/garbled request leaves the
//! channel untouched. The code-under-test target (the configured default repo
//! resolved to its FK ids) is held as [`SlackJobTarget`]; resolving
//! `default_repository` → ids is the wiring slice's job, not this layer's.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sbgh_core::db::NewBenchmarkSpec;
use sbgh_core::models::{
    BuildTarget, GitRefKind, Job, JobAxes, JobIntent, JobSource, NewJob, QueuedEventDetail,
    TaskKind,
};
use sbgh_intent::{IntentOutcome, IntentResolver};
use uuid::Uuid;

use crate::{
    ACK_REACTION, PublishUrgency, QUEUED_REACTION, ReportingIdentity, SlackClient,
    SlackMessageTarget, SlackProgressView, SlackSnapshotPublisher,
};
use sbgh_core::workload::{
    BenchmarkRequest, RequestLimits, resolve_benchmark_request, validate_benchmark_request,
};

/// One inbound Slack mention, normalized from the Socket Mode `app_mention`
/// envelope (the receive loop, wiring slice, builds these).
#[derive(Debug, Clone)]
pub struct MentionEvent {
    /// Slack workspace id (`team_id`) — checked against the allowlist.
    pub team_id: String,
    /// Slack user id of the sender — checked against the allowlist.
    pub user: String,
    /// Channel the mention was posted in.
    pub channel: String,
    /// Timestamp of the request message — the thread anchor + the message the
    /// status reaction is added to.
    pub message_ts: String,
    /// Raw message text, including the leading `<@bot>` mention.
    pub text: String,
}

/// Resolved code-under-test for Slack jobs.
#[derive(Debug, Clone, Copy)]
pub struct SlackJobTarget {
    pub installation_id: i64,
    pub repo_id: i64,
}

/// The non-secret policy needed to authorize and resolve Slack mentions.
#[derive(Debug, Clone)]
pub struct SlackConnectorConfig {
    default_rev: String,
    allowed_team_ids: Vec<String>,
    allowed_user_ids: Vec<String>,
}

impl SlackConnectorConfig {
    pub fn new(
        default_rev: impl Into<String>,
        allowed_team_ids: Vec<String>,
        allowed_user_ids: Vec<String>,
    ) -> Self {
        Self {
            default_rev: default_rev.into(),
            allowed_team_ids,
            allowed_user_ids,
        }
    }
}

pub struct SlackConnector {
    cfg: SlackConnectorConfig,
    target: SlackJobTarget,
    jobs: Arc<dyn BenchmarkQueue>,
    client: Arc<dyn SlackClient>,
    intent_resolver: Option<Arc<dyn IntentResolver>>,
    intent_rate_limit_per_minute: u32,
    max_clean_repetitions: u32,
    max_variants: u32,
    max_comparison_lifecycles: u32,
    binary_cache_enabled: bool,
    intent_calls: Mutex<HashMap<String, VecDeque<Instant>>>,
}

/// The only persistence capability the Slack intake path needs.
///
/// Keeping this port local to the consumer lets orchestration tests use a
/// small recorder while daemon composition adapts the real transactional
/// store.
#[async_trait]
pub trait BenchmarkQueue: Send + Sync + 'static {
    async fn create_unlinked_benchmark_group(
        &self,
        first_job_id: Uuid,
        specs: &[NewBenchmarkSpec],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> sbgh_core::Result<Job>;
}

impl SlackConnector {
    pub fn new(
        cfg: SlackConnectorConfig,
        target: SlackJobTarget,
        jobs: Arc<dyn BenchmarkQueue>,
        client: Arc<dyn SlackClient>,
    ) -> Self {
        Self {
            cfg,
            target,
            jobs,
            client,
            intent_resolver: None,
            intent_rate_limit_per_minute: 0,
            max_clean_repetitions: 5,
            max_variants: 2,
            max_comparison_lifecycles: 10,
            binary_cache_enabled: false,
            intent_calls: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_max_clean_repetitions(mut self, max_clean_repetitions: u32) -> Self {
        self.max_clean_repetitions = max_clean_repetitions.max(1);
        self
    }

    pub fn with_comparison_limits(
        mut self,
        max_variants: u32,
        max_comparison_lifecycles: u32,
    ) -> Self {
        self.max_variants = max_variants.max(1);
        self.max_comparison_lifecycles = max_comparison_lifecycles.max(1);
        self
    }

    pub fn with_binary_cache_enabled(mut self, enabled: bool) -> Self {
        self.binary_cache_enabled = enabled;
        self
    }

    pub fn with_intent_resolver(
        mut self,
        resolver: Arc<dyn IntentResolver>,
        rate_limit_per_minute: u32,
    ) -> Self {
        self.intent_resolver = Some(resolver);
        self.intent_rate_limit_per_minute = rate_limit_per_minute.max(1);
        self
    }

    /// Handle one mention end to end. Never returns an error — every failure is
    /// either an ephemeral reply (rejection) or a logged best-effort miss; the
    /// caller (receive loop) has already acked the envelope.
    ///
    /// The span carries the correlation fields (`ts`, then `job_id` once the
    /// job exists) so the whole request — including the nested LLM resolver
    /// — is traceable on one set of fields.
    #[tracing::instrument(
        level = "info",
        name = "slack_mention",
        skip_all,
        fields(
            channel = %event.channel,
            user = %event.user,
            ts = %event.message_ts,
            job_id = tracing::field::Empty,
        )
    )]
    pub async fn handle_mention(&self, event: MentionEvent) {
        tracing::info!("slack: mention received");
        // 1. Authz FIRST — an off-allowlist sender is rejected without parsing (or,
        //    later, spending an LLM call) on their input.
        if !self.is_authorized(&event.team_id, &event.user) {
            tracing::info!("slack: mention rejected — sender/workspace not on the allowlist");
            self.reject(&event, "not authorized to run benchmarks here")
                .await;
            return;
        }

        // 1b. Acknowledge immediately — resolution below may be a slow LLM
        //     round-trip. Removed on any rejection below.
        self.add_ack(&event).await;

        // 2. Resolve the workload (mention stripped → parser fast-path, then optional
        //    LLM resolver).
        let text = strip_leading_mention(&event.text);
        let request = match self
            .resolve_request_for_event(&event, text)
            .await
        {
            Ok(request) => request,
            Err(reason) => {
                self.reject_after_ack(&event, &reason)
                    .await;
                return;
            }
        };
        let request = match validate_benchmark_request(request, self.request_limits()) {
            Ok(request) => request,
            Err(e) => {
                self.reject_after_ack(&event, &e.to_string())
                    .await;
                return;
            }
        };
        if request.clean_repetitions() > 1 && !self.binary_cache_enabled {
            self.reject_after_ack(
                &event,
                "clean repetitions require the binary cache to be enabled",
            )
            .await;
            return;
        }
        // 3. Enqueue an ad-hoc benchmark group. A singleton request creates one spec; a
        //    comparison request creates one ordered spec per variant and queues only
        //    spec 0/run 0. Later specs are materialized by the DB-backed lazy chain.
        let (rev_label, bench_args, clean_repetitions, specs) = match request {
            BenchmarkRequest::Single(spec) => {
                let rev = spec
                    .rev
                    .clone()
                    .unwrap_or_else(|| self.cfg.default_rev.clone());
                let clean_repetitions = spec.clean_repetitions;
                let bench_args = spec.to_bench_args();
                let new_job = self.new_slack_benchmark_job(rev.clone());
                (
                    rev,
                    bench_args,
                    clean_repetitions,
                    vec![NewBenchmarkSpec::singleton(new_job, clean_repetitions as i32)],
                )
            }
            BenchmarkRequest::Comparison(comparison) => {
                let clean_repetitions = comparison
                    .workload
                    .clean_repetitions;
                let bench_args = comparison
                    .workload
                    .to_bench_args();
                let revs: Vec<_> = comparison
                    .variants
                    .iter()
                    .map(|variant| variant.rev.clone())
                    .collect();
                let specs = revs
                    .iter()
                    .cloned()
                    .map(|rev| {
                        NewBenchmarkSpec::singleton(
                            self.new_slack_benchmark_job(rev),
                            clean_repetitions as i32,
                        )
                    })
                    .collect();
                (revs.join(" vs "), bench_args, clean_repetitions, specs)
            }
        };
        tracing::info!(
            rev = %rev_label,
            variants = specs.len(),
            clean_repetitions,
            "slack: workload resolved — enqueuing"
        );
        tracing::debug!(bench_args = ?bench_args, "slack: resolved bench args");
        let reporting_identity =
            ReportingIdentity::for_request(&event.team_id, &event.channel, &event.message_ts);
        let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
            channel: event.channel.clone(),
            message_ts: event.message_ts.clone(),
            reporting_identity: Some(
                reporting_identity
                    .as_str()
                    .to_string(),
            ),
            bench_args: bench_args.clone(),
            clean_repetitions,
        })
        .expect("QueuedEventDetail serializes");

        // 3a. Reconcile or post before creating the job. The identity is stable
        //     across Socket Mode redelivery, so a lost post response can be
        //     adopted rather than duplicated when the reporter takes over.
        let job_id = Uuid::new_v4();
        let queued_snapshot = self
            .post_queued_snapshot(&event, &rev_label, clean_repetitions, reporting_identity)
            .await;
        let posted = queued_snapshot
            .publisher
            .message_ts()
            .await;

        // 3b. Create the job, its queued event, and (when posted) the
        //     plan-message event in one transaction — so the job is never
        //     claimable without its plan `ts`.
        if let Err(e) = self
            .jobs
            .create_unlinked_benchmark_group(job_id, &specs, &detail, posted.as_deref())
            .await
        {
            tracing::error!(error = %e, "slack: enqueue failed");
            // Turn the orphaned queued snapshot into a visible failure.
            self.fail_queued_snapshot(&queued_snapshot)
                .await;
            self.reject_after_ack(&event, "couldn't enqueue the benchmark — please retry")
                .await;
            return;
        }
        // The job now exists — pivot the span's correlation field to `job_id` so
        // the runner/reporter lines line up.
        tracing::Span::current().record("job_id", tracing::field::display(job_id));
        tracing::info!(snapshot_posted = posted.is_some(), "slack: ad-hoc benchmark enqueued");

        // 4. Accepted: swap 👀 → ⏳; the reporter drives ⏳ → 🚀 → ✅/❌ from here.
        self.remove_ack(&event).await;
        if let Err(e) = self
            .client
            .add_reaction(&event.channel, &event.message_ts, QUEUED_REACTION)
            .await
        {
            tracing::warn!(error = %e, "slack: add_reaction failed (job still enqueued)");
        }
    }

    /// Add the 👀 acknowledgment reaction. Best-effort.
    async fn add_ack(&self, event: &MentionEvent) {
        if let Err(e) = self
            .client
            .add_reaction(&event.channel, &event.message_ts, ACK_REACTION)
            .await
        {
            tracing::warn!(error = %e, "slack: add ack reaction failed (non-fatal)");
        } else {
            tracing::debug!(reaction = ACK_REACTION, "slack: acked mention (👀)");
        }
    }

    /// Remove the 👀 ack (replaced by ⏳ on accept, retired on rejection).
    /// Best-effort; absent is fine.
    async fn remove_ack(&self, event: &MentionEvent) {
        if let Err(e) = self
            .client
            .remove_reaction(&event.channel, &event.message_ts, ACK_REACTION)
            .await
        {
            tracing::debug!(error = %e, "slack: removing ack reaction (non-fatal; likely absent)");
        }
    }

    /// Retire the 👀 ack, then post the ephemeral rejection — for failures past
    /// the ack (resolution/cap/gate/enqueue).
    async fn reject_after_ack(&self, event: &MentionEvent, reason: &str) {
        self.remove_ack(event).await;
        self.reject(event, reason)
            .await;
    }

    /// Turn an already-posted queued snapshot into a visible failure when the job
    /// insert fails after the post, so the post-before-create window cannot
    /// leave a queued message for a job that never existed. Best-effort.
    async fn fail_queued_snapshot(&self, queued: &QueuedSnapshot) {
        let mut view = queued.view.clone();
        view.version = view.version.saturating_add(1);
        view.status = crate::SlackStatus::Failed;
        view.phase = None;
        view.details = vec!["Couldn't enqueue the benchmark — please retry.".into()];
        let result = queued
            .publisher
            .publish(view, PublishUrgency::Immediate)
            .await;
        if let Err(e) = result {
            tracing::warn!(error = %e, "slack: failing the orphaned queued snapshot failed (non-fatal)");
        }
    }

    fn request_limits(&self) -> RequestLimits {
        RequestLimits::new(
            self.max_clean_repetitions,
            self.max_variants,
            self.max_comparison_lifecycles,
        )
    }

    fn new_slack_benchmark_job(&self, rev: String) -> NewJob {
        NewJob {
            github_installation_id: self.target.installation_id,
            github_repo_id: self.target.repo_id,
            axes: JobAxes {
                source: JobSource::Slack,
                intent: JobIntent::AdhocBenchmark,
                task_kind: TaskKind::Benchmark,
                build_target: BuildTarget::StacksBench,
            },
            // `Branch` is the neutral default for a default-rev like `develop`;
            // `git_commit_hash` is `None`, so the rev resolves to a commit at
            // claim time — the reporter's `prepare` resolves a Slack job's bare
            // rev (branch/tag/SHA) via `resolve_commit`, so it passes the
            // empty-commit guard like a PR-head or tag job.
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: rev,
            git_commit_hash: None,
            git_committed_at: None,
            workload_key: None,
        }
    }

    async fn resolve_request_for_event(
        &self,
        event: &MentionEvent,
        text: &str,
    ) -> Result<BenchmarkRequest, String> {
        match resolve_benchmark_request(text) {
            Ok(request) => {
                tracing::info!(
                    request_kind = request.kind_label(),
                    "slack: workload resolved via deterministic parser fast-path"
                );
                return Ok(request);
            }
            Err(e) if self.intent_resolver.is_none() => return Err(e.to_string()),
            Err(_) => {}
        }

        let resolver = self
            .intent_resolver
            .as_ref()
            .expect("checked above");
        if !self.allow_intent_call(&event.user) {
            tracing::info!("slack: natural-language request rate-limited before the llm call");
            return Err("too many benchmark requests using natural language — please try again \
                        shortly"
                .into());
        }
        tracing::info!("slack: parser did not match — resolving via the llm intent resolver");
        match resolver.resolve(text).await {
            Ok(IntentOutcome::Resolved(request)) => Ok(request),
            Ok(IntentOutcome::Invalid(invalid)) => Err(invalid.user_message()),
            Err(e) => {
                tracing::warn!(error = %e, "slack: intent resolver failed");
                Err("couldn't resolve that benchmark request — please try again or use explicit \
                     flags"
                    .into())
            }
        }
    }

    fn allow_intent_call(&self, user: &str) -> bool {
        let limit = self
            .intent_rate_limit_per_minute
            .max(1) as usize;
        let now = Instant::now();
        let mut calls = self
            .intent_calls
            .lock()
            .unwrap();
        let user_calls = calls
            .entry(user.to_string())
            .or_default();
        if let Some(cutoff) = now.checked_sub(Duration::from_secs(60)) {
            while user_calls
                .front()
                .is_some_and(|t| *t < cutoff)
            {
                user_calls.pop_front();
            }
        }
        if user_calls.len() >= limit {
            return false;
        }
        user_calls.push_back(now);
        true
    }

    /// Post or reconcile the canonical queued snapshot. Best-effort: a lost
    /// response leaves the timestamp unset, and the claim-time publisher uses
    /// the same metadata identity to adopt the accepted message.
    async fn post_queued_snapshot(
        &self,
        event: &MentionEvent,
        rev: &str,
        clean_repetitions: u32,
        identity: ReportingIdentity,
    ) -> QueuedSnapshot {
        let target = SlackMessageTarget {
            channel: event.channel.clone(),
            thread_ts: event.message_ts.clone(),
        };
        let publisher =
            SlackSnapshotPublisher::new(self.client.clone(), target, identity.clone(), None, None);
        let mut view = SlackProgressView::queued(identity.clone(), "Benchmark", rev);
        if clean_repetitions > 1 {
            view.run = Some(crate::RunPosition {
                current: 1,
                total: clean_repetitions,
            });
        }
        if let Err(error) = publisher
            .publish(view.clone(), PublishUrgency::Immediate)
            .await
        {
            tracing::warn!(error = ?error, "slack: posting/reconciling queued snapshot failed (non-fatal)");
        }
        let message_ts = publisher.message_ts().await;
        tracing::info!(plan_ts = ?message_ts, "slack: queued snapshot ready");
        QueuedSnapshot { publisher, view }
    }

    /// A mention is authorized iff BOTH its workspace AND its sender are
    /// allowlisted (the authenticated socket says nothing about *who* sent it).
    fn is_authorized(&self, team_id: &str, user: &str) -> bool {
        self.cfg
            .allowed_team_ids
            .iter()
            .any(|t| t == team_id)
            && self
                .cfg
                .allowed_user_ids
                .iter()
                .any(|u| u == user)
    }

    async fn reject(&self, event: &MentionEvent, reason: &str) {
        if let Err(e) = self
            .client
            .post_ephemeral(&event.channel, &event.user, reason)
            .await
        {
            tracing::warn!(error = %e, "slack: post_ephemeral (rejection) failed");
        }
    }
}

struct QueuedSnapshot {
    publisher: Arc<SlackSnapshotPublisher>,
    view: SlackProgressView,
}

/// Drop a leading `<@bot>` mention token so the remaining text is the command
/// (the resolver is Slack-agnostic). `<@U…>` alone → empty (→ an empty-request
/// rejection downstream).
fn strip_leading_mention(text: &str) -> &str {
    let t = text.trim_start();
    if let Some(rest) = t.strip_prefix("<@") {
        // Past the closing `>` of the mention, then any following space.
        match rest.split_once('>') {
            Some((_, after)) => after.trim_start(),
            None => "",
        }
    } else {
        t
    }
}

#[cfg(test)]
#[path = "connector_tests.rs"]
mod tests;
