//! The Slack mention → benchmark orchestration (item `0002`, v5 Phase 1).
//!
//! Flow for one `@BenchBot bench …` mention, in this order:
//!   1. **authz** (team + user allowlist) — *before* anything else;
//!   2. **resolve** the workload (deterministic parser fast-path, then optional
//!      LLM intent resolver);
//!   3. **post** the queued live-timeline card in-thread, then **create** the
//!      job (default repo/rev, no webhook), recording the card's `ts` in the
//!      same transaction — so the job is never claimable without its
//!      plan-message identity and the reporter resumes the same card on claim;
//!   4. **react** ⏳ on the request.
//!
//! The card is posted before the job exists because its `ts` needs the job id;
//! if creation then fails, the posted card is turned into a visible failure.
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
use sbgh_core::config::SlackConfig;
use sbgh_core::db::{JobStore, NewBenchmarkSpec};
use sbgh_core::models::{
    BuildTarget, GitRefKind, Job, JobAxes, JobIntent, JobSource, NewJob, QueuedEventDetail,
    TaskKind,
};
use sbgh_intent::{IntentOutcome, IntentResolver};
use uuid::Uuid;

use crate::slack::card::{self, CardCtx, RepeatContext};
use crate::slack::client::{ACK_REACTION, QUEUED_REACTION, SlackClient};
use crate::slack::stream::initial_chunks_for_card;
use crate::slack::target::SlackJobTarget;
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

/// A posted queued plan card: its message `ts` and whether it's a live stream
/// (`start_plan_stream`) or a block-message fallback. The cleanup path needs
/// the distinction — a live stream is terminated via `stop_stream`, a block
/// card via `chat.update`.
struct PostedCard {
    ts: String,
    streamed: bool,
}

pub struct SlackConnector {
    cfg: SlackConfig,
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
/// small recorder while the production implementation delegates to the real
/// transactional [`JobStore`] boundary.
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

#[async_trait]
impl<T> BenchmarkQueue for T
where
    T: JobStore,
{
    async fn create_unlinked_benchmark_group(
        &self,
        first_job_id: Uuid,
        specs: &[NewBenchmarkSpec],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> sbgh_core::Result<Job> {
        JobStore::create_unlinked_benchmark_group(
            self,
            first_job_id,
            specs,
            queued_event_detail,
            plan_message_ts,
        )
        .await
    }
}

impl SlackConnector {
    pub fn new(
        cfg: SlackConfig,
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
        let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
            channel: event.channel.clone(),
            message_ts: event.message_ts.clone(),
            bench_args: bench_args.clone(),
            clean_repetitions,
        })
        .expect("QueuedEventDetail serializes");

        // 3a. Post the card before creating the job: its `ts` needs the job id,
        //     and recording it atomically with creation (3b) closes the
        //     claim-before-recorded race that otherwise double-posts. `None` if
        //     Slack rejected the post (the reporter then posts at claim).
        let job_id = Uuid::new_v4();
        let posted = self
            .post_queued_card(&event, &rev_label, &bench_args, clean_repetitions, job_id)
            .await;

        // 3b. Create the job, its queued event, and (when posted) the
        //     plan-message event in one transaction — so the job is never
        //     claimable without its plan `ts`.
        if let Err(e) = self
            .jobs
            .create_unlinked_benchmark_group(
                job_id,
                &specs,
                &detail,
                posted
                    .as_ref()
                    .map(|c| c.ts.as_str()),
            )
            .await
        {
            tracing::error!(error = %e, "slack: enqueue failed");
            // Turn the orphaned queued card into a visible failure.
            if let Some(card) = &posted {
                self.fail_posted_card(&event.channel, card)
                    .await;
            }
            self.reject_after_ack(&event, "couldn't enqueue the benchmark — please retry")
                .await;
            return;
        }
        // The job now exists — pivot the span's correlation field to `job_id` so
        // the runner/reporter lines line up.
        tracing::Span::current().record("job_id", tracing::field::display(job_id));
        tracing::info!(plan_card_posted = posted.is_some(), "slack: ad-hoc benchmark enqueued");

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

    /// Turn an already-posted queued card into a visible failure when the job
    /// insert fails after the post, so the post-before-create window can't
    /// leave a "queued" card for a job that never existed. A live stream can't
    /// be edited via `chat.update`, so it's terminated with `stop_stream`
    /// rendering the failure as terminal blocks; a block card is updated in
    /// place. Best-effort.
    async fn fail_posted_card(&self, channel: &str, card: &PostedCard) {
        let blocks = serde_json::json!([{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":warning: Couldn't enqueue the benchmark — please retry."
            }
        }]);
        let fallback = "Couldn't enqueue the benchmark";
        let result = if card.streamed {
            self.client
                .stop_stream(channel, &card.ts, None, &[], Some(&blocks))
                .await
        } else {
            self.client
                .update_blocks(channel, &card.ts, &blocks, fallback)
                .await
        };
        if let Err(e) = result {
            tracing::warn!(error = %e, "slack: failing the orphaned queued card failed (non-fatal)");
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

    /// Post the queued live-timeline card in-thread + persist its `ts` by
    /// `job_id` (the connector holds no `RunnableJob` pre-claim). Best-effort:
    /// a post or persist failure just means the claim-time reporter posts a
    /// fresh card. Pre-claim, the rev hasn't resolved to a commit yet, so
    /// the card carries the rev (not a SHA) until the reporter takes over.
    async fn post_queued_card(
        &self,
        event: &MentionEvent,
        rev: &str,
        bench_args: &[String],
        clean_repetitions: u32,
        job_id: Uuid,
    ) -> Option<PostedCard> {
        let job_id_str = job_id.to_string();
        let ctx = CardCtx {
            rev,
            commit: None,
            commit_url: None,
            job_id: &job_id_str,
            bench_args,
            // Pre-claim, the group has only run 0; show the requested clean-run
            // total so the queued card reads "N repetitions", not the in-process 1.
            repeat: (clean_repetitions > 1).then_some(RepeatContext {
                index: 0,
                total: clean_repetitions as i32,
            }),
            group_run: None,
            cached_build: None,
            cached_build_staging: false,
        };
        let card = card::queued_card(&ctx, None);
        let fallback = format!("Benchmarking {rev}");
        let stream_result = self
            .client
            .start_plan_stream(
                &event.channel,
                &event.message_ts,
                &event.user,
                &event.team_id,
                &fallback,
                &initial_chunks_for_card(&card),
            )
            .await;
        let (ts, streamed) = match stream_result {
            Ok(ts) => (ts, true),
            Err(e) => {
                tracing::warn!(error = ?e, "slack: start stream failed; falling back to block card");
                let blocks = card::render(&card);
                match self
                    .client
                    .post_blocks_in_thread(&event.channel, &event.message_ts, &blocks, &fallback)
                    .await
                {
                    Ok(ts) => (ts, false),
                    Err(e) => {
                        tracing::warn!(error = %e, "slack: posting queued plan card failed (non-fatal)");
                        return None;
                    }
                }
            }
        };
        tracing::info!(plan_ts = %ts, streamed, "slack: queued live-timeline card posted");
        // The caller records this `ts` atomically with job creation
        // (`create_unlinked_job`); a separate write here would race the claim.
        Some(PostedCard { ts, streamed })
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
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use sbgh_core::models::JobSource;

    use super::*;
    use crate::slack::test_support::RecordingBenchmarkQueue;
    use sbgh_core::workload::WorkloadSpec;

    const TARGET: SlackJobTarget = SlackJobTarget {
        installation_id: 100,
        repo_id: 10,
    };

    /// Records every Slack call so tests can assert exactly what was (and
    /// wasn't) posted.
    #[derive(Default)]
    struct FakeSlackClient {
        ephemerals: Mutex<Vec<(String, String, String)>>, // (channel, user, text)
        streams: Mutex<Vec<(String, String, String)>>,    // (channel, thread_ts, chunks-json)
        posts: Mutex<Vec<(String, String, String)>>,      // (channel, thread_ts, blocks-json)
        updates: Mutex<Vec<(String, String, String)>>,    // (channel, ts, blocks-json)
        stops: Mutex<Vec<(String, String, String)>>,      // (channel, ts, blocks-json)
        reactions: Mutex<Vec<(String, String, String)>>,  // (channel, ts, reaction)
        removed: Mutex<Vec<(String, String, String)>>,    // (channel, ts, reaction)
        fail_stream: AtomicBool,                          // force the block-card fallback
    }

    impl FakeSlackClient {
        /// Force `start_plan_stream` to fail so the queued card falls back to a
        /// block message (the non-streamed cleanup branch).
        fn fail_stream(&self) {
            self.fail_stream
                .store(true, Ordering::SeqCst);
        }

        /// Adds that weren't later removed — the reactions a user would still
        /// see. The 👀 ack is always removed (swapped for ⏳ or retired), so
        /// the net is ⏳ when accepted, nothing when rejected.
        fn net_reactions(&self) -> Vec<(String, String, String)> {
            let mut remaining = self
                .removed
                .lock()
                .unwrap()
                .clone();
            self.reactions
                .lock()
                .unwrap()
                .iter()
                .filter(|add| {
                    match remaining
                        .iter()
                        .position(|r| r == *add)
                    {
                        Some(pos) => {
                            remaining.remove(pos);
                            false
                        }
                        None => true,
                    }
                })
                .cloned()
                .collect()
        }
    }

    struct FakeIntentResolver {
        calls: AtomicUsize,
        outcome: Result<IntentOutcome, String>,
    }

    impl FakeIntentResolver {
        fn resolved(spec: WorkloadSpec) -> Self {
            Self::resolved_request(BenchmarkRequest::Single(spec))
        }

        fn resolved_request(request: BenchmarkRequest) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcome: Ok(IntentOutcome::Resolved(request)),
            }
        }

        fn invalid(reason: &str) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcome: Ok(IntentOutcome::Invalid(reason.into())),
            }
        }

        fn error(reason: &str) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcome: Err(reason.into()),
            }
        }

        fn calls(&self) -> usize {
            self.calls
                .load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl IntentResolver for FakeIntentResolver {
        async fn resolve(
            &self,
            _text: &str,
        ) -> Result<IntentOutcome, sbgh_intent::IntentProviderError> {
            self.calls
                .fetch_add(1, Ordering::SeqCst);
            self.outcome
                .clone()
                .map_err(sbgh_intent::IntentProviderError::Message)
        }
    }

    #[async_trait]
    impl SlackClient for FakeSlackClient {
        async fn post_ephemeral(
            &self,
            channel: &str,
            user: &str,
            text: &str,
        ) -> anyhow::Result<()> {
            self.ephemerals
                .lock()
                .unwrap()
                .push((channel.into(), user.into(), text.into()));
            Ok(())
        }

        async fn post_blocks_in_thread(
            &self,
            channel: &str,
            thread_ts: &str,
            blocks: &serde_json::Value,
            _fallback: &str,
        ) -> anyhow::Result<String> {
            self.posts
                .lock()
                .unwrap()
                .push((channel.into(), thread_ts.into(), blocks.to_string()));
            Ok("ts".into())
        }

        async fn start_plan_stream(
            &self,
            channel: &str,
            thread_ts: &str,
            _recipient_user_id: &str,
            _recipient_team_id: &str,
            _markdown_text: &str,
            chunks: &[crate::slack::stream::StreamChunk],
        ) -> anyhow::Result<String> {
            if self
                .fail_stream
                .load(Ordering::SeqCst)
            {
                anyhow::bail!("start stream failed (injected)");
            }
            self.streams
                .lock()
                .unwrap()
                .push((channel.into(), thread_ts.into(), serde_json::to_string(chunks).unwrap()));
            Ok("stream_ts".into())
        }

        async fn update_blocks(
            &self,
            channel: &str,
            ts: &str,
            blocks: &serde_json::Value,
            _fallback: &str,
        ) -> anyhow::Result<()> {
            self.updates
                .lock()
                .unwrap()
                .push((channel.into(), ts.into(), blocks.to_string()));
            Ok(())
        }

        async fn stop_stream(
            &self,
            channel: &str,
            ts: &str,
            _markdown_text: Option<&str>,
            _chunks: &[crate::slack::stream::StreamChunk],
            blocks: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            self.stops
                .lock()
                .unwrap()
                .push((
                    channel.into(),
                    ts.into(),
                    blocks
                        .map(|b| b.to_string())
                        .unwrap_or_default(),
                ));
            Ok(())
        }

        async fn add_reaction(
            &self,
            channel: &str,
            ts: &str,
            reaction: &str,
        ) -> anyhow::Result<()> {
            self.reactions
                .lock()
                .unwrap()
                .push((channel.into(), ts.into(), reaction.into()));
            Ok(())
        }

        async fn remove_reaction(
            &self,
            channel: &str,
            ts: &str,
            reaction: &str,
        ) -> anyhow::Result<()> {
            self.removed
                .lock()
                .unwrap()
                .push((channel.into(), ts.into(), reaction.into()));
            Ok(())
        }
    }

    fn cfg() -> SlackConfig {
        SlackConfig {
            enabled: true,
            app_token: Some("xapp-x".into()),
            bot_token: Some("xoxb-x".into()),
            default_repository: "octo/core".into(),
            default_rev: "develop".into(),
            allowed_team_ids: vec!["T_OK".into()],
            allowed_user_ids: vec!["U_OK".into()],
        }
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

    fn harness() -> (SlackConnector, Arc<RecordingBenchmarkQueue>, Arc<FakeSlackClient>) {
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_binary_cache_enabled(true);
        (connector, store, client)
    }

    fn harness_with_intent(
        resolver: Arc<FakeIntentResolver>,
    ) -> (SlackConnector, Arc<RecordingBenchmarkQueue>, Arc<FakeSlackClient>, Arc<FakeIntentResolver>)
    {
        harness_with_intent_rate(resolver, 5)
    }

    fn harness_with_intent_rate(
        resolver: Arc<FakeIntentResolver>,
        rate_limit_per_minute: u32,
    ) -> (SlackConnector, Arc<RecordingBenchmarkQueue>, Arc<FakeSlackClient>, Arc<FakeIntentResolver>)
    {
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_binary_cache_enabled(true)
            .with_intent_resolver(resolver.clone(), rate_limit_per_minute);
        (connector, store, client, resolver)
    }

    #[tokio::test]
    async fn accepted_request_enqueues_and_reacts_once() {
        let (c, store, slack) = harness();
        c.handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 1"))
            .await;

        // Exactly one ad-hoc job, with the resolved workload + default rev.
        let jobs = store.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].source, JobSource::Slack);
        assert_eq!(jobs[0].github_installation_id, 100);
        assert_eq!(jobs[0].github_repo_id, 10);
        assert_eq!(jobs[0].git_ref_display, "develop", "default rev when no --rev");

        // The queued detail carries the channel/ts + parsed bench_args.
        let queued = store
            .queued_event_detail(jobs[0].id)
            .unwrap();
        let detail: QueuedEventDetail = serde_json::from_value(queued).unwrap();
        match detail {
            QueuedEventDetail::SlackAdhoc {
                channel,
                message_ts,
                bench_args,
                clean_repetitions,
            } => {
                assert_eq!(channel, "C1");
                assert_eq!(message_ts, "1700000000.000100");
                assert_eq!(bench_args, vec!["--block", "184231", "--repetitions", "1"]);
                assert_eq!(clean_repetitions, 1);
            }
            other => panic!("expected SlackAdhoc detail, got {other:?}"),
        }

        // Net state is one ⏳ (the 👀 ack was swapped out); no ephemeral.
        let reactions = slack.net_reactions();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0], ("C1".into(), "1700000000.000100".into(), QUEUED_REACTION.into()));
        assert!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    /// An authorized mention gets 👀 the instant authz passes (before
    /// resolution), then it's swapped for ⏳ on accept.
    #[tokio::test]
    async fn ack_reaction_precedes_queued_on_accept() {
        let (c, _store, slack) = harness();
        c.handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 1"))
            .await;

        let added: Vec<String> = slack
            .reactions
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, r)| r.clone())
            .collect();
        assert_eq!(added, vec![ACK_REACTION.to_string(), QUEUED_REACTION.to_string()]);
        let removed: Vec<String> = slack
            .removed
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, r)| r.clone())
            .collect();
        assert_eq!(removed, vec![ACK_REACTION.to_string()], "ack retired in favor of ⏳");
    }

    /// A rejection after the ack (resolution failure) retires 👀 and never adds
    /// ⏳ — net state is no reaction.
    #[tokio::test]
    async fn post_ack_rejection_retires_the_ack() {
        let (c, store, slack) = harness();
        // txid + block are mutually exclusive → resolution fails after the ack.
        let txid = "0x".to_string() + &"1".repeat(64);
        c.handle_mention(event(&format!("<@U07BOT> bench --block 1 --txid {txid}")))
            .await;

        assert!(store.jobs().is_empty());
        let added: Vec<String> = slack
            .reactions
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, r)| r.clone())
            .collect();
        assert_eq!(added, vec![ACK_REACTION.to_string()], "only the ack was added");
        assert_eq!(
            slack
                .removed
                .lock()
                .unwrap()
                .len(),
            1,
            "and it was removed"
        );
        assert!(
            slack
                .net_reactions()
                .is_empty(),
            "no lifecycle reaction survives"
        );
    }

    /// An unauthorized mention is rejected before the ack stage, so no reaction
    /// (not even 👀) is ever added.
    #[tokio::test]
    async fn unauthorized_request_never_acks() {
        let (c, _store, slack) = harness();
        let mut ev = event("<@U07BOT> bench --block 1");
        ev.user = "U_EVIL".into();
        c.handle_mention(ev).await;

        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
                .is_empty(),
            "authz fails before the ack is added"
        );
        assert!(
            slack
                .removed
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    /// At enqueue, the connector posts the queued live-timeline card in-thread
    /// and persists its ts **by job id** (the pre-claim path), so the
    /// claim-time reporter resumes the same card.
    #[tokio::test]
    async fn accepted_request_posts_queued_card_and_records_its_ts() {
        let (c, store, slack) = harness();
        c.handle_mention(event("<@U07BOT> bench --block 184231"))
            .await;

        // One stream started under the request ts: the queued plan, Job row "Queued".
        // Scoped so the guard drops before the `await` below (clippy).
        {
            let streams = slack.streams.lock().unwrap();
            assert_eq!(streams.len(), 1);
            assert_eq!(streams[0].0, "C1");
            assert_eq!(streams[0].1, "1700000000.000100");
            assert!(
                streams[0]
                    .2
                    .contains("\"type\":\"task_update\""),
                "{}",
                streams[0].2
            );
            assert!(
                streams[0]
                    .2
                    .contains("Queued"),
                "Job row queued: {}",
                streams[0].2
            );
            assert!(
                slack
                    .posts
                    .lock()
                    .unwrap()
                    .is_empty(),
                "stream start succeeded, so no block fallback"
            );
        }

        // The card ts was passed through to the queue.
        let jobs = store.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            store
                .plan_message_ts(jobs[0].id)
                .as_deref(),
            Some("stream_ts"),
            "queued stream ts passed through with the request",
        );
    }

    /// A `create_unlinked_job` failure *after* a streamed card was posted
    /// terminates the stream with a failure (via `stop_stream`, not
    /// `chat.update`, which a live stream rejects), enqueues nothing, and
    /// rejects — so the window can't leave a fake "queued" stream card.
    #[tokio::test]
    async fn create_failure_after_streamed_card_stops_the_stream() {
        let (c, store, slack) = harness();
        store.fail_create();
        c.handle_mention(event("<@U07BOT> bench --block 184231"))
            .await;

        assert!(store.jobs().is_empty(), "create failed → no job");
        let stops = slack.stops.lock().unwrap();
        assert_eq!(stops.len(), 1, "the streamed card is stopped, not left queued");
        assert_eq!(stops[0].0, "C1");
        assert_eq!(stops[0].1, "stream_ts");
        assert!(
            stops[0]
                .2
                .contains("Couldn't enqueue"),
            "failure blocks rendered: {}",
            stops[0].2
        );
        assert!(
            slack
                .updates
                .lock()
                .unwrap()
                .is_empty(),
            "a live stream is not chat.update'd",
        );
        assert!(
            slack
                .net_reactions()
                .is_empty(),
            "👀 ack retired, no ⏳"
        );
        assert_eq!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .len(),
            1,
            "user told to retry",
        );
    }

    /// The block-fallback variant: when the card was a block message (stream
    /// start failed), the same create failure updates it in place.
    #[tokio::test]
    async fn create_failure_after_block_card_updates_it() {
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let slack = Arc::new(FakeSlackClient::default());
        slack.fail_stream(); // force the block-card fallback
        store.fail_create();
        let c = SlackConnector::new(cfg(), TARGET, store.clone(), slack.clone())
            .with_binary_cache_enabled(true);

        c.handle_mention(event("<@U07BOT> bench --block 184231"))
            .await;

        assert!(store.jobs().is_empty());
        assert_eq!(
            slack
                .posts
                .lock()
                .unwrap()
                .len(),
            1,
            "block fallback posted the card",
        );
        let updates = slack.updates.lock().unwrap();
        assert_eq!(updates.len(), 1, "the block card is updated in place");
        assert_eq!(updates[0].1, "ts");
        assert!(
            updates[0]
                .2
                .contains("Couldn't enqueue"),
            "{}",
            updates[0].2
        );
        assert!(
            slack
                .stops
                .lock()
                .unwrap()
                .is_empty(),
            "a block card is not stopped",
        );
    }

    #[tokio::test]
    async fn rev_override_sets_the_ref() {
        let (c, store, _slack) = harness();
        c.handle_mention(event("<@U07BOT> bench --block 1 --rev feature/x"))
            .await;
        assert_eq!(store.jobs()[0].git_ref_display, "feature/x");
    }

    #[tokio::test]
    async fn unauthorized_team_is_rejected_without_enqueue() {
        let (c, store, slack) = harness();
        let mut ev = event("<@U07BOT> bench --block 1");
        ev.team_id = "T_EVIL".into();
        c.handle_mention(ev).await;

        assert!(store.jobs().is_empty(), "no job for an off-allowlist workspace");
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
                .is_empty(),
            "no reaction"
        );
        assert_eq!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .len(),
            1,
            "one ephemeral rejection"
        );
    }

    #[tokio::test]
    async fn unauthorized_user_is_rejected_without_enqueue() {
        let (c, store, slack) = harness();
        let mut ev = event("<@U07BOT> bench --block 1");
        ev.user = "U_EVIL".into();
        c.handle_mention(ev).await;

        assert!(store.jobs().is_empty());
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn malformed_workload_is_rejected_without_enqueue() {
        let (c, store, slack) = harness();
        // txid + block are mutually exclusive.
        let txid = "0x".to_string() + &"1".repeat(64);
        c.handle_mention(event(&format!("<@U07BOT> bench --block 1 --txid {txid}")))
            .await;

        assert!(store.jobs().is_empty(), "no job for an unresolvable request");
        assert!(
            slack
                .net_reactions()
                .is_empty(),
            "rejected: ack retired, no ⏳"
        );
        let eph = slack
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("only one target mode"),
            "ephemeral carries the parse reason: {}",
            eph[0].2
        );
    }

    #[tokio::test]
    async fn clean_repetition_cap_rejects_before_enqueue_or_reaction() {
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_max_clean_repetitions(2);

        connector
            .handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 3"))
            .await;

        assert!(store.jobs().is_empty(), "over-cap request must not enqueue");
        assert!(
            client
                .net_reactions()
                .is_empty(),
            "over-cap request must not get accepted reaction",
        );
        let eph = client
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("too many clean repetitions"),
            "{}",
            eph[0].2
        );
    }

    #[tokio::test]
    async fn clean_repetitions_above_one_require_binary_cache() {
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_max_clean_repetitions(5)
            .with_binary_cache_enabled(false);

        connector
            .handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 2"))
            .await;

        assert!(store.jobs().is_empty(), "cache-off multi-run request must not enqueue");
        assert!(
            client
                .net_reactions()
                .is_empty(),
            "rejected request must not get accepted reaction",
        );
        let eph = client
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("binary cache"),
            "{}",
            eph[0].2
        );
    }

    #[tokio::test]
    async fn cache_enabled_clean_repetitions_above_one_enqueue_initial_run() {
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_max_clean_repetitions(5)
            .with_binary_cache_enabled(true);

        connector
            .handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 2"))
            .await;

        let jobs = store.jobs();
        assert_eq!(jobs.len(), 1, "multi-run requests enqueue run 0");
        assert_eq!(jobs[0].benchmark_run_index, 0);
        let queued = store
            .queued_event_detail(jobs[0].id)
            .unwrap();
        let detail: QueuedEventDetail = serde_json::from_value(queued).unwrap();
        let QueuedEventDetail::SlackAdhoc { clean_repetitions, .. } = detail else {
            panic!("expected SlackAdhoc detail");
        };
        assert_eq!(clean_repetitions, 2);
        assert_eq!(
            client
                .net_reactions()
                .iter()
                .map(|(_, _, r)| r.as_str())
                .collect::<Vec<_>>(),
            vec![QUEUED_REACTION],
            "accepted request settles on the ⏳ reaction",
        );
    }

    #[tokio::test]
    async fn deterministic_comparison_enqueues_ordered_group_run0() {
        let (c, store, slack) = harness();
        c.handle_mention(event(
            "<@U07BOT> bench --start-at 100 --count 3 --rev baseline --compare-rev candidate \
             --repetitions 1",
        ))
        .await;

        let jobs = store.jobs();
        assert_eq!(jobs.len(), 1, "comparison groups initially enqueue only spec 0/run 0");
        assert_eq!(jobs[0].git_ref_display, "baseline");
        assert_eq!(jobs[0].benchmark_run_index, 0);
        let specs = store.requested_specs_for_group(jobs[0].benchmark_group_id);
        assert_eq!(specs.len(), 2);
        assert_eq!(
            specs[0]
                .new_job
                .git_ref_display,
            "baseline"
        );
        assert_eq!(
            specs[1]
                .new_job
                .git_ref_display,
            "candidate"
        );
        assert_eq!(
            slack
                .net_reactions()
                .iter()
                .map(|(_, _, r)| r.as_str())
                .collect::<Vec<_>>(),
            vec![QUEUED_REACTION],
        );
        assert!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .is_empty(),
        );
    }

    #[tokio::test]
    async fn comparison_variant_cap_rejects_before_phase_1_gate() {
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_binary_cache_enabled(true)
            .with_comparison_limits(1, 10);

        connector
            .handle_mention(event(
                "<@U07BOT> bench --block 1 --rev baseline --compare-rev candidate",
            ))
            .await;

        assert!(store.jobs().is_empty());
        let eph = client
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("too many comparison refs"),
            "{}",
            eph[0].2
        );
    }

    #[tokio::test]
    async fn comparison_lifecycle_cap_rejects_before_phase_1_gate() {
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_binary_cache_enabled(true)
            .with_max_clean_repetitions(5)
            .with_comparison_limits(2, 9);

        connector
            .handle_mention(event(
                "<@U07BOT> bench --block 1 --rev baseline --compare-rev candidate --repetitions 5",
            ))
            .await;

        assert!(store.jobs().is_empty());
        let eph = client
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("requested 10 VM runs, max is 9"),
            "{}",
            eph[0].2
        );
    }

    #[tokio::test]
    async fn flag_shaped_request_uses_parser_fast_path_without_llm_call() {
        let resolver = Arc::new(FakeIntentResolver::invalid("should not be used"));
        let (c, store, _slack, resolver) = harness_with_intent(resolver);
        c.handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 1"))
            .await;

        assert_eq!(resolver.calls(), 0, "clean parser input bypasses LLM");
        assert_eq!(store.jobs().len(), 1);
    }

    #[tokio::test]
    async fn natural_language_can_resolve_through_llm() {
        let spec = WorkloadSpec {
            target: sbgh_core::workload::WorkloadTarget::BlockRange { start: 10, end: 12 },
            clean_repetitions: 1,
            warmup: Some(1),
            rev: Some("feature/nl".into()),
        };
        let resolver = Arc::new(FakeIntentResolver::resolved(spec));
        let (c, store, _slack, resolver) = harness_with_intent(resolver);
        c.handle_mention(event("<@U07BOT> bench blocks 10 to 12 twice on feature/nl"))
            .await;

        assert_eq!(resolver.calls(), 1);
        let jobs = store.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].git_ref_display, "feature/nl");
        let queued = store
            .queued_event_detail(jobs[0].id)
            .unwrap();
        let detail: QueuedEventDetail = serde_json::from_value(queued).unwrap();
        let QueuedEventDetail::SlackAdhoc {
            bench_args, clean_repetitions, ..
        } = detail
        else {
            panic!("expected SlackAdhoc");
        };
        assert_eq!(bench_args, vec!["--start-at", "10", "--count", "3", "--warmup", "1"]);
        assert_eq!(clean_repetitions, 1);
    }

    #[tokio::test]
    async fn natural_language_comparison_uses_same_group_planner() {
        let request = BenchmarkRequest::Comparison(sbgh_core::workload::ComparisonRequest {
            workload: WorkloadSpec {
                target: sbgh_core::workload::WorkloadTarget::Txids(vec![
                    "f426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce".into(),
                ]),
                clean_repetitions: 1,
                warmup: Some(10),
                rev: None,
            },
            variants: vec![
                sbgh_core::workload::ComparisonVariant {
                    rev: "sb-integration/3.4.0.0.2".into(),
                },
                sbgh_core::workload::ComparisonVariant {
                    rev: "sb-integration/3.4.0.0.3".into(),
                },
            ],
        });
        let resolver = Arc::new(FakeIntentResolver::resolved_request(request));
        let (c, store, slack, resolver) = harness_with_intent(resolver);
        c.handle_mention(event(
            "<@U07BOT> benchmark and compare tx \
             f426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce between 3.4.0.0.2 \
             and 3.4.0.0.3 with 10 warmup",
        ))
        .await;

        assert_eq!(resolver.calls(), 1);
        let jobs = store.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].git_ref_display, "sb-integration/3.4.0.0.2");
        let specs = store.requested_specs_for_group(jobs[0].benchmark_group_id);
        assert_eq!(
            specs
                .iter()
                .map(|spec| spec
                    .new_job
                    .git_ref_display
                    .as_str())
                .collect::<Vec<_>>(),
            vec!["sb-integration/3.4.0.0.2", "sb-integration/3.4.0.0.3"],
        );
        let queued = store
            .queued_event_detail(jobs[0].id)
            .unwrap();
        let detail: QueuedEventDetail = serde_json::from_value(queued).unwrap();
        let QueuedEventDetail::SlackAdhoc {
            bench_args, clean_repetitions, ..
        } = detail
        else {
            panic!("expected SlackAdhoc");
        };
        assert_eq!(
            bench_args,
            vec![
                "--txid",
                "f426738843949f576e4eff5ffbb148de9e1a638d20a03c6447cc70490f5156ce",
                "--repetitions",
                "1",
                "--warmup",
                "10",
            ],
        );
        assert_eq!(clean_repetitions, 1);
        assert!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn llm_resolved_clean_repetitions_are_capped_before_enqueue() {
        let spec = WorkloadSpec {
            target: sbgh_core::workload::WorkloadTarget::Blocks(vec![
                sbgh_core::workload::BlockSelector::Height(1),
            ]),
            clean_repetitions: 4,
            warmup: Some(0),
            rev: None,
        };
        let resolver = Arc::new(FakeIntentResolver::resolved(spec));
        let store = Arc::new(RecordingBenchmarkQueue::default());
        let slack = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), slack.clone())
            .with_intent_resolver(resolver.clone(), 5)
            .with_max_clean_repetitions(3);

        connector
            .handle_mention(event("<@U07BOT> benchmark block one four times"))
            .await;

        assert_eq!(resolver.calls(), 1);
        assert!(store.jobs().is_empty());
        assert!(
            slack
                .net_reactions()
                .is_empty()
        );
        assert!(
            slack
                .ephemerals
                .lock()
                .unwrap()[0]
                .2
                .contains("too many clean repetitions")
        );
    }

    #[tokio::test]
    async fn invalid_llm_resolution_rejects_without_enqueue_or_reaction() {
        let resolver = Arc::new(FakeIntentResolver::invalid("I need a block or txid"));
        let (c, store, slack, resolver) = harness_with_intent(resolver);
        c.handle_mention(event("<@U07BOT> please benchmark something"))
            .await;

        assert_eq!(resolver.calls(), 1);
        assert!(store.jobs().is_empty());
        assert!(
            slack
                .net_reactions()
                .is_empty()
        );
        let eph = slack
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert_eq!(eph[0].2, "I need a block or txid");
    }

    #[tokio::test]
    async fn provider_failure_rejects_without_enqueue_or_reaction() {
        let resolver = Arc::new(FakeIntentResolver::error("provider unavailable"));
        let (c, store, slack, resolver) = harness_with_intent(resolver);
        c.handle_mention(event("<@U07BOT> please benchmark block ten"))
            .await;

        assert_eq!(resolver.calls(), 1);
        assert!(store.jobs().is_empty());
        assert!(
            slack
                .net_reactions()
                .is_empty()
        );
        let eph = slack
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("couldn't resolve"),
            "ephemeral should be safe/generic: {}",
            eph[0].2
        );
    }

    #[tokio::test]
    async fn natural_language_rate_limit_rejects_without_second_provider_call() {
        let spec = WorkloadSpec {
            target: sbgh_core::workload::WorkloadTarget::Blocks(vec![
                sbgh_core::workload::BlockSelector::Height(1),
            ]),
            clean_repetitions: 1,
            warmup: Some(0),
            rev: None,
        };
        let resolver = Arc::new(FakeIntentResolver::resolved(spec));
        let (c, store, slack, resolver) = harness_with_intent_rate(resolver, 1);

        c.handle_mention(event("<@U07BOT> please benchmark block one"))
            .await;
        c.handle_mention(event("<@U07BOT> please benchmark block two"))
            .await;

        assert_eq!(resolver.calls(), 1, "second NL request is rejected before provider call");
        assert_eq!(store.jobs().len(), 1);
        // First request accepted (net ⏳), second rate-limited (ack retired) — net one.
        assert_eq!(slack.net_reactions().len(), 1);
        let eph = slack
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("too many benchmark requests"),
            "{}",
            eph[0].2
        );
    }

    #[tokio::test]
    async fn authz_is_checked_before_llm_resolution() {
        let spec = WorkloadSpec {
            target: sbgh_core::workload::WorkloadTarget::Blocks(vec![
                sbgh_core::workload::BlockSelector::Height(1),
            ]),
            clean_repetitions: 1,
            warmup: Some(0),
            rev: None,
        };
        let resolver = Arc::new(FakeIntentResolver::resolved(spec));
        let (c, _store, slack, resolver) = harness_with_intent(resolver);
        let mut ev = event("<@U07BOT> natural language please");
        ev.user = "U_EVIL".into();
        c.handle_mention(ev).await;

        assert_eq!(resolver.calls(), 0, "unauthorized input must not spend an LLM call");
        let eph = slack
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("not authorized")
        );
    }

    /// Ordering guarantee: a malformed command from an UNAUTHORIZED sender is
    /// rejected for **authz**, not parsing — authz runs before resolution.
    #[tokio::test]
    async fn authz_is_checked_before_resolution() {
        let (c, _store, slack) = harness();
        let mut ev = event("<@U07BOT> bench --totally-bogus");
        ev.user = "U_EVIL".into();
        c.handle_mention(ev).await;

        let eph = slack
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("not authorized"),
            "authz rejection must win over the parse error: {}",
            eph[0].2
        );
    }
}
