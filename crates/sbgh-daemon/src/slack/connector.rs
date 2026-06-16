//! The Slack mention → benchmark orchestration (item `0002`, v5 Phase 1).
//!
//! Flow for one `@BenchBot bench …` mention, in this order:
//!   1. **authz** (team + user allowlist) — *before* anything else;
//!   2. **resolve** the workload (deterministic parser fast-path, then optional
//!      LLM intent resolver);
//!   3. **enqueue** an ad-hoc job (default repo/rev, no webhook) carrying the
//!      Slack reporting provenance;
//!   4. **post** the queued live-timeline card in-thread (persisting its `ts`
//!      by job id, so the claim-time reporter resumes the same card) and
//!      **react** ⏳ on the request.
//!
//! A rejection at step 1 or 2 is an **ephemeral** reply (invoker-only) with
//! **no enqueue and no reaction**, so a denied/garbled request leaves the
//! channel untouched. The code-under-test target (the configured default repo
//! resolved to its FK ids) is held as [`SlackJobTarget`]; resolving
//! `default_repository` → ids is the wiring slice's job, not this layer's.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sbgh_core::config::SlackConfig;
use sbgh_core::db::JobStore;
use sbgh_core::models::{
    BuildTarget, GitRefKind, JobAxes, JobIntent, JobSource, NewJob, QueuedEventDetail, TaskKind,
};
use uuid::Uuid;

use crate::llm::intent::{IntentOutcome, IntentResolver};
use crate::slack::card::{self, CardCtx, RepeatContext};
use crate::slack::client::{QUEUED_REACTION, SlackClient};
use crate::slack::stream::initial_chunks_for_card;
use crate::slack::target::SlackJobTarget;
use crate::workload::{WorkloadSpec, resolve_workload};

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

pub struct SlackConnector {
    cfg: SlackConfig,
    target: SlackJobTarget,
    jobs: Arc<dyn JobStore>,
    client: Arc<dyn SlackClient>,
    intent_resolver: Option<Arc<dyn IntentResolver>>,
    intent_rate_limit_per_minute: u32,
    max_clean_repetitions: u32,
    binary_cache_enabled: bool,
    intent_calls: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl SlackConnector {
    pub fn new(
        cfg: SlackConfig,
        target: SlackJobTarget,
        jobs: Arc<dyn JobStore>,
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
            binary_cache_enabled: false,
            intent_calls: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_max_clean_repetitions(mut self, max_clean_repetitions: u32) -> Self {
        self.max_clean_repetitions = max_clean_repetitions.max(1);
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
    pub async fn handle_mention(&self, event: MentionEvent) {
        // 1. Authz FIRST — an off-allowlist sender is rejected without parsing (or,
        //    later, spending an LLM call) on their input.
        if !self.is_authorized(&event.team_id, &event.user) {
            self.reject(&event, "not authorized to run benchmarks here")
                .await;
            return;
        }

        // 2. Resolve the workload (mention stripped → parser fast-path, then optional
        //    LLM resolver).
        let text = strip_leading_mention(&event.text);
        let spec = match self
            .resolve_workload_for_event(&event, text)
            .await
        {
            Ok(spec) => spec,
            Err(reason) => {
                self.reject(&event, &reason)
                    .await;
                return;
            }
        };
        if spec.clean_repetitions > self.max_clean_repetitions {
            self.reject(
                &event,
                &format!(
                    "too many clean repetitions — requested {}, max is {}",
                    spec.clean_repetitions, self.max_clean_repetitions
                ),
            )
            .await;
            return;
        }
        if spec.clean_repetitions > 1 && !self.binary_cache_enabled {
            self.reject(&event, "clean repetitions require the binary cache to be enabled")
                .await;
            return;
        }
        // 3. Enqueue an ad-hoc job. Default repo (target ids) + rev (`--rev` override,
        //    else the configured default); the parsed workload becomes `bench_args`;
        //    channel/message_ts ride along as reporting provenance.
        let rev = spec
            .rev
            .clone()
            .unwrap_or_else(|| self.cfg.default_rev.clone());
        let bench_args = spec.to_bench_args();
        let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
            channel: event.channel.clone(),
            message_ts: event.message_ts.clone(),
            bench_args: bench_args.clone(),
            clean_repetitions: spec.clean_repetitions,
        })
        .expect("QueuedEventDetail serializes");
        let new_job = NewJob {
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
        };

        let job = match self
            .jobs
            .create_unlinked_job(&new_job, &detail)
            .await
        {
            Ok(job) => job,
            Err(e) => {
                tracing::error!(error = %e, "slack: enqueue failed");
                self.reject(&event, "couldn't enqueue the benchmark — please retry")
                    .await;
                return;
            }
        };

        // 4. Post the queued live-timeline card + persist its ts by job id, so the
        //    claim-time reporter resumes the SAME card (item 0023, Phase 3).
        self.post_queued_card(
            &event,
            &new_job.git_ref_display,
            &bench_args,
            spec.clean_repetitions,
            job.id,
        )
        .await;

        // 5. Exactly one ⏳ reaction on the request — the accepted-job signal.
        if let Err(e) = self
            .client
            .add_reaction(&event.channel, &event.message_ts, QUEUED_REACTION)
            .await
        {
            tracing::warn!(error = %e, "slack: add_reaction failed (job still enqueued)");
        }
    }

    async fn resolve_workload_for_event(
        &self,
        event: &MentionEvent,
        text: &str,
    ) -> Result<WorkloadSpec, String> {
        match resolve_workload(text) {
            Ok(spec) => return Ok(spec),
            Err(e) if self.intent_resolver.is_none() => return Err(e.to_string()),
            Err(_) => {}
        }

        let resolver = self
            .intent_resolver
            .as_ref()
            .expect("checked above");
        if !self.allow_intent_call(&event.user) {
            return Err("too many benchmark requests using natural language — please try again \
                        shortly"
                .into());
        }
        match resolver.resolve(text).await {
            Ok(IntentOutcome::Resolved(spec)) => Ok(spec),
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
    ) {
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
        let ts = match stream_result {
            Ok(ts) => ts,
            Err(e) => {
                tracing::warn!(error = ?e, "slack: start stream failed; falling back to block card");
                let blocks = card::render(&card);
                match self
                    .client
                    .post_blocks_in_thread(&event.channel, &event.message_ts, &blocks, &fallback)
                    .await
                {
                    Ok(ts) => ts,
                    Err(e) => {
                        tracing::warn!(error = %e, "slack: posting queued plan card failed (non-fatal)");
                        return;
                    }
                }
            }
        };
        if let Err(e) = self
            .jobs
            .record_plan_message_ts(job_id, &ts)
            .await
        {
            tracing::warn!(error = %e, "slack: persisting queued plan ts failed (non-fatal)");
        }
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use sbgh_core::db::InMemoryJobStore;
    use sbgh_core::models::JobSource;

    use super::*;

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
        reactions: Mutex<Vec<(String, String, String)>>,  // (channel, ts, reaction)
        removed: Mutex<Vec<(String, String, String)>>,    // (channel, ts, reaction)
    }

    struct FakeIntentResolver {
        calls: AtomicUsize,
        outcome: Result<IntentOutcome, String>,
    }

    impl FakeIntentResolver {
        fn resolved(spec: WorkloadSpec) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcome: Ok(IntentOutcome::Resolved(spec)),
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
        ) -> Result<IntentOutcome, crate::llm::intent::IntentProviderError> {
            self.calls
                .fetch_add(1, Ordering::SeqCst);
            self.outcome
                .clone()
                .map_err(crate::llm::intent::IntentProviderError::Message)
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
            self.streams
                .lock()
                .unwrap()
                .push((channel.into(), thread_ts.into(), serde_json::to_string(chunks).unwrap()));
            Ok("stream_ts".into())
        }

        async fn update_blocks(
            &self,
            _channel: &str,
            _ts: &str,
            _blocks: &serde_json::Value,
            _fallback: &str,
        ) -> anyhow::Result<()> {
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

    /// (connector, in-memory store handle, fake slack handle)
    fn harness() -> (SlackConnector, Arc<InMemoryJobStore>, Arc<FakeSlackClient>) {
        let store = Arc::new(InMemoryJobStore::new());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_binary_cache_enabled(true);
        (connector, store, client)
    }

    fn harness_with_intent(
        resolver: Arc<FakeIntentResolver>,
    ) -> (SlackConnector, Arc<InMemoryJobStore>, Arc<FakeSlackClient>, Arc<FakeIntentResolver>)
    {
        harness_with_intent_rate(resolver, 5)
    }

    fn harness_with_intent_rate(
        resolver: Arc<FakeIntentResolver>,
        rate_limit_per_minute: u32,
    ) -> (SlackConnector, Arc<InMemoryJobStore>, Arc<FakeSlackClient>, Arc<FakeIntentResolver>)
    {
        let store = Arc::new(InMemoryJobStore::new());
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
        let jobs = store.all_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].source, JobSource::Slack);
        assert_eq!(jobs[0].github_installation_id, 100);
        assert_eq!(jobs[0].github_repo_id, 10);
        assert_eq!(jobs[0].git_ref_display, "develop", "default rev when no --rev");

        // The queued detail carries the channel/ts + parsed bench_args.
        let queued = store
            .queued_event(jobs[0].id)
            .await
            .unwrap()
            .unwrap();
        let detail: QueuedEventDetail = serde_json::from_value(queued.detail.unwrap().0).unwrap();
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

        // Exactly one ⏳ reaction on the request; no ephemeral.
        let reactions = slack
            .reactions
            .lock()
            .unwrap();
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

        // The card ts was persisted by job id (read back via latest_plan_message_ts).
        let jobs = store.all_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            store
                .latest_plan_message_ts(jobs[0].id)
                .await
                .unwrap()
                .as_deref(),
            Some("stream_ts"),
            "queued stream ts persisted by job_id",
        );
    }

    #[tokio::test]
    async fn rev_override_sets_the_ref() {
        let (c, store, _slack) = harness();
        c.handle_mention(event("<@U07BOT> bench --block 1 --rev feature/x"))
            .await;
        assert_eq!(store.all_jobs()[0].git_ref_display, "feature/x");
    }

    #[tokio::test]
    async fn unauthorized_team_is_rejected_without_enqueue() {
        let (c, store, slack) = harness();
        let mut ev = event("<@U07BOT> bench --block 1");
        ev.team_id = "T_EVIL".into();
        c.handle_mention(ev).await;

        assert!(store.all_jobs().is_empty(), "no job for an off-allowlist workspace");
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

        assert!(store.all_jobs().is_empty());
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

        assert!(store.all_jobs().is_empty(), "no job for an unresolvable request");
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
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
                .contains("only one of"),
            "ephemeral carries the parse reason: {}",
            eph[0].2
        );
    }

    #[tokio::test]
    async fn clean_repetition_cap_rejects_before_enqueue_or_reaction() {
        let store = Arc::new(InMemoryJobStore::new());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_max_clean_repetitions(2);

        connector
            .handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 3"))
            .await;

        assert!(store.all_jobs().is_empty(), "over-cap request must not enqueue");
        assert!(
            client
                .reactions
                .lock()
                .unwrap()
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
        let store = Arc::new(InMemoryJobStore::new());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_max_clean_repetitions(5)
            .with_binary_cache_enabled(false);

        connector
            .handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 2"))
            .await;

        assert!(store.all_jobs().is_empty(), "cache-off multi-run request must not enqueue");
        assert!(
            client
                .reactions
                .lock()
                .unwrap()
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
        let store = Arc::new(InMemoryJobStore::new());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone())
            .with_max_clean_repetitions(5)
            .with_binary_cache_enabled(true);

        connector
            .handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 2"))
            .await;

        let jobs = store.all_jobs();
        assert_eq!(jobs.len(), 1, "multi-run requests enqueue run 0");
        assert_eq!(jobs[0].benchmark_run_index, 0);
        let queued = store
            .queued_event(jobs[0].id)
            .await
            .unwrap()
            .unwrap();
        let detail: QueuedEventDetail = serde_json::from_value(queued.detail.unwrap().0).unwrap();
        let QueuedEventDetail::SlackAdhoc { clean_repetitions, .. } = detail else {
            panic!("expected SlackAdhoc detail");
        };
        assert_eq!(clean_repetitions, 2);
        assert!(
            !client
                .reactions
                .lock()
                .unwrap()
                .is_empty(),
            "accepted request gets a reaction",
        );
    }

    #[tokio::test]
    async fn flag_shaped_request_uses_parser_fast_path_without_llm_call() {
        let resolver = Arc::new(FakeIntentResolver::invalid("should not be used"));
        let (c, store, _slack, resolver) = harness_with_intent(resolver);
        c.handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 1"))
            .await;

        assert_eq!(resolver.calls(), 0, "clean parser input bypasses LLM");
        assert_eq!(store.all_jobs().len(), 1);
    }

    #[tokio::test]
    async fn natural_language_can_resolve_through_llm() {
        let spec = WorkloadSpec {
            target: crate::workload::WorkloadTarget::BlockRange { start: 10, end: 12 },
            clean_repetitions: 1,
            warmup: Some(1),
            rev: Some("feature/nl".into()),
        };
        let resolver = Arc::new(FakeIntentResolver::resolved(spec));
        let (c, store, _slack, resolver) = harness_with_intent(resolver);
        c.handle_mention(event("<@U07BOT> bench blocks 10 to 12 twice on feature/nl"))
            .await;

        assert_eq!(resolver.calls(), 1);
        let jobs = store.all_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].git_ref_display, "feature/nl");
        let queued = store
            .queued_event(jobs[0].id)
            .await
            .unwrap()
            .unwrap();
        let detail: QueuedEventDetail = serde_json::from_value(queued.detail.unwrap().0).unwrap();
        let QueuedEventDetail::SlackAdhoc {
            bench_args, clean_repetitions, ..
        } = detail
        else {
            panic!("expected SlackAdhoc");
        };
        assert_eq!(
            bench_args,
            vec!["--start-at", "10", "--count", "3", "--repetitions", "1", "--warmup", "1"]
        );
        assert_eq!(clean_repetitions, 1);
    }

    #[tokio::test]
    async fn llm_resolved_clean_repetitions_are_capped_before_enqueue() {
        let spec = WorkloadSpec {
            target: crate::workload::WorkloadTarget::Blocks(vec![
                crate::workload::BlockSelector::Height(1),
            ]),
            clean_repetitions: 4,
            warmup: Some(0),
            rev: None,
        };
        let resolver = Arc::new(FakeIntentResolver::resolved(spec));
        let store = Arc::new(InMemoryJobStore::new());
        let slack = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), slack.clone())
            .with_intent_resolver(resolver.clone(), 5)
            .with_max_clean_repetitions(3);

        connector
            .handle_mention(event("<@U07BOT> benchmark block one four times"))
            .await;

        assert_eq!(resolver.calls(), 1);
        assert!(store.all_jobs().is_empty());
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
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
        assert!(store.all_jobs().is_empty());
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
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
        assert!(store.all_jobs().is_empty());
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
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
            target: crate::workload::WorkloadTarget::Blocks(vec![
                crate::workload::BlockSelector::Height(1),
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
        assert_eq!(store.all_jobs().len(), 1);
        assert_eq!(
            slack
                .reactions
                .lock()
                .unwrap()
                .len(),
            1
        );
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
            target: crate::workload::WorkloadTarget::Blocks(vec![
                crate::workload::BlockSelector::Height(1),
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
