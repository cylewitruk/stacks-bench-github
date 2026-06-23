//! The Slack live-timeline `plan` card (item `0023`, iteration v8).
//!
//! One [`SlackTimeline`] per Slack ad-hoc job, driven by the reporter at three
//! points: posted when the job starts running ([`SlackTimeline::started`]),
//! streamed forward as it advances **Job → Build → Run → Finalize**
//! ([`SlackTimeline::advance`], from the worker's phase events; `chat.update`
//! remains the fallback), and finalized at terminal
//! ([`SlackTimeline::completed`]/[`failed`](SlackTimeline::failed)/
//! [`cancelled`](SlackTimeline::cancelled)) — the ⏳ → ✅/❌ reaction swap
//! plus, on success, the results table + download button (via
//! [`crate::slack::card`]).
//!
//! Each row's title **tense-progresses** (future → present → past) and carries
//! an italic "what's happening now" detail while it's pending/in-progress that
//! the render layer clears on complete. The posted card's `ts` is persisted
//! (`set_plan_message_ts`) and read back on re-claim, so a daemon restart
//! resumes the **same** card. Every Slack call is non-fatal.

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::bench_summary::RunResult;
use crate::events::ProgressUpdate;
use crate::job_source::{RunnableJob, RunnableJobStore};
use crate::libvirt::format_elapsed;
use crate::slack::card::{self, CardCtx, RepeatContext, RepeatSummary, Results, STAGES, TASK_IDS};
use crate::slack::client::{
    COMPLETED_REACTION, FAILED_REACTION, QUEUED_REACTION, RUNNING_REACTION, SlackClient,
};
use crate::slack::progress::SlackProgressTranscript;
use crate::slack::stream::{
    StreamChunk, StreamFailure, StreamTaskStatus, TaskUpdate, chunks_for_card,
    classify_stream_error, terminal_chunks_for_card,
};

/// Slack marks a long-idle stream as no longer actively streaming after a few
/// minutes, painting pending rows as failed until a terminal update corrects
/// them. The dedicated keepalive task ([`SlackTimeline::spawn_keepalive`])
/// warms the stream every interval with a quiet task-update; 10s holds it
/// through long semantically-quiet phases and the provisioning gaps the VM
/// heartbeat doesn't cover. Also the debounce floor for the (VM-driven)
/// semantic `heartbeat`.
const SLACK_STREAM_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Outcome of one keepalive tick. `Alive` keeps the loop running (including the
/// idle windows between phases / runs); `Dead` stops it (the stream is gone and
/// the card has fallen back to `chat.update`, which doesn't expire).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keepalive {
    Alive,
    Dead,
}

pub struct SlackTimeline {
    client: Arc<dyn SlackClient>,
    jobs: Arc<dyn RunnableJobStore>,
    /// The user's mention message — the thread anchor + the reaction target.
    channel: String,
    request_ts: String,
    // ── Group-constant: set once, shared by every run in the group (v18). ──
    /// The benchmark group — the stable correlation id for the whole card's
    /// lifecycle (logs), since the timeline now spans all of a group's runs.
    group_id: Uuid,
    benchmark_spec_id: Uuid,
    requested_run_count: i32,
    group_artifact_prefix: String,
    bench_args: Vec<String>,
    /// The current run's index — sync-readable so the (lock-free) terminal /
    /// reaction predicates don't need the state lock. Updated by `begin_run`.
    run_index: AtomicI32,
    state: Mutex<State>,
}

struct State {
    // ── Run-specific render metadata, reset per run by `begin_run` (v18). ──
    /// The current run's job — for `set_plan_message_ts` (resume identity).
    job: RunnableJob,
    /// `job.id` as a string, for `ctx()` to borrow.
    job_id: String,
    rev: String,
    commit: String,
    commit_url: String,
    /// This run's Build phase served from the binary cache (item 0025, v9): the
    /// short fingerprint digest, surfaced on the Build row as "Reused cached
    /// build · …". Per-run (each run may hit/miss independently).
    cached_build: Option<String>,
    /// Set when this run's cache hit is being staged onto the source disk.
    cached_build_staging: Option<String>,
    /// The plan card's own message `ts` (`None` until posted; pre-seeded from
    /// the persisted value on re-claim so we resume the existing card).
    /// **Group-shared** — survives `begin_run` across runs.
    plan_ts: Option<String>,
    /// Whether `plan_ts` is still believed to be a Slack streaming message.
    /// Reclaimed/fallback cards may not be streamable; the first
    /// `message_not_in_streaming_state` flips this off and future updates use
    /// `chat.update` directly.
    streaming: bool,
    /// The furthest stage reached — the in-progress row (0..STAGES). Monotonic.
    stage: usize,
    /// Local stage start used for live elapsed details. Reconstructed on
    /// reclaim from "now", so timings stay best-effort without a schema
    /// change.
    stage_started_at: Instant,
    /// Terminal per-stage outputs captured as each row completes.
    stage_outputs: [Option<String>; STAGES],
    /// Last semantic stream update. Heartbeats are debounced against this so a
    /// long benchmark keeps Slack's stream active without spamming the API.
    last_stream_update_at: Instant,
    /// Compact progress transcript for the current run, reset by `begin_run`.
    progress: SlackProgressTranscript,
}

impl SlackTimeline {
    pub fn new(
        client: Arc<dyn SlackClient>,
        jobs: Arc<dyn RunnableJobStore>,
        job: RunnableJob,
        channel: String,
        request_ts: String,
        plan_message_ts: Option<String>,
    ) -> Self {
        Self {
            client,
            jobs,
            channel,
            request_ts,
            group_id: job.benchmark_group_id,
            benchmark_spec_id: job.benchmark_spec_id,
            requested_run_count: job.requested_run_count,
            group_artifact_prefix: job
                .group_artifact_prefix
                .clone(),
            bench_args: job.bench_args.clone(),
            run_index: AtomicI32::new(job.benchmark_run_index),
            state: Mutex::new(State {
                job_id: job.id.to_string(),
                rev: job.git_ref_display.clone(),
                commit: job.commit.clone(),
                commit_url: format!("https://github.com/{}/commit/{}", job.repository, job.commit),
                cached_build: None,
                cached_build_staging: None,
                job,
                streaming: plan_message_ts.is_some(),
                plan_ts: plan_message_ts,
                stage: 0,
                stage_started_at: Instant::now(),
                stage_outputs: std::array::from_fn(|_| None),
                last_stream_update_at: Instant::now(),
                progress: SlackProgressTranscript::default(),
            }),
        }
    }

    /// Re-point the group-scoped timeline at a new run of the same group (v18,
    /// `0047`): refresh **all** run-specific render state from `job` and reset
    /// the stage/cache labels, so run *N+1* inherits nothing from run *N*.
    /// The shared card identity (`plan_ts`/`streaming`/reactions) is
    /// deliberately left intact — it's the same Slack message across the
    /// group.
    pub async fn begin_run(&self, job: &RunnableJob) {
        self.run_index
            .store(job.benchmark_run_index, Ordering::SeqCst);
        let mut st = self.state.lock().await;
        st.job_id = job.id.to_string();
        st.rev = job.git_ref_display.clone();
        st.commit = job.commit.clone();
        st.commit_url = format!("https://github.com/{}/commit/{}", job.repository, job.commit);
        st.cached_build = None;
        st.cached_build_staging = None;
        st.stage = 0;
        st.stage_started_at = Instant::now();
        st.stage_outputs = std::array::from_fn(|_| None);
        st.progress = SlackProgressTranscript::default();
        st.job = job.clone();
    }

    /// Post (or, on re-claim, resume) the card with the job started — Job
    /// complete, Build in progress.
    pub async fn started(&self) {
        let (_stage, blocks, chunks, fallback) = {
            let mut st = self.state.lock().await;
            // The job has started → Build is the active row (Job is complete).
            if st.stage < 1 {
                st.stage = 1;
                st.stage_started_at = Instant::now();
            }
            st.last_stream_update_at = Instant::now();
            let (stage, blocks, mut chunks, fallback) = self.render_running_locked(&st, st.stage);
            chunks.extend(stage_started_event_chunks(
                stage,
                st.cached_build_staging
                    .is_some(),
            ));
            (stage, blocks, chunks, fallback)
        };
        self.upsert_stream_or_blocks(&blocks, &chunks, &fallback)
            .await;
        // Only the first run swaps ⏳ → 🚀; later repeats inherit the
        // already-running group reaction (no flicker).
        if self
            .run_index
            .load(Ordering::SeqCst)
            == 0
        {
            self.swap_reaction(RUNNING_REACTION)
                .await;
        }
    }

    pub async fn heartbeat(&self) {
        let (blocks, chunks, fallback) = {
            let mut st = self.state.lock().await;
            if !st.streaming || st.plan_ts.is_none() || st.stage >= STAGES {
                return;
            }
            if st
                .last_stream_update_at
                .elapsed()
                < SLACK_STREAM_KEEPALIVE_INTERVAL
            {
                return;
            }
            st.last_stream_update_at = Instant::now();
            let (_, blocks, chunks, fallback) = self.render_running_locked(&st, st.stage);
            (blocks, chunks, fallback)
        };
        self.upsert_stream_or_blocks(&blocks, &chunks, &fallback)
            .await;
    }

    /// Best-effort fine-grained progress. Streamed cards receive only newly
    /// reached milestones; block-update fallback renders the compact snapshot.
    pub async fn progress(&self, progress: &ProgressUpdate) {
        let (blocks, chunks, fallback) = {
            let mut st = self.state.lock().await;
            if progress.run_index != st.job.benchmark_run_index || st.stage != 2 {
                return;
            }
            let Some(delta) = st.progress.push(progress) else {
                return;
            };
            st.last_stream_update_at = Instant::now();
            let ctx = self.ctx(&st);
            let mut card = card::running_card(&ctx, st.stage);
            self.apply_timing(&mut card, &st);
            self.apply_progress(&mut card, &st);
            let chunks = vec![task_detail_event(2, &card.rows[2].title, &delta.details)];
            let blocks = card::render(&card);
            (blocks, chunks, card.title)
        };
        self.upsert_stream_or_blocks(&blocks, &chunks, &fallback)
            .await;
    }

    /// Spawn a background task that warms the Slack stream on a fixed cadence
    /// (independent of VM/phase activity), for the whole group's lifetime. The
    /// caller owns the handle and aborts it on a group-terminal reap. Slack
    /// expires a quiet stream's server-side state within minutes, and our phase
    /// events are far sparser than that during provisioning / long quiet phases
    /// / the inter-run gaps of a repeat group — so the card's liveness must be
    /// driven here, not as a side effect of the VM heartbeat.
    ///
    /// The task keeps looping while the stream is alive but idle (pre-stream,
    /// between phases, or between a group's runs where `begin_run` has reset
    /// the stage); it stops only when the stream is permanently gone (fell
    /// back to block updates), since `chat.update` doesn't expire.
    pub fn spawn_keepalive(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let tl = Arc::clone(self);
        tokio::spawn(async move {
            tracing::info!(
                group_id = %tl.group_id,
                interval_secs = SLACK_STREAM_KEEPALIVE_INTERVAL.as_secs(),
                "slack: stream keepalive task started",
            );
            let mut tick = tokio::time::interval(SLACK_STREAM_KEEPALIVE_INTERVAL);
            tick.tick().await; // the first tick is immediate — started() just appended.
            loop {
                tick.tick().await;
                if matches!(tl.touch_stream().await, Keepalive::Dead) {
                    break;
                }
            }
            tracing::info!(group_id = %tl.group_id, "slack: stream keepalive task stopped");
        })
    }

    /// One keepalive tick. Appends the smallest valid chunk — the current
    /// in-progress row, no new content — to reset the stream's TTL when there's
    /// a live row; otherwise a no-op. [`Keepalive::Dead`] means the stream is
    /// permanently gone (block-update mode), so the loop should stop;
    /// everything else returns [`Keepalive::Alive`] and keeps the loop
    /// running across the idle windows between phases and between runs.
    async fn touch_stream(&self) -> Keepalive {
        let (ts, chunk) = {
            let st = self.state.lock().await;
            if !st.streaming {
                // Fell back to `chat.update` — no stream to keep warm.
                tracing::info!(group_id = %self.group_id, "slack: keepalive: not streaming (block-update mode) — stopping");
                return Keepalive::Dead;
            }
            // Idle: pre-stream, or between phases / runs (stage reset by
            // `begin_run`). Keep looping — a later `started`/`advance` re-arms
            // the row to warm.
            if st.plan_ts.is_none() || st.stage == 0 || st.stage >= STAGES {
                tracing::info!(group_id = %self.group_id, stage = st.stage, has_plan_ts = st.plan_ts.is_some(), "slack: keepalive: idle (nothing to warm)");
                return Keepalive::Alive;
            }
            let ts = st
                .plan_ts
                .clone()
                .expect("checked is_none above");
            let ctx = self.ctx(&st);
            let chunk = StreamChunk::TaskUpdate(TaskUpdate::from_row(
                TASK_IDS[st.stage],
                &card::running_card(&ctx, st.stage).rows[st.stage],
            ));
            (ts, chunk)
        };
        match self
            .client
            .append_stream(&self.channel, &ts, std::slice::from_ref(&chunk))
            .await
        {
            Ok(()) => {
                tracing::info!(group_id = %self.group_id, ts = %ts, "slack: keepalive appended");
                Keepalive::Alive
            }
            Err(e) => {
                self.state
                    .lock()
                    .await
                    .streaming = false;
                tracing::info!(group_id = %self.group_id, error = ?e, "slack: keepalive found the stream inactive; switching to block updates");
                Keepalive::Dead
            }
        }
    }

    /// Advance to `stage` — append stream `task_update`s so the prior rows show
    /// complete and `stage` shows in-progress (`chat.update` fallback if the
    /// stream is inactive). Monotonic: a same/earlier stage (e.g. a repeat or
    /// out-of-order phase) is a no-op.
    pub async fn advance(&self, stage: usize) {
        let stage = stage.min(STAGES - 1);
        let (blocks, chunks, fallback) = {
            let mut st = self.state.lock().await;
            if stage <= st.stage {
                return;
            }
            let completed_stage = st.stage;
            self.finish_stage_locked(&mut st);
            st.stage = stage;
            st.stage_started_at = Instant::now();
            st.last_stream_update_at = Instant::now();
            let (_, blocks, mut chunks, fallback) = self.render_running_locked(&st, stage);
            chunks.extend(stage_completed_event_chunks(
                completed_stage,
                &st.stage_outputs,
                st.cached_build.as_deref(),
            ));
            chunks.extend(stage_started_event_chunks(
                stage,
                st.cached_build_staging
                    .is_some(),
            ));
            (blocks, chunks, fallback)
        };
        self.upsert_stream_or_blocks(&blocks, &chunks, &fallback)
            .await;
    }

    /// The Build phase is being served from a cached binary and the daemon is
    /// staging that binary onto the source disk. Show this as cached staging,
    /// not as a build VM.
    pub async fn mark_build_cache_staging(&self, digest: &str) {
        self.state
            .lock()
            .await
            .cached_build_staging = Some(digest.to_string());
        self.advance(1).await;
    }

    /// The Build phase was served from the binary cache (item 0025, v9): record
    /// the short fingerprint `digest` (surfaced on the Build row as "Reused
    /// cached build · …") and advance to Run. The digest is set once.
    pub async fn mark_build_cached(&self, digest: &str) {
        self.state
            .lock()
            .await
            .cached_build = Some(digest.to_string());
        self.advance(2).await;
    }

    pub fn is_repeat_group(&self) -> bool {
        self.requested_run_count > 1
    }

    pub fn is_final_repeat(&self) -> bool {
        self.run_index
            .load(Ordering::SeqCst)
            + 1
            >= self.requested_run_count
    }

    pub fn benchmark_spec_id(&self) -> Uuid {
        self.benchmark_spec_id
    }

    pub fn requested_run_count(&self) -> i32 {
        self.requested_run_count
    }

    pub fn group_artifact_prefix(&self) -> &str {
        &self.group_artifact_prefix
    }

    /// One repeat finished successfully, but the group still has more runs.
    /// Keep the shared plan alive and leave the request reaction as 🚀 (the
    /// group is still running); the final repeat owns `chat.stopStream`, result
    /// blocks, and the ✅ swap.
    pub async fn repeat_completed(&self) {
        let (blocks, chunks, fallback) = {
            let mut st = self.state.lock().await;
            self.finish_stage_locked(&mut st);
            st.stage = STAGES;
            st.last_stream_update_at = Instant::now();
            let ctx = self.ctx(&st);
            let mut card = card::repeat_finished_card(&ctx);
            self.apply_timing(&mut card, &st);
            let blocks = card::render(&card);
            let chunks = terminal_chunks_for_card(&card);
            (blocks, chunks, card.title)
        };
        self.upsert_stream_or_blocks(&blocks, &chunks, &fallback)
            .await;
    }

    /// Terminal success: all rows complete, the results table + download button
    /// beneath the plan; swap the lifecycle reaction (⏳/🚀) → ✅.
    pub async fn completed(
        &self,
        result: Option<RunResult>,
        db_url: Option<String>,
        repeat_summary: Option<RepeatSummary>,
    ) {
        let results = Results {
            metrics: result.as_ref(),
            repeat_summary: repeat_summary.as_ref(),
            db_url: db_url.as_deref(),
        };
        let (blocks, chunks, result_blocks, fallback) = {
            let mut st = self.state.lock().await;
            self.finish_stage_locked(&mut st);
            st.stage = STAGES;
            st.last_stream_update_at = Instant::now();
            let ctx = self.ctx(&st);
            let mut card = card::completed_card(&ctx, results);
            self.apply_timing(&mut card, &st);
            let blocks = card::render(&card);
            let chunks = terminal_chunks_for_card(&card);
            let result_blocks = serde_json::Value::Array(card::result_blocks(
                card.results
                    .as_ref()
                    .expect("completed card has results"),
            ));
            (blocks, chunks, result_blocks, card.title)
        };
        self.finish_stream_or_blocks(&blocks, &chunks, Some(&fallback), Some(&result_blocks))
            .await;
        self.swap_reaction(COMPLETED_REACTION)
            .await;
    }

    /// Terminal failure: the current row → error (carrying the message),
    /// earlier rows complete, later rows pending; swap the lifecycle reaction
    /// (⏳/🚀) → ❌.
    pub async fn failed(&self, error: &str) {
        self.failed_with_results(error, None, None)
            .await;
    }

    /// Terminal failure with partial repeat results rendered beneath the plan.
    pub async fn failed_with_results(
        &self,
        error: &str,
        repeat_summary: Option<RepeatSummary>,
        db_url: Option<String>,
    ) {
        self.terminate_error(&format!("Failed: {error}"), repeat_summary, db_url)
            .await;
    }

    /// Terminal cancellation: like [`failed`](Self::failed) but a cancel note.
    pub async fn cancelled(&self, reason: &str) {
        self.cancelled_with_results(reason, None, None)
            .await;
    }

    /// Terminal cancellation with partial repeat results rendered beneath the
    /// plan.
    pub async fn cancelled_with_results(
        &self,
        reason: &str,
        repeat_summary: Option<RepeatSummary>,
        db_url: Option<String>,
    ) {
        self.terminate_error(
            &format!("Cancelled: {reason}. Re-run by mentioning me with a new `bench …` request."),
            repeat_summary,
            db_url,
        )
        .await;
    }

    async fn terminate_error(
        &self,
        message: &str,
        repeat_summary: Option<RepeatSummary>,
        db_url: Option<String>,
    ) {
        let results = (repeat_summary.is_some() || db_url.is_some()).then_some(Results {
            metrics: None,
            repeat_summary: repeat_summary.as_ref(),
            db_url: db_url.as_deref(),
        });
        let (blocks, chunks, result_blocks, fallback) = {
            let st = self.state.lock().await;
            let stage = st.stage.min(STAGES - 1);
            let ctx = self.ctx(&st);
            let mut card = card::failed_card(&ctx, stage, message);
            card.results = results;
            self.apply_timing(&mut card, &st);
            let blocks = card::render(&card);
            let chunks = terminal_chunks_for_card(&card);
            let result_blocks = card
                .results
                .as_ref()
                .map(card::result_blocks)
                .map(serde_json::Value::Array);
            (blocks, chunks, result_blocks, card.title)
        };
        self.finish_stream_or_blocks(&blocks, &chunks, Some(&fallback), result_blocks.as_ref())
            .await;
        self.swap_reaction(FAILED_REACTION)
            .await;
    }

    /// The render context for this job — claimed, so the commit is resolved
    /// (the title carries the short SHA and the Build row links the
    /// commit).
    fn ctx<'a>(&'a self, st: &'a State) -> CardCtx<'a> {
        CardCtx {
            rev: &st.rev,
            commit: Some(&st.commit),
            commit_url: Some(&st.commit_url),
            job_id: &st.job_id,
            bench_args: &self.bench_args,
            repeat: (self.requested_run_count > 1).then_some(RepeatContext {
                index: st.job.benchmark_run_index,
                total: self.requested_run_count,
            }),
            cached_build: st.cached_build.as_deref(),
            cached_build_staging: st
                .cached_build_staging
                .is_some(),
        }
    }

    fn render_running_locked(
        &self,
        st: &State,
        stage: usize,
    ) -> (usize, serde_json::Value, Vec<StreamChunk>, String) {
        let ctx = self.ctx(st);
        let mut card = card::running_card(&ctx, stage);
        self.apply_timing(&mut card, st);
        self.apply_progress(&mut card, st);
        let blocks = card::render(&card);
        let chunks = chunks_for_card(&card);
        (stage, blocks, chunks, card.title)
    }

    fn finish_stage_locked(&self, st: &mut State) {
        if (1..STAGES).contains(&st.stage) && st.stage_outputs[st.stage].is_none() {
            st.stage_outputs[st.stage] =
                Some(format!("Completed in {}", format_elapsed(st.stage_started_at.elapsed())));
        }
    }

    fn apply_timing(&self, card: &mut card::Card, st: &State) {
        for (i, row) in card
            .rows
            .iter_mut()
            .enumerate()
        {
            if let Some(output) = &st.stage_outputs[i]
                && row.output.is_none()
            {
                row.output = Some(output.clone());
            }
            if i == st.stage
                && matches!(row.status, crate::bench_summary::PlanTaskStatus::InProgress)
                && let Some(details) = &row.details
            {
                row.details =
                    Some(format!("{details} · {}", format_elapsed(st.stage_started_at.elapsed())));
            }
        }
    }

    fn apply_progress(&self, card: &mut card::Card, st: &State) {
        if st.stage != 2 {
            return;
        }
        if let Some(snapshot) = st.progress.snapshot()
            && let Some(row) = card.rows.get_mut(2)
        {
            row.details = Some(snapshot);
        }
    }

    /// Stream the semantic task updates when the card was started as a stream;
    /// otherwise fall back to the legacy block post/update path. Non-fatal.
    async fn upsert_stream_or_blocks(
        &self,
        blocks: &serde_json::Value,
        chunks: &[crate::slack::stream::StreamChunk],
        fallback: &str,
    ) {
        let (existing, streaming) = {
            let st = self.state.lock().await;
            (st.plan_ts.clone(), st.streaming)
        };
        if let Some(ts) = existing {
            if streaming {
                match self
                    .client
                    .append_stream(&self.channel, &ts, chunks)
                    .await
                {
                    Ok(()) => return,
                    Err(e) => {
                        self.state
                            .lock()
                            .await
                            .streaming = false;
                        match classify_stream_error(&e.to_string()) {
                            StreamFailure::NotStreaming => {
                                tracing::info!(group_id = %self.group_id, error = ?e, "slack: stream inactive; switching to block updates");
                            }
                            StreamFailure::MissingMessage => {
                                tracing::info!(group_id = %self.group_id, error = ?e, "slack: stream message missing; reposting a fresh card");
                            }
                            StreamFailure::Other => {
                                tracing::warn!(group_id = %self.group_id, error = ?e, "slack: stream append failed; switching to block updates");
                            }
                        }
                    }
                }
            }
            self.update_blocks(&ts, blocks, fallback)
                .await;
        } else {
            self.post_blocks(blocks, fallback)
                .await;
        }
    }

    async fn finish_stream_or_blocks(
        &self,
        blocks: &serde_json::Value,
        chunks: &[crate::slack::stream::StreamChunk],
        markdown_text: Option<&str>,
        result_blocks: Option<&serde_json::Value>,
    ) {
        let (existing, streaming) = {
            let st = self.state.lock().await;
            (st.plan_ts.clone(), st.streaming)
        };
        if let Some(ts) = existing {
            if streaming {
                match self
                    .client
                    .stop_stream(&self.channel, &ts, markdown_text, chunks, result_blocks)
                    .await
                {
                    Ok(()) => return,
                    Err(e) => match classify_stream_error(&e.to_string()) {
                        StreamFailure::NotStreaming => {
                            self.state
                                .lock()
                                .await
                                .streaming = false;
                            tracing::info!(group_id = %self.group_id, error = ?e, "slack: stream inactive at terminal; falling back to block update");
                        }
                        StreamFailure::MissingMessage => {
                            self.state
                                .lock()
                                .await
                                .streaming = false;
                            tracing::info!(group_id = %self.group_id, error = ?e, "slack: terminal stream message missing; reposting a fresh card");
                        }
                        StreamFailure::Other => {
                            tracing::warn!(group_id = %self.group_id, error = ?e, "slack: stream stop failed; falling back to block update");
                        }
                    },
                }
            }
            self.update_blocks(&ts, blocks, markdown_text.unwrap_or("Benchmark finished"))
                .await;
        } else {
            self.post_blocks(blocks, markdown_text.unwrap_or("Benchmark finished"))
                .await;
        }
    }

    async fn update_blocks(&self, ts: &str, blocks: &serde_json::Value, fallback: &str) {
        if let Err(e) = self
            .client
            .update_blocks(&self.channel, ts, blocks, fallback)
            .await
        {
            // A gone/unowned message can't be updated — repost instead of
            // reporting nowhere.
            if matches!(classify_stream_error(&e.to_string()), StreamFailure::MissingMessage) {
                tracing::info!(group_id = %self.group_id, error = ?e, "slack: card message gone; reposting a fresh card");
                self.repost_card(blocks, fallback)
                    .await;
            } else {
                tracing::warn!(group_id = %self.group_id, error = ?e, "slack: plan update failed (non-fatal)");
            }
        }
    }

    /// The card message is gone/unowned — drop our `ts` and post a fresh card
    /// (persisting its new `ts`) so later updates and the repeat chain follow
    /// the live message, not the dead one.
    async fn repost_card(&self, blocks: &serde_json::Value, fallback: &str) {
        {
            let mut st = self.state.lock().await;
            st.plan_ts = None;
            st.streaming = false;
        }
        self.post_fresh_card(blocks, fallback)
            .await;
    }

    /// Post the block fallback if we don't yet have a stream/card (persisting
    /// its `ts` for resume-on-reclaim). Non-fatal.
    async fn post_blocks(&self, blocks: &serde_json::Value, fallback: &str) {
        let existing = self
            .state
            .lock()
            .await
            .plan_ts
            .clone();
        if let Some(ts) = existing {
            self.update_blocks(&ts, blocks, fallback)
                .await;
            return;
        }
        self.post_fresh_card(blocks, fallback)
            .await;
    }

    /// Post a brand-new plan card and persist its `ts`. Does not route through
    /// `update_blocks`, so the repost path (`update_blocks` → `repost_card` →
    /// here) can't form an async recursion cycle. Non-fatal.
    async fn post_fresh_card(&self, blocks: &serde_json::Value, fallback: &str) {
        match self
            .client
            .post_blocks_in_thread(&self.channel, &self.request_ts, blocks, fallback)
            .await
        {
            Ok(ts) => {
                let job = {
                    let mut st = self.state.lock().await;
                    st.plan_ts = Some(ts.clone());
                    st.streaming = false;
                    st.job.clone()
                };
                // Persist so a reclaimed job resumes this card. A failure
                // is non-fatal: a restart would post a fresh card.
                if let Err(e) = self
                    .jobs
                    .set_plan_message_ts(&job, &ts)
                    .await
                {
                    tracing::warn!(group_id = %self.group_id, error = ?e, "slack: persisting plan ts failed (non-fatal)");
                }
            }
            Err(e) => {
                tracing::warn!(group_id = %self.group_id, error = ?e, "slack: plan post failed (non-fatal)")
            }
        }
    }

    /// Retire whichever non-terminal reaction is present (⏳ or 🚀) and add
    /// `to`, driving the ⏳ → 🚀 → ✅/❌ chain. Each step is non-fatal;
    /// removing an absent reaction is a harmless no-op.
    async fn swap_reaction(&self, to: &str) {
        for from in [QUEUED_REACTION, RUNNING_REACTION] {
            if from != to
                && let Err(e) = self
                    .client
                    .remove_reaction(&self.channel, &self.request_ts, from)
                    .await
            {
                tracing::debug!(group_id = %self.group_id, reaction = from, error = ?e, "slack: removing prior reaction (non-fatal; likely absent)");
            }
        }
        if let Err(e) = self
            .client
            .add_reaction(&self.channel, &self.request_ts, to)
            .await
        {
            tracing::warn!(group_id = %self.group_id, reaction = to, error = ?e, "slack: adding reaction failed (non-fatal)");
        }
    }
}

fn stage_started_event_chunks(stage: usize, cached_build_staging: bool) -> Vec<StreamChunk> {
    match stage {
        1 if cached_build_staging => vec![task_detail_event(
            1,
            "Staging cached binary",
            "Preparing cached stacks-bench binary.",
        )],
        1 => vec![task_detail_event(1, "Building benchmark binaries", "Starting build VM.")],
        2 => vec![task_detail_event(2, "Running benchmark", "Benchmark started.")],
        3 => vec![task_detail_event(3, "Publishing results", "Publishing results.")],
        _ => Vec::new(),
    }
}

fn stage_completed_event_chunks(
    stage: usize,
    outputs: &[Option<String>; STAGES],
    cached_build: Option<&str>,
) -> Vec<StreamChunk> {
    match stage {
        1 => {
            let (title, output) = if let Some(digest) = cached_build {
                (format!("Reused cached build · {digest}"), "Cached binary staged.".to_string())
            } else {
                (
                    "Built benchmark binaries".to_string(),
                    outputs[1]
                        .clone()
                        .unwrap_or_else(|| "Build completed.".to_string()),
                )
            };
            vec![task_output_event(1, &title, output)]
        }
        2 => vec![task_output_event(
            2,
            "Benchmark run completed",
            outputs[2]
                .clone()
                .unwrap_or_else(|| "Benchmark run completed.".to_string()),
        )],
        3 => vec![task_output_event(
            3,
            "Benchmark completed",
            outputs[3]
                .clone()
                .unwrap_or_else(|| "Results published.".to_string()),
        )],
        _ => Vec::new(),
    }
}

fn task_detail_event(stage: usize, title: &str, details: &str) -> StreamChunk {
    StreamChunk::TaskUpdate(TaskUpdate::detail_event(
        TASK_IDS[stage],
        title,
        StreamTaskStatus::InProgress,
        details,
    ))
}

fn task_output_event(stage: usize, title: &str, output: String) -> StreamChunk {
    StreamChunk::TaskUpdate(TaskUpdate::output_event(
        TASK_IDS[stage],
        title,
        StreamTaskStatus::Complete,
        output,
    ))
}

/// Map a worker phase label to its plan stage (Build=1, Run=2, Finalize=3), or
/// `None` for phases that don't advance the live timeline (terminal `done`/
/// `error`, or an unknown label). The Job row (0) is complete once the job
/// starts; Phase 3 drives its queued/preparing states. Drives the reporter's
/// per-phase [`SlackTimeline::advance`] calls.
pub fn stage_for_phase(label: &str) -> Option<usize> {
    match label {
        "starting" | "building" => Some(1),
        "build_done" | "calibrating" | "running" => Some(2),
        "collecting" => Some(3),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use sbgh_core::models::{GitRefKind, ResolvedCommit};
    use uuid::Uuid;

    use super::*;
    use crate::job_source::{BaselineRef, ProgressTarget};

    /// Records the Slack calls a timeline makes; `post` hands back a fixed
    /// `ts`.
    #[derive(Default)]
    struct FakeSlack {
        posts: StdMutex<Vec<String>>, // blocks json of each post_blocks_in_thread
        updates: StdMutex<Vec<String>>, // "{ts}:{blocks}" of each update_blocks
        appends: StdMutex<Vec<String>>, // "{ts}:{chunks}" of each append_stream
        append_attempts: StdMutex<usize>,
        append_failures: StdMutex<Vec<String>>,
        update_failures: StdMutex<Vec<String>>, // errors injected into update_blocks
        stops: StdMutex<Vec<String>>,           // "{ts}:{chunks}:{blocks?}" of each stop_stream
        added: StdMutex<Vec<String>>,
        removed: StdMutex<Vec<String>>,
    }

    impl FakeSlack {
        /// Adds not cancelled by a matching remove — the reactions a user would
        /// still see. The ⏳ → 🚀 → ✅/❌ lifecycle leaves one at any settled
        /// point.
        fn live_reactions(&self) -> Vec<String> {
            let mut removed = self
                .removed
                .lock()
                .unwrap()
                .clone();
            self.added
                .lock()
                .unwrap()
                .iter()
                .filter(|add| {
                    match removed
                        .iter()
                        .position(|r| r == *add)
                    {
                        Some(pos) => {
                            removed.remove(pos);
                            false
                        }
                        None => true,
                    }
                })
                .cloned()
                .collect()
        }
    }

    #[async_trait]
    impl SlackClient for FakeSlack {
        async fn post_ephemeral(&self, _c: &str, _u: &str, _t: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn post_blocks_in_thread(
            &self,
            _c: &str,
            _ts: &str,
            blocks: &serde_json::Value,
            _f: &str,
        ) -> anyhow::Result<String> {
            self.posts
                .lock()
                .unwrap()
                .push(blocks.to_string());
            Ok("PLAN_TS".to_string())
        }
        async fn update_blocks(
            &self,
            _c: &str,
            ts: &str,
            blocks: &serde_json::Value,
            _f: &str,
        ) -> anyhow::Result<()> {
            if let Some(error) = self
                .update_failures
                .lock()
                .unwrap()
                .pop()
            {
                anyhow::bail!("slack chat.update failed: {error}");
            }
            self.updates
                .lock()
                .unwrap()
                .push(format!("{ts}:{blocks}"));
            Ok(())
        }
        async fn append_stream(
            &self,
            _channel: &str,
            ts: &str,
            chunks: &[crate::slack::stream::StreamChunk],
        ) -> anyhow::Result<()> {
            *self
                .append_attempts
                .lock()
                .unwrap() += 1;
            if let Some(error) = self
                .append_failures
                .lock()
                .unwrap()
                .pop()
            {
                anyhow::bail!("slack chat.appendStream failed: {error}");
            }
            self.appends
                .lock()
                .unwrap()
                .push(format!("{ts}:{}", serde_json::to_string(chunks).unwrap()));
            Ok(())
        }
        async fn stop_stream(
            &self,
            _channel: &str,
            ts: &str,
            _markdown_text: Option<&str>,
            chunks: &[crate::slack::stream::StreamChunk],
            blocks: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            self.stops
                .lock()
                .unwrap()
                .push(format!(
                    "{ts}:{}:{}",
                    serde_json::to_string(chunks).unwrap(),
                    blocks
                        .map(serde_json::Value::to_string)
                        .unwrap_or_default()
                ));
            Ok(())
        }
        async fn add_reaction(&self, _c: &str, _ts: &str, r: &str) -> anyhow::Result<()> {
            self.added
                .lock()
                .unwrap()
                .push(r.into());
            Ok(())
        }
        async fn remove_reaction(&self, _c: &str, _ts: &str, r: &str) -> anyhow::Result<()> {
            self.removed
                .lock()
                .unwrap()
                .push(r.into());
            Ok(())
        }
    }

    /// Records `set_plan_message_ts`; every other method is an unused stub.
    #[derive(Default)]
    struct RecordingStore {
        persisted: StdMutex<Vec<String>>,
    }

    #[async_trait]
    impl RunnableJobStore for RecordingStore {
        async fn set_plan_message_ts(
            &self,
            _job: &RunnableJob,
            message_ts: &str,
        ) -> anyhow::Result<()> {
            self.persisted
                .lock()
                .unwrap()
                .push(message_ts.into());
            Ok(())
        }
        // ── unused in timeline tests ──
        async fn claim_next(&self) -> anyhow::Result<Option<RunnableJob>> {
            Ok(None)
        }
        async fn load_runnable(&self, _id: Uuid) -> anyhow::Result<Option<RunnableJob>> {
            Ok(None)
        }
        async fn list_queued(&self) -> anyhow::Result<Vec<RunnableJob>> {
            Ok(vec![])
        }
        async fn start_running(
            &self,
            _job: &RunnableJob,
            _c: Option<ResolvedCommit>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn sweep_stuck_claims(&self, _l: chrono::Duration) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn complete(&self, _job: &RunnableJob, _s: &serde_json::Value) -> anyhow::Result<()> {
            Ok(())
        }
        async fn find_baseline(
            &self,
            _m: &str,
            _b: &str,
            _at: Option<chrono::DateTime<chrono::Utc>>,
            _w: &str,
        ) -> anyhow::Result<Option<BaselineRef>> {
            Ok(None)
        }
        async fn fail(
            &self,
            _job: &RunnableJob,
            _e: &str,
            _s: Option<&serde_json::Value>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn cancel(&self, _job: &RunnableJob, _r: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_comment_id(&self, _job: &RunnableJob, _id: i64) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_check_run(
            &self,
            _job: &RunnableJob,
            _id: i64,
            _u: Option<&str>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn running_job_ids(&self) -> anyhow::Result<Vec<Uuid>> {
            Ok(vec![])
        }
        async fn cancel_orphan(&self, _id: Uuid, _r: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    fn job() -> RunnableJob {
        RunnableJob {
            id: Uuid::nil(),
            benchmark_group_id: Uuid::new_v4(),
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            requested_run_count: 1,
            group_requested_run_count: 1,
            group_run_index: 0,
            baseline_calibration_id: None,
            group_artifact_prefix: Uuid::new_v4().to_string(),
            repository: "octo/core".into(),
            commit: "abcdef1234567890".into(),
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            task_kind: sbgh_core::models::TaskKind::Benchmark,
            build_target: sbgh_core::models::BuildTarget::StacksBench,
            workload_key: None,
            bench_args: vec![],
            progress: ProgressTarget::Slack {
                channel: "C1".into(),
                message_ts: "REQ_TS".into(),
                plan_message_ts: None,
            },
            claim_token: Some(Uuid::new_v4()),
        }
    }

    fn timeline(
        slack: Arc<FakeSlack>,
        store: Arc<RecordingStore>,
        resume_ts: Option<String>,
    ) -> SlackTimeline {
        SlackTimeline::new(slack, store, job(), "C1".into(), "REQ_TS".into(), resume_ts)
    }

    fn repeat_timeline(
        slack: Arc<FakeSlack>,
        store: Arc<RecordingStore>,
        resume_ts: Option<String>,
    ) -> SlackTimeline {
        let mut job = job();
        job.benchmark_run_index = 0;
        job.requested_run_count = 2;
        job.group_requested_run_count = 2;
        job.group_run_index = 0;
        SlackTimeline::new(slack, store, job, "C1".into(), "REQ_TS".into(), resume_ts)
    }

    fn progress_update(progress: u64) -> ProgressUpdate {
        ProgressUpdate {
            workflow_step: crate::events::WorkflowStep::Run,
            run_index: 0,
            requested_run_count: 1,
            phase: "replay".into(),
            progress,
            total: Some(100),
            message: Some("Replaying measured entries".into()),
        }
    }

    #[tokio::test]
    async fn started_posts_four_row_plan_and_persists_its_ts() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), None);

        tl.started().await;

        let posts = slack.posts.lock().unwrap();
        assert_eq!(posts.len(), 1);
        assert!(posts[0].contains("\"type\":\"plan\""), "{}", posts[0]);
        assert_eq!(
            posts[0]
                .matches("\"task_id\"")
                .count(),
            4,
            "four rows (Job/Build/Run/Finalize): {}",
            posts[0]
        );
        // Job complete (past tense), Build the active row (present tense, italic).
        assert!(posts[0].contains("Job started"), "{}", posts[0]);
        assert!(posts[0].contains("Building benchmark binaries"), "{}", posts[0]);
        assert!(posts[0].contains("\"status\":\"in_progress\""), "{}", posts[0]);
        assert!(posts[0].contains("\"italic\":true"), "active detail is italic: {}", posts[0]);
        assert!(posts[0].contains("Benchmarking develop @ abcdef12"), "live title: {}", posts[0]);
        assert_eq!(
            *store
                .persisted
                .lock()
                .unwrap(),
            vec!["PLAN_TS".to_string()]
        );
    }

    #[tokio::test]
    async fn advance_then_complete_appends_results_and_swaps_reaction() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("PLAN_TS".into()));

        tl.started().await; // append (Build in_progress)
        tl.advance(2).await; // update (Run in_progress)
        tl.advance(1).await; // monotonic: no-op (earlier stage)
        tl.completed(None, Some("https://s3/stacks-bench.db".into()), None)
            .await; // update (all complete + results)

        assert_eq!(
            slack
                .posts
                .lock()
                .unwrap()
                .len(),
            0,
            "resuming a connector-created stream must not post a fallback card"
        );
        let appends = slack.appends.lock().unwrap();
        assert_eq!(appends.len(), 2, "started() and advance(2) streamed (advance(1) was a no-op)");
        assert!(appends[0].starts_with("PLAN_TS:"), "{appends:?}");
        let stops = slack.stops.lock().unwrap();
        assert_eq!(stops.len(), 1, "completed stops the stream");
        // The completed stop carries the final result blocks + the download button.
        assert!(stops[0].contains("\"type\":\"markdown\""), "{}", stops[0]);
        assert!(stops[0].contains("## Benchmark Results"), "{}", stops[0]);
        assert!(stops[0].contains("Download Profiler Data"), "{}", stops[0]);
        assert!(stops[0].contains("\"style\":\"primary\""), "{}", stops[0]);
        assert!(
            !stops[0].contains("\"status\":\"in_progress\""),
            "all rows complete: {}",
            stops[0]
        );
        assert!(stops[0].contains("Benchmark develop @ abcdef12"), "terminal title: {}", stops[0]);
        // ⏳ → 🚀 (run 0 started) → ✅ (completed): only ✅ survives.
        assert!(
            slack
                .added
                .lock()
                .unwrap()
                .contains(&RUNNING_REACTION.to_string()),
            "run 0 going running swaps in 🚀",
        );
        assert_eq!(slack.live_reactions(), vec![COMPLETED_REACTION.to_string()]);
    }

    #[tokio::test]
    async fn progress_appends_only_new_milestones_to_the_run_task() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store, Some("PLAN_TS".into()));

        tl.started().await;
        tl.advance(2).await;
        tl.progress(&progress_update(1))
            .await;
        tl.progress(&progress_update(5))
            .await;
        tl.progress(&progress_update(12))
            .await;

        let appends = slack.appends.lock().unwrap();
        assert_eq!(
            appends.len(),
            4,
            "started + advance + two progress milestones; 5% stays quiet: {appends:?}",
        );
        assert!(appends[2].contains("\"id\":\"run\""), "{}", appends[2]);
        assert!(appends[2].contains("Measuring"), "{}", appends[2]);
        assert!(appends[2].contains("1 / 100 entries (0%)"), "{}", appends[2]);
        assert!(appends[3].contains("12 / 100 entries (10%)"), "{}", appends[3]);
    }

    #[tokio::test]
    async fn progress_block_fallback_renders_compact_snapshot() {
        let slack = Arc::new(FakeSlack::default());
        slack
            .append_failures
            .lock()
            .unwrap()
            .push("message_not_in_streaming_state".into());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store, Some("PLAN_TS".into()));

        tl.started().await; // injected append failure flips to block updates
        tl.advance(2).await;
        tl.progress(&progress_update(1))
            .await;
        tl.progress(&progress_update(12))
            .await;

        let updates = slack.updates.lock().unwrap();
        let latest = updates
            .last()
            .expect("progress updates blocks");
        assert!(latest.contains("Measuring"), "{latest}");
        assert!(latest.contains("1 / 100 entries (0%)"), "{latest}");
        assert!(latest.contains("12 / 100 entries (10%)"), "{latest}");
    }

    #[tokio::test]
    async fn resume_updates_the_existing_card_without_reposting() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        // Re-claim: the persisted ts is pre-seeded → started() resumes via update.
        let tl = timeline(slack.clone(), store.clone(), Some("OLD_TS".into()));

        tl.started().await;

        assert!(
            slack
                .posts
                .lock()
                .unwrap()
                .is_empty(),
            "reclaim must not repost"
        );
        assert!(
            slack
                .updates
                .lock()
                .unwrap()
                .is_empty(),
            "a live stream resumes via append, not chat.update"
        );
        let appends = slack.appends.lock().unwrap();
        assert_eq!(appends.len(), 1);
        assert!(appends[0].starts_with("OLD_TS:"), "resumes the persisted stream: {}", appends[0]);
        assert!(
            appends[0].contains("\"type\":\"task_update\""),
            "semantic task updates: {}",
            appends[0]
        );
        assert!(
            store
                .persisted
                .lock()
                .unwrap()
                .is_empty(),
            "no re-persist on resume"
        );
    }

    #[tokio::test]
    async fn streamed_completion_stops_stream_with_result_blocks() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await;
        tl.completed(None, Some("https://s3/stacks-bench.db".into()), None)
            .await;

        assert!(
            slack
                .updates
                .lock()
                .unwrap()
                .is_empty(),
            "no chat.update while streaming"
        );
        let stops = slack.stops.lock().unwrap();
        assert_eq!(stops.len(), 1);
        assert!(stops[0].starts_with("STREAM_TS:"), "{}", stops[0]);
        assert!(stops[0].contains("\"type\":\"task_update\""), "{}", stops[0]);
        assert!(
            stops[0].contains("\"type\":\"markdown\""),
            "results render as final bottom blocks: {}",
            stops[0]
        );
        assert!(stops[0].contains("Download Profiler Data"), "{}", stops[0]);
        // ⏳ → 🚀 → ✅: only the terminal ✅ is still live.
        assert_eq!(slack.live_reactions(), vec![COMPLETED_REACTION.to_string()]);
    }

    #[tokio::test]
    async fn repeat_completion_keeps_stream_open_for_next_run() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = repeat_timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await;
        tl.repeat_completed().await;

        assert!(
            slack
                .stops
                .lock()
                .unwrap()
                .is_empty(),
            "intermediate repeat must not stop the shared stream",
        );
        // Run 0 starting swapped ⏳ → 🚀; an intermediate repeat adds no
        // terminal, so 🚀 stays the live reaction across the group boundary.
        assert_eq!(
            slack.live_reactions(),
            vec![RUNNING_REACTION.to_string()],
            "mid-group shows running, not a terminal reaction",
        );
        let appends = slack.appends.lock().unwrap();
        assert_eq!(appends.len(), 2, "started + repeat-complete updates");
        assert!(appends[1].contains("repeat 1/2"), "{}", appends[1]);
        assert!(
            !appends[1].contains("\"type\":\"markdown\""),
            "no result blocks before final repeat: {}",
            appends[1],
        );
    }

    #[tokio::test]
    async fn heartbeat_stream_updates_are_debounced_keepalives() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await;
        tl.heartbeat().await;
        {
            let mut st = tl.state.lock().await;
            st.last_stream_update_at =
                Instant::now() - SLACK_STREAM_KEEPALIVE_INTERVAL - Duration::from_secs(1);
        }
        tl.heartbeat().await;

        let appends = slack.appends.lock().unwrap();
        assert_eq!(appends.len(), 2, "started() plus one debounced keepalive: {appends:?}",);
        assert!(
            appends
                .iter()
                .all(|append| !append.contains("\"type\":\"markdown_text\"")),
            "live stream updates must not append text below the plan: {appends:?}",
        );
        assert!(
            appends[1].contains("\"type\":\"task_update\""),
            "keepalive is a quiet semantic task refresh: {}",
            appends[1]
        );
    }

    #[tokio::test]
    async fn touch_stream_warms_an_active_stream_with_a_quiet_task_update() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await; // stage 1, streaming
        let before = slack
            .appends
            .lock()
            .unwrap()
            .len();
        assert_eq!(
            tl.touch_stream().await,
            Keepalive::Alive,
            "keepalive runs while the stream is active",
        );

        let appends = slack.appends.lock().unwrap();
        assert_eq!(appends.len(), before + 1, "one keepalive append");
        let last = appends.last().unwrap();
        assert!(last.starts_with("STREAM_TS:"), "{last}");
        assert!(last.contains("\"type\":\"task_update\""), "quiet task refresh: {last}");
        assert!(!last.contains("\"type\":\"markdown_text\""), "no visible text appended: {last}");
    }

    /// Idle (between phases / runs, e.g. `stage >= STAGES` after a non-final
    /// repeat) keeps the loop alive — it must not exit and abandon the next
    /// run.
    #[tokio::test]
    async fn touch_stream_stays_alive_but_quiet_when_idle() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await;
        // Simulate an intermediate repeat finishing (stage past the last row).
        tl.state.lock().await.stage = STAGES;
        let before = slack
            .appends
            .lock()
            .unwrap()
            .len();

        assert_eq!(tl.touch_stream().await, Keepalive::Alive, "idle, but the loop stays alive");
        assert_eq!(
            slack
                .appends
                .lock()
                .unwrap()
                .len(),
            before,
            "nothing appended while idle",
        );
    }

    #[tokio::test]
    async fn touch_stream_stops_when_the_stream_is_gone() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        // No resume ts → not streaming (block mode): nothing to keep warm.
        let tl = timeline(slack.clone(), store.clone(), None);

        assert_eq!(tl.touch_stream().await, Keepalive::Dead, "block mode → stop the loop");
        assert!(
            slack
                .appends
                .lock()
                .unwrap()
                .is_empty(),
            "nothing appended",
        );
    }

    #[tokio::test]
    async fn touch_stream_stops_after_the_stream_goes_inactive() {
        let slack = Arc::new(FakeSlack::default());
        slack
            .append_failures
            .lock()
            .unwrap()
            .push("message_not_in_streaming_state".into());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await;
        assert_eq!(tl.touch_stream().await, Keepalive::Dead, "a dead stream stops the loop");
        assert!(
            !tl.state
                .lock()
                .await
                .streaming,
            "and flips to block-update mode",
        );
    }

    // ── v18 (0047): group-scoped run handoff + session/registry ──

    /// `begin_run` refreshes all run-specific state from the new job and resets
    /// the stage/cache labels, while the shared card identity survives.
    #[tokio::test]
    async fn begin_run_resets_run_state_but_keeps_the_card() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        // Run 0: advance + stamp a cache label, so there's stale state to clear.
        tl.started().await;
        tl.mark_build_cached("run0digest")
            .await; // sets cached_build, stage → 2

        let mut run1 = job();
        run1.id = Uuid::from_u128(0xB0B);
        run1.benchmark_run_index = 1;
        run1.requested_run_count = 2;
        run1.group_requested_run_count = 2;
        run1.group_run_index = 1;
        run1.commit = "ffffffffffffffff".into();
        tl.begin_run(&run1).await;

        let st = tl.state.lock().await;
        assert_eq!(st.stage, 0, "stage reset for the new run");
        assert!(st.cached_build.is_none(), "run-0 cache label cleared");
        assert!(
            st.cached_build_staging
                .is_none()
        );
        assert_eq!(st.commit, "ffffffffffffffff", "run metadata refreshed");
        assert_eq!(st.job_id, Uuid::from_u128(0xB0B).to_string());
        assert_eq!(st.job.benchmark_run_index, 1);
        // Group-shared card identity is untouched.
        assert_eq!(st.plan_ts.as_deref(), Some("STREAM_TS"), "same card across runs");
        assert!(st.streaming, "stream stays live across the run boundary");
        drop(st);
        assert_eq!(
            tl.run_index
                .load(Ordering::SeqCst),
            1
        );
        assert!(tl.is_final_repeat(), "run 1 of 2 is the final repeat");
    }

    /// The registry returns one session per `(group, target)` across runs and
    /// builds the timeline only once; `reap` aborts the keepalive + removes it.
    #[tokio::test]
    async fn registry_shares_one_session_per_group_and_reaps() {
        use crate::slack::session::{SlackSessionRegistry, SlackTarget};

        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let registry = SlackSessionRegistry::new();
        let group = Uuid::from_u128(7);
        let target = SlackTarget {
            channel: "C1".into(),
            thread_ts: "REQ_TS".into(),
        };
        let built = std::cell::Cell::new(0);

        let s1 = registry.get_or_create(group, target.clone(), || {
            built.set(built.get() + 1);
            Arc::new(timeline(slack.clone(), store.clone(), Some("STREAM_TS".into())))
        });
        let s2 = registry.get_or_create(group, target.clone(), || {
            built.set(built.get() + 1);
            Arc::new(timeline(slack.clone(), store.clone(), None))
        });

        assert_eq!(built.get(), 1, "timeline built once for the group");
        assert!(Arc::ptr_eq(&s1, &s2), "same session across runs");
        assert_eq!(registry.len(), 1);

        registry.reap(group, &target);
        assert!(registry.is_empty(), "reaped from the registry");
        registry.reap(group, &target); // idempotent
    }

    /// `ensure_keepalive` is idempotent — N calls leave exactly one running
    /// task.
    #[tokio::test]
    async fn ensure_keepalive_is_idempotent() {
        use crate::slack::session::{SlackSessionRegistry, SlackTarget};

        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let registry = SlackSessionRegistry::new();
        let group = Uuid::from_u128(1);
        let target = SlackTarget {
            channel: "C1".into(),
            thread_ts: "REQ_TS".into(),
        };
        let session = registry.get_or_create(group, target.clone(), || {
            Arc::new(timeline(slack.clone(), store.clone(), Some("STREAM_TS".into())))
        });
        session
            .timeline()
            .started()
            .await;

        assert!(!session.keepalive_running(), "no keepalive before ensure");
        session.ensure_keepalive();
        assert!(session.keepalive_running(), "spawned");
        session.ensure_keepalive();
        session.ensure_keepalive();
        assert!(session.keepalive_running(), "still exactly one (no replace)");

        registry.reap(group, &target);
        assert!(!session.keepalive_running(), "reap aborts the keepalive");
    }

    /// The abandonment sweep reaps a session only when it's **both** idle past
    /// the grace TTL **and** its group has no active (queued/running) run — the
    /// Phase 3 gating that protects a healthy group's inter-run gap.
    #[tokio::test]
    async fn sweep_reaps_only_idle_and_inactive_sessions() {
        use std::collections::HashSet;
        use std::time::Duration;

        use crate::slack::session::{SlackSessionRegistry, SlackTarget};

        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let registry = SlackSessionRegistry::new();
        let group = Uuid::from_u128(5);
        let target = SlackTarget {
            channel: "C1".into(),
            thread_ts: "REQ_TS".into(),
        };
        let _ = registry.get_or_create(group, target.clone(), || {
            Arc::new(timeline(slack.clone(), store.clone(), Some("STREAM_TS".into())))
        });

        // Recently touched (idle < grace) → never reaped, even with no active runs.
        assert_eq!(registry.sweep_abandoned(Duration::from_secs(3600), &HashSet::new()), 0);
        assert_eq!(registry.len(), 1, "a freshly-touched session is not reaped");

        // Idle past grace BUT the group still has an active run → kept (the
        // inter-run carry-forward gap protection).
        let active: HashSet<Uuid> = [group].into_iter().collect();
        assert_eq!(registry.sweep_abandoned(Duration::ZERO, &active), 0);
        assert_eq!(registry.len(), 1, "an active group is never reaped, even when idle");

        // Idle past grace AND no active run → reaped, keepalive aborted.
        let session = registry
            .get(group, &target)
            .unwrap();
        session
            .timeline()
            .started()
            .await;
        session.ensure_keepalive();
        assert!(session.keepalive_running());
        assert_eq!(registry.sweep_abandoned(Duration::ZERO, &HashSet::new()), 1);
        assert!(registry.is_empty(), "abandoned session reaped");
        assert!(!session.keepalive_running(), "and its keepalive aborted");
    }

    #[tokio::test]
    async fn inactive_stream_switches_to_block_updates_without_retrying_appends() {
        let slack = Arc::new(FakeSlack::default());
        slack
            .append_failures
            .lock()
            .unwrap()
            .push("message_not_in_streaming_state".into());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await;
        tl.heartbeat().await;

        assert_eq!(
            *slack
                .append_attempts
                .lock()
                .unwrap(),
            1,
            "after Slack reports the stream inactive, future updates skip appendStream",
        );
        assert!(
            slack
                .appends
                .lock()
                .unwrap()
                .is_empty(),
            "the only append attempt failed before recording a successful append",
        );
        assert_eq!(
            slack
                .updates
                .lock()
                .unwrap()
                .len(),
            1,
            "only the failed started() update falls back; heartbeat stays quiet",
        );
    }

    #[tokio::test]
    async fn append_error_switches_to_block_updates_without_retrying_appends() {
        let slack = Arc::new(FakeSlack::default());
        slack
            .append_failures
            .lock()
            .unwrap()
            .push("invalid_chunks".into());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await;
        tl.advance(2).await;

        assert_eq!(
            *slack
                .append_attempts
                .lock()
                .unwrap(),
            1,
            "a persistent appendStream error disables streaming for this card",
        );
        assert_eq!(
            slack
                .updates
                .lock()
                .unwrap()
                .len(),
            2,
            "the failed started() and later advance both use block updates",
        );
    }

    /// When the inherited message is gone — append AND the block fallback both
    /// report `message_not_found` — repost a fresh card and persist its new
    /// `ts` instead of reporting onto a dead message.
    #[tokio::test]
    async fn missing_message_reposts_a_fresh_card_and_persists_its_ts() {
        let slack = Arc::new(FakeSlack::default());
        slack
            .append_failures
            .lock()
            .unwrap()
            .push("message_not_found".into());
        slack
            .update_failures
            .lock()
            .unwrap()
            .push("message_not_found".into());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("STREAM_TS".into()));

        tl.started().await;

        assert_eq!(
            slack
                .posts
                .lock()
                .unwrap()
                .len(),
            1,
            "a gone message reposts a fresh card",
        );
        assert!(
            slack
                .updates
                .lock()
                .unwrap()
                .is_empty(),
            "the block update failed (message gone) before recording",
        );
        assert_eq!(
            *store
                .persisted
                .lock()
                .unwrap(),
            vec!["PLAN_TS".to_string()],
            "the reposted card's new ts is persisted for resume + the repeat chain",
        );
    }

    #[tokio::test]
    async fn failed_marks_the_current_row_error_and_swaps_to_x() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("PLAN_TS".into()));

        tl.started().await;
        tl.advance(2).await; // failure during the Run stage
        tl.failed("boom: VM died")
            .await;

        let stops = slack.stops.lock().unwrap();
        let last = stops.last().unwrap();
        assert!(last.contains("\"status\":\"error\""), "an errored row: {last}");
        assert!(last.contains("Failed: boom: VM died"), "carries the reason: {last}");
        // The errored row shows `output` not italic details — that contract is
        // pinned in `card`'s `error_row_shows_output_not_details`; here the
        // still-pending Finalize row legitimately keeps its italic detail.
        assert!(last.contains("Running benchmark"), "errored at the Run row: {last}");
        // ⏳ → 🚀 (run 0 started) → ❌ (failed): only ❌ survives.
        assert_eq!(slack.live_reactions(), vec![FAILED_REACTION.to_string()]);
    }

    /// The first run going live swaps ⏳ → 🚀 on the request.
    #[tokio::test]
    async fn run_zero_started_swaps_queued_for_running() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), Some("PLAN_TS".into()));

        tl.started().await;

        assert_eq!(*slack.removed.lock().unwrap(), vec![QUEUED_REACTION.to_string()]);
        assert_eq!(*slack.added.lock().unwrap(), vec![RUNNING_REACTION.to_string()]);
        assert_eq!(slack.live_reactions(), vec![RUNNING_REACTION.to_string()]);
    }

    /// The reaction is group-scoped — a later run starting must not re-swap
    /// (run 0 already moved the request to 🚀), so there's no churn per repeat.
    #[tokio::test]
    async fn later_run_started_leaves_the_running_reaction_untouched() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let mut job = job();
        job.benchmark_run_index = 1;
        job.requested_run_count = 2;
        job.group_requested_run_count = 2;
        job.group_run_index = 1;
        let tl = SlackTimeline::new(
            slack.clone(),
            store.clone(),
            job,
            "C1".into(),
            "REQ_TS".into(),
            Some("PLAN_TS".into()),
        );

        tl.started().await;

        assert!(
            slack
                .added
                .lock()
                .unwrap()
                .is_empty(),
            "no reaction add when a follow-up run starts",
        );
        assert!(
            slack
                .removed
                .lock()
                .unwrap()
                .is_empty(),
            "no reaction remove when a follow-up run starts",
        );
    }

    #[test]
    fn phase_labels_map_to_stages() {
        assert_eq!(stage_for_phase("starting"), Some(1));
        assert_eq!(stage_for_phase("building"), Some(1));
        assert_eq!(stage_for_phase("build_done"), Some(2));
        assert_eq!(stage_for_phase("calibrating"), Some(2));
        assert_eq!(stage_for_phase("running"), Some(2));
        assert_eq!(stage_for_phase("collecting"), Some(3));
        assert_eq!(stage_for_phase("done"), None);
        assert_eq!(stage_for_phase("error"), None);
        assert_eq!(stage_for_phase("whatever"), None);
    }
}
