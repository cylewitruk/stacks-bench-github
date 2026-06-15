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
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::bench_summary::RunResult;
use crate::job_source::{RunnableJob, RunnableJobStore};
use crate::libvirt::format_elapsed;
use crate::slack::card::{self, CardCtx, Results, STAGES, TASK_IDS};
use crate::slack::client::{COMPLETED_REACTION, FAILED_REACTION, QUEUED_REACTION, SlackClient};
use crate::slack::stream::{
    StreamChunk, StreamFailure, StreamTaskStatus, TaskUpdate, chunks_for_card,
    classify_stream_error, terminal_chunks_for_card,
};

/// Slack appears to mark a long-idle stream as no longer actively streaming
/// after a few minutes, which paints pending tasks as failed in the client
/// until a later terminal update corrects them. Keep the stream alive with
/// quiet task-update heartbeats; no markdown text is appended.
const SLACK_STREAM_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

pub struct SlackTimeline {
    client: Arc<dyn SlackClient>,
    jobs: Arc<dyn RunnableJobStore>,
    job: RunnableJob,
    /// The user's mention message — the thread anchor + the reaction target.
    channel: String,
    request_ts: String,
    // Pre-extracted render metadata (the job's fields don't change post-claim).
    job_id: String,
    rev: String,
    commit: String,
    commit_url: String,
    /// Set once when this run's Build phase is served from the binary cache
    /// (item 0025, v9): the short fingerprint digest, surfaced on the Build row
    /// as "Reused cached build · …". A `OnceLock` so `ctx()` can borrow it
    /// without the state lock.
    cached_build: std::sync::OnceLock<String>,
    state: Mutex<State>,
}

struct State {
    /// The plan card's own message `ts` (`None` until posted; pre-seeded from
    /// the persisted value on re-claim so we resume the existing card).
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
        let job_id = job.id.to_string();
        let rev = job.git_ref_display.clone();
        let commit = job.commit.clone();
        let commit_url = format!("https://github.com/{}/commit/{}", job.repository, commit);
        Self {
            client,
            jobs,
            job,
            channel,
            request_ts,
            job_id,
            rev,
            commit,
            commit_url,
            cached_build: std::sync::OnceLock::new(),
            state: Mutex::new(State {
                streaming: plan_message_ts.is_some(),
                plan_ts: plan_message_ts,
                stage: 0,
                stage_started_at: Instant::now(),
                stage_outputs: std::array::from_fn(|_| None),
                last_stream_update_at: Instant::now(),
            }),
        }
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
            chunks.extend(stage_started_event_chunks(stage));
            (stage, blocks, chunks, fallback)
        };
        self.upsert_stream_or_blocks(&blocks, &chunks, &fallback)
            .await;
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
                self.cached_build
                    .get()
                    .map(String::as_str),
            ));
            chunks.extend(stage_started_event_chunks(stage));
            (blocks, chunks, fallback)
        };
        self.upsert_stream_or_blocks(&blocks, &chunks, &fallback)
            .await;
    }

    /// The Build phase was served from the binary cache (item 0025, v9): record
    /// the short fingerprint `digest` (surfaced on the Build row as "Reused
    /// cached build · …") and advance to Run. The digest is set once.
    pub async fn mark_build_cached(&self, digest: &str) {
        let _ = self
            .cached_build
            .set(digest.to_string());
        self.advance(2).await;
    }

    /// Terminal success: all rows complete, the results table + download button
    /// beneath the plan; swap ⏳ → ✅.
    pub async fn completed(&self, result: Option<RunResult>, db_url: Option<String>) {
        let results = Results {
            metrics: result.as_ref(),
            db_url: db_url.as_deref(),
        };
        let (blocks, chunks, result_blocks, fallback) = {
            let mut st = self.state.lock().await;
            self.finish_stage_locked(&mut st);
            st.stage = STAGES;
            st.last_stream_update_at = Instant::now();
            let ctx = self.ctx();
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
    /// earlier rows complete, later rows pending; swap ⏳ → ❌.
    pub async fn failed(&self, error: &str) {
        self.terminate_error(&format!("Failed: {error}"))
            .await;
    }

    /// Terminal cancellation: like [`failed`](Self::failed) but a cancel note.
    pub async fn cancelled(&self, reason: &str) {
        self.terminate_error(&format!(
            "Cancelled: {reason}. Re-run by mentioning me with a new `bench …` request."
        ))
        .await;
    }

    async fn terminate_error(&self, message: &str) {
        let (blocks, chunks, fallback) = {
            let st = self.state.lock().await;
            let stage = st.stage.min(STAGES - 1);
            let ctx = self.ctx();
            let mut card = card::failed_card(&ctx, stage, message);
            self.apply_timing(&mut card, &st);
            let blocks = card::render(&card);
            let chunks = terminal_chunks_for_card(&card);
            (blocks, chunks, card.title)
        };
        self.finish_stream_or_blocks(&blocks, &chunks, Some(&fallback), None)
            .await;
        self.swap_reaction(FAILED_REACTION)
            .await;
    }

    /// The render context for this job — claimed, so the commit is resolved
    /// (the title carries the short SHA and the Build row links the
    /// commit).
    fn ctx(&self) -> CardCtx<'_> {
        CardCtx {
            rev: &self.rev,
            commit: Some(&self.commit),
            commit_url: Some(&self.commit_url),
            job_id: &self.job_id,
            bench_args: &self.job.bench_args,
            cached_build: self
                .cached_build
                .get()
                .map(String::as_str),
        }
    }

    fn render_running_locked(
        &self,
        st: &State,
        stage: usize,
    ) -> (usize, serde_json::Value, Vec<StreamChunk>, String) {
        let ctx = self.ctx();
        let mut card = card::running_card(&ctx, stage);
        self.apply_timing(&mut card, st);
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
                        if matches!(
                            classify_stream_error(&e.to_string()),
                            StreamFailure::NotStreaming
                        ) {
                            tracing::info!(job_id = %self.job_id, error = ?e, "slack: stream inactive; switching to block updates");
                        } else {
                            tracing::warn!(job_id = %self.job_id, error = ?e, "slack: stream append failed; switching to block updates");
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
                    Err(e) => {
                        if matches!(
                            classify_stream_error(&e.to_string()),
                            StreamFailure::NotStreaming
                        ) {
                            self.state
                                .lock()
                                .await
                                .streaming = false;
                            tracing::info!(job_id = %self.job_id, error = ?e, "slack: stream inactive at terminal; falling back to block update");
                        } else {
                            tracing::warn!(job_id = %self.job_id, error = ?e, "slack: stream stop failed; falling back to block update");
                        }
                    }
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
            tracing::warn!(job_id = %self.job_id, error = ?e, "slack: plan update failed (non-fatal)");
        }
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
        match self
            .client
            .post_blocks_in_thread(&self.channel, &self.request_ts, blocks, fallback)
            .await
        {
            Ok(ts) => {
                {
                    let mut st = self.state.lock().await;
                    st.plan_ts = Some(ts.clone());
                    st.streaming = false;
                }
                // Persist so a reclaimed job resumes this card. A failure
                // is non-fatal: a restart would post a fresh card.
                if let Err(e) = self
                    .jobs
                    .set_plan_message_ts(&self.job, &ts)
                    .await
                {
                    tracing::warn!(job_id = %self.job_id, error = ?e, "slack: persisting plan ts failed (non-fatal)");
                }
            }
            Err(e) => {
                tracing::warn!(job_id = %self.job_id, error = ?e, "slack: plan post failed (non-fatal)")
            }
        }
    }

    /// Retire the queued ⏳ and add the terminal `to` reaction on the request
    /// message. Each step is non-fatal + independent.
    async fn swap_reaction(&self, to: &str) {
        if let Err(e) = self
            .client
            .remove_reaction(&self.channel, &self.request_ts, QUEUED_REACTION)
            .await
        {
            tracing::warn!(job_id = %self.job_id, error = ?e, "slack: removing ⏳ reaction failed (non-fatal)");
        }
        if let Err(e) = self
            .client
            .add_reaction(&self.channel, &self.request_ts, to)
            .await
        {
            tracing::warn!(job_id = %self.job_id, error = ?e, "slack: adding terminal reaction failed (non-fatal)");
        }
    }
}

fn stage_started_event_chunks(stage: usize) -> Vec<StreamChunk> {
    match stage {
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
        1 => vec![task_output_event(
            1,
            "Built benchmark binaries",
            cached_build
                .map(|digest| format!("Reused cached build · {digest}"))
                .or_else(|| outputs[1].clone())
                .unwrap_or_else(|| "Build completed.".to_string()),
        )],
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
        "build_done" | "running" => Some(2),
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
        stops: StdMutex<Vec<String>>, // "{ts}:{chunks}:{blocks?}" of each stop_stream
        added: StdMutex<Vec<String>>,
        removed: StdMutex<Vec<String>>,
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
        tl.completed(None, Some("https://s3/stacks-bench.db".into()))
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
        // ⏳ → ✅ swap.
        assert_eq!(*slack.removed.lock().unwrap(), vec![QUEUED_REACTION.to_string()]);
        assert_eq!(*slack.added.lock().unwrap(), vec![COMPLETED_REACTION.to_string()]);
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
        tl.completed(None, Some("https://s3/stacks-bench.db".into()))
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
        assert_eq!(*slack.removed.lock().unwrap(), vec![QUEUED_REACTION.to_string()]);
        assert_eq!(*slack.added.lock().unwrap(), vec![COMPLETED_REACTION.to_string()]);
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
        assert_eq!(*slack.added.lock().unwrap(), vec![FAILED_REACTION.to_string()]);
    }

    #[test]
    fn phase_labels_map_to_stages() {
        assert_eq!(stage_for_phase("starting"), Some(1));
        assert_eq!(stage_for_phase("building"), Some(1));
        assert_eq!(stage_for_phase("build_done"), Some(2));
        assert_eq!(stage_for_phase("running"), Some(2));
        assert_eq!(stage_for_phase("collecting"), Some(3));
        assert_eq!(stage_for_phase("done"), None);
        assert_eq!(stage_for_phase("error"), None);
        assert_eq!(stage_for_phase("whatever"), None);
    }
}
