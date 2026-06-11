//! The Slack live-timeline `plan` card (item `0023`, iteration v8).
//!
//! One [`SlackTimeline`] per Slack ad-hoc job, driven by the reporter at three
//! points: posted when the job starts running ([`SlackTimeline::started`]),
//! `chat.update`d as it advances **Job → Build → Run → Finalize**
//! ([`SlackTimeline::advance`], from the worker's phase events), and finalized
//! at terminal ([`SlackTimeline::completed`]/[`failed`](SlackTimeline::failed)/
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

use tokio::sync::Mutex;

use crate::bench_summary::RunResult;
use crate::job_source::{RunnableJob, RunnableJobStore};
use crate::slack::card::{self, CardCtx, Results, STAGES};
use crate::slack::client::{COMPLETED_REACTION, FAILED_REACTION, QUEUED_REACTION, SlackClient};

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
    /// The furthest stage reached — the in-progress row (0..STAGES). Monotonic.
    stage: usize,
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
                plan_ts: plan_message_ts,
                stage: 0,
            }),
        }
    }

    /// Post (or, on re-claim, resume) the card with the job started — Job
    /// complete, Build in progress.
    pub async fn started(&self) {
        let stage = {
            let mut st = self.state.lock().await;
            // The job has started → Build is the active row (Job is complete).
            st.stage = st.stage.max(1);
            st.stage
        };
        self.upsert(&card::running(&self.ctx(), stage))
            .await;
    }

    /// Advance to `stage` — `chat.update` the card so the prior rows show
    /// complete and `stage` shows in-progress. Monotonic: a same/earlier stage
    /// (e.g. a repeat or out-of-order phase) is a no-op.
    pub async fn advance(&self, stage: usize) {
        let stage = stage.min(STAGES - 1);
        {
            let mut st = self.state.lock().await;
            if stage <= st.stage {
                return;
            }
            st.stage = stage;
        }
        self.upsert(&card::running(&self.ctx(), stage))
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
        self.state.lock().await.stage = STAGES;
        let blocks = card::completed(
            &self.ctx(),
            Results {
                metrics: result.as_ref(),
                db_url: db_url.as_deref(),
            },
        );
        self.upsert(&blocks).await;
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
        let stage = self
            .state
            .lock()
            .await
            .stage
            .min(STAGES - 1);
        let blocks = card::failed(&self.ctx(), stage, message);
        self.upsert(&blocks).await;
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
            cached_build: self
                .cached_build
                .get()
                .map(String::as_str),
        }
    }

    /// Post the card if we don't yet have one (persisting its `ts` for
    /// resume-on-reclaim), else `chat.update` the existing one. Non-fatal.
    async fn upsert(&self, blocks: &serde_json::Value) {
        let fallback = format!("Benchmark {} @ {}", self.rev, self.short_commit());
        let existing = self
            .state
            .lock()
            .await
            .plan_ts
            .clone();
        match existing {
            Some(ts) => {
                if let Err(e) = self
                    .client
                    .update_blocks(&self.channel, &ts, blocks, &fallback)
                    .await
                {
                    tracing::warn!(job_id = %self.job_id, error = ?e, "slack: plan update failed (non-fatal)");
                }
            }
            None => {
                match self
                    .client
                    .post_blocks_in_thread(&self.channel, &self.request_ts, blocks, &fallback)
                    .await
                {
                    Ok(ts) => {
                        self.state
                            .lock()
                            .await
                            .plan_ts = Some(ts.clone());
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

    fn short_commit(&self) -> &str {
        self.commit
            .get(..8)
            .unwrap_or(&self.commit)
    }
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
            repository: "octo/core".into(),
            commit: "abcdef1234567890".into(),
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
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
        let tl = timeline(slack.clone(), store.clone(), None);

        tl.started().await; // post (Build in_progress)
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
            1,
            "only the first card is posted"
        );
        let updates = slack.updates.lock().unwrap();
        assert_eq!(updates.len(), 2, "advance(2) + completed (advance(1) was a no-op)");
        assert!(
            updates
                .iter()
                .all(|u| u.starts_with("PLAN_TS:")),
            "{updates:?}"
        );
        // The completed update carries the results blocks + the download button.
        assert!(updates[1].contains("\"type\":\"markdown\""), "{}", updates[1]);
        assert!(updates[1].contains("## Benchmark Results"), "{}", updates[1]);
        assert!(updates[1].contains("Download Profiler Data"), "{}", updates[1]);
        assert!(updates[1].contains("\"style\":\"primary\""), "{}", updates[1]);
        assert!(
            !updates[1].contains("\"status\":\"in_progress\""),
            "all rows complete: {}",
            updates[1]
        );
        assert!(
            updates[1].contains("Benchmark develop @ abcdef12"),
            "terminal title: {}",
            updates[1]
        );
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
        let updates = slack.updates.lock().unwrap();
        assert_eq!(updates.len(), 1);
        assert!(updates[0].starts_with("OLD_TS:"), "resumes the persisted card: {}", updates[0]);
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
    async fn failed_marks_the_current_row_error_and_swaps_to_x() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), None);

        tl.started().await;
        tl.advance(2).await; // failure during the Run stage
        tl.failed("boom: VM died")
            .await;

        let updates = slack.updates.lock().unwrap();
        let last = updates.last().unwrap();
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
