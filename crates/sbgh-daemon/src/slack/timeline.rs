//! The Slack live-timeline `plan` card (item 0002, slice B).
//!
//! One [`SlackTimeline`] per Slack ad-hoc job, driven by the reporter at three
//! points: posted when the job starts running (`started`), `chat.update`d as it
//! advances Build → Benchmark → Archive (`advance`, from the worker's phase
//! events), and finalized at terminal (`completed`/`failed`/`cancelled`) with
//! the ⏳ → ✅/❌ reaction swap on the request message.
//!
//! The posted card's `ts` is persisted (`set_plan_message_ts`) and read back on
//! re-claim, so a daemon restart resumes updating the **same** card instead of
//! posting a duplicate. Every Slack call is non-fatal (logged, never failing
//! the benchmark).

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::bench_summary::{self, PlanCard, PlanTaskStatus, RunResult};
use crate::job_source::{RunnableJob, RunnableJobStore};
use crate::slack::client::{COMPLETED_REACTION, FAILED_REACTION, QUEUED_REACTION, SlackClient};

/// The number of plan stages (Build, Benchmark, Archive).
const STAGES: usize = 3;

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
    state: Mutex<State>,
}

struct State {
    /// The plan card's own message `ts` (`None` until posted; pre-seeded from
    /// the persisted value on re-claim so we resume the existing card).
    plan_ts: Option<String>,
    /// The furthest stage reached (0..STAGES). Monotonic.
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
            state: Mutex::new(State {
                plan_ts: plan_message_ts,
                stage: 0,
            }),
        }
    }

    /// Post (or, on re-claim, resume) the card with the Build row in progress.
    pub async fn started(&self) {
        let blocks = self.plan_blocks(live_statuses(0), Default::default(), None);
        self.upsert(&blocks).await;
    }

    /// Advance to `stage` (Build=0, Benchmark=1, Archive=2) — `chat.update` the
    /// card so the prior rows show complete and `stage` shows in-progress.
    /// Monotonic: a same/earlier stage (e.g. a heartbeat) is a no-op.
    pub async fn advance(&self, stage: usize) {
        let stage = stage.min(STAGES - 1);
        {
            let mut st = self.state.lock().await;
            if stage <= st.stage {
                return;
            }
            st.stage = stage;
        }
        let blocks = self.plan_blocks(live_statuses(stage), Default::default(), None);
        self.upsert(&blocks).await;
    }

    /// Terminal success: all rows complete, metrics in the Benchmark row, the
    /// DB download on the Archive row (S3 only); swap ⏳ → ✅.
    pub async fn completed(&self, result: Option<RunResult>, db_url: Option<String>) {
        let metrics = bench_summary::metrics_output_text(result.as_ref());
        let outputs = [None, Some(metrics), None];
        let blocks =
            self.plan_blocks([PlanTaskStatus::Complete; STAGES], outputs, db_url.as_deref());
        self.upsert(&blocks).await;
        self.swap_reaction(COMPLETED_REACTION)
            .await;
    }

    /// Terminal failure: the current row → error (carrying the message), later
    /// rows pending; swap ⏳ → ❌.
    pub async fn failed(&self, error: &str) {
        self.terminate_error(format!("Failed: {error}"))
            .await;
    }

    /// Terminal cancellation: like [`failed`](Self::failed) but a cancel note.
    pub async fn cancelled(&self, reason: &str) {
        self.terminate_error(format!(
            "Cancelled: {reason}. Re-run by mentioning me with a new `bench …` request."
        ))
        .await;
    }

    async fn terminate_error(&self, message: String) {
        let stage = self.state.lock().await.stage;
        let mut outputs: [Option<String>; STAGES] = Default::default();
        outputs[stage] = Some(message);
        let blocks = self.plan_blocks(error_statuses(stage), outputs, None);
        self.upsert(&blocks).await;
        self.swap_reaction(FAILED_REACTION)
            .await;
    }

    /// Render the `plan` blocks for the given per-row statuses + outputs.
    fn plan_blocks(
        &self,
        statuses: [PlanTaskStatus; STAGES],
        outputs: [Option<String>; STAGES],
        db_url: Option<&str>,
    ) -> serde_json::Value {
        bench_summary::render_plan_blocks(&PlanCard {
            rev: &self.rev,
            commit: &self.commit,
            job_id: &self.job_id,
            statuses,
            outputs,
            db_url,
            commit_url: Some(&self.commit_url),
        })
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

/// Live statuses for `stage`: earlier rows complete, `stage` in-progress, later
/// pending.
fn live_statuses(stage: usize) -> [PlanTaskStatus; STAGES] {
    std::array::from_fn(|i| match i.cmp(&stage) {
        std::cmp::Ordering::Less => PlanTaskStatus::Complete,
        std::cmp::Ordering::Equal => PlanTaskStatus::InProgress,
        std::cmp::Ordering::Greater => PlanTaskStatus::Pending,
    })
}

/// Terminal-error statuses: earlier rows complete, `stage` error, later
/// pending.
fn error_statuses(stage: usize) -> [PlanTaskStatus; STAGES] {
    std::array::from_fn(|i| match i.cmp(&stage) {
        std::cmp::Ordering::Less => PlanTaskStatus::Complete,
        std::cmp::Ordering::Equal => PlanTaskStatus::Error,
        std::cmp::Ordering::Greater => PlanTaskStatus::Pending,
    })
}

/// Map a worker phase label to its plan stage (Build=0, Benchmark=1,
/// Archive=2), or `None` for phases that don't advance the live timeline
/// (terminal `done`/`error`, or an unknown label). Drives the reporter's
/// per-phase [`SlackTimeline::advance`] calls.
pub fn stage_for_phase(label: &str) -> Option<usize> {
    match label {
        "starting" | "building" => Some(0),
        "build_done" | "running" => Some(1),
        "collecting" => Some(2),
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
        updates: StdMutex<Vec<String>>, // blocks json of each update_blocks (by ts)
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
    async fn started_posts_the_plan_and_persists_its_ts() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), None);

        tl.started().await;

        // One post (the card), and its ts persisted for resume.
        let posts = slack.posts.lock().unwrap();
        assert_eq!(posts.len(), 1);
        assert!(posts[0].contains("\"type\":\"plan\""), "{}", posts[0]);
        assert!(posts[0].contains("in_progress"), "Build row in progress: {}", posts[0]);
        assert_eq!(
            *store
                .persisted
                .lock()
                .unwrap(),
            vec!["PLAN_TS".to_string()]
        );
        assert!(
            slack
                .updates
                .lock()
                .unwrap()
                .is_empty(),
            "no update before terminal"
        );
    }

    #[tokio::test]
    async fn advance_then_complete_updates_in_place_and_swaps_reaction() {
        let slack = Arc::new(FakeSlack::default());
        let store = Arc::new(RecordingStore::default());
        let tl = timeline(slack.clone(), store.clone(), None);

        tl.started().await; // post (Build in_progress)
        tl.advance(1).await; // update (Benchmark in_progress)
        tl.advance(0).await; // monotonic: no-op (earlier stage)
        tl.completed(None, Some("https://s3/db".into()))
            .await; // update (complete)

        // Exactly one post (the initial card); the rest are in-place updates.
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
        assert_eq!(updates.len(), 2, "advance(1) + completed (advance(0) was a no-op)");
        // All updates target the same persisted ts.
        assert!(
            updates
                .iter()
                .all(|u| u.starts_with("PLAN_TS:")),
            "{updates:?}"
        );
        // The completed update carries the DB link + all-complete.
        assert!(updates[1].contains("Download stacks-bench.db"), "{}", updates[1]);
        assert!(!updates[1].contains("in_progress"), "all rows complete: {}", updates[1]);
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
        tl.advance(1).await; // failure during the Benchmark stage
        tl.failed("boom: VM died")
            .await;

        let updates = slack.updates.lock().unwrap();
        let last = updates.last().unwrap();
        assert!(last.contains("\"status\":\"error\""), "an errored row: {last}");
        assert!(last.contains("boom: VM died"), "carries the error: {last}");
        assert_eq!(*slack.added.lock().unwrap(), vec![FAILED_REACTION.to_string()]);
    }

    #[test]
    fn phase_labels_map_to_stages() {
        assert_eq!(stage_for_phase("starting"), Some(0));
        assert_eq!(stage_for_phase("building"), Some(0));
        assert_eq!(stage_for_phase("build_done"), Some(1));
        assert_eq!(stage_for_phase("running"), Some(1));
        assert_eq!(stage_for_phase("collecting"), Some(2));
        assert_eq!(stage_for_phase("done"), None);
        assert_eq!(stage_for_phase("error"), None);
        assert_eq!(stage_for_phase("whatever"), None);
    }
}
