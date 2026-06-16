//! The Slack Web API surface the connector + reporter need, behind a trait so
//! the orchestration is testable without a live workspace. The real impl (bot
//! token + `reqwest`) and the Socket Mode receive loop land with the wiring
//! slice; everything here is driven by a fake in tests.

use async_trait::async_trait;

use crate::slack::stream::StreamChunk;

/// Acknowledgment reaction, added once a mention is authorized — before the
/// (possibly slow) workload resolution. Swapped for ⏳ on accept, removed on
/// rejection.
pub const ACK_REACTION: &str = "eyes";

/// Queued reaction, added on accept and swapped for 🚀 once running.
pub const QUEUED_REACTION: &str = "hourglass_flowing_sand";

/// Running reaction; the first run starting swaps ⏳ → 🚀.
pub const RUNNING_REACTION: &str = "rocket";

/// Terminal reaction for a completed job.
pub const COMPLETED_REACTION: &str = "white_check_mark";

/// Terminal reaction for a failed or cancelled job.
pub const FAILED_REACTION: &str = "x";

/// Slack Web API calls the connector + reporter make. Errors are surfaced so
/// callers can log them; a Slack hiccup is never fatal to the benchmark.
#[async_trait]
pub trait SlackClient: Send + Sync {
    /// Post a message visible **only** to `user` in `channel` — the
    /// invoker-only rejection surface (auth/parse failures), so a bad request
    /// never clutters the channel or a thread.
    async fn post_ephemeral(&self, channel: &str, user: &str, text: &str) -> anyhow::Result<()>;

    /// Post a Block Kit message (`blocks`) as a threaded reply under
    /// `thread_ts` — the live-timeline `plan` card. `fallback` is the plain
    /// notification/accessibility text Slack shows where blocks can't render.
    /// Returns the posted message's `ts` so the caller can `chat.update` it as
    /// the run advances (the live timeline).
    async fn post_blocks_in_thread(
        &self,
        channel: &str,
        thread_ts: &str,
        blocks: &serde_json::Value,
        fallback: &str,
    ) -> anyhow::Result<String>;

    /// Start a Slack streamed reply using `task_display_mode=plan`. Returns the
    /// stream message's `ts`, which is also the persisted plan-message
    /// identity. The fallback text stays in the trait boundary for callers, but
    /// the live Slack API rejects `markdown_text` when chunks are present.
    async fn start_plan_stream(
        &self,
        _channel: &str,
        _thread_ts: &str,
        _recipient_user_id: &str,
        _recipient_team_id: &str,
        _markdown_text: &str,
        _chunks: &[StreamChunk],
    ) -> anyhow::Result<String> {
        anyhow::bail!("slack streaming is not implemented by this client")
    }

    /// Append semantic stream chunks to an existing stream. The live Slack API
    /// rejects `markdown_text` when `chunks` are present, even though the docs
    /// still mark it as required for append calls.
    async fn append_stream(
        &self,
        _channel: &str,
        _ts: &str,
        _chunks: &[StreamChunk],
    ) -> anyhow::Result<()> {
        anyhow::bail!("slack streaming is not implemented by this client")
    }

    /// Stop a stream and optionally render bottom blocks (the terminal results
    /// table/download button).
    async fn stop_stream(
        &self,
        _channel: &str,
        _ts: &str,
        _markdown_text: Option<&str>,
        _chunks: &[StreamChunk],
        _blocks: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("slack streaming is not implemented by this client")
    }

    /// `chat.update` the message at `ts` in `channel` with new `blocks` — the
    /// live-timeline edit (Build → Benchmark → Archive status transitions).
    async fn update_blocks(
        &self,
        channel: &str,
        ts: &str,
        blocks: &serde_json::Value,
        fallback: &str,
    ) -> anyhow::Result<()>;

    /// Add an emoji `reaction` to the message at `ts` in `channel` — the
    /// lifecycle-status surface on the user's request message (no bot parent).
    async fn add_reaction(&self, channel: &str, ts: &str, reaction: &str) -> anyhow::Result<()>;

    /// Remove an emoji `reaction` from the message at `ts` in `channel` — used
    /// to retire the queued ⏳ before adding a terminal ✅/❌.
    async fn remove_reaction(&self, channel: &str, ts: &str, reaction: &str) -> anyhow::Result<()>;
}
