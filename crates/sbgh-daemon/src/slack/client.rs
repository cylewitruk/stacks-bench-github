//! The Slack Web API surface the connector + reporter need, behind a trait so
//! the orchestration is testable without a live workspace. The real impl (bot
//! token + `reqwest`) and the Socket Mode receive loop land with the wiring
//! slice; everything here is driven by a fake in tests.

use async_trait::async_trait;

/// The lifecycle-status reaction added to a request message while its job is
/// queued (the connector adds it; the reporter swaps it for one of the two
/// terminals below).
pub const QUEUED_REACTION: &str = "hourglass_flowing_sand";

/// Terminal reaction for a job that ran to completion (swaps ⏳).
pub const COMPLETED_REACTION: &str = "white_check_mark";

/// Terminal reaction for a job that failed or was cancelled (swaps ⏳).
pub const FAILED_REACTION: &str = "x";

/// Slack Web API calls the connector + reporter make. Errors are surfaced so
/// callers can log them; a Slack hiccup is never fatal to the benchmark.
#[async_trait]
pub trait SlackClient: Send + Sync {
    /// Post a message visible **only** to `user` in `channel` — the
    /// invoker-only rejection surface (auth/parse failures), so a bad request
    /// never clutters the channel or a thread.
    async fn post_ephemeral(&self, channel: &str, user: &str, text: &str) -> anyhow::Result<()>;

    /// Post `text` as a threaded reply under the message at `thread_ts` in
    /// `channel` — the terminal result surface, kept in the request's thread so
    /// it never spams the channel.
    async fn post_in_thread(
        &self,
        channel: &str,
        thread_ts: &str,
        text: &str,
    ) -> anyhow::Result<()>;

    /// Add an emoji `reaction` to the message at `ts` in `channel` — the
    /// lifecycle-status surface on the user's request message (no bot parent).
    async fn add_reaction(&self, channel: &str, ts: &str, reaction: &str) -> anyhow::Result<()>;

    /// Remove an emoji `reaction` from the message at `ts` in `channel` — used
    /// to retire the queued ⏳ before adding a terminal ✅/❌.
    async fn remove_reaction(&self, channel: &str, ts: &str, reaction: &str) -> anyhow::Result<()>;
}
