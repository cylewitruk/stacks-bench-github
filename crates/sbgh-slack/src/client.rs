//! Narrow outbound Slack Web API contract.

use async_trait::async_trait;

use crate::{ReportingIdentity, Result, SlackMessageTarget};

pub const ACK_REACTION: &str = "eyes";
pub const QUEUED_REACTION: &str = "hourglass_flowing_sand";
pub const RUNNING_REACTION: &str = "rocket";
pub const COMPLETED_REACTION: &str = "white_check_mark";
pub const FAILED_REACTION: &str = "x";

/// A bot-authored message found while reconciling a reporting identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundMessage {
    pub ts: String,
    pub text: String,
    pub snapshot_version: u64,
}

/// Slack calls needed by request intake and snapshot reporting.
#[async_trait]
pub trait SlackClient: Send + Sync {
    async fn post_ephemeral(&self, channel: &str, user: &str, text: &str) -> Result<()>;

    async fn post_message(
        &self,
        target: &SlackMessageTarget,
        text: &str,
        identity: &ReportingIdentity,
        snapshot_version: u64,
    ) -> Result<String>;

    async fn update_message(
        &self,
        channel: &str,
        ts: &str,
        text: &str,
        identity: &ReportingIdentity,
        snapshot_version: u64,
    ) -> Result<()>;

    /// Find exact bot-authored messages for `identity`, bounded to `target`.
    ///
    /// Lookup errors must be returned as errors, never converted to an empty
    /// result: callers use an empty result as permission to create.
    async fn find_messages(
        &self,
        target: &SlackMessageTarget,
        identity: &ReportingIdentity,
    ) -> Result<Vec<FoundMessage>>;

    async fn add_reaction(&self, channel: &str, ts: &str, reaction: &str) -> Result<()>;

    async fn remove_reaction(&self, channel: &str, ts: &str, reaction: &str) -> Result<()>;
}
