//! Slack integration boundary.
//!
//! This crate owns Socket Mode intake, the narrow Web API contract, request
//! orchestration, canonical snapshot rendering, and replay-safe message
//! publication. Database lookup and benchmark result policy remain in the
//! daemon.

mod api_client;
mod client;
mod connector;
mod error;
mod snapshot;
mod socket;

#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub use api_client::WebApiClient;
pub use client::{
    ACK_REACTION, COMPLETED_REACTION, FAILED_REACTION, FoundMessage, QUEUED_REACTION,
    RUNNING_REACTION, SlackClient,
};
pub use connector::{
    BenchmarkQueue, BenchmarkVariantRequest, MentionEvent, SlackConnector, SlackConnectorConfig,
    SlackJobTarget, SlackSubmissionActor,
};
pub use error::{Result, SlackError};
pub use snapshot::{
    MessageIdentityStore, PublishUrgency, ReportingIdentity, RunPosition, SlackMessageTarget,
    SlackProgress, SlackProgressView, SlackSnapshotPublisher, SlackStatus, render_snapshot,
};
pub use socket::{SlackSocketConfig, SocketRunOptions, mention_from_callback, run};
