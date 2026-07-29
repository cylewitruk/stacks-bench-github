//! Narrow persistence port for atomic task-submission creation.

use async_trait::async_trait;

use crate::submission::{PreparedSubmission, SubmissionError, SubmissionReceipt};

#[async_trait]
pub trait SubmissionStore: Send + Sync + 'static {
    async fn persist_submission(
        &self,
        prepared: &PreparedSubmission,
    ) -> Result<SubmissionReceipt, SubmissionError>;
}
