//! Daemon composition adapter for Slack's narrow enqueue port.

use std::sync::Arc;

use async_trait::async_trait;
use sbgh_core::db::{JobStore, NewBenchmarkSpec};
use sbgh_core::models::Job;
use sbgh_slack::BenchmarkQueue;
use uuid::Uuid;

/// Projects the broad transactional job store into the one operation Slack
/// intake needs. The adapter stays in the daemon so `sbgh-slack` never depends
/// on `JobStore`.
pub struct SlackBenchmarkQueue {
    jobs: Arc<dyn JobStore>,
}

impl SlackBenchmarkQueue {
    pub fn new(jobs: Arc<dyn JobStore>) -> Self {
        Self { jobs }
    }
}

#[async_trait]
impl BenchmarkQueue for SlackBenchmarkQueue {
    async fn create_unlinked_benchmark_group(
        &self,
        first_job_id: Uuid,
        specs: &[NewBenchmarkSpec],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> sbgh_core::Result<Job> {
        self.jobs
            .create_unlinked_benchmark_group(
                first_job_id,
                specs,
                queued_event_detail,
                plan_message_ts,
            )
            .await
    }
}
