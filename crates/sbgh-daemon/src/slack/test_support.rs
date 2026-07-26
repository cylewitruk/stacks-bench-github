use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use sbgh_core::db::NewBenchmarkSpec;
use sbgh_core::models::{Job, JobStatus};
use uuid::Uuid;

use super::connector::BenchmarkQueue;

#[derive(Default)]
pub(crate) struct RecordingBenchmarkQueue {
    calls: Mutex<Vec<RecordedCall>>,
    fail_create: AtomicBool,
}

struct RecordedCall {
    job: Job,
    specs: Vec<NewBenchmarkSpec>,
    queued_event_detail: serde_json::Value,
    plan_message_ts: Option<String>,
}

impl RecordingBenchmarkQueue {
    pub(crate) fn fail_create(&self) {
        self.fail_create
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn jobs(&self) -> Vec<Job> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.job.clone())
            .collect()
    }

    pub(crate) fn requested_specs_for_group(&self, group_id: Uuid) -> Vec<NewBenchmarkSpec> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .find(|call| call.job.benchmark_group_id == group_id)
            .map(|call| call.specs.clone())
            .unwrap_or_default()
    }

    pub(crate) fn queued_event_detail(&self, job_id: Uuid) -> Option<serde_json::Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .find(|call| call.job.id == job_id)
            .map(|call| {
                call.queued_event_detail
                    .clone()
            })
    }

    pub(crate) fn plan_message_ts(&self, job_id: Uuid) -> Option<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .find(|call| call.job.id == job_id)
            .and_then(|call| call.plan_message_ts.clone())
    }
}

#[async_trait]
impl BenchmarkQueue for RecordingBenchmarkQueue {
    async fn create_unlinked_benchmark_group(
        &self,
        first_job_id: Uuid,
        requested_specs: &[NewBenchmarkSpec],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> sbgh_core::Result<Job> {
        if self
            .fail_create
            .load(Ordering::SeqCst)
        {
            return Err(sbgh_core::Error::Other(anyhow::anyhow!(
                "injected benchmark queue failure"
            )));
        }
        let first = requested_specs
            .first()
            .ok_or_else(|| {
                sbgh_core::Error::Other(anyhow::anyhow!("benchmark group needs at least one spec"))
            })?;
        let now = Utc::now();
        let group_id = Uuid::new_v4();
        let new = &first.new_job;
        let job = Job {
            id: first_job_id,
            benchmark_group_id: group_id,
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            github_installation_id: new.github_installation_id,
            github_repo_id: new.github_repo_id,
            status: JobStatus::Queued,
            source: new.axes.source,
            intent: new.axes.intent,
            task_kind: new.axes.task_kind,
            build_target: new.axes.build_target,
            git_ref_kind: new.git_ref_kind,
            git_ref_display: new.git_ref_display.clone(),
            git_commit_hash: new.git_commit_hash.clone(),
            git_committed_at: new.git_committed_at,
            workload_key: new.workload_key.clone(),
            claim_token: None,
            claimed_at: None,
            created_at: now,
            updated_at: now,
        };
        self.calls
            .lock()
            .unwrap()
            .push(RecordedCall {
                job: job.clone(),
                specs: requested_specs.to_vec(),
                queued_event_detail: queued_event_detail.clone(),
                plan_message_ts: plan_message_ts.map(str::to_owned),
            });
        Ok(job)
    }
}
