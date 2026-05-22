//! In-memory `JobStore` for unit tests. No persistence, no transactions, no
//! `SKIP LOCKED` semantics; just a `Mutex<Vec<Job>>` that's good enough for
//! exercising handler/orchestrator control flow.

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::Utc;
use sqlx::types::Json;
use uuid::Uuid;

use crate::db::jobs::JobStore;
use crate::models::{Job, JobStatus, NewJob};
use crate::{Error, Result};

#[derive(Debug, Default)]
pub struct InMemoryJobStore {
    inner: Mutex<Vec<Job>>,
}

impl InMemoryJobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<Job> {
        self.inner
            .lock()
            .unwrap()
            .clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Job>> {
        self.inner.lock().unwrap()
    }
}

#[async_trait]
impl JobStore for InMemoryJobStore {
    async fn enqueue(&self, new: &NewJob) -> Result<Option<(Uuid, i64)>> {
        let mut jobs = self.lock();
        // Duplicate-detection on github_delivery_id; matches the partial
        // unique index in Postgres (multiple NULL deliveries are allowed).
        if let Some(delivery) = &new.github_delivery_id
            && jobs.iter().any(|j| {
                j.github_delivery_id
                    .as_deref()
                    == Some(delivery)
            })
        {
            return Ok(None);
        }
        let id = Uuid::new_v4();
        jobs.push(Job {
            id,
            status: JobStatus::Queued,
            repository: new.repository.clone(),
            pr_number: new.pr_number,
            head_sha: new.head_sha.clone(),
            requested_by: new.requested_by.clone(),
            command: new.command.clone(),
            args: Json(new.args.clone()),
            installation_id: new.installation_id,
            comment_id: None,
            github_delivery_id: new.github_delivery_id.clone(),
            queued_at: Utc::now(),
            started_at: None,
            finished_at: None,
            result: None,
            error: None,
        });
        let position = jobs
            .iter()
            .filter(|j| j.status == JobStatus::Queued)
            .count() as i64;
        Ok(Some((id, position)))
    }

    async fn claim_next(&self) -> Result<Option<Job>> {
        let mut jobs = self.lock();
        let Some(idx) = jobs
            .iter()
            .enumerate()
            .filter(|(_, j)| j.status == JobStatus::Queued)
            .min_by_key(|(_, j)| j.queued_at)
            .map(|(i, _)| i)
        else {
            return Ok(None);
        };
        jobs[idx].status = JobStatus::Running;
        jobs[idx].started_at = Some(Utc::now());
        Ok(Some(jobs[idx].clone()))
    }

    async fn complete(&self, id: Uuid, result: serde_json::Value) -> Result<()> {
        let mut jobs = self.lock();
        let job = find_mut(&mut jobs, id)?;
        job.status = JobStatus::Completed;
        job.finished_at = Some(Utc::now());
        job.result = Some(Json(result));
        Ok(())
    }

    async fn fail(&self, id: Uuid, error: &str, summary: Option<serde_json::Value>) -> Result<()> {
        let mut jobs = self.lock();
        let job = find_mut(&mut jobs, id)?;
        job.status = JobStatus::Failed;
        job.finished_at = Some(Utc::now());
        job.error = Some(error.into());
        if let Some(s) = summary {
            job.result = Some(Json(s));
        }
        Ok(())
    }

    async fn set_comment_id(&self, id: Uuid, comment_id: i64) -> Result<()> {
        let mut jobs = self.lock();
        let job = find_mut(&mut jobs, id)?;
        job.comment_id = Some(comment_id);
        Ok(())
    }

    async fn queue_position(&self, id: Uuid) -> Result<Option<i64>> {
        let jobs = self.lock();
        let Some(target) = jobs
            .iter()
            .find(|j| j.id == id)
        else {
            return Ok(None);
        };
        if target.status != JobStatus::Queued {
            return Ok(None);
        }
        let pos = jobs
            .iter()
            .filter(|j| j.status == JobStatus::Queued && j.queued_at <= target.queued_at)
            .count() as i64;
        Ok(Some(pos))
    }
}

fn find_mut(jobs: &mut [Job], id: Uuid) -> Result<&mut Job> {
    jobs.iter_mut()
        .find(|j| j.id == id)
        .ok_or_else(|| Error::Config(format!("job not found: {id}")))
}
