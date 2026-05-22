//! Posts/edits the PR comment owned by a job as its lifecycle advances.

use sbgh_core::github::GitHubApi;
use sbgh_core::models::Job;

pub struct ProgressReporter<'a> {
    gh: &'a dyn GitHubApi,
    job: &'a Job,
}

impl<'a> ProgressReporter<'a> {
    pub fn new(gh: &'a dyn GitHubApi, job: &'a Job) -> Self {
        Self { gh, job }
    }

    pub async fn started(&self) -> anyhow::Result<()> {
        self.update(&format!(
            ":rocket: benchmark `{id}` is running on commit `{sha}`.",
            id = self.job.id,
            sha = self.job.head_sha,
        ))
        .await
    }

    pub async fn completed(&self, summary: &serde_json::Value) -> anyhow::Result<()> {
        self.update(&format!(
            ":white_check_mark: benchmark `{id}` completed.\n\n```json\n{summary}\n```",
            id = self.job.id,
            summary = serde_json::to_string_pretty(summary).unwrap_or_default(),
        ))
        .await
    }

    pub async fn failed(&self, error: &str) -> anyhow::Result<()> {
        self.update(&format!(":x: benchmark `{id}` failed: {error}", id = self.job.id,))
            .await
    }

    async fn update(&self, body: &str) -> anyhow::Result<()> {
        let Some(comment_id) = self.job.comment_id else {
            tracing::warn!(job_id = %self.job.id, "no comment id; skipping update");
            return Ok(());
        };
        self.gh
            .update_pr_comment(self.job.installation_id, &self.job.repository, comment_id, body)
            .await?;
        Ok(())
    }
}
