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

    /// Update the PR comment with a short failure snippet.
    ///
    /// `error` is the full anyhow chain — internal paths, command flags,
    /// stderr dumps. Don't leak that to the PR; it's noisy and exposes
    /// orchestrator internals. Surface only the first line, truncated,
    /// and point at the orchestrator logs for details. The DB row still
    /// gets the full string via `JobStore::fail`.
    pub async fn failed(&self, error: &str) -> anyhow::Result<()> {
        let snippet = short_pr_error(error);
        self.update(&format!(
            ":x: benchmark `{id}` failed: `{snippet}`\n\n_(full details in the orchestrator logs)_",
            id = self.job.id,
        ))
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

/// Trim an error chain down to something safe to show a PR author:
///   - take only the first non-empty line (drops follow-on stderr lines,
///     debug-printed structs, etc.),
///   - strip the noisy "X failed with status exit status: Y: stdout=..." prefix
///     our shell wrapper attaches (the underlying tool's actual stderr is
///     what's interesting; the wrapper bits never are),
///   - cap at 160 chars with an ellipsis so a single very-long line can't blow
///     up the PR comment either.
fn short_pr_error(error: &str) -> String {
    const MAX_LEN: usize = 160;

    let first = error
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("(no error message)");

    // If our shell wrapper's prefix is present, prefer whatever follows
    // `stderr=` on the same line — that's the underlying tool's voice.
    let stripped = first
        .split_once("stderr=")
        .map(|(_, rhs)| rhs.trim_start())
        .filter(|s| !s.is_empty())
        .unwrap_or(first);

    if stripped.chars().count() <= MAX_LEN {
        stripped.to_string()
    } else {
        let mut out: String = stripped
            .chars()
            .take(MAX_LEN - 1)
            .collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::short_pr_error;

    #[test]
    fn short_message_unchanged() {
        assert_eq!(short_pr_error("disk full"), "disk full");
    }

    #[test]
    fn first_line_only() {
        let raw = "virsh start sbgh-x failed\nerror: detail\nmore detail";
        assert_eq!(short_pr_error(raw), "virsh start sbgh-x failed");
    }

    #[test]
    fn strips_shell_wrapper_prefix() {
        let raw = "virsh start sbgh-x failed with status exit status: 1: stdout= stderr=error: \
                   Failed to start domain";
        assert_eq!(short_pr_error(raw), "error: Failed to start domain");
    }

    #[test]
    fn truncates_long_single_line_with_ellipsis() {
        let raw = "a".repeat(500);
        let out = short_pr_error(&raw);
        assert!(out.chars().count() <= 160);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn empty_input_has_placeholder() {
        assert_eq!(short_pr_error(""), "(no error message)");
        assert_eq!(short_pr_error("\n\n   \n"), "(no error message)");
    }
}
