//! Posts/edits the PR comment owned by a job as its lifecycle advances.

use std::path::Path;

use sbgh_core::github::GitHubApi;

use crate::bench_summary::{self, RunResult};
use crate::job_source::{ProgressTarget, RunnableJob};

pub struct ProgressReporter<'a> {
    gh: &'a dyn GitHubApi,
    job: &'a RunnableJob,
}

impl<'a> ProgressReporter<'a> {
    pub fn new(gh: &'a dyn GitHubApi, job: &'a RunnableJob) -> Self {
        Self { gh, job }
    }

    pub async fn started(&self) -> anyhow::Result<()> {
        self.update(&format!(
            ":rocket: benchmark `{id}` is running on commit `{sha}`.",
            id = self.job.id,
            sha = self.job.commit,
        ))
        .await
    }

    pub async fn completed(&self, summary: &serde_json::Value) -> anyhow::Result<()> {
        // The daemon-side summary blob carries pointers to the
        // archived artifacts. Read + parse run.json (the actual
        // stacks-bench output) for the user-facing metrics; everything
        // else in the summary blob is operator/debugging detail that
        // doesn't belong in the PR comment.
        let archive_dir = summary
            .get("archive_dir")
            .and_then(|v| v.as_str())
            .unwrap_or("/var/lib/sbgh/results");
        let run_json_path = summary
            .get("run_json_archived_path")
            .and_then(|v| v.as_str());
        let parsed = run_json_path
            .map(Path::new)
            .and_then(read_run_json);

        let body = bench_summary::render_pr_comment(
            &self.job.id.to_string(),
            &self.job.commit,
            archive_dir,
            parsed.as_ref(),
        );
        self.update(&body).await
    }

    /// Update the PR comment with a short failure snippet.
    ///
    /// `error` is the full anyhow chain — internal paths, command flags,
    /// stderr dumps. Don't leak that to the PR; it's noisy and exposes
    /// daemon internals. Surface only the first line, truncated,
    /// and point at the daemon logs for details. The DB row still
    /// gets the full string via `JobStore::fail`.
    pub async fn failed(&self, error: &str) -> anyhow::Result<()> {
        let snippet = short_pr_error(error);
        self.update(&format!(
            ":x: benchmark `{id}` failed: `{snippet}`\n\n_(full details in the daemon logs)_",
            id = self.job.id,
        ))
        .await
    }

    async fn update(&self, body: &str) -> anyhow::Result<()> {
        match self.job.progress {
            ProgressTarget::PullRequestComment {
                comment_id: Some(comment_id), ..
            } => {
                self.gh
                    .update_pr_comment(
                        self.job.installation_id,
                        &self.job.repository,
                        comment_id,
                        body,
                    )
                    .await?;
                Ok(())
            }
            ProgressTarget::PullRequestComment { comment_id: None, .. } => {
                tracing::warn!(job_id = %self.job.id, "no comment id; skipping update");
                Ok(())
            }
            // New-schema BASELINE jobs (branch_push) have no PR, so
            // progress goes to logs only — no comment, and no intermediate
            // phase `job_event` rows yet (that timeline is still deferred).
            // The queued + terminal events are persisted elsewhere.
            // (PR-comment jobs take the branch above; comment posting for
            // those landed in the slice 11 cutover.)
            ProgressTarget::LogOnly => {
                tracing::debug!(job_id = %self.job.id, body, "progress (log-only)");
                Ok(())
            }
        }
    }
}

/// Best-effort read + parse of the archived `run.json`. Returns `None`
/// (and lets the renderer fall back to "missing/unparseable" text) on
/// any I/O or parse error — we never want a forensics gap to crash
/// the PR-comment update for a successful run.
fn read_run_json(path: &Path) -> Option<RunResult> {
    match std::fs::read(path) {
        Ok(bytes) => RunResult::from_bytes(&bytes),
        Err(e) => {
            tracing::warn!(error = %e, path = %path.display(), "read archived run.json failed");
            None
        }
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
