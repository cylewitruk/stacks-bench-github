//! Surfaces a job's lifecycle on its reporting target(s) — a PR comment
//! and/or a GitHub Check Run (roadmap-v4). The whole surface is **non-fatal**:
//! a reporting error is logged and swallowed, never failing the benchmark.

use std::path::Path;

use sbgh_core::github::{
    CheckRunConclusion, CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi,
};

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

    /// Claimed → running. Refresh the comment; the check stays `in_progress`
    /// (it transitions to `neutral` only at a terminal state).
    pub async fn started(&self) {
        self.update_comment(&format!(
            ":rocket: benchmark `{id}` is running on commit `{sha}`.",
            id = self.job.id,
            sha = self.job.commit,
        ))
        .await;
    }

    pub async fn completed(&self, summary: &serde_json::Value) {
        let body = self.completed_body(summary);
        self.update_comment(&body)
            .await;
        // Only PR jobs with a comment have one to point at; a baseline's commit
        // check is self-contained.
        let pointer = if self.has_comment() { "see comment / details" } else { "see details" };
        self.complete_check(CheckRunOutput {
            title: format!("benchmark {} — complete", self.job.id),
            summary: format!("commit `{}` — {pointer}", self.job.commit),
            text: Some(body),
        })
        .await;
    }

    /// Whether this job has a PR comment surface (a `PullRequest` target whose
    /// comment was actually posted).
    fn has_comment(&self) -> bool {
        matches!(self.job.progress, ProgressTarget::PullRequest { comment_id: Some(_), .. })
    }

    /// Terminal failure. `error` is the full anyhow chain (internal paths,
    /// stderr) — surface only a short, sanitized snippet; the DB row keeps the
    /// full string. A *failed* check is still `neutral` (non-blocking), never
    /// a red `failure`.
    pub async fn failed(&self, error: &str) {
        let snippet = short_pr_error(error);
        self.update_comment(&format!(
            ":x: benchmark `{id}` failed: `{snippet}`\n\n_(full details in the daemon logs)_",
            id = self.job.id,
        ))
        .await;
        self.complete_check(CheckRunOutput {
            title: format!("benchmark {} — failed", self.job.id),
            summary: format!("commit `{}` failed", self.job.commit),
            text: Some(format!("```\n{snippet}\n```\n\n_(full details in the daemon logs)_")),
        })
        .await;
    }

    /// The shared completed render (read + parse the archived `run.json` for
    /// the user-facing metrics) used by both the comment and the check.
    fn completed_body(&self, summary: &serde_json::Value) -> String {
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
        bench_summary::render_pr_comment(
            &self.job.id.to_string(),
            &self.job.commit,
            archive_dir,
            parsed.as_ref(),
        )
    }

    /// Edit the PR comment if this job has one. Non-fatal.
    async fn update_comment(&self, body: &str) {
        let ProgressTarget::PullRequest {
            comment_id: Some(comment_id), ..
        } = self.job.progress
        else {
            tracing::debug!(job_id = %self.job.id, "progress (no comment surface)");
            return;
        };
        if let Err(e) = self
            .gh
            .update_pr_comment(self.job.installation_id, &self.job.repository, comment_id, body)
            .await
        {
            tracing::warn!(job_id = %self.job.id, error = ?e, "update PR comment failed (non-fatal)");
        }
    }

    /// Complete this job's Check Run (`neutral`) if it has one. Non-fatal.
    async fn complete_check(&self, output: CheckRunOutput) {
        let check_run_id = match self.job.progress {
            ProgressTarget::PullRequest { check_run_id, .. } => check_run_id,
            ProgressTarget::CommitCheck { check_run_id } => check_run_id,
        };
        let Some(check_run_id) = check_run_id else {
            return;
        };
        if let Err(e) = self
            .gh
            .update_check_run(
                self.job.installation_id,
                &self.job.repository,
                check_run_id,
                CheckRunUpdate {
                    state: CheckRunState::Completed(CheckRunConclusion::Neutral),
                    output,
                },
            )
            .await
        {
            tracing::warn!(job_id = %self.job.id, error = ?e, "update Check Run failed (non-fatal)");
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
