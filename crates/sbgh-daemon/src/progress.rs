//! Surfaces a job's lifecycle on its reporting target(s) — a PR comment
//! and/or a GitHub Check Run (roadmap-v4). The whole surface is **non-fatal**:
//! a reporting error is logged and swallowed, never failing the benchmark.

use std::path::Path;

use sbgh_core::github::{
    CheckRunConclusion, CheckRunOutput, CheckRunState, CheckRunUpdate, GitHubApi,
};

use crate::artifact_store::ArtifactStore;
use crate::bench_summary::{self, RunResult};
use crate::comparison::BaselineComparison;
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
    /// (it concludes `success`/`failure` only at a terminal state).
    pub async fn started(&self) {
        self.update_comment(&format!(
            ":rocket: benchmark `{id}` is running on commit `{sha}`.",
            id = self.job.id,
            sha = self.job.commit,
        ))
        .await;
    }

    pub async fn completed(
        &self,
        store: &dyn ArtifactStore,
        summary: &serde_json::Value,
        comparison: Option<&BaselineComparison>,
    ) {
        let body = self.completed_body(store, summary, comparison);
        self.update_comment(&body)
            .await;
        // Only PR jobs with a comment have one to point at; a baseline's commit
        // check is self-contained.
        let pointer = if self.has_comment() { "see comment / details" } else { "see details" };
        // The benchmark RAN and produced results → success. (Perf is data, not
        // a gate — a regression doesn't flip this.)
        self.complete_check(
            CheckRunConclusion::Success,
            CheckRunOutput {
                title: format!("benchmark {} — complete", self.job.id),
                summary: format!("commit `{}` — {pointer}", self.job.commit),
                text: Some(body),
            },
        )
        .await;
    }

    /// Whether this job has a PR comment surface (a `PullRequest` target whose
    /// comment was actually posted).
    fn has_comment(&self) -> bool {
        matches!(self.job.progress, ProgressTarget::PullRequest { comment_id: Some(_), .. })
    }

    /// Terminal failure. `error` is the full anyhow chain (internal paths,
    /// stderr) — surface only a short, sanitized snippet; the DB row keeps the
    /// full string. The check is concluded `failure` (the benchmark failed to
    /// run) — a red ✗ that's non-blocking because the check isn't required.
    pub async fn failed(&self, error: &str) {
        let snippet = short_pr_error(error);
        self.update_comment(&format!(
            ":x: benchmark `{id}` failed: `{snippet}`\n\n_(full details in the daemon logs)_",
            id = self.job.id,
        ))
        .await;
        self.complete_check(
            CheckRunConclusion::Failure,
            CheckRunOutput {
                title: format!("benchmark {} — failed", self.job.id),
                summary: format!("commit `{}` failed", self.job.commit),
                text: Some(format!("```\n{snippet}\n```\n\n_(full details in the daemon logs)_")),
            },
        )
        .await;
    }

    /// Terminal **cancellation** (roadmap-v5 Phase 4C): the run was
    /// deliberately stopped (operator shutdown/abort, or crash-orphan
    /// recovery), not a benchmark failure. Concludes the check `cancelled` —
    /// GitHub renders it neutral-gray, not a red ✗ — and notes it on the
    /// comment. `reason` is a short, already-safe phrase (no error chain).
    pub async fn cancelled(&self, reason: &str) {
        // The comment surface only exists for PR jobs (`update_comment` is a
        // no-op otherwise), so its `/benchmark` copy never reaches a baseline.
        self.update_comment(&format!(
            ":no_entry_sign: benchmark `{id}` cancelled: {reason}. Re-run with `/benchmark`.",
            id = self.job.id,
        ))
        .await;
        // The check exists for BOTH PR and baseline jobs, so its re-trigger hint
        // must match how each is actually re-run.
        let hint = self.retrigger_hint();
        self.complete_check(
            CheckRunConclusion::Cancelled,
            CheckRunOutput {
                title: format!("benchmark {} — cancelled", self.job.id),
                summary: format!("commit `{}` — {reason}", self.job.commit),
                text: Some(format!("Cancelled: {reason}. {hint}")),
            },
        )
        .await;
    }

    /// How this job is re-triggered, for the cancelled-check text. A PR job
    /// re-runs via the `/benchmark` command; a baseline (`branch_push` /
    /// `tag_created`) check re-runs when its ref is pushed again.
    fn retrigger_hint(&self) -> &'static str {
        match self.job.progress {
            ProgressTarget::PullRequest { .. } => "Re-run with `/benchmark`.",
            ProgressTarget::CommitCheck { .. } => "Re-run by pushing the branch/tag again.",
        }
    }

    /// The shared completed render (read + parse the archived `run.json` for
    /// the user-facing metrics) used by both the comment and the check.
    fn completed_body(
        &self,
        store: &dyn ArtifactStore,
        summary: &serde_json::Value,
        comparison: Option<&BaselineComparison>,
    ) -> String {
        let archive_dir = summary
            .get("archive_dir")
            .and_then(|v| v.as_str())
            .unwrap_or("/var/lib/sbgh/results");
        // Resolve the run.json store **key** (Decision 0002) → local path → parse.
        let parsed = summary
            .get("run_json_archived_path")
            .and_then(|v| v.as_str())
            .and_then(|key| store.get(key).ok())
            .and_then(|p| read_run_json(&p));
        bench_summary::render_pr_comment(
            &self.job.id.to_string(),
            &self.job.commit,
            archive_dir,
            parsed.as_ref(),
            comparison,
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

    /// Complete this job's Check Run with `conclusion` if it has one.
    /// Non-fatal.
    async fn complete_check(&self, conclusion: CheckRunConclusion, output: CheckRunOutput) {
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
                    state: CheckRunState::Completed(conclusion),
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
    use sbgh_core::github::test_support::{FakeCall, FakeGitHub};
    use sbgh_core::github::{CheckRunConclusion, CheckRunState};
    use sbgh_core::models::GitRefKind;
    use uuid::Uuid;

    use super::{ProgressReporter, short_pr_error};
    use crate::job_source::{ProgressTarget, RunnableJob};

    fn check_job(check_run_id: i64) -> RunnableJob {
        RunnableJob {
            id: Uuid::new_v4(),
            repository: "acme/widgets".into(),
            commit: "abc123".into(),
            git_ref_display: "develop".into(),
            git_ref_kind: GitRefKind::Branch,
            installation_id: 7,
            workload_key: None,
            bench_args: vec![],
            progress: ProgressTarget::CommitCheck {
                check_run_id: Some(check_run_id),
            },
            claim_token: Some(Uuid::new_v4()),
        }
    }

    fn concluded_state(gh: &FakeGitHub) -> Option<CheckRunState> {
        gh.calls()
            .into_iter()
            .find_map(|c| match c {
                FakeCall::UpdateCheckRun { state, .. } => Some(state),
                _ => None,
            })
    }

    /// A completed benchmark concludes the check `success` — it RAN.
    #[tokio::test]
    async fn completed_concludes_check_success() {
        let gh = FakeGitHub::new();
        let job = check_job(11);
        let store = crate::artifact_store::LocalFsStore::new(std::env::temp_dir());
        ProgressReporter::new(&gh, &job)
            .completed(&store, &serde_json::json!({}), None)
            .await;
        assert_eq!(
            concluded_state(&gh),
            Some(CheckRunState::Completed(CheckRunConclusion::Success))
        );
    }

    /// A failed benchmark concludes the check `failure` — it didn't run.
    #[tokio::test]
    async fn failed_concludes_check_failure() {
        let gh = FakeGitHub::new();
        let job = check_job(11);
        ProgressReporter::new(&gh, &job)
            .failed("boom: VM died")
            .await;
        assert_eq!(
            concluded_state(&gh),
            Some(CheckRunState::Completed(CheckRunConclusion::Failure))
        );
    }

    /// A cancelled run (Phase 4C: operator abort / crash-orphan) concludes the
    /// check `cancelled` (neutral-gray), NOT `failure` — it was deliberately
    /// stopped, not broken.
    #[tokio::test]
    async fn cancelled_concludes_check_cancelled() {
        let gh = FakeGitHub::new();
        let job = check_job(11);
        ProgressReporter::new(&gh, &job)
            .cancelled("aborted by shutdown")
            .await;
        assert_eq!(
            concluded_state(&gh),
            Some(CheckRunState::Completed(CheckRunConclusion::Cancelled))
        );
    }

    /// The check's re-trigger hint matches how the job actually re-runs: a
    /// baseline (`CommitCheck`) must NOT say `/benchmark` (that's PR-only) — it
    /// re-runs by pushing the ref; a PR job does say `/benchmark`.
    #[tokio::test]
    async fn cancelled_check_text_matches_retrigger_path() {
        let check_text = |gh: &FakeGitHub| -> String {
            gh.calls()
                .into_iter()
                .find_map(|c| match c {
                    FakeCall::UpdateCheckRun { output, .. } => output.text,
                    _ => None,
                })
                .expect("check concluded with text")
        };

        // Baseline commit check → push-the-ref, never `/benchmark`.
        let gh = FakeGitHub::new();
        ProgressReporter::new(&gh, &check_job(11))
            .cancelled("aborted by shutdown")
            .await;
        let baseline = check_text(&gh);
        assert!(!baseline.contains("/benchmark"), "baseline must not say /benchmark: {baseline}");
        assert!(baseline.contains("push"), "baseline hint mentions pushing: {baseline}");

        // PR job → `/benchmark`.
        let gh_pr = FakeGitHub::new();
        let pr = RunnableJob {
            progress: ProgressTarget::PullRequest {
                pr_number: 7,
                comment_id: None,
                check_run_id: Some(22),
                check_run_url: None,
            },
            ..check_job(22)
        };
        ProgressReporter::new(&gh_pr, &pr)
            .cancelled("aborted by shutdown")
            .await;
        assert!(check_text(&gh_pr).contains("/benchmark"), "PR job re-runs via /benchmark");
    }

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
