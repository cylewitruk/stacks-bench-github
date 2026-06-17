//! Defensive parser + Markdown renderer for the `--json` output of
//! `stacks-bench bench run`. The schema is owned upstream and will
//! drift; this module **intentionally** uses `Option<T>` on every field
//! with `#[serde(default)]` so a renamed or removed field disappears
//! from the PR comment rather than crashing the daemon. The
//! caller decides whether the resulting render is "rich enough"; if
//! every field round-trips as `None`, the caller falls back to a
//! generic "completed" comment.
//!
//! Mirrors the upstream Rust shape closely, except for the optional-everywhere
//! relaxation and a compatibility layer across the legacy `data` envelope and
//! the schema-v1 `result` envelope. Extra fields from the JSON are ignored
//! silently by serde's default behaviour.

use sbgh_core::db::BaselineSelection;
use sbgh_core::github::encode_ref_path;
use serde::Deserialize;

use crate::comparison::{BaselineComparison, Verdict};

/// Top-level envelope written by `stacks-bench bench run --json`. The
/// envelope's `duration_secs` is the full wall-clock time (setup + chainstate
/// copy avoidance + replay). Legacy binaries put bench-only metrics under
/// `data`; schema-v1 binaries put them under `result` and identify the payload
/// with `result_type = "run", result_version = 1`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RunResult {
    pub schema_version: Option<u32>,
    pub success: Option<bool>,
    pub result_type: Option<String>,
    pub result_version: Option<u32>,
    pub duration_secs: Option<f64>,
    /// Legacy pre-schema payload.
    pub data: Option<RunData>,
    /// Schema-v1 payload. Only interpreted when `(result_type, result_version)`
    /// identifies a stable `bench run` payload.
    pub result: Option<RunData>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RunData {
    pub run_id: Option<i64>,
    #[serde(alias = "entries")]
    pub blocks: Option<u64>,
    #[serde(alias = "warmup_entries")]
    pub warmup_blocks: Option<u64>,
    #[serde(alias = "measured_entries")]
    pub measured_blocks: Option<u64>,
    pub sampled_metric_rows: Option<u64>,
    /// Schema-v1 mode details. Kept raw for now; a follow-up will use it to
    /// render neutral entry/block/tx labels once the upstream schema lands.
    pub mode_summary: Option<serde_json::Value>,
    /// Replay-only duration (excludes setup). Distinct from envelope
    /// `duration_secs`.
    pub duration_secs: Option<f64>,
    pub interrupted: Option<bool>,
    pub summary: Option<RunSummary>,
    pub targets: Option<Vec<TargetSummary>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RunSummary {
    pub total_duration_us: Option<u64>,
    pub setup_duration_us: Option<u64>,
    pub execution_duration_us: Option<u64>,
    pub commit_duration_us: Option<u64>,
    pub transactions: Option<u64>,
    pub clarity_runtime: Option<u64>,
    pub write_length: Option<u64>,
    pub read_length: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TargetSummary {
    pub kind: Option<String>,
    pub identifier: Option<String>,
    pub parent_block: Option<u64>,
    pub warmup_count: Option<u32>,
    pub measured_count: Option<u32>,
    pub summary: Option<RunSummary>,
}

impl RunResult {
    /// Parse from the raw `run.json` byte slice (typically read off
    /// disk after the VM has shut down). Returns `None` and logs on
    /// any parse failure — caller decides whether to fall back.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match serde_json::from_slice::<Self>(bytes) {
            Ok(r) => Some(r),
            Err(e) => {
                tracing::warn!(error = %e, "failed to parse run.json; PR comment will fall back");
                None
            }
        }
    }

    /// The stable `bench run` payload, regardless of envelope generation.
    pub fn run_data(&self) -> Option<&RunData> {
        if self.schema_version.is_some() {
            match (self.result_type.as_deref(), self.result_version) {
                (Some("run"), Some(1)) => self.result.as_ref(),
                _ => None,
            }
        } else {
            self.data.as_ref()
        }
    }
}

/// Render the per-job PR comment for a successful run.
///
/// `archive_dir` is the per-job archive path on the host (the place
/// where the binary + appdata + run.json got copied). It's shown in
/// the collapsed "Investigate locally" block so operators can rerun
/// stacks-bench against the same DB without hunting for paths.
///
/// Returns Markdown ready to be passed straight to `update_pr_comment`.
pub fn render_pr_comment(
    job_id: &str,
    head_sha: &str,
    archive_dir: &str,
    result: Option<&RunResult>,
    comparison: Option<&BaselineComparison>,
) -> String {
    let mut out = String::new();
    out.push_str(":white_check_mark: benchmark `");
    out.push_str(job_id);
    out.push_str("` complete (commit `");
    out.push_str(head_sha);
    out.push_str("`)\n\n");

    // roadmap-v7: the vs-baseline headline goes first (most visible). Absent for
    // baseline jobs and PR runs with no comparable baseline — then the absolute
    // table below stands alone, exactly as before.
    if let Some(c) = comparison {
        out.push_str(&render_comparison(c));
        out.push('\n');
    }

    if let Some(r) = result {
        let table = metric_table(r);
        if !table.is_empty() {
            out.push_str(&table);
            out.push('\n');
        }
    } else {
        out.push_str(
            "_(run.json missing or unparseable — see archive for raw stacks-bench output)_\n\n",
        );
    }

    out.push_str("<details><summary>Investigate locally</summary>\n\n");
    out.push_str("```bash\n");
    out.push_str("cd ");
    out.push_str(archive_dir);
    out.push('\n');
    out.push_str("./stacks-bench --db . bench list\n");
    if let Some(rid) = result
        .and_then(|r| r.run_data())
        .and_then(|d| d.run_id)
    {
        out.push_str(&format!("./stacks-bench --db . bench summary {rid}\n"));
    }
    out.push_str("```\n\n");
    out.push_str("</details>");
    out
}

/// roadmap-v7: the vs-baseline headline + provenance line. Names the delta on
/// the combined Execution+Commit budget, the confidence, the linked baseline
/// commit, how it was chosen, and a cross-fork-safe diff link.
fn render_comparison(c: &BaselineComparison) -> String {
    let cmp = &c.comparison;
    let pr_secs = cmp.pr_combined_us as f64 / 1_000_000.0;

    let (arrow, dir) = if cmp.delta_pct > 0.05 {
        (":small_red_triangle:", "slower")
    } else if cmp.delta_pct < -0.05 {
        (":small_red_triangle_down:", "faster")
    } else {
        ("≈", "no change")
    };
    let confidence = match cmp.sigma {
        Some(z) => format!("≈{z:.1}σ — {}", verdict_phrase(cmp.verdict)),
        None => "confidence provisional — set `[reporting].noise_cv_pct`".to_string(),
    };

    let short = c
        .baseline_commit
        .get(..8)
        .unwrap_or(&c.baseline_commit);
    let commit_url = format!("https://github.com/{}/commit/{}", c.baseline_repo, c.baseline_commit);
    // Encode the head ref's URL-unsafe chars (preserving `/`) the same way the
    // API client does — a branch with `#`/`%`/`?`/spaces would otherwise break
    // the diff link (Codex Phase-3 finding).
    let compare_url = format!(
        "https://github.com/{}/compare/{}...{}:{}",
        c.base_repo,
        c.baseline_commit,
        c.head_owner,
        encode_ref_path(&c.head_ref)
    );
    let selection = match c.selection {
        BaselineSelection::Exact => "exact fork-point".to_string(),
        BaselineSelection::NearestBefore => "nearest baseline before fork-point".to_string(),
    };
    let committed = c
        .baseline_committed_at
        .map(|d| format!(" · committed {}", d.format("%Y-%m-%d")))
        .unwrap_or_default();

    format!(
        "**Execution+Commit** {pr_secs:.1}s · {arrow} {:.1}% {dir} ({confidence})\nCompared \
         against [`{}@{short}`]({commit_url}) on `{}`{committed} · {selection} · \
         [diff]({compare_url})\n",
        cmp.delta_pct.abs(),
        c.baseline_repo,
        c.baseline_ref,
    )
}

fn verdict_phrase(v: Verdict) -> &'static str {
    match v {
        Verdict::Strong => "likely real",
        Verdict::Moderate => "moderate",
        Verdict::Weak => "weak signal",
        Verdict::Inconclusive => "within noise",
        Verdict::Provisional => "provisional",
    }
}

/// Human-readable form of the db link TTL, kept in sync with
/// `progress::DB_LINK_TTL` so the card's "expires in …" copy matches the signed
/// URL's lifetime.
pub const DB_LINK_TTL_HUMAN: &str = "3 days";

/// One Slack `plan` task row's status (the live timeline's per-row state).
/// Mapped to `slack_messaging`'s `TaskStatus` by [`crate::slack::card`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanTaskStatus {
    Pending,
    InProgress,
    Complete,
    Error,
}

/// Build the "Metric | Value" GitHub-flavoured Markdown table from
/// [`metric_rows`]. Returns an empty string if no metrics were present (caller
/// renders the fallback message). Shared by the PR comment and the v8 Slack
/// `markdown` results block ([`crate::slack::card`]).
pub fn metric_table(r: &RunResult) -> String {
    let rows = metric_rows(r);
    if rows.is_empty() {
        return String::new();
    }
    let mut out = String::from("| Metric | Value |\n| ---- | ---- |\n");
    for (k, v) in rows {
        out.push_str(&format!("| {k} | {v} |\n"));
    }
    out
}

/// Extract the user-facing `(label, value)` metric rows shared by every
/// renderer (GitHub table + Slack list). Skips any row whose source field is
/// `None`; an all-`None` result yields an empty `Vec` (caller renders a
/// fallback).
fn metric_rows(r: &RunResult) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = Vec::new();

    let data = r.run_data();
    if r.schema_version.is_some() && data.is_none() {
        return rows;
    }

    if let Some(m) = data.and_then(|d| d.measured_blocks) {
        let warmup = data
            .and_then(|d| d.warmup_blocks)
            .unwrap_or(0);
        rows.push((
            "Blocks measured".to_string(),
            format!("{} ({} warmup)", thousands(m), thousands(warmup)),
        ));
    }
    if let Some(d) = r.duration_secs {
        rows.push(("Run duration".to_string(), format_duration(d)));
    }
    if let Some(d) = data.and_then(|d| d.duration_secs) {
        rows.push(("Replay duration".to_string(), format_duration(d)));
    }

    if let Some(s) = data.and_then(|d| d.summary.as_ref()) {
        if let Some(tx) = s.transactions {
            rows.push(("Transactions replayed".to_string(), thousands(tx)));
        }

        // Per-block averages — only meaningful if we have a divisor.
        let divisor = data
            .and_then(|d| d.measured_blocks)
            .filter(|m| *m > 0);
        if let Some(setup) = s.setup_duration_us {
            rows.push(("Setup".to_string(), avg_us_per(setup, divisor)));
        }
        if let Some(exec) = s.execution_duration_us {
            rows.push(("Execution".to_string(), avg_us_per(exec, divisor)));
        }
        if let Some(commit) = s.commit_duration_us {
            rows.push(("Commit".to_string(), avg_us_per(commit, divisor)));
        }
        if let Some(cr) = s.clarity_runtime {
            rows.push(("Clarity runtime (total cost units)".to_string(), thousands(cr)));
        }
        if let (Some(read), Some(write)) = (s.read_length, s.write_length) {
            rows.push((
                "Read / write length".to_string(),
                format!("{} / {}", human_bytes(read), human_bytes(write)),
            ));
        } else if let Some(read) = s.read_length {
            rows.push(("Read length".to_string(), human_bytes(read)));
        } else if let Some(write) = s.write_length {
            rows.push(("Write length".to_string(), human_bytes(write)));
        }
    }

    if data.and_then(|d| d.interrupted) == Some(true) {
        rows.push(("Interrupted".to_string(), "**yes** (partial measurements)".to_string()));
    }

    rows
}

fn avg_us_per(total_us: u64, divisor: Option<u64>) -> String {
    match divisor {
        Some(d) => format!("{} µs avg ({} µs total)", thousands(total_us / d), thousands(total_us)),
        None => format!("{} µs (total)", thousands(total_us)),
    }
}

/// 1234567 → "1,234,567". Reads better in a results table than the
/// raw u64. Comma-grouping by reversing the digits, inserting commas
/// every 3, then reversing back — straightforward, no off-by-one
/// arithmetic to second-guess.
fn thousands<N: ToString>(n: N) -> String {
    let s = n.to_string();
    let (sign, digits) = match s.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", s.as_str()),
    };
    let reversed_grouped: String = digits
        .chars()
        .rev()
        .enumerate()
        .flat_map(|(i, c)| if i > 0 && i % 3 == 0 { vec![',', c] } else { vec![c] })
        .collect();
    let mut out = String::with_capacity(reversed_grouped.len() + sign.len());
    out.push_str(sign);
    out.extend(reversed_grouped.chars().rev());
    out
}

fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut idx = 0;
    while v >= 1024.0 && idx + 1 < UNITS.len() {
        v /= 1024.0;
        idx += 1;
    }
    if idx == 0 { format!("{} {}", b, UNITS[idx]) } else { format!("{:.1} {}", v, UNITS[idx]) }
}

fn format_duration(secs: f64) -> String {
    if !secs.is_finite() || secs < 0.0 {
        return format!("{secs}s");
    }
    let total = secs as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h > 0 {
        format!("{h}h {m}m {s}s")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{:.2}s", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_payload() {
        // Mirrors the real envelope shape: top-level success+duration_secs,
        // detail under `data`.
        let json = br#"{
            "success": true,
            "duration_secs": 2203.8,
            "data": {
                "run_id": 42,
                "blocks": 5000,
                "warmup_blocks": 1000,
                "measured_blocks": 4000,
                "duration_secs": 1964.9,
                "interrupted": false,
                "summary": {
                    "total_duration_us": 100000000,
                    "setup_duration_us": 9380000,
                    "execution_duration_us": 72940000,
                    "commit_duration_us": 18280000,
                    "transactions": 12345,
                    "clarity_runtime": 9876543210,
                    "write_length": 89000000,
                    "read_length": 245000000
                }
            }
        }"#;
        let r = RunResult::from_bytes(json).unwrap();
        assert_eq!(r.success, Some(true));
        assert_eq!(r.duration_secs, Some(2203.8));
        let d = r.run_data().unwrap();
        assert_eq!(d.run_id, Some(42));
        assert_eq!(d.measured_blocks, Some(4000));
        let s = d.summary.as_ref().unwrap();
        assert_eq!(s.transactions, Some(12345));
    }

    #[test]
    fn parse_schema_v1_run_payload() {
        let json = br#"{
            "schema_version": 1,
            "success": true,
            "result_type": "run",
            "result_version": 1,
            "duration_secs": 2203.8,
            "result": {
                "run_id": 42,
                "entries": 5000,
                "warmup_entries": 1000,
                "measured_entries": 4000,
                "sampled_metric_rows": 4000,
                "mode_summary": {
                    "mode": "range",
                    "sample_unit": "entry"
                },
                "duration_secs": 1964.9,
                "interrupted": false,
                "summary": {
                    "total_duration_us": 100000000,
                    "setup_duration_us": 9380000,
                    "execution_duration_us": 72940000,
                    "commit_duration_us": 18280000,
                    "transactions": 12345,
                    "clarity_runtime": 9876543210,
                    "write_length": 89000000,
                    "read_length": 245000000
                }
            }
        }"#;
        let r = RunResult::from_bytes(json).unwrap();
        assert_eq!(r.schema_version, Some(1));
        assert_eq!(r.result_type.as_deref(), Some("run"));
        assert_eq!(r.result_version, Some(1));
        let d = r.run_data().unwrap();
        assert_eq!(d.run_id, Some(42));
        assert_eq!(d.blocks, Some(5000));
        assert_eq!(d.measured_blocks, Some(4000));
        assert_eq!(d.warmup_blocks, Some(1000));
        assert_eq!(d.sampled_metric_rows, Some(4000));
        assert!(d.mode_summary.is_some());
    }

    #[test]
    fn parse_schema_v1_error_envelope_without_run_payload() {
        let json = br#"{
            "schema_version": 1,
            "success": false,
            "result_type": "error",
            "result_version": 0,
            "duration_secs": 1.23,
            "error": "bad args"
        }"#;
        let r = RunResult::from_bytes(json).unwrap();
        assert_eq!(r.success, Some(false));
        assert_eq!(r.error.as_deref(), Some("bad args"));
        assert!(r.run_data().is_none());
        assert!(metric_table(&r).is_empty());
    }

    #[test]
    fn unsupported_schema_v1_result_type_has_no_rich_metrics() {
        let json = br#"{
            "schema_version": 1,
            "success": true,
            "result_type": "run_show",
            "result_version": 1,
            "duration_secs": 1.23,
            "result": { "run_id": 1, "measured_entries": 10 }
        }"#;
        let r = RunResult::from_bytes(json).unwrap();
        assert!(r.run_data().is_none());
        assert!(metric_table(&r).is_empty());
    }

    #[test]
    fn parse_minimal_payload_keeps_unknown_fields_none() {
        let json = br#"{ "data": { "run_id": 1 } }"#;
        let r = RunResult::from_bytes(json).unwrap();
        let d = r.run_data().unwrap();
        assert_eq!(d.run_id, Some(1));
        assert!(d.summary.is_none());
        assert!(d.measured_blocks.is_none());
    }

    #[test]
    fn parse_ignores_unknown_fields_for_forward_compat() {
        let json = br#"{
            "future_envelope_field": true,
            "data": { "run_id": 7, "future_field": "we_dont_know_yet", "summary": { "transactions": 9 } }
        }"#;
        let r = RunResult::from_bytes(json).unwrap();
        let d = r.run_data().unwrap();
        assert_eq!(d.run_id, Some(7));
        assert_eq!(
            d.summary
                .as_ref()
                .unwrap()
                .transactions,
            Some(9)
        );
    }

    #[test]
    fn parse_garbage_returns_none() {
        assert!(RunResult::from_bytes(b"not json at all").is_none());
        assert!(RunResult::from_bytes(b"").is_none());
    }

    #[test]
    fn render_includes_table_when_metrics_present() {
        let r = RunResult {
            success: Some(true),
            duration_secs: Some(2203.8),
            data: Some(RunData {
                run_id: Some(42),
                measured_blocks: Some(5000),
                warmup_blocks: Some(1000),
                duration_secs: Some(1964.9),
                summary: Some(RunSummary {
                    transactions: Some(12345),
                    setup_duration_us: Some(9_380_000),
                    execution_duration_us: Some(72_940_000),
                    commit_duration_us: Some(18_280_000),
                    read_length: Some(245_000_000),
                    write_length: Some(89_000_000),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let md =
            render_pr_comment("job-1", "abc123", "/var/lib/sbgh/results/job-1", Some(&r), None);

        assert!(md.contains("benchmark `job-1`"));
        assert!(md.contains("(commit `abc123`)"));
        assert!(md.contains("| Metric | Value |"));
        assert!(md.contains("5,000 (1,000 warmup)"));
        assert!(md.contains("Run duration"));
        assert!(md.contains("Replay duration"));
        assert!(md.contains("12,345"));
        assert!(md.contains("µs avg"));
        // Investigate-locally block always included; bench summary uses run_id when
        // present.
        assert!(md.contains("cd /var/lib/sbgh/results/job-1"));
        assert!(md.contains("bench list"));
        assert!(md.contains("bench summary 42"));
    }

    #[test]
    fn render_falls_back_when_no_run_json() {
        let md = render_pr_comment("job-x", "deadbeef", "/some/path", None, None);
        assert!(md.contains("benchmark `job-x`"));
        assert!(md.contains("(commit `deadbeef`)"));
        assert!(md.contains("run.json missing"));
        // No bench summary line because we have no run_id.
        assert!(!md.contains("bench summary"));
    }

    #[test]
    fn render_omits_table_when_only_unknown_fields_parsed() {
        // RunResult with everything None — table should be empty,
        // fallback message kicks in.
        let r = RunResult::default();
        let md = render_pr_comment("j", "h", "/p", Some(&r), None);
        // No "| Metric | Value |" but also no "missing" message
        // (we DID parse, the result is just empty).
        assert!(!md.contains("| Metric"));
        assert!(!md.contains("missing"));
    }

    #[test]
    fn render_marks_interrupted_runs() {
        let r = RunResult {
            data: Some(RunData {
                interrupted: Some(true),
                measured_blocks: Some(100),
                ..Default::default()
            }),
            ..Default::default()
        };
        let md = render_pr_comment("j", "h", "/p", Some(&r), None);
        assert!(md.contains("Interrupted"));
        assert!(md.contains("partial measurements"));
    }

    // ─── roadmap-v7: vs-baseline headline render ───

    use crate::comparison::Comparison;

    fn sample_comparison() -> BaselineComparison {
        BaselineComparison {
            comparison: Comparison {
                base_combined_us: 1_000_000,
                pr_combined_us: 1_018_000,
                delta_pct: 1.8,
                sigma: Some(3.44),
                verdict: Verdict::Strong,
            },
            baseline_repo: "stacks-network/stacks-core".into(),
            baseline_commit: "abc1234def567".into(),
            baseline_ref: "develop".into(),
            baseline_committed_at: chrono::DateTime::from_timestamp(1_716_000_000, 0),
            selection: BaselineSelection::NearestBefore,
            base_repo: "cylewitruk/stacks-core".into(),
            head_owner: "cylewitruk".into(),
            head_ref: "feat/foo".into(),
        }
    }

    #[test]
    fn render_includes_vs_baseline_headline() {
        let c = sample_comparison();
        let md = render_pr_comment("j", "h", "/p", None, Some(&c));
        // Headline: combined metric, signed delta, sigma + verdict.
        assert!(md.contains("**Execution+Commit** 1.0s"), "{md}");
        assert!(md.contains("1.8% slower"), "{md}");
        assert!(md.contains("≈3.4σ — likely real"), "{md}");
        // Provenance: linked baseline commit (repo@short8), ref, selection, date.
        assert!(md.contains("[`stacks-network/stacks-core@abc1234d`]"), "{md}");
        assert!(md.contains("https://github.com/stacks-network/stacks-core/commit/abc1234def567"));
        assert!(md.contains("on `develop`"), "{md}");
        assert!(md.contains("committed 2024-05-18"), "{md}");
        assert!(md.contains("nearest baseline before fork-point"), "{md}");
        // Cross-fork-safe diff link on the PR's base repo.
        assert!(
            md.contains(
                "https://github.com/cylewitruk/stacks-core/compare/abc1234def567...cylewitruk:feat/foo"
            ),
            "{md}"
        );
    }

    #[test]
    fn render_no_comparison_has_no_headline() {
        let md = render_pr_comment("j", "h", "/p", None, None);
        assert!(!md.contains("Execution+Commit"), "no baseline → no vs-baseline headline");
    }

    #[test]
    fn render_provisional_when_no_sigma() {
        let mut c = sample_comparison();
        c.comparison.sigma = None;
        c.comparison.verdict = Verdict::Provisional;
        let md = render_pr_comment("j", "h", "/p", None, Some(&c));
        assert!(md.contains("confidence provisional"), "{md}");
        assert!(!md.contains('σ'), "no sigma figure when provisional: {md}");
    }

    #[test]
    fn render_encodes_head_ref_in_diff_link() {
        // A head branch with URL-significant chars must be encoded (Codex
        // Phase-3): `/` preserved, space → %20, `#` → %23.
        let mut c = sample_comparison();
        c.head_ref = "feat/a b#c".into();
        let md = render_pr_comment("j", "h", "/p", None, Some(&c));
        assert!(md.contains("...cylewitruk:feat/a%20b%23c"), "{md}");
    }

    // ─── helpers ───

    #[test]
    fn thousands_formats_correctly() {
        assert_eq!(thousands(0_u64), "0");
        assert_eq!(thousands(42_u64), "42");
        assert_eq!(thousands(1_000_u64), "1,000");
        assert_eq!(thousands(1_234_567_u64), "1,234,567");
        assert_eq!(thousands(12_345_678_u64), "12,345,678");
    }

    #[test]
    fn human_bytes_picks_unit() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(2 * 1024 * 1024), "2.0 MiB");
        assert_eq!(human_bytes(3_500_000_000), "3.3 GiB");
    }

    #[test]
    fn format_duration_picks_granularity() {
        assert_eq!(format_duration(0.5), "0.50s");
        assert_eq!(format_duration(45.0), "45.00s");
        assert_eq!(format_duration(125.0), "2m 5s");
        assert_eq!(format_duration(3725.0), "1h 2m 5s");
    }
}
