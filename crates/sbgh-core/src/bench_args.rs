//! Benchmark-argument resolution + the workload key.
//!
//! A job's *effective* `stacks-bench` workload args are resolved from two
//! sources: the per-job override stored in the queued event's provenance detail
//! (a `/benchmark` comment's tokens, or a baseline trigger policy's arg
//! string), falling back to the daemon's configured `default_args` when the
//! override is empty. The override REPLACES the default — there is no merge.
//!
//! The *workload key* is a stable hash of those effective args. It identifies
//! "the same benchmark workload" across jobs, so a PR run is only compared
//! against a baseline that measured the identical work (roadmap-v7). Structural
//! flags the daemon injects at run time (`--json`/`--db`/`bench
//! run`/`--source`/ `--dangerous-no-chainstate-copy`) are never part of the
//! stored args, so they never enter the key.
//!
//! Order- and whitespace-sensitive by current contract: it mirrors the bench
//! template's `read -r -a` word-splitting, so quoted/escaped values are not
//! supported (no current bench arg needs them).

use sha2::{Digest, Sha256};

use crate::models::QueuedEventDetail;

/// Effective workload args + their [`workload_key`], resolved together so call
/// sites don't recompute. `effective_args` are the canonical whitespace-split
/// tokens actually passed to `stacks-bench`; `workload_key` is their hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBenchArgs {
    pub effective_args: Vec<String>,
    pub workload_key: String,
}

/// Normalize a queued event's stored bench args to a token vec. `pr_comment`
/// carries a token vec directly; `branch_push`/`tag_created` carry a single
/// optional string split on whitespace; a `None` string yields no tokens;
/// `cache_warm` is a build-only job with no bench input → no tokens.
pub fn normalize_stored(detail: &QueuedEventDetail) -> Vec<String> {
    match detail {
        // `pr_comment` and `slack_adhoc` carry a token vec directly.
        QueuedEventDetail::PrComment { bench_args, .. }
        | QueuedEventDetail::SlackAdhoc { bench_args, .. } => bench_args.clone(),
        QueuedEventDetail::BranchPush { bench_args, .. }
        | QueuedEventDetail::TagCreated { bench_args, .. } => bench_args
            .as_deref()
            .map(|s| {
                s.split_whitespace()
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default(),
        // `cache_warm` is a build-only job — it runs no benchmark, so no args.
        QueuedEventDetail::CacheWarm { .. } => Vec::new(),
    }
}

/// As [`normalize_stored`], but from the raw JSONB `detail` value (the shape
/// persisted on the `queued` `job_event`). `None` distinguishes *unparseable*
/// detail from a parseable event that simply carries no args (`Some(vec![])`).
///
/// Use this where that distinction matters — notably the `workload_key`
/// backfill, where an unrecoverable historical row must get a NULL key (it must
/// NOT be falsely treated as the default workload). Runtime execution assembly
/// wants the infallible [`normalize_stored_value`] instead (a parse failure
/// there correctly falls back to `default_args`).
pub fn try_normalize_stored_value(detail: &serde_json::Value) -> Option<Vec<String>> {
    serde_json::from_value::<QueuedEventDetail>(detail.clone())
        .ok()
        .map(|d| normalize_stored(&d))
}

/// As [`try_normalize_stored_value`], but collapses unparseable detail to no
/// tokens — for the driver's runtime path, where that falls back to
/// `default_args`. Do NOT use for the backfill (see the fallible variant).
pub fn normalize_stored_value(detail: &serde_json::Value) -> Vec<String> {
    try_normalize_stored_value(detail).unwrap_or_default()
}

/// The string handed to `stacks-bench` (joined into the bench template). The
/// stored override REPLACES `default` when non-empty; an empty override falls
/// back to `default` verbatim.
pub fn effective_arg_string(stored: &[String], default: &str) -> String {
    if stored.is_empty() { default.to_string() } else { stored.join(" ") }
}

/// Stable key for "the same workload": lowercase-hex SHA-256 of the compact
/// JSON array of the effective arg tokens.
pub fn workload_key(effective_args: &[String]) -> String {
    let json =
        serde_json::to_string(effective_args).expect("Vec<String> always serializes to JSON");
    hex::encode(Sha256::digest(json.as_bytes()))
}

/// Resolve stored args + `default_args` into the effective tokens and their
/// [`workload_key`] in one pass.
pub fn resolve_bench_args(stored: &[String], default: &str) -> ResolvedBenchArgs {
    let effective_args: Vec<String> = effective_arg_string(stored, default)
        .split_whitespace()
        .map(String::from)
        .collect();
    let workload_key = workload_key(&effective_args);
    ResolvedBenchArgs { effective_args, workload_key }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT: &str =
        "--start-at 7800000 --count 5000 --warmup 1000 --no-profiler-kv --bench-spans-only";

    fn pr(args: &[&str]) -> QueuedEventDetail {
        QueuedEventDetail::PrComment {
            sender_id: 1,
            sender_login: "octocat".into(),
            comment_id: 7,
            pr_number: 42,
            subcommand: Some("run".into()),
            bench_args: args
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }

    fn branch(args: Option<&str>) -> QueuedEventDetail {
        QueuedEventDetail::BranchPush {
            branch: "develop".into(),
            trigger_id: 3,
            bench_args: args.map(String::from),
        }
    }

    fn tag(args: Option<&str>) -> QueuedEventDetail {
        QueuedEventDetail::TagCreated {
            tag: "v1".into(),
            trigger_id: 3,
            bench_args: args.map(String::from),
        }
    }

    fn slack(args: &[&str]) -> QueuedEventDetail {
        QueuedEventDetail::SlackAdhoc {
            channel: "C123".into(),
            message_ts: "1700000000.000100".into(),
            bench_args: args
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }

    fn cache_warm() -> QueuedEventDetail {
        QueuedEventDetail::CacheWarm {
            trigger_id: 3,
            git_ref: "release/3.2".into(),
            commit: "abc123".into(),
            build_target: crate::models::BuildTarget::StacksBench,
        }
    }

    #[test]
    fn normalize_cache_warm_has_no_args() {
        // A build-only warming job runs no benchmark → no tokens.
        assert!(normalize_stored(&cache_warm()).is_empty());
    }

    #[test]
    fn normalize_pr_comment_passes_tokens_through() {
        assert_eq!(normalize_stored(&pr(&["--count", "5000"])), vec!["--count", "5000"]);
        assert!(normalize_stored(&pr(&[])).is_empty());
    }

    #[test]
    fn normalize_slack_adhoc_passes_tokens_through() {
        assert_eq!(
            normalize_stored(&slack(&["--block", "184231", "--repetitions", "5"])),
            vec!["--block", "184231", "--repetitions", "5"]
        );
        assert!(normalize_stored(&slack(&[])).is_empty());
    }

    #[test]
    fn normalize_baseline_splits_string_or_empty() {
        assert_eq!(
            normalize_stored(&branch(Some("--start-at 100 --count 5"))),
            vec!["--start-at", "100", "--count", "5"]
        );
        assert!(normalize_stored(&branch(None)).is_empty());
        assert_eq!(normalize_stored(&tag(Some("--count 5"))), vec!["--count", "5"]);
    }

    #[test]
    fn normalize_value_round_trips_and_tolerates_garbage() {
        let v = serde_json::to_value(pr(&["--count", "5000"])).unwrap();
        assert_eq!(normalize_stored_value(&v), vec!["--count", "5000"]);
        assert!(normalize_stored_value(&serde_json::json!({ "nope": true })).is_empty());
    }

    /// The fallible variant separates "unparseable" (`None`, → NULL key in the
    /// backfill) from "parseable but argless" (`Some(vec![])`, a real bare
    /// run).
    #[test]
    fn try_normalize_distinguishes_unparseable_from_empty() {
        let garbage = serde_json::json!({ "nope": true });
        assert_eq!(try_normalize_stored_value(&garbage), None);

        let bare = serde_json::to_value(pr(&[])).unwrap();
        assert_eq!(try_normalize_stored_value(&bare), Some(vec![]));

        let args = serde_json::to_value(pr(&["--count", "5000"])).unwrap();
        assert_eq!(
            try_normalize_stored_value(&args),
            Some(vec!["--count".to_string(), "5000".to_string()])
        );
    }

    #[test]
    fn empty_override_falls_back_to_default() {
        assert_eq!(effective_arg_string(&[], DEFAULT), DEFAULT);
        assert_eq!(
            effective_arg_string(&["--count".into(), "5000".into()], DEFAULT),
            "--count 5000"
        );
    }

    #[test]
    fn key_is_compact_json_sha256() {
        let args = vec!["--count".to_string(), "5000".to_string()];
        let expected = hex::encode(Sha256::digest(r#"["--count","5000"]"#.as_bytes()));
        assert_eq!(workload_key(&args), expected);
        assert_eq!(workload_key(&args).len(), 64);
    }

    /// The load-bearing property (roadmap-v7): a bare `/benchmark` (empty
    /// override → default_args) and a baseline whose trigger args are NULL
    /// (also → default_args) resolve to the SAME key, so they compare.
    #[test]
    fn bare_pr_and_null_baseline_share_a_key() {
        let bare_pr = resolve_bench_args(&normalize_stored(&pr(&[])), DEFAULT);
        let null_baseline = resolve_bench_args(&normalize_stored(&branch(None)), DEFAULT);
        assert_eq!(bare_pr.workload_key, null_baseline.workload_key);
        assert_eq!(bare_pr.effective_args, null_baseline.effective_args);
    }

    /// A baseline configured with `default_args` verbatim also matches a bare
    /// PR.
    #[test]
    fn baseline_with_explicit_default_matches_bare_pr() {
        let bare_pr = resolve_bench_args(&normalize_stored(&pr(&[])), DEFAULT);
        let explicit = resolve_bench_args(&normalize_stored(&branch(Some(DEFAULT))), DEFAULT);
        assert_eq!(bare_pr.workload_key, explicit.workload_key);
    }

    /// A custom override yields a different key (→ no baseline match).
    #[test]
    fn custom_override_diverges() {
        let bare_pr = resolve_bench_args(&normalize_stored(&pr(&[])), DEFAULT);
        let custom = resolve_bench_args(&normalize_stored(&pr(&["--count", "9999"])), DEFAULT);
        assert_ne!(bare_pr.workload_key, custom.workload_key);
        assert_eq!(custom.effective_args, vec!["--count", "9999"]);
    }
}
