//! Pure benchmark comparison + confidence.
//!
//! Turns a PR run's metric + a baseline's metric into a delta on the combined
//! **Execution+Commit** budget, plus a confidence read expressed in sigmas
//! against the measured noise floor. Pure and side-effect-free; reporters
//! consume these values to render headlines, summaries, and verdicts.
//!
//! Why Execution+Commit and not wall-clock: the two buckets vary inversely
//! run-to-run but their **sum is conserved** at a low CV, while envelope
//! wall-clock carries VM-boot / archive / teardown noise unrelated to the
//! benched code (see the variance study + roadmap-v7).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sbgh_core::db::{BaselineSelection, BenchmarkRunMetric};
use sbgh_core::models::{JobMetric, SubmissionSpec};
use uuid::Uuid;

/// Confidence verdict for a delta, derived from how many sigmas it is against
/// the noise floor. `Provisional` means the noise floor isn't configured yet
/// (`noise_cv_pct` unset) — the delta is shown, the confidence isn't.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// `< 1σ` — within the noise; no signal.
    Inconclusive,
    /// `1–2σ` — weak signal.
    Weak,
    /// `2–3σ` — moderate.
    Moderate,
    /// `≥ 3σ` — strong (likely real).
    Strong,
    /// No `noise_cv_pct` configured — sigma unknown, delta only.
    Provisional,
}

/// The result of comparing a PR run against a baseline. Durations are the
/// combined Execution+Commit **totals** over the measured blocks (µs);
/// `delta_pct` is signed (`+` = slower than baseline). `sigma` is `None` when
/// the noise floor isn't configured.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Comparison {
    pub base_combined_us: i64,
    pub pr_combined_us: i64,
    pub delta_pct: f64,
    pub sigma: Option<f64>,
    pub verdict: Verdict,
}

impl Comparison {
    /// Combined Execution+Commit total of a metric (µs).
    fn combined(m: &JobMetric) -> i64 {
        m.execution_duration_us + m.commit_duration_us
    }
}

/// Compare a PR run's metric against a baseline's, on the combined
/// Execution+Commit budget. `noise_cv_pct` is the measured per-run coefficient
/// of variation **as a percent** (e.g. `0.37`); `None`/non-positive yields a
/// [`Verdict::Provisional`] (delta without a sigma).
///
/// Returns `None` (incomparable) when the two runs measured a different
/// workload — a defensive guard on `measured_blocks`/`warmup_blocks`, since the
/// caller already filtered by `workload_key` — or when the baseline's combined
/// total is non-positive (a degenerate metric we can't take a ratio of).
pub fn compare(pr: &JobMetric, base: &JobMetric, noise_cv_pct: Option<f64>) -> Option<Comparison> {
    // Belt-and-suspenders: the baseline lookup already matched on `workload_key`,
    // but a delta is only meaningful over the same measured blocks.
    if pr.measured_blocks != base.measured_blocks || pr.warmup_blocks != base.warmup_blocks {
        return None;
    }

    let base_combined = Comparison::combined(base);
    let pr_combined = Comparison::combined(pr);
    if base_combined <= 0 {
        return None;
    }

    let delta_pct = (pr_combined - base_combined) as f64 / base_combined as f64 * 100.0;

    let (sigma, verdict) = match noise_cv_pct {
        Some(cv) if cv > 0.0 => {
            // The difference of two independent single runs has √2× the
            // relative noise of one run, so 1σ on the delta is √2·CV.
            let sigma_diff = std::f64::consts::SQRT_2 * cv;
            let z = delta_pct.abs() / sigma_diff;
            (Some(z), verdict_for(z))
        }
        _ => (None, Verdict::Provisional),
    };

    Some(Comparison {
        base_combined_us: base_combined,
        pr_combined_us: pr_combined,
        delta_pct,
        sigma,
        verdict,
    })
}

/// Per-variant run aggregate used by multi-variant comparisons.
#[derive(Debug, Clone, PartialEq)]
pub struct VariantRunStats {
    pub requested: i32,
    pub completed: usize,
    pub combined_mean_us: Option<f64>,
    pub combined_min_us: Option<i64>,
    pub combined_max_us: Option<i64>,
    pub combined_stddev_us: Option<f64>,
    pub combined_cv_pct: Option<f64>,
    pub workload: Option<WorkloadShape>,
    pub workload_consistent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadShape {
    pub measured_blocks: i64,
    pub warmup_blocks: i64,
}

/// One spec/variant in a multi-variant comparison.
#[derive(Debug, Clone, PartialEq)]
pub struct ComparisonVariant {
    pub task_spec_id: Uuid,
    pub spec_index: i32,
    pub ref_display: String,
    pub commit: Option<String>,
    pub baseline_calibration_id: Option<i64>,
    pub stats: VariantRunStats,
}

/// Why a variant could not be compared against the baseline variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonUnavailable {
    MissingBaselineMetric,
    MissingVariantMetric,
    IncomparableWorkload,
    DegenerateBaseline,
}

/// One variant's delta against the submission's baseline variant (spec 0).
#[derive(Debug, Clone, PartialEq)]
pub struct VariantDelta {
    pub variant: ComparisonVariant,
    pub comparison: Option<Comparison>,
    pub unavailable: Option<ComparisonUnavailable>,
}

/// Multi-variant comparison: one baseline variant plus N deltas against it.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiVariantComparison {
    pub baseline: ComparisonVariant,
    pub variants: Vec<VariantDelta>,
}

impl MultiVariantComparison {
    pub fn from_specs(
        specs: &[SubmissionSpec],
        metrics_by_spec: &HashMap<Uuid, Vec<BenchmarkRunMetric>>,
        noise_cv_pct: Option<f64>,
    ) -> Option<Self> {
        let mut specs = specs.to_vec();
        specs.sort_by_key(|spec| spec.spec_index);
        let (baseline_spec, variant_specs) = specs.split_first()?;
        if variant_specs.is_empty() {
            return None;
        }

        let baseline = variant_from_spec(baseline_spec, metrics_by_spec);
        let variants = variant_specs
            .iter()
            .map(|spec| {
                let variant = variant_from_spec(spec, metrics_by_spec);
                let (comparison, unavailable) =
                    compare_variant(&baseline.stats, &variant.stats, noise_cv_pct);
                VariantDelta {
                    variant,
                    comparison,
                    unavailable,
                }
            })
            .collect();

        Some(Self { baseline, variants })
    }
}

fn variant_from_spec(
    spec: &SubmissionSpec,
    metrics_by_spec: &HashMap<Uuid, Vec<BenchmarkRunMetric>>,
) -> ComparisonVariant {
    let metrics = metrics_by_spec
        .get(&spec.id)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    ComparisonVariant {
        task_spec_id: spec.id,
        spec_index: spec.spec_index,
        ref_display: spec.git_ref_display.clone(),
        commit: spec.git_commit_hash.clone(),
        baseline_calibration_id: spec.baseline_calibration_id,
        stats: VariantRunStats::from_metrics(spec.requested_run_count, metrics),
    }
}

impl VariantRunStats {
    pub fn from_metrics(requested: i32, runs: &[BenchmarkRunMetric]) -> Self {
        let mut values = Vec::with_capacity(runs.len());
        let mut workload = None;
        let mut workload_consistent = true;

        for run in runs {
            values.push(Comparison::combined(&run.metric));
            let shape = WorkloadShape {
                measured_blocks: run.metric.measured_blocks,
                warmup_blocks: run.metric.warmup_blocks,
            };
            match workload {
                Some(existing) if existing != shape => workload_consistent = false,
                None => workload = Some(shape),
                _ => {}
            }
        }

        let aggregate = aggregate_values(&values);
        Self {
            requested,
            completed: runs.len(),
            combined_mean_us: aggregate
                .as_ref()
                .map(|stats| stats.mean_us),
            combined_min_us: aggregate
                .as_ref()
                .map(|stats| stats.min_us),
            combined_max_us: aggregate
                .as_ref()
                .map(|stats| stats.max_us),
            combined_stddev_us: aggregate
                .as_ref()
                .map(|stats| stats.stddev_us),
            combined_cv_pct: aggregate
                .as_ref()
                .map(|stats| stats.cv_pct),
            workload,
            workload_consistent,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AggregateStats {
    min_us: i64,
    max_us: i64,
    mean_us: f64,
    stddev_us: f64,
    cv_pct: f64,
}

fn aggregate_values(values: &[i64]) -> Option<AggregateStats> {
    let first = *values.first()?;
    let (mut min_us, mut max_us) = (first, first);
    let mut sum = 0.0;
    for &value in values {
        min_us = min_us.min(value);
        max_us = max_us.max(value);
        sum += value as f64;
    }
    let count = values.len() as f64;
    let mean_us = sum / count;
    let variance = if values.len() > 1 {
        values
            .iter()
            .map(|value| {
                let delta = *value as f64 - mean_us;
                delta * delta
            })
            .sum::<f64>()
            / (count - 1.0)
    } else {
        0.0
    };
    let stddev_us = variance.sqrt();
    let cv_pct = if mean_us > 0.0 { stddev_us / mean_us * 100.0 } else { 0.0 };
    Some(AggregateStats {
        min_us,
        max_us,
        mean_us,
        stddev_us,
        cv_pct,
    })
}

fn compare_variant(
    baseline: &VariantRunStats,
    variant: &VariantRunStats,
    noise_cv_pct: Option<f64>,
) -> (Option<Comparison>, Option<ComparisonUnavailable>) {
    let Some(base_combined) = baseline.combined_mean_us else {
        return (None, Some(ComparisonUnavailable::MissingBaselineMetric));
    };
    let Some(variant_combined) = variant.combined_mean_us else {
        return (None, Some(ComparisonUnavailable::MissingVariantMetric));
    };
    let comparable_workload = baseline.workload_consistent
        && variant.workload_consistent
        && baseline.workload.is_some()
        && baseline.workload == variant.workload;
    if !comparable_workload {
        return (None, Some(ComparisonUnavailable::IncomparableWorkload));
    }
    if base_combined <= 0.0 {
        return (None, Some(ComparisonUnavailable::DegenerateBaseline));
    }

    let delta_pct = (variant_combined - base_combined) / base_combined * 100.0;
    let (sigma, verdict) =
        comparison_confidence(baseline, variant, base_combined, delta_pct, noise_cv_pct);

    (
        Some(Comparison {
            base_combined_us: base_combined.round() as i64,
            pr_combined_us: variant_combined.round() as i64,
            delta_pct,
            sigma,
            verdict,
        }),
        None,
    )
}

fn comparison_confidence(
    baseline: &VariantRunStats,
    variant: &VariantRunStats,
    base_combined: f64,
    delta_pct: f64,
    noise_cv_pct: Option<f64>,
) -> (Option<f64>, Verdict) {
    if baseline.completed > 1 && variant.completed > 1 {
        let base_stddev = baseline
            .combined_stddev_us
            .unwrap_or(0.0);
        let variant_stddev = variant
            .combined_stddev_us
            .unwrap_or(0.0);
        let standard_error_us = ((base_stddev * base_stddev / baseline.completed as f64)
            + (variant_stddev * variant_stddev / variant.completed as f64))
            .sqrt();
        if standard_error_us > 0.0 {
            let standard_error_pct = standard_error_us / base_combined * 100.0;
            let sigma = delta_pct.abs() / standard_error_pct;
            return (Some(sigma), verdict_for(sigma));
        }
        let sigma = if delta_pct == 0.0 { 0.0 } else { f64::INFINITY };
        return (Some(sigma), verdict_for(sigma));
    }

    match noise_cv_pct {
        Some(cv) if cv > 0.0 => {
            let sigma_diff = std::f64::consts::SQRT_2 * cv;
            let sigma = delta_pct.abs() / sigma_diff;
            (Some(sigma), verdict_for(sigma))
        }
        _ => (None, Verdict::Provisional),
    }
}

/// A [`Comparison`] plus the provenance the report renders: which baseline it
/// cites (repo/commit/ref/date/selection) and the PR head identity for the
/// cross-fork-safe diff link. Built by the reporter (Phase 3), consumed by
/// `bench_summary`.
#[derive(Debug, Clone)]
pub struct BaselineComparison {
    pub comparison: Comparison,
    /// Baseline repo `owner/name` — may differ from the PR's (fork PR vs.
    /// upstream baseline); used for the commit link.
    pub baseline_repo: String,
    pub baseline_commit: String,
    pub baseline_ref: String,
    pub baseline_committed_at: Option<DateTime<Utc>>,
    pub selection: BaselineSelection,
    /// The PR's base repo `owner/name` + head identity, for the
    /// `…/compare/<baseline>...<head_owner>:<head_ref>` diff link.
    pub base_repo: String,
    pub head_owner: String,
    pub head_ref: String,
}

fn verdict_for(z: f64) -> Verdict {
    if z < 1.0 {
        Verdict::Inconclusive
    } else if z < 2.0 {
        Verdict::Weak
    } else if z < 3.0 {
        Verdict::Moderate
    } else {
        Verdict::Strong
    }
}

#[cfg(test)]
mod tests {
    use sbgh_core::models::{BuildTarget, GitRefKind, TaskKind};

    use super::*;

    /// Build a metric with the given Execution+Commit split + block counts;
    /// other fields are irrelevant to `compare`.
    fn metric(exec_us: i64, commit_us: i64, measured: i64, warmup: i64) -> JobMetric {
        JobMetric {
            job_id: uuid::Uuid::nil(),
            envelope_duration_us: 0,
            replay_duration_us: 0,
            total_duration_us: 0,
            setup_duration_us: 0,
            execution_duration_us: exec_us,
            commit_duration_us: commit_us,
            clarity_runtime: 0,
            transactions: 0,
            read_length: 0,
            write_length: 0,
            measured_blocks: measured,
            warmup_blocks: warmup,
            created_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        }
    }

    fn spec(index: i32, rev: &str, requested: i32, calibration_id: Option<i64>) -> SubmissionSpec {
        SubmissionSpec {
            id: uuid::Uuid::from_u128((index + 1) as u128),
            task_submission_id: uuid::Uuid::from_u128(99),
            spec_index: index,
            requested_run_count: requested,
            baseline_calibration_id: calibration_id,
            github_repo_id: 10,
            task_kind: TaskKind::Benchmark,
            build_target: BuildTarget::StacksBench,
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: rev.into(),
            git_commit_hash: Some(format!("{rev}-sha")),
            git_committed_at: None,
            workload_key: Some("wk".into()),
            created_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            updated_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        }
    }

    fn run(index: i32, metric: JobMetric) -> BenchmarkRunMetric {
        BenchmarkRunMetric { task_run_index: index, metric }
    }

    fn metrics(entries: impl IntoIterator<Item = (i32, JobMetric)>) -> Vec<BenchmarkRunMetric> {
        entries
            .into_iter()
            .map(|(index, metric)| run(index, metric))
            .collect()
    }

    fn metric_map(
        entries: impl IntoIterator<Item = (uuid::Uuid, Vec<BenchmarkRunMetric>)>,
    ) -> HashMap<uuid::Uuid, Vec<BenchmarkRunMetric>> {
        entries.into_iter().collect()
    }

    #[test]
    fn slower_run_is_positive_delta_with_sigma_and_verdict() {
        // base 1_000_000µs, pr 1_018_000µs → +1.8%. cv 0.37 → σ_diff ≈ 0.523,
        // z ≈ 3.44 → Strong.
        let base = metric(700_000, 300_000, 5000, 1000);
        let pr = metric(718_000, 300_000, 5000, 1000);
        let c = compare(&pr, &base, Some(0.37)).unwrap();
        assert_eq!(c.base_combined_us, 1_000_000);
        assert_eq!(c.pr_combined_us, 1_018_000);
        assert!((c.delta_pct - 1.8).abs() < 1e-9, "delta_pct = {}", c.delta_pct);
        let z = c.sigma.unwrap();
        assert!((z - 3.44).abs() < 0.02, "sigma = {z}");
        assert_eq!(c.verdict, Verdict::Strong);
    }

    #[test]
    fn faster_run_is_negative_delta_abs_sigma() {
        let base = metric(700_000, 300_000, 5000, 1000);
        let pr = metric(690_000, 300_000, 5000, 1000); // -1.0%
        let c = compare(&pr, &base, Some(0.37)).unwrap();
        assert!(c.delta_pct < 0.0);
        assert!((c.delta_pct + 1.0).abs() < 1e-9);
        // |−1.0%| / 0.523 ≈ 1.91 → Weak.
        assert_eq!(c.verdict, Verdict::Weak);
    }

    #[test]
    fn sub_noise_delta_is_inconclusive() {
        let base = metric(700_000, 300_000, 5000, 1000);
        let pr = metric(700_300, 300_000, 5000, 1000); // +0.03%
        let c = compare(&pr, &base, Some(0.37)).unwrap();
        assert_eq!(c.verdict, Verdict::Inconclusive);
        assert!(c.sigma.unwrap() < 1.0);
    }

    #[test]
    fn no_noise_floor_is_provisional_without_sigma() {
        let base = metric(700_000, 300_000, 5000, 1000);
        let pr = metric(740_000, 300_000, 5000, 1000);
        let c = compare(&pr, &base, None).unwrap();
        assert_eq!(c.verdict, Verdict::Provisional);
        assert!(c.sigma.is_none());
        // A zero/negative cv is treated the same as unset.
        assert_eq!(
            compare(&pr, &base, Some(0.0))
                .unwrap()
                .verdict,
            Verdict::Provisional
        );
    }

    #[test]
    fn workload_mismatch_is_incomparable() {
        let base = metric(700_000, 300_000, 5000, 1000);
        assert!(
            compare(&metric(700_000, 300_000, 4000, 1000), &base, Some(0.37)).is_none(),
            "different measured_blocks → incomparable",
        );
        assert!(
            compare(&metric(700_000, 300_000, 5000, 500), &base, Some(0.37)).is_none(),
            "different warmup_blocks → incomparable",
        );
    }

    #[test]
    fn degenerate_baseline_is_none() {
        let base = metric(0, 0, 5000, 1000);
        let pr = metric(700_000, 300_000, 5000, 1000);
        assert!(compare(&pr, &base, Some(0.37)).is_none());
    }

    #[test]
    fn multi_variant_comparison_is_baseline_plus_variant_deltas() {
        let base = spec(0, "release/3.4.0.0.2", 1, Some(11));
        let candidate = spec(1, "release/3.4.0.0.3", 1, Some(22));
        let metrics = metric_map([
            (base.id, metrics([(0, metric(700_000, 300_000, 5000, 1000))])),
            (candidate.id, metrics([(0, metric(718_000, 300_000, 5000, 1000))])),
        ]);

        let comparison = MultiVariantComparison::from_specs(
            &[candidate.clone(), base.clone()],
            &metrics,
            Some(0.37),
        )
        .expect("two specs yield a comparison summary");

        assert_eq!(
            comparison
                .baseline
                .ref_display,
            "release/3.4.0.0.2"
        );
        assert_eq!(
            comparison
                .baseline
                .baseline_calibration_id,
            Some(11)
        );
        assert_eq!(comparison.variants.len(), 1);
        let delta = &comparison.variants[0];
        assert_eq!(delta.variant.ref_display, "release/3.4.0.0.3");
        assert_eq!(
            delta
                .variant
                .baseline_calibration_id,
            Some(22)
        );
        assert!(delta.unavailable.is_none());
        assert!(
            (delta
                .comparison
                .as_ref()
                .unwrap()
                .delta_pct
                - 1.8)
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn repeated_multi_variant_comparison_uses_variant_means_and_sample_noise() {
        let base = spec(0, "base", 2, Some(1));
        let candidate = spec(1, "candidate", 2, Some(2));
        let metrics = metric_map([
            (
                base.id,
                metrics([
                    (0, metric(900_000, 100_000, 5000, 1000)),
                    (1, metric(920_000, 100_000, 5000, 1000)),
                ]),
            ),
            (
                candidate.id,
                metrics([
                    (0, metric(1_050_000, 100_000, 5000, 1000)),
                    (1, metric(1_070_000, 100_000, 5000, 1000)),
                ]),
            ),
        ]);

        let comparison =
            MultiVariantComparison::from_specs(&[base.clone(), candidate.clone()], &metrics, None)
                .unwrap();
        let delta = comparison.variants[0]
            .comparison
            .as_ref()
            .expect("repeated metrics compare without configured noise floor");

        assert_eq!(delta.base_combined_us, 1_010_000);
        assert_eq!(delta.pr_combined_us, 1_160_000);
        assert!((delta.delta_pct - 14.851_485).abs() < 1e-5);
        assert!(delta.sigma.is_some(), "sample noise classifies repeated comparisons");
        assert_eq!(delta.verdict, Verdict::Strong);
    }

    #[test]
    fn multi_variant_comparison_marks_sub_noise_single_run_inconclusive() {
        let base = spec(0, "base", 1, Some(1));
        let candidate = spec(1, "candidate", 1, Some(2));
        let metrics = metric_map([
            (base.id, metrics([(0, metric(700_000, 300_000, 5000, 1000))])),
            (candidate.id, metrics([(0, metric(700_300, 300_000, 5000, 1000))])),
        ]);

        let comparison =
            MultiVariantComparison::from_specs(&[base, candidate], &metrics, Some(0.37)).unwrap();
        assert_eq!(
            comparison.variants[0]
                .comparison
                .as_ref()
                .unwrap()
                .verdict,
            Verdict::Inconclusive,
        );
    }

    #[test]
    fn multi_variant_comparison_keeps_partial_missing_metric_data() {
        let base = spec(0, "base", 1, Some(1));
        let candidate = spec(1, "candidate", 1, Some(2));
        let metrics = metric_map([(base.id, metrics([(0, metric(700_000, 300_000, 5000, 1000))]))]);

        let comparison =
            MultiVariantComparison::from_specs(&[base, candidate], &metrics, Some(0.37)).unwrap();
        let delta = &comparison.variants[0];
        assert_eq!(delta.variant.stats.completed, 0);
        assert_eq!(delta.unavailable, Some(ComparisonUnavailable::MissingVariantMetric));
        assert!(delta.comparison.is_none());
    }

    #[test]
    fn multi_variant_comparison_preserves_workload_mismatch_guard() {
        let base = spec(0, "base", 1, Some(1));
        let candidate = spec(1, "candidate", 1, Some(2));
        let metrics = metric_map([
            (base.id, metrics([(0, metric(700_000, 300_000, 5000, 1000))])),
            (candidate.id, metrics([(0, metric(700_000, 300_000, 4000, 1000))])),
        ]);

        let comparison =
            MultiVariantComparison::from_specs(&[base, candidate], &metrics, Some(0.37)).unwrap();
        assert_eq!(
            comparison.variants[0].unavailable,
            Some(ComparisonUnavailable::IncomparableWorkload),
        );
    }
}
