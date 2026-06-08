//! roadmap-v7 Phase 2: pure baseline comparison + confidence.
//!
//! Turns a PR run's metric + a baseline's metric into a delta on the combined
//! **Execution+Commit** budget, plus a confidence read expressed in sigmas
//! against the measured noise floor. Pure and side-effect-free — the reporter
//! (Phase 3) consumes a [`Comparison`] to render the headline + verdict.
//!
//! Why Execution+Commit and not wall-clock: the two buckets vary inversely
//! run-to-run but their **sum is conserved** at a low CV, while envelope
//! wall-clock carries VM-boot / archive / teardown noise unrelated to the
//! benched code (see the variance study + roadmap-v7).

use chrono::{DateTime, Utc};
use sbgh_core::db::BaselineSelection;
use sbgh_core::models::JobMetric;

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
}
