//! Provider-neutral benchmark workload and request types.
//!
//! Conversational parsing belongs to the typed intent adapter. This module
//! validates resolved requests and renders the exact `stacks-bench` arguments
//! used by execution.

/// One block selector accepted by `stacks-bench --block`: either a canonical
/// block height or a 32-byte block hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockSelector {
    Height(u64),
    Hash(String),
}

impl BlockSelector {
    fn as_bench_arg(&self) -> String {
        match self {
            Self::Height(h) => h.to_string(),
            Self::Hash(h) => h.clone(),
        }
    }
}

/// What to profile — `--txid` and `--block` are mutually exclusive, so exactly
/// one variant is present, each with ≥ 1 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadTarget {
    Txids(Vec<String>),
    Blocks(Vec<BlockSelector>),
    /// Inclusive canonical block-height range. Emitted as `--start-at` +
    /// `--count`, not as one `--block` arg per height.
    BlockRange {
        start: u64,
        end: u64,
    },
}

/// A validated ad-hoc workload: the target plus the run parameters. The **code
/// under test** is *not* here — `rev` only *overrides* the configured default;
/// the connector resolves repo/rev separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadSpec {
    pub target: WorkloadTarget,
    /// User-facing `--repetitions` — daemon-level clean VM executions (≥ 1).
    /// The benchmark-submission planner fans this out as isolated VM runs.
    pub clean_repetitions: u32,
    /// `--warmup` — warmup iterations before measurement. `None` → default.
    pub warmup: Option<u32>,
    /// `--rev` — override the code-under-test rev (branch/tag/sha). `None` →
    /// the `[slack].default_rev`. **Not** a `stacks-bench` arg (see
    /// [`Self::to_bench_args`]).
    pub rev: Option<String>,
}

impl WorkloadSpec {
    /// The `stacks-bench` CLI args for one isolated VM execution — the
    /// **workload** flags only (`--txid`/`--block` or block range, plus
    /// `--warmup` where present). `rev` is the code-under-test, applied by the
    /// connector, not a bench arg.
    ///
    /// v15 repurposes user-facing repetitions as daemon-level clean runs.
    /// Targeted `--txid`/`--block` invocations still receive one in-process
    /// repetition for compatibility with that CLI shape; block-range mode does
    /// not support `--repetitions`.
    pub fn to_bench_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        let mut include_in_process_repetition = false;
        match &self.target {
            WorkloadTarget::Txids(txids) => {
                include_in_process_repetition = true;
                for t in txids {
                    args.push("--txid".to_string());
                    args.push(t.clone());
                }
            }
            WorkloadTarget::Blocks(blocks) => {
                include_in_process_repetition = true;
                for b in blocks {
                    args.push("--block".to_string());
                    args.push(b.as_bench_arg());
                }
            }
            WorkloadTarget::BlockRange { start, end } => {
                args.push("--start-at".to_string());
                args.push(start.to_string());
                args.push("--count".to_string());
                args.push((end - start + 1).to_string());
            }
        }
        if include_in_process_repetition {
            args.push("--repetitions".to_string());
            args.push("1".to_string());
        }
        if let Some(w) = self.warmup {
            args.push("--warmup".to_string());
            args.push(w.to_string());
        }
        args
    }
}

/// One code-under-test variant in a comparison submission. Internally this is
/// intentionally N-shaped; v22 Phase 1 caps the accepted length at validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComparisonVariant {
    pub rev: String,
}

/// A comparison request: one workload executed against multiple refs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComparisonRequest {
    /// The workload target/run settings. `rev` must be `None`; refs live in
    /// `variants`.
    pub workload: WorkloadSpec,
    pub variants: Vec<ComparisonVariant>,
}

/// A resolved benchmark request before enqueue planning. Both variants flow
/// through the same validation gate before the Slack connector creates a
/// singleton or multi-spec benchmark submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BenchmarkRequest {
    Single(WorkloadSpec),
    Comparison(ComparisonRequest),
}

impl BenchmarkRequest {
    pub fn clean_repetitions(&self) -> u32 {
        match self {
            Self::Single(spec) => spec.clean_repetitions,
            Self::Comparison(req) => req.workload.clean_repetitions,
        }
    }

    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::Single(_) => "single",
            Self::Comparison(_) => "comparison",
        }
    }
}

/// Request-level caps enforced after typed intent resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLimits {
    pub max_clean_repetitions: u32,
    pub max_variants: usize,
    pub max_comparison_lifecycles: u32,
}

impl RequestLimits {
    pub fn new(
        max_clean_repetitions: u32,
        max_variants: u32,
        max_comparison_lifecycles: u32,
    ) -> Self {
        Self {
            max_clean_repetitions: max_clean_repetitions.max(1),
            max_variants: max_variants.max(1) as usize,
            max_comparison_lifecycles: max_comparison_lifecycles.max(1),
        }
    }
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self::new(5, 2, 10)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestValidationError {
    TooManyCleanRepetitions { requested: u32, max: u32 },
    TooFewVariants { requested: usize },
    TooManyVariants { requested: usize, max: usize },
    TooManyComparisonLifecycles { requested: u32, max: u32 },
    EmptyVariantRef,
    DuplicateVariantRef(String),
    ComparisonWorkloadCarriesRev,
}

impl std::fmt::Display for RequestValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyCleanRepetitions { requested, max } => {
                write!(f, "too many clean repetitions — requested {requested}, max is {max}")
            }
            Self::TooFewVariants { requested } => {
                write!(f, "comparison requests need at least two refs — got {requested}")
            }
            Self::TooManyVariants { requested, max } => {
                write!(f, "too many comparison refs — requested {requested}, max is {max}")
            }
            Self::TooManyComparisonLifecycles { requested, max } => write!(
                f,
                "comparison request is too large — requested {requested} VM runs, max is {max}"
            ),
            Self::EmptyVariantRef => write!(f, "comparison refs must be non-empty"),
            Self::DuplicateVariantRef(rev) => write!(f, "comparison ref `{rev}` is duplicated"),
            Self::ComparisonWorkloadCarriesRev => {
                write!(f, "comparison refs must live in variants, not workload rev")
            }
        }
    }
}

impl std::error::Error for RequestValidationError {}

pub fn validate_benchmark_request(
    request: BenchmarkRequest,
    limits: RequestLimits,
) -> Result<BenchmarkRequest, RequestValidationError> {
    if request.clean_repetitions() > limits.max_clean_repetitions {
        return Err(RequestValidationError::TooManyCleanRepetitions {
            requested: request.clean_repetitions(),
            max: limits.max_clean_repetitions,
        });
    }
    match request {
        BenchmarkRequest::Single(spec) => Ok(BenchmarkRequest::Single(spec)),
        BenchmarkRequest::Comparison(mut comparison) => {
            if comparison
                .workload
                .rev
                .is_some()
            {
                return Err(RequestValidationError::ComparisonWorkloadCarriesRev);
            }
            if comparison.variants.len() < 2 {
                return Err(RequestValidationError::TooFewVariants {
                    requested: comparison.variants.len(),
                });
            }
            if comparison.variants.len() > limits.max_variants {
                return Err(RequestValidationError::TooManyVariants {
                    requested: comparison.variants.len(),
                    max: limits.max_variants,
                });
            }
            let lifecycles = (comparison.variants.len() as u32).saturating_mul(
                comparison
                    .workload
                    .clean_repetitions,
            );
            if lifecycles > limits.max_comparison_lifecycles {
                return Err(RequestValidationError::TooManyComparisonLifecycles {
                    requested: lifecycles,
                    max: limits.max_comparison_lifecycles,
                });
            }

            let mut seen = std::collections::BTreeSet::new();
            for variant in &mut comparison.variants {
                variant.rev = variant.rev.trim().to_string();
                if variant.rev.is_empty() {
                    return Err(RequestValidationError::EmptyVariantRef);
                }
                if !seen.insert(variant.rev.clone()) {
                    return Err(RequestValidationError::DuplicateVariantRef(variant.rev.clone()));
                }
            }
            Ok(BenchmarkRequest::Comparison(comparison))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_specs_render_only_execution_arguments() {
        let spec = WorkloadSpec {
            target: WorkloadTarget::BlockRange { start: 100, end: 102 },
            clean_repetitions: 3,
            warmup: Some(2),
            rev: Some("candidate".into()),
        };
        assert_eq!(
            spec.to_bench_args(),
            vec!["--start-at", "100", "--count", "3", "--warmup", "2"]
        );
        let blocks = WorkloadSpec {
            target: WorkloadTarget::Blocks(vec![
                BlockSelector::Height(9),
                BlockSelector::Hash("a".repeat(64)),
            ]),
            clean_repetitions: 7,
            warmup: None,
            rev: None,
        };
        assert_eq!(
            blocks.to_bench_args(),
            vec![
                "--block".to_string(),
                "9".to_string(),
                "--block".to_string(),
                "a".repeat(64),
                "--repetitions".to_string(),
                "1".to_string(),
            ]
        );
    }

    #[test]
    fn request_validation_caps_and_normalizes_comparisons() {
        let request = BenchmarkRequest::Comparison(ComparisonRequest {
            workload: WorkloadSpec {
                target: WorkloadTarget::Blocks(vec![BlockSelector::Height(1)]),
                clean_repetitions: 5,
                warmup: None,
                rev: None,
            },
            variants: vec![
                ComparisonVariant { rev: " base ".into() },
                ComparisonVariant { rev: " candidate ".into() },
            ],
        });
        let validated = validate_benchmark_request(request, RequestLimits::new(5, 2, 10)).unwrap();
        let BenchmarkRequest::Comparison(comparison) = validated else {
            panic!("expected comparison");
        };
        assert_eq!(
            comparison
                .variants
                .iter()
                .map(|v| v.rev.as_str())
                .collect::<Vec<_>>(),
            vec!["base", "candidate"]
        );

        let over = BenchmarkRequest::Comparison(ComparisonRequest {
            workload: WorkloadSpec {
                target: WorkloadTarget::Blocks(vec![BlockSelector::Height(1)]),
                clean_repetitions: 6,
                warmup: None,
                rev: None,
            },
            variants: vec![
                ComparisonVariant { rev: "base".into() },
                ComparisonVariant { rev: "candidate".into() },
            ],
        });
        assert!(matches!(
            validate_benchmark_request(over, RequestLimits::new(5, 2, 20)),
            Err(RequestValidationError::TooManyCleanRepetitions { .. })
        ));
    }

    #[test]
    fn request_validation_rejects_duplicate_or_oversized_comparisons() {
        let duplicate = BenchmarkRequest::Comparison(ComparisonRequest {
            workload: WorkloadSpec {
                target: WorkloadTarget::Blocks(vec![BlockSelector::Height(1)]),
                clean_repetitions: 1,
                warmup: None,
                rev: None,
            },
            variants: vec![
                ComparisonVariant { rev: "same".into() },
                ComparisonVariant { rev: " same ".into() },
            ],
        });
        assert!(matches!(
            validate_benchmark_request(duplicate, RequestLimits::new(5, 2, 10)),
            Err(RequestValidationError::DuplicateVariantRef(rev)) if rev == "same"
        ));

        let too_many_lifecycles = BenchmarkRequest::Comparison(ComparisonRequest {
            workload: WorkloadSpec {
                target: WorkloadTarget::Blocks(vec![BlockSelector::Height(1)]),
                clean_repetitions: 5,
                warmup: None,
                rev: None,
            },
            variants: vec![
                ComparisonVariant { rev: "base".into() },
                ComparisonVariant { rev: "candidate".into() },
            ],
        });
        assert!(matches!(
            validate_benchmark_request(too_many_lifecycles, RequestLimits::new(5, 2, 9)),
            Err(RequestValidationError::TooManyComparisonLifecycles { requested: 10, max: 9 })
        ));
    }
}
