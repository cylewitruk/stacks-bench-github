//! Workload resolution: user text → a validated [`WorkloadSpec`].
//!
//! This is the shared seam between *capture* (Slack mentions today, PR comments
//! later), *resolution* (text → spec), and *execution* (the existing bench
//! path). The deterministic impl ([`resolve_benchmark_request`]) is a flag
//! parser; the LLM path in [`crate::llm::intent`] produces the same structured
//! request. Either way, a resolver never emits raw `bench_args`, so it can't
//! inject arbitrary CLI flags.
//!
//! The grammar (provisional pending the Phase-0 `stacks-bench` spike): an
//! optional `bench` verb, then flags (each value as `--flag value` **or**
//! `--flag=value`) —
//!   `--txid <hex>` | `--block <height-or-hash>` (repeatable,
//!   **mutually exclusive**)
//!   `--start-at <height>` + (`--count <n>` | `--end-at <height>`)
//!   `--repetitions <n>`              (clean VM executions, ≥ 1)
//!   `--warmup <n>`                   (warmup iterations)
//!   `--rev <ref>`                    (override the code-under-test rev)
//!   `--compare-rev <ref>`            (repeatable; with `--rev`, comparison)
//!
//! v13 widens this seam: `--block` targets may be canonical heights or
//! hex-encoded block hashes, and all 32-byte hex inputs (`txid` / block hash)
//! are normalized by stripping an optional user-facing `0x` prefix before they
//! become `WorkloadSpec` values.
//!
//! Callers pass surface-specific wrappers already stripped (for example the
//! leading `@BenchBot` mention). Formatting concerns stay in the surface
//! adapter; this layer is deliberately surface-agnostic.

/// A Stacks txid/block hash is 32 bytes = 64 hex chars (an optional `0x`
/// prefix aside).
const HASH_HEX_LEN: usize = 64;

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
    /// The benchmark-group planner fans this out as isolated VM runs.
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

/// One code-under-test variant in a comparison group. Internally this is
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
/// singleton or multi-spec benchmark group.
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

/// Request-level caps enforced after either deterministic or LLM resolution.
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

/// Why a request couldn't be resolved. [`Display`](std::fmt::Display) renders a
/// short, user-facing reason — the connector posts it as the ephemeral
/// rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No content after the (optional) `bench` verb.
    Empty,
    /// No benchmark target was given.
    NoTarget,
    /// More than one benchmark target mode was given.
    MixedTargets,
    /// `--compare-rev` was given without the baseline `--rev`.
    MissingBaseRev,
    /// A flag expecting a value was the last token.
    MissingValue(String),
    /// A flag's value didn't validate.
    InvalidValue { flag: String, value: String, reason: String },
    /// An unrecognized `--flag`.
    UnknownFlag(String),
    /// A bare (non-flag) token where a flag was expected.
    UnexpectedToken(String),
    /// A scalar flag (`--repetitions`/`--warmup`/`--rev`) appeared twice.
    DuplicateFlag(String),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => {
                write!(f, "empty request — try `bench --block <height-or-hash> --repetitions <n>`")
            }
            Self::NoTarget => {
                write!(
                    f,
                    "no workload — give `--txid <hex>`, `--block <height-or-hash>`, or \
                     `--start-at <height> --count <n>`"
                )
            }
            Self::MixedTargets => {
                write!(f, "use only one target mode: `--txid`, `--block`, or `--start-at` range")
            }
            Self::MissingBaseRev => {
                write!(f, "`--compare-rev` needs an explicit baseline `--rev <ref>`")
            }
            Self::MissingValue(flag) => write!(f, "missing value for `{flag}`"),
            Self::InvalidValue { flag, value, reason } => {
                write!(f, "invalid value `{value}` for `{flag}`: {reason}")
            }
            Self::UnknownFlag(flag) => write!(f, "unknown flag `{flag}`"),
            Self::UnexpectedToken(tok) => write!(f, "unexpected `{tok}` — expected a `--flag`"),
            Self::DuplicateFlag(flag) => write!(f, "`{flag}` given more than once"),
        }
    }
}

impl std::error::Error for ResolveError {}

/// Resolve request `text` into either a singleton workload or an N-shaped
/// comparison request. Caps are deliberately enforced by
/// [`validate_benchmark_request`] so deterministic and LLM paths share one
/// policy gate.
pub fn resolve_benchmark_request(text: &str) -> Result<BenchmarkRequest, ResolveError> {
    let parsed = parse_workload_flags(text)?;
    let compare_revs = parsed.compare_revs.clone();
    let spec = parsed.into_workload()?;
    if compare_revs.is_empty() {
        return Ok(BenchmarkRequest::Single(spec));
    }
    let Some(base_rev) = spec.rev.clone() else {
        return Err(ResolveError::MissingBaseRev);
    };
    let mut variants = vec![ComparisonVariant { rev: base_rev }];
    variants.extend(
        compare_revs
            .into_iter()
            .map(|rev| ComparisonVariant { rev }),
    );
    Ok(BenchmarkRequest::Comparison(ComparisonRequest {
        workload: WorkloadSpec { rev: None, ..spec },
        variants,
    }))
}

#[derive(Debug, Clone, Default)]
struct ParsedWorkloadFlags {
    txids: Vec<String>,
    blocks: Vec<BlockSelector>,
    start_at: Option<u64>,
    end_at: Option<u64>,
    count: Option<u64>,
    repetitions: Option<u32>,
    warmup: Option<u32>,
    rev: Option<String>,
    compare_revs: Vec<String>,
    saw_any: bool,
}

impl ParsedWorkloadFlags {
    fn into_workload(self) -> Result<WorkloadSpec, ResolveError> {
        if !self.saw_any {
            return Err(ResolveError::Empty);
        }
        let has_range = self.start_at.is_some() || self.end_at.is_some() || self.count.is_some();
        let target_modes = u8::from(!self.txids.is_empty())
            + u8::from(!self.blocks.is_empty())
            + u8::from(has_range);
        if target_modes > 1 {
            return Err(ResolveError::MixedTargets);
        }
        let target = if !self.txids.is_empty() {
            WorkloadTarget::Txids(self.txids)
        } else if !self.blocks.is_empty() {
            WorkloadTarget::Blocks(self.blocks)
        } else if has_range {
            let start = self
                .start_at
                .ok_or_else(|| ResolveError::InvalidValue {
                    flag: "--start-at".into(),
                    value: "".into(),
                    reason: "required for block-range targets".into(),
                })?;
            if self.end_at.is_some() && self.count.is_some() {
                return Err(ResolveError::DuplicateFlag("--end-at/--count".into()));
            }
            let end = match (self.end_at, self.count) {
                (Some(end), None) => end,
                (None, Some(count)) => {
                    if count == 0 {
                        return Err(ResolveError::InvalidValue {
                            flag: "--count".into(),
                            value: count.to_string(),
                            reason: "must be at least 1".into(),
                        });
                    }
                    start
                        .checked_add(count - 1)
                        .ok_or_else(|| ResolveError::InvalidValue {
                            flag: "--count".into(),
                            value: count.to_string(),
                            reason: "range end overflows u64".into(),
                        })?
                }
                (None, None) => {
                    return Err(ResolveError::InvalidValue {
                        flag: "--start-at".into(),
                        value: start.to_string(),
                        reason: "requires `--count <n>` or `--end-at <height>`".into(),
                    });
                }
                (Some(_), Some(_)) => unreachable!("checked above"),
            };
            if start > end {
                return Err(ResolveError::InvalidValue {
                    flag: "--end-at".into(),
                    value: end.to_string(),
                    reason: "must be >= --start-at".into(),
                });
            }
            WorkloadTarget::BlockRange { start, end }
        } else {
            return Err(ResolveError::NoTarget);
        };

        Ok(WorkloadSpec {
            target,
            clean_repetitions: self.repetitions.unwrap_or(1),
            warmup: self.warmup,
            rev: self.rev,
        })
    }
}

fn parse_workload_flags(text: &str) -> Result<ParsedWorkloadFlags, ResolveError> {
    let mut tokens = text
        .split_whitespace()
        .peekable();

    // Skip an optional leading `bench` verb (`@BenchBot bench …` → `bench …` here).
    if tokens
        .peek()
        .is_some_and(|t| t.eq_ignore_ascii_case("bench"))
    {
        tokens.next();
    }

    let mut parsed = ParsedWorkloadFlags::default();

    while let Some(tok) = tokens.next() {
        parsed.saw_any = true;
        let raw = match tok.strip_prefix("--") {
            Some(_) => tok,
            None => return Err(ResolveError::UnexpectedToken(tok.to_string())),
        };
        // Every flag takes a value, accepted as either `--flag value` or
        // `--flag=value` (split on the first `=`, so a value may itself contain
        // one). An empty value (`--flag=`) is treated as missing.
        let (flag, value) = match raw.split_once('=') {
            Some((f, v)) => (f, v),
            None => (
                raw,
                tokens
                    .next()
                    .ok_or_else(|| ResolveError::MissingValue(raw.to_string()))?,
            ),
        };
        if value.is_empty() {
            return Err(ResolveError::MissingValue(flag.to_string()));
        }
        match flag {
            "--txid" => parsed
                .txids
                .push(validate_hash_hex(flag, value, "txid")?),
            "--block" => parsed
                .blocks
                .push(parse_block_selector(flag, value)?),
            "--start-at" => set_once(&mut parsed.start_at, flag, parse_u64(flag, value)?)?,
            "--end-at" => set_once(&mut parsed.end_at, flag, parse_u64(flag, value)?)?,
            "--count" => set_once(&mut parsed.count, flag, parse_u64(flag, value)?)?,
            "--repetitions" => {
                set_once(&mut parsed.repetitions, flag, parse_repetitions(flag, value)?)?
            }
            "--warmup" => set_once(&mut parsed.warmup, flag, parse_u32(flag, value)?)?,
            "--rev" => set_once(&mut parsed.rev, flag, value.to_string())?,
            "--compare-rev" => parsed
                .compare_revs
                .push(value.to_string()),
            other => return Err(ResolveError::UnknownFlag(other.to_string())),
        }
    }
    Ok(parsed)
}

/// Assign `slot` exactly once; a second assignment is a
/// [`ResolveError::DuplicateFlag`].
fn set_once<T>(slot: &mut Option<T>, flag: &str, value: T) -> Result<(), ResolveError> {
    if slot.is_some() {
        return Err(ResolveError::DuplicateFlag(flag.to_string()));
    }
    *slot = Some(value);
    Ok(())
}

fn parse_block_selector(flag: &str, value: &str) -> Result<BlockSelector, ResolveError> {
    match value.parse::<u64>() {
        Ok(h) => Ok(BlockSelector::Height(h)),
        Err(_) => validate_hash_hex(flag, value, "block hash").map(BlockSelector::Hash),
    }
}

fn parse_u32(flag: &str, value: &str) -> Result<u32, ResolveError> {
    value
        .parse::<u32>()
        .map_err(|_| ResolveError::InvalidValue {
            flag: flag.to_string(),
            value: value.to_string(),
            reason: "expected a non-negative integer".to_string(),
        })
}

fn parse_u64(flag: &str, value: &str) -> Result<u64, ResolveError> {
    value
        .parse::<u64>()
        .map_err(|_| ResolveError::InvalidValue {
            flag: flag.to_string(),
            value: value.to_string(),
            reason: "expected a non-negative integer".to_string(),
        })
}

/// User-facing `--repetitions` must be ≥ 1 (zero clean runs profiles nothing).
fn parse_repetitions(flag: &str, value: &str) -> Result<u32, ResolveError> {
    let n = parse_u32(flag, value)?;
    if n < 1 {
        return Err(ResolveError::InvalidValue {
            flag: flag.to_string(),
            value: value.to_string(),
            reason: "must be at least 1".to_string(),
        });
    }
    Ok(n)
}

/// Validate a txid or block hash: an optional `0x` prefix then
/// [`HASH_HEX_LEN`] hex chars. Accepted values are normalized to bare lowercase
/// hex before building [`WorkloadSpec`].
fn validate_hash_hex(flag: &str, value: &str, label: &str) -> Result<String, ResolveError> {
    let hex = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    let invalid = |reason: &str| ResolveError::InvalidValue {
        flag: flag.to_string(),
        value: value.to_string(),
        reason: reason.to_string(),
    };
    if hex.len() != HASH_HEX_LEN {
        return Err(invalid(&format!("expected a 64-character hex {label}")));
    }
    if !hex
        .bytes()
        .all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid("expected hex digits"));
    }
    Ok(hex.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> WorkloadSpec {
        match resolve_benchmark_request(s).unwrap() {
            BenchmarkRequest::Single(spec) => spec,
            BenchmarkRequest::Comparison(_) => panic!("expected single workload"),
        }
    }

    fn err(s: &str) -> ResolveError {
        resolve_benchmark_request(s).unwrap_err()
    }

    const TXID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn parses_a_block_workload_with_reps() {
        let spec = ok("bench --block 184231 --repetitions 5");
        assert_eq!(spec.target, WorkloadTarget::Blocks(vec![BlockSelector::Height(184231)]));
        assert_eq!(spec.clean_repetitions, 5);
        assert_eq!(spec.warmup, None);
        assert_eq!(spec.rev, None);
        assert_eq!(spec.to_bench_args(), vec!["--block", "184231", "--repetitions", "1"]);
    }

    #[test]
    fn accepts_equals_value_syntax() {
        // `--flag=value` is equivalent to `--flag value`, and the two forms mix.
        let spec = ok("bench --block=184231 --repetitions=5");
        assert_eq!(spec.target, WorkloadTarget::Blocks(vec![BlockSelector::Height(184231)]));
        assert_eq!(spec.clean_repetitions, 5);
        assert_eq!(
            ok("--block=1 --block 2").target,
            WorkloadTarget::Blocks(vec![BlockSelector::Height(1), BlockSelector::Height(2)])
        );
        // A rev may itself contain `=` (split on the first only).
        assert_eq!(
            ok("--block=1 --rev=a=b")
                .rev
                .as_deref(),
            Some("a=b")
        );
    }

    #[test]
    fn parses_block_range_with_count_or_end() {
        let spec = ok("bench --start-at 100 --count 3 --warmup 2");
        assert_eq!(spec.target, WorkloadTarget::BlockRange { start: 100, end: 102 });
        assert_eq!(
            spec.to_bench_args(),
            vec!["--start-at", "100", "--count", "3", "--warmup", "2"]
        );

        let spec = ok("bench --start-at=100 --end-at=101");
        assert_eq!(spec.target, WorkloadTarget::BlockRange { start: 100, end: 101 });
        assert_eq!(spec.to_bench_args(), vec!["--start-at", "100", "--count", "2"]);
    }

    #[test]
    fn block_range_rejects_incomplete_or_mixed_shapes() {
        assert!(matches!(
            err("--start-at 100"),
            ResolveError::InvalidValue { flag, .. } if flag == "--start-at"
        ));
        assert!(matches!(
            err("--count 3"),
            ResolveError::InvalidValue { flag, .. } if flag == "--start-at"
        ));
        assert!(matches!(
            err("--start-at 100 --count 0"),
            ResolveError::InvalidValue { flag, .. } if flag == "--count"
        ));
        assert_eq!(err("--block 1 --start-at 100 --count 1"), ResolveError::MixedTargets);
    }

    #[test]
    fn empty_equals_value_is_missing() {
        assert_eq!(err("--block="), ResolveError::MissingValue("--block".into()));
    }

    #[test]
    fn bench_verb_is_optional() {
        assert_eq!(ok("--block 7").target, WorkloadTarget::Blocks(vec![BlockSelector::Height(7)]));
    }

    #[test]
    fn repeatable_blocks_and_txids() {
        assert_eq!(
            ok("--block 1 --block 2 --block 3").target,
            WorkloadTarget::Blocks(vec![
                BlockSelector::Height(1),
                BlockSelector::Height(2),
                BlockSelector::Height(3)
            ])
        );
        let two_txids = format!("--txid {TXID} --txid {TXID}");
        let bare = TXID.trim_start_matches("0x");
        assert_eq!(
            ok(&two_txids).target,
            WorkloadTarget::Txids(vec![bare.to_string(), bare.to_string()])
        );
    }

    #[test]
    fn rev_is_captured_but_not_a_bench_arg() {
        let spec = ok("--block 9 --rev feature/x --warmup 2");
        assert_eq!(spec.rev.as_deref(), Some("feature/x"));
        assert_eq!(spec.warmup, Some(2));
        // rev is the code-under-test, not a workload arg.
        assert_eq!(
            spec.to_bench_args(),
            vec!["--block", "9", "--repetitions", "1", "--warmup", "2"]
        );
    }

    #[test]
    fn deterministic_comparison_request_is_n_shaped() {
        let request = resolve_benchmark_request(
            "bench --txid 0x1111111111111111111111111111111111111111111111111111111111111111 \
             --rev sb-integration/3.4.0.0.2 --compare-rev sb-integration/3.4.0.0.3 --repetitions \
             5 --warmup 10",
        )
        .unwrap();
        let BenchmarkRequest::Comparison(comparison) = request else {
            panic!("expected comparison");
        };
        assert_eq!(
            comparison
                .variants
                .iter()
                .map(|v| v.rev.as_str())
                .collect::<Vec<_>>(),
            vec!["sb-integration/3.4.0.0.2", "sb-integration/3.4.0.0.3"]
        );
        assert_eq!(comparison.workload.rev, None);
        assert_eq!(
            comparison
                .workload
                .clean_repetitions,
            5
        );
        assert_eq!(comparison.workload.warmup, Some(10));
    }

    #[test]
    fn comparison_requires_an_explicit_base_rev() {
        assert_eq!(
            resolve_benchmark_request("--block 1 --compare-rev feature/x").unwrap_err(),
            ResolveError::MissingBaseRev
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

    #[test]
    fn txid_without_0x_prefix_is_accepted() {
        let bare = &TXID[2..]; // strip 0x
        let spec = ok(&format!("--txid {bare}"));
        assert_eq!(spec.target, WorkloadTarget::Txids(vec![bare.to_string()]));
    }

    #[test]
    fn txid_prefix_is_stripped_and_normalized() {
        let mixed = "0xABCDEFabcdef1111111111111111111111111111111111111111111111111111";
        let spec = ok(&format!("--txid {mixed}"));
        assert_eq!(
            spec.target,
            WorkloadTarget::Txids(vec![
                "abcdefabcdef1111111111111111111111111111111111111111111111111111".to_string()
            ])
        );
    }

    #[test]
    fn block_hash_prefix_is_stripped_and_normalized() {
        let hash = "0xABCDEFabcdef2222222222222222222222222222222222222222222222222222";
        let spec = ok(&format!("--block {hash}"));
        assert_eq!(
            spec.target,
            WorkloadTarget::Blocks(vec![BlockSelector::Hash(
                "abcdefabcdef2222222222222222222222222222222222222222222222222222".to_string()
            )])
        );
        assert_eq!(
            spec.to_bench_args(),
            vec![
                "--block",
                "abcdefabcdef2222222222222222222222222222222222222222222222222222",
                "--repetitions",
                "1"
            ]
        );
    }

    #[test]
    fn mutually_exclusive_targets_rejected() {
        let mixed = format!("--block 1 --txid {TXID}");
        assert_eq!(err(&mixed), ResolveError::MixedTargets);
    }

    #[test]
    fn no_target_rejected() {
        assert_eq!(err("--repetitions 3"), ResolveError::NoTarget);
    }

    #[test]
    fn empty_rejected() {
        assert_eq!(err(""), ResolveError::Empty);
        assert_eq!(err("bench"), ResolveError::Empty);
        assert_eq!(err("   "), ResolveError::Empty);
    }

    #[test]
    fn missing_value_rejected() {
        assert_eq!(err("--block"), ResolveError::MissingValue("--block".into()));
    }

    #[test]
    fn invalid_block_rejected() {
        assert!(matches!(err("--block abc"), ResolveError::InvalidValue { .. }));
    }

    #[test]
    fn invalid_txid_rejected() {
        // too short
        assert!(matches!(err("--txid 0xabc"), ResolveError::InvalidValue { .. }));
        // right length, non-hex
        let nonhex = format!("--txid {}", "z".repeat(64));
        assert!(matches!(err(&nonhex), ResolveError::InvalidValue { .. }));
    }

    #[test]
    fn zero_repetitions_rejected() {
        assert!(matches!(err("--block 1 --repetitions 0"), ResolveError::InvalidValue { .. }));
    }

    #[test]
    fn unknown_flag_rejected() {
        assert_eq!(err("--block 1 --bogus x"), ResolveError::UnknownFlag("--bogus".into()));
    }

    #[test]
    fn unexpected_token_rejected() {
        assert_eq!(err("--block 1 stray"), ResolveError::UnexpectedToken("stray".into()));
    }

    #[test]
    fn duplicate_scalar_flag_rejected() {
        assert_eq!(
            err("--block 1 --repetitions 2 --repetitions 3"),
            ResolveError::DuplicateFlag("--repetitions".into())
        );
    }
}
