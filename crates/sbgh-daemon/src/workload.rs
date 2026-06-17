//! Workload resolution: user text → a validated [`WorkloadSpec`].
//!
//! This is the shared seam between *capture* (Slack mentions today, PR comments
//! later), *resolution* (text → spec), and *execution* (the existing bench
//! path). The deterministic impl ([`resolve_workload`]) is a flag parser; the
//! LLM path in [`crate::llm::intent`] produces the same structured spec. Either
//! way, a resolver never emits raw `bench_args`, so it can't inject arbitrary
//! CLI flags.
//!
//! The grammar (provisional pending the Phase-0 `stacks-bench` spike): an
//! optional `bench` verb, then flags (each value as `--flag value` **or**
//! `--flag=value`) —
//!   `--txid <hex>` | `--block <height-or-hash>` (repeatable,
//!   **mutually exclusive**)
//!   `--repetitions <n>`              (clean VM executions, ≥ 1)
//!   `--warmup <n>`                   (warmup iterations)
//!   `--rev <ref>`                    (override the code-under-test rev)
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
    /// The current v15 Phase 1 runtime still executes one run; later phases
    /// fan this out at the benchmark-group layer.
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

/// Why a request couldn't be resolved. [`Display`](std::fmt::Display) renders a
/// short, user-facing reason — the connector posts it as the ephemeral
/// rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No content after the (optional) `bench` verb.
    Empty,
    /// Neither `--txid` nor `--block` was given.
    NoTarget,
    /// Both `--txid` and `--block` were given (mutually exclusive).
    MixedTargets,
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
                write!(f, "no workload — give `--txid <hex>` or `--block <height-or-hash>`")
            }
            Self::MixedTargets => {
                write!(f, "use only one of `--txid` or `--block`, not both")
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

/// Resolve request `text` (mention already stripped) into a validated
/// [`WorkloadSpec`]. The deterministic v1 resolver; see the module docs for the
/// grammar and the LLM-resolver seam.
pub fn resolve_workload(text: &str) -> Result<WorkloadSpec, ResolveError> {
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

    let mut txids: Vec<String> = Vec::new();
    let mut blocks: Vec<BlockSelector> = Vec::new();
    let mut repetitions: Option<u32> = None;
    let mut warmup: Option<u32> = None;
    let mut rev: Option<String> = None;
    let mut saw_any = false;

    while let Some(tok) = tokens.next() {
        saw_any = true;
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
            "--txid" => txids.push(validate_hash_hex(flag, value, "txid")?),
            "--block" => blocks.push(parse_block_selector(flag, value)?),
            "--repetitions" => set_once(&mut repetitions, flag, parse_repetitions(flag, value)?)?,
            "--warmup" => set_once(&mut warmup, flag, parse_u32(flag, value)?)?,
            "--rev" => set_once(&mut rev, flag, value.to_string())?,
            other => return Err(ResolveError::UnknownFlag(other.to_string())),
        }
    }

    if !saw_any {
        return Err(ResolveError::Empty);
    }

    let target = match (txids.is_empty(), blocks.is_empty()) {
        (false, false) => return Err(ResolveError::MixedTargets),
        (true, true) => return Err(ResolveError::NoTarget),
        (false, true) => WorkloadTarget::Txids(txids),
        (true, false) => WorkloadTarget::Blocks(blocks),
    };

    Ok(WorkloadSpec {
        target,
        clean_repetitions: repetitions.unwrap_or(1),
        warmup,
        rev,
    })
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
        resolve_workload(s).unwrap()
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
    fn empty_equals_value_is_missing() {
        assert_eq!(
            resolve_workload("--block=").unwrap_err(),
            ResolveError::MissingValue("--block".into())
        );
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
        assert_eq!(resolve_workload(&mixed).unwrap_err(), ResolveError::MixedTargets);
    }

    #[test]
    fn no_target_rejected() {
        assert_eq!(resolve_workload("--repetitions 3").unwrap_err(), ResolveError::NoTarget);
    }

    #[test]
    fn empty_rejected() {
        assert_eq!(resolve_workload("").unwrap_err(), ResolveError::Empty);
        assert_eq!(resolve_workload("bench").unwrap_err(), ResolveError::Empty);
        assert_eq!(resolve_workload("   ").unwrap_err(), ResolveError::Empty);
    }

    #[test]
    fn missing_value_rejected() {
        assert_eq!(
            resolve_workload("--block").unwrap_err(),
            ResolveError::MissingValue("--block".into())
        );
    }

    #[test]
    fn invalid_block_rejected() {
        assert!(matches!(
            resolve_workload("--block abc").unwrap_err(),
            ResolveError::InvalidValue { .. }
        ));
    }

    #[test]
    fn invalid_txid_rejected() {
        // too short
        assert!(matches!(
            resolve_workload("--txid 0xabc").unwrap_err(),
            ResolveError::InvalidValue { .. }
        ));
        // right length, non-hex
        let nonhex = format!("--txid {}", "z".repeat(64));
        assert!(matches!(
            resolve_workload(&nonhex).unwrap_err(),
            ResolveError::InvalidValue { .. }
        ));
    }

    #[test]
    fn zero_repetitions_rejected() {
        assert!(matches!(
            resolve_workload("--block 1 --repetitions 0").unwrap_err(),
            ResolveError::InvalidValue { .. }
        ));
    }

    #[test]
    fn unknown_flag_rejected() {
        assert_eq!(
            resolve_workload("--block 1 --bogus x").unwrap_err(),
            ResolveError::UnknownFlag("--bogus".into())
        );
    }

    #[test]
    fn unexpected_token_rejected() {
        assert_eq!(
            resolve_workload("--block 1 stray").unwrap_err(),
            ResolveError::UnexpectedToken("stray".into())
        );
    }

    #[test]
    fn duplicate_scalar_flag_rejected() {
        assert_eq!(
            resolve_workload("--block 1 --repetitions 2 --repetitions 3").unwrap_err(),
            ResolveError::DuplicateFlag("--repetitions".into())
        );
    }
}
