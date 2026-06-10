//! Intent resolution: a Slack request's text → a validated [`WorkloadSpec`].
//!
//! This is the **seam** the design pins (v5): *capture* (the mention surface) ⟂
//! *resolution* (text → spec) ⟂ *execution* (the existing bench path). The v1
//! impl ([`resolve_workload`]) is a deterministic flag parser; a future LLM
//! resolver (`0020`) plugs in behind the same signature. Either way the output
//! is a **structured** spec that the same validation produces — a resolver
//! never emits raw `bench_args`, so it can't inject arbitrary CLI flags.
//!
//! The grammar (provisional pending the Phase-0 `stacks-bench` spike): an
//! optional `bench` verb, then flags (each value as `--flag value` **or**
//! `--flag=value`) —
//!   `--txid <hex>` | `--block <n>`   (each repeatable, **mutually exclusive**)
//!   `--repetitions <n>`              (how many times to run each, ≥ 1)
//!   `--warmup <n>`                   (warmup iterations)
//!   `--rev <ref>`                    (override the code-under-test rev)
//!
//! The caller passes text with the leading `@sbgh` mention already stripped
//! (Slack-formatting concerns stay in the connector; this layer is surface- and
//! Slack-agnostic so the LLM resolver can reuse it).

/// A Stacks txid is 32 bytes = 64 hex chars (an optional `0x` prefix aside).
const TXID_HEX_LEN: usize = 64;

/// What to profile — `--txid` and `--block` are mutually exclusive, so exactly
/// one variant is present, each with ≥ 1 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadTarget {
    Txids(Vec<String>),
    Blocks(Vec<u64>),
}

/// A validated ad-hoc workload: the target plus the run parameters. The **code
/// under test** is *not* here — `rev` only *overrides* the configured default;
/// the connector resolves repo/rev separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadSpec {
    pub target: WorkloadTarget,
    /// `--repetitions` — how many times to execute each target (≥ 1). `None` →
    /// the driver's configured default applies.
    pub repetitions: Option<u32>,
    /// `--warmup` — warmup iterations before measurement. `None` → default.
    pub warmup: Option<u32>,
    /// `--rev` — override the code-under-test rev (branch/tag/sha). `None` →
    /// the `[slack].default_rev`. **Not** a `stacks-bench` arg (see
    /// [`Self::to_bench_args`]).
    pub rev: Option<String>,
}

impl WorkloadSpec {
    /// The `stacks-bench` CLI args for this workload — the **workload** flags
    /// only (`--txid`/`--block`/`--repetitions`/`--warmup`). `rev` is the
    /// code-under-test, applied by the connector, not a bench arg.
    pub fn to_bench_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        match &self.target {
            WorkloadTarget::Txids(txids) => {
                for t in txids {
                    args.push("--txid".to_string());
                    args.push(t.clone());
                }
            }
            WorkloadTarget::Blocks(blocks) => {
                for b in blocks {
                    args.push("--block".to_string());
                    args.push(b.to_string());
                }
            }
        }
        if let Some(r) = self.repetitions {
            args.push("--repetitions".to_string());
            args.push(r.to_string());
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
                write!(f, "empty request — try `bench --block <height> --repetitions <n>`")
            }
            Self::NoTarget => write!(f, "no workload — give `--txid <hex>` or `--block <height>`"),
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

    // Skip an optional leading `bench` verb (`@sbgh bench …` → `bench …` here).
    if tokens
        .peek()
        .is_some_and(|t| t.eq_ignore_ascii_case("bench"))
    {
        tokens.next();
    }

    let mut txids: Vec<String> = Vec::new();
    let mut blocks: Vec<u64> = Vec::new();
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
            "--txid" => txids.push(validate_txid(flag, value)?),
            "--block" => blocks.push(parse_u64(flag, value)?),
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
        repetitions,
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

fn parse_u64(flag: &str, value: &str) -> Result<u64, ResolveError> {
    value
        .parse::<u64>()
        .map_err(|_| ResolveError::InvalidValue {
            flag: flag.to_string(),
            value: value.to_string(),
            reason: "expected a non-negative integer".to_string(),
        })
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

/// `--repetitions` must be ≥ 1 (zero runs profiles nothing).
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

/// Validate a txid (provisional, pending the Phase-0 spike): an optional `0x`
/// prefix then [`TXID_HEX_LEN`] hex chars. Stored verbatim (trimmed) so the
/// exact form the user typed reaches `stacks-bench`.
fn validate_txid(flag: &str, value: &str) -> Result<String, ResolveError> {
    let hex = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    let invalid = |reason: &str| ResolveError::InvalidValue {
        flag: flag.to_string(),
        value: value.to_string(),
        reason: reason.to_string(),
    };
    if hex.len() != TXID_HEX_LEN {
        return Err(invalid("expected a 64-character hex txid"));
    }
    if !hex
        .bytes()
        .all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid("expected hex digits"));
    }
    Ok(value.to_string())
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
        assert_eq!(spec.target, WorkloadTarget::Blocks(vec![184231]));
        assert_eq!(spec.repetitions, Some(5));
        assert_eq!(spec.warmup, None);
        assert_eq!(spec.rev, None);
        assert_eq!(spec.to_bench_args(), vec!["--block", "184231", "--repetitions", "5"]);
    }

    #[test]
    fn accepts_equals_value_syntax() {
        // `--flag=value` is equivalent to `--flag value`, and the two forms mix.
        let spec = ok("bench --block=184231 --repetitions=5");
        assert_eq!(spec.target, WorkloadTarget::Blocks(vec![184231]));
        assert_eq!(spec.repetitions, Some(5));
        assert_eq!(ok("--block=1 --block 2").target, WorkloadTarget::Blocks(vec![1, 2]));
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
        assert_eq!(ok("--block 7").target, WorkloadTarget::Blocks(vec![7]));
    }

    #[test]
    fn repeatable_blocks_and_txids() {
        assert_eq!(
            ok("--block 1 --block 2 --block 3").target,
            WorkloadTarget::Blocks(vec![1, 2, 3])
        );
        let two_txids = format!("--txid {TXID} --txid {TXID}");
        assert_eq!(
            ok(&two_txids).target,
            WorkloadTarget::Txids(vec![TXID.to_string(), TXID.to_string()])
        );
    }

    #[test]
    fn rev_is_captured_but_not_a_bench_arg() {
        let spec = ok("--block 9 --rev feature/x --warmup 2");
        assert_eq!(spec.rev.as_deref(), Some("feature/x"));
        assert_eq!(spec.warmup, Some(2));
        // rev is the code-under-test, not a workload arg.
        assert_eq!(spec.to_bench_args(), vec!["--block", "9", "--warmup", "2"]);
    }

    #[test]
    fn txid_without_0x_prefix_is_accepted() {
        let bare = &TXID[2..]; // strip 0x
        let spec = ok(&format!("--txid {bare}"));
        assert_eq!(spec.target, WorkloadTarget::Txids(vec![bare.to_string()]));
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
