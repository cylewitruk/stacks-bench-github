//! Human-friendly byte-size type for config values.
//!
//! Accepts the `<n><suffix>` syntax familiar from `lvcreate`,
//! `mkfs`, systemd unit files, etc.:
//!
//! | suffix      | meaning      |
//! | ----------- | ------------ |
//! | `K` / `Ki`  | 1024 bytes   |
//! | `M` / `Mi`  | 1024² bytes  |
//! | `G` / `Gi`  | 1024³ bytes  |
//! | `T` / `Ti`  | 1024⁴ bytes  |
//! | (no suffix) | bytes        |
//!
//! All suffixes are case-insensitive; the trailing `B` / `iB` is
//! optional (`16G`, `16GB`, `16GiB`, `16gib` all parse the same).
//! Always binary (`G` ≡ `Gi`) — decimal `k`/`m`/`g` (×1000) are
//! ambiguous and unfamiliar for memory sizing.
//!
//! Implements `Deserialize` for TOML/JSON config layers so a field
//! like `build_memory = "16G"` round-trips to a `MemorySize` with
//! `.as_bytes()` returning `17179869184`. The `Serialize` impl emits
//! the canonical short form (e.g. `"16G"` for exact GiB).

use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const KIB: u64 = 1024;
const MIB: u64 = KIB * 1024;
const GIB: u64 = MIB * 1024;
const TIB: u64 = GIB * 1024;

/// A memory size in bytes.
///
/// Construct via `MemorySize::from_bytes`, `MemorySize::from_str`, or
/// the `Deserialize` impl. Cheap (`Copy`); comparisons are byte-exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemorySize {
    bytes: u64,
}

impl MemorySize {
    pub const fn from_bytes(bytes: u64) -> Self {
        Self { bytes }
    }

    pub const fn from_kib(kib: u64) -> Self {
        Self { bytes: kib * KIB }
    }

    pub const fn from_mib(mib: u64) -> Self {
        Self { bytes: mib * MIB }
    }

    pub const fn from_gib(gib: u64) -> Self {
        Self { bytes: gib * GIB }
    }

    pub const fn as_bytes(self) -> u64 {
        self.bytes
    }

    pub const fn as_kib(self) -> u64 {
        self.bytes / KIB
    }

    pub const fn as_mib(self) -> u64 {
        self.bytes / MIB
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemorySizeParseError {
    #[error("empty memory size string")]
    Empty,
    #[error("unrecognised suffix `{0}` (use K/M/G/T or no suffix for bytes)")]
    UnknownSuffix(String),
    #[error("invalid numeric component: {0}")]
    InvalidNumber(#[from] ParseIntError),
}

impl FromStr for MemorySize {
    type Err = MemorySizeParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let s = raw.trim();
        if s.is_empty() {
            return Err(MemorySizeParseError::Empty);
        }

        // Find the boundary between digits and suffix. Walk forward
        // accepting digits + decimal point (we don't actually support
        // decimals but we want a clear error rather than parsing "1.5"
        // as just "1"). Anything past that is suffix.
        let split = s
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(s.len());
        let (num_str, suffix_raw) = s.split_at(split);
        if num_str.is_empty() {
            return Err(MemorySizeParseError::UnknownSuffix(s.to_string()));
        }
        let n: u64 = num_str.parse()?;

        let multiplier = match normalise_suffix(suffix_raw).as_str() {
            "" => 1,
            "k" => KIB,
            "m" => MIB,
            "g" => GIB,
            "t" => TIB,
            _ => return Err(MemorySizeParseError::UnknownSuffix(suffix_raw.to_string())),
        };

        Ok(Self { bytes: n * multiplier })
    }
}

/// Reduce a suffix to its single-letter canonical form. Accepts
/// `Gi`/`GiB`/`GB`/`G` (case-insensitive) → `g`.
fn normalise_suffix(raw: &str) -> String {
    let lower = raw
        .trim()
        .to_ascii_lowercase();
    let trimmed = lower
        .strip_suffix("ib")
        .or_else(|| lower.strip_suffix('b'))
        .or_else(|| lower.strip_suffix('i'))
        .unwrap_or(&lower);
    trimmed.to_string()
}

impl fmt::Display for MemorySize {
    /// Canonical short form: largest unit that divides evenly. So
    /// `MemorySize::from_gib(16)` → `"16G"`, `from_mib(512)` → `"512M"`,
    /// `from_bytes(1024)` → `"1K"`. Falls back to raw bytes if nothing
    /// divides cleanly (rare for human-set values).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = self.bytes;
        if b == 0 {
            return f.write_str("0");
        }
        for (mult, suffix) in [(TIB, "T"), (GIB, "G"), (MIB, "M"), (KIB, "K")] {
            if b % mult == 0 {
                return write!(f, "{}{}", b / mult, suffix);
            }
        }
        write!(f, "{b}")
    }
}

impl Serialize for MemorySize {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MemorySize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(MemorySizeVisitor)
    }
}

struct MemorySizeVisitor;
impl Visitor<'_> for MemorySizeVisitor {
    type Value = MemorySize;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(r#"a byte count or sized string like "16G", "512M", "1024K""#)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        MemorySize::from_str(v).map_err(de::Error::custom)
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        // Allow raw integer bytes for forward-compat / tooling that
        // produces JSON numbers rather than strings.
        Ok(MemorySize::from_bytes(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        if v < 0 {
            return Err(de::Error::custom("memory size must be non-negative"));
        }
        Ok(MemorySize::from_bytes(v as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plain_bytes() {
        assert_eq!(
            MemorySize::from_str("0")
                .unwrap()
                .as_bytes(),
            0
        );
        assert_eq!(
            MemorySize::from_str("42")
                .unwrap()
                .as_bytes(),
            42
        );
    }

    #[test]
    fn parse_suffixed_binary() {
        assert_eq!(
            MemorySize::from_str("1K")
                .unwrap()
                .as_bytes(),
            1024
        );
        assert_eq!(
            MemorySize::from_str("16G")
                .unwrap()
                .as_bytes(),
            16 * GIB
        );
        assert_eq!(
            MemorySize::from_str("512M")
                .unwrap()
                .as_bytes(),
            512 * MIB
        );
        assert_eq!(
            MemorySize::from_str("2T")
                .unwrap()
                .as_bytes(),
            2 * TIB
        );
    }

    #[test]
    fn parse_accepts_iec_variations() {
        // All of these are 16 GiB. Different humans write it different ways.
        for s in ["16G", "16g", "16GB", "16gb", "16Gi", "16gi", "16GiB", "16gib"] {
            assert_eq!(
                MemorySize::from_str(s)
                    .unwrap()
                    .as_bytes(),
                16 * GIB,
                "for {s}"
            );
        }
    }

    #[test]
    fn parse_strips_whitespace() {
        assert_eq!(
            MemorySize::from_str("  16G  ")
                .unwrap()
                .as_bytes(),
            16 * GIB
        );
    }

    #[test]
    fn parse_rejects_empty_and_unknown_suffix() {
        assert!(matches!(MemorySize::from_str(""), Err(MemorySizeParseError::Empty)));
        assert!(matches!(MemorySize::from_str("   "), Err(MemorySizeParseError::Empty)));
        assert!(matches!(MemorySize::from_str("16X"), Err(MemorySizeParseError::UnknownSuffix(_))));
        // Decimal is not supported; we want a clear error rather than
        // parsing "1.5G" as 1 GiB silently.
        assert!(MemorySize::from_str("1.5G").is_err());
        // Pure-suffix with no number isn't valid either.
        assert!(MemorySize::from_str("G").is_err());
    }

    #[test]
    fn display_picks_largest_clean_unit() {
        assert_eq!(MemorySize::from_gib(16).to_string(), "16G");
        assert_eq!(MemorySize::from_mib(512).to_string(), "512M");
        assert_eq!(MemorySize::from_kib(1).to_string(), "1K");
        assert_eq!(MemorySize::from_bytes(0).to_string(), "0");
        // Non-power-of-1024: falls back to bytes.
        assert_eq!(MemorySize::from_bytes(1500).to_string(), "1500");
    }

    #[test]
    fn round_trip_via_display() {
        for input in ["16G", "8G", "512M", "1024K"] {
            let parsed = MemorySize::from_str(input).unwrap();
            let displayed = parsed.to_string();
            let reparsed = MemorySize::from_str(&displayed).unwrap();
            assert_eq!(parsed, reparsed, "round-trip mismatch for {input}");
        }
    }

    #[test]
    fn serde_string_form() {
        // Real TOML: `field = "16G"` → MemorySize.
        let v: MemorySize = toml::from_str::<toml::Table>("field = \"16G\"\n")
            .unwrap()
            .remove("field")
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(v.as_bytes(), 16 * GIB);
    }

    #[test]
    fn serde_integer_form() {
        // Also accept a bare integer (interpreted as bytes) — useful
        // for tooling that emits JSON numbers rather than strings.
        let v: MemorySize = serde_json::from_str("16777216").unwrap();
        assert_eq!(v.as_bytes(), 16 * MIB);
    }
}
