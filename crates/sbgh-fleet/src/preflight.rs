//! Immutable, self-reported evidence of successful local readiness checks.

use serde::{Deserialize, Serialize};

use crate::{ProtocolError, Validate};

/// Worker-reported startup checks, not remote attestation or continuing health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightReport {
    /// Version of the check suite and configuration fingerprint encoding.
    pub revision: u32,
    /// Worker-clock Unix milliseconds when the checks completed successfully.
    pub completed_at_ms: i64,
    /// SHA-256 of the running executable, not the replacement at its install path.
    pub executable_sha256: String,
    /// SHA-256 of the loaded configuration's revision-specific encoding.
    pub configuration_sha256: String,
}

impl Validate for PreflightReport {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.revision == 0 || self.completed_at_ms <= 0 {
            return Err(ProtocolError::Invalid {
                field: "preflight",
                reason: "revision and completion timestamp must be positive".into(),
            });
        }
        for digest in [&self.executable_sha256, &self.configuration_sha256] {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(ProtocolError::Invalid {
                    field: "preflight",
                    reason: "digests must be lowercase SHA-256 hexadecimal".into(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preflight_report_validates_bounded_facts_without_claiming_supported_revision() {
        let report = PreflightReport {
            revision: 2,
            completed_at_ms: 1,
            executable_sha256: "ab".repeat(32),
            configuration_sha256: "cd".repeat(32),
        };
        report.validate().unwrap();
        for invalid in [
            PreflightReport { revision: 0, ..report.clone() },
            PreflightReport {
                completed_at_ms: 0,
                ..report.clone()
            },
            PreflightReport {
                executable_sha256: "AB".repeat(32),
                ..report.clone()
            },
            PreflightReport {
                configuration_sha256: "a".repeat(65),
                ..report
            },
        ] {
            std::assert_matches!(
                invalid.validate(),
                Err(ProtocolError::Invalid { field: "preflight", .. })
            );
        }
    }
}
