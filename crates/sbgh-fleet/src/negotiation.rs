//! Bounded revision selection and explicit task-semantic support.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{TaskPayload, WorkerCapability};

/// Pinned `0f18950` contract, not every historical binary reporting revision 1.
pub const PREVIOUS_PROTOCOL_REVISION: u16 = 1;
/// Revision introducing explicit feature negotiation, independent of lifecycle behavior.
pub const NEGOTIATED_PROTOCOL_REVISION: u16 = 2;

const MAX_FEATURES: usize = 64;
const MAX_FEATURE_BYTES: usize = 64;

/// Explicit task and lifecycle semantics understood by this daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolFeature {
    /// Existing build-only assignment and result contract.
    BuildOnlyV1,
    /// Existing benchmark assignment, repeats, calibration and result contract.
    BenchmarkV1,
    /// Existing monolithic dynamic-validation selector and result contract.
    BlockValidationV1,
    /// Cordon keeps the process resident and polling; it does not imply exit.
    ResidentCordon,
    /// Explicit idle shutdown targets only the negotiated worker session.
    ShutdownAfterIdle,
}

impl ProtocolFeature {
    /// Required semantics for an advertised task capability.
    pub const fn for_capability(capability: WorkerCapability) -> Self {
        match capability {
            WorkerCapability::BuildOnly => Self::BuildOnlyV1,
            WorkerCapability::Benchmark => Self::BenchmarkV1,
            WorkerCapability::BlockValidation => Self::BlockValidationV1,
        }
    }

    /// Parse a selected feature; unknown selections must never be enabled.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::supported()
            .into_iter()
            .find(|feature| feature.as_str() == name)
    }
    /// Stable wire/database feature name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BuildOnlyV1 => "build_only_v1",
            Self::BenchmarkV1 => "benchmark_v1",
            Self::BlockValidationV1 => "block_validation_v1",
            Self::ResidentCordon => "resident_cordon",
            Self::ShutdownAfterIdle => "shutdown_after_idle",
        }
    }

    /// Feature required in addition to the worker's task capability.
    pub const fn for_payload(payload: &TaskPayload) -> Self {
        match payload {
            TaskPayload::BuildOnly => Self::BuildOnlyV1,
            TaskPayload::Benchmark(_) => Self::BenchmarkV1,
            TaskPayload::BlockValidation(_) => Self::BlockValidationV1,
        }
    }

    /// All currently implemented optional semantics.
    fn supported() -> BTreeSet<Self> {
        [
            Self::BuildOnlyV1,
            Self::BenchmarkV1,
            Self::BlockValidationV1,
            Self::ResidentCordon,
            Self::ShutdownAfterIdle,
        ]
        .into()
    }

    /// Fixed deployed revision-1 semantics; never add features here.
    fn legacy() -> BTreeSet<Self> {
        [Self::BuildOnlyV1, Self::BenchmarkV1, Self::BlockValidationV1].into()
    }
}

/// Worker-advertised range and optional features; unknown features are not enabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolOffer {
    /// Lowest revision the worker can execute with that revision's full semantics.
    pub minimum_revision: u16,
    /// Highest revision the worker can execute.
    pub maximum_revision: u16,
    /// Features implemented by the worker for explicitly negotiated revisions.
    pub features: BTreeSet<String>,
}

/// Persisted result of negotiation, immutable for a worker session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolContract {
    /// Selected revision within the mutually supported window.
    pub revision: u16,
    /// Enabled semantics; never inferred from software version or task capability.
    pub features: BTreeSet<ProtocolFeature>,
}

/// Permanent negotiation failure; caller must not retry by silently downgrading.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NegotiationError {
    /// The advertised range or feature list is malformed.
    #[error("invalid protocol advertisement: {0}")]
    InvalidOffer(&'static str),
    /// Local compatibility policy is outside this binary's supported window.
    #[error("unsupported compatibility floor {0}; expected 1 or 2")]
    InvalidFloor(u16),
    /// No shared revision exists; operator action or a worker upgrade is required.
    #[error(
        "worker protocol range {minimum}..={maximum} does not overlap daemon range {floor}..=2; upgrade the worker or deploy a compatible daemon"
    )]
    Incompatible {
        /// Worker minimum revision.
        minimum: u16,
        /// Worker maximum revision.
        maximum: u16,
        /// Daemon compatibility floor.
        floor: u16,
    },
    /// The daemon selected a contract the worker did not offer.
    #[error("daemon selected an unadvertised protocol revision or feature")]
    InvalidSelection,
}

impl ProtocolOffer {
    /// Validate the response extension, retaining implicit semantics only for legacy requests.
    pub fn response(
        &self,
        revision: u16,
        extension: Option<&ProtocolContract>,
    ) -> Result<ProtocolContract, NegotiationError> {
        let selected = match extension {
            Some(contract) if contract.revision == revision => contract.clone(),
            None if self.maximum_revision == PREVIOUS_PROTOCOL_REVISION
                && revision == PREVIOUS_PROTOCOL_REVISION =>
            {
                Self::legacy(revision)?.negotiate(PREVIOUS_PROTOCOL_REVISION)?
            }
            _ => return Err(NegotiationError::InvalidSelection),
        };
        self.validate_selection(&selected)?;
        Ok(selected)
    }
    /// Reconcile the legacy version field with the optional negotiation extension.
    pub fn registration(version: u16, extension: Option<&Self>) -> Result<Self, NegotiationError> {
        let Some(offer) = extension else {
            return Self::legacy(version);
        };
        offer.validate()?;
        if version != offer.maximum_revision || version == PREVIOUS_PROTOCOL_REVISION {
            return Err(NegotiationError::InvalidOffer(
                "negotiation requires a matching maximum revision greater than 1",
            ));
        }
        Ok(offer.clone())
    }
    /// Advertise the negotiated revision and compatibility with the pinned predecessor.
    pub fn current() -> Self {
        Self {
            minimum_revision: PREVIOUS_PROTOCOL_REVISION,
            maximum_revision: NEGOTIATED_PROTOCOL_REVISION,
            features: ProtocolFeature::supported()
                .into_iter()
                .map(|feature| feature.as_str().into())
                .collect(),
        }
    }

    /// Interpret an absent negotiation extension only for the pinned revision-1 shape.
    pub fn legacy(revision: u16) -> Result<Self, NegotiationError> {
        if revision != PREVIOUS_PROTOCOL_REVISION {
            return Err(NegotiationError::InvalidOffer(
                "negotiation extension required after revision 1",
            ));
        }
        Ok(Self {
            minimum_revision: revision,
            maximum_revision: revision,
            features: BTreeSet::new(),
        })
    }

    /// Validate bounded peer input without treating unknown optional features as errors.
    pub fn validate(&self) -> Result<(), NegotiationError> {
        if self.minimum_revision == 0 || self.minimum_revision > self.maximum_revision {
            return Err(NegotiationError::InvalidOffer(
                "revision range must be nonzero and ordered",
            ));
        }
        if self.features.len() > MAX_FEATURES
            || self
                .features
                .iter()
                .any(|name| {
                    name.is_empty()
                        || name.len() > MAX_FEATURE_BYTES
                        || !name.bytes().all(|byte| {
                            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
                        })
                })
        {
            return Err(NegotiationError::InvalidOffer(
                "feature names must be bounded lowercase ASCII identifiers",
            ));
        }
        if self.maximum_revision == PREVIOUS_PROTOCOL_REVISION && !self.features.is_empty() {
            return Err(NegotiationError::InvalidOffer(
                "revision 1 has fixed semantics, not feature negotiation",
            ));
        }
        Ok(())
    }

    /// Select the highest shared revision and intersection of implemented features.
    pub fn negotiate(&self, floor: u16) -> Result<ProtocolContract, NegotiationError> {
        self.validate()?;
        if !(PREVIOUS_PROTOCOL_REVISION..=NEGOTIATED_PROTOCOL_REVISION).contains(&floor) {
            return Err(NegotiationError::InvalidFloor(floor));
        }
        let revision = self
            .maximum_revision
            .min(NEGOTIATED_PROTOCOL_REVISION);
        if revision
            < self
                .minimum_revision
                .max(floor)
        {
            return Err(NegotiationError::Incompatible {
                minimum: self.minimum_revision,
                maximum: self.maximum_revision,
                floor,
            });
        }
        let features = self.features_at(revision);
        Ok(ProtocolContract { revision, features })
    }

    /// Check a daemon response before any assignment or local resource creation.
    pub fn validate_selection(&self, selected: &ProtocolContract) -> Result<(), NegotiationError> {
        self.validate()?;
        if !(self.minimum_revision..=self.maximum_revision).contains(&selected.revision)
            || !(PREVIOUS_PROTOCOL_REVISION..=NEGOTIATED_PROTOCOL_REVISION)
                .contains(&selected.revision)
            || !selected
                .features
                .is_subset(&self.features_at(selected.revision))
            || (selected.revision == PREVIOUS_PROTOCOL_REVISION
                && selected.features != ProtocolFeature::legacy())
        {
            return Err(NegotiationError::InvalidSelection);
        }
        Ok(())
    }

    /// Revision 1 promises its entire fixed contract; later revisions opt into features.
    fn features_at(&self, revision: u16) -> BTreeSet<ProtocolFeature> {
        let supported = ProtocolFeature::supported();
        if revision == PREVIOUS_PROTOCOL_REVISION {
            ProtocolFeature::legacy()
        } else {
            supported
                .into_iter()
                .filter(|feature| {
                    self.features
                        .contains(feature.as_str())
                })
                .collect()
        }
    }
}

impl ProtocolContract {
    /// Whether a task capability's semantics are negotiated, independent of policy.
    pub fn supports_capability(&self, capability: WorkerCapability) -> bool {
        (PREVIOUS_PROTOCOL_REVISION..=NEGOTIATED_PROTOCOL_REVISION).contains(&self.revision)
            && self
                .features
                .contains(&ProtocolFeature::for_capability(capability))
    }

    /// Whether this contract can execute the payload, independently of authorization.
    pub fn supports_payload(&self, payload: &TaskPayload) -> bool {
        (PREVIOUS_PROTOCOL_REVISION..=NEGOTIATED_PROTOCOL_REVISION).contains(&self.revision)
            && self
                .features
                .contains(&ProtocolFeature::for_payload(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Status and offer admission share task-feature and revision checks.
    #[test]
    fn capability_support_requires_both_supported_revision_and_feature() {
        let mut contract = ProtocolOffer::current()
            .negotiate(1)
            .unwrap();
        assert!(contract.supports_capability(WorkerCapability::BuildOnly));
        contract
            .features
            .remove(&ProtocolFeature::for_capability(WorkerCapability::BuildOnly));
        assert!(!contract.supports_capability(WorkerCapability::BuildOnly));
        assert!(contract.supports_capability(WorkerCapability::Benchmark));
        contract.revision = NEGOTIATED_PROTOCOL_REVISION + 1;
        assert!(!contract.supports_capability(WorkerCapability::Benchmark));
    }

    #[test]
    fn negotiation_preserves_fixed_previous_contract_and_selects_current() {
        let legacy = ProtocolOffer::legacy(1).unwrap();
        let contract = legacy.negotiate(1).unwrap();
        assert_eq!(contract.revision, 1);
        assert_eq!(contract.features, ProtocolFeature::legacy());
        legacy
            .validate_selection(&contract)
            .unwrap();
        let current = ProtocolOffer::current();
        let negotiated = current.negotiate(1).unwrap();
        assert_eq!(negotiated.revision, 2);
        current
            .validate_selection(&negotiated)
            .unwrap();
        current
            .validate_selection(&contract)
            .unwrap();
    }

    #[test]
    fn negotiation_floor_rejects_previous_without_silent_downgrade() {
        std::assert_matches!(
            ProtocolOffer::legacy(1)
                .unwrap()
                .negotiate(2),
            Err(NegotiationError::Incompatible { .. })
        );
        assert_eq!(
            ProtocolOffer::current()
                .negotiate(2)
                .unwrap()
                .revision,
            2
        );
        for floor in [0, 3, u16::MAX] {
            assert_eq!(
                ProtocolOffer::current().negotiate(floor),
                Err(NegotiationError::InvalidFloor(floor))
            );
        }
        for revision in [0, 2, u16::MAX] {
            std::assert_matches!(
                ProtocolOffer::legacy(revision),
                Err(NegotiationError::InvalidOffer(_))
            );
        }
    }

    #[test]
    fn negotiation_intersects_features_and_does_not_invent_task_support() {
        let mut offer = ProtocolOffer::current();
        offer.features = ["build_only_v1".into(), "future_feature".into()].into();
        let contract = offer.negotiate(1).unwrap();
        assert_eq!(contract.features, [ProtocolFeature::BuildOnlyV1].into());
        assert!(contract.supports_payload(&TaskPayload::BuildOnly));
        assert!(
            !contract
                .features
                .contains(&ProtocolFeature::BenchmarkV1)
        );
        offer.features.clear();
        assert!(
            !offer
                .negotiate(1)
                .unwrap()
                .supports_payload(&TaskPayload::BuildOnly)
        );
    }

    #[test]
    fn negotiation_rejects_unadvertised_response_features_and_revisions() {
        let mut offer = ProtocolOffer::current();
        offer.features.clear();
        let mut selected = offer.negotiate(1).unwrap();
        selected
            .features
            .insert(ProtocolFeature::BuildOnlyV1);
        assert_eq!(offer.validate_selection(&selected), Err(NegotiationError::InvalidSelection));
        selected.features.clear();
        selected.revision = 3;
        assert_eq!(offer.validate_selection(&selected), Err(NegotiationError::InvalidSelection));
        selected.revision = 1;
        assert_eq!(offer.validate_selection(&selected), Err(NegotiationError::InvalidSelection));
    }

    #[test]
    fn negotiation_validates_bounds_and_supports_future_overlapping_ranges() {
        let mut offer = ProtocolOffer::current();
        offer.maximum_revision = 3;
        assert_eq!(
            offer
                .negotiate(1)
                .unwrap()
                .revision,
            2
        );
        offer.minimum_revision = 3;
        std::assert_matches!(offer.negotiate(1), Err(NegotiationError::Incompatible { .. }));
        for (minimum, maximum) in [(0, 2), (2, 1)] {
            offer.minimum_revision = minimum;
            offer.maximum_revision = maximum;
            std::assert_matches!(offer.validate(), Err(NegotiationError::InvalidOffer(_)));
        }
        for feature in ["", "x\n", "UPPER", &"x".repeat(65)] {
            let mut offer = ProtocolOffer::current();
            offer
                .features
                .insert(feature.into());
            std::assert_matches!(offer.validate(), Err(NegotiationError::InvalidOffer(_)));
        }
        let mut offer = ProtocolOffer::current();
        offer.features = (0..65)
            .map(|n| format!("feature_{n}"))
            .collect();
        std::assert_matches!(offer.validate(), Err(NegotiationError::InvalidOffer(_)));
        let mut legacy = ProtocolOffer::legacy(1).unwrap();
        legacy
            .features
            .insert("build_only_v1".into());
        std::assert_matches!(legacy.validate(), Err(NegotiationError::InvalidOffer(_)));
    }
}
