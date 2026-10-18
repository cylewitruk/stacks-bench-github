//! Bounded negotiation conversion at the protobuf boundary.

use std::collections::BTreeSet;

use sbgh_fleet::{
    PREVIOUS_PROTOCOL_REVISION, ProtocolContract, ProtocolError, ProtocolFeature, ProtocolOffer,
};

use super::{Wire, invalid, protocol_version, wire};

impl Wire for wire::ProtocolOffer {
    type Domain = ProtocolOffer;

    fn into_domain(self) -> Result<Self::Domain, ProtocolError> {
        let count = self.features.len();
        let offer = ProtocolOffer {
            minimum_revision: protocol_version(self.minimum_revision)?,
            maximum_revision: protocol_version(self.maximum_revision)?,
            features: self
                .features
                .into_iter()
                .collect(),
        };
        if offer.features.len() != count {
            return Err(invalid("protocol_offer.features", "duplicate feature name"));
        }
        offer
            .validate()
            .map_err(|error| invalid("protocol_offer", error.to_string()))?;
        Ok(offer)
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            minimum_revision: u32::from(value.minimum_revision),
            maximum_revision: u32::from(value.maximum_revision),
            features: value
                .features
                .into_iter()
                .collect(),
        }
    }
}

impl Wire for wire::ProtocolContract {
    type Domain = ProtocolContract;

    fn into_domain(self) -> Result<Self::Domain, ProtocolError> {
        let count = self.features.len();
        let features = self
            .features
            .into_iter()
            .map(|name| {
                ProtocolFeature::from_name(&name).ok_or_else(|| {
                    invalid("protocol_contract.features", "unrecognized selected feature")
                })
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if features.len() != count {
            return Err(invalid("protocol_contract.features", "duplicate selected feature"));
        }
        let contract = ProtocolContract {
            revision: protocol_version(self.revision)?,
            features,
        };
        ProtocolOffer::current()
            .validate_selection(&contract)
            .map_err(|error| invalid("protocol_contract", error.to_string()))?;
        Ok(contract)
    }

    fn from_domain(value: Self::Domain) -> Self {
        Self {
            revision: u32::from(value.revision),
            features: value
                .features
                .into_iter()
                .map(|feature| feature.as_str().into())
                .collect(),
        }
    }
}

/// Require agreement between the legacy response field and the explicit contract.
pub fn selected(
    version: u32,
    value: Option<wire::ProtocolContract>,
) -> Result<Option<ProtocolContract>, ProtocolError> {
    let revision = protocol_version(version)?;
    let Some(value) = value else {
        return if revision == PREVIOUS_PROTOCOL_REVISION {
            Ok(None)
        } else {
            Err(invalid("protocol_contract", "required for negotiated revisions"))
        };
    };
    let contract = value.into_domain()?;
    if contract.revision != revision {
        return Err(invalid("protocol_contract.revision", "does not match protocol_version"));
    }
    Ok(Some(contract))
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use uuid::Uuid;

    use super::*;
    use sbgh_fleet::{
        PreflightReport, RegisterSessionRequest, RegisterSessionResponse, ResourceFacts, Validate,
        WorkerCapability,
    };

    #[test]
    fn negotiation_registration_round_trips_both_wire_cohorts() {
        for extension in [None, Some(ProtocolOffer::current())] {
            let revision = extension
                .as_ref()
                .map_or(1, |offer| offer.maximum_revision);
            let request = RegisterSessionRequest {
                preflight: extension
                    .as_ref()
                    .map(|_| PreflightReport {
                        revision: 1,
                        completed_at_ms: 1_792_000_000_000,
                        executable_sha256: "ab".repeat(32),
                        configuration_sha256: "cd".repeat(32),
                    }),
                protocol_version: revision,
                protocol_offer: extension.clone(),
                worker_session_id: Uuid::new_v4(),
                software_version: "test".into(),
                advertised_capabilities: [WorkerCapability::BuildOnly].into(),
                resources: ResourceFacts {
                    logical_cpus: 2,
                    memory_bytes: 8 * 1024 * 1024 * 1024,
                },
            };
            request.validate().unwrap();
            let bytes = wire::RegisterRequest::from_domain(request.clone()).encode_to_vec();
            let decoded = wire::RegisterRequest::decode(bytes.as_slice())
                .unwrap()
                .into_domain()
                .unwrap();
            assert_eq!(decoded, request);
            let selected = ProtocolOffer::registration(revision, extension.as_ref())
                .unwrap()
                .negotiate(1)
                .unwrap();
            let response = RegisterSessionResponse {
                protocol_version: selected.revision,
                protocol_contract: extension.map(|_| selected),
                heartbeat_interval_ms: 10_000,
                lease_ttl_ms: 45_000,
                server_time_ms: 1_792_000_000_000,
            };
            let bytes = wire::RegisterResponse::from_domain(response.clone()).encode_to_vec();
            assert_eq!(
                wire::RegisterResponse::decode(bytes.as_slice())
                    .unwrap()
                    .into_domain()
                    .unwrap(),
                response
            );
        }
    }

    #[test]
    fn negotiation_wire_rejects_unknown_selections_duplicates_and_mismatched_revisions() {
        let contract = ProtocolOffer::current()
            .negotiate(1)
            .unwrap();
        assert!(selected(1, Some(wire::ProtocolContract::from_domain(contract))).is_err());
        assert!(selected(2, None).is_err());
        assert_eq!(selected(1, None).unwrap(), None);
        for features in [vec!["unknown_feature".into()], vec!["build_only_v1".into(); 2]] {
            assert!(
                wire::ProtocolContract { revision: 2, features }
                    .into_domain()
                    .is_err()
            );
        }
        assert!(
            wire::ProtocolOffer {
                minimum_revision: 1,
                maximum_revision: 2,
                features: vec!["build_only_v1".into(); 2]
            }
            .into_domain()
            .is_err()
        );
        assert!(
            wire::ProtocolOffer {
                minimum_revision: 1,
                maximum_revision: u32::MAX,
                features: vec![]
            }
            .into_domain()
            .is_err()
        );
        let request = wire::RegisterRequest {
            preflight: None,
            protocol_version: 1,
            worker_session_id: Uuid::new_v4().to_string(),
            software_version: "test".into(),
            advertised_capabilities: vec![wire::WorkerCapability::BuildOnly as i32],
            resources: Some(wire::ResourceFacts {
                logical_cpus: 2,
                memory_bytes: 1024,
            }),
            protocol_offer: Some(wire::ProtocolOffer::from_domain(ProtocolOffer::current())),
        };
        assert!(
            request
                .into_domain()
                .unwrap()
                .validate()
                .is_err()
        );
    }
}
