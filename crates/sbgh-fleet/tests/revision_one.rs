//! Frozen semantic identities from the deployed revision-1 contract.

use sbgh_fleet::{ReliableEventPayload, TaskPayload, TerminalOutcome, payload_digest};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

const DIGESTS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/fleet/revision-1/semantic-digests.json"
));
const SCHEMA: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/fleet/revision-1/proto/sbgh/fleet/v1/fleet.proto"
));

/// Provenance and fixed vectors; tests never regenerate expected values.
#[derive(Deserialize)]
struct Fixtures {
    /// Immutable source used to produce these identities.
    source_commit: String,
    /// Revision reported by the deployed producer.
    protocol_revision: u16,
    /// Each hashed domain variant and its expected identity.
    cases: Vec<Case>,
}

/// One serializer-sensitive semantic identity.
#[derive(Deserialize)]
struct Case {
    /// Diagnostic fixture name.
    name: String,
    /// Domain type to deserialize before hashing.
    kind: String,
    /// Historical serialized value, including nulls and collection order.
    value: Value,
    /// Digest emitted by the unmodified deployed serializer.
    sha256: String,
}

#[test]
fn revision_one_semantic_digests_remain_stable() {
    let fixtures: Fixtures = serde_json::from_str(DIGESTS).unwrap();
    assert_eq!(fixtures.source_commit, "0f18950ad086799015fd1af0d66a7e672a3e5e27");
    assert_eq!(fixtures.protocol_revision, 1);
    assert_eq!(fixtures.cases.len(), 14);
    for case in fixtures.cases {
        let (actual, serialized) = match case.kind.as_str() {
            "task" => {
                let value: TaskPayload = serde_json::from_value(case.value.clone()).unwrap();
                (payload_digest(&value).unwrap(), serde_json::to_value(value).unwrap())
            }
            "event" => {
                let value: ReliableEventPayload =
                    serde_json::from_value(case.value.clone()).unwrap();
                (payload_digest(&value).unwrap(), serde_json::to_value(value).unwrap())
            }
            "outcome" => {
                let value: TerminalOutcome = serde_json::from_value(case.value.clone()).unwrap();
                (payload_digest(&value).unwrap(), serde_json::to_value(value).unwrap())
            }
            other => panic!("unknown frozen domain {other}"),
        };
        assert_eq!(serialized, case.value, "{} changed representation", case.name);
        assert_eq!(actual, case.sha256, "{} changed semantic identity", case.name);
    }
}

#[test]
fn revision_one_baseline_files_are_immutable() {
    assert_eq!(
        hex::encode(Sha256::digest(SCHEMA)),
        "4f111bc214f6990f210ab2965f38189f1fba541fd47c6569e69c73fb48eb12bf"
    );
    assert_eq!(
        hex::encode(Sha256::digest(DIGESTS.as_bytes())),
        "9b8ca30c83d98c8a3b3ed20a0e9be62daa1e504f5fa48c91748d934335d35b2f"
    );
}
