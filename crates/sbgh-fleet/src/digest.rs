use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::ProtocolError;

/// Serialize JSON with recursively sorted object keys.
///
/// Protocol digests must not depend on map insertion order or serializer
/// implementation details.
fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let value = serde_json::to_value(value)
        .map_err(|error| ProtocolError::Serialization(error.to_string()))?;
    let mut out = Vec::new();
    write_value(&value, &mut out)?;
    Ok(out)
}

pub fn payload_digest<T: Serialize>(value: &T) -> Result<String, ProtocolError> {
    let bytes = canonical_json_bytes(value)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn write_value(value: &Value, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
    match value {
        Value::Object(map) => {
            out.push(b'{');
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, key)
                    .map_err(|error| ProtocolError::Serialization(error.to_string()))?;
                out.push(b':');
                write_value(&map[key], out)?;
            }
            out.push(b'}');
        }
        Value::Array(values) => {
            out.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_value(value, out)?;
            }
            out.push(b']');
        }
        scalar => serde_json::to_writer(&mut *out, scalar)
            .map_err(|error| ProtocolError::Serialization(error.to_string()))?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{BlockValidationPayload, InclusiveRange, TaskPayload, ValidationEpoch};

    #[test]
    fn digest_is_independent_of_object_insertion_order() {
        let left = json!({"b": 2, "a": {"d": 4, "c": 3}});
        let right = json!({"a": {"c": 3, "d": 4}, "b": 2});
        assert_eq!(payload_digest(&left).unwrap(), payload_digest(&right).unwrap());
    }

    #[test]
    fn digest_changes_when_semantic_payload_changes() {
        let original = TaskPayload::BlockValidation(BlockValidationPayload {
            epoch: ValidationEpoch::Nakamoto,
            range: InclusiveRange { start: 10, end: 20 },
            requested_shards: 4,
            max_concurrency: 2,
            timeout_secs: 600,
        });
        let mut changed = original.clone();
        let TaskPayload::BlockValidation(changed) = &mut changed else {
            unreachable!();
        };
        changed.range.end += 1;

        assert_ne!(payload_digest(&original).unwrap(), payload_digest(&changed).unwrap());
    }
}
