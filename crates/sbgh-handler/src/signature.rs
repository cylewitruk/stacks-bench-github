use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
#[error("invalid webhook signature")]
pub struct InvalidSignature;

pub fn verify_signature(
    secret: &str,
    body: &[u8],
    header_value: &str,
) -> Result<(), InvalidSignature> {
    let provided_hex = header_value
        .strip_prefix("sha256=")
        .ok_or(InvalidSignature)?;
    let provided = hex::decode(provided_hex).map_err(|_| InvalidSignature)?;

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| InvalidSignature)?;
    mac.update(body);
    let expected = mac.finalize().into_bytes();
    let expected_bytes: &[u8] = expected.as_ref();
    if bool::from(expected_bytes.ct_eq(&provided)) { Ok(()) } else { Err(InvalidSignature) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_matching_signature() {
        let secret = "supersecret";
        let body = b"{\"hello\":\"world\"}";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        assert!(verify_signature(secret, body, &signature).is_ok());
    }

    #[test]
    fn rejects_mismatch_and_missing_prefix() {
        assert!(verify_signature("s", b"body", "sha256=deadbeef").is_err());
        assert!(verify_signature("s", b"body", "abc123").is_err());
    }
}
