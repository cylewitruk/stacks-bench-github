//! GitHub webhook signature verification and event types.
//!
//! GitHub signs webhook deliveries with HMAC-SHA256 over the raw request body
//! using the webhook secret configured on the App. The header is
//! `X-Hub-Signature-256: sha256=<hex>`. The comparison MUST be constant-time
//! to prevent timing attacks.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

/// Verify the `X-Hub-Signature-256` header against the raw body.
pub fn verify_signature(secret: &str, body: &[u8], header_value: &str) -> Result<()> {
    let Some(provided_hex) = header_value.strip_prefix("sha256=") else {
        return Err(Error::InvalidSignature);
    };
    let provided = hex::decode(provided_hex).map_err(|_| Error::InvalidSignature)?;

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| Error::Config("invalid webhook secret".into()))?;
    mac.update(body);
    let expected = mac.finalize().into_bytes();

    let expected_bytes: &[u8] = expected.as_ref();
    if expected_bytes
        .ct_eq(&provided)
        .into()
    {
        Ok(())
    } else {
        Err(Error::InvalidSignature)
    }
}

// --- Event payloads (subset we care about) ---

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IssueCommentEvent {
    pub action: String,
    pub comment: Comment,
    pub issue: Issue,
    pub repository: Repository,
    pub sender: User,
    pub installation: Installation,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Comment {
    pub id: i64,
    pub body: String,
    pub user: User,
    pub author_association: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Issue {
    pub number: i64,
    #[serde(default)]
    pub pull_request: Option<PullRequestRef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestRef {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Repository {
    pub full_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct User {
    pub login: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Installation {
    pub id: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_round_trip() {
        let secret = "supersecret";
        let body = b"{\"hello\":\"world\"}";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let bytes = mac.finalize().into_bytes();
        let sig = format!("sha256={}", hex::encode::<&[u8]>(bytes.as_ref()));
        assert!(verify_signature(secret, body, &sig).is_ok());
    }

    #[test]
    fn signature_mismatch_rejected() {
        assert!(verify_signature("s", b"body", "sha256=deadbeef").is_err());
    }

    #[test]
    fn missing_prefix_rejected() {
        assert!(verify_signature("s", b"body", "abc123").is_err());
    }
}
