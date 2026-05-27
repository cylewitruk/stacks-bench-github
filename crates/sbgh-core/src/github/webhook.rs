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

/// Subset of the `installation` webhook payload the processor needs.
/// GitHub's full payload is large; we deserialise only the fields the
/// slice 3 / 4 handlers actually read. `installation.account` is the
/// GH account that installed the App — that's what we check against
/// `allowed_installer`.
///
/// `repositories` is GitHub's "repos this install can access" list
/// included on `installation.created` (and absent on the other
/// actions; `#[serde(default)]` so suspend/unsuspend/deleted don't
/// fail to parse). Slice 4 materialises these as initial memberships
/// at create time — without this, a fresh install would have no
/// `github_installation_repo` rows until a later
/// `installation_repositories.added` event happened.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationEvent {
    pub action: String,
    pub installation: InstallationDetails,
    #[serde(default)]
    pub repositories: Vec<InstallationRepository>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationDetails {
    pub id: i64,
    pub account: InstallationAccount,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationAccount {
    pub id: i64,
    pub login: String,
    /// GitHub returns `"User"`, `"Organization"`, or `"Bot"`. Lowercased
    /// in the model layer via `GithubAccountType` — we keep the raw
    /// string here and translate at the classifier boundary.
    #[serde(rename = "type")]
    pub account_type: String,
}

/// `installation_repositories.{added,removed}` event payload. GitHub
/// includes one OR BOTH of `repositories_added` / `repositories_removed`
/// depending on action; the handler reads whichever is relevant. The
/// repo objects here are identity-only — we fetch full lineage from
/// `/repos/{owner}/{repo}` separately to capture parent/source.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationRepositoriesEvent {
    pub action: String,
    pub installation: InstallationDetails,
    #[serde(default)]
    pub repositories_added: Vec<InstallationRepository>,
    #[serde(default)]
    pub repositories_removed: Vec<InstallationRepository>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InstallationRepository {
    pub id: i64,
    /// `"owner/name"` form. We split this at the handler boundary
    /// rather than asking GitHub to send separate owner/name fields
    /// (which it doesn't in this payload shape).
    pub full_name: String,
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
