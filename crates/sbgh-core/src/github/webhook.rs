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

/// GH user/account reference shipped on the `sender` of every event and
/// on PR `user` (author). Slice 6 reads `id` + `account_type` for the
/// `github_user` upsert; pre-slice-6 only `login` was needed. GitHub
/// returns `"User"` / `"Organization"` / `"Bot"` for `type`; we keep
/// the raw string here and translate at the classifier boundary so
/// payloads with unexpected casing surface a clear error rather than
/// failing deserialisation silently.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct User {
    pub id: i64,
    pub login: String,
    #[serde(rename = "type")]
    pub account_type: String,
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

// ─── Slice 5: pull_request / push / create webhook payloads ────────────

/// `pull_request.{opened,reopened,synchronize,...}` event payload.
/// Slice 5 reads `action`, `installation.id`, and the head/base repo
/// identities (for target+source policy evaluation). The PR's own ref
/// + SHA is captured for slice 7+ when we materialise PR subject rows.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestEvent {
    pub action: String,
    pub installation: Installation,
    pub repository: PullRequestRepo,
    pub pull_request: PullRequestBody,
    /// Slice 7 review fix: GH includes a `changes` object on
    /// `pull_request.edited` describing which fields the user actually
    /// modified. Title-only edits should NOT re-run policy eval
    /// (otherwise a typo fix becomes a `WouldEnqueueJob` signal, which
    /// turns into a real "title edit starts benchmark" bug once slice
    /// 9 flips the outcome to job creation). Re-eval only when the
    /// PR's base ref actually changed.
    #[serde(default)]
    pub changes: Option<PullRequestChanges>,
}

/// Slice 7 review fix: the `changes` object on `pull_request.edited`.
/// Only `base` is meaningful for policy eval — a base ref change can
/// shift the target repo identity. Title/body/etc. edits are absorbed
/// by the upsert path but do not warrant re-running policy.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestChanges {
    pub base: Option<serde_json::Value>,
}

/// Subset of GitHub's repository field that PR-related webhooks ship
/// at top level. Includes `id` (which slice 4's `IssueCommentEvent`'s
/// existing `Repository` deliberately omits — we don't widen that
/// struct to avoid breaking its test fixtures).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestRepo {
    pub id: i64,
    pub full_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestBody {
    pub number: i64,
    pub head: PullRequestBranchRef,
    pub base: PullRequestBranchRef,
    /// PR author. Slice 6 upserts this into `github_user` so slice 7's
    /// `github_pull_request.author_github_user_id` FK target exists.
    pub user: User,
    /// PR title. Slice 7 persists this as `github_pull_request.title`
    /// and refreshes on `pull_request.edited`.
    pub title: String,
}

/// PR head/base entry. `repo` is `Option` because GitHub may omit it
/// when a PR's branch was deleted from a fork (rare, but documented).
/// The handler treats a missing repo as a payload error.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PullRequestBranchRef {
    /// Branch name (GitHub field is `ref`; Rust keyword conflict so
    /// we rename via serde).
    #[serde(rename = "ref")]
    pub branch: String,
    pub sha: String,
    pub repo: Option<PullRequestRepo>,
}

/// `push` event payload. Slice 5 reads `ref` (the pushed branch path,
/// `"refs/heads/<name>"`), `repository.id`, and `installation.id`.
/// `forced` lets later slices distinguish push from force-push if
/// needed; not used in slice 5.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PushEvent {
    /// Full ref path, e.g. `"refs/heads/develop"`. The handler strips
    /// the `refs/heads/` prefix before comparing to a
    /// `TriggerMatchSpec::BranchPush.branch_name`.
    #[serde(rename = "ref")]
    pub ref_field: String,
    pub installation: Installation,
    pub repository: PullRequestRepo,
}

/// `create` event payload (fires on branch + tag creation). The
/// `ref_type` field distinguishes them; slice 5 evaluates
/// `trigger_kind = 'tag_created'` only when `ref_type == "tag"`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateEvent {
    /// Branch or tag name (NOT `refs/heads/...` prefixed — `create`
    /// payloads send the short name).
    #[serde(rename = "ref")]
    pub ref_field: String,
    /// `"tag"` or `"branch"` — slice 5 only acts on `"tag"`.
    pub ref_type: String,
    pub installation: Installation,
    pub repository: PullRequestRepo,
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
