//! GitHub App authentication.
//!
//! Two layers:
//!   1. App JWT — RS256-signed by the App private key, lifetime ≤ 10 min.
//!      Identifies the App itself; used only to mint installation tokens.
//!   2. Installation access token — ~1h lifetime, scoped to one installation.
//!      Used for all repository API calls (creating/updating PR comments,
//!      etc.).
//!
//! Installation tokens are cached in memory keyed by `installation_id` and
//! refreshed before they expire, since each mint is an API call.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{GitHubError, GitHubResult};

/// App-level credentials: the App Client ID and its RS256 private key.
///
/// We use the Client ID (e.g. `Iv23li...`) rather than the numeric App ID as
/// the JWT `iss` claim. Both are accepted by GitHub today, but Client ID is
/// the recommended path per GitHub's 2024 guidance and is the only form that
/// works on OAuth-style endpoints, so we'd rather standardise on it now.
#[derive(Clone)]
pub struct AppCredentials {
    client_id: String,
    encoding_key: Arc<EncodingKey>,
}

impl AppCredentials {
    /// Load credentials from a PEM file on disk.
    ///
    /// On Unix, the file must not be readable by group or world — a private
    /// signing key that anyone with shell access can read is essentially the
    /// same as no auth. We refuse to start rather than silently sign with a
    /// compromised key.
    pub fn from_pem_file(client_id: impl Into<String>, path: &Path) -> GitHubResult<Self> {
        check_key_permissions(path)?;
        let pem = std::fs::read(path)?;
        let encoding_key = EncodingKey::from_rsa_pem(&pem)
            .map_err(|e| GitHubError::Config(format!("invalid RSA private key: {e}")))?;
        Ok(Self {
            client_id: client_id.into(),
            encoding_key: Arc::new(encoding_key),
        })
    }

    /// Mint a short-lived (9 min) JWT for App-level authentication.
    pub fn mint_jwt(&self) -> GitHubResult<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs() as i64;

        // GitHub allows up to 10 min and accepts 60s of clock skew.
        let claims = JwtClaims {
            iat: now - 60,
            exp: now + (9 * 60),
            iss: self.client_id.clone(),
        };
        encode(&Header::new(Algorithm::RS256), &claims, &self.encoding_key).map_err(Into::into)
    }
}

#[derive(Serialize)]
struct JwtClaims {
    iat: i64,
    exp: i64,
    iss: String,
}

#[cfg(unix)]
fn check_key_permissions(path: &Path) -> GitHubResult<()> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path)
        .map_err(|e| GitHubError::Config(format!("stat {}: {e}", path.display())))?;
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(GitHubError::Config(format!(
            "private key {} is group/world-readable (mode {mode:o}); expected 0600",
            path.display(),
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_key_permissions(_path: &Path) -> GitHubResult<()> {
    // No portable equivalent on non-Unix; we trust the caller.
    Ok(())
}

#[cfg(all(test, unix))]
mod perm_tests {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn rejects_world_readable_key() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----")
            .unwrap();
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = check_key_permissions(f.path()).unwrap_err();
        assert!(
            err.to_string()
                .contains("group/world-readable")
        );
    }

    #[test]
    fn accepts_0600_key() {
        let f = NamedTempFile::new().unwrap();
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
        check_key_permissions(f.path()).unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct InstallationToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

/// In-memory cache of installation access tokens keyed by installation id.
/// Refresh threshold is intentionally large (5 min) so we never use a token
/// that's about to expire mid-request.
#[derive(Clone)]
pub struct InstallationTokenCache {
    creds: AppCredentials,
    api_base_url: String,
    http: reqwest::Client,
    cache: Arc<RwLock<HashMap<i64, InstallationToken>>>,
}

impl InstallationTokenCache {
    pub fn new(creds: AppCredentials, api_base_url: impl Into<String>) -> Self {
        Self {
            creds,
            api_base_url: api_base_url.into(),
            // A per-request timeout so a stalled GitHub connection can't hang
            // startup (`GET /app`) or block token minting indefinitely.
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .expect("building reqwest client"),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Fetch the App's numeric id via `GET /app` (App-JWT auth). Called once
    /// at daemon startup to scope the Check Run reconcile to our own runs
    /// (roadmap-v4) — avoids an operator-provided `app_id` config value.
    pub async fn app_id(&self) -> GitHubResult<i64> {
        let jwt = self.creds.mint_jwt()?;
        let url = format!(
            "{}/app",
            self.api_base_url
                .trim_end_matches('/')
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt}")).map_err(GitHubError::from)?,
        );
        headers.insert(ACCEPT, HeaderValue::from_static("application/vnd.github+json"));
        headers.insert(USER_AGENT, HeaderValue::from_static("sbgh-daemon"));

        let resp = self
            .http
            .get(&url)
            .headers(headers)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_default();
            return Err(GitHubError::Api(format!("GET /app failed: {status}: {body}")));
        }
        #[derive(Deserialize)]
        struct App {
            id: i64,
        }
        let app: App = resp.json().await?;
        Ok(app.id)
    }

    pub async fn token_for(&self, installation_id: i64) -> GitHubResult<String> {
        let now = Utc::now();
        let refresh_before = now + Duration::minutes(5);

        if let Some(t) = self
            .cache
            .read()
            .await
            .get(&installation_id)
            && t.expires_at > refresh_before
        {
            return Ok(t.token.clone());
        }

        let fresh = self
            .mint(installation_id)
            .await?;
        let token = fresh.token.clone();
        self.cache
            .write()
            .await
            .insert(installation_id, fresh);
        Ok(token)
    }

    async fn mint(&self, installation_id: i64) -> GitHubResult<InstallationToken> {
        let jwt = self.creds.mint_jwt()?;
        let url = format!(
            "{}/app/installations/{}/access_tokens",
            self.api_base_url
                .trim_end_matches('/'),
            installation_id
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt}")).map_err(GitHubError::from)?,
        );
        headers.insert(ACCEPT, HeaderValue::from_static("application/vnd.github+json"));
        headers.insert(USER_AGENT, HeaderValue::from_static("sbgh-handler"));

        let resp = self
            .http
            .post(&url)
            .headers(headers)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_default();
            return Err(GitHubError::Api(format!(
                "installation token mint failed: {status}: {body}"
            )));
        }

        #[derive(Deserialize)]
        struct Resp {
            token: String,
            expires_at: DateTime<Utc>,
        }
        let resp: Resp = resp.json().await?;
        Ok(InstallationToken {
            token: resp.token,
            expires_at: resp.expires_at,
        })
    }
}
