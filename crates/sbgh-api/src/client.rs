use std::path::Path;
use std::time::Duration;

use crate::dto::{
    AddTriggerRequest, AllowInstallerRequest, AllowPolicyRequest, AllowRepoRequest,
    DisableInstallerRequest, DisablePolicyRequest, DisableRepoRequest, GrantRoleResult,
    HealthResponse, InstallationView, InstallerView, JobView, PinTriggerRequest, PolicyView,
    RepoRootView, ResolveRepoResponse, RoleRequest, RoleView, TriggerView, UserView,
    WebhookSubmitResponse, WebhookSummary, WhoamiResponse,
};
use crate::error::ApiError;

/// Errors from a `/api` call.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    /// The server returned a non-2xx status. `code`/`message` come from the
    /// [`ApiError`] envelope when present, else the raw body.
    #[error("api error {status} ({code}): {message}")]
    Api { status: u16, code: String, message: String },
}

/// Thin typed client over the daemon `/api`.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    token: Option<String>,
    http: reqwest::Client,
}

impl Client {
    /// Default total request timeout. Generous enough for the admin
    /// endpoints (which do server-side GitHub resolution, itself bounded at
    /// ~10s) while still bounding a hung daemon.
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    /// `base_url` is the API root (e.g. `http://127.0.0.1:8787`). `token`
    /// is the bearer presented on every request — the cookie for
    /// admin/read, the shared token for the handler's ingest scope; `None`
    /// for unauthenticated probes like health.
    pub fn new(base_url: impl Into<String>, token: Option<String>) -> Self {
        Self::with_timeout(base_url, token, Self::DEFAULT_TIMEOUT)
    }

    /// Like [`Client::new`] with an explicit total request timeout. The
    /// web-facing handler pins a short one so a stalled daemon can't tie up
    /// request capacity (and delay GitHub's redelivery).
    pub fn with_timeout(
        base_url: impl Into<String>,
        token: Option<String>,
        timeout: Duration,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("building reqwest client");
        Self {
            base_url: base_url
                .into()
                .trim_end_matches('/')
                .to_string(),
            token,
            http,
        }
    }

    /// `GET /api/health` — liveness probe (no auth required server-side).
    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        self.get("/api/health").await
    }

    /// `GET /api/whoami` — the scope this client's token resolves to
    /// (requires `read`). Confirms the cookie/token reaches and is accepted
    /// by the daemon.
    pub async fn whoami(&self) -> Result<WhoamiResponse, ClientError> {
        self.get("/api/whoami").await
    }

    /// `POST /api/webhooks` (ingest scope) — submit a verified GitHub
    /// delivery: the raw `body` plus its `event` header and, when present,
    /// the `delivery` header. Absence is preserved (the header is omitted,
    /// not sent empty) so the daemon's "supported event needs a delivery
    /// id" check fires instead of recording an empty dedup key.
    pub async fn submit_webhook(
        &self,
        event: &str,
        delivery: Option<&str>,
        body: Vec<u8>,
    ) -> Result<WebhookSubmitResponse, ClientError> {
        let mut req = self
            .http
            .post(format!("{}/api/webhooks", self.base_url))
            .header("x-github-event", event)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(d) = delivery {
            req = req.header("x-github-delivery", d);
        }
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        decode(req.send().await?).await
    }

    /// `GET /api/webhooks` (read scope) — most-recent inbox rows, with
    /// optional filters.
    pub async fn list_webhooks(
        &self,
        event_type: Option<&str>,
        status: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<WebhookSummary>, ClientError> {
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(e) = event_type {
            query.push(("event_type", e.to_string()));
        }
        if let Some(s) = status {
            query.push(("status", s.to_string()));
        }
        if let Some(l) = limit {
            query.push(("limit", l.to_string()));
        }
        self.get_query("/api/webhooks", &query)
            .await
    }

    // ─── installers ────────────────────────────────────────────────────

    pub async fn list_installers(&self) -> Result<Vec<InstallerView>, ClientError> {
        self.get("/api/installers")
            .await
    }
    pub async fn allow_installer(
        &self,
        req: &AllowInstallerRequest,
    ) -> Result<InstallerView, ClientError> {
        self.post_json("/api/installers", req)
            .await
    }
    pub async fn disable_installer(
        &self,
        req: &DisableInstallerRequest,
    ) -> Result<InstallerView, ClientError> {
        self.post_json("/api/installers/disable", req)
            .await
    }

    // ─── repos ───────────────────────────────────────────────────────────

    pub async fn list_repos(&self) -> Result<Vec<RepoRootView>, ClientError> {
        self.get("/api/repos").await
    }
    pub async fn allow_repo(&self, req: &AllowRepoRequest) -> Result<RepoRootView, ClientError> {
        self.post_json("/api/repos", req)
            .await
    }
    pub async fn disable_repo(
        &self,
        req: &DisableRepoRequest,
    ) -> Result<RepoRootView, ClientError> {
        self.post_json("/api/repos/disable", req)
            .await
    }

    // ─── policies ────────────────────────────────────────────────────────

    pub async fn list_target_policies(
        &self,
        install_id: Option<i64>,
    ) -> Result<Vec<PolicyView>, ClientError> {
        self.get_query("/api/policies/target", &install_query(install_id))
            .await
    }
    pub async fn allow_target_policy(
        &self,
        req: &AllowPolicyRequest,
    ) -> Result<PolicyView, ClientError> {
        self.post_json("/api/policies/target", req)
            .await
    }
    pub async fn disable_target_policy(
        &self,
        req: &DisablePolicyRequest,
    ) -> Result<PolicyView, ClientError> {
        self.post_json("/api/policies/target/disable", req)
            .await
    }
    pub async fn list_source_policies(
        &self,
        install_id: Option<i64>,
    ) -> Result<Vec<PolicyView>, ClientError> {
        self.get_query("/api/policies/source", &install_query(install_id))
            .await
    }
    pub async fn allow_source_policy(
        &self,
        req: &AllowPolicyRequest,
    ) -> Result<PolicyView, ClientError> {
        self.post_json("/api/policies/source", req)
            .await
    }
    pub async fn disable_source_policy(
        &self,
        req: &DisablePolicyRequest,
    ) -> Result<PolicyView, ClientError> {
        self.post_json("/api/policies/source/disable", req)
            .await
    }
    pub async fn list_triggers(
        &self,
        install_id: Option<i64>,
        repo_id: Option<i64>,
    ) -> Result<Vec<TriggerView>, ClientError> {
        let mut q = install_query(install_id);
        if let Some(r) = repo_id {
            q.push(("repo_id", r.to_string()));
        }
        self.get_query("/api/policies/triggers", &q)
            .await
    }
    pub async fn add_trigger(&self, req: &AddTriggerRequest) -> Result<TriggerView, ClientError> {
        self.post_json("/api/policies/triggers", req)
            .await
    }
    pub async fn disable_trigger(&self, id: i64) -> Result<TriggerView, ClientError> {
        self.post_json(&format!("/api/policies/triggers/{id}/disable"), &())
            .await
    }
    pub async fn pin_trigger(
        &self,
        id: i64,
        req: &PinTriggerRequest,
    ) -> Result<TriggerView, ClientError> {
        self.post_json(&format!("/api/policies/triggers/{id}/pin"), req)
            .await
    }

    // ─── users / roles ───────────────────────────────────────────────────

    pub async fn list_users(&self) -> Result<Vec<UserView>, ClientError> {
        self.get("/api/users").await
    }
    pub async fn list_roles(&self, install_id: Option<i64>) -> Result<Vec<RoleView>, ClientError> {
        self.get_query("/api/roles", &install_query(install_id))
            .await
    }
    pub async fn grant_role(&self, req: &RoleRequest) -> Result<GrantRoleResult, ClientError> {
        self.post_json("/api/roles", req)
            .await
    }
    pub async fn revoke_role(&self, req: &RoleRequest) -> Result<RoleView, ClientError> {
        self.post_json("/api/roles/revoke", req)
            .await
    }

    // ─── installations / jobs ────────────────────────────────────────────

    pub async fn list_installations(&self) -> Result<Vec<InstallationView>, ClientError> {
        self.get("/api/installations")
            .await
    }

    /// `GET /api/resolve?owner=&repo=` (read scope) — resolve an
    /// `owner/repo` slug to its active `install_id` + `repo_id`. Backs the
    /// CLI's `--on owner/repo` sugar. Errors (404) if the daemon hasn't
    /// materialised the install or the repo yet.
    pub async fn resolve_repo(
        &self,
        owner: &str,
        repo: &str,
    ) -> Result<ResolveRepoResponse, ClientError> {
        self.get_query("/api/resolve", &[("owner", owner.to_string()), ("repo", repo.to_string())])
            .await
    }
    pub async fn list_jobs(
        &self,
        status: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<JobView>, ClientError> {
        let mut q: Vec<(&str, String)> = Vec::new();
        if let Some(s) = status {
            q.push(("status", s.to_string()));
        }
        if let Some(l) = limit {
            q.push(("limit", l.to_string()));
        }
        self.get_query("/api/jobs", &q)
            .await
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        self.get_query(path, &[])
            .await
    }

    async fn post_json<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let mut req = self
            .http
            .post(format!("{}{}", self.base_url, path))
            .json(body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        decode(req.send().await?).await
    }

    async fn get_query<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, ClientError> {
        let mut req = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .query(query);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        decode(req.send().await?).await
    }
}

/// Map a response into `T` (on 2xx) or a [`ClientError::Api`] — parsing the
/// [`ApiError`] envelope, falling back to the raw body if it isn't one.
async fn decode<T: serde::de::DeserializeOwned>(resp: reqwest::Response) -> Result<T, ClientError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp.json::<T>().await?);
    }
    let code = status.as_u16();
    let body = resp
        .text()
        .await
        .unwrap_or_default();
    Err(match serde_json::from_str::<ApiError>(&body) {
        Ok(e) => ClientError::Api {
            status: code,
            code: e.error.code,
            message: e.error.message,
        },
        Err(_) => ClientError::Api {
            status: code,
            code: "unknown".into(),
            message: body,
        },
    })
}

/// Build the `install_id` query param (or empty when `None`).
fn install_query(install_id: Option<i64>) -> Vec<(&'static str, String)> {
    install_id
        .map(|id| vec![("install_id", id.to_string())])
        .unwrap_or_default()
}

/// Read a daemon-generated cookie token from `path` (trimming whitespace).
/// Used by `sbgh-cli` to authenticate as the operator without a config.
pub fn read_cookie(path: &Path) -> std::io::Result<String> {
    Ok(std::fs::read_to_string(path)?
        .trim()
        .to_string())
}
