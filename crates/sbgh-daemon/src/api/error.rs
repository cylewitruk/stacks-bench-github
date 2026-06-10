//! Handler error type — maps to the `ApiError` JSON envelope + an HTTP
//! status. Route handlers return `Result<Json<T>, ApiErr>`.
//!
//! `message` is the **public** body returned to the client; `detail` is
//! internal context (raw SQL/connection/GitHub-response text) that is
//! logged server-side but never serialized — so a `/api` caller can't read
//! database/upstream internals off a 5xx.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sbgh_api::ApiError;
use sbgh_core::admin::{InstallerError, PolicyError, RepoError, UserError};

#[derive(Debug)]
pub struct ApiErr {
    status: StatusCode,
    code: &'static str,
    message: String,
    /// Logged, never returned to the client.
    detail: Option<String>,
}

impl ApiErr {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            detail: None,
        }
    }

    /// Attach internal context — logged on a 5xx, never serialized.
    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    /// Upstream GitHub resolution failed (`502`). The public message is
    /// generic; pass the raw upstream text via [`Self::with_detail`].
    fn github() -> Self {
        Self::new(StatusCode::BAD_GATEWAY, "github_resolution_failed", "GitHub API request failed")
    }

    /// A DB/store failure (`503`): generic public message + logged detail.
    fn db(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "db_unavailable", "database unavailable")
            .with_detail(detail)
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        // Server-side errors are worth a log (with the internal detail);
        // client errors stay quiet (the public response carries the reason).
        if self.status.is_server_error() {
            tracing::error!(
                status = %self.status,
                code = self.code,
                detail = self.detail.as_deref().unwrap_or(""),
                "api error: {}",
                self.message
            );
        }
        (self.status, Json(ApiError::new(self.code, self.message))).into_response()
    }
}

impl From<sqlx::Error> for ApiErr {
    fn from(e: sqlx::Error) -> Self {
        ApiErr::db(e.to_string())
    }
}

impl From<sbgh_core::Error> for ApiErr {
    fn from(e: sbgh_core::Error) -> Self {
        ApiErr::db(e.to_string())
    }
}

impl From<InstallerError> for ApiErr {
    fn from(e: InstallerError) -> Self {
        use InstallerError::*;
        match e {
            AccountNotFound(login) => {
                ApiErr::not_found(format!("github account `{login}` not found"))
            }
            NotOnAllowlist(id) => {
                ApiErr::not_found(format!("installer {id} is not on the allowlist"))
            }
            UnsupportedAccountType(t) => {
                ApiErr::bad_request(format!("unsupported account type `{t}`"))
            }
            GithubRejected { status, body } => {
                ApiErr::github().with_detail(format!("status={status} body={body}"))
            }
            Http(e) => ApiErr::github().with_detail(e.to_string()),
            Db(e) => ApiErr::db(e.to_string()),
        }
    }
}

impl From<RepoError> for ApiErr {
    fn from(e: RepoError) -> Self {
        use RepoError::*;
        match e {
            RepoNotFound(name) => ApiErr::not_found(format!("github repo `{name}` not found")),
            NotOnSupportedList(id) => {
                ApiErr::not_found(format!("repo {id} is not on the supported list"))
            }
            GithubRejected { status, body } => {
                ApiErr::github().with_detail(format!("status={status} body={body}"))
            }
            Http(e) => ApiErr::github().with_detail(e.to_string()),
            Db(e) => ApiErr::db(e.to_string()),
        }
    }
}

impl From<PolicyError> for ApiErr {
    fn from(e: PolicyError) -> Self {
        use PolicyError::*;
        match e {
            NotFound { install_id, repo_id } => {
                ApiErr::not_found(format!("no policy for install {install_id} / repo {repo_id}"))
            }
            TriggerNotFound(id) => ApiErr::not_found(format!("trigger policy {id} not found")),
            MissingTargetForTrigger { install_id, repo_id } => ApiErr::conflict(format!(
                "install {install_id} / repo {repo_id} has no enabled target policy — enable it \
                 before adding a trigger"
            )),
            InvalidMatchSpec(msg) => ApiErr::bad_request(format!("invalid match_spec: {msg}")),
            UnsupportedTriggerKind(kind) => ApiErr::bad_request(format!(
                "unsupported trigger kind `{kind:?}` — use `branch_push` or `tag_created`"
            )),
            Db(e) => ApiErr::db(e.to_string()),
        }
    }
}

impl From<UserError> for ApiErr {
    fn from(e: UserError) -> Self {
        use UserError::*;
        match e {
            AccountNotFound(login) => {
                ApiErr::not_found(format!("github account `{login}` not found"))
            }
            GrantNotFound {
                user_id,
                install_id,
                repo_id,
                role,
            } => ApiErr::not_found(format!(
                "no `{role:?}` grant for user {user_id} on install {install_id} repo {repo_id:?}"
            )),
            UnknownUser(id) => ApiErr::not_found(format!("user {id} is unknown")),
            NoActiveMembership { install_id, repo_id } => ApiErr::conflict(format!(
                "install {install_id} / repo {repo_id} has no active membership"
            )),
            UnsupportedAccountType(t) => {
                ApiErr::bad_request(format!("unsupported account type `{t}`"))
            }
            GithubRejected { status, body } => {
                ApiErr::github().with_detail(format!("status={status} body={body}"))
            }
            Http(e) => ApiErr::github().with_detail(e.to_string()),
            Db(e) => ApiErr::db(e.to_string()),
            Store(e) => ApiErr::db(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;

    use super::*;

    #[tokio::test]
    async fn db_error_returns_generic_message_not_internal_detail() {
        // A sqlx error must surface as a stable generic body — the raw
        // error text is logged server-side, never returned.
        let err: ApiErr = sqlx::Error::PoolClosed.into();
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let parsed: sbgh_api::ApiError = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.error.code, "db_unavailable");
        assert_eq!(parsed.error.message, "database unavailable");
    }
}
