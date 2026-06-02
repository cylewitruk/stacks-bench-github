//! Bearer-token auth + scope authorization for the `/api` server.
//!
//! Each configured token maps to a [`Scope`]; a route group is gated by
//! [`protect`], which authenticates the bearer (constant-time compare) and
//! authorizes it for the group's required scope. The path prefix is
//! cosmetic — the scope is the boundary.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use sbgh_api::ApiError;
use subtle::ConstantTimeEq;

/// What a token is allowed to do. Not a strict hierarchy: `ingest` is
/// orthogonal (handler-only), while `admin` implies `read`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Ingest,
    Read,
    Admin,
}

impl Scope {
    /// Whether a caller authenticated with `self` may use a route that
    /// requires `required`.
    pub fn satisfies(self, required: Scope) -> bool {
        match required {
            Scope::Ingest => self == Scope::Ingest,
            Scope::Read => matches!(self, Scope::Read | Scope::Admin),
            Scope::Admin => self == Scope::Admin,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Ingest => "ingest",
            Scope::Read => "read",
            Scope::Admin => "admin",
        }
    }
}

/// The configured tokens, each mapped to the scope it grants. Resolved by
/// constant-time comparison so a token can't be recovered by timing.
pub struct ApiTokens {
    admin: String,
    ingest: Option<String>,
    read: Option<String>,
}

impl ApiTokens {
    /// Construct the token registry, rejecting misconfigurations that
    /// would otherwise create a usable credential by accident: an
    /// empty/whitespace token (e.g. `SBGH_API_INGEST_TOKEN=""` would make
    /// `Authorization: Bearer ` resolve as `ingest`), or two scopes sharing
    /// the same token (a scope confusion).
    pub fn new(
        admin: String,
        ingest: Option<String>,
        read: Option<String>,
    ) -> anyhow::Result<Self> {
        let configured: Vec<(&str, &str)> = std::iter::once(("admin", admin.as_str()))
            .chain(
                ingest
                    .as_deref()
                    .map(|t| ("ingest", t)),
            )
            .chain(
                read.as_deref()
                    .map(|t| ("read", t)),
            )
            .collect();
        for (label, token) in &configured {
            if token.trim().is_empty() {
                anyhow::bail!("api {label} token is empty");
            }
        }
        for i in 0..configured.len() {
            for j in (i + 1)..configured.len() {
                if configured[i].1 == configured[j].1 {
                    anyhow::bail!(
                        "api {} and {} tokens are identical",
                        configured[i].0,
                        configured[j].0
                    );
                }
            }
        }
        drop(configured);
        Ok(Self { admin, ingest, read })
    }

    /// Resolve a presented bearer token to the scope it grants, or `None`
    /// if it matches no configured token.
    fn resolve(&self, presented: &str) -> Option<Scope> {
        // Defense in depth: an empty token never resolves (configured
        // tokens are non-empty by construction, but be explicit).
        if presented.is_empty() {
            return None;
        }
        if ct_eq(presented, &self.admin) {
            return Some(Scope::Admin);
        }
        if let Some(t) = &self.ingest {
            if ct_eq(presented, t) {
                return Some(Scope::Ingest);
            }
        }
        if let Some(t) = &self.read {
            if ct_eq(presented, t) {
                return Some(Scope::Read);
            }
        }
        None
    }
}

/// Constant-time string compare. Differing lengths are unequal — the
/// length check leaks only the length (tokens are fixed-length), not the
/// contents.
fn ct_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && bool::from(
            a.as_bytes()
                .ct_eq(b.as_bytes()),
        )
}

/// Gate `router`'s routes behind bearer auth for `required`. Uses
/// `route_layer` so it applies only to this group's routes — a 404
/// elsewhere isn't turned into a 401. Generic over the router state so it
/// composes with both stateless (health) and `ApiState` route groups.
pub fn protect<S>(router: Router<S>, tokens: Arc<ApiTokens>, required: Scope) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.route_layer(axum::middleware::from_fn(move |req: Request, next: Next| {
        let tokens = tokens.clone();
        async move { authorize(&tokens, required, req, next).await }
    }))
}

async fn authorize(tokens: &ApiTokens, required: Scope, mut req: Request, next: Next) -> Response {
    let Some(token) = bearer(&req) else {
        return reject(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing or malformed `Authorization: Bearer` token",
        );
    };
    let Some(scope) = tokens.resolve(&token) else {
        return reject(StatusCode::UNAUTHORIZED, "unauthorized", "invalid token");
    };
    if !scope.satisfies(required) {
        return reject(
            StatusCode::FORBIDDEN,
            "forbidden",
            &format!(
                "token scope `{}` is insufficient (need `{}`)",
                scope.as_str(),
                required.as_str()
            ),
        );
    }
    // Hand the resolved scope to the route (e.g. `/api/whoami`).
    req.extensions_mut()
        .insert(scope);
    next.run(req).await
}

/// Extract the token from an `Authorization: Bearer <token>` header
/// (scheme matched case-insensitively).
fn bearer(req: &Request) -> Option<String> {
    let value = req
        .headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim().to_string())
}

fn reject(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(ApiError::new(code, message))).into_response()
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn satisfies_matrix() {
        assert!(Scope::Admin.satisfies(Scope::Admin));
        assert!(Scope::Admin.satisfies(Scope::Read)); // admin implies read
        assert!(!Scope::Admin.satisfies(Scope::Ingest)); // ingest is orthogonal
        assert!(Scope::Read.satisfies(Scope::Read));
        assert!(!Scope::Read.satisfies(Scope::Admin));
        assert!(Scope::Ingest.satisfies(Scope::Ingest));
        assert!(!Scope::Ingest.satisfies(Scope::Read));
    }

    #[test]
    fn resolve_maps_tokens_to_scopes() {
        let t = ApiTokens::new("a".into(), Some("i".into()), Some("r".into())).unwrap();
        assert_eq!(t.resolve("a"), Some(Scope::Admin));
        assert_eq!(t.resolve("i"), Some(Scope::Ingest));
        assert_eq!(t.resolve("r"), Some(Scope::Read));
        assert_eq!(t.resolve("nope"), None);
        // An empty presented token never resolves.
        assert_eq!(t.resolve(""), None);
    }

    #[test]
    fn new_rejects_empty_and_duplicate_tokens() {
        // Empty / whitespace tokens (would make `Bearer ` a credential).
        assert!(ApiTokens::new("a".into(), Some("".into()), None).is_err());
        assert!(ApiTokens::new("a".into(), Some("   ".into()), None).is_err());
        assert!(ApiTokens::new("".into(), None, None).is_err());
        // Two scopes sharing a token (scope confusion).
        assert!(ApiTokens::new("dup".into(), Some("dup".into()), None).is_err());
        // A valid distinct set is accepted.
        assert!(ApiTokens::new("a".into(), Some("i".into()), Some("r".into())).is_ok());
    }

    fn test_router() -> Router {
        let tokens = Arc::new(
            ApiTokens::new("admintok".into(), Some("ingesttok".into()), Some("readtok".into()))
                .unwrap(),
        );
        Router::new()
            .merge(protect(
                Router::new().route("/ingest", get(|| async { "ok" })),
                tokens.clone(),
                Scope::Ingest,
            ))
            .merge(protect(
                Router::new().route("/read", get(|| async { "ok" })),
                tokens.clone(),
                Scope::Read,
            ))
            .merge(protect(
                Router::new().route("/admin", get(|| async { "ok" })),
                tokens,
                Scope::Admin,
            ))
    }

    async fn call(path: &str, token: Option<&str>) -> StatusCode {
        let mut builder = Request::builder().uri(path);
        if let Some(t) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        test_router()
            .oneshot(
                builder
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn missing_token_is_401() {
        assert_eq!(call("/read", None).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn invalid_token_is_401() {
        assert_eq!(call("/read", Some("bogus")).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn insufficient_scope_is_403() {
        assert_eq!(call("/admin", Some("readtok")).await, StatusCode::FORBIDDEN);
        assert_eq!(call("/read", Some("ingesttok")).await, StatusCode::FORBIDDEN);
        assert_eq!(call("/ingest", Some("readtok")).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn correct_scope_is_200() {
        assert_eq!(call("/ingest", Some("ingesttok")).await, StatusCode::OK);
        assert_eq!(call("/read", Some("readtok")).await, StatusCode::OK);
        assert_eq!(call("/admin", Some("admintok")).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_token_satisfies_read_route() {
        assert_eq!(call("/read", Some("admintok")).await, StatusCode::OK);
    }
}
