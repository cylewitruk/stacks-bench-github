use axum::{Extension, Json};
use sbgh_api::{HealthResponse, WhoamiResponse};

use crate::api::Scope;

/// `GET /api/health` — unauthenticated liveness probe.
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok".into() })
}

/// `GET /api/whoami` — echo the scope the caller's token resolved to. The
/// auth layer inserts the resolved [`Scope`] into the request extensions;
/// `read` scope gates the route (so `admin` reaches it too).
pub async fn whoami(Extension(scope): Extension<Scope>) -> Json<WhoamiResponse> {
    Json(WhoamiResponse { scope: scope.as_str().into() })
}
