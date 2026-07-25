use std::sync::Arc;

use sbgh_core::db::IngestStore;
use sbgh_postgres::Pool;

/// Shared state for the `/api` route handlers. The owner `pool` backs the
/// direct read queries + admin writes (Phase 3 moved the daemon to
/// the owner DSN); `ingest` is the write-through webhook inbox;
/// `gh_api_base` is the GitHub API root for server-side `login`/`owner-name`
/// → id resolution.
#[derive(Clone)]
pub struct ApiState {
    pub pool: Pool,
    pub ingest: Arc<dyn IngestStore>,
    /// GitHub API base for the admin endpoints' server-side
    /// login/owner-name → id resolution.
    pub gh_api_base: String,
}
