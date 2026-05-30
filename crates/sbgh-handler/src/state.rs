use std::sync::Arc;

use sbgh_core::config::HandlerConfig;
use sbgh_core::db::IngestStore;

/// Handler state.
///
/// Deliberately omits any GitHub-API client: the handler verifies HMAC
/// signatures and records the webhook to the inbox — nothing else. Since
/// the slice 11 cutover it is inbox-only (no `/benchmark` parse, authz,
/// or legacy `jobs` write — the processor owns all of that). All GitHub
/// API I/O happens from the orchestrator (which holds the App private
/// key). This is the security boundary the role split is built around —
/// keep it.
#[derive(Clone)]
pub struct AppState {
    pub config: HandlerConfig,
    pub ingest: Arc<dyn IngestStore>,
}
