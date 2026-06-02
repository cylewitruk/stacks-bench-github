use sbgh_api::Client;
use sbgh_core::config::HandlerConfig;

/// Handler state.
///
/// Deliberately omits any GitHub-API client AND any DB access: the handler
/// verifies HMAC signatures and forwards the verified delivery to the
/// daemon `/api` — nothing else. It owns no App private key and no
/// database role; the daemon (which holds both) owns the event-type
/// allowlist, payload parsing, inbox write, authorization, and job
/// creation. This is the security boundary the design is built around —
/// keep it.
#[derive(Clone)]
pub struct AppState {
    pub config: HandlerConfig,
    /// Typed client over the daemon `/api`, pre-loaded with the
    /// `ingest`-scope token.
    pub api: Client,
}
