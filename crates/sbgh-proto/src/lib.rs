//! Versioned, dependency-light worker-fleet wire contracts.
//!
//! These owned DTOs are deliberately neither database rows nor execution
//! backend types. Both sides validate them at the transport boundary.

mod canonical;
mod dto;
mod validate;

pub use canonical::{canonical_json_bytes, payload_digest};
pub use dto::*;
pub use validate::{
    MAX_VALIDATION_CONCURRENCY, MAX_VALIDATION_SHARDS, MAX_VALIDATION_TIMEOUT_SECS, ProtocolError,
    Validate,
};

/// Fleet upgrades require an exact daemon/worker version match. v3 makes
/// chainstate selection worker-local and reports the selected origin and
/// guest-observed coverage with block-validation results.
pub const PROTOCOL_VERSION: u16 = 3;

/// Prevent accidentally accepting unbounded JSON bodies at a different layer.
pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

/// Upper bound for one server-side assignment long poll.
///
/// The worker transport allows additional time for network and response
/// processing beyond this bound.
pub const MAX_LONG_POLL_SECS: u64 = 30;
