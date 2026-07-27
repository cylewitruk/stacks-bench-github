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

/// v25 coordinates daemon and worker upgrades and requires an exact match.
/// Fleet wire version. v2 adds the bounded, payload-derived offer requirements
/// needed for fail-closed local admission before a worker accepts a lease.
pub const PROTOCOL_VERSION: u16 = 2;

/// Prevent accidentally accepting unbounded JSON bodies at a different layer.
pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

/// Upper bound for one server-side assignment long poll.
///
/// The worker transport allows additional time for network and response
/// processing beyond this bound.
pub const MAX_LONG_POLL_SECS: u64 = 30;
