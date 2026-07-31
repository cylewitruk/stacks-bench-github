//! Transport-neutral worker-fleet contracts and state-machine invariants.
//!
//! Wire encodings convert to these values at the daemon and worker edges.
//! Persistence and execution code therefore remain independent of protobuf,
//! gRPC, and any future transport.

mod digest;
mod model;
mod validate;

pub use digest::payload_digest;
pub use model::*;
pub use validate::{
    MAX_VALIDATION_CONCURRENCY, MAX_VALIDATION_SHARDS, MAX_VALIDATION_TIMEOUT_SECS, ProtocolError,
    Validate, validate_block_validation_result,
};

/// Exact revision of the first deployed protobuf fleet protocol.
pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_LONG_POLL_SECS: u64 = 30;
