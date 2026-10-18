//! Transport-neutral worker-fleet contracts and state-machine invariants.
//!
//! Wire encodings convert to these values at the daemon and worker edges.
//! Persistence and execution code therefore remain independent of protobuf,
//! gRPC, and any future transport.

mod digest;
mod model;
mod negotiation;
mod preflight;
mod validate;

pub use digest::payload_digest;
pub use model::*;
pub use negotiation::{
    NEGOTIATED_PROTOCOL_REVISION, NegotiationError, PREVIOUS_PROTOCOL_REVISION, ProtocolContract,
    ProtocolFeature, ProtocolOffer,
};
pub use preflight::PreflightReport;
pub use validate::{
    MAX_TERMINAL_TEXT_BYTES, MAX_VALIDATION_CONCURRENCY, MAX_VALIDATION_SHARDS,
    MAX_VALIDATION_TIMEOUT_SECS, ProtocolError, Validate, normalize_terminal_text,
    validate_block_validation_result,
};

/// Pinned legacy wire and task-payload revision; negotiated registration uses a separate window.
pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_LONG_POLL_SECS: u64 = 30;
