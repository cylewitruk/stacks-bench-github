//! Generated protobuf/gRPC worker-fleet transport.
//!
//! The `.proto` schema is the sole wire source of truth. Generated messages
//! convert to transport-neutral [`sbgh_fleet`] values before entering daemon,
//! persistence, or execution logic.

mod convert;
mod error;

pub mod fleet {
    pub mod v1 {
        tonic::include_proto!("sbgh.fleet.v1");
    }
}

pub use convert::Wire;
pub use error::{FleetRpcError, status_detail};
