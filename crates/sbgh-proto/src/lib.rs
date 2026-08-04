//! Generated protobuf/gRPC worker-fleet transport.
//!
//! The `.proto` schema is the sole wire source of truth. Generated messages
//! convert to transport-neutral [`sbgh_fleet`] values before entering daemon,
//! persistence, or execution logic.

mod convert;
mod error;
mod service_mux;

pub mod fleet {
    pub mod v1 {
        tonic::include_proto!("sbgh.fleet.v1");
    }
}

pub use convert::Wire;
pub use error::{FleetRpcError, status_detail};
pub use service_mux::FleetServiceMux;

/// Canonical generated service name used by gRPC health publication and
/// worker health requests.
pub use fleet::v1::worker_fleet_service_server::SERVICE_NAME as WORKER_FLEET_SERVICE_NAME;
