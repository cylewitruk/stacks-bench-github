use prost::Message;
use tonic::{Code, Status};

use crate::fleet::v1::FleetErrorDetail;

/// Stable machine-readable fleet error mapped onto a gRPC status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetRpcError {
    pub status: Code,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
}

impl FleetRpcError {
    pub fn into_status(self) -> Status {
        let details = FleetErrorDetail {
            code: self.code,
            retryable: self.retryable,
            retry_after_ms: self.retry_after_ms,
        }
        .encode_to_vec();
        Status::with_details(self.status, self.message, details.into())
    }
}

pub fn status_detail(status: &Status) -> Option<FleetErrorDetail> {
    if status.details().is_empty() {
        return None;
    }
    FleetErrorDetail::decode(status.details())
        .ok()
        .filter(|detail| !detail.code.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unstructured_transport_status_has_no_application_detail() {
        assert!(status_detail(&Status::unavailable("connection lost")).is_none());
    }

    #[test]
    fn structured_status_requires_a_stable_code() {
        let status = Status::with_details(
            Code::Internal,
            "invalid detail",
            FleetErrorDetail {
                code: String::new(),
                retryable: true,
                retry_after_ms: Some(1),
            }
            .encode_to_vec()
            .into(),
        );
        assert!(status_detail(&status).is_none());
    }

    #[test]
    fn structured_status_round_trips() {
        let status = FleetRpcError {
            status: Code::ResourceExhausted,
            code: "worker_busy".into(),
            message: "try later".into(),
            retryable: true,
            retry_after_ms: Some(2_500),
        }
        .into_status();
        let detail = status_detail(&status).unwrap();
        assert_eq!(detail.code, "worker_busy");
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, Some(2_500));
    }
}
