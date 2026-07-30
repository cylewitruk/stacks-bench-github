use sbgh_proto::FleetRpcError;
use sbgh_proto::Wire;
use sbgh_proto::fleet::v1::worker_fleet_service_server::{
    WorkerFleetService, WorkerFleetServiceServer,
};
use sbgh_proto::fleet::v1::{
    AcceptRequest, AcceptResponse, CompleteAttemptRequest, CompleteAttemptResponse,
    CompleteCleanupRequest, CompleteCleanupResponse, DeregisterRequest, DeregisterResponse,
    FetchRepositoryCredentialRequest, FetchRepositoryCredentialResponse, GrantArtifactRequest,
    GrantArtifactResponse, HeartbeatRequest, HeartbeatResponse, ListCleanupRequest,
    ListCleanupResponse, PollRequest, PollResponse, PublishProgressRequest,
    PublishProgressResponse, PublishReliableEventRequest, PublishReliableEventResponse,
    RegisterRequest, RegisterResponse,
};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status};
use tower::limit::GlobalConcurrencyLimitLayer;

use super::service::{self, FleetRuntime, FleetService, ServiceCode, ServiceError};
use super::tls::{AuthenticatedPeer, MtlsListener};

#[derive(Clone)]
struct GrpcFleet {
    service: FleetService,
}

fn peer<T>(request: &Request<T>) -> Result<AuthenticatedPeer, Status> {
    request
        .extensions()
        .get::<AuthenticatedPeer>()
        .cloned()
        .ok_or_else(|| {
            rpc_error(
                Code::Unauthenticated,
                "missing_peer_identity",
                "authenticated worker connection identity is unavailable",
                false,
            )
        })
}

fn wire<T: Wire>(value: T) -> Result<T::Domain, Status> {
    value
        .into_domain()
        .map_err(|error| {
            rpc_error(Code::InvalidArgument, "invalid_protocol_message", &error.to_string(), false)
        })
}

fn rpc_error(status: Code, code: &str, message: &str, retryable: bool) -> Status {
    FleetRpcError {
        status,
        code: code.into(),
        message: message.into(),
        retryable,
        retry_after_ms: None,
    }
    .into_status()
}

fn service_error(error: ServiceError) -> Status {
    let status = match error.code {
        ServiceCode::InvalidArgument => Code::InvalidArgument,
        ServiceCode::PermissionDenied => Code::PermissionDenied,
        ServiceCode::FailedPrecondition => Code::FailedPrecondition,
        ServiceCode::ResourceExhausted => Code::ResourceExhausted,
        ServiceCode::Unavailable => Code::Unavailable,
        ServiceCode::Internal => Code::Internal,
    };
    FleetRpcError {
        status,
        code: error.stable_code.into(),
        message: error.message,
        retryable: error.retryable,
        retry_after_ms: error.retry_after_ms,
    }
    .into_status()
}

#[tonic::async_trait]
impl WorkerFleetService for GrpcFleet {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let peer = peer(&request)?;
        let response = service::register(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(RegisterResponse::from_domain(response)))
    }

    async fn poll(&self, request: Request<PollRequest>) -> Result<Response<PollResponse>, Status> {
        let peer = peer(&request)?;
        let response = service::poll(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(PollResponse::from_domain(response)))
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        let peer = peer(&request)?;
        let response = service::accept(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(AcceptResponse::from_domain(response)))
    }

    async fn fetch_repository_credential(
        &self,
        request: Request<FetchRepositoryCredentialRequest>,
    ) -> Result<Response<FetchRepositoryCredentialResponse>, Status> {
        let peer = peer(&request)?;
        let response =
            service::repository_credential(&self.service, &peer, wire(request.into_inner())?)
                .await
                .map_err(service_error)?;
        Ok(Response::new(FetchRepositoryCredentialResponse::from_domain(response)))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let peer = peer(&request)?;
        let response = service::heartbeat(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(HeartbeatResponse::from_domain(response)))
    }

    async fn publish_reliable_event(
        &self,
        request: Request<PublishReliableEventRequest>,
    ) -> Result<Response<PublishReliableEventResponse>, Status> {
        let peer = peer(&request)?;
        let response = service::event(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(PublishReliableEventResponse::from_domain(response)))
    }

    async fn publish_progress(
        &self,
        request: Request<PublishProgressRequest>,
    ) -> Result<Response<PublishProgressResponse>, Status> {
        let peer = peer(&request)?;
        let accepted = service::progress(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(PublishProgressResponse::from_domain(accepted)))
    }

    async fn grant_artifact(
        &self,
        request: Request<GrantArtifactRequest>,
    ) -> Result<Response<GrantArtifactResponse>, Status> {
        let peer = peer(&request)?;
        let response = service::artifact_grant(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(GrantArtifactResponse::from_domain(response)))
    }

    async fn complete_attempt(
        &self,
        request: Request<CompleteAttemptRequest>,
    ) -> Result<Response<CompleteAttemptResponse>, Status> {
        let peer = peer(&request)?;
        let response = service::complete(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(CompleteAttemptResponse::from_domain(response)))
    }

    async fn list_cleanup(
        &self,
        request: Request<ListCleanupRequest>,
    ) -> Result<Response<ListCleanupResponse>, Status> {
        let peer = peer(&request)?;
        let response = service::cleanup(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(ListCleanupResponse::from_domain(response)))
    }

    async fn complete_cleanup(
        &self,
        request: Request<CompleteCleanupRequest>,
    ) -> Result<Response<CompleteCleanupResponse>, Status> {
        let peer = peer(&request)?;
        let completed =
            service::cleanup_complete(&self.service, &peer, wire(request.into_inner())?)
                .await
                .map_err(service_error)?;
        Ok(Response::new(CompleteCleanupResponse::from_domain(completed)))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        let peer = peer(&request)?;
        let deregistered = service::deregister(&self.service, &peer, wire(request.into_inner())?)
            .await
            .map_err(service_error)?;
        Ok(Response::new(DeregisterResponse::from_domain(deregistered)))
    }
}

pub async fn run(runtime: FleetRuntime, shutdown: CancellationToken) -> anyhow::Result<()> {
    let config = runtime
        .service
        .config()
        .clone();
    let request_limit = config.max_concurrent_requests;
    let listener =
        MtlsListener::bind(config.listen, &config.server_certificate, &config.server_private_key)
            .await?;
    let local_addr = listener.local_addr();
    let grpc = GrpcFleet {
        service: runtime.service.clone(),
    };
    let fleet = WorkerFleetServiceServer::new(grpc)
        .max_decoding_message_size(sbgh_fleet::MAX_REQUEST_BYTES)
        .max_encoding_message_size(sbgh_fleet::MAX_REQUEST_BYTES);

    let maintenance_service = runtime.service.clone();
    let maintenance_shutdown = shutdown.clone();
    let maintenance = tokio::spawn(async move {
        service::maintenance_loop(maintenance_service, maintenance_shutdown).await;
    });

    tracing::info!(listen = %local_addr, "worker fleet protobuf/gRPC mTLS listener started");
    let serving = tonic::transport::Server::builder()
        .timeout(config.request_timeout())
        .layer(GlobalConcurrencyLimitLayer::new(request_limit))
        .serve_with_incoming_shutdown(fleet, listener, shutdown.cancelled_owned())
        .await;
    maintenance.abort();
    serving.map_err(anyhow::Error::from)
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use sbgh_proto::fleet::v1;
    use sbgh_proto::status_detail;

    use super::*;

    #[test]
    fn structured_error_detail_is_machine_readable() {
        let status = rpc_error(Code::FailedPrecondition, "stale_attempt", "diagnostic text", false);
        let detail = status_detail(&status).unwrap();
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(detail.code, "stale_attempt");
        assert!(!detail.retryable);
        assert!(
            detail
                .retry_after_ms
                .is_none()
        );
        assert!(!status.details().is_empty());
        assert!(v1::FleetErrorDetail::decode(status.details()).is_ok());
    }
}
