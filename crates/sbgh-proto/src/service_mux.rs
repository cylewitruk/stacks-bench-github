use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tonic::body::Body;
use tonic::codegen::Service;
use tonic::codegen::http::{Request, Response};
use tonic::server::NamedService;

const HEALTH_CHECK_PATH: &str = "/grpc.health.v1.Health/Check";

/// Route the standard gRPC health service beside the fleet service without
/// pulling Tonic's general-purpose Axum router into the protocol boundary.
#[derive(Clone)]
pub struct FleetServiceMux<F, H> {
    fleet: F,
    health: H,
}

impl<F, H> FleetServiceMux<F, H> {
    pub fn new(fleet: F, health: H) -> Self {
        Self { fleet, health }
    }
}

impl<F, H> Service<Request<Body>> for FleetServiceMux<F, H>
where
    F: Service<Request<Body>, Response = Response<Body>, Error = Infallible>,
    F::Future: Send + 'static,
    H: Service<Request<Body>, Response = Response<Body>, Error = Infallible>,
    H::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Both generated Tonic services are always-ready today. Polling both
        // is valid only under that invariant; a future buffered or reserving
        // service must replace this mux with reservation-aware routing.
        ready!(
            self.health
                .poll_ready(context)
        )?;
        ready!(self.fleet.poll_ready(context))?;
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        if request.uri().path() == HEALTH_CHECK_PATH {
            Box::pin(self.health.call(request))
        } else {
            Box::pin(self.fleet.call(request))
        }
    }
}

impl<F: NamedService, H> NamedService for FleetServiceMux<F, H> {
    const NAME: &'static str = F::NAME;
}
