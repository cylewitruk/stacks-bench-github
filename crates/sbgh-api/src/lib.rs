//! Shared wire types + a thin typed client for the daemon's `/api`.
//!
//! DTOs here are deliberately **API-shaped** (not the core/DB structs) so
//! internal columns added later don't leak into the public surface. The
//! server (daemon) and the clients (handler, `sbgh-cli`) all depend
//! on this crate, so a contract change breaks the build everywhere — the
//! compile-time-contract benefit of gRPC without proto/codegen.

mod client;
mod dto;
mod error;

pub use client::{Client, ClientError, read_cookie};
pub use dto::{
    AddTriggerRequest, AllowInstallerRequest, AllowPolicyRequest, AllowRepoRequest,
    BenchmarkReportDetail, BlockValidationReportDetail, BuildOnlyReportDetail,
    DisableInstallerRequest, DisablePolicyRequest, DisableRepoRequest,
    EnqueueBlockValidationRequest, EnqueueJobResponse, FleetCancellationResponse, FleetOverview,
    FleetRecoveryRequest, FleetRecoveryResponse, FleetSummaryView, FleetWorkerView,
    GrantRoleResult, HealthResponse, InstallationView, InstallerView, InvalidBlockDetail, JobView,
    PinTriggerRequest, PolicyView, RepoRootView, ReportArtifactView, ReportIdentityView,
    ReportLifecycleView, ReportRange, ResolveRepoResponse, RoleRequest, RoleView,
    SubmissionReportView, TaskReportView, TriggerView, UserView, WebhookSubmitResponse,
    WebhookSummary, WhoamiResponse, WorkerDrainRequest,
};
pub use error::{ApiError, ErrorBody};
