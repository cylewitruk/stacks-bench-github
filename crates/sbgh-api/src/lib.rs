//! Shared wire types + a thin typed client for the daemon's `/api`.
//!
//! DTOs here are deliberately **API-shaped** (not the core/DB structs) so
//! internal columns added later don't leak into the public surface. The
//! server (daemon) and the clients (handler, `sbgh-cli`) all depend
//! on this crate, so a contract change breaks the build everywhere — the
//! compile-time-contract benefit of gRPC without proto/codegen.

mod abort;
mod client;
mod dto;
mod error;
mod inspection;
mod lifecycle;
mod protocol;
mod scheduler;

pub use lifecycle::{
    CordonSemantic, WorkerAdmissionView, WorkerLifecycleAction, WorkerLifecycleReceiptView,
    WorkerLifecycleRequest, WorkerLifecycleView, WorkerMaintenanceView, WorkerPreflightView,
};
pub use protocol::{ProtocolPolicyChangeRequest, ProtocolPolicyView};

pub use scheduler::{SchedulerChangeRequest, SchedulerPolicyView, SchedulerStatusView};

pub use abort::{FleetAbortReceiptView, FleetAbortRequest, FleetAbortScope};
pub use client::{Client, ClientError, read_cookie};
pub use dto::{
    AddTriggerRequest, AllowInstallerRequest, AllowPolicyRequest, AllowRepoRequest,
    BenchmarkReportDetail, BlockValidationReportDetail, BlockValidationSelectionDetail,
    BlockValidationSelectionRequest, BuildOnlyReportDetail, DisableInstallerRequest,
    DisablePolicyRequest, DisableRepoRequest, EnqueueBlockValidationRequest, EnqueueJobResponse,
    FleetCancellationResponse, FleetOverview, FleetRecoveryRequest, FleetRecoveryResponse,
    FleetSummaryView, FleetWorkerView, GrantRoleResult, HealthResponse, InstallationView,
    InstallerView, InvalidBlockDetail, JobArtifactView, JobView, ObservedValidationIndexDetail,
    PinTriggerRequest, PolicyView, RepoRootView, ReportArtifactView, ReportForensicsView,
    ReportIdentityView, ReportLifecycleView, ReportRange, ResolveRepoResponse, RoleRequest,
    RoleView, SubmissionReportView, TaskReportView, TriggerView, UserView,
    ValidationEpochSegmentDetail, WebhookSubmitResponse, WebhookSummary, WhoamiResponse,
    WorkerCreateRequest, WorkerDrainRequest, WorkerIdentityRequest, WorkerIdentityView,
    WorkerPolicyView, WorkerProtocolOfferView, WorkerProtocolView, WorkerUpdateRequest,
};
pub use error::{ApiError, ErrorBody};
pub use inspection::{
    AttemptInspectionView, BenchmarkMetricsView, JobInspectionView, ProgressInspectionView,
    SubmissionSummaryView, WorkSnapshotView, WorkTargetView,
};
