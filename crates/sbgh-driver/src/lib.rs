//! Dependency-light contracts shared by workers and execution adapters.

pub mod artifact;
pub mod cache;
pub mod driver;
pub mod events;
pub mod execution;

pub use artifact::ArtifactSink;
pub use cache::{BinaryCacheStore, BuildFingerprint, CacheControl, CacheEnvironment, CachedBinary};
pub use driver::{
    BenchmarkRunContext, BenchmarkTaskSpec, Driver, DriverOutcome, DriverStatus, Placement,
    TaskContext, TaskSpec,
};
pub use events::{
    EventSink, PhaseLabel, ProgressUpdate, SinkClosed, SinkResult, Terminal, WorkerEvent,
    WorkflowStep,
};
pub use execution::{
    BenchmarkTask, ExecutionContext, ExecutionOutcome, ExecutionPlacement, ExecutionRequest,
    ExecutionTask, TaskStatus,
};
