#[cfg(feature = "test-support")]
pub mod in_memory_ingest;
#[cfg(feature = "test-support")]
pub mod in_memory_jobs;
#[cfg(feature = "test-support")]
pub mod in_memory_webhook;
pub mod ingest;
pub mod jobs;
pub mod migrate;
pub mod pool;
pub mod postgres_ingest;
pub mod postgres_jobs;
pub mod postgres_webhook;
pub mod webhook;

#[cfg(feature = "test-support")]
pub use in_memory_ingest::InMemoryIngestStore;
#[cfg(feature = "test-support")]
pub use in_memory_jobs::InMemoryJobStore;
#[cfg(feature = "test-support")]
pub use in_memory_webhook::{InMemoryWebhookInbox, InMemoryWebhookRow, SeedWebhook};
pub use ingest::{IngestOutcome, IngestStore, NewWebhook};
pub use jobs::JobStore;
pub use migrate::migrate;
pub use pool::{Pool, connect};
pub use postgres_ingest::PostgresIngestStore;
pub use postgres_jobs::PostgresJobStore;
pub use postgres_webhook::PostgresWebhookInbox;
pub use webhook::{ClaimedWebhook, WebhookInbox};
