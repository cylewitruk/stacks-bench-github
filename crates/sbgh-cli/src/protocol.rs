//! Compatibility-floor controls with explicit audit and rollback evidence.

use anyhow::ensure;
use clap::Subcommand;
use sbgh_api::{Client, ProtocolPolicyChangeRequest};
use uuid::Uuid;

use crate::output::{self, OutputArgs};

/// Deliberate protocol policy operations.
#[derive(Debug, Subcommand)]
pub enum Action {
    /// Inspect the current floor without changing worker state.
    Status(OutputArgs),
    /// Change the floor during paused, quiescent maintenance; also permits rollback.
    SetFloor {
        /// Lowest revision to admit; server validates its supported window.
        #[arg(long)]
        minimum_revision: u16,
        /// Human explanation retained in the audit.
        #[arg(long)]
        reason: String,
        /// Reference to completed rollback qualification.
        #[arg(long)]
        rollback_evidence: String,
        /// Reuse this UUID only to retry the same request after an uncertain response.
        #[arg(long)]
        request_id: Option<Uuid>,
        /// Presentation of the original receipt for this request.
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// Use the authenticated API; never silently pause, drain, or resume workers.
pub async fn run(client: &Client, action: Action) -> anyhow::Result<()> {
    let (policy, output) = match action {
        Action::Status(output) => (
            client
                .protocol_policy()
                .await?,
            output,
        ),
        Action::SetFloor {
            minimum_revision,
            reason,
            rollback_evidence,
            request_id,
            output,
        } => {
            for (value, limit) in [(&reason, 1024), (&rollback_evidence, 2048)] {
                ensure!(
                    !value.trim().is_empty() && value.chars().count() <= limit,
                    "reason and rollback evidence must be nonempty and bounded"
                );
            }
            let request_id = request_id
                .unwrap_or_else(Uuid::new_v4)
                .to_string();
            eprintln!("protocol request_id={request_id}");
            let policy = client
                .change_protocol_policy(&ProtocolPolicyChangeRequest {
                    request_id,
                    minimum_revision,
                    reason,
                    rollback_evidence,
                })
                .await?;
            (policy, output)
        }
    };
    let human = format!(
        "protocol_floor={} generation={} actor={} reason={} rollback_evidence={}\n",
        policy.minimum_revision,
        policy.generation,
        output::text(&policy.actor),
        output::text(&policy.reason),
        output::text(&policy.rollback_evidence)
    );
    output::print(&policy, &human, &output)
}
