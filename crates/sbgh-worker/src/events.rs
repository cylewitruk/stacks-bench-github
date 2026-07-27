use std::time::Duration;

use async_trait::async_trait;
use sbgh_driver::{
    EventSink, ExecutionRequest, PhaseLabel, ProgressUpdate, SinkClosed, SinkResult, Terminal,
    WorkerEvent,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::execution::{ExecutionDependencies, execute};
use sbgh_driver::TaskStatus;

pub struct ChannelSink {
    tx: mpsc::Sender<WorkerEvent>,
}

impl ChannelSink {
    pub fn new(tx: mpsc::Sender<WorkerEvent>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl EventSink for ChannelSink {
    async fn phase(&self, label: PhaseLabel, elapsed: Duration) -> SinkResult {
        self.tx
            .send(WorkerEvent::Phase { label, elapsed })
            .await
            .map_err(|_| SinkClosed)
    }

    async fn heartbeat(&self, label: PhaseLabel, elapsed: Duration) {
        let _ = self
            .tx
            .try_send(WorkerEvent::Heartbeat { label, elapsed });
    }

    async fn progress(&self, progress: ProgressUpdate) {
        let _ = self
            .tx
            .try_send(WorkerEvent::Progress(progress));
    }
}

pub async fn run_execution(
    request: ExecutionRequest,
    dependencies: ExecutionDependencies,
    events_tx: mpsc::Sender<WorkerEvent>,
    token: CancellationToken,
) -> bool {
    let job_id = request.context.job_id;
    let sink = ChannelSink::new(events_tx.clone());
    let outcome = execute(request, dependencies, &sink, &token).await;
    let terminal = if token.is_cancelled() {
        tracing::warn!(%job_id, "run cancelled; reporting aborted");
        Terminal::Aborted
    } else {
        match outcome {
            Ok(outcome) => match outcome.status {
                TaskStatus::Completed => Terminal::Completed {
                    summary: outcome.summary,
                    block_validation: outcome.block_validation,
                },
                TaskStatus::Failed(error) => Terminal::Failed {
                    error,
                    summary: outcome.summary,
                },
            },
            Err(error) => {
                tracing::error!(%job_id, error = ?error, "execution returned setup error");
                Terminal::SetupError { error: format!("{error:#}") }
            }
        }
    };
    let completed = matches!(terminal, Terminal::Completed { .. });
    let _ = events_tx
        .send(WorkerEvent::Finished(terminal))
        .await;
    completed
}

#[cfg(test)]
mod tests {
    use sbgh_driver::{ProgressUpdate, WorkflowStep};

    use super::*;

    fn progress(n: u64) -> ProgressUpdate {
        ProgressUpdate {
            workflow_step: WorkflowStep::Run,
            run_index: 0,
            requested_run_count: 1,
            phase: "replay".into(),
            progress: n,
            total: Some(100),
            message: None,
        }
    }

    #[tokio::test]
    async fn progress_events_are_dropped_under_backpressure() {
        let (tx, mut rx) = mpsc::channel(1);
        let sink = ChannelSink::new(tx);

        sink.progress(progress(1))
            .await;
        sink.progress(progress(2))
            .await;

        match rx.recv().await {
            Some(WorkerEvent::Progress(update)) => assert_eq!(update.progress, 1),
            other => panic!("expected first progress event, got {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }
}
