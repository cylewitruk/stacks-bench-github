//! Narrow Slack fakes for downstream and crate-local tests.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use sbgh_core::db::NewBenchmarkSpec;
use sbgh_core::models::{Job, JobStatus};
use uuid::Uuid;

use crate::{
    BenchmarkQueue, FoundMessage, ReportingIdentity, Result, SlackClient, SlackError,
    SlackMessageTarget,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostedMessage {
    pub target: SlackMessageTarget,
    pub ts: String,
    pub text: String,
    pub identity: ReportingIdentity,
    pub snapshot_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatedMessage {
    pub channel: String,
    pub ts: String,
    pub text: String,
    pub identity: ReportingIdentity,
    pub snapshot_version: u64,
}

#[derive(Default)]
pub struct FakeSlackClient {
    messages: Mutex<Vec<PostedMessage>>,
    updates: Mutex<Vec<UpdatedMessage>>,
    ephemeral: Mutex<Vec<(String, String, String)>>,
    reactions: Mutex<Vec<(String, String, String)>>,
    reaction_calls: Mutex<Vec<(String, String)>>,
    lose_post_response: Mutex<bool>,
    fail_lookup: Mutex<bool>,
    fail_update: Mutex<bool>,
}

impl FakeSlackClient {
    pub fn posts(&self) -> Vec<PostedMessage> {
        self.messages
            .lock()
            .unwrap()
            .clone()
    }

    pub fn updates(&self) -> Vec<UpdatedMessage> {
        self.updates
            .lock()
            .unwrap()
            .clone()
    }

    pub fn ephemeral(&self) -> Vec<(String, String, String)> {
        self.ephemeral
            .lock()
            .unwrap()
            .clone()
    }

    pub fn reactions(&self) -> Vec<(String, String, String)> {
        self.reactions
            .lock()
            .unwrap()
            .clone()
    }

    pub fn reaction_calls(&self) -> Vec<(String, String)> {
        self.reaction_calls
            .lock()
            .unwrap()
            .clone()
    }

    pub fn lose_next_post_response(&self) {
        *self
            .lose_post_response
            .lock()
            .unwrap() = true;
    }

    pub fn fail_next_lookup(&self) {
        *self
            .fail_lookup
            .lock()
            .unwrap() = true;
    }

    pub fn fail_next_update(&self) {
        *self
            .fail_update
            .lock()
            .unwrap() = true;
    }

    pub fn seed_message(
        &self,
        target: SlackMessageTarget,
        identity: ReportingIdentity,
        snapshot_version: u64,
        text: impl Into<String>,
    ) -> String {
        let mut messages = self.messages.lock().unwrap();
        let ts = format!("2.{}", messages.len() + 1);
        messages.push(PostedMessage {
            target,
            ts: ts.clone(),
            text: text.into(),
            identity,
            snapshot_version,
        });
        ts
    }
}

#[async_trait]
impl SlackClient for FakeSlackClient {
    async fn post_ephemeral(&self, channel: &str, user: &str, text: &str) -> Result<()> {
        self.ephemeral
            .lock()
            .unwrap()
            .push((channel.to_string(), user.to_string(), text.to_string()));
        Ok(())
    }

    async fn post_message(
        &self,
        target: &SlackMessageTarget,
        text: &str,
        identity: &ReportingIdentity,
        snapshot_version: u64,
    ) -> Result<String> {
        let ts = self.seed_message(target.clone(), identity.clone(), snapshot_version, text);
        if std::mem::take(
            &mut *self
                .lose_post_response
                .lock()
                .unwrap(),
        ) {
            return Err(SlackError::Protocol("simulated lost post response".into()));
        }
        Ok(ts)
    }

    async fn update_message(
        &self,
        channel: &str,
        ts: &str,
        text: &str,
        identity: &ReportingIdentity,
        snapshot_version: u64,
    ) -> Result<()> {
        self.updates
            .lock()
            .unwrap()
            .push(UpdatedMessage {
                channel: channel.to_string(),
                ts: ts.to_string(),
                text: text.to_string(),
                identity: identity.clone(),
                snapshot_version,
            });
        if std::mem::take(
            &mut *self
                .fail_update
                .lock()
                .unwrap(),
        ) {
            return Err(SlackError::Protocol("simulated update failure".into()));
        }
        if let Some(message) = self
            .messages
            .lock()
            .unwrap()
            .iter_mut()
            .find(|message| message.ts == ts && message.target.channel == channel)
        {
            message.text = text.to_string();
            message.snapshot_version = snapshot_version;
        }
        Ok(())
    }

    async fn find_messages(
        &self,
        target: &SlackMessageTarget,
        identity: &ReportingIdentity,
    ) -> Result<Vec<FoundMessage>> {
        if std::mem::take(
            &mut *self
                .fail_lookup
                .lock()
                .unwrap(),
        ) {
            return Err(SlackError::Protocol("simulated lookup failure".into()));
        }
        Ok(self
            .messages
            .lock()
            .unwrap()
            .iter()
            .filter(|message| &message.target == target && &message.identity == identity)
            .map(|message| FoundMessage {
                ts: message.ts.clone(),
                text: message.text.clone(),
                snapshot_version: message.snapshot_version,
            })
            .collect())
    }

    async fn add_reaction(&self, channel: &str, ts: &str, reaction: &str) -> Result<()> {
        self.reaction_calls
            .lock()
            .unwrap()
            .push(("add".into(), reaction.to_string()));
        self.reactions
            .lock()
            .unwrap()
            .push((channel.to_string(), ts.to_string(), reaction.to_string()));
        Ok(())
    }

    async fn remove_reaction(&self, channel: &str, ts: &str, reaction: &str) -> Result<()> {
        self.reaction_calls
            .lock()
            .unwrap()
            .push(("remove".into(), reaction.to_string()));
        let mut reactions = self.reactions.lock().unwrap();
        if let Some(index) = reactions
            .iter()
            .position(|entry| entry.0 == channel && entry.1 == ts && entry.2 == reaction)
        {
            reactions.remove(index);
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct RecordingBenchmarkQueue {
    calls: Mutex<Vec<RecordedQueueCall>>,
    fail_create: AtomicBool,
}

struct RecordedQueueCall {
    job: Job,
    specs: Vec<NewBenchmarkSpec>,
    queued_event_detail: serde_json::Value,
    plan_message_ts: Option<String>,
}

impl RecordingBenchmarkQueue {
    pub fn fail_create(&self) {
        self.fail_create
            .store(true, Ordering::SeqCst);
    }

    pub fn jobs(&self) -> Vec<Job> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.job.clone())
            .collect()
    }

    pub fn requested_specs_for_group(&self, group_id: Uuid) -> Vec<NewBenchmarkSpec> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .find(|call| call.job.benchmark_group_id == group_id)
            .map(|call| call.specs.clone())
            .unwrap_or_default()
    }

    pub fn queued_event_detail(&self, job_id: Uuid) -> Option<serde_json::Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .find(|call| call.job.id == job_id)
            .map(|call| {
                call.queued_event_detail
                    .clone()
            })
    }

    pub fn plan_message_ts(&self, job_id: Uuid) -> Option<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .find(|call| call.job.id == job_id)
            .and_then(|call| call.plan_message_ts.clone())
    }
}

#[async_trait]
impl BenchmarkQueue for RecordingBenchmarkQueue {
    async fn create_unlinked_benchmark_group(
        &self,
        first_job_id: Uuid,
        requested_specs: &[NewBenchmarkSpec],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> sbgh_core::Result<Job> {
        if self
            .fail_create
            .load(Ordering::SeqCst)
        {
            return Err(std::io::Error::other("injected benchmark queue failure").into());
        }
        let first = requested_specs
            .first()
            .ok_or_else(|| std::io::Error::other("benchmark group needs at least one spec"))?;
        let now = Utc::now();
        let group_id = Uuid::new_v4();
        let new = &first.new_job;
        let job = Job {
            id: first_job_id,
            benchmark_group_id: group_id,
            benchmark_spec_id: Uuid::new_v4(),
            benchmark_run_index: 0,
            github_installation_id: new.github_installation_id,
            github_repo_id: new.github_repo_id,
            status: JobStatus::Queued,
            source: new.axes.source,
            intent: new.axes.intent,
            task_kind: new.axes.task_kind,
            build_target: new.axes.build_target,
            git_ref_kind: new.git_ref_kind,
            git_ref_display: new.git_ref_display.clone(),
            git_commit_hash: new.git_commit_hash.clone(),
            git_committed_at: new.git_committed_at,
            workload_key: new.workload_key.clone(),
            claim_token: None,
            claimed_at: None,
            created_at: now,
            updated_at: now,
        };
        self.calls
            .lock()
            .unwrap()
            .push(RecordedQueueCall {
                job: job.clone(),
                specs: requested_specs.to_vec(),
                queued_event_detail: queued_event_detail.clone(),
                plan_message_ts: plan_message_ts.map(str::to_owned),
            });
        Ok(job)
    }
}
