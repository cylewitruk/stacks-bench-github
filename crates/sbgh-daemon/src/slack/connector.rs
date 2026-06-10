//! The Slack mention → benchmark orchestration (item `0002`, v5 Phase 1).
//!
//! Flow for one `@BenchBot bench …` mention, in this order:
//!   1. **authz** (team + user allowlist) — *before* anything else;
//!   2. **resolve** the workload (the deterministic parser seam);
//!   3. **enqueue** an ad-hoc job (default repo/rev, no webhook) carrying the
//!      Slack reporting provenance;
//!   4. **react** ⏳ on the request — the only channel-visible signal for an
//!      accepted job.
//!
//! A rejection at step 1 or 2 is an **ephemeral** reply (invoker-only) with
//! **no enqueue and no reaction**, so a denied/garbled request leaves the
//! channel untouched. The code-under-test target (the configured default repo
//! resolved to its FK ids) is held as [`SlackJobTarget`]; resolving
//! `default_repository` → ids is the wiring slice's job, not this layer's.

use std::sync::Arc;

use sbgh_core::config::SlackConfig;
use sbgh_core::db::JobStore;
use sbgh_core::models::{GitRefKind, JobKind, NewJob, QueuedEventDetail, TriggerKind};

use crate::slack::client::{QUEUED_REACTION, SlackClient};
use crate::slack::target::SlackJobTarget;
use crate::slack::workload::resolve_workload;

/// One inbound Slack mention, normalized from the Socket Mode `app_mention`
/// envelope (the receive loop, wiring slice, builds these).
#[derive(Debug, Clone)]
pub struct MentionEvent {
    /// Slack workspace id (`team_id`) — checked against the allowlist.
    pub team_id: String,
    /// Slack user id of the sender — checked against the allowlist.
    pub user: String,
    /// Channel the mention was posted in.
    pub channel: String,
    /// Timestamp of the request message — the thread anchor + the message the
    /// status reaction is added to.
    pub message_ts: String,
    /// Raw message text, including the leading `<@bot>` mention.
    pub text: String,
}

pub struct SlackConnector {
    cfg: SlackConfig,
    target: SlackJobTarget,
    jobs: Arc<dyn JobStore>,
    client: Arc<dyn SlackClient>,
}

impl SlackConnector {
    pub fn new(
        cfg: SlackConfig,
        target: SlackJobTarget,
        jobs: Arc<dyn JobStore>,
        client: Arc<dyn SlackClient>,
    ) -> Self {
        Self { cfg, target, jobs, client }
    }

    /// Handle one mention end to end. Never returns an error — every failure is
    /// either an ephemeral reply (rejection) or a logged best-effort miss; the
    /// caller (receive loop) has already acked the envelope.
    pub async fn handle_mention(&self, event: MentionEvent) {
        // 1. Authz FIRST — an off-allowlist sender is rejected without parsing (or,
        //    later, spending an LLM call) on their input.
        if !self.is_authorized(&event.team_id, &event.user) {
            self.reject(&event, "not authorized to run benchmarks here")
                .await;
            return;
        }

        // 2. Resolve the workload (mention stripped → the parser seam).
        let spec = match resolve_workload(strip_leading_mention(&event.text)) {
            Ok(spec) => spec,
            Err(e) => {
                self.reject(&event, &e.to_string())
                    .await;
                return;
            }
        };

        // 3. Enqueue an ad-hoc job. Default repo (target ids) + rev (`--rev` override,
        //    else the configured default); the parsed workload becomes `bench_args`;
        //    channel/message_ts ride along as reporting provenance.
        let rev = spec
            .rev
            .clone()
            .unwrap_or_else(|| self.cfg.default_rev.clone());
        let detail = serde_json::to_value(QueuedEventDetail::SlackAdhoc {
            channel: event.channel.clone(),
            message_ts: event.message_ts.clone(),
            bench_args: spec.to_bench_args(),
        })
        .expect("QueuedEventDetail serializes");
        let new_job = NewJob {
            github_installation_id: self.target.installation_id,
            github_repo_id: self.target.repo_id,
            job_kind: JobKind::AdHoc,
            trigger_kind: TriggerKind::SlackAdhoc,
            // `Branch` is the neutral default for a default-rev like `develop`;
            // `git_commit_hash` is `None`, so the rev resolves to a commit at
            // claim time — the reporter's `prepare` resolves a Slack job's bare
            // rev (branch/tag/SHA) via `resolve_commit`, so it passes the
            // empty-commit guard like a PR-head or tag job.
            git_ref_kind: GitRefKind::Branch,
            git_ref_display: rev,
            git_commit_hash: None,
            git_committed_at: None,
            workload_key: None,
        };

        if let Err(e) = self
            .jobs
            .create_adhoc_job(&new_job, &detail)
            .await
        {
            tracing::error!(error = %e, "slack: enqueue failed");
            self.reject(&event, "couldn't enqueue the benchmark — please retry")
                .await;
            return;
        }

        // 4. Exactly one ⏳ reaction on the request — the accepted-job signal.
        if let Err(e) = self
            .client
            .add_reaction(&event.channel, &event.message_ts, QUEUED_REACTION)
            .await
        {
            tracing::warn!(error = %e, "slack: add_reaction failed (job still enqueued)");
        }
    }

    /// A mention is authorized iff BOTH its workspace AND its sender are
    /// allowlisted (the authenticated socket says nothing about *who* sent it).
    fn is_authorized(&self, team_id: &str, user: &str) -> bool {
        self.cfg
            .allowed_team_ids
            .iter()
            .any(|t| t == team_id)
            && self
                .cfg
                .allowed_user_ids
                .iter()
                .any(|u| u == user)
    }

    async fn reject(&self, event: &MentionEvent, reason: &str) {
        if let Err(e) = self
            .client
            .post_ephemeral(&event.channel, &event.user, reason)
            .await
        {
            tracing::warn!(error = %e, "slack: post_ephemeral (rejection) failed");
        }
    }
}

/// Drop a leading `<@bot>` mention token so the remaining text is the command
/// (the resolver is Slack-agnostic). `<@U…>` alone → empty (→ an empty-request
/// rejection downstream).
fn strip_leading_mention(text: &str) -> &str {
    let t = text.trim_start();
    if let Some(rest) = t.strip_prefix("<@") {
        // Past the closing `>` of the mention, then any following space.
        match rest.split_once('>') {
            Some((_, after)) => after.trim_start(),
            None => "",
        }
    } else {
        t
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use sbgh_core::db::InMemoryJobStore;
    use sbgh_core::models::TriggerKind;

    use super::*;

    /// Records every Slack call so tests can assert exactly what was (and
    /// wasn't) posted.
    #[derive(Default)]
    struct FakeSlackClient {
        ephemerals: Mutex<Vec<(String, String, String)>>, // (channel, user, text)
        threads: Mutex<Vec<(String, String, String)>>,    // (channel, thread_ts, text)
        reactions: Mutex<Vec<(String, String, String)>>,  // (channel, ts, reaction)
        removed: Mutex<Vec<(String, String, String)>>,    // (channel, ts, reaction)
    }

    #[async_trait]
    impl SlackClient for FakeSlackClient {
        async fn post_ephemeral(
            &self,
            channel: &str,
            user: &str,
            text: &str,
        ) -> anyhow::Result<()> {
            self.ephemerals
                .lock()
                .unwrap()
                .push((channel.into(), user.into(), text.into()));
            Ok(())
        }
        async fn post_in_thread(
            &self,
            channel: &str,
            thread_ts: &str,
            text: &str,
        ) -> anyhow::Result<()> {
            self.threads
                .lock()
                .unwrap()
                .push((channel.into(), thread_ts.into(), text.into()));
            Ok(())
        }
        async fn post_blocks_in_thread(
            &self,
            _channel: &str,
            _thread_ts: &str,
            _blocks: &serde_json::Value,
            _fallback: &str,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn add_reaction(
            &self,
            channel: &str,
            ts: &str,
            reaction: &str,
        ) -> anyhow::Result<()> {
            self.reactions
                .lock()
                .unwrap()
                .push((channel.into(), ts.into(), reaction.into()));
            Ok(())
        }
        async fn remove_reaction(
            &self,
            channel: &str,
            ts: &str,
            reaction: &str,
        ) -> anyhow::Result<()> {
            self.removed
                .lock()
                .unwrap()
                .push((channel.into(), ts.into(), reaction.into()));
            Ok(())
        }
    }

    fn cfg() -> SlackConfig {
        SlackConfig {
            enabled: true,
            app_token: Some("xapp-x".into()),
            bot_token: Some("xoxb-x".into()),
            default_repository: "octo/core".into(),
            default_rev: "develop".into(),
            allowed_team_ids: vec!["T_OK".into()],
            allowed_user_ids: vec!["U_OK".into()],
        }
    }

    const TARGET: SlackJobTarget = SlackJobTarget {
        installation_id: 100,
        repo_id: 10,
    };

    fn event(text: &str) -> MentionEvent {
        MentionEvent {
            team_id: "T_OK".into(),
            user: "U_OK".into(),
            channel: "C1".into(),
            message_ts: "1700000000.000100".into(),
            text: text.into(),
        }
    }

    /// (connector, in-memory store handle, fake slack handle)
    fn harness() -> (SlackConnector, Arc<InMemoryJobStore>, Arc<FakeSlackClient>) {
        let store = Arc::new(InMemoryJobStore::new());
        let client = Arc::new(FakeSlackClient::default());
        let connector = SlackConnector::new(cfg(), TARGET, store.clone(), client.clone());
        (connector, store, client)
    }

    #[tokio::test]
    async fn accepted_request_enqueues_and_reacts_once() {
        let (c, store, slack) = harness();
        c.handle_mention(event("<@U07BOT> bench --block 184231 --repetitions 5"))
            .await;

        // Exactly one ad-hoc job, with the resolved workload + default rev.
        let jobs = store.all_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].trigger_kind, TriggerKind::SlackAdhoc);
        assert_eq!(jobs[0].github_installation_id, 100);
        assert_eq!(jobs[0].github_repo_id, 10);
        assert_eq!(jobs[0].git_ref_display, "develop", "default rev when no --rev");

        // The queued detail carries the channel/ts + parsed bench_args.
        let queued = store
            .queued_event(jobs[0].id)
            .await
            .unwrap()
            .unwrap();
        let detail: QueuedEventDetail = serde_json::from_value(queued.detail.unwrap().0).unwrap();
        match detail {
            QueuedEventDetail::SlackAdhoc {
                channel,
                message_ts,
                bench_args,
            } => {
                assert_eq!(channel, "C1");
                assert_eq!(message_ts, "1700000000.000100");
                assert_eq!(bench_args, vec!["--block", "184231", "--repetitions", "5"]);
            }
            other => panic!("expected SlackAdhoc detail, got {other:?}"),
        }

        // Exactly one ⏳ reaction on the request; no ephemeral.
        let reactions = slack
            .reactions
            .lock()
            .unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0], ("C1".into(), "1700000000.000100".into(), QUEUED_REACTION.into()));
        assert!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn rev_override_sets_the_ref() {
        let (c, store, _slack) = harness();
        c.handle_mention(event("<@U07BOT> bench --block 1 --rev feature/x"))
            .await;
        assert_eq!(store.all_jobs()[0].git_ref_display, "feature/x");
    }

    #[tokio::test]
    async fn unauthorized_team_is_rejected_without_enqueue() {
        let (c, store, slack) = harness();
        let mut ev = event("<@U07BOT> bench --block 1");
        ev.team_id = "T_EVIL".into();
        c.handle_mention(ev).await;

        assert!(store.all_jobs().is_empty(), "no job for an off-allowlist workspace");
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
                .is_empty(),
            "no reaction"
        );
        assert_eq!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .len(),
            1,
            "one ephemeral rejection"
        );
    }

    #[tokio::test]
    async fn unauthorized_user_is_rejected_without_enqueue() {
        let (c, store, slack) = harness();
        let mut ev = event("<@U07BOT> bench --block 1");
        ev.user = "U_EVIL".into();
        c.handle_mention(ev).await;

        assert!(store.all_jobs().is_empty());
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            slack
                .ephemerals
                .lock()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn malformed_workload_is_rejected_without_enqueue() {
        let (c, store, slack) = harness();
        // txid + block are mutually exclusive.
        let txid = "0x".to_string() + &"1".repeat(64);
        c.handle_mention(event(&format!("<@U07BOT> bench --block 1 --txid {txid}")))
            .await;

        assert!(store.all_jobs().is_empty(), "no job for an unresolvable request");
        assert!(
            slack
                .reactions
                .lock()
                .unwrap()
                .is_empty()
        );
        let eph = slack
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("only one of"),
            "ephemeral carries the parse reason: {}",
            eph[0].2
        );
    }

    /// Ordering guarantee: a malformed command from an UNAUTHORIZED sender is
    /// rejected for **authz**, not parsing — authz runs before resolution.
    #[tokio::test]
    async fn authz_is_checked_before_resolution() {
        let (c, _store, slack) = harness();
        let mut ev = event("<@U07BOT> bench --totally-bogus");
        ev.user = "U_EVIL".into();
        c.handle_mention(ev).await;

        let eph = slack
            .ephemerals
            .lock()
            .unwrap();
        assert_eq!(eph.len(), 1);
        assert!(
            eph[0]
                .2
                .contains("not authorized"),
            "authz rejection must win over the parse error: {}",
            eph[0].2
        );
    }
}
