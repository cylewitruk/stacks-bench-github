//! Canonical Slack snapshot model, renderer, and convergent publisher.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{FoundMessage, Result, SlackClient, SlackError};

const BAR_WIDTH: u64 = 20;
const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(30);

/// Stable opaque identity embedded in Slack message metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReportingIdentity(String);

impl ReportingIdentity {
    /// Derive a stable identity from Slack-authenticated request coordinates.
    ///
    /// The hash prevents workspace/channel identifiers from being exposed in
    /// message metadata while remaining stable across Socket Mode redelivery.
    pub fn for_request(team_id: &str, channel: &str, message_ts: &str) -> Self {
        let mut hasher = Sha256::new();
        for part in [team_id, channel, message_ts] {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        Self(hex::encode(hasher.finalize()))
    }

    pub fn parse(value: impl Into<String>) -> std::result::Result<Self, InvalidReportingIdentity> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(InvalidReportingIdentity);
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("reporting identity must be a 64-character hexadecimal digest")]
pub struct InvalidReportingIdentity;

/// Conversation and request thread containing the canonical bot message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SlackMessageTarget {
    pub channel: String,
    pub thread_ts: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SlackStatus {
    Queued,
    Preparing,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl SlackStatus {
    fn emoji(self) -> &'static str {
        match self {
            Self::Queued => ":hourglass_flowing_sand:",
            Self::Preparing => ":construction:",
            Self::Running => ":rocket:",
            Self::Completed => ":white_check_mark:",
            Self::Failed => ":x:",
            Self::Cancelled => ":octagonal_sign:",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Queued => "Queued",
            Self::Preparing => "Preparing",
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackProgress {
    pub label: String,
    pub current: u64,
    pub total: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunPosition {
    pub current: u32,
    pub total: u32,
}

/// Complete provider-neutral state rendered into one ordinary Slack message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackProgressView {
    pub identity: ReportingIdentity,
    /// Monotonic projection order. A stale update with a lower or equal
    /// version cannot replace a newer snapshot.
    pub version: u64,
    pub status: SlackStatus,
    pub title: String,
    pub reference: String,
    pub commit: Option<String>,
    pub phase: Option<String>,
    pub progress: Option<SlackProgress>,
    pub run: Option<RunPosition>,
    /// Daemon-selected result/comparison lines. They are rendered as literal
    /// text, not interpreted as Slack markup.
    pub details: Vec<String>,
    /// Trusted daemon-created HTTP(S) links.
    pub links: Vec<(String, String)>,
}

impl SlackProgressView {
    pub fn queued(
        identity: ReportingIdentity,
        title: impl Into<String>,
        reference: impl Into<String>,
    ) -> Self {
        Self {
            identity,
            version: 0,
            status: SlackStatus::Queued,
            title: title.into(),
            reference: reference.into(),
            commit: None,
            phase: Some("waiting for a worker".into()),
            progress: None,
            run: None,
            details: Vec::new(),
            links: Vec::new(),
        }
    }
}

/// Render a full canonical message without consulting previous Slack content.
pub fn render_snapshot(view: &SlackProgressView) -> String {
    let mut out = String::new();
    let title = literal(&view.title);
    let reference = literal(&view.reference);
    let _ = writeln!(out, "{} *{}* — {}", view.status.emoji(), title, view.status.label());
    let _ = writeln!(out, "Ref: `{}`", inline_code(&reference));
    if let Some(commit) = view.commit.as_deref() {
        let short = commit
            .get(..8)
            .unwrap_or(commit);
        let _ = writeln!(out, "Commit: `{}`", inline_code(&literal(short)));
    }
    if let Some(run) = view.run {
        let current = run
            .current
            .clamp(1, run.total.max(1));
        let _ = writeln!(out, "Run: {current}/{}", run.total.max(1));
    }

    if view.phase.is_some() || view.progress.is_some() {
        out.push_str("\n```text\n");
        if let Some(progress) = &view.progress {
            let (current, total) = bounded_progress(progress.current, progress.total);
            let label = fixed_label(&progress.label);
            match total {
                Some(total) => {
                    let filled = current.saturating_mul(BAR_WIDTH) / total.max(1);
                    let _ = writeln!(
                        out,
                        "{label} [{}{}] {current}/{total}",
                        "█".repeat(filled as usize),
                        "░".repeat((BAR_WIDTH - filled) as usize),
                    );
                }
                None => {
                    let _ = writeln!(out, "{label} {}", thousands(current));
                }
            }
        } else if let Some(phase) = view.phase.as_deref() {
            let _ = writeln!(out, "{:<16} {}", "Phase", literal_code(phase));
        }
        out.push_str("```\n");
    }

    if !view.details.is_empty() {
        out.push('\n');
        for line in &view.details {
            let _ = writeln!(out, "• {}", literal(line));
        }
    }
    if !view.links.is_empty() {
        out.push('\n');
        for (label, url) in &view.links {
            if let Some(url) = safe_http_url(url) {
                let _ = writeln!(out, "<{}|{}>", url, link_label(label));
            }
        }
    }
    out.trim_end().to_string()
}

fn bounded_progress(current: u64, total: Option<u64>) -> (u64, Option<u64>) {
    match total {
        Some(0) | None => (current, None),
        Some(total) => (current.min(total), Some(total)),
    }
}

fn fixed_label(value: &str) -> String {
    let value = literal_code(value);
    let mut chars = value.chars();
    let mut label: String = chars
        .by_ref()
        .take(16)
        .collect();
    if chars.next().is_some() {
        label.pop();
        label.push('…');
    }
    format!("{label:<16}")
}

fn literal(value: &str) -> String {
    value
        .replace('&', "＆")
        .replace('<', "‹")
        .replace('>', "›")
        .replace("@channel", "＠channel")
        .replace("@here", "＠here")
        .replace("@everyone", "＠everyone")
        .replace("```", "'''")
        .replace(['\r', '\n'], " ")
}

fn literal_code(value: &str) -> String {
    literal(value).replace('`', "'")
}

fn inline_code(value: &str) -> String {
    value.replace('`', "'")
}

fn link_label(value: &str) -> String {
    literal(value).replace('|', "¦")
}

fn safe_http_url(value: &str) -> Option<&str> {
    ((value.starts_with("https://") || value.starts_with("http://"))
        && !value
            .chars()
            .any(|character| character.is_control() || matches!(character, '<' | '>' | '|')))
    .then_some(value)
}

fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, byte) in digits.bytes().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(byte as char);
    }
    out
}

#[async_trait]
pub trait MessageIdentityStore: Send + Sync {
    async fn persist_message_ts(&self, message_ts: &str) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishUrgency {
    Debounced,
    Immediate,
}

struct PublisherState {
    message_ts: Option<String>,
    last_version: Option<u64>,
    last_text: Option<String>,
    last_update_at: Option<Instant>,
    pending: Option<SlackProgressView>,
    timer_generation: u64,
}

struct AppliedMessage {
    ts: String,
    text: String,
    version: u64,
}

/// Convergent publisher for one reporting identity.
pub struct SlackSnapshotPublisher {
    client: Arc<dyn SlackClient>,
    target: SlackMessageTarget,
    identity: ReportingIdentity,
    identity_store: Option<Arc<dyn MessageIdentityStore>>,
    update_interval: Duration,
    apply_lock: Mutex<()>,
    state: Mutex<PublisherState>,
}

impl SlackSnapshotPublisher {
    pub fn new(
        client: Arc<dyn SlackClient>,
        target: SlackMessageTarget,
        identity: ReportingIdentity,
        message_ts: Option<String>,
        identity_store: Option<Arc<dyn MessageIdentityStore>>,
    ) -> Arc<Self> {
        Self::with_update_interval(
            client,
            target,
            identity,
            message_ts,
            identity_store,
            DEFAULT_UPDATE_INTERVAL,
        )
    }

    pub fn with_update_interval(
        client: Arc<dyn SlackClient>,
        target: SlackMessageTarget,
        identity: ReportingIdentity,
        message_ts: Option<String>,
        identity_store: Option<Arc<dyn MessageIdentityStore>>,
        update_interval: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            target,
            identity,
            identity_store,
            update_interval,
            apply_lock: Mutex::new(()),
            state: Mutex::new(PublisherState {
                message_ts,
                last_version: None,
                last_text: None,
                last_update_at: None,
                pending: None,
                timer_generation: 0,
            }),
        })
    }

    /// Publish now or coalesce behind the bounded update interval.
    pub async fn publish(
        self: &Arc<Self>,
        view: SlackProgressView,
        urgency: PublishUrgency,
    ) -> Result<()> {
        if view.identity != self.identity {
            return Err(SlackError::Reconciliation(
                "snapshot identity does not match publisher identity".into(),
            ));
        }

        let mut state = self.state.lock().await;
        if state
            .last_version
            .is_some_and(|version| view.version <= version)
        {
            return Ok(());
        }
        let rendered = render_snapshot(&view);
        if state.last_text.as_deref() == Some(&rendered) {
            state.last_version = Some(view.version);
            return Ok(());
        }

        if urgency == PublishUrgency::Debounced
            && let Some(last) = state.last_update_at
            && last.elapsed() < self.update_interval
        {
            if state
                .pending
                .as_ref()
                .is_none_or(|pending| view.version > pending.version)
            {
                state.pending = Some(view);
            }
            state.timer_generation = state
                .timer_generation
                .wrapping_add(1);
            let generation = state.timer_generation;
            let delay = self
                .update_interval
                .saturating_sub(last.elapsed());
            drop(state);
            let weak = Arc::downgrade(self);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                if let Some(publisher) = weak.upgrade()
                    && let Err(error) = publisher
                        .flush_pending(generation)
                        .await
                {
                    tracing::warn!(error = ?error, "slack: deferred snapshot update failed");
                }
            });
            return Ok(());
        }

        state.pending = None;
        state.timer_generation = state
            .timer_generation
            .wrapping_add(1);
        drop(state);
        self.apply(view, rendered)
            .await
    }

    async fn flush_pending(self: &Arc<Self>, generation: u64) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.timer_generation != generation {
            return Ok(());
        }
        let Some(view) = state.pending.take() else {
            return Ok(());
        };
        let rendered = render_snapshot(&view);
        drop(state);
        self.apply(view, rendered)
            .await
    }

    async fn apply(&self, view: SlackProgressView, rendered: String) -> Result<()> {
        let _apply_guard = self.apply_lock.lock().await;
        {
            let state = self.state.lock().await;
            if state
                .last_version
                .is_some_and(|version| view.version <= version)
            {
                return Ok(());
            }
        }
        let (message_ts, last_version) = {
            let state = self.state.lock().await;
            (state.message_ts.clone(), state.last_version)
        };
        let applied = match (message_ts, last_version) {
            (Some(ts), Some(_)) => {
                self.client
                    .update_message(
                        &self.target.channel,
                        &ts,
                        &rendered,
                        &self.identity,
                        view.version,
                    )
                    .await?;
                AppliedMessage {
                    ts,
                    text: rendered,
                    version: view.version,
                }
            }
            (expected_ts, _) => {
                self.reconcile_or_create(expected_ts.as_deref(), &view, &rendered)
                    .await?
            }
        };

        let mut state = self.state.lock().await;
        if state
            .last_version
            .is_none_or(|version| applied.version > version)
        {
            state.message_ts = Some(applied.ts);
            state.last_version = Some(applied.version);
            state.last_text = Some(applied.text);
            state.last_update_at = Some(Instant::now());
        }
        Ok(())
    }

    async fn reconcile_or_create(
        &self,
        expected_ts: Option<&str>,
        view: &SlackProgressView,
        rendered: &str,
    ) -> Result<AppliedMessage> {
        let matches = self
            .client
            .find_messages(&self.target, &self.identity)
            .await?;
        match matches.as_slice() {
            [] if expected_ts.is_none() => {
                let ts = self
                    .client
                    .post_message(&self.target, rendered, &self.identity, view.version)
                    .await?;
                self.persist_message_ts(&ts)
                    .await?;
                Ok(AppliedMessage {
                    ts,
                    text: rendered.to_string(),
                    version: view.version,
                })
            }
            [] => {
                // A persisted timestamp from a pre-metadata deployment is
                // still authoritative. Upgrade that exact message in place.
                let ts = expected_ts.expect("guarded by match");
                self.client
                    .update_message(
                        &self.target.channel,
                        ts,
                        rendered,
                        &self.identity,
                        view.version,
                    )
                    .await?;
                Ok(AppliedMessage {
                    ts: ts.to_string(),
                    text: rendered.to_string(),
                    version: view.version,
                })
            }
            [found] => {
                self.reconcile_found(expected_ts, found, view, rendered)
                    .await
            }
            _ => Err(SlackError::Reconciliation(format!(
                "multiple Slack messages match reporting identity {}; refusing to choose",
                self.identity.as_str()
            ))),
        }
    }

    async fn reconcile_found(
        &self,
        expected_ts: Option<&str>,
        found: &FoundMessage,
        view: &SlackProgressView,
        rendered: &str,
    ) -> Result<AppliedMessage> {
        if expected_ts.is_some_and(|expected| expected != found.ts.as_str()) {
            return Err(SlackError::Reconciliation(format!(
                "persisted Slack timestamp {expected_ts:?} does not match reconciled timestamp {}",
                found.ts
            )));
        }

        let authoritative_restart = expected_ts.is_some();
        let applied = if found.snapshot_version < view.version {
            self.update_found(found, view.version, rendered)
                .await?
        } else if found.snapshot_version == view.version {
            if found.text != rendered {
                return Err(SlackError::Reconciliation(format!(
                    "Slack snapshot {} has conflicting text at version {}",
                    found.ts, found.snapshot_version
                )));
            }
            AppliedMessage {
                ts: found.ts.clone(),
                text: found.text.clone(),
                version: found.snapshot_version,
            }
        } else if authoritative_restart && found.text != rendered {
            // The durable timestamp says this is the message to resume. Advance
            // beyond its metadata fence before applying current projected state.
            self.update_found(
                found,
                found
                    .snapshot_version
                    .saturating_add(1),
                rendered,
            )
            .await?
        } else {
            // Search-before-create redelivery found a newer projection. Adopt
            // it verbatim; an old queued/failure render must not regress it.
            AppliedMessage {
                ts: found.ts.clone(),
                text: found.text.clone(),
                version: found.snapshot_version,
            }
        };
        if expected_ts.is_none() {
            self.persist_message_ts(&applied.ts)
                .await?;
        }
        Ok(applied)
    }

    async fn update_found(
        &self,
        found: &FoundMessage,
        version: u64,
        rendered: &str,
    ) -> Result<AppliedMessage> {
        self.client
            .update_message(&self.target.channel, &found.ts, rendered, &self.identity, version)
            .await?;
        Ok(AppliedMessage {
            ts: found.ts.clone(),
            text: rendered.to_string(),
            version,
        })
    }

    async fn persist_message_ts(&self, ts: &str) -> Result<()> {
        if let Some(store) = &self.identity_store {
            store
                .persist_message_ts(ts)
                .await?;
        }
        Ok(())
    }

    pub async fn message_ts(&self) -> Option<String> {
        self.state
            .lock()
            .await
            .message_ts
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::test_support::FakeSlackClient;

    struct RecordingIdentityStore {
        attempts: AtomicUsize,
        fail_once: AtomicBool,
    }

    impl RecordingIdentityStore {
        fn failing_once() -> Self {
            Self {
                attempts: AtomicUsize::new(0),
                fail_once: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl MessageIdentityStore for RecordingIdentityStore {
        async fn persist_message_ts(&self, _message_ts: &str) -> Result<()> {
            self.attempts
                .fetch_add(1, Ordering::SeqCst);
            if self
                .fail_once
                .swap(false, Ordering::SeqCst)
            {
                return Err(SlackError::Persistence(
                    "simulated timestamp persistence failure".into(),
                ));
            }
            Ok(())
        }
    }

    fn identity() -> ReportingIdentity {
        ReportingIdentity::for_request("T1", "C1", "1.2")
    }

    fn target() -> SlackMessageTarget {
        SlackMessageTarget {
            channel: "C1".into(),
            thread_ts: "1.2".into(),
        }
    }

    fn view(version: u64, status: SlackStatus) -> SlackProgressView {
        SlackProgressView {
            identity: identity(),
            version,
            status,
            title: "Benchmark <@U1> ```".into(),
            reference: "feature/`x`".into(),
            commit: Some("abcdef0123456789".into()),
            phase: Some("running".into()),
            progress: Some(SlackProgress {
                label: "replay".into(),
                current: 25,
                total: Some(100),
            }),
            run: Some(RunPosition { current: 1, total: 2 }),
            details: vec!["@channel done".into()],
            links: vec![("Profiler data".into(), "https://example.test/result".into())],
        }
    }

    #[test]
    fn identity_is_stable_and_opaque() {
        let first = identity();
        assert_eq!(first, ReportingIdentity::for_request("T1", "C1", "1.2"));
        assert_eq!(first.as_str().len(), 64);
        assert!(!first.as_str().contains("C1"));
    }

    #[test]
    fn renderer_is_deterministic_aligned_and_literal() {
        let rendered = render_snapshot(&view(1, SlackStatus::Running));
        assert_eq!(rendered, render_snapshot(&view(1, SlackStatus::Running)));
        assert!(rendered.contains("```text\n"));
        assert!(rendered.contains("[█████░░░░░░░░░░░░░░░] 25/100"));
        assert!(!rendered.contains("<@U1>"));
        assert!(!rendered.contains("@channel"));
        assert_eq!(
            rendered
                .matches("```")
                .count(),
            2
        );
    }

    #[test]
    fn renderer_covers_every_lifecycle_status() {
        for (status, emoji, label) in [
            (SlackStatus::Queued, ":hourglass_flowing_sand:", "Queued"),
            (SlackStatus::Preparing, ":construction:", "Preparing"),
            (SlackStatus::Running, ":rocket:", "Running"),
            (SlackStatus::Completed, ":white_check_mark:", "Completed"),
            (SlackStatus::Failed, ":x:", "Failed"),
            (SlackStatus::Cancelled, ":octagonal_sign:", "Cancelled"),
        ] {
            let rendered = render_snapshot(&view(1, status));
            assert!(rendered.starts_with(&format!("{emoji} *Benchmark")), "{rendered}");
            assert!(
                rendered
                    .lines()
                    .next()
                    .unwrap()
                    .ends_with(label),
                "{rendered}"
            );
        }
    }

    #[test]
    fn renderer_rejects_slack_link_delimiters_and_control_characters() {
        for unsafe_url in [
            "https://example.test/result|<!channel>",
            "https://example.test/<result>",
            "https://example.test/result\n<!here>",
            "javascript:https://example.test",
        ] {
            let mut snapshot = view(1, SlackStatus::Completed);
            snapshot.links = vec![("Result".into(), unsafe_url.into())];
            let rendered = render_snapshot(&snapshot);
            assert!(!rendered.contains("<https://"), "{rendered}");
            assert!(!rendered.contains("<!channel>"), "{rendered}");
            assert!(!rendered.contains("<!here>"), "{rendered}");
        }
    }

    #[test]
    fn progress_is_clamped() {
        let mut snapshot = view(1, SlackStatus::Running);
        snapshot
            .progress
            .as_mut()
            .unwrap()
            .current = 200;
        let rendered = render_snapshot(&snapshot);
        assert!(rendered.contains("100/100"));
        assert!(rendered.contains("████████████████████"));
    }

    #[tokio::test]
    async fn same_or_stale_snapshot_does_not_update() {
        let client = Arc::new(FakeSlackClient::default());
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            Some("2.3".into()),
            None,
            Duration::ZERO,
        );
        publisher
            .publish(view(2, SlackStatus::Running), PublishUrgency::Immediate)
            .await
            .unwrap();
        publisher
            .publish(view(1, SlackStatus::Queued), PublishUrgency::Immediate)
            .await
            .unwrap();
        publisher
            .publish(view(2, SlackStatus::Running), PublishUrgency::Immediate)
            .await
            .unwrap();
        assert_eq!(client.updates().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn progress_storm_coalesces_to_the_latest_snapshot() {
        let client = Arc::new(FakeSlackClient::default());
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            Some("2.3".into()),
            None,
            Duration::from_secs(30),
        );
        publisher
            .publish(view(1, SlackStatus::Running), PublishUrgency::Immediate)
            .await
            .unwrap();
        for version in 2..20 {
            let mut snapshot = view(version, SlackStatus::Running);
            snapshot
                .progress
                .as_mut()
                .unwrap()
                .current = version;
            publisher
                .publish(snapshot, PublishUrgency::Debounced)
                .await
                .unwrap();
        }
        assert_eq!(client.updates().len(), 1);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        let updates = client.updates();
        assert_eq!(updates.len(), 2);
        assert!(
            updates[1]
                .text
                .contains("19/100"),
            "{}",
            updates[1].text
        );
        assert_eq!(updates[1].snapshot_version, 19);
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_snapshot_fences_a_deferred_progress_render() {
        let client = Arc::new(FakeSlackClient::default());
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            Some("2.3".into()),
            None,
            Duration::from_secs(30),
        );
        publisher
            .publish(view(1, SlackStatus::Running), PublishUrgency::Immediate)
            .await
            .unwrap();
        let mut progress = view(2, SlackStatus::Running);
        progress
            .progress
            .as_mut()
            .unwrap()
            .current = 50;
        publisher
            .publish(progress, PublishUrgency::Debounced)
            .await
            .unwrap();
        publisher
            .publish(view(3, SlackStatus::Completed), PublishUrgency::Immediate)
            .await
            .unwrap();
        assert_eq!(client.updates().len(), 2);
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert_eq!(client.updates().len(), 2, "stale deferred render is fenced");
    }

    #[tokio::test]
    async fn search_reconciliation_adopts_a_newer_snapshot_without_overwriting_it() {
        let client = Arc::new(FakeSlackClient::default());
        let newer = render_snapshot(&view(10, SlackStatus::Failed));
        let ts = client.seed_message(target(), identity(), 10, &newer);
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            None,
            None,
            Duration::ZERO,
        );

        publisher
            .publish(view(1, SlackStatus::Queued), PublishUrgency::Immediate)
            .await
            .unwrap();

        assert!(client.updates().is_empty());
        assert_eq!(
            publisher
                .message_ts()
                .await
                .as_deref(),
            Some(ts.as_str())
        );
        assert_eq!(client.posts()[0].text, newer);
    }

    #[tokio::test]
    async fn equal_version_with_different_text_fails_closed() {
        let client = Arc::new(FakeSlackClient::default());
        client.seed_message(target(), identity(), 1, "conflicting render");
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            None,
            None,
            Duration::ZERO,
        );

        let error = publisher
            .publish(view(1, SlackStatus::Queued), PublishUrgency::Immediate)
            .await
            .unwrap_err();
        assert!(matches!(error, SlackError::Reconciliation(_)));
        assert!(client.updates().is_empty());
    }

    #[tokio::test]
    async fn persisted_timestamp_restart_advances_beyond_remote_version() {
        let client = Arc::new(FakeSlackClient::default());
        let ts = client.seed_message(
            target(),
            identity(),
            10,
            render_snapshot(&view(10, SlackStatus::Running)),
        );
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            Some(ts),
            None,
            Duration::ZERO,
        );

        publisher
            .publish(view(2, SlackStatus::Completed), PublishUrgency::Immediate)
            .await
            .unwrap();

        let updates = client.updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].snapshot_version, 11);
        assert!(
            updates[0]
                .text
                .contains("Completed")
        );
    }

    #[tokio::test]
    async fn lost_post_response_is_reconciled_without_duplicate() {
        let client = Arc::new(FakeSlackClient::default());
        client.lose_next_post_response();
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            None,
            None,
            Duration::ZERO,
        );
        assert!(
            publisher
                .publish(view(1, SlackStatus::Queued), PublishUrgency::Immediate)
                .await
                .is_err()
        );
        drop(publisher);
        let restarted = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            None,
            None,
            Duration::ZERO,
        );
        restarted
            .publish(view(2, SlackStatus::Running), PublishUrgency::Immediate)
            .await
            .unwrap();
        assert_eq!(client.posts().len(), 1);
        assert_eq!(client.updates().len(), 1);
    }

    #[tokio::test]
    async fn timestamp_persist_failure_is_reconciled_without_duplicate() {
        let client = Arc::new(FakeSlackClient::default());
        let store = Arc::new(RecordingIdentityStore::failing_once());
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            None,
            Some(store.clone()),
            Duration::ZERO,
        );
        assert!(
            publisher
                .publish(view(1, SlackStatus::Queued), PublishUrgency::Immediate)
                .await
                .is_err()
        );
        drop(publisher);
        let restarted = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            None,
            Some(store.clone()),
            Duration::ZERO,
        );
        restarted
            .publish(view(2, SlackStatus::Running), PublishUrgency::Immediate)
            .await
            .unwrap();
        assert_eq!(client.posts().len(), 1);
        assert_eq!(client.updates().len(), 1);
        assert_eq!(
            store
                .attempts
                .load(Ordering::SeqCst),
            2
        );
    }

    #[tokio::test]
    async fn failed_update_retries_on_the_next_snapshot() {
        let client = Arc::new(FakeSlackClient::default());
        client.fail_next_update();
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            Some("2.3".into()),
            None,
            Duration::ZERO,
        );
        assert!(
            publisher
                .publish(view(1, SlackStatus::Running), PublishUrgency::Immediate)
                .await
                .is_err()
        );
        publisher
            .publish(view(1, SlackStatus::Running), PublishUrgency::Immediate)
            .await
            .unwrap();
        assert_eq!(client.updates().len(), 2);
    }

    #[tokio::test]
    async fn lookup_failure_and_multiple_matches_fail_closed() {
        let client = Arc::new(FakeSlackClient::default());
        client.fail_next_lookup();
        let publisher = SlackSnapshotPublisher::with_update_interval(
            client.clone(),
            target(),
            identity(),
            None,
            None,
            Duration::ZERO,
        );
        assert!(
            publisher
                .publish(view(1, SlackStatus::Queued), PublishUrgency::Immediate)
                .await
                .is_err()
        );
        assert!(client.posts().is_empty());

        client.seed_message(target(), identity(), 1, "one");
        client.seed_message(target(), identity(), 1, "two");
        assert!(
            publisher
                .publish(view(2, SlackStatus::Running), PublishUrgency::Immediate)
                .await
                .is_err()
        );
        assert_eq!(client.posts().len(), 2, "only explicitly seeded messages exist");
    }
}
