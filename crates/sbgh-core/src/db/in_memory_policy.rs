//! In-memory `PolicyStore` for unit tests. Mirrors the Postgres
//! semantics — single Mutex serialises all access.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;
use crate::db::policy::PolicyStore;
use crate::models::{
    SourceRepoPolicy, TargetRepoPolicy, TriggerKind, TriggerMatchSpec, TriggerPolicy,
};

#[derive(Default)]
pub struct InMemoryPolicyStore {
    state: Mutex<State>,
    next_trigger_id: AtomicI64,
}

#[derive(Default)]
struct State {
    target: HashMap<(i64, i64), TargetRepoPolicy>,
    source: HashMap<(i64, i64), SourceRepoPolicy>,
    triggers: HashMap<i64, TriggerPolicy>,
}

impl InMemoryPolicyStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            next_trigger_id: AtomicI64::new(1),
        }
    }

    /// Test helper: pre-seed a target policy.
    pub fn seed_target(&self, install_id: i64, repo_id: i64, is_enabled: bool) {
        let now = Utc::now();
        self.state
            .lock()
            .unwrap()
            .target
            .insert(
                (install_id, repo_id),
                TargetRepoPolicy {
                    github_installation_id: install_id,
                    github_repo_id: repo_id,
                    is_enabled,
                    note: None,
                    created_at: now,
                    updated_at: now,
                },
            );
    }

    /// Test helper: pre-seed a source policy.
    pub fn seed_source(&self, install_id: i64, repo_id: i64, is_enabled: bool) {
        let now = Utc::now();
        self.state
            .lock()
            .unwrap()
            .source
            .insert(
                (install_id, repo_id),
                SourceRepoPolicy {
                    github_installation_id: install_id,
                    github_repo_id: repo_id,
                    is_enabled,
                    note: None,
                    created_at: now,
                    updated_at: now,
                },
            );
    }

    /// Test helper: pre-seed a trigger policy. Returns the assigned id.
    pub fn seed_trigger(
        &self,
        install_id: i64,
        repo_id: i64,
        kind: TriggerKind,
        spec: &TriggerMatchSpec,
        is_enabled: bool,
    ) -> i64 {
        let id = self
            .next_trigger_id
            .fetch_add(1, Ordering::SeqCst);
        let now = Utc::now();
        self.state
            .lock()
            .unwrap()
            .triggers
            .insert(
                id,
                TriggerPolicy {
                    id,
                    github_installation_id: install_id,
                    github_repo_id: repo_id,
                    trigger_kind: kind,
                    match_spec: serde_json::to_value(spec).expect("spec serialises"),
                    bench_args: None,
                    is_enabled,
                    note: None,
                    pinned: false,
                    pinned_until: None,
                    created_at: now,
                    updated_at: now,
                },
            );
        id
    }

    /// Test helper: set the pin flag (+ optional expiry) on a seeded trigger,
    /// normalizing `pinned_until` to `None` on unpin (mirrors the admin layer).
    /// No-op if `id` is unknown.
    pub fn set_trigger_pinned(&self, id: i64, pinned: bool, pinned_until: Option<DateTime<Utc>>) {
        if let Some(t) = self
            .state
            .lock()
            .unwrap()
            .triggers
            .get_mut(&id)
        {
            t.pinned = pinned;
            t.pinned_until = if pinned { pinned_until } else { None };
            t.updated_at = Utc::now();
        }
    }
}

#[async_trait]
impl PolicyStore for InMemoryPolicyStore {
    async fn lookup_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<TargetRepoPolicy>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .target
            .get(&(install_id, repo_id))
            .cloned())
    }

    async fn lookup_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<SourceRepoPolicy>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .source
            .get(&(install_id, repo_id))
            .cloned())
    }

    async fn list_enabled_triggers(
        &self,
        install_id: i64,
        repo_id: i64,
        kind: TriggerKind,
    ) -> Result<Vec<TriggerPolicy>> {
        // Mirrors the Postgres impl's JOIN: triggers only count as
        // enabled when their parent target_repo_policy row is ALSO
        // enabled (slice 5 follow-up review fix).
        let state = self.state.lock().unwrap();
        let parent_enabled = state
            .target
            .get(&(install_id, repo_id))
            .is_some_and(|p| p.is_enabled);
        if !parent_enabled {
            return Ok(Vec::new());
        }
        let mut rows: Vec<TriggerPolicy> = state
            .triggers
            .values()
            .filter(|t| {
                t.github_installation_id == install_id
                    && t.github_repo_id == repo_id
                    && t.trigger_kind == kind
                    && t.is_enabled
            })
            .cloned()
            .collect();
        rows.sort_by_key(|t| t.id);
        Ok(rows)
    }

    async fn list_pinned_triggers(&self) -> Result<Vec<TriggerPolicy>> {
        // Mirrors the Postgres impl: enabled + pinned triggers whose parent
        // target_repo_policy is also enabled, across every (install, repo).
        let state = self.state.lock().unwrap();
        let mut rows: Vec<TriggerPolicy> = state
            .triggers
            .values()
            .filter(|t| {
                t.is_enabled
                    && t.pinned
                    && state
                        .target
                        .get(&(t.github_installation_id, t.github_repo_id))
                        .is_some_and(|p| p.is_enabled)
            })
            .cloned()
            .collect();
        rows.sort_by_key(|t| t.id);
        Ok(rows)
    }

    async fn upsert_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        note: Option<&str>,
    ) -> Result<TargetRepoPolicy> {
        let now = Utc::now();
        let mut state = self.state.lock().unwrap();
        let row = state
            .target
            .entry((install_id, repo_id))
            .and_modify(|r| {
                r.is_enabled = true;
                if let Some(n) = note {
                    r.note = Some(n.to_string());
                }
                r.updated_at = now;
            })
            .or_insert_with(|| TargetRepoPolicy {
                github_installation_id: install_id,
                github_repo_id: repo_id,
                is_enabled: true,
                note: note.map(str::to_string),
                created_at: now,
                updated_at: now,
            })
            .clone();
        Ok(row)
    }

    async fn disable_target_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<TargetRepoPolicy>> {
        let mut state = self.state.lock().unwrap();
        match state
            .target
            .get_mut(&(install_id, repo_id))
        {
            Some(r) => {
                r.is_enabled = false;
                r.updated_at = Utc::now();
                Ok(Some(r.clone()))
            }
            None => Ok(None),
        }
    }

    async fn upsert_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        note: Option<&str>,
    ) -> Result<SourceRepoPolicy> {
        let now = Utc::now();
        let mut state = self.state.lock().unwrap();
        let row = state
            .source
            .entry((install_id, repo_id))
            .and_modify(|r| {
                r.is_enabled = true;
                if let Some(n) = note {
                    r.note = Some(n.to_string());
                }
                r.updated_at = now;
            })
            .or_insert_with(|| SourceRepoPolicy {
                github_installation_id: install_id,
                github_repo_id: repo_id,
                is_enabled: true,
                note: note.map(str::to_string),
                created_at: now,
                updated_at: now,
            })
            .clone();
        Ok(row)
    }

    async fn disable_source_policy(
        &self,
        install_id: i64,
        repo_id: i64,
    ) -> Result<Option<SourceRepoPolicy>> {
        let mut state = self.state.lock().unwrap();
        match state
            .source
            .get_mut(&(install_id, repo_id))
        {
            Some(r) => {
                r.is_enabled = false;
                r.updated_at = Utc::now();
                Ok(Some(r.clone()))
            }
            None => Ok(None),
        }
    }

    async fn add_trigger_policy(
        &self,
        install_id: i64,
        repo_id: i64,
        kind: TriggerKind,
        match_spec: &TriggerMatchSpec,
        bench_args: Option<&str>,
        note: Option<&str>,
    ) -> Result<TriggerPolicy> {
        let id = self
            .next_trigger_id
            .fetch_add(1, Ordering::SeqCst);
        let now = Utc::now();
        let row = TriggerPolicy {
            id,
            github_installation_id: install_id,
            github_repo_id: repo_id,
            trigger_kind: kind,
            match_spec: serde_json::to_value(match_spec).expect("spec serialises"),
            bench_args: bench_args.map(str::to_string),
            is_enabled: true,
            note: note.map(str::to_string),
            pinned: false,
            pinned_until: None,
            created_at: now,
            updated_at: now,
        };
        self.state
            .lock()
            .unwrap()
            .triggers
            .insert(id, row.clone());
        Ok(row)
    }

    async fn disable_trigger_policy(&self, trigger_id: i64) -> Result<Option<TriggerPolicy>> {
        let mut state = self.state.lock().unwrap();
        match state
            .triggers
            .get_mut(&trigger_id)
        {
            Some(r) => {
                r.is_enabled = false;
                r.updated_at = Utc::now();
                Ok(Some(r.clone()))
            }
            None => Ok(None),
        }
    }

    async fn list_triggers(&self, install_id: i64, repo_id: i64) -> Result<Vec<TriggerPolicy>> {
        let mut rows: Vec<TriggerPolicy> = self
            .state
            .lock()
            .unwrap()
            .triggers
            .values()
            .filter(|t| t.github_installation_id == install_id && t.github_repo_id == repo_id)
            .cloned()
            .collect();
        rows.sort_by_key(|t| t.id);
        Ok(rows)
    }

    async fn disable_target_and_triggers(&self, install_id: i64, repo_id: i64) -> Result<()> {
        // Single Mutex lock serialises with concurrent reads — same
        // guarantee the Postgres impl gets via the begin()/commit()
        // transaction.
        let mut state = self.state.lock().unwrap();
        let now = Utc::now();
        for t in state.triggers.values_mut() {
            if t.github_installation_id == install_id && t.github_repo_id == repo_id && t.is_enabled
            {
                t.is_enabled = false;
                t.updated_at = now;
            }
        }
        if let Some(t) = state
            .target
            .get_mut(&(install_id, repo_id))
            && t.is_enabled
        {
            t.is_enabled = false;
            t.updated_at = now;
        }
        Ok(())
    }
}
