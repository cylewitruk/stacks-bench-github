//! In-memory `RepoStore` for unit tests. Mirrors the Postgres lineage
//! semantics: identity upserts don't clobber lineage columns; the full
//! lineage path inserts ancestors first, then the leaf.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::Utc;

use crate::Result;
use crate::db::repo::{NewRepoIdentity, NewRepoLineage, RepoStore, SupportedRoot};
use crate::models::{GithubRepo, SupportedRepoRoot};

#[derive(Default)]
pub struct InMemoryRepoStore {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    repos: HashMap<i64, GithubRepo>,
    supported: HashMap<i64, SupportedRepoRoot>,
}

impl InMemoryRepoStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a `supported_repo_root` row + its identity row from a test.
    /// Convenience to skip the two-call CLI dance.
    pub fn seed_supported_root(&self, repo_id: i64, owner: &str, name: &str, is_enabled: bool) {
        let now = Utc::now();
        let mut state = self.state.lock().unwrap();
        state
            .repos
            .entry(repo_id)
            .or_insert_with(|| GithubRepo {
                id: repo_id,
                owner: owner.to_string(),
                name: name.to_string(),
                default_branch: None,
                is_fork: Some(false),
                parent_github_repo_id: None,
                fork_root_github_repo_id: None,
                lineage_checked_at: None,
                created_at: now,
                updated_at: now,
            });
        state.supported.insert(
            repo_id,
            SupportedRepoRoot {
                github_repo_id: repo_id,
                is_enabled,
                note: None,
                created_at: now,
                updated_at: now,
            },
        );
    }

    pub fn repo(&self, id: i64) -> Option<GithubRepo> {
        self.state
            .lock()
            .unwrap()
            .repos
            .get(&id)
            .cloned()
    }
}

#[async_trait]
impl RepoStore for InMemoryRepoStore {
    async fn lookup_repo(&self, github_repo_id: i64) -> Result<Option<GithubRepo>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .repos
            .get(&github_repo_id)
            .cloned())
    }

    async fn upsert_repo_identity(&self, identity: &NewRepoIdentity) -> Result<GithubRepo> {
        let mut state = self.state.lock().unwrap();
        let now = Utc::now();
        let row = state
            .repos
            .entry(identity.id)
            .and_modify(|r| {
                r.owner = identity.owner.clone();
                r.name = identity.name.clone();
                if identity
                    .default_branch
                    .is_some()
                {
                    r.default_branch = identity
                        .default_branch
                        .clone();
                }
                r.updated_at = now;
                // is_fork / parent / fork_root deliberately untouched.
            })
            .or_insert_with(|| GithubRepo {
                id: identity.id,
                owner: identity.owner.clone(),
                name: identity.name.clone(),
                default_branch: identity
                    .default_branch
                    .clone(),
                is_fork: None,
                parent_github_repo_id: None,
                fork_root_github_repo_id: None,
                lineage_checked_at: None,
                created_at: now,
                updated_at: now,
            })
            .clone();
        Ok(row)
    }

    async fn upsert_repo_lineage(&self, lineage: &NewRepoLineage) -> Result<GithubRepo> {
        // Ancestors first via the identity path, then the leaf with all
        // lineage columns populated. Done outside a shared lock would
        // race; the in-memory store uses a single Mutex so we acquire,
        // do the topological writes, release.
        let now = Utc::now();
        let mut state = self.state.lock().unwrap();

        let source_id = lineage
            .source
            .as_ref()
            .map(|src| {
                upsert_identity_locked(&mut state, src, now);
                src.id
            });
        let parent_id = lineage
            .parent
            .as_ref()
            .map(|par| {
                if Some(par.id) != source_id {
                    upsert_identity_locked(&mut state, par, now);
                }
                par.id
            });

        let row = state
            .repos
            .entry(lineage.repo.id)
            .and_modify(|r| {
                r.owner = lineage.repo.owner.clone();
                r.name = lineage.repo.name.clone();
                if lineage
                    .repo
                    .default_branch
                    .is_some()
                {
                    r.default_branch = lineage
                        .repo
                        .default_branch
                        .clone();
                }
                r.is_fork = Some(lineage.is_fork);
                r.parent_github_repo_id = parent_id;
                r.fork_root_github_repo_id = source_id;
                r.lineage_checked_at = Some(now);
                r.updated_at = now;
            })
            .or_insert_with(|| GithubRepo {
                id: lineage.repo.id,
                owner: lineage.repo.owner.clone(),
                name: lineage.repo.name.clone(),
                default_branch: lineage
                    .repo
                    .default_branch
                    .clone(),
                is_fork: Some(lineage.is_fork),
                parent_github_repo_id: parent_id,
                fork_root_github_repo_id: source_id,
                lineage_checked_at: Some(now),
                created_at: now,
                updated_at: now,
            })
            .clone();
        Ok(row)
    }

    async fn is_supported_lineage(&self, github_repo_id: i64) -> Result<bool> {
        let state = self.state.lock().unwrap();
        let Some(repo) = state
            .repos
            .get(&github_repo_id)
        else {
            return Ok(false);
        };
        let direct = state
            .supported
            .get(&repo.id)
            .is_some_and(|s| s.is_enabled);
        if direct {
            return Ok(true);
        }
        let via_root = repo
            .fork_root_github_repo_id
            .and_then(|root_id| state.supported.get(&root_id))
            .is_some_and(|s| s.is_enabled);
        Ok(via_root)
    }

    async fn upsert_supported_root(
        &self,
        github_repo_id: i64,
        note: Option<&str>,
    ) -> Result<SupportedRepoRoot> {
        let mut state = self.state.lock().unwrap();
        let now = Utc::now();
        let row = state
            .supported
            .entry(github_repo_id)
            .and_modify(|r| {
                r.is_enabled = true;
                if let Some(n) = note {
                    r.note = Some(n.to_string());
                }
                r.updated_at = now;
            })
            .or_insert_with(|| SupportedRepoRoot {
                github_repo_id,
                is_enabled: true,
                note: note.map(str::to_string),
                created_at: now,
                updated_at: now,
            })
            .clone();
        Ok(row)
    }

    async fn disable_supported_root(
        &self,
        github_repo_id: i64,
    ) -> Result<Option<SupportedRepoRoot>> {
        let mut state = self.state.lock().unwrap();
        match state
            .supported
            .get_mut(&github_repo_id)
        {
            Some(row) => {
                row.is_enabled = false;
                row.updated_at = Utc::now();
                Ok(Some(row.clone()))
            }
            None => Ok(None),
        }
    }

    async fn list_supported_roots(&self) -> Result<Vec<SupportedRoot>> {
        let state = self.state.lock().unwrap();
        let mut rows: Vec<SupportedRoot> = state
            .supported
            .values()
            .filter_map(|s| {
                state
                    .repos
                    .get(&s.github_repo_id)
                    .map(|r| SupportedRoot {
                        github_repo_id: s.github_repo_id,
                        owner: r.owner.clone(),
                        name: r.name.clone(),
                        is_enabled: s.is_enabled,
                        note: s.note.clone(),
                        created_at: s.created_at,
                        updated_at: s.updated_at,
                    })
            })
            .collect();
        rows.sort_by(|a, b| {
            a.owner
                .cmp(&b.owner)
                .then_with(|| a.name.cmp(&b.name))
        });
        Ok(rows)
    }
}

fn upsert_identity_locked(
    state: &mut State,
    identity: &NewRepoIdentity,
    now: chrono::DateTime<Utc>,
) {
    state
        .repos
        .entry(identity.id)
        .and_modify(|r| {
            r.owner = identity.owner.clone();
            r.name = identity.name.clone();
            if identity
                .default_branch
                .is_some()
            {
                r.default_branch = identity
                    .default_branch
                    .clone();
            }
            r.updated_at = now;
        })
        .or_insert_with(|| GithubRepo {
            id: identity.id,
            owner: identity.owner.clone(),
            name: identity.name.clone(),
            default_branch: identity
                .default_branch
                .clone(),
            is_fork: None,
            parent_github_repo_id: None,
            fork_root_github_repo_id: None,
            lineage_checked_at: None,
            created_at: now,
            updated_at: now,
        });
}
