//! Binary-cache pin manager (item `0025`, v9 Phase 2) — the I/O + orchestration
//! around the pure [`crate::pin_resolver`].
//!
//! Recomputing the pinned set: list the enabled + pinned trigger policies,
//! resolve each distinct target repo's **current** refs via a read-only
//! `git ls-remote` (the host mirror's `refs/heads/*` / `refs/tags/*` are stale
//! — only specific PR SHAs are fetched into it — so we ask the remote directly,
//! without mutating the shared mirror), tag each ref with its `repo_id`, feed
//! them into [`pin_resolver::pinned_commits`], and mark the matching cache
//! entries pinned ([`BinaryCache::set_pinned_by_commit`]) before re-evaluating
//! the size budget.
//!
//! **Pin mutation is all-or-nothing.** If *any* pinned repo can't be resolved
//! (missing identity, or an `ls-remote` failure — transient network, a private
//! repo, …) the recompute is skipped, leaving the existing `meta.pinned` state
//! untouched. A failed *refresh* must never become an unpin + evict of binaries
//! the operator pinned; the set is rebuilt on the next clean recompute. Never
//! fails the daemon (a disabled cache / unresolved env is a logged no-op).
//!
//! Resolution is **unauthenticated** `ls-remote`, so it covers public repos
//! (the Stacks repos this pins). A private pinned repo would fail to resolve
//! and thus preserve (not clear) pins — see [`ls_remote_refs`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sbgh_core::db::{PolicyStore, RepoStore};

use crate::binary_cache::{BinaryCache, CacheEnvironment};
use crate::libvirt::driver;
use crate::libvirt::shell::{Shell, spec};
use crate::pin_resolver::{RefKind, RemoteRef, commits_of, pinned_targets};

/// Bound a whole recompute so a slow / hung `ls-remote` can't stall its caller
/// (boot, or a job task's completion).
const RECOMPUTE_TIMEOUT_SECS: u64 = 30;

/// Per-repo `git ls-remote` timeout. The child is **killed** on timeout (via
/// [`Shell::run_bounded`]) so a hung remote can't leak a git process — distinct
/// from the overall [`RECOMPUTE_TIMEOUT_SECS`], which bounds the slot.
const LS_REMOTE_TIMEOUT_SECS: u64 = 20;

/// Owns the deps + the **shared** cache `Arc` for recomputing the pinned set.
/// The runner drives it on startup (before claiming) and after each job
/// execution (any worker return — a published build, a cache hit, or a
/// failure; recompute is idempotent). Sharing the driver's cache `Arc` is
/// load-bearing: publish (driver) and re-pin / evict (here) then coordinate
/// under the one cache mutex.
pub struct PinManager {
    cache: Arc<BinaryCache>,
    policy_store: Arc<dyn PolicyStore>,
    repo_store: Arc<dyn RepoStore>,
    shell: Arc<dyn Shell>,
    git_binary: PathBuf,
    golden_image: PathBuf,
}

impl PinManager {
    pub fn new(
        cache: Arc<BinaryCache>,
        policy_store: Arc<dyn PolicyStore>,
        repo_store: Arc<dyn RepoStore>,
        shell: Arc<dyn Shell>,
        git_binary: PathBuf,
        golden_image: PathBuf,
    ) -> Self {
        Self {
            cache,
            policy_store,
            repo_store,
            shell,
            git_binary,
            golden_image,
        }
    }

    /// Best-effort, timeout-bounded recompute against the current refs. Derives
    /// the daemon's current build environment from the golden image, then runs
    /// [`recompute_pins`]. Logs + swallows everything (never propagates): a
    /// slow `ls-remote` is bounded so it can't stall the caller.
    pub async fn recompute(&self, now: DateTime<Utc>) {
        let Some(env) = driver::current_cache_environment(&self.golden_image) else {
            tracing::warn!(
                "pin manager: cache environment unresolved (golden image?); skipping recompute"
            );
            return;
        };
        let fut = recompute_pins(
            self.cache.as_ref(),
            self.policy_store.as_ref(),
            self.repo_store.as_ref(),
            self.shell.as_ref(),
            &self.git_binary,
            &env,
            now,
        );
        match tokio::time::timeout(Duration::from_secs(RECOMPUTE_TIMEOUT_SECS), fut).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "pin recompute failed (best-effort)"),
            Err(_) => tracing::warn!(
                secs = RECOMPUTE_TIMEOUT_SECS,
                "pin recompute timed out (best-effort)"
            ),
        }
    }
}

/// Recompute the pinned set against the **current** remote refs and apply it to
/// `cache`: resolve each distinct pinned repo's refs (`ls-remote`), pin the
/// matching entries under `env`, then re-evaluate the budget. `env` is the
/// daemon's current build environment (see
/// [`driver::current_cache_environment`]) — only same-environment entries get
/// pinned. **All-or-nothing:** if any pinned repo can't be resolved, returns
/// `Ok(())` *without* mutating pins (the existing set is preserved). Errors
/// only on a store failure.
pub async fn recompute_pins(
    cache: &BinaryCache,
    policy_store: &dyn PolicyStore,
    repo_store: &dyn RepoStore,
    shell: &dyn Shell,
    git_binary: &Path,
    env: &CacheEnvironment,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let policies = policy_store
        .list_pinned_triggers()
        .await?;
    // Distinct repos only — two installs sharing a repo resolve its refs once.
    let repo_ids: BTreeSet<i64> = policies
        .iter()
        .map(|p| p.github_repo_id)
        .collect();

    // Resolve EVERY pinned repo's current refs up front. Pin mutation is
    // all-or-nothing: if any repo can't be resolved — a missing identity, or an
    // `ls-remote` failure (transient network/DNS, a private repo, …) — abort
    // WITHOUT touching `meta.pinned`. A failed *refresh* must never become an
    // unpin + evict of binaries the operator pinned; the set is rebuilt on the
    // next successful recompute. (An empty pinned set is NOT a failure — it
    // correctly unpins everything.)
    let mut refs: Vec<RemoteRef> = Vec::new();
    for repo_id in repo_ids {
        let Some(repo) = repo_store
            .lookup_repo(repo_id)
            .await?
        else {
            tracing::warn!(
                repo_id,
                "pin resolver: repo identity not found; preserving the existing pin set \
                 (recompute skipped)"
            );
            return Ok(());
        };
        let url = format!("https://github.com/{}/{}.git", repo.owner, repo.name);
        let Some(repo_refs) = ls_remote_refs(shell, git_binary, repo_id, &url).await else {
            tracing::warn!(
                repo_id,
                %url,
                "pin resolver: could not resolve refs; preserving the existing pin set \
                 (recompute skipped)"
            );
            return Ok(());
        };
        refs.extend(repo_refs);
    }

    let targets = pinned_targets(&policies, &refs, now);
    let commits = commits_of(&targets);
    cache.set_pinned_by_commit(&commits, env);
    cache.evict_to_budget();
    tracing::info!(
        pinned_policies = policies.len(),
        pinned_commits = commits.len(),
        "pin resolver: recomputed pinned set"
    );
    Ok(())
}

/// Run `git ls-remote <repo_url>` and parse its refs. `None` signals a
/// resolution **failure** (non-zero exit / exec error) — distinct from a
/// successful empty `Some(vec![])` — so the caller preserves existing pins
/// rather than treat "couldn't reach the remote" as "this repo has no refs".
///
/// Unauthenticated: works for **public** repos (the Stacks repos this pins). A
/// private pinned repo's `ls-remote` fails → `None` → recompute skipped (pins
/// preserved). Installation-authenticated access is a follow-up if private pins
/// ever matter.
async fn ls_remote_refs(
    shell: &dyn Shell,
    git_binary: &Path,
    repo_id: i64,
    repo_url: &str,
) -> Option<Vec<RemoteRef>> {
    let out = match shell
        .run_bounded(
            spec(git_binary, &["ls-remote", repo_url]),
            Duration::from_secs(LS_REMOTE_TIMEOUT_SECS),
        )
        .await
    {
        Ok(o) if o.status.success() => o,
        Ok(o) => {
            tracing::warn!(
                repo_url,
                code = o.status.code(),
                "pin resolver: `git ls-remote` failed"
            );
            return None;
        }
        Err(e) => {
            tracing::warn!(repo_url, error = %e, "pin resolver: `git ls-remote` errored");
            return None;
        }
    };
    Some(parse_ls_remote(repo_id, &String::from_utf8_lossy(&out.stdout)))
}

/// Parse `git ls-remote` stdout into one repo's branch + tag refs. Only
/// `refs/heads/*` and `refs/tags/*` are kept (`HEAD`, `refs/pull/*`, …
/// ignored).
///
/// **Annotated tags** appear twice — the tag-object SHA on `refs/tags/x` plus
/// the peeled commit on `refs/tags/x^{}`; the peeled commit wins, since cache
/// fingerprints are commit SHAs. A lightweight tag has only the one line (its
/// SHA is already a commit).
fn parse_ls_remote(repo_id: i64, stdout: &str) -> Vec<RemoteRef> {
    let mut refs: Vec<RemoteRef> = Vec::new();
    // tag name -> (direct SHA, peeled `^{}` commit)
    let mut tags: BTreeMap<String, (Option<String>, Option<String>)> = BTreeMap::new();

    for line in stdout.lines() {
        let mut cols = line.split_whitespace();
        let (Some(sha), Some(refname)) = (cols.next(), cols.next()) else {
            continue;
        };
        if let Some(name) = refname.strip_prefix("refs/heads/") {
            refs.push(RemoteRef {
                repo_id,
                name: name.to_string(),
                kind: RefKind::Branch,
                commit: sha.to_string(),
            });
        } else if let Some(tag) = refname.strip_prefix("refs/tags/") {
            match tag.strip_suffix("^{}") {
                Some(base) => {
                    tags.entry(base.to_string())
                        .or_default()
                        .1 = Some(sha.to_string())
                }
                None => {
                    tags.entry(tag.to_string())
                        .or_default()
                        .0 = Some(sha.to_string())
                }
            }
        }
    }

    for (name, (direct, peeled)) in tags {
        // Prefer the peeled commit (annotated tag); else the direct SHA
        // (lightweight tag == commit).
        if let Some(commit) = peeled.or(direct) {
            refs.push(RemoteRef {
                repo_id,
                name,
                kind: RefKind::Tag,
                commit,
            });
        }
    }
    refs
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use sbgh_core::db::{InMemoryPolicyStore, InMemoryRepoStore};
    use sbgh_core::models::{TriggerKind, TriggerMatchSpec};
    use tempfile::TempDir;

    use super::*;
    use crate::binary_cache::BinaryCache;
    use crate::libvirt::shell::test_support::{PreparedReply, RecordingShell};

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn test_env() -> CacheEnvironment {
        CacheEnvironment {
            profile: "release".into(),
            features: String::new(),
            rustflags: String::new(),
            target_triple: "x86_64-unknown-linux-gnu".into(),
            recipe_version: 1,
            image_id: "img-v1".into(),
            protocol_version: "v1".into(),
        }
    }

    #[test]
    fn parse_ls_remote_peels_annotated_tags_and_drops_noise() {
        // Branches, a lightweight tag, an annotated tag (object SHA + peeled
        // commit), and noise (`HEAD`, a PR ref) that must be ignored. Real
        // `ls-remote` separates the two columns with a TAB.
        let stdout = [
            "deadbeef\tHEAD",
            "c-develop\trefs/heads/develop",
            "c-feat\trefs/heads/feature/x",
            "c-light\trefs/tags/3.0.0.0.0",
            "tagobj99\trefs/tags/3.1.0.0.0",
            "c-annot\trefs/tags/3.1.0.0.0^{}",
            "c-pr\trefs/pull/42/head",
        ]
        .join("\n");
        let got = parse_ls_remote(7, &stdout);

        // 2 branches + 2 tags; HEAD + the PR ref dropped.
        assert_eq!(got.len(), 4, "kept refs: {got:?}");
        assert!(
            got.iter()
                .all(|r| r.repo_id == 7)
        );
        let find = |name: &str, kind: RefKind| {
            got.iter()
                .find(|r| r.name == name && r.kind == kind)
                .unwrap_or_else(|| panic!("missing {name} ({kind:?})"))
        };
        assert_eq!(find("develop", RefKind::Branch).commit, "c-develop");
        assert_eq!(find("feature/x", RefKind::Branch).commit, "c-feat");
        assert_eq!(
            find("3.0.0.0.0", RefKind::Tag).commit,
            "c-light",
            "a lightweight tag uses its own SHA"
        );
        assert_eq!(
            find("3.1.0.0.0", RefKind::Tag).commit,
            "c-annot",
            "an annotated tag uses the PEELED commit, not the tag-object SHA"
        );
    }

    /// End-to-end orchestration over in-memory stores + a canned `ls-remote`:
    /// a pinned `develop` policy on repo 10 resolves to `stacks-network/
    /// stacks-core`, pulls its current refs, and pins the matching cache entry.
    #[tokio::test]
    async fn recompute_pins_resolves_lsremote_and_pins_the_matching_entry() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        let env = test_env();

        // A published, *unpinned* binary for develop's current commit.
        let bin = tmp.path().join("bin");
        std::fs::write(&bin, vec![1u8; 64]).unwrap();
        let fp = env.fingerprint("c-dev".into(), "1.95.0".into());
        cache
            .publish(&fp, &bin, 10, false)
            .unwrap();

        // A pinned `develop` trigger on (install 1, repo 10), parent enabled.
        let policy_store = InMemoryPolicyStore::new();
        policy_store.seed_target(1, 10, true);
        let id = policy_store.seed_trigger(
            1,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
            true,
        );
        policy_store.set_trigger_pinned(id, true, None);

        // Repo 10 → stacks-network/stacks-core.
        let repo_store = InMemoryRepoStore::new();
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);

        // `ls-remote` returns develop → c-dev (+ a non-matching branch).
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout(
            "c-dev\trefs/heads/develop\nc-other\trefs/heads/main\n",
        ));

        recompute_pins(
            &cache,
            &policy_store,
            &repo_store,
            &shell,
            Path::new("/usr/bin/git"),
            &env,
            at("2026-06-01T00:00:00Z"),
        )
        .await
        .unwrap();

        // develop's binary is now pinned; main's commit was never published so
        // there's nothing to pin for it.
        assert!(
            cache
                .get(&fp, 11)
                .unwrap()
                .meta
                .pinned,
            "develop's binary must be pinned"
        );
        // And the resolver built the correct repo URL for ls-remote.
        let calls = shell.calls();
        assert_eq!(calls.len(), 1, "one ls-remote per distinct repo");
        assert_eq!(
            calls[0].args,
            vec!["ls-remote", "https://github.com/stacks-network/stacks-core.git"],
        );
    }

    /// Seed a real cache with a *pre-pinned* entry + a still-pinned policy on
    /// repo 10, then run a recompute whose ref resolution fails. The pin must
    /// survive — a failed refresh preserves the last known set.
    async fn run_failed_recompute(reply: PreparedReply, seed_repo: bool) -> bool {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        let env = test_env();
        let bin = tmp.path().join("bin");
        std::fs::write(&bin, vec![1u8; 64]).unwrap();
        let fp = env.fingerprint("c-dev".into(), "1.95.0".into());
        cache
            .publish(&fp, &bin, 10, false)
            .unwrap();
        // Pre-pin it (as a prior successful recompute would have).
        cache.set_pinned_by_commit(&HashSet::from(["c-dev".to_string()]), &env);
        assert!(
            cache
                .get(&fp, 11)
                .unwrap()
                .meta
                .pinned
        );

        let policy_store = InMemoryPolicyStore::new();
        policy_store.seed_target(1, 10, true);
        let id = policy_store.seed_trigger(
            1,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
            true,
        );
        policy_store.set_trigger_pinned(id, true, None);

        let repo_store = InMemoryRepoStore::new();
        if seed_repo {
            repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);
        }

        let shell = RecordingShell::new();
        shell.reply(reply);

        recompute_pins(
            &cache,
            &policy_store,
            &repo_store,
            &shell,
            Path::new("/usr/bin/git"),
            &env,
            at("2026-06-01T00:00:00Z"),
        )
        .await
        .unwrap();

        cache
            .get(&fp, 12)
            .unwrap()
            .meta
            .pinned
    }

    #[tokio::test]
    async fn recompute_preserves_pins_when_lsremote_fails() {
        // A transient `ls-remote` failure must NOT unpin (or evict) the entry.
        let still_pinned =
            run_failed_recompute(PreparedReply::fail("ssh: network is unreachable"), true).await;
        assert!(still_pinned, "a failed ls-remote must preserve the existing pin");
    }

    #[tokio::test]
    async fn recompute_preserves_pins_when_repo_identity_missing() {
        // The repo isn't seeded → `lookup_repo` returns None → abort, pins kept.
        // (The shell reply is unused — we abort before any ls-remote.)
        let still_pinned = run_failed_recompute(PreparedReply::ok(), false).await;
        assert!(still_pinned, "an unresolved repo identity must preserve the existing pin");
    }

    /// `PinManager::recompute` derives the env from the golden image (via
    /// `current_cache_environment`), then resolves + pins — the same wrap the
    /// runner uses on startup + after each job.
    #[tokio::test]
    async fn pin_manager_recompute_derives_env_from_golden_and_pins() {
        let tmp = TempDir::new().unwrap();
        // A golden-image file so `current_cache_environment` can fingerprint it.
        let golden = tmp
            .path()
            .join("golden.qcow2");
        std::fs::write(&golden, b"golden").unwrap();
        let env = driver::current_cache_environment(&golden).expect("env from golden image");

        let cache = Arc::new(BinaryCache::new(tmp.path().join("cache"), 1 << 30));
        let bin = tmp.path().join("bin");
        std::fs::write(&bin, vec![1u8; 64]).unwrap();
        let fp = env.fingerprint("c-dev".into(), "1.95.0".into());
        cache
            .publish(&fp, &bin, 10, false)
            .unwrap();

        let policy_store = Arc::new(InMemoryPolicyStore::new());
        policy_store.seed_target(1, 10, true);
        let id = policy_store.seed_trigger(
            1,
            10,
            TriggerKind::BranchPush,
            &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
            true,
        );
        policy_store.set_trigger_pinned(id, true, None);
        let repo_store = Arc::new(InMemoryRepoStore::new());
        repo_store.seed_supported_root(10, "stacks-network", "stacks-core", true);

        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::with_stdout("c-dev\trefs/heads/develop\n"));

        let pm = PinManager::new(
            cache.clone(),
            policy_store,
            repo_store,
            shell,
            PathBuf::from("/usr/bin/git"),
            golden,
        );
        pm.recompute(at("2026-06-01T00:00:00Z"))
            .await;

        assert!(
            cache
                .get(&fp, 11)
                .unwrap()
                .meta
                .pinned,
            "PinManager recompute derives the env + pins the matching entry"
        );
    }
}
