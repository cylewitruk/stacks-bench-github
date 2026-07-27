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

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sbgh_core::db::{JobStore, PolicyStore, RepoStore};
use sbgh_core::models::{
    BuildTarget, GitRefKind, GithubRepo, JobAxes, JobIntent, JobSource, NewJob, QueuedEventDetail,
    TaskKind,
};
use sbgh_driver::{CacheControl, CacheEnvironment};

use crate::pin_resolver::{PinnedTarget, RefKind, RemoteRef, commits_of, pinned_targets};
use sbgh_libvirt::{Shell, command_spec, current_cache_environment};

/// Bound a whole recompute so a slow / hung `ls-remote` can't stall its caller
/// (boot, or a job task's completion).
const RECOMPUTE_TIMEOUT_SECS: u64 = 30;

/// Per-repo `git ls-remote` timeout. The child is **killed** on timeout (via
/// [`Shell::run_bounded`]) so a hung remote can't leak a git process — distinct
/// from the overall [`RECOMPUTE_TIMEOUT_SECS`], which bounds the slot.
const LS_REMOTE_TIMEOUT_SECS: u64 = 20;

/// Warm-retry cooldown (item 0031): once a warm build for a `(repo, commit,
/// build_target)` fails/cancels and leaves the cache cold, don't re-enqueue it
/// for this long. Without it, the after-each-job recompute would re-fire the
/// same failing build forever (a bad pin → an endless one-at-a-time loop). A
/// transient failure recovers on the first recompute past the window; a fuller
/// retry policy (backoff / max attempts) is future work.
const WARM_RETRY_COOLDOWN_HOURS: i64 = 6;

/// Repository identity capability needed by pin recomputation.
#[async_trait]
pub trait RepoIdentityLookup: Send + Sync + 'static {
    async fn lookup_repo(&self, github_repo_id: i64) -> sbgh_core::Result<Option<GithubRepo>>;
}

#[async_trait]
impl<T> RepoIdentityLookup for T
where
    T: RepoStore + ?Sized,
{
    async fn lookup_repo(&self, github_repo_id: i64) -> sbgh_core::Result<Option<GithubRepo>> {
        RepoStore::lookup_repo(self, github_repo_id).await
    }
}

/// Owns the deps + the **shared** cache `Arc` for recomputing the pinned set.
/// The runner drives it on startup (before claiming) and after each job
/// execution (any worker return — a published build, a cache hit, or a
/// failure; recompute is idempotent). Sharing the driver's cache `Arc` is
/// load-bearing: publish (driver) and re-pin / evict (here) then coordinate
/// under the one cache mutex.
pub struct PinManager {
    cache: Arc<dyn CacheControl>,
    policy_store: Arc<dyn PolicyStore>,
    repo_store: Arc<dyn RepoIdentityLookup>,
    /// v11 (item 0031): the store warming enqueues build-only jobs into.
    jobs: Arc<dyn JobStore>,
    shell: Arc<dyn Shell>,
    git_binary: PathBuf,
    golden_image: PathBuf,
}

impl PinManager {
    pub fn new(
        cache: Arc<dyn CacheControl>,
        policy_store: Arc<dyn PolicyStore>,
        repo_store: Arc<dyn RepoIdentityLookup>,
        jobs: Arc<dyn JobStore>,
        shell: Arc<dyn Shell>,
        git_binary: PathBuf,
        golden_image: PathBuf,
    ) -> Self {
        Self {
            cache,
            policy_store,
            repo_store,
            jobs,
            shell,
            git_binary,
            golden_image,
        }
    }

    /// Best-effort, timeout-bounded recompute against the current refs. Derives
    /// the daemon's current build environment from the golden image, runs
    /// [`recompute_pins`] (protect), then [`warm_missing`] (warm) over the same
    /// resolved target set. Logs + swallows everything (never propagates): a
    /// slow `ls-remote` is bounded so it can't stall the caller.
    pub async fn recompute(&self, now: DateTime<Utc>) {
        let Some(env) = current_cache_environment(&self.golden_image) else {
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
        let targets =
            match tokio::time::timeout(Duration::from_secs(RECOMPUTE_TIMEOUT_SECS), fut).await {
                Ok(Ok(targets)) => targets,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "pin recompute failed (best-effort)");
                    return;
                }
                Err(_) => {
                    tracing::warn!(
                        secs = RECOMPUTE_TIMEOUT_SECS,
                        "pin recompute timed out (best-effort)"
                    );
                    return;
                }
            };
        // Warm any pinned ref whose binary the protect pass found missing
        // (resolve once → protect → warm). Local DB work, no network — not
        // inside the `ls-remote` timeout.
        warm_missing(self.jobs.as_ref(), self.cache.as_ref(), &targets, &env, now).await;
    }
}

/// Recompute the pinned set against the **current** remote refs and apply it to
/// `cache`: resolve each distinct pinned repo's refs (`ls-remote`), pin the
/// matching entries under `env`, then re-evaluate the budget. Returns the
/// resolved `PinnedTarget`s so the caller can warm them (see
/// [`warm_missing`]). `env` is the daemon's current build environment (see
/// [`driver::current_cache_environment`]) — only same-environment entries get
/// pinned. **All-or-nothing:** if any pinned repo can't be resolved, returns
/// `Ok(Vec::new())` *without* mutating pins (the existing set is preserved, and
/// nothing is warmed). Errors only on a store failure.
pub async fn recompute_pins(
    cache: &dyn CacheControl,
    policy_store: &dyn PolicyStore,
    repo_store: &dyn RepoIdentityLookup,
    shell: &dyn Shell,
    git_binary: &Path,
    env: &CacheEnvironment,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<PinnedTarget>> {
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
            return Ok(Vec::new());
        };
        let url = format!("https://github.com/{}/{}.git", repo.owner, repo.name);
        let Some(repo_refs) = ls_remote_refs(shell, git_binary, repo_id, &url).await else {
            tracing::warn!(
                repo_id,
                %url,
                "pin resolver: could not resolve refs; preserving the existing pin set \
                 (recompute skipped)"
            );
            return Ok(Vec::new());
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
    Ok(targets)
}

/// Map a resolved ref's kind to the `job`-table ref-kind axis.
fn git_ref_kind(kind: RefKind) -> GitRefKind {
    match kind {
        RefKind::Branch => GitRefKind::Branch,
        RefKind::Tag => GitRefKind::Tag,
    }
}

/// Pin warming (item `0031`, v11): for each resolved pinned target whose binary
/// is **missing** from the cache and which isn't already building (or only
/// recently failed — the [`WARM_RETRY_COOLDOWN_HOURS`] cooldown), enqueue a
/// silent build-only job (`source=daemon`, `intent=cache_warm`,
/// `task_kind=build_only`, `build_target=stacks_bench`). Best-effort — a
/// per-job store error is logged and skipped, never failing the recompute.
///
/// Today the only `build_target` is `stacks-bench`. The dedup is per
/// `(repo, commit, build_target)` (the build job's own slice of the queue); the
/// cache itself is commit-keyed (repo-agnostic), so a build at a commit warms
/// every repo's pin at that commit — an in-pass `seen` set avoids enqueuing the
/// same commit twice when two repos pin it.
async fn warm_missing(
    jobs: &dyn JobStore,
    cache: &dyn CacheControl,
    targets: &[PinnedTarget],
    env: &CacheEnvironment,
    now: DateTime<Utc>,
) {
    let build_target = BuildTarget::StacksBench;
    // A warm that failed/cancelled at or after this instant is still in cooldown.
    let failed_since = now - chrono::Duration::hours(WARM_RETRY_COOLDOWN_HOURS);
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut enqueued = 0usize;
    for t in targets {
        // Already warm: the cache holds a binary for this commit + environment.
        if cache.has_entry_for(&t.commit, env) {
            continue;
        }
        // Same commit already enqueued earlier this pass (two repos pinning it).
        if seen.contains(&t.commit) {
            continue;
        }
        // Already building, or recently failed (within the retry cooldown), for
        // this repo — don't enqueue a duplicate or hammer a failing build.
        match jobs
            .recent_build_attempt(t.repo_id, &t.commit, build_target, failed_since)
            .await
        {
            Ok(Some(_)) => continue,
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, repo_id = t.repo_id, commit = %t.commit, "warming: build dedup query failed; skipping");
                continue;
            }
        }

        let new_job = NewJob {
            github_installation_id: t.installation_id,
            github_repo_id: t.repo_id,
            axes: JobAxes {
                source: JobSource::Daemon,
                intent: JobIntent::CacheWarm,
                task_kind: TaskKind::BuildOnly,
                build_target,
            },
            git_ref_kind: git_ref_kind(t.ref_kind),
            git_ref_display: t.ref_name.clone(),
            git_commit_hash: Some(t.commit.clone()),
            git_committed_at: None,
            workload_key: None,
        };
        let detail = serde_json::to_value(QueuedEventDetail::CacheWarm {
            trigger_id: t.trigger_id,
            git_ref: t.ref_name.clone(),
            commit: t.commit.clone(),
            build_target,
        })
        .expect("QueuedEventDetail::CacheWarm serializes");

        // Warming has no Slack surface: a fresh id and no message timestamp.
        match jobs
            .create_unlinked_job(uuid::Uuid::new_v4(), &new_job, &detail, None)
            .await
        {
            Ok(job) => {
                seen.insert(t.commit.clone());
                enqueued += 1;
                tracing::info!(
                    job_id = %job.id,
                    repo_id = t.repo_id,
                    git_ref = %t.ref_name,
                    commit = %t.commit,
                    "warming: enqueued build-only job for an uncached pinned ref"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, repo_id = t.repo_id, commit = %t.commit, "warming: failed to enqueue build-only job");
            }
        }
    }
    if enqueued > 0 {
        tracing::info!(enqueued, "warming: enqueued build-only jobs this recompute");
    }
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
            command_spec(git_binary, &["ls-remote", repo_url]),
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

    use sbgh_core::db::{InstallationStore, JobFailure, JobStore, NewInstallation};
    use sbgh_core::models::{
        BuildTarget, GithubAccountType, JobIntent, JobSource, TaskKind, TriggerKind,
        TriggerMatchSpec,
    };
    use sbgh_postgres::db::{
        PostgresInstallationStore, PostgresJobStore, PostgresPolicyStore, PostgresRepoStore,
        TestDb, setup_pg_db,
    };
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use sbgh_libvirt::shell_test_support::{PreparedReply, RecordingShell};
    use sbgh_worker::BinaryCache;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn test_env() -> CacheEnvironment {
        CacheEnvironment {
            artifact: sbgh_driver::BuildArtifact::StacksBench,
            profile: "release".into(),
            features: String::new(),
            rustflags: String::new(),
            target_triple: "x86_64-unknown-linux-gnu".into(),
            recipe_version: 1,
            image_id: "img-v1".into(),
            protocol_version: "v1".into(),
        }
    }

    struct MissingRepoLookup;

    #[async_trait]
    impl RepoIdentityLookup for MissingRepoLookup {
        async fn lookup_repo(&self, _github_repo_id: i64) -> sbgh_core::Result<Option<GithubRepo>> {
            Ok(None)
        }
    }

    async fn pin_stores()
    -> (TestDb, Arc<PostgresPolicyStore>, Arc<PostgresRepoStore>, Arc<PostgresJobStore>) {
        let (db, pool) = setup_pg_db().await;
        let repo = Arc::new(PostgresRepoStore::new(pool.clone()));
        repo.seed_supported_root(10, "stacks-network", "stacks-core", true)
            .await;
        let installation = PostgresInstallationStore::new(pool.clone());
        installation
            .seed_allowed(1, "stacks-network", GithubAccountType::Organization, true)
            .await;
        installation
            .upsert_installation(&NewInstallation {
                id: 1,
                github_account_id: 1,
                account_login: "stacks-network".into(),
                account_type: GithubAccountType::Organization,
            })
            .await
            .unwrap();
        installation
            .add_or_restore_membership(1, 10)
            .await
            .unwrap();
        (
            db,
            Arc::new(PostgresPolicyStore::new(pool.clone())),
            repo,
            Arc::new(PostgresJobStore::new(pool)),
        )
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

    /// End-to-end orchestration over Postgres stores + a canned `ls-remote`:
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
        let (_db, policy_store, repo_store, _jobs) = pin_stores().await;
        policy_store
            .seed_target(1, 10, true)
            .await;
        let id = policy_store
            .seed_trigger(
                1,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        policy_store
            .set_trigger_pinned(id, true, None)
            .await;

        // `ls-remote` returns develop → c-dev (+ a non-matching branch).
        let shell = RecordingShell::new();
        shell.reply(PreparedReply::with_stdout(
            "c-dev\trefs/heads/develop\nc-other\trefs/heads/main\n",
        ));

        recompute_pins(
            &cache,
            policy_store.as_ref(),
            repo_store.as_ref(),
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
    async fn run_failed_recompute(
        reply: PreparedReply,
        repo_lookup_override: Option<Arc<dyn RepoIdentityLookup>>,
    ) -> (bool, usize, usize) {
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
                .pinned
        );

        let (_db, policy_store, repo_store, _jobs) = pin_stores().await;
        policy_store
            .seed_target(1, 10, true)
            .await;
        let id = policy_store
            .seed_trigger(
                1,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        policy_store
            .set_trigger_pinned(id, true, None)
            .await;

        let shell = RecordingShell::new();
        shell.reply(reply);
        let repo_lookup: Arc<dyn RepoIdentityLookup> = repo_lookup_override.unwrap_or(repo_store);

        let targets = recompute_pins(
            &cache,
            policy_store.as_ref(),
            repo_lookup.as_ref(),
            &shell,
            Path::new("/usr/bin/git"),
            &env,
            at("2026-06-01T00:00:00Z"),
        )
        .await
        .unwrap();

        (
            cache
                .get(&fp, 12)
                .unwrap()
                .pinned,
            shell.calls().len(),
            targets.len(),
        )
    }

    #[tokio::test]
    async fn recompute_preserves_pins_when_lsremote_fails() {
        // A transient `ls-remote` failure must NOT unpin (or evict) the entry.
        let (still_pinned, shell_calls, targets) =
            run_failed_recompute(PreparedReply::fail("ssh: network is unreachable"), None).await;
        assert!(still_pinned, "a failed ls-remote must preserve the existing pin");
        assert_eq!(shell_calls, 1, "the failing ls-remote was attempted");
        assert_eq!(targets, 0, "a failed refresh must not warm anything");
    }

    #[tokio::test]
    async fn recompute_preserves_pins_when_repo_identity_missing() {
        let (still_pinned, shell_calls, targets) =
            run_failed_recompute(PreparedReply::ok(), Some(Arc::new(MissingRepoLookup))).await;
        assert!(still_pinned, "an unresolved repo identity must preserve the existing pin");
        assert_eq!(shell_calls, 0, "missing identity must abort before ls-remote");
        assert_eq!(targets, 0, "a skipped recompute must not warm anything");
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
        let env = current_cache_environment(&golden).expect("env from golden image");

        let cache = Arc::new(BinaryCache::new(tmp.path().join("cache"), 1 << 30));
        let bin = tmp.path().join("bin");
        std::fs::write(&bin, vec![1u8; 64]).unwrap();
        let fp = env.fingerprint("c-dev".into(), "1.95.0".into());
        cache
            .publish(&fp, &bin, 10, false)
            .unwrap();

        let (_db, policy_store, repo_store, jobs) = pin_stores().await;
        policy_store
            .seed_target(1, 10, true)
            .await;
        let id = policy_store
            .seed_trigger(
                1,
                10,
                TriggerKind::BranchPush,
                &TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
                true,
            )
            .await;
        policy_store
            .set_trigger_pinned(id, true, None)
            .await;

        let shell = Arc::new(RecordingShell::new());
        shell.reply(PreparedReply::with_stdout("c-dev\trefs/heads/develop\n"));

        let pm = PinManager::new(
            cache.clone(),
            policy_store,
            repo_store,
            jobs,
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
                .pinned,
            "PinManager recompute derives the env + pins the matching entry"
        );
    }

    // ─── v11 (0031) pin warming ───────────────────────────────────────────

    fn warm_target(commit: &str) -> PinnedTarget {
        PinnedTarget {
            trigger_id: 7,
            installation_id: 1,
            repo_id: 10,
            ref_kind: RefKind::Branch,
            ref_name: "develop".into(),
            commit: commit.into(),
            bench_args: None,
        }
    }

    #[tokio::test]
    async fn warm_missing_enqueues_build_only_for_uncached_pin() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        let env = test_env();
        let (_db, _policy, _repo, jobs) = pin_stores().await;

        warm_missing(
            jobs.as_ref(),
            &cache,
            &[warm_target("c-cold")],
            &env,
            at("2026-06-01T00:00:00Z"),
        )
        .await;

        let all = jobs.all_jobs().await;
        assert_eq!(all.len(), 1, "one build-only job enqueued for the uncached pin");
        let job = &all[0];
        assert_eq!(job.source, JobSource::Daemon);
        assert_eq!(job.intent, JobIntent::CacheWarm);
        assert_eq!(job.task_kind, TaskKind::BuildOnly);
        assert_eq!(job.build_target, BuildTarget::StacksBench);
        assert_eq!(job.git_commit_hash.as_deref(), Some("c-cold"));
        assert_eq!(job.git_ref_display, "develop");
    }

    #[tokio::test]
    async fn warm_missing_skips_cached_pin() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        let env = test_env();
        // The cache already holds a binary for c-warm.
        let bin = tmp.path().join("bin");
        std::fs::write(&bin, vec![1u8; 64]).unwrap();
        let fp = env.fingerprint("c-warm".into(), "1.95.0".into());
        cache
            .publish(&fp, &bin, 10, false)
            .unwrap();

        let (_db, _policy, _repo, jobs) = pin_stores().await;
        warm_missing(
            jobs.as_ref(),
            &cache,
            &[warm_target("c-warm")],
            &env,
            at("2026-06-01T00:00:00Z"),
        )
        .await;

        assert!(
            jobs.all_jobs()
                .await
                .is_empty(),
            "a cached pin must not enqueue a warming build"
        );
    }

    #[tokio::test]
    async fn warm_missing_skips_in_flight_build() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        let env = test_env();
        let (_db, _policy, _repo, jobs) = pin_stores().await;

        // First pass enqueues a build-only job for the uncached pin.
        warm_missing(
            jobs.as_ref(),
            &cache,
            &[warm_target("c-cold")],
            &env,
            at("2026-06-01T00:00:00Z"),
        )
        .await;
        assert_eq!(jobs.all_jobs().await.len(), 1);

        // A second pass while the build is still in-flight (uncached, not yet
        // published) must NOT enqueue a duplicate.
        warm_missing(
            jobs.as_ref(),
            &cache,
            &[warm_target("c-cold")],
            &env,
            at("2026-06-01T00:00:00Z"),
        )
        .await;
        assert_eq!(
            jobs.all_jobs().await.len(),
            1,
            "an in-flight build-only job dedups the next warming pass"
        );
    }

    #[tokio::test]
    async fn warm_missing_respects_failed_cooldown() {
        let tmp = TempDir::new().unwrap();
        let cache = BinaryCache::new(tmp.path().join("cache"), 1 << 30);
        let env = test_env();
        let (_db, _policy, _repo, jobs) = pin_stores().await;

        // Enqueue a warm build, then drive it to a terminal FAILURE (the cache
        // stays cold — the fail-closed build-only contract).
        warm_missing(
            jobs.as_ref(),
            &cache,
            &[warm_target("c-fail")],
            &env,
            at("2026-06-01T00:00:00Z"),
        )
        .await;
        let job_id = jobs.all_jobs().await[0].id;
        let claimed = jobs
            .claim_next_queued(Uuid::new_v4())
            .await
            .unwrap()
            .unwrap();
        let token = claimed
            .claim_token
            .expect("claimed job carries a token");
        jobs.mark_running(job_id, token, None)
            .await
            .unwrap();
        jobs.fail_job(&JobFailure {
            job_id,
            claim_token: token,
            result: None,
            remark: "build failed".into(),
            event_detail: None,
        })
        .await
        .unwrap();
        let after_fail = Utc::now();

        // Within the cooldown: the recent failure blocks a re-enqueue — without
        // this guard the after-each-job recompute would retry it forever.
        warm_missing(jobs.as_ref(), &cache, &[warm_target("c-fail")], &env, after_fail).await;
        assert_eq!(
            jobs.all_jobs().await.len(),
            1,
            "a warm that just failed must not be retried immediately"
        );

        // Past the cooldown: warming retries the failed build once.
        warm_missing(
            jobs.as_ref(),
            &cache,
            &[warm_target("c-fail")],
            &env,
            after_fail + chrono::Duration::hours(WARM_RETRY_COOLDOWN_HOURS + 1),
        )
        .await;
        assert_eq!(
            jobs.all_jobs().await.len(),
            2,
            "past the cooldown, warming retries the failed build"
        );
    }
}
