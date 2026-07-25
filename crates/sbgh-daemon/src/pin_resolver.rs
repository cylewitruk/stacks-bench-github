//! Pin-set resolution for the binary cache (item `0025`, v9 Phase 2).
//!
//! Given the enabled + pinned trigger policies and the *current* remote refs,
//! resolve the set of source commits whose built `stacks-bench` binary must be
//! kept pin-protected from LRU eviction. A policy's `match_spec` selects
//! matching refs — one for an exact branch, **many** for a `BranchPrefix`
//! family (`sb-integration/3.`) or a tag regex — and each match contributes its
//! current commit.
//!
//! Two deliberate properties:
//!
//! - **Same predicate as triggering.** Refs are matched with the *exact*
//!   processor matchers ([`matches_branch_push`] / [`matches_tag_created`]), so
//!   the pinned set is precisely the set of refs the daemon would auto-bench.
//! - **Expiry lives here.** A pin whose `pinned_until <= now` is skipped
//!   without touching the DB, so the operator view can still surface it as
//!   `expired`.
//!
//! Pure + I/O-free: the remote refs are supplied by the caller (the `ls-remote`
//! helper lands in 2b.2), so the matching + expiry logic is unit-testable
//! without git or the network.
#![allow(dead_code)]

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sbgh_core::models::TriggerPolicy;

use crate::webhook_processor::{matches_branch_push, matches_tag_created};

/// A single remote ref as reported by `git ls-remote` for one repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRef {
    /// The `github_repo_id` this ref belongs to. Refs are per-repo (each comes
    /// from one repo's git URL), so the resolver only matches a policy against
    /// refs of the policy's **own** repo — two repos sharing a branch/tag name
    /// (e.g. both have `develop`) must not cross-pin.
    pub repo_id: i64,
    /// Short ref name — `develop`, `sb-integration/3.4.0.0.3`, `3.1.0.0.5`.
    pub name: String,
    pub kind: RefKind,
    /// The **commit** the ref resolves to (40-hex). For an annotated tag this
    /// MUST be the *peeled* commit (`git ls-remote`'s `refs/tags/x^{}` line),
    /// not the tag-object SHA — cache fingerprints are commit SHAs. The
    /// `ls-remote` parser (2b.2) does the peeling.
    pub commit: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefKind {
    Branch,
    Tag,
}

/// A resolved pin: a pinned policy matched to one of its repo's current refs,
/// with enough provenance for **both** eviction-protection (the `commit`, via
/// [`commits_of`]) and **warming** (item `0031-reusable-build-jobs`: enqueue a
/// build job for `repo_id` / `commit` under the policy's `bench_args`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedTarget {
    /// The matched `trigger_policy.id`.
    pub trigger_id: i64,
    pub installation_id: i64,
    pub repo_id: i64,
    pub ref_kind: RefKind,
    pub ref_name: String,
    pub commit: String,
    pub bench_args: Option<String>,
}

/// Resolve the pinned targets: for each pinned policy whose pin hasn't expired
/// at `now`, each of that policy's **own-repo** refs its `match_spec` matches
/// becomes a [`PinnedTarget`]. `policies` is the enabled + pinned set (see
/// `PolicyStore::list_pinned_triggers`), which spans repos — so matching is
/// scoped by `repo_id` to keep one repo's policies from pinning another's
/// like-named refs. Expiry is the only other filter, so an expired pin yields
/// no targets — it neither protects nor warms.
pub fn pinned_targets(
    policies: &[TriggerPolicy],
    refs: &[RemoteRef],
    now: DateTime<Utc>,
) -> Vec<PinnedTarget> {
    let mut targets = Vec::new();
    for policy in policies {
        if is_expired(policy, now) {
            continue;
        }
        for r in refs {
            if r.repo_id == policy.github_repo_id && ref_matches(&policy.match_spec, r) {
                targets.push(PinnedTarget {
                    trigger_id: policy.id,
                    installation_id: policy.github_installation_id,
                    repo_id: policy.github_repo_id,
                    ref_kind: r.kind,
                    ref_name: r.name.clone(),
                    commit: r.commit.clone(),
                    bench_args: policy.bench_args.clone(),
                });
            }
        }
    }
    targets
}

/// The distinct commit set across `targets` — the eviction-protection key for
/// `CacheControl::set_pinned_by_commit`.
pub fn commits_of(targets: &[PinnedTarget]) -> HashSet<String> {
    targets
        .iter()
        .map(|t| t.commit.clone())
        .collect()
}

/// A pin is expired when `pinned_until` is set and not in the future.
fn is_expired(policy: &TriggerPolicy, now: DateTime<Utc>) -> bool {
    policy
        .pinned_until
        .is_some_and(|until| until <= now)
}

/// Whether `r` is selected by `match_spec`, using the *same* predicate the
/// processor fires triggers with. A branch ref is tested against the
/// branch-push matcher (which also handles `BranchPrefix`); a tag ref against
/// the tag-regex matcher. A wrong-kind spec yields `false`, and an unparseable
/// spec / invalid regex is logged + skipped rather than failing the resolve.
fn ref_matches(match_spec: &serde_json::Value, r: &RemoteRef) -> bool {
    match r.kind {
        RefKind::Branch => matches_branch_push(match_spec, &r.name),
        RefKind::Tag => matches_tag_created(match_spec, &r.name).unwrap_or_else(|e| {
            tracing::warn!(error = %e, ref_name = %r.name, "pin resolver: tag match_spec failed; skipping ref");
            false
        }),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use sbgh_core::models::{TriggerKind, TriggerMatchSpec};

    use super::*;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    /// A pinned `TriggerPolicy` on `repo_id` carrying `spec`, with an optional
    /// expiry. The non-matching fields are filler — only `github_repo_id` /
    /// `match_spec` / `pinned_until` drive the resolver.
    fn policy_in(
        repo_id: i64,
        spec: TriggerMatchSpec,
        pinned_until: Option<DateTime<Utc>>,
    ) -> TriggerPolicy {
        let now = Utc
            .timestamp_opt(0, 0)
            .unwrap();
        TriggerPolicy {
            id: 1,
            github_installation_id: 1,
            github_repo_id: repo_id,
            trigger_kind: TriggerKind::BranchPush,
            match_spec: serde_json::to_value(spec).unwrap(),
            bench_args: None,
            is_enabled: true,
            note: None,
            pinned: true,
            pinned_until,
            created_at: now,
            updated_at: now,
        }
    }

    /// Single-repo policy on the default repo (`1`).
    fn policy(spec: TriggerMatchSpec, pinned_until: Option<DateTime<Utc>>) -> TriggerPolicy {
        policy_in(1, spec, pinned_until)
    }

    fn ref_in(repo_id: i64, name: &str, kind: RefKind, commit: &str) -> RemoteRef {
        RemoteRef {
            repo_id,
            name: name.into(),
            kind,
            commit: commit.into(),
        }
    }

    fn branch(name: &str, commit: &str) -> RemoteRef {
        ref_in(1, name, RefKind::Branch, commit)
    }

    fn tag(name: &str, commit: &str) -> RemoteRef {
        ref_in(1, name, RefKind::Tag, commit)
    }

    /// The commit set the matching tests assert on — derived from the targets.
    fn commits(
        policies: &[TriggerPolicy],
        refs: &[RemoteRef],
        now: DateTime<Utc>,
    ) -> HashSet<String> {
        commits_of(&pinned_targets(policies, refs, now))
    }

    #[test]
    fn pinned_targets_carry_provenance_and_drop_expired() {
        let refs = [branch("develop", "c-dev")];
        // An active pin → one target with full provenance (for warming).
        let active = policy(TriggerMatchSpec::BranchPush { branch_name: "develop".into() }, None);
        let targets = pinned_targets(&[active], &refs, at("2026-06-01T00:00:00Z"));
        assert_eq!(targets.len(), 1);
        let t = &targets[0];
        assert_eq!(t.trigger_id, 1);
        assert_eq!(t.installation_id, 1);
        assert_eq!(t.repo_id, 1);
        assert_eq!(t.ref_kind, RefKind::Branch);
        assert_eq!(t.ref_name, "develop");
        assert_eq!(t.commit, "c-dev");

        // An expired pin yields NO targets — it neither protects nor warms.
        let expired = policy(
            TriggerMatchSpec::BranchPush { branch_name: "develop".into() },
            Some(at("2026-05-01T00:00:00Z")),
        );
        assert!(
            pinned_targets(&[expired], &refs, at("2026-06-01T00:00:00Z")).is_empty(),
            "expired pins resolve to no targets",
        );
    }

    #[test]
    fn exact_branch_matches_only_its_ref() {
        let p = policy(TriggerMatchSpec::BranchPush { branch_name: "develop".into() }, None);
        let refs = [branch("develop", "c-dev"), branch("main", "c-main")];
        let got = commits(&[p], &refs, at("2026-06-01T00:00:00Z"));
        assert_eq!(got, HashSet::from(["c-dev".to_string()]));
    }

    #[test]
    fn branch_prefix_matches_the_whole_family() {
        let p = policy(
            TriggerMatchSpec::BranchPrefix {
                prefix: "sb-integration/3.".into(),
            },
            None,
        );
        let refs = [
            branch("sb-integration/3.4.0.0.3", "c-a"),
            branch("sb-integration/3.1.0.0.5", "c-b"),
            branch("sb-integration/2.9.9.9.9", "c-no"),
            branch("develop", "c-dev"),
        ];
        let got = commits(&[p], &refs, at("2026-06-01T00:00:00Z"));
        assert_eq!(got, HashSet::from(["c-a".to_string(), "c-b".to_string()]));
    }

    #[test]
    fn tag_regex_matches_tags_only() {
        let p = policy(
            TriggerMatchSpec::TagCreated {
                tag_pattern: r"^3\.\d+\.\d+\.\d+\.\d+$".into(),
            },
            None,
        );
        let refs = [
            tag("3.1.0.0.5", "c-tag"),
            tag("nightly", "c-no"),
            // A branch whose name *would* match the regex must NOT — tag spec,
            // branch ref.
            branch("3.1.0.0.5", "c-branch"),
        ];
        let got = commits(&[p], &refs, at("2026-06-01T00:00:00Z"));
        assert_eq!(got, HashSet::from(["c-tag".to_string()]));
    }

    #[test]
    fn an_expired_pin_contributes_nothing() {
        let spec = TriggerMatchSpec::BranchPush { branch_name: "develop".into() };
        let refs = [branch("develop", "c-dev")];

        // Expired yesterday → skipped.
        let expired = policy(spec.clone(), Some(at("2026-05-31T00:00:00Z")));
        assert!(commits(&[expired], &refs, at("2026-06-01T00:00:00Z")).is_empty());

        // Expiry in the future → active.
        let active = policy(spec.clone(), Some(at("2026-07-01T00:00:00Z")));
        assert_eq!(
            commits(&[active], &refs, at("2026-06-01T00:00:00Z")),
            HashSet::from(["c-dev".to_string()]),
        );

        // No expiry → active.
        let forever = policy(spec, None);
        assert_eq!(
            commits(&[forever], &refs, at("2026-06-01T00:00:00Z")),
            HashSet::from(["c-dev".to_string()]),
        );
    }

    #[test]
    fn multiple_policies_union_their_commits() {
        let refs =
            [branch("develop", "c-dev"), tag("3.1.0.0.5", "c-tag"), branch("main", "c-main")];
        let policies = [
            policy(TriggerMatchSpec::BranchPush { branch_name: "develop".into() }, None),
            policy(TriggerMatchSpec::TagCreated { tag_pattern: r"^3\.".into() }, None),
        ];
        let got = commits(&policies, &refs, at("2026-06-01T00:00:00Z"));
        assert_eq!(got, HashSet::from(["c-dev".to_string(), "c-tag".to_string()]));
    }

    #[test]
    fn matching_is_scoped_to_the_policys_repo() {
        // Two repos both have a `develop` branch, at different commits. A pinned
        // `develop` policy on repo 1 must pin ONLY repo 1's commit.
        let refs = [
            ref_in(1, "develop", RefKind::Branch, "c-repo1"),
            ref_in(2, "develop", RefKind::Branch, "c-repo2"),
        ];
        let p1 = policy_in(1, TriggerMatchSpec::BranchPush { branch_name: "develop".into() }, None);
        assert_eq!(
            commits(&[p1], &refs, at("2026-06-01T00:00:00Z")),
            HashSet::from(["c-repo1".to_string()]),
            "a policy pins only its own repo's like-named ref",
        );

        // A prefix policy on repo 2 fans out within repo 2 only — repo 1's
        // matching-named branch is untouched.
        let fam = [
            ref_in(1, "sb-integration/3.1.0.0.5", RefKind::Branch, "c1"),
            ref_in(2, "sb-integration/3.2.0.0.0", RefKind::Branch, "c2a"),
            ref_in(2, "sb-integration/3.3.0.0.0", RefKind::Branch, "c2b"),
        ];
        let p2 = policy_in(
            2,
            TriggerMatchSpec::BranchPrefix {
                prefix: "sb-integration/3.".into(),
            },
            None,
        );
        assert_eq!(
            commits(&[p2], &fam, at("2026-06-01T00:00:00Z")),
            HashSet::from(["c2a".to_string(), "c2b".to_string()]),
            "a prefix policy fans out within its own repo only",
        );
    }

    #[test]
    fn an_invalid_tag_regex_is_skipped_not_fatal() {
        let bad = policy(
            TriggerMatchSpec::TagCreated {
                tag_pattern: "(unclosed".into(),
            },
            None,
        );
        let good = policy(TriggerMatchSpec::BranchPush { branch_name: "develop".into() }, None);
        let refs = [tag("3.1.0.0.5", "c-tag"), branch("develop", "c-dev")];
        // The bad regex contributes nothing; the good policy still resolves.
        let got = commits(&[bad, good], &refs, at("2026-06-01T00:00:00Z"));
        assert_eq!(got, HashSet::from(["c-dev".to_string()]));
    }
}
