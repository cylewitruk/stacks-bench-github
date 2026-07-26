//! roadmap-v7 Slice 3: `GitHubApi::compare_commits` (merge-base resolve).
//!
//! Exercises the trait contract via `FakeGitHub`: a staged merge-base is
//! returned with its sha + date; an unstaged combo degrades to `None` (GitHub's
//! "no common ancestor" / `404`); the call is recorded with the cross-fork head
//! identity. The real `OctocrabClient` URL/parse path isn't mockable here —
//! it's validated live (cross-fork + same-repo `gh api
//! .../compare/base...owner:ref`, confirmed during Phase 0).

use chrono::{TimeZone, Utc};
use sbgh_github::GitHubApi;
use sbgh_github::test_support::{FakeCall, FakeGitHub};

#[tokio::test]
async fn compare_commits_returns_staged_cross_fork_merge_base() {
    let gh = FakeGitHub::new();
    let when = Utc
        .with_ymd_and_hms(2026, 5, 21, 15, 56, 14)
        .unwrap();
    // Cross-fork: base repo is the upstream, head lives in a fork.
    gh.set_merge_base(
        "stacks-network/stacks-core",
        "develop",
        "cylewitruk",
        "feat/foo",
        "fa58f05",
        Some(when),
    );

    let mb = gh
        .compare_commits(1, "stacks-network/stacks-core", "develop", "cylewitruk", "feat/foo")
        .await
        .unwrap()
        .expect("staged merge-base should resolve");
    assert_eq!(mb.hash, "fa58f05");
    assert_eq!(mb.committed_at, Some(when));

    // The call recorded the cross-fork head identity (owner + ref).
    assert!(
        gh.calls()
            .iter()
            .any(|c| matches!(
                c,
                FakeCall::CompareCommits { head_owner, head_ref, .. }
                    if head_owner == "cylewitruk" && head_ref == "feat/foo"
            )),
        "compare_commits call should be recorded with the fork head identity",
    );
}

#[tokio::test]
async fn compare_commits_unstaged_degrades_to_none() {
    let gh = FakeGitHub::new();
    // Nothing staged → models "no common ancestor" / 404 → Ok(None), NOT an
    // error (the caller then renders absolute-only metrics).
    let mb = gh
        .compare_commits(1, "o/r", "main", "o", "feature")
        .await
        .unwrap();
    assert!(mb.is_none());
}
