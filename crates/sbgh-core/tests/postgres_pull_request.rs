//! Slice 7 integration tests: `PostgresPullRequestStore` against real Postgres.
//! Covers upsert idempotency (title refreshed, immutable fields not
//! touched), `(target_repo, pr_number)` uniqueness, closed_at lifecycle
//! (set on close, cleared on reopen, idempotent), and the FK enforcement
//! on author + repos.

use sbgh_core::db::{NewPullRequest, PostgresPullRequestStore, PullRequestStore, setup_pg_db};

async fn seed_repo(pool: &sbgh_core::db::Pool, repo_id: i64, owner: &str, name: &str) {
    sqlx::query("INSERT INTO github_repo (id, owner, name) VALUES ($1, $2, $3)")
        .bind(repo_id)
        .bind(owner)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
}

async fn seed_user(pool: &sbgh_core::db::Pool, user_id: i64, login: &str) {
    sqlx::query("INSERT INTO github_user (id, login, user_type) VALUES ($1, $2, 'user')")
        .bind(user_id)
        .bind(login)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn upsert_creates_then_refreshes_title_only() {
    let (_db, pool) = setup_pg_db().await;
    seed_repo(&pool, 10, "o", "r").await;
    seed_repo(&pool, 20, "alice", "r").await;
    seed_user(&pool, 42, "alice").await;
    let store = PostgresPullRequestStore::new(pool.clone());

    let first = store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "initial title".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();
    assert_eq!(first.title, "initial title");
    assert!(first.closed_at.is_none(), "fresh PR is active");

    // Second upsert with renamed title; SHOULD refresh title.
    let refreshed = store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "edited title".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();
    assert_eq!(refreshed.id, first.id, "same row on conflict");
    assert_eq!(refreshed.title, "edited title");
    assert!(refreshed.updated_at >= first.created_at);
}

#[tokio::test]
async fn upsert_does_not_clobber_closed_at() {
    // A late opened/edited/synchronize event MUST NOT reopen a closed
    // PR. The dedicated `set_closed_at` path is the only writer of
    // closed_at.
    let (_db, pool) = setup_pg_db().await;
    seed_repo(&pool, 10, "o", "r").await;
    seed_repo(&pool, 20, "alice", "r").await;
    seed_user(&pool, 42, "alice").await;
    let store = PostgresPullRequestStore::new(pool.clone());

    store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "t".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();
    let closed = store
        .set_closed_at(10, 1, Some(chrono::Utc::now()))
        .await
        .unwrap()
        .unwrap();
    assert!(closed.closed_at.is_some());

    // Edited event arrives — upsert refreshes title but must NOT
    // clear closed_at.
    let refreshed = store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "edited after close".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();
    assert_eq!(refreshed.title, "edited after close");
    assert!(refreshed.closed_at.is_some(), "upsert must NOT silently reopen a closed PR");
}

#[tokio::test]
async fn unique_target_pr_number_is_enforced() {
    let (_db, pool) = setup_pg_db().await;
    seed_repo(&pool, 10, "o", "r").await;
    seed_repo(&pool, 20, "alice", "r").await;
    seed_repo(&pool, 30, "bob", "r").await;
    seed_user(&pool, 42, "alice").await;
    let store = PostgresPullRequestStore::new(pool.clone());

    // Two different PRs (different target repos), both pr_number=1 — OK.
    store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "pr in repo 10".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();
    store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 30,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "pr in repo 30".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();

    // But re-upsert on (target=10, pr_number=1) collapses, not error.
    let collapse = store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "refreshed".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();
    assert_eq!(collapse.title, "refreshed");
}

#[tokio::test]
async fn upsert_rejects_unknown_author() {
    let (_db, pool) = setup_pg_db().await;
    seed_repo(&pool, 10, "o", "r").await;
    seed_repo(&pool, 20, "alice", "r").await;
    let store = PostgresPullRequestStore::new(pool.clone());

    let err = store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "t".into(),
            author_github_user_id: 999,
        })
        .await;
    assert!(err.is_err(), "FK to github_user must reject unknown author");
}

#[tokio::test]
async fn upsert_rejects_unknown_repo() {
    let (_db, pool) = setup_pg_db().await;
    seed_repo(&pool, 10, "o", "r").await;
    seed_user(&pool, 42, "alice").await;
    let store = PostgresPullRequestStore::new(pool.clone());

    let err = store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 999,
            pr_number: 1,
            title: "t".into(),
            author_github_user_id: 42,
        })
        .await;
    assert!(err.is_err(), "FK to github_repo must reject unknown source repo");
}

#[tokio::test]
async fn set_closed_at_toggle_is_idempotent() {
    let (_db, pool) = setup_pg_db().await;
    seed_repo(&pool, 10, "o", "r").await;
    seed_repo(&pool, 20, "alice", "r").await;
    seed_user(&pool, 42, "alice").await;
    let store = PostgresPullRequestStore::new(pool.clone());
    store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 20,
            pr_number: 1,
            title: "t".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();

    // Close.
    let now = chrono::Utc::now();
    let closed = store
        .set_closed_at(10, 1, Some(now))
        .await
        .unwrap()
        .unwrap();
    assert!(closed.closed_at.is_some());
    // Re-close: timestamp updates (matches GH "close, reopen, close again").
    let later = now + chrono::Duration::seconds(60);
    let re_closed = store
        .set_closed_at(10, 1, Some(later))
        .await
        .unwrap()
        .unwrap();
    assert!(re_closed.closed_at.is_some());
    // Reopen: cleared.
    let reopened = store
        .set_closed_at(10, 1, None)
        .await
        .unwrap()
        .unwrap();
    assert!(reopened.closed_at.is_none());
}

#[tokio::test]
async fn set_closed_at_returns_none_for_unknown_pr() {
    let (_db, pool) = setup_pg_db().await;
    let store = PostgresPullRequestStore::new(pool.clone());
    let none = store
        .set_closed_at(999, 999, Some(chrono::Utc::now()))
        .await
        .unwrap();
    assert!(none.is_none(), "no row → Ok(None), not error");
}

#[tokio::test]
async fn internal_pr_with_target_and_source_same_repo_works() {
    // Internal (non-cross-fork) PR: target == source. Schema allows
    // (two separate FKs to the same row), code shouldn't reject it.
    let (_db, pool) = setup_pg_db().await;
    seed_repo(&pool, 10, "o", "r").await;
    seed_user(&pool, 42, "alice").await;
    let store = PostgresPullRequestStore::new(pool.clone());
    let pr = store
        .upsert_pull_request(&NewPullRequest {
            target_github_repo_id: 10,
            source_github_repo_id: 10,
            pr_number: 1,
            title: "internal".into(),
            author_github_user_id: 42,
        })
        .await
        .unwrap();
    assert_eq!(pr.target_github_repo_id, pr.source_github_repo_id);
}
