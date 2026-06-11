//! Live S3 round-trip for the artifact store (`0001`, iteration v4 Phase 2):
//! `S3Store` against a real S3-compatible server (the compose-managed MinIO,
//! started by the `minio` nextest setup script). This is the test the iteration
//! deferred — it proves, end to end, that an upload actually reaches the
//! bucket, that `get`/`exists` resolve from S3 (not just the local mirror), and
//! that a presigned GET fetches the object with **no credentials on the
//! client** — i.e. our SigV4 signing is accepted by a real verifier. Until this
//! passes, `kind = "s3"` must not be enabled on a host (see
//! docs/v3-to-v4-upgrade.md).
//!
//! Requires the MinIO container (`just test` starts it). A bare `cargo test`
//! without it will fail at `ensure_bucket`, the same contract the Postgres
//! suites have with their server.

use std::time::Duration;

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use sbgh_core::config::S3Config;
use uuid::Uuid;

// Daemon is a bin-only crate; pull the module under test in by path. It has no
// `crate::`-internal deps, so it stands alone.
#[path = "../src/artifact_store.rs"]
mod artifact_store;

use artifact_store::{ArtifactStore, S3Store, artifact_key};

const ENDPOINT: &str = "http://127.0.0.1:9100";
const BUCKET: &str = "sbgh-test";
const REGION: &str = "us-east-1";
const ACCESS: &str = "minioadmin";
const SECRET: &str = "minioadmin";

fn s3_config() -> S3Config {
    S3Config {
        endpoint: ENDPOINT.into(),
        bucket: BUCKET.into(),
        region: REGION.into(),
        access_key_id: ACCESS.into(),
        secret_access_key: SECRET.into(),
    }
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

/// Create the test bucket. The `minio` setup script's `--wait` already blocks
/// on the image's healthcheck, so MinIO is ready; the short retry is just
/// belt-and-suspenders against a transient first-request hiccup. Treats an
/// already-existing bucket (HTTP 200 or 409) as success. Panics — and so fails
/// the test loudly — if MinIO never becomes reachable.
async fn ensure_bucket() {
    let creds = Credentials::new(ACCESS, SECRET);
    let bucket =
        Bucket::new(url::Url::parse(ENDPOINT).unwrap(), UrlStyle::Path, BUCKET, REGION).unwrap();
    let http = http();
    let mut last = String::from("(no attempt)");
    for _ in 0..60 {
        let url = bucket
            .create_bucket(&creds)
            .sign(Duration::from_secs(60));
        match http.put(url).send().await {
            // 200 OK (created / us-east-1 re-create) or 409
            // BucketAlreadyOwnedByYou — both mean "bucket is there".
            Ok(r) if r.status().is_success() || r.status().as_u16() == 409 => return,
            Ok(r) => last = format!("HTTP {}", r.status()),
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("MinIO not ready (bucket create failed after retries): {last}");
}

#[tokio::test]
async fn s3_store_round_trips_against_minio() {
    ensure_bucket().await;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = s3_config();
    // A unique job id per run so repeated runs against the reused bucket don't
    // collide / read each other's objects.
    let job = format!("it-{}", Uuid::new_v4());
    let key = artifact_key(&job, "run.json");
    let payload = br#"{"success":true,"n":42}"#;

    let src = tmp.path().join("run.json");
    std::fs::write(&src, payload).unwrap();

    // --- put: local mirror + S3 upload ---
    let store_a = S3Store::new(tmp.path().join("mirror-a"), &cfg, http()).unwrap();
    let size = store_a.put(&key, &src).await;
    assert_eq!(size, Some(payload.len() as u64), "put returns the local byte size");

    // --- prove it reached the BUCKET (not just the local mirror) ---
    // A second store with a FRESH, empty mirror can only satisfy get()/exists()
    // by going to S3.
    let store_b = S3Store::new(tmp.path().join("mirror-b"), &cfg, http()).unwrap();
    assert!(store_b.exists(&key).await, "exists() finds the object via S3 HEAD");

    let got = store_b
        .get(&key)
        .await
        .expect("get() downloads from S3");
    assert_eq!(std::fs::read(&got).unwrap(), payload, "downloaded bytes match the upload");
    // The download landed in store_b's own mirror (not store_a's).
    assert!(
        got.starts_with(tmp.path().join("mirror-b")),
        "downloaded into the requesting store's mirror: {got:?}"
    );

    // --- presigned GET: fetch with NO credentials on the client ---
    // This is the SigV4 acceptance check — a real verifier (MinIO) honors the
    // signature our store mints.
    let url = store_a
        .signed_url(&key, Duration::from_secs(300))
        .expect("local-safe key signs");
    let resp = reqwest::get(url.as_str())
        .await
        .expect("presigned GET request");
    assert!(resp.status().is_success(), "presigned GET succeeds: HTTP {}", resp.status());
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), payload, "presigned GET returns the object bytes");

    // --- absent key: exists=false via S3 HEAD, get=NotFound ---
    let absent = artifact_key(&job, "missing.json");
    assert!(!store_b.exists(&absent).await, "exists() is false for an absent key");
    let err = store_b
        .get(&absent)
        .await
        .expect_err("get() of an absent key errors");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

/// The Slack download-link gate (v5 acceptance): `signed_url_if_fetchable`
/// links an object ONLY when it's actually in the **bucket** — not merely in
/// the local mirror. A failed upload leaves the mirror populated (Decision
/// 0003), so the gate must HEAD S3, never trust `exists` (which is
/// local-or-remote).
#[tokio::test]
async fn signed_url_if_fetchable_requires_bucket_presence() {
    ensure_bucket().await;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = s3_config();
    let job = format!("it-{}", Uuid::new_v4());
    let mirror = tmp.path().join("mirror");
    let store = S3Store::new(mirror.clone(), &cfg, http()).unwrap();
    let ttl = Duration::from_secs(300);

    // Uploaded → in the bucket → a link is offered.
    let present = artifact_key(&job, "uploaded.db");
    let src = tmp.path().join("uploaded.db");
    std::fs::write(&src, b"db-bytes").unwrap();
    store
        .put(&present, &src)
        .await;
    assert!(
        store
            .signed_url_if_fetchable(&present, ttl)
            .await
            .is_some(),
        "an uploaded object is linkable",
    );

    // Local mirror present, S3 object ABSENT (the failed-upload case): write the
    // mirror copy directly, never uploading. `exists` is true via the mirror,
    // but the gate must still decline — no dead Slack link.
    let local_only = artifact_key(&job, "local-only.db");
    let mirror_path = mirror.join(&local_only);
    std::fs::create_dir_all(mirror_path.parent().unwrap()).unwrap();
    std::fs::write(&mirror_path, b"db-bytes").unwrap();
    assert!(
        store
            .exists(&local_only)
            .await,
        "exists() is true via the local mirror"
    );
    assert!(
        store
            .signed_url_if_fetchable(&local_only, ttl)
            .await
            .is_none(),
        "a local-only object must NOT be linked (bucket HEAD declines)",
    );
}

/// `put_local_only` archives to the host mirror but never uploads — the binary
/// path: the in-VM `stacks-bench` build is large (~250-300 MB) and
/// non-portable, so it stays a host-side forensic copy and is kept out of
/// object storage, while the rest of the run (db/run.json/phase log) still
/// ships via `put`.
#[tokio::test]
async fn put_local_only_mirrors_without_uploading() {
    ensure_bucket().await;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = s3_config();
    let job = format!("it-{}", Uuid::new_v4());
    let mirror = tmp.path().join("mirror");
    let store = S3Store::new(mirror.clone(), &cfg, http()).unwrap();

    let key = artifact_key(&job, "stacks-bench");
    let payload = b"\x7fELF-pretend-binary";
    let src = tmp
        .path()
        .join("stacks-bench");
    std::fs::write(&src, payload).unwrap();

    // Local mirror is written, byte size returned (same contract as `put`).
    let size = store
        .put_local_only(&key, &src)
        .await;
    assert_eq!(size, Some(payload.len() as u64), "put_local_only returns the local byte size");

    // The host copy is fetchable via the mirror fast-path.
    let got = store
        .get(&key)
        .await
        .expect("local mirror serves get()");
    assert_eq!(std::fs::read(&got).unwrap(), payload);

    // But it never reached the bucket: a FRESH-mirror store (no local copy) does
    // an authoritative S3 HEAD and finds nothing.
    let fresh = S3Store::new(
        tmp.path()
            .join("mirror-fresh"),
        &cfg,
        http(),
    )
    .unwrap();
    assert!(
        !fresh.exists(&key).await,
        "put_local_only must NOT upload — a fresh mirror finds nothing in the bucket",
    );
    // And the download-link gate declines (not in the bucket).
    assert!(
        store
            .signed_url_if_fetchable(&key, Duration::from_secs(300))
            .await
            .is_none(),
        "a local-only artifact is never offered as a download link",
    );
}
