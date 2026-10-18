//! Shared local MinIO fixture; never uses deployment credentials.

use super::S3StoreConfig;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use std::time::Duration;

const ENDPOINT: &str = "http://127.0.0.1:9100";
const BUCKET: &str = "sbgh-test";
const REGION: &str = "us-east-1";
const ACCESS: &str = "minioadmin";
const SECRET: &str = "minioadmin";

/// Disposable MinIO configuration shared by local integration tests.
pub fn s3_config() -> S3StoreConfig {
    S3StoreConfig {
        endpoint: ENDPOINT.into(),
        bucket: BUCKET.into(),
        region: REGION.into(),
        access_key_id: ACCESS.into(),
        secret_access_key: SECRET.into(),
    }
}

/// Bounded HTTP client for local object-store fixtures.
pub fn http() -> reqwest::Client {
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
pub async fn ensure_bucket() {
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
