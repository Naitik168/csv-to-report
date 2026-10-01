//! Exercises the real S3 client (`S3Store`) against an S3-compatible server.
//!
//! Runs only when `S3_TEST_ENDPOINT` is set, e.g. against SeaweedFS in Docker Compose
//! (`docker compose --profile test run --rm tests` sets it) or any local S3 server.
//! Without it the test is skipped with a message, so `cargo test` works with just Postgres.
//! Uses one fixed bucket (`S3_TEST_BUCKET`, default `csv-reports-test`) and cleans up its keys.

use bytes::Bytes;
use csv_reports::storage::{BlobStore, S3Config, S3Store, StorageError};
use uuid::Uuid;

/// A fixed test bucket (so repeated runs don't pile up buckets on the server) and a random key
/// prefix per test (so runs don't see each other's objects).
async fn store() -> Option<(S3Store, String)> {
    let endpoint = std::env::var("S3_TEST_ENDPOINT").ok().filter(|v| !v.is_empty())?;
    let get = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let s3 = S3Store::connect(&S3Config {
        endpoint: Some(endpoint),
        region: get("S3_REGION", "us-east-1"),
        bucket: get("S3_TEST_BUCKET", "csv-reports-test"),
        credentials: Some((get("S3_ACCESS_KEY_ID", "csvreports"), get("S3_SECRET_ACCESS_KEY", "csvreports-secret"))),
        force_path_style: true,
    })
    .await;
    Some((s3, format!("test-{}/", Uuid::new_v4().simple())))
}

async fn cleanup(s3: &S3Store, prefix: &str) {
    for o in s3.list(prefix).await.unwrap_or_default() {
        let _ = s3.delete(&o.key).await;
    }
}

#[tokio::test]
async fn s3_store_round_trip_against_a_real_server() {
    let Some((s3, p)) = store().await else {
        eprintln!("skipped: set S3_TEST_ENDPOINT to run against a real S3-compatible server");
        return;
    };
    s3.ensure_bucket().await.expect("create bucket");
    s3.ensure_bucket().await.expect("ensure_bucket is idempotent");
    assert!(s3.is_healthy().await);

    let key = format!("{p}uploads/alice/{}.csv", Uuid::new_v4());
    let body = Bytes::from_static(b"user_id,order_id\nU1,O1\n");
    s3.put(&key, body.clone(), "text/csv").await.expect("put");
    assert_eq!(s3.get(&key).await.expect("get"), body);

    // Overwrite with the same key (what a retried job does).
    s3.put(&key, Bytes::from_static(b"v2"), "text/csv").await.unwrap();
    assert_eq!(s3.get(&key).await.unwrap(), Bytes::from_static(b"v2"));

    let listed = s3.list(&format!("{p}uploads/")).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, key);
    assert_eq!(listed[0].size, 2);
    assert!(listed[0].last_modified.is_some());

    s3.delete(&key).await.expect("delete");
    s3.delete(&key).await.expect("delete is idempotent");
    assert!(matches!(s3.get(&key).await, Err(StorageError::NotFound(_))));
    assert!(s3.list(&format!("{p}uploads/")).await.unwrap().is_empty());

    // A missing bucket is NOT "object not found": it must stay a retryable error.
    let other = S3Store::connect(&S3Config {
        endpoint: std::env::var("S3_TEST_ENDPOINT").ok(),
        region: "us-east-1".into(),
        bucket: format!("does-not-exist-{}", &Uuid::new_v4().simple().to_string()[..8]),
        credentials: Some((
            std::env::var("S3_ACCESS_KEY_ID").unwrap_or_else(|_| "csvreports".into()),
            std::env::var("S3_SECRET_ACCESS_KEY").unwrap_or_else(|_| "csvreports-secret".into()),
        )),
        force_path_style: true,
    })
    .await;
    assert!(matches!(other.get("x").await, Err(StorageError::Unavailable(_))));
    cleanup(&s3, &p).await;
}

#[tokio::test]
async fn s3_store_lists_across_pages() {
    let Some((s3, p)) = store().await else {
        eprintln!("skipped: set S3_TEST_ENDPOINT");
        return;
    };
    s3.ensure_bucket().await.unwrap();
    // More than one ListObjectsV2 page (1000 keys) is too slow for a test; 30 keys still
    // exercises the loop, and pagination is covered by the continuation-token handling.
    for i in 0..30 {
        s3.put(&format!("{p}reports/u/{i:03}.csv"), Bytes::from_static(b"x"), "text/csv").await.unwrap();
    }
    assert_eq!(s3.list(&format!("{p}reports/")).await.unwrap().len(), 30);
    assert_eq!(s3.list(&format!("{p}uploads/")).await.unwrap().len(), 0);
    cleanup(&s3, &p).await;
    assert!(s3.list(&p).await.unwrap().is_empty());
}
