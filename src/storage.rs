//! Object storage for uploaded CSV files and generated report files.
//!
//! The rest of the code only sees the [`BlobStore`] trait:
//! * [`S3Store`] talks to any S3-compatible service (SeaweedFS in Docker Compose, AWS S3 in
//!   production — only configuration changes).
//! * [`MemoryStore`] keeps objects in memory; used by tests, including simulated outages.
//!
//! Keys are deterministic (derived from the import/report id), so a retried job overwrites the
//! same object instead of leaving orphans behind:
//! * `uploads/{user_id}/{import_id}.csv`
//! * `reports/{user_id}/{report_id}.csv` and `.json`

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::{
    config::{
        retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion, Credentials, Region, RequestChecksumCalculation,
        ResponseChecksumValidation,
    },
    error::DisplayErrorContext,
    primitives::ByteStream,
    types::{BucketLocationConstraint, CreateBucketConfiguration},
    Client,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The object does not exist (never written, or deleted).
    #[error("object not found: {0}")]
    NotFound(String),
    /// Storage unreachable or returned an error. Usually transient.
    #[error("object storage error: {0}")]
    Unavailable(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: i64,
    pub last_modified: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(&self, key: &str, data: Bytes, content_type: &str) -> Result<(), StorageError>;
    async fn get(&self, key: &str) -> Result<Bytes, StorageError>;
    /// Idempotent: deleting a missing object succeeds.
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, StorageError>;
    /// Make sure the bucket exists (creates it if missing).
    async fn ensure_bucket(&self) -> Result<(), StorageError>;
    async fn is_healthy(&self) -> bool;
    /// Human-readable location, for logs.
    fn describe(&self) -> String;
}

pub fn upload_key(user_id: &str, import_id: Uuid) -> String {
    format!("uploads/{user_id}/{import_id}.csv")
}

pub fn report_key(user_id: &str, report_id: Uuid, ext: &str) -> String {
    format!("reports/{user_id}/{report_id}.{ext}")
}

// ---------------------------------------------------------------------------------------------
// S3
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct S3Config {
    /// Custom endpoint for S3-compatible services (e.g. `http://seaweedfs:8333`). None = AWS.
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    /// Static credentials. When None, the standard AWS credential chain is used
    /// (AWS_* env vars, shared profile, ECS/EKS task role, EC2 instance profile).
    pub credentials: Option<(String, String)>,
    /// Path-style addressing (`http://host/bucket/key`), required by most self-hosted services.
    pub force_path_style: bool,
}

impl S3Config {
    pub fn from_env() -> Self {
        let get = |k: &str, d: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| d.into());
        Self {
            endpoint: std::env::var("S3_ENDPOINT").ok().filter(|v| !v.trim().is_empty()),
            region: get("S3_REGION", "us-east-1"),
            bucket: get("S3_BUCKET", "csv-reports"),
            credentials: match (std::env::var("S3_ACCESS_KEY_ID"), std::env::var("S3_SECRET_ACCESS_KEY")) {
                (Ok(k), Ok(s)) if !k.trim().is_empty() && !s.trim().is_empty() => Some((k, s)),
                _ => None,
            },
            force_path_style: matches!(
                get("S3_FORCE_PATH_STYLE", "true").to_ascii_lowercase().as_str(),
                "true" | "1" | "yes"
            ),
        }
    }
}

pub struct S3Store {
    client: Client,
    bucket: String,
    region: String,
    endpoint: String,
}

impl S3Store {
    pub async fn connect(cfg: &S3Config) -> Self {
        let mut builder = match &cfg.credentials {
            Some((key, secret)) => aws_sdk_s3::config::Builder::new()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(Credentials::new(key, secret, None, None, "static-config")),
            // No explicit keys: the default AWS provider chain (env, profile, container/instance role).
            None => aws_sdk_s3::config::Builder::from(&aws_config::defaults(BehaviorVersion::latest()).load().await),
        };
        builder = builder
            .region(Region::new(cfg.region.clone()))
            .force_path_style(cfg.force_path_style)
            // Only send/verify checksums when an operation requires them: newer SDK defaults add
            // CRC32 trailers that some S3-compatible servers don't accept.
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(5))
                    .operation_attempt_timeout(Duration::from_secs(30))
                    .operation_timeout(Duration::from_secs(60))
                    .build(),
            )
            // A few fast SDK-level retries for blips; longer outages are handled by the job retry policy.
            .retry_config(RetryConfig::standard().with_max_attempts(3));
        if let Some(ep) = &cfg.endpoint {
            builder = builder.endpoint_url(ep);
        }
        Self {
            client: Client::from_conf(builder.build()),
            bucket: cfg.bucket.clone(),
            region: cfg.region.clone(),
            endpoint: cfg.endpoint.clone().unwrap_or_else(|| "aws".into()),
        }
    }

    async fn head_bucket(&self) -> Result<(), StorageError> {
        self.client.head_bucket().bucket(&self.bucket).send().await.map(|_| ()).map_err(|e| {
            StorageError::Unavailable(format!("bucket {} is not accessible: {}", self.bucket, DisplayErrorContext(e)))
        })
    }

    fn err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError {
        StorageError::Unavailable(DisplayErrorContext(e).to_string())
    }
}

#[async_trait]
impl BlobStore for S3Store {
    async fn put(&self, key: &str, data: Bytes, content_type: &str) -> Result<(), StorageError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(Self::err)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        let out = match self.client.get_object().bucket(&self.bucket).key(key).send().await {
            Ok(out) => out,
            Err(e) => {
                // Only a missing *key* is NotFound. A missing bucket (also a 404) is a configuration
                // or infrastructure problem and must stay retryable, not fail imports permanently.
                let not_found = e.as_service_error().map(|se| se.is_no_such_key()).unwrap_or(false);
                return Err(if not_found { StorageError::NotFound(key.to_string()) } else { Self::err(e) });
            }
        };
        let body = out.body.collect().await.map_err(Self::err)?;
        Ok(body.into_bytes())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.client.delete_object().bucket(&self.bucket).key(key).send().await.map_err(Self::err)?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, StorageError> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_continuation_token(token.clone())
                .send()
                .await
                .map_err(Self::err)?;
            for obj in page.contents() {
                out.push(ObjectInfo {
                    key: obj.key().unwrap_or_default().to_string(),
                    size: obj.size().unwrap_or(0),
                    last_modified: obj
                        .last_modified()
                        .and_then(|t| DateTime::<Utc>::from_timestamp(t.secs(), t.subsec_nanos())),
                });
            }
            match page.next_continuation_token() {
                Some(t) if page.is_truncated().unwrap_or(false) => token = Some(t.to_string()),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn ensure_bucket(&self) -> Result<(), StorageError> {
        if self.client.head_bucket().bucket(&self.bucket).send().await.is_ok() {
            return Ok(());
        }
        let mut req = self.client.create_bucket().bucket(&self.bucket);
        if self.region != "us-east-1" {
            req = req.create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from(self.region.as_str()))
                    .build(),
            );
        }
        match req.send().await {
            Ok(_) => {
                tracing::info!(bucket = %self.bucket, "created bucket");
                Ok(())
            }
            Err(e) => {
                let already = e
                    .as_service_error()
                    .map(|se| se.is_bucket_already_owned_by_you() || se.is_bucket_already_exists())
                    .unwrap_or(false);
                if already {
                    Ok(())
                } else {
                    Err(Self::err(e))
                }
            }
        }
    }

    async fn is_healthy(&self) -> bool {
        self.client.head_bucket().bucket(&self.bucket).send().await.is_ok()
    }

    fn describe(&self) -> String {
        format!("s3://{} ({})", self.bucket, self.endpoint)
    }
}

/// Connect to object storage and ensure the bucket exists, retrying while the service starts.
///
/// `create_bucket = false` only checks that the bucket exists (used by `storage-admin`, which
/// should never silently create an empty bucket when pointed at the wrong name).
pub async fn connect_s3_with_retry(cfg: &S3Config, create_bucket: bool) -> anyhow::Result<S3Store> {
    let store = S3Store::connect(cfg).await;
    let mut delay = Duration::from_millis(500);
    for attempt in 1..=30u32 {
        let ready = if create_bucket { store.ensure_bucket().await } else { store.head_bucket().await };
        match ready {
            Ok(()) => {
                tracing::info!(attempt, location = %store.describe(), "object storage ready");
                return Ok(store);
            }
            Err(e) if attempt < 30 => {
                tracing::warn!(attempt, error = %e, retry_in_ms = delay.as_millis() as u64, "object storage not reachable yet");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => return Err(e.into()),
        }
    }
    unreachable!()
}

// ---------------------------------------------------------------------------------------------
// In-memory (tests)
// ---------------------------------------------------------------------------------------------

/// In-memory store for tests. `set_available(false)` simulates a storage outage.
#[derive(Default)]
pub struct MemoryStore {
    objects: Mutex<BTreeMap<String, (Bytes, DateTime<Utc>)>>,
    down: AtomicBool,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_available(&self, available: bool) {
        self.down.store(!available, Ordering::SeqCst);
    }

    pub async fn keys(&self) -> Vec<String> {
        self.objects.lock().await.keys().cloned().collect()
    }

    /// Test helper: backdate an object (for age-based purge tests).
    pub async fn set_last_modified(&self, key: &str, at: DateTime<Utc>) {
        if let Some(entry) = self.objects.lock().await.get_mut(key) {
            entry.1 = at;
        }
    }

    fn check(&self) -> Result<(), StorageError> {
        if self.down.load(Ordering::SeqCst) {
            Err(StorageError::Unavailable("simulated outage".into()))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl BlobStore for MemoryStore {
    async fn put(&self, key: &str, data: Bytes, _content_type: &str) -> Result<(), StorageError> {
        self.check()?;
        self.objects.lock().await.insert(key.to_string(), (data, Utc::now()));
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        self.check()?;
        self.objects
            .lock()
            .await
            .get(key)
            .map(|(b, _)| b.clone())
            .ok_or_else(|| StorageError::NotFound(key.to_string()))
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.check()?;
        self.objects.lock().await.remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, StorageError> {
        self.check()?;
        Ok(self
            .objects
            .lock()
            .await
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, (b, t))| ObjectInfo { key: k.clone(), size: b.len() as i64, last_modified: Some(*t) })
            .collect())
    }

    async fn ensure_bucket(&self) -> Result<(), StorageError> {
        self.check()
    }

    async fn is_healthy(&self) -> bool {
        self.check().is_ok()
    }

    fn describe(&self) -> String {
        "memory".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_store_round_trip_and_outage() {
        let s = MemoryStore::new();
        s.put("uploads/a/1.csv", Bytes::from_static(b"x"), "text/csv").await.unwrap();
        assert_eq!(s.get("uploads/a/1.csv").await.unwrap(), Bytes::from_static(b"x"));
        assert_eq!(s.list("uploads/").await.unwrap().len(), 1);
        assert!(matches!(s.get("nope").await, Err(StorageError::NotFound(_))));
        s.set_available(false);
        assert!(matches!(s.get("uploads/a/1.csv").await, Err(StorageError::Unavailable(_))));
        s.set_available(true);
        s.delete("uploads/a/1.csv").await.unwrap();
        s.delete("uploads/a/1.csv").await.unwrap(); // idempotent
        assert!(s.list("").await.unwrap().is_empty());
    }

    #[test]
    fn keys_are_deterministic() {
        let id = Uuid::nil();
        assert_eq!(upload_key("alice", id), "uploads/alice/00000000-0000-0000-0000-000000000000.csv");
        assert_eq!(report_key("alice", id, "json"), "reports/alice/00000000-0000-0000-0000-000000000000.json");
    }
}
