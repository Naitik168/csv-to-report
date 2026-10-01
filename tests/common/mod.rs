//! Shared helpers for integration tests.
//!
//! Integration tests use `#[sqlx::test]`, which creates a fresh, migrated database per test
//! from `DATABASE_URL` (the role needs CREATEDB), so tests are isolated and can run in parallel.
#![allow(dead_code)]

use std::sync::Arc;

use bytes::Bytes;
use csv_reports::{
    config::WorkerConfig,
    jobs::{self, JobKind},
    report::ReportResult,
    storage::{self, BlobStore, MemoryStore},
    worker::WorkerContext,
};
use sqlx::PgPool;
use uuid::Uuid;

pub const SAMPLE: &str = include_str!("../../samples/orders_sample.csv");
pub const MANY_ERRORS: &str = include_str!("../../samples/orders_many_errors.csv");
pub const BAD_HEADER: &str = include_str!("../../samples/orders_bad_header.csv");
pub const EXPECTED_SAMPLE_REPORT: &str = include_str!("../../samples/expected_report_sample.csv");

/// Per-test environment: a fresh database plus an in-memory object store shared by the API
/// and the workers of the test.
#[derive(Clone)]
pub struct TestEnv {
    pub pool: PgPool,
    pub store: Arc<MemoryStore>,
}

impl TestEnv {
    pub fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone(), store: Arc::new(MemoryStore::new()) }
    }

    pub fn ctx(&self, worker_id: &str) -> WorkerContext {
        WorkerContext::new(self.pool.clone(), WorkerConfig::for_tests(worker_id), self.store.clone())
    }

    pub fn ctx_with(&self, worker_id: &str, f: impl FnOnce(&mut WorkerConfig)) -> WorkerContext {
        let mut cfg = WorkerConfig::for_tests(worker_id);
        f(&mut cfg);
        WorkerContext::new(self.pool.clone(), cfg, self.store.clone())
    }

    /// Create a queued import job the way the API does (file in object storage, row in Postgres).
    pub async fn create_import(&self, user: &str, csv: &str, max_attempts: i32) -> Uuid {
        let pool = &self.pool;
        ensure_user(pool, user).await;
        let id = Uuid::new_v4();
        let key = storage::upload_key(user, id);
        self.store.put(&key, Bytes::from(csv.to_string()), "text/csv").await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        assert!(jobs::insert_job(&mut tx, id, JobKind::Import, user, max_attempts, None).await.unwrap());
        sqlx::query(
            "INSERT INTO imports (id, user_id, filename, file_size, file_sha256, file_key) VALUES ($1,$2,'t.csv',$3,'x',$4)",
        )
        .bind(id)
        .bind(user)
        .bind(csv.len() as i64)
        .bind(&key)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        id
    }

    pub async fn create_report(&self, user: &str, import_ids: &[Uuid], max_attempts: i32) -> Uuid {
        let pool = &self.pool;
        ensure_user(pool, user).await;
        let id = Uuid::new_v4();
        let mut tx = pool.begin().await.unwrap();
        assert!(jobs::insert_job(&mut tx, id, JobKind::Report, user, max_attempts, None).await.unwrap());
        sqlx::query("INSERT INTO reports (id, user_id, import_ids) VALUES ($1,$2,$3)")
            .bind(id)
            .bind(user)
            .bind(import_ids)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    }

    /// The full report as written to object storage by the worker.
    pub async fn report_result(&self, report_id: Uuid) -> ReportResult {
        let key: String = sqlx::query_scalar("SELECT json_key FROM reports WHERE id = $1")
            .bind(report_id)
            .fetch_one(&self.pool)
            .await
            .unwrap();
        serde_json::from_slice(&self.store.get(&key).await.unwrap()).unwrap()
    }

    pub async fn report_csv(&self, report_id: Uuid) -> String {
        let key: String = sqlx::query_scalar("SELECT csv_key FROM reports WHERE id = $1")
            .bind(report_id)
            .fetch_one(&self.pool)
            .await
            .unwrap();
        String::from_utf8(self.store.get(&key).await.unwrap().to_vec()).unwrap()
    }
}

pub async fn ensure_user(pool: &PgPool, user: &str) {
    sqlx::query("INSERT INTO users (id) VALUES ($1) ON CONFLICT DO NOTHING").bind(user).execute(pool).await.unwrap();
}

#[derive(Debug, sqlx::FromRow)]
pub struct JobState {
    pub status: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub lease_token: Option<Uuid>,
}

pub async fn job_state(pool: &PgPool, id: Uuid) -> JobState {
    sqlx::query_as("SELECT status, attempts, last_error, lease_token FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub async fn count(pool: &PgPool, sql: &str, id: Uuid) -> i64 {
    sqlx::query_scalar(sql).bind(id).fetch_one(pool).await.unwrap()
}

pub async fn order_count(pool: &PgPool, import_id: Uuid) -> i64 {
    count(pool, "SELECT COUNT(*) FROM orders WHERE import_id = $1", import_id).await
}

/// Simulate time passing for a job: make its lease already expired.
pub async fn expire_lease(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

/// Simulate time passing for a job: make its retry due now.
pub async fn make_due(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE jobs SET available_at = now() WHERE id = $1").bind(id).execute(pool).await.unwrap();
}
