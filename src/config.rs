//! Configuration loaded from environment variables (12-factor style).
//! Every value has a sensible default so `docker compose up` needs no `.env`.

use std::{env, str::FromStr, time::Duration};

fn var<T: FromStr>(name: &str, default: T) -> T {
    match env::var(name) {
        Ok(v) if !v.trim().is_empty() => {
            v.trim().parse().unwrap_or_else(|_| panic!("environment variable {name} has an invalid value: {v:?}"))
        }
        _ => default,
    }
}

fn secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(var(name, default))
}

fn millis(name: &str, default: u64) -> Duration {
    Duration::from_millis(var(name, default))
}

#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub database_url: String,
    pub amqp_url: String,
    pub http_addr: String,
    /// Maximum accepted upload size in bytes.
    pub max_upload_bytes: usize,
    /// How many times a job may be attempted before it is marked failed.
    pub max_attempts: i32,
}

impl ApiConfig {
    pub fn from_env() -> Self {
        Self {
            database_url: var("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/csvreports".to_string()),
            amqp_url: var("AMQP_URL", "amqp://guest:guest@localhost:5672/%2f".to_string()),
            http_addr: var("HTTP_ADDR", "0.0.0.0:8080".to_string()),
            max_upload_bytes: var("MAX_UPLOAD_BYTES", 10 * 1024 * 1024),
            max_attempts: var("JOB_MAX_ATTEMPTS", 5),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub database_url: String,
    pub amqp_url: String,
    /// Unique id of this worker process (used in logs and lease ownership).
    pub worker_id: String,
    /// Max jobs processed concurrently per queue (RabbitMQ prefetch).
    pub concurrency: u16,
    /// How long a claim is valid without a heartbeat.
    pub lease: Duration,
    /// How often a running job renews its lease.
    pub heartbeat_interval: Duration,
    /// Hard timeout for one attempt; exceeding it counts as a transient failure.
    pub job_timeout: Duration,
    /// Retry backoff: base * 2^(attempt-1), capped at `backoff_max`, with jitter.
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    /// How often the sweeper runs (lease reaper + dispatcher).
    pub sweep_interval: Duration,
    /// Re-publish a queued job whose message was sent this long ago but never consumed.
    pub republish_after: Duration,
    /// Maximum number of invalid rows whose details are stored per import (all are counted).
    pub max_stored_row_errors: usize,
    /// Artificial delay inside report generation, to simulate a slow report.
    pub report_simulated_delay: Duration,
    /// Probability (0.0-1.0) of injecting a transient failure into an attempt. For demos only.
    pub chaos_failure_rate: f64,
    /// Time allowed for in-flight jobs to finish on SIGTERM.
    pub shutdown_grace: Duration,
}

impl WorkerConfig {
    pub fn from_env() -> Self {
        let host = env::var("HOSTNAME").unwrap_or_else(|_| "worker".into());
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..6];
        Self {
            database_url: var("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/csvreports".to_string()),
            amqp_url: var("AMQP_URL", "amqp://guest:guest@localhost:5672/%2f".to_string()),
            worker_id: var("WORKER_ID", format!("{host}-{suffix}")),
            concurrency: var("WORKER_CONCURRENCY", 4),
            lease: secs("JOB_LEASE_SECS", 30),
            heartbeat_interval: secs("JOB_HEARTBEAT_SECS", 10),
            job_timeout: secs("JOB_TIMEOUT_SECS", 300),
            backoff_base: millis("RETRY_BACKOFF_BASE_MS", 2_000),
            backoff_max: millis("RETRY_BACKOFF_MAX_MS", 300_000),
            sweep_interval: millis("SWEEP_INTERVAL_MS", 2_000),
            republish_after: secs("REPUBLISH_AFTER_SECS", 60),
            max_stored_row_errors: var("MAX_STORED_ROW_ERRORS", 1_000),
            report_simulated_delay: millis("REPORT_SIMULATED_DELAY_MS", 0),
            chaos_failure_rate: var("CHAOS_FAILURE_RATE", 0.0),
            shutdown_grace: secs("SHUTDOWN_GRACE_SECS", 20),
        }
    }

    /// Configuration suitable for tests: short lease, no delays, no chaos.
    pub fn for_tests(worker_id: &str) -> Self {
        Self {
            database_url: String::new(),
            amqp_url: String::new(),
            worker_id: worker_id.to_string(),
            concurrency: 1,
            lease: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
            job_timeout: Duration::from_secs(30),
            backoff_base: Duration::from_millis(10),
            backoff_max: Duration::from_millis(100),
            sweep_interval: Duration::from_millis(100),
            republish_after: Duration::from_secs(60),
            max_stored_row_errors: 1_000,
            report_simulated_delay: Duration::ZERO,
            chaos_failure_rate: 0.0,
            shutdown_grace: Duration::from_secs(1),
        }
    }
}
