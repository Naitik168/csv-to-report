//! Job execution.
//!
//! [`execute_job`] is the entry point for one delivery: claim → run with heartbeat and timeout
//! → record the outcome. It never panics on job errors and always leaves the job in a
//! well-defined state, so the message can always be acked afterwards.

mod consumer;
mod import;
mod report;
mod sweeper;

use std::sync::Arc;

use rand::Rng;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn, Instrument};
use uuid::Uuid;

pub use consumer::run_consumers;
pub use sweeper::{run_sweeper, sweep_once};

use crate::config::WorkerConfig;
use crate::jobs::{self, ClaimedJob, JobKind};
use crate::storage::BlobStore;

/// Why an attempt did not succeed. The distinction drives the retry policy.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// Retrying cannot help (bad file, bad request). Fail immediately.
    #[error("{0}")]
    Permanent(String),
    /// Might succeed later (DB/broker hiccup, timeout, injected chaos). Retry with backoff.
    #[error("{0:#}")]
    Transient(anyhow::Error),
    /// Another worker owns the job now; we must not touch it.
    #[error("lease lost")]
    LeaseLost,
}

impl From<sqlx::Error> for JobError {
    fn from(e: sqlx::Error) -> Self {
        // SQLSTATE class 22 (data exception) and 23 (integrity violation) are deterministic:
        // the same data fails the same way every time, so retrying only wastes attempts.
        if let sqlx::Error::Database(db) = &e {
            if let Some(code) = db.code() {
                if code.starts_with("22") || code.starts_with("23") {
                    return JobError::Permanent(format!("data rejected by the database (SQLSTATE {code}): {db}"));
                }
            }
        }
        JobError::Transient(e.into())
    }
}

/// What happened to a delivery. Returned for logging and tests.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// Not claimable: already running elsewhere, finished, not yet due, or unknown id.
    Skipped,
    Succeeded,
    RetryScheduled,
    Failed,
    LeaseLost,
}

#[derive(Clone)]
pub struct WorkerContext {
    pub pool: PgPool,
    pub config: Arc<WorkerConfig>,
    /// Object storage holding uploaded files and generated report files.
    pub store: Arc<dyn BlobStore>,
}

impl WorkerContext {
    pub fn new(pool: PgPool, config: WorkerConfig, store: Arc<dyn BlobStore>) -> Self {
        Self { pool, config: Arc::new(config), store }
    }
}

/// Claim and execute one job.
pub async fn execute_job(ctx: &WorkerContext, job_id: Uuid) -> Outcome {
    let cfg = &ctx.config;
    let job = match jobs::claim(&ctx.pool, job_id, &cfg.worker_id, cfg.lease).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            info!(%job_id, worker_id = %cfg.worker_id, "job not claimable (already taken, finished or not due); skipping");
            return Outcome::Skipped;
        }
        Err(e) => {
            // Could not even claim (DB down). The job stays queued; the dispatcher re-publishes it.
            warn!(%job_id, error = %e, "failed to claim job; it will be re-dispatched");
            return Outcome::Skipped;
        }
    };

    let span = tracing::info_span!(
        "job",
        job_id = %job.id,
        kind = job.kind.as_str(),
        user_id = %job.user_id,
        attempt = job.attempt,
        max_attempts = job.max_attempts,
        worker_id = %job.worker_id,
    );
    run_claimed(ctx, job).instrument(span).await
}

async fn run_claimed(ctx: &WorkerContext, job: ClaimedJob) -> Outcome {
    let cfg = &ctx.config;
    let started = std::time::Instant::now();
    info!("job started");

    // Heartbeat: keep extending the lease; cancel the work if the lease is lost.
    let lease_lost = CancellationToken::new();
    let heartbeat = tokio::spawn(heartbeat_loop(ctx.clone(), job.clone(), lease_lost.clone()));

    let work = async {
        maybe_inject_chaos(cfg.chaos_failure_rate)?;
        match job.kind {
            JobKind::Import => import::process(ctx, &job).await,
            JobKind::Report => report::process(ctx, &job).await,
        }
    };
    // `biased`: if the work finished (and committed) we must report that, even if a heartbeat
    // racing with the commit observed the job as no longer 'running' and fired lease_lost.
    let result = tokio::select! {
        biased;
        r = tokio::time::timeout(cfg.job_timeout, work) => match r {
            Ok(r) => r,
            Err(_) => Err(JobError::Transient(anyhow::anyhow!(
                "attempt timed out after {}s", cfg.job_timeout.as_secs()
            ))),
        },
        _ = lease_lost.cancelled() => Err(JobError::LeaseLost),
    };
    heartbeat.abort();
    let duration_ms = started.elapsed().as_millis() as u64;

    match result {
        Ok(()) => {
            info!(duration_ms, "job succeeded");
            Outcome::Succeeded
        }
        Err(JobError::LeaseLost) => {
            warn!(duration_ms, "lease lost; another worker owns this job now, discarding our work");
            Outcome::LeaseLost
        }
        Err(JobError::Permanent(msg)) => {
            error!(duration_ms, error_kind = "permanent", error = %msg, "job failed permanently; not retrying");
            record_failure(ctx, &job, &msg).await
        }
        Err(JobError::Transient(e)) if job.attempts_exhausted() => {
            let msg = format!("{e:#}");
            error!(duration_ms, error_kind = "transient", error = %msg, "job failed and retries are exhausted");
            record_failure(ctx, &job, &format!("{msg} (gave up after {} attempts)", job.attempt)).await
        }
        Err(JobError::Transient(e)) => {
            let msg = format!("{e:#}");
            let delay = jobs::backoff_delay(job.attempt, cfg.backoff_base, cfg.backoff_max, rand::thread_rng().gen());
            match jobs::schedule_retry(&ctx.pool, &job, &msg, delay).await {
                Ok(Some(next_attempt_at)) => {
                    warn!(duration_ms, error_kind = "transient", error = %msg, retry_in_ms = delay.as_millis() as u64,
                          %next_attempt_at, "job attempt failed; retry scheduled");
                    Outcome::RetryScheduled
                }
                Ok(None) => {
                    warn!("lease lost before retry could be scheduled");
                    Outcome::LeaseLost
                }
                Err(db) => {
                    // Can't write the outcome. The lease will expire and the reaper requeues the job.
                    error!(error = %msg, db_error = %db, "could not record retry; the lease reaper will recover the job");
                    Outcome::RetryScheduled
                }
            }
        }
    }
}

async fn record_failure(ctx: &WorkerContext, job: &ClaimedJob, msg: &str) -> Outcome {
    match jobs::fail(&ctx.pool, job, msg).await {
        Ok(true) => Outcome::Failed,
        Ok(false) => Outcome::LeaseLost,
        Err(db) => {
            error!(db_error = %db, "could not record failure; the lease reaper will recover the job");
            Outcome::Failed
        }
    }
}

async fn heartbeat_loop(ctx: WorkerContext, job: ClaimedJob, lease_lost: CancellationToken) {
    let mut interval = tokio::time::interval(ctx.config.heartbeat_interval);
    interval.tick().await; // first tick is immediate
    loop {
        interval.tick().await;
        match jobs::heartbeat(&ctx.pool, &job, ctx.config.lease).await {
            Ok(true) => tracing::debug!(job_id = %job.id, "lease extended"),
            Ok(false) => {
                warn!(job_id = %job.id, "heartbeat found the lease was lost");
                lease_lost.cancel();
                return;
            }
            // A transient DB error is not proof we lost the lease; keep trying until it expires.
            Err(e) => warn!(job_id = %job.id, error = %e, "heartbeat failed; will retry"),
        }
    }
}

fn maybe_inject_chaos(rate: f64) -> Result<(), JobError> {
    if rate > 0.0 && rand::thread_rng().gen::<f64>() < rate {
        return Err(JobError::Transient(anyhow::anyhow!(
            "chaos: injected transient failure (CHAOS_FAILURE_RATE={rate})"
        )));
    }
    Ok(())
}
