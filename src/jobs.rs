//! The job state machine, stored in Postgres.
//!
//! ```text
//!             claim (lease + fencing token)          success (fenced tx)
//!   queued ───────────────────────────────▶ running ─────────────────────▶ succeeded
//!     ▲  ▲                                   │  │
//!     │  │ transient error, attempts left    │  │ permanent error, or attempts exhausted
//!     │  └───────────── (backoff) ───────────┘  └──────────────────────────▶ failed
//!     │                                      │
//!     └──── lease expired (worker crashed / ─┘   (reaper; fails the job instead
//!           restarted / stalled)                  if attempts are exhausted)
//! ```
//!
//! Safety properties:
//! * **Exclusive claim** – `claim` is a single conditional `UPDATE ... WHERE status='queued'`;
//!   Postgres row locking guarantees at most one worker wins, however many receive the message.
//! * **Fencing** – every state change a worker makes is conditioned on the `lease_token`
//!   it received at claim time. A worker that lost its lease (e.g. paused for longer than the
//!   lease, then woke up) cannot overwrite the work of the worker that took over.
//! * **Crash recovery** – a crashed worker stops heart-beating; the reaper returns the job
//!   to `queued` once the lease expires. Attempts are counted at claim time, so a job that
//!   crashes its worker every time (poison pill) still ends in `failed`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    Import,
    Report,
}

impl JobKind {
    pub fn as_str(self) -> &'static str {
        match self {
            JobKind::Import => "import",
            JobKind::Report => "report",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "import" => Some(JobKind::Import),
            "report" => Some(JobKind::Report),
            _ => None,
        }
    }
}

/// A job this worker currently holds the lease for.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub id: Uuid,
    pub kind: JobKind,
    pub user_id: String,
    /// 1-based number of this attempt.
    pub attempt: i32,
    pub max_attempts: i32,
    pub lease_token: Uuid,
    pub worker_id: String,
}

impl ClaimedJob {
    pub fn attempts_exhausted(&self) -> bool {
        self.attempt >= self.max_attempts
    }
}

/// Insert a new queued job. Returns `None` when a job with the same idempotency key exists.
pub async fn insert_job(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    kind: JobKind,
    user_id: &str,
    max_attempts: i32,
    idempotency_key: Option<&str>,
) -> sqlx::Result<bool> {
    let inserted = sqlx::query(
        "INSERT INTO jobs (id, kind, user_id, status, max_attempts, idempotency_key)
         VALUES ($1, $2, $3, 'queued', $4, $5)
         ON CONFLICT (user_id, kind, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING",
    )
    .bind(id)
    .bind(kind.as_str())
    .bind(user_id)
    .bind(max_attempts)
    .bind(idempotency_key)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(inserted == 1)
}

pub async fn find_by_idempotency_key(
    pool: &PgPool,
    user_id: &str,
    kind: JobKind,
    key: &str,
) -> sqlx::Result<Option<Uuid>> {
    sqlx::query_scalar("SELECT id FROM jobs WHERE user_id = $1 AND kind = $2 AND idempotency_key = $3")
        .bind(user_id)
        .bind(kind.as_str())
        .bind(key)
        .fetch_optional(pool)
        .await
}

/// Atomically claim a queued, due job. Returns `None` if the job does not exist, is already
/// running/finished, or its retry backoff has not elapsed – i.e. someone else owns it or
/// there is nothing to do. This is what makes duplicate message delivery harmless.
pub async fn claim(pool: &PgPool, job_id: Uuid, worker_id: &str, lease: Duration) -> sqlx::Result<Option<ClaimedJob>> {
    let token = Uuid::new_v4();
    let mut tx = pool.begin().await?;
    let row: Option<(String, String, i32, i32)> = sqlx::query_as(
        "UPDATE jobs
            SET status = 'running',
                attempts = attempts + 1,
                lease_token = $2,
                locked_by = $3,
                lease_expires_at = now() + make_interval(secs => $4),
                started_at = COALESCE(started_at, now()),
                updated_at = now()
          WHERE id = $1 AND status = 'queued' AND available_at <= now()
          RETURNING kind, user_id, attempts, max_attempts",
    )
    .bind(job_id)
    .bind(token)
    .bind(worker_id)
    .bind(lease.as_secs_f64())
    .fetch_optional(&mut *tx)
    .await?;

    let Some((kind, user_id, attempt, max_attempts)) = row else {
        tx.rollback().await?;
        return Ok(None);
    };

    sqlx::query("INSERT INTO job_attempts (job_id, attempt, worker_id) VALUES ($1, $2, $3)")
        .bind(job_id)
        .bind(attempt)
        .bind(worker_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    Ok(Some(ClaimedJob {
        id: job_id,
        kind: JobKind::parse(&kind).expect("kind is constrained by a CHECK"),
        user_id,
        attempt,
        max_attempts,
        lease_token: token,
        worker_id: worker_id.to_string(),
    }))
}

/// Extend the lease of a running job. Returns `false` if the lease was lost.
pub async fn heartbeat(pool: &PgPool, job: &ClaimedJob, lease: Duration) -> sqlx::Result<bool> {
    let n = sqlx::query(
        "UPDATE jobs SET lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
          WHERE id = $1 AND lease_token = $2 AND status = 'running'",
    )
    .bind(job.id)
    .bind(job.lease_token)
    .bind(lease.as_secs_f64())
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n == 1)
}

/// Inside a transaction, lock the job row and verify we still hold the lease.
/// Every result write must happen after this check and in the same transaction, so
/// results and the `succeeded` state commit atomically, and only by the lease holder.
pub async fn fence(tx: &mut Transaction<'_, Postgres>, job: &ClaimedJob) -> sqlx::Result<bool> {
    let held: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM jobs WHERE id = $1 AND lease_token = $2 AND status = 'running' FOR UPDATE")
            .bind(job.id)
            .bind(job.lease_token)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(held.is_some())
}

/// Mark the job succeeded. Must be called in the same transaction as, and after, [`fence`].
pub async fn complete_in_tx(tx: &mut Transaction<'_, Postgres>, job: &ClaimedJob) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE jobs SET status = 'succeeded', finished_at = now(), updated_at = now(), last_error = NULL,
                         lease_token = NULL, lease_expires_at = NULL, locked_by = NULL
          WHERE id = $1 AND lease_token = $2",
    )
    .bind(job.id)
    .bind(job.lease_token)
    .execute(&mut **tx)
    .await?;
    finish_attempt(tx, job, "succeeded", None).await
}

/// Permanently fail the job. Returns `false` if the lease was lost (someone else owns it now).
pub async fn fail(pool: &PgPool, job: &ClaimedJob, error: &str) -> sqlx::Result<bool> {
    let mut tx = pool.begin().await?;
    let n = sqlx::query(
        "UPDATE jobs SET status = 'failed', last_error = $3, finished_at = now(), updated_at = now(),
                         lease_token = NULL, lease_expires_at = NULL, locked_by = NULL
          WHERE id = $1 AND lease_token = $2 AND status = 'running'",
    )
    .bind(job.id)
    .bind(job.lease_token)
    .bind(error)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if n == 0 {
        tx.rollback().await?;
        return Ok(false);
    }
    finish_attempt(&mut tx, job, "failed", Some(error)).await?;
    tx.commit().await?;
    Ok(true)
}

/// Put the job back in the queue, claimable after `delay`. The dispatcher publishes it when due.
/// Returns `None` if the lease was lost, otherwise the time of the next attempt.
pub async fn schedule_retry(
    pool: &PgPool,
    job: &ClaimedJob,
    error: &str,
    delay: Duration,
) -> sqlx::Result<Option<DateTime<Utc>>> {
    let mut tx = pool.begin().await?;
    let next: Option<DateTime<Utc>> = sqlx::query_scalar(
        "UPDATE jobs SET status = 'queued', last_error = $3,
                         available_at = now() + make_interval(secs => $4), published_at = NULL, updated_at = now(),
                         lease_token = NULL, lease_expires_at = NULL, locked_by = NULL
          WHERE id = $1 AND lease_token = $2 AND status = 'running'
          RETURNING available_at",
    )
    .bind(job.id)
    .bind(job.lease_token)
    .bind(error)
    .bind(delay.as_secs_f64())
    .fetch_optional(&mut *tx)
    .await?;
    if next.is_none() {
        tx.rollback().await?;
        return Ok(None);
    }
    finish_attempt(&mut tx, job, "retry_scheduled", Some(error)).await?;
    tx.commit().await?;
    Ok(next)
}

async fn finish_attempt(
    tx: &mut Transaction<'_, Postgres>,
    job: &ClaimedJob,
    outcome: &str,
    error: Option<&str>,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE job_attempts SET finished_at = now(), outcome = $3, error = $4 WHERE job_id = $1 AND attempt = $2",
    )
    .bind(job.id)
    .bind(job.attempt)
    .bind(outcome)
    .bind(error)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Result of reaping one job whose lease expired.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReapedJob {
    pub id: Uuid,
    pub kind: String,
    pub status: String,
    pub attempts: i32,
    pub previous_owner: Option<String>,
}

/// Recover jobs whose worker died (crash, OOM kill, container restart, network partition):
/// their lease expired without a heartbeat. The job goes back to `queued`, or to `failed`
/// if it already used all attempts. `SKIP LOCKED` keeps concurrent reapers (one per worker
/// instance) from blocking each other and skips a job whose owner is committing right now.
pub async fn reap_expired_leases(pool: &PgPool) -> sqlx::Result<Vec<ReapedJob>> {
    let mut tx = pool.begin().await?;
    let reaped: Vec<ReapedJob> = sqlx::query_as(
        "WITH expired AS (
             SELECT id, locked_by FROM jobs
              WHERE status = 'running' AND lease_expires_at < now()
              ORDER BY lease_expires_at
              LIMIT 100
              FOR UPDATE SKIP LOCKED
         )
         UPDATE jobs j
            SET status       = CASE WHEN j.attempts >= j.max_attempts THEN 'failed' ELSE 'queued' END,
                finished_at  = CASE WHEN j.attempts >= j.max_attempts THEN now() ELSE NULL END,
                last_error   = 'lease expired: worker ' || COALESCE(e.locked_by, '?')
                               || ' stopped responding during attempt ' || j.attempts
                               || ' (crash, restart or stall)',
                available_at = now(),
                published_at = NULL,
                lease_token = NULL, lease_expires_at = NULL, locked_by = NULL,
                updated_at = now()
           FROM expired e
          WHERE j.id = e.id
          RETURNING j.id, j.kind, j.status, j.attempts, e.locked_by AS previous_owner",
    )
    .fetch_all(&mut *tx)
    .await?;

    if !reaped.is_empty() {
        let ids: Vec<Uuid> = reaped.iter().map(|r| r.id).collect();
        sqlx::query(
            "UPDATE job_attempts SET finished_at = now(), outcome = 'lease_expired',
                    error = 'worker stopped responding (lease expired)'
              WHERE job_id = ANY($1) AND finished_at IS NULL",
        )
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(reaped)
}

/// Select queued jobs that are due and have no recent message in flight, and mark them as
/// published. Covers: the API crashing between committing a job and publishing it, a failed
/// publish, retries whose backoff elapsed, jobs returned by the reaper, and messages lost by
/// the broker (re-published after `republish_after`). Duplicates are harmless thanks to [`claim`].
pub async fn take_due_for_dispatch(
    pool: &PgPool,
    republish_after: Duration,
    limit: i64,
) -> sqlx::Result<Vec<(Uuid, JobKind)>> {
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "UPDATE jobs SET published_at = now()
          WHERE id IN (
                SELECT id FROM jobs
                 WHERE status = 'queued' AND available_at <= now()
                   AND (published_at IS NULL
                        OR published_at < available_at
                        OR published_at < now() - make_interval(secs => $1))
                 ORDER BY available_at
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED)
          RETURNING id, kind",
    )
    .bind(republish_after.as_secs_f64())
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().filter_map(|(id, kind)| JobKind::parse(&kind).map(|k| (id, k))).collect())
}

pub async fn mark_published(pool: &PgPool, job_id: Uuid) -> sqlx::Result<()> {
    sqlx::query("UPDATE jobs SET published_at = now() WHERE id = $1 AND status = 'queued'")
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Exponential backoff with jitter: `base * 2^(attempt-1)`, capped at `max`, then scaled
/// by a random factor in [0.5, 1.0] so retries from many jobs don't synchronise.
pub fn backoff_delay(attempt: i32, base: Duration, max: Duration, jitter: f64) -> Duration {
    let exp = 2f64.powi((attempt.max(1) - 1).min(30));
    let raw = (base.as_secs_f64() * exp).min(max.as_secs_f64());
    Duration::from_secs_f64(raw * (0.5 + 0.5 * jitter.clamp(0.0, 1.0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially_and_is_capped() {
        let base = Duration::from_secs(2);
        let max = Duration::from_secs(60);
        assert_eq!(backoff_delay(1, base, max, 1.0), Duration::from_secs(2));
        assert_eq!(backoff_delay(2, base, max, 1.0), Duration::from_secs(4));
        assert_eq!(backoff_delay(3, base, max, 1.0), Duration::from_secs(8));
        assert_eq!(backoff_delay(10, base, max, 1.0), Duration::from_secs(60));
        assert_eq!(backoff_delay(1000, base, max, 1.0), Duration::from_secs(60));
    }

    #[test]
    fn backoff_jitter_stays_within_half_to_full() {
        let base = Duration::from_secs(10);
        let max = Duration::from_secs(600);
        assert_eq!(backoff_delay(1, base, max, 0.0), Duration::from_secs(5));
        assert_eq!(backoff_delay(1, base, max, 0.5), Duration::from_secs_f64(7.5));
    }

    #[test]
    fn job_kind_round_trips() {
        for k in [JobKind::Import, JobKind::Report] {
            assert_eq!(JobKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(JobKind::parse("nope"), None);
    }
}
