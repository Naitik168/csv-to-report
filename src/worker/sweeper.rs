//! The sweeper runs periodically in every worker instance and performs two recovery duties:
//!
//! 1. **Lease reaper** – jobs stuck in `running` whose lease expired (worker crashed, was
//!    killed, or the whole system restarted mid-job) go back to `queued` (or `failed` when
//!    their attempts are used up).
//! 2. **Dispatcher** – queued jobs that are due but have no message in flight get
//!    (re)published: retries whose backoff elapsed, reaped jobs, jobs whose initial publish
//!    failed (API crashed or broker was down), and messages lost by the broker.
//!
//! Running it in every worker is safe: both steps use `FOR UPDATE SKIP LOCKED`, so
//! instances split the work instead of fighting over it, and duplicates are harmless anyway.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::WorkerContext;
use crate::jobs;
use crate::queue::JobPublisher;

pub async fn run_sweeper(ctx: WorkerContext, publisher: Arc<dyn JobPublisher>, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(ctx.config.sweep_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {}
        }
        if let Err(e) = sweep_once(&ctx, publisher.as_ref()).await {
            warn!(error = %e, "sweep failed; will retry on next tick");
        }
    }
}

/// One sweep. Returns (reaped, dispatched) counts.
pub async fn sweep_once(ctx: &WorkerContext, publisher: &dyn JobPublisher) -> anyhow::Result<(usize, usize)> {
    let reaped = jobs::reap_expired_leases(&ctx.pool).await?;
    for r in &reaped {
        warn!(
            job_id = %r.id, kind = %r.kind, attempts = r.attempts, new_status = %r.status,
            previous_owner = r.previous_owner.as_deref().unwrap_or("?"),
            "recovered job with expired lease (worker crashed or stalled)"
        );
    }

    let due = jobs::take_due_for_dispatch(&ctx.pool, ctx.config.republish_after, 100).await?;
    let mut dispatched = 0;
    for (job_id, kind) in &due {
        match publisher.publish(*kind, *job_id).await {
            Ok(()) => {
                dispatched += 1;
                info!(%job_id, kind = kind.as_str(), "dispatched queued job");
            }
            Err(e) => {
                // published_at was set; the job is retried after `republish_after`. Clear it so it is retried next tick.
                warn!(%job_id, error = %e, "failed to publish job; will retry");
                let _ = sqlx::query("UPDATE jobs SET published_at = NULL WHERE id = $1 AND status = 'queued'")
                    .bind(job_id)
                    .execute(&ctx.pool)
                    .await;
            }
        }
    }
    Ok((reaped.len(), dispatched))
}
