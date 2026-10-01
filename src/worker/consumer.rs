//! RabbitMQ consumer loop with automatic reconnection and graceful shutdown.
//!
//! Ack policy: a message is acked **after** `execute_job` has recorded the outcome in
//! Postgres (success, retry scheduled, or failure). If the worker dies before acking,
//! RabbitMQ redelivers the message; the redelivery either finds the job already finished
//! (claim is a no-op → ack) or still leased by the dead worker (no-op → ack; the lease
//! reaper recovers it). Either way the database decides, never the message.

use std::time::Duration;

use futures_util::StreamExt;
use lapin::{
    message::Delivery,
    options::{BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions},
    types::FieldTable,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, warn};

use super::{execute_job, WorkerContext};
use crate::queue::{self, JobMessage, IMPORT_QUEUE, REPORT_QUEUE};

pub async fn run_consumers(ctx: WorkerContext, amqp_url: String, shutdown: CancellationToken) {
    let mut delay = Duration::from_secs(1);
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        match consume_session(&ctx, &amqp_url, &shutdown).await {
            Ok(()) => return, // graceful shutdown
            Err(e) => {
                error!(error = %e, retry_in_ms = delay.as_millis() as u64, "consumer session ended unexpectedly; reconnecting");
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn consume_session(ctx: &WorkerContext, amqp_url: &str, shutdown: &CancellationToken) -> anyhow::Result<()> {
    let cfg = &ctx.config;
    let conn = queue::connect_with_retry(amqp_url).await?;
    let channel = conn.create_channel().await?;
    queue::declare_topology(&channel).await?;
    // Bound in-flight messages per consumer: this is the worker's concurrency limit.
    channel.basic_qos(cfg.concurrency, BasicQosOptions::default()).await?;

    let imports = channel
        .basic_consume(
            IMPORT_QUEUE,
            &format!("{}-imports", cfg.worker_id),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    let reports = channel
        .basic_consume(
            REPORT_QUEUE,
            &format!("{}-reports", cfg.worker_id),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!(worker_id = %cfg.worker_id, prefetch = cfg.concurrency, "consuming from {IMPORT_QUEUE} and {REPORT_QUEUE}");

    let tracker = TaskTracker::new();
    let mut deliveries = futures_util::stream::select(imports, reports);

    let result = loop {
        tokio::select! {
            _ = shutdown.cancelled() => break Ok(()),
            next = deliveries.next() => match next {
                Some(Ok(delivery)) => {
                    let ctx = ctx.clone();
                    tracker.spawn(handle_delivery(ctx, delivery));
                }
                Some(Err(e)) => break Err(anyhow::Error::from(e)),
                None => break Err(anyhow::anyhow!("consumer stream closed by broker")),
            }
        }
    };

    // Stop the broker from sending more messages to this worker while we drain.
    for tag in [format!("{}-imports", cfg.worker_id), format!("{}-reports", cfg.worker_id)] {
        let _ = channel.basic_cancel(&tag, BasicCancelOptions::default()).await;
    }

    // Let in-flight jobs finish (bounded) before closing the connection.
    tracker.close();
    let in_flight = tracker.len();
    if in_flight > 0 {
        info!(in_flight, grace_secs = cfg.shutdown_grace.as_secs(), "waiting for in-flight jobs");
    }
    if tokio::time::timeout(cfg.shutdown_grace, tracker.wait()).await.is_err() {
        warn!(
            still_running = tracker.len(),
            "grace period elapsed; unfinished jobs will be recovered by lease expiry on another worker"
        );
    }
    let _ = conn.close(200, "worker shutting down").await;
    result
}

async fn handle_delivery(ctx: WorkerContext, delivery: Delivery) {
    let message: JobMessage = match serde_json::from_slice(&delivery.data) {
        Ok(m) => m,
        Err(e) => {
            // Poison message: it can never be processed. Drop it instead of redelivering forever.
            error!(error = %e, body = %String::from_utf8_lossy(&delivery.data), "malformed queue message; rejecting");
            let _ = delivery.reject(BasicRejectOptions { requeue: false }).await;
            return;
        }
    };
    if delivery.redelivered {
        info!(job_id = %message.job_id, "message was redelivered by the broker");
    }

    let outcome = execute_job(&ctx, message.job_id).await;
    tracing::debug!(job_id = %message.job_id, ?outcome, "delivery handled");

    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
        // Connection dropped; the broker will redeliver and the redelivery will be a no-op.
        warn!(job_id = %message.job_id, error = %e, "failed to ack message; broker will redeliver (harmless)");
    }
}
