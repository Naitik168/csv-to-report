use std::sync::Arc;

use csv_reports::{
    config::WorkerConfig,
    db,
    queue::RabbitPublisher,
    storage::{connect_s3_with_retry, S3Config},
    telemetry,
    worker::{run_consumers, run_sweeper, WorkerContext},
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("worker");
    let config = WorkerConfig::from_env();
    tracing::info!(
        worker_id = %config.worker_id,
        concurrency = config.concurrency,
        lease_secs = config.lease.as_secs(),
        job_timeout_secs = config.job_timeout.as_secs(),
        chaos_failure_rate = config.chaos_failure_rate,
        "worker starting"
    );

    let pool = db::connect(&config.database_url, (config.concurrency as u32 * 2 + 4).max(8)).await?;
    db::migrate(&pool).await?;

    let amqp_url = config.amqp_url.clone();
    let store = Arc::new(connect_s3_with_retry(&S3Config::from_env(), true).await?);
    let ctx = WorkerContext::new(pool, config, store);
    let publisher = Arc::new(RabbitPublisher::new(amqp_url.clone()));
    let shutdown = CancellationToken::new();

    let sweeper = tokio::spawn(run_sweeper(ctx.clone(), publisher, shutdown.clone()));
    let consumers = tokio::spawn(run_consumers(ctx.clone(), amqp_url, shutdown.clone()));

    shutdown_signal().await;
    tracing::info!("shutdown requested: no new jobs will be taken; finishing in-flight jobs");
    shutdown.cancel();
    let _ = consumers.await;
    let _ = sweeper.await;
    tracing::info!("worker stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}
