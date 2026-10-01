use std::sync::Arc;

use csv_reports::{
    api,
    config::ApiConfig,
    db,
    queue::RabbitPublisher,
    storage::{connect_s3_with_retry, S3Config},
    telemetry,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("api");
    let config = ApiConfig::from_env();

    let pool = db::connect(&config.database_url, 20).await?;
    db::migrate(&pool).await?;

    let publisher = Arc::new(RabbitPublisher::new(config.amqp_url.clone()));
    // Warm up the broker connection; failure is fine (the worker-side dispatcher covers it).
    if !csv_reports::queue::JobPublisher::is_healthy(publisher.as_ref()).await {
        tracing::warn!("rabbitmq unavailable at startup; jobs will be dispatched once it is back");
    }

    let store = Arc::new(connect_s3_with_retry(&S3Config::from_env(), true).await?);

    let state = api::AppState { pool, publisher, store, max_attempts: config.max_attempts };
    let app = api::router(state, config.max_upload_bytes);

    let listener = tokio::net::TcpListener::bind(&config.http_addr).await?;
    tracing::info!(addr = %config.http_addr, "api listening; swagger ui at /swagger-ui");
    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;
    tracing::info!("api stopped");
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
    tracing::info!("shutdown signal received");
}
