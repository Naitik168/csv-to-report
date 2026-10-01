use std::time::Duration;

use sqlx::{postgres::PgPoolOptions, PgPool};

/// Embedded migrations (compiled into both binaries).
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Connect to Postgres, retrying with backoff so services survive starting
/// before the database is ready (or a database restart).
pub async fn connect(database_url: &str, max_connections: u32) -> anyhow::Result<PgPool> {
    let mut delay = Duration::from_millis(500);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                tracing::info!(attempt, "connected to postgres");
                return Ok(pool);
            }
            Err(e) if attempt < 30 => {
                tracing::warn!(attempt, error = %e, retry_in_ms = delay.as_millis() as u64, "postgres not reachable yet");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Apply migrations. Safe to call concurrently from several processes:
/// sqlx serialises migration runs with a Postgres advisory lock.
pub async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    MIGRATOR.run(pool).await?;
    tracing::info!("database migrations applied");
    Ok(())
}
