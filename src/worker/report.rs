//! Report processor.
//!
//! 1. Compute the aggregation (read-only, outside any transaction).
//! 2. Write the full report as CSV and JSON to object storage. Keys are derived from the report
//!    id, so a retry (or a worker that lost its lease) overwrites the same objects with the same
//!    content instead of creating new ones: the writes are idempotent.
//! 3. In ONE fenced transaction: store the object keys + a small summary and mark the job
//!    succeeded. The report only becomes visible as `completed` once its files exist.

use bytes::Bytes;
use sqlx::types::Json;
use tracing::info;
use uuid::Uuid;

use super::{JobError, WorkerContext};
use crate::jobs::{self, ClaimedJob};
use crate::report;
use crate::storage::{report_key, StorageError};

pub(super) async fn process(ctx: &WorkerContext, job: &ClaimedJob) -> Result<(), JobError> {
    let import_ids: Option<Vec<Uuid>> = sqlx::query_scalar("SELECT import_ids FROM reports WHERE id = $1")
        .bind(job.id)
        .fetch_optional(&ctx.pool)
        .await?;
    let import_ids = import_ids.ok_or_else(|| JobError::Permanent("report record not found".into()))?;

    // Simulates an expensive report so the asynchronous flow (and crash recovery) is observable.
    let delay = ctx.config.report_simulated_delay;
    if !delay.is_zero() {
        info!(delay_ms = delay.as_millis() as u64, "simulating slow report generation");
        tokio::time::sleep(delay).await;
    }

    let result = report::compute(&ctx.pool, &job.user_id, &import_ids).await?;
    info!(
        imports = import_ids.len(),
        customers = result.totals.customers,
        total_orders = result.totals.total_orders,
        "report computed"
    );

    // Write the report files before committing, so `completed` always implies the files exist.
    let csv_key = report_key(&job.user_id, job.id, "csv");
    let json_key = report_key(&job.user_id, job.id, "json");
    let json =
        serde_json::to_vec_pretty(&result).map_err(|e| JobError::Permanent(format!("serialising report: {e}")))?;
    put(ctx, &csv_key, Bytes::from(report::to_csv(&result)), "text/csv; charset=utf-8").await?;
    put(ctx, &json_key, Bytes::from(json), "application/json").await?;
    info!(%csv_key, %json_key, "report files written to object storage");

    let mut tx = ctx.pool.begin().await?;
    if !jobs::fence(&mut tx, job).await? {
        return Err(JobError::LeaseLost);
    }
    // files_deleted_at is cleared: these files were just (re)written, even if an operator
    // force-deleted earlier ones while the job was still pending.
    sqlx::query("UPDATE reports SET summary = $2, csv_key = $3, json_key = $4, files_deleted_at = NULL WHERE id = $1")
        .bind(job.id)
        .bind(Json(result.summary()))
        .bind(&csv_key)
        .bind(&json_key)
        .execute(&mut *tx)
        .await?;
    jobs::complete_in_tx(&mut tx, job).await?;
    tx.commit().await?;
    Ok(())
}

async fn put(ctx: &WorkerContext, key: &str, data: Bytes, content_type: &str) -> Result<(), JobError> {
    ctx.store.put(key, data, content_type).await.map_err(|e| match e {
        StorageError::NotFound(m) | StorageError::Unavailable(m) => {
            JobError::Transient(anyhow::anyhow!("writing {key} to object storage: {m}"))
        }
    })
}
