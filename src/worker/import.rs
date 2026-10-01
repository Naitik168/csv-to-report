//! Import processor: download the uploaded file from object storage, parse it, then write rows,
//! row errors, counters and the `succeeded` state in ONE fenced transaction.
//!
//! Transaction boundary: parsing happens outside the transaction (it is CPU work and may be
//! slow); the transaction only covers the writes. Because everything commits atomically, a
//! crash at any point leaves either nothing or the complete result – never half an import –
//! so a retry can simply start over. The `(import_id, line_number)` primary key is a second
//! line of defence against duplicate rows.

use sqlx::types::Json;
use tracing::info;

use super::{JobError, WorkerContext};
use crate::csv_import::{self, ParsedImport};
use crate::jobs::{self, ClaimedJob};
use crate::storage::StorageError;

/// Rows per INSERT statement (keeps bind parameters well under Postgres' limits).
const BATCH: usize = 1_000;

pub(super) async fn process(ctx: &WorkerContext, job: &ClaimedJob) -> Result<(), JobError> {
    let key: Option<String> =
        sqlx::query_scalar("SELECT file_key FROM imports WHERE id = $1").bind(job.id).fetch_optional(&ctx.pool).await?;
    let key = key.ok_or_else(|| JobError::Permanent("import record not found".into()))?;

    // Download the uploaded file from object storage. A missing object can never appear by
    // retrying (it was deleted); an unreachable store is a normal transient failure.
    let file = ctx.store.get(&key).await.map_err(|e| match e {
        StorageError::NotFound(_) => {
            JobError::Permanent(format!("uploaded file {key} is no longer in object storage (was it deleted?)"))
        }
        StorageError::Unavailable(msg) => JobError::Transient(anyhow::anyhow!("reading {key}: {msg}")),
    })?;
    let bytes = file.len();

    // CPU-bound parsing off the async runtime.
    let parsed: ParsedImport = tokio::task::spawn_blocking(move || csv_import::parse_orders(&file))
        .await
        .map_err(|e| JobError::Transient(anyhow::anyhow!("parser task panicked: {e}")))?
        .map_err(|e| JobError::Permanent(format!("invalid CSV file: {e}")))?;

    info!(
        bytes,
        total_rows = parsed.total_rows(),
        valid_rows = parsed.valid.len(),
        invalid_rows = parsed.invalid.len(),
        "csv parsed"
    );

    let mut tx = ctx.pool.begin().await?;
    if !jobs::fence(&mut tx, job).await? {
        return Err(JobError::LeaseLost);
    }

    // Defensive: the atomic commit means there is never partial data, but make the write idempotent anyway.
    sqlx::query("DELETE FROM orders WHERE import_id = $1").bind(job.id).execute(&mut *tx).await?;
    sqlx::query("DELETE FROM import_row_errors WHERE import_id = $1").bind(job.id).execute(&mut *tx).await?;

    for chunk in parsed.valid.chunks(BATCH) {
        sqlx::query(
            "INSERT INTO orders (import_id, line_number, customer_id, order_id, product, quantity, unit_price, status)
             SELECT $1, * FROM UNNEST($2::INT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::INT[], $7::NUMERIC[], $8::TEXT[])",
        )
        .bind(job.id)
        .bind(chunk.iter().map(|r| r.line).collect::<Vec<_>>())
        .bind(chunk.iter().map(|r| r.customer_id.clone()).collect::<Vec<_>>())
        .bind(chunk.iter().map(|r| r.order_id.clone()).collect::<Vec<_>>())
        .bind(chunk.iter().map(|r| r.product.clone()).collect::<Vec<_>>())
        .bind(chunk.iter().map(|r| r.quantity).collect::<Vec<_>>())
        .bind(chunk.iter().map(|r| r.unit_price).collect::<Vec<_>>())
        .bind(chunk.iter().map(|r| r.status.clone()).collect::<Vec<_>>())
        .execute(&mut *tx)
        .await?;
    }

    // Every invalid row is counted; details are stored up to a cap to bound storage for garbage uploads.
    let stored_errors = &parsed.invalid[..parsed.invalid.len().min(ctx.config.max_stored_row_errors)];
    for chunk in stored_errors.chunks(BATCH) {
        sqlx::query(
            "INSERT INTO import_row_errors (import_id, line_number, raw_record, errors)
             SELECT $1, * FROM UNNEST($2::INT[], $3::TEXT[], $4::JSONB[])",
        )
        .bind(job.id)
        .bind(chunk.iter().map(|r| r.line).collect::<Vec<_>>())
        .bind(chunk.iter().map(|r| r.raw.clone()).collect::<Vec<_>>())
        .bind(chunk.iter().map(|r| Json(&r.errors)).collect::<Vec<_>>())
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query("UPDATE imports SET total_rows = $2, valid_rows = $3, invalid_rows = $4 WHERE id = $1")
        .bind(job.id)
        .bind(parsed.total_rows() as i32)
        .bind(parsed.valid.len() as i32)
        .bind(parsed.invalid.len() as i32)
        .execute(&mut *tx)
        .await?;

    jobs::complete_in_tx(&mut tx, job).await?;
    tx.commit().await?;

    if !parsed.invalid.is_empty() {
        let first = &parsed.invalid[0];
        tracing::warn!(
            invalid_rows = parsed.invalid.len(),
            first_invalid_line = first.line,
            first_invalid_reason = %first.errors.iter().map(|e| format!("{}: {}", e.field, e.message)).collect::<Vec<_>>().join("; "),
            "import completed with invalid rows"
        );
    }
    Ok(())
}
