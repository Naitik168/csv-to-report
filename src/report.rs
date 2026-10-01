//! Report aggregation.
//!
//! Report semantics (documented in README):
//! * One row per customer (the CSV's `user_id` column) across the imports the report covers.
//! * Only orders with status `completed` are counted.
//! * Invalid rows were never imported, so they are naturally excluded; their count is
//!   included in the report for transparency.
//! * If the same `order_id` appears in several covered imports (e.g. the same file uploaded
//!   twice, or a corrected re-upload), only the row from the **most recent** import is counted,
//!   so re-uploading never double counts.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgExecutor;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct ReportRow {
    /// Customer id (the CSV `user_id` column).
    pub user_id: String,
    pub total_orders: i64,
    pub total_quantity: i64,
    #[schema(value_type = String, example = "1575.00")]
    pub total_amount: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ReportTotals {
    pub customers: i64,
    pub total_orders: i64,
    pub total_quantity: i64,
    #[schema(value_type = String, example = "4005.00")]
    pub total_amount: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ReportResult {
    pub generated_at: DateTime<Utc>,
    pub import_ids: Vec<Uuid>,
    /// Order statuses included in the totals.
    pub counted_statuses: Vec<String>,
    /// Invalid CSV rows (across the covered imports) that were excluded because they failed validation.
    pub excluded_invalid_rows: i64,
    /// Rows superseded by the same order_id in a more recent import.
    pub superseded_duplicate_orders: i64,
    pub rows: Vec<ReportRow>,
    pub totals: ReportTotals,
}

/// Everything about a report except its rows. Stored in Postgres and returned by
/// `GET /reports/{id}`; the full report (with rows) lives in object storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ReportResultSummary {
    pub generated_at: DateTime<Utc>,
    pub import_ids: Vec<Uuid>,
    pub counted_statuses: Vec<String>,
    pub excluded_invalid_rows: i64,
    pub superseded_duplicate_orders: i64,
    /// Number of rows (customers) in the report file.
    pub row_count: i64,
    pub totals: ReportTotals,
}

impl ReportResult {
    pub fn summary(&self) -> ReportResultSummary {
        ReportResultSummary {
            generated_at: self.generated_at,
            import_ids: self.import_ids.clone(),
            counted_statuses: self.counted_statuses.clone(),
            excluded_invalid_rows: self.excluded_invalid_rows,
            superseded_duplicate_orders: self.superseded_duplicate_orders,
            row_count: self.rows.len() as i64,
            totals: self.totals.clone(),
        }
    }
}

const COUNTED_STATUSES: [&str; 1] = ["completed"];

/// Compute the report for the given imports (which must belong to `user_id`).
pub async fn compute<'e, E>(db: E, user_id: &str, import_ids: &[Uuid]) -> sqlx::Result<ReportResult>
where
    E: PgExecutor<'e> + Copy,
{
    // Latest import wins for duplicated order ids (DISTINCT ON keeps the first row per order_id).
    let rows: Vec<ReportRow> = sqlx::query_as(
        "WITH latest AS (
             SELECT DISTINCT ON (o.order_id) o.customer_id, o.quantity, o.unit_price, o.status
               FROM orders o
               JOIN imports i ON i.id = o.import_id
              WHERE i.user_id = $1 AND o.import_id = ANY($2)
              ORDER BY o.order_id, i.created_at DESC, i.id DESC
         )
         SELECT customer_id AS user_id,
                COUNT(*)::BIGINT                     AS total_orders,
                SUM(quantity)::BIGINT                AS total_quantity,
                SUM(quantity * unit_price)::NUMERIC(14,2) AS total_amount
           FROM latest
          WHERE status = ANY($3)
          GROUP BY customer_id
          ORDER BY customer_id",
    )
    .bind(user_id)
    .bind(import_ids)
    .bind(&COUNTED_STATUSES[..])
    .fetch_all(db)
    .await?;

    let (invalid, duplicates): (i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COALESCE(SUM(invalid_rows), 0)::BIGINT FROM imports WHERE user_id = $1 AND id = ANY($2)),
            (SELECT (COUNT(*) - COUNT(DISTINCT o.order_id))::BIGINT
               FROM orders o JOIN imports i ON i.id = o.import_id
              WHERE i.user_id = $1 AND o.import_id = ANY($2))",
    )
    .bind(user_id)
    .bind(import_ids)
    .fetch_one(db)
    .await?;

    Ok(build_result(import_ids.to_vec(), rows, invalid, duplicates))
}

pub fn build_result(import_ids: Vec<Uuid>, rows: Vec<ReportRow>, invalid: i64, duplicates: i64) -> ReportResult {
    let totals = ReportTotals {
        customers: rows.len() as i64,
        total_orders: rows.iter().map(|r| r.total_orders).sum(),
        total_quantity: rows.iter().map(|r| r.total_quantity).sum(),
        total_amount: rows.iter().map(|r| r.total_amount).sum(),
    };
    ReportResult {
        generated_at: Utc::now(),
        import_ids,
        counted_statuses: COUNTED_STATUSES.iter().map(|s| s.to_string()).collect(),
        excluded_invalid_rows: invalid,
        superseded_duplicate_orders: duplicates,
        rows,
        totals,
    }
}

/// Render the report as CSV (matching the example in the assignment).
pub fn to_csv(report: &ReportResult) -> String {
    let mut w = csv::Writer::from_writer(Vec::new());
    w.write_record(["user_id", "total_orders", "total_quantity", "total_amount"]).expect("in-memory write");
    for r in &report.rows {
        w.write_record([
            r.user_id.clone(),
            r.total_orders.to_string(),
            r.total_quantity.to_string(),
            format!("{:.2}", r.total_amount),
        ])
        .expect("in-memory write");
    }
    String::from_utf8(w.into_inner().expect("in-memory flush")).expect("csv output is utf-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, orders: i64, qty: i64, amount: &str) -> ReportRow {
        ReportRow {
            user_id: id.into(),
            total_orders: orders,
            total_quantity: qty,
            total_amount: amount.parse().unwrap(),
        }
    }

    #[test]
    fn csv_matches_expected_format() {
        let report = build_result(vec![], vec![row("U001", 2, 5, "1575"), row("U005", 2, 7, "90.00")], 1, 0);
        assert_eq!(
            to_csv(&report),
            "user_id,total_orders,total_quantity,total_amount\nU001,2,5,1575.00\nU005,2,7,90.00\n"
        );
    }

    #[test]
    fn totals_are_summed() {
        let report = build_result(vec![], vec![row("A", 1, 2, "3.50"), row("B", 4, 5, "6.25")], 0, 0);
        assert_eq!(report.totals.customers, 2);
        assert_eq!(report.totals.total_orders, 5);
        assert_eq!(report.totals.total_quantity, 7);
        assert_eq!(report.totals.total_amount, "9.75".parse::<Decimal>().unwrap());
    }

    #[test]
    fn money_serialises_as_string_to_avoid_float_rounding() {
        let json = serde_json::to_value(row("A", 1, 1, "0.10")).unwrap();
        assert_eq!(json["total_amount"], "0.10");
    }
}
