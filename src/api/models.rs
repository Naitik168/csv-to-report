//! Request/response types exposed by the API (and documented in OpenAPI).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::csv_import::FieldError;
use crate::report::ReportResultSummary;

/// User-facing import state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImportState {
    /// Waiting for a worker.
    Queued,
    /// A previous attempt failed with a transient error; another attempt is scheduled (`next_attempt_at`).
    Retrying,
    /// A worker is processing the file.
    Processing,
    /// All rows imported.
    Completed,
    /// Valid rows imported; some rows were invalid and skipped (see `/imports/{id}/errors`).
    CompletedWithErrors,
    /// The import failed (unusable file, or retries exhausted). See `last_error`.
    Failed,
}

/// User-facing report state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportState {
    Queued,
    Retrying,
    Processing,
    Completed,
    Failed,
}

/// Internal job columns shared by imports and reports.
#[derive(Debug, Clone, FromRow)]
pub struct JobRow {
    pub id: Uuid,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub available_at: DateTime<Utc>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl JobRow {
    fn base_state(&self) -> &'static str {
        match self.status.as_str() {
            "queued" if self.attempts > 0 => "retrying",
            "queued" => "queued",
            "running" => "processing",
            "succeeded" => "completed",
            _ => "failed",
        }
    }

    pub fn import_state(&self, invalid_rows: Option<i32>) -> ImportState {
        match self.base_state() {
            "queued" => ImportState::Queued,
            "retrying" => ImportState::Retrying,
            "processing" => ImportState::Processing,
            "completed" if invalid_rows.unwrap_or(0) > 0 => ImportState::CompletedWithErrors,
            "completed" => ImportState::Completed,
            _ => ImportState::Failed,
        }
    }

    pub fn report_state(&self) -> ReportState {
        match self.base_state() {
            "queued" => ReportState::Queued,
            "retrying" => ReportState::Retrying,
            "processing" => ReportState::Processing,
            "completed" => ReportState::Completed,
            _ => ReportState::Failed,
        }
    }

    pub fn next_attempt_at(&self) -> Option<DateTime<Utc>> {
        (self.status == "queued" && self.attempts > 0).then_some(self.available_at)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct Links {
    #[serde(rename = "self")]
    pub self_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<String>,
    /// Download the originally uploaded file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_csv: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_json: Option<String>,
}

/// One processing attempt of a job.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, FromRow)]
pub struct AttemptInfo {
    pub attempt: i32,
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    /// `succeeded` | `failed` | `retry_scheduled` | `lease_expired` | null while running
    pub outcome: Option<String>,
    pub error: Option<String>,
}

/// Multipart form for uploads.
#[allow(dead_code)]
#[derive(ToSchema)]
pub struct UploadForm {
    /// The CSV file. Required columns: user_id,order_id,product,quantity,unit_price,status
    #[schema(value_type = String, format = Binary)]
    pub file: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ImportAccepted {
    pub import_id: Uuid,
    pub status: ImportState,
    pub links: Links,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ImportStatus {
    pub import_id: Uuid,
    pub filename: String,
    pub file_size: i64,
    pub file_sha256: String,
    /// Whether the uploaded file is still in object storage (see `GET /imports/{id}/file`).
    pub file_available: bool,
    /// When the file was removed with `storage-admin` (imported rows are kept).
    pub file_deleted_at: Option<DateTime<Utc>>,
    pub status: ImportState,
    /// Data rows in the file (excluding the header). Null until processed.
    pub total_rows: Option<i32>,
    pub valid_rows: Option<i32>,
    pub invalid_rows: Option<i32>,
    pub attempts: i32,
    pub max_attempts: i32,
    /// Error of the most recent failed attempt (or the final failure reason).
    pub last_error: Option<String>,
    /// When the next attempt is scheduled (only while `retrying`).
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub attempt_history: Vec<AttemptInfo>,
    pub links: Links,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ImportSummary {
    pub import_id: Uuid,
    pub filename: String,
    pub status: ImportState,
    pub total_rows: Option<i32>,
    pub valid_rows: Option<i32>,
    pub invalid_rows: Option<i32>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ImportList {
    pub items: Vec<ImportSummary>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RowErrorItem {
    /// Line number in the uploaded file (header = line 1).
    pub line_number: i32,
    /// The rejected row as it appeared in the file.
    pub raw_record: String,
    /// Every validation error found in the row.
    pub errors: Vec<FieldError>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ImportErrorsPage {
    pub import_id: Uuid,
    pub status: ImportState,
    /// Total invalid rows in the file.
    pub invalid_rows: Option<i32>,
    /// Number of invalid rows whose details are stored (capped by MAX_STORED_ROW_ERRORS).
    pub stored_errors: i64,
    pub items: Vec<RowErrorItem>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct ReportRequest {
    /// Imports to include. Omit (or send an empty body) to include all of your completed imports.
    #[serde(default)]
    pub import_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReportAccepted {
    pub report_id: Uuid,
    pub status: ReportState,
    pub import_ids: Vec<Uuid>,
    pub links: Links,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReportStatus {
    pub report_id: Uuid,
    pub status: ReportState,
    pub import_ids: Vec<Uuid>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub last_error: Option<String>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    /// Totals and counts, present once the report is `completed`. The full report (every row)
    /// is a file in object storage: see `links.download_csv` / `links.download_json`.
    pub summary: Option<ReportResultSummary>,
    /// Whether the report files are still in object storage.
    pub files_available: bool,
    /// When the report files were removed with `storage-admin`.
    pub files_deleted_at: Option<DateTime<Utc>>,
    pub attempt_history: Vec<AttemptInfo>,
    pub links: Links,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReportSummary {
    pub report_id: Uuid,
    pub status: ReportState,
    pub import_ids: Vec<Uuid>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReportList {
    pub items: Vec<ReportSummary>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct Pagination {
    /// Page size (1-500, default 50).
    pub limit: Option<i64>,
    /// Items to skip (default 0).
    pub offset: Option<i64>,
}

impl Pagination {
    pub fn resolve(&self) -> (i64, i64) {
        (self.limit.unwrap_or(50).clamp(1, 500), self.offset.unwrap_or(0).max(0))
    }
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct DownloadParams {
    /// `csv` (default) or `json`.
    pub format: Option<String>,
}
