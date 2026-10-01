use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use sqlx::{types::Json as DbJson, FromRow};
use uuid::Uuid;

use super::imports::{attempt_history, dispatch, storage_error};
use super::models::*;
use super::user::{ensure_user, idempotency_key, CurrentUser};
use super::{error::ErrorBody, ApiError, AppState};
use crate::jobs::{self, JobKind};
use crate::report::ReportResultSummary;

#[derive(FromRow)]
struct ReportRow {
    #[sqlx(flatten)]
    job: JobRow,
    import_ids: Vec<Uuid>,
    summary: Option<DbJson<ReportResultSummary>>,
    csv_key: Option<String>,
    json_key: Option<String>,
    files_deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

const REPORT_SELECT: &str = "
    SELECT j.id, j.status, j.attempts, j.max_attempts, j.available_at, j.last_error,
           j.created_at, j.started_at, j.finished_at, r.import_ids,
           r.summary, r.csv_key, r.json_key, r.files_deleted_at
      FROM jobs j JOIN reports r ON r.id = j.id";

fn report_links(id: Uuid, ready: bool) -> Links {
    Links {
        self_: format!("/reports/{id}"),
        download_csv: ready.then(|| format!("/reports/{id}/download?format=csv")),
        download_json: ready.then(|| format!("/reports/{id}/download?format=json")),
        ..Default::default()
    }
}

/// Request an asynchronous report over your imported data.
///
/// Returns `202 Accepted` immediately; poll `GET /reports/{id}` until `status` is `completed`,
/// then fetch the result from the response or `GET /reports/{id}/download`.
///
/// The set of imports is fixed when the report is requested: either the `import_ids` you pass
/// (all must be yours and completed) or, if omitted, all your completed imports.
#[utoipa::path(
    post, path = "/reports", tag = "reports",
    security(("user_id" = [])),
    params(("Idempotency-Key" = Option<String>, Header, description = "Optional key to make retries of this request safe")),
    request_body(content = ReportRequest, description = "Optional; an empty body means all completed imports"),
    responses(
        (status = 202, description = "Report job created", body = ReportAccepted),
        (status = 400, description = "Malformed body", body = ErrorBody),
        (status = 404, description = "An import id does not exist (or is not yours)", body = ErrorBody),
        (status = 409, description = "An import is not completed, or you have no completed imports", body = ErrorBody),
    )
)]
pub async fn create_report(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let idem_key = idempotency_key(&headers)?;
    let request: ReportRequest = if body.iter().all(|b| b.is_ascii_whitespace()) {
        ReportRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::BadRequest(format!("invalid JSON body: {e}")))?
    };

    // Idempotent replay short-circuit (before validation, so a replay returns the original job).
    if let Some(key) = idem_key.as_deref() {
        if let Some(existing) = jobs::find_by_idempotency_key(&state.pool, &user_id, JobKind::Report, key).await? {
            let status = fetch_report(&state, &user_id, existing).await?;
            return Ok(accepted(existing, status.status, status.import_ids, true));
        }
    }

    let import_ids = resolve_imports(&state, &user_id, request.import_ids).await?;

    ensure_user(&state.pool, &user_id).await?;
    let id = Uuid::new_v4();
    let mut tx = state.pool.begin().await?;
    if !jobs::insert_job(&mut tx, id, JobKind::Report, &user_id, state.max_attempts, idem_key.as_deref()).await? {
        // Lost a race with a concurrent request using the same key.
        tx.rollback().await?;
        let existing = jobs::find_by_idempotency_key(
            &state.pool,
            &user_id,
            JobKind::Report,
            idem_key.as_deref().unwrap_or_default(),
        )
        .await?
        .ok_or_else(|| ApiError::Conflict("idempotency key conflict; retry the request".into()))?;
        let status = fetch_report(&state, &user_id, existing).await?;
        return Ok(accepted(existing, status.status, status.import_ids, true));
    }
    sqlx::query("INSERT INTO reports (id, user_id, import_ids) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(&user_id)
        .bind(&import_ids)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    tracing::info!(report_id = %id, %user_id, imports = import_ids.len(), "report job created");
    dispatch(&state, JobKind::Report, id).await;
    Ok(accepted(id, ReportState::Queued, import_ids, false))
}

async fn resolve_imports(state: &AppState, user_id: &str, requested: Option<Vec<Uuid>>) -> Result<Vec<Uuid>, ApiError> {
    match requested.filter(|ids| !ids.is_empty()) {
        None => {
            let ids: Vec<Uuid> = sqlx::query_scalar(
                "SELECT id FROM jobs WHERE user_id = $1 AND kind = 'import' AND status = 'succeeded' ORDER BY created_at",
            )
            .bind(user_id)
            .fetch_all(&state.pool)
            .await?;
            if ids.is_empty() {
                return Err(ApiError::Conflict(
                    "you have no completed imports yet; upload a CSV and wait for it to complete".into(),
                ));
            }
            Ok(ids)
        }
        Some(mut ids) => {
            ids.sort();
            ids.dedup();
            let found: Vec<(Uuid, String)> =
                sqlx::query_as("SELECT id, status FROM jobs WHERE user_id = $1 AND kind = 'import' AND id = ANY($2)")
                    .bind(user_id)
                    .bind(&ids)
                    .fetch_all(&state.pool)
                    .await?;
            if let Some(missing) = ids.iter().find(|id| !found.iter().any(|(f, _)| f == *id)) {
                return Err(ApiError::NotFound(format!("import {missing} not found")));
            }
            if let Some((id, status)) = found.iter().find(|(_, s)| s != "succeeded") {
                return Err(ApiError::Conflict(format!(
                    "import {id} is not completed (job status: {status}); wait for it to complete or leave it out"
                )));
            }
            Ok(ids)
        }
    }
}

fn accepted(id: Uuid, status: ReportState, import_ids: Vec<Uuid>, replayed: bool) -> Response {
    let links = report_links(id, false);
    let mut headers = HeaderMap::new();
    headers.insert(header::LOCATION, links.self_.parse().expect("valid header"));
    if replayed {
        headers.insert("idempotent-replayed", "true".parse().expect("valid header"));
    }
    (StatusCode::ACCEPTED, headers, Json(ReportAccepted { report_id: id, status, import_ids, links })).into_response()
}

async fn fetch_report(state: &AppState, user_id: &str, id: Uuid) -> Result<ReportStatus, ApiError> {
    let row: ReportRow = sqlx::query_as(&format!("{REPORT_SELECT} WHERE j.id = $1 AND j.user_id = $2"))
        .bind(id)
        .bind(user_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("report {id} not found")))?;
    let history = attempt_history(&state.pool, id).await?;
    let status = row.job.report_state();
    Ok(ReportStatus {
        report_id: id,
        status,
        import_ids: row.import_ids,
        attempts: row.job.attempts,
        max_attempts: row.job.max_attempts,
        next_attempt_at: row.job.next_attempt_at(),
        last_error: row.job.last_error,
        created_at: row.job.created_at,
        started_at: row.job.started_at,
        finished_at: row.job.finished_at,
        summary: row.summary.map(|r| r.0),
        files_available: status == ReportState::Completed && row.files_deleted_at.is_none(),
        files_deleted_at: row.files_deleted_at,
        attempt_history: history,
        links: report_links(id, status == ReportState::Completed && row.files_deleted_at.is_none()),
    })
}

/// Get the status of a report, including its summary (totals) once it is completed.
#[utoipa::path(
    get, path = "/reports/{id}", tag = "reports",
    security(("user_id" = [])),
    params(("id" = Uuid, Path, description = "Report id")),
    responses(
        (status = 200, description = "Report status (and summary when completed)", body = ReportStatus),
        (status = 404, description = "Not found", body = ErrorBody),
    )
)]
pub async fn get_report(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    Path(id): Path<Uuid>,
) -> Result<Json<ReportStatus>, ApiError> {
    Ok(Json(fetch_report(&state, &user_id, id).await?))
}

/// List your reports, newest first.
#[utoipa::path(
    get, path = "/reports", tag = "reports",
    security(("user_id" = [])),
    params(Pagination),
    responses((status = 200, description = "Reports", body = ReportList))
)]
pub async fn list_reports(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    Query(page): Query<Pagination>,
) -> Result<Json<ReportList>, ApiError> {
    let (limit, offset) = page.resolve();
    let rows: Vec<ReportRow> =
        sqlx::query_as(&format!("{REPORT_SELECT} WHERE j.user_id = $1 ORDER BY j.created_at DESC LIMIT $2 OFFSET $3"))
            .bind(&user_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&state.pool)
            .await?;
    let items = rows
        .into_iter()
        .map(|r| ReportSummary {
            report_id: r.job.id,
            status: r.job.report_state(),
            import_ids: r.import_ids,
            attempts: r.job.attempts,
            last_error: r.job.last_error,
            created_at: r.job.created_at,
            finished_at: r.job.finished_at,
        })
        .collect();
    Ok(Json(ReportList { items, limit, offset }))
}

/// Download a completed report as CSV (default) or JSON. The file is served from object storage.
#[utoipa::path(
    get, path = "/reports/{id}/download", tag = "reports",
    security(("user_id" = [])),
    params(("id" = Uuid, Path, description = "Report id"), DownloadParams),
    responses(
        (status = 200, description = "Report file", content_type = "text/csv", body = String),
        (status = 400, description = "Unknown format", body = ErrorBody),
        (status = 404, description = "Not found", body = ErrorBody),
        (status = 409, description = "Report is not completed yet", body = ErrorBody),
        (status = 410, description = "Report files were removed with storage-admin", body = ErrorBody),
        (status = 503, description = "Object storage unavailable", body = ErrorBody),
    )
)]
pub async fn download_report(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    Path(id): Path<Uuid>,
    Query(params): Query<DownloadParams>,
) -> Result<Response, ApiError> {
    let format = params.format.unwrap_or_else(|| "csv".into()).to_ascii_lowercase();
    if format != "csv" && format != "json" {
        return Err(ApiError::BadRequest("format must be 'csv' or 'json'".into()));
    }
    let row: ReportRow = sqlx::query_as(&format!("{REPORT_SELECT} WHERE j.id = $1 AND j.user_id = $2"))
        .bind(id)
        .bind(&user_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("report {id} not found")))?;
    let status = row.job.report_state();
    if status != ReportState::Completed {
        let s = serde_json::to_value(status).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
        return Err(ApiError::Conflict(format!("report is not ready (status: {s}); poll GET /reports/{id}")));
    }
    if let Some(at) = row.files_deleted_at {
        return Err(ApiError::Gone(format!("the report files were removed from storage at {at}")));
    }
    let (key, content_type) =
        if format == "csv" { (row.csv_key, "text/csv; charset=utf-8") } else { (row.json_key, "application/json") };
    let key = key.ok_or_else(|| ApiError::Internal(anyhow::anyhow!("completed report {id} has no {format} key")))?;
    let body = state.store.get(&key).await.map_err(storage_error)?;
    let disposition = format!("attachment; filename=\"report-{id}.{format}\"");
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type.to_string()), (header::CONTENT_DISPOSITION, disposition)],
        body,
    )
        .into_response())
}
