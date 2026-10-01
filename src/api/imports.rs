use axum::{
    extract::{Multipart, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use sqlx::{types::Json as DbJson, FromRow};
use uuid::Uuid;

use super::models::*;
use super::user::{ensure_user, idempotency_key, CurrentUser};
use super::{error::ErrorBody, ApiError, AppState};
use crate::csv_import::FieldError;
use crate::jobs::{self, JobKind};
use crate::storage::{self, StorageError};

#[derive(FromRow)]
struct ImportRow {
    #[sqlx(flatten)]
    job: JobRow,
    filename: String,
    file_size: i64,
    file_sha256: String,
    file_key: String,
    file_deleted_at: Option<chrono::DateTime<chrono::Utc>>,
    total_rows: Option<i32>,
    valid_rows: Option<i32>,
    invalid_rows: Option<i32>,
}

const IMPORT_SELECT: &str = "
    SELECT j.id, j.status, j.attempts, j.max_attempts, j.available_at, j.last_error,
           j.created_at, j.started_at, j.finished_at,
           i.filename, i.file_size, i.file_sha256, i.file_key, i.file_deleted_at, i.total_rows, i.valid_rows, i.invalid_rows
      FROM jobs j JOIN imports i ON i.id = j.id";

fn import_links(id: Uuid) -> Links {
    Links {
        self_: format!("/imports/{id}"),
        errors: Some(format!("/imports/{id}/errors")),
        file: Some(format!("/imports/{id}/file")),
        ..Default::default()
    }
}

/// Publish a job notification right away (low latency). If it fails the job is still safely
/// stored as `queued`; the worker-side dispatcher publishes it within a few seconds.
pub(super) async fn dispatch(state: &AppState, kind: JobKind, id: Uuid) {
    match state.publisher.publish(kind, id).await {
        Ok(()) => {
            if let Err(e) = jobs::mark_published(&state.pool, id).await {
                tracing::warn!(job_id = %id, error = %e, "could not record publish time");
            }
        }
        Err(e) => tracing::warn!(
            job_id = %id, kind = kind.as_str(), error = %format!("{e:#}"),
            "could not publish job; the dispatcher will publish it"
        ),
    }
}

/// Upload a CSV file and start an asynchronous import.
///
/// Returns `202 Accepted` immediately with the import id; poll `GET /imports/{id}` for progress.
#[utoipa::path(
    post, path = "/imports", tag = "imports",
    security(("user_id" = [])),
    params(("Idempotency-Key" = Option<String>, Header, description = "Optional key to make retries of this request safe")),
    request_body(content = UploadForm, content_type = "multipart/form-data"),
    responses(
        (status = 202, description = "Import job created", body = ImportAccepted),
        (status = 400, description = "No file / empty file", body = ErrorBody),
        (status = 401, description = "Missing or invalid X-User-Id", body = ErrorBody),
        (status = 413, description = "File too large", body = ErrorBody),
    )
)]
pub async fn create_import(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Response, ApiError> {
    let idem_key = idempotency_key(&headers)?;

    let mut upload: Option<(String, Vec<u8>)> = None;
    while let Some(field) = multipart.next_field().await.map_err(multipart_error)? {
        if field.name() == Some("file") {
            let filename = field
                .file_name()
                .map(|f| f.chars().take(255).collect::<String>())
                .filter(|f| !f.trim().is_empty())
                .unwrap_or_else(|| "upload.csv".to_string());
            let bytes = field.bytes().await.map_err(multipart_error)?;
            upload = Some((filename, bytes.to_vec()));
        }
    }
    let (filename, data) = upload.ok_or_else(|| ApiError::BadRequest("multipart field 'file' is required".into()))?;
    if data.is_empty() {
        return Err(ApiError::BadRequest("the uploaded file is empty".into()));
    }

    ensure_user(&state.pool, &user_id).await?;
    let sha = hex::encode(Sha256::digest(&data));

    // Idempotent replay: answer from the database before touching object storage.
    if let Some(key) = idem_key.as_deref() {
        if let Some(existing) = jobs::find_by_idempotency_key(&state.pool, &user_id, JobKind::Import, key).await? {
            return replay(&state, &user_id, existing, &sha).await;
        }
    }

    let id = Uuid::new_v4();
    let file_key = storage::upload_key(&user_id, id);
    let size = data.len();

    // 1) Store the file. If this fails nothing has been created yet, so the client can simply retry.
    state.store.put(&file_key, Bytes::from(data), "text/csv").await.map_err(|e| {
        tracing::error!(%user_id, %file_key, error = %e, "could not store upload in object storage");
        ApiError::ServiceUnavailable("object storage is unavailable; please retry the upload".into())
    })?;

    // 2) Commit the job. The worker only ever sees committed jobs, whose file already exists.
    let committed: Result<bool, ApiError> = async {
        let mut tx = state.pool.begin().await?;
        if !jobs::insert_job(&mut tx, id, JobKind::Import, &user_id, state.max_attempts, idem_key.as_deref()).await? {
            return Ok(false); // lost a race with a concurrent request using the same Idempotency-Key
        }
        sqlx::query(
            "INSERT INTO imports (id, user_id, filename, file_size, file_sha256, file_key) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(id)
        .bind(&user_id)
        .bind(&filename)
        .bind(size as i64)
        .bind(&sha)
        .bind(&file_key)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }
    .await;

    let created = match committed {
        Ok(created) => created,
        Err(e) => {
            // A failed COMMIT is ambiguous: it may have landed before the connection broke.
            // Only remove the object when we can confirm the import row does not exist;
            // otherwise keep it (worst case an orphan, never a job without its file).
            let exists: Result<bool, sqlx::Error> =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM imports WHERE id = $1)")
                    .bind(id)
                    .fetch_one(&state.pool)
                    .await;
            if let Ok(false) = exists {
                remove_uncommitted_upload(&state, &file_key).await;
            }
            return Err(e);
        }
    };
    if !created {
        remove_uncommitted_upload(&state, &file_key).await;
        let existing = jobs::find_by_idempotency_key(
            &state.pool,
            &user_id,
            JobKind::Import,
            idem_key.as_deref().unwrap_or_default(),
        )
        .await?
        .ok_or_else(|| ApiError::Conflict("idempotency key conflict; retry the request".into()))?;
        return replay(&state, &user_id, existing, &sha).await;
    }

    tracing::info!(import_id = %id, %user_id, %filename, bytes = size, sha256 = %sha, %file_key, "import job created");
    dispatch(&state, JobKind::Import, id).await;
    Ok(accepted(id, ImportState::Queued, false))
}

/// The job was not created: remove the object we just stored (best effort; anything left behind
/// by a crash at exactly this point is found by `storage-admin orphans`).
async fn remove_uncommitted_upload(state: &AppState, file_key: &str) {
    if let Err(e) = state.store.delete(file_key).await {
        tracing::warn!(%file_key, error = %e, "could not remove object of an import that was not created");
    }
}

async fn replay(state: &AppState, user_id: &str, existing: Uuid, sha: &str) -> Result<Response, ApiError> {
    let status = fetch_import(state, user_id, existing).await?;
    if status.file_sha256 != sha {
        return Err(ApiError::Conflict(
            "Idempotency-Key was already used for a different file; use a new key for a new upload".into(),
        ));
    }
    tracing::info!(import_id = %existing, %user_id, "idempotent replay of import upload");
    Ok(accepted(existing, status.status, true))
}

fn accepted(id: Uuid, status: ImportState, replayed: bool) -> Response {
    let links = import_links(id);
    let mut headers = HeaderMap::new();
    headers.insert(header::LOCATION, links.self_.parse().expect("valid header"));
    if replayed {
        headers.insert("idempotent-replayed", "true".parse().expect("valid header"));
    }
    (StatusCode::ACCEPTED, headers, Json(ImportAccepted { import_id: id, status, links })).into_response()
}

fn multipart_error(e: axum::extract::multipart::MultipartError) -> ApiError {
    if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::PayloadTooLarge("uploaded file exceeds the maximum allowed size".into())
    } else {
        ApiError::BadRequest(format!("invalid multipart body: {}", e.body_text()))
    }
}

async fn fetch_import(state: &AppState, user_id: &str, id: Uuid) -> Result<ImportStatus, ApiError> {
    let row: ImportRow = sqlx::query_as(&format!("{IMPORT_SELECT} WHERE j.id = $1 AND j.user_id = $2"))
        .bind(id)
        .bind(user_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("import {id} not found")))?;
    let history = attempt_history(&state.pool, id).await?;
    Ok(ImportStatus {
        import_id: id,
        status: row.job.import_state(row.invalid_rows),
        next_attempt_at: row.job.next_attempt_at(),
        filename: row.filename,
        file_size: row.file_size,
        file_sha256: row.file_sha256,
        file_available: row.file_deleted_at.is_none(),
        file_deleted_at: row.file_deleted_at,
        total_rows: row.total_rows,
        valid_rows: row.valid_rows,
        invalid_rows: row.invalid_rows,
        attempts: row.job.attempts,
        max_attempts: row.job.max_attempts,
        last_error: row.job.last_error,
        created_at: row.job.created_at,
        started_at: row.job.started_at,
        finished_at: row.job.finished_at,
        attempt_history: history,
        links: import_links(id),
    })
}

pub(super) async fn attempt_history(pool: &sqlx::PgPool, job_id: Uuid) -> sqlx::Result<Vec<AttemptInfo>> {
    sqlx::query_as(
        "SELECT attempt, worker_id, started_at, finished_at, outcome, error
           FROM job_attempts WHERE job_id = $1 ORDER BY attempt",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await
}

/// Get the status and progress of an import.
#[utoipa::path(
    get, path = "/imports/{id}", tag = "imports",
    security(("user_id" = [])),
    params(("id" = Uuid, Path, description = "Import id")),
    responses(
        (status = 200, description = "Import status", body = ImportStatus),
        (status = 404, description = "Not found (or belongs to another user)", body = ErrorBody),
    )
)]
pub async fn get_import(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    Path(id): Path<Uuid>,
) -> Result<Json<ImportStatus>, ApiError> {
    Ok(Json(fetch_import(&state, &user_id, id).await?))
}

/// List your imports, newest first.
#[utoipa::path(
    get, path = "/imports", tag = "imports",
    security(("user_id" = [])),
    params(Pagination),
    responses((status = 200, description = "Imports", body = ImportList))
)]
pub async fn list_imports(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    Query(page): Query<Pagination>,
) -> Result<Json<ImportList>, ApiError> {
    let (limit, offset) = page.resolve();
    let rows: Vec<ImportRow> =
        sqlx::query_as(&format!("{IMPORT_SELECT} WHERE j.user_id = $1 ORDER BY j.created_at DESC LIMIT $2 OFFSET $3"))
            .bind(&user_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&state.pool)
            .await?;
    let items = rows
        .into_iter()
        .map(|r| ImportSummary {
            import_id: r.job.id,
            status: r.job.import_state(r.invalid_rows),
            filename: r.filename,
            total_rows: r.total_rows,
            valid_rows: r.valid_rows,
            invalid_rows: r.invalid_rows,
            attempts: r.job.attempts,
            last_error: r.job.last_error,
            created_at: r.job.created_at,
            finished_at: r.job.finished_at,
        })
        .collect();
    Ok(Json(ImportList { items, limit, offset }))
}

#[derive(FromRow)]
struct ErrorRow {
    line_number: i32,
    raw_record: String,
    errors: DbJson<Vec<FieldError>>,
}

/// List the invalid rows of an import and why each was rejected.
#[utoipa::path(
    get, path = "/imports/{id}/errors", tag = "imports",
    security(("user_id" = [])),
    params(("id" = Uuid, Path, description = "Import id"), Pagination),
    responses(
        (status = 200, description = "Invalid rows", body = ImportErrorsPage),
        (status = 404, description = "Not found", body = ErrorBody),
    )
)]
pub async fn list_import_errors(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    Path(id): Path<Uuid>,
    Query(page): Query<Pagination>,
) -> Result<Json<ImportErrorsPage>, ApiError> {
    let (limit, offset) = page.resolve();
    let import = fetch_import(&state, &user_id, id).await?;
    let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM import_row_errors WHERE import_id = $1")
        .bind(id)
        .fetch_one(&state.pool)
        .await?;
    let rows: Vec<ErrorRow> = sqlx::query_as(
        "SELECT line_number, raw_record, errors FROM import_row_errors
          WHERE import_id = $1 ORDER BY line_number LIMIT $2 OFFSET $3",
    )
    .bind(id)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(ImportErrorsPage {
        import_id: id,
        status: import.status,
        invalid_rows: import.invalid_rows,
        stored_errors: stored,
        items: rows
            .into_iter()
            .map(|r| RowErrorItem { line_number: r.line_number, raw_record: r.raw_record, errors: r.errors.0 })
            .collect(),
        limit,
        offset,
    }))
}

/// Download the originally uploaded CSV file from object storage.
#[utoipa::path(
    get, path = "/imports/{id}/file", tag = "imports",
    security(("user_id" = [])),
    params(("id" = Uuid, Path, description = "Import id")),
    responses(
        (status = 200, description = "The uploaded file", content_type = "text/csv", body = String),
        (status = 404, description = "Not found", body = ErrorBody),
        (status = 410, description = "The file was removed with storage-admin", body = ErrorBody),
        (status = 503, description = "Object storage unavailable", body = ErrorBody),
    )
)]
pub async fn download_import_file(
    State(state): State<AppState>,
    CurrentUser(user_id): CurrentUser,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let row: ImportRow = sqlx::query_as(&format!("{IMPORT_SELECT} WHERE j.id = $1 AND j.user_id = $2"))
        .bind(id)
        .bind(&user_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("import {id} not found")))?;
    if let Some(at) = row.file_deleted_at {
        return Err(ApiError::Gone(format!("the uploaded file was removed from storage at {at}")));
    }
    let data = state.store.get(&row.file_key).await.map_err(storage_error)?;
    let filename: String = row.filename.chars().filter(|c| c.is_ascii_graphic() && *c != '"' && *c != '\\').collect();
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8".to_string()),
            (header::CONTENT_DISPOSITION, format!("attachment; filename=\"{filename}\"")),
        ],
        data,
    )
        .into_response())
}

/// Map object-storage errors to HTTP errors for download endpoints.
pub(super) fn storage_error(e: StorageError) -> ApiError {
    match e {
        StorageError::NotFound(_) => ApiError::Gone("the file is no longer available in storage".into()),
        StorageError::Unavailable(msg) => {
            tracing::error!(error = %msg, "object storage error while serving a download");
            ApiError::ServiceUnavailable("object storage is unavailable; please retry".into())
        }
    }
}
