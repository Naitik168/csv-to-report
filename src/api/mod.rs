//! HTTP API (axum) with an OpenAPI document served through Swagger UI.

mod error;
mod imports;
mod models;
mod reports;
mod user;

use std::sync::Arc;

use axum::{extract::DefaultBodyLimit, extract::State, http::StatusCode, routing::get, Json, Router};
use serde_json::json;
use sqlx::PgPool;
use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::{DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::Level;
use utoipa::{
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_swagger_ui::SwaggerUi;

pub use error::ApiError;
pub use models::*;

use crate::queue::JobPublisher;
use crate::storage::BlobStore;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub publisher: Arc<dyn JobPublisher>,
    /// Object storage for uploaded files and report files.
    pub store: Arc<dyn BlobStore>,
    pub max_attempts: i32,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "CSV Import & Report API",
        version = "1.0.0",
        description = "Upload order CSVs, track asynchronous import jobs, and request asynchronous reports.\n\n\
                       **Authentication (demo):** every request identifies the caller with the `X-User-Id` header \
                       (click *Authorize* and enter e.g. `alice`). Users only see their own imports and reports.\n\n\
                       **Idempotency:** POST endpoints accept an optional `Idempotency-Key` header; repeating a request \
                       with the same key returns the original job instead of creating a new one."
    ),
    paths(
        imports::create_import, imports::list_imports, imports::get_import, imports::list_import_errors,
        imports::download_import_file,
        reports::create_report, reports::list_reports, reports::get_report, reports::download_report,
        health, ready,
    ),
    components(schemas(
        ImportAccepted, ImportStatus, ImportSummary, ImportList, ImportErrorsPage, RowErrorItem, UploadForm,
        ReportRequest, ReportAccepted, ReportStatus, ReportSummary, ReportList, AttemptInfo, Links,
        ImportState, ReportState, error::ErrorBody, error::ErrorDetail,
        crate::report::ReportResult, crate::report::ReportResultSummary, crate::report::ReportRow, crate::report::ReportTotals,
        crate::csv_import::FieldError,
    )),
    modifiers(&UserIdAuth),
    tags(
        (name = "imports", description = "CSV upload and import job tracking"),
        (name = "reports", description = "Asynchronous report generation"),
        (name = "ops", description = "Health checks"),
    )
)]
pub struct ApiDoc;

struct UserIdAuth;
impl Modify for UserIdAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "user_id",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                user::USER_HEADER,
                "Caller identity (demo auth). Letters, digits, '-', '_', '.', '@'; max 64 chars.",
            ))),
        );
    }
}

pub fn router(state: AppState, max_upload_bytes: usize) -> Router {
    let api = Router::new()
        .route("/imports", axum::routing::post(imports::create_import).get(imports::list_imports))
        .route("/imports/{id}", get(imports::get_import))
        .route("/imports/{id}/errors", get(imports::list_import_errors))
        .route("/imports/{id}/file", get(imports::download_import_file))
        .route("/reports", axum::routing::post(reports::create_report).get(reports::list_reports))
        .route("/reports/{id}", get(reports::get_report))
        .route("/reports/{id}/download", get(reports::download_report))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(state);

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .merge(api)
        // The limit applies to the whole multipart body; leave headroom for boundaries and part headers
        // so a file of exactly MAX_UPLOAD_BYTES is accepted.
        .layer(DefaultBodyLimit::max(max_upload_bytes + 64 * 1024))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|req: &axum::http::Request<_>| {
                    let request_id =
                        req.headers().get("x-request-id").and_then(|v| v.to_str().ok()).unwrap_or("-").to_string();
                    tracing::info_span!("http", method = %req.method(), path = %req.uri().path(), request_id)
                })
                .on_response(DefaultOnResponse::new().level(Level::INFO).latency_unit(LatencyUnit::Millis)),
        )
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
}

/// Liveness: the process is up.
#[utoipa::path(get, path = "/health", tag = "ops", responses((status = 200, description = "Process is alive")))]
async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

/// Readiness: database, message broker and object storage are reachable.
#[utoipa::path(get, path = "/ready", tag = "ops", responses(
    (status = 200, description = "Dependencies reachable"),
    (status = 503, description = "A dependency is unavailable"),
))]
async fn ready(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let db = sqlx::query("SELECT 1").execute(&state.pool).await.is_ok();
    let broker = state.publisher.is_healthy().await;
    let storage = state.store.is_healthy().await;
    let code = if db && broker && storage { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, Json(json!({ "database": db, "broker": broker, "storage": storage })))
}
