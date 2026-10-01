//! HTTP API tests: the real router against a real database, with a no-op publisher.
//! Background jobs are executed inline with `execute_job` to drive the full workflow.

mod common;

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use common::*;
use csv_reports::{
    admin::StorageAdmin,
    api::{self, AppState},
    db::MIGRATOR,
    queue::NoopPublisher,
    worker::{execute_job, Outcome},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

fn app(t: &TestEnv) -> Router {
    let state =
        AppState { pool: t.pool.clone(), publisher: Arc::new(NoopPublisher), store: t.store.clone(), max_attempts: 5 };
    api::router(state, 1024 * 1024)
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, headers, body)
}

fn json_body(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or_else(|_| panic!("not json: {}", String::from_utf8_lossy(bytes)))
}

fn upload_req(user: Option<&str>, csv: &str, idem: Option<&str>) -> Request<Body> {
    let boundary = "XBOUNDARYX";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"orders.csv\"\r\nContent-Type: text/csv\r\n\r\n{csv}\r\n--{boundary}--\r\n"
    );
    let mut b = Request::post("/imports").header("content-type", format!("multipart/form-data; boundary={boundary}"));
    if let Some(u) = user {
        b = b.header("x-user-id", u);
    }
    if let Some(k) = idem {
        b = b.header("idempotency-key", k);
    }
    b.body(Body::from(body)).unwrap()
}

fn get(path: &str, user: &str) -> Request<Body> {
    Request::get(path).header("x-user-id", user).body(Body::empty()).unwrap()
}

fn post_json(path: &str, user: &str, body: Value) -> Request<Body> {
    Request::post(path)
        .header("x-user-id", user)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn upload(app: &Router, user: &str, csv: &str) -> Uuid {
    let (status, headers, body) = send(app, upload_req(Some(user), csv, None)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{}", String::from_utf8_lossy(&body));
    let v = json_body(&body);
    assert_eq!(v["status"], "queued");
    let id: Uuid = v["import_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(headers["location"], format!("/imports/{id}"));
    id
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn full_workflow_upload_process_report_download(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);

    // 1. Upload returns immediately with a queued job.
    let import_id = upload(&app, "alice", SAMPLE).await;
    let (_, _, body) = send(&app, get(&format!("/imports/{import_id}"), "alice")).await;
    assert_eq!(json_body(&body)["status"], "queued");

    // 2. A worker processes it.
    assert_eq!(execute_job(&t.ctx("w1"), import_id).await, Outcome::Succeeded);
    let (status, _, body) = send(&app, get(&format!("/imports/{import_id}"), "alice")).await;
    assert_eq!(status, StatusCode::OK);
    let v = json_body(&body);
    assert_eq!(v["status"], "completed_with_errors");
    assert_eq!(
        (v["total_rows"].as_i64(), v["valid_rows"].as_i64(), v["invalid_rows"].as_i64()),
        (Some(12), Some(11), Some(1))
    );
    assert_eq!(v["attempt_history"][0]["outcome"], "succeeded");

    // 3. The invalid row is explained.
    let (_, _, body) = send(&app, get(&format!("/imports/{import_id}/errors"), "alice")).await;
    let v = json_body(&body);
    assert_eq!(v["items"][0]["line_number"], 12);
    assert_eq!(v["items"][0]["raw_record"], "U006,O1011,Keyboard,1,abc,completed");
    assert_eq!(v["items"][0]["errors"][0]["field"], "unit_price");

    // 4. Request a report (empty body = all completed imports); returns immediately.
    let req = Request::post("/reports").header("x-user-id", "alice").body(Body::empty()).unwrap();
    let (status, _, body) = send(&app, req).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let v = json_body(&body);
    let report_id: Uuid = v["report_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(v["import_ids"], json!([import_id]));

    // 5. Not downloadable until done.
    let (status, _, _) = send(&app, get(&format!("/reports/{report_id}/download"), "alice")).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // 6. Worker generates it; result available as JSON and CSV.
    assert_eq!(execute_job(&t.ctx("w1"), report_id).await, Outcome::Succeeded);
    let (_, _, body) = send(&app, get(&format!("/reports/{report_id}"), "alice")).await;
    let v = json_body(&body);
    assert_eq!(v["status"], "completed");
    assert_eq!(v["files_available"], true);
    assert_eq!(v["summary"]["row_count"], 6);
    assert_eq!(v["summary"]["totals"]["total_amount"], "3825.00");
    assert!(v.get("result").is_none(), "rows are served from object storage, not the status endpoint");
    assert_eq!(v["links"]["download_csv"], format!("/reports/{report_id}/download?format=csv"));

    let (status, headers, body) = send(&app, get(&format!("/reports/{report_id}/download"), "alice")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers["content-type"].to_str().unwrap().starts_with("text/csv"));
    assert_eq!(String::from_utf8(body).unwrap(), EXPECTED_SAMPLE_REPORT);

    let (status, _, body) = send(&app, get(&format!("/reports/{report_id}/download?format=json"), "alice")).await;
    assert_eq!(status, StatusCode::OK);
    let full = json_body(&body);
    assert_eq!(full["totals"]["total_amount"], "3825.00");
    assert_eq!(
        full["rows"][0],
        json!({"user_id": "U001", "total_orders": 2, "total_quantity": 5, "total_amount": "1575.00"})
    );

    // The original upload can be downloaded back from object storage.
    let (status, _, body) = send(&app, get(&format!("/imports/{import_id}/file"), "alice")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(String::from_utf8(body).unwrap(), SAMPLE);
    let keys = t.store.keys().await;
    assert!(keys.contains(&format!("uploads/alice/{import_id}.csv")));
    assert!(keys.contains(&format!("reports/alice/{report_id}.csv")));
    assert!(keys.contains(&format!("reports/alice/{report_id}.json")));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn requests_without_a_valid_user_are_rejected(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    let (status, _, body) = send(&app, upload_req(None, SAMPLE, None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(&body)["error"]["code"], "unauthorized");

    let (status, _, _) = send(&app, get("/imports", "bad user!")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn users_cannot_see_each_others_data(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    let import_id = upload(&app, "alice", SAMPLE).await;
    execute_job(&t.ctx("w1"), import_id).await;

    let (status, _, _) = send(&app, get(&format!("/imports/{import_id}"), "bob")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, _) = send(&app, get(&format!("/imports/{import_id}/errors"), "bob")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, _, body) = send(&app, get("/imports", "bob")).await;
    assert_eq!(json_body(&body)["items"], json!([]));

    // Bob cannot build a report over Alice's import.
    let (status, _, _) = send(&app, post_json("/reports", "bob", json!({"import_ids": [import_id]}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, _, body) = send(&app, get("/imports", "alice")).await;
    assert_eq!(json_body(&body)["items"].as_array().unwrap().len(), 1);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn idempotency_key_prevents_duplicate_jobs(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    let (s1, _, b1) = send(&app, upload_req(Some("alice"), SAMPLE, Some("upload-123"))).await;
    let (s2, h2, b2) = send(&app, upload_req(Some("alice"), SAMPLE, Some("upload-123"))).await;
    assert_eq!((s1, s2), (StatusCode::ACCEPTED, StatusCode::ACCEPTED));
    assert_eq!(json_body(&b1)["import_id"], json_body(&b2)["import_id"]);
    assert_eq!(h2["idempotent-replayed"], "true");

    // Same key for a different user is independent.
    let (_, _, b3) = send(&app, upload_req(Some("bob"), SAMPLE, Some("upload-123"))).await;
    assert_ne!(json_body(&b1)["import_id"], json_body(&b3)["import_id"]);

    // Reusing a key with a different file is a client bug: reject instead of silently returning the old import.
    let (s4, _, _) = send(&app, upload_req(Some("alice"), MANY_ERRORS, Some("upload-123"))).await;
    assert_eq!(s4, StatusCode::CONFLICT);

    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs").fetch_one(&pool).await.unwrap();
    assert_eq!(jobs, 2);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn report_request_validation(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    // No completed imports yet.
    let (status, _, body) = send(&app, post_json("/reports", "alice", json!({}))).await;
    assert_eq!(status, StatusCode::CONFLICT, "{}", String::from_utf8_lossy(&body));

    // Import exists but is not processed yet.
    let import_id = upload(&app, "alice", SAMPLE).await;
    let (status, _, _) = send(&app, post_json("/reports", "alice", json!({"import_ids": [import_id]}))).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Unknown import.
    let (status, _, _) = send(&app, post_json("/reports", "alice", json!({"import_ids": [Uuid::new_v4()]}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Malformed JSON.
    let req = Request::post("/reports").header("x-user-id", "alice").body(Body::from("{nope")).unwrap();
    let (status, _, _) = send(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Failed imports cannot be reported on.
    let bad = upload(&app, "alice", BAD_HEADER).await;
    assert_eq!(execute_job(&t.ctx("w1"), bad).await, Outcome::Failed);
    let (_, _, body) = send(&app, get(&format!("/imports/{bad}"), "alice")).await;
    let v = json_body(&body);
    assert_eq!(v["status"], "failed");
    assert!(v["last_error"].as_str().unwrap().contains("missing required column"));
    let (status, _, _) = send(&app, post_json("/reports", "alice", json!({"import_ids": [bad]}))).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn upload_validation(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    let (status, _, _) = send(&app, upload_req(Some("alice"), "", None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty file");

    let big = "x".repeat(2 * 1024 * 1024);
    let (status, _, _) = send(&app, upload_req(Some("alice"), &big, None)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

    // A file of exactly the configured limit (1 MiB in these tests) is accepted.
    let exact = "x".repeat(1024 * 1024);
    let (status, _, _) = send(&app, upload_req(Some("alice"), &exact, None)).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let req = Request::post("/imports")
        .header("x-user-id", "alice")
        .header("content-type", "multipart/form-data; boundary=B")
        .body(Body::from("--B\r\nContent-Disposition: form-data; name=\"other\"\r\n\r\nhi\r\n--B--\r\n"))
        .unwrap();
    let (status, _, _) = send(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "missing 'file' field");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn retrying_state_is_visible_to_the_user(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    let import_id = upload(&app, "alice", SAMPLE).await;
    let failing = t.ctx_with("w1", |c| {
        c.chaos_failure_rate = 1.0;
        c.backoff_base = std::time::Duration::from_secs(60);
        c.backoff_max = std::time::Duration::from_secs(60);
    });
    assert_eq!(execute_job(&failing, import_id).await, Outcome::RetryScheduled);
    let (_, _, body) = send(&app, get(&format!("/imports/{import_id}"), "alice")).await;
    let v = json_body(&body);
    assert_eq!(v["status"], "retrying");
    assert_eq!(v["attempts"], 1);
    assert!(v["next_attempt_at"].is_string());
    assert!(v["last_error"].as_str().unwrap().contains("chaos"));
    assert_eq!(v["attempt_history"][0]["outcome"], "retry_scheduled");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn openapi_and_health_endpoints(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    let (status, _, body) = send(&app, Request::get("/api-docs/openapi.json").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let spec = json_body(&body);
    for path in
        ["/imports", "/imports/{id}", "/imports/{id}/errors", "/reports", "/reports/{id}", "/reports/{id}/download"]
    {
        assert!(spec["paths"][path].is_object(), "{path} documented");
    }
    let (status, _, _) = send(&app, Request::get("/health").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = send(&app, Request::get("/ready").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = send(&app, Request::get("/swagger-ui/").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn upload_is_rejected_cleanly_when_object_storage_is_down(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    t.store.set_available(false);
    let (status, _, body) = send(&app, upload_req(Some("alice"), SAMPLE, None)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json_body(&body)["error"]["code"], "service_unavailable");
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs").fetch_one(&pool).await.unwrap();
    assert_eq!(jobs, 0, "no job is created without its file");

    let (status, _, body) = send(&app, Request::get("/ready").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json_body(&body)["storage"], false);

    t.store.set_available(true);
    upload(&app, "alice", SAMPLE).await;
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn removed_files_return_410_but_data_and_status_remain(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let app = app(&t);
    let import_id = upload(&app, "alice", SAMPLE).await;
    execute_job(&t.ctx("w1"), import_id).await;
    let (_, _, body) = send(&app, post_json("/reports", "alice", json!({}))).await;
    let report_id: Uuid = json_body(&body)["report_id"].as_str().unwrap().parse().unwrap();
    execute_job(&t.ctx("w1"), report_id).await;

    let admin = StorageAdmin::new(pool.clone(), t.store.clone());
    admin.delete_import_file(import_id, false).await.unwrap().unwrap();
    admin.delete_report_files(report_id, false).await.unwrap().unwrap();
    assert!(t.store.keys().await.is_empty());

    let (status, _, _) = send(&app, get(&format!("/imports/{import_id}/file"), "alice")).await;
    assert_eq!(status, StatusCode::GONE);
    let (status, _, _) = send(&app, get(&format!("/reports/{report_id}/download"), "alice")).await;
    assert_eq!(status, StatusCode::GONE);

    let (_, _, body) = send(&app, get(&format!("/imports/{import_id}"), "alice")).await;
    let v = json_body(&body);
    assert_eq!((v["file_available"].as_bool(), v["status"].as_str()), (Some(false), Some("completed_with_errors")));
    assert!(v["file_deleted_at"].is_string());
    let (_, _, body) = send(&app, get(&format!("/reports/{report_id}"), "alice")).await;
    let v = json_body(&body);
    assert_eq!(v["files_available"], false);
    assert_eq!(v["summary"]["totals"]["total_amount"], "3825.00", "summary survives in Postgres");

    // Imported rows are kept, so a new report still works after the upload file is gone.
    let (status, _, body) = send(&app, post_json("/reports", "alice", json!({"import_ids": [import_id]}))).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let new_report: Uuid = json_body(&body)["report_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(execute_job(&t.ctx("w1"), new_report).await, Outcome::Succeeded);
    let (_, _, body) = send(&app, get(&format!("/reports/{new_report}/download"), "alice")).await;
    assert_eq!(String::from_utf8(body).unwrap(), EXPECTED_SAMPLE_REPORT);
}
