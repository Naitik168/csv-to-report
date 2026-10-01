//! Integration tests for background processing: claiming, idempotency, crash recovery,
//! fencing, retries, timeouts and dispatch. Each test gets a fresh database.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use csv_reports::storage::BlobStore;
use csv_reports::{
    db::MIGRATOR,
    jobs,
    queue::NoopPublisher,
    report,
    worker::{execute_job, sweep_once, Outcome},
};
use sqlx::PgPool;

#[sqlx::test(migrator = "MIGRATOR")]
async fn sample_import_keeps_valid_rows_and_records_the_invalid_one(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    assert_eq!(execute_job(&t.ctx("w1"), id).await, Outcome::Succeeded);

    let st = job_state(&pool, id).await;
    assert_eq!(st.status, "succeeded");
    assert_eq!(st.attempts, 1);
    assert!(st.lease_token.is_none(), "lease is released on completion");

    let (total, valid, invalid): (i32, i32, i32) =
        sqlx::query_as("SELECT total_rows, valid_rows, invalid_rows FROM imports WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((total, valid, invalid), (12, 11, 1));
    assert_eq!(order_count(&pool, id).await, 11);

    let (line, raw, errors): (i32, String, serde_json::Value) =
        sqlx::query_as("SELECT line_number, raw_record, errors FROM import_row_errors WHERE import_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(line, 12);
    assert_eq!(raw, "U006,O1011,Keyboard,1,abc,completed");
    assert_eq!(errors[0]["field"], "unit_price");

    let outcome: String = sqlx::query_scalar("SELECT outcome FROM job_attempts WHERE job_id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outcome, "succeeded");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn report_for_sample_data_matches_expected_totals(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let import = t.create_import("alice", SAMPLE, 5).await;
    assert_eq!(execute_job(&t.ctx("w1"), import).await, Outcome::Succeeded);

    let report_id = t.create_report("alice", &[import], 5).await;
    assert_eq!(execute_job(&t.ctx("w1"), report_id).await, Outcome::Succeeded);

    let result = t.report_result(report_id).await;
    assert_eq!(report::to_csv(&result), EXPECTED_SAMPLE_REPORT);
    assert_eq!(t.report_csv(report_id).await, EXPECTED_SAMPLE_REPORT, "CSV file in object storage");
    assert_eq!(result.excluded_invalid_rows, 1);
    assert_eq!(result.totals.total_orders, 11);
    assert_eq!(result.totals.total_amount.to_string(), "3825.00");

    // Postgres keeps only the summary; the rows live in object storage.
    let summary: sqlx::types::Json<report::ReportResultSummary> =
        sqlx::query_scalar("SELECT summary FROM reports WHERE id = $1").bind(report_id).fetch_one(&pool).await.unwrap();
    assert_eq!(summary.0, result.summary());
    assert_eq!(summary.0.row_count, 6);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn concurrent_claims_only_one_worker_wins(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    let mut handles = Vec::new();
    for i in 0..10 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            jobs::claim(&pool, id, &format!("w{i}"), Duration::from_secs(30)).await.unwrap()
        }));
    }
    let mut winners = 0;
    for h in handles {
        if h.await.unwrap().is_some() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one worker may hold the job");
    assert_eq!(job_state(&pool, id).await.attempts, 1);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn duplicate_delivery_to_many_workers_processes_once(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    // Same message delivered to 5 workers at the same time.
    let mut handles = Vec::new();
    for i in 0..5 {
        let c = t.ctx(&format!("w{i}"));
        handles.push(tokio::spawn(async move { execute_job(&c, id).await }));
    }
    let outcomes: Vec<Outcome> = futures_join(handles).await;
    assert_eq!(outcomes.iter().filter(|o| **o == Outcome::Succeeded).count(), 1);
    assert_eq!(outcomes.iter().filter(|o| **o == Outcome::Skipped).count(), 4);

    // A late redelivery after completion is also a no-op.
    assert_eq!(execute_job(&t.ctx("late"), id).await, Outcome::Skipped);
    assert_eq!(order_count(&pool, id).await, 11, "rows are never duplicated");
}

async fn futures_join<T>(handles: Vec<tokio::task::JoinHandle<T>>) -> Vec<T> {
    let mut out = Vec::new();
    for h in handles {
        out.push(h.await.unwrap());
    }
    out
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn crashed_worker_job_is_recovered_by_the_reaper_and_completes_once(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;

    // Worker A claims the job and "crashes" (never finishes, never heart-beats).
    let a = jobs::claim(&pool, id, "worker-a", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(a.attempt, 1);
    // A redelivered message while A still holds a live lease is a no-op.
    assert_eq!(execute_job(&t.ctx("worker-b"), id).await, Outcome::Skipped);

    // The lease runs out; the sweeper recovers the job.
    expire_lease(&pool, id).await;
    let (reaped, dispatched) = sweep_once(&t.ctx("worker-b"), &NoopPublisher).await.unwrap();
    assert_eq!((reaped, dispatched), (1, 1), "reaped and re-dispatched");

    let st = job_state(&pool, id).await;
    assert_eq!(st.status, "queued");
    assert!(st.last_error.unwrap().contains("worker-a"));

    // Worker B processes it successfully.
    assert_eq!(execute_job(&t.ctx("worker-b"), id).await, Outcome::Succeeded);
    let st = job_state(&pool, id).await;
    assert_eq!((st.status.as_str(), st.attempts), ("succeeded", 2));
    assert_eq!(order_count(&pool, id).await, 11);

    let history: Vec<(i32, String, String)> =
        sqlx::query_as("SELECT attempt, worker_id, outcome FROM job_attempts WHERE job_id = $1 ORDER BY attempt")
            .bind(id)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        history,
        vec![(1, "worker-a".into(), "lease_expired".into()), (2, "worker-b".into(), "succeeded".into())]
    );
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn stale_worker_cannot_overwrite_the_new_owner(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    let stale = jobs::claim(&pool, id, "worker-a", Duration::from_secs(30)).await.unwrap().unwrap();

    // A stalls past its lease; B takes over and finishes.
    expire_lease(&pool, id).await;
    sweep_once(&t.ctx("worker-b"), &NoopPublisher).await.unwrap();
    assert_eq!(execute_job(&t.ctx("worker-b"), id).await, Outcome::Succeeded);

    // A wakes up: every write it attempts is fenced off.
    assert!(!jobs::heartbeat(&pool, &stale, Duration::from_secs(30)).await.unwrap());
    let mut tx = pool.begin().await.unwrap();
    assert!(!jobs::fence(&mut tx, &stale).await.unwrap());
    tx.rollback().await.unwrap();
    assert!(!jobs::fail(&pool, &stale, "late failure").await.unwrap());
    assert!(jobs::schedule_retry(&pool, &stale, "late retry", Duration::ZERO).await.unwrap().is_none());

    let st = job_state(&pool, id).await;
    assert_eq!(st.status, "succeeded", "B's result stands");
    assert_eq!(order_count(&pool, id).await, 11);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn transient_failures_back_off_then_give_up_after_max_attempts(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 3).await;
    // Long backoff so "not due yet" can't race with a slow test machine; make_due() skips it.
    let failing = t.ctx_with("w1", |c| {
        c.chaos_failure_rate = 1.0;
        c.backoff_base = Duration::from_secs(60);
        c.backoff_max = Duration::from_secs(60);
    });

    // Attempt 1 fails -> retry scheduled in the future.
    assert_eq!(execute_job(&failing, id).await, Outcome::RetryScheduled);
    let st = job_state(&pool, id).await;
    assert_eq!((st.status.as_str(), st.attempts), ("queued", 1));
    assert!(st.last_error.unwrap().contains("chaos"));
    let not_due: bool = sqlx::query_scalar("SELECT available_at > now() FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(not_due, "backoff delays the next attempt");
    assert_eq!(execute_job(&failing, id).await, Outcome::Skipped, "not claimable before backoff elapses");

    // Attempt 2 fails.
    make_due(&pool, id).await;
    assert_eq!(execute_job(&failing, id).await, Outcome::RetryScheduled);

    // Attempt 3 fails -> attempts exhausted -> failed.
    make_due(&pool, id).await;
    assert_eq!(execute_job(&failing, id).await, Outcome::Failed);
    let st = job_state(&pool, id).await;
    assert_eq!((st.status.as_str(), st.attempts), ("failed", 3));
    assert!(st.last_error.unwrap().contains("gave up after 3 attempts"));
    assert_eq!(order_count(&pool, id).await, 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn transient_failure_then_success_on_retry(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    assert_eq!(execute_job(&t.ctx_with("w1", |c| c.chaos_failure_rate = 1.0), id).await, Outcome::RetryScheduled);
    make_due(&pool, id).await;
    assert_eq!(execute_job(&t.ctx("w2"), id).await, Outcome::Succeeded);
    assert_eq!(job_state(&pool, id).await.attempts, 2);
    assert_eq!(order_count(&pool, id).await, 11);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn unusable_file_fails_permanently_without_retry(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", BAD_HEADER, 5).await;
    assert_eq!(execute_job(&t.ctx("w1"), id).await, Outcome::Failed);
    let st = job_state(&pool, id).await;
    assert_eq!((st.status.as_str(), st.attempts), ("failed", 1), "no retries for permanent errors");
    assert!(st.last_error.unwrap().contains("missing required column"));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn job_that_keeps_crashing_workers_eventually_fails(pool: PgPool) {
    let t = TestEnv::new(&pool);
    // Poison pill: every attempt crashes the worker. Attempts are counted at claim time,
    // so the reaper stops re-queueing once max_attempts is reached.
    let id = t.create_import("alice", SAMPLE, 2).await;
    for attempt in 1..=2 {
        jobs::claim(&pool, id, "crashy", Duration::from_secs(30)).await.unwrap().expect("claimable");
        expire_lease(&pool, id).await;
        sweep_once(&t.ctx("sweeper"), &NoopPublisher).await.unwrap();
        let st = job_state(&pool, id).await;
        let expected = if attempt < 2 { "queued" } else { "failed" };
        assert_eq!(st.status, expected, "after attempt {attempt}");
    }
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn attempt_timeout_is_a_transient_failure(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let import = t.create_import("alice", SAMPLE, 5).await;
    execute_job(&t.ctx("w1"), import).await;
    let report_id = t.create_report("alice", &[import], 5).await;
    let slow = t.ctx_with("w1", |c| {
        c.report_simulated_delay = Duration::from_millis(500);
        c.job_timeout = Duration::from_millis(50);
    });
    assert_eq!(execute_job(&slow, report_id).await, Outcome::RetryScheduled);
    assert!(job_state(&pool, report_id).await.last_error.unwrap().contains("timed out"));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn heartbeats_keep_long_jobs_alive(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let import = t.create_import("alice", SAMPLE, 5).await;
    execute_job(&t.ctx("w1"), import).await;
    let report_id = t.create_report("alice", &[import], 5).await;
    // The job takes 4x longer than the lease but heart-beats, so a concurrent sweeper never reaps it.
    let c = t.ctx_with("w1", |c| {
        c.lease = Duration::from_secs(1);
        c.heartbeat_interval = Duration::from_millis(200);
        c.report_simulated_delay = Duration::from_millis(2_500);
    });
    let job = tokio::spawn({
        let c = c.clone();
        async move { execute_job(&c, report_id).await }
    });
    for _ in 0..12 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let (reaped, _) = sweep_once(&t.ctx("sweeper"), &NoopPublisher).await.unwrap();
        assert_eq!(reaped, 0, "a heart-beating job must not be reaped");
    }
    assert_eq!(job.await.unwrap(), Outcome::Succeeded);
    assert_eq!(job_state(&pool, report_id).await.attempts, 1);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn dispatcher_publishes_undelivered_and_due_jobs_only(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let c = t.ctx("w1");
    // Job whose initial publish never happened (e.g. API crashed after commit).
    let id = t.create_import("alice", SAMPLE, 5).await;
    assert_eq!(sweep_once(&c, &NoopPublisher).await.unwrap(), (0, 1));
    // Recently published: not published again.
    assert_eq!(sweep_once(&c, &NoopPublisher).await.unwrap(), (0, 0));

    // A retry scheduled in the future is not dispatched until due.
    let claimed = jobs::claim(&pool, id, "w1", Duration::from_secs(30)).await.unwrap().unwrap();
    jobs::schedule_retry(&pool, &claimed, "boom", Duration::from_secs(3600)).await.unwrap().unwrap();
    assert_eq!(sweep_once(&c, &NoopPublisher).await.unwrap(), (0, 0));
    make_due(&pool, id).await;
    assert_eq!(sweep_once(&c, &NoopPublisher).await.unwrap(), (0, 1));

    // Finished jobs are never dispatched.
    assert_eq!(execute_job(&c, id).await, Outcome::Succeeded);
    sqlx::query("UPDATE jobs SET published_at = NULL").execute(&pool).await.unwrap();
    assert_eq!(sweep_once(&c, &NoopPublisher).await.unwrap(), (0, 0));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn reuploading_the_same_orders_does_not_double_count(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let first = t.create_import("alice", SAMPLE, 5).await;
    let second = t.create_import("alice", SAMPLE, 5).await;
    execute_job(&t.ctx("w1"), first).await;
    execute_job(&t.ctx("w1"), second).await;
    let report_id = t.create_report("alice", &[first, second], 5).await;
    assert_eq!(execute_job(&t.ctx("w1"), report_id).await, Outcome::Succeeded);

    let result = t.report_result(report_id).await;
    assert_eq!(report::to_csv(&result), EXPECTED_SAMPLE_REPORT);
    assert_eq!(result.superseded_duplicate_orders, 11);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn reports_only_see_the_requesting_users_data(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let alice = t.create_import("alice", SAMPLE, 5).await;
    let bob = t.create_import("bob", MANY_ERRORS, 5).await;
    execute_job(&t.ctx("w1"), alice).await;
    execute_job(&t.ctx("w1"), bob).await;
    // Even if a report somehow referenced another user's import, it is filtered out by user.
    let report_id = t.create_report("alice", &[alice, bob], 5).await;
    execute_job(&t.ctx("w1"), report_id).await;
    assert_eq!(t.report_csv(report_id).await, EXPECTED_SAMPLE_REPORT);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn many_invalid_rows_are_all_counted_and_details_capped(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", MANY_ERRORS, 5).await;
    let c = t.ctx_with("w1", |c| c.max_stored_row_errors = 3);
    assert_eq!(execute_job(&c, id).await, Outcome::Succeeded);
    let (valid, invalid): (i32, i32) = sqlx::query_as("SELECT valid_rows, invalid_rows FROM imports WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((valid, invalid), (2, 7));
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM import_row_errors WHERE import_id = $1", id).await, 3);
}

// ---------------------------------------------------------------------------------------------
// Object storage behaviour
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrator = "MIGRATOR")]
async fn storage_outage_is_transient_and_the_job_recovers(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    t.store.set_available(false);
    assert_eq!(execute_job(&t.ctx("w1"), id).await, Outcome::RetryScheduled);
    assert!(job_state(&pool, id).await.last_error.unwrap().contains("simulated outage"));

    t.store.set_available(true);
    make_due(&pool, id).await;
    assert_eq!(execute_job(&t.ctx("w1"), id).await, Outcome::Succeeded);
    assert_eq!(order_count(&pool, id).await, 11);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn missing_upload_object_fails_permanently(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    for key in t.store.keys().await {
        t.store.delete(&key).await.unwrap();
    }
    assert_eq!(execute_job(&t.ctx("w1"), id).await, Outcome::Failed);
    let st = job_state(&pool, id).await;
    assert_eq!(st.attempts, 1, "retrying cannot bring a deleted file back");
    assert!(st.last_error.unwrap().contains("no longer in object storage"));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn report_files_have_deterministic_keys_so_retries_overwrite(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let import = t.create_import("alice", SAMPLE, 5).await;
    execute_job(&t.ctx("w1"), import).await;
    let report_id = t.create_report("alice", &[import], 5).await;

    // Attempt 1 wrote (partial) files and then crashed before committing.
    jobs::claim(&pool, report_id, "w1", Duration::from_secs(30)).await.unwrap().unwrap();
    let csv_key = format!("reports/alice/{report_id}.csv");
    t.store.put(&csv_key, Bytes::from_static(b"partial garbage"), "text/csv").await.unwrap();
    expire_lease(&pool, report_id).await;
    sweep_once(&t.ctx("sweeper"), &NoopPublisher).await.unwrap();

    // Attempt 2 overwrites the same keys; no second copy is created.
    assert_eq!(execute_job(&t.ctx("w2"), report_id).await, Outcome::Succeeded);
    assert_eq!(execute_job(&t.ctx("w3"), report_id).await, Outcome::Skipped);
    assert_eq!(t.report_csv(report_id).await, EXPECTED_SAMPLE_REPORT);

    let report_keys: Vec<String> = t.store.keys().await.into_iter().filter(|k| k.starts_with("reports/")).collect();
    assert_eq!(
        report_keys,
        vec![format!("reports/alice/{report_id}.csv"), format!("reports/alice/{report_id}.json")],
        "exactly one CSV and one JSON per report"
    );
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn report_storage_outage_retries_and_does_not_complete_without_files(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let import = t.create_import("alice", SAMPLE, 5).await;
    execute_job(&t.ctx("w1"), import).await;
    let report_id = t.create_report("alice", &[import], 5).await;
    t.store.set_available(false);
    assert_eq!(execute_job(&t.ctx("w1"), report_id).await, Outcome::RetryScheduled);
    let (status, key): (String, Option<String>) =
        sqlx::query_as("SELECT j.status, r.csv_key FROM jobs j JOIN reports r ON r.id = j.id WHERE j.id = $1")
            .bind(report_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((status.as_str(), key), ("queued", None));
}
