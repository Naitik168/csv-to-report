//! Tests for explicit storage cleanup (`storage-admin`): files are kept until removed on purpose,
//! and removal is refused while a job still needs its file.

mod common;

use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use common::*;
use csv_reports::{
    admin::{AdminError, StorageAdmin, Target},
    db::MIGRATOR,
    storage::BlobStore,
    worker::{execute_job, Outcome},
};
use sqlx::PgPool;
use uuid::Uuid;

fn admin(t: &TestEnv) -> StorageAdmin {
    StorageAdmin::new(t.pool.clone(), t.store.clone())
}

async fn backdate_finish(pool: &PgPool, id: Uuid, hours: i64) {
    sqlx::query("UPDATE jobs SET finished_at = now() - make_interval(hours => $2) WHERE id = $1")
        .bind(id)
        .bind(hours as i32)
        .execute(pool)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn files_are_kept_until_explicitly_deleted(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    assert_eq!(execute_job(&t.ctx("w1"), id).await, Outcome::Succeeded);
    assert_eq!(t.store.keys().await.len(), 1, "processing does not delete the upload");

    let removed = admin(&t).delete_import_file(id, false).await.unwrap().unwrap();
    assert_eq!(removed.keys, vec![format!("uploads/alice/{id}.csv")]);
    assert!(t.store.keys().await.is_empty());
    assert_eq!(order_count(&pool, id).await, 11, "imported rows are kept");
    // Second delete is a no-op.
    assert!(admin(&t).delete_import_file(id, false).await.unwrap().is_none());
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn deleting_a_pending_jobs_file_is_refused_unless_forced(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    let err = admin(&t).delete_import_file(id, false).await.unwrap_err();
    assert!(matches!(err, AdminError::InProgress { .. }), "{err}");
    assert_eq!(t.store.keys().await.len(), 1);

    // --force deletes it; the import then fails permanently with a clear reason.
    admin(&t).delete_import_file(id, true).await.unwrap().unwrap();
    assert_eq!(execute_job(&t.ctx("w1"), id).await, Outcome::Failed);

    let err = admin(&t).delete_import_file(Uuid::new_v4(), false).await.unwrap_err();
    assert!(matches!(err, AdminError::NotFound { .. }));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn purge_only_touches_finished_jobs_older_than_the_cutoff(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let old_import = t.create_import("alice", SAMPLE, 5).await;
    let new_import = t.create_import("alice", SAMPLE, 5).await;
    let pending_import = t.create_import("alice", SAMPLE, 5).await;
    execute_job(&t.ctx("w1"), old_import).await;
    execute_job(&t.ctx("w1"), new_import).await;
    let old_report = t.create_report("alice", &[old_import], 5).await;
    execute_job(&t.ctx("w1"), old_report).await;
    backdate_finish(&pool, old_import, 48).await;
    backdate_finish(&pool, old_report, 48).await;

    // Dry run deletes nothing.
    let planned = admin(&t).purge(Duration::from_secs(24 * 3600), Target::All, true).await.unwrap();
    assert_eq!(planned.len(), 2);
    assert_eq!(t.store.keys().await.len(), 5);

    // Only uploads.
    let removed = admin(&t).purge(Duration::from_secs(24 * 3600), Target::Uploads, false).await.unwrap();
    assert_eq!(removed.iter().map(|r| r.id).collect::<Vec<_>>(), vec![old_import]);

    // Then everything that qualifies: only the old report remains to purge.
    let removed = admin(&t).purge(Duration::from_secs(24 * 3600), Target::All, false).await.unwrap();
    assert_eq!(removed.iter().map(|r| r.id).collect::<Vec<_>>(), vec![old_report]);

    let mut keys = t.store.keys().await;
    keys.sort();
    let mut expected = vec![format!("uploads/alice/{new_import}.csv"), format!("uploads/alice/{pending_import}.csv")];
    expected.sort();
    assert_eq!(keys, expected, "recent and pending files are untouched");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn orphans_are_found_and_recent_uploads_are_protected(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let live = t.create_import("alice", SAMPLE, 5).await;
    // Left behind by a crash between storing an upload and committing its job:
    let orphan_key = format!("uploads/alice/{}.csv", Uuid::new_v4());
    t.store.put(&orphan_key, Bytes::from_static(b"x"), "text/csv").await.unwrap();
    // An upload in flight right now (object written, DB row about to be committed):
    let fresh_key = format!("uploads/bob/{}.csv", Uuid::new_v4());
    t.store.put(&fresh_key, Bytes::from_static(b"y"), "text/csv").await.unwrap();
    t.store.set_last_modified(&orphan_key, Utc::now() - chrono::Duration::hours(3)).await;

    let orphans = admin(&t).orphans(Duration::from_secs(3600)).await.unwrap();
    assert_eq!(orphans.iter().map(|o| o.object.key.clone()).collect::<Vec<_>>(), vec![orphan_key.clone()]);

    admin(&t).delete_orphans(Duration::from_secs(3600), false, false).await.unwrap();
    let keys = t.store.keys().await;
    assert!(!keys.contains(&orphan_key));
    assert!(keys.contains(&fresh_key));
    assert!(keys.contains(&format!("uploads/alice/{live}.csv")));

    let listed = admin(&t).list(Target::All).await.unwrap();
    assert_eq!(listed.iter().filter(|f| f.job_status.as_deref() == Some("queued")).count(), 1);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn an_object_under_the_wrong_user_prefix_is_not_owned_by_the_record(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let id = t.create_import("alice", SAMPLE, 5).await;
    // Same id, but under another user's prefix: not the file the record points to.
    let foreign = format!("uploads/mallory/{id}.csv");
    t.store.put(&foreign, Bytes::from_static(b"x"), "text/csv").await.unwrap();
    t.store.set_last_modified(&foreign, Utc::now() - chrono::Duration::hours(3)).await;
    let orphans = admin(&t).orphans(Duration::from_secs(3600)).await.unwrap();
    assert_eq!(orphans.iter().map(|o| o.object.key.as_str()).collect::<Vec<_>>(), vec![foreign.as_str()]);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn mass_orphan_deletion_is_refused_without_confirmation(pool: PgPool) {
    // Simulates pointing the tool at the wrong (empty) database: every object looks orphaned.
    let t = TestEnv::new(&pool);
    for _ in 0..3 {
        let key = format!("uploads/alice/{}.csv", Uuid::new_v4());
        t.store.put(&key, Bytes::from_static(b"x"), "text/csv").await.unwrap();
        t.store.set_last_modified(&key, Utc::now() - chrono::Duration::hours(3)).await;
    }
    let err = admin(&t).delete_orphans(Duration::from_secs(3600), false, false).await.unwrap_err();
    assert!(matches!(err, AdminError::SuspiciousOrphanCount { orphans: 3, total: 3 }), "{err}");
    assert_eq!(t.store.keys().await.len(), 3, "nothing deleted");
    // Dry run is always allowed; explicit confirmation deletes.
    assert_eq!(admin(&t).delete_orphans(Duration::from_secs(3600), true, false).await.unwrap().len(), 3);
    admin(&t).delete_orphans(Duration::from_secs(3600), false, true).await.unwrap();
    assert!(t.store.keys().await.is_empty());
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn forced_report_deletion_before_completion_does_not_hide_the_final_files(pool: PgPool) {
    let t = TestEnv::new(&pool);
    let import = t.create_import("alice", SAMPLE, 5).await;
    execute_job(&t.ctx("w1"), import).await;
    let report_id = t.create_report("alice", &[import], 5).await;
    admin(&t).delete_report_files(report_id, true).await.unwrap().unwrap();
    // The worker then generates the report: its files exist and the record says so.
    assert_eq!(execute_job(&t.ctx("w1"), report_id).await, Outcome::Succeeded);
    let gone: bool = sqlx::query_scalar("SELECT files_deleted_at IS NOT NULL FROM reports WHERE id = $1")
        .bind(report_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!gone);
    assert_eq!(t.report_csv(report_id).await, EXPECTED_SAMPLE_REPORT);
}
