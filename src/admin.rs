//! Explicit storage cleanup, used by the `storage-admin` CLI.
//!
//! Policy: files in object storage are **kept until explicitly removed**. Nothing is deleted
//! automatically. Removing a file never removes database rows: imported orders stay (reports keep
//! working), and the import/report records show when their file was removed (`file_deleted_at`,
//! `files_deleted_at`). Downloads of removed files return `410 Gone`.
//!
//! Safety rules:
//! * Files of jobs that are still `queued`/`running` are refused unless `force` is set: deleting
//!   an upload under a pending import would make that import fail.
//! * Orphan detection ignores objects younger than `min_age`: during an upload the object is
//!   written a moment before its database row is committed.
//! * The object is deleted first, then the row is marked. If the process dies in between, running
//!   the command again finishes the job (S3 deletes are idempotent).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::storage::{report_key, BlobStore, ObjectInfo, StorageError};

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("{kind} {id} not found")]
    NotFound { kind: &'static str, id: Uuid },
    #[error("{kind} {id} is still {status}; its files are needed until it finishes (use --force to delete anyway)")]
    InProgress { kind: &'static str, id: Uuid, status: String },
    #[error("refusing to delete {orphans} of {total} objects as orphans; this usually means the command is pointed at the wrong database. Re-run with --yes if this is really intended")]
    SuspiciousOrphanCount { orphans: usize, total: usize },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Target {
    Uploads,
    Reports,
    All,
}

impl Target {
    fn uploads(self) -> bool {
        matches!(self, Target::Uploads | Target::All)
    }
    fn reports(self) -> bool {
        matches!(self, Target::Reports | Target::All)
    }
}

/// A file (or set of files) that was, or in dry-run mode would be, deleted.
#[derive(Debug, Clone, PartialEq)]
pub struct Removed {
    pub kind: &'static str,
    pub id: Uuid,
    pub keys: Vec<String>,
}

/// One stored object with what the database knows about it.
#[derive(Debug, Clone)]
pub struct StoredFile {
    pub object: ObjectInfo,
    pub kind: &'static str,
    pub id: Option<Uuid>,
    /// Job status, or None when the object is not referenced by any live record (an orphan).
    pub job_status: Option<String>,
}

pub struct StorageAdmin {
    pool: PgPool,
    store: Arc<dyn BlobStore>,
}

const TERMINAL: &str = "('succeeded', 'failed')";

impl StorageAdmin {
    pub fn new(pool: PgPool, store: Arc<dyn BlobStore>) -> Self {
        Self { pool, store }
    }

    /// Delete the uploaded file of one import.
    pub async fn delete_import_file(&self, id: Uuid, force: bool) -> Result<Option<Removed>, AdminError> {
        let row: Option<(String, Option<DateTime<Utc>>, String)> = sqlx::query_as(
            "SELECT i.file_key, i.file_deleted_at, j.status FROM imports i JOIN jobs j ON j.id = i.id WHERE i.id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let (key, deleted_at, status) = row.ok_or(AdminError::NotFound { kind: "import", id })?;
        if deleted_at.is_some() {
            return Ok(None);
        }
        if !force && !is_terminal(&status) {
            return Err(AdminError::InProgress { kind: "import", id, status });
        }
        self.store.delete(&key).await?;
        sqlx::query("UPDATE imports SET file_deleted_at = now() WHERE id = $1 AND file_deleted_at IS NULL")
            .bind(id)
            .execute(&self.pool)
            .await?;
        tracing::info!(import_id = %id, %key, forced = force, "deleted uploaded file");
        Ok(Some(Removed { kind: "import", id, keys: vec![key] }))
    }

    /// Delete the CSV and JSON files of one report.
    pub async fn delete_report_files(&self, id: Uuid, force: bool) -> Result<Option<Removed>, AdminError> {
        let row: Option<(String, Option<DateTime<Utc>>, String)> = sqlx::query_as(
            "SELECT r.user_id, r.files_deleted_at, j.status FROM reports r JOIN jobs j ON j.id = r.id WHERE r.id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let (user_id, deleted_at, status) = row.ok_or(AdminError::NotFound { kind: "report", id })?;
        if deleted_at.is_some() {
            return Ok(None);
        }
        if !force && !is_terminal(&status) {
            return Err(AdminError::InProgress { kind: "report", id, status });
        }
        // Keys are deterministic, so this also removes files left by an attempt that wrote them
        // but never committed (e.g. a report that later failed).
        let keys = vec![report_key(&user_id, id, "csv"), report_key(&user_id, id, "json")];
        for key in &keys {
            self.store.delete(key).await?;
        }
        sqlx::query("UPDATE reports SET files_deleted_at = now() WHERE id = $1 AND files_deleted_at IS NULL")
            .bind(id)
            .execute(&self.pool)
            .await?;
        tracing::info!(report_id = %id, forced = force, "deleted report files");
        Ok(Some(Removed { kind: "report", id, keys }))
    }

    /// Delete files of finished jobs that finished more than `older_than` ago.
    pub async fn purge(&self, older_than: Duration, target: Target, dry_run: bool) -> Result<Vec<Removed>, AdminError> {
        let secs = older_than.as_secs_f64();
        let mut removed = Vec::new();
        if target.uploads() {
            let ids: Vec<(Uuid, String)> = sqlx::query_as(&format!(
                "SELECT i.id, i.file_key FROM imports i JOIN jobs j ON j.id = i.id
                  WHERE i.file_deleted_at IS NULL AND j.status IN {TERMINAL}
                    AND j.finished_at < now() - make_interval(secs => $1)
                  ORDER BY j.finished_at"
            ))
            .bind(secs)
            .fetch_all(&self.pool)
            .await?;
            for (id, key) in ids {
                if dry_run {
                    removed.push(Removed { kind: "import", id, keys: vec![key] });
                } else if let Some(r) = self.delete_import_file(id, false).await? {
                    removed.push(r);
                }
            }
        }
        if target.reports() {
            let ids: Vec<(Uuid, String)> = sqlx::query_as(&format!(
                "SELECT r.id, r.user_id FROM reports r JOIN jobs j ON j.id = r.id
                  WHERE r.files_deleted_at IS NULL AND j.status IN {TERMINAL}
                    AND j.finished_at < now() - make_interval(secs => $1)
                  ORDER BY j.finished_at"
            ))
            .bind(secs)
            .fetch_all(&self.pool)
            .await?;
            for (id, user_id) in ids {
                if dry_run {
                    let keys = vec![report_key(&user_id, id, "csv"), report_key(&user_id, id, "json")];
                    removed.push(Removed { kind: "report", id, keys });
                } else if let Some(r) = self.delete_report_files(id, false).await? {
                    removed.push(r);
                }
            }
        }
        Ok(removed)
    }

    /// List stored objects together with the status of the job that owns them.
    pub async fn list(&self, target: Target) -> Result<Vec<StoredFile>, AdminError> {
        let mut out = Vec::new();
        for (prefix, kind) in [("uploads/", "import"), ("reports/", "report")] {
            if (kind == "import" && !target.uploads()) || (kind == "report" && !target.reports()) {
                continue;
            }
            let objects = self.store.list(prefix).await?;
            let ids: Vec<Uuid> = objects.iter().filter_map(|o| parse_key(&o.key).map(|(_, id)| id)).collect();
            // One query per prefix: (id, owner user_id, job status) of records whose files are live.
            let sql = if kind == "import" {
                "SELECT i.id, i.user_id, j.status FROM imports i JOIN jobs j ON j.id = i.id
                  WHERE i.id = ANY($1) AND i.file_deleted_at IS NULL"
            } else {
                "SELECT r.id, r.user_id, j.status FROM reports r JOIN jobs j ON j.id = r.id
                  WHERE r.id = ANY($1) AND r.files_deleted_at IS NULL"
            };
            let owners: Vec<(Uuid, String, String)> = sqlx::query_as(sql).bind(&ids).fetch_all(&self.pool).await?;
            let owners: HashMap<Uuid, (String, String)> =
                owners.into_iter().map(|(id, user, status)| (id, (user, status))).collect();
            for object in objects {
                let parsed = parse_key(&object.key).map(|(user, id)| (user.to_string(), id));
                // The object belongs to a record only if both the id and the user in the key match.
                let job_status = parsed.as_ref().and_then(|(user, id)| match owners.get(id) {
                    Some((owner, status)) if owner == user => Some(status.clone()),
                    _ => None,
                });
                out.push(StoredFile { object, kind, id: parsed.map(|(_, id)| id), job_status });
            }
        }
        Ok(out)
    }

    /// Objects no live record points to: left by a crash between storing an upload and
    /// committing its job, or by a deletion interrupted after marking. Objects younger than
    /// `min_age` are skipped because an upload writes its object just before committing its row.
    pub async fn orphans(&self, min_age: Duration) -> Result<Vec<StoredFile>, AdminError> {
        Ok(self.orphans_with_total(min_age).await?.0)
    }

    async fn orphans_with_total(&self, min_age: Duration) -> Result<(Vec<StoredFile>, usize), AdminError> {
        let cutoff = Utc::now() - chrono::Duration::from_std(min_age).unwrap_or_default();
        let all = self.list(Target::All).await?;
        let total = all.len();
        let orphans = all
            .into_iter()
            .filter(|f| f.job_status.is_none())
            .filter(|f| f.object.last_modified.map(|t| t < cutoff).unwrap_or(true))
            .collect();
        Ok((orphans, total))
    }

    /// Delete orphaned objects. As a guard against pointing the tool at the wrong (e.g. empty
    /// or restored) database, it refuses when more than half of all objects look orphaned,
    /// unless `allow_mass_delete` is set.
    pub async fn delete_orphans(
        &self,
        min_age: Duration,
        dry_run: bool,
        allow_mass_delete: bool,
    ) -> Result<Vec<StoredFile>, AdminError> {
        let (orphans, total) = self.orphans_with_total(min_age).await?;
        if !dry_run && !allow_mass_delete && orphans.len() * 2 > total && orphans.len() > 1 {
            return Err(AdminError::SuspiciousOrphanCount { orphans: orphans.len(), total });
        }
        if !dry_run {
            for f in &orphans {
                self.store.delete(&f.object.key).await?;
                tracing::info!(key = %f.object.key, "deleted orphaned object");
            }
        }
        Ok(orphans)
    }
}

fn is_terminal(status: &str) -> bool {
    status == "succeeded" || status == "failed"
}

/// `uploads/alice/<uuid>.csv` -> `("alice", <uuid>)`
fn parse_key(key: &str) -> Option<(&str, Uuid)> {
    let mut parts = key.split('/');
    let _prefix = parts.next()?;
    let user = parts.next()?;
    let file = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let stem = file.split('.').next()?;
    Uuid::parse_str(stem).ok().map(|id| (user, id))
}

/// Parse durations like `90s`, `15m`, `24h`, `7d` (a bare number means seconds).
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = num.parse().map_err(|_| format!("invalid duration {s:?}; use e.g. 30s, 15m, 24h, 7d"))?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        _ => return Err(format!("invalid duration unit in {s:?}; use s, m, h or d")),
    };
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604_800));
        assert!(parse_duration("7w").is_err());
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn owner_and_id_are_extracted_from_keys() {
        let id = Uuid::new_v4();
        assert_eq!(parse_key(&format!("uploads/alice/{id}.csv")), Some(("alice", id)));
        assert_eq!(parse_key(&format!("reports/bob/{id}.json")), Some(("bob", id)));
        assert_eq!(parse_key("uploads/alice/notes.txt"), None);
        assert_eq!(parse_key(&format!("uploads/a/b/{id}.csv")), None);
    }
}
