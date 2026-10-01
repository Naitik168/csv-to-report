//! `storage-admin`: explicit removal of files from object storage.
//!
//! Files are never deleted automatically; this command is the only way they are removed.
//!
//! ```text
//! storage-admin list    [--target uploads|reports|all]
//! storage-admin delete-import <IMPORT_ID> [--force]
//! storage-admin delete-report <REPORT_ID> [--force]
//! storage-admin purge   --older-than 7d [--target uploads|reports|all] [--dry-run]
//! storage-admin orphans [--min-age 1h] [--delete]
//! ```
//!
//! In Docker Compose: `docker compose run --rm api storage-admin <command>`.

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use csv_reports::{
    admin::{parse_duration, StorageAdmin, Target},
    db,
    storage::{connect_s3_with_retry, S3Config},
};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "storage-admin", about = "Explicitly remove uploaded CSV files and report files from object storage")]
struct Cli {
    /// No default on purpose: deletions must never run against an unintended database.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List stored files with the status of the import/report that owns them.
    List {
        #[arg(long, value_enum, default_value = "all")]
        target: Target,
    },
    /// Delete the uploaded file of one import (imported rows are kept).
    DeleteImport {
        id: Uuid,
        /// Also delete when the import has not finished yet (it will then fail).
        #[arg(long)]
        force: bool,
    },
    /// Delete the CSV and JSON files of one report.
    DeleteReport {
        id: Uuid,
        #[arg(long)]
        force: bool,
    },
    /// Delete files of finished imports/reports that finished longer ago than --older-than.
    Purge {
        /// e.g. 30m, 24h, 7d
        #[arg(long, value_parser = parse_duration)]
        older_than: Duration,
        #[arg(long, value_enum, default_value = "all")]
        target: Target,
        /// Show what would be deleted without deleting anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Find objects no import/report refers to (left behind by crashes). Add --delete to remove them.
    Orphans {
        /// Ignore objects younger than this (uploads write the object just before the DB row).
        #[arg(long, value_parser = parse_duration, default_value = "1h")]
        min_age: Duration,
        #[arg(long)]
        delete: bool,
        /// Allow deleting when more than half of all objects look orphaned (normally a sign of
        /// pointing at the wrong database).
        #[arg(long)]
        yes: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Human-readable output goes to stdout; diagnostics go to stderr (never JSON mixed into the table).
    csv_reports::telemetry::init_cli();
    let cli = Cli::parse();

    let pool = db::connect(&cli.database_url, 2).await?;
    let store = Arc::new(connect_s3_with_retry(&S3Config::from_env(), false).await?);
    let admin = StorageAdmin::new(pool, store);

    match cli.command {
        Command::List { target } => {
            let files = admin.list(target).await?;
            println!("{:<8} {:>10}  {:<20}  {:<10}  KEY", "KIND", "BYTES", "MODIFIED", "STATUS");
            for f in &files {
                println!(
                    "{:<8} {:>10}  {:<20}  {:<10}  {}",
                    f.kind,
                    f.object.size,
                    f.object.last_modified.map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string()).unwrap_or_default(),
                    f.job_status.as_deref().unwrap_or("ORPHAN"),
                    f.object.key
                );
            }
            println!("{} file(s)", files.len());
        }
        Command::DeleteImport { id, force } => match admin.delete_import_file(id, force).await? {
            Some(r) => println!("deleted {}", r.keys.join(", ")),
            None => println!("import {id}: file was already deleted"),
        },
        Command::DeleteReport { id, force } => match admin.delete_report_files(id, force).await? {
            Some(r) => println!("deleted {}", r.keys.join(", ")),
            None => println!("report {id}: files were already deleted"),
        },
        Command::Purge { older_than, target, dry_run } => {
            let removed = admin.purge(older_than, target, dry_run).await?;
            let verb = if dry_run { "would delete" } else { "deleted" };
            for r in &removed {
                println!("{verb} {} {}: {}", r.kind, r.id, r.keys.join(", "));
            }
            println!("{verb} files of {} import(s)/report(s)", removed.len());
        }
        Command::Orphans { min_age, delete, yes } => {
            let orphans = admin.delete_orphans(min_age, !delete, yes).await?;
            let verb = if delete { "deleted" } else { "orphan" };
            for f in &orphans {
                println!("{verb}: {} ({} bytes)", f.object.key, f.object.size);
            }
            println!(
                "{} orphaned object(s){}",
                orphans.len(),
                if delete { " deleted" } else { "; re-run with --delete to remove" }
            );
        }
    }
    Ok(())
}
