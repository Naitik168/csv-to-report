//! CSV import + asynchronous report generation.
//!
//! Crate layout:
//! * [`csv_import`] – pure CSV parsing / row validation (no I/O).
//! * [`report`]     – report aggregation query and CSV rendering.
//! * [`jobs`]       – job state machine in Postgres: claim, lease, fence, retry, reap, dispatch.
//! * [`storage`]    – object storage (S3-compatible) for uploaded files and report files.
//! * [`admin`]      – explicit storage cleanup used by the `storage-admin` CLI.
//! * [`queue`]      – RabbitMQ publishing / topology.
//! * [`worker`]     – executes claimed jobs (import + report processors, heartbeats, timeouts).
//! * [`api`]        – axum HTTP API + OpenAPI document.

pub mod admin;
pub mod api;
pub mod config;
pub mod csv_import;
pub mod db;
pub mod jobs;
pub mod queue;
pub mod report;
pub mod storage;
pub mod telemetry;
pub mod worker;
