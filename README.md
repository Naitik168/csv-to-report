# CSV Import & Asynchronous Report Service (Rust)

A small backend where multiple users upload order CSV files and request reports on the imported data. Both the import and the report run **asynchronously** in background workers. The system is designed to stay correct when jobs fail, workers crash, messages are duplicated, or the whole system restarts mid-job.

**Stack:** Rust (axum, sqlx, lapin, aws-sdk-s3, tokio) · PostgreSQL 16 · RabbitMQ 4 (quorum queues) · SeaweedFS (S3-compatible object storage) · Docker Compose · OpenAPI/Swagger UI

---

## Contents

1. [Quick start](#1-quick-start)
2. [Using the API](#2-using-the-api)
3. [Architecture](#3-architecture)
4. [Design decisions](#4-design-decisions)
5. [Failure handling](#5-failure-handling)
6. [Observability](#6-observability)
7. [Tests](#7-tests)
8. [Configuration](#8-configuration)
9. [Project layout](#9-project-layout)
10. [Trade-offs & Known Limitations](#10-trade-offs--known-limitations)
11. [Requirements checklist](#11-requirements-checklist)

---

## 1. Quick start

Prerequisite: Docker with Compose v2.

```bash
docker compose up --build
```

The first build compiles Rust and takes a few minutes; later builds are cached. When it's up:

| What                     | URL                                                                                                 |
| ------------------------ | --------------------------------------------------------------------------------------------------- |
| Swagger UI (try the API) | http://localhost:8080/swagger-ui                                                                    |
| OpenAPI document         | http://localhost:8080/api-docs/openapi.json                                                         |
| RabbitMQ management UI   | http://localhost:15672 (user `csv`, password `csv`)                                                 |
| Postgres                 | `localhost:55432` (user `postgres`, password `postgres`, db `csvreports`)                           |
| S3 API (SeaweedFS)       | `http://localhost:8333` (access key `csvreports`, secret `csvreports-secret`, bucket `csv-reports`) |

Compose starts `postgres`, `rabbitmq`, `seaweedfs` (object storage), `api`, and **two** `worker` replicas, which demonstrate safe concurrent processing. Migrations run and the bucket is created automatically on startup.

Run the end-to-end demo script (needs `curl` and `jq`):

```bash
./scripts/demo.sh
```

Run the test suite inside Docker:

```bash
docker compose --profile test run --rm tests
```

Stop everything with `docker compose down`, or `docker compose down -v` to also delete data.

If a port is taken on your machine, override the host ports: `API_HOST_PORT=18080 POSTGRES_HOST_PORT=15432 RABBITMQ_UI_HOST_PORT=25672 S3_HOST_PORT=18333 docker compose up --build`. RabbitMQ's AMQP port is deliberately not published, because only containers use it.

---

## 2. Using the API

Every request identifies the caller with an **`X-User-Id`** header (demo authentication, see [Trade-offs](#10-trade-offs--known-limitations)). Users only ever see their own imports and reports; other users' ids return `404`. In Swagger UI, click **Authorize** and enter a user id such as `alice`.

### Endpoints

| Method | Path                                      | Purpose                                                                                       |
| ------ | ----------------------------------------- | --------------------------------------------------------------------------------------------- |
| `POST` | `/imports`                                | Upload a CSV (multipart field `file`). Returns **202** with `import_id`.                      |
| `GET`  | `/imports`                                | List your imports (`?limit=&offset=`).                                                        |
| `GET`  | `/imports/{id}`                           | Import status, row counts, attempts, last error, attempt history.                             |
| `GET`  | `/imports/{id}/errors`                    | Invalid rows: line number, raw row, and every validation error.                               |
| `GET`  | `/imports/{id}/file`                      | Download the originally uploaded file from object storage (`410` if it was removed).          |
| `POST` | `/reports`                                | Request a report. Body `{"import_ids": [...]}` is optional. Returns **202** with `report_id`. |
| `GET`  | `/reports`                                | List your reports.                                                                            |
| `GET`  | `/reports/{id}`                           | Report status; includes a `summary` (totals and counts) once completed.                       |
| `GET`  | `/reports/{id}/download?format=csv\|json` | Download the full report file from object storage (`409` if not ready, `410` if removed).     |
| `GET`  | `/health`, `/ready`                       | Liveness; readiness (database + broker).                                                      |

Both `POST` endpoints accept an optional **`Idempotency-Key`** header. Retrying a request with the same key returns the original job (response header `Idempotent-Replayed: true`) instead of creating a duplicate. Reusing an upload key with a _different_ file returns `409`.

### Walkthrough with curl

```bash
# 1) Upload: returns immediately
curl -s -X POST localhost:8080/imports -H 'X-User-Id: alice' -F file=@samples/orders_sample.csv
# {"import_id":"9b0e…","status":"queued","links":{"self":"/imports/9b0e…","errors":"/imports/9b0e…/errors"}}

# 2) Track progress: queued -> processing -> completed | completed_with_errors | failed
curl -s localhost:8080/imports/9b0e… -H 'X-User-Id: alice'
# {"status":"completed_with_errors","total_rows":12,"valid_rows":11,"invalid_rows":1,"attempts":1, …}

# 3) See why rows were rejected
curl -s localhost:8080/imports/9b0e…/errors -H 'X-User-Id: alice'
# {"items":[{"line_number":12,"raw_record":"U006,O1011,Keyboard,1,abc,completed",
#            "errors":[{"field":"unit_price","message":"must be a decimal number, got \"abc\""}]}], …}

# 4) Request a report (empty body = all your completed imports): returns immediately
curl -s -X POST localhost:8080/reports -H 'X-User-Id: alice'
# {"report_id":"4c1d…","status":"queued","import_ids":["9b0e…"], …}

# 5) Poll, then download
curl -s localhost:8080/reports/4c1d… -H 'X-User-Id: alice'
curl -s "localhost:8080/reports/4c1d…/download?format=csv" -H 'X-User-Id: alice'
```

Report output for the sample file:

```csv
user_id,total_orders,total_quantity,total_amount
U001,2,5,1575.00
U002,2,3,680.00
U003,2,6,380.00
U004,2,3,850.00
U005,2,7,90.00
U006,1,1,250.00
```

> **Note on the example in the assignment brief.** The brief's example report lists U003 as `560.00`. Recomputing from the sample data gives 4 × 50.00 + 2 × 90.00 = **380.00**, which is what this system produces. The brief's example also omits U006. U006's valid order O1012 (250.00) is included here, and only the invalid O1011 row is excluded, as the brief specifies.

More API material:

- A Postman collection is in `postman/csv-reports.postman_collection.json`. It chains the ids automatically.
- A static copy of the OpenAPI spec is in `docs/openapi.json`.
- Extra sample files are in `samples/`, including one with many kinds of invalid rows and one with a bad header.

### Status values

| Import status           | Meaning                                                                       |
| ----------------------- | ----------------------------------------------------------------------------- |
| `queued`                | Waiting for a worker.                                                         |
| `processing`            | A worker holds the job.                                                       |
| `retrying`              | An attempt failed transiently; `next_attempt_at` says when the next one runs. |
| `completed`             | All rows imported.                                                            |
| `completed_with_errors` | Valid rows imported; invalid rows skipped and listed at `/errors`.            |
| `failed`                | The file is unusable, or retries were exhausted. `last_error` explains why.   |

Reports use the same states, without `completed_with_errors`. Every status response also includes `attempts`, `max_attempts`, `last_error`, and `attempt_history` (which worker ran each attempt, how long it took, and how it ended).

### Managing stored files (`storage-admin`)

Uploaded CSVs and generated report files are **kept in object storage until you remove them explicitly** with the `storage-admin` command. Nothing is deleted automatically. Removing a file never removes data from Postgres:

- Imported rows stay, so new reports keep working.
- Report summaries stay.
- The file's download endpoint returns `410 Gone`, and the status shows `file_available: false` or `files_available: false`, plus when the file was removed.

```bash
# Inside Docker Compose (the api image contains the CLI)
docker compose run --rm api storage-admin list                       # every stored file + status of its job
docker compose run --rm api storage-admin delete-import <import_id>  # one import's uploaded CSV
docker compose run --rm api storage-admin delete-report <report_id>  # one report's CSV + JSON
docker compose run --rm api storage-admin purge --older-than 7d --dry-run   # preview
docker compose run --rm api storage-admin purge --older-than 7d [--target uploads|reports|all]
docker compose run --rm api storage-admin orphans [--delete]         # objects no record points to
```

Safety rules:

- Files belonging to a job that is still `queued` or `running` are refused, because the job still needs them. `--force` overrides this, and the import then fails with a clear "file was deleted" error.
- `purge` only touches finished jobs.
- `orphans` ignores objects younger than one hour (`--min-age`), because an upload writes its object a moment before its database row is committed. An object only counts as owned if both the id _and_ the user in its key match the record.
- `orphans --delete` refuses when more than half of all objects look orphaned (usually a sign of the wrong database) unless `--yes` is given. The CLI has no default `DATABASE_URL`, so it never runs against an unintended database; Compose provides it.

---

## 3. Architecture

```mermaid
flowchart LR
    C[Client] -->|HTTP| API[api<br/>axum]
    API -->|1 - put uploaded file| S3[(SeaweedFS<br/>S3 object storage)]
    API -->|2 - insert job + file key, single tx| PG[(PostgreSQL<br/>source of truth)]
    API -->|3 - publish job_id, best effort| MQ[[RabbitMQ<br/>csv.imports / csv.reports]]
    MQ -->|deliver| W1[worker #1]
    MQ -->|deliver| W2[worker #2]
    W1 & W2 -->|claim / heartbeat / fenced commit| PG
    W1 & W2 -->|read uploads / write report files| S3
    W1 & W2 -. sweeper: reap expired leases,<br/>re-dispatch due jobs .-> MQ
```

- **`api`** accepts requests. For an upload it first writes the file to object storage. Then, in one transaction, it stores the job and its payload (the file's key, or the resolved report scope), commits, and publishes a small message `{job_id, kind}`. It never waits for processing.
- **SeaweedFS** (S3-compatible object storage) holds file contents: uploaded CSVs and the generated report files (CSV and JSON). Postgres stores only their keys.
- **RabbitMQ** carries only notifications that a job is ready. Two durable quorum queues are used, messages are persistent, publishing uses publisher confirms, and consumers use manual acks with bounded prefetch.
- **`worker`** consumes both queues. For each message it runs: **claim** the job in Postgres (atomic, with a lease) → **process** with heartbeat and timeout (download the upload from S3, or build the report and write its files to S3) → **commit the result and `succeeded` state in one fenced transaction** → **ack**.
- **Sweeper** runs inside every worker every 2 seconds. It returns jobs with expired leases to the queue, and it publishes queued jobs that are due but have no message in flight.

**PostgreSQL is the single source of truth. RabbitMQ is a delivery mechanism, not a state store.** A lost message, duplicate delivery, or broker restart can delay a job but can never corrupt or double-process it. A full walkthrough of every failure scenario is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

### Data model

| Table               | Purpose                                                                                                                                               |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `users`             | Callers (auto-created on first write).                                                                                                                |
| `jobs`              | State machine shared by imports and reports: status, attempts, backoff time, lease (token, owner, expiry), last error, idempotency key.               |
| `job_attempts`      | One row per attempt: worker, start and finish time, outcome, error.                                                                                   |
| `imports`           | Upload metadata, the object-storage `file_key`, `file_deleted_at`, and row counters.                                                                  |
| `orders`            | Valid imported rows. PK `(import_id, line_number)`.                                                                                                   |
| `import_row_errors` | Rejected rows: raw text and a JSON list of field errors.                                                                                              |
| `reports`           | The resolved `import_ids`, a small `summary` (`jsonb`: totals and counts), the object-storage keys of the CSV and JSON files, and `files_deleted_at`. |

The schema is defined by two migrations, applied automatically on startup:

- [`20260930000001_init.sql`](migrations/20260930000001_init.sql) creates the job model.
- [`20261001000001_object_storage.sql`](migrations/20261001000001_object_storage.sql) moves file contents to object storage. It is written as a real upgrade: existing databases keep all imported rows and report totals, old files are marked unavailable (downloads return `410`), and old imports that never ran are failed with a message asking for a re-upload.

### Job state machine

```
queued ──claim──▶ running ──success (fenced tx)──▶ succeeded
  ▲                 │  │
  │ backoff         │  └─ permanent error, or attempts exhausted ──▶ failed
  ├─────────────────┘ transient error with attempts left
  └──── lease expired (crash/restart/stall) ── reaper ─ (or failed if attempts exhausted)
```

---

## 4. Design decisions

**Why RabbitMQ.**

- It's a real broker with per-message acknowledgements. Unacked messages are redelivered automatically when a consumer dies.
- Prefetch gives natural backpressure and per-worker concurrency limits.
- Quorum queues are replicated and crash-safe, and the management UI makes queue depth visible.
- _Alternatives considered:_
  - Kafka is log-oriented and built for streaming; it's overkill for a work queue, and per-message retry is awkward.
  - Redis Streams have weaker durability guarantees.
  - A Postgres-only queue (`SKIP LOCKED`) would work, but the brief asks for a message queue.

**Object storage for files, and why SeaweedFS.**

- File contents (uploads and report files) live in S3-compatible object storage; Postgres holds only keys and metadata. This keeps the database small, keeps large blobs out of transactions and backups, and matches how this would run in production. The code talks plain S3 (the official `aws-sdk-s3`), so switching to AWS S3 is a configuration change: leave `S3_ENDPOINT` and the static keys unset, and the standard AWS credential chain (including IAM roles) is used.
- _Why SeaweedFS:_ it's mature, Apache-2.0 licensed, and runs as one container (`weed mini`) with credentials and the bucket configured by environment and flags.
- _Alternatives considered:_
  - MinIO stopped publishing its community Docker images, so a `docker compose up` that depends on it no longer works out of the box.
  - LocalStack requires an account and auth token since March 2026.
  - RustFS is a promising Rust-based option, but it reached 1.0 only on September 28, 2026.
- **Ordering and consistency:**
  - The API writes the object _before_ committing the job, so a worker never sees a job without its file. If storage is down, the upload fails with `503` and nothing is created.
  - If the commit fails, the API deletes the object, but only after confirming the import row really doesn't exist (a failed `COMMIT` may still have landed). A crash between the two steps leaves an orphan that `storage-admin orphans` finds.
  - Keys are deterministic (`uploads/{user}/{import_id}.csv`, `reports/{user}/{report_id}.csv|json`), so retries and stale workers overwrite the same objects instead of creating new ones.
  - A report is only marked `completed` after both of its files have been written.
- **Retention:** files are kept until they are explicitly removed with `storage-admin` (see [Managing stored files](#managing-stored-files-storage-admin)).

**How job state is represented.**

- There is one `jobs` table with four internal states: `queued`, `running`, `succeeded`, `failed`.
- Users see friendlier derived states, such as `retrying` (queued with attempts > 0) and `completed_with_errors`.
- Imports and reports share their primary key with `jobs`, so claiming, retrying, and recovery code is written once.

**How a worker safely claims a job.**

- A claim is a single conditional statement: `UPDATE jobs SET status='running', attempts=attempts+1, lease_token=<new uuid>, lease_expires_at=now()+30s WHERE id=$1 AND status='queued' AND available_at<=now()`.
- Postgres row locking guarantees exactly one winner, even when the same message reaches several workers.
- Losers get zero rows back and just ack the message.

**How duplicate processing is prevented.** There are four layers:

1. The claim is atomic, so only one worker at a time owns a job.
2. The fencing token means a worker that lost its lease cannot commit, because every write re-checks `lease_token` under `SELECT … FOR UPDATE`.
3. Result rows and the `succeeded` state commit in the same transaction, so a finished job is never re-run. Primary keys on `(import_id, line_number)` guard against duplicates as well.
4. `Idempotency-Key` headers prevent duplicate jobs from client retries.

**Where transactions begin and end.**

- API: _insert job + payload_ is one transaction. Publishing to RabbitMQ happens _after_ commit, so a worker can never receive an id that isn't committed yet.
- Worker, claim: one short transaction (claim plus an attempt row).
- Worker, processing: parsing or aggregation runs outside any transaction. Then one transaction does: fence (lock the job row and verify the lease) → write rows, errors, and counters (or the report result) → mark `succeeded` → commit.
- Holding transactions only for the write phase keeps them short. The atomic commit means a crash leaves either nothing or the full result, never half an import, so retries simply start over.

**How invalid CSV rows affect an import.**

- File-level problems fail the import permanently, with no retry, because retrying can't fix the file. These are: an empty file, missing required columns, duplicate columns, or broken CSV syntax.
- Row-level problems don't fail the import. The row is skipped, _every_ error in the row is recorded (not just the first), and the import finishes as `completed_with_errors` with counts.
- Validation rules:
  - All fields are required.
  - `quantity` must be an integer > 0.
  - `unit_price` must be a decimal ≥ 0 with at most 2 decimal places.
  - `status` must be one of `completed|pending|cancelled|refunded`.
  - `order_id` must be unique within the file.
  - The row must have the right number of fields, and text must be valid UTF-8.
- Header matching is case-insensitive and order-independent. Extra columns are ignored and a UTF-8 BOM is accepted.
- All invalid rows are counted. Details are stored for the first 1,000 per import, a configurable cap that stops a garbage upload from flooding the database.

**Report semantics.**

- One row per customer (the CSV `user_id` column), with `total_orders`, `total_quantity`, and `total_amount`.
- Only `completed` orders are counted.
- The report's scope, meaning which imports it covers, is fixed when it is requested. It's either the ids you send or all your completed imports. This makes retries deterministic.
- If the same `order_id` appears in several covered imports (for example, the same file uploaded twice), only the latest import's row counts, so re-uploads don't double-count.
- The result includes the number of excluded invalid rows and superseded duplicates for transparency.
- Money is `NUMERIC(12,2)` in Postgres and `rust_decimal` in Rust, and is serialised as strings. Floats are never used.

**How retries work.**

| Error kind | Examples                                                                                                                                        | Behaviour                                                                                                                                                                                              |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Transient  | DB error, object-storage error, timeout, broker hiccup, injected chaos                                                                          | Retry with exponential backoff and jitter: `2s × 2^(n-1)`, capped at 5 min, scaled by a random 0.5–1.0. After `JOB_MAX_ATTEMPTS` (default 5) the job becomes `failed` with "gave up after N attempts". |
| Permanent  | Bad header, empty file, missing record, uploaded file missing from object storage, data the DB rejects deterministically (SQLSTATE class 22/23) | `failed` immediately with no retry.                                                                                                                                                                    |
| Lease lost | Another worker owns the job now                                                                                                                 | Discard the work silently; don't touch state.                                                                                                                                                          |

- Retries are scheduled **in Postgres** (`available_at`) and dispatched by the sweeper. No broker plugins or TTL/dead-letter chains are needed, and pending retries survive a broker restart.
- Attempts are counted at _claim_ time. A job that crashes its worker every time (a poison pill) still ends up `failed` instead of looping forever.

---

## 5. Failure handling

| Scenario from the brief                              | What happens                                                                                                                                                                                                                                                        | Verified by                                                                                                                                                                                    |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Jobs can fail**                                    | Classified as transient (retry with backoff) or permanent (fail fast). The reason is visible in `last_error` and `attempt_history`.                                                                                                                                 | `transient_failures_back_off_then_give_up_after_max_attempts`, `unusable_file_fails_permanently_without_retry`                                                                                 |
| **Worker crashes mid-job**                           | Heartbeats stop, the lease (30s) expires, and a sweeper on any worker returns the job to `queued` and re-dispatches it. The attempt is recorded as `lease_expired`. The partial transaction was never committed, so the retry starts clean.                         | `crashed_worker_job_is_recovered_by_the_reaper_and_completes_once`; manual `kill -9` test (see below)                                                                                          |
| **Job picked up by more than one worker**            | The atomic claim gives exactly one winner; others skip and ack.                                                                                                                                                                                                     | `concurrent_claims_only_one_worker_wins`, `duplicate_delivery_to_many_workers_processes_once`                                                                                                  |
| **A stalled worker wakes up after losing its lease** | Fencing token: its heartbeat, commit, retry, and fail calls all no-op.                                                                                                                                                                                              | `stale_worker_cannot_overwrite_the_new_owner`                                                                                                                                                  |
| **System restarted during processing**               | Orphaned `running` jobs are reaped once their lease expires. Queued jobs sit in durable queues and in Postgres. The API and workers retry their DB and broker connections on startup, and consumers auto-reconnect.                                                 | Manual full-restart test (see below)                                                                                                                                                           |
| **Operations time out / error**                      | Each attempt has a hard timeout (`JOB_TIMEOUT_SECS`), treated as transient. The pool has an acquire timeout, and publishing waits at most 5s for a broker confirm.                                                                                                  | `attempt_timeout_is_a_transient_failure`                                                                                                                                                       |
| **Object storage down**                              | Uploads return `503` and create nothing (the client retries). Workers treat storage errors as transient: the job retries with backoff and completes once storage is back. A report is never marked `completed` without its files. `/ready` reports `storage:false`. | `storage_outage_is_transient_and_the_job_recovers`, `report_storage_outage_retries_and_does_not_complete_without_files`, `upload_is_rejected_cleanly_when_object_storage_is_down`; manual test |
| **Uploaded file deleted before processing**          | The import fails permanently with a clear reason; retrying can't bring the file back.                                                                                                                                                                               | `missing_upload_object_fails_permanently`                                                                                                                                                      |
| **Broker down when a job is created**                | The upload still returns 202 because the job is safely in Postgres. The sweeper publishes it when the broker returns. `/ready` reports `broker:false`.                                                                                                              | `dispatcher_publishes_undelivered_and_due_jobs_only`; manual test                                                                                                                              |
| **Message lost by the broker**                       | The sweeper re-publishes queued jobs whose last publish is older than `REPUBLISH_AFTER_SECS`.                                                                                                                                                                       | `dispatcher_publishes_undelivered_and_due_jobs_only`                                                                                                                                           |
| **Poison message** (unparseable body)                | Rejected without requeue and logged at error level.                                                                                                                                                                                                                 | Code: `worker/consumer.rs`                                                                                                                                                                     |
| **Worker shutdown (SIGTERM / deploy)**               | Stops taking new messages, waits up to `SHUTDOWN_GRACE_SECS` for in-flight jobs, then exits. Anything unfinished is recovered through lease expiry.                                                                                                                 | Manual test                                                                                                                                                                                    |
| **Long-running jobs**                                | Heartbeats extend the lease every 10s, so a job longer than the lease is never reaped by mistake.                                                                                                                                                                   | `heartbeats_keep_long_jobs_alive`                                                                                                                                                              |

### Failure demos you can run yourself

```bash
# Crash a worker in the middle of a report (reports take ~5s by default)
curl -s -X POST localhost:8080/reports -H 'X-User-Id: alice'   # note the report_id
docker compose kill -s SIGKILL worker                           # kill all worker replicas mid-job
docker compose up -d worker                                     # bring them back
# ~30s later (lease expiry) the report completes. attempt_history shows
# attempt 1 = lease_expired and attempt 2 = succeeded.

# Watch retries: 30% of attempts fail with an injected transient error
CHAOS_FAILURE_RATE=0.3 docker compose up -d worker
docker compose logs -f worker | grep -E "retry scheduled|gave up"

# Broker outage: uploads still succeed and are processed once the broker is back
docker compose stop rabbitmq
curl -s -X POST localhost:8080/imports -H 'X-User-Id: alice' -F file=@samples/orders_sample.csv
docker compose start rabbitmq
```

These scenarios were run against real Postgres and RabbitMQ during development:

- A `kill -9` of the worker holding a report: the other worker recovered it after lease expiry and the report completed correctly.
- A `kill -9` of _every_ process mid-job, followed by a restart: the orphaned job was recovered.
- RabbitMQ stopped during an upload: the upload returned 202, and the job was processed about 2s after the broker came back.
- A 50% chaos failure rate across 8 concurrent imports: all 8 completed after 1–3 attempts.

---

## 6. Observability

- **Structured JSON logs** (`LOG_FORMAT=json`, the default; use `pretty` for local dev), one object per line.
- Every job log line carries a `job` span with `job_id`, `kind`, `user_id`, `attempt`, `max_attempts`, and `worker_id`.
- Outcome events add `duration_ms`, `error_kind` (`transient` or `permanent`), `error`, `retry_in_ms`, and `next_attempt_at`.
- Import events add `total_rows`, `valid_rows`, `invalid_rows`, and the first invalid line and reason.
- Recovery events are logged at `WARN` with the `previous_owner`.
- Every HTTP request is logged at `INFO` with its status and latency. Each request gets an `x-request-id`, which is generated or propagated, returned in the response, and included in every log line for that request.

Example: a failed attempt, logged by a worker.

```json
{
  "timestamp": "2026-09-30T14:57:36.15Z",
  "level": "WARN",
  "message": "job attempt failed; retry scheduled",
  "duration_ms": 0,
  "error_kind": "transient",
  "error": "chaos: injected transient failure (CHAOS_FAILURE_RATE=0.5)",
  "retry_in_ms": 288,
  "next_attempt_at": "2026-09-30 14:57:36.437 UTC",
  "span": {
    "job_id": "2df78a88-…",
    "kind": "import",
    "user_id": "erin",
    "attempt": 1,
    "max_attempts": 5,
    "worker_id": "worker-3",
    "name": "job"
  }
}
```

Beyond the logs, users can see job history in the API (`attempt_history`), and operators can see queue depth and consumers in the RabbitMQ UI.

---

## 7. Tests

```bash
# In Docker (no local Rust needed)
docker compose --profile test run --rm tests

# Locally: needs Postgres (e.g. `docker compose up -d postgres seaweedfs`, Postgres on port 55432).
# Each test creates and drops its own throwaway database.
export DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres
export S3_TEST_ENDPOINT=http://localhost:8333   # optional: also run the real S3 client tests
cargo test
```

The suite has 61 tests. Integration tests use an in-memory object store (which can simulate outages), except `tests/s3_store.rs`, which runs the real S3 client against a real S3 server whenever `S3_TEST_ENDPOINT` is set. The Docker test profile points it at SeaweedFS; without it, those two tests are skipped with a message.

**22 unit tests** (no I/O) cover:

- CSV validation: the sample file, all errors collected per row, duplicate order ids, wrong field counts, BOM and column order, quoting, price precision and bounds, non-UTF-8 input and NUL bytes, and fatal header errors.
- Backoff math.
- Report CSV rendering and money serialisation.
- Object-key layout and the in-memory store.
- `storage-admin` duration parsing and key parsing.

**20 job-processing integration tests** run against real Postgres, one fresh database per test. They cover:

- The full import of the sample file.
- Report totals checked against the expected output, and the report files written to object storage.
- Concurrent claims and duplicate delivery to five workers.
- Crash recovery and stale-worker fencing.
- Retry → give up, retry → success, and permanent failures.
- Poison pills, timeouts, and heartbeats.
- The dispatcher.
- Re-upload deduplication and cross-user isolation.
- The invalid-row storage cap.
- Object storage outages (imports and reports), a deleted upload, and deterministic report keys that overwrite a crashed attempt's partial file.

**10 API tests** use the real router and a real database. They cover:

- The whole workflow (upload → process → errors → report → download), including downloading the original file.
- Authentication checks and cross-user isolation.
- Idempotency keys, including rejecting a reused key with a different file.
- Report request validation and upload validation (empty file, oversized file, a file exactly at the limit, missing field).
- The visible `retrying` state.
- Storage down at upload time (`503`, no job created, `/ready` false).
- Removed files returning `410` while statuses, summaries and new reports keep working.
- OpenAPI, health, and Swagger endpoints.

**7 `storage-admin` tests** cover:

- Files are kept until explicitly deleted, and deletion is idempotent.
- Deleting a pending job's file is refused unless forced.
- `purge` only touches finished jobs older than the cutoff (including `--dry-run` and `--target`).
- Orphan detection, including protection of in-flight uploads and objects under the wrong user's prefix.
- The mass-deletion guard (refuses when most objects look orphaned, unless `--yes`).
- A forced delete before a report finishes doesn't hide the files the report then writes.

**2 real S3 tests** cover the actual `aws-sdk-s3` client: bucket creation, put, get, overwrite, list, delete, not-found handling, and a missing _bucket_ staying a retryable error rather than "file not found". They use one fixed test bucket and clean up after themselves.

`scripts/demo.sh` is an end-to-end smoke test against the running Compose stack.

---

## 8. Configuration

All settings are environment variables with defaults. The Compose file sets the relevant ones.

| Variable                                         | Default                                                  | Used by       | Meaning                                                                                                             |
| ------------------------------------------------ | -------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                                   | `postgres://postgres:postgres@localhost:5432/csvreports` | all           | Postgres connection                                                                                                 |
| `AMQP_URL`                                       | `amqp://guest:guest@localhost:5672/%2f`                  | both          | RabbitMQ connection                                                                                                 |
| `S3_ENDPOINT`                                    | unset (= AWS)                                            | all           | S3-compatible endpoint, e.g. `http://seaweedfs:8333`                                                                |
| `S3_REGION` / `S3_BUCKET`                        | `us-east-1` / `csv-reports`                              | all           | Bucket location (created on startup if missing)                                                                     |
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`      | unset (Compose: `csvreports` / `csvreports-secret`)      | all           | Static S3 credentials. When unset, the standard AWS credential chain is used (env, profile, task or instance role). |
| `S3_FORCE_PATH_STYLE`                            | `true`                                                   | all           | Path-style URLs (needed by most self-hosted S3 servers); `false` for AWS                                            |
| `STORAGE_ADMIN_LOG`                              | `warn`                                                   | storage-admin | CLI log level (logs go to stderr; results to stdout)                                                                |
| `HTTP_ADDR`                                      | `0.0.0.0:8080`                                           | api           | Listen address                                                                                                      |
| `MAX_UPLOAD_BYTES`                               | `10485760` (10 MiB)                                      | api           | Maximum file size (`413` above it)                                                                                  |
| `JOB_MAX_ATTEMPTS`                               | `5`                                                      | api           | Attempts before a job is `failed`                                                                                   |
| `WORKER_ID`                                      | `$HOSTNAME-<random>`                                     | worker        | Identity in logs and leases                                                                                         |
| `WORKER_CONCURRENCY`                             | `4`                                                      | worker        | Prefetch per queue (parallel jobs)                                                                                  |
| `JOB_LEASE_SECS`                                 | `30`                                                     | worker        | Lease length without a heartbeat                                                                                    |
| `JOB_HEARTBEAT_SECS`                             | `10`                                                     | worker        | Lease renewal interval                                                                                              |
| `JOB_TIMEOUT_SECS`                               | `300`                                                    | worker        | Hard timeout per attempt                                                                                            |
| `RETRY_BACKOFF_BASE_MS` / `RETRY_BACKOFF_MAX_MS` | `2000` / `300000`                                        | worker        | Backoff curve                                                                                                       |
| `SWEEP_INTERVAL_MS`                              | `2000`                                                   | worker        | Reaper and dispatcher period                                                                                        |
| `REPUBLISH_AFTER_SECS`                           | `60`                                                     | worker        | Re-publish an unconsumed queued job after this long                                                                 |
| `MAX_STORED_ROW_ERRORS`                          | `1000`                                                   | worker        | Invalid-row details stored per import                                                                               |
| `REPORT_SIMULATED_DELAY_MS`                      | `0` (Compose: `5000`)                                    | worker        | Simulates an expensive report                                                                                       |
| `CHAOS_FAILURE_RATE`                             | `0`                                                      | worker        | Probability of injecting a transient failure (demo only)                                                            |
| `SHUTDOWN_GRACE_SECS`                            | `20`                                                     | worker        | Drain time on SIGTERM                                                                                               |
| `LOG_FORMAT` / `RUST_LOG`                        | `json` / `info`                                          | both          | Logging                                                                                                             |

---

## 9. Project layout

```
├── Cargo.toml / Cargo.lock
├── Dockerfile                 # multi-stage: builder → tests → slim runtime (non-root, 3 binaries)
├── docker-compose.yml         # postgres, rabbitmq, seaweedfs, api, worker ×2, tests (profile)
├── migrations/                # SQL schema (embedded in the binaries, applied on startup)
├── src/
│   ├── bin/api.rs             # HTTP server entry point
│   ├── bin/worker.rs          # worker entry point (consumers + sweeper + graceful shutdown)
│   ├── bin/storage_admin.rs   # CLI for explicit removal of stored files
│   ├── api/                   # handlers, models, errors, auth extractor, OpenAPI
│   ├── worker/                # execute_job, import/report processors, consumer, sweeper
│   ├── jobs.rs                # job state machine: claim, heartbeat, fence, retry, reap, dispatch
│   ├── csv_import.rs          # pure CSV parsing + validation
│   ├── report.rs              # aggregation SQL + CSV rendering
│   ├── queue.rs               # RabbitMQ topology + publisher
│   ├── storage.rs             # BlobStore trait: S3 (aws-sdk-s3) + in-memory implementations
│   ├── admin.rs               # storage-admin logic (list, delete, purge, orphans)
│   ├── config.rs, db.rs, telemetry.rs
├── tests/                     # integration tests (job processing, API, storage-admin, real S3)
├── samples/                   # sample CSVs + expected report
├── postman/                   # Postman collection
├── docs/ARCHITECTURE.md       # detailed design and failure analysis
├── docs/openapi.json          # exported OpenAPI spec
└── scripts/demo.sh            # end-to-end smoke test via curl
```

---

## 10. Trade-offs & Known Limitations

**Decisions and alternatives considered**

- **Postgres as the source of truth, with the queue used only for notifications.**
  - _Alternative:_ trust RabbitMQ acks alone.
  - _Rejected because_ a worker that crashes after committing but before acking would get the job redelivered and re-processed. The broker can't know whether the side effects happened.
  - _Cost:_ an extra claim query per job, plus a sweeper.
- **Retries scheduled in the database, not the broker.**
  - _Alternative:_ TTL queues with dead-letter exchanges, or the delayed-message plugin.
  - _Rejected because_ the DB approach needs no broker plugins, survives broker restarts, and keeps retry state visible to users (`next_attempt_at`).
  - _Cost:_ retry latency has sweep-interval granularity (2s).
- **All-or-nothing import transaction.**
  - Simple and crash-safe: no partial imports and no resume bookkeeping.
  - _Cost:_ one large transaction per file. That's fine up to the 10 MiB limit (≈100k+ rows), but it wouldn't scale to multi-GB files; see "With more time".
- **Files in object storage, written before the job is committed.**
  - The job and its file can't be committed atomically across two systems, so the order is chosen to make the failure harmless: at worst an orphaned object (found by `storage-admin orphans`), never a job without its file.
  - _Alternative:_ commit first, then upload. _Rejected because_ a crash in between leaves a job whose file never arrives.
- **Downloads are proxied through the API** rather than handed out as pre-signed S3 URLs.
  - This keeps per-user authorization in one place, and the S3 endpoint doesn't have to be reachable from clients (in Compose it's an internal hostname).
  - _Cost:_ file bytes flow through the API process. For large files, pre-signed URLs with a public S3 endpoint would be better.
- **Files are kept until explicitly removed** (your choice for this project), instead of expiring automatically.
  - Predictable, and nothing disappears while someone may still need it.
  - _Cost:_ storage grows until an operator runs `storage-admin purge`. A scheduled purge or an S3 lifecycle rule would automate it.
- **Skip-and-record invalid rows.**
  - _Alternative:_ reject the whole file on the first bad row, which is stricter but hostile to users.
  - Could be made configurable per upload, for example `?on_error=reject`.
- **Report scope fixed at request time,** so retries are deterministic. Imports that finish later are not included; request a new report.
- **Latest import wins for duplicate `order_id`s** across imports. This is a reasonable default for re-uploads and corrections, but it's a business rule that should be confirmed with stakeholders.
- **Sweeper in every worker** instead of a separate scheduler. That leaves no single point of failure and needs no leader election, and `SKIP LOCKED` prevents contention.

**Known limitations**

- **Authentication is a demo header** (`X-User-Id`) with no verification. It's isolated in one extractor (`api/user.rs`), so swapping in JWT/OIDC wouldn't touch the handlers.
- **Imports are at-least-once executed but exactly-once committed.** A crashed attempt re-runs from scratch, which is fine for idempotent parsing but wasteful for huge files.
- **Report `Idempotency-Key`s are not tied to the request body.** Reusing a key with different `import_ids` returns the original report. Uploads _are_ checked by file hash.
- **A retry that cannot be recorded because the DB is down gets no backoff.** It's recovered by lease expiry and re-run as soon as the lease is reaped. That's correct, just not delayed.
- **A stale worker can rewrite a report file after the new owner committed.** The content is identical except `generated_at`, because finished imports never change. After a purge, such a late write shows up as an orphan.
- **Duplicate messages are possible by design**: the API and the sweeper can both publish, and the sweeper republishes after 60s. They're harmless because of the claim, but they show up as `skipping` log lines.
- **No row-level progress while an import is processing.** The status is `processing` until the commit.
- **Reports are computed on the fly from `orders`.** At large scale they'd need pre-aggregation or materialised views.
- **No automatic retention.** Stored files are removed only by `storage-admin`, and there is no user-facing delete endpoint. Row errors and database records are never cleaned up.
- **Uploads are buffered in memory** (up to `MAX_UPLOAD_BYTES`) before being written to S3, instead of being streamed with multipart upload. That's fine at 10 MiB but not for very large files.
- **Single-node SeaweedFS** with no replication in Compose. For production this would be AWS S3 or a replicated cluster.
- **No metrics endpoint.** Observability is logs, the API, and the RabbitMQ UI.
- **Single-node RabbitMQ and Postgres** in Compose; there's no HA setup.

**With more time I would**

1. Stream large files end to end: multipart upload straight to S3, then a streaming import in chunks with a checkpoint (`last_committed_line`) inside the fenced transaction, so a crash resumes instead of restarting. This would also give real progress percentages.
2. Add Prometheus metrics (queue lag, job durations, attempt and outcome counters, reaped leases) and OpenTelemetry tracing across the API → queue → worker path, propagating the request id in message headers.
3. Add real authentication (JWT), per-user rate limits and quotas, and an admin endpoint to manually retry `failed` jobs.
4. Add a transactional outbox table instead of "publish after commit plus the sweeper as a safety net". It's the same guarantee but more explicit, and it removes the republish-interval heuristic.
5. Add a scheduled `storage-admin purge` (or S3 lifecycle rules), report expiry, and a user-facing delete endpoint.
6. Add property-based tests for the CSV parser and a chaos test harness that kills containers randomly under load.

---

## 11. Requirements checklist

| #   | Requirement                                                                         | Where                                                                               |
| --- | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| 1   | CSV upload API creates an import job and returns a way to track it                  | `POST /imports` → 202 + `import_id` + `Location`; `GET /imports/{id}`               |
| 2   | Background worker processes imports asynchronously and persists rows                | `worker/import.rs` → `orders` table                                                 |
| 3   | Invalid rows: decide, document, expose                                              | Section 4; `import_row_errors`; `GET /imports/{id}/errors`; `completed_with_errors` |
| 4   | Report request API returns immediately                                              | `POST /reports` → 202                                                               |
| 5   | Report worker generates asynchronously; result retrievable/downloadable             | `worker/report.rs`; `GET /reports/{id}`; `/download?format=csv\|json`               |
| 6   | Multiple users, concurrent imports and reports                                      | Per-user scoping; 2 worker replicas × 4 concurrency; concurrency tests              |
| –   | Jobs fail / worker crash / duplicate pickup / restart / timeouts / retry & recovery | Section 5                                                                           |
| –   | Rust, PostgreSQL, message queue (with justification)                                | Sections 3–4 (plus S3-compatible object storage for files)                          |
| –   | Everything runs in Docker with `docker compose up`                                  | `Dockerfile`, `docker-compose.yml`                                                  |
| –   | Swagger/OpenAPI or Postman                                                          | `/swagger-ui`, `docs/openapi.json`, `postman/`                                      |
| –   | Automated tests                                                                     | Section 7 (61 tests)                                                                |
| –   | Structured logging for jobs and failures                                            | Section 6                                                                           |
| –   | Stored files kept until explicitly removed                                          | Section 2, `storage-admin`                                                          |
| –   | README, architecture, schema/migrations, Trade-offs & Known Limitations             | This file, `docs/ARCHITECTURE.md`, `migrations/`, Section 10                        |
