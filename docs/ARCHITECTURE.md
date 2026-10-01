# Architecture & Failure Analysis

This document explains *why* the system is built the way it is and walks through every failure scenario the assignment lists. It's written as preparation for a design discussion. The README has the summary.

## 1. Guiding principle

> **The database decides; the queue only notifies.**

Every question of the form "has this job been done, and by whom?" is answered by a row in `jobs`, changed only by conditional updates that Postgres serialises. RabbitMQ messages carry nothing but a job id. This means that:

- **Losing** a message only delays a job, because the sweeper republishes it.
- **Duplicating** a message is a no-op, because the claim fails.
- **Redelivering** a message after a crash is a no-op, because the job is either finished or still leased.
- A **broker restart** loses no state. Pending retries live in Postgres, not in broker TTL queues.

## 2. Component responsibilities

| Component | Responsibility | Holds state? |
|---|---|---|
| `api` | Validate the request, store the uploaded file in object storage, persist job + payload atomically, publish a notification, serve status and downloads | No (stateless, horizontally scalable) |
| `worker` consumer | Receive notification → `execute_job` → ack | No |
| `worker` sweeper | Reap expired leases; publish due and undelivered jobs | No (every replica runs it; `SKIP LOCKED` splits the work) |
| PostgreSQL | Jobs, attempts, file keys, imported rows, row errors, report summaries | **Yes: source of truth for state** |
| SeaweedFS (S3) | Uploaded CSVs and generated report files (CSV + JSON) | **Yes: file contents only**, addressed by deterministic keys |
| RabbitMQ | Durable quorum queues `csv.imports`, `csv.reports` | Transient notifications only |

Imports and reports use separate queues. A burst of large imports then can't starve report generation, and each queue can be scaled or monitored independently.

## 3. Happy path

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as api
    participant DB as Postgres
    participant Q as RabbitMQ
    participant S3 as SeaweedFS (S3)
    participant W as worker

    C->>A: POST /imports (multipart file)
    A->>S3: PUT uploads/{user}/{import_id}.csv
    A->>DB: BEGIN; INSERT jobs(queued); INSERT imports(file_key); COMMIT
    A->>Q: publish {job_id} (persistent, confirm)
    A->>DB: UPDATE jobs SET published_at = now()
    A-->>C: 202 {import_id, status: queued}
    Q->>W: deliver {job_id}
    W->>DB: claim: UPDATE … SET running, attempts+1, lease_token=T WHERE status='queued' AND due
    W->>S3: GET uploads/{user}/{import_id}.csv
    W->>W: parse + validate CSV (outside tx), heartbeat every 10s
    W->>DB: BEGIN; SELECT … WHERE lease_token=T FOR UPDATE (fence)<br/>INSERT orders, errors; UPDATE counters; SET succeeded; COMMIT
    W->>Q: ack
    C->>A: GET /imports/{id}
    A-->>C: {status: completed_with_errors, valid_rows: 11, invalid_rows: 1}
```

The report flow is identical, except that `POST /reports` resolves and stores the list of `import_ids` up front, and the processor runs the aggregation query, writes `reports/{user}/{report_id}.csv` and `.json` to object storage, and then (in the fenced transaction) stores their keys and a small summary.

### Why write the file *before* committing the job

Object storage and Postgres can't share a transaction, so one write must come first. Writing the object first means the only possible inconsistency is an **orphaned object**: the commit failed or the API crashed. That is harmless, and `storage-admin orphans` finds it. The opposite order could leave a committed job whose file never arrives. For reports, the files are written before the job is marked `completed`, so `completed` always implies the files exist. Deterministic keys make every write idempotent: a retry or a stale worker overwrites the same objects.

### Why publish *after* commit

If the API published first and then crashed before committing, a worker would receive an id that doesn't exist. Publishing after commit leaves the opposite gap: the job is committed but the message is never sent. The sweeper closes that gap, because `published_at IS NULL` jobs are published within `SWEEP_INTERVAL_MS`. This is the transactional-outbox guarantee, implemented with the `jobs` table itself as the outbox.

## 4. The lease protocol

```
claim            heartbeat        heartbeat       fenced commit
  │  lease=30s      │ +30s           │ +30s             │
  ▼─────────────────▼────────────────▼─────────────────▼
  token T issued    WHERE token=T    WHERE token=T     SELECT … token=T FOR UPDATE → write → succeeded
```

- **Lease (`lease_expires_at`).** A claim is valid for `JOB_LEASE_SECS` (30s). A background task renews it every `JOB_HEARTBEAT_SECS` (10s), so a heartbeat can miss twice before the lease is in danger.
- **Fencing token (`lease_token`).** A new UUID is issued on every claim, and every write the worker makes includes `WHERE lease_token = T`. If the job was reaped and re-claimed, `T` no longer matches and the write affects zero rows. This is what makes "the old worker comes back to life" safe.
- **Fence under lock.** The final transaction starts with `SELECT … FOR UPDATE` on the job row. While it's held:
  - the reaper (`FOR UPDATE SKIP LOCKED`) skips the job;
  - a competing claim blocks, then re-evaluates `status='queued'`, finds `succeeded`, and gets zero rows.

  So no worker can claim the job between the fence check and the commit.
- **Lost lease detected mid-job.** If a heartbeat returns zero rows, the worker cancels its processing future (`tokio::select!` on a cancellation token) and discards its work.

### Why a lease instead of `SELECT … FOR UPDATE` held for the whole job

Holding a row lock for the whole job would need a transaction open for minutes. A crash would be detected by the TCP connection dying, which is fast. But a long transaction blocks vacuum, pins a pool connection per job, and a network partition can leave it hanging until TCP keepalive gives up. A lease plus heartbeat turns "is the worker alive?" into an explicit, configurable timeout, and keeps transactions short.

## 5. Failure scenarios, step by step

### 5.1 Worker crashes halfway through processing

1. Worker A claims job J: attempt 1, token T1, lease until t+30s.
2. A is `kill -9`'d while parsing. Its open transaction, if any, is rolled back by Postgres when the connection drops, so no partial rows exist.
3. RabbitMQ sees the channel close and redelivers the message to worker B. B's claim fails (J is still `running` with an unexpired lease), so B acks and drops it. The message is gone, which is fine: the DB still knows J.
4. At t+30s, any worker's sweeper reaps J:
   - `status=queued`, `last_error='lease expired: worker A stopped responding during attempt 1'`
   - the `job_attempts` row 1 is marked `lease_expired`
   - `published_at=NULL`
5. On its next tick, the sweeper publishes J. B claims it (attempt 2, token T2) and completes it.

Worst-case recovery time is lease (30s) + sweep interval (2s). Test: `crashed_worker_job_is_recovered_by_the_reaper_and_completes_once`.

### 5.2 A job is picked up by more than one worker

The API publishes, and the sweeper may also publish (for example, if the API's `mark_published` failed), so two workers can receive J at the same time. Both run the claim `UPDATE … WHERE id=J AND status='queued'`. Postgres takes a row lock for the first. The second waits, re-checks the `WHERE` clause against the committed row (`running`), and matches nothing. Exactly one claim returns a row.

Tests: `concurrent_claims_only_one_worker_wins` (10 parallel claims) and `duplicate_delivery_to_many_workers_processes_once` (5 parallel `execute_job` calls).

### 5.3 A slow worker loses its lease, then wakes up

1. Worker A (token T1) freezes for 60s, for example from a stop-the-world pause or a VM migration.
2. The reaper requeues J. Worker B claims it (T2) and commits the result.
3. A wakes up:
   - its heartbeat `WHERE lease_token=T1` updates 0 rows, so A cancels;
   - even if A reached its commit, `fence` returns false, so it returns `LeaseLost` and rolls back;
   - `fail` and `schedule_retry` are also fenced.

B's result stands. Test: `stale_worker_cannot_overwrite_the_new_owner`.

### 5.4 The system restarts while jobs are processing

Say Postgres, RabbitMQ, the API, and all workers restart together.

- `running` jobs keep their `lease_expires_at`. When workers come back, their sweepers reap the expired ones.
- `queued` jobs are either still in the durable quorum queues (persistent messages) or get republished by the sweeper.
- Retries scheduled for the future are rows with `available_at`, so they're unaffected.
- Services started before their dependencies retry connecting with backoff. Compose also orders startup with health checks.

### 5.5 Operations time out or return errors

| Operation | Protection |
|---|---|
| A whole job attempt | `tokio::time::timeout(JOB_TIMEOUT_SECS)` → transient error → retry with backoff |
| Getting a DB connection | Pool `acquire_timeout = 5s` → `sqlx::Error` → transient |
| DB query error mid-job | Transaction rolled back → transient → retry |
| Object storage error or timeout | SDK retries up to 3 times (5s connect, 30s per attempt), then transient → job retry with backoff. Upload API returns `503` and creates nothing |
| Uploaded object missing | Permanent: the import fails with a clear reason (the file was deleted with `storage-admin --force`) |
| Publish (API or sweeper) | 5s timeout on the publisher confirm; on failure the connection is dropped and rebuilt next time, and the job stays queued for the sweeper |
| Consumer connection lost | Reconnect loop with exponential backoff (1s → 30s); in-flight jobs finish, their acks fail, and redeliveries are no-ops |
| Can't even record a failure (DB down) | Logged; the lease expires and the reaper recovers the job once the DB is back |

### 5.6 Retry policy and when to stop

```
delay(n) = min(base · 2^(n-1), max) · U(0.5, 1.0)     base = 2s, max = 5 min
attempt:     1     2     3     4      5
delay ≈      1–2s  2–4s  4–8s  8–16s  → failed (JOB_MAX_ATTEMPTS = 5)
```

- **Jitter** prevents a thundering herd when many jobs fail together, for example during a DB blip.
- **Stop conditions:**
  - A *permanent* error fails the job immediately. Retrying a file with a missing column is pointless.
  - A *transient* error fails the job once `attempts ≥ max_attempts`.
  - A crash-looping job is also bounded, because attempts increment at claim time and the reaper checks the same limit.
- Operators and users see every attempt in `attempt_history`, including the worker, the timing, the outcome, and the error.

## 6. Transaction boundaries (summary)

| Step | Transaction | Why |
|---|---|---|
| API create job | (upload: `PUT` object first) then `INSERT jobs + INSERT imports/reports`, commit, *then* publish | No job without its file; no message for an uncommitted job |
| Claim | `UPDATE jobs (claim) + INSERT job_attempts` | Attempt history is always consistent with the claim |
| Heartbeat | Single `UPDATE` (autocommit) | Must not wait behind long transactions |
| Processing | *No transaction* while downloading, parsing, aggregating or writing report files | Keep transactions short; I/O and CPU work don't hold locks |
| Commit result | `fence (FOR UPDATE) → write results → succeeded → finish attempt` | Atomic, all-or-nothing, fenced |
| Retry / fail | `fenced UPDATE jobs + UPDATE job_attempts` | Consistent state and history |
| Reap | `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED) + UPDATE job_attempts` | Safe with many reapers running concurrently |

## 7. Invalid data policy

| Level | Examples | Effect | Visible as |
|---|---|---|---|
| File | Empty file, missing required column, duplicate column, broken quoting | Import `failed` permanently (no retry) | `status=failed`, `last_error` names the problem and the expected header |
| Row | Non-numeric or negative price, >2 decimals, quantity ≤ 0, missing field, unknown status, duplicate `order_id` in the file, wrong field count, non-UTF-8 text | Row skipped; *all* its errors recorded; import continues | `status=completed_with_errors`, `invalid_rows`, `GET /imports/{id}/errors` with line number, raw row, and per-field messages |

Row details are capped at `MAX_STORED_ROW_ERRORS` (1,000) per import so a garbage upload can't flood the database. Every invalid row is still counted.

## 8. File retention

Files are kept until someone removes them with `storage-admin`; nothing expires on its own. The command:
- refuses to touch files of jobs that are still `queued` or `running` (unless `--force`);
- deletes the object first and then records `file_deleted_at` / `files_deleted_at`, so re-running an interrupted delete finishes it (S3 deletes are idempotent);
- leaves all Postgres data in place, so imported rows keep feeding new reports;
- treats an object as owned only when both the id and the user in its key match a live record, and refuses to delete orphans in bulk (more than half of all objects) without `--yes`, which guards against running it against the wrong database.

Downloads of removed files return `410 Gone`.

## 9. Scaling notes

- **API:** stateless; add replicas behind a load balancer.
- **Workers:** add replicas (`docker compose up --scale worker=N`) or raise `WORKER_CONCURRENCY`. RabbitMQ round-robins deliveries and prefetch caps in-flight jobs per worker.
- **Database:** partial indexes (`WHERE status='queued'` and `WHERE status='running'`) keep the sweeper's queries cheap however many finished jobs accumulate. `orders` is indexed for the report query.
- **Bottlenecks at larger scale:**
  - Single-transaction imports of very large files; fix with chunked, checkpointed imports.
  - On-the-fly aggregation for reports; fix with pre-aggregation.
  - Files buffered in memory in the API and worker; fix with multipart upload and streaming reads.

  These are listed in the README's Trade-offs section.
