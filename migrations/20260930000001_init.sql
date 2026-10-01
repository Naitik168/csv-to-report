-- =====================================================================
-- Initial schema
--
-- Design notes
--  * `jobs` is the single source of truth for background-job state for
--    BOTH imports and reports. RabbitMQ only carries "job X is ready"
--    notifications; losing or duplicating a message never corrupts state.
--  * `imports` / `reports` share their primary key with `jobs` (1:1).
--  * A job moves through: queued -> running -> succeeded | failed
--    (running -> queued again on retry or when a lease expires).
-- =====================================================================

CREATE TABLE users (
    id          TEXT PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
    id                UUID PRIMARY KEY,
    kind              TEXT NOT NULL CHECK (kind IN ('import', 'report')),
    user_id           TEXT NOT NULL REFERENCES users (id),
    status            TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),

    -- retry bookkeeping
    attempts          INT  NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts      INT  NOT NULL CHECK (max_attempts > 0),
    available_at      TIMESTAMPTZ NOT NULL DEFAULT now(),   -- not claimable before this (backoff)
    last_error        TEXT,

    -- dispatch bookkeeping (when did we last push a message for this job)
    published_at      TIMESTAMPTZ,

    -- lease: the worker that currently owns the job
    lease_token       UUID,         -- fencing token, regenerated on every claim
    locked_by         TEXT,         -- worker id, for observability only
    lease_expires_at  TIMESTAMPTZ,  -- extended by heartbeats while processing

    -- client-supplied Idempotency-Key header (dedupes retried POSTs)
    idempotency_key   TEXT,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,

    CONSTRAINT running_jobs_hold_a_lease CHECK (
        status <> 'running' OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT terminal_jobs_are_finished CHECK (
        status NOT IN ('succeeded', 'failed') OR finished_at IS NOT NULL
    )
);

CREATE UNIQUE INDEX jobs_idempotency_key_uq
    ON jobs (user_id, kind, idempotency_key) WHERE idempotency_key IS NOT NULL;
-- dispatcher: "queued jobs that are due"
CREATE INDEX jobs_dispatch_idx ON jobs (available_at) WHERE status = 'queued';
-- reaper: "running jobs whose lease expired"
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at) WHERE status = 'running';
-- listing a user's imports / reports
CREATE INDEX jobs_user_idx ON jobs (user_id, kind, created_at DESC);

-- One row per processing attempt: gives users and operators a full history
-- of what happened to a job (which worker, how long, why it failed).
CREATE TABLE job_attempts (
    job_id       UUID NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt      INT  NOT NULL,
    worker_id    TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    outcome      TEXT CHECK (outcome IN ('succeeded', 'failed', 'retry_scheduled', 'lease_expired')),
    error        TEXT,
    PRIMARY KEY (job_id, attempt)
);

CREATE TABLE imports (
    id            UUID PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    user_id       TEXT NOT NULL REFERENCES users (id),
    filename      TEXT NOT NULL,
    file_size     BIGINT NOT NULL,
    file_sha256   TEXT NOT NULL,
    file_data     BYTEA NOT NULL,
    -- filled in when processing succeeds
    total_rows    INT,
    valid_rows    INT,
    invalid_rows  INT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Valid, imported CSV rows. `customer_id` holds the CSV's `user_id` column
-- (the customer who placed the order), which is different from the API
-- user who uploaded the file (imports.user_id).
CREATE TABLE orders (
    import_id    UUID NOT NULL REFERENCES imports (id) ON DELETE CASCADE,
    line_number  INT  NOT NULL,
    customer_id  TEXT NOT NULL,
    order_id     TEXT NOT NULL,
    product      TEXT NOT NULL,
    quantity     INT  NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    status       TEXT NOT NULL,
    -- (import_id, line_number) makes re-processing idempotent
    PRIMARY KEY (import_id, line_number),
    UNIQUE (import_id, order_id)
);
CREATE INDEX orders_order_id_idx ON orders (order_id);

-- Rejected CSV rows with the reason(s) they were rejected.
CREATE TABLE import_row_errors (
    import_id    UUID NOT NULL REFERENCES imports (id) ON DELETE CASCADE,
    line_number  INT  NOT NULL,
    raw_record   TEXT NOT NULL,
    errors       JSONB NOT NULL,   -- [{ "field": "...", "message": "..." }]
    PRIMARY KEY (import_id, line_number)
);

CREATE TABLE reports (
    id          UUID PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    user_id     TEXT NOT NULL REFERENCES users (id),
    -- the imports this report covers, resolved when the report is requested
    import_ids  UUID[] NOT NULL,
    result      JSONB,          -- filled in when generation succeeds
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
