-- =====================================================================
-- Move file contents from Postgres to S3-compatible object storage.
--
-- * imports.file_data (BYTEA)  -> imports.file_key (object key) + file_deleted_at
-- * reports.result   (JSONB)   -> reports.summary (totals/counts) + csv_key/json_key
--                                 (the full report is a file in object storage)
--
-- Existing rows (from before this migration) cannot have their bytes copied to object
-- storage from SQL. They keep all their imported data (orders, errors, report summaries);
-- only their *files* are marked as unavailable, so downloads return 410 Gone.
-- =====================================================================

-- ---------- imports ----------
ALTER TABLE imports
    ADD COLUMN file_key        TEXT,
    ADD COLUMN file_deleted_at TIMESTAMPTZ;

UPDATE imports
   SET file_key        = 'uploads/' || user_id || '/' || id || '.csv',
       file_deleted_at = now();       -- legacy bytes stayed in Postgres; not in object storage

-- A legacy import that never ran can no longer succeed (its file is not in object storage):
-- fail it with a clear reason instead of letting workers discover that later.
UPDATE jobs j
   SET status = 'failed', finished_at = now(), updated_at = now(),
       last_error = 'file was stored before the move to object storage and is no longer available; please upload it again',
       lease_token = NULL, lease_expires_at = NULL, locked_by = NULL
  FROM imports i
 WHERE i.id = j.id AND j.status IN ('queued', 'running');

ALTER TABLE imports
    ALTER COLUMN file_key SET NOT NULL,
    DROP COLUMN file_data;

-- ---------- reports ----------
ALTER TABLE reports
    ADD COLUMN summary          JSONB,
    ADD COLUMN csv_key          TEXT,
    ADD COLUMN json_key         TEXT,
    ADD COLUMN files_deleted_at TIMESTAMPTZ;

-- Keep the totals of already-generated reports; their row-level files do not exist.
UPDATE reports
   SET summary = (result - 'rows') || jsonb_build_object('row_count', jsonb_array_length(result -> 'rows')),
       files_deleted_at = now()
 WHERE result IS NOT NULL;

ALTER TABLE reports DROP COLUMN result;
