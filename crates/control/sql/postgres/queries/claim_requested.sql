UPDATE walrus.table_reload
SET status = 'exporting',
    lease_holder = $2,
    lease_expiry = now() + make_interval(secs => $3),
    updated_at = now()
WHERE reload_id IN (
    SELECT candidate.reload_id
    FROM walrus.table_reload AS candidate
    WHERE candidate.epoch = $1 AND candidate.status = 'requested'
      -- Do not start the next queued source request until the loader has published the current one.
      AND NOT EXISTS (
          SELECT 1 FROM walrus.table_reload AS active
          WHERE active.epoch = candidate.epoch
            AND active.source_schema = candidate.source_schema
            AND active.source_table = candidate.source_table
            AND active.status IN ('exporting', 'export_complete')
      )
      AND (
          -- A legacy direct request keeps its historical priority/duplicate semantics.
          candidate.source_request_id IS NULL
          OR (
              NOT EXISTS (
                  SELECT 1 FROM walrus.table_reload AS legacy
                  WHERE legacy.epoch = candidate.epoch
                    AND legacy.source_schema = candidate.source_schema
                    AND legacy.source_table = candidate.source_table
                    AND legacy.status = 'requested'
                    AND legacy.source_request_id IS NULL
              )
              -- Exactly the oldest source-WAL request for a table may enter the active state.
              AND NOT EXISTS (
                  SELECT 1 FROM walrus.table_reload AS earlier
                  WHERE earlier.epoch = candidate.epoch
                    AND earlier.source_schema = candidate.source_schema
                    AND earlier.source_table = candidate.source_table
                    AND earlier.status = 'requested'
                    AND earlier.source_request_id IS NOT NULL
                    AND earlier.reload_id < candidate.reload_id
              )
          )
      )
    ORDER BY candidate.reload_id
    LIMIT $4
    FOR UPDATE OF candidate SKIP LOCKED
)
RETURNING reload_id, epoch, source_schema, source_table,
          flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
          source_request_id, parent_request_id,
          request_scope AS "request_scope: ReloadScope",
          chunk_no, cursor_pk,
          start_lsn AS "start_lsn: Lsn",
          first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
          schema_version, restart_count, lease_holder, error
