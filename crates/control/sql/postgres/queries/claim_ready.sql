SELECT m.id, m.epoch, m.source_schema, m.source_table, m.s3_uri, m.kind, m.row_count,
       m.lsn_start AS "lsn_start: Lsn", m.lsn_end AS "lsn_end: Lsn", m.schema_version,
       m.status, m.reload_id
FROM walrus.file_manifest m
WHERE m.epoch = $1 AND m.source_schema = $2 AND m.source_table = $3 AND m.status = 'ready'
  AND (
      NOT EXISTS (
          SELECT 1 FROM walrus.table_reload r
          WHERE r.epoch = m.epoch
            AND r.source_schema = m.source_schema
            AND r.source_table = m.source_table
            AND r.status IN ('requested', 'exporting')
      )
      -- A later source reload may already be queued while the loader is publishing the current
      -- completed export. That queue must not deadlock the current cutover;
      -- Phase A itself stops at this export_complete attempt's H, then the next request remains
      -- paused normally.
      OR EXISTS (
          SELECT 1 FROM walrus.table_reload ready
          WHERE ready.epoch = m.epoch
            AND ready.source_schema = m.source_schema
            AND ready.source_table = m.source_table
            AND ready.status = 'export_complete'
      )
  )
ORDER BY m.lsn_end, m.id
LIMIT $4
