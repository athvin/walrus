UPDATE walrus.table_reload
SET status = 'export_complete', final_lsn = $2, updated_at = now()
WHERE reload_id = $1 AND status = 'exporting'
  AND start_lsn IS NOT NULL
  AND $2 >= start_lsn
  AND EXISTS (
    SELECT 1 FROM walrus.table_reload_marker
    WHERE reload_id = $1 AND marker_kind = 'baseline'
      AND lsn = walrus.table_reload.start_lsn
      AND schema_version = walrus.table_reload.schema_version
  )
  AND EXISTS (
    SELECT 1 FROM walrus.table_reload_marker
    WHERE reload_id = $1 AND marker_kind = 'end'
      AND lsn = $2
      AND schema_version = walrus.table_reload.schema_version
  )
