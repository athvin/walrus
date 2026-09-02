INSERT INTO walrus.table_reload_marker
    (reload_id, marker_kind, lsn, schema_version)
SELECT reload_id, 'end', $2, schema_version
FROM walrus.table_reload
WHERE reload_id = $1
  AND schema_version IS NOT NULL
  AND schema_version = $3
  AND (COALESCE(source_request_id, parent_request_id) IS NULL
       OR COALESCE(source_request_id, parent_request_id) = $4)
  AND source_schema = $5
  AND source_table = $6
  AND (
    (
      status = 'exporting'
      AND start_lsn IS NOT NULL
      AND start_lsn <= $2
    )
    OR EXISTS (
      SELECT 1 FROM walrus.table_reload_marker m
      WHERE m.reload_id = walrus.table_reload.reload_id
        AND m.marker_kind = 'end'
        AND m.lsn = $2
        AND m.schema_version = walrus.table_reload.schema_version
    )
  )
ON CONFLICT (reload_id, marker_kind) DO UPDATE
SET lsn = EXCLUDED.lsn
WHERE walrus.table_reload_marker.lsn = EXCLUDED.lsn
  AND walrus.table_reload_marker.schema_version = EXCLUDED.schema_version
RETURNING reload_id
