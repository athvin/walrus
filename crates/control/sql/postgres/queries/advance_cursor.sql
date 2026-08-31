UPDATE walrus.table_reload
SET chunk_no = $2,
    cursor_pk = $3,
    first_lsn = COALESCE(first_lsn, $4),
    schema_version = COALESCE(schema_version, $5),
    updated_at = now()
WHERE reload_id = $1 AND status = 'exporting' AND chunk_no = $2::bigint - 1
  AND (schema_version IS NULL OR schema_version = $5)
