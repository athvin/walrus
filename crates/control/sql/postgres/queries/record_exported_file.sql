UPDATE walrus.table_reload
SET chunk_no = chunk_no + 1,
    first_lsn = COALESCE(first_lsn, start_lsn),
    updated_at = now()
WHERE reload_id = $1
  AND status = 'exporting'
  AND start_lsn = $2
  AND schema_version = $3
  AND cursor_pk IS NULL
RETURNING chunk_no
