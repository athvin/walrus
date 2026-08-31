UPDATE walrus.table_reload
SET status = 'exporting',
    lease_holder = $2,
    lease_expiry = now() + make_interval(secs => $3),
    updated_at = now()
WHERE reload_id IN (
    SELECT reload_id FROM walrus.table_reload
    WHERE epoch = $1 AND status = 'requested'
    ORDER BY reload_id
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
RETURNING reload_id, epoch, source_schema, source_table,
          flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
          chunk_no, cursor_pk,
          first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
          schema_version, restart_count, lease_holder, error
