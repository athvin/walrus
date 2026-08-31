SELECT reload_id, epoch, source_schema, source_table,
       flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
       chunk_no, cursor_pk,
       first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
       schema_version, restart_count, lease_holder, error
FROM walrus.table_reload
WHERE reload_id = $1
