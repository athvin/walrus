SELECT reload_id, epoch, source_schema, source_table,
       flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
       source_request_id, parent_request_id,
       request_scope AS "request_scope: ReloadScope",
       chunk_no, cursor_pk,
       start_lsn AS "start_lsn: Lsn",
       first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
       schema_version, restart_count, lease_holder, error
FROM walrus.table_reload
WHERE reload_id = $1
