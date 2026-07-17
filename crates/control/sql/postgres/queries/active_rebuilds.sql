SELECT reload_id, epoch, source_schema, source_table,
       flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
       chunk_no, cursor_pk,
       first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
       schema_version, restart_count, lease_holder, error
FROM walrus.table_reload
WHERE epoch = $1 AND flavor = 'reload' AND status IN ('requested', 'exporting')
ORDER BY reload_id
