UPDATE walrus.table_reload
SET status = 'export_complete', final_lsn = $2, updated_at = now()
WHERE reload_id = $1 AND status = 'exporting'
