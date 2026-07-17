UPDATE walrus.table_reload
SET status = 'complete', updated_at = now()
WHERE reload_id = $1 AND status = 'export_complete'
