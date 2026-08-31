UPDATE walrus.table_reload
SET status = 'requested', lease_holder = NULL, lease_expiry = NULL, updated_at = now()
WHERE reload_id = $1 AND lease_holder = $2 AND status = 'exporting'
