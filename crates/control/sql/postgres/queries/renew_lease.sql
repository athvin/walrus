UPDATE walrus.table_reload
SET lease_expiry = now() + make_interval(secs => $3), updated_at = now()
WHERE reload_id = $1 AND lease_holder = $2 AND status = 'exporting'
