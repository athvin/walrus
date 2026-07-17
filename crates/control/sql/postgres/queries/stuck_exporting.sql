SELECT reload_id, lease_holder
FROM walrus.table_reload
WHERE epoch = $1 AND status = 'exporting'
  AND lease_expiry IS NOT NULL AND lease_expiry < now()
ORDER BY reload_id
