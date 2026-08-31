INSERT INTO walrus.table_reload
    (epoch, source_schema, source_table, flavor, status, restart_count,
     lease_holder, lease_expiry)
SELECT epoch, source_schema, source_table, flavor, 'exporting', $2,
       lease_holder, lease_expiry
FROM walrus.table_reload
WHERE reload_id = $1
RETURNING reload_id
