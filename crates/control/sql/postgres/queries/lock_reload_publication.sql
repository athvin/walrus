SELECT r.epoch, r.source_schema, r.source_table, r.start_lsn, r.final_lsn,
       r.schema_version, r.publication_nonce, r.publisher_owner_pod,
       r.publisher_fencing_token
FROM walrus.table_reload r
JOIN walrus.table_ownership o
  ON o.epoch = r.epoch
 AND o.source_schema = r.source_schema
 AND o.source_table = r.source_table
WHERE r.reload_id = $1
  AND r.status = 'publishing'
  AND r.publication_nonce = $2
  AND r.publisher_owner_pod = $3
  AND r.publisher_fencing_token = $4
  AND o.owner_pod = $3
  AND o.fencing_token = $4
  AND o.lease_expiry > statement_timestamp()
FOR UPDATE OF r, o
