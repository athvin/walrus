SELECT count(m.id) AS pending
FROM walrus.table_reload r
JOIN walrus.table_ownership o
  ON o.epoch = r.epoch
 AND o.source_schema = r.source_schema
 AND o.source_table = r.source_table
LEFT JOIN walrus.file_manifest m
  ON m.epoch = r.epoch
 AND m.source_schema = r.source_schema
 AND m.source_table = r.source_table
 AND m.lsn_end <= r.final_lsn
WHERE r.reload_id = $1
  AND r.status = 'publishing'
  AND r.publication_nonce = $2
  AND r.publisher_owner_pod = $3
  AND r.publisher_fencing_token = $4
  AND o.owner_pod = $3
  AND o.fencing_token = $4
  AND o.lease_expiry > statement_timestamp()
GROUP BY r.reload_id
