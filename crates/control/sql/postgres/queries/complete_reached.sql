UPDATE walrus.table_reload r
SET status = 'complete', updated_at = now()
FROM walrus.loader_checkpoint c
WHERE r.epoch = $1 AND r.source_schema = $2 AND r.source_table = $3
  AND r.status = 'export_complete'
  AND c.epoch = r.epoch AND c.source_schema = r.source_schema
  AND c.source_table = r.source_table
  AND r.final_lsn <= c.transformed_lsn
RETURNING r.reload_id
