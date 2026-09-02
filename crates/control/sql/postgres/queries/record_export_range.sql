UPDATE walrus.table_reload_export_range AS planned
SET status = 'complete',
    file_count = $5,
    row_count = $6,
    completed_at = COALESCE(planned.completed_at, now())
FROM walrus.table_reload AS reload
WHERE planned.reload_id = $1
  AND planned.range_no = $4
  AND planned.exporter_generation = $3
  AND reload.reload_id = planned.reload_id
  AND reload.status = 'exporting'
  AND reload.lease_holder = $2
  AND reload.exporter_generation = $3
  AND reload.lease_expiry > statement_timestamp()
  AND reload.export_snapshot IS NOT NULL
  AND reload.export_sealed_at IS NULL
  AND (
    planned.status = 'planned'
    OR (planned.status = 'complete'
        AND planned.file_count = $5
        AND planned.row_count = $6)
  )
RETURNING planned.range_no
