SELECT first_lsn AS "first_lsn: Lsn"
FROM walrus.table_reload
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
  AND flavor = 'reload'
  AND status IN ('requested', 'exporting', 'export_complete')
  AND first_lsn IS NOT NULL
ORDER BY reload_id DESC
LIMIT 1
