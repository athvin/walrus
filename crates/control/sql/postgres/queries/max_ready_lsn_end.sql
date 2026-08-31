SELECT MAX(lsn_end) AS "max_lsn_end: Lsn"
FROM walrus.file_manifest
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND status = 'ready'
