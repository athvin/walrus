DELETE FROM walrus.file_manifest
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
  AND kind <> 'reload' AND lsn_end <= $4
