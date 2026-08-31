SELECT id, epoch, source_schema, source_table,
       c_lsn AS "c_lsn: Lsn", c_event, c_tag, schema_version
FROM walrus.ddl_manifest
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND c_lsn > $4
ORDER BY c_lsn, id
