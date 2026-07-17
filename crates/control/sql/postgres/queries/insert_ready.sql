INSERT INTO walrus.file_manifest
    (epoch, source_schema, source_table, s3_uri, kind, row_count,
     lsn_start, lsn_end, schema_version, status, reload_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'ready', $10)
RETURNING id
