INSERT INTO walrus.ddl_manifest
    (epoch, source_schema, source_table, c_lsn, c_event, c_tag, schema_version, c_rel_oid, c_columns)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id
