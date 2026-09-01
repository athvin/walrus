INSERT INTO walrus.ddl_manifest
    (epoch, source_audit_id, source_schema, source_table, c_lsn, c_event, c_tag,
     schema_version, c_rel_oid, c_columns, c_dropped, c_ddl_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (epoch, source_audit_id) DO UPDATE
SET source_schema = EXCLUDED.source_schema,
    source_table = EXCLUDED.source_table,
    c_lsn = EXCLUDED.c_lsn,
    c_event = EXCLUDED.c_event,
    c_tag = EXCLUDED.c_tag,
    schema_version = EXCLUDED.schema_version,
    c_rel_oid = EXCLUDED.c_rel_oid,
    c_columns = EXCLUDED.c_columns,
    c_dropped = EXCLUDED.c_dropped,
    c_ddl_text = EXCLUDED.c_ddl_text
RETURNING id
