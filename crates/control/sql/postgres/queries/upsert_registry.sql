INSERT INTO walrus.schema_registry
    (epoch, source_schema, source_table, schema_version, descriptors, columns)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (epoch, source_schema, source_table, schema_version) DO UPDATE
    SET descriptors = EXCLUDED.descriptors, columns = EXCLUDED.columns
