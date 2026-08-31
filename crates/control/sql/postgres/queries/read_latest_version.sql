SELECT MAX(schema_version) AS "max_version"
FROM walrus.schema_registry
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
