INSERT INTO walrus.table_reload (epoch, source_schema, source_table, flavor)
VALUES ($1, $2, $3, $4)
RETURNING reload_id
