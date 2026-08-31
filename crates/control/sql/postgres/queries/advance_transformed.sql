INSERT INTO walrus.loader_checkpoint
    (epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn)
VALUES ($1, $2, $3, $4, $4)
ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
    SET transformed_lsn =
            GREATEST(walrus.loader_checkpoint.transformed_lsn, EXCLUDED.transformed_lsn),
        updated_at = now()
