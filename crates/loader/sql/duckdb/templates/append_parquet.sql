INSERT INTO "{table}_raw" ({quoted}, "_walrus_op", "_walrus_commit_lsn", "_walrus_lsn", "_walrus_sink_processed_at")
SELECT {quoted}, json_extract_string(walrus_pg_sink_meta, '$.op'), {commit_lsn_expr}, json_extract_string(walrus_pg_sink_meta, '$.lsn'), json_extract_string(walrus_pg_sink_meta, '$.sink_processed_at')
FROM read_parquet('{uri}') ON CONFLICT DO NOTHING
