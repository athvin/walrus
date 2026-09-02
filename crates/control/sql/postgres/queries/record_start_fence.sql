WITH candidate AS MATERIALIZED (
    SELECT reload_id
    FROM walrus.table_reload
    WHERE reload_id = $1
      AND (
        (
          status = 'exporting'
          AND (start_lsn IS NULL OR start_lsn = $2)
          AND (schema_version IS NULL OR schema_version = $3)
        )
        OR EXISTS (
          SELECT 1
          FROM walrus.table_reload_marker existing
          WHERE existing.reload_id = walrus.table_reload.reload_id
            AND existing.marker_kind = 'baseline'
            AND existing.lsn = $2
            AND existing.schema_version = $3
        )
      )
      AND (COALESCE(source_request_id, parent_request_id) IS NULL
           OR COALESCE(source_request_id, parent_request_id) = $4)
      AND source_schema = $5
      AND source_table = $6
    FOR UPDATE
), marker AS (
    INSERT INTO walrus.table_reload_marker
        (reload_id, marker_kind, lsn, schema_version)
    SELECT reload_id, 'baseline', $2, $3
    FROM candidate
    ON CONFLICT (reload_id, marker_kind) DO UPDATE
    SET lsn = EXCLUDED.lsn
    WHERE walrus.table_reload_marker.lsn = EXCLUDED.lsn
      AND walrus.table_reload_marker.schema_version = EXCLUDED.schema_version
    RETURNING reload_id
), frozen AS (
    UPDATE walrus.table_reload AS r
    SET start_lsn = $2,
        schema_version = COALESCE(r.schema_version, $3),
        updated_at = now()
    FROM marker
    WHERE r.reload_id = marker.reload_id
      AND r.status = 'exporting'
    RETURNING r.reload_id
)
SELECT reload_id FROM marker
