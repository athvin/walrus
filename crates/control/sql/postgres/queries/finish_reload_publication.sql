WITH fenced AS MATERIALIZED (
    SELECT r.reload_id, r.epoch, r.source_schema, r.source_table, r.final_lsn
    FROM walrus.table_reload r
    JOIN walrus.table_ownership o
      ON o.epoch = r.epoch
     AND o.source_schema = r.source_schema
     AND o.source_table = r.source_table
    WHERE r.reload_id = $1
      AND r.status = 'publishing'
      AND r.publication_nonce = $2
      AND r.publisher_owner_pod = $3
      AND r.publisher_fencing_token = $4
      AND o.owner_pod = $3
      AND o.fencing_token = $4
      AND o.lease_expiry > statement_timestamp()
    FOR UPDATE OF r, o
),
advanced AS (
    UPDATE walrus.loader_checkpoint c
    SET raw_appended_lsn = f.final_lsn,
        transformed_lsn = f.final_lsn,
        updated_at = now()
    FROM fenced f
    WHERE c.epoch = f.epoch
      AND c.source_schema = f.source_schema
      AND c.source_table = f.source_table
      AND c.raw_appended_lsn <= f.final_lsn
      AND c.transformed_lsn <= f.final_lsn
    RETURNING c.epoch, c.source_schema, c.source_table
),
finished AS (
    UPDATE walrus.table_reload r
    SET status = 'complete', updated_at = now()
    FROM fenced f, advanced c
    WHERE r.reload_id = f.reload_id
      AND c.epoch = f.epoch
      AND c.source_schema = f.source_schema
      AND c.source_table = f.source_table
    RETURNING r.reload_id
),
recovered AS (
    UPDATE walrus.table_integrity_recovery recovery
    SET status = 'recovered', updated_at = now()
    FROM fenced f, finished done
    WHERE done.reload_id = f.reload_id
      AND recovery.epoch = f.epoch
      AND recovery.source_schema = f.source_schema
      AND recovery.source_table = f.source_table
      AND recovery.status = 'retrying'
      AND recovery.recovery_reload_id = f.reload_id
    RETURNING recovery.epoch
)
SELECT EXISTS (SELECT 1 FROM finished) AS transitioned,
       EXISTS (
         SELECT 1
         FROM walrus.table_reload r
         JOIN walrus.loader_checkpoint c
           ON c.epoch = r.epoch
          AND c.source_schema = r.source_schema
          AND c.source_table = r.source_table
         WHERE r.reload_id = $1
           AND r.status = 'complete'
           AND r.publication_nonce = $2
           AND c.raw_appended_lsn = r.final_lsn
           AND c.transformed_lsn = r.final_lsn
       ) AS already_complete
