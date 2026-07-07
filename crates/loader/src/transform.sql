-- The raw→mirror transform (loader §5–§6, architecture §21) — the single source of truth run by both
-- the hermetic tests (PR 3.3) and Phase B (PR 3.4). Rendered per table by `transform.rs`:
-- {table} = table name; {pk_list} = PARTITION-BY key columns; {pk_join} = MERGE ON predicate;
-- {set_cols} = MATCHED-UPDATE assignments; {insert_cols}/{insert_vals} = NOT-MATCHED INSERT.
--
-- Step 1 — dedup <table>_raw to the WINNER per PK: the latest change by the tuple
-- (commit_lsn DESC, lsn DESC). `commit_lsn` is delivery order; `lsn` breaks intra-txn ties. **Deletes
-- stay in the window** (only truncate rows, op='t', are excluded — PR 3.5); the winner's op decides, so
-- a superseded earlier insert can never resurrect a deleted key (the resurrection guard, §5.3).
CREATE OR REPLACE TEMP TABLE _batch AS
SELECT * FROM "{table}_raw"
WHERE "_walrus_op" <> 't'
QUALIFY row_number() OVER (
    PARTITION BY {pk_list}
    ORDER BY "_walrus_commit_lsn" DESC, "_walrus_lsn" DESC
) = 1;

-- Step 2 — collapse into the mirror. Three branches encode the intra-batch PK-churn collapse rule:
--   MATCHED AND op='d'  → DELETE  (the winner is a tombstone: remove the key)
--   MATCHED             → UPDATE  (last-tuple-wins over any pre-existing mirror row, incl. d→i)
--   NOT MATCHED AND op<>'d' → INSERT  (a brand-new key; a phantom delete for an unseen key is a no-op)
MERGE INTO "{table}" AS t
USING _batch AS s
ON {pk_join}
WHEN MATCHED AND s."_walrus_op" = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET {set_cols}
WHEN NOT MATCHED AND s."_walrus_op" <> 'd' THEN INSERT ({insert_cols}) VALUES ({insert_vals});
