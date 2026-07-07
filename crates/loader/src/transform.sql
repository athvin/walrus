-- The raw→mirror transform (loader §5–§6, architecture §21) — the single source of truth run by both
-- the hermetic tests and Phase B. Rendered per table by `transform.rs`.
--
-- Step 0 — TRUNCATE pre-step (§5.5). A `t` row carries no tuple/PK, so it can't be a MERGE branch:
-- wipe the WHOLE mirror as of the latest truncate `(Ct, Lt)` in the tail (all rows, incl. earlier
-- cycles — the source table was emptied); the window below only repopulates rows STRICTLY after that
-- tuple. `{truncate_wipe}` is empty when the tail has no truncate.
{truncate_wipe}
-- Step 1+2 — dedup to the WINNER per PK (latest by the TUPLE (commit_lsn, lsn) DESC; deletes stay in,
-- the winner's op decides — the resurrection guard §5.3; the truncate boundary is the TUPLE, never the
-- scalar commit_lsn), then RESOLVE unchanged-TOAST (§5.6): pgoutput's `'u'` means "column not modified,
-- value absent" — NOT a real NULL. For each column named in the winner's `unchanged_toast` meta list,
-- substitute the last NON-sentinel value for that PK from `<table>_raw` **at or before** the winner's
-- tuple, falling back to the current mirror value LAST. A mirror-only lookup loses the value when the
-- setter and the unchanged-TOAST update land in the SAME batch (mirror has no row for that PK yet).
CREATE OR REPLACE TEMP TABLE _batch AS
WITH winners AS (
    SELECT * FROM "{table}_raw"
    WHERE "_walrus_op" <> 't' AND "_walrus_commit_lsn" > '{after_lsn}'{truncate_bound}
    QUALIFY row_number() OVER (
        PARTITION BY {pk_list}
        ORDER BY "_walrus_commit_lsn" DESC, "_walrus_lsn" DESC
    ) = 1
)
SELECT {resolved_select}
FROM winners s
LEFT JOIN "{table}" t ON {pk_join};

-- Step 3 — collapse into the mirror. Three branches encode the intra-batch PK-churn collapse rule:
--   MATCHED AND op='d'      → DELETE (the winner is a tombstone)
--   MATCHED                 → UPDATE (last-tuple-wins, incl. d→i)
--   NOT MATCHED AND op<>'d' → INSERT (new key; a phantom delete for an unseen key is a no-op)
MERGE INTO "{table}" AS t
USING _batch AS s
ON {pk_join}
WHEN MATCHED AND s."_walrus_op" = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET {set_cols}
WHEN NOT MATCHED AND s."_walrus_op" <> 'd' THEN INSERT ({insert_cols}) VALUES ({insert_vals});
