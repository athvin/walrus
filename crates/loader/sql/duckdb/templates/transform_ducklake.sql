-- DuckLake variant of transform.sql. DuckLake currently permits only one matched UPDATE/DELETE
-- action per MERGE, so deletes and upserts are split while remaining in the caller's one ACID
-- transaction/snapshot.
{truncate_wipe}
CREATE OR REPLACE TEMP TABLE _batch AS
WITH winners AS (
    SELECT * FROM "{table}_raw"
    WHERE "_walrus_op" <> 't' AND "_walrus_commit_lsn" >= '{after_lsn}'{truncate_bound}
    QUALIFY row_number() OVER (
        PARTITION BY {pk_list}
        ORDER BY "_walrus_commit_lsn" DESC, "_walrus_lsn" DESC
    ) = 1
)
SELECT {resolved_select}
FROM winners s
LEFT JOIN "{table}" t ON {pk_join};

MERGE INTO "{table}" AS t
USING (SELECT * FROM _batch WHERE "_walrus_op" = 'd') AS s
ON {pk_join}
WHEN MATCHED AND {guard} THEN DELETE;

MERGE INTO "{table}" AS t
USING (SELECT * FROM _batch WHERE "_walrus_op" <> 'd') AS s
ON {pk_join}
WHEN MATCHED AND {guard} THEN UPDATE SET {set_cols}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
