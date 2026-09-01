# DuckLake migration and operations

This document is the authoritative amendment to the original per-table `.duckdb` design. Walrus now
uses DuckDB as a transient execution engine attached to a shared DuckLake. DuckLake stores metadata
in a dedicated PostgreSQL database and durable Parquet data under one S3 prefix. The control-plane
PostgreSQL database remains separate and continues to own manifests, checkpoints, schema history,
reload state, and leases.

The sink is unchanged: it still consumes one lifelong logical replication slot and stages committed
CDC files in S3. The loader's Phase A append, Phase B raw-to-mirror transform, DDL reconciliation,
two watermarks, epoch reset, and reload semantics are also unchanged.

## Storage and query contract

Each source table gets a deterministic internal DuckLake schema derived from
`(source_schema, source_table)`. Epoch is stored in its metadata; a total restart wipes and rebuilds
the same namespace rather than leaking a retired generation. It contains:

- `<table>_raw`: the CDC log used for replay and reconciliation;
- `<table>`: the current mirror, including Walrus's internal applied-LSN guards;
- `_walrus_ingested_files`: the staged-file replay ledger; and
- `_walrus_meta`: schema, epoch, and reload watermarks.

The opaque schemas prevent internal table names from colliding when source schemas contain the same
table name. Readers do not depend on those names. Walrus publishes one stable read view:

```sql
SELECT * FROM walrus.<source_schema>.<table>_current;
```

The view hides internal LSN guard columns. Raw and internal mirror objects are implementation details
and may change. Cross-table reads are not a single source-Postgres transaction snapshot; that relaxed
consistency is inherited from the original per-table design.

## Prerequisites and configuration

Provision these independently:

1. The existing Walrus control PostgreSQL database.
2. A dedicated PostgreSQL database for DuckLake metadata. PostgreSQL 12 or newer is required by
   DuckLake's PostgreSQL catalog integration. Back it up as a catalog, not as the source of the actual
   row data.
3. A durable S3 bucket/prefix for DuckLake data. Keep it separate from the sink's staging prefix and
   grant writer pods read/write/delete/list access. Grant reader identities read/list only.

The release image contains `json`, `httpfs`, `aws`, `postgres`, and `ducklake` extensions installed
against the workspace-pinned DuckDB engine. Runtime downloading and auto-loading are disabled.

Required loader settings:

| Setting | Meaning |
|---|---|
| `WALRUS_DUCKLAKE__CATALOG_URL` | PostgreSQL URI for the dedicated metadata database (Secret) |
| `WALRUS_DUCKLAKE__DATA_PATH` | Stable `s3://bucket/prefix/` object-data root |
| `WALRUS_DUCKLAKE__METADATA_SCHEMA` | PostgreSQL schema owned by DuckLake; default `walrus_ducklake` |
| `WALRUS_DUCKLAKE__ATTACH_NAME` | DuckDB catalog/read prefix; default `walrus` |
| `WALRUS_DUCKLAKE__SNAPSHOT_RETENTION` | Time-travel retention; default `7d` |
| `WALRUS_DUCKLAKE__CLEANUP_GRACE` | Minimum age before unreferenced files are deleted; default `7d` |
| `WALRUS_DUCKLAKE__MAINTENANCE_INTERVAL` | Catalog cleanup cadence; default `24h` |
| `WALRUS_SHARD_COUNT` | Number of deterministic loader shards; begin at `1` |

`DATA_PATH` is persisted in the catalog. The loader attaches with `OVERRIDE_DATA_PATH=false`, so a
misconfigured deployment fails instead of silently moving new data to a different root.

## Catalog migration

Catalog schema changes are an explicit release operation. Normal loader startup uses
`AUTOMATIC_MIGRATION=false`.

For the local stack:

```sh
just up
just ducklake-migrate
```

For Kubernetes, use the same loader image that will run the workload:

```sh
kubectl apply -f deploy/k8s/ducklake-catalog-migrate-job.yaml
kubectl wait --for=condition=complete job/walrus-ducklake-catalog-migrate --timeout=10m
```

Give the migration identity DDL rights in the DuckLake metadata schema. The steady-state writer can
use a narrower role after migrations have completed. Never give analytic readers catalog DDL or S3
write/delete permissions.

## One-replica cutover from `.duckdb`

There is intentionally no file-to-lake importer in Walrus. Rebuild from source through the existing
reload protocol; it exercises the same type, DDL, snapshot-boundary, and crash-recovery paths as normal
operation.

1. Back up the control database and every current `.duckdb` file. Provision and migrate the catalog,
   then verify S3 access from the target workload identity.
2. Keep the sink running. Stop the old loader cleanly and make its files read-only. Do not delete or
   mutate them during the acceptance window.
3. Deploy the DuckLake loader with exactly one replica and `WALRUS_SHARD_COUNT=1`. Its empty lake may
   initially inherit advanced control checkpoints; it is not ready for reader cutover until every
   table reload below completes.
4. Queue rebuilding reloads for all registered tables. In the local harness, run `just reload-all`.
   In production, execute the equivalent `INSERT ... SELECT` from that recipe against control
   PostgreSQL, or request each table with the existing operator interface.
5. Wait until every new `walrus.table_reload` row is `complete`, no table is quarantined, the manifest
   queue is caught up, and loader readiness is green. Validate row counts, key uniqueness, null/error
   rates, representative aggregates, DDL shape, and fresh CDC after each reload's final LSN.
6. Attach a canary reader read-only and compare it with source truth and the frozen file-backed
   result. Move reader traffic only after those checks pass.
7. Keep the old files immutable for the chosen rollback window. Delete them only after the DuckLake
   catalog backup/restore drill, retention pass, and reader soak have succeeded.

Rollback means routing reads back to the frozen `.duckdb` snapshot while stopping DuckLake writers
and diagnosing the new path. Do not restart the old loader writer against advanced control
checkpoints: the frozen file no longer contains the intervening CDC. A writable rollback requires a
fresh source rebuild into the old release or restoration of a matching control-plane backup.

## Read-only DuckDB clients

Readers need the same pinned-compatible DuckLake/PostgreSQL/S3 extensions, a read-only PostgreSQL
catalog role, and read/list access to the DuckLake S3 prefix. Store credentials in temporary or
managed secrets rather than interpolating them into `ATTACH` SQL or logs.

```sql
INSTALL ducklake;
INSTALL postgres;
INSTALL httpfs;
INSTALL aws;
LOAD ducklake;
LOAD postgres;
LOAD httpfs;
LOAD aws;

CREATE TEMP SECRET walrus_catalog (
  TYPE postgres,
  URI 'postgresql://reader:REDACTED@catalog.example/walrus_ducklake'
);

-- Configure an S3 credential-chain or managed S3 secret for the data prefix first.
ATTACH 'ducklake:postgres:' AS walrus (
  META_SECRET 'walrus_catalog',
  METADATA_SCHEMA 'walrus_ducklake',
  META_SCHEMA 'walrus_ducklake',
  READ_ONLY
);

SELECT * FROM walrus.public.orders_current;
```

Do not set `DATA_PATH` on an existing read attachment; DuckLake loads the persisted value from the
catalog. `READ_ONLY` is required even when database grants are also read-only. The supported attach
shape is documented by DuckLake's [connection guide](https://ducklake.select/docs/stable/duckdb/usage/connecting).

## Retention, compaction, and backup

Every table worker periodically merges adjacent Parquet files and rewrites delete-heavy mirror/raw
files after Walrus's logical raw-retention prune. Shard zero alone performs catalog-wide snapshot
expiration, old-file cleanup, and orphan cleanup. DuckLake keeps old files until snapshots are
expired and cleanup runs, so lowering retention can make old time-travel versions and their files
irrecoverable.

Back up these as one recovery unit:

- the dedicated PostgreSQL catalog database; and
- the DuckLake S3 prefix at a compatible point in time/version.

Control PostgreSQL is a separate recovery unit for the replication pipeline. Test restoring all
three components. PostgreSQL catalog maintenance such as routine `VACUUM` remains the operator's
responsibility. DuckLake's [recommended maintenance](https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance)
explains why snapshot expiration and physical cleanup are separate operations.

## Horizontal writer rollout

Rendezvous hashing assigns each `(epoch, schema, table)` to one ordinal in
`0..WALRUS_SHARD_COUNT`. Each writer must hold both its renewable control-plane lease and a
table-keyed, epoch-independent PostgreSQL advisory lock on a dedicated catalog session. If that
session fails, the supervisor cancels all workers; a successor cannot acquire the second fence until
PostgreSQL has dropped the old session's locks.

Scale only after the one-replica cutover has soaked. This is an operator-controlled StatefulSet
reshard, not an HPA target.

To scale from 1 to `N` safely:

1. Set `WALRUS_SHARD_COUNT=N` while the StatefulSet still has one replica.
2. Roll/restart replica 0 and wait for it to become ready. Tables assigned to other ordinals are
   temporarily paused, never multiply written.
3. Set `spec.replicas=N`. New ordinals acquire the paused tables and resume from durable watermarks.
4. Verify that every registered table has exactly one live control lease and that all replicas are
   ready before considering the reshard complete.

To scale down from `N` to `M`, reverse the availability tradeoff safely: first reduce replicas to
`M`, then set `WALRUS_SHARD_COUNT=M` and roll the survivors. Removed-shard tables pause between those
steps. Never change the ring and pod count independently without completing this handoff; the fences
prevent corruption, but a mismatched rollout can leave tables deliberately unowned or make a new pod
wait for an old assignment to drain.

Adding or removing a shard moves only tables whose rendezvous winner changes. There is no data copy:
all replicas attach to the same catalog and object-data root.

## Verification gates

Hermetic tests continue to exercise the native `.duckdb` backend for fast transform/DDL/reload
coverage. The real compatibility contract uses PostgreSQL + MinIO:

```sh
just up
just ducklake-migrate
just ducklake-it
```

That contract covers attachment, S3-backed writes, the split DuckLake `MERGE`, idempotent replay,
the public view, keyless DuckLake table definitions, full rebuild, raw pruning, per-table file
maintenance, snapshot expiration, and cleanup procedure compatibility.
