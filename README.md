# walrus

A Postgres WAL → DuckDB replication service in Rust, built for Kubernetes. It streams the
Postgres logical-replication WAL, stages changes as Apache Arrow / Parquet in S3, appends them
into a per-table `<table>_raw` CDC log in DuckDB, and transforms that log into a `<table>`
current-state mirror on a user-chosen cadence.

> **Status:** design phase. The architecture sketch is in
> [`docs/architecture.md`](docs/architecture.md) — read and critique it before any code
> lands.

## Shape (see the design doc for detail)

- **`walrus-pg-sink`** — reads the WAL in memory, batches, converts Postgres → Arrow →
  Parquet, dumps to S3, records file locations + LSN ranges in a control table, and advances
  the replication slot only after that's durable (so the WAL can't run away).
- **`walrus-loader`** — polls the control table on a cadence, pulls Parquet from S3, **appends
  each CDC row verbatim into a `<table>_raw` log** (keeping `walrus_pg_sink_meta`), then
  **transforms that log into `<table>`** — dedup-to-latest by PK/LSN, then `MERGE` upsert/delete —
  the current-state mirror. Two DuckDB tables per file, not one MERGE target.

Licensed under [MIT](LICENSE).
