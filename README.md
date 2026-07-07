# walrus

A Postgres WAL → DuckDB replication service in Rust, built for Kubernetes. It consumes the
Postgres logical-replication WAL over one hand-rolled pgoutput stream (`proto_version 2`,
`streaming 'on'`), stages changes as Apache Arrow / Parquet in S3, appends them
into a per-table `<table>_raw` CDC log in DuckDB, and transforms that log into a `<table>`
current-state mirror on a user-chosen cadence.

> **Status:** design phase. The architecture sketch is in
> [`docs/architecture.md`](docs/architecture.md) — read and critique it before any code
> lands.

## Design docs

- [`docs/architecture.md`](docs/architecture.md) — the master architecture sketch: sink, loader,
  S3 hand-off, slot/WAL safety, snapshot bootstrap, the raw→mirror transform, K8s topology, and the
  verification plan.
- [`docs/walrus-pg-sink.md`](docs/walrus-pg-sink.md) — the **sink** deep-dive: Postgres → Arrow →
  Parquet → DuckDB type conversion, DDL capture via event triggers, and the K8s pod lifecycle
  (incl. graceful shutdown).
- [`docs/walrus-loader.md`](docs/walrus-loader.md) — the **loader** deep-dive: the work-handoff
  contract, commit-gating, the two-phase append→transform, the `insert → delete → insert` collapse,
  and the loader's K8s lifecycle & scaling.
- [`docs/proto-version.md`](docs/proto-version.md) — the empirical pgoutput companion:
  `proto_version 2` + `streaming 'on'` proven byte-by-byte from a live Postgres 16, with a
  reproducible Docker harness.

## Shape (see the design doc for detail)

- **`walrus-pg-sink`** — *non-negotiable job: take work off the WAL and write it to storage,
  fast, so the slot can't run away.* Reads the WAL in memory, batches, converts Postgres → Arrow →
  Parquet, dumps to S3, records file locations + LSN ranges in a control table, and advances the
  replication slot only after that's durable. Flushes when **any** limit trips — cadence
  (`max_fill_ms`), memory footprint (`max_bytes`), or record count (`max_rows`). It moves change
  events verbatim; it does not reconcile them.
- **`walrus-loader`** — *non-negotiable job: reconcile that work into the exact shape the data has
  in Postgres — accuracy over latency, not real-time.* Polls the control table on a cadence,
  pulls Parquet from S3, **appends each CDC row verbatim into a `<table>_raw` log** (keeping
  `walrus_pg_sink_meta`), then **transforms that log into `<table>`** — dedup-to-latest by
  PK/LSN, then `MERGE` upsert/delete — a current-state mirror that matches the source table. Two
  DuckDB tables per file, not one MERGE target.

Licensed under [MIT](LICENSE).
