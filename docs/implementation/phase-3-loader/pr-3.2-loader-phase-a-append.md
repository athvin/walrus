# PR 3.2 — Phase A: claim `ready` files → append verbatim to `<table>_raw` → watermark + delete

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader`, `control` · **Est. size:** M ·
> **Depends on:** PR 3.1 · **Unlocks:** PR 3.3

The loader's ingest half. Phase A **claims** the next `ready` manifest files in commit-LSN order,
reads each Parquet with DuckDB, **appends every row verbatim** into `<table>_raw` (promoting
`op` / `commit_lsn` / `lsn` / `sink_processed_at` from the JSON meta into typed columns), and then — in
**one control-DB transaction** — advances `raw_appended_lsn` **and** deletes the claimed queue rows.
Row-level `ON CONFLICT DO NOTHING` makes a re-run add nothing. No transform yet: this PR ends with a
faithful, idempotent CDC log.

## Why — learning objectives

By the end of this PR you will have practised:

- **Work-queue claiming** — the load-bearing `ORDER BY lsn_end, id` claim query (never
  `lsn_end > raw_appended_lsn`, which would skip equal-`lsn_end` snapshot files).
- **Verbatim append with promoted columns** — `INSERT … SELECT *, json_extract_string(meta, …)
  FROM read_parquet(:uri) ON CONFLICT DO NOTHING`.
- **Cross-database crash safety** — DuckDB and control Postgres can't share a txn, so the watermark
  advance + queue delete are one *Postgres* txn done **after** the DuckDB append commits.
- **Load-bearing idempotency** — why the `<table>_raw` composite PK (source PK + `sink_processed_at` +
  `lsn`) must stay *enforced*, not a backstop.

## Read first

- `../../walrus-loader.md#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue` — the claim
  query and why the queue *deletion* (not the watermark) is what advances the frontier.
- `../../walrus-loader.md#4-two-phase-apply--append-then-transform` — Phase A's exact SQL, the
  single-control-txn advance+delete, and the crash-window table (the two guards).
- `../../walrus-loader.md#51-the-two-tables-and-the-composite-raw-primary-key` — the raw composite PK
  that makes `ON CONFLICT DO NOTHING` correct.
- `../../architecture.md#delivery-semantics-ordering--idempotency` — "that row-level PK is load-bearing,
  not a mere backstop".

## Scope

**In scope**

- `control::file_manifest::claim_ready(… ORDER BY lsn_end, id LIMIT :n)` (extends PR 1.4) +
  `delete_claimed(ids)`.
- Phase A append: for each claimed file in LSN order, `read_parquet(:s3_uri)` → append verbatim to
  `<table>_raw` with the four promoted columns, `ON CONFLICT DO NOTHING`.
- One control-DB txn advancing `raw_appended_lsn = max(claimed lsn_end)` **and** deleting the claimed
  ids together.
- Re-run safety: replaying the same file appends **zero** new rows.

**Explicitly deferred** (do *not* build these here)

- The transform / Phase B (`<table>` mirror) → **PR 3.3 / 3.4**.
- Snapshot-file (`kind='snapshot'`) boundary handling → **PR 3.10** (append them here; they're just
  more `ready` rows).
- Poison / `failed` dead-lettering of a repeatedly-failing file → later hardening; for now surface the
  error and stop the cycle.
- `max_inflight` batching tuning → cadence lives in PR 3.4's loop; claim a fixed `max_files_per_cycle`.

## Files to create / modify

```
crates/loader/src/phase_a.rs        # new — claim → append → advance+delete
crates/loader/src/duck.rs           # modify — append_parquet(uri) into <table>_raw
crates/control/src/file_manifest.rs # modify — claim_ready(ORDER BY lsn_end,id), delete_claimed
crates/loader/tests/phase_a.rs      # new — compose integration test (seeded manifest + Parquet)
# no new Cargo deps (duckdb httpfs is bundled; SET s3_* from config)
```

## Skeleton

```rust
// crates/control/src/file_manifest.rs
pub struct ClaimedFile { pub id: i64, pub s3_uri: String, pub kind: FileKind,
                         pub lsn_end: common::Lsn, pub schema_version: i64 }

/// Load-bearing: ORDER BY lsn_end, id — NEVER `lsn_end > raw_appended_lsn`.
pub async fn claim_ready(pool: &PgPool, key: &TableKey, max_files: i64)
    -> Result<Vec<ClaimedFile>, control::Error> { todo!() }

pub async fn delete_claimed(tx: &mut PgTransaction<'_>, ids: &[i64])
    -> Result<(), control::Error> { todo!() }
```

```rust
// crates/loader/src/duck.rs  (extends PR 3.1)
impl TableDb {
    /// Append one Parquet file verbatim into <table>_raw, promoting op/commit_lsn/lsn/
    /// sink_processed_at out of walrus_pg_sink_meta. Idempotent via ON CONFLICT DO NOTHING.
    pub fn append_parquet(&self, s3_uri: &str) -> Result<u64, LoaderError> {
        // INSERT INTO <table>_raw
        //   SELECT *, json_extract_string(walrus_pg_sink_meta,'$.op'),
        //             json_extract_string(walrus_pg_sink_meta,'$.commit_lsn'),
        //             json_extract_string(walrus_pg_sink_meta,'$.lsn'),
        //             json_extract_string(walrus_pg_sink_meta,'$.sink_processed_at')
        //   FROM read_parquet(:s3_uri) ON CONFLICT DO NOTHING;
        todo!()
    }
}

// crates/loader/src/phase_a.rs
/// One Phase-A pass for one table. Returns the max lsn_end appended (None if the queue was empty).
pub async fn run_phase_a(ctx: &TableCtx) -> Result<Option<common::Lsn>, LoaderError> {
    // 1. claim_ready(ORDER BY lsn_end,id)  2. append each in LSN order (DuckDB txn, commit)
    // 3. ONE control-DB txn: UPDATE raw_appended_lsn = max(lsn_end); DELETE claimed ids
    todo!()
}
```

```rust
// crates/loader/tests/phase_a.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn appends_rows_verbatim_with_promoted_columns_and_meta_intact() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn advances_raw_watermark_and_deletes_the_claimed_manifest_rows() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn re_running_the_same_file_appends_zero_rows() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Seeded `ready` files (reuse a sink Parquet fixture) land in `<table>_raw` **verbatim**:
      `walrus_pg_sink_meta` intact **and** `op`/`commit_lsn`/`lsn`/`sink_processed_at` promoted to typed
      columns.
- [ ] Files are claimed and appended in **`(lsn_end, id)`** order; the claim filter is **not**
      `lsn_end > raw_appended_lsn`.
- [ ] After the append commits, **one** control-DB txn advances `raw_appended_lsn = max(lsn_end)` **and**
      deletes exactly the claimed manifest ids.
- [ ] Re-running the same file (crash-window replay) appends **zero** new rows (`ON CONFLICT DO NOTHING`
      on the enforced composite PK).
- [ ] `<table>` (the mirror) is **untouched** — Phase A never writes it.
- [ ] Docs/comments explain the two-guard idempotency (queue delete + row-level PK) and why the
      watermark can't cover the append-commit-to-queue-delete window.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p loader --test phase_a -- --ignored`
        asserting **`re_running_the_same_file_appends_zero_rows`** and
        **`advances_raw_watermark_and_deletes_the_claimed_manifest_rows`**.

## Hints & gotchas

- `SELECT *` from `read_parquet` must line up column-for-column with `<table>_raw`'s source columns +
  `walrus_pg_sink_meta`; the four promoted columns are appended **after** the star, in the raw table's
  trailing positions. Column mismatch is a silent shift — assert a value, not just a count.
- The composite raw PK includes `sink_processed_at` **and** `lsn`; a millisecond-resolution
  `sink_processed_at` collision between two events on one PK is broken by the always-distinct `lsn`.
  Don't drop `lsn` from the conflict target.
- Advance the watermark to `max(lsn_end)` of the **claimed** batch, not per-file — snapshot files share
  a `lsn_end`, so per-file advancement would be redundant but never wrong; batch-max is cheaper.
- Configure DuckDB S3 access with `SET s3_endpoint/s3_access_key_id/...` (MinIO in compose) once per
  connection at open; `read_parquet('s3://…')` then needs no per-call credentials.
- Keep the DuckDB append and the control-DB txn **strictly ordered**: append-commit *then* Postgres
  txn. A crash between them re-claims the still-`ready` file — which the row PK absorbs.

## References

- Design: `../../walrus-loader.md#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue`,
  `#4-two-phase-apply--append-then-transform`,
  `#51-the-two-tables-and-the-composite-raw-primary-key`;
  `../../architecture.md#delivery-semantics-ordering--idempotency`.
- Prev: [PR 3.1](./pr-3.1-loader-skeleton-bootstrap-lease.md) ·
  Next: [PR 3.3](./pr-3.3-loader-transform-template.md) · [Roadmap](../README.md)
