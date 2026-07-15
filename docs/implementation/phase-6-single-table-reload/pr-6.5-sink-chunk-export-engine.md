# PR 6.5 — the chunk export engine: watermark → echo → stamped Parquet

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/97

> **Phase:** 6 — single-table reload · **Crates touched:** `pg-sink`, `control` ·
> **Est. size:** L · **Depends on:** PR 6.4 · **Unlocks:** PR 6.6

The heart of the feature, and where walrus gets to be simpler than DBLog
([reload H2](../../single-table-reload.md#h2--one-monolithic-export-is-the-wrong-unit)): per
PK-ordered chunk, INSERT a watermark row → await the echo ⇒ `L_i` → SELECT the chunk on the side
connection → write Parquet with **every row stamped `commit_lsn = lsn = L_i`** → manifest row with
`lsn_end = L_i`, `kind='reload'`, the `reload_id`, the current `schema_version` → advance the
cursor. No stream pause, no chunk buffer, no high watermark: the early stamp plus Phase B's
`(commit_lsn, lsn)` dedup does the window-reconciliation at transform time, and chunk files
literally *sort into the log* through the loader's existing `(lsn_end, id)` claim order. Chunks
are short single statements — no hours-long REPEATABLE READ pinning xmin, and a crash resumes from
the cursor instead of row zero.

## Why — learning objectives

By the end of this PR you will have practised:

- **Keyset pagination done right** — the row-comparison continuation predicate
  `WHERE (pk₁,…,pkₙ) > ($1,…,$n) ORDER BY pk₁,…,pkₙ LIMIT k`, index-friendly and composite-safe,
  vs the OFFSET anti-pattern.
- **The stamping rule, applied** — snapshot rows carrying a conservative low watermark so any
  overlapping stream event wins dedup (H1's algebra, now in code).
- **Reusing a conversion pipeline** — driving the backfill's row→Arrow→Parquet path with a
  different meta stamp instead of writing a second one.
- **Resumable progress as data** — the cursor in `table_reload` is the *only* recovery state;
  if it's advanced before the manifest row is durable you've built a gap, after you've built a
  duplicate (and only one of those is safe here — know why).

## Read first

- `../../single-table-reload.md` — H1 (the stamp), H2 (chunking + "simpler than DBLog"), §5
  step 3 (this loop, verbatim).
- `crates/pg-sink/src/snapshot.rs` — `copy_table` (~104–235): the row→Arrow→Parquet→S3→manifest
  path this engine parameterises; how snapshot rows are stamped at `consistent_point` today.
- `crates/control/src/manifest.rs` — `insert_ready` + module doc (why equal-`lsn_end` files need
  the `id` tiebreak — your chunks rely on it).
- `crates/pg-sink/src/reload_signal.rs` (PR 6.3) — `subscribe` before INSERT; the `Echo` contract.

## Scope

**In scope**

- `ChunkExporter` replacing PR 6.4's stub: the loop above, driven from the cursor in
  `table_reload` (fresh start when `cursor_pk IS NULL`, resume otherwise).
- Chunk SELECT on the controller's side SQL connection: plain autocommit single statements —
  deliberately **no** long transaction, no `REPEATABLE READ`, no snapshot export.
- Stamping: reuse the backfill conversion so every chunk row's `SinkMeta` carries
  `commit_lsn = lsn = L_i` (snapshot-op semantics, same as bootstrap rows); Parquet lands in the
  epoch-prefixed S3 layout; manifest row `kind='reload'`, `reload_id`, `lsn_start = lsn_end = L_i`,
  current registry `schema_version`.
- Echo timeout (config `reload_echo_timeout_secs`): expiry ⇒ `fail(reload_id, "echo timeout —
  is walrus.reload_signal in the publication?")` — the loud version of H11's silent failure.
- Drain detection: a chunk returning fewer than `reload_chunk_rows` rows is the last; the
  `export_complete` status flip is **PR 6.9's** (the row simply stays `exporting`, fully drained,
  cursor at end — 6.9 gives it its ending).
- Config: `reload_chunk_rows` (default 10_000, ≥ 1), `reload_echo_timeout_secs`.

**Explicitly deferred** (do *not* build these here)

- `export_complete` + final watermark `H` and crash-resume-at-startup → **PR 6.9**.
- Mid-export DDL detection/restart → **PR 6.8** (this loop assumes one schema_version; 6.8
  enforces it).
- Loader-side anything → **PR 6.6/6.7**. CTID-range fan-out for huge tables → deferred goal §3
  (note the composition point in a comment).

## Files to create / modify

```
crates/pg-sink/src/reload_export.rs     # new — ChunkExporter (the loop)
crates/pg-sink/src/reload.rs            # modify — run_exporter drives ChunkExporter, not the stub
crates/pg-sink/src/config.rs            # modify — reload_chunk_rows, reload_echo_timeout_secs
crates/pg-sink/src/lib.rs               # modify — pub mod reload_export;
crates/pg-sink/tests/reload_export.rs   # new — compose: chunk count/stamps/coverage + resume
```

## Skeleton

```rust
// crates/pg-sink/src/reload_export.rs

/// One table's chunked export (reload §5.3). Owns a side SQL connection; talks to the consume
/// loop only through WatermarkWaiters; never touches the replication connection.
pub struct ChunkExporter {
    /* side conn · Arc<WatermarkWaiters> · control pool · object store · table shape
       (PgRelation at the reload's schema_version) · reload row · config slice */
}

impl ChunkExporter {
    /// Fresh start or cursor resume (H7): loop export_chunk until a short chunk says drained.
    pub async fn run(&mut self) -> Result<(), crate::Error> { todo!() }

    /// subscribe(reload_id, n) → INSERT signal row → await Echo ⇒ L_n (timeout ⇒ fail loudly)
    /// → SELECT next PK slice → Parquet, every row stamped commit_lsn = lsn = L_n → S3 →
    /// manifest (kind='reload', reload_id, lsn_start = lsn_end = L_n, schema_version) →
    /// advance_cursor. Returns rows read; < chunk_rows ⇒ the table is drained.
    async fn export_chunk(&mut self, chunk_no: i64) -> Result<usize, crate::Error> { todo!() }

    /// `WHERE (pk…) > ($…)` row comparison from the jsonb cursor; first chunk has no predicate.
    /// PK columns and their order come from the relation shape — never hardcode.
    fn continuation_sql(&self, cursor: Option<&serde_json::Value>) -> String { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn continuation_sql_is_row_comparison_for_composite_pk() { todo!() }
    #[test] fn first_chunk_has_no_predicate_and_orders_by_full_pk() { todo!() }
    #[test] fn short_chunk_means_drained() { todo!() }
}
```

```rust
// crates/pg-sink/tests/reload_export.rs

/// Seed 2,500 rows, chunk_rows=1,000 ⇒ exactly 3 manifest rows, kind='reload', strictly
/// increasing lsn_end, union of the 3 Parquet files == the table exactly (no dup, no miss),
/// every row's meta commit_lsn == its file's lsn_end.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn chunks_cover_the_table_exactly_with_per_chunk_stamps() { todo!() }

/// Cancel the exporter after chunk 1, run again: chunks 2..3 export, chunk 1 is NOT re-exported.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn resume_from_cursor_never_reexports_completed_chunks() { todo!() }

/// Signal table removed from the publication ⇒ echo timeout ⇒ failed row naming the fix.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn echo_timeout_fails_the_reload_with_publication_hint() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] 2,500 seeded rows at `reload_chunk_rows=1000` produce exactly 3 `kind='reload'` manifest
      rows with strictly increasing `lsn_end`, and DuckDB reading the 3 Parquet files back yields
      exactly the source rows — no duplicate, no miss (compose test 1).
- [x] Every chunk row's `SinkMeta` has `commit_lsn = lsn =` the chunk's `L_i` == the file's
      `lsn_end`; `first_lsn` in `table_reload` equals chunk 1's `L_1` and never changes.
- [x] The embedded cross-check holds on every chunk (`wal_insert_lsn < L_i`; violation counter
      stays 0 through the compose run).
- [x] Resume: a cancelled exporter restarted from the cursor re-exports nothing before the cursor
      (compose test 2); chunk files are content-identical regardless of resume.
- [x] Echo timeout produces `failed` with the publication hint, not a hang (compose test 3).
- [x] Concurrent writes to the table **during** the export end correct in raw math: overlapping
      stream events carry `commit_lsn > L_i` of any chunk containing the same PK (assert on the
      Parquet/meta level here; the mirror-level proof is PR 6.7's).
- [x] The replication stream for other tables never stalls during a full export (lag assertion).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test reload_export -- --ignored`

## What completed looks like

```
$ docker compose up --wait && just reload table='public.orders'   # orders has 2,500 rows
$ psql $CONTROL_URL -c "SELECT id, kind, reload_id, row_count, lsn_end FROM walrus.file_manifest
                        WHERE source_table='orders' AND kind='reload' ORDER BY lsn_end"
 id  |  kind  | reload_id | row_count | lsn_end
-----+--------+-----------+-----------+-----------
 101 | reload |         7 |      1000 | 0/1B00028
 102 | reload |         7 |      1000 | 0/1B000E0
 103 | reload |         7 |       500 | 0/1B00198

$ psql $CONTROL_URL -c "SELECT chunk_no, cursor_pk, first_lsn FROM walrus.table_reload WHERE reload_id=7"
 chunk_no |   cursor_pk    | first_lsn
----------+----------------+-----------
        3 | {"id": 2500}   | 0/1B00028
```

(The row stays `exporting` — PR 6.9 gives it `export_complete`.)

## Hints & gotchas

- **Cursor-vs-manifest ordering**: make the manifest `insert_ready` and `advance_cursor` the same
  control-pg transaction, or advance the cursor strictly *after* the manifest row is durable. A
  crash between "file in S3" and "cursor advanced" then re-exports one chunk — a duplicate the
  dedup algebra eats. The reverse order builds a **gap** nothing can heal. Duplicates are safe;
  gaps are not.
- Re-running a chunk after a crash re-INSERTs the same `(reload_id, chunk_no)` signal row — a PK
  conflict. `ON CONFLICT DO NOTHING` won't produce a fresh echo; use an attempt-scoped chunk key
  or delete-and-reinsert; either way the *new* echo's `L` is the stamp for the re-exported chunk.
- Read the PK values for the cursor from the **last row of the chunk you just wrote** — not a
  separate `MAX()` query (racy).
- `L_i` from a signal committed *before* the chunk SELECT means every chunk row was committed at
  some `C` — where? If `C ≤ L_i` the stream event (if any) at `C` loses dedup to nothing (chunk
  row wins nothing — they're equal-or-newer); if `C > L_i` the stream wins. Either way convergent.
  Write this as a comment above the stamp — it's the whole proof.
- Serialize cursor PK values through the same text representation the decoder uses for those
  types — a jsonb number round-trip that loses precision on `bigint` PKs is a silent gap.
- Interleave politely: `max_concurrent_reloads` bounds tables, `reload_chunk_rows` bounds each
  statement; there is no need for sleeps between chunks yet — measure first (phase-5 discipline).

## References

- Design: `../../single-table-reload.md` H1, H2, §5 step 3, §6 (race note from PR 6.3);
  `../../architecture.md#17-snapshot--backfill-bootstrap` (how bootstrap stamps at
  `consistent_point` — the shape being generalised); DBLog paper + Debezium incremental-snapshots
  blog (design-doc references).
- Prev: [PR 6.4](./pr-6.4-sink-reload-controller.md) ·
  Next: [PR 6.6](./pr-6.6-loader-pause-claims.md) · [Roadmap](../README.md)
