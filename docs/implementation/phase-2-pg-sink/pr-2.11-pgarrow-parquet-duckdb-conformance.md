# PR 2.11 — Parquet write + DuckDB read-back conformance harness (Tier-1)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/31

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow` · **Est. size:** L ·
> **Depends on:** PR 2.10 · **Unlocks:** PR 2.12 (and every Tier-2/3 PR reuses this harness)

This is the keystone PR of Phase 2b. It writes the `RecordBatch` to **Parquet** with arrow-rs, then proves
the mapping by reading that Parquet back through an **in-process DuckDB** and asserting *both* the inferred
DuckDB type *and* the value. It introduces the `duckdb` bundled compile **once**, behind a `conformance`
cargo feature and a CI cache — the one place the whole "DuckDB reads Parquet-native types" rule is
mechanically verified. Every later type PR (2.12–2.16) adds cases to this harness rather than re-inventing it.

## Why — learning objectives

By the end of this PR you will have practised:

- **arrow-rs `ArrowWriter` + `WriterProperties`** — compression and, critically, *not* corrupting the temporal
  logical types (MICROS, `isAdjustedToUTC`).
- **Feature-gated heavy dependencies** — `duckdb` (bundled C++) behind `--features conformance`, kept out of
  the default build so `cargo test` stays fast.
- **Golden round-trip testing** — write → `read_parquet` → assert `typeof(col)` **and** `col`, the only proof
  that the byte we wrote is the DuckDB type we intended.
- **CI caching of a bundled native build** — compiling DuckDB once and caching it.

## Read first

- [`../../walrus-pg-sink.md` §2.1](../../walrus-pg-sink.md#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types)
  — DuckDB ignores the `ARROW:schema` metadata and reads native Parquet logical types; MICROS; arrow-rs owns the bytes.
- [`../../walrus-pg-sink.md` §2.8](../../walrus-pg-sink.md#28-round-trip-conformance-tests-the-seams-that-must-be-proven)
  — the exact seam list: INT(16) annotation, Decimal(p,s), `timestamptz` vs `timestamp` (`isAdjustedToUTC`), MICROS.
- [`../README.md`](../README.md) "CI grows with the phases" — the 2.11 row adds the DuckDB-bundled conformance job.

## Scope

**In scope**

- `write_parquet(&RecordBatch, impl Write) -> Result<()>` and `write_parquet_bytes(&RecordBatch) -> Result<Vec<u8>>`.
- `default_writer_properties() -> WriterProperties` (compression; leave temporal coercion to arrow-rs's native MICROS output).
- A `conformance` feature adding `duckdb` (bundled) and a reusable test harness `read_parquet_rows(bytes, sql)`.
- Tier-1 conformance assertions: bool, smallint (INT(16)), int/bigint, float/double, `Decimal(p,s)`, utf8,
  bytea→BLOB, date, time (MICROS), `timestamp` vs `timestamptz` (`isAdjustedToUTC`), json.

**Explicitly deferred** (do *not* build these here)

- Tier-2/3 conformance cases → the PR that adds each type (2.12–2.16) appends to *this* harness.
- Writing to S3 / `object_store` (this PR writes to memory/temp files only) → **PR 2.24**.
- Compression tuning / row-group sizing knobs → sink batching, **PR 2.23–2.24**.

## Files to create / modify

```
crates/pg-to-arrow/Cargo.toml         # + parquet = "54"; [features] conformance = ["dep:duckdb"];
                                       #   duckdb = { version = "1", features = ["bundled"], optional = true }
                                       #   [dev-dependencies] tempfile = "3"
crates/pg-to-arrow/src/parquet.rs      # new: write_parquet, write_parquet_bytes, default_writer_properties
crates/pg-to-arrow/src/lib.rs          # + pub mod parquet;
crates/pg-to-arrow/tests/conformance.rs # new (#![cfg(feature = "conformance")]): harness + Tier-1 cases
.github/workflows/ci.yml               # + conformance job (feature-gated, sccache/registry cache for bundled duckdb)
```

## Skeleton

```rust
// crates/pg-to-arrow/src/parquet.rs
use arrow::array::RecordBatch;
use parquet::file::properties::WriterProperties;
use crate::error::Error;

/// arrow-rs writer settings: compression + arrow's native MICROS temporal encoding (no NANOS/MILLIS coercion).
pub fn default_writer_properties() -> WriterProperties { todo!() }

/// Stream one batch to Parquet using the walrus writer properties.
pub fn write_parquet<W: std::io::Write + Send>(batch: &RecordBatch, sink: W) -> Result<(), Error> { todo!() }

/// Convenience: write one batch to an in-memory Parquet buffer.
pub fn write_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>, Error> { todo!() }
```

```rust
// crates/pg-to-arrow/tests/conformance.rs
#![cfg(feature = "conformance")]

/// Write `bytes` to a temp .parquet, then run `sql` (which references read_parquet(?)) in in-process DuckDB.
/// Returns each row as (duckdb_typeof, rendered_value) for the projected column.
fn read_parquet_rows(bytes: &[u8], sql: &str) -> Vec<(String, String)> { todo!() }

#[test] fn smallint_reads_back_as_int16_smallint() { todo!() }        // typeof == 'SMALLINT'/'INTEGER'
#[test] fn decimal_10_2_reads_back_as_decimal_10_2() { todo!() }
#[test] fn timestamptz_is_adjusted_to_utc() { todo!() }               // typeof == 'TIMESTAMP WITH TIME ZONE'
#[test] fn timestamp_is_not_adjusted() { todo!() }                    // typeof == 'TIMESTAMP'
#[test] fn time_is_micros() { todo!() }
#[test] fn bytea_reads_back_as_blob() { todo!() }
#[test] fn json_is_verbatim() { todo!() }
#[test] fn bool_int_float_roundtrip_type_and_value() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `write_parquet_bytes` produces a valid Parquet file arrow-rs can read back to an equal `RecordBatch`.
- [x] Under `--features conformance`, each Tier-1 type write→`read_parquet`→asserts **`typeof(col)`** *and* the value.
- [x] `timestamptz` reads back as DuckDB `TIMESTAMP WITH TIME ZONE` and `timestamp` as `TIMESTAMP` (the
      `isAdjustedToUTC` distinction), both at microsecond resolution.
- [x] `numeric(10,2)` reads back as DuckDB `DECIMAL(10,2)` with the exact value; `bytea` reads back as `BLOB`.
- [x] Default build (`cargo test -p pg-to-arrow`) does **not** compile `duckdb`; the conformance tests only
      run under the feature.
- [x] CI gains a `conformance` job that builds bundled DuckDB with a cache and runs the feature tests green.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-to-arrow`
  - [x] `cargo test -p pg-to-arrow --features conformance` (the write→read_parquet→typeof assertions)

## Hints & gotchas

- **DuckDB ignores arrow-rs's `ARROW:schema` metadata.** Your assertion target is `typeof(col)` from
  `read_parquet`, *not* the round-tripped Arrow schema — the latter can be right while DuckDB still sees a BLOB.
- The bundled `duckdb` build is minutes-long cold; put it behind the feature so `cargo test` and clippy on
  the default set stay quick, and cache the compiled artifact in CI (this is the *only* PR that pays the cost).
- Do **not** hand-tune temporal coercion in `WriterProperties`: arrow-rs already emits `TIMESTAMP(MICROS,
  isAdjustedToUTC=…)` from `Timestamp(Microsecond, tz)`. Coercing to NANOS/MILLIS is exactly the bug §2.1 warns about.
- `read_parquet('file')` in DuckDB is simplest with a real temp path (`tempfile`); an httpfs/in-memory route
  is more setup than this harness needs.
- Keep `read_parquet_rows` generic (takes SQL) so PRs 2.12–2.16 add one `#[test]` each without touching the harness.
- Pin `arrow`, `parquet`, and `duckdb` to exact versions — a minor bump can change a logical-type annotation
  and quietly flip a `typeof` (this is why PR 2.16's UUID mapping is CI-guarded).

## References

- Design: [`../../walrus-pg-sink.md` §2.1](../../walrus-pg-sink.md#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types),
  [§2.8](../../walrus-pg-sink.md#28-round-trip-conformance-tests-the-seams-that-must-be-proven).
- Prev: [PR 2.10](./pr-2.10-pgarrow-tier1-recordbatch.md) · Next: [PR 2.12](./pr-2.12-pgarrow-interval-timetz.md) · [Roadmap](../README.md)
