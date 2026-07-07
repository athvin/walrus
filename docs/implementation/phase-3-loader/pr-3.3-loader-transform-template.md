# PR 3.3 — The raw→mirror transform SQL template + pure in-memory-DuckDB tests (crown jewel)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/56

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` · **Est. size:** L ·
> **Depends on:** PR 3.2 · **Unlocks:** PR 3.4

This is the correctness heart of the whole loader — and the single most-tested PR in the curriculum.
You ship the **parameterized transform SQL as one `const` / `.sql` template** (dedup window →
delete-after-ranking → three-branch `MERGE`), and prove it against `Connection::open_in_memory()` with
**hermetic** unit tests that replay every worked case from `walrus-loader.md §6`: `i→d→i`, `i→d`,
`i→u→d`, phantom `d`, `i(A)→d→i(B)`, and `d→i` on a pre-seeded key. No S3, no Postgres, no network —
pure SQL on scripted `<table>_raw` rows. The template you write here is the *exact* SQL Phase B will
run in PR 3.4, so test and production share one source of truth.

## Why — learning objectives

By the end of this PR you will have practised:

- **Window dedup-to-latest** — `row_number() OVER (PARTITION BY <pk> ORDER BY commit_lsn DESC, lsn DESC)`
  with `QUALIFY rn=1` and `* EXCLUDE (rn)`.
- **The delete-after-ranking rule** — keep `op='d'` rows *in* the window; let the **winner's `op`**
  decide, so a superseded earlier insert can never resurrect a deleted key.
- **`MERGE INTO` (DuckDB ≥ 1.4)** — the three branches (`MATCHED AND op='d' → DELETE`,
  `MATCHED → UPDATE`, `NOT MATCHED AND op<>'d' → INSERT`) encoding the collapse rule.
- **Composite-PK SQL generation** — expanding `<pk>` to `PARTITION BY k1,k2…`, `ON t.k1=s.k1 AND …`
  from the `schema_registry` key list, never assuming one column.
- **Hermetic SQL testing** — running the *production* template on in-memory DuckDB.

## Read first

- `../../walrus-loader.md#52-dedup-to-latest-per-primary-key-the-window` and
  `#53-deletes-are-filtered-after-ranking-the-resurrection-guard` — the window + the resurrection guard.
- `../../walrus-loader.md#54-the-merge-branches-and-composite-keys` — the three MERGE branches, the
  composite-PK generalization, and the `< 1.4.0` fallback.
- `../../walrus-loader.md#6-intra-batch-pk-churn--insert--delete--insert-worked-in-full` — the worked
  `pk=42` table and the **variant matrix** (§6.2) — your assertions come straight from here.
- `../../architecture.md#21-the-raw-to-mirror-transform-model` — the normative Intra-batch PK-churn
  collapse rule and the canonical SQL.

## Scope

**In scope**

- `transform.sql` (a `const &str` template, or an embedded `.sql`) parameterized by `<table>`, the
  `<pk>` column list, the non-key column list, and `:transformed_lsn`.
- A `TransformSql` builder that renders the template for a given `PgRelation` (single **and** composite
  PK), producing the dedup `CREATE TEMP TABLE _batch` + the `MERGE INTO`.
- Hermetic `#[test]`s over `open_in_memory()` covering the §6 worked cases and the counterfactual.

**Explicitly deferred** (do *not* build these here)

- Wiring the template into the live loop + advancing `transformed_lsn` → **PR 3.4**.
- TRUNCATE `(Ct, Lt)` pre-wipe + the `> (Ct, Lt)` window filter → **PR 3.5**.
- Unchanged-TOAST back-scan (the step-2 substitution) → **PR 3.6**.
- The per-PK `_applied_commit_lsn` guard columns → **PR 3.7** (write the plain 3-branch MERGE now).
- The `< 1.4.0` `ON CONFLICT` fallback → optional; target `MERGE INTO` and pin DuckDB 1.4.x.

## Files to create / modify

```
crates/loader/src/transform.rs     # new — TransformSql builder + embedded template
crates/loader/src/transform.sql    # new — the parameterized const template (single source of truth)
crates/loader/tests/transform.rs   # new — hermetic in-memory-DuckDB unit tests (§6 cases)
# no new Cargo deps (duckdb bundled from PR 3.1)
```

## Skeleton

```rust
// crates/loader/src/transform.rs

/// The transform template, rendered for one table. `{table}`, `{pk_list}`, `{pk_join}`,
/// `{set_cols}`, `{insert_cols}` are substituted; `:transformed_lsn` stays a bound parameter.
pub const TRANSFORM_SQL: &str = include_str!("transform.sql");

pub struct TransformSql { pub table: String, pub pk: Vec<String>, pub non_key: Vec<String> }

impl TransformSql {
    pub fn from_relation(rel: &common::PgRelation) -> Self { todo!() }

    /// Render the dedup CREATE TEMP TABLE _batch statement (rn=1 winner per PK, op<>'t').
    pub fn dedup_sql(&self) -> String { todo!() }
    /// Render the MERGE INTO with the three branches; composite-PK join predicate.
    pub fn merge_sql(&self) -> String { todo!() }
}

/// Runs dedup + MERGE against an already-populated <table>_raw. (Phase B calls this in PR 3.4.)
pub fn apply_transform(conn: &duckdb::Connection, t: &TransformSql, transformed_lsn: &common::Lsn)
    -> Result<(), LoaderError> { todo!() }
```

```rust
// crates/loader/tests/transform.rs — HERMETIC: Connection::open_in_memory(), no services.
use duckdb::Connection;

/// helper: create <table> + <table>_raw and seed raw with scripted (pk,op,commit_lsn,lsn,data) rows.
fn seed(conn: &Connection, rows: &[(i64, char, &str, &str, Option<&str>)]) { todo!() }

// §6.1 primary case + §6.3 set-based across many keys
#[test] fn n_keys_insert_delete_insert_each_row_equals_last_insert_and_count_is_n() { todo!() }
// §6.2 variant matrix
#[test] fn insert_then_delete_is_absent() { todo!() }                 // i → d
#[test] fn insert_update_delete_is_absent() { todo!() }               // i → u → d
#[test] fn phantom_delete_for_never_seen_key_is_noop() { todo!() }    // d (NOT MATCHED AND op<>'d')
#[test] fn insert_a_delete_insert_b_resolves_to_b() { todo!() }       // i(A) → d → i(B), A≠B
#[test] fn delete_then_insert_on_preseeded_key_updates_to_insert() { todo!() } // d → i (MATCHED UPDATE)
// §5.3 the counterfactual: prove pre-filtering op='d' would resurrect
#[test] fn deletes_filtered_after_ranking_never_resurrect_a_deleted_key() { todo!() }
// composite PK
#[test] fn composite_pk_partition_and_join_expand_to_all_key_columns() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] **`i→d→i` across N keys:** every mirror row equals its **last** insert and `COUNT(*) = N` — zero
      delete survivors, zero first-insert survivors.
- [x] **`i→d`, `i→u→d`, phantom `d`** → the key is **absent** from the mirror.
- [x] **`i(A)→d→i(B)` (A≠B)** → mirror = `B`; **`d→i` on a pre-seeded key** → mirror = the insert data
      (last-tuple-wins across a tombstone, MATCHED-UPDATE branch).
- [x] A test proves that **pre-filtering `op='d'` before the window would resurrect** a deleted key,
      and the shipped template (filter *after* ranking) does not.
- [x] The template renders correctly for a **composite** PK (`PARTITION BY k1,k2`; `ON t.k1=s.k1 AND
      t.k2=s.k2`), generated from the key list — not hard-coded to one column.
- [x] The tests run against `Connection::open_in_memory()` — **no docker compose**, no Postgres, no S3.
- [x] The transform ships as **one** template used by both the tests and (in PR 3.4) the loader.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader --test transform` (and `--workspace` stays green) — **no `--ignored`**,
        these are pure and fast.

## Hints & gotchas

- **Rank everything, then let the winner's `op` decide.** The `WHERE op <> 't'` in the window excludes
  *truncate* rows only (PR 3.5); deletes stay in so `rn=1` can legitimately land on a delete.
- `QUALIFY rn = 1` reads cleaner than a subquery, but you still need `* EXCLUDE (rn)` (or an explicit
  column list) so `rn` doesn't leak into `_batch` / the `MERGE`.
- The `NOT MATCHED AND s.op <> 'd'` guard is what makes a **phantom delete** a no-op — without it a
  MERGE dialect might try to INSERT a tombstone. Assert the phantom-`d` case explicitly.
- Give the two inserts in `i(A)→d→i(B)` **distinct data** so a passing test actually proves *B* won,
  not just "some insert won".
- Order strictly by the **tuple** `(commit_lsn DESC, lsn DESC)`. `commit_lsn` is delivery order; `lsn`
  breaks ties within one txn (and orders same-commit ops — which PR 3.5's TRUNCATE case leans on).
- Keep `<pk>` generation driven by the `schema_registry` key list even in tests — a single-column test
  that hard-codes `PARTITION BY id` won't catch a composite-PK rendering bug.
- ⚠ Never let a **normalized** type into the partition/join key (DuckDB normalizes `INTERVAL` for
  equality). It's a non-risk because the key is always the source PK, but keep interval/range columns
  out of `<pk>` by construction.

## References

- Design: `../../walrus-loader.md#52-dedup-to-latest-per-primary-key-the-window`,
  `#53-deletes-are-filtered-after-ranking-the-resurrection-guard`,
  `#54-the-merge-branches-and-composite-keys`,
  `#6-intra-batch-pk-churn--insert--delete--insert-worked-in-full`;
  `../../architecture.md#21-the-raw-to-mirror-transform-model`.
- Prev: [PR 3.2](./pr-3.2-loader-phase-a-append.md) ·
  Next: [PR 3.4](./pr-3.4-loader-phase-b.md) · [Roadmap](../README.md)
