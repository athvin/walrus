# PR 3.8 — DDL apply: additive changes (ADD COLUMN · RENAME · lossless widen · COMMENT)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/61

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader`, `control` · **Est. size:** M ·
> **Depends on:** PR 3.7 · **Unlocks:** PR 3.9

The loader keeps `<table>` at the *exact* current source shape and `<table>_raw` as an *additive*
history superset. Because the sink cuts a fresh Parquet file at every structural change (the
**homogeneous-file rule** — one `schema_version` per file), Phase B applies any pending structural DDL
from `ddl_manifest` + `schema_registry` **before** transforming data past that `schema_version` boundary.
This PR handles the **additive / lossless** classes: `ADD COLUMN`, `RENAME COLUMN`/`RENAME TABLE`
(tracked by `attnum`, not name), lossless/`widening ALTER COLUMN TYPE`, and `COMMENT` (a metadata
revision, mirrored onto `<table>` only). Both tables evolve at the correct LSN relative to data.

## Why — learning objectives

By the end of this PR you will have practised:

- **Schema-diff, not DDL-text replay** — deriving DuckDB `ALTER`s from `new − old` column sets in
  `schema_registry`, never parsing `c_ddl_text`.
- **`attnum`-tracked renames** — resolving a rename by position/attnum so it's unambiguous vs a
  drop+add.
- **Version-gated apply** — applying pending DDL when a file's `schema_version` exceeds the DuckDB
  table's current version, before merging that file.
- **The metadata-vs-structural split** — `COMMENT` mirrors but does not gate/cut a boundary.

## Read first

- `../../architecture.md#per-change-type-handling-schema-evolution-semantics` — the taxonomy table:
  the `COMMENT`, `ADD COLUMN`, `ALTER COLUMN TYPE (widening)`, `RENAME COLUMN`, `RENAME TABLE` rows and
  each one's `<table>` vs `<table>_raw` action.
- `../../walrus-loader.md#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild` — "DDL is applied
  before crossing its LSN"; the homogeneous-file rule; mirror = exact shape, raw = additive superset.
- `../../walrus-pg-sink.md#26-...` (the type-mapping descriptor) — how the loader rebuilds column types
  from `schema_registry` going forward.

## Scope

**In scope**

- `control::ddl_manifest` + `schema_registry` reads: pending structural changes ordered by `c_lsn`.
- A DDL applier that diffs old vs new column sets and emits DuckDB `ALTER TABLE` for: `ADD COLUMN`
  (mirror + nullable on raw), `RENAME COLUMN`/`RENAME TABLE` (both tables, `attnum`-tracked), lossless
  `ALTER COLUMN TYPE` (both tables), `COMMENT ON` (mirror only).
- Version-gating: apply pending DDL for a `schema_version` **before** transforming any file at that
  version (hook into Phase B and bootstrap step 4).

**Explicitly deferred** (do *not* build these here)

- **Destructive** DDL — `DROP COLUMN`, lossy `ALTER COLUMN TYPE` (quarantine), `DROP TABLE` → **PR 3.9**.
- `NOT NULL`/`DEFAULT`/`CHECK` enforcement (recorded for lineage, not enforced in v1) → out of scope
  everywhere in v1; just record.
- `CREATE TABLE` (new table added to publication → new `.duckdb` file) → bootstrap/registry path;
  reference but don't build the snapshot here (that's PR 3.10's neighborhood).

## Files to create / modify

```
crates/loader/src/ddl.rs           # new — diff schema_registry versions → DuckDB ALTERs (additive)
crates/loader/src/phase_b.rs       # modify — apply pending DDL before transforming past a version
crates/loader/src/bootstrap.rs     # modify — step 4 schema-reconcile uses the DDL applier
crates/control/src/ddl_manifest.rs # modify — pending_ddl(c_lsn ordered) + schema_registry version diff
crates/loader/tests/ddl_additive.rs # new — compose/unit per taxonomy row
```

## Skeleton

```rust
// crates/loader/src/ddl.rs
pub enum AdditiveChange {
    AddColumn { name: String, ty: common::TypeDescriptor, nullable: bool },
    RenameColumn { attnum: i32, from: String, to: String },
    RenameTable { from: String, to: String },
    WidenColumn { attnum: i32, name: String, new_ty: common::TypeDescriptor }, // lossless
    Comment { target: CommentTarget, text: Option<String> },                   // metadata only
}

/// Diff the old registry version against the new; produce the ordered additive changes.
pub fn diff_additive(old: &SchemaVersion, new: &SchemaVersion) -> Result<Vec<AdditiveChange>, LoaderError> { todo!() }

/// Apply to BOTH tables per the taxonomy (mirror = exact; raw = additive; COMMENT = mirror only).
pub fn apply_additive(conn: &duckdb::Connection, table: &str, changes: &[AdditiveChange])
    -> Result<(), LoaderError> { todo!() }

/// Ensure the DuckDB tables are at `target_version` before transforming a file at that version.
pub async fn reconcile_to_version(ctx: &TableCtx, target_version: i64) -> Result<(), LoaderError> { todo!() }
```

```rust
// crates/loader/tests/ddl_additive.rs
#[test] fn add_column_mirror_and_raw_nullable_old_rows_null() { todo!() }        // hermetic
#[test] fn rename_column_tracked_by_attnum_not_name() { todo!() }               // hermetic
#[test] fn widening_type_change_casts_in_place_both_tables() { todo!() }        // hermetic
#[test] fn comment_mirrored_onto_mirror_only_not_raw() { todo!() }              // hermetic
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn both_tables_evolve_at_the_correct_lsn_relative_to_data() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `ADD COLUMN` → `<table>` gets the column; `<table>_raw` gets it **nullable**; pre-change rows read
      NULL/default; post-change files align.
- [x] `RENAME COLUMN` / `RENAME TABLE` → both tables renamed, resolved by **`attnum`/position**, never
      as a drop+add.
- [x] A **lossless/widening** `ALTER COLUMN TYPE` → in-place cast on both tables; `pg-to-arrow` maps the
      new type going forward.
- [x] `COMMENT ON` → mirrored onto `<table>` only (not `<table>_raw`); it does **not** cut a
      `schema_version` boundary or gate data.
- [x] Pending DDL is applied **before** transforming any file at the new `schema_version` (no file
      straddles a boundary) — at both bootstrap and steady-state Phase B.
- [x] Derivation is `new − old` schema-diff — **no `c_ddl_text` parsing**.
- [x] Docs/comments explain the homogeneous-file rule and the structural-vs-metadata split.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p loader --test ddl_additive -- --ignored`
        asserting **`both_tables_evolve_at_the_correct_lsn_relative_to_data`**.

## Hints & gotchas

- Resolve renames by **`attnum`**, not name — a `RENAME a → b` followed by `ADD COLUMN a` would look
  like "a is unchanged, b is new" under name-matching and silently corrupt the mapping.
- The mirror gets the *exact* current shape; the raw log only ever **adds** columns / widens / follows
  renames — never destructively. `ADD COLUMN` on raw is **nullable** so old verbatim rows stay valid.
- Apply DDL **inside** the transform's DuckDB transaction boundary discipline: reconcile to the target
  version, then transform files at that version — a crash mid-way re-runs both harmlessly.
- A widening cast that DuckDB *can* do in place is additive; one that can fail is **lossy** → that's PR
  3.9. Classify by the registry type change, and leave a `// TODO(3.9): lossy → quarantine` fallthrough.
- `schema_version` gates **data** (structural); the metadata revision counter (comments/constraints)
  does not — don't bump the data gate on a `COMMENT`.

## References

- Design: `../../architecture.md#per-change-type-handling-schema-evolution-semantics`;
  `../../walrus-loader.md#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild`;
  `../../walrus-pg-sink.md#26-the-type-mapping-descriptor-how-the-loader-rebuilds-exactly`.
- Prev: [PR 3.7](./pr-3.7-loader-max-applied-lsn-guard.md) ·
  Next: [PR 3.9](./pr-3.9-loader-ddl-destructive.md) · [Roadmap](../README.md)
