<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.5 — Loader DuckDB DDL into `sql/duckdb/` templates

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 7 — conventions hardening · **Crates touched:** `loader`, docs ·
> **Est. size:** M · **Depends on:** — · **Unlocks:** —

The loader builds its DuckDB DDL with `format!` scattered through `duck.rs` — the mirror and `_raw`
`CREATE TABLE`s, the `_walrus_meta` seed, the S3 `SET` block, the `read_parquet` append, the user
view, the rebuild/wipe drops. This PR pulls the **fixed scaffolding** out into `.sql` template files
under `crates/loader/sql/duckdb/templates/`, loaded with `include_str!` and rendered by the exact
`.replace("{placeholder}", …)` pattern the crate already uses for `transform.sql` — which also moves
into the new tree. Per-table **column lists** stay interpolated in Rust (they can't be static). The
rendered DDL is byte-for-byte identical; this is a readability and single-source-of-truth move.

## Why — learning objectives

By the end of this PR you will have practised:

- **`include_str!` vs `query_file!`** — `include_str!` is **source-file-relative**; the loader has no
  compile-time SQL checking (it's the `duckdb` crate, not sqlx), so templates + string substitution
  are the right tool.
- **Drawing the static/dynamic boundary** — what belongs in a template (fixed structure with named
  holes) vs what must stay a Rust `format!` (a per-table column list of arbitrary length).
- **Proving a render is unchanged** — the `duck.rs` unit tests assert the emitted SQL is identical.

## Read first

- `crates/loader/src/transform.rs` + `src/transform.sql` — the existing `include_str!` +
  `.replace("{table}", …)` render pattern you are extending and mirroring.
- `crates/loader/src/duck.rs` — the `format!`-built DDL sites you are extracting.

## Scope

**In scope**

- Create `crates/loader/sql/duckdb/{templates,test}/`.
- **Move** `crates/loader/src/transform.sql` → `crates/loader/sql/duckdb/templates/transform.sql`;
  update `transform.rs`'s `include_str!` to the new **source-relative** path
  (`include_str!("../sql/duckdb/templates/transform.sql")`).
- Extract the fixed DDL scaffolding from `duck.rs` into templates with `{placeholder}` holes, rendered
  by `.replace(...)`: `create_mirror.sql`, `create_raw.sql`, `create_meta.sql`, `alter_add_applied.sql`,
  `create_user_view.sql`, `configure_s3.sql`, `append_parquet.sql`, `reload_rebuild_drop.sql`,
  `wipe_generation.sql`.
- Amend the README Conventions **SQL location** row with the DuckDB half (PR 7.4 added the Postgres
  half).

**Explicitly deferred** (do *not* build these here)

- Per-table **column-list** interpolation and the tiny bound-param one-liners (`schema_version()`,
  `set_*`, `DESCRIBE`, meta reads) — they **stay inline** in `duck.rs`; a template file adds noise, not
  clarity, and the column list is intrinsically dynamic.
- Control's sqlx queries → **PR 7.4**.

## Files to create / modify

```
crates/loader/sql/duckdb/templates/transform.sql        # moved from src/transform.sql
crates/loader/sql/duckdb/templates/{create_mirror,create_raw,create_meta,alter_add_applied,
  create_user_view,configure_s3,append_parquet,reload_rebuild_drop,wipe_generation}.sql  # new
crates/loader/sql/duckdb/test/.gitkeep                  # new — reserved slot
crates/loader/src/duck.rs                               # modify — format! DDL → include_str! + replace
crates/loader/src/transform.rs                          # modify — include_str! path for the moved transform.sql
docs/implementation/README.md                           # modify — amend Conventions "SQL location" row (DuckDB half)
```

## Skeleton

```sql
-- crates/loader/sql/duckdb/templates/create_user_view.sql
CREATE OR REPLACE VIEW "{table}_current" AS
SELECT * EXCLUDE (_applied_commit_lsn, _applied_lsn) FROM "{table}";
```

```rust
// crates/loader/src/duck.rs — render mirrors the transform.rs pattern
const CREATE_USER_VIEW: &str = include_str!("../sql/duckdb/templates/create_user_view.sql");

fn user_view_sql(table: &str) -> String {
    CREATE_USER_VIEW.replace("{table}", table)
}
```

```rust
// crates/loader/src/transform.rs — the moved transform.sql, new source-relative path
const TRANSFORM_SQL: &str = include_str!("../sql/duckdb/templates/transform.sql");
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] The nine fixed-DDL statements render from `sql/duckdb/templates/*.sql` via `include_str!` +
      `.replace(...)`; `transform.sql` lives under the new tree and `transform.rs` loads it there.
- [ ] The rendered SQL is **byte-identical** to before — the `duck.rs` unit tests (mirror/raw/meta/view
      creation, `open_in_memory`/`ensure_tables`) prove it; the per-table column-list interpolation is
      unchanged in Rust.
- [ ] No `.sqlx` impact (DuckDB is not sqlx); no template holds a column list.
- [ ] README Conventions **SQL location** row amended with the DuckDB half.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader` (and `--workspace` stays green)

## What completed looks like

```
$ find crates/loader/sql/duckdb/templates -name '*.sql' | wc -l
10                                   # transform.sql + the 9 DDL templates
$ grep -c 'CREATE TABLE\|CREATE OR REPLACE' crates/loader/src/duck.rs
0                                    # structural DDL now lives in templates
$ cargo test -p loader duck          # rendered-DDL assertions
test result: ok.
```

## Hints & gotchas

- `include_str!` is **relative to the source file** doing the include (`crates/loader/src/duck.rs` →
  `"../sql/duckdb/templates/…"`), unlike PR 7.4's `sqlx::query_file!` which is crate-root-relative.
  Getting this backwards is the classic failure here.
- Keep the placeholder vocabulary consistent with `transform.sql` (`{table}`, `{cols}`, `{primary_key}`,
  …) so the two render sites read the same way.
- Draw the line firmly: anything with a per-table, variable-length column list (the mirror/`_raw`
  column definitions, the `INSERT … SELECT` projection) keeps its `format!`/builder in Rust; the
  template holds only the surrounding fixed structure.
- Confirm no loader test hard-codes the old `src/transform.sql` path before moving it (there is none
  today — only `transform.rs:18` references it).

## References

- Design: `crates/loader/src/transform.rs` (the existing template precedent);
  `docs/implementation/README.md` "Conventions" (SQL row).
- Prev: [PR 7.4](./pr-7.4-control-sql-query-file.md) · Next:
  [PR 7.6](./pr-7.6-fix-unwrap-expect.md) · [Roadmap](../README.md)
