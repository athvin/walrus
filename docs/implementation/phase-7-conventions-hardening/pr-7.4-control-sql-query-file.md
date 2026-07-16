<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.4 — Control SQL into `sql/postgres/` via `query_file!`

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 7 — conventions hardening · **Crates touched:** `control`, docs ·
> **Est. size:** M · **Depends on:** — · **Unlocks:** —

The control crate carries ~35 inline `sqlx::query!` / `query_as!` blocks — every manifest claim,
checkpoint advance, epoch bump, and reload state transition is a string literal buried in Rust. This
PR moves each one into a named `.sql` file under `crates/control/sql/postgres/queries/` and switches
the call to `sqlx::query_file!` / `query_file_as!`. **Compile-time checking is preserved** — this is a
lateral move to files, not a downgrade to runtime SQL — and the `.sqlx` offline cache regenerates. The
`AS "col: Type"` type-cast overrides ride along inside the `.sql` files. Engine goes at the head of
the directory (`sql/postgres/`) so a second engine can never collide.

## Why — learning objectives

By the end of this PR you will have practised:

- **`sqlx::query_file!` mechanics** — path resolution relative to `CARGO_MANIFEST_DIR`, one statement
  per file, and how the `.sqlx` offline cache is keyed and regenerated.
- **Separating SQL from Rust** — the queries become greppable, diffable, and reviewable as SQL, while
  the Rust keeps only the parameter binding and result mapping.
- **Knowing the macro's limits** — which call sites *can't* move (runtime `sqlx::query()` with dynamic
  binds has no offline entry and stays inline).

## Read first

- `crates/control/src/reload.rs` — the densest site (16 macros) and the `AS "first_lsn: Lsn"` casts.
- `crates/control/src/checkpoint.rs` — the `query_as!(Checkpoint, …)` with `Lsn` casts, a good first
  conversion.
- [`sqlx::query_file!` docs](https://docs.rs/sqlx/latest/sqlx/macro.query_file.html) — path rules and
  offline behaviour.
- `docs/implementation/README.md` "CI grows" — the `sqlx prepare --check` gate (from PR 1.3) that must
  stay green.

## Scope

**In scope**

- Create `crates/control/sql/postgres/{queries,templates,test}/` (`templates`/`test` may start with a
  `.gitkeep`).
- Move every **static** `sqlx::query!` / `query_as!` / `query_scalar!` body in `control/src`
  (checkpoint 4, ddl_manifest 2, manifest 6, replication_state 3, schema_registry 3, reload 16) into a
  named `.sql` under `queries/`, and switch the call to `query_file!` / `query_file_as!`. Keep the
  `AS "col: Type"` overrides **in the `.sql`**.
- Regenerate and commit `.sqlx/` so `cargo sqlx prepare --check --workspace` stays green.
- Add the README Conventions **SQL location** row (Postgres half; PR 7.5 amends it with DuckDB).

**Explicitly deferred** (do *not* build these here)

- The loader's DuckDB DDL templates → **PR 7.5**.
- Relocating `/migrations/{source,control}/` — they **stay** (the `sqlx::migrate!` macro path, 19
  pg-sink test `include_str!`s, the k8s slot-init ConfigMap, CI, and `scripts/` all point at them;
  `sql/<engine>/` is for *query* text, not versioned migrations).
- `schema_registry::read_all_latest_registry` — a runtime `sqlx::query()` with `.bind()`/`try_get`,
  no offline entry; it **stays inline** (its module doc-comment already explains why).

## Files to create / modify

```
crates/control/sql/postgres/queries/*.sql   # new — one file per static query (~34)
crates/control/sql/postgres/{templates,test}/.gitkeep  # new — reserved by-engine slots
crates/control/src/{checkpoint,ddl_manifest,manifest,replication_state,schema_registry,reload}.rs  # modify — query! → query_file!
.sqlx/query-*.json                           # regenerated — offline cache
docs/implementation/README.md                # modify — add Conventions "SQL location" row (Postgres half)
```

## Skeleton

```sql
-- crates/control/sql/postgres/queries/read_checkpoint.sql
SELECT raw_appended_lsn AS "raw_appended_lsn: Lsn",
       transformed_lsn  AS "transformed_lsn: Lsn"
FROM loader_checkpoint
WHERE epoch = $1 AND schema_version = $2 AND table_name = $3;
```

```rust
// crates/control/src/checkpoint.rs — the call becomes a query_file_as!
// path is relative to CARGO_MANIFEST_DIR (= crates/control/)
sqlx::query_file_as!(Checkpoint, "sql/postgres/queries/read_checkpoint.sql", epoch, schema, table)
    .fetch_optional(ex)
    .await
    .map_err(ControlError::from_sqlx)
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Every static control query lives in `sql/postgres/queries/<name>.sql` and its call site uses
      `query_file!`/`query_file_as!`; the only inline SQL left in `control/src` is the documented
      runtime `read_all_latest_registry`.
- [ ] The `AS "col: Type"` overrides moved into the `.sql` files verbatim; result structs decode
      unchanged.
- [ ] `.sqlx/` regenerated and committed; `cargo sqlx prepare --check --workspace` is green (and the
      offline `gates`/clippy job, which has no DB, still compiles against the committed cache).
- [ ] `control`'s integration tests (which call the public fns, not the SQL) pass unchanged.
- [ ] README Conventions gains the **SQL location** row (Postgres half).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p control` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` → migrate control-pg → `cargo sqlx prepare --check --workspace`

## What completed looks like

```
$ ls crates/control/sql/postgres/queries | wc -l
34
$ grep -rn 'sqlx::query!\|sqlx::query_as!' crates/control/src | wc -l
0            # only query_file!/query_file_as! remain (plus the one runtime query())
$ cargo sqlx prepare --check --workspace
query cache is up-to-date
```

Every control query is now a reviewable `.sql` file, still compile-checked, cache still green.

## Hints & gotchas

- `query_file!` resolves its path from `CARGO_MANIFEST_DIR` (the crate dir), so the argument is
  `"sql/postgres/queries/x.sql"` — **not** a `src`-relative or repo-relative path. (Contrast PR 7.5's
  `include_str!`, which *is* source-file-relative — don't mix the two mental models.)
- One statement per file: the fns that issue two queries (e.g. `fail` = update + purge, `restart_for_ddl`)
  already call `query!` twice, so each maps cleanly to its own `.sql`.
- Regenerating the cache needs a **migrated** control PG: `docker compose up --wait`, run
  `sqlx migrate run --source migrations/control` against it, `DATABASE_URL=… cargo sqlx prepare --workspace`.
  Commit the resulting `.sqlx/*.json` in this same PR or `--check` fails in CI.
- Name the files after the fn/purpose (`claim_ready.sql`, `advance_transformed.sql`, `bump_epoch.sql`)
  so the call site reads self-documenting.
- This does not add a CI gate — `sqlx prepare --check` has existed since PR 1.3; do **not** add a
  CI-grows row for it.

## References

- Design: `docs/implementation/README.md` "Conventions" (new SQL row) + "CI grows" (PR 1.3 sqlx gate).
- Prev: [PR 7.3](./pr-7.3-tests-sibling-pg-sink.md) · Next:
  [PR 7.5](./pr-7.5-loader-duckdb-templates.md) · [Roadmap](../README.md)
