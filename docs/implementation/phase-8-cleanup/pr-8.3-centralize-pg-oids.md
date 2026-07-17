<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 8.3 — One home for Postgres OID constants (kill the re-hardcoded literals)

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 8 — cleanup · **Crates touched:** `common`, `pg-to-arrow`, `loader` ·
> **Est. size:** M · **Depends on:** PR 7.8 (phase 7 complete) · **Unlocks:** —

`crates/pg-to-arrow/src/oids.rs` is a complete, named list of `pg_catalog` base-type OIDs
(`INT4 = 23`, `NUMERIC = 1700`, `UUID = 2950`, …). Yet those same numbers are re-hardcoded
as **bare literals** in at least four other places:

- `crates/loader/src/duck.rs:381` — `duck_type()` matches `21 => …, 23 => …, 1700 => …`.
- `crates/loader/src/plan.rs:205-206` — `const INTERVAL: u32 = 1186; const TIMETZ: u32 = 1266;`.
- `crates/loader/src/ddl.rs:86-87` — `is_lossless_widen` matches `(21,23)|(21,20)|(23,20)|(700,701)`.
- `crates/common/src/pg_shape.rs:15` — `const NUMERIC_OID: u32 = 1700;`.

Four copies of the same magic numbers is four chances to disagree. This PR gives the OID
set a single source of truth and routes every reader through it.

## Why — learning objectives

By the end of this PR you will have practised:

- **Choosing the correct crate for a shared constant in a layering DAG.** The DAG is
  `common ← pg-to-arrow ← loader`. `pg_shape.rs` (in `common`, the lowest crate) *also*
  needs an OID — and `common` cannot import from `pg-to-arrow`. So the canonical home must
  be `common`; anything higher can't serve every reader.
- **`pub use` re-exports for back-compat.** Move the module down, then re-export it from
  `pg-to-arrow` so existing `pg_to_arrow::oids::…` paths keep compiling.
- **Deleting magic numbers** without changing behaviour — a pure readability/DRY refactor
  that stays green.

## Read first

- `crates/pg-to-arrow/src/oids.rs` — the canonical named list (this is what moves).
- `docs/implementation/README.md` "Crate dependency DAG" (lines ~100–106) — proves `common`
  is the only crate every reader can reach.
- `crates/loader/Cargo.toml` — confirm `loader` depends on `pg-to-arrow` *and* `common`
  (it does), so either import path is available; prefer importing from `common` directly.
- The four literal sites above.

## Scope

**In scope**

- Relocate the OID constants to `crates/common/src/oids.rs` (`pub mod oids;` in `common`);
  keep every constant name identical.
- In `pg-to-arrow`, replace `src/oids.rs`'s body with `pub use common::oids::*;` (or make
  `pg_to_arrow::oids` a re-export module) so downstream paths are unbroken.
- Replace the bare literals with named constants in `duck.rs` (`duck_type`), `plan.rs`
  (drop the two local `const`s, import `INTERVAL`/`TIMETZ`), `ddl.rs`
  (`is_lossless_widen` → `(INT2, INT4) | (INT2, INT8) | (INT4, INT8) | (FLOAT4, FLOAT8)`),
  and `pg_shape.rs` (`NUMERIC_OID` → the shared `NUMERIC`).

**Explicitly deferred** (do *not* build these here)

- Adding OIDs the code doesn't yet use, or reworking `duck_type`'s fallback logic — this is
  a *relocate-and-reference* PR, not a behaviour change.

## Files to create / modify

```
crates/common/src/oids.rs          # new (moved) — the canonical named OID constants
crates/common/src/lib.rs           # + pub mod oids;
crates/pg-to-arrow/src/oids.rs     # modify — becomes `pub use common::oids::*;` (or delete + re-export)
crates/loader/src/duck.rs          # modify — duck_type() uses named constants
crates/loader/src/plan.rs          # modify — drop local INTERVAL/TIMETZ consts; import them
crates/loader/src/ddl.rs           # modify — is_lossless_widen uses named OID pairs
crates/common/src/pg_shape.rs      # modify — NUMERIC_OID → oids::NUMERIC
```

## Skeleton

```rust
// crates/common/src/oids.rs   (moved verbatim from pg-to-arrow; names unchanged)
//! Canonical `pg_catalog` base-type OIDs (stable across Postgres installs).
pub const BOOL: u32 = 16;
pub const INT8: u32 = 20;
pub const INT2: u32 = 21;
pub const INT4: u32 = 23;
// … (the full existing list) …
pub const NUMERIC: u32 = 1700;
```

```rust
// crates/pg-to-arrow/src/oids.rs   (now a thin re-export — keeps `pg_to_arrow::oids::*` valid)
pub use common::oids::*;
```

```rust
// crates/loader/src/ddl.rs
use common::oids::{FLOAT4, FLOAT8, INT2, INT4, INT8};

fn is_lossless_widen(old: &PgColumn, new: &PgColumn) -> bool {
    matches!(
        (old.type_oid, new.type_oid),
        (INT2, INT4) | (INT2, INT8) | (INT4, INT8) | (FLOAT4, FLOAT8)
    )
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] The OID constants live in `common::oids`; `pg_to_arrow::oids::*` still resolves (via
      re-export) so no downstream import breaks.
- [ ] `duck.rs`, `plan.rs`, `ddl.rs`, and `pg_shape.rs` reference named constants; no bare
      OID integer literals remain in those four sites.
- [ ] `plan.rs` no longer defines local `const INTERVAL`/`const TIMETZ`;
      `pg_shape.rs` no longer defines `NUMERIC_OID`.
- [ ] Behaviour is identical — this is a rename/relocate; existing type tests
      (`plan_test`, `duck_test`, `pg_shape_test`, pg-to-arrow tier tests) pass unchanged.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`

## What completed looks like

```
$ rg -n 'const INTERVAL|const TIMETZ' crates/loader/src/plan.rs
$ rg -n 'NUMERIC_OID' crates/common/src/pg_shape.rs
$ # (both empty — the local consts are gone)

$ rg -n 'oids::' crates/loader/src/{duck,plan,ddl}.rs crates/common/src/pg_shape.rs | wc -l
   # > 0 — every reader now imports the shared constants
```

## Hints & gotchas

- **Move down, not up.** It's tempting to leave `oids.rs` in `pg-to-arrow` and import it
  into the loader — but `common::pg_shape` needs an OID too and `common` can't depend on
  `pg-to-arrow`. `common` is the only home that serves all three crates.
- Keep the constant *names* byte-identical during the move so the re-export is a pure alias
  and the diff stays reviewable.
- `duck_type()`'s `_ => "VARCHAR"` fallback stays — you're only naming the matched arms.
- Watch `#![deny(warnings)]`: an unused re-export or a now-unused `use` will fail CI; prune
  imports as you go.

## References

- Design: `docs/implementation/README.md` "Crate dependency DAG" (why `common` is the
  correct layer) and "Two deliberate structural notes" (neutral types live in `common`).
- Prev: [PR 8.2](./pr-8.2-manifest-kind-status-enums.md) · Next:
  [PR 8.4](./pr-8.4-domain-id-newtypes.md) · [Phase 8](./README.md) ·
  [Roadmap](../README.md)
