<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 8.4 — Domain-ID newtypes: `EpochNo` / `SchemaVersion` / `ReloadId` / `ManifestId` (opt-in)

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 8 — cleanup · **Crates touched:** `common`, `control`, `loader` (`pg-sink`) ·
> **Est. size:** L · **Depends on:** PR 7.8 (phase 7 complete) · **Unlocks:** —

Four semantically distinct identities all flow through the system as bare `i64`: **epoch**,
**schema_version**, **reload_id**, and **manifest id**. In `control`'s function signatures
alone there are **40+** `epoch: i64` / `schema_version: i64` / `reload_id: i64` parameters
(more in `loader`), and the type checker will happily let you pass an epoch where a schema
version is expected. `Lsn` (PR 0.3) already proved the antidote in this codebase — a newtype
that makes each identity its own type. This PR extends that blessed pattern to the remaining
domain scalars.

**This is the biggest-churn, lowest-urgency item in the phase** — no current bug depends on
it, and it touches many signatures. It is deliberately **opt-in**: take it whole, split it
per-type (8.4a `EpochNo`, 8.4b `SchemaVersion`, …), or defer it. Its value is defence in
depth plus the highest learning payoff in Phase 8.

## Why — learning objectives

By the end of this PR you will have practised:

- **The newtype pattern end-to-end** — `struct EpochNo(i64)` with `From`/`Into`, `Display`,
  `Ord`, and a `.get()`/`.0` accessor, exactly how `Lsn` is built.
- **`#[derive(sqlx::Type)] #[sqlx(transparent)]`** — decoding a newtype straight from an
  `int8` column with no manual glue (contrast PR 8.2's `text` enums, which needed
  hand-rolled conversion).
- **Compile-time prevention of argument transposition** — after this, `advance(epoch,
  schema_version)` can't be called with the two swapped.
- **Scoping a large mechanical refactor** — doing it type-by-type so each step stays green.

## Read first

- `crates/common/src/lsn.rs` + `lsn_test.rs` — the exact newtype shape to copy (Display,
  Ord, parse, sqlx integration behind the `sqlx` feature).
- `crates/control/src/{replication_state,schema_registry,manifest,reload}.rs` — the
  signatures carrying `epoch` / `schema_version` / `reload_id` / manifest `id`.
- `crates/loader/src/{phase_a,phase_b,plan,duck,epoch}.rs` — the loader-side consumers.
- The DB columns these map to (all `bigint`/`bigserial`) in `migrations/control/*.sql`.

## Scope

**In scope** (per newtype; land them independently if splitting)

- In `common`: `EpochNo(i64)`, `SchemaVersion(i64)`, `ReloadId(i64)`, `ManifestId(i64)`,
  each with `From<i64>`/`i64: From`, `Display`, derived `Ord`/`Eq`/`Copy`, and — behind the
  existing `sqlx` feature — `#[derive(sqlx::Type)] #[sqlx(transparent)]`.
- Thread each newtype through `control` signatures and their `loader`/`pg-sink` callers,
  replacing the bare `i64`s. Regenerate the `.sqlx` cache.

**Explicitly deferred** (do *not* build these here)

- A `QualifiedName` newtype for DuckDB identifier quoting is *optional* within this PR — do
  it only if you want the identifier-quoting invariant (`"{table}"` scattered across
  `duck.rs`/`ddl.rs`) enforced by the type system too. If it balloons the diff, split it out
  as PR 8.4e.
- Arrow/Parquet-internal `i64`s that aren't these four identities — leave them.

## Files to create / modify

```
crates/common/src/ids.rs           # new — the four newtypes (+ QualifiedName if in scope)
crates/common/src/ids_test.rs      # new — Display/round-trip/ordering tests
crates/common/src/lib.rs           # + pub mod ids;
crates/control/src/*.rs            # modify — signatures + row structs use the newtypes
crates/loader/src/*.rs             # modify — callers/consumers use the newtypes
crates/pg-sink/src/*.rs            # modify — manifest writers / epoch stamping (as needed)
```

## Skeleton

```rust
// crates/common/src/ids.rs

/// The total-restart generation counter (control-plane `replication_state.epoch`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type), sqlx(transparent))]
pub struct EpochNo(pub i64);

impl std::fmt::Display for EpochNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { todo!() }
}
impl From<i64> for EpochNo { fn from(v: i64) -> Self { todo!() } }

// … SchemaVersion(i64), ReloadId(i64), ManifestId(i64) — same shape …

#[cfg(test)]
#[path = "ids_test.rs"]
mod tests;
```

```rust
// crates/control/src/replication_state.rs   (illustrative signature change)
pub async fn bump_epoch(executor: impl PgExecutor<'_>, from: EpochNo) -> Result<EpochNo, ControlError> { todo!() }
```

## Definition of Done

A reviewer merges this PR (or each split sub-PR) when **all** of the following hold:

- [ ] The newtype(s) exist in `common::ids`, derive `sqlx::Type`/`transparent` behind the
      `sqlx` feature, and have `Display` + round-trip tests.
- [ ] The chosen identities are newtypes across `control` (and `loader`/`pg-sink` callers) —
      no bare `i64` for those parameters in the converted signatures.
- [ ] No accidental `.0` leakage where a newtype should flow end-to-end (accessor only at
      true boundaries: SQL binds, arithmetic, logging).
- [ ] `.sqlx` offline cache regenerated; `cargo sqlx prepare --check` passes.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `docker compose up --wait` + the control/loader integration + e2e reload suites
        (epoch + reload_id routing are load-bearing).

## What completed looks like

```
$ rg -c 'epoch: i64|schema_version: i64|reload_id: i64' crates/control/src -g '!*_test.rs'
   # drops toward 0 as each identity is converted (was ~43)

$ cargo test -p common ids   # Display + transparent-decode round-trip tests pass
```

Passing a `SchemaVersion` where an `EpochNo` is expected is now a **compile error**, not a
silent data bug.

## Hints & gotchas

- **`transparent` is the whole trick** for the `int8` columns — sqlx decodes the inner
  `i64` directly, so `query_file_as!` keeps working once the row field type changes. (This
  is why PR 8.2's `text` enums are harder: no transparent path there.)
- Do it **one identity at a time** and keep each step compiling. `EpochNo` first (smallest
  blast radius), `ManifestId` last (widest). A single mega-diff will fight `#![deny(warnings)]`
  the whole way.
- Derive `Ord` — the loader orders on `(lsn_end, id)` and compares epochs; keep those
  comparisons ergonomic.
- Resist adding methods the code doesn't need. `Lsn` earns its surface because it parses and
  formats; these are near-pure wrappers — `Display` + `From` + accessor is usually enough.
- If the diff gets unwieldy, **stop and split**. A merged `EpochNo`-only PR is worth more
  than a stalled all-four branch.

## References

- Design: `docs/implementation/README.md` "Conventions" + PR
  [0.3](../phase-0-foundations/pr-0.3-common-lsn-newtype.md) (`Lsn`) — the newtype precedent
  this generalizes.
- Prev: [PR 8.3](./pr-8.3-centralize-pg-oids.md) · Next:
  [PR 8.5](./pr-8.5-nits-cluster.md) · [Phase 8](./README.md) · [Roadmap](../README.md)
