<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 8.2 — Type the manifest `kind` and `status` (retire the stringly-typed columns)

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 8 — cleanup · **Crates touched:** `control`, `loader` ·
> **Est. size:** M · **Depends on:** PR 7.8 (phase 7 complete) · **Unlocks:** —

`ManifestRow.kind` and `ManifestRow.status` are `String`
(`crates/control/src/manifest.rs:27,32`), and the loader branches on them with raw string
compares (`crates/loader/src/phase_a.rs:134` `== "reload"`, `:149` `!= "reload"`, `:190`
`== "spill"`). The bug this invites is **already here**: the doc comment on `ManifestRow`
(lines 17–19) documents `kind` as `'snapshot' | 'stream' | 'reload'` and never mentions
`'spill'` — yet `phase_a.rs:190` reads `'spill'` at runtime. The type didn't know about a
value the code depends on. This PR closes that gap: model `kind` (and `status`) as Rust
enums so the closed set is written down once, the compiler enforces exhaustive handling,
and the doc-vs-reality drift becomes impossible.

## Why — learning objectives

By the end of this PR you will have practised:

- **Making illegal states unrepresentable** — a `ManifestKind` can't be `"spil"` (typo) or
  an undocumented value; a `match` must cover every arm.
- **Enum ⇄ text at a `TEXT` column boundary** — these columns are plain `text`, *not* a
  Postgres `ENUM` type, so you'll hand-roll `as_str()` / `FromStr` (or `TryFrom<&str>`)
  rather than lean on sqlx's PG-enum mapping. `control/src/reload.rs` already does exactly
  this for `ReloadStatus`/`ReloadFlavor` — copy that shape.
- **Replacing runtime string compares with `match`** — the three `phase_a` branches become
  a typed, exhaustive dispatch.

## Read first

- `crates/control/src/manifest.rs` — `ManifestRow` (the doc comment that omits `'spill'` is
  the smoking gun), `NewManifestFile`, `insert_ready`, `claim_ready` (note
  `query_file_as!` maps the columns).
- `crates/control/src/reload.rs` (around the `ReloadStatus` / `ReloadFlavor` enums) — the
  established `enum` + `as_str()` + `FromStr` idiom for a `text` column; mirror it.
- `crates/loader/src/phase_a.rs:134,149,190` — the three consumers to convert.
- Every `NewManifestFile { kind: … }` construction site in `pg-sink` (snapshot, stream,
  spill, reload writers) — grep `kind:` under `crates/pg-sink/src`.

## Scope

**In scope**

- In `control`: `pub enum ManifestKind { Snapshot, Stream, Spill, Reload }` and
  `pub enum ManifestStatus { Ready, Failed }` (confirm the full status set from
  `mark_failed.sql` / `claim_ready.sql` before finalizing the variants), each with
  `as_str()` + `FromStr`/`TryFrom<&str>` round-tripping the exact stored text.
- Change `ManifestRow.kind`/`.status` and `NewManifestFile.kind` to the enums; convert at
  the query boundary (read `String`, `parse()`; write `.as_str()`).
- Convert the three `phase_a.rs` comparisons to enum matches; fix the `ManifestRow` doc
  comment to list all four kinds.

**Explicitly deferred** (do *not* build these here)

- Migrating the DB columns to a Postgres `ENUM` type → not worth it; `text` + a Rust enum
  is the lighter, reversible choice and matches `reload.rs`.
- `reload_id` / `epoch` / `schema_version` newtypes → **PR 8.4**.

## Files to create / modify

```
crates/control/src/manifest.rs        # modify — enums + as_str/FromStr; typed row fields; fix doc comment
crates/control/src/manifest_test.rs   # modify/new — round-trip tests for both enums (incl. unknown → Err)
crates/loader/src/phase_a.rs          # modify — 3 string compares → enum match
crates/pg-sink/src/*.rs               # modify — NewManifestFile { kind } construction sites use the enum
```

## Skeleton

```rust
// crates/control/src/manifest.rs

/// The queue role of a manifest file. Stored as `text`; this enum is the closed set.
/// `Spill` is a speculative single-txn file (see loader Phase A commit-lsn override).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestKind { Snapshot, Stream, Spill, Reload }

impl ManifestKind {
    pub fn as_str(&self) -> &'static str { todo!() }
}
impl std::str::FromStr for ManifestKind {
    type Err = ControlError; // or a small dedicated ParseError
    fn from_str(s: &str) -> Result<Self, Self::Err> { todo!() }
}

pub struct ManifestRow {
    pub id: i64,
    // …
    pub kind: ManifestKind,     // was String
    pub status: ManifestStatus, // was String
    pub reload_id: Option<i64>,
}
```

```rust
// crates/loader/src/phase_a.rs   (illustrative — the shape, not the logic)

match f.kind {
    ManifestKind::Reload => { /* route_reload_file … */ todo!() }
    ManifestKind::Spill  => { /* commit_lsn override = f.lsn_end */ todo!() }
    ManifestKind::Snapshot | ManifestKind::Stream => { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `ManifestKind` and `ManifestStatus` exist with total `as_str()`/`FromStr`, and an
      unknown string parses to `Err` (tested), never a silent default.
- [ ] `ManifestRow`/`NewManifestFile` use the enums; no `kind: String`/`status: String`
      remain (`rg -n 'kind: String|status: String' crates/control/src/manifest.rs` empty).
- [ ] `phase_a.rs` has no `== "reload"` / `!= "reload"` / `== "spill"`; it matches the enum.
- [ ] The `ManifestRow` doc comment lists **all four** kinds (spill included).
- [ ] The `.sqlx` offline cache is regenerated if any query text changed
      (`cargo sqlx prepare` / `--check` passes).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p control -p loader` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then the existing loader integration/e2e reload tests
        still pass (kind routing is load-bearing for reload).

## What completed looks like

```
$ rg -n 'kind: String|status: String' crates/control/src/manifest.rs
$ # (no output)

$ rg -n '== "reload"|!= "reload"|== "spill"' crates/loader/src/phase_a.rs
$ # (no output — now a match on ManifestKind)

$ cargo test -p control manifest   # round-trip + unknown-value tests pass
```

## Hints & gotchas

- **`text`, not a PG `ENUM`.** Don't reach for `#[derive(sqlx::Type)]`'s default enum
  mapping — it expects a Postgres enum type. Read the column as `String` in the query fn and
  `.parse()` it into the Rust enum (or use `query_scalar` + convert). `reload.rs` is your
  template.
- Nail down the exact status set from the SQL before writing variants: `insert_ready` writes
  `'ready'`, `mark_failed` writes `'failed'` — confirm no others (e.g. a transient
  `'claimed'`) exist before you commit to two variants.
- `NewManifestFile` is built in several `pg-sink` writers (snapshot, stream, spill, reload
  export) — change them all in one PR or the crate won't compile.
- This is the highest-signal *type* cleanup in the phase precisely because the drift it
  prevents already occurred. Call that out in the PR description.

## References

- Design: `docs/implementation/README.md` "Conventions" (Errors — libraries: "modelled, not
  stringly-typed" — the same principle, applied to the manifest).
- Prev: [PR 8.1](./pr-8.1-sql-literal-helper.md) · Next:
  [PR 8.3](./pr-8.3-centralize-pg-oids.md) · [Phase 8](./README.md) ·
  [Roadmap](../README.md)
