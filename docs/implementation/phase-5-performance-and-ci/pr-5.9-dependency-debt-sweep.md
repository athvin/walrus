# PR 5.9 — Dependency & debt sweep: commit_ts, object_store advisories, the DuckDB LTS clock

> **Phase:** 5 — Performance & CI · **Crates touched:** `common`, `pg-sink`, workspace root,
> `deny.toml` · **Est. size:** M · **Depends on:** PR 5.8 · **Unlocks:** — (phase close)

Three debts, none new — all recorded in the tree or the design docs, all with a clock attached.
(1) `consume.rs` has a TODO: `SinkMeta.commit_ts` is not sourced from the Begin/Stream-Commit
message because `UtcTimestamp` lacks a from-Postgres-micros constructor — so the provenance column
ships without the one timestamp the design says it carries. (2) `deny.toml` carries two `quick-xml`
RUSTSEC ignores inherited from `object_store 0.11`; the fix is upstream — bump and delete the
ignores. (3) **DuckDB 1.4.x LTS community support ends 2026-09-16** — the architecture docs
explicitly say "pin the latest 1.4.x and plan the next-LTS bump"; this PR does the evaluation and
either lands the bump (the conformance suite is the safety net built for exactly this) or documents
the concrete blocker with a dated follow-up.

## Why — learning objectives

By the end of this PR you will have practised:

- **Epoch conversion without a library crutch** — pgoutput timestamps are µs since 2000-01-01 UTC
  (proto §4); turning that into an RFC 3339 `UtcTimestamp` correctly, with edge tests.
- **Advisory-driven upgrades** — reading a RUSTSEC ignore back to its root dependency, bumping
  through a minor-API migration, and shrinking the ignore list as the deliverable.
- **Pinned-stack upgrades** — moving an exact-pinned trio (`duckdb`/`arrow`/`parquet`) in lockstep,
  letting the conformance suite (built in PR 2.11 for this exact purpose) adjudicate.

## Read first

- `crates/pg-sink/src/consume.rs` — the `commit_ts` TODO and where Begin / Stream Commit metas are
  constructed; `crates/common/src/sink_meta.rs` — `UtcTimestamp`'s current constructors.
- `docs/proto-version.md` §4 — Begin: `Int64 commit ts = µs since 2000-01-01`.
- `deny.toml` — the two `RUSTSEC-2026-019{4,5}` ignores and their justification comments naming
  `object_store 0.11.2`.
- `docs/architecture.md` Open Q4(b) — the 1.4.x EOL note and the "plan the next-LTS bump" mandate;
  `Cargo.toml` — the pin comments explaining *why* arrow/parquet/duckdb are exact-pinned (the
  `arrow.uuid` annotation behaviour).
- `crates/pg-to-arrow/tests/conformance.rs` — the read-back suite that gates any bump.

## Scope

**In scope**

- **commit_ts:** add `UtcTimestamp::from_pg_micros(i64)` (µs since 2000-01-01T00:00:00Z) to
  `common`, unit-tested against known values (0 → `2000-01-01T00:00:00Z`, a negative value, a
  current-era value cross-checked against `psql`'s rendering). Populate `SinkMeta.commit_ts` from
  the Begin message on the normal path and from Stream Commit on the streamed path. Extend an
  existing integration assertion (e.g. the provenance check in the e2e thin slice) to verify the
  emitted `commit_ts` matches the transaction's actual commit time within tolerance.
- **object_store bump:** move `object_store` past the `quick-xml < 0.41` advisories (0.12.x line),
  migrate whatever small API drift hits `sink.rs`'s `BufWriter`/multipart use, delete both
  `deny.toml` ignores, and re-run the S3-touching integration tests against MinIO.
- **DuckDB next-LTS evaluation:** identify the current next LTS and the `duckdb-rs` release
  tracking it; attempt the lockstep bump of `duckdb` (+ `arrow`/`parquet` if the new `duckdb-rs`
  requires it) on a branch:
  - conformance suite green (UUID annotation, decimal, temporal MICROS, nested types) → land it,
    updating the pin-rationale comments and the MERGE-version note;
  - anything red that isn't a quick fix → **do not force it**; commit
    `docs/implementation/notes/duckdb-lts-bump.md` recording the exact blocker (crate versions
    tried, failing assertions), and the EOL date the project is now consciously running against.
- Prune stale `#[allow(...)]`s while touching these files (each one either still justified or
  removed).

**Explicitly deferred** (do *not* build these here)

- DuckDB ≥1.5 feature adoption (core `GEOMETRY`, `VARIANT`) — a mapping-design task
  (`walrus-pg-sink.md` §2.4 PostGIS note), not a dependency chore.
- Broad unpinning of `arrow`/`parquet` — the exact pins exist for the UUID annotation; they move
  only in lockstep with duckdb, never loosened to ranges.
- dependabot/renovate automation — out of curriculum scope (as in PR 4.7).

## Files to create / modify

```
crates/common/src/sink_meta.rs           # modify — UtcTimestamp::from_pg_micros + tests
crates/pg-sink/src/consume.rs            # modify — commit_ts from Begin / Stream Commit
Cargo.toml                               # modify — object_store bump; possibly the duckdb trio
deny.toml                                # modify — delete the two quick-xml ignores
crates/pg-sink/src/sink.rs               # modify (maybe) — object_store 0.12 API drift
docs/implementation/notes/duckdb-lts-bump.md   # new IF the bump is blocked — the dated finding
```

## Skeleton

```rust
// crates/common/src/sink_meta.rs  (shape)
impl UtcTimestamp {
    /// Postgres wire timestamps are microseconds since 2000-01-01T00:00:00Z (proto §4).
    /// NOT the Unix epoch: offset by 946_684_800 seconds.
    pub fn from_pg_micros(micros: i64) -> Result<Self, Error> { todo!() }
}

#[cfg(test)]
mod tests {
    #[test] fn pg_epoch_zero_is_y2k() { todo!() }          // 0 → 2000-01-01T00:00:00Z
    #[test] fn negative_micros_pre_y2k() { todo!() }
    #[test] fn round_trips_a_known_commit_ts() { todo!() } // value captured from a live Begin
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `SinkMeta.commit_ts` is the transaction's real commit timestamp (UTC, `Z`) on both the
      normal and streamed paths; the `consume.rs` TODO is gone; an integration assertion covers it.
- [ ] `cargo deny check` is green with **both** `quick-xml` ignores removed (the advisories list
      shrinks; nothing new is ignored to compensate).
- [ ] All S3-touching integration tests (parquet_put, manifest_insert, durability, spill) pass on
      the bumped `object_store` against MinIO.
- [ ] The DuckDB bump either **landed** (conformance + full workspace + compose suites green, pin
      comments updated) or is **documented** in `notes/duckdb-lts-bump.md` with the exact blocker
      and the EOL date — one of the two, explicitly.
- [ ] Every surviving `#[allow(...)]` in touched files has a current justification comment.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `cargo deny check`
  - [ ] compose integration + conformance jobs green

## Hints & gotchas

- The Postgres epoch offset is `946_684_800` seconds (2000-01-01 − 1970-01-01, no leap seconds in
  Unix time). Overflow-check the µs→jiff conversion: `i64::MAX` µs is far future, but a corrupt
  frame shouldn't panic the sink — return the error type, don't `unwrap`.
- On the streamed path the commit timestamp arrives in **Stream Commit** (same field layout as
  Commit) — the per-row metas for a streamed txn are built *before* commit, so `commit_ts`, like
  `commit_lsn`, gets stamped at commit time in `on_commit`/the demux resolution, not at decode
  time.
- `object_store` 0.11→0.12 renamed/reshaped parts of the multipart API (`BufWriter` construction
  and `PutPayload`); the compiler walks you through it — the durability *ordering* (close() =
  durability point) is the invariant to re-verify in the durability integration test, not the
  types.
- For the DuckDB attempt, change **one axis at a time**: first `duckdb` alone (its bundled engine
  version is the payload); only touch arrow/parquet if `duckdb-rs`'s arrow interop forces it. If
  the UUID conformance assertion fails, that is the pin rationale firing exactly as designed —
  record it, don't ignore-list it.
- `cargo update -p quick-xml` alone will NOT fix the advisories (0.11's object_store has a `<0.41`
  bound) — the bump must be `object_store` itself; verify with `cargo tree -i quick-xml`.

## References

- Design: `docs/proto-version.md` §4 (Begin layout); `docs/architecture.md` Open Q4(b) (LTS EOL);
  `docs/walrus-pg-sink.md` §2.4 (uuid pin rationale); PR 4.7 (deny.toml conventions).
- Prev: [PR 5.8](./pr-5.8-loader-hot-path.md) · [Roadmap](../README.md)
