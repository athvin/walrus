# PR 6.10 — the `resync` flavor: merge over the live mirror, no pause, no clear

> **Status:** 📋 Planned

> **Phase:** 6 — single-table reload · **Crates touched:** `pg-sink`, `loader` ·
> **Est. size:** S · **Depends on:** PR 6.7 · **Unlocks:** PR 6.11

Refresh and rebuild are different operations
([reload H3](../../single-table-reload.md#h3--refresh-and-rebuild-are-different-operations-the-proposal-conflates-them)):
`reload` clears and replaces (the quarantine flavor, PRs 6.6–6.7); **`resync`** merges chunks over
the *live* mirror — no pause, no `CREATE OR REPLACE`, the table stays queryable throughout. It
repairs stale and missing rows but **never removes phantoms**: a row that drifted into the mirror
and no longer exists upstream is in no chunk and gets no delete event. That caveat is the flavor's
defining property (Debezium's incremental snapshots share it) and this PR's job is as much to
*document* it as to implement it. Mechanically almost everything already exists — this PR removes
the 6.4 preflight rejection and makes the loader treat resync chunk files as plain appends. It
also retires the last design open question: resync chunks go through Phase A into raw like any
file (one uniform path; raw history preserved, unlike rebuild).

## Why — learning objectives

By the end of this PR you will have practised:

- **Shipping a feature by *removing* guards** — when the machinery was built flavor-shaped from
  the start, the second flavor is a diff of deletions plus documentation.
- **Documenting a sharp edge as API** — the phantom caveat decides when an operator picks
  `resync` vs `reload`; writing that decision guide is the deliverable.
- **Convergence without a clear** — the same `(commit_lsn, lsn)` algebra, now proving
  stale-row repair instead of full rebuild.

## Read first

- `../../single-table-reload.md` — H3 (both flavors, shared machinery, the phantom property),
  §6 (the raw-semantics open question this PR closes).
- PR 6.4's `PreflightRejection::ResyncNotYetImplemented` — the guard coming out.
- PR 6.7's `route_reload_file` — the resync arm that becomes real.

## Scope

**In scope**

- Lift the 6.4 preflight rejection; `just reload table='…' flavor='resync'` now runs end to end.
- Loader: resync-flavor `kind='reload'` files are plain Phase A appends — no pause (6.6 already
  scopes the predicate to `flavor='reload'`), no rebuild trigger, no purge, no meta latch;
  Phase B's MERGE repairs stale/missing rows.
- Completion: identical predicate (6.9's `transformed_lsn ≥ H`) — verify it needs zero changes.
- DDL restart (6.8) applies unchanged — confirm with a test, not a hope.
- The decision guide (three sentences, where operators look — the 6.11 runbook stub or the design
  doc): `resync` = cheap drift repair, keeps the table queryable, tolerates phantoms; `reload` =
  the truth reset.

**Explicitly deferred** (do *not* build these here)

- Phantom detection/repair under resync (a full-PK-set diff pass) — out of scope for the phase;
  the caveat is documented, the fix is `flavor='reload'`.
- Runbook + alerts → **PR 6.11**.

## Files to create / modify

```
crates/pg-sink/src/reload.rs           # modify — remove the resync preflight rejection
crates/loader/src/…                    # modify — resync arm in route_reload_file = plain append
docs/…                                 # modify — the flavor decision guide (runbook stub or design doc)
crates/loader/tests/reload_resync.rs   # new — compose: repair, phantom-survives, no-pause
```

## Skeleton

```rust
// crates/loader/tests/reload_resync.rs

/// Drift the mirror both ways (delete a row from DuckDB directly = "missing"; insert one = the
/// phantom), then resync: missing/stale rows repaired, the PHANTOM SURVIVES (the documented
/// caveat, asserted — if this test ever fails because the phantom died, the flavor semantics
/// changed and the docs are lying).
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn resync_repairs_drift_but_phantoms_survive() { todo!() }

/// During the resync: the table's claims never pause (its transformed_lsn keeps advancing on
/// stream traffic) and the mirror stays queryable.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn resync_never_pauses_the_table() { todo!() }

/// Raw receives the chunk rows (uniform Phase A path) — the open-question decision, pinned.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn resync_chunks_flow_through_raw() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `just reload table='public.orders' flavor='resync'` completes the full status walk with no
      pause, no `CREATE OR REPLACE`, no manifest purge, no meta-latch write.
- [ ] Stale and missing mirror rows are repaired; the planted phantom row survives — asserted,
      because it is the documented contract.
- [ ] Concurrent writes during the resync converge exactly as in 6.7's overlap tests (stream
      beats chunk stamp).
- [ ] A mid-resync DDL restarts the attempt through 6.8's path unchanged.
- [ ] Chunk rows appear in `<table>_raw` (the uniform-path decision), and the design doc's §6
      raw-semantics question is marked resolved with a pointer here.
- [ ] The flavor decision guide exists where operators will find it.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink -p loader` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p loader --test reload_resync -- --ignored`
        asserting all three tests above.

## What completed looks like

```
$ docker compose up --wait
$ duckdb data/public.orders.duckdb -c "DELETE FROM orders WHERE id=1"          # "missing"
$ duckdb data/public.orders.duckdb -c "INSERT INTO orders VALUES (9999, …)"    # the phantom
$ just reload table='public.orders' flavor='resync'
# … chunks drain; the table never pauses …
$ duckdb data/public.orders.duckdb -c "SELECT count(*) FROM orders WHERE id=1"     # 1 — repaired
$ duckdb data/public.orders.duckdb -c "SELECT count(*) FROM orders WHERE id=9999"  # 1 — the caveat
$ psql $CONTROL_URL -c "SELECT flavor, status FROM walrus.table_reload ORDER BY reload_id DESC LIMIT 1"
 flavor | status
--------+----------
 resync | complete
```

## Hints & gotchas

- The subtle correctness point: resync chunk rows stamped `L_i` can be *older* in dedup terms than
  what the mirror already holds (a stream event applied at `C > L_i` before the resync started).
  The per-PK max-applied-commit-LSN guard (PR 3.7) is what makes the chunk's stale copy a no-op
  instead of a regression — call it out in a comment; it's why resync is safe over a live table.
- "Missing" drift for the test must be planted in DuckDB directly (deleting via the source would
  emit a real tombstone and heal through the stream — that's not drift, that's CDC working).
- Resist making the loader look up flavor per chunk file — 6.7's trigger already fetched the
  reload row once; cache flavor by reload_id in the loader's in-memory table state.
- If the phantom-survives assertion feels wrong enough to "fix", that's the signal to use
  `flavor='reload'` — or to write the future full-diff pass as a *new* deferred goal, not to
  quietly mutate this one.

## References

- Design: `../../single-table-reload.md` H3, §6 (raw semantics — resolved here);
  `../../walrus-loader.md` §7 (the applied-LSN guard); Debezium incremental-snapshots blog
  (the shared phantom property).
- Prev: [PR 6.9](./pr-6.9-completion-crash-recovery.md) ·
  Next: [PR 6.11](./pr-6.11-reload-observability.md) · [Roadmap](../README.md)
