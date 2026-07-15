# PR 6.8 — restart-on-DDL: every reload is single-schema by construction

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/100

> **Phase:** 6 — single-table reload · **Crates touched:** `pg-sink`, `control` ·
> **Est. size:** M · **Depends on:** PR 6.5 · **Unlocks:** PR 6.9

Chunked export is what makes mid-reload DDL a real case: each chunk is a short statement, so an
`ALTER` slips in **between** chunks instead of queueing behind an hours-long COPY
([reload H9](../../single-table-reload.md#h9--ddl-landing-mid-reload)). The chosen policy is
**restart-on-DDL**: a schema change past the reload's first watermark invalidates the attempt —
fresh `reload_id`, purge the superseded chunk files, re-export from chunk zero at the newest
schema. Every attempt is single-schema *by construction*; the loader never handles
version-crossing inside a rebuild, only in the stream, where that logic already runs and is
tested. The failure mode is wasted export work — bounded and visible — guarded by a restart cap so
a migration-heavy week can't livelock a huge table's reload forever. (Per-chunk version tolerance
was considered and rejected: it exercises the loader's least-hardened corner against a
half-populated table, and its failure mode is silent mis-reconciliation. The design doc keeps it
as the revisit-if-measured alternative.)

## Why — learning objectives

By the end of this PR you will have practised:

- **Invalidate-and-retry over tolerate-and-reconcile** — choosing the design whose failure mode is
  *visible waste* instead of *silent corruption*, and writing the cap that bounds the waste.
- **Multi-statement state transitions** — fail-old + purge + insert-new as one control-pg
  transaction, so no observer ever sees two live attempts or an orphaned chunk file.
- **In-process signal reuse** — the sink already *writes* `ddl_manifest`; detecting "DDL landed on
  my table" is a version comparison, not a new watcher.

## Read first

- `../../single-table-reload.md` — H9 in full (the lock-queue reality, both candidates, why
  restart won), §5 step 5.
- `crates/pg-sink/src/ddl.rs` (PR 2.33) — where the sink bumps `schema_version`; the exporter's
  per-chunk check reads the same source of truth.
- PR 6.1's `fail()` — the same-transaction manifest purge this PR composes with;
  PR 6.5's `export_chunk` — where the check slots in.

## Scope

**In scope**

- Per-chunk staleness check: before each chunk, compare the table's current structural
  `schema_version` (registry / relation cache latest) against the attempt's frozen
  `table_reload.schema_version`. Equal ⇒ proceed; changed ⇒ restart procedure.
- `control::reload::restart_for_ddl(old_reload_id, reason)` — one transaction:
  `fail(old, "superseded: ddl bumped schema_version to N")` (which purges old chunk files, PR 6.1)
  + INSERT the successor row (`status='exporting'`, same table/flavor/lease,
  `restart_count = old + 1`, fresh cursor) — returns the new `reload_id`. Unless
  `restart_count + 1 > reload_max_restarts`: then fail-only + increment the cap-exhausted metric;
  no successor.
- The exporter swaps to the new `reload_id`, re-resolves the table shape at the new version, and
  starts from chunk zero.
- Config: `reload_max_restarts` (default 3, ≥ 0). Metrics: `walrus_reload_restarts_total`,
  cap-exhaustion counter (registered properly in PR 6.11's sweep; emit here).

**Explicitly deferred** (do *not* build these here)

- Loader-side anything — stale-file skipping already landed in **PR 6.7**; the purge makes stale
  files rare, 6.7's latch makes them harmless.
- Alert rules / dashboards on the new counters → **PR 6.11**.
- Per-chunk version tolerance — explicitly **not** built; the design doc records the revisit
  trigger ("restart churn on DDL-heavy tables becomes a measured problem").

## Files to create / modify

```
crates/pg-sink/src/reload_export.rs   # modify — per-chunk staleness check; attempt swap
crates/control/src/reload.rs          # modify — restart_for_ddl (fail-old + insert-successor, one txn)
crates/pg-sink/src/config.rs          # modify — reload_max_restarts
crates/common/src/metrics.rs          # modify — restarts + cap-exhausted counters
crates/pg-sink/tests/reload_ddl.rs    # new — compose: mid-export ALTER ⇒ restart; cap ⇒ failed
.sqlx/                                # regenerate — cargo sqlx prepare
```

## Skeleton

```rust
// crates/control/src/reload.rs  (addition, shape)

/// H9 restart: in ONE transaction, fail the old attempt (purging its kind='reload' manifest rows
/// via fail()'s coupling) and insert its successor with restart_count+1 — or, past the cap,
/// fail-only and return None. The partial unique index allows the successor only because the
/// predecessor turns terminal in the same transaction.
pub async fn restart_for_ddl(
    /* pool/txn, old: &ReloadRow, new_schema_version: i64, max_restarts: i32 */
) -> Result<Option<i64>, ControlError> { todo!() }
```

```rust
// crates/pg-sink/src/reload_export.rs  (change shape)

impl ChunkExporter {
    /// Before each chunk: the attempt exports at exactly one schema_version (frozen on chunk 1).
    /// A structural bump past it ⇒ Err(SchemaChanged{new_version}) ⇒ the controller runs
    /// restart_for_ddl and either relaunches at the new version or stops (cap).
    async fn check_schema_still_current(&self) -> Result<(), ExportInterrupt> { todo!() }
}

#[cfg(test)]
mod tests {
    #[test] fn schema_bump_between_chunks_interrupts_with_new_version() { todo!() }
    #[test] fn restart_cap_zero_means_first_ddl_fails_the_reload() { todo!() }
}
```

```rust
// crates/pg-sink/tests/reload_ddl.rs

/// Small chunks; ALTER TABLE ADD COLUMN mid-export ⇒ old row failed('superseded: …'),
/// successor row exporting with restart_count=1, old chunk files gone from the manifest,
/// reload completes at the new schema and the mirror has the new column.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn mid_export_ddl_restarts_fresh_attempt_at_new_schema() { todo!() }

/// reload_max_restarts=0 ⇒ the same DDL fails the reload outright; cap-exhausted counter = 1.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn restart_cap_exhaustion_fails_loudly() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] An `ALTER TABLE` landing between chunks produces: old attempt `failed` with the superseded
      reason, zero old chunk files left in the manifest, a successor attempt with
      `restart_count = 1` exporting from chunk zero at the new `schema_version` — and the reload
      then completes with the mirror in the new shape.
- [x] No moment exists (transactionally) with two non-terminal reloads for the table or with a
      terminal attempt's chunk files still claimable.
- [x] Metadata-only DDL (e.g. `COMMENT ON`) does **not** restart — the check compares structural
      versions only (PR 2.33's split).
- [x] Past `reload_max_restarts`, the reload fails with the cap in the error text and the
      cap-exhausted counter increments; no successor row appears.
- [x] `walrus_reload_restarts_total` counts each restart.
- [x] Docs/comments carry the H9 tradeoff and the revisit trigger.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink -p control` (and `--workspace` stays green); `cargo sqlx prepare --check`
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test reload_ddl -- --ignored`
        asserting both tests above.

## What completed looks like

```
$ docker compose up --wait && just reload table='public.orders'
$ psql $SOURCE_URL -c "ALTER TABLE public.orders ADD COLUMN priority int"   # mid-export
$ psql $CONTROL_URL -c "SELECT reload_id, status, restart_count, left(error, 40) AS error
                        FROM walrus.table_reload WHERE source_table='orders' ORDER BY reload_id"
 reload_id |  status   | restart_count |                error
-----------+-----------+---------------+--------------------------------------
         9 | failed    |             0 | superseded: ddl bumped schema_version
        10 | exporting |             1 |
# … and later:
        10 | complete  |             1 |
$ duckdb data/public.orders.duckdb -c "DESCRIBE orders" | grep priority     # the new column, present
```

## Hints & gotchas

- The check is cheap — the sink's own relation cache / registry already knows the latest
  structural version (`latest_for` in `relcache.rs`); don't add a per-chunk catalog query against
  the source.
- Check → signal-INSERT → echo → SELECT still leaves a window where DDL lands *after* the check
  and *before* the SELECT. That chunk exports at the old shape while the version bumps — the
  *next* chunk's check catches it and the restart throws that file away with the rest. Harmless,
  but only because the purge is total; say so in a comment.
- `restart_for_ddl` must reuse `fail()` rather than duplicating its purge — one place owns
  "terminal ⇒ no claimable files".
- Carrying the lease onto the successor row skips a pickup round-trip; make sure lease renewal
  keys on `reload_id` and follows the swap.
- Quarantine recovery composes for free: a restart re-exports at the newest schema, which is
  exactly where a lossy-cast recovery wants to land — the design doc's H9 closing point; worth a
  test-name nod in PR 6.12.

## References

- Design: `../../single-table-reload.md` H9 (both candidates + rationale), §5 step 5;
  `../../walrus-pg-sink.md` §3 (structural-vs-metadata split the check reuses).
- Prev: [PR 6.7](./pr-6.7-loader-rebuild-trigger.md) ·
  Next: [PR 6.9](./pr-6.9-completion-crash-recovery.md) · [Roadmap](../README.md)
