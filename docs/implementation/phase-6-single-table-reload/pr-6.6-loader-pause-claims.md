# PR 6.6 — pause the table's claims while a rebuild is in flight

> **Status:** 📋 Planned

> **Phase:** 6 — single-table reload · **Crates touched:** `control`, `loader` ·
> **Est. size:** S · **Depends on:** PR 6.1 · **Unlocks:** PR 6.7

"Stop processing table X" is nearly free in walrus — the loader discovers work only through
`file_manifest`, so pausing is *not claiming*
([reload §2](../../single-table-reload.md#2-what-the-proposal-gets-right)): rows accumulate as
`ready`, the Parquet stays in S3, the table's frontier freezes at `W`, and nothing is buffered or
lost. The pause is what makes the rebuild sound: if the loader kept claiming during the export, it
would apply-and-**retire** post-`W` stream files into the old mirror — and the rebuild would then
clear that mirror with those events gone from the queue forever. Freezing claims keeps every
post-`W` event in the manifest so the rebuild replays the world in `(lsn_end, id)` order. The
pause covers `requested`/`exporting` only and lifts at `export_complete` — and it applies **only**
to the `reload` flavor; `resync` never pauses (H3).

## Why — learning objectives

By the end of this PR you will have practised:

- **Correctness from a work-queue's shape** — retire-by-DELETE is why claiming-then-rebuilding
  loses data, and why not-claiming is a complete pause with zero record-level machinery.
- **`NOT EXISTS` claim predicates** — extending a hot claim query without a check-then-claim
  TOCTOU window.
- **Frontier reasoning** — why the frozen `W` plus chunk watermarks `L_i > W` means the
  checkpoint's monotonic `GREATEST()` never needs a rewind (H8).

## Read first

- `../../single-table-reload.md` — §2 ("pausing … is nearly free"), H8 (watermark monotonicity
  through the reload), §5 step 2, H3 (why only `reload` pauses).
- `crates/control/src/manifest.rs` — `claim_ready` (~78–103) and the module doc's claim-ordering
  invariants (this PR must not disturb them).
- `crates/control/src/checkpoint.rs` — the `GREATEST()` + CHECK watermark discipline the frozen
  frontier leans on.

## Scope

**In scope**

- `claim_ready` (or a thin wrapper the loader switches to) gains one predicate: skip tables with a
  live rebuild —
  `AND NOT EXISTS (SELECT 1 FROM walrus.table_reload r WHERE r.epoch = … AND r.source_schema = …
  AND r.source_table = … AND r.flavor = 'reload' AND r.status IN ('requested','exporting'))`.
  One query, no separate pre-check, no race between checking and claiming.
- The same predicate (or the PR 6.1 `active_rebuilds` read) surfaces in the loader's per-table
  loop so a paused table logs *why* it is idle once per pause, not per poll.
- Tests: claims for a paused table return empty; another table claims normally; `export_complete`
  / `complete` / `failed` all lift the pause; `resync` rows never pause anything.

**Explicitly deferred** (do *not* build these here)

- Acting on claimed `kind='reload'` files (the rebuild) → **PR 6.7**.
- Flipping `complete` when `transformed_lsn ≥ H` → **PR 6.9**.
- Alert-threshold handling for the paused table's growing `raw_append_lag_bytes` → **PR 6.11**
  (the lag grows *by design* during a pause; observability must say so).

## Files to create / modify

```
crates/control/src/manifest.rs        # modify — the NOT EXISTS pause predicate on claim_ready
crates/loader/src/…                   # modify — once-per-pause idle log in the per-table loop
crates/control/tests/…                # modify/new — pause/lift/other-table assertions
.sqlx/                                # regenerate — cargo sqlx prepare
```

## Skeleton

```rust
// crates/control/src/manifest.rs  (change shape)

/// Claim the next `ready` files for a table in commit order — UNLESS the table has a live
/// rebuild-flavor reload (requested|exporting), in which case claim nothing and let the rows
/// accumulate: the frontier freezes at W so the rebuild can replay everything (reload §2, H8).
/// resync never pauses (H3). The pause lifts at export_complete: pre-reload rows (lsn_end < L₁)
/// drain first into the old mirror — wasted-but-harmless — then the first reload file triggers
/// the rebuild (PR 6.7).
pub async fn claim_ready(/* unchanged signature */) -> Result<Vec<ManifestRow>, ControlError> {
    todo!() // existing query + the NOT EXISTS predicate
}
```

```rust
// crates/control/tests/reload_pause.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn live_rebuild_pauses_claims_for_that_table_only() { todo!() }

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn export_complete_and_terminal_states_lift_the_pause() { todo!() }

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn resync_flavor_never_pauses() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] With a `reload`-flavor row in `requested` or `exporting`, `claim_ready` for that table
      returns empty while its `ready` rows keep accumulating; a second table claims normally the
      whole time.
- [ ] Flipping the reload to `export_complete` (and equally `complete`/`failed`) makes the next
      claim return the backlog in unchanged `(lsn_end, id)` order.
- [ ] A `resync`-flavor row pauses nothing.
- [ ] The table's checkpoint watermarks simply stop advancing during the pause — no rewind, no
      CHECK violation, no special-casing in `checkpoint.rs`.
- [ ] The loader logs the pause reason once per pause (not once per poll).
- [ ] Docs/comments carry the why: claiming-then-rebuilding retires post-`W` events the rebuild
      can't replay.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p control -p loader` (and `--workspace` stays green); `cargo sqlx prepare --check`
  - [ ] `docker compose up --wait` then the three `reload_pause` assertions above, `-- --ignored`

## What completed looks like

```
$ docker compose up --wait
$ psql $CONTROL_URL -c "INSERT INTO walrus.table_reload (epoch, source_schema, source_table, flavor)
                        VALUES (1, 'public', 'orders', 'reload')"
$ psql $SOURCE_URL  -c "UPDATE public.orders SET note = 'during pause' WHERE id <= 5"
# loader log (once):
INFO  claims paused for public.orders  reason=rebuild-in-flight reload_id=8
$ psql $CONTROL_URL -c "SELECT count(*) FROM walrus.file_manifest
                        WHERE source_table='orders' AND status='ready'"    # grows, never shrinks
$ psql $CONTROL_URL -c "UPDATE walrus.table_reload SET status='failed', error='demo' WHERE reload_id=8"
# next poll: the backlog drains, transformed_lsn advances again
```

## Hints & gotchas

- Put the pause in the **query**, not in loader-side bookkeeping — the claim is the single choke
  point, and a predicate there can't drift out of sync with a cached table list.
- The correlated `NOT EXISTS` runs on every claim poll; `table_reload` is tiny, but give it the
  obvious index (`(epoch, source_schema, source_table) WHERE status IN (…)` is already close to
  the PR 6.1 partial unique index — check whether it's usable before adding another).
- Don't pause on `export_complete`: that is exactly when the loader must start claiming again to
  reach the first chunk file and trigger the rebuild (PR 6.7). Pausing through `export_complete`
  deadlocks the reload.
- `max_ready_lsn_end` (the lag gauge input) intentionally still sees the paused table's backlog —
  the lag metric *should* grow during a pause. Resist "fixing" it here; PR 6.11 documents it.

## References

- Design: `../../single-table-reload.md` §2, H3, H8, §5 step 2;
  `../../walrus-loader.md` §2 (manifest work-handoff), §4 (watermarks).
- Prev: [PR 6.5](./pr-6.5-sink-chunk-export-engine.md) ·
  Next: [PR 6.7](./pr-6.7-loader-rebuild-trigger.md) · [Roadmap](../README.md)
