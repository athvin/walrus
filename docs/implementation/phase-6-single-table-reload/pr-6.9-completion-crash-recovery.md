# PR 6.9 — completion & crash recovery: `export_complete`, `transformed_lsn ≥ H`, resume

> **Status:** 📋 Planned

> **Phase:** 6 — single-table reload · **Crates touched:** `pg-sink`, `loader`, `control` ·
> **Est. size:** M · **Depends on:** PR 6.7 · **Unlocks:** PR 6.10

Two endings, both owned by services that already exist
([reload H10](../../single-table-reload.md#h10--the-snowflake-sink-updates-the-state-when-its-complete--there-is-no-snowflake-sink)):
the **sink** flips `export_complete` carrying the final chunk watermark `H` when the table drains;
the **loader** flips `complete` once `transformed_lsn ≥ H` — meaning every chunk *and* every
overlapping stream event through `H` is in the mirror. Any future downstream consumer observes
`status='complete'` in control-pg; nobody gets a write path into someone else's state row. The
other half is crash recovery, and its one rule (H7): **WAL redelivery is not a recovery mechanism**
— by the time the sink restarts, the signals' LSNs are behind `confirmed_flush`, acked and gone.
Control-pg is the recovery path: on startup, scan for non-terminal reloads whose lease is ours or
expired, adopt, and resume from the chunk cursor.

## Why — learning objectives

By the end of this PR you will have practised:

- **Completion as a watermark predicate** — "done" is `transformed_lsn ≥ H`, a monotonic
  comparison the loader already knows how to make, not a new message.
- **Recovery from durable state, not from the stream** — the Debezium-offsets lesson: the cursor
  in control-pg is the *only* thing a resume needs.
- **Lease adoption** — distinguishing "mine, restarting" from "expired, orphaned" from "held,
  leave it alone" at startup.
- **Kill-testing** — compose tests that SIGKILL a service mid-flight and assert the invariant
  survived, PR 4.4's crash-safety discipline aimed at a new subsystem.

## Read first

- `../../single-table-reload.md` — H7 (recovery), H10 (completion ownership), §5 step 6 (the
  status walk's ending).
- `crates/pg-sink/src/reload_export.rs` (PR 6.5) — the drain detection this PR gives an ending to.
- `crates/control/src/checkpoint.rs` — where `transformed_lsn` advances; the loader's completion
  check reads the same watermark.
- `tests/e2e` crash-safety tests (PR 4.4) — the kill-and-assert harness shape.

## Scope

**In scope**

- Sink: on drain (short chunk), `complete_export(reload_id, H)` where `H` = the last chunk's
  `L_n`; the exporter task ends; the semaphore permit frees.
- Loader: each cycle, for tables with an `export_complete` reload — once
  `transformed_lsn ≥ final_lsn`, call `control::reload::complete(reload_id)`. Idempotent;
  at-least-once safe.
- Sink startup scan: non-terminal reloads with `lease_holder = me` **or** `lease_expiry < now()`
  ⇒ re-acquire the lease and resume: `exporting` rows re-enter the chunk loop at the cursor;
  `requested` rows go through normal pickup. Live-lease rows are left alone (single-sink today,
  future-proof anyway).
- Stuck-state surfacing: a non-terminal reload with an expired, unadopted lease is logged at warn
  each controller tick (the alert rule is PR 6.11's).

**Explicitly deferred** (do *not* build these here)

- Alert rules and the runbook's "unstick a reload" section → **PR 6.11**.
- Loader-crash-during-rebuild — already covered by PR 6.7's latch walk-through; no new machinery.
- `resync` completion — identical predicate, arrives with the flavor in **PR 6.10**.

## Files to create / modify

```
crates/pg-sink/src/reload_export.rs      # modify — drain ⇒ complete_export(H)
crates/pg-sink/src/reload.rs             # modify — startup scan + lease adoption; stuck-warn
crates/loader/src/…                      # modify — completion check in the per-table cycle
crates/control/src/reload.rs             # modify — resumable_by(holder) read; complete() from 6.1
crates/pg-sink/tests/reload_recovery.rs  # new — compose: kill mid-export ⇒ resume, complete
.sqlx/                                   # regenerate — cargo sqlx prepare
```

## Skeleton

```rust
// crates/control/src/reload.rs  (addition, shape)

/// Startup scan (H7): non-terminal reloads this sink may take — its own lease, or an expired one.
/// Re-acquires the lease in the same statement (guarded UPDATE … RETURNING) so two racing pods
/// can't both adopt.
pub async fn adopt_resumable(
    /* executor, epoch, holder, lease_ttl_secs */
) -> Result<Vec<ReloadRow>, ControlError> { todo!() }
```

```rust
// crates/pg-sink/tests/reload_recovery.rs

/// SIGKILL the sink after chunk k of n; restart it. The reload resumes at the cursor (no chunk
/// ≤ k re-exported — manifest row count for those chunks unchanged), drains, flips
/// export_complete with H = last chunk's watermark, and the loader flips complete only after
/// transformed_lsn ≥ H. Full status history: requested → exporting → export_complete → complete.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn kill_mid_export_resumes_from_cursor_and_completes() { todo!() }

/// complete is flipped by the LOADER, and only once transformed_lsn ≥ H — freeze the loader,
/// verify export_complete holds; unfreeze, verify complete.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn complete_waits_for_transformed_lsn_to_reach_h() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] A drained export flips `export_complete` with `final_lsn = H =` the last chunk's watermark;
      the exporter task exits and its concurrency permit frees.
- [ ] The loader flips `complete` exactly when `transformed_lsn ≥ H` — asserted both ways
      (holds at `export_complete` while the loader is frozen; flips after it catches up).
- [ ] SIGKILL mid-export + restart: the reload resumes from the cursor, re-exports nothing at or
      before it, and completes — the kill test passes repeatedly (run it 3×; crash timing varies).
- [ ] Startup adoption is race-safe (guarded UPDATE); a live foreign lease is never stolen.
- [ ] An expired, unadopted lease warns per tick with the reload_id and holder.
- [ ] The full status history lands in order; no state is skipped, none flips twice.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink -p loader -p control` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test reload_recovery -- --ignored`
        asserting both tests above.

## What completed looks like

```
$ docker compose up --wait && just reload table='public.orders'
$ docker compose kill walrus-pg-sink            # mid-export, chunk 2 of 3 done
$ docker compose up -d walrus-pg-sink
# sink log:
INFO  adopting reload  reload_id=12 status=exporting cursor_chunk=2 lease=walrus-sink-0
INFO  reload export complete  reload_id=12 chunks=3 final_lsn=0/1C40210
$ psql $CONTROL_URL -c "SELECT status, chunk_no, final_lsn FROM walrus.table_reload WHERE reload_id=12"
     status      | chunk_no | final_lsn
-----------------+----------+-----------
 export_complete |        3 | 0/1C40210
# … loader drains chunks + stream past H …
$ psql $CONTROL_URL -c "SELECT status FROM walrus.table_reload WHERE reload_id=12"
  status
----------
 complete
```

## Hints & gotchas

- `H` is data the sink already has — the last `advance_cursor`'s watermark. Don't re-derive it
  from the manifest; pass it through `complete_export` and let a CHECK (`final_lsn ≥ first_lsn`)
  catch confusion.
- The loader's completion check belongs *after* its Phase B advance in the cycle, reading the
  watermark it just wrote — checking before transforms adds a full poll of latency for nothing.
- `transformed_lsn ≥ H` covers chunk files because chunks enter Phase A/B like any file; it covers
  overlap because overlapping stream events sort after the chunks. One predicate, no bookkeeping.
- The kill test's sharp edge is the chunk-boundary crash (after S3 PUT, before cursor advance) —
  PR 6.5's ordering makes that a safe duplicate; assert the mirror is still exact, not that zero
  duplicate work happened.
- Adoption must not fight PR 6.4's pickup: `requested` rows with expired leases go through normal
  pickup, `exporting` rows through adoption — make the two queries disjoint on status.

## References

- Design: `../../single-table-reload.md` H7, H10, §5 step 6;
  `../../walrus-loader.md` §4 (watermarks); PR 4.4's crash-safety harness.
- Prev: [PR 6.8](./pr-6.8-ddl-invalidation-restart.md) ·
  Next: [PR 6.10](./pr-6.10-resync-flavor.md) · [Roadmap](../README.md)
