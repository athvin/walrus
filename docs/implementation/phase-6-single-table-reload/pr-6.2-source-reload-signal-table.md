# PR 6.2 — the source-side signal table: `walrus.reload_signal`, published

> **Status:** 📋 Planned

> **Phase:** 6 — single-table reload · **Crates touched:** `migrations/source`, `pg-sink` ·
> **Est. size:** S · **Depends on:** PR 6.1 · **Unlocks:** PR 6.3

Exactly one thing about a reload must travel **in-band** through the WAL: the chunk-start
watermark, because its *commit LSN is the datum*
([reload H4](../../single-table-reload.md#h4--one-status-row-in-the-source-db-written-by-three-services-is-the-wrong-state-store)).
This PR creates that vehicle: `walrus.reload_signal`, an **insert-only** table in the source
`walrus` schema, added to the publication the same way `ddl_audit` was. It also wires the
preflight: a signal table missing from the publication is the classic silent failure of every
Debezium-style setup — the echo just never arrives — so the sink must refuse to start a reload
path against a misconfigured source (H11), not time out mysteriously mid-export.

## Why — learning objectives

By the end of this PR you will have practised:

- **Volatile column DEFAULTs** — `DEFAULT pg_current_wal_insert_lsn()` evaluates per-INSERT,
  giving every signal row its embedded cross-check LSN with zero sink-side SQL.
- **Publication membership as a correctness precondition** — an unpublished signal table doesn't
  error; it silently never echoes. Preflight is the only honest failure mode.
- **Insert-only signal semantics** — no REPLICA IDENTITY concerns, a free audit trail, idempotency
  by PK (H5); *and* why the eventual pruning DELETEs must be invisible to the sink too.
- **Schema-scoped exclusions** — the `walrus` schema is already outside backfill and the DDL tap;
  verifying an invariant you inherit rather than re-building it.

## Read first

- `../../single-table-reload.md` — H1 (what the embedded LSN is and is *not* — a cross-check, never
  the stamp), H5 (insert-only rationale), H11 (the preflight gaps this PR closes).
- `migrations/source/0002_ddl_triggers.sql` — the precedent this PR mirrors: a `walrus`-schema
  table created by migration and `ALTER PUBLICATION … ADD TABLE`'d; note line ~49's
  `CONTINUE WHEN v_schema = 'walrus'` (the DDL tap already ignores this schema).
- `crates/pg-sink/src/preflight.rs` — the `manage_publication` path (~213–241) and the house
  terminal-error style for missing source objects.
- `crates/pg-sink/src/snapshot.rs` — the backfill's `schemaname != 'walrus'` exclusion the DoD
  asserts against.

## Scope

**In scope**

- `migrations/source/0003_reload_signal.sql`: the table (skeleton below) + publication add.
- Preflight: verify `walrus.reload_signal` exists, has its PK, and is **in the publication**;
  under `manage_publication=true` add it automatically (same idiom as the existing membership
  management), otherwise missing ⇒ terminal error naming the exact `ALTER PUBLICATION` to run.
- A compose test proving a manual INSERT into the signal table arrives in the decoded stream, and
  that backfill/bootstrap never copies the table.

**Explicitly deferred** (do *not* build these here)

- The sink *consuming* the echo (routing, waiters) → **PR 6.3**. In this PR the decoded insert may
  still look like an unknown user table to the consume path — that's fine and expected.
- Writing signal rows from the exporter → **PR 6.5**.
- Pruning old signal rows → note it as a `// TODO` for the operator runbook (**PR 6.11**); the
  table grows one tiny row per chunk.

## Files to create / modify

```
migrations/source/0003_reload_signal.sql   # new — table + ALTER PUBLICATION ADD
crates/pg-sink/src/preflight.rs            # modify — existence + PK + publication-membership check
crates/pg-sink/tests/reload_signal.rs      # new — compose: decode-visibility + backfill exclusion
```

## Skeleton

```sql
-- migrations/source/0003_reload_signal.sql  (shape)

-- Chunk-start watermarks (reload §H1/H2). INSERT-ONLY: each row is one chunk's low-watermark
-- write; the sink learns L_i from the row's *echo* in the replication stream, never from here.
CREATE TABLE IF NOT EXISTS walrus.reload_signal (
    reload_id      bigint      NOT NULL,
    chunk_no       bigint      NOT NULL,
    -- Evaluated at insert time. The CROSS-CHECK, not the stamp: insert position precedes the
    -- commit record, so this is strictly < the echo's commit LSN (asserted in PR 6.3).
    wal_insert_lsn pg_lsn      NOT NULL DEFAULT pg_current_wal_insert_lsn(),
    inserted_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (reload_id, chunk_no)
);

ALTER PUBLICATION walrus_pub ADD TABLE walrus.reload_signal;   -- 0002's ddl_audit precedent
```

```rust
// crates/pg-sink/src/preflight.rs  (addition, shape)

/// H11: an unpublished signal table never echoes — the whole reload mechanism silently never
/// fires. Missing table/PK/publication membership is TERMINAL (or auto-fixed under
/// manage_publication=true), with the exact remediation SQL in the error text.
async fn verify_reload_signal(/* client, manage_publication */) -> Result<(), PreflightError> { todo!() }
```

```rust
// crates/pg-sink/tests/reload_signal.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn signal_insert_is_visible_in_decoded_stream() { todo!() }

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn backfill_never_copies_walrus_reload_signal() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Migration `0003` applies to a fresh source and to one already at `0002`; the table is in
      `walrus_pub` afterwards.
- [ ] A manual `INSERT INTO walrus.reload_signal (reload_id, chunk_no) VALUES (…)` produces a
      decoded insert in the replication stream, with `wal_insert_lsn` populated by the DEFAULT —
      no explicit LSN in the INSERT.
- [ ] With the table removed from the publication, sink preflight fails **terminal** and the error
      names the `ALTER PUBLICATION` fix; with `manage_publication=true` it is added automatically.
- [ ] Bootstrap/backfill never emits a snapshot file for `walrus.reload_signal` (the existing
      `walrus`-schema exclusion, now pinned by a test).
- [ ] Docs/comments state the insert-only rule and that `wal_insert_lsn` is a cross-check, not the
      stamp.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test reload_signal -- --ignored`

## What completed looks like

```
$ docker compose up --wait
$ psql $SOURCE_URL -c "INSERT INTO walrus.reload_signal (reload_id, chunk_no) VALUES (1, 1)
                       RETURNING reload_id, chunk_no, wal_insert_lsn"
 reload_id | chunk_no | wal_insert_lsn
-----------+----------+----------------
         1 |        1 | 0/1A2B3C4
$ psql $SOURCE_URL -c "SELECT tablename FROM pg_publication_tables
                       WHERE pubname='walrus_pub' AND schemaname='walrus'"
   tablename
---------------
 ddl_audit
 reload_signal
```

…and the sink's log shows the decoded insert for `walrus.reload_signal` flowing past (routing it
is PR 6.3's job).

## Hints & gotchas

- `pg_current_wal_insert_lsn()` needs no special privilege on modern Postgres, but it is
  **volatile** — a multi-row INSERT evaluates it per row. Signals are single-row; note it anyway.
- The PK doubles as REPLICA IDENTITY DEFAULT — sufficient for an insert-only table; do not reach
  for `REPLICA IDENTITY FULL`.
- Future pruning DELETEs on this table will also flow through the slot. PR 6.3's routing must
  ignore non-insert ops on the signal table — leave that breadcrumb in a comment here.
- Check how `0002` handles re-running `ALTER PUBLICATION … ADD TABLE` (duplicate add errors);
  guard with a `pg_publication_tables` existence check or exception block, same as the precedent.

## References

- Design: `../../single-table-reload.md` H1, H5, H11;
  `../../walrus-pg-sink.md#3-ddl-capture--the-sinks-tap-on-the-source` (the published-internal-table
  precedent); Debezium signalling docs (linked from the design doc's references).
- Prev: [PR 6.1](./pr-6.1-control-table-reload-state-machine.md) ·
  Next: [PR 6.3](./pr-6.3-sink-echo-routing-watermark.md) · [Roadmap](../README.md)
