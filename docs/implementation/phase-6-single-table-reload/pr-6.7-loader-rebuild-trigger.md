# PR 6.7 — the rebuild trigger: first `reload` file ⇒ `CREATE OR REPLACE`, latest-id wins

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/99

> **Phase:** 6 — single-table reload · **Crates touched:** `loader`, `control` ·
> **Est. size:** L · **Depends on:** PR 6.5, PR 6.6 · **Unlocks:** PR 6.8

The loader needs **no new handshake** to start a rebuild
([reload H8](../../single-table-reload.md#h8--watermarks-manifest-and-the-rebuild-trigger-compose--if-youre-explicit)):
it claims in `(lsn_end, id)` order as always, and the first `kind='reload'` file whose `reload_id`
is *greater than* the one recorded in `_walrus_meta` **is** the trigger — `CREATE OR REPLACE`
mirror + raw at the file's `schema_version`, record the id, purge the superseded pending rows,
then ordinary Phase A/B. Chunk and stream files interleave through the existing claim order, which
is the "same batch" property the original proposal wanted — it falls out of manifest ordering, no
special batch logic. The other half is restart hygiene (H9): a *stale* reload_id file (from a
superseded attempt whose purge raced the claim) must be retired unapplied, never re-trigger a
rebuild — with a bigint reload_id, "latest wins" is a numeric compare against the meta store. This
PR also retires the raw-history open question: a rebuild discards the table's raw CDC history in
DuckDB (S3 Parquet persists per GC policy) — acceptable for quarantine recovery, and now written
down.

## Why — learning objectives

By the end of this PR you will have practised:

- **Coordination through data, not channels** — the manifest row *is* the message; idempotent by
  the recorded id, ordered by the claim query, zero new plumbing between sink and loader.
- **DuckDB `CREATE OR REPLACE` semantics** — atomically swapping mirror + raw at a specific
  registry `schema_version` (the compaction path's trick, re-aimed).
- **Convergence by algebra** — why phantom rows die (the clear), why overlap heals (chunk stamp
  `L_i` loses to any newer stream event), why deletes-during-reload land in the `NOT MATCHED AND
  op='d'` no-op branch.
- **Quarantine as a state you exit** — the lossy-cast quarantine from PR 3.9 clears because the
  rebuild replaces the data instead of retrying the cast on it.

## Read first

- `../../single-table-reload.md` — H8 (all three interplay bullets), H3 (rebuild ≠ refresh — this
  PR is the rebuild), H9 (latest-id hygiene), §5 step 4.
- `crates/loader/src/duck.rs` — `_walrus_meta` (~98–101, ~240–299): the k/v store gaining a
  `reload_id` key; how compaction/full-rebuild recreates tables from registry descriptors
  (PR 3.11's machinery — reuse, don't re-derive).
- `crates/loader/src/transform.rs` + `../../walrus-loader.md` §5 — the MERGE branches the
  convergence argument leans on.
- PR 3.9's quarantine marker — find where the lossy-cast terminal state lives; the rebuild must
  clear it.

## Scope

**In scope**

- Claim-loop branch on `kind == 'reload'`, comparing the file's `reload_id` to
  `_walrus_meta['reload_id']` (absent ⇒ 0):
  - **greater** ⇒ rebuild: look up the reload row (flavor + `first_lsn`); `CREATE OR REPLACE`
    `<table>` + `<table>_raw` at the **file's** `schema_version`; clear the quarantine marker if
    set; purge superseded pending manifest rows (`kind <> 'reload' AND lsn_end <= first_lsn` —
    new `control` helper); set the meta key; then Phase A the file normally.
  - **equal** ⇒ ordinary Phase A append (chunks 2…n of the current attempt).
  - **less** ⇒ stale: retire the manifest row unapplied (delete + debug log).
- `resync`-flavor `kind='reload'` files take the plain-append path (no clear, no purge) — the
  full flavor lands in PR 6.10; here the branch just must not rebuild for them.
- The raw-history decision as a doc comment + a line in `walrus-loader.md`'s raw section.

**Explicitly deferred** (do *not* build these here)

- Flipping `complete` at `transformed_lsn ≥ H` → **PR 6.9**.
- The `resync` end-to-end story → **PR 6.10**. DDL-restart generation of stale files → **PR 6.8**
  (this PR simulates staleness directly in tests).

## Files to create / modify

```
crates/loader/src/duck.rs             # modify — reload_id meta key (get/set); rebuild-at-version entry
crates/loader/src/…                   # modify — claim-loop branch (greater/equal/less)
crates/control/src/manifest.rs        # modify — delete_superseded(epoch, table, first_lsn)
crates/control/src/reload.rs          # modify — get(reload_id) read used by the trigger
crates/loader/tests/reload_rebuild.rs # new — compose: convergence, phantom death, stale skip
.sqlx/                                # regenerate — cargo sqlx prepare
```

## Skeleton

```rust
// crates/loader/src/duck.rs  (additions, shape)

/// The highest reload_id this .duckdb has rebuilt for — the idempotency latch (H8). Absent ⇒ 0.
pub fn recorded_reload_id(/* conn */) -> Result<i64, LoaderError> { todo!() }
pub fn set_recorded_reload_id(/* conn, reload_id */) -> Result<(), LoaderError> { todo!() }

/// CREATE OR REPLACE <table> + <table>_raw at `schema_version`, from registry descriptors —
/// the PR 3.11 rebuild path scoped to one reload. Discards raw history in DuckDB by design
/// (S3 persists per GC): the quarantine-recovery tradeoff, documented here.
pub fn rebuild_for_reload(/* conn, table, schema_version */) -> Result<(), LoaderError> { todo!() }
```

```rust
// claim-loop branch (shape — lives where Phase A dispatches claimed files)

/// kind='reload' routing (H8/H9):
///   file.reload_id > recorded  ⇒ rebuild-then-append (first chunk of a new attempt)
///   file.reload_id == recorded ⇒ plain append (later chunks)
///   file.reload_id < recorded  ⇒ stale attempt: retire unapplied
/// resync-flavor files always take the plain-append arm (PR 6.10 completes that flavor).
async fn route_reload_file(/* … */) -> Result<(), LoaderError> { todo!() }
```

```rust
// crates/loader/tests/reload_rebuild.rs

/// Drift the mirror (insert a phantom row directly into DuckDB), full-reload via the real sink
/// path, write concurrent source changes DURING the export ⇒ final mirror == source exactly:
/// phantom gone, concurrent updates present, deletes tombstoned.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn rebuild_converges_mirror_to_source_and_kills_phantoms() { todo!() }

/// Manifest rows with lsn_end <= first_lsn are purged at trigger time; later stream rows survive
/// and apply after the chunks in (lsn_end, id) order.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn superseded_pending_rows_are_purged_not_applied() { todo!() }

/// A file carrying reload_id < recorded is retired without touching DuckDB.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn stale_reload_file_is_skipped_and_retired() { todo!() }

/// A table quarantined by a lossy cast (PR 3.9) exits quarantine through the rebuild.
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn rebuild_clears_the_lossy_cast_quarantine() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] End-to-end convergence: phantom row killed by the clear, rows updated mid-export end at the
      stream's newer value (chunk stamp `L_i` loses dedup), rows deleted mid-export are absent,
      a delete for a row the rebuilt mirror never saw no-ops through the MERGE's
      `NOT MATCHED AND op='d'` branch.
- [x] The rebuild happens exactly once per reload_id (meta latch) — replaying the same chunk file
      after a crash does not re-clear the table.
- [x] Superseded pending rows (`lsn_end ≤ first_lsn`, non-reload kinds) are deleted at trigger
      time; stream files with `lsn_end > first_lsn` survive and apply **after** the chunks.
- [x] Stale-id files are retired unapplied; `resync`-flavor files never trigger a rebuild.
- [x] The quarantine marker clears; the table's watermarks resume from `W` forward — monotonic,
      no rewind (`checkpoint.rs` untouched).
- [x] The raw-history decision is written where raw semantics are documented.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader -p control` (and `--workspace` stays green); `cargo sqlx prepare --check`
  - [x] `docker compose up --wait` then `cargo test -p loader --test reload_rebuild -- --ignored`
        asserting all four tests above.

## What completed looks like

```
$ docker compose up --wait
$ duckdb data/public.orders.duckdb -c "INSERT INTO orders VALUES (9999, …)"   # the phantom
$ just reload table='public.orders'
$ psql $SOURCE_URL -c "UPDATE public.orders SET note='mid-reload' WHERE id=1; DELETE FROM public.orders WHERE id=2"
# … export drains, pause lifts, loader rebuilds and applies chunks + stream …
$ duckdb data/public.orders.duckdb -c "SELECT count(*) FROM orders WHERE id=9999"   # 0 — phantom dead
$ duckdb data/public.orders.duckdb -c "SELECT note FROM orders WHERE id=1"          # 'mid-reload'
$ duckdb data/public.orders.duckdb -c "SELECT (SELECT v FROM _walrus_meta WHERE k='reload_id')"  # 7
```

Mirror equals source, row for row — the diff query in the e2e harness returns empty.

## Hints & gotchas

- Order of operations at the trigger: rebuild DuckDB → clear quarantine → purge manifest → set the
  meta latch → append the triggering file. Crash anywhere before the latch ⇒ the redo re-runs the
  whole trigger idempotently (CREATE OR REPLACE is; the purge is; quarantine-clear is). Crash
  *after* the latch but before the append ⇒ the file is still claimed/ready and takes the
  `equal ⇒ append` arm. Walk this in a comment.
- The purge predicate is `kind <> 'reload' AND lsn_end <= first_lsn`: chunk 1 itself has
  `lsn_end = first_lsn` and must survive its own purge. Distinct transactions have distinct commit
  LSNs, but the `kind` filter is what makes the intent unambiguous — keep it.
- Rebuild at the **file's** `schema_version`, not "current" — PR 6.8 guarantees all of an
  attempt's chunks share one version, so the file's version is the attempt's version by
  construction.
- Pre-reload backlog (claimed between `export_complete` and the first chunk file) applies into the
  old mirror and is then thrown away by the clear — wasted work, bounded by one pause window, and
  the chunks re-cover every one of those commits (`C < L_1` ⇒ visible to chunk 1's SELECT). Don't
  optimise it away in this PR.
- The trigger runs under the table's ownership lease (PR 3.1); the reload lease (PR 6.4) is the
  *sink's*. They fence different actors — note the deferred-goal-§2 interaction, build nothing.

## References

- Design: `../../single-table-reload.md` H3, H8, H9, §5 step 4;
  `../../walrus-loader.md` §5 (MERGE branches), §5.7 (rebuild/compaction precedent);
  `../../architecture.md#18-single-slot-for-life--total-restart` (quarantine).
- Prev: [PR 6.6](./pr-6.6-loader-pause-claims.md) ·
  Next: [PR 6.8](./pr-6.8-ddl-invalidation-restart.md) · [Roadmap](../README.md)
