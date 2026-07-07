# PR 2.25 — Manifest INSERT (`lsn_end` = commit LSN) — durability step (b)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/45

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib),
> `control` · **Est. size:** S · **Depends on:** PR 2.24 · **Unlocks:** PR 2.26

The middle link of the durability chain. After the Parquet PUT is durable (PR 2.24), **commit** a
`file_manifest` row marking the object `ready` for the loader — `kind='stream'`, `lsn_end` = the batch's
**commit LSN** (never the max row LSN), epoch- and schema-version-stamped. The row is the loader's
work-queue entry. This must land **after** the PUT and **before** the slot advances (PR 2.26): if the
process dies after the PUT but before this commit, the batch re-streams and re-writes — at-least-once, no
loss.

## Why — learning objectives

By the end of this PR you will have practised:

- **Reusing a `control` model from the sink** — calling `control::file_manifest::insert_ready` (PR 1.4)
  from the sink instead of hand-rolling SQL, keeping the manifest contract in one place.
- **Commit-LSN discipline** — persisting `lsn_end` = commit LSN so the loader claims/orders by
  `(lsn_end, id)` and never skips a late-committing large txn (the correctness-critical rule).
- **Durable-write ordering** — the explicit PUT → COMMIT manifest sequence, and why a crash between them
  is safe.
- **Mapping a `WrittenObject` to a manifest row** — carrying `s3_uri`, `row_count`, `lsn_start/end`,
  `schema_version`, `epoch` through the seam.

## Read first

- `../../architecture.md` §1.5 The durability checkpoint (`#15-the-durability-checkpoint-wal-bounding-invariant`) —
  the invariant and the per-batch ordering **PUT → COMMIT manifest INSERT → standby update**.
- `../../architecture.md` "Coordination contract" (`#coordination-contract-control-plane-tables`) — the
  `file_manifest` columns, the `WHERE status='ready'` index, and **why every progress key is a COMMIT
  LSN, not a row LSN**.
- Prior PR 1.4 (`file_manifest` `insert_ready` / `claim_ready ORDER BY lsn_end, id` / `delete_claimed`),
  whose `insert_ready` this PR calls.

## Scope

**In scope**

- After a durable `WrittenObject`, INSERT one `file_manifest` row via `control`:
  `status='ready'`, `kind='stream'`, `lsn_start`, `lsn_end` = commit LSN, `row_count`, `s3_uri`,
  `schema_version`, `epoch`, `source_schema`, `source_table`.
- Enforce the ordering: manifest INSERT happens **only after** the PUT returns durable, and the row's
  commit is what makes the batch "durable" for PR 2.26.
- Idempotent-enough behaviour on retry (a duplicated INSERT after a crash produces a second `ready` row
  for the same object — the loader's row-level `ON CONFLICT` absorbs it; document this).

**Explicitly deferred** (do *not* build these here)

- Advancing `confirmed_flush_lsn` after the manifest commit → **PR 2.26**.
- **Speculative** (no `ready` row) staging for open streamed txns → **PR 2.30** (this PR is committed-only).
- `kind='snapshot'` rows with `lsn_end = consistent_point` → **PR 2.29**.
- `failed`/dead-letter transitions → later (loader-side lifecycle).

## Files to create / modify

```
crates/pg-sink/src/manifest.rs       # new — thin adapter: WrittenObject -> control::file_manifest::insert_ready
crates/pg-sink/src/consume.rs        # modify — after ParquetSink::put, commit the manifest row
crates/pg-sink/tests/manifest_insert.rs  # new — compose: object + row exist; lsn_end == commit LSN
```

## Skeleton

```rust
// crates/pg-sink/src/manifest.rs
use crate::sink::WrittenObject;

/// Record a durable object as a `ready` work-queue row. Called ONLY after the PUT is durable.
pub async fn record_ready(
    control: &control::Control,
    epoch: i64,
    obj: &WrittenObject,
) -> Result<i64, ManifestError> { // returns the manifest id
    todo!()
}

/// Build the row from the written object (kind='stream', lsn_end = commit LSN).
fn to_ready_row(epoch: i64, obj: &WrittenObject) -> control::NewManifestRow { todo!() }

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error(transparent)]
    Control(#[from] control::Error),
}
```

```rust
// crates/pg-sink/src/consume.rs  (durability ordering, steps a+b)
/// PUT to S3 (a) THEN commit the manifest row (b). Step (c) — advance slot — is PR 2.26.
pub async fn flush_batch(
    sink: &crate::sink::ParquetSink,
    control: &control::Control,
    epoch: i64,
    batch: crate::batch::SealedBatch,
) -> anyhow::Result<crate::sink::WrittenObject> {
    // let obj = sink.put(batch).await?;           // (a) durable in S3
    // manifest::record_ready(control, epoch, &obj).await?; // (b) committed in control DB
    // return obj (PR 2.26 advances the slot to obj.lsn_end)
    todo!()
}
```

```rust
// crates/pg-sink/tests/manifest_insert.rs
#[tokio::test] async fn object_and_manifest_row_both_exist_after_flush() { todo!() }
#[tokio::test] async fn manifest_lsn_end_equals_commit_lsn() { todo!() }
#[tokio::test] async fn row_is_ready_kind_stream_and_epoch_stamped() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] After a durable PUT, exactly one `file_manifest` row is committed via `control` with
      `status='ready'`, `kind='stream'`, epoch + `schema_version` stamped.
- [x] `lsn_end` on the row equals the batch's **commit LSN** (proven against a batch whose commit LSN
      differs from its max row LSN).
- [x] The manifest INSERT happens **strictly after** the PUT returns durable (ordering is enforced in
      `flush_batch`, not incidental).
- [x] A crash *before* the manifest commit leaves no `ready` row → the batch re-streams (documented;
      the loader's row-level `ON CONFLICT` covers a duplicate).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test manifest_insert`: object at key +
        `ready` manifest row both exist, and `lsn_end == commit LSN`.

## Hints & gotchas

- **Do not** open your own SQL for the manifest — call the `control` model from PR 1.4 so the `WHERE
  status='ready'` partial index and the `(lsn_end, id)` ordering contract stay in one place.
- `lsn_end` is the **commit LSN of the file's last transaction**, not `max(row.lsn)`. This is the bug the
  design spends a whole callout on: a max-row-LSN watermark silently drops a late-committing large txn.
  Carry the commit LSN from `SealedBatch`, don't recompute from rows.
- The manifest row's `id` (bigserial) is the tiebreaker for equal-`lsn_end` files (matters for snapshot
  files in PR 2.29, all sharing `consistent_point`). Don't try to make `lsn_end` unique.
- Keep this adapter thin — it maps `WrittenObject` → `NewManifestRow` and delegates. The correctness
  lives in `control` (PR 1.4) and in the ordering enforced by `flush_batch`.
- The manifest is a **work queue, not a history**: the loader *deletes* the row once appended. Don't add
  a "processed" flag here.

## References

- Design: `../../architecture.md` §1.5, "Coordination contract".
- Prev: [PR 2.24](./pr-2.24-sink-parquet-s3-put.md) · Next: [PR 2.26](./pr-2.26-sink-durability-checkpoint.md) · [Roadmap](../README.md)
