# PR 5.7 — Sink hot-path cleanup: proven bottlenecks only, with before/after numbers

> **Phase:** 5 — Performance & CI · **Crates touched:** `pg-sink`, `pg-to-arrow`, `common`,
> workspace root · **Est. size:** M · **Depends on:** PR 5.6 · **Unlocks:** PR 5.8

Now that 5.4/5.6 baselines exist, fix what they proved. Code inspection flagged three sink-side
candidates; this PR takes them **in measured-impact order and stops when the returns flatten** —
every change lands with its criterion delta recorded in `docs/benchmarks.md`, and behaviour is
pinned by the existing suites (golden vectors, integration, e2e). If a candidate's baseline share
turned out negligible, skip it and say so — "measured, not worth it" is a valid outcome this
curriculum wants you to practise writing down.

## Why — learning objectives

By the end of this PR you will have practised:

- **Optimizing against a baseline** — the discipline of one change → one bench run → one recorded
  delta, and declining changes the numbers don't justify.
- **Ownership over cloning** — restructuring an API from `&[T]` + internal `to_vec()` to passing
  `Vec<T>` through (the decoder already allocates the tuple; the batcher clones it again for no
  reason).
- **Amortizing serialization** — splitting a per-row JSON document into a batch-constant prefix and
  a per-row suffix written into a reused buffer, without changing the wire format.
- **Release-profile tuning** — what `lto = "thin"` and `codegen-units` actually buy on a real
  workload, measured not assumed.

## Read first

- `docs/benchmarks.md` — the 5.4 baseline shares (meta-JSON delta, text-heavy decode) and the 5.6
  bottleneck ranking. **This PR's scope is whatever those name, in that order.**
- `crates/pg-sink/src/batch.rs` — `TableBatcher::push` (`values.to_vec()` on every row) and
  `on_commit` (per-row `meta`/`batch_id` clones while draining `pending`).
- `crates/pg-to-arrow/src/batch.rs` — the `serde_json::to_string(meta)` per row; note which
  `SinkMeta` fields are batch-constant (epoch, batch_id, schema_version, source_schema/table,
  kind, sink_instance) vs per-row (op, lsn, commit_lsn, xid, unchanged_toast, sink_processed_at).
- `crates/common/src/sink_meta.rs` — `SinkMeta`'s serde shape; the JSON in `<table>_raw` and the
  loader's `json_extract_string` calls define the compatibility contract.

## Scope

**In scope** (each item gated on its baseline share; expected order:)

1. **Meta-JSON amortization.** Serialize the batch-constant fields once per sealed batch; per row,
   write only the varying fields into a reused `String` buffer (hand-assembled JSON or a two-struct
   serde split). **Byte-compatible output is the contract**: same keys, same formats — the loader's
   `$.op`/`$.commit_lsn`/`$.lsn`/`$.sink_processed_at` extraction and the e2e provenance assertions
   must pass unchanged. (Key order may shift; nothing may parse by position.)
2. **Kill the per-row clones.** `push(meta, values: Vec<TupleValue>)` takes ownership (the decode
   loop already owns the freshly parsed `Vec`); `on_commit` drains without re-cloning
   (`batch_id` as `Arc<str>` or stamped at append time). `estimate_row_bytes` must keep working —
   compute it before moving the values.
3. **`[profile.release]`:** add `lto = "thin"`; measure `codegen-units = 1` vs default on the 5.4
   suite and keep whichever wins meaningfully (record both).
4. Re-run the affected 5.4 benches + one 5.6 `mixed` run; append a before/after table to
   `docs/benchmarks.md` §History.

**Explicitly deferred** (do *not* build these here)

- Zero-copy text decode (borrowing `TupleValue::Text` from the frame buffer) — a lifetime-threading
  redesign of the decoder API; only justified if text-heavy decode still dominates *after* the
  clone fixes. Write the finding down for a future task instead.
- Concurrent per-table flushes (the single decode-task serialization) — changes ordering-sensitive
  durability machinery (§1.5/§1.6 invariants); needs its own carefully-tested PR if 5.6 shows S3
  PUT wall-time gating throughput.
- Loader-side fixes → **PR 5.8**.

## Files to create / modify

```
crates/pg-sink/src/batch.rs              # modify — ownership-taking push/on_commit
crates/pg-sink/src/consume.rs            # modify — pass owned Vec<TupleValue> through
crates/pg-to-arrow/src/batch.rs          # modify — amortized meta serialization
crates/common/src/sink_meta.rs           # modify (maybe) — split-serialization support
Cargo.toml                               # + [profile.release] lto = "thin" (+ measured cgu choice)
docs/benchmarks.md                       # modify — before/after deltas per change
```

## Skeleton

```rust
// crates/pg-sink/src/batch.rs  (shape)
impl TableBatcher {
    /// Takes ownership: the decoder already allocated this Vec; do not clone it again.
    pub fn push(&mut self, meta: SinkMeta, values: Vec<TupleValue>) -> Result<(), SinkError> {
        // estimate_row_bytes(&values) BEFORE the move; then self.pending.push((meta, values))
        todo!()
    }
}
```

```rust
// crates/pg-to-arrow/src/batch.rs  (shape)
/// Per-batch: the serialized batch-constant JSON fragment, computed once at builder creation.
/// Per-row: reuse `self.meta_buf`, splice the varying fields, append_value(&self.meta_buf).
/// OUTPUT CONTRACT: identical keys/values to the old serde_json::to_string(meta) — proven by a
/// unit test comparing old-path vs new-path serialization for representative metas.
fn append_meta(&mut self, meta: &SinkMeta) { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Each landed change has a criterion before/after delta in `docs/benchmarks.md`; each skipped
      candidate has a one-line "measured X%, not taken" note. No unmeasured "optimizations".
- [ ] A unit test proves new meta JSON ≡ old meta JSON (parse both, compare `serde_json::Value`,
      for metas with and without `unchanged_toast`).
- [ ] The golden-vector suite, `pg-sink` integration tests, and the e2e provenance/type-matrix
      tests pass unchanged — zero behavioural drift.
- [ ] `[profile.release]` documented in-line: why thin LTO, what cgu setting won, with the numbers.
- [ ] One 5.6 `mixed` re-run recorded — the micro-wins visible (or honestly not) at system level.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] compose integration suite green (the sink tests exercise the changed paths live)

## Hints & gotchas

- Serialize-splice carefully: `unchanged_toast` is `Vec<String>` and often empty — make sure the
  empty case serializes exactly as before (`[]` vs omitted changes the loader's `json_contains`
  behaviour in the TOAST path; check what the current serde derive emits and preserve it).
- `sink_processed_at` is per-row (UTC, `Z`); don't accidentally hoist it into the constant prefix.
- The `on_commit` path stamps `commit_lsn` into each pending row's meta — that field is *pending →
  known at commit*, which is why it can't be in the constant prefix either (spill files override it
  loader-side, but the committed path must stay per-row).
- Moving `Vec<TupleValue>` through `push` changes call sites in `consume.rs` and the stream-demux
  buffer (`stream_txn.rs` keys buffered rows per xid) — follow the compiler; the borrow checker is
  doing the refactor with you.
- Thin LTO typically costs ~10–20 % link time for mid-single-digit runtime wins on dispatch-heavy
  code like `append_value`'s downcasts — that trade is usually right for release artifacts; let the
  bench decide cgu=1 (it can double release build time for another few percent).
- If criterion deltas are within noise, say so and stop — the DoD rewards the honest null result.

## References

- Design: `docs/architecture.md` §1.4 (the meta column contract); plan findings
  (`batch.rs:107/126`, `pg-to-arrow/batch.rs:145`).
- Prev: [PR 5.6](./pr-5.6-e2e-throughput-harness.md) ·
  Next: [PR 5.8](./pr-5.8-loader-hot-path.md) · [Roadmap](../README.md)
