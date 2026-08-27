# Cache-friendly layout: audited, no layout churn

> **Status:** audited 2026-08-27 — **no change.** Every hot traversal in walrus is already a forward
> pass over a contiguous slice whose elements are consumed whole, the columnar (SoA) half of the
> pipeline is Arrow itself, and no atomic is written per row — so AoS→SoA, cache-line padding, and
> pointer-chasing rewrites all have no target here. The measured footprint half of this rule is
> already recorded in `docs/benchmarks.md:211-224`.

## What the rule asks, technique by technique

| Rule technique | walrus verdict |
|---|---|
| Array-of-structs → struct-of-arrays | Inapplicable: no hot loop reads a narrow field of a wide element repeatedly. The columnar destination is Arrow. |
| Hot/cold field splitting | Applied where it exists — as cold *code* splitting and as the const/row provenance split. The residual is a measured deferral, not an oversight. |
| Prefetching / cache-line chunking | Already implicit: every hot path is a sequential iterator over `&[u8]`, `&[TupleValue]`, or `Box<[Emit]>`. |
| Avoid pointer chasing | No linked structure exists; contiguity is already the deliberate choice at the two index sites. |
| `#[repr(C, align(64))]` padding | Declined: no atomic is touched per row, so there is no false-sharing surface to pad. |

## Why array-of-structs is the correct shape here

The rule's `Particle` example is wrong for walrus because its precondition — a loop that touches a
subset of each element's fields — does not occur. The three row-shaped arrays are all consumed whole:

- `TableBatcher::pending: Vec<(SinkMeta, Box<[TupleValue]>)>`
  (`crates/pg-sink/src/batch.rs:136-141`) is drained exactly once, and `on_commit`
  (`:224-233`) patches three meta fields and appends both halves of every element in the same
  iteration. Splitting the pair into parallel vectors would turn one sequential stream into two for
  no fewer bytes read.
- `StreamedTxn::changes: Vec<StreamedChange>` (`crates/pg-sink/src/stream_txn.rs:37-55`) is filtered
  by `sub_xid` in `survivors` (`:110-121`) and by `(oid, sub_xid)` in `take_stream` (`:96-104`) — but
  in both cases the *matching* element is then read or moved in full. The predicate's bytes and the
  consumer's bytes share a cache line, which is precisely the AoS advantage; an SoA split would pay
  a second stream to avoid loading bytes it is about to need. `StreamedChange` carries an exact 40 B
  guard, so roughly 1.6 buffered rows already share each 64-byte line.
- `parse_stream`'s `Vec<Message>` (`crates/pg-sink/src/pgoutput/mod.rs:543-556`) holds one 88 B
  element per decoded WAL record, each matched and consumed once by the router.

The one place a narrow per-column array *does* pay off is already built that way: `BatchBuilder`
keeps the routing plan as `Box<[Emit]>` — one byte per source column, guarded exactly at
`crates/pg-to-arrow/src/batch.rs:43-47` — separate from the fat `Box<[Box<dyn ArrayBuilder>]>`. The
460 ns/row `append_row` loop (`:171-215`) then walks the plan, the builder slice, and the schema
fields as three parallel cursors advanced by slice splits. That is the rule's own SoA advice, already
applied at the only site where the widths differ by two orders of magnitude.

Downstream of that loop the layout *is* struct-of-arrays: Arrow column builders are one contiguous
buffer per field, which is what the sink writes to Parquet and what DuckDB reads back.

## Hot/cold splitting: done where the split is real

Cold *code* is already split out of both benchmarked hot paths — `downcast_error` and `row_len_error`
carry `#[cold]`/`#[inline(never)]` (`crates/pg-to-arrow/src/batch.rs:702-721`), as does the decoder's
`eof` (`crates/pg-sink/src/pgoutput/reader.rs:187-195`), each with the reason recorded in place.

The data-side split that matters was taken in PR 5.7: `SinkMeta` is serialized as a batch-constant
`MetaConst` fragment plus a per-row `MetaRow` (`crates/common/src/sink_meta.rs:242-276`), so after the
first row of a file only the seven varying fields are read.

What remains is that the *storage* still carries the batch-constant strings once per buffered row,
because `BatchRouter::push` builds a whole `SinkMeta` per change (`crates/pg-sink/src/consume.rs:833-849`).
Removing that duplication is an owned-string change, not a layout change, and it is already an
explicitly measured deferral: `docs/benchmarks.md:246-262` (PR 11.16) declines `Arc<str>`/compact
strings until allocation profiling names `SinkMeta` strings an end-to-end sink limiter, and
`docs/benchmarks.md:170-204` shows the loader saturating first with sink inflight at zero. Reopening
it under a cache rule, with no new measurement, would be the speculative optimization the benchmark
record already refused.

## Pointer chasing: nothing to unchain

There is no `LinkedList`, no `Option<Box<Node>>` chain, and no `Rc<RefCell<_>>` graph in the
workspace. The two index structures are contiguity choices already argued in place:

- `RelationCache` keys a `BTreeMap` on `(oid, schema_version)` **so that every version of one oid is
  a contiguous range**, which `latest_for` walks instead of scanning the map
  (`crates/pg-sink/src/relcache.rs:38-59`); the comment there also records why `IndexMap` was declined.
- `InflightMeter::spill_order` collects a one-shot `BinaryHeap<(u64, u32, u32)>` of 16-byte tuples
  (`crates/pg-sink/src/memory.rs:94-100`) — a flat array, rebuilt per shed episode rather than kept
  as a linked priority structure.

The remaining indirection is the `Box<dyn ArrayBuilder>` per column. It is required by arrow-rs's
heterogeneous builder API (`StructBuilder::field_builder` is itself downcast-based) and is one of the
three deliberate dynamic-dispatch sites tabulated in `crates/pg-sink/src/batch.rs:12-18`. Replacing it
with a hand-rolled builder enum would not remove the inner boxes for `List`/`Struct` children.

## False sharing: no per-row atomic to pad

The rule's `PaddedCounter` targets counters written on every operation from several cores. walrus has
no such counter:

- The sink's `HealthState` holds a phase byte and three `AtomicBool`s
  (`crates/pg-sink/src/health.rs:92-100`), written only on state transitions and read together by one
  probe handler — so sharing a single line is the *cheaper* arrangement, not a hazard. The loader's
  `AtomicPhase` (`crates/loader/src/health.rs:62`) is the same shape.
- `WatermarkWaiters`' two `AtomicU64`s (`crates/pg-sink/src/reload_signal.rs:46-54`) move once per
  reload chunk and once per cross-check violation.
- Prometheus counters are bumped per sealed batch or per file, never per row
  (`crates/common/src/metrics.rs:403-422`).
- The loader has no worker pool to contend: each `!Send` DuckDB connection is pinned to one task on a
  single-threaded `LocalSet` (`crates/loader/src/main.rs:147-153`). Production code cannot grow one
  by accident either — `scripts/check-no-os-threads.sh` rejects direct `std::thread::spawn`, and
  `deny.toml:70-75` bans `rayon`/`rayon-core` outright.

Consistent with that, the tree carries `#[repr(transparent)]`, `#[repr(i32)]`, and `#[repr(u8)]` but
no `align(N)` anywhere. Adding 64-byte padding would inflate every guarded footprint above — the same
footprints `docs/benchmarks.md:211-224` measures — to buy nothing.

## What already guards the layout

Exact or budgeted `const _: () = assert!(size_of::<…>())` guards cover every type that multiplies by
row or column count: `Emit` (1 B), `TupleValue` (≤ 40 B), `Message` (≤ 88 B), `SinkMeta` (≤ 192 B),
`StreamedChange` (40 B), `PgColumn` (40 B), `PgRelation` (80 B), `RawCol` (48 B), `MirrorCol`
(≤ 88 B), `TablePlan` (48 B). A layout regression that would cost a cache line breaks compilation
rather than a benchmark.

## The re-open trigger

Re-open a layout change only when a named Criterion case in `docs/benchmarks.md` shows a loop that
(a) traverses at least thousands of elements per call, (b) reads a strict minority of each element's
bytes, and (c) accounts for more than half that benchmark's measured time — then compare the SoA
candidate against the committed AoS form on one machine, five runs, non-overlapping confidence
intervals or a median gain above 5 %.

Re-open `align(64)` padding only when an atomic is written on a per-row path from more than one
thread, which today would require the loader's one-task-per-file model or the sink's single decode
loop to change first.
