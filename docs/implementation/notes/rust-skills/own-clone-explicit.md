# Explicit `Clone`: the cost boundary, and who draws it

> **Status:** audited — every call site was already the rule's answer (`own-borrow-over-clone`
> re-measured those 143 sites the same week). One change: the streamed-transaction row buffer no
> longer derives a `Clone` nobody called. No manifest change, no behaviour change.

## What the rule asks

1. A type that owns heap data implements `Clone` and **not** `Copy`, so every duplication is a
   visible `.clone()` rather than an implicit memcpy.
2. Hand-write `Clone` when the fields differ in cost, or when one of them must *not* be duplicated
   (the rule's example is a cache that should rebuild instead).
3. `clone_from` when a clone lands in an existing binding, so the old allocation is reused.
4. Prefer not cloning at all: a reference, a `Cow`, an `Arc`, or simply taking the value by move.

## The `Copy` side: the compiler draws it, the lint table pins it

`Copy` requires every field to be `Copy`, so (1) is not a convention in this tree — it is a type
error. Every `#[derive(…, Copy, …)]` under `crates/*/src` is one of: a fieldless enum (`Op`,
`Kind`, `SlotAction`, `DrainOutcome`, `ReloadFlavor`, the `string_enum!` families), a zero-sized
marker (`Mirror`/`Raw`, `Idle`/`Streaming`, `Exported`/`NotExported`, `SystemClock`), a numeric
newtype (`Lsn`, `ManifestId`, `ReloadId`, `DdlId`, `TableId`, `Ratio`, `UtcTimestamp`), or a small
plain record (`HeartbeatConfig`, `BatchTriggers`, `StandbyStatus`, `SlotInfo`, `Checkpoints`,
`Echo`, `PendingSignal`, `HysteresisBand`, `Pt`).

The opposite mistake — a cheap type wearing `Clone` and implying a cost it does not have — is what
`missing_copy_implementations = "deny"` and `trivially_copy_pass_by_ref = "deny"` catch, with
`large_types_passed_by_value` / `large_stack_*` and `clippy.toml`'s
`enum-variant-size-threshold = 64` fencing the size axis from the other end.

One deliberate exception: `Reader<'a>` (`crates/pg-sink/src/pgoutput/reader.rs`) is a slice plus an
offset — Copy-eligible, and it stays `Clone`-only. std draws the same line for the same reason
(`slice::Iter` is `Clone`, not `Copy`): a cursor that duplicates implicitly loses its position at
the first accidental copy. Nothing would have flagged it either way, because
`missing_copy_implementations` skips lifetime-generic types.

## The `Clone` side: 143 sites, zero hand-written impls

* 143 `.clone()` lines under `crates/*/src` (sibling `*_test.rs` included). The buckets — owned
  storage built from borrowed input, a `'static` move into a spawned task, a handle bump
  (`Arc::clone`, `PgPool`, `CancellationToken`) — are enumerated in the `own-borrow-over-clone`
  audit and were re-checked here; no call site changed.
* **Zero `impl Clone` in production.** The tree's only hand-written one is `Bare` in
  `crates/common/src/string_enum_test.rs`, and it is load-bearing: that `string_enum!` invocation
  exercises the macro's *no-attribute* arm, so the generated enum carries no derives at all, while
  the generated `as_str(self)` takes `self` by value. Hence the explicit pair
  `impl Copy for Bare {}` and `impl Clone for Bare { … *self }`.
  * Consequence for the lint table: `clippy::expl_impl_clone_on_copy` (pedantic, unpinned) **cannot
    be adopted**. Its single site in the tree is that pair, and the fix it demands — write a derive
    instead — is precisely what the test must not have. Its correctness-shaped sibling
    `non_canonical_clone_impl` is already denied through the `suspicious` group.
* **No type has the rule's manual-`Clone` shape.** Nothing that derives `Clone` carries a field a
  clone should drop or rebuild: the two loader `RefCell`s live in `!Sync` types that derive nothing
  (see [`own-mutex-interior.md`](./own-mutex-interior.md)), `ParquetSink`'s `Arc<dyn ObjectStore>`
  is a shared handle whose clone is meant to share, and everything else is a plain record, a config
  struct, or a decoded row. A derive says the whole truth in each case.
* The decode hot path holds **no** clone at all: `crates/pg-sink/src/pgoutput/` contains zero
  `.clone()` calls — it borrows the wire buffer through `Reader` and moves what it builds.

## `clone_from`

Zero calls, and zero sites that want one. `clippy::assigning_clones = "deny"` (Cargo.toml) is what
makes `x = y.clone()` a build failure, and no production statement re-assigns a cloned value: the
only assignments that clone wrap the result in `Some(..)` (`InternalTables::note_relation`, twice,
once per control relation for the lifetime of a stream). The reuse-an-allocation half of this
question belongs to `mem-clone-from`.

## What changed

`StreamedChange` (`crates/pg-sink/src/stream_txn.rs`) derived `Clone`, and **nothing in the tree
ever called it** — rows are pushed by value, drained by `take_stream`'s `extract_if`, and read
through `iter_survivors`; even the values go to the batcher as `&c.values`. It is the one type
where an unused derive is more than clutter: it is the per-row element of an open transaction's
in-memory buffer, one `Box<[TupleValue]>` each, and `InflightMeter` charges those bytes exactly
once against the ceiling that decides when to spill. A clone would have been a second copy of that
buffer, invisible to the meter, compiling silently. The derive is gone, the doc comment says why,
and `crates/pg-sink/tests/move_capture_discipline.rs` — which already guards the clone-shaped half
of `closure-move-capture` — pins it with `the_buffered_streamed_row_stays_move_only`.

## What did not change, and why

**`pgoutput::Message` keeps its `Clone`.** Nothing clones a decoded message today either, but it is
a `pub`, `#[non_exhaustive]` value type at the decoder's boundary, and a consumer that wants to
retain one is making an ordinary, visible request. The buffering path already pays the *narrow*
version of that cost deliberately: `StreamDemux::on_change` clones the tuple `Vec` alone — not the
message — because `msg: &Message` and the row must be frozen into a `Box<[_]>`.

**The loader's plan records keep theirs.** `TablePlan`, `RawCol` and `MirrorCol` also derive a
`Clone` that no caller uses today, but they are `pub`, built once per table at plan time rather than
once per row, and nothing meters them. The argument that made `StreamedChange` move-only does not
reach them, and dropping a public derive on weaker evidence is tidying, not this rule.

**`SinkMeta`'s per-row `String` clones stay.** Turning `source_schema` / `source_table` /
`sink_instance` into `Arc<str>` needs serde's `rc` feature, which is a dependency decision rather
than a clone decision.

**The `Cow` half of "when to avoid clone" is already applied**: `common::sql::sql_literal`
(PR 9.5), `pg-to-arrow`'s range bounds and `arrow_emit_name` all return `Cow<'_, str>` so the
common path borrows.

## When to revisit

A new heap-owning type earns `#[derive(Clone)]` when a call site needs one — not by reflex. For a
type that exists once per decoded row or per WAL message, the answer is move-only until a caller
proves otherwise, and the meter is the argument: anything the `InflightMeter` or a batch's byte
accounting counts must not be duplicable behind their backs. A hand-written `Clone` is still the
right tool the day a type acquires a field whose duplicate would be wrong (a cache, a handle to a
per-owner resource); today none has.
