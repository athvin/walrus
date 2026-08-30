# Newtype safety: what is typed, and the transaction-id pair that is not

> **Status:** audited — the rule holds for every domain value the services hand around. One layout
> assertion was added to `pg_sink::memory::TableId`; the pgoutput transaction ids stay bare `u32`,
> and the single-`Xid` wrapper that looks like the obvious next step is rejected here, with its
> reason, because it would close none of the transpositions still possible.

## What the rule asks

Wrap primitives whose *meaning* differs in distinct types, so a swapped argument is a compile error
instead of a silent runtime one. Its four cases are ids that could be confused, units that must not
mix, validated strings, and two meanings of one representation — and it explicitly excludes the
`struct X(i32)` case where a single use makes confusion impossible.

## Census: every confusable value in the tree

Fields and types, not line numbers — a line drifts, a name does not.

| Domain value | How it is typed today | Verdict |
|---|---|---|
| Control-plane row keys and counters (`ManifestId`, `EpochNo`, `SchemaVersionNo`, `ReloadId`, `DdlId`, `crates/common/src/ids.rs`) | Transparent newtypes over `i64` with a hand-written SQLx `int8` delegation | Already the rule's answer |
| WAL byte address (`Lsn`, `crates/common/src/lsn.rs`) | Transparent newtype over `u64`, own hex grammar | Already the rule's answer |
| Commit and processing instants (`UtcTimestamp`, `crates/common/src/sink_meta.rs`) | Transparent newtype over `jiff::Timestamp` | Ordering key vs clock reading cannot be swapped |
| Relation OID inside the demux and meter (`TableId`, `crates/pg-sink/src/memory.rs`) | Transparent newtype over `u32` | Converted by [`type-newtype-ids`](./type-newtype-ids.md); layout now asserted (below) |
| Backpressure ratios (`Ratio`, `HysteresisBand`, `crates/pg-sink/src/memory.rs`) | Validated `f64` in the open unit interval, band built through a constructor | Rule's validated-value case, plus a cross-field check |
| Renewable lease TTL (`LeaseTtl`, `crates/loader/src/config.rs`) | `Duration` that has passed the renewal floor; `spawn_renewer` accepts nothing else | Rule's units case, done as parse-don't-validate |
| DuckDB table names (`DuckTable<Mirror>` / `DuckTable<Raw>`, `crates/loader/src/table_name.rs`) | One transparent newtype, two phantom kinds | Rule's "two meanings of one representation", exactly |
| SQL identifiers (`SqlIdent`, `crates/common/src/sql.rs`) | Validated string, `Display` emits the quoted form | Rule's validated-string case |
| Preflight table identity (`preflight::TableId { schema, table }`) | Named-field struct, not a `(String, String)` | The pair cannot transpose |
| Flush thresholds (`max_rows`, `max_bytes`, `max_inflight_bytes`) | `NonZeroU64` **named struct fields** of `BatchTriggers` / `SinkConfig` | No positional call site exists to swap |
| The two database URLs (`control_db_url`, `source_db_url`) | Bare `String`s | Each reaches exactly one differently-named connector (`control::connect`, `preflight::connect_source`); no function takes both |
| pgoutput wire scalars: relation OIDs, type OIDs, and transaction ids | Bare `u32` | Deferred — see below and [`type-newtype-ids`](./type-newtype-ids.md) |

Two structural facts keep the census this short. Every multi-value payload in the tree is a
named-field struct rather than a positional tuple or a long parameter list, and the workspace has no
function anywhere under `crates/*/src` taking two adjacent same-typed primitives that mean different
things — except the transaction-id pair discussed below.

## What changed

`TableId` was the only `#[repr(transparent)]` newtype in the workspace whose zero-cost claim lived
only in prose. Its doc comment states that the wrapper stays exactly one `u32` wide inside the
per-row `StreamedChange`; that was backed indirectly, by `stream_txn.rs`'s
`size_of::<StreamedChange>() == 40` budget, which would only notice a fattened `TableId` while
`StreamedChange` keeps its current shape. Every sibling — the five ids in `common`, `Lsn`,
`UtcTimestamp`, `DuckTable<K>` — pins its own layout directly, so `TableId` now does too:

```rust
const _: () =
    assert!(size_of::<TableId>() == size_of::<u32>() && align_of::<TableId>() == align_of::<u32>());
```

That is the rule's own zero-cost-abstraction check, at compile time rather than in a test, matching
what `pg-sink` already does for `Message`, `StreamedChange`, and `DecodeError`. No behaviour changed
and no test needed updating.

## What did not change: the transaction ids

`xid` / `sub_xid` / `top_xid` stay bare `u32` (396 textual occurrences across 38 files under
`crates/`), including the `SinkMeta::xid` field that is part of the `walrus_pg_sink_meta` wire
contract.

The interesting part is *why the obvious fix is the wrong one*. A single `Xid(u32)` newtype
threaded through the demux was drafted and rejected, because once `TableId` became a newtype it
closes nothing. Walk the sites where a bare xid sits beside another bare integer:

- `StreamDemux::on_change`'s destructuring already wraps the other half as `TableId(*relation_oid)`,
  so an OID and an xid cannot silently trade places — writing the swap requires deliberately
  wrapping the xid in `TableId`.
- `InflightMeter::add(key, bytes)` and `release(key)` pair a `u32` with a `u64`; the halves are not
  interchangeable.
- `StreamedChange` and `StagedSpill` are built as named-field literals.
- The two genuinely swappable pairs — `StreamDemux::claim_stream(key, top, bytes)`, where the key's
  sub-xid and the owning `top` are both xids, and `on_stream_abort(top_xid, sub_xid)` in both
  `stream_txn.rs` and `reload_signal.rs` — are pairs of *xids*. One `Xid` type makes both halves the
  same type and leaves the transposition compiling exactly as it does now.

So the single-`Xid` conversion is type decoration: about fifteen files (four production modules,
three sibling test modules, eight integration tests), a `.0` unwrap at each `RelationCache` and
`SinkMeta` boundary, and no transposition prevented. The rule's own "overkill" line applies.

Until a type closes it, the top/sub pair is held by tests, which is worth recording because they are
what a future refactor must keep passing: `owner_index_is_emptied_by_stream_commit` (fails if
`claim_stream`'s key and `top` trade places), `a_subtxn_abort_leaves_the_index_and_the_buffer_alone`
and `subtxn_abort_excludes_only_the_aborted_subxid` (fail if `on_stream_abort`'s arguments trade
places), and `subtransaction_aborted_signal_never_resolves_the_waiter` for the reload-signal side.

## The version that would be worth taking

Not `Xid`, but **two** types: `TopXid` and `SubXid`, with an explicit `impl From<TopXid> for SubXid`
naming the one real coercion in the protocol — a top-level row's sub-xid *is* the top xid, which is
what `xid.unwrap_or(top)` means today. That types the whole demux without ambiguity (`open:
HashMap<TopXid, _>`, `aborted: HashSet<SubXid>`, `owner: HashMap<(TableId, SubXid), TopXid>`,
`current_top: Option<TopXid>`, `PendingSignal::xid: Option<SubXid>`) and turns both live
transpositions into compile errors. It costs two conversions at protocol comparisons — the
whole-txn test `top_xid == sub_xid`, and the `unwrap_or` above — and the same fifteen files.

`SinkMeta::xid` should stay `u32` even then: it is a persisted cross-service key, and the demux
already unwraps at that boundary. (`#[serde(transparent)]` would preserve the JSON if it ever moves;
the amortized `MetaRow` carries `xid` by value, so that side is two lines, not a redesign.)

Take it when the friction is already being paid: a third xid-shaped parameter on a demux entry
point, a two-phase-commit (`Stream Prepare`) path that adds a prepared-txn id to the family, or a
real mix-up found in review or production. This refines the reversal trigger in
[`type-newtype-ids`](./type-newtype-ids.md), which named "two different xids as adjacent positional
`u32` parameters" without noting that `on_stream_abort` is already that shape and that one newtype
cannot fix it — the trigger is worth acting on, but only in the two-type form.

The relation-OID and type-OID half of that same deferral is unchanged and still whole-workspace:
`PgRelation::oid` is persisted inside `schema_registry.columns`, and the `common::oids` constants
are `match` patterns across `pg-to-arrow` and `loader`. `TableId` is the pg-sink-local slice of it
that was worth taking alone.

## When to revisit

A new domain value should arrive already wrapped — the tree has a shape for each flavour: transparent
integer id (`ids.rs`), validated scalar (`Ratio`, `LeaseTtl`), validated string (`SqlIdent`), phantom
kind (`DuckTable<K>`). New transparent newtypes get the layout assertion above in the same commit.
Formatting for these types is a separate question answered in
[`type-numeric-fmt.md`](./type-numeric-fmt.md); their use as hash keys, in
[`perf-ahash.md`](./perf-ahash.md).
