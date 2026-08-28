# Hasher choice: the std default stays (rule `perf-ahash`)

> **Status:** audited 2026-08-28 — **no change.** walrus has 15 production maps/sets. Every one is
> either bounded by the source catalog (tens of entries) or touched once per file/chunk/startup, and
> the per-row ones sit inside a pipeline whose committed profile puts the limiter in DuckDB, not in
> Rust. No benchmark in `docs/benchmarks.md` isolates a map operation, so the rule's own first key
> point — *"switch hashers only after confirming map operations appear in profiler output"* — is not
> satisfied. `ahash`, `rustc-hash` and `gxhash` stay out of the manifests, guarded by
> `scripts/no-speculative-deps.sh`.

## The inventory

Every `HashMap`/`HashSet` in production code (`crates/**/src`, excluding `*_test.rs`), with what
supplies its key and how often it is probed:

| site | key | key origin | probes per… | cardinality bound |
|---|---|---|---|---|
| `pg-sink/src/consume.rs:699` `BatchRouter::batchers` | `u32` relation OID | source WAL / catalog | 1 `entry` per row (`:830`) | published tables |
| `pg-sink/src/stream_txn.rs:134` `StreamDemux::open` | `u32` top-level xid | source WAL | 1 `get_mut` per streamed row (`:261`) | concurrently streaming txns |
| `pg-sink/src/stream_txn.rs:139` `StreamDemux::owner` | `(TableId, u32)` | source WAL | 1 `insert` per streamed row (`:194`) | open `(table, sub-xid)` streams |
| `pg-sink/src/stream_txn.rs:72` `StreamedTxn::keys` | `(TableId, u32)` | source WAL | 1 `insert` per streamed row (`:91`) | streams in one txn |
| `pg-sink/src/stream_txn.rs:76` `StreamedTxn::aborted` | `u32` sub-xid | source WAL | 1 probe per survivor at commit (`:126`) | rolled-back savepoints |
| `pg-sink/src/stream_txn.rs:491` local `batchers` | `TableId` | source WAL | 1 `entry` per survivor | tables in one streamed txn |
| `pg-sink/src/memory.rs:36` `InflightMeter::by_stream` | `(TableId, u32)` | source WAL | 1 `entry` per streamed row (`:55`) | bounded by `max_inflight_bytes` |
| `pg-sink/src/ddl.rs:86` `DdlConsumer::versions` | `(String, String)` schema+table | **source catalog** | 1 per DDL event (`:101`, `:122`) | replicated tables |
| `pg-sink/src/reload_signal.rs:50` `WatermarkWaiters::waiters` | `(ReloadId, i64)` | control-pg | 1 insert + 1 remove per reload chunk | in-flight chunks |
| `pg-sink/src/preflight.rs:468` `published_tables` | `TableId { schema, table }` | **source catalog** | once per startup | published tables |
| `pg-to-arrow/src/uuid_enum.rs:27` field metadata | `String` | walrus constant | once per uuid column | 1 |
| `loader/src/duck.rs:65` `TableDb::parquet_cols` | `SchemaVersionNo` | walrus | 1 per claimed file | schema versions |
| `loader/src/plan.rs:113` local `by_name` | `&str` column name | source catalog | once per plan build | columns |
| `loader/src/ddl.rs:142` local `kept` | `&str` column name | source catalog | once per DROP COLUMN diff | columns |
| `loader/src/phase_a.rs:62` `TableCtx::resync_ids` | `ReloadId` | control-pg | 1 per reload file | live reloads |

`pg-to-arrow/src/uuid_enum.rs:27` is not a walrus choice at all: arrow-rs's `Field::with_metadata`
takes `HashMap<String, String>` by signature, so its hasher is fixed by the dependency.

Two structures are deliberately *not* hash-based and stay that way: `RelationCache`
(`pg-sink/src/relcache.rs:42`) is a `BTreeMap` keyed `(oid, schema_version)` **because**
`latest_for` needs all versions of one oid to be a contiguous range (`:54-59`), and
`reload_export.rs`'s `BTreeSet` is ordered for the same reason. `latest_for` is the other per-row
index probe on the sink path, and this rule reaches neither: ordering is load-bearing, so no hasher
applies.

## Why no site clears the profile-first bar

The densest per-row map work in the tree is the streamed path, which hashes four small keys per row
(`open`, `keys`, `owner`, `by_stream`); the ordinary path hashes one (`batchers`). Set that against
the committed measurements:

- `docs/benchmarks.md:124` — decode is 248 ns/row (narrow) to 1 891 ns/row (wide30), dominated by
  per-cell `String` allocation.
- `docs/benchmarks.md:386` — `append_row` is 460 ns/row after the PR 5.7 meta amortization; the
  batch-constant JSON it removed was ~91 % of the pre-5.7 narrow figure.
- `docs/benchmarks.md:411-415` — end to end the sink sustains 6 081 rows/s (~164 µs/row of wall
  clock) with `inflight` at 0 while the loader accumulates lag. The sink is not the limiter, and the
  loader's own maps are probed once per *file* against a ~100 ms `append_parquet`.

The same `BatchRouter::push` that performs the one hot-path lookup also clones two schema strings,
builds a thirteen-field `SinkMeta` and reads the clock twice (`consume.rs:841-856`). Even granting
the rule's optimistic 2–4× hasher multiplier, the entire hashing cost is a rounding error against a
164 µs/row system budget — and, unlike the JSON and allocation work already measured, nothing has
observed it. Changing it now would be optimizing on a guess, which `docs/benchmarks.md:59` makes the
house rule against.

## Why `FxHash` specifically is the wrong pick here

The rule is explicit that a predictable hasher must never see externally-supplied keys. Two walrus
maps are keyed on **strings chosen by whoever has DDL rights on the replicated database**:
`DdlConsumer::versions` on `(source_schema, source_table)` and `preflight::published_tables` on
`TableId { schema, table }`. The source database is operator-configured, but its table names are
tenant data, not walrus constants — so a public unseeded hash function there is a collision surface
the default does not have. That rules out a blanket `type HashMap<K, V> = FxHashMap<K, V>` alias,
which is the shape this rule most often takes, and leaves only a per-site swap on maps that no
profile has implicated.

`ahash` would be safe on that axis (randomized per-process seed), but it buys the same unmeasured
nanoseconds while adding a direct dependency to a tree whose supply chain is gated four ways
(`deny.toml`). `gxhash` is doubly barred: it compiles only with AES/SIMD target features enabled, and
`crates/common/tests/build_profile.rs` fails the build when *any* build surface sets `target-cpu`,
`target-feature` or rustflags — the PR 16.12 ISA-floor decision (`opt-target-cpu.md`,
`opt-simd-portable.md`) that predates this rule.

## The dependency-free half of the rule, already taken

The cheapest hash is the one not computed, and walrus already minimizes probe *count* rather than
probe cost:

- `consume.rs:830`, `stream_txn.rs:512`, `memory.rs:55` route lookup-then-insert through `entry`.
- `reload_signal.rs:79-85` uses one `Entry::Occupied` for the generation check *and* the eviction,
  with the reason recorded in place — "a `get`-then-`remove` pair would hash twice while holding the
  registry lock".
- `stream_txn.rs:126`'s `aborted` probe is skipped entirely in the no-savepoint case: today's std
  returns from `contains` before hashing when the table is empty (an implementation detail of the
  vendored hashbrown, not a contract, but it is the common case here).

`with_capacity` (the rule's fourth key point) has no unserved site: the two `collect` sites
(`loader/src/plan.rs:113`, `loader/src/ddl.rs:142`) already reserve from an exact `size_hint` through
`FromIterator`, and every long-lived map's final size is a property of the source workload, not a
number the constructor knows. The one lookup that *does* waste work is
`DdlConsumer::version_of` (`ddl.rs:101-104`), which allocates two `String`s per probe to build an
owned tuple key — but that is a key-type question on a per-DDL-event path, not a hasher question, and
it is left alone here.

## What is guarded now

`scripts/no-speculative-deps.sh` gained `ahash`, `rustc-hash` and `gxhash`, each pointing at this
note. The guard is manifest-scoped on purpose: `ahash` is already in `Cargo.lock` transitively (both
0.7 and 0.8, through `arrow-array`/`arrow-select` and the `hashbrown` versions vendored by
`metrics-util` and `rkyv`), so a lock-file check would fail on day one and prove nothing. Any
other repackaging of the same function — the older `fxhash` crate, a hand-written `BuildHasher` —
falls under the same decision even though it is not spelled in the list.

## Reversal condition

Re-open when a sampled profile of `bench-e2e` or a Criterion case in `docs/benchmarks.md` attributes
a measurable share of sink CPU to `SipHash`/`RandomState` frames. The proposal must then name the
individual maps it converts (never a workspace-wide alias), keep `DdlConsumer::versions` and
`preflight::published_tables` on a DoS-resistant hasher because their keys are source-supplied
strings, and show an A/B on the affected bench with non-overlapping confidence intervals — the same
bar `mem-smallvec.md` and `mem-compact-string.md` were held to.
