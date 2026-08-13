# Compact strings in `SinkMeta` (PR 11.16)

> **Status:** deferred. The fresh append-row medians do not overturn the system bottleneck ranking.

## Evidence

PR 11.3 changed the repeated `batch_id` assignment in `TableBatcher::on_commit` from `clone` to
`clone_from`, so each pending row can reuse its existing `String` allocation. On that predecessor,
the unchanged `cargo bench -p pg-to-arrow --bench batch` suite measured these Criterion medians for
1,000 appended rows (Apple M2, macOS 26.5.2, rustc 1.95.0, default benchmark settings):

| shape | median | ns/row |
|---|---:|---:|
| `narrow_int4` | 757.89 µs | 757.89 |
| `wide30` | 1.4492 ms | 1,449.2 |
| `text_heavy` | 1.1449 ms | 1,144.9 |
| `tier2_fanout` | 1.3677 ms | 1,367.7 |

This run measures the current implementation only; it is not an A/B test of an alternative string
type. The committed PR 5.6 end-to-end profile remains the ranking evidence: under sustained mixed
and wide-text load, sink inflight and spills stayed at zero while loader lag accumulated. A local
sink micro-benchmark therefore cannot justify changing a cross-service type by itself.

## Tradeoffs retained

`SinkMeta` has four owned string fields: `batch_id`, `source_schema`, `source_table`, and
`sink_instance`. The benchmark's 36-byte batch id does not fit the usual 23-byte compact-string
inline capacity, production identifiers have no useful audited length distribution or upper bound,
and PR 11.3 already removed the known repeated assignment allocation. A 24-byte compact string
would not shrink the `SinkMeta` handle relative to `String`; it would add a dependency and
representation branches in exchange for unmeasured inline-allocation savings.

`Arc<str>` would reduce each field handle from three words to two and make clones shared, but serde's
implementations for `Arc` require enabling its `rc` feature. Workspace feature unification would
broaden that capability for every serde consumer, and every constructor plus the amortized
`MetaConst` serialization path would need conversion and byte-equivalence review. Although the
intended JSON values are still strings, `SinkMeta` is the sink/loader wire contract, so that review
cost cannot be dismissed as an internal layout swap.

No `compact_str`, `smartstring`, `ecow`, or serde `rc` feature is added by this task.

## Reversal condition

Revisit only when an end-to-end workload shows the sink saturating before the loader and an
allocation profile attributes a material share of sink CPU or resident memory to these `SinkMeta`
strings. A proposal must then include representative string-length data, an A/B append-row result,
unchanged JSON-wire tests, and the dependency/feature review for the selected representation.
