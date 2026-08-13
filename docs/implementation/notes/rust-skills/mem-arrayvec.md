# ArrayVec has no hard-capacity home in walrus (PR 11.14)

> **Status:** decided 2026-08-13 — declined, structurally. This decision is structural, not bench-gated:
> there is no legal compile-time bound to encode, so benchmarking an `ArrayVec<T, N>` candidate is
> not reachable.

## What the rule asks

`ArrayVec<T, N>` is correct only when the domain guarantees a hard `N`. Exceeding it cannot spill to
the heap; it rejects or panics. A typical observed length or a server configuration limit is not a
protocol guarantee.

## Why walrus has no `N`

`parse_tuple` reads its column count from pgoutput `Int16` at
`crates/pg-sink/src/pgoutput/mod.rs:223`. `Reader::int16` returns `u16`
(`crates/pg-sink/src/pgoutput/reader.rs:66`), so the wire can express 65,535 columns. PostgreSQL's
1,600-column table ceiling is server policy, not a decoder protocol constant; the guard tests also
prove that a 2,000-column wire tuple decodes successfully.

| candidate | length driven by | compile-time bound? |
|---|---|---|
| `Vec<TupleValue>` | pgoutput's unsigned 16-bit TupleData count | No; 1,600 is server policy and smaller values reject valid frames |
| `PgRelation.columns` (`crates/common/src/pg_shape.rs:89`) | user schema | No |
| per-commit `Vec<SealedBatch>` (`crates/pg-sink/src/consume.rs:321`) | transaction rows and configured batching | No |

## The arithmetic

PR 11.2's layout ratchet caps `TupleValue` at 40 B
(`crates/common/src/pg_shape.rs:134-135`). An inline capacity matching PostgreSQL's current ceiling
would reserve **1,600 × 40 B = 64,000 B** of stack for every tuple. Matching the wire's full `u16`
range would be dramatically larger. Neither is an acceptable fixed-capacity representation.

## What walrus does instead

`crates/pg-sink/src/pgoutput/mod.rs:224` uses `Vec::with_capacity(ncols as usize)`: one heap
allocation right-sized from the wire count, with no inline stack slab, spill branch, or artificial
runtime rejection. `mod_test.rs` checks that the returned capacity covers all 1,600 requested
columns and that the cursor consumes the complete frame.

## The one genuine hard cap

`Lsn` display always writes exactly 16 uppercase hex digits
(`crates/common/src/lsn.rs:89-92`). `ArrayString<16>` is therefore structurally possible, but it is
explicitly deferred to PR 11.16's small-string measurement; this task adds no `arrayvec` dependency.

## Revisit triggers

Reconsider a fixed-capacity container only when a protocol or physical invariant supplies a small,
compile-time maximum and exceeding it is impossible by construction. A benchmark alone cannot
create that invariant. For LSN rendering, revisit only if PR 11.16 shows string layout or allocation
to be material.
