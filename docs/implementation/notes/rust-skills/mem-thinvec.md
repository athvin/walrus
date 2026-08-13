# ThinVec vs walrus's niche-optimised Option<Vec> (PR 11.15)

> **Status:** decided 2026-08-13 — **declined: 24 B, identical to `Vec<T>`.** No Criterion
> benchmark was run because this is a compile-time layout question, not a throughput question.

## What the rule asks

`ThinVec<T>` stores its length and capacity with the heap allocation, reducing the handle from the
three words of `Vec<T>` to one. The source rule motivates it partly by claiming `Option<Vec<T>>`
costs an extra word, but its own layout example correctly shows `Option<Vec<u8>>` at 24 B—the same
size as `Vec<u8>`.

## What walrus measures

On the supported 64-bit target:

| type | size | evidence |
|---|---:|---|
| `Vec<TupleValue>` | 24 B | Rust standard layout on this target |
| `Option<Vec<TupleValue>>` (`crates/pg-sink/src/pgoutput/mod.rs:83`) | 24 B | PR 11.15 const relationship assertion |
| `PgRelation` | 80 B | `crates/common/src/pg_shape.rs` layout ratchet |
| `pgoutput::Message` | 88 B | `MESSAGE_MAX_BYTES` ratchet in `pgoutput/mod.rs` |

`Vec`'s non-null data pointer supplies the invalid bit pattern that `Option` uses for `None`, so the
optional `old` image in `Message::Update` already gets the niche at no extra handle cost.

## The enum arithmetic

Replacing the 24-byte optional vector with an 8-byte `ThinVec` handle could shrink `Update.old` by
16 B. It would shrink `Message` by **0 B**, because `Update` is not the size-defining variant:
`Message::Relation` carries the 80-byte `PgRelation` inline, leaving `Message` at 88 B including its
discriminant and padding. PR 11.10 explicitly deferred restructuring this cross-crate `Message`
layout; this evidence task does not reopen it.

## Supply-chain cost

`thin_vec`/`thinvec` has zero hits in `Cargo.lock`, unlike already-transitive `smallvec`, `arrayvec`,
`tinyvec`, and `bumpalo`. Adding it would create a new source and advisory surface and would need to
clear all four `cargo deny` families: advisories, licenses, bans, and sources. The eight-license SPDX
allow-list in `deny.toml` is intentionally minimal. That review cost is unjustified for a zero-byte
enum win.

## Semantics retained

`None` means an update carried no old image; `Some(vec![])` is a present empty image. Collapsing the
field to a plain vector with an empty sentinel would erase that protocol distinction and is not an
acceptable substitute.

## Revisit triggers

Revisit only if a future change makes `Update` the largest `Message` variant, or if profiling finds
many resident instances of another often-empty vector where a 16-byte handle reduction changes an
enclosing layout. Any proposal must remeasure the complete enclosing type and justify the new
dependency across all four supply-chain gates.
