# `AsMut` has no write target in walrus (PR 20.3)

> **Status:** decided 2026-08-15 — **do not adopt `impl AsMut<[u8]>`; deny
> `clippy::needless_pass_by_ref_mut` instead.** Revisit if walrus gains a fixed-length byte sink,
> such as a checksum or nonce buffer or a `[u8; N]` frame-header builder.

## The measured surface

The production inventory has 15 mutable container parameters. Twelve depend on concrete
`String`, `Vec`, or `BytesMut` operations, while three already use the mutable-slice API advocated
by the rule.

| # | Site | Parameter | Why the concrete type is right |
|---:|---|---|---|
| 1 | `crates/common/src/sink_meta.rs:284` | `buf: &mut String` | Appends serialized fields with `String::push_str`. |
| 2 | `crates/loader/src/plan.rs:142` | `raw_cols: &mut Vec<RawCol>` | Builds the raw-column plan with `Vec::push`. |
| 3 | `crates/loader/src/plan.rs:143` | `mirror_cols: &mut Vec<MirrorCol>` | Builds the mirror-column plan with `Vec::push`. |
| 4 | `crates/pg-sink/src/reload_signal.rs:296` | `v: &mut Vec<T>` | Takes ownership of the current vector, partitions it, and replaces it with the retained elements. |
| 5 | `crates/pg-sink/src/replication.rs:563` | `buf: &mut BytesMut` | Checks length, indexes the frame header, and removes a frame with `BytesMut::split_to`. |
| 6 | `crates/pg-to-arrow/src/batch.rs:343` | `scratch: &mut String` | Passes one reusable parsing buffer to typed leaves that clear and grow it. |
| 7 | `crates/pg-to-arrow/src/batch.rs:441` | `builders: &mut [Box<dyn ArrayBuilder>]` | Already uses the mutable slice form; destructuring needs the exact sibling-builder window. |
| 8 | `crates/pg-to-arrow/src/batch.rs:478` | `builders: &mut [Box<dyn ArrayBuilder>]` | Already uses the mutable slice form for the two timetz sibling builders. |
| 9 | `crates/pg-to-arrow/src/batch.rs:512` | `builders: &mut [Box<dyn ArrayBuilder>]` | Already uses the mutable slice form for the five range sibling builders. |
| 10 | `crates/pg-to-arrow/src/batch.rs:515` | `scratch: &mut String` | Hands the same reusable parsing buffer to `append_value`. |
| 11 | `crates/pg-to-arrow/src/batch.rs:576` | `scratch: &mut String` | Threads the reusable buffer through multirange parsing to `append_struct_bound`. |
| 12 | `crates/pg-to-arrow/src/batch.rs:621` | `scratch: &mut String` | Hands the reusable buffer to typed parsing leaves for multirange bounds. |
| 13 | `crates/pg-to-arrow/src/batch.rs:959` | `scratch: &mut String` | Date normalization requires `String::clear` and `String::push_str`. |
| 14 | `crates/pg-to-arrow/src/batch.rs:968` | `scratch: &mut String` | Time normalization requires `String::clear`, `String::push_str`, and `String::push`. |
| 15 | `crates/pg-to-arrow/src/batch.rs:977` | `scratch: &mut String` | Timestamp normalization also searches and edits in place with `String::find` and `String::replace_range`. |

The local `let mut builders` binding at `crates/pg-to-arrow/src/batch.rs:170` is mutable state, not
a function parameter, so it is not a sixteenth inventory row.

## What `impl AsMut<[u8]>` would have served

The generic bound is useful for a fixed-length byte fill: code that overwrites an existing region
without changing its length can accept a vector, slice, or array through the same API. walrus has
no such write target. The wire construction paths in `crates/pg-sink/src/replication.rs` grow owned
`Vec<u8>` values with `push` and `extend_from_slice`; their changing lengths are part of building
the frame. The pgoutput `Reader` in `crates/pg-sink/src/pgoutput/reader.rs` is instead read-only over
`&'a [u8]` and advances only its cursor.

The sole broad conversion bound is the legitimate read-only path input
`crates/loader/src/duck.rs:71 TableDb::open(path: impl AsRef<Path>)`. There is no `AsMut` bound to
widen, and retrofitting the concrete mutable-container roles would add generic instantiations
without admitting a useful new caller.

## What landed instead

The workspace now denies `clippy::needless_pass_by_ref_mut`, the mutable-borrow gate this audit
actually found missing. It retired the one reported site at
`crates/pg-sink/src/reload_export.rs:304`: `ChunkExporter::await_echo` only uses shared operations,
so its receiver is now `&self`. A compile-time unit guard pins that receiver, and
`crates/pg-sink/tests/asmut_absence.rs` scans Rust sources under `crates/` to keep the evidence
current if a genuine mutable conversion target appears later.

This is a single nursery lint, outside `clippy::all`. Denying it is deterministic because
`rust-toolchain.toml` pins `channel = "1.95.0"` exactly; a new stable release cannot silently widen
the lint while walrus's source is unchanged.
