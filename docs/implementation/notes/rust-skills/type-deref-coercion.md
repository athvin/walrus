# Explicit accessors instead of `Deref` inheritance

> **Status:** decided — domain newtypes expose intentional accessors; only genuine pointer,
> owned-container, or RAII semantics justify a reasoned `Deref` exception.

## What the convention is

Walrus domain newtypes expose only their intended conversion surface. The precedents are
`Lsn::as_u64` in `crates/common/src/lsn.rs:31`, `From<ManifestId> for i64` in
`crates/common/src/ids.rs:26`, and `DuckTable::as_str` from PR 18.11. Those APIs make conversion
visible at the call site. They do not inherit methods from `u64`, `i64`, or `str`.

The integration guard scans every Rust source file below `crates/*/src`, including nested modules,
and rejects unlisted `Deref` and `DerefMut` implementations. It deliberately does not scan
`tests/e2e/src`: that crate is a test harness, not production code.

## Why `Deref` would break it

`UtcTimestamp(jiff::Timestamp)` is the concrete invariant example. Its parse and render boundary in
`crates/common/src/sink_meta.rs:41-60` guarantees walrus's UTC RFC-3339 `Z` representation. A
`Deref<Target = jiff::Timestamp>` would surface every inner timestamp method through ordinary
method resolution, including conversions that can produce a non-UTC presentation, bypassing the
wrapper API that preserves the invariant. The same inheritance would make arithmetic-shaped IDs
behave like their inner integers and erode the distinction between an epoch, schema version, LSN,
reload, and manifest.

## Layout-transparent is not API-transparent

PR 18.2 made the SQL-facing newtypes `#[repr(transparent)]`. That promises byte layout and alignment
for encoding boundaries; it does not promise that the wrapper has the inner type's public API.
`EpochNo` and `SchemaVersionNo` exist precisely because two layout-identical `i64` values must not
be interchangeable in Rust. Explicit accessors preserve that distinction.

## How to add an allow-list entry

The current exception is `crates/pg-sink/src/reload_signal.rs`'s `SubscribeGuard`. It temporarily
exposes its oneshot receiver while `Drop` unregisters the matching in-flight watermark
subscription, so it is a genuine RAII guard rather than a domain newtype. Both its `Deref` and
`DerefMut` implementations are covered by the one exact path-and-wrapper entry.

If another legitimate implementation appears, add its repo-relative path, exact wrapper name, and
one-line reason to `ALLOWED` in `crates/common/tests/no_deref_inheritance.rs`. Also add a sentence
here establishing why the wrapper is a genuine smart pointer, owned→borrowed container, or RAII
guard. An entry for a domain newtype is not permitted; such a type must keep an explicit accessor.
