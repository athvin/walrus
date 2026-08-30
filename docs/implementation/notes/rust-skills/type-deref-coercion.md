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

## RAII guard is a reason, not a licence

`crates/pg-sink/src/reload_signal.rs`'s `SubscribeGuard` was the one allow-list entry, on the
grounds that its `Drop` unregisters the in-flight watermark subscription. Being a guard is what made
a `Deref` *arguable*; it is not what made it right. `MutexGuard<T>` derefs because handing out `&T`
IS what the guard is for, whereas `SubscribeGuard` exists to be awaited — it already implements
`Future`, and the only thing the `Deref`/`DerefMut` pair ever reached was `try_recv` in the module's
unit tests. Everything else it published — `close`, `blocking_recv` — was inherited API nobody asked
for: closing the channel behind the registry's back, or blocking a runtime thread.

It now exposes an inherent `try_recv` instead, so the guard's surface is exactly `.await`,
`try_recv`, and `Drop`. `ALLOWED` in `no_deref_inheritance.rs` is consequently empty; its matching
logic stays covered by a fixture list, so the mechanism still works the day an entry is earned.

## How to add an allow-list entry

Add the repo-relative path, exact wrapper name, and one-line reason to `ALLOWED` in
`crates/common/tests/no_deref_inheritance.rs`, and a paragraph here establishing that handing out
the inner type's *whole* API is the access the wrapper means to grant — a smart pointer, an
owned→borrowed container, or a guard in the `MutexGuard` sense. "It is a guard" and "the tests are
shorter that way" are not that argument; an inherent method covers the second. An entry for a domain
newtype is not permitted; such a type must keep an explicit accessor.
