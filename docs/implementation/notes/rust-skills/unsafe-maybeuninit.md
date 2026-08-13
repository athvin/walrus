# Uninitialized memory is encapsulated, not adopted (PR 12.6)

> **Status:** evaluated — **D**, `MaybeUninit` is not adopted. The one path that benefits from
> uninitialized spare capacity already reaches it through a safe API, and the bad forms are already
> denied by default.

## What the rule asks for

The `unsafe-maybeuninit` rule replaces `mem::uninitialized` and inappropriate `mem::zeroed` calls
with `MaybeUninit`, then permits construction of a `T` only after every byte has been written. That
is the right validity-invariant discipline for code that must own raw initialization.

The rule's good examples still finish with unsafe `assume_init` and `set_len` operations. Walrus
does not need those operations, and PR 12.1's workspace-wide `unsafe_code = "forbid"` already makes
such an implementation a compile error. This evaluation therefore records a **D** verdict instead
of importing the rule's unsafe implementation technique.

## What walrus actually does

`ReplicationStream::connect` in `crates/pg-sink/src/replication.rs` reserves 16 KiB with
`BytesMut::with_capacity`. Its `read_message` method passes that buffer to
`AsyncReadExt::read_buf`; Tokio writes through the `BufMut` interface into `BytesMut`'s
`UninitSlice` spare capacity and advances the initialized length behind a safe API. The retained
bytes in `rbuf` also survive cancellation of `read_buf`, so the feedback timer can interrupt a wait
without discarding a partial backend message.

The other capacity reservations are ordinary length-tracked collections. For example,
`BatchBuilder::new` reserves its builder vector and then pushes each builder,
`FixedSizeBinaryBuilder::with_capacity(0, width)` begins with zero logical values, and Phase A
reserves `claimed.len()` IDs before pushing them. None exposes an initialized `T` before a write.

## Measured

The audited first-party scope is Rust under `crates/*/src` and `tests/*/src`:

- 0 matches for `MaybeUninit|mem::uninitialized|mem::zeroed|assume_init|\.set_len\(`;
- 0 zero-filled `vec![0; …]` scratch buffers;
- 22 `with_capacity` calls, all used as length-tracked reservations.

The replication buffer is the only site using spare capacity for I/O, and its initialization is
encapsulated by `BytesMut` and `read_buf`. Arrow builders encapsulate their own storage; the other
vectors and strings reserve capacity and then grow through safe APIs.

## Lints that already cover the bad forms

`clippy::uninit_vec` and `clippy::uninit_assumed_init` are already **deny-by-default** in the pinned
Clippy. This task does not add or promote either lint: there is no lint-level delta to claim. The
workspace unsafe-code forbid independently rejects the unsafe completion operations shown by the
rule.

## The tripwire

`scripts/check-unsafe-invariants.sh` scans only the audited first-party source roots and prints any
offending `file:line`. Its isolated `--self-test` proves a clean `with_capacity` fixture passes and
a `.set_len(` fixture fails. CI runs the same guard at the front of the `gates` job, before Rust
formatting, Clippy, and tests.

Revisit this **D** decision only if walrus gains a first-party FFI buffer that must be initialized by
an external writer, or profiling against the bar in `docs/benchmarks.md` shows that safe zero-fill
is a material hot-path cost. Either case requires a new safety audit rather than weakening this
guard casually. PR 12.7 extends the same script with workspace-forbid tripwires; those checks are
deliberately absent here.
