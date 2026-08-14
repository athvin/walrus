# No diverging functions in walrus (PR 18.13)

> **Status:** decided — the guard is adopted, the technique is rejected.

## What the rule asks for

The `type-never-diverge` rule recommends a `!` return for functions that never return. Its positive
examples include `abort_with_error`, which calls `std::process::exit(1)`, and `panic_handler`. It
also notes that a never return is stable while using `!` as an arbitrary type argument is still
nightly-only; `std::convert::Infallible` is the stable stand-in for that latter use.

Those semantics are useful, but the exit technique conflicts with walrus's shutdown contract.

## What is actually true here

The evidence was measured on baseline commit `ac61e45`:

- `grep -rn --include='*.rs' -E '\->\s*!|process::exit' crates/ tests/ | wc -l` returned **0**.
- `crates/loader/src/main.rs:24` and `crates/pg-sink/src/main.rs:17` both declare `fn main() ->
  ExitCode`. Errors reach those boundaries as values instead of terminating the process below
  `main`.
- `common::ExitCode` is the documented `#[repr(i32)]` public contract at
  `crates/common/src/error.rs:124-141`; the conversion to the standard-library exit code starts at
  line 142.
- There are **41** textual `CancellationToken` matches in production source and **51** when sibling
  test modules are included. The actual signal-swallowing divergence at
  `crates/loader/src/shutdown.rs:78` is inside a spawned async block, not a function. Naming its
  task as `JoinHandle<!>` would also require the nightly-only arbitrary-type use.
- `crates/loader/src/apply_loop.rs:53` returns through `drain(&ctx)` when cancellation arrives. It
  does not diverge.
- The three production `TryFrom` implementations are genuinely fallible:
  `loader::health::LoaderPhase` and `pg_sink::health::Phase` reject unknown atomic bytes, while
  `pg_sink::memory::Ratio` rejects non-finite and out-of-range floats. None has an `Infallible`
  error or should be converted to one.
- Production contains no `panic!` or `todo!`. The one `unimplemented!` at
  `crates/pg-sink/src/backfill.rs:46` is an explicitly allowed, dead-code deferred-goal stub.

## The decision

Adopt the repository guard and reject the rule's `abort_with_error` technique. The guard scans all
Rust files below `crates/*/src`, reports each violation as `path:line: label`, proves its matcher
against planted in-memory examples, and starts with an empty allow-list.

Direct process termination does not run destructors. That would break the ordered graceful drain
documented in `crates/loader/src/shutdown.rs`, `docs/architecture.md` under “Graceful shutdown
(SIGTERM drain),” and `docs/walrus-loader.md` section 8.5. Walrus must finish its in-flight append
and transform, commit both watermarks, release the ownership lease, then checkpoint and close the
DuckDB file. Skipping those steps risks double application, a lease held until TTL expiry, and a
stale DuckDB lock that blocks the replacement pod.

Errors therefore remain values all the way to `main`, where the stable `ExitCode` contract maps
them to process status after cleanup has unwound.

## What would change this decision

A new exception would require evidence that the path owns no transactional, lease, file-lock, or
flush cleanup and that non-unwinding termination is part of an intentional process-boundary design.
That decision must update this note and add the exact source path to the guard's allow-list; it must
not arrive as a drive-by exemption.

An `Infallible` conversion would be reconsidered only if walrus gains a conversion that cannot
reject any input by construction. The three current `TryFrom` implementations do reject invalid
input and are not such targets.
