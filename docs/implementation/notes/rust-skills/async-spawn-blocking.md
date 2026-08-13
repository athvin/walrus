# No `spawn_blocking` for borrowed DuckDB work (PR 14.18)

> **Status:** decided rejection, plus one open finding. Current DuckDB operations borrow a
> `Send + !Sync` connection and cannot satisfy `tokio::task::spawn_blocking`'s closure bound as
> written. `LocalSet::run_until` explicitly disables `block_in_place` while polling the loader
> workers, so using it at those sites would panic. The resulting shared-thread stall is not accepted;
> it is filed below.

## What the rule asks for

The `async-spawn-blocking` rule treats work over 1 ms as work that should leave an async executor
thread. Its examples move owned `Vec<u8>`, `String`, or `PathBuf` values into a `Send + 'static`
closure. DuckDB table rewrites, pruning, and checkpoints easily cross that threshold, but Walrus's
steady-state API has a different ownership shape.

## 1. Why the bound cannot be met as written

Tokio 1.52.3 declares the relevant shape as:

```text
spawn_blocking<F, R>(f: F)
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static
```

Walrus pins `duckdb = "=1.10504.0"`. In that release,
`duckdb-1.10504.0/src/lib.rs` defines `Connection` with a
`RefCell<InnerConnection>` and immediately provides `unsafe impl Send for Connection {}`. It does
not provide `Sync`. `TableDb` adds another `RefCell` for its Parquet-column cache, so the resulting
facts are:

- `duckdb::Connection: Send` and `TableDb: Send`; an owned handle may move between threads.
- `duckdb::Connection: !Sync` and `TableDb: !Sync`; therefore `&Connection: !Send` and
  `&TableDb: !Send`, because a shared reference is `Send` only when its referent is `Sync`.

`crates/loader/src/duck_test.rs` instantiates positive `T: Send` bounds for both owned types.
`TableDb`'s rustdoc has two `compile_fail` cases that independently guard the negative shared-reference
facts without adding a trait-assertion dependency.

The steady-state blocking sites all borrow: `full_rebuild`, `full_rebuild_abortable`, `prune_raw`,
and the apply-loop drain receive `&duckdb::Connection`. Capturing one in a blocking-pool closure is
both `!Send` and non-`'static`. The existing `InterruptHandle` does not change those traits; it is a
separate `Send + Sync` cancellation handle.

An ownership redesign could type-check. Because `TableDb` is `Send`, a worker could move the whole
database into a blocking closure and recover it from the `JoinHandle`. That is not a local wrapper:
`TableDb` must first move out of `TableCtx`, and every success, DuckDB error, join failure, cancellation,
and panic path must leave worker ownership well-defined. Drain abort can be preserved by obtaining the
`InterruptHandle` before the move and letting the existing watcher interrupt the blocking query; the
handle is not the blocker. That restructure is deferred to the concurrency work rather than hidden in
this evidence task.

`TableDb::open` is the owned exception: an owned path can enter a blocking closure and the resulting
`TableDb` can return from it. It runs in the sequential bootstrap critical path, where the next step
cannot proceed until the file is open; offloading would preserve readiness latency while adding a join
failure path. It is therefore measured and declined here, not incorrectly classified as a borrowed
call.

## 2. Why `block_in_place` does not help

The loader builds a Tokio 1.52.3 multi-thread runtime and drives `run` with `runtime.block_on` on the
main thread. Tokio's multi-thread scheduler normally permits `block_in_place` in that outer
`block_on` context. The actual DuckDB calls, however, are polled through `LocalSet::run_until`.

In Tokio 1.52.3, `LocalSet::RunUntil::poll` installs `disallow_block_in_place()` before polling both
the `run_until` future and the local task queue. The scheduler then rejects `block_in_place` with its
"can call blocking only when running on the multi-threaded runtime" panic path. Thus it is not a
legal wrapper at any apply-worker DuckDB site. The distinction matters: the runtime is multi-threaded,
but the current polling scope deliberately forbids blocking-in-place.

Even without that explicit restriction, `block_in_place` would not solve the topology problem. It
can hand a runtime worker's scheduler core to another thread, but it neither moves nor advances tasks
owned by a `LocalSet`; those remain pinned to the one driver thread.

## 3. Open finding — all apply loops share one thread

`crates/loader/src/main.rs` creates one `LocalSet` and calls `spawn_local` once for every owned table.
That is one worker task per `.duckdb` file, not one OS thread per file. All those tasks are polled by
the same LocalSet driver thread.

`full_rebuild_abortable` then calls synchronous `full_rebuild` inside one of those tasks. Its
`CREATE OR REPLACE TABLE … AS SELECT` can run for seconds. During that time every other table misses
its apply cadence and cannot update `last_poll_completed_at`, the value `/healthz` uses to recognize
a live worker. The runtime-pool watcher can interrupt the current query on cancellation, but it cannot
poll the blocked sibling LocalSet tasks.

This ADR does not bless that cross-table stall. The follow-on is an ownership/topology change: either
move each owned `TableDb` through bounded blocking jobs with complete recovery semantics, or run one
`current_thread` runtime per `.duckdb` on its own OS thread. Either design must retain the
`InterruptHandle` drain guarantee and ordered lease release. It is explicitly not implemented here.

## 4. Measured and declined

Both health servers synchronously call `common::metrics::render()` in their `/metrics` handler. The
work is an in-memory text render whose cardinality is bounded by configured/owned table series; it
does not hold a DuckDB handle or perform I/O. No latency evidence justifies adding a blocking-pool hop,
so it remains synchronous. If profiles later show millisecond-scale render stalls, that measurement
would justify revisiting it independently of DuckDB ownership.

## What would reverse this

For DuckDB operations, landing the owned-`TableDb` move-and-recover design would satisfy
`spawn_blocking` and reverse this rejection for those operations. A `Sync` connection alone would not:
the closure would still need owned `'static` state. A per-file runtime/thread topology instead resolves
the shared-thread finding while intentionally keeping DuckDB calls synchronous on their dedicated
thread.
