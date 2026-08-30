# No `spawn_blocking` for borrowed DuckDB work (PR 14.18)

> **Status:** decided rejection, plus one open finding. Current DuckDB operations borrow a
> `Send + !Sync` connection and cannot satisfy `tokio::task::spawn_blocking`'s closure bound as
> written. `LocalSet::run_until` explicitly disables `block_in_place` while polling the loader
> workers, so using it at those sites would panic. The resulting shared-thread stall is not accepted;
> it is filed below. The sink's Parquet encode (§5) *can* meet the bound and is declined on evidence
> instead, alongside the `/metrics` render (§4).

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

`crates/loader/src/app.rs`'s `pipeline` creates one `LocalSet` and calls `JoinSet::spawn_local_on` once
for every owned table, then drives the whole fleet through a single `LocalSet::run_until`. That is one
worker task per `.duckdb` file, not one OS thread per file. All those tasks are polled by the same
LocalSet driver thread — the thread `main.rs`'s `runtime.block_on` occupies.

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

## 5. The sink's Parquet encode — same bar, same answer

`put_with_kind` in `crates/pg-sink/src/sink.rs` is the sink's counterpart to §1 — the one CPU-bound
block on its flush path, and the heaviest site this rule reaches outside the loader. It encodes a whole
sealed batch — `max_rows` 100 000 rows or `max_bytes` 128 MiB by default —
to Snappy-compressed Parquet on whichever runtime thread is polling the flush. Every producer reaches it
(streamed WAL, backfill, open-txn spill, reload export), and each one calls the synchronous
`Batcher::seal` → `into_record_batch` on that same thread immediately before, so the CPU block is the
Arrow finalization plus the encode.

`AsyncArrowWriter` does not change that. In parquet 54.3.1 it holds a synchronous
`ArrowWriter<Vec<u8>>`: `write` runs `sync_writer.write(batch)` before it awaits anything and touches the
async writer only when that batch closed a row group, and `close` → `finish` runs `sync_writer.finish()`
the same way. `default_writer_properties` leaves `max_row_group_size` at parquet's 1 048 576-row default,
so a default-sized batch is a single row group and its encode is one uninterrupted block of CPU on the
task. The `async` in that type name is the upload, not the compression.

Unlike §1 this site type-checks: `SealedBatch` owns its `RecordBatch`, which is `Send + 'static`, and
`pg_to_arrow` already exports the synchronous `write_parquet_bytes` such a closure would call. It is
declined anyway, on two grounds:

- **Nothing measured separates the encode from the PUT.** The only series over this path,
  `walrus_sink_batch_flush_latency_seconds`, is stamped inside `put_with_kind` around the encode and the
  multipart upload together, so no recorded number says which half dominates. Walrus prices an
  optimization against a `docs/benchmarks.md` delta; a blocking-pool hop plus a join-failure path bought
  on a hunch is the same trade §4 declines for `render()`.
- **It would buy a thread, not a task.** The consume loop awaits the flush either way, so offloading
  makes nothing in this pipeline happen sooner. It frees one of `worker_threads` runtime threads
  (`available_parallelism()` by default) for the duration, and tokio's work stealing already moves the
  health, heartbeat and reload-exporter tasks off a busy worker. That is the inverse of §3, where the
  blocked thread is the only driver the sibling tables have.

## What would reverse this

For DuckDB operations, landing the owned-`TableDb` move-and-recover design would satisfy
`spawn_blocking` and reverse this rejection for those operations. A `Sync` connection alone would not:
the closure would still need owned `'static` state. A per-file runtime/thread topology instead resolves
the shared-thread finding while intentionally keeping DuckDB calls synchronous on their dedicated
thread.

For §5, what reverses it is a profile that splits that histogram and shows the encode dominating. The
conversion is then local, and its shape is fixed by the durability point above: encode with
`pg_to_arrow::write_parquet_bytes` inside `spawn_blocking`, then hand the bytes to the same
`object_store::buffered::BufWriter` (whose `put` takes `Bytes` without a copy) and shut it down. What
must not follow is a rewrite to a single `ObjectStore::put`: `WrittenObject` may be returned only after
the multipart upload completes.
