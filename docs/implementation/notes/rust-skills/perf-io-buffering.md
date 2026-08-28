# Buffered IO: every stream walrus owns is already batched (rule `perf-io-buffering`)

> **Status:** audited 2026-08-28 — **no change.** walrus never opens a file for streaming read or
> write in production: the only local-disk paths are DuckDB's own C++ IO and a `remove_file`. The
> three byte streams it does own are each already in the form the rule asks for — S3 objects go
> through `object_store::buffered::BufWriter` behind an `AsyncArrowWriter` whose `close()` is awaited
> with `?`; the replication socket reads into a 16 KiB `BytesMut` framing buffer and writes whole
> pre-assembled messages; the health/metrics endpoints are hyper's. Every remaining `File` touch in
> the tree is a one-shot whole-file slurp or a single `write_all` of a complete in-memory buffer.
> Adding `BufReader`/`BufWriter` to any of them is the redundant second layer the rule's own last key
> point warns against.

## Inventory: every byte stream in the tree

| site | stream | direction | what batches it today |
|---|---|---|---|
| `pg-sink/src/sink.rs:118` | S3 object (multipart) | write | `object_store::buffered::BufWriter` under `AsyncArrowWriter` |
| `pg-sink/src/replication.rs:453` | source socket (CopyBoth) | read | 16 KiB `BytesMut` (`:127`) + `take_message` framing (`:636`) |
| `pg-sink/src/replication.rs:325`, `:342`, `:468`, `:536` | same socket | write | one `write_all` per complete, pre-built message |
| `pg-sink/src/health.rs:235`, `loader/src/health.rs:217` | health/metrics HTTP | both | axum/hyper's own connection buffers |
| `loader/src/duck.rs`, `loader/src/ddl.rs:363` | `.duckdb` files | both | DuckDB (C++); the Rust side only `remove_file`s |
| `common/src/telemetry.rs:103`/`:106` | process stdout | write | std's `Stdout` is a `LineWriter` — deliberate, see below |
| `pg-to-arrow/src/parquet.rs:30` | generic `W: Write` | write | only ever called with `&mut Vec<u8>` — no syscall at all |

No production site calls `File::open`, `File::create`, `OpenOptions`, `read_line`, `read_until` or
`lines()` on an IO type. The speculative open-txn spill (`pg-sink/src/stream_txn.rs:320-360`), the
one place a local temp file would be natural, PUTs to S3 staging instead — so it inherits row 1
rather than adding a disk stream.

## The one buffered writer, and its flush

`sink.rs:117-125` is the whole of walrus's buffered-writer surface. It matters because the rule's
first key point is the sharp edge here: a dropped `BufWriter` flushes and **discards the error**.
Nothing in walrus relies on that drop. `AsyncArrowWriter::close()` is awaited with `?` at `:125`, and
in the pinned parquet 54.3.1 that chains `close` → `finish` → `AsyncFileWriter::complete` →
`flush().await?` + `shutdown().await?`, which is what completes the multipart upload. So the flush
error is a `SinkError` on the same `?` the caller already handles.

The ordering downstream is what makes that load-bearing rather than tidy: `put_with_kind` returns a
`WrittenObject` only after `close()` returns `Ok`, and the manifest INSERT and the slot advance both
sit behind that return (the WAL-bounding invariant in the module header). A swallowed flush would
mean a manifest row pointing at an object that was never completed — the exact silent corruption the
rule describes, in the one shape walrus could suffer it. It cannot, because the error is propagated.

## The replication socket is a hand-rolled `BufReader`, and better

`read_message` (`replication.rs:448-461`) loops `take_message(&mut self.rbuf)` and only issues a
syscall when a full frame is not yet buffered, reading into the spare capacity of a `BytesMut` sized
16 KiB at connect (`:127`). That is `BufReader`'s bargain — one large read amortised over many small
framed takes — with two properties `BufReader` cannot give:

- **Zero copy.** `take_message` hands back `Bytes` slices of the read buffer (`:646-647`), and the
  XLogData payload rides that `Bytes` straight into the pgoutput decoder (`:40-41`). A
  `tokio::io::BufReader` would copy every WAL byte a second time and hand back `&[u8]`, which cannot
  be kept past the borrow.
- **Cancel safety.** Retained bytes survive a cancelled `read_buf`, which is what lets the feedback
  timer cancel a read mid-wait (`:446-447`, `:266-267`). That property is the reason the buffer is a
  field rather than a local, and the same reason a second buffering layer would be a liability.

The buffer does not degrade as it drains: once every frame taken out of it has been dropped, bytes
1.12's `BytesMut::reserve_inner` finds the allocation unique and reclaims the whole 16 KiB by moving
the unconsumed tail to the front, rather than allocating or shrinking the read window.

## Why the writes stay unbuffered

There are four write sites and each one already hands the kernel a complete message: a 39-byte
standby status built in a stack array (`build_standby_status`, `:586`; the width is a protocol
constant, `:581`), the 5-byte `CopyDone` (`:342`), and one `Vec` apiece for the startup packet and
each `'Q'` query (`:531-537`). There is no per-record or per-field write anywhere on this socket, so
the rule's "many small operations" premise is unmet before the cost question is even asked.

Cadence settles it: `DEFAULT_FEEDBACK_INTERVAL` is 1 s (`:38`), plus an immediate answer whenever the
walsender sets `reply_requested` (`:399-401`). That is order one syscall per second against the
millions the rule is written for. And buffering would be actively wrong twice over:

- **Feedback must not sit in a buffer.** Standby status is what keeps the primary from severing the
  connection at `wal_sender_timeout` (§1.9, module header). Bytes parked in a `BufWriter` are bytes
  the walsender has not seen; a keepalive that flushes "soon" is the reconnect storm the unconditional
  feedback path exists to prevent.
- **It would widen the cancel-safety window, not close it.** `:264-275` documents the residual — a
  dropped future can leave a partial frame — and names the fix as resumable outbound staging in a
  `wbuf` mirroring `rbuf`. A `BufWriter` is not that fix: it adds a second place unflushed bytes can
  sit across an await, with no resume protocol.

The measured picture agrees that nothing here is a bottleneck. `docs/benchmarks.md:258` puts the
sink's mean flush at 8.2 ms — an S3 round trip, not syscall overhead — while sink `inflight` and
`spill` both stay 0 and the loader is what saturates first (`:264-268`). `docs/benchmarks.md:59` is
the house rule against changing an IO path no measurement has implicated.

## Why the log writer stays unbuffered

`init_tracing` (`common/src/telemetry.rs:97-107`) installs a fmt layer over `io::stdout()`, i.e. std's
`LineWriter`: one write syscall per event, and per-WAL-record logging is `trace!` (`consume.rs:995`),
off under the default `info` filter — so production volume is per batch, commit and file.

Buffering it (a `BufWriter`, or `tracing-appender`'s non-blocking writer) would break the e2e suite by
construction. `spawn_sink` redirects the sink's stdout **and** stderr into `sink.log`
(`tests/e2e/src/lib.rs:734-741`), and the harness polls that file *while the sink runs*:
`await_spill` (`:179-191`) and `await_heartbeat_roundtrip` (`:589-601`) both loop against a deadline
on lines the sink has not necessarily finished writing. Line-buffering is what makes those
deterministic instead of flaky. The same property is why a crash keeps its last log line — the tail a
buffered writer drops is precisely the part a post-mortem needs.

## The remaining `File` touches are one-shot by shape

- `pg-to-arrow/tests/conformance.rs:63-67` and `loader/benches/append.rs:106-112` write a Parquet
  fixture with a single `write_all` of a complete in-memory buffer, then `flush()` explicitly. A
  `BufWriter` would copy the buffer a second time to issue the same one write.
- The guard tests and the log scrape read whole files (`fs::read_to_string`, ~20 sites), which sizes
  its buffer from the file's own metadata and reads it in large chunks. `Harness::grep_sink_log`
  (`tests/e2e/src/lib.rs:565-569`) then counts `matches` across the whole text — something
  `BufRead::lines()` cannot do, while allocating a `String` per line that `str::lines()` does not.
  `pg-sink/tests/asmut_absence.rs:43-49` is the same single sized slurp wearing an explicit
  `OpenOptions`.
- The dependency-free HTTP helpers (`pg-sink/tests/health.rs:19-33`, the two `metrics_scrape.rs`
  `get_body` helpers, `tests/e2e/src/lib.rs:811-831` and `:835-863`) send one `write_all` request and
  take the reply with one `read_to_end`.
- `tests/e2e/src/lib.rs:738` is a `File::create` the parent never writes to — it is handed to the
  child as `Stdio`, so the buffering that matters is the child's (above).

## Reversal condition

Re-open when walrus first streams to a local file or issues small writes in a loop. The concrete
triggers: the deferred `wbuf` outbound staging lands on the replication socket (`replication.rs:274`
— it should stage into a buffer *it* can resume, and that design subsumes this rule); a spill or
export ever targets local disk instead of S3 staging (`stream_txn.rs:320-360`); or `write_parquet`'s
generic `W` (`pg-to-arrow/src/parquet.rs:30`) is called with a `File` or socket rather than the
`Vec<u8>` every caller passes today — `ArrowWriter` emits page-sized writes, so that caller must
bring its own `BufWriter` and flush it before dropping.
