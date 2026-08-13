# No data parallelism in Walrus (PR 15.3)

> **Status:** decided 2026-08-13 — **rayon and rayon-core are banned in `deny.toml`.** Walrus is an
> ordered, I/O-bound pipeline; its one CPU-heavy stage is SQL run by DuckDB's own thread pool. There
> is no order-independent data-parallel hot path to hand to a second work-stealing scheduler.

## Why parallel decode would be wrong, not merely unnecessary

The replication input is one stateful ordered protocol stream. `DecodeLoop::run` consumes frames in
wire order while maintaining transaction state, relation-cache state, DDL state, batch routing, and
the durability checkpoint. Later messages may depend on structural or transactional messages that
immediately precede them. This is not a slice of independent values suitable for `par_iter()`.

The order remains observable after decode. The raw-to-mirror model selects the winner by maximum
`(commit_lsn, lsn)`, where `commit_lsn` orders transactions and `lsn` breaks ties inside one
transaction. Truncate boundaries, durability advancement, and reload low-watermark algebra use the
same ordered frontier. Sending messages through a work-stealing pool without an explicit ordered
merge would let later state transitions or durability effects overtake prerequisites.

Rayon can preserve indexed collection order for some terminal operations, so work stealing alone is
not a proof that every Rayon program reorders results. The stronger fact here is structural: Walrus
has no pre-collected independent message collection, and reconstructing an ordered commit barrier
around stateful parallel decode would add coordination without exposing independent CPU work. The
correct concurrency for the socket, object store, and control database remains async I/O.

## The evidence

| Probe | Result |
|---|---|
| `rg -n '^rayon\s*=' -g 'Cargo.toml'` | 0 direct dependencies |
| `rg -n '^name = "rayon' Cargo.lock` | no match; 0 of 493 locked packages |
| root `Cargo.toml` criterion entry | `default-features = false`; its comment names dropped `plotters/rayon/html_reports` |
| `crates/loader/benches/transform.rs` | `SET threads = 4`; the measured parallelism knob belongs to DuckDB |

The one `rayon` text mention in a manifest is therefore evidence of removal, not a dependency. The
new `cargo deny` entries turn this from the current absence into an executable policy: any direct or
transitive reintroduction of Rayon or its pool implementation fails the existing supply-chain job.

## Where the CPU actually goes

`docs/benchmarks.md` measures the loader's million-row transform inside DuckDB. Windowing,
`HASH_GROUP_BY`, and table rewrite work execute as SQL, and DuckDB owns the worker pool; the benchmark
pins four DuckDB threads so results do not drift with machine core count. Adding Rayon would create a
second scheduler around a database engine already responsible for the heavy parallel stage.

On the sink side, previously measured per-row Rust cost came from metadata serialization and
temporary allocation. PR 5.7 amortized constant metadata work, and PR 11.6 (whose ownership result is
recorded again by PR 15.2) reuses builder-owned scratch buffers. Neither optimization exposes a
large order-independent collection transform.

## What would reopen this

Revisit only if a representative profile identifies a per-batch, order-independent CPU stage that
dominates wall time after its input is sealed and before any ordered side effect—for example, a pure
Parquet preparation pass over an already immutable batch. A proposal must isolate that stage,
benchmark sequential and bounded parallel forms at production batch sizes, preserve deterministic
output and ordered commit publication, and account for contention with DuckDB and Tokio worker
pools. The same PR would amend this note and remove the two deny entries.

## See also

- Rule: `.claude/skills/rust-skills/rules/conc-rayon-par-iter.md`
- Ordered transform: `docs/architecture.md` §2.1
- Measured CPU work: `docs/benchmarks.md`
- Sibling concurrency decision: PR 15.4's forthcoming `conc-scoped-threads` note
