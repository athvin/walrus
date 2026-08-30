# walrus has no arena-shaped allocation lifetime (PR 11.17)

> **Status:** decided 2026-08-13 — **declined, structurally.** No arena-shaped lifetime exists to
> bump-allocate into; the loader's measured cost centre is inside DuckDB, not Rust allocation.
> Re-audited 2026-08-27: every precondition below is still absent and the evidence line ranges were
> refreshed against the current tree.

## What the rule asks

An arena makes each allocation a pointer bump and frees a group in one reset. That is useful when a
parse tree, request graph, or other set of linked allocations shares one lifetime and nothing
escapes it. The rule's scratch variant puts a reusable arena in `thread_local!` storage so repeated
work on a thread can amortise the arena capacity.

## Why the shape is absent

The decline is structural first and benchmark second:

| arena precondition | walrus shape | evidence |
|---|---|---|
| parse-tree or node graph | `Reader` is only a borrowed slice plus a cursor; `parse_tuple` creates one pre-sized flat `Vec<TupleValue>` | `crates/pg-sink/src/pgoutput/reader.rs:15-20`; `crates/pg-sink/src/pgoutput/mod.rs:258-284` |
| allocations with one reset lifetime | decoded row values move into transaction and batch ownership; there is no graph-wide reset boundary | `crates/pg-sink/src/pgoutput/mod.rs:258-284` |
| Rust allocation dominates a loader cycle | `apply_transform` renders SQL and hands it to DuckDB's `execute_batch` | `crates/loader/src/transform.rs:398-406` |
| worker pool for thread-local scratch | production has zero `thread_local!`; each `!Send` DuckDB connection is pinned to a `LocalSet`, one worker per file | `crates/loader/src/duck.rs:54-59`; `crates/loader/src/main.rs:139-145` |

The measured profile supports that structural result. `docs/benchmarks.md:157-168` shows the window
and `HASH_GROUP_BY` work dominating the 1M-row transform inside DuckDB. Its PR 5.8 record at
`docs/benchmarks.md:354-359` also declines a TOAST back-scan rewrite after an isolated **≈0 delta**.
There is no Rust allocation hot spot here to use as an arena benchmark target; inventing a
micro-benchmark would not establish an applicable lifetime.

## What walrus does instead

PR 11.6 took the standard-library reuse that matches the real batch lifetime. `BatchBuilder` owns
`meta_buf` and `ts_buf` scratch strings (`crates/pg-to-arrow/src/batch.rs:118-121`), clears and
refills `meta_buf` for each row (`:244-255`), and clears and refills the timestamp scratch at its
parse sites (`:973-1020`). This retains capacity across a batch without a new allocator, escape
lifetime, or thread-local state.

The borrow-first habit holds at the other decode sites too: `parse_range` / `parse_multirange` hand
back `ParsedRange<'_>` bounds borrowed from the input text rather than a node graph
(`crates/pg-to-arrow/src/range.rs:104-158`), and the loader's axum health handlers return a
`StatusCode` with no per-request allocation to pool (`crates/loader/src/health.rs:152-168`). Neither
leaves an arena-shaped lifetime behind.

## Dependency status

`bumpalo` is already transitive in `Cargo.lock` through `wasm-bindgen-macro-support` on all-target
resolution and through `zopfli` on the host build, but it has never been a direct workspace or
member dependency. `scripts/no-speculative-deps.sh` scans manifests—not the lockfile—to preserve
that distinction, along with the four other measured Phase 11 dependency declines.

## Revisit triggers

Revisit if walrus gains a recursive decode or planning graph whose allocations provably share one
non-escaping lifetime, or if a representative loader profile shows Rust allocation dominating a
cycle. A proposal must identify the reset boundary, prove that no arena reference escapes it, and
benchmark the real workload against the existing std-only scratch-buffer approach.
