# walrus has no arena-shaped allocation lifetime (PR 11.17)

> **Status:** decided 2026-08-13 — **declined, structurally.** No arena-shaped lifetime exists to
> bump-allocate into; the loader's measured cost centre is inside DuckDB, not Rust allocation.

## What the rule asks

An arena makes each allocation a pointer bump and frees a group in one reset. That is useful when a
parse tree, request graph, or other set of linked allocations shares one lifetime and nothing
escapes it. The rule's scratch variant puts a reusable arena in `thread_local!` storage so repeated
work on a thread can amortise the arena capacity.

## Why the shape is absent

The decline is structural first and benchmark second:

| arena precondition | walrus shape | evidence |
|---|---|---|
| parse-tree or node graph | `Reader` is only a borrowed slice plus a cursor; `parse_tuple` creates one pre-sized flat `Vec<TupleValue>` | `crates/pg-sink/src/pgoutput/reader.rs:15-18`; `crates/pg-sink/src/pgoutput/mod.rs:228-250` |
| allocations with one reset lifetime | decoded row values move into transaction and batch ownership; there is no graph-wide reset boundary | `crates/pg-sink/src/pgoutput/mod.rs:228-250` |
| Rust allocation dominates a loader cycle | `apply_transform` renders SQL and hands it to DuckDB's `execute_batch` | `crates/loader/src/transform.rs:368-375` |
| worker pool for thread-local scratch | production has zero `thread_local!`; each `!Send` DuckDB connection is pinned to a `LocalSet`, one worker per file | `crates/loader/src/duck.rs:38-42`; `crates/loader/src/main.rs:111-113` |

The measured profile supports that structural result. `docs/benchmarks.md:157-168` shows the window
and `HASH_GROUP_BY` work dominating the 1M-row transform inside DuckDB. Its PR 5.8 record at
`docs/benchmarks.md:319-324` also declines a TOAST back-scan rewrite after an isolated **≈0 delta**.
There is no Rust allocation hot spot here to use as an arena benchmark target; inventing a
micro-benchmark would not establish an applicable lifetime.

## What walrus does instead

PR 11.6 took the standard-library reuse that matches the real batch lifetime. `BatchBuilder` owns
`meta_buf` and `ts_buf` scratch strings (`crates/pg-to-arrow/src/batch.rs:81-84`), clears and refills
`meta_buf` for each row (`:185-206`), and clears and refills the timestamp scratch at its parse sites
(`:876-900`). This retains capacity across a batch without a new allocator, escape lifetime, or
thread-local state.

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
