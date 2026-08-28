# `format!` on a hot path (rule `anti-format-hot-path`)

> **Status:** audited 2026-08-28 — **no source change; one guard added.** This rule is the
> anti-pattern statement of three that already landed: `mem-write-over-format` (PR 11.4),
> `mem-avoid-format` (PR 11.5) and `mem-reuse-collections` (PR 11.6). Every path
> `docs/benchmarks.md` measures is `format!`-free, and each of the rule's "Good" spellings is in the
> tree at the site that needs it, with a recorded delta behind it. The gap was enforcement: the one
> lint that backs any of this, `format_push_string`, is `clippy::restriction` — outside every group
> the workspace table denies — and no test pinned it. `crates/common/tests/workspace_lints_inherited.rs`
> now does.

## What counts as a hot path here

Not a guess: `docs/benchmarks.md` names them, with numbers. The per-item paths are the pgoutput
decoder (`parse_tuple`, 236-1 822 ns/row), the Arrow row append (`append_row`, 460-837 ns/row after
PR 5.7), the loader transform (`apply_transform`, 25-474 ms/cycle) and Phase-A append
(`append_parquet`, ~103-175 ms/file). Their `format!` counts:

| benched path | module | production `format!` | what they are |
|---|---|---:|---|
| `parse_stream`, `parse_tuple` | `crates/pg-sink/src/pgoutput/` (7 files) | **0** | — |
| the decode routing loop | `crates/pg-sink/src/consume.rs`, `stream_txn.rs` | **0** | — |
| `append_row`, `finish` | `crates/pg-to-arrow/src/batch.rs` | 2 | both build an `Error::value_parse` payload — `text()` at `:915-921` and `parse_decimal`'s `err` closure at `:934-935`. The constructor is `#[cold]` (`crates/pg-to-arrow/src/error.rs:51-66`) and its payload boxed, so a well-formed cell never reaches either |
| `apply_transform` | `crates/loader/src/transform.rs` | 25 | the per-poll-cycle SQL render, O(columns) once per table per `poll_interval` |
| `append_parquet` | `crates/loader/src/duck.rs` | 1 on the path | `:247`'s `duck_with(\|\| format!(…))`, built only on the error return; the `DESCRIBE` beside it is cached per `schema_version` (`:251-269`, PR 5.8) |

Nothing in the tree renders a `format!` per row, per cell or per WAL record. The one loader entry is
the rule's own "when `format!` is fine" case one step out: a bounded render whose product is then
handed to DuckDB for 25-474 ms of work — and `docs/benchmarks.md:108-110` records that the loader's
time "is mostly not Rust", with `EXPLAIN ANALYZE` (`:235-246`) putting the cost in the window
dedup.

Repo-wide there are 191 textual `format!` occurrences under `crates/*/src` outside the `*_test.rs`
siblings; two are prose/doctest in `common/src/sql.rs:11,54`, leaving **189 production call sites**.
Every one of them is a diagnostic, a SQL/DDL string built once per table or schema version, or a
config-validation message.

## The rule's "Good" column, already in the tree

| the rule's spelling | walrus site | why it is there |
|---|---|---|
| reuse one buffer across iterations | `crates/pg-to-arrow/src/batch.rs:114-121,225-244` — `meta_buf` cleared and refilled with `push`/`push_str` per row, `meta_const` serialized once per sealed file | `docs/benchmarks.md:376-393` — −27.5 % narrow, −18.7 % wide30, −23.0 % text_heavy, −20.4 % tier2, against the isolated ~576 ns/row JSON cost at `:156-173` |
| a *caller-owned* scratch, not an allocation per call | `sink_meta.rs:334-355` appends into the caller's `String`; its sibling `:310-332` is named `to_` precisely because it allocates, and its doc says a per-row caller "has undone" the split | the same PR 5.7 delta |
| `push_str` + `push` over `format!` | `batch.rs:975-1023` — four RFC-3339 parsers share one cleared `ts_buf`; `:1004-1006` states in place that a `replacen` there "would mean a fresh `String` per timestamptz cell" | per-cell, on the 460 ns/row path |
| build incrementally in a single buffer | `crates/pg-sink/src/reload_export.rs:575-610` — `continuation_sql` seeds `sql` once and extends it with `write!(&mut sql, …)` rather than re-`format!`ing | the rule's `build_url` example, verbatim |
| a `Display` impl, caller controls allocation | `crates/common/src/lsn.rs:251-262` — both `Display` and `Debug` are one `write!` into the caller's formatter | `Lsn` is rendered on every stamped row and every log line |
| do not format until you know you need to | `crates/loader/src/duck_ext.rs:6-12,61-85` — `duck_with` takes `impl FnOnce() -> String` and is the trait's *required* method; the `&str` form is defaulted on top of it, so no receiver can make them disagree | 29 DuckDB call sites outside that module pay their operation text only on failure |
| hoist the render out of the repeated path | the precomputed metric label: `crates/loader/src/phase_a.rs:32-34` (`TableCtx::series`) and `crates/pg-sink/src/reload_export.rs:227` | metrics take `&str`; nothing formats a label per observation |
| no allocation at all for a log field | 29 `format_args!` sites across 9 production files | `format_args!` borrows and renders into the subscriber, e.g. `phase_b.rs:113-123` |

Two more sites are the rule's principle applied to a non-`format!` allocation, and are worth naming
because they show it is a habit rather than a coincidence: `crates/pg-sink/src/batch.rs:251-253`
builds the `batch_id` inside `get_or_insert_with`, so it costs one render per batch and not one per
`push`; and both row loops — `snapshot.rs:250-253` and `reload_export.rs:442-445` — refill one
`Vec<TupleValue>` scratch instead of allocating a fresh row buffer. `phase_b.rs:31-39` and
`phase_a.rs:366-374` each carry a comment explaining that the `"schema.table"` label is built
*inside* the `map_err` closure "so the every-cycle success path allocates nothing".

## The pattern walrus does not take

The rule's **Formatter Buffer Pool** is a `thread_local! { RefCell<String> }`. That is already a
recorded decline: `conc-thread-local.md` (PR 15.2, superseded by PR 11.6) — walrus keeps reusable
scratch on the object that owns the work and introduces no ambient per-thread state, which is
exactly what `meta_buf` and `ts_buf` above are. The pool's own docstring concedes it "still [costs]
one allocation per call"; the field on `BatchBuilder` costs none.

## The lint

The rule asks for `format_in_format_args = "warn"`. It is a `clippy::perf` lint, so `Cargo.toml:60`
(`all`) and `:128` (`perf`) already carry it at **deny** — stricter than asked — and a grep for a
`format!` nested inside a format-family macro's arguments finds nothing. Its complexity sibling
`useless_format` arrives the same way through `:108`. Naming either in the table would restate a
group entry and contradict the reach that entry records, so neither is added.

`format_push_string = "deny"` (`Cargo.toml:185-187`, PR 11.4) is the one that is *not* covered:
`clippy::restriction` is in no group, so `all`, `perf` and `complexity` all pass over it and the
named line is the only thing that reaches `push_str(&format!(..))`. There are zero sites, so
deleting that line turns nothing red anywhere.

## What this audit changed

Nothing in `crates/*/src`. One test:
`workspace_lints_inherited.rs::the_workspace_lint_table_still_denies_formatting_into_an_existing_buffer`
asserts the manifest pins `format_push_string = "deny"` exactly once, and asserts
`format_in_format_args` stays *absent* — so the decision above is a line in a test rather than a
silence. It follows the shape of the `dbg_macro` and `raw_stream_prints` guards beside it, including
the synthetic-table probe that keeps the scan from passing vacuously.

The rule's remaining shape — a `format!` inside a loop — has no lint in clippy at all, and a
source-scanning guard for it would fire on the legitimate low-frequency loops that exist
(`ddl.rs:305-341`, one iteration per destructive schema change; `reload_export.rs:341-352`, at most
three echo attempts per chunk; `bootstrap.rs:76-78` and `main.rs:128-129`, once per table at
startup). The benches stay its detector.

## Reversal condition

Re-open when a profile — `cargo instruments -t alloc --bench batch`, or `heaptrack` over a
`just bench-e2e` run (`docs/benchmarks.md:82-114`) — attributes a measurable share of decode, append
or transform time to formatting rather than to DuckDB, `serde_json` or Arrow. Rendering the loader's
transform SQL is the only candidate large enough to look at first, and it is bounded by
`poll_interval` against a 25-474 ms cycle; caching a `TransformSql` across cycles would need a
`schema_version` invalidation path and must clear the same bar `mem-smallvec.md` and
`mem-compact-string.md` were held to — a back-to-back delta with non-overlapping confidence
intervals, not an expectation.
