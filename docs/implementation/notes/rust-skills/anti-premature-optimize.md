# Optimizing before profiling (rule `anti-premature-optimize`)

> **Status:** audited 2026-08-28 — **no source change.** Every optimization committed to this tree
> already cites a back-to-back measurement in `docs/benchmarks.md` or a structural argument, and the
> declines cite one too. Four of the rule's five named premature optimizations have zero sites, each
> held there by a lint, by the `unsafe` forbid, by the toolchain pin, or by there being nothing to
> pool. The fifth, a custom global allocator, had no guard at all: `scripts/no-speculative-deps.sh`
> now names the four crates that install one, pointing at this note.

## The rule's five named premature optimizations, against this tree

| the rule's row | walrus state | what keeps it that way |
|---|---|---|
| `#[inline(always)]` everywhere | 0 sites in `crates` + `tests` | `clippy::inline_always = "deny"` (`Cargo.toml:236-239`); the escape hatch is a site-local allow citing a `docs/benchmarks.md` delta |
| `unsafe` for bounds-check removal | 0 `unsafe`, 0 `get_unchecked` | `unsafe_code = "forbid"` (`Cargo.toml:29`); the decoder additionally denies `clippy::indexing_slicing` (`crates/pg-sink/src/pgoutput/reader.rs:1`), so every hot-path read carries a bounds proof instead of skipping one |
| Custom allocator | 0 `#[global_allocator]` | a hand-written one needs `unsafe impl GlobalAlloc`, which the forbid above rejects; a crate one needs a manifest entry — guarded from this audit on (below) |
| Object pooling | none | the only pools are `sqlx::PgPool` (`crates/control/src/db.rs:99`) and Tokio's blocking pool (`crates/common/src/runtime.rs:9`) — bounded I/O resources, not recycled allocations |
| Manual SIMD | none | `opt-simd-portable.md`: `std::simd` is nightly-only against the pinned 1.95.0, intrinsics need `unsafe`, and `crates/common/tests/build_profile.rs` fails any build surface that sets `target-cpu`, `target-feature` or rustflags |

## Every optimization in the tree, and the measurement it cites

| site | what it does | evidence |
|---|---|---|
| `crates/pg-to-arrow/src/batch.rs:116-121,221-244` | serializes the batch-constant `SinkMeta` once per sealed file and splices `{const,row}` into a reused buffer | `docs/benchmarks.md:376-393` — −27.5 % narrow, −18.7 % wide30, −23.0 % text_heavy, −20.4 % tier2, against the isolated ~576 ns/row JSON cost at `:157-173` |
| `crates/loader/src/duck.rs:51-54,251-268` | caches the Parquet column list per `schema_version`, so N same-version files run one `DESCRIBE` | `docs/benchmarks.md:222-233` (a fixed ~10 ms/file, ~10 % of a narrow append) and `:417-430` |
| `crates/pg-sink/src/pgoutput/reader.rs` (14) + `crates/common/src/lsn.rs:66,73` | `#[inline]` exports the small cursor bodies across crate boundaries | `docs/benchmarks.md:304-322` — −19.1 / −9.4 / −18.1 % on the three `parse_tuple` shapes, measured back-to-back |
| `reader.rs:185-186`; `batch.rs:702-703,718`; `pg-to-arrow/src/error.rs:55`; `common/src/string_enum.rs:23` | `#[cold]` on the five error constructors, `#[inline(never)]` on the two whose payload allocates inside a hot loop | rationale in place at each site, sized against the 460 ns/row append and the per-cell decode path; `reader_test.rs` / `batch_test.rs` pin the payloads |
| `Cargo.toml:387-388` | `[profile.release] lto = "thin"` | `docs/benchmarks.md:395-400` — an **honest null** on the micro-benches (−0.8 % to +1 %, mixed sign), kept as cross-crate release-artifact hygiene, with `codegen-units` left at the default |
| 22 production `with_capacity` sites | reserve once from a length already in hand (PR 11.1) | not an optimization needing a profile: `n` is `rel.columns.len()`, `registry.len()`, `claimed.len()`, a frame length. Two are estimates and say so in place — `crates/loader/src/ddl.rs:215` (`changes.len() * 128`, on the once-per-DDL path) and `crates/pg-to-arrow/src/geometric.rs:130-134`, which pays one extra byte scan for an exact upper bound |

The declines are recorded on the same terms, each with a number rather than an opinion: `SmallVec`
(`docs/benchmarks.md:342-355` — 41 ns against a 25.1 ms cycle), compact strings (`:324-340`), the
TOAST back-scan rewrite (`:432-437` — ≈0 delta, DuckDB decorrelates it), the window-rescan audit
(`:439-443`), the sink's ownership-taking `push` (`:402-409` — deferred *because* PR 5.6 ranked the
loader as the limiter), arenas (`mem-arena-allocator.md`), and faster hashers (`perf-ahash.md`,
restated in place at `crates/pg-sink/src/consume.rs:698`). PR 11.12 (`:357-374`) is the sharpest of
them: it kept a change that *measured slower*, and says so, on the strength of its single-allocation
invariant rather than a timing claim.

`docs/benchmarks.md:56-64` is where the ordering the rule asks for is written down — bench the path,
profile it, change one thing, re-measure against a saved baseline — and `:40-53` records the harder
lesson behind it: absolute medians drift, so only a back-to-back delta counts.

## What this audit changed

Nothing in `crates/`. The one gap was enforcement, not practice: a global-allocator swap is the only
row in the rule's table that a single manifest line can reach from safe, stable Rust —
`#[global_allocator] static A: MiMalloc = MiMalloc;` needs no `unsafe` in walrus, so the forbid that
blocks a hand-written `GlobalAlloc` does not block a crate that ships one. `docs/benchmarks.md:111-114`
already refuses a global-allocator dependency "taken on spec" while pointing at the allocation
profilers that would justify one (`cargo instruments -t alloc`, `heaptrack`); this audit gives that
sentence a guard. `scripts/no-speculative-deps.sh` now also rejects `tikv-jemallocator`,
`jemallocator`, `mimalloc` and `snmalloc-rs` as **direct** dependencies of any manifest.

Unlike the container and hasher rows in that list, these four are absent from `Cargo.lock` entirely —
so the guard is a tripwire on a door nobody has opened, not a record of a resolution walrus already
depends on. Any other repackaging of the same idea falls under this decision even though it is not
spelled in the list.

## Reversal condition

Re-open when an allocation profile — `cargo instruments -t alloc` over `cargo bench -p pg-to-arrow
--bench batch`, or `heaptrack` on a `just bench-e2e` run — attributes a measurable share of sink or
loader time to `malloc`/`free` frames rather than to DuckDB, `serde_json`, or Arrow. A proposal must
show that A/B on the affected bench with non-overlapping confidence intervals, the same bar
`mem-smallvec.md`, `mem-compact-string.md` and `perf-ahash.md` were held to, and must delete the row
it obsoletes from `scripts/no-speculative-deps.sh` rather than working around it. The same bar
applies to anything else this rule covers: no `#[inline(always)]`, ISA floor, PGO pass, or
hand-rolled container lands on an expectation.
