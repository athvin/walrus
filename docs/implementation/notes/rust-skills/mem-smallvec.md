# SmallVec for usually-small collections (PR 11.13; re-audited repo-wide)

> **Status:** declined — keep `PgRelation::to_key_columns()` (renamed from `key_columns` by PR 27.2)
> returning its existing `Vec<&str>`, and do not add or promote a `smallvec` dependency. No
> usually-small collection anywhere in the workspace switches to an inline vector.
> `scripts/no-speculative-deps.sh` enforces this by naming this file as the decision of record.

## Evidence

The final `loader/keycols` Criterion verification measured median times of **41.554 ns** for a
one-key relation and **45.635 ns** for a three-key relation on the documented Apple M2 benchmark
machine. An initial run measured 43.083 ns and 43.061 ns; Criterion reported no detectable change
between runs. The fastest committed loader transform-cycle median is **25.1 ms**, making key
collection about 0.00017–0.00018% of that cycle (over 550,000× smaller). The two key counts are also
indistinguishable within their confidence intervals.

DuckDB windowing and merge work dominates the loader profile. Avoiding this tiny allocation cannot
materially affect throughput, while `SmallVec` would add an API/storage branch and expand the direct
dependency surface.

## Repo-wide census

PR 11.13 measured one site. The rule is repo-wide, so every other usually-small collection was
re-audited by inspection; each keeps its ordinary `Vec`, for the reason given.

- `crates/common/src/pg_shape.rs:122` — `to_key_columns() -> Vec<&str>`, 1–3 keys. The measured site
  above.
- `crates/pg-sink/src/pgoutput/mod.rs:258` — `parse_tuple() -> Vec<TupleValue>`, one per decoded
  row. This is the hottest candidate in the workspace and the one **structurally** blocked: the
  committed move-cost budgets cap `TupleValue` at 40 bytes (`pg_shape.rs:155`) and `Message` at 88
  (`mod.rs:213`), and `Message::Update` carries two tuple lists. With smallvec 1.x layout (a
  `capacity` word plus a union of the inline array and `(ptr, len)`), even
  `SmallVec<[TupleValue; 1]>` is 48 bytes — two of those is 96, so the *smallest* useful inline
  capacity overruns the 88-byte budget before `Update`'s own fields are counted, and
  `[TupleValue; 4]` (168 each) would put the variant near 350. The niche assert at
  `mod.rs:216-220` further assumes a pointer-carrying container for `Option<Vec<TupleValue>>`. No
  measurement can buy this back — the budgets would have to be raised first, deliberately.
- `crates/pg-to-arrow/src/geometric.rs:122` — `extract_points() -> Vec<Pt>`: one point
  (point/circle), two (lseg/box), unbounded (path/polygon). Textbook "usually small, sometimes
  large" shape, but `parse_path`/`parse_polygon` are `pub` and hand the container across crate
  boundaries, so adopting `SmallVec` would export a third-party type from `pg-to-arrow`'s API. No
  benched shape contains a geometric column, so there is nothing to measure against. It already
  makes a single allocation, sized from an exact `(`-count upper bound.
- `crates/pg-to-arrow/src/tier2.rs:136` — `toks: Vec<&str>` per interval literal, usually ≤ 8 tokens,
  read by index for lookahead. The only candidate a committed benchmark actually covers
  (`arrow/append_row/tier2_fanout`, 1,367.7 ns/row across three Tier-2 cells). Measurable but
  unmeasured: the reversal condition below is the route.
- `crates/pg-to-arrow/src/range.rs:234` — `split_members() -> Vec<&str>` per multirange value,
  usually 1–3 members. Unbenched.
- `crates/pg-to-arrow/src/geometric.rs:199` — `parse_line`'s three-element `Vec<&str>`. Fixed arity,
  so `ArrayVec` territory rather than `SmallVec`
  (`docs/implementation/notes/rust-skills/mem-arrayvec.md`, likewise declined), and off every
  benched shape.
- `crates/loader/src/transform.rs` (`to_pk_names`, `to_non_key_names`, the `Vec<String>` SQL
  fragment lists) and `crates/loader/src/plan.rs` — SQL-template construction, not per-row work.
  Each call renders a whole statement out of dozens of `format!` allocations, so the container is
  noise within its own function, let alone within the 25.1 ms cycle it feeds.

No manifest declares `smallvec`, `arrayvec`, `tinyvec`, or `thin-vec` as a direct dependency; the
`Cargo.lock` entries are transitive.

## Reversal condition

Revisit only if end-to-end profiling attributes a material share of loader cycle time or allocation
pressure to one of the sites above. A new proposal must include that profile, the observed length
distribution for that collection, and an isolated before/after benchmark whose confidence intervals
show a real improvement. A `parse_tuple` proposal must additionally raise `TUPLE_VALUE_MAX_BYTES` /
`MESSAGE_MAX_BYTES` on that evidence, in review. Until then, keep the ordinary `Vec` and make no
iterator or inline-vector rewrite.
