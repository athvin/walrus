# PR 5.8 — Loader hot-path cleanup: DESCRIBE cache + TOAST back-scan, if the numbers say so

> **Phase:** 5 — Performance & CI · **Crates touched:** `loader` · **Est. size:** M ·
> **Depends on:** PR 5.7 · **Unlocks:** PR 5.9

The loader-side counterpart to 5.7, scoped by the 5.5 baselines and the 5.6 ranking. Two named
candidates: (1) `append_parquet` runs a `DESCRIBE`/introspection query against every Parquet file
before appending, even though the homogeneous-file rule guarantees every file at a given
`schema_version` has the same columns — cache the column list per `(table, schema_version)`;
(2) the unchanged-TOAST resolution is a correlated subquery per TOAST column per winner row — if
5.5 measured it as expensive at realistic TOAST rates, rewrite it as a set-based pass. Same
discipline as 5.7: every change carries its before/after delta; correctness is pinned by the
hermetic transform tests and the e2e unchanged-TOAST test.

## Why — learning objectives

By the end of this PR you will have practised:

- **Exploiting an invariant you already paid for** — the sink's homogeneous-file rule
  (`walrus-pg-sink.md` §3.5) exists for DDL correctness; here it also makes per-file introspection
  redundant. Recognizing when one component's guarantee is another's optimization.
- **Set-based SQL rewrites** — turning a correlated `LIMIT 1` back-scan into a windowed
  last-value-ignore-nulls pass (or a lateral join), and proving equivalence with the existing
  scenario tests rather than by inspection.
- **Declining work** — if the numbers came back small, the deliverable is the recorded finding.

## Read first

- `docs/benchmarks.md` — the 5.5 DESCRIBE line item, the TOAST delta, and the transform scaling
  curve; the 5.6 ranking. **Scope = what these name.**
- `crates/loader/src/duck.rs` — `append_parquet` + `parquet_columns()`; where a
  `(schema_version → Vec<String>)` cache would live (`TableDb` already holds per-table state).
- `crates/loader/src/transform.rs` + `transform.sql` — the back-scan block; how the column list and
  `json_contains` filter are spliced per TOAST-eligible column.
- `crates/loader/tests/transform.rs` (the TOAST scenarios) and `tests/e2e` unchanged-TOAST test —
  the equivalence oracle for any rewrite.
- `docs/walrus-loader.md` §5.6 — why the back-scan exists (same-batch INSERT→TOAST-update) and the
  mirror-fallback `COALESCE` the rewrite must preserve.

## Scope

**In scope** (each gated on its measured share)

1. **Column-list cache.** Key: `(schema_version)` within a `TableDb` (already per-table). On cache
   miss, introspect once; on hit, skip the query. The manifest row carries `schema_version`, so the
   key is free. Invalidate nothing — a version's shape is immutable by the homogeneous-file rule;
   DDL creates a *new* version.
2. **TOAST back-scan rewrite** (only if 5.5's delta justifies it). Candidate shapes, benchmarked
   against each other before committing:
   - pre-compute, per TOAST-eligible column, a windowed
     `last_value(col IGNORE NULLS) OVER (PARTITION BY pk ORDER BY (commit_lsn, lsn))` carry-forward
     over the scan window (winners join against it), or
   - keep the correlated form but pre-filter to winner rows that actually carry a sentinel
     (`json_contains` on the winner first), so the subquery runs per *affected* row, not per row.
   The rewrite must preserve: per-column resolution (only sentinel columns substitute), the
   `(commit_lsn, lsn) ≤ winner` bound, and the mirror-value fallback.
3. **Window-rescan audit** (measurement follow-up, not a rewrite): confirm from the 5.5 scaling
   grid that the `>= after_lsn` tail scan stays O(tail) and is bounded in practice by the retention
   prune; if the audit finds pathological growth between prunes, file the finding — do not change
   the `>=` bound (it is load-bearing for the snapshot straddle, PR 3.10).
4. Re-run the affected 5.5 benches + one 5.6 run; record deltas in `docs/benchmarks.md` §History.

**Explicitly deferred** (do *not* build these here)

- Any change to the transform's ordering/guard semantics (the `(commit_lsn, lsn)` tuple, the
  per-PK applied guard) — correctness machinery, not a perf knob.
- Parallelism *within* a table (concurrent append + transform) — the single-writer model is a
  design decision (`walrus-loader.md` §8.1), not a bottleneck to engineer around.
- Full-rebuild/compaction tuning — off the hot cadence; revisit only with operational evidence.

## Files to create / modify

```
crates/loader/src/duck.rs                # modify — schema_version-keyed column-list cache
crates/loader/src/transform.rs           # modify (maybe) — back-scan rewrite behind the template
crates/loader/src/transform.sql          # modify (maybe) — set-based TOAST carry-forward
crates/loader/tests/transform.rs         # modify — equivalence cases for the rewrite (if taken)
docs/benchmarks.md                       # modify — deltas + the window-rescan audit note
```

## Skeleton

```rust
// crates/loader/src/duck.rs  (shape)
pub struct TableDb {
    // ...
    /// Parquet column lists by schema_version — immutable per version (homogeneous-file rule),
    /// so this cache never invalidates; a DDL bump is a new key.
    parquet_cols: HashMap<i64, Arc<Vec<String>>>,
}

impl TableDb {
    fn columns_for(&mut self, uri: &str, schema_version: i64) -> Result<Arc<Vec<String>>, LoaderError> {
        // hit → clone the Arc; miss → introspect the file once, insert, return
        todo!()
    }
}
```

```sql
-- transform.sql (candidate shape — winners pre-filtered before the correlated scan)
-- ... AND json_contains(s.walrus_pg_sink_meta, '$.unchanged_toast', '"{col}"') ...
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `append_parquet` introspects each `(table, schema_version)` **once**; a Phase-A cycle
      claiming N same-version files runs exactly one introspection (asserted by a test, e.g.
      counting via a probe or verifying the cache is populated after the first file).
- [ ] If the back-scan rewrite landed: the full existing TOAST test matrix passes unchanged
      (same-batch INSERT→update, pruned-value-via-mirror fallback, multi-column sentinels), plus at
      least one new equivalence case seeded to distinguish the old and new SQL if they diverged.
- [ ] If the rewrite was declined: `docs/benchmarks.md` records the measured share and the
      decision.
- [ ] The window-rescan audit conclusion is written down (O(tail) confirmed or a finding filed).
- [ ] Before/after deltas for everything landed, in `docs/benchmarks.md` §History.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace` (the hermetic transform suite is the main oracle)
  - [ ] compose integration: loader Phase A/B tests + the e2e unchanged-TOAST test

## Hints & gotchas

- The cache key is `schema_version` alone *within* a `TableDb` because the struct is already
  per-table — don't build a global map keyed by table too; keep state where the ownership model
  already puts it.
- Spill-kind files ride the same append path with a `commit_lsn` override — make sure the cached
  column list interacts correctly with that branch (the override changes an *expression*, not the
  column list; the cache is still valid).
- `last_value(... IGNORE NULLS)` semantics: the sentinel is **not** NULL — it's a marker value/flag
  in the meta JSON, and a real SQL NULL is a legitimate value that must NOT be carried forward
  (`walrus-pg-sink.md` §2.7). Any windowed rewrite has to treat "sentinel" as the gap to fill,
  never "NULL". This is the trap; the existing tests catch it — run them early and often.
- DuckDB supports `LATERAL` joins and `IGNORE NULLS` on window functions in the pinned 1.4.x — but
  verify the exact syntax against 1.4 docs, not current docs (features drift between LTS lines).
- `EXPLAIN ANALYZE` before/after on the 100k TOAST bench is the review artifact that makes the
  rewrite convincing — paste the operator-level diff into the PR description.

## References

- Design: `docs/walrus-loader.md` §5.6 (back-scan contract), §6.3 (O(new events));
  `docs/walrus-pg-sink.md` §3.5 (homogeneous-file rule → the cache's soundness).
- Prev: [PR 5.7](./pr-5.7-sink-hot-path.md) ·
  Next: [PR 5.9](./pr-5.9-dependency-debt-sweep.md) · [Roadmap](../README.md)
