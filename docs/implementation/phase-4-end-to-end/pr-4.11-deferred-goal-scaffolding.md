# PR 4.11 — Deferred-goal scaffolding: CTID-range snapshot & loader-sharding hooks

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/78

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `pg-sink`, `loader`, `control` (docs +
> inert hooks) · **Est. size:** S · **Depends on:** PR 4.10 · **Unlocks:** — (finish line)

The last PR. It does **not** build new v1 behaviour — it makes the three explicitly *deferred* design goals
discoverable and forward-compatible so a future contributor knows exactly where they plug in, and confirms
the inert forward-compat hooks (the loader's **fencing token** from PR 3.1) are present and unused. It
documents (a) parallel **CTID-range snapshotting** for faster backfill, (b) multi-pod **loader
table-sharding** for horizontal scale-out guarded by the fencing token, and (c) single-table
reload-while-streaming — each as a doc + a clearly-marked seam, with **no regression** to anything shipped.

## Why — learning objectives

By the end of this PR you will have practised:

- **Leaving good seams, not dead code** — documenting a deferred capability and placing a minimal,
  clearly-labelled extension point rather than half-implementing it.
- **Forward-compat with a fencing token** — confirming the token minted in PR 3.1 is the exact hook
  multi-pod sharding will later use, and that it is inert today (single active loader).
- **Reading the design's own "deferred vs non-goal" distinction** — these are *intended* capabilities,
  deliberately sequenced later, not permanent non-goals.
- **Finishing a curriculum cleanly** — a capstone that ties the roadmap off without scope-creeping v1.

## Read first

- `../../architecture.md#deferred-design-goals-to-solve-later` — the three deferred goals in priority
  order: single-table reload while streaming, multi-pod loader sharding (fencing token), and (nearest-term)
  parallel CTID-range backfill.
- `../../architecture.md#17-snapshot--backfill-bootstrap` — step 3 / Open Q9: CTID-range parallel `COPY`
  under the single already-exported snapshot (no new slot/epoch/ownership needed).
- `../../architecture.md#18-single-slot-for-life--total-restart` — why the **sink** stays a single consumer
  regardless; horizontal scale is a **loader-only** story.
- `../../walrus-loader.md` §8.1–§8.2 (loader bootstrap / lease) — where the fencing token from PR 3.1 lives.

## Scope

**In scope**

- A `deploy/` or `docs/` design note (`docs/deferred-goals.md` or a `deploy/README`) capturing the three
  deferred goals, their likely shapes, and **exactly which module/seam** each will extend — cross-linked to
  the design sections.
- **CTID-range snapshot seam:** a documented, `#[allow(dead_code)]` (or feature-gated `ctid_parallel`)
  extension point on the backfill path (PR 2.29) showing where a per-table CTID-range plan would fan out
  concurrent `COPY`s under the existing exported snapshot — inert, no default behaviour change.
- **Loader-sharding seam:** confirm the `fencing_token` in `walrus.table_ownership` (PR 3.1) is read and
  carried but unused for routing today; a doc paragraph + a `TableAssignment` placeholder describing the
  future consistent-hash ownership split.
- A regression check: the whole workspace and the e2e suite stay green — nothing here changes runtime
  behaviour.

**Explicitly deferred** (this PR *documents* the deferral; it does not implement any of them)

- Actual parallel CTID `COPY` execution, actual multi-pod ownership/resharding, and single-table reload —
  all remain future work with the seams this PR marks.

## Files to create / modify

```
docs/deferred-goals.md                   # new — the three goals, shapes, and exact seams (cross-linked)
crates/pg-sink/src/backfill.rs           # modify — documented CTID-range fan-out seam (inert / feature-gated)
crates/loader/src/ownership.rs           # modify — TableAssignment placeholder; fencing_token carried, unused
docs/implementation/README.md            # modify — tick 4.11 / note the curriculum is complete
# no new runtime deps
```

## Skeleton

```rust
// crates/pg-sink/src/backfill.rs  (inert seam — documented, not wired)

/// DEFERRED (architecture.md "Deferred design goals" #3 / §1.7 step 3, Open Q9):
/// parallel CTID-range snapshotting under the SINGLE already-exported snapshot.
/// Needs no new slot/epoch/ownership — only concurrent COPY of disjoint CTID ranges.
/// This is the seam; v1 runs a single COPY per table.
#[allow(dead_code)]
struct CtidRangePlan {
    table: String,
    ranges: Vec<(/* start ctid */ u64, /* end ctid */ u64)>,
}
#[allow(dead_code)]
fn plan_ctid_ranges(/* table stats */) -> CtidRangePlan { unimplemented!("deferred goal — see docs/deferred-goals.md") }
```

```rust
// crates/loader/src/ownership.rs  (forward-compat hook — inert today)

/// DEFERRED (architecture.md "Deferred design goals" #2): multi-pod loader table-sharding.
/// Today ONE loader owns ALL tables; `fencing_token` is minted (PR 3.1) and carried but not
/// used for routing. This placeholder marks where consistent-hash ownership will slot in.
#[allow(dead_code)]
struct TableAssignment {
    table: String,
    owner_replica: Option<String>, // always None (single active loader) until sharding lands
    fencing_token: u64,            // inert forward-compat hook from PR 3.1
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `docs/deferred-goals.md` documents all three deferred goals (single-table reload, loader sharding,
      CTID-range backfill) with their likely shape and the **exact seam** each extends, cross-linked to the
      design sections.
- [x] The CTID-range and loader-sharding seams exist as clearly-marked, **inert** extension points
      (`#[allow(dead_code)]` / feature-gated / `unimplemented!` with a pointer) — they change **no** default
      runtime behaviour.
- [x] The PR 3.1 `fencing_token` is confirmed present, carried, and unused for routing today (single active
      loader); the placeholder documents its future use.
- [x] The README roadmap marks PR 4.11 complete and notes the curriculum is finished.
- [x] **No regressions:** the full workspace and the e2e suite stay green.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings` (inert seams must not trip dead-code /
        unused lints — annotate them)
  - [x] `cargo test --workspace`
  - [x] `docker compose up --wait` then `cargo test -p e2e --features it -- --ignored` still passes end to
        end (no behaviour changed).

## Hints & gotchas

- The line to hold: these are **deferred goals, not non-goals**. Document them as "planned, sequenced
  later," and do not let a reviewer talk you into a half-implementation — a marked seam beats dead code.
- `#![deny(warnings)]` will flag your inert structs as dead code — annotate with `#[allow(dead_code)]` (or
  put them behind a non-default feature) so the workspace stays warning-clean.
- The **sink stays single-consumer** no matter what — horizontal scale is a *loader* story only. Do not add
  a sink-sharding seam; that would contradict the single-slot-for-life invariant (§1.8).
- CTID-range backfill is the **nearest-term** goal because it needs no new slot/epoch/ownership — say so in
  the doc, so a future contributor picks the cheapest win first.
- This is the finish line: verify the roadmap's checkboxes and cross-links all resolve, and that the e2e
  suite you built in 4.1–4.6 still passes unchanged.

## References

- Design: `../../architecture.md#deferred-design-goals-to-solve-later`, `#17-snapshot--backfill-bootstrap`,
  `#18-single-slot-for-life--total-restart`; `../../walrus-loader.md` §8.1–§8.2.
- Prev: [PR 4.10](./pr-4.10-observability-metrics.md) · Next: — (curriculum complete) ·
  [Roadmap](../README.md)
