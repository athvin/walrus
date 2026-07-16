# PR 6.12 — e2e: quarantine recovery + N-table scale (phase close)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/104

> **Phase:** 6 — single-table reload · **Crates touched:** `tests/e2e`, docs ·
> **Est. size:** L · **Depends on:** PR 6.1–6.11 · **Unlocks:** — (phase close)

The phase ends where the design began: **quarantine recovery is the feature's customer**
([reload §2](../../single-table-reload.md#2-what-the-proposal-gets-right)). v1's only terminal
state — the quarantine after a lossy `ALTER COLUMN TYPE` — today has exactly one exit, a
total-restart of *every* table. This PR proves the new exit end to end: quarantine a table the
same way PR 3.9's tests do, `just reload` it, and watch it come back — while every other table
streams on undisturbed, because that is the single-slot promise the whole design defends. The
second e2e proves the "at scale" clause of the original ask: N tables reloading concurrently under
`max_concurrent_reloads`, none of them limited by replication-slot count, all of them correct. It
closes with the docs sweep that flips the deferred goal from "planned" to "built".

## Why — learning objectives

By the end of this PR you will have practised:

- **Testing the promise, not the mechanism** — the load-bearing assertion is *other tables never
  stalled*, measured as their `transformed_lsn` strictly advancing during the reload window.
- **Chaos-composing prior harnesses** — 3.9's quarantine trigger + 4.4's kill discipline + this
  phase's reload path, in one scenario.
- **Closing a feature loop in the docs** — the deferred-goals → design-doc → curriculum → shipped
  trail a future contributor will follow.

## Read first

- `../../single-table-reload.md` — §5 end to end (this PR is §5 as a test), §2 (the customer).
- PR 3.9's lossy-quarantine e2e machinery; PR 4.6 (total-restart — the old exit this PR
  obsoletes for single tables).
- `tests/e2e` harness conventions (feature `it`, compose stack, the mirror-vs-source diff
  helper).

## Scope

**In scope**

- **e2e 1 — quarantine recovery:** seed traffic across ≥ 3 tables; force the lossy
  `ALTER COLUMN TYPE` quarantine on one; `just reload` it; assert: quarantine cleared, mirror ==
  source (the diff helper returns empty), raw rebuilt at the new schema, **and the other tables'
  `transformed_lsn` strictly advanced during the reload window** (sampled before/during/after —
  the no-stall proof).
- **e2e 2 — N-table scale:** continuous writes on 3 tables; request reloads on all 3 with
  `max_concurrent_reloads=2`; assert: never more than 2 `exporting` at once (poll the state
  table through the run), all 3 reach `complete`, all 3 mirrors exact, slot count on the source
  == 1 throughout (`pg_replication_slots`).
- Docs sweep: `../../deferred-goals.md` §1 gains its "implemented in Phase 6" line;
  `../../architecture.md`'s deferred-goals section pointer updated;
  `../../single-table-reload.md` gets a one-line header note pointing at this phase's task files;
  tick the Phase 6 boxes in the roadmap README as the PRs merged.

**Explicitly deferred** (do *not* build these here)

- CTID-range parallel chunk SELECTs for very large tables → deferred goal §3 composition, still
  deferred.
- Loader sharding interplay (reload lease × ownership lease under `replicas>1`) → deferred goal
  §2's future PR; the design doc's §6 note stands.

## Files to create / modify

```
tests/e2e/…/reload_quarantine.rs      # new — e2e 1 (feature `it`)
tests/e2e/…/reload_scale.rs           # new — e2e 2 (feature `it`)
docs/deferred-goals.md                # modify — §1 implemented-in-Phase-6 line
docs/architecture.md                  # modify — deferred-goals pointer
docs/single-table-reload.md           # modify — header note → phase-6 curriculum
docs/implementation/README.md         # modify — tick the phase-6 boxes as merged
```

## Skeleton

```rust
// tests/e2e/…/reload_quarantine.rs  (feature = "it")

/// The anchor use case (reload §2): lossy ALTER ⇒ quarantine ⇒ `just reload` ⇒ recovered —
/// while every other table's transformed_lsn strictly advances through the whole window.
#[tokio::test]
async fn quarantined_table_recovers_via_reload_without_stalling_others() { todo!() }
```

```rust
// tests/e2e/…/reload_scale.rs  (feature = "it")

/// The "at scale" clause: 3 concurrent reloads under max_concurrent_reloads=2, one slot on the
/// source the entire time, all mirrors exact at the end.
#[tokio::test]
async fn n_table_reloads_respect_the_cap_on_one_slot() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] e2e 1 passes: quarantine exit through reload; mirror == source exactly; the new column type
      is live in DuckDB; every non-reloading table's `transformed_lsn` recovers strictly past its
      pre-reload sample. *(Refinement: a rebuild-flavor quarantine takes the whole loader down by
      design — PR 3.9 — so "no stall, not even at pickup" holds for the reload's application window,
      not across the quarantine crash; the other tables recover once the restarted loader catches
      them up. The strict no-stall-during-a-reload promise is proven in e2e 2, where the loader
      stays up. `restart_count ∈ {0,1}` tolerated per Hints.)*
- [x] e2e 2 passes: ≤ 2 `exporting` at every sample; 3/3 `complete`; 3/3 mirrors exact;
      `SELECT count(*) FROM pg_replication_slots` == 1 at every sample.
- [x] Both tests survive 3 consecutive runs (verified 3× each locally + green in the CI e2e job).
- [x] The docs sweep is complete — deferred-goals §1, architecture pointer, design-doc header
      note, roadmap ticks — and every link resolves.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test --workspace`
  - [x] `docker compose up --wait` then the full e2e job (feature `it`) including
        **`quarantined_table_recovers_via_reload_without_stalling_others`** and
        **`n_table_reloads_respect_the_cap_on_one_slot`** — a dedicated CI `e2e` job (disk headroom).

## What completed looks like

```
$ cargo test -p e2e --features it -- reload
running 2 tests
test quarantined_table_recovers_via_reload_without_stalling_others ... ok
test n_table_reloads_respect_the_cap_on_one_slot ... ok

$ psql $SOURCE_URL -c "SELECT count(*) FROM pg_replication_slots"
 count
-------
     1
```

One slot. N reloads. No stall. That transcript is the original ask, verbatim — and the roadmap's
Phase 6 table is all ✅.

## Hints & gotchas

- The no-stall assertion needs traffic: the other tables must have a steady writer during the
  reload window, or their `transformed_lsn` legitimately sits still and the test proves nothing.
  Reuse the throughput harness's generator (PR 5.6) at a low rate.
- Sample `exporting` counts on a tight poll (the state table is cheap to read); a cap breach is a
  race you want to *catch*, not average away.
- The quarantine e2e re-runs PR 3.9's lossy scenario verbatim up to the quarantine, then diverges
  — factor the shared setup rather than copy it, so 3.9's test and this one can't drift apart.
- Expect the reload to interleave with the lossy DDL's own `ddl_manifest` row: the restart-on-DDL
  path (6.8) may legitimately fire once during recovery if timing lands that way. The test must
  tolerate `restart_count ∈ {0, 1}` — assert the *outcome*, not the path.
- CI time: two e2e scenarios with real exports — keep seed sizes small (thousands, not millions);
  the chunk math is already proven at 6.5's scale.

## References

- Design: `../../single-table-reload.md` §2, §5 (all six steps land here);
  `../../architecture.md#18-single-slot-for-life--total-restart` (the old exit);
  `../../deferred-goals.md#1-single-table-reload--re-sync-while-streaming` (the goal this phase
  retires).
- Prev: [PR 6.11](./pr-6.11-reload-observability.md) · Next: — *(phase close)* ·
  [Roadmap](../README.md)
