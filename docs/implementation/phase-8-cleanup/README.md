# Phase 8 — Codebase cleanup

A post-hardening **cleanup audit** of the finished v1 + reload codebase, written as
executable task files like every other phase. No new behaviour — every PR is a
behaviour-preserving refactor that stays green.

## The honest headline

The codebase is **genuinely strong**. Centralized `[workspace.lints]` deny `warnings`,
`clippy::all`, and (in production) `unwrap_used`/`expect_used`; a real `thiserror`
terminal-vs-transient error taxonomy with `anyhow` only at binary edges; exhaustive matches
(no lazy `_` arms where they'd hide drift); a ~51% test-to-code ratio with Go-style sibling
`*_test.rs`; only two TODO comments in the tree (both *retired*-note breadcrumbs, not open
work); and a blessed newtype precedent in `Lsn`. An automated quality pass over this tree
can plausibly report "zero issues."

That verdict is too lenient — there **are** real cleanups — but they are **DRY and
type-modeling refinements, not bugs or architectural faults**. Nothing here is on fire.
The tiers below rank *cleanup value*, not severity of any defect.

## The findings

| PR | Finding | Tier | Size | Anchor(s) |
|---|---|---|---|---|
| [8.1](./pr-8.1-sql-literal-helper.md) | SQL single-quote escaping hand-rolled in 6 production spots (2 identical) → one `common::sql::sql_literal` | 1 · consistency/auditability | S | `loader/src/duck.rs:132,162,176`; `loader/src/ddl.rs:232`; `pg-sink/src/reload_export.rs:578`; `pg-sink/src/preflight.rs:438` |
| [8.2](./pr-8.2-manifest-kind-status-enums.md) | `ManifestRow.kind`/`.status` are `String`; loader compares raw strings — and the doc comment already **omits `'spill'`** that the code reads. Model as enums. | 1 · type modeling | M | `control/src/manifest.rs:27,32`; `loader/src/phase_a.rs:134,149,190` |
| [8.3](./pr-8.3-centralize-pg-oids.md) | Postgres OIDs named once in `pg-to-arrow/oids.rs` but re-hardcoded as bare literals in 4 places. One home in `common::oids`. | 2 · DRY | M | `loader/src/duck.rs:381`; `loader/src/plan.rs:205-206`; `loader/src/ddl.rs:86-87`; `common/src/pg_shape.rs:15` |
| [8.4](./pr-8.4-domain-id-newtypes.md) | `epoch`/`schema_version`/`reload_id`/manifest `id` flow as bare `i64` (40+ params in `control` alone); freely swappable. Newtypes, extending the `Lsn` pattern. | 3 · defense-in-depth · **opt-in** | L | `control/src/*.rs`, `loader/src/*.rs` |
| [8.5](./pr-8.5-nits-cluster.md) | Nits: `pause_began` is `pub` but test-only; plan tier is implicit; `Clock` single-impl trait (documented **keep**). | 4 · nits | S | `loader/src/phase_a.rs:52`; `loader/src/plan.rs`; `pg-sink/src/batch.rs:20` |

## Suggested order

Quick, high-signal wins first; the big opt-in refactor last:

**8.1 → 8.2 → 8.3 → 8.5**, then **8.4** (opt-in, splittable per-type: 8.4a `EpochNo`,
8.4b `SchemaVersion`, …). Each PR is independent — none blocks another — so you can also
cherry-pick. 8.5 is the phase closer.

## Rejected / down-ranked (recorded so they aren't silently re-discovered)

- **`LoaderError::as_common()` "can silently drift" — REJECTED.** A design pass flagged that
  adding a `LoaderError` variant might desync the mapping to `common::Error`. Read
  `crates/loader/src/error.rs:39-59`: the `match` is **exhaustive with no `_` arm**, so a new
  variant is a *compile error* until it's handled. The compiler already is the guarantee.
  Not a finding.
- **"Zero issues / production-ready" — down-ranked, not accepted.** The pure quality lens
  under-counts (the DRY/type items above are real); the design lens over-reached (the
  `as_common` item). The truth is stated honestly per-finding, not at either extreme.
- **`Clock` single-impl trait — KEPT by design** (tracked in 8.5). It's a legitimate test
  seam (inject time, don't sleep), not dead generality. The audit result is a one-line doc
  note, not a removal.

## Definition of done (phase)

- All five task files exist, each with a complete *Definition of Done* and resolving
  Prev/Next/Phase/Roadmap links.
- Every anchor `file:line` above matches the current tree (re-run the greps in each task's
  *What completed looks like* before starting it — the tree moves).
- The roadmap [`../README.md`](../README.md) lists Phase 8 and its counts reconcile
  (`102 PRs across 9 phases`).

---

- Prev phase: [Phase 7 — conventions hardening](../phase-7-conventions-hardening/) ·
  [Roadmap](../README.md)
