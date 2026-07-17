<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.8 — Identifier convention + naming audit (phase close)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/114

> **Phase:** 7 — conventions hardening · **Crates touched:** docs (read-only audit over `crates/**`) ·
> **Est. size:** S · **Depends on:** PR 7.7 · **Unlocks:** — (phase close)

The phase closes by writing down the naming rule and proving the codebase already follows it. The
prompt that started this phase flagged `"first_lsn: Lsn"` as a bad column name — but it is sqlx's
compile-time **type-cast alias** (`SELECT first_lsn AS "first_lsn: Lsn"`), not a rename; the real
column is plain `first_lsn`. This PR records that distinction as a Conventions **Identifiers** row and
runs a small grep audit whose empty result is the proof: no walrus-authored identifier carries a
space, colon, or capital. It's a documentation + audit PR — no code changes — and it ticks the Phase 7
boxes.

## Why — learning objectives

By the end of this PR you will have practised:

- **Reading sqlx's `AS "col: Type"` syntax** — a type-cast the `query!` macro consumes at compile
  time, distinct from a SQL column alias.
- **Auditing by disproof** — writing the grep that *would* find a violation, and showing it finds
  none, as a durable, re-runnable guarantee.
- **Closing a phase in the docs** — the Conventions table is the deliverable; the roadmap boxes flip.

## Read first

- `crates/control/src/reload.rs` (and `checkpoint.rs`) — the `AS "…: Lsn"` casts, the audit's anchor.
- `crates/pg-to-arrow/src/{tier2,geometric,schema}.rs` — the source-mirror expansion columns
  (`{name}_months`, `{name}_lower`, …) and `SINK_META_COLUMN` (`walrus_pg_sink_meta`).
- `migrations/{control,source}/*.sql` — the persisted columns, all clean `lower_snake_case`.

## Scope

**In scope**

- Add the README Conventions **Identifiers** row (place under **Time**): walrus-authored columns are
  `lower_snake_case` (`_walrus_op`, `_walrus_commit_lsn`, `_walrus_lsn`, `_walrus_sink_processed_at`,
  `_applied_commit_lsn`, `_applied_lsn`, `walrus_pg_sink_meta`); source-derived DuckDB/Arrow columns
  are quoted **only** to mirror the source name faithfully (`{col}_months`, `{col}_lower`, …), never
  invented case-sensitive/spaced names of walrus's own; sqlx `AS "col: Type"` is a compile-time
  type-cast, not a rename.
- Run the audit greps and record them (with expected output) in *What completed looks like*.
- Phase close: tick the Phase 7 README boxes as the PRs merged; bump the intro PR/phase counts to
  `97 PRs across 8 phases` (line 19) and `97 PRs.` (line 148).

**Explicitly deferred** (do *not* build these here)

- Any code change — the sqlx casts are correct and required; removing them would break `Lsn`/enum
  decoding. This PR is docs + audit only.
- Touching `docs/architecture.md` — verified it does **not** reference the Conventions table; no sync
  needed.

## Files to create / modify

```
docs/implementation/README.md   # modify — add Conventions "Identifiers" row; tick Phase 7 boxes; bump counts (lines 19, 148)
```

## Skeleton

```markdown
<!-- README Conventions table — new row under "Time" -->
| Identifiers | walrus-authored columns are `lower_snake_case` (`_walrus_op`, `_walrus_commit_lsn`,
`_walrus_lsn`, `_walrus_sink_processed_at`, `_applied_commit_lsn`, `_applied_lsn`,
`walrus_pg_sink_meta`); source-derived DuckDB/Arrow columns are quoted only to mirror the source name
faithfully (`{col}_months`, `{col}_lower`, …); sqlx `AS "col: Type"` is a compile-time type-cast, not
a rename. |
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] The Conventions **Identifiers** row is added and reads cleanly in the table.
- [x] The audit is recorded and reproducible: the `AS "x: Type"` cast bucket resolves to sqlx casts
      only (17, all in `crates/control/sql/postgres/queries/*.sql` — PR 7.4 moved them there); the
      `{name}_…` bucket resolves to source-mirror expansion fields only; the disproving grep for quoted
      identifiers with a space/colon/uppercase that are *neither* returns **no** persisted
      walrus-authored column.
- [x] The `first_lsn: Lsn` false alarm is documented (one line: it's a cast alias, not a column).
- [x] Phase close: all Phase 7 roadmap boxes are ✅; the intro counts read `97 PRs across 8 phases`
      and `97 PRs.`; every Phase 7 link resolves.
- [x] **Green locally and in CI:** (docs-only — the standard gates still run)
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test --workspace`

## What completed looks like

```
$ # 1. the sqlx type-cast bucket — now in the .sql files (PR 7.4), none are column renames
$ grep -rEn 'AS "[a-z_]+: ' crates/control/sql/postgres/queries/*.sql | wc -l
17
$ # 2. the source-mirror bucket — expansion fields, lower_snake_case
$ grep -rEn 'format!\("\{name\}_' crates/pg-to-arrow/src | head
…_months …_days …_micros …_lower …_upper …

$ # 3. the disproof — a walrus-authored identifier with a space/colon/uppercase that is neither
$ grep -rEn '"[A-Za-z_][A-Za-z0-9_ ]*[A-Z ][A-Za-z0-9_ ]*"' crates/*/src \
    | grep -v ': ' | grep -iE 'create |alter | as "'
(no persisted-column matches)
```

Three buckets, one of them empty — the empty one is the guarantee. Phase 7 is all ✅.

## Hints & gotchas

- The audit's value is the **disproving** grep (#3): it's phrased to catch a real violation, and its
  emptiness is the durable claim. Keep it in the PR body so a future contributor can re-run it.
- Leading-underscore quoted idents (`"_walrus_meta"` in `duck.rs`) are quoted defensively (a leading
  `_` is legal unquoted anyway) — classify them as walrus-internal `lower_snake_case`, allowed.
- Don't "fix" the `AS "col: Type"` casts — they are load-bearing type overrides for the `Lsn` newtype
  and the reload enums; removing the quotes breaks compilation.
- This is the phase closer: after it merges, `next-task.sh` reports `DONE` again.

## References

- Design: `docs/implementation/README.md` "Conventions" (the Identifiers row this PR adds).
- Prev: [PR 7.7](./pr-7.7-deny-unwrap-expect-lint.md) · Next: — *(phase close)* ·
  [Roadmap](../README.md)
