---
name: sink-backfill-must-not-race-sigterm
description: The sink's first-bootstrap backfill must NOT be raced against the shutdown token — a mid-backfill abort silently loses pre-snapshot rows.
metadata:
  type: project
---

In `crates/pg-sink/src/main.rs::establish_stream`, the fresh-slot path creates the slot, bumps the
epoch, then backfills every published table before streaming. Do **not** wrap that backfill in a
`select!`/`timeout` against the shutdown token, even though every other long await in the sink races
it.

**Why:** the slot is created *before* the backfill runs. If SIGTERM aborted the backfill halfway, the
restart's `epoch::classify_slot` would find the slot present → `SlotAction::Resume` →
`current_or_new_epoch` reads the existing epoch → streaming resumes from `confirmed_flush` and the
backfill never re-runs. The un-copied tables' pre-snapshot rows are lost silently — there is no
"backfill incomplete" state to resume from.

**How to apply:** during async rule audits (cancellation-token, select-racing, timeout), treat the
`establish_stream` fresh-slot backfill as deliberately unraced and leave it alone. The loader's
`pipeline` bootstrap is safe to abort but is likewise left unraced — it is bounded and adding a
cancellation path would need a new `LoaderError` variant + exit code for no operational gain.
Related: [[loader-compose-tests-share-epoch]], [[dont-overbuild-verify-compose]].
