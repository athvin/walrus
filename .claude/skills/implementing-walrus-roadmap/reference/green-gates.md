# Green gates

"Green" for every walrus task means **at least** the three baseline gates below pass, plus any
task-specific gates its Definition of Done lists. Run the same commands locally that CI runs, so
"green locally" reliably predicts "green in CI".

## Contents
- Baseline gates (every task, from PR 0.1)
- Gates that switch on as phases land
- The fix-loop
- Testing layers (cheapest first)

## Baseline gates (every task, from PR 0.1)

```
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

`scripts/green.sh` runs all three from the repo root and reports which failed. To auto-fix
formatting, run `cargo fmt` (no `--check`) then re-run the gate. The workspace is
`#![deny(warnings)]` via `[workspace.lints]`, so any warning fails clippy.

## Gates that switch on as phases land

Only run a gate once the code that needs it exists. A task's DoD tells you which apply; this table
(mirrors README "CI grows with the phases") says when each first appears:

| From PR | Added gate |
|---|---|
| 0.1 | `fmt --check`, `clippy --all-targets -D warnings`, `build --workspace`, `test --workspace` |
| 0.6 | compose: `docker compose up --wait` → smoke assertion → `docker compose down` |
| 1.3 | integration vs compose (control PG); sqlx offline: `cargo sqlx prepare --check` |
| 2.11 | DuckDB conformance job (feature-gated, e.g. `--features conformance`) |
| 4.7 | `cargo-deny` (licenses / advisories / bans / sources) |
| 4.8–4.9 | image build; `kubeconform` / kind manifest validation |
| 4.1+ | full `tests/e2e` job (feature `it`) |

For compose-based gates, always `docker compose down` afterwards so the next task starts clean.

## The fix-loop

Run gate → read the failure → fix the code → re-run the **same** gate → repeat until it passes,
then move to the next gate. Never edit a gate command to make it pass. If clippy flags a lint the
task doesn't intend to address, fix the code — don't `#[allow]` it away unless the task's Hints say
to. Bound the loop: if the **same** gate still fails after **3** fix attempts, stop and ask the human
(surface the gate output) rather than spinning.

## Testing layers (prefer the cheapest that proves the thing)

1. **Pure unit** (ms, no Docker): `Lsn`, `SinkMeta`, the pgoutput decoder, the loader transform on
   an in-memory DuckDB. The two hardest correctness stories live here.
2. **Conformance** (feature-gated): write Parquet → read back with in-process DuckDB; assert both
   the inferred type and the value.
3. **Integration** (`docker compose up --wait`): a crate's `tests/` against a real Postgres / MinIO.
4. **End-to-end** (feature `it`, `tests/e2e/`): both services wired together against the compose stack.
