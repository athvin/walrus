# No production OS threads in Walrus (PR 15.4)

> **Status:** decided 2026-08-13 — **the zero-production-OS-thread invariant is now a CI gate.**
> Walrus concurrency is Tokio task topology with explicit cancellation and joins. Thread creation
> requires an ownership/topology design, not an incidental `std::thread` call.

## The evidence

| Probe | Result |
|---|---|
| `scripts/check-no-os-threads.sh` creation pattern over non-test `crates/*/src` | 0 production thread-creation/import sites across 84 Rust files |
| `rg -n 'std::thread::available_parallelism' crates/*/src` | 2 allowed runtime-sizing queries |
| `rg -n 'std::thread::spawn' crates/*/src` | 1 sibling test, PR 14.18's `TableDb: Send` assertion |
| production `tokio::spawn(` probe | 12 task-spawn sites |
| production `CancellationToken` probe | 41 references |

The distinction is deliberate. `available_parallelism` observes host topology; it does not create a
thread. `duck_test.rs` moves a `TableDb` through one test thread to exercise a trait bound. The CI
guard controls non-test production topology and leaves both facts intact.

## What `thread::scope` guarantees, and where Walrus gets part of it

`std::thread::scope` provides two guarantees: every child thread joins before the scope returns,
including panic paths, and children may borrow non-`'static` stack data for the scope lifetime.

Walrus reproduces the lifetime-management goal with several explicit async pieces rather than with
`tokio::spawn` alone:

- the reload controller derives a `child_token()` for each exporter, moves its
  `OwnedSemaphorePermit` into the task, owns exporters in a `JoinSet`, and drains or aborts-and-joins
  every entry before controller exit;
- loader apply handles are awaited during the supervisor's sequential drain;
- both health-server handles are awaited after their `CancellationToken` drives graceful shutdown;
- pipeline drop guards cancel shared tokens on success, error, or unwind.

Cancellation is not joining, and `tokio::spawn` by itself is detached. The owned `JoinSet`/handles
provide completion; the tokens request shutdown; RAII-owned permits release capacity on every exit.
Together those pieces form the current structured task topology.

Async does not reproduce scoped threads' borrowing guarantee. A `tokio::spawn` future must be
`'static`, so tasks move owned values and share longer-lived state through `Arc`. PR 15.1's
`Arc<AtomicI64>` reload-id handoff is a concrete example. This heap ownership is the cost of tasks
that may outlive the stack frame that created their future.

## The DuckDB boundary is `Send + !Sync`

PR 14.18 pins the exact type facts: `duckdb::Connection` and loader `TableDb` are `Send` but not
`Sync`. Owned movement across threads is legal and is proven in `duck_test.rs`; a shared borrow is
not `Send`, so apply-loop futures holding `&TableCtx` across awaits stay on the loader's `LocalSet`.

This is not proof that Walrus can never use an OS thread. It means doing so requires a real ownership
redesign: move each owned database out of its context, recover it on all exits, preserve
`InterruptHandle` cancellation, and maintain ordered lease release. PR 14.18 records that open
cross-table-stall finding. The guard ensures such a topology cannot arrive as an unreviewed spawn.

## Scope of the guard

`scripts/check-no-os-threads.sh` scans `crates/*/src/**/*.rs` while excluding sibling
`*_test.rs`. It rejects direct `std::thread::spawn`, `scope`, and `Builder` paths plus module imports
that could hide them. It intentionally permits direct `std::thread::available_parallelism` calls.

Unit and integration tests and benches are outside the invariant. They may create threads to assert
`Send` bounds, exercise interleavings, or measure thread-sensitive behavior. The script's isolated
self-test proves both allowed cases and both rejected syntactic forms without editing tracked Rust.

## What would reopen this

Reopen production OS threads only for a measured dominant stage or isolation requirement whose data
has an explicit owned `Send` boundary. For the known loader stall, an acceptable proposal must compare
move-and-recover blocking jobs with one dedicated current-thread runtime per database, demonstrate
independent table progress, preserve interrupt-driven drain and lease ordering, and update this guard
and note in the same change.

## See also

- Rule: `.claude/skills/rust-skills/rules/conc-scoped-threads.md`
- Blocking ownership analysis: `docs/implementation/notes/rust-skills/async-spawn-blocking.md`
- Sibling Rayon decision: `docs/implementation/notes/rust-skills/conc-rayon-par-iter.md`
