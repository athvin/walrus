# Interior mutability across threads: the `Mutex` census

> **Status:** audited — the rule was already satisfied everywhere it applies. One test fake moved
> from `std::sync::Mutex` to `parking_lot::Mutex`; no production code and no manifest changed.

## What the rule asks

Three things, in order of weight:

1. Shared mutable state **across threads** belongs in a `Mutex<T>`, not a `RefCell<T>` — `RefCell`
   is `!Sync`, so it cannot even be shared.
2. Poisoning must be handled deliberately rather than papered over.
3. Prefer `parking_lot::Mutex`: no poisoning, `lock()` returns the guard directly, smaller and
   faster under contention.

## Census: every interior-mutability site in the tree

Fields, not line numbers — a line drifts, a field name does not.

| Site | Type | Reached from >1 thread? | Verdict |
|---|---|---|---|
| `WatermarkWaiters::waiters` (`crates/pg-sink/src/reload_signal.rs`) | `parking_lot::Mutex<HashMap<…>>` | Yes — the decode loop resolves, exporter tasks subscribe | Already the rule's answer |
| `LoaderState::last_poll_completed_at` (`crates/loader/src/health.rs`) | `parking_lot::Mutex<Option<Instant>>` | Yes — apply workers stamp, the axum probe reads | Already the rule's answer |
| `TableDb::parquet_cols` (`crates/loader/src/duck.rs`) | `RefCell<HashMap<…>>` | **No, and must stay that way** | Must NOT become a lock — see below |
| `TableCtx::pause_logged`, `TableCtx::resync_ids` (`crates/loader/src/phase_a.rs`) | `Cell` / `RefCell` | No — one `TableCtx` per worker, `spawn_local`'d | Correct as-is |
| `FakeClock::offset` (`crates/pg-sink/src/batch_test.rs`) | was `std::sync::Mutex<Duration>` | Yes by type — `Clock: Send + Sync`, held behind `Arc` | **Converted to `parking_lot::Mutex`** |
| `BufWriter`'s buffer (`crates/common/src/telemetry_test.rs`) | `Arc<Mutex<Vec<u8>>>` (std) | No — `with_default` installs a thread-local subscriber | Kept on std, see below |
| `AtomicPhase` and friends (both `health.rs` files, `WatermarkWaiters`' two counters, `reload.rs`'s `current_reload_id`) | `Atomic*` | Yes | Single independent cells; a lock would only add cost (`conc-atomic-ordering`) |
| `SOURCE_LOCK` / `LOCK` statics (the `crates/*/tests` integration suites) | `tokio::sync::Mutex<()>` | Yes | Held across `.await` on purpose — a sync mutex is the wrong tool there |

There is no `Rc<RefCell<…>>`, no `UnsafeCell`, and no `static mut` anywhere: `unsafe_code =
"forbid"` rules out the last, and `crates/common/tests/storage_class_guard.rs` bans mutable globals
outright.

## What changed

`FakeClock` — the hand-advanced clock behind `batch_test.rs`'s `max_fill` cases — was the tree's
only place where genuinely shared, cross-thread interior mutability used `std::sync::Mutex`. It is
the rule's exact scenario (`Clock: Send + Sync`, every owner holds its clock behind an `Arc`, so a
`Cell` would not compile), and it was paying for that with two `.lock().unwrap()` poison checks.
`pg-sink` already depends on `parking_lot`, so the swap costs nothing and removes both unwraps.

## What did not change, and why

**The two `RefCell`s in the loader are load-bearing.** `TableDb` is `Send + !Sync` *by
construction*: that is what makes a future holding `&TableCtx` non-`Send`, which is what forces one
apply worker per `.duckdb` file on a `LocalSet` — the architecture that keeps DuckDB's single-writer
file lock honest. Two `compile_fail,E0277` doctests on `TableDb` pin the property. Replacing either
`RefCell` with a `Mutex` would make the type `Sync`, break those guards, and quietly license the
shared access the design forbids. `TableCtx`'s `Cell`/`RefCell` sit inside that same single-threaded
fence. This is the rule's own `RefCell` row — *single-threaded, minimal overhead* — not a violation
of it.

**`common`'s telemetry `BufWriter` stays on `std::sync::Mutex`.** Converting it would mean adding
`parking_lot` to the foundational crate's dev-dependencies, and the rule's argument for
`parking_lot` is contention performance. There is no contention: `tracing::subscriber::with_default`
installs a *thread-local* subscriber, so one test thread writes the buffer through its clones. A
poisoned lock there means the test body already panicked, and an `unwrap()` that reports a panic as
a panic is the right loud failure for a test. A new dependency edge on `common` is not worth two
lines.

**No `RwLock` appears anywhere**, which the sibling audit measured separately: see
[`own-rwlock-readers.md`](./own-rwlock-readers.md) for why both production locks are
write-dominated and stay mutexes.

## Poisoning, in practice

Production has **zero** `std::sync::Mutex`, so there is no production poison path to handle at all.
That is not an accident: `clippy::unwrap_used` and `clippy::expect_used` are denied workspace-wide
(PR 7.7), and `std`'s `Result`-returning `lock()` invites exactly those calls, so PR 7.6 made
`parking_lot` a direct dependency to keep the locking API and the unwrap policy aligned.
`scripts/check-lock-choice.sh` then requires a `// LOCK-CHOICE:` justification above every
`Mutex`/`RwLock` **field** under `crates/*/src`, excluding sibling `*_test.rs` files — which is why
`FakeClock::offset` needs no such comment and carries its rationale in a doc comment instead.

## When to revisit

A new shared-mutable field should reach for `parking_lot::Mutex` and say why on its `LOCK-CHOICE`
line. Reach for something else only with a reason this note can absorb: `tokio::sync::Mutex` if the
guard must cross an `.await` (none does today — see
`crates/common/tests/workspace_lints_inherited.rs` for the lint that watches), an `Atomic*` if the
state is one independent cell, or `RwLock` only against the measurement bar
[`own-rwlock-readers.md`](./own-rwlock-readers.md) sets. Converting either loader `RefCell` needs a
redesign of the `LocalSet` ownership model first, not a type swap.
