# Trait seams for dependencies in Walrus

> **Status:** audited 2026-08-27 — **the two seams that pay already exist** (`pg_sink::batch::Clock`
> and `object_store::ObjectStore`), and this pass added the missing half of the second one: a store
> *failure* double. **No new trait, no new dependency, and no hand-written `ObjectStore` impl.**

## What the rule asks, and where Walrus already answers it

The rule wants dependencies behind traits so tests can inject doubles instead of standing up the real
external system. Walrus reaches that outcome through three mechanisms, in descending order of how
often it uses them.

**1. A purpose-built trait, where the dependency is *time*.** `crates/pg-sink/src/batch.rs:60`
declares `Clock`, sealed and statically dispatched (`C: Clock`), with exactly one production impl —
`SystemClock` — and a doc comment that says outright the trait exists *for the test seam*. Its double
is `FakeClock` (`crates/pg-sink/src/batch_test.rs:10`), a hand-advanced clock, and it is what makes
the `max_fill` cadence trigger testable without sleeping. `TableBatcher`, `BatchRouter`,
`StreamDemux`, and `Backfill` are all generic over `C`, so the seam reaches the whole batching path.

**2. The dependency's own trait, where one exists.** The object store is never a concrete client in
the sink: `ParquetSink` holds `Arc<dyn ObjectStore>` (`crates/pg-sink/src/sink.rs:49`) and the
loader's bootstrap takes `&dyn ObjectStore` (`crates/loader/src/bootstrap.rs:63`). Unit tests inject
`object_store::memory::InMemory`; the compose-gated tests inject `AmazonS3` against MinIO.
`crates/pg-sink/src/batch.rs:12-23` already records *which* seam is static and which is dynamic, and
why — so the dispatch shape here is a decision, not an accident.

**3. Extract the pure core, where a trait would only wrap a protocol.** `Heartbeat` owns a real
`tokio_postgres::Client`, but every idle/round-trip *decision* lives in `BeatState`
(`crates/pg-sink/src/heartbeat.rs:130`), a private struct with no I/O that takes `now` as a
parameter — "unit-tested directly", as its own doc says. `replication.rs` and `snapshot.rs` do the
same with free functions (`build_standby_status`, `take_message`, `error_message`, `parse_dsn`,
`begin_snapshot_txn`, `select_text_sql`). This is why 626 of the 739 `#[test]`/`#[tokio::test]`
attributes under `crates/` carry no `#[ignore]` — only 113 need `docker compose up --wait`.

## What this pass changed

`crates/pg-sink/src/sink_test.rs` gained
`a_store_failure_propagates_as_the_transient_store_class`. The `SinkError::Store` arm had no
coverage while its `Encode` sibling did, and the reason was structural: `InMemory::delete` is
remove-or-ignore and returns `Ok` even for a key that was never written
(`object_store-0.11.2/src/memory.rs:294`), so the happy-path fake cannot reach the failure branch.
`LocalFileSystem::delete` maps `ErrorKind::NotFound` to `Error::NotFound`
(`object_store-0.11.2/src/local.rs:491`), so injecting *that* implementation of the same trait drives
`ParquetSink::delete` into `SinkError::Store` and on to `common::Error::ObjectStore` — the transient
class. Nothing is written, so nothing is cleaned up.

That is the rule's "Testing Error Paths" section applied at the seam Walrus already had, with zero
new crates: the point being demonstrated is that a *trait* dependency admits a second implementation
where a concrete client would not.

## Why no hand-written `FailingStore`, and no mockall

`object_store::ObjectStore` is declared `#[async_trait]` (`object_store-0.11.2/src/lib.rs:580`) with
eight required methods, and the crate does not re-export the macro. A stub therefore needs a direct
`async-trait` dependency — which `deny.toml:73` denies outside four audited wrappers, on the
ADR-backed grounds that AFIT closed that gap at Rust 1.75 and Walrus's MSRV is 1.95 — plus a second
crate to name `list`'s `BoxStream`. Weakening a supply-chain ban and adding a proc-macro to the build
graph to reach the same assertion a stock implementation already reaches is the wrong trade. The
`mockall` verdict is the same and recorded separately: Walrus declares 9 traits, 3 are `Sealed` markers,
`Clock` is sealed (so `#[automock]` cannot produce a usable mock) and needs monotonic *state* rather
than call expectations, and `FailureClass`'s test exists to exercise the trait's **default** bodies,
which a generated mock would replace.

## Why the remaining concrete dependencies stay concrete

| Dependency | Site | Why no trait |
|---|---|---|
| `sqlx::PgPool` | `pg-sink/src/{bootstrap,reload,reload_export}.rs`, `loader/src/{epoch,lease,phase_a}.rs` | Queries are compile-time-verified against the offline cache. A repository trait erases that check and duplicates every statement in the double. |
| `duckdb::Connection` | `loader/src/duck.rs:50` | Embedded and in-process — the "real system" is already a fast local double, and the loader's transforms *are* the SQL it runs, so a fake would assert nothing. |
| `tokio_postgres::Client` | `pg-sink/src/{heartbeat,snapshot,bootstrap}.rs` | The pure decisions are already extracted (mechanism 3); what is left is the statement text, which the compose tests exist to prove. |
| `TcpStream` | `pg-sink/src/replication.rs:95` | The hand-rolled wire protocol *is* the contract; framing and parsing are already free functions with unit tests. Making the typestate struct generic over `AsyncRead + AsyncWrite` is the one open idea — see below. |

## What would reopen this

Two triggers. First, a second Walrus-owned, **unsealed** trait that needs ordered call expectations
rather than injected state — that is the case for reconsidering `mockall`. Second, a decode-loop bug
class that the free-function tests structurally cannot reach: `ReplicationStream<S>` could become
generic over `tokio::io::AsyncRead + AsyncWrite + Unpin`, letting `next()`'s tag dispatch, its
error-response `bail`, and its feedback-on-timeout path run against `tokio::io::duplex()` with no
live walsender. That is a real gain, but it re-shapes the most delicate module in the sink and
changes a public type, so it needs its own PR with the compose suite green — not a rule pass. Either
change amends this note.

## See also

- Rule: `.claude/skills/rust-skills/rules/test-mock-traits.md`
- The clock seam and its dispatch table: `crates/pg-sink/src/batch.rs:12-113`
- The `async-trait` ban and its re-open trigger: `deny.toml:61-73`,
  `docs/implementation/notes/rust-skills/macro-proc-syn-quote.md`
- Declined-dependency precedent: `docs/implementation/notes/rust-skills/conc-rayon-par-iter.md`
