# No broadcast channel (PR 14.17)

> **Status:** decided — `tokio::sync::broadcast` is not used in Walrus, and its constructor is
> listed in `clippy.toml`'s `disallowed-methods`. This is a documented deviation from the
> `async-broadcast-pubsub` rule, based on the measurements below.

## What the rule asks for

The rule recommends `broadcast` when independent subscribers must each receive every message, such
as an event bus or real-time notification stream. Its history-preserving fan-out semantics are the
reason to choose it over a single-consumer queue or latest-value state channel.

## What the tree actually contains

The current production tree has no broadcast API reference:

```console
$ grep -rn --include='*.rs' 'tokio::sync::broadcast\|broadcast::' crates tests | wc -l
0
```

The single plain-text `broadcast` match in those paths is a PR 14.16 documentation comment describing
the epoch value as broadcast by its shared `watch` poller; it is not an API use. Shutdown instead uses
`CancellationToken` throughout both binaries:

```console
$ grep -rn 'CancellationToken' crates/*/src | grep -v '_test\.rs' | wc -l
41
```

Nine production `tokio::select!` arms directly await cancellation:

```console
$ rg -n '(?:_|\(\)) = [A-Za-z_][A-Za-z0-9_]*\.cancelled\(\)' crates/*/src --glob '!**/*_test.rs' | wc -l
9
```

The token topology has one explicit hierarchy edge:

```console
$ grep -rn 'child_token()' crates/*/src | grep -v '_test\.rs' | wc -l
1
```

That edge is `ReloadController::spawn_exporter` in `crates/pg-sink/src/reload.rs`: an exporter gets a
child token so its subtree can be cancelled without cancelling the pod. Both binaries also use a
cancellation-token `DropGuard` around their fallible pipelines, so early return and unwind initiate
the same drain as a signal.

Neither data path is a pub/sub event stream. The replication socket is borrowed by exactly one
`DecodeLoop::run` invocation, constructed in `crates/pg-sink/src/main.rs`; there is no second decoder
or event tap. The loader constructs exactly one `TableCtx` and one `spawn_local` apply loop per owned
`.duckdb` file in `crates/loader/src/main.rs`, preserving DuckDB's single-writer model.

## Why `CancellationToken` beats `broadcast` for the one fan-out we have

Using a broadcast channel for shutdown would impose three concrete costs:

1. Its payload must implement `Clone`, even though shutdown is a payload-free edge and could only
   send `()`.
2. It requires a ring-buffer capacity and a `RecvError::Lagged(n)` policy in every observer. A stop
   signal cannot meaningfully lag: cancellation is level-triggered and remains observable forever.
3. It provides no synchronous `is_cancelled()` check, child-token hierarchy, or RAII `DropGuard`.
   The apply loop and compaction use synchronous checks, while reload uses a child subtree and both
   binaries rely on the guard during teardown.

The signal-handler tasks also keep swallowing subsequent SIGTERM/SIGINT notifications after the
token is cancelled. That ordered, idempotent drain behavior does not need a queued message per signal.

## The comparison, for the record

| Property | broadcast | watch | mpsc | CancellationToken |
|---|---|---|---|---|
| Receivers | Multiple | Multiple | One | Multiple, hierarchical |
| Delivery | Every retained event | Latest value only | Every queued item to one receiver | One permanent cancellation edge |
| Slow observer | Misses overwritten events and reports `Lagged(n)` | Skips intermediate values | Backpressures sender when bounded | Observes the same cancelled state later |
| Payload `Clone` required | Yes | No | No | No payload |
| Walrus use | None; guarded by `clippy.toml` | Epoch fan-out, PR 14.16 | Bounded worker-failure queue, PR 14.14 | Shutdown and scoped task cancellation |

Walrus did not reject channels wholesale. In addition to bounded `mpsc` and `watch`, it uses
`oneshot` for `reload_signal` echo waiters; PR 14.5 tightened that existing request/response path.
Each adopted primitive matches its delivery semantics.

## What would reverse this

This decision reverses only when the decoded replication event stream gains a second in-process
consumer that must see every `ReplicationMessage` independently of the sink writer without adding
latency to that writer—for example, an audit/tap component. At that point the implementing PR must
choose an explicit capacity and `Lagged` policy, amend this ADR, and remove the `clippy.toml` entry.

A second pod does not reverse the decision: it is another process, covered by the deferred sharding
design in `docs/architecture.md` §1.8 and `docs/walrus-loader.md` §9.5, not another in-process
subscriber.

## Escape hatch

`#[allow(clippy::disallowed_methods)]` still compiles. That is intentional: the lint is a speed bump
that routes a prospective use to this decision, not a permanent wall. A reviewer should reject the
allow unless the same change satisfies the reversal rule and updates this ADR.
