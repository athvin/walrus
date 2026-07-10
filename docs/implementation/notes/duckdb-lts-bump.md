# DuckDB LTS evaluation (PR 5.9)

> **Status:** evaluated — no bump to make. The pinned stack already tracks the **latest** published
> `duckdb-rs`, a full minor line **past** the DuckDB 1.4.x LTS this task was written against.

## What the task expected

PR 5.9's brief (and `architecture.md` Open Q4(b)) assumed walrus was pinned to **DuckDB 1.4.x LTS**
(community support EOL **2026-09-16**) and asked this PR to "identify the next LTS and the `duckdb-rs`
release tracking it, attempt the lockstep bump, and either land it (conformance green) or document the
blocker." That premise is stale — the repo moved past 1.4.x before Phase 5 even began.

## What is actually pinned

`Cargo.toml` pins the exact trio (unchanged here):

```
duckdb  = "=1.10504.0"
arrow   = "=54.3.1"
parquet = "=54.3.1"
```

`duckdb-rs` adopted a new version scheme at DuckDB v1.5.0 (from its own README:
"Starting with DuckDB `v1.5.0`, the duckdb-rs version encodes the DuckDB version in its second semver
component"). Its `crate_version_to_duckdb_version` decoder (`libduckdb-sys/upgrade.sh`) is:

```
ENCODED = second semver component        # 1.10504.0 -> 10504
major   = ENCODED / 10000                #  = 1
minor   = (ENCODED / 100) % 100          #  = 5
patch   = ENCODED % 100                  #  = 4
```

So **`duckdb = "=1.10504.0"` bundles DuckDB engine `v1.5.4`** — set in PR 2.16 (uuid), alongside the
exact `arrow`/`parquet` pins that the `arrow.uuid` → Parquet-UUID annotation depends on
(`walrus-pg-sink.md` §2.4). The project has been on the 1.5.x line since PR 2.16; it was never on
1.4.x in committed history.

## The evaluation result

- **`1.10504.0` is the newest published `duckdb-rs`** (`cargo info duckdb` → `version: 1.10504.0`;
  the previous release is `1.10501.0` = engine `v1.5.1`). There is no newer release to bump *to*.
- The **1.4.x LTS EOL (2026-09-16)** the task worried about is therefore **moot** — walrus is already
  a minor line ahead of it, not running against that clock.
- The conformance suite (`crates/pg-to-arrow/tests/conformance.rs` — UUID annotation, decimal,
  temporal MICROS, nested types), built in PR 2.11 as the read-back safety net for exactly this kind
  of engine move, is **green** on `v1.5.4` (it gates every CI run on `main`).

Both branches of the DoD ("land the bump" / "document the blocker") assume there *is* a bump to make.
Here there is neither a bump nor a blocker: the stack is already current. This note is the explicit
record the DoD asks for.

## Go-forward clock

- The exact trio moves only in **lockstep** and only when a newer `duckdb-rs` ships **and** the
  conformance suite stays green — never loosened to ranges (the pins exist for UUID-annotation
  stability; `arrow`/`parquet` never move independently of `duckdb`).
- Re-evaluate when DuckDB designates the **next LTS beyond 1.5** and a `duckdb-rs` release tracks it;
  at that point redo this evaluation (bump `duckdb` alone first; touch `arrow`/`parquet` only if the
  new `duckdb-rs` forces the interop version, per the task's "one axis at a time" rule).
- If a future `duckdb-rs` bump makes the UUID conformance assertion fail, that is the pin rationale
  firing as designed — record it here, do **not** ignore-list it.
