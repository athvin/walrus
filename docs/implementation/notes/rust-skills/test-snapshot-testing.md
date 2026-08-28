# Snapshot testing in Walrus (insta)

> **Status:** audited 2026-08-27 — **not adopted, shortlist recorded.** The tree has **0** `.snap`
> files and no `insta` dependency, and across the **740** bare `#[test]` / `#[tokio::test]` attributes
> under `crates/` (plus 17 parameterised `#[tokio::test(…)]` in `tests/e2e`) there are exactly **two**
> hand-maintained rendered-output literals, both single SQL
> statements of ~60 and ~130 characters. The two renderers that would genuinely repay a committed
> snapshot are named below; adopting them is a *run-and-review* change (generate → `cargo insta
> review` → commit a reviewed artifact), which a source-only pass cannot honestly produce.

## What the rule asks, and where Walrus already sits

The rule's own table routes short scalars (`true`, `42`, `"ok"`) to `assert_eq!` and reserves
`assert_*_snapshot!` for multi-line or structured output. Walrus's assertions sit almost entirely in
the first column, and that is a property of the code under test rather than a stylistic accident:

- **The big literals in the test corpus are *inputs*, not outputs.** `sink_meta_test.rs`'s
  `DOCS_EXAMPLE`, `type_descriptor_test.rs`'s `DOCS_DESCRIPTOR` / `LEGACY_SCALAR_DESCRIPTOR`,
  `pg_shape_test.rs`'s legacy registry document, and the `figment` TOML jails in the three
  `config_test.rs` files are all fixtures fed *in*. A snapshot cannot replace a fixture; it records
  what a run produced.
- **The serialization assertions are scalars.** `serde_json::to_string(&Op::Insert)` is `"\"i\""`,
  `Tier::One` is `"1"`, `EpochNo(42)` is `"42"`, `Lsn` is one 16-hex-digit string. Pinning those in a
  `.snap` would move a one-glance fact into a second file.
- **The error assertions are one line each.** `LoaderError::Duck` renders
  `"DuckDB: append s3://bucket/f.parquet → orders_raw"`; the chain tests walk `source()` and probe for
  a substring rather than freezing a rendered chain. The *shape* of every message is already an
  executable repo-wide rule (`crates/common/tests/error_message_style.rs`), which is a stronger check
  than a per-message snapshot: it applies to messages nobody wrote a test for.

## The two sites shaped like the rule's "Bad" example

`crates/pg-sink/src/reload_export_test.rs:33` and `crates/pg-sink/src/snapshot_test.rs:41` are the
tree's only full-equality assertions against a rendered string:

```rust
assert_eq!(
    sql,
    "SELECT \"region\"::text, \"id\"::text, \"name\"::text \
         FROM \"public\".\"customers\" AS _src \
         ORDER BY _src.\"region\", _src.\"id\" LIMIT 1000"
);

assert_eq!(
    select,
    "SELECT \"id\"::text, \"status\"::text FROM \"public\".\"orders\""
);
```

Each is one statement, and each literal *is* the documentation of its contract: a `::text` cast on
every column, the `_src` alias, a full-composite `ORDER BY`, `LIMIT` last. The rule's threshold
("multi-line or structured output") is not met, and a `.snap` here would put the contract one file
away from the test that explains it. Left as-is.

## The census

| Candidate output | Size / shape | Verdict |
|---|---|---|
| `loader::transform::TransformSql::render` / `render_rebuild` | The 58-line `sql/duckdb/templates/transform.sql` under 11 substitutions | **The real candidate.** Today 6 `contains` probes pin ~5 fragments of it; everything else — comment block, dedup window, the three MERGE branches, the TOAST back-scan — is unpinned text |
| `pg_to_arrow::schema::build_schema` | An Arrow `Schema`: fields, types, nullability, metadata | **Second in line.** `orders_relation_maps_to_expected_tier1_schema` hand-checks 5 names and 4 types; `assert_debug_snapshot!` would additionally pin nullability and metadata that the field-by-field asserts skip |
| `loader::ddl::apply_additive` / `apply_destructive` | SQL per change, ~120 bytes each | **No site.** Both build their batch into a local `String` and execute it in the same function — there is no pure renderer to snapshot without first extracting one from production code, and the behaviour is covered against a live engine by `crates/loader/tests/{ddl_additive,ddl_destructive}.rs` |
| `common::metrics::render()` | Prometheus text exposition, dozens of series | **Anti-site.** The recorder is process-global and installed once, so the rendered text depends on which tests in the binary already ran and what they recorded. `render_lists_every_series` asserts presence of each name for exactly that reason; a byte snapshot would be flaky by construction |
| `pg_sink::health::ReadyBody` | `{"ready":…,"degraded":…}` | **Anti-site.** Two booleans |
| Config JSON (the rule's `assert_json_snapshot!(config)` example) | — | **Anti-site.** Walrus's config types are `Deserialize`-only; serializing one for a snapshot means adding a production `Serialize` derive purely for a test, against `notes/rust-skills/api-serde-optional.md` |
| CLI `--help` output | — | **No site.** There is no `clap`/`structopt` and no `std::env::args` reader anywhere; both binaries are config-driven services |

## Why this pass records the shortlist instead of landing it

Two independent reasons, and either alone is sufficient.

**A snapshot is a generated artifact.** The rule's workflow is run → `.snap.new` → review → commit.
The value is entirely in the *reviewed* diff; a hand-typed `.snap` is a hand-maintained expected
literal wearing a different extension — the exact thing the rule exists to remove. `render`'s output
is the 58-line template with eleven substitutions, including a multi-line `CASE WHEN … COALESCE((SELECT …))`
TOAST back-scan assembled by `format!`; transcribing it by eye would produce precisely the brittle
artifact the rule warns about, with no way to verify it in a pass that does not run tests.

**The manifest edit cannot stand alone.** `Cargo.lock` holds 499 packages and contains no `insta`,
`similar`, `console`, `linked-hash-map`, or `globset`, so adopting the crate adds new locked entries
with registry checksums that only a resolver can compute. Until they exist,
`scripts/proc-macro-guard.sh --check` fails inside its `cargo metadata … --locked --offline` probe
before any test runs — the workspace-shape gate rejects a manifest whose lock is stale.
Adding the dependency and the snapshots therefore belongs to one PR that can resolve, run, and review.

## What would reopen this

Adopt `insta` when a PR can run the suite, and scope it to the shortlist above rather than the tree:

1. `insta = { version = "1", default-features = false }` as a **dev**-dependency of `loader` (and
   `pg-to-arrow` if `build_schema` is included), matching the `criterion` / `proptest` precedent of
   trimming default features to keep the dev-dep tree lean.
2. **Named** snapshots (`insta::assert_snapshot!("tier1_orders_render", sql)`) — an unnamed snapshot
   keys off the test-function name, and this repo renames tests for descriptiveness, which would
   silently orphan the `.snap`.
3. `INSTA_UPDATE=no` in the CI test job, so an unreviewed snapshot fails rather than being written.
4. `*.snap.new` in `.gitignore`; the reviewed `.snap` files are committed and read in review.

The trigger to revisit earlier: a transform-template change that lands green because no test looked
at the part of the SQL it altered. That is the failure mode a committed render snapshot would have
caught, and it is the argument that should decide adoption — not the count of assertions it replaces.

## See also

- Rule: `.claude/skills/rust-skills/rules/test-snapshot-testing.md`
- The renderers: `crates/loader/src/transform.rs:195` (`render`), `:306` (`render_rebuild`),
  template `crates/loader/sql/duckdb/templates/transform.sql`
- The behavioural safety net those renderers already have: `crates/loader/tests/transform.rs`
  (hermetic — replays every `walrus-loader.md §6` case against `Connection::open_in_memory()`), plus
  the compose-gated `phase_b.rs`, `compaction.rs`, `reload_rebuild.rs`
- Message-shape gate that covers rendered errors repo-wide: `crates/common/tests/error_message_style.rs`
- Sibling testing-rule audit: `docs/implementation/notes/rust-skills/test-mock-traits.md`
- Declined/deferred-dependency precedent: `docs/implementation/notes/rust-skills/conc-rayon-par-iter.md`
