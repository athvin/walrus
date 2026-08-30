# serde `flatten` — evaluated and declined (PR 22.8)

> **Status:** evaluated — supported by the pin, declined for the current config API.

## What the rule claims

The local `serde-flatten` rule recommends flattening shared fields into their parent wire object,
but says that the attribute is incompatible with `deny_unknown_fields`
(`.claude/skills/rust-skills/rules/serde-flatten.md:69-80`). That caveat is not true of the version
walrus actually builds. `Cargo.lock:3392-3419` pins `serde`, `serde_core`, and `serde_derive` to
1.0.228, so the decision must follow that source and executable behavior rather than treating the
rule summary as dependency documentation.

The rule's other caveat is narrower and accurate: flattening a struct takes Serde's buffered
deserialization path. Whether that matters depends on a real deserialization call path; the mere
existence of a serialization benchmark is not evidence of a regression.

## What Serde 1.0.228 actually does

The pinned derive accepts flatten on named-field structs. Its validation rejects the attribute only
on tuple and newtype structs (`serde_derive` 1.0.228,
`src/internals/check.rs:99-136`); there is no check that rejects `deny_unknown_fields`.

The generated deserializer then implements a deliberate three-stage protocol:

1. It allocates a content buffer when a flattened field exists and collects keys not owned by
   direct outer fields (`serde_derive` 1.0.228, `src/de/struct_.rs:224-230,276-310`).
2. It deserializes the flattened field through `FlatMapDeserializer`
   (`serde_derive` 1.0.228, `src/de/struct_.rs:327-345`). `FlatStructAccess` offers that nested
   struct only keys from its declared field list, consuming recognized entries and leaving the
   others in the buffer (`serde` 1.0.228,
   `src/private/de.rs:3260-3274,3387-3446`).
3. When the outer container has `deny_unknown_fields`, the generated leftover check returns an
   `unknown field` error for the first unclaimed entry (`serde_derive` 1.0.228,
   `src/de/struct_.rs:347-360`).

The compiled fixture in `crates/common/src/lib_test.rs:38-61` exercises that exact combination with
strict inner and outer structs. Known flat keys deserialize into their correct owners, while an
additional `nonsense` key is rejected. This is supported behavior in walrus's pin, not a compile
error.

## The viable config candidate

`CommonConfig` declares `control_db_url`, `object_store`, `telemetry`, `startup_deadline`, and
`instance` directly (`crates/common/src/config.rs:19-33`). `LoaderConfig` repeats those five fields
(`crates/loader/src/config.rs:19-58`), as does `SinkConfig`
(`crates/pg-sink/src/config.rs:28-107`). A flattened `common: CommonConfig` field is therefore a
genuine declaration-de-duplication candidate.

That representation could preserve the flat TOML, YAML, JSON, and `WALRUS_*` environment keys:
flatten changes Rust ownership without adding a `common` key to the external map. It could also
preserve strict unknown-key rejection by keeping `deny_unknown_fields` on the outer service config,
as the executable fixture demonstrates. Today all five config containers remain strict:
`CommonConfig` and `ObjectStoreConfig` (`crates/common/src/config.rs:19-43`), `TelemetryConfig`
(`crates/common/src/telemetry.rs:38-48`), `LoaderConfig` (`crates/loader/src/config.rs:19-20`), and
`SinkConfig` (`crates/pg-sink/src/config.rs:28-30`). The terminal unknown-key test remains at
`crates/common/src/config_test.rs:111-124`, and the operator-facing contract remains documented in
`deploy/k8s/base/configmap.yaml:1-5`.

## Why walrus does not migrate it here

The external representation can stay flat, but the Rust API cannot. Both binaries and their tests
currently access and construct the five shared fields directly; examples include the loader
constructor and shipped-default assertions (`crates/loader/src/config_test.rs:4-15,78-99`) and the
sink equivalents (`crates/pg-sink/src/config_test.rs:4-17,70-104`). Replacing those public fields
with `config.common.control_db_url` and peers would require a coordinated call-site migration in
both services.

More importantly, the three config types own different policy today. `CommonConfig` supplies and
validates its defaults (`crates/common/src/config.rs:45-65,67-153`) and returns `common::Error`.
`LoaderConfig` has a different default set and validator, plus its own `ConfigError` conversion
(`crates/loader/src/config.rs:60-78,116-164`). `SinkConfig` has another default set, a structured
error enum, and substantially broader validation (`crates/pg-sink/src/config.rs:110-164,240-310`).
A production flatten refactor must decide which layer owns shared defaults and validation, how a
common validation failure maps into each service error, and whether direct public field access
remains supported. PR 22.8 does not disguise that API and policy migration as an attribute cleanup;
it deliberately keeps the five declarations and all behavior unchanged.

## Why the `SinkMeta` benchmark is not a blocker

`SinkMeta` is a stable flat wire document (`crates/common/src/sink_meta.rs:179-222`), but it has no
nested component waiting to be exposed. Its production hot path serializes the private
`MetaConst`/`MetaRow` projections and splices their JSON fragments
(`crates/common/src/sink_meta.rs:231-315`; `crates/pg-to-arrow/src/batch.rs:230-252`). On the other
side, the loader extracts named JSON keys in SQL rather than deserializing `SinkMeta` through Serde
(`crates/loader/sql/duckdb/templates/append_parquet.sql:1-3`).

The ≈576 ns/row figure measures full per-row **serialization** before that split
(`docs/benchmarks.md:78-95`); PR 5.7 then amortized the batch-constant portion and measured the new
serialization path (`docs/benchmarks.md:298-315`). It does not measure flatten's buffered
deserialization and cannot support a claim about a current loader hot-path regression. Walrus keeps
the existing flat document and projection-based serializer because there is no useful production
flatten candidate here, not because this benchmark proves flatten would be slower.

## The guard and executable evidence

`crates/common/src/lib_test.rs:5-35` scans the five audited, struct-bearing candidate modules with
`include_str!`: `config.rs`, `telemetry.rs`, `sink_meta.rs`, `pg_shape.rs`, and
`type_descriptor.rs`. It removes whitespace before checking standalone and combined flatten
arguments, reports the offending module, and includes a synthetic combined-form source fixture that
proves the guard bites. The same file's compiled fixture records the pinned strict-key behavior, and
its pointer test keeps this decision linked from `CommonConfig`.

The scan deliberately excludes scalar representations. The ID types are transparent newtypes
(`crates/common/src/ids.rs:45-53,76-86`), for which the pinned derive rejects flatten, and `Lsn`
implements scalar string serialization/deserialization manually
(`crates/common/src/lsn.rs:239-252`). Neither is a named-field struct flatten candidate. The guard
therefore protects the reviewed policy surface without pretending every Serde use has the same
shape.

## Re-open conditions

Reconsider flattening `CommonConfig` only as a coordinated config migration that proves all of the
following together:

- existing flat file and `WALRUS_*` environment keys remain compatible, including both deployed
  ConfigMaps;
- strict-unknown tests cover standalone common config plus both flattened outer service configs;
- every direct public field access and constructor in both binaries and their tests has an explicit
  migration plan;
- shipped defaults remain identical and their ownership between common and service configs is
  documented;
- common and service validation still run exactly once with the same bounds; and
- common validation failures map into each service's existing terminal error taxonomy without
  losing context.

Until one change can satisfy that complete contract and land its call-site migration atomically,
walrus keeps the duplicated fields, preserves the current production representation, and requires
this decision to be reopened before adding flatten to any guarded module.
