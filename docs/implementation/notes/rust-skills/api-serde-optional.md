# serde stays a hard dependency (PR 13.17)

> **Status:** evaluated — rejected.

## What the rule asks for

`api-serde-optional` recommends making Serde an optional dependency, exposing it through a
`serde = ["dep:serde"]` Cargo feature, and guarding derived implementations with
`#[cfg_attr(feature = "serde", derive(...))]`. That is useful for a published general-purpose
library whose consumers may not serialize its types. Walrus's existing example of that Cargo
mechanism is the optional `sqlx` dependency and `sqlx = ["dep:sqlx"]` feature in
`crates/common/Cargo.toml:11-36`.

## Why it does not apply here

The workspace has six members, all listed as repository-local paths in `Cargo.toml:1-3`. Every
member is version `0.1.0`; the representative library package block in
`crates/common/Cargo.toml:1-6` contains only workspace-owned edition, license, and Rust-version
metadata. The e2e member makes the intent explicit with `publish = false` in
`tests/e2e/Cargo.toml:1-7`. No member declares registry-facing description, repository, homepage,
documentation, keywords, or categories metadata, and there is no consumer outside this workspace.
There is therefore no downstream user whose build would become smaller by making Serde optional.

## Why serde is load-bearing, not incidental

Serialization is part of Walrus's wire and bootstrap contracts:

- `Lsn`'s hand-written implementations in `crates/common/src/lsn.rs:170-183` guarantee a padded
  hexadecimal string whose text ordering matches numeric WAL ordering.
- `SinkMeta` is the provenance document embedded in every Parquet row; its serialized field
  contract starts at `crates/common/src/sink_meta.rs:116-153`, and production performs amortized
  JSON serialization in the same module at `crates/common/src/sink_meta.rs:163-231`.
- `TypeDescriptor` is persisted through the schema registry and derives Serde at
  `crates/common/src/type_descriptor.rs:74-90`.
- Configuration loading depends on Serde's unknown-field rejection and defaults for
  `CommonConfig` (`crates/common/src/config.rs:18-21`), `SinkConfig`
  (`crates/pg-sink/src/config.rs:29-31`), and `LoaderConfig`
  (`crates/loader/src/config.rs:11-13`).

Making Serde optional would require feature guards around the manual `Lsn` implementations and
`cfg_attr` annotations throughout `SinkMeta`, `TypeDescriptor`, and all three configs. The
`control`, `loader`, and `pg-sink` dependency graphs would then enable the feature unconditionally
because they exchange these serialized values, moving complexity without removing Serde from any
shipped service.

## What we did instead

`common` already has a real optional-dependency seam: `sqlx = ["dep:sqlx"]` and an optional
dependency in `crates/common/Cargo.toml:11-36`, with its complete conditional implementation
surface in `crates/common/src/lsn.rs:186-223` and `crates/common/src/ids.rs:60-118`.

The workspace test enables that feature through `control`, and all-features Clippy enables it
directly, so neither observes the off state. The gates job now runs
`cargo check -p common --no-default-features --all-targets` in `.github/workflows/ci.yml:137-142`.
Package isolation is the load-bearing part: `common` has no default feature today, so the explicit
flag records and future-proofs the intent rather than changing the current feature set.

## What would reverse this

Revisit the decision if `common` is prepared for registry publication—the current internal member
list is in `Cargo.toml:1-3` and its minimal package metadata is in
`crates/common/Cargo.toml:1-6`—or if an in-repo consumer needs the domain types without
serialization. At that point the feature and `cfg_attr` mechanics from the rule would buy a real
consumer benefit, and both Serde-on and Serde-off builds should become explicit CI contracts.
