# SQLx lifetime naming in walrus (PR 27.5)

Status: superseded by PR 9.6
Baseline: `943808232acbba112fa809bfde308ec4495f2934`
Owning commit: `1e7cc0617cda6924dae4501e8f7f90d3c5cfcf63`

## What the rule recommends

A lifetime name should communicate a relationship. Short conventional names such as `'a`, `'de`,
and `'r` keep that relationship readable; when no relationship needs a name, Rust's anonymous
lifetime `'_` is more precise. It says that a lifetime parameter exists while leaving its identity
to inference.

That distinction applies directly to the hand-written SQLx codecs in `common`. An encoder impl has
no method parameter or return value tied to the trait lifetime, so `Encode<'_, Postgres>` carries
all the information available. A decoder receives `PgValueRef<'r>`, so its `'r` connects the
`Decode<'r, Postgres>` implementation to the borrowed Postgres value and remains meaningful.

## Why the original two-site finding is obsolete

The original PR 27.5 audit described two definitions written as
`impl<'q> Encode<'q, Postgres>`—one for `ManifestId` and one for `Lsn`—and no explicit workspace
policy for `clippy::elidable_lifetime_names`. That was the same source seam already selected by the
earlier `own-lifetime-elision` task.

PR 9.6 changed both definitions to `impl Encode<'_, Postgres>` and denied the Clippy lint. Repeating
those changes in PR 27.5 would be empty work at the baseline and would misattribute ownership of the
source and lint policy. The correct residual outcome is therefore historical and current-tree
evidence, with no new production change.

## PR 9.6 ownership: metadata, trailer, and complete diff

The owning commit is
`1e7cc0617cda6924dae4501e8f7f90d3c5cfcf63`, with subject
`refactor(PR 9.6): elide one-use lifetime names`. Its body contains the sole ownership trailer
`Walrus-Task: 9.6`.

Its complete three-path implementation diff is:

```diff
diff --git a/Cargo.toml b/Cargo.toml
@@
 redundant_clone = "deny"
 implicit_clone = "deny"
+# Nursery (outside `clippy::all`, so no `priority` juggling): flags a lifetime that must exist but
+# need not be named. Its coarser sibling `needless_lifetimes` already arrives through `all`.
+elidable_lifetime_names = "deny"

diff --git a/crates/common/src/ids.rs b/crates/common/src/ids.rs
@@
-    impl<'q> Encode<'q, Postgres> for ManifestId {
+    impl Encode<'_, Postgres> for ManifestId {

diff --git a/crates/common/src/lsn.rs b/crates/common/src/lsn.rs
@@
-    impl<'q> Encode<'q, Postgres> for Lsn {
+    impl Encode<'_, Postgres> for Lsn {
```

Those are the only changed lines: one root lint-policy addition and the two lifetime spellings. PR
9.6 did not change an encoder body, decoder, test, SQL file, or `.sqlx` cache entry. Its canonical
task is `docs/implementation/phase-9-rust-ownership/pr-9.6-own-lifetime-elision.md`.

## Current common SQLx codec census

At baseline `943808232acbba112fa809bfde308ec4495f2934`, the read-only probes report:

```text
rg -n "impl<'q>" crates/common/src
=> no matches

rg -n "impl Encode<'_, Postgres> for (ManifestId|EpochNo|SchemaVersionNo|ReloadId|Lsn)" \
  crates/common/src/ids.rs crates/common/src/lsn.rs
=> 5 matches: ManifestId, EpochNo, SchemaVersionNo, ReloadId, Lsn

rg -n "impl<'r> Decode<'r, Postgres> for (ManifestId|EpochNo|SchemaVersionNo|ReloadId|Lsn)" \
  crates/common/src/ids.rs crates/common/src/lsn.rs
=> 5 matches: ManifestId, EpochNo, SchemaVersionNo, ReloadId, Lsn

rg -n '^elidable_lifetime_names = "deny"$' Cargo.toml
=> 1 match in root Cargo.toml
```

The lint's adjacent comment still identifies it as nursery, outside `clippy::all`, and explains why
no group-priority adjustment is required. The zero/5/5/1 result proves both that the original
violation is absent and that the meaningful decoder lifetime was not erased for cosmetic symmetry.

## Later newtype extensions from PR 13.3 and PR 18.1

PR 13.3 commit `a07451dfb58f020664026bafe94913f3b389153c` introduced `EpochNo`. Its initial
SQLx support already paired `impl Encode<'_, Postgres> for EpochNo` with
`impl<'r> Decode<'r, Postgres> for EpochNo` and `PgValueRef<'r>`.

PR 18.1 commit `f4dc3f4849b2e0851b0426125a5d3c32a515d6be` introduced `SchemaVersionNo` and
`ReloadId`. Both likewise entered with anonymous encoders and named decoders. These additions grew
the codec census from PR 9.6's two types to five without reopening the policy decision, demonstrating
that the earlier lint and implementation shape continued to guide later hand-written codecs.

## Why the decoder lifetimes remain named

Each encoder's anonymous lifetime occurs only as the `Encode` trait parameter. Neither
`encode_by_ref` argument nor its return type refers to it, so a name would ask readers to track an
identity that carries no information.

Each decoder is different: the same `'r` occurs in both `Decode<'r, Postgres>` and
`PgValueRef<'r>`. The name states that the value being decoded is borrowed for the trait's lifetime.
It is short, conventional, and used; changing it to `'_` would discard a real relationship rather
than improve naming.

## No executable changes in PR 27.5

PR 27.5 adds this evidence note only. It does not edit Rust source, tests, the lint configuration,
Cargo manifests or lockfile, SQL or `.sqlx`, dependencies, workflows, scripts, the roadmap index, or
design documentation. It also adds no intended-red test or source-scanning guard: PR 9.6 owns the
implementation and the existing workspace lint is the executable regression policy.

## Reversal condition

Re-open the `name-lifetime-short` audit when a hand-written common SQLx implementation introduces a
bound-but-unused named lifetime, or when the explicit workspace
`elidable_lifetime_names = "deny"` policy is missing. Audit a new verbose-but-used lifetime elsewhere
at its actual site. A short lifetime that still relates an implementation to an input—such as every
current `Decode<'r>` / `PgValueRef<'r>` pair—does not reverse this conclusion.
