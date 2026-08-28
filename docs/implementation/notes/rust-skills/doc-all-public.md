# `doc-all-public`: the sweep the `missing_docs` note was waiting for

> **Status:** applied. Every `///`-documentable public item under `crates/*/src` now carries a doc
> comment. Three carve-outs remain, all structural rather than skipped: the 13 `string_enum!`
> variants (the exported macro's grammar cannot accept a per-variant attribute), the three
> `mod private { pub trait Sealed {} }` markers (not reachable from outside their crate), and the
> `missing_docs` lint entry itself, which belongs to [`lint-missing-docs`](./lint-missing-docs.md).

## What the rule asks for

`doc-all-public` asks for a `///` on every public item and names the shapes it wants covered:
structs, struct fields, enums, enum variants, functions, traits, trait methods, type aliases and
constants. It is the documentation itself; the lint that would enforce it is a separate rule.

## What was already true

Two thirds of the surface needed nothing. `clippy::missing_errors_doc = "deny"` fires on an exported
`Result`-returning function with no doc at all, so **every fallible public function already had one**
with an `# Errors` section — which is why `control`, whose public API is almost entirely `async fn …
-> Result<_, ControlError>`, had zero undocumented functions. Every module, crate root, bench and
integration test already carried a `//!`. The gap was structurally confined to the shapes no lint
reaches: infallible accessors, constructors, builder setters, named fields, enum variants, and one
table of constants.

## What this pass documented

| Surface | Count | Where the concentration was |
|---|---|---|
| Constants | 54 | `common/src/oids.rs` — the whole file |
| Named `pub` fields | ~150 | `control`'s six row mirrors (43), `pg-sink`'s handle structs (~55), `loader`'s (~28) |
| Enum variants | ~80 | the `thiserror` taxonomies, plus `ExitCode` (10) and the two `Phase` enums |
| Functions / methods | ~45 | `consume.rs`'s `DecodeLoopBuilder` (15), `memory.rs` (9), the probe readers |
| Structs / enums themselves | 4 | `RelationCache`, `LoaderState`, `LoaderConfig`, and `RangeFamily`'s two constructors |

Three of those deserve their reasoning recorded, because in each case the cheap doc would have been
the wrong one:

- **`oids.rs`.** The previous note called this the block "where the rule's payoff is weakest per line
  and its cost highest", because the constant's name is already the Postgres type name. That is true
  of `/// OID of \`bool\`.` and not of what landed: each constant now names the **SQL spelling a user
  would write** — which diverges from the catalog name exactly where it matters (`CHAR` is `"char"`,
  the internal single-byte type, not `char(n)`; `BPCHAR` *is* `character(n)`; `VARBIT` is
  `bit varying(n)`) — plus its walrus tier and, for Tier 1, the Arrow type
  `pg_to_arrow::schema::tier1_data_type` maps it to. `NUMERIC` is the payoff case: it is Tier 1 only
  when `p ≤ 38`, and Tier 3 otherwise, which no reader could recover from the name.
- **The row mirrors.** `epoch` / `source_schema` / `source_table` repeat across eight `control`
  structs, and a per-field restatement would have been noise. What each doc says instead is the
  field's *role in that row*: `ManifestRow::id` is documented as the tiebreaker half of the
  `(lsn_end, id)` claim order rather than as "the primary key", and `lsn_end` as the commit LSN the
  queue sorts on — the distinction the module doc calls the single load-bearing line.
- **The builder setters.** All 14 carried a `#[must_use]` and nothing else. Each now says what the
  collaborator it wires *does* in the loop, which is the one thing the parameter type does not say.

## The blocker that is still a blocker

`string_enum!`'s matcher (`crates/common/src/string_enum.rs:76-88`) captures attributes only for the
enum:

```rust
$(#[$attr:meta])*
$vis:vis enum $name:ident {
    error = $error:path;
    column = $column:literal;
    $($variant:ident => $text:literal),* $(,)?
}
```

There is no attribute fragment in front of `$variant`, and the expansion emits `$($variant,)*` bare,
so no call site can attach a `///` to one. That leaves 13 variants undocumented: `ManifestKind` (4)
and `ManifestStatus` (2) in `control/src/manifest.rs`, `ReloadFlavor` (2) and `ReloadStatus` (5) in
`control/src/reload.rs`. All four *enums* carry docs, and in each case the enum's own doc already
enumerates its variants in prose, so nothing about the vocabulary is undocumented — only the
per-variant rendering is.

The fix is one line of grammar (`$($(#[$vattr:meta])* $variant:ident => $text:literal),*`, re-emitted
as `$($(#[$vattr])* $variant,)*`) plus 13 call-site docs. It was **not** taken here, for a reason
specific to this pass rather than a judgement about the change: changing an exported macro's grammar
is exactly the edit that must be proven by compiling it, and executable validation is deferred in a
rule pass. A macro that silently stops matching would take the whole workspace with it. It belongs to
the change that can run `cargo check`.

## The other two carve-outs

- `mod private { pub trait Sealed {} }` in `common/src/sql.rs`, `loader/src/duck_ext.rs` and
  `pg-sink/src/batch.rs`. `pub` inside a private module, so they are unreachable downstream and
  `missing_docs` never fires on them; the sealing contract they implement is documented on the public
  trait that is sealed, which is where a reader looks.
- `common::__private` (`common/src/lib.rs:68-76`) is `#[doc(hidden)]` with a stability paragraph
  saying it is not API. Left alone deliberately.

## What this unblocks

[`lint-missing-docs`](./lint-missing-docs.md) deferred its manifest entry because the entry is a hard
build failure until the exported surface is documented — `warnings = "deny"` makes `warn` and `deny`
the same diagnostic here, so there is no gradual path. Steps 2–4 of that note's checklist are now
done. Step 1 (the macro grammar) is the remaining blocker, and step 5 (`missing_docs = "deny"` plus
its case in `crates/common/tests/workspace_lints_inherited.rs`) still needs the build-based proof that
note specifies: `cargo clippy --workspace --all-targets` and
`RUSTDOCFLAGS='-D missing_docs' cargo doc --workspace --no-deps`.

## What would reverse this note

- The `string_enum!` grammar change landing, which would close the last gap and let the lint go in.
- A new public item arriving undocumented. Nothing enforces this yet — that is precisely what the
  deferred lint entry is for, and until it lands this note is the only record that the surface was
  ever complete.

## See also

- `docs/implementation/notes/rust-skills/lint-missing-docs.md` — the inventory this pass consumed.
- `docs/implementation/notes/rust-skills/doc-crate-readme.md` — the other documentation-family rule,
  and the `publish = false` argument it turns on.
