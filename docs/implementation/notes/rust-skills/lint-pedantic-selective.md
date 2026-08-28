# Selecting from `clippy::pedantic` (rule `lint-pedantic-selective`)

> **Status:** audited 2026-08-28 — **five lints added, two declined.** walrus already took the
> rule's "Alternative: Explicit Opt-in" shape: the `pedantic` group is never named in
> `[workspace.lints.clippy]`, and its useful members are denied one at a time. This pass closed the
> gap against the rule's *Recommended Pedantic Lints* table by adding `unnested_or_patterns`,
> `used_underscore_binding`, `string_add_assign`, `semicolon_if_nothing_returned` and
> `wildcard_imports`, and recorded why `doc_markdown` and `unused_self` stay out.

## Why the group entry stays absent

The other six clippy groups walrus pins (`correctness`, `suspicious`, `style`, `complexity`,
`perf`, `cargo`) are — bar the last — all carried by `clippy::all`, so naming them costs no
diagnostic and buys immunity to a regrouping. `pedantic` is not carried by `clippy::all`, so a
group entry there is a *behaviour* change, and it is the one the rule warns about: enabling it
wholesale and then re-allowing the noisy members states less than a list of names does, because
each re-allow reads as a retreat rather than as a decision.

Three of its members are wrong for this tree specifically:

- `missing_panics_doc` cannot see five of the eight production panic sites. They are
  `tokio::time::interval`'s zero-period panic, raised inside tokio, so the lint's scan of the
  caller's own body finds nothing to document.
- `module_name_repetitions` describes `error::LoaderError` and every sibling that names its own
  module — the naming walrus chose so an error type is legible at its use site in another crate.
- `too_many_lines` prices a `pgoutput` decoder arm the same as a getter.

The inverse also holds, and is why this is a selection rather than a subset: two lints the usual
pedantic advice turns *off* are denied here on their own merits — `must_use_candidate` (PR 13.2)
and `missing_errors_doc`.

`crates/common/tests/workspace_lints_inherited.rs` asserts both halves of that decision together:
the group entry absent, and every named member of the pick present.

## The five added, and their censuses

| lint | sites found | disposition |
|---|---|---|
| `unnested_or_patterns` | 4 | fixed: `Some("yaml") \| Some("yml")` → `Some("yaml" \| "yml")` in the three config loaders, and `(INT2, INT4) \| (INT2, INT8)` → `(INT2, INT4 \| INT8)` in `loader/src/ddl.rs` |
| `used_underscore_binding` | 0 | every `_`-prefixed binding is an RAII guard, a discarded `write!` result, a compile-time assertion, or an ignored tuple slot or parameter — none is read |
| `string_add_assign` | 0 | every `a + b` in the tree is integer, `Instant`/`Duration` or float arithmetic |
| `semicolon_if_nothing_returned` | 8 | fixed: the seven wrapped `tracing::trace!` arms of `consume::trace_message`, plus the `append_value` arm of `push_geometric` in `pg-to-arrow/src/batch.rs` |
| `wildcard_imports` | 1 | scoped allow on the `pg_to_arrow::oids` forwarding shim |

The tree's other or-patterns are not nestable: they alternate *distinct* variants of one enum
(`TupleValue::Null | TupleValue::UnchangedToast`, the eight-arm `PreflightError` fan-in) or bare
char/OID ranges, which is the shape this lint exists to leave alone.

`semicolon_if_nothing_returned` has a narrower reach than its name suggests, and knowing the shape
is what makes the census closable by inspection: it fires only on a **multi-line** block whose tail
expression is unit-typed and whose snippet ends in neither `;` nor `}`. So a one-line match arm
(`Some(x) => b.append_value(*x),`) is out of reach because it is an expression and not a block; an
arm ending in `format!`/`matches!`/`Field::new(…)` is out of reach because the tail is not unit; and
`panic!`/`unimplemented!`/`bail!` are out of reach because their tail type is `!`, not `()`. What is
left is the statement someone wrote without its semicolon — eight of them, all found by grepping for
a call-shaped line immediately followed by a closing brace.

`wildcard_imports` deserves its own line because the deny rests on a carve-out clippy grants rather
than on anything the lint table says: `use super::*` inside a test module and `use x::prelude::*`
are skipped while `warn-on-all-wildcard-imports` keeps its default of `false`. 63 test modules here
take the first spelling and `lsn_test.rs` takes the second, so stating that one key as `true` would
put 64 imports in scope of a deny without the manifest moving —
`crates/common/tests/workspace_lints_inherited.rs` is what notices.

## `doc_markdown` — declined

The lint flags any camel-cased or `::`-bearing word in a doc comment that is not in backticks, and
excuses only what `clippy.toml`'s `doc-valid-idents` names. Clippy's default list carries
`PostgreSQL`, which is the single term of walrus's vocabulary it already knows; `DuckDB`, `MinIO`,
`pgoutput`, `CopyBoth`, `walrus-pg-sink` and the rest are not on it. So adopting this lint is
really adopting a curated allow-list, and a list is the wrong instrument for the underlying goal:
every one of those words *should* be in backticks at an identifier site, and the ones that are
exceptions (`DuckDB` as prose, not as an identifier) would be silenced globally by the list rather
than argued at the site. Re-open if the exception set turns out to be small enough to enumerate —
the decision is about the list, not about the lint.

## `unused_self` — declined

`unused_self` is one of the lints clippy gates behind `avoid-breaking-exported-api`, which defaults
to `true`. Under that default it is silent on the public API of `common`, `control` and
`pg-to-arrow` and live only inside `loader` and `pg-sink`, whose items nothing outside can reach —
an asymmetric policy that would read as a workspace rule while enforcing a two-crate one. That is
the same carve-out already noted for `trivially_copy_pass_by_ref`.

Making it uniform means setting `avoid-breaking-exported-api = false` in `clippy.toml`, and that key
is not scoped to one lint: it simultaneously widens `linkedlist`, `rc_buffer`, `ref_option`,
`trivially_copy_pass_by_ref`, `large_types_passed_by_value` and `needless_pass_by_ref_mut`, every
one of which is already denied here. Flipping it is therefore a six-lint decision wearing a
one-lint disguise, and belongs in its own pass with its own census.

## Reversal condition

Re-open the group question if `clippy::pedantic` is ever the shorter list — that is, if the members
walrus wants outnumber the members it must re-allow. The current count is the other way round.
