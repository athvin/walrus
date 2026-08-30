# Returned closure mappers in walrus (PR 25.1)

Status: superseded by PR 13.12
Baseline: `11742412b47325f52fdd7d770afbe7b036d27b0f`

## What the rule recommends

When an API must produce one concrete closure for its caller to invoke later, return-position
`impl Fn`, `impl FnMut`, or `impl FnOnce` names that closure without a heap allocation or virtual
dispatch. The weakest trait that meets the call contract is preferred. Boxing is appropriate only
when runtime branches return different closure types or callbacks must be stored heterogeneously in
a field or collection.

This rule does not require an API to manufacture a returned closure when no caller needs one.

## Why the original finding is obsolete

The original audit described an older loader with `LoaderError::Duck(String)`, 27 grep-visible
hand-written `map_err` closures plus one multiline closure, and no shared result-context API. That
tree no longer exists. At the recorded baseline, all five DuckDB consumers use
`DuckResultExt::{duck, duck_with}` and none constructs `LoaderError::Duck` directly.

The read-only audit found 29 extension-method result sites:

- 13 `.duck(...)` calls for constant operation context;
- 16 `.duck_with(...)` calls for formatted operation context; and
- one separate `duck_err(...)` call in `compaction::full_rebuild` after rollback and cancellation
  handling.

The last path is deliberately direct: it must attempt `ROLLBACK`, then treat a cancellation as
`Ok(())`, before converting a genuine DuckDB failure. Moving that construction earlier would change
the drain-time behavior.

The relevant baseline probes and results were:

```text
rg -o '\.duck\(' crates/loader/src/{duck,compaction,ddl,phase_b,transform}.rs | wc -l
=> 13
rg -o '\.duck_with\(' crates/loader/src/{duck,compaction,ddl,phase_b,transform}.rs | wc -l
=> 16
rg -n 'LoaderError::Duck \{' crates/loader/src/{duck,compaction,ddl,phase_b,transform}.rs
=> no matches
rg -n 'return Err\(duck_err\(' crates/loader/src/compaction.rs
=> one match at compaction.rs:65
rg -n --glob '*.rs' -- '-> impl Fn(Once|Mut)?' crates tests     => no matches
rg -n --glob '*.rs' 'Box<dyn Fn|Arc<dyn Fn|Rc<dyn Fn' crates tests => no matches
```

`duck_with(op: impl FnOnce() -> String)` accepts rather than returns a closure. Its implementation
invokes `op()` inside `Result::map_err`, so formatting happens only when the result is `Err`; the
successful path performs no context allocation. Adding
`duck_err(ctx) -> impl FnOnce(duckdb::Error) -> LoaderError` would not remove any current closure or
clarify ownership. It would duplicate the extension API with a parallel way to express the same
mapping.

## Earlier ownership and current proof

PR 10.3 owns the typed error chain. Its implementation commit `bf3c248` changed the loader variant
to `LoaderError::Duck { op, source: duckdb::Error }`, preserving the engine error for source-chain
walkers while retaining the `common::Error::Internal` and internal-exit-code mapping. Its test
commit is `5094692`.

PR 13.12 owns DuckDB context construction and consumer routing. Commit `865231a` added the extension
API's regression seam, and implementation commit `2c60214` (the commit carrying
`Walrus-Task: 13.12`) routed every then-existing result mapping plus the direct rebuild error through
one constructor. The current sole production `LoaderError::Duck { ... }` construction remains in
`crates/loader/src/duck_ext.rs`; the five consumer modules contain none. The later-added
truncate-boundary query uses the same extension abstraction, accounting for the current 29 result
sites.

The operation label is incident-facing context for the single loader process that owns every
per-table DuckDB file. The typed engine source remains available beneath that label, while the exit
boundary renders both fields for operators.

## Existing regression coverage

The sibling `duck_ext_test.rs` suite already covers the relevant behavior:

- `duck_preserves_the_typed_error_and_operation` pins constant operation text and the original typed
  DuckDB source;
- `duck_with_preserves_the_typed_error_and_formatted_operation` pins formatted context and the same
  source preservation; and
- `duck_with_does_not_format_on_success` proves the `FnOnce` context callback is not invoked for
  `Ok`.

The sibling `error_test.rs` suite separately proves that the structured DuckDB error exposes its
source and still maps to the internal exit classification. Together these tests cover the behavior
that a new returned mapper would otherwise need to reproduce.

## Reversal condition

Reconsider return-position `impl FnOnce` only when walrus has an API that must produce one concrete
closure for a caller to invoke later and neither an extension method nor a direct generic callback
expresses that ownership cleanly. If runtime branches must return heterogeneous closure types, or if
callbacks must be stored in a struct or collection, use the rule's boxed/dynamic alternative instead
of claiming that one opaque returned type can represent them.
