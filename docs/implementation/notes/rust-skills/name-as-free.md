# Free `as_*` conversions in walrus (PR 27.1)

Status: superseded by PR 13.4
Baseline: `ae87a92a3b06afa5fd54fe0713c4d8d0cc65648e`
Owning commit: `e2ada0dfb42dc15a0b155f8d385d477ff73b4f5d`

## What the rule recommends

An `as_*` name promises a free conversion: ordinarily a borrowed view such as `&T -> &U`, with no
allocation, I/O, or derived owned value. A conversion that borrows its input but creates an owned
output belongs under `to_*`; one that consumes its input belongs under `into_*`. Walrus also uses
the standard-library-style primitive projection form on `Copy` wrappers: returning an `f64` or
`u64` by value is still free and does not disguise ownership or allocation.

The original PR 27.1 audit found two references to
`LoaderError::as_common(&self) -> common::Error` within a seven-method production `as_*` surface.
That method cloned strings or formatted messages in every match arm, so its `as_*` prefix did not
describe its cost. The finding was correct for that historical tree, but the named method no longer
exists at this task's baseline.

## Why the original LoaderError finding is obsolete

PR 13.4 removed both the allocating `as_common` method and its caller. Reintroducing the originally
proposed `to_common` would create an ad-hoc API alongside the standard conversion that already owns
the seam. The current source/test census contains zero `as_common` references and zero
`to_common` definitions.

The reference conversion is the stronger final interface:

- `impl From<&LoaderError> for common::Error` retains the caller's `LoaderError` for logging;
- the standard blanket implementation also makes `Into<common::Error>` available for
  `&LoaderError`; and
- the conversion remains exhaustive, so a new `LoaderError` variant cannot silently avoid a
  mapping decision.

The conversion does allocate where required to produce the owned `common::Error`; `From` does not
promise that a conversion is free. Its standard trait identity makes a second cost-prefixed
inherent method unnecessary.

## PR 13.4 ownership and unchanged mapping semantics

The owning implementation commit is
`e2ada0dfb42dc15a0b155f8d385d477ff73b4f5d`, with subject
`feat(PR 13.4): standardize From conversions`. Its body ends with the sole ownership trailer
`Walrus-Task: 13.4`.

The historical diff changes only the adapter's interface around the match: it removes
`LoaderError::as_common`, installs `From<&LoaderError> for common::Error`, changes `self` to the
impl argument `e`, and routes the then-inherent `exit_code` method through
`common::Error::from(self)`. Every then-existing match arm and rendered message is preserved. At
the process boundary, the same commit replaced the direct inherent call with
`common::Error::from(&e).exit_code()` while retaining `e` for the preceding log statement.

The current exhaustive conversion preserves the same common-error classes and messages:

- configuration, control-DB, object-store, lease-contention, and quarantine failures retain their
  dedicated `common::Error` variants;
- DuckDB, corrupt-checkpoint, epoch-bump, registry-decode, LSN-parse, health, and loader-internal
  failures retain their formatted `common::Error::Internal` mappings;
- control transaction failures deliberately retain `common::Error::ControlDb`; and
- no mapping uses a wildcard arm, so every later variant is explicitly classified by the same
  conversion.

Consequently the public exit statuses are unchanged: configuration maps to 10, control DB to 11,
object store to 12, lease contention to 15, quarantine to 17, and internal failures to 70.

## Current LoaderError exit-code seam

PR 19.2 commit `bfe69abd1c360716a9ce1fbd3e089f4245e8aa38` later moved loader
classification from an inherent method to `impl FailureClass for LoaderError`. It did not replace
the PR 13.4 conversion. The current chain is:

```text
LoaderError
  -> FailureClass::exit_code
  -> common::Error::from(self).exit_code()
  -> common::ExitCode
  -> std::process::ExitCode
```

In loader `main`, the error is first logged and then `e.exit_code().into()` is returned. Thus the
borrowed reference conversion still owns the exact per-variant map, while PR 19.2 owns only the
trait-level routing to that map.

## Residual production `as_*` census

The baseline has exactly five textual production `fn as_*` definition sites:

| Definition | Receiver and return | Why it is free |
|---|---|---|
| `SqlIdent::as_raw` | `&self -> &str` | Borrows the identifier's stored string; no clone or allocation. |
| `Ratio::as_f64` | `self -> f64` | `Ratio` is `Copy` and projects its inner primitive directly. |
| `DuckTable<K>::as_str` | `&self -> &str` | Borrows the table name's stored `String`. |
| `string_enum!` generated `as_str` | `self -> &'static str` | Matches a `Copy` enum and returns a static literal. |
| `Lsn::as_u64` | `self -> u64` | `Lsn` is `Copy` and projects its inner primitive directly. |

The one textual `string_enum!` definition represents four production invocations:

- `ManifestKind` and `ManifestStatus` in `crates/control/src/manifest.rs`; and
- `ReloadFlavor` and `ReloadStatus` in `crates/control/src/reload.rs`.

All four enums derive `Copy`. Their generated match arms return the persisted `&'static str`
literals named directly by each invocation, so none formats, clones, allocates, or performs other
work. Macro expansions are not extra textual definition sites.

### Later addition: `UtcTimestamp::as_inner`

A re-audit against the raw rule added a sixth site by renaming, not by introducing a conversion:
`UtcTimestamp::inner(&self) -> &jiff::Timestamp` became `UtcTimestamp::as_inner`. The census above
only asked whether an existing `as_*` name overstated its cost; it did not ask the converse, whether
a free borrow was hiding under a name that does not say so. This one was: the method sits directly
beside `into_inner`, and `jiff::Timestamp` is `Copy`, so `inner()` read as ambiguous between the
borrow it is and the copy its neighbour returns. The rename makes the pair state the rule's
`as_`/`into_` cost distinction and matches the five sites above. It borrows a field and allocates
nothing, so the reversal condition below is untouched.

## Existing regression coverage

The sibling `crates/loader/src/error_test.rs` suite exercises representative mapping and source
behavior without any PR 27.1 changes:

- `duck_still_exits_internal` pins the DuckDB mapping to `Internal`;
- `registry_decode_preserves_source_and_exits_internal` pins its source and internal code;
- `lsn_parse_keeps_the_offending_input_in_the_chain` pins the parse source and internal code;
- `control_txn_failure_preserves_source_and_exits_control_db` pins the deliberate control-DB code;
  and
- `health_failure_preserves_source_and_exits_internal` pins its source and internal code.

The conversion remains exhaustive and the existing bootstrap coverage separately exercises the
lease-contention exit code. PR 13.4 therefore already owns the behavioral seam that an artificial
PR 27.1 red test would duplicate.

## Reversal condition

Re-open the `name-as-free` audit when a production `as_*` definition is introduced or changed so
that it returns an owned, computed, or allocating value. Audit that violation at its actual site
and choose the honest `to_*`, `into_*`, or standard conversion interface for its ownership model.
Such a future violation does not reverse PR 13.4's ownership of the removed LoaderError adapter and
does not justify recreating `to_common`.
