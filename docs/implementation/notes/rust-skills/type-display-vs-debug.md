# Display and Debug ownership (PR 18.4)

Status: superseded by PR 13.1

Owned postcondition: public Debug coverage plus `missing_debug_implementations = "deny"`

PR 13.1 is the single owner of the workspace's public-`Debug` policy. Its task commit is
`469b3c372baa24e2469a4093acff63b9dce12cfd` (`feat(PR 13.1): require Debug across public APIs`),
with the `Walrus-Task: 13.1` trailer. Repeating that sweep here would duplicate ownership and could
replace deliberate implementations such as `Lsn`'s diagnostic form.

## Current proof

The following evidence was reproduced on PR 18.4's immediate predecessor, `040758f`:

- `Cargo.toml` contains `missing_debug_implementations = "deny"` in the workspace Rust lint table.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes. Because the rustc
  lint is denied, that build is the authoritative proof that reachable public types across all
  workspace targets implement `Debug`.
- A workspace search finds no local `allow` or `expect` for `missing_debug_implementations` and no
  manifest that lowers it to `warn` or `allow`.
- `Lsn` remains the deliberate non-derived case: its hand-written `Debug` renders
  `Lsn(00000000019A2B3C)`, while its hand-written `Display` renders the canonical bare padded value
  `00000000019A2B3C`.
- The ID newtypes in `crates/common/src/ids.rs` derive `Debug` for diagnostic type identity and
  implement `Display` separately by formatting their inner integer.

## Separate Display invariant

`Display` remains an intentional user-facing contract; it is never derived and is never
implemented by delegating to `Debug`. Source scans found no `Display` in a derive attribute and no
`Display` implementation that calls `Debug::fmt`, `fmt::Debug`, or emits a debug format placeholder.
PR 13.1 therefore owns Debug coverage without owning or changing any Display behavior.

## Reversal condition

Re-audit this supersession decision if a public type is exempted from
`missing_debug_implementations` without a documented reason. Removal or weakening of the workspace
deny, or a crate ceasing to inherit the workspace lint, likewise invalidates the coverage proof;
restore ownership deliberately rather than adding a duplicate Debug sweep under PR 18.4.
