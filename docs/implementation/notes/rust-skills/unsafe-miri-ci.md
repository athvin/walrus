# Miri in CI — deferred (PR 12.7)

> **Status:** decided 2026-08-13 — **do not run Miri, and do not add a disabled Miri job;** the
> safe-only premise is enforced by `scripts/check-unsafe-invariants.sh`, and the concrete
> re-evaluation triggers are recorded below.
>
> **Re-verified 2026-08-27:** the precondition still holds and no trigger has fired; the counts and
> file:line anchors below were re-measured against the tree on that date.

## What the rule asks for

The `unsafe-miri-ci` rule says to “run `cargo miri test` in CI for every crate that contains
`unsafe` code.” Its own acceptable-to-skip list begins with crates that contain zero unsafe code.
Walrus currently satisfies that skip condition, but this decision also accounts for the FFI,
toolchain, and build-cost constraints that remain relevant if the source inventory changes.

## Why the precondition is empty

There are 0 first-party unsafe syntax sites across the 228 Rust files under `crates/` and `tests/`.
The two plain-word `unsafe` matches are documentation about unsafe DDL casts, not Rust syntax.
`Cargo.toml:19` sets `unsafe_code = "forbid"` in `[workspace.lints.rust]`, and every one of the six
workspace members opts into that table through `[lints] workspace = true`. A member therefore
cannot introduce an unsafe block or lift the policy with a local allow.

PRs 12.2–12.4 also deny the Rust-2024 unsafe-operation, extern-block, and unsafe-attribute legacy
forms (`Cargo.toml:20-32`). Those static policies make growth of first-party unsafe code a build
failure rather than a convention that can silently drift.

## Why the crates that link unsafe are the ones Miri cannot run

The loader links bundled DuckDB through `crates/loader/Cargo.toml:23`. The `pg-to-arrow`
conformance feature does the same at `crates/pg-to-arrow/Cargo.toml:30`. Both reach
`libduckdb-sys 1.10504.0` (`Cargo.lock:2137`), which builds and calls DuckDB C++; Miri interprets
MIR and cannot execute those foreign functions.

The remaining application crates are not a useful workspace-wide escape hatch. `pg-sink` and
`control` exercise real TCP and Postgres paths. The current tests carry 97 `#[ignore]` compose-gated
cases among 157 `#[tokio::test]` attributes — the 2026-08-13 figure of 131 also counted the 34
module-doc mentions of the attribute — so Miri's isolation model cannot turn the integration suite
into coverage merely by making it slower. This is why a future Miri run must start with the
specific crate and unit tests that own new first-party unsafe code.

## Why nightly is a second, independent cost

Miri requires a nightly toolchain plus a `cargo miri setup` sysroot build. Walrus instead pins
stable `1.95.0` exactly at `rust-toolchain.toml:4`, with a written reproducibility rationale; the
dedicated MSRV job at `.github/workflows/ci.yml:553-578` prevents that pin and the declared MSRV
from drifting. Adding Miri would create and maintain a second toolchain path on every push.

The project already makes build-cost choices on measured grounds. The release-profile rationale at
`Cargo.toml:203-211` adopts thin LTO but rejects `codegen-units = 1` because a few percent more
runtime performance did not justify roughly doubling the measured release build. Likewise, the
`bench` note at `justfile:32-36` keeps hardware-relative Criterion runs out of shared-runner CI.
No Miri runtime is claimed here; the known nightly installation and sysroot-build work is enough to
reject an unscoped job whose relevant first-party test set is empty.

## What walrus does instead

The always-on controls are static and effectively free: the workspace unsafe-code forbid and
Rust-2024 unsafe lints from PRs 12.1–12.4, compile-time `Send`/`Sync` assertions from PR 12.5, and
the uninitialized-memory guard and encapsulation record from PR 12.6. This PR extends that same
guard to verify both the root forbid and every manifest's lint inheritance, deriving member paths
from the workspace list so a newly added crate cannot be forgotten.

CI already runs the script before formatting, Clippy, and tests (`.github/workflows/ci.yml:125-126`,
ahead of `cargo fmt --check`, `clippy`, and `cargo test --workspace`). The inheritance half is
covered a second time by the compiled test
`crates/common/tests/workspace_lints_inherited.rs::every_member_opts_into_the_workspace_lint_table`,
which parses the same members list; the root `forbid` assertion is the script's alone, so weakening
that script is what would silently retire this decision. There is deliberately no Miri
workflow—enabled, disabled, or dispatch-only—to imply coverage that has never run green.

## Re-evaluation trigger

Reopen this decision on the first PR that needs `unsafe_code = "allow"` in any member or introduces
a first-party `unsafe extern` block. That PR must add Miri for that crate's relevant unit tests only,
never `--workspace`, and document how its FFI boundary is isolated. Also reopen it if a real defect
implicates dependency unsafe code that the static controls failed to expose.

A `-p common` experiment remains a possible starting point, not assumed coverage. Its configuration
tests use `figment::Jail` with a temporary current directory and scoped environment
(`crates/common/src/config_test.rs:44-57`), while its metrics tests install a process-wide
Prometheus recorder (`crates/common/src/metrics.rs:132-151`). Both would require investigating
`-Zmiri-disable-isolation`; neither was verified under Miri here, so this task does not adopt that
job on faith.
