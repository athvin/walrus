# PR 4.7 — Supply-chain CI: `cargo-deny` + a documented MSRV

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** workspace root (CI + `deny.toml`) ·
> **Est. size:** S · **Depends on:** PR 4.6 · **Unlocks:** PR 4.8

Adds the supply-chain gate the README's "CI grows with the phases" table promises from PR 4.7: a
`deny.toml` plus a CI job running `cargo deny check` across **advisories** (RUSTSEC), **licenses**
(allow-list), **bans** (duplicate / disallowed crates), and **sources** (only crates.io + pinned git). It
also pins and documents the project's **MSRV** so a toolchain drift is a CI failure, not a mystery build
break. Almost entirely config — no application logic.

## Why — learning objectives

By the end of this PR you will have practised:

- **Supply-chain hygiene as CI** — turning "we should audit dependencies" into an enforced gate that fails
  the build on a new advisory or an unapproved license.
- **`cargo-deny` configuration** — the four check families and how to express an allow-list, a git-source
  allow, and a duplicate-version policy without over-blocking.
- **MSRV discipline** — a `rust-version` in the workspace manifest, mirrored by the pinned
  `rust-toolchain.toml`, verified in CI.

## Read first

- `../README.md` — the **"CI grows with the phases"** table: PR 4.7 adds `cargo-deny` (licenses /
  advisories / bans / sources) and a documented MSRV.
- `../../architecture.md#sources` — the dependency set (`tokio`, `arrow`/`parquet`, `object_store`,
  `duckdb` bundled, `sqlx`, `tokio-postgres`, …) whose licenses the allow-list must cover.
- External: the `cargo-deny` book (advisories / licenses / bans / sources sections).

## Scope

**In scope**

- `deny.toml` at the workspace root configuring all four checks: `advisories` (deny vulnerabilities /
  unmaintained), `licenses` (SPDX allow-list covering the actual tree), `bans` (duplicate-version policy,
  optional skip list with justification), `sources` (crates.io + any pinned git allow).
- A CI job `supply-chain` running `cargo deny check` (installed via a pinned action or `cargo install`
  with a version pin), wired into the existing workflow.
- MSRV: set `rust-version` in the workspace `Cargo.toml`, document it in `../README.md`, and add a CI step
  (or matrix leg) that builds on the MSRV toolchain.

**Explicitly deferred** (do *not* build these here)

- Image builds / `kubeconform` → **PRs 4.8–4.9**. The metrics scrape job → **PR 4.10**.
- Automated dependency-update bots (dependabot/renovate) — out of scope for the curriculum.

## Files to create / modify

```
deny.toml                            # new — advisories / licenses / bans / sources
Cargo.toml                           # modify — [workspace.package] rust-version = "1.XX"
.github/workflows/ci.yml             # modify — add the `supply-chain` job + an MSRV build leg
docs/implementation/README.md        # modify — record the MSRV in the CI-grows table row
```

## Skeleton

```toml
# deny.toml  (fill in the specifics for the real dependency tree)

[advisories]
# deny known-vulnerable / unmaintained crates; keep an audited, justified ignore list (ideally empty)
ignore = []

[licenses]
# SPDX allow-list covering the actual tree (MIT / Apache-2.0 / BSD-3-Clause / Unicode-3.0 / …).
allow = [ /* TODO: enumerate from `cargo deny check licenses` output */ ]
confidence-threshold = 0.9

[bans]
# fail on multiple versions of the same crate unless explicitly skipped with a reason
multiple-versions = "warn"   # tighten to "deny" once the tree is de-duplicated
skip = []                    # each entry needs a justifying comment

[sources]
unknown-registry = "deny"
unknown-git = "deny"
allow-git = [ /* any pinned git dep, e.g. a patched arrow/pgwire fork, with a comment */ ]
```

```yaml
# .github/workflows/ci.yml  (new job — shape only)
  supply-chain:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: cargo-deny
        uses: EmbarkStudios/cargo-deny-action@v2   # pinned; or `cargo install cargo-deny --locked --version X`
        with:
          command: check advisories licenses bans sources
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `cargo deny check advisories licenses bans sources` passes locally on the current tree.
- [ ] The `licenses` allow-list covers **every** crate in the tree (no `unlicensed`/unknown), and each
      entry is a real SPDX id — no blanket `allow-osi-fsf-free = "both"` shortcut.
- [ ] The `sources` check denies unknown registries/git; any git dep is explicitly allow-listed with a
      justifying comment.
- [ ] The workspace declares a `rust-version` (MSRV) matching `rust-toolchain.toml`; CI builds on the MSRV
      and the value is recorded in `../README.md`.
- [ ] The `supply-chain` CI job is wired in and green.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `cargo deny check` (new gate, must be green)

## Hints & gotchas

- Generate the license allow-list from reality: run `cargo deny check licenses` once, read the failures,
  and add exactly the SPDX ids present. Do not paste a maximal list — an over-broad allow-list defeats the
  gate.
- `duckdb` bundled compiles vendored C/C++ — confirm its (and any transitive) license is covered; the
  bundled SQLite/DuckDB licensing is the classic surprise here.
- Start `bans.multiple-versions` at `"warn"` so the job goes green immediately, then tighten to `"deny"`
  in a follow-up once you've de-duplicated — don't block this PR on tree hygiene.
- Pin the `cargo-deny` version (action tag or `--version`) so a new release can't silently change the gate
  under you.
- MSRV must be a version the pinned dependencies actually support (arrow-rs / sqlx have their own MSRVs) —
  set it to the max of those, not an aspirational old one.

## References

- Design: `../README.md` ("CI grows with the phases"); `../../architecture.md#sources`.
- Prev: [PR 4.6](./pr-4.6-total-restart-epoch.md) · Next: [PR 4.8](./pr-4.8-dockerfiles.md) ·
  [Roadmap](../README.md)
