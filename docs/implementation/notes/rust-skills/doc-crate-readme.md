# `include_str!("../README.md")` has no crate to unify

> **Status:** decided — audited repo-wide, no source or manifest change. The rule's precondition
> (a per-crate `README.md` competing with a crate-root doc comment) does not exist here, and both of
> its rendering targets beyond GitHub are structurally unreachable.

## What the rule asks for

`doc-crate-readme` asks for two things per crate:

1. `#![doc = include_str!("../README.md")]` at the crate root, so the README is the single source of
   truth for the rustdoc front page instead of a second doc comment that drifts from it.
2. `readme = "README.md"` (and `documentation = "https://docs.rs/…"`) in `Cargo.toml`, so crates.io
   serves the same file.

Its stated payoff is "one file, three consistent rendering targets — GitHub, crates.io, and
docs.rs."

## What is actually true here

Measured across the whole tree:

- The workspace has **six** members (`Cargo.toml:3`): `crates/common`, `crates/control`,
  `crates/loader`, `crates/pg-sink`, `crates/pg-to-arrow`, `tests/e2e`.
- **Zero** of them contain a `README.md`. The repository has four in total, and none sits in a
  package root: `README.md` (the product landing page), `docs/implementation/README.md`,
  `docs/implementation/phase-8-cleanup/README.md`, `docs/examples/proto-version/README.md`.
- **Zero** `#![doc = include_str!(…)]` sites exist. Every `include_str!` in the tree pulls SQL
  templates, migrations, sibling source, or a decision note into a guard — the markdown ones
  (`mem-arrayvec.md`, `api-serde-optional.md`, the three `opt-*.md`) are read as test fixtures to be
  asserted against, never as a doc attribute to be rendered.
- **Zero** manifests declare `readme`, `documentation`, `description`, or `repository`.
- Every crate root already carries a `//!` block: `crates/common/src/lib.rs:9`,
  `crates/control/src/lib.rs:1`, `crates/loader/src/lib.rs:13`, `crates/pg-sink/src/lib.rs:9`,
  `crates/pg-to-arrow/src/lib.rs:1`, `tests/e2e/src/lib.rs:12`, plus both binary roots
  (`crates/pg-sink/src/main.rs:1`, `crates/loader/src/main.rs:8`).
- Three of those roots resolve **15** intra-doc links between them — `common` 6 (`[`Lsn`]`,
  `[`ids`]`, `[`SinkMeta`]`, `[`TypeDescriptor`]`, `[`CommonConfig`]`), `loader` 4 (`[`bootstrap`]`,
  `[`lease`]`, `[`health`]`, `[`duck`]`), `pg-sink` 5 (`[`pgoutput`]`, `[`config`]`,
  `[`bootstrap`]`, `[`health`]`, `[`shutdown`]`).
- The root `README.md` is 43 lines with **zero** fenced code blocks and **six** repo-relative links
  (`docs/architecture.md` twice, `docs/walrus-pg-sink.md`, `docs/walrus-loader.md`,
  `docs/proto-version.md`, `LICENSE`).

So there is no README/`//!` pair anywhere in this workspace, and therefore none of the drift the
rule exists to prevent. The one README and the six crate roots describe different things: the README
is a product and architecture overview aimed at a GitHub visitor, and each `//!` is a module map
aimed at someone reading that crate's API.

## The manifest half is already answered

`publish = false` at `Cargo.toml:14` is a stated decision, not an omission, and
`crates/common/tests/publish_policy.rs` fails when a member stops inheriting it. That disposes of
two of the rule's three rendering targets: nothing here reaches crates.io, and docs.rs only builds
what crates.io serves. `readme` is documented as a path *within* the package root; the only README
walrus owns sits two directories above every member, so the key could only be spelled as an escaping
`../../README.md` — pointing at a file for a landing page that will never be rendered.

`documentation = "https://docs.rs/…"` would name a URL that returns 404 for the same reason.

CI never builds the remaining target either: `.github/workflows/ci.yml` has no `cargo doc`,
`rustdoc`, or `RUSTDOCFLAGS` step. Rustdoc HTML here is a local `cargo doc` for a developer reading
one crate at a time.

## Why including the root README would cost more than it saves

Adding `#![doc = include_str!("../../README.md")]` to the five library roots would:

- Print the same product overview as the front page of five different crates, so `common`'s front
  page would open by explaining the sink's flush limits and the loader's `MERGE` collapse — none of
  which `common` contains.
- Break all six of the README's repo-relative links in the generated HTML. Rustdoc emits a plain
  relative markdown link as a relative `href` against the rendered page, so `docs/architecture.md`
  on `target/doc/common/index.html` resolves to `target/doc/common/docs/architecture.md` and 404s.
  Nothing catches this: `rustdoc::broken_intra_doc_links` only inspects intra-doc link syntax, so
  the workspace `warnings = "deny"` never sees a dead relative URL.
- Reach outside the package directory for a build input, which is exactly the shape that stops
  working the day a member becomes publishable.

The README's own code blocks are not the problem today — it has none — but `cargo test --workspace`
(`ci.yml:148`) does run lib doctests, so including it would silently make the product landing page a
doctest input, and the next contributor to paste a SQL or shell snippet into it would break CI from
a markdown file. The rule anticipates this and answers it with language tags; the point is only that
the include buys a maintenance obligation it does not repay here.

## Why per-crate READMEs are the wrong direction here

The other way to satisfy the rule literally is to move each `//!` block into a new
`crates/<name>/README.md` and include it back. That inverts the trade:

- The 15 intra-doc links above resolve under rustdoc whether they arrive via `//!` or via
  `#![doc = include_str!]`, but they render on GitHub as literal `[bootstrap]`, `[Lsn]`,
  `[pgoutput]` text with no target. GitHub is the *only* one of the rule's three surfaces walrus
  actually has, so the conversion degrades the surface that exists to serve two that do not.
- It creates five new documents whose sole purpose is to be read back into the file they came from,
  and moves crate documentation out of the file that Clippy, `deny(warnings)`, and every
  `include_str!`-based guard in `crates/common/tests/` already watch.

## The one real drift this audit found, and why it is not fixed here

`README.md:9-11` still reads "**Status:** design phase. The architecture sketch is in
`docs/architecture.md` — read and critique it before any code lands." That is stale against a tree
with two running binaries, a compose-gated e2e harness, and a control-plane migration set. It is
genuine README-versus-reality drift and it is the failure mode this rule cares about — but it is
prose in the project's public landing page, not a `doc-crate-readme` mechanism, and no
`include_str!` would have prevented it. Rewriting the front page is a change that should be made
deliberately by whoever owns the project's public framing, not folded into a documentation-plumbing
audit.

## What would reverse this

Any one of these makes the rule apply as written, for the affected crate only:

- A member drops `publish.workspace = true` and becomes a real crates.io package. It then needs
  `readme`, `documentation`, and the full `doc-cargo-metadata` field set, and it needs a README that
  lives in its own package root.
- A crate grows a `README.md` of its own for any reason. At that moment there are two documents
  describing one crate, and they must be unified with `#![doc = include_str!("../README.md")]`
  rather than maintained in parallel.
- The root README is split so that each member's section becomes a standalone document in that
  member's directory. That would create the per-crate READMEs this note says do not exist, and the
  include is then the correct way to stop them drifting from the roots.

## See also

- `docs/implementation/notes/rust-skills/api-serde-optional.md` — the same shape of argument: a rule
  whose precondition the tree does not meet, recorded rather than force-fitted.
- `crates/common/tests/publish_policy.rs` — the guard that keeps `publish = false` true, and with it
  the crates.io/docs.rs half of this decision.
