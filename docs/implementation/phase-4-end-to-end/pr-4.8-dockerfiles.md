# PR 4.8 — Multi-stage Dockerfiles with PID-1 SIGTERM for both binaries

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `deploy/docker` (+ CI) ·
> **Est. size:** M · **Depends on:** PR 4.7 · **Unlocks:** PR 4.9

Packages `walrus-pg-sink` and `walrus-loader` as small, reproducible container images with a **multi-stage
build** (cargo-chef-cached compile stage → slim runtime) and — the load-bearing detail — an **exec-form
entrypoint under `tini`** so the Rust process receives `SIGTERM` as **PID 1**. Everything the graceful
drains in PRs 2.28 / 3.12 do is worthless if a shell entrypoint swallows the signal and Kubernetes
`SIGKILL`s the pod with an in-flight batch. This PR makes the signal reach the process. It also adds a CI
image-build job.

## Why — learning objectives

By the end of this PR you will have practised:

- **The PID-1 signal problem** — why a non-exec shell entrypoint eats `SIGTERM`, and how `tini` / exec form
  fixes it so the drain code actually runs.
- **Multi-stage, cached Rust images** — a builder stage (cargo-chef or a manual dependency-cache layer)
  producing a static-ish binary, copied into a minimal runtime (distroless / debian-slim).
- **Building a `duckdb`-bundled binary in a container** — the runtime needs the right libc / CA certs; the
  bundled DuckDB is compiled in the builder stage.
- **`docker run` SIGTERM verification** — proving the signal reaches PID 1 before you ever touch K8s.

## Read first

- `../../walrus-pg-sink.md#45-graceful-shutdown--the-missing-piece` — the PID-1 / exec-form / `tini`
  requirement and why a shell entrypoint swallows `SIGTERM` (the whole reason this PR exists).
- `../../architecture.md#kubernetes-deployment` — the images are `walrus-pg-sink` / `walrus-loader`; the
  crate names (`pg-sink` / `loader`) are the build targets.
- `../../walrus-pg-sink.md#4-kubernetes-pod-lifecycle` — the termination sequence the container must honour
  (SIGTERM at T=0, no preStop).

## Scope

**In scope**

- `deploy/docker/Dockerfile.pg-sink` and `deploy/docker/Dockerfile.loader`: multi-stage (builder →
  runtime), building the respective binary release with the `duckdb` bundled feature where needed.
- **PID-1 SIGTERM:** `ENTRYPOINT` in **exec form** under `tini` (`--init` equivalent), so the binary is
  PID 1 (or a proper init reaps it) and receives `SIGTERM` directly.
- A `.dockerignore` to keep the build context small and cache-friendly.
- A CI `images` job that builds both images (and optionally loads them for a `docker run` smoke).
- A `docker run` smoke: start each image, send `SIGTERM`, assert the process handles it and exits 0 (not
  `SIGKILL`ed) within a bounded time.

**Explicitly deferred** (do *not* build these here)

- StatefulSets / probes / PDB / PVC / `terminationGracePeriodSeconds` → **PR 4.9** (this PR only makes the
  signal reach the process; the grace-period tuning lives in the manifests).
- Pushing images to a registry / release tagging — CI builds them, publishing is out of scope.
- Metrics endpoint wiring → **PR 4.10**.

## Files to create / modify

```
deploy/docker/Dockerfile.pg-sink     # new — multi-stage; exec-form ENTRYPOINT under tini
deploy/docker/Dockerfile.loader      # new — multi-stage; exec-form ENTRYPOINT under tini
.dockerignore                        # new — trim the build context (target/, .git, docs, …)
.github/workflows/ci.yml             # modify — add the `images` build (+ docker run SIGTERM smoke)
```

## Skeleton

```dockerfile
# deploy/docker/Dockerfile.pg-sink  (shape only — fill in pins)

# --- builder: compile the release binary (duckdb bundled where needed) ---
FROM rust:1.XX-slim AS builder
WORKDIR /src
# TODO: cargo-chef (or manual deps-cache layer) so dependency compiles cache across builds.
COPY . .
RUN cargo build --release -p pg-sink

# --- runtime: minimal, with tini as init + CA certs ---
FROM debian:stable-slim AS runtime          # or gcr.io/distroless/cc + tini
RUN apt-get update && apt-get install -y --no-install-recommends tini ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/pg-sink /usr/local/bin/pg-sink
# EXEC FORM entrypoint under tini → the binary is PID 1's child and gets SIGTERM directly.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/pg-sink"]
```

```dockerfile
# deploy/docker/Dockerfile.loader
# ... identical shape; build/copy `loader`; ENTRYPOINT ["/usr/bin/tini","--","/usr/local/bin/loader"]
```

```bash
# CI smoke (shape): SIGTERM must reach PID 1 and exit cleanly, not SIGKILL.
#   docker run -d --name s walrus-pg-sink:ci
#   docker stop --time 90 s        # sends SIGTERM, waits up to 90s before SIGKILL
#   test "$(docker inspect -f '{{.State.ExitCode}}' s)" = "0"
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Both images build in CI from the multi-stage Dockerfiles; the runtime image is slim (no build
      toolchain) and includes CA certs.
- [ ] The `ENTRYPOINT` is **exec form** under `tini`; a `docker run` + `SIGTERM` (via `docker stop`) makes
      each process **handle the signal and exit 0** within the grace window — it is **not** `SIGKILL`ed.
- [ ] `.dockerignore` excludes `target/`, `.git`, and docs so the build context is small and layer-cached.
- [ ] The `duckdb`-bundled binary (loader) runs in the runtime image (correct libc, no missing shared
      lib).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `docker build -f deploy/docker/Dockerfile.pg-sink .` and `…Dockerfile.loader .` succeed, then the
        `docker run` **SIGTERM-reaches-PID-1** smoke exits 0.

## Hints & gotchas

- The classic trap: `ENTRYPOINT sh -c "pg-sink"` (shell/JSON-array-with-shell) makes the shell PID 1 and it
  does **not** forward `SIGTERM` — the pod is `SIGKILL`ed and the drain never runs. Use exec form under
  `tini` (or `docker run --init` in the smoke).
- Even with exec form, if your binary spawns children (it shouldn't need to), `tini` reaps zombies — keep
  it as the init to be safe.
- Match the builder base to the pinned toolchain / MSRV from PR 4.7, and match glibc between builder and
  runtime (a musl static build avoids this but must still satisfy DuckDB's bundled C++ — verify it runs).
- Keep the two Dockerfiles nearly identical; the only real difference is the build target and binary name.
  Consider a shared base to avoid drift.
- Do **not** add a `preStop` hook here — the design says skip it for these non-serving consumers so the
  full grace budget goes to the drain (that decision is realised in PR 4.9's manifests).

## References

- Design: `../../walrus-pg-sink.md#45-graceful-shutdown--the-missing-piece`,
  `#4-kubernetes-pod-lifecycle`; `../../architecture.md#kubernetes-deployment`.
- Prev: [PR 4.7](./pr-4.7-ci-cargo-deny.md) · Next: [PR 4.9](./pr-4.9-kubernetes-manifests.md) ·
  [Roadmap](../README.md)
