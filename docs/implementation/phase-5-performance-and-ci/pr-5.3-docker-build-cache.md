# PR 5.3 — Docker image builds: BuildKit cache mounts + GHA layer cache

> **Phase:** 5 — Performance & CI · **Crates touched:** none (`deploy/docker/*`, CI) ·
> **Est. size:** M · **Depends on:** PR 5.2 · **Unlocks:** PR 5.4

The `images` job is the last stronghold of the cold DuckDB build: both Dockerfiles do `COPY . .`
then `cargo build --release`, so **every** CI run recompiles everything from scratch inside the
builder stage — the loader image alone costs ~20–30 min, unconditionally, because the Docker build
is isolated from both the runner's cargo cache and (after PR 5.2) sccache. Fix it with BuildKit
`RUN --mount=type=cache` for the cargo registry and target dir, plus `docker/build-push-action`
with the GHA cache backend so image layers persist across runs. Runtime images are unchanged —
same bases, same `tini` PID-1 entrypoints, same smoke test.

## Why — learning objectives

By the end of this PR you will have practised:

- **Why Docker builds don't cache cargo** — layer caching invalidates at the first changed `COPY`,
  and build containers see none of the host's caches; `--mount=type=cache` gives a RUN step a
  persistent directory that survives across builds without entering the image.
- **The cache-mount + binary-copy dance** — artifacts built inside a cache mount are not in the
  image layer; you must copy the binary out of the mount inside the same RUN step.
- **Remote layer cache** — `docker/build-push-action` with `cache-from`/`cache-to: type=gha`, and
  `mode=max` vs default (whether intermediate stages are cached).

## Read first

- `deploy/docker/Dockerfile.pg-sink` and `Dockerfile.loader` — current two-stage shape, the
  `SQLX_OFFLINE=true` env, and the loader's `build-essential` + runtime `libstdc++6`.
- `.github/workflows/ci.yml` `images` job + `scripts/image-smoke.sh` — the PID-1 SIGTERM contract
  the rebuilt images must still satisfy.
- External: Docker docs "Build cache — cache mounts"; `docker/setup-buildx-action` and
  `docker/build-push-action` READMEs (gha cache backend); note GHA cache's 10 GB/repo ceiling is
  shared with PR 5.2's sccache entries.

## Scope

**In scope**

- Rework both Dockerfiles' builder stages to:

  ```dockerfile
  RUN --mount=type=cache,target=/usr/local/cargo/registry \
      --mount=type=cache,target=/src/target \
      cargo build --release -p loader --bin walrus-loader \
   && cp target/release/walrus-loader /out/walrus-loader
  ```

  (binary copied out of the cache mount; the later `COPY --from=builder` points at `/out`).
- Switch the `images` job to `docker/setup-buildx-action` + `docker/build-push-action` with
  `push: false`, `load: true`, `cache-from: type=gha` / `cache-to: type=gha,mode=max`, and a
  per-image `scope` so sink and loader don't evict each other.
- Keep tags `walrus-pg-sink:ci` / `walrus-loader:ci` so `scripts/image-smoke.sh` runs unchanged.
- Record before/after `images` job times for (a) no-op rebuild, (b) source-only change (the common
  case — deps cached, only workspace crates recompile).

**Explicitly deferred** (do *not* build these here)

- cargo-chef-style dependency pre-cooking — the cache mounts already keep dependency artifacts warm;
  add chef only if mount caching proves insufficient.
- Publishing images to a registry (GHCR) — the curriculum builds and smokes but does not ship.
- sccache inside the builder — optional; only wire it if the cache mounts alone don't get the
  loader image under ~5 min warm (it needs network egress + the GHA cache URL/token passed as
  build secrets, which is real complexity).

## Files to create / modify

```
deploy/docker/Dockerfile.pg-sink     # modify — cache mounts, /out copy
deploy/docker/Dockerfile.loader      # modify — cache mounts, /out copy
.github/workflows/ci.yml             # modify — buildx + build-push-action with gha cache
docs/implementation/README.md        # modify — CI-grows table row for 5.3
```

## Skeleton

```yaml
# .github/workflows/ci.yml  (images job — shape only)
  images:
    needs: changes
    if: needs.changes.outputs.code == 'true'
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/free-disk-space
      - uses: docker/setup-buildx-action@v3
      - name: Build walrus-pg-sink image
        uses: docker/build-push-action@v6
        with:
          context: .
          file: deploy/docker/Dockerfile.pg-sink
          tags: walrus-pg-sink:ci
          load: true
          cache-from: type=gha,scope=pg-sink
          cache-to: type=gha,scope=pg-sink,mode=max
      # ... same for loader, scope=loader ...
      - name: PID-1 SIGTERM smoke
        run: bash scripts/image-smoke.sh
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Both Dockerfiles build with BuildKit cache mounts; the release binaries are copied out of the
      mount inside the build RUN (an image built with a cold cache and one built warm are
      byte-equivalent in their final stage).
- [ ] The `images` job uses buildx with `type=gha` cache; a re-run with **only a source-file
      change** rebuilds workspace crates but not dependencies (and not DuckDB), finishing in
      single-digit minutes. Before/after numbers in the PR description.
- [ ] `scripts/image-smoke.sh` passes unchanged: both images boot, reach their ready states, and
      exit 0 on SIGTERM (tini is still PID 1).
- [ ] Runtime stages unchanged: same base images, same installed packages
      (`tini`, `ca-certificates`, loader's `libstdc++6`), same entrypoints.
- [ ] **Green locally and in CI:** full workflow green on the PR itself; local
      `docker build` of both files still works without buildx-specific flags (BuildKit is the
      default builder in current Docker).

## Hints & gotchas

- **Everything in a cache mount vanishes from the image.** The classic failure: `cargo build`
  into a mounted `target/`, then a later `COPY --from=builder /src/target/release/...` — which
  no longer exists. Copy the binary to a non-mounted path (`/out`) *inside the same RUN*.
- Cache mounts are keyed by target path + platform. Two images sharing
  `/usr/local/cargo/registry` mounts is fine (both benefit); sharing `/src/target` between
  sink and loader builds is also fine — they're the same workspace, and the mount is
  concurrency-guarded by BuildKit (default `sharing=shared` is OK for cargo since 5.x uses
  file locks; use `sharing=locked` if you see corruption).
- `mode=max` caches intermediate stages (the builder!) — without it only the final stage's layers
  are cached, which is nearly useless here.
- The GHA cache backend + sccache (PR 5.2) share the repo's 10 GB budget; use scopes and watch for
  eviction churn in cache analytics before assuming a regression.
- `SQLX_OFFLINE=true` stays — the builder has no database; the `.sqlx` offline cache in the repo is
  what makes `sqlx::query!` compile.

## References

- Design: `../README.md` ("CI grows with the phases"); `docs/walrus-pg-sink.md` §4.5 (why PID-1
  SIGTERM handling exists — the smoke this PR must not break).
- Prev: [PR 5.2](./pr-5.2-ci-sccache.md) · Next: [PR 5.4](./pr-5.4-bench-sink-decode-arrow.md) ·
  [Roadmap](../README.md)
