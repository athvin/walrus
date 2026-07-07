# PR 4.9 — Kubernetes manifests: StatefulSets, PVC, probes, PDB

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `deploy/k8s` (+ CI) · **Est. size:** L ·
> **Depends on:** PR 4.8 · **Unlocks:** PR 4.10

Deploys both images to Kubernetes with the topology the design mandates and the hazards it explicitly
warns against. `walrus-pg-sink` and `walrus-loader` are each a **`StatefulSet` `replicas=1`** (one active
consumer of the single lifelong slot; one loader owning all `.duckdb` files). The loader gets a **RWO
PVC**. Probes are wired **exactly** as §4.3 requires — `startupProbe` gates bootstrap, `readinessProbe`
holds work, `livenessProbe` is **deadlock detection only** (never slot-lag). A **`maxUnavailable: 1`** PDB
(never `minAvailable: 1`) keeps `kubectl drain` working. `terminationGracePeriodSeconds` is set to the
measured drain (60–120s). Validated in CI with `kubeconform` / kind.

## Why — learning objectives

By the end of this PR you will have practised:

- **Single-active topology on K8s** — StatefulSet `replicas=1` as the "one consumer of one slot" guarantee,
  with Postgres itself as the real split-brain backstop.
- **Getting probes exactly right** — the three probes have three different jobs; tying liveness to slot lag
  would kill a catching-up pod into a restart loop.
- **The PDB foot-gun** — why `minAvailable: 1` on a single replica makes the pod **unevictable** and blocks
  node drains, the opposite of the self-healing goal.
- **Stateful loader storage** — a RWO PVC for the DuckDB files, and why exclusive file ownership means no
  naive HPA.

## Read first

- `../../architecture.md#kubernetes-deployment` — the topology table: StatefulSet replicas=1 for both, the
  loader PVC, probe wiring, the **`maxUnavailable: 1` / never `minAvailable: 1`** PDB rule, ConfigMap knobs,
  and the vertical-scaling constraint.
- `../../walrus-pg-sink.md#43-probes--get-these-exactly-right` — startup gates bootstrap; readiness holds
  work; liveness = **making-progress deadlock detection only**, never slot lag.
- `../../walrus-pg-sink.md#45-graceful-shutdown--the-missing-piece` — set
  `terminationGracePeriodSeconds` to the measured worst-case drain (60–120s), and **skip preStop**.
- `../../architecture.md#observability` — the ConfigMap must expose the cadence + heartbeat knobs.

## Scope

**In scope**

- A `deploy/k8s` kustomize base: a `StatefulSet` per service (`replicas=1`, Guaranteed QoS via equal
  requests/limits), the loader's `volumeClaimTemplates` (RWO PVC) for its `.duckdb` files.
- **Probes** wired to the existing endpoints: `startupProbe`→`/startup`, `readinessProbe`→`/ready`,
  `livenessProbe`→`/healthz` (progress-only). Startup suppresses liveness/readiness during a long catch-up.
- A **PodDisruptionBudget** `maxUnavailable: 1` (or none) for each — never `minAvailable: 1`.
- `terminationGracePeriodSeconds: 60–120` and **no** `preStop` hook.
- A `ConfigMap` for all knobs (sink `max_fill_ms`/`max_bytes`/`max_rows`/`max_inflight_bytes`, heartbeat
  `heartbeat_idle_after`/`heartbeat_roundtrip_deadline`, loader poll interval + compaction/retention
  cadence) and `Secret` references for DB/S3 creds (IRSA/Workload Identity, no static keys).
- `initContainers` for ordered bootstrap dependencies (e.g. wait-for control-DB migration / slot-init job).
- A CI `manifests` job running `kubeconform` (and optionally a `kind` apply-and-smoke).

**Explicitly deferred** (do *not* build these here)

- Multi-pod loader table-sharding (consistent hashing, PVC-per-replica, fencing) → a **deferred design
  goal** (PR 4.11). Manifests stay single-active.
- HPA / autoscaling — file ownership is exclusive; scaling is **vertical**. No HPA.
- Prometheus scrape config / dashboards → **PR 4.10**.

## Files to create / modify

```
deploy/k8s/base/kustomization.yaml
deploy/k8s/base/pg-sink-statefulset.yaml   # replicas=1, Guaranteed QoS, 3 probes, grace 60-120s, no preStop
deploy/k8s/base/loader-statefulset.yaml    # + volumeClaimTemplates RWO PVC for .duckdb
deploy/k8s/base/pdb.yaml                    # maxUnavailable: 1 (NEVER minAvailable: 1)
deploy/k8s/base/configmap.yaml              # all cadence + heartbeat knobs
deploy/k8s/base/secrets.example.yaml        # DB/S3 cred refs (IRSA/Workload Identity)
deploy/k8s/base/slot-init-job.yaml          # one-shot: create slot+publication+DDL trigger, record epoch
.github/workflows/ci.yml                    # modify — add the `manifests` kubeconform/kind job
```

## Skeleton

```yaml
# deploy/k8s/base/pg-sink-statefulset.yaml   (shape only)
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: walrus-pg-sink }
spec:
  replicas: 1                     # exactly one consumer of the single lifelong slot (§1.8)
  serviceName: walrus-pg-sink
  template:
    spec:
      terminationGracePeriodSeconds: 90     # measured drain (60–120); NO preStop hook
      containers:
        - name: pg-sink
          image: walrus-pg-sink:TAG
          resources:                        # Guaranteed QoS: requests == limits
            requests: { cpu: "1", memory: "1Gi" }
            limits:   { cpu: "1", memory: "1Gi" }
          startupProbe:   { httpGet: { path: /startup, port: 8080 }, failureThreshold: 60, periodSeconds: 5 }
          readinessProbe: { httpGet: { path: /ready,   port: 8080 } }
          livenessProbe:  { httpGet: { path: /healthz, port: 8080 } }   # progress-only; NEVER slot-lag
          envFrom: [ { configMapRef: { name: walrus-config } } ]
```

```yaml
# deploy/k8s/base/loader-statefulset.yaml   (adds the PVC; otherwise same shape)
spec:
  volumeClaimTemplates:
    - metadata: { name: duckdb }
      spec: { accessModes: ["ReadWriteOnce"], resources: { requests: { storage: 20Gi } } }
```

```yaml
# deploy/k8s/base/pdb.yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
spec:
  maxUnavailable: 1              # NEVER minAvailable: 1 on a single replica — it blocks kubectl drain
  selector: { matchLabels: { app: walrus-pg-sink } }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Each service is a `StatefulSet` `replicas=1` with Guaranteed QoS (requests == limits); the loader has
      a RWO `volumeClaimTemplates` PVC for its `.duckdb` files.
- [ ] All three probes are wired to the real endpoints with the correct semantics: startup gates bootstrap
      (and suppresses liveness/readiness during catch-up), readiness holds work, liveness is
      **progress/deadlock only** — **no probe references slot lag**.
- [ ] The PDB is `maxUnavailable: 1` (or absent) — **never** `minAvailable: 1` (verified: `kubectl drain`
      is not blocked).
- [ ] `terminationGracePeriodSeconds` is 60–120s and there is **no** `preStop` hook.
- [ ] The `ConfigMap` exposes every cadence + heartbeat knob; creds come from Secrets/IRSA, never inline
      static keys.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `kubeconform` validates all manifests (and, if used, a `kind` apply + `/ready` smoke passes).

## Hints & gotchas

- `minAvailable: 1` on a one-replica workload is the trap the design calls out by name: it makes the pod
  **unevictable**, so `kubectl drain` / node upgrades hang forever — the exact opposite of "self-healing
  through node drain." Use `maxUnavailable: 1` or omit the PDB.
- **Never** tie `livenessProbe` to slot lag or round-trip staleness — a pod catching up after an outage has
  high lag *by design* and would be killed into a CrashLoop. Liveness = the loop is making progress, full
  stop (§4.3).
- StatefulSet `replicas=1` is the anti-thrash guarantee; the *real* split-brain backstop is Postgres — a
  second `START_REPLICATION` on the active slot fails, so a replacement pod just retries with backoff.
- The loader PVC is RWO and single-owner — do **not** be tempted to bump `replicas` for throughput; file
  ownership is exclusive (sharding is the deferred goal in PR 4.11).
- Pin `kubeconform` to a Kubernetes API version; run it with the CRD/schema flags so custom fields don't
  false-pass.

## References

- Design: `../../architecture.md#kubernetes-deployment`, `#observability`;
  `../../walrus-pg-sink.md#43-probes--get-these-exactly-right`,
  `#45-graceful-shutdown--the-missing-piece`.
- Prev: [PR 4.8](./pr-4.8-dockerfiles.md) · Next: [PR 4.10](./pr-4.10-observability-metrics.md) ·
  [Roadmap](../README.md)
