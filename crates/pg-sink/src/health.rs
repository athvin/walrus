//! The three Kubernetes health endpoints, backed by a shared [`HealthState`] (§4.3).
//!
//! **Get the semantics exactly right** (the design flags a self-healing hazard in the master sketch):
//!
//! - `/startup` — 200 iff bootstrap is done. While it is non-200, Kubernetes runs *neither* liveness
//!   nor readiness, so a legitimately slow initial catch-up can never be killed mid-progress.
//! - `/ready`   — 200 iff `Ready` **and** not terminating; keeps the pod out of rotation otherwise.
//! - `/healthz` — liveness = **true deadlock only**. It is NOT gated on slot lag: a pod catching up
//!   after an outage has high lag *by definition*, and a lag-based liveness probe would kill it
//!   exactly when it is doing its job. High lag feeds `degraded` on readiness/health, never a kill.

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const PHASE_BOOTSTRAPPING: u8 = 0;
const PHASE_READY: u8 = 1;

/// The bootstrap phase the probes read. `Bootstrapping` gates the other two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Bootstrapping,
    Ready,
}

/// The snapshot every probe handler reads. All fields are atomics so the (future) replication loop
/// can update them without locking the probe path.
#[derive(Debug)]
pub struct HealthState {
    phase: AtomicU8,
    terminating: AtomicBool,
    degraded: AtomicBool,
    live: AtomicBool,
}

impl HealthState {
    /// A fresh state (`Bootstrapping`, live, not terminating), shared across the handlers and the loop.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the shared handle probes and loop hold
    pub fn new() -> Arc<Self> {
        Arc::new(HealthState {
            phase: AtomicU8::new(PHASE_BOOTSTRAPPING),
            terminating: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
            live: AtomicBool::new(true),
        })
    }

    /// Bootstrap finished → `/startup` and `/ready` may now answer 200.
    pub fn mark_ready(&self) {
        self.phase.store(PHASE_READY, Ordering::SeqCst);
    }

    /// SIGTERM received → drop out of rotation (`/ready` 503) while the loop drains.
    pub fn mark_terminating(&self) {
        self.terminating.store(true, Ordering::SeqCst);
    }

    /// High lag / stale heartbeat → surfaced on readiness+health, **never** a liveness kill.
    pub fn set_degraded(&self, degraded: bool) {
        self.degraded.store(degraded, Ordering::SeqCst);
    }

    /// The replication loop's deadlock detector flips this; `/healthz` reflects it.
    pub fn set_live(&self, live: bool) {
        self.live.store(live, Ordering::SeqCst);
    }

    pub fn phase(&self) -> Phase {
        match self.phase.load(Ordering::SeqCst) {
            PHASE_READY => Phase::Ready,
            _ => Phase::Bootstrapping,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.phase() == Phase::Ready && !self.terminating.load(Ordering::SeqCst)
    }

    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::SeqCst)
    }

    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }
}

async fn startup(State(state): State<Arc<HealthState>>) -> StatusCode {
    match state.phase() {
        Phase::Ready => StatusCode::OK,
        Phase::Bootstrapping => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// The `/ready` JSON body. `degraded` (stale heartbeat round-trip / high lag) is **reported, not
/// gating** — the status code follows `is_ready()` alone, so a degraded-but-catching-up sink stays in
/// rotation. Never gate readiness on `degraded` (§4.3).
#[derive(Debug, Serialize)]
struct ReadyBody {
    ready: bool,
    degraded: bool,
}

async fn ready(State(state): State<Arc<HealthState>>) -> (StatusCode, Json<ReadyBody>) {
    let ready = state.is_ready();
    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(ReadyBody {
            ready,
            degraded: state.is_degraded(),
        }),
    )
}

async fn healthz(State(state): State<Arc<HealthState>>) -> StatusCode {
    // Liveness = deadlock only. Deliberately independent of readiness/lag/degraded.
    if state.is_live() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The probe router, with the shared state injected.
pub fn router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/startup", get(startup))
        .route("/ready", get(ready))
        .route("/healthz", get(healthz))
        .with_state(state)
}

/// Serve the probes on an already-bound listener until `shutdown` is cancelled (graceful).
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    state: Arc<HealthState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

/// Bind `addr` and serve the probes (see [`serve_on`]).
pub async fn serve(
    addr: SocketAddr,
    state: Arc<HealthState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_on(listener, state, shutdown).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_gates_readiness() {
        let s = HealthState::new();
        assert_eq!(s.phase(), Phase::Bootstrapping);
        assert!(!s.is_ready());
        assert!(s.is_live(), "liveness is up from the start (deadlock-only)");

        s.mark_ready();
        assert_eq!(s.phase(), Phase::Ready);
        assert!(s.is_ready());

        // Terminating drops readiness but NOT liveness (§4.3).
        s.mark_terminating();
        assert!(!s.is_ready());
        assert!(s.is_live());
    }

    #[test]
    fn degraded_does_not_affect_liveness() {
        let s = HealthState::new();
        s.mark_ready();
        s.set_degraded(true);
        assert!(s.is_degraded());
        assert!(s.is_live(), "high lag must never fail liveness");
    }
}
