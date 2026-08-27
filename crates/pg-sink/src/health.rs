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

use anyhow::Context as _;
use axum::{
    Json, Router, extract::State, http::StatusCode, http::header, response::IntoResponse,
    routing::get,
};
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use tokio_util::sync::CancellationToken;

/// The bootstrap phase the probes read. `Bootstrapping` gates the other two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Phase {
    #[default]
    Bootstrapping = 0,
    Ready = 1,
}

/// An out-of-range pg-sink phase byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPhase(pub u8);

impl TryFrom<u8> for Phase {
    type Error = InvalidPhase;

    /// Decode a phase byte read back out of the atomic the probes share.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPhase`] carrying `byte` for anything other than `0`
    /// ([`Phase::Bootstrapping`]) or `1` ([`Phase::Ready`]). Only this module's typed stores write
    /// that atomic, so an unknown byte is a bug here rather than a probe input.
    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        // AtomicU8::default(), typed stores, and this decoder all rely on these exact bytes.
        const {
            assert!(
                Phase::Bootstrapping as u8 == 0,
                "Phase::Bootstrapping must stay byte 0 because AtomicPhase defaults to zero"
            );
            assert!(
                Phase::Ready as u8 == 1,
                "Phase::Ready must stay byte 1 so AtomicPhase store and decode agree"
            );
        }

        match byte {
            0 => Ok(Self::Bootstrapping),
            1 => Ok(Self::Ready),
            other => Err(InvalidPhase(other)),
        }
    }
}

/// A [`Phase`] stored atomically. Only enum values can be written through this wrapper.
#[derive(Debug, Default)]
struct AtomicPhase(AtomicU8);

impl AtomicPhase {
    fn store(&self, phase: Phase) {
        self.0.store(phase as u8, Ordering::SeqCst);
    }

    fn load(&self) -> Phase {
        let byte = self.0.load(Ordering::SeqCst);
        Phase::try_from(byte).unwrap_or_else(|InvalidPhase(invalid)| {
            tracing::error!(
                phase_byte = invalid,
                "invalid pg-sink health phase byte; defaulting to bootstrapping"
            );
            Phase::Bootstrapping
        })
    }
}

/// The snapshot every probe handler reads. All fields are atomics so the (future) replication loop
/// can update them without locking the probe path.
#[derive(Debug)]
pub struct HealthState {
    phase: AtomicPhase,
    terminating: AtomicBool,
    degraded: AtomicBool,
    live: AtomicBool,
}

impl HealthState {
    /// A fresh state (`Bootstrapping`, live, not terminating), shared across the handlers and the loop.
    #[allow(
        clippy::new_ret_no_self,
        reason = "intentionally returns the shared handle used by probes and the loop"
    )]
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(HealthState {
            phase: AtomicPhase::default(),
            terminating: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
            live: AtomicBool::new(true),
        })
    }

    /// Bootstrap finished → `/startup` and `/ready` may now answer 200.
    pub fn mark_ready(&self) {
        self.phase.store(Phase::Ready);
    }

    /// SIGTERM received → drop out of rotation (`/ready` 503) while the loop drains.
    pub fn mark_terminating(&self) {
        // Release publishes that SIGTERM was observed and the replication loop is draining.
        self.terminating.store(true, Ordering::Release);
    }

    /// High lag / stale heartbeat → surfaced on readiness+health, **never** a liveness kill.
    pub fn set_degraded(&self, degraded: bool) {
        // Relaxed: report-only flag; no payload is published and no behavior is gated on it.
        self.degraded.store(degraded, Ordering::Relaxed);
    }

    /// The replication loop's deadlock detector flips this; `/healthz` reflects it.
    pub fn set_live(&self, live: bool) {
        self.live.store(live, Ordering::Relaxed); // independent deadlock latch; no payload
    }

    pub fn phase(&self) -> Phase {
        self.phase.load()
    }

    pub fn is_ready(&self) -> bool {
        // Acquire pairs with mark_terminating; phase and termination are independent probe latches.
        self.phase() == Phase::Ready && !self.terminating.load(Ordering::Acquire)
    }

    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Relaxed) // independent deadlock latch; no payload
    }

    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed) // report-only; no payload is observed
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

/// The Prometheus text exposition (PR 4.10). No state — it reads the process-wide recorder, so it
/// lives happily on the same router as the state-backed probes.
async fn metrics() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        common::metrics::render(),
    )
}

/// The probe + metrics router, with the shared state injected.
pub fn router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/startup", get(startup))
        .route("/ready", get(ready))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Serve the probes on an already-bound listener until `shutdown` is cancelled (graceful).
///
/// # Errors
///
/// Returns [`anyhow::Error`] if Axum fails while accepting or serving a connection on `listener`.
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    state: Arc<HealthState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .context("serve health endpoints")?;
    Ok(())
}

/// Bind `addr` and serve the probes (see [`serve_on`]).
///
/// # Errors
///
/// Returns [`anyhow::Error`] if the address cannot be bound or [`serve_on`] fails while serving.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<HealthState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind health endpoints on {addr}"))?;
    serve_on(listener, state, shutdown).await
}

#[cfg(test)]
#[path = "health_test.rs"]
mod tests;
