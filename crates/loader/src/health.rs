//! The loader's K8s health endpoints (loader §8.3) — **the catch-up-lag trap avoided**.
//!
//! - `/startup` — 200 once bootstrap completes (gates the slow lease+DuckDB open).
//! - `/ready`   — 200 iff bootstrap done (leases held + files open) **and not quarantined**. Never
//!   gated on "backlog drained": a legitimately-behind loader is still *ready*; gating on lag flaps a
//!   busy pod out. A **quarantined** table (a failed lossy DDL cast, PR 3.9) degrades `/ready` — a loud,
//!   terminal signal, not a silent continue.
//! - `/healthz` — liveness = *progress*, read from an in-memory `last_poll_completed_at` stamped every
//!   cycle (even a no-op). It reflects **no** lag metric — an idle-but-healthy loader must stay live.

use axum::{
    Router, extract::State, http::StatusCode, http::header, response::IntoResponse, routing::get,
};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// The loader's health lifecycle — exactly one of three states.
///
/// `Quarantined` implies bootstrap finished: its producer is a failed lossy DDL cast in the apply
/// loop, which cannot run before bootstrap. That implication keeps `/startup` satisfied while
/// `/ready` degrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LoaderPhase {
    #[default]
    Bootstrapping = 0,
    Ready = 1,
    /// Latched by a failed lossy DDL cast. A reload rebuild is its only exit.
    Quarantined = 2,
}

/// An out-of-range loader phase byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPhase(pub u8);

impl TryFrom<u8> for LoaderPhase {
    type Error = InvalidPhase;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0 => Ok(Self::Bootstrapping),
            1 => Ok(Self::Ready),
            2 => Ok(Self::Quarantined),
            other => Err(InvalidPhase(other)),
        }
    }
}

/// A [`LoaderPhase`] stored atomically. Only enum values can be written through this wrapper.
#[derive(Debug, Default)]
struct AtomicPhase(AtomicU8);

impl AtomicPhase {
    fn store(&self, phase: LoaderPhase) {
        self.0.store(phase as u8, Ordering::SeqCst);
    }

    fn load(&self) -> LoaderPhase {
        let byte = self.0.load(Ordering::SeqCst);
        LoaderPhase::try_from(byte).unwrap_or_else(|InvalidPhase(invalid)| {
            tracing::error!(
                phase_byte = invalid,
                "invalid loader health phase byte; defaulting to bootstrapping"
            );
            LoaderPhase::Bootstrapping
        })
    }

    fn transition(&self, from: LoaderPhase, to: LoaderPhase) -> bool {
        self.0
            .compare_exchange(from as u8, to as u8, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

#[derive(Debug, Default)]
pub struct LoaderState {
    phase: AtomicPhase,
    /// The end of the last poll cycle — liveness proof, NOT a lag metric. `None` until bootstrap ends.
    // LOCK-CHOICE: parking_lot::Mutex — poll-cycle writes dominate the one-expression kubelet read; see docs/implementation/notes/rust-skills/own-rwlock-readers.md.
    last_poll_completed_at: Mutex<Option<Instant>>,
}

impl LoaderState {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(LoaderState::default())
    }

    /// Bootstrap finished: leases held + files open → `/startup` and `/ready` answer 200.
    pub fn mark_ready(&self) {
        self.phase.store(LoaderPhase::Ready);
    }

    /// `/startup` gate: bootstrap finished, including a later quarantine.
    pub fn is_started(&self) -> bool {
        matches!(
            self.phase.load(),
            LoaderPhase::Ready | LoaderPhase::Quarantined
        )
    }

    /// `/ready` answers 200 only in the ready phase.
    pub fn is_ready(&self) -> bool {
        matches!(self.phase.load(), LoaderPhase::Ready)
    }

    /// Latch the quarantine flag — a failed lossy DDL cast (PR 3.9). `/ready` degrades and stays
    /// degraded; the caller also logs an error-level alert and exits. Since PR 6.7 the latch has
    /// exactly one exit: a single-table-reload rebuild, which REPLACES the data instead of
    /// retrying the cast on it ([`LoaderState::clear_quarantine`]).
    pub fn quarantine(&self) {
        self.phase.store(LoaderPhase::Quarantined);
    }

    /// The one legitimate quarantine exit (PR 6.7): a reload rebuild just recreated the table at
    /// the attempt's schema_version, so the lossy cast the latch recorded no longer applies to
    /// anything — `/ready` recovers.
    pub fn clear_quarantine(&self) {
        let _transitioned = self
            .phase
            .transition(LoaderPhase::Quarantined, LoaderPhase::Ready);
    }

    pub fn is_quarantined(&self) -> bool {
        matches!(self.phase.load(), LoaderPhase::Quarantined)
    }

    /// Stamp progress — called at the end of **every** poll cycle (and once at bootstrap end so an
    /// idle loader stays live).
    pub fn stamp_poll(&self) {
        *self.last_poll_completed_at.lock() = Some(Instant::now());
    }

    /// Liveness = we have completed at least one cycle (progress stamped). Deliberately lag-free.
    pub fn is_live(&self) -> bool {
        self.last_poll_completed_at.lock().is_some()
    }
}

async fn startup(State(s): State<Arc<LoaderState>>) -> StatusCode {
    ok_or_unavailable(s.is_started())
}
async fn ready(State(s): State<Arc<LoaderState>>) -> StatusCode {
    ok_or_unavailable(s.is_ready())
}
async fn healthz(State(s): State<Arc<LoaderState>>) -> StatusCode {
    ok_or_unavailable(s.is_live())
}

const fn ok_or_unavailable(ok: bool) -> StatusCode {
    if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The Prometheus text exposition (PR 4.10) — stateless; reads the process-wide recorder.
async fn metrics() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        common::metrics::render(),
    )
}

pub fn router(state: Arc<LoaderState>) -> Router {
    Router::new()
        .route("/startup", get(startup))
        .route("/ready", get(ready))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Serve loader health and metrics routes until `shutdown` is cancelled.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if Axum fails while accepting or serving a connection on `listener`.
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    state: Arc<LoaderState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "health_test.rs"]
mod tests;
