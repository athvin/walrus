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
    extract::State, http::header, http::StatusCode, response::IntoResponse, routing::get, Router,
};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
pub struct LoaderState {
    ready: AtomicBool,
    /// Set once a table is quarantined by a failed lossy DDL cast (PR 3.9) — degrades `/ready`.
    /// A latch with exactly one exit: a single-table-reload rebuild (PR 6.7) replaces the data,
    /// so the failed cast no longer applies and the latch clears.
    quarantined: AtomicBool,
    /// The end of the last poll cycle — liveness proof, NOT a lag metric. `None` until bootstrap ends.
    last_poll_completed_at: Mutex<Option<Instant>>,
}

impl LoaderState {
    pub fn new() -> Arc<Self> {
        Arc::new(LoaderState::default())
    }

    /// Bootstrap finished: leases held + files open → `/startup` and `/ready` answer 200.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    /// `/startup` gate: bootstrap finished. Independent of a later quarantine (startup stays satisfied).
    pub fn is_started(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// `/ready` answers 200 iff bootstrap finished AND we are not quarantined (degraded).
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst) && !self.is_quarantined()
    }

    /// Latch the quarantine flag — a failed lossy DDL cast (PR 3.9). `/ready` degrades and stays
    /// degraded; the caller also logs an error-level alert and exits. Since PR 6.7 the latch has
    /// exactly one exit: a single-table-reload rebuild, which REPLACES the data instead of
    /// retrying the cast on it ([`LoaderState::clear_quarantine`]).
    pub fn quarantine(&self) {
        self.quarantined.store(true, Ordering::SeqCst);
    }

    /// The one legitimate quarantine exit (PR 6.7): a reload rebuild just recreated the table at
    /// the attempt's schema_version, so the lossy cast the latch recorded no longer applies to
    /// anything — `/ready` recovers.
    pub fn clear_quarantine(&self) {
        self.quarantined.store(false, Ordering::SeqCst);
    }

    pub fn is_quarantined(&self) -> bool {
        self.quarantined.load(Ordering::SeqCst)
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

fn ok_or_unavailable(ok: bool) -> StatusCode {
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
