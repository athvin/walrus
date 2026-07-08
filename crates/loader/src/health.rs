//! The loader's K8s health endpoints (loader §8.3) — **the catch-up-lag trap avoided**.
//!
//! - `/startup` — 200 once bootstrap completes (gates the slow lease+DuckDB open).
//! - `/ready`   — 200 iff bootstrap done (leases held + files open) **and not quarantined**. Never
//!   gated on "backlog drained": a legitimately-behind loader is still *ready*; gating on lag flaps a
//!   busy pod out. A **quarantined** table (a failed lossy DDL cast, PR 3.9) degrades `/ready` — a loud,
//!   terminal signal, not a silent continue.
//! - `/healthz` — liveness = *progress*, read from an in-memory `last_poll_completed_at` stamped every
//!   cycle (even a no-op). It reflects **no** lag metric — an idle-but-healthy loader must stay live.

use axum::{extract::State, http::StatusCode, routing::get, Router};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
pub struct LoaderState {
    ready: AtomicBool,
    /// Set once a table is quarantined by a failed lossy DDL cast (PR 3.9) — degrades `/ready`. A
    /// latch: quarantine is terminal in v1, never cleared at runtime.
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

    /// Latch the quarantine flag — a failed lossy DDL cast (PR 3.9). Terminal: `/ready` degrades and
    /// stays degraded. The caller also logs an error-level alert and exits.
    pub fn quarantine(&self) {
        self.quarantined.store(true, Ordering::SeqCst);
    }

    pub fn is_quarantined(&self) -> bool {
        self.quarantined.load(Ordering::SeqCst)
    }

    /// Stamp progress — called at the end of **every** poll cycle (and once at bootstrap end so an
    /// idle loader stays live).
    pub fn stamp_poll(&self) {
        *self.last_poll_completed_at.lock().unwrap() = Some(Instant::now());
    }

    /// Liveness = we have completed at least one cycle (progress stamped). Deliberately lag-free.
    pub fn is_live(&self) -> bool {
        self.last_poll_completed_at.lock().unwrap().is_some()
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

pub fn router(state: Arc<LoaderState>) -> Router {
    Router::new()
        .route("/startup", get(startup))
        .route("/ready", get(ready))
        .route("/healthz", get(healthz))
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
mod tests {
    use super::*;

    #[test]
    fn ready_and_live_are_independent() {
        let s = LoaderState::new();
        assert!(!s.is_ready(), "not ready until bootstrap");
        assert!(!s.is_live(), "not live until the first poll stamp");
        s.stamp_poll();
        assert!(s.is_live(), "a stamped cycle → live");
        assert!(!s.is_ready(), "live does not imply ready");
        s.mark_ready();
        assert!(s.is_ready());
    }

    #[test]
    fn quarantine_degrades_ready_but_not_startup() {
        let s = LoaderState::new();
        s.mark_ready();
        assert!(s.is_ready() && s.is_started(), "ready after bootstrap");

        s.quarantine();
        assert!(s.is_quarantined(), "quarantine latched");
        assert!(!s.is_ready(), "/ready degrades on quarantine");
        assert!(
            s.is_started(),
            "/startup stays satisfied — bootstrap did complete"
        );
    }
}
