//! The loader's K8s health endpoints (loader §8.3) — **the catch-up-lag trap avoided**.
//!
//! - `/startup` — 200 once bootstrap completes (gates lease/fence acquisition + DuckLake attach).
//! - `/ready`   — 200 iff local bootstrap is done, the initial frozen all-table reconciliation has
//!   published, and the process is **not quarantined**. It is never gated on ordinary WAL backlog:
//!   a legitimately-behind streaming loader is still ready. A **quarantined** table (a failed
//!   lossy DDL cast) degrades `/ready` — a loud, terminal signal, not a silent continue.
//! - `/healthz` — liveness = *progress*, read from an in-memory `last_poll_completed_at` stamped every
//!   cycle (even a no-op). It reflects **no** lag metric — an idle-but-healthy loader must stay live.

use axum::{
    Router, extract::State, http::StatusCode, http::header, response::IntoResponse, routing::get,
};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// The loader's health lifecycle — exactly one of three states.
///
/// [`Quarantined`](LoaderPhase::Quarantined) implies bootstrap finished: its producer is a failed
/// lossy DDL cast in the apply loop, which cannot run before bootstrap. That implication keeps
/// `/startup` satisfied while `/ready` degrades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum LoaderPhase {
    /// Leases/fences are not yet held and DuckLake connections are not yet open. Both `/startup` and `/ready`
    /// answer 503. The default, and byte `0` — which `AtomicPhase`'s zero default depends on.
    #[default]
    Bootstrapping = 0,
    /// Local bootstrap is complete. `/ready` additionally consults the generation-published latch.
    Ready = 1,
    /// Latched by a failed lossy DDL cast. A reload rebuild is its only exit.
    Quarantined = 2,
}

/// An out-of-range loader phase byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPhase(pub u8);

impl TryFrom<u8> for LoaderPhase {
    type Error = InvalidPhase;

    /// Decode a phase byte read back out of the atomic the probes share.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPhase`] carrying `byte` for anything other than `0`
    /// ([`LoaderPhase::Bootstrapping`]), `1` ([`LoaderPhase::Ready`]), or `2`
    /// ([`LoaderPhase::Quarantined`]). Only this module's typed stores write that atomic, so an
    /// unknown byte is a bug here rather than a probe input.
    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        // AtomicU8::default(), the typed stores, the quarantine compare_exchange, and this decoder
        // all rely on these exact bytes.
        const {
            assert!(
                LoaderPhase::Bootstrapping as u8 == 0,
                "LoaderPhase::Bootstrapping must stay byte 0 because AtomicPhase defaults to zero"
            );
            assert!(
                LoaderPhase::Ready as u8 == 1,
                "LoaderPhase::Ready must stay byte 1 so AtomicPhase store and decode agree"
            );
            assert!(
                LoaderPhase::Quarantined as u8 == 2,
                "LoaderPhase::Quarantined must stay byte 2 or clear_quarantine's compare_exchange \
                 swaps the wrong phase"
            );
        }

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
        // Release: `Ready` publishes bootstrap (leases held, files open) and `Quarantined` publishes
        // the failed cast that latched it. SeqCst would only add a total order with other atomics,
        // and this is the sole atomic every probe reads.
        self.0.store(phase as u8, Ordering::Release);
    }

    fn load(&self) -> LoaderPhase {
        // Acquire: pairs with the Release store, so a probe that sees a phase sees what produced it.
        let byte = self.0.load(Ordering::Acquire);
        LoaderPhase::try_from(byte).unwrap_or_else(|InvalidPhase(invalid)| {
            tracing::error!(
                phase_byte = invalid,
                "invalid loader health phase byte; defaulting to bootstrapping"
            );
            LoaderPhase::Bootstrapping
        })
    }

    fn transition(&self, from: LoaderPhase, to: LoaderPhase) -> bool {
        // AcqRel on success: the read half sees the store that latched `from` (the quarantining
        // cast), the write half publishes `to` like the plain store above. Relaxed on failure —
        // the losing byte is dropped on the floor, so nothing is read behind it.
        self.0
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }
}

/// The state the three Kubernetes probes read, shared by every table worker.
///
/// One process holds exactly one of these behind an `Arc`: the phase latch is process-wide (a
/// single quarantined table degrades the whole pod's `/ready`), and the poll stamp is whichever
/// worker finished a cycle most recently.
#[derive(Debug, Default)]
pub struct LoaderState {
    phase: AtomicPhase,
    /// Fresh all-table reconciliation gates external readiness independently of local startup.
    /// Keeping this separate from quarantine means a repaired table cannot accidentally advertise
    /// ready before the rest of its bootstrap group has published.
    generation_ready: AtomicBool,
    /// The end of the last poll cycle — liveness proof, NOT a lag metric. `None` until bootstrap ends.
    // LOCK-CHOICE: parking_lot::Mutex — poll-cycle writes dominate the one-expression kubelet read.
    last_poll_completed_at: Mutex<Option<Instant>>,
}

impl LoaderState {
    /// A fresh state, already wrapped in the `Arc` every caller needs.
    ///
    /// Hands back `Arc<Self>` rather than `Self` because there is no useful unshared owner: the
    /// probe router and the workers both hold it. That shape is why `clippy::new_ret_no_self` has a
    /// scoped allow on the sink's equivalent.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(LoaderState::default())
    }

    /// Local bootstrap finished for an already-published generation: leases held + files open →
    /// `/startup` and `/ready` answer 200.
    pub fn mark_ready(&self) {
        self.generation_ready.store(true, Ordering::Release);
        self.phase.store(LoaderPhase::Ready);
    }

    /// Local bootstrap finished, but the control generation is still reconciling its frozen table
    /// group. `/startup` succeeds and liveness runs; `/ready` remains gated.
    pub fn mark_reconciling(&self) {
        self.generation_ready.store(false, Ordering::Release);
        self.phase.store(LoaderPhase::Ready);
    }

    /// The sink promoted this generation after every table shadow was published.
    pub fn mark_generation_ready(&self) {
        self.generation_ready.store(true, Ordering::Release);
    }

    /// The sink durably retired this generation before replacing its lost slot. Drop readiness
    /// immediately; the loader process then drains and exits while the successor is established.
    pub fn mark_generation_retired(&self) {
        self.generation_ready.store(false, Ordering::Release);
    }

    // The four probe reads in this impl (`is_started`, `is_ready`, `is_quarantined`, `is_live`)
    // answer a question and change nothing, so discarding one is always a bug — hence the explicit
    // `#[must_use]` on each. `clippy::must_use_candidate` reaches none of them: `&self` on a struct
    // with interior mutability (the atomic phase, the poll-stamp mutex) reads to that lint as a
    // mutable — therefore side-effecting — argument. The mutators between them return `()` and
    // correctly carry nothing.
    /// `/startup` gate: bootstrap finished, including a later quarantine.
    #[must_use]
    pub fn is_started(&self) -> bool {
        matches!(
            self.phase.load(),
            LoaderPhase::Ready | LoaderPhase::Quarantined
        )
    }

    /// `/ready` answers 200 only after local startup and generation publication.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self.phase.load(), LoaderPhase::Ready)
            && self.generation_ready.load(Ordering::Acquire)
    }

    /// Latch the quarantine flag — a failed lossy DDL cast. `/ready` degrades and stays
    /// degraded; the caller also logs an error-level alert and exits. The latch has exactly one
    /// exit: a single-table-reload rebuild, which REPLACES the data instead of
    /// retrying the cast on it ([`LoaderState::clear_quarantine`]).
    pub fn quarantine(&self) {
        self.phase.store(LoaderPhase::Quarantined);
    }

    /// The one legitimate quarantine exit: a reload rebuild just recreated the table at
    /// the attempt's schema_version, so the lossy cast the latch recorded no longer applies to
    /// anything — `/ready` recovers.
    pub fn clear_quarantine(&self) {
        let _transitioned = self
            .phase
            .transition(LoaderPhase::Quarantined, LoaderPhase::Ready);
    }

    /// Whether the quarantine latch is set — the state `/ready` reports 503 for while `/startup`
    /// stays satisfied.
    #[must_use]
    pub fn is_quarantined(&self) -> bool {
        matches!(self.phase.load(), LoaderPhase::Quarantined)
    }

    /// Stamp progress — called at the end of **every** poll cycle (and once at bootstrap end so an
    /// idle loader stays live).
    pub fn stamp_poll(&self) {
        *self.last_poll_completed_at.lock() = Some(Instant::now());
    }

    /// Liveness = we have completed at least one cycle (progress stamped). Deliberately lag-free.
    #[must_use]
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

/// The Prometheus text exposition — stateless; reads the process-wide recorder.
async fn metrics() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        common::metrics::render(),
    )
}

/// The probe + metrics router, with the shared state injected.
///
/// Deliberately **not** `#[must_use]`, for the reason the sink's `health::router` records: axum's
/// `Router` already carries the attribute, so a second bare one is `clippy::double_must_use`.
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
