//! The idle heartbeat and its round-trip liveness signal (§1.9).
//!
//! An idle *published* set of tables cannot advance `confirmed_flush_lsn` — no commits flow, so
//! `restart_lsn` freezes and retained WAL grows unbounded until the source disk fills or the slot is
//! invalidated. The mitigation is a **sink-driven, idle-triggered** beat: one `UPDATE` to the
//! published `walrus.heartbeat` table over a *separate* ordinary SQL connection. Because that table
//! rides `walrus_pub`, the beat decodes back through the replication stream — and *that return* is a
//! round-trip liveness signal, distinct from both the keepalive (PR 2.20) and the durability
//! checkpoint (PR 2.26).
//!
//! **Three LSNs, kept apart:**
//! - *keepalive/received* (`write`) — moves **unconditionally** on every frame ([`crate::replication`]);
//! - the *beat's established point* — where an idle beat's commit lets the checkpoint advance;
//! - *`confirmed_flush_lsn`* (`flush`/`apply`) — moves **only after durability** ([`crate::checkpoint`]).
//!
//! **Never self-harm:** a stale round-trip during a legitimate catch-up sets a non-gating `degraded`
//! flag on `/ready`. It is *never* a `livenessProbe` kill and *never* a hard readiness gate — a
//! catching-up sink is stale by design (same reason liveness is never slot-lag, §4.3).

use common::TupleValue;
use std::time::Duration;
use tokio::time::Instant;

/// Bounds-validated in `config.rs`: both `> 0` and `idle_after < roundtrip_deadline`.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// Fire a beat only after the published tables have been idle this long (monotonic).
    pub idle_after: Duration,
    /// A beat still un-returned after this long marks the sink `degraded` (observability only).
    pub roundtrip_deadline: Duration,
}

/// OIDs (and layout) of walrus's own published tables — consumed for control, never materialised.
#[derive(Debug, Default, Clone)]
pub struct InternalTables {
    pub heartbeat_oid: Option<u32>,
    /// Column index of `walrus.heartbeat.beat_seq`, learned from its `Relation` message.
    heartbeat_beat_seq_col: Option<usize>,
    // pub ddl_audit_oid: Option<u32>,   // filled in PR 2.33
}

impl InternalTables {
    /// Is this relation OID one of walrus's own control tables (never staged as user data)?
    pub fn is_internal(&self, rel_oid: u32) -> bool {
        self.heartbeat_oid == Some(rel_oid)
    }

    /// Learn a walrus-internal table's OID + relevant column offsets from its `Relation` message.
    /// The `walrus.heartbeat` change is never cached in the [`crate::relcache::RelationCache`] (it is
    /// internal), so its `beat_seq` column index is captured here to read the round-trip seq later.
    pub fn note_relation(&mut self, relation: &common::PgRelation) {
        if relation.schema == "walrus" && relation.name == "heartbeat" {
            self.heartbeat_oid = Some(relation.oid);
            self.heartbeat_beat_seq_col =
                relation.columns.iter().position(|c| c.name == "beat_seq");
        }
    }

    /// Extract the returned `beat_seq` from a decoded `walrus.heartbeat` new-tuple (text format).
    pub fn beat_seq_of(&self, new: &[TupleValue]) -> Option<i64> {
        let idx = self.heartbeat_beat_seq_col?;
        match new.get(idx)? {
            TupleValue::Text(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }
}

/// Pure idle/round-trip bookkeeping — no I/O, so it is unit-tested directly (the async
/// [`Heartbeat`] wraps it around a real SQL connection).
#[derive(Debug, Clone)]
struct BeatState {
    cfg: HeartbeatConfig,
    last_beat: Option<Instant>,
    /// A beat's seq written and awaiting its return through the stream; `None` once it returns.
    pending_seq: Option<i64>,
    /// The last time a beat completed its round-trip (observability / logs).
    last_roundtrip: Option<Instant>,
}

impl BeatState {
    fn new(cfg: HeartbeatConfig) -> Self {
        BeatState {
            cfg,
            last_beat: None,
            pending_seq: None,
            last_roundtrip: None,
        }
    }

    /// Fire iff idle on **both** clocks: no user activity for `idle_after`, and no beat within
    /// `idle_after` (so beats never pile up under a still publication).
    fn should_beat(&self, now: Instant, last_activity: Instant) -> bool {
        let idle_activity = now.saturating_duration_since(last_activity) >= self.cfg.idle_after;
        let idle_beat = self
            .last_beat
            .is_none_or(|t| now.saturating_duration_since(t) >= self.cfg.idle_after);
        idle_activity && idle_beat
    }

    fn on_beat_sent(&mut self, seq: i64, now: Instant) {
        self.last_beat = Some(now);
        self.pending_seq = Some(seq);
    }

    /// The beat decoded back: `beat_seq` is monotonic, so a returned seq **≥** pending closes the
    /// round-trip (a coalesced/late beat still counts — never require exact lock-step).
    fn observe_return(&mut self, beat_seq: i64, now: Instant) {
        if self.pending_seq.is_some_and(|p| beat_seq >= p) {
            self.pending_seq = None;
            self.last_roundtrip = Some(now);
        }
    }

    /// Non-gating: an outstanding beat that has not returned within `roundtrip_deadline`. Under steady
    /// traffic (no pending beat) this is always `false` — the stream itself proves liveness.
    fn degraded(&self, now: Instant) -> bool {
        match (self.pending_seq, self.last_beat) {
            (Some(_), Some(sent)) => {
                now.saturating_duration_since(sent) > self.cfg.roundtrip_deadline
            }
            _ => false,
        }
    }
}

/// Owns a *separate* ordinary SQL connection to the source (distinct from the read-only replication
/// connection) plus the pure [`BeatState`]. The beat **must** ride the published `walrus.heartbeat`
/// table or `pgoutput` filters it out and there is no round-trip.
pub struct Heartbeat {
    sql: tokio_postgres::Client,
    instance: String,
    state: BeatState,
}

impl Heartbeat {
    /// Open the ordinary SQL connection and drive it in the background (`NoTls` — the dev/source auth
    /// is `trust`; TLS is the operator's network concern, not this control path).
    pub async fn connect(
        dsn: &str,
        instance: String,
        cfg: HeartbeatConfig,
    ) -> Result<Self, HeartbeatError> {
        let (sql, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(HeartbeatError::Connect)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "heartbeat SQL connection closed");
            }
        });
        Ok(Heartbeat {
            sql,
            instance,
            state: BeatState::new(cfg),
        })
    }

    /// The idle window — the decode loop uses it to pace its beat check.
    pub fn idle_after(&self) -> Duration {
        self.state.cfg.idle_after
    }

    /// Fire exactly one beat iff idle on both clocks; returns the seq written, or `None` (suppressed).
    /// The `UPDATE` rides the **published** `walrus.heartbeat` so it decodes back to us.
    pub async fn maybe_beat(
        &mut self,
        now: Instant,
        last_activity: Instant,
    ) -> Result<Option<i64>, HeartbeatError> {
        if !self.state.should_beat(now, last_activity) {
            return Ok(None);
        }
        let row = self
            .sql
            .query_one(
                "UPDATE walrus.heartbeat \
                 SET beat_seq = beat_seq + 1, ts = now(), sink_instance = $1 \
                 WHERE id = 1 RETURNING beat_seq",
                &[&self.instance],
            )
            .await
            .map_err(HeartbeatError::Beat)?;
        let seq: i64 = row.get(0);
        self.state.on_beat_sent(seq, now);
        Ok(Some(seq))
    }

    /// The heartbeat relation decoded back through the stream: record the round-trip.
    pub fn observe_return(&mut self, beat_seq: i64, now: Instant) {
        self.state.observe_return(beat_seq, now);
    }

    /// Non-gating health signal (see [`BeatState::degraded`]).
    pub fn degraded(&self, now: Instant) -> bool {
        self.state.degraded(now)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeartbeatError {
    #[error("connect heartbeat SQL connection: {0}")]
    Connect(#[source] tokio_postgres::Error),
    #[error("fire heartbeat UPDATE: {0}")]
    Beat(#[source] tokio_postgres::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{PgColumn, PgRelation, ReplicaIdentity};
    use pg_to_arrow::oids;

    fn cfg() -> HeartbeatConfig {
        HeartbeatConfig {
            idle_after: Duration::from_secs(10),
            roundtrip_deadline: Duration::from_secs(30),
        }
    }

    fn heartbeat_relation() -> PgRelation {
        PgRelation {
            oid: 90001,
            schema: "walrus".to_string(),
            name: "heartbeat".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                PgColumn {
                    name: "id".into(),
                    type_oid: oids::INT4,
                    type_modifier: -1,
                    is_key: true,
                },
                PgColumn {
                    name: "beat_seq".into(),
                    type_oid: oids::INT8,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        }
    }

    #[test]
    fn beat_suppressed_when_active_within_idle_after() {
        let s = BeatState::new(cfg());
        let now = Instant::now();
        // Activity 1s ago, idle_after is 10s → not idle → suppressed.
        assert!(!s.should_beat(now, now - Duration::from_secs(1)));
    }

    #[test]
    fn beat_fires_exactly_once_after_idle_threshold() {
        let mut s = BeatState::new(cfg());
        let now = Instant::now();
        let idle_since = now - Duration::from_secs(11); // idle > idle_after
        assert!(
            s.should_beat(now, idle_since),
            "idle past the threshold → beat"
        );
        // Simulate firing: on_beat_sent stamps last_beat = now.
        s.on_beat_sent(1, now);
        // Immediately after, the beat clock suppresses a second beat.
        assert!(
            !s.should_beat(now, idle_since),
            "just beat → suppressed until idle_after elapses again"
        );
        // After another idle_after with still no activity, it may beat again.
        let later = now + Duration::from_secs(11);
        assert!(s.should_beat(later, idle_since));
    }

    #[test]
    fn roundtrip_recorded_when_returned_seq_matches_pending() {
        let mut s = BeatState::new(cfg());
        let now = Instant::now();
        s.on_beat_sent(5, now);
        assert!(
            s.degraded(now + Duration::from_secs(31)),
            "un-returned past deadline → degraded"
        );
        // A returned seq ≥ pending closes it (coalesced beat: 6 ≥ 5).
        s.observe_return(6, now + Duration::from_secs(1));
        assert_eq!(s.pending_seq, None);
        assert!(
            !s.degraded(now + Duration::from_secs(31)),
            "round-trip clears degraded"
        );
    }

    #[test]
    fn degraded_true_only_after_deadline_without_roundtrip() {
        let mut s = BeatState::new(cfg());
        let now = Instant::now();
        assert!(!s.degraded(now), "no beat outstanding → never degraded");
        s.on_beat_sent(1, now);
        assert!(
            !s.degraded(now + Duration::from_secs(29)),
            "within deadline → not yet"
        );
        assert!(
            s.degraded(now + Duration::from_secs(31)),
            "past deadline, un-returned → degraded"
        );
    }

    #[test]
    fn internal_heartbeat_oid_is_recognised() {
        let mut it = InternalTables::default();
        assert!(!it.is_internal(90001), "unknown until we see its Relation");
        it.note_relation(&heartbeat_relation());
        assert!(it.is_internal(90001));
        assert!(!it.is_internal(42), "a user table is not internal");
        // beat_seq is the second column → read it back from a new-tuple.
        let new = vec![TupleValue::Text("1".into()), TupleValue::Text("7".into())];
        assert_eq!(it.beat_seq_of(&new), Some(7));
    }
}
