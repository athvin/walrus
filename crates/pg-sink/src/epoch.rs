//! Slot-loss classification and the total-restart decision (§1.8). The single lifelong slot is resumed
//! forever; the **only** time walrus opens a new one is when — on a **successful** source connection —
//! the slot is authoritatively gone: **absent** (`pg_replication_slots` empty) or **invalidated**
//! (`wal_status = 'lost'` after `max_slot_wal_keep_size` was exceeded). Then the change history since
//! `confirmed_flush_lsn` is permanently lost and the only correct recovery is a whole-system re-sync
//! under a bumped epoch.
//!
//! The single most dangerous bug here is a **false positive** — treating a network blip as slot loss
//! would nuke and re-snapshot the whole system on every hiccup. So classification is split from the
//! decision: [`classify_slot`] does the I/O (and maps a query failure to [`SlotStatus::Unreachable`]),
//! and the pure [`decide`] guarantees `Unreachable` routes to a retry, **never** a fresh slot. Only a
//! catalog that authoritatively says "connected, slot gone" opens a new generation.

use common::Lsn;

/// Result of inspecting the slot on a source connection. Only `Absent` / `Invalidated` — observed on a
/// **successful** connection — are slot loss; `Unreachable` is a hiccup (retry, never total-restart).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotStatus {
    /// Present and usable — resume from `confirmed_flush`.
    Healthy { confirmed_flush: Lsn },
    /// Connected, but `pg_replication_slots` has no row → the slot was dropped.
    Absent,
    /// Connected, but `wal_status = 'lost'` → the slot was invalidated (its WAL is gone).
    Invalidated,
    /// The classification query itself failed (connection lost) → transient, retry via backoff.
    Unreachable,
}

/// The bootstrap action a classified slot implies — the whole false-positive guard, as a pure function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotAction {
    /// Slot healthy → resume streaming from `confirmed_flush`.
    Resume { confirmed_flush: Lsn },
    /// Slot gone on a successful connection → open a fresh slot + re-snapshot. A **total-restart**
    /// (epoch bump, loud alert) when a prior epoch exists, or the very first bootstrap when none does —
    /// the caller distinguishes those only to decide whether to alert.
    FreshSlot,
    /// Could not classify (connection hiccup) → retry via the bootstrap backoff; **never** bump the epoch.
    Retry,
}

/// Decide what to do from a classified slot. Pure (no I/O) so the guard is unit-tested: `Unreachable`
/// must map to `Retry`, and only `Absent` / `Invalidated` to `FreshSlot`.
pub fn decide(status: &SlotStatus) -> SlotAction {
    match status {
        SlotStatus::Healthy { confirmed_flush } => SlotAction::Resume {
            confirmed_flush: *confirmed_flush,
        },
        SlotStatus::Absent | SlotStatus::Invalidated => SlotAction::FreshSlot,
        SlotStatus::Unreachable => SlotAction::Retry,
    }
}

/// Classify the slot over a **live** source connection (post-preflight): a present row with
/// `wal_status <> 'lost'` is `Healthy`; `wal_status = 'lost'` is `Invalidated`; no row is `Absent`; a
/// query error is `Unreachable` (the connection died — a hiccup, not slot loss). `wal_status` is the
/// PG14+ invalidation signal, distinct from an empty result (a dropped slot) — both are handled.
pub async fn classify_slot(client: &tokio_postgres::Client, slot: &str) -> SlotStatus {
    let rows = match client
        .query(
            "SELECT wal_status, confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, slot, "could not read pg_replication_slots (transient) → Unreachable");
            return SlotStatus::Unreachable;
        }
    };
    let Some(row) = rows.first() else {
        return SlotStatus::Absent;
    };
    let wal_status: Option<String> = row.get(0);
    if wal_status.as_deref() == Some("lost") {
        return SlotStatus::Invalidated;
    }
    let confirmed: Option<String> = row.get(1);
    let confirmed_flush = confirmed
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(Lsn::ZERO);
    SlotStatus::Healthy { confirmed_flush }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unreachable_never_triggers_total_restart() {
        // A connection hiccup must route to Retry — never FreshSlot (which would nuke + re-snapshot the
        // whole system on every transient blip). This is the load-bearing false-positive guard.
        assert_eq!(decide(&SlotStatus::Unreachable), SlotAction::Retry);
        assert_ne!(decide(&SlotStatus::Unreachable), SlotAction::FreshSlot);
    }

    #[test]
    fn absent_or_invalidated_on_success_triggers_total_restart() {
        // Both authoritative "connected, slot gone" states open a fresh slot (→ epoch bump when a prior
        // generation exists).
        assert_eq!(decide(&SlotStatus::Absent), SlotAction::FreshSlot);
        assert_eq!(decide(&SlotStatus::Invalidated), SlotAction::FreshSlot);
    }

    #[test]
    fn healthy_resumes_from_confirmed_flush() {
        let cf: Lsn = "0/1234".parse().unwrap();
        assert_eq!(
            decide(&SlotStatus::Healthy {
                confirmed_flush: cf
            }),
            SlotAction::Resume {
                confirmed_flush: cf
            }
        );
    }
}
