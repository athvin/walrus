//! Replication-slot management (bootstrap step 4).
//!
//! Verify the slot exists and read its resume position (`confirmed_flush_lsn`), or create it. Slot
//! *management* is done over an ordinary SQL connection (`pg_replication_slots` /
//! `pg_create_logical_replication_slot`) — the `START_REPLICATION` streaming itself is the
//! hand-rolled connection in [`crate::replication`].
//!
//! **Snapshot note:** SQL creation does not export a consistent snapshot (that needs the
//! `CREATE_REPLICATION_SLOT … SNAPSHOT 'export'` *replication* command). The exported snapshot is only
//! needed for the initial backfill (PR 2.29), so the spike creates via SQL and leaves `snapshot_name`
//! `None`; PR 2.29 will create via the replication command and keep the snapshot.

use anyhow::Context as _;
use common::Lsn;

/// A pre-existing slot's resume position.
#[derive(Debug, Clone, Copy)]
pub struct SlotInfo {
    /// The oldest WAL the slot still pins on the source — what actually holds disk.
    pub restart_lsn: Lsn,
    /// The position the slot's consumer has confirmed. This, not `restart_lsn`, is where streaming
    /// resumes.
    pub confirmed_flush_lsn: Lsn,
}

/// Whether the slot already existed or we just created it.
#[derive(Debug, Clone)]
pub enum SlotResume {
    /// The slot was already there; resume from what it has confirmed.
    Existing(SlotInfo),
    /// The slot was created by this run, so there is no history to resume — only the point the
    /// source became consistent, and possibly a snapshot to backfill from first.
    Created {
        /// The LSN at which the new slot became consistent; where streaming starts.
        consistent_point: Lsn,
        /// `None` for a SQL-created slot; the exported snapshot is PR 2.29 (backfill).
        snapshot_name: Option<String>,
    },
}

impl SlotResume {
    /// The LSN to hand `START_REPLICATION`. Resuming an existing slot means its
    /// `confirmed_flush_lsn`; a fresh slot means its creation point. (The server clamps up to its own
    /// value regardless.)
    #[must_use]
    pub const fn start_lsn(&self) -> Lsn {
        match self {
            SlotResume::Existing(info) => info.confirmed_flush_lsn,
            SlotResume::Created {
                consistent_point, ..
            } => *consistent_point,
        }
    }
}

/// `field` names the catalog column the text came from, so a bad value says *which* one it was.
/// Attaching context (rather than formatting the cause into an `anyhow!` message) keeps the typed
/// `LsnParseError` — which already carries the offending text — in the chain, so `{:#}` reads
/// "parse restart_lsn as a Postgres LSN: invalid LSN …" and `downcast_ref` still finds it.
fn parse_lsn(s: &str, field: &'static str) -> anyhow::Result<Lsn> {
    s.parse()
        .with_context(|| format!("parse {field} as a Postgres LSN"))
}

/// Read a slot's resume position **without** creating it — `None` if it does not exist. The bootstrap
/// (PR 2.29) uses this to decide between resuming (`Some`) and a first-time snapshot+backfill (`None`).
///
/// # Errors
///
/// Returns [`anyhow::Error`] if `pg_replication_slots` cannot be queried or either stored LSN is
/// malformed.
pub async fn read_slot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<Option<SlotInfo>> {
    let rows = client
        .query(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| format!("read replication slot {slot:?} from pg_replication_slots"))?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let restart: Option<String> = row.get(0);
    let confirmed: Option<String> = row.get(1);
    Ok(Some(SlotInfo {
        restart_lsn: restart
            .as_deref()
            .map(|s| parse_lsn(s, "restart_lsn"))
            .transpose()?
            .unwrap_or(Lsn::ZERO),
        confirmed_flush_lsn: confirmed
            .as_deref()
            .map(|s| parse_lsn(s, "confirmed_flush_lsn"))
            .transpose()?
            .unwrap_or(Lsn::ZERO),
    }))
}

/// Verify the slot (reading `restart_lsn` / `confirmed_flush_lsn`), or create it via SQL.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if slot inspection/creation fails or a returned LSN cannot be parsed.
pub async fn verify_or_create_slot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<SlotResume> {
    let rows = client
        .query(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| format!("verify replication slot {slot:?} in pg_replication_slots"))?;

    if let Some(row) = rows.first() {
        // A freshly-created slot can have NULL LSNs until first use — treat NULL as ZERO.
        let restart: Option<String> = row.get(0);
        let confirmed: Option<String> = row.get(1);
        let restart_lsn = restart
            .as_deref()
            .map(|s| parse_lsn(s, "restart_lsn"))
            .transpose()?
            .unwrap_or(Lsn::ZERO);
        let confirmed_flush_lsn = confirmed
            .as_deref()
            .map(|s| parse_lsn(s, "confirmed_flush_lsn"))
            .transpose()?
            .unwrap_or(Lsn::ZERO);
        return Ok(SlotResume::Existing(SlotInfo {
            restart_lsn,
            confirmed_flush_lsn,
        }));
    }

    let row = client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .with_context(|| format!("create logical replication slot {slot:?} with pgoutput"))?;
    let lsn: String = row.get(0);
    Ok(SlotResume::Created {
        consistent_point: parse_lsn(&lsn, "consistent_point")?,
        snapshot_name: None,
    })
}

#[cfg(test)]
#[path = "slot_test.rs"]
mod tests;
