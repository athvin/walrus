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

use common::Lsn;

/// A pre-existing slot's resume position.
#[derive(Debug, Clone, Copy)]
pub struct SlotInfo {
    pub restart_lsn: Lsn,
    pub confirmed_flush_lsn: Lsn,
}

/// Whether the slot already existed or we just created it.
#[derive(Debug, Clone)]
pub enum SlotResume {
    Existing(SlotInfo),
    Created {
        consistent_point: Lsn,
        /// `None` for a SQL-created slot; the exported snapshot is PR 2.29 (backfill).
        snapshot_name: Option<String>,
    },
}

impl SlotResume {
    /// The LSN to hand `START_REPLICATION`. Resuming an existing slot means its
    /// `confirmed_flush_lsn`; a fresh slot means its creation point. (The server clamps up to its own
    /// value regardless.)
    pub fn start_lsn(&self) -> Lsn {
        match self {
            SlotResume::Existing(info) => info.confirmed_flush_lsn,
            SlotResume::Created {
                consistent_point, ..
            } => *consistent_point,
        }
    }
}

fn parse_lsn(s: &str) -> anyhow::Result<Lsn> {
    s.parse()
        .map_err(|e| anyhow::anyhow!("could not parse LSN {s:?}: {e:?}"))
}

/// Verify the slot (reading `restart_lsn` / `confirmed_flush_lsn`), or create it via SQL.
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
        .await?;

    if let Some(row) = rows.first() {
        // A freshly-created slot can have NULL LSNs until first use — treat NULL as ZERO.
        let restart: Option<String> = row.get(0);
        let confirmed: Option<String> = row.get(1);
        let restart_lsn = restart
            .as_deref()
            .map(parse_lsn)
            .transpose()?
            .unwrap_or(Lsn::ZERO);
        let confirmed_flush_lsn = confirmed
            .as_deref()
            .map(parse_lsn)
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
        .await?;
    let lsn: String = row.get(0);
    Ok(SlotResume::Created {
        consistent_point: parse_lsn(&lsn)?,
        snapshot_name: None,
    })
}
