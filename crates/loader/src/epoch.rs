//! Total-restart on the loader side (§1.8). When the control plane opens a new generation (the sink
//! bumped `replication_state.epoch` after the single lifelong slot was lost/invalidated), every
//! `.duckdb` built for the retired generation holds stale `<table>`/`<table>_raw` data. The fix is a
//! whole-file **rebuild**: wipe the mirror + CDC log so the fresh new-epoch snapshot re-appends from
//! scratch and the transform re-derives the mirror. **Both watermarks reset for free** — the new epoch's
//! `loader_checkpoint` row is a fresh `(0/0, 0/0)`, since checkpoints are epoch-keyed.
//!
//! Detection is at **bootstrap** (compare each file's `_walrus_meta['epoch']` to the control epoch) and,
//! for a *running* loader, per poll ([`apply_loop`](crate::apply_loop) exits loudly on a bump so the
//! orchestrator restarts it into a rebuild). A rebuild is **whole-system** by construction — every table
//! shares the epoch and is rebuilt together; there is no per-table reload (a deferred goal, §1.8).

use crate::duck::TableDb;
use crate::error::LoaderError;

/// If `db` was built for an older generation than `control_epoch`, wipe its mirror + raw so the caller's
/// subsequent `ensure_tables*` recreates them empty and the new-epoch snapshot rebuilds the file. Returns
/// `true` iff a rebuild happened. A no-op (returns `false`) when the file is brand-new (never stamped) or
/// already at `control_epoch` — so first-bootstrap and steady resume are untouched.
pub fn rebuild_for_new_epoch(
    db: &TableDb,
    table: &str,
    control_epoch: i64,
) -> Result<bool, LoaderError> {
    match db.built_epoch()? {
        Some(built) if built < control_epoch => {
            tracing::error!(
                table,
                old_epoch = built,
                new_epoch = control_epoch,
                "TOTAL-RESTART: .duckdb was built for a retired generation — wiping mirror + raw to \
                 rebuild under the new epoch (both watermarks reset from the fresh checkpoint)"
            );
            db.wipe_generation(table)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{PgColumn, PgRelation, ReplicaIdentity};
    use std::path::Path;

    fn orders() -> PgRelation {
        let col = |name: &str, oid: u32, is_key: bool| PgColumn {
            name: name.into(),
            type_oid: oid,
            type_modifier: -1,
            is_key,
        };
        PgRelation {
            oid: 42,
            schema: "public".into(),
            name: "orders".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![col("id", 23, true), col("status", 25, false)],
        }
    }

    fn open_fresh(dir: &Path) -> TableDb {
        let db = TableDb::open(&dir.join("orders.duckdb")).unwrap();
        db.ensure_tables(&orders(), 1).unwrap();
        db
    }

    #[test]
    fn rebuild_wipes_a_stale_generation_and_is_idempotent() {
        let dir = std::env::temp_dir().join("walrus-loader-epoch-rebuild");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = open_fresh(&dir);

        // A file built for epoch 1 with a row in raw + mirror.
        db.set_built_epoch(1).unwrap();
        db.conn()
            .execute_batch(
                "INSERT INTO orders VALUES (1, 'x', '0', '0'); \
                 INSERT INTO orders_raw (id, status, walrus_pg_sink_meta, _walrus_op, \
                     _walrus_commit_lsn, _walrus_lsn, _walrus_sink_processed_at) \
                 VALUES (1, 'x', '{}', 'Insert', '0', '0', 't');",
            )
            .unwrap();

        // Control epoch bumped to 2 → the file is stale → rebuild wipes it (raw + mirror gone).
        assert!(rebuild_for_new_epoch(&db, "orders", 2).unwrap());
        // Recreate empty (as bootstrap does) and confirm the stale rows are gone.
        db.ensure_tables(&orders(), 1).unwrap();
        db.set_built_epoch(2).unwrap();
        let mirror: i64 = db
            .conn()
            .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
            .unwrap();
        let raw: i64 = db
            .conn()
            .query_row("SELECT count(*) FROM orders_raw", [], |r| r.get(0))
            .unwrap();
        assert_eq!((mirror, raw), (0, 0), "the retired generation was wiped");

        // Idempotent: already at epoch 2 → no rebuild.
        assert!(!rebuild_for_new_epoch(&db, "orders", 2).unwrap());
    }

    #[test]
    fn fresh_file_is_not_rebuilt() {
        let dir = std::env::temp_dir().join("walrus-loader-epoch-fresh");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = open_fresh(&dir);
        // Never stamped (built_epoch = None) → a fresh bootstrap, never a rebuild.
        assert!(!rebuild_for_new_epoch(&db, "orders", 1).unwrap());
        assert!(!rebuild_for_new_epoch(&db, "orders", 5).unwrap());
    }
}
