//! The loader's ordered, fail-fast bootstrap (loader §8.2). For each owned table: **(1)** acquire the
//! ownership lease (the first fence; a live owner → terminal) → **(2)** open/create the `.duckdb` file
//! and take its lock (the second fence) + ensure both `<table>` and `<table>_raw` → **(3)** load both
//! checkpoints and assert `transformed_lsn <= raw_appended_lsn`. Then verify the S3 read path once. The
//! **lease precedes the lock precedes any watermark read** — the fence is fully in place before the
//! read-then-write cycle a later PR runs.

use crate::config::LoaderConfig;
use crate::duck::TableDb;
use crate::error::LoaderError;
use crate::health::LoaderState;
use crate::lease;
use common::{Lsn, PgRelation};
use object_store::ObjectStore;
use std::path::Path;

/// The two commit-LSN watermarks for one table.
#[derive(Debug, Clone, Copy)]
pub struct Checkpoints {
    pub raw_appended_lsn: Lsn,
    pub transformed_lsn: Lsn,
}

/// One table this loader instance owns after bootstrap.
pub struct OwnedTable {
    pub schema: String,
    pub table: String,
    pub relation: PgRelation,
    pub fencing_token: i64,
    pub db: TableDb,
    pub checkpoints: Checkpoints,
}

impl OwnedTable {
    pub fn key(&self) -> (String, String) {
        (self.schema.clone(), self.table.clone())
    }
}

/// Run the ordered bootstrap for the current epoch's registered tables. Returns the owned tables (with
/// their open DuckDB connections and lease tokens). Any terminal step returns a classified
/// [`LoaderError`] that `main` maps to an exit code.
pub async fn bootstrap(
    cfg: &LoaderConfig,
    pool: &sqlx::PgPool,
    store: &dyn ObjectStore,
    state: &LoaderState,
) -> Result<Vec<OwnedTable>, LoaderError> {
    // The generation to load. The sink establishes it; until it does, there is nothing to own.
    let epoch = control::read_current_epoch(pool)
        .await?
        .ok_or_else(|| {
            LoaderError::Internal("no epoch established yet (sink has not bootstrapped)".into())
        })?
        .epoch;

    let registry = control::read_all_latest_registry(pool, epoch).await?;
    let mut owned = Vec::new();
    for row in registry {
        let version = row.schema_version;
        let rel: PgRelation = serde_json::from_value(row.columns)
            .map_err(|e| LoaderError::Internal(format!("decode registry columns: {e}")))?;

        // (1) FIRST FENCE: the ownership lease — before we ever touch the file.
        let lease = lease::acquire(
            pool,
            epoch,
            &rel.schema,
            &rel.name,
            &cfg.instance,
            cfg.lease_ttl,
        )
        .await?;

        // (2) SECOND FENCE: open the .duckdb read-write (takes the file lock) + ensure both tables, then
        // (4) reconcile a RESUMED .duckdb (its persisted `_walrus_meta` version < the registered latest)
        // UP TO that version — applying any additive DDL it missed before it processes more data (PR 3.8).
        // For a FRESH file this is a no-op (created at the latest shape, watermark already there); the
        // steady-state per-file forward reconcile lives in Phase A.
        let path = Path::new(&cfg.duckdb_dir).join(format!("{}.duckdb", rel.name));
        let db = TableDb::open(&path)?;
        // Build the DuckDB shape from the registry descriptors (Tier-2 emit/recombine, PR 4.2); with no
        // descriptors this is the plain scalar shape.
        db.ensure_tables_planned(
            &crate::plan::TablePlan::from_registry(&rel, &row.descriptors),
            version,
        )?;
        crate::ddl::reconcile_to_version(&db, pool, epoch, &rel.schema, &rel.name, version).await?;

        // (3) Load both watermarks (the fence is already held) and assert the DB-enforced invariant.
        control::ensure_checkpoint(pool, epoch, &rel.schema, &rel.name).await?;
        let cp = control::read_checkpoint(pool, epoch, &rel.schema, &rel.name)
            .await?
            .ok_or_else(|| {
                LoaderError::Internal(format!(
                    "checkpoint missing after ensure for {}.{}",
                    rel.schema, rel.name
                ))
            })?;
        if cp.transformed_lsn > cp.raw_appended_lsn {
            return Err(LoaderError::CorruptCheckpoint {
                table: format!("{}.{}", rel.schema, rel.name),
            });
        }

        tracing::info!(
            table = %format_args!("{}.{}", rel.schema, rel.name),
            fencing_token = lease.fencing_token,
            raw_appended = %cp.raw_appended_lsn,
            transformed = %cp.transformed_lsn,
            "owned: lease held, .duckdb open, watermarks loaded"
        );
        owned.push(OwnedTable {
            schema: rel.schema.clone(),
            table: rel.name.clone(),
            relation: rel,
            fencing_token: lease.fencing_token,
            db,
            checkpoints: Checkpoints {
                raw_appended_lsn: cp.raw_appended_lsn,
                transformed_lsn: cp.transformed_lsn,
            },
        });
    }

    // (5) Verify the S3 read path once (a `head` on a probe key: NotFound proves reachability).
    verify_s3_read(store).await?;

    // Liveness: stamp one poll so an idle-but-healthy loader is `/healthz` green (no poll loop yet).
    state.stamp_poll();
    Ok(owned)
}

/// Prove the staging bucket is readable. A `NotFound` on a probe key means "reachable, key absent" —
/// exactly what we want; any other error is a real object-store failure.
async fn verify_s3_read(store: &dyn ObjectStore) -> Result<(), LoaderError> {
    let probe = object_store::path::Path::from("__walrus_loader_probe__");
    match store.head(&probe).await {
        Ok(_) => Ok(()),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(LoaderError::ObjectStore(format!(
            "staging bucket not readable: {e}"
        ))),
    }
}
