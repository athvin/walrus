//! One `.duckdb` file's read-write connection (loader §8.1) — holding DuckDB's single-writer file lock,
//! the **second** fence (the lease is the first). Opening read-write takes an exclusive OS lock on the
//! file: if a still-live owner held it we'd fail opaquely here, which is why the ordered bootstrap
//! proves the lease is reclaimable *before* calling [`TableDb::open`].

use crate::error::LoaderError;
use crate::plan::TablePlan;
use common::PgRelation;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

// DuckDB DDL templates (see `sql/duckdb/templates/`). Fixed structure with `{placeholder}` holes,
// rendered by `.replace(...)`; per-table column lists stay interpolated in Rust (they can't be
// static). `include_str!` paths are source-file-relative (contrast `sqlx::query_file!`).
const CREATE_MIRROR: &str = include_str!("../sql/duckdb/templates/create_mirror.sql");
const ALTER_ADD_APPLIED: &str = include_str!("../sql/duckdb/templates/alter_add_applied.sql");
const CREATE_RAW: &str = include_str!("../sql/duckdb/templates/create_raw.sql");
const CREATE_USER_VIEW: &str = include_str!("../sql/duckdb/templates/create_user_view.sql");
const CREATE_META: &str = include_str!("../sql/duckdb/templates/create_meta.sql");
const CONFIGURE_S3: &str = include_str!("../sql/duckdb/templates/configure_s3.sql");
const APPEND_PARQUET: &str = include_str!("../sql/duckdb/templates/append_parquet.sql");
const RELOAD_REBUILD_DROP: &str = include_str!("../sql/duckdb/templates/reload_rebuild_drop.sql");
const WIPE_GENERATION: &str = include_str!("../sql/duckdb/templates/wipe_generation.sql");

/// Owns one table's `.duckdb` connection (mirror `<table>` + CDC log `<table>_raw`).
pub struct TableDb {
    conn: duckdb::Connection,
    /// Parquet column lists by `schema_version` (PR 5.8). A version's file shape is immutable — the
    /// sink's homogeneous-file rule (walrus-pg-sink §3.5) cuts a fresh file at every DDL bump, so all
    /// files at one version share their columns and a DDL bump is a *new* key. So this cache never
    /// invalidates, and a Phase-A cycle claiming N same-version files runs one `DESCRIBE`, not N.
    /// `RefCell` for interior mutability: `TableDb` is used single-threaded (DuckDB `Connection` is
    /// `!Send`, one per apply worker on a `LocalSet`).
    parquet_cols: RefCell<HashMap<i64, Arc<Vec<String>>>>,
}

impl TableDb {
    /// Open (or create) the file read-write, taking DuckDB's file lock. A stale lock behind an expired
    /// lease has already been reclaimed by the caller; a *live* owner would make this fail.
    pub fn open(path: &Path) -> Result<Self, LoaderError> {
        let conn = duckdb::Connection::open(path)
            .map_err(|e| LoaderError::Duck(format!("open {}: {e}", path.display())))?;
        Ok(TableDb {
            conn,
            parquet_cols: RefCell::new(HashMap::new()),
        })
    }

    /// `CREATE TABLE IF NOT EXISTS` for BOTH the mirror `<table>` and the CDC log `<table>_raw`
    /// (composite PK for at-least-once dedup), the user-facing `<table>_current` view, and a
    /// `_walrus_meta` row seeding this table's `schema_version` (PR 3.8's DDL-reconcile watermark).
    /// The seed is `ON CONFLICT DO NOTHING`, so an EXISTING `.duckdb` keeps its persisted, already-
    /// reconciled version across restarts — the additive DDL applier ([`crate::ddl`]) advances it.
    pub fn ensure_tables(&self, rel: &PgRelation, schema_version: i64) -> Result<(), LoaderError> {
        self.ensure_tables_planned(&crate::plan::TablePlan::tier1(rel), schema_version)
    }

    /// As [`TableDb::ensure_tables`], but from a full [`TablePlan`] — the mirror carries the recombined
    /// target types and `<table>_raw` the verbatim emit columns (Tier-2 decomposition, PR 4.2). The
    /// Tier-1 plan produces exactly the scalar shape `ensure_tables` always built.
    pub fn ensure_tables_planned(
        &self,
        plan: &TablePlan,
        schema_version: i64,
    ) -> Result<(), LoaderError> {
        let cols: Vec<String> = plan
            .mirror_cols
            .iter()
            .map(|c| format!("\"{}\" {}", c.name, c.duckdb_type))
            .collect();
        let keys: Vec<String> = plan
            .mirror_cols
            .iter()
            .filter(|c| c.is_key)
            .map(|c| format!("\"{}\"", c.name))
            .collect();
        let raw_cols: Vec<String> = plan
            .raw_cols
            .iter()
            .map(|c| format!("\"{}\" {}", c.name, c.duckdb_type))
            .collect();
        let table = &plan.table;

        // The mirror: current row per key, plus two HIDDEN guard columns (§7, ⚠ extends architecture.md)
        // recording the `(commit_lsn, lsn)` tuple that last shaped each row — the per-PK max-applied guard
        // that makes a stale straddle winner a no-op. Seeded from the low sentinel `0/0`; a pre-3.7 mirror
        // gains them via `ALTER … IF NOT EXISTS`, which back-fills existing rows with that sentinel (a
        // too-low seed just means the first real event wins — which is correct).
        let primary_key = if keys.is_empty() {
            String::new()
        } else {
            format!(", PRIMARY KEY ({})", keys.join(", "))
        };
        let mirror = CREATE_MIRROR
            .replace("{table}", table)
            .replace("{cols}", &cols.join(", "))
            .replace("{primary_key}", &primary_key);
        // Idempotent back-fill for a mirror created before PR 3.7 (compose resume).
        let applied_cols = ALTER_ADD_APPLIED.replace("{table}", table);
        // The user-facing projection: the mirror WITHOUT the internal guard columns (DoD §7 "hidden from
        // user projections"). Users read `<table>_current`; `_applied_*` never leak. Recreated by the DDL
        // applier after any structural change (a `SELECT *` view binds its columns at creation time).
        let user_view = user_view_sql(table);
        // The per-table DDL-reconcile watermark (PR 3.8). Seeded once; the applier advances it.
        let meta = CREATE_META.replace("{schema_version}", &schema_version.to_string());

        // The CDC log: every change verbatim (the emit columns), with the intact `walrus_pg_sink_meta`
        // JSON plus four columns PROMOTED out of it (op / commit_lsn / lsn / sink_processed_at) as sortable
        // 16-hex / RFC-3339 text. **Composite PK = source key + sink_processed_at + lsn** — the load-bearing
        // idempotency fence (a ms-resolution `sink_processed_at` collision is broken by the always-distinct
        // `lsn`): `ON CONFLICT DO NOTHING` makes a crash-window replay a no-op.
        let mut raw_pk = keys.clone();
        raw_pk.push("\"_walrus_sink_processed_at\"".into());
        raw_pk.push("\"_walrus_lsn\"".into());
        let raw = CREATE_RAW
            .replace("{table}", table)
            .replace("{raw_cols}", &raw_cols.join(", "))
            .replace("{raw_pk}", &raw_pk.join(", "));

        self.conn
            .execute_batch(&format!("{mirror} {applied_cols} {raw} {user_view} {meta}"))
            .map_err(|e| LoaderError::Duck(format!("ensure tables for {table}: {e}")))?;
        Ok(())
    }

    /// Configure DuckDB's bundled httpfs for the S3/MinIO staging bucket — **once per connection**, so
    /// `read_parquet('s3://…')` then needs no per-call credentials. For MinIO the endpoint is
    /// `host:port` (no scheme), path-style, TLS off.
    pub fn configure_s3(&self, s3: &S3Access) -> Result<(), LoaderError> {
        let esc = common::sql::sql_literal;
        let use_ssl = if s3.use_ssl { "true" } else { "false" };
        let sql = CONFIGURE_S3
            .replace("{region}", &esc(&s3.region))
            .replace("{endpoint}", &esc(&s3.endpoint))
            .replace("{use_ssl}", use_ssl)
            .replace("{access_key}", &esc(&s3.access_key_id))
            .replace("{secret_key}", &esc(&s3.secret_access_key));
        self.conn
            .execute_batch(&sql)
            .map_err(|e| LoaderError::Duck(format!("configure S3: {e}")))
    }

    /// Phase A (PR 3.2): append one Parquet file **verbatim** into `<table>_raw`, promoting
    /// `op`/`commit_lsn`/`lsn`/`sink_processed_at` out of `walrus_pg_sink_meta`. `ON CONFLICT DO NOTHING`
    /// on the composite PK makes a replay idempotent. Returns rows appended. **Never touches the mirror.**
    ///
    /// `commit_lsn_override` (PR 4.3 fix): for a **speculative-spill** file (manifest `kind = 'spill'`)
    /// the per-row `commit_lsn` in the Parquet is a *placeholder* — the file was written before its txn's
    /// commit LSN was known. A spill file is one whole transaction, so its authoritative `commit_lsn` is
    /// the file's `lsn_end` (stamped on the manifest at `Stream Commit`); passing `Some(lsn_end)` here
    /// stamps every appended row with it, so a concurrently-committed neighbour txn is never dropped by
    /// the transform's commit-LSN window (architecture.md §1.6). `None` keeps the verbatim per-row value.
    pub fn append_parquet(
        &self,
        table: &str,
        s3_uri: &str,
        schema_version: i64,
        commit_lsn_override: Option<&str>,
    ) -> Result<u64, LoaderError> {
        let uri = common::sql::sql_literal(s3_uri);
        // Map the file's columns into `<table>_raw` **by name**, not by position (PR 3.8). After an
        // `ADD COLUMN`, DuckDB appends the new column at the physical END of `<table>_raw` (after the
        // promoted columns), while the homogeneous file carries it in source order — a positional
        // `SELECT *` would then shift the promoted extracts by one. An explicit column list also lets an
        // OLDER-version file (fewer columns) NULL-fill the columns a later version added.
        // The list is cached per `schema_version` (PR 5.8) — introspected once, not per file.
        let file_cols = self.columns_for(&uri, schema_version)?;
        let quoted = file_cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let commit_lsn_expr = match commit_lsn_override {
            Some(lsn) => format!("'{}'", common::sql::sql_literal(lsn)),
            None => "json_extract_string(walrus_pg_sink_meta, '$.commit_lsn')".to_string(),
        };
        let sql = APPEND_PARQUET
            .replace("{table}", table)
            .replace("{quoted}", &quoted)
            .replace("{commit_lsn_expr}", &commit_lsn_expr)
            .replace("{uri}", &uri);
        let n = self
            .conn
            .execute(&sql, [])
            .map_err(|e| LoaderError::Duck(format!("append {s3_uri} → {table}_raw: {e}")))?;
        Ok(n as u64)
    }

    /// The Parquet column list for `schema_version`, introspecting `uri` **once** per version and
    /// caching it (PR 5.8; sound by the homogeneous-file rule — see [`TableDb::parquet_cols`]).
    fn columns_for(&self, uri: &str, schema_version: i64) -> Result<Arc<Vec<String>>, LoaderError> {
        if let Some(cols) = self.parquet_cols.borrow().get(&schema_version) {
            return Ok(Arc::clone(cols));
        }
        let cols = Arc::new(self.parquet_columns(uri)?);
        self.parquet_cols
            .borrow_mut()
            .insert(schema_version, Arc::clone(&cols));
        Ok(cols)
    }

    /// Number of distinct `schema_version`s whose column list is cached — test probe for PR 5.8.
    #[cfg(test)]
    pub fn cached_schema_versions(&self) -> usize {
        self.parquet_cols.borrow().len()
    }

    /// The column names of a staged Parquet file, in file order (source columns + `walrus_pg_sink_meta`).
    fn parquet_columns(&self, uri: &str) -> Result<Vec<String>, LoaderError> {
        let mut stmt = self
            .conn
            .prepare(&format!("DESCRIBE SELECT * FROM read_parquet('{uri}')"))
            .map_err(|e| LoaderError::Duck(format!("describe {uri}: {e}")))?;
        let cols = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| LoaderError::Duck(format!("describe {uri}: {e}")))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| LoaderError::Duck(format!("describe {uri}: {e}")))?;
        Ok(cols)
    }

    /// The `.duckdb` connection (later PRs run the transform SQL through it).
    pub fn conn(&self) -> &duckdb::Connection {
        &self.conn
    }

    /// This table's currently-reconciled `schema_version` (the `_walrus_meta` watermark). Persisted in
    /// the `.duckdb` file, so a restart resumes at the exact version its columns are already at.
    pub fn schema_version(&self) -> Result<i64, LoaderError> {
        self.conn
            .query_row(
                "SELECT v FROM \"_walrus_meta\" WHERE k = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .map_err(|e| LoaderError::Duck(format!("read schema_version: {e}")))
    }

    /// Advance the reconcile watermark after the additive DDL for `v` has been applied to both tables.
    pub fn set_schema_version(&self, v: i64) -> Result<(), LoaderError> {
        self.conn
            .execute(
                "UPDATE \"_walrus_meta\" SET v = ? WHERE k = 'schema_version'",
                duckdb::params![v],
            )
            .map_err(|e| LoaderError::Duck(format!("set schema_version: {e}")))?;
        Ok(())
    }

    /// The **epoch (generation)** this `.duckdb` was last built for (`_walrus_meta['epoch']`), or `None`
    /// if never stamped (a brand-new file — no `_walrus_meta` yet — or a pre-4.6 file). A value below the
    /// control-plane epoch means the mirror + raw hold a **retired generation** (total-restart, §1.8) and
    /// must be wiped before the new-epoch snapshot reloads.
    pub fn built_epoch(&self) -> Result<Option<i64>, LoaderError> {
        // A brand-new file has no `_walrus_meta` yet — probe first so this never errors on it.
        let has_meta: i64 = self
            .conn
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = '_walrus_meta'",
                [],
                |r| r.get(0),
            )
            .map_err(|e| LoaderError::Duck(format!("probe _walrus_meta: {e}")))?;
        if has_meta == 0 {
            return Ok(None);
        }
        // `max(v)` yields one row with NULL (→ `None`) when the 'epoch' key is absent (pre-4.6 file).
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT max(v) FROM \"_walrus_meta\" WHERE k = 'epoch'",
                [],
                |r| r.get(0),
            )
            .map_err(|e| LoaderError::Duck(format!("read built epoch: {e}")))?;
        Ok(v)
    }

    /// Stamp the generation this `.duckdb` is now built for (`_walrus_meta['epoch']`). Upserts, so it both
    /// records a fresh file's epoch and re-stamps a rebuilt file's new epoch.
    pub fn set_built_epoch(&self, epoch: i64) -> Result<(), LoaderError> {
        self.conn
            .execute(
                "INSERT INTO \"_walrus_meta\" (k, v) VALUES ('epoch', ?) \
                 ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                duckdb::params![epoch],
            )
            .map_err(|e| LoaderError::Duck(format!("set built epoch: {e}")))?;
        Ok(())
    }

    /// The highest `reload_id` this `.duckdb` has rebuilt for — the H8 idempotency latch (PR 6.7).
    /// Absent ⇒ 0, so any real (bigserial ≥ 1) reload triggers. `max(v)` yields NULL (→ 0) when
    /// the key is missing, mirroring [`TableDb::built_epoch`]'s probe-free read.
    pub fn recorded_reload_id(&self) -> Result<i64, LoaderError> {
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT max(v) FROM \"_walrus_meta\" WHERE k = 'reload_id'",
                [],
                |r| r.get(0),
            )
            .map_err(|e| LoaderError::Duck(format!("read recorded reload_id: {e}")))?;
        Ok(v.unwrap_or(0))
    }

    /// Latch the reload generation this `.duckdb` is now rebuilt for. With a monotonic bigserial
    /// id, "latest wins" (H9) is this upsert plus a numeric compare at the trigger.
    pub fn set_recorded_reload_id(&self, reload_id: i64) -> Result<(), LoaderError> {
        self.conn
            .execute(
                "INSERT INTO \"_walrus_meta\" (k, v) VALUES ('reload_id', ?) \
                 ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                duckdb::params![reload_id],
            )
            .map_err(|e| LoaderError::Duck(format!("set recorded reload_id: {e}")))?;
        Ok(())
    }

    /// The reload rebuild (PR 6.7 / reload H8, §5 step 4): atomically replace BOTH tables at the
    /// triggering file's `schema_version` — empty, at exactly the shape the attempt's chunks carry
    /// — then let ordinary Phase A/B replay chunks + post-`W` stream files in `(lsn_end, id)`
    /// order.
    ///
    /// **The raw-history decision (design §6, resolved here):** a rebuild DISCARDS the table's raw
    /// CDC history in DuckDB by design. The pre-reload raw rows describe the world the clear is
    /// replacing — replaying them against the rebuilt mirror would resurrect exactly the drift the
    /// reload exists to kill — and the staged Parquet persists in S3 per its GC policy for
    /// forensic replay. Acceptable for quarantine recovery, the feature's anchor use case.
    ///
    /// `_walrus_meta` survives (the epoch + reload_id latches live there); the schema_version
    /// watermark is set to the FILE's version explicitly — `ensure_tables_planned`'s seed is
    /// `ON CONFLICT DO NOTHING` and the pre-rebuild watermark may differ in either direction.
    pub fn rebuild_for_reload(
        &self,
        plan: &TablePlan,
        schema_version: i64,
    ) -> Result<(), LoaderError> {
        let table = &plan.table;
        self.conn
            .execute_batch(&RELOAD_REBUILD_DROP.replace("{table}", table))
            .map_err(|e| LoaderError::Duck(format!("reload rebuild drop for {table}: {e}")))?;
        self.ensure_tables_planned(plan, schema_version)?;
        self.set_schema_version(schema_version)?;
        Ok(())
    }

    /// Wipe a retired generation from this `.duckdb` (total-restart, §1.8): drop the user view, the mirror,
    /// the CDC log, and `_walrus_meta`. The caller then re-runs `ensure_tables*` to recreate them empty, so
    /// the fresh new-epoch snapshot re-appends into `<table>_raw` and the transform re-derives `<table>`
    /// from scratch (both watermarks reset — the new epoch's `loader_checkpoint` is a fresh `0/0`).
    pub fn wipe_generation(&self, table: &str) -> Result<(), LoaderError> {
        self.conn
            .execute_batch(&WIPE_GENERATION.replace("{table}", table))
            .map_err(|e| LoaderError::Duck(format!("wipe generation for {table}: {e}")))?;
        Ok(())
    }
}

/// The user-facing `<table>_current` view: the mirror minus the hidden `_applied_*` guard columns
/// (§7). A `SELECT *` view binds its column list at creation, so the DDL applier ([`crate::ddl`])
/// re-runs this after any structural change to pick up added/renamed columns.
pub(crate) fn user_view_sql(table: &str) -> String {
    CREATE_USER_VIEW.replace("{table}", table)
}

/// DuckDB S3/httpfs credentials for reading the staging bucket.
#[derive(Debug, Clone)]
pub struct S3Access {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub use_ssl: bool,
}

/// Map a Postgres type OID to a DuckDB column type. Unknown types fall back to `VARCHAR` (the loader
/// stages *text*-format tuples; the exact numeric/temporal fidelity is refined as the transform lands).
pub(crate) fn duck_type(oid: u32) -> &'static str {
    match oid {
        21 => "SMALLINT",                   // int2
        23 => "INTEGER",                    // int4
        20 => "BIGINT",                     // int8
        16 => "BOOLEAN",                    // bool
        700 => "REAL",                      // float4
        701 => "DOUBLE",                    // float8
        1700 => "DECIMAL(38,10)",           // numeric
        1082 => "DATE",                     // date
        1114 => "TIMESTAMP",                // timestamp
        1184 => "TIMESTAMP WITH TIME ZONE", // timestamptz
        2950 => "UUID",                     // uuid
        114 | 3802 => "JSON",               // json / jsonb
        17 => "BLOB",                       // bytea
        _ => "VARCHAR",                     // text, varchar, enums, and everything else
    }
}

#[cfg(test)]
#[path = "duck_test.rs"]
mod tests;
