//! One source table's DuckDB execution connection. Production connections are transient, in-memory
//! DuckDB instances attached to the shared PostgreSQL-catalogued DuckLake; [`TableDb::open`] remains
//! the native-file backend used by hermetic tests and migration verification.

use crate::config::DuckLakeConfig;
use crate::duck_ext::DuckResultExt;
use crate::error::LoaderError;
use crate::plan::TablePlan;
use common::oids::{
    BOOL, BYTEA, DATE, FLOAT4, FLOAT8, INT2, INT4, INT8, JSON, JSONB, NUMERIC, TIMESTAMP,
    TIMESTAMPTZ, UUID,
};
use common::sql::SqlStrExt;
use common::{EpochNo, ManifestId, PgRelation, Redacted, ReloadId, SchemaVersionNo};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

const EXTENSIONS: [&str; 5] = ["json", "httpfs", "aws", "postgres", "ducklake"];

/// Stable namespace for the per-source-table UUID-v5 DuckLake schema names.
const TABLE_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x4f02_efc2_39b3_4d9d_a860_22af_7291_8cc8);

// DuckDB DDL templates (see `sql/duckdb/templates/`). Fixed structure with `{placeholder}` holes,
// rendered by `.replace(...)`; per-table column lists stay interpolated in Rust (they can't be
// static). `include_str!` paths are source-file-relative (contrast `sqlx::query_file!`).
const CREATE_MIRROR: &str = include_str!("../sql/duckdb/templates/create_mirror.sql");
const ALTER_ADD_APPLIED: &str = include_str!("../sql/duckdb/templates/alter_add_applied.sql");
const CREATE_RAW: &str = include_str!("../sql/duckdb/templates/create_raw.sql");
const CREATE_INGEST_LEDGER: &str = include_str!("../sql/duckdb/templates/create_ingest_ledger.sql");
const CREATE_USER_VIEW: &str = include_str!("../sql/duckdb/templates/create_user_view.sql");
const CREATE_META: &str = include_str!("../sql/duckdb/templates/create_meta.sql");
const CONFIGURE_S3: &str = include_str!("../sql/duckdb/templates/configure_s3.sql");
const APPEND_PARQUET: &str = include_str!("../sql/duckdb/templates/append_parquet.sql");
const RELOAD_REBUILD_DROP: &str = include_str!("../sql/duckdb/templates/reload_rebuild_drop.sql");
const WIPE_GENERATION: &str = include_str!("../sql/duckdb/templates/wipe_generation.sql");
const MIGRATE_RAW_REPLAY_FENCE: &str =
    include_str!("../sql/duckdb/templates/migrate_raw_replay_fence.sql");

const CREATE_DUCKLAKE_META: &str = r#"
CREATE TABLE IF NOT EXISTS "_walrus_meta" (k VARCHAR, v BIGINT);
INSERT INTO "_walrus_meta"
SELECT 'schema_version', {schema_version}
WHERE NOT EXISTS (SELECT 1 FROM "_walrus_meta" WHERE k = 'schema_version');
"#;

const CREATE_DUCKLAKE_LEDGER: &str = r#"
CREATE TABLE IF NOT EXISTS "_walrus_ingested_files" (
    "s3_uri" VARCHAR NOT NULL,
    "manifest_id" BIGINT NOT NULL
);
"#;

/// Owns one table's DuckDB execution connection (mirror `<table>` + CDC log `<table>_raw`).
///
/// The owned handles are `Send`, but their shared references are not: `&T: Send` requires `T: Sync`,
/// while both types contain a `RefCell`. These compile-fail guards pin the boundary that prevents the
/// current borrowed connection API from satisfying `spawn_blocking`'s closure bound.
///
/// ```compile_fail,E0277
/// fn requires_send<T: Send>() {}
/// requires_send::<&'static duckdb::Connection>();
/// ```
///
/// ```compile_fail,E0277
/// fn requires_send<T: Send>() {}
/// requires_send::<&'static loader::duck::TableDb>();
/// ```
#[derive(Debug)]
pub struct TableDb {
    conn: duckdb::Connection,
    backend: Backend,
    /// Parquet column lists by `schema_version`. A version's file shape is immutable — the
    /// sink's homogeneous-file rule (walrus-pg-sink §3.5) cuts a fresh file at every DDL bump, so all
    /// files at one version share their columns and a DDL bump is a *new* key. So this cache never
    /// invalidates, and a Phase-A cycle claiming N same-version files runs one `DESCRIBE`, not N.
    /// `RefCell` provides interior mutability behind `&self`. `TableDb` is `Send + !Sync`:
    /// duckdb-rs declares `Connection: Send`, but the connection's `RefCell<InnerConnection>` and
    /// this cache's `RefCell` prevent shared access. That `!Sync` makes a future holding `&TableCtx`
    /// non-`Send`, hence one apply worker per `.duckdb` file on a `LocalSet`. Those tasks share one
    /// driver thread, so a long DuckDB call can delay sibling tables.
    /// `Arc<[String]>` keeps reads to one indirection while preserving `TableDb: Send`. The `Rc`
    /// this `LocalSet`-confined cache would otherwise invite is declined: `Rc` is `!Send`, so it
    /// would break `assert_send::<TableDb>()` below and foreclose the owned-move redesign that
    /// note leaves open.
    parquet_cols: RefCell<HashMap<SchemaVersionNo, Arc<[String]>>>,
    /// `true` only for a database created by a pre-ledger Walrus release. Such a raw table keeps
    /// its row-level primary key until Phase A has drained every possibly replayed manifest, then a
    /// one-time transactional CTAS removes it. Fresh databases never pay the per-row index cost.
    legacy_raw_replay_pk: Cell<bool>,
}

#[derive(Debug, Clone)]
enum Backend {
    Native,
    DuckLake(Box<DuckLakeTable>),
}

/// Names required to bridge the existing per-table SQL into one shared DuckLake catalog.
///
/// Every connection sets its default schema to `internal_schema`, so the transform can continue to
/// use the short `<table>`, `<table>_raw`, `_walrus_meta`, and `_walrus_ingested_files` names. Only
/// the compatibility view is published outside this schema.
#[derive(Debug, Clone)]
struct DuckLakeTable {
    attach_name: String,
    internal_schema: String,
    source_schema: String,
    source_table: String,
}

impl TableDb {
    /// Open (or create) the file read-write, taking DuckDB's file lock. A stale lock behind an expired
    /// lease has already been reclaimed by the caller; a *live* owner would make this fail.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if DuckDB cannot open the file or acquire its writer lock.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LoaderError> {
        let path = path.as_ref();
        let conn =
            duckdb::Connection::open(path).duck_with(|| format!("open {}", path.display()))?;
        Ok(TableDb {
            conn,
            backend: Backend::Native,
            parquet_cols: RefCell::new(HashMap::new()),
            legacy_raw_replay_pk: Cell::new(false),
        })
    }

    /// Open a transient DuckDB connection, load the pinned extensions, attach the shared DuckLake,
    /// and select this source table's isolated internal schema.
    ///
    /// The PostgreSQL URI is first stored in a temporary DuckDB secret and is never interpolated into
    /// the `ATTACH` path or an operation label, preventing driver errors from echoing credentials.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if extensions, credentials, the DuckLake attachment, or the
    /// table's namespace cannot be configured.
    pub fn open_ducklake(
        cfg: &DuckLakeConfig,
        _epoch: EpochNo,
        source_schema: &str,
        source_table: &str,
        s3: &S3Access,
    ) -> Result<Self, LoaderError> {
        let conn = attach_ducklake(cfg, s3, false)?;
        let attach = ident(&cfg.attach_name)?;

        let internal_schema = internal_schema(source_schema, source_table);
        let internal = ident(&internal_schema)?;
        let public = ident(source_schema)?;
        conn.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {attach}.{internal};\n\
             CREATE SCHEMA IF NOT EXISTS {attach}.{public};\n\
             USE {attach}.{internal};"
        ))
        .duck_with(|| format!("select DuckLake namespace for {source_schema}.{source_table}"))?;

        Ok(Self {
            conn,
            backend: Backend::DuckLake(Box::new(DuckLakeTable {
                attach_name: cfg.attach_name.clone(),
                internal_schema,
                source_schema: source_schema.to_string(),
                source_table: source_table.to_string(),
            })),
            parquet_cols: RefCell::new(HashMap::new()),
            legacy_raw_replay_pk: Cell::new(false),
        })
    }

    /// Whether this connection writes to the shared DuckLake rather than a native test database.
    #[must_use]
    pub const fn is_ducklake(&self) -> bool {
        matches!(self.backend, Backend::DuckLake(_))
    }

    /// `CREATE TABLE IF NOT EXISTS` for BOTH the mirror `<table>` and the CDC log `<table>_raw`
    /// (a heap; file-level replay is fenced by `_walrus_ingested_files`), the user-facing
    /// `<table>_current` view, and a `_walrus_meta` row seeding this table's `schema_version`
    /// (the DDL-reconcile watermark).
    /// The seed is `ON CONFLICT DO NOTHING`, so an EXISTING `.duckdb` keeps its persisted, already-
    /// reconciled version across restarts — the additive DDL applier ([`crate::ddl`]) advances it.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if DuckDB cannot create or reconcile the planned tables and view.
    pub fn ensure_tables(
        &self,
        rel: &PgRelation,
        schema_version: SchemaVersionNo,
    ) -> Result<(), LoaderError> {
        self.ensure_tables_planned(&crate::plan::TablePlan::tier1(rel), schema_version)
    }

    /// As [`TableDb::ensure_tables`], but from a full [`TablePlan`] — the mirror carries the recombined
    /// target types and `<table>_raw` the verbatim emit columns (Tier-2 decomposition). The
    /// Tier-1 plan produces exactly the scalar shape `ensure_tables` always built.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if any mirror, raw-table, view, or metadata DDL statement fails.
    pub(crate) fn ensure_tables_planned(
        &self,
        plan: &TablePlan,
        schema_version: SchemaVersionNo,
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
        let primary_key = if self.is_ducklake() || keys.is_empty() {
            String::new()
        } else {
            format!(", PRIMARY KEY ({})", keys.join(", "))
        };
        let mirror = CREATE_MIRROR
            .replace("{table}", table)
            .replace("{cols}", &cols.join(", "))
            .replace("{primary_key}", &primary_key);
        // Idempotent back-fill for a mirror created before the applied-LSN guard (compose resume).
        let applied_cols = ALTER_ADD_APPLIED.replace("{table}", table);
        // The user-facing projection: the mirror WITHOUT the internal guard columns (DoD §7 "hidden from
        // user projections"). Users read `<table>_current`; `_applied_*` never leak. Recreated by the DDL
        // applier after any structural change (a `SELECT *` view binds its columns at creation time).
        let user_view = user_view_sql(table);
        // The per-table DDL-reconcile watermark. Seeded once; the applier advances it.
        let meta = if self.is_ducklake() {
            CREATE_DUCKLAKE_META.replace("{schema_version}", &schema_version.to_string())
        } else {
            CREATE_META.replace("{schema_version}", &schema_version.to_string())
        };
        let ledger = if self.is_ducklake() {
            CREATE_DUCKLAKE_LEDGER
        } else {
            CREATE_INGEST_LEDGER
        };

        // The CDC log: every change verbatim (the emit columns), with the intact
        // `walrus_pg_sink_meta` JSON plus four promoted columns. It is deliberately a HEAP: a
        // per-row composite primary key made each append build an ART index even though replay is a
        // per-file event. `_walrus_ingested_files` is the much smaller idempotency fence.
        let raw = CREATE_RAW
            .replace("{table}", table)
            .replace("{raw_cols}", &raw_cols.join(", "));

        self.conn
            .execute_batch(&format!(
                "{mirror} {applied_cols} {raw} {user_view} {meta} {ledger}"
            ))
            .duck_with(|| format!("ensure tables for {table}"))?;
        // `CREATE TABLE IF NOT EXISTS` intentionally leaves an upgraded raw table's old primary
        // key intact. Phase A uses it as a compatibility fence until all potentially pre-appended
        // manifests have drained, then calls `migrate_legacy_replay_fence` exactly once.
        self.legacy_raw_replay_pk
            .set(!self.is_ducklake() && self.raw_has_primary_key(table)?);
        self.publish_current_view()?;
        Ok(())
    }

    /// Configure DuckDB's bundled httpfs for the S3/MinIO staging bucket — **once per connection**, so
    /// `read_parquet('s3://…')` then needs no per-call credentials. For MinIO the endpoint is
    /// `host:port` (no scheme), path-style, TLS off.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] when DuckDB rejects the httpfs/S3 configuration statements.
    pub fn configure_s3(&self, s3: &S3Access) -> Result<(), LoaderError> {
        if self.is_ducklake() {
            // `open_ducklake` creates a modern secret before ATTACH so both staging reads and
            // DuckLake-owned writes use the same refreshable credential source.
            return Ok(());
        }
        let esc = common::sql::sql_literal;
        let use_ssl = if s3.use_ssl { "true" } else { "false" };
        let sql = CONFIGURE_S3
            .replace("{region}", &esc(&s3.region))
            .replace("{endpoint}", &esc(&s3.endpoint))
            .replace("{use_ssl}", use_ssl)
            .replace("{access_key}", &esc(&s3.access_key_id))
            .replace("{secret_key}", &esc(s3.secret_access_key.expose()));
        self.conn.execute_batch(&sql).duck("configure S3")
    }

    /// Phase A: append one Parquet file **verbatim** into `<table>_raw`, promoting
    /// `op`/`commit_lsn`/`lsn`/`sink_processed_at` out of `walrus_pg_sink_meta`. The append and a
    /// marker in `_walrus_ingested_files` commit in one DuckDB transaction; a replay of the same
    /// immutable object URI returns zero without reopening the Parquet. **Never touches the mirror.**
    ///
    /// `commit_lsn_override`: for a **speculative-spill** file (manifest `kind = 'spill'`)
    /// the per-row `commit_lsn` in the Parquet is a *placeholder* — the file was written before its txn's
    /// commit LSN was known. A spill file is one whole transaction, so its authoritative `commit_lsn` is
    /// the file's `lsn_end` (stamped on the manifest at `Stream Commit`); passing `Some(lsn_end)` here
    /// stamps every appended row with it, so a concurrently-committed neighbour txn is never dropped by
    /// the transform's commit-LSN window (architecture.md §1.6). `None` keeps the verbatim per-row value.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Ident`] if the Parquet schema contains a column name that cannot be
    /// represented as a SQL identifier, or [`LoaderError::Duck`] if the schema cannot be inspected
    /// or its rows cannot be appended into the raw table.
    pub fn append_parquet(
        &self,
        table: &str,
        manifest_id: ManifestId,
        s3_uri: &str,
        schema_version: SchemaVersionNo,
        commit_lsn_override: Option<&str>,
    ) -> Result<u64, LoaderError> {
        let uri = common::sql::sql_literal(s3_uri);
        let on_conflict = if self.legacy_raw_replay_pk.get() {
            " ON CONFLICT DO NOTHING"
        } else {
            ""
        };
        self.in_txn("append manifest", |conn| {
            let ingested: bool = conn
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM \"_walrus_ingested_files\" WHERE s3_uri = ?)",
                    [s3_uri],
                    |row| row.get(0),
                )
                .duck_with(|| format!("check ingest marker for {s3_uri}"))?;
            if ingested {
                return Ok(0);
            }

            // Map the file's columns into `<table>_raw` **by name**, not by position.
            // The list is cached per `schema_version`. This happens after the marker check,
            // so a crash-window replay does not require the staged object to remain readable.
            let file_cols = self.columns_for(&uri, schema_version)?;
            let quoted = file_cols
                .iter()
                .map(|column| {
                    common::sql::SqlIdent::new(column)
                        .map(|ident| ident.to_string())
                        .map_err(|source| LoaderError::Ident {
                            uri: s3_uri.to_string(),
                            source,
                        })
                })
                .collect::<Result<Vec<_>, LoaderError>>()?
                .join(", ");
            let commit_lsn_expr = match commit_lsn_override {
                Some(lsn) => lsn.to_quoted_literal(),
                None => "json_extract_string(walrus_pg_sink_meta, '$.commit_lsn')".to_string(),
            };
            let sql = APPEND_PARQUET
                .replace("{table}", table)
                .replace("{quoted}", &quoted)
                .replace("{commit_lsn_expr}", &commit_lsn_expr)
                .replace("{uri}", &uri)
                .replace("{on_conflict}", on_conflict);
            let n = conn
                .execute(&sql, [])
                .duck_with(|| format!("append {s3_uri} → {table}_raw"))?;
            conn.execute(
                "INSERT INTO \"_walrus_ingested_files\" (s3_uri, manifest_id) VALUES (?, ?)",
                duckdb::params![s3_uri, manifest_id.0],
            )
            .duck_with(|| format!("record ingest marker for {s3_uri}"))?;
            Ok(u64::try_from(n).unwrap_or(u64::MAX))
        })
    }

    /// Whether this upgraded database still carries the old per-row replay index.
    #[must_use]
    pub(crate) const fn has_legacy_replay_fence(&self) -> bool {
        self.legacy_raw_replay_pk.get()
    }

    /// Replace an upgraded raw table with an identical heap after Phase A has proved there are no
    /// pending manifests that might have been appended by the old implementation. The CTAS
    /// replacement is transactional, so a crash leaves either the indexed old table or the complete
    /// heap, never a partial copy. Returns whether a migration ran.
    pub(crate) fn migrate_legacy_replay_fence(&self, table: &str) -> Result<bool, LoaderError> {
        if !self.legacy_raw_replay_pk.get() {
            return Ok(false);
        }
        if !self.raw_has_primary_key(table)? {
            self.legacy_raw_replay_pk.set(false);
            return Ok(false);
        }
        self.in_txn("migrate raw replay fence", |conn| {
            conn.execute_batch(&MIGRATE_RAW_REPLAY_FENCE.replace("{table}", table))
                .duck_with(|| format!("remove legacy replay primary key from {table}_raw"))
        })?;
        self.legacy_raw_replay_pk.set(false);
        Ok(true)
    }

    fn raw_has_primary_key(&self, table: &str) -> Result<bool, LoaderError> {
        self.conn
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM duckdb_constraints() \
                 WHERE table_name = ? AND constraint_type = 'PRIMARY KEY')",
                [format!("{table}_raw")],
                |row| row.get(0),
            )
            .duck_with(|| format!("inspect replay constraint on {table}_raw"))
    }

    /// The Parquet column list for `schema_version`, introspecting `uri` **once** per version and
    /// caching it (sound by the homogeneous-file rule — see [`TableDb::parquet_cols`]).
    fn columns_for(
        &self,
        uri: &str,
        schema_version: SchemaVersionNo,
    ) -> Result<Arc<[String]>, LoaderError> {
        // The shared borrow is released before the miss path: an `entry` call would hold the
        // `RefCell` across the DESCRIBE below, and one saved hash is nothing beside that query.
        let cached = { self.parquet_cols.borrow().get(&schema_version).cloned() };
        if let Some(columns) = cached {
            return Ok(columns);
        }
        let cols: Arc<[String]> = self.parquet_columns(uri)?.into();
        self.parquet_cols
            .borrow_mut()
            .insert(schema_version, Arc::clone(&cols));
        Ok(cols)
    }

    /// Number of distinct `schema_version`s whose column list is cached; exposed only to tests.
    #[cfg(test)]
    pub fn cached_schema_versions(&self) -> usize {
        self.parquet_cols.borrow().len()
    }

    /// The column names of a staged Parquet file, in file order (source columns + `walrus_pg_sink_meta`).
    fn parquet_columns(&self, uri: &str) -> Result<Vec<String>, LoaderError> {
        let mut stmt = self
            .conn
            .prepare(&format!("DESCRIBE SELECT * FROM read_parquet('{uri}')"))
            .duck_with(|| format!("describe {uri}"))?;
        let cols = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .duck_with(|| format!("describe {uri}"))?
            .collect::<Result<Vec<_>, _>>()
            .duck_with(|| format!("describe {uri}"))?;
        Ok(cols)
    }

    /// The `.duckdb` connection used by transform, compaction, and schema-reconciliation callers.
    ///
    /// Handing out the borrow does nothing on its own — the caller still has to run something
    /// through it — so a discarded call is a no-op. `clippy::must_use_candidate` cannot say so: the
    /// `parquet_cols` `RefCell` behind `&self` reads to that lint as a mutable — therefore
    /// side-effecting — argument, so the attribute is spelled out.
    #[must_use]
    pub const fn conn(&self) -> &duckdb::Connection {
        &self.conn
    }

    /// Run `f` inside one DuckDB transaction. Commits on success and attempts a best-effort rollback
    /// on error, so callers cannot forget the unhappy path. `what` names the transaction in the
    /// begin/commit error contexts and rollback warning.
    ///
    /// Bounded [`FnOnce`] because the body runs exactly once and may consume what it captured. This
    /// is the weakest bound that works, accepting move-consuming closures too. There is no `Send`
    /// bound: the body runs inline on the owning worker's `LocalSet` and never crosses a task or
    /// thread boundary.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if beginning or committing the transaction fails, or returns
    /// the body's error unchanged after attempting to roll back.
    pub fn in_txn<T>(
        &self,
        what: &str,
        f: impl FnOnce(&duckdb::Connection) -> Result<T, LoaderError>,
    ) -> Result<T, LoaderError> {
        self.conn
            .execute_batch("BEGIN TRANSACTION;")
            .duck_with(|| format!("begin {what} txn"))?;
        match f(&self.conn) {
            Ok(value) => {
                self.conn
                    .execute_batch("COMMIT;")
                    .duck_with(|| format!("commit {what} txn"))?;
                Ok(value)
            }
            Err(error) => {
                if let Err(rollback) = self.conn.execute_batch("ROLLBACK;") {
                    tracing::warn!(
                        error = %rollback,
                        transaction = what,
                        "transaction rollback failed; connection may be wedged"
                    );
                }
                Err(error)
            }
        }
    }

    /// This table's currently-reconciled `schema_version` (the `_walrus_meta` watermark). Persisted in
    /// the `.duckdb` file, so a restart resumes at the exact version its columns are already at.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if the metadata watermark cannot be read.
    pub fn schema_version(&self) -> Result<SchemaVersionNo, LoaderError> {
        let version = self
            .conn
            .query_row(
                "SELECT v FROM \"_walrus_meta\" WHERE k = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .duck("read schema_version")?;
        Ok(SchemaVersionNo(version))
    }

    /// The persisted watermark read **before** the seeding `ensure_tables*`, or `None` when this
    /// `.duckdb` has none yet (a brand-new file has no `_walrus_meta` at all). Only bootstrap needs
    /// this: it asks what version a file is already at while deciding whether to reconcile it
    /// forward, so it meets both a fresh and a resumed file. Every later caller runs after the seed
    /// and uses the probe-free [`TableDb::schema_version`], which Phase A reads per file.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if the metadata table probe or the watermark read fails — a
    /// real DuckDB fault, kept distinct from the absent watermark `Ok(None)` reports.
    pub fn stored_schema_version(&self) -> Result<Option<SchemaVersionNo>, LoaderError> {
        // A brand-new file has no `_walrus_meta` yet — probe first, exactly as `built_epoch` does.
        if !self.has_metadata_table()? {
            return Ok(None);
        }
        // `max(v)` yields one row with NULL (→ `None`) when the key is absent, as in `built_epoch`.
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT max(v) FROM \"_walrus_meta\" WHERE k = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .duck("read stored schema_version")?;
        Ok(v.map(SchemaVersionNo))
    }

    /// Advance the reconcile watermark after the additive DDL for `v` has been applied to both tables.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if updating the persisted schema watermark fails.
    pub fn set_schema_version(&self, version: SchemaVersionNo) -> Result<(), LoaderError> {
        self.conn
            .execute(
                "UPDATE \"_walrus_meta\" SET v = ? WHERE k = 'schema_version'",
                duckdb::params![version.0],
            )
            .duck("set schema_version")?;
        Ok(())
    }

    /// The **epoch (generation)** this `.duckdb` was last built for (`_walrus_meta['epoch']`), or `None`
    /// if never stamped (a brand-new file — no `_walrus_meta` yet — or a pre-4.6 file). A value below the
    /// control-plane epoch means the mirror + raw hold a **retired generation** (total-restart, §1.8) and
    /// must be wiped before the new-epoch snapshot reloads.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if the metadata table probe or epoch read fails.
    pub fn built_epoch(&self) -> Result<Option<EpochNo>, LoaderError> {
        // A brand-new file has no `_walrus_meta` yet — probe first so this never errors on it.
        if !self.has_metadata_table()? {
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
            .duck("read built epoch")?;
        Ok(v.map(EpochNo))
    }

    /// Stamp the generation this `.duckdb` is now built for (`_walrus_meta['epoch']`). Upserts, so it both
    /// records a fresh file's epoch and re-stamps a rebuilt file's new epoch.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if the epoch upsert fails.
    pub fn set_built_epoch(&self, epoch: EpochNo) -> Result<(), LoaderError> {
        if self.is_ducklake() {
            return self.replace_meta("epoch", epoch.0);
        }
        self.conn
            .execute(
                "INSERT INTO \"_walrus_meta\" (k, v) VALUES ('epoch', ?) \
                 ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                duckdb::params![epoch.0],
            )
            .duck("set built epoch")?;
        Ok(())
    }

    /// The highest `reload_id` this `.duckdb` has rebuilt for — the H8 idempotency latch.
    /// `None` when the latch was never set, so the first attempt of any kind triggers a rebuild; a
    /// caller can no longer confuse "never rebuilt" with a real id. `max(v)` yields NULL (→ `None`)
    /// when the key is missing, mirroring [`TableDb::built_epoch`]'s probe-free read.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if the reload latch cannot be read.
    pub fn recorded_reload_id(&self) -> Result<Option<ReloadId>, LoaderError> {
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT max(v) FROM \"_walrus_meta\" WHERE k = 'reload_id'",
                [],
                |r| r.get(0),
            )
            .duck("read recorded reload_id")?;
        Ok(v.map(ReloadId))
    }

    /// Latch the reload generation this `.duckdb` is now rebuilt for. With a monotonic bigserial
    /// id, "latest wins" (H9) is this upsert plus a numeric compare at the trigger.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if the reload-id upsert fails.
    pub fn set_recorded_reload_id(&self, reload_id: ReloadId) -> Result<(), LoaderError> {
        if self.is_ducklake() {
            return self.replace_meta("reload_id", reload_id.0);
        }
        self.conn
            .execute(
                "INSERT INTO \"_walrus_meta\" (k, v) VALUES ('reload_id', ?) \
                 ON CONFLICT (k) DO UPDATE SET v = excluded.v",
                duckdb::params![reload_id.0],
            )
            .duck("set recorded reload_id")?;
        Ok(())
    }

    fn has_metadata_table(&self) -> Result<bool, LoaderError> {
        let count: i64 = match &self.backend {
            Backend::Native => self
                .conn
                .query_row(
                    "SELECT count(*) FROM information_schema.tables \
                     WHERE table_name = '_walrus_meta'",
                    [],
                    |row| row.get(0),
                )
                .duck("probe _walrus_meta")?,
            Backend::DuckLake(names) => self
                .conn
                .query_row(
                    "SELECT count(*) FROM information_schema.tables \
                     WHERE table_catalog = ? AND table_schema = ? \
                       AND table_name = '_walrus_meta'",
                    duckdb::params![names.attach_name, names.internal_schema],
                    |row| row.get(0),
                )
                .duck("probe DuckLake _walrus_meta")?,
        };
        Ok(count > 0)
    }

    /// The reload rebuild (reload H8, §5 step 4): atomically replace BOTH tables at the
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
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if dropping, recreating, or stamping either table fails.
    pub(crate) fn rebuild_for_reload(
        &self,
        plan: &TablePlan,
        schema_version: SchemaVersionNo,
    ) -> Result<(), LoaderError> {
        let table = &plan.table;
        self.unpublish_current_view()?;
        self.conn
            .execute_batch(&RELOAD_REBUILD_DROP.replace("{table}", table))
            .duck_with(|| format!("reload rebuild drop for {table}"))?;
        self.ensure_tables_planned(plan, schema_version)?;
        self.set_schema_version(schema_version)?;
        Ok(())
    }

    /// Wipe a retired generation from this `.duckdb` (total-restart, §1.8): drop the user view, the mirror,
    /// the CDC log, and `_walrus_meta`. The caller then re-runs `ensure_tables*` to recreate them empty, so
    /// the fresh new-epoch snapshot re-appends into `<table>_raw` and the transform re-derives `<table>`
    /// from scratch (both watermarks reset — the new epoch's `loader_checkpoint` is a fresh `0/0`).
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if DuckDB cannot drop the retired generation's objects.
    pub fn wipe_generation(&self, table: &str) -> Result<(), LoaderError> {
        self.unpublish_current_view()?;
        self.conn
            .execute_batch(&WIPE_GENERATION.replace("{table}", table))
            .duck_with(|| format!("wipe generation for {table}"))?;
        self.legacy_raw_replay_pk.set(false);
        Ok(())
    }

    /// Re-publish the stable source-schema read contract after creating or evolving the internal
    /// mirror view. Native test databases have no shared catalog and therefore need no second view.
    pub(crate) fn publish_current_view(&self) -> Result<(), LoaderError> {
        let Backend::DuckLake(names) = &self.backend else {
            return Ok(());
        };
        let catalog = ident(&names.attach_name)?;
        let source_schema = ident(&names.source_schema)?;
        let internal_schema = ident(&names.internal_schema)?;
        let public_view = ident(&format!("{}_current", names.source_table))?;
        let internal_view = ident(&format!("{}_current", names.source_table))?;
        self.conn
            .execute_batch(&format!(
                "CREATE OR REPLACE VIEW {catalog}.{source_schema}.{public_view} AS \
                 SELECT * FROM {catalog}.{internal_schema}.{internal_view};"
            ))
            .duck_with(|| {
                format!(
                    "publish DuckLake view {}.{}_current",
                    names.source_schema, names.source_table
                )
            })
    }

    pub(crate) fn unpublish_current_view(&self) -> Result<(), LoaderError> {
        let Backend::DuckLake(names) = &self.backend else {
            return Ok(());
        };
        self.conn
            .execute_batch(&format!(
                "DROP VIEW IF EXISTS {}.{}.{};",
                ident(&names.attach_name)?,
                ident(&names.source_schema)?,
                ident(&format!("{}_current", names.source_table))?
            ))
            .duck_with(|| {
                format!(
                    "unpublish DuckLake view {}.{}_current",
                    names.source_schema, names.source_table
                )
            })
    }

    fn replace_meta(&self, key: &str, value: i64) -> Result<(), LoaderError> {
        self.in_txn("replace DuckLake table metadata", |conn| {
            conn.execute("DELETE FROM \"_walrus_meta\" WHERE k = ?", [key])
                .duck_with(|| format!("delete old {key} metadata"))?;
            conn.execute(
                "INSERT INTO \"_walrus_meta\" (k, v) VALUES (?, ?)",
                duckdb::params![key, value],
            )
            .duck_with(|| format!("insert {key} metadata"))?;
            Ok(())
        })
    }

    /// Compact this table's newly written files and rewrite delete-heavy files. Snapshot expiration
    /// is deliberately catalog-level and runs in the singleton maintenance loop instead.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] if any DuckLake maintenance procedure fails.
    pub fn maintain_files(&self, table: &str) -> Result<(), LoaderError> {
        let Backend::DuckLake(names) = &self.backend else {
            return Ok(());
        };
        let catalog = names.attach_name.to_quoted_literal();
        let schema = names.internal_schema.to_quoted_literal();
        let mirror = table.to_quoted_literal();
        let raw = format!("{table}_raw").to_quoted_literal();
        self.conn
            .execute_batch(&format!(
                "CALL ducklake_merge_adjacent_files({catalog}, {mirror}, schema => {schema});\n\
                 CALL ducklake_merge_adjacent_files({catalog}, {raw}, schema => {schema});\n\
                 CALL ducklake_rewrite_data_files({catalog}, {mirror}, schema => {schema});\n\
                 CALL ducklake_rewrite_data_files({catalog}, {raw}, schema => {schema});"
            ))
            .duck_with(|| format!("maintain DuckLake files for {table}"))
    }
}

/// Install the exact set of dynamic extensions required by the production loader into `directory`.
/// This is invoked by the image build (`walrus-loader --install-duckdb-extensions …`), not by the
/// normal service lifecycle.
///
/// # Errors
///
/// Returns [`LoaderError::File`] if `directory` cannot be created, or [`LoaderError::Duck`] if an
/// extension cannot be installed there.
pub fn install_extensions(directory: &Path) -> Result<(), LoaderError> {
    std::fs::create_dir_all(directory).map_err(|source| LoaderError::File {
        op: "create extension directory",
        path: directory.display().to_string(),
        source,
    })?;
    let conn = duckdb::Connection::open_in_memory().duck("open extension installer")?;
    let directory = directory.to_string_lossy().to_quoted_literal();
    conn.execute_batch(&format!("SET extension_directory = {directory};"))
        .duck("set extension install directory")?;
    for extension in EXTENSIONS {
        conn.execute_batch(&format!("INSTALL {extension};"))
            .duck_with(|| format!("install DuckDB extension {extension}"))?;
    }
    Ok(())
}

fn configure_extensions(
    conn: &duckdb::Connection,
    cfg: &DuckLakeConfig,
) -> Result<(), LoaderError> {
    if let Some(directory) = &cfg.extension_directory {
        conn.execute_batch(&format!(
            "SET extension_directory = {};",
            directory.to_quoted_literal()
        ))
        .duck("set extension directory")?;
    }
    if cfg.install_extensions {
        for extension in EXTENSIONS {
            conn.execute_batch(&format!("INSTALL {extension};"))
                .duck_with(|| format!("install DuckDB extension {extension}"))?;
        }
    }
    for extension in EXTENSIONS {
        conn.execute_batch(&format!("LOAD {extension};"))
            .duck_with(|| format!("load DuckDB extension {extension}"))?;
    }
    // Once every required artifact is resident, prohibit a typo or optional code path from fetching
    // and executing a new extension at runtime.
    conn.execute_batch(
        "SET autoinstall_known_extensions = false; SET autoload_known_extensions = false;",
    )
    .duck("disable runtime extension downloads")
}

fn attach_ducklake(
    cfg: &DuckLakeConfig,
    s3: &S3Access,
    automatic_migration: bool,
) -> Result<duckdb::Connection, LoaderError> {
    let conn = duckdb::Connection::open_in_memory().duck("open transient DuckDB")?;
    configure_extensions(&conn, cfg)?;
    configure_s3_secret(&conn, s3)?;

    let attach = ident(&cfg.attach_name)?;
    let metadata_schema = cfg.metadata_schema.to_quoted_literal();
    let data_path = cfg.data_path.to_quoted_literal();
    let catalog_uri = cfg.catalog_url.expose().to_quoted_literal();
    let migration = if automatic_migration {
        ", AUTOMATIC_MIGRATION true"
    } else {
        ""
    };
    let create_if_not_exists = if automatic_migration { "true" } else { "false" };
    conn.execute_batch(&format!(
        "CREATE OR REPLACE SECRET walrus_catalog (TYPE postgres, URI {catalog_uri});\n\
         ATTACH 'ducklake:postgres:' AS {attach} (META_SECRET 'walrus_catalog', \
             METADATA_SCHEMA {metadata_schema}, META_SCHEMA {metadata_schema}, DATA_PATH {data_path}, \
             DATA_INLINING_ROW_LIMIT 0, CREATE_IF_NOT_EXISTS {create_if_not_exists}, \
             OVERRIDE_DATA_PATH false{migration});"
    ))
    .duck("attach DuckLake catalog")?;
    Ok(conn)
}

/// Run catalog-wide retention and physical cleanup. Callers serialize this to shard zero; all
/// thresholds are explicit so upgrading DuckLake cannot silently change the storage policy.
///
/// # Errors
///
/// Returns [`LoaderError::Duck`] if attachment, snapshot expiration, or file cleanup fails.
pub fn maintain_catalog(cfg: &DuckLakeConfig, s3: &S3Access) -> Result<(), LoaderError> {
    let conn = attach_ducklake(cfg, s3, false)?;
    let catalog = cfg.attach_name.to_quoted_literal();
    let snapshot_seconds = cfg.snapshot_retention.as_secs();
    let cleanup_seconds = cfg.cleanup_grace.as_secs();
    conn.execute_batch(&format!(
        "CALL ducklake_expire_snapshots({catalog}, \
             older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{snapshot_seconds} seconds');\n\
         CALL ducklake_cleanup_old_files({catalog}, \
             older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{cleanup_seconds} seconds');\n\
         CALL ducklake_delete_orphaned_files({catalog}, \
             older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{cleanup_seconds} seconds');"
    ))
    .duck("run DuckLake catalog maintenance")
}

/// Explicitly create or migrate the DuckLake catalog with the pinned extension version. Normal
/// service startup never enables automatic migration.
///
/// # Errors
///
/// Returns [`LoaderError::Duck`] if the catalog cannot be attached, created, migrated, or verified.
pub fn migrate_catalog(cfg: &DuckLakeConfig, s3: &S3Access) -> Result<(), LoaderError> {
    let conn = attach_ducklake(cfg, s3, true)?;
    conn.execute_batch("SELECT 1;")
        .duck("verify migrated DuckLake catalog")
}

fn configure_s3_secret(conn: &duckdb::Connection, s3: &S3Access) -> Result<(), LoaderError> {
    let region = s3.region.to_quoted_literal();
    let sql = if s3.endpoint.is_empty() && s3.access_key_id.is_empty() {
        format!(
            "CREATE OR REPLACE SECRET walrus_s3 (TYPE s3, PROVIDER credential_chain, \
             REGION {region}, REFRESH auto);"
        )
    } else {
        let endpoint = s3.endpoint.to_quoted_literal();
        let key = s3.access_key_id.to_quoted_literal();
        let secret = s3.secret_access_key.expose().to_quoted_literal();
        let ssl = if s3.use_ssl { "true" } else { "false" };
        format!(
            "CREATE OR REPLACE SECRET walrus_s3 (TYPE s3, PROVIDER config, KEY_ID {key}, \
             SECRET {secret}, REGION {region}, ENDPOINT {endpoint}, URL_STYLE 'path', \
             USE_SSL {ssl});"
        )
    };
    conn.execute_batch(&sql).duck("configure S3 secret")
}

fn internal_schema(source_schema: &str, source_table: &str) -> String {
    let id = table_uuid(source_schema, source_table);
    format!("_walrus_{}", id.simple())
}

fn table_uuid(source_schema: &str, source_table: &str) -> uuid::Uuid {
    let key = format!("{source_schema}\0{source_table}");
    uuid::Uuid::new_v5(&TABLE_NAMESPACE, key.as_bytes())
}

/// Stable deterministic shard for a registered source table.
#[must_use]
pub fn table_shard(
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
    shard_count: NonZeroU32,
) -> u32 {
    let table = format!("{}\0{source_schema}\0{source_table}", epoch.0);
    let score = |shard| {
        let candidate = format!("{table}\0{shard}");
        u128::from_be_bytes(uuid::Uuid::new_v5(&TABLE_NAMESPACE, candidate.as_bytes()).into_bytes())
    };
    let mut best = (0, score(0));
    for shard in 1..shard_count.get() {
        let candidate = score(shard);
        if candidate > best.1 {
            best = (shard, candidate);
        }
    }
    best.0
}

fn ident(raw: &str) -> Result<common::sql::SqlIdent, LoaderError> {
    common::sql::SqlIdent::new(raw)
        .map_err(|source| LoaderError::Internal(format!("DuckLake identifier {raw:?}: {source}")))
}

/// The user-facing `<table>_current` view: the mirror minus the hidden `_applied_*` guard columns
/// (§7). A `SELECT *` view binds its column list at creation, so the DDL applier ([`crate::ddl`])
/// re-runs this after any structural change to pick up added/renamed columns.
pub(crate) fn user_view_sql(table: &str) -> String {
    CREATE_USER_VIEW.replace("{table}", table)
}

/// DuckDB S3/httpfs credentials for reading the staging bucket.
///
/// Only the secret half is [`Redacted`]: an access key id is an identifier that already appears in
/// bucket policies and audit trails, while `secret_access_key` *is* the credential — and this
/// struct derives `Debug`, which would otherwise put it one `?s3` away from a log line.
#[derive(Debug, Clone)]
pub struct S3Access {
    /// Host:port DuckDB's httpfs extension should talk to (MinIO in dev, S3 in production).
    pub endpoint: String,
    /// Region used to sign requests.
    pub region: String,
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key. Held only long enough to configure the connection; the wrapper is what
    /// keeps "never logged" a property of the type rather than a promise in a doc comment.
    pub secret_access_key: Redacted<String>,
    /// Whether to reach the endpoint over TLS. `false` for a plain-HTTP local MinIO.
    pub use_ssl: bool,
}

/// Map a Postgres type OID to a DuckDB column type. Unknown types fall back to `VARCHAR` (the loader
/// stages *text*-format tuples; the exact numeric/temporal fidelity is refined as the transform lands).
pub(crate) const fn duck_type(oid: u32) -> &'static str {
    match oid {
        INT2 => "SMALLINT",
        INT4 => "INTEGER",
        INT8 => "BIGINT",
        BOOL => "BOOLEAN",
        FLOAT4 => "REAL",
        FLOAT8 => "DOUBLE",
        NUMERIC => "DECIMAL(38,10)",
        DATE => "DATE",
        TIMESTAMP => "TIMESTAMP",
        TIMESTAMPTZ => "TIMESTAMP WITH TIME ZONE",
        UUID => "UUID",
        JSON | JSONB => "JSON",
        BYTEA => "BLOB",
        _ => "VARCHAR", // text, varchar, enums, and everything else
    }
}

// Compile-time proof of the loader's threading model. This closure is type-checked but never run or
// code-generated. The compiler derives every bound; no manual auto-trait implementation is needed.
const _: fn() = || {
    const fn assert_send<T: Send + ?Sized>() {}
    const fn assert_sync<T: Sync + ?Sized>() {}

    // Moves into `TableCtx` and then into the worker future.
    assert_send::<TableDb>();
    // The one value handed from a local worker to `tokio::spawn` during compaction drain.
    assert_send::<duckdb::InterruptHandle>();
    assert_sync::<duckdb::InterruptHandle>();
    // Shared by the health server and every worker.
    assert_send::<Arc<crate::health::LoaderState>>();
    assert_sync::<Arc<crate::health::LoaderState>>();

    // Pin walrus's own source of `!Sync`; DuckDB independently keeps the overall type `!Sync`.
    const fn cache_refcell(db: &TableDb) -> &RefCell<HashMap<SchemaVersionNo, Arc<[String]>>> {
        &db.parquet_cols
    }
    let _cache_refcell_fn = cache_refcell;
};

// Negative assertion: while the overall `TableDb` is not `Sync`, only the `()` impl applies and `_`
// resolves. If every source of `!Sync` is removed, both impls apply and this line is ambiguous. This
// is `static_assertions::assert_not_impl_all!` hand-rolled to avoid a direct dependency.
const _: fn() = || {
    trait AmbiguousIfSync<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}

    let _some_item = <TableDb as AmbiguousIfSync<_>>::some_item;
};

#[cfg(test)]
#[path = "duck_test.rs"]
mod tests;
