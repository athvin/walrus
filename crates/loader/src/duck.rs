//! One `.duckdb` file's read-write connection (loader §8.1) — holding DuckDB's single-writer file lock,
//! the **second** fence (the lease is the first). Opening read-write takes an exclusive OS lock on the
//! file: if a still-live owner held it we'd fail opaquely here, which is why the ordered bootstrap
//! proves the lease is reclaimable *before* calling [`TableDb::open`].

use crate::error::LoaderError;
use crate::plan::TablePlan;
use common::PgRelation;
use std::path::Path;

/// Owns one table's `.duckdb` connection (mirror `<table>` + CDC log `<table>_raw`).
pub struct TableDb {
    conn: duckdb::Connection,
}

impl TableDb {
    /// Open (or create) the file read-write, taking DuckDB's file lock. A stale lock behind an expired
    /// lease has already been reclaimed by the caller; a *live* owner would make this fail.
    pub fn open(path: &Path) -> Result<Self, LoaderError> {
        let conn = duckdb::Connection::open(path)
            .map_err(|e| LoaderError::Duck(format!("open {}: {e}", path.display())))?;
        Ok(TableDb { conn })
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
        let mut mirror = format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" ({}, \
             \"_applied_commit_lsn\" VARCHAR DEFAULT '0000000000000000', \
             \"_applied_lsn\" VARCHAR DEFAULT '0000000000000000'",
            cols.join(", ")
        );
        if !keys.is_empty() {
            mirror.push_str(&format!(", PRIMARY KEY ({})", keys.join(", ")));
        }
        mirror.push(')');
        // Idempotent back-fill for a mirror created before PR 3.7 (compose resume).
        let applied_cols = format!(
            "ALTER TABLE \"{table}\" ADD COLUMN IF NOT EXISTS \"_applied_commit_lsn\" VARCHAR DEFAULT '0000000000000000'; \
             ALTER TABLE \"{table}\" ADD COLUMN IF NOT EXISTS \"_applied_lsn\" VARCHAR DEFAULT '0000000000000000';"
        );
        // The user-facing projection: the mirror WITHOUT the internal guard columns (DoD §7 "hidden from
        // user projections"). Users read `<table>_current`; `_applied_*` never leak. Recreated by the DDL
        // applier after any structural change (a `SELECT *` view binds its columns at creation time).
        let user_view = user_view_sql(table);
        // The per-table DDL-reconcile watermark (PR 3.8). Seeded once; the applier advances it.
        let meta = format!(
            "CREATE TABLE IF NOT EXISTS \"_walrus_meta\" (k VARCHAR PRIMARY KEY, v BIGINT); \
             INSERT INTO \"_walrus_meta\" VALUES ('schema_version', {schema_version}) \
             ON CONFLICT (k) DO NOTHING;"
        );

        // The CDC log: every change verbatim (the emit columns), with the intact `walrus_pg_sink_meta`
        // JSON plus four columns PROMOTED out of it (op / commit_lsn / lsn / sink_processed_at) as sortable
        // 16-hex / RFC-3339 text. **Composite PK = source key + sink_processed_at + lsn** — the load-bearing
        // idempotency fence (a ms-resolution `sink_processed_at` collision is broken by the always-distinct
        // `lsn`): `ON CONFLICT DO NOTHING` makes a crash-window replay a no-op.
        let mut raw_pk = keys.clone();
        raw_pk.push("\"_walrus_sink_processed_at\"".into());
        raw_pk.push("\"_walrus_lsn\"".into());
        let raw = format!(
            "CREATE TABLE IF NOT EXISTS \"{table}_raw\" ({}, \"walrus_pg_sink_meta\" VARCHAR, \
             \"_walrus_op\" VARCHAR, \"_walrus_commit_lsn\" VARCHAR, \"_walrus_lsn\" VARCHAR, \
             \"_walrus_sink_processed_at\" VARCHAR, PRIMARY KEY ({}))",
            raw_cols.join(", "),
            raw_pk.join(", ")
        );

        self.conn
            .execute_batch(&format!(
                "{mirror}; {applied_cols} {raw}; {user_view} {meta}"
            ))
            .map_err(|e| LoaderError::Duck(format!("ensure tables for {table}: {e}")))?;
        Ok(())
    }

    /// Configure DuckDB's bundled httpfs for the S3/MinIO staging bucket — **once per connection**, so
    /// `read_parquet('s3://…')` then needs no per-call credentials. For MinIO the endpoint is
    /// `host:port` (no scheme), path-style, TLS off.
    pub fn configure_s3(&self, s3: &S3Access) -> Result<(), LoaderError> {
        let esc = |v: &str| v.replace('\'', "''");
        let use_ssl = if s3.use_ssl { "true" } else { "false" };
        let sql = format!(
            "INSTALL httpfs; LOAD httpfs; INSTALL json; LOAD json; \
             SET s3_region='{}'; SET s3_endpoint='{}'; SET s3_url_style='path'; \
             SET s3_use_ssl={use_ssl}; SET s3_access_key_id='{}'; SET s3_secret_access_key='{}';",
            esc(&s3.region),
            esc(&s3.endpoint),
            esc(&s3.access_key_id),
            esc(&s3.secret_access_key),
        );
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
        commit_lsn_override: Option<&str>,
    ) -> Result<u64, LoaderError> {
        let uri = s3_uri.replace('\'', "''");
        // Map the file's columns into `<table>_raw` **by name**, not by position (PR 3.8). After an
        // `ADD COLUMN`, DuckDB appends the new column at the physical END of `<table>_raw` (after the
        // promoted columns), while the homogeneous file carries it in source order — a positional
        // `SELECT *` would then shift the promoted extracts by one. An explicit column list also lets an
        // OLDER-version file (fewer columns) NULL-fill the columns a later version added.
        let file_cols = self.parquet_columns(&uri)?;
        let quoted = file_cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let commit_lsn_expr = match commit_lsn_override {
            Some(lsn) => format!("'{}'", lsn.replace('\'', "''")),
            None => "json_extract_string(walrus_pg_sink_meta, '$.commit_lsn')".to_string(),
        };
        let sql = format!(
            "INSERT INTO \"{table}_raw\" \
                 ({quoted}, \"_walrus_op\", \"_walrus_commit_lsn\", \"_walrus_lsn\", \
                  \"_walrus_sink_processed_at\") \
             SELECT {quoted}, \
                 json_extract_string(walrus_pg_sink_meta, '$.op'), \
                 {commit_lsn_expr}, \
                 json_extract_string(walrus_pg_sink_meta, '$.lsn'), \
                 json_extract_string(walrus_pg_sink_meta, '$.sink_processed_at') \
             FROM read_parquet('{uri}') ON CONFLICT DO NOTHING"
        );
        let n = self
            .conn
            .execute(&sql, [])
            .map_err(|e| LoaderError::Duck(format!("append {s3_uri} → {table}_raw: {e}")))?;
        Ok(n as u64)
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
}

/// The user-facing `<table>_current` view: the mirror minus the hidden `_applied_*` guard columns
/// (§7). A `SELECT *` view binds its column list at creation, so the DDL applier ([`crate::ddl`])
/// re-runs this after any structural change to pick up added/renamed columns.
pub(crate) fn user_view_sql(table: &str) -> String {
    format!(
        "CREATE OR REPLACE VIEW \"{table}_current\" AS \
         SELECT * EXCLUDE (\"_applied_commit_lsn\", \"_applied_lsn\") FROM \"{table}\";"
    )
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
mod tests {
    use super::*;
    use common::{PgColumn, PgRelation, ReplicaIdentity};

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

    /// Write a local `(id, status, walrus_pg_sink_meta)` Parquet whose rows carry `commit_lsn = placeholder`
    /// — mimicking a speculative spill written before its txn's commit LSN was known.
    fn write_local_fixture(dir: &Path, name: &str, ids: (i64, i64), placeholder: &str) -> String {
        let path = dir.join(name);
        let uri = path.to_string_lossy().replace('\'', "''");
        let w = duckdb::Connection::open_in_memory().unwrap();
        let meta = |lsn: &str| {
            format!(
                "{{\"op\":\"Insert\",\"commit_lsn\":\"{placeholder}\",\"lsn\":\"{lsn}\",\
                  \"sink_processed_at\":\"2026-07-08T12:00:0{lsn}Z\"}}"
            )
        };
        w.execute_batch(&format!(
            "CREATE TABLE fixture (id BIGINT, status VARCHAR, walrus_pg_sink_meta VARCHAR); \
             INSERT INTO fixture VALUES \
               ({}, 'a', '{}'), ({}, 'b', '{}'); \
             COPY fixture TO '{uri}' (FORMAT PARQUET);",
            ids.0,
            meta("1"),
            ids.1,
            meta("2"),
        ))
        .unwrap();
        uri
    }

    fn commit_lsns(db: &TableDb, ids: (i64, i64)) -> Vec<String> {
        let mut stmt = db
            .conn
            .prepare("SELECT \"_walrus_commit_lsn\" FROM orders_raw WHERE id IN (?, ?) ORDER BY id")
            .unwrap();
        stmt.query_map([ids.0, ids.1], |r| r.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// PR 4.3 fix: a `spill` file's per-row `commit_lsn` placeholder is overridden by the file's `lsn_end`
    /// (the real commit LSN), while a non-spill file appends the per-row value verbatim.
    #[test]
    fn spill_override_stamps_lsn_end_but_verbatim_otherwise() {
        let dir = std::env::temp_dir().join("walrus-loader-spill-override");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = TableDb::open(&dir.join("orders.duckdb")).unwrap();
        db.ensure_tables(&orders(), 1).unwrap();
        // Local Parquet + JSON extraction need the json extension (no S3 here → configure_s3 is not called).
        db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();

        // A spill file: rows carry the placeholder `0000000000000064`, but the file committed at `…00C8`.
        let placeholder = "0000000000000064";
        let lsn_end = "00000000000000C8";
        let spill = write_local_fixture(&dir, "spill.parquet", (1, 2), placeholder);
        let n = db.append_parquet("orders", &spill, Some(lsn_end)).unwrap();
        assert_eq!(n, 2);
        assert_eq!(
            commit_lsns(&db, (1, 2)),
            vec![lsn_end, lsn_end],
            "a spill file's rows are stamped with the file's lsn_end, not the placeholder"
        );

        // A non-spill (verbatim) file: the per-row placeholder is preserved.
        let batch = write_local_fixture(&dir, "batch.parquet", (3, 4), placeholder);
        let n = db.append_parquet("orders", &batch, None).unwrap();
        assert_eq!(n, 2);
        assert_eq!(
            commit_lsns(&db, (3, 4)),
            vec![placeholder, placeholder],
            "a non-spill file keeps its verbatim per-row commit_lsn"
        );
    }
}
