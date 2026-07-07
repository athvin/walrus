//! One `.duckdb` file's read-write connection (loader §8.1) — holding DuckDB's single-writer file lock,
//! the **second** fence (the lease is the first). Opening read-write takes an exclusive OS lock on the
//! file: if a still-live owner held it we'd fail opaquely here, which is why the ordered bootstrap
//! proves the lease is reclaimable *before* calling [`TableDb::open`].

use crate::error::LoaderError;
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
    /// (composite PK for at-least-once dedup). Initial shape only — DDL reconcile is PR 3.8/3.9.
    pub fn ensure_tables(&self, rel: &PgRelation) -> Result<(), LoaderError> {
        let cols: Vec<String> = rel
            .columns
            .iter()
            .map(|c| format!("\"{}\" {}", c.name, duck_type(c.type_oid)))
            .collect();
        let keys: Vec<String> = rel
            .columns
            .iter()
            .filter(|c| c.is_key)
            .map(|c| format!("\"{}\"", c.name))
            .collect();

        // The mirror: current row per key.
        let mut mirror = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" ({}",
            rel.name,
            cols.join(", ")
        );
        if !keys.is_empty() {
            mirror.push_str(&format!(", PRIMARY KEY ({})", keys.join(", ")));
        }
        mirror.push(')');

        // The CDC log: every change verbatim, with the intact `walrus_pg_sink_meta` JSON plus four
        // columns PROMOTED out of it (op / commit_lsn / lsn / sink_processed_at) as sortable 16-hex /
        // RFC-3339 text. **Composite PK = source key + sink_processed_at + lsn** — the load-bearing
        // idempotency fence (a ms-resolution `sink_processed_at` collision is broken by the always-
        // distinct `lsn`): `ON CONFLICT DO NOTHING` makes a crash-window replay a no-op.
        let mut raw_pk = keys.clone();
        raw_pk.push("\"_walrus_sink_processed_at\"".into());
        raw_pk.push("\"_walrus_lsn\"".into());
        let raw = format!(
            "CREATE TABLE IF NOT EXISTS \"{}_raw\" ({}, \"walrus_pg_sink_meta\" VARCHAR, \
             \"_walrus_op\" VARCHAR, \"_walrus_commit_lsn\" VARCHAR, \"_walrus_lsn\" VARCHAR, \
             \"_walrus_sink_processed_at\" VARCHAR, PRIMARY KEY ({}))",
            rel.name,
            cols.join(", "),
            raw_pk.join(", ")
        );

        self.conn
            .execute_batch(&format!("{mirror}; {raw};"))
            .map_err(|e| LoaderError::Duck(format!("ensure tables for {}: {e}", rel.name)))?;
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
    pub fn append_parquet(&self, table: &str, s3_uri: &str) -> Result<u64, LoaderError> {
        let uri = s3_uri.replace('\'', "''");
        // `SELECT *` yields the source columns + `walrus_pg_sink_meta` (its trailing Parquet column), in
        // the same order as `<table>_raw`'s leading columns; the four promoted extracts follow.
        let sql = format!(
            "INSERT INTO \"{table}_raw\" \
             SELECT *, \
                 json_extract_string(walrus_pg_sink_meta, '$.op'), \
                 json_extract_string(walrus_pg_sink_meta, '$.commit_lsn'), \
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

    /// The `.duckdb` connection (later PRs run the transform SQL through it).
    pub fn conn(&self) -> &duckdb::Connection {
        &self.conn
    }
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
fn duck_type(oid: u32) -> &'static str {
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
