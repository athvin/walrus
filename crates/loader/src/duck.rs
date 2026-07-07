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

        // The CDC log: every change, keyed by (source key…, commit LSN, row LSN) so a replayed batch is
        // idempotent. `_walrus_op` records I/U/D.
        let mut raw_pk = keys.clone();
        raw_pk.push("\"_walrus_commit_lsn\"".into());
        raw_pk.push("\"_walrus_lsn\"".into());
        let raw = format!(
            "CREATE TABLE IF NOT EXISTS \"{}_raw\" ({}, \"_walrus_op\" VARCHAR, \
             \"_walrus_commit_lsn\" BIGINT, \"_walrus_lsn\" BIGINT, PRIMARY KEY ({}))",
            rel.name,
            cols.join(", "),
            raw_pk.join(", ")
        );

        self.conn
            .execute_batch(&format!("{mirror}; {raw};"))
            .map_err(|e| LoaderError::Duck(format!("ensure tables for {}: {e}", rel.name)))?;
        Ok(())
    }

    /// The `.duckdb` connection (later PRs run the append/transform SQL through it).
    pub fn conn(&self) -> &duckdb::Connection {
        &self.conn
    }
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
