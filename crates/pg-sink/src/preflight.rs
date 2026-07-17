//! Source-side preflight (§1.1, architecture "Startup & bootstrap" steps 1–3, 6).
//!
//! Assert every server-side precondition before a single byte of WAL is read: the connecting role has
//! the `REPLICATION` privilege, `wal_level = logical`, server ≥ 14, slot / wal-sender headroom, the
//! publication covers `walrus.ddl_audit` + `walrus.heartbeat`, and every published **user** table has
//! a usable replica identity (a PK for `DEFAULT`). Any mismatch is **terminal** — a `PreflightError`
//! mapped to a distinct, greppable `ExitCode` (`CrashLoopBackOff`, not a silent slow failure).
//!
//! **Connection note:** `tokio-postgres` 0.7 has no API to open a `replication=database` connection
//! (and its config parser rejects the param), so the preflight runs its catalog checks over an
//! ordinary connection and asserts the `REPLICATION` privilege from `pg_roles` — a *more* reliable
//! capability check than "a superuser connect happened to succeed". The streaming replication
//! connection itself is established in PR 2.20. Catalog reads use the **simple query protocol**
//! (`simple_query`); read the version from the integer `server_version_num`, never the text
//! `version()`.

use crate::config::SinkConfig;
use std::collections::HashSet;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// A published table, `schema.table`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId {
    pub schema: String,
    pub table: String,
}

/// What the server reported for the two headline settings.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub version_num: i32,
    pub wal_level: String,
}

/// Strict rejects a keyless table; lenient quarantines and continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkMode {
    Strict,
    Lenient,
}

/// Outcome of the per-table PK preflight.
#[derive(Debug, Default, Clone)]
pub struct PkReport {
    pub ok: Vec<TableId>,
    pub quarantined: Vec<TableId>,
}

/// A terminal source-preflight mismatch. `main` maps it (via `common::Error`) to a distinct exit code.
#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    #[error("wal_level is {found}, need 'logical'")]
    WalLevel { found: String },
    #[error("server_version_num {found} < 140000 (proto v2 needs PG14+)")]
    ServerTooOld { found: i32 },
    #[error("no headroom: {kind} {used}/{max}")]
    NoHeadroom {
        kind: &'static str,
        used: i32,
        max: i32,
    },
    #[error("publication {pub_name} does not exist")]
    PublicationMissing { pub_name: String },
    #[error(
        "publication {pub_name} missing table {schema}.{table} \
         (fix: ALTER PUBLICATION {pub_name} ADD TABLE {schema}.{table})"
    )]
    PublicationGap {
        pub_name: String,
        schema: String,
        table: String,
    },
    #[error("table {schema}.{table} has no PRIMARY KEY / usable replica identity")]
    NoPrimaryKey { schema: String, table: String },
    #[error("missing REPLICATION privilege")]
    NoReplicationPriv,
    #[error("DDL capture not installed: {detail} (apply migrations/source/0002_ddl_triggers.sql)")]
    DdlCaptureMissing { detail: &'static str },
    #[error(
        "reload signal table not installed: {detail} \
         (apply migrations/source/0003_reload_signal.sql)"
    )]
    ReloadSignalMissing { detail: &'static str },
    #[error("preflight query failed: {0}")]
    Query(String),
}

impl From<PreflightError> for common::Error {
    fn from(e: PreflightError) -> Self {
        match e {
            // A keyless table has its own dedicated terminal class + exit code.
            PreflightError::NoPrimaryKey { schema, table } => common::Error::KeylessTable {
                table: format!("{schema}.{table}"),
            },
            other => common::Error::Preflight(other.to_string()),
        }
    }
}

/// Connect to the source for the preflight catalog checks. A transport failure (server still coming
/// up) is a *transient* [`common::Error::SourceDb`]; a server-side rejection (auth/config) is a
/// *terminal* [`common::Error::Preflight`]. The `REPLICATION` privilege itself is asserted from the
/// catalog by [`SourcePreflight::assert_server_prereqs`], not inferred from the connect succeeding.
pub async fn connect_source(url: &str) -> Result<Client, common::Error> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(|e| {
        if e.as_db_error().is_some() {
            // The server answered and refused (auth / bad config) — retrying won't help.
            common::Error::Preflight(format!("source connection rejected: {e}"))
        } else {
            // Transport-level (refused / timeout / DNS) — the server may still be coming up.
            common::Error::SourceDb(e.to_string())
        }
    })?;
    // Drive the connection in the background; it lives as long as `client` is held.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!("source connection closed: {e}");
        }
    });
    Ok(client)
}

/// The catalog assertions the sink runs over the source connection before reading WAL.
pub struct SourcePreflight<'a> {
    client: &'a Client,
    cfg: &'a SinkConfig,
}

impl<'a> SourcePreflight<'a> {
    pub fn new(client: &'a Client, cfg: &'a SinkConfig) -> Self {
        SourcePreflight { client, cfg }
    }

    /// The DDL-capture tap is installed (PR 2.33): the `walrus.ddl_audit` table has the sink's columns
    /// and **both** event triggers exist. Missing → terminal (schema changes would silently drift).
    pub async fn assert_ddl_capture(&self) -> Result<(), PreflightError> {
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns
                                WHERE table_schema='walrus' AND table_name='ddl_audit'
                                  AND column_name='c_columns')::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::DdlCaptureMissing {
                detail: "walrus.ddl_audit table/columns absent",
            });
        }
        for (name, event) in [
            ("walrus_intercept_ddl", "ddl_command_end"),
            ("walrus_intercept_drop", "sql_drop"),
        ] {
            let present = self
                .first_text(&format!(
                    "SELECT EXISTS (SELECT 1 FROM pg_event_trigger
                                    WHERE evtname='{name}' AND evtevent='{event}')::text",
                ))
                .await?;
            if present != "true" {
                return Err(PreflightError::DdlCaptureMissing {
                    detail: "event trigger missing",
                });
            }
        }
        Ok(())
    }

    /// The role has `REPLICATION`, `wal_level = logical`, `server_version_num ≥ 140000`, and free
    /// slot / wal-sender headroom.
    pub async fn assert_server_prereqs(&self) -> Result<ServerInfo, PreflightError> {
        // The role must be able to start a WAL sender (rolreplication, or a superuser).
        let can_replicate = self
            .first_text(
                "SELECT (rolreplication OR rolsuper)::text FROM pg_roles WHERE rolname = current_user",
            )
            .await?;
        if can_replicate != "true" {
            return Err(PreflightError::NoReplicationPriv);
        }
        let wal_level = self.setting("wal_level").await?;
        if wal_level != "logical" {
            return Err(PreflightError::WalLevel { found: wal_level });
        }
        let version_num = self.setting_i32("server_version_num").await?;
        if version_num < 140_000 {
            return Err(PreflightError::ServerTooOld { found: version_num });
        }
        // Headroom = *free* capacity over current usage (an existing slot still counts).
        self.assert_headroom(
            "replication_slots",
            "max_replication_slots",
            "SELECT count(*) FROM pg_replication_slots",
        )
        .await?;
        self.assert_headroom(
            "wal_senders",
            "max_wal_senders",
            "SELECT count(*) FROM pg_stat_replication",
        )
        .await?;
        Ok(ServerInfo {
            version_num,
            wal_level,
        })
    }

    /// The reload signal table (PR 6.2) is installed with its PK. Missing → terminal, because an
    /// absent/unpublished signal table doesn't error at reload time — the echo just silently never
    /// arrives (reload H11). Publication membership is asserted (and auto-added under
    /// `manage_publication`) by [`Self::assert_publication_covers`], which treats `reload_signal`
    /// as the third walrus-internal table; this existence check runs FIRST so a missing table gets
    /// the migration-naming error, not a failed `ALTER PUBLICATION`.
    pub async fn assert_reload_signal(&self) -> Result<(), PreflightError> {
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'walrus' AND c.relname = 'reload_signal'
                                  AND c.relkind = 'r')::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadSignalMissing {
                detail: "walrus.reload_signal table absent",
            });
        }
        // The PK doubles as REPLICA IDENTITY DEFAULT — all an insert-only table needs.
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM pg_index i
                                JOIN pg_class c ON c.oid = i.indrelid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'walrus' AND c.relname = 'reload_signal'
                                  AND i.indisprimary)::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadSignalMissing {
                detail: "walrus.reload_signal has no PRIMARY KEY",
            });
        }
        Ok(())
    }

    /// The publication exists and covers the walrus-internal tables — `heartbeat`, `ddl_audit`,
    /// and `reload_signal` (create/extend when `manage_publication`, else a gap is terminal, with
    /// the exact `ALTER PUBLICATION` fix in the error). `pg_publication_tables` already expands
    /// `FOR ALL TABLES` and partition roots, so we read it directly.
    pub async fn assert_publication_covers(&self) -> Result<(), PreflightError> {
        let pubname = &self.cfg.publication_name;
        let exists = self
            .count(&format!(
                "SELECT count(*) FROM pg_publication WHERE pubname = {}",
                lit(pubname)
            ))
            .await?
            > 0;
        if !exists {
            if self.cfg.manage_publication {
                self.exec(&format!(
                    "CREATE PUBLICATION {} FOR TABLE walrus.heartbeat, walrus.ddl_audit, \
                     walrus.reload_signal WITH (publish_via_partition_root = true)",
                    ident(pubname)
                ))
                .await?;
            } else {
                return Err(PreflightError::PublicationMissing {
                    pub_name: pubname.clone(),
                });
            }
        }

        let published = self.published_tables(pubname).await?;
        for (schema, table) in [
            ("walrus", "heartbeat"),
            ("walrus", "ddl_audit"),
            ("walrus", "reload_signal"),
        ] {
            let id = TableId {
                schema: schema.to_string(),
                table: table.to_string(),
            };
            if !published.contains(&id) {
                if self.cfg.manage_publication {
                    self.exec(&format!(
                        "ALTER PUBLICATION {} ADD TABLE {}.{}",
                        ident(pubname),
                        ident(schema),
                        ident(table)
                    ))
                    .await?;
                } else {
                    return Err(PreflightError::PublicationGap {
                        pub_name: pubname.clone(),
                        schema: schema.to_string(),
                        table: table.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Every published **user** table (schema ≠ `walrus`) has a usable replica identity: `DEFAULT`
    /// requires a PRIMARY KEY; `FULL`/`INDEX` are fine; `NOTHING` is never usable. Strict → terminal on
    /// the first offender; lenient → quarantine + alert + continue.
    pub async fn assert_tables_have_pk(&self, mode: PkMode) -> Result<PkReport, PreflightError> {
        let sql = format!(
            r#"SELECT pt.schemaname, pt.tablename, c.relreplident::text AS relreplident,
                      (EXISTS (SELECT 1 FROM pg_index i
                               WHERE i.indrelid = c.oid AND i.indisprimary))::text AS has_pk
               FROM pg_publication_tables pt
               JOIN pg_namespace n ON n.nspname = pt.schemaname
               JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
               WHERE pt.pubname = {} AND pt.schemaname <> 'walrus'"#,
            lit(&self.cfg.publication_name)
        );
        let mut report = PkReport::default();
        for msg in self.query(&sql).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let schema = row.get("schemaname").unwrap_or_default().to_string();
                let table = row.get("tablename").unwrap_or_default().to_string();
                let relreplident = row.get("relreplident").unwrap_or_default();
                // `boolean::text` renders as "true"/"false" (not "t"/"f") over the simple protocol.
                let has_pk = row.get("has_pk") == Some("true");
                // 'd' (default) needs a PK; 'f'/'i' carry a full/index identity; 'n' (nothing) never.
                let usable = match relreplident {
                    "n" => false,
                    "d" => has_pk,
                    _ => true,
                };
                let id = TableId { schema, table };
                if usable {
                    report.ok.push(id);
                } else {
                    match mode {
                        PkMode::Strict => {
                            return Err(PreflightError::NoPrimaryKey {
                                schema: id.schema,
                                table: id.table,
                            })
                        }
                        PkMode::Lenient => {
                            tracing::warn!(
                                schema = %id.schema, table = %id.table,
                                "ALERT: published table has no usable replica identity — quarantined (lenient)"
                            );
                            report.quarantined.push(id);
                        }
                    }
                }
            }
        }
        Ok(report)
    }

    // ---- helpers ------------------------------------------------------------------------------

    async fn query(&self, sql: &str) -> Result<Vec<SimpleQueryMessage>, PreflightError> {
        self.client
            .simple_query(sql)
            .await
            .map_err(|e| PreflightError::Query(e.to_string()))
    }

    async fn exec(&self, sql: &str) -> Result<(), PreflightError> {
        self.query(sql).await.map(|_| ())
    }

    /// First column of the first row, as text.
    async fn first_text(&self, sql: &str) -> Result<String, PreflightError> {
        for msg in self.query(sql).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                return Ok(row.get(0).unwrap_or_default().to_string());
            }
        }
        Err(PreflightError::Query(format!("no rows for `{sql}`")))
    }

    async fn setting(&self, name: &str) -> Result<String, PreflightError> {
        self.first_text(&format!("SELECT current_setting({})", lit(name)))
            .await
    }

    async fn setting_i32(&self, name: &str) -> Result<i32, PreflightError> {
        self.setting(name)
            .await?
            .trim()
            .parse()
            .map_err(|_| PreflightError::Query(format!("setting {name} is not an integer")))
    }

    async fn count(&self, sql: &str) -> Result<i32, PreflightError> {
        self.first_text(sql)
            .await?
            .trim()
            .parse()
            .map_err(|_| PreflightError::Query(format!("`{sql}` did not return a count")))
    }

    async fn assert_headroom(
        &self,
        kind: &'static str,
        max_setting: &str,
        used_sql: &str,
    ) -> Result<(), PreflightError> {
        let max = self.setting_i32(max_setting).await?;
        let used = self.count(used_sql).await?;
        if used >= max {
            return Err(PreflightError::NoHeadroom { kind, used, max });
        }
        Ok(())
    }

    async fn published_tables(&self, pubname: &str) -> Result<HashSet<TableId>, PreflightError> {
        let sql = format!(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = {}",
            lit(pubname)
        );
        let mut set = HashSet::new();
        for msg in self.query(&sql).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                set.insert(TableId {
                    schema: row.get("schemaname").unwrap_or_default().to_string(),
                    table: row.get("tablename").unwrap_or_default().to_string(),
                });
            }
        }
        Ok(set)
    }
}

/// A SQL string literal (single-quoted, quotes doubled).
fn lit(s: &str) -> String {
    format!("'{}'", common::sql::sql_literal(s))
}

/// A SQL identifier (double-quoted, quotes doubled).
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[cfg(test)]
#[path = "preflight_test.rs"]
mod tests;
