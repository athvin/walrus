//! Source-side preflight (§1.1, architecture "Startup & bootstrap" steps 1–3, 6).
//!
//! Assert every server-side precondition before a single byte of WAL is read: the connecting role has
//! the `REPLICATION` privilege, `wal_level = logical`, server ≥ 14, slot / wal-sender headroom, the
//! publication covers `walrus.ddl_audit` + `walrus.heartbeat`, and every published **user** table has
//! a usable replica identity (a PK for `DEFAULT`). Any mismatch is **terminal** — a
//! [`PreflightError`] mapped to a distinct, greppable [`common::ExitCode`] (`CrashLoopBackOff`, not
//! a silent slow failure).
//!
//! **Connection note:** `tokio-postgres` 0.7 has no API to open a `replication=database` connection
//! (and its config parser rejects the param), so the preflight runs its catalog checks over an
//! ordinary connection and asserts the `REPLICATION` privilege from `pg_roles` — a *more* reliable
//! capability check than "a superuser connect happened to succeed". The streaming replication
//! connection itself is established by [`crate::replication`]. Catalog reads use the **simple query protocol**
//! (`simple_query`); read the version from the integer `server_version_num`, never the text
//! `version()`.

use crate::config::SinkConfig;
use common::ReplicaIdentity;
use common::sql::{SqlIdent, SqlStrExt};
use std::collections::HashSet;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// A published table, `schema.table`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
}

/// What the server reported for the two headline settings.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    /// `server_version_num`, e.g. `140009`. Compared numerically, never against the version string.
    pub version_num: i32,
    /// `wal_level`, which must be `logical` for logical replication to be possible at all.
    pub wal_level: String,
}

/// Strict rejects a keyless table; lenient quarantines and continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkMode {
    /// A keyless published table fails preflight, so the sink refuses to start.
    Strict,
    /// A keyless table is quarantined and the sink starts without it, so one bad table does not
    /// block every other one.
    Lenient,
}

/// Outcome of the per-table PK preflight.
#[derive(Debug, Default, Clone)]
pub struct PkReport {
    /// Tables with a usable replica-identity key — the set that will be replicated.
    pub ok: Vec<TableId>,
    /// Keyless tables skipped under [`PkMode::Lenient`]. Always empty under
    /// [`PkMode::Strict`], which errors instead of reporting.
    pub quarantined: Vec<TableId>,
}

/// A terminal source-preflight mismatch. `main` maps it (via [`common::Error`]) to a distinct exit
/// code.
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PreflightError {
    /// `wal_level` is not `logical`, so no logical slot can stream. Changing it needs a restart of
    /// the source, which is why this is terminal rather than retried.
    #[error("wal_level is {found}, need 'logical'")]
    WalLevel { found: String },
    /// The source predates PG14 and cannot speak pgoutput protocol v2, which walrus requires for
    /// streamed transactions.
    #[error("server_version_num {found} < 140000 (proto v2 needs PG14+)")]
    ServerTooOld { found: i32 },
    /// The source is already at its `max_replication_slots` or `max_wal_senders` limit, so creating
    /// walrus's slot would fail later, in a worse place.
    #[error("no headroom: {kind} {used}/{max}")]
    NoHeadroom {
        kind: &'static str,
        used: i32,
        max: i32,
    },
    /// The configured publication does not exist on the source.
    #[error("publication {pub_name} does not exist")]
    PublicationMissing { pub_name: String },
    /// The publication exists but does not cover a table walrus was told to replicate. The message
    /// carries the `ALTER PUBLICATION` that fixes it, since that is the operator's next action.
    #[error(
        "publication {pub_name} missing table {schema}.{table} \
         (fix: ALTER PUBLICATION {pub_name} ADD TABLE {schema}.{table})"
    )]
    PublicationGap {
        pub_name: String,
        schema: String,
        table: String,
    },
    /// The publication exists, but its action flags or a per-table filter/column list make it an
    /// incomplete source of truth for full-table reconciliation.
    #[error(transparent)]
    PublicationCoverage(#[from] crate::source_catalog::PublicationCoverageIssue),
    /// A published table has no key, so its updates and deletes could not be applied downstream.
    /// Raised only under [`PkMode::Strict`]; [`PkMode::Lenient`] reports it in a [`PkReport`].
    #[error("table {schema}.{table} has no PRIMARY KEY / usable replica identity")]
    NoPrimaryKey { schema: String, table: String },
    /// The connecting role lacks `REPLICATION`, so it cannot open a replication connection.
    #[error("missing REPLICATION privilege")]
    NoReplicationPriv,
    /// The `walrus.ddl_audit` tap is absent or incomplete, so schema changes would drift silently.
    /// `detail` names which half is missing (the table or an event trigger).
    #[error("DDL capture not installed: {detail} (apply migrations/source/0002_ddl_triggers.sql)")]
    DdlCaptureMissing { detail: &'static str },
    /// The `walrus.reload_signal` table is absent, so no chunk export could ever learn its
    /// watermark. `detail` names what was missing.
    #[error(
        "reload signal table not installed: {detail} \
         (apply migrations/source/0003_reload_signal.sql)"
    )]
    ReloadSignalMissing { detail: &'static str },
    /// The append-only request/fence relation is absent or cannot identify its rows.
    #[error(
        "reload event table not installed: {detail} \
         (apply migrations/source/0004_reload_event.sql)"
    )]
    ReloadEventMissing { detail: &'static str },
    /// A catalog query failed on the wire. `source` keeps tokio-postgres's typed failure — SQLSTATE,
    /// severity, hint — reachable by [`source()`](std::error::Error::source)/`downcast_ref`, exactly
    /// as [`HeartbeatError`](crate::heartbeat::HeartbeatError) already does for the same client.
    ///
    /// `#[from]` rather than `#[source]`: every catalog read goes through one helper, so `?` there
    /// is the only conversion and there is nothing for a second one to be confused with.
    #[error("preflight query failed: {0}")]
    Query(#[from] tokio_postgres::Error),
    /// The catalog answered, but not with a value the preflight can read (no rows, a non-numeric
    /// setting). The assertion itself failed, so there is no underlying error to chain — and it is
    /// not a [`PreflightError::Query`]: the query worked.
    #[error("preflight catalog result unusable: {0}")]
    UnusableResult(String),
    /// A configured name cannot be rendered as a SQL identifier. `source` keeps *which* rule it
    /// broke, so a caller can branch on it instead of matching on the message. Also not a
    /// [`PreflightError::Query`]: the rejection happens before any statement reaches the server.
    #[error("invalid SQL identifier: {0}")]
    Ident(#[source] common::sql::IdentError),
}

impl From<PreflightError> for common::Error {
    /// The terminal class of a mismatch is data, never a guess — so this match is exhaustive (no
    /// `_` arm): a new variant must choose its class here, and its exit code in
    /// [`crate::exit::code_for`], instead of silently inheriting the generic ones.
    #[deny(clippy::wildcard_enum_match_arm)]
    fn from(e: PreflightError) -> Self {
        match &e {
            // A keyless table has its own dedicated terminal class + exit code.
            PreflightError::NoPrimaryKey { schema, table } => common::Error::KeylessTable {
                table: format!("{schema}.{table}"),
            },
            PreflightError::WalLevel { .. }
            | PreflightError::ServerTooOld { .. }
            | PreflightError::NoHeadroom { .. }
            | PreflightError::PublicationMissing { .. }
            | PreflightError::PublicationGap { .. }
            | PreflightError::PublicationCoverage(_)
            | PreflightError::NoReplicationPriv
            | PreflightError::DdlCaptureMissing { .. }
            | PreflightError::ReloadSignalMissing { .. }
            | PreflightError::ReloadEventMissing { .. }
            | PreflightError::Query(_)
            | PreflightError::UnusableResult(_)
            | PreflightError::Ident(_) => common::Error::Preflight(e.to_string()),
        }
    }
}

/// Connect to the source for the preflight catalog checks. A transport failure (server still coming
/// up) is a *transient* [`common::Error::SourceDb`]; a server-side rejection (auth/config) is a
/// *terminal* [`common::Error::Preflight`]. The `REPLICATION` privilege itself is asserted from the
/// catalog by [`SourcePreflight::assert_server_prereqs`], not inferred from the connect succeeding.
///
/// # Errors
///
/// Returns [`common::Error::SourceDb`] for a transport-level connection failure (transient), or
/// [`common::Error::Preflight`] when the server responds with a terminal authentication/configuration
/// rejection.
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
            tracing::warn!(error = %e, "source connection closed");
        }
    });
    Ok(client)
}

/// The catalog assertions the sink runs over the source connection before reading WAL.
#[derive(Debug)]
pub struct SourcePreflight<'a> {
    client: &'a Client,
    cfg: &'a SinkConfig,
}

impl<'a> SourcePreflight<'a> {
    /// Borrow a connected source client and the config whose expectations will be asserted against
    /// it. Borrows rather than owns: preflight runs once and the caller keeps using both.
    #[must_use]
    pub const fn new(client: &'a Client, cfg: &'a SinkConfig) -> Self {
        SourcePreflight { client, cfg }
    }

    /// The DDL-capture tap is installed: the `walrus.ddl_audit` table has the sink's columns
    /// and all three event triggers exist with the required guarded command tags. Missing → terminal
    /// (schema changes would silently drift).
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::DdlCaptureMissing`] when the audit shape or a trigger/tag is absent,
    /// [`PreflightError::Query`] when a catalog query fails, or [`PreflightError::UnusableResult`]
    /// when one answers with no row to read.
    pub async fn assert_ddl_capture(&self) -> Result<(), PreflightError> {
        if self
            .first_text(
                "SELECT (count(*) = 4)::text FROM information_schema.columns
                 WHERE table_schema='walrus' AND table_name='ddl_audit'
                   AND column_name IN ('c_columns', 'c_rel_oid', 'c_replica_identity', 'c_ddl_text')",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::DdlCaptureMissing {
                detail: "walrus.ddl_audit table/columns absent",
            });
        }
        for (name, event, function, tags) in [
            (
                "walrus_intercept_ddl",
                "ddl_command_end",
                "walrus.intercept_ddl()",
                "true",
            ),
            (
                "walrus_intercept_drop",
                "sql_drop",
                "walrus.intercept_ddl()",
                "true",
            ),
            (
                "walrus_guard_publication_ddl",
                "ddl_command_start",
                "walrus.guard_publication_ddl()",
                "evttags @> ARRAY['CREATE PUBLICATION', 'ALTER PUBLICATION', \
                                   'DROP PUBLICATION', 'ALTER SCHEMA']::text[]",
            ),
        ] {
            let present = self
                .first_text(&format!(
                    "SELECT EXISTS (SELECT 1 FROM pg_event_trigger
                                    WHERE evtname='{name}' AND evtevent='{event}'
                                      AND evtfoid = to_regprocedure('{function}')
                                      AND evtenabled IN ('O', 'A')
                                      AND ({tags}))::text",
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
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::NoReplicationPriv`], [`PreflightError::WalLevel`],
    /// [`PreflightError::ServerTooOld`], or [`PreflightError::NoHeadroom`] for a terminal prerequisite
    /// mismatch; catalog failures return [`PreflightError::Query`], and a setting that is missing or
    /// non-numeric returns [`PreflightError::UnusableResult`].
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

    /// The reload signal table is installed with its PK. Missing → terminal, because an
    /// absent/unpublished signal table doesn't error at reload time — the echo just silently never
    /// arrives (reload H11). Publication membership is asserted (and auto-added under
    /// `manage_publication`) by [`Self::assert_publication_covers`], which treats `reload_signal`
    /// as the third walrus-internal table; this existence check runs FIRST so a missing table gets
    /// the migration-naming error, not a failed `ALTER PUBLICATION`.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::ReloadSignalMissing`] when the table or primary key is absent,
    /// [`PreflightError::Query`] when a catalog query fails, or [`PreflightError::UnusableResult`]
    /// when one answers with no row to read.
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

    /// The append-only request/fence table exists with a primary key.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::ReloadEventMissing`] for a missing table/key and the normal query
    /// variants for catalog failures.
    pub async fn assert_reload_event(&self) -> Result<(), PreflightError> {
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'walrus' AND c.relname = 'reload_event'
                                  AND c.relkind = 'r')::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadEventMissing {
                detail: "walrus.reload_event table absent",
            });
        }
        if self
            .first_text(
                "SELECT EXISTS (SELECT 1 FROM pg_index i
                                JOIN pg_class c ON c.oid = i.indrelid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'walrus' AND c.relname = 'reload_event'
                                  AND i.indisprimary)::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadEventMissing {
                detail: "walrus.reload_event has no PRIMARY KEY",
            });
        }
        if self
            .first_text(
                "SELECT (count(*) = 10)::text
                 FROM information_schema.columns
                 WHERE table_schema = 'walrus' AND table_name = 'reload_event'
                   AND column_name IN (
                     'event_id', 'request_id', 'reload_id', 'event_kind', 'scope',
                     'source_schema', 'source_table', 'targets', 'schema_version',
                     'wal_insert_lsn'
                   )",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadEventMissing {
                detail: "walrus.reload_event is missing required request/fence columns",
            });
        }
        if self
            .first_text(
                "SELECT EXISTS (
                   SELECT 1
                   FROM pg_trigger t
                   JOIN pg_class c ON c.oid = t.tgrelid
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = 'walrus' AND c.relname = 'reload_event'
                     AND t.tgname = 'reload_event_append_only'
                     AND t.tgenabled IN ('O', 'A')
                 )::text",
            )
            .await?
            != "true"
        {
            return Err(PreflightError::ReloadEventMissing {
                detail: "walrus.reload_event append-only trigger absent or disabled",
            });
        }
        Ok(())
    }

    /// The publication emits INSERT/UPDATE/DELETE/TRUNCATE, covers the walrus-internal tables, and
    /// applies no row filters or column lists to any user target (create/extend/fix global flags
    /// when `manage_publication`, else a mismatch is terminal). `pg_publication_tables` expands
    /// `FOR ALL TABLES` and partition roots; [`crate::source_catalog`] additionally inspects the
    /// underlying membership rows so an explicit all-current-columns list is still rejected.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::PublicationMissing`] or [`PreflightError::PublicationGap`] when
    /// automatic publication management is disabled, [`PreflightError::Query`] when inspection or
    /// an authorized create/alter statement fails, [`PreflightError::UnusableResult`] when a catalog
    /// answer cannot be read, and [`PreflightError::Ident`] when a configured publication or table
    /// name is not a legal SQL identifier.
    pub async fn assert_publication_covers(&self) -> Result<(), PreflightError> {
        let pubname = &self.cfg.publication_name;
        // Parse once, up front: both statements below that name the publication as an *identifier*
        // reuse this proven value instead of re-running `SqlIdent::new` per call site (the create
        // path plus one per missing table — up to four validations of the same string).
        let pub_ident = ident(pubname)?;
        let exists = self
            .count(&format!(
                "SELECT count(*) FROM pg_publication WHERE pubname = {}",
                pubname.to_quoted_literal()
            ))
            .await?
            > 0;
        if !exists {
            if self.cfg.manage_publication {
                self.exec(&format!(
                    "CREATE PUBLICATION {pub_ident} FOR TABLE walrus.heartbeat, walrus.ddl_audit, \
                     walrus.reload_signal, walrus.reload_event \
                     WITH (publish_via_partition_root = true)"
                ))
                .await?;
            } else {
                return Err(PreflightError::PublicationMissing {
                    pub_name: pubname.clone(),
                });
            }
        }

        let mut actions = crate::source_catalog::publication_actions(self.client, pubname).await?;
        if actions.is_some_and(|actions| !actions.is_complete()) && self.cfg.manage_publication {
            self.exec(&format!(
                "ALTER PUBLICATION {pub_ident} SET \
                 (publish = 'insert, update, delete, truncate')"
            ))
            .await?;
            actions = crate::source_catalog::publication_actions(self.client, pubname).await?;
        }
        crate::source_catalog::require_publication_actions(pubname, actions)?;

        let published = self.published_tables(pubname).await?;
        for (schema, table) in [
            ("walrus", "heartbeat"),
            ("walrus", "ddl_audit"),
            ("walrus", "reload_signal"),
            ("walrus", "reload_event"),
        ] {
            let id = TableId {
                schema: schema.to_string(),
                table: table.to_string(),
            };
            if !published.contains(&id) {
                if self.cfg.manage_publication {
                    self.exec(&format!(
                        "ALTER PUBLICATION {pub_ident} ADD TABLE {}.{}",
                        ident(schema)?,
                        ident(table)?
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

        // Re-read after any authorized ADD TABLE above. Validate the exact effective targets the
        // decoder will see, not merely the four internal membership checks.
        let published = self.published_tables(pubname).await?;
        let required_internal = ["heartbeat", "ddl_audit", "reload_signal", "reload_event"];
        for id in published
            .iter()
            .filter(|id| id.schema != "walrus" || required_internal.contains(&id.table.as_str()))
        {
            let options = crate::source_catalog::publication_target_options(
                self.client,
                pubname,
                &id.schema,
                &id.table,
            )
            .await?;
            crate::source_catalog::require_full_target(pubname, &id.schema, &id.table, options)?;
        }
        Ok(())
    }

    /// Every published **user** table (schema ≠ `walrus`) has a usable replica identity: `DEFAULT`
    /// requires a PRIMARY KEY; `FULL`/`INDEX` are fine; `NOTHING` is never usable. Strict → terminal on
    /// the first offender; lenient → quarantine + alert + continue.
    ///
    /// # Errors
    ///
    /// Returns [`PreflightError::NoPrimaryKey`] for the first unusable table in strict mode, or
    /// [`PreflightError::Query`] / [`PreflightError::UnusableResult`] when publication/catalog rows
    /// cannot be read.
    pub async fn assert_tables_have_pk(&self, mode: PkMode) -> Result<PkReport, PreflightError> {
        let sql = format!(
            r#"SELECT pt.schemaname, pt.tablename, c.relreplident::text AS relreplident,
                      (EXISTS (SELECT 1 FROM pg_index i
                               WHERE i.indrelid = c.oid AND i.indisprimary))::text AS has_pk
               FROM pg_publication_tables pt
               JOIN pg_namespace n ON n.nspname = pt.schemaname
               JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
               WHERE pt.pubname = {} AND pt.schemaname <> 'walrus'"#,
            self.cfg.publication_name.to_quoted_literal()
        );
        let mut report = PkReport::default();
        for msg in self.query(&sql).await? {
            // Only `Row` carries catalog data; the command tag and row description carry none.
            let SimpleQueryMessage::Row(row) = msg else {
                continue;
            };
            let schema = row.get("schemaname").unwrap_or_default().to_string();
            let table = row.get("tablename").unwrap_or_default().to_string();
            let relreplident = row.get("relreplident").unwrap_or_default();
            // `boolean::text` renders as "true"/"false" (not "t"/"f") over the simple protocol.
            let has_pk = row.get("has_pk") == Some("true");
            // The catalog code is parsed into the shared enum rather than matched as raw text, so
            // `identity_is_usable` can be exhaustive. A code outside the catalog's four cannot
            // occur for a real `pg_class` row, and a gate that cannot classify a table's identity
            // must quarantine it rather than wave it through.
            let usable = relreplident
                .parse::<ReplicaIdentity>()
                .is_ok_and(|identity| identity_is_usable(identity, has_pk));
            let id = TableId { schema, table };
            if usable {
                report.ok.push(id);
            } else {
                match mode {
                    PkMode::Strict => {
                        return Err(PreflightError::NoPrimaryKey {
                            schema: id.schema,
                            table: id.table,
                        });
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
        Ok(report)
    }

    // ---- helpers ------------------------------------------------------------------------------

    async fn query(&self, sql: &str) -> Result<Vec<SimpleQueryMessage>, PreflightError> {
        Ok(self.client.simple_query(sql).await?)
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
        Err(PreflightError::UnusableResult(format!(
            "no rows for `{sql}`"
        )))
    }

    async fn setting(&self, name: &str) -> Result<String, PreflightError> {
        self.first_text(&format!(
            "SELECT current_setting({})",
            name.to_quoted_literal()
        ))
        .await
    }

    async fn setting_i32(&self, name: &str) -> Result<i32, PreflightError> {
        self.setting(name).await?.trim().parse().map_err(|_| {
            PreflightError::UnusableResult(format!("setting {name} is not an integer"))
        })
    }

    async fn count(&self, sql: &str) -> Result<i32, PreflightError> {
        self.first_text(sql)
            .await?
            .trim()
            .parse()
            .map_err(|_| PreflightError::UnusableResult(format!("`{sql}` did not return a count")))
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
            pubname.to_quoted_literal()
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

/// Can a published table participate in the unified resumable exporter? Every supported table
/// needs a real primary key; replica identity alone cannot provide the stable keyset cursor.
///
/// Exhaustive (no `_` arm) for the same reason the `From<PreflightError>` conversion above is:
/// which identities the sink can decode is data, never a guess, so a new [`ReplicaIdentity`]
/// variant must be classified here instead of silently inheriting "usable".
#[deny(clippy::wildcard_enum_match_arm)]
const fn identity_is_usable(identity: ReplicaIdentity, has_pk: bool) -> bool {
    match identity {
        ReplicaIdentity::Default | ReplicaIdentity::Full | ReplicaIdentity::Index => has_pk,
        ReplicaIdentity::Nothing => false,
    }
}

/// Validate a SQL identifier before its [`std::fmt::Display`] implementation quotes it.
fn ident(s: &str) -> Result<SqlIdent, PreflightError> {
    SqlIdent::new(s).map_err(PreflightError::Ident)
}

#[cfg(test)]
#[path = "preflight_test.rs"]
mod tests;
