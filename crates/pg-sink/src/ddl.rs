//! Transactional DDL capture — the sink side of the source event-trigger tap.
//!
//! The source trigger writes a structured row to the published `walrus.ddl_audit` table in the SAME
//! transaction as the DDL. Ordinary pgoutput transactions are known to have committed before they are
//! emitted, but streamed transactions are visible while still open and can later abort. Consequently a
//! decoded audit row is only a **provisional** schema boundary: the sink may use its post-change shape to
//! decode later rows in that transaction, but it does not publish the DDL manifest/registry or make the
//! version globally committed until `Commit`/`StreamCommit`. `StreamAbort` drops the complete provisional
//! state, including a rolled-back savepoint's DDL.
//!
//! `c_columns` is the correctness input. `c_ddl_text` is retained as best-effort audit context only and is
//! never replayed or parsed to determine the change.

use crate::relcache::RelationCache;
use common::{
    DdlId, EpochNo, Lsn, PgColumn, PgRelation, ReplicaIdentity, SchemaVersionNo, TupleValue,
};
use std::collections::HashMap;

/// The source transaction that owns a decoded DDL audit row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionScope {
    /// A normal `Begin … Commit` transaction. pgoutput carries no xid prefix on its changes.
    Ordinary,
    /// One streamed top-level transaction and the subtransaction that emitted this particular row.
    Streamed {
        /// Top-level xid named by `StreamStart`/`StreamCommit`.
        top_xid: u32,
        /// Per-message xid, used to discard a rolled-back savepoint precisely.
        sub_xid: u32,
    },
}

/// Result of observing one audit event before its transaction commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DdlObservation {
    /// New provisional version for a structural DDL; `None` for metadata-only events.
    pub structural_version: Option<SchemaVersionNo>,
    /// The source audit identity was already committed in control Postgres (WAL replay).
    pub replay: bool,
}

/// A decoded `walrus.ddl_audit` insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlEvent {
    /// Stable source identity (`walrus.ddl_audit.id`), used to make WAL replay idempotent.
    pub source_audit_id: i64,
    /// `pg_current_wal_lsn()` captured by the source trigger. Audit context, not the commit LSN.
    pub capture_lsn: Lsn,
    /// `ddl_command_end` or `sql_drop`.
    pub c_event: String,
    /// `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, `COMMENT`, and so on.
    pub c_tag: String,
    /// Schema of the affected table.
    pub source_schema: String,
    /// Name of the affected table.
    pub source_table: String,
    /// Source relation OID, including the last OID of a dropped table.
    pub c_rel_oid: Option<u32>,
    /// Post-change replica identity for a surviving table.
    pub c_replica_identity: Option<ReplicaIdentity>,
    /// Structured post-change column set; an empty array is the dropped-table sentinel.
    pub c_columns: Option<serde_json::Value>,
    /// Structured dropped-object identity, when supplied by `sql_drop`.
    pub c_dropped: Option<serde_json::Value>,
    /// Best-effort SQL text from `current_query()`, retained for audit/debugging only.
    pub c_ddl_text: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct AuditColumn {
    name: String,
    type_oid: u32,
    type_modifier: i32,
    #[serde(default)]
    is_key: Option<bool>,
}

impl DdlEvent {
    /// Extract an event from a decoded `ddl_audit` tuple by column name.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::MissingColumn`] for a missing/invalid required scalar,
    /// [`DdlError::ReplicaIdentity`] for an invalid catalog identity code, or [`DdlError::Json`] for
    /// malformed structured payloads.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub fn from_tuple(rel: &PgRelation, values: &[TupleValue]) -> Result<Self, DdlError> {
        let text = |name: &str| -> Option<String> {
            let idx = rel.columns.iter().position(|c| c.name == name)?;
            match values.get(idx)? {
                TupleValue::Text(s) => Some(s.clone()),
                TupleValue::Null | TupleValue::UnchangedToast | TupleValue::Binary(_) => None,
            }
        };
        let required = |name: &'static str| -> Result<String, DdlError> {
            text(name).ok_or(DdlError::MissingColumn(name))
        };
        let source_audit_id = required("id")?
            .parse()
            .map_err(|_| DdlError::MissingColumn("id"))?;
        let capture_lsn = required("c_lsn")?
            .parse()
            .map_err(|_| DdlError::MissingColumn("c_lsn"))?;
        let json = |name: &str| -> Result<Option<serde_json::Value>, DdlError> {
            text(name)
                .filter(|s| !s.is_empty())
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(Into::into)
        };
        let c_rel_oid = text("c_rel_oid")
            .map(|raw| raw.parse())
            .transpose()
            .map_err(|_| DdlError::MissingColumn("c_rel_oid"))?;
        let c_replica_identity = text("c_replica_identity")
            .map(|raw| {
                raw.parse()
                    .map_err(|_| DdlError::ReplicaIdentity(raw.clone()))
            })
            .transpose()?;
        Ok(DdlEvent {
            source_audit_id,
            capture_lsn,
            c_event: text("c_event").unwrap_or_default(),
            c_tag: text("c_tag").unwrap_or_default(),
            source_schema: text("c_schema").unwrap_or_default(),
            source_table: text("c_table").unwrap_or_default(),
            c_rel_oid,
            c_replica_identity,
            c_columns: json("c_columns")?,
            c_dropped: json("c_dropped")?,
            c_ddl_text: text("c_ddl_text"),
        })
    }

    /// Structural (schema version + file boundary) versus metadata-only.
    #[must_use]
    pub fn is_structural(&self) -> bool {
        !self.c_tag.eq_ignore_ascii_case("COMMENT")
    }

    /// Whether this is the `sql_drop` sentinel for a table that no longer has a catalog shape.
    #[must_use]
    pub fn is_table_drop(&self) -> bool {
        self.c_event.eq_ignore_ascii_case("sql_drop")
    }

    /// Build the authoritative post-change relation described by this event.
    ///
    /// The source snapshot deliberately contains a little more than [`PgColumn`] (`attnum`, nullability);
    /// serde ignores those fields here. On an upgraded source whose older trigger omitted `is_key`, the
    /// previous relation supplies it by column name. A dropped-table sentinel has no post-change
    /// relation and returns `None`; its unsupported tracked-identity change is commit-gated instead.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::Json`] when `c_columns` cannot decode to the expected column snapshot.
    pub fn relation_after(
        &self,
        previous: Option<&PgRelation>,
    ) -> Result<Option<PgRelation>, DdlError> {
        if !self.is_structural() || self.is_table_drop() {
            return Ok(None);
        }
        let Some(columns) = &self.c_columns else {
            return Ok(None);
        };
        let audit: Vec<AuditColumn> = serde_json::from_value(columns.clone())?;
        let prior_key = |name: &str| {
            previous
                .and_then(|rel| rel.columns.iter().find(|col| col.name == name))
                .is_some_and(|col| col.is_key)
        };
        let columns = audit
            .into_iter()
            .map(|col| PgColumn {
                is_key: col.is_key.unwrap_or_else(|| prior_key(&col.name)),
                name: col.name,
                type_oid: col.type_oid,
                type_modifier: col.type_modifier,
            })
            .collect();
        let Some(oid) = self.c_rel_oid.or_else(|| previous.map(|rel| rel.oid)) else {
            return Ok(None);
        };
        Ok(Some(PgRelation {
            oid,
            schema: self.source_schema.clone(),
            name: self.source_table.clone(),
            replica_identity: self
                .c_replica_identity
                .or_else(|| previous.map(|rel| rel.replica_identity))
                .unwrap_or(ReplicaIdentity::Default),
            columns,
        }))
    }
}

#[derive(Debug, Clone)]
struct PendingDdl {
    scope: TransactionScope,
    event: DdlEvent,
    version: SchemaVersionNo,
    identity_change: Option<TrackedTableIdentityChange>,
}

#[derive(Debug, Clone)]
struct PendingRegistry {
    scope: TransactionScope,
    row: control::RegistryRow,
}

#[derive(Debug, Clone, Copy)]
enum CommitSelector {
    Ordinary,
    Streamed(u32),
}

impl CommitSelector {
    const fn matches(self, scope: TransactionScope) -> bool {
        match (self, scope) {
            (Self::Ordinary, TransactionScope::Ordinary) => true,
            (Self::Streamed(expected), TransactionScope::Streamed { top_xid, .. }) => {
                expected == top_xid
            }
            (Self::Ordinary, TransactionScope::Streamed { .. })
            | (Self::Streamed(_), TransactionScope::Ordinary) => false,
        }
    }
}

/// Tracks committed and transaction-local schema versions and atomically publishes DDL history plus
/// provisional registry rows at the matching source commit.
#[derive(Debug)]
pub struct DdlConsumer {
    epoch: EpochNo,
    committed_versions: HashMap<(String, String), SchemaVersionNo>,
    pending: Vec<PendingDdl>,
    pending_registry: Vec<PendingRegistry>,
    processed: HashMap<i64, control::DdlRow>,
}

impl DdlConsumer {
    /// A consumer for one epoch. Tables default to schema version 1 until hydrated or changed.
    #[must_use]
    pub fn new(epoch: EpochNo) -> Self {
        Self {
            epoch,
            committed_versions: HashMap::new(),
            pending: Vec::new(),
            pending_registry: Vec::new(),
            processed: HashMap::new(),
        }
    }

    /// Hydrate committed versions from the relation cache restored at sink startup.
    pub fn hydrate_versions(&mut self, cache: &RelationCache) {
        for cached in cache {
            self.set_committed(
                &cached.relation.schema,
                &cached.relation.name,
                cached.schema_version,
            );
        }
    }

    /// Hydrate processed source audit identities and their committed versions from DDL history.
    pub fn hydrate_history(&mut self, history: Vec<control::DdlRow>) {
        for row in history {
            self.set_committed(&row.source_schema, &row.source_table, row.schema_version);
            self.processed.insert(row.source_audit_id, row);
        }
    }

    /// Highest globally committed version for a table, defaulting to 1.
    #[must_use]
    pub fn committed_version_of(&self, schema: &str, table: &str) -> SchemaVersionNo {
        find_version(&self.committed_versions, schema, table).unwrap_or(SchemaVersionNo(1))
    }

    /// Highest projected version across all currently visible pending transactions.
    ///
    /// Primarily a diagnostics/test accessor. Decode routing should use [`Self::version_for`] so one
    /// open streamed transaction never leaks its provisional version into another.
    #[must_use]
    pub fn version_of(&self, schema: &str, table: &str) -> SchemaVersionNo {
        self.pending
            .iter()
            .filter(|pending| table_matches(&pending.event, schema, table))
            .map(|pending| pending.version)
            .max()
            .unwrap_or_else(|| self.committed_version_of(schema, table))
    }

    /// Version visible inside one source transaction: committed state plus that transaction's own DDL.
    #[must_use]
    pub fn version_for(
        &self,
        scope: TransactionScope,
        schema: &str,
        table: &str,
    ) -> SchemaVersionNo {
        self.pending
            .iter()
            .filter(|pending| {
                same_transaction(pending.scope, scope)
                    && table_matches(&pending.event, schema, table)
            })
            .map(|pending| pending.version)
            .max()
            .unwrap_or_else(|| self.committed_version_of(schema, table))
    }

    /// Stage one decoded DDL event. No control-DB side effect occurs before commit.
    ///
    /// `previous_for_oid` must be the relation already tracked for `event.c_rel_oid`, if any. A
    /// rename, schema move, or drop is recorded as provisional transaction state here, but is not
    /// rejected yet: a streamed transaction can still abort. The matching [`Self::on_commit`] or
    /// [`Self::on_stream_commit`] returns the typed error before opening a control transaction.
    pub fn observe(
        &mut self,
        scope: TransactionScope,
        event: DdlEvent,
        previous_for_oid: Option<&PgRelation>,
    ) -> DdlObservation {
        if let Some(existing) = self.processed.get(&event.source_audit_id).cloned() {
            self.set_committed(
                &existing.source_schema,
                &existing.source_table,
                existing.schema_version,
            );
            return DdlObservation {
                structural_version: event.is_structural().then_some(existing.schema_version),
                replay: true,
            };
        }
        let current = self.version_for(scope, &event.source_schema, &event.source_table);
        let version = if event.is_structural() {
            SchemaVersionNo(current.0 + 1)
        } else {
            current
        };
        let structural_version = event.is_structural().then_some(version);
        let identity_change = previous_for_oid
            .and_then(|previous| TrackedTableIdentityChange::from_event(previous, &event));
        self.pending.push(PendingDdl {
            scope,
            event,
            version,
            identity_change,
        });
        DdlObservation {
            structural_version,
            replay: false,
        }
    }

    /// Whether `version` is provisional rather than globally committed.
    #[must_use]
    pub fn is_provisional(&self, schema: &str, table: &str, version: SchemaVersionNo) -> bool {
        self.pending.iter().any(|pending| {
            pending.version == version && table_matches(&pending.event, schema, table)
        })
    }

    /// Stage a registry row in the source transaction that owns it. A later Relation message for the
    /// same version and transaction replaces the trigger snapshot idempotently before commit.
    pub fn stage_registry(&mut self, scope: TransactionScope, row: control::RegistryRow) {
        if let Some(existing) = self.pending_registry.iter_mut().find(|pending| {
            same_transaction(pending.scope, scope)
                && pending.row.epoch == row.epoch
                && pending.row.source_schema == row.source_schema
                && pending.row.source_table == row.source_table
                && pending.row.schema_version == row.schema_version
        }) {
            existing.row = row;
            return;
        }
        self.pending_registry.push(PendingRegistry { scope, row });
    }

    /// Commit the ordinary transaction's DDL and registry state with its actual commit LSN.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::TrackedTableIdentityChange`] for a committed tracked-table rename,
    /// schema move, or drop, or [`DdlError::Control`] if atomic control persistence fails.
    pub async fn on_commit(
        &mut self,
        pool: &sqlx::PgPool,
        commit_lsn: Lsn,
    ) -> Result<(), DdlError> {
        self.commit_selected(pool, commit_lsn, CommitSelector::Ordinary)
            .await
    }

    /// Commit one streamed top-level transaction's DDL and registry state with its StreamCommit LSN.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::TrackedTableIdentityChange`] for a committed tracked-table rename,
    /// schema move, or drop, or [`DdlError::Control`] if atomic control persistence fails.
    pub async fn on_stream_commit(
        &mut self,
        pool: &sqlx::PgPool,
        top_xid: u32,
        commit_lsn: Lsn,
    ) -> Result<(), DdlError> {
        self.commit_selected(pool, commit_lsn, CommitSelector::Streamed(top_xid))
            .await
    }

    /// Discard DDL and provisional registry rows rolled back by `StreamAbort`.
    ///
    /// Returns the provisional `(schema, table, version)` cache entries the caller must remove.
    pub fn on_stream_abort(
        &mut self,
        top_xid: u32,
        sub_xid: u32,
    ) -> Vec<(String, String, SchemaVersionNo)> {
        let whole = top_xid == sub_xid;
        let aborts = |scope: TransactionScope| match scope {
            TransactionScope::Ordinary => false,
            TransactionScope::Streamed {
                top_xid: owner,
                sub_xid: sub,
            } => owner == top_xid && (whole || sub == sub_xid),
        };
        let removed = self
            .pending
            .extract_if(.., |pending| aborts(pending.scope))
            .collect::<Vec<_>>();
        self.pending_registry
            .retain(|pending| !aborts(pending.scope));
        removed
            .into_iter()
            .filter(|pending| pending.event.is_structural())
            .map(|pending| {
                (
                    pending.event.source_schema,
                    pending.event.source_table,
                    pending.version,
                )
            })
            .collect()
    }

    async fn commit_selected(
        &mut self,
        pool: &sqlx::PgPool,
        commit_lsn: Lsn,
        selector: CommitSelector,
    ) -> Result<(), DdlError> {
        let pending = self
            .pending
            .iter()
            .filter(|row| selector.matches(row.scope))
            .cloned()
            .collect::<Vec<_>>();
        let registry = self
            .pending_registry
            .iter()
            .filter(|row| selector.matches(row.scope))
            .cloned()
            .collect::<Vec<_>>();
        if pending.is_empty() && registry.is_empty() {
            return Ok(());
        }

        // Identity is part of a loader worker's durable routing key. Fail before any control row is
        // written; the replication Commit/StreamCommit consequently remains unacknowledged and an
        // operator cannot mistake an old canonical table for the renamed/recreated relation.
        if let Some(change) = pending
            .iter()
            .find_map(|pending| pending.identity_change.clone())
        {
            return Err(DdlError::TrackedTableIdentityChange(change));
        }

        let rows = pending
            .iter()
            .map(|pending| control::DdlRow {
                id: DdlId(0),
                epoch: self.epoch,
                source_audit_id: pending.event.source_audit_id,
                source_schema: pending.event.source_schema.clone(),
                source_table: pending.event.source_table.clone(),
                c_lsn: commit_lsn,
                c_event: pending.event.c_event.clone(),
                c_tag: pending.event.c_tag.clone(),
                schema_version: pending.version,
                c_rel_oid: pending.event.c_rel_oid,
                c_columns: pending.event.c_columns.clone(),
                c_dropped: pending.event.c_dropped.clone(),
                c_ddl_text: pending.event.c_ddl_text.clone(),
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.map_err(control::ControlError::from)?;
        for row in &rows {
            control::insert_ddl(&mut *tx, row).await?;
        }
        for pending in &registry {
            control::upsert_registry(&mut *tx, &pending.row).await?;
        }
        tx.commit().await.map_err(control::ControlError::from)?;

        for row in rows {
            self.set_committed(&row.source_schema, &row.source_table, row.schema_version);
            self.processed.insert(row.source_audit_id, row);
        }
        self.pending.retain(|row| !selector.matches(row.scope));
        self.pending_registry
            .retain(|row| !selector.matches(row.scope));
        Ok(())
    }

    fn set_committed(&mut self, schema: &str, table: &str, version: SchemaVersionNo) {
        let existing = self
            .committed_versions
            .iter_mut()
            .find(|((s, t), _)| s == schema && t == table);
        if let Some((_, current)) = existing {
            *current = (*current).max(version);
        } else {
            self.committed_versions
                .insert((schema.to_string(), table.to_string()), version);
        }
    }
}

/// A source identity mutation that cannot be reconciled into a worker frozen to `schema.table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedTableIdentityChange {
    /// Stable source relation OID that connected the audit event to the tracked table.
    pub relation_oid: u32,
    /// Identity already registered in Walrus.
    pub previous_schema: String,
    /// Identity already registered in Walrus.
    pub previous_table: String,
    /// Post-DDL schema, absent when the relation was dropped.
    pub new_schema: Option<String>,
    /// Post-DDL table, absent when the relation was dropped.
    pub new_table: Option<String>,
    /// Kind of unsupported identity mutation.
    pub kind: TrackedTableIdentityChangeKind,
}

impl TrackedTableIdentityChange {
    fn from_event(previous: &PgRelation, event: &DdlEvent) -> Option<Self> {
        if event.c_rel_oid != Some(previous.oid) {
            return None;
        }

        let dropped = event.is_table_drop();
        let kind = if dropped {
            TrackedTableIdentityChangeKind::Dropped
        } else if event.c_tag.eq_ignore_ascii_case("ALTER TABLE")
            && previous.schema != event.source_schema
        {
            TrackedTableIdentityChangeKind::SchemaMoved
        } else if event.c_tag.eq_ignore_ascii_case("ALTER TABLE")
            && previous.name != event.source_table
        {
            TrackedTableIdentityChangeKind::Renamed
        } else {
            return None;
        };

        Some(Self {
            relation_oid: previous.oid,
            previous_schema: previous.schema.clone(),
            previous_table: previous.name.clone(),
            new_schema: (!dropped).then(|| event.source_schema.clone()),
            new_table: (!dropped).then(|| event.source_table.clone()),
            kind,
        })
    }
}

impl std::fmt::Display for TrackedTableIdentityChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.new_schema, &self.new_table) {
            (Some(new_schema), Some(new_table)) => write!(
                f,
                "{}.{} (OID {}) was {} to {}.{}",
                self.previous_schema,
                self.previous_table,
                self.relation_oid,
                self.kind,
                new_schema,
                new_table
            ),
            _ => write!(
                f,
                "{}.{} (OID {}) was {}",
                self.previous_schema, self.previous_table, self.relation_oid, self.kind
            ),
        }
    }
}

/// Unsupported mutation represented by [`TrackedTableIdentityChange`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackedTableIdentityChangeKind {
    /// The relation name changed while its OID remained stable.
    Renamed,
    /// The relation moved to another schema while its OID remained stable.
    SchemaMoved,
    /// The tracked relation was dropped.
    Dropped,
}

impl std::fmt::Display for TrackedTableIdentityChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Renamed => "renamed",
            Self::SchemaMoved => "moved",
            Self::Dropped => "dropped",
        })
    }
}

fn find_version(
    versions: &HashMap<(String, String), SchemaVersionNo>,
    schema: &str,
    table: &str,
) -> Option<SchemaVersionNo> {
    versions
        .iter()
        .find(|((s, t), _)| s == schema && t == table)
        .map(|(_, version)| *version)
}

fn table_matches(event: &DdlEvent, schema: &str, table: &str) -> bool {
    event.source_schema == schema && event.source_table == table
}

const fn same_transaction(left: TransactionScope, right: TransactionScope) -> bool {
    match (left, right) {
        (TransactionScope::Ordinary, TransactionScope::Ordinary) => true,
        (
            TransactionScope::Streamed { top_xid: left, .. },
            TransactionScope::Streamed { top_xid: right, .. },
        ) => left == right,
        (TransactionScope::Ordinary, TransactionScope::Streamed { .. })
        | (TransactionScope::Streamed { .. }, TransactionScope::Ordinary) => false,
    }
}

/// DDL decode, structured-snapshot, and control-persistence failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DdlError {
    /// A required audit tuple column was absent or invalid.
    #[error("ddl_audit tuple missing/invalid column: {0}")]
    MissingColumn(&'static str),
    /// The source replica-identity catalog code was invalid.
    #[error("ddl_audit tuple has invalid replica identity: {0}")]
    ReplicaIdentity(String),
    /// A structured JSON payload was malformed.
    #[error("parse ddl_audit json: {0}")]
    Json(#[from] serde_json::Error),
    /// A committed DDL transaction changed the durable identity of a tracked relation.
    #[error("tracked table identity change is unsupported: {0}")]
    TrackedTableIdentityChange(TrackedTableIdentityChange),
    /// The atomic control-Postgres commit failed.
    #[error(transparent)]
    Control(#[from] control::ControlError),
}

#[cfg(test)]
#[path = "ddl_test.rs"]
mod tests;
