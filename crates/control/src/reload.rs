//! `table_reload` models: the single-table-reload state machine (reload H4/H5/H10).
//!
//! Control-pg owns the reload's brain; every transition below is a **guarded UPDATE** —
//! `UPDATE … WHERE status = <expected>` — so a lost race or an illegal jump changes zero rows and
//! surfaces as the typed [`ControlError::ReloadTransition`], never a silent double-claim. The
//! status walk is `requested → exporting → export_complete → complete`, with `failed` terminal
//! from the two middle states. There is deliberately **no** `superseded` status: a DDL restart
//! is [`fail()`](fail) with an explanatory reason plus a fresh successor row.
//!
//! `reload_id` remains the monotonic attempt identity used by the loader. Source-WAL requests also
//! carry a UUID idempotency key: one event may fan out to several tables, while replay of the same
//! `(epoch, request UUID, table)` returns the original attempt. Direct control-plane requests keep
//! the historical one-live-request behavior but also persist a private UUID namespace for their
//! source-side fence events.

use crate::{ControlError, parse::ParseEnumError};
use common::string_enum;
use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use sqlx::{Connection, PgConnection, PgExecutor, Row};
use uuid::Uuid;

string_enum! {
    /// Persisted spelling of a full-table reconciliation request. `resync` is retained for database
    /// and API compatibility, but is a behavioral alias of `reload`: both pause claims and publish
    /// a hidden-generation full rebuild through the same state machine.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
    #[sqlx(rename_all = "lowercase")]
    pub enum ReloadFlavor {
        error = ParseEnumError;
        column = "table_reload.flavor";
        Reload => "reload",
        Resync => "resync",
    }
}

string_enum! {
    /// Whether the source event names one table or is one child of an all-published-tables fanout.
    /// Every persisted `table_reload` row still represents exactly one concrete target table.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
    #[sqlx(rename_all = "snake_case")]
    pub enum ReloadScope {
        error = ParseEnumError;
        column = "table_reload.request_scope";
        Table => "table",
        AllPublished => "all_published",
    }
}

string_enum! {
    /// Durable, data-free boundaries for a reload. `Baseline` is the safe lower fence `F`; `End`
    /// is the capture-durability barrier `H`. Their rows exist even when an export has zero data
    /// files, so lifecycle progress never depends on a Parquet row.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
    #[sqlx(rename_all = "lowercase")]
    pub enum ReloadMarkerKind {
        error = ParseEnumError;
        column = "table_reload_marker.marker_kind";
        Baseline => "baseline",
        End => "end",
    }
}

/// A source-WAL reload request after it has been expanded to one concrete table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceReloadRequest<'a> {
    /// Slot generation in which the request event was decoded.
    pub epoch: EpochNo,
    /// Stable UUID carried by the source event. All-table children may share this UUID.
    pub source_request_id: Uuid,
    /// Optional group identity used to correlate a larger orchestration request.
    pub parent_request_id: Option<Uuid>,
    /// Single-table request or child of an all-published fanout.
    pub scope: ReloadScope,
    /// Concrete child table schema.
    pub source_schema: &'a str,
    /// Concrete child table name.
    pub source_table: &'a str,
    /// Persisted request spelling; `resync` remains accepted as a rebuild alias.
    pub flavor: ReloadFlavor,
}

/// One durable reload boundary record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReloadMarkerRow {
    /// Attempt the marker belongs to.
    pub reload_id: ReloadId,
    /// Lower baseline fence or upper durability barrier.
    pub kind: ReloadMarkerKind,
    /// Commit LSN of the boundary.
    pub lsn: Lsn,
    /// Frozen schema used by the attempt at this boundary.
    pub schema_version: SchemaVersionNo,
}

/// Immutable source identity carried by a start/end fence. Source-backed attempts require the
/// request UUID to match `source_request_id` (or a successor's/direct request's
/// `parent_request_id`). `None` remains representable only for reading pre-migration rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReloadFenceIdentity<'a> {
    /// Stable source-event namespace for this fence.
    pub request_id: Option<Uuid>,
    /// Exact target schema.
    pub source_schema: &'a str,
    /// Exact target table.
    pub source_table: &'a str,
    /// Frozen structural version carried by the event.
    pub schema_version: SchemaVersionNo,
}

string_enum! {
    /// `requested → exporting → export_complete → complete`; `failed` terminal from the middle. The
    /// SQL CHECK carries the same five values — belt and braces, like `loader_checkpoint`'s CHECK.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
    #[sqlx(rename_all = "snake_case")]
    pub enum ReloadStatus {
        error = ParseEnumError;
        column = "table_reload.status";
        Requested => "requested",
        Exporting => "exporting",
        ExportComplete => "export_complete",
        Complete => "complete",
        Failed => "failed",
    }
}

/// One reload attempt. `lease_expiry`/timestamps stay out of the model — every time comparison
/// happens in SQL (`now()`), like `table_ownership`, so the Rust side never holds a clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadRow {
    /// This attempt's identity — the `bigserial` "latest wins" key described in the module docs.
    pub reload_id: ReloadId,
    /// Generation the attempt belongs to; part of the one-live-reload uniqueness key.
    pub epoch: EpochNo,
    /// Schema of the table being reloaded.
    pub source_schema: String,
    /// Table being reloaded.
    pub source_table: String,
    /// Persisted request spelling; both values use the same rebuild behavior. See [`ReloadFlavor`].
    pub flavor: ReloadFlavor,
    /// Stable source-WAL request identity; `None` for legacy direct control-plane requests.
    pub source_request_id: Option<Uuid>,
    /// Fanout/ancestor UUID, or a direct request's private durable fence namespace.
    pub parent_request_id: Option<Uuid>,
    /// Whether this row is a direct table request or an all-published child.
    pub scope: ReloadScope,
    /// Where the attempt sits in the state walk; see [`ReloadStatus`].
    pub status: ReloadStatus,
    /// Last COMPLETED chunk; 0 = none exported yet.
    pub chunk_no: i64,
    /// Last exported PK bound (a JSON array, so composite PKs need no special casing); `None` = start.
    pub cursor_pk: Option<serde_json::Value>,
    /// Authoritative safe lower fence `F`, recorded before the first export query and therefore
    /// present for empty tables too. Legacy attempts may leave it `None`.
    pub start_lsn: Option<Lsn>,
    /// Legacy L₁ — the first data chunk's echo watermark. It remains for rolling compatibility,
    /// but new reconciliation correctness uses `start_lsn` instead.
    pub first_lsn: Option<Lsn>,
    /// H — set at `export_complete`; the loader flips `complete` once `transformed_lsn >= H`.
    pub final_lsn: Option<Lsn>,
    /// The single schema version this attempt exports at; frozen with the explicit start fence for
    /// unified attempts or the first chunk for legacy attempts.
    pub schema_version: Option<SchemaVersionNo>,
    /// DDL restarts consumed so far; `reload_max_restarts` caps it.
    pub restart_count: i32,
    /// The instance currently holding the exporter lease; `None` when unclaimed. Compared only in
    /// SQL against `now()`, so this side never decides whether the lease is live.
    pub lease_holder: Option<String>,
    /// Why the attempt reached `failed` — an operator-facing reason, set only on that transition.
    pub error: Option<String>,
}

/// Build a [`ReloadRow`] from one `table_reload` record.
///
/// A macro — not a function, a generic, or an `impl From<_> for ReloadRow` — for the one narrow
/// reason that sanctions reaching for one: every `sqlx::query_file!` mints its OWN record struct
/// inside its own expansion, so readers such as [`claim_requested`], [`adopt_resumable`],
/// [`active_rebuilds`], [`ready_rebuild`], and [`get`] hand in distinct, unnameable types that
/// merely happen to
/// share a field set. Rust has no structural typing, so there is no bound to make a function
/// generic over and no type to name in a `From` impl; the only alternative is this field mapping
/// written out four times. Sibling modules ([`crate::manifest`], [`crate::ddl_manifest`])
/// build their row struct inline precisely because each has a single reader.
///
/// `expr_2021` holds the capture to the 2021 expression grammar rather than edition 2024's wider
/// `expr`, and the `let` binds the argument once so a caller's expression is never re-evaluated
/// per field. That binding is hygienic, so the `|row| typed_reload_row!(row)` closures below pass
/// their own `row` in without the macro's `row` capturing or shadowing it.
macro_rules! typed_reload_row {
    ($row:expr_2021) => {{
        let row = $row;
        // `$crate::reload::` — not a bare `ReloadRow` — so the struct resolves in its defining
        // module rather than through whatever the expansion site happens to have imported.
        $crate::reload::ReloadRow {
            reload_id: row.reload_id.into(),
            epoch: row.epoch.into(),
            source_schema: row.source_schema,
            source_table: row.source_table,
            flavor: row.flavor,
            source_request_id: row.source_request_id,
            parent_request_id: row.parent_request_id,
            scope: row.request_scope,
            status: row.status,
            chunk_no: row.chunk_no,
            cursor_pk: row.cursor_pk,
            start_lsn: row.start_lsn,
            first_lsn: row.first_lsn,
            final_lsn: row.final_lsn,
            schema_version: row.schema_version.map(Into::into),
            restart_count: row.restart_count,
            lease_holder: row.lease_holder,
            error: row.error,
        }
    }};
}

/// INSERT a reload request (`status='requested'`); returns the new `reload_id`.
///
/// A second request while the table has a live reload violates the `table_reload_one_live`
/// partial unique index and maps to the typed [`ControlError::ReloadInProgress`] — matched by
/// SQLSTATE + constraint *name*, never by message text. After `complete`/`failed` the row leaves
/// the index and a new request succeeds.
///
/// # Errors
///
/// Returns [`ControlError::ReloadInProgress`] when the table already has a live attempt,
/// [`ControlError::CheckViolation`] for another rejected invariant, or
/// [`ControlError::Connect`] when the request cannot reach control Postgres.
pub async fn request(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
    flavor: ReloadFlavor,
) -> Result<ReloadId, ControlError> {
    let fence_request_id = Uuid::new_v4();
    let rec = sqlx::query_file!(
        "sql/postgres/queries/request_reload.sql",
        epoch.0,
        source_schema,
        source_table,
        flavor.as_str(),
        fence_request_id,
    )
    .fetch_one(ex)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(db) = &e
            && db.code().as_deref() == Some("23505")
            && db.constraint() == Some("table_reload_one_live")
        {
            return ControlError::ReloadInProgress {
                schema: source_schema.to_string(),
                table: source_table.to_string(),
            };
        }
        ControlError::from(e)
    })?;
    Ok(rec.reload_id.into())
}

/// Idempotently persist one concrete table child decoded from a source-WAL request event.
///
/// Replaying the same `(epoch, source_request_id, schema, table)` returns the original
/// [`ReloadId`], including after that attempt is terminal. An all-published request deliberately
/// reuses its source UUID across children; the table coordinates distinguish them. Reusing the key
/// with a different flavor, scope, or parent is rejected instead of silently changing history. A
/// distinct source request for a busy table is durably appended in `requested`; [`claim_requested`]
/// serializes those rows in per-table `reload_id` order once the current attempt is terminal.
///
/// # Errors
///
/// Returns [`ControlError::SourceRequestConflict`] when the UUID's immutable payload changed, or a
/// database connectivity/invariant error.
pub async fn request_from_source(
    ex: impl PgExecutor<'_>,
    request: &SourceReloadRequest<'_>,
) -> Result<ReloadId, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/request_reload_from_source.sql",
        request.epoch.0,
        request.source_schema,
        request.source_table,
        request.flavor.as_str(),
        request.source_request_id,
        request.parent_request_id,
        request.scope.as_str(),
    )
    .fetch_optional(ex)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(db) = &e
            && db.code().as_deref() == Some("23505")
            && db.constraint() == Some("table_reload_one_live")
        {
            return ControlError::ReloadInProgress {
                schema: request.source_schema.to_string(),
                table: request.source_table.to_string(),
            };
        }
        ControlError::from(e)
    })?;

    rec.map(|row| row.reload_id.into())
        .ok_or_else(|| ControlError::SourceRequestConflict {
            request_id: request.source_request_id,
            schema: request.source_schema.to_string(),
            table: request.source_table.to_string(),
        })
}

/// Claim up to `limit` `requested` rows for this holder: set the lease, flip to `exporting`.
///
/// `FOR UPDATE SKIP LOCKED` under the guarded UPDATE makes concurrent claimers partition the
/// queue instead of double-exporting; a fully-raced claimer just gets an empty `Vec`. Source-WAL
/// rows form a per-table FIFO: only the oldest may claim, and it waits until the current
/// `exporting | export_complete` attempt becomes terminal. Legacy direct requests keep their
/// immediate duplicate rejection and take priority over an unclaimed source queue.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the atomic claim/update query fails or its rows cannot be
/// decoded.
pub async fn claim_requested(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    holder: &str,
    lease_ttl_secs: i64,
    limit: i64,
) -> Result<Vec<ReloadRow>, ControlError> {
    let rows = sqlx::query_file!(
        "sql/postgres/queries/claim_requested.sql",
        epoch.0,
        holder,
        lease_ttl_secs as f64,
        limit,
    )
    .fetch_all(ex)
    .await?;
    Ok(rows.into_iter().map(|row| typed_reload_row!(row)).collect())
}

/// Return a claimed-but-never-started row to the queue: `exporting → requested`, lease cleared.
///
/// The controller's un-claim for infra failures BETWEEN claim and exporter spawn — a
/// dead preflight connection, a control-pg blip while recording a rejection. An infra error must
/// neither terminally `fail` a valid request nor leave it `exporting` unowned; back in
/// `requested`, the next tick re-claims and retries. Holder-guarded (only the claimant un-claims)
/// and `exporting`-guarded, so it can never clobber a row someone else adopted. The SQL also
/// requires the pristine cursor and no start fence: once F exists, returning to ordinary pickup
/// would lose ownership of the connection-local source snapshot and is therefore forbidden.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded release update cannot execute.
pub async fn release_claim(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    holder: &str,
) -> Result<bool, ControlError> {
    let done = sqlx::query_file!(
        "sql/postgres/queries/release_claim.sql",
        reload_id.0,
        holder,
    )
    .execute(ex)
    .await?;
    Ok(done.rows_affected() > 0)
}

/// Renew this holder's lease on a live export. Affects zero rows — returning `false` — if we no
/// longer hold it or the export left `exporting` (a phantom exporter must not renew).
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded lease update cannot execute.
pub async fn renew_lease(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    holder: &str,
    lease_ttl_secs: i64,
) -> Result<bool, ControlError> {
    let done = sqlx::query_file!(
        "sql/postgres/queries/renew_lease.sql",
        reload_id.0,
        holder,
        lease_ttl_secs as f64,
    )
    .execute(ex)
    .await?;
    Ok(done.rows_affected() > 0)
}

/// Freeze the authoritative safe lower fence `F` and schema, and create its durable baseline
/// marker in the same control-Postgres statement.
///
/// Unlike legacy `first_lsn`, this happens before any table query or Parquet file. Consequently an
/// empty export still has a baseline that can trigger reconstruction. An exact retry is accepted
/// while the attempt remains `exporting`; a changed LSN/schema or illegal state changes zero rows.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is `exporting` with compatible
/// frozen values, or a database connectivity/invariant error.
pub async fn record_start_fence(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    start_lsn: Lsn,
    identity: ReloadFenceIdentity<'_>,
) -> Result<(), ControlError> {
    let row = sqlx::query_file!(
        "sql/postgres/queries/record_start_fence.sql",
        reload_id.0,
        start_lsn as Lsn,
        identity.schema_version.0,
        identity.request_id,
        identity.source_schema,
        identity.source_table,
    )
    .fetch_optional(ex)
    .await?;
    if row.is_none() {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting (same start_lsn and schema_version)",
        });
    }
    Ok(())
}

/// Persist the data-free upper barrier `H` after target WAL through `H` is durable.
///
/// This does not change status: [`complete_export`] separately verifies the exact marker and then
/// performs `exporting → export_complete`. Splitting the operations makes the durability point
/// explicit and crash-replayable. The marker works for empty tables because it has no file/row
/// dependency.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is `exporting`, has a frozen
/// baseline/schema, and `H >= F`, or when an existing end marker disagrees.
pub async fn record_end_marker(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    final_lsn: Lsn,
    identity: ReloadFenceIdentity<'_>,
) -> Result<(), ControlError> {
    let row = sqlx::query_file!(
        "sql/postgres/queries/record_end_marker.sql",
        reload_id.0,
        final_lsn as Lsn,
        identity.schema_version.0,
        identity.request_id,
        identity.source_schema,
        identity.source_table,
    )
    .fetch_optional(ex)
    .await?;
    if row.is_none() {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting (frozen baseline, H >= F, same end marker)",
        });
    }
    Ok(())
}

/// Read the durable boundary records for one attempt in baseline-then-end order.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the marker rows cannot be read or decoded.
pub async fn read_markers(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
) -> Result<Vec<ReloadMarkerRow>, ControlError> {
    let rows = sqlx::query_file!("sql/postgres/queries/read_reload_markers.sql", reload_id.0,)
        .fetch_all(ex)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| ReloadMarkerRow {
            reload_id: row.reload_id.into(),
            kind: row.marker_kind,
            lsn: row.lsn,
            schema_version: row.schema_version.into(),
        })
        .collect())
}

/// Record chunk `chunk_no` done: bump the cursor, store the new PK bound.
///
/// On the first chunk this retains `first_lsn` as diagnostic compatibility data (the `COALESCE`
/// prevents later chunks from overwriting it). Every executable attempt has already frozen the
/// authoritative `start_lsn` and `schema_version` through [`record_start_fence`], and SQL requires
/// each chunk watermark to equal that exact F.
/// `schema_version` is always **asserted**: every reload attempt is
/// single-schema *by construction* (H9), so a later chunk arriving with a different version means
/// the export engine missed a DDL restart — the WHERE rejects it and the mismatch is the
/// same loud zero-rows error as any illegal transition, never a silent swallow. The
/// `chunk_no = $new - 1` guard makes the cursor strictly in-order: a duplicate or out-of-order
/// advance changes zero rows and errors. A restart uses a *fresh* row rather than ever
/// mutating the frozen fields.)
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] for an out-of-order chunk, stale status, or changed
/// schema version; database failures become [`ControlError::Connect`] or
/// [`ControlError::CheckViolation`].
pub async fn advance_cursor(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    chunk_no: i64,
    cursor_pk: &serde_json::Value,
    chunk_lsn: Lsn,
    schema_version: SchemaVersionNo,
) -> Result<(), ControlError> {
    let done = sqlx::query_file!(
        "sql/postgres/queries/advance_cursor.sql",
        reload_id.0,
        chunk_no,
        cursor_pk,
        chunk_lsn as Lsn,
        schema_version.0,
    )
    .execute(ex)
    .await?;
    if done.rows_affected() == 0 {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting (in-order chunk_no, consistent schema_version)",
        });
    }
    Ok(())
}

/// Record one completely uploaded reload file without imposing worker completion order.
///
/// Parallel workers can finish remote objects in any order, so the persisted `chunk_no` is a
/// completed-file count rather than a caller-assigned sequence number. The guarded increment is
/// atomic and returns the unique count assigned by Postgres. Callers insert the file manifest and
/// invoke this function in the **same control transaction**, after the remote object is complete;
/// either both rows become visible or neither does.
///
/// `cursor_pk IS NULL` prevents a rolling deployment from mixing this mode with the legacy
/// keyset-cursor exporter. `first_lsn` remains diagnostic compatibility data and is frozen to the
/// attempt's already-durable start fence on the first successful file. The exact start fence and
/// schema guards reject stale workers after a restart or DDL successor.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is still `exporting`, belongs to
/// the supplied start fence and schema, and has never entered legacy cursor mode. Database failures
/// become [`ControlError::Connect`] or [`ControlError::CheckViolation`].
pub async fn record_exported_file(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    start_lsn: Lsn,
    schema_version: SchemaVersionNo,
) -> Result<i64, ControlError> {
    let row = sqlx::query_file!(
        "sql/postgres/queries/record_exported_file.sql",
        reload_id.0,
        start_lsn as Lsn,
        schema_version.0,
    )
    .fetch_optional(ex)
    .await?;
    let Some(row) = row else {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting (cursor-free, same start_lsn and schema_version)",
        });
    };
    Ok(row.chunk_no)
}

/// `exporting → export_complete`, recording the final watermark `H`. The sink's last act; from
/// here the LOADER finishes the walk (`complete` once `transformed_lsn >= H`). Every request source
/// and both persisted flavor spellings must already have matching durable baseline/end markers.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is still `exporting` and its
/// markers match; database failures become [`ControlError::Connect`] or
/// [`ControlError::CheckViolation`].
pub async fn complete_export(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
    final_lsn: Lsn,
) -> Result<(), ControlError> {
    let done = sqlx::query_file!(
        "sql/postgres/queries/complete_export.sql",
        reload_id.0,
        final_lsn as Lsn,
    )
    .execute(ex)
    .await?;
    if done.rows_affected() == 0 {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting (with matching durable baseline/end markers)",
        });
    }
    Ok(())
}

/// `export_complete → complete` — the loader calls this once `transformed_lsn >= final_lsn`.
/// Terminal: the row leaves the `table_reload_one_live` index, allowing the oldest queued source
/// request to be claimed and direct/operator requests to be accepted again.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is `export_complete`, or
/// [`ControlError::Connect`] if the guarded update fails.
pub async fn complete(ex: impl PgExecutor<'_>, reload_id: ReloadId) -> Result<(), ControlError> {
    let done = sqlx::query_file!("sql/postgres/queries/complete.sql", reload_id.0,)
        .execute(ex)
        .await?;
    if done.rows_affected() == 0 {
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "export_complete",
        });
    }
    Ok(())
}

/// `exporting | export_complete → failed`, and — in the SAME transaction — delete this reload's
/// staged manifest rows. A failed reload must leave nothing for the loader to claim (H9), and
/// coupling the purge to the flip means no crash window can separate them.
///
/// Takes a connection (not an executor) because this is two statements under one transaction;
/// inside an outer transaction it nests as a savepoint, so fail-and-reissue callers can wrap it
/// with the successor INSERT atomically. The purge needs no `kind`
/// filter — only reload files carry a `reload_id` (that is the point of the nullable column).
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is in a failable live status;
/// transaction, update, purge, or commit failures become [`ControlError::Connect`] or
/// [`ControlError::CheckViolation`].
pub async fn fail(
    conn: &mut PgConnection,
    reload_id: ReloadId,
    reason: &str,
) -> Result<(), ControlError> {
    let mut tx = conn.begin().await?;
    let done = sqlx::query_file!("sql/postgres/queries/fail.sql", reload_id.0, reason,)
        .execute(&mut *tx)
        .await?;
    if done.rows_affected() == 0 {
        // Dropping `tx` rolls the savepoint/transaction back.
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting or export_complete",
        });
    }
    sqlx::query_file!("sql/postgres/queries/fail_purge_files.sql", reload_id.0,)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Would restarting an attempt with `restart_count` push it past `max_restarts`? The next
/// attempt would carry `restart_count + 1`, so the cap is exceeded when that exceeds the max — a
/// `max_restarts` of 0 fails the very first mid-export DDL. Pure so it unit-tests without a DB.
///
/// The successor count is `checked_add`ed, not `+`ed: `restart_count` is whatever the `int` column
/// holds, and at [`i32::MAX`] a bare add wraps to [`i32::MIN`] in release — reporting a *spent* cap
/// as unspent, which is the one answer a restart cap must never reach by accident. `None` is also
/// the exact answer, since no `i32` cap can hold `i32::MAX + 1`.
#[must_use]
pub const fn restart_would_exceed_cap(restart_count: i32, max_restarts: i32) -> bool {
    match restart_count.checked_add(1) {
        Some(next) => next > max_restarts,
        None => true,
    }
}

/// Why one exporter attempt must be superseded by a fresh fenced attempt.
#[derive(Debug, Clone, Copy)]
enum AttemptRestartCause {
    Ddl(SchemaVersionNo),
    LostSnapshot,
}

impl AttemptRestartCause {
    fn reason(self, capped: bool, max_restarts: i32) -> String {
        let cap = if capped {
            format!("; restart cap {max_restarts} exhausted")
        } else {
            String::new()
        };
        match self {
            Self::Ddl(new_schema_version) => {
                format!("superseded: ddl bumped schema_version to {new_schema_version}{cap}")
            }
            Self::LostSnapshot => {
                format!("superseded: exporter lost its source snapshot ownership{cap}")
            }
        }
    }
}

/// Restart one exporter attempt: in ONE transaction, fail the old attempt — [`fail`]'s coupling
/// purges its `kind='reload'` manifest rows, so no observer ever sees a terminal attempt with
/// claimable chunk files — and, unless the restart cap is spent, INSERT its successor.
///
/// The successor is born `exporting`, carrying the old row's identity **and its lease** (an
/// `INSERT … SELECT` copies `lease_holder`/`lease_expiry` verbatim, so the running exporter keeps
/// ownership and no pickup round-trip is spent) with a FRESH cursor: `chunk_no` 0, `cursor_pk`
/// NULL, and `schema_version` NULL so the successor resolves the current registry version before
/// opening its new snapshot. `restart_count` is `old + 1`. The `table_reload_one_live` partial unique index
/// tolerates the successor only because the predecessor turns terminal in the SAME transaction.
///
/// Returns the successor `reload_id`, or `None` when `restart_count + 1 > max_restarts`: then the
/// attempt is failed-only (the cap named in the reason) and no successor is written — visible
/// waste, never silent mis-reconciliation (the design's H9 choice).
///
async fn restart_attempt(
    conn: &mut PgConnection,
    old: &ReloadRow,
    max_restarts: i32,
    cause: AttemptRestartCause,
) -> Result<Option<ReloadId>, ControlError> {
    // Computed before `capped` is known, so it saturates for the same reason the cap check is
    // checked: an `i32::MAX` predecessor must not wrap here either. The capped path discards it, and
    // on the path that keeps it the cap has already proved the successor count is exact.
    let next_restart = old.restart_count.saturating_add(1);
    let capped = restart_would_exceed_cap(old.restart_count, max_restarts);
    let reason = cause.reason(capped, max_restarts);

    let mut tx = conn.begin().await?;
    // Reuse fail() (a savepoint inside this tx): one place owns "terminal ⇒ no claimable files".
    // The Transaction auto-derefs to the PgConnection fail() wants; its inner begin() nests as a
    // savepoint under this transaction.
    fail(&mut tx, old.reload_id, &reason).await?;
    if capped {
        // Fail-only: the reload is abandoned, its chunk files already purged by fail().
        tx.commit().await?;
        return Ok(None);
    }
    // The successor: copy identity + lease from the (now failed) predecessor, reset the cursor and
    // schema_version, bump restart_count. Selecting only the carried columns leaves chunk_no/
    // cursor_pk/first_lsn/final_lsn/schema_version/error at their table defaults (fresh start).
    let rec = sqlx::query_file!(
        "sql/postgres/queries/restart_for_ddl.sql",
        old.reload_id.0,
        next_restart,
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(rec.reload_id.into()))
}

/// H9 restart-on-DDL. The successor gets a fresh cursor, schema resolution, and F fence.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] if the predecessor can no longer be failed, or
/// [`ControlError::Connect`] / [`ControlError::CheckViolation`] if the transaction, purge,
/// successor insert, or commit fails.
pub async fn restart_for_ddl(
    conn: &mut PgConnection,
    old: &ReloadRow,
    new_schema_version: SchemaVersionNo,
    max_restarts: i32,
) -> Result<Option<ReloadId>, ControlError> {
    restart_attempt(
        conn,
        old,
        max_restarts,
        AttemptRestartCause::Ddl(new_schema_version),
    )
    .await
}

/// Crash recovery for an adopted exporter whose source snapshot no longer exists. A read-only
/// PostgreSQL snapshot is connection-local and cannot be resumed from a durable chunk cursor, so
/// the predecessor is failed/purged and a lease-carrying successor starts at chunk zero with a
/// fresh F. This uses the same bounded restart budget as DDL supersession.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] if the predecessor can no longer be failed, or
/// [`ControlError::Connect`] / [`ControlError::CheckViolation`] if the transaction, purge,
/// successor insert, or commit fails.
pub async fn restart_for_lost_snapshot(
    conn: &mut PgConnection,
    old: &ReloadRow,
    max_restarts: i32,
) -> Result<Option<ReloadId>, ControlError> {
    restart_attempt(conn, old, max_restarts, AttemptRestartCause::LostSnapshot).await
}

/// Crash recovery for an adopted attempt that has not committed any baseline chunk. The old
/// connection may nevertheless have appended F or H to source WAL just before it disappeared, so
/// reusing its fence identity would let a delayed marker bound a later snapshot. Atomically fail
/// the predecessor and create a fresh lease-carrying successor, but preserve `restart_count`: no
/// durable baseline work was lost, so this recovery does not spend the bounded DDL/snapshot-loss
/// restart budget.
///
/// The successor insert rechecks the pristine cursor under the same transaction. If the abandoned
/// exporter raced adoption and committed a chunk after the caller read `old`, the whole transition
/// rolls back and reports [`ControlError::ReloadTransition`]; a later adoption will then use the
/// ordinary, budgeted lost-snapshot path.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] if the predecessor is no longer live or no longer
/// pristine, or [`ControlError::Connect`] / [`ControlError::CheckViolation`] if the transaction,
/// purge, successor insert, or commit fails.
pub async fn restart_pristine_adoption(
    conn: &mut PgConnection,
    old: &ReloadRow,
) -> Result<ReloadId, ControlError> {
    let mut tx = conn.begin().await?;
    fail(
        &mut tx,
        old.reload_id,
        "superseded: adopted pristine attempt gets a fresh source fence identity",
    )
    .await?;
    let successor = sqlx::query(
        "INSERT INTO walrus.table_reload
             (epoch, source_schema, source_table, flavor, status, restart_count,
              lease_holder, lease_expiry, parent_request_id, request_scope)
         SELECT epoch, source_schema, source_table, flavor, 'exporting', $2,
                lease_holder, lease_expiry, COALESCE(source_request_id, parent_request_id),
                request_scope
         FROM walrus.table_reload
         WHERE reload_id = $1 AND chunk_no = 0 AND cursor_pk IS NULL
         RETURNING reload_id",
    )
    .bind(old.reload_id.0)
    .bind(old.restart_count)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(successor) = successor else {
        return Err(ControlError::ReloadTransition {
            reload_id: old.reload_id,
            expected: "exporting with no durable chunk progress",
        });
    };
    let successor_id = ReloadId(successor.try_get("reload_id")?);
    tx.commit().await?;
    Ok(successor_id)
}

/// The loader's completion flip (H10): every `export_complete` reload for this table whose
/// `final_lsn` (H) the mirror has now reached (`transformed_lsn >= H`) becomes `complete`. One
/// guarded batch UPDATE that JOINs `loader_checkpoint` for the live `transformed_lsn` — no extra
/// read, and a natural no-op (0 rows) on the vast majority of cycles that have no `export_complete`
/// reload. Idempotent and at-least-once safe (a re-run flips nothing — the row is already terminal),
/// so the loader can call it every cycle. Returns the reload_ids it completed (for the log). The
/// LOADER owns this flip; the sink never writes `complete` (H10 — no service gets a write path into
/// another's state row).
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the checkpoint-joined completion update cannot execute.
pub async fn complete_reached(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Vec<ReloadId>, ControlError> {
    let rows = sqlx::query_file!(
        "sql/postgres/queries/complete_reached.sql",
        epoch.0,
        source_schema,
        source_table,
    )
    .fetch_all(ex)
    .await?;
    Ok(rows.into_iter().map(|row| row.reload_id.into()).collect())
}

/// The authoritative `start_lsn` (`F`) below which a pending **rebuild** supersedes this table's
/// manifest files. A live `reload`-flavor reload's rebuild trigger will `CREATE OR
/// REPLACE` the mirror at the new schema and `delete_superseded` every non-reload file with
/// `lsn_end <= start_lsn` — so the loader must NOT reconcile (and possibly quarantine on) such a
/// file: it skips it and lets the rebuild replace the mirror. Only the explicit start fence is
/// authoritative; legacy `first_lsn` is diagnostic and can never authorize reconciliation. There
/// is at most one active (`exporting | export_complete`) attempt per table; later source-backed
/// `requested` rows carry no fence yet and remain queued behind it.
///
/// This is what closes the quarantine-recovery loop: without it, a lossy-`ALTER` stream file
/// (lower `lsn_end` than the reload's `first_lsn`) re-quarantines the loader on every restart before
/// it can reach the reload chunk file that would clear the quarantine.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the pending-rebuild lookup cannot execute or decode.
pub async fn reload_supersede_floor(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<Lsn>, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/reload_supersede_floor.sql",
        epoch.0,
        source_schema,
        source_table,
    )
    .fetch_optional(ex)
    .await?;
    Ok(rec.and_then(|r| r.first_lsn))
}

/// Crash recovery (H7): the `exporting` reloads this sink may resume — its OWN live lease when
/// `include_own_live_lease` is enabled for startup, or an EXPIRED lease on any later scan. Re-acquires the
/// lease in the SAME guarded `UPDATE … RETURNING` (with `FOR UPDATE SKIP LOCKED`) so two racing
/// pods can never both adopt one row. A live FOREIGN lease (`lease_holder <> me AND lease_expiry >
/// now()`) is deliberately excluded — never stolen. `requested` rows are excluded too: those go
/// through ordinary pickup ([`claim_requested`]), keeping the two paths disjoint on status.
///
/// Recovery reads from control-pg, NOT from WAL redelivery (H7): by restart time the signals' LSNs
/// are behind `confirmed_flush`, acked and gone — the chunk cursor on the returned row is the only
/// thing a resume needs.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the guarded adoption query fails or its rows cannot be
/// decoded.
pub async fn adopt_resumable(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    holder: &str,
    lease_ttl_secs: i64,
    limit: i64,
    include_own_live_lease: bool,
) -> Result<Vec<ReloadRow>, ControlError> {
    let rows = sqlx::query_file!(
        "sql/postgres/queries/adopt_resumable.sql",
        epoch.0,
        holder,
        lease_ttl_secs as f64,
        limit,
        include_own_live_lease,
    )
    .fetch_all(ex)
    .await?;
    Ok(rows.into_iter().map(|row| typed_reload_row!(row)).collect())
}

/// Genuinely stuck exports: `exporting` rows whose lease has expired and which nobody is
/// renewing — a dead exporter no startup scan adopted. Surfaced as a per-tick warning and alert.
/// `export_complete` rows with an expired lease are NOT stuck — they are waiting on
/// the loader, by design — so the filter is `exporting` only.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if expired exporters cannot be queried or decoded.
pub async fn stuck_exporting(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<(ReloadId, Option<String>)>, ControlError> {
    let rows = sqlx::query_file!("sql/postgres/queries/stuck_exporting.sql", epoch.0,)
        .fetch_all(ex)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.reload_id.into(), row.lease_holder))
        .collect())
}

/// Tables mid-rebuild — the loader-pause predicate's input.
///
/// This includes both persisted flavors because `resync` is a compatibility alias for the same
/// rebuild. It deliberately includes `requested | exporting` only: the pause MUST lift at
/// `export_complete`, because the rebuild is triggered by the loader claiming the chunk files;
/// pausing through `export_complete` would deadlock the reload forever.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if active rebuild rows cannot be queried or decoded.
pub async fn active_rebuilds(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<ReloadRow>, ControlError> {
    let rows = sqlx::query_file!("sql/postgres/queries/active_rebuilds.sql", epoch.0,)
        .fetch_all(ex)
        .await?;
    Ok(rows.into_iter().map(|row| typed_reload_row!(row)).collect())
}

/// Return the marker-complete rebuild ready for this table's hidden-generation reconciliation.
///
/// This is deliberately independent of `file_manifest`: an empty table has no reload Parquet file,
/// but its matching baseline/end records are sufficient to initialize and publish an empty shadow.
/// The query verifies that both marker LSNs and schema versions exactly match the attempt before
/// exposing it to the loader.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the ready rebuild cannot be queried or decoded.
pub async fn ready_rebuild(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<ReloadRow>, ControlError> {
    let row = sqlx::query_file!(
        "sql/postgres/queries/ready_rebuild.sql",
        epoch.0,
        source_schema,
        source_table,
    )
    .fetch_optional(ex)
    .await?;
    Ok(row.map(|row| typed_reload_row!(row)))
}

/// Read one reload attempt, if it exists.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the reload row cannot be queried or decoded.
pub async fn get(
    ex: impl PgExecutor<'_>,
    reload_id: ReloadId,
) -> Result<Option<ReloadRow>, ControlError> {
    let row = sqlx::query_file!("sql/postgres/queries/get.sql", reload_id.0,)
        .fetch_optional(ex)
        .await?;
    Ok(row.map(|row| typed_reload_row!(row)))
}

#[cfg(test)]
#[path = "reload_test.rs"]
mod tests;
