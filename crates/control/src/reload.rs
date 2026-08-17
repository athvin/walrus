//! `table_reload` models: the single-table-reload state machine (reload H4/H5/H10, PR 6.1).
//!
//! Control-pg owns the reload's brain; every transition below is a **guarded UPDATE** —
//! `UPDATE … WHERE status = <expected>` — so a lost race or an illegal jump changes zero rows and
//! surfaces as the typed [`ControlError::ReloadTransition`], never a silent double-claim. The
//! status walk is `requested → exporting → export_complete → complete`, with `failed` terminal
//! from the two middle states. There is deliberately **no** `superseded` status: a DDL restart
//! (PR 6.8) is `fail()` with an explanatory reason plus a fresh successor row.
//!
//! `reload_id` is a **bigserial, not a UUID** (a recorded deviation from the design doc): "honor
//! only the latest reload_id" (H9) becomes a numeric max, and the id fits the loader's
//! `_walrus_meta` `v BIGINT` store verbatim. The duplicate-request guarantee a UUID key was for
//! lives in the `table_reload_one_live` partial unique index instead — one non-terminal reload
//! per `(epoch, schema, table)`, enforced by the database, mapped to the typed
//! [`ControlError::ReloadInProgress`]. What a client-supplied UUID *would* have bought —
//! caller-side idempotency keys — is not needed: "the same table, again" *is* the idempotency
//! rule here.

use crate::{ControlError, parse::ParseEnumError};
use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use sqlx::{Connection, PgConnection, PgExecutor};

/// `reload` rebuilds (clear + re-export — the quarantine-recovery flavor); `resync` merges chunks
/// over the *live* mirror and tolerates phantoms (reload H3). Both flavors share every state and
/// every transition in this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum ReloadFlavor {
    Reload,
    Resync,
}

impl ReloadFlavor {
    /// The exact string the migration's CHECK constraint admits (second line of defense).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ReloadFlavor::Reload => "reload",
            ReloadFlavor::Resync => "resync",
        }
    }
}

impl std::str::FromStr for ReloadFlavor {
    type Err = ParseEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "reload" => Ok(ReloadFlavor::Reload),
            "resync" => Ok(ReloadFlavor::Resync),
            other => Err(ParseEnumError::new("table_reload.flavor", other)),
        }
    }
}

/// `requested → exporting → export_complete → complete`; `failed` terminal from the middle. The
/// SQL CHECK carries the same five values — belt and braces, like `loader_checkpoint`'s CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(rename_all = "snake_case")]
pub enum ReloadStatus {
    Requested,
    Exporting,
    ExportComplete,
    Complete,
    Failed,
}

impl ReloadStatus {
    /// The exact string the migration's CHECK constraint admits.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ReloadStatus::Requested => "requested",
            ReloadStatus::Exporting => "exporting",
            ReloadStatus::ExportComplete => "export_complete",
            ReloadStatus::Complete => "complete",
            ReloadStatus::Failed => "failed",
        }
    }
}

impl std::str::FromStr for ReloadStatus {
    type Err = ParseEnumError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "requested" => Ok(ReloadStatus::Requested),
            "exporting" => Ok(ReloadStatus::Exporting),
            "export_complete" => Ok(ReloadStatus::ExportComplete),
            "complete" => Ok(ReloadStatus::Complete),
            "failed" => Ok(ReloadStatus::Failed),
            other => Err(ParseEnumError::new("table_reload.status", other)),
        }
    }
}

/// One reload attempt. `lease_expiry`/timestamps stay out of the model — every time comparison
/// happens in SQL (`now()`), like `table_ownership`, so the Rust side never holds a clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadRow {
    pub reload_id: ReloadId,
    pub epoch: EpochNo,
    pub source_schema: String,
    pub source_table: String,
    pub flavor: ReloadFlavor,
    pub status: ReloadStatus,
    /// Last COMPLETED chunk; 0 = none exported yet.
    pub chunk_no: i64,
    /// Last exported PK bound (a JSON array, so composite PKs need no special casing); `None` = start.
    pub cursor_pk: Option<serde_json::Value>,
    /// L₁ — the first chunk's echo watermark; frozen by the first `advance_cursor`, immutable after.
    pub first_lsn: Option<Lsn>,
    /// H — set at `export_complete`; the loader flips `complete` once `transformed_lsn >= H`.
    pub final_lsn: Option<Lsn>,
    /// The single schema version this attempt exports at; frozen alongside `first_lsn`.
    pub schema_version: Option<SchemaVersionNo>,
    /// DDL restarts consumed so far (PR 6.8 caps it at `reload_max_restarts`).
    pub restart_count: i32,
    pub lease_holder: Option<String>,
    pub error: Option<String>,
}

macro_rules! typed_reload_row {
    ($row:expr_2021) => {{
        let row = $row;
        ReloadRow {
            reload_id: row.reload_id.into(),
            epoch: row.epoch.into(),
            source_schema: row.source_schema,
            source_table: row.source_table,
            flavor: row.flavor,
            status: row.status,
            chunk_no: row.chunk_no,
            cursor_pk: row.cursor_pk,
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
    let rec = sqlx::query_file!(
        "sql/postgres/queries/request_reload.sql",
        epoch.0,
        source_schema,
        source_table,
        flavor.as_str(),
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

/// Claim up to `limit` `requested` rows for this holder: set the lease, flip to `exporting`.
///
/// `FOR UPDATE SKIP LOCKED` under the guarded UPDATE makes concurrent claimers partition the
/// queue instead of double-exporting; a fully-raced claimer just gets an empty `Vec`.
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
/// The controller's un-claim for infra failures BETWEEN claim and exporter spawn (PR 6.4) — a
/// dead preflight connection, a control-pg blip while recording a rejection. An infra error must
/// neither terminally `fail` a valid request nor leave it `exporting` unowned; back in
/// `requested`, the next tick re-claims and retries. Holder-guarded (only the claimant un-claims)
/// and `exporting`-guarded, so it can never clobber a row someone else adopted.
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

/// Record chunk `chunk_no` done: bump the cursor, store the new PK bound.
///
/// On the FIRST chunk this freezes `first_lsn = L₁` (the `COALESCE`: later chunks legitimately
/// carry a new `L_i` each call, so their values are simply not the first and never overwrite it)
/// and `schema_version` — which, unlike the LSN, is also **asserted**: every reload attempt is
/// single-schema *by construction* (H9), so a later chunk arriving with a different version means
/// the export engine missed a DDL restart (PR 6.8) — the WHERE rejects it and the mismatch is the
/// same loud zero-rows error as any illegal transition, never a silent swallow. The
/// `chunk_no = $new - 1` guard makes the cursor strictly in-order: a duplicate or out-of-order
/// advance changes zero rows and errors. (PR 6.8 restarts with a *fresh* row rather than ever
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

/// `exporting → export_complete`, recording the final watermark `H`. The sink's last act; from
/// here the LOADER finishes the walk (PR 6.9: `complete` once `transformed_lsn >= H`).
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] unless the attempt is still `exporting`; database
/// failures become [`ControlError::Connect`] or [`ControlError::CheckViolation`].
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
            expected: "exporting",
        });
    }
    Ok(())
}

/// `export_complete → complete` — the loader calls this once `transformed_lsn >= final_lsn`
/// (PR 6.9). Terminal: the row leaves the `table_reload_one_live` index and the table can be
/// reloaded again.
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
/// inside an outer transaction it nests as a savepoint, so callers like PR 6.8's
/// fail-and-reissue can wrap it with the successor INSERT atomically. The purge needs no `kind`
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

/// Would restarting an attempt with `restart_count` push it past `max_restarts` (PR 6.8)? The next
/// attempt would carry `restart_count + 1`, so the cap is exceeded when that exceeds the max — a
/// `max_restarts` of 0 fails the very first mid-export DDL. Pure so it unit-tests without a DB.
#[must_use]
pub const fn restart_would_exceed_cap(restart_count: i32, max_restarts: i32) -> bool {
    restart_count + 1 > max_restarts
}

/// H9 restart-on-DDL (PR 6.8): in ONE transaction, fail the old attempt — [`fail`]'s coupling
/// purges its `kind='reload'` manifest rows, so no observer ever sees a terminal attempt with
/// claimable chunk files — and, unless the restart cap is spent, INSERT its successor.
///
/// The successor is born `exporting`, carrying the old row's identity **and its lease** (an
/// `INSERT … SELECT` copies `lease_holder`/`lease_expiry` verbatim, so the running exporter keeps
/// ownership and no pickup round-trip is spent) with a FRESH cursor: `chunk_no` 0, `cursor_pk`
/// NULL, and — the point of the whole exercise — `schema_version` NULL so chunk 1 re-freezes it at
/// the NEW version. `restart_count` is `old + 1`. The `table_reload_one_live` partial unique index
/// tolerates the successor only because the predecessor turns terminal in the SAME transaction.
///
/// Returns the successor `reload_id`, or `None` when `restart_count + 1 > max_restarts`: then the
/// attempt is failed-only (the cap named in the reason) and no successor is written — visible
/// waste, never silent mis-reconciliation (the design's H9 choice).
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] if the predecessor can no longer be failed, or
/// [`ControlError::Connect`] / [`ControlError::CheckViolation`] if the transaction, purge, successor
/// insert, or commit fails.
pub async fn restart_for_ddl(
    conn: &mut PgConnection,
    old: &ReloadRow,
    new_schema_version: SchemaVersionNo,
    max_restarts: i32,
) -> Result<Option<ReloadId>, ControlError> {
    let next_restart = old.restart_count + 1;
    let capped = restart_would_exceed_cap(old.restart_count, max_restarts);
    let reason = if capped {
        format!(
            "superseded: ddl bumped schema_version to {new_schema_version}; \
             restart cap {max_restarts} exhausted"
        )
    } else {
        format!("superseded: ddl bumped schema_version to {new_schema_version}")
    };

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

/// The loader's completion flip (PR 6.9 / H10): every `export_complete` reload for this table whose
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

/// The floor `first_lsn` (`L₁`) below which a pending **rebuild** supersedes this table's pending
/// manifest files (PR 6.12). A live `reload`-flavor reload's rebuild trigger will `CREATE OR
/// REPLACE` the mirror at the new schema and `delete_superseded` every non-reload file with
/// `lsn_end <= first_lsn` — so the loader must NOT reconcile (and possibly quarantine on) such a
/// file: it skips it and lets the rebuild replace the mirror. Returns `first_lsn` for a
/// `reload`-flavor reload in `requested|exporting|export_complete` with `first_lsn` frozen, else
/// `None` (there is at most one live reload per table — the `table_reload_one_live` index).
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

/// Startup crash-recovery (PR 6.9 / H7): the `exporting` reloads this sink may resume — its OWN
/// lease (a restart of the same instance) or an EXPIRED one (a dead instance). Re-acquires the
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
) -> Result<Vec<ReloadRow>, ControlError> {
    let rows = sqlx::query_file!(
        "sql/postgres/queries/adopt_resumable.sql",
        epoch.0,
        holder,
        lease_ttl_secs as f64,
        limit,
    )
    .fetch_all(ex)
    .await?;
    Ok(rows.into_iter().map(|row| typed_reload_row!(row)).collect())
}

/// Genuinely stuck exports (PR 6.9): `exporting` rows whose lease has expired and which nobody is
/// renewing — a dead exporter no startup scan adopted. Surfaced as a per-tick warn (the alert rule
/// is PR 6.11's). `export_complete` rows with an expired lease are NOT stuck — they are waiting on
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

/// Tables mid-rebuild — the loader-pause predicate's input (PR 6.6).
///
/// Deliberately `flavor = 'reload'` only (a `resync` never pauses anything — H3) and deliberately
/// `requested | exporting` only: the pause MUST lift at `export_complete`, because the rebuild is
/// *triggered by the loader claiming the chunk files* — pausing through `export_complete` would
/// deadlock the reload forever (PR 6.6's gotcha, baked in here so no caller re-derives it).
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
