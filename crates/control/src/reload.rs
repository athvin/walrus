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

use crate::ControlError;
use common::Lsn;
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
    pub fn as_str(self) -> &'static str {
        match self {
            ReloadFlavor::Reload => "reload",
            ReloadFlavor::Resync => "resync",
        }
    }
}

impl std::str::FromStr for ReloadFlavor {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "reload" => Ok(ReloadFlavor::Reload),
            "resync" => Ok(ReloadFlavor::Resync),
            other => Err(format!("unknown reload flavor: {other}")),
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
    pub fn as_str(self) -> &'static str {
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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "requested" => Ok(ReloadStatus::Requested),
            "exporting" => Ok(ReloadStatus::Exporting),
            "export_complete" => Ok(ReloadStatus::ExportComplete),
            "complete" => Ok(ReloadStatus::Complete),
            "failed" => Ok(ReloadStatus::Failed),
            other => Err(format!("unknown reload status: {other}")),
        }
    }
}

/// One reload attempt. `lease_expiry`/timestamps stay out of the model — every time comparison
/// happens in SQL (`now()`), like `table_ownership`, so the Rust side never holds a clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadRow {
    pub reload_id: i64,
    pub epoch: i64,
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
    pub schema_version: Option<i64>,
    /// DDL restarts consumed so far (PR 6.8 caps it at `reload_max_restarts`).
    pub restart_count: i32,
    pub lease_holder: Option<String>,
    pub error: Option<String>,
}

/// INSERT a reload request (`status='requested'`); returns the new `reload_id`.
///
/// A second request while the table has a live reload violates the `table_reload_one_live`
/// partial unique index and maps to the typed [`ControlError::ReloadInProgress`] — matched by
/// SQLSTATE + constraint *name*, never by message text. After `complete`/`failed` the row leaves
/// the index and a new request succeeds.
pub async fn request(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    source_schema: &str,
    source_table: &str,
    flavor: ReloadFlavor,
) -> Result<i64, ControlError> {
    let rec = sqlx::query!(
        r#"
        INSERT INTO walrus.table_reload (epoch, source_schema, source_table, flavor)
        VALUES ($1, $2, $3, $4)
        RETURNING reload_id
        "#,
        epoch,
        source_schema,
        source_table,
        flavor.as_str(),
    )
    .fetch_one(ex)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(db) = &e {
            if db.code().as_deref() == Some("23505")
                && db.constraint() == Some("table_reload_one_live")
            {
                return ControlError::ReloadInProgress {
                    schema: source_schema.to_string(),
                    table: source_table.to_string(),
                };
            }
        }
        ControlError::from_sqlx(e)
    })?;
    Ok(rec.reload_id)
}

/// Claim up to `limit` `requested` rows for this holder: set the lease, flip to `exporting`.
///
/// `FOR UPDATE SKIP LOCKED` under the guarded UPDATE makes concurrent claimers partition the
/// queue instead of double-exporting; a fully-raced claimer just gets an empty `Vec`.
pub async fn claim_requested(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    holder: &str,
    lease_ttl_secs: i64,
    limit: i64,
) -> Result<Vec<ReloadRow>, ControlError> {
    sqlx::query_as!(
        ReloadRow,
        r#"
        UPDATE walrus.table_reload
        SET status = 'exporting',
            lease_holder = $2,
            lease_expiry = now() + make_interval(secs => $3),
            updated_at = now()
        WHERE reload_id IN (
            SELECT reload_id FROM walrus.table_reload
            WHERE epoch = $1 AND status = 'requested'
            ORDER BY reload_id
            LIMIT $4
            FOR UPDATE SKIP LOCKED
        )
        RETURNING reload_id, epoch, source_schema, source_table,
                  flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
                  chunk_no, cursor_pk,
                  first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
                  schema_version, restart_count, lease_holder, error
        "#,
        epoch,
        holder,
        lease_ttl_secs as f64,
        limit,
    )
    .fetch_all(ex)
    .await
    .map_err(ControlError::from_sqlx)
}

/// Return a claimed-but-never-started row to the queue: `exporting → requested`, lease cleared.
///
/// The controller's un-claim for infra failures BETWEEN claim and exporter spawn (PR 6.4) — a
/// dead preflight connection, a control-pg blip while recording a rejection. An infra error must
/// neither terminally `fail` a valid request nor leave it `exporting` unowned; back in
/// `requested`, the next tick re-claims and retries. Holder-guarded (only the claimant un-claims)
/// and `exporting`-guarded, so it can never clobber a row someone else adopted.
pub async fn release_claim(
    ex: impl PgExecutor<'_>,
    reload_id: i64,
    holder: &str,
) -> Result<bool, ControlError> {
    let done = sqlx::query!(
        r#"
        UPDATE walrus.table_reload
        SET status = 'requested', lease_holder = NULL, lease_expiry = NULL, updated_at = now()
        WHERE reload_id = $1 AND lease_holder = $2 AND status = 'exporting'
        "#,
        reload_id,
        holder,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(done.rows_affected() > 0)
}

/// Renew this holder's lease on a live export. Affects zero rows — returning `false` — if we no
/// longer hold it or the export left `exporting` (a phantom exporter must not renew).
pub async fn renew_lease(
    ex: impl PgExecutor<'_>,
    reload_id: i64,
    holder: &str,
    lease_ttl_secs: i64,
) -> Result<bool, ControlError> {
    let done = sqlx::query!(
        r#"
        UPDATE walrus.table_reload
        SET lease_expiry = now() + make_interval(secs => $3), updated_at = now()
        WHERE reload_id = $1 AND lease_holder = $2 AND status = 'exporting'
        "#,
        reload_id,
        holder,
        lease_ttl_secs as f64,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
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
pub async fn advance_cursor(
    ex: impl PgExecutor<'_>,
    reload_id: i64,
    chunk_no: i64,
    cursor_pk: &serde_json::Value,
    chunk_lsn: Lsn,
    schema_version: i64,
) -> Result<(), ControlError> {
    let done = sqlx::query!(
        r#"
        UPDATE walrus.table_reload
        SET chunk_no = $2,
            cursor_pk = $3,
            first_lsn = COALESCE(first_lsn, $4),
            schema_version = COALESCE(schema_version, $5),
            updated_at = now()
        WHERE reload_id = $1 AND status = 'exporting' AND chunk_no = $2::bigint - 1
          AND (schema_version IS NULL OR schema_version = $5)
        "#,
        reload_id,
        chunk_no,
        cursor_pk,
        chunk_lsn as Lsn,
        schema_version,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
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
pub async fn complete_export(
    ex: impl PgExecutor<'_>,
    reload_id: i64,
    final_lsn: Lsn,
) -> Result<(), ControlError> {
    let done = sqlx::query!(
        r#"
        UPDATE walrus.table_reload
        SET status = 'export_complete', final_lsn = $2, updated_at = now()
        WHERE reload_id = $1 AND status = 'exporting'
        "#,
        reload_id,
        final_lsn as Lsn,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
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
pub async fn complete(ex: impl PgExecutor<'_>, reload_id: i64) -> Result<(), ControlError> {
    let done = sqlx::query!(
        r#"
        UPDATE walrus.table_reload
        SET status = 'complete', updated_at = now()
        WHERE reload_id = $1 AND status = 'export_complete'
        "#,
        reload_id,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
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
pub async fn fail(
    conn: &mut PgConnection,
    reload_id: i64,
    reason: &str,
) -> Result<(), ControlError> {
    let mut tx = conn.begin().await.map_err(ControlError::from_sqlx)?;
    let done = sqlx::query!(
        r#"
        UPDATE walrus.table_reload
        SET status = 'failed', error = $2, updated_at = now()
        WHERE reload_id = $1 AND status IN ('exporting', 'export_complete')
        "#,
        reload_id,
        reason,
    )
    .execute(&mut *tx)
    .await
    .map_err(ControlError::from_sqlx)?;
    if done.rows_affected() == 0 {
        // Dropping `tx` rolls the savepoint/transaction back.
        return Err(ControlError::ReloadTransition {
            reload_id,
            expected: "exporting or export_complete",
        });
    }
    sqlx::query!(
        "DELETE FROM walrus.file_manifest WHERE reload_id = $1",
        reload_id,
    )
    .execute(&mut *tx)
    .await
    .map_err(ControlError::from_sqlx)?;
    tx.commit().await.map_err(ControlError::from_sqlx)?;
    Ok(())
}

/// Would restarting an attempt with `restart_count` push it past `max_restarts` (PR 6.8)? The next
/// attempt would carry `restart_count + 1`, so the cap is exceeded when that exceeds the max — a
/// `max_restarts` of 0 fails the very first mid-export DDL. Pure so it unit-tests without a DB.
pub fn restart_would_exceed_cap(restart_count: i32, max_restarts: i32) -> bool {
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
pub async fn restart_for_ddl(
    conn: &mut PgConnection,
    old: &ReloadRow,
    new_schema_version: i64,
    max_restarts: i32,
) -> Result<Option<i64>, ControlError> {
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

    let mut tx = conn.begin().await.map_err(ControlError::from_sqlx)?;
    // Reuse fail() (a savepoint inside this tx): one place owns "terminal ⇒ no claimable files".
    // The Transaction auto-derefs to the PgConnection fail() wants; its inner begin() nests as a
    // savepoint under this transaction.
    fail(&mut tx, old.reload_id, &reason).await?;
    if capped {
        // Fail-only: the reload is abandoned, its chunk files already purged by fail().
        tx.commit().await.map_err(ControlError::from_sqlx)?;
        return Ok(None);
    }
    // The successor: copy identity + lease from the (now failed) predecessor, reset the cursor and
    // schema_version, bump restart_count. Selecting only the carried columns leaves chunk_no/
    // cursor_pk/first_lsn/final_lsn/schema_version/error at their table defaults (fresh start).
    let rec = sqlx::query!(
        r#"
        INSERT INTO walrus.table_reload
            (epoch, source_schema, source_table, flavor, status, restart_count,
             lease_holder, lease_expiry)
        SELECT epoch, source_schema, source_table, flavor, 'exporting', $2,
               lease_holder, lease_expiry
        FROM walrus.table_reload
        WHERE reload_id = $1
        RETURNING reload_id
        "#,
        old.reload_id,
        next_restart,
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(ControlError::from_sqlx)?;
    tx.commit().await.map_err(ControlError::from_sqlx)?;
    Ok(Some(rec.reload_id))
}

/// The loader's completion flip (PR 6.9 / H10): every `export_complete` reload for this table whose
/// `final_lsn` (H) the mirror has now reached (`transformed_lsn >= H`) becomes `complete`. One
/// guarded batch UPDATE that JOINs `loader_checkpoint` for the live `transformed_lsn` — no extra
/// read, and a natural no-op (0 rows) on the vast majority of cycles that have no `export_complete`
/// reload. Idempotent and at-least-once safe (a re-run flips nothing — the row is already terminal),
/// so the loader can call it every cycle. Returns the reload_ids it completed (for the log). The
/// LOADER owns this flip; the sink never writes `complete` (H10 — no service gets a write path into
/// another's state row).
pub async fn complete_reached(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    source_schema: &str,
    source_table: &str,
) -> Result<Vec<i64>, ControlError> {
    let rows = sqlx::query!(
        r#"
        UPDATE walrus.table_reload r
        SET status = 'complete', updated_at = now()
        FROM walrus.loader_checkpoint c
        WHERE r.epoch = $1 AND r.source_schema = $2 AND r.source_table = $3
          AND r.status = 'export_complete'
          AND c.epoch = r.epoch AND c.source_schema = r.source_schema
          AND c.source_table = r.source_table
          AND r.final_lsn <= c.transformed_lsn
        RETURNING r.reload_id
        "#,
        epoch,
        source_schema,
        source_table,
    )
    .fetch_all(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(rows.into_iter().map(|r| r.reload_id).collect())
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
pub async fn reload_supersede_floor(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<Lsn>, ControlError> {
    let rec = sqlx::query!(
        r#"
        SELECT first_lsn AS "first_lsn: Lsn"
        FROM walrus.table_reload
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
          AND flavor = 'reload'
          AND status IN ('requested', 'exporting', 'export_complete')
          AND first_lsn IS NOT NULL
        ORDER BY reload_id DESC
        LIMIT 1
        "#,
        epoch,
        source_schema,
        source_table,
    )
    .fetch_optional(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
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
pub async fn adopt_resumable(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    holder: &str,
    lease_ttl_secs: i64,
    limit: i64,
) -> Result<Vec<ReloadRow>, ControlError> {
    sqlx::query_as!(
        ReloadRow,
        r#"
        UPDATE walrus.table_reload
        SET lease_holder = $2,
            lease_expiry = now() + make_interval(secs => $3),
            updated_at = now()
        WHERE reload_id IN (
            SELECT reload_id FROM walrus.table_reload
            WHERE epoch = $1 AND status = 'exporting'
              AND (lease_holder = $2 OR lease_expiry < now())
            ORDER BY reload_id
            LIMIT $4
            FOR UPDATE SKIP LOCKED
        )
        RETURNING reload_id, epoch, source_schema, source_table,
                  flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
                  chunk_no, cursor_pk,
                  first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
                  schema_version, restart_count, lease_holder, error
        "#,
        epoch,
        holder,
        lease_ttl_secs as f64,
        limit,
    )
    .fetch_all(ex)
    .await
    .map_err(ControlError::from_sqlx)
}

/// Genuinely stuck exports (PR 6.9): `exporting` rows whose lease has expired and which nobody is
/// renewing — a dead exporter no startup scan adopted. Surfaced as a per-tick warn (the alert rule
/// is PR 6.11's). `export_complete` rows with an expired lease are NOT stuck — they are waiting on
/// the loader, by design — so the filter is `exporting` only.
pub async fn stuck_exporting(
    ex: impl PgExecutor<'_>,
    epoch: i64,
) -> Result<Vec<(i64, Option<String>)>, ControlError> {
    let rows = sqlx::query!(
        r#"
        SELECT reload_id, lease_holder
        FROM walrus.table_reload
        WHERE epoch = $1 AND status = 'exporting'
          AND lease_expiry IS NOT NULL AND lease_expiry < now()
        ORDER BY reload_id
        "#,
        epoch,
    )
    .fetch_all(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(rows
        .into_iter()
        .map(|r| (r.reload_id, r.lease_holder))
        .collect())
}

/// Tables mid-rebuild — the loader-pause predicate's input (PR 6.6).
///
/// Deliberately `flavor = 'reload'` only (a `resync` never pauses anything — H3) and deliberately
/// `requested | exporting` only: the pause MUST lift at `export_complete`, because the rebuild is
/// *triggered by the loader claiming the chunk files* — pausing through `export_complete` would
/// deadlock the reload forever (PR 6.6's gotcha, baked in here so no caller re-derives it).
pub async fn active_rebuilds(
    ex: impl PgExecutor<'_>,
    epoch: i64,
) -> Result<Vec<ReloadRow>, ControlError> {
    sqlx::query_as!(
        ReloadRow,
        r#"
        SELECT reload_id, epoch, source_schema, source_table,
               flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
               chunk_no, cursor_pk,
               first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
               schema_version, restart_count, lease_holder, error
        FROM walrus.table_reload
        WHERE epoch = $1 AND flavor = 'reload' AND status IN ('requested', 'exporting')
        ORDER BY reload_id
        "#,
        epoch,
    )
    .fetch_all(ex)
    .await
    .map_err(ControlError::from_sqlx)
}

/// Read one reload attempt, if it exists.
pub async fn get(
    ex: impl PgExecutor<'_>,
    reload_id: i64,
) -> Result<Option<ReloadRow>, ControlError> {
    sqlx::query_as!(
        ReloadRow,
        r#"
        SELECT reload_id, epoch, source_schema, source_table,
               flavor AS "flavor: ReloadFlavor", status AS "status: ReloadStatus",
               chunk_no, cursor_pk,
               first_lsn AS "first_lsn: Lsn", final_lsn AS "final_lsn: Lsn",
               schema_version, restart_count, lease_holder, error
        FROM walrus.table_reload
        WHERE reload_id = $1
        "#,
        reload_id,
    )
    .fetch_optional(ex)
    .await
    .map_err(ControlError::from_sqlx)
}

#[cfg(test)]
#[path = "reload_test.rs"]
mod tests;
