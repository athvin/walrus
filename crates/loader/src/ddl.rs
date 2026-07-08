//! Additive / lossless DDL apply (loader §5.7, architecture "per-change-type handling") — the loader's
//! schema-evolution half. **Schema-DIFF, never `c_ddl_text` replay:** each additive change is derived
//! from the `new − old` column sets in `schema_registry`, and columns are matched by **position
//! (attnum)**, never by name — a `RENAME a → b` followed by `ADD COLUMN a` must read as "position 0
//! renamed, a new column appended", which name-matching would silently corrupt.
//!
//! The **homogeneous-file rule** (one `schema_version` per Parquet file) lets the loader gate cleanly:
//! before it appends/transforms any file at version V it reconciles both DuckDB tables *up to* V, so no
//! file ever straddles a structural boundary. The mirror `<table>` is kept at the **exact** current
//! source shape; `<table>_raw` is an **additive superset** (columns only ever added / widened / renamed —
//! never dropped here), so old verbatim rows stay valid (a new column reads NULL for them).
//!
//! **Additive / lossless** (PR 3.8): `ADD COLUMN`, `RENAME COLUMN` / `RENAME TABLE`, a lossless/widening
//! `ALTER COLUMN TYPE`, and `COMMENT` (metadata — mirror only, does **not** cut a `schema_version`
//! boundary). **Destructive** (PR 3.9), where mirror and raw diverge most: `DROP COLUMN` (physical on
//! the mirror, retained-nullable on raw), a **lossy** `ALTER COLUMN TYPE` (attempt the in-place mirror
//! cast → on failure **quarantine + alert + stop**, an accepted terminal v1 outcome; raw is widened to
//! `VARCHAR`, never re-cast), and `DROP TABLE` (retire both tables + file). Raw is an additive superset:
//! it only ever adds / widens — it never destructively drops or re-casts history.

use crate::duck::{duck_type, user_view_sql, TableDb};
use crate::error::LoaderError;
use common::{PgColumn, PgRelation};

/// One `schema_version` of a table's shape — the `schema_registry` `columns` snapshot for that version.
pub struct SchemaVersion {
    pub version: i64,
    pub relation: PgRelation,
}

/// What a `COMMENT` targets. `COMMENT` is metadata: mirror only, never a data gate.
pub enum CommentTarget {
    Table,
    Column(String),
}

/// One additive/lossless structural (or metadata) change, derived by [`diff_additive`].
pub enum AdditiveChange {
    /// A column appended at the end (a higher attnum). Nullable on BOTH tables so pre-change rows read
    /// NULL and old verbatim `<table>_raw` rows stay valid.
    AddColumn(PgColumn),
    /// A position-tracked rename applied to both tables (never a drop+add).
    RenameColumn {
        position: usize,
        from: String,
        to: String,
    },
    /// A source `RENAME TABLE` — the mirror, its CDC log, and the user view all follow.
    RenameTable { from: String, to: String },
    /// A lossless/widening `ALTER COLUMN TYPE` (e.g. int4→int8) — an in-place DuckDB cast on both tables.
    WidenColumn {
        position: usize,
        name: String,
        new: PgColumn,
    },
    /// A `COMMENT ON` — a metadata revision mirrored onto `<table>` only; it does not gate data.
    Comment {
        target: CommentTarget,
        text: Option<String>,
    },
}

/// One destructive change (PR 3.9) — where mirror and raw **diverge**: the mirror follows the exact
/// current shape (physical drop / in-place cast), the raw log preserves history (retain / widen-only).
pub enum DestructiveChange {
    /// `DROP COLUMN` — physically dropped from `<table>`; **retained nullable** in `<table>_raw`.
    DropColumn { name: String },
    /// A lossy/incompatible `ALTER COLUMN TYPE` — attempt the in-place mirror cast (→ quarantine on
    /// failure); widen `<table>_raw` to `VARCHAR` (never re-cast historical values).
    LossyType { name: String, new: PgColumn },
    /// `DROP TABLE` — retire both DuckDB tables (the `.duckdb` file is retired by the caller).
    DropTable { name: String },
}

/// Whether the two columns map to the SAME DuckDB type (the only thing an `ALTER COLUMN TYPE` would
/// change) — a typmod-only tweak (e.g. `varchar(10)→varchar(20)`, both DuckDB `VARCHAR`) is a no-op.
fn same_duck_type(a: &PgColumn, b: &PgColumn) -> bool {
    duck_type(a.type_oid) == duck_type(b.type_oid)
}

/// A widening DuckDB *can* do in place without loss — the additive subset. Anything else whose DuckDB
/// type changes is **lossy/narrowing** and belongs to PR 3.9's quarantine path.
fn is_lossless_widen(old: &PgColumn, new: &PgColumn) -> bool {
    matches!(
        (old.type_oid, new.type_oid),
        (21, 23) | (21, 20) | (23, 20) // int2→int4→int8
            | (700, 701) // float4→float8
    )
}

/// The full classification of one version step: additive/lossless changes plus destructive ones
/// (PR 3.9). The sink cuts one file per structural change, so a step usually yields a single change.
#[derive(Default)]
pub struct SchemaDiff {
    pub additive: Vec<AdditiveChange>,
    pub destructive: Vec<DestructiveChange>,
}

/// Diff `old → new` by POSITION (attnum), classifying each change as additive or **destructive**
/// (PR 3.9). The homogeneous-file rule means one DDL per version, so a length change is unambiguous:
/// a drop shrinks the column count (never a rename, which keeps it), and an empty new column set is a
/// `DROP TABLE`. A type change is a lossless widen (additive) or a lossy/narrowing one (destructive).
pub fn diff(old: &SchemaVersion, new: &SchemaVersion) -> Result<SchemaDiff, LoaderError> {
    let mut d = SchemaDiff::default();

    // DROP TABLE: the registered version carries no columns (the sink's `sql_drop` sentinel).
    if new.relation.columns.is_empty() && !old.relation.columns.is_empty() {
        d.destructive.push(DestructiveChange::DropTable {
            name: old.relation.name.clone(),
        });
        return Ok(d);
    }

    if old.relation.name != new.relation.name {
        d.additive.push(AdditiveChange::RenameTable {
            from: old.relation.name.clone(),
            to: new.relation.name.clone(),
        });
    }

    let (oc, nc) = (&old.relation.columns, &new.relation.columns);
    if nc.len() < oc.len() {
        // DROP COLUMN(s): a length decrease is a drop (a rename keeps the count). Identify the dropped
        // columns by name-set difference — the surviving columns keep their names.
        let kept: std::collections::HashSet<&str> = nc.iter().map(|c| c.name.as_str()).collect();
        for o in oc.iter().filter(|o| !kept.contains(o.name.as_str())) {
            d.destructive.push(DestructiveChange::DropColumn {
                name: o.name.clone(),
            });
        }
        return Ok(d);
    }

    // len(new) >= len(old): position-matched rename / type-change over the common prefix, then any
    // trailing appended columns are ADDs.
    for (i, (o, n)) in oc.iter().zip(nc.iter()).enumerate() {
        if o.name != n.name {
            d.additive.push(AdditiveChange::RenameColumn {
                position: i,
                from: o.name.clone(),
                to: n.name.clone(),
            });
        }
        if !same_duck_type(o, n) {
            if is_lossless_widen(o, n) {
                d.additive.push(AdditiveChange::WidenColumn {
                    position: i,
                    name: n.name.clone(),
                    new: n.clone(),
                });
            } else {
                d.destructive.push(DestructiveChange::LossyType {
                    name: n.name.clone(),
                    new: n.clone(),
                });
            }
        }
    }
    for n in &nc[oc.len()..] {
        d.additive.push(AdditiveChange::AddColumn(n.clone()));
    }
    Ok(d)
}

/// The additive-only view of [`diff`] — errors if the step is destructive (use [`diff`] +
/// [`apply_destructive`] for those, PR 3.9). Kept so the additive path stays a total function.
pub fn diff_additive(
    old: &SchemaVersion,
    new: &SchemaVersion,
) -> Result<Vec<AdditiveChange>, LoaderError> {
    let d = diff(old, new)?;
    if !d.destructive.is_empty() {
        return Err(LoaderError::Internal(format!(
            "destructive change on {} — not an additive diff (PR 3.9)",
            new.relation.name
        )));
    }
    Ok(d.additive)
}

/// Apply the derived changes to the DuckDB tables per the taxonomy: mirror = exact shape, `<table>_raw`
/// = additive superset (nullable adds), `COMMENT` = mirror only. A `SELECT *` view binds its columns at
/// creation, so the user view is recreated after any structural change.
pub fn apply_additive(
    conn: &duckdb::Connection,
    table: &str,
    changes: &[AdditiveChange],
) -> Result<(), LoaderError> {
    let mut sql = String::new();
    let mut cur = table.to_string();
    let mut structural = false;
    for ch in changes {
        match ch {
            AdditiveChange::AddColumn(c) => {
                let ty = duck_type(c.type_oid);
                let name = &c.name;
                // Nullable on both (no NOT NULL): pre-change rows read NULL; old raw rows stay valid.
                sql.push_str(&format!(
                    "ALTER TABLE \"{cur}\" ADD COLUMN IF NOT EXISTS \"{name}\" {ty}; \
                     ALTER TABLE \"{cur}_raw\" ADD COLUMN IF NOT EXISTS \"{name}\" {ty};"
                ));
                structural = true;
            }
            AdditiveChange::RenameColumn { from, to, .. } => {
                sql.push_str(&format!(
                    "ALTER TABLE \"{cur}\" RENAME COLUMN \"{from}\" TO \"{to}\"; \
                     ALTER TABLE \"{cur}_raw\" RENAME COLUMN \"{from}\" TO \"{to}\";"
                ));
                structural = true;
            }
            AdditiveChange::WidenColumn { name, new, .. } => {
                let ty = duck_type(new.type_oid);
                sql.push_str(&format!(
                    "ALTER TABLE \"{cur}\" ALTER COLUMN \"{name}\" TYPE {ty}; \
                     ALTER TABLE \"{cur}_raw\" ALTER COLUMN \"{name}\" TYPE {ty};"
                ));
                structural = true;
            }
            AdditiveChange::RenameTable { from, to } => {
                sql.push_str(&format!(
                    "ALTER TABLE \"{cur}\" RENAME TO \"{to}\"; \
                     ALTER TABLE \"{cur}_raw\" RENAME TO \"{to}_raw\"; \
                     DROP VIEW IF EXISTS \"{from}_current\";"
                ));
                cur = to.clone();
                structural = true;
            }
            AdditiveChange::Comment { target, text } => {
                // Metadata only — mirror `<table>` never `<table>_raw`; does NOT set `structural`, so it
                // neither recreates the view nor implies a data gate.
                let lit = match text {
                    Some(t) => format!("'{}'", t.replace('\'', "''")),
                    None => "NULL".to_string(),
                };
                match target {
                    CommentTarget::Table => {
                        sql.push_str(&format!("COMMENT ON TABLE \"{cur}\" IS {lit};"))
                    }
                    CommentTarget::Column(col) => {
                        sql.push_str(&format!("COMMENT ON COLUMN \"{cur}\".\"{col}\" IS {lit};"))
                    }
                }
            }
        }
    }
    if structural {
        sql.push_str(&user_view_sql(&cur));
    }
    if sql.is_empty() {
        return Ok(());
    }
    conn.execute_batch(&sql)
        .map_err(|e| LoaderError::Duck(format!("apply additive DDL to {table}: {e}")))
}

/// Apply destructive changes (PR 3.9) — the mirror-vs-raw asymmetry is the whole point: the mirror
/// takes the exact current shape (physical drop / in-place cast), the raw log preserves history
/// (retain / widen-to-VARCHAR, **never** a re-cast that could fail on stored values). A lossy cast that
/// fails on the mirror returns [`LoaderError::Quarantine`] — a terminal, alerting v1 outcome (single-
/// table reload out of quarantine is **out of scope in v1**). `DROP TABLE` retires both DuckDB tables
/// idempotently (`IF EXISTS`); the `.duckdb` file is retired separately by [`retire_file`].
pub fn apply_destructive(
    conn: &duckdb::Connection,
    table: &str,
    changes: &[DestructiveChange],
) -> Result<(), LoaderError> {
    for ch in changes {
        match ch {
            DestructiveChange::DropColumn { name } => {
                // Mirror: physical drop. Raw: RETAIN the column (already nullable) — verbatim history; a
                // post-drop file simply NULL-fills it (name-explicit append). Recreate the view.
                let sql = format!(
                    "ALTER TABLE \"{table}\" DROP COLUMN IF EXISTS \"{name}\"; {}",
                    user_view_sql(table)
                );
                conn.execute_batch(&sql).map_err(|e| {
                    LoaderError::Duck(format!("drop column {name} on {table}: {e}"))
                })?;
            }
            DestructiveChange::LossyType { name, new } => {
                let ty = duck_type(new.type_oid);
                // Raw FIRST: widen to VARCHAR so rows of BOTH schema_versions coexist in one column;
                // never issue a CAST that could fail on historical values. Idempotent (already VARCHAR).
                conn.execute_batch(&format!(
                    "ALTER TABLE \"{table}_raw\" ALTER COLUMN \"{name}\" TYPE VARCHAR;"
                ))
                .map_err(|e| LoaderError::Duck(format!("widen raw {name} on {table}: {e}")))?;
                // Mirror: attempt the in-place cast. DuckDB validates before applying, so a failure
                // leaves the mirror unchanged → QUARANTINE (loud, terminal). Never silent data loss.
                if let Err(e) = conn.execute_batch(&format!(
                    "ALTER TABLE \"{table}\" ALTER COLUMN \"{name}\" TYPE {ty};"
                )) {
                    return Err(LoaderError::Quarantine {
                        table: table.to_string(),
                        reason: format!("lossy ALTER COLUMN {name} TYPE {ty} failed: {e}"),
                    });
                }
            }
            DestructiveChange::DropTable { name } => {
                conn.execute_batch(&format!(
                    "DROP VIEW IF EXISTS \"{name}_current\"; \
                     DROP TABLE IF EXISTS \"{name}\"; DROP TABLE IF EXISTS \"{name}_raw\";"
                ))
                .map_err(|e| LoaderError::Duck(format!("drop table {name}: {e}")))?;
            }
        }
    }
    Ok(())
}

/// Retire a dropped table's `.duckdb` file (call after its owning connection is closed). Idempotent —
/// a missing file (a crash mid-retire re-run) is success.
pub fn retire_file(path: &std::path::Path) -> Result<(), LoaderError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(LoaderError::Internal(format!(
            "retire {}: {e}",
            path.display()
        ))),
    }
}

/// Bring both DuckDB tables up to `target` by applying each version step's additive diff, advancing the
/// `_walrus_meta` watermark after each — **before** any file at that version is appended/transformed.
/// Idempotent: the watermark is persisted in the `.duckdb`, so a re-run resumes from where it left off.
pub async fn reconcile_to_version(
    db: &TableDb,
    pool: &sqlx::PgPool,
    epoch: i64,
    schema: &str,
    table: &str,
    target: i64,
) -> Result<(), LoaderError> {
    let mut cur = db.schema_version()?;
    while cur < target {
        let next = cur + 1;
        // A version with no registry pair to diff (e.g. a metadata-only revision that did not persist a
        // new `columns` snapshot) applies nothing structural — we still advance the watermark below.
        if let (Some(old), Some(new)) = (
            load_version(pool, epoch, schema, table, cur).await?,
            load_version(pool, epoch, schema, table, next).await?,
        ) {
            let d = diff(&old, &new)?;
            apply_additive(db.conn(), table, &d.additive)?;
            // Destructive changes (PR 3.9) apply after additive ones; a lossy cast failure short-circuits
            // with `Quarantine` and the watermark is NOT advanced (re-run re-quarantines idempotently).
            apply_destructive(db.conn(), table, &d.destructive)?;
        }
        db.set_schema_version(next)?;
        cur = next;
    }
    Ok(())
}

/// Load one `schema_version`'s relation from `schema_registry` (`None` if that version has no row).
async fn load_version(
    pool: &sqlx::PgPool,
    epoch: i64,
    schema: &str,
    table: &str,
    version: i64,
) -> Result<Option<SchemaVersion>, LoaderError> {
    let Some(row) = control::read_registry(pool, epoch, schema, table, version).await? else {
        return Ok(None);
    };
    let relation: PgRelation = serde_json::from_value(row.columns)
        .map_err(|e| LoaderError::Internal(format!("decode registry v{version} columns: {e}")))?;
    Ok(Some(SchemaVersion { version, relation }))
}
