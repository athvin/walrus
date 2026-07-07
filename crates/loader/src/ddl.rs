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
//! This PR handles the **additive / lossless** classes only: `ADD COLUMN`, `RENAME COLUMN` /
//! `RENAME TABLE`, a lossless/widening `ALTER COLUMN TYPE`, and `COMMENT` (metadata — mirror only, and
//! it does **not** cut a `schema_version` boundary). **Destructive** DDL (`DROP COLUMN`, lossy
//! `ALTER TYPE`, `DROP TABLE`) is deferred to PR 3.9 — a lossy/narrowing change here is an explicit
//! error with a `→ PR 3.9` marker, never a silent wrong cast.

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

/// Diff `old → new` by POSITION (attnum): rename (same position, changed name), widen (same position,
/// changed DuckDB type), append (trailing new columns), and a table rename. Order is
/// rename-before-widen so a combined rename+widen at one position lands correctly.
///
/// A **destructive** shape (fewer columns) or a **lossy** type change is an error with a `→ PR 3.9`
/// marker — this PR never silently applies a narrowing cast.
pub fn diff_additive(
    old: &SchemaVersion,
    new: &SchemaVersion,
) -> Result<Vec<AdditiveChange>, LoaderError> {
    let mut changes = Vec::new();
    if old.relation.name != new.relation.name {
        changes.push(AdditiveChange::RenameTable {
            from: old.relation.name.clone(),
            to: new.relation.name.clone(),
        });
    }
    let (oc, nc) = (&old.relation.columns, &new.relation.columns);
    if nc.len() < oc.len() {
        return Err(LoaderError::Internal(format!(
            "destructive column removal ({}→{} cols) on {} → PR 3.9",
            oc.len(),
            nc.len(),
            new.relation.name
        )));
    }
    for (i, (o, n)) in oc.iter().zip(nc.iter()).enumerate() {
        if o.name != n.name {
            changes.push(AdditiveChange::RenameColumn {
                position: i,
                from: o.name.clone(),
                to: n.name.clone(),
            });
        }
        if !same_duck_type(o, n) {
            if is_lossless_widen(o, n) {
                changes.push(AdditiveChange::WidenColumn {
                    position: i,
                    name: n.name.clone(),
                    new: n.clone(),
                });
            } else {
                // TODO(3.9): lossy/narrowing cast → quarantine (never a silent lossy in-place cast).
                return Err(LoaderError::Internal(format!(
                    "lossy type change on {} (oid {}→{}) → PR 3.9",
                    n.name, o.type_oid, n.type_oid
                )));
            }
        }
    }
    for n in &nc[oc.len()..] {
        changes.push(AdditiveChange::AddColumn(n.clone()));
    }
    Ok(changes)
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
            let changes = diff_additive(&old, &new)?;
            apply_additive(db.conn(), table, &changes)?;
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
