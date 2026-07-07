//! The raw→mirror transform (loader §5–§6) — **the correctness heart of the loader**. One parameterized
//! SQL template ([`transform.sql`](TRANSFORM_SQL)) rendered per table: a dedup window that keeps the
//! latest change per PK (deletes stay *in* the window; the winner's `op` decides — the resurrection
//! guard §5.3), then a three-branch `MERGE INTO` that collapses intra-batch PK churn (`i→d→i`, `i→u→d`,
//! `d→i`, phantom `d`). The same template is used by the hermetic tests here and by Phase B (PR 3.4).

use crate::error::LoaderError;
use common::{Lsn, PgRelation};

/// The transform template (single source of truth). Rendered by [`TransformSql::render`].
pub const TRANSFORM_SQL: &str = include_str!("transform.sql");

/// The latest `TRUNCATE` tuple `(Ct, Lt)` in the un-transformed tail — `(None, None)` if there is none.
/// The wipe boundary is the **tuple**, never the scalar `commit_lsn`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TruncateBoundary {
    pub ct: Option<Lsn>,
    pub lt: Option<Lsn>,
}

impl TruncateBoundary {
    pub fn none() -> Self {
        TruncateBoundary::default()
    }
    pub fn is_some(&self) -> bool {
        self.ct.is_some()
    }
}

/// A table's column layout for rendering the transform. `pk`/`non_key`/`all` preserve source order.
pub struct TransformSql {
    table: String,
    pk: Vec<String>,
    non_key: Vec<String>,
    all: Vec<String>,
}

impl TransformSql {
    pub fn from_relation(rel: &PgRelation) -> Self {
        let pk: Vec<String> = rel
            .columns
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.clone())
            .collect();
        let non_key: Vec<String> = rel
            .columns
            .iter()
            .filter(|c| !c.is_key)
            .map(|c| c.name.clone())
            .collect();
        let all: Vec<String> = rel.columns.iter().map(|c| c.name.clone()).collect();
        TransformSql {
            table: rel.name.clone(),
            pk,
            non_key,
            all,
        }
    }

    /// The latest `TRUNCATE` `(Ct, Lt)` in the tail (`op='t'`, `commit_lsn > after_lsn`), ordered by the
    /// tuple. `(None, None)` if the tail holds no truncate — every downstream predicate is NULL-safe.
    pub fn latest_truncate(
        &self,
        conn: &duckdb::Connection,
        after_lsn: &Lsn,
    ) -> Result<TruncateBoundary, LoaderError> {
        let sql = format!(
            "SELECT \"_walrus_commit_lsn\", \"_walrus_lsn\" FROM \"{}_raw\" \
             WHERE \"_walrus_op\" = 't' AND \"_walrus_commit_lsn\" > '{}' \
             ORDER BY \"_walrus_commit_lsn\" DESC, \"_walrus_lsn\" DESC LIMIT 1",
            self.table, after_lsn
        );
        let row: Option<(String, String)> = conn
            .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .ok(); // no rows → no truncate
        match row {
            None => Ok(TruncateBoundary::none()),
            Some((ct, lt)) => Ok(TruncateBoundary {
                ct: Some(
                    ct.parse()
                        .map_err(|e| LoaderError::Internal(format!("parse Ct {ct:?}: {e:?}")))?,
                ),
                lt: Some(
                    lt.parse()
                        .map_err(|e| LoaderError::Internal(format!("parse Lt {lt:?}: {e:?}")))?,
                ),
            }),
        }
    }

    /// Render the full rendered SQL (truncate wipe + dedup `CREATE TEMP TABLE _batch` + `MERGE INTO`),
    /// reading only the un-transformed tail (`commit_lsn > after_lsn`, and — if the tail has a truncate —
    /// only rows STRICTLY after the `(Ct, Lt)` tuple). Composite-PK-aware.
    pub fn render(&self, after_lsn: &Lsn, boundary: &TruncateBoundary) -> String {
        let q = |c: &str| format!("\"{c}\"");
        let pk_list = self.pk.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        let pk_join = self
            .pk
            .iter()
            .map(|c| format!("t.{} = s.{}", q(c), q(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        // MATCHED UPDATE assigns the non-key columns; if a table is all-PK, a self-assignment keeps the
        // UPDATE valid (a no-op) so `d→i` still lands via the MATCHED branch.
        let set_cols = if self.non_key.is_empty() {
            format!("{} = s.{}", q(&self.pk[0]), q(&self.pk[0]))
        } else {
            self.non_key
                .iter()
                .map(|c| format!("{} = s.{}", q(c), q(c)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let insert_cols = self.all.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        let insert_vals = self
            .all
            .iter()
            .map(|c| format!("s.{}", q(c)))
            .collect::<Vec<_>>()
            .join(", ");
        // The truncate wipe (whole mirror) + the tuple-boundary window filter — empty when no truncate.
        let (truncate_wipe, truncate_bound) = match (boundary.ct, boundary.lt) {
            (Some(ct), Some(lt)) => (
                format!("DELETE FROM \"{}\";", self.table),
                format!(" AND (\"_walrus_commit_lsn\", \"_walrus_lsn\") > ('{ct}', '{lt}')"),
            ),
            _ => (String::new(), String::new()),
        };
        TRANSFORM_SQL
            .replace("{table}", &self.table)
            .replace("{pk_list}", &pk_list)
            .replace("{pk_join}", &pk_join)
            .replace("{set_cols}", &set_cols)
            .replace("{insert_cols}", &insert_cols)
            .replace("{insert_vals}", &insert_vals)
            .replace("{after_lsn}", &after_lsn.to_string())
            .replace("{truncate_wipe}", &truncate_wipe)
            .replace("{truncate_bound}", &truncate_bound)
    }
}

/// Run the transform against `<table>_raw`, reading only `commit_lsn > after_lsn`: resolve the latest
/// truncate `(Ct, Lt)`, wipe the mirror if present, then dedup + MERGE the post-boundary tail. Phase B
/// (PR 3.4) calls this inside a DuckDB transaction so the wipe + repopulation are atomic.
pub fn apply_transform(
    conn: &duckdb::Connection,
    t: &TransformSql,
    after_lsn: &Lsn,
) -> Result<(), LoaderError> {
    let boundary = t.latest_truncate(conn, after_lsn)?;
    conn.execute_batch(&t.render(after_lsn, &boundary))
        .map_err(|e| LoaderError::Duck(format!("transform {}: {e}", t.table)))
}
