//! The raw→mirror transform (loader §5–§6) — **the correctness heart of the loader**. One parameterized
//! SQL template ([`transform.sql`](TRANSFORM_SQL)) rendered per table: a dedup window that keeps the
//! latest change per PK (deletes stay *in* the window; the winner's `op` decides — the resurrection
//! guard §5.3), then a three-branch `MERGE INTO` that collapses intra-batch PK churn (`i→d→i`, `i→u→d`,
//! `d→i`, phantom `d`). The same template is used by the hermetic tests here and by Phase B (PR 3.4).

use crate::error::LoaderError;
use common::PgRelation;

/// The transform template (single source of truth). Rendered by [`TransformSql::render`].
pub const TRANSFORM_SQL: &str = include_str!("transform.sql");

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

    /// Render the full rendered SQL (dedup `CREATE TEMP TABLE _batch` + `MERGE INTO`) for this table,
    /// reading only the un-transformed tail (`commit_lsn > after_lsn`) — generated from the **key list**,
    /// so a composite PK expands to `PARTITION BY k1,k2` / `ON t.k1=s.k1 AND t.k2=s.k2`.
    pub fn render(&self, after_lsn: &common::Lsn) -> String {
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
        TRANSFORM_SQL
            .replace("{table}", &self.table)
            .replace("{pk_list}", &pk_list)
            .replace("{pk_join}", &pk_join)
            .replace("{set_cols}", &set_cols)
            .replace("{insert_cols}", &insert_cols)
            .replace("{insert_vals}", &insert_vals)
            .replace("{after_lsn}", &after_lsn.to_string())
    }
}

/// Run the transform (dedup + MERGE) against `<table>_raw`, reading only `commit_lsn > after_lsn`.
/// Phase B (PR 3.4) calls this inside a DuckDB transaction.
pub fn apply_transform(
    conn: &duckdb::Connection,
    t: &TransformSql,
    after_lsn: &common::Lsn,
) -> Result<(), LoaderError> {
    conn.execute_batch(&t.render(after_lsn))
        .map_err(|e| LoaderError::Duck(format!("transform {}: {e}", t.table)))
}
