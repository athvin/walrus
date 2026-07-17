//! The raw→mirror transform (loader §5–§6) — **the correctness heart of the loader**. One parameterized
//! SQL template ([`transform.sql`](TRANSFORM_SQL)) rendered per table: a dedup window that keeps the
//! latest change per PK (deletes stay *in* the window; the winner's `op` decides — the resurrection
//! guard §5.3), then a three-branch `MERGE INTO` that collapses intra-batch PK churn (`i→d→i`, `i→u→d`,
//! `d→i`, phantom `d`). The same template is used by the hermetic tests here and by Phase B (PR 3.4).
//!
//! **⚠ Extends architecture.md (§7, Open Q8/Q13):** the per-PK max-applied-`(commit_lsn, lsn)` guard.
//! Each mutating MERGE branch is gated on `(s.commit_lsn, s.lsn) > (t._applied_commit_lsn, t._applied_lsn)`
//! and the window low bound is relaxed to `>=`, together closing two straddle faces — (A) the
//! equal-`commit_lsn` snapshot row and (B) a stale delete/re-insert across the watermark — while keeping
//! the mirror idempotent (the guard makes a re-applied boundary row a no-op). The full-rebuild (PR 3.11)
//! remains the safety net regardless; this makes the *incremental* path self-correcting.

use crate::error::LoaderError;
use common::{Lsn, PgRelation};

/// The transform template (single source of truth). Rendered by [`TransformSql::render`].
pub const TRANSFORM_SQL: &str = include_str!("../sql/duckdb/templates/transform.sql");

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

/// One mirror column and the SQL producing its value from the winning raw row `s` (and, for a
/// TOAST-resolvable scalar, the current mirror `t`). PR 4.2: a Tier-2 column recombines/flattens from
/// its emit columns; a Tier-1 scalar is the same TOAST-resolved passthrough the transform always used.
struct MirrorCol {
    name: String,
    is_key: bool,
    /// Whether `value_expr` recombines from raw emit columns (Tier-2) — in the full-rebuild the recombine
    /// happens in the raw arm of the union (the mirror baseline can't be decomposed back), then the
    /// column passes through; the incremental path just uses `value_expr` directly over the raw winner.
    is_recombine: bool,
    value_expr: String,
}

/// A table's mirror-column layout for rendering the transform (order preserved). Built from a
/// [`TablePlan`] — the Tier-1 plan reproduces the pre-descriptor scalar SQL exactly.
pub struct TransformSql {
    table: String,
    mirror: Vec<MirrorCol>,
}

impl TransformSql {
    /// Tier-1 (scalar) transform from a bare relation — unchanged from the pre-descriptor path.
    pub fn from_relation(rel: &PgRelation) -> Self {
        Self::from_plan(&crate::plan::TablePlan::tier1(rel))
    }

    /// The full transform from a schema [`TablePlan`]: each mirror column's value is precomputed as SQL
    /// over the winner `s` — a recombine expression (Tier-2), an unchanged-TOAST-resolved back-scan
    /// (Tier-1 non-key, §5.6), or a plain `s."col"` (keys / flat siblings).
    pub fn from_plan(plan: &crate::plan::TablePlan) -> Self {
        use crate::plan::MirrorValue;
        let q = |c: &str| format!("\"{c}\"");
        let table = &plan.table;
        let pk: Vec<&str> = plan
            .mirror_cols
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect();
        let r_pk_eq_s = pk
            .iter()
            .map(|k| format!("r.{} = s.{}", q(k), q(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mirror = plan
            .mirror_cols
            .iter()
            .map(|c| {
                let is_recombine = matches!(c.value, MirrorValue::Recombine(_));
                let value_expr = match &c.value {
                    MirrorValue::Recombine(expr) => expr.clone(),
                    MirrorValue::Passthrough {
                        toast_resolvable: false,
                    } => format!("s.{}", q(&c.name)),
                    MirrorValue::Passthrough {
                        toast_resolvable: true,
                    } => {
                        let qc = q(&c.name);
                        // Membership test on the JSON array text (`["big"]`) — the quotes disambiguate.
                        let listed = |alias: &str| {
                            format!(
                                "COALESCE(json_extract_string({alias}.\"walrus_pg_sink_meta\", '$.unchanged_toast'), '[]') LIKE '%\"{}\"%'",
                                c.name
                            )
                        };
                        format!(
                            "CASE WHEN {winner} THEN COALESCE(( \
                               SELECT r.{qc} FROM \"{table}_raw\" r \
                               WHERE {r_pk_eq_s} AND NOT ({raw}) \
                                 AND (r.\"_walrus_commit_lsn\", r.\"_walrus_lsn\") <= (s.\"_walrus_commit_lsn\", s.\"_walrus_lsn\") \
                               ORDER BY r.\"_walrus_commit_lsn\" DESC, r.\"_walrus_lsn\" DESC LIMIT 1), t.{qc}) \
                             ELSE s.{qc} END",
                            winner = listed("s"),
                            raw = listed("r"),
                        )
                    }
                };
                MirrorCol {
                    name: c.name.clone(),
                    is_key: c.is_key,
                    is_recombine,
                    value_expr,
                }
            })
            .collect();
        TransformSql {
            table: plan.table.clone(),
            mirror,
        }
    }

    fn pk_names(&self) -> Vec<&str> {
        self.mirror
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect()
    }
    fn non_key_names(&self) -> Vec<&str> {
        self.mirror
            .iter()
            .filter(|c| !c.is_key)
            .map(|c| c.name.as_str())
            .collect()
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

    /// Render the full rendered SQL (truncate wipe + dedup `CREATE TEMP TABLE _batch` + guarded `MERGE
    /// INTO`), reading the un-transformed tail (`commit_lsn >= after_lsn` — the `>=` re-examines the
    /// equal-`commit_lsn` snapshot straddle, §7 break face A; and — if the tail has a truncate — only
    /// rows STRICTLY after the `(Ct, Lt)` tuple). Composite-PK-aware.
    pub fn render(&self, after_lsn: &Lsn, boundary: &TruncateBoundary) -> String {
        let q = |c: &str| format!("\"{c}\"");
        let pk = self.pk_names();
        let non_key = self.non_key_names();
        let all: Vec<&str> = self.mirror.iter().map(|c| c.name.as_str()).collect();
        let pk_list = pk.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        let pk_join = pk
            .iter()
            .map(|c| format!("t.{} = s.{}", q(c), q(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        // MATCHED UPDATE assigns the non-key mirror columns; if a table is all-PK, a self-assignment keeps
        // the UPDATE valid (a no-op) so `d→i` still lands via the MATCHED branch. Every UPDATE also stamps
        // the hidden `_applied_*` guard columns with the winner's tuple (§7).
        let mut set_parts: Vec<String> = if non_key.is_empty() {
            vec![format!("{} = s.{}", q(pk[0]), q(pk[0]))]
        } else {
            non_key
                .iter()
                .map(|c| format!("{} = s.{}", q(c), q(c)))
                .collect()
        };
        set_parts.push("\"_applied_commit_lsn\" = s.\"_walrus_commit_lsn\"".into());
        set_parts.push("\"_applied_lsn\" = s.\"_walrus_lsn\"".into());
        let set_cols = set_parts.join(", ");
        // INSERT carries the mirror columns PLUS the hidden guard columns seeded from the winner's tuple.
        let mut insert_col_parts: Vec<String> = all.iter().map(|c| q(c)).collect();
        insert_col_parts.push("\"_applied_commit_lsn\"".into());
        insert_col_parts.push("\"_applied_lsn\"".into());
        let insert_cols = insert_col_parts.join(", ");
        let mut insert_val_parts: Vec<String> = all.iter().map(|c| format!("s.{}", q(c))).collect();
        insert_val_parts.push("s.\"_walrus_commit_lsn\"".into());
        insert_val_parts.push("s.\"_walrus_lsn\"".into());
        let insert_vals = insert_val_parts.join(", ");
        // The per-PK max-applied guard (§7, ⚠ extends architecture.md): a MUTATING branch fires only when
        // the winner's tuple is STRICTLY newer than what last shaped the mirror row. Row-value `>` is
        // lexicographic in DuckDB — exactly the `(commit_lsn, lsn)` order; do NOT hand-decompose it.
        let guard = "(s.\"_walrus_commit_lsn\", s.\"_walrus_lsn\") > \
                     (t.\"_applied_commit_lsn\", t.\"_applied_lsn\")";

        // `_batch`'s SELECT list: each mirror column's precomputed value expression over the raw winner
        // `s` (a Tier-2 recombine or a Tier-1 TOAST-resolved passthrough), then `_walrus_op` for the MERGE
        // branches and `_walrus_commit_lsn`/`_walrus_lsn` for the guard comparison and `_applied_*` stamps.
        let mut select_parts: Vec<String> = self
            .mirror
            .iter()
            .map(|c| format!("{} AS {}", c.value_expr, q(&c.name)))
            .collect();
        select_parts.push("s.\"_walrus_op\"".to_string());
        select_parts.push("s.\"_walrus_commit_lsn\"".to_string());
        select_parts.push("s.\"_walrus_lsn\"".to_string());
        let resolved_select = select_parts.join(", ");
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
            .replace("{resolved_select}", &resolved_select)
            .replace("{guard}", guard)
    }

    /// The table name (for compaction / prune SQL that lives outside the template).
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Render the atomic full-rebuild (PR 3.11): `CREATE OR REPLACE TABLE <table>` over **retained raw ∪
    /// the current mirror injected as an LSN-floor baseline**, reusing the same dedup/collapse (TRUNCATE
    /// tuple boundary, TOAST resolution, `(commit_lsn, lsn)` ranking) as the incremental path — dropping
    /// `op='d'` winners. The mirror baseline (each row tagged at its own `_applied_*` tuple, so real newer
    /// raw out-ranks it) guarantees a PK whose raw evidence was already pruned still contributes its
    /// current value. Staged through a TEMP table so the statement never reads and replaces `<table>` at
    /// once; the swap + view recreate run inside one transaction ([`crate::compaction::full_rebuild`]).
    ///
    /// PR 4.2: the union is over the MIRROR columns (not the raw emit columns), so a Tier-2 value's
    /// recombine happens in the raw arm (the mirror baseline can't be decomposed back into emit columns);
    /// the final resolve then only TOAST-resolves the Tier-1 columns and passes the recombined Tier-2 ones
    /// through.
    pub fn render_rebuild(&self, boundary: &TruncateBoundary) -> String {
        let q = |c: &str| format!("\"{c}\"");
        let t = &self.table;
        let pk = self.pk_names();
        let pk_list = pk.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        let pk_join = pk
            .iter()
            .map(|c| format!("t.{} = s.{}", q(c), q(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        // The raw arm collapses each mirror column FROM the raw emit columns (aliased `s`): a Tier-2
        // recombine, or a plain `s."col"` for Tier-1 (its TOAST resolution happens in the final SELECT).
        let raw_exprs: Vec<String> = self
            .mirror
            .iter()
            .map(|c| {
                let e = if c.is_recombine {
                    c.value_expr.clone()
                } else {
                    format!("s.{}", q(&c.name))
                };
                format!("{e} AS {}", q(&c.name))
            })
            .collect();
        let mirror_names = self
            .mirror
            .iter()
            .map(|c| q(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        // The union feeding the dedup: every retained raw change (op<>'t'), collapsed to mirror columns,
        // plus the current mirror as a baseline row per PK, tagged at that PK's `_applied_*` tuple with an
        // empty unchanged_toast meta. Both arms carry the same mirror-column names → `UNION ALL BY NAME`.
        let src = format!(
            "SELECT {raw}, s.\"walrus_pg_sink_meta\" AS \"walrus_pg_sink_meta\", \
                 s.\"_walrus_op\" AS \"_walrus_op\", s.\"_walrus_commit_lsn\" AS \"_walrus_commit_lsn\", \
                 s.\"_walrus_lsn\" AS \"_walrus_lsn\" FROM \"{t}_raw\" s WHERE s.\"_walrus_op\" <> 't' \
             UNION ALL BY NAME \
             SELECT {mirror_names}, '{{}}' AS \"walrus_pg_sink_meta\", 'i' AS \"_walrus_op\", \
                 \"_applied_commit_lsn\" AS \"_walrus_commit_lsn\", \
                 \"_applied_lsn\" AS \"_walrus_lsn\" FROM \"{t}\"",
            raw = raw_exprs.join(", "),
        );
        // The truncate tuple boundary applies to the union (the mirror baseline is post-truncate by
        // construction, so it survives); empty when the retained tail holds no truncate.
        let truncate_bound = match (boundary.ct, boundary.lt) {
            (Some(ct), Some(lt)) => {
                format!(" WHERE (\"_walrus_commit_lsn\", \"_walrus_lsn\") > ('{ct}', '{lt}')")
            }
            _ => String::new(),
        };
        // The rebuilt row list: Tier-1 columns TOAST-resolve (over `s` = the collapsed winner + the raw
        // back-scan + the mirror `t` fallback); Tier-2 (recombined) columns pass through `s`. Then the
        // `_applied_*` stamps re-seeded from the winner's tuple so the incremental guard keeps working.
        let mut cols: Vec<String> = self
            .mirror
            .iter()
            .map(|c| {
                let e = if c.is_recombine {
                    format!("s.{}", q(&c.name))
                } else {
                    c.value_expr.clone()
                };
                format!("{e} AS {}", q(&c.name))
            })
            .collect();
        cols.push("s.\"_walrus_commit_lsn\" AS \"_applied_commit_lsn\"".to_string());
        cols.push("s.\"_walrus_lsn\" AS \"_applied_lsn\"".to_string());
        let resolved = cols.join(", ");
        format!(
            "CREATE OR REPLACE TEMP TABLE \"_walrus_rebuild_{t}\" AS \
             WITH src AS ({src}), \
             winners AS (SELECT * FROM src{truncate_bound} \
                 QUALIFY row_number() OVER (PARTITION BY {pk_list} \
                     ORDER BY \"_walrus_commit_lsn\" DESC, \"_walrus_lsn\" DESC) = 1) \
             SELECT {resolved} FROM winners s LEFT JOIN \"{t}\" t ON {pk_join} \
             WHERE s.\"_walrus_op\" <> 'd'; \
             DROP VIEW IF EXISTS \"{t}_current\"; \
             CREATE OR REPLACE TABLE \"{t}\" AS SELECT * FROM \"_walrus_rebuild_{t}\"; \
             DROP TABLE \"_walrus_rebuild_{t}\"; \
             {view}",
            view = crate::duck::user_view_sql(t),
        )
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
