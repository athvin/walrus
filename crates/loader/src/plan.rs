//! The per-table **schema plan** (loader §2.6 / architecture "Types") — the bridge from the sink's
//! `schema_registry` [`TypeDescriptor`]s to the DuckDB `<table>_raw` (verbatim emit columns) and
//! `<table>` (mirror, recombined to the target type) shapes, and the transform's per-mirror-column value
//! expressions.
//!
//! Three shapes per source column:
//! - **Tier-1** (scalar / native, incl. `uuid`, `numeric`, `timestamptz`, `jsonb`, `bytea`): one emit
//!   column == the source column; the mirror holds it as the descriptor's DuckDB type.
//! - **Tier-2 recombine** (`interval`, `timetz`): several emit columns collapse to ONE mirror column via
//!   a DuckDB expression (`to_months(...)+to_days(...)+to_microseconds(...)`).
//! - **Tier-2 flat** (`range`): several emit columns pass through as several mirror columns (DuckDB has
//!   no range type — it is the 5 flat `_lower/_upper/_lower_inc/_upper_inc/_empty` siblings).
//!
//! A plan built from a bare [`PgRelation`] ([`TablePlan::tier1`]) reproduces the pre-descriptor scalar
//! behaviour exactly (emit == source column via [`crate::duck::duck_type`]), so the hermetic/compose
//! tests that pass a `PgRelation` are unchanged; the registry path ([`TablePlan::from_registry`]) adds
//! the Tier-2 shapes.

use common::{PgRelation, TypeDescriptor};

/// A `<table>_raw` column: the verbatim emit column the sink wrote to Parquet.
#[derive(Debug, Clone)]
pub struct RawCol {
    pub name: String,
    pub duckdb_type: String,
}

/// How a mirror column's value is produced from the winning raw row `s` (and, for a TOAST-resolvable
/// scalar, the current mirror `t`).
#[derive(Debug, Clone)]
pub enum MirrorValue {
    /// `s."<name>"` — a direct copy of the like-named raw column. `toast_resolvable` marks a Tier-1
    /// non-key scalar that may carry the unchanged-TOAST sentinel (resolved by the raw back-scan, §5.6).
    Passthrough { toast_resolvable: bool },
    /// A recombine SQL expression over the raw emit columns (already `s.`-qualified), e.g. an INTERVAL.
    Recombine(String),
}

/// A `<table>` mirror column: name, DuckDB type, key flag, and how its value is computed.
#[derive(Debug, Clone)]
pub struct MirrorCol {
    pub name: String,
    pub duckdb_type: String,
    pub is_key: bool,
    pub value: MirrorValue,
}

/// The full plan for one table: the raw emit columns and the mirror columns.
#[derive(Debug, Clone)]
pub struct TablePlan {
    pub table: String,
    pub raw_cols: Vec<RawCol>,
    pub mirror_cols: Vec<MirrorCol>,
}

impl TablePlan {
    /// The Tier-1 (scalar-only) plan from a bare relation — one emit column == source column via
    /// [`crate::duck::duck_type`], mirror = same. Reproduces the pre-descriptor behaviour exactly.
    pub fn tier1(rel: &PgRelation) -> Self {
        let mut raw_cols = Vec::new();
        let mut mirror_cols = Vec::new();
        for c in &rel.columns {
            let ty = crate::duck::duck_type(c.type_oid).to_string();
            raw_cols.push(RawCol {
                name: c.name.clone(),
                duckdb_type: ty.clone(),
            });
            mirror_cols.push(MirrorCol {
                name: c.name.clone(),
                duckdb_type: ty,
                is_key: c.is_key,
                value: MirrorValue::Passthrough {
                    toast_resolvable: !c.is_key,
                },
            });
        }
        TablePlan {
            table: rel.name.clone(),
            raw_cols,
            mirror_cols,
        }
    }

    /// The full plan from the registry: descriptors (Tier-1/2/3) aligned with the relation's columns (for
    /// the key flags). Falls back to the Tier-1 shape for any column without a descriptor.
    pub fn from_registry(rel: &PgRelation, descriptors: &[TypeDescriptor]) -> Self {
        let by_name: std::collections::HashMap<&str, &TypeDescriptor> =
            descriptors.iter().map(|d| (d.column.as_str(), d)).collect();
        let mut raw_cols = Vec::new();
        let mut mirror_cols = Vec::new();
        for c in &rel.columns {
            match by_name.get(c.name.as_str()) {
                None => {
                    // No descriptor — treat as a Tier-1 scalar.
                    let ty = crate::duck::duck_type(c.type_oid).to_string();
                    raw_cols.push(RawCol {
                        name: c.name.clone(),
                        duckdb_type: ty.clone(),
                    });
                    mirror_cols.push(MirrorCol {
                        name: c.name.clone(),
                        duckdb_type: ty,
                        is_key: c.is_key,
                        value: MirrorValue::Passthrough {
                            toast_resolvable: !c.is_key,
                        },
                    });
                }
                Some(d) => plan_column(
                    c.name.as_str(),
                    c.is_key,
                    d,
                    &mut raw_cols,
                    &mut mirror_cols,
                ),
            }
        }
        TablePlan {
            table: rel.name.clone(),
            raw_cols,
            mirror_cols,
        }
    }
}

/// Plan one column from its descriptor, appending to `raw_cols`/`mirror_cols`.
fn plan_column(
    name: &str,
    is_key: bool,
    d: &TypeDescriptor,
    raw_cols: &mut Vec<RawCol>,
    mirror_cols: &mut Vec<MirrorCol>,
) {
    let emit = parse_emit(&d.emit);
    // Tier-2 that recombines to a single DuckDB scalar (interval / timetz).
    if let Some(expr) = recombine_expr(d.pg_type_oid, &emit) {
        for (n, t) in &emit {
            raw_cols.push(RawCol {
                name: n.clone(),
                duckdb_type: t.clone(),
            });
        }
        mirror_cols.push(MirrorCol {
            name: name.to_string(),
            duckdb_type: d.duckdb.clone(),
            is_key: false, // an interval/timetz is never a replica-identity key
            value: MirrorValue::Recombine(expr),
        });
        return;
    }
    // Tier-1 / Tier-3: a single emit column == the source column, mirror = descriptor's DuckDB type.
    if emit.len() <= 1 {
        // The descriptor `duckdb` is the LOGICAL target (e.g. `UUID`, `TIMESTAMP WITH TIME ZONE`, `VARCHAR`
        // for a Tier-3 jsonb) — read_parquet yields it. The one exception is `numeric`, whose descriptor
        // `duckdb` is the bare `DECIMAL`; the precise `DECIMAL(p,s)` lives in the emit type, so prefer it.
        let ty = match emit.first() {
            Some((_, t)) if t.starts_with("DECIMAL(") => t.clone(),
            _ => d.duckdb.clone(),
        };
        raw_cols.push(RawCol {
            name: name.to_string(),
            duckdb_type: ty.clone(),
        });
        mirror_cols.push(MirrorCol {
            name: name.to_string(),
            duckdb_type: ty,
            is_key,
            value: MirrorValue::Passthrough {
                toast_resolvable: !is_key,
            },
        });
        return;
    }
    // Tier-2 flat (range / geometric): the emit columns pass through as several mirror columns — DuckDB
    // has no range/geo type, so a range IS its 5 flat siblings.
    for (n, t) in &emit {
        raw_cols.push(RawCol {
            name: n.clone(),
            duckdb_type: t.clone(),
        });
        mirror_cols.push(MirrorCol {
            name: n.clone(),
            duckdb_type: t.clone(),
            is_key: false,
            value: MirrorValue::Passthrough {
                toast_resolvable: false,
            },
        });
    }
}

/// Parse the descriptor `emit` list (`"name:ARROW_TYPE"`) into `(name, duckdb_type)` pairs.
fn parse_emit(emit: &[String]) -> Vec<(String, String)> {
    emit.iter()
        .filter_map(|e| {
            let (n, arrow) = e.rsplit_once(':')?;
            Some((n.to_string(), emit_arrow_to_duck(arrow)))
        })
        .collect()
}

/// The loader recombine expression for a type that collapses to one DuckDB scalar — over the winning raw
/// row `s`. `None` for anything that stays flat (Tier-1, range, geometric).
fn recombine_expr(pg_type_oid: u32, emit: &[(String, String)]) -> Option<String> {
    const INTERVAL: u32 = 1186;
    const TIMETZ: u32 = 1266;
    match pg_type_oid {
        INTERVAL if emit.len() == 3 => Some(format!(
            "to_months(s.\"{}\") + to_days(s.\"{}\") + to_microseconds(s.\"{}\")",
            emit[0].0, emit[1].0, emit[2].0
        )),
        TIMETZ if emit.len() == 2 => Some(format!(
            "make_timetz(s.\"{}\", s.\"{}\")",
            emit[0].0, emit[1].0
        )),
        _ => None,
    }
}

/// Map an emit Arrow type name ([`pg_to_arrow`'s `arrow_emit_name`]) to a DuckDB storage type for the
/// raw column. `DECIMAL(p,s)` passes through; `FIXEDBINARY`/`STRUCT`/`LIST` (only ever Tier-1 uuid, which
/// uses the descriptor `duckdb` string instead) fall back to `BLOB`/`VARCHAR`.
fn emit_arrow_to_duck(arrow: &str) -> String {
    match arrow {
        "BOOLEAN" => "BOOLEAN",
        "INT16" => "SMALLINT",
        "INT32" => "INTEGER",
        "INT64" => "BIGINT",
        "FLOAT" => "REAL",
        "DOUBLE" => "DOUBLE",
        "VARCHAR" => "VARCHAR",
        "BLOB" => "BLOB",
        "DATE" => "DATE",
        "TIME" => "TIME",
        "TIMESTAMPTZ" => "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP" => "TIMESTAMP",
        other if other.starts_with("DECIMAL(") => other,
        other if other.starts_with("FIXEDBINARY(") => "BLOB",
        _ => "VARCHAR",
    }
    .to_string()
}
