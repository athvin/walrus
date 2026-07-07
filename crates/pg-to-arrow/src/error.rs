//! `pg-to-arrow` error taxonomy.

/// Everything that can go wrong mapping a Postgres relation to Arrow.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The column's type is real but not handled at this tier yet — Tier-2 (interval/timetz/range/
    /// geometric) and Tier-3 (VARCHAR carriers) land in later PRs. We fail loudly rather than emit a
    /// wrong-but-compiling field, which is exactly the bug the PR 2.11 conformance tests exist to catch.
    #[error("type oid {oid} (typmod {typmod}) is not a Tier-1 type")]
    NotTier1 { oid: u32, typmod: i32 },
    #[error("relation {relation} has no columns")]
    EmptyRelation { relation: String },
    #[error("column {column}: cannot parse {value:?} as {data_type}")]
    ValueParse {
        column: String,
        value: String,
        data_type: String,
    },
    #[error("row has {got} values, relation has {expected} columns")]
    RowLenMismatch { expected: usize, got: usize },
    #[error("internal: builder downcast failed for column {column}")]
    Downcast { column: String },
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}
