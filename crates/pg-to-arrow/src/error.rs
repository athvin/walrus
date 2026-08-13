//! `pg-to-arrow` error taxonomy.

/// Cold parse-error detail boxed inside [`Error`] so successful per-cell conversions stay compact.
#[derive(Debug)]
pub struct ValueParseDetail {
    pub column: String,
    pub value: String,
    pub data_type: String,
}

/// Everything that can go wrong mapping a Postgres relation to Arrow.
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The column's type is real but not handled at this tier yet — Tier-2 (interval/timetz/range/
    /// geometric) and Tier-3 (VARCHAR carriers) land in later PRs. We fail loudly rather than emit a
    /// wrong-but-compiling field, which is exactly the bug the PR 2.11 conformance tests exist to catch.
    #[error("type oid {oid} (typmod {typmod}) is not a Tier-1 type")]
    NotTier1 { oid: u32, typmod: i32 },
    #[error("relation {relation} has no columns")]
    EmptyRelation { relation: String },
    #[error("column {}: cannot parse {:?} as {}", .0.column, .0.value, .0.data_type)]
    ValueParse(Box<ValueParseDetail>),
    #[error("row has {got} values, relation has {expected} columns")]
    RowLenMismatch { expected: usize, got: usize },
    #[error("internal: builder downcast failed for column {column}")]
    Downcast { column: String },
    #[error("arrow error: {0}")]
    Arrow(#[source] Box<arrow::error::ArrowError>),
    #[error("parquet error: {0}")]
    Parquet(#[source] Box<parquet::errors::ParquetError>),
}

impl From<arrow::error::ArrowError> for Error {
    fn from(error: arrow::error::ArrowError) -> Self {
        Self::Arrow(Box::new(error))
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(Box::new(error))
    }
}

impl Error {
    /// Build a boxed parse error without exposing its storage choice to call sites.
    ///
    /// Cold because a well-formed cell never constructs this diagnostic payload.
    #[cold]
    pub fn value_parse(
        column: impl Into<String>,
        value: impl Into<String>,
        data_type: impl Into<String>,
    ) -> Self {
        Self::ValueParse(Box::new(ValueParseDetail {
            column: column.into(),
            value: value.into(),
            data_type: data_type.into(),
        }))
    }
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<Error>() == 32,
    "pg_to_arrow::Error rides the per-row append_row Result; keep the cold payload boxed"
);
