//! Type-tagged DuckDB table names. The loader owns two tables per source table with deliberately
//! asymmetric semantics (`crate::ddl` module doc): the mirror `<table>` is kept at the exact current
//! source shape, while `<table>_raw` is an additive superset that never drops or re-casts history.
//! Passing one where the other belongs corrupts data, so they are different types.

use std::marker::PhantomData;

/// The mirror `<table>` — current row per PK, exact current source shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mirror;

/// The CDC log `<table>_raw` — every change verbatim, additive superset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Raw;

/// A DuckDB table name tagged with which of the two tables it names.
///
/// `PhantomData<K>` stores nothing, and the transparent representation makes that a guarantee
/// rather than an observation: `size_of::<DuckTable<K>>() == size_of::<String>()` for every `K`
/// (pinned in `table_name_test.rs`). The parameter exists purely so the compiler can tell a mirror
/// name from a raw name.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckTable<K> {
    name: String,
    _kind: PhantomData<K>,
}

impl<K> DuckTable<K> {
    /// The bare name, for interpolation into a quoted SQL identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.name
    }
}

impl DuckTable<Mirror> {
    /// Tag a mirror table name.
    pub fn new(name: impl Into<String>) -> Self {
        DuckTable {
            name: name.into(),
            _kind: PhantomData,
        }
    }

    /// This mirror's CDC log. The typed-name layer's single production suffix construction.
    #[must_use]
    pub fn raw(&self) -> DuckTable<Raw> {
        DuckTable {
            name: format!("{}_raw", self.name),
            _kind: PhantomData,
        }
    }
}

#[cfg(test)]
#[path = "table_name_test.rs"]
mod tests;
