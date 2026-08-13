//! Context helpers for DuckDB results.
//!
//! Every DuckDB failure in this crate is terminal and must identify its operation while preserving
//! the engine error as a typed source. This module keeps that construction in one place.

use crate::error::LoaderError;

/// Build the loader's typed DuckDB error for the direct-error path in full-rebuild handling.
pub(crate) fn duck_err(op: impl Into<String>, source: duckdb::Error) -> LoaderError {
    LoaderError::Duck {
        op: op.into(),
        source,
    }
}

/// Attach operation context to a DuckDB result while preserving its typed source error.
pub trait DuckResultExt<T> {
    /// Attach a constant operation description.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] with `op` and the original DuckDB error when the result is an
    /// error.
    fn duck(self, op: &str) -> Result<T, LoaderError>;

    /// Lazily build a formatted operation description only on the error path.
    ///
    /// # Errors
    ///
    /// Returns [`LoaderError::Duck`] with the generated operation and original DuckDB error when the
    /// result is an error.
    fn duck_with(self, op: impl FnOnce() -> String) -> Result<T, LoaderError>;
}

impl<T> DuckResultExt<T> for Result<T, duckdb::Error> {
    fn duck(self, op: &str) -> Result<T, LoaderError> {
        self.map_err(|source| duck_err(op, source))
    }

    fn duck_with(self, op: impl FnOnce() -> String) -> Result<T, LoaderError> {
        self.map_err(|source| duck_err(op(), source))
    }
}

#[cfg(test)]
#[path = "duck_ext_test.rs"]
mod tests;
