//! Context helpers for DuckDB results.
//!
//! Every DuckDB failure in this crate is terminal and must identify its operation while preserving
//! the engine error as a typed source. This module keeps that construction in one place.

use crate::error::LoaderError;

mod private {
    pub trait Sealed {}
}

/// Build the loader's typed DuckDB error for the direct-error path in full-rebuild handling.
pub(crate) fn duck_err(op: impl Into<String>, source: duckdb::Error) -> LoaderError {
    LoaderError::Duck {
        op: op.into(),
        source,
    }
}

/// Attach operation context to a DuckDB result while preserving its typed source error.
///
/// This trait is sealed: `.duck()` / `.duck_with()` can be called from anywhere the lib is a
/// dependency (integration tests, benches, a future crate), but only `loader` can implement it.
/// The whole point of the extension is that [`LoaderError::Duck`] is the *single* shape a DuckDB
/// failure takes — `op` plus the preserved engine source. A foreign impl over some other receiver
/// would reintroduce the hand-rolled mapping this module replaced, and it would make every method
/// added here later a breaking change instead of an internal one.
///
/// ```compile_fail
/// use loader::duck_ext::DuckResultExt;
/// use loader::error::LoaderError;
///
/// struct Outcome<T>(T);
///
/// impl<T> DuckResultExt<T> for Outcome<T> {
///     fn duck(self, _op: &str) -> Result<T, LoaderError> {
///         Ok(self.0)
///     }
///
///     fn duck_with(self, _op: impl FnOnce() -> String) -> Result<T, LoaderError> {
///         Ok(self.0)
///     }
/// }
/// ```
pub trait DuckResultExt<T>: private::Sealed {
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

/// The one receiver, for every payload `T`: attaching this context is only meaningful where the
/// failure already *is* a `duckdb::Error`. The seal impl sits with the blanket impl so the pair is
/// added or removed together.
impl<T> private::Sealed for Result<T, duckdb::Error> {}

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
