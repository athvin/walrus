//! Shared SQL string helpers for the crates that build DuckDB/Postgres statements by hand.

use std::borrow::Cow;
use std::fmt::{self, Write as _};

/// Escape a string for interpolation as a **single-quoted SQL string literal** by doubling every
/// `'`. The caller supplies the surrounding quotes (`format!("'{}'", sql_literal(s))`) — or
/// substitutes the result into a template whose placeholder already sits inside quotes.
///
/// Returns [`Cow::Borrowed`] when there is nothing to escape and [`Cow::Owned`] only when a `'` was
/// actually doubled.
///
/// This is literal escaping only; it is **not** identifier quoting (that doubles `"`).
///
/// ```
/// use common::sql::sql_literal;
/// assert_eq!(sql_literal("O'Brien"), "O''Brien");
/// assert_eq!(sql_literal("plain"), "plain");
/// ```
#[must_use]
pub fn sql_literal(s: &str) -> Cow<'_, str> {
    if s.contains('\'') {
        Cow::Owned(s.replace('\'', "''"))
    } else {
        Cow::Borrowed(s)
    }
}

/// Why a string cannot be represented as a SQL identifier.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentError {
    /// SQL identifiers cannot be empty.
    #[error("must not be empty")]
    Empty,
    /// A NUL would terminate the identifier on the wire.
    #[error("value {0:?} contains an interior NUL")]
    InteriorNul(String),
}

/// A validated SQL identifier: a table, column, schema, or publication name.
///
/// Construction rejects values that cannot safely reach the wire. [`Display`](fmt::Display)
/// always renders the SQL-standard double-quoted form, doubling interior double quotes. Contrast
/// [`sql_literal`], which escapes a value rather than a name.
///
/// ```
/// use common::sql::SqlIdent;
/// assert_eq!(SqlIdent::new("plain").unwrap().to_string(), "\"plain\"");
/// assert_eq!(SqlIdent::new("a\"b").unwrap().to_string(), "\"a\"\"b\"");
/// assert!(SqlIdent::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlIdent(String);

impl SqlIdent {
    /// Validate a raw identifier and preserve it for allocation-free quoting by [`Display`](fmt::Display).
    ///
    /// # Errors
    ///
    /// Returns [`IdentError::Empty`] for an empty name or [`IdentError::InteriorNul`] when the name
    /// contains a NUL byte.
    pub fn new(s: &str) -> Result<Self, IdentError> {
        if s.is_empty() {
            return Err(IdentError::Empty);
        }
        if s.contains('\0') {
            return Err(IdentError::InteriorNul(s.to_string()));
        }
        Ok(Self(s.to_string()))
    }

    /// Return the unquoted identifier for comparison with catalog values.
    #[must_use]
    pub fn as_raw(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SqlIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('"')?;
        for ch in self.0.chars() {
            if ch == '"' {
                f.write_char('"')?;
            }
            f.write_char(ch)?;
        }
        f.write_char('"')
    }
}

#[cfg(test)]
#[path = "sql_test.rs"]
mod tests;
