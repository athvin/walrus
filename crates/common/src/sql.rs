//! Shared SQL string helpers for the crates that build DuckDB/Postgres statements by hand.

use std::borrow::Cow;

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
pub fn sql_literal(s: &str) -> Cow<'_, str> {
    if s.contains('\'') {
        Cow::Owned(s.replace('\'', "''"))
    } else {
        Cow::Borrowed(s)
    }
}

#[cfg(test)]
#[path = "sql_test.rs"]
mod tests;
