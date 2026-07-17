//! Shared SQL string helpers for the crates that build DuckDB/Postgres statements by hand.

/// Escape a string for interpolation as a **single-quoted SQL string literal** by doubling every
/// `'`. The caller supplies the surrounding quotes (`format!("'{}'", sql_literal(s))`) — or
/// substitutes the result into a template whose placeholder already sits inside quotes.
///
/// This is literal escaping only; it is **not** identifier quoting (that doubles `"`).
///
/// ```
/// use common::sql::sql_literal;
/// assert_eq!(sql_literal("O'Brien"), "O''Brien");
/// assert_eq!(sql_literal("plain"), "plain");
/// ```
pub fn sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
#[path = "sql_test.rs"]
mod tests;
