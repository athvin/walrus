//! Typed rejections for control-plane text-column enums.
//!
//! `file_manifest.kind`/`.status` and `table_reload.flavor`/`.status` are text columns with SQL
//! `CHECK` constraints; their Rust enums are the second line of defence. A value outside the known
//! vocabulary is a data-integrity bug, so the exact rejected text is preserved as data.

/// A control-plane enum rejected a text value read from the database.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {expected}: {input:?}")]
pub struct ParseEnumError {
    /// Which vocabulary rejected the value, such as `"manifest kind"` or `"reload status"`.
    pub expected: &'static str,
    /// The exact text that was rejected, preserved verbatim.
    pub input: String,
}

impl ParseEnumError {
    /// Preserve a rejected enum value together with the vocabulary that rejected it.
    #[must_use]
    pub fn new(expected: &'static str, input: &str) -> Self {
        Self {
            expected,
            input: input.to_string(),
        }
    }
}

#[cfg(test)]
#[path = "parse_test.rs"]
mod tests;
