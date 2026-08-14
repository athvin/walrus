//! `FailureClass` — the terminal-vs-transient contract every walrus error enum implements.
//!
//! **Required:** [`FailureClass::is_terminal`] — the one thing an implementor must decide, and
//! deliberately the only one: an exhaustive `match` with no `_` arm makes an unclassified new
//! variant a compile error.
//!
//! **Defaulted:** [`FailureClass::is_transient`] is the exact complement of `is_terminal` and
//! should never be overridden. [`FailureClass::exit_code`] answers `Internal` (70) for an
//! unclassified failure; every error type whose values reach a `main` overrides it with the
//! documented per-variant code.

use crate::ExitCode;

pub trait FailureClass {
    /// REQUIRED. True when retrying under the startup deadline can never help — die now, non-zero.
    fn is_terminal(&self) -> bool;

    /// DEFAULTED. The exact complement of [`Self::is_terminal`]: a dependency that may still be
    /// coming up, so the bootstrap retries it with backoff. Do not override.
    fn is_transient(&self) -> bool {
        todo!("return the complement of is_terminal")
    }

    /// DEFAULTED. The process exit status for this failure. The default is the unclassified
    /// fallback; override it wherever the values can reach `std::process::ExitCode`.
    fn exit_code(&self) -> ExitCode {
        todo!("return the unclassified internal exit code")
    }
}

#[cfg(test)]
#[path = "failure_class_test.rs"]
mod tests;
