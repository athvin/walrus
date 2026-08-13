//! The one place an `anyhow::Error` becomes a process exit status.

use common::ExitCode;

/// Recover the distinct exit code for a failure that reached `main`.
pub fn code_for(err: &anyhow::Error) -> ExitCode {
    if let Some(e) = err.downcast_ref::<common::Error>() {
        return e.exit_code();
    }
    ExitCode::Internal
}

#[cfg(test)]
#[path = "exit_test.rs"]
mod tests;
