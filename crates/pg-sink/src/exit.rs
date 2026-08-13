//! The one place an `anyhow::Error` becomes a process exit status.

use common::ExitCode;

/// Recover the distinct exit code for a failure that reached `main`.
///
/// Tries the classified [`common::Error`] first, then each typed pg-sink error that can escape the
/// run loop, and finally the control crate's error. Anything unrecognised keeps the historical
/// [`ExitCode::Internal`] fallback.
pub fn code_for(err: &anyhow::Error) -> ExitCode {
    if let Some(e) = err.downcast_ref::<common::Error>() {
        return e.exit_code();
    }
    if let Some(e) = err.downcast_ref::<crate::sink::SinkError>() {
        return match e {
            crate::sink::SinkError::Encode(_) => ExitCode::Internal,
            crate::sink::SinkError::Store(_) => ExitCode::ObjectStore,
        };
    }
    if err
        .downcast_ref::<crate::manifest::ManifestError>()
        .is_some()
    {
        return ExitCode::ControlDb;
    }
    if err.downcast_ref::<crate::config::ConfigError>().is_some() {
        return ExitCode::Config;
    }
    if let Some(e) = err.downcast_ref::<crate::preflight::PreflightError>() {
        return match e {
            crate::preflight::PreflightError::NoPrimaryKey { .. } => ExitCode::KeylessTable,
            _ => ExitCode::Preflight,
        };
    }
    if err
        .downcast_ref::<crate::heartbeat::HeartbeatError>()
        .is_some()
    {
        return ExitCode::SourceDb;
    }
    if err.downcast_ref::<control::ControlError>().is_some() {
        return ExitCode::ControlDb;
    }
    ExitCode::Internal
}

#[cfg(test)]
#[path = "exit_test.rs"]
mod tests;
