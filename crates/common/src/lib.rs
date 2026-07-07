//! Shared primitives for walrus: errors + exit codes, `Lsn`, telemetry, config,
//! `SinkMeta`, and the neutral Postgres shape types. Populated PR by PR (0.2 →).

pub mod config;
pub mod error;
pub mod lsn;
pub mod telemetry;

pub use config::CommonConfig;
pub use error::{Error, ExitCode, Result};
pub use lsn::Lsn;
pub use telemetry::{init_tracing, TelemetryConfig};
