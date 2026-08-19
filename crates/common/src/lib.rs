#![cfg_attr(
    test,
    allow(
        clippy::let_underscore_must_use,
        reason = "unit tests intentionally discard cleanup results"
    )
)]

//! Shared primitives for walrus: errors + exit codes, `Lsn`, telemetry, config,
//! `SinkMeta`, and the neutral Postgres shape types. Populated PR by PR (0.2 →).

pub mod config;
pub mod error;
pub mod failure_class;
pub mod ids;
pub mod lsn;
pub mod metrics;
pub mod oids;
pub mod pg_shape;
pub mod runtime;
pub mod sink_meta;
pub mod sql;
pub mod telemetry;
pub mod type_descriptor;

// `string_enum!` is published from the crate root by `#[macro_export]`, so this module stays
// private: `use common::string_enum;` resolves the macro, not the module.
mod string_enum;

pub use config::CommonConfig;
pub use error::{Error, ExitCode, Result};
pub use failure_class::FailureClass;
pub use ids::{EpochNo, ManifestId, ReloadId, SchemaVersionNo};
pub use lsn::Lsn;
pub use pg_shape::{PgColumn, PgRelation, ReplicaIdentity, TupleValue};
pub use sink_meta::{Kind, Op, PG_EPOCH_UNIX_MICROS, PG_EPOCH_UNIX_SECS, SinkMeta, UtcTimestamp};
pub use telemetry::{TelemetryConfig, init_tracing};
pub use type_descriptor::{Tier, TypeDescriptor, TypeMeta};

#[cfg(test)]
#[path = "lib_test.rs"]
mod tests;
