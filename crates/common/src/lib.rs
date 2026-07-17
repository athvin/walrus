//! Shared primitives for walrus: errors + exit codes, `Lsn`, telemetry, config,
//! `SinkMeta`, and the neutral Postgres shape types. Populated PR by PR (0.2 →).

pub mod config;
pub mod error;
pub mod lsn;
pub mod metrics;
pub mod pg_shape;
pub mod sink_meta;
pub mod sql;
pub mod telemetry;
pub mod type_descriptor;

pub use config::CommonConfig;
pub use error::{Error, ExitCode, Result};
pub use lsn::Lsn;
pub use pg_shape::{PgColumn, PgRelation, ReplicaIdentity, TupleValue};
pub use sink_meta::{Kind, Op, SinkMeta, UtcTimestamp};
pub use telemetry::{init_tracing, TelemetryConfig};
pub use type_descriptor::{Tier, TypeDescriptor, TypeMeta};
