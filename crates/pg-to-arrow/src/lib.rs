//! `pg-to-arrow` — the Postgres → Arrow half of the sink.
//!
//! Consumes the neutral shape types (`common::PgRelation` / `PgColumn` / `TupleValue`) the decoder
//! produces and turns them into Arrow schemas, RecordBatches, and (later) Parquet. It depends on
//! `common` **only** — never on `pg-sink` — which is what lets it be unit-tested against hand-built
//! `PgRelation`s with no decoder in sight.
//!
//! PR 2.9 builds the Tier-1 (native 1:1) Arrow schema; values (2.10), Parquet + DuckDB conformance
//! (2.11), and the Tier-2/3 types (2.12+) follow.

pub mod batch;
pub mod error;
pub mod oids;
pub mod parquet;
pub mod schema;

pub use batch::BatchBuilder;
pub use error::Error;
pub use parquet::{default_writer_properties, write_parquet, write_parquet_bytes};
pub use schema::{build_schema, tier1_data_type, tier1_field, SINK_META_COLUMN};
