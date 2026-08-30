//! `pg-to-arrow` — the Postgres → Arrow half of the sink.
//!
//! Consumes the neutral shape types ([`common::PgRelation`] / [`common::PgColumn`] /
//! [`common::TupleValue`]) the decoder produces and turns them into Arrow schemas, RecordBatches,
//! and (later) Parquet. It depends on `common` **only** — never on `pg-sink` — which is what lets it
//! be unit-tested against hand-built [`PgRelation`](common::PgRelation)s with no decoder in sight.
//!
//! PR 2.9 builds the Tier-1 (native 1:1) Arrow schema; values (2.10), Parquet + DuckDB conformance
//! (2.11), and the Tier-2/3 types (2.12+) follow.

// `batch` and `parquet` declare nothing the `pub use` block below does not already publish —
// `BatchBuilder`, and the three writer entry points — so `pub` on either module would only add a
// second public path to each item. Keeping `parquet` crate-internal also spares readers a
// `pg_to_arrow::parquet` that shadows the `parquet` crate it wraps. The rest stay public: `oids` and
// `uuid_enum` are named as paths by consumers, and `descriptor`, `error`, `geometric`, `range`,
// `schema`, `tier2` and `tier3` each declare items the flat block deliberately leaves qualified.
pub(crate) mod batch;
pub mod descriptor;
pub mod error;
pub mod geometric;
pub mod oids;
pub(crate) mod parquet;
pub mod range;
pub mod schema;
pub mod tier2;
pub mod tier3;
pub mod uuid_enum;

/// Tolerance-based float assertions shared by sibling unit tests (PR 17.5).
#[cfg(test)]
mod approx;

pub use batch::BatchBuilder;
// Only the whole-relation entry point goes flat. The per-column pair (`describe_column`,
// `describe_column_with_labels`) stays module-qualified: a bare `describe_column` at the crate
// root reads like a peer of `build_schema` when it is really the inner step `describe_relation`
// maps over.
pub use descriptor::describe_relation;
pub use error::Error;
pub use parquet::{default_writer_properties, write_parquet, write_parquet_bytes};
pub use schema::{SINK_META_COLUMN, build_schema, emit_fields, tier1_data_type};
pub use tier2::{parse_interval, parse_timetz};
