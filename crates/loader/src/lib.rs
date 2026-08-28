#![cfg_attr(
    test,
    allow(
        clippy::let_underscore_must_use,
        reason = "unit tests intentionally discard cleanup results"
    )
)]
#![allow(
    clippy::future_not_send,
    reason = "futures borrowing Send + !Sync TableDb values are intentionally driven on a LocalSet"
)]

//! `walrus-loader` — reads the sink's staged Parquet from S3 and materialises it into per-table DuckDB
//! files (`<table>` mirror + `<table>_raw` CDC log). This PR is the first vertical slice: the ordered
//! fail-fast [`bootstrap`] that proves exclusive ownership (control-plane [`lease`] + DuckDB file lock)
//! and stands up [`health`] — no manifest file is claimed yet (that is PR 3.2).
//!
//! [`app`] is the entry point: [`app::run`] is the whole service lifecycle, so the `walrus-loader`
//! binary (`src/main.rs`) is only config, tracing, a runtime and an exit code.
//!
//! # Concurrency
//!
//! One apply worker per `.duckdb` file, all on a single `LocalSet`. [`duck`]'s
//! [`TableDb`](duck::TableDb) is `Send + !Sync`, so a future holding a `&` borrow of
//! [`TableCtx`](phase_a::TableCtx) or that [`TableDb`](duck::TableDb) across an await is
//! intentionally `!Send` and cannot be handed to `tokio::spawn` — which is what the crate-level
//! `clippy::future_not_send` allow above records for Clippy.

pub mod app;
pub mod apply_loop;
pub mod bootstrap;
pub mod compaction;
pub mod config;
pub mod ddl;
pub mod duck;
pub mod duck_ext;
pub mod epoch;
pub mod error;
pub mod health;
pub mod lease;
pub mod ownership;
pub mod phase_a;
pub mod phase_b;
// The registry → DuckDB schema plan is an implementation detail of `duck`, `transform` and the three
// drivers (`bootstrap`, `phase_a`, `phase_b`): no binary, integration test or bench names it. Crate
// visibility keeps `TablePlan`'s shape free to move with the type system it bridges, and is why the
// four entry points that build or consume one are `pub(crate)` too.
pub(crate) mod plan;
pub mod shutdown;
pub mod supervisor;
pub mod table_name;
pub mod transform;
