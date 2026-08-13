// The loader runs one worker per `.duckdb` file on a `LocalSet`. `TableDb` is Send + !Sync, so a
// future holding `&TableCtx` or `&TableDb` across an await is intentionally !Send (see duck.rs).
#![allow(
    clippy::future_not_send,
    reason = "futures borrowing Send + !Sync TableDb values are intentionally driven on a LocalSet"
)]

//! `walrus-loader` — reads the sink's staged Parquet from S3 and materialises it into per-table DuckDB
//! files (`<table>` mirror + `<table>_raw` CDC log). This PR is the first vertical slice: the ordered
//! fail-fast [`bootstrap`] that proves exclusive ownership (control-plane [`lease`] + DuckDB file lock)
//! and stands up [`health`] — no manifest file is claimed yet (that is PR 3.2).

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
pub mod plan;
pub mod shutdown;
pub mod supervisor;
pub mod transform;
