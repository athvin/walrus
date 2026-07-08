//! `walrus-loader` — reads the sink's staged Parquet from S3 and materialises it into per-table DuckDB
//! files (`<table>` mirror + `<table>_raw` CDC log). This PR is the first vertical slice: the ordered
//! fail-fast [`bootstrap`] that proves exclusive ownership (control-plane [`lease`] + DuckDB file lock)
//! and stands up [`health`] — no manifest file is claimed yet (that is PR 3.2).

pub mod apply_loop;
pub mod bootstrap;
pub mod config;
pub mod ddl;
pub mod duck;
pub mod error;
pub mod health;
pub mod lease;
pub mod phase_a;
pub mod phase_b;
pub mod transform;
