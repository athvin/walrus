//! `pg_sink` — the walrus Postgres sink library.
//!
//! The hand-rolled pgoutput decoder lives in [`pgoutput`] (driven by golden byte vectors from
//! `pg-sink/tests/`). From PR 2.18 the crate is also a **runnable service**: [`config`] loads and
//! validates settings, [`bootstrap`] runs the ordered fail-fast preflight, [`health`] serves the K8s
//! probes, and [`shutdown`] fans one `CancellationToken` out of SIGTERM/SIGINT. The thin
//! `walrus-pg-sink` binary (`src/main.rs`) wires them together; the replication loop fills in later.

pub mod bootstrap;
pub mod config;
pub mod health;
pub mod pgoutput;
pub mod shutdown;
