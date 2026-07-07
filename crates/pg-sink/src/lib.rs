//! `pg_sink` — the walrus Postgres sink library. The hand-rolled pgoutput decoder lives here (in
//! [`pgoutput`]) so `pg-sink/tests/` can drive it with the golden byte vectors; the thin
//! `walrus-pg-sink` binary (`src/main.rs`) wires it to the live replication stream from PR 2.18.

pub mod pgoutput;
