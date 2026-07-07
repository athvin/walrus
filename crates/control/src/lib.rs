//! The walrus control plane: sqlx access to the coordination-contract tables
//! (`replication_state`, `file_manifest`, `loader_checkpoint`, `schema_registry`, `ddl_manifest`).
//!
//! This crate owns the control-DB connection pool and the versioned migration set. Row-level
//! models (manifest claim/insert, checkpoint upsert, registry) land in PRs 1.4–1.6; this PR is
//! just the schema, the ability to apply it, and the connect path every later model reuses.

pub mod checkpoint;
pub mod db;
pub mod manifest;
pub mod replication_state;

pub use checkpoint::{
    advance_raw_appended, advance_transformed, ensure_checkpoint, read_checkpoint, Checkpoint,
};
pub use db::{connect, run_migrations, ControlError};
pub use manifest::{
    claim_ready, delete_claimed, insert_ready, mark_failed, ManifestRow, NewManifestFile,
};
pub use replication_state::{insert_epoch, read_current_epoch, ReplicationState};
