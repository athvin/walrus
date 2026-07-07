//! The walrus control plane: sqlx access to the coordination-contract tables
//! (`replication_state`, `file_manifest`, `loader_checkpoint`, `schema_registry`, `ddl_manifest`).
//!
//! This crate owns the control-DB connection pool and the versioned migration set. Row-level
//! models (manifest claim/insert, checkpoint upsert, registry) land in PRs 1.4–1.6; this PR is
//! just the schema, the ability to apply it, and the connect path every later model reuses.

pub mod checkpoint;
pub mod db;
pub mod ddl_manifest;
pub mod manifest;
pub mod replication_state;
pub mod schema_registry;
pub mod table_ownership;

pub use checkpoint::{
    advance_raw_appended, advance_transformed, ensure_checkpoint, read_checkpoint, Checkpoint,
};
pub use db::{connect, run_migrations, ControlError};
pub use ddl_manifest::{insert_ddl, read_pending_ddl, DdlRow};
pub use manifest::{
    claim_ready, delete_claimed, insert_ready, mark_failed, ManifestRow, NewManifestFile,
};
pub use replication_state::{insert_epoch, read_current_epoch, ReplicationState};
pub use schema_registry::{
    read_all_latest_registry, read_latest_version, read_registry, upsert_registry, RegistryRow,
};
pub use table_ownership::{acquire_lease, release_lease, renew_lease, Lease};
