//! The walrus control plane: sqlx access to the coordination-contract tables
//! (`replication_state`, `file_manifest`, `loader_checkpoint`, `schema_registry`, `ddl_manifest`).
//!
//! This crate owns the control-DB connection pool, versioned migrations, and row-level models for
//! manifest claims, checkpoints, schema history, ownership, and reload coordination.

// Every item these eight modules declare is published by the `pub use` block below, so `pub` on the
// modules themselves would only mint a second public path for each one. Crate visibility makes that
// flat block the whole API, which leaves the row models, their queries and the module boundaries
// between them free to move without breaking a consumer. `reload` is the documented exception: its
// transition functions stay module-qualified (see the note on its re-export), so it must stay
// reachable as a path.
pub(crate) mod checkpoint;
pub(crate) mod db;
pub(crate) mod ddl_manifest;
pub(crate) mod manifest;
pub(crate) mod parse;
pub mod reload;
pub(crate) mod replication_state;
pub(crate) mod schema_registry;
pub(crate) mod table_ownership;

pub use checkpoint::{
    Checkpoint, advance_raw_appended, advance_transformed, ensure_checkpoint, read_checkpoint,
};
pub use db::{ControlError, connect, run_migrations};
pub use ddl_manifest::{DdlRow, insert_ddl, read_pending_ddl};
pub use manifest::{
    ManifestKind, ManifestRow, ManifestStatus, NewManifestFile, claim_ready, delete_claimed,
    delete_superseded, insert_ready, mark_failed, max_ready_lsn_end,
};
pub use parse::ParseEnumError;
// The reload transition functions stay module-qualified (`reload::request`, `reload::fail`, …):
// several of their names (`renew_lease`, `complete`, `get`) would collide with or read vaguer
// than the flat exports above. Only the types go flat.
pub use reload::{ReloadFlavor, ReloadRow, ReloadStatus};
pub use replication_state::{
    ReplicationState, ReplicationStatus, bump_epoch, insert_epoch, read_current_epoch,
};
pub use schema_registry::{
    RegistryRow, read_all_latest_registry, read_latest_version, read_registry, upsert_registry,
};
pub use table_ownership::{Lease, acquire_lease, release_lease, renew_lease};
