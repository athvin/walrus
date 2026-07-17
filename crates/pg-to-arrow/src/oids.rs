//! Canonical `pg_catalog` base-type OIDs — moved down to `common` (PR 8.3) so `common::pg_shape`
//! and `loader` can share the single source of truth. Re-exported here to keep `pg_to_arrow::oids::*`
//! resolving for this crate's existing call sites.

pub use common::oids::*;
