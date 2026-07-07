//! Canonical `pg_catalog` base-type OIDs (stable across Postgres installs).

pub const BOOL: u32 = 16;
pub const BYTEA: u32 = 17;
pub const CHAR: u32 = 18;
pub const INT8: u32 = 20;
pub const INT2: u32 = 21;
pub const INT4: u32 = 23;
pub const TEXT: u32 = 25;
pub const JSON: u32 = 114;
pub const FLOAT4: u32 = 700;
pub const FLOAT8: u32 = 701;
pub const BPCHAR: u32 = 1042;
pub const VARCHAR: u32 = 1043;
pub const DATE: u32 = 1082;
pub const TIME: u32 = 1083;
pub const TIMESTAMP: u32 = 1114;
pub const TIMESTAMPTZ: u32 = 1184;
// Tier-2 decompositions (PR 2.12): each fans out to several sibling columns (§2.4).
pub const INTERVAL: u32 = 1186;
pub const TIMETZ: u32 = 1266;
pub const NUMERIC: u32 = 1700;
pub const JSONB: u32 = 3802;
