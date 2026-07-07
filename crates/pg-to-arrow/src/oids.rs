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
// Range families → 5 flat sibling columns (PR 2.13). OIDs are stable pg_catalog built-ins.
pub const INT4RANGE: u32 = 3904;
pub const NUMRANGE: u32 = 3906;
pub const TSRANGE: u32 = 3908;
pub const TSTZRANGE: u32 = 3910;
pub const DATERANGE: u32 = 3912;
pub const INT8RANGE: u32 = 3926;
// Multirange families (PG14+) → LIST<STRUCT> (PR 2.13).
pub const INT4MULTIRANGE: u32 = 4451;
pub const NUMMULTIRANGE: u32 = 4532;
pub const TSMULTIRANGE: u32 = 4533;
pub const TSTZMULTIRANGE: u32 = 4534;
pub const DATEMULTIRANGE: u32 = 4535;
pub const INT8MULTIRANGE: u32 = 4536;
// Native geometric types → STRUCT/LIST of doubles (PR 2.14).
pub const POINT: u32 = 600;
pub const LSEG: u32 = 601;
pub const PATH: u32 = 602;
pub const BOX: u32 = 603;
pub const POLYGON: u32 = 604;
pub const LINE: u32 = 628;
pub const CIRCLE: u32 = 718;
// Tier-3 canonical-text carriers → VARCHAR (PR 2.15): no lossless structural target.
pub const XML: u32 = 142;
pub const XID: u32 = 28;
pub const CIDR: u32 = 650;
pub const MACADDR8: u32 = 774;
pub const MACADDR: u32 = 829;
pub const INET: u32 = 869;
pub const BIT: u32 = 1560;
pub const VARBIT: u32 = 1562;
pub const TXID_SNAPSHOT: u32 = 2970;
pub const PG_LSN: u32 = 3220;
pub const TSVECTOR: u32 = 3614;
pub const TSQUERY: u32 = 3615;
pub const XID8: u32 = 5069;
// uuid → native DuckDB UUID via the arrow.uuid extension (PR 2.16).
pub const UUID: u32 = 2950;
// Postgres `FirstNormalObjectId`: user-defined types (incl. enums) get OIDs at/above this. The sink
// treats a non-builtin OID as `enum → VARCHAR` for now; PR 2.22 resolves enum-ness from the catalog.
pub const FIRST_NORMAL_OID: u32 = 16384;
pub const NUMERIC: u32 = 1700;
pub const JSONB: u32 = 3802;
