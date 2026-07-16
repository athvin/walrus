//! `SinkMeta` — the provenance document embedded in every Parquet row.
//!
//! Each row walrus writes carries one added column, `walrus_pg_sink_meta`, a JSON document
//! bunching *all* batch/row provenance. The sink **serializes** [`SinkMeta`] into that column; the
//! loader persists it verbatim into `<table>_raw` and **promotes** `op`, `commit_lsn`, `lsn`, and
//! `sink_processed_at` to typed columns, then drops the meta from the derived `<table>` mirror
//! (it's provenance, not current state).
//!
//! **The JSON keys and value shapes here are a cross-service wire contract** (architecture.md
//! §1.4): the sink and loader must agree byte-for-byte, so a renamed field or a stray offset on a
//! timestamp silently breaks the loader. Field names match the documented keys 1:1; `Op`/`Kind`
//! serialize to the documented scalars; and every datetime is UTC RFC-3339 with a `Z` suffix.

use crate::{Error, Lsn, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The change operation. Serializes to a single lowercase char: `i` | `u` | `d` | `t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    #[serde(rename = "i")]
    Insert,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "t")]
    Truncate,
}

/// Where the row originated: an exported-snapshot backfill row, a live WAL-stream row, or a
/// single-table-reload chunk row (PR 6.5 — stamped `commit_lsn = lsn = L_i`, snapshot-op
/// semantics so any overlapping stream event wins the loader's dedup).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Snapshot,
    Stream,
    Reload,
}

/// A UTC instant rendered as RFC-3339 with a `Z` suffix — walrus's only legal datetime form.
///
/// Wrapping [`jiff::Timestamp`] (which is *always* a UTC instant) makes it impossible for a caller
/// to emit a local or source-offset timestamp: the inner value has no offset, and serialization
/// always renders the `Z` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcTimestamp(jiff::Timestamp);

impl UtcTimestamp {
    /// The current instant, in UTC.
    pub fn now() -> Self {
        UtcTimestamp(jiff::Timestamp::now())
    }

    /// Parse an RFC-3339 string, **rejecting** anything not already normalized to UTC `Z` — a
    /// numeric offset (e.g. `+02:00`) is refused rather than silently converted, so the wire form
    /// is always UTC (architecture.md §1.4).
    pub fn parse_rfc3339(s: &str) -> Result<Self> {
        if !(s.ends_with('Z') || s.ends_with('z')) {
            return Err(Error::Internal(format!(
                "timestamp {s:?} must be UTC with a 'Z' suffix, not a numeric offset"
            )));
        }
        let ts: jiff::Timestamp = s
            .parse()
            .map_err(|e| Error::Internal(format!("invalid RFC-3339 timestamp {s:?}: {e}")))?;
        Ok(UtcTimestamp(ts))
    }

    /// Build from a pgoutput wire timestamp: **microseconds since 2000-01-01T00:00:00Z** (proto §4) —
    /// the Postgres epoch, NOT the Unix epoch. Offsets by the fixed 946_684_800 s (2000-01-01 −
    /// 1970-01-01; Unix time has no leap seconds) and defers to jiff's range check, so a corrupt or
    /// overflowing frame is a decode error — **never a panic** (PR 5.9; retires the `commit_ts` TODO).
    pub fn from_pg_micros(pg_micros: i64) -> Result<Self> {
        // 2000-01-01T00:00:00Z expressed as microseconds since the Unix epoch.
        const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;
        let unix_micros = pg_micros
            .checked_add(PG_EPOCH_OFFSET_MICROS)
            .ok_or_else(|| {
                Error::Internal(format!("pgoutput commit_ts overflow: {pg_micros} µs"))
            })?;
        let ts = jiff::Timestamp::from_microsecond(unix_micros).map_err(|e| {
            Error::Internal(format!(
                "pgoutput commit_ts {pg_micros} µs out of range: {e}"
            ))
        })?;
        Ok(UtcTimestamp(ts))
    }
}

impl Serialize for UtcTimestamp {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        // jiff::Timestamp's Display is RFC-3339 with a `Z` suffix — exactly walrus's wire form.
        s.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for UtcTimestamp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s: String = Deserialize::deserialize(d)?;
        UtcTimestamp::parse_rfc3339(&s).map_err(serde::de::Error::custom)
    }
}

/// The provenance document embedded (as JSON `Utf8`) in every Parquet row's `walrus_pg_sink_meta`.
///
/// **Field order and keys are a cross-service wire contract** (architecture.md §1.4) — the loader
/// reads this back verbatim. Deserialization is lenient about *unknown* keys on purpose, so a
/// newer sink can add a provenance field without breaking an older loader mid-rollout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkMeta {
    /// The change operation (`i`/`u`/`d`/`t`).
    pub op: Op,
    /// Per-row WAL LSN — the per-PK last-writer tiebreaker only (zero-padded 16-hex).
    pub lsn: Lsn,
    /// Transaction commit LSN — **the** order/watermark key (zero-padded 16-hex).
    pub commit_lsn: Lsn,
    /// Transaction commit time (UTC `Z`).
    pub commit_ts: UtcTimestamp,
    /// Source transaction id.
    pub xid: u32,
    /// Generation counter that namespaces all control-plane state (Postgres `bigint`).
    pub epoch: i64,
    /// UUID of the Parquet batch this row belongs to.
    pub batch_id: String,
    /// Structural schema version of the source relation (Postgres `bigint`).
    pub schema_version: i64,
    /// Source schema name.
    pub source_schema: String,
    /// Source table name.
    pub source_table: String,
    /// Whether the row came from the exported snapshot or the live stream.
    pub kind: Kind,
    /// Columns delivered as unchanged-TOAST placeholders (values absent from the wire).
    pub unchanged_toast: Vec<String>,
    /// Stable identity of the sink pod that produced this row.
    pub sink_instance: String,
    /// When the sink processed this row (UTC `Z`) — promoted to a typed `<table>_raw` column.
    pub sink_processed_at: UtcTimestamp,
}

// --- amortized serialization (PR 5.7) -------------------------------------------------------------
//
// The meta column dominates `append_row` (PR 5.4: `serde_json::to_string(SinkMeta)` ≈ 576 ns/row,
// ~91 % of the narrow-row cost). Within one sealed Parquet file the *batch-constant* fields never
// change, so the batcher serializes them ONCE and, per row, serializes only the varying fields —
// splicing the two into `{const,row}`. Byte-equivalence with `to_string(SinkMeta)` is guaranteed by
// construction: these borrow structs carry the identical field names and types (no serde attributes
// on `SinkMeta`), so each field serializes exactly as before; only the key ORDER shifts, and the
// loader parses by key (`$.op`, …), never by position. Proven by `amortized_meta_matches_full`.

/// The batch-constant subset of [`SinkMeta`] — the same for every row of one sealed file.
#[derive(Serialize)]
struct MetaConst<'a> {
    epoch: i64,
    batch_id: &'a str,
    schema_version: i64,
    source_schema: &'a str,
    source_table: &'a str,
    kind: &'a Kind,
    sink_instance: &'a str,
}

/// The per-row subset of [`SinkMeta`].
#[derive(Serialize)]
struct MetaRow<'a> {
    op: &'a Op,
    lsn: &'a Lsn,
    commit_lsn: &'a Lsn,
    commit_ts: &'a UtcTimestamp,
    xid: u32,
    unchanged_toast: &'a [String],
    sink_processed_at: &'a UtcTimestamp,
}

/// The inner of a serialized JSON object (`{…}`) with the braces removed. Both fragments are
/// non-empty structs, so the output is always `{…}` and this never underflows.
fn object_inner(s: &str) -> &str {
    s.get(1..s.len().saturating_sub(1)).unwrap_or("")
}

impl SinkMeta {
    /// The batch-constant fields as a brace-less JSON fragment (e.g. `"epoch":7,"batch_id":"…",…`),
    /// serialized once per sealed batch and cached by the batcher.
    pub fn const_json_inner(&self) -> std::result::Result<String, serde_json::Error> {
        let s = serde_json::to_string(&MetaConst {
            epoch: self.epoch,
            batch_id: &self.batch_id,
            schema_version: self.schema_version,
            source_schema: &self.source_schema,
            source_table: &self.source_table,
            kind: &self.kind,
            sink_instance: &self.sink_instance,
        })?;
        Ok(object_inner(&s).to_string())
    }

    /// Append the per-row fields (brace-less) to `buf`; the batcher wraps `{const,row}` around them.
    pub fn write_row_json_inner(
        &self,
        buf: &mut String,
    ) -> std::result::Result<(), serde_json::Error> {
        let s = serde_json::to_string(&MetaRow {
            op: &self.op,
            lsn: &self.lsn,
            commit_lsn: &self.commit_lsn,
            commit_ts: &self.commit_ts,
            xid: self.xid,
            unchanged_toast: &self.unchanged_toast,
            sink_processed_at: &self.sink_processed_at,
        })?;
        buf.push_str(object_inner(&s));
        Ok(())
    }
}

#[cfg(test)]
#[path = "sink_meta_test.rs"]
mod tests;
