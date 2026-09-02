//! `file_manifest` models: the sink's insert-ready, the loader's claim-in-commit-order, and the
//! queue's retire-by-delete.
//!
//! The manifest is a **work queue, not a history**. The single load-bearing line is the claim
//! ordering: `ORDER BY lsn_end, id` — commit LSN, then `id` as the tiebreaker. It is *not*
//! `lsn_end > raw_appended_lsn`: legacy snapshot files can share one `consistent_point` and therefore
//! one `lsn_end`, so a `>` filter would skip them forever. And it keys on the **commit** LSN, not
//! a max-row LSN, or a late-committing large transaction would be silently dropped. Retiring a file
//! is a `DELETE`, not a status flip — the queue's frontier advances by removal.

use crate::{
    ControlError, ddl_manifest::DdlRow, parse::ParseEnumError, schema_registry::RegistryRow,
};
use common::string_enum;
use common::{EpochNo, Lsn, ManifestId, ReloadId, SchemaVersionNo, UtcTimestamp};
use sqlx::{PgExecutor, PgPool, Row};
use std::collections::{BTreeMap, BTreeSet};

// Each table below is the exact persisted string contract for its `file_manifest` text column.
string_enum! {
    /// The kind of a `file_manifest` row — the canonical enum for the `kind` text column, shared by the
    /// sink (which writes it; pg-sink re-exports this as `FileKind`) and the loader (which routes on it).
    ///
    /// `Spill` is a *single* streamed transaction written before its commit LSN was known; the
    /// loader treats the file's `lsn_end` — not the per-row placeholder — as the authoritative
    /// `commit_lsn` for its rows. `Reload` chunk files enter the same `(lsn_end, id)` claim
    /// order carrying a `reload_id`; `Snapshot` is retained for legacy manifest compatibility.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ManifestKind {
        error = ParseEnumError;
        column = "file_manifest.kind";
        Snapshot => "snapshot",
        Stream => "stream",
        Spill => "spill",
        Reload => "reload",
    }
}

string_enum! {
    /// The lifecycle state of a `file_manifest` row: `Ready` to claim, or integrity-fenced `Failed`.
    /// A failed row is paired with table-level recovery state and deliberately blocks publication;
    /// it is never a license to skip data. Applied rows are DELETED (see [`delete_claimed`]).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ManifestStatus {
        error = ParseEnumError;
        column = "file_manifest.status";
        Ready => "ready",
        Failed => "failed",
    }
}

/// A `ready` file the loader can claim. The column set is exactly what the claim query reads.
///
/// `kind` is `Snapshot | Stream | Spill | Reload` — reload chunk files enter this same
/// queue and sort into the same `(lsn_end, id)` order, carrying the `reload_id` the loader's
/// rebuild trigger routes on. `Snapshot` remains readable for legacy manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRow {
    /// The row's primary key, and the *tiebreaker* half of the `(lsn_end, id)` claim order — so it
    /// is load-bearing for ordering, not just a handle.
    pub id: ManifestId,
    /// Generation this file belongs to; a retired generation's rows are never claimed.
    pub epoch: EpochNo,
    /// Source schema the file's rows came from.
    pub source_schema: String,
    /// Source table the file's rows came from.
    pub source_table: String,
    /// Where the Parquet lives. Durable in object storage *before* this row exists.
    pub s3_uri: String,
    /// Which producer wrote it, which is what the loader routes on; see [`ManifestKind`].
    pub kind: ManifestKind,
    /// Rows in the file, for backlog accounting. Not a correctness input.
    pub row_count: i64,
    /// Exact stored object size in bytes.
    pub object_size: i64,
    /// SHA-256 of the exact Parquet object bytes. The database constrains this to 32 bytes.
    pub sha256: Vec<u8>,
    /// Lowest commit LSN in the file — diagnostic only; the queue never orders on it.
    pub lsn_start: Lsn,
    /// Highest **commit** LSN in the file: the frontier this file advances, and the primary claim
    /// sort key. Legacy snapshot files can share one `lsn_end`, which is why the claim uses
    /// `>=`-free ordering rather than a `>` filter.
    pub lsn_end: Lsn,
    /// The relation shape these rows were encoded at, so the loader can reconstruct the types.
    pub schema_version: SchemaVersionNo,
    /// `Ready` to claim, or dead-lettered `Failed`; see [`ManifestStatus`].
    pub status: ManifestStatus,
    /// `Some` only for `kind='reload'` chunk files; the purge/routing key.
    pub reload_id: Option<ReloadId>,
    /// Atomic per-table stream group. `None` only for legacy/ordinary/reload singleton rows.
    pub stream_group_id: Option<ManifestGroupId>,
    /// Stable zero-based position within `stream_group_id`.
    pub stream_group_ordinal: Option<i64>,
    /// Real streamed-transaction commit timestamp, used to correct speculative spill metadata.
    pub stream_commit_ts: Option<String>,
    /// Top-level transaction id from the protocol-v2 `StreamCommit` receipt.
    pub stream_top_xid: Option<i64>,
    /// Number of children that must be appended atomically for this group.
    pub stream_group_expected_files: Option<i64>,
    /// Sum of all child row counts recorded by the atomic publication transaction.
    pub stream_group_row_count: Option<i64>,
}

/// One per-table atomic publication group produced by a protocol-v2 streamed transaction.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestGroupId(pub i64);

/// What the sink inserts after its Parquet is durable in S3.
///
/// Comparable like the [`ManifestRow`] it becomes, so a mapper that builds one can be asserted as a
/// whole record instead of field by field (which silently skips whatever the assertion forgot).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewManifestFile {
    /// Generation this file belongs to.
    pub epoch: EpochNo,
    /// Source schema the file's rows came from.
    pub source_schema: String,
    /// Source table the file's rows came from.
    pub source_table: String,
    /// Where the Parquet already lives — the insert happens only after the PUT is durable.
    pub s3_uri: String,
    /// Which producer wrote it; see [`ManifestKind`].
    pub kind: ManifestKind,
    /// Rows in the file, for backlog accounting.
    pub row_count: i64,
    /// Exact stored object size in bytes.
    pub object_size: i64,
    /// SHA-256 of the exact Parquet object bytes.
    pub sha256: Vec<u8>,
    /// Lowest commit LSN in the file — diagnostic only.
    pub lsn_start: Lsn,
    /// Highest **commit** LSN in the file — the value the claim order sorts on.
    pub lsn_end: Lsn,
    /// The relation shape these rows were encoded at.
    pub schema_version: SchemaVersionNo,
    /// Set (with `kind=Reload`) only by the chunk export engine; `None` otherwise.
    pub reload_id: Option<ReloadId>,
}

/// Every object materialised for one `StreamCommit`. The control layer groups these by table and
/// publishes the transaction receipt, group parents, and child manifests in one PostgreSQL commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewStreamCommitPublication {
    /// Slot generation that owns the receipt and every child row.
    pub epoch: EpochNo,
    /// Protocol-v2 top-level transaction id.
    pub top_xid: u32,
    /// Authoritative source commit LSN shared by DDL and every file.
    pub commit_lsn: Lsn,
    /// Authoritative source commit timestamp.
    pub commit_ts: UtcTimestamp,
    /// DDL audit rows whose source transaction ends at this same `StreamCommit`.
    pub ddl_rows: Vec<DdlRow>,
    /// Provisional relation shapes made globally visible by this same `StreamCommit`.
    pub registry_rows: Vec<RegistryRow>,
    /// Every object materialised for the transaction, grouped per table during publication.
    pub files: Vec<NewManifestFile>,
}

/// Whether this exact streamed commit became visible now or had already committed before a crash
/// that prevented the source ACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishStreamOutcome {
    Published,
    AlreadyPublished,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StreamFileReplayShape {
    kind: String,
    row_count: i64,
    lsn_start: Lsn,
    lsn_end: Lsn,
    schema_version: SchemaVersionNo,
}

impl From<&NewManifestFile> for StreamFileReplayShape {
    fn from(file: &NewManifestFile) -> Self {
        Self {
            kind: file.kind.as_str().to_string(),
            row_count: file.row_count,
            lsn_start: file.lsn_start,
            lsn_end: file.lsn_end,
            schema_version: file.schema_version,
        }
    }
}

fn stream_file_shape_json(files: &[StreamFileReplayShape]) -> serde_json::Value {
    serde_json::Value::Array(
        files
            .iter()
            .map(|file| {
                serde_json::json!({
                    "kind": file.kind,
                    "row_count": file.row_count,
                    "lsn_start": file.lsn_start.as_u64(),
                    "lsn_end": file.lsn_end.as_u64(),
                    "schema_version": file.schema_version.0,
                })
            })
            .collect(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamDdlReplayShape {
    source_audit_id: i64,
    source_schema: String,
    source_table: String,
    c_event: String,
    c_tag: String,
    schema_version: SchemaVersionNo,
    c_rel_oid: Option<u32>,
    c_columns: Option<serde_json::Value>,
    c_dropped: Option<serde_json::Value>,
    c_ddl_text: Option<String>,
}

impl From<&DdlRow> for StreamDdlReplayShape {
    fn from(row: &DdlRow) -> Self {
        Self {
            source_audit_id: row.source_audit_id,
            source_schema: row.source_schema.clone(),
            source_table: row.source_table.clone(),
            c_event: row.c_event.clone(),
            c_tag: row.c_tag.clone(),
            schema_version: row.schema_version,
            c_rel_oid: row.c_rel_oid,
            c_columns: row.c_columns.clone(),
            c_dropped: row.c_dropped.clone(),
            c_ddl_text: row.c_ddl_text.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamRegistryReplayShape {
    source_schema: String,
    source_table: String,
    schema_version: SchemaVersionNo,
    descriptors: Vec<common::TypeDescriptor>,
    columns: serde_json::Value,
}

impl From<&RegistryRow> for StreamRegistryReplayShape {
    fn from(row: &RegistryRow) -> Self {
        Self {
            source_schema: row.source_schema.clone(),
            source_table: row.source_table.clone(),
            schema_version: row.schema_version,
            descriptors: row.descriptors.clone(),
            columns: row.columns.clone(),
        }
    }
}

const fn stream_publication_conflict(publication: &NewStreamCommitPublication) -> ControlError {
    ControlError::StreamPublicationConflict {
        epoch: publication.epoch,
        top_xid: publication.top_xid,
        commit_lsn: publication.commit_lsn,
    }
}

async fn validate_existing_stream_publication(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    publication_id: i64,
    publication: &NewStreamCommitPublication,
) -> Result<(), ControlError> {
    let expected_commit_ts = publication.commit_ts.to_string();
    let mut expected_groups = BTreeMap::<(String, String), Vec<StreamFileReplayShape>>::new();
    for file in &publication.files {
        expected_groups
            .entry((file.source_schema.clone(), file.source_table.clone()))
            .or_default()
            .push(file.into());
    }
    for files in expected_groups.values_mut() {
        files.sort();
    }

    // Lock the durable group parents while reading their children. Loader retirement updates the
    // parent in the same transaction that removes the children, so this sees either a complete
    // ready/failed child set or the durable applied aggregate, never a torn transition.
    let durable_groups = sqlx::query(
        "SELECT id, epoch, top_xid, source_schema, source_table, commit_lsn, commit_ts, \
                expected_files, row_count, file_shape, status \
         FROM walrus.stream_manifest_group WHERE publication_id = $1 \
         ORDER BY source_schema, source_table FOR SHARE",
    )
    .bind(publication_id)
    .fetch_all(&mut **tx)
    .await?;
    if durable_groups.len() != expected_groups.len() {
        return Err(stream_publication_conflict(publication));
    }

    let mut seen_groups = BTreeSet::new();
    for group in durable_groups {
        let group_id: i64 = group.try_get("id")?;
        let source_schema: String = group.try_get("source_schema")?;
        let source_table: String = group.try_get("source_table")?;
        let key = (source_schema, source_table);
        if !seen_groups.insert(key.clone()) {
            return Err(ControlError::ManifestInvariant {
                message: format!("stream publication {publication_id} has a duplicate table group"),
            });
        }
        let Some(expected_files) = expected_groups.get(&key) else {
            return Err(stream_publication_conflict(publication));
        };
        let expected_count =
            i64::try_from(expected_files.len()).map_err(|_| ControlError::ManifestInvariant {
                message: format!("too many replay files in {}.{}", key.0, key.1),
            })?;
        let expected_rows = expected_files.iter().try_fold(0_i64, |sum, file| {
            sum.checked_add(file.row_count)
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("replay row-count overflow in {}.{}", key.0, key.1),
                })
        })?;
        let durable_epoch: i64 = group.try_get("epoch")?;
        let durable_top_xid: i64 = group.try_get("top_xid")?;
        let durable_commit_lsn: Lsn = group.try_get("commit_lsn")?;
        let durable_commit_ts: String = group.try_get("commit_ts")?;
        let durable_count: i64 = group.try_get("expected_files")?;
        let durable_rows: i64 = group.try_get("row_count")?;
        let durable_shape: serde_json::Value = group.try_get("file_shape")?;
        let status: String = group.try_get("status")?;
        if durable_epoch != publication.epoch.0
            || durable_top_xid != i64::from(publication.top_xid)
            || durable_commit_lsn != publication.commit_lsn
            || durable_commit_ts != expected_commit_ts
            || durable_count != expected_count
            || durable_rows != expected_rows
            || durable_shape != stream_file_shape_json(expected_files)
        {
            return Err(stream_publication_conflict(publication));
        }

        let children = sqlx::query(
            "SELECT kind, row_count, lsn_start, lsn_end, schema_version \
             FROM walrus.file_manifest WHERE stream_group_id = $1 \
             ORDER BY stream_group_ordinal",
        )
        .bind(group_id)
        .fetch_all(&mut **tx)
        .await?;
        if children.is_empty() && matches!(status.as_str(), "applied" | "superseded") {
            // Applied and snapshot-superseded child manifests are intentionally retired. The
            // parent permanently retains and has just proven their complete semantic shape.
            continue;
        }
        if children.len() != expected_files.len() || !matches!(status.as_str(), "ready" | "failed")
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream group {group_id} status {status:?} has {} children but expected {durable_count}",
                    children.len()
                ),
            });
        }
        let mut durable_files = children
            .into_iter()
            .map(|child| {
                Ok(StreamFileReplayShape {
                    kind: child.try_get("kind")?,
                    row_count: child.try_get("row_count")?,
                    lsn_start: child.try_get("lsn_start")?,
                    lsn_end: child.try_get("lsn_end")?,
                    schema_version: SchemaVersionNo(child.try_get("schema_version")?),
                })
            })
            .collect::<Result<Vec<_>, ControlError>>()?;
        durable_files.sort();
        if &durable_files != expected_files {
            return Err(stream_publication_conflict(publication));
        }
    }
    if seen_groups.len() != expected_groups.len() {
        return Err(stream_publication_conflict(publication));
    }

    let mut expected_ddl = publication
        .ddl_rows
        .iter()
        .map(StreamDdlReplayShape::from)
        .collect::<Vec<_>>();
    expected_ddl.sort_by_key(|row| row.source_audit_id);
    let mut durable_ddl = sqlx::query(
        "SELECT source_audit_id, source_schema, source_table, c_event, c_tag, schema_version, \
                c_rel_oid, c_columns, c_dropped, c_ddl_text \
         FROM walrus.ddl_manifest WHERE epoch = $1 AND c_lsn = $2 \
         ORDER BY source_audit_id",
    )
    .bind(publication.epoch.0)
    .bind(publication.commit_lsn)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|row| {
        Ok(StreamDdlReplayShape {
            source_audit_id: row.try_get("source_audit_id")?,
            source_schema: row.try_get("source_schema")?,
            source_table: row.try_get("source_table")?,
            c_event: row.try_get("c_event")?,
            c_tag: row.try_get("c_tag")?,
            schema_version: SchemaVersionNo(row.try_get("schema_version")?),
            c_rel_oid: row
                .try_get::<Option<sqlx::postgres::types::Oid>, _>("c_rel_oid")?
                .map(|oid| oid.0),
            c_columns: row.try_get("c_columns")?,
            c_dropped: row.try_get("c_dropped")?,
            c_ddl_text: row.try_get("c_ddl_text")?,
        })
    })
    .collect::<Result<Vec<_>, ControlError>>()?;
    durable_ddl.sort_by_key(|row| row.source_audit_id);
    if durable_ddl != expected_ddl {
        return Err(stream_publication_conflict(publication));
    }

    // Registry rows are immutable history keyed by table/version rather than receipt id. Validate
    // every replayed key and its exact shape; DdlConsumer only prepares registry rows owned by the
    // DDL events compared above.
    for expected in publication
        .registry_rows
        .iter()
        .map(StreamRegistryReplayShape::from)
    {
        let durable = sqlx::query(
            "SELECT descriptors, columns FROM walrus.schema_registry \
             WHERE epoch=$1 AND source_schema=$2 AND source_table=$3 AND schema_version=$4",
        )
        .bind(publication.epoch.0)
        .bind(&expected.source_schema)
        .bind(&expected.source_table)
        .bind(expected.schema_version.0)
        .fetch_optional(&mut **tx)
        .await?
        .map(|row| {
            Ok::<_, ControlError>(StreamRegistryReplayShape {
                source_schema: expected.source_schema.clone(),
                source_table: expected.source_table.clone(),
                schema_version: expected.schema_version,
                descriptors: row
                    .try_get::<sqlx::types::Json<Vec<common::TypeDescriptor>>, _>("descriptors")?
                    .0,
                columns: row.try_get("columns")?,
            })
        })
        .transpose()?;
        if durable.as_ref() != Some(&expected) {
            return Err(stream_publication_conflict(publication));
        }
    }
    Ok(())
}

/// Validate that every protocol-v2 group in one claim is complete and internally identical to its
/// durable parent receipt. A malformed group is never handed to DuckLake as a partial transaction.
///
/// # Errors
///
/// Returns [`ControlError::ManifestInvariant`] for incomplete, duplicated, or internally
/// inconsistent group metadata.
pub fn validate_claimed_groups(rows: &[ManifestRow]) -> Result<(), ControlError> {
    let mut groups: BTreeMap<ManifestGroupId, Vec<&ManifestRow>> = BTreeMap::new();
    for row in rows {
        if row.object_size <= 0 || row.sha256.len() != 32 || row.row_count <= 0 {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "manifest {} has an invalid object fingerprint/count",
                    row.id
                ),
            });
        }
        match row.stream_group_id {
            Some(group_id) => groups.entry(group_id).or_default().push(row),
            None => {
                if row.stream_group_ordinal.is_some()
                    || row.stream_commit_ts.is_some()
                    || row.stream_top_xid.is_some()
                    || row.stream_group_expected_files.is_some()
                    || row.stream_group_row_count.is_some()
                {
                    return Err(ControlError::ManifestInvariant {
                        message: format!(
                            "ungrouped manifest {} carries stream-group metadata",
                            row.id
                        ),
                    });
                }
            }
        }
    }

    for (group_id, children) in groups {
        let Some(first) = children.first().copied() else {
            return Err(ControlError::ManifestInvariant {
                message: format!("stream group {} has no children", group_id.0),
            });
        };
        let expected =
            first
                .stream_group_expected_files
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("stream group {} has no expected file count", group_id.0),
                })?;
        let group_rows =
            first
                .stream_group_row_count
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("stream group {} has no expected row count", group_id.0),
                })?;
        let top_xid = first
            .stream_top_xid
            .ok_or_else(|| ControlError::ManifestInvariant {
                message: format!("stream group {} has no top xid", group_id.0),
            })?;
        let commit_ts =
            first
                .stream_commit_ts
                .as_deref()
                .ok_or_else(|| ControlError::ManifestInvariant {
                    message: format!("stream group {} has no commit timestamp", group_id.0),
                })?;
        if expected <= 0
            || usize::try_from(expected).ok() != Some(children.len())
            || !(0..=i64::from(u32::MAX)).contains(&top_xid)
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream group {} returned {} children but expected {expected}",
                    group_id.0,
                    children.len()
                ),
            });
        }

        let mut ordinals = BTreeSet::new();
        let mut actual_rows = 0_i64;
        for child in children {
            if !matches!(child.kind, ManifestKind::Stream | ManifestKind::Spill)
                || child.reload_id.is_some()
                || child.stream_group_expected_files != Some(expected)
                || child.stream_group_row_count != Some(group_rows)
                || child.stream_top_xid != Some(top_xid)
                || child.stream_commit_ts.as_deref() != Some(commit_ts)
                || child.lsn_end != first.lsn_end
                || child.epoch != first.epoch
                || child.source_schema != first.source_schema
                || child.source_table != first.source_table
            {
                return Err(ControlError::ManifestInvariant {
                    message: format!(
                        "stream group {} has inconsistent child {}",
                        group_id.0, child.id
                    ),
                });
            }
            let ordinal =
                child
                    .stream_group_ordinal
                    .ok_or_else(|| ControlError::ManifestInvariant {
                        message: format!(
                            "stream group {} child {} has no ordinal",
                            group_id.0, child.id
                        ),
                    })?;
            if ordinal < 0 || ordinal >= expected || !ordinals.insert(ordinal) {
                return Err(ControlError::ManifestInvariant {
                    message: format!(
                        "stream group {} has invalid/duplicate ordinal {ordinal}",
                        group_id.0
                    ),
                });
            }
            actual_rows = actual_rows.checked_add(child.row_count).ok_or_else(|| {
                ControlError::ManifestInvariant {
                    message: format!("stream group {} row count overflow", group_id.0),
                }
            })?;
        }
        if actual_rows != group_rows {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream group {} row count {actual_rows} does not match receipt {group_rows}",
                    group_id.0
                ),
            });
        }
    }
    Ok(())
}

/// Atomically publish all DDL, registry rows, and per-table file groups belonging to one
/// protocol-v2 `StreamCommit`. The unique transaction receipt is retained for epoch-long replay
/// idempotency, and a replay must match its durable semantic shape before it is accepted.
///
/// # Errors
///
/// Returns [`ControlError::ManifestInvariant`] for an invalid payload or corrupt durable group,
/// [`ControlError::StreamPublicationConflict`] when an existing receipt has different semantics,
/// [`ControlError::ImmutableHistoryConflict`] when a new receipt collides with different durable
/// DDL/registry history, or another [`ControlError`] if PostgreSQL cannot validate or commit the
/// publication.
pub async fn publish_stream_commit(
    pool: &PgPool,
    publication: &NewStreamCommitPublication,
) -> Result<PublishStreamOutcome, ControlError> {
    let mut ddl_audit_ids = BTreeSet::new();
    let mut ddl_versions = BTreeSet::new();
    for row in &publication.ddl_rows {
        if row.epoch != publication.epoch
            || row.c_lsn != publication.commit_lsn
            || !ddl_audit_ids.insert(row.source_audit_id)
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream xid {} contains DDL audit {} outside epoch/commit boundary",
                    publication.top_xid, row.source_audit_id
                ),
            });
        }
        ddl_versions.insert((
            row.source_schema.as_str(),
            row.source_table.as_str(),
            row.schema_version,
        ));
    }
    let mut registry_versions = BTreeSet::new();
    for row in &publication.registry_rows {
        let key = (
            row.source_schema.as_str(),
            row.source_table.as_str(),
            row.schema_version,
        );
        if row.epoch != publication.epoch
            || !registry_versions.insert(key)
            || !ddl_versions.contains(&key)
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream xid {} contains registry row {}.{} v{} outside epoch {}",
                    publication.top_xid,
                    row.source_schema,
                    row.source_table,
                    row.schema_version,
                    publication.epoch
                ),
            });
        }
    }
    for file in &publication.files {
        if file.epoch != publication.epoch
            || !matches!(file.kind, ManifestKind::Stream | ManifestKind::Spill)
            || file.reload_id.is_some()
            || file.lsn_end != publication.commit_lsn
            || file.row_count <= 0
            || file.object_size <= 0
            || file.sha256.len() != 32
        {
            return Err(ControlError::ManifestInvariant {
                message: format!(
                    "stream xid {} contains invalid file {} for commit {}",
                    publication.top_xid, file.s3_uri, publication.commit_lsn
                ),
            });
        }
    }

    let commit_ts = publication.commit_ts.to_string();
    let mut tx = pool.begin().await?;
    let inserted: Option<i64> = sqlx::query_scalar(
        "INSERT INTO walrus.stream_txn_publication \
         (epoch, top_xid, commit_lsn, commit_ts) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (epoch, commit_lsn) DO NOTHING RETURNING id",
    )
    .bind(publication.epoch.0)
    .bind(i64::from(publication.top_xid))
    .bind(publication.commit_lsn)
    .bind(&commit_ts)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(publication_id) = inserted else {
        let existing: Option<(i64, i64, String)> = sqlx::query_as(
            "SELECT id, top_xid, commit_ts FROM walrus.stream_txn_publication \
             WHERE epoch=$1 AND commit_lsn=$2 FOR SHARE",
        )
        .bind(publication.epoch.0)
        .bind(publication.commit_lsn)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((publication_id, existing_top_xid, existing_commit_ts)) = existing else {
            return Err(ControlError::StreamPublicationConflict {
                epoch: publication.epoch,
                top_xid: publication.top_xid,
                commit_lsn: publication.commit_lsn,
            });
        };
        if existing_top_xid != i64::from(publication.top_xid) || existing_commit_ts != commit_ts {
            return Err(ControlError::StreamPublicationConflict {
                epoch: publication.epoch,
                top_xid: publication.top_xid,
                commit_lsn: publication.commit_lsn,
            });
        }
        validate_existing_stream_publication(&mut tx, publication_id, publication).await?;
        tx.rollback().await?;
        return Ok(PublishStreamOutcome::AlreadyPublished);
    };

    // The receipt, schema history, provisional relation shapes, and every data object become
    // visible in one control transaction. In particular, no loader can observe a streamed DDL
    // boundary whose sibling files have not committed yet (or vice versa).
    for row in &publication.ddl_rows {
        crate::ddl_manifest::insert_ddl(&mut *tx, row).await?;
    }
    for row in &publication.registry_rows {
        crate::schema_registry::upsert_registry(&mut *tx, row).await?;
    }

    let mut by_table: BTreeMap<(&str, &str), Vec<&NewManifestFile>> = BTreeMap::new();
    for file in &publication.files {
        by_table
            .entry((&file.source_schema, &file.source_table))
            .or_default()
            .push(file);
    }

    for ((schema, table), files) in by_table {
        let expected_files =
            i64::try_from(files.len()).map_err(|_| ControlError::ManifestInvariant {
                message: format!("too many files in stream group {schema}.{table}"),
            })?;
        let row_count = files
            .iter()
            .try_fold(0_i64, |sum, file| sum.checked_add(file.row_count))
            .ok_or_else(|| ControlError::ManifestInvariant {
                message: format!("row count overflow in stream group {schema}.{table}"),
            })?;
        let mut replay_shape = files
            .iter()
            .map(|file| StreamFileReplayShape::from(*file))
            .collect::<Vec<_>>();
        replay_shape.sort();
        let replay_shape = stream_file_shape_json(&replay_shape);
        let group_id: i64 = sqlx::query_scalar(
            "INSERT INTO walrus.stream_manifest_group \
             (publication_id, epoch, top_xid, source_schema, source_table, commit_lsn, \
              commit_ts, expected_files, row_count, file_shape, status) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'ready') RETURNING id",
        )
        .bind(publication_id)
        .bind(publication.epoch.0)
        .bind(i64::from(publication.top_xid))
        .bind(schema)
        .bind(table)
        .bind(publication.commit_lsn)
        .bind(&commit_ts)
        .bind(expected_files)
        .bind(row_count)
        .bind(replay_shape)
        .fetch_one(&mut *tx)
        .await?;

        for (ordinal, file) in files.into_iter().enumerate() {
            let ordinal = i64::try_from(ordinal).map_err(|_| ControlError::ManifestInvariant {
                message: format!("file ordinal overflow in stream group {schema}.{table}"),
            })?;
            sqlx::query(
                "INSERT INTO walrus.file_manifest \
                 (epoch, source_schema, source_table, s3_uri, kind, row_count, object_size, \
                  sha256, lsn_start, lsn_end, schema_version, status, reload_id, \
                  stream_group_id, stream_group_ordinal) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'ready',NULL,$12,$13)",
            )
            .bind(file.epoch.0)
            .bind(&file.source_schema)
            .bind(&file.source_table)
            .bind(&file.s3_uri)
            .bind(file.kind.as_str())
            .bind(file.row_count)
            .bind(file.object_size)
            .bind(&file.sha256)
            .bind(file.lsn_start)
            .bind(file.lsn_end)
            .bind(file.schema_version.0)
            .bind(group_id)
            .bind(ordinal)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;
    Ok(PublishStreamOutcome::Published)
}

/// Return every durable object URI still referenced by this epoch's manifest, regardless of queue
/// status. Startup orphan collection uses the complete set so a failed or concurrently claimed row
/// is never mistaken for garbage.
///
/// # Errors
///
/// Returns [`ControlError`] if the manifest inventory cannot be read.
pub async fn list_manifest_uris(
    executor: impl PgExecutor<'_>,
    epoch: EpochNo,
) -> Result<Vec<String>, ControlError> {
    Ok(
        sqlx::query_scalar("SELECT s3_uri FROM walrus.file_manifest WHERE epoch = $1 ORDER BY id")
            .bind(epoch.0)
            .fetch_all(executor)
            .await?,
    )
}

/// Insert a `status='ready'` row with `lsn_end` set to the commit LSN; returns the new `id`.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] when the insert fails, or [`ControlError::CheckViolation`] if
/// the manifest values violate a database invariant.
pub async fn insert_ready(
    executor: impl PgExecutor<'_>,
    f: &NewManifestFile,
) -> Result<ManifestId, ControlError> {
    let reload_id = f.reload_id.map(|id| id.0);
    let rec = sqlx::query(include_str!("../sql/postgres/queries/insert_ready.sql"))
        .bind(f.epoch.0)
        .bind(&f.source_schema)
        .bind(&f.source_table)
        .bind(&f.s3_uri)
        .bind(f.kind.as_str())
        .bind(f.row_count)
        .bind(f.object_size)
        .bind(&f.sha256)
        .bind(f.lsn_start)
        .bind(f.lsn_end)
        .bind(f.schema_version.0)
        .bind(reload_id)
        .fetch_one(executor)
        .await?;
    Ok(ManifestId(rec.try_get("id")?))
}

/// Claim the next `ready` files for a table **in commit order**.
///
/// `ORDER BY lsn_end, id` — `id` breaks equal-`lsn_end` ties. There is deliberately **no**
/// `lsn_end > raw_appended_lsn` predicate: that would skip equal-`lsn_end` legacy snapshot files.
///
/// **The pause predicate (reload §2/H8):** while either persisted reload flavor is
/// `requested|exporting|export_complete|publishing`, this generic live-table claim remains closed.
/// Rows accumulate `ready` and the ordinary frontier freezes at `W`; only the publication-specific
/// claim path may drain the frozen `[F,H]` set while it owns the publishing lease. Keeping the pause
/// in the query makes the gate one statement with no check-then-claim TOCTOU, and prevents a generic
/// worker from retiring rows that the atomic reload publication still owns. The legacy `resync`
/// spelling has exactly the same pause and cutover behavior.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the atomic claim query fails, or [`ControlError::Decode`] if
/// a stored manifest kind or status is outside its checked enum set.
pub async fn claim_ready(
    executor: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
    limit: i64,
) -> Result<Vec<ManifestRow>, ControlError> {
    // The `kind`/`status` text columns decode to `String` here, then parse into the typed enums.
    // A value outside the known set is a data-integrity bug (the sink only ever writes `as_str()`),
    // so it maps to the terminal `Decode`.
    let rows = sqlx::query(include_str!("../sql/postgres/queries/claim_ready.sql"))
        .bind(epoch.0)
        .bind(source_schema)
        .bind(source_table)
        .bind(limit)
        .fetch_all(executor)
        .await?;
    let rows = rows
        .into_iter()
        .map(|r| {
            let kind: String = r.try_get("kind")?;
            let status: String = r.try_get("status")?;
            Ok(ManifestRow {
                id: ManifestId(r.try_get("id")?),
                epoch: EpochNo(r.try_get("epoch")?),
                source_schema: r.try_get("source_schema")?,
                source_table: r.try_get("source_table")?,
                s3_uri: r.try_get("s3_uri")?,
                kind: kind.parse()?,
                row_count: r.try_get("row_count")?,
                object_size: r.try_get("object_size")?,
                sha256: r.try_get("sha256")?,
                lsn_start: r.try_get("lsn_start")?,
                lsn_end: r.try_get("lsn_end")?,
                schema_version: SchemaVersionNo(r.try_get("schema_version")?),
                status: status.parse()?,
                reload_id: r.try_get::<Option<i64>, _>("reload_id")?.map(ReloadId),
                stream_group_id: r
                    .try_get::<Option<i64>, _>("stream_group_id")?
                    .map(ManifestGroupId),
                stream_group_ordinal: r.try_get("stream_group_ordinal")?,
                stream_commit_ts: r.try_get("stream_commit_ts")?,
                stream_top_xid: r.try_get("stream_top_xid")?,
                stream_group_expected_files: r.try_get("stream_group_expected_files")?,
                stream_group_row_count: r.try_get("stream_group_row_count")?,
            })
        })
        .collect::<Result<Vec<_>, ControlError>>()?;
    validate_claimed_groups(&rows)?;
    Ok(rows)
}

/// The newest `ready` file's commit LSN for a table — the head of the Phase-A backlog — or `None`
/// when the queue is empty. Powers the `walrus_loader_raw_append_lag_bytes` gauge: the lag
/// is this minus `raw_appended_lsn`. `MAX` over an empty set is SQL `NULL` → `None`.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the backlog query cannot reach or read control Postgres.
pub async fn max_ready_lsn_end(
    executor: impl PgExecutor<'_>,
    epoch: EpochNo,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<Lsn>, ControlError> {
    let row = sqlx::query_file!(
        "sql/postgres/queries/max_ready_lsn_end.sql",
        epoch.0,
        source_schema,
        source_table,
    )
    .fetch_one(executor)
    .await?;
    Ok(row.max_lsn_end)
}

/// Retire claimed rows — the queue's "done" is a `DELETE`, not a status flip. Returns the count.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the claimed-row delete cannot be executed.
pub async fn delete_claimed(
    executor: impl PgExecutor<'_>,
    ids: &[ManifestId],
) -> Result<u64, ControlError> {
    // The transparent `Type` impl carries no `PgHasArrayType`, so unwrap to `&[i64]` for the array bind.
    let raw: Vec<i64> = ids.iter().map(|id| id.0).collect();
    let row = sqlx::query(include_str!("../sql/postgres/queries/delete_claimed.sql"))
        .bind(raw.as_slice())
        .fetch_one(executor)
        .await?;
    let deleted: i64 = row.try_get("deleted_count")?;
    u64::try_from(deleted).map_err(|_| ControlError::ManifestInvariant {
        message: format!("delete_claimed returned invalid row count {deleted}"),
    })
}

/// Purge a rebuilding table's SUPERSEDED pending rows at trigger time: every non-reload row with
/// `lsn_end <= F` describes a commit covered by the post-F consistent baseline, so applying it
/// after the rebuild would only re-apply history the replacement already contains. Baseline chunk
/// files themselves carry `lsn_end = F`; the `kind` filter is what lets them survive their own
/// purge. No status filter: a dead-lettered (`failed`) pre-F file is
/// equally superseded. The exact publication and live table-ownership token are locked in the same
/// transaction as the purge. Complete protocol-v2 groups are retired as `superseded`; an
/// incomplete or semantically changed group aborts the entire operation. Idempotent (a re-run
/// deletes nothing). Returns rows purged.
///
/// # Errors
///
/// Returns [`ControlError::ReloadTransition`] for a stale publisher,
/// [`ControlError::ManifestInvariant`] for a torn group, or the underlying database error.
pub async fn delete_publication_superseded(
    pool: &PgPool,
    publication: &crate::reload::ReloadPublication,
    owner_pod: &str,
    fencing_token: i64,
) -> Result<u64, ControlError> {
    let mut tx = pool.begin().await?;
    crate::reload::lock_publication(&mut *tx, publication, owner_pod, fencing_token).await?;
    let row = sqlx::query(include_str!(
        "../sql/postgres/queries/delete_superseded.sql"
    ))
    .bind(publication.epoch.0)
    .bind(&publication.source_schema)
    .bind(&publication.source_table)
    .bind(publication.start_lsn)
    .fetch_one(&mut *tx)
    .await?;
    let invalid_groups: i64 = row.try_get("invalid_groups")?;
    if invalid_groups != 0 {
        tx.rollback().await?;
        return Err(ControlError::ManifestInvariant {
            message: format!(
                "reload {} cannot supersede {invalid_groups} incomplete or changed stream groups",
                publication.reload_id
            ),
        });
    }
    let deleted: i64 = row.try_get("deleted_count")?;
    let deleted = u64::try_from(deleted).map_err(|_| ControlError::ManifestInvariant {
        message: format!("superseded purge returned invalid row count {deleted}"),
    })?;
    tx.commit().await?;
    Ok(deleted)
}

#[cfg(test)]
#[path = "manifest_test.rs"]
mod tests;
