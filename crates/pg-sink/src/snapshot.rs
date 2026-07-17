//! Snapshot / backfill bootstrap (§1.7) — copy the rows that already exist, then stream.
//!
//! Streaming from a fresh slot only ever sees changes *after* the slot's consistent point; the rows
//! already in the tables must be backfilled first. The slot is therefore created with the replication
//! command `CREATE_REPLICATION_SLOT … (SNAPSHOT 'export')`, which returns a **`consistent_point`** LSN
//! and a **`snapshot_name`**. Every published user table is then copied under a read-only
//! `REPEATABLE READ` transaction that runs `SET TRANSACTION SNAPSHOT '<name>'` — one consistent MVCC
//! read. **There is no "COPY at an LSN": consistency comes from the exported snapshot, not a
//! time-travel read.** Those rows flow through the *same* Arrow → Parquet → S3 → manifest path as
//! streamed changes, marked `kind='snapshot'` and all sharing `lsn_end = consistent_point`
//! (`id`-disambiguated downstream, never by `lsn_end` alone). After backfill the sink streams from that
//! same `consistent_point`, so a row written *during* backfill is not double-counted: the snapshot
//! (as of `consistent_point`) does not contain it, and it arrives once as a post-`consistent_point`
//! stream change.
//!
//! **Exported-snapshot lifetime:** the snapshot dies the moment any other command runs on the
//! slot-creating replication connection or it closes. [`SnapshotConn`] therefore holds that connection
//! strictly idle until every [`Backfill`] session has attached (`SET TRANSACTION SNAPSHOT`); a copy
//! that attaches after it closes is a terminal error (`ERROR: invalid snapshot identifier`).

use crate::batch::{BatchTriggers, Clock, SystemClock, TableBatcher};
use crate::relcache::RelationCache;
use crate::replication::ReplicationStream;
use crate::sink::{FileKind, ParquetSink};
use anyhow::Context;
use common::{
    Kind, Lsn, Op, PgColumn, PgRelation, ReplicaIdentity, SinkMeta, TupleValue, UtcTimestamp,
};
use std::sync::Arc;
use tokio_postgres::NoTls;

/// The two values `CREATE_REPLICATION_SLOT … (SNAPSHOT 'export')` returns. All snapshot files share
/// `lsn_end = consistent_point`.
#[derive(Debug, Clone)]
pub struct ExportedSnapshot {
    pub consistent_point: Lsn,
    pub snapshot_name: String,
}

/// Holds the slot-creating replication connection, which **must stay open + idle** so the exported
/// snapshot remains valid until every backfill session has attached to it.
pub struct SnapshotConn {
    stream: ReplicationStream,
    exported: Option<ExportedSnapshot>,
}

impl SnapshotConn {
    /// Open a replication connection and complete startup (no `START_REPLICATION` yet).
    pub async fn connect(dsn: &str) -> anyhow::Result<Self> {
        Ok(SnapshotConn {
            stream: ReplicationStream::connect(dsn)
                .await
                .context("open replication connection for snapshot export")?,
            exported: None,
        })
    }

    /// `CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput (SNAPSHOT 'export')`. The connection now holds
    /// the exported snapshot — do **not** run anything else on it until backfill is done.
    pub async fn create_slot_with_snapshot(
        &mut self,
        slot: &str,
    ) -> anyhow::Result<ExportedSnapshot> {
        let (consistent_point, snapshot_name) =
            self.stream.create_replication_slot_export(slot).await?;
        let snap = ExportedSnapshot {
            consistent_point,
            snapshot_name,
        };
        tracing::info!(
            slot,
            consistent_point = %snap.consistent_point,
            snapshot_name = %snap.snapshot_name,
            "created replication slot with exported snapshot"
        );
        self.exported = Some(snap.clone());
        Ok(snap)
    }

    /// The consistent point streaming will resume from (available after slot creation).
    pub fn consistent_point(&self) -> Option<Lsn> {
        self.exported.as_ref().map(|s| s.consistent_point)
    }

    /// Hand off to streaming: `START_REPLICATION` from `consistent_point` (this ends the exported
    /// snapshot, which is safe once every backfill session has attached). Consumes the connection.
    pub async fn into_stream(
        mut self,
        slot: &str,
        publication: &str,
    ) -> anyhow::Result<ReplicationStream> {
        let snap = self
            .exported
            .context("create_slot_with_snapshot must run before streaming")?;
        self.stream
            .start_streaming(slot, snap.consistent_point, publication)
            .await
            .context("hand off snapshot → streaming from consistent_point")?;
        Ok(self.stream)
    }
}

/// One serial per-table backfill over an ordinary SQL connection (distinct from the replication one).
pub struct Backfill {
    client: tokio_postgres::Client,
    triggers: BatchTriggers,
    clock: Arc<dyn Clock>,
    epoch: i64,
    instance: String,
}

impl Backfill {
    pub async fn connect(
        dsn: &str,
        epoch: i64,
        instance: String,
        triggers: BatchTriggers,
        statement_timeout: std::time::Duration,
    ) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .context("open backfill SQL connection")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "backfill SQL connection closed");
            }
        });
        // `0` (disabled) is Postgres' default — only override for a positive bound.
        let ms = statement_timeout.as_millis();
        if ms > 0 {
            client
                .batch_execute(&format!("SET statement_timeout = {ms}"))
                .await
                .context("set backfill statement_timeout")?;
        }
        Ok(Backfill {
            client,
            triggers,
            clock: Arc::new(SystemClock),
            epoch,
            instance,
        })
    }

    /// Copy one table under the exported snapshot into `kind='snapshot'` Parquet + manifest rows, all
    /// sharing `lsn_end = consistent_point`. Returns the row count copied. Output is chunked by the same
    /// `max_rows`/`max_bytes` caps as streamed batches (so a large table becomes many files).
    pub async fn copy_table(
        &mut self,
        rel: &PgRelation,
        snap: &ExportedSnapshot,
        sink: &ParquetSink,
        pool: &sqlx::PgPool,
        schema_version: i64,
    ) -> anyhow::Result<u64> {
        // Attach the exported snapshot — the ONLY consistency mechanism (no "COPY at an LSN"). If the
        // slot-creating connection has closed, `SET TRANSACTION SNAPSHOT` fails here (terminal).
        self.client
            .batch_execute(&begin_snapshot_txn(&snap.snapshot_name))
            .await
            .context("BEGIN REPEATABLE READ + SET TRANSACTION SNAPSHOT (snapshot expired?)")?;

        let cached = RelationCache::default()
            .upsert_from_relation(rel.clone(), schema_version)
            .context("build Arrow schema for backfill")?;
        let mut batcher = TableBatcher::new(cached, self.triggers, self.clock.clone())
            .context("create backfill batcher")?;

        // Every column cast ::text gives the type's output form — identical to pgoutput's text tuples,
        // so the shared Arrow conversion applies unchanged.
        let rows = self
            .client
            .query(&select_text_sql(rel), &[])
            .await
            .context("backfill SELECT under snapshot")?;

        let mut copied = 0u64;
        for row in &rows {
            batcher.push(
                self.snapshot_meta(rel, snap, schema_version),
                &row_to_tuple(row, rel.columns.len()),
            );
            // Snapshot rows have no per-row commit boundary: promote them all at the shared
            // consistent_point so the loader's (commit_lsn, lsn) dedup lets any later stream change win.
            // They also have no real commit *time* (pre-existing data), so commit_ts is the
            // snapshot-capture instant — provenance only, never an ordering key (PR 5.9).
            batcher
                .on_commit(snap.consistent_point, UtcTimestamp::now())
                .context("promote snapshot rows")?;
            copied += 1;
            if batcher.should_flush() {
                flush_snapshot(sink, pool, self.epoch, &mut batcher).await?;
            }
        }
        if batcher.committed_rows() > 0 {
            flush_snapshot(sink, pool, self.epoch, &mut batcher).await?;
        }
        // Read-only txn — end it to release the imported snapshot.
        self.client
            .batch_execute("COMMIT")
            .await
            .context("end backfill read transaction")?;
        tracing::info!(
            source_table = %format_args!("{}.{}", rel.schema, rel.name),
            rows = copied,
            "backfilled table under exported snapshot"
        );
        Ok(copied)
    }

    fn snapshot_meta(
        &self,
        rel: &PgRelation,
        snap: &ExportedSnapshot,
        schema_version: i64,
    ) -> SinkMeta {
        SinkMeta {
            op: Op::Insert,
            // Snapshot rows carry no per-row source LSN; the consistent_point stands in for both.
            lsn: snap.consistent_point,
            commit_lsn: Lsn::ZERO, // patched to consistent_point by the batcher's on_commit
            commit_ts: UtcTimestamp::now(),
            xid: 0,
            epoch: self.epoch,
            batch_id: String::new(),
            schema_version,
            source_schema: rel.schema.clone(),
            source_table: rel.name.clone(),
            kind: Kind::Snapshot,
            unchanged_toast: vec![],
            sink_instance: self.instance.clone(),
            sink_processed_at: UtcTimestamp::now(),
        }
    }
}

async fn flush_snapshot(
    sink: &ParquetSink,
    pool: &sqlx::PgPool,
    epoch: i64,
    batcher: &mut TableBatcher,
) -> anyhow::Result<()> {
    let sealed = batcher.seal().context("seal snapshot batch")?;
    let obj =
        crate::consume::flush_batch_kind(sink, pool, epoch, sealed, FileKind::Snapshot).await?;
    tracing::info!(uri = %obj.s3_uri, rows = obj.row_count, lsn_end = %obj.lsn_end, "backfill: snapshot file durable");
    Ok(())
}

/// `BEGIN … REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '<name>';` — the exported snapshot is
/// the whole point (snapshot names are server-generated safe tokens).
fn begin_snapshot_txn(snapshot_name: &str) -> String {
    format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; \
         SET TRANSACTION SNAPSHOT '{snapshot_name}';"
    )
}

/// `SELECT "c1"::text, "c2"::text, … FROM "schema"."table"` — every column as its text output form.
fn select_text_sql(rel: &PgRelation) -> String {
    let cols: Vec<String> = rel
        .columns
        .iter()
        .map(|c| format!("\"{}\"::text", c.name))
        .collect();
    format!(
        "SELECT {} FROM \"{}\".\"{}\"",
        cols.join(", "),
        rel.schema,
        rel.name
    )
}

fn row_to_tuple(row: &tokio_postgres::Row, ncols: usize) -> Vec<TupleValue> {
    (0..ncols)
        .map(|i| match row.get::<_, Option<String>>(i) {
            Some(s) => TupleValue::Text(s),
            None => TupleValue::Null,
        })
        .collect()
}

/// Every published **user** table (`schema ≠ walrus`) — the walrus-internal `heartbeat`/`ddl_audit`
/// tables are control-plane and are never snapshotted.
pub async fn published_user_tables(
    client: &tokio_postgres::Client,
    publication: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let rows = client
        .query(
            "SELECT schemaname, tablename FROM pg_publication_tables
             WHERE pubname = $1 AND schemaname <> 'walrus'
             ORDER BY schemaname, tablename",
            &[&publication],
        )
        .await
        .context("list published user tables")?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect())
}

/// Build a [`PgRelation`] shape from the source catalog (`pg_class`/`pg_attribute`/`pg_index`) — the
/// snapshot path needs the shape *before* any streamed `Relation` message arrives.
pub async fn describe_source_relation(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> anyhow::Result<PgRelation> {
    let head = client
        .query_one(
            "SELECT c.oid::int8, c.relreplident::text
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &table],
        )
        .await
        .with_context(|| format!("describe {schema}.{table}: relation not found"))?;
    let oid: i64 = head.get(0);
    let relreplident: String = head.get(1);

    let rows = client
        .query(
            "SELECT a.attname,
                    a.atttypid::int8            AS type_oid,
                    a.atttypmod                 AS type_modifier,
                    COALESCE(bool_or(i.indisprimary OR i.indisreplident), false) AS is_key
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
             LEFT JOIN pg_index i
                 ON i.indrelid = c.oid AND a.attnum = ANY (i.indkey)
                 AND (i.indisprimary OR i.indisreplident)
             WHERE n.nspname = $1 AND c.relname = $2
             GROUP BY a.attname, a.atttypid, a.atttypmod, a.attnum
             ORDER BY a.attnum",
            &[&schema, &table],
        )
        .await
        .with_context(|| format!("describe {schema}.{table}: read columns"))?;

    let columns = rows
        .iter()
        .map(|r| PgColumn {
            name: r.get::<_, String>(0),
            type_oid: r.get::<_, i64>(1) as u32,
            type_modifier: r.get::<_, i32>(2),
            is_key: r.get::<_, bool>(3),
        })
        .collect();

    Ok(PgRelation {
        oid: oid as u32,
        schema: schema.to_string(),
        name: table.to_string(),
        replica_identity: match relreplident.as_str() {
            "f" => ReplicaIdentity::Full,
            "n" => ReplicaIdentity::Nothing,
            "i" => ReplicaIdentity::Index,
            _ => ReplicaIdentity::Default,
        },
        columns,
    })
}

#[cfg(test)]
#[path = "snapshot_test.rs"]
mod tests;
