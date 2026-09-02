//! Source-catalog queries shared by bootstrap reconciliation and table reloads.
//!
//! These helpers describe the exact publication inventory and the relation shape pgoutput will
//! advertise. They do not read table data or open a PostgreSQL snapshot.

use anyhow::Context;
use common::{PgColumn, PgRelation, ReplicaIdentity};

/// Session/xact advisory-lock key shared with `walrus.guard_publication_ddl()` in source migration
/// 0002. The bytes spell `walruspb`; changing either side would silently remove serialization.
pub const PUBLICATION_DDL_GUARD_KEY: i64 = 0x7761_6c72_7573_7062;

/// The four row-change families a PostgreSQL publication may emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationActions {
    insert: bool,
    update: bool,
    delete: bool,
    truncate: bool,
}

impl PublicationActions {
    /// Whether the publication carries every mutation the downstream mirror must reconcile.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.insert && self.update && self.delete && self.truncate
    }

    fn disabled(self) -> String {
        [
            (!self.insert).then_some("INSERT"),
            (!self.update).then_some("UPDATE"),
            (!self.delete).then_some("DELETE"),
            (!self.truncate).then_some("TRUNCATE"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(", ")
    }
}

/// Effective publication membership and any per-relation restriction on one exact target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationTargetOptions {
    published: bool,
    row_filter: bool,
    column_list: bool,
    topology_stable: bool,
}

/// A publication configuration that cannot provide a lossless full-table WAL overlay.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PublicationCoverageIssue {
    /// The configured publication disappeared.
    #[error("publication {publication} does not exist")]
    MissingPublication { publication: String },
    /// At least one mutation family is disabled globally for the publication.
    #[error(
        "publication {publication} does not publish required operations: {disabled} \
         (need INSERT, UPDATE, DELETE, TRUNCATE)"
    )]
    DisabledOperations {
        publication: String,
        disabled: String,
    },
    /// The exact relation is absent from the effective publication inventory.
    #[error("table {schema}.{table} is not in publication {publication}")]
    MissingTarget {
        publication: String,
        schema: String,
        table: String,
    },
    /// A row predicate would make the baseline/WAL overlay omit matching changes.
    #[error(
        "publication {publication} applies a row filter to {schema}.{table}; \
         full-table reconciliation requires every row"
    )]
    RowFilter {
        publication: String,
        schema: String,
        table: String,
    },
    /// A column list would make the baseline/WAL overlay omit part of the row shape.
    #[error(
        "publication {publication} applies a column list to {schema}.{table}; \
         full-table reconciliation requires every column"
    )]
    ColumnList {
        publication: String,
        schema: String,
        table: String,
    },
    /// Table topology or indirect membership can change the visible row set without publication DDL.
    #[error(
        "table {schema}.{table} has topology-dependent membership in publication {publication}; \
         full-table reconciliation requires a plain non-inherited table published directly or \
         through FOR ALL TABLES"
    )]
    TopologyDependent {
        publication: String,
        schema: String,
        table: String,
    },
}

/// Read the publication's global action flags without locking it.
///
/// # Errors
///
/// Returns the source query error. `Ok(None)` means the named publication does not exist.
pub async fn publication_actions<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    publication: &str,
) -> Result<Option<PublicationActions>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT pubinsert, pubupdate, pubdelete, pubtruncate
             FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?;
    Ok(row.map(|row| PublicationActions {
        insert: row.get(0),
        update: row.get(1),
        delete: row.get(2),
        truncate: row.get(3),
    }))
}

/// Try to acquire the orchestration SQL session's shared publication-DDL guard. The source event
/// trigger takes the same key exclusively for publication DDL; the raw replication session takes a
/// second shared try-lock so loss of either connection cannot silently remove the only guard.
///
/// Exporter sessions must not acquire it: they start after an operator's exclusive request can have
/// queued and could starve behind that writer. The raw replication client uses a nonblocking try-lock
/// during startup and fails the whole pipeline if it cannot join the initial guard set.
///
/// # Errors
///
/// Returns the source query error.
pub async fn try_acquire_publication_ddl_guard(
    client: &tokio_postgres::Client,
) -> Result<bool, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_lock_shared($1)",
            &[&PUBLICATION_DDL_GUARD_KEY],
        )
        .await?;
    Ok(row.get(0))
}

/// Inspect effective membership and every target option relevant to a lossless full-row overlay.
///
/// The recursive ancestor walk matters for partitions: a leaf can inherit the effective entry from
/// an explicitly-published partition root. `pg_publication_tables.rowfilter` catches the effective
/// predicate; `pg_publication_rel.prattrs` catches even an explicit list that happens to name every
/// current column and would otherwise look identical to an unrestricted `attnames` array. Reading
/// those PostgreSQL-15 additions through `to_jsonb(record)` keeps the same query valid on supported
/// PostgreSQL 14 sources, where the keys are simply absent and neither restriction exists.
///
/// # Errors
///
/// Returns the source query error.
pub async fn publication_target_options<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    publication: &str,
    schema: &str,
    table: &str,
) -> Result<PublicationTargetOptions, tokio_postgres::Error> {
    let row = client
        .query_one(
            "WITH RECURSIVE target(relid) AS (
               SELECT c.oid
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = $2 AND c.relname = $3 AND c.relkind IN ('r', 'p')
             ), ancestors(relid) AS (
               SELECT relid FROM target
               UNION
               SELECT i.inhparent
               FROM pg_catalog.pg_inherits i
               JOIN ancestors a ON a.relid = i.inhrelid
             )
             SELECT
               EXISTS (
                 SELECT 1 FROM pg_catalog.pg_publication_tables pt
                 WHERE pt.pubname = $1 AND pt.schemaname = $2 AND pt.tablename = $3
               ) AS published,
               EXISTS (
                 SELECT 1 FROM pg_catalog.pg_publication_tables pt
                 WHERE pt.pubname = $1 AND pt.schemaname = $2 AND pt.tablename = $3
                   AND pg_catalog.to_jsonb(pt)->>'rowfilter' IS NOT NULL
               ) OR EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_publication p
                 JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = p.oid
                 JOIN ancestors a ON a.relid = pr.prrelid
                 WHERE p.pubname = $1
                   AND pg_catalog.to_jsonb(pr)->>'prqual' IS NOT NULL
               ) AS row_filter,
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_publication p
                 JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid = p.oid
                 JOIN ancestors a ON a.relid = pr.prrelid
                 WHERE p.pubname = $1
                   AND pg_catalog.to_jsonb(pr)->>'prattrs' IS NOT NULL
               ) AS column_list,
               EXISTS (
                 SELECT 1
                 FROM target t
                 JOIN pg_catalog.pg_class c ON c.oid = t.relid
                 WHERE c.relkind = 'r'
                   AND NOT c.relispartition
                   AND NOT EXISTS (
                     SELECT 1 FROM pg_catalog.pg_inherits i
                     WHERE i.inhrelid = t.relid OR i.inhparent = t.relid
                   )
                   AND EXISTS (
                     SELECT 1
                     FROM pg_catalog.pg_publication p
                     WHERE p.pubname = $1
                       AND (
                         p.puballtables
                         OR EXISTS (
                           SELECT 1 FROM pg_catalog.pg_publication_rel pr
                           WHERE pr.prpubid = p.oid AND pr.prrelid = t.relid
                         )
                       )
                   )
               ) AS topology_stable",
            &[&publication, &schema, &table],
        )
        .await?;
    Ok(PublicationTargetOptions {
        published: row.get(0),
        row_filter: row.get(1),
        column_list: row.get(2),
        topology_stable: row.get(3),
    })
}

/// Require all four global publication action flags.
///
/// # Errors
///
/// Returns [`PublicationCoverageIssue::MissingPublication`] or
/// [`PublicationCoverageIssue::DisabledOperations`] for an incomplete configuration.
pub fn require_publication_actions(
    publication: &str,
    actions: Option<PublicationActions>,
) -> Result<(), PublicationCoverageIssue> {
    let Some(actions) = actions else {
        return Err(PublicationCoverageIssue::MissingPublication {
            publication: publication.to_string(),
        });
    };
    if !actions.is_complete() {
        return Err(PublicationCoverageIssue::DisabledOperations {
            publication: publication.to_string(),
            disabled: actions.disabled(),
        });
    }
    Ok(())
}

/// Require exact membership with neither a row filter nor a column list.
///
/// # Errors
///
/// Returns the matching [`PublicationCoverageIssue`] when the target is absent or restricted.
pub fn require_full_target(
    publication: &str,
    schema: &str,
    table: &str,
    options: PublicationTargetOptions,
) -> Result<(), PublicationCoverageIssue> {
    if !options.published {
        return Err(PublicationCoverageIssue::MissingTarget {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    if options.row_filter {
        return Err(PublicationCoverageIssue::RowFilter {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    if options.column_list {
        return Err(PublicationCoverageIssue::ColumnList {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    if !options.topology_stable {
        return Err(PublicationCoverageIssue::TopologyDependent {
            publication: publication.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    Ok(())
}

/// Convert a catalog `::int8` OID without wrapping negative or oversized values.
fn catalog_oid(raw: i64) -> anyhow::Result<u32> {
    u32::try_from(raw).with_context(|| format!("catalog OID {raw} is outside the u32 range"))
}

/// Every published **user** table (`schema ≠ walrus`). The walrus-internal tables are
/// control-plane inputs, not reconciliation targets.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if publication membership cannot be queried or decoded.
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

/// Build a [`PgRelation`] shape from the source catalog (`pg_class`/`pg_attribute`/`pg_index`).
/// Bootstrap reconciliation and reload fencing need this shape before a streamed `Relation`
/// message necessarily arrives.
///
/// # Errors
///
/// Returns [`anyhow::Error`] when the relation is absent, catalog queries/decoding fail, an OID is
/// outside the supported integer range, or `relreplident` is invalid.
pub async fn describe_source_relation<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    schema: &str,
    table: &str,
) -> anyhow::Result<PgRelation> {
    // The relation head and its column list are independent catalog reads. Polling both futures
    // together pipelines them over one tokio-postgres connection.
    let (head, rows) = tokio::try_join!(
        async {
            client
                .query_one(
                    "SELECT c.oid::int8, c.relreplident::text
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = $1 AND c.relname = $2",
                    &[&schema, &table],
                )
                .await
                .with_context(|| format!("describe {schema}.{table}: relation not found"))
        },
        async {
            // Reproduce pgoutput's Relation-column set and key flags exactly. Generated
            // columns are absent from Relation/tuple messages, so including them here would
            // make a live catalog check disagree with the same-version registry shape.
            // DEFAULT marks the primary key, INDEX marks only the chosen replica-identity
            // index, FULL marks every published column, and NOTHING marks none.
            client
                .query(
                    "SELECT a.attname,
                            a.atttypid::int8            AS type_oid,
                            a.atttypmod                 AS type_modifier,
                            CASE c.relreplident
                              WHEN 'f' THEN true
                              WHEN 'd' THEN COALESCE(bool_or(i.indisprimary), false)
                              WHEN 'i' THEN COALESCE(bool_or(i.indisreplident), false)
                              ELSE false
                            END AS is_key
                     FROM pg_class c
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     JOIN pg_attribute a
                         ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                         AND a.attgenerated = ''
                     LEFT JOIN pg_index i
                         ON i.indrelid = c.oid
                         AND (i.indisprimary OR i.indisreplident)
                         AND EXISTS (
                           SELECT 1
                           FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                           WHERE k.attnum = a.attnum AND k.ord <= i.indnkeyatts
                         )
                     WHERE n.nspname = $1 AND c.relname = $2
                     GROUP BY c.relreplident, a.attname, a.atttypid, a.atttypmod, a.attnum
                     ORDER BY a.attnum",
                    &[&schema, &table],
                )
                .await
                .with_context(|| format!("describe {schema}.{table}: read columns"))
        },
    )?;
    let oid: i64 = head.get(0);
    let relreplident: String = head.get(1);

    let columns = rows
        .iter()
        .map(|r| {
            let type_oid = r.get::<_, i64>(1);
            Ok(PgColumn {
                name: r.get::<_, String>(0),
                type_oid: catalog_oid(type_oid)
                    .with_context(|| format!("describe {schema}.{table}: attribute type OID"))?,
                type_modifier: r.get::<_, i32>(2),
                is_key: r.get::<_, bool>(3),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(PgRelation {
        oid: catalog_oid(oid)
            .with_context(|| format!("describe {schema}.{table}: relation OID"))?,
        schema: schema.to_string(),
        name: table.to_string(),
        replica_identity: relreplident
            .parse::<ReplicaIdentity>()
            .with_context(|| format!("describe {schema}.{table}: replica identity"))?,
        columns,
    })
}

#[cfg(test)]
#[path = "source_catalog_test.rs"]
mod tests;
