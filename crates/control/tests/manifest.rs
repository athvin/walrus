#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for the `file_manifest` queue models.
//!
//! Each test runs inside a rolled-back transaction and namespaces its rows by a unique `epoch`, so
//! the tests are isolated from each other and idempotent across runs. Gated behind the
//! `integration` feature (needs the compose control Postgres).
#![cfg(feature = "integration")]

use common::{DdlId, EpochNo, Lsn, SchemaVersionNo, UtcTimestamp};
use control::NewManifestFile;
use control::{
    DdlRow, NewStreamCommitPublication, PublishStreamOutcome, RegistryRow, claim_ready, connect,
    delete_claimed, insert_ready, list_manifest_uris, publish_stream_commit, run_migrations,
};
use sqlx::postgres::PgPool;
use uuid::Uuid;

fn control_dsn() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn pool() -> PgPool {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    pool
}

fn file(epoch: EpochNo, table: &str, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!(
            "s3://walrus/{epoch}/public/{table}/{lsn_end}-{}.parquet",
            Uuid::new_v4()
        ),
        kind: control::ManifestKind::Stream,
        row_count: 1,
        object_size: 1,
        sha256: vec![0; 32],
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: SchemaVersionNo(1),
        reload_id: None,
    }
}

#[tokio::test]
async fn claim_orders_by_lsn_end_then_id() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_001);

    // Insert with lsn_ends out of order, plus two files that SHARE one lsn_end (0/20).
    let a = insert_ready(&mut *tx, &file(epoch, "t", "0/30"))
        .await
        .unwrap();
    let b = insert_ready(&mut *tx, &file(epoch, "t", "0/10"))
        .await
        .unwrap();
    let c = insert_ready(&mut *tx, &file(epoch, "t", "0/20"))
        .await
        .unwrap();
    let d = insert_ready(&mut *tx, &file(epoch, "t", "0/20"))
        .await
        .unwrap();
    assert!(c < d, "c was inserted before d, so its serial id is lower");

    let claimed = claim_ready(&mut *tx, epoch, "public", "t", 100)
        .await
        .unwrap();
    let order: Vec<common::ManifestId> = claimed.iter().map(|r| r.id).collect();
    // (lsn_end ASC, id ASC): 0/10 (b), then 0/20 (c before d), then 0/30 (a).
    assert_eq!(order, vec![b, c, d, a]);

    // The commit-LSN values round-trip through pg_lsn keeping their ordering.
    assert_eq!(claimed[0].lsn_end, "0/10".parse::<Lsn>().unwrap());
    assert_eq!(claimed[3].lsn_end, "0/30".parse::<Lsn>().unwrap());

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn claim_does_not_skip_equal_lsn_end_snapshot_files() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_002);

    // Many snapshot files sharing one lsn_end (the exported snapshot's consistent_point).
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(
            insert_ready(&mut *tx, &file(epoch, "snap", "0/AAAA"))
                .await
                .unwrap(),
        );
    }

    let claimed = claim_ready(&mut *tx, epoch, "public", "snap", 100)
        .await
        .unwrap();
    // ALL five are claimed (none skipped by an `lsn_end >` filter), in ascending id order.
    assert_eq!(claimed.len(), 5);
    assert_eq!(claimed.iter().map(|r| r.id).collect::<Vec<_>>(), ids);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn delete_claimed_retires_exactly_the_given_ids() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_003);

    let id1 = insert_ready(&mut *tx, &file(epoch, "d", "0/10"))
        .await
        .unwrap();
    let id2 = insert_ready(&mut *tx, &file(epoch, "d", "0/20"))
        .await
        .unwrap();
    let id3 = insert_ready(&mut *tx, &file(epoch, "d", "0/30"))
        .await
        .unwrap();

    let n = delete_claimed(&mut *tx, &[id1, id3]).await.unwrap();
    assert_eq!(n, 2);

    let remaining = claim_ready(&mut *tx, epoch, "public", "d", 100)
        .await
        .unwrap();
    assert_eq!(
        remaining.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![id2]
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn manifest_uri_inventory_includes_every_status_and_only_the_requested_epoch() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(900_005);

    let ready = file(epoch, "inventory", "0/10");
    let failed = file(epoch, "inventory", "0/20");
    insert_ready(&mut *tx, &ready).await.unwrap();
    let failed_id = insert_ready(&mut *tx, &failed).await.unwrap();
    sqlx::query("UPDATE walrus.file_manifest SET status = 'failed' WHERE id = $1")
        .bind(failed_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    insert_ready(&mut *tx, &file(EpochNo(epoch.0 + 1), "inventory", "0/30"))
        .await
        .unwrap();

    assert_eq!(
        list_manifest_uris(&mut *tx, epoch).await.unwrap(),
        vec![ready.s3_uri, failed.s3_uri],
        "failed manifests remain durable references; other epochs do not"
    );

    tx.rollback().await.unwrap();
}

async fn clear_stream_publication_epoch(pool: &PgPool, epoch: EpochNo) {
    sqlx::query("WITH authorized AS MATERIALIZED (SELECT set_config('walrus.manifest_delete_protocol','2',true) AS protocol) DELETE FROM walrus.file_manifest WHERE epoch = $1 AND (SELECT protocol='2' FROM authorized)")
        .bind(epoch.0)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM walrus.stream_manifest_group WHERE epoch = $1")
        .bind(epoch.0)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM walrus.stream_txn_publication WHERE epoch = $1")
        .bind(epoch.0)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM walrus.ddl_manifest WHERE epoch = $1")
        .bind(epoch.0)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM walrus.schema_registry WHERE epoch = $1")
        .bind(epoch.0)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn streamed_ddl_and_registry_share_manifest_rollback_and_replay_receipt() {
    let pool = pool().await;
    let epoch = EpochNo(900_006);
    let commit_lsn = Lsn::new(0x200);
    clear_stream_publication_epoch(&pool, epoch).await;

    let ddl = DdlRow {
        id: DdlId(0),
        epoch,
        source_audit_id: 70_001,
        source_schema: "public".into(),
        source_table: "atomic_stream".into(),
        c_lsn: commit_lsn,
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        schema_version: SchemaVersionNo(2),
        c_rel_oid: Some(42),
        c_columns: Some(serde_json::json!([{"name": "id", "type_oid": 23}])),
        c_dropped: None,
        c_ddl_text: Some("ALTER TABLE atomic_stream ADD COLUMN value text".into()),
    };
    let registry = RegistryRow {
        epoch,
        source_schema: "public".into(),
        source_table: "atomic_stream".into(),
        schema_version: SchemaVersionNo(2),
        descriptors: Vec::new(),
        columns: ddl.c_columns.clone().unwrap(),
    };
    let mut first = file(epoch, "atomic_stream", "0/200");
    first.schema_version = SchemaVersionNo(2);
    first.s3_uri = format!("s3://walrus/{epoch}/public/atomic_stream/duplicate.parquet");
    let second = first.clone();
    let mut publication = NewStreamCommitPublication {
        epoch,
        top_xid: 857,
        commit_lsn,
        commit_ts: "2026-09-02T12:34:56Z".parse::<UtcTimestamp>().unwrap(),
        ddl_rows: vec![ddl],
        registry_rows: vec![registry],
        files: vec![first, second],
    };

    publish_stream_commit(&pool, &publication)
        .await
        .expect_err("the duplicate object URI must roll back the whole control transaction");
    for relation in [
        "stream_txn_publication",
        "stream_manifest_group",
        "ddl_manifest",
        "schema_registry",
    ] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM walrus.{relation} WHERE epoch = $1"
        ))
        .bind(epoch.0)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "{relation} leaked out of the failed publication");
    }

    publication.files[1].s3_uri =
        format!("s3://walrus/{epoch}/public/atomic_stream/second.parquet");
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::Published
    );
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished,
        "lost-ACK replay must validate and reuse the complete durable receipt"
    );

    let mut changed_rows = publication.clone();
    changed_rows.files[0].row_count += 1;
    assert!(matches!(
        publish_stream_commit(&pool, &changed_rows).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));
    let mut changed_ddl = publication.clone();
    changed_ddl.ddl_rows[0].source_audit_id += 1;
    assert!(matches!(
        publish_stream_commit(&pool, &changed_ddl).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));
    let mut changed_xid = publication.clone();
    changed_xid.top_xid += 1;
    assert!(matches!(
        publish_stream_commit(&pool, &changed_xid).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));

    let child_ids = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM walrus.file_manifest WHERE epoch = $1 ORDER BY id",
    )
    .bind(epoch.0)
    .fetch_all(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE walrus.file_manifest SET status='failed' WHERE id=$1")
        .bind(child_ids[0])
        .execute(&pool)
        .await
        .unwrap();
    let child_ids = child_ids.into_iter().map(Into::into).collect::<Vec<_>>();
    assert_eq!(
        delete_claimed(&pool, &child_ids).await.unwrap(),
        0,
        "a complete ID list must not retire a group containing a failed child"
    );
    sqlx::query("UPDATE walrus.file_manifest SET status='ready' WHERE stream_group_id = (SELECT stream_group_id FROM walrus.file_manifest WHERE id=$1)")
        .bind(child_ids[0].0)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        delete_claimed(&pool, &child_ids).await.unwrap(),
        2,
        "the complete ready group retires atomically"
    );
    assert_eq!(
        publish_stream_commit(&pool, &publication).await.unwrap(),
        PublishStreamOutcome::AlreadyPublished,
        "lost-ACK replay remains exact after child queue rows have been retired"
    );
    assert!(matches!(
        publish_stream_commit(&pool, &changed_rows).await,
        Err(control::ControlError::StreamPublicationConflict { .. })
    ));

    let ddl_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.ddl_manifest WHERE epoch = $1 AND c_lsn = $2",
    )
    .bind(epoch.0)
    .bind(commit_lsn)
    .fetch_one(&pool)
    .await
    .unwrap();
    let registry_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.schema_registry WHERE epoch = $1 AND source_table = $2",
    )
    .bind(epoch.0)
    .bind("atomic_stream")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((ddl_count, registry_count), (1, 1));

    clear_stream_publication_epoch(&pool, epoch).await;
}
