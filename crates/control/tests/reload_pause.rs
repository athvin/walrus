#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for the loader-pause claim predicate.
//!
//! "Pausing is not claiming" (reload §2): a live reload of either persisted flavor in
//! `requested|exporting|export_complete|publishing` makes generic `claim_ready` return nothing for
//! THAT table while its `ready` rows accumulate; every other table claims normally. A fenced
//! publication-specific claim drains the frozen `[F,H]` set, and only terminal states lift the
//! generic pause. `resync` remains a compatibility spelling for the identical rebuild behavior.
#![cfg(feature = "integration")]

use common::{EpochNo, Lsn, SchemaVersionNo};
use control::reload::{
    self, ExportRangePlan, ExportSnapshot, ReloadFenceIdentity, ReloadFlavor, ReloadScope,
    SourceReloadRequest,
};
use control::{ManifestRow, NewManifestFile};
use control::{
    acquire_lease, claim_ready, connect, delete_claimed, ensure_checkpoint, insert_ready,
    max_ready_lsn_end, run_migrations,
};
use sqlx::postgres::{PgConnection, PgPool};
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

fn stream_file(epoch: EpochNo, table: &str, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/{lsn_end}.parquet"),
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

fn ids(rows: &[ManifestRow]) -> Vec<common::ManifestId> {
    rows.iter().map(|r| r.id).collect()
}

async fn finish_fenced(conn: &mut PgConnection, reload_id: common::ReloadId, h: Lsn) {
    let row = reload::get(&mut *conn, reload_id).await.unwrap().unwrap();
    let schema_version = row.schema_version.unwrap_or(SchemaVersionNo(1));
    let f = row.start_lsn.or(row.first_lsn).unwrap_or(h);
    let holder = row.lease_holder.clone().unwrap();
    let lease = row.exporter_lease(&holder).unwrap();
    let identity = ReloadFenceIdentity {
        request_id: row.source_request_id.or(row.parent_request_id),
        source_schema: &row.source_schema,
        source_table: &row.source_table,
        schema_version,
    };
    reload::record_start_fence(&mut *conn, reload_id, f, identity)
        .await
        .unwrap();
    let snapshot = format!("1:{}:", reload_id.0 + 2);
    reload::begin_export_plan(
        conn,
        &lease,
        f,
        schema_version,
        ExportSnapshot {
            identity: &snapshot,
            xmin: 1,
            xmax: reload_id.0 + 2,
        },
        &[ExportRangePlan {
            range_no: 0,
            full_scan: true,
            start_block: None,
            end_block: None,
        }],
    )
    .await
    .unwrap();
    reload::record_export_range(&mut *conn, &lease, 0, 0, 0)
        .await
        .unwrap();
    reload::seal_export(conn, &lease, f, schema_version)
        .await
        .unwrap();
    reload::record_end_marker(&mut *conn, reload_id, h, identity)
        .await
        .unwrap();
    reload::complete_export(&mut *conn, &lease, h)
        .await
        .unwrap();
}

#[tokio::test]
async fn live_rebuild_pauses_claims_for_that_table_only() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_001);

    insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/20"))
        .await
        .unwrap();
    let other = insert_ready(&mut *tx, &stream_file(epoch, "customers", "0/10"))
        .await
        .unwrap();

    // `requested` already pauses (the pause must cover the whole pre-export window)…
    reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "a requested rebuild pauses the table's claims"
    );
    // …and `exporting` keeps the pause.
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty()
    );

    // The OTHER table claims normally the whole time, and the paused table's backlog stays
    // visible (the lag gauge SHOULD grow during a pause by design).
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "customers", 100)
            .await
            .unwrap()),
        vec![other]
    );
    assert_eq!(
        max_ready_lsn_end(&mut *tx, epoch, "public", "orders")
            .await
            .unwrap(),
        Some("0/20".parse().unwrap()),
        "ready rows accumulate; nothing is lost or hidden from the backlog gauge"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn publication_claim_drains_through_h_before_complete_lifts_the_generic_pause() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_002);

    // Backlog inserted OUT of order, so the publication claim must return it in
    // `(lsn_end, id)` order.
    let c = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/30"))
        .await
        .unwrap();
    let a = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    let b = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/20"))
        .await
        .unwrap();
    let after_h = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/200"))
        .await
        .unwrap();

    let orders = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty()
    );

    // `export_complete` keeps the generic path paused. The loader must first acquire the table
    // fence and enter the publication-specific path.
    let h: Lsn = "0/100".parse().unwrap();
    finish_fenced(&mut tx, orders, h).await;
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "export_complete remains paused on the generic claim path"
    );
    assert_eq!(
        reload::active_rebuilds(&mut *tx, epoch)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![orders],
        "export_complete remains an active rebuild"
    );

    let owner = "loader-a";
    let lease = acquire_lease(&mut *tx, epoch, "public", "orders", owner, 60)
        .await
        .unwrap()
        .unwrap();
    ensure_checkpoint(&mut *tx, epoch, "public", "orders")
        .await
        .unwrap();
    let publication = reload::claim_publication(
        &mut *tx,
        epoch,
        "public",
        "orders",
        owner,
        lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "publishing remains paused on the generic claim path"
    );
    assert_eq!(
        reload::active_rebuilds(&mut *tx, epoch)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![orders],
        "publishing remains an active rebuild"
    );

    let through_h =
        reload::claim_publication_ready(&mut *tx, &publication, owner, lease.fencing_token, 100)
            .await
            .unwrap();
    assert_eq!(
        ids(&through_h),
        vec![a, b, c],
        "the publication claim drains only [F,H] in unchanged (lsn_end, id) order"
    );
    assert_eq!(delete_claimed(&mut *tx, &ids(&through_h)).await.unwrap(), 3);
    assert!(
        reload::publication_drained(&mut *tx, &publication, owner, lease.fencing_token)
            .await
            .unwrap()
    );
    assert!(
        reload::finish_publication(&mut *tx, &publication, owner, lease.fencing_token)
            .await
            .unwrap(),
        "the first finish transitions publishing to complete"
    );
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()),
        vec![after_h],
        "complete lifts the generic pause while preserving rows above H"
    );

    // `failed` equally lifts: a second table walks requested → exporting → failed.
    let f = insert_ready(&mut *tx, &stream_file(epoch, "customers", "0/10"))
        .await
        .unwrap();
    let cust = reload::request(&mut *tx, epoch, "public", "customers", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "customers", 100)
            .await
            .unwrap()
            .is_empty()
    );
    reload::fail(&mut tx, cust, "demo").await.unwrap();
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "customers", 100)
            .await
            .unwrap()),
        vec![f],
        "a failed reload lifts the pause (its own chunk files were purged; stream rows survive)"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn resync_alias_pauses_in_both_live_states() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_003);

    insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "the resync compatibility spelling pauses while requested"
    );
    // Flip to exporting and probe again to cover both live states.
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "the resync compatibility spelling remains paused while exporting"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn completed_resync_can_drain_before_a_queued_source_reload_starts() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(920_004);

    let manifest = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    let current = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();

    // Source-backed rows queue even behind an active legacy resync. Both spellings engage the
    // pause, so the current attempt must complete its fenced publication before the queued one
    // starts.
    let queued = reload::request_from_source(
        &mut *tx,
        &SourceReloadRequest {
            epoch,
            source_request_id: Uuid::from_u128(0x920_004),
            parent_request_id: None,
            scope: ReloadScope::Table,
            source_schema: "public",
            source_table: "orders",
            flavor: ReloadFlavor::Reload,
        },
    )
    .await
    .unwrap();
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "the queued reload pauses claims until the current export is ready to cut over"
    );

    finish_fenced(&mut tx, current, "0/20".parse().unwrap()).await;
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "export_complete keeps the generic claim paused"
    );
    assert!(
        reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "the queued reload still waits for the resync's loader completion"
    );

    let owner = "loader-a";
    let lease = acquire_lease(&mut *tx, epoch, "public", "orders", owner, 60)
        .await
        .unwrap()
        .unwrap();
    ensure_checkpoint(&mut *tx, epoch, "public", "orders")
        .await
        .unwrap();
    let publication = reload::claim_publication(
        &mut *tx,
        epoch,
        "public",
        "orders",
        owner,
        lease.fencing_token,
    )
    .await
    .unwrap()
    .unwrap();
    let claimed =
        reload::claim_publication_ready(&mut *tx, &publication, owner, lease.fencing_token, 100)
            .await
            .unwrap();
    assert_eq!(ids(&claimed), vec![manifest]);
    assert_eq!(delete_claimed(&mut *tx, &ids(&claimed)).await.unwrap(), 1);
    assert!(
        reload::publication_drained(&mut *tx, &publication, owner, lease.fencing_token)
            .await
            .unwrap()
    );
    reload::finish_publication(&mut *tx, &publication, owner, lease.fencing_token)
        .await
        .unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![queued]
    );

    tx.rollback().await.unwrap();
}
