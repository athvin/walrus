#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Compose-gated integration tests for the loader-pause claim predicate (PR 6.6).
//!
//! "Pausing is not claiming" (reload §2): a live `flavor='reload'` reload in
//! `requested|exporting` makes `claim_ready` return nothing for THAT table while its `ready`
//! rows accumulate; every other table claims normally; `export_complete` (and the terminal
//! states) lift the pause and the backlog drains in unchanged `(lsn_end, id)` order; `resync`
//! never pauses. Rolled-back transactions + unique epochs, like the manifest tests.
#![cfg(feature = "integration")]

use common::Lsn;
use control::reload::{self, ReloadFlavor};
use control::{claim_ready, connect, insert_ready, max_ready_lsn_end, run_migrations};
use control::{ManifestRow, NewManifestFile};
use sqlx::postgres::PgPool;

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

fn stream_file(epoch: i64, table: &str, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/{lsn_end}.parquet"),
        kind: control::ManifestKind::Stream,
        row_count: 1,
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: 1,
        reload_id: None,
    }
}

fn ids(rows: &[ManifestRow]) -> Vec<i64> {
    rows.iter().map(|r| r.id).collect()
}

#[tokio::test]
async fn live_rebuild_pauses_claims_for_that_table_only() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 920_001;

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
    assert!(claim_ready(&mut *tx, epoch, "public", "orders", 100)
        .await
        .unwrap()
        .is_empty());

    // The OTHER table claims normally the whole time, and the paused table's backlog stays
    // visible (the lag gauge SHOULD grow during a pause — PR 6.11 documents it).
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
async fn export_complete_and_terminal_states_lift_the_pause() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 920_002;

    // Backlog inserted OUT of order, so the lift must return it in (lsn_end, id) order.
    let c = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/30"))
        .await
        .unwrap();
    let a = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    let b = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/20"))
        .await
        .unwrap();

    let orders = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    assert!(claim_ready(&mut *tx, epoch, "public", "orders", 100)
        .await
        .unwrap()
        .is_empty());

    // export_complete lifts the pause — exactly then the loader MUST claim again to reach the
    // chunk files and trigger the rebuild (pausing through export_complete deadlocks, PR 6.7).
    let h: Lsn = "0/100".parse().unwrap();
    reload::complete_export(&mut *tx, orders, h).await.unwrap();
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()),
        vec![a, b, c],
        "the backlog drains in unchanged (lsn_end, id) order"
    );
    // `complete` keeps it lifted.
    reload::complete(&mut *tx, orders).await.unwrap();
    assert_eq!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .len(),
        3
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
    assert!(claim_ready(&mut *tx, epoch, "public", "customers", 100)
        .await
        .unwrap()
        .is_empty());
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
async fn resync_flavor_never_pauses() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 920_003;

    let id = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/10"))
        .await
        .unwrap();
    reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()),
        vec![id],
        "a requested resync pauses nothing (H3 — it merges over the LIVE mirror)"
    );
    // Claiming consumes nothing (the queue retires by DELETE, which the loader does after
    // appending) — flip to exporting and claim again to cover both live states.
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        ids(&claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()),
        vec![id],
        "an exporting resync pauses nothing either"
    );

    tx.rollback().await.unwrap();
}
