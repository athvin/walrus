#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect are test setup and assertions"
)]
//! Compose-gated integration coverage for the bootstrap reconciliation group.
#![cfg(feature = "integration")]

use common::{Lsn, SchemaVersionNo};
use control::reload::{self, ReloadFenceIdentity, ReloadFlavor, ReloadScope, SourceReloadRequest};
use control::{
    ControlError, ReplicationStatus, bump_bootstrap_epoch, bump_epoch, complete_bootstrap, connect,
    mark_total_restart, read_bootstrap_progress, read_current_epoch, run_migrations,
};
use sqlx::Connection;
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

#[tokio::test]
async fn bootstrap_epoch_atomically_binds_inventory_and_enforces_its_size() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let request_id = Uuid::from_u128(0xb007_0001);
    let targets = serde_json::json!([
        {"schema": "public", "table": "customers"},
        {"schema": "public", "table": "orders"}
    ]);
    let before = read_current_epoch(&mut *tx).await.unwrap();
    let expected_current = before.as_ref().map(|state| state.epoch);

    // The migration constraint rejects a count that disagrees with the frozen target array. Use a
    // savepoint because a real CHECK violation aborts its surrounding transaction.
    {
        let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
        let err = bump_bootstrap_epoch(
            &mut *savepoint,
            expected_current,
            "walrus-bootstrap-invalid",
            "0/700".parse().unwrap(),
            request_id,
            1,
            &targets,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, ControlError::CheckViolation { .. }),
            "target-count mismatch must be a typed invariant failure: {err:?}"
        );
        savepoint.rollback().await.unwrap();
    }
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap(),
        before,
        "a rejected bootstrap must not leave a partially-created epoch"
    );

    let created_lsn: Lsn = "0/800".parse().unwrap();
    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-atomic",
        created_lsn,
        request_id,
        2,
        &targets,
    )
    .await
    .unwrap()
    .expect("the observed current epoch still wins the compare-and-set");

    let current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .expect("the new bootstrap epoch is current");
    assert_eq!(current.epoch, epoch);
    assert_eq!(current.slot_name, "walrus-bootstrap-atomic");
    assert_eq!(current.created_lsn, created_lsn);
    assert_eq!(current.status, ReplicationStatus::Bootstrapping);

    let progress = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .expect("a bootstrapping epoch exposes its bound group");
    assert_eq!(progress.request_id, request_id);
    assert_eq!(progress.expected_tables, 2);
    assert_eq!(progress.targets, targets);
    assert_eq!(progress.children, 0);
    assert_eq!(progress.complete, 0);
    assert_eq!(progress.failed, 0);
    assert!(!progress.is_ready());

    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "an inventory binding alone cannot promote an epoch"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn total_restart_intent_is_idempotent_and_guarded_by_the_current_epoch() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let predecessor = bump_epoch(
        &mut *tx,
        "walrus-restart-predecessor",
        "0/810".parse().unwrap(),
        ReplicationStatus::Streaming,
    )
    .await
    .unwrap();
    let current = bump_epoch(
        &mut *tx,
        "walrus-restart-current",
        "0/820".parse().unwrap(),
        ReplicationStatus::Streaming,
    )
    .await
    .unwrap();

    assert!(
        !mark_total_restart(&mut *tx, predecessor).await.unwrap(),
        "a stale observer must lose the guard instead of arming an old generation"
    );
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap().unwrap().status,
        ReplicationStatus::Streaming
    );

    assert!(mark_total_restart(&mut *tx, current).await.unwrap());
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap().unwrap().status,
        ReplicationStatus::TotalRestart
    );
    assert!(
        mark_total_restart(&mut *tx, current).await.unwrap(),
        "a crash recovery must be able to re-arm the same current epoch"
    );

    let empty_targets = serde_json::json!([]);
    let stale = bump_bootstrap_epoch(
        &mut *tx,
        Some(predecessor),
        "walrus-restart-stale",
        "0/830".parse().unwrap(),
        Uuid::from_u128(0xb007_0010),
        0,
        &empty_targets,
    )
    .await
    .unwrap();
    assert_eq!(
        stale, None,
        "a stale observer cannot open a successor to a non-current epoch"
    );

    let successor = bump_bootstrap_epoch(
        &mut *tx,
        Some(current),
        "walrus-restart-successor",
        "0/840".parse().unwrap(),
        Uuid::from_u128(0xb007_0011),
        0,
        &empty_targets,
    )
    .await
    .unwrap()
    .expect("the exact current epoch can open one successor");
    let duplicate = bump_bootstrap_epoch(
        &mut *tx,
        Some(current),
        "walrus-restart-duplicate",
        "0/850".parse().unwrap(),
        Uuid::from_u128(0xb007_0012),
        0,
        &empty_targets,
    )
    .await
    .unwrap();
    assert_eq!(
        duplicate, None,
        "the same observed predecessor cannot be used to create e3 after e2 wins"
    );
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap().unwrap().epoch,
        successor
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn bootstrap_promotes_only_after_every_bound_child_completes() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let request_id = Uuid::from_u128(0xb007_0002);
    let targets = serde_json::json!([
        {"schema": "public", "table": "customers"},
        {"schema": "public", "table": "orders"}
    ]);
    let expected_current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .map(|state| state.epoch);
    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-progress",
        "0/900".parse().unwrap(),
        request_id,
        2,
        &targets,
    )
    .await
    .unwrap()
    .expect("the observed current epoch still wins the compare-and-set");

    let orders = SourceReloadRequest {
        epoch,
        source_request_id: request_id,
        parent_request_id: Some(request_id),
        scope: ReloadScope::AllPublished,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let customers = SourceReloadRequest {
        source_table: "customers",
        ..orders
    };

    let orders_id = reload::request_from_source(&mut *tx, &orders)
        .await
        .unwrap();
    let progress = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((progress.children, progress.complete), (1, 0));
    assert!(!progress.is_ready());
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap()
    );

    let customers_id = reload::request_from_source(&mut *tx, &customers)
        .await
        .unwrap();
    let progress = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((progress.children, progress.complete), (2, 0));
    assert!(!progress.is_ready());

    let claimed = reload::claim_requested(&mut *tx, epoch, "bootstrap-sink", 60, 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);

    let first_f: Lsn = "0/A00".parse().unwrap();
    let first_h: Lsn = "0/A80".parse().unwrap();
    let orders_fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "orders",
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, orders_id, first_f, orders_fence)
        .await
        .unwrap();
    reload::record_end_marker(&mut *tx, orders_id, first_h, orders_fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, orders_id, first_h)
        .await
        .unwrap();
    reload::complete(&mut *tx, orders_id).await.unwrap();

    let progress = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((progress.children, progress.complete), (2, 1));
    assert!(!progress.is_ready());
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "one completed child must not publish a partial generation"
    );

    let second_f: Lsn = "0/B00".parse().unwrap();
    let second_h: Lsn = "0/B80".parse().unwrap();
    let customers_fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "customers",
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, customers_id, second_f, customers_fence)
        .await
        .unwrap();
    reload::record_end_marker(&mut *tx, customers_id, second_h, customers_fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, customers_id, second_h)
        .await
        .unwrap();
    reload::complete(&mut *tx, customers_id).await.unwrap();

    let ready = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((ready.children, ready.complete, ready.failed), (2, 2, 0));
    assert!(ready.is_ready());
    assert!(
        !complete_bootstrap(&mut *tx, epoch, Uuid::from_u128(0xdead_beef))
            .await
            .unwrap(),
        "a different source request cannot promote the generation"
    );
    assert!(
        complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap()
    );

    let current = read_current_epoch(&mut *tx).await.unwrap().unwrap();
    assert_eq!(current.epoch, epoch);
    assert_eq!(current.status, ReplicationStatus::Streaming);
    assert!(
        read_bootstrap_progress(&mut *tx, epoch)
            .await
            .unwrap()
            .is_none(),
        "a promoted generation no longer reports pending bootstrap work"
    );
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap(),
        "promotion is an idempotent guarded transition"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn bootstrap_progress_uses_the_latest_ddl_restart_attempt_per_target() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let request_id = Uuid::from_u128(0xb007_0003);
    let targets = serde_json::json!([{"schema": "public", "table": "orders"}]);
    let expected_current = read_current_epoch(&mut *tx)
        .await
        .unwrap()
        .map(|state| state.epoch);
    let epoch = bump_bootstrap_epoch(
        &mut *tx,
        expected_current,
        "walrus-bootstrap-restart",
        "0/C00".parse().unwrap(),
        request_id,
        1,
        &targets,
    )
    .await
    .unwrap()
    .expect("the observed current epoch still wins the compare-and-set");
    let child = SourceReloadRequest {
        epoch,
        source_request_id: request_id,
        parent_request_id: Some(request_id),
        scope: ReloadScope::AllPublished,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let predecessor_id = reload::request_from_source(&mut *tx, &child).await.unwrap();
    reload::claim_requested(&mut *tx, epoch, "bootstrap-sink", 60, 1)
        .await
        .unwrap();
    let predecessor = reload::get(&mut *tx, predecessor_id)
        .await
        .unwrap()
        .unwrap();

    let successor_id = reload::restart_for_ddl(&mut tx, &predecessor, SchemaVersionNo(2), 3)
        .await
        .unwrap()
        .expect("the restart budget permits a successor");
    assert!(successor_id > predecessor_id);
    let predecessor = reload::get(&mut *tx, predecessor_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(predecessor.status, reload::ReloadStatus::Failed);
    let successor = reload::get(&mut *tx, successor_id).await.unwrap().unwrap();
    assert_eq!(successor.parent_request_id, Some(request_id));
    assert_eq!(successor.scope, ReloadScope::AllPublished);

    let restarted = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (restarted.children, restarted.complete, restarted.failed),
        (1, 0, 0),
        "the failed predecessor is history; only its live successor represents this target"
    );
    assert!(
        !complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap()
    );

    let f: Lsn = "0/D00".parse().unwrap();
    let h: Lsn = "0/D80".parse().unwrap();
    let successor_fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "orders",
        schema_version: SchemaVersionNo(2),
    };
    reload::record_start_fence(&mut *tx, successor_id, f, successor_fence)
        .await
        .unwrap();
    reload::record_end_marker(&mut *tx, successor_id, h, successor_fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, successor_id, h)
        .await
        .unwrap();
    reload::complete(&mut *tx, successor_id).await.unwrap();

    let complete = read_bootstrap_progress(&mut *tx, epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (complete.children, complete.complete, complete.failed),
        (1, 1, 0)
    );
    assert!(complete.is_ready());
    assert!(
        complete_bootstrap(&mut *tx, epoch, request_id)
            .await
            .unwrap()
    );
    assert_eq!(
        read_current_epoch(&mut *tx).await.unwrap().unwrap().status,
        ReplicationStatus::Streaming
    );

    tx.rollback().await.unwrap();
}
