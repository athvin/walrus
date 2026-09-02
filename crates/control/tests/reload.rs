#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for the `table_reload` state machine.
//!
//! Same discipline as the manifest tests: every test runs inside a rolled-back transaction and
//! namespaces its rows by a unique `epoch`, so runs are isolated and idempotent. Statements that
//! provoke a real SQL error (the duplicate-request unique violation) run under a nested
//! savepoint, because a failed statement aborts the enclosing Postgres transaction.
#![cfg(feature = "integration")]

use common::{EpochNo, FailureClass, Lsn, ReloadId, SchemaVersionNo};
use control::reload::{
    self, ReloadFenceIdentity, ReloadFlavor, ReloadMarkerKind, ReloadScope, ReloadStatus,
    SourceReloadRequest,
};
use control::{ControlError, NewManifestFile, claim_ready, connect, insert_ready, run_migrations};
use sqlx::Connection;
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

/// A staged reload chunk file: `kind='reload'` carrying its `reload_id` (stamped `lsn = L_i`).
fn chunk_file(epoch: EpochNo, table: &str, reload_id: ReloadId, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/reload-{reload_id}-{lsn_end}.parquet"),
        kind: control::ManifestKind::Reload,
        row_count: 1,
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: SchemaVersionNo(1),
        reload_id: Some(reload_id),
    }
}

/// `lease_expiry` as a comparable number — the model omits the column by design (every time
/// comparison lives in SQL), so tests that care probe it directly.
async fn expiry_epoch(ex: impl sqlx::PgExecutor<'_>, reload_id: ReloadId) -> f64 {
    sqlx::query_scalar::<_, f64>(
        "SELECT extract(epoch FROM lease_expiry)::float8
         FROM walrus.table_reload WHERE reload_id = $1",
    )
    .bind(reload_id.0)
    .fetch_one(ex)
    .await
    .unwrap()
}

/// Complete an ordinary test attempt through the same explicit F/baseline/H protocol used by the
/// exporter. Tests that exercise invalid or mismatched markers call the lower-level functions
/// directly instead.
async fn finish_fenced(conn: &mut PgConnection, reload_id: ReloadId, h: Lsn) {
    let row = reload::get(&mut *conn, reload_id).await.unwrap().unwrap();
    let schema_version = row.schema_version.unwrap_or(SchemaVersionNo(1));
    let f = row.start_lsn.or(row.first_lsn).unwrap_or(h);
    let identity = ReloadFenceIdentity {
        request_id: row.source_request_id.or(row.parent_request_id),
        source_schema: &row.source_schema,
        source_table: &row.source_table,
        schema_version,
    };
    reload::record_start_fence(&mut *conn, reload_id, f, identity)
        .await
        .unwrap();
    reload::record_end_marker(&mut *conn, reload_id, h, identity)
        .await
        .unwrap();
    reload::complete_export(&mut *conn, reload_id, h)
        .await
        .unwrap();
}

/// An ordinary stream file — `reload_id` stays NULL, exactly like every pre-6.1 row.
fn stream_file(epoch: EpochNo, table: &str, lsn_end: &str) -> NewManifestFile {
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
        schema_version: SchemaVersionNo(1),
        reload_id: None,
    }
}

#[tokio::test]
async fn full_status_walk_and_duplicate_request_rejected() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_001);

    let id = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();

    // A duplicate request hits the `table_reload_one_live` partial unique index and surfaces as
    // the TYPED already-in-progress error — probed under a savepoint, since the unique violation
    // aborts its (sub)transaction.
    {
        let mut sp = Connection::begin(&mut *tx).await.unwrap();
        let err = reload::request(&mut *sp, epoch, "public", "orders", ReloadFlavor::Reload)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ControlError::ReloadInProgress { ref schema, ref table }
                if schema == "public" && table == "orders"),
            "expected the typed ReloadInProgress, got: {err:?}"
        );
        assert!(
            err.is_terminal(),
            "retrying a duplicate request never helps"
        );
        sp.rollback().await.unwrap();
    }

    // A reload on a DIFFERENT table is untouched by the index.
    let other = reload::request(&mut *tx, epoch, "public", "customers", ReloadFlavor::Resync)
        .await
        .unwrap();
    assert!(other > id, "bigserial: monotonic ids");

    // The pause engages at REQUEST time, not claim time, for both persisted spellings.
    let rebuilds = reload::active_rebuilds(&mut *tx, epoch).await.unwrap();
    assert_eq!(
        rebuilds.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![id, other],
        "reload and resync are both active rebuilds while requested"
    );
    assert_eq!(rebuilds[0].status, ReloadStatus::Requested);
    assert_eq!(rebuilds[1].status, ReloadStatus::Requested);
    assert_eq!(rebuilds[1].flavor, ReloadFlavor::Resync);

    // Claim honors the batch cap and hands out the OLDEST request first.
    let claimed = reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1, "limit=1 claims exactly one row");
    let orders = &claimed[0];
    assert_eq!(orders.reload_id, id, "oldest reload_id first");
    assert_eq!(orders.status, ReloadStatus::Exporting);
    assert_eq!(orders.flavor, ReloadFlavor::Reload);
    assert_eq!(orders.lease_holder.as_deref(), Some("sink-a"));
    assert_eq!(orders.chunk_no, 0);
    assert_eq!(orders.first_lsn, None);
    assert_eq!(orders.source_request_id, None);
    let orders_fence_request_id = orders
        .parent_request_id
        .expect("direct requests persist a private source-fence namespace");

    // The claim SET a real lease: expiry sits in the future. The model omits lease_expiry by
    // design (no clock in Rust), so probe it with SQL — the loader's shutdown test's idiom.
    let now_epoch: f64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::float8")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let exp_claim = expiry_epoch(&mut *tx, id).await;
    assert!(
        exp_claim > now_epoch,
        "claim set lease_expiry in the future"
    );

    // The one_live index guards the WHOLE non-terminal breadth, not just `requested`: a
    // duplicate request against the now-`exporting` row is rejected identically.
    {
        let mut sp = Connection::begin(&mut *tx).await.unwrap();
        let err = reload::request(&mut *sp, epoch, "public", "orders", ReloadFlavor::Reload)
            .await
            .unwrap_err();
        assert!(matches!(err, ControlError::ReloadInProgress { .. }));
        sp.rollback().await.unwrap();
    }

    // The second requested row (the resync) is still there; a cap above the queue drains it.
    let rest = reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        rest.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![other]
    );
    assert_eq!(rest[0].flavor, ReloadFlavor::Resync);
    let other_fence_request_id = rest[0]
        .parent_request_id
        .expect("every direct request persists a source-fence namespace");
    assert_ne!(
        other_fence_request_id, orders_fence_request_id,
        "direct requests must not derive their durable namespace from a reusable bigint id"
    );

    // Nothing left in `requested`: a latecomer gets an empty Vec, not an error. (The
    // cross-connection SKIP LOCKED race is exercised in
    // `concurrent_claimers_partition_the_queue_via_skip_locked` below.)
    let raced = reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
        .await
        .unwrap();
    assert!(raced.is_empty());

    // The holder renews — and the lease observably extends (same frozen now(), bigger ttl);
    // a phantom does not.
    assert!(
        reload::renew_lease(&mut *tx, id, "sink-a", 3600)
            .await
            .unwrap()
    );
    let exp_renewed = expiry_epoch(&mut *tx, id).await;
    assert!(
        exp_renewed > exp_claim + 3000.0,
        "renew pushed lease_expiry out by the new ttl"
    );
    assert!(
        !reload::renew_lease(&mut *tx, id, "sink-zombie", 60)
            .await
            .unwrap()
    );

    // Freeze F + schema before any chunk. Every baseline chunk must carry that exact F; a stale
    // producer cannot smuggle a per-chunk Lᵢ from a different source snapshot into this attempt.
    let l1: Lsn = "0/100".parse().unwrap();
    let l2: Lsn = "0/200".parse().unwrap();
    reload::record_start_fence(
        &mut *tx,
        id,
        l1,
        ReloadFenceIdentity {
            request_id: Some(orders_fence_request_id),
            source_schema: "public",
            source_table: "orders",
            schema_version: SchemaVersionNo(7),
        },
    )
    .await
    .unwrap();
    reload::advance_cursor(
        &mut *tx,
        id,
        1,
        &serde_json::json!([42]),
        l1,
        SchemaVersionNo(7),
    )
    .await
    .unwrap();
    let err = reload::advance_cursor(
        &mut *tx,
        id,
        2,
        &serde_json::json!([84]),
        l2,
        SchemaVersionNo(7),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    reload::advance_cursor(
        &mut *tx,
        id,
        2,
        &serde_json::json!([84]),
        l1,
        SchemaVersionNo(7),
    )
    .await
    .unwrap();

    // A mismatched schema_version is ASSERTED, not swallowed: every attempt is single-schema by
    // construction (H9), so version 9 mid-attempt means the export engine missed a DDL restart.
    let err = reload::advance_cursor(
        &mut *tx,
        id,
        3,
        &serde_json::json!([99]),
        l1,
        SchemaVersionNo(9),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.chunk_no, 2, "the rejected mismatch advanced nothing");
    assert_eq!(row.cursor_pk, Some(serde_json::json!([84])));
    assert_eq!(row.first_lsn, Some(l1), "first_lsn is frozen on chunk 1");
    assert_eq!(
        row.schema_version,
        Some(SchemaVersionNo(7)),
        "schema_version is frozen on chunk 1"
    );

    // exporting → export_complete records the final watermark H…
    let h: Lsn = "0/300".parse().unwrap();
    finish_fenced(&mut tx, id, h).await;
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::ExportComplete);
    assert_eq!(row.final_lsn, Some(h));
    assert_eq!(
        reload::active_rebuilds(&mut *tx, epoch)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![other],
        "the orders pause lifts at export_complete; the exporting resync alias on customers remains active"
    );

    // …but the one_live index still guards `export_complete` (non-terminal): no new request yet.
    {
        let mut sp = Connection::begin(&mut *tx).await.unwrap();
        let err = reload::request(&mut *sp, epoch, "public", "orders", ReloadFlavor::Reload)
            .await
            .unwrap_err();
        assert!(matches!(err, ControlError::ReloadInProgress { .. }));
        sp.rollback().await.unwrap();
    }

    // …and the loader finishes the walk. Terminal ⇒ the table is requestable again.
    reload::complete(&mut *tx, id).await.unwrap();
    let again = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    assert!(
        again > id,
        "a fresh attempt gets a fresh, larger reload_id (latest wins = max)"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_request_is_idempotent_per_table_and_supports_fanout() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_006);
    let source_request_id = Uuid::from_u128(0x100);
    let parent_request_id = Some(Uuid::from_u128(0x200));

    let orders = SourceReloadRequest {
        epoch,
        source_request_id,
        parent_request_id,
        scope: ReloadScope::AllPublished,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let orders_id = reload::request_from_source(&mut *tx, &orders)
        .await
        .unwrap();
    let replayed_id = reload::request_from_source(&mut *tx, &orders)
        .await
        .unwrap();
    assert_eq!(
        replayed_id, orders_id,
        "WAL redelivery must return the original attempt"
    );

    let customers = SourceReloadRequest {
        source_table: "customers",
        ..orders
    };
    let customers_id = reload::request_from_source(&mut *tx, &customers)
        .await
        .unwrap();
    assert_ne!(
        customers_id, orders_id,
        "one all-published event fans out to distinct table attempts"
    );

    let row = reload::get(&mut *tx, orders_id).await.unwrap().unwrap();
    assert_eq!(row.source_request_id, Some(source_request_id));
    assert_eq!(row.parent_request_id, parent_request_id);
    assert_eq!(row.scope, ReloadScope::AllPublished);

    let changed_payload = SourceReloadRequest {
        scope: ReloadScope::Table,
        ..orders
    };
    let err = reload::request_from_source(&mut *tx, &changed_payload)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ControlError::SourceRequestConflict { request_id, ref schema, ref table }
            if request_id == source_request_id && schema == "public" && table == "orders"
    ));

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_restart_successors_keep_the_source_uuid_when_parent_differs() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_016);

    for (table, source_request_id, parent_request_id, pristine) in [
        (
            "source_parent_ddl",
            Uuid::from_u128(0x1601),
            Uuid::from_u128(0x2601),
            false,
        ),
        (
            "source_parent_pristine",
            Uuid::from_u128(0x1602),
            Uuid::from_u128(0x2602),
            true,
        ),
    ] {
        let request = SourceReloadRequest {
            epoch,
            source_request_id,
            parent_request_id: Some(parent_request_id),
            scope: ReloadScope::Table,
            source_schema: "public",
            source_table: table,
            flavor: ReloadFlavor::Reload,
        };
        let old_id = reload::request_from_source(&mut *tx, &request)
            .await
            .unwrap();
        let claimed = reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 1)
            .await
            .unwrap();
        assert_eq!(
            claimed.iter().map(|row| row.reload_id).collect::<Vec<_>>(),
            vec![old_id]
        );
        let old = reload::get(&mut *tx, old_id).await.unwrap().unwrap();

        let successor_id = if pristine {
            reload::restart_pristine_adoption(&mut tx, &old)
                .await
                .unwrap()
        } else {
            reload::restart_for_ddl(&mut tx, &old, SchemaVersionNo(2), 3)
                .await
                .unwrap()
                .expect("DDL restart is below the cap")
        };
        let successor = reload::get(&mut *tx, successor_id).await.unwrap().unwrap();
        assert_eq!(successor.source_request_id, None);
        assert_eq!(
            successor.parent_request_id,
            Some(source_request_id),
            "a successor carries the original source event UUID as its fence namespace"
        );
        assert_ne!(
            successor.parent_request_id,
            Some(parent_request_id),
            "the correlation parent must not replace the source event identity"
        );

        let wrong_identity = ReloadFenceIdentity {
            request_id: Some(parent_request_id),
            source_schema: "public",
            source_table: table,
            schema_version: SchemaVersionNo(2),
        };
        let error =
            reload::record_start_fence(&mut *tx, successor_id, Lsn::new(0x100), wrong_identity)
                .await
                .unwrap_err();
        assert!(matches!(error, ControlError::ReloadTransition { .. }));

        reload::record_start_fence(
            &mut *tx,
            successor_id,
            Lsn::new(0x100),
            ReloadFenceIdentity {
                request_id: Some(source_request_id),
                ..wrong_identity
            },
        )
        .await
        .unwrap();
    }

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_requests_queue_fifo_until_the_current_attempt_is_terminal() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_008);

    let first_request = SourceReloadRequest {
        epoch,
        source_request_id: Uuid::from_u128(0x801),
        parent_request_id: None,
        scope: ReloadScope::Table,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let first = reload::request_from_source(&mut *tx, &first_request)
        .await
        .unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![first]
    );

    // Unlike direct requests, a new source UUID is accepted while this table is exporting.
    let second_request = SourceReloadRequest {
        source_request_id: Uuid::from_u128(0x802),
        ..first_request
    };
    let second = reload::request_from_source(&mut *tx, &second_request)
        .await
        .unwrap();
    assert!(second > first);
    assert!(
        reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "the next source request must wait while the first is exporting"
    );

    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let first_fence = ReloadFenceIdentity {
        request_id: Some(first_request.source_request_id),
        source_schema: first_request.source_schema,
        source_table: first_request.source_table,
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, first, f, first_fence)
        .await
        .unwrap();
    reload::record_end_marker(&mut *tx, first, h, first_fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, first, h).await.unwrap();

    // A third request is also durable while the current attempt waits on loader cutover.
    let third_request = SourceReloadRequest {
        source_request_id: Uuid::from_u128(0x803),
        ..first_request
    };
    let third = reload::request_from_source(&mut *tx, &third_request)
        .await
        .unwrap();
    assert!(third > second);
    assert!(
        reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "export_complete remains active until the loader publishes it"
    );

    reload::complete(&mut *tx, first).await.unwrap();
    let claimed = reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        claimed.iter().map(|row| row.reload_id).collect::<Vec<_>>(),
        vec![second],
        "only the oldest queued request for a table may claim"
    );
    assert_eq!(
        reload::get(&mut *tx, third).await.unwrap().unwrap().status,
        ReloadStatus::Requested
    );

    // A failed active attempt is terminal too, so the next FIFO entry may start.
    reload::fail(&mut tx, second, "test failure").await.unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "sink-c", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![third]
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn current_export_complete_cuts_over_despite_a_later_source_request() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_009);
    let manifest = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/180"))
        .await
        .unwrap();

    let current_request = SourceReloadRequest {
        epoch,
        source_request_id: Uuid::from_u128(0x901),
        parent_request_id: None,
        scope: ReloadScope::Table,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let current = reload::request_from_source(&mut *tx, &current_request)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    let later_request = SourceReloadRequest {
        source_request_id: Uuid::from_u128(0x902),
        ..current_request
    };
    let later = reload::request_from_source(&mut *tx, &later_request)
        .await
        .unwrap();

    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "the exporting attempt and queued request keep Phase A paused"
    );

    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let current_fence = ReloadFenceIdentity {
        request_id: Some(current_request.source_request_id),
        source_schema: current_request.source_schema,
        source_table: current_request.source_table,
        schema_version: SchemaVersionNo(1),
    };
    reload::record_start_fence(&mut *tx, current, f, current_fence)
        .await
        .unwrap();
    reload::record_end_marker(&mut *tx, current, h, current_fence)
        .await
        .unwrap();
    reload::complete_export(&mut *tx, current, h).await.unwrap();

    assert!(
        reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "the later request cannot start before the current cutover completes"
    );
    assert_eq!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![manifest],
        "export_complete must override the queued request's pause long enough to cut over"
    );

    reload::complete(&mut *tx, current).await.unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![later]
    );
    assert!(
        claim_ready(&mut *tx, epoch, "public", "orders", 100)
            .await
            .unwrap()
            .is_empty(),
        "once the queued request starts, it owns the normal pause"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn data_free_markers_drive_an_empty_resync_alias_rebuild() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_007);
    let request = SourceReloadRequest {
        epoch,
        source_request_id: Uuid::from_u128(0x300),
        parent_request_id: None,
        scope: ReloadScope::Table,
        source_schema: "public",
        source_table: "empty_table",
        flavor: ReloadFlavor::Resync,
    };
    let reload_id = reload::request_from_source(&mut *tx, &request)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 1)
        .await
        .unwrap();

    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let schema_version = SchemaVersionNo(9);
    let fence = ReloadFenceIdentity {
        request_id: Some(request.source_request_id),
        source_schema: request.source_schema,
        source_table: request.source_table,
        schema_version,
    };

    let err = reload::complete_export(&mut *tx, reload_id, h)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::record_end_marker(&mut *tx, reload_id, h, fence)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    let err = reload::record_start_fence(
        &mut *tx,
        reload_id,
        f,
        ReloadFenceIdentity {
            request_id: Some(Uuid::from_u128(0xdead)),
            ..fence
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ControlError::ReloadTransition { .. }),
        "an old source request must not attach its fence to a reused bigint reload_id"
    );

    reload::record_start_fence(&mut *tx, reload_id, f, fence)
        .await
        .unwrap();
    reload::record_start_fence(&mut *tx, reload_id, f, fence)
        .await
        .unwrap();
    let row = reload::get(&mut *tx, reload_id).await.unwrap().unwrap();
    assert_eq!(row.start_lsn, Some(f));
    assert_eq!(row.first_lsn, None, "no data chunk was needed");
    assert_eq!(row.chunk_no, 0, "the empty export wrote no data file");
    assert_eq!(row.schema_version, Some(schema_version));
    assert_eq!(
        reload::reload_supersede_floor(&mut *tx, epoch, "public", "empty_table")
            .await
            .unwrap(),
        Some(f),
        "the explicit fence, not a first file, is authoritative"
    );

    let err = reload::record_end_marker(&mut *tx, reload_id, "0/FF".parse().unwrap(), fence)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::record_end_marker(
        &mut *tx,
        reload_id,
        h,
        ReloadFenceIdentity {
            schema_version: SchemaVersionNo(schema_version.0 + 1),
            ..fence
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    reload::record_end_marker(&mut *tx, reload_id, h, fence)
        .await
        .unwrap();
    reload::record_end_marker(&mut *tx, reload_id, h, fence)
        .await
        .unwrap();
    let markers = reload::read_markers(&mut *tx, reload_id).await.unwrap();
    assert_eq!(markers.len(), 2);
    assert_eq!(markers[0].kind, ReloadMarkerKind::Baseline);
    assert_eq!(markers[0].lsn, f);
    assert_eq!(markers[0].schema_version, schema_version);
    assert_eq!(markers[1].kind, ReloadMarkerKind::End);
    assert_eq!(markers[1].lsn, h);
    assert!(
        reload::ready_rebuild(&mut *tx, epoch, "public", "empty_table")
            .await
            .unwrap()
            .is_none(),
        "durable markers do not skip the exporting → export_complete transition"
    );

    reload::complete_export(&mut *tx, reload_id, h)
        .await
        .unwrap();
    let row = reload::get(&mut *tx, reload_id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::ExportComplete);
    assert_eq!(row.final_lsn, Some(h));
    reload::record_start_fence(&mut *tx, reload_id, f, fence)
        .await
        .expect("an exact start-event replay remains idempotent after the status flip");
    reload::record_end_marker(&mut *tx, reload_id, h, fence)
        .await
        .expect("an exact end-event replay remains idempotent after the status flip");
    let ready = reload::ready_rebuild(&mut *tx, epoch, "public", "empty_table")
        .await
        .unwrap()
        .expect("marker-only empty reload is discoverable without a manifest file");
    assert_eq!(ready.reload_id, reload_id);
    assert_eq!(ready.chunk_no, 0);
    assert_eq!(ready.flavor, ReloadFlavor::Resync);
    reload::complete(&mut *tx, reload_id).await.unwrap();
    reload::record_start_fence(&mut *tx, reload_id, f, fence)
        .await
        .expect("an exact start-event replay remains idempotent after completion");

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn wrong_state_transition_changes_zero_rows() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_002);

    let id = reload::request(&mut *tx, epoch, "public", "t", ReloadFlavor::Reload)
        .await
        .unwrap();
    let h: Lsn = "0/300".parse().unwrap();

    // Every jump out of `requested` that isn't a claim is illegal — the guarded UPDATE matches
    // zero rows and errors, and the row is provably untouched. (No savepoints needed: a
    // zero-row UPDATE is not a SQL error, so the transaction stays healthy.)
    let err = reload::complete_export(&mut *tx, id, h).await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { reload_id, .. } if reload_id == id));
    let err = reload::complete(&mut *tx, id).await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::advance_cursor(
        &mut *tx,
        id,
        1,
        &serde_json::json!([1]),
        h,
        SchemaVersionNo(1),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let err = reload::fail(&mut tx, id, "nope").await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        ReloadStatus::Requested,
        "illegal jumps changed nothing"
    );
    assert_eq!(row.error, None);

    // Claim it, then try to skip export_complete: exporting → complete is equally illegal.
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();
    let err = reload::complete(&mut *tx, id).await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    // An out-of-order cursor advance (chunk 2 before chunk 1) is a loud error too.
    let err = reload::advance_cursor(
        &mut *tx,
        id,
        2,
        &serde_json::json!([1]),
        h,
        SchemaVersionNo(1),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    // Walk to terminal, then confirm terminal states reject everything.
    finish_fenced(&mut tx, id, h).await;
    reload::complete(&mut *tx, id).await.unwrap();
    let err = reload::fail(&mut tx, id, "too late").await.unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::Complete);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn release_claim_returns_the_row_to_the_queue() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_005);

    let id = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();

    // A phantom can't release someone else's claim; releasing a `requested` row is a no-op too.
    assert!(
        !reload::release_claim(&mut *tx, id, "sink-zombie")
            .await
            .unwrap()
    );

    // The claimant releases: back to `requested`, lease cleared, immediately re-claimable — the
    // controller's un-claim path for infra failures between claim and exporter spawn.
    assert!(reload::release_claim(&mut *tx, id, "sink-a").await.unwrap());
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::Requested);
    assert_eq!(row.lease_holder, None);
    assert!(!reload::release_claim(&mut *tx, id, "sink-a").await.unwrap());

    let reclaimed = reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        reclaimed.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![id],
        "a released claim is re-claimable"
    );
    let fence_request_id = reclaimed[0]
        .parent_request_id
        .expect("direct requests persist a private source-fence namespace");

    reload::record_start_fence(
        &mut *tx,
        id,
        Lsn::new(100),
        control::ReloadFenceIdentity {
            request_id: Some(fence_request_id),
            source_schema: "public",
            source_table: "orders",
            schema_version: SchemaVersionNo(1),
        },
    )
    .await
    .unwrap();
    assert!(
        !reload::release_claim(&mut *tx, id, "sink-b").await.unwrap(),
        "a fenced attempt must keep its snapshot ownership semantics"
    );
    assert_eq!(
        reload::get(&mut *tx, id).await.unwrap().unwrap().status,
        ReloadStatus::Exporting
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn fail_purges_this_reloads_manifest_rows_only() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_003);

    // Two live reloads on different tables, both exporting.
    let r1 = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    let r2 = reload::request(&mut *tx, epoch, "public", "customers", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();

    // Staged chunk files for both reloads, plus an ordinary stream file (reload_id IS NULL).
    insert_ready(&mut *tx, &chunk_file(epoch, "orders", r1, "0/10"))
        .await
        .unwrap();
    insert_ready(&mut *tx, &chunk_file(epoch, "orders", r1, "0/20"))
        .await
        .unwrap();
    let keep_chunk = insert_ready(&mut *tx, &chunk_file(epoch, "customers", r2, "0/10"))
        .await
        .unwrap();
    let keep_stream = insert_ready(&mut *tx, &stream_file(epoch, "orders", "0/30"))
        .await
        .unwrap();

    reload::fail(
        &mut tx,
        r1,
        "echo timeout: is walrus.reload_signal published?",
    )
    .await
    .unwrap();

    // The failed reload is terminal with its reason recorded…
    let row = reload::get(&mut *tx, r1).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::Failed);
    assert!(row.error.as_deref().unwrap().contains("echo timeout"));

    // …its chunk files are GONE (purged in the same transaction as the flip)…
    let orders_left = claim_ready(&mut *tx, epoch, "public", "orders", 100)
        .await
        .unwrap();
    assert_eq!(
        orders_left.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![keep_stream],
        "only the stream file survives for orders"
    );
    assert_eq!(
        orders_left[0].reload_id, None,
        "stream rows never carry a reload_id"
    );

    // …and the OTHER reload's chunk file is untouched. Its reload is still `exporting`, which
    // pauses `claim_ready` for that table; flip it to `export_complete` first, which doubles as a
    // pause-lift assertion.
    let h: Lsn = "0/500".parse().unwrap();
    finish_fenced(&mut tx, r2, h).await;
    let customers_left = claim_ready(&mut *tx, epoch, "public", "customers", 100)
        .await
        .unwrap();
    assert_eq!(
        customers_left.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![keep_chunk]
    );
    assert_eq!(customers_left[0].kind, control::ManifestKind::Reload);
    assert_eq!(customers_left[0].reload_id, Some(r2));

    // A failed reload is terminal: the table is immediately requestable again.
    let r3 = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    assert!(r3 > r1);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn pristine_adoption_restart_refreshes_identity_without_spending_budget() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_014);

    for (table, start_lsn) in [
        ("adopted_pre_f", None),
        ("adopted_pre_chunk", Some(Lsn::new(0x100))),
    ] {
        let old_id = reload::request(&mut *tx, epoch, "public", table, ReloadFlavor::Reload)
            .await
            .unwrap();
        reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 1)
            .await
            .unwrap();
        let claimed = reload::get(&mut *tx, old_id).await.unwrap().unwrap();
        let request_id = claimed
            .parent_request_id
            .expect("direct requests persist a private source-fence namespace");
        if let Some(f) = start_lsn {
            reload::record_start_fence(
                &mut *tx,
                old_id,
                f,
                ReloadFenceIdentity {
                    request_id: Some(request_id),
                    source_schema: "public",
                    source_table: table,
                    schema_version: SchemaVersionNo(4),
                },
            )
            .await
            .unwrap();
        }
        let old = reload::get(&mut *tx, old_id).await.unwrap().unwrap();
        assert_eq!((old.chunk_no, old.cursor_pk.as_ref()), (0, None));

        let successor_id = reload::restart_pristine_adoption(&mut tx, &old)
            .await
            .unwrap();
        let failed = reload::get(&mut *tx, old_id).await.unwrap().unwrap();
        assert_eq!(failed.status, ReloadStatus::Failed);
        assert!(
            failed
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("adopted pristine attempt")
        );

        let successor = reload::get(&mut *tx, successor_id).await.unwrap().unwrap();
        assert_ne!(successor.reload_id, old.reload_id);
        assert_eq!(successor.parent_request_id, Some(request_id));
        assert_eq!(successor.status, ReloadStatus::Exporting);
        assert_eq!(
            successor.restart_count, old.restart_count,
            "pre-F and pre-chunk adoption must not consume the bounded restart budget"
        );
        assert_eq!((successor.chunk_no, successor.cursor_pk), (0, None));
        assert_eq!(
            successor.start_lsn, None,
            "the successor establishes fresh F"
        );
        assert_eq!(successor.final_lsn, None);
        assert_eq!(successor.schema_version, None);
        assert_eq!(successor.lease_holder.as_deref(), Some("sink-a"));
    }

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn lost_snapshot_restart_purges_predecessor_and_creates_fresh_successor() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = EpochNo(910_013);

    let old_id = reload::request(
        &mut *tx,
        epoch,
        "public",
        "snapshot_lost",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 1)
        .await
        .unwrap();
    let f: Lsn = "0/100".parse().unwrap();
    let schema_version = SchemaVersionNo(4);
    let fence_request_id = reload::get(&mut *tx, old_id)
        .await
        .unwrap()
        .unwrap()
        .parent_request_id
        .expect("direct requests persist a private source-fence namespace");
    reload::record_start_fence(
        &mut *tx,
        old_id,
        f,
        ReloadFenceIdentity {
            request_id: Some(fence_request_id),
            source_schema: "public",
            source_table: "snapshot_lost",
            schema_version,
        },
    )
    .await
    .unwrap();
    insert_ready(
        &mut *tx,
        &chunk_file(epoch, "snapshot_lost", old_id, "0/100"),
    )
    .await
    .unwrap();
    reload::advance_cursor(
        &mut *tx,
        old_id,
        1,
        &serde_json::json!([42]),
        f,
        schema_version,
    )
    .await
    .unwrap();
    let old = reload::get(&mut *tx, old_id).await.unwrap().unwrap();

    let successor_id = reload::restart_for_lost_snapshot(&mut tx, &old, 3)
        .await
        .unwrap()
        .expect("the first snapshot-loss restart is below the cap");

    let failed = reload::get(&mut *tx, old_id).await.unwrap().unwrap();
    assert_eq!(failed.status, ReloadStatus::Failed);
    assert!(
        failed
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("lost its source snapshot ownership")
    );
    let old_files: i64 =
        sqlx::query_scalar("SELECT count(*) FROM walrus.file_manifest WHERE reload_id = $1")
            .bind(old_id.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(
        old_files, 0,
        "the predecessor's staged chunk is purged atomically"
    );

    let successor = reload::get(&mut *tx, successor_id).await.unwrap().unwrap();
    assert_eq!(successor.status, ReloadStatus::Exporting);
    assert_eq!(successor.restart_count, 1);
    assert_eq!(successor.chunk_no, 0);
    assert_eq!(successor.cursor_pk, None);
    assert_eq!(
        successor.start_lsn, None,
        "the successor establishes fresh F"
    );
    assert_eq!(successor.first_lsn, None);
    assert_eq!(successor.final_lsn, None);
    assert_eq!(successor.schema_version, None);
    assert_eq!(successor.lease_holder.as_deref(), Some("sink-a"));

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn concurrent_claimers_partition_the_queue_via_skip_locked() {
    let pool = pool().await;
    let epoch = EpochNo(910_004);

    // SKIP LOCKED is only observable ACROSS transactions, so this test needs committed fixtures
    // (unlike the rolled-back-txn discipline above). Clean up leftovers from any crashed prior
    // run first — a stale non-terminal row would trip the one_live index — and again at the end.
    let cleanup = || async {
        sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
            .bind(epoch)
            .execute(&pool)
            .await
            .unwrap();
    };
    cleanup().await;
    let r1 = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    let r2 = reload::request(&pool, epoch, "public", "customers", ReloadFlavor::Reload)
        .await
        .unwrap();

    // Claimer A locks the oldest request and HOLDS its transaction open…
    let mut tx_a = pool.begin().await.unwrap();
    let a = reload::claim_requested(&mut *tx_a, epoch, "sink-a", 60, 1)
        .await
        .unwrap();
    assert_eq!(a.iter().map(|r| r.reload_id).collect::<Vec<_>>(), vec![r1]);

    // …and claimer B, on a separate connection, neither blocks nor double-claims: FOR UPDATE
    // SKIP LOCKED steps over A's locked (still-uncommitted) row and hands B only the other one.
    let mut tx_b = pool.begin().await.unwrap();
    let b = reload::claim_requested(&mut *tx_b, epoch, "sink-b", 60, 10)
        .await
        .unwrap();
    assert_eq!(
        b.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![r2],
        "B must skip A's locked row — overlap here means a double export"
    );

    tx_a.rollback().await.unwrap();
    tx_b.rollback().await.unwrap();
    cleanup().await;
}

#[tokio::test]
async fn concurrent_claimer_cannot_skip_a_locked_fifo_head_for_the_same_table() {
    let pool = pool().await;
    let epoch = EpochNo(910_010);

    // This lock-order property needs committed fixtures and two connections. As in the general
    // SKIP LOCKED test, clean this test-owned epoch before and after in case an earlier run died.
    let cleanup = || async {
        sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
            .bind(epoch)
            .execute(&pool)
            .await
            .unwrap();
    };
    cleanup().await;
    let first_request = SourceReloadRequest {
        epoch,
        source_request_id: Uuid::from_u128(0xA01),
        parent_request_id: None,
        scope: ReloadScope::Table,
        source_schema: "public",
        source_table: "orders",
        flavor: ReloadFlavor::Reload,
    };
    let first = reload::request_from_source(&pool, &first_request)
        .await
        .unwrap();
    let second_request = SourceReloadRequest {
        source_request_id: Uuid::from_u128(0xA02),
        ..first_request
    };
    reload::request_from_source(&pool, &second_request)
        .await
        .unwrap();

    let mut tx_a = pool.begin().await.unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx_a, epoch, "sink-a", 60, 1)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![first]
    );

    // A's status flip is uncommitted. B skips A's row lock but must not overtake it by claiming
    // the next source UUID for the same table.
    let mut tx_b = pool.begin().await.unwrap();
    assert!(
        reload::claim_requested(&mut *tx_b, epoch, "sink-b", 60, 10)
            .await
            .unwrap()
            .is_empty(),
        "SKIP LOCKED must not turn a per-table FIFO into concurrent exports"
    );

    tx_a.rollback().await.unwrap();
    assert_eq!(
        reload::claim_requested(&mut *tx_b, epoch, "sink-b", 60, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.reload_id)
            .collect::<Vec<_>>(),
        vec![first],
        "after the head lock is released, the head — not its successor — is claimable"
    );

    tx_b.rollback().await.unwrap();
    cleanup().await;
}
