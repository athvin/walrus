#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect make failed setup and protocol assertions immediate"
)]
//! End-to-end proof that a post-start table reload enters through the published source-WAL event,
//! is fenced in control PG, and atomically replaces the DuckLake table without losing DML committed
//! inside the export window.
//!
//! Run this target alone with one test thread because the harness owns fixed ports, one replication
//! slot, and one DuckLake metadata schema:
//!
//! `cargo test -p e2e --features it --test source_reload_event -- --ignored --test-threads=1`

#![cfg(feature = "it")]

use common::{Lsn, ReloadId};
use control::{ReloadMarkerKind, ReloadStatus};
use e2e::Harness;
use std::time::Duration;
use uuid::Uuid;

async fn await_baseline(h: &Harness, request_id: Uuid, deadline: Duration) -> (ReloadId, Lsn) {
    let started = tokio::time::Instant::now();
    loop {
        let marker = sqlx::query_as::<_, (i64, String)>(
            "SELECT r.reload_id, m.lsn::text
             FROM walrus.table_reload r
             JOIN walrus.table_reload_marker m USING (reload_id)
             WHERE r.epoch = $1
               AND r.source_request_id = $2
               AND r.source_schema = 'public'
               AND r.source_table = 'orders'
               AND m.marker_kind = 'baseline'",
        )
        .bind(h.epoch)
        .bind(request_id)
        .fetch_optional(h.control_pool())
        .await
        .unwrap();
        if let Some((reload_id, lsn)) = marker {
            return (reload_id.into(), lsn.parse().unwrap());
        }
        assert!(
            started.elapsed() < deadline,
            "source request {request_id} was not decoded and fenced within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn await_complete(
    h: &Harness,
    reload_id: ReloadId,
    deadline: Duration,
) -> control::ReloadRow {
    let started = tokio::time::Instant::now();
    loop {
        let row = control::reload::get(h.control_pool(), reload_id)
            .await
            .unwrap()
            .expect("decoded source reload has a control row");
        if row.status == ReloadStatus::Complete {
            return row;
        }
        assert_ne!(
            row.status,
            ReloadStatus::Failed,
            "source-driven reload failed: {:?}",
            row.error
        );
        assert!(
            started.elapsed() < deadline,
            "reload {reload_id} remained {:?} beyond {deadline:?}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn source_wal_request_reconciles_dml_between_f_and_h() {
    let mut h = Harness::start().await.unwrap();

    // Establish a non-empty, fully mirrored steady state before asking for a post-start rebuild.
    // Enough rows ensure the exporter has a real Parquet PUT to block while MinIO is paused.
    h.source_batch(
        "INSERT INTO public.orders (id, status)
         SELECT g, 'before-' || g FROM generate_series(1, 2000) g;",
    )
    .await
    .unwrap();
    let seed_floor = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.orders SET status = 'steady' WHERE id = 1")
        .await
        .unwrap();
    h.await_transformed_past("orders", seed_floor, Duration::from_secs(120))
        .await
        .unwrap();

    // With object storage paused, the request and F event still decode into control PG, but the
    // first non-empty baseline chunk cannot become durable. The exporter therefore cannot append H.
    h.stall_s3().await.unwrap();
    let request_id = Uuid::new_v4();
    h.request_table_reload(request_id, "public", "orders")
        .await
        .unwrap();

    let source_request = sqlx::query_as::<_, (String, String, String, String, serde_json::Value)>(
        "SELECT request_id::text, event_kind, source_schema, source_table, targets
         FROM walrus.reload_event WHERE event_id = $1",
    )
    .bind(request_id)
    .fetch_one(h.source_pool())
    .await
    .unwrap();
    assert_eq!(source_request.0, request_id.to_string());
    assert_eq!(source_request.1, "request");
    assert_eq!(source_request.2, "public");
    assert_eq!(source_request.3, "orders");
    assert_eq!(source_request.4, serde_json::json!([]));

    let (reload_id, f) = await_baseline(&h, request_id, Duration::from_secs(60)).await;
    let row = control::reload::get(h.control_pool(), reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.source_request_id, Some(request_id));
    assert_eq!(row.start_lsn, Some(f));
    assert_eq!(row.status, ReloadStatus::Exporting);
    let end_before_dml: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.table_reload_marker
         WHERE reload_id = $1 AND marker_kind = 'end'",
    )
    .bind(reload_id.0)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(
        end_before_dml, 0,
        "H cannot exist while the baseline PUT is blocked"
    );

    // These commits are strictly after observed F and strictly before H: MinIO remains paused.
    // Together they exercise overwrite, ghost removal, a replica-key move, and a brand-new row.
    h.source_batch(
        "BEGIN;
         UPDATE public.orders SET status = 'updated-between-f-h' WHERE id = 1;
         DELETE FROM public.orders WHERE id = 2;
         UPDATE public.orders SET id = 20003, status = 'moved-between-f-h' WHERE id = 3;
         INSERT INTO public.orders (id, status) VALUES (30001, 'inserted-between-f-h');
         COMMIT;",
    )
    .await
    .unwrap();
    let dml_upper_bound = h.source_wal_lsn().await.unwrap();
    assert!(f < dml_upper_bound, "the controlled DML committed after F");
    let end_while_blocked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.table_reload_marker
         WHERE reload_id = $1 AND marker_kind = 'end'",
    )
    .bind(reload_id.0)
    .fetch_one(h.control_pool())
    .await
    .unwrap();
    assert_eq!(
        end_while_blocked, 0,
        "H stayed blocked until after the controlled DML"
    );

    h.unstall_s3().await.unwrap();
    let completed = await_complete(&h, reload_id, Duration::from_secs(180)).await;
    let markers = control::reload::read_markers(h.control_pool(), reload_id)
        .await
        .unwrap();
    assert_eq!(
        markers.len(),
        2,
        "one durable F marker and one durable H marker"
    );
    assert_eq!(markers[0].kind, ReloadMarkerKind::Baseline);
    assert_eq!(markers[0].lsn, f);
    assert_eq!(markers[1].kind, ReloadMarkerKind::End);
    let h_lsn = markers[1].lsn;
    assert!(
        h_lsn > dml_upper_bound,
        "H {h_lsn} must follow the post-DML WAL position {dml_upper_bound}"
    );
    assert_eq!(completed.start_lsn, Some(f));
    assert_eq!(completed.final_lsn, Some(h_lsn));

    let source_protocol_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.reload_event
         WHERE request_id = $1
           AND (event_kind = 'request' OR reload_id = $2)",
    )
    .bind(request_id)
    .bind(reload_id.0)
    .fetch_one(h.source_pool())
    .await
    .unwrap();
    assert_eq!(
        source_protocol_rows, 3,
        "the source protocol log retains request plus F/H fence events"
    );

    // `complete` is written only after the loader publishes its hidden rebuild. Release its DuckDB
    // lock, then require exact source equality and explicitly check the four in-window mutations.
    h.stop_loader().await.unwrap();
    h.assert_mirror_equals_source("orders").await.unwrap();
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_current WHERE id = 2 OR id = 3"
        )
        .unwrap(),
        0,
        "delete and old side of key move leave no ghosts"
    );
    assert_eq!(
        h.duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_current
             WHERE (id = 1 AND status = 'updated-between-f-h')
                OR (id = 20003 AND status = 'moved-between-f-h')
                OR (id = 30001 AND status = 'inserted-between-f-h')"
        )
        .unwrap(),
        3,
        "all post-F upserts survive the atomic cutover"
    );
}
