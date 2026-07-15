//! Compose-gated integration tests for the `table_reload` state machine (PR 6.1).
//!
//! Same discipline as the manifest tests: every test runs inside a rolled-back transaction and
//! namespaces its rows by a unique `epoch`, so runs are isolated and idempotent. Statements that
//! provoke a real SQL error (the duplicate-request unique violation) run under a nested
//! savepoint, because a failed statement aborts the enclosing Postgres transaction.
#![cfg(feature = "integration")]

use common::Lsn;
use control::reload::{self, ReloadFlavor, ReloadStatus};
use control::{claim_ready, connect, insert_ready, run_migrations, ControlError, NewManifestFile};
use sqlx::postgres::PgPool;
use sqlx::Connection;

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
fn chunk_file(epoch: i64, table: &str, reload_id: i64, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/reload-{reload_id}-{lsn_end}.parquet"),
        kind: "reload".to_string(),
        row_count: 1,
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: 1,
        reload_id: Some(reload_id),
    }
}

/// `lease_expiry` as a comparable number — the model omits the column by design (every time
/// comparison lives in SQL), so tests that care probe it directly.
async fn expiry_epoch(ex: impl sqlx::PgExecutor<'_>, reload_id: i64) -> f64 {
    sqlx::query_scalar::<_, f64>(
        "SELECT extract(epoch FROM lease_expiry)::float8
         FROM walrus.table_reload WHERE reload_id = $1",
    )
    .bind(reload_id)
    .fetch_one(ex)
    .await
    .unwrap()
}

/// An ordinary stream file — `reload_id` stays NULL, exactly like every pre-6.1 row.
fn stream_file(epoch: i64, table: &str, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/{lsn_end}.parquet"),
        kind: "stream".to_string(),
        row_count: 1,
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: 1,
        reload_id: None,
    }
}

#[tokio::test]
async fn full_status_walk_and_duplicate_request_rejected() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 910_001;

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

    // The pause engages at REQUEST time, not claim time: `orders` is an active rebuild while
    // still `requested` (PR 6.6 pauses that table's claims from this moment); the resync never
    // shows up here.
    let rebuilds = reload::active_rebuilds(&mut *tx, epoch).await.unwrap();
    assert_eq!(
        rebuilds.iter().map(|r| r.reload_id).collect::<Vec<_>>(),
        vec![id],
        "a requested reload already pauses; a resync never does"
    );
    assert_eq!(rebuilds[0].status, ReloadStatus::Requested);

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

    // Nothing left in `requested`: a latecomer gets an empty Vec, not an error. (The
    // cross-connection SKIP LOCKED race is exercised in
    // `concurrent_claimers_partition_the_queue_via_skip_locked` below.)
    let raced = reload::claim_requested(&mut *tx, epoch, "sink-b", 60, 10)
        .await
        .unwrap();
    assert!(raced.is_empty());

    // The holder renews — and the lease observably extends (same frozen now(), bigger ttl);
    // a phantom does not.
    assert!(reload::renew_lease(&mut *tx, id, "sink-a", 3600)
        .await
        .unwrap());
    let exp_renewed = expiry_epoch(&mut *tx, id).await;
    assert!(
        exp_renewed > exp_claim + 3000.0,
        "renew pushed lease_expiry out by the new ttl"
    );
    assert!(!reload::renew_lease(&mut *tx, id, "sink-zombie", 60)
        .await
        .unwrap());

    // Chunk 1 freezes L₁ + schema_version; chunk 2's newer L₂ must NOT overwrite the frozen L₁.
    let l1: Lsn = "0/100".parse().unwrap();
    let l2: Lsn = "0/200".parse().unwrap();
    reload::advance_cursor(&mut *tx, id, 1, &serde_json::json!([42]), l1, 7)
        .await
        .unwrap();
    reload::advance_cursor(&mut *tx, id, 2, &serde_json::json!([84]), l2, 7)
        .await
        .unwrap();

    // A mismatched schema_version is ASSERTED, not swallowed: every attempt is single-schema by
    // construction (H9), so version 9 mid-attempt means the export engine missed a DDL restart.
    let err = reload::advance_cursor(&mut *tx, id, 3, &serde_json::json!([99]), l2, 9)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.chunk_no, 2, "the rejected mismatch advanced nothing");
    assert_eq!(row.cursor_pk, Some(serde_json::json!([84])));
    assert_eq!(row.first_lsn, Some(l1), "first_lsn is frozen on chunk 1");
    assert_eq!(
        row.schema_version,
        Some(7),
        "schema_version is frozen on chunk 1"
    );

    // exporting → export_complete records the final watermark H…
    let h: Lsn = "0/300".parse().unwrap();
    reload::complete_export(&mut *tx, id, h).await.unwrap();
    let row = reload::get(&mut *tx, id).await.unwrap().unwrap();
    assert_eq!(row.status, ReloadStatus::ExportComplete);
    assert_eq!(row.final_lsn, Some(h));
    assert!(
        reload::active_rebuilds(&mut *tx, epoch)
            .await
            .unwrap()
            .is_empty(),
        "the pause lifts at export_complete — holding it would deadlock the rebuild (PR 6.6)"
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
async fn wrong_state_transition_changes_zero_rows() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 910_002;

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
    let err = reload::advance_cursor(&mut *tx, id, 1, &serde_json::json!([1]), h, 1)
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
    let err = reload::advance_cursor(&mut *tx, id, 2, &serde_json::json!([1]), h, 1)
        .await
        .unwrap_err();
    assert!(matches!(err, ControlError::ReloadTransition { .. }));

    // Walk to terminal, then confirm terminal states reject everything.
    reload::complete_export(&mut *tx, id, h).await.unwrap();
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
    let epoch = 910_005;

    let id = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(&mut *tx, epoch, "sink-a", 60, 10)
        .await
        .unwrap();

    // A phantom can't release someone else's claim; releasing a `requested` row is a no-op too.
    assert!(!reload::release_claim(&mut *tx, id, "sink-zombie")
        .await
        .unwrap());

    // The claimant releases: back to `requested`, lease cleared, immediately re-claimable — the
    // controller's un-claim path for infra failures between claim and exporter spawn (PR 6.4).
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

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn fail_purges_this_reloads_manifest_rows_only() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 910_003;

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

    // …and the OTHER reload's chunk file is untouched. (Its reload is still `exporting`, which
    // since PR 6.6 pauses claim_ready for that table — flip it to export_complete first, which
    // doubles as a pause-lift assertion.)
    let h: Lsn = "0/500".parse().unwrap();
    reload::complete_export(&mut *tx, r2, h).await.unwrap();
    let customers_left = claim_ready(&mut *tx, epoch, "public", "customers", 100)
        .await
        .unwrap();
    assert_eq!(
        customers_left.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![keep_chunk]
    );
    assert_eq!(customers_left[0].kind, "reload");
    assert_eq!(customers_left[0].reload_id, Some(r2));

    // A failed reload is terminal: the table is immediately requestable again.
    let r3 = reload::request(&mut *tx, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    assert!(r3 > r1);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn concurrent_claimers_partition_the_queue_via_skip_locked() {
    let pool = pool().await;
    let epoch = 910_004;

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
