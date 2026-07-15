//! Reload controller pickup against compose (`#[ignore]` — needs source + control PG). A
//! `requested` row flips to `exporting` within one poll cadence with a live, observably-advancing
//! lease; doomed requests (unpublished / keyless) fail fast with operator-readable reasons while a
//! `resync` of a keyed table is accepted (PR 6.10); the `max_concurrent_reloads` cap holds under
//! three requests while the replication
//! stream keeps flowing. The scheduling/lease-cancel semantics are unit-tested in
//! `src/reload.rs`; each test runs its own controller against its own epoch, so tests never
//! claim each other's rows.
//!
//!   cargo test -p pg-sink --test reload_pickup -- --ignored

use control::reload::{self, ReloadFlavor, ReloadStatus};
use pg_sink::consume::on_frame;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::reload::{ReloadController, ReloadControllerConfig};
use pg_sink::reload_signal::WatermarkWaiters;
use pg_sink::replication::ReplicationStream;
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn admin() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

async fn pool_for(epoch: i64) -> sqlx::PgPool {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    // Leftover non-terminal rows from a crashed prior run would trip the one_live index.
    sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
    pool
}

fn controller_cfg(epoch: i64, cap: usize) -> ReloadControllerConfig {
    ReloadControllerConfig {
        poll_interval: Duration::from_millis(200), // fast cadence for the test
        max_concurrent_reloads: cap,
        lease_ttl: Duration::from_secs(6), // renewal at 2s — observable within one test
        instance: "walrus-sink-test".to_string(),
        publication_name: "walrus_pub".to_string(),
        epoch,
        chunk_rows: 1000,
        // No decode loop resolves echoes in these tests, so exporters PARK on the echo await —
        // exactly the observable-scheduling role PR 6.4's stub used to play. The echo/export
        // behaviour itself is reload_export.rs's suite.
        echo_timeout: Duration::from_secs(3600),
        reload_max_restarts: 3,
    }
}

fn minio(epoch: i64) -> ParquetSink {
    ParquetSink::new(
        std::sync::Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("walrus")
                .with_region("us-east-1")
                .with_endpoint("http://localhost:9000")
                .with_access_key_id("minioadmin")
                .with_secret_access_key("minioadmin")
                .with_allow_http(true)
                .build()
                .unwrap(),
        ),
        "walrus".to_string(),
        epoch,
    )
}

/// The exporter reads the reload's schema_version from the registry — seed one per target table
/// (in production the streaming sink registers every published table long before a reload).
async fn seed_registry(
    admin: &tokio_postgres::Client,
    pool: &sqlx::PgPool,
    epoch: i64,
    tables: &[&str],
) {
    for table in tables {
        let rel = pg_sink::snapshot::describe_source_relation(admin, "public", table)
            .await
            .unwrap();
        let row = control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: table.to_string(),
            schema_version: 1,
            descriptors: pg_to_arrow::descriptor::describe_relation(&rel),
            columns: serde_json::to_value(&rel).unwrap(),
        };
        control::upsert_registry(pool, &row).await.unwrap();
    }
}

async fn status_of(pool: &sqlx::PgPool, reload_id: i64) -> (ReloadStatus, Option<String>) {
    let row = reload::get(pool, reload_id).await.unwrap().unwrap();
    (row.status, row.error)
}

/// Poll until the row reaches `want` (the controller's cadence is 200ms; give it a few).
async fn await_status(pool: &sqlx::PgPool, reload_id: i64, want: ReloadStatus) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if status_of(pool, reload_id).await.0 == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("reload {reload_id} never reached {want:?}"));
}

/// The exact SQL `just reload` runs (keep in sync with the justfile recipe): epoch comes from
/// `MAX(epoch)` over `replication_state`, the table arg splits on the dot. Runs in a rolled-back
/// transaction so the seeded epoch and the inserted request leave no trace.
#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG)"]
async fn just_reload_recipe_sql_selects_current_epoch_and_parses_table() {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO walrus.replication_state (epoch, slot_name, created_lsn, status)
         VALUES (640004, 'walrus_recipe_test', '0/0', 'streaming')",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    let (epoch, schema, table, flavor): (i64, String, String, String) = sqlx::query_as(
        "INSERT INTO walrus.table_reload (epoch, source_schema, source_table, flavor) \
         SELECT COALESCE(MAX(epoch), 1), split_part('public.orders', '.', 1), \
                split_part('public.orders', '.', 2), 'reload' \
         FROM walrus.replication_state \
         RETURNING epoch, source_schema, source_table, flavor",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(epoch, 640_004, "the recipe targets the CURRENT (max) epoch");
    assert_eq!((schema.as_str(), table.as_str()), ("public", "orders"));
    assert_eq!(flavor, "reload");
    tx.rollback().await.unwrap();
}

async fn lease_expiry_epoch(pool: &sqlx::PgPool, reload_id: i64) -> f64 {
    sqlx::query_scalar::<_, f64>(
        "SELECT extract(epoch FROM lease_expiry)::float8
         FROM walrus.table_reload WHERE reload_id = $1",
    )
    .bind(reload_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn pickup_flips_to_exporting_with_a_live_advancing_lease() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = 640_001;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = pool_for(epoch).await;
    seed_registry(&admin, &pool, epoch, &["orders"]).await;

    let token = CancellationToken::new();
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        Arc::new(WatermarkWaiters::default()),
        minio(epoch),
        controller_cfg(epoch, 2),
        token.clone(),
    );

    // `just reload table='public.orders'` — the same INSERT the recipe runs.
    let id = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    await_status(&pool, id, ReloadStatus::Exporting).await;

    let row = reload::get(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.lease_holder.as_deref(), Some("walrus-sink-test"));
    let now_epoch: f64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::float8")
        .fetch_one(&pool)
        .await
        .unwrap();
    let exp1 = lease_expiry_epoch(&pool, id).await;
    assert!(exp1 > now_epoch, "the lease is live");

    // The exporter parks on its echo await while its lease renews at TTL/3 (2s here): expiry
    // must advance. Poll rather than sleep-once — a loaded runner can delay a renewal tick.
    let exp2 = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let e = lease_expiry_epoch(&pool, id).await;
            if e > exp1 {
                return e;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("lease_expiry observably advances while the exporter runs");
    assert!(exp2 > exp1);

    token.cancel();
    handle.await.unwrap();
    sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn preflight_failures_land_in_failed_with_reasons() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = 640_002;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    // A published-but-keyless table: the dev publication is FOR ALL TABLES, so existence ⇒
    // membership; what it lacks is a PK.
    admin
        .batch_execute(
            "DROP TABLE IF EXISTS public._walrus_rl_keyless;
             CREATE TABLE public._walrus_rl_keyless (x int)",
        )
        .await
        .unwrap();
    let pool = pool_for(epoch).await;

    let token = CancellationToken::new();
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        Arc::new(WatermarkWaiters::default()),
        minio(epoch),
        controller_cfg(epoch, 2),
        token.clone(),
    );

    // (a) Not in the publication (a table that doesn't exist is by definition unpublished).
    let ghost = reload::request(&pool, epoch, "public", "ghost_table", ReloadFlavor::Reload)
        .await
        .unwrap();
    // (b) Published but keyless.
    let keyless = reload::request(
        &pool,
        epoch,
        "public",
        "_walrus_rl_keyless",
        ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    // (c) Resync of a published, keyed table: NO LONGER rejected (PR 6.10 lifted the guard) — it
    // passes preflight like a `reload` and reaches `exporting` (here it parks on the echo await,
    // no resolver runs, exactly like the accepted-request case above).
    let resync = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();

    await_status(&pool, ghost, ReloadStatus::Failed).await;
    await_status(&pool, keyless, ReloadStatus::Failed).await;
    await_status(&pool, resync, ReloadStatus::Exporting).await;

    let (_, err) = status_of(&pool, ghost).await;
    assert!(
        err.as_deref()
            .unwrap()
            .contains("is not in the publication"),
        "ghost: {err:?}"
    );
    let (_, err) = status_of(&pool, keyless).await;
    assert!(
        err.as_deref().unwrap().contains("has no primary key"),
        "keyless: {err:?}"
    );

    token.cancel();
    handle.await.unwrap();
    admin
        .batch_execute("DROP TABLE IF EXISTS public._walrus_rl_keyless")
        .await
        .unwrap();
    sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn cap_of_two_holds_and_the_stream_keeps_flowing() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = 640_003;
    let slot = "walrus_reload_pickup";
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.customers WHERE region = 'rl' AND id = 640003",
            &[],
        )
        .await
        .unwrap();
    let pool = pool_for(epoch).await;
    seed_registry(&admin, &pool, epoch, &["orders", "customers", "items"]).await;

    // A live stream BEFORE the controller starts — the no-stall probe.
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    let token = CancellationToken::new();
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        Arc::new(WatermarkWaiters::default()),
        minio(epoch),
        controller_cfg(epoch, 2),
        token.clone(),
    );

    // Three valid requests, cap two: the third must WAIT (the stub exporters never finish, so a
    // permit never frees — `requested` is exactly where it stays).
    let a = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Reload)
        .await
        .unwrap();
    let b = reload::request(&pool, epoch, "public", "customers", ReloadFlavor::Reload)
        .await
        .unwrap();
    let c = reload::request(&pool, epoch, "public", "items", ReloadFlavor::Reload)
        .await
        .unwrap();

    await_status(&pool, a, ReloadStatus::Exporting).await;
    await_status(&pool, b, ReloadStatus::Exporting).await;

    // Sample across several poll cadences: never more than two exporting; the third never claimed.
    for _ in 0..8 {
        let exporting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM walrus.table_reload WHERE epoch = $1 AND status = 'exporting'",
        )
        .bind(epoch)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exporting <= 2, "cap breached: {exporting} exporting");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert_eq!(
        status_of(&pool, c).await.0,
        ReloadStatus::Requested,
        "the third waits for a permit"
    );

    // Free a permit through the SHIPPED path: steal `a`'s lease, so its exporter's next renewal
    // (≤ 2s) returns false → LostLease → its permit drops → the controller's next tick claims the
    // third request. This drives lost-lease-cancels-the-exporter AND third-starts-when-a-permit-
    // frees through the real controller, not a test copy. (Row `a` stays `exporting` under the
    // thief — from here on 3 rows carry that status, but only 2 are ever OUR exporters.)
    sqlx::query("UPDATE walrus.table_reload SET lease_holder = 'lease-thief' WHERE reload_id = $1")
        .bind(a)
        .execute(&pool)
        .await
        .unwrap();
    await_status(&pool, c, ReloadStatus::Exporting).await;

    // The replication stream never paused while the controller worked: a user change written NOW
    // decodes promptly (the controller runs on its own connections, off the decode path).
    admin
        .execute(
            "INSERT INTO public.customers (region, id, name) VALUES ('rl', 640003, 'no-stall')",
            &[],
        )
        .await
        .unwrap();
    // Match ONLY the customers insert by its relation OID — the exporters' own reload_signal
    // inserts also decode as Inserts and must not satisfy the no-stall probe.
    let mut ctx = StreamCtx::default();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut customers_oid: Option<u32> = None;
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            match on_frame(&mut ctx, frame).unwrap() {
                Some(Message::Relation { relation, .. }) if relation.name == "customers" => {
                    customers_oid = Some(relation.oid);
                }
                Some(Message::Insert { relation_oid, .. })
                    if customers_oid == Some(relation_oid) =>
                {
                    return;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the USER insert decodes while the controller holds two exports");

    token.cancel();
    handle.await.unwrap();
    drop(stream);
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
    admin
        .execute(
            "DELETE FROM public.customers WHERE region = 'rl' AND id = 640003",
            &[],
        )
        .await
        .unwrap();
    sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
}
