//! Reload controller pickup against compose (`#[ignore]` — needs source + control PG). A
//! `requested` row flips to `exporting` within one poll cadence with a live, observably-advancing
//! lease; doomed requests (unpublished / keyless / resync) fail fast with operator-readable
//! reasons; the `max_concurrent_reloads` cap holds under three requests while the replication
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

    let token = CancellationToken::new();
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        Arc::new(WatermarkWaiters::default()),
        controller_cfg(epoch, 2),
        token.clone(),
    )
    .await
    .unwrap();

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

    // The stub exporter parks while its lease renews at TTL/3 (2s here): expiry must advance.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let exp2 = lease_expiry_epoch(&pool, id).await;
    assert!(
        exp2 > exp1,
        "lease_expiry observably advances while the exporter runs ({exp1} → {exp2})"
    );

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
        controller_cfg(epoch, 2),
        token.clone(),
    )
    .await
    .unwrap();

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
    // (c) Resync: rejected until PR 6.10 lifts it.
    let resync = reload::request(&pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();

    await_status(&pool, ghost, ReloadStatus::Failed).await;
    await_status(&pool, keyless, ReloadStatus::Failed).await;
    await_status(&pool, resync, ReloadStatus::Failed).await;

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
    let (_, err) = status_of(&pool, resync).await;
    assert!(
        err.as_deref().unwrap().contains("PR 6.10"),
        "resync: {err:?}"
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
        controller_cfg(epoch, 2),
        token.clone(),
    )
    .await
    .unwrap();

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

    // The replication stream never paused while the controller worked: a user change written NOW
    // decodes promptly (the controller runs on its own connections, off the decode path).
    admin
        .execute(
            "INSERT INTO public.customers (region, id, name) VALUES ('rl', 640003, 'no-stall')",
            &[],
        )
        .await
        .unwrap();
    let mut ctx = StreamCtx::default();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            if let Some(Message::Insert { .. }) = on_frame(&mut ctx, frame).unwrap() {
                return;
            }
        }
    })
    .await
    .expect("a user insert decodes while the controller holds two exports");

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
