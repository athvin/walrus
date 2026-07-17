#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Loader bootstrap against compose (`#[ignore]` — needs control PG + MinIO). Bootstrap acquires the
//! ownership lease, opens `<table>.duckdb` with both `<table>` and `<table>_raw`, loads the watermarks,
//! and verifies S3 read. A second live-lease instance exits terminal; a stale lock behind an expired
//! lease is reclaimed. The lease/DuckDB logic is unit-tested in the library.
//!
//!   cargo test -p loader --test bootstrap -- --ignored

use common::{PgColumn, PgRelation, ReplicaIdentity};
use loader::bootstrap::bootstrap;
use loader::config::LoaderConfig;
use loader::error::LoaderError;
use loader::health::LoaderState;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, is_key: bool| PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

fn cfg(pod: &str, dir: &std::path::Path, ttl: Duration) -> LoaderConfig {
    LoaderConfig {
        control_db_url: control_url(),
        object_store: common::config::ObjectStoreConfig {
            bucket: "walrus".into(),
            endpoint: Some("http://localhost:9000".into()),
            region: "us-east-1".into(),
        },
        instance: pod.into(),
        duckdb_dir: dir.to_string_lossy().into_owned(),
        lease_ttl: ttl,
        ..LoaderConfig::default()
    }
}

/// Seed a fresh epoch as the current one + register `orders`, cleaning any prior control state.
async fn seed(pool: &sqlx::PgPool, epoch: i64) {
    for tbl in [
        "table_ownership",
        "loader_checkpoint",
        "schema_registry",
        "replication_state",
    ] {
        let _ = sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(pool)
            .await;
    }
    control::insert_epoch(
        pool,
        &control::ReplicationState {
            epoch,
            slot_name: "walrus_slot".into(),
            created_lsn: "0/0".parse().unwrap(),
            status: "streaming".into(),
        },
    )
    .await
    .unwrap();
    let rel = orders();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: rel.schema.clone(),
            source_table: rel.name.clone(),
            schema_version: 1,
            descriptors: Vec::new(),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("walrus-loader-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn table_exists(db: &loader::duck::TableDb, name: &str) -> bool {
    let conn = db.conn();
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
            |r| r.get(0),
        )
        .unwrap();
    n > 0
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn bootstrap_creates_duckdb_with_both_tables_and_takes_the_lease() {
    let _g = LOCK.lock().await;
    let epoch = 3_100_101;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    seed(&pool, epoch).await;
    let dir = tmpdir("bootstrap");
    let cfg = cfg("loader-a", &dir, Duration::from_secs(30));
    let state = LoaderState::new();

    let owned = bootstrap(&cfg, &pool, store().as_ref(), &state)
        .await
        .unwrap();
    assert_eq!(owned.len(), 1, "owns the one registered table");
    let orders = &owned[0];
    assert!(
        table_exists(&orders.db, "orders").await,
        "mirror table exists"
    );
    assert!(
        table_exists(&orders.db, "orders_raw").await,
        "CDC log table exists"
    );
    assert!(
        !state.is_ready(),
        "bootstrap does not itself mark ready (main does)"
    );
    assert!(
        state.is_live(),
        "bootstrap stamped one poll → /healthz green"
    );

    // The lease is held by us.
    let owner: String = sqlx::query_scalar(
        "SELECT owner_pod FROM walrus.table_ownership WHERE epoch=$1 AND source_table='orders'",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(owner, "loader-a");
    assert!(
        orders.db.conn().execute_batch("SELECT 1").is_ok(),
        ".duckdb file lock is held (open RW)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn second_instance_with_live_lease_exits_terminal() {
    let _g = LOCK.lock().await;
    let epoch = 3_100_102;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    seed(&pool, epoch).await;
    let dir_a = tmpdir("live-a");
    let state = LoaderState::new();

    // Instance A takes the lease (live, 30s) and keeps its DuckDB connection open.
    let _owned_a = bootstrap(
        &cfg("loader-a", &dir_a, Duration::from_secs(30)),
        &pool,
        store().as_ref(),
        &state,
    )
    .await
    .unwrap();

    // Instance B, while A's lease is live, must fail terminal with LeaseContended.
    let dir_b = tmpdir("live-b");
    let res = bootstrap(
        &cfg("loader-b", &dir_b, Duration::from_secs(30)),
        &pool,
        store().as_ref(),
        &LoaderState::new(),
    )
    .await;
    let err = res.err().expect("a live lease must be terminal");
    assert!(
        matches!(err, LoaderError::LeaseContended { .. }),
        "a live lease is terminal: {err:?}"
    );
    assert_eq!(err.exit_code(), common::ExitCode::LeaseContended);

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn stale_lock_expired_lease_is_reclaimed_and_opened() {
    let _g = LOCK.lock().await;
    let epoch = 3_100_103;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    seed(&pool, epoch).await;
    let dir = tmpdir("stale");
    let state = LoaderState::new();

    // Instance A takes a SHORT-TTL lease then "dies": dropping its TableDb releases the DuckDB lock.
    {
        let owned_a = bootstrap(
            &cfg("loader-dead", &dir, Duration::from_millis(500)),
            &pool,
            store().as_ref(),
            &state,
        )
        .await
        .unwrap();
        assert_eq!(owned_a[0].fencing_token, 1);
    } // owned_a dropped → file lock released; the lease row remains but will expire.

    tokio::time::sleep(Duration::from_millis(900)).await; // lease expires

    // Instance B reclaims the expired lease and opens the (now-unlocked) file. Token bumps to 2.
    let owned_b = bootstrap(
        &cfg("loader-b", &dir, Duration::from_secs(30)),
        &pool,
        store().as_ref(),
        &LoaderState::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        owned_b[0].fencing_token, 2,
        "reclaim by a new owner bumps the fencing token"
    );
    assert!(table_exists(&owned_b[0].db, "orders").await);

    let _ = std::fs::remove_dir_all(&dir);
}
