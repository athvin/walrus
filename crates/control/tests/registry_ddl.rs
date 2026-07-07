//! Compose-gated integration tests for `schema_registry` and `ddl_manifest`.
//!
//! Each test runs inside a rolled-back transaction under a unique `epoch`. Gated behind the
//! `integration` feature (needs the PR 0.6 control PG).
#![cfg(feature = "integration")]

use common::{Lsn, Tier, TypeDescriptor, TypeMeta};
use control::{
    connect, insert_ddl, read_latest_version, read_pending_ddl, read_registry, run_migrations,
    upsert_registry, DdlRow, RegistryRow,
};
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

fn descriptors() -> Vec<TypeDescriptor> {
    vec![
        TypeDescriptor {
            column: "id".to_string(),
            pg_type_oid: 23,
            pg_type: "int4".to_string(),
            tier: Tier::One,
            arrow: "Int32".to_string(),
            duckdb: "INTEGER".to_string(),
            emit: vec!["id:INT32".to_string()],
            recombine: None,
            meta: TypeMeta::default(),
        },
        TypeDescriptor {
            column: "duration".to_string(),
            pg_type_oid: 1186,
            pg_type: "interval".to_string(),
            tier: Tier::Two,
            arrow: "Struct/Decomposed".to_string(),
            duckdb: "INTERVAL".to_string(),
            emit: vec![
                "duration_months:INT32".to_string(),
                "duration_days:INT32".to_string(),
                "duration_micros:INT64".to_string(),
            ],
            recombine: Some("to_months(m)+to_days(d)+to_microseconds(us)".to_string()),
            meta: TypeMeta::default(),
        },
    ]
}

fn registry_row(epoch: i64, version: i64) -> RegistryRow {
    RegistryRow {
        epoch,
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: version,
        descriptors: descriptors(),
        columns: serde_json::json!([
            {"name": "id", "attnum": 1, "not_null": true},
            {"name": "duration", "attnum": 2, "not_null": false}
        ]),
    }
}

fn ddl(epoch: i64, c_lsn: &str, version: i64) -> DdlRow {
    DdlRow {
        id: 0, // ignored on insert
        epoch,
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        c_lsn: c_lsn.parse().unwrap(),
        c_event: "ddl_command_end".to_string(),
        c_tag: "ALTER TABLE".to_string(),
        schema_version: version,
    }
}

#[tokio::test]
async fn registry_round_trips_a_type_descriptor_set() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 800_001;

    let row = registry_row(epoch, 3);
    upsert_registry(&mut *tx, &row).await.unwrap();

    let read = read_registry(&mut *tx, epoch, "public", "orders", 3)
        .await
        .unwrap()
        .unwrap();
    // Descriptors round-trip byte-for-byte equal through jsonb.
    assert_eq!(read.descriptors, row.descriptors);
    assert_eq!(read.columns, row.columns);
    assert_eq!(read.schema_version, 3);

    // An unknown version reads as None.
    assert!(read_registry(&mut *tx, epoch, "public", "orders", 99)
        .await
        .unwrap()
        .is_none());

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn upsert_registry_is_idempotent_per_version() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 800_002;

    let row = registry_row(epoch, 1);
    upsert_registry(&mut *tx, &row).await.unwrap();
    upsert_registry(&mut *tx, &row).await.unwrap(); // same version again

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.schema_registry WHERE epoch = $1 AND schema_version = $2",
    )
    .bind(epoch)
    .bind(1_i64)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        count, 1,
        "a re-write of the same version must not duplicate"
    );

    // read_latest_version reports the max across versions.
    upsert_registry(&mut *tx, &registry_row(epoch, 5))
        .await
        .unwrap();
    let latest = read_latest_version(&mut *tx, epoch, "public", "orders")
        .await
        .unwrap();
    assert_eq!(latest, Some(5));
    // and None for an unknown table.
    assert_eq!(
        read_latest_version(&mut *tx, epoch, "public", "no_such")
            .await
            .unwrap(),
        None
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn ddl_row_round_trips_with_commit_lsn() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 800_003;

    let id = insert_ddl(&mut *tx, &ddl(epoch, "0/500", 5), None, None)
        .await
        .unwrap();
    assert!(id > 0);

    let pending = read_pending_ddl(
        &mut *tx,
        epoch,
        "public",
        "orders",
        "0/100".parse().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].c_lsn, "0/500".parse::<Lsn>().unwrap());
    assert_eq!(pending[0].c_tag, "ALTER TABLE");
    assert_eq!(pending[0].c_event, "ddl_command_end");
    assert_eq!(pending[0].schema_version, 5);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn read_pending_ddl_orders_by_c_lsn() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 800_004;

    // Insert out of LSN order.
    insert_ddl(&mut *tx, &ddl(epoch, "0/300", 3), None, None)
        .await
        .unwrap();
    insert_ddl(&mut *tx, &ddl(epoch, "0/100", 1), None, None)
        .await
        .unwrap();
    insert_ddl(&mut *tx, &ddl(epoch, "0/200", 2), None, None)
        .await
        .unwrap();

    let all = read_pending_ddl(&mut *tx, epoch, "public", "orders", "0/0".parse().unwrap())
        .await
        .unwrap();
    let lsns: Vec<Lsn> = all.iter().map(|r| r.c_lsn).collect();
    assert_eq!(
        lsns,
        vec![
            "0/100".parse().unwrap(),
            "0/200".parse().unwrap(),
            "0/300".parse().unwrap()
        ]
    );

    // after_lsn is a strict lower bound: only c_lsn > 0/150 (i.e. 0/200, 0/300).
    let after = read_pending_ddl(
        &mut *tx,
        epoch,
        "public",
        "orders",
        "0/150".parse().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].c_lsn, "0/200".parse::<Lsn>().unwrap());

    tx.rollback().await.unwrap();
}
