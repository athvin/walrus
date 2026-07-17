#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! End-to-end: the anchor use case (reload §2) — a lossy `ALTER COLUMN TYPE` quarantine recovered
//! via `just reload`, while the other tables stream on. The real sink+loader run; the lossy
//! narrowing is INJECTED (a v2 int2 registry + a v2 manifest file, as `ddl_destructive.rs` does)
//! rather than driven by a source `ALTER` — a real ALTER validates the source data, so the loader
//! (applying changes in LSN order) never sees a value that overflows by the time it reconciles; the
//! quarantine is fundamentally a DuckDB cast the source already accepted. The injection makes the
//! mirror's `99999` meet the int2 narrowing → genuine `LoaderError::Quarantine` → the loader exits.
//!
//! Then: `reload` the table, restart the loader, and it recovers. Three PR 6.12 loader fixes make
//! recovery possible: (1) a worker error cancels the shutdown token so a multi-table quarantine
//! *exits* the loader instead of deadlocking (`main.rs`); (2) bootstrap SKIPS the forward reconcile
//! when a rebuild reload is pending, so the restart doesn't re-quarantine before Phase A runs
//! (`bootstrap.rs`); (3) Phase A skips the superseded version-crossing blocker so the reload chunk's
//! rebuild clears the quarantine (`phase_a.rs`). The other tables' `transformed_lsn` strictly
//! advances across the reload window — they recover once the restarted loader catches them up.
//!
//!   cargo test -p e2e --features it -- --ignored quarantined_table_recovers

#![cfg(feature = "it")]

use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity};
use e2e::Harness;
use std::time::Duration;

// Harness-owned fixtures (created before bootstrap so the loader owns them, PR 6.12).
const OTHERS: [&str; 2] = ["rl1", "rl2"];

fn col(name: &str, oid: u32, key: bool) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key: key,
    }
}

fn q_target_rel(n_oid: u32) -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "q_target".into(),
        replica_identity: ReplicaIdentity::Default,
        // int4=23, text=25, int2=21
        columns: vec![
            col("id", 23, true),
            col("status", 25, false),
            col("n", n_oid, false),
        ],
    }
}

/// Write a one-row `(id, status, n)` v2 Parquet to MinIO (the reconcile trigger's file — its data is
/// never appended, the quarantine fires on the reconcile before the append). DuckDB writes it.
fn write_v2_file(epoch: i64, uri_key: &str) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    w.execute_batch(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='us-east-1'; SET s3_endpoint='localhost:9000'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin'; \
         CREATE TABLE fixture (id INTEGER, status VARCHAR, n VARCHAR, walrus_pg_sink_meta VARCHAR); \
         INSERT INTO fixture VALUES (2, 'x', '5', \
           '{\"op\":\"Insert\",\"commit_lsn\":\"00000000000000C8\",\"lsn\":\"00000000000000C8\",\
             \"sink_processed_at\":\"2026-07-16T00:00:00Z\"}');",
    )
    .unwrap();
    let uri = format!("s3://walrus/{epoch}/public/q_target/{uri_key}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

async fn transformed(h: &Harness, table: &str) -> Lsn {
    control::read_checkpoint(h.control_pool(), h.epoch, "public", table)
        .await
        .unwrap()
        .map(|c| c.transformed_lsn)
        .unwrap_or(Lsn::ZERO)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn quarantined_table_recovers_via_reload_without_stalling_others() {
    let mut h = Harness::start().await.unwrap();
    let epoch = h.epoch;

    // q_target is the quarantine target (harness-owned, id/status/n int4); rl1/rl2 keep streaming.
    // The harness truncated all of them at start, so just seed rows.
    h.source_batch("INSERT INTO public.q_target VALUES (1, 's1', 99999);")
        .await
        .unwrap();
    for t in OTHERS {
        h.source_batch(&format!(
            "INSERT INTO public.{t} SELECT g, 'v' || g FROM generate_series(1, 200) g;"
        ))
        .await
        .unwrap();
    }
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.q_target SET status = 's1' WHERE id = 1")
        .await
        .unwrap();
    for t in OTHERS {
        h.source_exec(&format!(
            "UPDATE public.{t} SET status = 'seed' WHERE id = 1"
        ))
        .await
        .unwrap();
    }
    h.await_transformed_past("q_target", before, Duration::from_secs(90))
        .await
        .unwrap();
    for t in OTHERS {
        h.await_transformed_past(t, before, Duration::from_secs(90))
            .await
            .unwrap();
    }

    // A low-rate writer keeps rl1/rl2 advancing across the whole window (else their transformed_lsn
    // legitimately sits still and the recovery assertion proves nothing).
    let churn = {
        let src = h.source_pool().clone();
        tokio::spawn(async move {
            for r in 0..60 {
                for t in OTHERS {
                    let _ = sqlx::query(&format!(
                        "UPDATE public.{t} SET status = 'c{r}' WHERE id = ((random()*199)::int + 1)"
                    ))
                    .execute(&src)
                    .await;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    };

    // INJECT the lossy narrowing: register q_target v2 (n int2) and drop a v2 file into the queue. The
    // running loader claims it, reconciles the mirror's 99999 to int2 → overflow → QUARANTINE → the
    // loader process exits (a worker Err cancels the token).
    // Own the pool (PgPool is Arc-backed) so it doesn't borrow `h` across the `&mut h` calls
    // (await_loader_exited / restart_loader) below.
    let pool = h.control_pool().clone();
    control::upsert_registry(
        &pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".into(),
            source_table: "q_target".into(),
            schema_version: 2,
            descriptors: Vec::new(),
            columns: serde_json::to_value(q_target_rel(21)).unwrap(),
        },
    )
    .await
    .unwrap();
    let uri = write_v2_file(epoch, "inject-v2");
    control::insert_ready(
        &pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "q_target".into(),
            s3_uri: uri,
            kind: control::ManifestKind::Stream,
            row_count: 1,
            lsn_start: "0/C8".parse().unwrap(),
            lsn_end: "0/C8".parse().unwrap(),
            schema_version: 2,
            reload_id: None,
        },
    )
    .await
    .unwrap();

    h.await_loader_exited(Duration::from_secs(60))
        .await
        .unwrap();
    // Sample the other tables at the quarantine freeze — the pre-reload baseline.
    let pre_others: Vec<Lsn> = {
        let mut v = Vec::new();
        for t in OTHERS {
            v.push(transformed(&h, t).await);
        }
        v
    };

    // The operator's fix: reload q_target. The sink (still up) exports it at v2 from the source, whose
    // n=99999 fits int4 but must fit int2 for the rebuild — so shrink it first (the value the mirror
    // could never cast is corrected at the source; the reload carries the fitting value).
    h.source_exec("UPDATE public.q_target SET n = 100 WHERE id = 1")
        .await
        .unwrap();
    let reload_id = control::reload::request(
        &pool,
        epoch,
        "public",
        "q_target",
        control::reload::ReloadFlavor::Reload,
    )
    .await
    .unwrap();

    // Wait for the sink's export to complete, then restart the loader to apply + rebuild.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let row = control::reload::get(&pool, reload_id)
            .await
            .unwrap()
            .unwrap();
        if matches!(
            row.status,
            control::reload::ReloadStatus::ExportComplete | control::reload::ReloadStatus::Complete
        ) {
            break;
        }
        assert!(
            row.status != control::reload::ReloadStatus::Failed,
            "reload failed: {:?}",
            row.error
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "export never completed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    h.restart_loader().await.unwrap();

    // The loader recovers q_target (skips the superseded v2 blocker → reload chunk → rebuild → clear)
    // and resumes the other tables. Wait for q_target to reach `complete`.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let st = control::reload::get(&pool, reload_id)
            .await
            .unwrap()
            .unwrap()
            .status;
        if st == control::reload::ReloadStatus::Complete {
            break;
        }
        assert!(
            st != control::reload::ReloadStatus::Failed,
            "reload failed post-restart"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "reload never completed"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    churn.abort();

    // The single-slot promise: post-restart the loader caught the OTHER tables up while q_target
    // rebuilt — each advances strictly past its pre-reload (quarantine-freeze) sample. Poll with a
    // deadline while the loader is still running (transformed_lsn lives in control-pg, so we read it
    // before stopping the loader for the DuckDB diff).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    for (t, pre) in OTHERS.iter().zip(pre_others.iter()) {
        loop {
            if transformed(&h, t).await > *pre {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{t} transformed_lsn never advanced past the pre-reload freeze {pre}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    // Exactly one slot on the source throughout.
    let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_replication_slots")
        .fetch_one(h.source_pool())
        .await
        .unwrap();
    assert_eq!(slots, 1, "one slot on the source (got {slots})");

    // A DDL restart may or may not have fired during recovery (tolerate restart_count ∈ {0,1}).
    let done = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        done.restart_count <= 1,
        "restart_count ∈ {{0,1}}: {}",
        done.restart_count
    );

    // Recovery in DuckDB: stop the loader (it locks each `.duckdb`), then read the mirror. q_target
    // equals source, and the narrowed int2 column is live with the recovered value.
    h.stop_loader().await.unwrap();
    h.assert_mirror_equals_source("q_target").await.unwrap();
    let n_type = h
        .duckdb_rows("q_target", "SELECT typeof(n) FROM q_target WHERE id = 1")
        .unwrap();
    assert_eq!(
        n_type,
        vec!["SMALLINT".to_string()],
        "n narrowed to int2 in the mirror"
    );
    let n_val = h
        .duckdb_scalar("q_target", "SELECT n FROM q_target WHERE id = 1")
        .unwrap();
    assert_eq!(
        n_val, 100,
        "the recovered value is the source's fitting one"
    );
}
