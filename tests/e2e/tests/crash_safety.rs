//! End-to-end crash safety (`architecture.md` "Crash safety" + §1.5): `SIGKILL` the sink mid-batch and
//! the loader mid-MERGE, restart both, and prove the pipeline reaches the **same** state it would have
//! without the crash — **effectively-once, no loss, no dupes, no resurrected deletes**. This is the payoff
//! of the durability rules built earlier: the sink advances `confirmed_flush_lsn` only after the S3 PUT +
//! manifest are durable, so an ungraceful kill re-streams a few already-durable changes; the loader's raw
//! `APPEND … ON CONFLICT DO NOTHING` + the guarded `MERGE` collapse that at-least-once replay into an
//! effectively-once result.
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

/// SIGKILL the sink after a batch's Parquet PUT (mid-batch — between the durable S3 object and the standby
/// update that would advance the slot), keep writing while it is down, restart it, and converge: the
/// mirror equals the source with no loss (the un-acked batch re-streams) and no dupes (append dedup).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn sink_killed_mid_batch_loses_nothing() {
    let mut h = Harness::start().await.expect("bring up sink + loader");

    // Batch 1: a steady stream of committed rows. Each small txn is a fresh commit the sink must PUT.
    for i in 0..400 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'batch1')"
        ))
        .await
        .unwrap();
    }
    // Wait until at least one Parquet object is durable in S3 — the sink is mid-batch (object PUT, but the
    // slot may not have advanced yet). Killing here exercises the "crash between PUT and standby" window.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while h.s3_list("orders").await.unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "no Parquet object appeared"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    h.kill_sink().await.unwrap();

    // Batch 2: committed WHILE the sink is dead — buffered in the slot's retained WAL. A correct resume
    // re-streams from `confirmed_flush_lsn` and loses none of these.
    for i in 400..800 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'batch2')"
        ))
        .await
        .unwrap();
    }
    h.restart_sink().await.expect("sink restarts and resumes");

    // Converge: capture a post-write watermark, drop a sentinel that lands last, await the loader past it.
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("pipeline converges after sink restart");
    h.stop_loader().await.unwrap();

    // Effectively-once: the mirror equals the source exactly — every committed row present once, no dupes.
    let n = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders_current")
        .unwrap();
    assert_eq!(
        n, 801,
        "all 800 rows + the sentinel, none lost, none duplicated"
    );
    h.assert_mirror_equals_source("orders").await.unwrap();
}

/// SIGKILL the loader right after Phase A appends a batch to `<table>_raw` but before Phase B's guarded
/// MERGE has caught up (`transformed_lsn < raw_appended_lsn` — the mid-MERGE window), restart it, and
/// converge: both watermarks recover consistently, the mirror equals the source, and a previously-applied
/// DELETE does **not** resurrect (the per-PK max-applied guard holds through the idempotent re-apply).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn loader_killed_mid_merge_is_idempotent() {
    let mut h = Harness::start().await.expect("bring up sink + loader");

    // Seed rows 0..300 and let the mirror catch up fully.
    for i in 0..300 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'seed')"
        ))
        .await
        .unwrap();
    }
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(120))
        .await
        .expect("seed converges");

    // A DELETE that has been applied to the mirror — it must stay deleted after the crash + replay (no
    // resurrection). Delete id=150 and let it land.
    let before_del = h.source_wal_lsn().await.unwrap();
    h.source_exec("DELETE FROM public.orders WHERE id = 150")
        .await
        .unwrap();
    h.source_exec("UPDATE public.orders SET status = 'sentinel2' WHERE id = 999999")
        .await
        .unwrap();
    h.await_transformed_past("orders", before_del, Duration::from_secs(120))
        .await
        .expect("delete converges");

    // A fresh batch (300..700) to give the loader Phase-A work to be killed in the middle of.
    let raw_before = control::read_checkpoint(h.control_pool(), h.epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap()
        .raw_appended_lsn;
    for i in 300..700 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'batch')"
        ))
        .await
        .unwrap();
    }
    // Kill the loader the instant Phase A has appended past the old watermark — Phase B has not caught up,
    // so `transformed_lsn < raw_appended_lsn`: the ungraceful kill lands mid-MERGE.
    h.await_raw_appended_past("orders", raw_before, Duration::from_secs(60))
        .await
        .expect("Phase A appends the new batch");
    h.kill_loader().await.unwrap();
    h.restart_loader()
        .await
        .expect("loader reclaims lease + resumes");

    // Converge, then compare. Idempotent re-apply must not double-apply or resurrect id=150.
    let before2 = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.orders SET status = 'sentinel3' WHERE id = 999999")
        .await
        .unwrap();
    h.await_transformed_past("orders", before2, Duration::from_secs(180))
        .await
        .expect("pipeline converges after loader restart");
    h.stop_loader().await.unwrap();

    // Both watermarks recovered consistently (Phase B never runs ahead of Phase A — the DB CHECK enforces
    // `transformed_lsn <= raw_appended_lsn`; convergence above proved they met).
    let resurrected = h
        .duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_current WHERE id = 150",
        )
        .unwrap();
    assert_eq!(
        resurrected, 0,
        "the applied DELETE did NOT resurrect on replay"
    );
    // Effectively-once: mirror == source (0..700 minus 150, plus the sentinel), no double-apply.
    h.assert_mirror_equals_source("orders").await.unwrap();
}
