//! End-to-end total-restart (`architecture.md` "Slot loss / total-restart" + §1.8): when the single
//! lifelong slot is **lost** on a successful connection, the sink bumps the epoch, opens a new slot with
//! a fresh exported snapshot, and re-snapshots every table under the new generation; the loaders detect
//! the new epoch and rebuild every `.duckdb` (raw re-appended, mirror re-derived), resetting **both**
//! watermarks. And the load-bearing guard: a **transient disconnect is NOT slot loss** — it resumes from
//! `confirmed_flush` and must never bump the epoch.
//!
//! The self-heal model is crash-and-restart (§ startup/bootstrap): dropping the slot terminates the
//! sink's walsender, so the sink exits; on restart it classifies the slot and total-restarts. The loader
//! likewise exits on a detected epoch bump and rebuilds at its next bootstrap. The harness plays the
//! orchestrator that restarts the crashed processes.
//!
//!   docker compose -f deploy/docker/docker-compose.yml up --wait
//!   cargo test -p e2e --features it -- --ignored
#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

/// Drop the slot mid-run → the sink total-restarts (epoch bump + re-snapshot) and the loader rebuilds
/// every `.duckdb` under the new generation, resetting both watermarks; the mirror converges to the
/// source (a prior DELETE stays deleted — the mirror is re-derived from the new snapshot, not the stale one).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn dropping_the_slot_triggers_epoch_bump_and_full_rebuild() {
    let mut h = Harness::start().await.expect("bring up sink + loader");
    assert_eq!(h.current_epoch().await.unwrap(), 1, "starts at epoch 1");

    // Steady state under epoch 1: seed rows + a DELETE, converged into the mirror.
    for i in 0..200 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'gen1')"
        ))
        .await
        .unwrap();
    }
    h.source_exec("DELETE FROM public.orders WHERE id = 100")
        .await
        .unwrap();
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("epoch-1 converges");

    // DROP the slot mid-run → the sink's walsender is terminated and the sink exits; the change history
    // since confirmed_flush is now gone. Restart the sink: on a SUCCESSFUL connection it finds the slot
    // ABSENT → TOTAL-RESTART (bump the epoch, new slot, re-snapshot every table under epoch 2).
    h.drop_slot().await.expect("drop the replication slot");
    h.restart_sink()
        .await
        .expect("sink restarts → total-restart");
    let new_epoch = h
        .await_epoch_past(1, Duration::from_secs(60))
        .await
        .expect("the slot loss bumped the epoch");
    assert_eq!(new_epoch, 2, "a new generation was opened");
    h.refresh_epoch().await.unwrap();
    assert!(
        h.sink_log_contains("TOTAL-RESTART"),
        "the sink logged a loud total-restart"
    );

    // The loader must rebuild every .duckdb under the new epoch. It self-exits on the detected bump; kill
    // it (tolerant of an already-exited process) and restart so bootstrap wipes + rebuilds under epoch 2.
    h.stop_loader().await.unwrap();
    h.restart_loader()
        .await
        .expect("loader restarts → rebuild under epoch 2");

    // Converge under epoch 2, then compare.
    let before2 = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.orders SET status = 'sentinel2' WHERE id = 999999")
        .await
        .unwrap();
    h.await_transformed_past("orders", before2, Duration::from_secs(240))
        .await
        .expect("epoch-2 converges");
    h.stop_loader().await.unwrap();

    // Both watermarks reset then advanced under the NEW epoch — the epoch-2 checkpoint is a fresh row that
    // has moved off `0/0` (its predecessor was epoch 1, untouched).
    let cp = control::read_checkpoint(h.control_pool(), 2, "public", "orders")
        .await
        .unwrap()
        .expect("epoch-2 checkpoint exists");
    assert!(
        cp.transformed_lsn > common::Lsn::ZERO && cp.raw_appended_lsn >= cp.transformed_lsn,
        "epoch-2 watermarks reset then advanced consistently"
    );

    // Rebuilt-from-the-new-snapshot mirror == source (0..200 minus id=100, plus the sentinel).
    h.assert_mirror_equals_source("orders").await.unwrap();
    let n = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders_current")
        .unwrap();
    assert_eq!(n, 200, "199 gen1 rows (id=100 deleted) + sentinel");
    let resurrected = h
        .duckdb_scalar(
            "orders",
            "SELECT count(*) FROM orders_current WHERE id = 100",
        )
        .unwrap();
    assert_eq!(
        resurrected, 0,
        "the deleted row did not resurrect through the rebuild"
    );
}

/// A transient disconnect (walsender terminated, slot intact) must NOT total-restart: the sink resumes
/// from `confirmed_flush` under the SAME epoch — the false-positive guard (§1.8).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn transient_disconnect_does_not_trigger_total_restart() {
    let mut h = Harness::start().await.expect("bring up sink + loader");
    for i in 0..100 {
        h.source_exec(&format!(
            "INSERT INTO public.orders (id, status) VALUES ({i}, 'gen1')"
        ))
        .await
        .unwrap();
    }
    let before = h.source_wal_lsn().await.unwrap();
    h.source_exec("INSERT INTO public.orders (id, status) VALUES (999999, 'sentinel')")
        .await
        .unwrap();
    h.await_transformed_past("orders", before, Duration::from_secs(180))
        .await
        .expect("converges");
    assert_eq!(
        h.current_epoch().await.unwrap(),
        1,
        "epoch 1 before the blip"
    );

    // A TRANSIENT disconnect: terminate the sink's walsender WITHOUT dropping the slot (the slot survives).
    // The sink exits on the dropped connection; restart it — it classifies the slot HEALTHY (present) and
    // RESUMES from confirmed_flush. This must NOT bump the epoch or re-snapshot.
    h.terminate_walsender()
        .await
        .expect("bounce the sink's replication connection");
    h.restart_sink().await.expect("sink restarts → resume");

    // A fresh write converges under the SAME epoch.
    let before2 = h.source_wal_lsn().await.unwrap();
    h.source_exec("UPDATE public.orders SET status = 'sentinel2' WHERE id = 999999")
        .await
        .unwrap();
    h.await_transformed_past("orders", before2, Duration::from_secs(180))
        .await
        .expect("resume converges");
    h.stop_loader().await.unwrap();

    assert_eq!(
        h.current_epoch().await.unwrap(),
        1,
        "a transient disconnect must NOT bump the epoch (no total-restart)"
    );
    assert!(
        !h.sink_log_contains("TOTAL-RESTART"),
        "the restarted sink resumed — it did NOT total-restart on a transient blip"
    );
    h.assert_mirror_equals_source("orders").await.unwrap();
    let n = h
        .duckdb_scalar("orders", "SELECT count(*) FROM orders_current")
        .unwrap();
    assert_eq!(n, 101, "100 rows + sentinel, resumed intact");
}
