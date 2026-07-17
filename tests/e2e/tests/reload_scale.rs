#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! End-to-end: N-table reloads at scale on ONE slot (PR 6.12, reload §2/§5). Three tables are
//! seeded and streamed by the real sink+loader, then reloaded concurrently with the sink's
//! `max_concurrent_reloads = 2`. The load-bearing assertions: never more than 2 `exporting` at any
//! sample, exactly **one** replication slot on the source the entire time, all three reloads reach
//! `complete`, and all three mirrors equal the source row-for-row. This is the "at scale" clause of
//! the original ask — N reloads, one slot, no per-reload slot proliferation.
//!
//!   cargo test -p e2e --features it -- --ignored n_table_reloads

#![cfg(feature = "it")]

use e2e::Harness;
use std::time::Duration;

// Harness-owned fixtures (created before bootstrap so the loader owns them, PR 6.12).
const TABLES: [&str; 3] = ["rl1", "rl2", "rl3"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker compose up --wait (source PG + control PG + MinIO)"]
async fn n_table_reloads_respect_the_cap_on_one_slot() {
    let mut h = Harness::start().await.unwrap();

    // Seed the harness-owned tables (truncated at start) and let the pipeline mirror each. 5k rows
    // is a middle ground: big enough that the reloads take real work (so the cap semaphore genuinely
    // gates the third), small enough that all three apply + complete comfortably in the deadline.
    for t in TABLES {
        h.source_batch(&format!(
            "INSERT INTO public.{t} SELECT g, 'v' || g FROM generate_series(1, 5000) g;"
        ))
        .await
        .unwrap();
    }
    let before = h.source_wal_lsn().await.unwrap();
    // A tiny post-seed write per table gives the loader a watermark to converge past.
    for t in TABLES {
        h.source_exec(&format!(
            "UPDATE public.{t} SET status = 'seeded' WHERE id = 1"
        ))
        .await
        .unwrap();
    }
    for t in TABLES {
        h.await_transformed_past(t, before, Duration::from_secs(90))
            .await
            .unwrap();
    }

    // Request a rebuild-flavor reload on all three at once. The sink's cap is 2. Own the pool
    // (Arc-backed) so it doesn't borrow `h` across the `stop_loader()` (`&mut h`) before the diff.
    let epoch = h.epoch;
    let pool = h.control_pool().clone();
    let mut reload_ids = Vec::new();
    for t in TABLES {
        let id = control::reload::request(
            &pool,
            epoch,
            "public",
            t,
            control::reload::ReloadFlavor::Reload,
        )
        .await
        .unwrap();
        reload_ids.push(id);
    }

    // (No concurrent source churn here — the reloads' own signal/echo traffic exercises the one
    // slot, and a static source keeps the final mirror==source comparison free of catch-up races.
    // The "other tables keep streaming during a reload" no-stall promise is proven in
    // `reload_quarantine.rs`.)

    // Poll to completion, sampling the two invariants on a tight cadence: the cap is never breached
    // (≤ 2 exporting at every sample) and exactly one slot exists throughout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    let mut max_exporting = 0i64;
    loop {
        let exporting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM walrus.table_reload WHERE epoch = $1 AND status = 'exporting'",
        )
        .bind(epoch)
        .fetch_one(&pool)
        .await
        .unwrap();
        max_exporting = max_exporting.max(exporting);
        assert!(
            exporting <= 2,
            "cap breached: {exporting} exporting at once"
        );

        let slots: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_replication_slots")
            .fetch_one(h.source_pool())
            .await
            .unwrap();
        assert_eq!(slots, 1, "exactly one slot throughout (got {slots})");

        let complete: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM walrus.table_reload WHERE epoch = $1 AND status = 'complete'",
        )
        .bind(epoch)
        .fetch_one(&pool)
        .await
        .unwrap();
        if complete == TABLES.len() as i64 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "reloads did not all complete in time ({complete}/3 complete)"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // All three reached `complete`, none ever exceeded the cap of 2 concurrent exporters (the
    // load-bearing invariant, asserted at every sample above). `max_exporting` is logged, not
    // asserted `>= 2`: a fast reload can slip through `exporting` between samples, so observing 2
    // concurrent is best-effort — the never-breached bound plus the unit test
    // `cap_of_two_holds_and_the_stream_keeps_flowing` are the real concurrency proof.
    eprintln!("max concurrently-exporting observed: {max_exporting}");
    for id in reload_ids {
        let row = control::reload::get(&pool, id).await.unwrap().unwrap();
        assert_eq!(row.status, control::reload::ReloadStatus::Complete);
    }

    // Every mirror equals its source, row for row. Stop the loader first — it holds an exclusive
    // lock on each `.duckdb`, so the diff helper opens the files read-only only once it's down.
    h.stop_loader().await.unwrap();
    for t in TABLES {
        h.assert_mirror_equals_source(t).await.unwrap();
    }
}
