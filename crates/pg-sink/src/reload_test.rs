#![allow(
    clippy::unreachable,
    reason = "unit-test fakes: unreachable arms assert scripted lease outcomes"
)]

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn a_panicking_exporter_is_observed_not_swallowed() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(async { panic!("exporter blew up mid-PUT") });
    let joined = set.join_next().await.expect("one task was spawned");
    assert_eq!(observe_exporter_end(joined), ExporterExit::Panicked);

    set.spawn(async {});
    let joined = set.join_next().await.expect("one task was spawned");
    assert_eq!(observe_exporter_end(joined), ExporterExit::Completed);
}

#[tokio::test(start_paused = true)]
async fn drain_is_bounded_and_aborts_a_wedged_exporter() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(std::future::pending::<()>());
    let budget = Duration::from_secs(5);
    let started = tokio::time::Instant::now();

    drain_exporters(&mut set, budget).await;

    assert!(set.is_empty(), "the aborted task must be joined");
    assert_eq!(started.elapsed(), budget);
}

#[test]
fn preflight_rejections_read_as_operator_reasons() {
    // These strings land verbatim in table_reload.error — they ARE the operator UX.
    assert_eq!(
        PreflightRejection::NotPublished("public".into(), "ghost".into()).to_string(),
        "table public.ghost is not in the publication"
    );
    assert_eq!(
        PreflightRejection::NoPrimaryKey("public".into(), "keyless".into()).to_string(),
        "table public.keyless has no primary key"
    );
}

#[test]
fn an_unpublished_keyless_table_reports_the_publication_gap_first() {
    // The two catalog reads run concurrently, so BOTH answers are always available here: the
    // precedence is `classify_target`'s ordering alone. A table that is neither published nor keyed
    // must still name the publication — the gap the operator fixes first.
    let both_wrong = classify_target(false, false, "public", "ghost").unwrap_err();
    assert!(
        matches!(both_wrong, PreflightRejection::NotPublished(..)),
        "not-published outranks no-primary-key; got {both_wrong}"
    );

    let keyless = classify_target(true, false, "public", "keyless").unwrap_err();
    assert!(
        matches!(keyless, PreflightRejection::NoPrimaryKey(..)),
        "a published keyless table is rejected for its key; got {keyless}"
    );

    assert!(classify_target(true, true, "public", "orders").is_ok());
}

#[test]
fn an_ad_hoc_preflight_failure_is_infra_never_a_rejection() {
    // `preflight` propagates its catalog queries with `?`; the From impl is what keeps a dead
    // connection out of `table_reload.error` as a false "not in the publication" reason.
    let outcome = PreflightOutcome::from(anyhow::anyhow!("connection closed mid-query"));

    match outcome {
        PreflightOutcome::Infra(e) => assert_eq!(e.to_string(), "connection closed mid-query"),
        PreflightOutcome::Rejected(r) => panic!("an infra failure must never reject: {r}"),
    }
}

#[tokio::test(start_paused = true)]
async fn lost_lease_cancels_the_exporter() {
    let token = CancellationToken::new();
    // First renewal succeeds, second reports the lease gone. The `AsyncFnMut` bound lets the
    // attempt mutate a plain local counter — no `Arc<AtomicUsize>` to smuggle it out of the
    // closure, which the old `R: FnMut() -> Fut` bound would have forced.
    let mut calls = 0_usize;
    let end = lease_guarded_export(
        token,
        Duration::from_secs(20),
        async || {
            calls += 1;
            Ok(calls == 1)
        },
        async {
            std::future::pending::<()>().await;
            unreachable!()
        },
    )
    .await;
    assert!(matches!(end, ExporterEnd::LostLease), "got {end:?}");
    assert_eq!(calls, 2);
}

#[tokio::test(start_paused = true)]
async fn transient_renewal_errors_do_not_cancel() {
    let token = CancellationToken::new();
    let cancel = token.clone();
    let mut calls = 0_usize;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(70)).await;
        cancel.cancel();
    });
    let end = lease_guarded_export(
        token,
        Duration::from_secs(20),
        async || {
            calls += 1;
            Err(anyhow::anyhow!("control-pg blinked"))
        },
        async {
            std::future::pending::<()>().await;
            unreachable!()
        },
    )
    .await;
    // Errors are retried (the lease expiry is the real deadline); only cancellation ends it.
    assert!(matches!(end, ExporterEnd::Cancelled), "got {end:?}");
    assert!(calls >= 3);
}

#[tokio::test]
async fn cap_of_two_schedules_third_request_only_after_a_permit_frees() {
    // The scheduling shape `tick` relies on: permits live inside the exporter tasks, so a
    // third export starts only when one of the first two releases its permit.
    let semaphore = Arc::new(Semaphore::new(2));
    let started: Vec<Arc<AtomicUsize>> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let mut releases = Vec::new();
    // A `JoinSet`, like the controller's own exporter set — the fake exporters are held and drained
    // exactly the way `spawn_exporter` / `drain_exporters` hold and drain the real ones.
    let mut exporters = tokio::task::JoinSet::new();
    for flag in &started {
        let sem = Arc::clone(&semaphore);
        let flag = Arc::clone(flag);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        releases.push(Some(tx));
        exporters.spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            flag.store(1, Ordering::SeqCst);
            let _ = rx.await; // park like the PR 6.5 stub, holding the permit
        });
    }

    // Two acquire; the third is parked on the semaphore.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started_count = || {
        started
            .iter()
            .filter(|f| f.load(Ordering::SeqCst) == 1)
            .count()
    };
    assert_eq!(started_count(), 2, "cap of two holds; the third waits");

    // Free exactly ONE permit (release a task that actually started): the third now runs.
    let running_idx = started
        .iter()
        .position(|f| f.load(Ordering::SeqCst) == 1)
        .unwrap();
    releases[running_idx].take().unwrap().send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(started_count(), 3, "the third started once a permit freed");

    for tx in releases.into_iter().flatten() {
        let _ = tx.send(());
    }
    while let Some(joined) = exporters.join_next().await {
        joined.unwrap(); // a panicking fake exporter must fail the test, not be swallowed
    }
    assert_eq!(semaphore.available_permits(), 2, "permits returned on exit");
}
