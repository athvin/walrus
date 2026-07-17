use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

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

#[tokio::test(start_paused = true)]
async fn lost_lease_cancels_the_exporter() {
    let token = CancellationToken::new();
    // First renewal succeeds, second reports the lease gone.
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in = calls.clone();
    let end = lease_guarded_export(
        token,
        Duration::from_secs(20),
        move || {
            let n = calls_in.fetch_add(1, Ordering::SeqCst);
            async move { Ok(n == 0) }
        },
        async {
            std::future::pending::<()>().await;
            unreachable!()
        },
    )
    .await;
    assert!(matches!(end, ExporterEnd::LostLease), "got {end:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn transient_renewal_errors_do_not_cancel() {
    let token = CancellationToken::new();
    let cancel = token.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in = calls.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(70)).await;
        cancel.cancel();
    });
    let end = lease_guarded_export(
        token,
        Duration::from_secs(20),
        move || {
            calls_in.fetch_add(1, Ordering::SeqCst);
            async move { Err(anyhow::anyhow!("control-pg blinked")) }
        },
        async {
            std::future::pending::<()>().await;
            unreachable!()
        },
    )
    .await;
    // Errors are retried (the lease expiry is the real deadline); only cancellation ends it.
    assert!(matches!(end, ExporterEnd::Cancelled), "got {end:?}");
    assert!(calls.load(Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn cap_of_two_schedules_third_request_only_after_a_permit_frees() {
    // The scheduling shape `tick` relies on: permits live inside the exporter tasks, so a
    // third export starts only when one of the first two releases its permit.
    let semaphore = Arc::new(Semaphore::new(2));
    let started: Vec<Arc<AtomicUsize>> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let mut releases = Vec::new();
    let mut handles = Vec::new();
    for flag in &started {
        let sem = semaphore.clone();
        let flag = flag.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        releases.push(Some(tx));
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            flag.store(1, Ordering::SeqCst);
            let _ = rx.await; // park like the PR 6.5 stub, holding the permit
        }));
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
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(semaphore.available_permits(), 2, "permits returned on exit");
}
