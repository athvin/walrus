use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// When cancellation and periodic work are both ready, shutdown must win every selection.
#[tokio::test]
async fn biased_select_prefers_cancellation() {
    let token = CancellationToken::new();
    token.cancel();
    let mut tick = tokio::time::interval(Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut cancels = 0_u32;
    for _ in 0..100 {
        tokio::select! {
            () = token.cancelled() => cancels += 1,
            _ = tick.tick() => {}
        }
    }
    assert_eq!(cancels, 100, "shutdown must always win a ready race");
}
