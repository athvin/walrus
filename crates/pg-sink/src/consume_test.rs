use std::cell::Cell;
use std::time::Duration;

#[test]
fn build_names_the_first_missing_field() {
    // The DecodeLoopBuilder compile-fail doctest is the real dropped-setter regression; a runtime
    // test cannot express a compile-time unused-result error.
    let Err(error) = super::DecodeLoop::builder().build() else {
        panic!("an empty builder must reject its first missing field");
    };
    assert_eq!(
        error.to_string(),
        "decode loop builder: missing required field `stream`"
    );
}

/// Stands in for a frame future that makes partial progress before completing.
async fn stepwise(progress: &Cell<u32>, steps: u32) {
    for _ in 0..steps {
        tokio::time::sleep(Duration::from_millis(10)).await;
        progress.set(progress.get() + 1);
    }
}

#[tokio::test(start_paused = true)]
async fn pinned_branch_survives_other_arm_firing() {
    let progress = Cell::new(0);
    let frame = stepwise(&progress, 5);
    tokio::pin!(frame);
    let mut ticker = tokio::time::interval(Duration::from_millis(3));
    let mut interruptions = 0_u32;
    loop {
        tokio::select! {
            () = &mut frame => break,
            _ = ticker.tick() => interruptions += 1,
        }
    }
    assert!(interruptions > 0, "the sibling arm must win at least once");
    assert_eq!(progress.get(), 5, "partial progress must survive");
}

#[tokio::test(start_paused = true)]
async fn recreated_branch_loses_progress() {
    let progress = Cell::new(0);
    let mut ticker = tokio::time::interval(Duration::from_millis(3));
    let mut interruptions = 0_u32;
    for _ in 0..10 {
        tokio::select! {
            () = stepwise(&progress, 5) => break,
            _ = ticker.tick() => interruptions += 1,
        }
    }
    assert_eq!(interruptions, 10, "the sibling arm must keep interrupting");
    assert_eq!(progress.get(), 0, "recreating the future loses progress");
}
