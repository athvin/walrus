use super::*;

#[tokio::test]
async fn token_is_live_until_cancelled() {
    let token = install_signal_handlers();
    assert!(!token.is_cancelled());
    // A cancel from another source trips the same token and unwinds the signal task.
    token.cancel();
    token.cancelled().await;
    assert!(token.is_cancelled());
}
