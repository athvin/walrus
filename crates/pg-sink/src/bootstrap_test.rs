use super::*;

#[tokio::test]
async fn retry_returns_immediately_on_terminal() {
    let deadline = Instant::now() + Duration::from_secs(3600);
    let mut calls = 0;
    let out: Result<(), Error> = retry_transient(deadline, "x", || {
        calls += 1;
        async { Err(Error::Config("bad".into())) }
    })
    .await;
    assert!(matches!(out, Err(Error::Config(_))));
    assert_eq!(calls, 1, "terminal errors are not retried");
}

#[tokio::test]
async fn retry_gives_up_at_deadline_with_the_transient_error() {
    // Deadline already elapsed → one attempt, then surface the transient error.
    let deadline = Instant::now();
    let out: Result<(), Error> = retry_transient(deadline, "control database", || async {
        Err(Error::ControlDb("connection refused".into()))
    })
    .await;
    match out {
        Err(e) => {
            assert!(e.is_transient());
            assert_eq!(e.exit_code(), common::ExitCode::ControlDb);
        }
        Ok(()) => panic!("expected the transient error to be surfaced"),
    }
}

#[tokio::test]
async fn retry_succeeds_after_a_transient_blip() {
    let deadline = Instant::now() + Duration::from_secs(3600);
    let mut attempts = 0;
    let out: Result<u8, Error> = retry_transient(deadline, "object store", || {
        attempts += 1;
        async move {
            if attempts < 2 {
                Err(Error::ObjectStore("503".into()))
            } else {
                Ok(7)
            }
        }
    })
    .await;
    assert_eq!(out.unwrap(), 7);
    assert_eq!(attempts, 2);
}
