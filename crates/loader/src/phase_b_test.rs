use super::queue_drained_through;

#[test]
fn explicit_end_marker_completes_without_a_data_row() {
    let end = "0/200".parse().unwrap();
    assert!(
        queue_drained_through(None, end),
        "an empty ordered queue is enough once the durable H marker exists"
    );
}

#[test]
fn end_marker_waits_for_every_file_at_or_below_h() {
    let end = "0/200".parse().unwrap();
    assert!(!queue_drained_through(Some("0/1ff".parse().unwrap()), end));
    assert!(!queue_drained_through(Some(end), end));
    assert!(queue_drained_through(Some("0/201".parse().unwrap()), end));
}
