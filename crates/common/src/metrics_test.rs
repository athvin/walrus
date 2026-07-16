use super::*;

#[test]
fn render_lists_every_series() {
    init();
    init_table_series("public.demo");
    // Exercise a couple of helpers to prove the wired path renders too.
    set_wal_status(0);
    record_batch_flush(0.01, 4096);
    set_transform_lag("public.demo", 0);

    let text = render();
    for name in names::SINK_ALL {
        assert!(
            text.contains(name),
            "sink series {name} missing from /metrics"
        );
    }
    for name in names::LOADER_ALL {
        assert!(
            text.contains(name),
            "loader series {name} missing from /metrics"
        );
    }
}
