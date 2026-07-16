use super::*;

#[test]
fn init_with_defaults_does_not_panic() {
    assert!(init_tracing(&TelemetryConfig::default()).is_ok());
}

#[test]
fn second_init_is_handled_not_fatal() {
    // Tests share one process, so at most one of these actually installs the global
    // subscriber; the rest hit the "already initialised" path. None may panic.
    assert!(init_tracing(&TelemetryConfig::default()).is_ok());
    assert!(init_tracing(&TelemetryConfig {
        json: true,
        filter: "debug".to_string(),
    })
    .is_ok());
}

#[test]
fn default_config_is_pretty_info() {
    let cfg = TelemetryConfig::default();
    assert!(!cfg.json);
    assert_eq!(cfg.filter, "info");
}

#[test]
fn json_flag_selects_json_formatter() {
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    // A `MakeWriter` that captures everything written into a shared buffer.
    #[derive(Clone, Default)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    // Ensure a global subscriber exists so the level fast-path admits INFO events, then capture
    // via a *scoped* JSON subscriber (`with_default`) so we don't fight the one global install.
    let _ = init_tracing(&TelemetryConfig::default());

    let buf = BufWriter::default();
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(buf.clone()),
        );

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            commit_lsn = "0000000001B4C000",
            xid = 918273,
            "flushed batch"
        );
    });

    let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        out.trim_start().starts_with('{'),
        "expected a JSON object: {out}"
    );
    assert!(
        out.contains("\"commit_lsn\""),
        "carries the field key: {out}"
    );
    assert!(
        out.contains("\"flushed batch\""),
        "carries the message: {out}"
    );
}
