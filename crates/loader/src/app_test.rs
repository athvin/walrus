use super::*;
use common::ObjectStoreConfig;

/// A config differing from the default only in the two object-store knobs `duck_s3_access` reads,
/// spelled as `WALRUS_OBJECT_STORE__ENDPOINT` / `__REGION` would deserialize them.
fn cfg(endpoint: Option<&str>) -> LoaderConfig {
    LoaderConfig {
        object_store: ObjectStoreConfig {
            bucket: "walrus-staging".to_string(),
            endpoint: endpoint.map(String::from),
            region: "eu-west-2".to_string(),
        },
        ..LoaderConfig::default()
    }
}

/// DuckDB's httpfs wants a scheme-less `host:port`; the scheme is what selects TLS, and MinIO in
/// compose is served over plain HTTP.
#[test]
fn an_http_endpoint_loses_its_scheme_and_leaves_tls_off() {
    let access = duck_s3_access(&cfg(Some("http://minio:9000")));

    assert_eq!(access.endpoint, "minio:9000");
    assert!(!access.use_ssl);
}

#[test]
fn an_https_endpoint_loses_its_scheme_and_turns_tls_on() {
    let access = duck_s3_access(&cfg(Some("https://s3.eu-west-2.amazonaws.com")));

    assert_eq!(access.endpoint, "s3.eu-west-2.amazonaws.com");
    assert!(access.use_ssl);
}

/// A scheme-less endpoint is already in DuckDB's spelling, so it passes through untouched — and
/// stays plain HTTP, because only `https://` asks for TLS.
#[test]
fn a_scheme_less_endpoint_passes_through_verbatim() {
    let access = duck_s3_access(&cfg(Some("localhost:9000")));

    assert_eq!(access.endpoint, "localhost:9000");
    assert!(!access.use_ssl);
}

/// `endpoint: None` means real AWS, where DuckDB derives the host from the region itself.
#[test]
fn no_endpoint_yields_an_empty_host_and_the_configured_region() {
    let access = duck_s3_access(&cfg(None));

    assert!(access.endpoint.is_empty());
    assert!(!access.use_ssl);
    assert_eq!(access.region, "eu-west-2");
}
