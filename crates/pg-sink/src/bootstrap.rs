//! The ordered, fail-fast bootstrap scaffold — shared steps 2–4 (§4.2, architecture "Shared
//! bootstrap"). Step 1 (config load/validate) is `SinkConfig::load`; step 4 (bind health) is in
//! `main::run`. This module does the two dependency checks between them:
//!
//! 2. **control Postgres reachable + migrations current** — `connect` failures are *transient*
//!    (Postgres may still be coming up during a rollout), retried with backoff to the startup
//!    deadline; ensuring migrations is idempotent.
//! 3. **object-store canary** — `put` + `get` + `delete` of a tiny key (not just `head`: some
//!    S3-compatibles answer `head` on a nonexistent bucket differently). Transient, same retry.
//!
//! **Transient vs terminal is modelled as data** ([`common::Error::is_terminal`]): a terminal error
//! (bad config) returns immediately; a transient one (S3 5xx, PG "still coming up") is retried until
//! the deadline, after which the last error is returned and `main` maps its distinct exit code.

use crate::config::SinkConfig;
use crate::preflight::{self, SourcePreflight};
use common::config::ObjectStoreConfig;
use common::Error;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// What the shared bootstrap hands the (future) replication loop: the control-plane pool, a live
/// canary-verified object store, and the preflighted source connection (PR 2.20 opens the actual
/// streaming replication connection).
pub struct BootstrapCtx {
    pub control_pool: PgPool,
    pub object_store: Arc<dyn ObjectStore>,
    pub source_client: tokio_postgres::Client,
}

const CANARY_PAYLOAD: &[u8] = b"walrus bootstrap canary";

/// Time budget for a single dependency attempt: at most 5s, and never more than the deadline leaves
/// (with a small floor so a nearly-elapsed deadline still gets one real attempt).
fn attempt_budget(deadline: Instant) -> Duration {
    deadline
        .saturating_duration_since(Instant::now())
        .clamp(Duration::from_millis(500), Duration::from_secs(5))
}

/// Run shared steps 2–4's preconditions, retrying transient deps until `deadline`.
pub async fn run_shared(cfg: &SinkConfig, deadline: Instant) -> Result<BootstrapCtx, Error> {
    // Step 2: control Postgres reachable (transient) + migrations current (idempotent). Each connect
    // attempt is bounded so sqlx's own pool-acquire timeout can't blow past our `startup_deadline`.
    let control_pool = retry_transient(deadline, "control database", || async {
        let budget = attempt_budget(deadline);
        match tokio::time::timeout(budget, control::connect(&cfg.control_db_url)).await {
            Ok(Ok(pool)) => Ok(pool),
            Ok(Err(e)) => Err(Error::ControlDb(e.to_string())),
            Err(_) => Err(Error::ControlDb(format!(
                "connect attempt did not complete within {budget:?}"
            ))),
        }
    })
    .await?;
    control::run_migrations(&control_pool)
        .await
        .map_err(|e| Error::ControlDb(format!("ensure control migrations current: {e}")))?;
    tracing::info!("control database reachable and migrations current");

    // Step 3: object-store canary (transient). Build once (config-derived, not retried), then
    // put/get/delete a tiny key until it succeeds or the deadline passes.
    let object_store = build_object_store(&cfg.object_store)
        .map_err(|e| Error::ObjectStore(format!("build object store: {e}")))?;
    retry_transient(deadline, "object store", || {
        let store = object_store.clone();
        let instance = cfg.instance.clone();
        async move {
            object_store_canary(store.as_ref(), &instance)
                .await
                .map_err(|e| Error::ObjectStore(e.to_string()))
        }
    })
    .await?;
    tracing::info!("object-store canary (put/get/delete) passed");

    // Step 6: source-side preflight. The connect is transient (server may be coming up); every
    // assertion is terminal — a wrong wal_level / missing publication / keyless table can't self-heal.
    let source_client = retry_transient(deadline, "source database", || {
        let url = cfg.source_db_url.clone();
        async move { preflight::connect_source(&url).await }
    })
    .await?;
    let pf = SourcePreflight::new(&source_client, cfg);
    let server = pf.assert_server_prereqs().await?;
    pf.assert_publication_covers().await?;
    let pk = pf.assert_tables_have_pk(cfg.pk_mode()).await?;
    tracing::info!(
        version_num = server.version_num,
        ok_tables = pk.ok.len(),
        quarantined = pk.quarantined.len(),
        "source preflight passed"
    );

    Ok(BootstrapCtx {
        control_pool,
        object_store,
        source_client,
    })
}

/// Build the S3/MinIO client from config. Credentials come from the environment
/// (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`); a `Some(endpoint)` selects MinIO/localstack and
/// allows plain HTTP.
fn build_object_store(cfg: &ObjectStoreConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region);
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }
    Ok(Arc::new(builder.build()?))
}

/// Prove write→read→delete round-trips against the bucket. A `head` alone is not enough — some
/// S3-compatibles answer `head` on a missing bucket ambiguously.
async fn object_store_canary(
    store: &dyn ObjectStore,
    instance: &str,
) -> Result<(), object_store::Error> {
    let key = Path::from(format!("_walrus/canary/{instance}"));
    store
        .put(&key, PutPayload::from_static(CANARY_PAYLOAD))
        .await?;
    let got = store.get(&key).await?.bytes().await?;
    if got.as_ref() != CANARY_PAYLOAD {
        return Err(object_store::Error::Generic {
            store: "canary",
            source: "read-back bytes did not match what was written".into(),
        });
    }
    store.delete(&key).await?;
    Ok(())
}

/// Retry `op` while it returns a *transient* error, backing off up to `deadline`. Returns
/// immediately on success or a *terminal* error; after the deadline, returns the last transient
/// error (whose distinct exit code `main` surfaces).
async fn retry_transient<T, F, Fut>(deadline: Instant, what: &str, mut op: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let mut backoff = Duration::from_millis(200);
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) if e.is_terminal() => return Err(e),
            Err(e) => {
                let now = Instant::now();
                if now >= deadline {
                    tracing::error!("{what} still unavailable at startup deadline: {e}");
                    return Err(e);
                }
                let wait = backoff.min(deadline.saturating_duration_since(now));
                tracing::warn!("{what} unavailable (transient), retrying in {wait:?}: {e}");
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
}
