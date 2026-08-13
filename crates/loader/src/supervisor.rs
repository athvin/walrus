//! Worker-to-supervisor failure reporting for the loader's local apply tasks.

use crate::error::LoaderError;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// One apply worker's terminal failure.
#[derive(Debug)]
pub struct WorkerFailure {
    pub schema: String,
    pub table: String,
    pub error: LoaderError,
}

impl WorkerFailure {
    /// Return the operator-facing `schema.table` key.
    #[must_use]
    pub fn table_key(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

/// Create a bounded failure channel with one slot per worker.
#[must_use]
pub fn failure_channel(
    workers: usize,
) -> (mpsc::Sender<WorkerFailure>, mpsc::Receiver<WorkerFailure>) {
    mpsc::channel(workers.max(1))
}

/// Report a failure without ever parking the worker.
pub fn report(tx: &mpsc::Sender<WorkerFailure>, failure: WorkerFailure) {
    match tx.try_send(failure) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(failure)) => {
            tracing::warn!(
                table = %failure.table_key(),
                error = %failure.error,
                "worker failure channel full; dropping additional failure"
            );
        }
        Err(mpsc::error::TrySendError::Closed(failure)) => {
            tracing::warn!(
                table = %failure.table_key(),
                error = %failure.error,
                "worker failure channel closed; supervisor already exited"
            );
        }
    }
}

/// Race worker failure reports against the sequential worker drain.
///
/// The first failure cancels the shared token so healthy workers finish draining, then returns that
/// original typed error once all workers have stopped. Additional failures are logged without
/// replacing the first failure.
pub async fn supervise<D>(
    mut rx: mpsc::Receiver<WorkerFailure>,
    token: &CancellationToken,
    drain: D,
) -> Option<WorkerFailure>
where
    D: std::future::Future<Output = ()>,
{
    tokio::pin!(drain);
    let mut first = None;
    loop {
        tokio::select! {
            biased;
            Some(failure) = rx.recv() => {
                if first.is_none() {
                    tracing::error!(
                        table = %failure.table_key(),
                        error = %failure.error,
                        "apply worker failed; cancelling loader"
                    );
                    token.cancel();
                    first = Some(failure);
                } else {
                    tracing::error!(
                        table = %failure.table_key(),
                        error = %failure.error,
                        "additional apply worker failed during drain"
                    );
                }
            }
            () = &mut drain => return first,
        }
    }
}

#[cfg(test)]
#[path = "supervisor_test.rs"]
mod tests;
