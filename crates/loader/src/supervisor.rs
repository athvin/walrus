//! Worker-to-supervisor failure reporting for the loader's local apply tasks.

use crate::error::LoaderError;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[path = "supervisor_test.rs"]
mod tests;
