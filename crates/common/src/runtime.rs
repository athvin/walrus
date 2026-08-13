//! Runtime sizing policy shared by both walrus binaries.
//!
//! Runtime construction stays in each binary. This module shares only the bounds, automatic
//! default, and validation text so the loader and sink cannot drift apart.

/// Upper bound on Tokio worker threads.
pub const MAX_WORKER_THREADS: usize = 64;

/// Ceiling for Tokio's blocking pool.
pub const MAX_BLOCKING_THREADS: usize = 16;

/// Return the configured worker count, or the process's usable parallelism, or one.
#[must_use]
pub fn resolve_worker_threads(configured: Option<usize>) -> usize {
    configured.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
    })
}

/// Validate an optional worker count against the inclusive `1..=64` bound.
///
/// # Errors
///
/// Returns a detail string when a configured count is zero or exceeds
/// [`MAX_WORKER_THREADS`].
pub fn validate_worker_threads(configured: Option<usize>) -> Result<(), String> {
    let Some(threads) = configured else {
        return Ok(());
    };
    if threads == 0 {
        return Err("must be >= 1 (omit for automatic sizing)".to_string());
    }
    if threads > MAX_WORKER_THREADS {
        return Err(format!(
            "must be <= {MAX_WORKER_THREADS} (got {threads}; omit for automatic sizing)"
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;
