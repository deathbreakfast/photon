//! Bounded handler worker pool per subscription partition.

use std::sync::Arc;

use tokio::sync::Semaphore;

/// Limits concurrent handler tasks for a subscription.
pub struct WorkerPool {
    semaphore: Arc<Semaphore>,
}

impl WorkerPool {
    /// Create a pool with at most `max_concurrent` in-flight handlers.
    #[must_use]
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent.max(1))),
        }
    }

    /// Pool size from `PHOTON_HANDLER_POOL_SIZE` (default 64).
    #[must_use]
    pub fn from_env() -> Self {
        let max = std::env::var("PHOTON_HANDLER_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64);
        Self::new(max)
    }

    /// Acquire a permit before running a handler task.
    ///
    /// # Panics
    ///
    /// Panics if an internal lock is poisoned.
    pub async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed")
    }
}
