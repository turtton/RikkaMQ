use std::num::NonZeroUsize;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MQConfig {
    /// Number of workers to process the queue.
    ///
    /// [`crate::worker::WorkerControl::start_workers`] spawns exactly this
    /// many worker tasks. The value is non-zero so a queue cannot be started
    /// with no workers by accident.
    pub worker_count: NonZeroUsize,
    /// Number of delayed deliveries before a message is moved to failed.
    ///
    /// Redis Streams reports the current delivery count for PEL retries. When
    /// that count is greater than or equal to `max_retry`, a delayed operation
    /// is persisted in the failed hash instead of delayed again.
    /// [`crate::error::ErrorOperation::Fail`] sends the message to failed
    /// immediately, regardless of this value.
    pub max_retry: u32,
    /// Delay before a pending message may be claimed for retry.
    pub retry_delay: Duration,
}

impl MQConfig {
    pub fn worker_count(mut self, worker_count: NonZeroUsize) -> Self {
        self.worker_count = worker_count;
        self
    }

    pub fn max_retry(mut self, max_retry: u32) -> Self {
        self.max_retry = max_retry;
        self
    }

    pub fn retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }
}

impl Default for MQConfig {
    fn default() -> Self {
        Self {
            worker_count: NonZeroUsize::new(4).expect("default worker_count is non-zero"),
            max_retry: 3,
            retry_delay: Duration::from_secs(180),
        }
    }
}
