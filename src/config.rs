use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MQConfig {
    /// Number of workers to process the queue
    ///
    /// This used in [`crate::mq::MessageQueue::start_workers`]
    pub worker_count: i32,
    /// Number of additional retries after the initial attempt when the
    /// handler returns [`crate::error::ErrorOperation::Delay`].
    ///
    /// The handler is invoked at most `max_retry + 1` times in total: once
    /// for the initial delivery plus up to `max_retry` re-deliveries through
    /// the Redis Streams consumer group PEL (XPENDING/XCLAIM). When the
    /// delivery count exceeds `max_retry`, the message is moved to the
    /// failed hash. [`crate::error::ErrorOperation::Fail`] sends the message
    /// to the failed hash immediately, regardless of this value.
    pub max_retry: i32,
    /// Delay between retries
    pub retry_delay: Duration,
}

impl MQConfig {
    pub fn worker_count(mut self, worker_count: i32) -> Self {
        self.worker_count = worker_count;
        self
    }

    pub fn max_retry(mut self, max_retry: i32) -> Self {
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
            worker_count: 4,
            max_retry: 3,
            retry_delay: Duration::from_secs(180),
        }
    }
}
