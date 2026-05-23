use std::num::NonZeroUsize;
use std::time::Duration;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RetryPolicy {
    /// Retry after a constant interval.
    Fixed(Duration),
    /// Retry with exponential growth capped at `max`.
    Exponential {
        initial: Duration,
        factor: f64,
        max: Duration,
    },
}

impl RetryPolicy {
    /// Lower-bound prefilter for the server-side XPENDING IDLE query;
    /// per-message eligibility is enforced client-side using
    /// [`RetryPolicy::delay_for`] with the message's delivery count.
    pub fn min_delay(&self) -> Duration {
        match self {
            Self::Fixed(delay) => *delay,
            Self::Exponential { initial, .. } => *initial,
        }
    }

    /// Total elapsed time required between handler attempt N and attempt N+1.
    ///
    /// `delivered_count` is the number of times the message has already been
    /// delivered, so the first retry corresponds to `delivered_count = 1`.
    pub fn delay_for(&self, delivered_count: u32) -> Duration {
        match self {
            Self::Fixed(delay) => *delay,
            Self::Exponential {
                initial,
                factor,
                max,
            } => exponential_delay(*initial, *factor, *max, delivered_count),
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::Fixed(Duration::from_secs(180))
    }
}

fn exponential_delay(initial: Duration, factor: f64, max: Duration, delivered_count: u32) -> Duration {
    let factor = if factor.is_finite() {
        factor.max(1.0)
    } else {
        1.0
    };
    let exponent = delivered_count.saturating_sub(1);
    if factor == 1.0 {
        return initial.min(max);
    }
    if exponent > i32::MAX as u32 {
        return max;
    }
    let exponent = exponent as i32;
    let multiplier = factor.powi(exponent);
    if !multiplier.is_finite() {
        return max;
    }
    let delay_secs = initial.as_secs_f64() * multiplier;
    if !delay_secs.is_finite() || delay_secs >= max.as_secs_f64() {
        return max;
    }
    Duration::try_from_secs_f64(delay_secs).unwrap_or(max).min(max)
}

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
    /// Retry interval policy for delayed messages.
    ///
    /// [`RetryPolicy::Fixed`] preserves a constant delay. [`RetryPolicy::Exponential`]
    /// grows from `initial` by `factor` up to `max`. [`RetryPolicy::min_delay`]
    /// is used only as a coarse Redis XPENDING IDLE prefilter before
    /// per-message retry eligibility is checked.
    pub retry_policy: RetryPolicy,
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

    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }
}

impl Default for MQConfig {
    fn default() -> Self {
        Self {
            worker_count: NonZeroUsize::new(4).expect("default worker_count is non-zero"),
            max_retry: 3,
            retry_policy: RetryPolicy::Fixed(Duration::from_secs(180)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RetryPolicy;
    use std::time::Duration;

    #[test]
    fn fixed_delay_for_returns_same_duration() {
        let delay = Duration::from_millis(250);
        let policy = RetryPolicy::Fixed(delay);

        for delivered_count in [0, 1, 5, u32::MAX] {
            assert_eq!(policy.delay_for(delivered_count), delay);
        }
    }

    #[test]
    fn fixed_min_delay_returns_fixed_duration() {
        let delay = Duration::from_secs(3);
        assert_eq!(RetryPolicy::Fixed(delay).min_delay(), delay);
    }

    #[test]
    fn exponential_delay_grows_and_saturates_to_max() {
        let policy = RetryPolicy::Exponential {
            initial: Duration::from_millis(100),
            factor: 2.0,
            max: Duration::from_secs(10),
        };

        assert_eq!(policy.delay_for(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for(3), Duration::from_millis(400));
        assert_eq!(policy.delay_for(10), Duration::from_secs(10));
        assert_eq!(policy.delay_for(100), Duration::from_secs(10));
        assert_eq!(policy.delay_for(u32::MAX), Duration::from_secs(10));
    }

    #[test]
    fn exponential_min_delay_returns_initial() {
        let initial = Duration::from_millis(100);
        let policy = RetryPolicy::Exponential {
            initial,
            factor: 2.0,
            max: Duration::from_secs(10),
        };

        assert_eq!(policy.min_delay(), initial);
    }

    #[test]
    fn exponential_factor_one_does_not_grow() {
        let initial = Duration::from_millis(100);
        let policy = RetryPolicy::Exponential {
            initial,
            factor: 1.0,
            max: Duration::from_secs(10),
        };

        for delivered_count in [0, 1, 5, u32::MAX] {
            assert_eq!(policy.delay_for(delivered_count), initial);
        }
    }

    #[test]
    fn exponential_non_positive_or_nan_factor_is_treated_as_one() {
        let initial = Duration::from_millis(100);
        for factor in [f64::NAN, 0.0, -2.0] {
            let policy = RetryPolicy::Exponential {
                initial,
                factor,
                max: Duration::from_secs(10),
            };

            for delivered_count in [0, 1, 5, u32::MAX] {
                assert_eq!(policy.delay_for(delivered_count), initial);
            }
        }
    }
}
