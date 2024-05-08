use destructure::Destructure;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Information about a queued message
///
/// ```
/// use uuid::Uuid;
/// use rikka_mq::info::QueueInfo;
///
/// let info = QueueInfo::new(Uuid::new_v4(), "Data");
/// ```
#[derive(Debug, Serialize, Deserialize, Destructure)]
pub struct QueueInfo<I, D> {
    id: I,
    data: D,
}

impl<I: Display, T> QueueInfo<I, T> {
    pub fn new(id: I, data: T) -> Self {
        Self { id, data }
    }
}

/// Information about a failed or delayed message
#[derive(Debug, Serialize, Deserialize, Destructure)]
pub struct ErroredInfo<I, D> {
    id: I,
    data: D,
    stack_trace: String,
}

impl<I: Display, D> ErroredInfo<I, D> {
    pub fn new(id: I, data: D, stack_trace: String) -> Self {
        Self {
            id,
            data,
            stack_trace,
        }
    }
}
