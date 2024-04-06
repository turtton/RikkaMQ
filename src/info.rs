use destructure::Destructure;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Information about a queued message
///
/// Basically, it is recommended to use [`QueueInfo::from`]
///
/// ```
/// use rikka_mq::info::QueueInfo;
///
/// let info = QueueInfo::from("Data");
/// ```
#[derive(Debug, Serialize, Deserialize, Destructure)]
pub struct QueueInfo<T> {
    id: Uuid,
    data: T,
}

impl<T> QueueInfo<T> {
    pub fn new(id: Uuid, data: T) -> Self {
        Self { id, data }
    }
}

impl<T> From<T> for QueueInfo<T> {
    fn from(value: T) -> Self {
        Self {
            id: Uuid::new_v4(),
            data: value,
        }
    }
}

/// Information about a failed or delayed message
#[derive(Debug, Serialize, Deserialize, Destructure)]
pub struct ErroredInfo<T> {
    id: Uuid,
    data: T,
    stack_trace: String,
}

impl<T> ErroredInfo<T> {
    pub fn new(id: Uuid, data: T, stack_trace: String) -> Self {
        Self {
            id,
            data,
            stack_trace,
        }
    }
}
