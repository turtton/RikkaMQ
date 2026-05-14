use crate::error::Error;
use crate::info::QueueInfo;
use std::future::Future;

pub trait MessageQueue<I, T>: Send + Sync + 'static {
    fn enqueue(&self, info: QueueInfo<I, T>) -> impl Future<Output = Result<(), Error>> + Send;
}

pub trait QueueStats {
    fn queued_len(&self) -> impl Future<Output = Result<u64, Error>> + Send;
}
