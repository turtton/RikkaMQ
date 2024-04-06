use crate::config::MQConfig;
use crate::error::Error;
use crate::handler::Handler;
use crate::info::{ErroredInfo, QueueInfo};
use serde::{Deserialize, Serialize};
use std::future::Future;
use uuid::Uuid;

pub trait MessageQueue<M, T>: 'static + Sync + Send
where
    M: 'static + Clone + Sync + Send,
    T: 'static + Clone + Serialize + for<'de> Deserialize<'de> + Sync + Send,
{
    type DatabaseConnection;

    /// Create a new message queue
    fn new<H>(
        db: Self::DatabaseConnection,
        module: M,
        name: String,
        config: MQConfig,
        process: H,
    ) -> Self
    where
        H: Handler<M, T>;

    /// Starts the number of workers set in [`MQConfig::worker_count`]
    fn start_workers(&self);

    /// Queue a new message
    fn queue(&self, info: QueueInfo<T>) -> impl Future<Output = Result<(), Error>> + Send;

    /// Get the number of queued messages
    fn get_queued_len(&self) -> impl Future<Output = Result<usize, Error>> + Send;

    /// Get the delayed messages
    fn get_delayed_infos(
        &self,
        size: i64,
        offset: i64,
    ) -> impl Future<Output = Result<Vec<ErroredInfo<T>>, Error>> + Send;

    /// Get a delayed message
    fn get_delayed_info(
        &self,
        id: &Uuid,
    ) -> impl Future<Output = Result<Option<ErroredInfo<T>>, Error>> + Send;

    /// Get the number of delayed messages
    fn get_delayed_len(&self) -> impl Future<Output = Result<usize, Error>> + Send;

    /// Get the failed messages
    fn get_failed_infos(
        &self,
        size: i64,
        offset: i64,
    ) -> impl Future<Output = Result<Vec<ErroredInfo<T>>, Error>> + Send;

    /// Get a failed message
    fn get_failed_info(
        &self,
        id: &Uuid,
    ) -> impl Future<Output = Result<Option<ErroredInfo<T>>, Error>> + Send;

    /// Get the number of failed messages
    fn get_failed_len(&self) -> impl Future<Output = Result<usize, Error>> + Send;

    /// Retry a failed message
    ///
    /// This will remove the failed message and queue it again.
    /// If the message is not found, it will return [`Ok`]
    fn retry_failed(&self, id: &Uuid) -> impl Future<Output = Result<(), Error>> + Send;
}
