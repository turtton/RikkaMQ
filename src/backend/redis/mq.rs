use super::builder::RedisMessageQueueBuilder;
use super::store::{RedisQueueStore, RedisStoreOps};
use crate::config::MQConfig;
use crate::error::Error;
use crate::handler::HandlerFn;
use crate::info::QueueInfo;
use crate::mq::{MessageQueue, QueueStats};
use crate::worker::{run_worker, WorkerControl, WorkerSet};
use deadpool_redis::redis::Client;
use deadpool_redis::Pool;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct RedisMessageQueue<I, T> {
    pub(crate) ops: RedisStoreOps,
    pub(crate) config: MQConfig,
    pub(crate) consumer_id_generator: Arc<dyn Fn() -> String + Send + Sync + 'static>,
    pub(crate) _marker: PhantomData<fn() -> (I, T)>,
}

impl<I, T> RedisMessageQueue<I, T> {
    pub fn builder() -> RedisMessageQueueBuilder<I, T> {
        RedisMessageQueueBuilder::default()
    }

    pub(crate) fn from_parts(
        pool: Pool,
        blocking_client: Client,
        name: String,
        config: MQConfig,
        consumer_id_generator: Arc<dyn Fn() -> String + Send + Sync + 'static>,
    ) -> Self {
        Self {
            ops: RedisStoreOps::new(pool, blocking_client, name),
            config,
            consumer_id_generator,
            _marker: PhantomData,
        }
    }
}

impl<I, T> MessageQueue<I, T> for RedisMessageQueue<I, T>
where
    I: Display + Serialize + Send + Sync + 'static,
    T: Serialize + Send + Sync + 'static,
{
    async fn enqueue(&self, info: QueueInfo<I, T>) -> Result<(), Error> {
        self.ops.insert_waiting(&info).await
    }
}

impl<I, T> QueueStats for RedisMessageQueue<I, T>
where
    I: Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    async fn queued_len(&self) -> Result<u64, Error> {
        self.ops.stream_len().await
    }
}

impl<M, I, T> WorkerControl<M, I, T> for RedisMessageQueue<I, T>
where
    M: Clone + Send + Sync + 'static,
    I: Clone + Display + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    T: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    async fn start_workers(&self, module: M, handler: HandlerFn<M, T>) -> Result<WorkerSet, Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker_count = self.config.worker_count.get();
        let mut blocking_connections = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            blocking_connections.push(self.ops.blocking_connection().await?);
        }
        let mut handles = Vec::with_capacity(worker_count);
        for blocking_connection in blocking_connections {
            let store = RedisQueueStore::<I, T>::new(
                self.ops.clone(),
                blocking_connection,
                (self.consumer_id_generator)(),
                self.config.retry_policy.clone(),
            );
            let module = module.clone();
            let handler = handler.clone();
            let config = self.config.clone();
            let shutdown_rx = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                run_worker(module, handler, store, config, shutdown_rx).await
            }));
        }
        Ok(WorkerSet::new(shutdown_tx, handles))
    }
}
