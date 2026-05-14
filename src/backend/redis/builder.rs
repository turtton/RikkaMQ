use super::mq::RedisMessageQueue;
use crate::config::MQConfig;
use crate::error::Error;
use deadpool_redis::Pool;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct RedisMessageQueueBuilder<I, T> {
    pool: Option<Pool>,
    name: Option<String>,
    config: MQConfig,
    consumer_id_generator: Option<Arc<dyn Fn() -> String + Send + Sync + 'static>>,
    _marker: PhantomData<fn() -> (I, T)>,
}

impl<I, T> Default for RedisMessageQueueBuilder<I, T> {
    fn default() -> Self {
        Self {
            pool: None,
            name: None,
            config: MQConfig::default(),
            consumer_id_generator: None,
            _marker: PhantomData,
        }
    }
}

impl<I, T> RedisMessageQueueBuilder<I, T> {
    pub fn pool(mut self, pool: Pool) -> Self {
        self.pool = Some(pool);
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn config(mut self, config: MQConfig) -> Self {
        self.config = config;
        self
    }

    pub fn consumer_id_generator<F>(mut self, generator: F) -> Self
    where
        F: Fn() -> String + Send + Sync + 'static,
    {
        self.consumer_id_generator = Some(Arc::new(generator));
        self
    }

    pub fn build(self) -> Result<RedisMessageQueue<I, T>, Error> {
        let pool = self
            .pool
            .ok_or_else(|| Error::protocol("redis builder", "pool is required"))?;
        let name = self
            .name
            .ok_or_else(|| Error::protocol("redis builder", "name is required"))?;
        let consumer_id_generator = self
            .consumer_id_generator
            .ok_or_else(|| Error::protocol("redis builder", "consumer_id_generator is required"))?;
        Ok(RedisMessageQueue::from_parts(
            pool,
            name,
            self.config,
            consumer_id_generator,
        ))
    }
}
