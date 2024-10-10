use crate::config::MQConfig;
use crate::define::redis::{delayed, failed, QueueData, RedisJobInternal};
use crate::error::{Error, ErrorOperation};
use crate::handler::{ErasedIntoRoute, Handler, MakeErasedHandler};
use crate::info::{DestructErroredInfo, DestructQueueInfo, ErroredInfo, QueueInfo};
use crate::mq::MessageQueue;
use deadpool_redis::{Pool, PoolError};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::time::Duration;
#[cfg(feature = "tokio")]
use tokio::time::sleep;
#[cfg(feature = "tracing")]
use tracing::{debug, error, warn};

impl From<PoolError> for Error {
    fn from(error: PoolError) -> Self {
        Error::DatabaseError(Box::new(error))
    }
}

pub struct RedisMessageQueue<M, I, T>
where
    M: 'static + Clone + Sync + Send,
    I: 'static + Clone + Serialize + for<'de> Deserialize<'de> + Display + Sync + Send,
    T: 'static + Clone + Serialize + for<'de> Deserialize<'de> + Sync + Send,
{
    name: String,
    db: Pool,
    module: M,
    config: MQConfig,
    id_generator: Mutex<Box<fn() -> I>>,
    worker_process: Mutex<Box<dyn ErasedIntoRoute<M, T>>>,
    _data_type: PhantomData<T>,
}

impl<M, I, T> RedisMessageQueue<M, I, T>
where
    M: 'static + Clone + Send + Sync,
    I: 'static + Serialize + for<'de> Deserialize<'de> + Display + Clone + Sync + Send,
    T: Clone + Serialize + for<'de> Deserialize<'de> + Sync + Send,
{
    #[tracing::instrument(skip(db, module, id_generator, block))]
    async fn listen(
        db: Pool,
        module: M,
        name: String,
        config: MQConfig,
        id_generator: Box<fn() -> I>,
        block: Box<dyn ErasedIntoRoute<M, T>>,
    ) {
        let member_id = id_generator();
        let member_name = format!("consumer:{}", member_id);
        loop {
            let QueueData {
                id: queue_id,
                delivered_count,
                info,
            } = {
                let mut con = match db.get().await {
                    Ok(con) => con,
                    Err(report) => {
                        #[cfg(feature = "tracing")]
                        error!("{report:?}");
                        #[cfg(feature = "tokio")]
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let mut result = RedisJobInternal::pop_pending::<I, T>(
                    &mut con,
                    &name,
                    &member_name,
                    &config.retry_delay,
                )
                .await;
                if result.is_err() || result.as_ref().is_ok_and(Option::is_none) {
                    result = RedisJobInternal::pop_to_process(&mut con, &name, &member_name).await;
                }
                match result {
                    Ok(Some(data)) => data,
                    Ok(None) => continue,
                    Err(report) => {
                        #[cfg(feature = "tracing")]
                        error!("{report:?}");
                        #[cfg(feature = "tokio")]
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            };
            #[cfg(feature = "tracing")]
            debug!("Processing Id: {queue_id}, TryCount: {delivered_count}");
            let DestructQueueInfo { id: info_id, data }: DestructQueueInfo<I, T> =
                info.into_destruct();
            let result = block
                .clone_box()
                .convert(module.clone(), data.clone())
                .await;
            {
                let transact = db.get().await;
                let mut con = match transact {
                    Ok(con) => con,
                    Err(report) => {
                        #[cfg(feature = "tracing")]
                        error!("{report:?}");
                        continue;
                    }
                };

                if let Err(report) = result {
                    let is_failed = matches!(&report, ErrorOperation::Fail(_));
                    if is_failed || delivered_count > config.max_retry.into() {
                        if let Err(report) = RedisJobInternal::push_failed_info(
                            &mut con,
                            &name,
                            info_id.clone(),
                            data,
                            format!(
                                "Task failed or {} time delayed: {:?}",
                                config.max_retry, report
                            ),
                        )
                        .await
                        {
                            #[cfg(feature = "tracing")]
                            error!("{report:?}");
                        }
                        #[cfg(feature = "tracing")]
                        error!("Failed Id: {queue_id}, TryCount: {delivered_count}");
                    } else if let ErrorOperation::Delay(_) = report {
                        if let Err(report) = RedisJobInternal::push_delayed_info(
                            &mut con,
                            &name,
                            info_id,
                            data,
                            format!("{report:?}"),
                        )
                        .await
                        {
                            #[cfg(feature = "tracing")]
                            error!("{report:?}");
                        }
                        #[cfg(feature = "tracing")]
                        warn!("Delayed Id: {queue_id}, TryCount: {delivered_count}, Report: {report:?}");
                        continue;
                    }
                } else {
                    #[cfg(feature = "tracing")]
                    debug!("Done Id: {queue_id}, TryCount: {delivered_count}");
                }
                if let Err(report) = RedisJobInternal::mark_done(&mut con, &name, &queue_id).await {
                    #[cfg(feature = "tracing")]
                    error!("{report:?}");
                } else if delivered_count > 0 {
                    if let Err(report) =
                        RedisJobInternal::remove_delayed_info(&mut con, &name, &info_id).await
                    {
                        #[cfg(feature = "tracing")]
                        error!("{report:?}");
                    };
                };
            }
        }
    }
}

impl<M, I, T> MessageQueue<M, I, T> for RedisMessageQueue<M, I, T>
where
    M: 'static + Clone + Send + Sync,
    I: 'static + Clone + Serialize + for<'de> Deserialize<'de> + Display + Sync + Send,
    T: 'static + Clone + Serialize + for<'de> Deserialize<'de> + Sync + Send,
{
    type DatabaseConnection = Pool;

    fn new<H>(
        db: Self::DatabaseConnection,
        module: M,
        name: String,
        config: MQConfig,
        id_generator: fn() -> I,
        process: H,
    ) -> Self
    where
        H: Handler<M, T>,
    {
        let container =
            MakeErasedHandler::new(process, |handler, module, data| handler.call(module, data));
        Self {
            name,
            db,
            module,
            config,
            id_generator: Mutex::new(Box::new(id_generator)),
            worker_process: Mutex::new(Box::new(container)),
            _data_type: PhantomData,
        }
    }

    fn start_workers(&self) {
        let mut i = 0;
        loop {
            if i >= self.config.worker_count {
                break;
            }
            let db = self.db.clone();
            let module = self.module.clone();
            let process = self.worker_process.lock();
            let process = match process {
                Ok(guard) => guard.clone_box(),
                Err(_) => continue,
            };
            let id_generator = self.id_generator.lock();
            let id_generator = match id_generator {
                Ok(guard) => guard.clone(),
                Err(_) => continue,
            };
            let name = self.name.clone();
            let config = self.config.clone();
            tokio::spawn(async move {
                RedisMessageQueue::<M, I, T>::listen(
                    db,
                    module,
                    name,
                    config,
                    id_generator,
                    process,
                )
                .await;
            });
            i += 1;
        }
    }

    async fn queue(&self, info: QueueInfo<I, T>) -> Result<(), Error> {
        let name = &self.name;
        let mut con = self.db.get().await?;
        RedisJobInternal::insert_waiting(&mut con, name, &info).await
    }

    async fn get_queued_len(&self) -> Result<usize, Error> {
        let name = &self.name;
        let mut con = self.db.get().await?;
        let size = RedisJobInternal::get_stream_len(&mut con, name).await?;
        let size = usize::try_from(size)?;
        Ok(size)
    }

    async fn get_delayed_infos(
        &self,
        size: i64,
        offset: i64,
    ) -> Result<Vec<ErroredInfo<I, T>>, Error> {
        let name = delayed(&self.name);
        let mut con = self.db.get().await?;
        RedisJobInternal::get_infos_from_hash(&mut con, &name, &size, &offset).await
    }

    async fn get_delayed_info(&self, id: &I) -> Result<Option<ErroredInfo<I, T>>, Error> {
        let name = delayed(&self.name);
        let mut con = self.db.get().await?;
        RedisJobInternal::get_info_from_hash(&mut con, &name, id).await
    }

    async fn get_delayed_len(&self) -> Result<usize, Error> {
        let name = delayed(&self.name);
        let mut con = self.db.get().await?;
        RedisJobInternal::get_hash_len(&mut con, &name).await
    }

    async fn get_failed_infos(
        &self,
        size: i64,
        offset: i64,
    ) -> Result<Vec<ErroredInfo<I, T>>, Error> {
        let mut con = self.db.get().await?;
        let name = failed(&self.name);
        RedisJobInternal::get_infos_from_hash(&mut con, &name, &size, &offset).await
    }

    async fn get_failed_info(&self, id: &I) -> Result<Option<ErroredInfo<I, T>>, Error> {
        let mut con = self.db.get().await?;
        let name = failed(&self.name);
        RedisJobInternal::get_info_from_hash(&mut con, &name, id).await
    }

    async fn get_failed_len(&self) -> Result<usize, Error> {
        let name = failed(&self.name);
        let mut con = self.db.get().await?;
        RedisJobInternal::get_hash_len(&mut con, &name).await
    }

    async fn retry_failed(&self, id: &I) -> Result<(), Error> {
        let mut con = self.db.get().await?;
        let failed = failed(&self.name);
        let failed_info: Option<ErroredInfo<I, T>> =
            RedisJobInternal::get_info_from_hash(&mut con, &failed, id).await?;
        if let Some(info) = failed_info {
            RedisJobInternal::remove_failed_info(&mut con, &self.name, id).await?;
            let DestructErroredInfo {
                id,
                data,
                stack_trace: _,
            }: DestructErroredInfo<I, T> = info.into_destruct();
            let info = QueueInfo::new(id, data.clone());
            RedisJobInternal::insert_waiting(&mut con, &self.name, &info).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::config::MQConfig;
    use crate::define::redis::mq::RedisMessageQueue;
    use crate::define::redis::test::{create_pool, TestData};
    use crate::error::{Error, ErrorOperation};
    use crate::info::QueueInfo;
    use crate::mq::MessageQueue;
    use deadpool_redis::Pool;
    use rand::random;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;
    use tracing::info;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use uuid::Uuid;

    #[derive(Debug)]
    struct Module {
        pool: Pool,
    }

    #[test_with::env(REDIS_URL)]
    #[tokio::test]
    async fn test_mq() -> Result<(), Error> {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "debug".into()),
            )
            .with(tracing_subscriber::fmt::layer())
            .init();
        let pool = create_pool()?;
        let name = "test_mq".to_string();
        let config = MQConfig::default()
            .max_retry(3)
            .retry_delay(Duration::from_secs(1));
        let module = Arc::new(Module { pool: pool.clone() });
        let mq = RedisMessageQueue::new(
            pool.clone(),
            module,
            name,
            config,
            Uuid::new_v4,
            |module: Arc<Module>, data: TestData| async move {
                info!("module: {module:?}, data: {data:?}");
                let pool = &module.pool;
                let _con = pool
                    .get()
                    .await
                    .map_err(|e| ErrorOperation::Delay(format!("{e:?}")))?;
                // Any transactions between db
                sleep(Duration::from_millis(20)).await;
                // 50% change of failure
                if random() {
                    Ok(())
                } else {
                    match i32::from_str("aasdfasdf") {
                        Ok(_) => Ok(()),
                        Err(e) => Err(ErrorOperation::Delay(format!("{e:?}"))),
                    }
                }
            },
        );

        mq.start_workers();

        for i in 0..100 {
            let data = TestData {
                a: format!("test:{i}"),
            };
            let data = QueueInfo::new(Uuid::new_v4(), data);
            // Queue
            mq.queue(data).await?;
        }

        loop {
            let wait = mq.get_queued_len().await?;
            let delayed = mq.get_delayed_len().await?;
            let failed = mq.get_failed_len().await?;
            info!("Count: {wait}, Delayed: {delayed}, Failed: {failed}");
            if wait + delayed == 0 {
                if failed > 0 {
                    for info in mq.get_failed_infos(100, 0).await? {
                        mq.retry_failed(&info.into_destruct().id).await?;
                    }
                } else {
                    break;
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}
