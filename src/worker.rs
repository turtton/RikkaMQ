use crate::config::MQConfig;
use crate::error::{Error, ErrorOperation};
use crate::handler::HandlerFn;
use crate::info::QueueInfo;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[doc(hidden)]
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

pub struct WorkerSet {
    pub shutdown_tx: watch::Sender<bool>,
    pub handles: Vec<JoinHandle<Result<(), Error>>>,
}

impl WorkerSet {
    pub async fn shutdown(self) -> Result<(), Error> {
        self.shutdown_tx
            .send(true)
            .map_err(|e| Error::Shutdown(Box::new(e)))?;
        self.join().await
    }

    pub async fn join(self) -> Result<(), Error> {
        for handle in self.handles {
            handle.await.map_err(|e| Error::Join(Box::new(e)))??;
        }
        Ok(())
    }
}

pub trait WorkerControl<M, I, T>
where
    M: Clone + Send + Sync + 'static,
{
    fn start_workers(
        &self,
        module: M,
        handler: HandlerFn<M, T>,
    ) -> impl Future<Output = Result<WorkerSet, Error>> + Send;
}

#[derive(Debug)]
#[doc(hidden)]
pub struct ClaimedMessage<I, T> {
    pub stream_id: String,
    pub delivered_count: u32,
    pub info: QueueInfo<I, T>,
}

#[doc(hidden)]
pub trait QueueStore<I, T>: Send + Sync {
    fn claim_next<'a>(&'a self) -> StoreFuture<'a, Option<ClaimedMessage<I, T>>>;
    fn ack<'a>(&'a self, stream_id: &'a str) -> StoreFuture<'a, ()>;
    fn record_delayed<'a>(
        &'a self,
        info: QueueInfo<I, T>,
        error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> StoreFuture<'a, ()>;
    fn record_failed<'a>(
        &'a self,
        info: QueueInfo<I, T>,
        error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> StoreFuture<'a, ()>;
    fn remove_delayed<'a>(&'a self, id: &'a I) -> StoreFuture<'a, ()>;
}

#[doc(hidden)]
pub async fn run_worker<M, I, T, S>(
    module: M,
    handler: HandlerFn<M, T>,
    store: S,
    config: MQConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Error>
where
    M: Clone + Send + Sync + 'static,
    I: Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    S: QueueStore<I, T>,
{
    loop {
        let claimed = tokio::select! {
            changed = shutdown_rx.changed() => {
                changed.map_err(|e| Error::Shutdown(Box::new(e)))?;
                if *shutdown_rx.borrow() { break; }
                continue;
            }
            claimed = store.claim_next() => claimed?,
        };

        let Some(claimed) = claimed else {
            tokio::task::yield_now().await;
            continue;
        };
        let info_id = claimed.info.id().clone();
        let data = claimed.info.data().clone();
        match handler(module.clone(), data).await {
            Ok(()) => {
                store.ack(&claimed.stream_id).await?;
                if claimed.delivered_count > 0 {
                    store.remove_delayed(&info_id).await?;
                }
            }
            Err(ErrorOperation::Delay(error)) if claimed.delivered_count < config.max_retry => {
                store.record_delayed(claimed.info, error).await?;
                store.ack(&claimed.stream_id).await?;
            }
            Err(ErrorOperation::Delay(error)) => {
                store.record_failed(claimed.info, error).await?;
                store.ack(&claimed.stream_id).await?;
            }
            Err(ErrorOperation::Fail(error)) => {
                store.record_failed(claimed.info, error).await?;
                store.ack(&claimed.stream_id).await?;
            }
        }
    }
    Ok(())
}
