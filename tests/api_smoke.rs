#![cfg(feature = "tokio")]

use rikka_mq::config::MQConfig;
use rikka_mq::error::{Error, ErrorOperation};
use rikka_mq::handler::into_handler;
use rikka_mq::info::QueueInfo;
use rikka_mq::worker::{ClaimedMessage, QueueStore, WorkerControl, WorkerSet};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Clone)]
struct SmokeQueue {
    store: SmokeStore,
}

impl SmokeQueue {
    fn new() -> Self {
        Self {
            store: SmokeStore::default(),
        }
    }

    async fn enqueue(&self, info: QueueInfo<u64, String>) -> Result<(), Error> {
        self.store
            .items
            .lock()
            .map_err(lock_error)?
            .push_back(ClaimedMessage {
                stream_id: info.id().to_string(),
                delivered_count: 0,
                info,
            });
        Ok(())
    }
}

impl WorkerControl<(), u64, String> for SmokeQueue {
    async fn start_workers(
        &self,
        module: (),
        handler: rikka_mq::handler::HandlerFn<(), String>,
    ) -> Result<WorkerSet, Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(rikka_mq::worker::run_worker(
            module,
            handler,
            self.store.clone(),
            MQConfig::default(),
            shutdown_rx,
        ));
        Ok(WorkerSet {
            shutdown_tx,
            handles: vec![handle],
        })
    }
}

#[derive(Clone, Default)]
struct SmokeStore {
    items: Arc<Mutex<VecDeque<ClaimedMessage<u64, String>>>>,
}

impl QueueStore<u64, String> for SmokeStore {
    fn claim_next<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ClaimedMessage<u64, String>>, Error>> + Send + 'a>>
    {
        Box::pin(async move { Ok(self.items.lock().map_err(lock_error)?.pop_front()) })
    }

    fn ack<'a>(
        &'a self,
        _stream_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn record_delayed<'a>(
        &'a self,
        _info: QueueInfo<u64, String>,
        _error: Box<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn record_failed<'a>(
        &'a self,
        _info: QueueInfo<u64, String>,
        _error: Box<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn remove_delayed<'a>(
        &'a self,
        _id: &'a u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn smoke_public_api_surface_without_redis() -> Result<(), Error> {
    let queue = SmokeQueue::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_for_handler = seen.clone();
    let handler = into_handler(move |(): (), data: String| {
        let seen = seen_for_handler.clone();
        async move {
            seen.lock()
                .map_err(|e| ErrorOperation::Fail(Box::new(std::io::Error::other(e.to_string()))))?
                .push(data);
            Ok(())
        }
    });

    queue
        .enqueue(QueueInfo::new(1, "hello".to_string()))
        .await?;
    let workers = queue.start_workers((), handler).await?;
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    workers.shutdown().await?;

    assert_eq!(seen.lock().map_err(lock_error)?.as_slice(), ["hello"]);
    Ok(())
}

fn lock_error<T>(err: std::sync::PoisonError<T>) -> Error {
    Error::Backend(Box::new(std::io::Error::other(err.to_string())))
}
