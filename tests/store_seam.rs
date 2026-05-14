#![cfg(feature = "tokio")]

use rikka_mq::config::MQConfig;
use rikka_mq::error::{Error, ErrorOperation};
use rikka_mq::handler::into_handler;
use rikka_mq::info::QueueInfo;
use rikka_mq::worker::{run_worker, ClaimedMessage, QueueStore};
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Claim(String),
    Ack(String),
    RecordDelayed(u64),
    RecordFailed(u64),
    RemoveDelayed(u64),
}

#[derive(Clone, Default)]
struct MemoryStore {
    claims: Arc<Mutex<VecDeque<ClaimedMessage<u64, String>>>>,
    events: Arc<Mutex<Vec<Event>>>,
    fail_record_failed: bool,
}

impl MemoryStore {
    fn with_claim(delivered_count: u32) -> Self {
        let store = Self::default();
        store
            .claims
            .lock()
            .expect("test mutex should not be poisoned")
            .push_back(ClaimedMessage {
                stream_id: "stream-1".to_string(),
                delivered_count,
                info: QueueInfo::new(42, "payload".to_string()),
            });
        store
    }

    fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .expect("test mutex should not be poisoned")
            .clone()
    }
}

impl QueueStore<u64, String> for MemoryStore {
    fn claim_next<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ClaimedMessage<u64, String>>, Error>> + Send + 'a>>
    {
        Box::pin(async move {
            let claim = self.claims.lock().map_err(lock_error)?.pop_front();
            if let Some(claim) = &claim {
                self.events
                    .lock()
                    .map_err(lock_error)?
                    .push(Event::Claim(claim.stream_id.clone()));
            }
            Ok(claim)
        })
    }

    fn ack<'a>(
        &'a self,
        stream_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            self.events
                .lock()
                .map_err(lock_error)?
                .push(Event::Ack(stream_id.to_string()));
            Ok(())
        })
    }

    fn record_delayed<'a>(
        &'a self,
        info: QueueInfo<u64, String>,
        _error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            self.events
                .lock()
                .map_err(lock_error)?
                .push(Event::RecordDelayed(*info.id()));
            Ok(())
        })
    }

    fn record_failed<'a>(
        &'a self,
        info: QueueInfo<u64, String>,
        _error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            self.events
                .lock()
                .map_err(lock_error)?
                .push(Event::RecordFailed(*info.id()));
            if self.fail_record_failed {
                return Err(Error::Backend(Box::new(SimpleError("record failed"))));
            }
            Ok(())
        })
    }

    fn remove_delayed<'a>(
        &'a self,
        id: &'a u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            self.events
                .lock()
                .map_err(lock_error)?
                .push(Event::RemoveDelayed(*id));
            Ok(())
        })
    }
}

#[tokio::test]
async fn ok_ack_and_remove_delayed_when_retry_entry() -> Result<(), Error> {
    let store = MemoryStore::with_claim(1);
    run_once(store.clone(), into_handler(|(), _| async { Ok(()) })).await?;
    assert_eq!(
        store.events(),
        vec![
            Event::Claim("stream-1".into()),
            Event::Ack("stream-1".into()),
            Event::RemoveDelayed(42)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn delay_under_max_retry_records_delayed_before_ack() -> Result<(), Error> {
    let store = MemoryStore::with_claim(2);
    run_once(
        store.clone(),
        into_handler(|(), _| async { Err(ErrorOperation::Delay(Box::new(SimpleError("delay")))) }),
    )
    .await?;
    assert_eq!(
        store.events(),
        vec![
            Event::Claim("stream-1".into()),
            Event::RecordDelayed(42),
            Event::Ack("stream-1".into())
        ]
    );
    Ok(())
}

#[tokio::test]
async fn delay_at_max_retry_records_failed_before_ack() -> Result<(), Error> {
    let store = MemoryStore::with_claim(3);
    run_once(
        store.clone(),
        into_handler(|(), _| async { Err(ErrorOperation::Delay(Box::new(SimpleError("delay")))) }),
    )
    .await?;
    assert_eq!(
        store.events(),
        vec![
            Event::Claim("stream-1".into()),
            Event::RecordFailed(42),
            Event::Ack("stream-1".into())
        ]
    );
    Ok(())
}

#[tokio::test]
async fn fail_records_failed_before_ack() -> Result<(), Error> {
    let store = MemoryStore::with_claim(0);
    run_once(
        store.clone(),
        into_handler(|(), _| async { Err(ErrorOperation::Fail(Box::new(SimpleError("fail")))) }),
    )
    .await?;
    assert_eq!(
        store.events(),
        vec![
            Event::Claim("stream-1".into()),
            Event::RecordFailed(42),
            Event::Ack("stream-1".into())
        ]
    );
    Ok(())
}

#[tokio::test]
async fn record_failed_error_skips_ack() {
    let store = MemoryStore {
        fail_record_failed: true,
        ..MemoryStore::with_claim(3)
    };
    let result = run_once(
        store.clone(),
        into_handler(|(), _| async { Err(ErrorOperation::Delay(Box::new(SimpleError("delay")))) }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        store.events(),
        vec![Event::Claim("stream-1".into()), Event::RecordFailed(42)]
    );
}

async fn run_once(
    handler_store: MemoryStore,
    handler: rikka_mq::handler::HandlerFn<(), String>,
) -> Result<(), Error> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let config = MQConfig {
        worker_count: NonZeroUsize::new(1).expect("test worker_count is non-zero"),
        max_retry: 3,
        retry_delay: std::time::Duration::from_millis(1),
    };
    let handle = tokio::spawn(run_worker((), handler, handler_store, config, shutdown_rx));
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    shutdown_tx
        .send(true)
        .map_err(|e| Error::Shutdown(Box::new(e)))?;
    handle.await.map_err(|e| Error::Join(Box::new(e)))?
}

#[derive(Debug)]
struct SimpleError(&'static str);

impl std::fmt::Display for SimpleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl StdError for SimpleError {}

fn lock_error<T>(err: std::sync::PoisonError<T>) -> Error {
    Error::Backend(Box::new(std::io::Error::other(err.to_string())))
}
