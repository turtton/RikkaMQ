use crate::config::MQConfig;
use crate::error::{Error, ErrorOperation};
use crate::handler::HandlerFn;
use crate::info::QueueInfo;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub(crate) type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

pub struct WorkerSet {
    shutdown_tx: watch::Sender<bool>,
    handles: Vec<JoinHandle<Result<(), Error>>>,
}

impl WorkerSet {
    pub(crate) fn new(
        shutdown_tx: watch::Sender<bool>,
        handles: Vec<JoinHandle<Result<(), Error>>>,
    ) -> Self {
        Self {
            shutdown_tx,
            handles,
        }
    }

    pub async fn shutdown(self) -> Result<(), Error> {
        let signal_err = self
            .shutdown_tx
            .send(true)
            .err()
            .map(|e| Error::Shutdown(Box::new(e)));
        Self::join_handles(self.handles, signal_err).await
    }

    pub async fn join(self) -> Result<(), Error> {
        Self::join_handles(self.handles, None).await
    }

    async fn join_handles(
        handles: Vec<JoinHandle<Result<(), Error>>>,
        signal_err: Option<Error>,
    ) -> Result<(), Error> {
        let mut first_err = None;
        for handle in handles {
            match handle.await {
                Err(join_err) if first_err.is_none() => {
                    first_err = Some(Error::Join(Box::new(join_err)));
                }
                Ok(Err(worker_err)) if first_err.is_none() => {
                    first_err = Some(worker_err);
                }
                Err(_) | Ok(Err(_)) | Ok(Ok(())) => {}
            }
        }
        if first_err.is_none() {
            first_err = signal_err;
        }
        first_err.map_or(Ok(()), Err)
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
pub(crate) struct ClaimedMessage<I, T> {
    pub(crate) stream_id: String,
    pub(crate) delivered_count: u32,
    pub(crate) info: QueueInfo<I, T>,
}

pub(crate) trait QueueStore<I, T>: Send + Sync {
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

pub(crate) async fn run_worker<M, I, T, S>(
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
        match shutdown_rx.has_changed() {
            Err(_) => break,
            Ok(true) => {
                if *shutdown_rx.borrow_and_update() {
                    break;
                }
            }
            Ok(false) => {}
        }

        let claimed = store.claim_next().await?;

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
            }
            Err(ErrorOperation::Delay(error)) => {
                let delayed_id = claimed.info.id().clone();
                let had_retry_state = claimed.delivered_count > 0;
                store.record_failed(claimed.info, error).await?;
                store.ack(&claimed.stream_id).await?;
                if had_retry_state {
                    store.remove_delayed(&delayed_id).await?;
                }
            }
            Err(ErrorOperation::Fail(error)) => {
                let delayed_id = claimed.info.id().clone();
                let had_retry_state = claimed.delivered_count > 0;
                store.record_failed(claimed.info, error).await?;
                store.ack(&claimed.stream_id).await?;
                if had_retry_state {
                    store.remove_delayed(&delayed_id).await?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{run_worker, ClaimedMessage, QueueStore, StoreFuture, WorkerSet};
    use crate::config::{MQConfig, RetryPolicy};
    use crate::error::{Error, ErrorOperation};
    use crate::handler::{into_handler, HandlerFn};
    use crate::info::QueueInfo;
    use std::collections::VecDeque;
    use std::error::Error as StdError;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{watch, Notify};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Event {
        ClaimNextError,
        Claim(String),
        Ack(String),
        RecordDelayed(u64),
        RecordFailed(u64),
        RemoveDelayed(u64),
    }

    #[derive(Clone, Default)]
    struct MemoryStore {
        state: Arc<Mutex<MemoryStoreState>>,
    }

    #[derive(Default)]
    struct MemoryStoreState {
        claims: VecDeque<ClaimedMessage<u64, String>>,
        events: Vec<Event>,
        script: ErrorScript,
        counts: CallCounts,
    }

    #[derive(Default)]
    struct ErrorScript {
        claim_next: Vec<usize>,
        ack: Vec<usize>,
        record_delayed: Vec<usize>,
        record_failed: Vec<usize>,
        remove_delayed: Vec<usize>,
    }

    #[derive(Default)]
    struct CallCounts {
        claim_next: usize,
        ack: usize,
        record_delayed: usize,
        record_failed: usize,
        remove_delayed: usize,
    }

    impl MemoryStore {
        fn with_claim(delivered_count: u32) -> Self {
            let store = Self::default();
            store
                .state
                .lock()
                .expect("test mutex should not be poisoned")
                .claims
                .push_back(ClaimedMessage {
                    stream_id: "stream-1".to_string(),
                    delivered_count,
                    info: QueueInfo::new(42, "payload".to_string()),
                });
            store
        }

        fn events(&self) -> Vec<Event> {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .events
                .clone()
        }

        fn fail_claim_next_on(self, call: usize) -> Self {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .script
                .claim_next
                .push(call);
            self
        }

        #[allow(dead_code)]
        fn fail_ack_on(self, call: usize) -> Self {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .script
                .ack
                .push(call);
            self
        }

        fn fail_record_delayed_on(self, call: usize) -> Self {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .script
                .record_delayed
                .push(call);
            self
        }

        fn fail_record_failed_on(self, call: usize) -> Self {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .script
                .record_failed
                .push(call);
            self
        }

        fn fail_remove_delayed_on(self, call: usize) -> Self {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .script
                .remove_delayed
                .push(call);
            self
        }
    }

    impl QueueStore<u64, String> for MemoryStore {
        fn claim_next<'a>(&'a self) -> StoreFuture<'a, Option<ClaimedMessage<u64, String>>> {
            Box::pin(async move {
                let mut state = self.state.lock().map_err(lock_error)?;
                state.counts.claim_next += 1;
                let call = state.counts.claim_next;
                if state.script.claim_next.contains(&call) {
                    state.events.push(Event::ClaimNextError);
                    return Err(Error::Backend(Box::new(SimpleError("claim next"))));
                }
                let claim = state.claims.pop_front();
                if let Some(claim) = &claim {
                    state.events.push(Event::Claim(claim.stream_id.clone()));
                }
                Ok(claim)
            })
        }

        fn ack<'a>(&'a self, stream_id: &'a str) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                let mut state = self.state.lock().map_err(lock_error)?;
                state.counts.ack += 1;
                let call = state.counts.ack;
                state.events.push(Event::Ack(stream_id.to_string()));
                if state.script.ack.contains(&call) {
                    return Err(Error::Backend(Box::new(SimpleError("ack"))));
                }
                Ok(())
            })
        }

        fn record_delayed<'a>(
            &'a self,
            info: QueueInfo<u64, String>,
            _error: Box<dyn StdError + Send + Sync + 'static>,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                let mut state = self.state.lock().map_err(lock_error)?;
                state.counts.record_delayed += 1;
                let call = state.counts.record_delayed;
                state.events.push(Event::RecordDelayed(*info.id()));
                if state.script.record_delayed.contains(&call) {
                    return Err(Error::Backend(Box::new(SimpleError("record delayed"))));
                }
                Ok(())
            })
        }

        fn record_failed<'a>(
            &'a self,
            info: QueueInfo<u64, String>,
            _error: Box<dyn StdError + Send + Sync + 'static>,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                let mut state = self.state.lock().map_err(lock_error)?;
                state.counts.record_failed += 1;
                let call = state.counts.record_failed;
                state.events.push(Event::RecordFailed(*info.id()));
                if state.script.record_failed.contains(&call) {
                    return Err(Error::Backend(Box::new(SimpleError("record failed"))));
                }
                Ok(())
            })
        }

        fn remove_delayed<'a>(&'a self, id: &'a u64) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                let mut state = self.state.lock().map_err(lock_error)?;
                state.counts.remove_delayed += 1;
                let call = state.counts.remove_delayed;
                state.events.push(Event::RemoveDelayed(*id));
                if state.script.remove_delayed.contains(&call) {
                    return Err(Error::Backend(Box::new(SimpleError("remove delayed"))));
                }
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn retry_success_removes_delayed_after_ack() -> Result<(), Error> {
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
    async fn delay_under_max_retry_records_delayed_without_ack() -> Result<(), Error> {
        let store = MemoryStore::with_claim(2);
        run_once(
            store.clone(),
            into_handler(|(), _| async { Err(ErrorOperation::Delay(Box::new(SimpleError("delay")))) }),
        )
        .await?;
        assert_eq!(
            store.events(),
            vec![Event::Claim("stream-1".into()), Event::RecordDelayed(42)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminal_delay_after_retry_removes_delayed_after_ack() -> Result<(), Error> {
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
                Event::Ack("stream-1".into()),
                Event::RemoveDelayed(42)
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminal_fail_after_retry_removes_delayed_after_ack() -> Result<(), Error> {
        let store = MemoryStore::with_claim(1);
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
                Event::Ack("stream-1".into()),
                Event::RemoveDelayed(42)
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

    #[tokio::test(start_paused = true)]
    async fn ok_and_fail_do_not_backoff_sleep() -> Result<(), Error> {
        let ok_store = MemoryStore::with_claim(1);
        run_until_first_idle(ok_store.clone(), into_handler(|(), _| async { Ok(()) })).await?;
        assert_eq!(
            ok_store.events(),
            vec![
                Event::Claim("stream-1".into()),
                Event::Ack("stream-1".into()),
                Event::RemoveDelayed(42)
            ]
        );

        let fail_store = MemoryStore::with_claim(1);
        run_until_first_idle(
            fail_store.clone(),
            into_handler(|(), _| async { Err(ErrorOperation::Fail(Box::new(SimpleError("fail")))) }),
        )
        .await?;
        assert_eq!(
            fail_store.events(),
            vec![
                Event::Claim("stream-1".into()),
                Event::RecordFailed(42),
                Event::Ack("stream-1".into()),
                Event::RemoveDelayed(42)
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn record_failed_error_skips_ack() {
        let store = MemoryStore::with_claim(3).fail_record_failed_on(1);
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

    #[tokio::test]
    async fn record_delayed_error_skips_ack() {
        let store = MemoryStore::with_claim(2).fail_record_delayed_on(1);
        let result = run_once(
            store.clone(),
            into_handler(|(), _| async { Err(ErrorOperation::Delay(Box::new(SimpleError("delay")))) }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            store.events(),
            vec![Event::Claim("stream-1".into()), Event::RecordDelayed(42)]
        );
    }

    #[tokio::test]
    async fn fail_record_failed_error_skips_ack() {
        let store = MemoryStore::with_claim(0).fail_record_failed_on(1);
        let result = run_once(
            store.clone(),
            into_handler(|(), _| async { Err(ErrorOperation::Fail(Box::new(SimpleError("fail")))) }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            store.events(),
            vec![Event::Claim("stream-1".into()), Event::RecordFailed(42)]
        );
    }

    /// The Ok path acknowledges before delayed-hash cleanup because the handler
    /// has already succeeded; a cleanup failure should surface to the caller,
    /// but it cannot retroactively make the acknowledged stream entry pending.
    #[tokio::test]
    async fn remove_delayed_error_after_ok_keeps_prior_ack() {
        let store = MemoryStore::with_claim(1).fail_remove_delayed_on(1);
        let result = run_once(store.clone(), into_handler(|(), _| async { Ok(()) })).await;

        assert!(result.is_err());
        assert_eq!(
            store.events(),
            vec![
                Event::Claim("stream-1".into()),
                Event::Ack("stream-1".into()),
                Event::RemoveDelayed(42)
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_handler_in_flight_drains_cleanly() -> Result<(), Error> {
        let store = MemoryStore::with_claim(0);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handler_started = Arc::new(Notify::new());
        let handler_continue = Arc::new(Notify::new());
        let handler_started_for_closure = handler_started.clone();
        let handler_continue_for_closure = handler_continue.clone();
        let handle = tokio::spawn(run_worker(
            (),
            into_handler(move |(), _| {
                let handler_started = handler_started_for_closure.clone();
                let handler_continue = handler_continue_for_closure.clone();
                async move {
                    handler_started.notify_one();
                    handler_continue.notified().await;
                    Ok(())
                }
            }),
            store.clone(),
            test_config(),
            shutdown_rx,
        ));

        handler_started.notified().await;
        assert_eq!(store.events(), vec![Event::Claim("stream-1".into())]);
        shutdown_tx
            .send(true)
            .map_err(|e| Error::Shutdown(Box::new(e)))?;
        handler_continue.notify_one();

        handle.await.map_err(|e| Error::Join(Box::new(e)))??;
        assert_eq!(
            store.events(),
            vec![Event::Claim("stream-1".into()), Event::Ack("stream-1".into())]
        );
        Ok(())
    }

    #[tokio::test]
    async fn claim_next_error_exits_worker_without_ack() {
        let store = MemoryStore::with_claim(0).fail_claim_next_on(1);
        let result = run_until_first_idle(store.clone(), into_handler(|(), _| async { Ok(()) })).await;

        assert!(result.is_err());
        assert_eq!(store.events(), vec![Event::ClaimNextError]);
    }

    #[tokio::test]
    async fn worker_set_shutdown_joins_after_signal_error() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        drop(shutdown_rx);
        let marker = Arc::new(Mutex::new(Vec::new()));
        let marker_for_worker = marker.clone();
        let handle = tokio::spawn(async move {
            marker_for_worker
                .lock()
                .map_err(lock_error)?
                .push("joined");
            Ok(())
        });

        let result = WorkerSet::new(shutdown_tx, vec![handle]).shutdown().await;
        assert!(matches!(result, Err(Error::Shutdown(_))));
        assert_eq!(*marker.lock().expect("test mutex should not be poisoned"), ["joined"]);
    }

    #[tokio::test]
    async fn worker_set_join_awaits_all_after_first_error() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let marker = Arc::new(Mutex::new(Vec::new()));
        let first = tokio::spawn(async { Err(Error::Backend(Box::new(SimpleError("first")))) });
        let marker_for_second = marker.clone();
        let second = tokio::spawn(async move {
            marker_for_second
                .lock()
                .map_err(lock_error)?
                .push("second");
            Ok(())
        });

        let result = WorkerSet::new(shutdown_tx, vec![first, second]).join().await;
        assert!(matches!(result, Err(Error::Backend(_))));
        assert_eq!(*marker.lock().expect("test mutex should not be poisoned"), ["second"]);
    }

    #[tokio::test]
    async fn worker_set_shutdown_prefers_worker_error_after_signal_error() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        drop(shutdown_rx);
        let handle = tokio::spawn(async { Err(Error::Backend(Box::new(SimpleError("worker")))) });

        let result = WorkerSet::new(shutdown_tx, vec![handle]).shutdown().await;
        assert!(matches!(result, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn worker_exits_gracefully_when_shutdown_sender_dropped() {
        let store = MemoryStore::default();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run_worker(
            (),
            into_handler(|(), _| async { Ok(()) }),
            store,
            test_config(),
            shutdown_rx,
        ));
        drop(shutdown_tx);

        let result = handle.await.map_err(|e| Error::Join(Box::new(e))).unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn smoke_worker_control_runs_internal_store() -> Result<(), Error> {
        let store = MemoryStore::with_claim(0);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_handler = seen.clone();
        run_once(
            store,
            into_handler(move |(): (), data: String| {
                let seen = seen_for_handler.clone();
                async move {
                    seen.lock()
                        .map_err(|e| ErrorOperation::Fail(Box::new(std::io::Error::other(e.to_string()))))?
                        .push(data);
                    Ok(())
                }
            }),
        )
        .await?;

        assert_eq!(seen.lock().map_err(lock_error)?.as_slice(), ["payload"]);
        Ok(())
    }

    async fn run_once(handler_store: MemoryStore, handler: HandlerFn<(), String>) -> Result<(), Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run_worker((), handler, handler_store, test_config(), shutdown_rx));
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        shutdown_tx
            .send(true)
            .map_err(|e| Error::Shutdown(Box::new(e)))?;
        handle.await.map_err(|e| Error::Join(Box::new(e)))?
    }

    async fn run_until_first_idle(
        handler_store: MemoryStore,
        handler: HandlerFn<(), String>,
    ) -> Result<(), Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(run_worker((), handler, handler_store, test_config(), shutdown_rx));
        tokio::task::yield_now().await;
        shutdown_tx
            .send(true)
            .map_err(|e| Error::Shutdown(Box::new(e)))?;
        handle.await.map_err(|e| Error::Join(Box::new(e)))?
    }

    fn test_config() -> MQConfig {
        MQConfig {
            worker_count: NonZeroUsize::new(1).expect("test worker_count is non-zero"),
            max_retry: 3,
            retry_policy: RetryPolicy::Fixed(std::time::Duration::from_millis(1)),
        }
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
}
