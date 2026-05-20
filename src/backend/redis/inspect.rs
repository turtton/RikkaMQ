use super::keys::{delayed, failed};
use super::mq::RedisMessageQueue;
use crate::error::Error;
use crate::info::{ErroredInfo, QueueInfo, StoredErroredInfo};
use crate::inspect::{Cursor, FailedRetry, QueueInspector, ScanPage};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::fmt::Display;

impl<I, T> QueueInspector<I, T> for RedisMessageQueue<I, T>
where
    I: Display + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    T: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    async fn delayed_len(&self) -> Result<u64, Error> {
        self.ops.hash_len(&delayed(self.ops.name())).await
    }

    async fn failed_len(&self) -> Result<u64, Error> {
        self.ops.hash_len(&failed(self.ops.name())).await
    }

    async fn get_delayed(&self, id: &I) -> Result<Option<QueueInfo<I, T>>, Error> {
        let stored: Option<StoredErroredInfo<I, T>> =
            self.ops.get_hash(&delayed(self.ops.name()), id).await?;
        Ok(stored.map(|info| QueueInfo::new(info.id, info.data)))
    }

    async fn get_failed(&self, id: &I) -> Result<Option<ErroredInfo<I, T>>, Error> {
        let stored: Option<StoredErroredInfo<I, T>> =
            self.ops.get_hash(&failed(self.ops.name()), id).await?;
        Ok(stored.map(Into::into))
    }

    async fn scan_delayed(
        &self,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<ScanPage<QueueInfo<I, T>>, Error> {
        let start = cursor.as_ref().map_or("0", Cursor::as_str);
        let (items, next) = self
            .ops
            .scan_hash::<StoredErroredInfo<I, T>>(&delayed(self.ops.name()), limit, start)
            .await?;
        Ok(ScanPage {
            items: items
                .into_iter()
                .map(|info| QueueInfo::new(info.id, info.data))
                .collect(),
            next_cursor: (next != "0").then(|| Cursor::new(next)),
        })
    }

    async fn scan_failed(
        &self,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<ScanPage<ErroredInfo<I, T>>, Error> {
        let start = cursor.as_ref().map_or("0", Cursor::as_str);
        let (items, next) = self
            .ops
            .scan_hash::<StoredErroredInfo<I, T>>(&failed(self.ops.name()), limit, start)
            .await?;
        Ok(ScanPage {
            items: items.into_iter().map(Into::into).collect(),
            next_cursor: (next != "0").then(|| Cursor::new(next)),
        })
    }
}

impl<I, T> FailedRetry<I> for RedisMessageQueue<I, T>
where
    I: Display + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    T: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    async fn retry_failed(&self, id: &I) -> Result<(), Error> {
        retry_failed_via(self, id).await
    }
}

pub(crate) trait FailedRetrySeam<I, T>: Sync {
    fn load_failed(
        &self,
        id: &I,
    ) -> impl Future<Output = Result<Option<StoredErroredInfo<I, T>>, Error>> + Send;
    fn insert_waiting(&self, info: &QueueInfo<I, T>) -> impl Future<Output = Result<(), Error>> + Send;
    fn remove_failed_info(&self, id: &I) -> impl Future<Output = Result<(), Error>> + Send;
}

pub(crate) async fn retry_failed_via<S, I, T>(seam: &S, id: &I) -> Result<(), Error>
where
    S: FailedRetrySeam<I, T>,
    I: Clone,
{
    if let Some(info) = seam.load_failed(id).await? {
        let waiting = QueueInfo::new(info.id.clone(), info.data);
        seam.insert_waiting(&waiting).await?;
        seam.remove_failed_info(&info.id).await?;
    }
    Ok(())
}

impl<I, T> FailedRetrySeam<I, T> for RedisMessageQueue<I, T>
where
    I: Display + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    T: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    async fn load_failed(&self, id: &I) -> Result<Option<StoredErroredInfo<I, T>>, Error> {
        self.ops.get_hash(&failed(self.ops.name()), id).await
    }

    async fn insert_waiting(&self, info: &QueueInfo<I, T>) -> Result<(), Error> {
        self.ops.insert_waiting(info).await
    }

    async fn remove_failed_info(&self, id: &I) -> Result<(), Error> {
        self.ops.remove_failed(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::{retry_failed_via, FailedRetrySeam};
    use crate::error::Error;
    use crate::info::{QueueInfo, StoredErroredInfo};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Event {
        LoadFailed(u64),
        InsertWaiting(u64),
        RemoveFailed(u64),
    }

    #[derive(Clone, Default)]
    struct RetrySeamFake {
        state: Arc<Mutex<RetrySeamState>>,
    }

    #[derive(Default)]
    struct RetrySeamState {
        failed: HashMap<u64, StoredErroredInfo<u64, String>>,
        waiting: HashMap<u64, QueueInfo<u64, String>>,
        events: Vec<Event>,
        fail_insert_waiting: bool,
        fail_remove_failed: bool,
    }

    impl RetrySeamFake {
        fn with_failed(id: u64) -> Self {
            let seam = Self::default();
            seam.state
                .lock()
                .expect("test mutex should not be poisoned")
                .failed
                .insert(
                    id,
                    StoredErroredInfo {
                        id,
                        data: "payload".to_string(),
                        error: "failed".to_string(),
                    },
                );
            seam
        }

        fn fail_insert_waiting(self) -> Self {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .fail_insert_waiting = true;
            self
        }

        fn fail_remove_failed(self) -> Self {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .fail_remove_failed = true;
            self
        }

        fn events(&self) -> Vec<Event> {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .events
                .clone()
        }

        fn has_failed(&self, id: u64) -> bool {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .failed
                .contains_key(&id)
        }

        fn has_waiting(&self, id: u64) -> bool {
            self.state
                .lock()
                .expect("test mutex should not be poisoned")
                .waiting
                .contains_key(&id)
        }
    }

    impl FailedRetrySeam<u64, String> for RetrySeamFake {
        async fn load_failed(&self, id: &u64) -> Result<Option<StoredErroredInfo<u64, String>>, Error> {
            let mut state = self.state.lock().map_err(lock_error)?;
            state.events.push(Event::LoadFailed(*id));
            Ok(state.failed.get(id).map(|info| StoredErroredInfo {
                id: info.id,
                data: info.data.clone(),
                error: info.error.clone(),
            }))
        }

        async fn insert_waiting(&self, info: &QueueInfo<u64, String>) -> Result<(), Error> {
            let mut state = self.state.lock().map_err(lock_error)?;
            let id = *info.id();
            state.events.push(Event::InsertWaiting(id));
            if state.fail_insert_waiting {
                return Err(Error::Backend(Box::new(SimpleError("insert waiting"))));
            }
            state
                .waiting
                .insert(id, QueueInfo::new(id, info.data().clone()));
            Ok(())
        }

        async fn remove_failed_info(&self, id: &u64) -> Result<(), Error> {
            let mut state = self.state.lock().map_err(lock_error)?;
            state.events.push(Event::RemoveFailed(*id));
            if state.fail_remove_failed {
                return Err(Error::Backend(Box::new(SimpleError("remove failed"))));
            }
            state.failed.remove(id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn retry_failed_remove_error_leaves_waiting_and_failed() {
        let seam = RetrySeamFake::with_failed(42).fail_remove_failed();
        let result = retry_failed_via(&seam, &42).await;

        assert!(result.is_err());
        assert!(seam.has_waiting(42));
        assert!(seam.has_failed(42));
        assert_eq!(
            seam.events(),
            vec![
                Event::LoadFailed(42),
                Event::InsertWaiting(42),
                Event::RemoveFailed(42)
            ]
        );
    }

    #[tokio::test]
    async fn retry_failed_insert_error_never_removes_failed() {
        let seam = RetrySeamFake::with_failed(42).fail_insert_waiting();
        let result = retry_failed_via(&seam, &42).await;

        assert!(result.is_err());
        assert!(!seam.has_waiting(42));
        assert!(seam.has_failed(42));
        assert_eq!(
            seam.events(),
            vec![Event::LoadFailed(42), Event::InsertWaiting(42)]
        );
    }

    #[derive(Debug)]
    struct SimpleError(&'static str);

    impl std::fmt::Display for SimpleError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for SimpleError {}

    fn lock_error<T>(err: std::sync::PoisonError<T>) -> Error {
        Error::Backend(Box::new(std::io::Error::other(err.to_string())))
    }
}
