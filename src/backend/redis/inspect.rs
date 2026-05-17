use super::keys::{delayed, failed};
use super::mq::RedisMessageQueue;
use crate::error::Error;
use crate::info::{ErroredInfo, QueueInfo, StoredErroredInfo};
use crate::inspect::{Cursor, FailedRetry, QueueInspector, ScanPage};
use serde::{Deserialize, Serialize};
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
        let stored: Option<StoredErroredInfo<I, T>> = self
            .ops
            .get_hash(&failed(self.ops.name()), id)
            .await?;
        if let Some(info) = stored {
            let waiting = QueueInfo::new(info.id.clone(), info.data);
            self.ops.insert_waiting(&waiting).await?;
            self.ops.remove_failed(&info.id).await?;
        }
        Ok(())
    }
}
