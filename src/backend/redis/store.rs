use super::keys::{delayed, failed, group, QUEUE_FIELD};
use crate::error::Error;
use crate::info::{ErroredInfo, QueueInfo, StoredErroredInfo};
use crate::worker::{ClaimedMessage, QueueStore};
use deadpool_redis::redis::streams::StreamReadOptions;
use deadpool_redis::redis::{cmd, AsyncCommands, Value};
use deadpool_redis::{Connection, Pool};
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt::Display;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct RedisStoreOps {
    pool: Pool,
    name: String,
    /// Tracks whether the consumer group has been ensured on Redis for
    /// this process; ensures `XGROUP CREATE MKSTREAM` runs at most once
    /// per [`RedisMessageQueue`] (Phase 3 task 14).
    group_initialized: Arc<tokio::sync::OnceCell<()>>,
}

impl RedisStoreOps {
    pub(crate) fn new(pool: Pool, name: String) -> Self {
        Self {
            pool,
            name,
            group_initialized: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    async fn connection(&self) -> Result<Connection, Error> {
        Ok(self.pool.get().await?)
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Idempotently ensure the consumer group exists. Runs the actual
    /// `XGROUP CREATE MKSTREAM` only on the first successful call; later
    /// callers observe the cached `()` and skip the round-trip entirely.
    pub(crate) async fn ensure_group(&self, con: &mut Connection) -> Result<(), Error> {
        if self.group_initialized.initialized() {
            return Ok(());
        }
        self.group_initialized
            .get_or_try_init(|| Self::create_group_once(con, &self.name))
            .await?;
        Ok(())
    }

    async fn create_group_once(con: &mut Connection, name: &str) -> Result<(), Error> {
        let result: Result<Value, _> = con.xgroup_create_mkstream(name, group(name), "0").await;
        match result {
            Ok(_) => Ok(()),
            // BUSYGROUP means the group already exists on the server (e.g.
            // another process initialized it). We treat that as success
            // because the post-condition - "group exists" - holds either way.
            Err(err) if err.to_string().contains("BUSYGROUP") => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) async fn insert_waiting<I, T>(&self, info: &QueueInfo<I, T>) -> Result<(), Error>
    where
        I: Display + Serialize,
        T: Serialize,
    {
        let mut con = self.connection().await?;
        self.ensure_group(&mut con).await?;
        let serialized = serde_json::to_string(info).map_err(|e| Error::Serialize(Box::new(e)))?;
        let _: Value = con
            .xadd(&self.name, "*", &[(QUEUE_FIELD, serialized)])
            .await?;
        Ok(())
    }

    pub(crate) async fn stream_len(&self) -> Result<u64, Error> {
        let mut con = self.connection().await?;
        let result: Value = con.xlen(&self.name).await?;
        match result {
            Value::Int(size) => u64::try_from(size).map_err(|e| Error::Protocol {
                op: "XLEN",
                detail: e.to_string(),
            }),
            other => Err(Error::protocol(
                "XLEN",
                format!("expected integer, got {other:?}"),
            )),
        }
    }

    pub(crate) async fn hash_len(&self, key: &str) -> Result<u64, Error> {
        let mut con = self.connection().await?;
        let result: Value = con.hlen(key).await?;
        match result {
            Value::Int(size) => u64::try_from(size).map_err(|e| Error::Protocol {
                op: "HLEN",
                detail: e.to_string(),
            }),
            other => Err(Error::protocol(
                "HLEN",
                format!("expected integer, got {other:?}"),
            )),
        }
    }

    pub(crate) async fn get_hash<T>(&self, key: &str, id: &impl Display) -> Result<Option<T>, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut con = self.connection().await?;
        let result: Value = con.hget(key, id.to_string()).await?;
        match result {
            Value::BulkString(data) => serde_json::from_slice(&data)
                .map(Some)
                .map_err(|e| Error::Deserialize(Box::new(e))),
            Value::Nil => Ok(None),
            other => Err(Error::protocol(
                "HGET",
                format!("expected bulk string or nil, got {other:?}"),
            )),
        }
    }

    pub(crate) async fn scan_hash<T>(
        &self,
        key: &str,
        limit: usize,
        cursor: &str,
    ) -> Result<(Vec<T>, String), Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        if limit == 0 {
            return Ok((Vec::new(), cursor.to_string()));
        }
        let mut con = self.connection().await?;
        let result: Value = cmd("HSCAN")
            .arg(key)
            .arg(cursor)
            .arg("COUNT")
            .arg(limit)
            .query_async(&mut con)
            .await?;
        let bulk = match result {
            Value::Array(bulk) => bulk,
            other => {
                return Err(Error::protocol(
                    "HSCAN",
                    format!("expected array, got {other:?}"),
                ))
            }
        };
        let (next, entries) = match bulk.as_slice() {
            [Value::BulkString(next), Value::Array(entries)] => {
                (std::str::from_utf8(next)?.to_string(), entries)
            }
            other => {
                return Err(Error::protocol(
                    "HSCAN",
                    format!("unexpected response shape: {other:?}"),
                ))
            }
        };
        let mut items = Vec::new();
        for pair in entries.chunks(2) {
            match pair {
                [Value::BulkString(_id), Value::BulkString(data)] => {
                    items.push(
                        serde_json::from_slice(data)
                            .map_err(|e| Error::Deserialize(Box::new(e)))?,
                    );
                }
                other => {
                    return Err(Error::protocol(
                        "HSCAN",
                        format!("unexpected entry shape: {other:?}"),
                    ))
                }
            }
        }
        Ok((items, next))
    }

    pub(crate) async fn remove_failed(&self, id: &impl Display) -> Result<(), Error> {
        let mut con = self.connection().await?;
        let _: Value = con.hdel(failed(&self.name), id.to_string()).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct RedisQueueStore<I, T> {
    ops: RedisStoreOps,
    consumer_id: String,
    retry_delay: Duration,
    _marker: PhantomData<fn() -> (I, T)>,
}

impl<I, T> RedisQueueStore<I, T> {
    pub(crate) fn new(ops: RedisStoreOps, consumer_id: String, retry_delay: Duration) -> Self {
        Self {
            ops,
            consumer_id,
            retry_delay,
            _marker: PhantomData,
        }
    }
}

impl<I, T> RedisQueueStore<I, T>
where
    I: Display + Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    async fn pop_to_process(
        &self,
        con: &mut Connection,
    ) -> Result<Option<ClaimedMessage<I, T>>, Error> {
        let options = StreamReadOptions::default()
            .block(1000)
            .count(1)
            .group(group(&self.ops.name), &self.consumer_id);
        let result: Value = con
            .xread_options(&[&self.ops.name], &[">"], &options)
            .await?;
        parse_xread(result, "XREAD", 0)
    }

    async fn pop_pending(
        &self,
        con: &mut Connection,
    ) -> Result<Option<ClaimedMessage<I, T>>, Error> {
        self.ops.ensure_group(con).await?;
        let time_millis = u64::try_from(self.retry_delay.as_millis())?;
        let group_name = group(&self.ops.name);
        let value: Value = cmd("XPENDING")
            .arg(&self.ops.name)
            .arg(&group_name)
            .arg("IDLE")
            .arg(time_millis)
            .arg("-")
            .arg("+")
            .arg(1)
            .query_async(con)
            .await?;
        let bulk = match value {
            Value::Array(bulk) => bulk,
            other => {
                return Err(Error::protocol(
                    "XPENDING",
                    format!("expected array, got {other:?}"),
                ))
            }
        };
        if bulk.is_empty() {
            return Ok(None);
        }
        let entry = match bulk.as_slice() {
            [Value::Array(entry)] => entry,
            other => {
                return Err(Error::protocol(
                    "XPENDING",
                    format!("unexpected response shape: {other:?}"),
                ))
            }
        };
        let (id, delivered_count) = match entry.as_slice() {
            [Value::BulkString(id), Value::BulkString(_owner), _idle, Value::Int(count)] => {
                (std::str::from_utf8(id)?.to_string(), u32::try_from(*count)?)
            }
            other => {
                return Err(Error::protocol(
                    "XPENDING",
                    format!("unexpected entry shape: {other:?}"),
                ))
            }
        };
        let result: Value = con
            .xclaim(
                &self.ops.name,
                &group_name,
                &self.consumer_id,
                time_millis,
                &[&id],
            )
            .await?;
        parse_xclaim(result, delivered_count)
    }

    async fn mark_done(&self, stream_id: &str) -> Result<(), Error> {
        let mut con = self.ops.connection().await?;
        let _: Value = con
            .xack(&self.ops.name, group(&self.ops.name), &[stream_id])
            .await?;
        let _: Value = con.xdel(&self.ops.name, &[stream_id]).await?;
        Ok(())
    }

    async fn push_delayed(
        &self,
        info: QueueInfo<I, T>,
        error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        self.push_errored(delayed(&self.ops.name), info, error)
            .await
    }

    async fn push_failed(
        &self,
        info: QueueInfo<I, T>,
        error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        self.push_errored(failed(&self.ops.name), info, error).await
    }

    async fn push_errored(
        &self,
        key: String,
        info: QueueInfo<I, T>,
        error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        let id_string = info.id().to_string();
        let (id, data) = info.into_parts();
        let stored: StoredErroredInfo<I, T> = ErroredInfo::new(id, data, error).into();
        let raw = serde_json::to_string(&stored).map_err(|e| Error::Serialize(Box::new(e)))?;
        let mut con = self.ops.connection().await?;
        let _: Value = con.hset(key, id_string, raw).await?;
        Ok(())
    }

    async fn remove_delayed_inner(&self, id: &I) -> Result<(), Error> {
        let mut con = self.ops.connection().await?;
        let _: Value = con.hdel(delayed(&self.ops.name), id.to_string()).await?;
        Ok(())
    }
}

impl<I, T> QueueStore<I, T> for RedisQueueStore<I, T>
where
    I: Display + Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    fn claim_next<'a>(
        &'a self,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<ClaimedMessage<I, T>>, Error>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut con = self.ops.connection().await?;
            match self.pop_pending(&mut con).await? {
                Some(claimed) => Ok(Some(claimed)),
                None => self.pop_to_process(&mut con).await,
            }
        })
    }

    fn ack<'a>(
        &'a self,
        stream_id: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move { self.mark_done(stream_id).await })
    }

    fn record_delayed<'a>(
        &'a self,
        info: QueueInfo<I, T>,
        error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move { self.push_delayed(info, error).await })
    }

    fn record_failed<'a>(
        &'a self,
        info: QueueInfo<I, T>,
        error: Box<dyn StdError + Send + Sync + 'static>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move { self.push_failed(info, error).await })
    }

    fn remove_delayed<'a>(
        &'a self,
        id: &'a I,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move { self.remove_delayed_inner(id).await })
    }
}

fn parse_xread<I, T>(
    result: Value,
    op: &'static str,
    delivered_count: u32,
) -> Result<Option<ClaimedMessage<I, T>>, Error>
where
    I: for<'de> Deserialize<'de>,
    T: for<'de> Deserialize<'de>,
{
    let bulk = match result {
        Value::Array(bulk) => bulk,
        Value::Nil => return Ok(None),
        other => {
            return Err(Error::protocol(
                op,
                format!("expected array or nil, got {other:?}"),
            ))
        }
    };
    let streams = match bulk.as_slice() {
        [Value::Array(streams)] => streams,
        [] => return Ok(None),
        other => {
            return Err(Error::protocol(
                op,
                format!("unexpected response shape: {other:?}"),
            ))
        }
    };
    let entries = match streams.as_slice() {
        [Value::BulkString(_name), Value::Array(entries)] => entries,
        other => {
            return Err(Error::protocol(
                op,
                format!("unexpected stream shape: {other:?}"),
            ))
        }
    };
    let entry = match entries.as_slice() {
        [Value::Array(entry)] => entry,
        [] => return Ok(None),
        other => {
            return Err(Error::protocol(
                op,
                format!("unexpected entries shape: {other:?}"),
            ))
        }
    };
    parse_stream_entry(entry, op, delivered_count)
}

fn parse_xclaim<I, T>(
    result: Value,
    delivered_count: u32,
) -> Result<Option<ClaimedMessage<I, T>>, Error>
where
    I: for<'de> Deserialize<'de>,
    T: for<'de> Deserialize<'de>,
{
    let entries = match result {
        Value::Array(entries) => entries,
        other => {
            return Err(Error::protocol(
                "XCLAIM",
                format!("expected array, got {other:?}"),
            ))
        }
    };
    let entry = match entries.as_slice() {
        [Value::Array(entry)] => entry,
        [] => return Ok(None),
        other => {
            return Err(Error::protocol(
                "XCLAIM",
                format!("unexpected entries shape: {other:?}"),
            ))
        }
    };
    parse_stream_entry(entry, "XCLAIM", delivered_count)
}

fn parse_stream_entry<I, T>(
    entry: &[Value],
    op: &'static str,
    delivered_count: u32,
) -> Result<Option<ClaimedMessage<I, T>>, Error>
where
    I: for<'de> Deserialize<'de>,
    T: for<'de> Deserialize<'de>,
{
    let (stream_id, fields) = match entry {
        [Value::BulkString(id), Value::Array(fields)] => {
            (std::str::from_utf8(id)?.to_string(), fields)
        }
        other => {
            return Err(Error::protocol(
                op,
                format!("unexpected entry shape: {other:?}"),
            ))
        }
    };
    match fields.as_slice() {
        [Value::BulkString(_field), Value::BulkString(data)] => {
            let info = serde_json::from_slice(data).map_err(|e| Error::Deserialize(Box::new(e)))?;
            Ok(Some(ClaimedMessage {
                stream_id,
                delivered_count,
                info,
            }))
        }
        other => Err(Error::protocol(
            op,
            format!("unexpected fields shape: {other:?}"),
        )),
    }
}
