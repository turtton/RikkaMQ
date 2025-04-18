pub mod mq;

use crate::error::Error;
use crate::info::{ErroredInfo, QueueInfo};
use deadpool_redis::redis::streams::StreamReadOptions;
use deadpool_redis::redis::{cmd, AsyncCommands, ErrorKind, RedisError, RedisResult, Value};
use deadpool_redis::Connection;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::str::from_utf8;
use std::time::Duration;

#[derive(Debug)]
pub(in crate::define::redis) struct QueueData<I: Display, T> {
    id: String,
    delivered_count: i64,
    info: QueueInfo<I, T>,
}

const QUEUE_FIELD: &str = "info";

fn group(name: &str) -> String {
    format!("g:{name}")
}

fn failed(name: &str) -> String {
    format!("failed:{name}")
}

fn delayed(name: &str) -> String {
    format!("delayed:{name}")
}

impl From<Value> for Error {
    fn from(value: Value) -> Self {
        Error::ConversionError(Box::new(RedisError::from((
            ErrorKind::ParseError,
            "Failed to parse",
            format!("value: {value:?}"),
        ))))
    }
}

impl From<Vec<Value>> for Error {
    fn from(value: Vec<Value>) -> Self {
        Error::ConversionError(Box::new(RedisError::from((
            ErrorKind::ParseError,
            "Failed to parse",
            format!("value: {value:?}"),
        ))))
    }
}

impl From<&Vec<Value>> for Error {
    fn from(value: &Vec<Value>) -> Self {
        Error::ConversionError(Box::new(RedisError::from((
            ErrorKind::ParseError,
            "Failed to parse",
            format!("value: {value:?}"),
        ))))
    }
}

impl From<&[Value]> for Error {
    fn from(value: &[Value]) -> Self {
        Error::ConversionError(Box::new(RedisError::from((
            ErrorKind::ParseError,
            "Failed to parse",
            format!("value: {value:?}"),
        ))))
    }
}

impl From<RedisError> for Error {
    fn from(value: RedisError) -> Self {
        Error::DatabaseError(Box::new(value))
    }
}

struct RedisJobInternal;

impl RedisJobInternal {
    async fn create_group(con: &mut Connection, name: &str) -> RedisResult<Value> {
        con.xgroup_create_mkstream(name, group(name), 0).await
    }

    async fn insert_waiting<I: Display + Serialize, T: Serialize>(
        con: &mut Connection,
        name: &str,
        info: &QueueInfo<I, T>,
    ) -> Result<(), Error> {
        // Ignore error
        let _ = Self::create_group(con, name).await;
        let serialize = serde_json::to_string(info)?;
        let _: Value = con.xadd(name, "*", &[(QUEUE_FIELD, &serialize)]).await?;
        Ok(())
    }

    /// Get waiting message and mark it as pending messages
    ///
    /// See https://redis.io/docs/latest/commands/xread/
    async fn pop_to_process<I: Display + for<'de> Deserialize<'de>, T>(
        con: &mut Connection,
        name: &str,
        member: &str,
    ) -> Result<Option<QueueData<I, T>>, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        let options = StreamReadOptions::default()
            .block(1000)
            .count(1)
            .group(group(name), member);
        let result: Value = con.xread_options(&[name], &[">"], &options).await?;
        let bulk = match result {
            Value::Array(bulk) => bulk,
            Value::Nil => return Ok(None),
            _ => return Err(result.into()),
        };
        let bulk = match bulk.as_slice() {
            [Value::Array(bulk)] => bulk,
            _ => return Err(bulk.into()),
        };
        let bulk = match bulk.as_slice() {
            [Value::BulkString(_name), Value::Array(bulk)] => bulk,
            _ => return Err(bulk.into()),
        };
        let bulk = match bulk.as_slice() {
            [Value::Array(bulk)] => bulk,
            _ => return Err(bulk.into()),
        };
        let (id, bulk) = match bulk.as_slice() {
            [Value::BulkString(id), Value::Array(bulk)] => (id, bulk),
            _ => return Err(bulk.into()),
        };
        let data = match bulk.as_slice() {
            [Value::BulkString(_field), Value::BulkString(data)] => data,
            _ => return Err(bulk.into()),
        };
        Ok(Some(QueueData {
            id: from_utf8(id)?.to_string(),
            delivered_count: 0,
            info: serde_json::from_slice(data)?,
        }))
    }

    async fn mark_done(con: &mut Connection, name: &str, id: &str) -> Result<(), Error> {
        let _: Value = con.xack(name, group(name), &[id]).await?;
        let _: Value = con.xdel(name, &[id]).await?;
        Ok(())
    }

    /// Get pending message that marked before `idle_time`
    ///
    /// See https://redis.io/docs/latest/commands/xpending/
    async fn pop_pending<I: Display + for<'de> Deserialize<'de>, T>(
        con: &mut Connection,
        name: &str,
        own_member: &str,
        idle_time: &Duration,
    ) -> Result<Option<QueueData<I, T>>, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        // Ignore error
        let _ = Self::create_group(con, name).await;
        let time_millis = u64::try_from(idle_time.as_millis())?;
        let group = group(name);
        let value: Value = cmd("XPENDING")
            .arg(name)
            .arg(&group)
            .arg("IDLE")
            .arg(time_millis)
            .arg("-")
            .arg("+")
            .arg(1) // count
            .query_async(con)
            .await?;

        let bulk = match value {
            Value::Array(bulk) => bulk,
            _ => return Err(value.into()),
        };
        if bulk.is_empty() {
            return Ok(None);
        }
        let bulk = match bulk.as_slice() {
            [Value::Array(bulk)] => bulk,
            _ => return Err(bulk.into()),
        };
        let (id, count) = match bulk.as_slice() {
            [Value::BulkString(id), Value::BulkString(_original_owner), _time, Value::Int(count)] => {
                (from_utf8(id)?.to_string(), *count)
            }
            _ => return Err(bulk.into()),
        };

        let result: Value = con
            .xclaim(name, &group, own_member, time_millis, &[&id])
            .await?;

        let bulk = match result {
            Value::Array(bulk) => bulk,
            _ => return Err(result.into()),
        };
        let bulk = match bulk.as_slice() {
            [Value::Array(bulk)] => bulk,
            _ => return Err(bulk.into()),
        };
        let bulk = match bulk.as_slice() {
            [Value::BulkString(_id), Value::Array(bulk)] => bulk,
            _ => return Err(bulk.into()),
        };
        match bulk.as_slice() {
            [Value::BulkString(_field), Value::BulkString(data)] => {
                let info: QueueInfo<I, T> = serde_json::from_slice(data)?;

                Ok(Some(QueueData {
                    id,
                    delivered_count: count,
                    info,
                }))
            }
            _ => Err(bulk.into()),
        }
    }

    /// Push message as delayed.
    /// Please mark done the message after calling this function.
    async fn push_delayed_info<I: Display + Serialize, T: Serialize>(
        con: &mut Connection,
        name: &str,
        id: I,
        data: T,
        info: String,
    ) -> Result<(), Error> {
        let string_id = id.to_string();
        let info = ErroredInfo::new(id, data, info);
        let raw = serde_json::to_string(&info)?;
        let _: Value = con.hset(delayed(name), &string_id, &raw).await?;
        Ok(())
    }

    async fn remove_delayed_info<I: Display>(
        con: &mut Connection,
        name: &str,
        id: &I,
    ) -> Result<(), Error> {
        let _: Value = con.hdel(delayed(name), id.to_string()).await?;
        Ok(())
    }

    async fn remove_failed_info<I: Display>(
        con: &mut Connection,
        name: &str,
        id: &I,
    ) -> Result<(), Error> {
        let _: Value = con.hdel(failed(name), id.to_string()).await?;
        Ok(())
    }

    async fn get_hash_len(con: &mut Connection, name: &str) -> Result<usize, Error> {
        let result: Value = con.hlen(name).await?;
        let size = match result {
            Value::Int(size) => size,
            _ => return Err(result.into()),
        };
        let size = usize::try_from(size)?;
        Ok(size)
    }

    async fn push_failed_info<I: Display + Serialize, T: Serialize>(
        con: &mut Connection,
        name: &str,
        id: I,
        data: T,
        info: String,
    ) -> Result<(), Error> {
        let raw_id = id.to_string();
        let data = ErroredInfo::new(id, data, info);
        let raw = serde_json::to_string(&data)?;
        let _: Value = con.hset(failed(name), &raw_id, &raw).await?;
        Ok(())
    }

    async fn get_stream_len(con: &mut Connection, name: &str) -> Result<i64, Error> {
        let result: Value = con.xlen(name).await?;
        if let Value::Int(size) = result {
            Ok(size)
        } else {
            Err(result.into())
        }
    }

    async fn get_infos_from_hash<T: for<'de> Deserialize<'de>>(
        con: &mut Connection,
        name: &str,
        size: &i64,
        offset: &i64,
    ) -> Result<Vec<T>, Error> {
        if *size <= 0 {
            return Ok(vec![]);
        }
        let result: Value = cmd("HSCAN")
            .arg(name)
            .arg(offset)
            .arg("COUNT")
            .arg(size)
            .query_async(con)
            .await?;
        let bulk = match result {
            Value::Array(bulk) => bulk,
            _ => return Err(result.into()),
        };
        let bulk = match bulk.as_slice() {
            [Value::BulkString(_offset), Value::Array(bulk)] => bulk,
            _ => return Err(bulk.into()),
        };
        let usize = usize::try_from(*size)?;
        // HSCAN may return more than size
        bulk.chunks(2)
            .take(usize)
            .map(|pair| match pair {
                [Value::BulkString(_id), Value::BulkString(data)] => {
                    let info = serde_json::from_slice(data)?;
                    Ok(info)
                }
                _ => Err(pair.into()),
            })
            .collect()
    }

    async fn get_info_from_hash<I: Display, T: for<'de> Deserialize<'de>>(
        con: &mut Connection,
        name: &str,
        id: &I,
    ) -> Result<Option<T>, Error> {
        let result: Value = con.hget(name, id.to_string()).await?;
        match result {
            Value::BulkString(data) => {
                let info = serde_json::from_slice(&data)?;
                Ok(Some(info))
            }
            Value::Nil => Ok(None),
            _ => Err(result.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::define::redis::{delayed, failed, QueueData, RedisJobInternal};
    use crate::error::Error;
    use crate::info::{ErroredInfo, QueueInfo};
    use deadpool_redis::{Config, CreatePoolError, Pool, Runtime};
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio::time::sleep;
    use uuid::Uuid;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct TestData {
        pub a: String,
    }

    const REDIS_URL: &str = "REDIS_URL";

    impl From<CreatePoolError> for Error {
        fn from(error: CreatePoolError) -> Self {
            Error::DatabaseError(Box::new(error))
        }
    }

    pub fn create_pool() -> Result<Pool, Error> {
        let url = dotenvy::var(REDIS_URL).unwrap();
        let cfg = Config::from_url(url);
        let db = cfg.create_pool(Some(Runtime::Tokio1))?;
        Ok(db)
    }

    #[test_with::env(REDIS_URL)]
    #[tokio::test]
    async fn test_positive() -> Result<(), Error> {
        let pool = create_pool()?;
        let mut con = pool.get().await?;
        let name = "test_positive";
        let member = "m_test_positive";
        let data = TestData {
            a: "testtss".to_string(),
        };
        let info = QueueInfo::new(Uuid::new_v4(), data);

        let len = RedisJobInternal::get_stream_len(&mut con, name).await?;
        assert_eq!(len, 0);

        RedisJobInternal::insert_waiting(&mut con, name, &info).await?;

        let len = RedisJobInternal::get_stream_len(&mut con, name).await?;
        assert_eq!(len, 1);
        let result: QueueData<Uuid, TestData> =
            RedisJobInternal::pop_to_process(&mut con, name, member)
                .await?
                .unwrap();
        println!("result: {result:?}");

        sleep(Duration::from_secs(1)).await;
        let pending: Option<QueueData<Uuid, TestData>> =
            RedisJobInternal::pop_pending(&mut con, name, member, &Duration::from_millis(500))
                .await?;
        println!("result: {pending:?}");

        let len = RedisJobInternal::get_stream_len(&mut con, name).await?;
        assert_eq!(len, 1);

        RedisJobInternal::mark_done(&mut con, name, &result.id).await?;

        let len = RedisJobInternal::get_stream_len(&mut con, name).await?;
        assert_eq!(len, 0);
        Ok(())
    }

    #[test_with::env(REDIS_URL)]
    #[tokio::test]
    async fn test_delayed() {
        let pool = create_pool().unwrap();
        let mut con = pool.get().await.unwrap();
        let name = "test_delayed";
        let member = "m_test_delayed";
        let data = TestData {
            a: "testtss".to_string(),
        };
        let info = QueueInfo::new(Uuid::new_v4(), data);
        RedisJobInternal::insert_waiting(&mut con, name, &info)
            .await
            .unwrap();
        let result: QueueData<Uuid, TestData> =
            RedisJobInternal::pop_to_process(&mut con, name, member)
                .await
                .unwrap()
                .unwrap();
        println!("result: {result:?}");
        let info = result.info.into_destruct();
        RedisJobInternal::push_delayed_info(
            &mut con,
            name,
            info.id,
            info.data.clone(),
            "delayed".to_string(),
        )
        .await
        .unwrap();

        let delayed_name = delayed(name);
        let delayed = RedisJobInternal::get_info_from_hash::<Uuid, ErroredInfo<Uuid, TestData>>(
            &mut con,
            &delayed_name,
            &info.id,
        )
        .await
        .unwrap();
        assert!(delayed.is_some());
        let delayed = delayed.unwrap().into_destruct();
        assert_eq!(delayed.data, info.data);

        let len = RedisJobInternal::get_hash_len(&mut con, &delayed_name)
            .await
            .unwrap();
        assert_eq!(len, 1);

        RedisJobInternal::remove_delayed_info(&mut con, name, &info.id)
            .await
            .unwrap();

        let len = RedisJobInternal::get_hash_len(&mut con, &delayed_name)
            .await
            .unwrap();
        assert_eq!(len, 0);

        RedisJobInternal::mark_done(&mut con, name, &result.id)
            .await
            .unwrap();
    }

    #[test_with::env(REDIS_URL)]
    #[tokio::test]
    async fn test_failed() {
        let pool = create_pool().unwrap();
        let mut con = pool.get().await.unwrap();
        let name = "test_failed";
        let member = "m_test_failed";
        let data = TestData {
            a: "testtss".to_string(),
        };
        let info = QueueInfo::new(Uuid::new_v4(), data);
        RedisJobInternal::insert_waiting(&mut con, name, &info)
            .await
            .unwrap();
        let result: QueueData<Uuid, TestData> =
            RedisJobInternal::pop_to_process(&mut con, name, member)
                .await
                .unwrap()
                .unwrap();
        println!("result: {result:?}");
        let info = result.info.into_destruct();
        RedisJobInternal::push_failed_info(
            &mut con,
            name,
            info.id,
            info.data.clone(),
            "failed".to_string(),
        )
        .await
        .unwrap();
        RedisJobInternal::mark_done(&mut con, name, &result.id)
            .await
            .unwrap();

        let failed_name = failed(name);
        let delayed = RedisJobInternal::get_info_from_hash::<Uuid, ErroredInfo<Uuid, TestData>>(
            &mut con,
            &failed_name,
            &info.id,
        )
        .await
        .unwrap();
        assert!(delayed.is_some());
        let delayed = delayed.unwrap().into_destruct();
        assert_eq!(delayed.data, info.data);

        let len = RedisJobInternal::get_hash_len(&mut con, &failed_name)
            .await
            .unwrap();
        assert_eq!(len, 1);

        RedisJobInternal::remove_failed_info(&mut con, name, &info.id)
            .await
            .unwrap();

        let len = RedisJobInternal::get_hash_len(&mut con, &failed_name)
            .await
            .unwrap();
        assert_eq!(len, 0);
    }
}
