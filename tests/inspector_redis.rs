#![cfg(all(feature = "redis", feature = "tokio"))]

use deadpool_redis::redis::cmd;
use deadpool_redis::{Config, Runtime};
use rikka_mq::backend::redis::RedisMessageQueue;
use rikka_mq::config::{MQConfig, RetryPolicy};
use rikka_mq::error::{Error, ErrorOperation};
use rikka_mq::handler::into_handler;
use rikka_mq::info::{ErroredInfo, QueueInfo};
use rikka_mq::inspect::{Cursor, QueueInspector};
use rikka_mq::mq::{MessageQueue, QueueStats};
use rikka_mq::worker::WorkerControl;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use uuid::Uuid;

type TestResult = Result<(), Error>;
type TestQueue = RedisMessageQueue<u64, String>;

#[ignore = "requires Docker for Redis testcontainer"]
#[tokio::test(flavor = "multi_thread")]
async fn queued_len_reflects_enqueue_and_consume() -> TestResult {
    let setup = setup(MQConfig::default()).await?;
    let mq = &setup.mq;

    assert_eq!(mq.queued_len().await?, 0);
    enqueue_messages(mq, 0..5).await?;
    assert_eq!(mq.queued_len().await?, 5);

    let completed = Arc::new(AtomicUsize::new(0));
    let completed_for_handler = completed.clone();
    let workers = mq
        .start_workers(
            (),
            into_handler(move |(), _payload: String| {
                let completed = completed_for_handler.clone();
                async move {
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }),
        )
        .await?;

    wait_for("handler to complete five messages", Duration::from_secs(10), || {
        let completed = completed.clone();
        async move { Ok((completed.load(Ordering::SeqCst) == 5).then_some(())) }
    })
    .await?;
    tokio::time::timeout(Duration::from_secs(2), workers.shutdown())
        .await
        .map_err(|e| Error::Shutdown(Box::new(e)))??;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(mq.queued_len().await?, 0);
    assert_eq!(completed.load(Ordering::SeqCst), 5);
    Ok(())
}

#[ignore = "requires Docker for Redis testcontainer"]
#[tokio::test(flavor = "multi_thread")]
async fn failed_len_and_get_failed_round_trip() -> TestResult {
    let setup = setup(MQConfig::default()).await?;
    let mq = &setup.mq;
    let ids = enqueue_messages(mq, 0..3).await?;

    let workers = mq
        .start_workers(
            (),
            into_handler(|(), payload: String| async move {
                Err(ErrorOperation::Fail(Box::new(std::io::Error::other(format!(
                    "failed {payload}"
                )))))
            }),
        )
        .await?;

    wait_for_failed_len(mq, 3).await?;
    for id in &ids {
        let failed = mq
            .get_failed(id)
            .await?
            .ok_or_else(|| Error::Backend(Box::new(std::io::Error::other("missing failed entry"))))?;
        assert_valid_errored_info(&failed);
        assert_eq!(failed.id(), id);
        assert_eq!(failed.data(), &payload_for(*id));
    }

    tokio::time::timeout(Duration::from_secs(2), workers.shutdown())
        .await
        .map_err(|e| Error::Shutdown(Box::new(e)))??;
    Ok(())
}

#[ignore = "requires Docker for Redis testcontainer"]
#[tokio::test(flavor = "multi_thread")]
async fn delayed_len_and_get_delayed_round_trip() -> TestResult {
    let setup = setup(MQConfig {
        retry_policy: RetryPolicy::Fixed(Duration::from_secs(60)),
        max_retry: 5,
        ..MQConfig::default()
    })
    .await?;
    let mq = &setup.mq;
    let ids = enqueue_messages(mq, 0..2).await?;

    let workers = mq
        .start_workers(
            (),
            into_handler(|(), payload: String| async move {
                Err(ErrorOperation::Delay(Box::new(std::io::Error::other(format!(
                    "delayed {payload}"
                )))))
            }),
        )
        .await?;

    wait_for("delayed_len to reach all entries", Duration::from_secs(4), || async {
        Ok((mq.delayed_len().await? == 2).then_some(()))
    })
    .await?;
    for id in &ids {
        let delayed = mq
            .get_delayed(id)
            .await?
            .ok_or_else(|| Error::Backend(Box::new(std::io::Error::other("missing delayed entry"))))?;
        assert_eq!(delayed.id(), id);
        assert_eq!(delayed.data(), &payload_for(*id));
    }
    assert_eq!(mq.delayed_len().await?, 2);

    tokio::time::timeout(Duration::from_secs(2), workers.shutdown())
        .await
        .map_err(|e| Error::Shutdown(Box::new(e)))??;
    Ok(())
}

#[ignore = "requires Docker for Redis testcontainer"]
#[tokio::test(flavor = "multi_thread")]
async fn scan_failed_walks_full_set_with_cursor() -> TestResult {
    let setup = setup(MQConfig::default()).await?;
    let mq = &setup.mq;
    let expected: HashSet<_> = enqueue_messages(mq, 0..25).await?.into_iter().collect();

    let workers = mq
        .start_workers(
            (),
            into_handler(|(), payload: String| async move {
                Err(ErrorOperation::Fail(Box::new(std::io::Error::other(format!(
                    "failed {payload}"
                )))))
            }),
        )
        .await?;

    wait_for_failed_len(mq, 25).await?;
    let mut cursor: Option<Cursor> = None;
    let mut collected = Vec::new();
    loop {
        let page = mq.scan_failed(7, cursor.take()).await?;
        collected.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    assert_errored_ids(collected, &expected);
    tokio::time::timeout(Duration::from_secs(2), workers.shutdown())
        .await
        .map_err(|e| Error::Shutdown(Box::new(e)))??;
    Ok(())
}

#[ignore = "requires Docker for Redis testcontainer"]
#[tokio::test(flavor = "multi_thread")]
async fn scan_delayed_walks_full_set_with_cursor() -> TestResult {
    let setup = setup(MQConfig {
        worker_count: NonZeroUsize::new(1).expect("test worker_count is non-zero"),
        retry_policy: RetryPolicy::Fixed(Duration::from_secs(60)),
        max_retry: 50,
    })
    .await?;
    let mq = &setup.mq;
    let expected: HashSet<_> = enqueue_messages(mq, 0..15).await?.into_iter().collect();

    let workers = mq
        .start_workers(
            (),
            into_handler(|(), payload: String| async move {
                Err(ErrorOperation::Delay(Box::new(std::io::Error::other(format!(
                    "delayed {payload}"
                )))))
            }),
        )
        .await?;

    wait_for_delayed_len(mq, 15).await?;
    let mut cursor: Option<Cursor> = None;
    let mut collected = Vec::new();
    loop {
        let page = mq.scan_delayed(5, cursor.take()).await?;
        collected.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    assert_queue_ids(collected, &expected);
    assert_eq!(mq.delayed_len().await?, 15);
    tokio::time::timeout(Duration::from_secs(2), workers.shutdown())
        .await
        .map_err(|e| Error::Shutdown(Box::new(e)))??;
    Ok(())
}

#[ignore = "requires Docker for Redis testcontainer"]
#[tokio::test(flavor = "multi_thread")]
async fn cursor_is_opaque_across_concurrent_scans() -> TestResult {
    let setup = setup(MQConfig::default()).await?;
    let mq = &setup.mq;
    enqueue_messages(mq, 0..10).await?;
    let workers = mq
        .start_workers(
            (),
            into_handler(|(), payload: String| async move {
                Err(ErrorOperation::Fail(Box::new(std::io::Error::other(format!(
                    "failed {payload}"
                )))))
            }),
        )
        .await?;
    wait_for_failed_len(mq, 10).await?;

    let (left, right) = tokio::try_join!(mq.scan_failed(5, None), mq.scan_failed(5, None))?;
    assert!(!left.items.is_empty());
    assert!(!right.items.is_empty());

    tokio::time::timeout(Duration::from_secs(2), workers.shutdown())
        .await
        .map_err(|e| Error::Shutdown(Box::new(e)))??;
    Ok(())
}

struct TestSetup {
    mq: TestQueue,
    _container: ContainerAsync<GenericImage>,
}

async fn setup(config: MQConfig) -> Result<TestSetup, Error> {
    let image = GenericImage::new("redis", "7-alpine").with_exposed_port(6379.into());
    let container: ContainerAsync<GenericImage> = image
        .start()
        .await
        .map_err(|e| Error::Backend(Box::new(e)))?;
    let host_port = container
        .get_host_port_ipv4(6379)
        .await
        .map_err(|e| Error::Backend(Box::new(e)))?;
    let redis_url = format!("redis://127.0.0.1:{host_port}");
    let cfg = Config::from_url(redis_url.clone());
    let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
    wait_for_redis(&pool).await?;
    prefer_cursor_friendly_hash_encoding(&pool).await?;
    let mq = RedisMessageQueue::<u64, String>::builder()
        .pool(pool)
        .redis_url(redis_url)
        .name(format!("inspector_test_{}", Uuid::new_v4()))
        .config(config)
        .consumer_id_generator(|| format!("consumer-{}", Uuid::new_v4()))
        .build()?;
    Ok(TestSetup {
        mq,
        _container: container,
    })
}

async fn prefer_cursor_friendly_hash_encoding(pool: &deadpool_redis::Pool) -> Result<(), Error> {
    let mut con = pool.get().await?;
    let _: String = cmd("CONFIG")
        .arg("SET")
        .arg("hash-max-listpack-entries")
        .arg("1")
        .query_async(&mut con)
        .await?;
    Ok(())
}

async fn wait_for_redis(pool: &deadpool_redis::Pool) -> Result<(), Error> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut con) = pool.get().await {
            let ping: Result<String, _> = cmd("PING").query_async(&mut con).await;
            if ping.as_deref() == Ok("PONG") {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "redis testcontainer did not become ready",
            ))));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn enqueue_messages(
    mq: &TestQueue,
    ids: impl Iterator<Item = u64>,
) -> Result<Vec<u64>, Error> {
    let mut enqueued = Vec::new();
    for id in ids {
        mq.enqueue(QueueInfo::new(id, payload_for(id))).await?;
        enqueued.push(id);
    }
    Ok(enqueued)
}

async fn wait_for_failed_len(mq: &TestQueue, expected: u64) -> Result<(), Error> {
    wait_for("failed_len to reach expected count", Duration::from_secs(10), || async {
        Ok((mq.failed_len().await? == expected).then_some(()))
    })
    .await
}

async fn wait_for_delayed_len(mq: &TestQueue, expected: u64) -> Result<(), Error> {
    wait_for("delayed_len to reach expected count", Duration::from_secs(10), || async {
        Ok((mq.delayed_len().await? == expected).then_some(()))
    })
    .await
}

async fn wait_for<T, F, Fut>(description: &'static str, max_wait: Duration, mut condition: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>, Error>>,
{
    tokio::time::timeout(max_wait, async move {
        loop {
            if let Some(value) = condition().await? {
                return Ok(value);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| {
        Error::Backend(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            description,
        )))
    })?
}

fn assert_errored_ids(items: Vec<ErroredInfo<u64, String>>, expected: &HashSet<u64>) {
    assert_eq!(items.len(), expected.len());
    let mut seen = HashSet::new();
    for item in items {
        assert_valid_errored_info(&item);
        assert_eq!(item.data(), &payload_for(*item.id()));
        assert!(seen.insert(*item.id()), "duplicate failed id {}", item.id());
    }
    assert_eq!(&seen, expected);
}

fn assert_queue_ids(items: Vec<QueueInfo<u64, String>>, expected: &HashSet<u64>) {
    assert_eq!(items.len(), expected.len());
    let mut seen = HashSet::new();
    for item in items {
        assert_eq!(item.data(), &payload_for(*item.id()));
        assert!(seen.insert(*item.id()), "duplicate delayed id {}", item.id());
    }
    assert_eq!(&seen, expected);
}

fn assert_valid_errored_info(info: &ErroredInfo<u64, String>) {
    assert!(!info.error().to_string().is_empty());
}

fn payload_for(id: u64) -> String {
    format!("payload-{id}")
}
