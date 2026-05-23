#![cfg(all(feature = "redis", feature = "tokio"))]

//! Redis retry integration tests. These require Docker because they start a
//! Redis 7-alpine testcontainer.

use deadpool_redis::redis::cmd;
use deadpool_redis::{Config, Runtime};
use rikka_mq::backend::redis::RedisMessageQueue;
use rikka_mq::config::{MQConfig, RetryPolicy};
use rikka_mq::error::{Error, ErrorOperation};
use rikka_mq::handler::into_handler;
use rikka_mq::info::QueueInfo;
use rikka_mq::inspect::QueueInspector;
use rikka_mq::mq::{MessageQueue, QueueStats};
use rikka_mq::worker::WorkerControl;
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

#[ignore = "requires Docker for Redis 7-alpine testcontainer"]
#[tokio::test(flavor = "multi_thread")]
async fn delay_three_times_then_ok_eventually_drains_stream_and_delayed() -> TestResult {
    let setup = setup(MQConfig {
        worker_count: NonZeroUsize::new(1).expect("test worker_count is non-zero"),
        max_retry: 3,
        retry_policy: RetryPolicy::Exponential {
            initial: Duration::from_millis(100),
            factor: 2.0,
            max: Duration::from_secs(1),
        },
    })
    .await?;
    let mq = &setup.mq;
    mq.enqueue(QueueInfo::new(42, "payload".to_string()))
        .await?;

    let invocations = Arc::new(AtomicUsize::new(0));
    let invocations_for_handler = invocations.clone();
    let workers = mq
        .start_workers(
            (),
            into_handler(move |(), _payload: String| {
                let invocations = invocations_for_handler.clone();
                async move {
                    let attempt = invocations.fetch_add(1, Ordering::SeqCst) + 1;
                    if attempt <= 3 {
                        Err(ErrorOperation::Delay(Box::new(std::io::Error::other(
                            format!("delayed attempt {attempt}"),
                        ))))
                    } else {
                        Ok(())
                    }
                }
            }),
        )
        .await?;

    wait_for(
        "retry to drain stream and delayed hash",
        Duration::from_secs(20),
        || async {
            let queued = mq.queued_len().await?;
            let delayed = mq.delayed_len().await?;
            let failed = mq.failed_len().await?;
            let count = invocations.load(Ordering::SeqCst);
            Ok((queued == 0 && delayed == 0 && failed == 0 && count == 4).then_some(()))
        },
    )
    .await?;

    assert_eq!(mq.queued_len().await?, 0);
    assert_eq!(mq.delayed_len().await?, 0);
    assert_eq!(mq.failed_len().await?, 0);
    assert_eq!(invocations.load(Ordering::SeqCst), 4);
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
    let mq = RedisMessageQueue::<u64, String>::builder()
        .pool(pool)
        .redis_url(redis_url)
        .name(format!("retry_test_{}", Uuid::new_v4()))
        .config(config)
        .consumer_id_generator(|| format!("consumer-{}", Uuid::new_v4()))
        .build()?;
    Ok(TestSetup {
        mq,
        _container: container,
    })
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

async fn wait_for<T, F, Fut>(
    description: &'static str,
    max_wait: Duration,
    mut condition: F,
) -> Result<T, Error>
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
