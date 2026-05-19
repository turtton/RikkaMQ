#![cfg(all(feature = "redis", feature = "tokio"))]

use deadpool_redis::redis::cmd;
use deadpool_redis::{Config, Runtime};
use rikka_mq::backend::redis::RedisMessageQueue;
use rikka_mq::config::MQConfig;
use rikka_mq::error::Error;
use rikka_mq::handler::into_handler;
use rikka_mq::info::QueueInfo;
use rikka_mq::mq::MessageQueue;
use rikka_mq::worker::WorkerControl;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payload {
    value: usize,
}

#[ignore = "requires Docker for Redis testcontainer"]
#[tokio::test]
async fn worker_shutdown_joins_within_five_seconds() -> Result<(), Error> {
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
    let mq = RedisMessageQueue::<Uuid, Payload>::builder()
        .pool(pool)
        .redis_url(redis_url)
        .name(format!("worker_shutdown_{}", Uuid::new_v4()))
        .config(MQConfig {
            worker_count: NonZeroUsize::new(2).expect("test worker_count is non-zero"),
            max_retry: 0,
            retry_delay: Duration::from_millis(10),
        })
        .consumer_id_generator(|| format!("consumer-{}", Uuid::new_v4()))
        .build()?;
    let completed = Arc::new(AtomicUsize::new(0));
    let seen = completed.clone();
    let workers = mq
        .start_workers(
            (),
            into_handler(move |(), _payload: Payload| {
                let seen = seen.clone();
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }),
        )
        .await?;

    for value in 0..5 {
        mq.enqueue(QueueInfo::new(Uuid::new_v4(), Payload { value }))
            .await?;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while completed.load(Ordering::SeqCst) < 5 {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Shutdown(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "workers did not complete five messages",
            ))));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    tokio::time::timeout(Duration::from_secs(5), workers.shutdown())
        .await
        .map_err(|e| Error::Shutdown(Box::new(e)))??;
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
