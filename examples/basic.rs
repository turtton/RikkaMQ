use deadpool_redis::{Config, Pool, Runtime};
use rikka_mq::backend::redis::RedisMessageQueue;
use rikka_mq::config::{MQConfig, RetryPolicy};
use rikka_mq::error::Error;
use rikka_mq::handler::into_handler;
use rikka_mq::info::QueueInfo;
use rikka_mq::mq::{MessageQueue, QueueStats};
use rikka_mq::worker::WorkerControl;
use std::num::NonZeroUsize;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use tracing::info;
use uuid::Uuid;

type ExampleResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
type ExampleQueue = RedisMessageQueue<u64, String>;

#[tokio::main]
async fn main() -> ExampleResult {
    init_tracing();

    let setup = setup_queue(MQConfig {
        worker_count: NonZeroUsize::new(2).expect("example worker_count is non-zero"),
        max_retry: 3,
        retry_policy: RetryPolicy::Fixed(Duration::from_millis(500)),
    })
    .await?;
    let mq = &setup.mq;

    info!("starting two workers for basic enqueue/ack demo");
    let workers = mq
        .start_workers(
            (),
            into_handler(|(), data: String| async move {
                info!("processing: {data}");
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(())
            }),
        )
        .await?;

    for id in 1..=5 {
        let data = format!("hello-{id}");
        mq.enqueue(QueueInfo::new(id, data)).await?;
    }
    info!("enqueued 5 messages");

    wait_for_empty_queue(mq).await?;
    info!("queue is empty; waiting for Ctrl-C or 2s fallback before shutdown");

    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|e| Error::Backend(Box::new(e)))?;
            info!("Ctrl-C received; shutting workers down");
        }
        () = tokio::time::sleep(Duration::from_secs(2)) => {
            info!("fallback shutdown timer fired");
        }
    }

    let shutdown_result = tokio::time::timeout(Duration::from_secs(3), workers.shutdown()).await;
    let final_depth = mq.queued_len().await?;
    shutdown_result
        .map_err(|_| Error::Backend(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "workers did not shut down within 3s",
        ))))??;
    info!(queued_len = final_depth, "basic example completed cleanly");
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

async fn setup_queue(config: MQConfig) -> Result<ExampleSetup, Error> {
    info!("starting redis:7-alpine testcontainer");
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
    let pool = Config::from_url(redis_url.clone()).create_pool(Some(Runtime::Tokio1))?;
    wait_for_redis(&pool).await?;

    let queue_name = format!("rikka_basic_{}", Uuid::new_v4());
    info!(%redis_url, %queue_name, "building RedisMessageQueue<u64, String>");
    let mq = RedisMessageQueue::<u64, String>::builder()
        .pool(pool)
        .redis_url(redis_url)
        .name(queue_name)
        .config(config)
        .consumer_id_generator(|| format!("consumer-{}", Uuid::new_v4()))
        .build()?;

    Ok(ExampleSetup {
        mq,
        _container: container,
    })
}

async fn wait_for_redis(pool: &Pool) -> Result<(), Error> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut con) = pool.get().await {
            let ping: Result<String, _> = deadpool_redis::redis::cmd("PING")
                .query_async(&mut con)
                .await;
            if ping.as_deref() == Ok("PONG") {
                info!("redis testcontainer is ready");
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "redis testcontainer did not become ready",
            ))));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_empty_queue(mq: &ExampleQueue) -> Result<(), Error> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let queued = mq.queued_len().await?;
        info!(queued_len = queued, "observed queue depth");
        if queued == 0 {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "queue did not drain within 30s",
            ))));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

struct ExampleSetup {
    mq: ExampleQueue,
    _container: ContainerAsync<GenericImage>,
}
