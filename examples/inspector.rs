use deadpool_redis::{Config, Pool, Runtime};
use rikka_mq::backend::redis::RedisMessageQueue;
use rikka_mq::config::{MQConfig, RetryPolicy};
use rikka_mq::error::{Error, ErrorOperation};
use rikka_mq::handler::into_handler;
use rikka_mq::info::{ErroredInfo, QueueInfo};
use rikka_mq::inspect::{Cursor, QueueInspector};
use rikka_mq::mq::MessageQueue;
use rikka_mq::worker::WorkerControl;
use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::num::NonZeroUsize;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use tracing::info;
use uuid::Uuid;

type ExampleResult = Result<(), Box<dyn StdError + Send + Sync>>;
type ExampleQueue = RedisMessageQueue<u64, String>;

#[tokio::main]
async fn main() -> ExampleResult {
    init_tracing();

    let setup = setup_queue(MQConfig {
        worker_count: NonZeroUsize::new(1).expect("example worker_count is non-zero"),
        max_retry: 0,
        retry_policy: RetryPolicy::Fixed(Duration::from_millis(50)),
    })
    .await?;
    let mq = &setup.mq;

    info!("starting inspector workers that fail odd ids and ack even ids");
    let workers = mq
        .start_workers(
            (),
            into_handler(|(), data: String| async move {
                let id = parse_id(&data).map_err(|e| ErrorOperation::Fail(Box::new(e)))?;
                if id % 2 == 1 {
                    Err(ErrorOperation::Fail(Box::new(StringStdError(format!(
                        "forced-fail-for-{id}"
                    )))))
                } else {
                    Ok(())
                }
            }),
        )
        .await?;

    for id in 1..=30 {
        mq.enqueue(QueueInfo::new(id, format!("message-{id}")))
            .await?;
    }
    info!("enqueued 30 messages for failed scan");

    wait_for_failed_len(mq, 15).await?;
    let failed = scan_failed_entries(mq).await?;
    if failed.len() != 15 {
        return Err(Box::new(StringStdError(format!(
            "expected 15 failed entries, scanned {}",
            failed.len()
        ))) as Box<dyn StdError + Send + Sync>);
    }
    info!(count = failed.len(), "scanned failed entries");

    let shutdown_result = tokio::time::timeout(Duration::from_secs(3), workers.shutdown()).await;
    let final_failed = mq.failed_len().await?;
    shutdown_result.map_err(|_| {
        Error::Backend(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "workers did not shut down within 3s",
        )))
    })??;
    info!(
        failed_len = final_failed,
        "inspector example completed cleanly"
    );
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
    prefer_cursor_friendly_hash_encoding(&pool).await?;

    let queue_name = format!("rikka_inspector_{}", Uuid::new_v4());
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

async fn prefer_cursor_friendly_hash_encoding(pool: &Pool) -> Result<(), Error> {
    let mut con = pool.get().await?;
    let _: String = deadpool_redis::redis::cmd("CONFIG")
        .arg("SET")
        .arg("hash-max-listpack-entries")
        .arg("1")
        .query_async(&mut con)
        .await?;
    info!("configured Redis hash encoding for multi-page HSCAN demo");
    Ok(())
}

async fn wait_for_failed_len(mq: &ExampleQueue, expected: u64) -> Result<(), Error> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut next_log = tokio::time::Instant::now();
    loop {
        let failed = mq.failed_len().await?;
        if tokio::time::Instant::now() >= next_log {
            info!(failed_len = failed, expected, "waiting for failed messages");
            next_log += Duration::from_millis(500);
        }
        if failed == expected {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Backend(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "failed_len did not reach expected count within 30s",
            ))));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn scan_failed_entries(mq: &ExampleQueue) -> Result<Vec<ErroredInfo<u64, String>>, Error> {
    let mut cursor: Option<Cursor> = None;
    let mut collected = Vec::new();
    loop {
        let page = mq.scan_failed(7, cursor.take()).await?;
        let page_size = page.items.len();
        collected.extend(page.items);
        info!(
            page_size,
            running_total = collected.len(),
            "scanned failed page"
        );
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(collected),
        }
    }
}

fn parse_id(data: &str) -> Result<u64, StringStdError> {
    data.rsplit_once('-')
        .ok_or_else(|| StringStdError(format!("missing id in {data}")))?
        .1
        .parse()
        .map_err(|e| StringStdError(format!("invalid id in {data}: {e}")))
}

#[derive(Debug, Clone)]
struct StringStdError(String);

impl Display for StringStdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for StringStdError {}

struct ExampleSetup {
    mq: ExampleQueue,
    _container: ContainerAsync<GenericImage>,
}
