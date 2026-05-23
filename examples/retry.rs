use deadpool_redis::{Config, Pool, Runtime};
use rikka_mq::backend::redis::RedisMessageQueue;
use rikka_mq::config::{MQConfig, RetryPolicy};
use rikka_mq::error::{Error, ErrorOperation};
use rikka_mq::handler::into_handler;
use rikka_mq::info::QueueInfo;
use rikka_mq::inspect::QueueInspector;
use rikka_mq::mq::{MessageQueue, QueueStats};
use rikka_mq::worker::WorkerControl;
use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::num::NonZeroUsize;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
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
        max_retry: 3,
        retry_policy: RetryPolicy::Exponential {
            initial: Duration::from_millis(100),
            factor: 2.0,
            max: Duration::from_secs(2),
        },
    })
    .await?;
    let mq = &setup.mq;

    let transient_attempts = Arc::new(AtomicU32::new(0));
    let attempts_for_handler = transient_attempts.clone();

    info!("starting retry worker for exponential backoff demo");
    let workers = mq
        .start_workers(
            (),
            into_handler(move |(), data: String| {
                let attempts = attempts_for_handler.clone();
                async move {
                    let id = parse_id(&data).map_err(|e| ErrorOperation::Fail(Box::new(e)))?;
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    info!(id, attempt, "handling transient message");
                    if attempt <= 3 {
                        Err(ErrorOperation::Delay(Box::new(StringStdError(format!(
                            "transient-delay-attempt-{attempt}"
                        )))))
                    } else {
                        Ok(())
                    }
                }
            }),
        )
        .await?;

    mq.enqueue(QueueInfo::new(42, "transient-error-42".to_string()))
        .await?;
    info!("enqueued transient message id=42");
    wait_for_handler_invocation(&transient_attempts).await?;
    wait_for_delayed_observation(mq, &transient_attempts).await?;
    wait_for_all_clear(mq).await?;
    wait_for_invocation_count(&transient_attempts, 4).await?;
    let retries = transient_attempts.load(Ordering::SeqCst).saturating_sub(1);
    let queued_len = mq.queued_len().await?;
    let delayed_len = mq.delayed_len().await?;
    let failed_len = mq.failed_len().await?;
    info!(
        retries,
        handler_invocations = transient_attempts.load(Ordering::SeqCst),
        queued_len,
        delayed_len,
        failed_len,
        "message id=42 succeeded after retries"
    );

    let shutdown_result = tokio::time::timeout(Duration::from_secs(2), workers.shutdown()).await;
    shutdown_result.map_err(|_| {
        Error::Backend(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "workers did not shut down within 2s",
        )))
    })??;
    info!(
        queued_len,
        delayed_len, failed_len, "retry example completed cleanly"
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

    let queue_name = format!("rikka_retry_{}", Uuid::new_v4());
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

async fn wait_for_handler_invocation(counter: &AtomicU32) -> Result<(), Error> {
    wait_for("handler invocation", || async {
        Ok((counter.load(Ordering::SeqCst) > 0).then_some(()))
    })
    .await
}

async fn wait_for_delayed_observation(mq: &ExampleQueue, counter: &AtomicU32) -> Result<(), Error> {
    wait_for("delayed retry observation", || async {
        let delayed = mq.delayed_len().await?;
        let attempts = counter.load(Ordering::SeqCst);
        info!(
            delayed_len = delayed,
            delivered_count = attempts,
            "observing retry state"
        );
        Ok((delayed > 0).then_some(()))
    })
    .await
}

async fn wait_for_all_clear(mq: &ExampleQueue) -> Result<(), Error> {
    wait_for("queue and delayed hashes to clear", || async {
        let queued = mq.queued_len().await?;
        let delayed = mq.delayed_len().await?;
        info!(
            queued_len = queued,
            delayed_len = delayed,
            "waiting for transient success"
        );
        Ok((queued + delayed == 0).then_some(()))
    })
    .await
}

async fn wait_for_invocation_count(counter: &AtomicU32, expected: u32) -> Result<(), Error> {
    wait_for("handler invocation count", || async {
        let invocations = counter.load(Ordering::SeqCst);
        Ok((invocations == expected).then_some(()))
    })
    .await
}

async fn wait_for<T, F, Fut>(description: &'static str, mut condition: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>, Error>>,
{
    tokio::time::timeout(Duration::from_secs(30), async move {
        loop {
            if let Some(value) = condition().await? {
                return Ok(value);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
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
