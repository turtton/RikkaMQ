# RikkaMQ

Simple message queue library for rust

## Retry semantics

A handler can return `Ok(())`, `Err(ErrorOperation::Delay(_))`, or
`Err(ErrorOperation::Fail(_))`.

- `Ok(())` acknowledges the message.
- `Delay` keeps the message in the consumer group's pending list. After
  `MQConfig::retry_delay` has elapsed it is re-delivered to a worker. The
  handler runs at most `max_retry + 1` times in total (one initial attempt
  plus up to `max_retry` re-deliveries). Once that budget is exceeded the
  message is moved to the failed hash.
- `Fail` skips the retry budget and sends the message to the failed hash
  immediately.

Failed messages stay in the failed hash until `retry_failed(&id)` is called
or they are removed manually.

### Redis implementation

Library dependencies

- deadpool-redis
- redis (via deadpool-redis)
- tokio runtime
- tracing(optional)

Example application dependencies also need `serde`, `uuid`, `dotenvy`, and
`tracing-subscriber` if you use the sample below as-is.

Migration note: the Redis implementation moved from
`rikka_mq::define::redis::mq::RedisMessageQueue` to
`rikka_mq::backend::redis::RedisMessageQueue` in v0.2.

```toml
rikka-mq = { version = "0.2.0-alpha.1", features = ["redis", "tracing"] }
```

```rust
use deadpool_redis::{Config, Pool, Runtime};
use rikka_mq::backend::redis::RedisMessageQueue;
use rikka_mq::config::MQConfig;
use rikka_mq::error::ErrorOperation;
use rikka_mq::handler::into_handler;
use rikka_mq::info::QueueInfo;
use rikka_mq::mq::MessageQueue;
use rikka_mq::worker::WorkerControl;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueData {
    a: String,
}

#[derive(Debug)]
struct Module {
    pool: Pool,
}

#[tokio::main]
async fn main() -> Result<(), rikka_mq::error::Error> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    let url = dotenvy::var("REDIS_URL").unwrap();
    let cfg = Config::from_url(url.clone());
    let redis_pool = cfg.create_pool(Some(Runtime::Tokio1))?;
    let module = Arc::new(Module { pool: redis_pool.clone() });
    let mq = RedisMessageQueue::<Uuid, QueueData>::builder()
        .pool(redis_pool)
        .redis_url(url)
        .name("mq_name")
        .config(MQConfig::default())
        .consumer_id_generator(|| format!("consumer-{}", Uuid::new_v4()))
        .build()?;

    let handler = into_handler(|module: Arc<Module>, data: QueueData| async move {
            let pool = &module.pool;
            let _con = pool.get().await
                .map_err(|e| ErrorOperation::Delay(Box::new(e)))?;
            // Any transactions between db
            info!("{:?}, {:?}", module, data);
            Ok(())
    });

    let workers = mq.start_workers(module, handler).await?;

    for i in 0..1000 {
        let data = QueueData {
            a: format!("message:{i}"),
        };
        let data = QueueInfo::new(Uuid::new_v4(), data);
        // Queue
        mq.enqueue(data).await?;
    }

    tokio::signal::ctrl_c()
        .await
        .map_err(|e| rikka_mq::error::Error::Backend(Box::new(e)))?;
    workers.shutdown().await?;
    Ok(())
}
```

The Redis worker path uses the pool for non-blocking commands such as `XADD`,
`XACK`, `XDEL`, `HSCAN`, `XCLAIM`, and retry metadata updates, while each worker
opens one independent connection from `redis_url` for `XREAD BLOCK`. `pool` and
`redis_url` must point to the same Redis deployment/database. This explicit URL
keeps blocking reads out of the deadpool pool, so enqueue and maintenance
operations do not starve when the worker count matches the pool size.

# Development notes

KeyDB(for testing redis implementation)

```shell
podman run --rm --name rikka-mq -p 6379:6379 docker.io/eqalpha/keydb
```

`tests/worker_shutdown.rs` uses `testcontainers` and is marked `#[ignore]`;
run it with `cargo test --test worker_shutdown --all-features -- --ignored`
when Docker is available.

## Benchmarks

Criterion benchmarks live under `benches/`. See `benches/README.md` for the
Redis enqueue starvation benchmark, its `--all-features` invocation, Docker
requirement, and baseline notes.
