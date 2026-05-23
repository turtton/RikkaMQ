# RikkaMQ

Simple message queue library for rust

## Retry semantics

A handler can return `Ok(())`, `Err(ErrorOperation::Delay(_))`, or
`Err(ErrorOperation::Fail(_))`.

- `Ok(())` acknowledges the message.
- `Delay` records the message in the delayed hash and leaves it pending until
  its retry policy makes it eligible. The handler runs at most `max_retry + 1`
  times in total (one initial attempt plus up to `max_retry` re-deliveries).
  Once that budget is exceeded the message is moved to the failed hash.
- `Fail` skips the retry budget and sends the message to the failed hash
  immediately.

Delayed and failed messages stay in their respective hashes until they are
removed manually or moved back to the queue.

Retries are driven by the Redis Streams PEL (XPENDING + XCLAIM with
MIN-IDLE-TIME). The configured `RetryPolicy` determines per-attempt idle
threshold; workers may observe a slightly larger effective delay under load.
`min_delay()` is used only as a coarse server-side prefilter.

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
use rikka_mq::config::{MQConfig, RetryPolicy};
use rikka_mq::error::ErrorOperation;
use rikka_mq::handler::into_handler;
use rikka_mq::info::QueueInfo;
use rikka_mq::mq::MessageQueue;
use rikka_mq::worker::WorkerControl;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
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
        .config(MQConfig {
            retry_policy: RetryPolicy::Fixed(Duration::from_secs(180)),
            ..MQConfig::default()
        })
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

`RetryPolicy::Exponential { initial, factor, max }` is also available when retry intervals should grow between attempts.

## Operational surface

Quick reference of traits and types used for queue operations and inspection:

- `MessageQueue<I, T>`: Core trait for enqueuing messages.
- `QueueStats`: Provides `queued_len()` to check the number of messages in the active stream.
- `QueueInspector<I, T>`: Tools for inspecting delayed and failed messages (depth, get, scan).
- `FailedRetry<I>`: Provides `retry_failed(&id)` to replay messages from the failed hash.
- `Cursor` / `ScanPage<T>`: Opaque cursor and result page for incremental scanning.
- `ErroredInfo<I, T>`: Metadata for failed messages including the original error.

### Inspecting queue depth

Assuming you already have `mq` from the example above, you can check the live depth of each internal state:

```rust
use rikka_mq::inspect::QueueInspector;
use rikka_mq::mq::QueueStats;

// Active stream depth (includes messages waiting for delivery and in-flight)
let queued = mq.queued_len().await?;
// Messages recorded as delayed after a Delay error
let delayed = mq.delayed_len().await?;
// Messages that exceeded retry budget or returned a Fail error
let failed = mq.failed_len().await?;
```

- `queued_len`: Monitor this to scale workers or detect ingestion spikes.
- `delayed_len`: High numbers here indicate frequent transient failures or slow processing of retries.
- `failed_len`: These require manual intervention or a `retry_failed` call.

### Paging through failed/delayed entries

Iteration over failed or delayed messages uses an opaque `Cursor`. This ensures large sets can be scanned incrementally without blocking the Redis server.

```rust
use rikka_mq::inspect::{Cursor, QueueInspector};
use tracing::info;

let mut cursor: Option<Cursor> = None;
loop {
    let page = mq.scan_failed(100, cursor).await?;
    for item in page.items {
        info!("ID: {:?}, Data: {:?}, Error: {}", item.id(), item.data(), item.error());
    }
    cursor = page.next_cursor;
    if cursor.is_none() {
        break;
    }
}
```

`ScanPage::items` contains `Vec<ErroredInfo<I, T>>` for `scan_failed` and `Vec<QueueInfo<I, T>>` for `scan_delayed`. Note that `Cursor` is an opaque token intended for use as a continuation for the same scan.

### Replaying a failed message

When a message is in the failed hash, you can move it back to the active queue for another delivery attempt.

```rust
use rikka_mq::inspect::FailedRetry;

// Replay a message by its ID
mq.retry_failed(&id).await?;
```

RikkaMQ uses enqueue-before-delete semantics: if the process crashes between enqueuing and deleting from the failed hash, the message might be duplicated but is never lost. Retried messages re-enter the queue fresh and respect the configured `retry_policy`.

The Redis worker path uses the pool for non-blocking commands such as `XADD`,
`XACK`, `XDEL`, `HSCAN`, `XCLAIM`, and retry metadata updates, while each worker
opens one independent connection from `redis_url` for `XREAD BLOCK`. `pool` and
`redis_url` must point to the same Redis deployment/database. This explicit URL
keeps blocking reads out of the deadpool pool, so enqueue and maintenance
operations do not starve when the worker count matches the pool size.

## Examples

Runnable Redis examples live under `examples/` and start `redis:7-alpine` with
testcontainers, so a local Docker daemon is required.

- `basic`: enqueue, handler ack, queue depth polling, and graceful shutdown.
- `inspector`: failed-message paging with `Cursor` and `ErroredInfo`.
- `retry`: exponential retry backoff plus `retry_failed` replay.

Run one with `cargo run --example basic --features examples`.

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
requirement, and baseline notes. The v0.2.0-alpha.1 baseline records a median
of ~6.75 ms and a p95 of ~7.66 ms for a 50-enqueue batch against
`redis:7-alpine`, well within the 1s / 2s pass criteria.
