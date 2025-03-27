# RikkaMQ

Simple message queue library for rust

### Redis implementation

Dependencies

- deadpool-redis
- redis
- tokio
- tracing(optional)

```toml
rikka-mq = { version = "", features = ["redis", "tracing"] }
```

```rust
use deadpool_redis::{Config, CreatePoolError, Pool, Runtime};
use rikka_mq::config::MQConfig;
use rikka_mq::define::redis::mq::RedisMessageQueue;
use rikka_mq::info::QueueInfo;
use rikka_mq::mq::MessageQueue;
use serde::{Deserialize, Serialize};
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
    let url = dotenvy::var(REDIS_URL).unwrap();
    let cfg = Config::from_url(url);
    let redis_pool = cfg.create_pool(Some(Runtime::Tokio1))?;
    let name = "mq_name".to_string();
    let module = Arc::new(redis_pool.clone());
    let mq = RedisMessageQueue::new(
        redis_pool,
        module,
        name,
        MQConfig::default(),
        UUID::new_v4,
        |module: Arc<Module>, data: TestData| async move {
            let pool = &module.pool;
            let con = pool.get().await
                .map_err(|e| ErrorOperation::Delay(format!("{e:?}")))?;
            // Any transactions between db
            info!("{:?}, {:?}", module, data);
            Ok(())
        },
    );

    mq.start_workers();

    for i in 0..1000 {
        let data = TestData {
            a: format!("message:{i}"),
        };
        let data = QueueInfo::new(Uuid::new_v4(), data);
        // Queue
        mq.queue(data).await?;
    }

    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

# Development notes

KeyDB(for testing redis implementation)

```shell
podman run --rm --name rikka-mq -p 6379:6379 docker.io/eqalpha/keydb
```
