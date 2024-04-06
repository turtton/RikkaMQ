# RikkaMQ

Simple message queue library for rust

Depends

- uuid
- serde

### Redis implementation

Depends

- deadpool-redis
- redis
- tokio

```rust
use crate::define::redis::mq::RedisMessageQueue;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueData {
    a: String,
}

#[tokio::main]
async fn main() -> Result<(), rikka_mq::error::Error> {
    let redis_pool = // Create pool
    let name = "mq_name".to_string();
    let mq = RedisMessageQueue::new(redis_pool, (), name, MQConfig::default(), |_, data: TestData| async move {
        println!("{:?}", data);
        Ok(())
    });

    mq.start_workers();

    for i in 0..1000 {
        let data = QueueData {
            a: format!("message:{i}"),
        };
        let data = QueueInfo::from(data);
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
