#![cfg(all(feature = "redis", feature = "tokio"))]

use rikka_mq::backend::redis::RedisMessageQueue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
struct MyData {
    value: String,
}

#[allow(dead_code)]
fn _compile_only() -> Result<(), rikka_mq::error::Error> {
    use deadpool_redis::{Config, Runtime};
    use rikka_mq::config::MQConfig;
    use rikka_mq::info::QueueInfo;
    use rikka_mq::mq::MessageQueue;
    use rikka_mq::worker::WorkerControl;

    fn assert_worker_control<Q>(queue: &Q)
    where
        Q: WorkerControl<(), Uuid, MyData>,
    {
        let _ = queue;
    }

    let config = Config::from_url("redis://127.0.0.1:6379");
    let pool = config.create_pool(Some(Runtime::Tokio1))?;
    let queue = RedisMessageQueue::<Uuid, MyData>::builder()
        .pool(pool)
        .redis_url("redis://127.0.0.1:6379")
        .name("compile-only")
        .config(MQConfig::default())
        .consumer_id_generator(|| format!("consumer-{}", Uuid::new_v4()))
        .build()?;

    assert_worker_control(&queue);
    let data = MyData {
        value: "payload".to_string(),
    };
    let enqueue_future = queue.enqueue(QueueInfo::new(Uuid::new_v4(), data));
    drop(enqueue_future);
    Ok(())
}

#[allow(dead_code)]
async fn _compile_worker_lifecycle(
    queue: RedisMessageQueue<Uuid, MyData>,
) -> Result<(), rikka_mq::error::Error> {
    use rikka_mq::error::ErrorOperation;
    use rikka_mq::handler::into_handler;
    use rikka_mq::worker::WorkerControl;

    let handler = into_handler(|(): (), _data: MyData| async move {
        if std::hint::black_box(false) {
            return Err(ErrorOperation::Fail(Box::new(std::io::Error::other(
                "compile-only",
            ))));
        }
        Ok(())
    });
    let workers = queue.start_workers((), handler).await?;
    if std::hint::black_box(false) {
        workers.join().await?;
    } else {
        workers.shutdown().await?;
    }
    Ok(())
}

#[test]
fn public_api_compile_smoke_is_present() {
    let _ = _compile_only;
    let _ = _compile_worker_lifecycle;
}
