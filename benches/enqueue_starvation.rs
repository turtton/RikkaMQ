use criterion::{criterion_group, Criterion};

const CRITERION_HELP: &str = r#"Criterion Benchmark

Usage: enqueue_starvation [OPTIONS] [FILTER]

Arguments:
  [FILTER]  Skip benchmarks whose names do not contain FILTER.

Options:
      --save-baseline <save-baseline>  Save results under a named baseline.
      --baseline <baseline>            Compare to a named baseline.
      --list                           List all benchmarks.
      --sample-size <sample-size>      Changes the default size of the sample for this run.
      --measurement-time <seconds>     Changes the default measurement time for this run.
  -h, --help                           Print help.
"#;

mod bench {
    use criterion::{BenchmarkId, Criterion, Throughput};
    use deadpool_redis::redis::cmd;
    use deadpool_redis::{Config, Pool, PoolConfig, Runtime};
    use rikka_mq::backend::redis::RedisMessageQueue;
    use rikka_mq::config::MQConfig;
    use rikka_mq::error::Error;
    use rikka_mq::handler::into_handler;
    use rikka_mq::info::QueueInfo;
    use rikka_mq::mq::MessageQueue;
    use rikka_mq::worker::{WorkerControl, WorkerSet};
    use serde::{Deserialize, Serialize};
    use std::num::NonZeroUsize;
    use std::time::Duration;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, GenericImage};
    use tokio::runtime::Runtime as TokioRuntime;
    use uuid::Uuid;

    const ENQUEUE_COUNT: u64 = 50;
    const WORKER_COUNT: usize = 4;
    const HANDLER_SLEEP: Duration = Duration::from_secs(2);
    const WORKER_SETTLE: Duration = Duration::from_millis(200);

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Payload {
        value: u64,
    }

    pub fn register(c: &mut Criterion) {
        let runtime = TokioRuntime::new().expect("tokio runtime for enqueue starvation bench");
        let mut group = c.benchmark_group("enqueue_starvation");
        group.throughput(Throughput::Elements(ENQUEUE_COUNT));
        group.bench_function(BenchmarkId::new("pool_equals_workers", ENQUEUE_COUNT), |b| {
            b.iter_custom(|iterations| {
                runtime.block_on(async move {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let mut state = BenchState::start().await.expect("bench setup should succeed");
                        elapsed += state.measure_enqueue_wall_clock().await;
                        state.shutdown().await.expect("workers should shut down cleanly");
                    }
                    elapsed
                })
            });
        });
        group.finish();
    }

    struct BenchState {
        _container: ContainerAsync<GenericImage>,
        mq: RedisMessageQueue<Uuid, Payload>,
        workers: Option<WorkerSet>,
    }

    impl BenchState {
        async fn start() -> Result<Self, Error> {
            let image = GenericImage::new("redis", "7-alpine").with_exposed_port(6379.into());
            let container = image
                .start()
                .await
                .map_err(|e| Error::Backend(Box::new(e)))?;
            let host_port = container
                .get_host_port_ipv4(6379)
                .await
                .map_err(|e| Error::Backend(Box::new(e)))?;
            let redis_url = format!("redis://127.0.0.1:{host_port}");
            let mut cfg = Config::from_url(redis_url.clone());
            cfg.pool = Some(PoolConfig::new(WORKER_COUNT));
            let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
            wait_for_redis(&pool).await?;

            let mq = RedisMessageQueue::<Uuid, Payload>::builder()
                .pool(pool)
                .redis_url(redis_url)
                .name(format!("bench_starvation_{}", Uuid::new_v4()))
                .config(MQConfig {
                    worker_count: NonZeroUsize::new(WORKER_COUNT)
                        .expect("bench worker_count is non-zero"),
                    max_retry: 0,
                    retry_delay: Duration::from_secs(1),
                })
                .consumer_id_generator(|| Uuid::new_v4().to_string())
                .build()?;
            let handler = into_handler(|(), _payload: Payload| async move {
                tokio::time::sleep(HANDLER_SLEEP).await;
                Ok(())
            });
            let workers = mq.start_workers((), handler).await?;
            tokio::time::sleep(WORKER_SETTLE).await;

            Ok(Self {
                _container: container,
                mq,
                workers: Some(workers),
            })
        }

        async fn measure_enqueue_wall_clock(&self) -> Duration {
            let started = tokio::time::Instant::now();
            let handles = (0..ENQUEUE_COUNT).map(|value| {
                let mq = self.mq.clone();
                tokio::spawn(async move {
                    mq.enqueue(QueueInfo::new(Uuid::new_v4(), Payload { value }))
                        .await
                })
            });

            for handle in handles {
                handle
                    .await
                    .expect("enqueue task should not panic")
                    .expect("enqueue should complete");
            }
            started.elapsed()
        }

        async fn shutdown(&mut self) -> Result<(), Error> {
            if let Some(workers) = self.workers.take() {
                workers.shutdown().await?;
            }
            Ok(())
        }
    }

    async fn wait_for_redis(pool: &Pool) -> Result<(), Error> {
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
}

fn enqueue_starvation(c: &mut Criterion) {
    bench::register(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_secs(10));
    targets = enqueue_starvation
}

fn main() {
    if std::env::args_os().any(|arg| arg == "--help" || arg == "-h") {
        println!("{CRITERION_HELP}");
        return;
    }

    benches();
    Criterion::default().configure_from_args().final_summary();
}
