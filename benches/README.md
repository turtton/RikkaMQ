# Benchmarks

## enqueue_starvation

`enqueue_starvation` verifies Phase 3 task 15: Redis workers use dedicated
connections for `XREAD BLOCK`, so blocking worker reads do not starve enqueue
(`XADD`) calls through the deadpool pool.

Run it with Docker available:

```shell
cargo bench --all-features --bench enqueue_starvation
```

Each measured iteration starts one Redis `testcontainers` container, configures
`worker_count = 4` with `pool.max_size = 4`, runs a handler that sleeps for two
seconds, lets the workers settle into their blocking read loop, and measures
wall-clock time until one batch of 50 concurrent enqueue calls finishes.
Container startup, worker startup, worker settling, and shutdown are excluded
from the Criterion sample duration.
Because enqueue uses non-blocking pool connections while workers block on
dedicated connections, the absolute pass criteria are:

- median time for the 50-enqueue batch: **1s or less**
- p95 time for the 50-enqueue batch: **2s or less**

The historical v0.1.3 API is too different for this benchmark source to compile
unchanged, so do not generate a v0.1.3 baseline from this file. Evaluate current
code against the absolute criteria above. For v0.2 and later releases, save a
release baseline and compare future runs with Criterion:

```shell
cargo bench --all-features --bench enqueue_starvation -- --save-baseline v020
cargo bench --all-features --bench enqueue_starvation -- --baseline v020
```
