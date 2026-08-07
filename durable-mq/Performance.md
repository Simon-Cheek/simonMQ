# Performance and Durability Analysis

## Durability

Unit and acceptance tests were run to ensure the stability of `durable-mq` and that the queue guaranteed
the maximum attempts of delivery (or success) despite a crash which could occur at any time.

- Tested that all messages acked by the queue eventually are sent to all subscribers (or maxed out attempts)
- Tested that messages and queue state are preserved upon checkpointing
- Tested that WAL handles a crash during recovery
- Tested that WAL handles a crash during checkpointing
- Tested that WAL handles a crash during recovery after a checkpoint
- Tested that WAL still works correctly when under high load
- Tested that heavy traffic is sustainable and durable even during checkpointing

The tests were done through the usage of scripts that consistently ran and reran an instance of the queue
while multiple publishers and subscribers.

Tests were run on a Debian VM on Proxmox running on a Mini PC with 32 GB RAM and 6c/12t.

## Performance

Four versions of the queue were tested under load:
- `push-mq`, with no durability guarantees and a simpler implementation
- `durable-off`, the durable-mq base structure with no WAL used
- `durable-nosync`, the durable-mq with the WAL but without `fsync`, a primary bottleneck to performance
- `durable-sync` the full durable-mq with `fsync`

Two types of tests were ran:
- Standard traffic levels: 225 msgs/second
- High traffic levels: 1200 msgs/second

In each test, 3 queues managed messages sent between 3 publishers and 3 subscribers. Each publisher sent messages
to all 3 queues and each queue sent messages to each subscriber. Latency was measured as the duration between
initiating an enqueue and receiving a `202 ACCEPTED` response from the queue.

Performance measurements showed that `durable-sync` has a max throughput of about 888 msgs/second.
High traffic level tests show how `durable-sync` is able to handle backpressure - and how it exposes some design issues.

Each type of test was ran 10 times with raw data aggregated to produce p50 and p99 latency metrics.
Throughput levels were sustained for 30 seconds each time with a 5-second warmup time.

### Standard Traffic Levels (225 msgs/second)

Measures latency stats against each of the 4 systems.

| System           | Accepted msg/s | p50 ms | p99 ms      |
|------------------|---|---|-------------|
| `push-mq`        | 225 | 0.80 | 1.62 |
| `durable-off`    | 225 | 0.80 | 1.68 |
| `durable-nosync` | 225 | 0.83 | 1.58 |
| `durable-sync`   | 225 | **3.11** | **13.30**   |


### Heavy Traffic Levels (1200 msgs/second)

Measures latency stats against each of the 4 systems.

| System           | Accepted msg/s | p50 ms   | p99 ms    |
|------------------|----------------|----------|-----------|
| `push-mq`        | 1200           | 0.63     | 1.14      |
| `durable-off`    | 1200           | 0.64     | 1.14      |
| `durable-nosync` | 1200           | 0.64     | 1.16      |
| `durable-sync`   | 888            | **6296** | **10713** |


### Discussion

`durable-sync` eventually caps out at around 888 msgs/second, but it is important to note that for
each message accepted by the system, subscribers received each one. All messages survived the heavy throughput.
Not a single message was lost across all repetitions of the test.

*`durable-sync` has a major design flaw.* During the 30 seconds of heavy load, `durable-sync` would only 
deliver about 600 of the messages in total. The rest were delivered after the period of high traffic once there
room for the queue workers to access the queues. The workers would be more likely to contend less with the publishers 
in an environment with more than 3 queues, but the problem still stands.

### Future Work

Changes need to be implemented to address some of the performance bottlenecks:
- Group commits for the WAL
- Granular locking to prevent mutex contention between enqueues and delivery workers
- Chunking of WAL reads (if WAL grows unbounded, recovery could cause the queue to exceed heap size)