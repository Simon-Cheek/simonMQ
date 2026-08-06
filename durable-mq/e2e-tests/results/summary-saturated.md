# Benchmark summary

Load generator host(s): simon

Offered load: 1200 msg/s. Baseline arm: `durable-off`.
Throughput and latency are medians across repetitions; brackets are the interquartile range.

| Arm | Reps | Accepted msg/s (IQR) | vs durable-off | p50 ms (IQR) | p99 ms (IQR) | Failed | Saturated |
|---|---|---|---|---|---|---|---|
| `push-mq` | 10 | 1200 [1200–1200] | 1.00x | 0.64 [0.63–0.64] | 1.14 [1.14–1.14] | 0 | no |
| `durable-off` | 10 | 1200 [1200–1200] | — | 0.64 [0.64–0.65] | 1.14 [1.14–1.15] | 0 | no |
| `durable-nosync` | 10 | 1200 [1200–1200] | 1.00x | 0.65 [0.65–0.65] | 1.16 [1.15–1.16] | 0 | no |
| `durable-sync` | 10 | 1200 [1200–1200] | 1.00x | 7277.87 [7207.02–7411.43] | 12237.45 [12054.26–12420.61] | 0 | **delivery** |

## Delivery

| Arm | Accepted | Delivered | Ratio |
|---|---|---|---|
| `push-mq` | 300000 | 300000 | 1.000 |
| `durable-off` | 300000 | 300000 | 1.000 |
| `durable-nosync` | 300000 | 300000 | 1.000 |
| `durable-sync` | 300000 | 637 | 0.002 |

Both counts cover the measurement window only. A ratio slightly below 1 is expected — publishes are acknowledged before delivery happens, so whatever is in flight when the run stops is never counted. A ratio far below 1 means the delivery path could not keep up with the publish path, and any latency figure from that arm is a queueing delay rather than a service time.
